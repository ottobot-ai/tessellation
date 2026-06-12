package io.constellationnetwork.currency.l0.snapshot.services

import cats.Applicative
import cats.data.NonEmptySet
import cats.effect.std.Supervisor
import cats.effect.{Async, Temporal}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.currency.l0.metrics.updateStateChannelRetryParametersMetrics
import io.constellationnetwork.domain.allowance_list.AllowanceListEntry
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.node.shared.domain.cluster.storage.L0ClusterStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.http.p2p.clients.StateChannelSnapshotClient
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.IdentifierStorage
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.{Hashed, Hasher}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import org.typelevel.log4cats.slf4j.Slf4jLogger

trait StateChannelBinarySender[F[_]] {
  def enqueue(
    binary: Hashed[StateChannelSnapshotBinary],
    currencySnapshotOrdinal: SnapshotOrdinal,
    lastGlobalSnapshotSigners: Option[NonEmptySet[PeerId]]
  ): F[Unit]

  /** Confirm binaries that landed in this finality-gated global snapshot, advance the GC watermark to gl0's authoritative currency ord
    * (from GSI), and prune below finality.
    *
    * `gl0KnownCurrencyOrd` is the authoritative source for stale-Pending GC — it MUST come from
    * `GlobalSnapshotInfo.lastCurrencySnapshots[ourIdentifier].ordinal`, not inferred from local hash matches. Pass `None` to skip GC (e.g.
    * tests, or when GSI isn't available for the caller). See `BinaryTracker.markAsConfirmed`.
    */
  def confirm(
    globalSnapshot: Hashed[GlobalIncrementalSnapshot],
    gl0KnownCurrencyOrd: Option[SnapshotOrdinal] = None,
    lastFinalizedGlobalOrdinal: Option[SnapshotOrdinal] = None
  ): F[Unit]

  /** Soft-confirm via an unfinalized (best-tip) gl0 snapshot. Operational signal only — tracks which pending binaries have landed in gl0
    * best-tip so retry-mode can keep cap healthy without waiting on G1 finality. Does NOT prune, does NOT promote Pending→Confirmed (a
    * best-tip reorg cannot strand a binary). #123.
    */
  def softConfirm(globalSnapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit]

  def clearPending: F[Unit]
}

object StateChannelBinarySender {
  def make[F[_]: Async: Hasher: Metrics](
    identifierStorage: IdentifierStorage[F],
    globalL0ClusterStorage: L0ClusterStorage[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    stateChannelSnapshotClient: StateChannelSnapshotClient[F],
    stateChannelAllowanceLists: Option[Map[Address, NonEmptySet[PeerId]]],
    selfId: PeerId,
    environment: AppEnvironment,
    customPeersAllowanceList: Option[Set[AllowanceListEntry]]
  )(implicit S: Supervisor[F]): F[StateChannelBinarySender[F]] = {
    val logger = Slf4jLogger.getLoggerFromName(this.getClass.getName)

    for {
      tracker <- BinaryTracker.make[F]
      poster = new BinaryPoster[F](
        identifierStorage,
        globalL0ClusterStorage,
        stateChannelSnapshotClient,
        stateChannelAllowanceLists,
        selfId,
        environment,
        customPeersAllowanceList,
        tracker
      )
      retryTickR <- cats.effect.Ref.of[F, Long](0L)
      sender = new StateChannelBinarySenderImpl[F](
        tracker,
        poster,
        lastGlobalSnapshotStorage,
        identifierStorage,
        retryTickR,
        logger
      )
      _ <- startBackgroundWorker(sender, lastGlobalSnapshotStorage, logger)
    } yield sender
  }

  private def startBackgroundWorker[F[_]: Async](
    sender: StateChannelBinarySenderImpl[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    logger: org.typelevel.log4cats.SelfAwareStructuredLogger[F]
  )(implicit S: Supervisor[F]): F[Unit] = {

    def runWorker: F[Unit] =
      fs2.Stream
        .awakeEvery[F](5.seconds)
        .evalMap(_ =>
          lastGlobalSnapshotStorage.get.flatMap {
            case Some(snapshot) =>
              sender.processQueue(snapshot)
            case None =>
              sender.processQueueWithoutSnapshot
          }
        )
        .compile
        .drain
        .handleErrorWith { err =>
          logger.error(err)("[Queue] Worker crashed, restarting in 1 second") >>
            Temporal[F].sleep(1.second) >>
            runWorker
        }

    S.supervise(runWorker).void
  }

  private class StateChannelBinarySenderImpl[F[_]: Async: Hasher: Metrics](
    tracker: BinaryTracker[F],
    poster: BinaryPoster[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    identifierStorage: IdentifierStorage[F],
    // RetryMode tick counter — drives the re-send cadence gate (see processRetryMode). Ref (cats-idiomatic), allocated in `make`.
    retryTickCounter: cats.effect.Ref[F, Long],
    logger: org.typelevel.log4cats.SelfAwareStructuredLogger[F]
  )(implicit S: Supervisor[F])
      extends StateChannelBinarySender[F] {

    def enqueue(
      binary: Hashed[StateChannelSnapshotBinary],
      currencySnapshotOrdinal: SnapshotOrdinal,
      lastGlobalSnapshotSigners: Option[NonEmptySet[PeerId]]
    ): F[Unit] =
      for {
        currentGlobalOrdinal <- lastGlobalSnapshotStorage.getOrdinal.map(_.getOrElse(SnapshotOrdinal.MinValue))
        _ <- tracker.enqueue(binary, currencySnapshotOrdinal, currentGlobalOrdinal)
        _ <- logger.info(s"[Queue] Enqueued binary ${binary.hash} at ordinal $currencySnapshotOrdinal")
      } yield ()

    def confirm(
      globalSnapshot: Hashed[GlobalIncrementalSnapshot],
      gl0KnownCurrencyOrd: Option[SnapshotOrdinal] = None,
      lastFinalizedGlobalOrdinal: Option[SnapshotOrdinal] = None
    ): F[Unit] =
      for {
        identifier <- identifierStorage.get
        confirmedHashes <- getConfirmedHashes(identifier, globalSnapshot)
        state <- tracker.getState
        oldRetryMode = state.retryMode
        proof = GlobalSnapshotConfirmationProof.fromGlobalSnapshot(globalSnapshot)
        _ <- tracker.markAsConfirmed(confirmedHashes, proof, gl0KnownCurrencyOrd)
        updatedState <- tracker.getState
        retryMode = RetryStrategy.shouldEnterRetryMode(updatedState, globalSnapshot.ordinal)
        _ <- tracker.updateState(_.copy(retryMode = retryMode))
        _ <- tracker.updateState(RetryStrategy.updateRetryParameters(_, oldRetryMode))
        // Finality-gated pruning: defaults to the snapshot's own ordinal (BFT — every snapshot
        // is immediately final). For Nakamoto GL0, the caller MUST supply the actual finalized
        // ordinal from GL0's finality endpoint or confirmed binaries get dropped before the
        // containing GL0 snapshot is durable, losing them on reorg. See task #6/#7 in
        // NAKAMOTO-PLAN.md for the wire-up.
        finalizedOrdinal = lastFinalizedGlobalOrdinal.getOrElse(globalSnapshot.ordinal)
        _ <- tracker.pruneFinalizedBelow(finalizedOrdinal)
        metricsState <- tracker.getState
        _ <- updateStateChannelRetryParametersMetrics(metricsState)
      } yield ()

    def softConfirm(globalSnapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit] =
      for {
        identifier <- identifierStorage.get
        observedHashes <- getConfirmedHashes(identifier, globalSnapshot)
        _ <-
          if (observedHashes.nonEmpty) {
            tracker.softObserve(observedHashes) >>
              logger.debug(s"[Queue] Soft-observed ${observedHashes.size} binaries at best-tip ord=${globalSnapshot.ordinal.show}")
          } else Applicative[F].unit
      } yield ()

    def clearPending: F[Unit] =
      logger.info("[Queue] Clearing all pending binaries") >> tracker.clear

    def processQueue(globalSnapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit] = {
      val lastGlobalSnapshotSigners = globalSnapshot.signed.proofs.map(_.id.toPeerId).some
      tracker.getState.flatMap { state =>
        if (state.retryMode) {
          processRetryMode(state.cap.value.toInt, lastGlobalSnapshotSigners)
        } else {
          processNormalMode(lastGlobalSnapshotSigners)
        }
      }
    }

    def processQueueWithoutSnapshot: F[Unit] =
      tracker.getState.flatMap { state =>
        if (!state.retryMode) {
          processNormalMode(none)
        } else {
          logger.info("[Queue] Retry mode active but no global snapshot available") >>
            Applicative[F].unit
        }
      }

    private def processNormalMode(signers: Option[NonEmptySet[PeerId]]): F[Unit] =
      tracker.getPendingToRetry(10).flatMap { pending =>
        val unsent = pending.filter(_.sendsSoFar.value === 0L)
        if (unsent.nonEmpty) {
          logger.info(s"[Queue] Processing ${unsent.size} unsent binaries") >>
            unsent.traverse_(p => sendBinaryInBackground(p, signers))
        } else {
          Applicative[F].unit
        }
      }

    private def processRetryMode(cap: Int, signers: Option[NonEmptySet[PeerId]]): F[Unit] = {
      // CONTIGUOUS-PREFIX retry (2026-06-11, run bpc2yyegf — the data-channel "continuity hole").
      //
      // The previous "1 oldest + (cap-1) newest" mix was designed for the pre-sharding direct-admission
      // world, where gl0's `onlyPossibleReferences` could place any binary that chained off SOMETHING it
      // had. Under execution sharding, gl0 consumes binaries through `chainLinkOrder`ed checkpoint windows
      // anchored at the shard's per-MG tip — only a CONTIGUOUS parent→child prefix starting at the tip's
      // child is consumable. The mix therefore manufactured a permanent send-gap: with confirmation
      // latency (committee-gate → checkpoint → quorum → gl0-adopt, ~1-2 min) structurally exceeding the
      // ~9s production cadence, RetryMode never exits, cap collapses, and each tick transmitted queue
      // positions #1, #47, #48 of the oldest-48 window — positions #2..#46 were NEVER SENT to any gl0.
      // The shard producer then sat at produce-skip no-chain-link forever while ~190 binaries piled up
      // (and the two endpoints it DID receive were unusable without the middle).
      //
      // The oldest PENDING binary is by construction the child of gl0's last-confirmed tip, so the
      // consumable set is exactly the oldest-first contiguous prefix — send that, sized to outrun
      // production during one confirmation round trip. Run bfcnpd5vc measured the consequence of a
      // small floor: checkpoint windows are capped at exactly this batch size (the producer can only
      // window what has been SENT contiguously), so floor 16 → 16-binary windows at ~1 adoption/min =
      // +9/min catch-up — the genesis backlog took 10+ min to drain and allow-spend windows expired
      // mid-lag (NoActiveAllowSpend). Floor 64 had the same shape one octave up (run 10 / design §5.8):
      // ANY per-tick cap becomes the window-size ceiling, so the sender — not the protocol — ends up
      // governing how much chain one fold can advance.
      //
      // UN-CAPPED BUT CADENCE-GATED (run 12 post-mortem, 2026-06-12). The first un-cap shipped the FULL
      // pending prefix EVERY 5s tick; with confirmation lagging, that re-POSTed the same ~100 binaries
      // ~20×/min/mg — an intake storm on gl0 (committee-gate churn + orphan-buffer thrash) that starved
      // the very checkpoint pipeline whose confirmations would have drained the queue (circular). The
      // window-size goal only needs each binary DELIVERED ONCE (gl0 buffers + dedupes by hash); re-sends
      // exist solely for loss recovery. So: NEVER-SENT binaries ship immediately on every tick
      // (first-delivery latency preserved, no size cap), and the full contiguous prefix re-ships only
      // every RESEND_EVERY_TICKS-th tick (~30s — matched to the checkpoint confirmation RTT).
      val inflightCeiling = 1024
      val ResendEveryTicks = 6L
      retryTickCounter.getAndUpdate(_ + 1L).flatMap { tick =>
        val fullResendDue = tick % ResendEveryTicks === 0L
        tracker.getPendingToRetry(inflightCeiling).flatMap { allPending =>
          val sortedAsc = allPending.sortBy(_.currencySnapshotOrdinal.value.value)
          val toRetry = if (fullResendDue) sortedAsc else sortedAsc.filter(_.sendsSoFar.value === 0L)
          if (toRetry.nonEmpty) {
            logger.info(
              s"[RetryMode] Processing ${toRetry.size} binaries (cap=$cap, " +
                s"mix=${if (fullResendDue) "contiguous-prefix-full-resend" else "unsent-only"}, " +
                s"totalPending=${allPending.size})"
            ) >>
              toRetry.traverse_(p => sendBinaryInBackground(p, signers))
          } else {
            Applicative[F].unit
          }
        }
      }
    }

    private def sendBinaryInBackground(
      pending: PendingBinary,
      signers: Option[NonEmptySet[PeerId]] = none
    ): F[Unit] =
      S.supervise(
        poster
          .post(pending.binary, signers)
          .flatMap(peerId => logger.info(s"[Queue] Sent ${pending.binary.hash} via $peerId"))
          .handleErrorWith(err => logger.warn(s"[Queue] Failed to send ${pending.binary.hash}: ${err.getMessage}"))
      ).void

    private def getConfirmedHashes(
      identifier: Address,
      globalSnapshot: Hashed[GlobalIncrementalSnapshot]
    ): F[Set[io.constellationnetwork.security.hash.Hash]] = {
      val binaries = globalSnapshot.stateChannelSnapshots.get(identifier).toList.flatMap(_.toList)
      binaries.traverse(_.toHashed).map(_.map(_.hash)).map(_.toSet)
    }
  }
}
