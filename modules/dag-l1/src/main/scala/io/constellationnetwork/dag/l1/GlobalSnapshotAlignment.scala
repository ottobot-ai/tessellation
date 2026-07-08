package io.constellationnetwork.dag.l1

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import scala.concurrent.duration.DurationInt

import io.constellationnetwork.dag.l1.domain.snapshot.programs.SnapshotProcessor.SnapshotProcessingResult
import io.constellationnetwork.dag.l1.modules._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.modules.SharedStorages
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo, StateProof}
import io.constellationnetwork.security._

import fs2.Stream
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class GlobalSnapshotAlignment[F[
  _
]: Async: Parallel: HasherSelector: SecurityProvider: JsonSerializer, P <: StateProof, S <: Snapshot, SI <: SnapshotInfo[
  P
], R <: CliMethod](
  services: Services[F, P, S, SI, R],
  programs: Programs[F, P, S, SI],
  storages: Storages[F, P, S, SI],
  sharedStorages: SharedStorages[F]
)(implicit withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit) {

  private val maxEpochProgressesBehind = 5L
  private implicit val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLoggerFromName[F](this.getClass.getName)

  private def withRetry[A](
    operation: F[A],
    operationName: String,
    maxRetries: Int = 3
  ): F[A] = {
    import retry._

    retryingOnSomeErrors(
      policy = RetryPolicies.limitRetries[F](maxRetries),
      isWorthRetrying = (_: Throwable) => true.pure[F],
      onError = (err: Throwable, details: RetryDetails) => logger.warn(err)(s"$operationName failed on attempt ${details.retriesSoFar + 1}")
    )(operation).handleErrorWith { e =>
      logger.error(e)(s"$operationName failed after $maxRetries retries") >>
        Async[F].raiseError(e)
    }
  }

  def performCheckAlignment(): F[Unit] = {
    def checkSynchronization(
      lastGlobalSnapshotFromStorage: Hashed[GlobalIncrementalSnapshot],
      lastGlobalSnapshotFromNetwork: Hashed[GlobalIncrementalSnapshot]
    ) =
      if (
        lastGlobalSnapshotFromStorage.epochProgress.value.value + maxEpochProgressesBehind < lastGlobalSnapshotFromNetwork.epochProgress.value.value
      ) {
        val message = "Detected synchronization issue: TooFarEpochProgress. Forcing re-download"
        logger.info(message) >>
          storages.globalL0Alignment.updateShouldRedownload(
            value = true,
            reasons = List(message)
          )
      } else {
        ().pure
      }

    for {
      _ <- logger.info("Checking global snapshot alignment")
      maybeLastSnapshotOnStorage <- sharedStorages.lastGlobalSnapshot.get
      lastCombinedGlobalSnapshotFromNetwork <- services.globalL0.pullLatestSnapshot
      _ <- maybeLastSnapshotOnStorage match {
        case Some(lastSnapshotOnStorage) =>
          val (lastGlobalSnapshotFromNetwork, _) = lastCombinedGlobalSnapshotFromNetwork
          checkSynchronization(lastSnapshotOnStorage, lastGlobalSnapshotFromNetwork)
        case None =>
          val message = "Last snapshot not found on storage, forcing re-download!"
          logger.info(message) >>
            storages.globalL0Alignment.updateShouldRedownload(
              value = true,
              reasons = List(message)
            )
      }
    } yield ()
  }

  def performL0PeerDiscovery(): F[Unit] =
    storages.lastSnapshot.get.flatMap {
      case None =>
        storages.l0Cluster.getRandomPeer.flatMap(p => programs.l0PeerDiscovery.discoverFrom(p))
      case Some(latestSnapshot) =>
        programs.l0PeerDiscovery.discover(latestSnapshot.signed.proofs.map(_.id).map(PeerId._Id.reverseGet))
    }

  def performGlobalSnapshotProcessingUntilCaughtUp()(
    implicit stateProofSelector: GlobalStateProofSelector
  ): F[Unit] = {
    def loop(isFirstCall: Boolean): F[Unit] =
      performGlobalSnapshotProcessing().flatMap {
        case Left(_) if isFirstCall                => loop(isFirstCall = false)
        case Left(_)                               => ().pure
        case Right(snapshots) if snapshots.isEmpty => ().pure
        case Right(_)                              => loop(isFirstCall = false)
      }
    loop(isFirstCall = true)
  }

  def performGlobalSnapshotProcessing()(
    implicit stateProofSelector: StateProofSelector
  ): F[Either[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo), List[Hashed[GlobalIncrementalSnapshot]]]] = {
    def logSnapshots(
      snapshots: Either[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo), List[Hashed[GlobalIncrementalSnapshot]]]
    ): F[Unit] = {
      def log(snapshot: Hashed[GlobalIncrementalSnapshot]) =
        logger.info(s"Pulled following global snapshot: ${SnapshotReference.fromHashedSnapshot(snapshot).show}")

      snapshots match {
        case Left((snapshot, _)) => log(snapshot)
        case Right(snapshots)    => snapshots.traverse(log).void
      }
    }

    def processSnapshots(
      snapshots: Either[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo), List[Hashed[GlobalIncrementalSnapshot]]]
    ): F[List[SnapshotProcessingResult]] =
      snapshots match {
        case Left((snapshot, state)) =>
          withRetry(
            operation = HasherSelector[F].withCurrent { implicit hasher =>
              programs.snapshotProcessor.process((snapshot, state).asLeft[Hashed[GlobalIncrementalSnapshot]]).map(List(_))
            },
            operationName = s"Process single snapshot ${SnapshotReference.fromHashedSnapshot(snapshot).show}"
          )
        case Right(snapshots) =>
          withRetry(
            operation = performSnapshotsBatchProcessing(snapshots),
            operationName = s"Process ${snapshots.size} snapshots batch"
          )
      }

    def logResults(results: List[SnapshotProcessingResult]): F[Unit] =
      results.traverse(result => logger.info(s"Snapshot processing result: ${result.show}")).void.handleErrorWith { e =>
        logger.warn(e)("Failed to log snapshot processing results")
      }

    // Finality-gated pull (#122). Followers consume only depth-k-finalized gl0 snapshots;
    // by construction, gl0 reorgs can only happen within the depth-k window, so the
    // follower never observes a snapshot that later gets reorg'd away. The destructive
    // RedownloadNeeded path (replaceByRefs) and the StateProofMismatch recovery path
    // become unreachable on the happy path — they remain as defense-in-depth.
    //
    // Three cases:
    //   - No stored last snapshot: must bootstrap via the legacy pullGlobalSnapshots Left
    //     path (which fetches the canonical latest with majority verification).
    //   - finalizedOrdinal not yet known (None) or <= lastOrdinal: idle until finality
    //     advances. At node startup, gl0 hasn't built up depth-k confirmations yet, so
    //     the follower must wait — this is the "bootstrap finality" case.
    //   - finalizedOrdinal > lastOrdinal: pull (lastOrdinal, finalizedOrdinal].
    def pullFinalityGated: F[Either[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo), List[Hashed[GlobalIncrementalSnapshot]]]] =
      (sharedStorages.lastGlobalSnapshot.getOrdinal, services.globalL0.pullLatestFinalizedOrdinal).flatMapN {
        case (None, _) =>
          // Bootstrap: no stored snapshot yet. Defer to the legacy path which fetches
          // canonical-latest with majority verification, then takes the Left branch in
          // processSnapshots to perform a download. Finality gating kicks in on the next
          // tick once lastGlobalSnapshot is populated.
          logger.info("pullFinalityGated: no stored last snapshot, falling back to bootstrap pull") >>
            services.globalL0.pullGlobalSnapshots
        case (Some(lastOrd), None) =>
          logger
            .info(s"pullFinalityGated: waiting on gl0 finality (last=${lastOrd.show}, finalized=unknown); idling")
            .as(List.empty[Hashed[GlobalIncrementalSnapshot]].asRight)
        case (Some(lastOrd), Some(finalizedOrd)) if finalizedOrd <= lastOrd =>
          logger
            .debug(s"pullFinalityGated: caught up to finality (last=${lastOrd.show}, finalized=${finalizedOrd.show})")
            .as(List.empty[Hashed[GlobalIncrementalSnapshot]].asRight)
        case (Some(lastOrd), Some(finalizedOrd)) =>
          logger.info(s"pullFinalityGated: pulling (last=${lastOrd.show}, finalized=${finalizedOrd.show}]") >>
            services.globalL0.pullFinalizedGlobalSnapshots(lastOrd, finalizedOrd).map(_.asRight)
      }

    for {
      snapshots <- withRetry(
        operation = pullFinalityGated,
        operationName = "Pull finality-gated global snapshots"
      )
      _ <- logSnapshots(snapshots)
      results <- processSnapshots(snapshots)
      _ <- logResults(results)
    } yield snapshots
  }

  // Mirrors ml0's StateChannel orphan recovery (currency-l0/StateChannel.scala): when
  // createContext raises StateProofMismatch the local lastState diverged from the producer's
  // claim, indicating dl1 stored a snapshot from a gl0 producer whose head was on a transient
  // fork that the gl0 layer later reorg'd away from. Without explicit recovery the batch loop
  // skips this snapshot but the local stores still hold the orphan, so every subsequent batch
  // hits parent-hash mismatch and stalls. Fix: fetch canonical latest from majority, realign
  // MPT, atomically swap lastGlobalSnapshot + lastNGlobalSnapshot via setForRecovery, and set
  // shouldRedownload so the next aligned snapshot rebuilds dl1-local state via the
  // RedownloadNeeded path. Recovery best-effort — if pullLatestSnapshot fails we fall through
  // to the legacy log+shouldRedownload behavior and retry next 10s tick.
  private def recoverFromOrphan(
    failedSnapshot: Hashed[GlobalIncrementalSnapshot],
    cause: Throwable
  )(
    implicit stateProofSelector: StateProofSelector
  ): F[Unit] = {
    val ref = SnapshotReference.fromHashedSnapshot(failedSnapshot).show
    val recovery = HasherSelector[F].withCurrent { implicit hasher =>
      for {
        // 3c-A: pull gl0's SIGNED MPT byte map (+ snapshot + GSI) and store the bytes VERBATIM via `loadBytes` (no
        // `syncFromGlobalSnapshotInfo` re-encode → no `recomputed ≠ signed` drift). The GSI rides along ONLY for
        // `setForRecovery`; it is never re-encoded into the MPT.
        canonical <- services.globalL0.pullLatestMptEntries
        (canonicalSnapshot, canonicalState, canonicalEntries) = canonical
        _ <- logger.info(
          s"DL1 #117 DIAG recoverFromOrphan failed=$ref pulled canonical ord=${canonicalSnapshot.ordinal.show} " +
            s"hash=${canonicalSnapshot.hash.show.take(12)} balances.size=${canonicalState.balances.size} " +
            s"scHashes.size=${canonicalState.lastStateChannelSnapshotHashes.size}"
        )
        // The bytes are OPTIONAL: gl0's byte route 404s at a sparse combined-checkpoint ordinal, in which case
        // `pullLatestMptEntries` already degraded to legacy `pullLatestSnapshot` and returns `None`. On `Some` load the SIGNED
        // bytes VERBATIM. The post-load root recompute BELOW is DIAG-only — preserved unchanged either way.
        //
        // FINDING-S01 (`None` fallback): the GSI has NO field for the MPT-native ConsumedAllowSpends (33) / Slashings (34)
        // partitions, and this recovery previously adopted the bare re-encode UNVERIFIED (the recompute below is DIAG-only) —
        // silently committing a mirror whose root diverges from the snapshot's SIGNED `stateProof.mptRoot` and then swapping
        // the follow storages onto it. `syncFromGlobalSnapshotInfoVerified` is CHECK-then-write; on `false` NOTHING was
        // written and we RAISE so the enclosing `recovery.handleErrorWith` falls back to the legacy log+shouldRedownload
        // behavior (no unverified adopt, no clobbered mirror). A legacy `mptRoot = None` snapshot keeps the plain rebuild.
        _ <- canonicalEntries match {
          case Some(bytes) => sharedStorages.mptStore.loadBytes(bytes, canonicalSnapshot.ordinal)
          case None =>
            canonicalSnapshot.signed.value.stateProof.mptRoot match {
              case None => sharedStorages.mptStore.syncFromGlobalSnapshotInfo(canonicalState, canonicalSnapshot.ordinal)
              case signedRoot @ Some(_) =>
                sharedStorages.mptStore
                  .syncFromGlobalSnapshotInfoVerified(canonicalState, canonicalSnapshot.ordinal, signedRoot)
                  .flatMap {
                    case true => Async[F].unit
                    case false =>
                      new RuntimeException(
                        s"DL1 recoverFromOrphan: GSI fallback at ord=${canonicalSnapshot.ordinal.show} cannot reproduce the " +
                          s"snapshot's SIGNED stateProof.mptRoot (MPT-native ConsumedAllowSpends/Slashings not carried by the " +
                          s"GSI). NOT adopting (store untouched); falling back to shouldRedownload."
                      ).raiseError[F, Unit]
                  }
            }
        }
        // DIAG: recompute sidecar-free (apples-to-apples with the signed `stateProof.mptRoot`), NOT `getRootHashForOrdinal`
        // (which includes the path-dependent SystemNamespace sidecars). With the verbatim `loadBytes` this equals the
        // signed root by construction on honest input.
        afterBytes <- sharedStorages.mptStore.underlying.entries
        postSyncRoot <- GlobalSnapshotInfo.sidecarFreeMptRoot[F](afterBytes)
        _ <- logger.info(
          s"DL1 #117 DIAG post-load sidecar-free mptRoot=${postSyncRoot.show.take(12)} at ord=${canonicalSnapshot.ordinal.show} " +
            s"(signed=${canonicalSnapshot.signed.value.stateProof.mptRoot.map(_.show.take(12)).getOrElse("none")})"
        )
        _ <- sharedStorages.lastGlobalSnapshot.setForRecovery(canonicalSnapshot, canonicalState)
        _ <- sharedStorages.lastNGlobalSnapshot.setForRecovery(canonicalSnapshot, canonicalState)
        _ <- storages.globalL0Alignment.updateShouldRedownload(
          value = true,
          reasons = List(s"DL1 setForRecovery to canonical ord=${canonicalSnapshot.ordinal.show} after StateProofMismatch at $ref")
        )
        _ <- logger.info(s"DL1 recovered to canonical ord=${canonicalSnapshot.ordinal.show}; will resume on next tick")
      } yield ()
    }
    logger.warn(s"DL1 StateProofMismatch at $ref (${cause.getMessage}); fetching canonical latest from majority for setForRecovery") >>
      recovery.handleErrorWith { recoveryErr =>
        logger.error(recoveryErr)(s"DL1 setForRecovery recovery failed for $ref, falling back to log+shouldRedownload") >>
          storages.globalL0Alignment.updateShouldRedownload(
            value = true,
            reasons = List(s"DL1 StateProofMismatch at $ref (recovery failed: ${recoveryErr.getMessage})")
          )
      }
  }

  // gl1 inclusion-proof follow DEADLOCK FIX (#287-adjacent): register the pulled FINALIZED batch into the by-ordinal lastN
  // index BEFORE the per-ordinal walk. gl1's GSI comes from gl0's LATEST-finalized slice (ordinal M = the batch tip), but
  // `DAGSnapshotProcessor.applyGlobalSnapshotFn` resolves M's signed `stateProof` from the LOCAL lastN window via
  // `getByOrdinal(M)`, and the per-ordinal `setLastNSnapshots` only advances that window AFTER a successful apply. So while
  // processing the FIRST ordinal of the batch, `getByOrdinal(M)` was `None` → the slice verify deferred forever → the batch
  // halted → the window never advanced → permanent deadlock (the 102-defer / 0-Verified gl1-formation symptom). Populating the
  // index up-front from the already-pulled, chain-link-verified, finality-gated batch lets `getByOrdinal(M)` resolve so the
  // verify succeeds and the walk advances. This touches ONLY the by-ordinal index that `getByOrdinal` (the slice verify) reads —
  // it does NOT advance the chain pointer (`lastGlobalSnapshot`) nor the `(snapshot, state)` combined window (`getLastN`,
  // currency-only); those still advance per-ordinal post-apply via `setLastNSnapshots`. `set` re-registering the same ordinals
  // post-apply is idempotent-by-ordinal (a plain map upsert). Finality-gating (#122) is preserved: the batch is finalized.
  private def performSnapshotsBatchProcessing(
    snapshots: List[Hashed[GlobalIncrementalSnapshot]]
  )(
    implicit stateProofSelector: StateProofSelector
  ): F[List[SnapshotProcessingResult]] =
    sharedStorages.lastNGlobalSnapshot.registerFinalized(snapshots) >>
      (snapshots, List.empty[SnapshotProcessingResult]).tailRecM {
        case (snapshot :: nextSnapshots, aggResults) =>
          HasherSelector[F].withCurrent { implicit hasher =>
            programs.snapshotProcessor
              .process(snapshot.asRight[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)])
          }
            .map(result => (nextSnapshots, aggResults :+ result).asLeft[List[SnapshotProcessingResult]])
            .handleErrorWith {
              case e: io.constellationnetwork.node.shared.infrastructure.snapshot.GlobalSnapshotContextFunctions.StateProofMismatch =>
                recoverFromOrphan(snapshot, e)
                  .as((nextSnapshots, aggResults).asLeft[List[SnapshotProcessingResult]])
              case e: io.constellationnetwork.dag.l1.domain.snapshot.programs.DAGSnapshotProcessor.FollowSliceVerificationError =>
                // gl1 own-slice follow CUTOVER (S3b′): the follow slice is unavailable or failed to verify against the
                // trusted signed roots. Do NOT advance, do NOT set shouldRedownload (no recovery storm — distinct from the
                // StateProofMismatch path above). Just log + stop this batch; gl1 retries on the next 10s tick. The chain
                // pointer (`lastGlobalSnapshot`) is untouched because `lastSnapshotStorage.set` only runs after a successful
                // `applySnapshotFn` in `processAlignment`.
                logger
                  .info(
                    s"Follow-slice not ready / verify deferred at ${SnapshotReference.fromHashedSnapshot(snapshot).show}: " +
                      s"${e.getMessage} — not advancing, will retry next tick"
                  )
                  .as(aggResults.asRight[(List[Hashed[GlobalIncrementalSnapshot]], List[SnapshotProcessingResult])])
              case e =>
                val message = s"Failed to process snapshot ${SnapshotReference.fromHashedSnapshot(snapshot).show}, skipping"
                for {
                  _ <- storages.globalL0Alignment.updateShouldRedownload(
                    value = true,
                    reasons = List(message)
                  )
                  _ <- logger.error(e)(message)
                } yield (nextSnapshots, aggResults).asLeft[List[SnapshotProcessingResult]]
            }

        case (Nil, aggResults) =>
          aggResults.asRight[(List[Hashed[GlobalIncrementalSnapshot]], List[SnapshotProcessingResult])].pure[F]
      }

  private def checkAlignment: Stream[F, Unit] = Stream
    .awakeEvery(1.minute)
    .evalMap { _ =>
      withRetry(
        operation = performCheckAlignment(),
        operationName = "Check alignment"
      )
    }
    .handleErrorWith {
      case e =>
        Stream.eval(logger.error(e)("Check alignment stream failed, restarting")) ++ checkAlignment
    }

  private def l0PeerDiscovery: Stream[F, Unit] = Stream
    .awakeEvery(10.seconds)
    .evalMap { _ =>
      withRetry(
        operation = performL0PeerDiscovery(),
        operationName = "L0 peer discovery"
      )
    }
    .handleErrorWith { e =>
      Stream.eval(logger.error(e)("L0 peer discovery stream failed, restarting")) ++ l0PeerDiscovery
    }

  private def globalSnapshotProcessing(
    implicit stateProofSelector: GlobalStateProofSelector
  ): Stream[F, Unit] = Stream
    .awakeEvery(10.seconds)
    .evalMap { _ =>
      performGlobalSnapshotProcessing().void
    }
    .handleErrorWith { e =>
      Stream.eval(logger.error(e)("Global snapshot processing stream failed, restarting")) ++ globalSnapshotProcessing
    }

  def runtime()(
    implicit stateProofSelector: GlobalStateProofSelector
  ): Stream[F, Unit] =
    Stream(l0PeerDiscovery, globalSnapshotProcessing, checkAlignment)
      .covary[F]
      .parJoinUnbounded
}

object GlobalSnapshotAlignment {
  def make[F[
    _
  ]: Async: Parallel: HasherSelector: SecurityProvider: JsonSerializer, P <: StateProof, S <: Snapshot, SI <: SnapshotInfo[
    P
  ], R <: CliMethod](
    services: Services[F, P, S, SI, R],
    programs: Programs[F, P, S, SI],
    storages: Storages[F, P, S, SI],
    sharedStorages: SharedStorages[F]
  )(implicit withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit): GlobalSnapshotAlignment[F, P, S, SI, R] =
    new GlobalSnapshotAlignment[F, P, S, SI, R](services, programs, storages, sharedStorages)
}
