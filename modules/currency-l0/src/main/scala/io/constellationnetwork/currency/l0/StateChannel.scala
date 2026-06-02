package io.constellationnetwork.currency.l0

import java.security.KeyPair

import cats.data.OptionT
import cats.effect.Async
import cats.effect.std.Supervisor
import cats.syntax.all._
import cats.{Applicative, Parallel}

import scala.concurrent.duration._

import io.constellationnetwork.currency.dataApplication.BaseDataApplicationL0Service
import io.constellationnetwork.currency.l0.cli.method.Run
import io.constellationnetwork.currency.l0.metrics.updateFailedConfirmingStateChannelBinaryMetrics
import io.constellationnetwork.currency.l0.modules.{Programs, Services, Storages}
import io.constellationnetwork.currency.schema.globalSnapshotSync.{GlobalSnapshotSync, GlobalSnapshotSyncReference}
import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kernel.{:: => _, _}
import io.constellationnetwork.node.shared.domain.snapshot.Validator
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.modules.SharedStorages
import io.constellationnetwork.node.shared.snapshot.currency.{CurrencySnapshotEvent, GlobalSnapshotSyncEvent}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.CompatCodecs._

import fs2.Stream
import io.circe.Json
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object StateChannel {

  private val awakePeriod = 10.seconds

  def run[F[_]: Async: HasherSelector: SecurityProvider: Metrics: Parallel: JsonSerializer: Hasher](
    services: Services[F, Run],
    storages: Storages[F],
    sharedStorages: SharedStorages[F],
    programs: Programs[F],
    dataApplicationService: Option[BaseDataApplicationL0Service[F]],
    selfKeyPair: KeyPair,
    enqueueConsensusEventFn: CurrencySnapshotEvent => Cell[F, StackF, _, Either[CellError, Ω], _]
  )(
    implicit S: Supervisor[F],
    stateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F](this.getClass.getName)

    val globalL0SnapshotProcessing: Stream[F, Unit] =
      Stream
        .awakeEvery[F](awakePeriod)
        .evalMap(_ =>
          performGlobalL0SnapshotProcess(
            storages,
            sharedStorages,
            services,
            dataApplicationService,
            selfKeyPair,
            enqueueConsensusEventFn
          ).handleErrorWith { error =>
            logger.error(error)("Error during global L0 snapshot processing")
          }
        )

    val globalL0PeerDiscovery: Stream[F, Unit] =
      Stream
        .awakeEvery[F](awakePeriod)
        .evalMap(_ =>
          performGlobalL0PeerDiscovery(storages, programs).handleErrorWith { error =>
            logger.error(error)("Error during global L0 peer discovery")
          }
        )

    Stream(globalL0SnapshotProcessing, globalL0PeerDiscovery).parJoin(2)
  }

  def performGlobalL0SnapshotProcess[F[_]: Async: HasherSelector: Metrics: SecurityProvider: Parallel: JsonSerializer: Hasher](
    storages: Storages[F],
    sharedStorages: SharedStorages[F],
    services: Services[F, Run],
    dataApplicationService: Option[BaseDataApplicationL0Service[F]],
    selfKeyPair: KeyPair,
    enqueueConsensusEventFn: CurrencySnapshotEvent => Cell[F, StackF, _, Either[CellError, Ω], _]
  )(
    implicit S: Supervisor[F],
    stateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): F[Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F](this.getClass.getName)

    def triggerOnGlobalSnapshotPullHook(snapshot: Hashed[GlobalIncrementalSnapshot], context: GlobalSnapshotInfo): F[Unit] =
      dataApplicationService.traverse_ { service =>
        service
          .onGlobalSnapshotPull(snapshot, context)
          .handleErrorWith(error => logger.error(error)("An unexpected error occurred in onGlobalSnapshotPull"))
      }

    def sendGlobalSnapshotSyncConsensusEvent(snapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit] = {
      val selfPeerId = selfKeyPair.getPublic.toId.toPeerId

      val lastSentGlobalSnapshotSync = OptionT(storages.lastGlobalSnapshotSync.get).orElse {
        OptionT(storages.snapshot.head).flatMapF {
          case (_, info) =>
            info.globalSnapshotSyncView
              .flatMap(_.get(selfPeerId))
              .traverse(GlobalSnapshotSyncReference.of[F])
              .map(_.orElse(GlobalSnapshotSyncReference.empty.some))
        }
      }.value

      (lastSentGlobalSnapshotSync, storages.session.getToken).flatMapN {
        case (Some(lastGlobalSnapshotSyncRef), Some(session)) =>
          val sync = GlobalSnapshotSync(lastGlobalSnapshotSyncRef.ordinal, snapshot.ordinal, snapshot.hash, session)
          for {
            signedGlobalSnapshotSync <- Signed.forAsyncHasher(sync, selfKeyPair)
            globalSyncEvent = GlobalSnapshotSyncEvent(signedGlobalSnapshotSync)
            _ <- enqueueConsensusEventFn(globalSyncEvent).run()
            _ <- storages.lastGlobalSnapshotSync.set(globalSyncEvent.value)
          } yield ()
        case (Some(_), None) =>
          logger.warn("Couldn't send GlobalSnapshotSyncEvent. Session is missing.")
        case (None, Some(_)) =>
          logger.warn("Couldn't send GlobalSnapshotSyncEvent. Last sent reference is missing")
        case _ =>
          logger.error("Couldn't construct GlobalSnapshotSyncEvent. Last sent reference and session are missing")
      }
    }

    // Typed-scodec initialization — writes per-field `ImmutableCodec[V]` bytes consistent
    // with `mptStateProof` and typed reads. No JSON blob intermediate.
    def ensureMptInitialized(ordinal: SnapshotOrdinal, state: GlobalSnapshotInfo): F[Unit] =
      sharedStorages.mptStore.syncFromGlobalSnapshotInfo(state, ordinal)

    def persistGlobalSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
      for {
        _ <- storages.globalSnapshotsWithStateFileStorage
          .write(snapshot.ordinal, GlobalSnapshotWithState(snapshot.signed, state))
        _ <- storages.globalSnapshotsWithStateDeltasFileStorage
          .write(snapshot.ordinal, GlobalSnapshotWithStateDeltas(snapshot.signed, state.activeAllowSpends, state.activeTokenLocks))
      } yield ()

    def handleInitialSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
      for {
        _ <- logger.info(s"Initializing global snapshot storages with ordinal=${snapshot.ordinal}")
        _ <- storages.lastSyncGlobalSnapshot.setInitial(snapshot, state)
        _ <- sharedStorages.lastNGlobalSnapshot.setInitialFetchingGL0(snapshot, state, services.globalL0.asLeft.some, none)
        _ <- sharedStorages.lastGlobalSnapshot.setInitial(snapshot, state)
        _ <- persistGlobalSnapshot(snapshot, state)
        _ <- ensureMptInitialized(snapshot.ordinal, state)
        _ <- triggerOnGlobalSnapshotPullHook(snapshot, state)
        _ <- logger.info(s"Successfully initialized global snapshot storages with ordinal=${snapshot.ordinal}")
      } yield ()

    // Catch StateProofMismatch raised by createContext when ml0's stored lastState diverges
    // from the producer's claimed mptRoot — happens when ml0 saved a snapshot from a gl0
    // producer whose local-head was on a transient fork that the gl0 layer later reorg'd
    // away from (maxvalid-tk). Without recovery the loop retries the same orphan forever
    // (observed: 554 retries on ord 322 stalled spend tests at the bigset run).
    //
    // Recovery: fetch the canonical latest from majority and atomically swap all three
    // storage refs via `setForRecovery`. Unlike `clear`, setForRecovery keeps stores
    // populated, so downstream consumers (StateChannelBinarySender,
    // CurrencySnapshotConsensusStateCreator, CurrencyMessagesService,
    // StateChannelSnapshotService) continue to see a valid lastGlobalSnapshot and consensus
    // stays Ready. MPT is realigned to the canonical state via syncFromGlobalSnapshotInfo.
    def recoverFromOrphan(failedOrdinal: SnapshotOrdinal, cause: Throwable): F[Unit] =
      for {
        _ <- logger.warn(
          s"ml0 StateProofMismatch at ord=${failedOrdinal.show} (${cause.getMessage}); fetching canonical latest from majority for setForRecovery"
        )
        canonical <- services.globalL0.pullLatestSnapshot
        (canonicalSnapshot, canonicalState) = canonical
        _ <- ensureMptInitialized(canonicalSnapshot.ordinal, canonicalState)
        _ <- storages.lastSyncGlobalSnapshot.setForRecovery(canonicalSnapshot, canonicalState)
        _ <- sharedStorages.lastNGlobalSnapshot.setForRecovery(canonicalSnapshot, canonicalState)
        _ <- sharedStorages.lastGlobalSnapshot.setForRecovery(canonicalSnapshot, canonicalState)
        _ <- persistGlobalSnapshot(canonicalSnapshot, canonicalState)
        _ <- triggerOnGlobalSnapshotPullHook(canonicalSnapshot, canonicalState)
        // Drop pending state-channel binaries built against the orphan gl0 chain. Their
        // `globalSyncView` references gl0 ordinals whose hashes no longer match canonical;
        // gl0 will reject them with `Forced globalSyncView hash mismatch`, leaving the queue
        // poisoned (totalPending grows monotonically as ml0 produces new binaries). Clearing
        // here lets the next currency-consensus round build binaries against the new canonical
        // gl0 view, restoring the metagraph propagation chain on gl0. Binaries already
        // confirmed by gl0 (state-channel-snapshot included in a finalized global snapshot)
        // are unaffected — `markAsConfirmed` already removed them. (#113)
        _ <- services.stateChannelBinarySender.clearPending
        _ <- logger.info(
          s"ml0 recovered to canonical ord=${canonicalSnapshot.ordinal.show}; will resume pulling forward on next tick"
        )
      } yield ()

    // ADOPT-AND-VERIFY follow state-advance (task #12). On the global-FOLLOW path ml0 no longer RE-EXECUTES global
    // consensus (`createContext`) to derive the next GlobalSnapshotInfo for ordinal N. Instead it fetches gl0's typed
    // per-ordinal `StateChangesAccumulator` change-set, applies the delta for N on top of `lastState` to get a candidate
    // GSI, recomputes the MPT root incrementally, and adopts the candidate ONLY IF that root EQUALS the signed snapshot's
    // `stateProof.mptRoot` (the `withTransaction` Commit/Rollback gate inside `adoptAndVerifyChangeSetDelta`). The signed
    // mptRoot is the ONLY trust anchor: ANY miss (no client, `baseOrdinal=None`, missing delta for N, incomplete
    // preSyncBytes, or a tampered delta → recomputed root ≠ signed) rolls back the MPT tx and FALLS BACK to the full
    // `createContext` path for that ordinal — ml0 NEVER advances its global state on a mismatch.
    //
    // `createContext` is RETAINED, unchanged, for (a) this fallback and (b) ml0 producing its OWN currency snapshots — only
    // the global-follow state-advance is replaced.
    def deriveFollowContext(
      snapshot: Hashed[GlobalIncrementalSnapshot],
      lastSnapshot: Hashed[GlobalIncrementalSnapshot],
      lastState: GlobalSnapshotInfo
    ): F[GlobalSnapshotInfo] = {
      val fallbackCreateContext: F[GlobalSnapshotInfo] =
        services.globalSnapshotContextFunctions.createContext(
          lastState,
          lastSnapshot.signed,
          snapshot.signed,
          services.globalL0.pullGlobalSnapshot
        )

      def adopt(deltaN: io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator): F[GlobalSnapshotInfo] =
        io.constellationnetwork.schema.mpt.GlobalStateConverter
          .adoptAndVerifyChangeSetDelta[F](
            sharedStorages.mptStore,
            lastState,
            deltaN,
            snapshot.signed.value.stateProof.mptRoot,
            snapshot.ordinal
          )
          .flatMap {
            case Some(adoptedGsi) =>
              logger
                .debug(
                  s"ml0 adopt-and-verify: adopted ordinal=${snapshot.ordinal.show} (recomputed mptRoot matched signed stateProof.mptRoot)"
                )
                .as(adoptedGsi)
            case None =>
              logger.info(
                s"ml0 adopt-and-verify: verify FAILED for ordinal=${snapshot.ordinal.show} (recomputed mptRoot ≠ signed " +
                  s"or legacy-format); MPT rolled back, falling back to createContext"
              ) >> fallbackCreateContext
          }

      // `GlobalSnapshotInfo` carries no ordinal, so the state ml0 currently holds is identified by `lastSnapshot.ordinal`
      // (the parent of `snapshot`; chain-link guarantees `snapshot.ordinal === lastSnapshot.ordinal.next`).
      // `getChangeSetSince(lastSnapshot.ordinal)` returns the contiguous deltas for `(lastSnapshot.ordinal, latestFinalized]`.
      // We adopt only the ONE delta for `snapshot.ordinal` here; the per-ordinal `processSnapshotList` loop advances the
      // rest, each verified against its OWN signed mptRoot. (Re-fetching the change-set per ordinal-step is a perf
      // follow-up — the verify gate's correctness does not depend on batching.)
      val since = lastSnapshot.ordinal
      services.globalL0.getChangeSetSince(since).flatMap {
        case Some(resp) if resp.baseOrdinal.contains(since) =>
          resp.deltas.collectFirst { case (o, d) if o === snapshot.ordinal => d } match {
            case Some(deltaN) => adopt(deltaN)
            case None =>
              logger.debug(
                s"ml0 adopt-and-verify: change-set has no delta for ordinal=${snapshot.ordinal.show} " +
                  s"(base=${since.show}, latest=${resp.latestOrdinal.show}); falling back to createContext"
              ) >> fallbackCreateContext
          }
        case _ =>
          // No client wired, gl0 returned `baseOrdinal=None` (since fell out of the ring), or no response — full path.
          fallbackCreateContext
      }
    }

    def handleIncrementalSnapshot(
      snapshot: Hashed[GlobalIncrementalSnapshot],
      lastSnapshot: Hashed[GlobalIncrementalSnapshot],
      lastState: GlobalSnapshotInfo
    ): F[Unit] =
      // Don't call `ensureMptInitialized(lastSnapshot.ordinal, lastState)` here. It's lossy:
      // syncFromGlobalSnapshotInfo clears the MPT and rebuilds from GSI fields only, which
      // can't fully reproduce the MPT — entries that the incremental writer correctly produced
      // (e.g. expiry-index buckets whose source record is no longer active but whose bucket
      // wasn't explicitly removed) get silently dropped. After the previous snapshot's
      // accept() call, the MPT is already at lastSnapshot.ordinal state via syncFromStateChanges
      // (the lossless incremental path). Initialization happens via handleInitialSnapshot at
      // startup and via recoverFromOrphan / setForRecovery on rollback paths.
      (for {
        _ <- logger.info(s"Processing incremental snapshot ordinal=${snapshot.ordinal}")
        context <- deriveFollowContext(snapshot, lastSnapshot, lastState)
        _ <- storages.lastSyncGlobalSnapshot.set(snapshot, context)
        _ <- sharedStorages.lastNGlobalSnapshot.set(snapshot, context)
        _ <- sharedStorages.lastGlobalSnapshot.set(snapshot, context)
        _ <- persistGlobalSnapshot(snapshot, context)
        _ <- sendGlobalSnapshotSyncConsensusEvent(snapshot)
        _ <- triggerOnGlobalSnapshotPullHook(snapshot, context)
        // Fetch GL0's authoritative finalized ordinal so we only prune SC binaries whose
        // containing GL0 snapshot is actually durable. In BFT GL0 mode every snapshot is
        // immediately final and the endpoint returns the snapshot's own ordinal — same as
        // the legacy behavior. In Nakamoto GL0 mode the endpoint returns the lagging
        // depth-k / attestation-2/3 marker, so binaries stay re-sendable until their
        // containing snapshot is finalized — closes the reorg-loses-binaries gap.
        // Best-effort: if the fetch fails (network blip, BFT GL0 with old binary), fall
        // back to the snapshot's own ordinal which is the legacy default.
        finalizedOrdinal <- services.globalL0.pullLatestFinalizedOrdinal.handleError(_ => none)
        // Extract gl0's authoritative current currency ord for *our* metagraph
        // identifier from the GSI we just computed (#125). This is the watermark for
        // GC'ing stale Pending binaries: anything below this ord on our local fork is
        // definitively past — gl0 has accepted a later currency snapshot for us, so
        // older Pendings (e.g. from a brief metagraph fork that gl0 didn't pick) can
        // never land. Without this, ml0's queue accumulates indefinitely under chain
        // drift and slows tight-budget tests like data-with-fee (iter25 failure mode).
        ourIdentifier <- storages.identifier.get
        gl0KnownCurrencyOrd = context.lastCurrencySnapshots.get(ourIdentifier).map {
          case Left(genesisSnap)   => genesisSnap.value.ordinal
          case Right((incSnap, _)) => incSnap.value.ordinal
        }
        _ <- services.stateChannelBinarySender.confirm(snapshot, gl0KnownCurrencyOrd, finalizedOrdinal).handleErrorWith { error =>
          logger.error(error)("Error when confirming state channel binary") >>
            updateFailedConfirmingStateChannelBinaryMetrics() >>
            Async[F].unit
        }
      } yield ()).handleErrorWith {
        case e: io.constellationnetwork.node.shared.infrastructure.snapshot.GlobalSnapshotContextFunctions.StateProofMismatch =>
          recoverFromOrphan(snapshot.ordinal, e)
        case other => Async[F].raiseError(other)
      }

    // ml0 is on an orphan fork at `lastSnapshot.ordinal` if the incoming snapshot is for the
    // very next ordinal but its `lastSnapshotHash` doesn't point at our local hash. Differs
    // from the StateProofMismatch orphan path (which fires inside `handleIncrementalSnapshot`
    // after createContext disagrees on mptRoot): here we never even reach acceptance, because
    // the chain-link check rejects the snapshot upfront, leaving the loop dropping every pull
    // forever. Treat it the same way — fetch canonical latest from majority and reset.
    def isOrphanFork(lastSnapshot: Hashed[GlobalIncrementalSnapshot], snapshot: Hashed[GlobalIncrementalSnapshot]): Boolean =
      snapshot.ordinal === lastSnapshot.ordinal.next && snapshot.signed.value.lastSnapshotHash =!= lastSnapshot.hash

    def processSnapshotList(snapshots: List[Hashed[GlobalIncrementalSnapshot]]): F[Unit] =
      snapshots.tailRecM {
        case Nil =>
          ().asRight[List[Hashed[GlobalIncrementalSnapshot]]].pure[F]

        case snapshot :: nextSnapshots =>
          storages.lastSyncGlobalSnapshot.get.flatMap {
            case Some(lastSnapshot) if !Validator.isNextSnapshot(lastSnapshot, snapshot.signed.value) =>
              if (isOrphanFork(lastSnapshot, snapshot))
                logger
                  .warn(
                    s"ml0 orphan fork detected at ord=${lastSnapshot.ordinal.show}: incoming ord=${snapshot.ordinal.show} parent=${snapshot.signed.value.lastSnapshotHash.value
                        .take(12)} ≠ local=${lastSnapshot.hash.value.take(12)}; triggering recovery"
                  ) >>
                  recoverFromOrphan(
                    lastSnapshot.ordinal,
                    new RuntimeException(s"orphan fork at ord=${lastSnapshot.ordinal.show} (chain-link mismatch)")
                  ).as(().asRight[List[Hashed[GlobalIncrementalSnapshot]]])
              else
                logger
                  .warn(
                    s"Skipping non-next global snapshot ordinal=${snapshot.ordinal.show} (last=${lastSnapshot.ordinal.show}), dropping ${nextSnapshots.size + 1} remaining"
                  )
                  .as(().asRight[List[Hashed[GlobalIncrementalSnapshot]]])

            case _ =>
              storages.lastSyncGlobalSnapshot.getCombined.flatMap {
                case Some((lastSnapshot, lastState)) =>
                  handleIncrementalSnapshot(snapshot, lastSnapshot, lastState)
                    .as(nextSnapshots.asLeft[Unit])

                case None =>
                  logger
                    .warn(
                      s"Cannot process global snapshot ordinal=${snapshot.ordinal.show}: lastSyncGlobalSnapshot is empty, dropping ${nextSnapshots.size + 1} remaining"
                    )
                    .as(().asRight[List[Hashed[GlobalIncrementalSnapshot]]])
              }
          }
      }

    // Finality-gated pull (#122). ml0 consumes only depth-k-finalized gl0 snapshots —
    // gl0 reorgs are bounded by the depth-k window, so ml0 never observes a snapshot
    // that later gets reorg'd away. The recoverFromOrphan path therefore becomes
    // unreachable on the happy path; kept as defense-in-depth and logged as WARN if
    // it ever fires.
    //
    // State-advancement gating vs SC-binary confirmation (#123): G1 correctly gates
    // *state advancement* (only finalized snapshots feed accept() / MPT updates), but
    // SC-binary confirmation is a separate operational signal. A binary that landed in
    // gl0 best-tip (unfinalized) is already making progress through consensus, and
    // retry-mode shouldn't shrink cap to 0 while waiting on the slower finality marker.
    // So we pull the full best-tip range, soft-observe the unfinalized slice via
    // `stateChannelBinarySender.softConfirm` (operational signal — doesn't prune, doesn't
    // promote Pending→Confirmed), then return only the finalized slice for state derivation.
    //
    // ml0 already fetches `pullLatestFinalizedOrdinal` in handleIncrementalSnapshot for
    // SC-binary pruning (:236). We fetch it here too so we never advance state past
    // unfinalized snapshots; the two calls are cheap (single peer endpoint) and the
    // values are monotonic per node so transient inconsistency between calls is harmless.
    def softObserveUnfinalized(unfinalized: List[Hashed[GlobalIncrementalSnapshot]]): F[Unit] =
      unfinalized.traverse_(services.stateChannelBinarySender.softConfirm)

    val pullFinalityGated: F[Either[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo), List[Hashed[GlobalIncrementalSnapshot]]]] =
      (storages.lastSyncGlobalSnapshot.get.map(_.map(_.ordinal)), services.globalL0.pullLatestFinalizedOrdinal).flatMapN {
        case (None, None) =>
          // Bootstrap: no stored snapshot yet AND gl0 hasn't reported a finalized ordinal.
          // Idle and retry — bootstrapping against the gl0 best-tip while gl0 is still
          // resolving its own first incrementals causes ml0's first currency snapshot's
          // globalSyncView to reference a transient gl0 hash that gl0 later reorgs away
          // from. The receiver-side check (CurrencySnapshotAcceptanceManager:329) then
          // rejects with `Forced globalSyncView hash mismatch` and the metagraph never
          // advances past genesis on gl0's view. Waiting for at least one gl0 incremental
          // (ord >= 1) to finalize ensures it is stable as a globalSyncView referent. #217.
          logger
            .info("ml0 pullFinalityGated: bootstrap idle — waiting for gl0 first finality (None)")
            .as(List.empty[Hashed[GlobalIncrementalSnapshot]].asRight)
        case (None, Some(finalizedOrd)) if finalizedOrd.value.value < BigInt(1) =>
          // Bootstrap gate strict variant: gl0 reports finalized ord=0 (genesis only).
          // ord=0 is trivially finalized but no incremental exists yet — gl0 may still be
          // racing/reorging ord=1. Idle until at least one incremental is finalized. #217.
          logger
            .info(s"ml0 pullFinalityGated: bootstrap idle — gl0 finalizedOrd=${finalizedOrd.show} < 1 (no incremental finalized yet)")
            .as(List.empty[Hashed[GlobalIncrementalSnapshot]].asRight)
        case (None, Some(finalizedOrd)) =>
          // Bootstrap: no stored snapshot yet. gl0 has finalized at least one incremental
          // (finalizedOrd >= 1) so bootstrap is now safe — the lineage anchor for any
          // subsequent globalSyncView reference is stable. Defer to legacy bootstrap pull
          // (canonical latest with majority verification) which yields a Left →
          // handleInitialSnapshot. Finality gating kicks in on the next 10s tick once
          // lastSyncGlobalSnapshot is set.
          logger.info(s"ml0 pullFinalityGated: bootstrapping after gl0 first finality observed (finalizedOrd=${finalizedOrd.show})") >>
            services.globalL0.pullGlobalSnapshots
        case (Some(lastOrd), None) =>
          logger
            .info(s"ml0 pullFinalityGated: waiting on gl0 finality (last=${lastOrd.show}, finalized=unknown); idling")
            .as(List.empty[Hashed[GlobalIncrementalSnapshot]].asRight)
        case (Some(lastOrd), Some(finalizedOrd)) if finalizedOrd <= lastOrd =>
          // No finalized state to advance to. But best-tip may have unfinalized snapshots
          // containing our binaries — still pull them for soft-observation so retry-mode
          // doesn't starve for confirmation signal during the gap. Single HTTP call, no
          // state mutation.
          logger
            .debug(
              s"ml0 pullFinalityGated: caught up to finality (last=${lastOrd.show}, finalized=${finalizedOrd.show}); polling best-tip for SC observation"
            ) >>
            services.globalL0
              .pullGlobalSnapshots(lastOrd)
              .flatMap {
                case Left(_)          => Applicative[F].unit
                case Right(snapshots) => softObserveUnfinalized(snapshots.filter(_.ordinal > finalizedOrd))
              }
              .as(List.empty[Hashed[GlobalIncrementalSnapshot]].asRight)
        case (Some(lastOrd), Some(finalizedOrd)) =>
          // Pull raw (up to best-tip), split into finalized + unfinalized. Finalized feeds
          // state advancement (return path); unfinalized feeds soft-observation (operational).
          logger.info(s"ml0 pullFinalityGated: pulling (last=${lastOrd.show}, finalized=${finalizedOrd.show}]") >>
            services.globalL0.pullGlobalSnapshots(lastOrd).flatMap {
              case Left(_) =>
                // Bootstrap tuple — shouldn't happen here (we have lastOrd). Preserve the
                // original `pullFinalizedGlobalSnapshots` behavior of treating it as a
                // transient state and returning empty (idle); the bootstrap path runs
                // separately via handleInitialSnapshot from the None case above.
                logger
                  .info(s"pullFinalityGated: bootstrap tuple returned unexpectedly for last=${lastOrd.show}, returning empty")
                  .as(List.empty[Hashed[GlobalIncrementalSnapshot]].asRight)
              case Right(allSnapshots) =>
                val (finalized, unfinalized) = allSnapshots.partition(_.ordinal <= finalizedOrd)
                softObserveUnfinalized(unfinalized) >>
                  logger
                    .info(
                      s"pullFinalityGated: kept ${finalized.size} finalized, soft-observed ${unfinalized.size} unfinalized" +
                        s" (last=${lastOrd.show}, finalized=${finalizedOrd.show})"
                    )
                    .whenA(unfinalized.nonEmpty)
                    .as(finalized.asRight)
            }
      }

    pullFinalityGated.flatMap {
      case Left((snapshot, state)) =>
        handleInitialSnapshot(snapshot, state)

      case Right(Nil) =>
        Applicative[F].unit

      case Right(snapshots) =>
        processSnapshotList(snapshots)
    }
  }

  def performGlobalL0PeerDiscovery[F[_]: Async](
    storages: Storages[F],
    programs: Programs[F]
  ): F[Unit] =
    storages.lastSyncGlobalSnapshot.get.flatMap {
      case None =>
        storages.globalL0Cluster.getRandomPeer.flatMap(p => programs.globalL0PeerDiscovery.discoverFrom(p))

      case Some(latestSnapshot) =>
        programs.globalL0PeerDiscovery.discover(latestSnapshot.signed.proofs.map(_.id).map(PeerId._Id.reverseGet))
    }
}
