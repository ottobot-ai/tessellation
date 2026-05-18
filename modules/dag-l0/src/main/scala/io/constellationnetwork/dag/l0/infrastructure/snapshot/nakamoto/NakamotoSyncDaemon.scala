package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.security.KeyPair

import cats.effect.kernel.{Async, Clock, Ref}
import cats.effect.std.{Semaphore, Supervisor}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.dag.l0.infrastructure.snapshot._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event.{DAGEvent, GlobalSnapshotEvent}
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.consensus.ConsensusFunctions
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage, SnapshotStorage}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.{GossipStream, SidecarClient}
import io.constellationnetwork.node.shared.infrastructure.mempool.EventMempool
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{LddConfig, TipAttestation => DomainTipAttestation}
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.Signature
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.vrf.EcVrf25519
import io.constellationnetwork.serde.codecs.instances.CompatCodecs._

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import io.estatico.newtype.ops._
import io.grpc.ManagedChannel
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Subscribes to sidecar GossipSub for live snapshots and attestations.
  *
  * Uses NakamotoChainStore for fork-aware storage instead of raw SnapshotStorage.prepend. Records attestations in TipTracker for finality
  * tracking.
  */
object NakamotoSyncDaemon {

  private val CatchUpThreshold = 6L
  private val CatchUpCooldownMs = 10000L // Don't retry catch-up more often than every 10s

  /** Confirmation depth k — same as SnapshotLeaderLoop.ConfirmationDepthK. Used as the boundary between Tier 2 (sequential walk-back) and
    * Tier 3 (full catch-up + backfill). Gaps > k mean the network has finalized past our tip; sequential fetch won't work.
    */
  private val ConfirmationDepthK: Long =
    sys.env.get("NAKAMOTO_CONFIRMATION_DEPTH").flatMap(_.toLongOption).getOrElse(255L)

  private val vrf = EcVrf25519.default

  /** Derive VRF output from proof bytes. The chain store needs the output (not the proof) for eta computation. The producer stores
    * vrfOutput directly, but gossip only carries the proof — we must derive the output here to match what the producer stored.
    */
  private def vrfOutputFromProof(proofBytes: Array[Byte]): Array[Byte] =
    vrf.vrfProofToHash(proofBytes).getOrElse(proofBytes) // fallback to raw proof if derivation fails

  /** Ethereum-style mempool reconciliation after catch-up/reorg.
    *
    * Evicts DAG blocks whose transactions reference a lastTxRef that no longer matches the new context. Keeps events whose transactions are
    * still unconfirmed in the new state — they should be included in the next snapshot.
    *
    * Non-DAG events (state channel, allow spend, etc.) are preserved since they have separate validation semantics handled by the
    * acceptance manager.
    */
  private def reconcileMempool[F[_]: Async](
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    context: GlobalSnapshotInfo,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    for {
      hashes <- eventMempool.getEventHashes
      events <- eventMempool.getMultiple(hashes)
      staleHashes = events.collect {
        case (hash, hashed) =>
          hashed.signed.value match {
            case DAGEvent(signedBlock) =>
              val isStale = signedBlock.value.transactions.exists { signedTx =>
                val tx = signedTx.value
                val contextRef = context.lastTxRefs.getOrElse(tx.source, TransactionReference.empty)
                // Block is stale if the context's lastTxRef for this source
                // differs from the transaction's parent ref — meaning the context
                // has already processed a different transaction chain for this address.
                contextRef =!= TransactionReference.empty && contextRef =!= tx.parent
              }
              if (isStale) Some(hash) else None
            case _ => None // keep non-DAG events
          }
      }.flatten.toSet
      poolSize <- eventMempool.size
      _ <-
        if (staleHashes.nonEmpty)
          logger.info(
            s"🧹 Mempool reconciliation: evicting ${staleHashes.size} stale DAG blocks, keeping ${poolSize - staleHashes.size} events"
          ) >> eventMempool.remove(staleHashes)
        else
          logger.info(s"🧹 Mempool reconciliation: all $poolSize events valid for new context, keeping all")
    } yield ()

  /** After storing a snapshot, check if any buffered gossip snapshots were waiting for it as their parent. If so, process them via
    * handleSnapshot (which will now find the parent in the chain store and validate successfully). This creates a validation cascade from
    * the shared genesis ancestor through the gossip chain.
    */
  private def drainPendingChildren[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector: Metrics](
    storedHash: Hash,
    stateRef: Ref[F, SyncState],
    pendingParentRef: Ref[F, Map[Hash, List[pb.Snapshot]]],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    chainSyncManager: ChainSyncManager.ChainSyncManagerAlgebra[F],
    channel: ManagedChannel,
    dataDir: java.nio.file.Path,
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F],
    logger: org.typelevel.log4cats.Logger[F]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit,
    supervisor: Supervisor[F]
  ): F[Unit] =
    pendingParentRef.modify { m =>
      val children = m.getOrElse(storedHash, List.empty)
      (m - storedHash, children)
    }.flatMap { children =>
      if (children.isEmpty) Async[F].unit
      else
        logger.info(s"🔗 Draining ${children.size} buffered snapshot(s) whose parent ${storedHash.value.take(12)} is now available") >>
          children.traverse_ { childSnap =>
            handleSnapshot(
              childSnap,
              stateRef,
              pendingParentRef,
              chainStore,
              nodeStorage,
              tipTracker,
              stakeRegistry,
              sidecarClient,
              selfId,
              keyPair,
              lddConfig,
              eligibilityChecker,
              lastKnownSlotRef,
              epochStateRef,
              etaRotationSnapshots,
              consensusFns,
              snapshotStorage,
              lastGlobalSnapshotStorage,
              lastNGlobalSnapshotStorage,
              productionGate,
              mptStore,
              mptOverlay,
              eventMempool,
              chainSyncManager,
              channel,
              dataDir,
              operationalKeyMaker,
              kesRegistry,
              logger
            )
          }
    }

  final case class SyncState(
    networkTipOrdinal: Long,
    networkTipHash: Option[Hash],
    localTipOrdinal: Long,
    isReady: Boolean,
    lastCatchUpAttemptMs: Long = 0L
  )

  object SyncState {
    def initial: SyncState = SyncState(0L, None, 0L, isReady = false)
  }

  def run[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector: Metrics](
    channel: ManagedChannel,
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    snapshotSemaphore: Semaphore[F],
    productionGate: ProductionGate[F],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    dataDir: java.nio.file.Path,
    processMetagraphBinary: io.constellationnetwork.statechannel.StateChannelOutput => F[Unit],
    // (#196) Sink for inbound AllowSpendBlock gossip — same queue
    // GlobalSnapshotEventsPublisherDaemon drains into the event mempool.
    // Replaces the HTTP POST path from AllowSpendBlockRoutes (which was
    // wired to `queues.l1AllowSpendOutput`); the new outbox-backed gossip
    // path feeds the same queue from a different transport.
    enqueueAllowSpendBlock: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.swap.AllowSpendBlock
    ] => F[Unit],
    // Shared `Option` ref so other components (e.g. the reactive finality-walkback
    // ChainSyncRequestQueue) can route through the same hash-keyed dedup + fetch
    // machinery without needing their own `ChainSyncManager`. We publish our
    // internally-constructed manager into this Ref once it's built; consumers
    // read-through it and no-op if the producer hasn't bound yet.
    sharedChainSyncManagerRef: Ref[F, Option[ChainSyncManager.ChainSyncManagerAlgebra[F]]],
    // §1.2 Slice 5/6/9: KES parallel-signing infrastructure (sender) + load-bearing
    // receiver-side verify. The OperationalKeyMaker signs attestations + snapshots with
    // the operator's KES product key at the period derived from `EtaCalculation.rotationPeriod`.
    // The KesRegistry holds the (peerId → (kesMasterVk, offset)) map needed by receivers to
    // verify incoming KES sigs and drop them when verification fails.
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F]
  )(
    implicit globalStateProofSelector: io.constellationnetwork.schema.GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit,
    supervisor: Supervisor[F]
  ): fs2.Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("NakamotoSyncDaemon")

    // Buffer for gossip snapshots whose parent isn't in the chain store yet.
    // Keyed by missing parent hash → list of raw gossip snapshots waiting for that parent.
    // When a snapshot is stored in the chain store, we check this buffer and validate any
    // snapshots that were waiting for it. This creates a validation cascade from genesis.
    fs2.Stream.eval(Ref.of[F, Map[Hash, List[pb.Snapshot]]](Map.empty)).flatMap { pendingParentRef =>
      fs2.Stream.eval(Ref.of[F, SyncState](SyncState.initial)).flatMap { stateRef =>
        // ChainSyncManager for active parent fetching. Uses a Ref to break the
        // circular dependency: handleSnapshot needs chainSyncManager, but
        // chainSyncManager's callback needs handleSnapshot.
        fs2.Stream
          .eval(
            Ref.of[F, Option[ChainSyncManager.ChainSyncManagerAlgebra[F]]](None).flatMap { csRef =>
              ChainSyncManager
                .make[F](
                  channel,
                  { resp: io.constellationnetwork.node.shared.domain.nakamoto.chainsync.ChainSyncStateResponse[pb.Snapshot] =>
                    // #56.8: ADT pattern-match on the chain-sync disposition. Today's `handleSnapshot`
                    // pipeline doesn't yet differentiate finalized vs provisional — both are validated
                    // and stored identically. The structural distinction lands here so #56.10's
                    // overlay-aware accept() can route Provisional fetches to overlay-local commits
                    // (matching codex's NakamotoSyncDaemon catch in plan rev4 phase E) and Finalized
                    // fetches straight to base. NotFound logs and unwinds the inflight tracker.
                    import io.constellationnetwork.node.shared.domain.nakamoto.chainsync.ChainSyncStateResponse
                    val fetchedSnap: Option[pb.Snapshot] = resp match {
                      case ChainSyncStateResponse.Finalized(snap)      => Some(snap)
                      case ChainSyncStateResponse.Provisional(snap, _) => Some(snap)
                      case ChainSyncStateResponse.NotFound(_)          => None
                    }
                    fetchedSnap match {
                      case Some(snap) =>
                        csRef.get.flatMap {
                          case Some(csm) =>
                            snapshotSemaphore.permit.use { _ =>
                              handleSnapshot(
                                snap,
                                stateRef,
                                pendingParentRef,
                                chainStore,
                                nodeStorage,
                                tipTracker,
                                stakeRegistry,
                                sidecarClient,
                                selfId,
                                keyPair,
                                lddConfig,
                                eligibilityChecker,
                                lastKnownSlotRef,
                                epochStateRef,
                                etaRotationSnapshots,
                                consensusFns,
                                snapshotStorage,
                                lastGlobalSnapshotStorage,
                                lastNGlobalSnapshotStorage,
                                productionGate,
                                mptStore,
                                mptOverlay,
                                eventMempool,
                                csm,
                                channel,
                                dataDir,
                                operationalKeyMaker,
                                kesRegistry,
                                logger
                              )
                            }
                          case None => Async[F].unit
                        }
                      case None =>
                        // NotFound — peer doesn't have the requested hash. Inflight tracking is
                        // unwound by ChainSyncManager's `guaranteeCase`; nothing further to do here.
                        Async[F].unit
                    }
                  }
                )
                .flatMap(csm => csRef.set(Some(csm)) >> sharedChainSyncManagerRef.set(Some(csm)).as(csm))
            }
          )
          .flatMap { chainSyncManager =>
            // Gossip stream with two-layer reconnection:
            //
            // Layer 1 (sidecar-triggered): When the sidecar's mesh health monitor
            //   recovers from degradation, it closes the gRPC Subscribe stream.
            //   handleErrorWith catches the error and re-subscribes.
            //
            // Layer 2 (idle watchdog): If the gRPC connection dies silently (e.g.,
            //   Docker network disconnect — gRPC Java doesn't propagate channel
            //   failures to blocking server-stream reads), the stream hangs forever.
            //   A concurrent watchdog checks the last-received timestamp every 30s
            //   and raises an error after 120s of idle. In normal operation (messages
            //   every ~10s), the watchdog never fires.
            //
            // Shared state (pendingParentRef, stateRef, chainSyncManager) survives.
            def gossipStream: fs2.Stream[F, Unit] =
              fs2.Stream
                .eval(Ref.of[F, Long](System.currentTimeMillis()))
                .flatMap { lastMsgRef =>
                  val watchdog = fs2.Stream.fixedRate[F](30.seconds).evalMap { _ =>
                    Async[F].delay(System.currentTimeMillis()).flatMap { now =>
                      lastMsgRef.get.flatMap { lastMsg =>
                        val idleMs = now - lastMsg
                        if (idleMs > 120000L)
                          logger.warn(s"Gossip stream idle for ${idleMs / 1000}s, forcing reconnect") >>
                            Async[F].raiseError[Unit](new RuntimeException(s"Gossip idle timeout (${idleMs / 1000}s)"))
                        else
                          Async[F].unit
                      }
                    }
                  }

                  val gossip = GossipStream
                    .subscribe[F](channel)
                    .evalMap { msg =>
                      lastMsgRef.set(System.currentTimeMillis()) >>
                        (msg.body match {
                          case pb.GossipMessage.Body.Snapshot(snap) =>
                            val incomingOrdinal = snap.ordinal
                            chainStore.bestTipOrdinal.flatMap { currentBestOrdinal =>
                              val wouldWin = incomingOrdinal > currentBestOrdinal.getOrElse(0L)
                              (if (wouldWin) productionGate.pause(ProductionGate.BetterGossipReceived)
                               else Async[F].unit) >>
                                snapshotSemaphore.permit.use { _ =>
                                  handleSnapshot(
                                    snap,
                                    stateRef,
                                    pendingParentRef,
                                    chainStore,
                                    nodeStorage,
                                    tipTracker,
                                    stakeRegistry,
                                    sidecarClient,
                                    selfId,
                                    keyPair,
                                    lddConfig,
                                    eligibilityChecker,
                                    lastKnownSlotRef,
                                    epochStateRef,
                                    etaRotationSnapshots,
                                    consensusFns,
                                    snapshotStorage,
                                    lastGlobalSnapshotStorage,
                                    lastNGlobalSnapshotStorage,
                                    productionGate,
                                    mptStore,
                                    mptOverlay,
                                    eventMempool,
                                    chainSyncManager,
                                    channel,
                                    dataDir,
                                    operationalKeyMaker,
                                    kesRegistry,
                                    logger
                                  )
                                } >>
                                productionGate.resume(ProductionGate.BetterGossipReceived)
                            }

                          case pb.GossipMessage.Body.Attestation(att) =>
                            handleAttestation(att, tipTracker, kesRegistry, etaRotationSnapshots, logger)

                          case pb.GossipMessage.Body.MetagraphBinary(mb) =>
                            handleMetagraphBinary(mb, processMetagraphBinary, logger)

                          case _: pb.GossipMessage.Body.MetagraphAttestation =>
                            // S2.5 wire-format is in place; the receiver-verify + aggregator.record
                            // wiring is Slice S3 (load-bearing pre-inclusion gate). No-op here
                            // keeps the gossip stream flowing in the warn-only window.
                            Async[F].unit

                          case pb.GossipMessage.Body.AllowSpendBlock(asb) =>
                            handleAllowSpendBlock(asb, enqueueAllowSpendBlock, logger)

                          case _: pb.GossipMessage.Body.Rumor =>
                            Async[F].unit

                          case pb.GossipMessage.Body.Empty =>
                            Async[F].unit
                        })
                    }

                  gossip.concurrently(watchdog)
                }
                .handleErrorWith { e =>
                  fs2.Stream.eval(
                    logger.warn(s"Gossip stream error: ${e.getMessage}. Reconnecting in 5s...")
                  ) ++ fs2.Stream.sleep_[F](5.seconds) ++ gossipStream
                } ++ fs2.Stream.eval(
                // Normal termination: gRPC StreamObserver.onError puts None in the
                // queue, causing fromQueueNoneTerminated to end the stream normally
                // (not as an error). This happens when the sidecar connection dies
                // during a partition. Restart after a short delay.
                logger.warn("Gossip stream terminated (sidecar connection lost). Reconnecting in 5s...")
              ) ++ fs2.Stream.sleep_[F](5.seconds) ++ gossipStream

            gossipStream
          }
      }
    }
  }

  private def handleSnapshot[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector: Metrics](
    snap: pb.Snapshot,
    stateRef: Ref[F, SyncState],
    pendingParentRef: Ref[F, Map[Hash, List[pb.Snapshot]]],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    chainSyncManager: ChainSyncManager.ChainSyncManagerAlgebra[F],
    channel: ManagedChannel,
    dataDir: java.nio.file.Path,
    // §1.2 Slice 5/6/9: KES infrastructure for sender-side signing + receiver-side load-bearing verify.
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F],
    logger: org.typelevel.log4cats.Logger[F]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit,
    supervisor: Supervisor[F]
  ): F[Unit] =
    for {
      producerShort <- Async[F].pure(snap.producerId.toByteArray.take(4).map("%02x".format(_)).mkString)
      _ <- logger.info(
        s"📥 Received snapshot ordinal=${snap.ordinal} slot=${snap.slot} parentSlot=${snap.parentSlot} from=$producerShort"
      )

      // Per-producer counter for cross-peer eligibility / dominance analysis. Tagged
      // with the 8-char producer prefix so it can be queried as
      // `topk(N, dag_nakamoto_snapshots_received_by_producer) by (producer_id)`.
      _ <- Metrics[F].incrementCounter(
        "dag_nakamoto_snapshots_received_by_producer",
        Seq(Metrics.unsafeLabelName("producer_id") -> producerShort)
      )

      // Deserialize payload
      parsed =
        if (snap.payload.size() > 0) {
          val payloadStr = snap.payload.toByteArray.map(_.toChar).mkString
          (for {
            json <- io.circe.parser.parse(payloadStr)
            snapshotJson <- json.hcursor.get[io.circe.Json]("snapshot")
            contextJson <- json.hcursor.get[io.circe.Json]("context")
            snapshot <- snapshotJson.as[Signed[GlobalIncrementalSnapshot]]
            context <- contextJson.as[GlobalSnapshotInfo]
          } yield (snapshot, context)).toOption
        } else None

      genesisEta <- epochStateRef.get.map(_.genesisEta)
      // Use parentSlot from gossip message for gap (same inputs as producer used)
      slotGap = snap.slot - snap.parentSlot
      parentHash = Hash(new String(snap.parentHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))

      // Chain-derived eta: must be computed from the INCOMING snapshot's chain,
      // not our local best tip. The producer used eta derived from its own chain
      // (walking back from its parent). If we use our local tip's chain, we'll
      // compute a different eta when forks diverge → VRF verification fails.
      //
      // Strategy: walk from the incoming snapshot's parentHash.
      // Fallback: if the incoming snapshot carries an eta field, use it directly
      // (trust-but-verify: we verify the snapshot's chain ancestry separately).
      //
      // Rotation period is keyed on the **predecessor ordinal** (`snap.ordinal - 1`),
      // mirroring the producer side at `SnapshotLeaderLoop:281` where `lastChainOrdinal`
      // (= bestTipOrdinal at production time, = N-1 for snap N) is the divisor input.
      // Period assignment must be a function of the snapshot itself so every honest
      // verifier reaches the same conclusion regardless of where their local bestTip is.
      // Using `snap.ordinal` directly causes an off-by-one at every R boundary: the
      // producer of ord=R uses (R-1)/R = period (R-1)/R → 0, but a naive verifier
      // computes R/R = 1, expecting derived eta where the SlotCertificate carries
      // genesisEta. This caused iter35's finalization stall at ord=99 (R=100 boundary).
      // See `docs/nakamoto/attestation-and-finality.md` §1.
      currentPeriod = EtaCalculation.rotationPeriod(math.max(0L, snap.ordinal - 1), etaRotationSnapshots)
      eta <-
        if (currentPeriod <= 0) {
          Async[F].pure(genesisEta)
        } else {
          chainStore.vrfOutputsForPeriodFrom(currentPeriod - 1, etaRotationSnapshots, parentHash).flatMap { chainOutputs =>
            if (chainOutputs.nonEmpty) {
              Async[F].pure(EtaCalculation.computeEta(genesisEta, currentPeriod, chainOutputs.map(_._2)))
            } else {
              // Parent chain not in our store (different fork or pruned).
              // Fall back to the eta embedded in the snapshot itself.
              // This is safe: VRF proof verification ensures the producer
              // was eligible under THIS eta, and content validation later
              // ensures the snapshot's state transitions are correct.
              parsed.flatMap(_._1.value.eta) match {
                case Some(etaHash) =>
                  // Hash wraps a hex string — decode to 32 bytes
                  val hexStr = etaHash.value
                  val decoded = hexStr.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
                  logger
                    .info(
                      s"Using embedded eta from snapshot (parent chain not in store, period=$currentPeriod, eta=${hexStr.take(16)}..)"
                    )
                    .as(decoded)
                case None =>
                  // No embedded eta and no chain data — use genesis
                  Async[F].pure(genesisEta)
              }
            }
          }
        }

      // Validation pipeline + chainStore.store. Pre-Phase E this body ran inside
      // `mptStore.withTransaction` so `accept()`'s mid-flight MPT mutations could be
      // rolled back when the snapshot didn't become the new bestTip. Phase D
      // (commit caa3559e) routed `accept()` through the overlay algebra
      // (`overlay.checkout` / `overlay.commit`); under `MptOverlay.OverlayMode.MultiBranch`
      // each download lands in its own pending `ChangeSet` — non-canonical branches sit
      // in pending until eviction (#56.9) drops them or `finalizeBranch` promotes the
      // canonical chain. The savepoint bracket is obsolete and removed here: the
      // overlay's branching IS the rollback. Under the current `Passthrough` wiring,
      // writes still land in base immediately and we lose the daemon-side rollback
      // semantics — that's intentional migration scaffolding, byte-equivalent to the
      // legacy path under #107's parity gate. The proper isolation arrives when
      // Phase J flips production wiring to `MultiBranch`.
      validationResult <- parsed match {
        case Some((signedSnapshot, context)) =>
          chainStore.get(parentHash).flatMap {
            case Some(parentStored) =>
              NakamotoSnapshotValidator.validate[F](
                signedSnapshot = signedSnapshot,
                context = context,
                slot = snap.slot,
                vrfProof = snap.vrfProof.toByteArray,
                vrfPublicKey = snap.vrfPublicKey.toByteArray,
                producerIdBytes = snap.producerId.toByteArray,
                eta = eta,
                slotGap = slotGap,
                stakeRegistry = stakeRegistry,
                lddConfig = lddConfig,
                eligibilityChecker = eligibilityChecker,
                consensusFns = consensusFns,
                lastSignedArtifact = parentStored.signedSnapshot,
                lastContext = parentStored.context,
                getByOrdinal = { (ordinal: SnapshotOrdinal) =>
                  snapshotStorage.get(ordinal).flatMap {
                    case Some(s) => HasherSelector[F].withCurrent(implicit h => s.toHashed[F].map(_.some))
                    case None =>
                      chainStore.getByOrdinal(ordinal.value.value).flatMap {
                        case Some(stored) =>
                          HasherSelector[F].withCurrent(implicit h => stored.signedSnapshot.toHashed[F].map(_.some))
                        case None => Async[F].pure(None: Option[Hashed[GlobalIncrementalSnapshot]])
                      }
                  }
                },
                mptOverlay = mptOverlay
              )
            case None =>
              // Parent not in chain store. Three-tier gap handling:
              // Tier 1 (<=6): buffer + ChainSync parent fetch (normal gossip latency)
              // Tier 2 (>6, <=k): sequential walk-back via ChainSync (moderate drift)
              // Tier 3 (>k): full catch-up — network finalized past us
              chainStore.bestTipOrdinal.flatMap { localBestOrdinal =>
                val localOrd = localBestOrdinal.getOrElse(0L)
                val gap = snap.ordinal - localOrd
                if (gap > ConfirmationDepthK) {
                  logger.warn(
                    s"🔄 Tier 3: gap=$gap > k=$ConfirmationDepthK for ordinal=${snap.ordinal}. Triggering full catch-up."
                  ) >>
                    Async[F].pure(
                      NakamotoSnapshotValidator.ParentNotFound: NakamotoSnapshotValidator.ValidationResult
                    )
                } else if (gap > CatchUpThreshold) {
                  logger.info(
                    s"⏳ Tier 2: gap=$gap (>$CatchUpThreshold, <=$ConfirmationDepthK) for ordinal=${snap.ordinal}. " +
                      s"Sequential walk-back from parent ${parentHash.value.take(12)}."
                  ) >>
                    pendingParentRef.update { m =>
                      val existing = m.getOrElse(parentHash, List.empty)
                      m.updated(parentHash, existing :+ snap)
                    } >>
                    chainSyncManager.requestMissing(parentHash) >>
                    Async[F].pure(
                      NakamotoSnapshotValidator.ParentBuffered: NakamotoSnapshotValidator.ValidationResult
                    )
                } else {
                  logger.info(
                    s"⏳ Tier 1: Parent ${parentHash.value.take(12)} not in chain store for ordinal=${snap.ordinal} (gap=$gap). Buffering."
                  ) >>
                    pendingParentRef.update { m =>
                      val existing = m.getOrElse(parentHash, List.empty)
                      m.updated(parentHash, existing :+ snap)
                    } >>
                    chainSyncManager.requestMissing(parentHash) >>
                    Async[F].pure(
                      NakamotoSnapshotValidator.ParentBuffered: NakamotoSnapshotValidator.ValidationResult
                    )
                }
              }
          }
        case None =>
          // No payload — fall back to VRF-only validation (legacy/PoC). No accept() and
          // therefore no MPT mutation.
          val vrfVK = snap.vrfPublicKey.toByteArray
          val proof = snap.vrfProof.toByteArray
          val producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)
          val producerId = peer.PeerId(producerHex)
          stakeRegistry.relativeStake(producerId).flatMap { producerStake =>
            val vrfValidF =
              if (vrfVK.isEmpty || proof.isEmpty) false.pure[F]
              else
                eligibilityChecker.verifyEligibility(
                  vrfVK = vrfVK,
                  slot = Slot(NonNegLong.unsafeFrom(snap.slot)),
                  slotGap = slotGap,
                  eta = eta,
                  relativeStake = producerStake,
                  config = lddConfig,
                  proof = proof
                )
            vrfValidF.map { vrfValid =>
              if (vrfValid) NakamotoSnapshotValidator.Valid(null, null) // VRF-only, no snapshot data
              else NakamotoSnapshotValidator.VrfOnlyFailed(snap.slot)
            }
          }
      }
      storeOutcome <- validationResult match {
        case v: NakamotoSnapshotValidator.Valid if v.snapshot != null && v.context != null =>
          for {
            isNew <- chainStore.store(
              v.snapshot,
              v.context,
              snap.ordinal,
              snap.slot,
              parentHash,
              vrfOutputFromProof(snap.vrfProof.toByteArray)
            )
            bestTipOpt <- chainStore.bestTip
            thisHash <- HasherSelector[F].withCurrent(implicit h => v.snapshot.toHashed[F].map(_.hash))
            becameBest = isNew && bestTipOpt.exists(_.hash === thisHash)
            _ <- Async[F].whenA(isNew && !becameBest) {
              logger.info(
                s"🔀 Stored Nakamoto snapshot at ordinal=${snap.ordinal} as fork branch (not bestTip) — overlay isolates pending branches from canonical."
              )
            }
          } yield (becameBest, v.snapshot.some, v.context.some)
        case _ =>
          (false, none[Signed[GlobalIncrementalSnapshot]], none[GlobalSnapshotInfo]).pure[F]
      }
      (becameBest, signedOpt, ctxOpt) = storeOutcome

      _ <- (validationResult: NakamotoSnapshotValidator.ValidationResult) match {
        case NakamotoSnapshotValidator.Valid(_, _) =>
          processValidSnapshot(
            snap,
            signedOpt,
            ctxOpt,
            becameBest,
            stateRef,
            chainStore,
            nodeStorage,
            tipTracker,
            sidecarClient,
            selfId,
            keyPair,
            lastKnownSlotRef,
            epochStateRef,
            etaRotationSnapshots,
            snapshotStorage,
            lastGlobalSnapshotStorage,
            lastNGlobalSnapshotStorage,
            productionGate,
            operationalKeyMaker,
            kesRegistry,
            logger
          ) >> {
            // This snapshot is now stored — drain any children that were waiting for it.
            val storedHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
            drainPendingChildren(
              storedHash,
              stateRef,
              pendingParentRef,
              chainStore,
              nodeStorage,
              tipTracker,
              stakeRegistry,
              sidecarClient,
              selfId,
              keyPair,
              lddConfig,
              eligibilityChecker,
              lastKnownSlotRef,
              epochStateRef,
              etaRotationSnapshots,
              consensusFns,
              snapshotStorage,
              lastGlobalSnapshotStorage,
              lastNGlobalSnapshotStorage,
              productionGate,
              mptStore,
              mptOverlay,
              eventMempool,
              chainSyncManager,
              channel,
              dataDir,
              operationalKeyMaker,
              kesRegistry,
              logger
            )
          }
        case NakamotoSnapshotValidator.ParentNotFound =>
          // Parent not found — network is ahead of us (restart scenario).
          catchUpFromGossip(
            snap,
            parsed,
            stateRef,
            chainStore,
            snapshotStorage,
            lastGlobalSnapshotStorage,
            lastNGlobalSnapshotStorage,
            lastKnownSlotRef,
            mptStore,
            eventMempool,
            productionGate,
            channel,
            dataDir,
            logger
          )
        case _: NakamotoSnapshotValidator.ContentMismatch =>
          // Content mismatch — could be normal fork or restart scenario.
          // Only catch up if incoming ordinal is significantly ahead of our canonical tip.
          snapshotStorage.head.flatMap {
            case Some((localTip, _)) =>
              val localOrd = localTip.ordinal.value.value
              val gap = snap.ordinal - localOrd
              if (gap >= CatchUpThreshold) {
                logger.warn(
                  s"\uD83D\uDD04 Content mismatch with ordinal gap=$gap (local=$localOrd, incoming=${snap.ordinal}). Triggering catch-up."
                ) >>
                  catchUpFromGossip(
                    snap,
                    parsed,
                    stateRef,
                    chainStore,
                    snapshotStorage,
                    lastGlobalSnapshotStorage,
                    lastNGlobalSnapshotStorage,
                    lastKnownSlotRef,
                    mptStore,
                    eventMempool,
                    productionGate,
                    channel,
                    dataDir,
                    logger
                  )
              } else {
                // Normal Nakamoto fork — store the snapshot as an alternative branch in
                // chainStore WITHOUT updating canonical state (snapshotStorage,
                // lastGlobalSnapshotStorage, MPT). Content hasn't been validated against
                // this fork's parent state yet — validation is deferred to reorg time.
                //
                // VRF + signature + slot-cert are already validated (proof of eligibility).
                // Content validation (state proof match) requires the fork's parent context
                // which we don't have locally. If ChainSelection later picks this fork as
                // denser (reorg), we validate by triggering catch-up which resets state to
                // the fork's context + MPT self-healing.
                logger.info(
                  s"🔀 Fork at ordinal=${snap.ordinal} slot=${snap.slot} (gap=$gap). Storing as tentative branch (deferred validation)."
                ) >>
                  Metrics[F].incrementCounter("dag_nakamoto_forks_stored") >>
                  storeForkBranch(
                    snap,
                    stateRef,
                    chainStore,
                    tipTracker,
                    snapshotStorage,
                    lastGlobalSnapshotStorage,
                    lastNGlobalSnapshotStorage,
                    lastKnownSlotRef,
                    mptStore,
                    mptOverlay,
                    eventMempool,
                    productionGate,
                    logger
                  )
              }
            case None =>
              catchUpFromGossip(
                snap,
                parsed,
                stateRef,
                chainStore,
                snapshotStorage,
                lastGlobalSnapshotStorage,
                lastNGlobalSnapshotStorage,
                lastKnownSlotRef,
                mptStore,
                eventMempool,
                productionGate,
                channel,
                dataDir,
                logger
              )
          }
        case NakamotoSnapshotValidator.ParentBuffered =>
          // Already buffered for validation when parent arrives — nothing more to do
          Async[F].unit
        case invalid: NakamotoSnapshotValidator.Invalid =>
          Metrics[F].incrementCounter("dag_nakamoto_snapshots_rejected") >>
            logger.warn(s"❌ REJECTED snapshot slot=${snap.slot} ordinal=${snap.ordinal}: $invalid")
      }
    } yield ()

  /** Process a VRF-validated snapshot AFTER the validate+chainStore.store bracket has decided whether to commit MPT.
    *
    *   - `signedSnapshot`/`context` are `None` for VRF-only payloads (no body to thread through canonical state).
    *   - `becameBestTip` gates the canonical storage updates (snapshotStorage / last*Snapshot / lastKnownSlot). When false, the snapshot
    *     was stored as a fork branch (or duplicate) in chainStore and MPT was rolled back; we still update tip-tracking, attestation, and
    *     ready-transition.
    */
  private def processValidSnapshot[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    snap: pb.Snapshot,
    signedSnapshot: Option[Signed[GlobalIncrementalSnapshot]],
    context: Option[GlobalSnapshotInfo],
    becameBestTip: Boolean,
    stateRef: Ref[F, SyncState],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    // §1.2 Slice 9: snapshot KES gate runs BEFORE any state mutation. On no-sig /
    // decode-fail / verify-fail / step-out-of-range, drop the snapshot entirely — no
    // networkTip update, no canonical-storage write, no self-attestation emit. The
    // no-registry-entry carve-out accepts so Ed25519-authenticated peers awaiting
    // their Slice 10 (#179) reg-cert finality aren't silently dropped.
    val kesGate: F[Boolean] = KesGossipVerification.verifySnapshot(
      messageBytes = snap.hash.toByteArray,
      kesSigBytes = snap.kesSignature.toByteArray,
      producerId = peer.PeerId(Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)),
      producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString),
      ordinal = snap.ordinal,
      kesRegistry = kesRegistry,
      etaRotationSnapshots = etaRotationSnapshots,
      logger = logger
    )
    kesGate.flatMap { kesOk =>
      if (!kesOk)
        logger.warn(s"⚠️ KES gate dropped snapshot ord=${snap.ordinal} — no state mutation").as(())
      else
        processValidSnapshotInner(
          snap,
          signedSnapshot,
          context,
          becameBestTip,
          stateRef,
          chainStore,
          nodeStorage,
          tipTracker,
          sidecarClient,
          selfId,
          keyPair,
          lastKnownSlotRef,
          epochStateRef,
          etaRotationSnapshots,
          snapshotStorage,
          lastGlobalSnapshotStorage,
          lastNGlobalSnapshotStorage,
          productionGate,
          operationalKeyMaker,
          kesRegistry,
          logger
        )
    }
  }

  private def processValidSnapshotInner[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    snap: pb.Snapshot,
    signedSnapshot: Option[Signed[GlobalIncrementalSnapshot]],
    context: Option[GlobalSnapshotInfo],
    becameBestTip: Boolean,
    stateRef: Ref[F, SyncState],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    for {
      // Update network tip tracking
      _ <- stateRef.update { s =>
        if (snap.ordinal > s.networkTipOrdinal)
          s.copy(
            networkTipOrdinal = snap.ordinal,
            networkTipHash = Some(Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8)))
          )
        else s
      }

      // Canonical-state updates only when this snapshot became the bestTip in the chainStore.
      // After Phase E, the validate+store path no longer wraps MPT writes in a savepoint
      // bracket — accept() routes through the overlay algebra (Phase D, commit caa3559e),
      // so the MPT mutations for this snapshot are already in flight (Passthrough → base
      // immediately; MultiBranch → pending ChangeSet under the parent's branch). On the
      // fork-branch path we still must NOT update canonical storage; the overlay's
      // pending isolation (or the eventual finalizeBranch promotion) handles state.
      _ <- (signedSnapshot, context) match {
        case (Some(signed), Some(ctx)) if becameBestTip =>
          productionGate.pause(ProductionGate.ReorgInProgress) >>
            HasherSelector[F].withCurrent { implicit hasher =>
              signed.toHashed[F].flatMap { hashed =>
                snapshotStorage.setHeadForRecovery(signed, ctx) >>
                  lastGlobalSnapshotStorage.setForRecovery(hashed, ctx) >>
                  lastNGlobalSnapshotStorage.setForRecovery(hashed, ctx) >>
                  logger.debug(s"Updated canonical storage to ordinal=${snap.ordinal} slot=${snap.slot}") >>
                  Metrics[F].incrementCounter("dag_nakamoto_snapshots_received") >>
                  Metrics[F].updateGauge("dag_nakamoto_ordinal", snap.ordinal) >>
                  Metrics[F].recordDistribution("dag_nakamoto_slot_gap", (snap.slot - snap.parentSlot).toInt)
              }
            } >>
            chainStore.bestTipSlot.flatMap {
              case Some(bestSlot) => lastKnownSlotRef.set(Some(bestSlot))
              case None           => Async[F].unit
            } >>
            productionGate.resume(ProductionGate.ReorgInProgress)
        case _ => Async[F].unit
      }

      // Record in TipTracker (snapshot producer attests to their own tip).
      // `attestedAt` is OUR wall-clock receive time (epoch ms via Clock[F].realTime,
      // NOT System.currentTimeMillis()) — so TipTracker's "newer wins" rule sees
      // the latest arrival from each peer. The unit is wall-clock millis, not a
      // consensus slot — kept open as the future Ouroboros-Chronos timestamp
      // gossip surface, per `docs/nakamoto/attestation-and-finality.md`.
      tipHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
      tipSlot = Slot(NonNegLong.unsafeFrom(snap.slot))
      producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)
      producerId = peer.PeerId(producerHex)
      nowMs <- Clock[F].realTime.map(_.toMillis)
      att = DomainTipAttestation(tipHash, tipSlot, snap.ordinal, nowMs)
      // The producer-implicit attestation uses OUR local clock for `attestedAt`, so the
      // skew gate (`TipTracker.MaxAttestationSkewMs`) is trivially satisfied here.
      _ <- tipTracker.recordAttestation(producerId, att, nowMs)

      // Check if we should transition to Ready
      state <- stateRef.get
      nodeState <- nodeStorage.getNodeState
      _ <- Async[F].whenA(!state.isReady && nodeState =!= NodeState.Ready) {
        val caughtUp = state.networkTipOrdinal - snap.ordinal <= CatchUpThreshold
        Async[F].whenA(caughtUp) {
          logger.info(s"Caught up (local=${snap.ordinal}, network=${state.networkTipOrdinal}). → Ready.") >>
            stateRef.update(_.copy(isReady = true, localTipOrdinal = snap.ordinal)) >>
            nodeStorage.setNodeState(NodeState.Ready) >>
            logger.info(s"🟢 Node Ready — VRF production begins")
        }
      }

      // Emit OUR attestation only when chain-selection promoted this peer snapshot
      // to our local bestTip (a Phase 0 → 1 transition for us, per
      // `docs/nakamoto/attestation-and-finality.md` §0/§4). The earlier
      // unconditional emit (commit `6e49b7d5`) moved selfId.tipHash onto any
      // arriving fork-branch snap, which the canonical-hash filter in
      // `TipTracker.highestFinalizedOrdinal` then zeroed out — disenfranchising
      // us on our own canonical chain. The 5s re-attestation ticker
      // (`SnapshotLeaderLoop.scala:396-412`) remains as a safety net for the
      // case where bestTip flips between this branch and the ticker firing.
      //
      // The producer-implicit attestation above (recordAttestation for the
      // producer's `peerId`) stays unconditional — it credits the producer
      // for what they produced, independent of our chain selection.
      //
      // `attestedAt` is wall-clock epoch ms via `Clock[F].realTime` (NOT
      // `System.currentTimeMillis()`, NOT a consensus slot). Wall-clock
      // semantics are deliberate, keeping the field reusable as a future
      // Ouroboros-Chronos-style timestamp-claim surface.
      _ <- Async[F].whenA(becameBestTip) {
        Clock[F].realTime.map(_.toMillis).flatMap { attestedAt =>
          emitAttestation(
            snap,
            attestedAt,
            sidecarClient,
            tipTracker,
            selfId,
            keyPair,
            operationalKeyMaker,
            etaRotationSnapshots,
            logger
          )
        }
      }

      // §1.2 Slice 9: KES verification of the incoming snapshot's `kes_signature` field is
      // run BEFORE this body via `processValidSnapshot`'s outer `kesGate`. By the time we
      // reach here the gate has already passed (otherwise the snapshot was rejected and
      // this code is unreachable).

    } yield ()

  /** Store a fork-branch snapshot WITHOUT updating canonical state.
    *
    * The snapshot has valid VRF + signature + slot-cert but its content doesn't match our local state (it was built on a different fork).
    * We store it tentatively in the chain store. If ChainSelection picks this fork as denser (reorg), we trigger catch-up to adopt the new
    * state — this resets our canonical view to the fork's context and performs MPT self-healing, which is the deferred validation step.
    *
    * Stale fork branches are pruned when finality advances past them (chainStore.finalize).
    */
  private def storeForkBranch[F[
    _
  ]: Async: cats.Parallel: HasherSelector: io.constellationnetwork.json.JsonSerializer: Metrics: SecurityProvider](
    snap: pb.Snapshot,
    stateRef: Ref[F, SyncState],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    tipTracker: TipTracker[F],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    productionGate: ProductionGate[F],
    logger: org.typelevel.log4cats.Logger[F]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): F[Unit] =
    if (snap.payload.size() == 0) Async[F].unit
    else {
      val payloadStr = snap.payload.toByteArray.map(_.toChar).mkString
      val result = for {
        json <- io.circe.parser.parse(payloadStr)
        snapshotJson <- json.hcursor.get[io.circe.Json]("snapshot")
        contextJson <- json.hcursor.get[io.circe.Json]("context")
        snapshot <- snapshotJson.as[Signed[GlobalIncrementalSnapshot]]
        context <- contextJson.as[GlobalSnapshotInfo]
      } yield (snapshot, context)

      result match {
        case Right((signedSnapshot, context)) =>
          val parentHash = Hash(new String(snap.parentHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
          for {
            // Record the previous best tip so we can detect reorgs
            prevBestTip <- chainStore.bestTip.map(_.map(_.hash))

            // Store in chainStore — ChainSelection may switch bestTip if this fork is denser
            isNew <- chainStore.store(
              signedSnapshot,
              context,
              snap.ordinal,
              snap.slot,
              parentHash,
              vrfOutputFromProof(snap.vrfProof.toByteArray)
            )

            // Check if a reorg happened (bestTip changed to a different chain)
            newBestTip <- chainStore.bestTip.map(_.map(_.hash))
            reorgHappened = isNew && prevBestTip =!= newBestTip

            _ <-
              if (reorgHappened) {
                // ChainSelection picked this fork as denser — adopt its state via catch-up.
                // This is the deferred validation: catch-up resets canonical state to the fork's
                // context and performs MPT self-healing. Subsequent snapshots that build on the
                // new canonical tip will go through the normal Valid path (full content validation).
                logger.info(
                  s"🔄 Reorg to fork at ordinal=${snap.ordinal} slot=${snap.slot} (denser chain). Validating via catch-up."
                ) >>
                  productionGate.pause(ProductionGate.ReorgInProgress) >>
                  HasherSelector[F].withCurrent { implicit hasher =>
                    signedSnapshot.toHashed[F].flatMap { hashed =>
                      snapshotStorage.setTentativeHead(signedSnapshot, context) >>
                        lastGlobalSnapshotStorage.setForRecovery(hashed, context) >>
                        lastNGlobalSnapshotStorage.setForRecovery(hashed, context)
                    }
                  } >>
                  // MPT self-healing: rebuild from the fork's state.
                  //
                  // Before re-syncing the underlying base store from the fork's GlobalSnapshotInfo,
                  // we MUST clean up the overlay's pending/finalized refs. `syncFromGlobalSnapshotInfo`
                  // operates on the base `MptStore` directly (clears + repopulates) and bypasses the
                  // overlay entirely. Without this `finalizeBranch` call the overlay's `pendingRef`
                  // would still hold orphan local-fork branches whose deltas were built against the
                  // pre-reorg base — those branches show up in `bestTipsFn`, get marked protected by
                  // `walkAncestorsInPending`, and block `evictIfOverCap` from making progress
                  // (pendingRef grows unboundedly past cap, see #116 iter14 forensics).
                  //
                  // The reorg-replace `case None` arm in `MptOverlay.finalizeBranch` handles the
                  // expected case here ("canonical not in pending, locally-rejected fork branches
                  // still resident"): it clears `pendingRef`, resets `lastCommittedBranchRef`, and
                  // emits a WARN that the caller is responsible for base resync (which is exactly
                  // what we do next via `syncFromGlobalSnapshotInfo`).
                  HasherSelector[F].withCurrent { implicit hasher =>
                    signedSnapshot.toHashed[F].flatMap { hashed =>
                      mptOverlay
                        .finalizeBranch(
                          io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(hashed.hash),
                          SnapshotOrdinal.unsafeApply(snap.ordinal)
                        )
                        .void >>
                        mptStore.syncFromGlobalSnapshotInfo(context, SnapshotOrdinal.unsafeApply(snap.ordinal))
                    }
                  } >>
                  // Reconcile event mempool — evict stale DAG blocks, keep unconfirmed.
                  reconcileMempool(eventMempool, context, logger) >>
                  chainStore.bestTipSlot.flatMap {
                    case Some(bestSlot) => lastKnownSlotRef.set(Some(bestSlot))
                    case None           => Async[F].unit
                  } >>
                  productionGate.resume(ProductionGate.ReorgInProgress) >>
                  Metrics[F].incrementCounter("dag_nakamoto_reorgs")
              } else if (isNew) {
                // Fork stored but not canonical — just log
                Metrics[F].incrementCounter("dag_nakamoto_forks_stored")
              } else Async[F].unit

            // Record attestation regardless (producer attests their own tip). `attestedAt`
            // is OUR wall-clock receive time via Clock[F].realTime — same Chronos-prep
            // semantics as the becameBestTip-branch site above. Skew gate trivially
            // passes since `attestedAt` and `now` are both this node's local clock.
            tipHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
            tipSlot = Slot(NonNegLong.unsafeFrom(snap.slot))
            producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)
            producerId = peer.PeerId(producerHex)
            nowMs <- Clock[F].realTime.map(_.toMillis)
            att = DomainTipAttestation(tipHash, tipSlot, snap.ordinal, nowMs)
            _ <- tipTracker.recordAttestation(producerId, att, nowMs)
          } yield ()

        case Left(err) =>
          logger.warn(s"⚠️ Failed to deserialize fork-branch payload: ${err.getMessage}")
      }
    }

  private def handleAttestation[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    att: pb.TipAttestation,
    tipTracker: TipTracker[F],
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F],
    etaRotationSnapshots: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    // tipHash bytes are the UTF-8 encoding of the hex hash string — decode back to string
    val tipHash = Hash(new String(att.tipHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
    val tipSlot = Slot(NonNegLong.unsafeFrom(att.tipSlot))
    val attesterHex = Hex(att.attesterId.toByteArray.map("%02x".format(_)).mkString)
    val attesterId = peer.PeerId(attesterHex)
    val sigBytes = att.signature.toByteArray
    val kesSigBytes = att.kesSignature.toByteArray
    // `att.attestedAt` is wall-clock epoch ms set by the peer (Clock[F].realTime).
    // Used purely for "newer-wins" ordering in TipTracker; no slot interpretation.
    val domainAtt = DomainTipAttestation(tipHash, tipSlot, att.tipOrdinal, att.attestedAt)

    if (sigBytes.isEmpty) {
      logger.warn(s"⚠️ Rejecting unsigned attestation for ordinal=${att.tipOrdinal} from=${attesterHex.value.take(16)}...")
    } else {
      // Verify signature using the same Hasher pipeline: JSON-encode the domain TipAttestation → hash → verify
      HasherSelector[F].withCurrent { implicit hasher =>
        for {
          attHash <- domainAtt.hash
          publicKey <- attesterHex.toPublicKey[F]
          valid <- Signing.verifySignature(attHash.getBytes, sigBytes)(publicKey)
          // Chronos-prep: capture OUR local clock when the peer's attestation lands so
          // `TipTracker.recordAttestation` can compare against the peer's claimed
          // `attestedAt`. Badly-skewed peers (or attackers forging timestamps) are
          // dropped inside the tracker before they pollute `T_count` finality (#136).
          nowMs <- Clock[F].realTime.map(_.toMillis)
          _ <-
            if (valid)
              // §1.2 Slice 9: KES verification runs BEFORE TipTracker.recordAttestation so we
              // can drop the attestation without un-recording it. Returns false on no-sig /
              // decode-fail / verify-fail / step-out-of-range; the no-registry-entry carve-out
              // returns true so Ed25519-authenticated peers that haven't registered yet
              // (Slice 10 mid-life joiners) aren't silently dropped.
              KesGossipVerification
                .verifyAttestation(
                  messageBytes = attHash.getBytes,
                  kesSigBytes = kesSigBytes,
                  attesterId = attesterId,
                  attesterHex = attesterHex,
                  tipOrdinal = att.tipOrdinal,
                  kesRegistry = kesRegistry,
                  etaRotationSnapshots = etaRotationSnapshots,
                  logger = logger
                )
                .flatMap { kesOk =>
                  if (kesOk)
                    tipTracker.recordAttestation(attesterId, domainAtt, nowMs) >>
                      logger.info(
                        s"📨 Attestation for ordinal=${att.tipOrdinal} from=${attesterHex.value.take(16)}..."
                      )
                  else
                    Async[F].unit // rejection already logged inside verifyAttestation
                }
            else
              logger.warn(
                s"⚠️ Rejecting attestation with invalid signature for ordinal=${att.tipOrdinal} from=${attesterHex.value.take(16)}..."
              )
        } yield ()
      }
    }
  }

  /** Route an incoming AllowSpendBlock from gossip into the same `l1AllowSpendOutput` queue the (now-removed) HTTP POST endpoint populated.
    * Sender serializes `Signed[AllowSpendBlock]` via JsonSerializer in `Swap.sendBlockToL0`; we use the same typeclass to deserialize.
    * Errors (decode failure) are logged and swallowed — gossip is fire-and-forget. (#196)
    */
  private def handleAllowSpendBlock[F[_]: Async: io.constellationnetwork.json.JsonSerializer](
    asb: pb.AllowSpendBlock,
    enqueue: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.swap.AllowSpendBlock
    ] => F[Unit],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.swap.AllowSpendBlock
    import io.constellationnetwork.security.signature.Signed

    val bytes = asb.payload.toByteArray
    io.constellationnetwork.json
      .JsonSerializer[F]
      .deserialize[Signed[AllowSpendBlock]](bytes)
      .flatMap {
        case Left(err) =>
          logger.warn(s"⚠️ Rejecting allow-spend-block gossip: decode failed (${err.getMessage})")
        case Right(signed) =>
          enqueue(signed)
            .handleErrorWith(e => logger.warn(s"⚠️ enqueueAllowSpendBlock failed: ${e.getMessage}"))
      }
  }

  /** Route an incoming state channel binary from gossip into the same acceptance pipeline as the HTTP POST endpoint. The sender serialized
    * Signed[StateChannelSnapshotBinary] via the project's JsonSerializer (JSON + Brotli); we use the same typeclass to deserialize. Errors
    * (decode failure, address parse failure) are logged and swallowed — gossip is fire-and-forget with no sender to reply to.
    */
  private def handleMetagraphBinary[F[_]: Async: io.constellationnetwork.json.JsonSerializer](
    mb: pb.MetagraphBinary,
    processMetagraphBinary: io.constellationnetwork.statechannel.StateChannelOutput => F[Unit],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
    import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary}
    import io.constellationnetwork.security.signature.Signed
    import eu.timepit.refined.refineV

    refineV[DAGAddressRefined](mb.address) match {
      case Left(err) =>
        logger.warn(s"⚠️ Rejecting metagraph-binary gossip: invalid address '${mb.address}' ($err)")
      case Right(refined) =>
        val address = Address(refined)
        val bytes = mb.binary.toByteArray
        io.constellationnetwork.json
          .JsonSerializer[F]
          .deserialize[Signed[StateChannelSnapshotBinary]](bytes)
          .flatMap {
            case Left(err) =>
              logger.warn(s"⚠️ Rejecting metagraph-binary gossip: decode failed for $address (${err.getMessage})")
            case Right(signed) =>
              val output = StateChannelOutput(address, signed)
              processMetagraphBinary(output)
                .handleErrorWith(e => logger.warn(s"⚠️ processMetagraphBinary failed for $address: ${e.getMessage}"))
          }
    }
  }

  // Package-visible so SnapshotLeaderLoop can route producer-self-attestation through the
  // same code path peer-received snapshots use. Unifies the two attestation sites: any future
  // gating, signing semantics, or broadcast policy applies uniformly.
  //
  // `attestedAt` is **wall-clock epoch milliseconds** sourced via `Clock[F].realTime` by the
  // caller (NEVER `System.currentTimeMillis()`). It is NOT a consensus slot. Wall-clock
  // semantics here are deliberate — keeps the field reusable as a future Ouroboros-Chronos-style
  // timestamp-claim surface (gossiping `attestedAt` values lets the network distill a consensus
  // time without needing NTP). Today the value is only used by `TipTracker.recordAttestation`
  // for the "newer wins" rule, which compares Longs.
  def emitAttestation[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    snap: pb.Snapshot,
    attestedAt: Long,
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    tipTracker: TipTracker[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    etaRotationSnapshots: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    // snap.hash bytes are the UTF-8 encoding of the hex hash string — decode back to string
    val tipHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
    val tipSlot = Slot(NonNegLong.unsafeFrom(snap.slot))
    emitTipAttestation(
      tipHash,
      tipSlot,
      snap.ordinal,
      attestedAt,
      sidecarClient,
      tipTracker,
      selfId,
      keyPair,
      operationalKeyMaker,
      etaRotationSnapshots,
      logger
    )
  }

  // Primitive variant for callers that have tip hash/slot/ordinal directly (e.g. the finality
  // monitor's bestTip-change ticker re-attesting after chainSelection moves).
  // See `emitAttestation` comment re: `attestedAt` semantics — wall-clock epoch ms via
  // `Clock[F].realTime`, sourced by the caller; this function does not read the clock.
  def emitTipAttestation[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    tipHash: Hash,
    tipSlot: Slot,
    tipOrdinal: Long,
    attestedAt: Long,
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    tipTracker: TipTracker[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    etaRotationSnapshots: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    // Record locally first (so our own TipTracker sees it). The caller already sourced
    // `attestedAt` from `Clock[F].realTime`, so passing the same value as `now` makes
    // the skew gate (`TipTracker.MaxAttestationSkewMs`) trivially pass on self-emit.
    val localAtt = DomainTipAttestation(tipHash, tipSlot, tipOrdinal, attestedAt)
    tipTracker.recordAttestation(selfId, localAtt, attestedAt) >>
      // Sign via the standard Hasher pipeline: JSON-encode the domain TipAttestation → hash → sign the hash.
      // Same path as SignatureProof.fromData / Signed.forAsyncHasher — no custom serialization.
      HasherSelector[F].withCurrent { implicit hasher =>
        for {
          attHash <- localAtt.hash
          sig <- Signature.fromHash[F](keyPair.getPrivate, attHash)
          sigBytes = sig.coerce.toBytes
          // §1.2 Slice 5: parallel-sign the attestation hash with KES. Period is derived from
          // the tip's ordinal, NOT slot — slots are LDD-paced and lumpy. The receiver re-derives
          // the same period from `tipOrdinal` in the wire message so verification stays
          // self-consistent without any extra wire field.
          //
          // Sender failure (signAt returns Left) → empty wire field, receiver treats as no-sig.
          // Don't drop the attestation. Period eta-aligned evolution lands in Slice 8; this slice
          // signs at the period without any pre-evolve step.
          kesPeriod = EtaCalculation.rotationPeriod(tipOrdinal, etaRotationSnapshots).toInt
          kesAttempt <- operationalKeyMaker.signAt(kesPeriod, attHash.value.getBytes(java.nio.charset.StandardCharsets.UTF_8))
          kesSigBytes <- kesAttempt match {
            case Right(kSig) =>
              val bytes = io.constellationnetwork.security.kes.OperationalKeyMaker.encodeSignature(kSig)
              Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_signed_total") >>
                logger
                  .info(
                    s"🔐 KES-ATT ord=$tipOrdinal period=$kesPeriod sub-sig=${bytes.take(8).map("%02x".format(_)).mkString} (${bytes.length}B)"
                  )
                  .as(bytes)
            case Left(err) =>
              Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_sign_failed_total") >>
                logger
                  .warn(s"⚠️ KES-ATT sign failed for ord=$tipOrdinal period=$kesPeriod: ${err.message} — emitting unsigned wire field")
                  .as(Array.empty[Byte])
          }
          att = SidecarClient.mkAttestation(
            tipHash = tipHash.value.getBytes(java.nio.charset.StandardCharsets.UTF_8),
            tipSlot = tipSlot.value.value,
            tipOrdinal = tipOrdinal,
            attestedAt = attestedAt,
            attesterId = selfId.value.toBytes,
            signature = sigBytes,
            kesSignature = kesSigBytes
          )
          _ <- sidecarClient.publishAttestation(att).void.handleErrorWith { e =>
            logger.warn(s"Failed to emit attestation: ${e.getMessage}")
          }
        } yield ()
      }
  }

  /** Catch up from a gossip payload when parent is missing (restart scenario).
    *
    * Instead of rejecting the snapshot, use it to reset local state to the network tip. The gossip message already contains the full
    * Signed[GlobalIncrementalSnapshot] + GlobalSnapshotInfo. We skip validation (can't validate without parent) but reset our canonical
    * storage so subsequent gossip messages WILL have parents we recognize.
    */
  private def catchUpFromGossip[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector: Metrics](
    snap: pb.Snapshot,
    parsed: Option[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)],
    stateRef: Ref[F, SyncState],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    mptStore: MptStore[F, GlobalStateKey],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    productionGate: ProductionGate[F],
    channel: ManagedChannel,
    dataDir: java.nio.file.Path,
    logger: org.typelevel.log4cats.Logger[F]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit,
    supervisor: Supervisor[F]
  ): F[Unit] = {
    val now = System.currentTimeMillis()
    stateRef.get.flatMap { state =>
      if (now - state.lastCatchUpAttemptMs < CatchUpCooldownMs) {
        // Cooldown — don't spam catch-up attempts
        logger.debug(
          s"⏳ Catch-up cooldown (${(CatchUpCooldownMs - (now - state.lastCatchUpAttemptMs)) / 1000}s remaining), " +
            s"skipping ordinal=${snap.ordinal}"
        )
      } else {
        parsed match {
          case Some((signedSnapshot, context)) =>
            val parentHash = Hash(new String(snap.parentHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
            for {
              _ <- stateRef.update(_.copy(lastCatchUpAttemptMs = now))
              _ <- logger.warn(
                s"\uD83D\uDD04 CATCH-UP: No parent found for ordinal=${snap.ordinal} slot=${snap.slot}. " +
                  s"Resetting local state to network tip."
              )
              _ <- productionGate.pause("catch-up-sync")

              // Store in chain store (seed this snapshot as our new starting point)
              _ <- chainStore.store(
                signedSnapshot,
                context,
                snap.ordinal,
                snap.slot,
                parentHash,
                vrfOutputFromProof(snap.vrfProof.toByteArray)
              )

              // Update ALL canonical storages — snapshotStorage is what SnapshotLeaderLoop
              // reads for its parent. Without this, the leader loop produces at the OLD ordinal
              // after catch-up, causing the node to fall behind again immediately.
              _ <- HasherSelector[F].withCurrent { implicit hasher =>
                snapshotStorage.setHeadForRecovery(signedSnapshot, context) >>
                  signedSnapshot.toHashed[F].flatMap { hashed =>
                    lastGlobalSnapshotStorage.setForRecovery(hashed, context) >>
                      lastNGlobalSnapshotStorage.setForRecovery(hashed, context)
                  }
              }

              // MPT full sync from the context we received — critical for
              // the acceptance manager to validate subsequent snapshots
              _ <- logger.info(s"\uD83D\uDD04 CATCH-UP: Syncing MPT from received context...")
              _ <- HasherSelector[F].withCurrent { implicit hasher =>
                mptStore.syncFromGlobalSnapshotInfo(context, SnapshotOrdinal(NonNegLong.unsafeFrom(snap.ordinal)))
              }
              // Don't overwrite lastKnownSlotRef with the gossip slot — it may be
              // ahead of our wall clock, making slotGap negative and blocking VRF
              // eligibility. Production will update it after its next successful store.

              // Reconcile event mempool — evict DAG blocks whose transactions are
              // already consumed in the new context's lastTxRefs (prevents double-spend).
              // Keep events whose transactions are still unconfirmed (prevents starvation).
              _ <- reconcileMempool(eventMempool, context, logger)

              _ <- stateRef.update(
                _.copy(
                  networkTipOrdinal = snap.ordinal,
                  networkTipHash = Some(Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))),
                  localTipOrdinal = snap.ordinal
                )
              )

              _ <- productionGate.resume("catch-up-sync")
              _ <- Metrics[F].incrementCounter("dag_nakamoto_catchups")
              _ <- logger.info(
                s"\u2705 CATCH-UP complete: now at ordinal=${snap.ordinal} slot=${snap.slot}. " +
                  s"Subsequent gossip should find parents."
              )

              // Start backfill daemon in background to fill the gap from caught-up tip down to genesis.
              // The parent hash of the caught-up snapshot is the starting point for the walk-back.
              _ <- {
                val cursor = BackfillDaemon.BackfillCursor(
                  nextHashToFetch = parentHash.value,
                  targetOrdinal = 1L,
                  currentOrdinal = snap.ordinal,
                  startedAtOrdinal = snap.ordinal,
                  completedChunks = Set.empty,
                  createdAtMs = System.currentTimeMillis()
                )
                // Scope the backfill fiber to the app Supervisor so shutdown cancels cleanly.
                // Previously a raw Async.start leaked a fiber that outlived the owning daemon.
                supervisor
                  .supervise(
                    BackfillDaemon
                      .run[F](cursor, channel, snapshotStorage, productionGate, dataDir)
                      .handleErrorWith(e => logger.warn(s"Backfill daemon failed: ${e.getMessage}"))
                  )
                  .void
              }
            } yield ()
          case None =>
            logger.warn(
              s"\u274c No parent found for ordinal=${snap.ordinal} and no payload to catch up from"
            )
        }
      }
    }
  }
}
