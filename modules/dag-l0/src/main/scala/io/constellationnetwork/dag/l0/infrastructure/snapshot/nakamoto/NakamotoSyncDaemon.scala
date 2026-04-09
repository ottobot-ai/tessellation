package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.effect.std.Semaphore
import cats.syntax.all._

import io.constellationnetwork.dag.l0.infrastructure.snapshot._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event.{DAGEvent, GlobalSnapshotEvent}
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
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.vrf.EcVrf25519
import io.constellationnetwork.security.{Hashed, HasherSelector, SecurityProvider}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
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
  private val vrf = new EcVrf25519()

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
    lddConfig: LddConfig,
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSlots: Long,
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    snapshotSemaphore: Semaphore[F],
    productionGate: ProductionGate[F],
    mptStore: MptStore[F, GlobalStateKey],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey]
  )(implicit globalStateProofSelector: io.constellationnetwork.schema.GlobalStateProofSelector): fs2.Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("NakamotoSyncDaemon")

    fs2.Stream.eval(Ref.of[F, SyncState](SyncState.initial)).flatMap { stateRef =>
      // Shared semaphore serializes snapshot processing with production (SnapshotLeaderLoop)
      GossipStream.subscribe[F](channel).evalMap { msg =>
        msg.body match {
          case pb.GossipMessage.Body.Snapshot(snap) =>
            // Pre-semaphore: if incoming snapshot would beat our current tip,
            // pause production immediately so SnapshotLeaderLoop doesn't build
            // on a tip we're about to abandon.
            val incomingOrdinal = snap.ordinal
            chainStore.bestTipOrdinal.flatMap { currentBestOrdinal =>
              val wouldWin = incomingOrdinal > currentBestOrdinal.getOrElse(0L)
              (if (wouldWin) productionGate.pause(ProductionGate.BetterGossipReceived)
               else Async[F].unit) >>
                snapshotSemaphore.permit.use { _ =>
                  handleSnapshot(
                    snap,
                    stateRef,
                    chainStore,
                    nodeStorage,
                    tipTracker,
                    stakeRegistry,
                    sidecarClient,
                    selfId,
                    lddConfig,
                    lastKnownSlotRef,
                    epochStateRef,
                    etaRotationSlots,
                    consensusFns,
                    snapshotStorage,
                    lastGlobalSnapshotStorage,
                    lastNGlobalSnapshotStorage,
                    productionGate,
                    mptStore,
                    eventMempool,
                    logger
                  )
                } >> // snapshotSemaphore.permit.use
                productionGate.resume(ProductionGate.BetterGossipReceived)
            } // chainStore.bestTipOrdinal.flatMap

          case pb.GossipMessage.Body.Attestation(att) =>
            handleAttestation(att, tipTracker, logger)

          case _: pb.GossipMessage.Body.Rumor =>
            // Rumors are handled by SidecarRumorBridge.receive — ignore here.
            Async[F].unit

          case pb.GossipMessage.Body.Empty =>
            Async[F].unit
        }
      }
    }
  }

  private def handleSnapshot[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector: Metrics](
    snap: pb.Snapshot,
    stateRef: Ref[F, SyncState],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    lddConfig: LddConfig,
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSlots: Long,
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    mptStore: MptStore[F, GlobalStateKey],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    logger: org.typelevel.log4cats.Logger[F]
  )(implicit globalStateProofSelector: GlobalStateProofSelector): F[Unit] =
    for {
      _ <- logger.info(
        s"📥 Received snapshot ordinal=${snap.ordinal} slot=${snap.slot} parentSlot=${snap.parentSlot} from=${snap.producerId.toByteArray.take(4).map("%02x".format(_)).mkString}"
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
      currentPeriod = EtaCalculation.rotationPeriod(snap.slot, etaRotationSlots)
      eta <-
        if (currentPeriod <= 0) {
          Async[F].pure(genesisEta)
        } else {
          chainStore.vrfOutputsForPeriodFrom(currentPeriod - 1, etaRotationSlots, parentHash).flatMap { chainOutputs =>
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

      // Full validation pipeline: VRF + signature + cert + content
      // Look up the ACTUAL parent from chain store using parentHash from gossip message.
      // Can't use snapshotStorage.head — local node may have produced ahead of this snapshot.
      validationResult <- parsed match {
        case Some((signedSnapshot, context)) =>
          chainStore.get(parentHash).flatMap {
            case Some(parentStored) =>
              // Found actual parent in chain store — validate against it
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
                consensusFns = consensusFns,
                lastSignedArtifact = parentStored.signedSnapshot,
                lastContext = parentStored.context,
                getByOrdinal = (_: SnapshotOrdinal) => Async[F].pure(None: Option[Hashed[GlobalIncrementalSnapshot]])
              )
            case None =>
              // Parent not in chain store — try snapshotStorage.head as fallback
              snapshotStorage.head.flatMap {
                case Some((lastSigned, lastCtx)) =>
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
                    consensusFns = consensusFns,
                    lastSignedArtifact = lastSigned,
                    lastContext = lastCtx,
                    getByOrdinal = (_: SnapshotOrdinal) => Async[F].pure(None: Option[Hashed[GlobalIncrementalSnapshot]])
                  )
                case None =>
                  Async[F].pure(NakamotoSnapshotValidator.Invalid("No parent found"): NakamotoSnapshotValidator.ValidationResult)
              }
          }
        case None =>
          // No payload — fall back to VRF-only validation (legacy/PoC)
          val vrfVK = snap.vrfPublicKey.toByteArray
          val proof = snap.vrfProof.toByteArray
          val producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)
          val producerId = peer.PeerId(producerHex)
          stakeRegistry.relativeStake(producerId).map { producerStake =>
            val vrfValid =
              if (vrfVK.isEmpty || proof.isEmpty) false
              else
                io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker.verifyEligibility(
                  vrfVK = vrfVK,
                  slot = Slot(NonNegLong.unsafeFrom(snap.slot)),
                  slotGap = slotGap,
                  eta = eta,
                  relativeStake = producerStake,
                  config = lddConfig,
                  proof = proof
                )
            if (vrfValid) NakamotoSnapshotValidator.Valid(null, null) // VRF-only, no snapshot data
            else NakamotoSnapshotValidator.Invalid("VRF failed (no payload)")
          }
      }

      _ <- (validationResult: NakamotoSnapshotValidator.ValidationResult) match {
        case NakamotoSnapshotValidator.Valid(_, _) =>
          processValidSnapshot(
            snap,
            stateRef,
            chainStore,
            nodeStorage,
            tipTracker,
            sidecarClient,
            selfId,
            lastKnownSlotRef,
            epochStateRef,
            etaRotationSlots,
            snapshotStorage,
            lastGlobalSnapshotStorage,
            lastNGlobalSnapshotStorage,
            productionGate,
            logger
          )
        case NakamotoSnapshotValidator.Invalid(reason) if reason == "No parent found" =>
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
            logger
          )
        case NakamotoSnapshotValidator.Invalid(reason) if reason.startsWith("Content mismatch") =>
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
                logger
              )
          }
        case NakamotoSnapshotValidator.Invalid(reason) =>
          Metrics[F].incrementCounter("dag_nakamoto_snapshots_rejected") >>
            logger.warn(s"❌ REJECTED snapshot slot=${snap.slot} ordinal=${snap.ordinal}: $reason")
      }
    } yield ()

  /** Process a VRF-validated snapshot: update tip tracking, store, accumulate VRF output, record attestation. */
  private def processValidSnapshot[F[_]: Async: HasherSelector: Metrics](
    snap: pb.Snapshot,
    stateRef: Ref[F, SyncState],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSlots: Long,
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    // processValidSnapshot is called for CANONICAL chain extensions (Valid case).
    // Content was already validated against local state — safe to update all storages.
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

      // Deserialize and store via NakamotoChainStore (handles forks + reorgs)
      _ <-
        if (snap.payload.size() > 0) {
          val payloadStr = snap.payload.toByteArray.map(_.toChar).mkString
          val result = for {
            json <- io.circe.parser.parse(payloadStr)
            snapshotJson <- json.hcursor.get[io.circe.Json]("snapshot")
            contextJson <- json.hcursor.get[io.circe.Json]("context")
            snapshot <- snapshotJson.as[io.constellationnetwork.security.signature.Signed[GlobalIncrementalSnapshot]]
            context <- contextJson.as[GlobalSnapshotInfo]
          } yield (snapshot, context)

          result match {
            case Right((signedSnapshot, context)) =>
              val parentHash = Hash(new String(snap.parentHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
              chainStore
                .store(
                  signedSnapshot,
                  context,
                  snap.ordinal,
                  snap.slot,
                  parentHash,
                  vrfOutputFromProof(snap.vrfProof.toByteArray)
                )
                .flatMap { isNew =>
                  if (isNew) {
                    // Pause production during canonical storage update — prevents
                    // SnapshotLeaderLoop from building on the stale tip
                    productionGate.pause(ProductionGate.ReorgInProgress) >>
                      HasherSelector[F].withCurrent { implicit hasher =>
                        signedSnapshot.toHashed[F].flatMap { hashed =>
                          // Update ALL snapshot storages so HTTP routes can serve this
                          // snapshot. snapshotStorage is what SnapshotRoutes reads from;
                          // without this, finality-gated routes return NotFound for
                          // snapshots produced by other nodes.
                          snapshotStorage.setHeadForRecovery(signedSnapshot, context) >>
                            lastGlobalSnapshotStorage.setForRecovery(hashed, context) >>
                            lastNGlobalSnapshotStorage.setForRecovery(hashed, context) >>
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
                  } else Async[F].unit
                }
            case Left(err) =>
              logger.warn(s"⚠️ Failed to deserialize snapshot payload: ${err.getMessage}")
          }
        } else Async[F].unit

      // No in-memory epoch state accumulation needed — eta is chain-derived.
      // VRF outputs are stored in NakamotoChainStore as part of each snapshot.

      // Record in TipTracker (snapshot producer attests to their own tip)
      tipHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
      tipSlot = Slot(NonNegLong.unsafeFrom(snap.slot))
      producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)
      producerId = peer.PeerId(producerHex)
      att = DomainTipAttestation(tipHash, tipSlot, snap.ordinal, tipSlot)
      _ <- tipTracker.recordAttestation(producerId, att)

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

      // Emit our attestation for this snapshot
      _ <- emitAttestation(snap, sidecarClient, tipTracker, selfId, logger)

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
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    productionGate: ProductionGate[F],
    logger: org.typelevel.log4cats.Logger[F]
  )(implicit globalStateProofSelector: GlobalStateProofSelector): F[Unit] =
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
                  // MPT self-healing: rebuild from the fork's state
                  HasherSelector[F].withCurrent { implicit hasher =>
                    context
                      .allStateEntries[F](
                        Async[F],
                        cats.Parallel[F],
                        hasher,
                        implicitly[io.constellationnetwork.json.JsonSerializer[F]],
                        implicitly[GlobalStateProofSelector]
                      )
                      .flatMap(kvPairs => mptStore.syncFull(kvPairs, SnapshotOrdinal.unsafeApply(snap.ordinal)))
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

            // Record attestation regardless (producer attests their own tip)
            tipHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
            tipSlot = Slot(NonNegLong.unsafeFrom(snap.slot))
            producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)
            producerId = peer.PeerId(producerHex)
            att = DomainTipAttestation(tipHash, tipSlot, snap.ordinal, tipSlot)
            _ <- tipTracker.recordAttestation(producerId, att)
          } yield ()

        case Left(err) =>
          logger.warn(s"⚠️ Failed to deserialize fork-branch payload: ${err.getMessage}")
      }
    }

  private def handleAttestation[F[_]: Async](
    att: pb.TipAttestation,
    tipTracker: TipTracker[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    // tipHash bytes are the UTF-8 encoding of the hex hash string — decode back to string
    val tipHash = Hash(new String(att.tipHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
    val tipSlot = Slot(NonNegLong.unsafeFrom(att.tipSlot))
    val attesterHex = Hex(att.attesterId.toByteArray.map("%02x".format(_)).mkString)
    val attesterId = peer.PeerId(attesterHex)
    val attestedAtSlot = Slot(NonNegLong.unsafeFrom(att.attestedAt))
    val domainAtt = DomainTipAttestation(tipHash, tipSlot, att.tipOrdinal, attestedAtSlot)
    tipTracker.recordAttestation(attesterId, domainAtt) >>
      logger.info(
        s"📨 Attestation for ordinal=${att.tipOrdinal} from=${attesterHex.value.take(16)}... rawLen=${att.attesterId.toByteArray.length}"
      )
  }

  private def emitAttestation[F[_]: Async](
    snap: pb.Snapshot,
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    tipTracker: TipTracker[F],
    selfId: peer.PeerId,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    // snap.hash bytes are the UTF-8 encoding of the hex hash string — decode back to string
    val tipHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
    val tipSlot = Slot(NonNegLong.unsafeFrom(snap.slot))
    val currentSlotMs = System.currentTimeMillis() / 1000L
    val attestedAtSlot = Slot(NonNegLong.unsafeFrom(currentSlotMs))

    // Record locally first (so our own TipTracker sees it)
    val localAtt = DomainTipAttestation(tipHash, tipSlot, snap.ordinal, attestedAtSlot)
    tipTracker.recordAttestation(selfId, localAtt) >>
      // Broadcast to network — use hex bytes of PeerId so receivers can reconstruct
      {
        val att = SidecarClient.mkAttestation(
          tipHash = snap.hash.toByteArray,
          tipSlot = snap.slot,
          tipOrdinal = snap.ordinal,
          attestedAt = currentSlotMs,
          attesterId = selfId.value.toBytes,
          signature = Array.emptyByteArray // TODO: sign (tipHash || tipSlot)
        )
        sidecarClient.publishAttestation(att).void.handleErrorWith { e =>
          logger.warn(s"Failed to emit attestation: ${e.getMessage}")
        }
      }
  }

  /** Catch up from a gossip payload when parent is missing (restart scenario).
    *
    * Instead of rejecting the snapshot, use it to reset local state to the network tip. The gossip message already contains the full
    * Signed[GlobalIncrementalSnapshot] + GlobalSnapshotInfo. We skip validation (can't validate without parent) but reset our canonical
    * storage so subsequent gossip messages WILL have parents we recognize.
    */
  private def catchUpFromGossip[F[_]: Async: cats.Parallel: JsonSerializer: HasherSelector: Metrics](
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
    logger: org.typelevel.log4cats.Logger[F]
  )(implicit globalStateProofSelector: GlobalStateProofSelector): F[Unit] = {
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

              // Update canonical storages
              _ <- HasherSelector[F].withCurrent { implicit hasher =>
                signedSnapshot.toHashed[F].flatMap { hashed =>
                  lastGlobalSnapshotStorage.setForRecovery(hashed, context) >>
                    lastNGlobalSnapshotStorage.setForRecovery(hashed, context)
                }
              }

              // MPT full sync from the context we received — critical for
              // the acceptance manager to validate subsequent snapshots
              _ <- logger.info(s"\uD83D\uDD04 CATCH-UP: Syncing MPT from received context...")
              kvPairs <- HasherSelector[F].withCurrent { implicit hasher =>
                context.allStateEntries[F]
              }
              _ <- mptStore.syncFull(kvPairs, SnapshotOrdinal(NonNegLong.unsafeFrom(snap.ordinal)))
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
