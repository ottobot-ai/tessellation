package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{StakeRegistry, TipTracker}
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.{GossipStream, SidecarClient}
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{LddConfig, TipAttestation => DomainTipAttestation}
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, peer}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.{HasherSelector, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import io.grpc.ManagedChannel
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Subscribes to sidecar GossipSub for live snapshots and attestations.
  *
  * Uses NakamotoChainStore for fork-aware storage instead of raw SnapshotStorage.prepend. Records attestations in TipTracker for finality
  * tracking.
  */
object NakamotoSyncDaemon {

  private val CatchUpThreshold = 2L

  final case class SyncState(
    networkTipOrdinal: Long,
    networkTipHash: Option[Hash],
    localTipOrdinal: Long,
    isReady: Boolean
  )

  object SyncState {
    def initial: SyncState = SyncState(0L, None, 0L, isReady = false)
  }

  def run[F[_]: Async: SecurityProvider: HasherSelector](
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
    etaRotationSlots: Long
  ): fs2.Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("NakamotoSyncDaemon")

    fs2.Stream.eval(Ref.of[F, SyncState](SyncState.initial)).flatMap { stateRef =>
      GossipStream.subscribe[F](channel).evalMap { msg =>
        msg.body match {
          case pb.GossipMessage.Body.Snapshot(snap) =>
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
              logger
            )

          case pb.GossipMessage.Body.Attestation(att) =>
            handleAttestation(att, tipTracker, logger)

          case pb.GossipMessage.Body.Empty =>
            Async[F].unit
        }
      }
    }
  }

  private def handleSnapshot[F[_]: Async: HasherSelector](
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
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    for {
      _ <- logger.info(
        s"📥 Received snapshot ordinal=${snap.ordinal} slot=${snap.slot} from=${snap.producerId.toByteArray.take(4).map("%02x".format(_)).mkString}"
      )

      // ── VRF Proof Validation ──
      // Verify the producer was legitimately elected for this slot
      producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)
      producerId = peer.PeerId(producerHex)
      producerStake <- stakeRegistry.relativeStake(producerId)
      epochState <- epochStateRef.get
      lastSlot <- lastKnownSlotRef.get
      slotGap = lastSlot.fold(snap.slot)(snap.slot - _)

      vrfValid = {
        val vrfVK = snap.vrfPublicKey.toByteArray
        val proof = snap.vrfProof.toByteArray
        if (vrfVK.isEmpty || proof.isEmpty) {
          false // Missing VRF data
        } else {
          io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker.verifyEligibility(
            vrfVK = vrfVK,
            slot = Slot(NonNegLong.unsafeFrom(snap.slot)),
            slotGap = slotGap,
            eta = epochState.currentEta,
            relativeStake = producerStake,
            config = lddConfig,
            proof = proof
          )
        }
      }

      _ <-
        if (!vrfValid) {
          logger.warn(
            s"❌ REJECTED snapshot slot=${snap.slot} ordinal=${snap.ordinal} from=${producerHex.value
                .take(8)} — VRF verification failed (stake=$producerStake, gap=$slotGap)"
          )
        } else {
          logger.debug(s"✅ VRF valid for slot=${snap.slot} from=${producerHex.value.take(8)}")
        }

      // Skip storage + attestation if VRF is invalid
      _ <- Async[F].whenA(vrfValid) {
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
          logger
        )
      }
    } yield ()

  /** Process a VRF-validated snapshot: update tip tracking, store, accumulate VRF output, record attestation. */
  private def processValidSnapshot[F[_]: Async: HasherSelector](
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
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    for {
      // Update network tip tracking
      _ <- stateRef.update { s =>
        if (snap.ordinal > s.networkTipOrdinal)
          s.copy(
            networkTipOrdinal = snap.ordinal,
            networkTipHash = Some(Hash(snap.hash.toByteArray.map("%02x".format(_)).mkString))
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
              val parentHash = Hash(snap.parentHash.toByteArray.map("%02x".format(_)).mkString)
              chainStore
                .store(
                  signedSnapshot,
                  context,
                  snap.ordinal,
                  snap.slot,
                  parentHash,
                  snap.vrfProof.toByteArray
                )
                .flatMap { isNew =>
                  if (isNew) {
                    // Update slot ref from chain store's best tip (may differ from this snapshot if fork was weaker)
                    chainStore.bestTipSlot.flatMap {
                      case Some(bestSlot) => lastKnownSlotRef.set(Some(bestSlot))
                      case None           => Async[F].unit
                    }
                  } else Async[F].unit
                }
            case Left(err) =>
              logger.warn(s"⚠️ Failed to deserialize snapshot payload: ${err.getMessage}")
          }
        } else Async[F].unit

      // Accumulate VRF output from received snapshot for epoch rotation
      // This ensures ALL nodes rotate eta uniformly, not just producers
      _ <- {
        val vrfProofBytes = snap.vrfProof.toByteArray
        val vrf = new io.constellationnetwork.security.vrf.EcVrf25519()
        vrf.vrfProofToHash(vrfProofBytes) match {
          case Some(vrfOutput) =>
            epochStateRef.update(SharedEpochState.accumulate(_, vrfOutput, snap.slot, etaRotationSlots))
          case None =>
            logger.warn(s"⚠️ Failed to extract VRF output from proof for slot=${snap.slot}")
        }
      }

      // Record in TipTracker (snapshot producer attests to their own tip)
      tipHash = Hash(snap.hash.toByteArray.map("%02x".format(_)).mkString)
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
          logger.info(s"✅ Caught up (local=${snap.ordinal}, network=${state.networkTipOrdinal}). → Ready.") >>
            stateRef.update(_.copy(isReady = true, localTipOrdinal = snap.ordinal)) >>
            nodeStorage.setNodeState(NodeState.Ready) >>
            logger.info(s"🟢 Node Ready — VRF production begins")
        }
      }

      // Emit our attestation for this snapshot
      _ <- emitAttestation(snap, sidecarClient, tipTracker, selfId, logger)

    } yield ()

  private def handleAttestation[F[_]: Async](
    att: pb.TipAttestation,
    tipTracker: TipTracker[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    val tipHash = Hash(att.tipHash.toByteArray.map("%02x".format(_)).mkString)
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
    val tipHash = Hash(snap.hash.toByteArray.map("%02x".format(_)).mkString)
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
}
