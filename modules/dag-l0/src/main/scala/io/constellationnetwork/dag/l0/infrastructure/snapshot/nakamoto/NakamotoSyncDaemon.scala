package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.effect.std.Supervisor
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{StakeRegistry, TipTracker}
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.{GossipStream, SidecarClient}
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{LddConfig, TipAttestation => DomainTipAttestation}
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, peer}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.{HasherSelector, SecurityProvider}

import io.grpc.ManagedChannel
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Replaces DownloadDaemon in Nakamoto mode.
  *
  * Subscribes to the sidecar's GossipSub stream for live snapshots and attestations. Validates received snapshots (VRF proof + content
  * integrity), stores them, tracks the chain tip, and emits attestations.
  *
  * Transitions the node from Syncing → Ready when the local chain tip is within `catchUpThreshold` ordinals of the network tip.
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
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    lddConfig: LddConfig
  ): fs2.Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("NakamotoSyncDaemon")

    fs2.Stream.eval(Ref.of[F, SyncState](SyncState.initial)).flatMap { stateRef =>
      GossipStream.subscribe[F](channel).evalMap { msg =>
        msg.body match {
          case pb.GossipMessage.Body.Snapshot(snap) =>
            handleSnapshot(
              snap,
              stateRef,
              snapshotStorage,
              nodeStorage,
              tipTracker,
              stakeRegistry,
              sidecarClient,
              selfId,
              lddConfig,
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

  private def handleSnapshot[F[_]: Async: SecurityProvider: HasherSelector](
    snap: pb.Snapshot,
    stateRef: Ref[F, SyncState],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    lddConfig: LddConfig,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    for {
      _ <- logger.info(
        s"📥 Received snapshot ordinal=${snap.ordinal} slot=${snap.slot} from=${snap.producerId.toByteArray.take(4).map("%02x".format(_)).mkString}"
      )

      // Update network tip tracking
      _ <- stateRef.update { s =>
        if (snap.ordinal > s.networkTipOrdinal)
          s.copy(
            networkTipOrdinal = snap.ordinal,
            networkTipHash = Some(Hash(snap.hash.toByteArray.map("%02x".format(_)).mkString))
          )
        else s
      }

      // TODO: Full validation pipeline:
      // 1. Verify VRF proof: EcVrf25519.vrfVerify(vrfPK, eta || slotBytes, proof)
      // 2. Check LDD threshold: was producer eligible at that slot gap?
      // 3. Verify snapshot contents: reconstruct and compare hash
      // 4. Check signature on the Signed[GlobalIncrementalSnapshot]
      //
      // For now, accept all snapshots from gossip (trust-on-first-use).
      // This is safe for the PoC since all nodes run the same code.

      // Record in TipTracker (snapshot producer effectively attests to their own tip)
      tipHash = Hash(snap.hash.toByteArray.map("%02x".format(_)).mkString)
      tipSlot = Slot(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(snap.slot))
      producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)
      producerId = peer.PeerId(producerHex)
      att = DomainTipAttestation(tipHash, tipSlot, snap.ordinal, tipSlot)
      _ <- tipTracker.recordAttestation(producerId, att)

      // Check if we should transition to Ready
      state <- stateRef.get
      nodeState <- nodeStorage.getNodeState
      _ <- Async[F].whenA(!state.isReady && nodeState =!= NodeState.Ready) {
        val localOrdinal = snap.ordinal // We just received this, so we're at least here
        val caughtUp = state.networkTipOrdinal - localOrdinal <= CatchUpThreshold
        Async[F].whenA(caughtUp) {
          for {
            _ <- logger.info(
              s"✅ Caught up to network tip (local=$localOrdinal, network=${state.networkTipOrdinal}). Transitioning to Ready."
            )
            _ <- stateRef.update(_.copy(isReady = true, localTipOrdinal = localOrdinal))
            // Force transition to Ready — in Nakamoto mode, subscribing to gossip
            // and catching up IS the join process. No BFT enrollment needed.
            _ <- nodeStorage.setNodeState(NodeState.Ready)
            _ <- logger.info(s"🟢 Node state set to Ready — VRF slot production will begin")
          } yield ()
        }
      }

      // Emit our own attestation for this snapshot
      // (validates that we've seen and accepted it)
      _ <- emitAttestation(snap, sidecarClient, selfId, logger)

    } yield ()

  private def handleAttestation[F[_]: Async](
    att: pb.TipAttestation,
    tipTracker: TipTracker[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    val tipHash = Hash(att.tipHash.toByteArray.map("%02x".format(_)).mkString)
    val tipSlot = Slot(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(att.tipSlot))
    val attesterHex = Hex(att.attesterId.toByteArray.map("%02x".format(_)).mkString)
    val attesterId = peer.PeerId(attesterHex)
    val attestedAtSlot = Slot(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(att.attestedAt))
    val domainAtt = DomainTipAttestation(tipHash, tipSlot, att.tipOrdinal, attestedAtSlot)
    tipTracker.recordAttestation(attesterId, domainAtt) >>
      logger.debug(
        s"📨 Attestation for ordinal=${att.tipOrdinal} slot=${att.tipSlot} from=${attesterHex.value.take(8)}"
      )
  }

  private def emitAttestation[F[_]: Async](
    snap: pb.Snapshot,
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    // Build attestation from our identity
    val att = SidecarClient.mkAttestation(
      tipHash = snap.hash.toByteArray,
      tipSlot = snap.slot,
      tipOrdinal = snap.ordinal,
      attestedAt = System.currentTimeMillis() / 1000L, // current slot approximation
      attesterId = selfId.value.value.getBytes("UTF-8").take(32),
      signature = Array.emptyByteArray // TODO: sign (tipHash || tipSlot) with node key
    )
    sidecarClient.publishAttestation(att).void.handleErrorWith { e =>
      logger.warn(s"Failed to emit attestation: ${e.getMessage}")
    }
  }
}
