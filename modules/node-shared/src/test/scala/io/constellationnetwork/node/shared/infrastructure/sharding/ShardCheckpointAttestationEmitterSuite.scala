package io.constellationnetwork.node.shared.infrastructure.sharding

import java.security.SecureRandom

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardTipTracker
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient.SidecarClientAlgebra
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, LddConfig}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import com.google.protobuf.ByteString
import io.grpc.ManagedChannel
import weaver.MutableIOSuite

/** Tests for [[ShardCheckpointAttestationEmitter]] — the `T_count_shard` quorum-closure emit seam.
  *
  * Coverage:
  *   1. '''Happy path''': `emit(...)` records the self-attestation into the per-shard tracker, publishes a
  *      `ShardCheckpointAttestationWire`, and the published Ed25519 signature verifies under the emitter's VK over the canonical checkpoint
  *      hash bytes (the same bytes the producer + acceptance manager use). 2. '''Skip when shard not tracked''': `emit` for a shard with no
  *      `shardEta` is a no-op — no publish, no tracker record.
  *
  * Mirrors `ShardCheckpointProducerSuite`'s fixture strategy: real `EligibilityChecker` + `Hasher` + Ed25519 keypair, stubbed `KesSigner`
  * (constant payload — KES verification is the gl0-acceptance path, not exercised here) and a recording `SidecarClient`.
  */
object ShardCheckpointAttestationEmitterSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], EligibilityChecker[IO])

  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8).asResource
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38).asResource
      ec = EligibilityChecker.make[IO](log1p, exp)
    } yield (h, sp, ec)

  private val shardZero: ShardId = ShardId.unsafeApply(0)

  private val random = {
    val r = SecureRandom.getInstance("SHA1PRNG")
    r.setSeed(0x53_48_43_41_54_54L) // ASCII "SHCATT"
    r
  }

  private def randomVrfSk(): Array[Byte] = {
    val sk = new Array[Byte](32)
    random.nextBytes(sk)
    sk
  }

  private def randomShardEta(): Array[Byte] = {
    val eta = new Array[Byte](32)
    random.nextBytes(eta)
    eta
  }

  /** A KES signer returning a fixed payload — the emitter just carries it on the wire; KES verification is the gl0-acceptance path. */
  private val stubKesSigner: ShardCheckpointProducer.KesSigner[IO] =
    ShardCheckpointProducer.KesSigner.fixed[IO](period = 3, signatureBytes = Array.fill[Byte](8)(0x7))

  /** Recording sidecar client: captures every published `ShardCheckpointAttestationWire`. */
  private def recordingSidecar(ref: Ref[IO, List[ShardCheckpointAttestationWire]]): SidecarClientAlgebra[IO] =
    new SidecarClientAlgebra[IO] {
      def publishSnapshot(msg: Snapshot) = IO.pure(PublishResponse(ok = true))
      def publishAttestation(msg: TipAttestation) = IO.pure(PublishResponse(ok = true))
      def publishRumor(msg: Rumor) = IO.pure(PublishResponse(ok = true))
      def publishMetagraphBinary(msg: MetagraphBinary) = IO.pure(PublishResponse(ok = true))
      def publishMetagraphAttestation(msg: MetagraphAttestation) = IO.pure(PublishResponse(ok = true))
      def publishAllowSpendBlock(payload: Array[Byte]) = IO.pure(PublishResponse(ok = true))
      def publishDAGBlock(payload: Array[Byte]) = IO.pure(PublishResponse(ok = true))
      def publishTokenLockBlock(payload: Array[Byte]) = IO.pure(PublishResponse(ok = true))
      def publishShardCheckpoint(msg: ShardCheckpointWire) = IO.pure(PublishResponse(ok = true))
      def publishShardCheckpointAttestation(msg: ShardCheckpointAttestationWire) =
        ref.update(msg :: _).as(PublishResponse(ok = true))
      def confirmFinalized(topic: String, msgIds: List[Array[Byte]]) = IO.pure(ConfirmFinalizedResponse(dropped = msgIds.size))
      def health = IO.pure(HealthResponse(healthy = true))
      def peers = IO.pure(PeerCountResponse(total = 0))
      def channel: ManagedChannel = throw new UnsupportedOperationException("stub")
    }

  private val checkpointHash: Hash = Hash.fromBytes("shard-checkpoint-under-attestation".getBytes("UTF-8"))

  test("happy path: emit records self-attestation, publishes, and the Ed25519 sig verifies under the VK") { res =>
    implicit val (h, sp, ec) = res
    val shardEta = randomShardEta()
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(kp.getPublic)
      published <- Ref.of[IO, List[ShardCheckpointAttestationWire]](Nil)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      emitter = ShardCheckpointAttestationEmitter.make[IO](
        selfPeerId = selfId,
        selfKeyPair = kp,
        selfVrfSk = randomVrfSk(),
        kesSigner = stubKesSigner,
        eligibilityChecker = ec,
        sidecarClient = recordingSidecar(published),
        tipTrackerFor = sid => if (sid == shardZero) Some(tracker) else None,
        // Slice S4: epoch-keyed eta resolver. Fixed precomputed eta for shardZero regardless of epoch — the emitter
        // threads the resolved eta into its VRF membership proof; rotation correctness is covered in ShardSlotLeaderSuite.
        shardEtaFor = (sid, _) => IO.pure(if (sid == shardZero) Some(shardEta) else None),
        sigmaInCommittee = Ratio(1, 4),
        lddConfig = LddConfig.Default
      )
      _ <- emitter.emit(shardZero, checkpointHash, Slot.unsafeApply(5L), EtaPeriod(0L))
      wires <- published.get
      // Self-attestation recorded locally (excludeSelf=false to see it; default excludeSelf would hide it).
      selfCount <- tracker.attestationCountFor(checkpointHash, excludeSelf = false)
      // Verify the published wire: shard id + checkpoint hash + the Ed25519 sig verifies over the canonical hash bytes.
      wire = wires.head
      attestation <- ShardCheckpointWireCodecs.shardCheckpointAttestationFromWire[IO](wire)
      msgBytes = checkpointHash.value.getBytes("UTF-8")
      pubKey <- selfId.value.toPublicKey[IO]
      edOk <- Signing.verifySignature[IO](msgBytes, attestation.attesterSignature.ed25519Sig.toBytes)(pubKey)
    } yield
      expect.all(
        wires.size == 1,
        attestation.shardId == shardZero,
        attestation.checkpointHash == checkpointHash,
        attestation.attesterSignature.peerId == selfId,
        attestation.attesterSignature.kesTreeStep == 3,
        edOk,
        selfCount == 1
      )
  }

  test("skip: emit for an untracked shard (no shardEta) is a no-op — no publish, no record") { res =>
    implicit val (h, sp, ec) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(kp.getPublic)
      published <- Ref.of[IO, List[ShardCheckpointAttestationWire]](Nil)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      emitter = ShardCheckpointAttestationEmitter.make[IO](
        selfPeerId = selfId,
        selfKeyPair = kp,
        selfVrfSk = randomVrfSk(),
        kesSigner = stubKesSigner,
        eligibilityChecker = ec,
        sidecarClient = recordingSidecar(published),
        tipTrackerFor = _ => Some(tracker),
        shardEtaFor = (_, _) => IO.pure(Option.empty[Array[Byte]]), // no eta for any shard ⇒ skip
        sigmaInCommittee = Ratio(1, 4),
        lddConfig = LddConfig.Default
      )
      _ <- emitter.emit(shardZero, checkpointHash, Slot.unsafeApply(5L), EtaPeriod(0L))
      wires <- published.get
      count <- tracker.attestationCountFor(checkpointHash, excludeSelf = false)
    } yield expect.all(wires.isEmpty, count == 0)
  }
}
