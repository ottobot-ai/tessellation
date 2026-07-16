package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.kernel.Ref
import cats.effect.{IO, Resource}

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.Duration

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import io.grpc.ManagedChannel
import weaver.MutableIOSuite

object WatchtowerFraudProofCertificateGateSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      securityProvider <- SecurityProvider.forAsync[IO]
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
    } yield (hasher, securityProvider)

  private val metagraph = Address.fromBytes("certificate-gate-mg".getBytes("UTF-8"))
  private val claimedRoot = Hash("11" * 32)
  private val reproducedRoot = Hash("22" * 32)

  private def checkpoint: ShardCheckpoint = {
    val peerId = PeerId(Hex("33" * 64))
    val binary = Signed(
      StateChannelSnapshotBinary(Hash.empty, Array.emptyByteArray, SnapshotFee.MinValue),
      NonEmptySet.of(SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("44" * 64)), Signature(Hex("55" * 70))))
    )
    val delta = ShardDerivedStateDelta.empty.copy(
      includedSnapshots = SortedMap(metagraph -> NonEmptyList.one(binary)),
      perMetagraphMptRoots = SortedMap(metagraph -> claimedRoot)
    )

    ShardCheckpoint(
      shardId = ShardId.unsafeApply(0),
      parentCheckpointHash = Hash.empty,
      shardOrdinal = ShardOrdinal.Root.next,
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(100L)),
      slot = io.constellationnetwork.schema.nakamoto.slot.Slot.unsafeApply(1L),
      derivedStateDelta = delta,
      committeeSignatures = NonEmptyList.one(CommitteeMemberSignature(peerId, Hex(""), Hex(""), Hex(""), 0)),
      epoch = io.constellationnetwork.schema.nakamoto.EtaPeriod.Zero
    )
  }

  private def manager(
    certificate: Either[String, Unit],
    mismatches: List[WatchtowerMismatch],
    replayCalls: Ref[IO, Int]
  ): ShardCheckpointGl0AcceptanceManager[IO] =
    new ShardCheckpointGl0AcceptanceManager[IO] {
      private def unused[A](name: String): IO[A] = IO.raiseError(new AssertionError(s"unexpected manager call: $name"))

      def evaluate(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] = unused("evaluate")

      def evaluateForSigning(
        checkpoint: ShardCheckpoint
      ): IO[Either[VerifiedShardCheckpointFailure, VerifiedShardCheckpoint]] = unused("evaluateForSigning")

      def verifyEmbedded(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] = unused("verifyEmbedded")

      def verifyExecutionCertificate(checkpoint: ShardCheckpoint): IO[Either[String, Unit]] = IO.pure(certificate)

      def verifyCommitteeSignature(
        checkpoint: ShardCheckpoint,
        signature: CommitteeMemberSignature
      ): IO[Either[String, Unit]] = unused("verifyCommitteeSignature")

      def watchtowerReExec(checkpoint: ShardCheckpoint): IO[List[WatchtowerMismatch]] =
        replayCalls.update(_ + 1).as(mismatches)

      def noteAdopted(shardId: ShardId, shardOrdinal: ShardOrdinal, checkpointHash: Hash): IO[Unit] = unused("noteAdopted")

      def lastAdoptedOrd(shardId: ShardId): IO[Option[ShardOrdinal]] = unused("lastAdoptedOrd")

      def lastAdoptedAnchor(shardId: ShardId): IO[Option[Hash]] = unused("lastAdoptedAnchor")

      def lastAdoptedCheckpoint(shardId: ShardId): IO[Option[(ShardOrdinal, Hash)]] = unused("lastAdoptedCheckpoint")
    }

  private def client(publishCalls: Ref[IO, Int]): SidecarClient.SidecarClientAlgebra[IO] =
    new SidecarClient.SidecarClientAlgebra[IO] {
      def publishSnapshot(msg: Snapshot) = IO.pure(PublishResponse(ok = true))
      def publishAttestation(msg: TipAttestation) = IO.pure(PublishResponse(ok = true))
      def publishRumor(msg: Rumor) = IO.pure(PublishResponse(ok = true))
      def publishMetagraphBinary(msg: MetagraphBinary) = IO.pure(PublishResponse(ok = true))
      def publishMetagraphAttestation(msg: MetagraphAttestation) = IO.pure(PublishResponse(ok = true))
      def publishAllowSpendBlock(payload: Array[Byte]) = IO.pure(PublishResponse(ok = true))
      def publishDAGBlock(payload: Array[Byte]) = IO.pure(PublishResponse(ok = true))
      def publishTokenLockBlock(payload: Array[Byte]) = IO.pure(PublishResponse(ok = true))
      def publishShardCheckpoint(msg: ShardCheckpointWire) = IO.pure(PublishResponse(ok = true))
      def publishShardCheckpointAttestation(msg: ShardCheckpointAttestationWire) = IO.pure(PublishResponse(ok = true))
      def publishFraudProof(msg: FraudProofEnvelopeWire): IO[PublishResponse] =
        publishCalls.update(_ + 1).as(PublishResponse(ok = true))
      def confirmFinalized(topic: String, msgIds: List[Array[Byte]]) = IO.pure(ConfirmFinalizedResponse(dropped = 0))
      def health = IO.pure(HealthResponse(healthy = true))
      def peers = IO.pure(PeerCountResponse(total = 0))
      def channel: ManagedChannel = throw new UnsupportedOperationException("stub")
    }

  private def emitter(
    certificate: Either[String, Unit],
    mismatches: List[WatchtowerMismatch],
    replayCalls: Ref[IO, Int],
    publishCalls: Ref[IO, Int]
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[WatchtowerFraudProofEmitter[IO]] =
    KeyPairGenerator.makeKeyPair[IO].map { keyPair =>
      WatchtowerFraudProofEmitter.make[IO](
        selfPeerId = PeerId.fromPublic(keyPair.getPublic),
        selfKeyPair = keyPair,
        acceptanceManager = manager(certificate, mismatches, replayCalls),
        sidecarClient = client(publishCalls),
        publishAttempts = 1,
        publishRetryDelay = Duration.Zero
      )
    }

  test("an incomplete execution certificate is rejected before replay or fraud-proof publication") { res =>
    implicit val (hasher, securityProvider) = res
    for {
      replayCalls <- Ref.of[IO, Int](0)
      publishCalls <- Ref.of[IO, Int](0)
      watchtower <- emitter(
        Left("execution quorum missing"),
        List(WatchtowerMismatch(metagraph, claimedRoot, reproducedRoot)),
        replayCalls,
        publishCalls
      )
      _ <- watchtower.emit(checkpoint)
      replays <- replayCalls.get
      publishes <- publishCalls.get
    } yield expect.all(replays == 0, publishes == 0)
  }

  test("a complete execution certificate plus affirmative mismatch emits portable evidence") { res =>
    implicit val (hasher, securityProvider) = res
    for {
      replayCalls <- Ref.of[IO, Int](0)
      publishCalls <- Ref.of[IO, Int](0)
      watchtower <- emitter(
        Right(()),
        List(WatchtowerMismatch(metagraph, claimedRoot, reproducedRoot)),
        replayCalls,
        publishCalls
      )
      _ <- watchtower.emit(checkpoint)
      replays <- replayCalls.get
      publishes <- publishCalls.get
    } yield expect.all(replays == 1, publishes == 1)
  }

  test("certificate-authenticated but unavailable replay emits no evidence") { res =>
    implicit val (hasher, securityProvider) = res
    for {
      replayCalls <- Ref.of[IO, Int](0)
      publishCalls <- Ref.of[IO, Int](0)
      watchtower <- emitter(Right(()), Nil, replayCalls, publishCalls)
      _ <- watchtower.emit(checkpoint)
      replays <- replayCalls.get
      publishes <- publishCalls.get
    } yield expect.all(replays == 1, publishes == 0)
  }
}
