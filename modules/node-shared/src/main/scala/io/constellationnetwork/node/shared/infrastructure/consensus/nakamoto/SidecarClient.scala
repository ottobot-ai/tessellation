package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import java.util.concurrent.TimeUnit

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._

import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

/** gRPC client for the Go libp2p sidecar.
  *
  * The sidecar manages GossipSub mesh networking. This client talks to it over localhost gRPC to publish snapshots/attestations and
  * subscribe to incoming gossip messages.
  */
object SidecarClient {

  final case class SidecarConfig(
    host: String = "127.0.0.1",
    grpcPort: Int = 50051
  )

  trait SidecarClientAlgebra[F[_]] {
    def publishSnapshot(msg: Snapshot): F[PublishResponse]
    def publishAttestation(msg: TipAttestation): F[PublishResponse]
    def publishRumor(msg: Rumor): F[PublishResponse]
    def publishMetagraphBinary(msg: MetagraphBinary): F[PublishResponse]

    /** Publish a per-metagraph committee VRF attestation (Slice S2). The wire payload is a proto-encoded `MetagraphAttestation` produced by
      * the JVM; the sidecar treats it as opaque, gossips it on the metagraph-attestation topic, and adds it to the durable-publish outbox
      * (#196).
      */
    def publishMetagraphAttestation(msg: MetagraphAttestation): F[PublishResponse]

    /** Publish a Signed[AllowSpendBlock] to all GL0 nodes via the durable- publish outbox. Replaces the single-peer HTTP POST path from
      * `Swap.sendBlockToL0` (#196).
      *
      * @param payload
      *   serialized `Signed[AllowSpendBlock]` bytes (via JsonSerializer); opaque to the sidecar but used by the sidecar's outbox to derive
      *   the message id (sha256 first 32 bytes).
      */
    def publishAllowSpendBlock(payload: Array[Byte]): F[PublishResponse]

    /** Ack to the sidecar that the listed message ids on `topic` have reached Phase-3 finality and may be dropped from the outbox. Each id
      * is the sha256 (first 32 bytes) of the same payload bytes the JVM published.
      *
      * For AllowSpendBlock + MetagraphBinary: id = sha256(serializedSigned), computed via `Hasher[F].hashBytes` so the Hasher typeclass is
      * the single source of hash truth (per project rule `feedback_use_hasher_no_manual_serialize`). For MetagraphAttestation: id =
      * sha256(wire-level proto bytes).
      *
      * Topic literals come from [[OutboxTopic]] — DO NOT pass string literals directly so a future rename can't drift the JVM/sidecar pair.
      */
    def confirmFinalized(topic: String, msgIds: List[Array[Byte]]): F[ConfirmFinalizedResponse]

    def health: F[HealthResponse]
    def peers: F[PeerCountResponse]
    def channel: ManagedChannel
  }

  /** Outbox topic labels — kept in lockstep with the Go sidecar's [[grpcserver.TopicAllowSpendBlock]] et al. so a typo here can't desync
    * the wire-format vocabulary. Distinct from the GossipSub topic strings (which carry version paths); these are short stable labels.
    */
  object OutboxTopic {
    val AllowSpendBlock = "allow-spend-block"
    val MetagraphBinary = "metagraph-binary"
    val MetagraphAttestation = "metagraph-attestation"
  }

  /** Create a gRPC client Resource that opens a channel and cleans up on release. */
  def makeResource[F[_]: Async](config: SidecarConfig): Resource[F, SidecarClientAlgebra[F]] =
    Resource
      .make(
        Async[F].delay(
          ManagedChannelBuilder
            .forAddress(config.host, config.grpcPort)
            .usePlaintext()
            .keepAliveTime(30, TimeUnit.SECONDS)
            .keepAliveTimeout(10, TimeUnit.SECONDS)
            .keepAliveWithoutCalls(true)
            .build()
        )
      )(ch => Async[F].delay(ch.shutdown()).void)
      .map(ch => fromChannel[F](ch))

  /** Build algebra from an existing channel. */
  def fromChannel[F[_]: Async](ch: ManagedChannel): SidecarClientAlgebra[F] = {
    val stub = SidecarServiceGrpc.stub(ch)

    new SidecarClientAlgebra[F] {
      private def liftFuture[A](fa: => scala.concurrent.Future[A]): F[A] =
        Async[F].fromFuture(Async[F].delay(fa))

      def publishSnapshot(msg: Snapshot): F[PublishResponse] =
        liftFuture(stub.publishSnapshot(msg))

      def publishAttestation(msg: TipAttestation): F[PublishResponse] =
        liftFuture(stub.publishAttestation(msg))

      def publishRumor(msg: Rumor): F[PublishResponse] =
        liftFuture(stub.publishRumor(msg))

      def publishMetagraphBinary(msg: MetagraphBinary): F[PublishResponse] =
        liftFuture(stub.publishMetagraphBinary(msg))

      def publishMetagraphAttestation(msg: MetagraphAttestation): F[PublishResponse] =
        liftFuture(stub.publishMetagraphAttestation(msg))

      def publishAllowSpendBlock(payload: Array[Byte]): F[PublishResponse] =
        liftFuture(stub.publishAllowSpendBlock(AllowSpendBlock(payload = ByteString.copyFrom(payload))))

      def confirmFinalized(topic: String, msgIds: List[Array[Byte]]): F[ConfirmFinalizedResponse] =
        liftFuture(
          stub.confirmFinalized(
            ConfirmFinalizedRequest(
              topic = topic,
              messageIds = msgIds.map(ByteString.copyFrom)
            )
          )
        )

      def health: F[HealthResponse] =
        liftFuture(stub.health(HealthRequest()))

      def peers: F[PeerCountResponse] =
        liftFuture(stub.peerCount(PeerCountRequest()))

      def channel: ManagedChannel = ch
    }
  }

  // ─── Helpers for constructing proto messages ───

  def mkSnapshot(
    hash: Array[Byte],
    slot: Long,
    ordinal: Long,
    parentHash: Array[Byte],
    vrfProof: Array[Byte],
    vrfPublicKey: Array[Byte],
    eta: Array[Byte],
    payload: Array[Byte],
    producerId: Array[Byte],
    parentSlot: Long = 0L,
    // §1.2 Slice 6: optional KES parallel signature over the snapshot artifact hash.
    // Empty by default (pre-Slice-6 callers + sender-side signAt failures both
    // produce an empty wire field; receivers tolerate empty in Slice 5/6 — load-bearing
    // verification lands in Slice 9).
    kesSignature: Array[Byte] = Array.empty[Byte]
  ): Snapshot =
    Snapshot(
      hash = ByteString.copyFrom(hash),
      slot = slot,
      ordinal = ordinal,
      parentHash = ByteString.copyFrom(parentHash),
      vrfProof = ByteString.copyFrom(vrfProof),
      vrfPublicKey = ByteString.copyFrom(vrfPublicKey),
      eta = ByteString.copyFrom(eta),
      payload = ByteString.copyFrom(payload),
      producerId = ByteString.copyFrom(producerId),
      parentSlot = parentSlot,
      kesSignature = ByteString.copyFrom(kesSignature)
    )

  def mkRumor(
    signedRumorBytes: Array[Byte],
    contentType: String,
    originId: Array[Byte]
  ): Rumor =
    Rumor(
      signedRumorBytes = ByteString.copyFrom(signedRumorBytes),
      contentType = contentType,
      originId = ByteString.copyFrom(originId)
    )

  def mkAttestation(
    tipHash: Array[Byte],
    tipSlot: Long,
    tipOrdinal: Long,
    attestedAt: Long,
    attesterId: Array[Byte],
    signature: Array[Byte],
    // §1.2 Slice 5: optional KES parallel signature over the attestation hash.
    // Empty by default (pre-Slice-5 callers + sender-side signAt failures both
    // produce an empty wire field; receivers tolerate empty in Slice 5/6 — load-bearing
    // verification lands in Slice 9).
    kesSignature: Array[Byte] = Array.empty[Byte]
  ): TipAttestation =
    TipAttestation(
      tipHash = ByteString.copyFrom(tipHash),
      tipSlot = tipSlot,
      tipOrdinal = tipOrdinal,
      attestedAt = attestedAt,
      attesterId = ByteString.copyFrom(attesterId),
      signature = ByteString.copyFrom(signature),
      kesSignature = ByteString.copyFrom(kesSignature)
    )

  def mkMetagraphBinary(
    address: String,
    binary: Array[Byte]
  ): MetagraphBinary =
    MetagraphBinary(
      address = address,
      binary = ByteString.copyFrom(binary)
    )
}
