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

    /** Slice S2: publish a per-metagraph committee attestation. The Go sidecar's `PublishMetagraphAttestation` RPC was wired in S2
      * (`de15d0b3d`); this is the JVM caller surface used by `MetagraphCommitteeGate` in S3 once the sender path goes load-bearing.
      */
    def publishMetagraphAttestation(msg: MetagraphAttestation): F[PublishResponse]

    def health: F[HealthResponse]
    def peers: F[PeerCountResponse]
    def channel: ManagedChannel
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

  /** Slice S2/S3 helper: construct a `MetagraphAttestation` proto from raw byte fields. The fields' meanings match the proto definition in
    * `p2p/proto/sidecar.proto` — kept here so callers don't have to import the proto types directly. `kesSignature` is REQUIRED in S3
    * (load-bearing flip); empty by default for tests that exercise the "reject empty KES" path.
    */
  def mkMetagraphAttestation(
    peerIdBytes: Array[Byte],
    metagraphAddress: String,
    parentHash: Array[Byte],
    binaryHash: Array[Byte],
    committeeVrfProof: Array[Byte],
    signature: Array[Byte],
    kesSignature: Array[Byte] = Array.empty[Byte]
  ): MetagraphAttestation =
    MetagraphAttestation(
      peerId = ByteString.copyFrom(peerIdBytes),
      metagraphAddress = metagraphAddress,
      parentHash = ByteString.copyFrom(parentHash),
      binaryHash = ByteString.copyFrom(binaryHash),
      committeeVrfProof = ByteString.copyFrom(committeeVrfProof),
      signature = ByteString.copyFrom(signature),
      kesSignature = ByteString.copyFrom(kesSignature)
    )
}
