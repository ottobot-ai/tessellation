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
    parentSlot: Long = 0L
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
      parentSlot = parentSlot
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
    signature: Array[Byte]
  ): TipAttestation =
    TipAttestation(
      tipHash = ByteString.copyFrom(tipHash),
      tipSlot = tipSlot,
      tipOrdinal = tipOrdinal,
      attestedAt = attestedAt,
      attesterId = ByteString.copyFrom(attesterId),
      signature = ByteString.copyFrom(signature)
    )
}
