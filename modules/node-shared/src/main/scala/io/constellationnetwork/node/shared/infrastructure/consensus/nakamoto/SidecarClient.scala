package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import cats.effect.kernel.Async

import io.circe._
import io.circe.generic.semiauto._
import org.http4s._
import org.http4s.circe._
import org.http4s.client.Client

/** HTTP bridge to the Go libp2p sidecar.
  *
  * For PoC we use a simple REST API instead of gRPC to avoid adding protobuf/ScalaPB dependencies to the tessellation build. Production
  * will switch to proper gRPC via ScalaPB.
  *
  * The sidecar exposes a small HTTP API alongside its gRPC service: POST /publish/snapshot — broadcast a snapshot POST /publish/attestation
  * — broadcast an attestation GET /health — sidecar health GET /peers — peer count SSE /subscribe — server-sent events stream
  */
object SidecarClient {

  final case class SidecarConfig(
    host: String = "127.0.0.1",
    httpPort: Int = 50052
  ) {
    def baseUri: Uri = Uri.unsafeFromString(s"http://$host:$httpPort")
  }

  // Wire types for the HTTP bridge
  final case class SnapshotMsg(
    hash: String,
    slot: Long,
    ordinal: Long,
    parentHash: String,
    vrfProof: String,
    vrfPublicKey: String,
    eta: String,
    payload: String,
    producerId: String
  )
  object SnapshotMsg {
    implicit val encoder: Encoder[SnapshotMsg] = deriveEncoder
    implicit val decoder: Decoder[SnapshotMsg] = deriveDecoder
  }

  final case class AttestationMsg(
    tipHash: String,
    tipSlot: Long,
    tipOrdinal: Long,
    attestedAt: Long,
    attesterId: String,
    signature: String
  )
  object AttestationMsg {
    implicit val encoder: Encoder[AttestationMsg] = deriveEncoder
    implicit val decoder: Decoder[AttestationMsg] = deriveDecoder
  }

  final case class PublishResult(ok: Boolean, error: Option[String] = None)
  object PublishResult {
    implicit val decoder: Decoder[PublishResult] = deriveDecoder
  }

  final case class PeerCount(total: Int, meshSnapshots: Int, meshAttestations: Int)
  object PeerCount {
    implicit val decoder: Decoder[PeerCount] = deriveDecoder
  }

  final case class HealthStatus(healthy: Boolean, uptimeSeconds: Long, peerCount: Int)
  object HealthStatus {
    implicit val decoder: Decoder[HealthStatus] = deriveDecoder
  }

  /** Incoming gossip message from the sidecar. */
  sealed trait GossipEvent
  object GossipEvent {
    final case class IncomingSnapshot(msg: SnapshotMsg) extends GossipEvent
    final case class IncomingAttestation(msg: AttestationMsg) extends GossipEvent
  }

  trait SidecarClientAlgebra[F[_]] {
    def publishSnapshot(msg: SnapshotMsg): F[PublishResult]
    def publishAttestation(msg: AttestationMsg): F[PublishResult]
    def health: F[HealthStatus]
    def peers: F[PeerCount]
  }

  def make[F[_]: Async](
    client: Client[F],
    config: SidecarConfig
  ): SidecarClientAlgebra[F] = new SidecarClientAlgebra[F] {
    private val base = config.baseUri

    implicit val snapshotEntityEncoder: EntityEncoder[F, SnapshotMsg] = jsonEncoderOf[F, SnapshotMsg]
    implicit val attestationEntityEncoder: EntityEncoder[F, AttestationMsg] = jsonEncoderOf[F, AttestationMsg]
    implicit val publishResultEntityDecoder: EntityDecoder[F, PublishResult] = jsonOf[F, PublishResult]
    implicit val healthEntityDecoder: EntityDecoder[F, HealthStatus] = jsonOf[F, HealthStatus]
    implicit val peerCountEntityDecoder: EntityDecoder[F, PeerCount] = jsonOf[F, PeerCount]

    def publishSnapshot(msg: SnapshotMsg): F[PublishResult] = {
      val req = Request[F](Method.POST, base / "publish" / "snapshot").withEntity(msg)
      client.expect[PublishResult](req)
    }

    def publishAttestation(msg: AttestationMsg): F[PublishResult] = {
      val req = Request[F](Method.POST, base / "publish" / "attestation").withEntity(msg)
      client.expect[PublishResult](req)
    }

    def health: F[HealthStatus] =
      client.expect[HealthStatus](base / "health")

    def peers: F[PeerCount] =
      client.expect[PeerCount](base / "peers")
  }
}
