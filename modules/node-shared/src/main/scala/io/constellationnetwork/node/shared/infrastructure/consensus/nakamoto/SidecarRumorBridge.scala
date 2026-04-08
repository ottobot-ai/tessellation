package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import java.nio.charset.StandardCharsets

import cats.effect.Async
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.gossip.{Gossip => GossipAlg}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._
import io.constellationnetwork.schema.gossip.RumorRaw
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, HasherSelector}

import fs2.Stream
import io.circe.parser.{decode => circeDecode}
import io.circe.syntax._
import io.grpc.ManagedChannel
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Bridges Tessellation rumor gossip onto the Go libp2p sidecar GossipSub transport.
  *
  * The sidecar treats the rumor envelope as opaque bytes (JSON-serialized `Signed[RumorRaw]`); the JVM is responsible for signing,
  * validation, and dispatch. This bridge has two halves:
  *
  *   - **Outbound** ([[publishFn]]): wired into `Gossip.setSidecarPublishFn`. Every rumor passing through `Gossip.spread` is also
  *     forwarded to the sidecar via `PublishRumor`.
  *   - **Inbound** ([[receive]]): subscribes to the sidecar's `GossipMessage` stream, filters `Rumor` bodies, deserializes back to
  *     `Signed[RumorRaw]`, recomputes the hash, and offers to `rumorQueue`. The existing `GossipDaemon.consumeRumors` pipeline then
  *     validates signature + collateral and dispatches via the registered `RumorHandler`s — meaning **CL0 BFT consensus messages,
  *     Tessellation events, and any other rumor type ride for free** without changes to their handlers.
  *
  * Wire format: `signed.asJson.noSpaces.getBytes(UTF_8)`. JSON is sized for the existing `application.conf` rumor capacities and
  * matches the format already used by the legacy HTTP gossip routes.
  */
object SidecarRumorBridge {

  /** Build the outbound publish callback. Pass to `gossip.setSidecarPublishFn`. */
  def publishFn[F[_]: Async](
    sidecarClient: SidecarClient.SidecarClientAlgebra[F]
  ): GossipAlg.SidecarPublishFn[F] = { hashedRumor =>
    val signed = hashedRumor.signed
    val bytes = signed.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
    val contentType = signed.value.contentType.value
    val originBytes = originIdBytes(signed.value)
    val rumor = SidecarClient.mkRumor(
      signedRumorBytes = bytes,
      contentType = contentType,
      originId = originBytes
    )
    sidecarClient.publishRumor(rumor).flatMap { resp =>
      if (resp.ok) Async[F].unit
      else Async[F].raiseError(new RuntimeException(s"sidecar publishRumor failed: ${resp.error}"))
    }
  }

  /** Inbound receive loop. Subscribes to the sidecar gossip stream, parses Rumor messages, and offers `Hashed[RumorRaw]` to the
    * shared `rumorQueue` so the existing `GossipDaemon.consumeRumors` pipeline picks them up.
    *
    * Run as a supervised background fiber.
    */
  def receive[F[_]: Async](
    channel: ManagedChannel,
    rumorQueue: Queue[F, Hashed[RumorRaw]]
  )(implicit S: Supervisor[F], hasherSelector: HasherSelector[F]): F[Unit] = {
    val logger = Slf4jLogger.getLogger[F]

    S.supervise {
      GossipStream
        .subscribe[F](channel)
        .collect { case msg if msg.body.isRumor => msg.getRumor }
        .evalMap { rumor =>
          val bytes = rumor.signedRumorBytes.toByteArray
          val jsonString = new String(bytes, StandardCharsets.UTF_8)
          circeDecode[Signed[RumorRaw]](jsonString) match {
            case Right(signed) =>
              hasherSelector
                .withCurrent(implicit hasher => signed.toHashed)
                .flatMap(rumorQueue.offer)
                .handleErrorWith(err => logger.warn(err)(s"Failed to hash/enqueue sidecar rumor (contentType=${rumor.contentType})"))
            case Left(err) =>
              logger.warn(s"Failed to decode sidecar rumor (contentType=${rumor.contentType}): ${err.getMessage}")
          }
        }
        .handleErrorWith { err =>
          Stream.eval(logger.error(err)("Sidecar rumor receive stream failed")) >> Stream.empty
        }
        .compile
        .drain
    }.void
  }

  private def originIdBytes(rumor: RumorRaw): Array[Byte] =
    rumor match {
      case io.constellationnetwork.schema.gossip.PeerRumorRaw(origin, _, _, _) =>
        peerIdBytes(origin)
      case _ => Array.emptyByteArray
    }

  private def peerIdBytes(peerId: PeerId): Array[Byte] =
    peerId.value.value.getBytes(StandardCharsets.UTF_8)
}
