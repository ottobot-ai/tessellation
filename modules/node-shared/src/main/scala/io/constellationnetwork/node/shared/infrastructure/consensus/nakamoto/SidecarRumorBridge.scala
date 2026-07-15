package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import java.nio.charset.StandardCharsets

import cats.effect.Async
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all._

import scala.concurrent.duration._

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
  *   - **Outbound** ([[publishFn]]): wired into `Gossip.setSidecarPublishFn`. Every rumor passing through `Gossip.spread` is also forwarded
  *     to the sidecar via `PublishRumor`.
  *   - **Inbound** ([[receive]]): subscribes to the sidecar's `GossipMessage` stream, filters `Rumor` bodies, deserializes back to
  *     `Signed[RumorRaw]`, recomputes the hash, and offers to `rumorQueue`. The existing `GossipDaemon.consumeRumors` pipeline then
  *     validates signature + collateral and dispatches via the registered `RumorHandler`s — meaning **CL0 BFT consensus messages,
  *     Tessellation events, and any other rumor type ride for free** without changes to their handlers.
  *
  * Wire format: `signed.asJson.noSpaces.getBytes(UTF_8)`. JSON is sized for the existing `application.conf` rumor capacities and matches
  * the format already used by the legacy HTTP gossip routes.
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

  /** Inbound receive loop. Subscribes to the sidecar gossip stream RUMOR-ONLY (`SubscribeTopics.rumorOnly`), parses Rumor messages, and
    * offers `Hashed[RumorRaw]` to the shared `rumorQueue` so the existing `GossipDaemon.consumeRumors` pipeline picks them up.
    *
    * '''Rumor-only filter (FINDING-F1).''' This stream previously subscribed to EVERYTHING and dropped non-rumors via the `isRumor`
    * collect. That was not just wasteful: the sidecar's shard-checkpoint families ride SHARED fan-in channels (one drainer), so this stream
    * race-drained ~half the shard checkpoints away from the `NakamotoSyncDaemon` and silently discarded them. The explicit filter confines
    * this stream to the rumor family; the collect remains as belt-and-braces against a misrouted body.
    *
    * '''Reconnect (FINDING-F8).''' Mirrors the daemon's two-layer restart: on error OR normal termination (sidecar restart, mesh-recovery
    * stream reset) the loop re-subscribes after a short delay instead of dying permanently — previously the first disconnect killed inbound
    * sidecar rumors on this node for the rest of the process lifetime.
    *
    * Run as a supervised background fiber.
    */
  def receive[F[_]: Async](
    channel: ManagedChannel,
    rumorQueue: Queue[F, Hashed[RumorRaw]]
  )(implicit S: Supervisor[F], hasherSelector: HasherSelector[F]): F[Unit] = {
    val logger = Slf4jLogger.getLogger[F]

    def receiveStream: Stream[F, Unit] =
      GossipStream
        .subscribe[F](channel, SidecarClient.SubscribeTopics.rumorOnly)
        .collect { case msg if msg.body.isRumor => msg.getRumor }
        .evalMap { rumor =>
          val bytes = rumor.signedRumorBytes.toByteArray
          val jsonString = new String(bytes, StandardCharsets.UTF_8)
          circeDecode[Signed[RumorRaw]](jsonString) match {
            case Right(signed) =>
              hasherSelector
                .withCurrent(implicit hasher => signed.toHashed)
                // tryOffer-DROP, not blocking offer (2026-06-10): the rumor queue is bounded. Blocking here would
                // stall this GossipStream drain and migrate the backlog into GossipStream's own queue. Dropping an
                // inbound PEER rumor bounds memory, but Nakamoto mode does not run the legacy pull-gossip rounds,
                // so this path is lossy until durable retry/outbox recovery lands. (Local-produced rumors use a
                // supervised blocking offer in Gossip.spread; those must not drop.)
                .flatMap { hashed =>
                  rumorQueue.tryOffer(hashed).flatMap {
                    case true => Async[F].unit
                    case false =>
                      logger.warn(
                        s"Rumor queue full — dropping inbound sidecar rumor (contentType=${rumor.contentType}); Nakamoto transport retry is not guaranteed"
                      )
                  }
                }
                .handleErrorWith(err => logger.warn(err)(s"Failed to hash/enqueue sidecar rumor (contentType=${rumor.contentType})"))
            case Left(err) =>
              logger.warn(s"Failed to decode sidecar rumor (contentType=${rumor.contentType}): ${err.getMessage}")
          }
        }
        .handleErrorWith { err =>
          Stream.eval(logger.warn(err)(s"Sidecar rumor receive stream failed. Reconnecting in ${ReconnectDelay.toSeconds}s...")) ++
            Stream.sleep_[F](ReconnectDelay) ++ receiveStream
        } ++ Stream.eval(
        // Normal termination (server completed the stream / connection lost): same restart path the
        // NakamotoSyncDaemon uses — never leave the bridge permanently dead (F8).
        logger.warn(s"Sidecar rumor receive stream terminated. Reconnecting in ${ReconnectDelay.toSeconds}s...")
      ) ++ Stream.sleep_[F](ReconnectDelay) ++ receiveStream

    S.supervise(receiveStream.compile.drain).void
  }

  /** Restart delay after a stream error/termination — matches the `NakamotoSyncDaemon` gossip-stream reconnect cadence. */
  private val ReconnectDelay: FiniteDuration = 5.seconds

  private def originIdBytes(rumor: RumorRaw): Array[Byte] =
    rumor match {
      case io.constellationnetwork.schema.gossip.PeerRumorRaw(origin, _, _, _) =>
        peerIdBytes(origin)
      case _ => Array.emptyByteArray
    }

  private def peerIdBytes(peerId: PeerId): Array[Byte] =
    peerId.value.value.getBytes(StandardCharsets.UTF_8)
}
