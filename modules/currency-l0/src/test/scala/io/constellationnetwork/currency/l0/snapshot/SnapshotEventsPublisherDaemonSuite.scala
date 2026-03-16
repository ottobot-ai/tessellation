package io.constellationnetwork.currency.l0.snapshot

import java.security.KeyPair

import cats.effect.std.{Queue, Supervisor}
import cats.effect.{IO, Resource}

import scala.concurrent.TimeoutException
import scala.concurrent.duration.DurationInt

import io.constellationnetwork.block.generators.signedBlockGen
import io.constellationnetwork.currency.dataApplication.DataUpdate
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.gossip.Gossip
import io.constellationnetwork.node.shared.infrastructure.metrics.NoOpMetrics
import io.constellationnetwork.node.shared.infrastructure.snapshot.daemon.SnapshotEventsPublisherDaemon
import io.constellationnetwork.node.shared.snapshot.currency.{BlockEvent, CurrencySnapshotEvent}
import io.constellationnetwork.schema.generation.Generation
import io.constellationnetwork.schema.gossip.{Counter, Ordinal, RumorRaw}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.numeric.PosLong
import fs2.Stream
import io.circe.{Encoder, Json}
import weaver.MutableIOSuite
import weaver.scalacheck.Checkers

object SnapshotEventsPublisherDaemonSuite extends MutableIOSuite with Checkers {

  override type Res =
    (Supervisor[IO], Hasher[IO], SecurityProvider[IO], KeyPair)

  override def sharedResource: Resource[IO, Res] =
    for {
      s <- Supervisor[IO](await = true)
      implicit0(js: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].toResource
      implicit0(sp: SecurityProvider[IO]) <- SecurityProvider.forAsync[IO]
      h = Hasher.forJson[IO]
      kp <- KeyPairGenerator.makeKeyPair[IO].toResource
    } yield (s, h, sp, kp)

  private def waitUntilQueueSize(queue: Queue[IO, _], expectedSize: Int, attempt: Int = 0): IO[Unit] =
    for {
      size <- queue.size
      res <-
        if (size == expectedSize) {
          IO.unit
        } else if (attempt >= 30) {
          IO.raiseError(new TimeoutException)
        } else {
          IO.sleep(1.second).flatMap(_ => waitUntilQueueSize(queue, expectedSize, attempt + 1))
        }
    } yield res

  test("should publish events to gossip") { res =>
    implicit val (s, h, sp, keyPair) = res

    implicit val m = NoOpMetrics.make

    implicit val e = {
      implicit val noopEncoder: Encoder[DataUpdate] = new Encoder[DataUpdate] {
        final def apply(a: DataUpdate): Json = Json.Null
      }
      CurrencySnapshotEvent.encoder
    }

    val blockEvent1 = signedBlockGen.sample.map(BlockEvent(_)).get
    val blockEvent2 = signedBlockGen.sample.map(BlockEvent(_)).get

    val selfId = PeerId(Hex("0000000000000000"))

    for {
      rumorQueue: Queue[IO, Hashed[RumorRaw]] <- Queue.unbounded[IO, Hashed[RumorRaw]]

      l1OutputQueue <- Queue.unbounded[IO, Option[CurrencySnapshotEvent]]
      _ <- l1OutputQueue.offer(Some(blockEvent1))
      _ <- l1OutputQueue.offer(Some(blockEvent2))
      _ <- l1OutputQueue.offer(None)

      events = Stream.fromQueueNoneTerminated(l1OutputQueue)
      gossip <- Gossip.make[IO](rumorQueue, selfId, Generation(PosLong(1)), keyPair)
      _ <- SnapshotEventsPublisherDaemon.make(gossip, events).spawn.start
      _ <- waitUntilQueueSize(l1OutputQueue, 0)
      _ <- waitUntilQueueSize(rumorQueue, 2)
      rumorQueueSize <- rumorQueue.size
    } yield expect.eql(2, rumorQueueSize)
  }
}
