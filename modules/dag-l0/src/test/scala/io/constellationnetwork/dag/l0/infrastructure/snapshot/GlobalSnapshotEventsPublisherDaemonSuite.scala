package io.constellationnetwork.dag.l0.infrastructure.snapshot

import cats.effect.{IO, Ref}
import cats.syntax.all._

import scala.reflect.runtime.universe.TypeTag

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.gossip.Gossip
import io.constellationnetwork.node.shared.infrastructure.mempool.{EventMempool, MempoolConfig, StateKeyExtractor}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import derevo.circe.magnolia.encoder
import derevo.derive
import io.circe.Encoder
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.SimpleIOSuite

object GlobalSnapshotEventsPublisherDaemonSuite extends SimpleIOSuite {

  @derive(encoder)
  final case class TestEvent(value: String)

  private val noKeys: StateKeyExtractor[IO, TestEvent, Unit] =
    _ => Set.empty[Unit].pure[IO]

  test("Nakamoto publisher signs each accepted event, stores it in the mempool, and gossips it") {
    SecurityProvider.forAsync[IO].use { implicit securityProvider =>
      for {
        implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO]
        implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
        keyPair <- KeyPairGenerator.makeKeyPair[IO]
        spread <- Ref.of[IO, List[TestEvent]](Nil)
        mempool <- EventMempool.make[IO, TestEvent, Unit](noKeys, MempoolConfig(maxSize = 8))
        gossip = recordingGossip(spread)
        _ <- GlobalSnapshotEventsPublisherDaemon.signAndPublish(
          TestEvent("native-ingress"),
          keyPair,
          mempool,
          gossip,
          Slf4jLogger.getLogger[IO]
        )
        size <- mempool.size
        published <- spread.get
      } yield expect.all(size == 1, published == List(TestEvent("native-ingress")))
    }
  }

  private def recordingGossip(publishedRef: Ref[IO, List[TestEvent]]): Gossip[IO] =
    new Gossip[IO] {
      def spread[A: TypeTag: Encoder](rumorContent: A): IO[Unit] =
        publishedRef.update(rumorContent.asInstanceOf[Signed[TestEvent]].value :: _)

      def spreadCommon[A: TypeTag: Encoder](rumorContent: A): IO[Unit] =
        IO.raiseError(new AssertionError(s"unexpected common rumor: $rumorContent"))

      def spreadDirect[A: TypeTag: Encoder](rumorContent: A, targets: Set[PeerId]): IO[Unit] =
        IO.raiseError(new AssertionError(s"unexpected direct rumor to $targets: $rumorContent"))

      def setDirectPushFn(fn: Gossip.DirectPushFn[IO]): IO[Unit] = IO.unit

      def setSidecarPublishFn(fn: Gossip.SidecarPublishFn[IO]): IO[Unit] = IO.unit
    }
}
