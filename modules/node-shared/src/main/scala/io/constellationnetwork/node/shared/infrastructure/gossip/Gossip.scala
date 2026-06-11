package io.constellationnetwork.node.shared.infrastructure.gossip

import java.security.KeyPair

import cats.effect.std.{Queue, Supervisor}
import cats.effect.{Async, Ref}
import cats.syntax.all._

import scala.reflect.runtime.universe.TypeTag

import io.constellationnetwork.ext.cats.syntax.next._
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.node.shared.domain.gossip.{Gossip => GossipAlg}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.generation.Generation
import io.constellationnetwork.schema.gossip._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.{Hashed, Hasher, SecurityProvider}

import io.circe.Encoder
import io.circe.syntax._
import org.typelevel.log4cats.slf4j.Slf4jLogger

object Gossip {

  def make[F[_]: Async: SecurityProvider: Hasher: Metrics](
    rumorQueue: Queue[F, Hashed[RumorRaw]],
    selfId: PeerId,
    generation: Generation,
    keyPair: KeyPair
  )(implicit S: Supervisor[F]): F[GossipAlg[F]] =
    for {
      counter <- Ref.of[F, Counter](Counter.MinValue)
      directPushRef <- Ref.of[F, Option[GossipAlg.DirectPushFn[F]]](None)
      sidecarPublishRef <- Ref.of[F, Option[GossipAlg.SidecarPublishFn[F]]](None)
    } yield
      new GossipAlg[F] {

        private val rumorLogger = Slf4jLogger.getLoggerFromName[F](rumorLoggerName)

        def spread[A: TypeTag: Encoder](rumorContent: A): F[Unit] =
          for {
            contentJson <- rumorContent.asJson.pure[F]
            count <- counter.getAndUpdate(_.next)
            rumor = PeerRumorRaw(selfId, Ordinal(generation, count), contentJson, ContentType.of[A])
            _ <- signAndOffer(rumor)
          } yield ()

        def spreadCommon[A: TypeTag: Encoder](rumorContent: A): F[Unit] =
          for {
            contentJson <- rumorContent.asJson.pure[F]
            rumor = CommonRumorRaw(contentJson, ContentType.of[A])
            _ <- signAndOffer(rumor)
          } yield ()

        def spreadDirect[A: TypeTag: Encoder](rumorContent: A, targets: Set[PeerId]): F[Unit] =
          for {
            contentJson <- rumorContent.asJson.pure[F]
            count <- counter.getAndUpdate(_.next)
            rumor = PeerRumorRaw(selfId, Ordinal(generation, count), contentJson, ContentType.of[A])
            hashed <- signAndOfferReturn(rumor)
            maybeFn <- directPushRef.get
            _ <- maybeFn.traverse_(fn =>
              fn(hashed, targets.excl(selfId)).handleErrorWith(err => rumorLogger.warn(err)(s"Direct push failed, gossip will propagate"))
            )
          } yield ()

        def setDirectPushFn(fn: GossipAlg.DirectPushFn[F]): F[Unit] =
          directPushRef.set(fn.some)

        def setSidecarPublishFn(fn: GossipAlg.SidecarPublishFn[F]): F[Unit] =
          sidecarPublishRef.set(fn.some)

        private def signAndOffer(rumor: RumorRaw): F[Unit] =
          signAndOfferReturn(rumor).void

        private def signAndOfferReturn(rumor: RumorRaw): F[Hashed[RumorRaw]] =
          for {
            signedRumor <- rumor.sign(keyPair)
            hashedRumor <- signedRumor.toHashed
            // NEVER block the caller on a full rumor queue (2026-06-11, run b0xkqny6n post-mortem).
            // `spread` is called from many fibers — including, transitively, fibers in the
            // GossipDaemon CONSUMER pipeline (rumor handlers) and the l1-event publisher daemon.
            // A plain blocking `offer` on the bounded queue turns queue-full into a cluster-wide
            // self-deadlock: the consumer blocks offering into its own queue, the queue never
            // drains, and every other `spread` caller wedges behind it (observed: all 5 gl0
            // nodes' rumor publishing flatlined at 04:23:12 and never recovered, killing all
            // l1-event intake for the rest of the run). Local rumors must not DROP either
            // (own-rumor counters must stay gap-free), so fall back to a supervised background
            // offer: the caller proceeds immediately and the parked offer completes as soon as
            // the consumer frees a slot.
            offered <- rumorQueue.tryOffer(hashedRumor)
            _ <- if (offered) Async[F].unit else S.supervise(rumorQueue.offer(hashedRumor)).void
            _ <- metrics.updateRumorsSpread(signedRumor)
            _ <- logSpread(hashedRumor)
            maybeSidecarFn <- sidecarPublishRef.get
            _ <- maybeSidecarFn.traverse_ { fn =>
              fn(hashedRumor).handleErrorWith(err => rumorLogger.warn(err)(s"Sidecar publish failed, gossip will propagate"))
            }
          } yield hashedRumor

        private def logSpread(hashedRumor: Hashed[RumorRaw]): F[Unit] =
          rumorLogger.info(
            s"Rumor spread {hash=${hashedRumor.hash.show}, rumor=${hashedRumor.signed.value.show}"
          )

      }

}
