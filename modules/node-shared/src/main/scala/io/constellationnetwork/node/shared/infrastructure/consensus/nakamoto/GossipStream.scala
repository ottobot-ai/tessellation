package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import java.util.concurrent.atomic.AtomicReference

import cats.effect.kernel.Async
import cats.effect.std.{Dispatcher, Queue}
import cats.effect.syntax.all._
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._

import fs2.{Pull, Stream}
import io.grpc.ManagedChannel
import io.grpc.stub.{ClientCallStreamObserver, ClientResponseObserver}

/** Converts the sidecar's server-streaming Subscribe RPC into an fs2.Stream.
  *
  * Uses cats-effect Dispatcher to safely bridge gRPC's StreamObserver callbacks into the F effect system.
  *
  * '''Topic filter (FINDING-F1).''' Every subscription MUST declare the message families it consumes ([[SidecarClient.SubscribeTopics]]):
  * the sidecar's shard-checkpoint families ride SHARED node-lifetime fan-in channels that tolerate exactly ONE drainer — two subscribe-all
  * streams race-drained them and the rumor bridge silently discarded the shard checkpoints it won (~half). The parameter is deliberately
  * required (no default): an accidental subscribe-all is exactly the bug this fixes. The sidecar validates labels and fails the stream on
  * an unknown one.
  *
  * '''Cancel-on-close (zombie-stream guard).''' The underlying gRPC call is cancelled when the fs2 stream's scope closes. Without this, a
  * JVM-side stream failure (e.g. an `evalMap` error triggering the caller's reconnect) leaked the old server-side subscription, which kept
  * draining the shared shard channels while its client-side observer dropped everything — the same F1 race re-entering through a single
  * logical consumer.
  */
object GossipStream {

  final case class SubscriptionProtocolError(detail: String) extends IllegalStateException(s"Invalid sidecar Subscribe handshake: $detail")

  /** Subscribe to incoming gossip messages from the sidecar as an fs2.Stream, filtered to `topics` (labels from
    * [[SidecarClient.SubscribeTopics]]).
    */
  def subscribe[F[_]: Async](
    channel: ManagedChannel,
    profile: SidecarClient.SubscriptionProfile,
    readiness: SidecarSubscriptionReadiness[F]
  ): Stream[F, GossipMessage] = {
    val stub = SidecarServiceGrpc.stub(channel)

    Stream.bracket(readiness.begin(profile.lane))(readiness.release).flatMap { attempt =>
      Stream.resource(Dispatcher.sequential[F]).flatMap { dispatcher =>
        Stream.eval(Queue.unbounded[F, Either[Throwable, Option[GossipMessage]]]).flatMap { queue =>
          // Guard `unsafeRunAndForget` against the case where the cats-effect Dispatcher
          // resource has already been released by the time gRPC fires its callbacks. The gRPC
          // StreamObserver lifecycle is independent of the fs2 Stream's resource scope: when the
          // outer stream finishes (or the channel is shut down), the Dispatcher closes first and
          // gRPC may then deliver `onError`/`onCompleted` afterwards, throwing
          // IllegalStateException: Dispatcher already closed. The state is correct — the
          // consumer has already moved on — so we swallow it as expected shutdown noise.
          def safeRun(action: F[Unit]): Unit =
            try dispatcher.unsafeRunAndForget(action)
            catch { case _: IllegalStateException => () }

          // ClientResponseObserver (vs plain StreamObserver) so gRPC hands us the underlying
          // call handle before the RPC starts — that handle is what lets the bracket below
          // CANCEL the server-side stream when this fs2 scope closes.
          val callRef = new AtomicReference[Option[ClientCallStreamObserver[SubscribeRequest]]](None)
          val observer = new ClientResponseObserver[SubscribeRequest, GossipMessage] {
            override def beforeStart(requestStream: ClientCallStreamObserver[SubscribeRequest]): Unit =
              callRef.set(Some(requestStream))

            override def onNext(value: GossipMessage): Unit =
              safeRun(queue.offer(Right(Some(value))))

            override def onError(t: Throwable): Unit =
              safeRun(readiness.release(attempt).guarantee(queue.offer(Left(t))))

            override def onCompleted(): Unit =
              safeRun(readiness.release(attempt).guarantee(queue.offer(Right(None))))
          }

          val startSubscription: F[Unit] =
            validateProfile[F](profile) >> Async[F].delay {
              stub.subscribe(SubscribeRequest(topics = profile.topics, role = profile.role), observer)
            }

          // Cancelling an already-terminated call is a permitted no-op in grpc-java, so the
          // release is unconditional; `voidError` swallows any raciness with channel shutdown.
          val cancelSubscription: F[Unit] = Async[F].delay {
            callRef.get().foreach(_.cancel("subscriber stream scope closed", null))
          }.voidError

          val responses = Stream.bracket(startSubscription)(_ => cancelSubscription) >> Stream
            .fromQueueUnterminated(queue)
            .evalMap(_.liftTo[F])
            .takeWhile(_.nonEmpty)
            .unNone

          validateStartedFirst(responses, profile, attempt, readiness)
        }
      }
    }
  }

  private def validateProfile[F[_]: Async](profile: SidecarClient.SubscriptionProfile): F[Unit] =
    Async[F]
      .raiseError[Unit](SubscriptionProtocolError("topics must be explicit, nonempty, and distinct"))
      .unlessA(
        profile.topics.nonEmpty && profile.topics.distinct.size === profile.topics.size
      ) >>
      Async[F]
        .raiseError[Unit](SubscriptionProtocolError("subscriber role must be explicit"))
        .whenA(profile.role.isSubscriberRoleUnspecified || profile.role.isUnrecognized)

  private def validateStartedFirst[F[_]: Async](
    responses: Stream[F, GossipMessage],
    profile: SidecarClient.SubscriptionProfile,
    attempt: SidecarSubscriptionReadiness.Attempt,
    readiness: SidecarSubscriptionReadiness[F]
  ): Stream[F, GossipMessage] = {
    def rejectLaterStarted(message: GossipMessage): F[GossipMessage] = message.body match {
      case _: GossipMessage.Body.Started =>
        Async[F].raiseError(SubscriptionProtocolError("SubscribeStarted appeared more than once"))
      case _ => message.pure[F]
    }

    responses.pull.uncons1.flatMap {
      case None => Pull.raiseError(SubscriptionProtocolError("stream ended before SubscribeStarted"))
      case Some((first, tail)) =>
        first.body match {
          case GossipMessage.Body.Started(started) =>
            Pull.eval(
              validateStarted(profile, started) >> readiness.acknowledge(
                attempt,
                SidecarSubscriptionReadiness.Acknowledgement(started.sidecarSessionId, started.streamGeneration)
              )
            ) >> tail.evalMap(rejectLaterStarted).pull.echo
          case other =>
            Pull.raiseError(SubscriptionProtocolError(s"first response was ${other.getClass.getSimpleName}, not SubscribeStarted"))
        }
    }.stream
  }

  private def validateStarted[F[_]: Async](
    profile: SidecarClient.SubscriptionProfile,
    started: SubscribeStarted
  ): F[Unit] =
    Async[F]
      .raiseError[Unit](
        SubscriptionProtocolError(s"role mismatch: requested=${profile.role.name}, acknowledged=${started.role.name}")
      )
      .unlessA(started.role.value === profile.role.value) >>
      Async[F]
        .raiseError[Unit](
          SubscriptionProtocolError(
            s"topic mismatch: requested=${profile.topics.mkString("[", ",", "]")}, acknowledged=${started.topics.mkString("[", ",", "]")}"
          )
        )
        .unlessA(started.topics === profile.topics) >>
      Async[F]
        .raiseError[Unit](SubscriptionProtocolError("sidecar_session_id was empty"))
        .whenA(started.sidecarSessionId.trim.isEmpty) >>
      Async[F]
        .raiseError[Unit](SubscriptionProtocolError("stream_generation must be positive"))
        .whenA(started.streamGeneration <= 0L)
}
