package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import cats.effect.kernel.Async
import cats.effect.std.{Dispatcher, Queue}

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._

import fs2.Stream
import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver

/** Converts the sidecar's server-streaming Subscribe RPC into an fs2.Stream.
  *
  * Uses cats-effect Dispatcher to safely bridge gRPC's StreamObserver callbacks into the F effect system.
  */
object GossipStream {

  /** Subscribe to incoming gossip messages from the sidecar as an fs2.Stream. */
  def subscribe[F[_]: Async](channel: ManagedChannel): Stream[F, GossipMessage] = {
    val stub = SidecarServiceGrpc.stub(channel)

    Stream.resource(Dispatcher.sequential[F]).flatMap { dispatcher =>
      Stream.eval(Queue.unbounded[F, Option[GossipMessage]]).flatMap { queue =>
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

        val startSubscription: F[Unit] = Async[F].delay {
          stub.subscribe(
            SubscribeRequest(),
            new StreamObserver[GossipMessage] {
              override def onNext(value: GossipMessage): Unit =
                safeRun(queue.offer(Some(value)))

              override def onError(t: Throwable): Unit =
                safeRun(queue.offer(None))

              override def onCompleted(): Unit =
                safeRun(queue.offer(None))
            }
          )
        }

        Stream.eval(startSubscription) >> Stream.fromQueueNoneTerminated(queue)
      }
    }
  }
}
