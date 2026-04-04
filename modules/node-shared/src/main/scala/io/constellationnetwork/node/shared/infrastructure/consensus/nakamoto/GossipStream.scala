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
        val startSubscription: F[Unit] = Async[F].delay {
          stub.subscribe(
            SubscribeRequest(),
            new StreamObserver[GossipMessage] {
              override def onNext(value: GossipMessage): Unit =
                dispatcher.unsafeRunAndForget(queue.offer(Some(value)))

              override def onError(t: Throwable): Unit =
                dispatcher.unsafeRunAndForget(queue.offer(None))

              override def onCompleted(): Unit =
                dispatcher.unsafeRunAndForget(queue.offer(None))
            }
          )
        }

        Stream.eval(startSubscription) >> Stream.fromQueueNoneTerminated(queue)
      }
    }
  }
}
