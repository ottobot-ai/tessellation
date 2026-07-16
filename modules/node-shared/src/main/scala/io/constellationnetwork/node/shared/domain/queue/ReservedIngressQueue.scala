package io.constellationnetwork.node.shared.domain.queue

import cats.effect.Ref
import cats.effect.kernel.{Concurrent, Resource}
import cats.effect.std.Queue
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._

trait ReservedIngressQueue[F[_], A] {
  import ReservedIngressQueue.{OfferResult, Usage}

  def tryOffer(value: A, retainedBytes: Long): F[OfferResult]

  /** Holds the item's byte and item reservation until `process` succeeds, fails, or is canceled. */
  def takeAndUse[B](process: A => F[B]): F[B]

  def usage: F[Usage]
}

object ReservedIngressQueue {

  final case class Limits(maxItems: Int, maxBytes: Long, maxItemBytes: Long)
  final case class Usage(outstandingItems: Int, outstandingBytes: Long)

  sealed trait OfferResult

  object OfferResult {
    case object Accepted extends OfferResult
    final case class InvalidRetainedBytes(actual: Long) extends OfferResult
    final case class ItemTooLarge(actual: Long, maximum: Long) extends OfferResult
    final case class ItemCapacityExceeded(maximum: Int) extends OfferResult
    final case class ByteCapacityExceeded(actual: Long, available: Long, maximum: Long) extends OfferResult
  }

  private final case class Pending[F[_], A](value: A, retainedBytes: Long, released: Ref[F, Boolean])

  def bounded[F[_]: Concurrent, A](limits: Limits): F[ReservedIngressQueue[F, A]] =
    for {
      _ <- validateLimits[F](limits)
      queue <- Queue.bounded[F, Pending[F, A]](limits.maxItems)
      usageRef <- Ref.of[F, Usage](Usage(0, 0L))
    } yield new ReservedIngressQueue[F, A] {

      def tryOffer(value: A, retainedBytes: Long): F[OfferResult] =
        if (retainedBytes <= 0L) OfferResult.InvalidRetainedBytes(retainedBytes).pure[F].widen[OfferResult]
        else if (retainedBytes > limits.maxItemBytes)
          OfferResult.ItemTooLarge(retainedBytes, limits.maxItemBytes).pure[F].widen[OfferResult]
        else
          Concurrent[F].uncancelable { _ =>
            Ref.of[F, Boolean](false).flatMap { released =>
              val pending = Pending(value, retainedBytes, released)

              usageRef
                .modify { current =>
                  if (current.outstandingItems >= limits.maxItems)
                    current -> Left(OfferResult.ItemCapacityExceeded(limits.maxItems): OfferResult)
                  else {
                    val availableBytes = limits.maxBytes - current.outstandingBytes
                    if (retainedBytes > availableBytes)
                      current -> Left(
                        OfferResult.ByteCapacityExceeded(retainedBytes, availableBytes, limits.maxBytes): OfferResult
                      )
                    else
                      Usage(
                        current.outstandingItems + 1,
                        current.outstandingBytes + retainedBytes
                      ) -> Right(())
                  }
                }
                .flatMap {
                  case Left(rejection) => rejection.pure[F]
                  case Right(()) =>
                    queue.tryOffer(pending).flatMap {
                      case true => OfferResult.Accepted.pure[F].widen[OfferResult]
                      case false =>
                        release(pending) >> Concurrent[F].raiseError[OfferResult](
                          new IllegalStateException(
                            s"Reserved ingress queue accounting diverged after reservation: limits=$limits"
                          )
                        )
                    }
                }
            }
          }

      def takeAndUse[B](process: A => F[B]): F[B] =
        Resource
          .makeFull[F, Pending[F, A]](poll => poll(queue.take))(release)
          .use(pending => process(pending.value))

      def usage: F[Usage] = usageRef.get

      private def release(pending: Pending[F, A]): F[Unit] =
        Concurrent[F].uncancelable { _ =>
          pending.released.modify {
            case true  => true -> false
            case false => true -> true
          }.flatMap {
            case false => Concurrent[F].unit
            case true =>
              usageRef
                .modify { current =>
                  val next = Usage(
                    current.outstandingItems - 1,
                    current.outstandingBytes - pending.retainedBytes
                  )
                  if (next.outstandingItems < 0 || next.outstandingBytes < 0L)
                    current -> Left(
                      new IllegalStateException(
                        s"Reserved ingress queue released more capacity than admitted: current=$current pendingBytes=${pending.retainedBytes}"
                      ): Throwable
                    )
                  else next -> Right(())
                }
                .flatMap {
                  case Left(error) => Concurrent[F].raiseError[Unit](error)
                  case Right(())   => Concurrent[F].unit
                }
          }
        }
    }

  private def validateLimits[F[_]: Concurrent](limits: Limits): F[Unit] =
    Concurrent[F].raiseWhen(limits.maxItems <= 0)(
      new IllegalArgumentException(s"maxItems must be positive: ${limits.maxItems}")
    ) >>
      Concurrent[F].raiseWhen(limits.maxBytes <= 0L)(
        new IllegalArgumentException(s"maxBytes must be positive: ${limits.maxBytes}")
      ) >>
      Concurrent[F].raiseWhen(limits.maxItemBytes <= 0L || limits.maxItemBytes > limits.maxBytes)(
        new IllegalArgumentException(
          s"maxItemBytes must be positive and <= maxBytes: item=${limits.maxItemBytes} total=${limits.maxBytes}"
        )
      )
}
