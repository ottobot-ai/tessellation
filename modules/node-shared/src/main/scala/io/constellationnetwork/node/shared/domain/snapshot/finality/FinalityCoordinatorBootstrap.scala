package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityDurableCasResult.{AlreadyInstalled, Installed}
import io.constellationnetwork.security.mpt.{DurableMptImageStore, MptActivePublication}

sealed trait FinalityCoordinatorBootstrapStatus extends Product with Serializable

object FinalityCoordinatorBootstrapStatus {

  /** The two local journals matched while the MPT publication lease was held. This is not activation or canonicality authority. */
  case object LocalPublicationBound extends FinalityCoordinatorBootstrapStatus
  final case class RecoveryRequired(record: RecoveryRecordPointer) extends FinalityCoordinatorBootstrapStatus
}

sealed abstract class FinalityCoordinatorBootstrapError(message: String) extends RuntimeException(message)

object FinalityCoordinatorBootstrapError {
  final case class KernelRejected(error: FinalityCoordinatorKernelError)
      extends FinalityCoordinatorBootstrapError(s"Finality coordinator bootstrap mutation was rejected: $error")

  final case class UnexpectedCoordinatorMode(mode: CoordinatorMode)
      extends FinalityCoordinatorBootstrapError(s"Finality coordinator bootstrap observed an unexpected mode: $mode")
}

/** Runtime-dark startup boundary between the exact durable MPT publication journal and the local finality coordinator journal.
  *
  * The outer MPT lease remains held while every finality-head read, initialization, or recovery CAS runs, establishing the only permitted
  * lock order: MPT publication, then finality head. This boundary does not inspect the MPT entries, authenticate canonicality, authorize a
  * finality transition, select a branch, or initialize the effect outbox.
  */
object FinalityCoordinatorBootstrap {
  import FinalityCoordinatorBootstrapError._
  import FinalityCoordinatorBootstrapStatus._

  def bootstrap[F[_]: Async](
    mpt: DurableMptImageStore[F],
    finality: FinalityDurableStore[F]
  ): F[FinalityCoordinatorBootstrapStatus] =
    mpt.withVerifiedActivePublication { verifiedPublication =>
      finality.coordinatorHead.flatMap {
        case None => FinalityCoordinatorKernel.initializeDurably(verifiedPublication, finality).as(LocalPublicationBound)
        case Some(coordinator) =>
          coordinator.value.mode match {
            case CoordinatorMode.Running =>
              verifiedPublication.read.flatMap { activePublication =>
                if (coordinator.value.publication == activePublication) Async[F].pure(LocalPublicationBound)
                else enterPublicationMismatchRecovery(finality, coordinator, activePublication)
              }
            case CoordinatorMode.RecoveryRequired(record) => Async[F].pure(RecoveryRequired(record))
          }
      }
    }

  private def enterPublicationMismatchRecovery[F[_]: Async](
    finality: FinalityDurableStore[F],
    coordinator: DurablyVerifiedCoordinatorHead,
    activePublication: MptActivePublication
  ): F[FinalityCoordinatorBootstrapStatus] =
    for {
      digest <- Async[F].fromEither(
        FinalityIdentity
          .publicationMismatchDigest(coordinator.value.publication, activePublication)
          .leftMap(error => KernelRejected(FinalityCoordinatorKernelError.IdentityDerivationFailed(error)))
      )
      mutation <- Async[F].fromEither(
        FinalityCoordinatorKernel
          .enterRecovery(coordinator.value, RecoveryReason.PublicationMismatch(activePublication, digest))
          .leftMap(KernelRejected)
      )
      installed <- finality.compareAndSetCoordinator(coordinator, mutation)
      record <- casValue(installed).value.mode match {
        case CoordinatorMode.RecoveryRequired(pointer) => Async[F].pure(pointer)
        case mode                                      => Async[F].raiseError[RecoveryRecordPointer](UnexpectedCoordinatorMode(mode))
      }
    } yield RecoveryRequired(record)

  private def casValue[A](result: FinalityDurableCasResult[A]): A =
    result match {
      case Installed(value)        => value
      case AlreadyInstalled(value) => value
    }
}
