package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore._
import io.constellationnetwork.node.shared.domain.nakamoto.nipopow.{NipopowProofReadGate, NipopowProofUnavailable, TowerFinalizer}
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, HasherSelector}

/** Serialized, branch-relative catch-up for the local NIPoPoW tower.
  *
  * A [[TowerCatchupTarget]] is a caller-supplied branch, not a canonicality claim. The coordinator proves exact ancestry on that branch with
  * [[NakamotoChainStore.NakamotoChainStoreAlgebra.walkBackExact]], processes the oldest unprocessed suffix first, and advances its cursor only
  * after the corresponding tower operation succeeds. It never chooses between branches. A target behind or divergent from the exact cursor
  * requires an external clean-rebuild protocol.
  *
  * The cursor is intentionally distinct from `SettledOrdinalTracker`: the latter is legacy k2 telemetry and may advance independently of
  * tower work. This in-memory cursor is not crash durability; durable cursor/tower atomicity remains a recovery-layer responsibility.
  */
trait TowerCatchupCoordinator[F[_]] extends NipopowProofReadGate[F] {

  /** Close the internal proof-read gate under the same mutex used by [[advance]] and [[whenReady]]. A request which retained a provider
    * object before the external HTTP reference was withdrawn must still observe a non-ready state.
    */
  def withdrawProofReads: F[Unit]

  def advance(target: TowerCatchupTarget): F[TowerCatchupRun]

  def state: F[TowerCatchupState]

  def isReady: F[Boolean]
}

final case class TowerCatchupTarget(
  sourceTip: ExactWalkPosition,
  eligibleThrough: SnapshotOrdinal
)

final case class ResolvedTowerCatchupTarget(
  sourceTip: ExactWalkPosition,
  eligibleThrough: ExactWalkPosition
)

sealed trait TowerCatchupCursor extends Product with Serializable

object TowerCatchupCursor {
  case object BeforeFirst extends TowerCatchupCursor
  final case class Processed(position: ExactWalkPosition) extends TowerCatchupCursor
}

sealed trait TowerCatchupProcessOutcome extends Product with Serializable {
  def position: ExactWalkPosition
}

object TowerCatchupProcessOutcome {
  final case class ProcessedWithCertificate(position: ExactWalkPosition) extends TowerCatchupProcessOutcome
  final case class SkippedNoCertificate(position: ExactWalkPosition) extends TowerCatchupProcessOutcome
}

sealed trait TowerCatchupProcessError extends Product with Serializable {
  def position: ExactWalkPosition
  def description: String
  def retryable: Boolean
}

object TowerCatchupProcessError {
  final case class SnapshotUnavailable(position: ExactWalkPosition) extends TowerCatchupProcessError {
    val description: String = "exact snapshot bytes are unavailable"
    val retryable: Boolean = true
  }

  final case class SnapshotReadFailed(position: ExactWalkPosition, cause: String) extends TowerCatchupProcessError {
    val description: String = s"exact snapshot read failed: $cause"
    val retryable: Boolean = true
  }

  final case class FinalizerFailed(position: ExactWalkPosition, cause: String) extends TowerCatchupProcessError {
    val description: String = s"tower finalizer failed: $cause"
    val retryable: Boolean = true
  }

  final case class SignedOrdinalMismatch(position: ExactWalkPosition, actual: SnapshotOrdinal) extends TowerCatchupProcessError {
    val description: String = s"signed ordinal ${actual.value.value} does not match exact position ${position.ordinal.value.value}"
    val retryable: Boolean = false
  }

  final case class SignedParentMismatch(position: ExactWalkPosition, expected: io.constellationnetwork.security.hash.Hash)
      extends TowerCatchupProcessError {
    val description: String = s"signed parent does not match exact link ${expected.value}"
    val retryable: Boolean = false
  }

  final case class ContentHashFailed(position: ExactWalkPosition, cause: String) extends TowerCatchupProcessError {
    val description: String = s"exact snapshot rehash failed: $cause"
    val retryable: Boolean = false
  }

  final case class ContentHashMismatch(position: ExactWalkPosition, actual: io.constellationnetwork.security.hash.Hash)
      extends TowerCatchupProcessError {
    val description: String = s"rehash ${actual.value} does not match exact hash ${position.hash.value}"
    val retryable: Boolean = false
  }
}

/** Side-effect-free preparation of one exact link followed by one local tower commit. `commit` must either fail without publication or
  * complete all of its tower mutation before returning success. The coordinator masks that commit together with exact-cursor publication.
  */
final case class CommittedTowerCatchup[F[_]](
  outcome: TowerCatchupProcessOutcome,
  observe: F[Unit]
)

final case class PreparedTowerCatchup[F[_]](
  position: ExactWalkPosition,
  commit: F[Either[TowerCatchupProcessError, CommittedTowerCatchup[F]]]
)

trait TowerCatchupProcessor[F[_]] {
  def prepare(link: ExactWalkLink): F[Either[TowerCatchupProcessError, PreparedTowerCatchup[F]]]
}

sealed trait TowerCatchupWaitingReason extends Product with Serializable

object TowerCatchupWaitingReason {
  final case class ExactHistoryUnavailable(
    missing: ExactWalkPosition,
    reason: ExactWalkIncompleteReason
  ) extends TowerCatchupWaitingReason

  final case class ExactHistoryReadFailed(error: ExactWalkError.StorageReadFailed) extends TowerCatchupWaitingReason

  final case class ProcessingFailed(error: TowerCatchupProcessError) extends TowerCatchupWaitingReason

  final case class ProcessorRaised(position: ExactWalkPosition, cause: String) extends TowerCatchupWaitingReason
}

sealed trait TowerCatchupRebuildReason extends Product with Serializable

object TowerCatchupRebuildReason {
  final case class TargetBehindCursor(cursor: ExactWalkPosition, target: ExactWalkPosition) extends TowerCatchupRebuildReason
  final case class CursorNotOnTargetBranch(
    cursor: ExactWalkPosition,
    branchPosition: ExactWalkPosition,
    target: ExactWalkPosition
  ) extends TowerCatchupRebuildReason
}

sealed trait TowerCatchupRecoveryReason extends Product with Serializable

object TowerCatchupRecoveryReason {
  final case class ExactWalkRejected(error: ExactWalkError) extends TowerCatchupRecoveryReason
  final case class MalformedExactPath(description: String) extends TowerCatchupRecoveryReason
  final case class ProcessingRejected(error: TowerCatchupProcessError) extends TowerCatchupRecoveryReason
  final case class WalkRaised(cause: String) extends TowerCatchupRecoveryReason
}

sealed trait TowerCatchupState extends Product with Serializable {
  def cursor: TowerCatchupCursor
}

object TowerCatchupState {
  final case class Idle(
    cursor: TowerCatchupCursor,
    lastTarget: Option[ResolvedTowerCatchupTarget]
  ) extends TowerCatchupState

  final case class CatchingUp(
    cursor: TowerCatchupCursor,
    target: ResolvedTowerCatchupTarget
  ) extends TowerCatchupState

  final case class Waiting(
    cursor: TowerCatchupCursor,
    target: TowerCatchupTarget,
    reason: TowerCatchupWaitingReason
  ) extends TowerCatchupState

  final case class RebuildRequired(
    cursor: TowerCatchupCursor,
    target: ResolvedTowerCatchupTarget,
    reason: TowerCatchupRebuildReason
  ) extends TowerCatchupState

  final case class RecoveryRequired(
    cursor: TowerCatchupCursor,
    target: TowerCatchupTarget,
    reason: TowerCatchupRecoveryReason
  ) extends TowerCatchupState

  final case class Ready(
    cursor: TowerCatchupCursor,
    target: ResolvedTowerCatchupTarget
  ) extends TowerCatchupState
}

final case class TowerCatchupRun(
  state: TowerCatchupState,
  outcomes: Vector[TowerCatchupProcessOutcome]
)

object TowerCatchupCoordinator {
  import TowerCatchupCursor._
  import TowerCatchupProcessError._
  import TowerCatchupProcessOutcome._
  import TowerCatchupRecoveryReason._
  import TowerCatchupRebuildReason._
  import TowerCatchupState._
  import TowerCatchupWaitingReason._

  val DefaultMaxPerRun: Int = 100

  def make[F[_]: Async: HasherSelector](
    chainStore: NakamotoChainStoreAlgebra[F],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    towerFinalizer: TowerFinalizer[F],
    maxPerRun: Int = DefaultMaxPerRun
  ): F[TowerCatchupCoordinator[F]] =
    makeWithProcessor(
      chainStore.walkBackExact,
      snapshotProcessor(snapshotStorage, towerFinalizer),
      maxPerRun
    )

  private[nakamoto] def makeWithProcessor[F[_]: Async](
    walkBackExact: (ExactWalkPosition, SnapshotOrdinal, Int) => F[Either[ExactWalkError, ExactWalkResult]],
    processor: TowerCatchupProcessor[F],
    maxPerRun: Int = DefaultMaxPerRun
  ): F[TowerCatchupCoordinator[F]] =
    if (maxPerRun <= 0) Async[F].raiseError(new IllegalArgumentException(s"maxPerRun must be positive, got $maxPerRun"))
    else
      (
        Ref.of[F, TowerCatchupState](Idle(BeforeFirst, none)),
        Semaphore[F](1L)
      ).tupled.map { case (stateRef, mutex) =>
        new TowerCatchupCoordinator[F] {

          def state: F[TowerCatchupState] = stateRef.get

          def isReady: F[Boolean] = state.map {
            case _: Ready => true
            case _        => false
          }

          def withdrawProofReads: F[Unit] =
            mutex.permit.use { _ =>
              stateRef.update {
                case ready: Ready => Idle(ready.cursor, ready.target.some)
                case current      => current
              }
            }

          def whenReady[A](read: => F[A]): F[Either[NipopowProofUnavailable, A]] =
            mutex.permit.use { _ =>
              stateRef.get.flatMap {
                case _: Ready            => read.map(_.asRight[NipopowProofUnavailable])
                case _: RebuildRequired  => Async[F].pure(NipopowProofUnavailable.RebuildRequired.asLeft[A])
                case _: RecoveryRequired => Async[F].pure(NipopowProofUnavailable.RecoveryRequired.asLeft[A])
                case _                   => Async[F].pure(NipopowProofUnavailable.CatchupNotReady.asLeft[A])
              }
            }

          def advance(target: TowerCatchupTarget): F[TowerCatchupRun] =
            mutex.permit.use { _ =>
              stateRef.get.flatMap {
                case terminal: RebuildRequired  => Async[F].pure(TowerCatchupRun(terminal, Vector.empty))
                case terminal: RecoveryRequired => Async[F].pure(TowerCatchupRun(terminal, Vector.empty))
                case current                    => advanceFrom(current.cursor, target)
              }
            }

          private def advanceFrom(cursor: TowerCatchupCursor, target: TowerCatchupTarget): F[TowerCatchupRun] = {
            val sourceOrdinal = target.sourceTip.ordinal.value.value
            val eligibleOrdinal = target.eligibleThrough.value.value
            val cursorOrdinal = cursor match {
              case BeforeFirst         => 0L
              case Processed(position) => position.ordinal.value.value
            }
            val lowerOrdinal =
              if (eligibleOrdinal < cursorOrdinal) eligibleOrdinal
              else
                cursor match {
                  case BeforeFirst if eligibleOrdinal > 0L => 1L
                  case _                                   => cursorOrdinal
                }

            requiredSteps(sourceOrdinal, lowerOrdinal) match {
              case Left(description) =>
                publish(
                  RecoveryRequired(cursor, target, MalformedExactPath(description)),
                  Vector.empty
                )
              case Right(maxSteps) =>
                walkBackExact(target.sourceTip, SnapshotOrdinal.unsafeApply(lowerOrdinal), maxSteps).attempt.flatMap {
                  case Left(error) =>
                    publish(
                      RecoveryRequired(cursor, target, WalkRaised(renderCause(error))),
                      Vector.empty
                    )
                  case Right(Left(error: ExactWalkError.StorageReadFailed)) =>
                    publish(Waiting(cursor, target, ExactHistoryReadFailed(error)), Vector.empty)
                  case Right(Left(error)) =>
                    publish(RecoveryRequired(cursor, target, ExactWalkRejected(error)), Vector.empty)
                  case Right(Right(ExactWalkResult.Incomplete(_, missing, reason))) =>
                    publish(Waiting(cursor, target, ExactHistoryUnavailable(missing, reason)), Vector.empty)
                  case Right(Right(ExactWalkResult.Complete(path))) =>
                    handleCompletePath(cursor, target, SnapshotOrdinal.unsafeApply(lowerOrdinal), path)
                }
            }
          }

          private def handleCompletePath(
            cursor: TowerCatchupCursor,
            requested: TowerCatchupTarget,
            expectedLowerOrdinal: SnapshotOrdinal,
            pathNewestFirst: Vector[ExactWalkLink]
          ): F[TowerCatchupRun] =
            validatePath(requested.sourceTip, expectedLowerOrdinal, pathNewestFirst) match {
              case Left(description) =>
                publish(RecoveryRequired(cursor, requested, MalformedExactPath(description)), Vector.empty)
              case Right(_) =>
                pathNewestFirst.find(_.position.ordinal == requested.eligibleThrough) match {
                  case None =>
                    publish(
                      RecoveryRequired(
                        cursor,
                        requested,
                        MalformedExactPath(
                          s"exact path omitted eligible ordinal ${requested.eligibleThrough.value.value}"
                        )
                      ),
                      Vector.empty
                    )
                  case Some(eligibleLink) =>
                    val resolved = ResolvedTowerCatchupTarget(requested.sourceTip, eligibleLink.position)
                    cursor match {
                      case Processed(position) if requested.eligibleThrough.value.value < position.ordinal.value.value =>
                        publish(
                          RebuildRequired(cursor, resolved, TargetBehindCursor(position, eligibleLink.position)),
                          Vector.empty
                        )
                      case Processed(position) =>
                        pathNewestFirst.find(_.position.ordinal == position.ordinal) match {
                          case Some(onBranch) if onBranch.position != position =>
                            publish(
                              RebuildRequired(
                                cursor,
                                resolved,
                                CursorNotOnTargetBranch(position, onBranch.position, eligibleLink.position)
                              ),
                              Vector.empty
                            )
                          case None =>
                            publish(
                              RecoveryRequired(
                                cursor,
                                requested,
                                MalformedExactPath(s"exact path omitted cursor ordinal ${position.ordinal.value.value}")
                              ),
                              Vector.empty
                            )
                          case Some(_) => processSuffix(cursor, resolved, pathNewestFirst)
                        }
                      case BeforeFirst => processSuffix(cursor, resolved, pathNewestFirst)
                    }
                }
            }

          private def processSuffix(
            cursor: TowerCatchupCursor,
            target: ResolvedTowerCatchupTarget,
            pathNewestFirst: Vector[ExactWalkLink]
          ): F[TowerCatchupRun] = {
            val afterOrdinal = cursor match {
              case BeforeFirst         => 0L
              case Processed(position) => position.ordinal.value.value
            }
            val eligibleOrdinal = target.eligibleThrough.ordinal.value.value
            val suffix = pathNewestFirst.reverseIterator
              .filter { link =>
                val ordinal = link.position.ordinal.value.value
                ordinal > afterOrdinal && ordinal <= eligibleOrdinal
              }
              .take(maxPerRun)
              .toVector

            if (suffix.isEmpty) publish(Ready(cursor, target), Vector.empty)
            else
              publishState(CatchingUp(cursor, target)) >> processLinks(
                remaining = suffix.toList,
                cursor = cursor,
                target = target,
                outcomes = Vector.empty
              )
            }

          private def processLinks(
            remaining: List[ExactWalkLink],
            cursor: TowerCatchupCursor,
            target: ResolvedTowerCatchupTarget,
            outcomes: Vector[TowerCatchupProcessOutcome]
          ): F[TowerCatchupRun] =
            remaining match {
              case Nil =>
                cursor match {
                  case Processed(position) if position == target.eligibleThrough =>
                    publish(Ready(cursor, target), outcomes)
                  case _ => publish(Idle(cursor, target.some), outcomes)
                }
              case link :: tail =>
                processor.prepare(link).attempt.flatMap {
                  case Left(error) =>
                    publish(
                      Waiting(cursor, TowerCatchupTarget(target.sourceTip, target.eligibleThrough.ordinal), ProcessorRaised(link.position, renderCause(error))),
                      outcomes
                    )
                  case Right(Left(error)) if error.retryable =>
                    publish(
                      Waiting(cursor, TowerCatchupTarget(target.sourceTip, target.eligibleThrough.ordinal), ProcessingFailed(error)),
                      outcomes
                    )
                  case Right(Left(error)) =>
                    publish(
                      RecoveryRequired(
                        cursor,
                        TowerCatchupTarget(target.sourceTip, target.eligibleThrough.ordinal),
                        ProcessingRejected(error)
                      ),
                      outcomes
                    )
                  case Right(Right(prepared)) if prepared.position != link.position =>
                    publish(
                      RecoveryRequired(
                        cursor,
                        TowerCatchupTarget(target.sourceTip, target.eligibleThrough.ordinal),
                        MalformedExactPath(
                          s"prepared position ${prepared.position} does not match requested link ${link.position}"
                        )
                      ),
                      outcomes
                    )
                  case Right(Right(prepared)) =>
                    commitPrepared(prepared, link, cursor, target, outcomes).flatMap {
                      case Left(done) => Async[F].pure(done)
                      case Right((nextCursor, outcome)) => processLinks(tail, nextCursor, target, outcomes :+ outcome)
                    }
                }
            }

          /** Mask exactly the local tower mutation and matching exact-cursor publication. Preparation remains cancelable and performs no
            * mutation. A cancellation requested while `commit` runs is observed only after the cursor names the committed exact hash, so a
            * same-ordinal sibling cannot be mistaken for unprocessed work and skipped by the finalizer's ordinal de-duplication watermark.
            */
          private def commitPrepared(
            prepared: PreparedTowerCatchup[F],
            link: ExactWalkLink,
            cursor: TowerCatchupCursor,
            target: ResolvedTowerCatchupTarget,
            outcomes: Vector[TowerCatchupProcessOutcome]
          ): F[Either[TowerCatchupRun, (TowerCatchupCursor, TowerCatchupProcessOutcome)]] =
            Async[F].uncancelable { _ =>
              prepared.commit.attempt.flatMap {
                case Left(error) =>
                  publish(
                    Waiting(
                      cursor,
                      TowerCatchupTarget(target.sourceTip, target.eligibleThrough.ordinal),
                      ProcessorRaised(link.position, renderCause(error))
                    ),
                    outcomes
                  ).map(_.asLeft[(TowerCatchupCursor, CommittedTowerCatchup[F])])
                case Right(Left(error)) if error.retryable =>
                  publish(
                    Waiting(cursor, TowerCatchupTarget(target.sourceTip, target.eligibleThrough.ordinal), ProcessingFailed(error)),
                    outcomes
                  ).map(_.asLeft[(TowerCatchupCursor, CommittedTowerCatchup[F])])
                case Right(Left(error)) =>
                  publish(
                    RecoveryRequired(
                      cursor,
                      TowerCatchupTarget(target.sourceTip, target.eligibleThrough.ordinal),
                      ProcessingRejected(error)
                    ),
                    outcomes
                  ).map(_.asLeft[(TowerCatchupCursor, CommittedTowerCatchup[F])])
                case Right(Right(committed)) if committed.outcome.position != link.position =>
                  publish(
                    RecoveryRequired(
                      cursor,
                      TowerCatchupTarget(target.sourceTip, target.eligibleThrough.ordinal),
                      MalformedExactPath(
                        s"processor outcome ${committed.outcome.position} does not match requested link ${link.position}"
                      )
                    ),
                    outcomes
                  ).map(_.asLeft[(TowerCatchupCursor, CommittedTowerCatchup[F])])
                case Right(Right(committed)) =>
                  val nextCursor: TowerCatchupCursor = Processed(link.position)
                  publishState(CatchingUp(nextCursor, target)).as((nextCursor, committed).asRight[TowerCatchupRun])
              }
            }.flatMap {
              case Left(done) => Async[F].pure(done.asLeft[(TowerCatchupCursor, TowerCatchupProcessOutcome)])
              case Right((nextCursor, committed)) =>
                committed.observe.attempt.void.as((nextCursor, committed.outcome).asRight[TowerCatchupRun])
            }

          private def publish(next: TowerCatchupState, outcomes: Vector[TowerCatchupProcessOutcome]): F[TowerCatchupRun] =
            publishState(next).as(TowerCatchupRun(next, outcomes))

          private def publishState(next: TowerCatchupState): F[Unit] = stateRef.set(next)
        }
      }

  private def snapshotProcessor[F[_]: Async: HasherSelector](
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    towerFinalizer: TowerFinalizer[F]
  ): TowerCatchupProcessor[F] = new TowerCatchupProcessor[F] {
    def prepare(link: ExactWalkLink): F[Either[TowerCatchupProcessError, PreparedTowerCatchup[F]]] = {
      val position = link.position
      snapshotStorage.get(position.hash).attempt.flatMap {
        case Left(error) =>
          Async[F].pure(Left(SnapshotReadFailed(position, renderCause(error))))
        case Right(None) =>
          Async[F].pure(Left(SnapshotUnavailable(position)))
        case Right(Some(signed)) if signed.value.ordinal =!= position.ordinal =>
          Async[F].pure(Left(SignedOrdinalMismatch(position, signed.value.ordinal)))
        case Right(Some(signed)) if signed.value.lastSnapshotHash =!= link.parentHash =>
          Async[F].pure(Left(SignedParentMismatch(position, link.parentHash)))
        case Right(Some(signed)) =>
          rehash(position, signed).flatMap {
            case Left(error) => Async[F].pure(Left(error))
            case Right(_) if signed.value.slotCertificate.isEmpty =>
              Async[F].pure(
                Right(
                  PreparedTowerCatchup(
                    position,
                    Async[F].pure(
                      Right(CommittedTowerCatchup(SkippedNoCertificate(position), Async[F].unit))
                    )
                  )
                )
              )
            case Right(hashed) =>
              towerFinalizer.prepare(hashed).attempt.map {
                case Left(error) => Left(FinalizerFailed(position, renderCause(error)))
                case Right(prepared) =>
                  Right(
                    PreparedTowerCatchup(
                      position,
                      prepared.commit.attempt.map {
                        case Left(error) => Left(FinalizerFailed(position, renderCause(error)))
                        case Right(level0Count) =>
                          Right(
                            CommittedTowerCatchup(
                              ProcessedWithCertificate(position),
                              level0Count.fold(Async[F].unit)(prepared.observe)
                            )
                          )
                      }
                    )
                  )
              }
          }
      }
    }

    private def rehash(
      position: ExactWalkPosition,
      signed: Signed[GlobalIncrementalSnapshot]
    ): F[Either[TowerCatchupProcessError, io.constellationnetwork.security.Hashed[GlobalIncrementalSnapshot]]] = {
      implicit val hasher: Hasher[F] = HasherSelector[F].getForOrdinal(position.ordinal)
      signed.toHashed[F].attempt.map {
        case Left(error) => Left(ContentHashFailed(position, renderCause(error)))
        case Right(hashed) if hashed.hash =!= position.hash => Left(ContentHashMismatch(position, hashed.hash))
        case Right(hashed)                                => Right(hashed)
      }
    }
  }

  private def validatePath(
    sourceTip: ExactWalkPosition,
    expectedLowerOrdinal: SnapshotOrdinal,
    pathNewestFirst: Vector[ExactWalkLink]
  ): Either[String, Unit] =
    pathNewestFirst.headOption match {
      case None => Left("exact walk returned an empty complete path")
      case Some(head) if head.position != sourceTip =>
        Left(s"exact path head ${head.position} does not match source tip $sourceTip")
      case Some(_) if pathNewestFirst.last.position.ordinal =!= expectedLowerOrdinal =>
        Left(
          s"exact path ended at ${pathNewestFirst.last.position.ordinal.value.value}, expected ${expectedLowerOrdinal.value.value}"
        )
      case Some(_) =>
        pathNewestFirst.zip(pathNewestFirst.drop(1)).toList.traverse_ {
          case (newer, older) =>
            val expectedNewerOrdinal = older.position.ordinal.value.value + 1L
            if (newer.position.ordinal.value.value =!= expectedNewerOrdinal)
              Left(
                s"non-contiguous exact path ${newer.position.ordinal.value.value} -> ${older.position.ordinal.value.value}"
              )
            else if (newer.parentHash =!= older.position.hash)
              Left(s"exact path parent ${newer.parentHash.value} does not match ${older.position.hash.value}")
            else Right(())
        }
    }

  private def requiredSteps(sourceOrdinal: Long, targetOrdinal: Long): Either[String, Int] = {
    val required = BigInt(sourceOrdinal) - BigInt(targetOrdinal) + 1
    if (required <= 0)
      Left(s"source tip ordinal $sourceOrdinal is below required ordinal $targetOrdinal")
    else if (required > BigInt(Int.MaxValue))
      Left(s"exact walk requires $required steps, exceeding Int.MaxValue")
    else Right(required.toInt)
  }

  private def renderCause(error: Throwable): String =
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getName)
}
