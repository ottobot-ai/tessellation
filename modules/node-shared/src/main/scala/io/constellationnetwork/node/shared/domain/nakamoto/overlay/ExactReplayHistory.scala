package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.data.NonEmptyList
import cats.effect.Concurrent
import cats.effect.kernel.Ref
import cats.effect.std.Semaphore
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.util.control.NoStackTrace

import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot

/** Exact content-addressed position requested from a replay-history source.
  *
  * Ordinal is carried because retained snapshot stores may need it for a hash-checked disk fallback. It never authorizes an ordinal-only
  * lookup: a source must return the artifact whose content hash equals `hash`, or a typed failure.
  */
final case class ExactReplayHistoryPosition(hash: Hash, ordinal: SnapshotOrdinal)

/** Session work bounds. `maxSteps` bounds the retained path and counts the anchor; `maxUniqueTargets` bounds each request batch. */
final case class ExactReplayHistoryBounds(maxSteps: Int, maxUniqueTargets: Int)

/** Structural source used by the dark replay-history session.
  *
  * The source is responsible for content-rehashing returned signed bytes under the snapshot ordinal's hash era. Implementing this trait or
  * obtaining a session proves neither authentication, execution, canonical selection, Phase 2, nor economic-state validity.
  */
trait ExactReplayHistorySource[F[_]] {
  def fetchExact(
    position: ExactReplayHistoryPosition
  ): F[Either[ExactReplayHistoryFailure, Hashed[GlobalIncrementalSnapshot]]]
}

sealed abstract class ExactReplayHistoryFailure(message: String)
    extends RuntimeException(message)
    with NoStackTrace
    with Product
    with Serializable

sealed trait ExactReplayHistoryUnavailable { self: ExactReplayHistoryFailure => }
sealed trait ExactReplayHistoryCorrupt { self: ExactReplayHistoryFailure => }
sealed trait ExactReplayHistoryNotAncestor { self: ExactReplayHistoryFailure => }
sealed trait ExactReplayHistoryInvalid { self: ExactReplayHistoryFailure => }
sealed trait ExactReplayHistoryBoundsFailure { self: ExactReplayHistoryFailure => }
sealed trait ExactReplayHistoryStorageFailure { self: ExactReplayHistoryFailure => }

object ExactReplayHistoryFailure {
  final case class InvalidBounds(bounds: ExactReplayHistoryBounds)
      extends ExactReplayHistoryFailure(s"Exact replay-history bounds must be positive: $bounds")
      with ExactReplayHistoryInvalid
      with ExactReplayHistoryBoundsFailure

  final case class InvalidReference(reference: GlobalSnapshotStateRef, field: String, value: Hash)
      extends ExactReplayHistoryFailure(s"Exact replay-history reference has invalid $field '${value.value}': $reference")
      with ExactReplayHistoryInvalid

  final case class TargetAboveAnchor(anchor: GlobalSnapshotStateRef, target: SnapshotOrdinal)
      extends ExactReplayHistoryFailure(
        s"Exact replay-history target ${target.value.value} is above anchor ${anchor.ordinal.value.value}"
      )
      with ExactReplayHistoryInvalid
      with ExactReplayHistoryNotAncestor

  final case class TooManyUniqueTargets(actual: Int, maximum: Int)
      extends ExactReplayHistoryFailure(s"Exact replay-history target count $actual exceeds maximum $maximum")
      with ExactReplayHistoryBoundsFailure

  final case class TraversalLimitExceeded(
    anchor: GlobalSnapshotStateRef,
    target: SnapshotOrdinal,
    requiredSteps: BigInt,
    maximum: Int
  ) extends ExactReplayHistoryFailure(
        s"Exact replay-history from ${anchor.ordinal.value.value} to ${target.value.value} requires $requiredSteps steps, maximum=$maximum"
      )
      with ExactReplayHistoryBoundsFailure

  final case class ConflictingTargetsAtOrdinal(
    ordinal: SnapshotOrdinal,
    first: GlobalSnapshotStateRef,
    second: GlobalSnapshotStateRef
  ) extends ExactReplayHistoryFailure(s"Two different exact replay-history targets claim ordinal ${ordinal.value.value}")
      with ExactReplayHistoryInvalid
      with ExactReplayHistoryNotAncestor

  final case class ArtifactUnavailable(position: ExactReplayHistoryPosition)
      extends ExactReplayHistoryFailure(s"Exact replay-history artifact is unavailable at $position")
      with ExactReplayHistoryUnavailable

  final case class SameOrdinalSibling(position: ExactReplayHistoryPosition, foundHash: Hash)
      extends ExactReplayHistoryFailure(
        s"Exact replay-history found sibling ${foundHash.value} instead of ${position.hash.value} at ${position.ordinal.value.value}"
      )
      with ExactReplayHistoryUnavailable
      with ExactReplayHistoryNotAncestor

  final case class SourceHashEraUnavailable(position: ExactReplayHistoryPosition, detail: String)
      extends ExactReplayHistoryFailure(s"Exact replay-history hash era unavailable at $position: $detail")
      with ExactReplayHistoryUnavailable

  final case class SourceVerificationUnavailable(position: ExactReplayHistoryPosition, detail: String)
      extends ExactReplayHistoryFailure(s"Exact replay-history verification unavailable at $position: $detail")
      with ExactReplayHistoryUnavailable

  final case class SourceReadFailed(position: ExactReplayHistoryPosition, detail: String)
      extends ExactReplayHistoryFailure(s"Exact replay-history source read failed at $position: $detail")
      with ExactReplayHistoryUnavailable
      with ExactReplayHistoryStorageFailure

  final case class SourceReadCancelled(position: ExactReplayHistoryPosition)
      extends ExactReplayHistoryFailure(s"Exact replay-history source read was cancelled at $position")
      with ExactReplayHistoryUnavailable
      with ExactReplayHistoryStorageFailure

  final case class SourceCorrupt(position: ExactReplayHistoryPosition, detail: String)
      extends ExactReplayHistoryFailure(s"Exact replay-history source is corrupt at $position: $detail")
      with ExactReplayHistoryCorrupt

  final case class ReturnedArtifactMismatch(
    expected: ExactReplayHistoryPosition,
    observedHash: Hash,
    observedOrdinal: SnapshotOrdinal
  ) extends ExactReplayHistoryFailure(
        s"Exact replay-history source returned hash=${observedHash.value} ordinal=${observedOrdinal.value.value} for $expected"
      )
      with ExactReplayHistoryCorrupt
      with ExactReplayHistoryNotAncestor

  final case class MissingCommittedRoot(position: ExactReplayHistoryPosition)
      extends ExactReplayHistoryFailure(s"Exact replay-history artifact at $position has no committed MPT root")
      with ExactReplayHistoryUnavailable

  final case class InvalidCommittedRoot(position: ExactReplayHistoryPosition, root: Hash)
      extends ExactReplayHistoryFailure(s"Exact replay-history artifact at $position has invalid MPT root '${root.value}'")
      with ExactReplayHistoryCorrupt

  final case class InvalidArtifactParent(position: ExactReplayHistoryPosition, parent: Hash)
      extends ExactReplayHistoryFailure(s"Exact replay-history artifact at $position has invalid parent '${parent.value}'")
      with ExactReplayHistoryCorrupt

  final case class BrokenParentLink(child: GlobalSnapshotStateRef, parent: GlobalSnapshotStateRef)
      extends ExactReplayHistoryFailure(
        s"Exact replay-history parent link is broken: child=${child.hash.value} parent=${parent.hash.value}"
      )
      with ExactReplayHistoryCorrupt
      with ExactReplayHistoryNotAncestor

  final case class AncestryCycle(anchor: GlobalSnapshotStateRef, repeated: ExactReplayHistoryPosition)
      extends ExactReplayHistoryFailure(
        s"Exact replay-history from ${anchor.hash.value} repeats ${repeated.hash.value} at ${repeated.ordinal.value.value}"
      )
      with ExactReplayHistoryCorrupt
      with ExactReplayHistoryNotAncestor

  final case class PrematureChainRoot(child: GlobalSnapshotStateRef, requested: SnapshotOrdinal)
      extends ExactReplayHistoryFailure(
        s"Exact replay-history reached chain root ${child.ordinal.value.value} before requested ${requested.value.value}"
      )
      with ExactReplayHistoryUnavailable
      with ExactReplayHistoryNotAncestor

  final case class SessionArtifactMissing(anchor: GlobalSnapshotStateRef, requested: SnapshotOrdinal)
      extends ExactReplayHistoryFailure(
        s"Exact replay-history session at ${anchor.hash.value} has no resolved artifact at ${requested.value.value}"
      )
      with ExactReplayHistoryInvalid

  final case class RequestedReferenceNotAncestor(
    requested: GlobalSnapshotStateRef,
    observed: GlobalSnapshotStateRef
  ) extends ExactReplayHistoryFailure(
        s"Requested exact replay-history reference ${requested.hash.value} is not the anchor ancestor ${observed.hash.value}"
      )
      with ExactReplayHistoryNotAncestor

  final case class AnchorMismatch(expected: GlobalSnapshotStateRef, observed: GlobalSnapshotStateRef)
      extends ExactReplayHistoryFailure(
        s"Exact replay-history artifact $observed does not match caller-supplied anchor $expected"
      )
      with ExactReplayHistoryInvalid
      with ExactReplayHistoryNotAncestor
}

/** One source-rehashed snapshot and the complete state reference derived from its signed body.
  *
  * This is structural history, not an execution/finality capability. Only the sealed session issuer in this file can construct it.
  */
sealed abstract class ExactReplayHistoryArtifact private[overlay] (
  val reference: GlobalSnapshotStateRef,
  val snapshot: Hashed[GlobalIncrementalSnapshot]
) {
  override def equals(other: Any): Boolean = other match {
    case that: ExactReplayHistoryArtifact => reference == that.reference && snapshot == that.snapshot
    case _                                => false
  }

  override def hashCode(): Int = 31 * reference.## + snapshot.##

  override def toString: String = s"ExactReplayHistoryArtifact(reference=$reference)"
}

/** Requested artifacts returned oldest first. Intermediate ancestors are retained inside the session but omitted unless requested. */
sealed abstract class ExactReplayHistoryBatch private[overlay] (
  val anchor: GlobalSnapshotStateRef,
  val artifactsOldestFirst: Vector[ExactReplayHistoryArtifact]
) {
  lazy val byOrdinal: SortedMap[SnapshotOrdinal, ExactReplayHistoryArtifact] =
    SortedMap.from(artifactsOldestFirst.iterator.map(artifact => artifact.reference.ordinal -> artifact))
}

/** Opaque, non-serializable session over one exact anchor.
  *
  * The session stabilizes structural reads across retry paths. It is intentionally not a `CanonicalPhase2Lease`, cannot authorize state
  * use, and has no codec. Each exact source position is observed at most once per session, including raised errors and typed failures.
  */
sealed abstract class ExactReplayHistorySession[F[_]] private[overlay] () {
  def anchor: GlobalSnapshotStateRef

  def resolveOrdinals(
    targets: NonEmptyList[SnapshotOrdinal]
  ): F[Either[ExactReplayHistoryFailure, ExactReplayHistoryBatch]]

  def resolveExact(
    targets: NonEmptyList[GlobalSnapshotStateRef]
  ): F[Either[ExactReplayHistoryFailure, ExactReplayHistoryBatch]]
}

object ExactReplayHistorySession {
  import ExactReplayHistoryFailure._

  private final case class SessionState(
    pathNewestFirst: Vector[ExactReplayHistoryArtifact],
    artifactsByOrdinal: Map[SnapshotOrdinal, ExactReplayHistoryArtifact],
    visitedHashes: Set[Hash],
    outcomes: Map[ExactReplayHistoryPosition, Either[ExactReplayHistoryFailure, ExactReplayHistoryArtifact]]
  )

  private final class IssuedArtifact(
    reference: GlobalSnapshotStateRef,
    snapshot: Hashed[GlobalIncrementalSnapshot]
  ) extends ExactReplayHistoryArtifact(reference, snapshot)

  private final class IssuedBatch(
    anchor: GlobalSnapshotStateRef,
    artifactsOldestFirst: Vector[ExactReplayHistoryArtifact]
  ) extends ExactReplayHistoryBatch(anchor, artifactsOldestFirst)

  def open[F[_]: Concurrent](
    anchor: GlobalSnapshotStateRef,
    bounds: ExactReplayHistoryBounds,
    source: ExactReplayHistorySource[F]
  ): F[Either[ExactReplayHistoryFailure, ExactReplayHistorySession[F]]] =
    validateBounds(bounds) match {
      case Left(error) => Concurrent[F].pure(Left(error))
      case Right(_) =>
        validateReference(anchor) match {
          case Left(error) => Concurrent[F].pure(Left(error))
          case Right(_) =>
            val position = ExactReplayHistoryPosition(anchor.hash, anchor.ordinal)
            observe(source, position).flatMap {
              case Left(error) => Concurrent[F].pure(Left(error))
              case Right(hashed) =>
                deriveArtifact(position, hashed) match {
                  case Left(error) => Concurrent[F].pure(Left(error))
                  case Right(artifact) if artifact.reference != anchor =>
                    Concurrent[F].pure(Left(AnchorMismatch(anchor, artifact.reference)))
                  case Right(artifact) =>
                    val initial = SessionState(
                      Vector(artifact),
                      Map(artifact.reference.ordinal -> artifact),
                      Set(artifact.reference.hash),
                      Map(position -> Right(artifact))
                    )

                    for {
                      state <- Ref.of[F, SessionState](initial)
                      gate <- Semaphore[F](1L)
                    } yield Right(new IssuedSession[F](anchor, bounds, source, state, gate))
                }
            }
        }
    }

  private final class IssuedSession[F[_]: Concurrent](
    val anchor: GlobalSnapshotStateRef,
    bounds: ExactReplayHistoryBounds,
    source: ExactReplayHistorySource[F],
    stateRef: Ref[F, SessionState],
    gate: Semaphore[F]
  ) extends ExactReplayHistorySession[F] {

    def resolveOrdinals(
      targets: NonEmptyList[SnapshotOrdinal]
    ): F[Either[ExactReplayHistoryFailure, ExactReplayHistoryBatch]] = {
      val unique = SortedSet.from(targets.toList)

      Concurrent[F].map(resolve(unique))(
        _.flatMap(state => artifactsAt(state, unique).map(new IssuedBatch(anchor, _)))
      )
    }

    def resolveExact(
      targets: NonEmptyList[GlobalSnapshotStateRef]
    ): F[Either[ExactReplayHistoryFailure, ExactReplayHistoryBatch]] =
      normalizeExactTargets(targets).flatMap {
        case Left(error) => Concurrent[F].pure(Left(error))
        case Right(byOrdinal) =>
          Concurrent[F].map(resolve(SortedSet.from(byOrdinal.keys)))(
            _.flatMap { state =>
              byOrdinal.toVector
                .traverse { case (ordinal, expected) =>
                  artifactAt(state, ordinal).flatMap { observed =>
                    Either.cond(
                      observed.reference == expected,
                      observed,
                      RequestedReferenceNotAncestor(expected, observed.reference): ExactReplayHistoryFailure
                    )
                  }
                }
                .map(artifacts => new IssuedBatch(anchor, artifacts))
            }
          )
      }

    private def resolve(
      targets: SortedSet[SnapshotOrdinal]
    ): F[Either[ExactReplayHistoryFailure, SessionState]] =
      validateRequest(targets) match {
        case Left(error) => Concurrent[F].pure(Left(error))
        case Right(oldestTarget) =>
          gate.permit.use(_ => extendTo(oldestTarget))
      }

    private def extendTo(target: SnapshotOrdinal): F[Either[ExactReplayHistoryFailure, SessionState]] = {
      def loop: F[Either[ExactReplayHistoryFailure, SessionState]] =
        stateRef.get.flatMap { state =>
          val oldest = state.pathNewestFirst.last

          if (oldest.reference.ordinal.value.value <= target.value.value)
            Concurrent[F].pure(Right(state))
          else if (oldest.reference.ordinal == SnapshotOrdinal.MinValue || oldest.reference.parentHash == Hash.empty)
            Concurrent[F].pure(Left(PrematureChainRoot(oldest.reference, target)))
          else {
            val parentOrdinal = SnapshotOrdinal.unsafeApply(oldest.reference.ordinal.value.value - 1L)
            val position = ExactReplayHistoryPosition(oldest.reference.parentHash, parentOrdinal)

            if (state.visitedHashes.contains(position.hash))
              Concurrent[F].pure(Left(AncestryCycle(anchor, position)))
            else
              observeCached(position).flatMap {
                case Left(error) => Concurrent[F].pure(Left(error))
                case Right(parent) =>
                  validateDirectParent(oldest.reference, parent.reference) match {
                    case Left(error) => Concurrent[F].pure(Left(error))
                    case Right(_) =>
                      stateRef.update(current =>
                        current.copy(
                          pathNewestFirst = current.pathNewestFirst :+ parent,
                          artifactsByOrdinal = current.artifactsByOrdinal.updated(parent.reference.ordinal, parent),
                          visitedHashes = current.visitedHashes + parent.reference.hash
                        )
                      ) >> loop
                  }
              }
          }
        }

      loop
    }

    private def observeCached(
      position: ExactReplayHistoryPosition
    ): F[Either[ExactReplayHistoryFailure, ExactReplayHistoryArtifact]] =
      stateRef.get.flatMap(_.outcomes.get(position) match {
        case Some(outcome) => Concurrent[F].pure(outcome)
        case None =>
          val cancelled: Either[ExactReplayHistoryFailure, ExactReplayHistoryArtifact] = Left(SourceReadCancelled(position))

          Concurrent[F].uncancelable { poll =>
            Concurrent[F]
              .onCancel(
                poll(observe(source, position)),
                stateRef.update(state => state.copy(outcomes = state.outcomes.updated(position, cancelled)))
              )
              .map(_.flatMap(deriveArtifact(position, _)))
              .flatTap(outcome => stateRef.update(state => state.copy(outcomes = state.outcomes.updated(position, outcome))))
          }
      })

    private def validateRequest(
      targets: SortedSet[SnapshotOrdinal]
    ): Either[ExactReplayHistoryFailure, SnapshotOrdinal] = {
      val oldest = targets.head
      if (targets.size > bounds.maxUniqueTargets)
        Left(TooManyUniqueTargets(targets.size, bounds.maxUniqueTargets))
      else if (targets.last.value.value > anchor.ordinal.value.value)
        Left(TargetAboveAnchor(anchor, targets.last))
      else {
        val required = BigInt(anchor.ordinal.value.value) - BigInt(oldest.value.value) + 1

        if (required > BigInt(bounds.maxSteps))
          Left(TraversalLimitExceeded(anchor, oldest, required, bounds.maxSteps))
        else Right(oldest)
      }
    }

    private def normalizeExactTargets(
      targets: NonEmptyList[GlobalSnapshotStateRef]
    ): F[Either[ExactReplayHistoryFailure, SortedMap[SnapshotOrdinal, GlobalSnapshotStateRef]]] =
      Concurrent[F].pure(
        targets.toList.foldLeft[Either[ExactReplayHistoryFailure, SortedMap[SnapshotOrdinal, GlobalSnapshotStateRef]]](
          Right(SortedMap.empty)
        ) {
          case (acc, target) =>
            for {
              current <- acc
              _ <- validateReference(target)
              next <- current.get(target.ordinal) match {
                case Some(existing) if existing != target =>
                  Left(ConflictingTargetsAtOrdinal(target.ordinal, existing, target): ExactReplayHistoryFailure)
                case _ => Right(current.updated(target.ordinal, target))
              }
            } yield next
        }
      )

    private def artifactsAt(
      state: SessionState,
      ordinals: Iterable[SnapshotOrdinal]
    ): Either[ExactReplayHistoryFailure, Vector[ExactReplayHistoryArtifact]] =
      ordinals.iterator.toVector.traverse(artifactAt(state, _))

    private def artifactAt(
      state: SessionState,
      ordinal: SnapshotOrdinal
    ): Either[ExactReplayHistoryFailure, ExactReplayHistoryArtifact] =
      state.artifactsByOrdinal.get(ordinal).toRight(SessionArtifactMissing(anchor, ordinal): ExactReplayHistoryFailure)
  }

  private def validateBounds(bounds: ExactReplayHistoryBounds): Either[ExactReplayHistoryFailure, Unit] =
    Either.cond(bounds.maxSteps > 0 && bounds.maxUniqueTargets > 0, (), InvalidBounds(bounds): ExactReplayHistoryFailure)

  private def validateReference(reference: GlobalSnapshotStateRef): Either[ExactReplayHistoryFailure, Unit] =
    if (!isCanonicalNonEmpty(reference.hash))
      Left(InvalidReference(reference, "snapshot hash", reference.hash))
    else if (!isCanonicalParent(reference.ordinal, reference.parentHash))
      Left(InvalidReference(reference, "parent hash", reference.parentHash))
    else if (!isCanonicalNonEmpty(reference.mptRoot.value))
      Left(InvalidReference(reference, "MPT root", reference.mptRoot.value))
    else Right(())

  private def observe[F[_]: Concurrent](
    source: ExactReplayHistorySource[F],
    position: ExactReplayHistoryPosition
  ): F[Either[ExactReplayHistoryFailure, Hashed[GlobalIncrementalSnapshot]]] =
    source.fetchExact(position).attempt.map {
      case Left(error) =>
        Left(SourceReadFailed(position, Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getName)))
      case Right(result) => result
    }

  private def deriveArtifact(
    expected: ExactReplayHistoryPosition,
    hashed: Hashed[GlobalIncrementalSnapshot]
  ): Either[ExactReplayHistoryFailure, ExactReplayHistoryArtifact] = {
    val value = hashed.signed.value

    if (hashed.hash != expected.hash || value.ordinal != expected.ordinal)
      Left(ReturnedArtifactMismatch(expected, hashed.hash, value.ordinal))
    else if (!isCanonicalNonEmpty(hashed.hash))
      Left(SourceCorrupt(expected, "returned content hash is reserved or non-canonical"))
    else if (!isCanonicalParent(value.ordinal, value.lastSnapshotHash))
      Left(InvalidArtifactParent(expected, value.lastSnapshotHash))
    else
      value.stateProof.mptRoot match {
        case None => Left(MissingCommittedRoot(expected))
        case Some(root) if !isCanonicalNonEmpty(root) => Left(InvalidCommittedRoot(expected, root))
        case Some(root) =>
          val reference = GlobalSnapshotStateRef(value.ordinal, hashed.hash, value.lastSnapshotHash, MptRoot(root))
          Right(new IssuedArtifact(reference, hashed))
      }
  }

  private def validateDirectParent(
    child: GlobalSnapshotStateRef,
    parent: GlobalSnapshotStateRef
  ): Either[ExactReplayHistoryFailure, Unit] =
    Either.cond(
      child.parentHash == parent.hash && BigInt(child.ordinal.value.value) == BigInt(parent.ordinal.value.value) + 1,
      (),
      BrokenParentLink(child, parent): ExactReplayHistoryFailure
    )

  private def isCanonicalNonEmpty(hash: Hash): Boolean = {
    val value = hash.value
    hash != Hash.empty && value.length == 64 && value.forall(c => (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))
  }

  private def isCanonicalParent(ordinal: SnapshotOrdinal, hash: Hash): Boolean =
    isCanonicalNonEmpty(hash) || (ordinal == SnapshotOrdinal.MinValue && hash == Hash.empty)
}
