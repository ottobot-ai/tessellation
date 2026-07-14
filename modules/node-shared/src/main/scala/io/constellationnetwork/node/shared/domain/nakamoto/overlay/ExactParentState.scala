package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import scala.annotation.tailrec
import scala.util.control.NoStackTrace

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot

/** Claimed identity of one snapshot's complete consensus MPT state.
  *
  * This value is structural data, not proof that the snapshot, parent, or MPT root
  * was authenticated. The FinalityGate-bound loader must establish those facts
  * before this reference can participate in an execution session.
  */
final case class SnapshotStateRef(
  ordinal: SnapshotOrdinal,
  branch: BranchId,
  parent: BranchId,
  mptRoot: MptRoot
)

private[overlay] object SnapshotStateRefValidation {
  def isCanonical(hash: Hash): Boolean = {
    val value = hash.value
    value.length == 64 && value.forall(c => c >= '0' && c <= '9' || c >= 'a' && c <= 'f')
  }
}

/** Structurally contiguous path from one supplied base reference to one exact
  * requested reference. This is not an execution capability: it contains no
  * authenticated base, captured state bytes, semantic-era proof, or overlay
  * generation. Only the eventual overlay-owned atomic acquisition may mint an
  * execution session.
  */
sealed trait ResolvedParentLineage extends Product with Serializable {
  def parent: SnapshotStateRef
  def base: SnapshotStateRef
  def lineage: Vector[SnapshotStateRef]
}

sealed trait ParentStateUnavailableReason extends Product with Serializable

object ParentStateUnavailableReason {
  case object RequestedParentMissing extends ParentStateUnavailableReason
  final case class MissingAncestorOf(child: SnapshotStateRef) extends ParentStateUnavailableReason
}

sealed abstract class ParentStateError(message: String)
    extends RuntimeException(message)
    with NoStackTrace
    with Product
    with Serializable

object ParentStateError {
  final case class InvalidLineageLimit(maxSteps: Int)
      extends ParentStateError(s"Exact-parent lineage limit must be positive, got $maxSteps")

  final case class LineageLimitExceeded(
    requestedParent: SnapshotStateRef,
    next: BranchId,
    maxSteps: Int,
    path: Vector[BranchId]
  ) extends ParentStateError(
        s"Exact-parent lineage for ${requestedParent.branch.value.value} exceeds maxSteps=$maxSteps at ${next.value.value}"
      )

  final case class ReservedBaseSentinel(state: SnapshotStateRef)
      extends ParentStateError(s"Hash.empty/BranchId.base cannot identify a supplied base or pending lineage: $state")

  final case class NonCanonicalStateIdentity(state: SnapshotStateRef, field: String, value: Hash)
      extends ParentStateError(s"Exact parent state has a non-canonical $field '${value.value}': $state")

  final case class PendingIndexMismatch(indexedAs: BranchId, stored: SnapshotStateRef)
      extends ParentStateError(s"Pending branch index ${indexedAs.value.value} does not match stored identity ${stored.branch.value.value}")

  final case class PendingBaseCollision(base: SnapshotStateRef, stored: SnapshotStateRef)
      extends ParentStateError(s"Pending branch map shadows supplied base ${base.branch.value.value}: $stored")

  final case class ParentStateUnavailable(
    requestedParent: SnapshotStateRef,
    missingAncestor: BranchId,
    reason: ParentStateUnavailableReason
  ) extends ParentStateError(
        s"Exact parent ${requestedParent.branch.value.value} is unavailable at ancestor ${missingAncestor.value.value}: $reason"
      )

  final case class ParentStateMismatch(requested: SnapshotStateRef, stored: SnapshotStateRef)
      extends ParentStateError(
        s"Exact parent identity mismatch for ${requested.branch.value.value}: requested=$requested stored=$stored"
      )

  final case class OrdinalDiscontinuity(parent: SnapshotStateRef, child: SnapshotStateRef)
      extends ParentStateError(
        s"Exact parent ordinal discontinuity: parent=${parent.ordinal.value.value} child=${child.ordinal.value.value}"
      )

  final case class AncestryCycle(requestedParent: SnapshotStateRef, repeated: BranchId, path: Vector[BranchId])
      extends ParentStateError(
        s"Exact parent ancestry cycle for ${requestedParent.branch.value.value}: repeated=${repeated.value.value}"
      )
}

/** Pure, bounded exact-ancestry resolver.
  *
  * It follows only the requested path and never interprets an unknown hash as the
  * base. Unrelated malformed siblings do not halt a valid path; insertion and
  * recovery must quarantine those entries before they become authoritative.
  */
object ExactParentResolver {
  import ParentStateError._
  import ParentStateUnavailableReason._

  private final case class ResolvedLineage(
    parent: SnapshotStateRef,
    base: SnapshotStateRef,
    lineage: Vector[SnapshotStateRef]
  ) extends ResolvedParentLineage

  def resolve(
    base: SnapshotStateRef,
    pending: Map[BranchId, SnapshotStateRef],
    requested: SnapshotStateRef,
    maxSteps: Int
  ): Either[ParentStateError, ResolvedParentLineage] =
    if (maxSteps <= 0) Left(InvalidLineageLimit(maxSteps))
    else
      validateStateIdentity(base)
        .flatMap(_ => Either.cond(base.branch != BranchId.base, (), ReservedBaseSentinel(base)))
        .flatMap(_ => validateBaseCollision(base, pending))
        .flatMap(_ => validateStateIdentity(requested))
        .flatMap { _ =>
          if (requested.branch == base.branch)
            if (requested == base) Right(ResolvedLineage(requested, base, Vector.empty))
            else Left(ParentStateMismatch(requested, base))
          else if (requested.branch == BranchId.base || requested.parent == BranchId.base)
            Left(ReservedBaseSentinel(requested))
          else
            walk(base, pending, requested, maxSteps)
        }

  private def validateBaseCollision(
    base: SnapshotStateRef,
    pending: Map[BranchId, SnapshotStateRef]
  ): Either[ParentStateError, Unit] =
    pending.get(base.branch) match {
      case Some(stored) => Left(PendingBaseCollision(base, stored))
      case None         => Right(())
    }

  private def validateStateIdentity(state: SnapshotStateRef): Either[ParentStateError, Unit] =
    if (!SnapshotStateRefValidation.isCanonical(state.branch.value))
      Left(NonCanonicalStateIdentity(state, "snapshot hash", state.branch.value))
    else if (!SnapshotStateRefValidation.isCanonical(state.parent.value))
      Left(NonCanonicalStateIdentity(state, "parent hash", state.parent.value))
    else if (!SnapshotStateRefValidation.isCanonical(state.mptRoot.value))
      Left(NonCanonicalStateIdentity(state, "MPT root", state.mptRoot.value))
    else Right(())

  private def validateIndexed(
    indexedAs: BranchId,
    stored: SnapshotStateRef
  ): Either[ParentStateError, SnapshotStateRef] =
    if (stored.branch != indexedAs) Left(PendingIndexMismatch(indexedAs, stored))
    else validateStateIdentity(stored).map(_ => stored)

  private def walk(
    base: SnapshotStateRef,
    pending: Map[BranchId, SnapshotStateRef],
    requested: SnapshotStateRef,
    maxSteps: Int
  ): Either[ParentStateError, ResolvedParentLineage] = {
    @tailrec
    def loop(
      expected: SnapshotStateRef,
      suffix: Vector[SnapshotStateRef],
      visited: Set[BranchId],
      visitedPath: Vector[BranchId],
      steps: Int
    ): Either[ParentStateError, Vector[SnapshotStateRef]] =
      if (steps >= maxSteps)
        Left(LineageLimitExceeded(requested, expected.branch, maxSteps, visitedPath))
      else if (visited.contains(expected.branch))
        Left(AncestryCycle(requested, expected.branch, visitedPath :+ expected.branch))
      else
        pending.get(expected.branch) match {
          case None =>
            val reason = suffix.headOption
              .map(MissingAncestorOf)
              .getOrElse(RequestedParentMissing)
            Left(ParentStateUnavailable(requested, expected.branch, reason))

          case Some(candidate) =>
            validateIndexed(expected.branch, candidate) match {
              case Left(error) => Left(error)
              case Right(stored) if stored != expected => Left(ParentStateMismatch(expected, stored))
              case Right(stored) if stored.parent == BranchId.base => Left(ReservedBaseSentinel(stored))
              case Right(stored) if stored.parent == base.branch =>
                if (isDirectSuccessor(base, stored)) Right(stored +: suffix)
                else Left(OrdinalDiscontinuity(base, stored))
              case Right(stored) if visited.contains(stored.parent) || stored.parent == stored.branch =>
                Left(AncestryCycle(requested, stored.parent, visitedPath :+ stored.branch :+ stored.parent))
              case Right(stored) =>
                pending.get(stored.parent) match {
                  case None =>
                    Left(ParentStateUnavailable(requested, stored.parent, MissingAncestorOf(stored)))
                  case Some(parentCandidate) =>
                    validateIndexed(stored.parent, parentCandidate) match {
                      case Left(error) => Left(error)
                      case Right(parent) if !isDirectSuccessor(parent, stored) => Left(OrdinalDiscontinuity(parent, stored))
                      case Right(parent) =>
                        loop(parent, stored +: suffix, visited + stored.branch, visitedPath :+ stored.branch, steps + 1)
                    }
                }
            }
        }

    loop(requested, Vector.empty, Set.empty, Vector.empty, steps = 0).map { lineage =>
      ResolvedLineage(requested, base, lineage)
    }
  }

  private def isDirectSuccessor(parent: SnapshotStateRef, child: SnapshotStateRef): Boolean =
    child.parent == parent.branch && BigInt(child.ordinal.value.value) == BigInt(parent.ordinal.value.value) + 1
}
