package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import scala.annotation.tailrec
import scala.util.control.NoStackTrace

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, GlobalSnapshotStateRef}
import io.constellationnetwork.security.hash.Hash

private[overlay] object SnapshotStateIdentityValidation {
  def isCanonical(hash: Hash): Boolean = {
    val value = hash.value
    value.length == 64 && value.forall(c => c >= '0' && c <= '9' || c >= 'a' && c <= 'f')
  }
}

/** Structurally contiguous path from one supplied base reference to one exact requested reference. This is not an execution capability: it
  * contains no authenticated base, captured state bytes, semantic-era proof, or overlay generation. Only the eventual overlay-owned atomic
  * acquisition may mint an execution session.
  */
sealed trait ResolvedParentLineage extends Product with Serializable {
  def parent: GlobalSnapshotStateRef
  def base: GlobalSnapshotStateRef
  def lineage: Vector[GlobalSnapshotStateRef]
}

sealed trait ParentStateUnavailableReason extends Product with Serializable

object ParentStateUnavailableReason {
  case object RequestedParentMissing extends ParentStateUnavailableReason
  final case class MissingAncestorOf(child: GlobalSnapshotStateRef) extends ParentStateUnavailableReason
}

sealed abstract class ParentStateError(message: String) extends RuntimeException(message) with NoStackTrace with Product with Serializable

object ParentStateError {
  final case class InvalidLineageLimit(maxSteps: Int)
      extends ParentStateError(s"Exact-parent lineage limit must be positive, got $maxSteps")

  final case class LineageLimitExceeded(
    requestedParent: GlobalSnapshotStateRef,
    next: BranchId,
    maxSteps: Int,
    path: Vector[BranchId]
  ) extends ParentStateError(
        s"Exact-parent lineage for ${requestedParent.hash.value} exceeds maxSteps=$maxSteps at ${next.value.value}"
      )

  final case class ReservedBaseSentinel(state: GlobalSnapshotStateRef)
      extends ParentStateError(s"Hash.empty/BranchId.base cannot identify a supplied base or pending lineage: $state")

  final case class NonCanonicalStateIdentity(state: GlobalSnapshotStateRef, field: String, value: Hash)
      extends ParentStateError(s"Exact parent state has a non-canonical $field '${value.value}': $state")

  final case class PendingIndexMismatch(indexedAs: BranchId, stored: GlobalSnapshotStateRef)
      extends ParentStateError(s"Pending branch index ${indexedAs.value.value} does not match stored identity ${stored.hash.value}")

  final case class PendingBaseCollision(base: GlobalSnapshotStateRef, stored: GlobalSnapshotStateRef)
      extends ParentStateError(s"Pending branch map shadows supplied base ${base.hash.value}: $stored")

  final case class PendingBaseBranchCollision(base: GlobalSnapshotStateRef)
      extends ParentStateError(s"Pending branch map shadows supplied base ${base.hash.value}")

  final case class ParentStateUnavailable(
    requestedParent: GlobalSnapshotStateRef,
    missingAncestor: BranchId,
    reason: ParentStateUnavailableReason
  ) extends ParentStateError(
        s"Exact parent ${requestedParent.hash.value} is unavailable at ancestor ${missingAncestor.value.value}: $reason"
      )

  final case class ParentStateMismatch(requested: GlobalSnapshotStateRef, stored: GlobalSnapshotStateRef)
      extends ParentStateError(
        s"Exact parent identity mismatch for ${requested.hash.value}: requested=$requested stored=$stored"
      )

  final case class BranchOrdinalMismatch(
    requestedParent: GlobalSnapshotStateRef,
    branch: BranchId,
    expected: SnapshotOrdinal,
    observed: SnapshotOrdinal
  ) extends ParentStateError(
        s"Exact parent branch ${branch.value.value} has ordinal ${observed.value.value}, expected ${expected.value.value} " +
          s"while resolving ${requestedParent.hash.value}"
      )

  final case class BranchParentMismatch(
    requestedParent: GlobalSnapshotStateRef,
    branch: BranchId,
    expected: BranchId,
    observed: BranchId
  ) extends ParentStateError(
        s"Exact parent branch ${branch.value.value} links to ${observed.value.value}, expected ${expected.value.value} " +
          s"while resolving ${requestedParent.hash.value}"
      )

  final case class ParentStateRootMismatch(state: GlobalSnapshotStateRef, observed: Hash)
      extends ParentStateError(
        s"Exact parent state root mismatch for ${state.hash.value}: " +
          s"expected=${state.mptRoot.value.value} observed=${observed.value}"
      )

  final case class MalformedCapturedStateValue(state: GlobalSnapshotStateRef, key: io.constellationnetwork.security.hex.Hex)
      extends ParentStateError(
        s"Exact parent state ${state.hash.value} contains a null value at physical key ${key.value}"
      )

  final case class HistoricalStakeUnavailable(parent: GlobalSnapshotStateRef, period: EtaPeriod)
      extends ParentStateError(
        s"Historical stake period ${period.value} is unavailable in exact parent ${parent.hash.value}"
      )

  final case class InvalidEtaRotationSnapshots(value: Long)
      extends ParentStateError(s"Exact-parent historical stake lookup requires a positive eta rotation length, got $value")

  final case class HistoricalStakeWriteOrdinalOverflow(period: EtaPeriod, etaRotationSnapshots: Long)
      extends ParentStateError(
        s"Historical stake write ordinal overflows for period=${period.value}, etaRotationSnapshots=$etaRotationSnapshots"
      )

  final case class FinalizedBaseUnavailable(parent: GlobalSnapshotStateRef)
      extends ParentStateError(
        s"Exact finalized MPT base is unavailable while resolving parent ${parent.hash.value}"
      )

  final case class OrdinalDiscontinuity(parent: GlobalSnapshotStateRef, child: GlobalSnapshotStateRef)
      extends ParentStateError(
        s"Exact parent ordinal discontinuity: parent=${parent.ordinal.value.value} child=${child.ordinal.value.value}"
      )

  final case class AncestryCycle(requestedParent: GlobalSnapshotStateRef, repeated: BranchId, path: Vector[BranchId])
      extends ParentStateError(
        s"Exact parent ancestry cycle for ${requestedParent.hash.value}: repeated=${repeated.value.value}"
      )
}

/** Pure, bounded exact-ancestry resolver.
  *
  * It follows only the requested path and never interprets an unknown hash as the base. Unrelated malformed siblings do not halt a valid
  * path; insertion and recovery must quarantine those entries before they become authoritative.
  */
object ExactParentResolver {
  import ParentStateError._
  import ParentStateUnavailableReason._

  private final case class ResolvedLineage(
    parent: GlobalSnapshotStateRef,
    base: GlobalSnapshotStateRef,
    lineage: Vector[GlobalSnapshotStateRef]
  ) extends ResolvedParentLineage

  def resolve(
    base: GlobalSnapshotStateRef,
    pending: Map[BranchId, GlobalSnapshotStateRef],
    requested: GlobalSnapshotStateRef,
    maxSteps: Int
  ): Either[ParentStateError, ResolvedParentLineage] =
    if (maxSteps <= 0) Left(InvalidLineageLimit(maxSteps))
    else
      validateStateIdentity(base)
        .flatMap(_ => Either.cond(base.hash != Hash.empty, (), ReservedBaseSentinel(base)))
        .flatMap(_ => validateBaseCollision(base, pending))
        .flatMap(_ => validateStateIdentity(requested))
        .flatMap { _ =>
          if (requested.hash == base.hash)
            if (requested == base) Right(ResolvedLineage(requested, base, Vector.empty))
            else Left(ParentStateMismatch(requested, base))
          else if (requested.hash == Hash.empty || requested.parentHash == Hash.empty)
            Left(ReservedBaseSentinel(requested))
          else
            walk(base, pending, requested, maxSteps)
        }

  private def validateBaseCollision(
    base: GlobalSnapshotStateRef,
    pending: Map[BranchId, GlobalSnapshotStateRef]
  ): Either[ParentStateError, Unit] =
    pending.get(branchId(base)) match {
      case Some(stored) => Left(PendingBaseCollision(base, stored))
      case None         => Right(())
    }

  private def validateStateIdentity(state: GlobalSnapshotStateRef): Either[ParentStateError, Unit] =
    if (state.hash == Hash.empty)
      Left(ReservedBaseSentinel(state))
    else if (state.mptRoot.value == Hash.empty)
      Left(ReservedBaseSentinel(state))
    else if (state.ordinal != SnapshotOrdinal.MinValue && state.parentHash == Hash.empty)
      Left(ReservedBaseSentinel(state))
    else if (!SnapshotStateIdentityValidation.isCanonical(state.hash))
      Left(NonCanonicalStateIdentity(state, "snapshot hash", state.hash))
    else if (!SnapshotStateIdentityValidation.isCanonical(state.parentHash))
      Left(NonCanonicalStateIdentity(state, "parent hash", state.parentHash))
    else if (!SnapshotStateIdentityValidation.isCanonical(state.mptRoot.value))
      Left(NonCanonicalStateIdentity(state, "MPT root", state.mptRoot.value))
    else Right(())

  private def validateIndexed(
    indexedAs: BranchId,
    stored: GlobalSnapshotStateRef
  ): Either[ParentStateError, GlobalSnapshotStateRef] =
    if (stored.hash != indexedAs.value) Left(PendingIndexMismatch(indexedAs, stored))
    else validateStateIdentity(stored).map(_ => stored)

  private def walk(
    base: GlobalSnapshotStateRef,
    pending: Map[BranchId, GlobalSnapshotStateRef],
    requested: GlobalSnapshotStateRef,
    maxSteps: Int
  ): Either[ParentStateError, ResolvedParentLineage] = {
    @tailrec
    def loop(
      expected: GlobalSnapshotStateRef,
      suffix: Vector[GlobalSnapshotStateRef],
      visited: Set[BranchId],
      visitedPath: Vector[BranchId],
      steps: Int
    ): Either[ParentStateError, Vector[GlobalSnapshotStateRef]] =
      if (steps >= maxSteps)
        Left(LineageLimitExceeded(requested, branchId(expected), maxSteps, visitedPath))
      else if (visited.contains(branchId(expected)))
        Left(AncestryCycle(requested, branchId(expected), visitedPath :+ branchId(expected)))
      else
        pending.get(branchId(expected)) match {
          case None =>
            val reason = suffix.headOption
              .map(MissingAncestorOf)
              .getOrElse(RequestedParentMissing)
            Left(ParentStateUnavailable(requested, branchId(expected), reason))

          case Some(candidate) =>
            validateIndexed(branchId(expected), candidate) match {
              case Left(error)                                      => Left(error)
              case Right(stored) if stored != expected              => Left(ParentStateMismatch(expected, stored))
              case Right(stored) if stored.parentHash == Hash.empty => Left(ReservedBaseSentinel(stored))
              case Right(stored) if stored.parentHash == base.hash =>
                if (isDirectSuccessor(base, stored)) Right(stored +: suffix)
                else Left(OrdinalDiscontinuity(base, stored))
              case Right(stored) if visited.contains(parentBranchId(stored)) || stored.parentHash == stored.hash =>
                Left(AncestryCycle(requested, parentBranchId(stored), visitedPath :+ branchId(stored) :+ parentBranchId(stored)))
              case Right(stored) =>
                pending.get(parentBranchId(stored)) match {
                  case None =>
                    Left(ParentStateUnavailable(requested, parentBranchId(stored), MissingAncestorOf(stored)))
                  case Some(parentCandidate) =>
                    validateIndexed(parentBranchId(stored), parentCandidate) match {
                      case Left(error)                                         => Left(error)
                      case Right(parent) if !isDirectSuccessor(parent, stored) => Left(OrdinalDiscontinuity(parent, stored))
                      case Right(parent) =>
                        loop(parent, stored +: suffix, visited + branchId(stored), visitedPath :+ branchId(stored), steps + 1)
                    }
                }
            }
        }

    loop(requested, Vector.empty, Set.empty, Vector.empty, steps = 0).map { lineage =>
      ResolvedLineage(requested, base, lineage)
    }
  }

  private def branchId(state: GlobalSnapshotStateRef): BranchId = BranchId(state.hash)

  private def parentBranchId(state: GlobalSnapshotStateRef): BranchId = BranchId(state.parentHash)

  private def isDirectSuccessor(parent: GlobalSnapshotStateRef, child: GlobalSnapshotStateRef): Boolean =
    child.parentHash == parent.hash && BigInt(child.ordinal.value.value) == BigInt(parent.ordinal.value.value) + 1
}
