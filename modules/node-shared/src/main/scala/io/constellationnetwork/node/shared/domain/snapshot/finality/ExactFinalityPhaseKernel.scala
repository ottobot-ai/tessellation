package io.constellationnetwork.node.shared.domain.snapshot.finality

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong

/** Dark, pure lifecycle kernel for exact-reference GL0 finality phases.
  *
  * This kernel deliberately accepts already-verified capabilities. It does not authenticate or execute a snapshot, select a canonical
  * frontier, evaluate depth, sample peers, define optimistic evidence, publish state, or authorize a consumer. Its only responsibility is to
  * make the phase/replacement state machine executable without collapsing identity to an ordinal.
  *
  * No direct production-source caller is permitted until the objective selector, qualification verifiers, durable coordinator, and
  * reversible effect transaction are complete. The package-scoped capability constructors are source-level staging hygiene, not a JVM
  * security boundary; activation requires construction behind the completed verifier/coordinator boundary.
  */
private[finality] object ExactFinalityPhaseKernel {

  sealed trait Phase extends Product

  object Phase {
    case object P0Pending extends Phase
    case object P1Provisional extends Phase
    case object P2Operational extends Phase
  }

  sealed trait ExactStatus extends Product

  object ExactStatus {
    case object Unknown extends ExactStatus
    final case class Observed(phase: Phase) extends ExactStatus
    final case class Canonical(phase: Phase) extends ExactStatus
    final case class Orphaned(was: Phase, replacement: GlobalSnapshotStateRef) extends ExactStatus
  }

  sealed trait RetentionStatus extends Product

  object RetentionStatus {
    case object Unknown extends RetentionStatus
    case object Retained extends RetentionStatus
    case object RetentionMature extends RetentionStatus
  }

  sealed trait RecoveryReason extends Product

  object RecoveryReason {
    final case class MissingAncestor(requiredHash: Hash, competingTip: GlobalSnapshotStateRef) extends RecoveryReason
    final case class DisconnectedCanonicalFrontier(
      currentTip: GlobalSnapshotStateRef,
      competingTip: GlobalSnapshotStateRef
    ) extends RecoveryReason
    final case class BranchRevisionExhausted(current: CanonicalBranchRevision) extends RecoveryReason
    final case class LineageRevisionExhausted(current: CanonicalLineageRevision) extends RecoveryReason
  }

  sealed trait Mode extends Product

  object Mode {
    case object Running extends Mode
    final case class RecoveryRequired(reason: RecoveryReason) extends Mode
  }

  /** Opaque pointer to qualification bytes whose semantics were verified before this kernel was called.
    *
    * `OperationalRail` names only the two ratified rails. The pointer does not become valid because it is wrapped here.
    */
  final case class OperationalEvidenceRef(
    rail: OperationalRail,
    qualifyingDescendant: GlobalSnapshotStateRef,
    evidence: ImmutableArtifactPointer
  )

  final case class OperationalEvidenceRecord(
    lineageRevision: CanonicalLineageRevision,
    operationalTarget: GlobalSnapshotStateRef,
    evidence: OperationalEvidenceRef
  )

  /** Capability issued only after complete local execution and snapshot authentication. */
  final class ValidatedExecutedCandidate private[finality] (val ref: GlobalSnapshotStateRef)

  /** Capability issued only by the future objective canonical-frontier verifier.
    *
    * The pointed-to fork-choice payload remains opaque until O15 defines the frontier, cutoff, and late-reveal rules. The branch CAS detects
    * a canonical mutation, but cannot detect a contender observed after verification because `Observe` does not advance it. Before
    * activation, one owned coordinator queue/critical section must serialize `Observe`, the final eligible-contender drain/recheck, and
    * application of this capability. The local revision is not a consensus cutoff and receipt of a losing P0 candidate does not dequalify
    * current P2 state.
    */
  final class VerifiedCanonicalSelection private[finality] (
    val expectedBranchRevision: CanonicalBranchRevision,
    val selectedTip: GlobalSnapshotStateRef,
    val decisionEvidence: ImmutableArtifactPointer
  )

  /** Capability issued only after the selected rail's exact portable evidence has been verified.
    *
    * This class intentionally carries no optimistic transcript schema.
    */
  final class VerifiedOperationalQualification private[finality] (
    val expectedLineageRevision: CanonicalLineageRevision,
    val operationalTarget: GlobalSnapshotStateRef,
    val evidence: OperationalEvidenceRef
  )

  /** Local retention observation. It can change retention metadata only. */
  final class VerifiedRetentionMaturity private[finality] (val ref: GlobalSnapshotStateRef)

  sealed trait Command extends Product

  object Command {
    final case class Observe(candidate: ValidatedExecutedCandidate) extends Command
    final case class Select(selection: VerifiedCanonicalSelection) extends Command
    final case class Qualify(qualification: VerifiedOperationalQualification) extends Command
    final case class MarkRetentionMature(maturity: VerifiedRetentionMaturity) extends Command
  }

  sealed trait Event extends Product

  object Event {
    final case class CandidateObserved(ref: GlobalSnapshotStateRef) extends Event

    final case class CanonicalAdvanced(
      oldTip: Option[GlobalSnapshotStateRef],
      newTip: GlobalSnapshotStateRef,
      adoptedOldestFirst: Vector[GlobalSnapshotStateRef],
      decisionEvidence: ImmutableArtifactPointer,
      branchRevision: CanonicalBranchRevision
    ) extends Event

    final case class CanonicalReplaced(
      oldTip: GlobalSnapshotStateRef,
      newTip: GlobalSnapshotStateRef,
      commonAncestor: GlobalSnapshotStateRef,
      orphanedOldestFirst: Vector[GlobalSnapshotStateRef],
      adoptedOldestFirst: Vector[GlobalSnapshotStateRef],
      decisionEvidence: ImmutableArtifactPointer,
      branchRevision: CanonicalBranchRevision,
      lineageRevision: CanonicalLineageRevision
    ) extends Event

    final case class OperationalAdvanced(
      oldHead: Option[GlobalSnapshotStateRef],
      newHead: GlobalSnapshotStateRef,
      newlyOperationalOldestFirst: Vector[GlobalSnapshotStateRef],
      evidence: OperationalEvidenceRef,
      branchRevision: CanonicalBranchRevision,
      lineageRevision: CanonicalLineageRevision
    ) extends Event

    final case class OperationalReplaced(
      oldHead: GlobalSnapshotStateRef,
      newHead: Option[GlobalSnapshotStateRef],
      commonAncestor: GlobalSnapshotStateRef,
      orphanedOperationalOldestFirst: Vector[GlobalSnapshotStateRef],
      adoptedOperationalOldestFirst: Vector[GlobalSnapshotStateRef],
      branchRevision: CanonicalBranchRevision,
      lineageRevision: CanonicalLineageRevision
    ) extends Event

    final case class RetentionMatured(ref: GlobalSnapshotStateRef) extends Event
    final case class EnteredRecovery(reason: RecoveryReason) extends Event
  }

  sealed trait Error extends Product

  object Error {
    final case class Halted(recovery: Mode.RecoveryRequired) extends Error
    final case class InvalidReference(ref: GlobalSnapshotStateRef, detail: String) extends Error
    final case class HashCollision(existing: GlobalSnapshotStateRef, incoming: GlobalSnapshotStateRef) extends Error
    final case class InvalidLineage(
      child: GlobalSnapshotStateRef,
      knownParent: GlobalSnapshotStateRef,
      detail: String
    ) extends Error
    final case class UnknownCandidate(ref: GlobalSnapshotStateRef) extends Error
    final case class NotCanonical(ref: GlobalSnapshotStateRef) extends Error
    final case class StaleCanonicalSelection(
      expected: CanonicalBranchRevision,
      actual: CanonicalBranchRevision
    ) extends Error
    final case class StaleOperationalQualification(
      expected: CanonicalLineageRevision,
      actual: CanonicalLineageRevision
    ) extends Error
    final case class QualificationLineageMismatch(
      operationalTarget: GlobalSnapshotStateRef,
      qualifyingDescendant: GlobalSnapshotStateRef
    ) extends Error
    final case class WrongEvidenceKind(expected: FinalityArtifactKind, actual: FinalityArtifactKind) extends Error
  }

  final case class State(
    candidatesByHash: Map[Hash, GlobalSnapshotStateRef],
    phaseByHash: Map[Hash, Phase],
    canonicalOldestFirst: Vector[GlobalSnapshotStateRef],
    operationalHead: Option[GlobalSnapshotStateRef],
    operationalEvidenceHistory: Map[Hash, Vector[OperationalEvidenceRecord]],
    orphanedByHash: Map[Hash, ExactStatus.Orphaned],
    retentionMature: Set[Hash],
    branchRevision: CanonicalBranchRevision,
    lineageRevision: CanonicalLineageRevision,
    mode: Mode
  ) {

    def statusOf(ref: GlobalSnapshotStateRef): ExactStatus =
      candidatesByHash.get(ref.hash) match {
        case Some(stored) if stored == ref && canonicalOldestFirst.contains(ref) =>
          ExactStatus.Canonical(phaseByHash.getOrElse(ref.hash, Phase.P0Pending))
        case Some(stored) if stored == ref =>
          orphanedByHash.getOrElse(ref.hash, ExactStatus.Observed(phaseByHash.getOrElse(ref.hash, Phase.P0Pending)))
        case _ => ExactStatus.Unknown
      }

    def retentionStatusOf(ref: GlobalSnapshotStateRef): RetentionStatus =
      if (!candidatesByHash.get(ref.hash).contains(ref)) RetentionStatus.Unknown
      else if (retentionMature.contains(ref.hash)) RetentionStatus.RetentionMature
      else RetentionStatus.Retained

    def refsAtOrdinal(ordinal: SnapshotOrdinal): Set[GlobalSnapshotStateRef] =
      candidatesByHash.valuesIterator.filter(_.ordinal == ordinal).toSet

    def requireOperational(ref: GlobalSnapshotStateRef): Either[Error, OperationalEvidenceRecord] =
      mode match {
        case recovery: Mode.RecoveryRequired => Left(Error.Halted(recovery))
        case Mode.Running =>
          statusOf(ref) match {
            case ExactStatus.Canonical(Phase.P2Operational) =>
              operationalEvidenceHistory
                .get(ref.hash)
                .flatMap(_.lastOption)
                .toRight(Error.NotCanonical(ref): Error)
            case _ => Left(Error.NotCanonical(ref))
          }
      }
  }

  object State {
    val empty: State =
      State(
        candidatesByHash = Map.empty,
        phaseByHash = Map.empty,
        canonicalOldestFirst = Vector.empty,
        operationalHead = None,
        operationalEvidenceHistory = Map.empty,
        orphanedByHash = Map.empty,
        retentionMature = Set.empty,
        branchRevision = CanonicalBranchRevision(NonNegLong.unsafeFrom(0L)),
        lineageRevision = CanonicalLineageRevision(NonNegLong.unsafeFrom(0L)),
        mode = Mode.Running
      )
  }

  final case class Transition(state: State, events: Vector[Event])

  def applyCommand(state: State, command: Command): Either[Error, Transition] =
    state.mode match {
      case recovery: Mode.RecoveryRequired => Left(Error.Halted(recovery))
      case Mode.Running =>
        command match {
          case Command.Observe(candidate)            => observe(state, candidate.ref)
          case Command.Select(selection)             => select(state, selection)
          case Command.Qualify(qualification)        => qualify(state, qualification)
          case Command.MarkRetentionMature(maturity) => markRetentionMature(state, maturity.ref)
        }
    }

  private def observe(state: State, ref: GlobalSnapshotStateRef): Either[Error, Transition] =
    validateReference(ref).flatMap { _ =>
      state.candidatesByHash.get(ref.hash) match {
        case Some(existing) if existing != ref => Left(Error.HashCollision(existing, ref))
        case Some(_)                           => Right(Transition(state, Vector.empty))
        case None =>
          Right(
            Transition(
              state.copy(
                candidatesByHash = state.candidatesByHash.updated(ref.hash, ref),
                phaseByHash = state.phaseByHash.updated(ref.hash, Phase.P0Pending)
              ),
              Vector(Event.CandidateObserved(ref))
            )
          )
      }
    }

  private def select(state: State, selection: VerifiedCanonicalSelection): Either[Error, Transition] = {
    val selectedTip = selection.selectedTip

    Either
      .cond(
        selection.expectedBranchRevision == state.branchRevision,
        (),
        Error.StaleCanonicalSelection(selection.expectedBranchRevision, state.branchRevision): Error
      )
      .flatMap(_ =>
        Either.cond(
          selection.decisionEvidence.kind == FinalityArtifactKind.ForkChoiceDecisionEvidence,
          (),
          Error.WrongEvidenceKind(
            FinalityArtifactKind.ForkChoiceDecisionEvidence,
            selection.decisionEvidence.kind
          ): Error
        )
      )
      .flatMap(_ => exactCandidate(state, selectedTip))
      .map(_ => lineageFrom(state, selectedTip))
      .flatMap {
        case Left(LineageFailure.Recover(reason)) => Right(enterRecovery(state, reason))
        case Left(LineageFailure.Reject(error))   => Left(error)
        case Right(nextCanonical) =>
        state.canonicalOldestFirst.lastOption match {
          case None =>
            nextBranchRevision(state.branchRevision) match {
              case Left(reason) => Right(enterRecovery(state, reason))
              case Right(nextBranch) =>
                val phases = nextCanonical.foldLeft(state.phaseByHash) { (acc, ref) =>
                  acc.updated(ref.hash, Phase.P1Provisional)
                }
                Right(
                  Transition(
                    state.copy(
                      phaseByHash = phases,
                      canonicalOldestFirst = nextCanonical,
                      orphanedByHash = state.orphanedByHash -- nextCanonical.iterator.map(_.hash),
                      branchRevision = nextBranch
                    ),
                    Vector(
                      Event.CanonicalAdvanced(
                        None,
                        selectedTip,
                        nextCanonical,
                        selection.decisionEvidence,
                        nextBranch
                      )
                    )
                  )
                )
            }

          case Some(currentTip) if currentTip == selectedTip =>
            Right(Transition(state, Vector.empty))

          case Some(currentTip) =>
            val commonCount = commonPrefixSize(state.canonicalOldestFirst, nextCanonical)
            if (commonCount == 0)
              Right(enterRecovery(state, RecoveryReason.DisconnectedCanonicalFrontier(currentTip, selectedTip)))
            else {
              val commonAncestor = nextCanonical(commonCount - 1)
              val orphaned = state.canonicalOldestFirst.drop(commonCount)
              val adopted = nextCanonical.drop(commonCount)
              val extension = orphaned.isEmpty

              if (extension) {
                nextBranchRevision(state.branchRevision) match {
                  case Left(reason) => Right(enterRecovery(state, reason))
                  case Right(nextBranch) =>
                    val nextPhases = adopted.foldLeft(state.phaseByHash) { (acc, ref) =>
                      acc.updated(ref.hash, Phase.P1Provisional)
                    }
                    Right(
                      Transition(
                        state.copy(
                          phaseByHash = nextPhases,
                          canonicalOldestFirst = nextCanonical,
                          orphanedByHash = state.orphanedByHash -- adopted.iterator.map(_.hash),
                          branchRevision = nextBranch
                        ),
                        Vector(
                          Event.CanonicalAdvanced(
                            Some(currentTip),
                            selectedTip,
                            adopted,
                            selection.decisionEvidence,
                            nextBranch
                          )
                        )
                      )
                    )
                }
              } else
                (
                  nextBranchRevision(state.branchRevision),
                  nextLineageRevision(state.lineageRevision)
                ) match {
                  case (Left(reason), _) => Right(enterRecovery(state, reason))
                  case (_, Left(reason)) => Right(enterRecovery(state, reason))
                  case (Right(nextBranch), Right(nextLineage)) =>
                    val nextOrphaned = orphaned.foldLeft(state.orphanedByHash) { (acc, ref) =>
                      acc.updated(
                        ref.hash,
                        ExactStatus.Orphaned(state.phaseByHash.getOrElse(ref.hash, Phase.P0Pending), selectedTip)
                      )
                    } -- adopted.iterator.map(_.hash)
                    // Re-adoption never restores P2 by identity alone. The retained evidence is audit history;
                    // a fresh verified qualification capability is required on the new lineage.
                    val nextPhases = adopted.foldLeft(state.phaseByHash) { (acc, ref) =>
                      acc.updated(ref.hash, Phase.P1Provisional)
                    }
                    val nextOperationalHead =
                      nextCanonical.reverseIterator.find(ref => nextPhases.get(ref.hash).contains(Phase.P2Operational))
                    val orphanedOperational =
                      orphaned.filter(ref => state.phaseByHash.get(ref.hash).contains(Phase.P2Operational))
                    val adoptedOperational =
                      adopted.filter(ref => nextPhases.get(ref.hash).contains(Phase.P2Operational))
                    val replacement = Event.CanonicalReplaced(
                      currentTip,
                      selectedTip,
                      commonAncestor,
                      orphaned,
                      adopted,
                      selection.decisionEvidence,
                      nextBranch,
                      nextLineage
                    )
                    val operationalReplacement = state.operationalHead.collect {
                      case old if !nextOperationalHead.contains(old) =>
                        Event.OperationalReplaced(
                          old,
                          nextOperationalHead,
                          commonAncestor,
                          orphanedOperational,
                          adoptedOperational,
                          nextBranch,
                          nextLineage
                        ): Event
                    }

                    Right(
                      Transition(
                        state.copy(
                          phaseByHash = nextPhases,
                          canonicalOldestFirst = nextCanonical,
                          operationalHead = nextOperationalHead,
                          orphanedByHash = nextOrphaned,
                          branchRevision = nextBranch,
                          lineageRevision = nextLineage
                        ),
                        replacement +: operationalReplacement.toVector
                      )
                    )
                }
            }
        }
    }
  }

  private def qualify(
    state: State,
    qualification: VerifiedOperationalQualification
  ): Either[Error, Transition] = {
    val target = qualification.operationalTarget
    val evidence = qualification.evidence

    for {
      _ <-
        Either.cond(
          qualification.expectedLineageRevision == state.lineageRevision,
          (),
          Error.StaleOperationalQualification(qualification.expectedLineageRevision, state.lineageRevision): Error
        )
      _ <- exactCandidate(state, target)
      _ <- exactCandidate(state, evidence.qualifyingDescendant)
      targetIndex <- canonicalIndex(state, target)
      descendantIndex <- canonicalIndex(state, evidence.qualifyingDescendant)
      _ <-
        Either.cond(
          targetIndex <= descendantIndex,
          (),
          Error.QualificationLineageMismatch(target, evidence.qualifyingDescendant): Error
        )
      expectedKind = evidence.rail match {
        case OperationalRail.DecidedAttestationTWeight => FinalityArtifactKind.DecidedAttestationEvidence
        case OperationalRail.CanonicalDepthK1          => FinalityArtifactKind.DepthK1Evidence
      }
      _ <-
        Either.cond(
          evidence.evidence.kind == expectedKind,
          (),
          Error.WrongEvidenceKind(expectedKind, evidence.evidence.kind): Error
        )
    } yield {
      if (state.phaseByHash.get(target.hash).contains(Phase.P2Operational))
        Transition(state, Vector.empty)
      else {
        val operationalPrefix = state.canonicalOldestFirst.take(targetIndex + 1)
        val newlyOperational =
          operationalPrefix.filterNot(ref => state.phaseByHash.get(ref.hash).contains(Phase.P2Operational))
        val nextPhases = operationalPrefix.foldLeft(state.phaseByHash) { (acc, ref) =>
          acc.updated(ref.hash, Phase.P2Operational)
        }
        val evidenceRecord = OperationalEvidenceRecord(state.lineageRevision, target, evidence)
        val nextEvidence = newlyOperational.foldLeft(state.operationalEvidenceHistory) { (acc, ref) =>
          acc.updated(ref.hash, acc.getOrElse(ref.hash, Vector.empty) :+ evidenceRecord)
        }

        Transition(
          state.copy(
            phaseByHash = nextPhases,
            operationalHead = Some(target),
            operationalEvidenceHistory = nextEvidence
          ),
          Vector(
            Event.OperationalAdvanced(
              state.operationalHead,
              target,
              newlyOperational,
              evidence,
              state.branchRevision,
              state.lineageRevision
            )
          )
        )
      }
    }
  }

  private def markRetentionMature(state: State, ref: GlobalSnapshotStateRef): Either[Error, Transition] =
    exactCandidate(state, ref).map { _ =>
      if (state.retentionMature.contains(ref.hash)) Transition(state, Vector.empty)
      else
        Transition(
          state.copy(retentionMature = state.retentionMature + ref.hash),
          Vector(Event.RetentionMatured(ref))
        )
    }

  private def lineageFrom(
    state: State,
    tip: GlobalSnapshotStateRef
  ): Either[LineageFailure, Vector[GlobalSnapshotStateRef]] = {
    @annotation.tailrec
    def loop(
      current: GlobalSnapshotStateRef,
      newestFirst: Vector[GlobalSnapshotStateRef],
      seen: Set[Hash]
    ): Either[LineageFailure, Vector[GlobalSnapshotStateRef]] =
      if (seen.contains(current.hash))
        Left(LineageFailure.Reject(Error.InvalidLineage(current, current, "cycle in exact parent lineage")))
      else if (current.ordinal.value.value == 0L)
        if (current.parentHash == Hash.empty) Right(current +: newestFirst)
        else
          Left(
            LineageFailure.Reject(
              Error.InvalidLineage(current, current, "ordinal-zero candidate does not use the genesis parent sentinel")
            )
          )
      else
        state.candidatesByHash.get(current.parentHash) match {
          case Some(parent) if BigInt(parent.ordinal.value.value) + 1 == BigInt(current.ordinal.value.value) =>
            loop(parent, current +: newestFirst, seen + current.hash)
          case Some(parent) =>
            Left(
              LineageFailure.Reject(
                Error.InvalidLineage(current, parent, "known parent ordinal is not exactly child ordinal minus one")
              )
            )
          case None =>
            Left(LineageFailure.Recover(RecoveryReason.MissingAncestor(current.parentHash, tip)))
        }

    loop(tip, Vector.empty, Set.empty)
  }

  private sealed trait LineageFailure extends Product

  private object LineageFailure {
    final case class Recover(reason: RecoveryReason) extends LineageFailure
    final case class Reject(error: Error) extends LineageFailure
  }

  private def commonPrefixSize(
    left: Vector[GlobalSnapshotStateRef],
    right: Vector[GlobalSnapshotStateRef]
  ): Int =
    left.iterator.zip(right.iterator).takeWhile { case (a, b) => a == b }.size

  private def canonicalIndex(state: State, ref: GlobalSnapshotStateRef): Either[Error, Int] = {
    val index = state.canonicalOldestFirst.indexOf(ref)
    Either.cond(index >= 0, index, Error.NotCanonical(ref): Error)
  }

  private def exactCandidate(state: State, ref: GlobalSnapshotStateRef): Either[Error, GlobalSnapshotStateRef] =
    state.candidatesByHash.get(ref.hash).filter(_ == ref).toRight(Error.UnknownCandidate(ref): Error)

  private def nextLineageRevision(
    current: CanonicalLineageRevision
  ): Either[RecoveryReason, CanonicalLineageRevision] =
    if (current.value.value == Long.MaxValue) Left(RecoveryReason.LineageRevisionExhausted(current))
    else
      Right(
        CanonicalLineageRevision(
          NonNegLong.unsafeFrom(current.value.value + 1L)
        )
      )

  private def nextBranchRevision(
    current: CanonicalBranchRevision
  ): Either[RecoveryReason, CanonicalBranchRevision] =
    if (current.value.value == Long.MaxValue) Left(RecoveryReason.BranchRevisionExhausted(current))
    else
      Right(
        CanonicalBranchRevision(
          NonNegLong.unsafeFrom(current.value.value + 1L)
        )
      )

  private def enterRecovery(state: State, reason: RecoveryReason): Transition =
    Transition(
      state.copy(mode = Mode.RecoveryRequired(reason)),
      Vector(Event.EnteredRecovery(reason))
    )

  private def validateReference(ref: GlobalSnapshotStateRef): Either[Error, Unit] = {
    val ordinal = ref.ordinal.value.value
    val hashValid = canonicalNonZeroHash(ref.hash)
    val parentValid = if (ordinal == 0L) ref.parentHash == Hash.empty else canonicalNonZeroHash(ref.parentHash)
    val rootValid = canonicalNonZeroHash(ref.mptRoot.value)

    Either.cond(
      hashValid && parentValid && rootValid,
      (),
      Error.InvalidReference(ref, "hash, parent sentinel, or MPT root is not canonical"): Error
    )
  }

  private def canonicalNonZeroHash(hash: Hash): Boolean =
    hash != Hash.empty &&
      hash.value.length == 64 &&
      hash.value.forall(character =>
        character >= '0' && character <= '9' || character >= 'a' && character <= 'f'
      )
}
