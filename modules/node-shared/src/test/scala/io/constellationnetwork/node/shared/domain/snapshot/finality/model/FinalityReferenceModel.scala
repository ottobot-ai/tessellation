package io.constellationnetwork.node.shared.domain.snapshot.finality.model

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash

/** Pure executable specification for the target hash-bound GL0 finality gadget.
  *
  * This model deliberately does not implement sampling, fork choice, persistence, or runtime effects. It accepts a parameterized decided-
  * attestation result and a preselected fork-choice winner, then specifies the exact-ref P0/P1/P2, replacement, recovery, and retention
  * transitions that the production implementation must reproduce.
  */
object FinalityReferenceModel {

  type SnapshotRef = GlobalSnapshotStateRef
  val SnapshotRef: GlobalSnapshotStateRef.type = GlobalSnapshotStateRef

  final case class Weight(numerator: BigInt, denominator: BigInt) {
    require(numerator >= 0, "weight numerator must be non-negative")
    require(denominator > 0, "weight denominator must be positive")

    def >=(other: Weight): Boolean =
      numerator * other.denominator >= other.numerator * denominator
  }

  /** All open optimistic-finality parameters are explicit model inputs. None is a production default. */
  final case class AvalancheParameters(
    sampleSizeK: Int,
    alpha: Int,
    beta: Int,
    decidedWeightThreshold: Weight
  ) {
    require(sampleSizeK > 0, "K must be positive")
    require(alpha > 0 && alpha <= sampleSizeK, "alpha must be in (0, K]")
    require(beta > 0, "beta must be positive")
  }

  final case class ProtocolParameters(
    k1: Long,
    k2Retention: Long,
    avalanche: AvalancheParameters
  ) {
    require(k1 > 0L, "k1 must be positive")
    require(k2Retention >= k1, "k2 retention recommendation must be at least k1")
  }

  sealed trait Phase extends Product with Serializable
  object Phase {
    case object P0Pending extends Phase
    case object P1Provisional extends Phase
    case object P2Operational extends Phase
  }

  sealed trait ExactStatus extends Product with Serializable
  object ExactStatus {
    case object Unknown extends ExactStatus
    final case class Canonical(phase: Phase) extends ExactStatus
    final case class Observed(phase: Phase) extends ExactStatus
    final case class Orphaned(was: Phase, replacement: Option[SnapshotRef]) extends ExactStatus
  }

  sealed trait RetentionStatus extends Product with Serializable
  object RetentionStatus {
    case object Retained extends RetentionStatus
    case object RetentionMature extends RetentionStatus
  }

  sealed trait Mode extends Product with Serializable
  object Mode {
    case object Running extends Mode
    final case class RecoveryRequired(missingAncestor: Hash, competingTip: SnapshotRef) extends Mode
  }

  sealed trait ForkChoiceRule extends Product with Serializable
  object ForkChoiceRule {
    case object MaxValidTk extends ForkChoiceRule
    case object MaxValidBg extends ForkChoiceRule
  }

  /** Evidence supplied only after the parameterized Avalanche cascade has emitted a decided attestation. */
  final case class DecidedAttestationEvidence(
    decidedRef: SnapshotRef,
    parameters: AvalancheParameters,
    decidedWeight: Weight
  )

  sealed trait Phase2Evidence extends Product with Serializable
  object Phase2Evidence {
    final case class Optimistic(value: DecidedAttestationEvidence) extends Phase2Evidence
    case object Depth extends Phase2Evidence
  }

  sealed trait Command extends Product with Serializable
  object Command {
    final case class ObserveExecutedCandidate(ref: SnapshotRef) extends Command

    /** Submit a winner already selected by the valid-chain comparator. The model verifies only the rule boundary: divergence at depth <= k1
      * must be tagged maxvalid-tk, while deeper divergence must be tagged maxvalid-bg.
      */
    final case class SelectCanonical(selectedTip: SnapshotRef, rule: ForkChoiceRule) extends Command

    final case class QualifyPhase2(ref: SnapshotRef, evidence: Phase2Evidence) extends Command
    final case class MarkRetentionMature(ref: SnapshotRef) extends Command
  }

  sealed trait Event extends Product with Serializable
  object Event {
    final case class CandidateObserved(ref: SnapshotRef) extends Event
    final case class CanonicalAdvanced(oldTip: Option[SnapshotRef], newTip: SnapshotRef) extends Event
    final case class CanonicalReplaced(
      oldTip: SnapshotRef,
      newTip: SnapshotRef,
      commonAncestor: SnapshotRef,
      orphaned: List[SnapshotRef],
      adopted: List[SnapshotRef],
      rule: ForkChoiceRule
    ) extends Event
    final case class Phase2Advanced(ref: SnapshotRef, evidence: Phase2Evidence) extends Event
    final case class Phase2Replaced(
      oldHead: SnapshotRef,
      newHead: Option[SnapshotRef],
      commonAncestor: SnapshotRef
    ) extends Event
    final case class EnteredRecovery(missingAncestor: Hash, competingTip: SnapshotRef) extends Event
    final case class RetentionMatured(ref: SnapshotRef) extends Event
  }

  sealed trait ModelError extends Product with Serializable
  object ModelError {
    final case class UnknownSnapshot(ref: SnapshotRef) extends ModelError
    final case class HashCollision(existing: SnapshotRef, incoming: SnapshotRef) extends ModelError
    final case class NotCanonical(ref: SnapshotRef) extends ModelError
    final case class WrongSelectionRule(expected: ForkChoiceRule, supplied: ForkChoiceRule, divergenceDepth: Long) extends ModelError
    final case class InsufficientEvidence(evidence: Phase2Evidence) extends ModelError
    final case class InsufficientRetentionDepth(observed: Long, required: Long) extends ModelError
    final case class Halted(recovery: Mode.RecoveryRequired) extends ModelError
  }

  final case class State(
    parameters: ProtocolParameters,
    candidates: Map[Hash, SnapshotRef],
    phases: Map[Hash, Phase],
    canonicalHashes: Set[Hash],
    canonicalTip: Option[SnapshotRef],
    phase2Head: Option[SnapshotRef],
    orphaned: Map[Hash, ExactStatus.Orphaned],
    retentionMature: Set[Hash],
    mode: Mode
  ) {
    def statusOf(ref: SnapshotRef): ExactStatus =
      candidates.get(ref.hash) match {
        case Some(stored) if stored == ref && canonicalHashes.contains(ref.hash) =>
          ExactStatus.Canonical(phases.getOrElse(ref.hash, Phase.P0Pending))
        case Some(stored) if stored == ref =>
          orphaned.getOrElse(ref.hash, ExactStatus.Observed(phases.getOrElse(ref.hash, Phase.P0Pending)))
        case _ => ExactStatus.Unknown
      }

    def retentionStatus(ref: SnapshotRef): RetentionStatus =
      if (retentionMature.contains(ref.hash)) RetentionStatus.RetentionMature else RetentionStatus.Retained

    def refsAtOrdinal(ordinal: SnapshotOrdinal): Set[SnapshotRef] =
      candidates.valuesIterator.filter(_.ordinal == ordinal).toSet

    def requireOperational(ref: SnapshotRef): Either[ModelError, Unit] =
      statusOf(ref) match {
        case ExactStatus.Canonical(Phase.P2Operational) => Right(())
        case _                                          => Left(ModelError.NotCanonical(ref))
      }
  }

  object State {
    def empty(parameters: ProtocolParameters): State =
      State(
        parameters = parameters,
        candidates = Map.empty,
        phases = Map.empty,
        canonicalHashes = Set.empty,
        canonicalTip = None,
        phase2Head = None,
        orphaned = Map.empty,
        retentionMature = Set.empty,
        mode = Mode.Running
      )
  }

  final case class StepResult(state: State, events: List[Event])

  def step(state: State, command: Command): Either[ModelError, StepResult] =
    state.mode match {
      case recovery: Mode.RecoveryRequired => Left(ModelError.Halted(recovery))
      case Mode.Running =>
        command match {
          case Command.ObserveExecutedCandidate(ref) => observe(state, ref)
          case Command.SelectCanonical(ref, rule)    => selectCanonical(state, ref, rule)
          case Command.QualifyPhase2(ref, evidence)  => qualifyPhase2(state, ref, evidence)
          case Command.MarkRetentionMature(ref)      => markRetentionMature(state, ref)
        }
    }

  private def observe(state: State, ref: SnapshotRef): Either[ModelError, StepResult] =
    state.candidates.get(ref.hash) match {
      case Some(existing) if existing != ref => Left(ModelError.HashCollision(existing, ref))
      case Some(_)                           => Right(StepResult(state, Nil))
      case None =>
        Right(
          StepResult(
            state.copy(
              candidates = state.candidates.updated(ref.hash, ref),
              phases = state.phases.updated(ref.hash, Phase.P0Pending)
            ),
            List(Event.CandidateObserved(ref))
          )
        )
    }

  private def selectCanonical(
    state: State,
    selectedTip: SnapshotRef,
    suppliedRule: ForkChoiceRule
  ): Either[ModelError, StepResult] =
    exactCandidate(state, selectedTip).flatMap { _ =>
      ancestry(state, selectedTip) match {
        case Left(missing) =>
          val recovery = Mode.RecoveryRequired(missing, selectedTip)
          Right(
            StepResult(
              state.copy(mode = recovery),
              List(Event.EnteredRecovery(missing, selectedTip))
            )
          )
        case Right(newChain) =>
          state.canonicalTip match {
            case None =>
              val hashes = newChain.iterator.map(_.hash).toSet
              val nextPhases = newChain.foldLeft(state.phases) { (acc, ref) =>
                acc.updated(ref.hash, Phase.P1Provisional)
              }
              Right(
                StepResult(
                  state.copy(
                    phases = nextPhases,
                    canonicalHashes = hashes,
                    canonicalTip = Some(selectedTip)
                  ),
                  List(Event.CanonicalAdvanced(None, selectedTip))
                )
              )

            case Some(oldTip) if oldTip == selectedTip => Right(StepResult(state, Nil))

            case Some(oldTip) =>
              ancestry(state, oldTip) match {
                case Left(missing) =>
                  val recovery = Mode.RecoveryRequired(missing, selectedTip)
                  Right(StepResult(state.copy(mode = recovery), List(Event.EnteredRecovery(missing, selectedTip))))
                case Right(oldChain) =>
                  val oldByHash = oldChain.iterator.map(r => r.hash -> r).toMap
                  val mrcaIndex = newChain.indexWhere(r => oldByHash.contains(r.hash))
                  if (mrcaIndex < 0) {
                    val missing = newChain.last.parentHash
                    val recovery = Mode.RecoveryRequired(missing, selectedTip)
                    Right(StepResult(state.copy(mode = recovery), List(Event.EnteredRecovery(missing, selectedTip))))
                  } else {
                    val mrca = newChain(mrcaIndex)
                    val oldMrcaIndex = oldChain.indexWhere(_.hash == mrca.hash)
                    val divergenceDepth = math.max(mrcaIndex, oldMrcaIndex).toLong
                    val expectedRule =
                      if (divergenceDepth <= state.parameters.k1) ForkChoiceRule.MaxValidTk else ForkChoiceRule.MaxValidBg

                    if (suppliedRule != expectedRule)
                      Left(ModelError.WrongSelectionRule(expectedRule, suppliedRule, divergenceDepth))
                    else {
                      val newHashes = newChain.iterator.map(_.hash).toSet
                      val orphanedRefs = oldChain.take(oldMrcaIndex)
                      val adoptedRefs = newChain.take(mrcaIndex)
                      val nextOrphaned = orphanedRefs.foldLeft(state.orphaned) { (acc, ref) =>
                        acc.updated(ref.hash, ExactStatus.Orphaned(state.phases.getOrElse(ref.hash, Phase.P0Pending), Some(selectedTip)))
                      } -- adoptedRefs.iterator.map(_.hash)
                      val nextPhases = adoptedRefs.foldLeft(state.phases) { (acc, ref) =>
                        val restored = state.orphaned.get(ref.hash).map(_.was).getOrElse(Phase.P1Provisional)
                        acc.updated(ref.hash, restored)
                      }
                      val nextP2 = newChain.find(r => nextPhases.get(r.hash).contains(Phase.P2Operational))
                      val replacementEvent =
                        if (mrca.hash == oldTip.hash)
                          Event.CanonicalAdvanced(Some(oldTip), selectedTip): Event
                        else
                          Event.CanonicalReplaced(
                            oldTip,
                            selectedTip,
                            mrca,
                            orphanedRefs.reverse,
                            adoptedRefs.reverse,
                            suppliedRule
                          ): Event
                      val phase2Event = state.phase2Head.collect {
                        case previous if nextP2.forall(_.hash != previous.hash) =>
                          Event.Phase2Replaced(previous, nextP2, mrca): Event
                      }.toList

                      Right(
                        StepResult(
                          state.copy(
                            phases = nextPhases,
                            canonicalHashes = newHashes,
                            canonicalTip = Some(selectedTip),
                            phase2Head = nextP2,
                            orphaned = nextOrphaned
                          ),
                          replacementEvent :: phase2Event
                        )
                      )
                    }
                  }
              }
          }
      }
    }

  private def qualifyPhase2(
    state: State,
    ref: SnapshotRef,
    evidence: Phase2Evidence
  ): Either[ModelError, StepResult] =
    exactCandidate(state, ref).flatMap { _ =>
      if (!state.canonicalHashes.contains(ref.hash)) Left(ModelError.NotCanonical(ref))
      else if (!qualifies(state, ref, evidence)) Left(ModelError.InsufficientEvidence(evidence))
      else if (state.phases.get(ref.hash).contains(Phase.P2Operational)) Right(StepResult(state, Nil))
      else
        ancestry(state, ref) match {
          case Left(missing) =>
            val recovery = Mode.RecoveryRequired(missing, ref)
            Right(StepResult(state.copy(mode = recovery), List(Event.EnteredRecovery(missing, ref))))
          case Right(chain) =>
            val canonicalAncestors = chain.filter(r => state.canonicalHashes.contains(r.hash))
            val nextPhases = canonicalAncestors.foldLeft(state.phases) { (acc, r) =>
              acc.updated(r.hash, Phase.P2Operational)
            }
            val nextHead = state.phase2Head match {
              case Some(current) if current.ordinal.value.value > ref.ordinal.value.value => current
              case _                                                                      => ref
            }
            Right(
              StepResult(
                state.copy(phases = nextPhases, phase2Head = Some(nextHead)),
                List(Event.Phase2Advanced(ref, evidence))
              )
            )
        }
    }

  private def markRetentionMature(
    state: State,
    ref: SnapshotRef
  ): Either[ModelError, StepResult] =
    exactCandidate(state, ref).flatMap { _ =>
      if (!state.canonicalHashes.contains(ref.hash)) Left(ModelError.NotCanonical(ref))
      else if (canonicalDepth(state, ref).forall(_ < state.parameters.k2Retention))
        Left(ModelError.InsufficientRetentionDepth(canonicalDepth(state, ref).getOrElse(-1L), state.parameters.k2Retention))
      else if (state.retentionMature.contains(ref.hash)) Right(StepResult(state, Nil))
      else
        Right(
          StepResult(
            state.copy(retentionMature = state.retentionMature + ref.hash),
            List(Event.RetentionMatured(ref))
          )
        )
    }

  private def qualifies(state: State, ref: SnapshotRef, evidence: Phase2Evidence): Boolean =
    evidence match {
      case Phase2Evidence.Optimistic(value) =>
        value.decidedRef == ref &&
        value.parameters == state.parameters.avalanche &&
        value.decidedWeight >= state.parameters.avalanche.decidedWeightThreshold
      case Phase2Evidence.Depth => canonicalDepth(state, ref).exists(_ >= state.parameters.k1)
    }

  private def canonicalDepth(state: State, ref: SnapshotRef): Option[Long] =
    state.canonicalTip.flatMap { tip =>
      ancestry(state, tip).toOption.flatMap { chain =>
        val targetDepth = chain.indexWhere(_ == ref)
        Option.when(targetDepth >= 0)(targetDepth.toLong)
      }
    }

  private def exactCandidate(state: State, ref: SnapshotRef): Either[ModelError, SnapshotRef] =
    state.candidates.get(ref.hash).filter(_ == ref).toRight(ModelError.UnknownSnapshot(ref))

  /** Tip-first ancestry. Hash.empty is the explicit root-parent sentinel. Missing parent bytes fail closed. */
  private def ancestry(state: State, tip: SnapshotRef): Either[Hash, List[SnapshotRef]] = {
    @annotation.tailrec
    def loop(current: SnapshotRef, acc: List[SnapshotRef], seen: Set[Hash]): Either[Hash, List[SnapshotRef]] =
      if (seen.contains(current.hash)) Left(current.hash)
      else if (current.parentHash == Hash.empty) Right((current :: acc).reverse)
      else
        state.candidates.get(current.parentHash) match {
          case None         => Left(current.parentHash)
          case Some(parent) => loop(parent, current :: acc, seen + current.hash)
        }

    loop(tip, Nil, Set.empty)
  }
}
