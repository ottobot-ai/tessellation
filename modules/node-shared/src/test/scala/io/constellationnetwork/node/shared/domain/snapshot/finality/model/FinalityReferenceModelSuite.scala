package io.constellationnetwork.node.shared.domain.snapshot.finality.model

import io.constellationnetwork.node.shared.domain.snapshot.finality.model.FinalityReferenceModel._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot

import weaver.SimpleIOSuite

object FinalityReferenceModelSuite extends SimpleIOSuite {

  private val avalancheA = AvalancheParameters(
    sampleSizeK = 11,
    alpha = 7,
    beta = 13,
    decidedWeightThreshold = Weight(2, 3)
  )

  private val parameters = ProtocolParameters(k1 = 2L, k2Retention = 6L, avalanche = avalancheA)

  private def hash(char: Char): Hash = Hash(char.toString * 64)
  private def ord(value: Long): SnapshotOrdinal = SnapshotOrdinal.unsafeApply(value)

  private def ref(value: Long, char: Char, parent: Hash, rootChar: Char): SnapshotRef =
    SnapshotRef(ord(value), hash(char), parent, MptRoot(hash(rootChar)))

  private val genesis = ref(0L, 'a', Hash.empty, '0')
  private val a1 = ref(1L, '1', genesis.hash, 'b')
  private val a2 = ref(2L, '2', a1.hash, 'c')
  private val a3 = ref(3L, '3', a2.hash, 'd')
  private val a4 = ref(4L, '8', a3.hash, 'e')
  private val a5 = ref(5L, '9', a4.hash, 'f')
  private val a6 = ref(6L, 'e', a5.hash, '7')
  // Hash.empty is 64 zeroes and is the ancestry root sentinel, so never use it as a fixture's snapshot hash.
  private val a7 = SnapshotRef(ord(7L), Hash("01" * 32), a6.hash, MptRoot(hash('8')))
  private val a8 = ref(8L, '7', a7.hash, '9')
  private val a9 = ref(9L, 'f', a8.hash, '0')

  private def applyCommand(state: State, command: Command): State =
    FinalityReferenceModel.step(state, command).fold(err => throw new AssertionError(err.toString), _.state)

  private def observeAll(initial: State, refs: List[SnapshotRef]): State =
    refs.foldLeft(initial)((state, next) => applyCommand(state, Command.ObserveExecutedCandidate(next)))

  private def decided(ref: SnapshotRef, weight: Weight = Weight(2, 3)): Phase2Evidence =
    Phase2Evidence.Optimistic(DecidedAttestationEvidence(ref, avalancheA, weight))

  private def canonicalA: State = {
    val observed = observeAll(State.empty(parameters), List(genesis, a1, a2, a3))
    applyCommand(observed, Command.SelectCanonical(a3, ForkChoiceRule.MaxValidTk))
  }

  private def canonicalLong: State = {
    val observed = observeAll(State.empty(parameters), List(genesis, a1, a2, a3, a4, a5, a6, a7, a8, a9))
    applyCommand(observed, Command.SelectCanonical(a9, ForkChoiceRule.MaxValidTk))
  }

  pureTest("[FIN-M-001] exact candidate moves P0 -> P1 -> P2 without an ordinal-only status") {
    val observed = observeAll(State.empty(parameters), List(genesis, a1, a2, a3))
    val selected = applyCommand(observed, Command.SelectCanonical(a3, ForkChoiceRule.MaxValidTk))
    val qualified = applyCommand(selected, Command.QualifyPhase2(a1, Phase2Evidence.Depth))

    expect.all(
      observed.statusOf(a1) == ExactStatus.Observed(Phase.P0Pending),
      selected.statusOf(a1) == ExactStatus.Canonical(Phase.P1Provisional),
      qualified.statusOf(a1) == ExactStatus.Canonical(Phase.P2Operational),
      qualified.phase2Head.contains(a1)
    )
  }

  pureTest("[FIN-S-001] same ordinal has two exact refs and ordinal-only lookup is ambiguous") {
    val b1 = ref(1L, '4', genesis.hash, 'e')
    val observed = observeAll(State.empty(parameters), List(genesis, a1, b1))
    val selected = applyCommand(observed, Command.SelectCanonical(a1, ForkChoiceRule.MaxValidTk))
    val qualified = applyCommand(selected, Command.QualifyPhase2(a1, decided(a1)))

    expect.all(
      qualified.refsAtOrdinal(ord(1L)) == Set(a1, b1),
      qualified.requireOperational(a1) == Right(()),
      qualified.requireOperational(b1) == Left(ModelError.NotCanonical(b1))
    )
  }

  pureTest("[FIN-M-003] decided-attestation OR k1 depth independently qualifies the exact canonical hash") {
    val selected = canonicalA
    val byOptimistic = applyCommand(selected, Command.QualifyPhase2(a2, decided(a2)))
    val byDepth = applyCommand(selected, Command.QualifyPhase2(a1, Phase2Evidence.Depth))
    val belowOptimistic = FinalityReferenceModel.step(
      selected,
      Command.QualifyPhase2(a2, decided(a2, Weight(1, 2)))
    )
    val belowDepth = FinalityReferenceModel.step(selected, Command.QualifyPhase2(a2, Phase2Evidence.Depth))

    expect.all(
      byOptimistic.requireOperational(a2) == Right(()),
      byDepth.requireOperational(a1) == Right(()),
      belowOptimistic == Left(
        ModelError.InsufficientEvidence(
          decided(a2, Weight(1, 2))
        )
      ),
      belowDepth == Left(ModelError.InsufficientEvidence(Phase2Evidence.Depth))
    )
  }

  pureTest("[FIN-M-003] optimistic evidence is parameter-bound; no K/alpha/beta value is hardcoded") {
    val selected = canonicalA
    val otherParameters = avalancheA.copy(sampleSizeK = 17, alpha = 9, beta = 21)
    val wrongParameterEvidence = Phase2Evidence.Optimistic(
      DecidedAttestationEvidence(a2, otherParameters, Weight(1, 1))
    )

    expect(
      FinalityReferenceModel.step(selected, Command.QualifyPhase2(a2, wrongParameterEvidence)) ==
        Left(ModelError.InsufficientEvidence(wrongParameterEvidence))
    )
  }

  pureTest("[FIN-S-001A] decided attestation for one exact ref cannot qualify another candidate") {
    val selected = canonicalA
    val wrongTargetEvidence = decided(a1)

    expect(
      FinalityReferenceModel.step(selected, Command.QualifyPhase2(a2, wrongTargetEvidence)) ==
        Left(ModelError.InsufficientEvidence(wrongTargetEvidence))
    )
  }

  pureTest("[FIN-D-001] shallow replacement requires maxvalid-tk and deep replacement requires maxvalid-bg") {
    val shallow2 = ref(2L, '4', a1.hash, 'e')
    val deep1 = ref(1L, '5', genesis.hash, 'f')
    val deep2 = ref(2L, '6', deep1.hash, '7')
    val deep3 = ref(3L, '7', deep2.hash, '8')
    val withFork = observeAll(canonicalA, List(shallow2, deep1, deep2, deep3))
    val wrongShallow = FinalityReferenceModel.step(withFork, Command.SelectCanonical(shallow2, ForkChoiceRule.MaxValidBg))
    val rightShallow = FinalityReferenceModel.step(withFork, Command.SelectCanonical(shallow2, ForkChoiceRule.MaxValidTk))
    val wrongDeep = FinalityReferenceModel.step(withFork, Command.SelectCanonical(deep3, ForkChoiceRule.MaxValidTk))
    val rightDeep = FinalityReferenceModel.step(withFork, Command.SelectCanonical(deep3, ForkChoiceRule.MaxValidBg))

    expect.all(
      wrongShallow == Left(ModelError.WrongSelectionRule(ForkChoiceRule.MaxValidTk, ForkChoiceRule.MaxValidBg, 2L)),
      rightShallow.isRight,
      wrongDeep == Left(ModelError.WrongSelectionRule(ForkChoiceRule.MaxValidBg, ForkChoiceRule.MaxValidTk, 3L)),
      rightDeep.isRight
    )
  }

  pureTest("[FIN-D-002][FIN-S-001A] same-height replacement emits exact Replace and P2 Rollback events") {
    val oldP2 = applyCommand(canonicalA, Command.QualifyPhase2(a3, decided(a3)))
    val b1 = ref(1L, '4', genesis.hash, 'e')
    val b2 = ref(2L, '5', b1.hash, 'f')
    val b3 = ref(3L, '6', b2.hash, '7')
    val withFork = observeAll(oldP2, List(b1, b2, b3))
    val result = FinalityReferenceModel.step(withFork, Command.SelectCanonical(b3, ForkChoiceRule.MaxValidBg))

    result match {
      case Right(StepResult(next, events)) =>
        val replacement = events.collectFirst { case value: Event.CanonicalReplaced => value }
        val rollback = events.collectFirst { case value: Event.Phase2Replaced => value }
        expect.all(
          replacement.exists(r => r.oldTip == a3 && r.newTip == b3 && r.oldTip.ordinal == r.newTip.ordinal),
          rollback.exists(r => r.oldHead == a3 && r.newHead.contains(genesis) && r.commonAncestor == genesis),
          next.statusOf(a3) == ExactStatus.Orphaned(Phase.P2Operational, Some(b3)),
          next.statusOf(b3) == ExactStatus.Canonical(Phase.P1Provisional),
          next.requireOperational(a3) == Left(ModelError.NotCanonical(a3)),
          next.phase2Head.contains(genesis)
        )
      case Left(error) => failure(s"unexpected model error: $error")
    }
  }

  pureTest("[FIN-D-003] k2 maturity is retention metadata and cannot block a denser replacement") {
    val oldP2 = applyCommand(canonicalLong, Command.QualifyPhase2(a3, decided(a3)))
    val mature = applyCommand(oldP2, Command.MarkRetentionMature(a3))
    val b1 = ref(1L, '4', genesis.hash, 'e')
    val b2 = ref(2L, '5', b1.hash, 'f')
    val b3 = ref(3L, '6', b2.hash, '7')
    val b4 = ref(4L, 'b', b3.hash, '8')
    val b5 = ref(5L, 'c', b4.hash, '9')
    val b6 = ref(6L, 'd', b5.hash, '0')
    val withFork = observeAll(mature, List(b1, b2, b3, b4, b5, b6))
    val replaced = FinalityReferenceModel.step(withFork, Command.SelectCanonical(b6, ForkChoiceRule.MaxValidBg))

    replaced match {
      case Right(StepResult(next, _)) =>
        expect.all(
          mature.retentionStatus(a3) == RetentionStatus.RetentionMature,
          next.canonicalTip.contains(b6),
          next.retentionStatus(a3) == RetentionStatus.RetentionMature,
          next.statusOf(a3).isInstanceOf[ExactStatus.Orphaned]
        )
      case Left(error) => failure(s"k2 metadata incorrectly blocked replacement: $error")
    }
  }

  pureTest("[FIN-D-003] k2 maturity derives depth from canonical ancestry") {
    val shallow = FinalityReferenceModel.step(canonicalA, Command.MarkRetentionMature(a3))
    val sibling = ref(3L, '4', a2.hash, 'e')
    val withSibling = applyCommand(canonicalA, Command.ObserveExecutedCandidate(sibling))
    val noncanonical = FinalityReferenceModel.step(withSibling, Command.MarkRetentionMature(sibling))

    expect.all(
      shallow == Left(ModelError.InsufficientRetentionDepth(0L, parameters.k2Retention)),
      noncanonical == Left(ModelError.NotCanonical(sibling))
    )
  }

  pureTest("[FIN-M-003][FIN-D-003] P2 qualification and k2 maturity commands are idempotent") {
    val qualified = FinalityReferenceModel.step(canonicalA, Command.QualifyPhase2(a3, decided(a3)))

    qualified match {
      case Right(StepResult(phase2, firstEvents)) =>
        val repeatedQualification = FinalityReferenceModel.step(phase2, Command.QualifyPhase2(a3, decided(a3)))
        val matured = FinalityReferenceModel.step(canonicalLong, Command.MarkRetentionMature(a3))
        matured match {
          case Right(StepResult(retained, maturityEvents)) =>
            val repeatedMaturity = FinalityReferenceModel.step(
              retained,
              Command.MarkRetentionMature(a3)
            )
            expect.all(
              firstEvents == List(Event.Phase2Advanced(a3, decided(a3))),
              repeatedQualification == Right(StepResult(phase2, Nil)),
              maturityEvents == List(Event.RetentionMatured(a3)),
              repeatedMaturity == Right(StepResult(retained, Nil))
            )
          case Left(error) => failure(s"unexpected first maturity error: $error")
        }
      case Left(error) => failure(s"unexpected first qualification error: $error")
    }
  }

  pureTest("[FIN-D-003][FIN-W-002] missing ancestry enters RecoveryRequired and halts further mutation") {
    val selected = canonicalA
    val missing = hash('8')
    val disconnected = ref(8L, '9', missing, 'a')
    val observed = applyCommand(selected, Command.ObserveExecutedCandidate(disconnected))
    val recoveryResult = FinalityReferenceModel.step(observed, Command.SelectCanonical(disconnected, ForkChoiceRule.MaxValidBg))

    recoveryResult match {
      case Right(StepResult(recovering, List(Event.EnteredRecovery(missingHash, competingTip)))) =>
        val attemptedMutation = FinalityReferenceModel.step(
          recovering,
          Command.QualifyPhase2(a3, Phase2Evidence.Depth)
        )
        expect.all(
          missingHash == missing,
          competingTip == disconnected,
          recovering.canonicalTip == selected.canonicalTip,
          recovering.phase2Head == selected.phase2Head,
          attemptedMutation == Left(ModelError.Halted(Mode.RecoveryRequired(missing, disconnected)))
        )
      case other => failure(s"expected exact RecoveryRequired transition, got $other")
    }
  }
}
