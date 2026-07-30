package io.constellationnetwork.node.shared.domain.snapshot.finality

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._

import io.constellationnetwork.node.shared.domain.snapshot.finality.ExactFinalityPhaseKernel.Command._
import io.constellationnetwork.node.shared.domain.snapshot.finality.ExactFinalityPhaseKernel.Event._
import io.constellationnetwork.node.shared.domain.snapshot.finality.ExactFinalityPhaseKernel.ExactStatus._
import io.constellationnetwork.node.shared.domain.snapshot.finality.ExactFinalityPhaseKernel.Mode._
import io.constellationnetwork.node.shared.domain.snapshot.finality.ExactFinalityPhaseKernel.Phase._
import io.constellationnetwork.node.shared.domain.snapshot.finality.ExactFinalityPhaseKernel._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot

import eu.timepit.refined.types.numeric.NonNegLong
import org.scalacheck.Gen
import scodec.bits.ByteVector
import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers

object ExactFinalityPhaseKernelSuite extends SimpleIOSuite with Checkers {

  private final case class ReorgScenario(
    mainTipOrdinal: Int,
    forkAtOrdinal: Int,
    forkSuffixLength: Int,
    qualifyAtOrdinal: Int
  )

  private implicit val reorgScenarioShow: cats.Show[ReorgScenario] =
    cats.Show.fromToString

  private def hash(seed: Int): Hash = Hash(f"$seed%064x")
  private def branchRevision(value: Long): CanonicalBranchRevision =
    CanonicalBranchRevision(NonNegLong.unsafeFrom(value))
  private def lineageRevision(value: Long): CanonicalLineageRevision =
    CanonicalLineageRevision(NonNegLong.unsafeFrom(value))

  private def stateRef(
    ordinal: Long,
    seed: Int,
    parentHash: Hash
  ): GlobalSnapshotStateRef =
    GlobalSnapshotStateRef(
      SnapshotOrdinal.unsafeApply(ordinal),
      hash(seed),
      parentHash,
      MptRoot(hash(seed + 100000))
    )

  private val genesis: GlobalSnapshotStateRef =
    stateRef(ordinal = 0, seed = 1, parentHash = Hash.empty)

  private def extend(
    prefix: Vector[GlobalSnapshotStateRef],
    finalOrdinal: Int,
    seedBase: Int
  ): Vector[GlobalSnapshotStateRef] =
    ((prefix.last.ordinal.value.value.toInt + 1) to finalOrdinal).foldLeft(prefix) {
      case (chain, ordinal) =>
        chain :+ stateRef(ordinal.toLong, seedBase + ordinal, chain.last.hash)
    }

  private def artifact(
    kind: FinalityArtifactKind,
    seed: Int
  ): ImmutableArtifactPointer =
    FinalityIdentity
      .artifactPointerFromBytes(kind, ByteVector.fromValidHex(f"$seed%08x"))
      .fold(throw _, identity)

  private def selectionEvidence(tip: GlobalSnapshotStateRef): ImmutableArtifactPointer =
    FinalityIdentity
      .artifactPointerFromBytes(
        FinalityArtifactKind.ForkChoiceDecisionEvidence,
        ByteVector.view(tip.hash.value.getBytes(StandardCharsets.UTF_8))
      )
      .fold(throw _, identity)

  private def applyOrThrow(
    state: State,
    command: Command
  ): Transition =
    applyCommand(state, command).fold(
      error => throw new IllegalStateException(s"Unexpected exact-finality transition failure: $error"),
      identity
    )

  private def observeAll(
    initial: State,
    refs: Iterable[GlobalSnapshotStateRef]
  ): State =
    refs.foldLeft(initial) {
      case (state, ref) => applyOrThrow(state, Observe(new ValidatedExecutedCandidate(ref))).state
    }

  private def select(
    state: State,
    tip: GlobalSnapshotStateRef
  ): Transition =
    applyOrThrow(
      state,
      Select(new VerifiedCanonicalSelection(state.branchRevision, tip, selectionEvidence(tip)))
    )

  private def qualifyByDepth(
    state: State,
    target: GlobalSnapshotStateRef,
    descendant: GlobalSnapshotStateRef,
    evidenceSeed: Int
  ): Transition =
    applyOrThrow(
      state,
      Qualify(
        new VerifiedOperationalQualification(
          state.lineageRevision,
          target,
          OperationalEvidenceRef(
            OperationalRail.CanonicalDepthK1,
            descendant,
            artifact(FinalityArtifactKind.DepthK1Evidence, evidenceSeed)
          )
        )
      )
    )

  private def buildScenario(
    scenario: ReorgScenario
  ): (Vector[GlobalSnapshotStateRef], Vector[GlobalSnapshotStateRef]) = {
    val main = extend(Vector(genesis), scenario.mainTipOrdinal, seedBase = 1000)
    val commonPrefix = main.take(scenario.forkAtOrdinal + 1)
    val fork =
      extend(
        commonPrefix,
        scenario.forkAtOrdinal + scenario.forkSuffixLength,
        seedBase = 10000
      )

    main -> fork
  }

  pureTest("[FIN-M-001] phases are exact-reference states and do not leak across one ordinal") {
    val main = extend(Vector(genesis), finalOrdinal = 2, seedBase = 1000)
    val sibling = stateRef(ordinal = 1, seed = 9001, parentHash = genesis.hash)
    val observed = observeAll(State.empty, main :+ sibling)
    val canonical = select(observed, main.last).state
    val operational = qualifyByDepth(canonical, main(1), main(2), evidenceSeed = 101).state

    expect.same(Canonical(P2Operational), operational.statusOf(genesis)) &&
    expect.same(Canonical(P2Operational), operational.statusOf(main(1))) &&
    expect.same(Canonical(P1Provisional), operational.statusOf(main(2))) &&
    expect.same(Observed(P0Pending), operational.statusOf(sibling)) &&
    expect.same(Set(main(1), sibling), operational.refsAtOrdinal(SnapshotOrdinal.unsafeApply(1L))) &&
    expect(operational.requireOperational(main(1)).isRight) &&
    expect.same(main(1), operational.requireOperational(genesis).toOption.get.operationalTarget) &&
    expect(operational.requireOperational(sibling).isLeft)
  }

  pureTest("[FIN-D-002] exact replacement rolls P2 back and ABA does not resurrect historical P2") {
    val main = extend(Vector(genesis), finalOrdinal = 3, seedBase = 1000)
    val fork = extend(main.take(2), finalOrdinal = 3, seedBase = 10000)
    val observed = observeAll(State.empty, main ++ fork)
    val selectedMain = select(observed, main.last).state
    val operationalMain = qualifyByDepth(selectedMain, main(2), main(3), evidenceSeed = 102).state
    val replacement = select(operationalMain, fork.last)

    val replacementEvent = replacement.events.collectFirst { case event: CanonicalReplaced => event }
    val operationalEvent = replacement.events.collectFirst { case event: OperationalReplaced => event }

    val replacementChecks =
      expect.same(
        Some(
          CanonicalReplaced(
            main.last,
            fork.last,
            main(1),
            main.drop(2),
            fork.drop(2),
            selectionEvidence(fork.last),
            CanonicalBranchRevision(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(2L)),
            CanonicalLineageRevision(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(1L))
          )
        ),
        replacementEvent
      ) &&
        expect.same(
          Some(
            OperationalReplaced(
              main(2),
              Some(main(1)),
              main(1),
              Vector(main(2)),
              Vector.empty,
              CanonicalBranchRevision(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(2L)),
              CanonicalLineageRevision(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(1L))
            )
          ),
          operationalEvent
        ) &&
        expect.same(Orphaned(P2Operational, fork.last), replacement.state.statusOf(main(2))) &&
        expect.same(Orphaned(P1Provisional, fork.last), replacement.state.statusOf(main(3))) &&
        expect.same(Canonical(P1Provisional), replacement.state.statusOf(fork(2))) &&
        expect.same(Canonical(P1Provisional), replacement.state.statusOf(fork(3))) &&
        expect.same(Some(main(1)), replacement.state.operationalHead)

    val returned = select(replacement.state, main.last)

    val requalified =
      qualifyByDepth(returned.state, main(2), main(3), evidenceSeed = 1002).state
    val main2EvidenceHistory = requalified.operationalEvidenceHistory(main(2).hash)

    replacementChecks &&
    expect.same(Canonical(P1Provisional), returned.state.statusOf(main(2))) &&
    expect.same(Canonical(P1Provisional), returned.state.statusOf(main(3))) &&
    expect.same(Some(main(1)), returned.state.operationalHead) &&
    expect.same(2L, returned.state.lineageRevision.value.value) &&
    expect.same(Vector(0L, 2L), main2EvidenceHistory.map(_.lineageRevision.value.value)) &&
    expect.same(2, main2EvidenceHistory.map(_.evidence.evidence.id).distinct.size)
  }

  pureTest("[FIN-R-001] unavailable exact ancestry enters absorbing RecoveryRequired") {
    val missingParent = hash(7000)
    val dangling = stateRef(ordinal = 2, seed = 7001, parentHash = missingParent)
    val observed = observeAll(State.empty, Vector(genesis, dangling))
    val recovery = select(observed, dangling)
    val expectedReason = RecoveryReason.MissingAncestor(missingParent, dangling)

    val halted = applyCommand(
      recovery.state,
      Observe(new ValidatedExecutedCandidate(stateRef(ordinal = 1, seed = 7000, parentHash = genesis.hash)))
    )

    expect.same(RecoveryRequired(expectedReason), recovery.state.mode) &&
    expect.same(Vector(EnteredRecovery(expectedReason)), recovery.events) &&
    expect.same(Left(Error.Halted(RecoveryRequired(expectedReason))), halted)
  }

  pureTest("[FIN-R-003] RecoveryRequired revokes authority to serve every previously operational reference") {
    val main = extend(Vector(genesis), finalOrdinal = 2, seedBase = 1000)
    val selected = select(observeAll(State.empty, main), main.last).state
    val operational = qualifyByDepth(selected, main(1), main(2), evidenceSeed = 108).state
    val missingParent = hash(8100)
    val dangling = stateRef(ordinal = 3L, seed = 8101, parentHash = missingParent)
    val withDangling = observeAll(operational, Vector(dangling))
    val recovery = select(withDangling, dangling)
    val expected = RecoveryRequired(RecoveryReason.MissingAncestor(missingParent, dangling))

    expect.same(expected, recovery.state.mode) &&
    expect(
      Vector(genesis, main(1)).forall(ref => recovery.state.requireOperational(ref) == Left(Error.Halted(expected)))
    )
  }

  pureTest("[FIN-R-002] retention maturity is local metadata and cannot qualify or select a snapshot") {
    val sibling = stateRef(ordinal = 1, seed = 8001, parentHash = genesis.hash)
    val before = observeAll(State.empty, Vector(genesis, sibling))
    val transition =
      applyOrThrow(before, MarkRetentionMature(new VerifiedRetentionMaturity(sibling)))
    val after = transition.state

    expect.same(Vector(RetentionMatured(sibling)), transition.events) &&
    expect.same(RetentionStatus.RetentionMature, after.retentionStatusOf(sibling)) &&
    expect.same(
      RetentionStatus.Unknown,
      after.retentionStatusOf(sibling.copy(mptRoot = MptRoot(hash(999999))))
    ) &&
    expect.same(
      RetentionStatus.Unknown,
      after.retentionStatusOf(stateRef(ordinal = 9, seed = 9999, parentHash = hash(9998)))
    ) &&
    expect.same(Observed(P0Pending), after.statusOf(sibling)) &&
    expect.same(before.canonicalOldestFirst, after.canonicalOldestFirst) &&
    expect.same(before.operationalHead, after.operationalHead) &&
    expect.same(before.phaseByHash, after.phaseByHash) &&
    expect.same(before.lineageRevision, after.lineageRevision) &&
    expect.same(before.operationalEvidenceHistory, after.operationalEvidenceHistory)
  }

  pureTest("qualification rejects an evidence kind that does not match its ratified rail") {
    val main = extend(Vector(genesis), finalOrdinal = 1, seedBase = 1000)
    val canonical = select(observeAll(State.empty, main), main.last).state
    val wrongEvidence = OperationalEvidenceRef(
      OperationalRail.DecidedAttestationTWeight,
      main.last,
      artifact(FinalityArtifactKind.DepthK1Evidence, seed = 103)
    )
    val result =
      applyCommand(
        canonical,
        Qualify(new VerifiedOperationalQualification(canonical.lineageRevision, main.last, wrongEvidence))
      )

    expect.same(
      Left(
        Error.WrongEvidenceKind(
          FinalityArtifactKind.DecidedAttestationEvidence,
          FinalityArtifactKind.DepthK1Evidence
        )
      ),
      result
    )
  }

  pureTest("canonical selection rejects a pointer that is not fork-choice decision evidence") {
    val main = extend(Vector(genesis), finalOrdinal = 1, seedBase = 1000)
    val observed = observeAll(State.empty, main)
    val wrongEvidence = artifact(FinalityArtifactKind.DepthK1Evidence, seed = 111)
    val result =
      applyCommand(
        observed,
        Select(new VerifiedCanonicalSelection(observed.branchRevision, main.last, wrongEvidence))
      )

    expect.same(
      Left(
        Error.WrongEvidenceKind(
          FinalityArtifactKind.ForkChoiceDecisionEvidence,
          FinalityArtifactKind.DepthK1Evidence
        )
      ),
      result
    )
  }

  pureTest("first P2 qualification evidence is not overwritten when a later rail advances the prefix") {
    val main = extend(Vector(genesis), finalOrdinal = 2, seedBase = 1000)
    val canonical = select(observeAll(State.empty, main), main.last).state
    val depthQualified = qualifyByDepth(canonical, main(1), main(2), evidenceSeed = 105).state
    val decidedEvidence = OperationalEvidenceRef(
      OperationalRail.DecidedAttestationTWeight,
      main(2),
      artifact(FinalityArtifactKind.DecidedAttestationEvidence, seed = 106)
    )
    val decidedQualified =
      applyOrThrow(
        depthQualified,
        Qualify(new VerifiedOperationalQualification(depthQualified.lineageRevision, main(2), decidedEvidence))
      ).state

    val genesisEvidence = decidedQualified.operationalEvidenceHistory(genesis.hash)
    val firstTargetEvidence = decidedQualified.operationalEvidenceHistory(main(1).hash)
    val secondTargetEvidence = decidedQualified.operationalEvidenceHistory(main(2).hash)

    expect.same(1, genesisEvidence.size) &&
    expect.same(1, firstTargetEvidence.size) &&
    expect.same(1, secondTargetEvidence.size) &&
    expect.same(OperationalRail.CanonicalDepthK1, genesisEvidence.head.evidence.rail) &&
    expect.same(OperationalRail.CanonicalDepthK1, firstTargetEvidence.head.evidence.rail) &&
    expect.same(OperationalRail.DecidedAttestationTWeight, secondTargetEvidence.head.evidence.rail)
  }

  pureTest("a delayed canonical-selection capability cannot replace a newer branch revision") {
    val main = extend(Vector(genesis), finalOrdinal = 1, seedBase = 1000)
    val fork = extend(Vector(genesis), finalOrdinal = 1, seedBase = 10000)
    val observed = observeAll(State.empty, main ++ fork)
    val delayedMainSelection =
      new VerifiedCanonicalSelection(observed.branchRevision, main.last, selectionEvidence(main.last))
    val selectedFork =
      applyOrThrow(
        observed,
        Select(new VerifiedCanonicalSelection(observed.branchRevision, fork.last, selectionEvidence(fork.last)))
      ).state
    val staleResult = applyCommand(selectedFork, Select(delayedMainSelection))

    expect.same(
      Left(Error.StaleCanonicalSelection(observed.branchRevision, selectedFork.branchRevision)),
      staleResult
    ) &&
    expect.same(fork, selectedFork.canonicalOldestFirst) &&
    expect.same(Running, selectedFork.mode)
  }

  pureTest("a qualification capability from an earlier lineage cannot become valid again after ABA") {
    val main = extend(Vector(genesis), finalOrdinal = 2, seedBase = 1000)
    val fork = extend(Vector(genesis), finalOrdinal = 2, seedBase = 10000)
    val observed = observeAll(State.empty, main ++ fork)
    val selectedMain = select(observed, main.last).state
    val staleQualification =
      new VerifiedOperationalQualification(
        selectedMain.lineageRevision,
        main(1),
        OperationalEvidenceRef(
          OperationalRail.CanonicalDepthK1,
          main.last,
          artifact(FinalityArtifactKind.DepthK1Evidence, seed = 107)
        )
      )
    val selectedFork = select(selectedMain, fork.last).state
    val returnedMain = select(selectedFork, main.last).state
    val staleResult = applyCommand(returnedMain, Qualify(staleQualification))

    expect.same(
      Left(
        Error.StaleOperationalQualification(
          selectedMain.lineageRevision,
          returnedMain.lineageRevision
        )
      ),
      staleResult
    ) &&
    expect.same(Canonical(P1Provisional), returnedMain.statusOf(main(1))) &&
    expect.same(Running, returnedMain.mode)
  }

  pureTest("authority-bearing events distinguish delayed delivery across exact-hash ABA") {
    val main = extend(Vector(genesis), finalOrdinal = 1, seedBase = 1000)
    val fork = extend(Vector(genesis), finalOrdinal = 1, seedBase = 10000)
    val observed = observeAll(State.empty, main ++ fork)
    val selectedMain = select(observed, main.last).state
    val firstQualification = qualifyByDepth(selectedMain, main.last, main.last, evidenceSeed = 109)
    val firstEvent = firstQualification.events.collectFirst { case event: OperationalAdvanced => event }.get
    val selectedFork = select(firstQualification.state, fork.last).state
    val returnedMain = select(selectedFork, main.last).state
    val secondQualification = qualifyByDepth(returnedMain, main.last, main.last, evidenceSeed = 110)
    val secondEvent = secondQualification.events.collectFirst { case event: OperationalAdvanced => event }.get

    expect.same(firstEvent.newHead, secondEvent.newHead) &&
    expect.same(branchRevision(1L), firstEvent.branchRevision) &&
    expect.same(lineageRevision(0L), firstEvent.lineageRevision) &&
    expect.same(branchRevision(3L), secondEvent.branchRevision) &&
    expect.same(lineageRevision(2L), secondEvent.lineageRevision) &&
    expect(firstEvent != secondEvent)
  }

  pureTest("canonical extension preserves lineage and a previously verified exact qualification") {
    val first = extend(Vector(genesis), finalOrdinal = 1, seedBase = 1000)
    val complete = extend(first, finalOrdinal = 2, seedBase = 1000)
    val observed = observeAll(State.empty, complete)
    val selected = select(observed, first.last).state
    val qualification =
      new VerifiedOperationalQualification(
        selected.lineageRevision,
        first.last,
        OperationalEvidenceRef(
          OperationalRail.CanonicalDepthK1,
          first.last,
          artifact(FinalityArtifactKind.DepthK1Evidence, seed = 104)
        )
      )
    val extension = select(selected, complete.last)
    val qualifiedAfterExtension = applyOrThrow(extension.state, Qualify(qualification)).state

    expect.same(Canonical(P2Operational), qualifiedAfterExtension.statusOf(genesis)) &&
    expect.same(Canonical(P2Operational), qualifiedAfterExtension.statusOf(first.last)) &&
    expect.same(Canonical(P1Provisional), qualifiedAfterExtension.statusOf(complete.last)) &&
    expect.same(0L, qualifiedAfterExtension.lineageRevision.value.value) &&
    expect.same(2L, qualifiedAfterExtension.branchRevision.value.value) &&
    expect.same(
      Vector(
        CanonicalAdvanced(
          Some(first.last),
          complete.last,
          Vector(complete.last),
          selectionEvidence(complete.last),
          CanonicalBranchRevision(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(2L))
        )
      ),
      extension.events
    )
  }

  pureTest("branch revision exhaustion fails closed on initial selection and extension") {
    val main = extend(Vector(genesis), finalOrdinal = 2, seedBase = 1000)
    val observed = observeAll(State.empty, main)
    val maxBranch = branchRevision(Long.MaxValue)

    val initialState = observed.copy(branchRevision = maxBranch)
    val initialRecovery = select(initialState, main(1))
    val initialReason = RecoveryReason.BranchRevisionExhausted(maxBranch)
    val initialHalted =
      applyCommand(initialRecovery.state, Observe(new ValidatedExecutedCandidate(main(2))))

    val selected = select(observed, main(1)).state
    val extensionState = selected.copy(branchRevision = maxBranch)
    val extensionRecovery = select(extensionState, main(2))

    expect.same(RecoveryRequired(initialReason), initialRecovery.state.mode) &&
    expect.same(Left(Error.Halted(RecoveryRequired(initialReason))), initialHalted) &&
    expect.same(RecoveryRequired(initialReason), extensionRecovery.state.mode) &&
    expect.same(main.take(2), extensionRecovery.state.canonicalOldestFirst) &&
    expect.same(maxBranch, extensionRecovery.state.branchRevision)
  }

  pureTest("lineage revision exhaustion rejects replacement without partially advancing branch state") {
    val main = extend(Vector(genesis), finalOrdinal = 1, seedBase = 1000)
    val fork = extend(Vector(genesis), finalOrdinal = 1, seedBase = 10000)
    val selectedMain = select(observeAll(State.empty, main ++ fork), main.last).state
    val maxLineage = lineageRevision(Long.MaxValue)
    val before = selectedMain.copy(lineageRevision = maxLineage)
    val recovery = select(before, fork.last)

    expect.same(
      RecoveryRequired(RecoveryReason.LineageRevisionExhausted(maxLineage)),
      recovery.state.mode
    ) &&
    expect.same(before.branchRevision, recovery.state.branchRevision) &&
    expect.same(before.canonicalOldestFirst, recovery.state.canonicalOldestFirst) &&
    expect.same(before.operationalHead, recovery.state.operationalHead)
  }

  pureTest("Long.MaxValue ordinal adjacency is checked without signed overflow") {
    val missingGrandparent = hash(8200)
    val parent =
      stateRef(
        ordinal = Long.MaxValue - 1L,
        seed = 8201,
        parentHash = missingGrandparent
      )
    val child =
      stateRef(
        ordinal = Long.MaxValue,
        seed = 8202,
        parentHash = parent.hash
      )
    val observed = observeAll(State.empty, Vector(parent, child))
    val recovery = select(observed, child)

    expect.same(
      RecoveryRequired(RecoveryReason.MissingAncestor(missingGrandparent, child)),
      recovery.state.mode
    )
  }

  pureTest("same hash with a different exact reference is rejected as a collision") {
    val observed = observeAll(State.empty, Vector(genesis))
    val collision = genesis.copy(mptRoot = MptRoot(hash(8300)))
    val result =
      applyCommand(observed, Observe(new ValidatedExecutedCandidate(collision)))

    expect.same(Left(Error.HashCollision(genesis, collision)), result)
  }

  pureTest("a known parent with an ordinal gap is rejected without fail-stopping the node") {
    val gap = stateRef(ordinal = 2L, seed = 8400, parentHash = genesis.hash)
    val observed = observeAll(State.empty, Vector(genesis, gap))
    val result =
      applyCommand(
        observed,
        Select(new VerifiedCanonicalSelection(observed.branchRevision, gap, selectionEvidence(gap)))
      )

    result match {
      case Left(Error.InvalidLineage(child, knownParent, detail)) =>
        expect.same(gap, child) &&
        expect.same(genesis, knownParent) &&
        expect(detail.contains("ordinal")) &&
        expect.same(Running, observed.mode)
      case other => failure(s"Expected typed invalid-lineage rejection, got $other")
    }
  }

  test("[FIN-M-001][FIN-D-002] PROPERTY: independent exact-frontier oracle agrees across reorg and ABA") {
    val scenarioGen =
      for {
        mainTip <- Gen.chooseNum(3, 12)
        forkAt <- Gen.chooseNum(0, mainTip - 1)
        forkSuffixLength <- Gen.chooseNum(1, 8)
        qualifyAt <- Gen.chooseNum(0, mainTip)
      } yield ReorgScenario(mainTip, forkAt, forkSuffixLength, qualifyAt)

    forall(scenarioGen) { scenario =>
      val (main, fork) = buildScenario(scenario)
      val observed = observeAll(State.empty, main ++ fork)
      val selectedMain = select(observed, main.last).state
      val qualified =
        qualifyByDepth(
          selectedMain,
          main(scenario.qualifyAtOrdinal),
          main.last,
          evidenceSeed = 200 + scenario.qualifyAtOrdinal
        ).state
      val replacement = select(qualified, fork.last)

      // This expected state is derived solely from the generated chain geometry.
      val expectedOperationalOrdinal = math.min(scenario.qualifyAtOrdinal, scenario.forkAtOrdinal)
      val expectedOperationalHead = main(expectedOperationalOrdinal)
      val commonPrefixChecks =
        main
          .take(scenario.forkAtOrdinal + 1)
          .zipWithIndex
          .forall {
            case (ref, ordinal) =>
              val expectedPhase =
                if (ordinal <= expectedOperationalOrdinal) P2Operational else P1Provisional
              replacement.state.statusOf(ref) == Canonical(expectedPhase)
          }
      val forkSuffixChecks =
        fork.drop(scenario.forkAtOrdinal + 1).forall(ref => replacement.state.statusOf(ref) == Canonical(P1Provisional))
      val orphanedChecks =
        main
          .drop(scenario.forkAtOrdinal + 1)
          .zipWithIndex
          .forall {
            case (ref, suffixIndex) =>
              val ordinal = scenario.forkAtOrdinal + suffixIndex + 1
              val expectedPreviousPhase =
                if (ordinal <= scenario.qualifyAtOrdinal) P2Operational else P1Provisional
              replacement.state.statusOf(ref) == Orphaned(expectedPreviousPhase, fork.last)
          }
      val exactReplacementEvent =
        replacement.events.collectFirst { case event: CanonicalReplaced => event }.contains(
          CanonicalReplaced(
            main.last,
            fork.last,
            main(scenario.forkAtOrdinal),
            main.drop(scenario.forkAtOrdinal + 1),
            fork.drop(scenario.forkAtOrdinal + 1),
            selectionEvidence(fork.last),
            CanonicalBranchRevision(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(2L)),
            CanonicalLineageRevision(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(1L))
          )
        )
      val expectedOrphanedOperational =
        main
          .drop(scenario.forkAtOrdinal + 1)
          .filter(_.ordinal.value.value <= scenario.qualifyAtOrdinal.toLong)
      val exactOperationalReplacement =
        if (scenario.qualifyAtOrdinal <= scenario.forkAtOrdinal)
          replacement.events.forall(event => !event.isInstanceOf[OperationalReplaced])
        else
          replacement.events.collectFirst { case event: OperationalReplaced => event }.contains(
            OperationalReplaced(
              main(scenario.qualifyAtOrdinal),
              Some(expectedOperationalHead),
              main(scenario.forkAtOrdinal),
              expectedOrphanedOperational,
              Vector.empty,
              CanonicalBranchRevision(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(2L)),
              CanonicalLineageRevision(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(1L))
            )
          )

      val returned = select(replacement.state, main.last)
      val returnedSuffixIsFreshP1 =
        main
          .drop(scenario.forkAtOrdinal + 1)
          .forall(ref => returned.state.statusOf(ref) == Canonical(P1Provisional))

      expect.same(fork, replacement.state.canonicalOldestFirst) &&
      expect(commonPrefixChecks) &&
      expect(forkSuffixChecks) &&
      expect(orphanedChecks) &&
      expect(exactReplacementEvent) &&
      expect(exactOperationalReplacement) &&
      expect.same(Some(expectedOperationalHead), replacement.state.operationalHead) &&
      expect.same(1L, replacement.state.lineageRevision.value.value) &&
      expect(returnedSuffixIsFreshP1) &&
      expect.same(Some(expectedOperationalHead), returned.state.operationalHead) &&
      expect.same(2L, returned.state.lineageRevision.value.value)
    }
  }

  pureTest("source guard proves no direct production JVM-source reference outside the dark kernel") {
    val root = repositoryRoot()
    val productionSources =
      jvmSources(root.resolve("modules")).filter { path =>
        path.iterator().asScala.map(_.toString).toVector.sliding(2).contains(Vector("src", "main"))
      }
    val callers =
      productionSources
        .filter(path => read(path).contains("ExactFinalityPhaseKernel"))
        .map(root.relativize)
        .map(_.toString)
        .toSet
    val kernelPath =
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/snapshot/finality/ExactFinalityPhaseKernel.scala"
    val kernelSource = read(root.resolve(kernelPath))
    val forbiddenLiveDependencies =
      Set("FinalityGate", "finalizeSelectedAt", "ChainSelection", "SnowballAccumulator").filter(kernelSource.contains)

    expect.same(Set(kernelPath), callers) &&
    expect.same(Set.empty[String], forbiddenLiveDependencies)
  }

  private def repositoryRoot(): Path = {
    val start = Paths.get(System.getProperty("user.dir")).toAbsolutePath.normalize()
    Iterator
      .iterate(start)(path => Option(path.getParent).orNull)
      .takeWhile(_ != null)
      .find(path => Files.isDirectory(path.resolve("modules/node-shared")))
      .getOrElse(throw new IllegalStateException(s"Unable to locate repository root from $start"))
  }

  private def jvmSources(root: Path): Vector[Path] = {
    val sourceExtensions = Set(".scala", ".java", ".kt", ".kts")
    val stream = Files.walk(root)
    try
      stream
        .iterator()
        .asScala
        .filter(path => Files.isRegularFile(path) && sourceExtensions.exists(path.toString.endsWith))
        .toVector
    finally stream.close()
  }

  private def read(path: Path): String =
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
}
