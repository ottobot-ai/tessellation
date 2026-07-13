package io.constellationnetwork.node.shared.domain.nakamoto.overlay.model

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.model.ExactBranchReferenceModel._

import weaver.SimpleIOSuite

object ExactBranchReferenceModelSuite extends SimpleIOSuite {

  private val parentOfGenesis = SnapshotHash("parent-of-genesis")

  private def image(entries: (String, Long)*): StateImage = StateImage(entries.toMap)

  private def ref(ordinal: Long, hash: String, parent: SnapshotHash, postImage: StateImage): SnapshotRef =
    SnapshotRef(ordinal, SnapshotHash(hash), parent, postImage.root)

  private val genesisImage = image("alice" -> 100L)
  private val genesis = ref(0L, "g", parentOfGenesis, genesisImage)

  private def anchor(ref: SnapshotRef, image: StateImage): VerifiedBaseAnchor =
    VerifiedBaseAnchor.bind(ref, PersistedBase(ref, image)).fold(err => throw new AssertionError(err.toString), identity)

  private val genesisAnchor = anchor(genesis, genesisImage)

  private val a1Image = image("alice" -> 90L, "bob" -> 10L)
  private val a1 = ref(1L, "a1", genesis.hash, a1Image)
  private val a1Branch = PendingBranch(a1, StateDelta(Map("alice" -> 90L, "bob" -> 10L), Set.empty))

  private val a2Image = image("alice" -> 80L, "bob" -> 20L)
  private val a2 = ref(2L, "a2", a1.hash, a2Image)
  private val a2Branch = PendingBranch(a2, StateDelta(Map("alice" -> 80L, "bob" -> 20L), Set.empty))

  private val a3Image = image("alice" -> 75L, "bob" -> 25L)
  private val a3 = ref(3L, "a3", a2.hash, a3Image)
  private val a3Branch = PendingBranch(a3, StateDelta(Map("alice" -> 75L, "bob" -> 25L), Set.empty))

  private def applied[A](result: Result[A]): State = result match {
    case Result.Applied(state, _) => state
    case other                    => throw new AssertionError(s"expected applied result, got $other")
  }

  private def stageAll(initial: State, branches: PendingBranch*): State =
    branches.foldLeft(initial)((state, branch) => applied(ExactBranchReferenceModel.stage(state, branch)))

  private def canonicalState: State =
    stageAll(State.initial(genesisAnchor), a1Branch, a2Branch, a3Branch)

  pureTest("[ROOT-002] restart binds the base only to the exact hash, ordinal, and root") {
    val prior = canonicalState
    val exact = ExactBranchReferenceModel.restart(prior, genesis, PersistedBase(genesis, genesisImage))
    val wrongHashRef = genesis.copy(hash = SnapshotHash("other-g"))
    val wrongOrdinalRef = genesis.copy(ordinal = 1L)
    val wrongRootRef = genesis.copy(stateRoot = StateRoot("wrong-root"))
    val wrongHash = ExactBranchReferenceModel.restart(prior, genesis, PersistedBase(wrongHashRef, genesisImage))
    val wrongOrdinal = ExactBranchReferenceModel.restart(prior, genesis, PersistedBase(wrongOrdinalRef, genesisImage))
    val wrongRoot = ExactBranchReferenceModel.restart(prior, genesis, PersistedBase(wrongRootRef, genesisImage))

    expect.all(
      exact == Result.Applied(State.initial(genesisAnchor), ()),
      wrongHash == Result.RecoveryRequired(prior, RecoveryCause.RestartIdentityMismatch(genesis, wrongHashRef)),
      wrongOrdinal == Result.RecoveryRequired(prior, RecoveryCause.RestartIdentityMismatch(genesis, wrongOrdinalRef)),
      wrongRoot == Result.RecoveryRequired(prior, RecoveryCause.RestartIdentityMismatch(genesis, wrongRootRef)),
      wrongHash.state == prior,
      wrongOrdinal.state == prior,
      wrongRoot.state == prior
    )
  }

  pureTest("[ROOT-002] restart rejects persisted bytes whose reproduced root does not match the bound ref") {
    val prior = canonicalState
    val corruptImage = image("alice" -> 101L)
    val result = ExactBranchReferenceModel.restart(prior, genesis, PersistedBase(genesis, corruptImage))

    expect.all(
      result == Result.RecoveryRequired(prior, RecoveryCause.RestartRootMismatch(genesis.stateRoot, corruptImage.root)),
      result.state == prior
    )
  }

  pureTest("[ROOT-003] exact-parent acquisition reproduces a complete ordinal-consistent ancestry") {
    val state = canonicalState
    val result = ExactBranchReferenceModel.acquireExactParent(state, a3)

    expect(
      result == Result.Applied(
        state,
        ExactParentSession(a3, a3Image, Vector(a1, a2, a3))
      )
    )
  }

  pureTest("[ROOT-003] unknown or incomplete ancestry requires recovery with zero state mutation") {
    val unknown = ref(4L, "unknown", a3.hash, a3Image)
    val unknownResult = ExactBranchReferenceModel.acquireExactParent(canonicalState, unknown)
    val incompleteState = stageAll(State.initial(genesisAnchor), a2Branch)
    val incompleteResult = ExactBranchReferenceModel.acquireExactParent(incompleteState, a2)

    expect.all(
      unknownResult == Result.RecoveryRequired(canonicalState, RecoveryCause.UnknownExactParent(unknown)),
      incompleteResult == Result.RecoveryRequired(incompleteState, RecoveryCause.MissingAncestor(a2, a1.hash)),
      unknownResult.state == canonicalState,
      incompleteResult.state == incompleteState
    )
  }

  pureTest("[ROOT-003] ordinal gaps and reproduced-root mismatches require recovery without installing state") {
    val gapRef = ref(3L, "gap", a1.hash, a2Image)
    val gapBranch = PendingBranch(gapRef, a2Branch.delta)
    val gapState = stageAll(State.initial(genesisAnchor), a1Branch, gapBranch)
    val gapResult = ExactBranchReferenceModel.acquireExactParent(gapState, gapRef)
    val badRootRef = a2.copy(hash = SnapshotHash("bad-root"), stateRoot = StateRoot("claimed-root"))
    val badRootBranch = PendingBranch(badRootRef, a2Branch.delta)
    val badRootState = stageAll(State.initial(genesisAnchor), a1Branch, badRootBranch)
    val badRootResult = ExactBranchReferenceModel.acquireExactParent(badRootState, badRootRef)

    expect.all(
      gapResult == Result.RecoveryRequired(gapState, RecoveryCause.OrdinalGap(a1, gapRef)),
      badRootResult == Result.RecoveryRequired(
        badRootState,
        RecoveryCause.ReproducedRootMismatch(badRootRef, a2Image.root)
      ),
      gapResult.state == gapState,
      badRootResult.state == badRootState
    )
  }

  pureTest("[ROOT-004] prefix finalization retains exact canonical descendants and drops siblings") {
    val b2Image = image("alice" -> 85L, "bob" -> 10L, "carol" -> 5L)
    val b2 = ref(2L, "b2", a1.hash, b2Image)
    val b2Branch = PendingBranch(b2, StateDelta(Map("alice" -> 85L, "carol" -> 5L), Set.empty))
    val disconnectedImage = image("mallory" -> 1L)
    val disconnected = ref(10L, "disconnected", SnapshotHash("missing"), disconnectedImage)
    val disconnectedBranch = PendingBranch(disconnected, StateDelta(Map("mallory" -> 1L), Set("alice")))
    val before = stageAll(canonicalState, b2Branch, disconnectedBranch)
    val result = ExactBranchReferenceModel.finalizePrefix(before, a1, a3)
    val expectedPending = Map(a2.hash -> a2Branch, a3.hash -> a3Branch)
    val expectedAnchor = anchor(a1, a1Image)
    val expectedState = State(expectedAnchor, expectedPending)

    expect.all(
      result == Result.Applied(expectedState, ()),
      result.state.pending.keySet == Set(a2.hash, a3.hash),
      !result.state.pending.contains(b2.hash),
      !result.state.pending.contains(disconnected.hash),
      ExactBranchReferenceModel.acquireExactParent(expectedState, a3) ==
        Result.Applied(expectedState, ExactParentSession(a3, a3Image, Vector(a2, a3)))
    )
  }

  pureTest("[ROOT-004] injected finalization failure leaves the complete prior model state unchanged") {
    val before = canonicalState
    val result = ExactBranchReferenceModel.finalizePrefix(
      before,
      a2,
      a3,
      FinalizationFault.BeforeAtomicInstall
    )

    expect.all(
      result == Result.FinalizationFailed(before, "injected before atomic install"),
      result.state == before,
      result.state.anchor == genesisAnchor,
      result.state.pending == before.pending
    )
  }

  pureTest("[ROOT-004] finalization cannot splice a prefix from a sibling branch") {
    val b2Image = image("alice" -> 85L, "bob" -> 10L, "carol" -> 5L)
    val b2 = ref(2L, "b2", a1.hash, b2Image)
    val b2Branch = PendingBranch(b2, StateDelta(Map("alice" -> 85L, "carol" -> 5L), Set.empty))
    val before = stageAll(canonicalState, b2Branch)
    val result = ExactBranchReferenceModel.finalizePrefix(before, b2, a3)

    expect.all(
      result == Result.RecoveryRequired(before, RecoveryCause.CanonicalTipDoesNotContain(b2, a3)),
      result.state == before
    )
  }
}
