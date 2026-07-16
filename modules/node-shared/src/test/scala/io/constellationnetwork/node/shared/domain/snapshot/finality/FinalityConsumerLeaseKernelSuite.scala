package io.constellationnetwork.node.shared.domain.snapshot.finality

import io.constellationnetwork.node.shared.app.{CurrencyL1, DagL0, DagL1}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityConsumerLeaseFailure._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityConsumerLeaseKernel._
import io.constellationnetwork.node.shared.domain.snapshot.finality.Phase2CommitResult.{Committed, Stale}
import io.constellationnetwork.node.shared.domain.snapshot.finality.Phase2EvidenceComponent._
import io.constellationnetwork.node.shared.domain.snapshot.finality.Phase2ReferencePolicy.ExactCanonicalAncestor
import io.constellationnetwork.node.shared.domain.snapshot.finality.Phase2UseScope._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object FinalityConsumerLeaseKernelSuite extends SimpleIOSuite {
  private val released = FinalityCodecFixtures.releasedCore
  private val target = released.pointer.target
  private val prior = FinalityCodecFixtures.priorState

  private def hash(seed: Int): Hash = FinalityCodecFixtures.hash(seed)
  private def revision(value: Long): CanonicalBranchRevision = CanonicalBranchRevision(NonNegLong.unsafeFrom(value))
  private def lineage(value: Long): CanonicalLineageRevision = CanonicalLineageRevision(NonNegLong.unsafeFrom(value))

  private def releasedAt(reference: GlobalSnapshotStateRef): ReleasedCore = {
    val baseQualification = released.payload.qualification.asInstanceOf[OperationalQualification.DecidedAttestationTWeight]
    val qualification = baseQualification.copy(
      operationalTarget = reference,
      qualifyingDescendant = reference,
      ancestorClosure = None
    )
    val receipt = released.payload.receipt.copy(
      target = reference,
      activePublication = released.payload.receipt.activePublication.copy(
        image = released.payload.receipt.activePublication.image.map(_.copy(anchor = reference))
      )
    )

    released.copy(
      pointer = released.pointer.copy(target = reference),
      payload = released.payload.copy(qualification = qualification, receipt = receipt)
    )
  }

  private val admissionScope = BinaryAdmission(Address.fromBytes("mg-a".getBytes("UTF-8")), hash(700), hash(701))

  private def selection(
    exactTarget: GlobalSnapshotStateRef,
    path: Vector[GlobalSnapshotStateRef],
    branchRevision: Long
  ): CanonicalSelectionToken = {
    val tip = path.last
    val lineageCommitment = PathCommitment(
      PathSummary(
        PathRole.CanonicalLineage,
        exactTarget,
        tip,
        NonNegLong.unsafeFrom(path.size.toLong)
      ),
      FinalityCodecFixtures.pathManifestArtifact,
      FinalityIdentity.pathEntriesRoot(path).fold(throw _, identity)
    )
    CanonicalSelectionToken(
      revision(branchRevision),
      tip,
      exactTarget,
      ForkChoiceDecision(FinalityCodecFixtures.selectionEvidenceArtifact),
      lineageCommitment
    )
  }

  private def state(
    branchRevision: Long = 1L,
    lineageRevision: Long = 0L,
    canonical: Vector[GlobalSnapshotStateRef] = Vector(prior, target),
    currentSelections: Option[Map[GlobalSnapshotStateRef, CanonicalSelectionToken]] = None,
    releases: Map[GlobalSnapshotStateRef, ReleasedCore] = Map(target -> released),
    sink: Phase2ConsumerSink = Phase2ConsumerSink.empty
  ): FinalityConsumerLeaseState =
    FinalityConsumerLeaseState(
      CoordinatorMode.Running,
      revision(branchRevision),
      lineage(lineageRevision),
      canonical,
      currentSelections.getOrElse(
        releases.keysIterator.map { exactTarget =>
          val targetIndex = canonical.indexOf(exactTarget)
          exactTarget -> selection(exactTarget, canonical.drop(targetIndex), branchRevision)
        }.toMap
      ),
      releases,
      sink
    )

  private def exactReadbacks(value: ReleasedCore, selected: CanonicalSelectionToken): Phase2LocalReadbacks = {
    val qualification = value.payload.qualification
    val receipt = value.payload.receipt

    Phase2LocalReadbacks(
      released = Some(value),
      qualification = Some(qualification.scope),
      qualificationEvidence = Some(qualification.evidence.artifact),
      decisionEvidence = Some(selected.decision.evidence),
      lineage = Some(selected.lineage),
      publication = Some(receipt.activePublication),
      semanticReceipt = Some(receipt.semanticReceipt),
      authenticatedAnchorReceipt = Some(receipt.authenticatedAnchorReceipt)
    )
  }

  private def acquire(
    base: FinalityConsumerLeaseState,
    scope: Phase2UseScope = admissionScope,
    exactTarget: GlobalSnapshotStateRef = target
  ): CanonicalPhase2Lease =
    (for {
      descriptor <- capture(base, scope, exactTarget)
      verified <- verifyReadbacks(
        descriptor,
        exactReadbacks(base.releasedByTarget(exactTarget), base.currentSelectionByTarget(exactTarget))
      )
      lease <- acquireIfCurrent(base, verified)
    } yield lease).fold(error => throw new AssertionError(s"expected lease, got $error"), identity)

  private def command(
    id: Int,
    scope: Phase2UseScope = admissionScope,
    exactTarget: GlobalSnapshotStateRef = target,
    payload: Int = 800
  ): Phase2ClosedCommand =
    Phase2ClosedCommand(hash(id), hash(payload), scope, exactTarget)

  pureTest("the dark exact-ancestor scope slice assigns every implemented purpose explicitly") {
    val mg = Address.fromBytes("mg-scopes".getBytes("UTF-8"))
    val shard = ShardId.unsafeApply(2)
    val scopes: List[Phase2UseScope] = List(
      BinaryAdmission(mg, hash(1), hash(2)),
      BinaryConfirmationRequeue(mg, hash(3)),
      CurrencySnapshotReplay(mg, hash(19)),
      ShardExecutionBase(shard, hash(4)),
      CheckpointInclusion(shard, hash(5)),
      CheckpointAnchorAdvancement(shard, hash(6)),
      AssignedWatchtowerReplay(shard, hash(7)),
      ChallengeAdjudication(hash(8)),
      CrossMetagraphSettlement(hash(9)),
      HistoricalEconomicRead(hash(10)),
      OptimisticSamplerRegistryContext(hash(11)),
      TowerEligibilityRegistryContext(hash(12)),
      TowerProofServing(hash(13)),
      ProtocolCorrection(hash(14)),
      FollowerAdoption(DagL0),
      ExactServing(hash(15)),
      BootstrapBundleServing(hash(16)),
      RetentionRecovery(hash(17)),
      DownstreamEventDelivery(hash(18))
    )

    expect(scopes.size == 19) && expect(scopes.forall(Phase2ReferencePolicy.forScope(_) == ExactCanonicalAncestor))
  }

  pureTest("acquisition, verified-readback, and lease capabilities are not Java-serializable") {
    val base = state()
    val descriptor = capture(base, admissionScope, target).toOption.get
    val verified =
      verifyReadbacks(descriptor, exactReadbacks(base.releasedByTarget(target), base.currentSelectionByTarget(target))).toOption.get
    val lease = acquireIfCurrent(base, verified).toOption.get

    expect(!classOf[java.io.Serializable].isAssignableFrom(descriptor.getClass)) &&
    expect(!classOf[java.io.Serializable].isAssignableFrom(verified.getClass)) &&
    expect(!classOf[java.io.Serializable].isAssignableFrom(lease.getClass))
  }

  pureTest("same-ordinal substitution is a typed replacement, never exact-reference inheritance") {
    val replacement = target.copy(hash = hash(900), mptRoot = target.mptRoot.copy(value = hash(901)))
    val replacedState = state(
      branchRevision = 2L,
      lineageRevision = 1L,
      canonical = Vector(prior, replacement),
      releases = Map.empty
    )

    expect(capture(replacedState, admissionScope, target) == Left(ReferenceReplaced(target, replacement)))
  }

  pureTest("every GlobalSnapshotStateRef field is part of the captured exact identity") {
    val variants = List(
      target.copy(ordinal = SnapshotOrdinal.unsafeApply(target.ordinal.value.value + 1L)),
      target.copy(hash = hash(910)),
      target.copy(parentHash = hash(911)),
      target.copy(mptRoot = target.mptRoot.copy(value = hash(912)))
    )

    expect(variants.forall(candidate => capture(state(), admissionScope, candidate).isLeft))
  }

  pureTest("missing and mismatched local evidence cannot mint the verified capability") {
    val descriptor = capture(state(), admissionScope, target).toOption.get
    val base = state()
    val missing = exactReadbacks(released, base.currentSelectionByTarget(target)).copy(decisionEvidence = None)
    val missingQualification = exactReadbacks(released, base.currentSelectionByTarget(target)).copy(qualificationEvidence = None)
    val mismatched = exactReadbacks(released, base.currentSelectionByTarget(target))
      .copy(publication = Some(released.payload.receipt.beforePublication))

    expect(verifyReadbacks(descriptor, missing) == Left(EvidenceMissing(target, DecisionEvidence))) &&
    expect(verifyReadbacks(descriptor, missingQualification) == Left(EvidenceMissing(target, QualificationEvidence))) &&
    expect(verifyReadbacks(descriptor, mismatched) == Left(EvidenceMismatch(target, ActiveMptPublication)))
  }

  pureTest("an absent active MPT image cannot be captured as a Phase-2 release") {
    val withoutImage = released.copy(
      payload = released.payload.copy(
        receipt = released.payload.receipt.copy(
          activePublication = released.payload.receipt.activePublication.copy(image = None)
        )
      )
    )
    val base = state(releases = Map(target -> withoutImage))

    expect(capture(base, admissionScope, target) == Left(EvidenceMismatch(target, ReleasedCoreRecord)))
  }

  pureTest("selection lineage count, root, and interior sequence are bound to the canonical target-to-tip slice") {
    val middle = FinalityCodecFixtures.state(42L, 920, 41).copy(parentHash = target.hash)
    val tip = FinalityCodecFixtures.state(43L, 921, 920).copy(parentHash = middle.hash)
    val canonical = Vector(prior, target, middle, tip)
    val exactSelection = selection(target, Vector(target, middle, tip), branchRevision = 2L)
    val wrongCount = exactSelection.copy(
      lineage = exactSelection.lineage.copy(
        summary = exactSelection.lineage.summary.copy(entryCount = NonNegLong.unsafeFrom(2L))
      )
    )
    val wrongRoot = exactSelection.copy(lineage = exactSelection.lineage.copy(entriesRoot = hash(922)))
    val substitutedMiddle = middle.copy(hash = hash(923))
    val interiorRoot = FinalityIdentity.pathEntriesRoot(Vector(target, substitutedMiddle, tip)).fold(throw _, identity)
    val wrongInterior = exactSelection.copy(lineage = exactSelection.lineage.copy(entriesRoot = interiorRoot))

    def capturedWith(selected: CanonicalSelectionToken) =
      capture(
        state(
          branchRevision = 2L,
          canonical = canonical,
          currentSelections = Some(Map(target -> selected))
        ),
        admissionScope,
        target
      )

    expect(capturedWith(wrongCount) == Left(EvidenceMismatch(target, CanonicalLineage))) &&
    expect(capturedWith(wrongRoot) == Left(EvidenceMismatch(target, CanonicalLineage))) &&
    expect(capturedWith(wrongInterior) == Left(EvidenceMismatch(target, CanonicalLineage)))
  }

  pureTest("target-indexed selection evidence permits multiple retained exact operational ancestors") {
    val priorRelease = releasedAt(prior)
    val releases = Map(prior -> priorRelease, target -> released)
    val selections = Map(
      prior -> selection(prior, Vector(prior, target), branchRevision = 1L),
      target -> selection(target, Vector(target), branchRevision = 1L)
    )
    val base = state(releases = releases, currentSelections = Some(selections))

    val priorLease = acquire(base, HistoricalEconomicRead(hash(930)), prior)
    val targetLease = acquire(base, ShardExecutionBase(ShardId.unsafeApply(0), hash(931)), target)

    expect(priorLease.target == prior) && expect(targetLease.target == target)
  }

  pureTest("malformed or sentinel-bearing canonical paths fail before capability capture") {
    val brokenParent = target.copy(parentHash = hash(940))
    val emptyHash = prior.copy(hash = Hash.empty)

    val broken = state(
      canonical = Vector(prior, brokenParent),
      currentSelections = Some(Map.empty),
      releases = Map.empty
    )
    val sentinel = state(
      canonical = Vector(emptyHash, target),
      currentSelections = Some(Map.empty),
      releases = Map.empty
    )

    expect(capture(broken, admissionScope, brokenParent).left.exists(_.isInstanceOf[MalformedCanonicalLineage])) &&
    expect(capture(sentinel, admissionScope, target).left.exists(_.isInstanceOf[MalformedCanonicalLineage]))
  }

  pureTest("a branch mutation during unlocked verification fails the acquisition CAS") {
    val before = state()
    val descriptor = capture(before, admissionScope, target).toOption.get
    val verified = verifyReadbacks(
      descriptor,
      exactReadbacks(before.releasedByTarget(target), before.currentSelectionByTarget(target))
    ).toOption.get
    val extensionDuringVerification = before.copy(branchRevision = revision(2L))

    expect(
      acquireIfCurrent(extensionDuringVerification, verified) == Left(
        AcquisitionStale(revision(1L), revision(2L), Phase2ConsumerSink.empty.revision, Phase2ConsumerSink.empty.revision)
      )
    )
  }

  pureTest("a pure descendant extension preserves an exact-ancestor lease after complete recheck") {
    val before = state()
    val lease = acquire(before)
    val descendant = FinalityCodecFixtures.state(42L, 950, 41).copy(parentHash = target.hash)
    val extension = before.copy(
      branchRevision = revision(2L),
      canonicalOldestFirst = before.canonicalOldestFirst :+ descendant,
      currentSelectionByTarget = Map(target -> selection(target, Vector(target, descendant), branchRevision = 2L))
    )
    val result = commitIfCurrent(extension, lease, command(951))

    expect(result.isInstanceOf[Committed])
  }

  pureTest("selection evidence cannot change without advancing the branch revision") {
    val before = state()
    val lease = acquire(before)
    val current = before.currentSelectionByTarget(target)
    val changedDecision = current.copy(
      decision = ForkChoiceDecision(FinalityCodecFixtures.artifact(FinalityArtifactKind.ForkChoiceDecisionEvidence, 955))
    )
    val malformedMutation = before.copy(currentSelectionByTarget = Map(target -> changedDecision))

    expect(
      commitIfCurrent(malformedMutation, lease, command(956)) ==
        Stale(malformedMutation, EvidenceMismatch(target, CanonicalLineage))
    )
  }

  pureTest("a replacement cannot masquerade as descendant extension when lineage advancement is missing") {
    val oldTip = FinalityCodecFixtures.state(42L, 957, 41).copy(parentHash = target.hash)
    val before = state(
      canonical = Vector(prior, target, oldTip),
      currentSelections = Some(Map(target -> selection(target, Vector(target, oldTip), branchRevision = 1L)))
    )
    val lease = acquire(before)

    val replacementTip = FinalityCodecFixtures.state(42L, 958, 41).copy(parentHash = target.hash)
    val missingLineageAdvance = state(
      branchRevision = 2L,
      lineageRevision = 0L,
      canonical = Vector(prior, target, replacementTip),
      currentSelections = Some(Map(target -> selection(target, Vector(target, replacementTip), branchRevision = 2L)))
    )

    expect(
      commitIfCurrent(missingLineageAdvance, lease, command(959)) ==
        Stale(missingLineageAdvance, EvidenceMismatch(target, CanonicalLineage))
    )
  }

  pureTest("replacement above a surviving target invalidates the old lineage lease") {
    val before = state()
    val lease = acquire(before)
    val replacementAbove = before.copy(branchRevision = revision(2L), lineageRevision = lineage(1L))
    val result = commitIfCurrent(replacementAbove, lease, command(960))

    expect(result == Stale(replacementAbove, LineageReplaced(lineage(0L), lineage(1L))))
  }

  pureTest("A to B to A cannot revive a lease from A's earlier lineage generation") {
    val firstA = state()
    val oldLease = acquire(firstA)
    val returnedA = firstA.copy(branchRevision = revision(3L), lineageRevision = lineage(2L))

    expect(commitIfCurrent(returnedA, oldLease, command(970)) == Stale(returnedA, LineageReplaced(lineage(0L), lineage(2L))))
  }

  pureTest("a lease cannot be repurposed or redirected to another exact target") {
    val before = state()
    val lease = acquire(before)
    val wrongScope = FollowerAdoption(CurrencyL1)
    val wrongTarget = target.copy(hash = hash(980))

    expect(commitIfCurrent(before, lease, command(981, scope = wrongScope)) == Stale(before, WrongPurpose(admissionScope, wrongScope))) &&
    expect(commitIfCurrent(before, lease, command(982, exactTarget = wrongTarget)) == Stale(before, WrongTarget(target, wrongTarget)))
  }

  pureTest("closed commands are idempotent but command-id collisions fail closed") {
    val before = state()
    val lease = acquire(before)
    val closed = command(990)
    val first = commitIfCurrent(before, lease, closed).asInstanceOf[Committed]
    val retry = commitIfCurrent(first.state, lease, closed)
    val collision = commitIfCurrent(first.state, lease, closed.copy(payloadDigest = hash(991)))

    expect(retry == Phase2CommitResult.AlreadyCommitted(first.state, first.command)) &&
    expect(
      collision == Phase2CommitResult.RecoveryRequired(
        first.state,
        CommandIdCollision(first.command, first.command.copy(payloadDigest = hash(991)))
      )
    )
  }

  pureTest("closed command identities reject empty and noncanonical hashes") {
    val before = state()
    val lease = acquire(before)
    val emptyId = command(995).copy(commandId = Hash.empty)
    val malformedDigest = command(996).copy(payloadDigest = Hash("ABC"))

    expect(
      commitIfCurrent(before, lease, emptyId) ==
        Stale(before, InvalidCommandIdentity(Phase2CommandIdentityField.CommandId))
    ) &&
    expect(
      commitIfCurrent(before, lease, malformedDigest) ==
        Stale(before, InvalidCommandIdentity(Phase2CommandIdentityField.PayloadDigest))
    )
  }

  pureTest("scope values remain distinct across otherwise identical follower commands") {
    expect(FollowerAdoption(DagL1) != FollowerAdoption(CurrencyL1))
  }
}
