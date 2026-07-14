package io.constellationnetwork.node.shared.domain.snapshot.finality

import scala.util.Try

import cats.data.NonEmptyList

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityBaseCodecs.{
  pathChunkPayloadCodec,
  pathManifestPayloadCodec
}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityIntentValidator._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt._

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.bits.ByteVector
import weaver.SimpleIOSuite

object FinalityIntentValidatorSuite extends SimpleIOSuite {

  private final case class PathDraft(
    commitment: PathCommitment,
    payload: PathManifestPayload,
    entries: List[GlobalSnapshotStateRef]
  )

  private final case class CoreFixture(
    batch: FinalityCoreBatch,
    manifest: FinalityEffectManifest,
    paths: List[ResolvedPathManifest],
    commands: List[ScopedEffectCommand],
    context: CoreValidationContext
  )

  private val effectKinds: List[EffectKind] = List(
    EffectKind.ChainStoreProjection,
    EffectKind.SnapshotStorageProjection,
    EffectKind.TipTrackerProjection,
    EffectKind.OverlayCacheProjection,
    EffectKind.ServiceAvailabilityWatermarkProjection,
    EffectKind.LatestSliceProjection,
    EffectKind.FollowProjectionRing,
    EffectKind.AccumulatorChangesetPromotion,
    EffectKind.SignedBytesPromotion,
    EffectKind.SidecarOutboxReconciliation,
    EffectKind.ShardAnchorAndWatermarkReconciliation,
    EffectKind.BinaryConfirmationAndRequeue,
    EffectKind.CommitteeAdmissionMaintenance,
    EffectKind.EtaCommitteeAnchorReconciliation,
    EffectKind.MempoolReconciliation,
    EffectKind.TowerIndexReconciliation,
    EffectKind.DownstreamFollowerEventEnqueue,
    EffectKind.SnapshotRetentionPruning
  )

  private def hash(label: String): Hash = Hash.fromBytes(label.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  private def nonNeg(value: Long): NonNegLong = NonNegLong.unsafeFrom(value)

  private def artifact(kind: FinalityArtifactKind, label: String): ImmutableArtifactPointer =
    FinalityIdentity
      .artifactPointerFromBytes(kind, ByteVector.view(label.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
      .fold(throw _, identity)

  private def state(ordinal: Long, label: String, parent: Hash): GlobalSnapshotStateRef =
    GlobalSnapshotStateRef(
      SnapshotOrdinal.unsafeApply(ordinal),
      hash(s"snapshot-$label"),
      parent,
      MptRoot(hash(s"mpt-$label"))
    )

  private def child(parent: GlobalSnapshotStateRef, label: String): GlobalSnapshotStateRef =
    state(parent.ordinal.value.value + 1L, label, parent.hash)

  private def path(role: PathRole, entries: List[GlobalSnapshotStateRef]): PathDraft = {
    require(entries.nonEmpty && entries.size <= PathChunk.MaxEntries)
    val root = FinalityIdentity.pathEntriesRoot(entries).fold(throw _, identity)
    val summary = PathSummary(role, entries.head, entries.last, nonNeg(entries.size.toLong))
    val payload = PathManifestPayload(summary, root)
    val manifest = FinalityIdentity
      .artifactPointer(FinalityArtifactKind.PathManifest, pathManifestPayloadCodec, payload)
      .fold(throw _, identity)
    PathDraft(PathCommitment(summary, manifest, root), payload, entries)
  }

  private def resolve(intentId: IntentId, draft: PathDraft): ResolvedPathManifest = {
    val chunk = PathChunk(
      intentId,
      draft.commitment.manifest.id,
      nonNeg(0L),
      NonEmptyList.fromListUnsafe(draft.entries),
      None
    )
    val pointer = FinalityIdentity
      .artifactPointer(FinalityArtifactKind.PathChunk, pathChunkPayloadCodec, chunk)
      .fold(throw _, identity)
    ResolvedPathManifest(intentId, draft.commitment, draft.payload, List(ResolvedPathChunk(pointer, chunk)))
  }

  private def plan(previous: Option[EffectManifestPointer], commands: List[ScopedEffectCommand]): EffectPlanCommitment =
    EffectPlanCommitment(
      previous,
      commands(0).commitment,
      commands(1).commitment,
      commands(2).commitment,
      commands(3).commitment,
      commands(4).commitment,
      commands(5).commitment,
      commands(6).commitment,
      commands(7).commitment,
      commands(8).commitment,
      commands(9).commitment,
      commands(10).commitment,
      commands(11).commitment,
      commands(12).commitment,
      commands(13).commitment,
      commands(14).commitment,
      commands(15).commitment,
      commands(16).commitment,
      commands(17).commitment
    )

  private def manifest(
    scope: EffectScope,
    previous: Option[EffectManifestPointer],
    commands: List[ScopedEffectCommand]
  ): FinalityEffectManifest =
    FinalityEffectManifest(
      scope,
      previous,
      commands(0),
      commands(1),
      commands(2),
      commands(3),
      commands(4),
      commands(5),
      commands(6),
      commands(7),
      commands(8),
      commands(9),
      commands(10),
      commands(11),
      commands(12),
      commands(13),
      commands(14),
      commands(15),
      commands(16),
      commands(17)
    )

  private def buildCore(
    selectedEntries: List[GlobalSnapshotStateRef],
    qualificationEntries: Option[List[GlobalSnapshotStateRef]] = None,
    previous: Option[(ReleasedCore, CoreFixture)] = None
  ): CoreFixture = {
    val target = selectedEntries.head
    val selectedTip = selectedEntries.last
    val lineage = path(PathRole.CanonicalLineage, selectedEntries)
    val adopted = path(PathRole.Adopted, List(target))
    val closure = qualificationEntries.map(path(PathRole.OperationalAncestorClosure, _))
    val selectionEvidence = artifact(FinalityArtifactKind.CanonicalSelectionEvidence, s"selection-${selectedTip.hash.value}")
    val qualificationEvidence = artifact(FinalityArtifactKind.DecidedAttestationEvidence, s"qualification-${target.hash.value}")
    val semanticState = artifact(FinalityArtifactKind.PreparedSemanticState, s"semantic-${target.hash.value}")
    val authenticatedAnchor = artifact(FinalityArtifactKind.AuthenticatedTargetAnchor, s"anchor-${target.hash.value}")
    val placeholder = IntentId(hash("placeholder-intent"))
    val domain = FinalityDomain(hash("network"), hash("genesis"), hash("protocol-era"))
    val generation = ReleaseGeneration(nonNeg(previous.fold(0L)(_._1.pointer.generation.value.value + 1L)))
    val attempt = IntentAttempt(nonNeg(0L))
    val transitionShape = TransitionShape.Advance(adopted.commitment)
    val transitionDigest = FinalityIdentity.transitionDigest(transitionShape).fold(throw _, identity)
    val previousManifestPointer = previous.map(_._1.payload.effectManifest)
    val provisionalEffectScope = EffectScope(domain, placeholder, generation, previous.map(_._1.pointer.target), transitionDigest, target)
    val provisionalCommands = effectKinds.zipWithIndex.map {
      case (kind, index) =>
        val predecessor = previous.map(_._2.commands(index))
        val expectedBefore = predecessor.fold(EffectStateDigest(hash(s"effect-before-$index-${target.hash.value}")))(_.desiredAfter)
        val expectedRevision = predecessor.fold(EffectSinkRevision(nonNeg(0L)))(_.desiredRevision)
        val draft = ScopedEffectCommand(
          provisionalEffectScope,
          kind,
          EffectId(hash("placeholder-effect-id")),
          expectedBefore,
          expectedRevision,
          EffectStateDigest(hash(s"effect-after-$index-${target.hash.value}")),
          EffectSinkRevision(nonNeg(expectedRevision.value.value + 1L)),
          ScopedArtifactRef(placeholder, artifact(FinalityArtifactKind.EffectPayload, s"effect-payload-$index-${target.hash.value}")),
          predecessor.map(_.effectId)
        )
        draft.copy(effectId = FinalityIdentity.effectId(draft.identityPreimage).fold(throw _, identity))
    }
    val expectedPublication = previous.fold(MptActivePublication(MptPublicationRevision(0L), None))(_._1.payload.receipt.activePublication)
    val image = MptImageReceipt(
      DurableMptImageStore.CurrentFormatVersion,
      generation.value.value,
      MptImageId(hash(s"image-id-${target.hash.value}")),
      target,
      MptImageCodecEra(hash("image-codec-era")),
      MptImageRootEra(hash("image-root-era")),
      MptImageDigest(hash(s"image-digest-${target.hash.value}")),
      1
    )
    val targetPublication = MptActivePublication(MptPublicationRevision(expectedPublication.revision.value + 1L), Some(image))
    val preparedCommitment = PreparedCoreCommitment(
      target,
      image,
      expectedPublication,
      targetPublication,
      semanticState,
      authenticatedAnchor
    )
    val selection = CanonicalSelectionToken(
      CanonicalBranchRevision(nonNeg(0L)),
      selectedTip,
      target,
      selectionEvidence,
      lineage.commitment
    )
    val qualifyingDescendant = qualificationEntries.fold(target)(_.last)
    val qualificationScope = OperationalQualificationScope(
      OperationalRail.DecidedAttestationTWeight,
      target,
      qualifyingDescendant,
      closure.map(_.commitment),
      qualificationEvidence
    )
    val scope = IntentScope(
      domain,
      generation,
      attempt,
      previous.map(_._1.pointer),
      selection,
      transitionShape,
      qualificationScope,
      preparedCommitment,
      plan(previousManifestPointer, provisionalCommands),
      target
    )
    val intentId = FinalityIdentity.intentId(scope).fold(throw _, identity)
    val effectScope = provisionalEffectScope.copy(intent = intentId)
    val commands = provisionalCommands.map(command => command.copy(scope = effectScope, payload = command.payload.copy(intentId = intentId)))
    val exactManifest = manifest(effectScope, previousManifestPointer, commands)
    val manifestPointer = FinalityIdentity.effectManifestPointer(exactManifest).fold(throw _, identity)
    val adoptedRef = PathManifestRef(adopted.commitment, ScopedArtifactRef(intentId, adopted.commitment.manifest))
    val closureRef = closure.map(value => PathManifestRef(value.commitment, ScopedArtifactRef(intentId, value.commitment.manifest)))
    val qualification = OperationalQualification.DecidedAttestationTWeight(
      target,
      qualifyingDescendant,
      closureRef,
      ScopedArtifactRef(intentId, qualificationEvidence)
    )
    val prepared = PreparedCoreTarget(
      target,
      image,
      expectedPublication,
      targetPublication,
      ScopedArtifactRef(intentId, semanticState),
      ScopedArtifactRef(intentId, authenticatedAnchor)
    )
    val batch = FinalityCoreBatch(
      intentId,
      scope,
      ScopedArtifactRef(intentId, selectionEvidence),
      CoreTransition.Advance(adoptedRef),
      qualification,
      prepared,
      manifestPointer
    )
    val paths = List(resolve(intentId, lineage), resolve(intentId, adopted)) ++ closure.toList.map(resolve(intentId, _))
    val context = CoreValidationContext(
      previous.map(_._1),
      previous.map { case (priorReleased, priorFixture) => PreviousEffectManifest(priorReleased.payload.effectManifest, priorFixture.manifest) }
    )
    CoreFixture(batch, exactManifest, paths, commands, context)
  }

  private def violations[A](result: ValidationResult[A]): List[Violation] =
    result.fold(_.toChain.toList, _ => Nil)

  private def released(fixture: CoreFixture): ReleasedCore = {
    val batch = fixture.batch
    val receipt = ReleasedCoreReceipt(
      batch.intentId,
      batch.scope.generation,
      batch.scope.target,
      batch.prepared.expectedBefore,
      batch.prepared.targetPublication,
      ScopedArtifactRef(batch.intentId, artifact(FinalityArtifactKind.AppliedSemanticStateReceipt, "released-semantic")),
      ScopedArtifactRef(batch.intentId, artifact(FinalityArtifactKind.AuthenticatedAnchorReceipt, "released-anchor"))
    )
    val payload = ReleasedCoreRecordPayload(batch.qualification, receipt, batch.effectManifest)
    val record = FinalityIdentity
      .artifactPointer(FinalityArtifactKind.ReleasedCoreRecord, FinalityCoreCodecs.releasedCoreRecordPayloadCodec, payload)
      .fold(throw _, identity)
    ReleasedCore(
      ReleasedCorePointer(batch.scope.generation, batch.intentId, batch.scope.target, record),
      ScopedArtifactRef(batch.intentId, record),
      payload
    )
  }

  pureTest("core validation requires the exact complete resolved path multiset") {
    val target = state(10L, "path-target", hash("path-parent"))
    val fixture = buildCore(List(target))
    val valid = validateResolvedCorePaths(fixture.batch, fixture.paths)
    val omitted = validateResolvedCorePaths(fixture.batch, fixture.paths.dropRight(1))
    val duplicated = validateResolvedCorePaths(fixture.batch, fixture.paths :+ fixture.paths.head)
    val substituteDraft = path(PathRole.Orphaned, List(target))
    val substituted = validateResolvedCorePaths(fixture.batch, fixture.paths.updated(1, resolve(fixture.batch.intentId, substituteDraft)))

    expect.all(
      valid.isValid,
      violations(omitted).exists(_.path == "batch.resolvedPaths"),
      violations(duplicated).exists(_.path == "batch.resolvedPaths"),
      violations(substituted).exists(_.path == "batch.resolvedPaths")
    )
  }

  pureTest("every resolved path entry must contain a canonical nonzero state reference") {
    val oldest = state(15L, "entry-oldest", hash("entry-parent"))
    val middle = child(oldest, "entry-middle")
    val newest = child(middle, "entry-newest")
    val intentId = IntentId(hash("entry-validation-intent"))
    val valid = resolve(intentId, path(PathRole.CanonicalLineage, List(oldest, middle, newest)))
    val zeroMiddleRoot = middle.copy(mptRoot = MptRoot(Hash.empty))
    val invalidChunk = valid.chunks.head.value.copy(entriesOldestFirst = NonEmptyList.of(oldest, zeroMiddleRoot, newest))
    val invalid = valid.copy(chunks = List(valid.chunks.head.copy(value = invalidChunk)))

    expect.all(
      validateResolvedPath(valid).isValid,
      violations(validateResolvedPath(invalid)).exists(_.path == "path.entries[1].mptRoot")
    )
  }

  pureTest("qualification closure must be an exact prefix of canonical lineage") {
    val target = state(20L, "prefix-target", hash("prefix-parent"))
    val selected = child(target, "selected-child")
    val fork = child(target, "qualified-fork")
    val validFixture = buildCore(List(target, selected), Some(List(target, selected)))
    val forkFixture = buildCore(List(target, selected), Some(List(target, fork)))

    expect.all(
      validateCoreBatch(validFixture.batch, validFixture.manifest, validFixture.paths, validFixture.context).isValid,
      violations(validateCoreBatch(forkFixture.batch, forkFixture.manifest, forkFixture.paths, forkFixture.context))
        .exists(_.path == "batch.qualification.ancestorClosure")
    )
  }

  pureTest("inherited rollback closure must traverse the exact orphaned suffix") {
    val mrca = state(30L, "rollback-mrca", hash("rollback-parent"))
    val old1 = child(mrca, "old-1")
    val old2 = child(old1, "old-2")
    val old3 = child(old2, "old-3")
    val replacement = child(mrca, "replacement")
    val bad1 = child(mrca, "bad-1")
    val bad2 = child(bad1, "bad-2")
    val bad3 = child(bad2, "bad-3")
    val fixture = buildCore(List(mrca))
    val lineage = path(PathRole.CanonicalLineage, List(mrca, replacement))
    val orphaned = path(PathRole.Orphaned, List(old1, old2))
    val closure = path(PathRole.OperationalAncestorClosure, List(mrca, old1, old2, old3))
    val badClosure = path(PathRole.OperationalAncestorClosure, List(mrca, bad1, bad2, bad3))

    def rollbackBatch(closureDraft: PathDraft): FinalityCoreBatch = {
      val intentId = fixture.batch.intentId
      val evidence = fixture.batch.qualification.evidence
      val selection = fixture.batch.scope.selection.copy(selectedTip = replacement, operationalTarget = mrca, lineage = lineage.commitment)
      val orphanedRef = PathManifestRef(orphaned.commitment, ScopedArtifactRef(intentId, orphaned.commitment.manifest))
      val closureRef = PathManifestRef(closureDraft.commitment, ScopedArtifactRef(intentId, closureDraft.commitment.manifest))
      val transition = CoreTransition.DensityRollbackToOperationalMrca(mrca, orphanedRef, fixture.batch.selectionEvidence)
      val qualification = OperationalQualification.DecidedAttestationTWeight(mrca, closureDraft.entries.last, Some(closureRef), evidence)
      fixture.batch.copy(
        scope = fixture.batch.scope.copy(
          selection = selection,
          transition = transition.shape,
          qualification = qualification.scope,
          target = mrca
        ),
        transition = transition,
        qualification = qualification
      )
    }

    def resolved(batch: FinalityCoreBatch, closureDraft: PathDraft): List[ResolvedPathManifest] =
      List(lineage, orphaned, closureDraft).map(resolve(batch.intentId, _))

    val validBatch = rollbackBatch(closure)
    val invalidBatch = rollbackBatch(badClosure)

    expect.all(
      validateResolvedCorePaths(validBatch, resolved(validBatch, closure)).isValid,
      violations(validateResolvedCorePaths(invalidBatch, resolved(invalidBatch, badClosure)))
        .exists(_.path == "batch.qualification.ancestorClosure")
    )
  }

  pureTest("density replacement requires the exact immediate divergence and a different target") {
    val mrca = state(35L, "replacement-mrca", hash("replacement-parent"))
    val old1 = child(mrca, "replacement-old-1")
    val old2 = child(old1, "replacement-old-2")
    val new1 = child(mrca, "replacement-new-1")
    val new2 = child(new1, "replacement-new-2")
    val lateFork = child(old1, "replacement-late-fork")
    val fixture = buildCore(List(new2))
    val orphaned = path(PathRole.Orphaned, List(old1, old2))
    val adopted = path(PathRole.Adopted, List(new1, new2))
    val noOpAdopted = path(PathRole.Adopted, List(old1, old2))
    val lateDivergenceAdopted = path(PathRole.Adopted, List(old1, lateFork))
    val densityEvidence = artifact(FinalityArtifactKind.DensityDecisionEvidence, "replacement-density-evidence")

    def replacementBatch(adoptedDraft: PathDraft): (FinalityCoreBatch, PathDraft) = {
      val intentId = fixture.batch.intentId
      val target = adoptedDraft.entries.last
      val lineage = path(PathRole.CanonicalLineage, List(target))
      val transition = CoreTransition.DensityReplacement(
        mrca,
        PathManifestRef(orphaned.commitment, ScopedArtifactRef(intentId, orphaned.commitment.manifest)),
        PathManifestRef(adoptedDraft.commitment, ScopedArtifactRef(intentId, adoptedDraft.commitment.manifest)),
        ScopedArtifactRef(intentId, densityEvidence)
      )
      val selection = fixture.batch.scope.selection.copy(
        selectedTip = target,
        operationalTarget = target,
        lineage = lineage.commitment
      )
      fixture.batch.copy(
        scope = fixture.batch.scope.copy(selection = selection, transition = transition.shape, target = target),
        transition = transition
      ) -> lineage
    }

    def resolved(batch: FinalityCoreBatch, lineage: PathDraft, adoptedDraft: PathDraft): List[ResolvedPathManifest] =
      List(lineage, orphaned, adoptedDraft).map(resolve(batch.intentId, _))

    val (validBatch, validLineage) = replacementBatch(adopted)
    val (noOpBatch, noOpLineage) = replacementBatch(noOpAdopted)
    val (lateDivergenceBatch, lateDivergenceLineage) = replacementBatch(lateDivergenceAdopted)

    expect.all(
      validateResolvedCorePaths(validBatch, resolved(validBatch, validLineage, adopted)).isValid,
      violations(validateResolvedCorePaths(noOpBatch, resolved(noOpBatch, noOpLineage, noOpAdopted)))
        .exists(_.path == "batch.transition.commonAncestor"),
      violations(
        validateResolvedCorePaths(
          lateDivergenceBatch,
          resolved(lateDivergenceBatch, lateDivergenceLineage, lateDivergenceAdopted)
        )
      ).exists(_.path == "batch.transition.commonAncestor")
    )
  }

  pureTest("density rollback may stop at the MRCA but cannot follow the orphaned first child") {
    val mrca = state(38L, "rollback-selection-mrca", hash("rollback-selection-parent"))
    val old1 = child(mrca, "rollback-selection-old-1")
    val old2 = child(old1, "rollback-selection-old-2")
    val replacement = child(mrca, "rollback-selection-replacement")
    val fixture = buildCore(List(mrca))
    val orphaned = path(PathRole.Orphaned, List(old1, old2))
    val atMrca = path(PathRole.CanonicalLineage, List(mrca))
    val replacementLineage = path(PathRole.CanonicalLineage, List(mrca, replacement))
    val oldLineage = path(PathRole.CanonicalLineage, List(mrca, old1, old2))
    val densityEvidence = artifact(FinalityArtifactKind.DensityDecisionEvidence, "rollback-selection-density-evidence")

    def rollbackBatch(lineage: PathDraft): FinalityCoreBatch = {
      val intentId = fixture.batch.intentId
      val transition = CoreTransition.DensityRollbackToOperationalMrca(
        mrca,
        PathManifestRef(orphaned.commitment, ScopedArtifactRef(intentId, orphaned.commitment.manifest)),
        ScopedArtifactRef(intentId, densityEvidence)
      )
      val selection = fixture.batch.scope.selection.copy(
        selectedTip = lineage.entries.last,
        operationalTarget = mrca,
        lineage = lineage.commitment
      )
      fixture.batch.copy(
        scope = fixture.batch.scope.copy(selection = selection, transition = transition.shape, target = mrca),
        transition = transition
      )
    }

    def resolved(batch: FinalityCoreBatch, lineage: PathDraft): List[ResolvedPathManifest] =
      List(lineage, orphaned).map(resolve(batch.intentId, _))

    val atMrcaBatch = rollbackBatch(atMrca)
    val replacementBatch = rollbackBatch(replacementLineage)
    val oldBranchBatch = rollbackBatch(oldLineage)

    expect.all(
      validateResolvedCorePaths(atMrcaBatch, resolved(atMrcaBatch, atMrca)).isValid,
      validateResolvedCorePaths(replacementBatch, resolved(replacementBatch, replacementLineage)).isValid,
      violations(validateResolvedCorePaths(oldBranchBatch, resolved(oldBranchBatch, oldLineage)))
        .exists(_.path == "batch.scope.selection.lineage")
    )
  }

  pureTest("scope, exact manifest pointer, and transition digest are independently enforced") {
    val target = state(40L, "scope-target", hash("scope-parent"))
    val fixture = buildCore(List(target))
    val wrongScope = fixture.manifest.scope.copy(target = child(target, "wrong-scope-target"))
    val wrongScopeManifest = fixture.manifest.copy(
      chainStoreProjection = fixture.manifest.chainStoreProjection.copy(scope = wrongScope)
    )
    val wrongDigestScope = fixture.manifest.scope.copy(transitionDigest = hash("wrong-transition-digest"))
    val wrongDigestCommands = fixture.commands.map(_.copy(scope = wrongDigestScope))
    val wrongDigestManifest = manifest(wrongDigestScope, None, wrongDigestCommands)
    val substitutedPointer = fixture.batch.copy(effectManifest = EffectManifestPointer(
      fixture.batch.scope.generation,
      EffectManifestId(hash("substituted-manifest-id")),
      EffectManifestDigest(hash("substituted-manifest-digest"))
    ))

    val valid = validateCoreBatch(fixture.batch, fixture.manifest, fixture.paths, fixture.context)
    val scopeViolations = violations(validateCoreBatch(fixture.batch, wrongScopeManifest, fixture.paths, fixture.context))
    val digestViolations = violations(validateCoreBatch(fixture.batch, wrongDigestManifest, fixture.paths, fixture.context))
    val pointerViolations = violations(validateCoreBatch(substitutedPointer, fixture.manifest, fixture.paths, fixture.context))

    expect.all(
      valid.isValid,
      scopeViolations.exists(_.path == "effectManifest.chainStoreProjection.scope"),
      digestViolations.exists(_.path == "effectManifest.scope.transitionDigest"),
      pointerViolations.exists(_.path == "batch.effectManifest")
    )
  }

  pureTest("effect history authenticates the prior manifest and forbids cross-generation lane replay") {
    val priorTarget = state(45L, "prior-effect-target", hash("prior-effect-parent"))
    val priorFixture = buildCore(List(priorTarget))
    val priorReleased = released(priorFixture)
    val currentTarget = child(priorTarget, "current-effect-target")
    val current = buildCore(List(currentTarget), previous = Some(priorReleased -> priorFixture))

    val malformedPriorScope = priorFixture.manifest.scope.copy(intent = IntentId(hash("wrong-prior-intent")))
    val malformedPriorCommands = priorFixture.commands.map(command =>
      command.copy(scope = malformedPriorScope, payload = command.payload.copy(intentId = malformedPriorScope.intent))
    )
    val malformedPrior = manifest(malformedPriorScope, None, malformedPriorCommands)
    val malformedPriorContext = current.context.copy(
      previousEffects = Some(PreviousEffectManifest(priorReleased.payload.effectManifest, malformedPrior))
    )
    val malformedPriorKind = priorFixture.manifest.copy(
      chainStoreProjection = priorFixture.manifest.chainStoreProjection.copy(kind = EffectKind.TipTrackerProjection)
    )
    val malformedKindContext = current.context.copy(
      previousEffects = Some(PreviousEffectManifest(priorReleased.payload.effectManifest, malformedPriorKind))
    )
    val reusedIdManifest = current.manifest.copy(
      chainStoreProjection = current.manifest.chainStoreProjection.copy(effectId = priorFixture.commands.head.effectId)
    )
    val mutatedCommandManifest = current.manifest.copy(
      chainStoreProjection = current.manifest.chainStoreProjection.copy(
        desiredAfter = EffectStateDigest(hash("mutated-command-state"))
      )
    )
    val wrongDomain = FinalityDomain(hash("other-network"), hash("other-genesis"), hash("other-era"))
    val wrongDomainScope = current.manifest.scope.copy(domain = wrongDomain)
    val wrongDomainCommands = current.commands.map { command =>
      val draft = command.copy(scope = wrongDomainScope)
      draft.copy(effectId = FinalityIdentity.effectId(draft.identityPreimage).fold(throw _, identity))
    }
    val wrongDomainManifest = manifest(wrongDomainScope, current.manifest.previous, wrongDomainCommands)
    val wrongExpectedRevision = current.manifest.copy(
      chainStoreProjection = current.manifest.chainStoreProjection.copy(expectedRevision = EffectSinkRevision(nonNeg(0L)))
    )
    val wrongDesiredRevision = current.manifest.copy(
      chainStoreProjection = current.manifest.chainStoreProjection.copy(
        desiredRevision = current.manifest.chainStoreProjection.expectedRevision
      )
    )

    expect.all(
      validateCoreBatch(current.batch, current.manifest, current.paths, current.context).isValid,
      violations(validateCoreBatch(current.batch, current.manifest, current.paths, malformedPriorContext))
        .exists(_.path == "effectManifest.previous.scope.intent"),
      violations(validateCoreBatch(current.batch, current.manifest, current.paths, malformedKindContext))
        .exists(_.path == "effectManifest.previous.chainStoreProjection.kind"),
      violations(validateCoreBatch(current.batch, reusedIdManifest, current.paths, current.context))
        .exists(_.path == "effectManifest.chainStoreProjection.effectId"),
      violations(validateCoreBatch(current.batch, mutatedCommandManifest, current.paths, current.context))
        .exists(_.path == "effectManifest.chainStoreProjection.effectId"),
      violations(validateCoreBatch(current.batch, wrongDomainManifest, current.paths, current.context))
        .exists(_.path == "effectManifest.scope.domain"),
      violations(validateCoreBatch(current.batch, wrongExpectedRevision, current.paths, current.context))
        .exists(_.path == "effectManifest.chainStoreProjection.expectedRevision"),
      violations(validateCoreBatch(current.batch, wrongDesiredRevision, current.paths, current.context))
        .exists(_.path == "effectManifest.chainStoreProjection.desiredRevision")
    )
  }

  pureTest("effect receipts require a derived command id and distinguish mutation from read-back no-op") {
    val fixture = buildCore(List(state(49L, "receipt-target", hash("receipt-parent"))))
    val original = fixture.commands.head
    val noOpDraft = original.copy(
      effectId = EffectId(hash("placeholder-noop-id")),
      desiredAfter = original.expectedBefore,
      desiredRevision = original.expectedRevision
    )
    val noOp = noOpDraft.copy(effectId = FinalityIdentity.effectId(noOpDraft.identityPreimage).fold(throw _, identity))
    val appliedNoOp = AppliedEffectReceipt(
      fixture.batch.effectManifest,
      noOp.effectId,
      noOp.expectedBefore,
      noOp.expectedRevision,
      noOp.desiredAfter,
      noOp.desiredRevision
    )
    val readBackNoOp = AlreadyAppliedEffectReceipt(
      fixture.batch.effectManifest,
      noOp.effectId,
      noOp.desiredAfter,
      noOp.desiredRevision
    )
    val forged = original.copy(effectId = EffectId(hash("caller-selected-effect-id")))
    val forgedReceipt = AlreadyAppliedEffectReceipt(
      fixture.batch.effectManifest,
      forged.effectId,
      forged.desiredAfter,
      forged.desiredRevision
    )

    expect.all(
      violations(validateEffectReceipt(noOp, fixture.batch.effectManifest, appliedNoOp))
        .exists(_.path == "effectReceipt"),
      validateEffectReceipt(noOp, fixture.batch.effectManifest, readBackNoOp).isValid,
      violations(validateEffectReceipt(forged, fixture.batch.effectManifest, forgedReceipt))
        .exists(_.path == "effectReceipt.command.effectId")
    )
  }

  pureTest("generation Long.MaxValue fails validation without overflowing or throwing") {
    val target = state(50L, "overflow-target", hash("overflow-parent"))
    val fixture = buildCore(List(target))
    val prior = released(fixture)
    val maxGeneration = ReleaseGeneration(nonNeg(Long.MaxValue))
    val impossiblePrior = prior.copy(pointer = prior.pointer.copy(generation = maxGeneration))
    val attempted = Try(
      validateCoreBatch(
        fixture.batch,
        fixture.manifest,
        fixture.paths,
        CoreValidationContext(Some(impossiblePrior), Some(PreviousEffectManifest(fixture.batch.effectManifest, fixture.manifest)))
      )
    )

    expect.all(
      attempted.isSuccess,
      attempted.toOption.exists(result => violations(result).exists(_.path == "batch.scope.generation"))
    )
  }

  pureTest("RecoveryRequired is absorbing and recovery entry is bound to its exact record") {
    val priorAudit = AuditPointer(AuditRecordId(hash("prior-audit-id")), AuditRecordDigest(hash("prior-audit-digest")))
    val before = CoordinatorHead(
      HeadRevision(nonNeg(0L)),
      None,
      CoordinatorMode.Running,
      None,
      None,
      EffectsIndex(None, None),
      Some(priorAudit)
    )
    val record = RecoveryRecord(
      HeadRevision(nonNeg(1L)),
      None,
      None,
      None,
      EffectsIndex(None, None),
      RecoveryReason.UnknownCore(hash("recovery-reason")),
      Some(priorAudit)
    )
    val recordPointer = FinalityIdentity.recoveryPointer(record).fold(throw _, identity)
    val afterWithoutAudit = before.copy(
      revision = HeadRevision(nonNeg(1L)),
      mode = CoordinatorMode.RecoveryRequired(recordPointer),
      auditTail = None
    )
    val entryAudit = CoordinatorAuditRecord(
      CoordinatorMutationKind.RecoveryEntered,
      Some(before.commitment),
      afterWithoutAudit.commitment,
      before.auditTail
    )
    val entered = afterWithoutAudit.copy(auditTail = Some(FinalityIdentity.auditPointer(entryAudit).fold(throw _, identity)))
    val entry = validateCoordinatorTransition(Some(before), entered, entryAudit, Some(record))

    val illegalWithoutAudit = entered.copy(revision = HeadRevision(nonNeg(2L)), auditTail = None)
    val illegalAudit = CoordinatorAuditRecord(
      CoordinatorMutationKind.RecoveryEntered,
      Some(entered.commitment),
      illegalWithoutAudit.commitment,
      entered.auditTail
    )
    val illegal = illegalWithoutAudit.copy(auditTail = Some(FinalityIdentity.auditPointer(illegalAudit).fold(throw _, identity)))
    val exit = validateCoordinatorTransition(Some(entered), illegal, illegalAudit, None)

    expect.all(entry.isValid, violations(exit).exists(_.path == "head.mode"))
  }

  pureTest("startup dependency recovery requires a canonical nonzero diagnostic digest") {
    val priorAudit = AuditPointer(
      AuditRecordId(hash("startup-prior-audit-id")),
      AuditRecordDigest(hash("startup-prior-audit-digest"))
    )
    val before = CoordinatorHead(
      HeadRevision(nonNeg(0L)),
      None,
      CoordinatorMode.Running,
      None,
      None,
      EffectsIndex(None, None),
      Some(priorAudit)
    )

    def transition(reason: RecoveryReason): ValidationResult[CoordinatorHead] = {
      val record = RecoveryRecord(
        HeadRevision(nonNeg(1L)),
        None,
        None,
        None,
        EffectsIndex(None, None),
        reason,
        Some(priorAudit)
      )
      val recordPointer = FinalityIdentity.recoveryPointer(record).getOrElse(
        RecoveryRecordPointer(
          RecoveryRecordId(hash("invalid-startup-record-id")),
          RecoveryRecordDigest(hash("invalid-startup-record-digest"))
        )
      )
      val afterWithoutAudit = before.copy(
        revision = HeadRevision(nonNeg(1L)),
        mode = CoordinatorMode.RecoveryRequired(recordPointer),
        auditTail = None
      )
      val audit = CoordinatorAuditRecord(
        CoordinatorMutationKind.RecoveryEntered,
        Some(before.commitment),
        afterWithoutAudit.commitment,
        before.auditTail
      )
      val after = afterWithoutAudit.copy(auditTail = Some(FinalityIdentity.auditPointer(audit).fold(throw _, identity)))

      validateCoordinatorTransition(Some(before), after, audit, Some(record))
    }

    val valid = transition(RecoveryReason.StartupDependencyFailure(hash("startup-diagnostic")))
    val zero = transition(RecoveryReason.StartupDependencyFailure(Hash.empty))

    expect.all(
      valid.isValid,
      violations(zero).exists(_.path == "recoveryRecord.reason.reasonDigest")
    )
  }

  pureTest("release binds the exact record and outbox advancement requires all 18 receipts") {
    val target = state(60L, "release-target", hash("release-parent"))
    val fixture = buildCore(List(target))
    val exactReleased = released(fixture)
    val active = ActiveCoreIntent(
      FinalityCoreBatchPointer(
        fixture.batch.intentId,
        fixture.batch.scope.attempt,
        fixture.batch.scope.generation,
        artifact(FinalityArtifactKind.CoreBatch, "core-batch")
      ),
      fixture.batch.scope,
      CoreStage.CoreApplied(exactReleased.payload.receipt)
    )
    val priorAudit = AuditPointer(AuditRecordId(hash("release-prior-audit-id")), AuditRecordDigest(hash("release-prior-audit-digest")))
    val before = CoordinatorHead(
      HeadRevision(nonNeg(0L)),
      Some(fixture.batch.scope.attempt),
      CoordinatorMode.Running,
      None,
      Some(active),
      EffectsIndex(None, None),
      Some(priorAudit)
    )
    val afterWithoutAudit = CoordinatorHead(
      HeadRevision(nonNeg(1L)),
      Some(fixture.batch.scope.attempt),
      CoordinatorMode.Running,
      Some(exactReleased),
      None,
      EffectsIndex(Some(fixture.batch.effectManifest), Some(fixture.batch.scope.generation)),
      None
    )
    val audit = CoordinatorAuditRecord(
      CoordinatorMutationKind.Released,
      Some(before.commitment),
      afterWithoutAudit.commitment,
      before.auditTail
    )
    val after = afterWithoutAudit.copy(auditTail = Some(FinalityIdentity.auditPointer(audit).fold(throw _, identity)))
    val releaseTransition = validateCoordinatorTransition(Some(before), after, audit)
    val releaseRecord = validateReleasedCore(fixture.batch, exactReleased)
    val substitutedRecord = validateReleasedCore(
      fixture.batch,
      exactReleased.copy(pointer = exactReleased.pointer.copy(record = artifact(FinalityArtifactKind.ReleasedCoreRecord, "wrong-record")))
    )

    val receipts = fixture.commands.map(command =>
      AppliedEffectReceipt(
        fixture.batch.effectManifest,
        command.effectId,
        command.expectedBefore,
        command.expectedRevision,
        command.desiredAfter,
        command.desiredRevision
      )
    )
    val completion = CompletedEffectManifest(fixture.batch.effectManifest, fixture.manifest, receipts)
    val outboxBefore = FinalityEffectOutboxHead(EffectOutboxRevision(nonNeg(0L)), EffectOutboxCursor(None, None))
    val outboxAfter = FinalityEffectOutboxHead(
      EffectOutboxRevision(nonNeg(1L)),
      EffectOutboxCursor(Some(fixture.batch.scope.generation), Some(fixture.batch.effectManifest))
    )
    val complete = validateOutboxTransition(outboxBefore, outboxAfter, after, List(completion))
    val incomplete = validateOutboxTransition(outboxBefore, outboxAfter, after, List(completion.copy(receipts = receipts.dropRight(1))))
    val wrongRevisionReceipt = receipts.head.copy(sinkRevision = receipts.head.observedBeforeRevision)
    val wrongRevision = validateOutboxTransition(
      outboxBefore,
      outboxAfter,
      after,
      List(completion.copy(receipts = wrongRevisionReceipt :: receipts.tail))
    )
    val noOp = validateOutboxTransition(
      outboxBefore,
      outboxBefore.copy(revision = EffectOutboxRevision(nonNeg(1L))),
      after,
      Nil
    )

    expect.all(
      releaseTransition.isValid,
      releaseRecord.isValid,
      violations(substitutedRecord).exists(_.path == "released.pointer.record"),
      complete.isValid,
      violations(incomplete).exists(_.path.startsWith("outbox.completions[0].receipts")),
      violations(wrongRevision).exists(_.path == "effectReceipt.sinkRevision"),
      violations(noOp).exists(_.path == "outbox.cursor")
    )
  }
}
