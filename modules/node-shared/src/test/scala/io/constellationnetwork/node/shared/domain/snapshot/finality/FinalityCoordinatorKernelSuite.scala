package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.data.NonEmptyList

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityBaseCodecs.{pathChunkPayloadCodec, pathManifestPayloadCodec}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoordinatorKernelError._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityIntentValidator._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt._

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.bits.ByteVector
import shapeless.test.illTyped
import weaver.FunSuite

object FinalityCoordinatorKernelSuite extends FunSuite {

  private final case class PathFixture(
    commitment: PathCommitment,
    entries: NonEmptyList[GlobalSnapshotStateRef]
  )

  private final case class Fixture(
    batch: FinalityCoreBatch,
    manifest: FinalityEffectManifest,
    paths: List[ResolvedPathManifest],
    context: CoreValidationContext,
    appliedReceipt: ReleasedCoreReceipt,
    released: ReleasedCore
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

  private def hash(seed: Int): Hash = Hash(f"$seed%064x")
  private def nonNeg(value: Long): NonNegLong = NonNegLong.unsafeFrom(value)

  private def state(ordinal: Long, seed: Int, parentSeed: Int): GlobalSnapshotStateRef =
    GlobalSnapshotStateRef(
      SnapshotOrdinal.unsafeApply(ordinal),
      hash(seed),
      hash(parentSeed),
      MptRoot(hash(seed + 10000))
    )

  private def artifact(kind: FinalityArtifactKind, seed: Int): ImmutableArtifactPointer =
    FinalityIdentity
      .artifactPointerFromBytes(kind, ByteVector.fromValidHex(f"$seed%08x"))
      .fold(throw _, identity)

  private def path(
    role: PathRole,
    entries: NonEmptyList[GlobalSnapshotStateRef]
  ): PathFixture = {
    val root = FinalityIdentity.pathEntriesRoot(entries.toList).fold(throw _, identity)
    val summary = PathSummary(role, entries.head, entries.last, nonNeg(entries.size.toLong))
    val payload = PathManifestPayload(summary, root)
    val manifest = FinalityIdentity
      .artifactPointer(FinalityArtifactKind.PathManifest, pathManifestPayloadCodec, payload)
      .fold(throw _, identity)

    PathFixture(PathCommitment(summary, manifest, root), entries)
  }

  private def resolvePath(intentId: IntentId, fixture: PathFixture): ResolvedPathManifest = {
    val chunk = PathChunk(
      intentId,
      fixture.commitment.manifest.id,
      nonNeg(0L),
      fixture.entries,
      None
    )
    val chunkArtifact = FinalityIdentity
      .artifactPointer(FinalityArtifactKind.PathChunk, pathChunkPayloadCodec, chunk)
      .fold(throw _, identity)

    ResolvedPathManifest(
      intentId,
      fixture.commitment,
      fixture.commitment.payload,
      List(ResolvedPathChunk(chunkArtifact, chunk))
    )
  }

  private def effectPlan(
    commands: List[EffectCommandCommitment]
  ): EffectPlanCommitment =
    EffectPlanCommitment(
      previous = None,
      chainStoreProjection = commands(0),
      snapshotStorageProjection = commands(1),
      tipTrackerProjection = commands(2),
      overlayCacheProjection = commands(3),
      serviceAvailabilityWatermarkProjection = commands(4),
      latestSliceProjection = commands(5),
      followProjectionRing = commands(6),
      accumulatorChangesetPromotion = commands(7),
      signedBytesPromotion = commands(8),
      sidecarOutboxReconciliation = commands(9),
      shardAnchorAndWatermarkReconciliation = commands(10),
      binaryConfirmationAndRequeue = commands(11),
      committeeAdmissionMaintenance = commands(12),
      etaCommitteeAnchorReconciliation = commands(13),
      mempoolReconciliation = commands(14),
      towerIndexReconciliation = commands(15),
      downstreamFollowerEventEnqueue = commands(16),
      snapshotRetentionPruning = commands(17)
    )

  private def effectManifest(
    scope: EffectScope,
    commands: List[ScopedEffectCommand]
  ): FinalityEffectManifest =
    FinalityEffectManifest(
      scope = scope,
      previous = None,
      chainStoreProjection = commands(0),
      snapshotStorageProjection = commands(1),
      tipTrackerProjection = commands(2),
      overlayCacheProjection = commands(3),
      serviceAvailabilityWatermarkProjection = commands(4),
      latestSliceProjection = commands(5),
      followProjectionRing = commands(6),
      accumulatorChangesetPromotion = commands(7),
      signedBytesPromotion = commands(8),
      sidecarOutboxReconciliation = commands(9),
      shardAnchorAndWatermarkReconciliation = commands(10),
      binaryConfirmationAndRequeue = commands(11),
      committeeAdmissionMaintenance = commands(12),
      etaCommitteeAnchorReconciliation = commands(13),
      mempoolReconciliation = commands(14),
      towerIndexReconciliation = commands(15),
      downstreamFollowerEventEnqueue = commands(16),
      snapshotRetentionPruning = commands(17)
    )

  private def fixture: Fixture = {
    val target = state(1L, 20, 19)
    val generation = ReleaseGeneration(nonNeg(0L))
    val attempt = IntentAttempt(nonNeg(0L))
    val domain = FinalityDomain(hash(300), hash(301), hash(302))
    val lineage = path(PathRole.CanonicalLineage, NonEmptyList.one(target))
    val adopted = path(PathRole.Adopted, NonEmptyList.one(target))
    val selectionEvidence = artifact(FinalityArtifactKind.ForkChoiceDecisionEvidence, 100)
    val qualificationEvidence = artifact(FinalityArtifactKind.DecidedAttestationEvidence, 101)
    val semanticState = artifact(FinalityArtifactKind.PreparedSemanticState, 102)
    val authenticatedAnchor = artifact(FinalityArtifactKind.AuthenticatedTargetAnchor, 103)
    val image = MptImageReceipt(
      formatVersion = DurableMptImageStore.CurrentFormatVersion,
      generation = 0L,
      imageId = MptImageId(hash(110)),
      anchor = target,
      codecEra = MptImageCodecEra(hash(111)),
      rootEra = MptImageRootEra(hash(112)),
      digest = MptImageDigest(hash(113)),
      entryCount = 1
    )
    val expectedPublication = MptActivePublication(MptPublicationRevision(0L), None)
    val targetPublication = MptActivePublication(MptPublicationRevision(1L), Some(image))
    val preparedCommitment = PreparedCoreCommitment(
      target,
      image,
      expectedPublication,
      targetPublication,
      semanticState,
      authenticatedAnchor
    )
    val transitionShape = TransitionShape.Advance(adopted.commitment)
    val transitionDigest = FinalityIdentity.transitionDigest(transitionShape).fold(throw _, identity)
    val commandCommitments = effectKinds.zipWithIndex.map {
      case (kind, index) =>
        val seed = 200 + index * 10
        val expectedBefore = EffectStateDigest(hash(seed + 2))
        val expectedRevision = EffectSinkRevision(nonNeg(0L))
        val desiredAfter = EffectStateDigest(hash(seed + 3))
        val desiredRevision = EffectSinkRevision(nonNeg(1L))
        val payload = artifact(FinalityArtifactKind.EffectPayload, seed + 4)
        val identityPreimage = EffectCommandIdentity(
          domain,
          generation,
          None,
          transitionDigest,
          target,
          kind,
          expectedBefore,
          expectedRevision,
          desiredAfter,
          desiredRevision,
          payload,
          None
        )
        EffectCommandCommitment(
          kind = kind,
          effectId = FinalityIdentity.effectId(identityPreimage).fold(throw _, identity),
          expectedBefore = expectedBefore,
          expectedRevision = expectedRevision,
          desiredAfter = desiredAfter,
          desiredRevision = desiredRevision,
          payload = payload,
          predecessor = None
        )
    }
    val plan = effectPlan(commandCommitments)
    val scope = IntentScope(
      domain = domain,
      generation = generation,
      attempt = attempt,
      expectedPrior = None,
      selection = CanonicalSelectionToken(
        CanonicalBranchRevision(nonNeg(1L)),
        target,
        target,
        ForkChoiceDecision(selectionEvidence),
        lineage.commitment
      ),
      transition = transitionShape,
      qualification = OperationalQualificationScope(
        OperationalRail.DecidedAttestationTWeight,
        target,
        target,
        None,
        qualificationEvidence
      ),
      prepared = preparedCommitment,
      effects = plan,
      target = target
    )
    val intentId = FinalityIdentity.intentId(scope).fold(throw _, identity)
    val effectScope = EffectScope(domain, intentId, generation, None, transitionDigest, target)
    val commands = commandCommitments.map { command =>
      ScopedEffectCommand(
        scope = effectScope,
        kind = command.kind,
        effectId = command.effectId,
        expectedBefore = command.expectedBefore,
        expectedRevision = command.expectedRevision,
        desiredAfter = command.desiredAfter,
        desiredRevision = command.desiredRevision,
        payload = ScopedArtifactRef(intentId, command.payload),
        predecessor = command.predecessor
      )
    }
    val manifest = effectManifest(effectScope, commands)
    val manifestPointer = FinalityIdentity.effectManifestPointer(manifest).fold(throw _, identity)
    val adoptedRef = PathManifestRef(
      adopted.commitment,
      ScopedArtifactRef(intentId, adopted.commitment.manifest)
    )
    val qualification = OperationalQualification.DecidedAttestationTWeight(
      target,
      target,
      None,
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
    val appliedReceipt = ReleasedCoreReceipt(
      intentId,
      generation,
      target,
      expectedPublication,
      targetPublication,
      ScopedArtifactRef(intentId, artifact(FinalityArtifactKind.AppliedSemanticStateReceipt, 310)),
      ScopedArtifactRef(intentId, artifact(FinalityArtifactKind.AuthenticatedAnchorReceipt, 311))
    )
    val releasedPayload = ReleasedCoreRecordPayload(qualification, appliedReceipt, manifestPointer)
    val releasedArtifact = FinalityIdentity
      .artifactPointer(
        FinalityArtifactKind.ReleasedCoreRecord,
        FinalityCoreCodecs.releasedCoreRecordPayloadCodec,
        releasedPayload
      )
      .fold(throw _, identity)
    val releasedPointer = ReleasedCorePointer(generation, intentId, target, releasedArtifact)
    val released = ReleasedCore(
      releasedPointer,
      ScopedArtifactRef(intentId, releasedArtifact),
      releasedPayload
    )
    Fixture(
      batch,
      manifest,
      List(resolvePath(intentId, lineage), resolvePath(intentId, adopted)),
      CoreValidationContext(None, None, expectedPublication),
      appliedReceipt,
      released
    )
  }

  private def success(result: FinalityCoordinatorKernel.Result): FinalityCoordinatorMutation =
    result.fold(error => throw new IllegalStateException(error.toString), identity)

  private def auditIsDerived(mutation: FinalityCoordinatorMutation): Boolean =
    FinalityIdentity.auditPointer(mutation.audit).toOption.exists(pointer => mutation.head.auditTail.contains(pointer))

  private def initialized: FinalityCoordinatorMutation =
    success(FinalityCoordinatorKernel.initialize(fixture.batch.prepared.expectedBefore))

  private def prepared(initial: CoordinatorHead, value: Fixture): FinalityCoordinatorMutation =
    success(
      FinalityCoordinatorKernel.prepare(
        initial,
        value.batch,
        value.manifest,
        value.paths,
        value.context
      )
    )

  test("initialize and prepare derive every pointer and return complete durability prerequisites") {
    val value = fixture
    val init = initialized
    val prepare = prepared(init.head, value)
    val bundle = prepare.preparedBundle.get

    expect.all(
      init.audit.mutation == CoordinatorMutationKind.Initialized,
      init.head.revision.value.value == 0L,
      init.head.publication == value.batch.prepared.expectedBefore,
      auditIsDerived(init),
      prepare.audit.mutation == CoordinatorMutationKind.Prepared,
      prepare.head.revision.value.value == 1L,
      auditIsDerived(prepare),
      bundle.batch == value.batch,
      bundle.effectManifest == value.manifest,
      bundle.resolvedPaths == value.paths,
      prepare.head.active.exists(_.batch == bundle.pointer),
      FinalityIdentity
        .artifactPointer(
          FinalityArtifactKind.CoreBatch,
          FinalityCoreCodecs.finalityCoreBatchPayloadCodec,
          bundle.batch
        )
        .toOption
        .contains(bundle.pointer.artifact)
    )
  }

  test("mutation construction and unverified state-changing entry points are unavailable to callers") {
    illTyped("""FinalityCoordinatorMutation(null, null, None, None)""")
    illTyped("""FinalityCoordinatorKernel.initialize(null).toOption.get.copy(head = null)""")
    illTyped("""FinalityCoordinatorKernel.coreApplied(null, null)""")
    illTyped("""FinalityCoordinatorKernel.release(null, null, null)""")
    illTyped("""FinalityCoordinatorKernel.restorationStarted(null, null)""")
    illTyped("""FinalityCoordinatorKernel.restoredAbandoned(null, null)""")
    illTyped("""FinalityCoordinatorKernel.retireAbandoned(null)""")

    expect(true)
  }

  test("recovery entry derives its immutable record, freezes core state, and is absorbing") {
    val value = fixture
    val prepare = prepared(initialized.head, value)
    val reason = RecoveryReason.UnknownCore(hash(400))
    val recovery = success(FinalityCoordinatorKernel.enterRecovery(prepare.head, reason))
    val record = recovery.recoveryRecord.get
    val derived = FinalityIdentity.recoveryPointer(record).toOption
    val repeated = FinalityCoordinatorKernel.enterRecovery(recovery.head, RecoveryReason.UnknownCore(hash(401)))
    val attemptedPrepare = FinalityCoordinatorKernel.prepare(
      recovery.head,
      value.batch,
      value.manifest,
      value.paths,
      value.context
    )

    expect.all(
      recovery.audit.mutation == CoordinatorMutationKind.RecoveryEntered,
      recovery.head.mode == derived.map(CoordinatorMode.RecoveryRequired).get,
      record.enteredAt == recovery.head.revision,
      record.lastAttempt == prepare.head.lastAttempt,
      record.released == prepare.head.released,
      record.active == prepare.head.active,
      record.publication == prepare.head.publication,
      record.effects == prepare.head.effects,
      record.priorAudit == prepare.head.auditTail,
      auditIsDerived(recovery),
      repeated.swap.toOption.exists(_.isInstanceOf[ValidationFailed]),
      attemptedPrepare.swap.toOption.exists(_.isInstanceOf[ValidationFailed])
    )
  }

  test("prepare rejects a batch whose canonical intent identity does not match its scope") {
    val value = fixture
    val invalid = value.batch.copy(intentId = IntentId(hash(500)))
    val result = FinalityCoordinatorKernel.prepare(
      initialized.head,
      invalid,
      value.manifest,
      value.paths,
      value.context
    )

    expect(result.swap.toOption.exists(_.isInstanceOf[ValidationFailed]))
  }

  test("prepare binds the complete prior release and previous-effect origin to the current head") {
    val value = fixture
    val releasedHead = initialized.head.copy(
      released = Some(value.released),
      publication = value.released.payload.receipt.activePublication,
      effects = EffectsIndex(Some(value.batch.effectManifest), Some(value.batch.scope.generation))
    )
    val substitutedPrior = value.released.copy(
      payload = value.released.payload.copy(
        receipt = value.appliedReceipt.copy(
          semanticReceipt = ScopedArtifactRef(
            value.batch.intentId,
            artifact(FinalityArtifactKind.AppliedSemanticStateReceipt, 505)
          )
        )
      )
    )
    val substitutedContext = CoreValidationContext(
      Some(substitutedPrior),
      Some(PreviousEffectManifest(value.batch.effectManifest, value.manifest)),
      releasedHead.publication
    )
    val wrongManifestPointer = value.batch.effectManifest.copy(id = EffectManifestId(hash(506)))
    val wrongEffectContext = CoreValidationContext(
      Some(value.released),
      Some(PreviousEffectManifest(wrongManifestPointer, value.manifest)),
      releasedHead.publication
    )
    val wrongPublicationContext = CoreValidationContext(
      Some(value.released),
      Some(PreviousEffectManifest(value.batch.effectManifest, value.manifest)),
      value.batch.prepared.expectedBefore
    )
    val substitutedResult = FinalityCoordinatorKernel.prepare(
      releasedHead,
      value.batch,
      value.manifest,
      value.paths,
      substitutedContext
    )
    val wrongEffectResult = FinalityCoordinatorKernel.prepare(
      releasedHead,
      value.batch,
      value.manifest,
      value.paths,
      wrongEffectContext
    )
    val wrongPublicationResult = FinalityCoordinatorKernel.prepare(
      releasedHead,
      value.batch,
      value.manifest,
      value.paths,
      wrongPublicationContext
    )

    expect.all(
      substitutedResult.swap.toOption.exists(_.isInstanceOf[CoreContextReleasedMismatch]),
      wrongEffectResult.swap.toOption.exists(_.isInstanceOf[CoreContextEffectsMismatch]),
      wrongPublicationResult.swap.toOption.exists(_.isInstanceOf[CoreContextPublicationMismatch])
    )
  }

  test("exhausted revisions fail without constructing a head") {
    val init = initialized
    val exhausted = init.head.copy(revision = HeadRevision(nonNeg(Long.MaxValue)))
    val recoveryAtExhaustion = FinalityCoordinatorKernel.enterRecovery(
      exhausted,
      RecoveryReason.JournalCorruption(hash(520))
    )

    expect(recoveryAtExhaustion.swap.toOption.contains(RevisionExhausted(exhausted.revision)))
  }
}
