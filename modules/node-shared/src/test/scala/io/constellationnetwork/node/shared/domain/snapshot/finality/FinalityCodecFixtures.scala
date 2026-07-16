package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.data.NonEmptyList

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt._

import eu.timepit.refined.types.numeric.NonNegLong

private[finality] object FinalityCodecFixtures {

  def hash(seed: Int): Hash = Hash(f"$seed%064x")
  def nonNeg(value: Long): NonNegLong = NonNegLong.unsafeFrom(value)

  def state(ordinal: Long, seed: Int, parentSeed: Int): GlobalSnapshotStateRef =
    GlobalSnapshotStateRef(
      SnapshotOrdinal.unsafeApply(ordinal),
      hash(seed),
      hash(parentSeed),
      MptRoot(hash(seed + 10000))
    )

  def artifact(kind: FinalityArtifactKind, seed: Int): ImmutableArtifactPointer =
    ImmutableArtifactPointer(
      kind,
      ArtifactEncoding(hash(seed)),
      ArtifactId(hash(seed + 1)),
      ArtifactDigest(hash(seed + 2)),
      nonNeg(seed.toLong + 3L)
    )

  val generation: ReleaseGeneration = ReleaseGeneration(nonNeg(5L))
  val attempt: IntentAttempt = IntentAttempt(nonNeg(9L))
  val intentId: IntentId = IntentId(hash(1))
  val finalityDomain: FinalityDomain = FinalityDomain(hash(400), hash(401), hash(402))

  val priorState: GlobalSnapshotStateRef = state(40L, 40, 39)
  val targetState: GlobalSnapshotStateRef = state(41L, 41, 40)

  val pathManifestArtifact: ImmutableArtifactPointer = artifact(FinalityArtifactKind.PathManifest, 100)
  val pathSummary: PathSummary =
    PathSummary(PathRole.Adopted, priorState, targetState, nonNeg(2L))
  val pathCommitment: PathCommitment =
    PathCommitment(pathSummary, pathManifestArtifact, hash(104))
  val pathManifestPayload: PathManifestPayload = pathCommitment.payload
  val pathManifestRef: PathManifestRef =
    PathManifestRef(pathCommitment, ScopedArtifactRef(intentId, pathManifestArtifact))
  val pathChunk: PathChunk =
    PathChunk(
      intentId,
      pathManifestArtifact.id,
      nonNeg(0L),
      NonEmptyList.of(priorState, targetState),
      None
    )

  val selectionEvidenceArtifact: ImmutableArtifactPointer =
    artifact(FinalityArtifactKind.ForkChoiceDecisionEvidence, 110)
  val selection: CanonicalSelectionToken =
    CanonicalSelectionToken(
      CanonicalBranchRevision(nonNeg(12L)),
      targetState,
      targetState,
      ForkChoiceDecision(selectionEvidenceArtifact),
      pathCommitment
    )

  val qualificationEvidenceArtifact: ImmutableArtifactPointer =
    artifact(FinalityArtifactKind.DecidedAttestationEvidence, 120)
  val qualificationScope: OperationalQualificationScope =
    OperationalQualificationScope(
      OperationalRail.DecidedAttestationTWeight,
      targetState,
      targetState,
      None,
      qualificationEvidenceArtifact
    )
  val qualification: OperationalQualification =
    OperationalQualification.DecidedAttestationTWeight(
      targetState,
      targetState,
      None,
      ScopedArtifactRef(intentId, qualificationEvidenceArtifact)
    )

  val imageReceipt: MptImageReceipt =
    MptImageReceipt(
      formatVersion = 2,
      generation = 27L,
      imageId = MptImageId(hash(130)),
      anchor = targetState,
      codecEra = MptImageCodecEra(hash(131)),
      rootEra = MptImageRootEra(hash(132)),
      digest = MptImageDigest(hash(133)),
      entryCount = 7
    )
  val expectedPublication: MptActivePublication =
    MptActivePublication(MptPublicationRevision(30L), None)
  val targetPublication: MptActivePublication =
    MptActivePublication(MptPublicationRevision(31L), Some(imageReceipt))

  val semanticStateArtifact: ImmutableArtifactPointer =
    artifact(FinalityArtifactKind.PreparedSemanticState, 140)
  val authenticatedAnchorArtifact: ImmutableArtifactPointer =
    artifact(FinalityArtifactKind.AuthenticatedTargetAnchor, 150)
  val preparedTarget: PreparedCoreTarget =
    PreparedCoreTarget(
      targetState,
      imageReceipt,
      expectedPublication,
      targetPublication,
      ScopedArtifactRef(intentId, semanticStateArtifact),
      ScopedArtifactRef(intentId, authenticatedAnchorArtifact)
    )

  val effectKinds: List[EffectKind] = List(
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

  val effectScope: EffectScope =
    EffectScope(finalityDomain, intentId, generation, Some(priorState), hash(160), targetState)

  private def effectCommand(kind: EffectKind, index: Int): ScopedEffectCommand = {
    val seed = 200 + index * 10
    val command = ScopedEffectCommand(
      effectScope,
      kind,
      EffectId(hash(seed)),
      EffectStateDigest(hash(seed + 2)),
      EffectSinkRevision(nonNeg(7L)),
      EffectStateDigest(hash(seed + 3)),
      EffectSinkRevision(nonNeg(8L)),
      ScopedArtifactRef(intentId, artifact(FinalityArtifactKind.EffectPayload, seed + 4)),
      Option.when(index > 0)(EffectId(hash(seed - 10)))
    )

    command.copy(effectId = FinalityIdentity.effectId(command.identityPreimage).fold(throw _, identity))
  }

  val effectCommands: List[ScopedEffectCommand] =
    effectKinds.zipWithIndex.map { case (kind, index) => effectCommand(kind, index) }

  val previousEffectManifest: EffectManifestPointer =
    EffectManifestPointer(
      ReleaseGeneration(nonNeg(4L)),
      EffectManifestId(hash(390)),
      EffectManifestDigest(hash(391))
    )
  val effectManifestPointer: EffectManifestPointer =
    EffectManifestPointer(generation, EffectManifestId(hash(392)), EffectManifestDigest(hash(393)))

  val effectPlan: EffectPlanCommitment =
    EffectPlanCommitment(
      previous = Some(previousEffectManifest),
      chainStoreProjection = effectCommands(0).commitment,
      snapshotStorageProjection = effectCommands(1).commitment,
      tipTrackerProjection = effectCommands(2).commitment,
      overlayCacheProjection = effectCommands(3).commitment,
      serviceAvailabilityWatermarkProjection = effectCommands(4).commitment,
      latestSliceProjection = effectCommands(5).commitment,
      followProjectionRing = effectCommands(6).commitment,
      accumulatorChangesetPromotion = effectCommands(7).commitment,
      signedBytesPromotion = effectCommands(8).commitment,
      sidecarOutboxReconciliation = effectCommands(9).commitment,
      shardAnchorAndWatermarkReconciliation = effectCommands(10).commitment,
      binaryConfirmationAndRequeue = effectCommands(11).commitment,
      committeeAdmissionMaintenance = effectCommands(12).commitment,
      etaCommitteeAnchorReconciliation = effectCommands(13).commitment,
      mempoolReconciliation = effectCommands(14).commitment,
      towerIndexReconciliation = effectCommands(15).commitment,
      downstreamFollowerEventEnqueue = effectCommands(16).commitment,
      snapshotRetentionPruning = effectCommands(17).commitment
    )

  val effectManifest: FinalityEffectManifest =
    FinalityEffectManifest(
      scope = effectScope,
      previous = Some(previousEffectManifest),
      chainStoreProjection = effectCommands(0),
      snapshotStorageProjection = effectCommands(1),
      tipTrackerProjection = effectCommands(2),
      overlayCacheProjection = effectCommands(3),
      serviceAvailabilityWatermarkProjection = effectCommands(4),
      latestSliceProjection = effectCommands(5),
      followProjectionRing = effectCommands(6),
      accumulatorChangesetPromotion = effectCommands(7),
      signedBytesPromotion = effectCommands(8),
      sidecarOutboxReconciliation = effectCommands(9),
      shardAnchorAndWatermarkReconciliation = effectCommands(10),
      binaryConfirmationAndRequeue = effectCommands(11),
      committeeAdmissionMaintenance = effectCommands(12),
      etaCommitteeAnchorReconciliation = effectCommands(13),
      mempoolReconciliation = effectCommands(14),
      towerIndexReconciliation = effectCommands(15),
      downstreamFollowerEventEnqueue = effectCommands(16),
      snapshotRetentionPruning = effectCommands(17)
    )

  val intentScope: IntentScope =
    IntentScope(
      finalityDomain,
      generation,
      attempt,
      None,
      selection,
      TransitionShape.Advance(pathCommitment),
      qualificationScope,
      preparedTarget.commitment,
      effectPlan,
      targetState
    )

  val coreBatch: FinalityCoreBatch =
    FinalityCoreBatch(
      intentId,
      intentScope,
      ScopedArtifactRef(intentId, selectionEvidenceArtifact),
      CoreTransition.Advance(pathManifestRef),
      qualification,
      preparedTarget,
      effectManifestPointer
    )

  val releasedReceipt: ReleasedCoreReceipt =
    ReleasedCoreReceipt(
      intentId,
      generation,
      targetState,
      expectedPublication,
      targetPublication,
      ScopedArtifactRef(intentId, artifact(FinalityArtifactKind.AppliedSemanticStateReceipt, 410)),
      ScopedArtifactRef(intentId, artifact(FinalityArtifactKind.AuthenticatedAnchorReceipt, 420))
    )
  val releasedRecordPayload: ReleasedCoreRecordPayload =
    ReleasedCoreRecordPayload(qualification, releasedReceipt, effectManifestPointer)
  val releasedRecordArtifact: ImmutableArtifactPointer =
    artifact(FinalityArtifactKind.ReleasedCoreRecord, 430)
  val releasedPointer: ReleasedCorePointer =
    ReleasedCorePointer(generation, intentId, targetState, releasedRecordArtifact)
  val releasedCore: ReleasedCore =
    ReleasedCore(releasedPointer, ScopedArtifactRef(intentId, releasedRecordArtifact), releasedRecordPayload)

  val coreBatchPointer: FinalityCoreBatchPointer =
    FinalityCoreBatchPointer(intentId, attempt, generation, artifact(FinalityArtifactKind.CoreBatch, 440))
  val activeCore: ActiveCoreIntent =
    ActiveCoreIntent(coreBatchPointer, intentScope, CoreStage.CoreApplied(releasedReceipt))
  val effectsIndex: EffectsIndex = EffectsIndex(Some(effectManifestPointer), Some(generation))
  val auditPointer: AuditPointer = AuditPointer(AuditRecordId(hash(450)), AuditRecordDigest(hash(451)))
  val recoveryPointer: RecoveryRecordPointer =
    RecoveryRecordPointer(RecoveryRecordId(hash(452)), RecoveryRecordDigest(hash(453)))

  val recoveryRecord: RecoveryRecord =
    RecoveryRecord(
      HeadRevision(nonNeg(33L)),
      Some(attempt),
      Some(releasedCore),
      Some(activeCore),
      targetPublication,
      effectsIndex,
      RecoveryReason.EffectConflict(
        EffectKind.TowerIndexReconciliation,
        EffectStateDigest(hash(454)),
        EffectStateDigest(hash(455))
      ),
      Some(auditPointer)
    )

  val coordinatorHead: CoordinatorHead =
    CoordinatorHead(
      HeadRevision(nonNeg(34L)),
      CanonicalLineageRevision(nonNeg(7L)),
      Some(attempt),
      CoordinatorMode.RecoveryRequired(recoveryPointer),
      Some(releasedCore),
      Some(activeCore),
      targetPublication,
      effectsIndex,
      Some(auditPointer)
    )

  val auditRecord: CoordinatorAuditRecord =
    CoordinatorAuditRecord(
      CoordinatorMutationKind.RecoveryEntered,
      Some(coordinatorHead.commitment.copy(revision = HeadRevision(nonNeg(33L)))),
      coordinatorHead.commitment,
      Some(auditPointer)
    )

  val terminalEffectReceipt: TerminalEffectReceipt =
    AppliedEffectReceipt(
      effectManifestPointer,
      effectCommands.head.effectId,
      EffectStateDigest(hash(459)),
      EffectSinkRevision(nonNeg(2L)),
      EffectStateDigest(hash(460)),
      EffectSinkRevision(nonNeg(3L))
    )

  val effectOutboxHead: FinalityEffectOutboxHead =
    FinalityEffectOutboxHead(
      EffectOutboxRevision(nonNeg(4L)),
      EffectOutboxCursor(Some(generation), Some(effectManifestPointer))
    )
}
