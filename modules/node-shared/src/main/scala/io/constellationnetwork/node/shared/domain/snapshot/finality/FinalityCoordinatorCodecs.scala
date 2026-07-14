package io.constellationnetwork.node.shared.domain.snapshot.finality

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityBaseCodecs._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoreCodecs._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityEffectCodecs._

import scodec.Codec
import scodec.codecs.{discriminated, provide, uint8}
import shapeless.{::, HNil}

/** Canonical greenfield ScodecV1 payload codecs for the coordinator and durable recovery records.
  * Store framing is responsible for magic, format version, and checksums.
  */
object FinalityCoordinatorCodecs {

  implicit val coordinatorMutationKindCodec: Codec[CoordinatorMutationKind] =
    discriminated[CoordinatorMutationKind]
      .by(uint8)
      .typecase(1, provide(CoordinatorMutationKind.Initialized))
      .typecase(2, provide(CoordinatorMutationKind.Prepared))
      .typecase(3, provide(CoordinatorMutationKind.CoreApplied))
      .typecase(4, provide(CoordinatorMutationKind.RestorationStarted))
      .typecase(5, provide(CoordinatorMutationKind.RestoredAbandoned))
      .typecase(6, provide(CoordinatorMutationKind.AbandonedRetired))
      .typecase(7, provide(CoordinatorMutationKind.Released))
      .typecase(8, provide(CoordinatorMutationKind.RecoveryEntered))

  implicit val auditRecordIdCodec: Codec[AuditRecordId] = requiredHashCodec.xmap(AuditRecordId(_), _.value)
  implicit val auditRecordDigestCodec: Codec[AuditRecordDigest] = requiredHashCodec.xmap(AuditRecordDigest(_), _.value)
  implicit val auditPointerCodec: Codec[AuditPointer] =
    (auditRecordIdCodec :: auditRecordDigestCodec).xmap[AuditPointer](
      { case id :: digest :: HNil => AuditPointer(id, digest) },
      value => value.id :: value.digest :: HNil
    )

  implicit val recoveryRecordIdCodec: Codec[RecoveryRecordId] = requiredHashCodec.xmap(RecoveryRecordId(_), _.value)
  implicit val recoveryRecordDigestCodec: Codec[RecoveryRecordDigest] =
    requiredHashCodec.xmap(RecoveryRecordDigest(_), _.value)
  implicit val recoveryRecordPointerCodec: Codec[RecoveryRecordPointer] =
    (recoveryRecordIdCodec :: recoveryRecordDigestCodec).xmap[RecoveryRecordPointer](
      { case id :: digest :: HNil => RecoveryRecordPointer(id, digest) },
      value => value.id :: value.digest :: HNil
    )

  private val unknownCanonicalityCodec: Codec[RecoveryReason.UnknownCanonicality] =
    (globalSnapshotStateRefCodec :: requiredHashCodec).xmap[RecoveryReason.UnknownCanonicality](
      { case target :: missingHash :: HNil => RecoveryReason.UnknownCanonicality(target, missingHash) },
      value => value.target :: value.missingHash :: HNil
    )

  private val missingCoreBatchCodec: Codec[RecoveryReason.MissingCoreBatch] =
    finalityCoreBatchPointerCodec.xmap(RecoveryReason.MissingCoreBatch(_), _.batch)

  private val corruptCoreBatchCodec: Codec[RecoveryReason.CorruptCoreBatch] =
    (finalityCoreBatchPointerCodec :: requiredHashCodec).xmap[RecoveryReason.CorruptCoreBatch](
      { case batch :: observedDigest :: HNil => RecoveryReason.CorruptCoreBatch(batch, observedDigest) },
      value => value.batch :: value.observedDigest :: HNil
    )

  private val coreConflictCodec: Codec[RecoveryReason.CoreConflict] =
    (strictOption(releasedCorePointerCodec) :: strictOption(releasedCorePointerCodec)).xmap[RecoveryReason.CoreConflict](
      { case expected :: actual :: HNil => RecoveryReason.CoreConflict(expected, actual) },
      value => value.expected :: value.actual :: HNil
    )

  private val unknownCoreCodec: Codec[RecoveryReason.UnknownCore] =
    requiredHashCodec.xmap(RecoveryReason.UnknownCore(_), _.reasonDigest)

  private val missingEffectManifestCodec: Codec[RecoveryReason.MissingEffectManifest] =
    effectManifestPointerCodec.xmap(RecoveryReason.MissingEffectManifest(_), _.manifest)

  private val corruptEffectManifestCodec: Codec[RecoveryReason.CorruptEffectManifest] =
    (effectManifestPointerCodec :: requiredHashCodec).xmap[RecoveryReason.CorruptEffectManifest](
      { case manifest :: observedDigest :: HNil => RecoveryReason.CorruptEffectManifest(manifest, observedDigest) },
      value => value.manifest :: value.observedDigest :: HNil
    )

  private val effectConflictCodec: Codec[RecoveryReason.EffectConflict] =
    (effectKindCodec :: effectStateDigestCodec :: effectStateDigestCodec).xmap[RecoveryReason.EffectConflict](
      { case kind :: expected :: actual :: HNil => RecoveryReason.EffectConflict(kind, expected, actual) },
      value => value.kind :: value.expected :: value.actual :: HNil
    )

  private val unknownEffectCodec: Codec[RecoveryReason.UnknownEffect] =
    (effectKindCodec :: requiredHashCodec).xmap[RecoveryReason.UnknownEffect](
      { case kind :: reasonDigest :: HNil => RecoveryReason.UnknownEffect(kind, reasonDigest) },
      value => value.kind :: value.reasonDigest :: HNil
    )

  private val journalCorruptionCodec: Codec[RecoveryReason.JournalCorruption] =
    requiredHashCodec.xmap(RecoveryReason.JournalCorruption(_), _.observedDigest)

  private val startupDependencyFailureCodec: Codec[RecoveryReason.StartupDependencyFailure] =
    requiredHashCodec.xmap(RecoveryReason.StartupDependencyFailure(_), _.reasonDigest)

  implicit val recoveryReasonCodec: Codec[RecoveryReason] =
    discriminated[RecoveryReason]
      .by(uint8)
      .typecase(1, unknownCanonicalityCodec)
      .typecase(2, missingCoreBatchCodec)
      .typecase(3, corruptCoreBatchCodec)
      .typecase(4, coreConflictCodec)
      .typecase(5, unknownCoreCodec)
      .typecase(6, missingEffectManifestCodec)
      .typecase(7, corruptEffectManifestCodec)
      .typecase(8, effectConflictCodec)
      .typecase(9, unknownEffectCodec)
      .typecase(10, journalCorruptionCodec)
      .typecase(11, startupDependencyFailureCodec)

  implicit val effectsIndexCodec: Codec[EffectsIndex] =
    (strictOption(effectManifestPointerCodec) :: strictOption(releaseGenerationCodec)).xmap[EffectsIndex](
      { case tail :: highGeneration :: HNil => EffectsIndex(tail, highGeneration) },
      value => value.tail :: value.highGeneration :: HNil
    )

  private val rawRecoveryRecordCodec: Codec[RecoveryRecord] =
    (headRevisionCodec :: strictOption(intentAttemptCodec) :: strictOption(releasedCoreCodec) ::
      strictOption(activeCoreIntentCodec) :: effectsIndexCodec :: recoveryReasonCodec :: strictOption(auditPointerCodec))
      .xmap[RecoveryRecord](
        { case enteredAt :: lastAttempt :: released :: active :: effects :: reason :: priorAudit :: HNil =>
          RecoveryRecord(enteredAt, lastAttempt, released, active, effects, reason, priorAudit)
        },
        value =>
          value.enteredAt :: value.lastAttempt :: value.released :: value.active :: value.effects :: value.reason ::
            value.priorAudit :: HNil
      )

  val recoveryRecordPayloadCodec: Codec[RecoveryRecord] = rawRecoveryRecordCodec.complete

  private val recoveryRequiredCodec: Codec[CoordinatorMode.RecoveryRequired] =
    recoveryRecordPointerCodec.xmap(CoordinatorMode.RecoveryRequired(_), _.record)

  implicit val coordinatorModeCodec: Codec[CoordinatorMode] =
    discriminated[CoordinatorMode]
      .by(uint8)
      .typecase(1, provide(CoordinatorMode.Running))
      .typecase(2, recoveryRequiredCodec)

  private val rawCoordinatorHeadCodec: Codec[CoordinatorHead] =
    (headRevisionCodec :: strictOption(intentAttemptCodec) :: coordinatorModeCodec :: strictOption(releasedCoreCodec) ::
      strictOption(activeCoreIntentCodec) :: effectsIndexCodec :: strictOption(auditPointerCodec)).xmap[CoordinatorHead](
      { case revision :: lastAttempt :: mode :: released :: active :: effects :: auditTail :: HNil =>
        CoordinatorHead(revision, lastAttempt, mode, released, active, effects, auditTail)
      },
      value =>
        value.revision :: value.lastAttempt :: value.mode :: value.released :: value.active :: value.effects ::
          value.auditTail :: HNil
    )

  val coordinatorHeadPayloadCodec: Codec[CoordinatorHead] = rawCoordinatorHeadCodec.complete

  implicit val coordinatorHeadCommitmentCodec: Codec[CoordinatorHeadCommitment] =
    (headRevisionCodec :: strictOption(intentAttemptCodec) :: coordinatorModeCodec :: strictOption(releasedCoreCodec) ::
      strictOption(activeCoreIntentCodec) :: effectsIndexCodec).xmap[CoordinatorHeadCommitment](
      { case revision :: lastAttempt :: mode :: released :: active :: effects :: HNil =>
        CoordinatorHeadCommitment(revision, lastAttempt, mode, released, active, effects)
      },
      value =>
        value.revision :: value.lastAttempt :: value.mode :: value.released :: value.active :: value.effects :: HNil
    )

  private val rawCoordinatorAuditRecordCodec: Codec[CoordinatorAuditRecord] =
    (coordinatorMutationKindCodec :: strictOption(coordinatorHeadCommitmentCodec) :: coordinatorHeadCommitmentCodec ::
      strictOption(auditPointerCodec)).xmap[CoordinatorAuditRecord](
      { case mutation :: before :: after :: priorAudit :: HNil =>
        CoordinatorAuditRecord(mutation, before, after, priorAudit)
      },
      value => value.mutation :: value.before :: value.after :: value.priorAudit :: HNil
    )

  val coordinatorAuditRecordPayloadCodec: Codec[CoordinatorAuditRecord] = rawCoordinatorAuditRecordCodec.complete
}
