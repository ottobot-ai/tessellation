package io.constellationnetwork.node.shared.domain.snapshot.finality

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityBaseCodecs._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityEffectCodecs.{
  effectManifestPointerCodec,
  effectPlanCommitmentCodec
}

import scodec.Codec
import scodec.codecs.{discriminated, provide, uint8}
import shapeless.{::, HNil}

/** Canonical greenfield ScodecV1 payload codecs for immutable finality intents and core receipts. */
object FinalityCoreCodecs {

  private val rawIntentScopeCodec: Codec[IntentScope] =
    (finalityDomainCodec :: releaseGenerationCodec :: intentAttemptCodec :: strictOption(releasedCorePointerCodec) ::
      canonicalSelectionTokenCodec :: transitionShapeCodec :: operationalQualificationScopeCodec :: preparedCoreCommitmentCodec ::
      effectPlanCommitmentCodec :: globalSnapshotStateRefCodec).xmap[IntentScope](
      {
        case domain :: generation :: attempt :: expectedPrior :: selection :: transition :: qualification :: prepared :: effects ::
            target :: HNil =>
          IntentScope(
            domain,
            generation,
            attempt,
            expectedPrior,
            selection,
            transition,
            qualification,
            prepared,
            effects,
            target
          )
      },
      value =>
        value.domain :: value.generation :: value.attempt :: value.expectedPrior :: value.selection :: value.transition ::
          value.qualification :: value.prepared :: value.effects :: value.target :: HNil
    )

  val intentScopePayloadCodec: Codec[IntentScope] = rawIntentScopeCodec.complete

  implicit val finalityCoreBatchPointerCodec: Codec[FinalityCoreBatchPointer] =
    (intentIdCodec :: intentAttemptCodec :: releaseGenerationCodec :: immutableArtifactPointerCodec)
      .xmap[FinalityCoreBatchPointer](
        { case intentId :: attempt :: generation :: artifact :: HNil =>
          FinalityCoreBatchPointer(intentId, attempt, generation, artifact)
        },
        value => value.intentId :: value.attempt :: value.generation :: value.artifact :: HNil
      )

  private val rawFinalityCoreBatchCodec: Codec[FinalityCoreBatch] =
    (intentIdCodec :: rawIntentScopeCodec :: scopedArtifactRefCodec :: coreTransitionCodec :: operationalQualificationCodec ::
      preparedCoreTargetCodec :: effectManifestPointerCodec).xmap[FinalityCoreBatch](
      {
        case intentId :: scope :: selectionEvidence :: transition :: qualification :: prepared :: effectManifest :: HNil =>
          FinalityCoreBatch(intentId, scope, selectionEvidence, transition, qualification, prepared, effectManifest)
      },
      value =>
        value.intentId :: value.scope :: value.selectionEvidence :: value.transition :: value.qualification :: value.prepared ::
          value.effectManifest :: HNil
    )

  val finalityCoreBatchPayloadCodec: Codec[FinalityCoreBatch] = rawFinalityCoreBatchCodec.complete

  implicit val releasedCoreReceiptCodec: Codec[ReleasedCoreReceipt] =
    (intentIdCodec :: releaseGenerationCodec :: globalSnapshotStateRefCodec :: mptActivePublicationCodec ::
      mptActivePublicationCodec :: scopedArtifactRefCodec :: scopedArtifactRefCodec).xmap[ReleasedCoreReceipt](
      {
        case intentId :: generation :: target :: beforePublication :: activePublication :: semanticReceipt ::
            anchorReceipt :: HNil =>
          ReleasedCoreReceipt(
            intentId,
            generation,
            target,
            beforePublication,
            activePublication,
            semanticReceipt,
            anchorReceipt
          )
      },
      value =>
        value.intentId :: value.generation :: value.target :: value.beforePublication :: value.activePublication ::
          value.semanticReceipt :: value.authenticatedAnchorReceipt :: HNil
    )

  private val rawReleasedCoreRecordPayloadCodec: Codec[ReleasedCoreRecordPayload] =
    (operationalQualificationCodec :: releasedCoreReceiptCodec :: effectManifestPointerCodec)
      .xmap[ReleasedCoreRecordPayload](
        { case qualification :: receipt :: effectManifest :: HNil =>
          ReleasedCoreRecordPayload(qualification, receipt, effectManifest)
        },
        value => value.qualification :: value.receipt :: value.effectManifest :: HNil
      )

  val releasedCoreRecordPayloadCodec: Codec[ReleasedCoreRecordPayload] = rawReleasedCoreRecordPayloadCodec.complete

  implicit val releasedCoreCodec: Codec[ReleasedCore] =
    (releasedCorePointerCodec :: scopedArtifactRefCodec :: rawReleasedCoreRecordPayloadCodec).xmap[ReleasedCore](
      { case pointer :: record :: payload :: HNil => ReleasedCore(pointer, record, payload) },
      value => value.pointer :: value.record :: value.payload :: HNil
    )

  implicit val objectiveOrphanCauseCodec: Codec[ObjectiveOrphanCause] =
    (intentIdCodec :: canonicalSelectionTokenCodec :: canonicalSelectionTokenCodec :: globalSnapshotStateRefCodec ::
      scopedArtifactRefCodec).xmap[ObjectiveOrphanCause](
      { case intentId :: superseded :: replacement :: ancestor :: evidence :: HNil =>
        ObjectiveOrphanCause(intentId, superseded, replacement, ancestor, evidence)
      },
      value =>
        value.intentId :: value.supersededSelection :: value.replacementSelection :: value.commonAncestor :: value.evidence :: HNil
    )

  implicit val priorCoreRestorationReceiptCodec: Codec[PriorCoreRestorationReceipt] =
    (intentIdCodec :: mptActivePublicationCodec :: mptActivePublicationCodec :: scopedArtifactRefCodec :: scopedArtifactRefCodec)
      .xmap[PriorCoreRestorationReceipt](
        { case intentId :: before :: restored :: semantic :: anchor :: HNil =>
          PriorCoreRestorationReceipt(intentId, before, restored, semantic, anchor)
        },
        value =>
          value.intentId :: value.beforeRestoration :: value.restoredPublication :: value.semanticReceipt ::
            value.authenticatedAnchorReceipt :: HNil
      )

  private val coreAppliedCodec: Codec[CoreStage.CoreApplied] =
    releasedCoreReceiptCodec.xmap(CoreStage.CoreApplied(_), _.receipt)

  private val restoringPriorCodec: Codec[CoreStage.RestoringPrior] =
    objectiveOrphanCauseCodec.xmap(CoreStage.RestoringPrior(_), _.cause)

  private val restoredAbandonedCodec: Codec[CoreStage.RestoredAbandoned] =
    (objectiveOrphanCauseCodec :: priorCoreRestorationReceiptCodec).xmap[CoreStage.RestoredAbandoned](
      { case cause :: receipt :: HNil => CoreStage.RestoredAbandoned(cause, receipt) },
      value => value.cause :: value.receipt :: HNil
    )

  implicit val coreStageCodec: Codec[CoreStage] =
    discriminated[CoreStage]
      .by(uint8)
      .typecase(1, provide(CoreStage.Prepared))
      .typecase(2, coreAppliedCodec)
      .typecase(3, restoringPriorCodec)
      .typecase(4, restoredAbandonedCodec)

  implicit val activeCoreIntentCodec: Codec[ActiveCoreIntent] =
    (finalityCoreBatchPointerCodec :: rawIntentScopeCodec :: coreStageCodec).xmap[ActiveCoreIntent](
      { case batch :: scope :: stage :: HNil => ActiveCoreIntent(batch, scope, stage) },
      value => value.batch :: value.scope :: value.stage :: HNil
    )
}
