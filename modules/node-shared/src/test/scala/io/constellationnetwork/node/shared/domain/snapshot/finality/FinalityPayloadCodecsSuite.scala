package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.data.NonEmptyList

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityBaseCodecs._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCodecFixtures._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoordinatorCodecs._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoreCodecs._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityEffectCodecs._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.{MptActivePublication, MptImageId, MptPublicationRevision}
import io.constellationnetwork.serde.codecs.Primitives
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => rawHashCodec}

import scodec.Codec
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import weaver.FunSuite

object FinalityPayloadCodecsSuite extends FunSuite {

  private val trailingByte: BitVector = ByteVector(0x7f).bits
  private val discardedDraftPrefix: BitVector = ByteVector.fromValidHex("46494e3101").bits

  private def encoded[A](codec: Codec[A], value: A): BitVector =
    codec.encode(value).require

  private def rejects[A](codec: Codec[A], bits: BitVector): Boolean =
    codec.decodeValue(bits).toEither.isLeft

  private def roundTripsAndRejectsTrailing[A](codec: Codec[A], value: A): Boolean = {
    val bits = encoded(codec, value)
    codec.decodeValue(bits).toEither.toOption.contains(value) && rejects(codec, bits ++ trailingByte)
  }

  private def rejectsTags[A](codec: Codec[A]): Boolean =
    rejects(codec.complete, encoded(uint8, 0)) && rejects(codec.complete, encoded(uint8, 255))

  private def rejectsDiscardedDraftEnvelope[A](codec: Codec[A], value: A): Boolean =
    rejects(codec, discardedDraftPrefix ++ encoded(codec, value))

  private def manifestCommands(manifest: FinalityEffectManifest): List[ScopedEffectCommand] =
    List(
      manifest.chainStoreProjection,
      manifest.snapshotStorageProjection,
      manifest.tipTrackerProjection,
      manifest.overlayCacheProjection,
      manifest.serviceAvailabilityWatermarkProjection,
      manifest.latestSliceProjection,
      manifest.followProjectionRing,
      manifest.accumulatorChangesetPromotion,
      manifest.signedBytesPromotion,
      manifest.sidecarOutboxReconciliation,
      manifest.shardAnchorAndWatermarkReconciliation,
      manifest.binaryConfirmationAndRequeue,
      manifest.committeeAdmissionMaintenance,
      manifest.etaCommitteeAnchorReconciliation,
      manifest.mempoolReconciliation,
      manifest.towerIndexReconciliation,
      manifest.downstreamFollowerEventEnqueue,
      manifest.snapshotRetentionPruning
    )

  private def planCommands(plan: EffectPlanCommitment): List[EffectCommandCommitment] =
    List(
      plan.chainStoreProjection,
      plan.snapshotStorageProjection,
      plan.tipTrackerProjection,
      plan.overlayCacheProjection,
      plan.serviceAvailabilityWatermarkProjection,
      plan.latestSliceProjection,
      plan.followProjectionRing,
      plan.accumulatorChangesetPromotion,
      plan.signedBytesPromotion,
      plan.sidecarOutboxReconciliation,
      plan.shardAnchorAndWatermarkReconciliation,
      plan.binaryConfirmationAndRequeue,
      plan.committeeAdmissionMaintenance,
      plan.etaCommitteeAnchorReconciliation,
      plan.mempoolReconciliation,
      plan.towerIndexReconciliation,
      plan.downstreamFollowerEventEnqueue,
      plan.snapshotRetentionPruning
    )

  test("every exposed greenfield ScodecV1 payload round-trips and rejects trailing bytes") {
    expect.all(
      roundTripsAndRejectsTrailing(pathManifestPayloadCodec, pathManifestPayload),
      roundTripsAndRejectsTrailing(pathChunkPayloadCodec, pathChunk),
      roundTripsAndRejectsTrailing(intentScopePayloadCodec, intentScope),
      roundTripsAndRejectsTrailing(finalityCoreBatchPayloadCodec, coreBatch),
      roundTripsAndRejectsTrailing(releasedCoreRecordPayloadCodec, releasedRecordPayload),
      roundTripsAndRejectsTrailing(effectCommandIdentityCodec, effectCommands.head.identityPreimage),
      roundTripsAndRejectsTrailing(finalityEffectManifestPayloadCodec, effectManifest),
      roundTripsAndRejectsTrailing(terminalEffectReceiptPayloadCodec, terminalEffectReceipt),
      roundTripsAndRejectsTrailing(finalityEffectOutboxHeadPayloadCodec, effectOutboxHead),
      roundTripsAndRejectsTrailing(recoveryRecordPayloadCodec, recoveryRecord),
      roundTripsAndRejectsTrailing(coordinatorHeadPayloadCodec, coordinatorHead),
      roundTripsAndRejectsTrailing(coordinatorAuditRecordPayloadCodec, auditRecord)
    )
  }

  test("strict options reject every tag except zero and one") {
    val optionCodec = strictOption(intentAttemptCodec).complete

    expect.all(
      optionCodec.decodeValue(encoded(uint8, 0)).toEither.toOption.contains(None),
      rejects(optionCodec, encoded(uint8, 2)),
      rejects(optionCodec, encoded(uint8, 255))
    )
  }

  test("closed unions reject zero and unknown tags, including removed variants") {
    expect.all(
      rejectsTags(finalityArtifactKindCodec),
      rejectsTags(pathRoleCodec),
      rejectsTags(operationalRailCodec),
      rejectsTags(operationalQualificationCodec),
      rejectsTags(transitionShapeCodec),
      rejectsTags(coreTransitionCodec),
      rejectsTags(publicationRestorationCodec),
      rejectsTags(coreStageCodec),
      rejectsTags(effectKindCodec),
      rejectsTags(terminalEffectReceiptPayloadCodec),
      rejectsTags(coordinatorMutationKindCodec),
      rejectsTags(recoveryReasonCodec),
      rejectsTags(coordinatorModeCodec),
      rejects(finalityArtifactKindCodec.complete, encoded(uint8, 7)),
      (16 to 19).forall(tag => rejects(finalityArtifactKindCodec.complete, encoded(uint8, tag))),
      rejects(terminalEffectReceiptPayloadCodec, encoded(uint8, 3)),
      rejects(coordinatorMutationKindCodec.complete, encoded(uint8, 9))
    )
  }

  test("fork-choice decision binds one opaque tag-8 evidence commitment") {
    val decision = ForkChoiceDecision(selectionEvidenceArtifact)

    expect.all(
      roundTripsAndRejectsTrailing(forkChoiceDecisionCodec.complete, decision),
      encoded(finalityArtifactKindCodec, FinalityArtifactKind.ForkChoiceDecisionEvidence).toByteVector.headOption.contains(8.toByte),
      finalityArtifactKindCodec.decodeValue(encoded(uint8, 8)).toEither.toOption.contains(FinalityArtifactKind.ForkChoiceDecisionEvidence),
      rejects(finalityArtifactKindCodec.complete, encoded(uint8, 7))
    )
  }

  test("generic fork-choice replacement and rollback retain closed canonical transition tags") {
    val orphaned = pathManifestRef.copy(
      commitment = pathManifestRef.commitment.copy(
        summary = pathManifestRef.summary.copy(role = PathRole.Orphaned)
      )
    )
    val adopted = pathManifestRef.copy(
      commitment = pathManifestRef.commitment.copy(
        summary = pathManifestRef.summary.copy(role = PathRole.Adopted)
      )
    )
    val replacement = CoreTransition.ForkChoiceReplacement(targetState, orphaned, adopted)
    val rollback = CoreTransition.ForkChoiceRollbackToOperationalMrca(targetState, orphaned)

    expect.all(
      roundTripsAndRejectsTrailing(coreTransitionCodec.complete, replacement),
      roundTripsAndRejectsTrailing(coreTransitionCodec.complete, rollback),
      roundTripsAndRejectsTrailing(transitionShapeCodec.complete, replacement.shape),
      roundTripsAndRejectsTrailing(transitionShapeCodec.complete, rollback.shape),
      encoded(coreTransitionCodec, replacement).toByteVector.headOption.contains(2.toByte),
      encoded(coreTransitionCodec, rollback).toByteVector.headOption.contains(3.toByte)
    )
  }

  test("publication restoration variants round-trip with closed canonical tags") {
    val priorUnchanged = PublicationRestoration.PriorUnchanged(expectedPublication)
    val restored = MptActivePublication(
      MptPublicationRevision(targetPublication.revision.value + 1L),
      expectedPublication.image
    )
    val appliedTargetReverted = PublicationRestoration.AppliedTargetReverted(targetPublication, restored)
    val cause = ForkChoiceOrphanClaim(
      intentId,
      selection,
      selection,
      targetState,
      ScopedArtifactRef(intentId, selectionEvidenceArtifact)
    )
    val restoring = CoreStage.RestoringPrior(cause, appliedTargetReverted)

    expect.all(
      roundTripsAndRejectsTrailing(publicationRestorationCodec.complete, priorUnchanged),
      roundTripsAndRejectsTrailing(publicationRestorationCodec.complete, appliedTargetReverted),
      roundTripsAndRejectsTrailing(coreStageCodec.complete, restoring),
      encoded(publicationRestorationCodec, priorUnchanged).toByteVector.headOption.contains(1.toByte),
      encoded(publicationRestorationCodec, appliedTargetReverted).toByteVector.headOption.contains(2.toByte),
      rejectsTags(publicationRestorationCodec)
    )
  }

  test("startup dependency failure is appended at tag 11 and requires a canonical nonzero digest") {
    val valid = RecoveryReason.StartupDependencyFailure(hash(901))
    val validBits = encoded(recoveryReasonCodec, valid)
    val zero = RecoveryReason.StartupDependencyFailure(Hash.empty)
    val uppercase = RecoveryReason.StartupDependencyFailure(Hash("AB" * 32))

    expect.all(
      validBits.toByteVector.headOption.contains(0x0b.toByte),
      recoveryReasonCodec.decodeValue(validBits).toEither.toOption.contains(valid),
      recoveryReasonCodec.encode(zero).toEither.isLeft,
      recoveryReasonCodec.encode(uppercase).toEither.isLeft
    )
  }

  test("reserved empty identity hashes fail closed") {
    val emptyHashBits = encoded(rawHashCodec, Hash.empty)
    val emptyImageReceipt = imageReceipt.copy(imageId = MptImageId(Hash.empty))

    expect.all(
      rejects(requiredHashCodec.complete, emptyHashBits),
      rejects(intentIdCodec.complete, emptyHashBits),
      rejects(artifactIdCodec.complete, emptyHashBits),
      rejects(artifactDigestCodec.complete, emptyHashBits),
      rejects(artifactEncodingCodec.complete, emptyHashBits),
      rejects(effectIdCodec.complete, emptyHashBits),
      rejects(effectStateDigestCodec.complete, emptyHashBits),
      rejects(effectManifestIdCodec.complete, emptyHashBits),
      rejects(effectManifestDigestCodec.complete, emptyHashBits),
      rejects(auditRecordIdCodec.complete, emptyHashBits),
      rejects(auditRecordDigestCodec.complete, emptyHashBits),
      rejects(recoveryRecordIdCodec.complete, emptyHashBits),
      rejects(recoveryRecordDigestCodec.complete, emptyHashBits),
      mptImageReceiptCodec.encode(emptyImageReceipt).toEither.isLeft
    )
  }

  test("noncanonical in-memory hash spellings fail before encoding") {
    val uppercase = Hash("AB" * 32)

    expect.all(
      requiredHashCodec.encode(uppercase).toEither.isLeft,
      globalSnapshotStateRefCodec.encode(targetState.copy(hash = uppercase)).toEither.isLeft,
      globalSnapshotStateRefCodec.encode(targetState.copy(parentHash = uppercase)).toEither.isLeft
    )
  }

  test("negative numeric encodings fail before payload construction") {
    val negativeLong = encoded(int64, -1L)
    val negativeInt = encoded(int32, -1)
    val negativeChunkIndex =
      encoded(intentIdCodec, intentId) ++ encoded(artifactIdCodec, pathManifestArtifact.id) ++ negativeLong

    expect.all(
      rejects(nonNegativeLongCodec.complete, negativeLong),
      rejects(nonNegativeIntCodec.complete, negativeInt),
      rejects(releaseGenerationCodec.complete, negativeLong),
      rejects(intentAttemptCodec.complete, negativeLong),
      rejects(headRevisionCodec.complete, negativeLong),
      rejects(canonicalBranchRevisionCodec.complete, negativeLong),
      rejects(mptPublicationRevisionCodec.complete, negativeLong),
      rejects(effectSinkRevisionCodec.complete, negativeLong),
      rejects(effectOutboxRevisionCodec.complete, negativeLong),
      rejects(globalSnapshotStateRefCodec.complete, negativeLong),
      rejects(mptImageReceiptCodec.complete, negativeInt),
      rejects(pathChunkPayloadCodec, negativeChunkIndex),
      mptPublicationRevisionCodec.encode(MptPublicationRevision(-1L)).toEither.isLeft
    )
  }

  test("path chunks reject empty and over-limit entry sets on both decode and encode") {
    val prefix =
      encoded(intentIdCodec, intentId) ++
        encoded(artifactIdCodec, pathManifestArtifact.id) ++
        encoded(Primitives.nonNegLongCodec, nonNeg(0L))
    val tooLargeChunk =
      pathChunk.copy(entriesOldestFirst = NonEmptyList.fromListUnsafe(List.fill(PathChunk.MaxEntries + 1)(targetState)))

    expect.all(
      rejects(pathChunkPayloadCodec, prefix ++ encoded(uint16, 0)),
      rejects(pathChunkPayloadCodec, prefix ++ encoded(uint16, PathChunk.MaxEntries + 1)),
      pathChunkPayloadCodec.encode(tooLargeChunk).toEither.isLeft
    )
  }

  test("path manifest identity is independent of chunk framing") {
    val nextArtifact = artifact(FinalityArtifactKind.PathChunk, 500)
    val secondChunkPointer =
      PathChunkPointer(intentId, pathManifestArtifact.id, nonNeg(1L), nextArtifact)
    val oneChunkLayout = List(pathChunk)
    val twoChunkLayout = List(
      pathChunk.copy(entriesOldestFirst = NonEmptyList.one(priorState), next = Some(secondChunkPointer)),
      pathChunk.copy(
        chunkIndex = nonNeg(1L),
        entriesOldestFirst = NonEmptyList.one(targetState),
        next = None
      )
    )
    val expectedManifestBytes =
      encoded(pathSummaryCodec, pathManifestPayload.summary) ++ encoded(requiredHashCodec, pathManifestPayload.entriesRoot)

    expect.all(
      oneChunkLayout.flatMap(_.entriesOldestFirst.toList) == twoChunkLayout.flatMap(_.entriesOldestFirst.toList),
      oneChunkLayout != twoChunkLayout,
      encoded(pathManifestPayloadCodec, pathManifestPayload) == expectedManifestBytes,
      pathCommitment.payload == pathManifestPayload
    )
  }

  test("effect manifest and intent commitment retain all 18 fields in fixed tag order") {
    val decodedManifest =
      finalityEffectManifestPayloadCodec.decodeValue(encoded(finalityEffectManifestPayloadCodec, effectManifest)).require
    val decodedScope = intentScopePayloadCodec.decodeValue(encoded(intentScopePayloadCodec, intentScope)).require
    val expectedIds = effectCommands.map(_.effectId)
    val encodedTags = effectKinds.map(kind => encoded(effectKindCodec, kind).toByteVector.head.toInt & 0xff)

    expect.all(
      manifestCommands(decodedManifest).map(_.kind) == effectKinds,
      manifestCommands(decodedManifest).map(_.effectId) == expectedIds,
      planCommands(decodedScope.effects).map(_.kind) == effectKinds,
      planCommands(decodedScope.effects).map(_.effectId) == expectedIds,
      encodedTags == (1 to 18).toList
    )
  }

  test("greenfield ScodecV1 payloads have no discarded-draft compatibility path") {
    expect.all(
      rejectsDiscardedDraftEnvelope(pathManifestPayloadCodec, pathManifestPayload),
      rejectsDiscardedDraftEnvelope(pathChunkPayloadCodec, pathChunk),
      rejectsDiscardedDraftEnvelope(intentScopePayloadCodec, intentScope),
      rejectsDiscardedDraftEnvelope(finalityCoreBatchPayloadCodec, coreBatch),
      rejectsDiscardedDraftEnvelope(releasedCoreRecordPayloadCodec, releasedRecordPayload),
      rejectsDiscardedDraftEnvelope(finalityEffectManifestPayloadCodec, effectManifest),
      rejectsDiscardedDraftEnvelope(terminalEffectReceiptPayloadCodec, terminalEffectReceipt),
      rejectsDiscardedDraftEnvelope(finalityEffectOutboxHeadPayloadCodec, effectOutboxHead),
      rejectsDiscardedDraftEnvelope(recoveryRecordPayloadCodec, recoveryRecord),
      rejectsDiscardedDraftEnvelope(coordinatorHeadPayloadCodec, coordinatorHead),
      rejectsDiscardedDraftEnvelope(coordinatorAuditRecordPayloadCodec, auditRecord)
    )
  }
}
