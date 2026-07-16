package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.data.NonEmptyList

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt._
import io.constellationnetwork.serde.codecs.Primitives
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec._
import scodec.bits.BitVector
import scodec.codecs._
import shapeless.{::, HNil}

/** Shared canonical ScodecV1 codecs for greenfield finality payloads.
  *
  * These codecs intentionally have no legacy decoder. Persisted top-level values are exposed as complete codecs by the core, effects, and
  * coordinator objects.
  */
private[finality] object FinalityBaseCodecs {

  private val snapshotOrdinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]

  private val canonicalHashCodec: Codec[Hash] =
    hashCodec.exmap(
      hash =>
        if (isCanonicalHash(hash)) Attempt.successful(hash)
        else Attempt.failure(Err("hash must be 64-character lowercase hexadecimal")),
      hash =>
        if (isCanonicalHash(hash)) Attempt.successful(hash)
        else Attempt.failure(Err("hash must be 64-character lowercase hexadecimal"))
    )

  val requiredHashCodec: Codec[Hash] =
    canonicalHashCodec.exmap(
      hash =>
        if (hash == Hash.empty) Attempt.failure(Err("reserved empty hash"))
        else Attempt.successful(hash),
      hash =>
        if (hash == Hash.empty) Attempt.failure(Err("reserved empty hash"))
        else Attempt.successful(hash)
    )

  val nonNegativeLongCodec: Codec[Long] =
    int64.exmap(
      value => if (value >= 0L) Attempt.successful(value) else Attempt.failure(Err(s"negative long: $value")),
      value => if (value >= 0L) Attempt.successful(value) else Attempt.failure(Err(s"negative long: $value"))
    )

  val nonNegativeIntCodec: Codec[Int] =
    int32.exmap(
      value => if (value >= 0) Attempt.successful(value) else Attempt.failure(Err(s"negative int: $value")),
      value => if (value >= 0) Attempt.successful(value) else Attempt.failure(Err(s"negative int: $value"))
    )

  /** Canonical byte option. Only `0x00` and `0x01` are accepted. */
  def strictOption[A](inner: Codec[A]): Codec[Option[A]] =
    new Codec[Option[A]] {
      override val sizeBound: SizeBound = SizeBound.atLeast(8L)

      override def encode(value: Option[A]): Attempt[BitVector] =
        value match {
          case None => uint8.encode(0)
          case Some(innerValue) =>
            for {
              tag <- uint8.encode(1)
              body <- inner.encode(innerValue)
            } yield tag ++ body
        }

      override def decode(bits: BitVector): Attempt[DecodeResult[Option[A]]] =
        uint8.decode(bits).flatMap {
          case DecodeResult(0, remainder) => Attempt.successful(DecodeResult(None, remainder))
          case DecodeResult(1, remainder) => inner.decode(remainder).map(result => result.map(Some(_)))
          case DecodeResult(other, _)     => Attempt.failure(Err(s"strict option tag must be 0 or 1, got $other"))
        }
    }

  implicit val releaseGenerationCodec: Codec[ReleaseGeneration] =
    Primitives.nonNegLongCodec.xmap(ReleaseGeneration(_), _.value)
  implicit val intentAttemptCodec: Codec[IntentAttempt] =
    Primitives.nonNegLongCodec.xmap(IntentAttempt(_), _.value)
  implicit val headRevisionCodec: Codec[HeadRevision] =
    Primitives.nonNegLongCodec.xmap(HeadRevision(_), _.value)
  implicit val canonicalBranchRevisionCodec: Codec[CanonicalBranchRevision] =
    Primitives.nonNegLongCodec.xmap(CanonicalBranchRevision(_), _.value)
  implicit val canonicalLineageRevisionCodec: Codec[CanonicalLineageRevision] =
    Primitives.nonNegLongCodec.xmap(CanonicalLineageRevision(_), _.value)

  implicit val intentIdCodec: Codec[IntentId] = requiredHashCodec.xmap(IntentId(_), _.value)
  implicit val artifactIdCodec: Codec[ArtifactId] = requiredHashCodec.xmap(ArtifactId(_), _.value)
  implicit val artifactDigestCodec: Codec[ArtifactDigest] = requiredHashCodec.xmap(ArtifactDigest(_), _.value)
  implicit val artifactEncodingCodec: Codec[ArtifactEncoding] = requiredHashCodec.xmap(ArtifactEncoding(_), _.value)

  implicit val finalityArtifactKindCodec: Codec[FinalityArtifactKind] =
    discriminated[FinalityArtifactKind]
      .by(uint8)
      .typecase(1, provide(FinalityArtifactKind.CoreBatch))
      .typecase(2, provide(FinalityArtifactKind.ReleasedCoreRecord))
      .typecase(3, provide(FinalityArtifactKind.PathManifest))
      .typecase(4, provide(FinalityArtifactKind.PathChunk))
      .typecase(5, provide(FinalityArtifactKind.DecidedAttestationEvidence))
      .typecase(6, provide(FinalityArtifactKind.DepthK1Evidence))
      .typecase(7, provide(FinalityArtifactKind.ForkChoiceDecisionEvidence))
      .typecase(8, provide(FinalityArtifactKind.PreparedSemanticState))
      .typecase(9, provide(FinalityArtifactKind.AuthenticatedTargetAnchor))
      .typecase(10, provide(FinalityArtifactKind.AppliedSemanticStateReceipt))
      .typecase(11, provide(FinalityArtifactKind.AuthenticatedAnchorReceipt))
      .typecase(12, provide(FinalityArtifactKind.PriorSemanticStateReceipt))
      .typecase(13, provide(FinalityArtifactKind.PriorAnchorReceipt))
      .typecase(14, provide(FinalityArtifactKind.EffectPayload))

  implicit val immutableArtifactPointerCodec: Codec[ImmutableArtifactPointer] =
    (finalityArtifactKindCodec :: artifactEncodingCodec :: artifactIdCodec :: artifactDigestCodec :: Primitives.nonNegLongCodec)
      .xmap[ImmutableArtifactPointer](
        {
          case kind :: encoding :: id :: digest :: byteLength :: HNil =>
            ImmutableArtifactPointer(kind, encoding, id, digest, byteLength)
        },
        value => value.kind :: value.encoding :: value.id :: value.digest :: value.byteLength :: HNil
      )

  implicit val scopedArtifactRefCodec: Codec[ScopedArtifactRef] =
    (intentIdCodec :: immutableArtifactPointerCodec).xmap[ScopedArtifactRef](
      { case intentId :: artifact :: HNil => ScopedArtifactRef(intentId, artifact) },
      value => value.intentId :: value.artifact :: HNil
    )

  implicit val globalSnapshotStateRefCodec: Codec[GlobalSnapshotStateRef] = {
    val mptRootCodec = requiredHashCodec.xmap(MptRoot(_), (root: MptRoot) => root.value)

    (snapshotOrdinalCodec :: requiredHashCodec :: canonicalHashCodec :: mptRootCodec).xmap[GlobalSnapshotStateRef](
      { case ordinal :: hash :: parentHash :: mptRoot :: HNil => GlobalSnapshotStateRef(ordinal, hash, parentHash, mptRoot) },
      value => value.ordinal :: value.hash :: value.parentHash :: value.mptRoot :: HNil
    )
  }

  implicit val pathRoleCodec: Codec[PathRole] =
    discriminated[PathRole]
      .by(uint8)
      .typecase(1, provide(PathRole.Adopted))
      .typecase(2, provide(PathRole.Orphaned))
      .typecase(3, provide(PathRole.OperationalAncestorClosure))
      .typecase(4, provide(PathRole.CanonicalLineage))

  implicit val pathSummaryCodec: Codec[PathSummary] =
    (pathRoleCodec :: globalSnapshotStateRefCodec :: globalSnapshotStateRefCodec :: Primitives.nonNegLongCodec)
      .xmap[PathSummary](
        { case role :: oldest :: newest :: entryCount :: HNil => PathSummary(role, oldest, newest, entryCount) },
        value => value.role :: value.oldest :: value.newest :: value.entryCount :: HNil
      )

  implicit val pathCommitmentCodec: Codec[PathCommitment] =
    (pathSummaryCodec :: immutableArtifactPointerCodec :: requiredHashCodec)
      .xmap[PathCommitment](
        {
          case summary :: manifest :: entriesRoot :: HNil =>
            PathCommitment(summary, manifest, entriesRoot)
        },
        value => value.summary :: value.manifest :: value.entriesRoot :: HNil
      )

  private val rawPathManifestPayloadCodec: Codec[PathManifestPayload] =
    (pathSummaryCodec :: requiredHashCodec).xmap[PathManifestPayload](
      { case summary :: entriesRoot :: HNil => PathManifestPayload(summary, entriesRoot) },
      value => value.summary :: value.entriesRoot :: HNil
    )

  val pathManifestPayloadCodec: Codec[PathManifestPayload] = rawPathManifestPayloadCodec.complete

  implicit val pathManifestRefCodec: Codec[PathManifestRef] =
    (pathCommitmentCodec :: scopedArtifactRefCodec).xmap[PathManifestRef](
      { case commitment :: manifest :: HNil => PathManifestRef(commitment, manifest) },
      value => value.commitment :: value.manifest :: HNil
    )

  implicit val forkChoiceDecisionCodec: Codec[ForkChoiceDecision] =
    immutableArtifactPointerCodec.xmap(ForkChoiceDecision(_), _.evidence)

  implicit val canonicalSelectionTokenCodec: Codec[CanonicalSelectionToken] =
    (canonicalBranchRevisionCodec :: globalSnapshotStateRefCodec :: globalSnapshotStateRefCodec :: forkChoiceDecisionCodec ::
      pathCommitmentCodec).xmap[CanonicalSelectionToken](
      {
        case branchRevision :: selectedTip :: operationalTarget :: decision :: lineage :: HNil =>
          CanonicalSelectionToken(branchRevision, selectedTip, operationalTarget, decision, lineage)
      },
      value => value.branchRevision :: value.selectedTip :: value.operationalTarget :: value.decision :: value.lineage :: HNil
    )

  implicit val pathChunkPointerCodec: Codec[PathChunkPointer] =
    (intentIdCodec :: artifactIdCodec :: Primitives.nonNegLongCodec :: immutableArtifactPointerCodec)
      .xmap[PathChunkPointer](
        {
          case intentId :: manifestId :: chunkIndex :: artifact :: HNil =>
            PathChunkPointer(intentId, manifestId, chunkIndex, artifact)
        },
        value => value.intentId :: value.manifestId :: value.chunkIndex :: value.artifact :: HNil
      )

  private val pathChunkEntryCountCodec: Codec[Int] =
    uint16.exmap(
      count =>
        if (count >= 1 && count <= PathChunk.MaxEntries) Attempt.successful(count)
        else Attempt.failure(Err(s"path chunk entry count must be in [1,${PathChunk.MaxEntries}], got $count")),
      count =>
        if (count >= 1 && count <= PathChunk.MaxEntries) Attempt.successful(count)
        else Attempt.failure(Err(s"path chunk entry count must be in [1,${PathChunk.MaxEntries}], got $count"))
    )

  private val pathChunkEntriesCodec: Codec[NonEmptyList[GlobalSnapshotStateRef]] =
    listOfN(pathChunkEntryCountCodec, globalSnapshotStateRefCodec).exmap(
      values =>
        NonEmptyList
          .fromList(values)
          .fold[Attempt[NonEmptyList[GlobalSnapshotStateRef]]](Attempt.failure(Err("path chunk cannot be empty")))(
            Attempt.successful
          ),
      values => Attempt.successful(values.toList)
    )

  private val rawPathChunkCodec: Codec[PathChunk] =
    (intentIdCodec :: artifactIdCodec :: Primitives.nonNegLongCodec :: pathChunkEntriesCodec :: strictOption(pathChunkPointerCodec))
      .xmap[PathChunk](
        {
          case intentId :: manifestId :: chunkIndex :: entries :: next :: HNil =>
            PathChunk(intentId, manifestId, chunkIndex, entries, next)
        },
        value => value.intentId :: value.manifestId :: value.chunkIndex :: value.entriesOldestFirst :: value.next :: HNil
      )

  val pathChunkPayloadCodec: Codec[PathChunk] = rawPathChunkCodec.complete

  implicit val operationalRailCodec: Codec[OperationalRail] =
    discriminated[OperationalRail]
      .by(uint8)
      .typecase(1, provide(OperationalRail.DecidedAttestationTWeight))
      .typecase(2, provide(OperationalRail.CanonicalDepthK1))

  implicit val operationalQualificationScopeCodec: Codec[OperationalQualificationScope] =
    (operationalRailCodec :: globalSnapshotStateRefCodec :: globalSnapshotStateRefCodec :: strictOption(pathCommitmentCodec) ::
      immutableArtifactPointerCodec).xmap[OperationalQualificationScope](
      {
        case rail :: target :: descendant :: closure :: evidence :: HNil =>
          OperationalQualificationScope(rail, target, descendant, closure, evidence)
      },
      value => value.rail :: value.operationalTarget :: value.qualifyingDescendant :: value.ancestorClosure :: value.evidence :: HNil
    )

  private val decidedAttestationQualificationCodec: Codec[OperationalQualification.DecidedAttestationTWeight] =
    (globalSnapshotStateRefCodec :: globalSnapshotStateRefCodec :: strictOption(pathManifestRefCodec) :: scopedArtifactRefCodec)
      .xmap[OperationalQualification.DecidedAttestationTWeight](
        {
          case target :: descendant :: closure :: evidence :: HNil =>
            OperationalQualification.DecidedAttestationTWeight(target, descendant, closure, evidence)
        },
        value => value.operationalTarget :: value.qualifyingDescendant :: value.ancestorClosure :: value.evidence :: HNil
      )

  private val depthK1QualificationCodec: Codec[OperationalQualification.CanonicalDepthK1] =
    (globalSnapshotStateRefCodec :: globalSnapshotStateRefCodec :: strictOption(pathManifestRefCodec) :: scopedArtifactRefCodec)
      .xmap[OperationalQualification.CanonicalDepthK1](
        {
          case target :: descendant :: closure :: evidence :: HNil =>
            OperationalQualification.CanonicalDepthK1(target, descendant, closure, evidence)
        },
        value => value.operationalTarget :: value.qualifyingDescendant :: value.ancestorClosure :: value.evidence :: HNil
      )

  implicit val operationalQualificationCodec: Codec[OperationalQualification] =
    discriminated[OperationalQualification]
      .by(uint8)
      .typecase(1, decidedAttestationQualificationCodec)
      .typecase(2, depthK1QualificationCodec)

  private val advanceShapeCodec: Codec[TransitionShape.Advance] =
    pathCommitmentCodec.xmap(TransitionShape.Advance(_), _.adopted)

  private val forkChoiceReplacementShapeCodec: Codec[TransitionShape.ForkChoiceReplacement] =
    (globalSnapshotStateRefCodec :: pathCommitmentCodec :: pathCommitmentCodec)
      .xmap[TransitionShape.ForkChoiceReplacement](
        {
          case ancestor :: orphaned :: adopted :: HNil =>
            TransitionShape.ForkChoiceReplacement(ancestor, orphaned, adopted)
        },
        value => value.commonAncestor :: value.orphaned :: value.adopted :: HNil
      )

  private val forkChoiceRollbackShapeCodec: Codec[TransitionShape.ForkChoiceRollbackToOperationalMrca] =
    (globalSnapshotStateRefCodec :: pathCommitmentCodec)
      .xmap[TransitionShape.ForkChoiceRollbackToOperationalMrca](
        {
          case mrca :: orphaned :: HNil =>
            TransitionShape.ForkChoiceRollbackToOperationalMrca(mrca, orphaned)
        },
        value => value.operationalMrca :: value.orphaned :: HNil
      )

  implicit val transitionShapeCodec: Codec[TransitionShape] =
    discriminated[TransitionShape]
      .by(uint8)
      .typecase(1, advanceShapeCodec)
      .typecase(2, forkChoiceReplacementShapeCodec)
      .typecase(3, forkChoiceRollbackShapeCodec)

  private val advanceTransitionCodec: Codec[CoreTransition.Advance] =
    pathManifestRefCodec.xmap(CoreTransition.Advance(_), _.adopted)

  private val forkChoiceReplacementTransitionCodec: Codec[CoreTransition.ForkChoiceReplacement] =
    (globalSnapshotStateRefCodec :: pathManifestRefCodec :: pathManifestRefCodec)
      .xmap[CoreTransition.ForkChoiceReplacement](
        {
          case ancestor :: orphaned :: adopted :: HNil =>
            CoreTransition.ForkChoiceReplacement(ancestor, orphaned, adopted)
        },
        value => value.commonAncestor :: value.orphaned :: value.adopted :: HNil
      )

  private val forkChoiceRollbackTransitionCodec: Codec[CoreTransition.ForkChoiceRollbackToOperationalMrca] =
    (globalSnapshotStateRefCodec :: pathManifestRefCodec)
      .xmap[CoreTransition.ForkChoiceRollbackToOperationalMrca](
        {
          case mrca :: orphaned :: HNil =>
            CoreTransition.ForkChoiceRollbackToOperationalMrca(mrca, orphaned)
        },
        value => value.operationalMrca :: value.orphaned :: HNil
      )

  implicit val coreTransitionCodec: Codec[CoreTransition] =
    discriminated[CoreTransition]
      .by(uint8)
      .typecase(1, advanceTransitionCodec)
      .typecase(2, forkChoiceReplacementTransitionCodec)
      .typecase(3, forkChoiceRollbackTransitionCodec)

  implicit val releasedCorePointerCodec: Codec[ReleasedCorePointer] =
    (releaseGenerationCodec :: intentIdCodec :: globalSnapshotStateRefCodec :: immutableArtifactPointerCodec)
      .xmap[ReleasedCorePointer](
        { case generation :: intentId :: target :: record :: HNil => ReleasedCorePointer(generation, intentId, target, record) },
        value => value.generation :: value.intentId :: value.target :: value.record :: HNil
      )

  implicit val finalityDomainCodec: Codec[FinalityDomain] =
    (requiredHashCodec :: requiredHashCodec :: requiredHashCodec :: requiredHashCodec).xmap[FinalityDomain](
      {
        case networkId :: genesisHash :: protocolEra :: parameterHash :: HNil =>
          FinalityDomain(networkId, genesisHash, protocolEra, parameterHash)
      },
      value => value.networkId :: value.genesisHash :: value.protocolEra :: value.parameterHash :: HNil
    )

  private val mptImageIdCodec: Codec[MptImageId] = requiredHashCodec.xmap(MptImageId(_), _.value)
  private val mptImageDigestCodec: Codec[MptImageDigest] = requiredHashCodec.xmap(MptImageDigest(_), _.value)
  private val mptImageCodecEraCodec: Codec[MptImageCodecEra] = requiredHashCodec.xmap(MptImageCodecEra(_), _.value)
  private val mptImageRootEraCodec: Codec[MptImageRootEra] = requiredHashCodec.xmap(MptImageRootEra(_), _.value)

  implicit val mptImageReceiptCodec: Codec[MptImageReceipt] =
    (nonNegativeIntCodec :: nonNegativeLongCodec :: mptImageIdCodec :: globalSnapshotStateRefCodec :: mptImageCodecEraCodec ::
      mptImageRootEraCodec :: mptImageDigestCodec :: nonNegativeIntCodec).xmap[MptImageReceipt](
      {
        case formatVersion :: generation :: imageId :: anchor :: codecEra :: rootEra :: digest :: entryCount :: HNil =>
          MptImageReceipt(formatVersion, generation, imageId, anchor, codecEra, rootEra, digest, entryCount)
      },
      value =>
        value.formatVersion :: value.generation :: value.imageId :: value.anchor :: value.codecEra :: value.rootEra ::
          value.digest :: value.entryCount :: HNil
    )

  implicit val mptPublicationRevisionCodec: Codec[MptPublicationRevision] =
    nonNegativeLongCodec.xmap(MptPublicationRevision(_), _.value)

  implicit val mptActivePublicationCodec: Codec[MptActivePublication] =
    (mptPublicationRevisionCodec :: strictOption(mptImageReceiptCodec)).xmap[MptActivePublication](
      { case revision :: image :: HNil => MptActivePublication(revision, image) },
      value => value.revision :: value.image :: HNil
    )

  implicit val preparedCoreCommitmentCodec: Codec[PreparedCoreCommitment] =
    (globalSnapshotStateRefCodec :: mptImageReceiptCodec :: mptActivePublicationCodec :: mptActivePublicationCodec ::
      immutableArtifactPointerCodec :: immutableArtifactPointerCodec).xmap[PreparedCoreCommitment](
      {
        case target :: preparedImage :: expectedBefore :: targetPublication :: semanticState :: authenticatedAnchor :: HNil =>
          PreparedCoreCommitment(target, preparedImage, expectedBefore, targetPublication, semanticState, authenticatedAnchor)
      },
      value =>
        value.target :: value.preparedImage :: value.expectedBefore :: value.targetPublication :: value.semanticState ::
          value.authenticatedAnchor :: HNil
    )

  implicit val preparedCoreTargetCodec: Codec[PreparedCoreTarget] =
    (globalSnapshotStateRefCodec :: mptImageReceiptCodec :: mptActivePublicationCodec :: mptActivePublicationCodec ::
      scopedArtifactRefCodec :: scopedArtifactRefCodec).xmap[PreparedCoreTarget](
      {
        case target :: preparedImage :: expectedBefore :: targetPublication :: semanticState :: authenticatedAnchor :: HNil =>
          PreparedCoreTarget(target, preparedImage, expectedBefore, targetPublication, semanticState, authenticatedAnchor)
      },
      value =>
        value.target :: value.preparedImage :: value.expectedBefore :: value.targetPublication :: value.semanticState ::
          value.authenticatedAnchor :: HNil
    )

  private def isCanonicalHash(hash: Hash): Boolean =
    hash.value.length == 64 && hash.value.forall(character => character >= '0' && character <= '9' || character >= 'a' && character <= 'f')
}
