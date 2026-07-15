package io.constellationnetwork.node.shared.domain.snapshot.finality

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityBaseCodecs._

import scodec.Codec
import scodec.codecs.{discriminated, provide, uint8}
import shapeless.{::, HNil}

/** Canonical greenfield ScodecV1 payload codecs for generation-indexed finality effects. */
object FinalityEffectCodecs {

  implicit val effectKindCodec: Codec[EffectKind] =
    discriminated[EffectKind]
      .by(uint8)
      .typecase(1, provide(EffectKind.ChainStoreProjection))
      .typecase(2, provide(EffectKind.SnapshotStorageProjection))
      .typecase(3, provide(EffectKind.TipTrackerProjection))
      .typecase(4, provide(EffectKind.OverlayCacheProjection))
      .typecase(5, provide(EffectKind.ServiceAvailabilityWatermarkProjection))
      .typecase(6, provide(EffectKind.LatestSliceProjection))
      .typecase(7, provide(EffectKind.FollowProjectionRing))
      .typecase(8, provide(EffectKind.AccumulatorChangesetPromotion))
      .typecase(9, provide(EffectKind.SignedBytesPromotion))
      .typecase(10, provide(EffectKind.SidecarOutboxReconciliation))
      .typecase(11, provide(EffectKind.ShardAnchorAndWatermarkReconciliation))
      .typecase(12, provide(EffectKind.BinaryConfirmationAndRequeue))
      .typecase(13, provide(EffectKind.CommitteeAdmissionMaintenance))
      .typecase(14, provide(EffectKind.EtaCommitteeAnchorReconciliation))
      .typecase(15, provide(EffectKind.MempoolReconciliation))
      .typecase(16, provide(EffectKind.TowerIndexReconciliation))
      .typecase(17, provide(EffectKind.DownstreamFollowerEventEnqueue))
      .typecase(18, provide(EffectKind.SnapshotRetentionPruning))

  implicit val effectIdCodec: Codec[EffectId] = requiredHashCodec.xmap(EffectId(_), _.value)
  implicit val effectStateDigestCodec: Codec[EffectStateDigest] =
    requiredHashCodec.xmap(EffectStateDigest(_), _.value)
  implicit val effectSinkRevisionCodec: Codec[EffectSinkRevision] =
    io.constellationnetwork.serde.codecs.Primitives.nonNegLongCodec.xmap(EffectSinkRevision(_), _.value)
  implicit val effectManifestIdCodec: Codec[EffectManifestId] =
    requiredHashCodec.xmap(EffectManifestId(_), _.value)
  implicit val effectManifestDigestCodec: Codec[EffectManifestDigest] =
    requiredHashCodec.xmap(EffectManifestDigest(_), _.value)

  implicit val effectManifestPointerCodec: Codec[EffectManifestPointer] =
    (releaseGenerationCodec :: effectManifestIdCodec :: effectManifestDigestCodec).xmap[EffectManifestPointer](
      { case generation :: id :: digest :: HNil => EffectManifestPointer(generation, id, digest) },
      value => value.generation :: value.id :: value.digest :: HNil
    )

  implicit val effectScopeCodec: Codec[EffectScope] =
    (finalityDomainCodec :: intentIdCodec :: releaseGenerationCodec :: strictOption(globalSnapshotStateRefCodec) :: requiredHashCodec ::
      globalSnapshotStateRefCodec).xmap[EffectScope](
      {
        case domain :: intent :: generation :: priorState :: transitionDigest :: target :: HNil =>
          EffectScope(domain, intent, generation, priorState, transitionDigest, target)
      },
      value => value.domain :: value.intent :: value.generation :: value.priorState :: value.transitionDigest :: value.target :: HNil
    )

  private val rawEffectCommandIdentityCodec: Codec[EffectCommandIdentity] =
    (finalityDomainCodec :: releaseGenerationCodec :: strictOption(globalSnapshotStateRefCodec) :: requiredHashCodec ::
      globalSnapshotStateRefCodec :: effectKindCodec :: effectStateDigestCodec :: effectSinkRevisionCodec ::
      effectStateDigestCodec :: effectSinkRevisionCodec :: immutableArtifactPointerCodec ::
      strictOption(effectIdCodec)).xmap[EffectCommandIdentity](
      {
        case domain :: generation :: priorState :: transitionDigest :: target :: kind :: expectedBefore ::
            expectedRevision :: desiredAfter :: desiredRevision :: payload :: predecessor :: HNil =>
          EffectCommandIdentity(
            domain,
            generation,
            priorState,
            transitionDigest,
            target,
            kind,
            expectedBefore,
            expectedRevision,
            desiredAfter,
            desiredRevision,
            payload,
            predecessor
          )
      },
      value =>
        value.domain :: value.generation :: value.priorState :: value.transitionDigest :: value.target :: value.kind ::
          value.expectedBefore :: value.expectedRevision :: value.desiredAfter :: value.desiredRevision :: value.payload ::
          value.predecessor :: HNil
    )

  implicit val effectCommandIdentityCodec: Codec[EffectCommandIdentity] = rawEffectCommandIdentityCodec.complete

  implicit val effectCommandCommitmentCodec: Codec[EffectCommandCommitment] =
    (effectKindCodec :: effectIdCodec :: effectStateDigestCodec :: effectSinkRevisionCodec ::
      effectStateDigestCodec :: effectSinkRevisionCodec :: immutableArtifactPointerCodec ::
      strictOption(effectIdCodec)).xmap[EffectCommandCommitment](
      {
        case kind :: effectId :: expectedBefore :: expectedRevision :: desiredAfter ::
            desiredRevision :: payload :: predecessor :: HNil =>
          EffectCommandCommitment(
            kind,
            effectId,
            expectedBefore,
            expectedRevision,
            desiredAfter,
            desiredRevision,
            payload,
            predecessor
          )
      },
      value =>
        value.kind :: value.effectId :: value.expectedBefore :: value.expectedRevision ::
          value.desiredAfter :: value.desiredRevision :: value.payload :: value.predecessor :: HNil
    )

  implicit val scopedEffectCommandCodec: Codec[ScopedEffectCommand] =
    (effectScopeCodec :: effectKindCodec :: effectIdCodec :: effectStateDigestCodec ::
      effectSinkRevisionCodec :: effectStateDigestCodec :: effectSinkRevisionCodec :: scopedArtifactRefCodec ::
      strictOption(effectIdCodec)).xmap[ScopedEffectCommand](
      {
        case scope :: kind :: effectId :: expectedBefore :: expectedRevision :: desiredAfter ::
            desiredRevision :: payload :: predecessor :: HNil =>
          ScopedEffectCommand(
            scope,
            kind,
            effectId,
            expectedBefore,
            expectedRevision,
            desiredAfter,
            desiredRevision,
            payload,
            predecessor
          )
      },
      value =>
        value.scope :: value.kind :: value.effectId :: value.expectedBefore :: value.expectedRevision ::
          value.desiredAfter :: value.desiredRevision :: value.payload :: value.predecessor :: HNil
    )

  private final case class CommitmentGroupA(
    chainStore: EffectCommandCommitment,
    snapshotStorage: EffectCommandCommitment,
    tipTracker: EffectCommandCommitment,
    overlayCache: EffectCommandCommitment,
    serviceAvailabilityWatermark: EffectCommandCommitment,
    latestSlice: EffectCommandCommitment,
    followProjectionRing: EffectCommandCommitment,
    accumulatorChangeset: EffectCommandCommitment,
    signedBytes: EffectCommandCommitment
  )

  private final case class CommitmentGroupB(
    sidecarOutbox: EffectCommandCommitment,
    shardAnchorAndWatermark: EffectCommandCommitment,
    binaryConfirmationAndRequeue: EffectCommandCommitment,
    committeeAdmission: EffectCommandCommitment,
    etaCommitteeAnchor: EffectCommandCommitment,
    mempool: EffectCommandCommitment,
    towerIndex: EffectCommandCommitment,
    downstreamFollowerEvent: EffectCommandCommitment,
    snapshotRetention: EffectCommandCommitment
  )

  private val commitmentGroupACodec: Codec[CommitmentGroupA] =
    (effectCommandCommitmentCodec :: effectCommandCommitmentCodec :: effectCommandCommitmentCodec ::
      effectCommandCommitmentCodec :: effectCommandCommitmentCodec :: effectCommandCommitmentCodec ::
      effectCommandCommitmentCodec :: effectCommandCommitmentCodec :: effectCommandCommitmentCodec).xmap[CommitmentGroupA](
      {
        case chainStore :: snapshotStorage :: tipTracker :: overlayCache :: serviceAvailabilityWatermark :: latestSlice ::
            followProjectionRing :: accumulatorChangeset :: signedBytes :: HNil =>
          CommitmentGroupA(
            chainStore,
            snapshotStorage,
            tipTracker,
            overlayCache,
            serviceAvailabilityWatermark,
            latestSlice,
            followProjectionRing,
            accumulatorChangeset,
            signedBytes
          )
      },
      value =>
        value.chainStore :: value.snapshotStorage :: value.tipTracker :: value.overlayCache :: value.serviceAvailabilityWatermark ::
          value.latestSlice :: value.followProjectionRing :: value.accumulatorChangeset :: value.signedBytes :: HNil
    )

  private val commitmentGroupBCodec: Codec[CommitmentGroupB] =
    (effectCommandCommitmentCodec :: effectCommandCommitmentCodec :: effectCommandCommitmentCodec ::
      effectCommandCommitmentCodec :: effectCommandCommitmentCodec :: effectCommandCommitmentCodec ::
      effectCommandCommitmentCodec :: effectCommandCommitmentCodec :: effectCommandCommitmentCodec).xmap[CommitmentGroupB](
      {
        case sidecarOutbox :: shardAnchorAndWatermark :: binaryConfirmationAndRequeue :: committeeAdmission ::
            etaCommitteeAnchor :: mempool :: towerIndex :: downstreamFollowerEvent :: snapshotRetention :: HNil =>
          CommitmentGroupB(
            sidecarOutbox,
            shardAnchorAndWatermark,
            binaryConfirmationAndRequeue,
            committeeAdmission,
            etaCommitteeAnchor,
            mempool,
            towerIndex,
            downstreamFollowerEvent,
            snapshotRetention
          )
      },
      value =>
        value.sidecarOutbox :: value.shardAnchorAndWatermark :: value.binaryConfirmationAndRequeue :: value.committeeAdmission ::
          value.etaCommitteeAnchor :: value.mempool :: value.towerIndex :: value.downstreamFollowerEvent ::
          value.snapshotRetention :: HNil
    )

  implicit val effectPlanCommitmentCodec: Codec[EffectPlanCommitment] =
    (strictOption(effectManifestPointerCodec) :: commitmentGroupACodec :: commitmentGroupBCodec)
      .xmap[EffectPlanCommitment](
        {
          case previous :: a :: b :: HNil =>
            EffectPlanCommitment(
              previous,
              a.chainStore,
              a.snapshotStorage,
              a.tipTracker,
              a.overlayCache,
              a.serviceAvailabilityWatermark,
              a.latestSlice,
              a.followProjectionRing,
              a.accumulatorChangeset,
              a.signedBytes,
              b.sidecarOutbox,
              b.shardAnchorAndWatermark,
              b.binaryConfirmationAndRequeue,
              b.committeeAdmission,
              b.etaCommitteeAnchor,
              b.mempool,
              b.towerIndex,
              b.downstreamFollowerEvent,
              b.snapshotRetention
            )
        },
        value =>
          value.previous ::
            CommitmentGroupA(
              value.chainStoreProjection,
              value.snapshotStorageProjection,
              value.tipTrackerProjection,
              value.overlayCacheProjection,
              value.serviceAvailabilityWatermarkProjection,
              value.latestSliceProjection,
              value.followProjectionRing,
              value.accumulatorChangesetPromotion,
              value.signedBytesPromotion
            ) ::
            CommitmentGroupB(
              value.sidecarOutboxReconciliation,
              value.shardAnchorAndWatermarkReconciliation,
              value.binaryConfirmationAndRequeue,
              value.committeeAdmissionMaintenance,
              value.etaCommitteeAnchorReconciliation,
              value.mempoolReconciliation,
              value.towerIndexReconciliation,
              value.downstreamFollowerEventEnqueue,
              value.snapshotRetentionPruning
            ) :: HNil
      )

  private final case class CommandGroupA(
    chainStore: ScopedEffectCommand,
    snapshotStorage: ScopedEffectCommand,
    tipTracker: ScopedEffectCommand,
    overlayCache: ScopedEffectCommand,
    serviceAvailabilityWatermark: ScopedEffectCommand,
    latestSlice: ScopedEffectCommand,
    followProjectionRing: ScopedEffectCommand,
    accumulatorChangeset: ScopedEffectCommand,
    signedBytes: ScopedEffectCommand
  )

  private final case class CommandGroupB(
    sidecarOutbox: ScopedEffectCommand,
    shardAnchorAndWatermark: ScopedEffectCommand,
    binaryConfirmationAndRequeue: ScopedEffectCommand,
    committeeAdmission: ScopedEffectCommand,
    etaCommitteeAnchor: ScopedEffectCommand,
    mempool: ScopedEffectCommand,
    towerIndex: ScopedEffectCommand,
    downstreamFollowerEvent: ScopedEffectCommand,
    snapshotRetention: ScopedEffectCommand
  )

  private val commandGroupACodec: Codec[CommandGroupA] =
    (scopedEffectCommandCodec :: scopedEffectCommandCodec :: scopedEffectCommandCodec :: scopedEffectCommandCodec ::
      scopedEffectCommandCodec :: scopedEffectCommandCodec :: scopedEffectCommandCodec :: scopedEffectCommandCodec ::
      scopedEffectCommandCodec).xmap[CommandGroupA](
      {
        case chainStore :: snapshotStorage :: tipTracker :: overlayCache :: serviceAvailabilityWatermark :: latestSlice ::
            followProjectionRing :: accumulatorChangeset :: signedBytes :: HNil =>
          CommandGroupA(
            chainStore,
            snapshotStorage,
            tipTracker,
            overlayCache,
            serviceAvailabilityWatermark,
            latestSlice,
            followProjectionRing,
            accumulatorChangeset,
            signedBytes
          )
      },
      value =>
        value.chainStore :: value.snapshotStorage :: value.tipTracker :: value.overlayCache :: value.serviceAvailabilityWatermark ::
          value.latestSlice :: value.followProjectionRing :: value.accumulatorChangeset :: value.signedBytes :: HNil
    )

  private val commandGroupBCodec: Codec[CommandGroupB] =
    (scopedEffectCommandCodec :: scopedEffectCommandCodec :: scopedEffectCommandCodec :: scopedEffectCommandCodec ::
      scopedEffectCommandCodec :: scopedEffectCommandCodec :: scopedEffectCommandCodec :: scopedEffectCommandCodec ::
      scopedEffectCommandCodec).xmap[CommandGroupB](
      {
        case sidecarOutbox :: shardAnchorAndWatermark :: binaryConfirmationAndRequeue :: committeeAdmission ::
            etaCommitteeAnchor :: mempool :: towerIndex :: downstreamFollowerEvent :: snapshotRetention :: HNil =>
          CommandGroupB(
            sidecarOutbox,
            shardAnchorAndWatermark,
            binaryConfirmationAndRequeue,
            committeeAdmission,
            etaCommitteeAnchor,
            mempool,
            towerIndex,
            downstreamFollowerEvent,
            snapshotRetention
          )
      },
      value =>
        value.sidecarOutbox :: value.shardAnchorAndWatermark :: value.binaryConfirmationAndRequeue :: value.committeeAdmission ::
          value.etaCommitteeAnchor :: value.mempool :: value.towerIndex :: value.downstreamFollowerEvent ::
          value.snapshotRetention :: HNil
    )

  private val rawFinalityEffectManifestCodec: Codec[FinalityEffectManifest] =
    (effectScopeCodec :: strictOption(effectManifestPointerCodec) :: commandGroupACodec :: commandGroupBCodec)
      .xmap[FinalityEffectManifest](
        {
          case scope :: previous :: a :: b :: HNil =>
            FinalityEffectManifest(
              scope,
              previous,
              a.chainStore,
              a.snapshotStorage,
              a.tipTracker,
              a.overlayCache,
              a.serviceAvailabilityWatermark,
              a.latestSlice,
              a.followProjectionRing,
              a.accumulatorChangeset,
              a.signedBytes,
              b.sidecarOutbox,
              b.shardAnchorAndWatermark,
              b.binaryConfirmationAndRequeue,
              b.committeeAdmission,
              b.etaCommitteeAnchor,
              b.mempool,
              b.towerIndex,
              b.downstreamFollowerEvent,
              b.snapshotRetention
            )
        },
        value =>
          value.scope :: value.previous ::
            CommandGroupA(
              value.chainStoreProjection,
              value.snapshotStorageProjection,
              value.tipTrackerProjection,
              value.overlayCacheProjection,
              value.serviceAvailabilityWatermarkProjection,
              value.latestSliceProjection,
              value.followProjectionRing,
              value.accumulatorChangesetPromotion,
              value.signedBytesPromotion
            ) ::
            CommandGroupB(
              value.sidecarOutboxReconciliation,
              value.shardAnchorAndWatermarkReconciliation,
              value.binaryConfirmationAndRequeue,
              value.committeeAdmissionMaintenance,
              value.etaCommitteeAnchorReconciliation,
              value.mempoolReconciliation,
              value.towerIndexReconciliation,
              value.downstreamFollowerEventEnqueue,
              value.snapshotRetentionPruning
            ) :: HNil
      )

  val finalityEffectManifestPayloadCodec: Codec[FinalityEffectManifest] = rawFinalityEffectManifestCodec.complete

  private val appliedReceiptCodec: Codec[AppliedEffectReceipt] =
    (effectManifestPointerCodec :: effectIdCodec :: effectStateDigestCodec :: effectSinkRevisionCodec :: effectStateDigestCodec ::
      effectSinkRevisionCodec)
      .xmap[AppliedEffectReceipt](
        {
          case manifest :: effectId :: before :: beforeRevision :: state :: revision :: HNil =>
            AppliedEffectReceipt(manifest, effectId, before, beforeRevision, state, revision)
        },
        value =>
          value.manifest :: value.effectId :: value.observedBefore :: value.observedBeforeRevision :: value.observedState ::
            value.sinkRevision :: HNil
      )

  private val alreadyAppliedReceiptCodec: Codec[AlreadyAppliedEffectReceipt] =
    (effectManifestPointerCodec :: effectIdCodec :: effectStateDigestCodec :: effectSinkRevisionCodec)
      .xmap[AlreadyAppliedEffectReceipt](
        {
          case manifest :: effectId :: state :: revision :: HNil =>
            AlreadyAppliedEffectReceipt(manifest, effectId, state, revision)
        },
        value => value.manifest :: value.effectId :: value.observedState :: value.sinkRevision :: HNil
      )

  private val rawTerminalEffectReceiptCodec: Codec[TerminalEffectReceipt] =
    discriminated[TerminalEffectReceipt]
      .by(uint8)
      .typecase(1, appliedReceiptCodec)
      .typecase(2, alreadyAppliedReceiptCodec)

  val terminalEffectReceiptPayloadCodec: Codec[TerminalEffectReceipt] = rawTerminalEffectReceiptCodec.complete

  implicit val effectOutboxRevisionCodec: Codec[EffectOutboxRevision] =
    io.constellationnetwork.serde.codecs.Primitives.nonNegLongCodec.xmap(EffectOutboxRevision(_), _.value)

  implicit val effectOutboxCursorCodec: Codec[EffectOutboxCursor] =
    (strictOption(releaseGenerationCodec) :: strictOption(effectManifestPointerCodec)).xmap[EffectOutboxCursor](
      { case completedThrough :: completedManifest :: HNil => EffectOutboxCursor(completedThrough, completedManifest) },
      value => value.completedThrough :: value.completedManifest :: HNil
    )

  private val rawFinalityEffectOutboxHeadCodec: Codec[FinalityEffectOutboxHead] =
    (effectOutboxRevisionCodec :: effectOutboxCursorCodec).xmap[FinalityEffectOutboxHead](
      { case revision :: cursor :: HNil => FinalityEffectOutboxHead(revision, cursor) },
      value => value.revision :: value.cursor :: HNil
    )

  val finalityEffectOutboxHeadPayloadCodec: Codec[FinalityEffectOutboxHead] = rawFinalityEffectOutboxHeadCodec.complete
}
