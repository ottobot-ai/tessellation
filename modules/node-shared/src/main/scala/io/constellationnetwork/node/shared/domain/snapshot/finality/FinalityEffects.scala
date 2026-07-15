package io.constellationnetwork.node.shared.domain.snapshot.finality

import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong

/** Closed set of durable projections driven by a released GL0 finality generation.
  *
  * None of these effects decides finality or makes economic state authoritative. The released core and its exact MPT publication are
  * established separately.
  */
sealed trait EffectKind extends Product with Serializable

object EffectKind {
  case object ChainStoreProjection extends EffectKind
  case object SnapshotStorageProjection extends EffectKind
  case object TipTrackerProjection extends EffectKind
  case object OverlayCacheProjection extends EffectKind

  /** Rebuildable serving/retention watermark. It is never fork-choice truth. */
  case object ServiceAvailabilityWatermarkProjection extends EffectKind
  case object LatestSliceProjection extends EffectKind
  case object FollowProjectionRing extends EffectKind
  case object AccumulatorChangesetPromotion extends EffectKind
  case object SignedBytesPromotion extends EffectKind
  case object SidecarOutboxReconciliation extends EffectKind
  case object ShardAnchorAndWatermarkReconciliation extends EffectKind
  case object BinaryConfirmationAndRequeue extends EffectKind
  case object CommitteeAdmissionMaintenance extends EffectKind
  case object EtaCommitteeAnchorReconciliation extends EffectKind
  case object MempoolReconciliation extends EffectKind
  case object TowerIndexReconciliation extends EffectKind
  case object DownstreamFollowerEventEnqueue extends EffectKind
  case object SnapshotRetentionPruning extends EffectKind
}

final case class EffectId(value: Hash)

/** Derived in memory from [[EffectId]] for sink retries; never carried in a durable payload. */
final case class EffectIdempotencyKey private[finality] (value: Hash)
final case class EffectStateDigest(value: Hash)
final case class EffectSinkRevision(value: NonNegLong)

final case class EffectManifestId(value: Hash)
final case class EffectManifestDigest(value: Hash)

/** Content-addressed identity of one immutable, generation-indexed effect manifest. */
final case class EffectManifestPointer(
  generation: ReleaseGeneration,
  id: EffectManifestId,
  digest: EffectManifestDigest
)

/** Scope repeated by every effect command and checked against its manifest.
  *
  * `priorState` identifies the released state replaced by this generation. The manifest's `previous` pointer binds the corresponding
  * immutable release-order history without embedding an unbounded pending queue in the coordinator head.
  */
final case class EffectScope(
  domain: FinalityDomain,
  intent: IntentId,
  generation: ReleaseGeneration,
  priorState: Option[GlobalSnapshotStateRef],
  transitionDigest: Hash,
  target: GlobalSnapshotStateRef
)

/** Exact, idempotent command for one sink.
  *
  * `predecessor` is the immediately preceding command in the same sink lane. Commands are processed in release order and every predecessor
  * requires its own `Applied` or independently read-back `AlreadyApplied` receipt. State and revision form one compare-and-set boundary: a
  * real mutation advances the finality-owned sink revision exactly once, while an explicit no-op preserves it.
  */
final case class ScopedEffectCommand(
  scope: EffectScope,
  kind: EffectKind,
  effectId: EffectId,
  expectedBefore: EffectStateDigest,
  expectedRevision: EffectSinkRevision,
  desiredAfter: EffectStateDigest,
  desiredRevision: EffectSinkRevision,
  payload: ScopedArtifactRef,
  predecessor: Option[EffectId]
) {
  def commitment: EffectCommandCommitment =
    EffectCommandCommitment(
      kind,
      effectId,
      expectedBefore,
      expectedRevision,
      desiredAfter,
      desiredRevision,
      payload.artifact,
      predecessor
    )

  /** Complete scope-independent preimage from which [[effectId]] is derived.
    *
    * Intent and attempt are deliberately absent: retrying the exact same durable command retains its identity, while every state-bearing
    * input is committed.
    */
  def identityPreimage: EffectCommandIdentity =
    EffectCommandIdentity(
      scope.domain,
      scope.generation,
      scope.priorState,
      scope.transitionDigest,
      scope.target,
      kind,
      expectedBefore,
      expectedRevision,
      desiredAfter,
      desiredRevision,
      payload.artifact,
      predecessor
    )
}

/** Canonical input to a durable effect identity.
  *
  * This product is a protocol preimage, not a second command representation. It excludes caller-selected identifiers and all retry-only
  * scope.
  */
final case class EffectCommandIdentity(
  domain: FinalityDomain,
  generation: ReleaseGeneration,
  priorState: Option[GlobalSnapshotStateRef],
  transitionDigest: Hash,
  target: GlobalSnapshotStateRef,
  kind: EffectKind,
  expectedBefore: EffectStateDigest,
  expectedRevision: EffectSinkRevision,
  desiredAfter: EffectStateDigest,
  desiredRevision: EffectSinkRevision,
  payload: ImmutableArtifactPointer,
  predecessor: Option[EffectId]
)

/** Scope-independent identity of one effect command.
  *
  * Omitting the intent-scoped wrapper makes this suitable for [[IntentScope]]: the resulting intent identifier can then scope the exact
  * manifest without a hash cycle.
  */
final case class EffectCommandCommitment(
  kind: EffectKind,
  effectId: EffectId,
  expectedBefore: EffectStateDigest,
  expectedRevision: EffectSinkRevision,
  desiredAfter: EffectStateDigest,
  desiredRevision: EffectSinkRevision,
  payload: ImmutableArtifactPointer,
  predecessor: Option[EffectId]
)

/** Complete scope-independent effect plan committed by an intent.
  *
  * This remains an explicit product so every durable projection is mandatory in the canonical schema. In particular, chain-store indexing
  * and snapshot-byte publication are separate externally observable sinks.
  */
final case class EffectPlanCommitment(
  previous: Option[EffectManifestPointer],
  chainStoreProjection: EffectCommandCommitment,
  snapshotStorageProjection: EffectCommandCommitment,
  tipTrackerProjection: EffectCommandCommitment,
  overlayCacheProjection: EffectCommandCommitment,
  serviceAvailabilityWatermarkProjection: EffectCommandCommitment,
  latestSliceProjection: EffectCommandCommitment,
  followProjectionRing: EffectCommandCommitment,
  accumulatorChangesetPromotion: EffectCommandCommitment,
  signedBytesPromotion: EffectCommandCommitment,
  sidecarOutboxReconciliation: EffectCommandCommitment,
  shardAnchorAndWatermarkReconciliation: EffectCommandCommitment,
  binaryConfirmationAndRequeue: EffectCommandCommitment,
  committeeAdmissionMaintenance: EffectCommandCommitment,
  etaCommitteeAnchorReconciliation: EffectCommandCommitment,
  mempoolReconciliation: EffectCommandCommitment,
  towerIndexReconciliation: EffectCommandCommitment,
  downstreamFollowerEventEnqueue: EffectCommandCommitment,
  snapshotRetentionPruning: EffectCommandCommitment
)

/** One immutable manifest per successfully released generation.
  *
  * This is deliberately a product rather than a collection. Omitting, adding, or reordering a finality sink is a schema change. A sink with
  * no mutation for a generation still carries an explicit command whose before/after digests are equal.
  */
final case class FinalityEffectManifest(
  scope: EffectScope,
  previous: Option[EffectManifestPointer],
  chainStoreProjection: ScopedEffectCommand,
  snapshotStorageProjection: ScopedEffectCommand,
  tipTrackerProjection: ScopedEffectCommand,
  overlayCacheProjection: ScopedEffectCommand,
  serviceAvailabilityWatermarkProjection: ScopedEffectCommand,
  latestSliceProjection: ScopedEffectCommand,
  followProjectionRing: ScopedEffectCommand,
  accumulatorChangesetPromotion: ScopedEffectCommand,
  signedBytesPromotion: ScopedEffectCommand,
  sidecarOutboxReconciliation: ScopedEffectCommand,
  shardAnchorAndWatermarkReconciliation: ScopedEffectCommand,
  binaryConfirmationAndRequeue: ScopedEffectCommand,
  committeeAdmissionMaintenance: ScopedEffectCommand,
  etaCommitteeAnchorReconciliation: ScopedEffectCommand,
  mempoolReconciliation: ScopedEffectCommand,
  towerIndexReconciliation: ScopedEffectCommand,
  downstreamFollowerEventEnqueue: ScopedEffectCommand,
  snapshotRetentionPruning: ScopedEffectCommand
) {
  def commitment: EffectPlanCommitment =
    EffectPlanCommitment(
      previous,
      chainStoreProjection.commitment,
      snapshotStorageProjection.commitment,
      tipTrackerProjection.commitment,
      overlayCacheProjection.commitment,
      serviceAvailabilityWatermarkProjection.commitment,
      latestSliceProjection.commitment,
      followProjectionRing.commitment,
      accumulatorChangesetPromotion.commitment,
      signedBytesPromotion.commitment,
      sidecarOutboxReconciliation.commitment,
      shardAnchorAndWatermarkReconciliation.commitment,
      binaryConfirmationAndRequeue.commitment,
      committeeAdmissionMaintenance.commitment,
      etaCommitteeAnchorReconciliation.commitment,
      mempoolReconciliation.commitment,
      towerIndexReconciliation.commitment,
      downstreamFollowerEventEnqueue.commitment,
      snapshotRetentionPruning.commitment
    )
}

sealed trait TerminalEffectReceipt extends Product with Serializable {
  def manifest: EffectManifestPointer
  def effectId: EffectId
  def observedState: EffectStateDigest
  def sinkRevision: EffectSinkRevision
}

/** The sink moved from the exact expected state/revision to the exact desired pair. */
final case class AppliedEffectReceipt(
  manifest: EffectManifestPointer,
  effectId: EffectId,
  observedBefore: EffectStateDigest,
  observedBeforeRevision: EffectSinkRevision,
  observedState: EffectStateDigest,
  sinkRevision: EffectSinkRevision
) extends TerminalEffectReceipt

/** The desired state was already present and was independently read back. */
final case class AlreadyAppliedEffectReceipt(
  manifest: EffectManifestPointer,
  effectId: EffectId,
  observedState: EffectStateDigest,
  sinkRevision: EffectSinkRevision
) extends TerminalEffectReceipt

final case class EffectOutboxRevision(value: NonNegLong)

/** Highest contiguous generation for which every one of the 18 commands has a verified terminal receipt. Both fields are absent before the
  * first completion.
  */
final case class EffectOutboxCursor(
  completedThrough: Option[ReleaseGeneration],
  completedManifest: Option[EffectManifestPointer]
)

/** Mutable outbox scan cursor.
  *
  * Its canonical codec must use a checksummed envelope. This value is only a rebuildable progress index: immutable manifests plus
  * per-command receipts are the authority for whether work remains. It contains no pending-work vector and cannot advance across a
  * generation with a missing receipt.
  */
final case class FinalityEffectOutboxHead(
  revision: EffectOutboxRevision,
  cursor: EffectOutboxCursor
)
