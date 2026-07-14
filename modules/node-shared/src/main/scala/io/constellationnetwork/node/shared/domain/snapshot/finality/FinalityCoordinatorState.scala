package io.constellationnetwork.node.shared.domain.snapshot.finality

import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptActivePublication

/** Content-addressed coordinator audit-chain identity. */
final case class AuditRecordId(value: Hash)
final case class AuditRecordDigest(value: Hash)
final case class AuditPointer(id: AuditRecordId, digest: AuditRecordDigest)

final case class RecoveryRecordId(value: Hash)
final case class RecoveryRecordDigest(value: Hash)
final case class RecoveryRecordPointer(id: RecoveryRecordId, digest: RecoveryRecordDigest)

/** Closed local coordinator mutations recorded in the immutable audit chain. None of these values is portable fork-choice or finality
  * evidence.
  */
sealed trait CoordinatorMutationKind extends Product with Serializable

object CoordinatorMutationKind {
  case object Initialized extends CoordinatorMutationKind
  case object Prepared extends CoordinatorMutationKind
  case object CoreApplied extends CoordinatorMutationKind
  case object RestorationStarted extends CoordinatorMutationKind
  case object RestoredAbandoned extends CoordinatorMutationKind
  case object AbandonedRetired extends CoordinatorMutationKind
  case object Released extends CoordinatorMutationKind
  case object RecoveryEntered extends CoordinatorMutationKind
}

/** Objective reason that normal coordinator mutation must stop.
  *
  * The reason is retained in an immutable recovery record rather than embedded in the mutable coordinator head. None of these variants
  * selects a winning branch.
  */
sealed trait RecoveryReason extends Product with Serializable

object RecoveryReason {
  final case class UnknownCanonicality(target: GlobalSnapshotStateRef, missingHash: Hash) extends RecoveryReason

  final case class MissingCoreBatch(batch: FinalityCoreBatchPointer) extends RecoveryReason

  final case class CorruptCoreBatch(batch: FinalityCoreBatchPointer, observedDigest: Hash) extends RecoveryReason

  final case class CoreConflict(expected: Option[ReleasedCorePointer], actual: Option[ReleasedCorePointer]) extends RecoveryReason

  final case class UnknownCore(reasonDigest: Hash) extends RecoveryReason

  final case class MissingEffectManifest(manifest: EffectManifestPointer) extends RecoveryReason

  final case class CorruptEffectManifest(manifest: EffectManifestPointer, observedDigest: Hash) extends RecoveryReason

  final case class EffectConflict(kind: EffectKind, expected: EffectStateDigest, actual: EffectStateDigest) extends RecoveryReason

  final case class UnknownEffect(kind: EffectKind, reasonDigest: Hash) extends RecoveryReason

  final case class JournalCorruption(observedDigest: Hash) extends RecoveryReason

  /** Local startup verified the coordinator journal and audit chain, but could not verify every dependency needed to expose the persisted
    * Running head. The digest commits to a canonically framed local diagnostic; it is not portable fork-choice evidence and cannot select a
    * branch.
    */
  final case class StartupDependencyFailure(reasonDigest: Hash) extends RecoveryReason

  /** The exact verified MPT publication observed at startup differed from the coordinator's frozen `RecoveryRecord.publication`.
    * `reasonDigest` is derived from both complete canonical Scodec publications under a dedicated domain.
    */
  final case class PublicationMismatch(observed: MptActivePublication, reasonDigest: Hash) extends RecoveryReason
}

/** Immutable, content-addressed detail for a fail-closed coordinator stop. */
final case class RecoveryRecord(
  enteredAt: HeadRevision,
  lastAttempt: Option[IntentAttempt],
  released: Option[ReleasedCore],
  active: Option[ActiveCoreIntent],
  publication: MptActivePublication,
  effects: EffectsIndex,
  reason: RecoveryReason,
  priorAudit: Option[AuditPointer]
)

sealed trait CoordinatorMode extends Product with Serializable

object CoordinatorMode {
  case object Running extends CoordinatorMode

  /** Absorbing for ordinary coordinator transitions. Recovery exit requires a separate verified reconstruction operation, not a normal head
    * mutation.
    */
  final case class RecoveryRequired(record: RecoveryRecordPointer) extends CoordinatorMode
}

/** Constant-size index into immutable effect manifests. Mutable receipt progress lives exclusively in the independently checksummed
  * [[FinalityEffectOutboxHead]], so effect delivery cannot occupy or block the coordinator CAS used for urgent core rollback. Pending work
  * is never embedded in the coordinator head.
  */
final case class EffectsIndex(
  tail: Option[EffectManifestPointer],
  highGeneration: Option[ReleaseGeneration]
)

/** Mutable coordinator journal head.
  *
  * `publication` is the exact effective MPT CAS cursor. It is independent of the immutable last released record because restoring an
  * unreleased applied target republishes the prior image at a newer revision.
  *
  * Successful release is permitted only from `CoreStage.CoreApplied`; it clears `active`, installs `released`, and advances
  * `effects.tail/highGeneration`. Restoration or abandonment never changes `released`. `RecoveryRequired` is absorbing for ordinary
  * transitions. The pure validator and durable store CAS enforce these structural rules. CoreApplied, Released, and restoration authority
  * remain disabled until a future package-owned runtime can prove the required exact state/effect readbacks.
  */
final case class CoordinatorHead(
  revision: HeadRevision,
  lastAttempt: Option[IntentAttempt],
  mode: CoordinatorMode,
  released: Option[ReleasedCore],
  active: Option[ActiveCoreIntent],
  publication: MptActivePublication,
  effects: EffectsIndex,
  auditTail: Option[AuditPointer]
) {
  def commitment: CoordinatorHeadCommitment =
    CoordinatorHeadCommitment(revision, lastAttempt, mode, released, active, publication, effects)
}

/** Exact mutable-head content excluding its audit pointer.
  *
  * The exclusion is deliberate: the immutable audit record commits to this value, and the resulting audit pointer is then installed in the
  * mutable head. Including `auditTail` here would create a content-address cycle.
  */
final case class CoordinatorHeadCommitment(
  revision: HeadRevision,
  lastAttempt: Option[IntentAttempt],
  mode: CoordinatorMode,
  released: Option[ReleasedCore],
  active: Option[ActiveCoreIntent],
  publication: MptActivePublication,
  effects: EffectsIndex
)

/** Immutable append-only explanation of one coordinator-head replacement.
  *
  * `priorAudit` must equal the replaced head's `auditTail`; the new head must equal `after` and point its `auditTail` at this record. The
  * pure transition validator owns those equalities.
  */
final case class CoordinatorAuditRecord(
  mutation: CoordinatorMutationKind,
  before: Option[CoordinatorHeadCommitment],
  after: CoordinatorHeadCommitment,
  priorAudit: Option[AuditPointer]
)
