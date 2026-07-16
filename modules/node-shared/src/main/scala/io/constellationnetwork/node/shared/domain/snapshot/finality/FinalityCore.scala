package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.data.NonEmptyList

import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.{MptActivePublication, MptImageReceipt}

import eu.timepit.refined.types.numeric.NonNegLong

/** A release generation advances only when a new exact Phase-2 core is released. Retries of the same prospective release use distinct
  * [[IntentAttempt]] values.
  */
final case class ReleaseGeneration(value: NonNegLong)

/** Monotone local attempt number. It is deliberately independent of release generation. */
final case class IntentAttempt(value: NonNegLong)

/** Compare-and-set revision of the local coordinator head. */
final case class HeadRevision(value: NonNegLong)

/** Local fork-choice mutation token.
  *
  * This is not consensus evidence and must never be transported as proof that a branch won. The future live fork-choice integration must
  * compare-and-set this exact revision immediately before release; merely recording the token does not prevent a stale worker from
  * releasing after canonical selection changes.
  */
final case class CanonicalBranchRevision(value: NonNegLong)

/** Local canonical-lineage generation.
  *
  * Unlike [[CanonicalBranchRevision]], a pure descendant extension does not advance this value. Rollback, replacement, reconstruction, or
  * clearing does. It is a local crash-consistent CAS input in the target design, not transported consensus evidence.
  */
final case class CanonicalLineageRevision(value: NonNegLong)

/** Domain-separated hash of the canonical [[IntentScope]] bytes.
  *
  * The scope contains only unscoped immutable commitments. Artifacts may therefore bind this identifier without making its preimage
  * circular.
  */
final case class IntentId(value: Hash)

final case class ArtifactId(value: Hash)
final case class ArtifactDigest(value: Hash)
final case class ArtifactEncoding(value: Hash)

sealed trait FinalityArtifactKind extends Product with Serializable

object FinalityArtifactKind {
  case object CoreBatch extends FinalityArtifactKind
  case object ReleasedCoreRecord extends FinalityArtifactKind
  case object PathManifest extends FinalityArtifactKind
  case object PathChunk extends FinalityArtifactKind
  case object DecidedAttestationEvidence extends FinalityArtifactKind
  case object DepthK1Evidence extends FinalityArtifactKind
  case object ForkChoiceDecisionEvidence extends FinalityArtifactKind
  case object PreparedSemanticState extends FinalityArtifactKind
  case object AuthenticatedTargetAnchor extends FinalityArtifactKind
  case object AppliedSemanticStateReceipt extends FinalityArtifactKind
  case object AuthenticatedAnchorReceipt extends FinalityArtifactKind
  case object PriorSemanticStateReceipt extends FinalityArtifactKind
  case object PriorAnchorReceipt extends FinalityArtifactKind
  case object EffectPayload extends FinalityArtifactKind

  val all: List[FinalityArtifactKind] = List(
    CoreBatch,
    ReleasedCoreRecord,
    PathManifest,
    PathChunk,
    DecidedAttestationEvidence,
    DepthK1Evidence,
    ForkChoiceDecisionEvidence,
    PreparedSemanticState,
    AuthenticatedTargetAnchor,
    AppliedSemanticStateReceipt,
    AuthenticatedAnchorReceipt,
    PriorSemanticStateReceipt,
    PriorAnchorReceipt,
    EffectPayload
  )
}

/** Immutable content-addressed pointer. The pointed-to bytes are verified against both `id` and `digest` before they are interpreted.
  */
final case class ImmutableArtifactPointer(
  kind: FinalityArtifactKind,
  encoding: ArtifactEncoding,
  id: ArtifactId,
  digest: ArtifactDigest,
  byteLength: NonNegLong
)

/** An immutable artifact whose canonical payload is valid for exactly one intent. */
final case class ScopedArtifactRef(intentId: IntentId, artifact: ImmutableArtifactPointer)

sealed trait PathRole extends Product with Serializable

object PathRole {
  case object Adopted extends PathRole
  case object Orphaned extends PathRole
  case object OperationalAncestorClosure extends PathRole
  case object CanonicalLineage extends PathRole
}

/** Metadata for an oldest-to-newest contiguous snapshot path.
  *
  * `entryCount` has no protocol history-window ceiling. The actual path is held in independently bounded chunks so a deep objective reorg
  * is a recovery task, never an invalid fork-choice result.
  */
final case class PathSummary(
  role: PathRole,
  oldest: GlobalSnapshotStateRef,
  newest: GlobalSnapshotStateRef,
  entryCount: NonNegLong
)

/** Scope-independent commitment to the complete content-addressed path. */
final case class PathCommitment(
  summary: PathSummary,
  manifest: ImmutableArtifactPointer,
  entriesRoot: Hash
) {
  def payload: PathManifestPayload = PathManifestPayload(summary, entriesRoot)
}

/** Canonical payload addressed by a `PathManifest` artifact.
  *
  * `entriesRoot` commits to the canonical oldest-to-newest state-reference sequence, not to its local chunk framing. This keeps the
  * manifest identity independent of chunk pointers which themselves bind the manifest ID, avoiding a content-address cycle. Recovery
  * locates chunk zero by `(intentId, manifest.id, 0)`, follows contiguous `next` pointers through the unique terminal `None`, and requires
  * exactly `summary.entryCount` entries.
  */
final case class PathManifestPayload(
  summary: PathSummary,
  entriesRoot: Hash
)

/** Intent-bound manifest for a path. No complete path is embedded here. */
final case class PathManifestRef(commitment: PathCommitment, manifest: ScopedArtifactRef) {
  def summary: PathSummary = commitment.summary
}

/** The portable evidence commitment for one claimed fork-choice result.
  *
  * The evidence payload remains opaque until O-15 defines the objective frontier selector and its verifier. Merely decoding this wrapper
  * does not establish canonicality or identify a pairwise maxvalid rule.
  */
final case class ForkChoiceDecision(evidence: ImmutableArtifactPointer)

/** The local selection observed by the worker which prepared an intent.
  *
  * `branchRevision` is a local compare-and-set guard, not portable finality proof. The decision and complete lineage commitment are part of
  * the intent preimage, preventing a worker from substituting different branch evidence under the same intent identifier.
  */
final case class CanonicalSelectionToken(
  branchRevision: CanonicalBranchRevision,
  selectedTip: GlobalSnapshotStateRef,
  operationalTarget: GlobalSnapshotStateRef,
  decision: ForkChoiceDecision,
  lineage: PathCommitment
)

/** Exact pointer to one bounded chunk in a path manifest. */
final case class PathChunkPointer(
  intentId: IntentId,
  manifestId: ArtifactId,
  chunkIndex: NonNegLong,
  artifact: ImmutableArtifactPointer
)

/** One oldest-to-newest piece of a manifest path.
  *
  * Construction is intentionally public for canonical decoding. The pure validator must reject empty chunks and chunks larger than
  * [[MaxEntries]].
  */
final case class PathChunk(
  intentId: IntentId,
  manifestId: ArtifactId,
  chunkIndex: NonNegLong,
  entriesOldestFirst: NonEmptyList[GlobalSnapshotStateRef],
  next: Option[PathChunkPointer]
)

object PathChunk {
  val MaxEntries: Int = 256
}

sealed trait OperationalRail extends Product with Serializable

object OperationalRail {
  case object DecidedAttestationTWeight extends OperationalRail
  case object CanonicalDepthK1 extends OperationalRail
}

/** Scope-independent qualification identity included in [[IntentScope]].
  *
  * `ancestorClosure = None` is a direct qualification and requires the target to equal the qualifying snapshot. `Some(path)` proves that
  * the operational target is an ancestor of the qualifying descendant on the selected tine.
  */
final case class OperationalQualificationScope(
  rail: OperationalRail,
  operationalTarget: GlobalSnapshotStateRef,
  qualifyingDescendant: GlobalSnapshotStateRef,
  ancestorClosure: Option[PathCommitment],
  evidence: ImmutableArtifactPointer
)

/** Exactly the two ratified Phase-2 qualification rails.
  *
  * The evidence need not be freshly produced for the current transition. During fork-choice rollback, an MRCA retains operationality
  * through the original rail's evidence plus `ancestorClosure`, even when the qualifying descendant is now on the orphaned suffix. This is
  * inheritance of one of these two rails, not a third finality source.
  */
sealed trait OperationalQualification extends Product with Serializable {
  def operationalTarget: GlobalSnapshotStateRef
  def qualifyingDescendant: GlobalSnapshotStateRef
  def ancestorClosure: Option[PathManifestRef]
  def evidence: ScopedArtifactRef
  def scope: OperationalQualificationScope
}

object OperationalQualification {
  final case class DecidedAttestationTWeight(
    operationalTarget: GlobalSnapshotStateRef,
    qualifyingDescendant: GlobalSnapshotStateRef,
    ancestorClosure: Option[PathManifestRef],
    evidence: ScopedArtifactRef
  ) extends OperationalQualification {
    val scope: OperationalQualificationScope =
      OperationalQualificationScope(
        OperationalRail.DecidedAttestationTWeight,
        operationalTarget,
        qualifyingDescendant,
        ancestorClosure.map(_.commitment),
        evidence.artifact
      )
  }

  final case class CanonicalDepthK1(
    operationalTarget: GlobalSnapshotStateRef,
    qualifyingDescendant: GlobalSnapshotStateRef,
    ancestorClosure: Option[PathManifestRef],
    evidence: ScopedArtifactRef
  ) extends OperationalQualification {
    val scope: OperationalQualificationScope =
      OperationalQualificationScope(
        OperationalRail.CanonicalDepthK1,
        operationalTarget,
        qualifyingDescendant,
        ancestorClosure.map(_.commitment),
        evidence.artifact
      )
  }
}

/** Scope-independent transition identity included in [[IntentScope]]. */
sealed trait TransitionShape extends Product with Serializable

object TransitionShape {
  final case class Advance(adopted: PathCommitment) extends TransitionShape

  final case class ForkChoiceReplacement(
    commonAncestor: GlobalSnapshotStateRef,
    orphaned: PathCommitment,
    adopted: PathCommitment
  ) extends TransitionShape

  /** Roll back to an already operational MRCA while the replacement suffix is still provisional. There is deliberately no adopted suffix in
    * this shape.
    */
  final case class ForkChoiceRollbackToOperationalMrca(
    operationalMrca: GlobalSnapshotStateRef,
    orphaned: PathCommitment
  ) extends TransitionShape
}

/** The exact immutable identity of a previously released core. */
final case class ReleasedCorePointer(
  generation: ReleaseGeneration,
  intentId: IntentId,
  target: GlobalSnapshotStateRef,
  record: ImmutableArtifactPointer
)

/** Domain separation for local intent identifiers.
  *
  * `parameterHash` names the exact consensus-parameter object in force for the artifact. A receiver's local configuration is never a
  * substitute for this committed identity.
  */
final case class FinalityDomain(networkId: Hash, genesisHash: Hash, protocolEra: Hash, parameterHash: Hash)

/** Scope hashed to form [[IntentId]].
  *
  * All referenced commitments are scope-independent immutable pointers. No [[ScopedArtifactRef]] appears here, so deriving `IntentId` is
  * non-circular.
  */
final case class IntentScope(
  domain: FinalityDomain,
  generation: ReleaseGeneration,
  attempt: IntentAttempt,
  expectedPrior: Option[ReleasedCorePointer],
  selection: CanonicalSelectionToken,
  transition: TransitionShape,
  qualification: OperationalQualificationScope,
  prepared: PreparedCoreCommitment,
  effects: EffectPlanCommitment,
  target: GlobalSnapshotStateRef
)

/** Exactly the three core transition forms. */
sealed trait CoreTransition extends Product with Serializable {
  def shape: TransitionShape
}

object CoreTransition {
  final case class Advance(adopted: PathManifestRef) extends CoreTransition {
    val shape: TransitionShape = TransitionShape.Advance(adopted.commitment)
  }

  final case class ForkChoiceReplacement(
    commonAncestor: GlobalSnapshotStateRef,
    orphaned: PathManifestRef,
    adopted: PathManifestRef
  ) extends CoreTransition {
    val shape: TransitionShape =
      TransitionShape.ForkChoiceReplacement(commonAncestor, orphaned.commitment, adopted.commitment)
  }

  /** Objective fork choice moved Phase 2 back to the still-operational MRCA. A replacement suffix may later advance through a separate
    * intent.
    */
  final case class ForkChoiceRollbackToOperationalMrca(
    operationalMrca: GlobalSnapshotStateRef,
    orphaned: PathManifestRef
  ) extends CoreTransition {
    val shape: TransitionShape =
      TransitionShape.ForkChoiceRollbackToOperationalMrca(operationalMrca, orphaned.commitment)
  }
}

/** Prepared exact target core. MPT publication revision is independent of the release generation and may not be inferred from it.
  */
final case class PreparedCoreTarget(
  target: GlobalSnapshotStateRef,
  preparedImage: MptImageReceipt,
  expectedBefore: MptActivePublication,
  targetPublication: MptActivePublication,
  semanticState: ScopedArtifactRef,
  authenticatedAnchor: ScopedArtifactRef
) {
  def commitment: PreparedCoreCommitment =
    PreparedCoreCommitment(
      target,
      preparedImage,
      expectedBefore,
      targetPublication,
      semanticState.artifact,
      authenticatedAnchor.artifact
    )
}

/** Scope-independent commitment to every load-bearing prepared-core claim. */
final case class PreparedCoreCommitment(
  target: GlobalSnapshotStateRef,
  preparedImage: MptImageReceipt,
  expectedBefore: MptActivePublication,
  targetPublication: MptActivePublication,
  semanticState: ImmutableArtifactPointer,
  authenticatedAnchor: ImmutableArtifactPointer
)

/** Immutable pointer to a complete finality core batch. */
final case class FinalityCoreBatchPointer(
  intentId: IntentId,
  attempt: IntentAttempt,
  generation: ReleaseGeneration,
  artifact: ImmutableArtifactPointer
)

/** Durable, scope-bound work needed to apply one already-decided finality change. This object never samples, votes, selects a branch, or
  * decides finality.
  */
final case class FinalityCoreBatch(
  intentId: IntentId,
  scope: IntentScope,
  selectionEvidence: ScopedArtifactRef,
  transition: CoreTransition,
  qualification: OperationalQualification,
  prepared: PreparedCoreTarget,
  effectManifest: EffectManifestPointer
)

/** Verified result of the core mutation. Both semantic and anchor readback are required in addition to the exact active MPT publication.
  */
final case class ReleasedCoreReceipt(
  intentId: IntentId,
  generation: ReleaseGeneration,
  target: GlobalSnapshotStateRef,
  beforePublication: MptActivePublication,
  activePublication: MptActivePublication,
  semanticReceipt: ScopedArtifactRef,
  authenticatedAnchorReceipt: ScopedArtifactRef
)

/** Exact released-record payload hashed independently of its pointer.
  *
  * Keeping the pointer outside this payload makes content addressing non-circular. `ReleasedCore.pointer.record` commits to the canonical
  * bytes of this payload, while `ReleasedCore.record` is the intent-scoped wrapper around that same immutable pointer.
  */
final case class ReleasedCoreRecordPayload(
  qualification: OperationalQualification,
  receipt: ReleasedCoreReceipt,
  effectManifest: EffectManifestPointer
)

final case class ReleasedCore(
  pointer: ReleasedCorePointer,
  record: ScopedArtifactRef,
  payload: ReleasedCoreRecordPayload
)

/** Structurally bound claim that an unreleased target lost canonicality.
  *
  * This value is not authority by itself. O-15's future verifier must decode the committed fork-choice evidence and prove the exact tines,
  * true MRCA, and target exclusion before a runtime may use the claim to restore state.
  */
final case class ForkChoiceOrphanClaim(
  intentId: IntentId,
  supersededSelection: CanonicalSelectionToken,
  replacementSelection: CanonicalSelectionToken,
  commonAncestor: GlobalSnapshotStateRef,
  evidence: ScopedArtifactRef
)

/** Exact MPT publication transition selected for abandonment.
  *
  * `RestoringPrior` carries this as a plan; `RestoredAbandoned` repeats it in a receipt. The value does not prove that an external MPT
  * mutation occurred.
  */
sealed trait PublicationRestoration extends Product with Serializable

object PublicationRestoration {

  /** The target was never published, so the exact CAS prior remains active. */
  final case class PriorUnchanged(publication: MptActivePublication) extends PublicationRestoration

  /** The target was active and the prior image was republished at the next revision. Publication revisions never rewind.
    */
  final case class AppliedTargetReverted(
    transitionFrom: MptActivePublication,
    restored: MptActivePublication
  ) extends PublicationRestoration
}

/** Claimed readback locators after abandoning an unreleased target publication. A future runtime must independently verify all MPT,
  * semantic, and anchor receipts before constructing this terminal state.
  */
final case class PriorCoreRestorationReceipt(
  intentId: IntentId,
  publication: PublicationRestoration,
  semanticReceipt: ScopedArtifactRef,
  authenticatedAnchorReceipt: ScopedArtifactRef
)

/** Modeled progress of an unreleased core mutation.
  *
  * Release is a coordinator-head operation from `CoreApplied`; it is not a stage. Once restoration starts, the only legal terminal stage is
  * `RestoredAbandoned`, so a restored target can never later be released.
  */
sealed trait CoreStage extends Product with Serializable

object CoreStage {
  case object Prepared extends CoreStage
  final case class CoreApplied(receipt: ReleasedCoreReceipt) extends CoreStage
  final case class RestoringPrior(
    claim: ForkChoiceOrphanClaim,
    publication: PublicationRestoration
  ) extends CoreStage
  final case class RestoredAbandoned(claim: ForkChoiceOrphanClaim, receipt: PriorCoreRestorationReceipt) extends CoreStage

  def canAdvance(from: CoreStage, to: CoreStage): Boolean =
    (from, to) match {
      case (Prepared, _: CoreApplied)                => true
      case (Prepared, _: RestoringPrior)             => true
      case (_: CoreApplied, _: RestoringPrior)       => true
      case (_: RestoringPrior, _: RestoredAbandoned) => true
      case _                                         => false
    }
}

/** Core intent retained by the coordinator head while its mutation is incomplete. */
final case class ActiveCoreIntent(
  batch: FinalityCoreBatchPointer,
  scope: IntentScope,
  stage: CoreStage
)
