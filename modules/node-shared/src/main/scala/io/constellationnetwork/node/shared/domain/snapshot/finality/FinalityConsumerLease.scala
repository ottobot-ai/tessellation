package io.constellationnetwork.node.shared.domain.snapshot.finality

import io.constellationnetwork.node.shared.app.Layer
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptActivePublication

import eu.timepit.refined.types.numeric.NonNegLong

/** Closed registry implemented by the dark exact-ancestor slice.
  *
  * This ADT deliberately has no generic/string escape hatch and no wire codec. It is not the activation-complete O-16B registry:
  * current-Phase-2-head `/latest` serving is deliberately absent until that separate policy is implemented and tested. `ExactServing`
  * means an exact-reference request and must never be used for `/latest`. Adding a consumer requires adding a distinct case and updating
  * the exhaustive policy match in [[Phase2ReferencePolicy.forScope]].
  */
sealed trait Phase2UseScope extends Product with Serializable

object Phase2UseScope {
  final case class BinaryAdmission(metagraphId: Address, parent: Hash, binary: Hash) extends Phase2UseScope
  final case class BinaryConfirmationRequeue(metagraphId: Address, binary: Hash) extends Phase2UseScope
  final case class ShardExecutionBase(shardId: ShardId, checkpoint: Hash) extends Phase2UseScope
  final case class CheckpointInclusion(shardId: ShardId, checkpoint: Hash) extends Phase2UseScope
  final case class CheckpointAnchorAdvancement(shardId: ShardId, checkpoint: Hash) extends Phase2UseScope
  final case class AssignedWatchtowerReplay(shardId: ShardId, checkpoint: Hash) extends Phase2UseScope
  final case class ChallengeAdjudication(challenge: Hash) extends Phase2UseScope
  final case class CrossMetagraphSettlement(operationId: Hash) extends Phase2UseScope
  final case class HistoricalEconomicRead(operationId: Hash) extends Phase2UseScope
  final case class OptimisticSamplerRegistryContext(round: Hash) extends Phase2UseScope
  final case class TowerEligibilityRegistryContext(trial: Hash) extends Phase2UseScope
  final case class TowerProofServing(requestId: Hash) extends Phase2UseScope
  final case class ProtocolCorrection(correctionId: Hash) extends Phase2UseScope
  final case class FollowerAdoption(layer: Layer) extends Phase2UseScope
  final case class ExactServing(requestId: Hash) extends Phase2UseScope
  final case class BootstrapBundleServing(requestId: Hash) extends Phase2UseScope
  final case class RetentionRecovery(operationId: Hash) extends Phase2UseScope
  final case class DownstreamEventDelivery(eventId: Hash) extends Phase2UseScope
}

/** Local reference-use policy. This dark first slice has only exact, still-canonical Phase-2 ancestors.
  *
  * In particular there is no latest-head substitution policy: a new head or same-ordinal replacement cannot satisfy a capability captured
  * for another exact reference. This does not classify or activate `/latest` serving; that O-16B policy remains an explicit activation
  * gate rather than being guessed by this kernel.
  */
sealed trait Phase2ReferencePolicy extends Product with Serializable

object Phase2ReferencePolicy {
  case object ExactCanonicalAncestor extends Phase2ReferencePolicy

  /** Compiler-visible exhaustive policy assignment. Do not replace this with a default branch. */
  def forScope(scope: Phase2UseScope): Phase2ReferencePolicy =
    scope match {
      case _: Phase2UseScope.BinaryAdmission                   => ExactCanonicalAncestor
      case _: Phase2UseScope.BinaryConfirmationRequeue         => ExactCanonicalAncestor
      case _: Phase2UseScope.ShardExecutionBase                => ExactCanonicalAncestor
      case _: Phase2UseScope.CheckpointInclusion               => ExactCanonicalAncestor
      case _: Phase2UseScope.CheckpointAnchorAdvancement       => ExactCanonicalAncestor
      case _: Phase2UseScope.AssignedWatchtowerReplay          => ExactCanonicalAncestor
      case _: Phase2UseScope.ChallengeAdjudication             => ExactCanonicalAncestor
      case _: Phase2UseScope.CrossMetagraphSettlement          => ExactCanonicalAncestor
      case _: Phase2UseScope.HistoricalEconomicRead            => ExactCanonicalAncestor
      case _: Phase2UseScope.OptimisticSamplerRegistryContext  => ExactCanonicalAncestor
      case _: Phase2UseScope.TowerEligibilityRegistryContext   => ExactCanonicalAncestor
      case _: Phase2UseScope.TowerProofServing                 => ExactCanonicalAncestor
      case _: Phase2UseScope.ProtocolCorrection                => ExactCanonicalAncestor
      case _: Phase2UseScope.FollowerAdoption                  => ExactCanonicalAncestor
      case _: Phase2UseScope.ExactServing                      => ExactCanonicalAncestor
      case _: Phase2UseScope.BootstrapBundleServing            => ExactCanonicalAncestor
      case _: Phase2UseScope.RetentionRecovery                 => ExactCanonicalAncestor
      case _: Phase2UseScope.DownstreamEventDelivery           => ExactCanonicalAncestor
    }
}

private[finality] final case class Phase2ConsumerSinkRevision(value: NonNegLong)

private[finality] final case class Phase2AppliedCommand(
  commandId: Hash,
  payloadDigest: Hash,
  scope: Phase2UseScope,
  target: GlobalSnapshotStateRef,
  releaseGeneration: ReleaseGeneration,
  lineageRevision: CanonicalLineageRevision
)

private[finality] final case class Phase2ConsumerSink(
  revision: Phase2ConsumerSinkRevision,
  applied: Map[Hash, Phase2AppliedCommand]
)

private[finality] object Phase2ConsumerSink {
  val empty: Phase2ConsumerSink = Phase2ConsumerSink(Phase2ConsumerSinkRevision(NonNegLong.MinValue), Map.empty)
}

/** Immutable coordinator view consumed by the dark pure kernel.
  *
  * This is not finality evidence. A future runtime must construct it under the finality coordinator's serialization boundary from a
  * durably verified head, exact canonical lineage, and retained released records. `currentSelectionByTarget` is deliberately target
  * indexed: each retained operational ancestor binds the current decision and exact target-to-selected-tip lineage. A singleton token
  * would incorrectly make only the newest operational target usable. No live runtime constructs this view today.
  */
private[finality] final case class FinalityConsumerLeaseState(
  mode: CoordinatorMode,
  branchRevision: CanonicalBranchRevision,
  lineageRevision: CanonicalLineageRevision,
  canonicalOldestFirst: Vector[GlobalSnapshotStateRef],
  currentSelectionByTarget: Map[GlobalSnapshotStateRef, CanonicalSelectionToken],
  releasedByTarget: Map[GlobalSnapshotStateRef, ReleasedCore],
  sink: Phase2ConsumerSink
)

/** Exact local readbacks produced outside the coordinator lock.
  *
  * The values are existing released-core artifacts, not new portable evidence. `None` means the independently addressed local artifact or
  * readback was unavailable; it never means verification succeeded.
  */
private[finality] final case class Phase2LocalReadbacks(
  released: Option[ReleasedCore],
  qualification: Option[OperationalQualificationScope],
  qualificationEvidence: Option[ImmutableArtifactPointer],
  decisionEvidence: Option[ImmutableArtifactPointer],
  lineage: Option[PathCommitment],
  publication: Option[MptActivePublication],
  semanticReceipt: Option[ScopedArtifactRef],
  authenticatedAnchorReceipt: Option[ScopedArtifactRef]
)

private[finality] sealed trait Phase2EvidenceComponent extends Product with Serializable

private[finality] object Phase2EvidenceComponent {
  case object ReleasedCoreRecord extends Phase2EvidenceComponent
  case object Qualification extends Phase2EvidenceComponent
  case object QualificationEvidence extends Phase2EvidenceComponent
  case object DecisionEvidence extends Phase2EvidenceComponent
  case object CanonicalLineage extends Phase2EvidenceComponent
  case object ActiveMptPublication extends Phase2EvidenceComponent
  case object SemanticReceipt extends Phase2EvidenceComponent
  case object AuthenticatedAnchorReceipt extends Phase2EvidenceComponent
}

private[finality] sealed trait Phase2CanonicalLineageDefect extends Product with Serializable

private[finality] object Phase2CanonicalLineageDefect {
  case object Empty extends Phase2CanonicalLineageDefect
  final case class DuplicateHash(hash: Hash) extends Phase2CanonicalLineageDefect
  final case class DuplicateOrdinal(left: GlobalSnapshotStateRef, right: GlobalSnapshotStateRef)
      extends Phase2CanonicalLineageDefect
  final case class NonConsecutiveOrdinal(parent: GlobalSnapshotStateRef, child: GlobalSnapshotStateRef)
      extends Phase2CanonicalLineageDefect
  final case class OrdinalExhausted(parent: GlobalSnapshotStateRef) extends Phase2CanonicalLineageDefect
  final case class BrokenParent(parent: GlobalSnapshotStateRef, child: GlobalSnapshotStateRef)
      extends Phase2CanonicalLineageDefect
  final case class InvalidReference(reference: GlobalSnapshotStateRef) extends Phase2CanonicalLineageDefect
}

private[finality] sealed trait Phase2CommandIdentityField extends Product with Serializable

private[finality] object Phase2CommandIdentityField {
  case object CommandId extends Phase2CommandIdentityField
  case object PayloadDigest extends Phase2CommandIdentityField
}

private[finality] sealed trait FinalityConsumerLeaseFailure extends Product with Serializable
private[finality] sealed trait Phase2StaleFailure extends FinalityConsumerLeaseFailure
private[finality] sealed trait Phase2ReplacementFailure extends FinalityConsumerLeaseFailure
private[finality] sealed trait Phase2MissingFailure extends FinalityConsumerLeaseFailure
private[finality] sealed trait Phase2WrongPurposeFailure extends FinalityConsumerLeaseFailure
private[finality] sealed trait Phase2EvidenceFailure extends FinalityConsumerLeaseFailure

private[finality] object FinalityConsumerLeaseFailure {
  final case class RecoveryRequired(mode: CoordinatorMode.RecoveryRequired) extends FinalityConsumerLeaseFailure

  final case class ReferenceMissing(target: GlobalSnapshotStateRef) extends Phase2MissingFailure
  final case class ReleasedCoreMissing(target: GlobalSnapshotStateRef) extends Phase2MissingFailure
  final case class EvidenceMissing(target: GlobalSnapshotStateRef, component: Phase2EvidenceComponent)
      extends Phase2MissingFailure
      with Phase2EvidenceFailure

  final case class ReferenceReplaced(target: GlobalSnapshotStateRef, replacement: GlobalSnapshotStateRef)
      extends Phase2ReplacementFailure
  final case class LineageReplaced(expected: CanonicalLineageRevision, observed: CanonicalLineageRevision)
      extends Phase2ReplacementFailure

  final case class AcquisitionStale(
    expectedBranch: CanonicalBranchRevision,
    observedBranch: CanonicalBranchRevision,
    expectedSink: Phase2ConsumerSinkRevision,
    observedSink: Phase2ConsumerSinkRevision
  ) extends Phase2StaleFailure
  final case class BranchRevisionRegressed(expectedAtLeast: CanonicalBranchRevision, observed: CanonicalBranchRevision)
      extends Phase2StaleFailure
  final case class SinkRevisionStale(expected: Phase2ConsumerSinkRevision, observed: Phase2ConsumerSinkRevision)
      extends Phase2StaleFailure

  final case class WrongPurpose(expected: Phase2UseScope, observed: Phase2UseScope) extends Phase2WrongPurposeFailure
  final case class WrongTarget(expected: GlobalSnapshotStateRef, observed: GlobalSnapshotStateRef)
      extends Phase2WrongPurposeFailure

  final case class EvidenceMismatch(target: GlobalSnapshotStateRef, component: Phase2EvidenceComponent)
      extends Phase2EvidenceFailure
  final case class ReleasedCoreChanged(expected: ReleasedCorePointer, observed: Option[ReleasedCorePointer])
      extends Phase2EvidenceFailure
  final case class MalformedCanonicalLineage(defect: Phase2CanonicalLineageDefect) extends Phase2EvidenceFailure
  final case class LineageIdentityFailed(error: FinalityIdentityError) extends Phase2EvidenceFailure

  final case class InvalidCommandIdentity(field: Phase2CommandIdentityField) extends FinalityConsumerLeaseFailure

  final case class CommandIdCollision(existing: Phase2AppliedCommand, attempted: Phase2AppliedCommand)
      extends FinalityConsumerLeaseFailure
  final case class RevisionExhausted(revision: Phase2ConsumerSinkRevision) extends FinalityConsumerLeaseFailure
}

/** First short-boundary capability. It is local, non-case, non-serializable, and has no public constructor. */
private[finality] final class Phase2Acquisition private[finality] (
  private[finality] val scope: Phase2UseScope,
  private[finality] val policy: Phase2ReferencePolicy,
  private[finality] val target: GlobalSnapshotStateRef,
  private[finality] val released: ReleasedCore,
  private[finality] val selection: CanonicalSelectionToken,
  private[finality] val branchRevision: CanonicalBranchRevision,
  private[finality] val lineageRevision: CanonicalLineageRevision,
  private[finality] val sinkRevision: Phase2ConsumerSinkRevision
)

/** Package-owned proof that every supplied exact local readback identity matched one acquisition descriptor.
  *
  * It is intentionally not a case class and has no codec. It does not prove the artifact bytes were authenticated or semantically
  * reproduced; that package-owned verifier is an activation gate. It is not portable finality evidence and cannot be constructed from a
  * Boolean.
  */
private[finality] final class VerifiedPhase2Acquisition private[finality] (
  private[finality] val acquisition: Phase2Acquisition
)

/** Current-use capability minted only after the second short acquisition CAS. */
private[finality] final class CanonicalPhase2Lease private[finality] (
  private[finality] val scope: Phase2UseScope,
  private[finality] val policy: Phase2ReferencePolicy,
  private[finality] val target: GlobalSnapshotStateRef,
  private[finality] val released: ReleasedCore,
  private[finality] val observedSelection: CanonicalSelectionToken,
  private[finality] val observedBranchRevision: CanonicalBranchRevision,
  private[finality] val lineageRevision: CanonicalLineageRevision,
  private[finality] val sinkRevision: Phase2ConsumerSinkRevision
)

private[finality] final case class Phase2ClosedCommand(
  commandId: Hash,
  payloadDigest: Hash,
  scope: Phase2UseScope,
  target: GlobalSnapshotStateRef
)

private[finality] sealed trait Phase2CommitResult extends Product with Serializable

private[finality] object Phase2CommitResult {
  final case class Committed(state: FinalityConsumerLeaseState, command: Phase2AppliedCommand) extends Phase2CommitResult
  final case class AlreadyCommitted(state: FinalityConsumerLeaseState, command: Phase2AppliedCommand) extends Phase2CommitResult
  final case class Stale(state: FinalityConsumerLeaseState, failure: FinalityConsumerLeaseFailure) extends Phase2CommitResult
  final case class RecoveryRequired(state: FinalityConsumerLeaseState, failure: FinalityConsumerLeaseFailure)
      extends Phase2CommitResult
}

/** Dark, pure O-16 race kernel.
  *
  * It neither selects a branch nor decides Phase 2, verifies portable evidence, publishes an MPT, signs anything, or mutates a live sink.
  * No `FinalityGate`, checkpoint, tracker, service, or consensus path calls it. Activation requires the missing evidence/readback verifier,
  * durable consumer journal, and complete FOLLOW-008 integration gates.
  */
private[finality] object FinalityConsumerLeaseKernel {
  import FinalityConsumerLeaseFailure._
  import Phase2CommitResult._
  import Phase2EvidenceComponent._

  def capture(
    state: FinalityConsumerLeaseState,
    scope: Phase2UseScope,
    target: GlobalSnapshotStateRef
  ): Either[FinalityConsumerLeaseFailure, Phase2Acquisition] =
    for {
      _ <- running(state)
      _ <- validateCanonicalLineage(state.canonicalOldestFirst)
      released <- currentReleased(state, target)
      _ <- validateReleased(target, released)
      selection <- validateCurrentSelection(state, target)
    } yield
      new Phase2Acquisition(
        scope,
        Phase2ReferencePolicy.forScope(scope),
        target,
        released,
        selection,
        state.branchRevision,
        state.lineageRevision,
        state.sink.revision
      )

  /** Compare independently obtained exact readbacks with the captured released-core descriptor.
    *
    * This is the package-owned identity-comparison boundary, not the O-15 fork-choice/qualification artifact verifier. A future runtime may
    * call it only after authenticating and reproducing every supplied local readback. Keeping the kernel dark prevents these structural
    * comparisons from becoming authority.
    */
  def verifyReadbacks(
    acquisition: Phase2Acquisition,
    observed: Phase2LocalReadbacks
  ): Either[FinalityConsumerLeaseFailure, VerifiedPhase2Acquisition] = {
    val released = acquisition.released
    val qualification = released.payload.qualification
    val receipt = released.payload.receipt

    for {
      _ <- requireObserved(acquisition.target, ReleasedCoreRecord, observed.released, released)
      _ <- requireObserved(acquisition.target, Qualification, observed.qualification, qualification.scope)
      _ <- requireObserved(
        acquisition.target,
        QualificationEvidence,
        observed.qualificationEvidence,
        qualification.evidence.artifact
      )
      _ <- requireObserved(acquisition.target, DecisionEvidence, observed.decisionEvidence, acquisition.selection.decision.evidence)
      _ <- requireObserved(acquisition.target, CanonicalLineage, observed.lineage, acquisition.selection.lineage)
      _ <- requireObserved(acquisition.target, ActiveMptPublication, observed.publication, receipt.activePublication)
      _ <- requireObserved(acquisition.target, SemanticReceipt, observed.semanticReceipt, receipt.semanticReceipt)
      _ <- requireObserved(
        acquisition.target,
        AuthenticatedAnchorReceipt,
        observed.authenticatedAnchorReceipt,
        receipt.authenticatedAnchorReceipt
      )
    } yield new VerifiedPhase2Acquisition(acquisition)
  }

  /** Second short acquisition boundary. Every captured CAS input must still match exactly. */
  def acquireIfCurrent(
    state: FinalityConsumerLeaseState,
    verified: VerifiedPhase2Acquisition
  ): Either[FinalityConsumerLeaseFailure, CanonicalPhase2Lease] = {
    val acquisition = verified.acquisition

    for {
      _ <- running(state)
      _ <- Either.cond(
        state.lineageRevision == acquisition.lineageRevision,
        (),
        LineageReplaced(acquisition.lineageRevision, state.lineageRevision): FinalityConsumerLeaseFailure
      )
      _ <- Either.cond(
        state.branchRevision == acquisition.branchRevision && state.sink.revision == acquisition.sinkRevision,
        (),
        AcquisitionStale(
          acquisition.branchRevision,
          state.branchRevision,
          acquisition.sinkRevision,
          state.sink.revision
        ): FinalityConsumerLeaseFailure
      )
      _ <- validateCanonicalLineage(state.canonicalOldestFirst)
      released <- currentReleased(state, acquisition.target)
      selection <- validateCurrentSelection(state, acquisition.target)
      _ <- exactReleased(acquisition.released, released)
      _ <- Either.cond(
        selection == acquisition.selection,
        (),
        AcquisitionStale(
          acquisition.branchRevision,
          state.branchRevision,
          acquisition.sinkRevision,
          state.sink.revision
        ): FinalityConsumerLeaseFailure
      )
    } yield
      new CanonicalPhase2Lease(
        acquisition.scope,
        acquisition.policy,
        acquisition.target,
        acquisition.released,
        acquisition.selection,
        acquisition.branchRevision,
        acquisition.lineageRevision,
        acquisition.sinkRevision
      )
  }

  /** Final short boundary for one closed deterministic command; no caller callback executes under this operation. */
  def commitIfCurrent(
    state: FinalityConsumerLeaseState,
    lease: CanonicalPhase2Lease,
    command: Phase2ClosedCommand
  ): Phase2CommitResult = {
    val attempted = Phase2AppliedCommand(
      command.commandId,
      command.payloadDigest,
      lease.scope,
      lease.target,
      lease.released.pointer.generation,
      lease.lineageRevision
    )

    currentForCommit(state, lease, command) match {
      case Left(failure @ FinalityConsumerLeaseFailure.RecoveryRequired(_)) =>
        Phase2CommitResult.RecoveryRequired(state, failure)
      case Left(failure)                       => Stale(state, failure)
      case Right(_) =>
        state.sink.applied.get(command.commandId) match {
          case Some(existing) if existing == attempted => AlreadyCommitted(state, existing)
          case Some(existing)                           => Phase2CommitResult.RecoveryRequired(state, CommandIdCollision(existing, attempted))
          case None if state.sink.revision != lease.sinkRevision =>
            Stale(state, SinkRevisionStale(lease.sinkRevision, state.sink.revision))
          case None =>
            nextSinkRevision(state.sink.revision) match {
              case Left(failure) => Phase2CommitResult.RecoveryRequired(state, failure)
              case Right(nextRevision) =>
                val nextSink = Phase2ConsumerSink(nextRevision, state.sink.applied.updated(command.commandId, attempted))
                Committed(state.copy(sink = nextSink), attempted)
            }
        }
    }
  }

  private def currentForCommit(
    state: FinalityConsumerLeaseState,
    lease: CanonicalPhase2Lease,
    command: Phase2ClosedCommand
  ): Either[FinalityConsumerLeaseFailure, Unit] =
    for {
      _ <- running(state)
      _ <- Either.cond(
        command.scope == lease.scope,
        (),
        WrongPurpose(lease.scope, command.scope): FinalityConsumerLeaseFailure
      )
      _ <- Either.cond(
        command.target == lease.target,
        (),
        WrongTarget(lease.target, command.target): FinalityConsumerLeaseFailure
      )
      _ <- Either.cond(
        canonicalNonEmptyHash(command.commandId),
        (),
        InvalidCommandIdentity(Phase2CommandIdentityField.CommandId): FinalityConsumerLeaseFailure
      )
      _ <- Either.cond(
        canonicalNonEmptyHash(command.payloadDigest),
        (),
        InvalidCommandIdentity(Phase2CommandIdentityField.PayloadDigest): FinalityConsumerLeaseFailure
      )
      _ <- Either.cond(
        state.lineageRevision == lease.lineageRevision,
        (),
        LineageReplaced(lease.lineageRevision, state.lineageRevision): FinalityConsumerLeaseFailure
      )
      _ <- Either.cond(
        state.branchRevision.value.value >= lease.observedBranchRevision.value.value,
        (),
        BranchRevisionRegressed(lease.observedBranchRevision, state.branchRevision): FinalityConsumerLeaseFailure
      )
      _ <- validateCanonicalLineage(state.canonicalOldestFirst)
      released <- currentReleased(state, lease.target)
      selection <- validateCurrentSelection(state, lease.target)
      _ <- selectionRemainsCurrent(state, lease, selection)
      _ <- exactReleased(lease.released, released)
    } yield ()

  private def currentReleased(
    state: FinalityConsumerLeaseState,
    target: GlobalSnapshotStateRef
  ): Either[FinalityConsumerLeaseFailure, ReleasedCore] =
    if (state.canonicalOldestFirst.contains(target))
      state.releasedByTarget.get(target).toRight(ReleasedCoreMissing(target): FinalityConsumerLeaseFailure)
    else
      state.canonicalOldestFirst.find(_.ordinal == target.ordinal) match {
        case Some(replacement) => Left(ReferenceReplaced(target, replacement))
        case None              => Left(ReferenceMissing(target))
      }

  private def exactReleased(
    expected: ReleasedCore,
    observed: ReleasedCore
  ): Either[FinalityConsumerLeaseFailure, Unit] =
    for {
      _ <- validateReleased(expected.pointer.target, observed)
      _ <- Either.cond(
        observed == expected,
        (),
        ReleasedCoreChanged(expected.pointer, Some(observed.pointer)): FinalityConsumerLeaseFailure
      )
    } yield ()

  private def validateReleased(
    target: GlobalSnapshotStateRef,
    released: ReleasedCore
  ): Either[FinalityConsumerLeaseFailure, Unit] = {
    val pointer = released.pointer
    val qualification = released.payload.qualification
    val receipt = released.payload.receipt
    val expectedQualificationKind = qualification match {
      case _: OperationalQualification.DecidedAttestationTWeight => FinalityArtifactKind.DecidedAttestationEvidence
      case _: OperationalQualification.CanonicalDepthK1          => FinalityArtifactKind.DepthK1Evidence
    }

    val consistent =
      pointer.target == target &&
        receipt.target == target &&
        qualification.operationalTarget == target &&
        pointer.generation == receipt.generation &&
        released.record.intentId == pointer.intentId &&
        released.record.artifact == pointer.record &&
        qualification.evidence.intentId == pointer.intentId &&
        artifactIsValid(qualification.evidence.artifact, expectedQualificationKind) &&
        receipt.intentId == pointer.intentId &&
        receipt.semanticReceipt.intentId == pointer.intentId &&
        artifactIsValid(receipt.semanticReceipt.artifact, FinalityArtifactKind.AppliedSemanticStateReceipt) &&
        receipt.authenticatedAnchorReceipt.intentId == pointer.intentId &&
        artifactIsValid(receipt.authenticatedAnchorReceipt.artifact, FinalityArtifactKind.AuthenticatedAnchorReceipt) &&
        artifactIsValid(pointer.record, FinalityArtifactKind.ReleasedCoreRecord) &&
        receipt.activePublication.image.exists(_.anchor == target)

    Either.cond(
      consistent,
      (),
      EvidenceMismatch(target, ReleasedCoreRecord): FinalityConsumerLeaseFailure
    )
  }

  private def validateCurrentSelection(
    state: FinalityConsumerLeaseState,
    target: GlobalSnapshotStateRef
  ): Either[FinalityConsumerLeaseFailure, CanonicalSelectionToken] =
    state.currentSelectionByTarget
      .get(target)
      .toRight(EvidenceMissing(target, CanonicalLineage): FinalityConsumerLeaseFailure)
      .flatMap { selection =>
        val currentTip = state.canonicalOldestFirst.lastOption
        val targetIndex = state.canonicalOldestFirst.indexOf(target)
        val tipIndex = currentTip.fold(-1)(state.canonicalOldestFirst.indexOf)
        val selectedSlice =
          if (targetIndex >= 0 && tipIndex >= targetIndex) state.canonicalOldestFirst.slice(targetIndex, tipIndex + 1)
          else Vector.empty

        for {
          entriesRoot <- FinalityIdentity.pathEntriesRoot(selectedSlice).left.map(LineageIdentityFailed)
          _ <- Either.cond(
            selection.branchRevision == state.branchRevision &&
              currentTip.contains(selection.selectedTip) &&
              selectedSlice.nonEmpty &&
              selection.operationalTarget == target &&
              selection.lineage.summary.role == PathRole.CanonicalLineage &&
              selection.lineage.summary.oldest == target &&
              selection.lineage.summary.newest == selection.selectedTip &&
              selection.lineage.summary.entryCount.value == selectedSlice.size.toLong &&
              artifactIsValid(selection.lineage.manifest, FinalityArtifactKind.PathManifest) &&
              selection.lineage.entriesRoot == entriesRoot &&
              artifactIsValid(selection.decision.evidence, FinalityArtifactKind.ForkChoiceDecisionEvidence) &&
              stateRefIsNonEmpty(selection.selectedTip),
            (),
            EvidenceMismatch(target, CanonicalLineage): FinalityConsumerLeaseFailure
          )
        } yield selection
      }

  private def selectionRemainsCurrent(
    state: FinalityConsumerLeaseState,
    lease: CanonicalPhase2Lease,
    current: CanonicalSelectionToken
  ): Either[FinalityConsumerLeaseFailure, Unit] =
    if (state.branchRevision == lease.observedBranchRevision)
      Either.cond(
        current == lease.observedSelection,
        (),
        EvidenceMismatch(lease.target, CanonicalLineage): FinalityConsumerLeaseFailure
      )
    else {
      val targetIndex = state.canonicalOldestFirst.indexOf(lease.target)
      val observedTipIndex = state.canonicalOldestFirst.indexOf(lease.observedSelection.selectedTip)
      val observedPrefix =
        if (targetIndex >= 0 && observedTipIndex >= targetIndex)
          state.canonicalOldestFirst.slice(targetIndex, observedTipIndex + 1)
        else Vector.empty

      for {
        entriesRoot <- FinalityIdentity.pathEntriesRoot(observedPrefix).left.map(LineageIdentityFailed)
        _ <- Either.cond(
          observedPrefix.nonEmpty &&
            lease.observedSelection.operationalTarget == lease.target &&
            lease.observedSelection.lineage.summary.role == PathRole.CanonicalLineage &&
            lease.observedSelection.lineage.summary.oldest == lease.target &&
            lease.observedSelection.lineage.summary.newest == lease.observedSelection.selectedTip &&
            lease.observedSelection.lineage.summary.entryCount.value == observedPrefix.size.toLong &&
            lease.observedSelection.lineage.entriesRoot == entriesRoot,
          (),
          EvidenceMismatch(lease.target, CanonicalLineage): FinalityConsumerLeaseFailure
        )
      } yield ()
    }

  private def requireObserved[A](
    target: GlobalSnapshotStateRef,
    component: Phase2EvidenceComponent,
    observed: Option[A],
    expected: A
  ): Either[FinalityConsumerLeaseFailure, Unit] =
    observed match {
      case None => Left(EvidenceMissing(target, component))
      case Some(value) =>
        Either.cond(value == expected, (), EvidenceMismatch(target, component): FinalityConsumerLeaseFailure)
    }

  private def validateCanonicalLineage(
    canonical: Vector[GlobalSnapshotStateRef]
  ): Either[FinalityConsumerLeaseFailure, Unit] = {
    import Phase2CanonicalLineageDefect._

    val defect =
      canonical.headOption match {
        case None => Some(Empty: Phase2CanonicalLineageDefect)
        case Some(_) =>
          canonical.find(reference => !stateRefIsNonEmpty(reference)).map(InvalidReference)
            .orElse(
              canonical
                .groupBy(_.hash)
                .collectFirst {
                  case (hash, entries) if entries.sizeCompare(1) > 0 => DuplicateHash(hash): Phase2CanonicalLineageDefect
                }
                .orElse(
                  canonical.zip(canonical.drop(1)).collectFirst {
                    case (left, right) if left.ordinal == right.ordinal =>
                      DuplicateOrdinal(left, right): Phase2CanonicalLineageDefect
                    case (parent, _) if parent.ordinal.value.value == Long.MaxValue =>
                      OrdinalExhausted(parent): Phase2CanonicalLineageDefect
                    case (parent, child) if child.ordinal.value.value != parent.ordinal.value.value + 1L =>
                      NonConsecutiveOrdinal(parent, child): Phase2CanonicalLineageDefect
                    case (parent, child) if child.parentHash != parent.hash =>
                      BrokenParent(parent, child): Phase2CanonicalLineageDefect
                  }
                )
            )
      }

    defect.toLeft(()).left.map(MalformedCanonicalLineage)
  }

  private def stateRefIsNonEmpty(ref: GlobalSnapshotStateRef): Boolean =
    canonicalNonEmptyHash(ref.hash) &&
      canonicalNonEmptyHash(ref.mptRoot.value) &&
      (ref.ordinal.value.value == 0L || canonicalNonEmptyHash(ref.parentHash))

  private def artifactIsValid(pointer: ImmutableArtifactPointer, expectedKind: FinalityArtifactKind): Boolean =
    pointer.kind == expectedKind &&
      canonicalNonEmptyHash(pointer.encoding.value) &&
      canonicalNonEmptyHash(pointer.id.value) &&
      canonicalNonEmptyHash(pointer.digest.value) &&
      pointer.byteLength.value > 0L

  private def canonicalNonEmptyHash(hash: Hash): Boolean =
    hash != Hash.empty &&
      hash.value.length == 64 &&
      hash.value.forall(character => character >= '0' && character <= '9' || character >= 'a' && character <= 'f')

  private def running(state: FinalityConsumerLeaseState): Either[FinalityConsumerLeaseFailure, Unit] =
    state.mode match {
      case CoordinatorMode.Running => Right(())
      case recovery: CoordinatorMode.RecoveryRequired =>
        Left(FinalityConsumerLeaseFailure.RecoveryRequired(recovery))
    }

  private def nextSinkRevision(
    current: Phase2ConsumerSinkRevision
  ): Either[FinalityConsumerLeaseFailure, Phase2ConsumerSinkRevision] =
    if (current.value.value == Long.MaxValue) Left(RevisionExhausted(current))
    else Right(Phase2ConsumerSinkRevision(NonNegLong.unsafeFrom(current.value.value + 1L)))
}
