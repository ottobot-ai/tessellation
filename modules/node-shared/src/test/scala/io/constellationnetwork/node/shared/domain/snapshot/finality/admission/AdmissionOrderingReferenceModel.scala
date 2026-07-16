package io.constellationnetwork.node.shared.domain.snapshot.finality.admission

import io.constellationnetwork.node.shared.domain.snapshot.finality._

import eu.timepit.refined.types.numeric.NonNegLong

/** Test-only revision tuple used to exercise replacement ordering.
  *
  * This is not the complete revision/readback descriptor required to acquire or use a `CanonicalPhase2Lease`.
  */
private[finality] final case class AdmissionOrderingContext(
  releaseGeneration: ReleaseGeneration,
  lineageRevision: CanonicalLineageRevision,
  branchRevision: CanonicalBranchRevision
)

/** Opaque equality scope for the ordering model.
  *
  * `opaqueArtifactId` is supplied by the test harness. This model does not resolve an immutable artifact pointer, check a digest or byte
  * length, load bytes, or prove that this identifier commits any particular bytes. It is not local or portable admission authority.
  */
private[finality] final case class AdmissionOrderingScope(
  opaqueArtifactId: ArtifactId,
  context: AdmissionOrderingContext
)

/** Compare-and-set revision of this abstract ordering state only. It is not a consumer-sink revision. */
private[finality] final case class AdmissionOrderingRevision(value: NonNegLong)

private[finality] sealed trait AdmissionCleanupProgress extends Product with Serializable

private[finality] object AdmissionCleanupProgress {
  case object ReplacementOrdered extends AdmissionCleanupProgress
  case object InverseOrdered extends AdmissionCleanupProgress
  case object RequeueOrdered extends AdmissionCleanupProgress
}

private[finality] sealed trait AdmissionOrderingStage extends Product with Serializable

private[finality] object AdmissionOrderingStage {
  case object Current extends AdmissionOrderingStage
  final case class Invalidated(
    displacedScope: AdmissionOrderingScope,
    cleanup: AdmissionCleanupProgress
  ) extends AdmissionOrderingStage
}

/** Minimal transition vocabulary derived from L-23/O-16E ordering requirements.
  *
  * `ApplyForwardMutation` is deliberately generic and repeatable. It does not identify or order any concrete consumer or sink.
  * `OrderInverse` and `OrderRequeue` model only the abstract requirement that inverse cleanup precede requeue after replacement.
  */
private[finality] sealed trait AdmissionOrderingTransition extends Product with Serializable

private[finality] object AdmissionOrderingTransition {
  case object ApplyForwardMutation extends AdmissionOrderingTransition
  final case class Invalidate(replacement: AdmissionOrderingScope) extends AdmissionOrderingTransition
  case object OrderInverse extends AdmissionOrderingTransition
  case object OrderRequeue extends AdmissionOrderingTransition
}

private[finality] final case class AdmissionOrderingCommand(
  scope: AdmissionOrderingScope,
  observedContext: AdmissionOrderingContext,
  expectedRevision: AdmissionOrderingRevision,
  idempotencyKey: EffectIdempotencyKey,
  transition: AdmissionOrderingTransition
)

/** Result of an abstract external idempotency-record lookup.
  *
  * The state does not retain an unbounded command map. A future complete consumer journal must define the durable record/compaction policy
  * and atomically order its command with finality and its independently revisioned sink. This model implements none of those stores or
  * their crash boundaries; each observation is a pre-supplied input to one sequential model step.
  */
private[finality] sealed trait AdmissionIdempotencyObservation extends Product with Serializable

private[finality] object AdmissionIdempotencyObservation {
  case object AuthoritativelyAbsent extends AdmissionIdempotencyObservation
  final case class Recorded(record: AdmissionIdempotencyRecord) extends AdmissionIdempotencyObservation
  case object HistoryUnavailable extends AdmissionIdempotencyObservation
}

/** Test-model retry record binding the exact abstract transition. It is not portable evidence or a state-validity capability. */
private[finality] final case class AdmissionIdempotencyRecord(
  command: AdmissionOrderingCommand,
  before: AdmissionOrderingState,
  after: AdmissionOrderingState
) {
  def key: EffectIdempotencyKey = command.idempotencyKey
}

private[finality] final case class AdmissionOrderingState private[admission] (
  scope: AdmissionOrderingScope,
  stage: AdmissionOrderingStage,
  revision: AdmissionOrderingRevision
)

private[finality] sealed trait AdmissionStaleReason extends Product with Serializable

private[finality] object AdmissionStaleReason {
  final case class ScopeMismatch(expected: AdmissionOrderingScope, supplied: AdmissionOrderingScope) extends AdmissionStaleReason
  final case class ContextChanged(expected: AdmissionOrderingContext, observed: AdmissionOrderingContext) extends AdmissionStaleReason
  final case class ScopeInvalidated(current: AdmissionOrderingScope, displaced: AdmissionOrderingScope) extends AdmissionStaleReason
  final case class ReplacementNotNewer(current: AdmissionOrderingContext, replacement: AdmissionOrderingContext)
      extends AdmissionStaleReason
}

private[finality] sealed trait AdmissionRetryReason extends Product with Serializable

private[finality] object AdmissionRetryReason {
  final case class RevisionChanged(expected: AdmissionOrderingRevision, actual: AdmissionOrderingRevision)
      extends AdmissionRetryReason
  final case class UnexpectedStage(actual: AdmissionOrderingStage, requested: AdmissionOrderingTransition)
      extends AdmissionRetryReason
}

private[finality] sealed trait AdmissionRecoveryReason extends Product with Serializable

private[finality] object AdmissionRecoveryReason {
  final case class IdempotencyConflict(
    key: EffectIdempotencyKey,
    recorded: AdmissionOrderingCommand,
    conflicting: AdmissionOrderingCommand
  ) extends AdmissionRecoveryReason
  final case class IdempotencyLookupMismatch(expected: EffectIdempotencyKey, observed: EffectIdempotencyKey)
      extends AdmissionRecoveryReason
  final case class IdempotencyHistoryUnavailable(key: EffectIdempotencyKey) extends AdmissionRecoveryReason
  final case class InvalidIdempotencyRecord(record: AdmissionIdempotencyRecord) extends AdmissionRecoveryReason
  final case class IdempotencyStateConflict(
    recordedBefore: AdmissionOrderingState,
    recordedAfter: AdmissionOrderingState,
    actual: AdmissionOrderingState
  ) extends AdmissionRecoveryReason
  final case class ReplacementReconciliationRequired(
    current: AdmissionOrderingScope,
    requested: AdmissionOrderingScope
  ) extends AdmissionRecoveryReason
  final case class ReplacementRevisionExhausted(current: AdmissionOrderingContext) extends AdmissionRecoveryReason
  final case class RevisionExhausted(revision: AdmissionOrderingRevision) extends AdmissionRecoveryReason
}

private[finality] sealed trait AdmissionOrderingOutcome extends Product with Serializable {
  def state: AdmissionOrderingState
}

private[finality] object AdmissionOrderingOutcome {
  final case class Applied(state: AdmissionOrderingState, idempotencyRecord: AdmissionIdempotencyRecord)
      extends AdmissionOrderingOutcome

  /** The current state is exactly equal to the record's committed after-state. */
  final case class AlreadyAppliedCurrent(state: AdmissionOrderingState, idempotencyRecord: AdmissionIdempotencyRecord)
      extends AdmissionOrderingOutcome

  /** The exact transition is in history but current state has advanced. This is explicitly not success or current authority. */
  final case class HistoricallyRecorded(state: AdmissionOrderingState, idempotencyRecord: AdmissionIdempotencyRecord)
      extends AdmissionOrderingOutcome
  final case class Stale(state: AdmissionOrderingState, reason: AdmissionStaleReason) extends AdmissionOrderingOutcome
  final case class Retry(state: AdmissionOrderingState, reason: AdmissionRetryReason) extends AdmissionOrderingOutcome
  final case class RecoveryRequired(state: AdmissionOrderingState, reason: AdmissionRecoveryReason)
      extends AdmissionOrderingOutcome
}

/** Test-only ordering and invalidation reference model.
  *
  * This model intentionally does not implement P6's `CanonicalPhase2Lease` or `commitIfCurrent`. It has no exact
  * `GlobalSnapshotStateRef`, exhaustive purpose scope, released-core/readback identity, immutable pointer/digest/length verification,
  * independent consumer-sink digest/revision capability, explicit complete durable command identity, readback proof, signature, quorum,
  * economic authority, persistence adapter, or live integration. It only tests abstract current-scope mutation ordering, replacement
  * invalidation, inverse-before-requeue cleanup, sequential revision outcomes, and idempotency. It does not model sink execution,
  * readback, crash boundaries, or concurrent linearizability. It does not model or alter direct GL1-to-GL0 DAG-token acceptance, which
  * remains universally executed by GL0 validators.
  */
private[finality] object AdmissionOrderingReferenceModel {
  import AdmissionCleanupProgress._
  import AdmissionOrderingOutcome._
  import AdmissionOrderingStage._
  import AdmissionOrderingTransition._
  import AdmissionRecoveryReason._
  import AdmissionRetryReason._
  import AdmissionStaleReason._

  def initialize(
    scope: AdmissionOrderingScope,
    revision: AdmissionOrderingRevision
  ): AdmissionOrderingState =
    AdmissionOrderingState(scope, Current, revision)

  def applyCommand(
    before: AdmissionOrderingState,
    command: AdmissionOrderingCommand,
    idempotency: AdmissionIdempotencyObservation
  ): AdmissionOrderingOutcome =
    idempotency match {
      case AdmissionIdempotencyObservation.AuthoritativelyAbsent =>
        applyFresh(before, command)
      case AdmissionIdempotencyObservation.Recorded(record) =>
        applyRecorded(before, command, record)
      case AdmissionIdempotencyObservation.HistoryUnavailable =>
        RecoveryRequired(before, IdempotencyHistoryUnavailable(command.idempotencyKey))
    }

  private def applyRecorded(
    current: AdmissionOrderingState,
    command: AdmissionOrderingCommand,
    record: AdmissionIdempotencyRecord
  ): AdmissionOrderingOutcome =
    if (record.key != command.idempotencyKey)
      RecoveryRequired(current, IdempotencyLookupMismatch(command.idempotencyKey, record.key))
    else if (record.command != command)
      RecoveryRequired(current, IdempotencyConflict(command.idempotencyKey, record.command, command))
    else if (!isValidRecord(record))
      RecoveryRequired(current, InvalidIdempotencyRecord(record))
    else if (current == record.after)
      AlreadyAppliedCurrent(current, record)
    else if (current.revision.value.value > record.after.revision.value.value)
      HistoricallyRecorded(current, record)
    else
      RecoveryRequired(current, IdempotencyStateConflict(record.before, record.after, current))

  private def isValidRecord(record: AdmissionIdempotencyRecord): Boolean =
    applyFresh(record.before, record.command) match {
      case Applied(expectedAfter, _) => expectedAfter == record.after
      case _                         => false
    }

  private def applyFresh(
    before: AdmissionOrderingState,
    command: AdmissionOrderingCommand
  ): AdmissionOrderingOutcome =
    if (command.scope != before.scope)
      Stale(before, ScopeMismatch(before.scope, command.scope))
    else if (command.observedContext != before.scope.context)
      Stale(before, ContextChanged(before.scope.context, command.observedContext))
    else if (command.expectedRevision != before.revision)
      Retry(before, RevisionChanged(command.expectedRevision, before.revision))
    else
      command.transition match {
        case ApplyForwardMutation => applyForward(before, command)
        case Invalidate(replacement) => invalidate(before, command, replacement)
        case OrderInverse             => orderInverse(before, command)
        case OrderRequeue             => orderRequeue(before, command)
      }

  private def applyForward(
    before: AdmissionOrderingState,
    command: AdmissionOrderingCommand
  ): AdmissionOrderingOutcome =
    before.stage match {
      case Current => advance(before, command, before.scope, Current)
      case invalidated: Invalidated =>
        Stale(before, ScopeInvalidated(before.scope, invalidated.displacedScope))
    }

  private def invalidate(
    before: AdmissionOrderingState,
    command: AdmissionOrderingCommand,
    replacement: AdmissionOrderingScope
  ): AdmissionOrderingOutcome =
    if (replacementRevisionExhausted(before.scope.context))
      RecoveryRequired(before, ReplacementRevisionExhausted(before.scope.context))
    else if (!isNewerReplacement(before.scope.context, replacement.context))
      Stale(before, ReplacementNotNewer(before.scope.context, replacement.context))
    else
      before.stage match {
        case Current =>
          advance(before, command, replacement, Invalidated(before.scope, ReplacementOrdered))
        case _: Invalidated =>
          RecoveryRequired(before, ReplacementReconciliationRequired(before.scope, replacement))
      }

  private def replacementRevisionExhausted(current: AdmissionOrderingContext): Boolean =
    current.lineageRevision.value.value == Long.MaxValue || current.branchRevision.value.value == Long.MaxValue

  private def orderInverse(
    before: AdmissionOrderingState,
    command: AdmissionOrderingCommand
  ): AdmissionOrderingOutcome =
    before.stage match {
      case invalidated @ Invalidated(_, ReplacementOrdered) =>
        advance(before, command, before.scope, invalidated.copy(cleanup = InverseOrdered))
      case _ => Retry(before, UnexpectedStage(before.stage, command.transition))
    }

  private def orderRequeue(
    before: AdmissionOrderingState,
    command: AdmissionOrderingCommand
  ): AdmissionOrderingOutcome =
    before.stage match {
      case invalidated @ Invalidated(_, InverseOrdered) =>
        advance(before, command, before.scope, invalidated.copy(cleanup = RequeueOrdered))
      case _ => Retry(before, UnexpectedStage(before.stage, command.transition))
    }

  private def isNewerReplacement(
    current: AdmissionOrderingContext,
    replacement: AdmissionOrderingContext
  ): Boolean =
    replacement.releaseGeneration.value.value >= current.releaseGeneration.value.value &&
      replacement.lineageRevision.value.value > current.lineageRevision.value.value &&
      replacement.branchRevision.value.value > current.branchRevision.value.value

  private def advance(
    before: AdmissionOrderingState,
    command: AdmissionOrderingCommand,
    scope: AdmissionOrderingScope,
    stage: AdmissionOrderingStage
  ): AdmissionOrderingOutcome = {
    val current = before.revision.value.value
    if (current == Long.MaxValue)
      RecoveryRequired(before, RevisionExhausted(before.revision))
    else {
      val nextRevision = AdmissionOrderingRevision(NonNegLong.unsafeFrom(current + 1L))
      val after = AdmissionOrderingState(scope, stage, nextRevision)
      val record = AdmissionIdempotencyRecord(command, before, after)
      Applied(after, record)
    }
  }
}
