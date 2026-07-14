package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.data.NonEmptyChain

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoreCodecs.finalityCoreBatchPayloadCodec
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityIntentValidator.{
  CoreValidationContext,
  ResolvedPathManifest,
  ValidationResult,
  Violation
}

import eu.timepit.refined.types.numeric.NonNegLong

sealed trait FinalityCoordinatorKernelError extends Product with Serializable

object FinalityCoordinatorKernelError {
  final case class IdentityDerivationFailed(error: FinalityIdentityError) extends FinalityCoordinatorKernelError
  final case class ValidationFailed(violations: NonEmptyChain[Violation]) extends FinalityCoordinatorKernelError
  final case class CoreContextReleasedMismatch(
    expected: Option[ReleasedCore],
    actual: Option[ReleasedCore]
  ) extends FinalityCoordinatorKernelError
  final case class CoreContextEffectsMismatch(
    expected: Option[EffectManifestPointer],
    actual: Option[EffectManifestPointer]
  ) extends FinalityCoordinatorKernelError
  final case class RevisionExhausted(revision: HeadRevision) extends FinalityCoordinatorKernelError
}

/** Exact validated prepare inputs and the internally derived batch pointer.
  * Every field must be persisted before the prepared coordinator head is installed.
  */
final case class PreparedCoreBundle(
  batch: FinalityCoreBatch,
  pointer: FinalityCoreBatchPointer,
  effectManifest: FinalityEffectManifest,
  resolvedPaths: List[ResolvedPathManifest]
)

/** Read-only output of one pure coordinator-head mutation.
  *
  * Construction is sealed inside [[FinalityCoordinatorKernel]] so callers cannot
  * bypass transition validation with a public constructor or `copy`. The audit
  * record and optional recovery record are immutable prerequisites for installing
  * `head`. `preparedBundle` is present only for `Prepared` and holds every exact
  * validated artifact which must be durable before the head CAS.
  */
sealed trait FinalityCoordinatorMutation extends Product with Serializable {
  def head: CoordinatorHead
  def audit: CoordinatorAuditRecord
  def recoveryRecord: Option[RecoveryRecord]
  def preparedBundle: Option[PreparedCoreBundle]
}

/** Pure construction boundary for the local finality coordinator journal.
  *
  * This kernel never chooses a branch, decides finality, creates evidence, votes,
  * or performs I/O. It constructs only the closed coordinator mutation graph and
  * returns a result after the canonical identity and cross-field validators pass.
  * `RecoveryRequired` has no exit operation here.
  */
object FinalityCoordinatorKernel {
  import FinalityCoordinatorKernelError._

  type Result = Either[FinalityCoordinatorKernelError, FinalityCoordinatorMutation]

  private final case class KernelMutation(
    head: CoordinatorHead,
    audit: CoordinatorAuditRecord,
    recoveryRecord: Option[RecoveryRecord],
    preparedBundle: Option[PreparedCoreBundle]
  ) extends FinalityCoordinatorMutation

  def initialize: Result = {
    val draft = CoordinatorHead(
      revision = HeadRevision(NonNegLong.MinValue),
      lastAttempt = None,
      mode = CoordinatorMode.Running,
      released = None,
      active = None,
      effects = EffectsIndex(None, None),
      auditTail = None
    )

    seal(None, draft, CoordinatorMutationKind.Initialized, None, None)
  }

  def prepare(
    before: CoordinatorHead,
    batch: FinalityCoreBatch,
    effectManifest: FinalityEffectManifest,
    resolvedPaths: List[ResolvedPathManifest],
    context: CoreValidationContext
  ): Result =
    for {
      _ <- Either.cond(
        context.priorReleased == before.released,
        (),
        CoreContextReleasedMismatch(before.released, context.priorReleased): FinalityCoordinatorKernelError
      )
      expectedPreviousEffects = before.released.map(_.payload.effectManifest)
      actualPreviousEffects = context.previousEffects.map(_.pointer)
      _ <- Either.cond(
        actualPreviousEffects == expectedPreviousEffects,
        (),
        CoreContextEffectsMismatch(expectedPreviousEffects, actualPreviousEffects): FinalityCoordinatorKernelError
      )
      _ <- validated(FinalityIntentValidator.validateCoreBatch(batch, effectManifest, resolvedPaths, context))
      artifact <- identity(
        FinalityIdentity.artifactPointer(
          FinalityArtifactKind.CoreBatch,
          finalityCoreBatchPayloadCodec,
          batch
        )
      )
      pointer = FinalityCoreBatchPointer(
        batch.intentId,
        batch.scope.attempt,
        batch.scope.generation,
        artifact
      )
      revision <- nextRevision(before)
      active = ActiveCoreIntent(pointer, batch.scope, CoreStage.Prepared)
      draft = before.copy(
        revision = revision,
        lastAttempt = Some(batch.scope.attempt),
        active = Some(active),
        auditTail = None
      )
      result <- seal(
        Some(before),
        draft,
        CoordinatorMutationKind.Prepared,
        None,
        Some(PreparedCoreBundle(batch, pointer, effectManifest, resolvedPaths))
      )
    } yield result

  // CoreApplied, Released, and objective-restoration states remain representable
  // on disk for recovery and audit. No kernel entry point may mint them until a
  // package-owned durable runtime supplies opaque capabilities proving the exact
  // MPT/semantic/anchor CAS readback and holds the selected branch revision
  // through the coordinator-head CAS.

  def enterRecovery(before: CoordinatorHead, reason: RecoveryReason): Result =
    for {
      revision <- nextRevision(before)
      record = RecoveryRecord(
        enteredAt = revision,
        lastAttempt = before.lastAttempt,
        released = before.released,
        active = before.active,
        effects = before.effects,
        reason = reason,
        priorAudit = before.auditTail
      )
      pointer <- identity(FinalityIdentity.recoveryPointer(record))
      draft = before.copy(
        revision = revision,
        mode = CoordinatorMode.RecoveryRequired(pointer),
        auditTail = None
      )
      result <- seal(
        Some(before),
        draft,
        CoordinatorMutationKind.RecoveryEntered,
        Some(record),
        None
      )
    } yield result

  private def seal(
    before: Option[CoordinatorHead],
    draft: CoordinatorHead,
    mutation: CoordinatorMutationKind,
    recoveryRecord: Option[RecoveryRecord],
    preparedBundle: Option[PreparedCoreBundle]
  ): Result = {
    val audit = CoordinatorAuditRecord(
      mutation = mutation,
      before = before.map(_.commitment),
      after = draft.commitment,
      priorAudit = before.flatMap(_.auditTail)
    )

    for {
      auditPointer <- identity(FinalityIdentity.auditPointer(audit))
      head = draft.copy(auditTail = Some(auditPointer))
      _ <- validated(
        FinalityIntentValidator.validateCoordinatorTransition(before, head, audit, recoveryRecord)
      )
    } yield KernelMutation(head, audit, recoveryRecord, preparedBundle)
  }

  private def nextRevision(
    head: CoordinatorHead
  ): Either[FinalityCoordinatorKernelError, HeadRevision] = {
    val current = head.revision.value.value
    if (current == Long.MaxValue) Left(RevisionExhausted(head.revision))
    else Right(HeadRevision(NonNegLong.unsafeFrom(current + 1L)))
  }

  private def identity[A](
    derived: Either[FinalityIdentityError, A]
  ): Either[FinalityCoordinatorKernelError, A] =
    derived.left.map(IdentityDerivationFailed)

  private def validated[A](
    result: ValidationResult[A]
  ): Either[FinalityCoordinatorKernelError, A] =
    result.toEither.left.map(ValidationFailed)
}
