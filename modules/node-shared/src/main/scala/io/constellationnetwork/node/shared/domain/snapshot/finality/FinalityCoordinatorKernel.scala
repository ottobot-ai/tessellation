package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.data.NonEmptyChain
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoreCodecs.finalityCoreBatchPayloadCodec
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityIntentValidator._
import io.constellationnetwork.security.mpt.{MptActivePublication, VerifiedActiveMptPublication}

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
  final case class CoreContextPublicationMismatch(
    expected: MptActivePublication,
    actual: MptActivePublication
  ) extends FinalityCoordinatorKernelError
  final case class RevisionExhausted(revision: HeadRevision) extends FinalityCoordinatorKernelError
}

/** Exact validated prepare inputs and the internally derived batch pointer. Every field must be persisted before the prepared coordinator
  * head is installed.
  */
final case class PreparedCoreBundle(
  batch: FinalityCoreBatch,
  pointer: FinalityCoreBatchPointer,
  effectManifest: FinalityEffectManifest,
  resolvedPaths: List[ResolvedPathManifest]
)

/** Read-only output of one pure coordinator-head mutation.
  *
  * Construction is sealed inside [[FinalityCoordinatorKernel]] so callers cannot bypass transition validation with a public constructor or
  * `copy`. The audit record and optional recovery record are immutable prerequisites for installing `head`. `preparedBundle` is present
  * only for `Prepared` and holds every exact validated artifact which must be durable before the head CAS.
  */
sealed trait FinalityCoordinatorMutation extends Product with Serializable {
  def head: CoordinatorHead
  def audit: CoordinatorAuditRecord
  def recoveryRecord: Option[RecoveryRecord]
  def preparedBundle: Option[PreparedCoreBundle]
}

/** Validated construction boundary for the local finality coordinator journal.
  *
  * This kernel never chooses a branch, decides finality, creates evidence, or votes. Pure transition entry points construct only the closed
  * coordinator mutation graph after canonical identity and cross-field validation. The private initialization path additionally consumes a
  * verified MPT lease and persists the mutation before returning. `RecoveryRequired` has no exit operation here.
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

  /** Raw construction used only by the package-owned effectful bootstrap boundary.
    *
    * Object-private visibility prevents production or test callers from supplying a publication cursor. Bootstrap must enter through
    * [[FinalityCoordinatorBootstrap]].
    */
  private def initializeMutation(initialPublication: MptActivePublication): Result = {
    val draft = CoordinatorHead(
      revision = HeadRevision(NonNegLong.MinValue),
      lastAttempt = None,
      mode = CoordinatorMode.Running,
      released = None,
      active = None,
      publication = initialPublication,
      effects = EffectsIndex(None, None),
      auditTail = None
    )

    seal(None, draft, CoordinatorMutationKind.Initialized, None, None)
  }

  /** Consume an exact store-minted publication lease and install the derived initial coordinator before returning.
    *
    * Neither the raw publication nor the initialization mutation crosses this boundary. The caller must already hold the MPT publication
    * lease; [[FinalityCoordinatorBootstrap]] owns that lock ordering.
    */
  private[finality] def initializeDurably[F[_]: Async](
    verifiedPublication: VerifiedActiveMptPublication[F],
    finality: FinalityDurableStore[F]
  ): F[Unit] =
    for {
      activePublication <- verifiedPublication.read
      mutation <- Async[F].fromEither(
        initializeMutation(activePublication).leftMap(FinalityCoordinatorBootstrapError.KernelRejected)
      )
      installed <- finality.initializeCoordinator(mutation)
    } yield
      installed match {
        case FinalityDurableCasResult.Installed(_)        => ()
        case FinalityDurableCasResult.AlreadyInstalled(_) => ()
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
      _ <- Either.cond(
        context.currentPublication == before.publication,
        (),
        CoreContextPublicationMismatch(before.publication, context.currentPublication): FinalityCoordinatorKernelError
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

  // CoreApplied and Released remain representable in durable records for recovery
  // and audit. Restoration stages are schema-only and the durable store rejects
  // them. No kernel entry point may mint any of these states until a package-owned
  // runtime supplies opaque capabilities proving the exact MPT/semantic/anchor CAS
  // readback and holds the selected branch revision through the coordinator-head CAS.

  def enterRecovery(before: CoordinatorHead, reason: RecoveryReason): Result =
    for {
      revision <- nextRevision(before)
      record = RecoveryRecord(
        enteredAt = revision,
        lastAttempt = before.lastAttempt,
        released = before.released,
        active = before.active,
        publication = before.publication,
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
