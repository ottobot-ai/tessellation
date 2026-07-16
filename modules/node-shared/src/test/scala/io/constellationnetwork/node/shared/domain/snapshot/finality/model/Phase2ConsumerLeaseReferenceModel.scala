package io.constellationnetwork.node.shared.domain.snapshot.finality.model

import io.constellationnetwork.node.shared.domain.snapshot.finality.{CanonicalBranchRevision, CanonicalLineageRevision, ReleaseGeneration}
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef

/** Pure, test-only model of the O-16 current-use race boundary.
  *
  * This model deliberately does not issue a production capability, verify Phase-2 evidence, define the exhaustive consumer-purpose
  * registry, or select a canonical branch. It starts after those decisions and specifies only the two short compare-and-set boundaries
  * around unlocked work: exact-reference acquisition and `commitIfCurrent`.
  */
object Phase2ConsumerLeaseReferenceModel {

  final case class SinkRevision(value: Long) {
    require(value >= 0L, "sink revision must be non-negative")
    def next: SinkRevision = SinkRevision(Math.addExact(value, 1L))
  }

  /** Illustrative policies only. O-16's exhaustive purpose inventory remains an activation blocker. */
  sealed trait ReferenceUsePolicy extends Product with Serializable
  object ReferenceUsePolicy {
    case object ExactCanonicalAncestor extends ReferenceUsePolicy
    case object LatestOperationalHead extends ReferenceUsePolicy
  }

  sealed trait CoordinatorMode extends Product with Serializable
  object CoordinatorMode {
    case object Running extends CoordinatorMode
    case object RecoveryRequired extends CoordinatorMode
  }

  final case class AppliedCommand(
    id: String,
    payload: String,
    target: GlobalSnapshotStateRef,
    releaseGeneration: ReleaseGeneration,
    lineageRevision: CanonicalLineageRevision,
    policy: ReferenceUsePolicy
  )

  final case class SinkState(revision: SinkRevision, applied: Map[String, AppliedCommand])

  object SinkState {
    val empty: SinkState = SinkState(SinkRevision(0L), Map.empty)
  }

  final case class State(
    mode: CoordinatorMode,
    branchRevision: CanonicalBranchRevision,
    lineageRevision: CanonicalLineageRevision,
    canonicalOldestFirst: Vector[GlobalSnapshotStateRef],
    operational: Map[GlobalSnapshotStateRef, ReleaseGeneration],
    sink: SinkState
  ) {
    require(canonicalOldestFirst.map(_.hash).distinct.size == canonicalOldestFirst.size, "canonical hashes must be unique")

    def latestOperational: Option[GlobalSnapshotStateRef] =
      canonicalOldestFirst.reverseIterator.find(operational.contains)
  }

  final case class AcquisitionDescriptor private (
    target: GlobalSnapshotStateRef,
    policy: ReferenceUsePolicy,
    releaseGeneration: ReleaseGeneration,
    branchRevision: CanonicalBranchRevision,
    lineageRevision: CanonicalLineageRevision,
    sinkRevision: SinkRevision
  )

  /** Marker that the immutable work selected by the descriptor completed. It conveys no current authority. */
  final case class VerifiedDescriptor private (descriptor: AcquisitionDescriptor)

  final case class Lease private (
    target: GlobalSnapshotStateRef,
    policy: ReferenceUsePolicy,
    releaseGeneration: ReleaseGeneration,
    observedBranchRevision: CanonicalBranchRevision,
    lineageRevision: CanonicalLineageRevision,
    sinkRevision: SinkRevision
  )

  final case class ClosedCommand(id: String, payload: String)

  sealed trait Failure extends Product with Serializable
  object Failure {
    case object RecoveryRequired extends Failure
    final case class NotCanonical(target: GlobalSnapshotStateRef) extends Failure
    final case class NotOperational(target: GlobalSnapshotStateRef) extends Failure
    final case class NotLatestOperational(target: GlobalSnapshotStateRef, latest: Option[GlobalSnapshotStateRef]) extends Failure
    case object AcquisitionChanged extends Failure
    case object LeaseLineageChanged extends Failure
    case object ReleaseChanged extends Failure
    case object SinkChanged extends Failure
    final case class CommandIdCollision(existing: AppliedCommand, attempted: AppliedCommand) extends Failure
  }

  sealed trait CommitResult extends Product with Serializable
  object CommitResult {
    final case class Committed(state: State, command: AppliedCommand) extends CommitResult
    final case class AlreadyCommitted(state: State, command: AppliedCommand) extends CommitResult
    final case class Stale(state: State, failure: Failure) extends CommitResult
    final case class RecoveryRequired(state: State, failure: Failure) extends CommitResult
  }

  def capture(
    state: State,
    target: GlobalSnapshotStateRef,
    policy: ReferenceUsePolicy
  ): Either[Failure, AcquisitionDescriptor] =
    currentTarget(state, target, policy).map { releaseGeneration =>
      AcquisitionDescriptor(
        target,
        policy,
        releaseGeneration,
        state.branchRevision,
        state.lineageRevision,
        state.sink.revision
      )
    }

  /** Models immutable evidence/readback verification outside every coordinator and sink lock. */
  def verify(descriptor: AcquisitionDescriptor): VerifiedDescriptor = VerifiedDescriptor(descriptor)

  /** The second short acquisition boundary compares the complete captured descriptor before minting a model lease. */
  def acquireIfCurrent(state: State, verified: VerifiedDescriptor): Either[Failure, Lease] = {
    val descriptor = verified.descriptor

    if (
      state.branchRevision != descriptor.branchRevision ||
      state.lineageRevision != descriptor.lineageRevision ||
      state.sink.revision != descriptor.sinkRevision
    ) Left(Failure.AcquisitionChanged)
    else
      currentTarget(state, descriptor.target, descriptor.policy).flatMap { currentRelease =>
        Either.cond(
          currentRelease == descriptor.releaseGeneration,
          Lease(
            descriptor.target,
            descriptor.policy,
            descriptor.releaseGeneration,
            descriptor.branchRevision,
            descriptor.lineageRevision,
            descriptor.sinkRevision
          ),
          Failure.AcquisitionChanged
        )
      }
  }

  /** Final short boundary for one closed, deterministic sink command.
    *
    * A descendant extension may preserve an exact-ancestor lease, but every replacement changes `lineageRevision` and invalidates all old
    * leases. Idempotence is checked only after currentness, so an old-generation command never regains authority by already existing.
    */
  def commitIfCurrent(state: State, lease: Lease, command: ClosedCommand): CommitResult = {
    val attempted = AppliedCommand(
      command.id,
      command.payload,
      lease.target,
      lease.releaseGeneration,
      lease.lineageRevision,
      lease.policy
    )

    currentForCommit(state, lease) match {
      case Left(Failure.RecoveryRequired) => CommitResult.RecoveryRequired(state, Failure.RecoveryRequired)
      case Left(failure)                  => CommitResult.Stale(state, failure)
      case Right(_) =>
        state.sink.applied.get(command.id) match {
          case Some(existing) if existing == attempted => CommitResult.AlreadyCommitted(state, existing)
          case Some(existing) =>
            CommitResult.RecoveryRequired(state, Failure.CommandIdCollision(existing, attempted))
          case None if state.sink.revision != lease.sinkRevision =>
            CommitResult.Stale(state, Failure.SinkChanged)
          case None =>
            val nextSink = SinkState(state.sink.revision.next, state.sink.applied.updated(command.id, attempted))
            CommitResult.Committed(state.copy(sink = nextSink), attempted)
        }
    }
  }

  private def currentForCommit(state: State, lease: Lease): Either[Failure, Unit] =
    if (state.mode == CoordinatorMode.RecoveryRequired) Left(Failure.RecoveryRequired)
    else if (state.lineageRevision != lease.lineageRevision) Left(Failure.LeaseLineageChanged)
    else
      currentTarget(state, lease.target, lease.policy).flatMap { release =>
        Either.cond(release == lease.releaseGeneration, (), Failure.ReleaseChanged)
      }

  private def currentTarget(
    state: State,
    target: GlobalSnapshotStateRef,
    policy: ReferenceUsePolicy
  ): Either[Failure, ReleaseGeneration] =
    if (state.mode == CoordinatorMode.RecoveryRequired) Left(Failure.RecoveryRequired)
    else if (!state.canonicalOldestFirst.contains(target)) Left(Failure.NotCanonical(target))
    else
      state.operational.get(target) match {
        case None => Left(Failure.NotOperational(target))
        case Some(release) =>
          policy match {
            case ReferenceUsePolicy.ExactCanonicalAncestor => Right(release)
            case ReferenceUsePolicy.LatestOperationalHead =>
              Either.cond(state.latestOperational.contains(target), release, Failure.NotLatestOperational(target, state.latestOperational))
          }
      }
}
