package io.constellationnetwork.node.shared.domain.nakamoto.overlay.model

import scala.annotation.tailrec

/** Pure executable specification for exact-parent overlay access and atomic prefix finalization.
  *
  * Hashes and roots are symbolic because this model specifies identity and transition rules, not a particular MPT implementation. A
  * production implementation must replace `StateImage.root` with its canonical MPT root calculation while preserving these transitions.
  */
object ExactBranchReferenceModel {

  final case class SnapshotHash(value: String) extends AnyVal
  final case class StateRoot(value: String) extends AnyVal

  final case class SnapshotRef(
    ordinal: Long,
    hash: SnapshotHash,
    parentHash: SnapshotHash,
    stateRoot: StateRoot
  ) {
    require(ordinal >= 0L, "snapshot ordinal must be non-negative")
  }

  final case class StateImage(entries: Map[String, Long]) {
    def root: StateRoot =
      StateRoot(entries.toList.sortBy(_._1).map { case (key, value) => s"${key.length}:$key:$value" }.mkString("|"))
  }

  final case class StateDelta(upserts: Map[String, Long], removals: Set[String]) {
    require(upserts.keySet.intersect(removals).isEmpty, "a key cannot be both upserted and removed")

    def applyTo(image: StateImage): StateImage = {
      val afterRemovals = image.entries -- removals
      StateImage(upserts.foldLeft(afterRemovals) { case (entries, (key, value)) => entries.updated(key, value) })
    }
  }

  object StateDelta {
    val empty: StateDelta = StateDelta(Map.empty, Set.empty)
  }

  final case class PersistedBase(ref: SnapshotRef, image: StateImage)

  final case class VerifiedBaseAnchor private (ref: SnapshotRef, image: StateImage)

  object VerifiedBaseAnchor {
    def bind(expected: SnapshotRef, persisted: PersistedBase): Either[RecoveryCause, VerifiedBaseAnchor] =
      if (persisted.ref != expected)
        Left(RecoveryCause.RestartIdentityMismatch(expected, persisted.ref))
      else if (persisted.image.root != expected.stateRoot)
        Left(RecoveryCause.RestartRootMismatch(expected.stateRoot, persisted.image.root))
      else
        Right(VerifiedBaseAnchor(expected, persisted.image))

    private[ExactBranchReferenceModel] def fromReproduction(ref: SnapshotRef, image: StateImage): VerifiedBaseAnchor = {
      require(image.root == ref.stateRoot, "a reproduced anchor must match the snapshot state root")
      VerifiedBaseAnchor(ref, image)
    }
  }

  final case class PendingBranch(ref: SnapshotRef, delta: StateDelta)

  final case class State(anchor: VerifiedBaseAnchor, pending: Map[SnapshotHash, PendingBranch])

  object State {
    def initial(anchor: VerifiedBaseAnchor): State = State(anchor, Map.empty)
  }

  final case class ExactParentSession(parent: SnapshotRef, image: StateImage, reproducedPath: Vector[SnapshotRef])

  sealed trait RecoveryCause extends Product with Serializable
  object RecoveryCause {
    final case class RestartIdentityMismatch(expected: SnapshotRef, persisted: SnapshotRef) extends RecoveryCause
    final case class RestartRootMismatch(expected: StateRoot, reproduced: StateRoot) extends RecoveryCause
    final case class UnknownExactParent(requested: SnapshotRef) extends RecoveryCause
    final case class ExactRefMismatch(requested: SnapshotRef, stored: SnapshotRef) extends RecoveryCause
    final case class MissingAncestor(child: SnapshotRef, missingParent: SnapshotHash) extends RecoveryCause
    final case class OrdinalGap(parent: SnapshotRef, child: SnapshotRef) extends RecoveryCause
    final case class ReproducedRootMismatch(ref: SnapshotRef, reproduced: StateRoot) extends RecoveryCause
    final case class CanonicalTipDoesNotContain(finalized: SnapshotRef, canonicalTip: SnapshotRef) extends RecoveryCause
  }

  sealed trait Result[+A] extends Product with Serializable {
    def state: State
  }
  object Result {
    final case class Applied[A](state: State, value: A) extends Result[A]
    final case class RecoveryRequired(state: State, cause: RecoveryCause) extends Result[Nothing]
    final case class FinalizationFailed(state: State, reason: String) extends Result[Nothing]
  }

  sealed trait FinalizationFault extends Product with Serializable
  object FinalizationFault {
    case object None extends FinalizationFault
    case object BeforeAtomicInstall extends FinalizationFault
  }

  def stage(state: State, branch: PendingBranch): Result[Unit] =
    state.pending.get(branch.ref.hash) match {
      case None                                 => Result.Applied(state.copy(pending = state.pending.updated(branch.ref.hash, branch)), ())
      case Some(existing) if existing == branch => Result.Applied(state, ())
      case Some(existing) =>
        Result.RecoveryRequired(state, RecoveryCause.ExactRefMismatch(branch.ref, existing.ref))
    }

  def acquireExactParent(state: State, requested: SnapshotRef): Result[ExactParentSession] =
    reproduce(state, requested) match {
      case Left(cause) => Result.RecoveryRequired(state, cause)
      case Right((image, path)) =>
        Result.Applied(state, ExactParentSession(requested, image, path.map(_.ref)))
    }

  def restart(state: State, expected: SnapshotRef, persisted: PersistedBase): Result[Unit] =
    VerifiedBaseAnchor.bind(expected, persisted) match {
      case Left(cause)   => Result.RecoveryRequired(state, cause)
      case Right(anchor) => Result.Applied(State.initial(anchor), ())
    }

  /** Atomically folds the selected prefix into the verified base.
    *
    * Only descendants on the exact `canonicalTip` lineage survive. Folded ancestors, siblings, and disconnected pending branches are
    * discarded. A failure before installation returns the original state byte-for-byte.
    */
  def finalizePrefix(
    state: State,
    finalized: SnapshotRef,
    canonicalTip: SnapshotRef,
    fault: FinalizationFault = FinalizationFault.None
  ): Result[Unit] =
    (reproduce(state, finalized), reproduce(state, canonicalTip)) match {
      case (Left(cause), _) => Result.RecoveryRequired(state, cause)
      case (_, Left(cause)) => Result.RecoveryRequired(state, cause)
      case (Right((finalizedImage, _)), Right((_, canonicalPath))) =>
        val finalizedIndex =
          if (finalized == state.anchor.ref) -1
          else canonicalPath.indexWhere(_.ref == finalized)

        if (finalized != state.anchor.ref && finalizedIndex < 0)
          Result.RecoveryRequired(state, RecoveryCause.CanonicalTipDoesNotContain(finalized, canonicalTip))
        else {
          val descendants = canonicalPath.drop(finalizedIndex + 1)
          val nextAnchor = VerifiedBaseAnchor.fromReproduction(finalized, finalizedImage)
          val nextPending = descendants.iterator.map(branch => branch.ref.hash -> branch).toMap
          val next = State(nextAnchor, nextPending)

          fault match {
            case FinalizationFault.None                => Result.Applied(next, ())
            case FinalizationFault.BeforeAtomicInstall => Result.FinalizationFailed(state, "injected before atomic install")
          }
        }
    }

  private def reproduce(
    state: State,
    requested: SnapshotRef
  ): Either[RecoveryCause, (StateImage, Vector[PendingBranch])] =
    if (requested == state.anchor.ref)
      Right((state.anchor.image, Vector.empty))
    else
      lineage(state, requested).flatMap { path =>
        path
          .foldLeft[Either[RecoveryCause, StateImage]](Right(state.anchor.image)) {
            case (acc, branch) =>
              acc.flatMap { image =>
                val reproduced = branch.delta.applyTo(image)
                if (reproduced.root == branch.ref.stateRoot) Right(reproduced)
                else Left(RecoveryCause.ReproducedRootMismatch(branch.ref, reproduced.root))
              }
          }
          .map(_ -> path)
      }

  private def lineage(state: State, requested: SnapshotRef): Either[RecoveryCause, Vector[PendingBranch]] = {
    @tailrec
    def loop(childRef: SnapshotRef, suffix: Vector[PendingBranch]): Either[RecoveryCause, Vector[PendingBranch]] =
      state.pending.get(childRef.hash) match {
        case None                                   => Left(RecoveryCause.UnknownExactParent(childRef))
        case Some(stored) if stored.ref != childRef => Left(RecoveryCause.ExactRefMismatch(childRef, stored.ref))
        case Some(stored) if stored.ref.parentHash == state.anchor.ref.hash =>
          if (stored.ref.ordinal == state.anchor.ref.ordinal + 1L) Right(stored +: suffix)
          else Left(RecoveryCause.OrdinalGap(state.anchor.ref, stored.ref))
        case Some(stored) =>
          state.pending.get(stored.ref.parentHash) match {
            case None => Left(RecoveryCause.MissingAncestor(stored.ref, stored.ref.parentHash))
            case Some(parent) if parent.ref.ordinal + 1L != stored.ref.ordinal =>
              Left(RecoveryCause.OrdinalGap(parent.ref, stored.ref))
            case Some(parent) => loop(parent.ref, stored +: suffix)
          }
      }

    loop(requested, Vector.empty)
  }
}
