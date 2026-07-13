package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.effect.kernel.Ref
import cats.syntax.all._
import cats.{Applicative, Monad}

import io.constellationnetwork.schema.SnapshotOrdinal

/** Current release-gate abstraction and migration surface for the target GL0 finality gadget.
  *
  * Target GL0 finality belongs to an exact `(ordinal, hash)` and has P0 Pending, P1 Provisional, and reversible P2 Operational. P2 is
  * selected by decided-attestation `T_weight` or canonical k1 depth; a density reorg orphans the old hash and triggers rollback/re-follow.
  * There is no global BFT vote/lock/QC or Phase 3.
  *
  * '''Implementation gap.''' This trait exposes only a monotone ordinal watermark and therefore cannot represent same-ordinal hash
  * replacement, per-hash phase, or reorg notification. [[fromRef]] is transitional HTTP gating, not the complete target FinalityGate.
  *
  * [[passThrough]] remains appropriate for ML0's separate BFT snapshot consensus. [[fromRef]] currently releases ordinals at-or-below the
  * local watermark. Routes take this type implicitly so the transitional gate is at least uniform.
  *
  * Routes take `FinalityGate[F]` as an implicit so gating is uniform across the codebase rather than hand-coded at each call site.
  */
trait FinalityGate[F[_]] {

  /** Transitional latest P2 ordinal visible to consumers. Target API must also expose the exact hash and reorg replacement. */
  def finalizedOrdinal: F[Option[SnapshotOrdinal]]

  /** Transitional ordinal-only release check. ML0 pass-through is always true; GL0 currently compares against the local P2 watermark.
    */
  def isServable(ordinal: SnapshotOrdinal): F[Boolean]
}

object FinalityGate {
  def apply[F[_]](implicit F: FinalityGate[F]): FinalityGate[F] = F

  /** ML0 BFT/non-GL0 pass-through instance. Caller provides head access without introducing a SnapshotStorage dependency.
    */
  def passThrough[F[_]: Applicative](readHeadOrdinal: F[Option[SnapshotOrdinal]]): FinalityGate[F] =
    new FinalityGate[F] {
      def finalizedOrdinal: F[Option[SnapshotOrdinal]] = readHeadOrdinal
      def isServable(ordinal: SnapshotOrdinal): F[Boolean] = true.pure[F]
    }

  /** Transitional Nakamoto ordinal watermark. This cannot satisfy the target exact-hash/reorg contract by itself.
    */
  def fromRef[F[_]: Monad](ref: Ref[F, SnapshotOrdinal]): FinalityGate[F] =
    new FinalityGate[F] {
      def finalizedOrdinal: F[Option[SnapshotOrdinal]] = ref.get.map(Some(_))
      def isServable(ordinal: SnapshotOrdinal): F[Boolean] =
        ref.get.map(finalized => ordinal <= finalized)
    }

  /** Trivial unconditional instance — everything is servable, finalized is None. Use only in test fixtures where finality doesn't apply. */
  def unrestricted[F[_]: Applicative]: FinalityGate[F] =
    new FinalityGate[F] {
      def finalizedOrdinal: F[Option[SnapshotOrdinal]] = Option.empty[SnapshotOrdinal].pure[F]
      def isServable(ordinal: SnapshotOrdinal): F[Boolean] = true.pure[F]
    }
}
