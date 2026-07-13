package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.functor._

import io.constellationnetwork.schema.SnapshotOrdinal

/** Injected, write-restricted view over a legacy-named local k2 retention watermark. Target GL0 has no "settled" Phase 3: k2 = 100*k1 is
  * only recommended retention, proof-service, and automatic-rollback capacity. The HTTP field and `T_depth2` writer remain transitional
  * observability and must not be interpreted as consensus finality.
  *
  * '''Distinct from `nakamotoFinalizedOrdinalRef` (k₁).''' This marker MUST be backed by its OWN `Ref` — NEVER aliased to the k₁
  * production-floor ref. Reporting k₁ here (or letting a route write it) would corrupt the k₁ production floor: production is gated at
  * "at-or-below finalized" on `nakamotoFinalizedOrdinalRef`, so a k₂-valued write there would stall block production. The only writer is
  * the legacy `T_depth2` branch in `SnapshotLeaderLoop.finalityMonitor`, via [[markSettled]]; every other consumer gets the read-only
  * [[settledOrdinal]].
  *
  * [[markSettled]] is internally monotone — a value not strictly greater than the current one is ignored — so the marker can never regress,
  * even under a mis-wired caller.
  */
trait SettledOrdinalTracker[F[_]] {

  /** Advance the local legacy k2 watermark iff `ordinal` is greater; otherwise a no-op. It cannot advance a protocol phase or validity.
    */
  def markSettled(ordinal: SnapshotOrdinal): F[Unit]

  /** Current legacy local-retention ordinal. `SnapshotOrdinal.MinValue` until the chain first advances past k2. Read-only telemetry.
    */
  def settledOrdinal: F[SnapshotOrdinal]
}

object SettledOrdinalTracker {

  /** Allocate a tracker backed by a FRESH `Ref` initialised to `SnapshotOrdinal.MinValue`. G1: this is a NEW ref, never the k₁
    * `nakamotoFinalizedOrdinalRef` — see the class doc. Convenience for callers (e.g. tests) that don't need to share the backing ref; the
    * production wiring uses [[makeFromRef]] instead.
    */
  def make[F[_]: Sync]: F[SettledOrdinalTracker[F]] =
    Ref.of[F, SnapshotOrdinal](SnapshotOrdinal.MinValue).map(makeFromRef[F])

  /** Build a tracker as a write-restricted view over an externally owned `Ref`. Live code also hands the ref to `NakamotoChainStore` as a
    * legacy k2 floor; that fork-choice role is a known target violation. The tracker exposes the current writer/route surface while the
    * store owns recovery reset.
    *
    * The caller must still pass a ref distinct from the current k1 watermark so legacy wiring cannot corrupt that separate state.
    *
    * [[markSettled]] stays internally monotone, so even though the store can now reset the shared ref, the tracker itself never regresses
    * it.
    */
  def makeFromRef[F[_]: Sync](ref: Ref[F, SnapshotOrdinal]): SettledOrdinalTracker[F] =
    new SettledOrdinalTracker[F] {
      def markSettled(ordinal: SnapshotOrdinal): F[Unit] =
        ref.update(prev => if (ordinal.value.value > prev.value.value) ordinal else prev)

      def settledOrdinal: F[SnapshotOrdinal] = ref.get
    }
}
