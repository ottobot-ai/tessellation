package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.functor._

import io.constellationnetwork.schema.SnapshotOrdinal

/** Injected, write-restricted marker for the k₂ "settled" (Phase 2 → Phase 3 archival) ordinal — the deepest ordinal past the
  * `keepDepthBehindFinalized` archival gate (k₂ = 100·k₁, the single canonical k₂ accessor on `NakamotoConfig`). Promoted out of the
  * fiber-local `lastArchivalOrdinalRef` in `SnapshotLeaderLoop` (Track-3 S1) so the HTTP layer can read it (`GET
  * /global-snapshots/settled`) without owning the leader loop's internal state — the same DI shape as `FinalityTriggerView`.
  *
  * '''Distinct from `nakamotoFinalizedOrdinalRef` (k₁).''' This marker MUST be backed by its OWN `Ref` — NEVER aliased to the k₁
  * production-floor ref. Reporting k₁ here (or letting a route write it) would corrupt the k₁ production floor: production is gated at
  * "at-or-below finalized" on `nakamotoFinalizedOrdinalRef`, so a k₂-valued write there would stall block production. The only writer is
  * the `T_depth2` archival sink in `SnapshotLeaderLoop.finalityMonitor`, via [[markSettled]]; every other consumer (routes) gets the
  * read-only [[settledOrdinal]].
  *
  * [[markSettled]] is internally monotone — a value not strictly greater than the current one is ignored — so the marker can never regress,
  * even under a mis-wired caller.
  */
trait SettledOrdinalTracker[F[_]] {

  /** Advance the settled (k₂-archival) marker to `ordinal` iff it is strictly greater than the current value; otherwise a no-op. Called
    * ONLY at the `T_depth2` archival sink in `SnapshotLeaderLoop.finalityMonitor`.
    */
  def markSettled(ordinal: SnapshotOrdinal): F[Unit]

  /** The current settled (k₂-archival) ordinal. `SnapshotOrdinal.MinValue` until the chain first advances past k₂ (cold start). Read-only
    * surface for the HTTP route.
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

  /** Build a tracker as a write-restricted VIEW over an EXTERNALLY-owned `Ref` (Track-3 S1.5 "marker split"). The same ref is handed to
    * `NakamotoChainStore` as `nakamotoSettledOrdinalRef`, so there is exactly ONE settled (k₂) source: this tracker is the monotone
    * advance-and-read surface for the `T_depth2` sink + the `/settled` route, while the store owns the reset (`unsafe_clearFinality` sets
    * it back to `MinValue`, in lock-step with `nakamotoFinalizedOrdinalRef`).
    *
    * '''Still distinct from `nakamotoFinalizedOrdinalRef` (k₁).''' The caller MUST pass a ref that is NOT the k₁ production-floor ref — see
    * the class doc. This constructor only shares the settled ref with the store/fork-choice (k₂ consumers); it never aliases k₁.
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
