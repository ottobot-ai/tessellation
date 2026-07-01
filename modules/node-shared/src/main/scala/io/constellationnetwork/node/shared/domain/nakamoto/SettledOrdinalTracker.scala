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
    * `nakamotoFinalizedOrdinalRef` — see the class doc.
    */
  def make[F[_]: Sync]: F[SettledOrdinalTracker[F]] =
    Ref.of[F, SnapshotOrdinal](SnapshotOrdinal.MinValue).map { ref =>
      new SettledOrdinalTracker[F] {
        def markSettled(ordinal: SnapshotOrdinal): F[Unit] =
          ref.update(prev => if (ordinal.value.value > prev.value.value) ordinal else prev)

        def settledOrdinal: F[SnapshotOrdinal] = ref.get
      }
    }
}
