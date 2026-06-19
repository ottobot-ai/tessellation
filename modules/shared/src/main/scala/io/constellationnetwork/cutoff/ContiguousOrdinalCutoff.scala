package io.constellationnetwork.cutoff

import io.constellationnetwork.schema.SnapshotOrdinal

/** Retention policy that keeps a CONTIGUOUS recent window of the last `depth` ordinals, intersected with the caller-supplied lower bound
  * `cutoffOrdinal`.
  *
  * Contrast with [[LogarithmicOrdinalCutoff]], which keeps `current`, `current-1`, `current-2`, then geometrically-spaced older ordinals
  * (so the kept set has GAPS below the head). That gappy shape is fine for stores whose readers always ask for the head (or re-fold from a
  * sparse anchor), but it is WRONG for a store whose reader resolves a recent-but-not-head ordinal and demands that EXACT ordinal be
  * present — a logarithmic gap there yields a 404.
  *
  * The 3c-A signed-bytes store (`mpt_snapshot_info_signed`) is exactly such a store: `FinalizedSnapshotReader.latestMptEntriesResponse`
  * resolves the latest combined checkpoint at-or-below the finalized ordinal and reads that EXACT ordinal's signed bytes. Keeping a
  * contiguous recent window guarantees that resolved ordinal is retained.
  *
  * `cutoff(cutoffOrdinal, current)` returns `{current-depth+1 .. current} ∩ [cutoffOrdinal, current]` (clamped at 0). `depth <= 0` is
  * treated as `depth == 1` (keep only `current`); a non-positive window is never useful and we never want to return the empty set for a
  * valid `current`.
  */
object ContiguousOrdinalCutoff {
  def make(depth: Int): OrdinalCutoff = new OrdinalCutoff {
    def cutoff(cutoffOrdinal: SnapshotOrdinal, currentOrdinal: SnapshotOrdinal): Set[SnapshotOrdinal] = {
      val current = currentOrdinal.value.value
      val effectiveDepth = Math.max(1L, depth.toLong)
      // current - depth + 1, clamped at 0 (ordinals are non-negative).
      val windowStart = Math.max(0L, current - effectiveDepth + 1L)
      // Intersect the contiguous window with the lower bound the caller requires.
      val lowerBound = Math.max(windowStart, cutoffOrdinal.value.value)

      if (lowerBound > current) Set.empty[SnapshotOrdinal]
      else (lowerBound to current).map(SnapshotOrdinal.unsafeApply).toSet
    }
  }
}
