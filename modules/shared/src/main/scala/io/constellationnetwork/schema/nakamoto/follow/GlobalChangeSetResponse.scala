package io.constellationnetwork.schema.nakamoto.follow

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator

/** Response of the gl0 changeset producer for the currency-l0 (ml0) adopt-and-verify follow path (task #12, slice 2).
  *
  * Carries the per-ordinal typed state-deltas a follower holding the global state AT `baseOrdinal` needs to reach `latestOrdinal`. Unlike
  * the gl1 5-field slice ([[GlobalFollowSliceResponse]], which diffs two endpoint projections into ONE delta), a full-state follower
  * applies each ordinal's [[StateChangesAccumulator]] SEQUENTIALLY and matches the resulting MPT root against THAT ordinal's signed
  * `mptRoot` — so the deltas are an ordered, contiguous list, not a merged diff. (A GSI-diff is insufficient here: the accumulator also
  * carries the system expiry-index changes, which live only in the MPT, not the GSI.)
  *
  *   - `baseOrdinal = Some(b)`: `deltas` are the contiguous accumulators for ordinals `(b, latestOrdinal]`, applied on the state held at
  *     `b`. Empty `deltas` with `baseOrdinal = Some(latestOrdinal)` is a steady-state no-op (follower already at the tip).
  *   - `baseOrdinal = None`: the requested base is older than the retained ring (evicted) — the follower falls back to a full-GSI adopt.
  */
case class GlobalChangeSetResponse(
  latestOrdinal: SnapshotOrdinal,
  baseOrdinal: Option[SnapshotOrdinal],
  deltas: List[(SnapshotOrdinal, StateChangesAccumulator)]
)
