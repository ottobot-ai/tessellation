package io.constellationnetwork.schema.nakamoto.follow

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.security.smt.SmtProof

/** One per-ordinal entry of a [[GlobalChangeSetResponse]]: the typed state-delta a full-state follower (ml0) applies for ordinal
  * [[ordinal]], PLUS the §3-NIPoPoW historical-commitment SMT proofs that let the follower VERIFY (not merely trust) the signed `smtRoot`
  * by construction (Slice B).
  *
  *   - [[accumulator]] — the exact [[StateChangesAccumulator]] gl0 applied at [[ordinal]] (INCLUDING the system expiry-index changes a
  *     GSI-diff would miss); applied on the state held at the parent ordinal, the recomputed MPT root must match `ordinal`'s signed
  *     `mptRoot`. THIS is the ledger trust anchor — see [[GlobalChangeSetResponse]].
  *   - [[smtInclusionProof]] — `proveAt(ordinal, ordinal − k)`: an inclusion proof of the eligible ordinal's [[PerOrdinalCommitment]]
  *     against `smtRoot(ordinal)`. The follower derives the SAME commitment leaf itself, binds the proof's leaf bytes to its derived leaf
  *     (leaf-binding), then folds the proof to the SIGNED `smtRoot`. `None` in the genesis/warmup window `ordinal ≤ k` (where `smtRoot` is
  *     `None`) or when the eligible ordinal's root is out of retention on the producer.
  *   - [[smtAbsenceAtParent]] — `proveAt(ordinal − 1, ordinal − k)`: an absence (or inclusion) proof of the same eligible ordinal against
  *     the PARENT snapshot's `smtRoot(ordinal − 1)`. Populated by the producer for a FUTURE transition-verification variant (proving the
  *     eligible commitment first appears at `ordinal`, not before); ml0 IGNORES it in this slice. `None` under the same warmup/retention
  *     conditions as [[smtInclusionProof]].
  *
  * Both proof fields are `Option`: a missing/absent proof degrades that ordinal to mptRoot-only adoption (additive verification — the SMT
  * channel never blocks the ledger gate).
  */
final case class GlobalChangeSetDelta(
  ordinal: SnapshotOrdinal,
  accumulator: StateChangesAccumulator,
  smtInclusionProof: Option[SmtProof],
  smtAbsenceAtParent: Option[SmtProof]
)

/** Response of the gl0 changeset producer for the currency-l0 (ml0) adopt-and-verify follow path (task #12, slice 2; SMT proofs added in
  * Slice B).
  *
  * Carries the per-ordinal typed state-deltas a follower holding the global state AT `baseOrdinal` needs to reach `latestOrdinal`. Unlike
  * the gl1 5-field slice ([[GlobalFollowSliceResponse]], which diffs two endpoint projections into ONE delta), a full-state follower
  * applies each ordinal's [[StateChangesAccumulator]] SEQUENTIALLY and matches the resulting MPT root against THAT ordinal's signed
  * `mptRoot` — so the deltas are an ordered, contiguous list, not a merged diff. (A GSI-diff is insufficient here: the accumulator also
  * carries the system expiry-index changes, which live only in the MPT, not the GSI.) Each delta additionally carries the SMT proofs that
  * let the follower verify the signed `smtRoot` by construction (see [[GlobalChangeSetDelta]]).
  *
  *   - `baseOrdinal = Some(b)`: `deltas` are the contiguous accumulators for ordinals `(b, latestOrdinal]`, applied on the state held at
  *     `b`. Empty `deltas` with `baseOrdinal = Some(latestOrdinal)` is a steady-state no-op (follower already at the tip).
  *   - `baseOrdinal = None`: the requested base is older than the retained ring (evicted) — the follower falls back to a full-GSI adopt.
  */
case class GlobalChangeSetResponse(
  latestOrdinal: SnapshotOrdinal,
  baseOrdinal: Option[SnapshotOrdinal],
  deltas: List[GlobalChangeSetDelta]
)
