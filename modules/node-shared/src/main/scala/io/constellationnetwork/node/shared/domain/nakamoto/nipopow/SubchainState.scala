package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import derevo.cats.eqv
import derevo.derive

/** §3 NIPoPoW SubchainState (paper §3.4): cumulative count of level-µ hits per super-level from genesis to this snapshot. One entry per
  * super-level (indices 0..L-2 = levels 1..L-1). Level-0 hits are implicit (every snapshot is an L0 hit by chain construction) so they
  * don't appear here.
  *
  * '''Why store counts and not weights.''' The per-block weight contribution `θ(δ)^α + exponential bonus` is a verifier-side computation;
  * the producer only needs to hand the verifier enough information to reconstruct cumulative weight without traversing the chain.
  * Cumulative counts × per-level weight contribution is sufficient.
  *
  * '''Compactness contract.''' Vector size MUST equal [[SuperLevelParams.SuperLevelCount]] = L-1 = 9. Wire encoding is fixed-width
  * per-level int64 → 72 bytes/snapshot for L=10.
  */
@derive(eqv)
final case class SubchainState(levelCounts: Vector[Long]) {
  require(
    levelCounts.size == SuperLevelParams.SuperLevelCount,
    s"SubchainState.levelCounts must have ${SuperLevelParams.SuperLevelCount} entries (one per super-level L1..L${SuperLevelParams.SuperLevelCount}), got ${levelCounts.size}"
  )

  /** Cumulative count at super-level µ (1..L-1). Returns 0 if µ is out of range — defensive; callers should validate. */
  def countAt(level: Int): Long =
    if (level >= 1 && level <= SuperLevelParams.SuperLevelCount) levelCounts(level - 1) else 0L
}

object SubchainState {

  /** Genesis subchain state — all super-level counts at 0. Every node anchors here. */
  val Genesis: SubchainState = SubchainState(Vector.fill(SuperLevelParams.SuperLevelCount)(0L))
}
