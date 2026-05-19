package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import io.constellationnetwork.numerics.Ratio

/** Per-super-level shifted-exponential parameters from `docs/nakamoto/NIPOPOW-PROPOSAL.md` §2.2 Table.
  *
  * Threshold: `θ_µ(g_µ) = p_µ^max · (1 − exp(−(g_µ − ψ_super) / σ_µ))` for `g_µ ≥ ψ_super`, else 0.
  *
  * Values validated at 10M slots in `~/repos/research-nipopos-2026/paper/main.tex` Table 1 (achieved-vs-target rates within 1–13%
  * per level). All values exact `Ratio` — no `Double` arithmetic on the threshold path.
  */
final case class SuperLevelParam(
  level: Int,
  pMax: Ratio,
  sigma: Ratio,
  targetDensity: Ratio
)

object SuperLevelParams {

  /** Per-super-level dormancy threshold from the paper. With `ψ_super = 1` a block at `g_µ = 1` (immediately after the previous
    * level-µ hit) has zero level-µ threshold — the burst-zero condition relied on by Theorem 4.2 (Burst Resistance).
    *
    * Distinct from the L0 LDD ramp's `ψ_L0` (slot-gap dormancy in `f(δ)`). See proposal §2.2 "Note on symbols".
    */
  val PsiSuper: Long = 1L

  /** Total levels including L0. Super-levels are µ ∈ {1, …, L-1}; L0 is implicit (every snapshot is a level-0 hit by chain
    * existence — the L0 production gate runs against `f(δ)`, not against the NIPoPoW trial framework).
    */
  val L: Int = 10

  /** L1–L9 parameter table. Indexed [0..8] = levels [1..9]. */
  val Levels: Vector[SuperLevelParam] = Vector(
    // p_µ^max  σ_µ      target
    SuperLevelParam(level = 1, pMax = Ratio(BigInt(1131), BigInt(1000)), sigma = Ratio(BigInt(1), BigInt(2)),
      targetDensity = Ratio(BigInt(1), BigInt(2))),
    SuperLevelParam(level = 2, pMax = Ratio(BigInt(346), BigInt(1000)), sigma = Ratio(BigInt(1), BigInt(2)),
      targetDensity = Ratio(BigInt(1), BigInt(4))),
    SuperLevelParam(level = 3, pMax = Ratio(BigInt(322), BigInt(1000)), sigma = Ratio(BigInt(792), BigInt(100)),
      targetDensity = Ratio(BigInt(1), BigInt(8))),
    SuperLevelParam(level = 4, pMax = Ratio(BigInt(249), BigInt(1000)), sigma = Ratio(BigInt(303), BigInt(10)),
      targetDensity = Ratio(BigInt(1), BigInt(16))),
    SuperLevelParam(level = 5, pMax = Ratio(BigInt(77), BigInt(1000)), sigma = Ratio(BigInt(275), BigInt(10)),
      targetDensity = Ratio(BigInt(1), BigInt(32))),
    SuperLevelParam(level = 6, pMax = Ratio(BigInt(27), BigInt(1000)), sigma = Ratio(BigInt(405), BigInt(10)),
      targetDensity = Ratio(BigInt(1), BigInt(64))),
    SuperLevelParam(level = 7, pMax = Ratio(BigInt(13), BigInt(1000)), sigma = Ratio(BigInt(56), BigInt(1)),
      targetDensity = Ratio(BigInt(1), BigInt(128))),
    SuperLevelParam(level = 8, pMax = Ratio(BigInt(4), BigInt(1000)), sigma = Ratio(BigInt(64), BigInt(1)),
      targetDensity = Ratio(BigInt(1), BigInt(256))),
    SuperLevelParam(level = 9, pMax = Ratio(BigInt(2), BigInt(1000)), sigma = Ratio(BigInt(72), BigInt(1)),
      targetDensity = Ratio(BigInt(1), BigInt(512)))
  )

  /** Number of super-levels (L1..L9). The level-0 trial is implicit. */
  val SuperLevelCount: Int = Levels.size

  /** Lookup the params for a given super-level (1..L-1). Returns `None` if out of range. */
  def at(level: Int): Option[SuperLevelParam] =
    if (level >= 1 && level <= SuperLevelCount) Some(Levels(level - 1)) else None
}
