package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import io.constellationnetwork.numerics.Ratio

import derevo.cats.eqv
import derevo.derive

/** Result of a single level-µ trial. The producer computes `SuperLevelCount` of these per slot, one per super-level
  * µ ∈ {1..L-1}; level-0 is implicit (every existing snapshot is an L0 hit by chain existence — the L0 production gate
  * runs against the standard slot-gap snowplow `f(δ)`, not against this framework).
  *
  *   - `level` — the super-level µ (1..L-1)
  *   - `tau` — `Blake2b512(ρ_S ‖ "TEST-" ++ µ) / 2^512`, exact `Ratio` ∈ [0, 1)
  *   - `effectiveThreshold` — `θ_µ^eff(g_µ, δ_S) = θ_µ(g_µ) · min(1, δ_S/γ)`. Trial passes iff `tau < effectiveThreshold`.
  *   - `passed` — `tau < effectiveThreshold`
  *
  * Determinism: all fields derive from `(ρ_S, g_µ, δ_S, γ, params)` via exact `Ratio` arithmetic and the Bifrost
  * continued-fraction `Exp` interpreter — byte-identical across all JVMs/CPUs (same property the existing L0
  * `EligibilityChecker` relies on).
  */
@derive(eqv)
final case class LevelTrial(
  level: Int,
  tau: Ratio,
  effectiveThreshold: Ratio,
  passed: Boolean
)
