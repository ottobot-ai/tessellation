package io.constellationnetwork.schema.nakamoto

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.RatioInstances._

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** LDD Snowplow configuration.
  *
  * Three-regime threshold: f(δ) = 0 if δ < ψ ; fA × (δ - ψ) / (γ - ψ) if ψ ≤ δ < γ ; fB if δ ≥ γ.
  *
  * `amplitude` and `baselineDifficulty` are stored as exact `Ratio` so the threshold computation is byte-identical across all JVMs/CPUs —
  * eliminating the IEEE 754 nondeterminism that Double introduced.
  *
  * @param lddCutoff
  *   γ — slots where the ramp reaches amplitude
  * @param offset
  *   ψ — minimum gap before any eligibility
  * @param baselineDifficulty
  *   fB — difficulty in recovery region (δ ≥ γ)
  * @param amplitude
  *   fA — peak difficulty at the ramp top
  */
// NO embedded value default lives here. The LDD parameters are consensus-critical and AUTHORITATIVE in HOCON
// (`nakamoto.ldd` in application.conf, per-env overridable) — a source-level `Default` silently drifts from that
// config (γ=15-here vs γ=16-in-config is exactly the class of bug this removal prevents). Production reads the
// config; tests construct an explicit fixture (see `LddConfigFixture` in `shared` test scope).
@derive(encoder, decoder, eqv, show)
case class LddConfig(
  lddCutoff: Int,
  offset: Int,
  baselineDifficulty: Ratio,
  amplitude: Ratio
)
