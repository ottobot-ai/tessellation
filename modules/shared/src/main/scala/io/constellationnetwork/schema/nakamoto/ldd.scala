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
@derive(encoder, decoder, eqv, show)
case class LddConfig(
  lddCutoff: Int,
  offset: Int,
  baselineDifficulty: Ratio,
  amplitude: Ratio
)

object LddConfig {

  /** Decimal digits of precision used when parsing Double-typed env vars (`NAKAMOTO_LDD_BASELINE`, `NAKAMOTO_LDD_AMPLITUDE`) into `Ratio`.
    * 18 = limit of Double mantissa. After this point the value is locked in as an exact rational and never touches Double again on the
    * consensus path.
    */
  val DoubleParsePrecision: Int = 18

  /** Default Taktikos parameters from Bifrost production. fA = 1/2, fB = 1/20. */
  val Default: LddConfig = LddConfig(
    lddCutoff = 15,
    offset = 1,
    baselineDifficulty = Ratio(1, 20),
    amplitude = Ratio(1, 2)
  )

  /** Construct from Double-typed inputs (env vars, legacy callers). Locks the value to `DoubleParsePrecision` decimal digits.
    */
  def fromDoubles(lddCutoff: Int, offset: Int, baselineDifficulty: Double, amplitude: Double): LddConfig =
    LddConfig(
      lddCutoff,
      offset,
      Ratio(baselineDifficulty, DoubleParsePrecision),
      Ratio(amplitude, DoubleParsePrecision)
    )
}
