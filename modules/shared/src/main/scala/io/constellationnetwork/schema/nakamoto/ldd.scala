package io.constellationnetwork.schema.nakamoto

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/**
 * LDD Snowplow configuration.
 *
 * Three-regime threshold:
 *   f(δ) = 0                          if δ < ψ
 *          fA × (δ - ψ) / (γ - ψ)    if ψ ≤ δ < γ
 *          fB                          if δ ≥ γ
 *
 * @param lddCutoff γ - slots where ramp reaches amplitude
 * @param offset ψ - minimum gap before any eligibility
 * @param baselineDifficulty fB - difficulty in recovery region (δ ≥ γ)
 * @param amplitude fA - peak difficulty at the ramp top
 */
@derive(encoder, decoder, eqv, show)
case class LddConfig(
  lddCutoff: Int,
  offset: Int,
  baselineDifficulty: Double,
  amplitude: Double
)

object LddConfig {

  /** Default Taktikos parameters from Bifrost production */
  val Default: LddConfig = LddConfig(
    lddCutoff = 15,
    offset = 0,
    baselineDifficulty = 0.05, // fB = 1/20
    amplitude = 0.5            // fA = 1/2
  )
}
