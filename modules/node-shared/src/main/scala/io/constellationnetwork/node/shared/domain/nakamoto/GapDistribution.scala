package io.constellationnetwork.node.shared.domain.nakamoto

import java.math.MathContext

import scala.annotation.tailrec

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._

/** LDD gap-distribution helpers.
  *
  * The LDD snowplow assigns each slot δ since the last leader a per-validator success probability `f(δ)`. The probability that no leader is
  * elected in `K-1` consecutive slots given relative stake `r` is:
  *
  * `P(gap ≥ K) = ∏_{δ=1..K-1} (1 − f(δ))^r`
  *
  * Two evaluation paths are exposed:
  *
  *   - [[gapAtLeastExact]] / [[percentileGapExact]] — `Ratio` arithmetic, r = 1 (sole validator). Byte-identical across JVMs/CPUs so the
  *     result is safe to use on the consensus path. Slow for large K (numerator grows roughly factorially) but trivial at the K ≈ 100 scale
  *     the tail tests care about.
  *   - [[gapAtLeastApprox]] — `BigDecimal` arithmetic, arbitrary `r ∈ (0, 1]`. NON-deterministic across nodes (BigDecimal `pow` of a
  *     fractional exponent uses `Math.pow(Double, Double)` internally). METRICS-ONLY — do NOT route through consensus.
  *
  * Parameters are pinned to the Bifrost-prod defaults (`LddConfig.Default`). `FB` is a hard constant in production; tests may override
  * locally but production callers must not.
  *
  * See `docs/nakamoto/attestation-and-finality.md` §5.
  */
object GapDistribution {

  /** ψ — minimum gap before any eligibility. */
  val Psi: Int = 1

  /** γ — slot where the ramp reaches `FA`. */
  val Gamma: Int = 15

  /** Peak ramp amplitude. */
  val FA: Ratio = Ratio(1, 2)

  /** Baseline difficulty for δ ≥ γ. DO NOT raise this in production. */
  val FB: Ratio = Ratio(1, 20)

  /** f(δ) under the three-regime LDD snowplow. Returns an exact [[Ratio]] suitable for consensus-side arithmetic. */
  def thresholdAtGap(delta: Int): Ratio =
    if (delta < Psi) Ratio.Zero
    else if (delta < Gamma) Ratio(BigInt(delta - Psi), BigInt(Gamma - Psi)) * FA
    else FB

  /** P(gap ≥ K) at r = 1 (sole validator). Exact `Ratio`; deterministic across nodes — safe for consensus paths.
    *
    * For r = 1, `(1 − f(δ))^1 == (1 − f(δ))`, so the product collapses to a chain of `Ratio` multiplications and no exponent solver is
    * required. Returns `Ratio.One` for `K ≤ 1` (a 0-slot gap is trivially ≥ 0).
    */
  def gapAtLeastExact(k: Int): Ratio =
    if (k <= 1) Ratio.One
    else (1 until k).foldLeft(Ratio.One)((acc, delta) => acc * (Ratio.One - thresholdAtGap(delta)))

  // METRICS-ONLY: result is non-deterministic across nodes; do NOT use in consensus paths.
  /** P(gap ≥ K) at arbitrary `r`. BigDecimal-based: fractional `r` routes through `Math.pow(Double, Double)` internally, so the result is
    * non-deterministic across JVMs/CPUs.
    */
  def gapAtLeastApprox(k: Int, r: BigDecimal, mc: MathContext): BigDecimal =
    if (k <= 1) BigDecimal(1, mc)
    else {
      val one = BigDecimal(1, mc)
      (1 until k).foldLeft(one) { (acc, delta) =>
        val base = (one - thresholdAtGap(delta).toBigDecimal).round(mc)
        val raised =
          if (base.signum <= 0) BigDecimal(0, mc)
          else BigDecimal(Math.pow(base.toDouble, r.toDouble), mc)
        (acc * raised).round(mc)
      }
    }

  /** Smallest K such that `gapAtLeastExact(K) ≤ ε`. Walks K upward from `Psi` (the first slot where `f` > 0 starts to bite, i.e. δ = ψ + 1)
    * since smaller K trivially have `P(gap ≥ K) = 1`. Caps at `MaxKSearch` so a pathologically small ε returns a defined value rather than
    * looping forever.
    */
  def percentileGapExact(epsilon: Ratio): Int = {
    @tailrec
    def loop(k: Int): Int =
      if (k >= MaxKSearch) k
      else if (gapAtLeastExact(k) <= epsilon) k
      else loop(k + 1)
    loop(Psi)
  }

  /** Hard cap on the K search horizon. Beyond this, `(19/20)^K` is already below ~1e-23 — well past any meaningful percentile threshold an
    * audit/metric would request.
    */
  val MaxKSearch: Int = 1024
}
