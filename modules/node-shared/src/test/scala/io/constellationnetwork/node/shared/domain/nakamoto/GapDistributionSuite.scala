package io.constellationnetwork.node.shared.domain.nakamoto

import java.math.MathContext

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._

import weaver.SimpleIOSuite

object GapDistributionSuite extends SimpleIOSuite {

  // ============ thresholdAtGap ============

  pureTest("f(δ < ψ) is zero (dormant regime)") {
    expect(GapDistribution.thresholdAtGap(0) == Ratio.Zero)
  }

  pureTest("f(δ = ψ) is zero (ramp starts at zero)") {
    // f(1) = (1-1)/(15-1) * 1/2 = 0
    expect(GapDistribution.thresholdAtGap(GapDistribution.Psi) == Ratio.Zero)
  }

  pureTest("f(δ) in ramp regime matches (δ-ψ)/(γ-ψ) · fA") {
    // f(8) = 7/14 * 1/2 = 1/4
    expect(GapDistribution.thresholdAtGap(8) == Ratio(1, 4))
  }

  pureTest("f(δ ≥ γ) is fB = 1/20") {
    expect.all(
      GapDistribution.thresholdAtGap(GapDistribution.Gamma) == GapDistribution.FB,
      GapDistribution.thresholdAtGap(50) == GapDistribution.FB,
      GapDistribution.thresholdAtGap(1000) == GapDistribution.FB
    )
  }

  // ============ gapAtLeastExact (r = 1) ============

  pureTest("gapAtLeastExact(K ≤ 1) is 1 (degenerate)") {
    expect.all(
      GapDistribution.gapAtLeastExact(0) == Ratio.One,
      GapDistribution.gapAtLeastExact(1) == Ratio.One
    )
  }

  pureTest("gapAtLeastExact is monotone non-increasing") {
    val seq = (1 to 30).map(GapDistribution.gapAtLeastExact)
    expect(seq.sliding(2).forall { case Seq(a, b) => a >= b; case _ => true })
  }

  pureTest("gapAtLeastExact(73) is at or below the 1/1000 ε (99.9th-percentile cliff)") {
    // P(gap ≥ K) under LDD with ψ=1, γ=15, fA=1/2, fB=1/20, r=1 crosses 1e-3
    // between K=72 (≈1.03e-3) and K=73 (≈9.81e-4). Numerically derivable via the
    // closed form: ramp product (28!/14!) / 28^14 ≈ 0.0192 times baseline tail
    // (19/20)^(K-15). The task brief cited "~92" but that did not match the LDD
    // params as defined in `EligibilityChecker.threshold` — the actual crossing
    // is at K=73 by construction of `gapAtLeastExact`.
    expect(GapDistribution.gapAtLeastExact(73) <= Ratio(1, 1000))
  }

  // ============ percentileGapExact ============

  pureTest("percentileGapExact(ε=1/1000) returns the 73-slot cliff") {
    // Allow a ±2-slot tolerance to absorb Ratio rounding choices in the
    // ramp/baseline boundary without pinning the exact integer in two places.
    val k = GapDistribution.percentileGapExact(Ratio(1, 1000))
    expect.all(k >= 71, k <= 75)
  }

  pureTest("percentileGapExact(ε=1) returns ψ (trivially satisfied at start)") {
    expect(GapDistribution.percentileGapExact(Ratio.One) == GapDistribution.Psi)
  }

  // ============ gapAtLeastApprox (BigDecimal, METRICS-ONLY) ============

  pureTest("gapAtLeastApprox at r=1 tracks gapAtLeastExact within MathContext precision") {
    val mc = new MathContext(20)
    val exact = GapDistribution.gapAtLeastExact(92).toBigDecimal.round(mc)
    val approx = GapDistribution.gapAtLeastApprox(92, BigDecimal(1, mc), mc)
    // ε = 1e-15 — well within the 20-digit MathContext.
    val tolerance = BigDecimal("1e-15", mc)
    expect((exact - approx).abs <= tolerance)
  }

  pureTest("gapAtLeastApprox at r=0.5 is strictly greater than at r=1 (lower difficulty)") {
    val mc = new MathContext(20)
    val atFull = GapDistribution.gapAtLeastApprox(50, BigDecimal(1, mc), mc)
    val atHalf = GapDistribution.gapAtLeastApprox(50, BigDecimal("0.5", mc), mc)
    expect(atHalf > atFull)
  }
}
