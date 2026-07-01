package io.constellationnetwork.node.shared.domain.nakamoto

import java.math.MathContext

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.nakamoto.LddConfigFixture

import weaver.SimpleIOSuite

object GapDistributionSuite extends SimpleIOSuite {

  // Production-aligned LDD curve (γ=16, ψ=1, fA=1/2, fB=1/20). The gap distribution is a pure function of this config —
  // there is no hardcoded curve in `GapDistribution` anymore, so the analysis tracks whatever the chain runs.
  private val cfg = LddConfigFixture.production

  // ============ thresholdAtGap ============

  pureTest("f(δ < ψ) is zero (dormant regime)") {
    expect(GapDistribution.thresholdAtGap(0, cfg) == Ratio.Zero)
  }

  pureTest("f(δ = ψ) is zero (ramp starts at zero)") {
    // f(1) = (1-1)/(16-1) * 1/2 = 0
    expect(GapDistribution.thresholdAtGap(cfg.offset, cfg) == Ratio.Zero)
  }

  pureTest("f(δ) in ramp regime matches (δ-ψ)/(γ-ψ) · fA") {
    // f(8) = (8-1)/(16-1) * 1/2 = 7/15 * 1/2 = 7/30
    expect(GapDistribution.thresholdAtGap(8, cfg) == Ratio(7, 30))
  }

  pureTest("f(δ ≥ γ) is fB = 1/20") {
    expect.all(
      GapDistribution.thresholdAtGap(cfg.lddCutoff, cfg) == cfg.baselineDifficulty,
      GapDistribution.thresholdAtGap(50, cfg) == cfg.baselineDifficulty,
      GapDistribution.thresholdAtGap(1000, cfg) == cfg.baselineDifficulty
    )
  }

  // ============ gapAtLeastExact (r = 1) ============

  pureTest("gapAtLeastExact(K ≤ 1) is 1 (degenerate)") {
    expect.all(
      GapDistribution.gapAtLeastExact(0, cfg) == Ratio.One,
      GapDistribution.gapAtLeastExact(1, cfg) == Ratio.One
    )
  }

  pureTest("gapAtLeastExact is monotone non-increasing") {
    val seq = (1 to 30).map(GapDistribution.gapAtLeastExact(_, cfg))
    expect(seq.sliding(2).forall { case Seq(a, b) => a >= b; case _ => true })
  }

  pureTest("gapAtLeastExact crosses the 1/1000 ε at the γ=16 99.9th-percentile cliff (K=68)") {
    // Under LDD with ψ=1, γ=16, fA=1/2, fB=1/20, r=1, P(gap ≥ K) crosses 1e-3 between K=67 (≈1.03e-3) and
    // K=68 (≈9.82e-4). Exact by construction of `gapAtLeastExact` (verified with rational arithmetic). Note the
    // γ=16 cliff (68) is EARLIER than the old γ=15 cliff (73): at γ=16 the ramp keeps a high hazard through δ=15
    // (f=7/15) instead of dropping to baseline, pulling the tail in.
    expect.all(
      GapDistribution.gapAtLeastExact(67, cfg) > Ratio(1, 1000),
      GapDistribution.gapAtLeastExact(68, cfg) <= Ratio(1, 1000)
    )
  }

  // ============ percentileGapExact ============

  pureTest("percentileGapExact(ε=1/1000) returns the 68-slot cliff") {
    // ±2-slot tolerance to absorb any Ratio rounding at the ramp/baseline boundary without pinning the exact
    // integer in two places.
    val k = GapDistribution.percentileGapExact(Ratio(1, 1000), cfg)
    expect.all(k >= 66, k <= 70)
  }

  pureTest("percentileGapExact(ε=1) returns ψ (trivially satisfied at start)") {
    expect(GapDistribution.percentileGapExact(Ratio.One, cfg) == cfg.offset)
  }

  // ============ gapAtLeastApprox (BigDecimal, METRICS-ONLY) ============

  pureTest("gapAtLeastApprox at r=1 tracks gapAtLeastExact within MathContext precision") {
    val mc = new MathContext(20)
    val exact = GapDistribution.gapAtLeastExact(92, cfg).toBigDecimal.round(mc)
    val approx = GapDistribution.gapAtLeastApprox(92, BigDecimal(1, mc), mc, cfg)
    // ε = 1e-15 — well within the 20-digit MathContext.
    val tolerance = BigDecimal("1e-15", mc)
    expect((exact - approx).abs <= tolerance)
  }

  pureTest("gapAtLeastApprox at r=0.5 is strictly greater than at r=1 (lower difficulty)") {
    val mc = new MathContext(20)
    val atFull = GapDistribution.gapAtLeastApprox(50, BigDecimal(1, mc), mc, cfg)
    val atHalf = GapDistribution.gapAtLeastApprox(50, BigDecimal("0.5", mc), mc, cfg)
    expect(atHalf > atFull)
  }
}
