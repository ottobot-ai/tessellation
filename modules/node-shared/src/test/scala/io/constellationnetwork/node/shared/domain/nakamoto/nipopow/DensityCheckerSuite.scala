package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._

import weaver.FunSuite

/** §3 NIPoPoW S4.4 — [[DensityChecker]] unit tests.
  *
  * Pure-function checks: `relativeError(observed, target)` exact `Ratio` arithmetic.
  */
object DensityCheckerSuite extends FunSuite {

  test("relativeError — observed == target → 0") {
    val r = DensityChecker.relativeError(Ratio(BigInt(1), BigInt(2)), Ratio(BigInt(1), BigInt(2)))
    expect(r == Ratio.Zero)
  }

  test("relativeError — observed = 2 * target → 100% (1)") {
    val r = DensityChecker.relativeError(Ratio(BigInt(1), BigInt(1)), Ratio(BigInt(1), BigInt(2)))
    expect(r == Ratio.One)
  }

  test("relativeError — observed = target * 1.05 → 5%") {
    // observed = 21/40, target = 1/2 = 20/40. Diff = 1/40. relErr = (1/40) / (20/40) = 1/20.
    val r = DensityChecker.relativeError(Ratio(BigInt(21), BigInt(40)), Ratio(BigInt(1), BigInt(2)))
    expect(r == Ratio(BigInt(1), BigInt(20)))
  }

  test("relativeError — observed below target (negative direction) → |diff|/target") {
    // observed = 1/4, target = 1/2. Diff = 1/4 (target - observed). relErr = (1/4) / (1/2) = 1/2.
    val r = DensityChecker.relativeError(Ratio(BigInt(1), BigInt(4)), Ratio(BigInt(1), BigInt(2)))
    expect(r == Ratio(BigInt(1), BigInt(2)))
  }

  test("relativeError — target == 0 → defensively returns 1 (100%)") {
    val r = DensityChecker.relativeError(Ratio(BigInt(5), BigInt(100)), Ratio.Zero)
    expect(r == Ratio.One)
  }

  test("relativeError(observed: Long, totalLevel0: Long, target) — sanity at level-µ") {
    // L1 target = 1/2. Observed: 50 hits over 100 L0 → density = 1/2. relErr = 0.
    val r = DensityChecker.relativeError(50L, 100L, Ratio(BigInt(1), BigInt(2)))
    expect(r == Ratio.Zero)
  }

  test("relativeError(Long, Long, Ratio) — totalLevel0 == 0 → defensively returns 1") {
    val r = DensityChecker.relativeError(5L, 0L, Ratio(BigInt(1), BigInt(2)))
    expect(r == Ratio.One)
  }

  test("relativeError — 5% bound boundary case") {
    // observed = 21/40, target = 20/40 = 1/2. relErr = 1/20 = exactly 5%. At the boundary.
    val r = DensityChecker.relativeError(Ratio(BigInt(21), BigInt(40)), Ratio(BigInt(1), BigInt(2)))
    expect(r <= TowerVerifier.DensityRelativeErrorBound)
  }

  test("relativeError — just over 5% bound case") {
    // observed = 22/40, target = 20/40 = 1/2. Diff = 2/40 = 1/20. relErr = (1/20) / (1/2) = 1/10 = 10%.
    val r = DensityChecker.relativeError(Ratio(BigInt(22), BigInt(40)), Ratio(BigInt(1), BigInt(2)))
    expect(r > TowerVerifier.DensityRelativeErrorBound)
  }
}
