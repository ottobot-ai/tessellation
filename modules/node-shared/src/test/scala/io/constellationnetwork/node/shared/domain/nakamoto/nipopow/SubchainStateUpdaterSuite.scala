package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import io.constellationnetwork.numerics.Ratio

import weaver.FunSuite

object SubchainStateUpdaterSuite extends FunSuite {

  private def trial(level: Int, passed: Boolean): LevelTrial =
    LevelTrial(level, tau = Ratio.Zero, effectiveThreshold = Ratio.Zero, passed = passed)

  private def trials(passes: Vector[Boolean]): Vector[LevelTrial] =
    passes.zipWithIndex.map { case (p, i) => trial(i + 1, p) }

  private val allFail: Vector[LevelTrial] =
    trials(Vector.fill(SuperLevelParams.SuperLevelCount)(false))

  private val allPass: Vector[LevelTrial] =
    trials(Vector.fill(SuperLevelParams.SuperLevelCount)(true))

  test("Genesis state has all-zero counts") {
    expect(SubchainState.Genesis.levelCounts == Vector.fill(SuperLevelParams.SuperLevelCount)(0L))
  }

  test("countAt out-of-range µ returns 0") {
    expect(SubchainState.Genesis.countAt(0) == 0L)
      .and(expect(SubchainState.Genesis.countAt(SuperLevelParams.SuperLevelCount + 1) == 0L))
      .and(expect(SubchainState.Genesis.countAt(-1) == 0L))
  }

  test("updateFrom — all-fail leaves counts unchanged") {
    val updated = SubchainStateUpdater.updateFrom(SubchainState.Genesis, allFail)
    expect(updated == SubchainState.Genesis)
  }

  test("updateFrom — all-pass increments every count by 1") {
    val updated = SubchainStateUpdater.updateFrom(SubchainState.Genesis, allPass)
    expect(updated.levelCounts == Vector.fill(SuperLevelParams.SuperLevelCount)(1L))
  }

  test("updateFrom — only level-3 passes → only level-3 count incremented") {
    val passes = Vector.tabulate(SuperLevelParams.SuperLevelCount)(i => i == 2) // level 3 = index 2
    val updated = SubchainStateUpdater.updateFrom(SubchainState.Genesis, trials(passes))
    val expected = Vector.tabulate(SuperLevelParams.SuperLevelCount)(i => if (i == 2) 1L else 0L)
    expect(updated.levelCounts == expected)
  }

  test("updateFrom — independence: non-nested level passes (e.g. L3 passes, L1 fails)") {
    // L1 fails, L3 passes — this is independent NOT-nested rarity. Per paper §6.
    val passes = Vector.tabulate(SuperLevelParams.SuperLevelCount)(i => i == 2 || i == 5) // L3 and L6
    val updated = SubchainStateUpdater.updateFrom(SubchainState.Genesis, trials(passes))
    expect(updated.countAt(1) == 0L)
      .and(expect(updated.countAt(3) == 1L))
      .and(expect(updated.countAt(6) == 1L))
      .and(expect(updated.countAt(9) == 0L))
  }

  test("updateFrom — composes: applying 5 all-pass updates → all counts = 5") {
    val s5 = (1 to 5).foldLeft(SubchainState.Genesis)((s, _) => SubchainStateUpdater.updateFrom(s, allPass))
    expect(s5.levelCounts == Vector.fill(SuperLevelParams.SuperLevelCount)(5L))
  }

  test("updateFrom — composes: 10 mixed updates → exact counts") {
    // Snapshot 1: L1, L2 pass. Snapshot 2: L1 pass. Snapshot 3: L9 pass. Rest fail.
    val passesPerSnap = Vector(
      Vector.tabulate(SuperLevelParams.SuperLevelCount)(i => i == 0 || i == 1),
      Vector.tabulate(SuperLevelParams.SuperLevelCount)(_ == 0),
      Vector.tabulate(SuperLevelParams.SuperLevelCount)(_ == 8)
    )
    val finalState = passesPerSnap.foldLeft(SubchainState.Genesis) { (s, passes) =>
      SubchainStateUpdater.updateFrom(s, trials(passes))
    }
    val expected = Vector(2L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, 1L)
    expect(finalState.levelCounts == expected)
  }

  test("updateFrom rejects wrong-size trial vector") {
    val tooShort = trials(Vector.fill(SuperLevelParams.SuperLevelCount - 1)(true))
    expect(scala.util.Try(SubchainStateUpdater.updateFrom(SubchainState.Genesis, tooShort)).isFailure)
  }

  test("updateFrom rejects out-of-order trials") {
    // Same passes, but trial level numbers shuffled to [2, 1, 3, 4, ...]
    val swapped = trials(Vector.fill(SuperLevelParams.SuperLevelCount)(true))
    val outOfOrder = swapped.updated(0, swapped(1).copy(level = 2)).updated(1, swapped(0).copy(level = 1))
    expect(scala.util.Try(SubchainStateUpdater.updateFrom(SubchainState.Genesis, outOfOrder)).isFailure)
  }
}
