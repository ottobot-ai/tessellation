package io.constellationnetwork.cutoff

import io.constellationnetwork.schema.SnapshotOrdinal

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

object ContiguousOrdinalCutoffSuite extends FunSuite {

  implicit class SnapshotOrdinalOps(n: Long) {
    def toSnapshotOrdinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))
  }

  private def longsOf(s: Set[SnapshotOrdinal]): Set[Long] = s.toList.map(_.value.value).toSet

  private def cutoffFrom0(depth: Int)(current: Long): Set[Long] =
    longsOf(ContiguousOrdinalCutoff.make(depth).cutoff(0L.toSnapshotOrdinal, current.toSnapshotOrdinal))

  test("keeps a contiguous window of the last `depth` ordinals") {
    // depth=5, current=100 ⇒ {96, 97, 98, 99, 100}
    expect.same(cutoffFrom0(5)(100L), (96L to 100L).toSet)
  }

  test("window is exactly `depth` wide when far from 0") {
    val kept = cutoffFrom0(64)(1000L)
    expect.same(kept.size, 64) &&
    expect.same(kept.max, 1000L) &&
    expect.same(kept.min, 1000L - 64 + 1L) &&
    // contiguous: no gaps
    expect.same(kept, (kept.min to kept.max).toSet)
  }

  test("clamps the window start at 0 when current < depth") {
    // depth=512, current=3 ⇒ {0, 1, 2, 3} (cannot go below 0)
    expect.same(cutoffFrom0(512)(3L), Set(0L, 1L, 2L, 3L))
  }

  test("current itself is always retained across a range of currents") {
    val currents = List(0L, 1L, 7L, 100L, 511L, 512L, 513L, 99999L)
    expect(currents.forall(c => cutoffFrom0(512)(c).contains(c)))
  }

  test("intersects with the cutoffOrdinal lower bound") {
    // depth=100 would keep {401..500}, but cutoffOrdinal=450 raises the floor ⇒ {450..500}
    val kept = longsOf(
      ContiguousOrdinalCutoff
        .make(100)
        .cutoff(450L.toSnapshotOrdinal, 500L.toSnapshotOrdinal)
    )
    expect.same(kept, (450L to 500L).toSet)
  }

  test("cutoffOrdinal above the window keeps only [cutoffOrdinal .. current]") {
    // depth=10 would keep {491..500}, cutoffOrdinal=495 (inside the window) ⇒ {495..500}
    val kept = longsOf(
      ContiguousOrdinalCutoff
        .make(10)
        .cutoff(495L.toSnapshotOrdinal, 500L.toSnapshotOrdinal)
    )
    expect.same(kept, (495L to 500L).toSet)
  }

  test("cutoffOrdinal == current keeps exactly {current}") {
    val kept = longsOf(
      ContiguousOrdinalCutoff
        .make(512)
        .cutoff(500L.toSnapshotOrdinal, 500L.toSnapshotOrdinal)
    )
    expect.same(kept, Set(500L))
  }

  test("cutoffOrdinal above current yields the empty set") {
    val kept = ContiguousOrdinalCutoff
      .make(512)
      .cutoff(600L.toSnapshotOrdinal, 500L.toSnapshotOrdinal)
    expect.same(kept, Set.empty[SnapshotOrdinal])
  }

  test("depth <= 0 degrades to keeping only `current`") {
    expect.same(cutoffFrom0(0)(500L), Set(500L)) &&
    expect.same(cutoffFrom0(-5)(500L), Set(500L))
  }

  test("the kept set never contains an ordinal below cutoffOrdinal") {
    val cases = List(500000L, 500123L, 500999L, 600000L)
    expect(cases.forall { current =>
      val cutoff = current - 200L
      val kept = ContiguousOrdinalCutoff
        .make(512)
        .cutoff(cutoff.toSnapshotOrdinal, current.toSnapshotOrdinal)
      kept.forall(_.value.value >= cutoff)
    })
  }
}
