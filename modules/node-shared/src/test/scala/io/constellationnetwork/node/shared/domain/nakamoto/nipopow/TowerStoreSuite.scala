package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.effect.IO
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object TowerStoreSuite extends SimpleIOSuite {

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  private def h(s: String): Hash = Hash(s.padTo(64, '0').take(64))

  private def trial(level: Int, passed: Boolean): LevelTrial =
    LevelTrial(level, Ratio.Zero, Ratio.Zero, passed)

  private def trials(levels: Set[Int]): Vector[LevelTrial] =
    (1 to SuperLevelParams.SuperLevelCount).toVector.map(l => trial(l, levels.contains(l)))

  // Each test gets its own fresh store — the in-memory impl is stateful and would leak across tests
  // if we used a shared resource.
  private def fresh: IO[TowerStore[IO]] = TowerStore.inMemory[IO]

  test("empty store — entriesAtLevel returns Nil; latestAt returns None; cumulativeCount = 0") {
    for {
      store <- fresh
      e <- store.entriesAtLevel(1, ord(0))
      l <- store.latestAt(1)
      c <- store.cumulativeCount(1)
    } yield expect(e == Nil).and(expect(l.isEmpty)).and(expect(c == 0L))
  }

  test("appendAtFinality — passes at L1+L3 store both; non-passes do not") {
    for {
      store <- fresh
      _ <- store.appendAtFinality(ord(5), h("aa"), trials(Set(1, 3)))
      l1 <- store.entriesAtLevel(1, ord(0))
      l2 <- store.entriesAtLevel(2, ord(0))
      l3 <- store.entriesAtLevel(3, ord(0))
    } yield
      expect(l1 == List(TowerEntry(1, ord(5), h("aa")))).and(expect(l2 == Nil)).and(expect(l3 == List(TowerEntry(3, ord(5), h("aa")))))
  }

  test("latestAt — returns most-recent entry per level") {
    for {
      store <- fresh
      _ <- store.appendAtFinality(ord(10), h("a1"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(20), h("a2"), trials(Set(1, 2)))
      _ <- store.appendAtFinality(ord(30), h("a3"), trials(Set(2)))
      latestL1 <- store.latestAt(1)
      latestL2 <- store.latestAt(2)
      latestL3 <- store.latestAt(3)
    } yield
      expect(latestL1 == Some(TowerEntry(1, ord(20), h("a2"))))
        .and(expect(latestL2 == Some(TowerEntry(2, ord(30), h("a3")))))
        .and(expect(latestL3.isEmpty))
  }

  test("cumulativeCount — counts per-level passes only") {
    for {
      store <- fresh
      _ <- store.appendAtFinality(ord(100), h("p1"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(101), h("p2"), trials(Set(1, 2)))
      _ <- store.appendAtFinality(ord(102), h("p3"), trials(Set.empty))
      c1 <- store.cumulativeCount(1)
      c2 <- store.cumulativeCount(2)
      c3 <- store.cumulativeCount(3)
    } yield expect(c1 == 2L).and(expect(c2 == 1L)).and(expect(c3 == 0L))
  }

  test("entriesAtLevel — since filter is inclusive lower bound") {
    for {
      store <- fresh
      _ <- store.appendAtFinality(ord(50), h("e1"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(60), h("e2"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(70), h("e3"), trials(Set(1)))
      fromMid <- store.entriesAtLevel(1, ord(60))
      fromAfter <- store.entriesAtLevel(1, ord(61))
    } yield
      expect(fromMid == List(TowerEntry(1, ord(60), h("e2")), TowerEntry(1, ord(70), h("e3"))))
        .and(expect(fromAfter == List(TowerEntry(1, ord(70), h("e3")))))
  }

  test("pruneBelow — drops entries strictly below keepFrom; idempotent") {
    for {
      store <- fresh
      _ <- store.appendAtFinality(ord(10), h("z1"), trials(Set(1, 2)))
      _ <- store.appendAtFinality(ord(20), h("z2"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(30), h("z3"), trials(Set(2)))
      _ <- store.pruneBelow(ord(20))
      l1 <- store.entriesAtLevel(1, ord(0))
      l2 <- store.entriesAtLevel(2, ord(0))
      _ <- store.pruneBelow(ord(20))
      l1Again <- store.entriesAtLevel(1, ord(0))
    } yield
      expect(l1 == List(TowerEntry(1, ord(20), h("z2"))))
        .and(expect(l2 == List(TowerEntry(2, ord(30), h("z3")))))
        .and(expect(l1Again == l1))
  }

  test("monotonic ordinal append preserves per-level ordering") {
    val ordinals = List(1L, 3L, 5L, 9L, 13L)
    for {
      store <- fresh
      _ <- ordinals.traverse_(o => store.appendAtFinality(ord(o), h(s"o$o"), trials(Set(1))))
      l1 <- store.entriesAtLevel(1, ord(0))
    } yield expect(l1.map(_.ordinal.value.value) == ordinals)
  }
}
