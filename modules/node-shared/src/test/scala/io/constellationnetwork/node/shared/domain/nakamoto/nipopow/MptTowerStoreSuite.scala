package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** §3 NIPoPoW S3 — [[MptTowerStore]] suite. Mirrors [[TowerStoreSuite]] semantics against the MPT-backed impl, asserting that the
  * `TowerStore[F]` contract holds independently of the storage backend: monotone ordinal append, per-level prefix scan, latest-at-level,
  * cumulative count, and idempotent prune.
  */
object MptTowerStoreSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  /** Build a synthetic `Hash` from a short label: emit the label's bytes as hex then right-pad to 64 chars. Ensures the hex is parseable
    * by [[io.constellationnetwork.serde.codecs.instances.HashCodec]] (lowercase hex only) regardless of the label's text content.
    */
  private def h(s: String): Hash =
    Hash(s.getBytes("UTF-8").map(b => f"${b & 0xff}%02x").mkString.padTo(64, '0').take(64))

  private def trial(level: Int, passed: Boolean): LevelTrial =
    LevelTrial(level, Ratio.Zero, Ratio.Zero, passed)

  private def trials(levels: Set[Int]): Vector[LevelTrial] =
    (1 to SuperLevelParams.SuperLevelCount).toVector.map(l => trial(l, levels.contains(l)))

  // Each test allocates its own MPT store — the MPT producer is stateful and tests must not share state.
  private def fresh(res: Res): IO[TowerStore[IO]] = {
    implicit val (hh: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO]) = res
    val _ = (sp, js) // sp/js are required by the Resource setup but unused at call site
    MptTowerStore.inMemory[IO]
  }

  test("empty store — entriesAtLevel returns Nil; latestAt returns None; cumulativeCount = 0") { res =>
    for {
      store <- fresh(res)
      e <- store.entriesAtLevel(1, ord(0))
      l <- store.latestAt(1)
      c <- store.cumulativeCount(1)
    } yield expect(e == Nil).and(expect(l.isEmpty)).and(expect(c == 0L))
  }

  test("appendAtFinality — passes at L1+L3 store both; non-passes do not") { res =>
    for {
      store <- fresh(res)
      _ <- store.appendAtFinality(ord(5), h("aa"), trials(Set(1, 3)))
      l1 <- store.entriesAtLevel(1, ord(0))
      l2 <- store.entriesAtLevel(2, ord(0))
      l3 <- store.entriesAtLevel(3, ord(0))
    } yield
      expect(l1 == List(TowerEntry(1, ord(5), h("aa")))).and(expect(l2 == Nil)).and(expect(l3 == List(TowerEntry(3, ord(5), h("aa")))))
  }

  test("latestAt — returns most-recent entry per level") { res =>
    for {
      store <- fresh(res)
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

  test("cumulativeCount — counts per-level passes only") { res =>
    for {
      store <- fresh(res)
      _ <- store.appendAtFinality(ord(100), h("p1"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(101), h("p2"), trials(Set(1, 2)))
      _ <- store.appendAtFinality(ord(102), h("p3"), trials(Set.empty))
      c1 <- store.cumulativeCount(1)
      c2 <- store.cumulativeCount(2)
      c3 <- store.cumulativeCount(3)
    } yield expect(c1 == 2L).and(expect(c2 == 1L)).and(expect(c3 == 0L))
  }

  test("entriesAtLevel — since filter is inclusive lower bound") { res =>
    for {
      store <- fresh(res)
      _ <- store.appendAtFinality(ord(50), h("e1"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(60), h("e2"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(70), h("e3"), trials(Set(1)))
      fromMid <- store.entriesAtLevel(1, ord(60))
      fromAfter <- store.entriesAtLevel(1, ord(61))
    } yield
      expect(fromMid == List(TowerEntry(1, ord(60), h("e2")), TowerEntry(1, ord(70), h("e3"))))
        .and(expect(fromAfter == List(TowerEntry(1, ord(70), h("e3")))))
  }

  test("pruneBelow — drops entries strictly below keepFrom; idempotent") { res =>
    for {
      store <- fresh(res)
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

  test("monotonic ordinal append preserves per-level ordering — MPT lex prefix scan returns ordinals in order") { res =>
    val ordinals = List(1L, 3L, 5L, 9L, 13L)
    for {
      store <- fresh(res)
      _ <- ordinals.traverse_(o => store.appendAtFinality(ord(o), h(s"o$o"), trials(Set(1))))
      l1 <- store.entriesAtLevel(1, ord(0))
    } yield expect(l1.map(_.ordinal.value.value) == ordinals)
  }

  test("interleaved-ordinal append at different levels — per-level ordering preserved independently") { res =>
    // L1 and L2 hit at different ordinals; verify each level's ordering is independent of the other's writes.
    for {
      store <- fresh(res)
      _ <- store.appendAtFinality(ord(100), h("x1"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(50), h("x2"), trials(Set(2))) // out-of-order vs L1
      _ <- store.appendAtFinality(ord(200), h("x3"), trials(Set(1, 2)))
      _ <- store.appendAtFinality(ord(75), h("x4"), trials(Set(2))) // out-of-order vs L2's first entry
      l1 <- store.entriesAtLevel(1, ord(0))
      l2 <- store.entriesAtLevel(2, ord(0))
    } yield
      expect(l1.map(_.ordinal.value.value) == List(100L, 200L))
        .and(expect(l2.map(_.ordinal.value.value) == List(50L, 75L, 200L)))
  }

  test("TowerEntryKey hex layout — fixed-width 24 chars (8 level + 16 ordinal), lex-sortable") {
    val k1 = TowerEntryKey(1, ord(0))
    val k2 = TowerEntryKey(1, ord(1))
    val k3 = TowerEntryKey(1, ord(0xffffffffL))
    val k4 = TowerEntryKey(2, ord(0))
    for {
      h1 <- TowerEntryKey.toHexF[IO](k1)
      h2 <- TowerEntryKey.toHexF[IO](k2)
      h3 <- TowerEntryKey.toHexF[IO](k3)
      h4 <- TowerEntryKey.toHexF[IO](k4)
    } yield
      expect(h1.value.length == 24)
        .and(expect(h1.value < h2.value)) // ordinal 0 < ordinal 1 within same level
        .and(expect(h2.value < h3.value))
        .and(expect(h3.value < h4.value)) // last L1 ordinal < first L2 ordinal
        .and(expect(TowerEntryKey.levelPrefix(1).value == "00000001"))
  }
}
