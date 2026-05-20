package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, StakeDistribution}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{immutableCodec => stakeDistributionImmutable}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Spec assertions for the §G3 MPT-primary historical-stake-snapshots reader.
  *
  * The reader is a thin point-read against `GlobalStateKey.historicalStakeSnapshotsKey[F](period)`.
  * The boundary writer (`GSAM.accept()` via `AcceptanceMptStateChanges.applyStateChanges`) lands one
  * MPT entry per stored period from the GSI's `historicalStakeSnapshots` map. The MPT projection in
  * `GlobalStateConverter.toAllStateKeyValueBytes` produces byte-identical entries (verified by
  * `GsamWritePathParitySuite`); this suite asserts that the reader observes those entries.
  *
  * Covers the three properties the migration depends on:
  *
  *   1. Lookup of a non-existent period → `None` (warmup / pre-boundary semantics).
  *   2. Lookup of an existing period after a boundary write → `Some(distribution)` byte-equal to the
  *      GSI snapshot that landed on the writer side.
  *   3. Byte-determinism: two independent MPT builds with the same input agree on every lookup —
  *      the core property §G3 buys over GSI iteration (closes the cross-node drift class on the
  *      historical-distribution read path).
  */
object HistoricalStakeReaderSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  // ---- helpers --------------------------------------------------------------

  private def pid(label: String): PeerId =
    PeerId(Hex(label.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  private def dist(stakes: (PeerId, BigInt)*): StakeDistribution =
    StakeDistribution(SortedMap(stakes: _*))

  /** Build an MPT store seeded with `historical` entries by writing each `(period, distribution)`
    * directly via the same key derivation the GSAM boundary writer uses
    * (`AcceptanceMptStateChanges.applyStateChanges` → `historicalStakeSnapshotsKey[F](period)` →
    * `mpt.insert[StakeDistribution]`). The codec is the canonical `StakeDistributionCodec` shared by
    * writer and reader.
    *
    * Why direct writes, not `syncFromGlobalSnapshotInfo`. The bootstrap-projector at
    * `GlobalStateConverter.toAllStateKeyValueBytes` includes the historical-stake-snapshots field, but
    * the in-place writer `syncFromGlobalSnapshotInfo` does not — that path is intended to seed/reset
    * the live-state partitions and the historical-snapshots partition is incrementally written by
    * GSAM at every boundary ordinal. Writing here directly through `store.insert[StakeDistribution]`
    * mirrors the boundary-time path exercised in production by `AcceptanceMptStateChanges`.
    */
  private def mkStore(
    historical: SortedMap[EtaPeriod, StakeDistribution]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] = {
    val _ = stakeDistributionImmutable
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      entries <- historical.toList.traverse {
        case (period, dist) => GlobalStateKey.historicalStakeSnapshotsKey[IO](period).map(_ -> dist)
      }.map(_.toMap)
      _ <- store.insert[StakeDistribution](entries)
      _ <- store.build(SnapshotOrdinal(NonNegLong(1L))).void
    } yield store
  }

  // ---- core spec assertions ------------------------------------------------

  test("lookup non-existent period → None") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore(SortedMap.empty)
      reader = GlobalStateReader.fromMptStore[IO](store)
      hist = HistoricalStakeReader.make[IO](reader)
      out <- hist.lookup(EtaPeriod(0L))
    } yield expect(out.isEmpty)
  }

  test("lookup existing period after boundary write → Some(distribution) byte-equal to GSI") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val nB = pid("node-B")
    val period = EtaPeriod(5L)
    val expected = dist(nA -> BigInt(1000), nB -> BigInt(500))
    for {
      store <- mkStore(SortedMap(period -> expected))
      reader = GlobalStateReader.fromMptStore[IO](store)
      hist = HistoricalStakeReader.make[IO](reader)
      out <- hist.lookup(period)
    } yield expect.same(Some(expected), out)
  }

  test("lookup wrong period returns None when other periods are present") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val present = EtaPeriod(3L)
    val absent = EtaPeriod(4L)
    for {
      store <- mkStore(SortedMap(present -> dist(nA -> BigInt(100))))
      reader = GlobalStateReader.fromMptStore[IO](store)
      hist = HistoricalStakeReader.make[IO](reader)
      hit <- hist.lookup(present)
      miss <- hist.lookup(absent)
    } yield
      expect.same(Some(dist(nA -> BigInt(100))), hit) &&
        expect(miss.isEmpty)
  }

  test("multiple periods retained — each lookup hits the correct one") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val nB = pid("node-B")
    val nC = pid("node-C")
    // Mirrors the production retention (last 4 periods). Each period's value differs in both peer
    // membership and amounts so a stray lookup that returned a different period's bytes would fail
    // the equality assertion below.
    val p1 = EtaPeriod(7L) -> dist(nA -> BigInt(100))
    val p2 = EtaPeriod(8L) -> dist(nA -> BigInt(150), nB -> BigInt(200))
    val p3 = EtaPeriod(9L) -> dist(nB -> BigInt(300), nC -> BigInt(50))
    val p4 = EtaPeriod(10L) -> dist(nA -> BigInt(500), nB -> BigInt(500), nC -> BigInt(500))
    val all = SortedMap(p1, p2, p3, p4)
    for {
      store <- mkStore(all)
      reader = GlobalStateReader.fromMptStore[IO](store)
      hist = HistoricalStakeReader.make[IO](reader)
      r1 <- hist.lookup(p1._1)
      r2 <- hist.lookup(p2._1)
      r3 <- hist.lookup(p3._1)
      r4 <- hist.lookup(p4._1)
    } yield
      expect.same(Some(p1._2), r1) &&
        expect.same(Some(p2._2), r2) &&
        expect.same(Some(p3._2), r3) &&
        expect.same(Some(p4._2), r4)
  }

  test("byte-determinism — two independent MPT builds agree on the same lookup") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val nB = pid("node-B")
    val nC = pid("node-C")
    val period = EtaPeriod(11L)
    // Determinism is the core property §G3 buys: under GSI iteration, two nodes' in-memory map
    // walks could produce divergent intermediate state on identical inputs; under MPT point read
    // the key is `Hash(period.value.toString)` and the value is scodec-encoded `StakeDistribution`,
    // both byte-identical across independent builds.
    val expected = dist(nA -> BigInt(100), nB -> BigInt(200), nC -> BigInt(50))
    val historical = SortedMap(period -> expected)
    for {
      store1 <- mkStore(historical)
      store2 <- mkStore(historical)
      r1 <- HistoricalStakeReader.make[IO](GlobalStateReader.fromMptStore[IO](store1)).lookup(period)
      r2 <- HistoricalStakeReader.make[IO](GlobalStateReader.fromMptStore[IO](store2)).lookup(period)
    } yield expect.same(r1, r2) && expect.same(Some(expected), r1)
  }

  test("byte-determinism across periods — independent builds agree on every period") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val nB = pid("node-B")
    // Cross-node determinism over a multi-period set; mirrors what `StakeRegistry.stakeWeightedMpt`
    // would observe across the live retention window.
    val historical = SortedMap(
      EtaPeriod(0L) -> dist(nA -> BigInt(50)),
      EtaPeriod(1L) -> dist(nA -> BigInt(100), nB -> BigInt(100)),
      EtaPeriod(2L) -> dist(nB -> BigInt(250))
    )
    for {
      store1 <- mkStore(historical)
      store2 <- mkStore(historical)
      h1 = HistoricalStakeReader.make[IO](GlobalStateReader.fromMptStore[IO](store1))
      h2 = HistoricalStakeReader.make[IO](GlobalStateReader.fromMptStore[IO](store2))
      r1 <- historical.keys.toList.traverse(p => h1.lookup(p).map(p -> _))
      r2 <- historical.keys.toList.traverse(p => h2.lookup(p).map(p -> _))
    } yield expect.same(r1, r2)
  }

  test("negative period lookup → None (warmup-bootstrap window edge case)") { res =>
    implicit val (h, _, js) = res
    // `EtaPeriod` permits negatives during `currentEtaPeriod - 2` queries before period 2. The reader
    // must return None for those (the registry falls through to the live aggregate). Verified here so
    // the migration doesn't accidentally introduce a synthetic key for negative periods.
    for {
      store <- mkStore(SortedMap(EtaPeriod(0L) -> dist(pid("n") -> BigInt(1))))
      reader = GlobalStateReader.fromMptStore[IO](store)
      hist = HistoricalStakeReader.make[IO](reader)
      out <- hist.lookup(EtaPeriod(-1L))
    } yield expect(out.isEmpty)
  }
}
