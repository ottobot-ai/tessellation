package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.{IO, Ref, Resource}

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, HistoricalStakeSnapshot, StakeDistribution}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash

import weaver.MutableIOSuite

/** Spec assertions for [[EtaStateManager]] — Path 1 of the heap-leak workstream.
  *
  *   - Periods 0 and 1 return `genesisEta` without touching MPT or chain.
  *   - Period ≥ 2 with MPT cache hit returns the cached eta bytes.
  *   - Period ≥ 2 with MPT cache miss falls back to chain-walk and computes via `EtaCalculation.computeEta`.
  *   - Chain-walk recompute is memoized in-process so repeated `getEta(N)` calls on a cache-miss path don't re-walk.
  *   - Empty chain walk (no VRF outputs) returns `genesisEta`.
  */
object EtaStateManagerSuite extends MutableIOSuite {

  type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, j)

  // In-memory HistoricalStakeReader stub. Tests pre-populate `Ref[F, SortedMap[EtaPeriod, HistoricalStakeSnapshot]]`
  // and the stub looks up against that.
  private def stubReader(
    state: Ref[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]]
  ): HistoricalStakeReader[IO] = new HistoricalStakeReader[IO] {
    def lookup(period: EtaPeriod)(implicit hasher: Hasher[IO]): IO[Option[HistoricalStakeSnapshot]] =
      state.get.map(_.get(period))
  }

  // Helper: build a `Hash` from a 32-byte array.
  private def hashOf(bytes: Array[Byte]): Hash = {
    require(bytes.length == 32)
    Hash(bytes.map(b => f"$b%02x").mkString)
  }

  private val genesisEta: Array[Byte] = Array.fill[Byte](32)(0x00.toByte)

  test("period 0 → genesisEta (no MPT, no chain)") { res =>
    implicit val (h, _) = res
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkRef <- Ref.of[IO, List[Long]](Nil) // record which period(s) were walked
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, p => walkRef.update(p :: _).as(List.empty[(Long, Array[Byte])]))
      out <- mgr.getEta(0L)
      walked <- walkRef.get
    } yield expect.all(out.sameElements(genesisEta), walked.isEmpty)
  }

  test("period 1 → genesisEta (no MPT, no chain)") { res =>
    implicit val (h, _) = res
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkRef <- Ref.of[IO, List[Long]](Nil)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, p => walkRef.update(p :: _).as(List.empty[(Long, Array[Byte])]))
      out <- mgr.getEta(1L)
      walked <- walkRef.get
    } yield expect.all(out.sameElements(genesisEta), walked.isEmpty)
  }

  test("period 2 with MPT cache hit returns the cached eta bytes") { res =>
    implicit val (h, _) = res
    val etaBytes = Array.fill[Byte](32)(0x42.toByte)
    val etaHash = hashOf(etaBytes)
    val cached = HistoricalStakeSnapshot(StakeDistribution.Empty, etaHash)
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap(EtaPeriod(2L) -> cached))
      walkRef <- Ref.of[IO, List[Long]](Nil)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, p => walkRef.update(p :: _).as(List.empty[(Long, Array[Byte])]))
      out <- mgr.getEta(2L)
      walked <- walkRef.get
    } yield
      expect.all(
        out.sameElements(etaBytes), // cache hit, exact bytes returned
        walked.isEmpty // walk was NOT engaged
      )
  }

  test("period 2 with MPT cache miss + non-empty chain walk → computed eta via EtaCalculation.computeEta") { res =>
    implicit val (h, _) = res
    val vrfOutputs = List[(Long, Array[Byte])](
      (0L, Array.fill[Byte](16)(0x01.toByte)),
      (1L, Array.fill[Byte](16)(0x02.toByte))
    )
    val expectedEta = EtaCalculation.computeEta(genesisEta, 2L, vrfOutputs.map(_._2))
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkRef <- Ref.of[IO, List[Long]](Nil)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, sp => walkRef.update(sp :: _).as(vrfOutputs))
      out <- mgr.getEta(2L)
      walked <- walkRef.get
    } yield
      expect.all(
        out.sameElements(expectedEta),
        walked == List(1L) // walk was called for source period = currentPeriod - 1 = 1
      )
  }

  test("period 2 with MPT cache miss + empty chain walk → genesisEta (degenerate fallback)") { res =>
    implicit val (h, _) = res
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkRef <- Ref.of[IO, List[Long]](Nil)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, sp => walkRef.update(sp :: _).as(List.empty[(Long, Array[Byte])]))
      out <- mgr.getEta(3L)
      walked <- walkRef.get
    } yield expect.all(out.sameElements(genesisEta), walked == List(2L))
  }

  test("MPT cache miss → repeated getEta(period) walks only once (in-process memoization)") { res =>
    implicit val (h, _) = res
    val vrfOutputs = List[(Long, Array[Byte])]((0L, Array.fill[Byte](16)(0x11.toByte)))
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkCount <- Ref.of[IO, Int](0)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, _ => walkCount.update(_ + 1).as(vrfOutputs))
      out1 <- mgr.getEta(2L)
      out2 <- mgr.getEta(2L)
      out3 <- mgr.getEta(2L)
      count <- walkCount.get
    } yield
      expect.all(
        out1.sameElements(out2),
        out2.sameElements(out3),
        count == 1 // walk called once; subsequent calls hit the in-process cache
      )
  }

  test("eta cache priority: MPT hit beats in-process walk cache (MPT is authoritative)") { res =>
    implicit val (h, _) = res
    val walkVrf = List[(Long, Array[Byte])]((0L, Array.fill[Byte](16)(0x99.toByte)))
    val mptEtaBytes = Array.fill[Byte](32)(0x55.toByte)
    val mptEntry = HistoricalStakeSnapshot(StakeDistribution.Empty, hashOf(mptEtaBytes))
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkCount <- Ref.of[IO, Int](0)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, _ => walkCount.update(_ + 1).as(walkVrf))
      // First call: MPT miss, walk-cache populated with `EtaCalculation.computeEta(...)` over walkVrf.
      _ <- mgr.getEta(2L)
      // Now populate MPT: subsequent calls must return the MPT-pinned eta, not the prior walk-cache.
      _ <- mptRef.update(_.updated(EtaPeriod(2L), mptEntry))
      out <- mgr.getEta(2L)
    } yield expect.same(true, out.sameElements(mptEtaBytes))
  }
}
