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
  *   - Period 0 returns `genesisEta` without touching MPT or chain.
  *   - Period 1 follows the COMPUTED convention (#259): MPT-lookup → chain-walk → `EtaCalculation.computeEta(genesisEta, 1, …)`,
  *     byte-matching the wire / eligibility / committee eta. Only an empty chain walk falls back to `genesisEta`.
  *   - Period ≥ 1 with MPT cache hit returns the cached eta bytes.
  *   - Period ≥ 1 with MPT cache miss falls back to chain-walk and computes via `EtaCalculation.computeEta`.
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

  test("period 1 with MPT cache miss + non-empty chain walk → computed eta (COMPUTED convention, #259)") { res =>
    implicit val (h, _) = res
    // #259 unification: period 1 is NOT special-cased to genesis. With a non-empty chain walk over
    // period 0's VRF outputs it computes `EtaCalculation.computeEta(genesisEta, 1, outputs)` — the SAME
    // bytes the wire / eligibility / committee eta produces. The walk fires for source period = 0.
    val vrfOutputs = List[(Long, Array[Byte])](
      (0L, Array.fill[Byte](16)(0x01.toByte)),
      (1L, Array.fill[Byte](16)(0x02.toByte))
    )
    val expectedEta = EtaCalculation.computeEta(genesisEta, 1L, vrfOutputs.map(_._2))
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkRef <- Ref.of[IO, List[Long]](Nil)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, sp => walkRef.update(sp :: _).as(vrfOutputs))
      out <- mgr.getEta(1L)
      walked <- walkRef.get
    } yield
      expect.all(
        out.sameElements(expectedEta),
        walked == List(0L) // walk was called for source period = currentPeriod - 1 = 0
      )
  }

  test("period 1 with MPT cache miss + empty chain walk → genesisEta (warmup fall-through)") { res =>
    implicit val (h, _) = res
    // Before any period-0 VRF outputs exist the walk is empty, so period 1 falls back to genesisEta —
    // matching `SnapshotLeaderLoop`'s empty-`vrfOutputsForPeriod(0)` branch. The walk IS engaged
    // (source period 0), unlike period 0 which short-circuits before touching MPT/chain.
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkRef <- Ref.of[IO, List[Long]](Nil)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, sp => walkRef.update(sp :: _).as(List.empty[(Long, Array[Byte])]))
      out <- mgr.getEta(1L)
      walked <- walkRef.get
    } yield expect.all(out.sameElements(genesisEta), walked == List(0L))
  }

  test("period 1 with MPT cache hit returns the cached eta bytes") { res =>
    implicit val (h, _) = res
    // The MPT boundary record for period 1 (written by GSAM at the period-0 closing boundary, ord R-1)
    // is now authoritative for period 1 just like any higher period.
    val etaBytes = Array.fill[Byte](32)(0x37.toByte)
    val cached = HistoricalStakeSnapshot(StakeDistribution.Empty, hashOf(etaBytes))
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap(EtaPeriod(1L) -> cached))
      walkRef <- Ref.of[IO, List[Long]](Nil)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, p => walkRef.update(p :: _).as(List.empty[(Long, Array[Byte])]))
      out <- mgr.getEta(1L)
      walked <- walkRef.get
    } yield expect.all(out.sameElements(etaBytes), walked.isEmpty)
  }

  // #259 byte-exactness: getEta(1) MUST equal SnapshotLeaderLoop's wire / eligibility eta at period 1.
  // The leader computes `eta = if (currentPeriod <= 0) genesisEta else { val o = vrfOutputsForPeriod(0);
  // if (o.nonEmpty) computeEta(genesisEta, 1, o.map(_._2)) else genesisEta }`. With the SAME chain-walk
  // source and the SAME genesis-on-empty fallback, getEta(1) reproduces those exact bytes — so the
  // producer's MPT boundary record (etaForPeriod=getEta) == committee draw == wire == follower-adopt.
  test("#259 byte-exactness: getEta(1) == SnapshotLeaderLoop wire eta at period 1 (non-empty walk)") { res =>
    implicit val (h, _) = res
    val period0Outputs = List[(Long, Array[Byte])](
      (0L, Array.fill[Byte](16)(0xa1.toByte)),
      (1L, Array.fill[Byte](16)(0xb2.toByte)),
      (2L, Array.fill[Byte](16)(0xc3.toByte))
    )
    // Mirror of SnapshotLeaderLoop.scala:572-583 at currentPeriod=1.
    val leaderWireEta: Array[Byte] =
      if (period0Outputs.nonEmpty) EtaCalculation.computeEta(genesisEta, 1L, period0Outputs.map(_._2)) else genesisEta
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      reader = stubReader(mptRef)
      // chainWalkFallback(period-1) is invoked with source period 0; return period-0 outputs.
      mgr <- EtaStateManager.make[IO](genesisEta, reader, sp => if (sp == 0L) IO.pure(period0Outputs) else IO.pure(Nil))
      getEta1 <- mgr.getEta(1L)
    } yield expect(getEta1.sameElements(leaderWireEta))
  }

  test("#259 byte-exactness: getEta(1) == SnapshotLeaderLoop wire eta at period 1 (empty walk → genesis)") { res =>
    implicit val (h, _) = res
    // Mirror of SnapshotLeaderLoop's empty-`vrfOutputsForPeriod(0)` branch: both yield genesisEta.
    val leaderWireEta: Array[Byte] = genesisEta // empty period-0 outputs branch
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, _ => IO.pure(List.empty[(Long, Array[Byte])]))
      getEta1 <- mgr.getEta(1L)
    } yield expect(getEta1.sameElements(leaderWireEta))
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
