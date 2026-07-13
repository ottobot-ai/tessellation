package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.{IO, Ref, Resource}

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager.{EtaSourceRange, EtaSourceUnavailable}
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, HistoricalStakeSnapshot, StakeDistribution}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash

import weaver.MutableIOSuite

/** Spec assertions for [[EtaStateManager]] — Path 1 of the heap-leak workstream.
  *
  *   - Periods 0 AND 1 short-circuit to `EtaCalculation.bootstrapEta(genesisEta, period)` (= `computeEta(genesisEta, period, List.empty)`)
  *     without touching MPT or chain — the Cardano/Praos bootstrap convention (fold NO VRF outputs, distinct per period). Period 1 is
  *     INDEPENDENT of any period-0 VRF outputs and ignores any MPT entry, so the first eta rotation (period 0 → 1) cannot fork.
  *   - Period ≥ 2 with MPT cache hit returns the cached eta bytes.
  *   - Period ≥ 2 with MPT cache miss falls back to chain-walk and computes via `EtaCalculation.computeEta`.
  *   - Chain-walk recompute is memoized in-process so repeated `getEta(N)` calls on a cache-miss path don't re-walk.
  *   - Incomplete or empty chain ranges for period ≥ 2 fail closed and are never cached as eta.
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

  private def complete(outputs: List[(Long, Array[Byte])]): EtaSourceRange = EtaSourceRange.Complete(outputs)
  private def incomplete(outputs: List[(Long, Array[Byte])] = Nil): EtaSourceRange = EtaSourceRange.Incomplete(outputs)

  test("period 0 → bootstrapEta(genesisEta, 0) (no MPT, no chain)") { res =>
    implicit val (h, _) = res
    // Cardano/Praos bootstrap: period 0 short-circuits to bootstrapEta(genesisEta, 0) without touching
    // MPT or the chain walk — and it is NOT raw genesisEta.
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkRef <- Ref.of[IO, List[Long]](Nil) // record which period(s) were walked
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, (p, _) => walkRef.update(p :: _).as(incomplete()))
      out <- mgr.getEta(0L)
      walked <- walkRef.get
    } yield expect.all(out.sameElements(EtaCalculation.bootstrapEta(genesisEta, 0L)), walked.isEmpty)
  }

  test("period 1 → bootstrapEta(genesisEta, 1) regardless of chain walk (no walk engaged)") { res =>
    implicit val (h, _) = res
    // Cardano/Praos bootstrap: period 1 short-circuits to bootstrapEta(genesisEta, 1) — it folds NO VRF
    // outputs, so the chain walk is NEVER engaged even when it WOULD return non-empty period-0 outputs.
    val vrfOutputs = List[(Long, Array[Byte])](
      (0L, Array.fill[Byte](16)(0x01.toByte)),
      (1L, Array.fill[Byte](16)(0x02.toByte))
    )
    val expectedEta = EtaCalculation.bootstrapEta(genesisEta, 1L)
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkRef <- Ref.of[IO, List[Long]](Nil)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, (sp, _) => walkRef.update(sp :: _).as(complete(vrfOutputs)))
      out <- mgr.getEta(1L)
      walked <- walkRef.get
    } yield
      expect.all(
        out.sameElements(expectedEta),
        !out.sameElements(genesisEta), // bootstrapEta(g,1) is NOT raw genesis
        walked.isEmpty // walk NOT engaged — period 1 is genesis-derivable, no VRF dependency
      )
  }

  test("period 1 is INDEPENDENT of period-0 chain walk (distinct etas yield same bootstrapEta)") { res =>
    implicit val (h, _) = res
    // The key bootstrap invariant: period 1 does not fold period-0's (still-unsettled) VRF outputs, so
    // an empty walk and a non-empty walk both yield bootstrapEta(genesisEta, 1) — the first rotation
    // (period 0 → 1) cannot fork on disagreement about period 0's outputs.
    val nonEmptyWalk = List[(Long, Array[Byte])]((0L, Array.fill[Byte](16)(0x05.toByte)))
    val expectedEta = EtaCalculation.bootstrapEta(genesisEta, 1L)
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      reader = stubReader(mptRef)
      mgrEmpty <- EtaStateManager.make[IO](genesisEta, reader, (_, _) => IO.pure(incomplete()))
      mgrFull <- EtaStateManager.make[IO](genesisEta, reader, (_, _) => IO.pure(complete(nonEmptyWalk)))
      outEmpty <- mgrEmpty.getEta(1L)
      outFull <- mgrFull.getEta(1L)
    } yield expect.all(outEmpty.sameElements(expectedEta), outFull.sameElements(expectedEta), outEmpty.sameElements(outFull))
  }

  test("period 1 ignores any MPT cache entry (bootstrap short-circuit precedes MPT lookup)") { res =>
    implicit val (h, _) = res
    // Under the bootstrap convention period 1 is genesis-derivable and short-circuits BEFORE the MPT
    // lookup — so even a populated period-1 MPT entry is ignored; getEta(1) is always bootstrapEta(g,1).
    val mptEtaBytes = Array.fill[Byte](32)(0x37.toByte)
    val cached = HistoricalStakeSnapshot(StakeDistribution.Empty, hashOf(mptEtaBytes))
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap(EtaPeriod(1L) -> cached))
      walkRef <- Ref.of[IO, List[Long]](Nil)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, (p, _) => walkRef.update(p :: _).as(incomplete()))
      out <- mgr.getEta(1L)
      walked <- walkRef.get
    } yield
      expect.all(
        out.sameElements(EtaCalculation.bootstrapEta(genesisEta, 1L)),
        !out.sameElements(mptEtaBytes), // the MPT entry is NOT consulted at period 1
        walked.isEmpty
      )
  }

  // Bootstrap byte-exactness: getEta(1) MUST equal SnapshotLeaderLoop's wire / eligibility eta at period 1.
  // Under the Cardano/Praos bootstrap convention the leader keys on `currentPeriod <= 1` and routes period 1
  // through `EtaCalculation.bootstrapEta(genesisEta, 1)` — folding NO VRF outputs — so getEta(1) reproduces
  // those exact bytes regardless of the chain walk: producer record == committee draw == wire == follower-adopt.
  test("bootstrap byte-exactness: getEta(1) == SnapshotLeaderLoop wire eta at period 1 (non-empty walk)") { res =>
    implicit val (h, _) = res
    val period0Outputs = List[(Long, Array[Byte])](
      (0L, Array.fill[Byte](16)(0xa1.toByte)),
      (1L, Array.fill[Byte](16)(0xb2.toByte)),
      (2L, Array.fill[Byte](16)(0xc3.toByte))
    )
    // Mirror of SnapshotLeaderLoop's `currentPeriod <= 1` bootstrap branch: bootstrapEta(genesisEta, 1).
    val leaderWireEta: Array[Byte] = EtaCalculation.bootstrapEta(genesisEta, 1L)
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      reader = stubReader(mptRef)
      // Even with non-empty period-0 outputs available, period 1 ignores them under bootstrap.
      mgr <- EtaStateManager.make[IO](genesisEta, reader, (sp, _) => IO.pure(if (sp == 0L) complete(period0Outputs) else incomplete()))
      getEta1 <- mgr.getEta(1L)
    } yield expect(getEta1.sameElements(leaderWireEta))
  }

  test("bootstrap byte-exactness: getEta(1) == SnapshotLeaderLoop wire eta at period 1 (empty walk)") { res =>
    implicit val (h, _) = res
    // Both yield bootstrapEta(genesisEta, 1) — the leader's `currentPeriod <= 1` branch is walk-independent.
    val leaderWireEta: Array[Byte] = EtaCalculation.bootstrapEta(genesisEta, 1L)
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, (_, _) => IO.pure(incomplete()))
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
      mgr <- EtaStateManager.make[IO](genesisEta, reader, (p, _) => walkRef.update(p :: _).as(incomplete()))
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
      mgr <- EtaStateManager.make[IO](genesisEta, reader, (sp, _) => walkRef.update(sp :: _).as(complete(vrfOutputs)))
      out <- mgr.getEta(2L)
      walked <- walkRef.get
    } yield
      expect.all(
        out.sameElements(expectedEta),
        walked == List(1L) // walk was called for source period = currentPeriod - 1 = 1
      )
  }

  test("period >=2 with MPT cache miss + incomplete empty chain range fails closed") { res =>
    implicit val (h, _) = res
    // A lagging node must defer; substituting bootstrap eta would diverge from a caught-up node.
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkRef <- Ref.of[IO, List[Long]](Nil)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, (sp, _) => walkRef.update(sp :: _).as(incomplete()))
      out <- mgr.getEta(3L).attempt
      walked <- walkRef.get
    } yield
      expect.all(
        out.left.exists(_.isInstanceOf[EtaSourceUnavailable]),
        walked == List(2L)
      )
  }

  test("period >=2 rejects a nonempty partial prefix and a complete empty range") { res =>
    implicit val (h, _) = res
    val partial = List[(Long, Array[Byte])]((7L, Array.fill[Byte](16)(0x71.toByte)))
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      reader = stubReader(mptRef)
      partialMgr <- EtaStateManager.make[IO](genesisEta, reader, (_, _) => IO.pure(incomplete(partial)))
      emptyMgr <- EtaStateManager.make[IO](genesisEta, reader, (_, _) => IO.pure(complete(Nil)))
      partialResult <- partialMgr.getEta(2L).attempt
      emptyResult <- emptyMgr.getEta(2L).attempt
    } yield
      expect.all(
        partialResult.left.exists(_.isInstanceOf[EtaSourceUnavailable]),
        emptyResult.left.exists(_.isInstanceOf[EtaSourceUnavailable])
      )
  }

  test("MPT cache miss → repeated getEta(period) walks only once (in-process memoization)") { res =>
    implicit val (h, _) = res
    val vrfOutputs = List[(Long, Array[Byte])]((0L, Array.fill[Byte](16)(0x11.toByte)))
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkCount <- Ref.of[IO, Int](0)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, (_, _) => walkCount.update(_ + 1).as(complete(vrfOutputs)))
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
      mgr <- EtaStateManager.make[IO](genesisEta, reader, (_, _) => walkCount.update(_ + 1).as(complete(walkVrf)))
      // First call: MPT miss, walk-cache populated with `EtaCalculation.computeEta(...)` over walkVrf.
      _ <- mgr.getEta(2L)
      // Now populate MPT: subsequent calls must return the MPT-pinned eta, not the prior walk-cache.
      _ <- mptRef.update(_.updated(EtaPeriod(2L), mptEntry))
      out <- mgr.getEta(2L)
    } yield expect.same(true, out.sameElements(mptEtaBytes))
  }

  test("getEtaAt binds derivation to the exact parent and never consumes a sibling MPT eta") { res =>
    implicit val (h, _) = res
    val parentA = hashOf(Array.fill[Byte](32)(0x0a.toByte))
    val parentB = hashOf(Array.fill[Byte](32)(0x0b.toByte))
    val outputsA = List[(Long, Array[Byte])]((1L, Array.fill[Byte](16)(0x21.toByte)))
    val outputsB = List[(Long, Array[Byte])]((1L, Array.fill[Byte](16)(0x31.toByte)))
    val siblingMptEta = Array.fill[Byte](32)(0x55.toByte)
    val siblingEntry = HistoricalStakeSnapshot(StakeDistribution.Empty, hashOf(siblingMptEta))
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](
        SortedMap(EtaPeriod(2L) -> siblingEntry)
      )
      seenParents <- Ref.of[IO, List[Option[Hash]]](Nil)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](
        genesisEta,
        reader,
        (_, parent) =>
          seenParents.update(parent :: _) >> IO.pure(
            complete(if (parent.contains(parentA)) outputsA else outputsB)
          )
      )
      ambient <- mgr.getEta(2L)
      atA <- mgr.getEtaAt(2L, parentA)
      atB <- mgr.getEtaAt(2L, parentB)
      seen <- seenParents.get
    } yield
      expect.all(
        ambient.sameElements(siblingMptEta),
        atA.sameElements(EtaCalculation.computeEta(genesisEta, 2L, outputsA.map(_._2))),
        atB.sameElements(EtaCalculation.computeEta(genesisEta, 2L, outputsB.map(_._2))),
        !atA.sameElements(atB),
        seen.toSet == Set(Some(parentA), Some(parentB))
      )
  }

  // Track-3 S4 eta gate: after a base revert drops the in-process walk cache via `forgetUncommitted`,
  // `getEta` must re-derive over the now-canonical chain — byte-identical to a fresh bootstrap peer that
  // never held the stale (pre-reorg) entry. There is no parallel eta-revert; the MPT-lookup → chain-walk
  // path is the single source of truth.
  test("forgetUncommitted: drops the walk cache so getEta re-derives over the canonical chain (=== fresh bootstrap peer)") { res =>
    implicit val (h, _) = res
    // Pre-reorg chain outputs vs the canonical (post-reorg) chain outputs — deliberately different so a
    // stale cache is observable.
    val preReorg = List[(Long, Array[Byte])]((0L, Array.fill[Byte](16)(0x01.toByte)))
    val canonical = List[(Long, Array[Byte])]((0L, Array.fill[Byte](16)(0x09.toByte)))
    for {
      mptRef <- Ref.of[IO, SortedMap[EtaPeriod, HistoricalStakeSnapshot]](SortedMap.empty)
      walkRef <- Ref.of[IO, List[(Long, Array[Byte])]](preReorg)
      reader = stubReader(mptRef)
      mgr <- EtaStateManager.make[IO](genesisEta, reader, (_, _) => walkRef.get.map(complete))
      // Populate the walk cache from the PRE-reorg chain.
      staleComputed <- mgr.getEta(2L)
      // Reorg the underlying chain to the canonical branch. Without a cache drop the suppression cache
      // still serves the pre-reorg eta.
      _ <- walkRef.set(canonical)
      stillStale <- mgr.getEta(2L)
      // S4 base-revert hook: drop the cache → getEta re-derives over the canonical chain.
      _ <- mgr.forgetUncommitted
      reDerived <- mgr.getEta(2L)
      // A fresh bootstrap peer that only ever saw the canonical chain.
      freshMgr <- EtaStateManager.make[IO](genesisEta, reader, (_, _) => IO.pure(complete(canonical)))
      freshEta <- freshMgr.getEta(2L)
    } yield
      expect.all(
        staleComputed.sameElements(EtaCalculation.computeEta(genesisEta, 2L, preReorg.map(_._2))),
        stillStale.sameElements(staleComputed), // cache suppressed the re-walk (stale value served)
        reDerived.sameElements(EtaCalculation.computeEta(genesisEta, 2L, canonical.map(_._2))),
        !reDerived.sameElements(stillStale), // the drop actually changed the answer
        reDerived.sameElements(freshEta) // === fresh bootstrap peer's eta
      )
  }
}
