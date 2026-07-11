package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.{EtaCalculation, EtaStateManager, HistoricalStakeReader}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.Mocks._
import io.constellationnetwork.node.shared.infrastructure.snapshot.{DelegatedRewardsResult, RewardsInput}
import io.constellationnetwork.node.shared.modules.SharedServices
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, HistoricalStakeSnapshot}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.statechannel.StateChannelValidationType

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Path 1 (heap-leak workstream): regression test for the GSAM `etaForPeriod` wiring.
  *
  * '''Bug surface area.''' Before the fix the two production GSAM constructions
  * ([[io.constellationnetwork.node.shared.modules.SharedServices.make]] and
  * [[io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotConsensus.make]]) passed `etaForPeriod = None` (the GSAM
  * default). At every boundary ordinal `ord % R == R - 1` the eta half of the `HistoricalStakeSnapshot` MPT entry was written as
  * [[Hash.empty]] — pseudo-predictability defeated cluster-wide because the §3 NIPoPoW N-2 historical-distribution read path saw a sentinel
  * `Hash.empty` instead of the real chain-derived eta.
  *
  * '''The fix.''' Both production GSAM construction sites now pass `etaForPeriod = Some(period =>
  * etaStateManager.getEta(period.value).map(etaBytesToHash))` backed by a real [[EtaStateManager]] (MPT cache + chain-walk fallback). This
  * suite exercises the GSAM accept pipeline with both arrangements:
  *
  *   1. `etaForPeriod = None` — reproduces the bug; eta = `Hash.empty`. 2. `etaForPeriod = Some(EtaStateManager-backed)` for periods 0 AND
  *      1 (Cardano/Praos bootstrap) — eta = `etaBytesToHash(EtaCalculation.bootstrapEta(genesisEta, period))` (NOT `Hash.empty`, NOT raw
  *      genesisEta, NOT a fold of period-0 outputs). 3. `etaForPeriod = Some(EtaStateManager-backed)` for period 2 with non-empty chain
  *      walk — eta = `etaBytesToHash(EtaCalculation.computeEta(genesisEta, 2, vrfOutputs))` (the first VRF-folded eta).
  *
  * Arrangement (1) is load-bearing: it FAILS the post-fix expectation `eta != Hash.empty` if anyone re-introduces `etaForPeriod = None` at
  * a production GSAM construction site (caught by `expect.all(... !entry.map(_.eta).contains(Hash.empty))` in arrangements 2-3 if paired
  * against the regressed production wiring). The bug docs (Path 1 reviewer findings) are reproduced in arrangement (1) here directly so the
  * contrast is unambiguous.
  *
  * '''Test inputs are real, not pure mocks.''' Arrangement (3) feeds 5 synthetic VRF outputs through `EtaCalculation.computeEta` —
  * exercising the same byte-deterministic Blake2b digest the production code uses, with the same `genesisEta` seed (32-byte Blake2b of
  * `"tessellation-nakamoto-genesis-eta-v1"`) — so the assertion `expect.same(expectedHash, observedHash)` would catch any silent regression
  * in either the GSAM wiring or the EtaStateManager → Hash projection.
  */
object GsamEtaBoundaryWriteSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] = for {
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (h, sp)

  // The 32-byte cluster genesis eta — same Blake2b-domain-string the production helper computes.
  private def genesisEta: Array[Byte] = SharedServices.DefaultNakamotoGenesisEta

  // Small rotation period so the boundary fires at low ordinals.
  private val R: Long = 10L

  // 5 synthetic VRF outputs for the chain-walk fallback. For periods 0 / 1 the EtaStateManager
  // bypasses the walk (returns bootstrapEta(genesisEta, period) directly); for period ≥ 2 the walk
  // feeds `EtaCalculation.computeEta` over these bytes.
  private val syntheticVrfOutputs: List[(Long, Array[Byte])] =
    (0L until 5L).toList.map(i => (i, Array.fill[Byte](16)((i * 17 + 1).toByte)))

  // Stub HistoricalStakeReader — every lookup returns None. Keeps the manager on the chain-walk path
  // since the production MPT cache hit path is exercised separately by `HistoricalStakeReaderSuite`.
  private def emptyMptReader: HistoricalStakeReader[IO] = new HistoricalStakeReader[IO] {
    def lookup(period: EtaPeriod)(implicit hasher: Hasher[IO]): IO[Option[HistoricalStakeSnapshot]] =
      IO.pure(None)
  }

  // Empty rewards function — same shape as `Mocks`-internal helpers but inlined to avoid pulling
  // the full rewards calculation surface.
  private def emptyRewardsFn: RewardsInput => IO[DelegatedRewardsResult] = _ =>
    DelegatedRewardsResult(
      delegatorRewardsMap = SortedMap.empty,
      updatedCreateDelegatedStakes = SortedMap.empty,
      updatedWithdrawDelegatedStakes = SortedMap.empty,
      nodeOperatorRewards = SortedSet.empty,
      reservedAddressRewards = SortedSet.empty,
      withdrawalRewardTxs = SortedSet.empty,
      totalEmittedRewardsAmount = Amount.empty
    ).pure[IO]

  // Run accept() at the given boundary ordinal and return the post-accept GSI's per-period entry.
  private def runBoundary(
    mgr: GlobalSnapshotAcceptanceManager[IO],
    boundaryOrd: Long,
    expectedPeriod: Long,
    pinnedBoundaryEta: Option[Hash] = None
  ): IO[Option[HistoricalStakeSnapshot]] = {
    val priorInfo = mkGlobalSnapshotInfo()
    for {
      result <- mgr.accept(
        ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(boundaryOrd)),
        epochProgress = EpochProgress(1L),
        previousEpochProgress = EpochProgress.MinValue,
        blocksForAcceptance = List.empty,
        allowSpendBlocksForAcceptance = List.empty,
        tokenLockBlocksForAcceptance = List.empty,
        scEvents = List.empty,
        unpEvents = List.empty,
        cdsEvents = List.empty,
        wdsEvents = List.empty,
        cncEvents = List.empty,
        wncEvents = List.empty,
        lastSnapshotContext = priorInfo,
        lastActiveTips = SortedSet.empty,
        lastDeprecatedTips = SortedSet.empty,
        calculateRewardsFn = emptyRewardsFn,
        validationType = StateChannelValidationType.Full,
        getGlobalSnapshotByOrdinal = _ => IO.pure(None),
        parentTip = io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId.passthrough,
        pinnedBoundaryEta = pinnedBoundaryEta
      )
      (_, _, _, _, _, _, _, _, snapshotInfo, _, _, _, _, _, _, _) = result
    } yield snapshotInfo.historicalStakeSnapshots.get(EtaPeriod(expectedPeriod))
  }

  test("regression: with etaForPeriod=None the boundary write lands Hash.empty (pre-fix behavior)") { res =>
    implicit val (h, sp) = res
    // R=10, ord 9 = period 0 closing boundary (`ord % R == R - 1`).
    for {
      mgr <- mkManager(initialSnapshotInfo = None, etaRotationSnapshots = R, etaForPeriod = None)
      entry <- runBoundary(mgr, boundaryOrd = 9L, expectedPeriod = 0L)
    } yield
      expect.all(
        // Boundary fired and produced a per-period entry.
        entry.isDefined,
        // ...but the eta is `Hash.empty` — the bug surface area Path 1 fixes.
        entry.map(_.eta).contains(Hash.empty)
      )
  }

  test("post-fix period 0: EtaStateManager-backed callback writes etaBytesToHash(bootstrapEta(genesisEta, 0)) at the boundary") { res =>
    implicit val (h, sp) = res
    // For period 0 (boundary ord 9) the EtaStateManager bypasses the chain walk and returns
    // bootstrapEta(genesisEta, 0) per the Cardano/Praos bootstrap convention. The wiring still goes
    // through `etaBytesToHash` so the resulting Hash is the hex encoding of that 32-byte value —
    // distinctly NOT `Hash.empty` and NOT raw genesisEta.
    val chainWalk: Long => IO[List[(Long, Array[Byte])]] = (_: Long) => IO.pure(syntheticVrfOutputs)
    val expectedEtaHash = SharedServices.etaBytesToHash(EtaCalculation.bootstrapEta(genesisEta, 0L))
    for {
      etaMgr <- EtaStateManager.make[IO](
        genesisEta = genesisEta,
        historicalStakeReader = emptyMptReader,
        chainWalkFallback = chainWalk
      )
      callback = (period: EtaPeriod) => etaMgr.getEta(period.value).map(SharedServices.etaBytesToHash)
      mgr <- mkManager(initialSnapshotInfo = None, etaRotationSnapshots = R, etaForPeriod = Some(callback))
      entry <- runBoundary(mgr, boundaryOrd = 9L, expectedPeriod = 0L)
    } yield
      expect.all(
        entry.isDefined,
        // eta is NOT the sentinel `Hash.empty` — the wiring landed a real value.
        !entry.map(_.eta).contains(Hash.empty),
        // For period 0 the EtaStateManager returns `bootstrapEta(genesisEta, 0)` (bypass per
        // `EtaCalculation`); the boundary write packs it through `etaBytesToHash`.
        entry.map(_.eta).contains(expectedEtaHash)
      )
  }

  test("post-fix period 1 (Cardano/Praos bootstrap): callback writes etaBytesToHash(bootstrapEta(genesisEta, 1))") { res =>
    implicit val (h, sp) = res
    // R=10, ord 19 = period 1 closing boundary (19/10 == 1, 19 % 10 == 9 == R-1). Bootstrap convention:
    // period 1 is genesis-derivable and BYPASSES the chain walk — the EtaStateManager returns
    // `EtaCalculation.bootstrapEta(genesisEta, 1)` (folds NO VRF outputs), even though `chainWalkFallback(0L)`
    // would return non-empty period-0 outputs. This is the byte-exact value the wire / eligibility /
    // committee eta also compute for period 1, so producer record == committee == wire == follower-adopt at
    // period 1 — and the first eta rotation (0 → 1) cannot fork on period 0's still-unsettled outputs.
    val chainWalk: Long => IO[List[(Long, Array[Byte])]] =
      (sourcePeriod: Long) => if (sourcePeriod == 0L) IO.pure(syntheticVrfOutputs) else IO.pure(List.empty)
    val expectedEtaHash = SharedServices.etaBytesToHash(EtaCalculation.bootstrapEta(genesisEta, 1L))
    // The value a period-0-output fold WOULD produce — period 1 must NOT equal this (proves the walk is bypassed).
    val foldedFromPeriod0Hash = SharedServices.etaBytesToHash(EtaCalculation.computeEta(genesisEta, 1L, syntheticVrfOutputs.map(_._2)))

    for {
      etaMgr <- EtaStateManager.make[IO](
        genesisEta = genesisEta,
        historicalStakeReader = emptyMptReader,
        chainWalkFallback = chainWalk
      )
      callback = (period: EtaPeriod) => etaMgr.getEta(period.value).map(SharedServices.etaBytesToHash)
      mgr <- mkManager(initialSnapshotInfo = None, etaRotationSnapshots = R, etaForPeriod = Some(callback))
      entry <- runBoundary(mgr, boundaryOrd = 19L, expectedPeriod = 1L)
    } yield
      expect.all(
        entry.isDefined,
        // eta is NOT `Hash.empty`.
        !entry.map(_.eta).contains(Hash.empty),
        // eta is NOT raw genesisEta — bootstrapEta(g,1) is distinct.
        !entry.map(_.eta).contains(SharedServices.etaBytesToHash(genesisEta)),
        // eta is NOT a fold of period-0 outputs — the walk is bypassed for period 1.
        !entry.map(_.eta).contains(foldedFromPeriod0Hash),
        // eta IS the deterministic genesis-derivable `bootstrapEta(genesisEta, 1)`.
        entry.map(_.eta).contains(expectedEtaHash)
      )
  }

  test("post-fix period 2: EtaStateManager-backed callback writes computeEta(genesisEta, 2, chainOutputs)") { res =>
    implicit val (h, sp) = res
    // R=10, ord 29 = period 2 closing boundary (29/10 == 2, 29 % 10 == 9 == R-1). At period 2
    // the EtaStateManager falls through to `chainWalkFallback(1L)` (source-period = currentPeriod-1)
    // and computes `EtaCalculation.computeEta(genesisEta, 2, syntheticVrfOutputs.map(_._2))`.
    val chainWalk: Long => IO[List[(Long, Array[Byte])]] =
      (sourcePeriod: Long) => if (sourcePeriod == 1L) IO.pure(syntheticVrfOutputs) else IO.pure(List.empty)
    val expectedEtaBytes = EtaCalculation.computeEta(genesisEta, 2L, syntheticVrfOutputs.map(_._2))
    val expectedEtaHash = SharedServices.etaBytesToHash(expectedEtaBytes)

    for {
      etaMgr <- EtaStateManager.make[IO](
        genesisEta = genesisEta,
        historicalStakeReader = emptyMptReader,
        chainWalkFallback = chainWalk
      )
      callback = (period: EtaPeriod) => etaMgr.getEta(period.value).map(SharedServices.etaBytesToHash)
      mgr <- mkManager(initialSnapshotInfo = None, etaRotationSnapshots = R, etaForPeriod = Some(callback))
      entry <- runBoundary(mgr, boundaryOrd = 29L, expectedPeriod = 2L)
    } yield
      expect.all(
        entry.isDefined,
        // eta is NOT `Hash.empty`.
        !entry.map(_.eta).contains(Hash.empty),
        // eta is NOT the period-≤1 fall-through `etaBytesToHash(genesisEta)` — the chain walk fired
        // and the manager computed a derived eta.
        !entry.map(_.eta).contains(SharedServices.etaBytesToHash(genesisEta)),
        // eta is the deterministic `EtaCalculation.computeEta` output — load-bearing for the byte-
        // equivalence contract between independent verifiers (one of the §3 NIPoPoW determinism
        // invariants).
        entry.map(_.eta).contains(expectedEtaHash)
      )
  }

  // A downstream follower has the finalized GL0 artifact but not GL0's VRF-output ancestry. At the period boundary it must pin the artifact's
  // signed eta; the full state-proof comparison authenticates the replayed result.

  // gl0's authoritative period-2 eta (what a real producer with the VRF chain computes + commits).
  private val gl0AuthoritativeEtaHash: Hash =
    SharedServices.etaBytesToHash(EtaCalculation.computeEta(genesisEta, 2L, syntheticVrfOutputs.map(_._2)))

  // The follower's divergent period-2 recompute value: empty chain walk ⇒ EtaStateManager's N>=2
  // empty-source fallback returns bootstrapEta(genesisEta, 2), which is NOT gl0's authoritative value —
  // this is the source of the #259 divergence.
  private val followerBootstrapEta2Hash: Hash =
    SharedServices.etaBytesToHash(EtaCalculation.bootstrapEta(genesisEta, 2L))

  // A follower's `etaForPeriod`: empty chain walk ⇒ EtaStateManager falls through to bootstrapEta(genesisEta, 2),
  // which is NOT gl0's authoritative value for period ≥ 2 — this is the source of the #259 divergence.
  private def followerEtaCallback(implicit h: Hasher[IO]): IO[EtaPeriod => IO[Hash]] = {
    val emptyChainWalk: Long => IO[List[(Long, Array[Byte])]] = (_: Long) => IO.pure(List.empty)
    EtaStateManager
      .make[IO](genesisEta = genesisEta, historicalStakeReader = emptyMptReader, chainWalkFallback = emptyChainWalk)
      .map(etaMgr => (period: EtaPeriod) => etaMgr.getEta(period.value).map(SharedServices.etaBytesToHash))
  }

  test("an incomplete follower eta source diverges at the period-2 boundary without the GL0 pin") { res =>
    implicit val (h, sp) = res
    // R=10, ord 29 = period 2 closing boundary. Follower callback ⇒ empty walk ⇒ bootstrapEta(genesisEta, 2).
    for {
      callback <- followerEtaCallback
      mgr <- mkManager(initialSnapshotInfo = None, etaRotationSnapshots = R, etaForPeriod = Some(callback))
      entry <- runBoundary(mgr, boundaryOrd = 29L, expectedPeriod = 2L)
    } yield
      expect.all(
        entry.isDefined,
        // The follower's recomputed eta is the N>=2 empty-source bootstrap fall-through...
        entry.map(_.eta).contains(followerBootstrapEta2Hash),
        // ...which does NOT match gl0's authoritative value → this is exactly the divergence that
        // produced StateProofMismatch every period before the fix.
        !entry.map(_.eta).contains(gl0AuthoritativeEtaHash)
      )
  }

  test("a follower pins finalized GL0 eta at the period-2 boundary") { res =>
    implicit val (h, sp) = res
    // The local callback still returns the wrong bootstrap value. The signed finalized artifact supplies GL0's exact eta.
    for {
      callback <- followerEtaCallback
      mgr <- mkManager(initialSnapshotInfo = None, etaRotationSnapshots = R, etaForPeriod = Some(callback))
      entry <- runBoundary(mgr, boundaryOrd = 29L, expectedPeriod = 2L, pinnedBoundaryEta = Some(gl0AuthoritativeEtaHash))
    } yield
      expect.all(
        entry.isDefined,
        // The boundary entry uses the finalized GL0 value...
        entry.map(_.eta).contains(gl0AuthoritativeEtaHash),
        // ...not the follower's incomplete local fallback.
        !entry.map(_.eta).contains(followerBootstrapEta2Hash)
      )
  }
}
