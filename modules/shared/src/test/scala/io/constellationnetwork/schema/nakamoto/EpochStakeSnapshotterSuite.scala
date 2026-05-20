package io.constellationnetwork.schema.nakamoto

import cats.data.NonEmptySet

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Unit suite for §3 NIPoPoW S0.4 `EpochStakeSnapshotter.snapshot` and the S0.5 backfill
  * `EpochStakeSnapshotter.backfillGenesisStakeSnapshots`.
  *
  * No `F[_]`, no async — `EpochStakeSnapshotter` is a pure transformation over `GlobalSnapshotInfo`.
  */
object EpochStakeSnapshotterSuite extends FunSuite {

  // ---- helpers --------------------------------------------------------------

  // Mirrors the helper in `StakeRegistrySuite` — keep the byte-by-byte construction identical so
  // a shared `GlobalSnapshotInfo` shape across the two suites stays trivially comparable.
  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  private def addr(tag: String): Address =
    Address.fromBytes(tag.getBytes("UTF-8"))

  private val anyProofs: NonEmptySet[SignatureProof] =
    NonEmptySet.one[SignatureProof](
      SignatureProof(Id(Hex("00")), Signature(Hex(Hash.empty.value)))
    )

  private def mkDelegated(nodeId: PeerId, amt: Long, source: Address): DelegatedStakeRecord = {
    val create = UpdateDelegatedStake.Create(
      source = source,
      nodeId = nodeId,
      amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(amt)),
      fee = DelegatedStakeFee(NonNegLong(0L)),
      tokenLockRef = Hash.empty,
      parent = DelegatedStakeReference.empty
    )
    DelegatedStakeRecord(
      event = Signed(create, anyProofs),
      createdAt = SnapshotOrdinal.MinValue,
      rewards = Amount(NonNegLong(0L)),
      currentTokenLockRef = None,
      currentAmount = None
    )
  }

  private def mkCollateral(nodeId: PeerId, amt: Long, source: Address): NodeCollateralRecord = {
    val create = UpdateNodeCollateral.Create(
      source = source,
      nodeId = nodeId,
      amount = NodeCollateralAmount(NonNegLong.unsafeFrom(amt)),
      fee = NodeCollateralFee(NonNegLong(0L)),
      tokenLockRef = Hash.empty,
      parent = NodeCollateralReference.empty
    )
    NodeCollateralRecord(event = Signed(create, anyProofs), createdAt = SnapshotOrdinal.MinValue)
  }

  private def mkSnapshotInfo(
    delegated: Map[PeerId, Long],
    collateral: Map[PeerId, Long]
  ): GlobalSnapshotInfo = {
    val delegatedMap: SortedMap[Address, SortedSet[DelegatedStakeRecord]] =
      delegated.zipWithIndex.foldLeft(SortedMap.empty[Address, SortedSet[DelegatedStakeRecord]]) {
        case (acc, ((nodeId, amt), idx)) =>
          val a = addr(s"delegated-source-$idx")
          acc.updated(a, SortedSet(mkDelegated(nodeId, amt, a)))
      }
    val collateralMap: SortedMap[Address, SortedSet[NodeCollateralRecord]] =
      collateral.zipWithIndex.foldLeft(SortedMap.empty[Address, SortedSet[NodeCollateralRecord]]) {
        case (acc, ((nodeId, amt), idx)) =>
          val a = addr(s"collateral-source-$idx")
          acc.updated(a, SortedSet(mkCollateral(nodeId, amt, a)))
      }
    GlobalSnapshotInfo.empty.copy(
      activeDelegatedStakes = Some(delegatedMap),
      activeNodeCollaterals = Some(collateralMap)
    )
  }

  // ---- snapshot pure-function tests ----------------------------------------

  test("snapshot: empty GSI returns the empty distribution") {
    val info = GlobalSnapshotInfo.empty
    val dist = EpochStakeSnapshotter.snapshot(info)
    expect.same(StakeDistribution.Empty, dist)
  }

  test("snapshot: single-tier delegated stakes sum into the distribution map") {
    val a = pid("a")
    val b = pid("b")
    val info = mkSnapshotInfo(delegated = Map(a -> 100L, b -> 250L), collateral = Map.empty)
    val dist = EpochStakeSnapshotter.snapshot(info)
    expect.same(BigInt(100L), dist.stakeOf(a)) && expect.same(BigInt(250L), dist.stakeOf(b))
  }

  test("snapshot: delegated + collateral are summed per peer") {
    val a = pid("a")
    val info = mkSnapshotInfo(delegated = Map(a -> 100L), collateral = Map(a -> 50L))
    val dist = EpochStakeSnapshotter.snapshot(info)
    expect.same(BigInt(150L), dist.stakeOf(a))
  }

  test("snapshot: peer with no record returns BigInt(0) via stakeOf") {
    val a = pid("a")
    val b = pid("b")
    val info = mkSnapshotInfo(delegated = Map(a -> 100L), collateral = Map.empty)
    val dist = EpochStakeSnapshotter.snapshot(info)
    expect.same(BigInt(0L), dist.stakeOf(b))
  }

  // ---- backfillGenesisStakeSnapshots tests ---------------------------------

  test("backfill: empty GSI seeds {-2, -1, 0} with StakeDistribution.Empty") {
    val info = GlobalSnapshotInfo.empty
    val out = EpochStakeSnapshotter.backfillGenesisStakeSnapshots(info)
    val keys = out.historicalStakeSnapshots.keySet
    val expected = Set(EtaPeriod(-2L), EtaPeriod(-1L), EtaPeriod(0L))
    expect.same(expected, keys) &&
    expect(out.historicalStakeSnapshots.values.forall(_ == StakeDistribution.Empty))
  }

  test("backfill: stake-bearing GSI seeds the three keys with the genesis distribution") {
    val a = pid("a")
    val b = pid("b")
    val info = mkSnapshotInfo(delegated = Map(a -> 100L, b -> 200L), collateral = Map.empty)
    val genesisDist = EpochStakeSnapshotter.snapshot(info)
    val out = EpochStakeSnapshotter.backfillGenesisStakeSnapshots(info)
    val expected: SortedMap[EtaPeriod, StakeDistribution] =
      SortedMap(EtaPeriod(-2L) -> genesisDist, EtaPeriod(-1L) -> genesisDist, EtaPeriod(0L) -> genesisDist)
    expect.same(expected, out.historicalStakeSnapshots)
  }

  test("backfill: idempotent — applying twice produces the same output") {
    val a = pid("a")
    val info = mkSnapshotInfo(delegated = Map(a -> 100L), collateral = Map.empty)
    val once = EpochStakeSnapshotter.backfillGenesisStakeSnapshots(info)
    val twice = EpochStakeSnapshotter.backfillGenesisStakeSnapshots(once)
    expect.same(once.historicalStakeSnapshots, twice.historicalStakeSnapshots)
  }

  test("backfill: preserves prior entries outside {-2, -1, 0} and overrides those three") {
    // Simulate a non-genesis GSI that already has stamps for {5} and a stale {0} from a prior run.
    val a = pid("a")
    val stale = StakeDistribution(SortedMap(a -> BigInt(9999L)))
    val info = mkSnapshotInfo(delegated = Map(a -> 100L), collateral = Map.empty).copy(
      historicalStakeSnapshots = SortedMap[EtaPeriod, StakeDistribution](
        EtaPeriod(0L) -> stale,
        EtaPeriod(5L) -> stale
      )
    )
    val genesisDist = EpochStakeSnapshotter.snapshot(info)
    val out = EpochStakeSnapshotter.backfillGenesisStakeSnapshots(info)
    // Period 5 untouched, periods {-2, -1, 0} all set to the genesis distribution (NOT the stale).
    expect.same(stale, out.historicalStakeSnapshots(EtaPeriod(5L))) &&
    expect.same(genesisDist, out.historicalStakeSnapshots(EtaPeriod(-2L))) &&
    expect.same(genesisDist, out.historicalStakeSnapshots(EtaPeriod(-1L))) &&
    expect.same(genesisDist, out.historicalStakeSnapshots(EtaPeriod(0L)))
  }

  test("backfill: leaves all other GSI fields unchanged") {
    val a = pid("a")
    val info = mkSnapshotInfo(delegated = Map(a -> 100L), collateral = Map.empty)
    val out = EpochStakeSnapshotter.backfillGenesisStakeSnapshots(info)
    expect.same(info.activeDelegatedStakes, out.activeDelegatedStakes) &&
    expect.same(info.activeNodeCollaterals, out.activeNodeCollaterals) &&
    expect.same(info.balances, out.balances) &&
    expect.same(info.lastTxRefs, out.lastTxRefs)
  }
}
