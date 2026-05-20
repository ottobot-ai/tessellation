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
  // Deleted 2026-05-20 — S0.5 helper reverted out of `EpochStakeSnapshotter` after the
  // overnight bisect showed `backfillGenesisStakeSnapshots` causes a consensus stall at
  // `assertRewardAndTokenUnlock` ord=41 (see memory `project_nipopow_s0_s3_landing` for
  // full trace). Tests will be re-introduced when a corrected S0.5 design lands.
}
