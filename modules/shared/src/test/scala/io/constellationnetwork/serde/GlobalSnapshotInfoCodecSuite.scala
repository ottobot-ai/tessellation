package io.constellationnetwork.serde

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.GlobalSnapshotInfo
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotInfoCodec._
import io.constellationnetwork.serde.implicits._

import weaver.FunSuite

/** Round-trip suite for the 18-field `GlobalSnapshotInfo` capstone codec. */
object GlobalSnapshotInfoCodecSuite extends FunSuite {

  private def empty = GlobalSnapshotInfo(
    lastStateChannelSnapshotHashes = SortedMap.empty,
    lastTxRefs = SortedMap.empty,
    balances = SortedMap.empty,
    lastCurrencySnapshots = SortedMap.empty,
    lastCurrencySnapshotsProofs = SortedMap.empty,
    activeAllowSpends = None,
    activeTokenLocks = None,
    tokenLockBalances = None,
    lastAllowSpendRefs = None,
    lastTokenLockRefs = None,
    updateNodeParameters = None,
    activeDelegatedStakes = None,
    delegatedStakesWithdrawals = None,
    activeNodeCollaterals = None,
    nodeCollateralWithdrawals = None,
    priceState = None,
    metagraphSyncData = None,
    historicalStakeSnapshots = SortedMap.empty
  )

  test("Empty GlobalSnapshotInfo round-trips (all options absent, all maps empty)") {
    expect(empty.immutableBytes.fromImmutableBytes[GlobalSnapshotInfo] == Right(empty))
  }

  test("Empty GlobalSnapshotInfo has a minimal encoded size") {
    // 3 required maps (6 uint16 zero prefixes, 2 bytes each = 6 bytes for 3 count-only maps = 6 bytes,
    // but actually 2 more required maps = 5 × 2 = 10 bytes) + 12 absent-option discriminators (12 bytes)
    //   required-map count prefixes: 5 × 2 = 10 bytes + 1 for historicalStakeSnapshots = 12 bytes
    //   option discriminators:        12 × 1 = 12 bytes
    //   total:                        24 bytes
    expect(empty.immutableBytes.length == 24L)
  }

  test("GlobalSnapshotInfo with Some(empty) options round-trips") {
    val sample = empty.copy(
      activeAllowSpends = Some(SortedMap.empty),
      activeTokenLocks = Some(SortedMap.empty),
      tokenLockBalances = Some(SortedMap.empty),
      lastAllowSpendRefs = Some(SortedMap.empty),
      lastTokenLockRefs = Some(SortedMap.empty),
      updateNodeParameters = Some(SortedMap.empty),
      activeDelegatedStakes = Some(SortedMap.empty),
      delegatedStakesWithdrawals = Some(SortedMap.empty),
      activeNodeCollaterals = Some(SortedMap.empty),
      nodeCollateralWithdrawals = Some(SortedMap.empty),
      priceState = Some(SortedMap.empty),
      metagraphSyncData = Some(SortedMap.empty)
    )
    expect(sample.immutableBytes.fromImmutableBytes[GlobalSnapshotInfo] == Right(sample))
  }
}
