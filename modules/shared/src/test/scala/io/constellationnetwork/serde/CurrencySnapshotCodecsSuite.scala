package io.constellationnetwork.serde

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotCodecs._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.MatchesRegex
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Round-trip suite for the currency-snapshot codec family. */
object CurrencySnapshotCodecsSuite extends FunSuite {

  private def v0 = SnapshotVersion(Refined.unsafeApply[String, MatchesRegex["^(0\\.0\\.1|1\\.0\\.0)$"]]("0.0.1"))

  private def minimalV1 = CurrencyIncrementalSnapshotV1(
    ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(1L)),
    height = Height(NonNegLong.unsafeFrom(0L)),
    subHeight = SubHeight(NonNegLong.unsafeFrom(0L)),
    lastSnapshotHash = Hash("0" * 64),
    blocks = SortedSet.empty,
    rewards = SortedSet.empty,
    tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
    stateProof = CurrencySnapshotStateProofV1(Hash("a" * 64), Hash("b" * 64)),
    epochProgress = epoch.EpochProgress(NonNegLong.unsafeFrom(0L)),
    dataApplication = None,
    version = v0
  )

  test("CurrencyIncrementalSnapshotV1 round-trips (minimal)") {
    expect(minimalV1.immutableBytes.fromImmutableBytes[CurrencyIncrementalSnapshotV1] == Right(minimalV1))
  }

  private def minimalCurrent = CurrencyIncrementalSnapshot(
    ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(1L)),
    height = Height(NonNegLong.unsafeFrom(0L)),
    subHeight = SubHeight(NonNegLong.unsafeFrom(0L)),
    lastSnapshotHash = Hash("0" * 64),
    blocks = SortedSet.empty,
    rewards = SortedSet.empty,
    tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
    stateProof = CurrencySnapshotStateProof(
      Hash("a" * 64),
      Hash("b" * 64),
      None,
      None,
      None,
      None,
      None,
      None,
      None
    ),
    epochProgress = epoch.EpochProgress(NonNegLong.unsafeFrom(0L)),
    dataApplication = None,
    messages = None,
    globalSnapshotSyncs = None,
    feeTransactions = None,
    artifacts = None,
    allowSpendBlocks = None,
    tokenLockBlocks = None,
    globalSyncView = None,
    version = v0
  )

  test("Current CurrencyIncrementalSnapshot round-trips (minimal, all options absent)") {
    expect(minimalCurrent.immutableBytes.fromImmutableBytes[CurrencyIncrementalSnapshot] == Right(minimalCurrent))
  }

  test("Current CurrencyIncrementalSnapshot round-trips with Some(empty) optional fields") {
    val sample = minimalCurrent.copy(
      messages = Some(SortedSet.empty),
      globalSnapshotSyncs = Some(SortedSet.empty),
      feeTransactions = Some(SortedSet.empty),
      artifacts = Some(SortedSet.empty),
      allowSpendBlocks = Some(SortedSet.empty),
      tokenLockBlocks = Some(SortedSet.empty)
    )
    expect(sample.immutableBytes.fromImmutableBytes[CurrencyIncrementalSnapshot] == Right(sample))
  }

  private def minimalFull = CurrencySnapshot(
    ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(0L)),
    height = Height(NonNegLong.unsafeFrom(0L)),
    subHeight = SubHeight(NonNegLong.unsafeFrom(0L)),
    lastSnapshotHash = Hash("0" * 64),
    blocks = SortedSet.empty,
    rewards = SortedSet.empty,
    tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
    info = CurrencySnapshotInfoV1(SortedMap.empty, SortedMap.empty),
    epochProgress = epoch.EpochProgress(NonNegLong.unsafeFrom(0L)),
    dataApplication = None,
    globalSyncView = None,
    version = v0
  )

  test("CurrencySnapshot (full) round-trips (minimal)") {
    expect(minimalFull.immutableBytes.fromImmutableBytes[CurrencySnapshot] == Right(minimalFull))
  }
}
