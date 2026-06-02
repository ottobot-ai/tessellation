package io.constellationnetwork.serde

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.{SystemIndexDelta, TokenLockExpiryKey}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.StateChangesAccumulatorCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.auto._
import weaver.FunSuite

/** Round-trip suite for the per-ordinal `StateChangesAccumulator` wire codec (currency-l0 adopt path, task #14). */
object StateChangesAccumulatorCodecSuite extends FunSuite {

  test("empty accumulator round-trips (all partitions empty)") {
    val empty = StateChangesAccumulator()
    expect(empty.immutableBytes.fromImmutableBytes[StateChangesAccumulator] == Right(empty))
  }

  test("accumulator with a populated expiry-index delta round-trips") {
    val addr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")
    val hash: Hash = Hash("0000000000000000000000000000000000000000000000000000000000000001")
    val bucket = SystemIndexDelta.EpochBucket[TokenLockExpiryKey](
      adds = SortedMap(EpochProgress(10L) -> Set(TokenLockExpiryKey(addr, hash))),
      removes = SortedMap.empty
    )
    val sample = StateChangesAccumulator(
      tokenLockExpiryIndex = bucket,
      removedTokenLockKeys = Set(addr),
      removedHistoricalStakeSnapshotKeys = Set.empty
    )
    expect(sample.immutableBytes.fromImmutableBytes[StateChangesAccumulator] == Right(sample))
  }
}
