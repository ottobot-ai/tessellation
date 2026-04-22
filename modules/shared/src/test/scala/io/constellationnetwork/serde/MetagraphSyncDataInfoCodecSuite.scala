package io.constellationnetwork.serde

import scala.collection.immutable.SortedSet

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.serde.codecs.instances.MetagraphSyncDataInfoCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.bits.ByteVector
import weaver.FunSuite

/** Round-trip + structural suite for `MetagraphSyncDataInfo`.
  *
  * Exercises the `SortedSetCodec` helper end-to-end (with `SortedSet[SnapshotOrdinal]`), and
  * demonstrates that the empty-set case has no non-emptiness invariant to worry about.
  */
object MetagraphSyncDataInfoCodecSuite extends FunSuite {

  private def ord(n: Long) = SnapshotOrdinal(NonNegLong.unsafeFrom(n))
  private def ep(n: Long) = EpochProgress(NonNegLong.unsafeFrom(n))

  test("Empty unappliedOrdinals encodes to 8 + 8 + 2 = 18 bytes") {
    val sample = MetagraphSyncDataInfo(ord(10L), ep(3L), SortedSet.empty)
    val bytes = sample.immutableBytes
    expect(bytes.length == 18L) and
      expect(bytes.drop(16L) == ByteVector.fromValidHex("0000")) // empty set uint16
  }

  test("Non-empty unappliedOrdinals round-trips") {
    val sample = MetagraphSyncDataInfo(
      ord(100L),
      ep(7L),
      SortedSet(ord(1L), ord(2L), ord(3L))
    )
    expect(sample.immutableBytes.fromImmutableBytes[MetagraphSyncDataInfo] == Right(sample))
  }

  test("Set elements are sorted on encode (deterministic bytes for same mathematical set)") {
    val a = MetagraphSyncDataInfo(ord(0L), ep(0L), SortedSet(ord(3L), ord(1L), ord(2L)))
    val b = MetagraphSyncDataInfo(ord(0L), ep(0L), SortedSet(ord(1L), ord(2L), ord(3L)))
    expect(a.immutableBytes == b.immutableBytes)
  }
}
