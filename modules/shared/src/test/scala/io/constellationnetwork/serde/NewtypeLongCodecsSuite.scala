package io.constellationnetwork.serde

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Golden + round-trip suite for the newtype-over-NonNegLong family.
  *
  * These types all share the same shape: `case class X(value: NonNegLong)`.
  * Their codec is a one-liner (`nonNegLongCodec.xmap(X(_), _.value)`) and their
  * on-wire bytes are identical to `Balance`: 8 bytes big-endian non-negative
  * int64. The canary here proves the pattern replicates cleanly — if a future
  * codec refactor somehow produces different bytes for the same logical value,
  * one of these tests fails and the drift is caught immediately.
  */
object NewtypeLongCodecsSuite extends FunSuite {

  // Samples + goldens declared up front. Using `def` (not `val`) to avoid the
  // scalafix unused-declaration false-positive when references live inside
  // `test(...)` closures that are registered but not yet executed at definition
  // time.
  private def amountSample: Amount = Amount(NonNegLong.unsafeFrom(1_000_000L))
  private def amountGolden = GoldenVectors.load("Amount-scodec-v1")

  private def ordinalSample: SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(42L))
  private def ordinalGolden = GoldenVectors.load("SnapshotOrdinal-scodec-v1")

  private def epochSample: EpochProgress = EpochProgress(NonNegLong.unsafeFrom(100L))
  private def epochGolden = GoldenVectors.load("EpochProgress-scodec-v1")

  test("Amount encodes to golden bytes") {
    expect(amountSample.immutableBytes == amountGolden)
  }

  test("Amount decodes golden bytes back to sample") {
    expect(amountGolden.fromImmutableBytes[Amount] == Right(amountSample))
  }

  test("SnapshotOrdinal encodes to golden bytes") {
    expect(ordinalSample.immutableBytes == ordinalGolden)
  }

  test("SnapshotOrdinal decodes golden bytes back to sample") {
    expect(ordinalGolden.fromImmutableBytes[SnapshotOrdinal] == Right(ordinalSample))
  }

  test("EpochProgress encodes to golden bytes") {
    expect(epochSample.immutableBytes == epochGolden)
  }

  test("EpochProgress decodes golden bytes back to sample") {
    expect(epochGolden.fromImmutableBytes[EpochProgress] == Right(epochSample))
  }

  test("all three newtype codecs produce exactly 8 bytes (fixed-width int64)") {
    expect(amountSample.immutableBytes.length == 8L) and
      expect(ordinalSample.immutableBytes.length == 8L) and
      expect(epochSample.immutableBytes.length == 8L)
  }

  test("different newtype values with the same underlying Long produce identical bytes") {
    val shared: Long = 777L
    val asAmount = Amount(NonNegLong.unsafeFrom(shared)).immutableBytes
    val asOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(shared)).immutableBytes
    val asEpoch = EpochProgress(NonNegLong.unsafeFrom(shared)).immutableBytes
    // Since all three are newtype-over-NonNegLong with no discriminator byte,
    // their bytes for the same numeric value are identical. Call sites that
    // need type-distinct encoding (e.g. an MPT key namespace) must add an
    // explicit type tag upstream of the codec.
    expect(asAmount == asOrdinal) and expect(asOrdinal == asEpoch)
  }
}
