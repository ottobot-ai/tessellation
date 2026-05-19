package io.constellationnetwork.serde

import io.constellationnetwork.merkletree.MerkleRoot
import io.constellationnetwork.schema.{GlobalSnapshotStateProof, GlobalSnapshotStateProofV1}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotStateProofCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegInt
import scodec.bits.ByteVector
import weaver.FunSuite

/** Round-trip + structural suite for the state-proof family:
  *   - `GlobalSnapshotStateProofV1` (4 fields, legacy pre-MPT shape).
  *   - `GlobalSnapshotStateProof` (18 fields, current transitional shape with optional MPT root + NIPoPoW S0 historical-stake root).
  *
  * These compose `HashCodec`, `MerkleRootCodec`, and `OptionCodec.option`. Rather than a single hand-computed hex golden for each, we
  * assert:
  *   - total encoded length is predictable (fixed contributions from required fields + 1-or-(1+body) per Option field),
  *   - the byte offset of each field is stable (so MPT-as-primary storage can peek),
  *   - round-trip returns the same Scala value.
  */
object GlobalSnapshotStateProofCodecSuite extends FunSuite {

  private def h(byte: String): Hash = Hash(byte * 32)

  private def h1 = h("aa")
  private def h2 = h("bb")
  private def h3 = h("cc")

  private def merkleRoot = MerkleRoot(NonNegInt.unsafeFrom(7), h("dd"))

  // ---- V1 ------------------------------------------------------------------

  private def v1AllPresent = GlobalSnapshotStateProofV1(h1, h2, h3, Some(merkleRoot))
  private def v1CurrencyAbsent = GlobalSnapshotStateProofV1(h1, h2, h3, None)

  test("V1 with present currency-snapshots proof is 3*32 + 1 + 36 = 133 bytes") {
    expect(v1AllPresent.immutableBytes.length == 133L)
  }

  test("V1 with absent currency-snapshots proof is 3*32 + 1 = 97 bytes") {
    expect(v1CurrencyAbsent.immutableBytes.length == 97L)
  }

  test("V1 first 3 × 32 bytes are the three required hashes in order") {
    val bytes = v1AllPresent.immutableBytes
    expect(bytes.slice(0L, 32L) == ByteVector.fromValidHex("aa" * 32))
      .and(expect(bytes.slice(32L, 64L) == ByteVector.fromValidHex("bb" * 32)))
      .and(expect(bytes.slice(64L, 96L) == ByteVector.fromValidHex("cc" * 32)))
  }

  test("V1 byte at offset 96 is the currency-proof present/absent flag") {
    val presentFlag = v1AllPresent.immutableBytes.drop(96L).head
    val absentFlag = v1CurrencyAbsent.immutableBytes.drop(96L).head
    // `Some` and `None` must produce distinct discriminator bytes, and `None` is zero-byte by convention.
    expect(absentFlag == 0x00.toByte).and(expect(presentFlag != 0x00.toByte))
  }

  test("V1 round-trips, both present and absent currency-proof cases") {
    expect(v1AllPresent.immutableBytes.fromImmutableBytes[GlobalSnapshotStateProofV1] == Right(v1AllPresent))
      .and(expect(v1CurrencyAbsent.immutableBytes.fromImmutableBytes[GlobalSnapshotStateProofV1] == Right(v1CurrencyAbsent)))
  }

  // ---- Current (18-field) --------------------------------------------------

  private def allAbsentCurrent: GlobalSnapshotStateProof =
    GlobalSnapshotStateProof(
      h1,
      h2,
      h3,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )

  private def allPresentCurrent: GlobalSnapshotStateProof =
    GlobalSnapshotStateProof(
      h1,
      h2,
      h3,
      Some(merkleRoot),
      Some(h("01")),
      Some(h("02")),
      Some(h("03")),
      Some(h("04")),
      Some(h("05")),
      Some(h("06")),
      Some(h("07")),
      Some(h("08")),
      Some(h("09")),
      Some(h("0a")),
      Some(h("0b")),
      Some(h("0c")),
      Some(h("0d")),
      Some(h("0e"))
    )

  test("Current, all options absent: 3*32 + 15 * 1 = 111 bytes") {
    expect(allAbsentCurrent.immutableBytes.length == 111L)
  }

  test("Current, all options present: 3*32 + 1 + 36 + 14 * (1+32) = 595 bytes") {
    // required 96 + merkleRoot option (1+36) + 14 hash options × 33 = 96 + 37 + 462 = 595
    expect(allPresentCurrent.immutableBytes.length == 595L)
  }

  test("Current, all options absent — every tail byte is 0x00") {
    val tail = allAbsentCurrent.immutableBytes.drop(96L)
    expect(tail.length == 15L).and(expect(tail.toArray.forall(_ == 0x00.toByte)))
  }

  test("Current round-trips — all absent and all present") {
    expect(allAbsentCurrent.immutableBytes.fromImmutableBytes[GlobalSnapshotStateProof] == Right(allAbsentCurrent))
      .and(expect(allPresentCurrent.immutableBytes.fromImmutableBytes[GlobalSnapshotStateProof] == Right(allPresentCurrent)))
  }

  test("Current field-order invariant: swapping two same-type options produces different bytes") {
    val a = allPresentCurrent.copy(
      activeAllowSpends = Some(h("ff")),
      activeTokenLocks = Some(h("01"))
    )
    val b = allPresentCurrent.copy(
      activeAllowSpends = Some(h("01")),
      activeTokenLocks = Some(h("ff"))
    )
    expect(a.immutableBytes != b.immutableBytes)
  }

  test("Current historicalStakeSnapshots is the LAST field — last 33 bytes when set are present-flag + hash") {
    val sole = allAbsentCurrent.copy(historicalStakeSnapshots = Some(h("0e")))
    val bytes = sole.immutableBytes
    val tailSlice = bytes.drop(bytes.length - 33L)
    // Last 32 bytes must be the hash; the preceding byte is the Some discriminator (non-zero).
    expect(tailSlice.head != 0x00.toByte).and(expect(tailSlice.drop(1) == ByteVector.fromValidHex("0e" * 32)))
  }
}
