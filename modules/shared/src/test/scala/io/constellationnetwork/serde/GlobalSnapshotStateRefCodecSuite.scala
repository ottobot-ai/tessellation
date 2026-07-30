package io.constellationnetwork.serde

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotStateRefCodec
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotStateRefCodec._
import io.constellationnetwork.serde.implicits._

import scodec.bits.ByteVector
import weaver.FunSuite

/** ScodecV1 canary for the authority-free identity of an exact GL0 snapshot state.
  *
  * This suite freezes only the four-field value shape. It deliberately supplies no finality phase, qualification evidence, branch
  * authentication, or signing authority.
  */
object GlobalSnapshotStateRefCodecSuite extends FunSuite {

  private val snapshotHash =
    Hash("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
  private val parentHash =
    Hash("202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f")
  private val stateRoot =
    Hash("404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f")

  private val sample =
    GlobalSnapshotStateRef(
      SnapshotOrdinal.unsafeApply(0x0102030405060708L),
      snapshotHash,
      parentHash,
      MptRoot(stateRoot)
    )

  private val golden = GoldenVectors.load("GlobalSnapshotStateRef-scodec-v1")

  private def decode(bytes: ByteVector): Either[SerdeError, GlobalSnapshotStateRef] =
    bytes.fromImmutableBytes[GlobalSnapshotStateRef]

  private def replace(bytes: ByteVector, offset: Long, length: Long, replacement: ByteVector): ByteVector =
    bytes.take(offset) ++ replacement ++ bytes.drop(offset + length)

  test("golden freezes ordinal, hash, parentHash, and mptRoot in that order") {
    expect.all(
      sample.immutableBytes == golden,
      golden.length == 104L,
      golden.take(8L) == ByteVector.fromValidHex("0102030405060708"),
      golden.slice(8L, 40L) == ByteVector.fromValidHex(snapshotHash.value),
      golden.slice(40L, 72L) == ByteVector.fromValidHex(parentHash.value),
      golden.drop(72L) == ByteVector.fromValidHex(stateRoot.value)
    )
  }

  test("golden decodes and re-encodes byte-identically") {
    expect(decode(golden) == Right(sample))
      .and(expect(decode(golden).map(_.immutableBytes) == Right(golden)))
  }

  test("boundary ordinals round-trip with only the ordinal-zero empty-parent exception") {
    val genesis =
      GlobalSnapshotStateRef(SnapshotOrdinal.MinValue, snapshotHash, Hash.empty, MptRoot(stateRoot))
    val max =
      GlobalSnapshotStateRef(SnapshotOrdinal.unsafeApply(Long.MaxValue), snapshotHash, parentHash, MptRoot(stateRoot))

    expect.all(
      decode(genesis.immutableBytes) == Right(genesis),
      decode(max.immutableBytes) == Right(max)
    )
  }

  test("every field independently changes canonical bytes") {
    val variants = List(
      sample.copy(ordinal = SnapshotOrdinal.unsafeApply(sample.ordinal.value.value + 1L)),
      sample.copy(hash = Hash("60" * 32)),
      sample.copy(parentHash = Hash("61" * 32)),
      sample.copy(mptRoot = MptRoot(Hash("62" * 32)))
    )

    expect(variants.forall(_.immutableBytes != golden))
  }

  test("the value shape contains no phase or qualification evidence") {
    val fields = sample.productElementNames.toList

    expect(fields == List("ordinal", "hash", "parentHash", "mptRoot"))
      .and(expect(fields.forall(name => !name.toLowerCase.contains("phase"))))
      .and(expect(fields.forall(name => !name.toLowerCase.contains("evidence"))))
      .and(expect(fields.forall(name => !name.toLowerCase.contains("proof"))))
  }

  test("complete decode rejects every truncated length and trailing bytes") {
    val truncatedResults = (0L until golden.length).map(length => decode(golden.take(length)))
    val trailingResults = (0 to 255).map(byte => decode(golden :+ byte.toByte))

    expect(truncatedResults.forall(_.isLeft))
      .and(expect(trailingResults.forall(_.isLeft)))
  }

  test("decode rejects negative ordinal bytes") {
    val negativeOrdinal = replace(golden, 0L, 8L, ByteVector.fromValidHex("8000000000000000"))

    expect(decode(negativeOrdinal).isLeft)
  }

  test("decode rejects empty snapshot hash and empty MPT root") {
    val empty = ByteVector.fill(32L)(0.toByte)
    val emptySnapshotHash = replace(golden, 8L, 32L, empty)
    val emptyMptRoot = replace(golden, 72L, 32L, empty)

    expect(decode(emptySnapshotHash).isLeft)
      .and(expect(decode(emptyMptRoot).isLeft))
  }

  test("decode requires the empty parent sentinel exactly at ordinal zero") {
    val empty = ByteVector.fill(32L)(0.toByte)
    val emptyParent = replace(golden, 40L, 32L, empty)
    val ordinalZeroWithEmptyParent =
      replace(emptyParent, 0L, 8L, ByteVector.fill(8L)(0.toByte))
    val ordinalZeroWithNonemptyParent =
      replace(golden, 0L, 8L, ByteVector.fill(8L)(0.toByte))

    expect(decode(emptyParent).isLeft)
      .and(expect(decode(ordinalZeroWithNonemptyParent).isLeft))
      .and(
        expect(
          decode(ordinalZeroWithEmptyParent) ==
            Right(GlobalSnapshotStateRef(SnapshotOrdinal.MinValue, snapshotHash, Hash.empty, MptRoot(stateRoot)))
        )
      )
  }

  test("encode rejects malformed or noncanonical hash spellings in every hash field") {
    val malformed = List(
      sample.copy(hash = Hash("A" * 64)),
      sample.copy(parentHash = Hash("B" * 64)),
      sample.copy(mptRoot = MptRoot(Hash("C" * 64))),
      sample.copy(hash = Hash("a" * 63)),
      sample.copy(parentHash = Hash("g" * 64)),
      sample.copy(mptRoot = MptRoot(Hash("not-a-hash")))
    )

    expect(malformed.forall(ref => GlobalSnapshotStateRefCodec.codec.encode(ref).toEither.isLeft))
  }

  test("encode requires the empty parent sentinel exactly at ordinal zero") {
    val invalid = List(
      sample.copy(hash = Hash.empty),
      sample.copy(parentHash = Hash.empty),
      sample.copy(mptRoot = MptRoot(Hash.empty)),
      sample.copy(ordinal = SnapshotOrdinal.MinValue)
    )

    expect(invalid.forall(ref => GlobalSnapshotStateRefCodec.codec.encode(ref).toEither.isLeft))
  }
}
