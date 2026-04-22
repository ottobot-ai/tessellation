package io.constellationnetwork.serde

import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.TransactionCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.FunSuite

/** Golden + round-trip suite for `Transaction` — the first "real" multi-field consensus type in the scodec library.
  *
  * Covers the full wire layout (six distinct fields including both fixed-width and variable-width components) and exercises the field-order
  * invariant: swapping two fields of the same type (source ↔ destination) must produce different bytes.
  */
object TransactionCodecSuite extends FunSuite {

  // Known-valid 40-char addresses from genesis.csv.
  private val srcAddr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")
  private val dstAddr = AddressCodec.unsafeFromLiteral("DAG1UUPsDext9pvuoiyNTM72SX4t1xyod4Q1uXiM")

  private def sample: Transaction = Transaction(
    source = srcAddr,
    destination = dstAddr,
    amount = TransactionAmount(PosLong.unsafeFrom(1000L)),
    fee = TransactionFee(NonNegLong.unsafeFrom(5L)),
    parent = TransactionReference(
      ordinal = TransactionOrdinal(NonNegLong.unsafeFrom(7L)),
      hash = Hash("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
    ),
    salt = TransactionSalt(0xdeadbeefcafebabeL)
  )

  private def golden = GoldenVectors.load("Transaction-scodec-v1")

  test("Transaction encodes to golden bytes") {
    expect(sample.immutableBytes == golden)
  }

  test("Transaction decodes golden bytes back to sample") {
    expect(golden.fromImmutableBytes[Transaction] == Right(sample))
  }

  test("Transaction round-trips via the typeclass layer") {
    val bytes = sample.immutableBytes
    val decoded = bytes.fromImmutableBytes[Transaction]
    expect(decoded == Right(sample))
  }

  test("Transaction total encoded size = 146 bytes for 40-char addresses") {
    // src:41 + dst:41 + amount:8 + fee:8 + parent:40 + salt:8 = 146.
    // (Each 40-char address is 1-byte length prefix + 40 ASCII bytes = 41.)
    expect(sample.immutableBytes.length == 146L)
  }

  test("Swapping source and destination produces different bytes (field-order invariant)") {
    val swapped = sample.copy(source = dstAddr, destination = srcAddr)
    expect(sample.immutableBytes != swapped.immutableBytes)
  }

  test("Changing the salt produces different bytes but keeps length constant") {
    val otherSalt = sample.copy(salt = TransactionSalt(0L))
    val a = sample.immutableBytes
    val b = otherSalt.immutableBytes
    expect(a != b).and(expect(a.length == b.length))
  }

  test("Changing a parent hash byte produces different bytes in the hash region") {
    val altered = sample.copy(parent = sample.parent.copy(hash = Hash("f" * 64)))
    val a = sample.immutableBytes
    val b = altered.immutableBytes
    // They differ in the parent.hash region: bytes 106..137 (32 bytes).
    expect(a != b).and(expect(a.length == b.length)).and(expect(a.take(106) == b.take(106))) // everything before the hash matches
  }

  test("Transaction bytes end with the salt (last 8 bytes)") {
    val bytes = sample.immutableBytes
    val saltRegion = bytes.drop(bytes.length - 8)
    val expected = scodec.bits.ByteVector.fromValidHex("deadbeefcafebabe")
    expect(saltRegion == expected)
  }
}
