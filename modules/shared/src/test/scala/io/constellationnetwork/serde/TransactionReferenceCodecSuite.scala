package io.constellationnetwork.serde

import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.bits.ByteVector
import weaver.FunSuite

/** Golden + round-trip suite for `TransactionReference` — the first compound scodec-era codec.
  *
  * Fixed 40-byte layout: `[ordinal:8][hash:32]`. Tests both the full encoding and the byte-offset random-access property (hash at offset 8)
  * that the MPT-as-primary workstream depends on.
  */
object TransactionReferenceCodecSuite extends FunSuite {

  private def sampleHashHex: String =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

  private def sample: TransactionReference =
    TransactionReference(
      ordinal = TransactionOrdinal(NonNegLong.unsafeFrom(42L)),
      hash = Hash(sampleHashHex)
    )

  private def golden = GoldenVectors.load("TransactionReference-scodec-v1")

  test("TransactionReference encodes to golden bytes") {
    expect(sample.immutableBytes == golden)
  }

  test("TransactionReference decodes golden bytes back to sample") {
    expect(golden.fromImmutableBytes[TransactionReference] == Right(sample))
  }

  test("TransactionReference is exactly 40 bytes (8 ordinal + 32 hash)") {
    expect(sample.immutableBytes.length == 40L)
  }

  test("ordinal bytes occupy the first 8 bytes (byte-offset random access)") {
    val encoded = sample.immutableBytes
    val ordinalSlice = encoded.take(8)
    // 42 as big-endian int64
    expect(ordinalSlice == ByteVector.fromValidHex("000000000000002a"))
  }

  test("hash bytes occupy bytes 8..39 (byte-offset random access)") {
    val encoded = sample.immutableBytes
    val hashSlice = encoded.drop(8)
    expect(hashSlice == ByteVector.fromValidHex(sampleHashHex)).and(expect(hashSlice.length == 32L))
  }

  test("round-trip preserves both fields independently") {
    val samples = Seq(
      TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(0L)), Hash.empty),
      TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(1L)), Hash("ff" * 32)),
      TransactionReference(
        TransactionOrdinal(NonNegLong.unsafeFrom(Long.MaxValue)),
        Hash("deadbeef" * 8)
      )
    )
    val decoded = samples.map { s =>
      s.immutableBytes.fromImmutableBytes[TransactionReference]
    }
    expect(decoded == samples.map(Right(_)))
  }

  test("wrong-length encoded bytes fail decode") {
    // 39 bytes is one short.
    val truncated = sample.immutableBytes.dropRight(1)
    val result = truncated.fromImmutableBytes[TransactionReference]
    result match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure, got $other")
    }
  }
}
