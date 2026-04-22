package io.constellationnetwork.serde

import io.constellationnetwork.schema.BlockReference
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.security.hash.ProofsHash
import io.constellationnetwork.serde.codecs.instances.BlockReferenceCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.bits.ByteVector
import weaver.FunSuite

/** Golden + round-trip suite for `BlockReference` — `(Height, ProofsHash)`.
  *
  * Fixed 40-byte layout: `[height:8][hash:32]`. Byte-offset random access is the key invariant for any storage record that wants to peek at
  * the hash without decoding height.
  */
object BlockReferenceCodecSuite extends FunSuite {

  private def sampleHashHex: String =
    "89abcdef89abcdef89abcdef89abcdef89abcdef89abcdef89abcdef89abcdef"

  private def sample: BlockReference =
    BlockReference(
      height = Height(NonNegLong.unsafeFrom(123L)),
      hash = ProofsHash(sampleHashHex)
    )

  private def golden = GoldenVectors.load("BlockReference-scodec-v1")

  test("BlockReference encodes to golden bytes") {
    expect(sample.immutableBytes == golden)
  }

  test("BlockReference decodes golden bytes back to sample") {
    expect(golden.fromImmutableBytes[BlockReference] == Right(sample))
  }

  test("BlockReference is exactly 40 bytes (8 height + 32 hash)") {
    expect(sample.immutableBytes.length == 40L)
  }

  test("height bytes occupy the first 8 bytes (byte-offset random access)") {
    val encoded = sample.immutableBytes
    val heightSlice = encoded.take(8)
    // 123 as big-endian int64
    expect(heightSlice == ByteVector.fromValidHex("000000000000007b"))
  }

  test("hash bytes occupy bytes 8..39 (byte-offset random access)") {
    val encoded = sample.immutableBytes
    val hashSlice = encoded.drop(8)
    expect(hashSlice == ByteVector.fromValidHex(sampleHashHex)).and(expect(hashSlice.length == 32L))
  }

  test("round-trip preserves edge values") {
    val samples = Seq(
      BlockReference(Height(NonNegLong.unsafeFrom(0L)), ProofsHash("00" * 32)),
      BlockReference(Height(NonNegLong.unsafeFrom(1L)), ProofsHash("ff" * 32)),
      BlockReference(Height(NonNegLong.unsafeFrom(Long.MaxValue)), ProofsHash("deadbeef" * 8))
    )
    val decoded = samples.map(_.immutableBytes.fromImmutableBytes[BlockReference])
    expect(decoded == samples.map(Right(_)))
  }

  test("wrong-length encoded bytes fail decode") {
    val truncated = sample.immutableBytes.dropRight(1)
    truncated.fromImmutableBytes[BlockReference] match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure, got $other")
    }
  }
}
