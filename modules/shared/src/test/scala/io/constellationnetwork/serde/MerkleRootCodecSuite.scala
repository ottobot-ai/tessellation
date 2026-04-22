package io.constellationnetwork.serde

import io.constellationnetwork.merkletree.MerkleRoot
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.MerkleRootCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegInt
import scodec.bits.ByteVector
import weaver.FunSuite

/** Golden + round-trip suite for `MerkleRoot` — `(leafCount: NonNegInt, hash: Hash)`.
  *
  * Fixed 36-byte layout: `[leafCount:4][hash:32]`.
  */
object MerkleRootCodecSuite extends FunSuite {

  private def sampleHashHex: String = "1234567890abcdef" * 4

  private def sample: MerkleRoot = MerkleRoot(
    leafCount = NonNegInt.unsafeFrom(42),
    hash = Hash(sampleHashHex)
  )

  test("MerkleRoot is exactly 36 bytes (4 leafCount + 32 hash)") {
    expect(sample.immutableBytes.length == 36L)
  }

  test("leafCount occupies first 4 bytes, hash occupies bytes 4..35") {
    val bytes = sample.immutableBytes
    expect(bytes.take(4) == ByteVector.fromValidHex("0000002a")).and(expect(bytes.drop(4) == ByteVector.fromValidHex(sampleHashHex)))
  }

  test("MerkleRoot round-trips through the typeclass layer") {
    expect(sample.immutableBytes.fromImmutableBytes[MerkleRoot] == Right(sample))
  }

  test("Negative leafCount bytes fail decode (refinement enforced)") {
    val bytes = ByteVector.fromValidHex("ffffffff") ++ ByteVector.fromValidHex(sampleHashHex)
    bytes.fromImmutableBytes[MerkleRoot] match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure, got $other")
    }
  }
}
