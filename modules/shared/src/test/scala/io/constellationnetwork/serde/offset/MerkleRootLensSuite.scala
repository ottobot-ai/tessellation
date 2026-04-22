package io.constellationnetwork.serde.offset

import io.constellationnetwork.merkletree.MerkleRoot
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.MerkleRootCodec
import io.constellationnetwork.serde.codecs.offset._

import eu.timepit.refined.types.numeric.NonNegInt
import weaver.FunSuite

/** Lens suite for `MerkleRoot` — 36 bytes fixed (4 leafCount + 32 hash). */
object MerkleRootLensSuite extends FunSuite {

  test("MerkleRootLenses read both fields and agree with full decode") {
    val sample = MerkleRoot(NonNegInt.unsafeFrom(42), Hash("ab" * 32))
    val bytes = MerkleRootCodec.codec.encode(sample).require.toByteVector

    expect(MerkleRootLenses.leafCount.read(bytes).require == sample.leafCount)
      .and(expect(MerkleRootLenses.hash.read(bytes).require == sample.hash))
      .and(expect(bytes.length == 36L))
  }

  test("MerkleRootLenses rejects truncated bytes") {
    val sample = MerkleRoot(NonNegInt.unsafeFrom(1), Hash("0" * 64))
    val bytes = MerkleRootCodec.codec.encode(sample).require.toByteVector
    val truncated = bytes.take(10)
    expect(MerkleRootLenses.hash.read(truncated).isFailure)
  }
}
