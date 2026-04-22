package io.constellationnetwork.serde.codecs.offset

import io.constellationnetwork.merkletree.MerkleRoot
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.Primitives.nonNegIntCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec

import eu.timepit.refined.types.numeric.NonNegInt

/** Byte-offset lenses for `MerkleRoot` — 36 bytes fixed:
  *
  *   [0..3]   leafCount (4-byte big-endian NonNegInt)
  *   [4..35]  hash (32 raw bytes)
  */
object MerkleRootLenses {
  val leafCount: FieldLens[MerkleRoot, NonNegInt] =
    FieldLens.fixed(offset = 0L, length = 4L, nonNegIntCodec)

  val hash: FieldLens[MerkleRoot, Hash] =
    FieldLens.fixed(offset = 4L, length = 32L, HashCodec.codec)
}
