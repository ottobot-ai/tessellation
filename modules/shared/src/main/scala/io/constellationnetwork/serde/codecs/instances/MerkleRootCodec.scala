package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.merkletree.MerkleRoot
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.Primitives._

import eu.timepit.refined.types.numeric.NonNegInt
import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `MerkleRoot` — `(leafCount: NonNegInt, hash: Hash)`.
  *
  * Wire layout (36 bytes, fully fixed-width):
  *   - bytes 0..3   : leafCount (4-byte big-endian NonNegInt)
  *   - bytes 4..35  : hash      (32 raw bytes)
  *
  * Consensus contract: FROZEN. Used inside `GlobalSnapshotStateProofV1` /
  * `GlobalSnapshotStateProof` as the optional currency-snapshot-proof witness.
  */
object MerkleRootCodec {

  private val leafCountCodec: Codec[NonNegInt] = nonNegIntCodec
  private val hashCodec: Codec[Hash] = HashCodec.codec

  implicit val codec: Codec[MerkleRoot] =
    (leafCountCodec :: hashCodec)
      .xmap[MerkleRoot](
        { case lc :: h :: HNil => MerkleRoot(lc, h) },
        m => m.leafCount :: m.hash :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[MerkleRoot] = ImmutableCodec.fromScodecCodec(codec)
}
