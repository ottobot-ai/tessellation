package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.BlockReference
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.security.hash.ProofsHash
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec.{proofsCodec => proofsHashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `BlockReference` — `(height: Height, hash: ProofsHash)`.
  *
  * Wire layout (40 bytes, fully fixed-width):
  *   - bytes 0..7 : height (Height as 8-byte big-endian NonNegLong)
  *   - bytes 8..39 : hash (ProofsHash as 32 raw bytes)
  *
  * Matches `TransactionReference`'s layout pattern (ordinal + hash). Fully fixed-width so byte-offset random access works for any consumer
  * that wants to peek at the hash without decoding height.
  *
  * Golden: `BlockReference-scodec-v1.hex`.
  */
object BlockReferenceCodec {

  private val heightCodec: Codec[Height] = Codec[Height]
  private val hashCodec: Codec[ProofsHash] = proofsHashCodec

  implicit val codec: Codec[BlockReference] =
    (heightCodec :: hashCodec)
      .xmap[BlockReference](
        { case h :: ph :: HNil => BlockReference(h, ph) },
        b => b.height :: b.hash :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[BlockReference] = ImmutableCodec.fromScodecCodec(codec)
}
