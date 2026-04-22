package io.constellationnetwork.serde.codecs.offset

import io.constellationnetwork.schema.BlockReference
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.swap.{AllowSpendOrdinal, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock.{TokenLockOrdinal, TokenLockReference}
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.serde.codecs.instances.HashCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.Codec

/** Byte-offset lenses for the `*Reference` family. All 40 bytes fixed-width:
  *
  *   [0..7]   ordinal-like (8 bytes big-endian NonNegLong)
  *   [8..39]  hash (32 raw bytes)
  *
  * These lenses let a consumer read, for example, just the `hash` field of a
  * `TransactionReference` from a byte slice on disk without decoding the ordinal or any surrounding
  * record. The typical use is "given the first 2 parent-block-reference's hash in a `Block`
  * serialization, walk the chain without loading the full blocks".
  */
object BlockReferenceLenses {
  val height: FieldLens[BlockReference, Height] =
    FieldLens.fixed(offset = 0L, length = 8L, Codec[Height])

  val hash: FieldLens[BlockReference, ProofsHash] =
    FieldLens.fixed(offset = 8L, length = 32L, HashCodec.proofsCodec)
}

object TransactionReferenceLenses {
  val ordinal: FieldLens[TransactionReference, TransactionOrdinal] =
    FieldLens.fixed(offset = 0L, length = 8L, Codec[TransactionOrdinal])

  val hash: FieldLens[TransactionReference, Hash] =
    FieldLens.fixed(offset = 8L, length = 32L, HashCodec.codec)
}

object AllowSpendReferenceLenses {
  val ordinal: FieldLens[AllowSpendReference, AllowSpendOrdinal] =
    FieldLens.fixed(offset = 0L, length = 8L, Codec[AllowSpendOrdinal])

  val hash: FieldLens[AllowSpendReference, Hash] =
    FieldLens.fixed(offset = 8L, length = 32L, HashCodec.codec)
}

object TokenLockReferenceLenses {
  val ordinal: FieldLens[TokenLockReference, TokenLockOrdinal] =
    FieldLens.fixed(offset = 0L, length = 8L, Codec[TokenLockOrdinal])

  val hash: FieldLens[TokenLockReference, Hash] =
    FieldLens.fixed(offset = 8L, length = 32L, HashCodec.codec)
}
