package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.tokenLock.{TokenLockOrdinal, TokenLockReference}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `TokenLockReference` — `(ordinal: TokenLockOrdinal, hash: Hash)`.
  *
  * Fixed 40-byte layout, identical to the other *Reference types.
  */
object TokenLockReferenceCodec {

  private val ordinalCodec: Codec[TokenLockOrdinal] = Codec[TokenLockOrdinal]

  implicit val codec: Codec[TokenLockReference] =
    (ordinalCodec :: hashCodec)
      .xmap[TokenLockReference](
        { case o :: h :: HNil => TokenLockReference(o, h) },
        r => r.ordinal :: r.hash :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[TokenLockReference] = ImmutableCodec.fromScodecCodec(codec)
}
