package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.swap.{AllowSpendOrdinal, AllowSpendReference}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `AllowSpendReference` — `(ordinal: AllowSpendOrdinal, hash: Hash)`.
  *
  * Fixed 40-byte layout, identical to `TransactionReference` / `BlockReference` / `TokenLockReference`.
  */
object AllowSpendReferenceCodec {

  private val ordinalCodec: Codec[AllowSpendOrdinal] = Codec[AllowSpendOrdinal]

  implicit val codec: Codec[AllowSpendReference] =
    (ordinalCodec :: hashCodec)
      .xmap[AllowSpendReference](
        { case o :: h :: HNil => AllowSpendReference(o, h) },
        r => r.ordinal :: r.hash :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[AllowSpendReference] = ImmutableCodec.fromScodecCodec(codec)
}
