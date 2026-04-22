package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.serde.ImmutableCodec

import scodec.Codec

/** Canonical scodec codec for `CurrencyId` — single-field wrapper around `Address`. */
object CurrencyIdCodec {

  private val addressCodec: Codec[Address] = AddressCodec.codec

  implicit val codec: Codec[CurrencyId] =
    addressCodec.xmap[CurrencyId](CurrencyId(_), _.value)

  implicit val immutableCodec: ImmutableCodec[CurrencyId] = ImmutableCodec.fromScodecCodec(codec)
}
