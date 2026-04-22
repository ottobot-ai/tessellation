package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.ListCodec.list
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{codec => allowSpendRefCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencyIdCodec.{codec => currencyIdCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `AllowSpend` — 8-field record.
  *
  * Wire layout (declared field order):
  *   - source : Address (variable, length-prefixed)
  *   - destination : Address (variable, length-prefixed)
  *   - currencyId : Option[CurrencyId] (1 byte + 41-or-52 if Some)
  *   - amount : SwapAmount (8 bytes)
  *   - fee : AllowSpendFee (8 bytes)
  *   - parent : AllowSpendReference (40 bytes)
  *   - lastValidEpochProgress : EpochProgress (8 bytes)
  *   - approvers : List[Address] (uint16 count + entries)
  *
  * Consensus contract: FROZEN.
  */
object AllowSpendCodec {

  private val currencyIdOptCodec: Codec[Option[CurrencyId]] = option(currencyIdCodec)
  private val approversCodec: Codec[List[Address]] = list(addressCodec)
  private val amountCodec: Codec[SwapAmount] = Codec[SwapAmount]
  private val feeCodec: Codec[AllowSpendFee] = Codec[AllowSpendFee]
  private val epochCodec: Codec[EpochProgress] = Codec[EpochProgress]

  implicit val codec: Codec[AllowSpend] =
    (addressCodec :: addressCodec :: currencyIdOptCodec ::
      amountCodec :: feeCodec :: allowSpendRefCodec ::
      epochCodec :: approversCodec)
      .xmap[AllowSpend](
        {
          case src :: dst :: cid :: amt :: fee :: parent :: lve :: approvers :: HNil =>
            AllowSpend(src, dst, cid, amt, fee, parent, lve, approvers)
        },
        a =>
          a.source ::
            a.destination ::
            a.currencyId ::
            a.amount ::
            a.fee ::
            a.parent ::
            a.lastValidEpochProgress ::
            a.approvers ::
            HNil
      )

  implicit val immutableCodec: ImmutableCodec[AllowSpend] = ImmutableCodec.fromScodecCodec(codec)
}
