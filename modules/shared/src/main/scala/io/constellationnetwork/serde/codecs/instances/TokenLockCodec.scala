package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencyIdCodec.{codec => currencyIdCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{codec => tokenLockRefCodec}

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `TokenLock` — 7-field record.
  *
  * Wire layout (declared field order):
  *   - source : Address
  *   - amount : TokenLockAmount (8 bytes PosLong)
  *   - fee : TokenLockFee (8 bytes NonNegLong)
  *   - parent : TokenLockReference (40 bytes)
  *   - currencyId : Option[CurrencyId]
  *   - unlockEpoch : Option[EpochProgress] (1 + 8 bytes if Some)
  *   - replaceTokenLockRef : Option[Hash] (1 + 32 bytes if Some)
  *
  * Consensus contract: FROZEN.
  */
object TokenLockCodec {

  private val currencyIdOptCodec: Codec[Option[CurrencyId]] = option(currencyIdCodec)
  private val epochCodec: Codec[EpochProgress] = Codec[EpochProgress]
  private val unlockEpochOptCodec: Codec[Option[EpochProgress]] = option(epochCodec)
  private val replaceRefOptCodec: Codec[Option[Hash]] = option(hashCodec)
  private val amountCodec: Codec[TokenLockAmount] = Codec[TokenLockAmount]
  private val feeCodec: Codec[TokenLockFee] = Codec[TokenLockFee]

  implicit val codec: Codec[TokenLock] =
    (addressCodec :: amountCodec :: feeCodec :: tokenLockRefCodec ::
      currencyIdOptCodec :: unlockEpochOptCodec :: replaceRefOptCodec)
      .xmap[TokenLock](
        {
          case src :: amt :: fee :: parent :: cid :: unlock :: replace :: HNil =>
            TokenLock(src, amt, fee, parent, cid, unlock, replace)
        },
        t =>
          t.source ::
            t.amount ::
            t.fee ::
            t.parent ::
            t.currencyId ::
            t.unlockEpoch ::
            t.replaceTokenLockRef ::
            HNil
      )

  implicit val immutableCodec: ImmutableCodec[TokenLock] = ImmutableCodec.fromScodecCodec(codec)
}
