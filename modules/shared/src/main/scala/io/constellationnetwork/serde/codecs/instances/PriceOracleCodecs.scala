package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.NonNegFraction
import io.constellationnetwork.schema.artifact.PricingUpdate
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.priceOracle._
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.Primitives._
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.{NonNegative, Positive}
import scodec.Codec
import scodec.codecs.{discriminated, provide, uint8}
import shapeless.{::, HNil}

/** Canonical scodec codecs for the price-oracle family:
  *   - `TokenId` — sealed ADT, currently 2 known variants (DAG, USD).
  *   - `TokenPair` — `(base: TokenId, quote: TokenId)`.
  *   - `NonNegFraction` — `(numerator: NonNegLong, denominator: PosLong)`, 16 bytes fixed.
  *   - `PriceFraction` — `(TokenPair, NonNegFraction)`.
  *   - `PricingUpdate` — single-field wrapper around `PriceFraction`.
  *   - `PriceRecord` — 6-field record.
  *
  * Discriminator bytes for `TokenId` (consensus-frozen — do NOT renumber):
  *   - 0x00: DAG (CryptoToken)
  *   - 0x01: USD (FiatToken)
  *
  * Flat discrimination across the whole `TokenId` tree (not nested CryptoToken/FiatToken tags) keeps the wire a single byte per TokenId
  * today. When new tokens appear, add new discriminator codes — up to 254 more before we'd need to widen to uint16.
  */
object PriceOracleCodecs {

  // ---- TokenId -------------------------------------------------------------

  implicit val tokenIdCodec: Codec[TokenId] =
    discriminated[TokenId]
      .by(uint8)
      .typecase(0, provide(DAG))
      .typecase(1, provide(USD))

  implicit val tokenIdImmutableCodec: ImmutableCodec[TokenId] = ImmutableCodec.fromScodecCodec(tokenIdCodec)

  // ---- TokenPair -----------------------------------------------------------

  implicit val tokenPairCodec: Codec[TokenPair] =
    (tokenIdCodec :: tokenIdCodec)
      .xmap[TokenPair](
        { case b :: q :: HNil => TokenPair(b, q) },
        p => p.base :: p.quote :: HNil
      )

  implicit val tokenPairImmutableCodec: ImmutableCodec[TokenPair] = ImmutableCodec.fromScodecCodec(tokenPairCodec)

  // ---- NonNegFraction ------------------------------------------------------

  private val nonNegLongRefinedCodec: Codec[Long Refined NonNegative] =
    nonNegLongCodec.xmap[Long Refined NonNegative](identity, identity)

  private val posLongRefinedCodec: Codec[Long Refined Positive] =
    posLongCodec.xmap[Long Refined Positive](identity, identity)

  implicit val nonNegFractionCodec: Codec[NonNegFraction] =
    (nonNegLongRefinedCodec :: posLongRefinedCodec)
      .xmap[NonNegFraction](
        { case n :: d :: HNil => NonNegFraction(n, d) },
        f => f.numerator :: f.denominator :: HNil
      )

  implicit val nonNegFractionImmutableCodec: ImmutableCodec[NonNegFraction] =
    ImmutableCodec.fromScodecCodec(nonNegFractionCodec)

  // ---- PriceFraction / PricingUpdate --------------------------------------

  import io.constellationnetwork.schema.priceOracle.PriceFraction

  implicit val priceFractionCodec: Codec[PriceFraction] =
    (tokenPairCodec :: nonNegFractionCodec)
      .xmap[PriceFraction](
        { case tp :: v :: HNil => PriceFraction(tp, v) },
        p => p.tokenPair :: p.value :: HNil
      )

  implicit val priceFractionImmutableCodec: ImmutableCodec[PriceFraction] =
    ImmutableCodec.fromScodecCodec(priceFractionCodec)

  implicit val pricingUpdateCodec: Codec[PricingUpdate] =
    priceFractionCodec.xmap[PricingUpdate](PricingUpdate(_), _.price)

  implicit val pricingUpdateImmutableCodec: ImmutableCodec[PricingUpdate] =
    ImmutableCodec.fromScodecCodec(pricingUpdateCodec)

  // ---- PriceRecord ---------------------------------------------------------

  private val epochCodec: Codec[EpochProgress] = Codec[EpochProgress]

  implicit val priceRecordCodec: Codec[PriceRecord] =
    (pricingUpdateCodec :: pricingUpdateCodec :: pricingUpdateCodec :: posIntCodec :: epochCodec :: epochCodec)
      .xmap[PriceRecord](
        {
          case cur :: up :: sum :: n :: nw :: ua :: HNil =>
            PriceRecord(cur, up, sum, n, nw, ua)
        },
        r => r.currentPrice :: r.upcomingPrice :: r.currentSum :: r.currentNumEvents :: r.nextWindowChange :: r.updatedAt :: HNil
      )

  implicit val priceRecordImmutableCodec: ImmutableCodec[PriceRecord] =
    ImmutableCodec.fromScodecCodec(priceRecordCodec)
}
