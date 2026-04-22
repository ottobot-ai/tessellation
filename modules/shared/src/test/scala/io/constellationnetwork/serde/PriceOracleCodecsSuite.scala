package io.constellationnetwork.serde

import io.constellationnetwork.schema.NonNegFraction
import io.constellationnetwork.schema.artifact.PricingUpdate
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.priceOracle._
import io.constellationnetwork.serde.codecs.instances.PriceOracleCodecs._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.{NonNegative, Positive}
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import scodec.bits.ByteVector
import weaver.FunSuite

/** Round-trip + structural suite for the price-oracle codec family.
  *
  * Covers: TokenId sum type, TokenPair, NonNegFraction, PriceFraction, PricingUpdate, PriceRecord.
  */
object PriceOracleCodecsSuite extends FunSuite {

  // ---- TokenId -------------------------------------------------------------

  test("DAG encodes as single byte 0x00") {
    val id: TokenId = DAG
    expect(id.immutableBytes == ByteVector.fromValidHex("00"))
  }

  test("USD encodes as single byte 0x01") {
    val id: TokenId = USD
    expect(id.immutableBytes == ByteVector.fromValidHex("01"))
  }

  test("Unknown TokenId discriminator byte fails decode") {
    val bad = ByteVector.fromValidHex("ff")
    bad.fromImmutableBytes[TokenId] match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure, got $other")
    }
  }

  test("TokenId round-trips for every variant") {
    val samples: Seq[TokenId] = Seq(DAG, USD)
    val decoded = samples.map(_.immutableBytes.fromImmutableBytes[TokenId])
    expect(decoded == samples.map(Right(_)))
  }

  // ---- TokenPair -----------------------------------------------------------

  test("TokenPair encodes as 2 bytes (base + quote discriminators)") {
    val bytes = TokenPair.DAG_USD.immutableBytes
    expect(bytes.length == 2L) and
      expect(bytes == ByteVector.fromValidHex("0001"))
  }

  test("TokenPair round-trips") {
    val p = TokenPair.DAG_USD
    expect(p.immutableBytes.fromImmutableBytes[TokenPair] == Right(p))
  }

  // ---- NonNegFraction ------------------------------------------------------

  test("NonNegFraction is exactly 16 bytes (8 numerator + 8 denominator)") {
    val frac = NonNegFraction(
      Refined.unsafeApply[Long, NonNegative](7L),
      Refined.unsafeApply[Long, Positive](2L)
    )
    val bytes = frac.immutableBytes
    expect(bytes.length == 16L) and
      expect(bytes == ByteVector.fromValidHex("00000000000000070000000000000002"))
  }

  test("NonNegFraction round-trips") {
    val frac = NonNegFraction(
      Refined.unsafeApply[Long, NonNegative](100L),
      Refined.unsafeApply[Long, Positive](7L)
    )
    expect(frac.immutableBytes.fromImmutableBytes[NonNegFraction] == Right(frac))
  }

  // ---- PriceFraction / PricingUpdate --------------------------------------

  private def sampleFraction = NonNegFraction(
    Refined.unsafeApply[Long, NonNegative](5L),
    Refined.unsafeApply[Long, Positive](10L)
  )

  test("PriceFraction round-trips") {
    val pf = PriceFraction(TokenPair.DAG_USD, sampleFraction)
    expect(pf.immutableBytes.fromImmutableBytes[PriceFraction] == Right(pf))
  }

  test("PricingUpdate is a transparent wrapper — same bytes as its inner PriceFraction") {
    val pf = PriceFraction(TokenPair.DAG_USD, sampleFraction)
    val pu = PricingUpdate(pf)
    expect(pu.immutableBytes == pf.immutableBytes)
  }

  // ---- PriceRecord ---------------------------------------------------------

  test("PriceRecord round-trips with all 6 fields distinct") {
    val p1 = PricingUpdate(PriceFraction(TokenPair(DAG, USD), sampleFraction))
    val p2 = PricingUpdate(PriceFraction(TokenPair(USD, DAG), sampleFraction))
    val p3 = PricingUpdate(PriceFraction(TokenPair(DAG, DAG), sampleFraction))
    val rec = PriceRecord(
      currentPrice = p1,
      upcomingPrice = p2,
      currentSum = p3,
      currentNumEvents = PosInt.unsafeFrom(42),
      nextWindowChange = EpochProgress(NonNegLong.unsafeFrom(1000L)),
      updatedAt = EpochProgress(NonNegLong.unsafeFrom(500L))
    )
    expect(rec.immutableBytes.fromImmutableBytes[PriceRecord] == Right(rec))
  }
}
