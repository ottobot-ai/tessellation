package io.constellationnetwork.numerics

import cats.{Eq, Show}

import io.circe.{Decoder, Encoder, Json}

/** JSON, Eq, and Show instances for [[Ratio]]. Kept in a separate Apache-2.0 file so the imported `Ratio.scala` (MPL-2.0) can stay
  * byte-identical to upstream Bifrost.
  *
  * On-wire encoding: `{ "n": <BigInt>, "d": <BigInt> }`. The gcd field is recomputed at `Ratio.apply` so it does not need to round-trip.
  */
object RatioInstances {

  implicit val ratioEncoder: Encoder[Ratio] = Encoder.instance { r =>
    Json.obj(
      "n" -> Encoder[BigInt].apply(r.numerator),
      "d" -> Encoder[BigInt].apply(r.denominator)
    )
  }

  implicit val ratioDecoder: Decoder[Ratio] = Decoder.instance { c =>
    for {
      n <- c.downField("n").as[BigInt]
      d <- c.downField("d").as[BigInt]
    } yield Ratio(n, d)
  }

  implicit val ratioEq: Eq[Ratio] = Eq.fromUniversalEquals[Ratio]

  implicit val ratioShow: Show[Ratio] = Show.show(_.toString)
}
