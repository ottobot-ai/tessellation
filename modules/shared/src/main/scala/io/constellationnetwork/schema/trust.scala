package io.constellationnetwork.schema

import cats.Show
import cats.syntax.either._

import eu.timepit.refined.api.Refined
import eu.timepit.refined.cats._
import eu.timepit.refined.numeric.Interval
import eu.timepit.refined.refineV
import fs2.data.csv.{CellDecoder, DecoderError}

/** Refined `[-1.0, 1.0]` Double used by the seedlist `bias` column. */
object trust {

  type TrustValueRefinement = Interval.Closed[-1.0, 1.0]
  type TrustValueRefined = Double Refined TrustValueRefinement

  implicit def showTrustValue: Show[TrustValueRefined] = s => s"TrustValue(value=${s.value})"

  implicit val trustValueRefinedCellDecoder: CellDecoder[TrustValueRefined] =
    CellDecoder.doubleDecoder.emap {
      refineV[TrustValueRefinement](_)
        .leftMap(new DecoderError(_))
    }
}
