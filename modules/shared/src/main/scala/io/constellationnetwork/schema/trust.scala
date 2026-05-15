package io.constellationnetwork.schema

import cats.Show
import cats.syntax.either._

import eu.timepit.refined.api.Refined
import eu.timepit.refined.cats._
import eu.timepit.refined.numeric.Interval
import eu.timepit.refined.refineV
import fs2.data.csv.{CellDecoder, DecoderError}

/** Remaining refined-Double type aliases used for non-consensus weighting (seedlist `bias`, weighted-sampling primitive).
  *
  * Historical context: this object once carried the TrustStorage / l0Trust scoring stack. That mechanism was never wired into consensus and
  * was removed wholesale. Only the primitive `TrustValueRefined` type alias remains because the seedlist still uses a closed `[-1.0, 1.0]`
  * bias column. The name is kept to avoid churning the CSV format and refined evidence.
  */
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
