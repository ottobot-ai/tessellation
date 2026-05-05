/*
 * Adapted from Bifrost (https://github.com/Topl/Bifrost), originally
 * licensed under the Mozilla Public License 2.0. The Tessellation
 * project is otherwise Apache-2.0; this file remains under MPL-2.0.
 *
 * Source: numerics/src/main/scala/co/topl/numerics/interpreters/Log1pInterpreter.scala
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package io.constellationnetwork.numerics.interpreters

import cats.Monad
import cats.implicits._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.algebras.Log1p
import io.constellationnetwork.numerics.implicits._

/** Continued-fraction approximation of `ln(1 + x)`. Avoids catastrophic cancellation when `x` is small (the typical
  * regime for our LDD eligibility math, where `1 - difficulty` is close to 1). Computed entirely in `Ratio`, so
  * the result is reproducible across all JVMs/CPUs.
  */
object Log1pInterpreter extends LentzMethod {

  def make[F[_]: Monad](maxIterations: Int, precision: Int): F[Log1p[F]] =
    new Log1p[F] {
      override def evaluate(x: Ratio): F[Ratio] =
        Log1pInterpreter.log1p(x, maxIterations, precision)._1.pure[F]
    }.pure[F]

  private[numerics] def log1p(x: Ratio, maxIter: Int, prec: Int): (Ratio, Boolean, Int) = {
    def a(j: Int): Ratio = j match {
      case 0 => Ratio.Zero
      case 1 => x
      case _ => Ratio(j - 1) * Ratio(j - 1) * x
    }
    def b(j: Int): Ratio = j match {
      case 0 => Ratio.Zero
      case 1 => Ratio.One
      case _ => Ratio(j) - Ratio(j - 1) * x
    }
    // log1p(0) = 0 mathematically. The Lentz iteration degenerates at x=0 (drifts to tinyFactor instead).
    // The Taktikos consensus path can hit x=0 (e.g. slotGap == offset → difficulty = 0 → log1p(-0)),
    // so we short-circuit here to match the mathematical definition exactly. Mirrors ExpInterpreter's
    // x=0 special case.
    if (x == Ratio.Zero) (Ratio.Zero, true, 0)
    else modified_lentz_method(maxIter, prec, a, b)
  }
}
