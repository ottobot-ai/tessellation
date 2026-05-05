/*
 * Adapted from Bifrost (https://github.com/Topl/Bifrost), originally
 * licensed under the Mozilla Public License 2.0. The Tessellation
 * project is otherwise Apache-2.0; this file remains under MPL-2.0.
 *
 * Source: numerics/src/main/scala/co/topl/numerics/interpreters/ExpInterpreter.scala
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package io.constellationnetwork.numerics.interpreters

import cats.Monad
import cats.implicits._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.algebras.Exp
import io.constellationnetwork.numerics.implicits._

/** Continued-fraction approximation of `exp(x)` evaluated in `Ratio`. Reproducible across all JVMs/CPUs. */
object ExpInterpreter extends LentzMethod {

  def make[F[_]: Monad](maxIterations: Int, precision: Int): F[Exp[F]] =
    new Exp[F] {
      override def evaluate(x: Ratio): F[Ratio] =
        ExpInterpreter.exp(x, maxIterations, precision)._1.pure[F]
    }.pure[F]

  private[numerics] def exp(x: Ratio, maxIter: Int, prec: Int): (Ratio, Boolean, Int) = {
    def a(j: Int): Ratio = j match {
      case 0 => Ratio.Zero
      case 1 => Ratio.One
      case 2 => Ratio.NegativeOne * x
      case _ => Ratio(-j + 2) * x
    }
    def b(j: Int): Ratio = j match {
      case 0 => Ratio.Zero
      case 1 => Ratio.One
      case _ => Ratio(j - 1) + x
    }
    if (x == Ratio.Zero) (Ratio.One, true, 0)
    else modified_lentz_method(maxIter, prec, a, b)
  }
}
