/*
 * Adapted from Bifrost (https://github.com/Topl/Bifrost), originally
 * licensed under the Mozilla Public License 2.0. The Tessellation
 * project is otherwise Apache-2.0; this file remains under MPL-2.0.
 *
 * Source: numerics/src/main/scala/co/topl/numerics/interpreters/LentzMethod.scala
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package io.constellationnetwork.numerics.interpreters

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._

/** Modified Lentz's method (Numerical Recipes in Fortran 77, 2nd ed., §5.2) for evaluating continued-fraction
  * expansions in exact `Ratio` arithmetic. The two interpreters that build on this — [[Log1pInterpreter]] and
  * [[ExpInterpreter]] — supply different `a(j)` / `b(j)` coefficient sequences but share the same iterative
  * skeleton.
  */
trait LentzMethod {

  /** Evaluate the continued fraction defined by the coefficient sequences `a(j)` and `b(j)` until the relative
    * change drops below `10^-(prec+1)` or `maxIter` iterations are reached.
    *
    * @return tuple `(approximation, didNotConverge, iterations)`
    */
  private[numerics] def modified_lentz_method(
    maxIter: Int,
    prec: Int,
    a: Int => Ratio,
    b: Int => Ratio
  ): (Ratio, Boolean, Int) = {
    val bigFactor = BigInt(10).pow(prec + 10)
    val tinyFactor = Ratio(1, bigFactor)
    val truncationError: Ratio = Ratio(1, BigInt(10).pow(prec + 1))
    var fj: Ratio = if (b(0) == Ratio.Zero) tinyFactor else b(0)
    var cj: Ratio = fj
    var dj: Ratio = Ratio.Zero
    var deltaj = Ratio.One
    var error: Boolean = true
    def loop(j: Int): Unit = {
      dj = b(j) + a(j) * dj
      if (dj == Ratio.Zero) dj = tinyFactor
      cj = b(j) + a(j) / cj
      if (cj == Ratio.Zero) cj = tinyFactor
      dj = Ratio(dj.denominator, dj.numerator)
      deltaj = cj * dj
      fj = fj * deltaj
      error = j match {
        case _ if j > 1 => (deltaj - Ratio.One).abs > truncationError
        case _          => true
      }
    }
    var j = 1
    while (j < maxIter + 1 && error) {
      loop(j)
      j = j + 1
    }
    if (fj.denominator < 0) fj = Ratio(-fj.numerator, -fj.denominator)
    (fj, error, j)
  }
}
