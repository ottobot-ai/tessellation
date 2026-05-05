/*
 * Adapted from Bifrost (https://github.com/Topl/Bifrost), originally
 * licensed under the Mozilla Public License 2.0. The Tessellation
 * project is otherwise Apache-2.0; this file remains under MPL-2.0.
 *
 * Source: models/src/main/scala/co/topl/models/utility/Ratio.scala
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package io.constellationnetwork.numerics

import scala.annotation.tailrec

/** Exact rational number with `numerator / denominator` reduced by gcd at construction time. Used throughout the
  * Taktikos consensus path (eligibility threshold, attestation weight, finality threshold) so that two honest nodes
  * compute byte-identical results regardless of JVM, CPU, or JIT tier — eliminating the IEEE 754 nondeterminism risk
  * that Double would introduce.
  */
case class Ratio(numerator: BigInt, denominator: BigInt, greatestCommonDenominator: BigInt) {

  override def toString(): String =
    numerator.toString + (if (denominator != 1) "/" + denominator else "")

  override def equals(that: Any): Boolean =
    that match {
      case that: Ratio => numerator == that.numerator && denominator == that.denominator
      case _           => false
    }

  override def hashCode: Int =
    41 * numerator.hashCode() + denominator.hashCode()
}

object Ratio {

  val One: Ratio = Ratio(1)
  val Zero: Ratio = Ratio(0)
  val NegativeOne: Ratio = Ratio(-1)

  def apply(n: BigInt): Ratio = apply(n, BigInt(1))

  def apply(n: BigInt, d: BigInt): Ratio = {
    val gcdVal = gcd(n, d)
    Ratio(n / gcdVal, d / gcdVal, gcdVal)
  }

  def apply(i: Int): Ratio = Ratio(BigInt(i), BigInt(1))

  def apply(n: Int, d: Int): Ratio = apply(BigInt(n), BigInt(d))

  /** Convert a Double to Ratio with `prec` decimal digits of precision. Loses precision; intended for parsing
    * Double-typed config (env vars) into the consensus-deterministic representation at boot time. After this point
    * the value never touches Double again.
    */
  def apply(double: Double, prec: Int): Ratio = {
    val d = BigInt(10).pow(prec)
    val n = (BigDecimal(double).setScale(prec, BigDecimal.RoundingMode.DOWN) * BigDecimal(d)).toBigInt
    new Ratio(n, d, gcd(n, d))
  }

  @tailrec
  private def gcd(a: BigInt, b: BigInt): BigInt =
    if (b == 0) a else gcd(b, a % b)
}
