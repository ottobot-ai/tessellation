/*
 * Adapted from Bifrost (https://github.com/Topl/Bifrost), originally
 * licensed under the Mozilla Public License 2.0. The Tessellation
 * project is otherwise Apache-2.0; this file remains under MPL-2.0.
 *
 * Source: numerics/src/main/scala/co/topl/numerics/RatioOps.scala
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package io.constellationnetwork.numerics

object RatioOps {

  trait Implicits {

    implicit class Ops(ratio: Ratio) {

      def inverse: Ratio =
        Ratio(ratio.denominator, ratio.numerator)

      def abs: Ratio =
        Ratio(
          ratio.numerator match {
            case num if num < 0 => -num
            case num            => num
          },
          ratio.denominator match {
            case den if den < 0 => -den
            case den            => den
          }
        )

      def pow(n: Int): Ratio =
        Ratio(ratio.numerator.pow(n), ratio.denominator.pow(n))

      def unary_- : Ratio =
        Ratio(-ratio.numerator, ratio.denominator)

      def *(that: Int): Ratio = Ratio(ratio.numerator * that, ratio.denominator)
      def /(that: Int): Ratio = Ratio(ratio.numerator, ratio.denominator * that)
      def *(that: Long): Ratio = Ratio(ratio.numerator * that, ratio.denominator)
      def /(that: Long): Ratio = Ratio(ratio.numerator, ratio.denominator * that)
      def *(that: BigInt): Ratio = Ratio(ratio.numerator * that, ratio.denominator)

      def +(that: Ratio): Ratio =
        Ratio(
          ratio.numerator * that.denominator + that.numerator * ratio.denominator,
          ratio.denominator * that.denominator
        )

      def -(that: Ratio): Ratio =
        Ratio(
          ratio.numerator * that.denominator - that.numerator * ratio.denominator,
          ratio.denominator * that.denominator
        )

      def *(that: Ratio): Ratio =
        Ratio(ratio.numerator * that.numerator, ratio.denominator * that.denominator)

      def /(that: Ratio): Ratio =
        Ratio(ratio.numerator * that.denominator, ratio.denominator * that.numerator)

      def <(that: Ratio): Boolean =
        that.denominator * ratio.numerator < that.numerator * ratio.denominator

      def >(that: Ratio): Boolean =
        that.denominator * ratio.numerator > that.numerator * ratio.denominator

      def <=(that: Ratio): Boolean =
        that.denominator * ratio.numerator <= that.numerator * ratio.denominator

      def >=(that: Ratio): Boolean =
        that.denominator * ratio.numerator >= that.numerator * ratio.denominator

      def toBigDecimal: BigDecimal =
        BigDecimal(ratio.numerator) / BigDecimal(ratio.denominator)

      /** Lossy conversion to Double. Use only for diagnostic logging or metrics — never on the consensus path. */
      def toDouble: Double = toBigDecimal.toDouble

      def round: BigInt =
        if (ratio.numerator.abs > ratio.denominator.abs) ratio.numerator.abs / ratio.denominator.abs
        else BigInt(1)
    }
  }

  object implicits extends Implicits
}
