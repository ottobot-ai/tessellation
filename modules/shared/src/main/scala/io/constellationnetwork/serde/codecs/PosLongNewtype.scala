package io.constellationnetwork.serde.codecs

import io.constellationnetwork.serde.ImmutableCodec

import eu.timepit.refined.types.numeric.PosLong
import scodec.Codec

/** Evidence that `T` is a wrapper around a single `PosLong`. Parallel to
  * `NonNegLongNewtype` — same shape, different refinement predicate (> 0
  * rather than ≥ 0).
  *
  * Types that fit: `TransactionAmount`, `SwapAmount`, `TokenLockAmount`,
  * `Counter`, and any future monetary-amount-like newtype that must be
  * strictly positive.
  *
  * Refactor safety identical to `NonNegLongNewtype`: the one-line
  * registration with `T(_)` stops compiling if `T` gains a second field.
  */
trait PosLongNewtype[T] {
  def wrap(n: PosLong): T
  def unwrap(t: T): PosLong
}

object PosLongNewtype {
  def apply[T](implicit ev: PosLongNewtype[T]): PosLongNewtype[T] = ev

  def instance[T](wrapFn: PosLong => T, unwrapFn: T => PosLong): PosLongNewtype[T] =
    new PosLongNewtype[T] {
      def wrap(n: PosLong): T = wrapFn(n)
      def unwrap(t: T): PosLong = unwrapFn(t)
    }

  implicit def derivedCodec[T](implicit ev: PosLongNewtype[T]): Codec[T] =
    Primitives.posLongCodec.xmap(ev.wrap, ev.unwrap)

  implicit def derivedImmutableCodec[T](implicit ev: PosLongNewtype[T]): ImmutableCodec[T] =
    ImmutableCodec.fromScodecCodec(derivedCodec[T](ev))
}
