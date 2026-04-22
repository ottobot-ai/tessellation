package io.constellationnetwork.serde.codecs

import io.constellationnetwork.serde.ImmutableCodec

import scodec.Codec
import scodec.codecs.int64

/** Evidence that `T` is a wrapper around a single signed `Long`.
  *
  * Unlike the refined-Long shapes, there's no validation on decode — a
  * signed `Long` accepts any 8 bytes. The only type currently fitting is
  * `TransactionSalt`, which is a random nonce and carries no numeric
  * invariant.
  *
  * Wire format: 8 bytes big-endian int64. Fixed-width, random-access friendly.
  */
trait LongNewtype[T] {
  def wrap(n: Long): T
  def unwrap(t: T): Long
}

object LongNewtype {
  def apply[T](implicit ev: LongNewtype[T]): LongNewtype[T] = ev

  def instance[T](wrapFn: Long => T, unwrapFn: T => Long): LongNewtype[T] =
    new LongNewtype[T] {
      def wrap(n: Long): T = wrapFn(n)
      def unwrap(t: T): Long = unwrapFn(t)
    }

  implicit def derivedCodec[T](implicit ev: LongNewtype[T]): Codec[T] =
    int64.xmap(ev.wrap, ev.unwrap)

  implicit def derivedImmutableCodec[T](implicit ev: LongNewtype[T]): ImmutableCodec[T] =
    ImmutableCodec.fromScodecCodec(derivedCodec[T](ev))
}
