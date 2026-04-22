package io.constellationnetwork.serde.codecs

import io.constellationnetwork.serde.ImmutableCodec

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.Codec

/** Evidence that `T` is a wrapper around a single `NonNegLong`. Types that fit the shape — `case class T(value: NonNegLong)` or the
  * `@newtype` equivalent — get a canonical `Codec[T]` and `ImmutableCodec[T]` for free, via the implicits on this companion.
  *
  * This is *shape-specific* derivation, not automatic derivation. The engineer still writes one explicit registration per type (wrap +
  * unwrap). That line fails to compile if `T` ever gains a second field — the refactor-silent-break mode that bans full macro/reflection
  * derivation for consensus types does not exist here.
  *
  * Example:
  * {{{
  *   // in some Shapes.scala:
  *   implicit val balanceShape: NonNegLongNewtype[Balance] =
  *     NonNegLongNewtype.instance(Balance(_), _.value)
  *
  *   // downstream — Codec[Balance] / ImmutableCodec[Balance] resolve automatically.
  *   ImmutableCodec[Balance].immutableBytes(someBalance)
  * }}}
  *
  * If a future `Balance` refactor becomes `case class Balance(value: NonNegLong, scale: Int)`, the `Balance(_)` constructor reference above
  * stops compiling — the bug is surfaced at the codec registration, not in a downstream signature that silently flips.
  */
trait NonNegLongNewtype[T] {
  def wrap(n: NonNegLong): T
  def unwrap(t: T): NonNegLong
}

object NonNegLongNewtype {
  def apply[T](implicit ev: NonNegLongNewtype[T]): NonNegLongNewtype[T] = ev

  /** Standard constructor — used at each per-type registration. */
  def instance[T](wrapFn: NonNegLong => T, unwrapFn: T => NonNegLong): NonNegLongNewtype[T] =
    new NonNegLongNewtype[T] {
      def wrap(n: NonNegLong): T = wrapFn(n)
      def unwrap(t: T): NonNegLong = unwrapFn(t)
    }

  // Note on shapeless `Generic` derivation: a `deriveFromGeneric` helper
  // using `Generic.Aux[T, NonNegLong :: HNil]` was tried and works for plain
  // case classes (e.g. `SnapshotOrdinal`), but `@newtype`-annotated classes
  // (Balance, Amount, EpochProgress, and the rest) hide their structure from
  // `Generic`. With 7/8 of our newtype-over-Long targets being `@newtype`, the
  // boilerplate savings weren't worth the inconsistency of two registration
  // patterns. Reverted to explicit `.instance(wrap, unwrap)` for all.

  /** Derived `Codec[T]`: scodec codec for any type with a `NonNegLongNewtype` witness. Wire format: 8 bytes big-endian non-negative int64 —
    * identical to the underlying `NonNegLong`'s canonical bytes, no discriminator, no tag.
    */
  implicit def derivedCodec[T](implicit ev: NonNegLongNewtype[T]): Codec[T] =
    Primitives.nonNegLongCodec.xmap(ev.wrap, ev.unwrap)

  /** Derived `ImmutableCodec[T]`: the `Codec[T]` above lifted into the typeclass layer. Hash / signing / persist all flow through this
    * without any per-type boilerplate beyond the shape registration.
    */
  implicit def derivedImmutableCodec[T](implicit ev: NonNegLongNewtype[T]): ImmutableCodec[T] =
    ImmutableCodec.fromScodecCodec(derivedCodec[T](ev))
}
