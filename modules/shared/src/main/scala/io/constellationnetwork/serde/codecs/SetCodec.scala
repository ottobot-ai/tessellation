package io.constellationnetwork.serde.codecs

import cats.Order

import scala.collection.immutable.SortedSet

import io.constellationnetwork.serde.codecs.SortedSetCodec.{sortedSet, sortedSetCanonical}

import scodec.Codec

/** Canonical scodec codec for an unordered `Set[A]`.
  *
  * Scala's `Set` has no stable iteration order, so a structural encoding of it would be non-deterministic — unacceptable for a wire form
  * that must be bit-exact across nodes. This codec imposes a canonical order: encode sorts the elements by `Order[A]` (delegating to the
  * existing [[SortedSetCodec.sortedSet]] spec — a uint16 count prefix followed by the element codec in ascending order), and decode returns
  * a plain `Set[A]`. Round-trips by value.
  */
object SetCodec {
  def set[A: Order](inner: Codec[A]): Codec[Set[A]] =
    sortedSet(inner).xmap[Set[A]](
      ss => ss.toSet,
      s => SortedSet.empty[A](Order[A].toOrdering) ++ s
    )

  def setCanonical[A: Order](inner: Codec[A]): Codec[Set[A]] =
    sortedSetCanonical(inner).xmap[Set[A]](
      ss => ss.toSet,
      s => SortedSet.empty[A](Order[A].toOrdering) ++ s
    )
}
