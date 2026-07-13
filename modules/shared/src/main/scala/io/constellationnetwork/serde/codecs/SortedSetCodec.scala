package io.constellationnetwork.serde.codecs

import cats.Order

import scala.collection.immutable.SortedSet

import scodec.codecs.{listOfN, uint16}
import scodec.{Attempt, Codec, Err}

/** Generic scodec codec factory for `SortedSet[A]`.
  *
  * Wire format: 2-byte length prefix (uint16) + elements in sorted order.
  *
  * Parallel to `NonEmptySetCodec.nonEmptySet` but accepts the empty case — `SortedSet` has no non-empty contract, unlike `NonEmptySet`.
  * Zero-length prefix on decode returns `SortedSet.empty` rather than a failure.
  *
  * Determinism: encode explicitly sorts by the codec's `Order[A]` rather than trusting the input `SortedSet`'s retained ordering; decode
  * requires that same strict order.
  *
  * Not marked implicit — call sites invoke `sortedSet(...)` explicitly with the element codec and its `Order[A]`, matching the
  * `NonEmptySetCodec` / `SortedMapCodec` pattern.
  *
  * Consensus contract: FROZEN. 2-byte count + sorted elements.
  */
object SortedSetCodec {

  def sortedSet[A: Order](inner: Codec[A]): Codec[SortedSet[A]] =
    make(inner, canonicalizeOnEncode = false)

  /** Variant for element codecs that normalize their source representation while decoding, such as mixed-case hex to lowercase. */
  def sortedSetCanonical[A: Order](inner: Codec[A]): Codec[SortedSet[A]] =
    make(inner, canonicalizeOnEncode = true)

  private def make[A: Order](inner: Codec[A], canonicalizeOnEncode: Boolean): Codec[SortedSet[A]] = {
    implicit val ordering: Ordering[A] = Order[A].toOrdering

    listOfN(uint16, inner).exmap(
      list =>
        if (CanonicalCollectionCodec.isStrictlyIncreasing(list)) Attempt.successful(SortedSet.from(list))
        else Attempt.failure(Err("SortedSet decode: elements must be strictly increasing")),
      // A SortedSet retains its construction-time Ordering. Always encode by
      // the protocol Order[A] owned by this codec.
      (s: SortedSet[A]) =>
        if (canonicalizeOnEncode)
          CanonicalCollectionCodec.sortByCanonicalKey(s.toList, inner, identity[A], "SortedSet")
        else Attempt.successful(s.toList.sorted(ordering))
    )
  }
}
