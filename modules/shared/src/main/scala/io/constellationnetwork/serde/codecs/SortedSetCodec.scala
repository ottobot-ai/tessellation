package io.constellationnetwork.serde.codecs

import cats.Order

import scala.collection.immutable.SortedSet

import scodec.Codec
import scodec.codecs.{listOfN, uint16}

/** Generic scodec codec factory for `SortedSet[A]`.
  *
  * Wire format: 2-byte length prefix (uint16) + elements in sorted order.
  *
  * Parallel to `NonEmptySetCodec.nonEmptySet` but accepts the empty case — `SortedSet` has no non-empty contract, unlike `NonEmptySet`.
  * Zero-length prefix on decode returns `SortedSet.empty` rather than a failure.
  *
  * Determinism: `SortedSet`'s natural iteration is already `Order[A]`-sorted; both encode and decode go through the same sort order.
  *
  * Not marked implicit — call sites invoke `sortedSet(...)` explicitly with the element codec and its `Order[A]`, matching the
  * `NonEmptySetCodec` / `SortedMapCodec` pattern.
  *
  * Consensus contract: FROZEN. 2-byte count + sorted elements.
  */
object SortedSetCodec {

  def sortedSet[A: Order](inner: Codec[A]): Codec[SortedSet[A]] = {
    implicit val ordering: Ordering[A] = Order[A].toOrdering

    listOfN(uint16, inner).xmap(
      list => SortedSet.from(list),
      (s: SortedSet[A]) => s.toList
    )
  }
}
