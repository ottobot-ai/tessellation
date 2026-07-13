package io.constellationnetwork.serde.codecs

import cats.Order
import cats.data.NonEmptySet

import scala.collection.immutable.SortedSet

import scodec.codecs.{listOfN, uint16}
import scodec.{Attempt, Codec, Err}

/** Generic scodec codec factory for `NonEmptySet[A]`.
  *
  * Wire format: 2-byte length prefix (uint16) + elements in sorted order. The encode path sorts deterministically via `Order[A]`; the
  * decode path validates non-emptiness and rebuilds the sorted set. Feeding the codec a zero-length prefix yields
  * `SerdeError.ScodecFailure` — an empty `NonEmptySet` is by definition invalid.
  *
  * Signed payloads' proofs live in this shape: `NonEmptySet[SignatureProof]`. The determinism is load-bearing — two nodes serializing the
  * same `NonEmptySet` must produce bit-identical bytes, which only holds if the iteration order is a function of the values (sorted) rather
  * than insertion order.
  *
  * Not marked implicit because `A` needs an explicit `Order[A]` in scope — we don't want generic implicit derivation producing surprise
  * instances for arbitrary element types. Call sites explicitly invoke `nonEmptySet(...)` with the inner codec.
  */
object NonEmptySetCodec {

  def nonEmptySet[A: Order](inner: Codec[A]): Codec[NonEmptySet[A]] =
    make(inner, canonicalizeOnEncode = false)

  /** Variant for element codecs that normalize their source representation while decoding, such as mixed-case hex to lowercase. */
  def nonEmptySetCanonical[A: Order](inner: Codec[A]): Codec[NonEmptySet[A]] =
    make(inner, canonicalizeOnEncode = true)

  private def make[A: Order](inner: Codec[A], canonicalizeOnEncode: Boolean): Codec[NonEmptySet[A]] = {
    implicit val ordering: Ordering[A] = Order[A].toOrdering

    listOfN(uint16, inner).exmap(
      list =>
        if (!CanonicalCollectionCodec.isStrictlyIncreasing(list))
          Attempt.failure(Err("NonEmptySet decode: elements must be strictly increasing"))
        else
          NonEmptySet
            .fromSet(SortedSet.from(list))
            .fold[Attempt[NonEmptySet[A]]](
              Attempt.failure(Err("NonEmptySet decode: empty collection"))
            )(Attempt.successful),
      // NonEmptySet wraps a SortedSet and therefore also carries an arbitrary
      // construction-time Ordering. The codec Order[A] is the wire order.
      // Re-check strictness after sorting: a codec-specific canonical Order can
      // collapse two differently represented source values to the same wire
      // value (for example upper/lower-case spellings of identical hex bytes).
      // Emitting both would produce bytes that this codec's strict decoder
      // rejects, so fail closed at encode instead.
      (nes: NonEmptySet[A]) => {
        val sortedAttempt =
          if (canonicalizeOnEncode)
            CanonicalCollectionCodec.sortByCanonicalKey(nes.toSortedSet.toList, inner, identity[A], "NonEmptySet")
          else Attempt.successful(nes.toSortedSet.toList.sorted(ordering))

        sortedAttempt.flatMap { sorted =>
          if (canonicalizeOnEncode || CanonicalCollectionCodec.isStrictlyIncreasing(sorted)) Attempt.successful(sorted)
          else Attempt.failure(Err("NonEmptySet encode: elements must be strictly increasing in canonical wire order"))
        }
      }
    )
  }
}
