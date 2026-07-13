package io.constellationnetwork.serde.codecs

import cats.Order

import scodec.{Attempt, Codec, Err}

private[codecs] object CanonicalCollectionCodec {

  def isStrictlyIncreasing[A: Order](values: List[A]): Boolean =
    values.zip(values.drop(1)).forall { case (left, right) => Order[A].lt(left, right) }

  /** Canonicalize each sort key exactly once, then sort the original values by those canonical keys. Codec failures stay in `Attempt`
    * rather than escaping through `Order.compare`, and serialization work is O(n) rather than O(n log n).
    */
  def sortByCanonicalKey[A, K: Order](
    values: List[A],
    keyCodec: Codec[K],
    key: A => K,
    collectionName: String
  ): Attempt[List[A]] =
    values
      .foldLeft(Attempt.successful(List.empty[(A, K)])) { (accAttempt, value) =>
        for {
          acc <- accAttempt
          bits <- keyCodec.encode(key(value))
          canonicalKey <- keyCodec.complete.decodeValue(bits)
        } yield (value -> canonicalKey) :: acc
      }
      .flatMap { decoratedReversed =>
        val decorated = decoratedReversed.reverse.sortBy(_._2)(Order[K].toOrdering)
        val canonicalKeys = decorated.map(_._2)

        if (isStrictlyIncreasing(canonicalKeys)) Attempt.successful(decorated.map(_._1))
        else Attempt.failure(Err(s"$collectionName encode: keys/elements must be unique in canonical wire form"))
      }
}
