package io.constellationnetwork.serde.codecs

import cats.Order

import scodec.Codec
import scodec.codecs.{listOfN, uint16}

/** Generic scodec codec factory for plain `Map[K, V]`.
  *
  * Wire format: 2-byte count + entries in sorted-by-key order.
  *
  * Determinism: plain `Map` iteration is not ordered, so the encode path sorts explicitly using
  * `Order[K]` to produce canonical bytes. On decode we rebuild a plain `Map` (callers that need
  * sortedness should use `SortedMap` from the start).
  */
object MapCodec {

  def map[K: Order, V](keyCodec: Codec[K], valueCodec: Codec[V]): Codec[Map[K, V]] = {
    implicit val ordering: Ordering[K] = Order[K].toOrdering

    val entryCodec: Codec[(K, V)] = keyCodec.pairedWith(valueCodec)

    listOfN(uint16, entryCodec).xmap(
      list => list.toMap,
      (m: Map[K, V]) => m.toList.sortBy(_._1)
    )
  }
}
