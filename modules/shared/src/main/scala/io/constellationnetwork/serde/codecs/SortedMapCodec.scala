package io.constellationnetwork.serde.codecs

import cats.Order

import scala.collection.immutable.SortedMap

import scodec.codecs.{listOfN, uint16}
import scodec.{Attempt, Codec}

/** Generic scodec codec factory for `SortedMap[K, V]`.
  *
  * Wire format: 2-byte length prefix (uint16) + entries in sorted-by-key order.
  *
  * Determinism: the encode path iterates via `SortedMap`'s natural iteration, which is already
  * `Order[K]`-sorted. Two nodes serializing the same logical map produce bit-identical bytes.
  *
  * Not marked implicit — `K` needs an explicit `Order[K]` and both `K`/`V` need explicit codecs.
  * Call sites invoke `sortedMap(keyCodec, valueCodec)` explicitly.
  *
  * Consensus contract: FROZEN. 2-byte count + sorted (key, value) pairs. Changing the prefix
  * width or the sort order breaks every historical hash that covers a map field.
  */
object SortedMapCodec {

  def sortedMap[K: Order, V](keyCodec: Codec[K], valueCodec: Codec[V]): Codec[SortedMap[K, V]] = {
    implicit val ordering: Ordering[K] = Order[K].toOrdering

    val entryCodec: Codec[(K, V)] = keyCodec.pairedWith(valueCodec)

    listOfN(uint16, entryCodec).xmap(
      list => SortedMap.from(list),
      (m: SortedMap[K, V]) => m.toList
    )
  }

  /** Encode side asserts the input is genuinely sorted. Useful when the call site builds the map
    * and wants the codec to surface any sort-order bug at encode time. Kept as a separate entry
    * point because the default `sortedMap` is already deterministic without this extra check.
    */
  def sortedMapStrict[K: Order, V](keyCodec: Codec[K], valueCodec: Codec[V]): Codec[SortedMap[K, V]] = {
    implicit val ordering: Ordering[K] = Order[K].toOrdering

    val entryCodec: Codec[(K, V)] = keyCodec.pairedWith(valueCodec)

    listOfN(uint16, entryCodec).exmap(
      list => Attempt.successful(SortedMap.from(list)),
      (m: SortedMap[K, V]) => {
        val keysSorted = m.keys.toList
        val resorted = keysSorted.sorted(ordering)
        if (keysSorted == resorted) Attempt.successful(m.toList)
        else
          Attempt.failure(
            scodec.Err(s"SortedMap encode: keys not in sorted order — got $keysSorted, expected $resorted")
          )
      }
    )
  }
}
