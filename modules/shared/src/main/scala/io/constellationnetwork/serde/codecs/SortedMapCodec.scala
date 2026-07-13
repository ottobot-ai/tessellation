package io.constellationnetwork.serde.codecs

import cats.Order

import scala.collection.immutable.SortedMap

import scodec.codecs.{listOfN, uint16}
import scodec.{Attempt, Codec, Err}

/** Generic scodec codec factory for `SortedMap[K, V]`.
  *
  * Wire format: 2-byte length prefix (uint16) + entries in sorted-by-key order.
  *
  * Determinism: the encode path explicitly sorts by the codec's `Order[K]`. It does not trust the `Ordering[K]` retained by the input
  * `SortedMap`, which may differ. Two nodes serializing the same logical map produce bit-identical bytes.
  *
  * Not marked implicit — `K` needs an explicit `Order[K]` and both `K`/`V` need explicit codecs. Call sites invoke `sortedMap(keyCodec,
  * valueCodec)` explicitly.
  *
  * Consensus contract: FROZEN. 2-byte count + sorted (key, value) pairs. Changing the prefix width or the sort order breaks every
  * historical hash that covers a map field.
  */
object SortedMapCodec {

  def sortedMap[K: Order, V](keyCodec: Codec[K], valueCodec: Codec[V]): Codec[SortedMap[K, V]] =
    make(keyCodec, valueCodec, canonicalizeKeysOnEncode = false)

  /** Variant for key codecs that normalize their source representation while decoding, such as mixed-case hex to lowercase. */
  def sortedMapCanonical[K: Order, V](keyCodec: Codec[K], valueCodec: Codec[V]): Codec[SortedMap[K, V]] =
    make(keyCodec, valueCodec, canonicalizeKeysOnEncode = true)

  private def make[K: Order, V](
    keyCodec: Codec[K],
    valueCodec: Codec[V],
    canonicalizeKeysOnEncode: Boolean
  ): Codec[SortedMap[K, V]] = {
    implicit val ordering: Ordering[K] = Order[K].toOrdering

    val entryCodec: Codec[(K, V)] = keyCodec.pairedWith(valueCodec)

    listOfN(uint16, entryCodec).exmap(
      list =>
        if (CanonicalCollectionCodec.isStrictlyIncreasing(list.map(_._1))) Attempt.successful(SortedMap.from(list))
        else Attempt.failure(Err("SortedMap decode: keys must be strictly increasing")),
      // A SortedMap carries its own Ordering, which need not agree with the
      // protocol Order[K] supplied to this codec. Canonicalize explicitly so
      // logically equal maps cannot hash differently based on construction.
      (m: SortedMap[K, V]) =>
        if (canonicalizeKeysOnEncode)
          CanonicalCollectionCodec.sortByCanonicalKey(m.toList, keyCodec, (entry: (K, V)) => entry._1, "SortedMap")
        else Attempt.successful(m.toList.sortBy(_._1)(ordering))
    )
  }

  /** Encode side asserts the input is genuinely sorted. Useful when the call site builds the map and wants the codec to surface any
    * sort-order bug at encode time. Kept as a separate entry point because the default `sortedMap` is already deterministic without this
    * extra check.
    */
  def sortedMapStrict[K: Order, V](keyCodec: Codec[K], valueCodec: Codec[V]): Codec[SortedMap[K, V]] = {
    implicit val ordering: Ordering[K] = Order[K].toOrdering

    val entryCodec: Codec[(K, V)] = keyCodec.pairedWith(valueCodec)

    listOfN(uint16, entryCodec).exmap(
      list =>
        if (CanonicalCollectionCodec.isStrictlyIncreasing(list.map(_._1))) Attempt.successful(SortedMap.from(list))
        else Attempt.failure(Err("SortedMap decode: keys must be strictly increasing")),
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
