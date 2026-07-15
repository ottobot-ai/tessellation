package io.constellationnetwork.serde

import cats.Order
import cats.data.NonEmptySet

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.serde.codecs.MapCodec.map
import io.constellationnetwork.serde.codecs.NonEmptySetCodec.nonEmptySet
import io.constellationnetwork.serde.codecs.SetCodec.set
import io.constellationnetwork.serde.codecs.SortedMapCodec.{sortedMap, sortedMapStrict}
import io.constellationnetwork.serde.codecs.SortedSetCodec.sortedSet

import scodec.Codec
import scodec.bits.BitVector
import scodec.codecs.{uint16, uint8}
import weaver.FunSuite

object CanonicalCollectionCodecsSuite extends FunSuite {

  private implicit val intOrder: Order[Int] = Order.fromOrdering[Int]

  private val entryCodec: Codec[(Int, Int)] = uint8.pairedWith(uint8)

  private def encoded[A](codec: Codec[A], value: A): BitVector =
    codec.encode(value).require

  private def encodedEntries(entries: List[(Int, Int)]): BitVector =
    encoded(uint16, entries.size) ++ entries.foldLeft(BitVector.empty) {
      case (bits, entry) =>
        bits ++ encoded(entryCodec, entry)
    }

  private def encodedElements(elements: List[Int]): BitVector =
    encoded(uint16, elements.size) ++ elements.foldLeft(BitVector.empty) {
      case (bits, element) =>
        bits ++ encoded(uint8, element)
    }

  private def rejects[A](codec: Codec[A], bits: BitVector): Boolean =
    codec.decodeValue(bits).toEither.isLeft

  test("Map codec preserves canonical round trips") {
    val codec = map(uint8, uint8)
    val value = Map(3 -> 30, 1 -> 10, 2 -> 20)

    expect(codec.decodeValue(encoded(codec, value)).require == value)
  }

  test("Map codec rejects duplicate keys") {
    expect(rejects(map(uint8, uint8), encodedEntries(List(1 -> 10, 1 -> 20))))
  }

  test("Map codec rejects noncanonical key ordering") {
    expect(rejects(map(uint8, uint8), encodedEntries(List(2 -> 20, 1 -> 10))))
  }

  test("SortedMap codecs preserve canonical round trips") {
    val value = SortedMap(3 -> 30, 1 -> 10, 2 -> 20)
    val regular = sortedMap(uint8, uint8)
    val strict = sortedMapStrict(uint8, uint8)

    expect(regular.decodeValue(encoded(regular, value)).require == value)
      .and(expect(strict.decodeValue(encoded(strict, value)).require == value))
  }

  test("SortedMap codec ignores the value's construction-time ordering") {
    val codec = sortedMap(uint8, uint8)
    val ascending = SortedMap(1 -> 10, 2 -> 20, 3 -> 30)
    val descending = ascending.foldLeft(SortedMap.empty[Int, Int](Ordering.Int.reverse))(_ + _)

    expect(encoded(codec, descending) == encoded(codec, ascending))
  }

  test("SortedMap codecs reject duplicate keys") {
    val duplicate = encodedEntries(List(1 -> 10, 1 -> 20))

    expect(rejects(sortedMap(uint8, uint8), duplicate))
      .and(expect(rejects(sortedMapStrict(uint8, uint8), duplicate)))
  }

  test("SortedMap codecs reject noncanonical key ordering") {
    val descending = encodedEntries(List(2 -> 20, 1 -> 10))

    expect(rejects(sortedMap(uint8, uint8), descending))
      .and(expect(rejects(sortedMapStrict(uint8, uint8), descending)))
  }

  test("Set codecs preserve canonical round trips") {
    val sortedValue = SortedSet(3, 1, 2)
    val plainValue = Set(3, 1, 2)
    val sortedCodec = sortedSet(uint8)
    val plainCodec = set(uint8)

    expect(sortedCodec.decodeValue(encoded(sortedCodec, sortedValue)).require == sortedValue)
      .and(expect(plainCodec.decodeValue(encoded(plainCodec, plainValue)).require == plainValue))
  }

  test("SortedSet codec ignores the value's construction-time ordering") {
    val codec = sortedSet(uint8)
    val ascending = SortedSet(1, 2, 3)
    val descending = SortedSet.empty[Int](Ordering.Int.reverse) ++ ascending

    expect(encoded(codec, descending) == encoded(codec, ascending))
  }

  test("Set codecs reject duplicate elements") {
    val duplicate = encodedElements(List(1, 1))

    expect(rejects(sortedSet(uint8), duplicate))
      .and(expect(rejects(set(uint8), duplicate)))
  }

  test("Set codecs reject noncanonical element ordering") {
    val descending = encodedElements(List(2, 1))

    expect(rejects(sortedSet(uint8), descending))
      .and(expect(rejects(set(uint8), descending)))
  }

  test("NonEmptySet codec preserves canonical round trips") {
    val codec = nonEmptySet(uint8)
    val value = NonEmptySet.of(3, 1, 2)

    expect(codec.decodeValue(encoded(codec, value)).require == value)
  }

  test("NonEmptySet codec ignores the wrapped set's construction-time ordering") {
    val codec = nonEmptySet(uint8)
    val ascending = NonEmptySet.of(1, 2, 3)
    val descending = NonEmptySet.fromSetUnsafe(SortedSet.empty[Int](Ordering.Int.reverse) ++ List(1, 2, 3))

    expect(encoded(codec, descending) == encoded(codec, ascending))
  }

  test("NonEmptySet codec rejects duplicate elements") {
    expect(rejects(nonEmptySet(uint8), encodedElements(List(1, 1))))
  }

  test("NonEmptySet codec rejects noncanonical element ordering") {
    expect(rejects(nonEmptySet(uint8), encodedElements(List(2, 1))))
  }
}
