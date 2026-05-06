package io.constellationnetwork.serde

import scala.collection.immutable.{SortedMap, TreeMap}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Diagnostic suite for #70: dl1 follower computes a `lastCurrencySnapshots` accumulator with the same scodec bytes as gl0's leader but a
  * different `toString.hashCode`. This suite probes which construction path (SortedMap.empty vs deserialize, ++ ordering, Some(empty) vs
  * None semantics under various builders) produces equal-bytes-but-unequal-toString.
  */
object CurrencySnapshotInfoToStringDivergenceSuite extends FunSuite {

  private val addr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")

  private def info(someEmpties: Boolean): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap(addr -> TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(5L)), Hash("1" * 64))),
      balances = SortedMap(addr -> Balance(NonNegLong.unsafeFrom(500L))),
      lastMessages = if (someEmpties) Some(SortedMap.empty) else None,
      lastFeeTxRefs = if (someEmpties) Some(SortedMap.empty) else None,
      lastAllowSpendRefs = if (someEmpties) Some(SortedMap.empty) else None,
      activeAllowSpends = if (someEmpties) Some(SortedMap.empty) else None,
      globalSnapshotSyncView = if (someEmpties) Some(SortedMap.empty) else None,
      lastTokenLockRefs = if (someEmpties) Some(SortedMap.empty) else None,
      activeTokenLocks = if (someEmpties) Some(SortedMap.empty) else None
    )

  // --- 1. Some(empty) vs None: do they produce different scodec bytes? ---

  test("Some(SortedMap.empty) and None produce DIFFERENT scodec bytes") {
    val withSomeEmpty = info(someEmpties = true)
    val withNone = info(someEmpties = false)
    val bytesA = withSomeEmpty.immutableBytes
    val bytesB = withNone.immutableBytes
    expect(bytesA != bytesB)
  }

  // --- 2. Round-trip preserves toString.hashCode? ---

  test("CurrencySnapshotInfo round-trip preserves toString.hashCode (Some(empty) form)") {
    val original = info(someEmpties = true)
    val roundTripped = original.immutableBytes.fromImmutableBytes[CurrencySnapshotInfo].toOption.get
    val origHash = "%08x".format(original.toString.hashCode)
    val rtHash = "%08x".format(roundTripped.toString.hashCode)
    expect.eql(origHash, rtHash)
  }

  test("CurrencySnapshotInfo round-trip preserves toString.hashCode (None form)") {
    val original = info(someEmpties = false)
    val roundTripped = original.immutableBytes.fromImmutableBytes[CurrencySnapshotInfo].toOption.get
    val origHash = "%08x".format(original.toString.hashCode)
    val rtHash = "%08x".format(roundTripped.toString.hashCode)
    expect.eql(origHash, rtHash)
  }

  // --- 3. SortedMap construction variants: do they have same toString.hashCode? ---

  test("SortedMap built from .empty + tuple == SortedMap(tuple) toString-wise") {
    val a: SortedMap[Address, Balance] = SortedMap.empty[Address, Balance] + (addr -> Balance(NonNegLong.unsafeFrom(1L)))
    val b: SortedMap[Address, Balance] = SortedMap(addr -> Balance(NonNegLong.unsafeFrom(1L)))
    expect.eql(a.toString, b.toString)
  }

  test("SortedMap built from TreeMap.from vs SortedMap.from: same toString") {
    val pairs = List(addr -> Balance(NonNegLong.unsafeFrom(1L)))
    val a: SortedMap[Address, Balance] = SortedMap.from(pairs)
    val b: SortedMap[Address, Balance] = TreeMap.from(pairs)
    expect.eql(a.toString, b.toString)
  }

  test("SortedMap[K,V].view.to(SortedMap) vs SortedMap.from: same toString") {
    val pairs = SortedMap(addr -> Balance(NonNegLong.unsafeFrom(1L)))
    val viaViewTo = pairs.view.to(SortedMap)
    val fresh: SortedMap[Address, Balance] = SortedMap.from(pairs)
    expect.eql(viaViewTo.toString, fresh.toString)
  }

  // --- 4. Outer container divergence: probe whether ++ on SortedMap[Address, Either[...]]
  //        with one entry produces a different toString depending on build order. ---

  test("Outer container: SortedMap.empty + Right(tuple)  vs  SortedMap.empty.updated(addr, Right(tuple))") {
    val tuple: (CurrencySnapshotInfo, Int) = (info(someEmpties = true), 0)
    type V = Either[String, (CurrencySnapshotInfo, Int)]
    val a: SortedMap[Address, V] = SortedMap.empty[Address, V] + (addr -> Right(tuple))
    val b: SortedMap[Address, V] = SortedMap.empty[Address, V].updated(addr, Right(tuple))
    expect.eql(a.toString, b.toString)
  }

  test("Outer container: SortedMap(addr -> Right(tuple)) toString stable across two construction calls") {
    val tuple: (CurrencySnapshotInfo, Int) = (info(someEmpties = true), 0)
    type V = Either[String, (CurrencySnapshotInfo, Int)]
    val a: SortedMap[Address, V] = SortedMap(addr -> Right(tuple))
    val b: SortedMap[Address, V] = SortedMap(addr -> Right(tuple))
    expect.eql(a.toString.hashCode, b.toString.hashCode)
  }

  // --- 5. Probing array fields & sets: do equal SortedSet-of-Signed objects produce same toString? ---

  test("Two equal CurrencySnapshotInfo instances have equal toString") {
    val a = info(someEmpties = true)
    val b = info(someEmpties = true)
    expect.eql(a.toString, b.toString)
  }

  test("CurrencySnapshotInfo with Some(empty) and CurrencySnapshotInfo with Some(SortedMap()) — are these distinguishable?") {
    val a = info(someEmpties = true)
    val b = a.copy(lastMessages = Some(SortedMap()))
    expect.eql(a.toString, b.toString)
  }

  // --- 6. Array[Byte] hypothesis: DataApplicationPart has Array[Byte] fields whose toString
  //        is reference-identity-based. Two instances with EQUAL content but DIFFERENT
  //        Array references have different toString but should serialize to equal scodec bytes. ---

  test("Array[Byte].toString returns reference identity not content") {
    val a = Array[Byte](1, 2, 3)
    val b = Array[Byte](1, 2, 3)
    // Two distinct Array[Byte] with equal content produce DIFFERENT toString (reference-based)
    expect(a.toString != b.toString)
    expect(a.sameElements(b))
  }

  test("DataApplicationPart with equal-content Array[Byte] but different references: toString DIFFERS") {
    val a = DataApplicationPart(
      onChainState = Array[Byte](1, 2, 3),
      blocks = List(Array[Byte](4, 5, 6), Array[Byte](7, 8, 9)),
      calculatedStateProof = Hash("a" * 64),
      updateHashes = None
    )
    val b = DataApplicationPart(
      onChainState = Array[Byte](1, 2, 3),
      blocks = List(Array[Byte](4, 5, 6), Array[Byte](7, 8, 9)),
      calculatedStateProof = Hash("a" * 64),
      updateHashes = None
    )
    val aHash = "%08x".format(a.toString.hashCode)
    val bHash = "%08x".format(b.toString.hashCode)
    // EXPECT they differ — Array[Byte] toString is reference-based
    expect(aHash != bHash)
  }

  test("SortedMap containing values with Array[Byte] inside: toString DIFFERS for distinct array refs with same content") {
    val a: SortedMap[Address, Array[Byte]] = SortedMap(addr -> Array[Byte](1, 2, 3))
    val b: SortedMap[Address, Array[Byte]] = SortedMap(addr -> Array[Byte](1, 2, 3))
    val aHash = "%08x".format(a.toString.hashCode)
    val bHash = "%08x".format(b.toString.hashCode)
    expect(aHash != bHash)
  }
}
