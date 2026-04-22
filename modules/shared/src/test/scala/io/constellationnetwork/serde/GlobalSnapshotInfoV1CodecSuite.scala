package io.constellationnetwork.serde

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.GlobalSnapshotInfoV1
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotInfoV1Codec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.bits.ByteVector
import weaver.FunSuite

/** Round-trip + structural suite for `GlobalSnapshotInfoV1` — 3 sorted maps.
  *
  * Exercises the `SortedMapCodec` helper end-to-end with three different value types (Hash, TransactionReference, Balance) and two
  * independent keys.
  */
object GlobalSnapshotInfoV1CodecSuite extends FunSuite {

  private val addrA = AddressCodec.unsafeFromLiteral("DAG1UUPsDext9pvuoiyNTM72SX4t1xyod4Q1uXiM")
  private val addrB = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")

  private def sample: GlobalSnapshotInfoV1 = GlobalSnapshotInfoV1(
    lastStateChannelSnapshotHashes = SortedMap(
      addrA -> Hash("aa" * 32),
      addrB -> Hash("bb" * 32)
    ),
    lastTxRefs = SortedMap(
      addrA -> TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(1L)), Hash("01" * 32))
    ),
    balances = SortedMap(
      addrA -> Balance(NonNegLong.unsafeFrom(1_000_000L)),
      addrB -> Balance(NonNegLong.unsafeFrom(2_000_000L))
    )
  )

  private def empty: GlobalSnapshotInfoV1 = GlobalSnapshotInfoV1(
    SortedMap.empty,
    SortedMap.empty,
    SortedMap.empty
  )

  test("V1 info with all maps empty is exactly 6 bytes (three uint16 zero prefixes)") {
    expect(empty.immutableBytes == ByteVector.fromValidHex("000000000000")).and(expect(empty.immutableBytes.length == 6L))
  }

  test("V1 info round-trips end-to-end") {
    val bytes = sample.immutableBytes
    expect(bytes.fromImmutableBytes[GlobalSnapshotInfoV1] == Right(sample))
  }

  test("V1 info first two bytes are the uint16 entry count of the first map") {
    val bytes = sample.immutableBytes
    // First map has 2 entries → 0x0002
    expect(bytes.take(2) == ByteVector.fromValidHex("0002"))
  }

  test("Swapping balance values between addresses changes the encoded bytes") {
    val swapped = sample.copy(
      balances = SortedMap(
        addrA -> sample.balances(addrB),
        addrB -> sample.balances(addrA)
      )
    )
    expect(sample.immutableBytes != swapped.immutableBytes).and(expect(sample.immutableBytes.length == swapped.immutableBytes.length))
  }

  test("Map order is deterministic — building the same map by different insertion order is the same bytes") {
    val a: SortedMap[Address, Balance] = SortedMap(
      addrA -> Balance(NonNegLong.unsafeFrom(1L)),
      addrB -> Balance(NonNegLong.unsafeFrom(2L))
    )
    val b: SortedMap[Address, Balance] = SortedMap(
      addrB -> Balance(NonNegLong.unsafeFrom(2L)),
      addrA -> Balance(NonNegLong.unsafeFrom(1L))
    )
    val infoA = empty.copy(balances = a)
    val infoB = empty.copy(balances = b)
    expect(infoA.immutableBytes == infoB.immutableBytes)
  }
}
