package io.constellationnetwork.serde

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateFieldId._
import io.constellationnetwork.schema.mpt.PartitionNamespace._
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, PartitionNamespace}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{immutableCodec => addressImmutableCodec}
import io.constellationnetwork.serde.codecs.instances.GlobalStateKeyCodec._
import io.constellationnetwork.serde.implicits._

import scodec.bits.ByteVector
import weaver.FunSuite

/** Suite for the first sum-type codec: `PartitionNamespace` (5-variant ADT), `GlobalStateFieldId` (19 case objects), and the
  * `GlobalStateKey` compound that binds them together.
  *
  * Byte-layout contract:
  *   - uint8 discriminator for `PartitionNamespace`: {Hypergraph=0, Empty=1, Metagraph=2, Address=3, Hash=4}
  *   - uint8 id for `GlobalStateFieldId` (0..18)
  *   - `GlobalStateKey` is four fields concatenated: network | fieldId | contract | user.
  */
object GlobalStateKeyCodecSuite extends FunSuite {

  private val addr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")

  // --- PartitionNamespace discriminator -------------------------------------

  test("HypergraphNamespace encodes as single byte 0x00") {
    val ns: PartitionNamespace = HypergraphNamespace
    expect(ns.immutableBytes == ByteVector.fromValidHex("00"))
  }

  test("EmptyNamespace encodes as single byte 0x01") {
    val ns: PartitionNamespace = EmptyNamespace
    expect(ns.immutableBytes == ByteVector.fromValidHex("01"))
  }

  private def addressBytes(a: Address): ByteVector = addressImmutableCodec.immutableBytes(a)

  test("MetagraphNamespace(addr) encodes as 0x02 + address bytes") {
    val ns: PartitionNamespace = MetagraphNamespace(addr)
    val bytes = ns.immutableBytes
    expect(bytes.head == 0x02.toByte).and(expect(bytes.tail == addressBytes(addr)))
  }

  test("AddressNamespace(addr) encodes as 0x03 + address bytes") {
    val ns: PartitionNamespace = AddressNamespace(addr)
    val bytes = ns.immutableBytes
    expect(bytes.head == 0x03.toByte).and(expect(bytes.tail == addressBytes(addr)))
  }

  test("HashNamespace(h) encodes as 0x04 + 32-byte hash") {
    val h = Hash("ab" * 32)
    val ns: PartitionNamespace = HashNamespace(h)
    val bytes = ns.immutableBytes
    expect(bytes.head == 0x04.toByte).and(expect(bytes.tail == ByteVector.fromValidHex("ab" * 32))).and(expect(bytes.length == 33L))
  }

  test("Unknown discriminator byte fails decode") {
    val bad = ByteVector.fromValidHex("ff")
    bad.fromImmutableBytes[PartitionNamespace] match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure, got $other")
    }
  }

  test("PartitionNamespace round-trips for every variant") {
    val samples: Seq[PartitionNamespace] = Seq(
      HypergraphNamespace,
      EmptyNamespace,
      MetagraphNamespace(addr),
      AddressNamespace(addr),
      HashNamespace(Hash("0" * 64))
    )
    val decoded = samples.map(_.immutableBytes.fromImmutableBytes[PartitionNamespace])
    expect(decoded == samples.map(Right(_)))
  }

  // --- GlobalStateFieldId ---------------------------------------------------

  test("GlobalStateFieldId encodes as single byte matching `toInt`") {
    val samples: Seq[GlobalStateFieldId] = Seq(
      LastStateChannelSnapshotHashes, // 0
      Balances, // 2
      ActiveDelegatedStakes, // 13
      MetagraphSyncData // 18
    )
    val actual = samples.map(_.immutableBytes)
    val expected = samples.map(f => ByteVector.fromByte(f.toInt.toByte))
    expect(actual == expected)
  }

  test("GlobalStateFieldId round-trips for every registered id") {
    val expectedInts = 0 to 34
    val ids = expectedInts.map(GlobalStateFieldId.fromInt)
    val decoded = ids.flatten.map(_.immutableBytes.fromImmutableBytes[GlobalStateFieldId])
    expect.all(
      ids.forall(_.nonEmpty),
      decoded == ids.flatten.map(Right(_))
    )
  }

  test("field 24 is the rooted genesis operator KES+VRF registry") {
    val decoded = ByteVector.fromByte(24.toByte).fromImmutableBytes[GlobalStateFieldId]
    expect.all(
      decoded == Right(GenesisOperatorKeys),
      (25 to 34).forall(GlobalStateFieldId.fromInt(_).nonEmpty)
    )
  }

  test("Unregistered GlobalStateFieldId byte fails decode") {
    val bad = ByteVector.fromValidHex("ff")
    bad.fromImmutableBytes[GlobalStateFieldId] match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure, got $other")
    }
  }

  // --- GlobalStateKey -------------------------------------------------------

  test("GlobalStateKey encodes as network | fieldId | contract | user in order") {
    val key = GlobalStateKey(
      networkNamespace = HypergraphNamespace,
      fieldId = Balances,
      contractNamespace = EmptyNamespace,
      userNamespace = AddressNamespace(addr)
    )
    // Inline the expected encoding to avoid subtype-vs-base-type implicit churn.
    val expected =
      ByteVector.fromValidHex("00") ++ // HypergraphNamespace discriminator
        ByteVector.fromByte(Balances.toInt.toByte) ++
        ByteVector.fromValidHex("01") ++ // EmptyNamespace discriminator
        (ByteVector.fromValidHex("03") ++ addressBytes(addr)) // AddressNamespace discriminator + addr
    expect(key.immutableBytes == expected)
  }

  test("GlobalStateKey round-trips") {
    val key = GlobalStateKey(
      networkNamespace = HypergraphNamespace,
      fieldId = ActiveDelegatedStakes,
      contractNamespace = MetagraphNamespace(addr),
      userNamespace = AddressNamespace(addr)
    )
    expect(key.immutableBytes.fromImmutableBytes[GlobalStateKey] == Right(key))
  }

  test("Swapping contract ↔ user namespaces produces different bytes (field-order invariant)") {
    val addr2 = AddressCodec.unsafeFromLiteral("DAG1UUPsDext9pvuoiyNTM72SX4t1xyod4Q1uXiM")
    val a = GlobalStateKey(
      HypergraphNamespace,
      Balances,
      AddressNamespace(addr),
      AddressNamespace(addr2)
    )
    val b = a.copy(contractNamespace = a.userNamespace, userNamespace = a.contractNamespace)
    expect(a.immutableBytes != b.immutableBytes)
  }

  test("helper constructors round-trip through the codec") {
    val keys = Seq(
      GlobalStateKey.metagraph(addr, LastTxRefs),
      GlobalStateKey.hypergraph(Balances, addr),
      GlobalStateKey.hypergraph(ActiveAllowSpends, addr, addr),
      GlobalStateKey.hypergraph(PriceState, Some(addr), addr),
      GlobalStateKey.hypergraph(PriceState, None, addr)
    )
    val decoded = keys.map(_.immutableBytes.fromImmutableBytes[GlobalStateKey])
    expect(decoded == keys.map(Right(_)))
  }
}
