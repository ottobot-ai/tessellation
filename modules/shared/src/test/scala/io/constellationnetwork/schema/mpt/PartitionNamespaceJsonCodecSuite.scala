package io.constellationnetwork.schema.mpt

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.PartitionNamespace._
import io.constellationnetwork.security.hash.Hash

import io.circe.syntax._
import weaver.FunSuite

/** JSON (Circe) round-trip for `PartitionNamespace`.
  *
  * Distinct from `GlobalStateKeyCodecSuite` (binary/scodec `immutableBytes`) and
  * `GlobalStateKeySerializationSuite` (one-way `toHex`): this guards the *JSON* codec used on the
  * wire (the inclusion-proof / `GlobalStateProofRoutes` path), which previously decoded the address
  * field via `Address.fromBytes(s.getBytes)` — `fromBytes` SHA-256-hashes its input
  * (`address.scala:30-40`), so it produced the address OF THE HASH OF the address string, making
  * `Address`/`Metagraph` namespaces non-round-trippable over JSON. The fix decodes via the canonical
  * `Decoder[Address]` (the exact inverse of the encoder's `addr.value.value`).
  */
object PartitionNamespaceJsonCodecSuite extends FunSuite {

  // Mint a valid DAG address from a seed (legitimate use of fromBytes — derive-from-bytes; the
  // codec bug was misusing fromBytes in the *decoder* on an already-formed address string).
  private val addr: Address = Address.fromBytes("partition-namespace-json-codec-test-seed".getBytes)

  private def roundTrips(ns: PartitionNamespace): Boolean =
    ns.asJson.as[PartitionNamespace] == Right(ns)

  test("AddressNamespace JSON round-trips (regression: was lossy via Address.fromBytes)") {
    expect(roundTrips(AddressNamespace(addr)))
  }

  test("MetagraphNamespace JSON round-trips (regression: was lossy via Address.fromBytes)") {
    expect(roundTrips(MetagraphNamespace(addr)))
  }

  test("Hash / Hypergraph / Empty / System namespaces JSON round-trip") {
    expect.all(
      roundTrips(HashNamespace(Hash("ab" * 32))),
      roundTrips(HypergraphNamespace),
      roundTrips(EmptyNamespace),
      roundTrips(SystemNamespace(SystemNamespaceLabel.ActiveAddressIndex))
    )
  }
}
