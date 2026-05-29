package io.constellationnetwork.schema.nakamoto.follow

import cats.Eq

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.GlobalStateFieldId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.prover.attestation.MerklePatriciaRangeProof

import io.circe._
import io.circe.syntax._

/** gl0 → gl1 follow-proof payload (Axis 2, Slice 1 — see `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`).
  *
  * Carries, for one finalized global ordinal, everything a follower needs to verify the slice of global state it actually consumes
  * (balances + last-ref maps) WITHOUT re-executing the snapshot. The follower verifies this against the trusted root it already holds —
  * `signedSnapshot.stateProof.mptRoot` — via [[FollowVerifyCore.verifyConsumedFields]].
  *
  *   - `ordinal` — the finalized global ordinal this proof is anchored at. Carried for the follower's bookkeeping; the cryptographic anchor
  *     is `committedRoot`.
  *   - `committedRoot` — the MPT root the prover built the range proofs against. The verifier rejects the whole payload unless this equals
  *     the follower's trusted `attestedRoot` (contract bar #1). Every range proof in `fields` is rooted at this same hash.
  *   - `fields` — per consumed [[GlobalStateFieldId]], a [[MerklePatriciaRangeProof]] over that field's '''full''' key-range. The range proof
  *     proves structure + inclusion + exclusion boundaries (so neither a hidden update nor a forged absence can pass), but its leaves carry
  *     only `dataDigest` — NOT the value bytes.
  *   - `values` — per field, the `(keyHex → valueHex)` pairs the leaves commit to. The value bytes travel alongside the proof because the
  *     range proof's leaves only hold `dataDigest`; the verifier binds `Hasher.hashBytes(valueHex.toBytes) == leaf.dataDigest` internally
  *     (contract bar #2). Mirrors how [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProof.value]] carries the
  *     value next to the proof. `valueHex` is the scodec `ImmutableCodec`-encoded value — the exact bytes the global MPT writer feeds the
  *     producer (`MptStore.insert[V]` → `ImmutableCodec[V].immutableBytes`), so `hashBytes` reproduces the leaf's `dataDigest` exactly.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`): fresh payload for the follow path, no compat ceremony. Field order
  * here is the protocol contract once Slice 2 wires it onto the wire — add a field = bump the case class explicitly, same discipline as
  * [[io.constellationnetwork.schema.sharding.ShardCheckpoint]].
  */
final case class GlobalFollowProof(
  ordinal: SnapshotOrdinal,
  committedRoot: Hash,
  fields: SortedMap[GlobalStateFieldId, MerklePatriciaRangeProof],
  values: SortedMap[GlobalStateFieldId, SortedMap[Hex, Hex]]
)

object GlobalFollowProof {

  // `GlobalStateFieldId` has no circe KeyEncoder/KeyDecoder (only Encoder[Int]); encode the maps as
  // association lists keyed by the fieldId's Int code so the payload round-trips without inventing a
  // key-codec on the schema type. `Hex` has KeyEncoder/KeyDecoder, so the inner value map is a plain object.
  private implicit val fieldRangeMapEncoder: Encoder[SortedMap[GlobalStateFieldId, MerklePatriciaRangeProof]] =
    Encoder.encodeList[(GlobalStateFieldId, MerklePatriciaRangeProof)].contramap(_.toList)

  private implicit val fieldRangeMapDecoder: Decoder[SortedMap[GlobalStateFieldId, MerklePatriciaRangeProof]] =
    Decoder.decodeList[(GlobalStateFieldId, MerklePatriciaRangeProof)].map(pairs => SortedMap.from(pairs))

  private implicit val fieldValuesMapEncoder: Encoder[SortedMap[GlobalStateFieldId, SortedMap[Hex, Hex]]] =
    Encoder.encodeList[(GlobalStateFieldId, SortedMap[Hex, Hex])].contramap(_.toList)

  private implicit val fieldValuesMapDecoder: Decoder[SortedMap[GlobalStateFieldId, SortedMap[Hex, Hex]]] =
    Decoder.decodeList[(GlobalStateFieldId, SortedMap[Hex, Hex])].map(pairs => SortedMap.from(pairs))

  implicit val encoder: Encoder[GlobalFollowProof] = (p: GlobalFollowProof) =>
    Json.obj(
      "ordinal" -> p.ordinal.asJson,
      "committedRoot" -> p.committedRoot.asJson,
      "fields" -> p.fields.asJson,
      "values" -> p.values.asJson
    )

  implicit val decoder: Decoder[GlobalFollowProof] = (c: HCursor) =>
    for {
      ordinal <- c.downField("ordinal").as[SnapshotOrdinal]
      committedRoot <- c.downField("committedRoot").as[Hash]
      fields <- c.downField("fields").as[SortedMap[GlobalStateFieldId, MerklePatriciaRangeProof]]
      values <- c.downField("values").as[SortedMap[GlobalStateFieldId, SortedMap[Hex, Hex]]]
    } yield GlobalFollowProof(ordinal, committedRoot, fields, values)

  // `MerklePatriciaRangeProof` has no Eq instance; compare structurally via the canonical JSON encoding,
  // which is byte-stable (sorted maps, deterministic field order). Eq over the whole payload follows.
  implicit val eq: Eq[GlobalFollowProof] = Eq.by[GlobalFollowProof, Json](_.asJson)
}
