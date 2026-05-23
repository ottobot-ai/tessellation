package io.constellationnetwork.schema.sharding

import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** Fraud-proof envelope — wire shape reserved in v1, dispute handler ships in v2.
  *
  * Purpose: a challenger that re-executes the shard committee's derivations and gets a different result submits this envelope as a
  * slashing-evidence tx (`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §10.2). The handler is v2 because the
  * social-cost-vs-bond-balance design needs production data we don't have yet.
  *
  * '''Why land the wire shape now even though there's no handler.''' Greenfield (see memory `feedback_greenfield_no_wire_compat`) means we
  * don't need to preserve wire-format compatibility. But the schema is what gets serialized into snapshot files and the slashing partition;
  * pinning the field set before v2 means the v2 handler can be added without disturbing already-written disk state. Cost is ~30 lines of
  * inert schema; benefit is one fewer schema rev when the handler lands.
  *
  * '''Why `Hex` for [[reexecutionWitness]], not `Array[Byte]`.''' The design doc (§3.2) sketches it as `Array[Byte]`, but the project
  * convention for "opaque variable-length bytes carried in a Circe-serialized case class" is `Hex` (newtype over `String`) — same choice
  * made by [[io.constellationnetwork.schema.slashing.MetagraphAttestation.kesSignature]], `CommitteeMemberSignature.vrfProof`, etc. Bytes
  * encoded as a Circe array of ints round-trip but bloat the JSON ~3× and complicate human inspection. `Hex` round-trips to the same
  * underlying bytes deterministically and keeps the wire shape consistent across the package.
  *
  * '''Witness schema TBD per derivation type.''' v2 handler will define re-execution witness shapes per shard-derivation kind (MPT root
  * mismatch witness is one shape; SC binary chain-link mismatch is another; cross-shard receipt drift is a third). v1 keeps the field as
  * opaque `Hex` so the disputed-derivation hash is the only structured payload. See §11.3 for the v2 expansion plan.
  *
  * @param shardId
  *   which shard's checkpoint is being disputed
  * @param disputedCheckpointHash
  *   `Hasher` of the disputed [[ShardCheckpointSigPreimage]] — names the specific checkpoint the dispute targets
  * @param claimedDerivation
  *   the `mptRoot` of [[ShardDerivedStateDelta]] as signed by the shard committee (the committee's claim)
  * @param challengerDerivation
  *   the `mptRoot` the challenger computed from independent re-execution (the alleged correct value)
  * @param reexecutionWitness
  *   opaque bytes — the witness proving the challenger's re-execution is correct. v1: reserved. v2: per-derivation-type schema (§11.3)
  * @param challengerSignature
  *   Ed25519 over the rest of the envelope. Routed through `Hasher[F]` over a derevo-derived preimage when v2 lands; in v1 the field is
  *   carried but no validator reads it
  */
@derive(encoder, decoder, eqv, show)
final case class FraudProofEnvelope(
  shardId: ShardId,
  disputedCheckpointHash: Hash,
  claimedDerivation: Hash,
  challengerDerivation: Hash,
  reexecutionWitness: Hex,
  challengerSignature: Hex
)
