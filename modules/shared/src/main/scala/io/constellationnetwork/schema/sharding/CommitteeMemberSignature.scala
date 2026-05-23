package io.constellationnetwork.schema.sharding

import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex

import derevo.cats.{eqv, order, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** A single committee member's contribution to a shard checkpoint.
  *
  * Membership in the committee is proven by the VRF proof (same primitive as `CommitteeSortition.verifyMembership`, see
  * `docs/nakamoto/COMMITTEE-SORTITION-DESIGN.md` §5). The Ed25519 long-term signature and the KES product signature both cover the
  * canonical checkpoint hash (`Hasher[F]` of [[ShardCheckpointSigPreimage]] — the envelope sans `committeeSignatures`). gl0 acceptance
  * applies all three predicates per signer.
  *
  * '''Field order rationale.''' Mirrors `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §3.1 exactly so the on-wire JSON shape
  * matches the documented spec one-to-one. Encoding via derevo magnolia uses field positional order; reordering silently changes the
  * canonical hash bytes, which would break cross-version signature verification (see [[ShardCheckpointSigPreimage]] scaladoc on
  * wire-byte stability).
  *
  * '''Why `Hex` for raw bytes.''' Same rationale as `MetagraphAttestation` (see [[io.constellationnetwork.schema.slashing.MetagraphAttestation]]
  * scaladoc): `Hex` is a newtype over `String` so the case class gets free Circe `decoder`/`encoder` instances and round-trips
  * deterministically. The Hex form is exactly what the wire-side `ByteString.copyFrom` ↔ JVM bytes path produces.
  *
  * '''Why no outer `Signed[_]` envelope around the committee member's contribution.''' All three signatures (Ed25519, KES product, VRF
  * proof) live in the body directly. KES product + VRF proof are independently load-bearing (KES proves the operator's per-period key
  * controls the long-term identity; VRF proof proves committee membership at the sortition slot); the Ed25519 sig is the gossip-layer
  * convention so receivers can authenticate the message origin without parsing the KES tree. Wrapping in `Signed[_]` would inflate the
  * wire bytes with a redundant fourth proof and the canonical-hash pre-image. Same design decision as
  * [[io.constellationnetwork.schema.slashing.SlashableEvidence]].
  *
  * @param peerId
  *   committee member who signed (operator identity)
  * @param vrfProof
  *   committee-membership proof under `CommitteeSortition` for this `(epoch, shardId, slot)` draw
  * @param ed25519Sig
  *   operator's long-term Ed25519 signature over the canonical checkpoint hash
  * @param kesProductSig
  *   KES product signature over the canonical checkpoint hash (proves per-period operational key control of the long-term identity)
  * @param kesTreeStep
  *   sender-tree-internal step the KES sig was emitted at — enables non-interactive receiver verify (see #211 wire-step landing)
  */
@derive(encoder, decoder, order, eqv, show)
final case class CommitteeMemberSignature(
  peerId: PeerId,
  vrfProof: Hex,
  ed25519Sig: Hex,
  kesProductSig: Hex,
  kesTreeStep: Int
)
