package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId}
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** Slice 16 — shard-checkpoint equivocation evidence.
  *
  * Companion piece to [[io.constellationnetwork.schema.slashing.SlashableEvidence]]: the same algebra (cryptographically verifiable proof
  * of a single signer producing two contradictory artefacts) lifted to the shard-checkpoint layer per
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §10.1.
  *
  * '''Equivocation identity.''' The equivocation key is `(shardId, parentCheckpointHash)` — exactly mirroring the metagraph case where the
  * key is `(metagraph_address, parent_hash)`. Two distinct `ShardCheckpoint` envelopes that share that key and carry a signature from the
  * same `peerId` in their respective `committeeSignatures` lists prove the offender signed both children. The KES forward-security property
  * (`SLASHING-DESIGN.md` §7) means an honest signer who deleted their old SK cannot produce the second signature — so two distinct KES
  * signatures on two distinct children at the same key is adversarial action by construction.
  *
  * '''Why this is a separate evidence type, not a variant of `SlashableEvidence`.''' The existing schema is tightly coupled to
  * [[io.constellationnetwork.schema.slashing.MetagraphAttestation]] bodies, which carry the per-binary attestation fields (`binaryHash`,
  * `vrfPublicKey`, `senderTreeStep`). Shard-checkpoint equivocation lives one layer up: the offence is producing two conflicting
  * [[ShardCheckpoint]] envelopes, not two conflicting metagraph attestations. The on-wire fields needed to prove it (full envelopes with
  * their `committeeSignatures` lists vs. raw attestation bodies) don't compose into a single ADT cleanly. Following the design-doc §10.1
  * sketch literally — separate evidence case class, same ledger-effect pipeline downstream.
  *
  * '''No bounty/submitter fields on this case class.''' Unlike [[io.constellationnetwork.schema.slashing.SlashableEvidence]] the evidence
  * here is the pure equivocation proof — the bounty / submitter / replay-bound-signature concerns live on the wrapping L0 tx type. Slice 16
  * delivers the evidence + validator; the GSAM accept-path agent will choose whether to expose the bounty surface via the existing
  * [[io.constellationnetwork.schema.slashing.SlashableEvidence]] envelope (extended with a `kind` discriminator) or a parallel L0 tx — both
  * routes preserve byte-equivalence of the underlying equivocation proof since the validation algebra below is the load-bearing piece.
  *
  * '''Validator coverage.''' See [[ShardCheckpointEquivocationValidator]] — six checks total, mirroring the slashing safety bar
  * (`feedback_slashing_safety_bar`):
  *
  *   1. Parent-hash match (both children point to the same parent + evidence header agrees) — without this the two children belong to
  *      different fork points and the signer was not equivocating, just voting on disjoint chain positions.
  *   1. Shard-id match (both children belong to the same shard + evidence header agrees) — without this the signatures aren't on
  *      contradictory artefacts but on parallel-universe shard outputs.
  *   1. Distinct child canonical hashes (`Hasher[F].hash(child.signingPreimage)`) — equal hashes ⇒ duplicate retransmission of the same
  *      checkpoint, not equivocation.
  *   1. Signer present in BOTH `committeeSignatures` lists — without this the evidence pair fails to attribute the offence to a single key.
  *   1. Both signer Ed25519 signatures verify under the signer's long-term VK (recovered from `equivocatingSigner.value.toPublicKey`) over
  *      the respective child's preimage-hash bytes — the load-bearing cryptographic proof.
  *   1. Both signer KES product signatures verify under the signer's KES master VK at the wire-carried `kesTreeStep` — the deliberate
  *      adversarial-action proof (KES forward-security means the offender had to keep two-period material live, which is provably wrong).
  *
  * '''Determinism contract.''' Same as [[io.constellationnetwork.node.shared.domain.nakamoto.slashing.SlashableEvidenceValidator]]: every
  * honest node computing this validator over the same `(evidence, kesRegistry)` inputs returns byte-equivalent accept/reject. No clock, no
  * env reads, no consensus-state heuristics. The `Hasher[F]` invocations (signing-preimage hashing for cryptographic verification) are
  * deterministic by construction — same canonical-JSON serialization surface as everywhere else in the codebase.
  *
  * '''Frozen wire shape.''' The fields here are part of the consensus contract — they participate in any wrapping L0 tx's canonical bytes.
  * Adding optional fields, reordering, or wrapping a field in `Option` silently changes the digest and breaks cross-version evidence
  * verification. Same discipline as [[io.constellationnetwork.schema.slashing.SlashableEvidence.BountyDigestPreimage]]; if the schema needs
  * to evolve, version the case class explicitly (`ShardCheckpointEquivocationEvidenceV2`) and version the validator branch.
  *
  * @param shardId
  *   the shard whose committee produced the conflicting checkpoints. Header equality with `childA.shardId` / `childB.shardId` is asserted
  *   by validator step 2 — keeping the field redundantly on the envelope means downstream MPT-key derivation can use it without re-deriving
  *   from either child.
  * @param parentCheckpointHash
  *   the parent checkpoint both children chain off. Header equality with `childA.parentCheckpointHash` / `childB.parentCheckpointHash` is
  *   asserted by validator step 1 — same MPT-key-derivation rationale as `shardId`.
  * @param childA
  *   first conflicting child checkpoint (full envelope; the validator extracts the offender's `CommitteeMemberSignature` from this child's
  *   `committeeSignatures` list).
  * @param childB
  *   second conflicting child checkpoint (full envelope; same as `childA`, distinct canonical hash).
  * @param equivocatingSigner
  *   the committee member who signed both children. Must appear in BOTH children's `committeeSignatures` lists (validator step 4).
  */
@derive(decoder, encoder, eqv, show)
final case class ShardCheckpointEquivocationEvidence(
  shardId: ShardId,
  parentCheckpointHash: Hash,
  childA: ShardCheckpoint,
  childB: ShardCheckpoint,
  equivocatingSigner: PeerId
)

/** Rejection reasons for [[ShardCheckpointEquivocationValidator]] — one variant per validator step.
  *
  * Sealed ADT (per `feedback_no_string_matching`): the validator returns the exact step that fired so detection metrics + slasher audit-log
  * can name the failure mode without re-deriving from a string. Mirrors [[io.constellationnetwork.schema.slashing.SlashingRejection]]'s
  * shape — one case per step in [[ShardCheckpointEquivocationValidator]]'s `validate` flow.
  */
@derive(eqv, show)
sealed trait ShardCheckpointEquivocationRejection extends Product with Serializable

object ShardCheckpointEquivocationRejection {

  /** §10.1 step 1 — `childA.parentCheckpointHash != childB.parentCheckpointHash` or one of them disagrees with
    * `evidence.parentCheckpointHash`. Different parents ⇒ two distinct chain positions; signing each is not equivocation.
    */
  @derive(eqv, show)
  final case class ParentMismatch(parentA: Hash, parentB: Hash, evidenceParent: Hash) extends ShardCheckpointEquivocationRejection

  /** §10.1 step 2 — `childA.shardId != childB.shardId` or one of them disagrees with `evidence.shardId`. Different shards ⇒ the signatures
    * aren't on contradictory artefacts.
    */
  @derive(eqv, show)
  final case class ShardMismatch(shardA: ShardId, shardB: ShardId, evidenceShard: ShardId) extends ShardCheckpointEquivocationRejection

  /** §10.1 step 3 — `Hasher[F].hash(childA.signingPreimage) == Hasher[F].hash(childB.signingPreimage)`. Equal canonical hashes ⇒ duplicate
    * retransmission of the same checkpoint, not equivocation.
    */
  @derive(eqv, show)
  final case class SameChildHash(hash: Hash) extends ShardCheckpointEquivocationRejection

  /** §10.1 step 4 — `equivocatingSigner` is not present in at least one of the two children's `committeeSignatures` lists. Without a
    * signature attributed to the accused key the evidence fails to prove the offence. Distinguishes `OnChildA` vs `OnChildB` for diagnostic
    * precision in the audit log.
    */
  @derive(eqv, show)
  sealed trait SignerNotPresent extends ShardCheckpointEquivocationRejection
  object SignerNotPresent {
    @derive(eqv, show)
    final case class OnChildA(signer: PeerId) extends SignerNotPresent
    @derive(eqv, show)
    final case class OnChildB(signer: PeerId) extends SignerNotPresent
  }

  /** §10.1 step 5 — the signer's Ed25519 signature on one of the children does not verify under the signer's recovered long-term VK. The
    * `OnChildA`/`OnChildB` distinction is preserved in the rejection so detector metrics can name which leg failed.
    */
  @derive(eqv, show)
  sealed trait InvalidEd25519Signature extends ShardCheckpointEquivocationRejection
  object InvalidEd25519Signature {
    case object OnChildA extends InvalidEd25519Signature
    case object OnChildB extends InvalidEd25519Signature
  }

  /** §10.1 step 6 — the signer's KES product signature on one of the children does not verify under the signer's KES master VK at the
    * wire-carried `kesTreeStep`. Same fail-closed semantics as [[SlashableEvidenceValidator]]: if the signer has no `KesRegistry` entry, we
    * can't establish the equivocation evidence cryptographically, so we reject. The `OnChildA`/`OnChildB` distinction mirrors
    * [[InvalidEd25519Signature]] for diagnostic parity.
    */
  @derive(eqv, show)
  sealed trait InvalidKesSignature extends ShardCheckpointEquivocationRejection
  object InvalidKesSignature {
    case object OnChildA extends InvalidKesSignature
    case object OnChildB extends InvalidKesSignature
  }
}
