package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId, ShardOrdinal}
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
  * '''Candidate identity.''' Two children must share shard, parent, shard ordinal, and execution period before comparison. This proves the
  * same operator authenticated both artifacts, but it does not yet prove misconduct: execution signatures attest reproduced state validity,
  * and the protocol has not specified a Nakamoto/staircase-safe exclusivity rule forbidding an honest signer from attesting two
  * individually valid competing proposals. The validator therefore fails closed as `Unverifiable` after authenticating both legs. A BFT
  * lock must not be invented to fill this design gap.
  *
  * '''Why this is a separate evidence type, not a variant of `SlashableEvidence`.''' The existing schema is tightly coupled to
  * [[io.constellationnetwork.schema.slashing.MetagraphAttestation]] bodies, which carry the per-binary attestation fields (`binaryHash`,
  * `vrfPublicKey`, `senderTreeStep`). Shard-checkpoint equivocation lives one layer up: the offence is producing two conflicting
  * [[ShardCheckpoint]] envelopes, not two conflicting metagraph attestations. The on-wire fields needed to prove it (full envelopes with
  * their `committeeSignatures` lists vs. raw attestation bodies) don't compose into a single ADT cleanly. Following the design-doc §10.1
  * sketch literally — separate evidence case class, same ledger-effect pipeline downstream.
  *
  * '''No bounty/submitter fields on this case class.''' Unlike [[io.constellationnetwork.schema.slashing.SlashableEvidence]] the evidence
  * here authenticates the two signatures only. No bounty or ledger slash may be wired until signer exclusivity is specified and this
  * validator can prove that rule without BFT locking.
  *
  * '''Validator coverage.''' See [[ShardCheckpointEquivocationValidator]]:
  *
  *   1. Parent-hash match (both children point to the same parent + evidence header agrees) — without this the two children belong to
  *      different fork points and the signer was not equivocating, just voting on disjoint chain positions.
  *   1. Shard-id match (both children belong to the same shard + evidence header agrees) — without this the signatures aren't on
  *      contradictory artefacts but on parallel-universe shard outputs.
  *   1. Shard-ordinal and execution-period match — different heights or committee draws are not equivocation.
  *   1. Distinct child canonical hashes (`Hasher[F].hash(child.signingPreimage)`) — equal hashes ⇒ duplicate retransmission of the same
  *      checkpoint, not equivocation.
  *   1. Signer present in BOTH `committeeSignatures` lists — without this the evidence pair fails to attribute the offence to a single key.
  *   1. Both signer Ed25519 signatures verify under the signer's long-term VK (recovered from `equivocatingSigner.value.toPublicKey`) over
  *      the respective child's preimage-hash bytes — the load-bearing cryptographic proof.
  *   1. Both signer KES product signatures verify under the exact historical atomic pair at `checkpoint.epoch - registeredOffset`; the wire
  *      step must equal that derivation and is never authority.
  *   1. Both registered-key possession proofs verify under the same pair over the historical shard eta and signed slot.
  *
  * '''Determinism contract.''' Same as [[io.constellationnetwork.node.shared.domain.nakamoto.slashing.SlashableEvidenceValidator]]: every
  * honest node computing this validator over the same evidence and exact historical resolver view returns the same
  * `Valid`/`Invalid`/`Unverifiable` result. No receiver-current registry may fill missing history.
  *
  * '''Wire shape.''' The fields participate in any wrapping L0 transaction's canonical bytes. This fork is greenfield, so an incomplete
  * fork-only shape must be replaced atomically rather than retained behind V1/V2 compatibility branches.
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

  /** Children at different shard ordinals are not competing children at one chain height. */
  @derive(eqv, show)
  final case class ShardOrdinalMismatch(ordinalA: ShardOrdinal, ordinalB: ShardOrdinal) extends ShardCheckpointEquivocationRejection

  /** Execution committee identity changes with eta period. Two signatures from different periods do not prove same-draw equivocation. */
  @derive(eqv, show)
  final case class ExecutionPeriodMismatch(periodA: EtaPeriod, periodB: EtaPeriod) extends ShardCheckpointEquivocationRejection

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

  /** §10.1 step 6 — the signer's KES product signature does not verify at the historically derived tree step. Missing historical state is
    * not this rejection; it is a typed `Unverifiable` result and cannot slash.
    */
  @derive(eqv, show)
  sealed trait InvalidKesSignature extends ShardCheckpointEquivocationRejection
  object InvalidKesSignature {
    case object OnChildA extends InvalidKesSignature
    case object OnChildB extends InvalidKesSignature
  }

  /** §10.1 step 7 — registered-VRF-key possession proof failed under the exact atomic pair and historical shard eta. */
  @derive(eqv, show)
  sealed trait InvalidVrfProof extends ShardCheckpointEquivocationRejection
  object InvalidVrfProof {
    case object OnChildA extends InvalidVrfProof
    case object OnChildB extends InvalidVrfProof
  }
}
