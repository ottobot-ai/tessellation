package io.constellationnetwork.schema.slashing

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{FraudProofEnvelope, ShardCheckpoint}
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** WATCHTOWER invalid-state-proof slashing evidence — the `InvalidStateProof` tier of `docs/nakamoto/SLASHING-DESIGN.md` (the 100% tier per
  * §10.4 severity ordering, above equivocation and far above non-participation).
  *
  * '''Algebra.''' Companion to [[SlashableEvidence]] (metagraph equivocation) and `ShardCheckpointEquivocationEvidence` (checkpoint
  * equivocation): the SAME "cryptographically verifiable proof of a deviation, byte-identical verdict on every honest node" discipline,
  * here for a committee that signed a checkpoint whose per-MG derivation is WRONG. Unlike equivocation (two conflicting artefacts), the
  * offence is a single checkpoint with a derivation that an honest re-execution does not reproduce — so the evidence carries the FULL
  * disputed [[ShardCheckpoint]] envelope and the verdict RE-DERIVES from its own signed bytes (`InvalidStateProofValidator`), never
  * trusting the challenger.
  *
  * '''Why the full envelope, not just a hash.''' The deterministic verdict must re-run pinned-base currency recreation over the
  * checkpoint's `includedSnapshots(metagraphAddress)`; those bytes live inside the envelope. Carrying the full `ShardCheckpoint` makes the
  * evidence SELF-CONTAINED — any gl0 node validates it without needing the checkpoint in its local store (it may have pruned it). The
  * committee signatures inside the envelope are the cryptographic binding that "this committee signed THIS derivation".
  *
  * '''Slash identity (double-slash key).''' `(shardId, disputedCheckpointHash)` — every committee member who signed THIS checkpoint is a
  * slash target (all of them attested the same wrong derivation; §10.2 "every committee signer deviated"). The MPT double-slash guard keys
  * on this pair so the same wrong checkpoint is slashed at most once regardless of how many watchtowers submit.
  *
  * '''No bounty/submitter signature on this case class.''' Mirrors `ShardCheckpointEquivocationEvidence`: the pure proof lives here; the
  * bounty / submitter / replay-bound-signature ride on the embedded [[fraudProof]] (a [[FraudProofEnvelope]] whose `challengerSignature`
  * binds the submitter). The GSAM accept-path credits the bounty to `fraudProof.submitterId`.
  *
  * '''Determinism contract.''' Same as [[SlashableEvidenceValidator]]: every honest node computing `InvalidStateProofValidator` over the
  * same evidence and retained signed execution base returns byte-equivalent accept/reject. No clock, env read, peer response, or mutable
  * best-tip input participates; an unavailable base rejects without slashing.
  *
  * '''Frozen wire shape.''' Fields are consensus-load-bearing (they are the bytes a gl0 snapshot serializes when it slashes). Adding /
  * reordering / wrapping a field silently changes the encoding; bump explicitly (`InvalidStateProofEvidenceV2`) and version the validator
  * branch. Same discipline as [[SlashableEvidence.BountyDigestPreimage]].
  *
  * @param shardId
  *   the shard whose committee produced the disputed checkpoint. Header equality with `disputedCheckpoint.shardId` and `fraudProof.shardId`
  *   is asserted by the validator.
  * @param disputedCheckpoint
  *   the full signed checkpoint envelope under dispute — the verdict re-derives the per-MG root from its `includedSnapshots`. Its canonical
  *   `Hasher[F](signingPreimage)` MUST equal `fraudProof.disputedCheckpointHash` (validator step).
  * @param metagraphAddress
  *   the metagraph inside the checkpoint whose derivation is wrong. MUST equal `fraudProof.metagraphAddress`.
  * @param attestedRoot
  *   the committee-attested per-MG root (`disputedCheckpoint.derivedStateDelta.perMetagraphMptRoots(metagraphAddress)`) carried redundantly
  *   for the MPT slash record; the validator reads the signed value off the envelope, not this field.
  * @param fraudProof
  *   the watchtower's [[FraudProofEnvelope]] — carries the submitter identity (`submitterId`) + the replay-binding `challengerSignature`
  *   the validator verifies, plus the challenger's (untrusted) reference roots for triage.
  */
@derive(decoder, encoder, eqv, show)
final case class InvalidStateProofEvidence(
  shardId: io.constellationnetwork.schema.sharding.ShardId,
  disputedCheckpoint: ShardCheckpoint,
  metagraphAddress: Address,
  attestedRoot: Hash,
  fraudProof: FraudProofEnvelope
) {

  /** The committee members who signed the disputed checkpoint — the full slash target set (§10.2: every signer attested the wrong
    * derivation). Deduplicated by `PeerId` (a doubled signer must not be slashed twice within one checkpoint).
    */
  def slashTargets: List[PeerId] =
    disputedCheckpoint.committeeSignatures.toList.map(_.peerId).distinct
}

object InvalidStateProofEvidence {

  /** Canonical `Order` keyed by `(shardId, fraudProof.disputedCheckpointHash)` — the SAME `(shardId, disputedCheckpointHash)` double-slash
    * identity the slash uses. A MANUAL instance (not `@derive(order)`) because the embedded [[ShardCheckpoint]] is not `Order` (and need
    * not be — the dispute identity is the checkpoint HASH, carried on the fraud proof). This makes `SortedSet[InvalidStateProofEvidence]`
    * canonical so the snapshot `fraudProofs` consensus field serializes byte-deterministically on every node, and two disputes over the
    * SAME wrong checkpoint coalesce in the set (the on-chain double-slash guard handles cross-ordinal dedup).
    */
  implicit val order: cats.Order[InvalidStateProofEvidence] =
    cats.Order.by(e => (e.shardId, e.fraudProof.disputedCheckpointHash))

  implicit val ordering: scala.math.Ordering[InvalidStateProofEvidence] = order.toOrdering
}

/** Rejection reasons for `InvalidStateProofValidator` — one variant per validator step. Sealed ADT (per `feedback_no_string_matching`): the
  * validator returns the exact step that fired so the slasher audit-log / metrics name the failure without parsing a string. Mirrors
  * [[SlashingRejection]] / `ShardCheckpointEquivocationRejection`.
  */
@derive(eqv, show)
sealed trait InvalidStateProofRejection extends Product with Serializable

object InvalidStateProofRejection {

  /** Header mismatch — `evidence.shardId`, `disputedCheckpoint.shardId`, and `fraudProof.shardId` do not all agree. */
  @derive(eqv, show)
  final case class ShardMismatch(
    evidenceShard: io.constellationnetwork.schema.sharding.ShardId,
    checkpointShard: io.constellationnetwork.schema.sharding.ShardId,
    fraudProofShard: io.constellationnetwork.schema.sharding.ShardId
  ) extends InvalidStateProofRejection

  /** Metagraph mismatch — `evidence.metagraphAddress` and `fraudProof.metagraphAddress` disagree. The disputed MG must be unambiguous. */
  @derive(eqv, show)
  final case class MetagraphMismatch(evidenceMg: Address, fraudProofMg: Address) extends InvalidStateProofRejection

  /** The carried `disputedCheckpoint`'s canonical hash does not equal `fraudProof.disputedCheckpointHash`. The fraud proof must target the
    * SAME checkpoint the evidence carries (binds the two), otherwise a challenger could pair an honest checkpoint with a fraud proof for a
    * different one.
    */
  @derive(eqv, show)
  final case class CheckpointHashMismatch(computed: Hash, claimed: Hash) extends InvalidStateProofRejection

  /** The disputed MG is not present in the checkpoint's `includedSnapshots` — there is no derivation to re-execute, so no wrong-derivation
    * to prove.
    */
  @derive(eqv, show)
  final case class MetagraphNotInCheckpoint(metagraphAddress: Address) extends InvalidStateProofRejection

  /** The challenger's [[FraudProofEnvelope.challengerSignature]] does not verify under the submitter's long-term Ed25519 key over the
    * canonical `FraudProofSigPreimage`. Prevents replay-style bounty hijack and unsigned spam.
    */
  case object InvalidChallengerSignature extends InvalidStateProofRejection

  /** The double-slash guard fired — a slash record already exists for `(shardId, disputedCheckpointHash)`. Only the first
    * invalid-state-proof for a given wrong checkpoint slashes; subsequent submissions are rejected here.
    */
  @derive(eqv, show)
  final case class AlreadySlashed(shardId: io.constellationnetwork.schema.sharding.ShardId, disputedCheckpointHash: Hash)
      extends InvalidStateProofRejection

  /** '''The load-bearing rejection''': the dispute is NOT upheld — the honest re-derivation REPRODUCED the committee-attested root, so the
    * committee did NOT deviate. A frivolous / forged fraud proof lands here. Carries both roots for the audit log. This is the safety
    * floor: an honest committee can never be slashed because the verdict recomputes the honest root and only upholds on a genuine
    * divergence.
    */
  @derive(eqv, show)
  final case class DisputeNotUpheld(honestReDerivedRoot: Hash, attestedRoot: Hash) extends InvalidStateProofRejection

  /** FAIL-CLOSED (Track-1 execution-base-pin, FINDING-B1): the honest re-derivation is UNAVAILABLE on this node — the injected
    * `reDerivePerMgRoot` returned the `Hash.empty` "cannot re-derive" sentinel (the disputed checkpoint's pinned `executionBaseOrdinal` is
    * unresolvable below this node's byte-store retention / not yet reached, or the derivation OMITted/deferred). "This node can't check" is
    * NOT evidence the committee deviated, so an unverifiable dispute is NEVER upheld — upholding demands an affirmative pinned-base
    * re-derivation that mismatches the attested root. Mirrors the node-local watchtower trigger
    * (`ShardCheckpointGl0AcceptanceManager.watchtowerReExec`), which filters `Hash.empty` as "can't check" rather than raising a dispute.
    */
  @derive(eqv, show)
  final case class CannotRederive(metagraphAddress: Address) extends InvalidStateProofRejection
}
