package io.constellationnetwork.schema.sharding

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** WATCHTOWER fraud-proof envelope — a non-committee gl0 node's challenge to a quorum-signed shard checkpoint whose attested per-metagraph
  * derivation it could not reproduce by re-executing the SAME committee derivation
  * (`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §10.2 "wrong-derivation"; `docs/nakamoto/WATCHTOWER-FRAUD-PROOF-DESIGN.md`).
  *
  * '''The threat this closes.''' A committee may sign a root that GL0 replay does not reproduce. Signatures never authorize that state:
  * `ShardCheckpointGl0AcceptanceManager.verifyEmbedded` re-executes every included CL1 transition and rejects the mismatch before adoption.
  * This envelope makes the already-detected, affirmative mismatch self-contained so every GL0 node can independently verify the evidence
  * and slash the signers without trusting the challenger.
  *
  * '''Determinism of the verdict (the load-bearing invariant).''' The dispute consumer recomputes from the checkpoint's signed binaries at
  * its signed execution-base ordinal, using the same retained finalized GL0 prior as producer and adopter. The challenger's
  * [[challengerDerivation]] and [[claimedDerivation]] are diagnostic hints only; the verdict never trusts them. If the pinned base is not
  * locally available, verification fails closed without slashing.
  *
  * '''Why `Hex` for [[reexecutionWitness]].''' Project convention for "opaque variable-length bytes carried in a Circe-serialized case
  * class" (same choice as `MetagraphAttestation.kesSignature`, `CommitteeMemberSignature.vrfProof`). Here it carries the canonical
  * independently recreated per-metagraph MPT root. It is a hint, not trusted.
  *
  * '''Frozen wire shape (consensus-load-bearing once a wrapping evidence tx exists).''' Field set + order participate in
  * [[InvalidStateProofEvidence]]'s canonical bytes. Adding/reordering/wrapping a field silently changes the digest. Bump explicitly
  * (`FraudProofEnvelopeV2`) — never mutate the field set. Same discipline as `SlashableEvidence.BountyDigestPreimage`.
  *
  * @param shardId
  *   which shard's checkpoint is being disputed
  * @param disputedCheckpointHash
  *   `Hasher` of the disputed [[ShardCheckpointSigPreimage]] — names the specific checkpoint the dispute targets. The verdict resolves the
  *   full signed envelope by this hash (from the local shard chain store or the carried evidence) and re-derives from ITS bytes.
  * @param metagraphAddress
  *   the metagraph inside the checkpoint whose per-MG derivation is disputed. The verdict re-derives ONLY this MG's root from
  *   `includedSnapshots(metagraphAddress)` — a checkpoint can carry many MGs but a fraud proof targets one wrong derivation.
  * @param gl0AnchorOrdinal
  *   the disputed checkpoint's wire-carried `gl0AnchorOrdinal` — the derivation context (fee-cutover ordinal) the verdict MUST pass to
  *   pinned-base replay to reproduce the producer's root. Read off the signed checkpoint; carried here so the verdict can sanity-check.
  * @param claimedDerivation
  *   the per-MG root the shard committee SIGNED (`perMetagraphMptRoots(metagraphAddress)`) — the committee's claim. UNTRUSTED on the wire
  *   (the verdict reads the committee's claim from the signed checkpoint, not from this field); carried for diagnostics.
  * @param challengerDerivation
  *   the canonical per-metagraph MPT root the challenger computed from independent pinned-base re-execution — the alleged correct value.
  *   UNTRUSTED: the verdict recomputes it. Carried so a node can fast-triage before the full re-derivation.
  * @param reexecutionWitness
  *   opaque bytes — currently the challenger's reference-root bytes (== `challengerDerivation`), reserved for a future per-derivation-kind
  *   witness schema (§11.3). Hint only; the verdict reproduces it.
  * @param challengerSignature
  *   Ed25519 (long-term key, recovered from the submitter `PeerId`) over the `Hasher[F]` of the rest of the envelope. Binds the dispute to
  *   its submitter so a replay attacker cannot lift any bounty; the verdict verifies it before recomputing.
  */
@derive(encoder, decoder, eqv, show)
final case class FraudProofEnvelope(
  shardId: ShardId,
  disputedCheckpointHash: Hash,
  metagraphAddress: Address,
  gl0AnchorOrdinal: SnapshotOrdinal,
  claimedDerivation: Hash,
  challengerDerivation: Hash,
  reexecutionWitness: Hex,
  challengerSignature: Hex,
  submitterId: PeerId
) {

  /** Pure projection: the envelope sans [[challengerSignature]] — the bytes the submitter signs (and a verifier re-derives + checks). Same
    * sign-the-preimage discipline as [[ShardCheckpoint.signingPreimage]] / `SlashableEvidence.BountyDigestPreimage`.
    */
  def signingPreimage: FraudProofSigPreimage =
    FraudProofSigPreimage(
      shardId = shardId,
      disputedCheckpointHash = disputedCheckpointHash,
      metagraphAddress = metagraphAddress,
      gl0AnchorOrdinal = gl0AnchorOrdinal,
      claimedDerivation = claimedDerivation,
      challengerDerivation = challengerDerivation,
      reexecutionWitness = reexecutionWitness,
      submitterId = submitterId
    )
}

/** Canonical pre-image for the bytes the challenger signs over a [[FraudProofEnvelope]] (envelope minus the signature itself).
  *
  * '''Frozen wire shape.''' Field order/set are part of the dispute contract — they feed `Hasher[F]` and any wrapping evidence tx's bytes.
  * Reordering / wrapping silently changes the digest. Bump explicitly (`FraudProofSigPreimageV2`) if it must evolve.
  */
@derive(encoder, decoder, eqv, show)
final case class FraudProofSigPreimage(
  shardId: ShardId,
  disputedCheckpointHash: Hash,
  metagraphAddress: Address,
  gl0AnchorOrdinal: SnapshotOrdinal,
  claimedDerivation: Hash,
  challengerDerivation: Hash,
  reexecutionWitness: Hex,
  submitterId: PeerId
)
