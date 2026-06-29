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
  * '''The threat this closes.''' `ShardCheckpointGl0AcceptanceManager.verifyEmbedded` admits a checkpoint on `kQuorum` distinct committee
  * signatures WITHOUT re-execution (the common path). A corrupt committee that reaches quorum can therefore attest a WRONG
  * `perMetagraphMptRoots` and have every gl0 adopt it. The watchtower is the approval-check that runs the per-MG re-derivation even when
  * quorum was met — so a SINGLE honest re-executing node catches the wrong root, gossips this envelope, and every gl0 INDEPENDENTLY re-runs
  * the deterministic verdict (never trusting the challenger's claimed roots — they recompute). Upheld ⇒ revert + slash the committee.
  *
  * '''Determinism of the verdict (the load-bearing invariant).''' The dispute consumer recomputes the honest derivation from the disputed
  * checkpoint's OWN signed bytes ([[disputedCheckpointHash]] resolves the cached envelope whose `includedSnapshots(metagraphAddress)` are
  * the inputs) via the PURE, prior-independent `GlobalSnapshotStateChannelEventsProcessor.deriveMetagraphRoot` — `noGlobalSnapshotLookup`,
  * empty prior, so the result is a pure function of `(metagraphAddress, includedSnapshots(mg), gl0AnchorOrdinal)` and byte-identical on
  * every node regardless of its local MPT height. The challenger's [[challengerDerivation]] / [[claimedDerivation]] are carried as a HINT
  * for logging / fast-path triage only; the verdict NEVER trusts them — it recomputes (see `InvalidStateProofValidator`). This is why the
  * verdict keys on the PURE derivation rather than the node-local-`S(N)`-dependent PIN-1 `perMetagraphMptRoots` encoding: PIN-1 carries
  * carry-forward fields folded over the producer's `S(N)`, which a lagging-but-honest node does not share, so a PIN-1 verdict could
  * false-slash an honest committee on a node whose `S(N)` differs. The pure derivation has no such dependency.
  *
  * '''Why `Hex` for [[reexecutionWitness]].''' Project convention for "opaque variable-length bytes carried in a Circe-serialized case
  * class" (same choice as `MetagraphAttestation.kesSignature`, `CommitteeMemberSignature.vrfProof`). Here it carries the canonical
  * `deriveMetagraphRoot` PURE reference root the challenger computed — the value the verdict reproduces. It is a hint, NOT trusted.
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
  *   `deriveMetagraphRoot` to reproduce the producer's root. Read off the signed checkpoint; carried here so the verdict can sanity-check.
  * @param claimedDerivation
  *   the per-MG root the shard committee SIGNED (`perMetagraphMptRoots(metagraphAddress)`) — the committee's claim. UNTRUSTED on the wire
  *   (the verdict reads the committee's claim from the signed checkpoint, not from this field); carried for diagnostics.
  * @param challengerDerivation
  *   the canonical PURE reference root (`deriveMetagraphRoot`) the challenger computed from independent re-execution — the alleged correct
  *   value. UNTRUSTED: the verdict RECOMPUTES it. Carried so a node can fast-triage before the full re-derivation.
  * @param reexecutionWitness
  *   opaque bytes — currently the challenger's PURE reference root bytes (== `challengerDerivation`), reserved for a future
  *   per-derivation-kind witness schema (§11.3). Hint only; the verdict reproduces it.
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
