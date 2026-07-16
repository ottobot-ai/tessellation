package io.constellationnetwork.schema.sharding

import cats.data.NonEmptyList

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, GlobalSnapshotStateRef}
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** Shard checkpoint envelope.
  *
  * Produced by an execution-shard committee using deterministic staircase duty (`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md`).
  * The current envelope is transitional: for CL1 it carries exact ordered signed snapshot inputs and reproducible root claims, but it does
  * not yet carry the target canonical namespace-confined byte diff. Its execution base is a signed claim naming one exact snapshot state;
  * the reference does not by itself prove Phase-2 qualification or freshness.
  *
  * Checkpoints are content-bearing. A shard with no replayable state-channel binary emits no checkpoint; receivers reject empty windows so
  * a signed no-op cannot advance fork choice or finality.
  *
  * [[gl0AnchorOrdinal]] is a legacy-named inclusion-height hint only. It must never authorize execution, weaken the pinned base, or rebase
  * a checkpoint onto the receiver's live head. The schema binds replay to a claimed exact `(ordinal, hash, parentHash, mptRoot)` reference;
  * a consumer must separately authenticate canonical Phase-2 status. Once authenticated, the signed [[executionBase]] is the only replay
  * prior; ordinal equality alone is never sufficient.
  *
  * '''Hash exclusion for signatures.''' The [[committeeSignatures]] field is excluded when computing the bytes a signer signs; see
  * [[ShardCheckpointSigPreimage]] for the canonical pre-image case class. Every signer hashes that pre-image via `Hasher[F]`, signs the
  * bytes with Ed25519, signs them again with KES product, and emits both plus the registered-VRF-key possession proof. A state-validity
  * signature is emitted only after that signer independently replays the complete checkpoint input and matches its claimed result.
  *
  * @param shardId
  *   which shard produced this checkpoint
  * @param parentCheckpointHash
  *   chain-link in the shard's mini-chain (`Hash` of the parent checkpoint envelope, i.e. `Hasher` of this envelope without
  *   `committeeSignatures` — same canonical preimage as [[ShardCheckpointSigPreimage]])
  * @param shardOrdinal
  *   monotonic per-shard sequence number (`parent.shardOrdinal.next`)
  * @param gl0AnchorOrdinal
  *   legacy-named inclusion scheduling hint. It is signed but is not an execution base, validity floor, or authority to adopt against a
  *   later GL0 state. [[slot]] independently drives deterministic staircase duty
  * @param slot
  *   the shared genesis-anchored slot in which the scheduled staircase producer emitted the checkpoint. Signed (part of
  *   [[ShardCheckpointSigPreimage]]) so a relayer cannot alter it. Receive and embedded validation enforce strict monotonicity against the
  *   exact retained parent and derive producer duty from the resulting signed gap. There is currently no consensus-pinned upper skew bound:
  *   a far-future slot can select a later rank early and, if certified, prevent honest descendants until the shared clock catches up. A
  *   future bound must use a deterministic protocol clock/anchor, never receiver wall clock. maxvalid-tk's lower-slot preference does not
  *   remove that liveness attack.
  * @param derivedStateDelta
  *   transitional payload of included snapshots and locally-checkable root claims. Current ordinary GL0 adoption universally replays these
  *   inputs; the target replaces that transitional cost with replay-before-sign, positive watchtower replay coverage, certificate checking,
  *   namespace-confined diff application, and root recomputation by noncommittee GL0 nodes
  * @param committeeSignatures
  *   transitional `NonEmptyList` whose head is interpreted as the producer signature and whose tail contains execution signatures. Honest
  *   aggregation appends and canonically orders only the tail. Because this entire field is excluded from [[ShardCheckpointSigPreimage]],
  *   reordering can change the nominal producer label, although duty validation rejects every head except the unique scheduled signer.
  *   Explicit producer identity/role binding remains desirable for accountability. Every execution signature requires independent replay;
  *   quorum alone never substitutes for execution, and positive watchtower replay coverage is required before target GL0 inclusion
  * @param epoch
  *   execution-membership epoch, used to reproduce the public VK-hash committee set and shard-eta proof domain
  * @param executionBase
  *   claimed exact GL0 state replayed by the producer, every execution signer, and every watchtower. Replay consumers require all four
  *   fields to match the resolved snapshot and retained bytes to reproduce `mptRoot`; a same-ordinal sibling cannot silently replace it.
  *   This check proves state identity, not Phase-2 qualification/freshness. Consensus-load-bearing because it is part of the signed
  *   preimage
  */
@derive(encoder, decoder, eqv, show)
final case class ShardCheckpoint(
  shardId: ShardId,
  parentCheckpointHash: Hash,
  shardOrdinal: ShardOrdinal,
  gl0AnchorOrdinal: SnapshotOrdinal,
  slot: Slot,
  derivedStateDelta: ShardDerivedStateDelta,
  committeeSignatures: NonEmptyList[CommitteeMemberSignature],
  epoch: EtaPeriod,
  executionBase: GlobalSnapshotStateRef
) {

  /** Nominal checkpoint producer under the transitional head convention. Receive-side staircase validation checks this signer before
    * replay. Signature-list order itself is not signed, so explicit producer-role binding remains an accountability hardening item.
    */
  def producerSignature: CommitteeMemberSignature = committeeSignatures.head

  /** Pure projection: strip the signatures field and yield the canonical pre-image used for [[committeeSignatures]] and chain-linking.
    *
    * Verifier path: receiver re-derives this preimage from the envelope it received, hashes via `Hasher[F]`, then runs each signer's
    * Ed25519 / KES / VRF predicates over the resulting hash. Producer path: each signer constructs this preimage from the proposed
    * envelope, hashes, signs, and contributes a [[CommitteeMemberSignature]].
    *
    * '''Why a method, not a separate constructor.''' The preimage is always derived from a full envelope at both producer and verifier
    * sites — there's no path where one would build the preimage independently. Co-locating the projection on the envelope keeps the "what
    * bytes get signed" contract one method-call away from the envelope itself.
    */
  def signingPreimage: ShardCheckpointSigPreimage =
    ShardCheckpointSigPreimage(
      shardId = shardId,
      parentCheckpointHash = parentCheckpointHash,
      shardOrdinal = shardOrdinal,
      gl0AnchorOrdinal = gl0AnchorOrdinal,
      slot = slot,
      derivedStateDelta = derivedStateDelta,
      epoch = epoch,
      executionBase = executionBase
    )
}

/** Canonical pre-image for the bytes a committee member signs (and for the chain-link hash a parent checkpoint exposes to its child).
  *
  * Fields are exactly [[ShardCheckpoint]]'s fields minus [[ShardCheckpoint.committeeSignatures]] — the field that the signatures cover.
  *
  * '''Wire-byte stability (consensus-load-bearing).''' Field order and field set here are part of the consensus contract. Adding a field,
  * reordering, or wrapping a field in `Option` silently changes the derevo magnolia-derived Circe JSON, which silently changes the
  * `Hasher[F]` output, which silently changes the bytes every signer expected to sign and every verifier expects to verify. Bump the case
  * class explicitly and version the verifier branch once this greenfield schema has been deployed. Same frozen-shape discipline as
  * [[io.constellationnetwork.schema.slashing.SlashableEvidence.BountyDigestPreimage]].
  */
@derive(encoder, decoder, eqv, show)
final case class ShardCheckpointSigPreimage(
  shardId: ShardId,
  parentCheckpointHash: Hash,
  shardOrdinal: ShardOrdinal,
  gl0AnchorOrdinal: SnapshotOrdinal,
  slot: Slot,
  derivedStateDelta: ShardDerivedStateDelta,
  epoch: EtaPeriod,
  executionBase: GlobalSnapshotStateRef
)
