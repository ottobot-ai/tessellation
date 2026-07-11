package io.constellationnetwork.schema.sharding

import cats.data.NonEmptyList

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** Shard checkpoint envelope.
  *
  * Produced by an execution-shard committee using deterministic staircase duty (`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md`).
  * The envelope is content-agnostic with respect to the metagraph type carried inside [[derivedStateDelta]]. For CL1, the envelope
  * transports signed snapshot bytes for GL0 execution and locally reproducible root claims.
  *
  * Checkpoints are content-bearing. A shard with no replayable state-channel binary emits no checkpoint; receivers reject empty windows so
  * a signed no-op cannot advance fork choice or finality.
  *
  * '''Loose coupling to gl0 ord via [[gl0AnchorOrdinal]].''' The checkpoint nominates the gl0 ordinal it expects to ride into; gl0 accepts
  * at this ord or any later ord (within the §7.2 lookahead bound). This decouples shard chain pace from gl0 pace so a busy shard doesn't
  * stall waiting for a slow gl0 round, and a slow shard doesn't force gl0 to wait.
  *
  * '''Hash exclusion for signatures.''' The [[committeeSignatures]] field is excluded when computing the bytes a signer signs; see
  * [[ShardCheckpointSigPreimage]] for the canonical pre-image case class. Every signer hashes that pre-image via `Hasher[F]`, signs the
  * bytes with Ed25519, signs them again with KES product, and emits both plus the registered-VRF-key possession proof.
  *
  * @param shardId
  *   which shard produced this checkpoint
  * @param parentCheckpointHash
  *   chain-link in the shard's mini-chain (`Hash` of the parent checkpoint envelope, i.e. `Hasher` of this envelope without
  *   `committeeSignatures` — same canonical preimage as [[ShardCheckpointSigPreimage]])
  * @param shardOrdinal
  *   monotonic per-shard sequence number (`parent.shardOrdinal.next`)
  * @param gl0AnchorOrdinal
  *   loose coupling — the gl0 ord this checkpoint expects to ride into; gl0 accepts at this or any later ord (see §7.2). Chain-link DATA
  *   only; [[slot]] drives deterministic staircase duty.
  * @param slot
  *   the shared genesis-anchored slot in which the scheduled staircase producer emitted the checkpoint. Signed (part of
  *   [[ShardCheckpointSigPreimage]]) so a relayer cannot alter it. Validity bounds at receive: strictly monotone vs the parent checkpoint's
  *   slot; the full skew bound (`<= now + eps`) is enforced during cryptographic checkpoint validation. maxvalid-tk prefers the LOWER slot
  *   on ties, so inflating the slot is self-defeating and deflating it is blocked by monotonicity
  * @param derivedStateDelta
  *   included snapshots and locally-checkable root claims. GL0 re-executes included CL1 snapshots before adoption.
  * @param committeeSignatures
  *   `NonEmptyList` because every checkpoint must have at least its producer signature. Finality uses the configured `kQuorum` count or the
  *   depth fallback; economic validity always requires GL0 replay regardless of signature count.
  * @param epoch
  *   execution-membership epoch, used to reproduce the public VK-hash committee set and shard-eta proof domain
  * @param executionBaseOrdinal
  *   pinned execution base. The producer and every verifier resolve the same finalized prior at this ordinal before re-executing
  *   [[ShardDerivedStateDelta.includedSnapshots]]. Consensus-load-bearing because it is part of the signed preimage.
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
  executionBaseOrdinal: SnapshotOrdinal = SnapshotOrdinal.MinValue
) {

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
      executionBaseOrdinal = executionBaseOrdinal
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
  executionBaseOrdinal: SnapshotOrdinal
)
