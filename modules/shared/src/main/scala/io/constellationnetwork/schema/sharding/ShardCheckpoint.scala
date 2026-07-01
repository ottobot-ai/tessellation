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
  * Produced by the shard's micro-Taktikos chain (`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §5–§6). The envelope is
  * content-agnostic with respect to the metagraph type carried inside [[derivedStateDelta]]. Custom data-application metagraphs (per the
  * `sharding-direction-clarified` direction note) ride the same envelope; only the per-MG content varies.
  *
  * Granularity = Option C per [`SHARD-CHECKPOINT-GRANULARITY.md`](../../../../../../../../docs/nakamoto/SHARD-CHECKPOINT-GRANULARITY.md)
  * §2.4: one checkpoint per gl0 ord per shard by default, with `T_burst` and `T_alive` escape hatches. Empty checkpoints (no SC binaries
  * this window) are allowed iff the `T_alive` liveness ping fires; otherwise the shard simply has no entry this ord.
  *
  * '''Loose coupling to gl0 ord via [[gl0AnchorOrdinal]].''' The checkpoint nominates the gl0 ordinal it expects to ride into; gl0 accepts
  * at this ord or any later ord (within the §7.2 lookahead bound). This decouples shard chain pace from gl0 pace so a busy shard doesn't
  * stall waiting for a slow gl0 round, and a slow shard doesn't force gl0 to wait.
  *
  * '''Hash exclusion for signatures.''' The [[committeeSignatures]] field is excluded when computing the bytes a signer signs; see
  * [[ShardCheckpointSigPreimage]] for the canonical pre-image case class. Every signer hashes that pre-image via `Hasher[F]`, signs the
  * bytes with Ed25519, signs them again with KES product, and emits both plus the VRF membership proof.
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
  *   only — NOT the lottery clock (that is [[slot]]; design §5.7, owner-corrected 2026-06-11)
  * @param slot
  *   the wall-clock slot (shared genesis-anchored grid, `slotDuration`-parametrized) at which the producer won the shard-leader lottery.
  *   Signed (part of [[ShardCheckpointSigPreimage]]) so a relayer cannot alter it. Validity bounds at receive: strictly monotone vs the
  *   parent checkpoint's slot; the full skew bound (`<= now + eps`) lands with the Slice-13 cryptographic `verifyLeader`. maxvalid-tk
  *   prefers the LOWER slot on ties, so inflating the slot is self-defeating and deflating it is blocked by monotonicity
  * @param derivedStateDelta
  *   per-MG state contribution this shard produced for this checkpoint window
  * @param emittedReceipts
  *   cross-shard receipts emitted by this checkpoint (typically `MetagraphSyncDataWrite` for cross-shard SpendActions; see §8)
  * @param committeeSignatures
  *   `NonEmptyList` because acceptance requires `>= ceil(2/3 K_S)` signatures (§5.3); empty would be vacuously safe but is a
  *   protocol-violation signal — keep the wire shape forbid it
  * @param epoch
  *   sortition epoch this committee was drawn from (so verifiers can look up the right active set for VRF verification)
  * @param diffBaseOrdinal
  *   '''Track-1 diff-base-pin.''' The cluster-uniform gl0 finalized ordinal the committee cut `derivedStateDelta.perMetagraphStateDiff`
  *   over — the producer's `mptStore.lastPersistedOrdinal` captured ATOMICALLY with its diff-prior read. Carried so every byteDiff-adopter
  *   (`GlobalSnapshotAcceptanceManager.deriveAdoptedCurrencyState`) and every re-executor (committee `reExecDerivationWithDiff`,
  *   watchtower) reads the per-MG prior `S(N)` at THE SAME pinned ordinal, instead of each node's own (per-node-lagging) `overlay.base`.
  *   This is what removes the `S(N)-lags` drop-deadlock the authoritative-override masking currently papers over. NOT [[gl0AnchorOrdinal]]
  *   (that is the exec/fee-cutover CONTEXT for the metagraph derivation — a future ordinal the checkpoint rides INTO, not the base the diff
  *   was cut FROM). Consensus-load-bearing: part of the signed [[ShardCheckpointSigPreimageV2]]. `numShards = 1` never builds checkpoints,
  *   so the default ([[io.constellationnetwork.schema.SnapshotOrdinal.MinValue]], used only by the pre-sharding regression bar and unit
  *   fixtures) is byte-neutral there.
  */
@derive(encoder, decoder, eqv, show)
final case class ShardCheckpoint(
  shardId: ShardId,
  parentCheckpointHash: Hash,
  shardOrdinal: ShardOrdinal,
  gl0AnchorOrdinal: SnapshotOrdinal,
  slot: Slot,
  derivedStateDelta: ShardDerivedStateDelta,
  emittedReceipts: List[CrossShardReceipt],
  committeeSignatures: NonEmptyList[CommitteeMemberSignature],
  epoch: EtaPeriod,
  diffBaseOrdinal: SnapshotOrdinal = SnapshotOrdinal.MinValue
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
  def signingPreimage: ShardCheckpointSigPreimageV2 =
    ShardCheckpointSigPreimageV2(
      shardId = shardId,
      parentCheckpointHash = parentCheckpointHash,
      shardOrdinal = shardOrdinal,
      gl0AnchorOrdinal = gl0AnchorOrdinal,
      slot = slot,
      derivedStateDelta = derivedStateDelta,
      emittedReceipts = emittedReceipts,
      epoch = epoch,
      diffBaseOrdinal = diffBaseOrdinal
    )
}

/** Canonical pre-image for the bytes a committee member signs (and for the chain-link hash a parent checkpoint exposes to its child).
  *
  * Fields are exactly [[ShardCheckpoint]]'s fields minus [[ShardCheckpoint.committeeSignatures]] — the field that the signatures cover.
  *
  * '''Wire-byte stability (consensus-load-bearing).''' Field order and field set here are part of the consensus contract. Adding a field,
  * reordering, or wrapping a field in `Option` silently changes the derevo magnolia-derived Circe JSON, which silently changes the
  * `Hasher[F]` output, which silently changes the bytes every signer expected to sign and every verifier expects to verify. Bump the case
  * class explicitly (`ShardCheckpointSigPreimageV2`) and version the verifier branch — never silently mutate the field set. Same
  * frozen-shape discipline as [[io.constellationnetwork.schema.slashing.SlashableEvidence.BountyDigestPreimage]].
  */
@derive(encoder, decoder, eqv, show)
final case class ShardCheckpointSigPreimage(
  shardId: ShardId,
  parentCheckpointHash: Hash,
  shardOrdinal: ShardOrdinal,
  gl0AnchorOrdinal: SnapshotOrdinal,
  slot: Slot,
  derivedStateDelta: ShardDerivedStateDelta,
  emittedReceipts: List[CrossShardReceipt],
  epoch: EtaPeriod
)

/** '''V2 canonical pre-image (Track-1 diff-base-pin — greenfield HARD CUTOVER).''' Identical to [[ShardCheckpointSigPreimage]] plus the
  * consensus-load-bearing [[ShardCheckpoint.diffBaseOrdinal]], appended as the last field. Per the frozen-shape discipline documented on
  * [[ShardCheckpointSigPreimage]], the `diffBaseOrdinal` field is NOT added to V1 in place (that would silently mutate the V1 signed
  * bytes); it is bumped into this V2 case class and [[ShardCheckpoint.signingPreimage]] is repointed here. Greenfield (no wire-compat):
  * there is NO dual-codec / version discriminator — the whole cluster hashes V2, so every committee member signs and every verifier
  * verifies the V2-encoded bytes. V1 is retained only for the round-trip fixture test; nothing in the produce/verify path references it.
  */
@derive(encoder, decoder, eqv, show)
final case class ShardCheckpointSigPreimageV2(
  shardId: ShardId,
  parentCheckpointHash: Hash,
  shardOrdinal: ShardOrdinal,
  gl0AnchorOrdinal: SnapshotOrdinal,
  slot: Slot,
  derivedStateDelta: ShardDerivedStateDelta,
  emittedReceipts: List[CrossShardReceipt],
  epoch: EtaPeriod,
  diffBaseOrdinal: SnapshotOrdinal
)
