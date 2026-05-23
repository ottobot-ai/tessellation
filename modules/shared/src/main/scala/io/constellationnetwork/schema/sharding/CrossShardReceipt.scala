package io.constellationnetwork.schema.sharding

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** Cross-shard receipt — an asynchronous message from one shard committee to another.
  *
  * Two roles in v1 (see [`CROSS-SHARD-PROTOCOL-RESEARCH.md`](../../../../../../../../docs/nakamoto/CROSS-SHARD-PROTOCOL-RESEARCH.md) §3.1
  * Option I + §3.2/§3.3 for the cross-MG SpendAction case):
  *
  *   1. `StateReadAck`: optional carrier for "I served your read request with proof X at ord Y" — surfaced only for observability /
  *      monitoring. Not consensus-load-bearing because the read happened P2P during the shard's accept window (see
  *      `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §8).
  *
  *   1. `MetagraphSyncDataWrite`: cross-MG `MetagraphSyncManager.updateFromSpendActions` effect — shard X's emitting SpendAction targets
  *      shard Y's metagraph; shard X records the intent here; shard Y consumes from gl0's aggregated view at the next checkpoint window.
  *
  * '''Scope for slice 1.''' Only `MetagraphSyncDataWrite` is defined; `StateReadAck` is documented above and slated for a follow-up slice
  * if observability needs it. The sealed-trait shape with one initial case keeps the codec path warm for the second case to drop in without
  * a schema rev (greenfield — see `feedback_greenfield_no_wire_compat` — but the cleanest extension point is still one case-class variant
  * per role).
  */
@derive(encoder, decoder, eqv, show)
sealed trait CrossShardReceipt extends Product with Serializable

object CrossShardReceipt {

  /** Cross-MG `MetagraphSyncManager.updateFromSpendActions` effect carried from the source shard to the target shard via gl0.
    *
    * '''Producer side.''' Shard `sourceShardId` executes `SpendAction`s emitted by `sourceMetagraph`. When a SpendAction's `currencyId`
    * resolves to a metagraph that lives in a different shard (`targetShardId` ≠ `sourceShardId`), the source shard's committee cannot
    * directly write to the target metagraph's MPT subtree (that's the target shard's authority). The source committee instead emits this
    * receipt and lets gl0 aggregate.
    *
    * '''Consumer side.''' At gl0 acceptance, the receipt is folded into the target metagraph's `MetagraphSyncDataInfo` via the existing
    * `updateFromSpendActions` path. The target shard's next checkpoint observes the updated sync-data from the gl0 snapshot it rides on.
    *
    * '''Why both source and target shardId are carried.''' Defensive: the receiver re-runs the static `shardIdFor(metagraphAddress)`
    * mapping (§4.1) to verify both sides land on the expected shards. If `numShards` is misconfigured at any operator the shardId fields
    * surface the disagreement explicitly, preventing silent cross-shard mis-routing.
    *
    * '''Why `sourceCheckpointHash` is included.''' Traceability — operators investigating an unexpected target-side state change can trace
    * back to the exact source checkpoint that emitted the receipt. Not load-bearing for consensus (the gl0 snapshot already carries the
    * source `ShardCheckpoint`'s signature).
    *
    * @param sourceShardId
    *   shard that emitted the receipt (derives from `shardIdFor(sourceMetagraph)` per §4.1)
    * @param sourceMetagraph
    *   metagraph in the source shard whose SpendAction triggered the cross-shard write
    * @param sourceCheckpointHash
    *   `Hasher`-of-checkpoint at the source — observability / audit trail
    * @param targetShardId
    *   shard that owns the destination metagraph (derives from `shardIdFor(targetMetagraph)` per §4.1)
    * @param targetMetagraph
    *   metagraph receiving the sync-data increment
    * @param increment
    *   the [[MetagraphSyncDataInfo]] delta to fold into the target's existing sync-data record
    */
  @derive(encoder, decoder, eqv, show)
  final case class MetagraphSyncDataWrite(
    sourceShardId: ShardId,
    sourceMetagraph: Address,
    sourceCheckpointHash: Hash,
    targetShardId: ShardId,
    targetMetagraph: Address,
    increment: MetagraphSyncDataInfo
  ) extends CrossShardReceipt
}
