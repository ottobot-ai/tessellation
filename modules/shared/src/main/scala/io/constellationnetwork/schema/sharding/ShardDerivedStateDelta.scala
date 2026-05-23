package io.constellationnetwork.schema.sharding

import cats.Show
import cats.data.NonEmptyList

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SharedArtifact
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import derevo.cats.eqv
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** The per-shard contribution to gl0-derived state for one checkpoint cycle.
  *
  * The values are deltas relative to the prior gl0 snapshot's accepted state; gl0 applies them as MPT writes under each metagraph's
  * subtree. The shape mirrors today's `GlobalSnapshotAcceptanceManager` per-MG output sections but is now produced by the shard committee
  * instead of every gl0 op (`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §3.1, §3.5).
  *
  * Each map key is a metagraph address belonging to this shard (per the static assignment in §4); cross-shard MG addresses are NOT included
  * here — they surface in [[ShardCheckpoint.emittedReceipts]] as [[CrossShardReceipt]] entries.
  *
  * '''Wire-byte stability.''' Adding a field is the canonical extension point — additional per-MG derivations migrated from gl0 to shards
  * (Tier 1 expansion) slot in here. The case class is a derevo-derived Circe encoder; field order is consensus-load-bearing because the
  * canonical preimage hash includes this entire structure. Reordering or wrapping a field in `Option` silently changes the bytes — bump the
  * case class explicitly if the field set has to evolve.
  *
  * '''Why `SortedMap`, not `Map`.''' The MPT writes gl0 produces from this delta have to be byte-identical across all honest nodes. `Map`'s
  * iteration order isn't deterministic; `SortedMap` is, and the underlying `Ordering[Address]` is well-defined. Same rationale applies to
  * every map field below.
  *
  * @param perMetagraphMptRoots
  *   per-MG MPT subtree root: this shard's contribution to the metagraph-tree-of-trees (see SHARDABILITY-MAP.md §3.7). gl0 stitches these
  *   into the global stateProof
  * @param includedSnapshots
  *   per-MG accepted SC binary chain (chain-linked from the prior checkpoint's tip). Mirrors the per-MG NonEmptyList from today's
  *   `stateChannelSnapshots`; carried inside the checkpoint so gl0 doesn't re-run chain-link, only checks the envelope sig
  * @param tokenLockBalancesDelta
  *   per-MG token-lock balance delta (per `TokenLockStateManager.updateTokenLockBalances`). Outer key = metagraphAddress; inner key =
  *   holderAddress; value = new Balance after this checkpoint
  * @param perMetagraphArtifacts
  *   per-MG extracted [[SharedArtifact]]s (SpendAction, PricingUpdate, GlobalSnapshotsProcessed) attributable to MGs in this shard.
  *   Cross-MG SpendActions whose currencyId targets a different shard are surfaced separately via [[CrossShardReceipt]] in §8
  * @param perMetagraphSyncDataDelta
  *   per-MG sync-data updates produced from each MG's own snapshots (the per-MG slice of
  *   `MetagraphSyncManager.updateFromCurrencySnapshots`; the cross-MG `updateFromSpendActions` slice is §8 territory)
  */
@derive(encoder, decoder, eqv)
final case class ShardDerivedStateDelta(
  perMetagraphMptRoots: SortedMap[Address, Hash],
  includedSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
  tokenLockBalancesDelta: SortedMap[Address, SortedMap[Address, Balance]],
  perMetagraphArtifacts: SortedMap[Address, List[SharedArtifact]],
  perMetagraphSyncDataDelta: SortedMap[Address, MetagraphSyncDataInfo]
)

object ShardDerivedStateDelta {

  /** Empty delta — no per-MG contributions this cycle. Used for liveness-ping checkpoints (T_alive trigger fires while no SC activity has
    * occurred since the last checkpoint; see
    * [`SHARD-CHECKPOINT-GRANULARITY.md`](../../../../../../../../docs/nakamoto/SHARD-CHECKPOINT-GRANULARITY.md) §2.4 Option C).
    */
  val empty: ShardDerivedStateDelta =
    ShardDerivedStateDelta(
      perMetagraphMptRoots = SortedMap.empty,
      includedSnapshots = SortedMap.empty,
      tokenLockBalancesDelta = SortedMap.empty,
      perMetagraphArtifacts = SortedMap.empty,
      perMetagraphSyncDataDelta = SortedMap.empty
    )

  /** Manual `Show` instance to dodge the cats / `OrphanInstances.showSortedMapAsList` ambiguity that bites `@derive(show)` over any
    * `SortedMap[K, V]` inside the `io.constellationnetwork.schema` package object (see `StakeDistribution.show` for the same workaround).
    * `Show.fromToString` is fine — this is only used for diagnostic output, not consensus bytes.
    */
  implicit val show: Show[ShardDerivedStateDelta] = Show.fromToString
}
