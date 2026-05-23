# Hierarchical Shard Checkpoints — Design Proposal

**Status:** draft for critic review. No code commitment. Written 2026-05-23 against
`feature/serde-typeclass-shim` at HEAD `6e49b7d5`.

**Scope:** spec for a sharded gl0 execution architecture in which a sortitioned
subset of gl0 operators (a *shard committee*) re-runs per-metagraph derivations,
signs a *shard checkpoint*, and gl0 accepts the checkpoint via signature
threshold rather than re-running. Replaces the current per-(metagraph, parentHash)
admission gate (`MetagraphCommitteeGate`) and the lag-only orphan buffer
(`MetagraphOrphanBuffer`). Greenfield — no migration ceremony.

Cross-references (read in conjunction):

- [`SHARDABILITY-MAP.md`](./SHARDABILITY-MAP.md) — per-derivation Tier 1 / 2 / 3 classification.
- [`CROSS-SHARD-PROTOCOL-RESEARCH.md`](./CROSS-SHARD-PROTOCOL-RESEARCH.md) — Option I (state-request + MPT inclusion proof) chosen for cross-shard reads.
- [`SHARD-CHECKPOINT-GRANULARITY.md`](./SHARD-CHECKPOINT-GRANULARITY.md) — Option C (committee-driven variable-content checkpoints) at default per-gl0-ord cadence.
- [`attestation-and-finality.md`](./attestation-and-finality.md) — Phase 0/1/2/3 finality model, `FinalityTrigger[F]` typeclass.
- [`SLASHING-DESIGN.md`](./SLASHING-DESIGN.md) — equivocation evidence + ledger effects to extend.
- [`COMMITTEE-SORTITION-DESIGN.md`](./COMMITTEE-SORTITION-DESIGN.md) — per-metagraph VRF-threshold primitive (`Option A`) being repurposed.
- Memory: `[[project-sharding-direction-clarified]]`, `[[project-259-root-cause-metagraph-fork-2026-05-22]]`,
  `[[project-cross-shard-cq-collapse-bound]]`, `[[project-post-nipopow-phase-order]]`,
  `[[feedback-greenfield-no-wire-compat]]`, `[[feedback-prefer-hocon-over-sysenv]]`.

---

## §1 Problem statement

### §1.1 What fails today

E2e at `8 gl0 + 4 mg + 4 shards` fails when gl0 reorgs at low ordinals orphan
metagraph binary chains. The full forensic chain (memory:
`[[project-259-root-cause-metagraph-fork-2026-05-22]]`):

1. ml0 produces a currency snapshot at metagraph M referencing parent hash
   `b22c…`. The gl0 committee at the time attests `885e…` parent `b22c…` —
   threshold reached.
2. Between attestation and gl0 accept, gl0's own consensus reorgs and its
   `lastStateChannelSnapshotHashes[M]` advances to a *different* binary
   `b62c…` at the same height (a competing binary admitted on the winning
   branch).
3. Every subsequent ml0 snapshot carries `lastSnapshotHash = b22c…`
   (chained off the parent ml0 saw at production time), but gl0's
   `GlobalSnapshotStateChannelAcceptanceManager.onlyPossibleReferences`
   (`GlobalSnapshotStateChannelAcceptanceManager.scala:191-202`) expects
   `b62c…`. The chain-link check rejects every binary forever.
4. m0 stays at `gl0-ord=2` while ml0 builds out to ord 50+. The metagraph is
   dead from gl0's point of view.

### §1.2 Why narrow fixes don't address it

The existing toolkit covers two adjacent races but not this one:

- **`MetagraphOrphanBuffer`** (`MetagraphOrphanBuffer.scala:1-120`) handles the
  *lag race*: ml0 outpaces gl0's GSI catch-up, so a child binary arrives before
  its parent has been written into `lastStateChannelSnapshotHashes`. The buffer
  parks the child, drains when the parent admits, then re-feeds. Structurally
  it cannot fix the *reorg race*: when gl0's chain reorgs *away* from the
  binary the committee already attested, the parent the orphan was waiting on
  is no longer canonical — the buffer holds binaries forever (or evicts them
  to FIFO ceiling, identical outcome).
- **`MetagraphParentOrdinalResolver` with finalized-only walk** (memory
  `[[project-259-root-cause-metagraph-fork-2026-05-22]]` proposed it as an
  inexpensive fix) would let gl0 admit a binary whose `lastSnapshotHash`
  matches *any* hash in gl0's finalized history for M, not just the current
  best-tip. This patches *this* failure mode but doesn't change the underlying
  structural property: ml0 has no protocol-level coupling to "which gl0 fork
  won". As cluster scale grows the probability of a divergent attest goes up;
  patching the resolver buys time but not strategy.
- **`MetagraphCommitteeGate`** (`MetagraphCommitteeGate.scala:23-99`) is
  per-(metagraph, parentHash) and already enforces an attestation quorum
  before admission. The bug is *not* that the gate is too permissive — every
  step is already gated. The bug is that gl0 can reorg *after* the gate fires
  and *before* the binary lands in a stable global snapshot, by which point
  the binary's chain is irreversibly tied to the loser branch's predecessor.

### §1.3 The structural answer

Every gl0 operator re-runs every metagraph's derivations: parsing currency
snapshots (`GlobalSnapshotStateChannelEventsProcessor.processCurrencySnapshots`,
395 LOC), computing `tokenLockBalances` (`TokenLockStateManager.updateTokenLockBalances`,
~50 LOC), extracting per-MG artifacts (`GlobalSnapshotAcceptanceManager.scala:1292-1357`),
chain-linking SC binaries (`GlobalSnapshotStateChannelAcceptanceManager`, 263 LOC).
This is the unsharded gl0 cost that `[[project-sharding-direction-clarified]]`
calls out as the next architectural bottleneck.

Sharding inverts the protocol coupling: instead of a binary needing to
race-survive gl0's fork-choice, it gets *embedded in a committee-signed
checkpoint*. The checkpoint is the unit gl0 accepts. gl0's own reorg can't
orphan a metagraph chain because the checkpoint commits to the chain's
prefix — if gl0 reorgs at ord N, the winning branch admits the same
checkpoint at ord N or N+1, carrying the same per-MG binaries forward.
The per-(metagraph, parentHash) admission race goes away because there's no
per-binary admission anymore — there's per-checkpoint admission, and a
checkpoint contains many binaries.

Cited in [`SHARDABILITY-MAP.md`](./SHARDABILITY-MAP.md) §5.4 Tier 1 list:
moving §2.1 (SC chain-link admission), §2.2 (per-MG currency snapshot
re-execution — *the heaviest single piece by CPU*), §2.3 (per-MG token-lock
balance derivation), §2.4 (per-MG artifact extraction) to shard committees
yields the bulk of the CPU win and *also* structurally fixes the orphan-on-reorg
class because the per-MG binary chain is no longer a unit gl0 admits directly.

---

## §2 Architectural overview

### §2.1 Three-layer model

```
+-------------------------------------------------------------+
|  ml0 (metagraph currency-l0, unchanged BFT consensus)       |
|  produces:  Signed[StateChannelSnapshotBinary]              |
+----------+-----------+-----------+-----------+--------------+
           |           |           |           |
           v           v           v           v
  +---------------+ +---------------+    (broadcast to gl0 ops via sidecar)
  | Shard 0       | | Shard 1       |
  | committee     | | committee     |  ...etc per shard
  | (subset of    | | (subset of    |
  | gl0 ops by    | | gl0 ops by    |
  | VRF sortition)| | VRF sortition)|
  +-------+-------+ +-------+-------+
          |                 |
          | per-shard mini-Taktikos chain:
          | leader VRF -> proposer -> committee attests ->
          | FinalityTrigger (T_count | T_depth1) -> Phase 2
          |
          v signed ShardCheckpoint envelopes
+-------------------------------------------------------------+
|  gl0 leader (Taktikos snowplow, unchanged)                  |
|  per-ord includes:  SortedMap[ShardId, ShardCheckpoint]     |
|  per shard admission:                                       |
|    - sig threshold ok -> accept (T_count_shard path)        |
|    - depth-only fallback -> re-execute and verify           |
|                            (T_depth1_shard path)            |
|  applies derivedStateDelta to MPT                           |
+-------------------------------------------------------------+
```

### §2.2 What changes vs today

| Concern | Today | After |
|---|---|---|
| Per-MG re-execution | every gl0 op (`accept()` at GSAM:977-2072) | shard committee only |
| Per-MG SC chain-link admission | every gl0 op via `GlobalSnapshotStateChannelAcceptanceManager` (263 LOC) | shard committee only; gl0 accepts the checkpoint envelope |
| `MetagraphCommitteeGate` per-(metagraph, parentHash) | load-bearing for binary admission (`MetagraphCommitteeGate.scala:23-99`) | **deleted** — replaced by shard committee on the per-shard mini-chain |
| `MetagraphOrphanBuffer` lag bridge | load-bearing for ml0-outpacing-gl0-GSI catch-up | **deleted** — checkpoint envelope holds chain-linked binary lists; shard chain handles within-shard lag |
| `GlobalIncrementalSnapshot.stateChannelSnapshots` field | per-MG `SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]` (`GlobalIncrementalSnapshot.scala:108`) | **removed** — replaced by `shardCheckpoints` (§3) |
| `GlobalSnapshotStateChannelEventsProcessor.processCurrencySnapshots` | runs on every gl0 op under `parTraverse` (`GlobalSnapshotStateChannelEventsProcessor.scala:240`) | runs on shard committee members only; output deltas surface via checkpoint |
| Cross-MG `SpendActionValidator` | runs against full per-MG balance/allow-spend matrix on every gl0 op (`SpendActionValidator.scala:115-262`) | Tier 2: shard committee fetches cross-shard state via Option I (P2P + MPT inclusion proof), validates locally |
| `MetagraphSyncManager.updateFromSpendActions` cross-MG writes (`MetagraphSyncManager.scala:103-129`) | runs against the union of all MGs' SpendActions per-ord | Tier 2: shard committee emits a cross-shard receipt; target shard consumes |
| Slashing surface | equivocation on `(metagraph_address, parent_hash)` per `[[SLASHING-DESIGN.md]]` §1 | additive: also `(shard_id, parent_checkpoint_hash)` for checkpoint envelopes |

### §2.3 What does not change

- ml0 / cl1 / dl1 BFT consensus — out of scope per `[[project-post-nipopow-phase-order]]`. Metagraphs remain BFT internally.
- gl0 Taktikos LDD snowplow + maxvalid-tk (`SnapshotLeaderLoop`, `ChainSelection.scala`) — leader VRF, eta rotation, depth-k finality all unchanged.
- DAG-layer block acceptance, rewards, price oracle, NIPoPoW boundary writes, delegated stake / node collateral, MPT writer — all Tier 3 per [`SHARDABILITY-MAP.md`](./SHARDABILITY-MAP.md) §4. Stay on gl0 leader.
- `Hasher[F]` typeclass for consensus-bytes serialization (`[[feedback-use-hasher-no-manual-serialize]]`).
- HOCON typed config (`[[feedback-prefer-hocon-over-sysenv]]`).

---

## §3 Schema

### §3.1 Wrapping case classes (new)

Living under `modules/shared/src/main/scala/io/constellationnetwork/schema/sharding/`
(new package, mirrors `schema/nakamoto/` for sortition / KES / NIPoPoW types):

```scala
package io.constellationnetwork.schema.sharding

import cats.data.NonEmptyList
import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.nakamoto.StakeDistribution.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import derevo.cats.{eqv, order, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import eu.timepit.refined.types.numeric.NonNegInt

/** Identifier for a static shard (range 0..M-1 inclusive, with M = numShards). */
@derive(encoder, decoder, order, eqv, show)
final case class ShardId(value: NonNegInt)

/** Monotonic per-shard sequence number for the shard's mini-Taktikos chain. */
@derive(encoder, decoder, order, eqv, show)
final case class ShardOrdinal(value: Long) extends AnyVal

/** A single committee member's contribution to a shard checkpoint.
  *
  * Membership in the committee is proven by the VRF proof (same primitive as
  * `CommitteeSortition.verifyMembership`, see `COMMITTEE-SORTITION-DESIGN.md`
  * §5). The Ed25519 long-term signature and the KES product signature both
  * cover the canonical checkpoint hash (Hasher of the envelope sans the
  * `committeeSignatures` field). Verification by gl0 acceptance applies all
  * three predicates per signer.
  */
@derive(encoder, decoder, order, eqv, show)
final case class CommitteeMemberSignature(
  peerId: PeerId,
  vrfProof: Hex,         // committee-membership proof under CommitteeSortition
  ed25519Sig: Hex,       // long-term-key signature over checkpointHash
  kesProductSig: Hex,    // KES product signature over checkpointHash
  kesTreeStep: Int       // sender-tree-internal step the KES sig was emitted at (see #211)
)

/** The per-shard contribution to gl0-derived state for one checkpoint cycle.
  *
  * The values are deltas relative to the prior gl0 snapshot's accepted state;
  * gl0 applies them as MPT writes under each metagraph's subtree. The shape
  * mirrors today's `GlobalSnapshotAcceptanceManager` per-MG output sections
  * but is now produced by the shard committee instead of every gl0 op.
  *
  * Each map key is a metagraph address belonging to this shard (per the static
  * assignment in §4); cross-shard MG addresses are NOT included here.
  *
  * Adding a new field is the canonical extension point — additional per-MG
  * derivations migrated from gl0 to shards (Tier 1 expansion) slot in here.
  */
@derive(encoder, decoder, eqv, show)
final case class ShardDerivedStateDelta(
  // per-MG MPT subtree root: this shard's contribution to the metagraph-tree-of-trees
  // (see SHARDABILITY-MAP.md §3.7). gl0 stitches these into the global stateProof.
  perMetagraphMptRoots: SortedMap[Address, Hash],

  // per-MG accepted SC binary chain (chain-linked from the prior checkpoint's tip).
  // Mirrors the per-MG NonEmptyList from today's stateChannelSnapshots; carried inside
  // the checkpoint so gl0 doesn't re-run chain-link, only checks the envelope sig.
  includedSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],

  // per-MG token-lock balance delta (per TokenLockStateManager.updateTokenLockBalances).
  // Key is (metagraphAddress -> holderAddress -> new Balance after this checkpoint).
  tokenLockBalancesDelta: SortedMap[Address, SortedMap[Address, Balance]],

  // per-MG extracted SharedArtifacts (SpendAction, PricingUpdate, GlobalSnapshotsProcessed)
  // attributable to MGs in this shard. Cross-MG SpendActions whose currencyId targets a
  // different shard are surfaced separately (§8 cross-shard receipts).
  perMetagraphArtifacts: SortedMap[Address, List[io.constellationnetwork.schema.artifact.SharedArtifact]],

  // per-MG sync-data updates produced from each MG's own snapshots (the per-MG slice of
  // MetagraphSyncManager.updateFromCurrencySnapshots; the cross-MG updateFromSpendActions
  // slice is §8 territory).
  perMetagraphSyncDataDelta: SortedMap[Address, io.constellationnetwork.schema.metagraphSync.MetagraphSyncDataInfo]
)

/** Cross-shard receipt — an asynchronous message from one shard committee to another.
  *
  * Two roles in v1 (see [`CROSS-SHARD-PROTOCOL-RESEARCH.md`](./CROSS-SHARD-PROTOCOL-RESEARCH.md) §3.1 Option I + §3.2/§3.3 for the
  * cross-MG SpendAction case):
  *   1. `StateReadAck`: optional carrier for "I served your read request with proof X
  *      at ord Y" — surfaced only for observability / monitoring. Not consensus-load-bearing
  *      because the read happened P2P during the shard's accept window (see §8).
  *   2. `MetagraphSyncDataWrite`: cross-MG `MetagraphSyncManager.updateFromSpendActions`
  *      effect — shard X's emitting SpendAction targets shard Y's metagraph; shard X
  *      records the intent here; shard Y consumes from gl0's aggregated view at the
  *      next checkpoint window.
  */
@derive(encoder, decoder, eqv, show)
sealed trait CrossShardReceipt
object CrossShardReceipt {
  final case class MetagraphSyncDataWrite(
    targetShardId: ShardId,
    targetMetagraph: Address,
    sourceMetagraph: Address,
    sourceShardId: ShardId,
    sourceCheckpointHash: Hash,    // for traceability
    increment: io.constellationnetwork.schema.metagraphSync.MetagraphSyncDataInfo
  ) extends CrossShardReceipt
}

/** Shard checkpoint envelope.
  *
  * Produced by the shard's micro-Taktikos chain. The envelope is content-agnostic
  * with respect to the metagraph type carried inside `derivedStateDelta`. Custom
  * data-application metagraphs (per [[project-sharding-direction-clarified]]) ride
  * the same envelope; only the per-MG content varies.
  *
  * Granularity = Option C per [`SHARD-CHECKPOINT-GRANULARITY.md`](./SHARD-CHECKPOINT-GRANULARITY.md) §2.4: one
  * checkpoint per gl0 ord per shard by default, with T_burst and T_alive escape
  * hatches. Empty checkpoints (no SC binaries this window) are allowed iff the
  * T_alive liveness ping fires; otherwise the shard simply has no entry this ord.
  */
@derive(encoder, decoder, eqv, show)
final case class ShardCheckpoint(
  shardId: ShardId,
  parentCheckpointHash: Hash,            // chain-link in shard's mini-chain
  shardOrdinal: ShardOrdinal,            // monotonic per-shard
  gl0AnchorOrdinal: SnapshotOrdinal,     // loose coupling — the gl0 ord this checkpoint expects to ride into; gl0 accepts at this or any later ord (see §7.2)
  derivedStateDelta: ShardDerivedStateDelta,
  emittedReceipts: List[CrossShardReceipt],
  committeeSignatures: NonEmptyList[CommitteeMemberSignature],
  epoch: EtaPeriod                       // sortition epoch this committee was drawn from
)
```

### §3.2 Fraud-proof envelope (reserved wire format)

```scala
/** Fraud-proof envelope — wire shape reserved in v1, dispute handler ships in v2.
  *
  * Purpose: a challenger that re-executes the shard committee's derivations and
  * gets a different result submits this envelope as a slashing-evidence tx (see
  * §10.2). The handler is v2 because the social-cost-vs-bond-balance design needs
  * production data we don't have yet. Reserving the bytes now (greenfield doesn't
  * require it, but cheap insurance) keeps the schema stable when the handler lands.
  */
@derive(encoder, decoder, eqv, show)
final case class FraudProofEnvelope(
  shardId: ShardId,
  disputedCheckpointHash: Hash,
  claimedDerivation: Hash,               // mptRoot of derivedStateDelta as signed by the shard committee
  challengerDerivation: Hash,            // mptRoot the challenger computed from independent re-execution
  reexecutionWitness: Array[Byte],       // proof bytes — schema TBD per derivation type, see §11.3
  challengerSignature: Hex               // Ed25519 over the rest of the envelope
)
```

### §3.3 Canonical hash for signatures

The `committeeSignatures` field is excluded when computing the bytes a signer
signs. The canonical pre-image is:

```scala
final case class ShardCheckpointSigPreimage(
  shardId: ShardId,
  parentCheckpointHash: Hash,
  shardOrdinal: ShardOrdinal,
  gl0AnchorOrdinal: SnapshotOrdinal,
  derivedStateDelta: ShardDerivedStateDelta,
  emittedReceipts: List[CrossShardReceipt],
  epoch: EtaPeriod
)
// Hashed via Hasher[F].hash(preimage) — Circe JSON + SHA-256 per [[feedback-use-hasher-no-manual-serialize]].
```

Every signer hashes this case class via `Hasher[F]` (the project-wide rule),
signs the bytes with Ed25519, signs them again with KES product, and emits both
plus the VRF membership proof. Verifier re-derives the same bytes and runs all
three predicates.

### §3.4 `GlobalIncrementalSnapshot` evolution

`modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalIncrementalSnapshot.scala:101-127`
is the current schema. The new field replaces `stateChannelSnapshots`:

```scala
// BEFORE
case class GlobalIncrementalSnapshot(
  ...
  stateChannelSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],  // line 108
  ...
)

// AFTER (greenfield — old field removed, no compat shim)
case class GlobalIncrementalSnapshot(
  ...
  shardCheckpoints: SortedMap[ShardId, Signed[ShardCheckpoint]],   // one per shard per ord; absent shards = no checkpoint produced this ord
  ...
)
```

`stateChannelSnapshots` is removed. The `Option`-typed legacy fields layered
around it in `GlobalIncrementalSnapshotV1` (lines 41-99 — the V1 codec for
chain-store snapshots produced before this schema change) are NOT modified,
because chain-store snapshot files on disk written before this schema change
must remain readable on bootstrap (the on-disk readability constraint). However,
new gl0 instances do not produce `V1`-format snapshots; the V1 case class is
read-only for backwards-deserialization of older chain-store files.

Per `[[feedback-greenfield-no-wire-compat]]`: there is no version-flip ceremony.
A single binary handles the schema. Existing chain-store snapshot files
deserialize because the `V1` codec stays; new acceptance always produces the
new shape.

Per `SHARD-CHECKPOINT-GRANULARITY.md` §2.4 Option C, each shard contributes at
most one checkpoint per gl0 ord by default. An event-trigger burst can emit
extra checkpoints; if more than one fires within a gl0 ord, gl0 includes the
later one (the one whose `parentCheckpointHash` chains off the prior). The map
shape is `SortedMap[ShardId, Signed[ShardCheckpoint]]` — a single entry per
shard per ord, chain-linked at the shard layer. Variable-burst handling at the
shard layer is described in §6.

### §3.5 MPT key layout — no new partitions required

The schema uses existing partitions:
- Per-MG MPT subtree roots fit under the existing `LastCurrencySnapshotsProofs`
  (`GlobalStateKey.scala:182`, fieldId 4) per-metagraph entries. The shard
  committee produces the subtree root; gl0 stitches it into the global tree.
- Per-MG token-lock balances continue to land in `TokenLockBalances`
  (`GlobalStateKey.scala:187`, fieldId 9), keyed by `(metagraphAddress, holderAddress)`.
- Per-MG accepted SC binaries continue to land in
  `LastStateChannelSnapshotHashes` / `LastCurrencySnapshots` /
  `LastIncrementalCurrencySnapshots` / `LastCurrencySnapshotInfo` (fieldIds 0,
  3, 5, 6) per existing layout.
- Cross-MG `MetagraphSyncManager.updateFromSpendActions` effects land in
  `MetagraphSyncData` (`GlobalStateKey.scala:196`, fieldId 18) per existing layout.

The shard-layer chain (the mini-Taktikos chain that produces checkpoints) does
NOT need an MPT partition — checkpoints are content-addressed by hash, indexed
by `(shardId, parentCheckpointHash)` in a sidecar in-memory store (analogous to
the existing `NakamotoChainStore` but per-shard) and gossiped between shard
members. Only the *accepted* checkpoint (the one folded into the gl0 snapshot)
is durable, and it's durable as a field of the global snapshot itself.

---

## §4 Shard assignment

### §4.1 Static hash mapping

Per [`CROSS-SHARD-MITIGATION-PROPOSAL.md`](./CROSS-SHARD-MITIGATION-PROPOSAL.md)
§2.7 and `[[project-sharding-direction-clarified]]`:

```scala
def shardIdFor[F[_]: Sync: Hasher](metagraphAddress: Address, numShards: Int): F[ShardId] = {
  require(numShards > 0, s"numShards must be positive, got $numShards")
  Hasher[F]
    .hash(metagraphAddress)
    .map { h =>
      val bi = BigInt(1, h.value.getBytes("UTF-8")) // canonical bytes of the hex Hash
      ShardId(NonNegInt.unsafeFrom((bi mod BigInt(numShards)).toInt))
    }
}
```

- `numShards` = `M` is a cluster-wide cluster-config constant. v1 fixes it at
  cluster start. Changes require a coordinated cluster restart (greenfield —
  no upgrade ceremony required).
- The mapping is deterministic: every gl0 op computes the same `shardId` for a
  given metagraph address. No coordination round needed.
- The function is byte-equivalent across implementations because it routes
  through `Hasher[F]` (`[[feedback-use-hasher-no-manual-serialize]]`).

### §4.2 HOCON config

Adds to `modules/node-shared/src/main/resources/application.conf` under the
existing `nakamoto { ... }` block (line 281):

```hocon
nakamoto {
  # ...existing keys...

  sharding {
    # Number of static shards. Cluster-wide constant; metagraph -> shard is
    # deterministic via `Hasher.hash(metagraphAddress) mod num-shards`.
    # Test/laptop: 4 shards over 8 operators × 4 metagraphs.
    # Production target: scales with operator count; expect 10-100 metagraphs/shard.
    num-shards = 4
    num-shards = ${?NAKAMOTO_NUM_SHARDS}

    # Target committee size per shard per epoch (the `K_target` of
    # COMMITTEE-SORTITION-DESIGN.md §5). v1: fixed cluster-wide; v2 → per-metagraph;
    # v3 → census-derived.
    committee-k-target = 4
    committee-k-target = ${?NAKAMOTO_SHARD_COMMITTEE_K_TARGET}

    # Shard chain finality params (§5.3 — per-shard FinalityTrigger instances).
    # T_count_shard threshold = ceil(2 K_S / 3) where K_S is observed committee size for that ord.
    # T_depth1_shard fallback: depth in the shard's own mini-chain.
    finality {
      depth-k1-shard = 8      # ~8 shard-ords ≈ ~56s at default cadence; greatly tightens fallback vs gl0's k₁=255
    }

    # Granularity (Option C per SHARD-CHECKPOINT-GRANULARITY.md):
    checkpoint {
      # Default emission cadence (committee-local wall clock). Matches gl0 ord cadence
      # so the steady-state is one checkpoint per shard per gl0 ord.
      t-checkpoint-ms = 7000

      # Event-trigger burst cap. If accumulated SC binaries since last checkpoint exceed
      # this, the committee may emit early.
      t-burst-binaries = 20

      # Liveness ping cadence. If no SC activity in t-alive-ms AND no checkpoint emitted in
      # that window, the committee MUST emit an empty checkpoint as a liveness signal.
      t-alive-ms = 30000
    }
  }
}
```

Typed surface on `NakamotoConfig` (`config/types.scala:85-89`):

```scala
case class NakamotoConfig(
  etaRotationSnapshots: PosLong,
  keepDepthBehindFinalized: PosLong,
  localEvents: LocalEventsConfig,
  sharding: ShardingConfig             // NEW
)

case class ShardingConfig(
  numShards: PosInt,
  committeeKTarget: PosInt,
  finality: ShardFinalityConfig,
  checkpoint: ShardCheckpointConfig
)

case class ShardFinalityConfig(
  depthK1Shard: PosLong
)

case class ShardCheckpointConfig(
  tCheckpointMs: PosLong,
  tBurstBinaries: PosInt,
  tAliveMs: PosLong
)
```

All call sites read `sharedConfig.nakamoto.sharding.<field>` — no `sys.env.get`
per `[[feedback-prefer-hocon-over-sysenv]]`. The existing
`MetagraphCommitteeGate.scala`'s direct `sys.env.get("NAKAMOTO_COMMITTEE_K_TARGET")`
(lines 53-56) was already a violation; the migration to `ShardingConfig.committeeKTarget`
happens as part of code removal (§12).

### §4.3 Late-registered metagraphs

When a metagraph registers after the cluster has started (genesis ordinal > 0
relative to the new metagraph), the deterministic shard assignment applies
immediately at registration ord. No re-balancing is performed in v1.

This means shard load is governed by hash uniformity — for `M ≪ N_metagraphs`
the law of large numbers gives roughly balanced shards. For pathological cases
where many high-volume metagraphs collide on one shard, v2 introduces
consistent hashing with virtual nodes or per-epoch redistribution
(`[[project-sharding-direction-clarified]]` v2 backlog).

### §4.4 Composition with `m=1` (one shard per metagraph) variant

[`CROSS-SHARD-MITIGATION-PROPOSAL.md`](./CROSS-SHARD-MITIGATION-PROPOSAL.md)
§2.4 locks `m=1, S=M` for v1 in *its* analysis. This design generalises to
`S ≤ M` (multiple MGs per shard). With `S = M` (the conservative case) each
shard contains exactly one metagraph; with `S < M` shards aggregate. Both work
under the schema (the `SortedMap[Address, ...]` per `ShardDerivedStateDelta`
naturally holds any number of MGs).

For e2e at `4 mg + 4 shards`, the test happens to be in the `S = M` regime. For
production at `1K-3K metagraphs / 100 shards` the per-shard aggregate is in the
`S ≪ M` regime, identical schema.

---

## §5 Within-committee consensus — mini-Taktikos chain

### §5.1 No BFT inside the committee

Per the brief (and reflecting `[[project-sharding-direction-clarified]]`):
**each shard runs its own micro-Taktikos chain**. We do NOT introduce a new
BFT consensus inside the committee. Reasons:

1. The cluster already runs Taktikos at gl0; building BFT inside the committee
   would mean two consensus protocols to harden, debug, and slash against.
2. The Taktikos primitives (LDD eligibility, maxvalid-tk, depth-k finality,
   attestation triggers) compose into a smaller chain trivially — every
   primitive listed below is already implemented in the codebase, just scoped
   per-shard.
3. BFT inside a committee re-introduces the "1/3 corruption ⇒ full shard
   compromise" failure mode that
   `[[project-cross-shard-cq-collapse-bound]]` Option A specifically avoids
   via VRF sortition + slashing.

### §5.2 Components reused per-shard

| Primitive | Scope today | Scope per-shard |
|---|---|---|
| `EligibilityChecker.checkEligibility` (`EligibilityChecker.scala:57-74`) | gl0 leader VRF | per-shard slot leader VRF; active stake set = this shard's committee members |
| `CommitteeSortition.isInCommittee` (`CommitteeSortition.scala:51-58`) | per-(metagraph, parentHash) attestation eligibility | per-(shard, epoch) committee membership — the very thing being sortitioned |
| `ChainSelection.standardCompare` (Taktikos maxvalid-tk) (`ChainSelection.scala`) | gl0 fork choice on `NakamotoChainStore` | shard-chain fork choice on a per-shard analogue store |
| `FinalityTrigger[F]` typeclass (`FinalityTrigger.scala:27-47`) | gl0 Phase 1→2 / 2→3 advancement | per-shard Phase 1→2 advancement (`T_count_shard`, `T_depth1_shard`) |
| `TipTracker` (`TipTracker.scala`) | gl0 attestation accumulator | per-shard attestation accumulator |
| `SlotClock` (`SlotClock.scala`) | gl0-wide global slot clock | same global slot clock — see §5.4 |

### §5.3 Slot leader election within a shard

Each gl0 slot tick (~1 Hz, see [`SHARD-CHECKPOINT-GRANULARITY.md`](./SHARD-CHECKPOINT-GRANULARITY.md) §1.1) every
committee member of shard `s` runs:

```scala
for {
  shardEta <- EpochStateManager.shardEta(currentEpoch, shardId)
  // shardEta derives by domain-separating gl0's eta with shardId:
  //   shardEta = Hasher.hash(ShardEtaInput("shard", gl0Eta, shardId))
  // Domain-separated so a shard leader VRF doesn't leak gl0 leader eligibility.
  isLeader <- EligibilityChecker.checkEligibility(
    vrfSk,
    slot,
    slotGap,
    shardEta,
    committeeRelativeStake,    // 1 / K_S — uniform within committee per #216 stable σ rule
    shardLddConfig
  )
} yield isLeader
```

- The relative stake denominator inside the committee is uniform `1 / K_S`
  (per `[[project-216-committee-stake-drift-fix]]`'s `committeeStake = 1/N`
  pattern). All committee members have equal voting weight for slot-leader
  selection.
- LDD calibration (`shardLddConfig`) keeps expected inter-checkpoint cadence
  near `t-checkpoint-ms` (7s by default).
- Multiple winners are possible per slot (LDD's non-uniform property). When
  multiple winners produce checkpoints, the shard's `ChainSelection` picks via
  the same Taktikos rule gl0 uses — `standardCompare` on the shard chain.

### §5.4 Phase semantics within a shard

| Phase | Name | Trigger advancing INTO it |
|---|---|---|
| 0 | PENDING | A new shard-chain tip is produced by a slot leader and gossiped to committee members |
| 1 | PROVISIONAL | `ChainSelection.standardCompare` on the shard chain elects this tip as ancestor of bestTip |
| 2 | SETTLED | Any of: `T_count_shard` (≥ ⌈2 K_S / 3⌉ committee members attest), `T_weight_shard` (uniform K_S → identical to T_count_shard at uniform σ, kept for parity), `T_depth1_shard` (shard-chain depth > `depth-k1-shard`, default 8 shard-ords ≈ 56s) |
| 3 | ARCHIVAL | NOT required for shard chains in v1 — gl0 is the archival anchor (a checkpoint admitted into gl0 ord N is archival-finalized when gl0 ord N reaches Phase 3). No per-shard ARCHIVAL pruning logic needed |

A checkpoint at Phase 2 in the shard chain is eligible to be included in the
next gl0 ord by the gl0 leader (§7). gl0's per-(shardId) admission check
distinguishes Phase 2 *via T_count_shard* (signature-threshold-only, common
case) from Phase 2 *via T_depth1_shard only* (degraded liveness; gl0 re-executes
to verify before accepting — §7.3).

### §5.5 What state the shard chain holds

In-memory per gl0 op participating in shard `s`:

- A `NakamotoChainStore`-analogue scoped to this shard: maps
  `parentCheckpointHash -> Set[Signed[ShardCheckpoint]]` (the fork DAG of
  shard tips). Bounded by the same `keep-depth-behind-finalized` window
  pattern as gl0's `NakamotoChainStore.byHash` (application.conf:301).
- A `TipTracker` of inbound attestations from other shard members:
  `Map[PeerId, (shardOrdinal, checkpointHash)]`.
- A small set of `FinalityTrigger[F]` instances (`T_count_shard`,
  `T_depth1_shard`) built via the existing
  `FinalityTrigger.fromRef[F]` (`FinalityTrigger.scala:124-144`).

Non-shard gl0 ops (those not in the committee for shard `s`) hold no shard
chain state for `s`. They consume the eventually-accepted checkpoint from
the gl0 snapshot they finalize.

### §5.6 Eta scoping

`shardEta` is derived per-(epoch, shardId) from gl0's `eta` via Hasher domain
separation:

```scala
final case class ShardEtaInput(tag: String, gl0Eta: Hex, shardId: ShardId)
// tag = "shard"; computed once per epoch boundary per shard.
```

This keeps the per-shard slot lottery independent across shards (no global
shard alignment artefact) while inheriting gl0's eta freshness guarantees
(`attestation-and-finality.md` §1).

---

## §6 Shard checkpoint production

### §6.1 Cadence & triggers (Option C per `SHARD-CHECKPOINT-GRANULARITY.md`)

A committee member assembles and signs a checkpoint when ANY of:

| Trigger | Description | Default | Source ref |
|---|---|---|---|
| `T_checkpoint` | wall-clock since last emission ≥ `t-checkpoint-ms` (default 7s) AND there are new SC binaries OR a forced `T_alive` | per gl0 ord boundary by intent | `SHARD-CHECKPOINT-GRANULARITY.md` §4.5 row 1 |
| `T_burst` | accumulated SC binaries since last emission ≥ `t-burst-binaries` (default 20) | event-trigger early emission | `SHARD-CHECKPOINT-GRANULARITY.md` §4.5 row 2 |
| `T_alive` | no emission in `t-alive-ms` (default 30s) | liveness ping (allows empty payload) | `SHARD-CHECKPOINT-GRANULARITY.md` §5.2 |

Empty checkpoints (`derivedStateDelta` with empty maps and no `includedSnapshots`)
are valid only when emitted by `T_alive`. gl0's admission (§7) skips empty
checkpoints from inclusion unless the empty payload's `parentCheckpointHash`
chains to an earlier non-empty checkpoint that hasn't been included yet (in
which case the empty acts as a liveness anchor that the prior is still alive
in the shard chain).

### §6.2 What the committee re-executes

The shard committee runs the per-MG slice of `GlobalSnapshotAcceptanceManager.accept()`
restricted to MGs `m` for which `shardIdFor(m) == thisShardId`:

| Step today (`GlobalSnapshotAcceptanceManager.scala`) | New scope |
|---|---|
| SC chain-link admission (line 1258-1272, calls `GlobalSnapshotStateChannelAcceptanceManager.accept`) | shard committee for its MGs |
| Per-MG currency snapshot re-execution (line 1258-1272, `processStateChannelEvents → processCurrencySnapshots`) | shard committee for its MGs |
| Per-MG token-lock balance derivation (line 1504-1509, `tokenLockStateManager.updateTokenLockBalances`) | shard committee for its MGs |
| Per-MG SharedArtifact extraction (lines 1292-1357) | shard committee for its MGs |
| `MetagraphSyncManager.updateFromCurrencySnapshots` per-MG slice (`MetagraphSyncManager.scala:72-101`) | shard committee for its MGs |
| Per-MG MPT subtree root computation | shard committee for its MGs (output: `derivedStateDelta.perMetagraphMptRoots`) |

The output `ShardDerivedStateDelta` is the deterministic transcript of these
steps for the MGs in shard `s`, contemporary with `gl0AnchorOrdinal`. Every
honest committee member computes byte-equivalent bytes — that's the property
that makes slashing for wrong-derivation a clean cryptographic identity (§10.2).

### §6.3 Signing

A slot leader's path on producing a checkpoint:

1. Compute `derivedStateDelta` per §6.2.
2. Construct `ShardCheckpointSigPreimage` (§3.3). Hash it via `Hasher[F]`.
3. Sign with Ed25519 long-term key, sign with KES product key.
4. Construct `CommitteeMemberSignature(self, vrfProof, ed25519Sig, kesProductSig, kesTreeStep)`.
   `vrfProof` is the *committee-membership* VRF proof produced at sortition time
   (CommitteeSortition output for this epoch) — NOT the slot-leader VRF proof.
5. Wrap as `Signed[ShardCheckpoint]` with the slot leader as the single signer,
   and gossip to other shard committee members.

Other committee members receiving the checkpoint:

1. Verify the slot leader's signature (Ed25519 + KES + committee VRF — same
   three-predicate verification as `MetagraphCommitteeGate.recordReceivedAttestation`
   today, `MetagraphCommitteeGate.scala:42-47`).
2. Re-execute the per-MG derivations independently (§6.2).
3. Check that the locally-computed `ShardCheckpointSigPreimage` byte-equals
   the leader's. If equal, sign the same preimage with their own keys and
   gossip their `CommitteeMemberSignature`. If unequal, do NOT sign — instead
   record the divergence locally for fraud-proof construction (§10.2).
4. The slot leader (or any aggregator) collects signatures into the
   `Signed[ShardCheckpoint]` envelope's `committeeSignatures` list.

### §6.4 Gossip path

Reuses existing sidecar gossip primitives:

- Per-shard topic: `shard-checkpoint-<shardId>`. Members of shard `s`
  subscribe to topic `s`. Non-members don't subscribe (the sharding payoff —
  if they're not in the shard they shed the network load).
- The sidecar (Go libp2p GossipSub bridged via gRPC at
  `SidecarClient`, `attestation-and-finality.md` §2) publishes
  `pb.ShardCheckpoint` and `pb.ShardCheckpointAttestation` analogous to
  today's `pb.MetagraphBinary` and `pb.MetagraphAttestation`.
- A separate gl0-wide topic carries the *fully-signed* checkpoint envelope
  (the one with `committeeSignatures` populated past threshold) for
  consumption by the gl0 leader. Non-shard gl0 ops subscribe to *all* shards'
  envelope-published topics (light load: one signed envelope per shard per gl0
  ord ≈ S × 1 message / 7s). They use these to verify and apply the
  derivedStateDelta when their gl0 ord finalizes.

The wire protobuf schema mirrors today's `pb.MetagraphAttestation` shape:
`(shardId, parentCheckpointHash, shardOrdinal, gl0AnchorOrdinal, payload_bytes,
attester_pid, vrf_proof, long_term_sig, kes_sig, kes_tree_step)`. Wire format
spec out of scope for this doc.

---

## §7 gl0 admission

### §7.1 Inclusion at the gl0 leader

When the gl0 leader builds a snapshot at ord `N`, for each shard `s` it:

1. Looks at the locally-observed pool of `Signed[ShardCheckpoint]` envelopes
   for `s` with `gl0AnchorOrdinal ≤ N`.
2. Filters to envelopes that have reached Phase 2 in shard `s`'s mini-chain
   (per the leader's local `FinalityTrigger[F]` instances for shard `s`).
   Non-committee gl0 leaders also track shard mini-chains for shards they
   don't sit in — they're consumers of the chain just like the committee
   members are, just without the right to produce; the `FinalityTrigger`
   instances are part of the gl0 leader's state.
3. From the Phase-2-qualified set for `s`, picks the longest chain whose tip
   is also Phase 2 (Taktikos `maxvalid-tk` at the shard layer; this is the
   same `ChainSelection.standardCompare` used at gl0).
4. Includes the tip envelope in the new gl0 snapshot at `shardCheckpoints[s]`.

If shard `s` produced no Phase-2 envelope with `gl0AnchorOrdinal ≤ N`, the
leader omits `s` from `shardCheckpoints` for ord `N`. That is the normal
no-progress case (shard idle, no SC binaries) per §6.1.

### §7.2 Loose coupling — `gl0AnchorOrdinal`

The `gl0AnchorOrdinal` field is the shard committee's *expected* gl0 ord at
production time, not a hard constraint. gl0 accepts a checkpoint with
`gl0AnchorOrdinal ≤ N` at ord `N` (i.e., a checkpoint expecting ord N may
ride into N, N+1, N+2…). This handles a class of race conditions:

- Shard `s` produces a checkpoint expecting gl0 ord 100. gl0 ord 100 is then
  delayed by a reorg or slow slot.
- The checkpoint can be included in gl0 ord 101 or 102, no problem. The
  committee doesn't have to re-issue.
- Conversely, a checkpoint with `gl0AnchorOrdinal > N` is rejected at ord N
  — the shard committee is ahead of gl0's view.

This is the same "loose coupling" rationale `SHARD-CHECKPOINT-GRANULARITY.md`
§3.5 uses to argue that the shard's `coversToWallClock` need not strictly equal
the gl0 boundary.

### §7.3 Differentiated admission check

When gl0 ord `N` accept includes `shardCheckpoints[s] = sc`, the per-shard
acceptance manager (new — see §13 implementation slice 9) runs:

```scala
// Phase 2 via T_count_shard ⇒ signature-only path (the common case)
val countQualified: Boolean = sc.committeeSignatures.size >= ceilTwoThirds(observedShardKSize)
val depthOnlyQualified: Boolean = !countQualified && shardDepthAtCheckpoint(sc) >= shardingConfig.finality.depthK1Shard

if (countQualified) {
  // Verify each CommitteeMemberSignature (3 predicates: VRF + Ed25519 + KES, all over
  // ShardCheckpointSigPreimage). Reject if any signature fails. Apply derivedStateDelta.
  verifyAllAndApply(sc)
} else if (depthOnlyQualified) {
  // No attestation quorum but the shard chain has advanced past depth k1_shard with this
  // checkpoint as ancestor. Re-execute the derivations from authoritative MPT state and
  // compare the recomputed mptRoot with sc.derivedStateDelta.perMetagraphMptRoots.
  val reexecuted = reexecutePerMgDerivations(sc)
  if (reexecuted.matchesByteEquivalent(sc.derivedStateDelta)) {
    applyVerified(sc)
  } else {
    // Mismatch: signer(s) deviated from determinism. Slash them (§10.2 wrong-derivation),
    // reject this checkpoint, the gl0 acceptance fails this shard for this ord.
    slashAndReject(sc, reexecuted)
  }
} else {
  // Neither path qualifies; the checkpoint is not eligible to be included.
  reject(sc, "Not Phase 2 in shard chain")
}
```

This gives **graceful liveness degradation**: a single online committee member
can keep the shard advancing via `T_depth1_shard` (no quorum required, the
shard chain just grows), at the cost of pausing the sharding benefit during the
degraded window (gl0 has to re-execute). When the committee quorum recovers
in subsequent epochs, the cheap `T_count_shard` path resumes.

### §7.4 What gl0 does with `derivedStateDelta`

The fields of `derivedStateDelta` are applied as MPT writes per the existing
partition layout (§3.5):

- `perMetagraphMptRoots` → stitches into `LastCurrencySnapshotsProofs` (fieldId 4) per MG
- `includedSnapshots` → unfolds chain-linked into `LastStateChannelSnapshotHashes` (0), `LastCurrencySnapshots` (3), `LastIncrementalCurrencySnapshots` (5), `LastCurrencySnapshotInfo` (6) per MG
- `tokenLockBalancesDelta` → writes to `TokenLockBalances` (9) keyed by `(metagraphAddress, holderAddress)`
- `perMetagraphArtifacts` → consumed at the global pass: cross-shard `SpendAction`s go into the per-shard cross-shard receipt queue (§8), single-shard `SpendAction`s feed directly into the global accept's `spendActions` map (currently at GSAM:1344 — the input shape doesn't change, the *source* of the per-MG entries does)
- `perMetagraphSyncDataDelta` → writes to `MetagraphSyncData` (18) per MG

The gl0 leader no longer runs `processCurrencySnapshots`, `updateTokenLockBalances`,
or the per-MG artifact extraction loops in-line — those steps are replaced
by stitching the union of shards' `derivedStateDelta`s. The gl0 leader DOES
still run all Tier 3 work ([`SHARDABILITY-MAP.md`](./SHARDABILITY-MAP.md) §4):
DAG block acceptance, rewards, price oracle, NIPoPoW boundaries, delegated
stake / node collateral, MPT writer, etc.

---

## §8 Cross-shard interaction (Tier 2)

### §8.1 Choice — Option I from `CROSS-SHARD-PROTOCOL-RESEARCH.md`

[`CROSS-SHARD-PROTOCOL-RESEARCH.md`](./CROSS-SHARD-PROTOCOL-RESEARCH.md) §4.1
recommends Option I: state-request + MPT inclusion proof. v1 adopts this.

### §8.2 The cross-shard read case — `SpendActionValidator`

The motivating case ([`SHARDABILITY-MAP.md`](./SHARDABILITY-MAP.md) §3.1):
shard X is validating a SpendAction emitted by metagraph M_x but referencing
an `AllowSpend` whose source belongs to metagraph M_y in shard Y. Today
(`SpendActionValidator.scala:115-262`) the validator runs against a globally-
materialized `activeAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]`
that includes every metagraph's scope.

Under sharding, shard X holds only the per-MG-in-X slice plus the DAG-global
(`None`) slice. For the cross-shard reference:

```
// shard X's committee processing a SpendAction with currencyId = Some(M_y) where shardIdFor(M_y) = Y ≠ X

// 1. Identify the cross-shard read: need ActiveAllowSpends(Some(M_y), sourceAddr).
// 2. Issue a P2P request to any (preferably fastest) shard-Y member via the gl0 overlay:
//      GET /shard/Y/mpt/proof?key=hypergraph(ActiveAllowSpends, Some(M_y), sourceAddr)&anchor=gl0Ord-1
// 3. Shard Y responds with (allowSpendValue, MerklePatriciaInclusionProof) rooted at shard Y's
//    last committee-signed checkpoint's mptRoot (which is in gl0's last accepted snapshot).
// 4. Shard X verifies the proof against shard Y's mptRoot (which shard X already knows from
//    gl0's last snapshot's per-shard mptRoot stub). Reuses `HistoricalMptProofService.proofAtBranch`
//    on the verifier side; on the prover side a per-shard subtree-rooted variant.
// 5. Shard X injects the proven `AllowSpend` into the local SpendActionValidator's input map and
//    proceeds. The proven entry is byte-equivalent to what an unsharded gl0 op would have read.
```

### §8.3 Read-after-write staleness

The proof is rooted at shard Y's *last committee-signed checkpoint that
landed in a gl0 snapshot*. That is at most one gl0 snapshot stale (~7s). A
SpendAction-on-AllowSpend that was issued in shard Y at the current gl0 ord
will not be visible to shard X until the next gl0 ord.

For v1 the staleness is **accepted** as per
[`CROSS-SHARD-PROTOCOL-RESEARCH.md`](./CROSS-SHARD-PROTOCOL-RESEARCH.md) §4.3.
Most cross-metagraph workflows are not latency-tight enough to notice. The
class of workflows that needs sub-7s cross-shard visibility (real-time multi-MG
atomic settlement) is the v2 escalation target for Option III.

### §8.4 Cross-shard writes — `MetagraphSyncManager.updateFromSpendActions`

The cross-shard *write* surface ([`SHARDABILITY-MAP.md`](./SHARDABILITY-MAP.md)
§3.3): metagraph M_x's SpendAction targeting metagraph M_y triggers a write to
`metagraphSyncData[M_y]`. Today this happens in
`MetagraphSyncManager.updateFromSpendActions` (`MetagraphSyncManager.scala:103-129`)
at the gl0 leader's `accept()`.

Under sharding the write is encoded as a `CrossShardReceipt.MetagraphSyncDataWrite`
(§3.1), emitted in shard X's checkpoint at `emittedReceipts`. The gl0 leader,
when stitching ords, applies these receipts:

```scala
// Inside the gl0 leader's per-ord accept, after applying perMetagraphSyncDataDelta deltas
// from every included shard checkpoint, walk the union of emittedReceipts:
included.values.flatMap(_.value.emittedReceipts).foreach {
  case CrossShardReceipt.MetagraphSyncDataWrite(targetShard, targetMg, _, _, _, increment) =>
    // The target MG is in `targetShard`. Apply the increment to MetagraphSyncData(targetMg) MPT entry.
    // gl0 is the canonical applier — no shard-Y round trip needed because the write is from
    // shard X's authoritative SpendAction, signed by shard X's committee.
    applyMetagraphSyncDataIncrement(targetMg, increment)
}
```

This stays atomic with gl0's existing MPT writer. Shard Y's committee will see
the resulting state at its next checkpoint's `gl0AnchorOrdinal`.

### §8.5 Reuse of `HistoricalMptProofService`

The existing `HistoricalMptProofService.proofAtBranch` (`HistoricalMptProofService.scala:23-29`)
takes a `BranchId + SnapshotOrdinal + GlobalStateKey` and returns a
`MerklePatriciaInclusionProof`. The proof verifier on shard X uses the standard
MPT verifier against the per-shard subtree root carried in gl0's last snapshot's
`stateProof`.

The new prover-side service (shard committee side):

```scala
trait ShardSubtreeProofService[F[_]] {
  // Generate a proof rooted at shard Y's last committee-signed checkpoint's mptRoot
  // for key K within M's subtree.
  def proofForKey(
    shardId: ShardId,
    metagraphAddress: Address,
    key: GlobalStateKey,
    anchorGl0Ord: SnapshotOrdinal
  ): F[Either[ProofError, MerklePatriciaInclusionProof]]
}
```

Implementation adapts `HistoricalMptProofService` to the per-shard subtree:
each shard committee maintains the subtree at the per-MG subtree root the
committee signed. The proof is structurally identical (same MPT verifier on
the consumer side); the difference is the root.

### §8.6 Cross-shard read protocol — wire shape

P2P request (gl0 overlay; reuses existing peer communication primitives):

```
GET /shard/{shardId}/proof
  ?metagraph={metagraphAddress}
  &key={hexKey}
  &anchor={gl0SnapshotOrdinal}
```

Response (200):
```json
{
  "value": "<base64-encoded-value-bytes>",
  "proof": "<base64-encoded-MerklePatriciaInclusionProof>",
  "rootedAtCheckpoint": "<hex hash of the shard checkpoint this proof anchors at>"
}
```

The verifier on the requester side:
1. Looks up `rootedAtCheckpoint` in the last gl0-finalized snapshot's `shardCheckpoints[shardId]`. Reject if mismatch (the responder is serving from a checkpoint not finalized in gl0 — wait or retry).
2. Extracts `perMetagraphMptRoots[metagraphAddress]` from that checkpoint's `derivedStateDelta` — this is the verifier root.
3. Runs the standard `MerklePatriciaInclusionProof.verify` against the root.
4. If verify passes, uses `value` as the read result.

Fan-out: the requester can query multiple shard-Y members in parallel and use
the first valid response (per [`CROSS-SHARD-PROTOCOL-RESEARCH.md`](./CROSS-SHARD-PROTOCOL-RESEARCH.md)
§5.4 liveness backstop). All honest members serve byte-equivalent proofs.

### §8.7 What stays out of scope for v1

- **Strong atomicity** (Option III escrow / pre-commit). Cross-shard atomic
  writes that need round-trip commitment go to v2 backlog.
- **Cross-shard NIPoPoW composition** — per [`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) and explicitly out of scope per
  [`CROSS-SHARD-PROTOCOL-RESEARCH.md`](./CROSS-SHARD-PROTOCOL-RESEARCH.md) §0.
- **Receipts for cross-shard *reads*** (Option II) — Option I's synchronous-pull
  shape avoids the multi-round async cost; receipts only carry cross-shard
  *writes* (§8.4) which are already async in semantics.

---

## §9 Liveness recovery

### §9.1 Stall — shard committee can't reach quorum

Within an epoch, if shard `s`'s committee can't reach `T_count_shard` (≥ ⌈2 K_S
/ 3⌉ attesters), the shard chain still advances via slot leaders producing
checkpoints — those checkpoints just sit at Phase 1 (PROVISIONAL). After
`depth-k1-shard` shard ords (default 8 ≈ 56s) `T_depth1_shard` fires and the
checkpoint qualifies for Phase 2 via the depth path. gl0 accepts it via the
re-execute-and-verify path (§7.3).

Latency cost of degraded operation:

| Healthy | Degraded |
|---|---|
| `T_count_shard` fires within ~1 gl0 ord (~7s) | `T_depth1_shard` fires after ~8 shard ords (~56s) |
| gl0 admission is signature-only (cheap) | gl0 admission re-executes (CPU cost equal to today's unsharded) |
| sharding payoff fully realized | sharding payoff paused for this shard until quorum recovers |

### §9.2 Epoch boundary rotation

At each eta-period boundary (per `[[project-consensus-epoch-staggering]]` —
Cardano-style), the committee for shard `s` is *re-sortitioned* from the
N-2 stake distribution. A previously-degraded shard recovers if the new
committee's stake distribution is healthier.

Per the brief and `[[project-sharding-direction-clarified]]`: **no mid-epoch
committee reshuffling**. The committee is fixed for the epoch. The soft cap
on liveness degradation is the epoch length (default `eta-rotation-snapshots`
= 2550 snapshots ≈ ~5 hours at 7s/ord production cadence; e2e tests run at
100 ≈ ~12 min).

### §9.3 Comparison to other protocols

- **NEAR Nightshade**: chunk producers per shard rotate per epoch; chunks with
  insufficient endorsement can still be included via "chunk-only producer"
  fallback (analogous to our `T_depth1_shard` path). Reference:
  [`CROSS-SHARD-PROTOCOL-RESEARCH.md`](./CROSS-SHARD-PROTOCOL-RESEARCH.md)
  §1.3.
- **Polkadot Elastic Scaling**: parachain backers per slot can fall back to
  relay-chain-included blocks even when collator committee underperforms.
  Reference: [`SHARD-CHECKPOINT-GRANULARITY.md`](./SHARD-CHECKPOINT-GRANULARITY.md)
  §4.5 Polkadot row.

Neither uses mid-epoch reshuffling either; the soft-cap-on-epoch property is
standard.

### §9.4 Hard partition — whole shard offline

If shard `s` produces no Phase 2 checkpoint for ≥ `T_partition_hard` (default
~150s, ≈ `5 × t-alive-ms` per [`SHARD-CHECKPOINT-GRANULARITY.md`](./SHARD-CHECKPOINT-GRANULARITY.md)
§5.2), the gl0 leader logs a `dag_nakamoto_shard_partition_total{shardId=s}`
counter increment and emits a `SHARD-PARTITION-SUSPECT` warning. Operator
intervention (out-of-band) is required; the shard's metagraphs are dead until
the next epoch boundary at minimum. v1 does not implement automatic emergency
rotation; v2 may.

---

## §10 Slashing

Additive to [`SLASHING-DESIGN.md`](./SLASHING-DESIGN.md). All ledger-effect
machinery (§5 of that doc) is reused intact.

### §10.1 Equivocation — same shard committee member signing two children of the same parent

The equivocation key is `(shardId, parentCheckpointHash)`. Two valid
`CommitteeMemberSignature` instances from the same `peerId` over two distinct
`Signed[ShardCheckpoint]`s with the same `(shardId, parentCheckpointHash)`
and different `Signed[ShardCheckpoint].hash` are slashable. This is the same
algebra `SLASHING-DESIGN.md` §1 uses for metagraph attestations, scoped to the
shard checkpoint level.

```scala
final case class ShardCheckpointEquivocationEvidence(
  shardId: ShardId,
  parentCheckpointHash: Hash,
  signatureA: CommitteeMemberSignature,
  signatureB: CommitteeMemberSignature,    // same peerId, same parentCheckpointHash, different child hash
  childCheckpointHashA: Hash,
  childCheckpointHashB: Hash,
  submitterId: PeerId,
  bountySignature: Hex
)
```

Validator (extends `SlashableEvidenceValidator`):
1. `signatureA.peerId == signatureB.peerId`
2. Both `vrfProof` fields verify under the same operator VK against the same
   `(shardEta, shardId, epoch)` (per §5.6 / §5.3)
3. Both `ed25519Sig` fields verify under the same operator long-term VK over
   their respective `childCheckpointHash` preimages
4. Both `kesProductSig` fields verify under the same KES master VK at the
   declared `kesTreeStep` per `KesRegistry`
5. `childCheckpointHashA != childCheckpointHashB`
6. Both preimages share the same `(shardId, parentCheckpointHash, epoch)`
7. Not already slashed (MPT key `slashings/<peer_id>/<shardId>/<parent_checkpoint_hash>`)
8. Within evidence window (per `SLASHING-DESIGN.md` §6 `evidence_window` = 100 epochs)
9. Submitter signature verifies

Ledger effect: same as `SLASHING-DESIGN.md` §5 — stake reduction, eviction,
bounty, burn.

### §10.2 Wrong derivation — signed checkpoint diverges from deterministic re-execution

If a committee member signs a `ShardCheckpoint` whose `derivedStateDelta` does
not match the byte-equivalent result of re-running the per-MG derivations from
the authoritative MPT state (§6.2), that is slashable. Detection happens at
honest committee members during the per-§6.3 verify step, and at gl0 during
the depth-fallback path (§7.3).

```scala
final case class WrongDerivationEvidence(
  shardId: ShardId,
  disputedCheckpointHash: Hash,
  signer: CommitteeMemberSignature,         // signed the (wrong) derivedStateDelta
  envelope: FraudProofEnvelope,             // §3.2 — carries claimedDerivation, challengerDerivation, witness
  submitterId: PeerId,
  bountySignature: Hex
)
```

**v1**: the wire format ships (per §3.2 / §11.3) so we have the bytes in the
binary. **The dispute handler itself ships in v2**. Reason: re-execution
witness sizes for the various derivation paths (`processCurrencySnapshots`,
`updateTokenLockBalances`, artifact extraction) need empirical sizing under
production load before we can finalise the witness shape; the handler design
will also benefit from v1 operating data on how often divergences occur (none
expected in healthy operation; the value comes from the deterrent).

Honest committee members observing divergence in v1 (between their local
re-execution and a signed checkpoint they received) record the divergence in a
local audit log and WARN. They do NOT yet submit slashing evidence — they
*could* but the handler isn't there to accept it. The local audit log is the
data we need to size the v2 witness format.

### §10.3 Non-participation accumulator

Per-epoch counter, per (peerId, shardId):

```scala
final case class NonParticipationCounter(
  shardId: ShardId,
  peerId: PeerId,
  epoch: EtaPeriod,
  electedAsSlotLeaderCount: Long,        // # slots where this peer was elected as leader (per shard's EligibilityChecker)
  failedToProduceCount: Long,             // # of those slots where they didn't emit a checkpoint
  receivedCheckpointCount: Long,          // # of checkpoints from other slot leaders this peer should have attested
  failedToAttestCount: Long               // # of those they didn't attest within attestation window
)
```

At epoch boundary, gl0 reads these counters from a per-(shard, epoch) MPT
partition (new system-namespaced partition, fieldId TBD in the implementation
slice). If for a given (peerId, shardId, epoch):

```scala
failedToProduceCount.toRatio / electedAsSlotLeaderCount > nonParticipationThreshold  // default 0.5
||
failedToAttestCount.toRatio / receivedCheckpointCount > nonParticipationThreshold
```

then the operator is slashed at the epoch boundary with reduced severity
(`slash_fraction_non_participation` default 0.05 = 5%, much smaller than
equivocation's 100%). Reuses the same `SLASHING-DESIGN.md` §5 ledger-effect
pipeline.

### §10.4 Severity ordering

| Surface | Default `slash_fraction` | Rationale |
|---|---|---|
| Equivocation (same parent, two children) | 1.00 (100%, per `SLASHING-DESIGN.md` §6) | Cryptographic proof of intent; closes adaptive-corruption window |
| Wrong derivation (signed garbage) | 0.50 (v2; tunable) | Provably divergent from deterministic re-execution; same severity tier as equivocation but lower because honest-validator-with-buggy-implementation tail risk |
| Non-participation (failed to produce or attest threshold) | 0.05 (5%) | No malicious intent required; cheap signal to eject persistently-offline operators |

The HOCON keys go under `nakamoto.sharding.slashing.{...}` (typed
`ShardSlashingConfig` with three `Ratio` fields).

---

## §11 Crypto primitives

### §11.1 v1 — per-signer Ed25519 + KES product

`CommitteeMemberSignature` (§3.1) carries three predicates per signer:

1. **VRF membership proof**: produced by `CommitteeSortition.isInCommittee`
   (`CommitteeSortition.scala:51-58`), verified by
   `CommitteeSortition.verifyMembership` (lines 63-71). Already implemented.
2. **Ed25519 long-term signature**: over `Hasher[F](ShardCheckpointSigPreimage)`.
   Uses the existing operator long-term key.
3. **KES product signature**: same payload, signed with the operator's KES tree
   at the current period. Verified via the shared `KesRegistry`
   (`KesRegistry.scala`) per `[[project-211-kes-wire-step-landed]]`'s
   pattern — `senderTreeStep` carried on the wire, non-interactive verify.

Verification of an envelope's `committeeSignatures` is a fold over the list,
running all three predicates per entry. Total cost per envelope: `K_S * (1 VRF
verify + 1 Ed25519 verify + 1 KES verify)`. At `K_S = 4` per shard and `S = 4`
shards per gl0 ord, total per-ord cost is `16 * (3 verifies) = 48 verifies` —
modest.

### §11.2 v2 — BLS aggregate signatures

When BouncyCastle 1.85 lands (separate research workstream — single line bump
in `project/Dependencies.scala`), the wire shape becomes:

```scala
final case class CommitteeAggregateSignature(
  vrfProofs: List[CommitteeMemberVrfProof],   // one per member, kept individually for membership verification
  blsAggregate: Hex,                          // single aggregate sig over checkpointHash, verified once
  signers: NonEmptyList[PeerId]               // identifies which operator keys contributed
)
```

Verification: 1 BLS verify against an aggregated public key (multiplication of
all signer VKs). Per-envelope cost shrinks to `K_S verifies (VRF) + 1 verify
(BLS)` — strictly cheaper at scale.

Per `[[feedback-greenfield-no-wire-compat]]`: we do NOT pre-bake a "BLS-or-individual"
capability flag. When BLS lands, we replace `CommitteeMemberSignature` with
`CommitteeAggregateSignature` in the schema, rebuild, redeploy.

### §11.3 Fraud-proof envelope schema reserved in v1

§3.2's `FraudProofEnvelope` ships in v1 binary. The `reexecutionWitness:
Array[Byte]` field is a tagged-union container; in v1 it deserializes to a
sealed trait whose concrete cases are added in v2 alongside the dispute
handler. For v1 the field carries an empty `Array.empty[Byte]` (no challenger
construction); the case class is in the schema so the wire bytes don't shift
between v1 and v2.

If the v2 design requires *radically* different witness contents (large
enough that just versioning the inner trait won't suffice), greenfield rules
allow rewriting the envelope at v2 deploy time without compat ceremony.

---

## §12 Code removal

The following code is deleted as part of v1:

### §12.1 `MetagraphCommitteeGate` (current)

- File: `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/MetagraphCommitteeGate.scala`
  (363 LOC total — the trait declaration is lines 23-99; companion at 101-…)
- Call sites:
  - `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala` — gate is invoked on every received `pb.MetagraphBinary` and on every received `pb.MetagraphAttestation`.
  - `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala` — gate is constructed and wired into the daemon.
- Replacement: the per-shard committee runs `CommitteeSortition` per epoch
  (not per binary), and the shard chain's own `FinalityTrigger[F]` instances
  drive checkpoint Phase 2 admission. The per-binary attestation gating is
  *subsumed* by the per-checkpoint envelope signing; a binary is admitted into
  gl0 *by virtue of being inside a Phase-2 checkpoint envelope*, not by
  reaching a per-binary threshold.

### §12.2 `MetagraphOrphanBuffer`

- File: `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/MetagraphOrphanBuffer.scala` (~150 LOC)
- Call sites: `NakamotoSyncDaemon` (drain on admission), `MetagraphParentOrdinalResolver` wrapper.
- Replacement: the shard chain handles the parent-hash chain-link of
  consecutive ml0 binaries inside the committee. Out-of-order binaries
  delivered to a shard member buffer in the shard chain's natural fork DAG
  store and unwind when the parent arrives, exactly like Taktikos tine
  selection on gl0 today. No separate orphan-buffer abstraction needed.

### §12.3 `stateChannelSnapshots` field on `GlobalIncrementalSnapshot`

- File: `modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalIncrementalSnapshot.scala:108`.
- The field is removed from the *new* `GlobalIncrementalSnapshot` case class
  (line 102-127). The legacy `GlobalIncrementalSnapshotV1` case class (lines
  41-99) keeps the field for backwards-deserialization of older chain-store
  files; the V1→new conversion in `GlobalIncrementalSnapshotV1.toGlobalIncrementalSnapshot`
  (lines 57-80) is updated to map the legacy `stateChannelSnapshots` into a
  synthetic single-shard `shardCheckpoints[ShardId.zero]` for replay
  determinism (or, simpler: the V1 conversion produces an empty
  `shardCheckpoints` map and treats the V1 snapshot as pre-sharding-genesis;
  decision held for the implementation slice).
- All read sites of `stateChannelSnapshots` move to read from
  `shardCheckpoints[shardId].derivedStateDelta.includedSnapshots[address]`
  via a typed accessor on the new schema.

### §12.4 Per-binary admission path in `GlobalSnapshotStateChannelAcceptanceManager`

- File: `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala` (263 LOC)
- The entire per-binary chain-link admission (lines 50-220) is deleted from
  gl0's accept path. The trait `GlobalSnapshotStateChannelAcceptanceManager`
  remains as a *shard-internal* helper (the shard committee uses the same
  chain-link logic to build `derivedStateDelta.includedSnapshots`), but gl0
  no longer calls it during `accept()` — gl0 reads the already-chain-linked
  list from the accepted shard checkpoint.

### §12.5 GSAM rewiring

`GlobalSnapshotAcceptanceManager.accept()` is restructured to consume
`shardCheckpoints` from the inputs and stitch them. The 1095-LOC
`accept()` body (`GlobalSnapshotAcceptanceManager.scala:977-2072`)
divides cleanly into:

- *Stays at gl0 (Tier 3)*: steps #1, #2, #3, #4, #5, #6, #7-#12 (DAG-layer),
  #19-#20 (txrefs), #23-#24 (rewards), #26 (pricing), #29-#41 (TokenLock and
  AllowSpend DAG-layer), #44-#47 (DAG balance updates), #49-#62 (cleanup,
  rewards, NIPoPoW boundary, MPT writer). See [`SHARDABILITY-MAP.md`](./SHARDABILITY-MAP.md)
  §4 for the full classification.
- *Moves to shard committee (Tier 1)*: steps #14, #15 (per-MG materialization
  → produced by shard committee), #16-#18 (state channel processing → shard
  produces this), #21 (currencyBalances extraction → shard produces this),
  #22 (artifact extraction → shard produces this), #42-#43 (token-lock
  balances per MG → shard produces this), #52 (`MetagraphSyncManager` per-MG
  slice → shard produces this).
- *Replaced by cross-shard receipts (Tier 2)*: step #25
  (`SpendActionValidator` cross-currency lookup → §8), step #52 cross-MG
  slice (`updateFromSpendActions` → §8.4).
- *Replaced by stitching*: step #48 (Merkle tree over MGs → per-MG subtree
  roots come from `derivedStateDelta.perMetagraphMptRoots`; gl0 stitches
  into a global tree of trees, see [`SHARDABILITY-MAP.md`](./SHARDABILITY-MAP.md)
  §3.7).

The post-refactor `accept()` body is materially smaller — initial estimate
~500-700 LOC vs today's 1095 — and the per-MG complexity moves out of the
hot path.

---

## §13 Implementation slices

Each slice is independently implementable and independently testable. Dependencies
are explicit; arrows show "depends on" (later slice ⇒ earlier slice).

| # | Slice | LOC est. | Key files (new / modified) | Depends on |
|---|---|---|---|---|
| **1** | **`schema.sharding.{ShardId, ShardOrdinal, ShardDerivedStateDelta, CrossShardReceipt, ShardCheckpoint, CommitteeMemberSignature, FraudProofEnvelope}`** | ~250 LOC + tests | new package `modules/shared/src/main/scala/io/constellationnetwork/schema/sharding/` | — (independent) |
| **2** | **`NakamotoConfig.sharding: ShardingConfig` + HOCON keys** | ~60 LOC | `application.conf:281+`, `config/types.scala:85-89` | — (independent) |
| **3** | **`shardIdFor(metagraphAddress, M): F[ShardId]`** | ~30 LOC + tests | `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/ShardAssignment.scala` (new) | 1, 2 |
| **4** | **`GlobalIncrementalSnapshot.shardCheckpoints` field + V1 conversion** | ~80 LOC + tests | `GlobalIncrementalSnapshot.scala:101-127`, `GlobalIncrementalSnapshotV1` conversion at line 56-80 | 1 |
| **5** | **Per-shard `ShardChainStore[F]` (analog of `NakamotoChainStore`, scoped per shard)** | ~250 LOC + tests | new `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/sharding/ShardChainStore.scala` | 1, 3 |
| **6** | **Per-shard `ShardTipTracker[F]` + per-shard `FinalityTrigger[F]` (`T_count_shard`, `T_depth1_shard`)** | ~200 LOC + tests | new `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/sharding/ShardFinalityTriggers.scala` reusing `FinalityTrigger.fromRef[F]` (`FinalityTrigger.scala:124-144`) | 1, 5 |
| **7** | **Per-shard slot-leader VRF using `EligibilityChecker.checkEligibility` scoped to committee + `shardEta` domain separation (§5.6)** | ~150 LOC + tests | new `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/sharding/ShardSlotLeader.scala`; reuses `EligibilityChecker.scala:57-74` and `EpochStateManager` for `shardEta` | 1, 3 |
| **8** | **`ShardCheckpointProducer[F]` (assembles `ShardDerivedStateDelta` by per-MG re-execution; signs; gossips)** | ~400 LOC + tests | new `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointProducer.scala`; reuses `GlobalSnapshotStateChannelEventsProcessor`, `TokenLockStateManager`, `MetagraphSyncManager` for the per-MG slices | 1, 3, 4, 5, 6, 7 |
| **9** | **`ShardCheckpointGl0AcceptanceManager[F]` (differentiated check per §7.3, MPT write)** | ~300 LOC + tests | new `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/ShardCheckpointGl0AcceptanceManager.scala`; replaces parts of `GlobalSnapshotAcceptanceManager.scala:1258-1272` per §12.5 | 1, 4, 8 |
| **10** | **`ShardSubtreeProofService[F]` (proof generation per §8.5) + HTTP route under `/shard/{shardId}/proof`** | ~250 LOC + tests | new `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/sharding/ShardSubtreeProofService.scala`; reuses `HistoricalMptProofService.scala:23-29` | 1, 8 |
| **11** | **Cross-shard read integration in `SpendActionValidator`** | ~150 LOC + tests | modifies `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/SpendActionValidator.scala` (currently 278 LOC); adds an injected `ShardSubtreeProofClient[F]` parameter | 10 |
| **12** | **`CrossShardReceipt` consumer in gl0 leader (§8.4 `MetagraphSyncDataWrite` application)** | ~120 LOC + tests | modifies `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/MetagraphSyncManager.scala` (148 LOC); the cross-MG `updateFromSpendActions` (lines 103-129) replaced by consume-receipts loop | 1, 9 |
| **13** | **GSAM `accept()` refactor — stitching from `shardCheckpoints`, removing per-binary admission inline** | ~400 LOC (net delete, per §12.5) | modifies `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala` (currently 2076 LOC) | 9, 11, 12 |
| **14** | **Sidecar gossip wire format `pb.ShardCheckpoint` + `pb.ShardCheckpointAttestation` + per-shard topic** | ~200 LOC + tests | modifies `.proto` files + `node-shared` sidecar bindings (rough analog of `pb.MetagraphAttestation`); subscribe/publish wire in `NakamotoSyncDaemon` | 1, 8 |
| **15** | **Removal of `MetagraphCommitteeGate` + `MetagraphOrphanBuffer` + call sites** | ~-500 LOC (net delete) | deletes `MetagraphCommitteeGate.scala`, `MetagraphOrphanBuffer.scala`; rewires `NakamotoSyncDaemon`, `GlobalSnapshotConsensus`, `MetagraphParentOrdinalResolver` | 13, 14 |
| **16** | **Equivocation slashing for `(shardId, parentCheckpointHash)` (§10.1)** | ~180 LOC + tests | new `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/ShardCheckpointEquivocationEvidence.scala`; new validator alongside `SlashableEvidenceValidator` | 1, 13 |
| **17** | **Non-participation accumulator + epoch-boundary slashing (§10.3)** | ~250 LOC + tests | new system-namespaced MPT partition (new `SystemNamespaceLabel`); new boundary-write hook in gl0 accept | 1, 13 |
| **18** | **`FraudProofEnvelope` schema wire-reservation only (no handler) — confirm encoder/decoder land in v1 binary** | ~80 LOC + tests | included in slice 1 schema package; just a wire test confirming round-trip | 1 |
| **19** | **HOCON `T_partition_hard` warning emit + Prometheus counters for shard-level metrics** | ~100 LOC | adds metrics to `dag_nakamoto_shard_*` namespace; warning emit in `GlobalSnapshotConsensus` | 9, 14 |
| **20** | **e2e validation at `8 gl0 + 4 mg + 4 shards` topology** | n/a (test code) | new `test-scripts/test/sharding/`; reuses existing `just test` infrastructure with `--shards=4 --num-mg=4` extension to `set-env.sh` per `[[feedback-extend-args-not-recipes]]` | All previous slices |

**Critical path: 1 → 4 → 5 → 6 → 7 → 8 → 9 → 13 → 14 → 15 → 20.** That's the
spine; slashing (16, 17), proof service (10), cross-shard validation (11, 12)
can fan out in parallel once their predecessors land.

**Parallelism opportunities:**
- 1 and 2 are wholly independent — start in parallel.
- 4, 6, 7, 10 all depend only on 1 (and partly on others). Multiple agents can
  work them in parallel after 1 lands.
- 16, 17, 18 depend only on 1 (or 1 + 13). 16 and 17 are good "back-pressure"
  slices to dispatch in parallel with the spine.

---

## §14 Test plan

### §14.1 Unit tests

| Component | Coverage |
|---|---|
| `schema.sharding` codecs (slice 1) | Circe round-trip per type, including `Option`-typed and `NonEmptyList`-typed fields; arbitrary generators wired so property tests can compose them |
| `shardIdFor` (slice 3) | Determinism across runs (same address → same shardId); uniform distribution over `M` for a large random address sample; `M = 1` collapses to single shard |
| `ShardChainStore` (slice 5) | Forks DAG insertion, parent-child walk, `bestTip` selection under multi-tip / single-tip; bounded retention; reorg-safe ops |
| `ShardFinalityTriggers` (slice 6) | `T_count_shard` qualifies at ⌈2 K_S / 3⌉ attesters; `T_depth1_shard` qualifies at `depth-k1-shard`; monotonicity (Ref never goes backwards) |
| `ShardSlotLeader` (slice 7) | VRF eligibility under `K_S = 4`, σ = 1/4 each, slot range 1-100 — expected leader count matches LDD-derived rate within 95% CI; `shardEta` domain separation (two shards' `shardEta` differ even at the same gl0 `eta`) |
| `ShardCheckpointProducer` (slice 8) | Per-MG derivations match the unsharded `GlobalSnapshotAcceptanceManager.accept()` byte-equivalent for the same MG inputs; signing produces verifiable envelopes |
| `ShardCheckpointGl0AcceptanceManager` (slice 9) | `T_count_shard` path admits valid envelopes, rejects envelopes with bad signatures; `T_depth1_shard` path re-executes and admits when matches, rejects (and slashes) when mismatches |
| `ShardSubtreeProofService` (slice 10) | Proof round-trip: generate, verify against per-MG subtree root, value matches |
| `SpendActionValidator` with cross-shard (slice 11) | Cross-MG SpendAction validates correctly when proof is supplied; rejects when proof is invalid; same-shard SpendAction bypasses the proof path |
| `CrossShardReceipt` consumer (slice 12) | `MetagraphSyncDataWrite` applied to target MG; idempotent re-application is a no-op |
| Slashing validators (slice 16, 17) | Same shapes as `SlashableEvidenceValidator` test patterns; equivocation pair triggers slash; non-equivocating signatures do not |

### §14.2 Integration tests

| Scenario | Validates |
|---|---|
| Single gl0 + 2 shards + 2 MGs, single committee member per shard, no traffic | Liveness pings fire every `t-alive-ms`; gl0 admits the empty checkpoints; no `dag_nakamoto_shard_partition_total` increments |
| Single gl0 + 2 shards + 2 MGs, single committee member per shard, light traffic | `T_count_shard` fires on every checkpoint; gl0 admits via signature-only path; CPU on gl0 leader reduces vs unsharded baseline |
| Single gl0 + 2 shards + 2 MGs, single committee member per shard, committee member offline for 60s | `T_depth1_shard` fires at depth 8; gl0 admits via re-execute path; no slashing fires |
| Single gl0 + 2 shards + 2 MGs, cross-MG `SpendAction` between shards | Shard X correctly fetches AllowSpend from shard Y via P2P + proof; validation succeeds; balance updates reflect in next gl0 ord |
| Single gl0 + 2 shards + 2 MGs, induced equivocation (two checkpoints with same `(shardId, parentCheckpointHash)`, different child hashes, signed by same key) | Equivocation evidence is constructed and validated; ledger effect applies; signer stake = 0 post-evidence |

### §14.3 e2e test (slice 20)

The target topology is `8 gl0 + 4 mg + 4 shards`. The reference workload is
the existing `just test` battery as documented in
`[[project-iter-v23-full-pass-8gl0-4mg-4shards]]`:

| Workflow | Expectation |
|---|---|
| `dag-cluster` sanity | PASS — sharding is transparent at the DAG layer |
| `currency-transactions` | PASS — per-MG flow now routes via shard committee |
| `token-locks` + `token-locks-expiration` | PASS — `TokenLockBalances` MPT writes routed via shard committee output |
| `allow-spends-and-spend-transactions.js` (the Tier 2 motivator) | PASS — cross-shard `SpendAction` flows through Option I proof path |
| `delegated-staking` | PASS — Tier 3, unaffected |
| `metagraph-rewards` | PASS — Tier 3, unaffected |
| `multi-metagraph-sanity` | PASS — establishes per-shard isolation works in practice |
| Fork recovery (induced gl0 reorg at low ord) | PASS where `[[project-259-root-cause-metagraph-fork-2026-05-22]]` failed — checkpoint envelope structurally precludes the orphan-on-reorg race |

The new e2e args (per `[[feedback-extend-args-not-recipes]]`):
- `--shards=N` → sets `NAKAMOTO_NUM_SHARDS=N` (also propagated via HOCON
  override) — does not become a new `just` recipe.

### §14.4 Determinism / regression fixture tests

Per `[[project-test-vector-pattern]]` Tier 1: a `derivedStateDelta` fixture
library. Each fixture is a pre-computed input (per-MG prior state + new
binaries) and the expected `derivedStateDelta` bytes. Tests verify that
`ShardCheckpointProducer` produces the same bytes from the input. Regression
catch for any change to a per-MG derivation that would break cross-cluster
byte equivalence.

### §14.5 Performance benchmark (advisory, post-e2e)

Baseline: e2e at `8 gl0 + 4 mg + 0 shards` (unsharded). Measure gl0 leader
`accept()` CPU per ord.

Post-sharding: e2e at `8 gl0 + 4 mg + 4 shards`. Measure same.

Expected: per-MG re-execution CPU drops to ~1/`(N/K_S)` per gl0 op (only
committee members do it), at the cost of signature verification. At `K_S = 4`
and `N = 8`, expected gl0-op-level CPU reduction is ~50%. The full target
(`K_S/N → 0` for large N) requires N ≫ K_S, not tested in this topology.

---

## §15 Open questions

Flagged for human review. None are blockers for landing the spine; they are
edge-case + ergonomics calls that the critic should validate.

### §15.1 Committee size for the `8 gl0 + 4 shards` topology

With `N = 8` operators and `S = 4` shards, the natural committee partition is
`K_S = 2` per shard (each operator in 1 shard) or `K_S = 4` per shard (each
operator in 2 shards on average — fits CommitteeSortition's stochastic
sampling). The default in §4.2 (`committee-k-target = 4`) assumes the latter
(redundant membership). Should we instead choose `K_S = 2` for the e2e
topology and accept the lower honest-majority margin (Chernoff per
[`COMMITTEE-SORTITION-DESIGN.md`](./COMMITTEE-SORTITION-DESIGN.md) §6 gives
6×10⁻² at K=50 — at K=4 the margin is significantly weaker, but e2e isn't
adversarial)?

Answer needed before slice 7 lands. **Suggested**: default `K_target = N/S
rounded up` for the topology, leave HOCON override for production where the
operator/shard count goes up dramatically.

### §15.2 What happens to `shardId` mapping when `M` (numShards) changes?

If an operator restarts with a different `NAKAMOTO_NUM_SHARDS`, the
deterministic mapping changes. All metagraphs may move to different shards;
the on-disk MPT state (per-MG entries keyed by metagraph address) is
unaffected, but the in-memory shard chain state is.

**Suggested**: enforce that all gl0 ops in a cluster boot with the same
`numShards`. Discrepancy causes a fail-fast at startup (config sanity check
against a cluster-wide config-hash in the genesis). v1 doesn't ship the
fail-fast; the operator runbook should warn that mid-cluster `numShards`
changes require a coordinated restart (greenfield rule — no online
upgrade ceremony).

### §15.3 `shardEta` derivation — domain-separation tag value

The proposal uses `"shard"` as the tag (§5.6). Should it be more specific
(e.g., `"shard-leader"` to leave room for `"shard-committee"` or
`"shard-attestation"` if we add more per-shard VRF uses later)? Currently the
committee membership VRF reuses `CommitteeSortition`'s `"committee"` tag,
which would collide if we naively reused it for the shard slot leader VRF.

**Suggested**: use distinct tags. `"shard-leader"` for the per-shard slot leader
VRF; `"committee"` (existing) stays for committee-membership. Decided in
slice 7.

### §15.4 `gl0AnchorOrdinal` admission window — bound on the lookahead

§7.2 says gl0 admits a checkpoint with `gl0AnchorOrdinal ≤ N` at ord N.
Should there be a *lower* bound — i.e., reject checkpoints whose
`gl0AnchorOrdinal` is too old? E.g., if a shard produces a checkpoint at gl0
ord 100 but for some reason it doesn't make it into a gl0 snapshot until gl0
ord 110, is the checkpoint still valid?

**Suggested**: yes, valid up to `N - gl0AnchorOrdinal ≤ depth-k1-shard` (the
same window the shard chain uses for its own finality). Older checkpoints
must be re-issued by the committee (the shard chain has already advanced
past that ancestor). Decided in slice 9.

### §15.5 Empty checkpoint inclusion semantics

§3.4 says gl0 omits a shard from `shardCheckpoints` if the shard produced no
Phase-2 envelope. §6.1 says empty checkpoints (from `T_alive` liveness ping)
are valid but skipped from gl0 inclusion. **Are empty checkpoints ever
included in the gl0 snapshot?**

**Suggested**: NO for v1 — empty checkpoints are a within-shard liveness
signal, observed by other shard members (gossiped, used to confirm "shard
member X is still alive even if no SC traffic"). gl0's inclusion is binary
on "did this shard advance state?". Decision recorded in §3.4.

### §15.6 Late-binding shard chain bootstrap

A gl0 op joining mid-epoch needs to fetch the current state of each shard
chain it is committee member of (to participate). Today the equivalent for
gl0 itself is the chain-sync protocol ([`SYNC-PROTOCOL.md`](./SYNC-PROTOCOL.md)).
The shard chains need an analogous per-shard sync — at minimum the depth-k1-shard
recent envelopes from any honest shard member. **Spec out of scope for this doc**;
flagged for the implementation slice 8 doc.

### §15.7 Burst-trigger and shard chain depth interaction

[`SHARD-CHECKPOINT-GRANULARITY.md`](./SHARD-CHECKPOINT-GRANULARITY.md) §4.5
says under T_burst the shard publishes 2-3 checkpoints per gl0 ord. The
shard's own `depth-k1-shard` is measured in shard ords, so multiple
checkpoints per gl0 ord push the depth-k1 watermark forward inside the
gl0-ord window. **Does the T_depth1_shard gate fire correctly under burst?**

**Suggested**: yes — the shard chain's depth grows linearly in shard ords,
regardless of gl0's cadence. A burst that produces 3 shard ords inside one
gl0 ord just means the `T_depth1_shard` fires sooner in wall-clock time for
that burst's predecessor. **Decided in slice 6 design.**

### §15.8 Fee-deduction determinism across shards ([`SHARDABILITY-MAP.md`](./SHARDABILITY-MAP.md) §3.5)

Per [`SHARDABILITY-MAP.md`](./SHARDABILITY-MAP.md) open question §6.2:
ml0 fee deductions touch DAG-global balances (owner addresses). Under
sharding, each shard's committee independently produces "MG M deducted F
from owner_M" deltas. If two shards' MGs share an owner address (rare in
production but not type-enforced), the deltas conflict and the underflow
check in `GlobalSnapshotStateChannelEventsProcessor.scala:349` becomes
ordering-sensitive.

**Suggested**: gl0 canonicalises the fee-deduction order at the global-pass
stitching step (apply deltas in `(shardId, metagraphAddress)` sorted order).
Documented in §12.5 GSAM rewiring; precise location in the new
`ShardCheckpointGl0AcceptanceManager` slice 9 implementation.

### §15.9 Validation of `getGlobalSnapshotByOrdinal` callback under sharding

Per [`SHARDABILITY-MAP.md`](./SHARDABILITY-MAP.md) open question §6.1: the
`CurrencySnapshotContextFunctions.createContext` callback (passed at
`GlobalSnapshotStateChannelEventsProcessor.scala:122`) lets cl1 validators
reach back into global state. Under sharding, the shard committee runs this
callback — but does it have access to the full global state, or only its
shard's slice?

**Suggested**: full access (the shard committee is still a gl0 op — it has
the gl0 MPT). The callback is a *read* of global state, which is exactly
the same global MPT every op holds. The shard-vs-non-shard distinction is
about who *writes* the per-MG state, not who *reads* the global. Confirmed
during slice 8 implementation by tracing every consumer of the callback
inside cl1 — flagged for the slice 8 design as a confirmation pass, not a
spec change.

### §15.10 Empty-shard / single-MG shard cost amortisation

At test scale (`8 gl0 + 4 mg + 4 shards`, `M = S`, each shard has exactly
one MG), the per-shard committee overhead may dominate the per-MG re-execution
saving — each MG now has its own committee mini-chain with its own slot leader
VRF, attestations, etc. **Is the sharding payoff still net positive at this
topology?**

**Suggested**: probably yes (the structural fix for orphan-on-reorg is the
load-bearing benefit; the CPU saving is secondary at this scale). But this is
the open-question that the e2e benchmark in §14.5 will measure empirically.
Not blocking for the spec.

---

**Not implemented; awaiting review.**
