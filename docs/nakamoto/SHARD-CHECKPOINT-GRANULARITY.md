# Shard checkpoint granularity — recommendation

> **HISTORICAL, SUPERSEDED RECOMMENDATION.** This document assumes an abandoned
> shard topology and proof/adoption model; it does not describe the current v1
> assignment, committee lifecycle, or GL0 economic-validity gate. Do not use it
> to drive implementation. It is retained only as decision history; see
> [`../../AGENTS.md`](../../AGENTS.md), ADR-0016, and ADR-0017.

**Status:** research recommendation. No code commitment. Written 2026-05-22
against `feature/serde-typeclass-shim`.
**Scope:** how often a shard committee should publish a "shard sub-snapshot"
(checkpoint aggregating per-metagraph state derivations) for inclusion in gl0,
given that metagraph (ml0) snapshots produce on a different cadence than gl0.

Cross-references:
- [`CROSS-SHARD-MITIGATION-PROPOSAL.md`](./CROSS-SHARD-MITIGATION-PROPOSAL.md)
  §2.4 — Option A locks `m=1`, `S=M` (one shard per metagraph) for v1; this
  doc assumes that baseline and asks the granularity question for v2 when
  one shard maps to multiple metagraphs (`m=1`, `S<M`).
- [`attestation-and-finality.md`](./attestation-and-finality.md) §0.2 —
  `T_count` / `T_weight` / `T_depth1` / `T_depth2` trigger stack at gl0.
- [`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) §2 — per-shard NIPoPoW
  tower is sub-snapshot-anchored; the granularity model determines what a
  tower entry's underlying unit is.
- [`modules/shared/.../schema/GlobalIncrementalSnapshot.scala`](../../modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalIncrementalSnapshot.scala)
  line 108 — current `stateChannelSnapshots: SortedMap[Address,
  NonEmptyList[Signed[StateChannelSnapshotBinary]]]` is the existing
  variable-list-per-metagraph precedent we extend.
- [`modules/node-shared/.../GlobalSnapshotStateChannelAcceptanceManager.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala)
  lines 50-201 — the existing chain-link discipline (`lastSnapshotHash`),
  first-sight registry, and `selectStateChannels` unfold pattern that the
  recommended checkpoint scheme inherits.

---

## §0 TL;DR

**Recommendation: Option C (committee-driven variable-content checkpoints),
operated in the per-gl0-ord cadence mode by default.** The checkpoint is a
"transaction-like" envelope: a signed list of however many ml0 binaries the
shard committee has gossiped between consecutive checkpoint emissions, plus
a small metadata header (`shardId`, `seq`, `lastCheckpointHash`,
`coversFromOrd / coversToOrd` per metagraph). The default emission cadence is
"one checkpoint per shard per gl0 ord" (Option B's cadence), but the schema
permits emitting earlier on burst load or skipping when nothing has changed.

This subsumes Options A and B as endpoints of the same envelope:
- **Option A behaviour** is recovered by reducing the cooldown to zero and
  setting the per-binary cap to 1 (one binary per checkpoint).
- **Option B behaviour** is recovered by binding emission to the gl0 slot
  tick.

Justification: the chain-link discipline at
`GlobalSnapshotStateChannelAcceptanceManager` already requires gl0 to admit
a *list* of binaries per metagraph per ord and unfold a chain from
`priorLastStateChannelSnapshotHashes[address]`. The recommended checkpoint
schema layers a per-shard signed envelope around that list — preserving
the existing chain-link admission code path while collapsing per-shard
gossip and slashing into a single signed unit. The "transaction-like"
framing matches gl0's existing event-trigger / time-trigger composition
(`GlobalSnapshotEventsPublisherDaemon`) and inherits its bounded-volume
property.

---

## §1 Cadence facts

### 1.1 gl0 snapshot cadence

**~7s per ordinal in production.** Verified from code:

- `modules/dag-l0/.../snapshot/nakamoto/SnapshotLeaderLoop.scala:454-457`:
  `slotDurationMs = sys.env.get("NAKAMOTO_SLOT_DURATION_MS")...getOrElse(1000L)`
  — 1s slot tick. Tests can override to 500ms (`set-env.sh --fast-slots`
  recipe).
- `modules/dag-l0/.../snapshot/nakamoto/SnapshotLeaderLoop.scala:593` (comment):
  "With LDD targeting ~15% slot fill, slots run ~6× sparser than 1s" —
  i.e., ~6-7s expected ordinal cadence with LDD snowplow at the production
  `f` parameter.
- `modules/node-shared/src/main/resources/application.conf:325` (comment):
  "buffer at 8 events/ord × 7s/ord" — operational reference for the 7s
  cadence assumption.
- `modules/node-shared/src/main/resources/application.conf:290`:
  `eta-rotation-snapshots = 2550` (≈ 5h at 7s, matches the "~6h at 7s"
  language in `CROSS-SHARD-MITIGATION-PROPOSAL.md` §2.4 within rounding).

The cadence is **probabilistic**, not fixed: an LDD snowplow with `f ≈
0.15` means each 1s slot has independent eligibility per validator, with
the per-slot-with-any-leader probability calibrated so the expected
inter-block time is ~7s. The empirical p50 hovers near 7s; the tail
(`p99`) is several multiples of that.

### 1.2 Metagraph (ml0) snapshot cadence

ml0 currency consensus uses **BFT facilitator rounds**, not Nakamoto LDD.
Verified from code:

- `modules/currency-l0/src/main/resources/currency-l0.conf:18-44`:
  ```
  snapshot { consensus {
    time-trigger-interval  = 43 seconds
    event-trigger-threshold = 1
    event-trigger-cooldown  = 5s
  } }
  ```
  ml0 fires consensus on **whichever comes first**: ≥1 event in the
  mempool + 5s cooldown (`event-trigger`), or 43s wall-clock
  (`time-trigger`). Trigger semantics lives at
  `modules/dag-l0/.../GlobalSnapshotEventsPublisherDaemon.scala:51-118`
  and `modules/currency-l0/.../CurrencySnapshotEventsPublisherDaemon.scala:39-77`.
- `modules/node-shared/.../config/types.scala:208-230`:
  `ConsensusConfig.timeTriggerInterval` / `eventTriggerCooldown` / etc are
  the typed surface.

**Operational ml0 cadence:**
- **Busy metagraph** (≥1 tx/round): ~5s per snapshot (driven by
  `event-trigger-cooldown`).
- **Idle metagraph** (no traffic): ~43s per snapshot (driven by
  `time-trigger-interval`).
- **Burst metagraph**: not strictly < 5s — cooldown is enforced, so a
  spike of N txs in 100ms still produces one snapshot 5s later, not N
  snapshots.

### 1.3 The cadence ratio

| Scenario | ml0 period | gl0 period | Ratio (ml0 snaps per gl0 ord) |
|---|---|---|---|
| Idle metagraph | ~43s | ~7s | **1 ml0 / 6 gl0 ords** (sparse) |
| Active metagraph | ~5s | ~7s | **~1.4 ml0 per gl0 ord** (≈parity) |
| Burst metagraph | ~5s floor | ~7s | **1.4 ml0 per gl0 ord** (cooldown-capped) |

**Key observation:** the assertion in the task statement ("metagraph
snapshots produce faster than gl0") is true on **active** metagraphs
(~5s < ~7s, by ~1.4×) but **false** on idle ones (~43s ≫ ~7s, by ~6×).
The granularity model needs to handle both: a checkpoint scheme that
assumes "ml0 is always faster" will starve idle-shard slashing surface
and produce spurious checkpoints; one that assumes "ml0 is always
slower" will not exploit the active-shard latency win.

### 1.4 Per-shard aggregation expectation

Per `CROSS-SHARD-MITIGATION-PROPOSAL.md` §2.4, v1 locks `S = M` (one shard
per metagraph). The cadence-mismatch problem is most acute at v2 when one
shard aggregates many metagraphs. At the proposed production scale
"10K-100K operators × 1K-3K metagraphs" with N/S = 10-100 operators/shard,
the natural shard-to-metagraph ratio at v2 is ~10-30 metagraphs/shard
(target: keep N/S above the GKL §5.2 honest-majority floor of N/S ≥ 12 at
the chosen `α_global ≈ 0.33` bound; the doc lands on N/S = 10-100).

At 10 metagraphs/shard, **active-metagraph aggregate**: ~10 / 5s = 2 ml0
snapshots/second across the shard's serviced metagraphs, vs gl0's ~1
ord / 7s = 0.14 ords/second. So in the worst case, a shard sees **~14
ml0 snapshots per gl0 ord** if every assigned metagraph is busy. At 30
metagraphs/shard the worst case is ~42 ml0 per gl0 ord.

This is the volume that drives the per-ord inclusion field sizing.

---

## §2 Option mechanics

### 2.1 Schema framing — existing precedent

gl0 already includes a variable-size per-metagraph list per ord:

```scala
// modules/shared/.../schema/GlobalIncrementalSnapshot.scala:108
stateChannelSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
```

And it already chain-links them via `lastSnapshotHash` at acceptance
(`GlobalSnapshotStateChannelAcceptanceManager.onlyPossibleReferences`,
lines 191-202). All three options below are evolutions of this field;
the question is what's the per-shard signed envelope around the list and
when does the envelope get emitted.

### 2.2 Option A — Per-metagraph-snapshot checkpoint (independent cadence)

**Mechanics:**

- Every time a metagraph in shard `s` produces a new ml0 snapshot, the
  shard committee (via Snowball / its existing BFT round) signs a
  one-binary checkpoint and gossips it to gl0.
- gl0 includes all checkpoints received between consecutive gl0 ords.
- Cadence is **metagraph-driven**: shard committee fires whenever any
  member metagraph produces.

**Schema sketch** (orientation only — full schema is out of scope):
```
ShardCheckpoint_A := {
  shardId            : ShardId,
  seq                : Long,                    // monotonic per-shard
  metagraphAddress   : Address,                 // the single mg this binary covers
  lastSnapshotHash   : Hash,                    // chain-link in the mg's binary chain
  binary             : Signed[StateChannelSnapshotBinary],
  committeeSignature : NonEmptySet[SignatureProof]  // shard quorum
}
```

**gl0 inclusion field:**
```
shardCheckpoints: SortedMap[ShardId, NonEmptyList[Signed[ShardCheckpoint_A]]]
```
Variable-length list per shard per ord, mirroring the existing per-Address
list.

**Inclusion behaviour:** every shard contributes 0-to-many entries per
ord. Worst-case under §1.4 active load (S = 10 mg, all active): 14
entries per shard per ord.

### 2.3 Option B — Per-gl0-ord checkpoint (locked cadence)

**Mechanics:**

- Each shard publishes **exactly one** checkpoint per gl0 ord (or zero
  if it has no progress). The checkpoint contains **all** ml0 binaries
  the shard's assigned metagraphs produced in that window.
- The shard committee waits for the gl0 slot boundary, then assembles
  the bundle and signs it.
- Cadence is **gl0-driven**: shard committee fires at the same tick
  rhythm as gl0 ordinal advance.

**Schema sketch:**
```
ShardCheckpoint_B := {
  shardId            : ShardId,
  coversGlobalOrd    : SnapshotOrdinal,         // the gl0 ord this checkpoint is for
  lastCheckpointHash : Hash,                    // chain-link in this shard's checkpoint chain
  perMetagraph       : SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
  committeeSignature : NonEmptySet[SignatureProof]
}
```

**gl0 inclusion field:**
```
shardCheckpoints: SortedMap[ShardId, Signed[ShardCheckpoint_B]]   // exactly one per shard
```

**Inclusion behaviour:** at most one entry per shard per ord. Bounded
volume regardless of ml0 burst.

**Latency cost:** an ml0 snapshot produced at gl0-ord N + 0.5 must wait
0.5 × 7s = 3.5s on average for the next gl0 boundary, then 7s for the gl0
round itself — total p50 ~10.5s ml0→gl0 visibility, p99 worse.

### 2.4 Option C — Committee-driven variable-content checkpoints (recommended)

**Mechanics:**

- The shard committee maintains its own checkpoint clock, decoupled from
  both ml0 and gl0 cadence.
- Each checkpoint contains "whatever ml0 binaries have been gossiped to
  the committee since the previous checkpoint" — variable size, like a
  Bitcoin block packaging mempool transactions
  ([Bitcoin mempool reference](https://cointelegraph.com/learn/articles/what-is-the-bitcoin-mempool):
  block packaging is variable-size, capped, fee-prioritised; the same
  pattern transposed to "ml0 binaries since last checkpoint").
- Emission policy is configurable per shard:
  - **Time-trigger** (default): emit every `T_checkpoint` (default `=
    gl0 expected period = 7s`, matching Option B's cadence).
  - **Event-trigger**: emit early if `> T_burst` ml0 binaries have
    accumulated (default `T_burst = 20`, set conservatively above the
    expected 14 binaries/ord at 10mg-per-shard active load).
  - **Empty-skip**: don't emit if no new ml0 binaries since last
    checkpoint AND `< T_alive` time has passed (default `T_alive =
    30s`, so an idle shard sends one liveness ping every ~30s).
- Cadence is **committee-driven**, with the time-trigger default being
  Option B's cadence and the event-trigger covering active-shard bursts.

**Schema sketch:**
```
ShardCheckpoint_C := {
  shardId            : ShardId,
  seq                : Long,                    // monotonic per-shard
  lastCheckpointHash : Hash,                    // per-shard chain-link
  coversToWallClock  : Long,                    // committee's local clock at emission
  perMetagraph       : SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
  // (empty perMetagraph allowed iff `seq > prev.seq + T_alive_in_seqs` — liveness ping)
  committeeSignature : NonEmptySet[SignatureProof]
}
```

**gl0 inclusion field:**
```
shardCheckpoints: SortedMap[ShardId, NonEmptyList[Signed[ShardCheckpoint_C]]]
```
Variable-list-per-shard per ord, like the existing `stateChannelSnapshots`
shape. A gl0 ord typically includes 1 checkpoint per shard; under
burst-trigger, may include 2-3.

**Inclusion behaviour:** bounded by emission policy. Worst case at
default config: 2-3 entries per shard per ord (one normal + 1-2
event-triggered bursts). At 1000-3000 shards × 1.5 avg entries/ord =
~1.5K-4.5K shard-checkpoint entries per gl0 ord. Compare to current
per-Address binary list: at 1000 metagraphs × ~1 binary/ord/mg = ~1000
binary entries/ord — same order of magnitude.

---

## §3 Trade-off matrix

| Criterion | Option A (per-ml0) | Option B (per-gl0) | Option C (committee, default per-gl0 cadence) |
|---|---|---|---|
| **End-to-end latency (active mg)** | **~5s** (ml0 → checkpoint → next gl0 ord = ~5s, ml0 visible at gl0 by next ord) | ~10.5s p50 (~5s ml0 + 3.5s wait + 7s gl0 = up to 15s p99) | ~10.5s default; **~5s with event-trigger burst path** (configurable) |
| **End-to-end latency (idle mg)** | ~50s (43s ml0 + 7s gl0) — but checkpoint fires immediately | ~50s — same | ~50s — same; liveness ping makes shard observable, mg snapshot still on ml0 cadence |
| **gl0 inclusion field size** | **Highest** (≤ N/S × ml0-rate per ord per shard; up to ~14 entries/shard at active load) | **Lowest** (exactly 1 per shard per ord) | Bounded by `T_burst` cap (default 2-3 entries/shard per ord at active load); same order as B |
| **Committee signature rounds** | **Highest** (one per ml0 snapshot, ~per 5s on busy mg) | Lowest (one per gl0 ord, ~per 7s) | Configurable; default = B's rate, burst = up to ~3× B's rate |
| **Reorg robustness** | **High** (each checkpoint is independently committee-signed; if gl0 ord N reorged, the same checkpoints can be re-included in the winning branch) | **High** (checkpoint covers a window, not a gl0 ord directly; re-includable in winning branch as a unit) | **High** (variable contents but `coversToWallClock` not bound to gl0 ord; re-includable) |
| **Liveness — slow shard** | If committee misses a round, the next ml0 snapshot triggers the next attempt — natural catch-up | If committee misses a gl0 ord, next gl0 ord catches up the whole window — but if the shard committee is silent for K gl0 ords without empty-pings, gl0 has no signal | **Best** — empty-pings give an explicit liveness signal even on idle shards |
| **Slashing surface (equivocation key)** | per-(shard, seq, mg-binary) — fine-grained, smallest evidence size | per-(shard, gl0-ord) — coarse-grained; equivocation evidence must cover the bundle | per-(shard, seq) — natural envelope per slashing-evidence pair; matches "two checkpoints with same `(shardId, seq)` signed by quorum" pattern |
| **Empty-checkpoint cost** | N/A (only fire on ml0 production) | Implicit (a zero-binary checkpoint or absent ord) | Explicit liveness ping every `T_alive` seconds — small but non-zero gossip load |
| **Cross-shard receipt timing** | Predictably **fastest** but irregular (driven by other shard's ml0 cadence — varies per shard) | Predictably **slowest** but regular (every gl0 ord, exactly one) | Predictable on the default cadence; burst-aware for active shards |
| **gl0 verification cost per ord** | High (N_shards × ~14 sig-verifies + chain-link checks) | Lowest (N_shards × 1 sig-verify) | Bounded (N_shards × ≤3 sig-verifies); same order as B |
| **Implementation complexity (new code)** | Moderate — checkpoint per ml0 binary is mechanically simple but the inclusion field grows large; committee-signing on every ml0 snapshot adds shard-internal coordination per ml0 | Moderate — committee must align on gl0 boundary detection (clock skew, tick alignment); empty-shard handling needs special case | **Highest** — three trigger modes (time, event, liveness-ping) to implement; emission policy is configurable, more knobs to tune. **Subsumes A and B as endpoint configurations.** |
| **Composes with existing GSAM acceptance** | Tightest — one binary per checkpoint matches existing per-Address chain-link unfold directly | Tight — the per-metagraph map nested inside the checkpoint is one extra unwrap layer | Tight — same as B; the chain-link discipline lives one level down (per-mg-Address inside the checkpoint) |
| **Composes with NIPoPoW per-shard tower** | Tower entry per ml0 snapshot — fine resolution but many tower entries to maintain | Tower entry per gl0 ord — natural cadence, matches existing per-shard tower assumption | Tower entry per checkpoint seq — middle ground; matches the per-shard "epoch in this shard's local clock" framing |

---

## §4 Recommendation

**Option C, operated in per-gl0-ord cadence as default.**

### 4.1 Why C over A and B

**Option A** has the best latency on active metagraphs (~5s ml0-to-gl0
visibility) but pays for it with:
- Highest committee-signing load on the shard (proportional to ml0
  production rate × |mgs in shard| — at v2 worst case, the committee
  signs ~14 checkpoints per gl0 ord per shard).
- Largest gl0 inclusion field, growing with the active-metagraph count.
- Per-snapshot slashing surface, requiring evidence to be constructed per
  ml0 binary (more evidence, smaller individual pieces).

**Option B** has the simplest schema (one checkpoint per shard per ord)
and tightest verification cost, but:
- Worst latency on active metagraphs (~10.5s p50, up to 15s p99 — a 2×
  hit vs A).
- Tight coupling to gl0 cadence: a shard committee that's slow to detect
  the gl0 boundary (clock skew, tick alignment) loses a full window.
- No graceful handling of "this shard has nothing to say but wants to
  signal it's alive" — either a degenerate zero-binary entry or absent
  (silence indistinguishable from partition).

**Option C** keeps B's default cadence (so verification cost and
inclusion-field size stay at B's order of magnitude) and adds two
escape hatches that solve B's specific failures:
- **Event-trigger burst path** recovers A's latency on active shards
  when load justifies it. A shard seeing > `T_burst` ml0 binaries
  accumulate can emit early; for the steady-state case it doesn't, so
  costs match B.
- **Explicit liveness ping** distinguishes "shard alive, no activity"
  from "shard partitioned" — fixing B's silence-ambiguity.

Crucially, **Option C subsumes A and B** as configuration endpoints. If
operational experience shows B's cadence is fine, C with `T_burst = ∞`
and `T_alive = ∞` *is* B. If it shows A's latency is needed everywhere,
C with `T_checkpoint = 0` *is* A. Picking C now is a Pareto-dominant
choice over picking A or B and re-debating later.

### 4.2 How the recommendation handles the cadence mismatch

The cadence mismatch decomposes into three regimes:

1. **Active mg, ml0 faster than gl0 (~5s ml0 < ~7s gl0).** Multiple ml0
   binaries accumulate per gl0 ord. Default C cadence (1 checkpoint per
   gl0 ord) batches these into one checkpoint per shard, matching gl0's
   natural ord rate. **No per-binary committee round; no per-binary
   gl0 entry.** If a shard is consistently bursting > `T_burst`
   binaries per checkpoint window, the event-trigger fires early — the
   shard publishes 2-3 checkpoints per gl0 ord, still bounded.

2. **Idle mg, ml0 slower than gl0 (~43s ml0 ≫ ~7s gl0).** Most gl0 ords
   see no new ml0 binaries from this metagraph. Default C cadence would
   produce empty checkpoints every ord — wasteful. The
   **empty-skip + liveness-ping** mode addresses this: skip emission
   until either (a) a new ml0 binary arrives, or (b) `T_alive` has
   elapsed since last emission (default 30s ≈ 4 gl0 ords). gl0 sees
   ~1 checkpoint per shard per ~30s for idle shards, ~1/ord for active
   shards. **gl0's per-ord inclusion field stays bounded by the
   N_shards × small-constant; the constant is closer to 1 when most
   shards are idle.**

3. **Mixed shard (some mgs active, some idle).** The committee accumulates
   binaries from active mgs into the next emission; idle mgs contribute
   nothing per round. The checkpoint is a `SortedMap[Address, …]` per
   shard, so a partially-populated entry is the normal case. **No
   special handling needed.**

### 4.3 Why default to B's cadence, not A's

The latency win in A (~5s vs ~10.5s) is meaningful for active mgs but
not transformative — both A and B leave the user-to-finality path well
above ml0 internal consensus time. The downside of A (large inclusion
field, more committee rounds, larger slashing evidence corpus) compounds
at production scale (1K-3K metagraphs × 10-30 active-rate ml0 / ord).
B's cadence is the safer default; the burst-trigger path is the
opt-in escape hatch for genuinely-active shards.

### 4.4 What C inherits from existing code

- **Chain-link discipline** lives inside the checkpoint's
  `perMetagraph` map at the per-Address level, identical to existing
  `GlobalSnapshotStateChannelAcceptanceManager.onlyPossibleReferences`
  (lines 191-202). The shard's checkpoint chain (`lastCheckpointHash` →
  `lastCheckpointHash`) is a new chain at the shard envelope level.
- **`NonEmptyList`-per-key inclusion** matches the existing precedent
  `stateChannelSnapshots: SortedMap[Address, NonEmptyList[...]]`. The new
  field becomes `shardCheckpoints: SortedMap[ShardId,
  NonEmptyList[Signed[ShardCheckpoint_C]]]`.
- **Trigger composition** mirrors `GlobalSnapshotEventsPublisherDaemon`
  (`eventTriggerThreshold` + `eventTriggerCooldown`) and
  `CurrencySnapshotEventsPublisherDaemon` (43s `timeTriggerInterval` +
  5s `eventTriggerCooldown`). The "shard committee emits on
  whichever-trigger-first" pattern is already present in the codebase;
  reuse the same `ConsensusTrigger` ADT plus `EventTriggerGuard` shape.
- **Per-shard finality fallback** uses the existing `T_depth1` /
  `T_count` / `T_weight` trigger stack (see `attestation-and-finality.md`
  §0.2). Reframed: gl0's existing finality applies to the gl0 ords
  that *contain* the checkpoints; the shard's local commitment is the
  committee signature on the envelope.

### 4.5 Reference systems — where this recommendation lands

| System | Inner cadence | Outer cadence | Inclusion model | Our analog |
|---|---|---|---|---|
| Polkadot parachain → relay | 6s parablock | 6s relay block; backing + inclusion = 12s | One PoV per relay block; multiple parablocks per PoV under Elastic Scaling | Option C with burst-trigger — multi-parablock-in-one-PoV is exactly C's event-trigger path |
| Cosmos zone → hub | varies per zone (~6s typical) | varies per hub | Relayer-mediated, async; packets in any batch | More like Option A (per-packet) but relayer-latency-dominated rather than consensus-aligned. Not directly applicable — IBC packets aren't aggregated into a single signed envelope at the hub |
| Ethereum slot → epoch | 12s slot | 6.4min epoch (32 slots) | Per-slot attestations aggregated by per-committee aggregators within the slot | Closest analog to Option B's "fixed cadence with explicit aggregation step". Aggregator role parallels our shard committee |
| Bitcoin tx → block | mempool (continuous) | ~10min block | Variable-size block packaging mempool txs, capped at block weight | Option C in spirit — variable-contents-per-envelope, emission cadence detached from input cadence, capped |

The recommendation is closest to **Polkadot's Elastic Scaling**
(default 1 parablock/relay-block + burst absorption) and **Bitcoin's
block packaging** (variable contents per fixed-cadence envelope). The
slashing / committee model is closest to **Ethereum's per-slot
committee + aggregator** pattern but with the cadence question
inverted — Ethereum aggregates within a fixed-slot, we aggregate over
multiple ml0 productions within a variable-size envelope.

---

## §5 Empty / liveness considerations

### 5.1 The signal

For each shard, gl0 needs to distinguish three states:

| State | Cluster signal | gl0 detectability |
|---|---|---|
| Shard active, has new ml0 binaries | Non-empty checkpoint at every committee emission | Trivial — checkpoint present |
| Shard alive, no ml0 activity this period | **Need**: explicit liveness ping | **Without ping**: silence — indistinguishable from partition |
| Shard partitioned, no committee quorum | Silence | **Without ping**: indistinguishable from idle |

### 5.2 Recommended liveness scheme

**Empty-skip-with-T_alive-ping**, parameter values:

- `T_alive = 30s` (≈ 4 gl0 ords): the committee MUST emit a checkpoint
  every `T_alive` even if `perMetagraph` is empty. Empty pings carry
  only `(shardId, seq, lastCheckpointHash, coversToWallClock,
  committeeSignature)`.
- `T_burst = 20 binaries`: the committee MAY emit early if accumulated
  binaries exceed the cap.
- `T_checkpoint = 7s` (≈ gl0 cadence): default non-empty emission rate.

This gives:
- **Active shard**: ~1 checkpoint/ord, each non-empty.
- **Idle shard**: ~1 checkpoint per `T_alive` = 1 per 30s = ~1 every 4
  gl0 ords. The intermediate ords contain no entry for this shard. gl0
  observes `seq` advances monotonically.
- **Partitioned shard**: no checkpoints. After `T_partition = 2 ×
  T_alive = 60s` of silence, gl0 marks the shard as
  liveness-suspect — surfaceable to metrics for operator alerting.
  After `T_partition_hard` (e.g. 5 × T_alive = 150s, ≈ 21 gl0 ords),
  trigger the emergency-rotation path from
  `CROSS-SHARD-MITIGATION-PROPOSAL.md` §2.4 ("fallback if shard
  quorum drops <2/3 for K snapshots").

**Network noise budget for empty pings:** at S = 3000 shards and
T_alive = 30s, the cluster sees `3000 / 30 = 100` empty checkpoints per
second steady-state. Each empty ping is ~96 bytes (sig) + ~32 bytes
(hash) + ~24 bytes (header) ≈ 150 bytes. Steady-state idle-ping
bandwidth: 100 × 150 bytes/s = 15 KB/s gossip traffic. Negligible at
production scale (compared to e.g. the 8 events/ord × 7s/ord ≈ 1.1
events/sec already-baseline rumor traffic at one node — this is
per-cluster, not per-node).

### 5.3 Equivocation evidence under empty pings

An empty ping is still a signed envelope; equivocation construction
works identically:

```
slashable_evidence(shardId, seq) :=
  Signed[ShardCheckpoint_C] with seq=S, committeeSignature_A
  Signed[ShardCheckpoint_C] with seq=S, committeeSignature_B
  where committee_A ≠ committee_B (any signer in both)
```

Empty-vs-non-empty content at the same `seq` is also slashable —
the committee cannot truthfully sign two different envelopes at the
same sequence. This is consistent with the "evidence + determinism"
principle from `SLASHING-DESIGN.md` and the `:feedback_slashing_safety_bar`
rule.

---

## §6 Open questions

1. **`T_burst` calibration.** The default of 20 binaries/checkpoint is
   chosen above the expected 14 binaries/ord active-load worst case at
   N/S = 10. Need a sim or production-trace pass to confirm this is the
   right safety margin. **Likely re-tuned post-launch.**

2. **Committee quorum threshold for the envelope signature.** Should
   the shard committee use a 2/3 threshold (matching `T_count` /
   `T_weight`)? Tied to the per-shard finality model — sim work
   pending. **Likely: τ = 2/3 to match gl0 finality semantics.**

3. **Cross-shard receipt timing under C.** If shard 1 needs to read
   shard 2's state, when can it expect shard 2's checkpoint to be
   available? Under C's variable cadence, the expected latency
   distribution is wider than under B. Open whether this matters in
   practice (cross-shard reads are inclusion-proof-based per
   `CROSS-SHARD-MITIGATION-PROPOSAL.md` §2.6 — proofs are constructed
   per-leaf, not per-checkpoint, so checkpoint cadence may be a
   second-order effect). **Needs cross-shard read-path design before
   the granularity choice matters.**

4. **Interaction with the staggered rotation.** Per
   `CROSS-SHARD-MITIGATION-PROPOSAL.md` §2.4, 1/4 of operators rotate
   per eta-boundary (~6h). During the rotation window, a checkpoint
   may be signed by a committee that's about to lose 1/4 of its
   members. Does the next-epoch committee re-sign or accept the
   handover? **Likely: handover-anchor checkpoint at the boundary, but
   needs design.**

5. **gl0 acceptance ordering within an ord.** gl0 currently
   chain-link-unfolds per-Address binaries via
   `selectStateChannels` (line 204). Under C, the unfold lives one
   level down (inside the checkpoint's `perMetagraph` map). The
   per-shard checkpoint chain (`lastCheckpointHash`) adds a
   second-level chain. **Needs explicit unfold-order spec** — likely
   "chain-link per-shard envelopes first, then per-metagraph within
   each envelope".

6. **Interaction with NIPoPoW per-shard tower.** Tower entries are
   anchored at shard sub-snapshots. Under C, the natural tower anchor
   is the shard's per-`seq` envelope, not per-ml0-snapshot. **Needs
   alignment with `NIPOPOW-PROPOSAL.md` §2 — likely no change in
   spirit but the doc currently assumes per-sub-snapshot anchoring
   which under C becomes per-checkpoint anchoring.**

7. **Should `T_checkpoint` track gl0 ord rate explicitly or just be a
   fixed wall-clock?** Tying to gl0 ord rate adds a dependency
   (committee needs to track gl0 ord advance); fixed wall-clock is
   simpler but may drift. **Likely: fixed wall-clock with cluster-wide
   default, soft-tracked against gl0 ord rate for monitoring.**

---

*Document end.*
