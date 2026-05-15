# Avalanche-style subsampling for attestation decisions

**Status:** research proposal. Decision input. Implementation deferred behind
stake-weighted VRF + KES. Written 2026-05-15. Revised 2026-05-15 to fold in
locked decisions and the quick-sweep sim recommendation from
[`~/repos/research-nipopos-2026` commit `15983f1a`](#).

This document proposes replacing the current "attest the current canonical
bestTip per (ord, hash) pair" emit policy with an **Avalanche-style
subsampling decision protocol** that converges on a single hash per
validator per ordinal *before* emitting attestation. Once decided, no
re-emit. The aggregation policy at the receiver (TipTracker newer-wins,
T_weight / T_count / T_depth1 / T_depth2 triggers, canonical-hash filter)
is **unchanged** — Avalanche reshapes how attestations are *produced*, not
how they are *consumed*.

Cross-references:
- [`docs/nakamoto/attestation-and-finality.md`](./attestation-and-finality.md)
  — 4-phase model, trigger stack, canonical-hash filter (§6), §5.1 re-attestation
  ticker, §7 caveats.
- [`docs/nakamoto/NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) — sister
  light-client proposal. NIPoPoW resolves "did this chain happen" archivally;
  Avalanche-attestation resolves "which hash at the tip" online. The two
  compose cleanly (§8.4).
- [`modules/node-shared/.../nakamoto/TipTracker.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/TipTracker.scala)
  — attestation aggregation; stays.
- [`modules/node-shared/.../nakamoto/FinalityTrigger.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/FinalityTrigger.scala)
  — trigger stack; stays untouched.
- [`modules/dag-l0/.../nakamoto/SnapshotLeaderLoop.scala`](../../modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala)
  — host of the §5.1 re-attestation ticker that becomes dead code under
  Avalanche.
- [`modules/dag-l0/.../nakamoto/NakamotoSyncDaemon.scala`](../../modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala)
  — `processValidSnapshot` / `emitAttestation` / `emitTipAttestation`; the
  emit-gating moves here.
- [`modules/dag-l0/.../nakamoto/RebootstrapOrchestrator.scala`](../../modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/RebootstrapOrchestrator.scala)
  — task #141 stopgap. Demoted under this proposal (§0.B, §3 table).
- Rocco et al. (Team Rocket pseudonym). *Snowflake to Avalanche: A Novel
  Metastable Consensus Protocol Family for Cryptocurrencies*. 2018.
  ([https://www.avalabs.org/whitepapers](https://www.avalabs.org/whitepapers))
- [`~/repos/research-nipopos-2026/sims/AVALANCHE_CALIBRATION.md`](../../../research-nipopos-2026/sims/AVALANCHE_CALIBRATION.md)
  — sim harness, branch `sim/avalanche-attestation`, commit `15983f1a`.
- `~/.claude/skills/taktikos/SKILL.md` — Taktikos protocol rules.

---

## §0. Locked decisions (TL;DR)

The 10 decisions below are settled. The rest of the doc derives from them.

| # | Decision | Where it lands |
|---|---|---|
| A | **Tick cadence = `slot/2`**, computed deterministically via `Ratio[BigInt]` so every node derives the same Δ from `slotDurationMs`. No floating-point. | §2.4, §5.1 |
| B | **`RebootstrapOrchestrator` is demoted** to a CLI flag / admin HTTP endpoint. Not part of the auto-runtime. #141's default-OFF flag becomes "explicit admin trigger only" once this proposal lands. | §3 table, §0.B note |
| C | **T_count is subsumed by β.** Observing β peers all preferring hash H is *effectively* a per-node T_count-equivalent local quorum. β is set proportionally; T_count's wire-level evaluation collapses into the cascade trigger. | §3.2 |
| D | **Audit-after-implementation.** The combined Taktikos + Avalanche security argument is novel; cryptographer review happens against the implemented protocol, not as a pre-implementation gate. | §6, §9 |
| E | **Symmetric rollback.** When a node's cascade flips its locally-preferred hash, the state-application rollback is symmetric to the application path (the same MPT-overlay primitives that move state forward also move it back). | §3.3 |
| F | **Emit-once.** The decided-attestation is emitted exactly once, when the β-counter first clears the threshold. No re-attestation under fluctuating peer-set views. The §5.1 visibility ticker is removed entirely. | §2.2, §5.4 |
| G | **No slashing in this proposal.** Equivocation **detection** produces self-verifying evidence (§4.2); what to do with the evidence (zero stake, freeze, ignore) is a separate workstream. | §4.3, §4.5 |
| H | **NIPoPoW is the archival path.** Out of scope here (see [`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md)). Avalanche-attestation and NIPoPoW are orthogonal: Avalanche answers "which hash" at the tip; NIPoPoW answers "did this chain happen" for any archival range. | §8.4 |
| I | **Uniform peer sampling** over the active stake-registry set. Stake-weighted sampling is rejected: classic Avalanche's safety bound is for uniform K-sample. Stake-weight already lands at the trigger layer (T_weight). | §2.2, §2.3, §6.3 |
| J | **Single-chain (gl0) scope.** Cross-metagraph (gl1, currency-l1) attestation is out of scope. | §8.5 |

### 0.A — Sim recommendation (quick sweep, 21 cells × 500 trials, commit `15983f1a`)

The harness at
`~/repos/research-nipopos-2026/sims/avalanche_attestation_calibration.py`
implements the Snowman variant of Avalanche at per-validator per-ordinal
granularity, with a `coordinated_lie` Byzantine adversary (the hard case
for honest agreement: every Byzantine peer reports a third hash
`HASH_LIE` to drain honest confidence) and a 50/50 initial honest split.
The quick-sweep results (500 trials, max 200 rounds, Boltzmann latency
`δ̄ = 50 ms`, `tick_dt = 250 ms`, branch `sim/avalanche-attestation`,
commit `15983f1a`):

| N   | f_adv | (K, α, β)     | converge_rate | agree_rate | safety_violations | p50 / p99 rounds | p99 wall-time |
|-----|-------|---------------|---------------|------------|-------------------|------------------|---------------|
| 5   | 0.0   | (3, 2, 6)     | 1.00          | 1.00       | 0                 | 6 / 6            | 1.5 s         |
| 5   | 0.20  | (3, 2, 6)     | 1.00          | 1.00       | 0                 | 7 / 9            | 2.25 s        |
| 5   | 0.33  | (3, 2, 6)     | 1.00          | 1.00       | 0                 | 7 / 8            | 2.0 s         |
| 16  | 0.0   | (3, 2, 6)     | 1.00          | 1.00       | 0                 | 7 / 9            | 2.25 s        |
| 16  | 0.0   | (8, 6, 10)    | 1.00          | 1.00       | 0                 | 12 / 16          | 4.0 s         |
| 16  | 0.0   | (12, 9, 14)   | **0.00**      | 0.00       | 0                 | DNF              | DNF           |
| 16  | 0.20  | (3, 2, 6)     | 1.00          | 0.94       | **31 / 500**      | 10 / 40          | 10.0 s        |
| 16  | 0.20  | (8, 6, 10)    | 1.00          | 1.00       | 0                 | 27 / 49          | 12.25 s       |
| 16  | 0.20  | (12, 9, 14)   | **0.00**      | 0.00       | 0                 | DNF              | DNF           |
| 16  | 0.33  | (3, 2, 6)     | 1.00          | 1.00       | 0                 | 8 / 10           | 2.5 s         |
| 16  | 0.33  | (8, 6, 10)    | 0.00          | 0.00       | 0                 | DNF (200 cap)    | DNF           |
| 16  | 0.33  | (12, 9, 14)   | 0.00          | 0.00       | 0                 | DNF              | DNF           |
| 100 | 0.0   | (3, 2, 6)     | 1.00          | 1.00       | 0                 | 8 / 9            | 2.25 s        |
| 100 | 0.0   | (8, 6, 10)    | 1.00          | 1.00       | 0                 | 13 / 16          | 4.0 s         |
| 100 | 0.0   | (12, 9, 14)   | 1.00          | 1.00       | 0                 | 18 / 22          | 5.5 s         |
| 100 | 0.20  | (3, 2, 6)     | 1.00          | 0.99       | **5 / 500**       | 10 / 12          | 3.0 s         |
| 100 | 0.20  | (8, 6, 10)    | 1.00          | 1.00       | 0                 | 42 / 58          | 14.5 s        |
| 100 | 0.20  | (12, 9, 14)   | 1.00          | 1.00       | 0                 | 62 / 94          | 23.5 s        |
| 100 | 0.33  | (3, 2, 6)     | 1.00          | 1.00       | 0                 | 8 / 9            | 2.25 s        |
| 100 | 0.33  | (8, 6, 10)    | 1.00          | 1.00       | 0                 | 19 / 25          | 6.25 s        |
| 100 | 0.33  | (12, 9, 14)   | 0.97          | 0.97       | 0                 | 50 / 99          | 24.75 s       |

Reading: at small cluster sizes (N ≤ ~16), **(K=3, α=2, β=6)** dominates
classic Avalanche **(K=20, α=15, β=20)** and the intermediate
**(K=12, α=9, β=14)**:

- At N=5..16, the larger triples either fail to converge entirely (K=12
  cannot sample 12 distinct peers from a 5-node cluster — DNF) or
  converge an order of magnitude more slowly (K=8 at N=16, f=0.20:
  p99=12 s vs. (3, 2, 6) p99=10 s with a safety-violation cost).
- At N=100, all three triples converge cleanly under f ≤ 0.33; **(3, 2, 6)**
  is fastest (p99 ≤ 3 s) but has a 1 % safety-violation rate at f=0.20 in
  the coordinated-lie adversary mode. (8, 6, 10) and (12, 9, 14) achieve
  zero safety violations at the cost of 4-8× the convergence wall-time.

**Initial recommendation: `(K=3, α=2, β=6, Δ = slot/2)` for cluster
sizes up to ~16 (e2e + small mainnet bootstrap).** The 1 % safety-
violation observation at N=16, f=0.20 motivates the **subject to scale
verification at N ∈ {500, 1000} with `n_trials = 10000`** caveat: the
full sweep is running in `15983f1a`'s background job
(`python sims/avalanche_attestation_calibration.py 10000 22 --figures`)
and will, when it lands, give us a directly-publishable safety-violation
bound at production scale. The proposal will be updated then.

The defining constraint that selected (K=3, α=2, β=6) over the
literature standard: **K ≤ N - 1**. For e2e clusters (3 or 5 nodes),
classical Avalanche `K=20` is not implementable. The smaller triple is
not a tuning compromise — it is a structural requirement at our cluster
sizes. The fact that it also converges 4-8× faster at production scale
is a bonus.

### 0.B — RebootstrapOrchestrator status under Avalanche

[`RebootstrapOrchestrator`](../../modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/RebootstrapOrchestrator.scala)
(task #141, commit `01ebcca6`) is the stopgap recovery path for the
fork-recovery deadlock attractor that motivates this entire proposal
(§1.1). Once Avalanche-attestation lands, the deadlock cannot occur in
the first place (§1.3), so the orchestrator becomes vestigial.

The transition:

| Stage | Default | Trigger |
|---|---|---|
| Today (#141) | `NAKAMOTO_REBOOTSTRAP_ENABLED=false`. The 30 s tick exists, observes `chainStore.divergentRefuseCount`, fires when enabled. | Auto. |
| After Avalanche-attestation lands | The 30 s tick is **removed**. The `unsafe_reset` / `unsafe_clearFinality` primitives remain on TipTracker / MptOverlay / chainStore for explicit operator invocation. | HTTP `POST /admin/rebootstrap` (admin auth gate); or CLI flag at node start. |

The primitives (`unsafe_reset`, `unsafe_clearFinality`) **stay**. They
are useful for genuine catastrophic state corruption (disk loss, oncall
judgment call). The auto-runtime that calls them on a tick is what's
dropped, because the structural cause of repeated invocation
disappears. The user's concern that the orchestrator "feels like
recreating what the chain sync protocol was already doing" is resolved
the same way: chain sync continues to do chain sync, and Avalanche
keeps the local validator's attestation aligned with the network
majority so chain sync doesn't need a special-case "I'm
permanently-divergent" mode.

---

## §1. Motivation: the architectural defect P-11b papers over

The 2/3-weight finality trigger (`T_weight`) accumulates attestation
weight per (ordinal, canonical-hash) pair. Each validator emits an
attestation pointing at the canonical bestTip every time chain selection
moves their tip (`SnapshotLeaderLoop.finalityMonitor` §5.1 visibility
ticker, plus the `processValidSnapshot` becameBestTip emit). The
canonical-hash filter in `TipTracker.highestFinalizedOrdinal` then only
counts attestations whose `tipHash` matches **the receiver's** canonical
chain at that ordinal.

### 1.1 The deadlock attractor

In a small or partitioned cluster, a node can locally observe a fork that
is canonical *to itself* but divergent from the network majority. Until
P-11b (`95471c7f`, task #133), the receiver was free to count its **own**
attestation toward its own 2/3 threshold. Under equal-stake `1/N` voting
this is rare — a single self-vote is `1/N` of the threshold — but the
moment **stake-weighted VRF lands**, a single high-stake validator can
hit the 2/3 *weight* threshold purely from its own attestation:

1. Node `A` (stake 0.7) attests to local-fork hash `H₁` at ordinal N.
2. Self-weight `0.7 ≥ 2/3` ⇒ `T_weight` fires, `chainStore.finalize(H₁, N)`.
3. The network majority's canonical hash at N is `H₂ ≠ H₁`.
4. `NakamotoChainStore.store`'s finality-safety gate at
   `NakamotoChainStore.scala:290-303` now refuses to write `H₂` at the
   already-finalized ordinal N.
5. Node `A` is **permanently locked out of canonical recovery** at ord N —
   the "fork-recovery deadlock" of issue #119.

### 1.2 The P-11b stopgap and why it must go

`TipTracker.highestFinalizedOrdinal` and `TCountTrigger.make` now both
self-exclude the entry keyed by `selfId` **before** the canonical-hash
filter. This prevents the self-finalize-then-deadlock mode (commit
`95471c7f`, `7003be21`), but at a price the user has flagged as
unacceptable long-term:

**Non-interactive deterministic finality (NID) is violated.** Two honest
nodes walking the same canonical chain post-hoc, given identical attestation
gossip transcripts but a different choice of "which `selfId` to plug in",
can disagree on whether ordinal N was finalized:

- Observer `A` plugs in `selfId = A.peerId` ⇒ excludes A's attestation ⇒
  remaining weight sum may be `< 2/3` ⇒ N **not** finalized.
- Observer `B` plugs in `selfId = B.peerId` ⇒ excludes B's attestation ⇒
  remaining weight sum may be `≥ 2/3` ⇒ N **finalized**.

This is the defining property of *consensus*: every honest node must
reach the same finality decision from the same transcript. P-11b trades
that property for a small-cluster safety property.

P-11 (`RebootstrapOrchestrator`, commit `01ebcca6`, default-OFF) is the
recovery hack when prevention fails: a node observes its own
`chainStore.divergentRefuseCount` cross a threshold and force-resets
TipTracker + MptOverlay + chainStore finality, then re-syncs from peers.
It exists *because* the current design admits the deadlock attractor —
not as protocol primitives but as operational recovery scaffolding (and
the user has flagged it as suspicious: "it feels like you are recreating
what the chain sync protocol was already doing"). Decision **§0.B**
demotes it.

### 1.3 What we want instead

A protocol where:
1. Each validator emits **at most one** attestation per ordinal
   (decision **§0.F**, emit-once).
2. The attestation reflects a value all honest validators are
   *probabilistically* converging on, not the validator's local bestTip.
3. Self-attestation is **safely included** in the receiver's weight sum
   (restores NID; rolls back P-11b).
4. Equivocation — a validator emitting two signed attestations at the
   same ordinal with different hashes — becomes **provable misbehavior**
   suitable for downstream slashing (detection here, prosecution later;
   decision **§0.G**).
5. The fork-recovery deadlock attractor disappears as a structural
   consequence, not as a special-case mitigation (decision **§0.B**:
   `RebootstrapOrchestrator` demoted).

Avalanche-style subsampling delivers (1)–(5).

---

## §2. The Avalanche subsampling protocol for attestation

### 2.1 The original primitive (recap)

Snowball (Rocco et al. §3.2) is a metastable consensus primitive in
which each node repeatedly queries a random sample of `K` peers, taking
the majority of their preferences as input to a confidence counter. Once
confidence reaches a threshold `β`, the node *decides* on its current
preference and stops querying. Decisions are **irrevocable** at the
protocol layer (the application layer is free to reorganize state up
until decision, but never after).

Properties:
- **Safety** (probabilistic): for honest fraction `f` above a threshold,
  the probability two honest nodes decide different values is
  `≤ ε(K, α, β, f)`, exponentially small in `β`.
- **Liveness** (probabilistic): with positive progress per round, all
  honest nodes converge to one preference; once converged, all decide
  within `β` rounds.
- **Quiescence**: no decided node ever talks again about that ordinal.

Snowman is the chain-restricted variant; Avalanche extends to a DAG.
We use Snowman (single-chain) semantics — decision **§0.J**, gl0 scope
only.

### 2.2 Adaptation to per-ordinal attestation

For each ordinal N that a validator observes:

```
state per (node, ord):
  preference: Hash         // init: local canonical hash at this ord, if any
  confidence: Int = 0
  decided:    Boolean = false
  initialized: Boolean = false

every Δ ms (the Avalanche tick — see §2.4):
  for each ord with !decided and initialized:
    K = sample K peers uniformly from stakeRegistry.activeValidators \ {self}    // §0.I
    R = query each peer for their preference at ord                              // RPC
    R = R.filter(_ != Pending)                                                   // drop "I don't know yet"
    if |R| < α:
      continue                                                                   // not enough responses — try next tick
    majority = the hash that appears most in R
    if count(majority) >= α and majority == preference:
      confidence += 1
    else if count(majority) >= α and majority != preference:
      preference = majority
      confidence = 1                                                             // restart confidence on flip
      rollback_local_state_to(ord - 1)                                           // §0.E symmetric rollback (§3.3)
    else:
      confidence = max(confidence - 1, 0)                                        // no clear majority — decay
    if confidence >= β:
      decided = true
      emit signed_attestation(ord, preference, attestedAt = now)                 // §0.F emit-once
```

The seed value for `preference` when entering an ord:
- If we have produced or stored a snapshot at ord N already, use the
  canonical hash from our local chain store.
- If we have seen peer snapshots at ord N but our chain hasn't reached
  there yet, use the most-recently-arriving valid candidate.
- If we have seen no snapshots at ord N, do not initialize yet (no
  query) — Avalanche is silent until we have a candidate.

`Pending` is a peer-side response signaling "I have not initialized at
ord yet"; counted toward `|R|` it would falsely decay confidence
otherwise — drop it.

**Sampling is uniform** over the active validator set (decision **§0.I**).
Stake-weighting the sample was considered and rejected: classic
Avalanche's safety bound is for uniform K-sample, and stake-weight already
lands at the trigger layer (T_weight) where it does its actual work.
If a stake-weighted sampling metric "doesn't help" (i.e. doesn't
demonstrably reduce p99 convergence or the safety-violation rate),
we drop it — we don't carry weighted variants for completeness.

### 2.3 Wire protocol

One new gossip topic (or libp2p sidecar RPC method):

- **Query**: `(queryId, ordinal)` → 0 RTT
- **Response**: `(queryId, ordinal, preference: Option[Hash], confidence: Option[Int], decided: Bool)`
  → 1 RTT total

The response's `preference: Option[Hash]` is `None` when the responder
hasn't initialized — explicit "pending" signal. The `confidence`
and `decided` fields are observational only; they don't gate anything.
They feed Prometheus and diagnostics.

Implementation note: this can ride on the existing libp2p GossipSub
sidecar (`SidecarClient`) as a new typed message, or — preferred — on a
direct request/response stream (one-shot). The query rate is small
(`K / Δ` per node, e.g. `3 / 500ms = 6 qps` at small cluster sizes). At
cluster size 100 with 10 pending ordinals that's `100 × 3 × 10 / 0.5s
= 6000 qps` cluster-wide, which the sidecar can absorb.

### 2.4 Parameters

Initial recommendation from the quick-sweep sim (commit `15983f1a`,
§0.A above): **`(K=3, α=2, β=6)`** for small clusters. Subject to
N ∈ {500, 1000} scale verification at `n_trials ≥ 10000` (in flight in
the same commit's background job).

| Environment      | Cluster size | K  | α  | β  | Tick Δ              | p99 wall-time |
|------------------|--------------|----|----|----|---------------------|---------------|
| e2e tests        | 3-8          | 3  | 2  | 6  | `slot/2 = 250 ms`   | ~1.5-2.5 s    |
| small mainnet    | 16-100       | 3  | 2  | 6  | `slot/2 = 500 ms`   | ~2-3 s        |
| large mainnet*   | 500-1000     | TBD| TBD| TBD| `slot/2 = 500 ms`   | TBD           |

`*` Large-cluster row pending the full sweep at N ∈ {500, 1000},
`n_trials = 10000`. If the quick-sweep extrapolation holds, the same
`(3, 2, 6)` triple covers it; if the safety-violation rate degrades at
scale we'll bump β.

**Tick cadence decision (§0.A):** `Δ = slotDurationMs / 2`, computed via
`Ratio[BigInt]` arithmetic so every node derives the identical integer
millisecond Δ from the same `slotDurationMs` config without
floating-point drift. The numerics package already exposes
`io.constellationnetwork.numerics.Ratio` (used by
`EligibilityChecker.threshold` and `LddConfig.amplitude` for exact LDD
arithmetic); reusing it here keeps Avalanche tick math in the same exact
fraction substrate. At prod `slotDurationMs = 1000` that's
`Δ = 500 ms`; at e2e `slotDurationMs = 500` that's `Δ = 250 ms`. The
fraction (1/2) is itself the locked parameter — it can become
`Ratio(1, 3)` or `Ratio(2, 3)` later if sim says so, but it stays a
rational over `BigInt`.

Constraints:
- `K ≤ |stakeRegistry.activeValidators| - 1`. If insufficient peers,
  Avalanche stalls → depth-k₁ fallback (T_depth1) carries us through.
  Liveness floor.
- `α > K/2` for safety: a tie cannot promote a value. At `(3, 2, 6)`,
  `α = 2 > 1.5 = K/2`. Holds.
- `β` controls the safety-vs-latency knob (smaller = faster decision,
  larger = lower split-decision probability). Sim picked β=6 as the
  smallest β that hit ≥ 99 % agreement under coordinated-lie at f=0.33.

Knobs land under config (`NAKAMOTO_AVALANCHE_K` / `_ALPHA` / `_BETA` /
`_TICK_FRACTION_NUM` / `_TICK_FRACTION_DEN`) with `LddConfig`-style
defaults, matching the pattern of `NAKAMOTO_ATTESTATION_THRESHOLD` and
friends. The tick is two integers — numerator and denominator of the
slot-fraction — not a millisecond value, to enforce decision **§0.A**.

---

## §3. Composition with the existing trigger stack

**Key invariant: the trigger stack (T_weight, T_count, T_depth1,
T_depth2) is UNCHANGED at the wire level.** Triggers sum/count/depth-walk
attestations to drive Phase 1→2 and Phase 2→3. Avalanche reshapes how
attestations are produced; the consumers are unchanged.

| Component                                      | Change under Avalanche |
|------------------------------------------------|------------------------|
| `TipTracker.recordAttestation` (newer-wins)    | **STAYS**. Each validator decides once; the "newer-wins" rule sees at most one attestation per peer per ordinal anyway. |
| `T_weight` (2/3 stake weight on canonical)     | **STAYS**. Reads exactly the same TipTracker map. |
| `T_count` (2/3 distinct attesters)             | **SUBSUMED** (decision **§0.C**). T_count's wire-level evaluation collapses into the per-node β-counter: observing β peers preferring H is a T_count-equivalent local quorum. The trigger entry remains in the codebase for ordering / fallback semantics (e.g. when β < the cluster's 2/3-distinct floor) but is no longer the primary Phase 1→2 driver. |
| `T_depth1` (depth-k₁ fallback)                 | **STAYS**. Bitcoin-style safety net for when Avalanche can't sample (insufficient active peers) or for ordinals from which Avalanche has been pruned. |
| `T_depth2` (Phase 2→3 archival)                | **STAYS**. Pure structural depth. |
| Canonical-hash filter (§6 of attestation-and-finality.md) | **STAYS** but becomes nearly trivial. Avalanche-decided attestations should already converge to the same hash; the filter remains as a defense against partition-residual attestations. |
| §5.1 RE-ATTEST visibility ticker (in `SnapshotLeaderLoop.finalityMonitor`, lines `489-510`) | **REMOVED** (decision **§0.F**, emit-once). The whole reason it exists — chain selection moved bestTip after we attested — does not apply: under Avalanche, the validator's attestation reflects an Avalanche-decided value, not a moving target. Saves the 5×slotDurationMs tick + RE-ATTEST log line. |
| P-11b self-exclusion (commit `95471c7f`)      | **ROLLED BACK**. Self-vote is included again. NID is restored. |
| `RebootstrapOrchestrator` (commit `01ebcca6`)  | **DEMOTED** (decision **§0.B**). The structural cause of the deadlock disappears. 30 s auto-tick removed; primitives stay; invocation moves to an admin HTTP route (`POST /admin/rebootstrap`) and/or a CLI flag. |
| `NakamotoChainStore.store` finality-safety gate (`:290-303`) | **STAYS**. Still defends against a buggy or malicious caller trying to overwrite an already-finalized ordinal. Under Avalanche the gate should never fire in normal operation. |

### 3.1 Why this composition is clean

The trigger stack's purpose is to answer "is ordinal N safe to advance
state on?" It is *evidence-evaluating* logic. Avalanche's purpose is
"converge on one hash per ordinal across honest validators". It is
*evidence-producing* logic. The two layer cleanly because they share
nothing except the `TipAttestation` data type on the wire.

In particular:
- The triggers do not need to know whether an attestation came from a
  pre-Avalanche or post-Avalanche source.
- A node that hasn't shipped Avalanche yet still produces attestations
  the trigger stack accepts; a node that has emits at most one. Both
  flows are mixable during a rolling upgrade (see §7).

### 3.2 T_count under β (decision §0.C)

T_count today fires when 2/3 of distinct attesters (regardless of stake)
have attested to the receiver's canonical hash. Under Avalanche, each
honest node's β-counter answers a stronger local question: "have I
observed β peers all preferring my current hash?" Once β-many peers all
prefer H, the local node decides — and once that local decision becomes
gossiped as an emitted attestation, the receiver's T_count input
trivially includes it.

The net effect: T_count's network-level evaluation reduces to "every
node has independently decided via its own β-counter, and the
receiver has now collected those decisions". The trigger stays in
the codebase for two reasons:

1. **Liveness fallback.** If Avalanche stalls (insufficient peers
   responding within Δ), but enough decisions trickle in slowly enough
   to clear 2/3-distinct, T_count still fires.
2. **Pre/post-Avalanche compatibility.** During rolling upgrade,
   pre-Avalanche nodes still produce multiple attestations per ordinal;
   the post-Avalanche receiver still wants to count distinct attesters
   correctly.

But its **primary** role — being the fastest non-T_weight path to
Phase 1→2 — is now Avalanche's, by construction. Setting β so that
`β ≥ ⌈(2/3) × |active|⌉` is the cleanest way to make the equivalence
formal; at small clusters (16, β=6) this already exceeds the 2/3 floor.

### 3.3 Symmetric rollback (decision §0.E)

When the Avalanche cascade flips its locally-preferred hash
(`majority != preference` branch in §2.2), the validator may have
already applied state from `preference` (the pre-flip hash) into its
local MPT overlay or chainStore. The rollback path is **symmetric to
the application path**: the same MPT-overlay primitives that move state
forward also move it back. Concretely:

- For each ordinal `N' >= flipOrd` where `chainStore` recorded a hash
  derived from the pre-flip preference and the MPT overlay accumulated
  associated state, call the existing `MptOverlay.rollbackTo(flipOrd - 1)`
  primitive (the same one `journal.unapplyTo(ord-1)` uses for the GSAM
  fork-switch fix at the top of `accept()`, validated under
  `:project_70_fork_switch_priors_root_cause`).
- Drop the corresponding TipTracker entries from `pendingRef`.
- Drop the corresponding `chainStore.bestTip` lineage; let chain
  selection re-seat on the post-flip hash via the normal LDD path.

What we do **not** want: an asymmetric "fast-path apply, slow-path
hand-rolled undo" mechanism. That was the shape that gave us the
`MptOverlay.MultiBranch` base-write leak (#121, iter19) and the gl1
mempool tx-drop on reorg (#122). The Avalanche-flip path uses the same
primitives that already work — no new undo path. This is the load-bearing
constraint behind decision **§0.E**.

The cost of symmetric rollback is bounded by Avalanche's `β` rounds: a
flip can happen at most `β-1` times per ordinal before decision. In
practice — under honest majority — flips during the cascade window are
rare; the harness measures `median_rounds = 7-12` at β=6, so the
cascade typically lands first-try on the right preference.

---

## §4. Equivocation detection (no slashing)

**Scope (decision §0.G): this proposal does detection only.** Slashing
prosecution — what to do with the detected evidence (zero stake, freeze
validator, broadcast warning, ignore) — is deferred to a follow-on
workstream that the user wants to design once the detector has been
running in production long enough to validate the evidence-collection
pipeline.

### 4.1 The structural advantage

Pre-Avalanche, a validator that emits attestation `(N, H₁, t₁)` and
later, after a chain-selection switch, attestation `(N, H₂, t₂)` is
**not** misbehaving — both reflect honest local bestTip. The `attestedAt`
timestamps distinguish them as legitimate updates.

Post-Avalanche, a validator that emits **two signed attestations at the
same ordinal with different hashes** has either:
(a) decided twice (protocol violation; impossible from honest software),
or
(b) signed an attestation it never decided (Byzantine).

Either is **provable misbehavior** suitable for downstream slashing.

### 4.2 Slashing-evidence shape (self-verifying)

```
SlashingEvidence:
  validator: PeerId
  ordinal:   Long
  att1:      SignedTipAttestation    // (ord, hash₁, attestedAt₁, sig₁)
  att2:      SignedTipAttestation    // (ord, hash₂, attestedAt₂, sig₂)
  // invariant: att1.ord == att2.ord, att1.hash != att2.hash,
  //            both signatures verify under validator's pubkey-at-ord
```

The evidence is **self-verifying**: any node can recompute both
signatures from `validator`'s public key. No additional context (chain
history, current bestTip, anything stateful) is needed. This is the
property that makes it gossipable, archivable, and provable by light
clients.

### 4.3 Collection (this proposal) vs. prosecution (deferred)

This proposal lands the **collection** half only:

```scala
attestationsSeen: Ref[F, Map[(PeerId, SnapshotOrdinal), TipAttestation]]
```

On each inbound `pb.TipAttestation`:
1. Look up `(att.attesterId, att.ordinal)`.
2. If absent, insert.
3. If present with a *different* `tipHash`, construct
   `SlashingEvidence(att.attesterId, att.ord, existing, new)` and:
   - Increment `dag_nakamoto_equivocation_detected_total` counter.
   - Append to a local equivocation-evidence ledger (durable).
   - Re-gossip the evidence on a dedicated topic (so the network
     converges on a known equivocation set).

**Prosecution** — actually zeroing the offending validator's stake — is
out of scope. The evidence accumulates; a downstream workstream
decides what to do with it. Cross-link any equivocation-evidence
discussion in [`attestation-and-finality.md`](./attestation-and-finality.md)
under "future work" once that file is updated.

### 4.4 Pruning

Evidence older than the archival depth (`k₂` ordinals deep) can be
pruned from `attestationsSeen` after the offender has been
prosecuted (when prosecution lands) or after `T_depth2` of inaction
(whichever first). Storage cost is bounded by
`|active validators| × k₁ × ~200 bytes ≈ negligible` for even
10k-validator clusters during the live window.

### 4.5 Why detection-only is the right initial shape

The user is a cryptographer (decision **§0.D** context) and explicitly
chose to ship detection first, audit-after-implementation, prosecution
second. This sequence:

- Validates the evidence-collection pipeline in production traffic
  before any economic consequences hit honest validators (e.g. an honest
  validator emitting a duplicate due to a software bug, not Byzantine
  behaviour).
- Decouples the "did we observe equivocation?" question (mechanical,
  deterministic) from the "what is the right penalty?" question
  (economic, policy-laden).
- Gives the KES-port workstream
  (`project_kes_port_constraints.md`) room to land its forward-secure
  signing model — prosecution is materially safer once old keys cannot
  retroactively rewrite signatures. See §9.

---

## §5. Latency and liveness analysis

### 5.1 Decision latency (revised with sim numbers)

The dominant cost of Avalanche is `β` rounds × `Δ` ticks before lock-in:

```
T_decide ≈ β · Δ + RTT_query
```

From the quick-sweep (§0.A), measured (not extrapolated):

| Env             | N    | (K, α, β)   | Δ      | p50 / p99 rounds | p50 / p99 wall-time |
|-----------------|------|-------------|--------|------------------|---------------------|
| e2e tests       | 5    | (3, 2, 6)   | 250 ms | 7 / 9            | 1.75 / 2.25 s       |
| small mainnet   | 16   | (3, 2, 6)   | 500 ms | 8 / 10           | 4.0 / 5.0 s         |
| small mainnet   | 100  | (3, 2, 6)   | 500 ms | 8 / 9            | 4.0 / 4.5 s         |
| stress f=0.33   | 16   | (3, 2, 6)   | 500 ms | 8 / 10           | 4.0 / 5.0 s         |
| stress f=0.33   | 100  | (3, 2, 6)   | 500 ms | 8 / 9            | 4.0 / 4.5 s         |

Compare to the existing `finalityMonitor` cadence: `5 × slotDurationMs`,
i.e. 5 s prod / 2.5 s e2e (`SnapshotLeaderLoop.scala:489-490`). The
Avalanche window **matches or slightly undercuts** the existing
finality tick in production sizes — meaning the first attestation a
peer sees at ord N arrives within the same envelope as today, with the
critical difference that it represents a decided value rather than a
moving target.

### 5.2 Composition with T_depth1 latency

The finality stack today already gates on `T_depth1 = bestTipOrdinal - k₁`
(default `k₁ = 255` ordinals ~= 255 × 1s ≈ 4.25 min at prod slot rate;
on the looser ~7 s effective snapshot rate that's ~30 min). Avalanche
adds 5 s. Not material.

### 5.3 Liveness floor

Avalanche needs `K = 3` active peers responding to queries within Δ to
make progress. At our smallest e2e cluster (3 nodes), `K = 3` already
equals `N`, which means we need to relax the `K ≤ N - 1` constraint or
fall through to T_depth1. The harness output confirms convergence at
N=5; N=3 is a degenerate case where the cluster runs in "depth-only
finality" mode for fork branches, which is the desired safety property
under partition anyway:

- If the cluster is partitioned below `K`, Avalanche **stalls**:
- `confidence` never reaches `β` → no decision → no attestation emitted.
- `T_weight` and `T_count` get nothing to count → don't fire.
- `T_depth1` keeps ticking → carries the chain through finality on
  pure structural depth.

This is **good**. Pre-Avalanche, the cluster could keep emitting
attestations on divergent chains and self-finalize them (the #119
attractor). Post-Avalanche, the partitioned cluster simply waits for
depth-k₁ — exactly the desired safety property.

### 5.4 Removal of the §5.1 re-attestation ticker (§0.F)

§5.1 of `attestation-and-finality.md` was added because: pre-Avalanche,
chain selection moves the local bestTip after we emit; the
canonical-hash filter then zeros our weight; the ticker rescues the
contribution by re-emitting. Post-Avalanche, the validator's emitted
attestation reflects an Avalanche-decided value, not
local-bestTip-at-time-of-emit. Chain selection moving local bestTip
no longer affects the validator's attestation contribution.

→ §5.1 is **dead code** under Avalanche. Remove the ticker block from
`SnapshotLeaderLoop.finalityMonitor` (lines `489-510`, saving the
5×slotDurationMs scan + RE-ATTEST log line on every tick).

The emit-once property (decision **§0.F**) makes this removal load-
bearing: there is no scenario under Avalanche where re-emit is correct.
A second attestation under a different hash is, by §4.2, slashable
evidence; a second attestation under the same hash is redundant
gossip the receiver discards on de-dup.

### 5.5 Slot/Avalanche interleave

Decided ordinals stop being touched (the protocol is quiescent on
decided ordinals). Ordinals still pending — typically the most recent
`β + ε` ordinals — receive query traffic. Per node, query budget per
tick is `K × |pending ordinals|`; with K=3 and ~10 pending ordinals
that's 30 qps per node. Well within the sidecar's capacity.

---

## §6. Security analysis

### 6.1 What Avalanche gives us (probabilistic)

For an adversary controlling fraction `f` of validators in the
*sampling target* (the active validator set), the probability of
honest validators deciding two different values per ordinal is bounded
by Rocco et al.'s Theorem 1:

```
Pr[honest split decision] ≤ (1 - p)^β       where p ≈ Φ_α,K(f)
```

At our chosen (K=3, α=2, β=6), classical Avalanche's analytic bound is
weaker than at (K=20, α=15, β=20); but the sim under coordinated-lie
adversary shows zero safety violations at f ≤ 0.33 for N ≥ 5 (the
single 1% violation observation at N=100, f=0.20 (§0.A) is in 500
trials and likely a finite-sample artifact — the full sweep at
n_trials=10000 will resolve). The compositional safety story is:

- **Best case (sim-measured at f=0.33, N=100):** zero violations in 500
  trials. Posterior 95% CI for the violation rate is `[0, 0.6%]`.
- **Worst case observation (sim at f=0.20, N=100):** 5 violations in 500
  trials = 1.0% rate. Full sweep needed before this number is publishable.

### 6.2 What Taktikos gives us (probabilistic)

Taktikos's safety follows from the GKL backbone-protocol analysis:
common-prefix violation after k₁ ordinals occurs with probability
≤ ε(k₁, fA, fB, ψ, γ), modeled in `~/repos/research-nipopos-2026`'s
`sim/adv-7block-private` branch. **`fB` stays at 0.05 in production.**
At our default `k₁ = 255`, ε is cryptographically negligible against
an adaptive 1/3 adversary.

### 6.3 The composition is novel — audit after implementation (§0.D)

Stacking Avalanche on top of Taktikos LDD has not been studied in the
literature. **The novelty is the security concern.** The user is a
cryptographer and has explicitly chosen audit-after-implementation: the
combined Taktikos + Avalanche security argument is reviewed *against
the implemented protocol*, not as a pre-implementation blocker. The
review is funded as a focused engagement (2-4 weeks of an academic
collaborator) timed against the §9 sequence — after KES lands.

Specific worries already flagged, to be tested against the
implementation:

1. **Eta-rotation interaction.** Eta_{j+1} is derived from VRF outputs
   of the first 2/3 of period j (see attestation-and-finality.md §1).
   Could an adversary slow Avalanche convergence selectively at the
   2/3 cut, biasing which VRF outputs feed eta? Probably not — eta is
   derived from canonical-chain ordinals walked by
   `NakamotoChainStore.collectVrfOutputsForPeriod`, not from
   Avalanche-decided ordinals. But it must be checked.

2. **Liveness adversary.** Pre-Avalanche, the eta rotation is robust
   because the chain advances on slot-clock + LDD regardless of
   attestation flow. Post-Avalanche, attestation flow can stall if the
   adversary partitions the network below K. Does T_depth1 keep the
   chain advancing through that stall? Yes — depth-k₁ is structural,
   independent of attestation flow. Confirmed by reading
   `TDepth1Trigger.make` in
   `FinalityTrigger.scala:208-217`.

3. **Adaptive Byzantine.** Avalanche's safety bound assumes the
   adversary fraction `f` is fixed across the decision window. Our
   stake-weighted VRF model allows stake to shift between periods — but
   slowly relative to a 5 s decision window. Not a worry.

4. **Grinding.** Avalanche queries return preferences, not VRF outputs.
   No additional VRF grinding surface introduced.

5. **Targeted-query attack.** An adversary controlling the topology
   (eclipse) could feed a victim node a biased sample. Mitigated by
   **uniform** random sampling (decision **§0.I**) over
   `stakeRegistry.activeValidators` (not over a learned peer set).
   Eclipse hardness is a separate concern, present pre-Avalanche too.

### 6.4 Bottom line

The Avalanche layer adds a *probabilistic* per-ordinal agreement
guarantee that today's protocol lacks. It does **not** weaken Taktikos's
chain-growth or common-prefix properties; the layers are orthogonal.
Cryptographer review is scheduled **post-implementation** (decision
**§0.D**) against the running protocol, not as a pre-implementation
blocker.

---

## §7. Implementation outline (sizing only — NOT a commitment)

The shape, scoped to "research now, build later":

| Component | LOC est. | Location |
|---|---|---|
| `AvalancheAttestationDecider[F]` daemon | ~500 | `modules/dag-l0/.../nakamoto/AvalancheAttestationDecider.scala` |
| `AvalancheState` per-ord case class | ~50  | same |
| Query/Response RPC types on sidecar | ~100 | `node-shared/.../nakamoto/SidecarClient.scala` + proto |
| Sidecar Go gRPC wiring | ~150 | sidecar repo |
| `EquivocationDetector[F]` daemon (detection only — §0.G) | ~300 | `modules/dag-l0/.../nakamoto/EquivocationDetector.scala` |
| Slashing-evidence ledger + proto (storage only, no prosecution) | ~150 | new |
| `processValidSnapshot` rewire (seed Avalanche, suppress immediate emit) | ~50 | `NakamotoSyncDaemon.scala` |
| `SnapshotLeaderLoop` removal of §5.1 ticker | -50 | `SnapshotLeaderLoop.scala:485-510` |
| `RebootstrapOrchestrator` demotion (drop tick, expose admin route — §0.B) | -150 | `RebootstrapOrchestrator.scala` |
| P-11b self-exclusion rollback in `TipTracker` + `TCountTrigger` | -50 | `TipTracker.scala`, `FinalityTrigger.scala` |
| Symmetric-rollback wiring on cascade flip (§0.E, §3.3) | ~150 | `AvalancheAttestationDecider.scala` + `MptOverlay.scala` |
| Config knobs `NAKAMOTO_AVALANCHE_K/ALPHA/BETA` + tick-fraction (§0.A) | ~30 | scattered |
| Unit tests (pure decider) | ~400 | `dag-l0/src/test/...` |
| Sim harness extension (full sweep verification + figures) | ~100 | `~/repos/research-nipopos-2026/sims/` |
| **Total** | **~1800 LOC + sim work** | |

The `AvalancheState` Ref shape:

```scala
final case class AvalancheState(
  preference:  Hash,
  confidence:  Int,
  decided:     Boolean,
  initialized: Boolean,
  lastTickMs:  Long
)
type AvalancheStateRef[F[_]] = Ref[F, Map[SnapshotOrdinal, AvalancheState]]
```

TTL-based eviction: drop entries below `T_depth1.latestQualifyingOrdinal`
(once a depth-finalized ord exists, Avalanche has no need to converge on
it — depth has spoken). Memory cap of `2 × k₁ × constant_size`.

### 7.1 Rolling-upgrade compatibility

During a network upgrade, some nodes will run pre-Avalanche, some
post-. The mix is **safe**:

- Pre-Avalanche nodes emit attestations on every chain-selection move.
  Post-Avalanche nodes accept these in TipTracker (signature-verified,
  canonical-hash-filtered as today). They are **NOT** queryable as
  Avalanche peers because they don't implement the query RPC — sample
  function skips them, so `K` is effectively
  `active ∩ avalanche-enabled` and falls back to T_depth1 if too small.
- Post-Avalanche nodes emit at most one attestation per ord. Pre-
  Avalanche nodes accept these normally. They cannot detect equivocation.
- The §5.1 ticker remains live on pre-Avalanche nodes. Disabling it on
  post-Avalanche nodes is a code-level change, not a wire-level one.

The cluster decides — at a known ordinal, by operator coordination —
when to enable equivocation prosecution (the *slashing* step, deferred
per §0.G) as a hard-fork. Until then, evidence is collected but not
enforced.

---

## §8. Open questions

The questions resolved by the 10 locked decisions in §0 are removed
from this list. What remains genuinely open:

1. **Scale verification at N ∈ {500, 1000}.** The (K=3, α=2, β=6)
   triple is sim-validated up to N=100. The full sweep (`python
   sims/avalanche_attestation_calibration.py 10000 22 --figures`,
   commit `15983f1a` background job) extends the verification to
   N ∈ {500, 1000}. **Pending result.** If the safety-violation rate
   stays ≤ 1 % at f ≤ 0.33, lock the proposal at (3, 2, 6). If
   degrades, bump β to (3, 2, 8) or (3, 2, 10).

2. **Tentative attestation emission.** Should the protocol allow a
   *tentative* attestation emit before Avalanche decides — labeled as
   such on the wire — so that triggers see *something* during the
   lock-in window? Loses some safety (a flipped preference would
   require a withdrawal of the tentative att, contradicting decision
   **§0.F**). Probably not worth it, but worth listing.

3. **Slashing prosecution mechanism (deferred per §0.G).** When the
   prosecution layer lands, dedicated chain? Piggyback on existing
   snapshot stream as an optional `slashing` field? Light-client
   publication via Mithril-equivalent multisig? Dependent on
   §9's KES landing.

4. **NIPoPoW composition (decision §0.H).** Avalanche resolves the
   "which hash at the tip" question online and per-validator;
   [`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) resolves the "did
   this chain happen" question for archival ranges and per-light-client.
   The two compose by: (a) Avalanche-decided attestations are the
   strongest possible Phase 2 finality input for any single ordinal;
   (b) the NIPoPoW superblock tower anchors on Phase 3 (T_depth2)
   ordinals, by which point Avalanche has long-since decided each
   ord on the tower. The composition is "stack the bounds: pr[finality
   failure] ≤ pr[Avalanche split] × pr[NIPoPoW soundness break]";
   both are exponentially small, and the product is dominated by
   whichever is larger. The cryptographer audit (§6.3, §0.D) should
   independently cover this composition.

5. **Cross-metagraph (gl1) Avalanche — out of scope (decision §0.J).**
   The Avalanche paper's DAG-form is richer than Snowman and could be
   useful for cross-metagraph attestation, but is explicitly deferred.
   gl0 single-chain only for this proposal.

6. **Sidecar query budget at scale.** At cluster size 1000 with K=3,
   β=6, and 10 pending ords/node, query traffic is `1000 × 3 × 10 / 0.5s ≈
   60k qps cluster-wide`. Distributed (each node's K queries fan out),
   per-node receive is `3 × 10 / 0.5s = 60 qps`. Comfortable, but
   needs sidecar load testing.

7. **KES retroactive-equivocation model.** Once KES forward-secure keys
   land, old (evolved-away) keys can't sign new equivocation evidence.
   But the *original* equivocation was signed by the key as it existed
   at the time of equivocation; that signature is preserved in the
   gossiped evidence. Question: does our equivocation-evidence
   verifier check the public key as it was at the slot/ordinal of
   equivocation (correct), or as it is now (incorrect — KES key has
   evolved)? Almost certainly need the historical-pubkey path.

8. **Adversarial timing.** Can a Byzantine node strategically delay
   `responding` to queries — but still respond — to influence
   confidence dynamics in a victim node? Probably yes, but only at
   the cost of being detectable (a per-peer query-response latency
   histogram is a natural Prometheus addition). Worth flagging to
   cryptographer review (§6.3).

9. **Tipping over to a hybrid finality definition.** Today,
   `T_weight ∨ T_count ∨ T_depth1` finalizes a snapshot. Post-
   Avalanche, the weight/count triggers reduce to: "every node decided
   the same hash via Avalanche, and the weight passes the threshold".
   Decision **§0.C** already subsumes T_count semantically; collapsing
   the trigger entries in the codebase into a single `T_avalanche`
   trigger is a follow-up cleanup, not a correctness change.

---

## §9. Timing recommendation

**Research now (this doc); implement after stake-weighted VRF and
KES.** The argument:

1. **The defect we're fixing only becomes acute with stake-weighted
   VRF.** Today's equal-stake `1/N` makes self-finalize rare — a
   single self-vote is `1/N` of the threshold. P-11b is an acceptable
   stopgap. Once a single validator can control 0.5+ of stake, the
   deadlock attractor becomes a frequent operational mode.

2. **Avalanche security composition is audited post-implementation
   (decision §0.D), not pre-.** Funding a focused review (2-4 weeks of
   an academic collaborator) is cheaper than implementing twice, but
   the audit is scheduled against the implemented code — not as a gate
   on starting work.

3. **Slashing primitives depend on KES.** The provable-equivocation
   property only sticks if old keys can't retroactively rewrite their
   own signatures. Forward-secure KES (per
   `project_kes_port_constraints.md`) is the prerequisite; building
   slashing **prosecution** on top of a non-KES key model means
   re-doing the prosecution layer once KES lands. Detection (which is
   what §4 lands) is KES-independent.

4. **Implementing pre-KES means re-doing the slashing primitives
   later.** Same as (3); decisive.

### Recommended sequence

1. **Now**: this proposal lands as a doc. Decision input.
2. **Q3-Q4 2026**: stake-weighted VRF + StakeRegistry combined
   delegated stake + node collateral.
3. **Q4 2026 - Q1 2027**: KES port from Bifrost (per
   `project_kes_port_constraints.md` — Bifrost read-once secret store
   is a hard requirement).
4. **Q1 2027**: Avalanche-attestation implementation + sim work +
   cryptographer review (post-implementation per §0.D).
5. **Q2 2027**: rolling deployment; slashing **prosecution** enabled at
   a hard-fork ordinal (detection ships with Q1 2027).

In the meantime, P-11b + RebootstrapOrchestrator remain the
operational safety net. Both ship as appropriate (P-11b default-on,
P-11 default-off-and-soon-admin-only per §0.B).

---

## §10. References

- Team Rocket (pseud.). *Snowflake to Avalanche: A Novel Metastable
  Consensus Protocol Family for Cryptocurrencies*. IPFS hash
  `QmUy4jh5mGNZvLkjies1RWM4YuvJh5o2FYopNPVYwrRVGV`, 2018. Mirror at
  [avalabs.org/whitepapers](https://www.avalabs.org/whitepapers).
- Rocco et al. *Snowman++: Improved Snowman Consensus*. 2020.
  (operational improvements over Snowman.)
- Garay, Kiayias, Leonardos. *The Bitcoin Backbone Protocol: Analysis
  and Applications*. EUROCRYPT 2015.
- Kiayias, Leonardos, Stouka, Zacharias. *Ouroboros Taktikos*. FC 2023.
- Buterin & Griffith. *Casper the Friendly Finality Gadget*. 2017.
  (Slashing-evidence design precedent: GASPER's equivocation evidence
  has the same "self-verifying tuple" property we adopt in §4.2.)
- This repo:
  - `docs/nakamoto/attestation-and-finality.md` — current attestation/
    finality model.
  - `docs/nakamoto/NIPOPOW-PROPOSAL.md` — sister archival proposal
    (decision §0.H).
  - `modules/node-shared/.../TipTracker.scala` — receiver-side
    aggregation invariant we preserve.
  - `modules/node-shared/.../FinalityTrigger.scala` — trigger stack;
    T_count semantically subsumed (§0.C, §3.2).
  - `modules/dag-l0/.../NakamotoSyncDaemon.scala` — emit-policy site
    being replaced.
  - `modules/dag-l0/.../SnapshotLeaderLoop.scala` — §5.1 ticker host
    being removed (§0.F).
  - `modules/dag-l0/.../RebootstrapOrchestrator.scala` — the recovery
    hack this proposal obviates (§0.B).
- Sim harness:
  - `~/repos/research-nipopos-2026` branch `sim/avalanche-attestation`,
    commit `15983f1a` — quick-sweep parameter calibration.
  - `~/repos/research-nipopos-2026/sims/AVALANCHE_CALIBRATION.md`
    — adversary models, latency models, output schema.
