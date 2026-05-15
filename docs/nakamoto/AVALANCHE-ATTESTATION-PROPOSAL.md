# Avalanche-style subsampling for attestation decisions

**Status:** research proposal. Not scheduled for implementation. Decision
input only. Written 2026-05-15.

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
- [`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/TipTracker.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/TipTracker.scala)
  — attestation aggregation; stays.
- [`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala`](../../modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala)
  — `processValidSnapshot` / `emitAttestation` / `emitTipAttestation`; the
  emit-gating moves here.
- [`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/FinalityTrigger.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/FinalityTrigger.scala)
  — trigger stack; stays untouched.
- Rocco et al. (Team Rocket pseudonym). *Snowflake to Avalanche: A Novel
  Metastable Consensus Protocol Family for Cryptocurrencies*. 2018.
  ([https://www.avalabs.org/whitepapers](https://www.avalabs.org/whitepapers))
- ~/.claude/skills/taktikos/SKILL.md — Taktikos protocol rules.

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
not as protocol primitives but as operational recovery scaffolding.

### 1.3 What we want instead

A protocol where:
1. Each validator emits **at most one** attestation per ordinal.
2. The attestation reflects a value all honest validators are
   *probabilistically* converging on, not the validator's local bestTip.
3. Self-attestation is **safely included** in the receiver's weight sum
   (restores NID).
4. Equivocation — a validator emitting two signed attestations at the
   same ordinal with different hashes — becomes **provable misbehavior**
   suitable for slashing.
5. The fork-recovery deadlock attractor disappears as a structural
   consequence, not as a special-case mitigation.

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
- **Liveness** (probabilistic): with positive probability per round, all
  honest nodes converge to one preference; once converged, all decide
  within `β` rounds.
- **Quiescence**: no decided node ever talks again about that ordinal.

Snowman is the chain-restricted variant; Avalanche extends to a DAG.
We use Snowman (single-chain) semantics.

### 2.2 Adaptation to per-ordinal attestation

For each ordinal N that a validator observes:

```
state per (node, ord):
  preference: Hash         // init: local canonical hash at this ord, if any
  confidence: Int = 0
  decided:    Boolean = false
  initialized: Boolean = false

every Δ ms (the Avalanche tick — see §5):
  for each ord with !decided and initialized:
    K = sample K peers (uniform over the active validator set)
    R = query each peer for their preference at ord       // RPC
    R = R.filter(_ != Pending)                             // drop "I don't know yet"
    if |R| < α:
      continue                                             // not enough responses — try next tick
    majority = the hash that appears most in R
    if count(majority) >= α and majority == preference:
      confidence += 1
    else if count(majority) >= α and majority != preference:
      preference = majority
      confidence = 1                                       // restart confidence on flip
    else:
      confidence = max(confidence - 1, 0)                  // no clear majority — decay
    if confidence >= β:
      decided = true
      emit signed_attestation(ord, preference, attestedAt = now)
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
(`K / Δ` per node, e.g. `20 / 500ms = 40 qps`). At cluster size 100 with
1k ordinals in flight that's 40k qps cluster-wide, which the sidecar can
absorb.

### 2.4 Parameters

Classic Avalanche uses `K=20, α=15, β=20` over thousands of nodes. We
must re-calibrate for our cluster sizes. Indicative starting points (to
be confirmed by simulation, §8):

| Environment      | Cluster | K  | α  | β  | Tick Δ |
|------------------|---------|----|----|----|--------|
| e2e tests        | 3-8     | 3  | 2  | 8  | 250 ms |
| small mainnet    | 50-100  | 12 | 9  | 14 | 500 ms |
| large mainnet    | 500+    | 20 | 15 | 20 | 500 ms |

Constraints:
- `K ≤ active validator count`. If insufficient peers, Avalanche stalls
  → depth-k₁ fallback (T_depth1) carries us through. Liveness floor.
- `α > K/2` for safety: a tie cannot promote a value.
- `β` controls the safety-vs-latency knob (smaller = faster decision,
  larger = lower split-decision probability).

Knobs land under env (`NAKAMOTO_AVALANCHE_K` / `_ALPHA` / `_BETA` /
`_TICK_MS`) with `LddConfig`-style defaults, matching the pattern of
`NAKAMOTO_ATTESTATION_THRESHOLD` and friends.

---

## §3. Composition with the existing trigger stack

**Key invariant: the trigger stack (T_weight, T_count, T_depth1,
T_depth2) is UNCHANGED.** Triggers sum/count/depth-walk attestations to
drive Phase 1→2 and Phase 2→3. Avalanche reshapes how attestations are
produced; the consumers are unchanged.

| Component                                      | Change under Avalanche |
|------------------------------------------------|------------------------|
| `TipTracker.recordAttestation` (newer-wins)    | **STAYS**. Each validator decides once; the "newer-wins" rule sees at most one attestation per peer per ordinal anyway. |
| `T_weight` (2/3 stake weight on canonical)     | **STAYS**. Reads exactly the same TipTracker map. |
| `T_count` (2/3 distinct attesters)             | **STAYS**. Same TipTracker map. |
| `T_depth1` (depth-k₁ fallback)                 | **STAYS**. Bitcoin-style safety net for when Avalanche can't sample (insufficient active peers) or for ordinals from which Avalanche has been pruned. |
| `T_depth2` (Phase 2→3 archival)                | **STAYS**. Pure structural depth. |
| Canonical-hash filter (§6 of attestation-and-finality.md) | **STAYS** but becomes nearly trivial. Avalanche-decided attestations should already converge to the same hash; the filter remains as a defense against partition-residual attestations. |
| §5.1 RE-ATTEST visibility ticker              | **REMOVED**. The whole reason it exists — chain selection moved bestTip after we attested — does not apply: under Avalanche, the validator's attestation reflects an Avalanche-decided value, not a moving target. |
| P-11b self-exclusion (commit `95471c7f`)      | **ROLLED BACK**. Self-vote is included again. NID is restored. |
| `RebootstrapOrchestrator` (commit `01ebcca6`)  | **DEMOTED**. The structural cause of the deadlock disappears. Keep as an operator-triggered manual recovery endpoint (HTTP POST `/admin/rebootstrap`); drop the 30s tick. Useful for genuine catastrophic state corruption (disk, oncall judgment) but no longer needed as a routine self-healing mechanism. |
| `NakamotoChainStore.store` finality-safety gate (`:290-303`) | **STAYS**. Still defends against a buggy or malicious caller trying to overwrite an already-finalized ordinal. Under Avalanche the gate should never fire in normal operation (because finalization implies Avalanche agreement). |

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

---

## §4. Equivocation slashing

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

Either is **provable misbehavior** suitable for slashing.

### 4.2 Slashing-evidence shape

```
SlashingEvidence:
  validator: PeerId
  ordinal:   Long
  att1:      SignedTipAttestation    // (ord, hash₁, attestedAt₁, sig₁)
  att2:      SignedTipAttestation    // (ord, hash₂, attestedAt₂, sig₂)
  // invariant: att1.ord == att2.ord, att1.hash != att2.hash,
  //            both signatures verify under validator's pubkey
```

The evidence is **self-verifying**: any node can recompute both
signatures from `validator`'s public key. No additional context (chain
history, current bestTip, anything stateful) is needed. This is the
property that makes it gossipable, archivable, and provable by light
clients.

### 4.3 Collection and prosecution

A new daemon `EquivocationDetector` subscribes to the existing attestation
gossip topic and maintains:

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

Prosecution — actually zeroing the offending validator's stake — happens
in a future block-content acceptance pass: a global L0 snapshot may
include a `slashing` field listing equivocation evidence; honest
validators reject snapshots claiming slashes whose evidence doesn't
verify; on acceptance, `StakeRegistry` zeros the offender's stake at
ordinal N+something. Spec is out of scope here (~200 LOC for the writer
side; needs careful design); §9 argues KES must land first.

### 4.4 Pruning

Evidence older than the archival depth (`k₂` ordinals deep) can be
pruned from `attestationsSeen` after the offender has been prosecuted
(or after `T_depth2` of inaction, whichever first). Storage cost is
bounded by `|active validators| × k₁ × ~200 bytes ≈ negligible` for
even 10k-validator clusters during the live window.

---

## §5. Latency and liveness analysis

### 5.1 Decision latency

The dominant cost of Avalanche is `β` rounds × `Δ` ticks before lock-in:

```
T_decide ≈ β · Δ + RTT_query
```

Indicative numbers (with the §2.4 parameters):

| Env             | K  | α  | β  | Δ      | T_decide |
|-----------------|----|----|----|--------|----------|
| e2e tests       | 3  | 2  | 8  | 250 ms | ~2 s     |
| small mainnet   | 12 | 9  | 14 | 500 ms | ~7 s     |
| large mainnet   | 20 | 15 | 20 | 500 ms | ~10 s    |

Compare to the existing `finalityMonitor` cadence: `5 × slotDurationMs`,
i.e. 5 s prod / 2.5 s e2e (`SnapshotLeaderLoop.scala:489-490`). The
Avalanche window slightly *exceeds* the existing finality tick in prod
sizes — meaning **the first attestation a peer sees at ord N would now
arrive ~5-10 seconds later than today**. That latency is paid back by
the elimination of the §5.1 re-attestation churn.

### 5.2 Composition with T_depth1 latency

The finality stack today already gates on `T_depth1 = bestTipOrdinal - k₁`
(default `k₁ = 255` ordinals ~= 255 × 7s ≈ 30 min at prod slot rate).
Avalanche adds 10 s. Not material.

### 5.3 Liveness floor

Avalanche needs `K` active peers responding to queries within Δ to make
progress. If the cluster is partitioned below `K`, Avalanche **stalls**:

- `confidence` never reaches `β` → no decision → no attestation emitted.
- `T_weight` and `T_count` get nothing to count → don't fire.
- `T_depth1` keeps ticking → carries the chain through finality on
  pure structural depth.

This is **good**. Pre-Avalanche, the cluster could keep emitting
attestations on divergent chains and self-finalize them (the #119
attractor). Post-Avalanche, the partitioned cluster simply waits for
depth-k₁ — exactly the desired safety property.

### 5.4 Interaction with §5.1 re-attestation ticker

§5.1 was added because: pre-Avalanche, chain selection moves the local
bestTip after we emit; the canonical-hash filter then zeros our weight;
the ticker rescues the contribution by re-emitting. Post-Avalanche, the
validator's emitted attestation reflects an Avalanche-decided value,
not local-bestTip-at-time-of-emit. Chain selection moving local bestTip
no longer affects the validator's attestation contribution.

→ §5.1 is **obsolete** under Avalanche. Remove the ticker block from
`SnapshotLeaderLoop.finalityMonitor` (saving the 5s scan + RE-ATTEST
log line on every tick).

### 5.5 Slot/Avalanche interleave

Decided ordinals stop being touched (the protocol is quiescent on
decided ordinals). Ordinals still pending — typically the most recent
`β + ε` ordinals — receive query traffic. Per node, query budget per
tick is `K × |pending ordinals|`; with K=20 and ~10 pending ordinals
that's 200 qps per node. Well within the sidecar's capacity.

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

For K=20, α=15, f=1/3: `p ≈ 0.999`; with β=20: split probability
< 10⁻⁶⁰. The bound is extremely strong against passive Byzantine.

### 6.2 What Taktikos gives us (probabilistic)

Taktikos's safety follows from the GKL backbone-protocol analysis:
common-prefix violation after k₁ ordinals occurs with probability
≤ ε(k₁, fA, fB, ψ, γ), modeled in `~/repos/research-nipopos-2026`'s
sim/adv-7block-private branch. At our default `k₁ = 255`, ε is
cryptographically negligible against an adaptive 1/3 adversary.

### 6.3 The composition is novel

Stacking Avalanche on top of Taktikos LDD has not been studied in the
literature. **The novelty is the security concern.** Specific worries
worth raising with a cryptographer:

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
   `TDepth1Trigger.make` in FinalityTrigger.scala:208-217.

3. **Adaptive Byzantine.** Avalanche's safety bound assumes the
   adversary fraction `f` is fixed across the decision window. Our
   stake-weighted VRF model allows stake to shift between periods — but
   slowly relative to a 10s decision window. Not a worry.

4. **Grinding.** Avalanche queries return preferences, not VRF outputs.
   No additional VRF grinding surface introduced.

5. **Targeted-query attack.** An adversary controlling the topology
   (eclipse) could feed a victim node a biased sample. Mitigated by
   uniform random sampling over `stakeRegistry.activeValidators` (not
   over a learned peer set). Eclipse hardness is a separate concern,
   present pre-Avalanche too.

### 6.4 Bottom line

The Avalanche layer adds a *probabilistic* per-ordinal agreement
guarantee that today's protocol lacks. It does **not** weaken Taktikos's
chain-growth or common-prefix properties; the layers are orthogonal.
**The combined protocol's security needs cryptographer review** before
production deployment — not because we expect a problem, but because
unstudied compositions sometimes leak unexpected attacks.

---

## §7. Implementation outline (sizing only — NOT a commitment)

The shape, scoped to "research now, build later":

| Component | LOC est. | Location |
|---|---|---|
| `AvalancheAttestationDecider[F]` daemon | ~500 | `modules/dag-l0/.../nakamoto/AvalancheAttestationDecider.scala` |
| `AvalancheState` per-ord case class | ~50  | same |
| Query/Response RPC types on sidecar | ~100 | `node-shared/.../nakamoto/SidecarClient.scala` + proto |
| Sidecar Go gRPC wiring | ~150 | sidecar repo |
| `EquivocationDetector[F]` daemon | ~300 | `modules/dag-l0/.../nakamoto/EquivocationDetector.scala` |
| Slashing-evidence ledger + proto | ~200 | new |
| `processValidSnapshot` rewire (seed Avalanche, suppress immediate emit) | ~50 | `NakamotoSyncDaemon.scala` |
| `SnapshotLeaderLoop` removal of §5.1 ticker | -30 | `SnapshotLeaderLoop.scala` |
| `RebootstrapOrchestrator` demotion (drop tick, expose admin route) | -100 | `RebootstrapOrchestrator.scala` |
| Config knobs `NAKAMOTO_AVALANCHE_K/ALPHA/BETA/TICK_MS` | ~30 | scattered |
| Unit tests (pure decider) | ~400 | `dag-l0/src/test/...` |
| Sim harness extension | ~300 | `~/repos/research-nipopos-2026` sim/ |
| **Total** | **~2000 LOC + sim work** | |

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
  function skips them, so K is effectively `active ∩ avalanche-enabled`.
- Post-Avalanche nodes emit at most one attestation per ord. Pre-
  Avalanche nodes accept these normally. They cannot detect equivocation.
- The §5.1 ticker remains live on pre-Avalanche nodes. Disabling it on
  post-Avalanche nodes is a code-level change, not a wire-level one.

The cluster decides — at a known ordinal, by operator coordination —
when to enable equivocation prosecution (the *slashing* step) as a
hard-fork. Until then, evidence is collected but not enforced.

---

## §8. Open questions

1. **K / α / β calibration.** The §2.4 numbers are pulled from
   intuition. They need to be sim-validated against:
   - Cluster sizes 3, 5, 8, 100, 500.
   - Adversary fractions f ∈ {0, 0.1, 0.2, 0.33}.
   - Partition scenarios (50/50 split, 70/30, 90/10).
   - Network latency distributions (50, 200, 500, 1000ms p99 RTT).

2. **Tentative attestation emission.** Should the protocol allow a
   *tentative* attestation emit before Avalanche decides — labeled as
   such on the wire — so that triggers see *something* during the
   lock-in window? Loses some safety (a flipped preference would
   require a withdrawal of the tentative att). Probably not worth it,
   but worth listing.

3. **Slashing-evidence handling.** Dedicated chain? Piggyback on
   existing snapshot stream as an optional `slashing` field? Light-
   client publication via Mithril-equivalent multisig? Dependent on
   §9's KES landing.

4. **Stake-weighted vs uniform sampling.** Classic Avalanche samples
   uniformly. Stake-weighted sampling would give large validators
   correspondingly larger influence — but Avalanche's safety argument
   is for uniform K-sample. Stake-weighting the sample is a
   modification that needs independent analysis. Default: **uniform**;
   the stake-weight property lands at the *trigger* layer (T_weight),
   not the *decision* layer.

5. **KES retroactive-slashing model.** Once KES forward-secure keys
   land, old (evolved-away) keys can't sign new equivocation evidence.
   But the *original* equivocation was signed by the key as it existed
   at the time of equivocation; that signature is preserved in the
   gossiped evidence. Question: does our equivocation-evidence
   verifier check the public key as it was at the slot/ordinal of
   equivocation (correct), or as it is now (incorrect — KES key has
   evolved)? Almost certainly need the historical-pubkey path.

6. **DAG-level Avalanche.** This proposal uses Snowman (chain-form).
   The Avalanche paper's DAG-form is richer but adds complexity. Could
   it be useful for cross-metagraph (gl1) attestation? Out of scope
   here.

7. **Sidecar query budget.** At cluster size 1000 with K=20, β=20, and
   10 pending ords/node, query traffic is `1000 × 20 × 10 / 0.5s ≈
   400k qps cluster-wide`. Distributed (each node's K queries fan out),
   per-node receive is `20 × 10 / 0.5s = 400 qps`. Comfortable, but
   needs sidecar load testing.

8. **Interaction with the Phase-3 light-client anchor.** Mithril-style
   aggregate-signature certificates land at T_depth2 (Phase 2→3, k₂
   deep). Avalanche decisions occur much earlier (close to T_weight /
   T_count cadence). Is there value in an Avalanche-attested
   certificate at depth-k₁ for medium-trust light clients? Probably,
   but separate workstream.

9. **Tipping over to a hybrid finality definition.** Today,
   `T_weight ∨ T_count ∨ T_depth1` finalizes a snapshot. Post-
   Avalanche, the weight/count triggers reduce to: "every node decided
   the same hash via Avalanche, and the weight passes the threshold".
   An honest cluster *should* see weight and count fire simultaneously
   once `2/3 · β · Δ ≈ 7s` after first arrival. Should we collapse
   the weight + count triggers into a single `T_avalanche` trigger
   that just counts decided-attestations and reports the
   threshold-crossing ordinal? Cleaner spec, but requires the trigger
   stack to inspect per-attestation "decided" status — currently the
   wire format has no such field. Either add it or leave the triggers
   alone. Either way, the choice doesn't change correctness.

10. **Adversarial timing.** Can a Byzantine node strategically delay
    `responding` to queries — but still respond — to influence
    confidence dynamics in a victim node? Probably yes, but only at
    the cost of being detectable (a per-peer query-response latency
    histogram is a natural Prometheus addition). Worth flagging to
    cryptographer review.

---

## §9. Timing recommendation

**Research now (this doc); implement after stake-weighted VRF and
KES.** The argument:

1. **The defect we're fixing only becomes acute with stake-weighted
   VRF.** Today's equal-stake `1/N` makes self-finalize rare — a
   single self-vote is `1/N` of the threshold. P-11b is an acceptable
   stopgap. Once a single validator can control 0.5+ of stake, the
   deadlock attractor becomes a frequent operational mode.

2. **Avalanche security composition needs cryptographer review.**
   The combined Taktikos + Avalanche security argument is not in the
   literature. Funding a focused review (2-4 weeks of an academic
   collaborator) is cheaper than implementing twice.

3. **Slashing primitives depend on KES.** The provable-equivocation
   property only sticks if old keys can't retroactively rewrite their
   own signatures. Forward-secure KES (per
   `project_kes_port_constraints.md`) is the prerequisite; building
   slashing on top of a non-KES key model means re-doing the slashing
   layer once KES lands.

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
   cryptographer review.
5. **Q2 2027**: rolling deployment; slashing prosecution enabled at a
   hard-fork ordinal.

In the meantime, P-11b + RebootstrapOrchestrator remain the
operational safety net. Both ship default-on (P-11b) and default-off
(P-11) as appropriate.

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
  - `modules/node-shared/.../TipTracker.scala` — receiver-side
    aggregation invariant we preserve.
  - `modules/dag-l0/.../NakamotoSyncDaemon.scala` — emit-policy site
    being replaced.
  - `modules/dag-l0/.../RebootstrapOrchestrator.scala` — the recovery
    hack this proposal obviates.
