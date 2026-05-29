# Execution Sharding — Committee-Verify-Trust Design

**Status:** DESIGN FOR REVIEW (2026-05-28). Supersedes the producer-side assumptions in
`HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §6/§12 where they conflict; that doc remains the
schema/edge-case reference.

**One-line goal (user, verbatim intent):** Algorand-style VRF committees own a *bucket* of
metagraphs; **only the committee re-executes** that bucket's state transitions; every other gl0
node and downstream consumer does **crypto verification only** and **trusts the claimed state**;
**slashing is added later** to harden the trust.

---

## 1. Why this doc exists (what last night's e2e proved)

The 8gl0+4mg+4shards e2e failed at the multi-metagraph test: **gl0 saw 0/4 metagraphs**. Root cause
(from gl0 node logs): the shard checkpoint **producer is never invoked** — `verifyEmbedded`=0,
`produce()`=0. The producer sources its input from `signed.value.stateChannelSnapshots` — gl0's
*own*, post-chain-link, committed SC map (`SnapshotLeaderLoop.scala:1579`,
`ShardCheckpointFanOut.run` scaladoc). Two consequences:

1. The `CHANGE-3` filter (Axis-1a) empties that map at `numShards>1` → the fan-out's `traverse`
   no-ops → no checkpoint → nothing to adopt → it stays empty. **Circular starvation,
   unbootstrappable.**
2. Even without the filter, because the producer feeds off gl0's chain-link output, it **inherits
   the #259 freeze**: when gl0's chain-link stalls (tip divergence), `stateChannelSnapshots` stops
   advancing → the producer starves → no checkpoint can bypass the freeze.

See `diagrams/shard-current-broken.png`. **The adopt side (Axis-1a `verifyEmbedded`) is correct but
starved.** The fix is architectural: invert the producer's input.

---

## 2. The inversion (target architecture)

See `diagrams/shard-inverted-flow.png`.

> Today: `metagraph → gl0 chain-link → stateChannelSnapshots → producer (reads gl0 output)`
> Target: `metagraph → shard-committee topic (direct) → committee chain-links + re-executes → checkpoint → gl0 verifies envelope + trusts`

**gl0 never chain-links raw metagraph binaries for sharded MGs.** The metagraph (ml0) gossips its
binaries directly to its shard's committee topic. The committee — a VRF-sortitioned subset of gl0
operators — does the chain-link admission *itself*, re-executes the per-MG slice, and signs a
checkpoint. Every other gl0 node + consumer verifies the committee-signature threshold over the
checkpoint bytes and adopts the claimed state without re-executing. This is the Polkadot
collator→backing-group→relay-chain shape (§4).

This is also the **full sharding payoff**: non-committee gl0 ops shed the per-binary load (they
process one signed checkpoint per shard per ord, not every metagraph binary).

---

## 3. What we already have (don't rebuild)

| Built | Where | Reuse as |
|---|---|---|
| VRF per-operator-key sortition | `CommitteeSortition` (S1-S3) | committee selection (§5.1) |
| Static bucketing `hash(addr) mod numShards` | `ShardAssignment.shardIdFor` | metagraph→shard map |
| `ShardCheckpoint` / `ShardDerivedStateDelta` schema | `schema/sharding` | the "claim" object (§5.2) |
| Per-shard chain store + finality triggers | `ShardChainStore`, `ShardFinalityTriggers` | shard mini-chain |
| Deterministic adopt-verifier (sig-threshold) | `ShardCheckpointGl0AcceptanceManager.verifyEmbedded` | v1 acceptance gate (§5.3) |
| Chain-link admission algorithm | `GlobalSnapshotStateChannelAcceptanceManager` (`onlyPossibleReferences`) | **move into the committee** as a shard-internal helper (§6.2) |
| Equivocation slash + non-participation counter | `slashing/ShardCheckpointEquivocation*`, `ShardNonParticipationCounter` | first objective slash + liveness kickout (§5.6) |
| KES forward-secure signing | `KesSigner` | committee attestation signing |

The gap is **the producer's input + the chain-link location**, not the committee primitives.

---

## 4. Cross-network grounding (learn, don't invent)

| Network | Validity guarantee | DA guarantee | Slashing | Posture |
|---|---|---|---|---|
| **Algorand** | sig-threshold of VRF-sortitioned stake (verifiers don't re-exec) | none | none | pure sig-threshold; safe only via whole-network honest-majority |
| **Polkadot / ELVES** | small backing re-exec **+ VRF approval-checkers re-exec a random sample** + disputes | **deterministic ⅓+1 chunk-holding gate** | **~100% false-valid**, rollback | **hybrid — the gold standard for our model** |
| **NEAR Nightshade 2.0** | **every chunk re-executed by ⅔-stake sample over a state witness, *before* inclusion** | witness erasure-coded in distribution | kickout (no fraud proof needed) | **validity-before-inclusion**, no challenge window |
| **Ethereum** | (rollups' problem) | **DAS / PeerDAS (statistical)** | consensus faults only | DA primitive only |
| **Celestia / EigenLayer** | — / per-AVS | DAS + bad-encoding fraud proof / — | — / **objective+intersubjective** | DA + slashing decomposition |

**Lessons we adopt:**
- **Algorand** → committee *selection*: VRF self-sortition with the proof attached so any node
  verifies membership; **re-sortition per epoch** (player-replaceability — the committee owning a
  bucket is unpredictable until it acts).
- **Polkadot/ELVES** → the *exact layered shape* we want: compact on-chain receipt (our checkpoint)
  + **post-hoc-unpredictable** VRF-sampled approval-checkers + disputes + **asymmetric slashing**
  (~100% for signing a wrong post-state-root, ~0% for honest dissent) + rollback. This is our v1→v2
  path drawn for us.
- **NEAR** → **state-witness / stateless verification**: ship a partial-trie witness with the
  checkpoint so a verifier re-derives the post-state-root from O(touched) data without holding the
  bucket's state — and **validity-before-inclusion** (admit only with ⅔ endorsement, never roll
  back). This maps directly onto our MPT-primary subtree-stub substrate.
- **Ethereum/Celestia** → erasure-code + sampling for the v2 DA layer (so a fraud proof is always
  constructable). Prefer Polkadot's *deterministic* ⅓+1 hold-gate over statistical sampling at our
  small validator scale.
- **EigenLayer** → bond stake **per bucket**; slash on **objective, evidence-carrying** faults
  only; defer anything needing human judgment.

**The one coupling that dominates everything** (see `diagrams/shard-security-layering.png`):
*committee size is a function of whether the v2 backstop exists yet.*
- Algorand uses **large** one-shot committees (cert exp≈1500) precisely because there is **no
  escalation fallback** — the single sample must clear honest-majority alone.
- Polkadot uses **tiny** samples (backing≈5, approval≈25/~17 honest) **only because** selection is
  post-hoc-unpredictable **and** disputes escalate to all validators.
- **Us:** at 8 operators / 4 shards a per-bucket committee is small. A bare v1 sig-threshold over a
  small committee is **not** production-secure (our own cross-shard CQ-collapse bound: α_total >
  1/(2S) breaks per-shard honest-majority). v1 is a **functional/liveness milestone** (honest
  testnet); production security needs v2.

---

## 5. The layered model

### v1 — committee sig-threshold trust (ships first; unblocks #259)

1. **Selection** (Algorand + NEAR): VRF self-sortition of gl0 operator keys into a per-bucket
   committee, **stake-weighted**, **re-sortitioned per epoch** (drawn from N-2 stake, matching the
   existing staggering), VRF proof attached for membership verification. *(Have: `CommitteeSortition`.)*
2. **The claim** (Polkadot candidate receipt): the committee signs a compact `ShardCheckpoint`:
   bucket id, parent-checkpoint hash, **pre-state-root + post-state-root**, `includedSnapshots`
   (the chain-linked binary list), and `derivedStateDelta`. Canonical bytes hashed via `Hasher[F]`.
3. **Acceptance gate** (NEAR validity-before-inclusion): gl0 admits a checkpoint **only with >⅔
   endorsement by the bucket's committee stake** — `verifyEmbedded` (already built, deterministic).
   The accepted set is a **pure function of (checkpoint bytes, VRF-verified sigs, threshold)** →
   every honest node admits byte-identically → no divergence (our determinism invariant). **No
   re-execution by non-committee in v1.**

### v2 — availability + fraud-proof / slashing (hardens trust; lets committees shrink)

4. **DA gate** (Polkadot, deterministic): erasure-code the checkpoint's underlying data; block
   checkpoint finality until ≥⅓+1 of a defined set hold a chunk — so a dispute is always
   constructable.
5. **Approval-checking** (ELVES — the key inheritance): a small VRF-sortitioned set of
   **non-committee** approvers, selected **after** the checkpoint is signed (unpredictable),
   re-executes a random subset and votes approve or **raises a dispute**; escalate (more checkers)
   on no-show/disagreement.
6. **Disputes + slashing** (Polkadot schedule + EigenLayer decomposition): resolve at ⅔; **objective,
   evidence-carrying**, asymmetric slashing (~100% false-valid via re-execution-over-witness as the
   evidence; ~0% honest dissent) + equivocation slash *(have)* + rollback/re-derive on
   proven-invalid.

**Strongest end-state (NEAR alternative to a challenge window):** ship a **state witness** with the
checkpoint; the committee (or post-hoc approvers) re-execute over the witness **before inclusion**.
Converts the blind sig-threshold into validity-before-inclusion, removes rollback, and fits our
MPT-primary substrate. **Recommendation:** design the checkpoint so the witness can be added without
a schema break, so v2 can choose challenge-window *or* witness-before-inclusion empirically.

---

## 6. Concrete rewiring (the implementation, addressing the code-map barriers)

The research mapped five barriers; here's how each resolves.

### 6.1 Producer input: move production to the raw-intake point
**Barrier:** production runs in `SnapshotLeaderLoop.onSlotWon` on the already-validated snapshot; raw
binaries are only reachable async in `NakamotoSyncDaemon`'s gossip handler.
**Fix:** the metagraph gossips binaries to a **per-shard committee topic** (`shard-checkpoint-<id>`,
already specified §6.4). The committee buffers raw binaries in the **shard's fork-DAG store**
(design §12.1 — no separate orphan buffer). The producer reads from this buffer, **not** from
`stateChannelSnapshots`. Production triggers on the shard's own cadence (`T_checkpoint`/`T_burst`/
`T_alive`), decoupled from the gl0-leader win.

### 6.2 Chain-link moves into the committee (shard-internal)
**Barrier:** `onlyPossibleReferences` runs on gl0 only, anchored on gl0's
`lastStateChannelSnapshotHashes`.
**Fix:** keep `GlobalSnapshotStateChannelAcceptanceManager` as a **shard-internal helper** (design
§12.4); the committee runs it against the **prior checkpoint's per-MG tip**
(`priorCheckpoint.derivedStateDelta.includedSnapshots(mg).last.hash`). Expose that tip on the
`ShardChainStore` API (currently absent). Deterministic: same algorithm, same anchor → all committee
members build identical `includedSnapshots`.

### 6.3 Full per-MG slice (the §6.2 transcript)
The committee fills the **whole** `derivedStateDelta` (currency re-exec, token-lock balances,
artifacts, sync data, per-MG MPT root) — not just `perMetagraphMptRoots` + `includedSnapshots`. gl0
then **adopts without re-deriving** (the execution-sharding payoff). *(Today `assembleDelta` leaves
the other delta fields empty and gl0 re-derives — acceptable as an interim 6.3a step, but the goal
is gl0 skips re-exec entirely.)*

### 6.4 gl0 verify + adopt + embed
gl0 runs `verifyEmbedded` (sig-threshold, deterministic) → adopts `derivedStateDelta` into the GSI →
embeds the **full** checkpoint in the global snapshot (execution-sharded: full data in gl0).

### 6.5 Retire gl0's chain-link + the incremental field
- `GlobalSnapshotStateChannelAcceptanceManager` admission is **deleted from gl0's accept path**
  (stays as the shard helper). Gate on `numShards>1`; `numShards=1` keeps the legacy path
  byte-identical (regression bar).
- **`GlobalIncrementalSnapshot.stateChannelSnapshots`** (the *incremental* field carrying the
  per-ord binaries) → readers move to `shardCheckpoints[shardId].derivedStateDelta.includedSnapshots`.
  (Distinct from **`GlobalSnapshotInfo.lastStateChannelSnapshotHashes`**, the *GSI tip hash* — that
  is retained as derived state, updated from the adopted checkpoint, no longer driving gl0
  admission.) Research found **5-7 readers**, all metagraph-exclusive (no DAG-native overlap):
  `SnapshotLeaderLoop` (outbox), `GlobalSnapshotConsensusFunctions` (proposal), `GlobalSnapshotContextFunctions`
  (validator), `StateChannelBinarySender` (cl0 push), `DataApplicationTraverse`, `CurrencySnapshotProcessor`.

### 6.6 Bootstrap is free (your insight — §15.6 drops from v1)
A joining operator can't sit on a committee until N-2 epochs after registering, by which time it has
synced gl0. Because we are **execution-sharded, not data-sharded**, the full finalized checkpoints
(incl. `includedSnapshots`) are embedded in gl0 snapshots → syncing gl0 reconstructs the entire
finalized shard chain for free. Only the sub-finality tip comes from the live shard topic — same as
gl0's own recent-tip gossip. **No separate per-shard sync protocol for v1.** (Reopens only if we
move to *data*-sharding in a future phase — then Polkadot-style availability recovery applies.)

---

## 7. Determinism (the #261 split lesson, preserved)

In v1 no non-committee node re-derives anything, so there is nothing to diverge on. The accepted set
is `f(checkpoint bytes, set of VRF-verified committee sigs, threshold)` — a pure function. Committee
members building the checkpoint must be byte-deterministic among themselves (same chain-link
admission + same per-MG re-exec → identical `signingPreimage`); a member that computes differently
simply doesn't sign (and in v2 that divergence is the slashing evidence). gl0's
`committeeMembership(shardId, epoch)` is seedlist-derived (deterministic across peers).

---

## 8. Invariants preserved (the non-negotiables)

1. Metagraphs stay **BFT internally** — we shard gl0-side execution, not metagraph consensus.
2. gl0 global chain stays **universal Taktikos** — only execution is bucketed.
3. gl0 committed state stays **byte-deterministic** across honest nodes (adoption is a crypto check,
   never a node-local re-derivation).
4. Committees are **VRF-sortitioned subsets of gl0 operators**, not a separate validator class.
5. **Evidence-based, deterministic slashing** + KES-signed attestations + MPT-primary with
   state-proof verification at gl0.

---

## 9. Slicing / implementation plan

Each slice compiles + keeps `numShards=1` byte-identical; e2e at the end of R-3.

- **R-1 — metagraph→committee intake.** Per-shard committee topic subscription; committee buffers
  raw binaries in the shard fork-DAG store. *(producer no longer reads `stateChannelSnapshots`.)*
- **R-2 — shard-internal chain-link.** Expose prior-checkpoint per-MG tip on `ShardChainStore`; run
  `GlobalSnapshotStateChannelAcceptanceManager` inside the producer against that tip to build
  `includedSnapshots`; re-exec the per-MG slice into the full `derivedStateDelta`.
- **R-3 — gl0 adopt-only + retire chain-link.** gl0 `verifyEmbedded` → adopt full delta (skip
  re-derivation) → embed; delete gl0 chain-link admission for sharded MGs; move
  `stateChannelSnapshots` readers to `includedSnapshots`. **e2e: 8gl0+4mg+4shards, expect
  token-lock-expiration PASS + multi-metagraph PASS.**
- **R-4 (v2, later) — DA + approval-checking + slashing.** Erasure-code + ⅓+1 hold-gate;
  post-hoc VRF approvers re-execute a sample; disputes + asymmetric objective slashing; optional
  state-witness for validity-before-inclusion.

---

## 10. Open decisions for review

1. **Committee size for v1.** Given no v2 backstop yet, v1 is honest-testnet-only at small
   committee sizes. Options: (a) ship v1 at `--shards=N` degenerate (whole operator set per bucket =
   max safety, no real sharding payoff) to unblock #259 + prove the pipeline, then shrink once v2
   lands; (b) ship v1 with real sortition but label it testnet-only. **Recommend (a) for the
   #259-unblock e2e, (b)/v2 for production.**
2. **v2 path:** Polkadot challenge-window (DA + post-hoc approvers + rollback) vs NEAR
   witness-before-inclusion (re-exec over witness, no rollback). **Recommend designing the
   checkpoint to allow either**; lean NEAR-witness as the end-state (fits MPT-primary, no rollback).
3. **DA mechanism (v2):** Polkadot deterministic ⅓+1 hold vs Ethereum/Celestia sampling. **Recommend
   Polkadot-style deterministic gate** at our validator scale.
4. **6.3 sequencing:** ship R-3 with gl0 still re-deriving from `includedSnapshots` (smaller change,
   fixes #259) and move the full-delta-skip-re-exec to a follow-on? Or do the full delta in R-2?

---

## Appendix — diagrams
- `diagrams/shard-current-broken.png` — today's broken coupling (circular starvation + #259 freeze).
- `diagrams/shard-inverted-flow.png` — the target inversion (committee owns the bucket; gl0 verifies envelopes).
- `diagrams/shard-security-layering.png` — v1 sig-threshold → v2 DA+approval+slashing (NEAR-witness alt); the committee-size↔backstop coupling.
