# Avalanche-style subsampling for attestation decisions

**Status:** research proposal. Decision input. Implementation deferred behind
stake-weighted VRF + KES. Written 2026-05-15. Revised 2026-05-15 to fold in
locked decisions and the quick-sweep sim recommendation from
[`~/repos/research-nipopos-2026` commit `15983f1a`](#). Revised again
2026-05-15 to flip cascade semantics from **Snowflake → Snowball** (per-color
persistent confidence accumulator, decide by margin) and fold in two
Frosty-derived insights — see §0 TL;DR and §2.2 / §10A. Revised again
2026-05-15 to repoint all sim references to the **full 1896-cell × 10000-trial
GPU sweep** (`~/repos/research-nipopos-2026/sims/data/avalanche_attestation_full_gpu_n10000.json`,
commit `d8f4639`, 7.1 min wall-clock on RTX 5090) and to fold in the new
**split_honest** finding that turns the §2.2 Snowflake → Snowball flip from a
theoretical robustness gain into an empirical necessity at scale (§0.K.2,
§2.4, §6.1).

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
- Lewis-Pye, Buchwald, Buttolph, O'Grady, Sekniqi. *Frosty: Bringing
  strong liveness guarantees to the Snow family of consensus protocols*.
  arXiv:2404.14250, 2024. ([html](https://arxiv.org/html/2404.14250v5))
  — Snowflake+ dual-threshold variant + epoch-change liveness module.
- Amores-Sesar, Schneider. *An Analysis of Avalanche Consensus*.
  arXiv:2401.02811, 2024. — independent Snowflake / Snowball / Avalanche
  formal-model analysis. Cited in §6.1 for the safety bound delta.
- [`~/repos/research-nipopos-2026/sims/AVALANCHE_CALIBRATION.md`](../../../research-nipopos-2026/sims/AVALANCHE_CALIBRATION.md)
  — sim harness, branch `sim/avalanche-attestation`. The data referenced
  throughout is the full 1896-cell × 10000-trial GPU sweep at
  `~/repos/research-nipopos-2026/sims/data/avalanche_attestation_full_gpu_n10000.json`
  (commit `d8f4639`, 7.1 min wall-clock on RTX 5090).
- `~/.claude/skills/taktikos/SKILL.md` — Taktikos protocol rules.

---

## §0. Locked decisions (TL;DR)

The 12 decisions below are settled. The rest of the doc derives from them.

| # | Decision | Where it lands |
|---|---|---|
| A | **Tick cadence = `slot/2`**, computed deterministically via `Ratio[BigInt]` so every node derives the same Δ from `slotDurationMs`. No floating-point. | §2.4, §5.1 |
| B | **`RebootstrapOrchestrator` is demoted** to a CLI flag / admin HTTP endpoint. Not part of the auto-runtime. #141's default-OFF flag becomes "explicit admin trigger only" once this proposal lands. | §3 table, §0.B note |
| C | **T_count is subsumed by β.** Observing β peers all preferring hash H is *effectively* a per-node T_count-equivalent local quorum. β is set proportionally; T_count's wire-level evaluation collapses into the cascade trigger. | §3.2 |
| D | **Audit-after-implementation.** The combined Taktikos + Avalanche security argument is novel; cryptographer review happens against the implemented protocol, not as a pre-implementation gate. | §6, §9 |
| E | **Symmetric rollback.** When a node's cascade flips its locally-preferred hash, the state-application rollback is symmetric to the application path (the same MPT-overlay primitives that move state forward also move it back). | §3.3 |
| F | **Emit-once.** The decided-attestation is emitted exactly once, when the Snowball accumulator margin first clears β (formerly: the Snowflake counter cleared β). No re-attestation under fluctuating peer-set views. The §5.1 visibility ticker is removed entirely. | §2.2, §5.4 |
| G | **No slashing in this proposal.** Equivocation **detection** produces self-verifying evidence (§4.2); what to do with the evidence (zero stake, freeze, ignore) is a separate workstream. | §4.3, §4.5 |
| H | **NIPoPoW is the archival path.** Out of scope here (see [`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md)). Avalanche-attestation and NIPoPoW are orthogonal: Avalanche answers "which hash" at the tip; NIPoPoW answers "did this chain happen" for any archival range. | §8.4 |
| I | **Uniform peer sampling** over the active stake-registry set. Stake-weighted sampling is rejected: classic Avalanche's safety bound is for uniform K-sample. Stake-weight already lands at the trigger layer (T_weight). | §2.2, §2.3, §6.3 |
| J | **Single-chain (gl0) scope.** Cross-metagraph (gl1, currency-l1) attestation is out of scope. | §8.5 |
| **K** | **Snowball semantics (not Snowflake).** Per-color persistent confidence accumulator; decide hash H when accumulator(H) exceeds the second-highest accumulator by margin β. Robust to adversarial timing of split queries — accumulated history cannot be reset by a single flip. Confirmed-defective in our prior text (§2.2) and in the sim harness, which is also Snowflake; both must be re-implemented. | §2.2, §2.4 |
| **L** | **Frosty as future work, not blocker.** The Frosty paper (Lewis-Pye et al. 2024) supplies a dual-threshold Snowflake+ variant and an epoch-change liveness module. The dual-threshold idea is a candidate refinement for our Snowball cascade (§10A.1); the liveness module is largely subsumed by our existing T_depth1 / T_depth2 stack (§10A.2) but flagged as cross-link. | §10A |

### 0.A — Sim recommendation (full 1896-cell × 10000-trial GPU sweep, commit `d8f4639`)

The harness at
`~/repos/research-nipopos-2026/sims/avalanche_attestation_calibration.py`
implements the Snowman variant of Avalanche at per-validator per-ordinal
granularity, with two Byzantine adversary modes — `coordinated_lie`
(every Byzantine peer reports a third hash `HASH_LIE` to drain honest
confidence) and `split_honest` (Byzantine peers strategically mirror the
victim's *minority* preference to keep the cascade flipping; the
Snowflake-killer attack) — and a 50/50 initial honest split. The full
GPU sweep
(`~/repos/research-nipopos-2026/sims/data/avalanche_attestation_full_gpu_n10000.json`,
1896 cells × 10000 trials/cell, 7.1 min wall-clock on RTX 5090, max
200 rounds, Boltzmann (`δ̄ = 50 ms`) and Pareto latency,
`tick_dt = 250 ms`, branch `sim/avalanche-attestation`, commit
`d8f4639`) reports the following headlines (coordinated_lie, boltzmann):

| N    | f_adv | (K, α, β)        | converge | safety_viol | p50 / p99 rounds |
|------|-------|------------------|----------|-------------|------------------|
| 5    | 0.33  | (3, 2, 10)       | 1.0000   | 0.0000      | 11 / 12          |
| 8    | 0.33  | (3, 2, 10)       | 1.0000   | 0.0000      | 11 / 13          |
| 16   | 0.33  | (3, 2, 10)       | 1.0000   | 0.0000      | 12 / 14          |
| 32   | 0.33  | (3, 2, 10)       | 1.0000   | 0.0000      | 12 / 13          |
| 100  | 0.33  | (3, 2, 10)       | 1.0000   | 0.0000      | 12 / 13          |
| 500  | 0.33  | (3, 2, 10)       | 1.0000   | 0.0000      | 13 / 13          |
| 1000 | 0.33  | (3, 2, 10)       | 1.0000   | 0.0000      | 13 / 13          |
| 32   | 0.33  | **(20, 15, 20)** | **0.0000** | 0.0000    | DNF (200 cap)    |
| 100  | 0.33  | **(20, 15, 20)** | **0.0000** | 0.0000    | DNF (200 cap)    |
| 500  | 0.33  | **(20, 15, 20)** | **0.0000** | 0.0000    | DNF (200 cap)    |
| 1000 | 0.33  | **(20, 15, 20)** | **0.0000** | 0.0000    | DNF (200 cap)    |
| 8    | 0.20  | (3, 2, 6)        | 1.0000   | **0.0132**  | 8 / 23           |
| 16   | 0.20  | (3, 2, 6)        | 1.0000   | **0.0700**  | 10 / 36          |
| 32   | 0.20  | (3, 2, 6)        | 0.9999   | **0.0843**  | 10 / 36          |
| 100  | 0.20  | (3, 2, 6)        | 1.0000   | 0.0120      | 10 / 12          |
| 500  | 0.20  | (3, 2, 6)        | 1.0000   | 0.0000      | 10 / 11          |
| 1000 | 0.20  | (3, 2, 6)        | 1.0000   | 0.0001      | 11 / 11          |

Reading:
- **(K=3, α=2, β=10) is the universal winner under coordinated_lie at
  f=0.33**: 100 % convergence and 0 safety violations across the entire
  cluster-size range N ∈ {5, 8, 16, 32, 100, 500, 1000} with 10000 trials
  per cell. Median 11-13 rounds.
- **(K=20, α=15, β=20) — the literature-standard "classical Avalanche"
  triple — FAILS catastrophically at high f**: 0 / 10000 trials converge
  at f=0.33 for *every* N ≥ 32. The failure boundary in this sweep is
  f ≈ 0.25 (at N=100 the triple drops from 42.7 % convergence at f=0.20
  to 0 % at f=0.25 and above).
- **(K=3, α=2, β=6) is fast but unsafe under coordinated_lie at f=0.20**:
  up to **8.4 % safety violations** at N=32, dropping to 0–1.2 % at
  N ∈ {100, 500, 1000}. β=10 zeroes it everywhere. This is the empirical
  basis for the β=10 upgrade over the prior β=6 recommendation.

**Initial recommendation: `(K=3, α=2, β=10, Δ = slot/2)` for cluster
sizes 5–1000.** The β=10 setting (vs. the prior β=6) is empirically
required to zero coordinated_lie violations at small N; the split_honest
data in §0.K.2 makes the case stronger still and motivates the §2.2
Snowflake → Snowball flip empirically. β is **subject to re-sim under
Snowball semantics**, which is in flight as a parallel workstream —
Snowball is expected to permit lower β at equivalent safety, but this
needs the re-sim before being locked.

The defining constraint that selected (K=3, α=2) over the literature
standard: **K ≤ N - 1**. For e2e clusters (3 or 5 nodes), classical
Avalanche `K=20` is not implementable. The smaller triple is not a
tuning compromise — it is a structural requirement at our cluster
sizes. The full sweep above confirms it is also strictly faster (median
11-13 rounds vs. ≥ 200 rounds DNF) at our adversary level.

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

### 0.K — Snowflake → Snowball (decision K)

The cascade described in our earlier §2.2 and implemented in
`avalanche_attestation_calibration.py` is **Snowflake** semantics:
`if majority == preference: confidence += 1; else: preference = majority;
confidence = 1`. The confidence counter resets on every flip — there is
no per-color history. This is the variant the original Rocco et al.
paper (2018, §3.1) describes as the simplest member of the family;
Snowball (§3.2 of the same paper, and §2.2 of Amores-Sesar & Schneider's
formal analysis arXiv:2401.02811) adds **persistent per-color confidence
counters** that accumulate over the entire protocol execution. A flip
under Snowball does not erase prior accumulated evidence; the decision
rule becomes "decide H when `accum(H) − max{accum(H') : H' ≠ H} ≥ β`".

Why this matters for our setting: under an adversary that strategically
times split queries against a victim node — the canonical
`split_honest` mode in the harness, where Byzantine peers mirror the
victim's *minority* preference to keep the cascade flipping — Snowflake's
reset-on-flip is the load-bearing weakness. The sim's 0–8.4 % safety-
violation rate at f=0.20 with (K=3, α=2, β=6) under coordinated_lie
(§0.A) and **up to 71.4 % at f=0.33 under split_honest** (§0.K.2) is
the predicted signature of this attack. Snowball at the **same** β is
strictly more robust because the adversary must overcome accumulated
history, not just one round; under the split-honest mode this is the
difference between converging on a safe answer and converging on a
disagreement.

The full-sweep evidence is consistent with this prediction. The
1896-cell × 10000-trial GPU sweep
(`~/repos/research-nipopos-2026/sims/data/avalanche_attestation_full_gpu_n10000.json`,
commit `d8f4639`, 7.1 min on RTX 5090) confirms:
- the Snowflake split-attack signature at (K=3, α=2, β=6) under
  coordinated_lie (§0.A.i headline table);
- the catastrophic failure of literature-standard (K=20, α=15, β=20)
  at high f_adv across all N (§0.A.i);
- the **Snowflake-specific weakness against `split_honest`** that
  grows with cluster size — the §0.K.2 finding, which is the empirical
  basis for the §2.2 Snowflake → Snowball flip.

### 0.K.1 — Headline against the literature standard (full sweep)

| N    | f_adv | (K, α, β)        | convergence | safety_violations | med_rounds |
|------|-------|------------------|-------------|-------------------|------------|
| 32   | 0.33  | (3, 2, 10)       | 1.0000      | 0.0000            | 12         |
| 100  | 0.33  | (3, 2, 10)       | 1.0000      | 0.0000            | 12         |
| 500  | 0.33  | (3, 2, 10)       | 1.0000      | 0.0000            | 13         |
| 1000 | 0.33  | (3, 2, 10)       | 1.0000      | 0.0000            | 13         |
| 32   | 0.33  | **(20, 15, 20)** | **0.0000**  | 0.0000            | DNF (cap)  |
| 100  | 0.33  | **(20, 15, 20)** | **0.0000**  | 0.0000            | DNF (cap)  |
| 500  | 0.33  | **(20, 15, 20)** | **0.0000**  | 0.0000            | DNF (cap)  |
| 1000 | 0.33  | **(20, 15, 20)** | **0.0000**  | 0.0000            | DNF (cap)  |
| 100  | 0.20  | **(20, 15, 20)** | 0.4266      | 0.0000            | 182 / 200  |

**Headline: K=20 / α=15 / β=20 (the literature-standard "classical
Avalanche" triple) FAILS at f_adv=0.33 — 0 / 10000 trials converge at
every N from 32 to 1000, hitting the 200-round cap.** Even at f=0.20
it converges only 42.7 % of the time at N=100, and at a p50 of 182
rounds (vs. our (3, 2, 10) at 12 rounds). This is the single strongest
argument against the literature parameters for our setting and the
strongest justification for keeping (K=3, α=2) as the operating point —
with **β=10 as the empirical floor** that zeroes the
coordinated_lie violations at all measured N.

### 0.K.2 — split_honest: the empirical case for Snowball

The full sweep also evaluates the `split_honest` adversary — Byzantine
peers mirror the victim's minority preference to time-engineer flips
that reset Snowflake's `confidence` counter. Under (K=3, α=2, β=10)
at f_adv=0.33, the Snowflake-tuned harness reports a safety-violation
rate that **grows with cluster size**:

| N    | (K, α, β) | adversary    | converge | safety_violations |
|------|-----------|--------------|----------|-------------------|
| 5    | (3, 2, 10) | split_honest | 1.0000   | 0.0000            |
| 8    | (3, 2, 10) | split_honest | 0.9997   | 0.0005            |
| 16   | (3, 2, 10) | split_honest | 0.9996   | **0.0099**        |
| 32   | (3, 2, 10) | split_honest | 0.9977   | **0.0445**        |
| 100  | (3, 2, 10) | split_honest | 0.9994   | **0.0985**        |
| 500  | (3, 2, 10) | split_honest | 0.9989   | **0.1671**        |
| 1000 | (3, 2, 10) | split_honest | 0.9995   | **0.1415**        |
| 1000 | (3, 2, 6)  | split_honest | 0.9999   | **0.7136**        |

Reading: even at the empirically-tuned β=10, **Snowflake leaks
9.85–16.71 % safety violations against split_honest** in the
N ∈ {100, 500, 1000} band that matches our production-cluster sizing.
The β=6 row at N=1000 is the cautionary cell — 71.4 % violations —
demonstrating that the Snowflake confidence-reset is a load-bearing
defect of the cascade, not a tunable parameter. Raising β further
does not close the gap because the adversary can always time *more*
flips faster than the consecutive counter can climb.

**This is the empirical justification for the §2.2 Snowflake → Snowball
flip** that landed in the prior revision (decision K, §0.K). Snowball's
per-color persistent accumulator cannot be reset by a single split
query; the adversary must overcome the cumulative margin, which a
strictly-honest majority of K-samples maintains over time. The
predicted outcome of the parallel Snowball re-sim (in flight at the
time of this revision) is that the 16.71 % violation rate at N=500
collapses to ≤ 0.1 % at the same β=10 — turning the §2.2 flip from a
theoretical-robustness gain into the **operational requirement at
production scale**.

### 0.L — Frosty TL;DR (decision L; Lewis-Pye et al. 2024)

Frosty (arXiv:2404.14250) supplies two enhancements to the Snow family.
We adopt **neither directly in this proposal**; both are documented in
§10A and cross-linked to future work.

1. **Snowflake+** (dual-threshold cascade): replace single α with two
   thresholds α₁ (flip) and α₂ (count). Splits the "decide to switch
   preference" and "decide to deepen confidence" decisions into two
   distinct sample-fraction thresholds, with α₁ ≤ α₂. Used to produce a
   clean consistency proof. Our Snowball semantics (decision K) already
   resolves the dominant attack (Snowflake split); Snowflake+ is an
   orthogonal refinement that can stack on top of Snowball at low cost.
   Candidate for a Phase 2 refinement. See §10A.1.

2. **Liveness module + epoch change**: when the cascade fails to make
   progress (adversary > O(√n) attacking liveness), trigger an "epoch
   change" to a temporary quorum-based protocol. **We do not need this**:
   our T_depth1 (depth-k₁) fallback already provides a structural
   liveness floor independent of attestation flow. Our depth-k₁
   fallback is to Frosty's liveness module as Bitcoin's longest-chain
   rule is to PBFT's view change — different shape, same purpose.
   See §10A.2.

Frosty's recommended parameters (k=80, α₁=41, α₂=72, β=12, n≥500) are
tuned for a much larger validator set than ours (cf. (3, 2, 6) at N=16).
The synchrony assumption (Δ-bounded message delivery) matches our
setting at the Avalanche-tick layer. The Byzantine bound f < n/5 is
**tighter than ours** (we tolerate f ≤ 1/3 at the chain layer via
Taktikos LDD, and the Avalanche layer composes with — not replaces —
that bound). We do **not** import Frosty's f < n/5 assumption.

`fB = 0.05` in production is unchanged — Frosty has no LDD analog and
no recommendation that contradicts it.

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

The Snow family (Rocco et al. 2018; Amores-Sesar & Schneider 2024) is
a sequence of metastable consensus primitives, each adding state to the
previous:

- **Slush**: each node repeatedly queries a random K-sample of peers and
  adopts the α-majority color. Decision is by fixed round count.
  Stateless across rounds.
- **Snowflake**: adds a single `confidence` counter that **resets on
  every color flip**. Decision after β *consecutive* same-color
  α-majority queries. The reset is the load-bearing weakness:
  adversarially-timed splits can keep `confidence` low indefinitely.
- **Snowball**: replaces the single counter with a **per-color
  persistent accumulator** `accum: Color → Int`. Each α-majority query
  for color C increments `accum(C)`. A color flip occurs when
  `accum(C') > accum(C_current)` for some C' (the node prefers the
  color with the highest accumulator). Decision when
  `accum(C_decided) − max{accum(C') : C' ≠ C_decided} ≥ β`. **No
  reset on flip** — accumulated history persists, so the adversary
  cannot erase prior evidence.
- **Snowman**: chain-restricted Snowball (per-block, hash-chained).
  We use this variant — decision **§0.J**, gl0 single-chain.
- **Avalanche**: DAG-form, multiple Snowballs in parallel. Out of
  scope.

We adopt **Snowball** semantics (decision **§0.K**). Decisions are
**irrevocable** at the protocol layer; the application layer rolls back
state on flip up until decision (decision **§0.E**) but never after.

Properties:
- **Safety** (probabilistic): for honest fraction `f` above a threshold,
  the probability two honest nodes decide different colors is
  `≤ ε(K, α, β, f)`, exponentially small in `β`. Snowball's bound is at
  least as good as Snowflake's at the same β, and strictly better
  against the coordinated-split adversary (§0.K, §6.1).
- **Liveness** (probabilistic): with positive progress per round, all
  honest nodes converge to one color; once converged, all decide
  within `β` rounds.
- **Quiescence**: no decided node ever talks again about that ordinal.

### 2.2 Adaptation to per-ordinal attestation (Snowball)

For each ordinal N that a validator observes:

```
state per (node, ord):
  preference: Hash                          // init: local canonical hash at this ord, if any
  accum:      Map[Hash, Int] = {}           // per-hash persistent accumulator (Snowball, §0.K)
  decided:    Boolean = false
  initialized: Boolean = false

every Δ ms (the Avalanche tick — see §2.4):
  for each ord with !decided and initialized:
    K = sample K peers uniformly from stakeRegistry.activeValidators \ {self}    // §0.I
    R = query each peer for their preference at ord                              // RPC
    R = R.filter(_ != Pending)                                                   // drop "I don't know yet"
    if |R| < α:
      continue                                                                   // not enough responses — try next tick
    topHash = the hash that appears most in R
    if count(topHash) >= α:
      accum(topHash) += 1                                                        // Snowball: persistent per-color tally
      // re-derive preference: the hash with the highest accumulator
      newPreference = argmax(accum)
      if newPreference != preference:
        rollback_local_state_to(ord - 1)                                         // §0.E symmetric rollback (§3.3)
        preference = newPreference                                               // accum is NOT reset
    // (else: no α-majority — no accumulator increment, no flip)
    // Decision: margin of top accumulator over the runner-up clears β
    sorted = accum.values.sorted(descending)
    if sorted.length >= 1 and (sorted[0] - sorted.getOrElse(1, 0)) >= β:
      decided = true
      emit signed_attestation(ord, preference, attestedAt = now)                 // §0.F emit-once
```

**Key contrast with Snowflake (§2.1):** there is no `confidence: Int`
that resets on flip. The accumulator carries history across the whole
cascade. A flip is just "the hash with the highest cumulative
α-majority count changed" — it does not zero anyone's tally. The
decision condition is **margin-based** (`top − runnerUp ≥ β`), not
consecutive-count-based.

The seed value for `preference` when entering an ord:
- If we have produced or stored a snapshot at ord N already, use the
  canonical hash from our local chain store. Initialize `accum` with
  the seed hash set to 0 (no a-priori weight); the first
  α-majority query increments it.
- If we have seen peer snapshots at ord N but our chain hasn't reached
  there yet, use the most-recently-arriving valid candidate.
- If we have seen no snapshots at ord N, do not initialize yet (no
  query) — Avalanche is silent until we have a candidate.

`Pending` is a peer-side response signaling "I have not initialized at
ord yet"; counted toward `|R|` it would skew the α-majority test
(false-quorum-of-zero) — drop it.

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
- **Response**: `(queryId, ordinal, preference: Option[Hash], accum: Map[Hash, Int], decided: Bool)`
  → 1 RTT total

The response's `preference: Option[Hash]` is `None` when the responder
hasn't initialized — explicit "pending" signal. The `accum` map (the
full per-hash accumulator) and `decided` flag are **observational only**;
they don't gate anything in the querier's cascade (the querier only
needs `preference` to drive its own Snowball step). They feed
Prometheus and diagnostics, and they're useful for after-the-fact
audit of which hash the network was converging on at any given tick.
At small cluster sizes the `accum` map is bounded to ~3-5 entries (the
distinct fork tips at this ord).

Implementation note: this can ride on the existing libp2p GossipSub
sidecar (`SidecarClient`) as a new typed message, or — preferred — on a
direct request/response stream (one-shot). The query rate is small
(`K / Δ` per node, e.g. `3 / 500ms = 6 qps` at small cluster sizes). At
cluster size 100 with 10 pending ordinals that's `100 × 3 × 10 / 0.5s
= 6000 qps` cluster-wide, which the sidecar can absorb.

### 2.4 Parameters

**Initial recommendation (Snowflake-empirical floor; subject to
re-sim under Snowball): `(K=3, α=2, β=10)` across cluster sizes
5–1000.** The β=10 setting is the empirical floor from the full GPU
sweep (§0.A.i) — it zeroes coordinated_lie violations at every measured
N, and is the *minimum* β that does so for N=32 (where β=6 leaks
8.43 %). β under Snowball is expected to be **equal or lower** for the
same agreement rate because adversarial split timing cannot reset the
accumulator; the §0.K.2 split_honest data shows Snowflake-β=10 still
leaks 9.85–16.71 % violations at N ∈ {100, 500, 1000}, so the Snowball
re-sim is load-bearing for the production recommendation.

| Environment      | Cluster size | K  | α  | β (Snowflake empirical / Snowball pending) | Tick Δ              | p99 rounds        |
|------------------|--------------|----|----|--------------------------------------------|---------------------|-------------------|
| e2e tests        | 3-8          | 3  | 2  | 10 / 6-10 (re-sim)                         | `slot/2 = 250 ms`   | 12-13 (≈ 3 s)     |
| small mainnet    | 16-100       | 3  | 2  | 10 / 6-10 (re-sim)                         | `slot/2 = 500 ms`   | 13-14 (≈ 7 s)     |
| large mainnet    | 500-1000     | 3  | 2  | 10 / 6-10 (re-sim)                         | `slot/2 = 500 ms`   | 13 (≈ 6.5 s)      |

The full GPU sweep at N ∈ {500, 1000}, n_trials = 10000 (§0.A.i) has
**already landed under Snowflake semantics** and confirms the
coordinated_lie convergence story; what remains is the Snowball
re-sim, which is in flight as a parallel workstream. If the Snowball
sweep confirms the prediction (β=6 zeroes split_honest at N=1000), the
production parameter locks at (K=3, α=2, β=6, Δ = slot/2). If Snowball
still shows residual split_honest violations at the f=0.33 / N=500
worst-case cell, β bumps to 8 or 10 (also subject to re-sim — Snowball
β is the lifetime margin, not the consecutive count, so a Snowflake-β
to Snowball-β mapping is non-trivial).

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
- `α > K/2` for safety: a tie cannot promote a value. At `(3, 2, β)`,
  `α = 2 > 1.5 = K/2`. Holds.
- `β` is the **margin** between the top and runner-up accumulator at
  decision time (Snowball, §2.2). Snowflake β was the consecutive-
  count threshold; Snowball β is the lifetime-margin threshold. The
  knob is the same — safety vs. latency — but the underlying state
  is richer, which is why a smaller β suffices for the same agreement
  rate.

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
have attested to the receiver's canonical hash. Under Avalanche-Snowball,
each honest node's per-color accumulator answers a stronger local
question: "is my preferred hash H's lifetime α-majority count ahead of
the runner-up by margin β?" Once `accum(H) − runnerUp ≥ β`, the local
node decides — and once that local decision becomes gossiped as an
emitted attestation, the receiver's T_count input trivially includes it.

The net effect: T_count's network-level evaluation reduces to "every
node has independently decided via its own Snowball margin, and the
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
formal; at small clusters (N=16, β=10) this comfortably exceeds the
2/3 floor.

### 3.3 Symmetric rollback (decision §0.E)

When the Snowball cascade flips its locally-preferred hash
(`newPreference != preference` branch in §2.2), the validator may have
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
- **Snowball-specific:** the `accum` map is **not** reset on flip
  (that's the entire point of Snowball over Snowflake). State rollback
  in the application layer is decoupled from the cascade's per-color
  history.

What we do **not** want: an asymmetric "fast-path apply, slow-path
hand-rolled undo" mechanism. That was the shape that gave us the
`MptOverlay.MultiBranch` base-write leak (#121, iter19) and the gl1
mempool tx-drop on reorg (#122). The Avalanche-flip path uses the same
primitives that already work — no new undo path. This is the load-bearing
constraint behind decision **§0.E**.

The cost of symmetric rollback is bounded by the number of flips a
single ordinal can undergo before decision, which under Snowball is
itself bounded by **the time it takes for the leading accumulator to
pull `β` ahead of the runner-up**. Under honest majority, flips during
the cascade window are rare; the (Snowflake-tuned) full sweep measures
`median_rounds = 11-13` at β=10 across N ∈ {5..1000}, so the cascade
typically lands first-try on the right preference. Snowball is expected
to lower the flip rate further because a single round of split queries
cannot reverse accumulator order if the leading hash has a comfortable
margin.

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

The dominant cost of Avalanche is `β`-equivalent rounds × `Δ` ticks
before lock-in:

```
T_decide ≈ β · Δ + RTT_query
```

(For Snowflake, β is the consecutive-count threshold; for Snowball, β
is the lifetime-margin threshold. Under non-adversarial conditions the
two are within a small constant factor — Snowball reaches `accum(H) −
runnerUp ≥ β` in roughly β rounds once everyone agrees.)

From the full GPU sweep (§0.A.i) at the empirical β=10 — note these
numbers are Snowflake-tuned; Snowball is expected to match-or-beat
them at the same β, and the parallel re-sim will replace this table
with Snowball values when it lands:

| Env             | N    | (K, α, β)    | Δ      | p50 / p99 rounds | p50 / p99 wall-time |
|-----------------|------|--------------|--------|------------------|---------------------|
| e2e tests       | 5    | (3, 2, 10)   | 250 ms | 11 / 12          | 2.75 / 3.0 s        |
| e2e tests       | 8    | (3, 2, 10)   | 250 ms | 11 / 13          | 2.75 / 3.25 s       |
| small mainnet   | 16   | (3, 2, 10)   | 500 ms | 12 / 14          | 6.0 / 7.0 s         |
| small mainnet   | 100  | (3, 2, 10)   | 500 ms | 12 / 13          | 6.0 / 6.5 s         |
| stress f=0.33   | 500  | (3, 2, 10)   | 500 ms | 13 / 13          | 6.5 / 6.5 s         |
| stress f=0.33   | 1000 | (3, 2, 10)   | 500 ms | 13 / 13          | 6.5 / 6.5 s         |

Compare to the existing `finalityMonitor` cadence: `5 × slotDurationMs`,
i.e. 5 s prod / 2.5 s e2e (`SnapshotLeaderLoop.scala:489-490`). At
β=10 the Avalanche window is **~6.5 s p99 in production** vs. the
existing 5 s tick — within the same envelope but slightly slower
(motivated by the empirically-required β=10 floor under Snowflake;
Snowball is expected to bring this back into ≤ 5 s territory if β=6
suffices). The critical difference is that the Avalanche-emitted
attestation represents a *decided* value rather than a moving target,
and triggers no §5.1 re-emit ticker (§0.F).

### 5.2 Composition with T_depth1 latency

The finality stack today already gates on `T_depth1 = bestTipOrdinal - k₁`
(default `k₁ = 255` ordinals ~= 255 × 1s ≈ 4.25 min at prod slot rate;
on the looser ~7 s effective snapshot rate that's ~30 min). Avalanche
adds ~6.5 s p99 under the empirical Snowflake-β=10 (and likely ~3-4 s
under a confirmed Snowball-β=6). Not material against the
T_depth1 envelope either way.

### 5.3 Liveness floor

Avalanche needs `K = 3` active peers responding to queries within Δ to
make progress. At our smallest e2e cluster (3 nodes), `K = 3` already
equals `N`, which means we need to relax the `K ≤ N - 1` constraint or
fall through to T_depth1. The harness output confirms convergence at
N=5; N=3 is a degenerate case where the cluster runs in "depth-only
finality" mode for fork branches, which is the desired safety property
under partition anyway:

- If the cluster is partitioned below `K`, Avalanche **stalls**:
- `accum` never accumulates a `β`-margin → no decision → no
  attestation emitted.
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

This bound is for **Snowflake**. The Snowball bound (Amores-Sesar &
Schneider 2024, §5.5) is **at least as good** at the same parameters
and **strictly better** against a coordinated-split adversary, because
the accumulator cannot be erased — Snowflake's exponent is tied to
"consecutive same-color rounds" whereas Snowball's is tied to
"cumulative margin", and the adversary cannot keep the cumulative
margin below threshold without also losing the underlying α-majority
race. **At our chosen (K=3, α=2) the literature bound is loose; the
full GPU sweep is our source of truth.**

At our chosen (K=3, α=2, β=10) under Snowflake, the full GPU sweep
(§0.A.i, 10000 trials/cell) shows:

- **Under `coordinated_lie` at f=0.33: zero violations** across
  N ∈ {5, 8, 16, 32, 100, 500, 1000} at 10000 trials/cell each (95 %
  Clopper-Pearson upper bound on violation rate: ~0.037 %).
- **Under `split_honest` at f=0.33** (§0.K.2): 9.85 % violations at
  N=100, 16.71 % at N=500, 14.15 % at N=1000 — i.e. the Snowflake
  cascade is *not* safe at production scale against the split-honest
  adversary even at the empirically-tuned β=10. β=6 at the same
  adversary and N=1000 reaches **71.4 %**.

**Under Snowball semantics (decision §0.K, §2.2) we predict the
split_honest violations collapse to ≤ 0.1 % at the same β=10**,
because the dominant attack mode (adversary times split queries to
keep `confidence` resetting) is exactly the mode the accumulator
neutralizes. The §2.2 Snowflake → Snowball flip is therefore not
just a theoretical-robustness gain — it is the **load-bearing
empirical fix** for the split_honest weakness measured in §0.K.2.
**Pending re-sim confirmation** (parallel workstream in flight).

The compositional safety story (Snowflake-tuned, to be re-derived
under Snowball):

- **Best case (full sweep at f=0.33, N=1000, coordinated_lie):** 0
  violations in 10000 trials. Posterior 95 % CI for the violation rate
  is `[0, 0.037 %]`.
- **Worst case observation under coordinated_lie (full sweep, β=6,
  f=0.20, N=32):** 843 violations in 10000 trials = 8.43 % rate. β=10
  zeroes this everywhere we sampled.
- **Worst case observation under split_honest (full sweep, β=10,
  f=0.33, N=500):** 1671 violations in 10000 trials = 16.71 % rate.
  Under Snowball this is predicted to drop to ≪ 0.1 % at the same β=10
  (and possibly the same β=6, which the re-sim must confirm).

The classical-Avalanche (K=20, α=15, β=20) row of the full sweep
(§0.A.i, §0.K.1) is the cautionary tale: 0 % convergence at f=0.33
across all N from 32 to 1000, regardless of which semantics. This is
not a Snowflake-vs-Snowball issue; it is a `K > N - 1`-effectively
issue (slow recruitment of α=15 honest peers under ≥ 25 % adversarial
fraction), and is the strongest argument against the literature
parameters for our setting.

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

The `AvalancheState` Ref shape (Snowball, §2.2):

```scala
final case class AvalancheState(
  preference:  Hash,
  accum:       Map[Hash, Int],     // per-color persistent accumulator (Snowball)
  decided:     Boolean,
  initialized: Boolean,
  lastTickMs:  Long
)
type AvalancheStateRef[F[_]] = Ref[F, Map[SnapshotOrdinal, AvalancheState]]
```

`accum` is bounded in practice to a handful of entries per ordinal
(distinct fork-tip hashes observed at this ord). Memory per ord is
`O(observed_hashes × (hash_size + int_size))` ≈ a few hundred bytes
worst-case.

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

1. **Snowball re-sim.** The full GPU sweep (1896 cells × 10000 trials,
   commit `d8f4639`) is **Snowflake** — per decision §0.K, this is the
   defect being fixed. The Snowflake data is sufficient to set the
   *empirical floor* (K=3, α=2, β=10 zeroes coordinated_lie at every
   measured N) and to *demonstrate the need* for the §2.2 flip
   (Snowflake-β=10 still leaks 9.85–16.71 % violations against
   split_honest at N ∈ {100, 500, 1000} — §0.K.2). What it cannot do
   is set the production β: that requires Snowball semantics. The
   re-sim must (a) port the inner loop from Snowflake's
   `confidence: Int / reset on flip` to Snowball's
   `accum: Map[Hash, Int] / margin decision`; (b) re-run the full sweep
   at the same (K, α, β, N, f, adversary) grid. **Parallel workstream
   in flight at the time of this revision.** Predicted outcome:
   Snowball-β=6 reaches 0 safety violations everywhere
   Snowflake-β=10 does *and* zeroes the split_honest violations
   measured in §0.K.2. If confirmed, lock the proposal at
   `(K=3, α=2, β=6)`. If Snowball-β=6 still shows residual violations
   at the worst-case cell (split_honest, f=0.33, N=500), bump β to
   8 or 10 (also subject to confirmation against the empirical
   coordinated_lie floor — see §2.4 caveat on the Snowflake-β to
   Snowball-β mapping).

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

6. **Sidecar query budget at scale.** At cluster size 1000 with K=3
   and 10 pending ords/node, query traffic is `1000 × 3 × 10 / 0.5s ≈
   60k qps cluster-wide` (independent of β — β only sets when the
   per-ordinal cascade *terminates* its query stream, not the per-tick
   rate). Distributed (each node's K queries fan out), per-node
   receive is `3 × 10 / 0.5s = 60 qps`. Comfortable, but
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
   accumulator dynamics in a victim node? Less of a concern under
   Snowball (the accumulator is monotone, so delaying a response
   merely defers — not erases — its contribution), but a per-peer
   query-response latency histogram is still a natural Prometheus
   addition. Worth flagging to cryptographer review (§6.3).

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

## §10A. Frosty-derived enhancements (future work)

Frosty (Lewis-Pye, Buchwald, Buttolph, O'Grady, Sekniqi, arXiv:2404.14250,
2024) supplies two enhancements to the Snow family. Per decision §0.L
we adopt **neither directly in this proposal**, but each is documented
here as a candidate refinement.

### 10A.1 Snowflake+ dual-threshold cascade — candidate refinement

**What it is.** Frosty's Snowflake+ replaces Snowflake's single
threshold α with two: α₁ (the flip threshold — sample count to switch
preference) and α₂ (the count threshold — sample count to deepen
confidence), with α₁ ≤ α₂. The decision rule becomes:

```
if ≥ α₁ sampled values are 1 - val:   val := 1 - val; count := 0
elif ≥ α₂ sampled values equal val:   count += 1
if count >= β:                        decide(val)
```

**Why it matters.** Splitting "flip" and "deepen" into two thresholds
lets the protocol be **more conservative about flipping** (small α₁
means flips are easy and frequent) while **also more conservative
about deciding** (large α₂ means decisions require near-unanimity).
The resulting cleanly-analyzable safety bound is what Frosty's
consistency proof depends on (Lewis-Pye et al. §4, Theorem 1).
Frosty's recommended values are α₁=41, α₂=72 (k=80) — both well above
the Snowflake α=72 single-threshold equivalent.

**Why it's deferred.** Our Snowball semantics (decision §0.K, §2.2)
already resolves the dominant attack mode (Snowflake reset-on-flip).
Adding Snowflake+ dual thresholds on top of Snowball ("Snowball+")
is straightforward — replace the single `α` check with two — but
needs its own sim sweep to find the right (α₁, α₂, β) triple at our
small-cluster (K=3) setting. The implementation cost is small
(~20 LOC change to the cascade + new config knobs); the cost is
in the calibration work, not the code.

**Failure mode it would prevent (over Snowball alone):** a borderline
adversary that consistently produces split queries at the α boundary
— neither giving the victim a clear α-majority for the leading hash,
nor a clear α-majority for the trailing one. Snowball still
progresses, but slowly. Snowflake+ at small α₁ would let the victim
flip preference more readily; at large α₂, hold off deciding until
clearly converged. Net effect: smoother latency distribution under
this specific adversary class.

**Implementation cost.** ~20 LOC change to the cascade plus new
config knobs `NAKAMOTO_AVALANCHE_ALPHA_1` / `_ALPHA_2`. Sim re-
calibration: extend the full GPU sweep to a 2D (α₁, α₂) grid for each
(K, β) cell — within an order of magnitude of the existing 1896-cell
sweep budget (7.1 min wall-clock).

**Cross-link.** If we adopt Snowflake+ later, the rolling-upgrade
shape is identical to the Snowflake → Snowball flip: it's a node-
local cascade change, not a wire-protocol change. The query / response
RPC types don't change.

### 10A.2 Frosty liveness module — subsumed by T_depth1

**What it is.** Frosty's central claim is that even Snowman has a
liveness vulnerability when the adversary exceeds O(√n) processors:
termination can become polynomial in n rather than logarithmic. The
fix is an "epoch change" trigger that, when the cascade fails to
make progress within an expected window, switches to a temporary
**quorum-based** (e.g. Tendermint or Simplex — see Frosty for partial
synchrony, arXiv:2506.09823) protocol to force-finalize the next
block, then returns to Snowman.

**Why it's subsumed for our setting.** We already have an
operational analog: **T_depth1 (depth-k₁ structural finality,
default k₁ = 255)** carries the chain through whenever attestation
flow stalls. The shape differs (Frosty's quorum-based protocol is a
hot-path safety net; T_depth1 is a slow-path longest-chain-style
liveness floor), but the **role is the same**: a guarantee that
finality eventually happens even when the fast cascade fails to
converge.

Concretely, the §5.3 ("Liveness floor") analysis shows:
- If the cluster is partitioned below K, Avalanche stalls → `accum`
  never accumulates a β-margin → T_weight and T_count starve →
  T_depth1 carries the chain on pure structural depth.
- T_depth1 is **structural**, independent of attestation flow.
  It runs on slot-clock + LDD, not on Avalanche queries.

This is, modulo terminology, exactly Frosty's epoch-change pattern:
"detect the cascade has stalled, switch to a fallback that doesn't
depend on the cascade's progress, finalize from that fallback".

**Why we don't import Frosty's specific module.** Frosty's epoch-
change uses a quorum-based protocol — Tendermint in the synchronous
version, Simplex in the partial-synchrony version. Importing either
would require us to either (a) run a parallel quorum protocol that
duplicates depth-k₁'s job, or (b) wire epoch-change into the Avalanche
cascade as an explicit trigger condition (which adds complexity for
no gain over the existing T_depth1 fallback). Our setting is "Avalanche
as evidence-producer, multi-trigger finality stack as
evidence-consumer" (per §3), not "Avalanche as the only path to
finality"; Frosty's setting is the latter. The architectural fit
isn't there.

**Cross-link to future work.** If at some point we want to remove the
T_depth1 / T_depth2 triggers entirely and rely on Avalanche as the
sole finality input, Frosty's epoch-change *would* become essential.
That would be a separate, much more invasive proposal.

### 10A.3 Frosty parameters at our cluster size — not directly useful

Frosty's recommended parameters (k=80, α₁=41, α₂=72, β=12, n ≥ 500)
are tuned for large permissioned validator sets and a fully-synchronous
network with bounded message delivery Δ. At our (K=3, N=16) operating
point Frosty's k=80 is structurally impossible (K ≤ N - 1 binds).
The proof bounds Frosty derives also assume f < n/5 — strictly
tighter than our f ≤ 1/3 at the chain layer (Taktikos LDD bound). We
do not import the f < n/5 assumption; the **Avalanche layer composes
with** — does not replace — Taktikos's bound.

`fB = 0.05` in production is unchanged. Frosty has no LDD analog and
no recommendation that contradicts our fB.

---

## §10B. Sharding reference — Avalanche Subnets (out of scope)

Avalanche Subnets ([build.avax.network/academy](https://build.avax.network/academy/avalanche-l1/avalanche-fundamentals/04-creating-an-l1/03-network-architecture))
is Avalanche's architectural answer to horizontal scaling: each subnet
is a sovereign network running its own Snowman instance over a
**dynamic subset** of the platform's validators (validators may
participate in multiple subnets; the primary network's P-chain is the
authoritative registry of subnet membership and validator-set
composition). Cross-subnet asset bridging happens via the P-chain
plus the Avalanche Warp Messaging protocol; the architectural pattern
is "hierarchical Snowman, per-subnet validator sets, cross-subnet
implicit voting via an attestation DAG".

For the long-term **sharding workstream** in this codebase (see
`project_consensus_sharding_roadmap.md` and `project_sharding_strategic_signals.md`),
Avalanche Subnets is the architectural reference: per-shard Snowman
(or Snowball-attestation, in our terminology) instances; cross-shard
attestations through the global Taktikos chain (the P-chain analog);
validator-set partitioning by combined delegated stake + node
collateral. **This proposal does not spec sharding** — sharding is a
separate, much larger workstream that depends on KES, stake-weighted
VRF, and a redesign of the L0/L1 layer separation. Avalanche Subnets
is cross-linked here only to anchor the reference for that future
work.

---

## §10. References

- Team Rocket (pseud.). *Snowflake to Avalanche: A Novel Metastable
  Consensus Protocol Family for Cryptocurrencies*. IPFS hash
  `QmUy4jh5mGNZvLkjies1RWM4YuvJh5o2FYopNPVYwrRVGV`, 2018. Mirror at
  [avalabs.org/whitepapers](https://www.avalabs.org/whitepapers).
  Original Slush / Snowflake / Snowball / Avalanche family. The
  Snowball semantics in §2.2 are the §3.2 variant.
- Rocco et al. *Snowman++: Improved Snowman Consensus*. 2020.
  (operational improvements over Snowman.)
- Lewis-Pye, Buchwald, Buttolph, O'Grady, Sekniqi. *Frosty: Bringing
  strong liveness guarantees to the Snow family of consensus protocols*.
  arXiv:2404.14250, 2024. ([html](https://arxiv.org/html/2404.14250v5))
  Snowflake+ dual-threshold cascade (§10A.1) and epoch-change liveness
  module (§10A.2). Synchronous model, f < n/5.
- Lewis-Pye et al. *Frosty for partial synchrony*. arXiv:2506.09823,
  2025. Frosty's adaptation to partial synchrony using Simplex as the
  epoch-change fallback. Reference for §10A.2.
- Amores-Sesar, Schneider. *An Analysis of Avalanche Consensus*.
  arXiv:2401.02811, 2024. Independent formal analysis of the Snow
  family with explicit Snowflake vs Snowball comparison. Cited in
  §0.K, §6.1.
- Garay, Kiayias, Leonardos. *The Bitcoin Backbone Protocol: Analysis
  and Applications*. EUROCRYPT 2015.
- Kiayias, Leonardos, Stouka, Zacharias. *Ouroboros Taktikos*. FC 2023.
- Buterin & Griffith. *Casper the Friendly Finality Gadget*. 2017.
  (Slashing-evidence design precedent: GASPER's equivocation evidence
  has the same "self-verifying tuple" property we adopt in §4.2.)
- Avalanche Network Architecture documentation
  ([build.avax.network/academy](https://build.avax.network/academy/avalanche-l1/avalanche-fundamentals/04-creating-an-l1/03-network-architecture)).
  Subnets architecture, P-chain registry, validator-set partitioning.
  Reference for §10B (long-term sharding workstream).
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
    commit `d8f4639` — full GPU-backed sweep at 1896 cells × 10000
    trials. Snowflake semantics; Snowball re-sim is a parallel
    workstream (decision §0.K, open question §8.1).
  - `~/repos/research-nipopos-2026/sims/data/avalanche_attestation_full_gpu_n10000.json`
    — **primary data file referenced throughout this proposal.** 1896
    cells across (K, α, β, N, f_adv, adversary, latency) grid; 10000
    trials per cell; 7.1 min wall-clock on RTX 5090. Headlines in
    §0.A.i, §0.K.1, §0.K.2, §5.1, §6.1.
  - `~/repos/research-nipopos-2026/sims/AVALANCHE_CALIBRATION.md`
    — adversary models (coordinated_lie, split_honest), latency models
    (boltzmann, pareto), output schema.
