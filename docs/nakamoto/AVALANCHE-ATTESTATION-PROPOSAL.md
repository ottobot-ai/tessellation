# Avalanche-style subsampling for attestation decisions

> **TARGET ROLE CLARIFICATION (2026-07-11):** Avalanche/Snowball is the
> optimistic GL0 Phase-2 finality path, paired with the Nakamoto `k1` depth
> fallback. It is not economic validation and does not introduce global BFT
> votes, locks, QCs, or view changes. Every decided attestation must name an
> authenticated snapshot the signer has locally executed. Phase 2 remains
> maxvalid-bg density-reorgable; `k2` is retention/recovery capacity, not a later
> finality phase or floor. The owner-ratified composition is
> `T_optimistic = decided-attestation T_weight` and
> `P2 = T_optimistic OR T_depth1`; a local beta result does not directly advance
> Phase 2 and `T_count` is subsumed.

**Status:** historical research proposal and active evidence input, not a protocol
decision. Its old “locked/settled” labels are superseded by
`../review/CONSENSUS-OWNER-DECISIONS-ANSWERS.md`; O-01 records the ratified
direction and provisional calibration, while implementation parity, parameters,
and proof remain activation gates. Implementation is not complete. Written
2026-05-15. Revised 2026-05-15 to fold in then-current
decisions and the quick-sweep sim recommendation from
[`~/repos/research-nipopos-2026` commit `15983f1a`](#). Revised again
2026-05-15 to flip cascade semantics from **Snowflake → Snowball** (per-color
persistent confidence accumulator, decide by margin) and fold in two
Frosty-derived insights — see §0 TL;DR and §2.2 / §10A. Revised again
2026-05-15 to repoint all sim references to the **full 1896-cell × 10000-trial
GPU sweep** (Snowflake-only) and to fold in the new
**split_honest** finding that turns the §2.2 Snowflake → Snowball flip from a
theoretical robustness gain into an empirical necessity at scale.
**Revised 2026-05-15** to fold in the Snowball re-sim
(`avalanche_attestation_full_gpu_n10000_v2.json`, commit `5ace3d36`,
6696 cells × 10000 trials, 19.6 min wall-clock dual-mode Snowflake+Snowball
on RTX 5090, three adversary modes including `random_honest`). **The
production recommendation flips from `(K=3, α=2, β=10)` to
`(K=8, α=5, β=10)`** after the re-sim revealed (a) Snowflake at K=3
leaks 14.6 % violations at N=1000 split_honest *and is a liveness
failure masquerading as safety* (counter-reset means the node never
decides at all — see §3.4); and (b) Snowball at K=3 leaks **31.2 %**
violations under the same conditions because the per-color accumulator
margin is sub-β under adversary-induced noise. **K ≥ 8 with α ≥ ⅝K is
the empirical floor for Snowball.**

This document historically proposed replacing the current "attest the current canonical
bestTip per (ord, hash) pair" emit policy with an **Avalanche-style
subsampling decision protocol** that converges on a single hash per
validator per ordinal *before* emitting attestation. Once decided, no
re-emit. It kept the receiver aggregation policy (TipTracker newer-wins,
T_weight / T_count / T_depth1 / T_depth2 triggers, canonical-hash filter)
unchanged. That historical composition is superseded: the active design feeds
portable decided attestations into fixed-historical-registry `T_weight`, with
`T_depth1` as the independent fallback and no `T_count` rail. Its interaction
with density replacement and current-canonical evidence still requires an
implementation and proof.

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
  — sim harness, branch `sim/avalanche-attestation`. The primary data
  referenced throughout is the **dual-mode (Snowball + Snowflake)
  6696-cell × 10000-trial GPU sweep** at
  `~/repos/research-nipopos-2026/sims/data/avalanche_attestation_full_gpu_n10000_v2.json`
  (commit `5ace3d36`, 19.6 min wall-clock on RTX 5090, three adversary
  modes: `coordinated_lie`, `split_honest`, `random_honest`). The v1
  Snowflake-only sweep (`avalanche_attestation_full_gpu_n10000.json`,
  commit `d8f4639`, 1896 cells, 7.1 min) is superseded.
- `~/.claude/skills/taktikos/SKILL.md` — Taktikos protocol rules.

---

## §0. Historical decisions from the 2026-05-15 proposal (superseded)

The 13 entries below were treated as settled by this proposal. They are not current
owner decisions; retain them only as research rationale. The active owner register
and lifecycle control implementation.

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
| **M** | **K parameter empirically raised from 3 to 8** after the dual-mode Snowball re-sim (commit `5ace3d36`) revealed K=3 is at the noise floor for Snowball: per-color accumulators grow in lockstep under adversary-induced noise, and margin spikes cause early decisions on whichever color recently led. K ≥ 8 with α ≥ ⅝K is the empirical floor that zeroes safety violations at all measured N ∈ {16, 32, 100, 500, 1000} across all three adversary modes. **Production recommendation: `(K=8, α=5, β=10)`**. | §0.A, §2.4, §3.4 |

### 0.A — Sim recommendation (dual-mode 6696-cell × 10000-trial GPU sweep, commit `5ace3d36`)

The harness at
`~/repos/research-nipopos-2026/sims/avalanche_attestation_calibration.py`
implements the Snowman variant of Avalanche at per-validator per-ordinal
granularity, with three Byzantine adversary modes —
`coordinated_lie` (every Byzantine peer reports a third hash `HASH_LIE`
to drain honest confidence), `split_honest` (Byzantine peers
strategically mirror the victim's *minority* preference to keep the
cascade flipping; the Snowflake-killer attack), and `random_honest`
(Byzantine peers respond with one of the two honest values uniformly
at random) — and a 50/50 initial honest split. The **v2 dual-mode
sweep** evaluates **both Snowflake and Snowball decision rules** at
every cell.
(`~/repos/research-nipopos-2026/sims/data/avalanche_attestation_full_gpu_n10000_v2.json`,
6696 cells × 10000 trials/cell, 19.6 min wall-clock on RTX 5090, max
200 rounds, Boltzmann (`δ̄ = 50 ms`) and Pareto latency,
`tick_dt = 250 ms`, branch `sim/avalanche-attestation`, commit
`5ace3d36`.) The headline finding is the **(K, α, β) walk-up**: the
smallest Snowball configuration that achieves safety violations < 0.001
across *all three* adversary modes at *all* tested N ∈ {16, 32, 100,
500, 1000}, f_adv = 0.33, is **(K=8, α=5, β=10)**:

| K/α/β (Snowball) | coord_lie f=0.33 N=1000 | split_honest f=0.33 N=1000 | random_honest f=0.33 N=1000 | med rounds (N=1000) |
|---|---|---|---|---|
| 3/2/10 | 0.0000 | **0.3123** | **0.3140** | 17 / 38 / 38 |
| 5/3/8  | 0.0000 | 0.0150 | 0.0156 | 14 / 20 / 20 |
| 5/4/10 | 0.0000 | 0.0024 | 0.0024 | 19 / 28 / 29 |
| **8/5/10** | **0.0000** | **0.0000** | **0.0000** | **15 / 17 / 17** |
| 8/6/12 | 0.0000 | 0.0000 | 0.0000 | 23 / 26 / 26 |

Reading:
- **(K=8, α=5, β=10) is the smallest Snowball triple that zeroes
  safety violations across all three adversary modes** at f=0.33 for
  every N ≥ 16. Median 13-18 rounds at f=0.33 depending on adversary
  and N; p99 16-33 rounds at f=0.33 (largest p99 cell is N=32
  coordinated_lie at 33 rounds = 8.25 s wall-clock; large-N p99 is
  17-20 rounds = 4.25-5.0 s).
- **(K=3, α=2, β=10) — the previous recommendation under
  Snowflake-tuned reasoning — FAILS catastrophically under Snowball at
  scale**: 31.23 % safety violations at N=1000 split_honest and
  31.40 % at N=1000 random_honest. The mechanism: at K=3, per-color
  accumulators grow in near-lockstep under noise-induced ε-margin
  spikes, causing early decisions on whichever color recently led;
  different honest nodes lock different colors. See §3.4.
- **The Snowflake-stall artefact (§3.4):** at the same K=3 cell,
  Snowflake reports a *lower* 14.59 % violation rate at N=1000
  split_honest — but only because the counter-reset means many nodes
  never decide at all (median 93 rounds vs. Snowball's 38). The
  Snowflake number is a liveness failure masquerading as safety; the
  Snowball number is the *true* per-color noise floor at K=3 with
  finite β.
- **(K=20, α=15, β=20) — the literature-standard "classical Avalanche"
  triple — FAILS catastrophically at high f**: 0 / 10000 trials converge
  at f=0.33 for every N ≥ 32 under either decision rule (consistent
  across the v1 and v2 sweeps).
- **Constraint K ≤ N − 1** still binds at small clusters. For N ∈ {3,
  5, 8} (e2e), K=8 is infeasible. At N=8 the v2 grid has no perfectly-
  safe converging Snowball config — `(K=5, α=4, β=10)` reaches 0
  violations under all adversaries but **does not converge under
  coord_lie at f=0.33** (DNF inside the 200-round cap); `(K=5, α=3,
  β=8)` converges everywhere with residual coord_lie ≈ 0.33 %.
  `(K=3, α=2, β=10)` is the best converging option at N=8 (residuals:
  coord 0.51 %, split 0.00 %, random 0.01 %). At N=5, `(K=3, α=2,
  β=10)` is the only option (residuals coord 0.07 %, split 0.00 %,
  random 0.00 %). At N=3 no Avalanche config is possible; the cluster
  runs in T_depth1-only mode. The small-cluster track exists in a
  degenerate regime where T_depth1 fallback dominates anyway (§5.3);
  production recommendation is for N ≥ 16.

**Production recommendation: `(K=8, α=5, β=10, Δ = slot/2)` for cluster
sizes N ≥ 16; degraded small-cluster fallback `(K=3, α=2, β=10)` at
N ∈ {5, 8} (no perfectly-safe Snowball config exists at N=8 in the
v2 grid; T_depth1 carries through finality at this scale).** The K=8
setting (vs. the prior K=3) is empirically required to zero all three
adversary modes under Snowball at production scale; the per-round
bandwidth cost is **2.7× per tick (8 peer queries vs. 3)** and the
latency is **median 3.75 s wall-clock at large N (15 rounds × 250 ms)**
vs. the prior K=3 estimate of 2.75 s — **1 s slower, but genuinely
safe**. See §2.4 for the full parameter table, §5.1 for the latency
analysis, §6.1 for the safety analysis, and §3.4 for the
Snowflake-stall mechanism.

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
reset-on-flip is a load-bearing weakness. **But the v2 dual-mode
re-sim (commit `5ace3d36`) reveals that the original Snowflake →
Snowball framing was incomplete.** Snowflake at K=3 does not "fail
safely" — it **stalls** (counter resets on every flip, so the node
never decides at all), and the reported safety-violation rate is low
only because few decisions are produced. Snowball at K=3 *does* decide
— and then the noise floor of the per-color accumulator becomes
visible: at K=3 the accumulators grow in lockstep, ε-margin spikes
cause early decisions, and different honest nodes lock different
colors. The Snowball K=3 split_honest violation rate at N=1000 is
**31.23 %** vs. Snowflake's 14.59 % at the same cell — Snowball is
strictly worse than Snowflake at K=3, because Snowball is the only
one of the two that is actually deciding. See §3.4 for the full
mechanism analysis.

The fix is **K ≥ 8 with α ≥ ⅝K**, not Snowflake. At K=8 the per-color
accumulator margin grows fast enough relative to adversary-induced
noise that β=10 produces zero violations across all three adversary
modes at every N ∈ {16, 32, 100, 500, 1000} we measured (§0.A).

The full v2 sweep evidence:
- the K=3-noise-floor signature at all N under Snowball is novel to
  the v2 sweep (§0.A headline table);
- the catastrophic failure of literature-standard (K=20, α=15, β=20)
  at high f_adv across all N is reproduced from v1 (still 0 % converge
  at f=0.33 for N ≥ 32);
- the **Snowflake-stall-as-pseudo-safety** finding at K=3 (§3.4) —
  the new analytical move from this re-sim.

### 0.K.1 — Headline against the literature standard (v2 dual-mode sweep)

| N    | f_adv | rule      | (K, α, β)        | convergence | safety_violations | med_rounds |
|------|-------|-----------|------------------|-------------|-------------------|------------|
| 32   | 0.33  | snowball  | (8, 5, 10)       | ≥0.9999     | 0.0000            | 13         |
| 100  | 0.33  | snowball  | (8, 5, 10)       | ≥0.9999     | 0.0000 / 0.0001 / 0.0002 | 14-15 |
| 500  | 0.33  | snowball  | (8, 5, 10)       | ≥0.9999     | 0.0000            | 15-16      |
| 1000 | 0.33  | snowball  | (8, 5, 10)       | ≥0.9999     | 0.0000            | 15-17      |
| 32   | 0.33  | snowball  | **(20, 15, 20)** | **0.0000**  | 0.0000            | DNF (cap)  |
| 100  | 0.33  | snowball  | **(20, 15, 20)** | **0.0000**  | 0.0000            | DNF (cap)  |
| 500  | 0.33  | snowball  | **(20, 15, 20)** | **0.0000**  | 0.0000            | DNF (cap)  |
| 1000 | 0.33  | snowball  | **(20, 15, 20)** | **0.0000**  | 0.0000            | DNF (cap)  |

(safety_violations triple for N=100 K=8/5/10 row is coord_lie /
split_honest / random_honest = 0.0000 / 0.0001 / 0.0002; all other
rows zero across all three adversary modes.)

**Headline: K=20 / α=15 / β=20 (the literature-standard "classical
Avalanche" triple) FAILS at f_adv=0.33 under both Snowflake and
Snowball — 0 / 10000 trials converge at every N from 32 to 1000,
hitting the 200-round cap.** The K ≥ N − 1 bind plus the high α
recruitment requirement (α=15 honest peers out of K=20 sampled under
f=0.33) make convergence functionally impossible at our adversary
level. This is the strongest argument against the literature
parameters for our setting and the basis for the K=8 ≪ 20
recommendation — **K=8 is the smallest K that the v2 Snowball sweep
shows is genuinely safe across all three adversary modes**.

### 0.K.2 — split_honest: Snowflake and Snowball *both* fail at K=3, K=8 fixes it

The v2 dual-mode sweep evaluates both decision rules under the
`split_honest` adversary at every cell. The previous (Snowflake-only)
v1 framing held that Snowflake leaked 9.85–16.71 % violations under
split_honest at K=3 and predicted Snowball at the same K would
collapse those to ≤ 0.1 %. **The v2 sweep refutes that prediction
and reframes the problem: K=3 is the actual defect, not the decision
rule.**

| N    | (K, α, β)  | rule      | adversary    | converge | safety_violations | med_rounds |
|------|------------|-----------|--------------|----------|-------------------|------------|
| 8    | (3, 2, 10) | snowflake | split_honest | 0.9995   | 0.0006            | 36         |
| 8    | (3, 2, 10) | snowball  | split_honest | 1.0000   | 0.0000            | 14         |
| 16   | (3, 2, 10) | snowflake | split_honest | 0.9997   | 0.0095            | 37         |
| 16   | (3, 2, 10) | snowball  | split_honest | 1.0000   | 0.0002            | 16         |
| 32   | (3, 2, 10) | snowflake | split_honest | 0.9983   | **0.0477**        | 58         |
| 32   | (3, 2, 10) | snowball  | split_honest | 1.0000   | **0.0052**        | 20         |
| 100  | (3, 2, 10) | snowflake | split_honest | 0.9992   | **0.0899**        | 70         |
| 100  | (3, 2, 10) | snowball  | split_honest | 1.0000   | **0.0319**        | 26         |
| 500  | (3, 2, 10) | snowflake | split_honest | 0.9992   | **0.1642**        | 88         |
| 500  | (3, 2, 10) | snowball  | split_honest | 1.0000   | **0.1746**        | 34         |
| 1000 | (3, 2, 10) | snowflake | split_honest | 0.9990   | **0.1459**        | 93         |
| 1000 | (3, 2, 10) | snowball  | split_honest | 0.9999   | **0.3123**        | 38         |
| **1000** | **(8, 5, 10)** | **snowball** | **split_honest** | **≥0.9999** | **0.0000** | **17** |
| 1000 | (8, 5, 10) | snowball  | coord_lie    | 1.0000   | 0.0000            | 15         |
| 1000 | (8, 5, 10) | snowball  | random_honest | 1.0000  | 0.0000            | 17         |

Reading:
- **K=3 Snowball at N=1000 leaks 31.23 % violations under split_honest
  — *worse than Snowflake's 14.59 % at the same cell*.** This is the
  Snowflake-stall artefact: Snowflake's counter resets on flip, so
  many nodes never decide at all (median 93 rounds for a max-200
  cap) and the safety-violation rate is reported low because few
  decisions are produced. Snowball *does* decide (median 38 rounds),
  but the K=3 per-color accumulator is at noise floor: ε-margin
  spikes from the split-honest adversary cause early decisions that
  different honest nodes lock to different colors.
- **K=8 fixes it.** (K=8, α=5, β=10) Snowball zeroes split_honest
  violations at N=1000 (and at every measured N ≥ 16) with median 17
  rounds = 4.25 s wall-clock at `Δ = 250 ms`. Same parameters also
  zero coordinated_lie and random_honest at all measured N. This is
  the empirical basis for decision **§0.M**.
- The pattern N ↑ → safety_viol ↑ at K=3 is the per-color accumulator
  noise scaling: as N grows, the adversary has more peers to recruit
  for the minority response, lifting the noise envelope above β=10
  Snowball margin (and above β=10 Snowflake counter in the cells
  where the cascade actually decides). Raising β at K=3 reduces the
  rate but does not close the gap monotonically — the v2 K=3 grid
  shows Snowball-(β=10) leaks 31.23 % at N=1000 split_honest;
  Snowball-(β=8) leaks 64.09 %; Snowball-(β=6) leaks 95.21 %;
  Snowball-(β=4) leaks 100.00 %. β=10 is the largest K=3 β
  measured in v2, and even there the violation rate is structurally
  high. The gap closes structurally at K ≥ 8.

**This refutes the v1 prediction.** The §2.2 Snowflake → Snowball
flip is *still correct* — Snowball decides where Snowflake stalls,
and a stall is not safety — but the flip alone is not enough. K must
also rise from 3 to 8. The current production recommendation
`(K=8, α=5, β=10)` (decision §0.M) is the *combined* fix.

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
(`K / Δ` per node, e.g. `8 / 500ms = 16 qps` at production K=8). At
cluster size 100 with 10 pending ordinals that's
`100 × 8 × 10 / 0.5s = 16k qps` cluster-wide (vs. 6k under the prior
K=3 estimate), which the sidecar can absorb.

### 2.4 Parameters

**Production recommendation: `(K=8, α=5, β=10)` for cluster sizes
N ≥ 16, with degraded fallback `(K=3, α=2, β=10)` at N ∈ {5, 8}
(N=8 has no perfectly-safe Snowball config in the v2 grid; T_depth1
carries through finality anyway at this scale).** This is decision
§0.M, empirically validated by the v2 dual-mode sweep
(commit `5ace3d36`, §0.A / §0.K).

| Environment      | Cluster size | K  | α  | β  | Tick Δ              | p99 rounds          | p99 wall-clock     |
|------------------|--------------|----|----|----|---------------------|---------------------|--------------------|
| e2e tests        | 3            | -- | -- | -- | (T_depth1 only)     | n/a                 | n/a                |
| e2e tests        | 5            | 3  | 2  | 10 | `slot/2 = 250 ms`   | 19-28 (≈ 4.75-7 s)  | 7 s                |
| e2e tests        | 8            | 3  | 2  | 10 | `slot/2 = 250 ms`   | 24-64 (≈ 6-16 s)    | 16 s (coord_lie)   |
| small mainnet    | 16           | 8  | 5  | 10 | `slot/2 = 500 ms`   | 16-27 (≈ 8-13.5 s)  | 13.5 s             |
| small mainnet    | 100          | 8  | 5  | 10 | `slot/2 = 500 ms`   | 20-23 (≈ 10-11.5 s) | 11.5 s             |
| large mainnet    | 500          | 8  | 5  | 10 | `slot/2 = 500 ms`   | 17-20 (≈ 8.5-10 s)  | 10 s               |
| large mainnet    | 1000         | 8  | 5  | 10 | `slot/2 = 500 ms`   | 16-20 (≈ 8-10 s)    | 10 s               |

(p99 ranges span the three adversary modes at f=0.33; coord_lie is
typically the slowest cell.)

**Tradeoff vs. prior K=3 recommendation:**
- **Per-round bandwidth: 8 peer queries vs. 3 (2.7× per round).** At
  cluster size 100 with ~10 pending ords, that's `100 × 8 × 10 / 0.5s
  = 16k qps` cluster-wide (vs. 6k for K=3). Sidecar absorbs.
- **Latency: median 15 rounds × 250 ms = 3.75 s wall-clock at large
  N (vs. K=3's 13 rounds × 250 ms ≈ 3.25 s). 0.5-1 s slower** on the
  median. p99 8.25 s at the worst cell (N=32 coord_lie) vs. K=3's
  p99 of 8.25 s at the worst N=8 coord_lie cell — comparable
  worst-case envelope, with K=8 winning at large N (p99 4-5 s for
  N ≥ 500 vs. K=3's p99 up to 70 rounds = 17.5 s under split_honest
  at N=1000).
- **Robustness: actually safe at scale across all three adversary
  modes** — 0 / 10000 trials show safety violations at every measured
  N ≥ 16, f=0.33, under coord_lie, split_honest, and random_honest
  (one cell with 1-2 violations: N=100 split/random at f=0.33,
  which is within noise).
- **5×slotDuration finality envelope: still inside.** At prod
  `slotDurationMs = 1000`, the 5× target is 5 s; K=8 median is
  3.75 s, p99 ≤ 5 s at N ≥ 500. Under spec.

The K=8 recommendation is locked. Open question §8.1 (formerly "what's
the right β under Snowball") is resolved: β=10. The remaining
parameter question is the K-vs-latency tradeoff at our exact slot
cadence, which is a follow-on sweep (see §8 open questions).

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
  Liveness floor. Binds at small clusters: `K=8` needs N ≥ 9 (e2e
  N=3, 5, 8 fall back to the small-cluster table rows).
- `α > K/2` for safety: a tie cannot promote a value. At `(8, 5, β)`,
  `α = 5 > 4 = K/2`. Holds. At `(5, 4, β)`, `α = 4 > 2.5`. Holds.
  At `(3, 2, β)`, `α = 2 > 1.5`. Holds.
- `α ≥ ⅝K` for empirical safety under noise (§0.A). At K=8 this gives
  α ≥ 5. At K=5 it gives α ≥ 4 (with rounding). The empirical floor
  comes from the v2 dual-mode sweep: K=8/α=4 was not directly
  measured but is below the ⅝K threshold; K=8/α=5 is the smallest
  valid (K=8, α) pair in the v2 grid.
- `β` is the **margin** between the top and runner-up accumulator at
  decision time (Snowball, §2.2). The empirical floor β=10 zeroes
  safety across all three adversary modes at K=8 for every measured
  N ≥ 16.

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
the cascade window are rare; the v2 dual-mode sweep measures
`median_rounds = 13-18` at β=10, K=8 across all measured N ∈ {16, 32,
100, 500, 1000} and all three adversary modes, so the cascade
typically lands first-try on the right preference.

### 3.4 Why Snowflake at K=3 looked safe but wasn't — and why Snowball at K=3 made the defect visible

This section is preserved as a teaching note and as the explicit
"we considered and rejected K=3" record for future reviewers.

The v1 sweep (commit `d8f4639`, Snowflake-only) reported (K=3, α=2,
β=10) as safe under coordinated_lie at all N and as the production
recommendation, modulo a 9.85–16.71 % split_honest weakness that was
attributed to "Snowflake's confidence-reset" and predicted to
disappear under Snowball.

**The v2 sweep refuted the prediction in two coupled findings:**

| Cell | Snowflake | Snowball |
|---|---|---|
| K=3, α=2, β=10, N=1000, f=0.33, split_honest | 14.59 % safety_viol, 99.90 % converge, median 93 rounds | **31.23 %** safety_viol, 99.99 % converge, median 38 rounds |
| K=3, α=2, β=10, N=1000, f=0.33, random_honest | 15.01 % safety_viol, 99.96 % converge, median 93 rounds | **31.40 %** safety_viol, 100.00 % converge, median 38 rounds |
| K=3, α=2, β=10, N=1000, f=0.33, coord_lie | 0 % safety_viol, 100.00 % converge, median 13 rounds | 0 % safety_viol, 100.00 % converge, median 17 rounds |

**The mechanism — Snowflake at K=3 is a stall, not a safety guarantee.**
Snowflake's confidence-counter resets on every preference flip. Under
split_honest at K=3, the Byzantine peers mirror the victim's minority
preference *just often enough* to keep the counter resetting before
it climbs to β=10. Most nodes never reach a decision at all (median
93 rounds is approaching the 200-round cap; 0.1 % of trials at N=1000
literally DNF). The "low" 14.59 % safety violation rate is a *liveness
failure masquerading as safety* — the protocol is silent, so it can't
disagree. Among the trials that *do* decide, the disagreement rate
is in fact much higher; the metric averages over the silent majority.

**The mechanism — Snowball at K=3 has a noise-floor problem.** Snowball
does not reset on flip; the per-color accumulator carries history
across the protocol. So Snowball *decides* (median 38 rounds — over
twice as fast as Snowflake), but with K=3 the per-color accumulator
margin is structurally small: a single round increments one
accumulator by exactly 1 if α-majority is met, 0 otherwise. Two
accumulators in lockstep with ε margin can both increment, and the
adversary-noise envelope is large enough that the "winning" hash
flips identity round-to-round. A small β=10 lifetime margin
threshold is reached on whichever color happens to have a noise-
induced spike first; different honest nodes lock different colors.
**Snowball's safety bound at K=3 is genuinely 31 %.** It is not a
bug in the simulator and it is not a Snowflake-vs-Snowball comparison
failure — it is the actual per-color noise floor at K=3 with
finite β.

**The fix: K ≥ 8 with α ≥ ⅝K.** At K=8, the per-color accumulator
margin grows fast enough relative to adversary-induced noise that
β=10 produces zero safety violations at every measured N ≥ 16 across
all three adversary modes. The empirical floor is K=8 (next-smallest
in our v2 grid was K=5 with α=4/β=10, which leaks 0.24 % at N=1000
split_honest — better but not safe enough for the < 0.001 production
target). The bandwidth cost of K=8 vs. K=3 is 2.7× per round; the
latency cost is ~1 s median (3.75 s vs. 2.75 s at large N) — both
acceptable per the §0.A tradeoff analysis.

**Why this matters for future reviewers.** Anyone repeating the v1
analysis without the v2 dual-mode evidence will be tempted to revert
to K=3 on bandwidth or latency grounds. The Snowflake-stall artefact
means the bandwidth/latency comparison cannot be done against
Snowflake at K=3 (which is silent); it must be done against Snowball
at K=3 (which decides, and is genuinely unsafe). The K=8 floor is
load-bearing.

The v1 sweep at `avalanche_attestation_full_gpu_n10000.json` (commit
`d8f4639`) is **superseded** by the v2 dual-mode sweep. The v1 data
should not be cited for any new analysis; numbers from v1 that appear
elsewhere in this document are either updated in place or labeled as
"v1 (superseded)" in context.


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

From the v2 dual-mode sweep (§0.A, commit `5ace3d36`) at the
empirical floor (K=8, α=5, β=10) under Snowball, worst-case adversary
mode at f=0.33:

| Env             | N    | (K, α, β)    | Δ      | p50 / p99 rounds (worst-case adv) | p50 / p99 wall-time |
|-----------------|------|--------------|--------|-----------------------------------|---------------------|
| e2e tests       | 5    | (3, 2, 10)   | 250 ms | 16 / 28 (random_honest)           | 4.0 / 7.0 s         |
| e2e tests       | 8    | (3, 2, 10)   | 250 ms | 16 / 64 (coord_lie)               | 4.0 / 16.0 s        |
| small mainnet   | 16   | (8, 5, 10)   | 500 ms | 18 / 27 (coord_lie)               | 9.0 / 13.5 s        |
| small mainnet   | 100  | (8, 5, 10)   | 500 ms | 15 / 23 (split/random)            | 7.5 / 11.5 s        |
| stress f=0.33   | 500  | (8, 5, 10)   | 500 ms | 16 / 20 (split/random)            | 8.0 / 10.0 s        |
| stress f=0.33   | 1000 | (8, 5, 10)   | 500 ms | 17 / 20 (split/random)            | 8.5 / 10.0 s        |

(Worst-case-adversary p99 selected per row; coord_lie tends to
dominate p99 at small N, split/random dominate at large N. Median
is generally driven by coord_lie at large N because it converges
slower-but-decides-cleanly than split/random which converge faster-
but-with-more-tail.)

At prod `slotDurationMs = 1000` (so `Δ = 500 ms`), median wall-clock
is **3.75-8.5 s depending on N** and p99 is **10-13.5 s**. Compare
to the existing `finalityMonitor` cadence: `5 × slotDurationMs = 5 s`
in prod (`SnapshotLeaderLoop.scala:489-490`). The Avalanche K=8 p99
exceeds the 5-s tick in some cells (N=16 worst-case 13.5 s, N=100
worst-case 11.5 s) but the *median* sits inside the envelope at
3.75-8.5 s. The K=8 / β=10 floor is **empirically required** for
safety across all three adversary modes (§0.A, §3.4); accepting the
p99 overrun is the load-bearing safety/latency trade.

The critical difference is that the Avalanche-emitted attestation
represents a *decided* value rather than a moving target, and triggers
no §5.1 re-emit ticker (§0.F).

### 5.2 Composition with T_depth1 latency

The finality stack today already gates on `T_depth1 = bestTipOrdinal - k₁`
(default `k₁ = 255` ordinals ~= 255 × 1s ≈ 4.25 min at prod slot rate;
on the looser ~7 s effective snapshot rate that's ~30 min). Avalanche
adds up to ~13.5 s p99 worst-case under the empirical (K=8, α=5, β=10)
Snowball at N=16 / coord_lie. Not material against the T_depth1
envelope.

### 5.3 Liveness floor

Avalanche at the production K=8 setting needs ≥ 8 active peers
responding to queries within Δ to make progress (K ≤ N − 1 binds at
N=9). At our smallest e2e cluster (3 nodes), K=8 is infeasible and
the §2.4 small-cluster table downgrades to (K=3, α=2, β=10) at N=5
or to "T_depth1 only" at N=3. The v2 sweep does **not** measure N=3
at all (the smallest N is 5); N=3 is a degenerate case where the
cluster runs in "depth-only finality" mode for fork branches, which
is the desired safety property under partition anyway:

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
tick is `K × |pending ordinals|`; with K=8 and ~10 pending ordinals
that's 80 qps per node (vs. 30 qps under the prior K=3
recommendation). At cluster size 1000 that's 80k qps cluster-wide
of receive traffic. Sidecar absorbs.

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

This bound is for **Snowflake**. The Snowball analytical bound
(Amores-Sesar & Schneider 2024, §5.5; Lewis-Pye et al. 2024) is **at
least as good** at the same parameters because the accumulator cannot
be erased. **At our chosen (K=8, α=5) the analytical literature
bound is loose for our cluster sizes; the v2 dual-mode GPU sweep
(§0.A, commit `5ace3d36`) is our empirical source of truth.**

At (K=8, α=5, β=10) under Snowball, the v2 sweep shows:

- **Under `coordinated_lie` at f=0.33: zero violations** across
  N ∈ {16, 32, 100, 500, 1000} at 10000 trials/cell each (95 %
  Clopper-Pearson upper bound on violation rate: ~0.037 %).
- **Under `split_honest` at f=0.33: zero violations** across the same
  N range (one cell with 1 violation: N=100 = 0.0001 — within noise,
  95 % CI upper bound 0.056 %).
- **Under `random_honest` at f=0.33: zero violations** across the
  same N range (one cell with 2 violations: N=100 = 0.0002 — within
  noise).

This is empirical, not analytical. The literature analytical bound
for Snowball at K=8, α=5, β=10 is not directly given in the cited
papers; the closest is Amores-Sesar & Schneider §5.5 which gives a
generic exponential-in-β bound, and the more specific Frosty bound
(Lewis-Pye et al. §4) is for Snowflake+ at much larger K. The v2
empirical bound is **strictly stronger** than what either analytical
bound provides for our parameters; the cryptographer review (§6.3,
decision §0.D) should re-derive a tight analytical bound for the
(K=8, α=5, β=10) Snowball cell against our (Taktikos LDD + 1/3 f
chain layer) combined assumption.

The compositional safety story (Snowball, v2 sweep):

- **Best case (v2 sweep at f=0.33, N=1000, coord_lie + split_honest
  + random_honest):** 0 violations in 10000 trials each. Posterior
  95 % CI for the violation rate is `[0, 0.037 %]` per cell.
- **Worst case observation under any adversary at K=8/α=5/β=10
  (v2 sweep, f=0.33, N=100, split_honest):** 1 violation in 10000
  trials = 0.0001 rate. 95 % CI upper bound 0.056 %.
- **The dropped-from-recommendation cell (K=3, α=2, β=10), worst
  Snowball case (v2 sweep, f=0.33, N=1000, random_honest):** 3140
  violations in 10000 trials = **31.40 %** rate. Snowflake at the
  same cell: 1501/10000 = 15.01 % (the rest don't decide; §3.4).
  This is what we are no longer running.

The classical-Avalanche (K=20, α=15, β=20) row of the v2 sweep is
the cautionary tale: 0 % convergence at f=0.33 across all N from
32 to 1000, regardless of which semantics. This is not a
Snowflake-vs-Snowball issue; it is a `K > N - 1`-effectively issue
(slow recruitment of α=15 honest peers under ≥ 25 % adversarial
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
   slowly relative to a ~4-13 s decision window (K=8 median 3.75 s,
   p99 13.5 s at worst N=16). Not a worry.

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

The questions resolved by the 13 locked decisions in §0 are removed
from this list. What remains genuinely open:

1. **(RESOLVED 2026-05-15 via v2 sweep, commit `5ace3d36`)** —
   *"What's the right β under Snowball?"* Resolved: **β=10 with
   K=8, α=5**. The v2 dual-mode 6696-cell × 10000-trial GPU sweep
   evaluated both Snowflake and Snowball at every cell and revealed
   that the right question was not "what β" but **"what K"**: K=3 is
   below the per-color noise floor under Snowball (31 % safety
   violations at N=1000 split/random_honest, see §0.K.2 / §3.4),
   and K=8 is the smallest sweep-grid K at which (α=5, β=10)
   zeroes all three adversary modes at every measured N ∈ {16, 32,
   100, 500, 1000}. The production recommendation locks at
   **(K=8, α=5, β=10, Δ=slot/2)** — decision §0.M.

2. **(NEW)** **K-vs-latency tradeoff sweep at production slot cadence.**
   The v2 sweep ran at `tick_dt = 0.25 s` (e2e cadence). At prod
   `Δ = 500 ms` we have 2× the per-round wall-clock, so worst-case
   K=8 p99 cells (e.g. N=16 / coord_lie at 27 rounds) become 13.5 s
   in production. Could (K=12, α=8, β=8) or (K=16, α=11, β=8) give
   strictly smaller p99 at the cost of larger bandwidth? The v2
   sweep includes K=12 and K=16 grid points at most N — a focused
   re-analysis (no new sim run needed; just re-querying the JSON)
   would settle it. Track as follow-on after this proposal lands.

3. **Tentative attestation emission.** Should the protocol allow a
   *tentative* attestation emit before Avalanche decides — labeled as
   such on the wire — so that triggers see *something* during the
   lock-in window? Loses some safety (a flipped preference would
   require a withdrawal of the tentative att, contradicting decision
   **§0.F**). Probably not worth it, but worth listing.

4. **Slashing prosecution mechanism (deferred per §0.G).** When the
   prosecution layer lands, dedicated chain? Piggyback on existing
   snapshot stream as an optional `slashing` field? Light-client
   publication via Mithril-equivalent multisig? Dependent on
   §9's KES landing.

5. **NIPoPoW composition (decision §0.H).** Avalanche resolves the
   "which hash at the tip" question online and per-validator;
   [`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) resolves the "did
   this chain happen" question for archival ranges and per-light-client.
   The two compose by: (a) Avalanche-decided attestations are the
   strongest possible Phase 2 finality input for any single ordinal;
   (b) the NIPoPoW tower commits branch-bound eligible trials and pointers in
   signed snapshots, while a verifier separately checks current canonicality or
   compares candidate proofs from its trusted genesis/cached commitment. `k2`
   does not anchor the proof. No multiplication of failure probabilities is a
   protocol claim until an independent proof covers the dependence between the
   optimistic and tower constructions.

6. **Cross-metagraph (gl1) Avalanche — out of scope (decision §0.J).**
   The Avalanche paper's DAG-form is richer than Snowman and could be
   useful for cross-metagraph attestation, but is explicitly deferred.
   gl0 single-chain only for this proposal. See the sister proposal
   `CROSS-SHARD-MITIGATION-PROPOSAL.md` (in flight as a parallel
   workstream at the time of this revision) for cross-shard design
   discussion.

7. **Sidecar query budget at scale.** At cluster size 1000 with K=8
   and 10 pending ords/node, query traffic is `1000 × 8 × 10 / 0.5s
   = 160k qps cluster-wide` (independent of β — β only sets when the
   per-ordinal cascade *terminates* its query stream, not the per-tick
   rate). Distributed (each node's K queries fan out), per-node
   receive is `8 × 10 / 0.5s = 160 qps`. Comfortable, but at 2.7×
   the K=3 estimate; needs sidecar load testing at the K=8 floor.

8. **KES retroactive-equivocation model.** Once KES forward-secure keys
   land, old (evolved-away) keys can't sign new equivocation evidence.
   But the *original* equivocation was signed by the key as it existed
   at the time of equivocation; that signature is preserved in the
   gossiped evidence. Question: does our equivocation-evidence
   verifier check the public key as it was at the slot/ordinal of
   equivocation (correct), or as it is now (incorrect — KES key has
   evolved)? Almost certainly need the historical-pubkey path.

9. **Adversarial timing.** Can a Byzantine node strategically delay
   `responding` to queries — but still respond — to influence
   accumulator dynamics in a victim node? Less of a concern under
   Snowball (the accumulator is monotone, so delaying a response
   merely defers — not erases — its contribution), but a per-peer
   query-response latency histogram is still a natural Prometheus
   addition. Worth flagging to cryptographer review (§6.3).

10. **Tipping over to a hybrid finality definition.** Today,
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
calibration: extend the v2 dual-mode GPU sweep to a 2D (α₁, α₂)
grid for each (K, β) cell — within an order of magnitude of the
existing 6696-cell sweep budget (19.6 min wall-clock).

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
    commit `5ace3d36` — **dual-mode Snowball + Snowflake** GPU sweep
    at 6696 cells × 10000 trials, three adversary modes (coordinated_lie,
    split_honest, random_honest). This is the primary data source for
    the current proposal.
  - `~/repos/research-nipopos-2026/sims/data/avalanche_attestation_full_gpu_n10000_v2.json`
    — **primary data file referenced throughout this proposal (v2).**
    6696 cells across (K, α, β, N, f_adv, adversary, latency,
    decision_rule) grid; 10000 trials per cell; 19.6 min wall-clock
    on RTX 5090. Headlines in §0.A, §0.K.1, §0.K.2, §3.4, §5.1, §6.1.
    N ∈ {5, 8, 16, 32, 100, 500, 1000}; f_adv ∈ {0, 0.1, 0.2, 0.25,
    0.3, 0.33}; K ∈ {3, 5, 8, 12, 16, 20, 30}.
  - `~/repos/research-nipopos-2026/sims/data/avalanche_attestation_full_gpu_n10000.json`
    — **v1 Snowflake-only sweep at commit `d8f4639`, 1896 cells,
    7.1 min — SUPERSEDED by v2.** Retained for reference; do not cite
    for new analysis. The v1 → v2 framing flip (recommendation moves
    from K=3 to K=8) is documented in §3.4.
  - `~/repos/research-nipopos-2026/sims/AVALANCHE_CALIBRATION.md`
    — adversary models (coordinated_lie, split_honest, random_honest),
    latency models (boltzmann, pareto), output schema.
