# GKL composition for the integrated Tessellation-Nakamoto stack

**Status:** scaffolding (2026-05-15). Empirical bounds are tagged for mechanical update by sister sims (Snowball / event-stream / NIPoPoW / cross-shard) currently in flight.

This note characterises the safety and liveness of the integrated stack
(Taktikos LDD election + maxvalid-tk chain selection + NIPoPoW level-mu
chains + 4-trigger finality + Avalanche/Snowball attestation cascade
+ metagraph cross-shard aggregation) through the Garay-Kiayias-Leonardos
trinity: **Chain Growth (CG)**, **Chain Quality (CQ)**, and **Common
Prefix (CP)**.

Each section lays out the per-shard bound (the well-trodden direction),
the cross-shard composition, and any open analytical gap. Bounds derived
from existing material are cited inline. New derivations (cross-shard
CQ stake-concentration; tower amplification under L independent VRF
trials) are flagged explicitly.

Cross-references throughout this document:

- Taktikos paper: `~/repos/research-nipopos-2026/paper/main.tex`
- Per-level density notes: `~/repos/research-nipopos-2026/docs/TAKTIKOS-NOTES.md`
- Depth-k sims: `~/repos/research-nipopos-2026/sims/FINDINGS_adv_depth_expanded.md`,
  `sims/adv_delay_sweep.py`, `sims/FINDINGS_adv_7block_private.md`
- Snowball sim: `~/repos/research-nipopos-2026/sims/AVALANCHE_CALIBRATION.md`,
  `sims/data/avalanche_attestation_full_gpu_n10000.json` (commit `d8f4639`)
- NIPoPoW integration: `docs/nakamoto/NIPOPOW-PROPOSAL.md` (commit `bcb42140`)
- Attestation cascade: `docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md` (commit `be1c8250`)
- Finality phase model: `docs/nakamoto/attestation-and-finality.md`
- Trigger typeclass: `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/FinalityTrigger.scala`

---

## 1. Setup

**Notation.**

| Symbol | Meaning |
|---|---|
| `N` | total validators in the active stake-registry set |
| `S` | number of shards (metagraphs + global L0) |
| `r_s` | active stake fraction in shard `s` |
| `r` | total active stake; `r = Σ_s r_s` |
| `alpha` | adversary stake fraction (global) |
| `K`, `α_cascade`, `β` | Snowball sample size, majority threshold, decision margin |
| `k₁` | depth-1 finality parameter; production default `255` |
| `k₂` | archival depth; production default `65536` |
| `L` | NIPoPoW level count; default `10` (L0..L9) |
| `ψ_L0, γ, f_A, f_B` | LDD snowplow parameters; production defaults `0, 15, 0.5, 0.05` |
| `ψ_super` | per-super-level dormant period; default `1` |
| `(p_µ^max, σ_µ)` | per-level shifted-exponential params (paper Table 1) |
| `R` | eta rotation period in snapshots; production `2550` |
| `Δ` | network message delay bound |

The L0 LDD threshold is the paper's snowplow (eq. 2):

```
f(δ) = 0                              δ < ψ_L0
       f_A · (δ − ψ_L0)/(γ − ψ_L0)    ψ_L0 ≤ δ < γ
       f_B                            δ ≥ γ
```

with per-validator eligibility `φ(δ, α) = 1 − (1 − f(δ))^α`. Production
defaults `(ψ_L0, γ, f_A, f_B) = (0, 15, 0.5, 0.05)` are sourced from
`LddConfig.Default`. **`f_B = 0.05` is fixed for this analysis** —
see `project_sharding_strategic_signals`; this document does not propose
LDD parameter changes.

**Adversary model.** Static 1/3-bounded by stake (`alpha ≤ 1/3`), can
delay messages up to `Δ` but cannot equivocate VRF outputs (because they
are committed via signed slot certificates and rotated through KES). The
adversary is permitted any internal coordination, including
stake-concentration into a target shard (§5).

---

## 2. Per-shard GKL bounds

### 2.1 Chain Growth (per shard)

At any slot with snowplow gap `δ`, the probability that no validator in
shard `s` is eligible is

```
P[no extension | δ] = (1 − f(δ))^{r_s}
```

The expected next-block gap at fill rate `f_eff ≈ 15%` (paper §2.1
parameters) is ≈ 7.7 slots when summed across honest stake at full
density — a single honest staker with `r_s = 1` extends one base block
per `E[gap] ≈ 7.7` slots. Cite **paper §3 Adversary Dilemma proof
(Theorem 4.1)**, which derives the per-slot weight rate
`r(δ) ∝ δ^{α−1} · M(δ)` from this gap distribution.

[empirical, source: `sims/RESULTS.md` row "L0 = 0.140 / 0.147",
status: current] The L0 fill at 5-staker 10M-slot simulation is 14.7%,
within 5% of the analytic 15% target.

### 2.2 Chain Quality (per shard)

The Taktikos paper proves μ-CQ at the per-shard level via the
**Burst Resistance Theorem (Thm 4.2 — `thm:burst`)**: an adversary
forging at slot gap `δ = 1` contributes per-block weight smaller than
honest by a factor of `δ̄^α` where `δ̄ ≈ 7` is the honest mean gap.
At `α = 2` this is a 49× weight disadvantage per block.

The **Super-Level Suppression Theorem (Thm 4.4 — `thm:suppression`)**
extends this: an adversary at `δ = 1` has its super-level hit
probability scaled by `s(1) = 1/γ ≈ 0.067` — a **93% per-level
suppression** independent of stake fraction.

The single-shard CQ bound in any window of length `λ ≥ k₁`:

```
fraction of honest-authored blocks ≥ (1 − alpha) − ε(alpha, λ)
```

where `ε → 0` exponentially in `λ` under the Taktikos model. The
paper's separation theorem (§4.3, `sec:separation`) shows that
**LDD gating asymmetry alone — independent of weighting — improves the
single-chain consistency bound by ≈ 20%** over static-threshold PoS.

[empirical, source: `paper/figures/fig5_strategies.pdf`, status: current]
Honest win rate against moderate adversary (gap 4..7): **75.3% with
gating**, **51.7% without** at 50-block races, `α = 2`.

### 2.3 Common Prefix (per shard)

The depth-1 trigger `T_depth1` enforces `tip.ord − lastFinalizedOrdinal
> k₁ = 255`. Per-attempt reorg risk at this depth under a 1/3 adversary
at zero network delay:

[empirical, source: `sims/FINDINGS_adv_depth_expanded.md`, 10M trials,
status: current] Measured tail through `k = 134`; weighted-LS fit
slope `−0.040 ± 0.001 log10/block`; extrapolated `risk(k = 255) ≈
3 × 10⁻¹¹`, `risk(k = 277) ≤ 10⁻¹²`. Headroom-safe recommendation
**`k = 290`** absorbs a 9-block model-range uncertainty (`k=275..284`)
plus 6 blocks against tail softening.

**Sensitivity to network delay** — degrades sharply:

[empirical, source: `sims/adv_delay_sweep.py` at `k = 31`, status: current]
Per-attempt risk doubles approximately every `Δ = 2.5 s`; at `Δ = 5 s`
risk is ≈ 2× the `Δ = 0` baseline; at `Δ = 8 s` it is catastrophic
(approaching 50% for moderate adversaries). This is the binding
operational constraint on the gossip layer.

The depth-2 trigger `T_depth2` enforces `k₂ = 65536` for ARCHIVAL
phase. At this depth CP-violation probability is structurally bounded
by the `f_B = 0.05` stationary tail: each additional decade requires
~24.7 blocks (cf. §2.3 fit slope), so `k₂ = 65536` ≫ `255` gives
many orders of margin beyond the 10⁻¹² target.

---

## 3. Snowball attestation tightens CP at the tip

### 3.1 The Snowball CP bound

The Snowball cascade (per `docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md`,
parameters `(K=3, α_cascade=2, β=10)`) provides an **additional CP bound
at the tip**: for each ordinal, honest validators converge on a single
hash within `O(β)` rounds. The proposal's empirical operating point
gives:

[empirical, source: `sims/data/avalanche_attestation_full_gpu_n10000.json`
(commit `d8f4639`), status: current]
At `f_adv = 0.33`, coordinated_lie adversary, Boltzmann latency
`δ̄ = 50 ms`, tick `Δ = 250 ms`:

| N | convergence | safety_viol | p50 / p99 rounds |
|---|---|---|---|
| 5..1000 | 1.0000 | 0.0000 | 11..13 / 12..14 |

Under split_honest the Snowflake variant leaks 9.85..16.71% safety
violations at N ∈ {100..1000}; the Snowball flip (decision K in the
proposal) is **empirically required at production scale**, with re-sim
under Snowball semantics in flight.

The Snowball-CP bound takes the form

```
P[safety violation at ordinal N] ≤ ε_snowball(K, α_cascade, β, f_adv)
```

where `ε_snowball` is the per-ordinal disagreement rate measured by
the calibration sweep. For the locked operating point
`(K=3, α_cascade=2, β=10)` under coordinated_lie at the production
target `f_adv ≤ 0.33`, the measured value is `ε_snowball = 0` across
N ∈ {5..1000} with 10k trials per cell. The 95% Wilson upper bound is
**`ε_snowball ≤ 3.7 × 10⁻⁴`** at this sample size; tighter bounds
require larger sweeps.

### 3.2 The 4-trigger composition

The Phase 1 → 2 transition fires on the **max-of** the three
parallel triggers (`FinalityTrigger.maxLatestQualifyingOrdinal`):

- `T_weight` — 2/3 stake-weighted attestation
- `T_count` — 2/3 distinct-attester count (self-excluded, canonical-hash-filtered)
- `T_depth1` — `bestTipOrd − k₁` structural depth fallback

Phase 2 → 3 fires on `T_depth2 = bestTipOrd − k₂`.

The combined CP bound at the tip is

```
P[CP violation past tip-anchored ordinal N]
    ≤ min( ε_snowball, ε_chain(k₁), ε_chain(k₂) )
```

where the strongest available trigger dominates. Specifically:

- Within ~5 s of production, Snowball converges (median 11–13 rounds
  × 250 ms = 2.75..3.25 s). At this point ε_snowball is the binding
  bound on tip CP.
- Within `k₁ = 255` ordinals of depth, `T_depth1` fires; ε_chain(k₁)
  binds and is the floor for non-attestation-driven finality.
- At `k₂ = 65536` depth, `T_depth2` binds; ε_chain(k₂) ≪ 10⁻¹².

The tiered guarantee: **the strongest available trigger fires first**.
A node in a partition with too few peers for Snowball / T_weight to
converge still finalises via T_depth1 (operationally; the cluster
might wait `k₁ × E[gap] ≈ 255 × 7.7 ≈ 33 min` for the structural
fallback). A fully-connected cluster sees Snowball + T_count within
seconds.

[empirical, source: `paper/main.tex` §5.3 Strategy comparison Table 4,
status: current] Honest win % at fork-length 5 blocks (the
smallest-window CP regime) is **98%** under Taktikos LDD + gating
vs **52%** under Praos flat — same gating, different L0 election.
The LDD difficulty gradient is load-bearing for early-window CP.

---

## 4. NIPoPoW level-μ chains: amplification, not weakening

### 4.1 The independent-trials construction

Each base-block extension runs **L independent VRF trials** (NIPoPoW
proposal §2.1, paper §3.1):

```
τ_µ(S) := H_512(ρ_S ‖ "TEST-" ++ µ) / 2^512    µ ≥ 1
```

A snapshot `S` is a level-µ superblock iff `τ_µ < θ_µ^eff(g_µ, δ_S)`,
where `θ_µ^eff(g_µ, δ_S) = θ_µ(g_µ) · min(1, δ_S/γ)` is the shifted-
exponential threshold gated by the L0 slot gap. Per paper §3.1 and the
proposal's "degrees of freedom" argument, the construction is
**independent, not nested**: hitting L3 does not imply L1.

### 4.2 The exponential-rarity amplification argument

For a level-µ superblock to be a valid tower entry at base ordinal `N`,
the adversary must simultaneously:

1. **Win the L0 trial** at the chosen slot gap (probability `f(δ)·r_s`
   for stake `r_s`).
2. **Win the level-µ trial** against `θ_µ^eff(g_µ, δ_S)`.
3. **Have stake at slot `sl_N`** (i.e. the slot when `S` was produced).

Per paper Table 1 + `docs/TAKTIKOS-NOTES.md` row "L9", target conditional
density at level 9 is `1/2^9 ≈ 0.2%` (achieved 0.16% at 10M slots).
Each level-µ trial succession has probability ≈ `θ_µ^eff` per slot.
Because the L trials are domain-separated by `H_512(ρ ‖ "TEST-µ")`,
each test draw is independent of the others; **the adversary cannot
piggyback a level-µ win on a level-(µ−1) win**.

The per-shard per-tower-entry forgery probability is bounded by:

```
P[forge level-µ entry at base ord N | stake r_adv]
    ≤ r_adv · f(δ̄) · θ_µ^eff(g_µ, δ̄)
    ≈ r_adv · f(δ̄) · (1/2^µ) · s(δ̄)
```

This is **exponentially decreasing in µ** at the per-attempt level.
Per the proposal §1.2, classical NIPoPoW PoS analogues (`τ < 2^{−µ}`
flat conditional) give the same density but **zero security gain over
chain length** — an adversary producing N base blocks automatically
gets N/2^µ level-µ "superblocks". The LDD-anchored construction
breaks this by making each level a real independent eligibility test:
an adversary forging at gap δ = 1 scores **zero** on all super-levels
(burst-zero via ψ_super = 1 and via slot-gap gating
`s(1) = 1/γ ≈ 0.067`).

The "degrees of freedom" argument formalised:

> To forge a tower of length T at level µ, an adversary needs T
> successful level-µ trials in a row. Each trial has independent
> probability ≤ `1/2^µ · s(δ_adv)`. The forging chain must also remain
> chain-eligible at L0 — i.e. an unbroken sequence of L0 wins under
> `f(δ)`. The compound probability is the product of two independent
> exponentially-rare event streams, giving per-tower-step rarity
> approximately `r_adv · f(δ̄) · 1/2^µ · s(δ_adv)`.

This **amplifies, not weakens** the per-shard CP bound at the
tower-anchored ordinals. For a level-µ tower entry, the adversary
needs `2^µ` more attempts than at L0 to forge a counterfeit. The
NIPoPoW tower is **strictly additional evidence** atop per-shard CP;
removing tower entries does not weaken `T_depth1` or `T_depth2`.

### 4.3 What this means at the integration level

NIPoPoW tower entries are produced at the same rate as snapshots
(L0 = 100%, L1 = 50%, ..., L9 = 0.2%), but **anchoring** is gated to
Phase 3 (paper §5, proposal §5.1). A light client verifying a
tower-mediated chain proof inherits per-shard CP at the anchor
ordinals, and the level-µ rarity gives proof-size amplification
(O(m·log N) headers per the proposal §5.7).

The proposal's `T_depth2` hook is the **only place** tower entries are
trusted. The cumulative-weight function `W(C) = Σ φ(δ)^α · (1 + Σ 2^µ
· 𝟙[hit])` (paper §3.3) is **never** consulted by chain selection;
chain selection stays on `maxvalid-tk` to preserve grinding resistance
(paper §5.5 Fig 9 falsification — gap-based weight collapses settlement
at 10% adversary stake under nothing-at-stake grinding).

---

## 5. Cross-shard composition

`S` shards each produce per-shard sub-snapshots; the global L0
aggregates via metagraph state-channel binaries (commits `877963f4`,
`40dc4c2b`). Cross-shard GKL composition:

### 5.1 Cross-shard CG

Under independent VRF, cross-shard CG is the intersection of per-shard
CGs:

```
rate_cross = min_s rate_s
```

If all shards have equal `r_s = r/S`, this reduces to per-shard
rate (the slowest shard binds). The honest cross-shard rate at
`S = 5` shards, equal stake, `f_eff = 15%`, becomes one global L0
aggregation per `max_s E[gap_s] ≈ 7.7` slots — unchanged vs single-
shard rate, modulo aggregator overhead.

[derived here, status: TODO empirical confirmation by `sims/cross_shard.py`]
Under unequal stake distribution `(r_1 ≥ r_2 ≥ ... ≥ r_S)`, the
slowest shard binds; expected slowest-shard gap scales as
`1/(f_eff · r_S)`. A shard with `r_S = 1/(2S)` (half the equal-stake
allocation) sees gap ≈ `2 · E[gap_s] ≈ 15.4` slots.

### 5.2 Cross-shard CQ — the stake-concentration attack

**This is the load-bearing safety concern for the cross-shard
composition.**

An adversary with global stake `α_total` can **concentrate** all of
that stake into a single target shard. If the target shard has
"home" stake fraction `r_s_home` and the adversary contributes
`α_total · r` units, the adversary's **local** stake fraction in the
target shard becomes:

```
α_local = α_total · r / (r_s_home · r + α_total · r)
        = α_total / (r_s_home + α_total)
```

For `S` equal shards with `r_s_home = r/S`:

```
α_local = α_total / (1/S + α_total)
       = (α_total · S) / (1 + α_total · S)
```

This dominates 1/3 when `α_total · S > 1/(2)`, i.e.

```
α_total > 1 / (2S)
```

Worked examples (cross-shard CQ collapse threshold):

| S | α_total threshold for `α_local > 1/3` | α_local at α_total = 1/3 |
|---|---|---|
| 3 | 1/6 ≈ 0.167 | 1/2 (50% local in concentrated shard) |
| 5 | 1/10 = 0.10 | 5/8 ≈ 0.625 |
| 10 | 1/20 = 0.05 | 10/13 ≈ 0.769 |

**Derived:** if the adversary can concentrate fully, the per-shard CQ
bound that holds at global `α_total` is dominated by the per-shard
behaviour at `α_local`. For `S ≥ 3` and `α_total > 1/(2S)`, the
targeted shard's per-shard CQ collapses even though `α_total ≤ 1/3`.

This is the analytical gap that the sharding workstream must address
(see §6 open questions). Without a defence — VRF-sortition of
validators into shards, random shard reassignment per epoch, stake-
weighted shard sampling — the cross-shard CQ bound is dictated by the
attacker's concentration freedom, not by the global stake fraction.

### 5.3 Cross-shard CP

Cross-shard CP composes via the aggregator. Two regimes:

**Regime A — Aggregator waits for per-shard `T_depth1`.** Each
per-shard sub-snapshot is included by the global L0 only after the
shard's local `T_depth1` fires (depth `k₁` deep in the shard's
chain). Cross-shard CP then inherits per-shard CP:

```
ε_cross_CP ≤ Σ_s ε_chain_s(k₁) + ε_global_CP(k₁_global)
```

The union bound across shards is tight because per-shard fork events
are independent under independent VRF.

**Regime B — Aggregator trusts only Snowball at the sub-snapshot
boundary.** Earlier inclusion (one Snowball convergence per
sub-snapshot, ~3 s) reduces latency but **caps cross-shard CP at
Snowball-CP**:

```
ε_cross_CP ≤ Σ_s ε_snowball_s + ε_global_CP
```

The current proposal architecture: gl0 is the cross-shard aggregator;
per-shard sub-snapshots are state-channel binaries finalised at the
shard's gl1/cl1/ml0 level then pulled by gl0 via the
`pullFinalityGated` interface (per `attestation-and-finality.md` §0.4,
"G1 follower consumption" row). G1 = Phase 2 boundary, so today's
deployment runs **Regime A** (waits for per-shard Phase 2 / Settled
before cross-shard inclusion).

The "global L0 is a shard of shards" framing is explicit in the
proposal: gl0 runs its own Taktikos election + maxvalid-tk + finality
triggers atop the aggregated per-shard inputs. Cross-shard CP
composes hierarchically: each shard's `T_depth1` ⇒ shard inclusion in
gl0; gl0's own `T_depth1` ⇒ cross-shard CP.

---

## 6. Open questions & analytical gaps

These are enumerated for the sharding workstream and the Tier-2 plan.
This document does not attempt to close them.

### 6.1 Stake-concentration mitigation

§5.2's cross-shard CQ collapse is the binding cross-shard safety
question. Candidate defences:

- **VRF-sortition of validators into shards.** A validator's
  per-epoch shard assignment is derived from VRF(sk, epoch_seed),
  preventing self-selection. Open: assignment cadence (per-epoch?
  per-eta-period?), reassignment cost vs grinding-resistance trade-off.
- **Random shard reassignment per epoch.** Mitigates static-shard
  concentration but introduces hand-off cost (state migration,
  re-attestation churn). Open: stake-weighted vs uniform reassignment.
- **Stake-weighted shard sampling at the aggregator.** gl0 weights
  per-shard sub-snapshot trust by the shard's `r_s`. Marginal —
  doesn't prevent concentration but reduces its CQ impact.
- **Hybrid.** Combine VRF-sortition for validator placement with a
  proof-of-stake-weighted minimum threshold per shard.

### 6.2 Aggregator's own GKL bounds

The proposal makes "gl0 is a shard of shards" implicit. Should be
made explicit:

- **CG_global** is the gl0 Taktikos rate, not the per-shard rate.
- **CQ_global** is dictated by the gl0 validator set, which is the
  full global validator set (gl0 has no stake-locality concept today).
- **CP_global** is the gl0 `T_depth1` / `T_depth2`. Per-shard CP
  composes into CP_global via the union bound in §5.3.

The cross-shard analysis is then literally per-shard analysis applied
twice: once at each metagraph, once at gl0.

### 6.3 Latency model

Cross-shard messages must traverse global L0 — sub-snapshots are
finalised at the shard, pulled by gl0, included in gl0's aggregation,
then echoed back to the destination shard. This adds **3+ snapshots
of latency** to any cross-shard read:

```
shard A produces tx → shard A T_depth1 (~k₁ × E[gap] = 33 min)
                   → gl0 pulls (Phase 2 boundary)
                   → gl0 includes in own snapshot
                   → gl0 T_depth1
                   → shard B observes
```

The 33-minute single-trigger latency at `k₁ = 255` is structural; the
attestation-driven triggers (`T_weight` / `T_count`) shorten this to
seconds when they fire, but the depth fallback is the binding upper
bound. Tier-2 work: characterise the conditional latency
distribution under varying attestation success rates.

### 6.4 Delayed-finality and cross-shard view consistency

When `T_depth1` fires in shard A before Snowball converges (e.g. in a
partition where shard A has < `K` peers), shard A advances to Phase 2
without attestation consensus. If gl0's view of shard A is gated on
`pullFinalityGated`, gl0 sees shard A's depth-fallback result and
includes it.

A separate honest minority of shard A peers may have a different
canonical-hash view (Snowball would have flipped them, but couldn't
converge with too few peers). When the partition heals, the minority
peers' view differs from gl0's included view.

**Open:** does this break cross-shard view consistency, or does the
gl0 inclusion canonicalise the partition's "wrong" tip? The
`MptOverlay` multi-branch design (the "balmy branching" plan) is the
substrate that mitigates this — gl0 carries multi-branch state and
can re-resolve in the union-of-partitions view. Whether the
cross-shard composition preserves this property under all partition
patterns is unproven.

### 6.5 Historical stake-distribution snapshotting

A NIPoPoW light client verifying tower entries dated to eta period
`j` needs the stake distribution at period `j` (per proposal §3.3).
No mechanism today snapshots this; it is a prerequisite for KES-gated
tower anchoring. The same question is unavoidable for any
stake-weighted protocol (Mithril, BLS aggregate sigs).

### 6.6 Tower L beyond 9

The paper validates L = 10 up to 10M slots. For an indefinite-lifetime
chain at ~2,500 ord/day, levels above L9 might be useful 5+ years
out. Header overhead scales linearly: L=10 → 480 bytes;
L=15 → 720 bytes. Open: when (if ever) to add L10..L15.

---

## 7. Summary table

| Property | Per-shard bound | Cross-shard composition | Empirical anchor |
|---|---|---|---|
| **CG** | `1/E[gap]` ≈ 1 block per 7.7 slots at `r_s = 1` | `min_s rate_s` (independent VRF) | `sims/RESULTS.md` L0 14.7% fill |
| **CQ** | Honest fraction ≥ `(1 − α_local) − ε(α_local, λ)`; LDD gating gives ≈ 20% improvement over flat-PoS | Dominated by `α_local = α_total / (r_s_home + α_total)` under concentration attack; collapses at `α_total > 1/(2S)` | `paper/fig5_strategies` 75.3% honest win @ moderate adv |
| **CP tip** | `ε_snowball ≤ 3.7 × 10⁻⁴` at 10k trial; expected `< 0.1%` post-Snowball re-sim | Regime A: `Σ_s ε_snowball_s + ε_global_CP`; Regime B: gl0 trusts per-shard SETTLED | `sims/data/avalanche_attestation_full_gpu_n10000.json` |
| **CP depth-k₁** | `≈ 3 × 10⁻¹¹` at `k = 255`, `Δ = 0` | Inherited via aggregator wait | `FINDINGS_adv_depth_expanded.md` extrapolation |
| **CP depth-k₂** | `≪ 10⁻¹²` at `k = 65536` | Inherited | structurally bounded by f_B tail |
| **NIPoPoW level-µ rarity** | `r_adv · f(δ̄) · 1/2^µ · s(δ̄)` per slot per level | Per-shard (no cross-shard tower in v1) | `paper/main.tex` Table 1, `sims/RESULTS.md` |

---

## 8. References

### Repository

- `paper/main.tex` §3 (Construction), §4 (Security), §5 (Evaluation)
- `paper/main.tex` Theorem 4.1 (Adversary Dilemma), 4.2 (Burst
  Resistance), 4.4 (Super-Level Suppression)
- `paper/main.tex` Fig 9 (`fig:grinding_comparison`,
  `paper/figures/weight_grinding_intuition.pdf`)
- `docs/TAKTIKOS-NOTES.md` §"KEY INSIGHT: Per-Level LDD is the Novel
  Contribution" — degrees-of-freedom argument
- `sims/RESULTS.md` — per-level density validation at 10M slots
- `sims/FINDINGS_adv_depth_expanded.md` — depth-k = 255 extrapolation
- `sims/FINDINGS_adv_7block_private.md` — 7-block private-fork baseline
- `sims/adv_delay_sweep.py` — `Δ` sensitivity at k = 31
- `sims/AVALANCHE_CALIBRATION.md` + `sims/data/avalanche_attestation_full_gpu_n10000.json`
  — Snowball calibration

### Tessellation-Nakamoto

- `docs/nakamoto/NIPOPOW-PROPOSAL.md` (`bcb42140`) — NIPoPoW for Taktikos
- `docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md` (`be1c8250`) —
  Snowball cascade
- `docs/nakamoto/attestation-and-finality.md` — 4-phase model +
  trigger stack
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/FinalityTrigger.scala`
  — typeclass + four concrete triggers

### Foundational

- Garay, Kiayias, Leonardos. *The Bitcoin Backbone Protocol*. EUROCRYPT 2015.
- David, Gaži, Kiayias, Russell. *Ouroboros Praos*. EUROCRYPT 2018.
- Schutza, Behrens, Duong, Aman. *Ouroboros Taktikos*. FC 2023.
- Kiayias, Miller, Zindros. *Non-Interactive Proofs of Proof-of-Work*. FC 2020.
- Rocco et al. *Snowflake to Avalanche*. 2018.
- Amores-Sesar, Schneider. *An Analysis of Avalanche Consensus*. arXiv:2401.02811, 2024.
- Lewis-Pye et al. *Frosty*. arXiv:2404.14250, 2024.

---

*Scaffolding. Empirical bounds tagged `[empirical, source: <file>,
status: TODO|current]` for mechanical update by sister sims. Tier-2
work follows from §6 open questions.*
