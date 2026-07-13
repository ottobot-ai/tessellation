# NIPoPoW for Tessellation-Taktikos — design proposal

**Status:** research proposal. No code commitment in this document — sizing only.
**Date:** 2026-05-15.
**Scope:** the light-client / finality-cert workstream, as an alternative to porting Mithril.

> **2026-07-12 integration correction:** the paper controls the tower
> construction, not Tessellation phase policy. Any Phase-3, `T_depth2`, or
> immutable-`k2` integration language below is rejected. The target commits
> branch-bound trial/pointer state in signed snapshots, verifies proofs from a
> trusted genesis or cached canonical commitment, and separately proves current
> canonicality/compares candidate chains. `k2` is retention capacity only.

## Provenance

This proposal is the implementation-side translation of the user's
paper-in-progress

> *Superblock Proofs for Proof-of-Stake: How Local Dynamic Difficulty Enables NIPoPoW-Style Light Clients*

in the companion repository [`~/repos/research-nipopos-2026`](../../../research-nipopos-2026).
The paper, its 11 reproducible figures, and the simulation results under
`sims/` are the **authoritative ground truth** for the construction.
This document is the engineering deliverable that takes the paper and
answers "what does it look like to put this into Tessellation-Nakamoto,
and when?".

A previous draft of this document (`2b697941`) was written against
**classical PoW NIPoPoW** (Kiayias-Miller-Zindros 2017) with **nested
rarity** — block at level µ iff `H(B) ≤ T · 2^(-µ)` so a µ-superblock
automatically inherits all lower levels. That is the wrong model for
Taktikos. The paper rejects the PoS-analog (flat conditional `τ < 2^(-µ)`)
on the explicit grounds that it provides **zero security advantage over
chain length** (`paper/main.tex` §3.1, "Why not flat conditional
probabilities?"). The refactor below replaces that framing with the
paper's actual construction: **L independent VRF trials per slot with
domain-separated inputs**, each gated independently by the LDD ramp.

Key paper artifacts referenced repeatedly here:

| Artifact | Role |
|---|---|
| `paper/main.tex` §3.1 (Multi-Level Eligibility) | The L independent trials, shifted-exponential thresholds, ψ=1 burst zone |
| `paper/main.tex` §3.2 (Slot-Gap Gating) | L0-gap coupling via `min(1, δ/γ)` |
| `paper/main.tex` §3.3 (Cumulative Weight) | `maxvalid-weighted` heaviest-chain rule + α=2 amplification |
| `paper/main.tex` §3.4 (Subchain State) | 48L-byte per-header `S = (S_0, …, S_{L-1})` vector |
| `paper/main.tex` §4.3 (Separation Theorem) | Election vs weight; LDD gating → 20% consistency, weighting → proof size |
| `paper/main.tex` §4.4 (Suppression Theorem) | Burst loss 93% / honest loss 53% at the gating step |
| `paper/main.tex` §5.5 + **Fig 9** (`fig:grinding_comparison`) | **Headline empirical falsification** — ramp weight collapses settlement at 10% adversary stake vs 30% for plain LDD |
| `paper/main.tex` §5.6 + Figs 10/11 | Non-portability — Praos flat collapses to 69% honest-win |
| `paper/main.tex` §5.7 | Light-client overhead — `≈ 5,100` headers for a 1.43M-block chain at `(m=50, k=100)` |
| `paper/main.tex` §6 ("Independence vs. nesting") | Independent level assignments — a block may hit L3 without hitting L1 |
| `docs/TAKTIKOS-NOTES.md` §"KEY INSIGHT: Per-Level LDD is the Novel Contribution" | Degrees-of-freedom argument |
| `sims/RESULTS.md` | Per-level density validation at 10M slots |
| `sims/run_grinding_ci.py` + `sims/weight_grinding_intuition.py` | Fig 9 generators |

The construction in §2 below mirrors the paper's. Everything outside §2 —
the KES interplay, the trust-anchor protocol, the composition with our
4-phase finality model, and the implementation/timing — is engineering
material new to this document.

---

## §1 — Headline result: why this construction, not classical NIPoPoW

### 1.1 Fig 9 — the falsification

`paper/main.tex` Fig 9 (`fig:grinding_comparison`,
`paper/figures/weight_grinding_intuition.pdf`) is the empirical falsification
that motivates this entire redesign. Three schemes are run against a
grinding (nothing-at-stake, branch-proliferating) adversary; settlement
violation probability is plotted against adversary stake:

| Scheme | Adversary stake at which settlement collapses |
|---|---|
| Static-threshold PoS, block-count chain selection | ≈ **35%** |
| Taktikos LDD, block-count chain selection (no ramp weighting) | ≈ **30%** |
| Taktikos LDD + ramp weight `w(g) = g/δ̄`, weight-based selection | ≈ **10%** |

The surprise: **adding gap-based ramp weighting REDUCES grinding
resistance from ~30% to ~10% adversary stake.** A naive port of PoW-style
cumulative-work selection to PoS — exactly what a classical NIPoPoW
translation would do — collapses settlement under a grinding adversary
who exploits branches with accumulated "weight debt".

This is *the* result that forces the separation theorem (§4.3): the
LDD-gated election function governs settlement; the cumulative-weight
function governs only the light-client proof size, never settlement
chain selection. **Settlement chain selection stays on `maxvalid-tk`
(longest, then lower head slot).** Cumulative weight is a NIPoPoW
engineering parameter, not a chain-selection rule.

### 1.2 Why NIPoPoW over Mithril

Engineering surface:

| | Mithril | NIPoPoW (this construction) |
|---|---|---|
| Crypto primitives | BLS12-381 (pairings, aggregation) + concentrated-stake lottery | Merkle trees (already have) + VRF outputs (already produce) |
| New consensus participants | Mithril signer subset + aggregator | None — every snapshot producer is implicitly a participant |
| Coordination | Threshold multi-sig requires liveness of ≥ k signers per epoch | None — proof is built post-hoc from existing chain data |
| Proof type | Constant-size certificate per checkpoint | Logarithmic-size superblock tower |
| New code modules (rough order of magnitude) | BLS12-381 pairing library, signer protocol, aggregator service, on-chain registry, cert-distribution gossip — multi-month port | Tower data structure + builder + verifier — see §6 |

Trust model is equivalent: both schemes assume non-overwhelming
adversary fraction (≤ 1/3 for our LDD parameters, sim-validated to
k=255 risk ≈ 10⁻¹¹) and KES forward-security for retroactive-rewrite
resistance. Neither defends against a long-range adversary with an
unrevoked historical signing key — KES does, shared between schemes.

Proof size: Mithril is O(1) per checkpoint and wins on raw bytes at
long lookbacks (Cardano: ~73 certs/year at 5d cadence). NIPoPoW is
O(m · log N) — paper §5.7: `≈ 5,100` headers for a 1.43M-block chain
at `(m=50, k=100)`, a 280× reduction. NIPoPoW wins on proof flexibility:
each tower entry is an arbitrary-ordinal anchor, so light clients can
query "is tx T in snapshot N?" for any N in the tower, not only at
certificate boundaries.

### 1.3 What we already have that makes NIPoPoW close-to-free

1. **VRF outputs at every snapshot.** `EligibilityChecker.vrfOutputAsRatio`
   already produces `Ratio ∈ [0, 1)` from each snapshot's VRF output via
   `BigInt(1, vrfOutput) / 2^512`. This is exactly the input distribution
   that each level-µ test consumes after domain-separated rehashing.
2. **A Merkle Patricia Trie (MPT) substrate** for snapshot-indexed state.
   The same MPT primitive that supports state-proof verification
   (`MptOverlay`, `MptStore`, `HistoricalMptProofService`) is the natural
   store for the per-level subchain pointers.
3. **The 4-phase finality model.** Phase 3 (ARCHIVAL) is explicitly
   reserved as the tower-anchoring phase, wired against `T_depth2`
   (commit `06455f98`, default k₂ = 65536). See
   `attestation-and-finality.md` §5.4.

NIPoPoW is "almost free" relative to Mithril precisely because these
three primitives are already in production.

---

## §2 — NIPoPoW construction for Taktikos

This section restates the paper construction, using our terminology and
linking each piece back to existing protocol artifacts.

### 2.1 Multi-level eligibility — L independent VRF trials per slot

For each snapshot S, let `ρ_S` be its 64-byte VRF output (produced by
`EligibilityChecker.checkEligibility` and embedded in the snapshot's
slot-cert). **The producer runs L independent eligibility trials, one
per level µ ∈ {0, 1, …, L-1}**, all derived from the same `ρ_S` via
**domain-separated rehashing** (`paper/main.tex` §3.1, eq. 1 + eq. 5):

```
τ_0(S) := Blake2b512(ρ_S ‖ "TEST")        / 2^512        // base-level trial
τ_µ(S) := Blake2b512(ρ_S ‖ "TEST-" ++ µ)  / 2^512   µ ≥ 1 // level-µ trial
```

Pass condition at level µ:

```
S is a level-µ superblock  ⟺  τ_µ(S) < θ_µ^eff(g_µ, δ_S)
```

where `θ_µ^eff` is the L0-gap-gated per-level threshold (§2.2 below)
and `g_µ` is the **base-block gap** (snapshots since the previous
level-µ hit, **not slots**). The level-0 trial is the standard Taktikos
base-chain eligibility against the L0 snowplow `f(δ)`; levels 1..L-1
are super-level trials with their own per-level threshold curves.

**This is L independent trials, not nested rarity.** Crucially:

- **No nesting.** A snapshot may hit L3 without hitting L1. Independence
  is a deliberate construction choice (paper §6 "Independence vs. nesting"):
  > *"This construction produces independent level assignments (a block
  > may hit L3 without hitting L1), unlike PoW where µ-superblock status
  > implies (µ-1)-superblock status. Independence strengthens security
  > by requiring the adversary to satisfy L independent constraints
  > simultaneously, and prevents a fortuitous high-level hit from
  > automatically boosting all lower levels."*
- **No "rarity-by-leading-zeros" formulation.** The classical PoW NIPoPoW
  rule (`H(B) ≤ T · 2^(-µ)` → µ-superblock; µ → (µ-1) automatically) does
  not transfer: the PoS analog `τ < 2^(-µ)` gives correct densities but
  zero security gain over chain length (paper §3.1 "Why not flat
  conditional probabilities?").
- **Degrees of freedom.** Per the user (and paper §4 + `docs/TAKTIKOS-NOTES.md`
  §"KEY INSIGHT"), running each level as a separate trial against a
  domain-separated VRF input **increases the degrees of freedom an
  adversary must bias** to forge a tower. To produce a counterfeit
  level-µ superblock, the adversary must win the level-µ trial
  specifically — they cannot piggyback on a level-(µ-1) win. Combined
  with (i) per-level base-block-gap thresholds and (ii) L0 slot-gap
  gating, no single forging strategy maximizes multiple levels
  simultaneously — the paper's Adversary Dilemma theorem (4.1).

The trial set is **L tests per slot, one VRF output, L domain
separators**, computed at production time.

### 2.2 Per-level thresholds

Two thresholds compose into the effective per-level eligibility used in
§2.1's pass condition.

**1. Shifted-exponential threshold over the base-block gap `g_µ`**
(snapshots since the previous level-µ hit; paper §3.1 eq. 3):

```
θ_µ(g_µ) = p_µ^max · (1 − exp(−(g_µ − ψ_super) / σ_µ))   if g_µ ≥ ψ_super
         = 0                                              if g_µ <  ψ_super
```

with **`ψ_super = 1` for all super-levels** (the dormant period from
`paper/main.tex` Table 1) and per-level `(p_µ^max, σ_µ)` from the same
table (validated at 10M slots, achieved-vs-target rates within 1–13%
per level):

| Level | `p_µ^max` | `σ_µ` | Target density | Achieved (10M slots) |
|---|---|---|---|---|
| L1 | 1.131 | 0.50 | 50.0% | 49.4% |
| L2 | 0.346 | 0.50 | 25.0% | 24.8% |
| L3 | 0.322 | 7.92 | 12.5% | 12.3% |
| L4 | 0.249 | 30.3 | 6.25% | 6.25% |
| L5 | 0.077 | 27.5 | 3.12% | 3.35% |
| L6 | 0.027 | 40.5 | 1.56% | 1.52% |
| L7 | 0.013 | 56.0 | 0.78% | 0.82% |
| L8 | 0.004 | 64.0 | 0.39% | 0.34% |
| L9 | 0.002 | 72.0 | 0.20% | 0.16% |

`ψ_super = 1` ensures that a block produced immediately after the
previous level-µ hit (`g_µ = 1`) has **zero** super-level threshold —
the burst-zero condition the paper relies on for Theorem 4.2 (Burst
Resistance).

**Note on symbols.** `ψ_super = 1` here is the **per-super-level dormancy
threshold**, distinct from the **L0 LDD ramp's ψ** (the slot-gap dormancy
in `f(δ)`, set to `ψ_L0 = 0` in the paper's L0 experiments) and from the
**level-spacing geometry** (which is governed by the per-level `(p_µ^max,
σ_µ)` pair, not by any single "level spacing" constant). The proposal
uses `ψ_L0` and `ψ_super` consistently throughout to disambiguate; no
parameter called "ψ" controls **both** L0 dormancy and per-level spacing.

**2. L0 slot-gap gating** based on the L0 LDD slot gap `δ_S` the
snapshot's own production satisfied (paper §3.2 eq. 5):

```
θ_µ^eff(g_µ, δ_S) := θ_µ(g_µ) · min(1, δ_S / γ)
```

with `γ = 15` (our `LddConfig.Default.lddCutoff`). This couples super-level
credit directly to L0 production timing. From paper §3.2: a burst block
at `δ = 1` has its super-level thresholds reduced to **6.7% of the
ungated value**; an honest block at `δ = 7` retains **47%**. Theorem
4.4 quantifies this as a 93% per-level suppression against bursts.

### 2.3 Tower structure

The "superblock tower" of a chain prefix C is the per-level subsequence
of level-µ hits:

```
Tower(C) = {
  Level 0: every snapshot in C
  Level 1: {S ∈ C : τ_1(S) < θ_1^eff(g_1, δ_S)}
  Level 2: {S ∈ C : τ_2(S) < θ_2^eff(g_2, δ_S)}
  ...
  Level L-1: {S ∈ C : τ_{L-1}(S) < θ_{L-1}^eff(g_{L-1}, δ_S)}
}
```

By construction the expected density of level-µ snapshots is `1/2^µ`
(paper §5.2 / Fig 5, validated at 10M slots with achieved-vs-target
error < 13% per level).

**Tower entries are recorded in the snapshot header.** Per paper §3.4,
each snapshot carries a vector of length L:

```
subchainState : Vector[L][(slot, height, tipHash)]    // 48 bytes per level
```

When block B hits level µ: `S_µ(B) = (sl, h_µ^parent + 1, hash(B))`.
Otherwise: `S_µ(B) = S_µ(parent)` — carried forward. For L=10, this is
**480 bytes per snapshot**; at ~2,500 ord/day this is ~1.2 MB/day of
chain-state overhead. The vector lets a verifier walk back along any
single level without materialising the full chain.

### 2.4 Cumulative weight — used **only for NIPoPoW proofs, not chain selection**

This is the load-bearing consequence of §1.1 and is repeated here for
emphasis. The paper defines a cumulative-weight function (§3.3 eq. 6):

```
w(B) = θ_L0(δ_B)^α · (1 + Σ_{µ=1..L-1} 2^µ · 𝟙[B hits level µ])
W(C) = Σ_{B ∈ C} w(B)
```

with `α = 2` recommended. But:

- **For settlement chain selection** we keep `maxvalid-tk` unchanged
  (longest, then lower head slot). This is the existing Taktikos rule,
  the one that survives grinding adversaries (Fig 9, 30% adversary
  stake floor).
- **For the NIPoPoW proof** we use cumulative weight to define
  per-block "difficulty" — `W(C)` is what anchors the probabilistic
  sampling in the light-client protocol (paper §4.3, §6 "Separation").

A grinding adversary against `maxvalid-weighted` (heaviest-chain
selection) exploits dormant branches' weight-debt accumulation; against
`maxvalid-tk` they cannot, because length is unaffected by gap timing.
The light-client verifier's use of `W(C)` to distinguish two competing
proofs is robust because **the underlying chain it summarises was
selected by `maxvalid-tk`** — by the time the tower is built, the
single-chain assumption is in effect.

### 2.5 Common-prefix and (m, k) calibration

The paper's `(m, k)` translation to our Taktikos parameters:

| Paper symbol | Paper role | Taktikos analog | Tessellation value |
|---|---|---|---|
| `k` | tip window — number of L0 headers near the tip included verbatim | k₁ (depth-1 finality) | **255** (`ConfirmationDepthK`) |
| `m` | per-level overlap parameter | n/a (paper's own) | paper-recommended **m = 50** |
| `k₂` | n/a in paper | archival depth — tower-eligible | **65536** (`ArchivalDepthK`) |

**Why `k = k₁ = 255`:** The tip window is the most recent `k` L0
snapshots, included verbatim. Our depth-1 finality guarantee (`T_depth1`)
is "no honest node will reorg past `k₁`". Setting `k > k₁` would
include snapshots a light client cannot validate against canonical chain
state. Per our depth-k sims (`adv_depth_optimization.py`, k ≤ 80 measured
+ exponential-fit extrapolation), per-attempt reorg risk at k=255 under
a 1/3 adversary with no delay is ≈ 3·10⁻¹¹.

**Why `k₂ = 65536` is the tower-eligibility cutoff:** Tower entries
deeper than the tip window must be Phase 3 (ARCHIVAL) — CP-violation
probability < 10⁻¹² at our LDD parameters (see
`SnapshotLeaderLoop.scala:425-430`). Tower entries shallower than `k₂`
risk being on a transient fork.

**Why `m = 50` carries over:** It is the per-level overlap parameter in
the paper's `m + k` proof formulation, calibrated to ensure adversary
chains of equal length but lower weight are distinguishable with
probability > `1 − exp(−Θ(m))`. It is a property of the shifted-exponential
construction, not of our finality cadence (paper §5.7).

### 2.6 Inclusion proofs

A light-client query is parameterised on a snapshot ordinal N (or a
range). The verifier wants to confirm "snapshot N is on the canonical
chain past the trust anchor". Two-step procedure:

1. **Verifier already has** an anchor `(anchorOrd, anchorTowerRoot)`
   from the genesis-anchor bootstrap (§4).
2. **Server returns** a Merkle inclusion proof:
   - The tower entry containing snapshot N (i.e. the highest level µ
     such that N lies between two level-µ tower entries),
   - Plus a chain of level-µ → level-(µ-1) descents tying N back through
     the levels to the anchor.

The "Merkle inclusion proof" reuses the existing MPT primitive: an MPT
keyed `(level, ordinal) → snapshotHash`. The proof shape is exactly an
MPT inclusion proof — the same shape `HistoricalMptProofService` already
serves for state-proof verification.

Per-snapshot `subchainState` (§2.3) makes the descent trivial: from any
level-µ tower entry, the snapshot header itself contains
`subchainState[µ-1].tipHash` pointing at the previous level-(µ-1) entry —
no MPT lookup needed for the descent, only for the anchoring at the top
level.

### 2.7 Genesis anchor

The single load-bearing trust assumption a light client must accept:

```
(genesisHash, genesisVrfPubKeys[], genesisStakeDistribution)
```

This is hard-coded into the light-client binary (or distributed alongside
it, signed by a project key). It is **identical** to the trust assumption
a Tessellation-Nakamoto validator currently makes when starting from
scratch (cf. genesis JSON in `compose-runner.sh`). The asymmetry is that
today a validator catches up via the chain-sync protocol and re-derives
the entire chain. A light client cannot afford the full chain; the
NIPoPoW tower is the compact replacement.

---

## §3 — Interplay with KES forward-security

### 3.1 What KES gives us

KES (Key-Evolving Signatures, Bellare-Miner 1999, deployed in Ouroboros
Praos) guarantees: once a validator's signing key for *time period j*
is destroyed (typically at the boundary of period j+1), no party —
including the original key holder — can produce signatures dated to
period j or earlier.

In Tessellation-Taktikos this means: once eta-rotation period j's KES
keys are evolved past, **no rewrite of period j's snapshots is
cryptographically possible**, even if an adversary later compromises
every validator's *current* key material.

### 3.2 Why this matters for NIPoPoW

NIPoPoW proofs are **retroactive evidence**. A light client receives a
tower proof claiming "level-µ tower entry at ordinal N has hash H" and
checks the VRF eligibility of that entry. The check requires:

- The entry's slot-cert signature is valid under some validator's KES key.
- The validator was a member of the stake distribution at that ordinal.

**Without KES**, an adversary who *later* compromises a validator's
historical key can forge a snapshot dated to period j with arbitrarily
favourable VRF output, win all L domain-separated trials by recomputation,
claim it as a level-µ superblock for any µ, and present a counterfeit
tower. **With KES**, period-j keys are destroyed at the boundary of
period j+1; tower entries past the next-period boundary are permanently
un-forgeable.

### 3.3 Bootstrap ordering — non-negotiable

| Sub-track | Why it must come before NIPoPoW |
|---|---|
| **Stake-weighted VRF** | Each tower entry's eligibility depends on the producer's stake at production time. Today we have placeholder uniform-stake VRF; under uniform stake the level-µ threshold is fixed across validators, but light-client verification needs the *historical* stake distribution. |
| **KES port** (see `project_kes_port_constraints.md`) | Tower entries past the next eta-rotation boundary must be un-forgeable; KES is the only thing that guarantees this. |
| **Historical stake-distribution snapshotting** | Light clients verifying tower entries dated to period j need to know who held what stake at period j. We do not currently checkpoint this. |

The user-stated direction at `project_sharding_strategic_signals` lines
these up: stake-weighted VRF first, then KES, then NIPoPoW. This document
matches that order.

### 3.4 What if we skip KES?

A NIPoPoW prototype without KES would still **work in healthy networks**
— honest validators don't retroactively forge superblocks anyway — but
it would be **broken under adaptive corruption**, which is one of the
explicit threat models NIPoPoW is supposed to handle. The light-client
trust model degrades to "trust that no historical validator key has been
compromised", which is qualitatively no better than checkpointing.

**Strong recommendation**: do not ship NIPoPoW to external light clients
before KES. It is acceptable to **build the tower data structure and
ship the in-protocol observability** before KES, on the model that
internal validators and bridges can use the tower as an early
correctness gate without claiming the full NIPoPoW security model.

---

## §4 — Light-client trust-anchor protocol

### 4.1 Bootstrap

A fresh light client follows this protocol:

1. **Load anchor.** Hard-coded `(genesisHash, genesisVrfPubKeys[],
   genesisStakeDistribution)` shipped with the binary.

2. **Fetch tower.** Query one or more peers for the tower as of their
   current tip:
   ```
   GET /nakamoto/tower
   → {
       tipOrdinal: N,
       tipHash: H_N,
       levels: [
         { level: 9, entries: [(ord, hash, sig), ...] },
         { level: 8, entries: [(ord, hash, sig), ...] },
         ...
         { level: 0, entries: <tip-window of last k₁ snapshots> }
       ],
       proofToGenesis: { mptProofBytes: ... }
     }
   ```

3. **Verify tower self-consistency.** For each level µ from L-1 down to 0:
   - Every entry's VRF must verify against the producer's pubkey-at-the-time.
   - Every entry's `τ_µ` must satisfy `θ_µ^eff` at its claimed `(g_µ, δ_S)`.
   - Adjacent entries' `subchainState[µ]` pointers must form a chain
     (each entry points at the previous).
   - The level-µ entries between two level-(µ+1) entries must be
     consistent with the level-(µ+1) entries' `subchainState[µ+1]`.

4. **Verify density.** Sanity-check that achieved per-level densities
   are within tolerance of `1/2^µ`. Gross density violations indicate
   either an adversarial tower or a buggy server. Tolerance pulled from
   paper Table 1: ≤ 13% per-level relative error at 10M slots.

5. **Verify eta rotation.** Each level-µ entry's VRF input includes an
   eta. The light client traces eta-rotation against the snapshots
   present in lower levels of the tower. Since `eta_{j+1}` is derived
   from VRF outputs in the first 2/3 of period j, the tower's level-0
   tip window plus higher-level entries together must contain enough
   VRF outputs to recompute each eta along the way.

6. **Cache anchor.** Store `(tipOrdinal, tipHash, towerRoot)` for
   subsequent incremental update queries.

### 4.2 Incremental update

On a later query (light client has cached anchor at ord `N_old`, wants
to catch up to ord `N_new`):

```
GET /nakamoto/tower/since/{N_old}
→ {
     newTowerSuffix: { ... },   // tower entries above N_old
     witnessChain:   { ... },   // L0 snapshots from N_old to N_old + k₁ (proof of CP)
   }
```

Verify the witness chain matches the cached tower's tip-window
continuation, then verify the new tower suffix as in §4.1.

### 4.3 Inclusion query

```
GET /nakamoto/inclusion?ordinal={N}
→ {
     snapshotHeader: { ... },           // the queried snapshot
     levelMembership: [µ_0, µ_1, ...],  // levels at which this snapshot hits (may be empty)
     anchorPath: [tower entries from N up to the tower tip]
   }
```

If the queried snapshot is itself a tower entry at some level, the
`anchorPath` is short. If it is a base-level snapshot only, the path
includes the nearest tower entries surrounding it plus the descent.

### 4.4 Failure modes

| Failure | Detection | Recovery |
|---|---|---|
| Peer serves a tower for a divergent chain | Verification at §4.1 step 3-5 fails (VRF or eta mismatch) | Try another peer; alert on >N peers diverging |
| Peer serves an outdated tower | `tipOrdinal` lower than expected; ok if not adversarial | Refresh or accept (light client is permissive) |
| All peers serve same divergent tower (Sybil) | Cannot detect from inside the protocol — same as any P2P bootstrap problem | Mitigated by anchor diversity: light client ships with multiple peer URLs |
| Tower violates density bound | §4.1 step 4 fails | Reject tower as malformed/adversarial |

The "all peers Sybil" failure mode is structural to permissionless P2P
and is **not unique to NIPoPoW** — Mithril has the same problem. The
standard mitigation, anchor diversity, applies to both.

### 4.5 Why not on-chain anchoring

An obvious alternative is to anchor tower roots in a parent chain (e.g.
periodically publish to Ethereum). We do not propose this for v1
because:

- It introduces a parent-chain trust dependency.
- It adds cost.
- It does not improve the security model — the cryptographic VRF + KES
  verification at the light client is already complete.

If we later want a sidechain-style application of NIPoPoW (cf.
Kiayias-Zindros 2020, PoW Sidechains), on-chain anchoring becomes part
of that sidechain's protocol. For light-client v1, peer-served towers
suffice.

---

## §5 — Composition with the 4-phase finality model

The 4-phase model (`attestation-and-finality.md` §0.1) gives us the
right seams for the tower:

| Phase | What snapshots in this phase can do for the tower |
|---|---|
| **0 (PENDING)** | Cannot anchor a tower entry — rivals at the same ordinal expected, no canonical hash yet. |
| **1 (PROVISIONAL)** | Cannot anchor a tower entry — chain-quality contested. |
| **2 (SETTLED)** | Conditionally tower-eligible: snapshot is on the canonical bestTip ancestor, no chain-selection rule will touch it, but a sufficiently-deep CP violation is still > 10⁻¹². **Not anchored at this point.** |
| **3 (ARCHIVAL)** | Tower-anchored. Depth ≥ k₂ = 65536. CP violation < 10⁻¹². Eligible for `subchainState[µ]` pointer publication and tower-tip update. |

### 5.1 The T_depth2 hook

Tower anchoring fires off **exactly** at `T_depth2.evaluateAndAdvance`
(`SnapshotLeaderLoop.scala:460`). Today this trigger:

- Bumps `lastArchivalOrdinalRef`.
- Logs `ARCHIVAL-FINALIZED` at INFO.
- Drives the Prometheus counter / gauge pair.
- Calls `mptOverlay.pruneBelow(archivalQualifying)` (commit `173e6a7d`, #139).

The tower-publication sink plugs in at the same advance point. The doc
in `attestation-and-finality.md` §5.4 already lists this as **reserved**:

> *Reserved Phase-3 sinks (aggregate-signature certificate, light-client
> trust anchor) plug in at the same advance point without touching the
> trigger.*

That reservation is what this proposal proposes to fill.

### 5.2 Pre-Phase-3 tower production

The L independent τ_µ trials and slot-cert publication of `subchainState`
are *production-time* concerns — they happen when the snapshot is forged,
regardless of phase. Headers carry tower-bits from day one of any version
that ships this. The tower is *anchored* (in the sense that a light
client may rely on it) only once entries are Phase 3.

This separation is important for backwards compatibility: a snapshot
emitted by a NIPoPoW-aware producer is consumable by a non-aware
verifier (`subchainState` is just bytes), and a snapshot emitted by a
non-aware producer is interpretable as having `µ = 0` everywhere (no
super-level claims). A clean velvet-fork-compatible deployment, per
Kiayias-Karantias-Zindros 2021 ("Velvet path").

### 5.3 Chain-selection rule is unchanged

This is the critical engineering constraint enforced by Fig 9 (§1.1).
**Settlement chain selection stays on `maxvalid-tk`** — longest, then
lower head slot. The cumulative weight `W(C)` from §2.4 is **never**
consulted by `SnapshotLeaderLoop.bestTipFn` or any code path that
selects the chain the cluster builds on. Cumulative weight is computed
only by `TowerProofBuilder` when serving a NIPoPoW proof to an external
light client.

Operationally, this means: no edits to `ChainSelection`, `bestTipFn`,
or `maxvalid-tk` are part of this proposal. The proposal's surface
strictly *adds* tower production + tower serving; it does not modify
existing chain-selection logic.

### 5.4 Chain-quality observable composition

Our chain-quality observable (commit `866cd598`, `dag_nakamoto_chain_quality`
gauge ∈ {1, 2, 3}) gives operational evidence of the number of finality
triggers satisfied per ordinal. The tower's level-µ density gives
cryptographic evidence of how heavy each ordinal's accumulated weight
is. They are complementary, with no overlap:

- The gauge tells operators "this chain is suspiciously single-trigger" in real time.
- The tower tells external light clients "this chain is cumulatively heavy" post-archival.

### 5.5 Velvet fork path

Per Kiayias-Karantias-Zindros (2021, AFT '21), a NIPoPoW deployment can
be backwards-compatible — older nodes ignore the `subchainState` field,
newer nodes consume it. The security analysis there shows graceful
degradation: NIPoPoW security degrades with the fraction of *participating*
(interlinking) producers. For us the interlinking cost per snapshot is
~480 bytes header overhead plus L-1 Blake2b512 evaluations (negligible),
so the assumption is that **all nodes upgrade**, and the velvet model is
only relevant for the rollout transition (a few weeks).

---

## §6 — Implementation outline (sizing, NOT a commitment)

Estimates assume KES is already in place (§3.3) and that historical
stake-distribution snapshotting is also done.

### 6.1 Phase A — Tower production at the producer (~800 LOC)

| Component | Approx LOC | Where |
|---|---|---|
| `LevelTrial` (the L domain-separated τ_µ derivations from one ρ) | ~120 | `modules/node-shared/.../nakamoto/nipopow/LevelTrial.scala` |
| `LevelThreshold` (shifted exp + slot-gap gating) | ~150 | `modules/node-shared/.../nakamoto/nipopow/LevelThreshold.scala` |
| `SubchainState` data + codecs | ~120 | `modules/shared/.../schema/nakamoto/nipopow/SubchainState.scala` |
| `SubchainStateUpdater` (called per-snapshot, updates the vector from parent + the L trial outcomes) | ~150 | new |
| `SuperLevelParams` (the L0–L9 parameter table from paper Table 1) | ~60 | new |
| Slot-cert serialization changes (carry SubchainState in header) | ~100 | edits to slot-cert schema + scodec impls |
| Tests (L independent trials; per-level density at scale; parameter validation) | ~100 | `modules/node-shared/.../nakamoto/nipopow/LevelTrialSpec.scala` + `SubchainStateSpec.scala` |

### 6.2 Phase B — Tower store + finalize sink (~600 LOC)

| Component | Approx LOC | Where |
|---|---|---|
| `TowerStore[F]` trait + `MptTowerStore[F]` impl | ~250 | `modules/node-shared/.../nakamoto/nipopow/TowerStore.scala` |
| Tower MPT keyed `(level, ordinal) → snapshotHash` | ~80 | reuses `MptStore` primitive |
| `TowerFinalizer` (Phase-3 sink — appends entry on `T_depth2.advance`) | ~120 | wired into `SnapshotLeaderLoop.finalityMonitor` |
| `MptOverlay.pruneBelow` extension for tower entries | ~50 | open: do we prune deepest levels or retain forever? |
| Tests | ~100 | `TowerFinalizerSpec.scala` |

### 6.3 Phase C — Tower proof builder + verifier (~700 LOC)

| Component | Approx LOC | Where |
|---|---|---|
| `TowerProofBuilder[F]` (serve `GET /nakamoto/tower`) | ~250 | `modules/dag-l0/.../http/routes/TowerRoutes.scala` |
| `TowerVerifier` (pure `F[_]`; checks all L trials, gating, density) | ~250 | `modules/node-shared/.../nakamoto/nipopow/TowerVerifier.scala` |
| Density-violation detector | ~50 | helper |
| Eta-rotation reconstruction inside verifier | ~80 | reuses `EtaCalculation` |
| Tests | ~70 | round-trip + adversarial-tower tests |

### 6.4 Phase D — HTTP routes + metrics (~300 LOC)

| Component | Approx LOC | Where |
|---|---|---|
| `GET /nakamoto/tower` | ~80 | route |
| `GET /nakamoto/tower/since/{N}` | ~80 | route |
| `GET /nakamoto/inclusion?ordinal={N}` | ~100 | route |
| Prometheus: `dag_nakamoto_tower_entries_total{level}`, `dag_nakamoto_tower_density_relative_error{level}` | ~40 | |

### 6.5 Phase E (optional v2) — Light-client SDK (~1,500 LOC, separate module)

A standalone Java/Scala/TS module that consumes the HTTP routes and
performs verification client-side. Not v1.

### 6.6 Totals

- **v1 (in-node, phases A–D):** ~2,400 LOC. Estimated 6–10 engineer-weeks
  with one experienced contributor.
- **v2 (SDK, phase E):** +1,500 LOC + SDK distribution work (probably
  multi-language, +4–6 weeks).

These are **estimates with significant uncertainty** — the security-
sensitive code is small (verifier is ~250 LOC), but parameter-tuning
and density-validation at production scale is the hard part and is
empirical.

---

## §7 — Timing recommendation

### 7.1 The three options

**Option A: Implement now, before KES + stake-weighted VRF.**

- Faster time-to-feature.
- Internal observability gain (per-level densities provide cluster-wide
  chain-quality evidence beyond the gauge).
- Risk: built on placeholder uniform-stake VRF; KES not present →
  broken under adaptive corruption; would have to be re-validated after
  KES lands and possibly re-tuned.

**Option B: Research now, implement after KES.**

- Design parameters depend on stake-weighted VRF + KES; better to fix
  design while those primitives are fresh.
- No code commitment now; this doc is the deliverable.
- No product blocker — we have no external light-client demand today.
- Implementation lands cleanly post-KES with the security model intact.

**Option C: Defer indefinitely; do Mithril when light clients demand it.**

- Avoid sunk research cost.
- Risk: when demand arrives, Mithril is multi-month and adds a heavy
  dependency (BLS pairings + committee protocol).
- Risk: design parameters drift as the protocol evolves; harder to
  revisit cold.

### 7.2 Recommendation: Option B

**Land this design doc now.** Defer code work until KES and
stake-weighted VRF are in place. Concretely:

1. Commit this design doc (today).
2. Reserve `subchainState` bytes in the slot-cert header schema —
   *velvet-fork-friendly*: add the bytes now, ignore on read, so we can
   populate them later without a protocol break.
3. After KES + stake-weighted VRF land (post-sharding P0.1 per
   `project_sharding_strategic_signals`), revisit this doc and either
   ship Phases A–D or defer further depending on demand at that time.
4. Open questions (§8 below) on tower compression, historical stake
   distribution checkpointing, and level count L remain parked here
   for the implementation revisit.

The expected value of having a design lock now is high; the expected
value of having code now is low (no consumer of the tower in production
today).

---

## §8 — Open questions for the user

1. **Tower compression.** The paper publishes one tower entry per
   qualifying snapshot — straightforward append-only structure. An
   alternative is to publish only "burst-maximal" entries (the highest
   level reached per chain window). The paper is **append-only** in
   §3.4 — `S_µ(B)` is just carried forward from parent when B doesn't
   hit µ, and updated to `(sl, h_µ^parent+1, hash(B))` when B does hit.
   Compression is not a paper concept; it would be a Tessellation-specific
   extension.

   **Default if no input: append-only**, matching the paper. Decision
   affects ~30 LOC in `TowerFinalizer`.

2. **Historical stake distribution checkpointing.** Tower entries are
   produced under the *current* stake distribution at the time of
   forging. A light client verifying a tower entry from year N needs
   to know who held what stake at year N. We have no mechanism to
   snapshot this today.

   Options:
   - **(a)** Snapshot at every `k₂` boundary (heavy: 65536-ordinal frequency × full distribution).
   - **(b)** Snapshot at coarser intervals (e.g. eta-rotation period boundaries — R = 2550 currently).
   - **(c)** Defer to a sidecar / aggregator service.

   This is a larger design decision than NIPoPoW per se — it is the
   same question Mithril would have to answer for its lottery. Worth
   pulling forward into the stake-weighted-VRF design.

3. **Tower level count L.** The paper uses L = 10 (L0–L9). For a
   10M-slot validation that's plenty; for our ~2,500 ord/day cadence
   and an indefinite-lifetime chain, levels above L9 might be useful
   5+ years out. Header overhead scales linearly: L=10 → 480 bytes;
   L=12 → 576 bytes; L=15 → 720 bytes.

   **Default if no input: L=10**, matching the paper's validated table.

4. **Genesis-anchor signing / distribution.** Hard-coding the genesis
   anchor into the binary is the simplest option. Are we comfortable
   with that ops model, or do we want a signed-by-project-key external
   anchor file?

5. **Velvet-fork rollout vs hard-fork.** The `subchainState` header
   reservation lets us velvet-fork in NIPoPoW. Hard-forking is also fine
   since we have no production chain yet. The velvet path is mostly
   useful if Tessellation goes live before NIPoPoW lands.

6. **Should the per-level `(p_µ^max, σ_µ)` parameter table from the
   paper be re-tuned for our specific LDD parameters?** The paper's
   Table 1 is validated under `ψ_super = 1` (per-super-level dormancy,
   constant across L1..L9) and uses paper-experiment L0 LDD parameters
   `(ψ_L0 = 0, γ = 15, f_A = 0.5, f_B = 0.05)`. Tessellation production
   uses the same `(γ, f_A, f_B)` values. **`f_B = 0.05` is the
   production setting and stays there for this exercise**
   (`project_sharding_strategic_signals`; raising `f_B` is a separate
   conversation that this proposal does NOT take a position on). At
   small differences in `ψ_L0`, our sims suggest the per-level table
   holds, but the validation should be re-run on our cadence before
   final tuning. Half-day sim run, not a redesign.

---

## §9 — References

### Primary

- **`~/repos/research-nipopos-2026/`** — the user's in-progress paper, simulation code, and reproducible figures. **The authoritative reference for this proposal.** Cited throughout above.
- `~/repos/research-nipopos-2026/paper/main.tex` — full LaTeX source.
- `~/repos/research-nipopos-2026/paper/figures/weight_grinding_intuition.pdf` — Fig 9 (`fig:grinding_comparison`), the headline empirical falsification.
- `~/repos/research-nipopos-2026/sims/RESULTS.md` — per-level density validation at 10M slots.
- `~/repos/research-nipopos-2026/sims/run_grinding_ci.py` + `sims/weight_grinding_intuition.py` — Fig 9 generators.

### Local

- `docs/nakamoto/attestation-and-finality.md` — 4-phase model + finality triggers. §5.4 reserves the Phase-3 light-client sink anticipated here.
- `docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md` — sibling proposal; same style.
- `docs/nakamoto/SYNC-PROTOCOL.md` — full-validator chain-sync; complementary.
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala` — `vrfOutputAsRatio` is the input function for §2.1.
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/FinalityTrigger.scala` — `T_depth2` is the §5.1 hook.
- `~/.claude/skills/taktikos/SKILL.md` — Taktikos protocol primitives and adversarial sim results.
- `~/.claude/projects/.../memory/project_kes_port_constraints.md` — KES port constraints (§3.3).

### Foundational

- Kiayias, Miller, Zindros. *Non-Interactive Proofs of Proof-of-Work*. FC 2020 / eprint 2017/963. (The classical PoW NIPoPoW we are **not** porting directly.)
- Kiayias, Lamprou, Stouka. *Proofs of Proofs of Work with Sublinear Complexity*. FC 2016.
- Bünz, Kiffer, Luu, Zamani. *FlyClient: Super-Light Clients for Cryptocurrencies*. IEEE S&P 2020.
- Karantias, Kiayias, Zindros. *The Velvet Path to Superlight Blockchain Clients*. AFT 2021.
- Kiayias, Zindros. *Proof-of-Work Sidechains*. FC 2020.
- Schutza, Behrens, Duong, Aman. *Ouroboros Taktikos: Regularizing Proof-of-Stake via Dynamic Difficulty*. FC 2023. (The LDD paper.)
- David, Gaži, Kiayias, Russell. *Ouroboros Praos*. EUROCRYPT 2018. (VRF lottery + KES + epoch nonce stability.)
- Bellare, Miner. *A Forward-Secure Digital Signature Scheme*. CRYPTO 1999. (KES.)
- Chaidos, Kiayias. *Mithril: Stake-Based Threshold Multisignatures*. ASIACRYPT 2024. (The contrast point.)

---

*Living document. Implementation-side decisions parked at §8 until KES
+ stake-weighted VRF have landed and we revisit. Today's commit is
research-only.*
