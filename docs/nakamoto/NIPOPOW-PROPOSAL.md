# NIPoPoW for Tessellation-Taktikos — design proposal

**Status:** research proposal. No code commitment in this document — sizing only.
**Date:** 2026-05-15.
**Scope:** the light-client / finality-cert workstream, as an alternative to porting Mithril.

## Provenance

This proposal is the implementation-side translation of the research paper

> *Superblock Proofs for Proof-of-Stake: How Local Dynamic Difficulty Enables NIPoPoW-Style Light Clients*

in the companion repository [`~/repos/research-nipopos-2026`](../../../research-nipopos-2026)
(branch `nipopos-2026`). The paper, its 11 reproducible figures, and the
simulation results under `sims/` are the **authoritative ground truth**
for the construction. This document is the engineering deliverable that
takes the paper and answers "what does it look like to put this into
Tessellation-Nakamoto, and when?".

Key paper artifacts referenced repeatedly here:

| Artifact | Role |
|---|---|
| `paper/main.tex` §3 (Construction) | Multi-level VRF eligibility, slot-gap gating, cumulative weight |
| `paper/main.tex` §4 (Security Analysis) | Adversary dilemma, burst resistance, separation theorem |
| `paper/main.tex` §5.5 + Fig 9 | Headline empirical result: ramp weighting *hurts* grinding resistance |
| `paper/main.tex` §5.6 + Fig 10/11 | Non-portability theorem: NIPoPoW degrades to 69% under Praos flat |
| `paper/main.tex` §5.7 | Light-client overhead (≈ 5,100 headers for 1.43M-block chain, ~280× reduction) |
| `docs/PROJECT-NOTES.md` | Separation theorem onboarding |
| `docs/TAKTIKOS-NOTES.md` §"SOLVED: Shifted Exponential Construction" | The L1–L9 parameter tuning |
| `sims/RESULTS.md` | Validated parameter table at 10M slots |
| `sims/challengerModel_ramp.py` + `sims/run_grinding_ci.py` | Fig 9 grinding-comparison ground truth |

The construction in §2 below mirrors the paper's. Everything outside §2 — the
KES interplay, the trust-anchor protocol, the composition with our 4-phase
finality model, and the implementation/timing — is engineering material new
to this document.

---

## §1 — Why NIPoPoW over Mithril

### 1.1 Engineering surface

| | Mithril | NIPoPoW |
|---|---|---|
| Crypto primitives required | BLS12-381 (pairings, aggregation) + concentrated-stake lottery | Merkle trees (already have) + VRF outputs (already produce) |
| New consensus participants | Mithril signer subset + aggregator | None — every snapshot producer is implicitly a NIPoPoW participant by virtue of forging |
| Coordination | Threshold multi-sig requires liveness from a committee of `≥ k` signers per epoch | None — proof is built post-hoc from existing chain data |
| Proof type | Constant-size certificate per checkpoint | Logarithmic-size superblock tower |
| New code modules (rough order of magnitude) | BLS12-381 pairing library, signer protocol, aggregator service, on-chain registry, cert-distribution gossip — multi-month port |[1] | Tower data structure + builder + verifier — see §6 |

[1] Mithril references and signature-aggregation alternatives are catalogued
in `~/repos/research-nipopos-2026/paper/REFERENCES-RESEARCH.md` §2.

### 1.2 Trust model

Equivalent. Both schemes assume:

- The chain has a non-overwhelming adversary fraction (≤ 1/3 for our LDD parameters; sim-validated).
- The light client either ships with a hard-coded genesis anchor, or accepts that bootstrap from arbitrary peers is a P2P problem.
- KES forward-security is in effect for retroactive-rewrite resistance.

Neither scheme protects against a long-range adversary with an unrevoked
historical signing key — that defense is **KES**, and is shared. See §3.

### 1.3 Proof size comparison

For a chain of N base snapshots:

- Mithril: O(1) per checkpoint (constant). For Cardano-equivalent finality cadence (~5d, ~432,000 slots between certs at 1s/slot), a year of certs is ~73 certs.
- NIPoPoW (this construction): O(m·log N) headers per proof. From `paper/main.tex` §5.7: a 1.43M-block chain with `(m=50, k=100)` yields a 5,100-header proof — ~280× reduction over the full chain. For our currently-targeted production chain (~2,500 ord/day after Taktikos drift settles, k₁=255, k₂=65536), the ratio is similar in shape but the absolute number is smaller because the chain is shorter.

**Mithril wins on raw proof size at long lookbacks.** NIPoPoW wins on
proof flexibility: each tower entry is an arbitrary-ordinal anchor, so
light clients can query "is tx T in snapshot N?" for any N in the tower,
not only at certificate boundaries.

### 1.4 What we already have that makes NIPoPoW free

Three primitives that Mithril doesn't reuse:

1. **VRF outputs at every snapshot.** `EligibilityChecker.vrfOutputAsRatio`
   already produces `Ratio ∈ [0, 1)` from each snapshot's VRF output via
   `BigInt(1, vrfOutput) / 2^512`. This is exactly the input distribution
   that level-µ thresholding consumes.
2. **A Merkle Patricia Trie (MPT) substrate** for snapshot-indexed state.
   The same MPT primitive we built for state-proof verification (`MptOverlay`,
   `MptStore`, the historical-proof service) is the natural store for the
   per-level subchain pointers.
3. **The 4-phase finality model.** Phase 3 (ARCHIVAL) is explicitly
   reserved as the eligible-for-tower-anchoring phase — wired against
   `T_depth2` (commit `06455f98`, default k₂ = 65536). See `attestation-and-finality.md` §5.4.

NIPoPoW is "almost free" relative to Mithril precisely because these
three primitives are already in production. Mithril would require
~all-new BLS infrastructure.

---

## §2 — NIPoPoW construction for Taktikos

This section restates the paper construction, using our terminology and
linking each piece back to existing protocol artifacts.

### 2.1 Superblock level definition

For each snapshot S, let `ρ_S` be its 64-byte VRF output (produced by
`EligibilityChecker.checkEligibility` and embedded in the snapshot's
slot-cert). The **paper's construction** uses domain-separated
re-hashing rather than the leading-zero count:

```
For each level µ ∈ {1, …, L-1}:
  τ_µ(S) := Blake2b512(ρ_S ‖ ("TEST-" ++ µ)) / 2^512     // Ratio ∈ [0,1)
  S is a level-µ superblock iff   τ_µ(S) < θ_µ^eff(g_µ, δ_S)
```

Two thresholds compose, both already characterised in the paper:

1. **Shifted-exponential threshold over the base-block gap g_µ** (snapshots since the previous level-µ hit):
   ```
   θ_µ(g_µ) = p_µ^max · (1 - exp(-(g_µ - ψ) / σ_µ))   if g_µ ≥ ψ
            = 0                                         if g_µ < ψ
   ```
   with `ψ = 1` (dormant period — burst resistance) and the
   `(p_µ^max, σ_µ)` table from `paper/main.tex` Tab 1 (validated at
   10M slots, achieved-vs-target rates within 1–13%).

2. **Slot-gap gating** based on the L0 LDD slot gap δ_S that the snapshot's own production satisfied:
   ```
   θ_µ^eff(g_µ, δ_S) := θ_µ(g_µ) · min(1, δ_S / γ)
   ```
   with `γ = 15` (our `LddConfig.Default.lddCutoff`).

The choice of these two thresholds (not leading-zero count) is the paper's
**central technical contribution**. The "leading-zero µ" formulation
common in PoW NIPoPoW literature would give the correct *densities* but
**zero security advantage** over plain chain length — see `paper/main.tex`
§3.1, "Why not flat conditional probabilities?". The shifted-exponential
+ gating combination is what makes superblock weight an actual cost
function for an adversary.

### 2.2 Tower structure

The "superblock tower" of a chain prefix C is the per-level subsequence:

```
Tower(C) = {
  Level 0: every snapshot in C
  Level 1: {S ∈ C : S is a level-1 superblock}
  Level 2: {S ∈ C : S is a level-2 superblock}
  ...
  Level L-1: {S ∈ C : S is a level-(L-1) superblock}
}
```

By construction (paper §3 + sim §5.2), the expected density of level-µ
snapshots in the chain is `1/2^µ`, validated at 10M slots with
achieved-vs-target error < 13% per level.

**Tower entries are recorded in the snapshot header.** Per
`paper/main.tex` §3.4, each snapshot carries:

```
subchainState : Vector[L][(slot, height, tipHash)]
```

— 48 bytes per level. For L=10 (matching the paper's L0–L9 sweep), this
is 480 bytes per snapshot. At ~2,500 ord/day, this is ~1.2 MB/day of
chain-state overhead.

The vector lets a verifier walk back along any single level without
materialising the full chain: each level-µ tip points at the previous
level-µ hit, recursively.

### 2.3 Common-prefix and m, k calibration

The paper derives its `(m, k)` from Praos-style CP bounds. **Translation
to our Taktikos parameters**:

| Paper symbol | Paper role | Taktikos analog | Tessellation value |
|---|---|---|---|
| `k` | tip window — number of L0 headers near the tip included verbatim | k₁ (depth-1 finality) | **255** (`ConfirmationDepthK`) |
| `m` | security parameter — per-level overlap requirement | n/a (paper's own) | paper-recommended **m = 50** |
| `k₂` | n/a in paper | archival depth — tower-eligible | **65536** (`ArchivalDepthK`) |

**Why `k = k₁ = 255`:** The tip window in the proof is the most recent
`k` L0 snapshots, included verbatim. Our depth-1 finality guarantee
(`T_depth1`) is precisely "no honest node will reorg past `k₁`". The
tip window is therefore at most `k₁` deep before it would conflict with
finalized state — which is the right cutoff. Setting k > k₁ would
include snapshots a light client could not validate against canonical
chain state.

**Why `k₂ = 65536` is the tower-eligibility cutoff:** Tower entries
deeper than the tip window must be *archival* — Phase 3 in our model
(see §5). At k₂ = 65536, common-prefix violation probability under our
LDD + 1/3 adversary is well below 10⁻¹² (Cardano-equivalent — see
`SnapshotLeaderLoop.scala:425-430`). Tower entries shallower than k₂
risk being on a transient fork at the time the tower is built.

**Why `m = 50` carries over:** This is the per-level overlap parameter
in the paper's `m + k` proof formulation, defined to ensure the highest
common level between two competing proofs contains enough blocks to
identify the heavier (canonical) chain. It is a property of the
shifted-exponential construction, not of our finality cadence. The
paper validates this empirically against 500-race fork ensembles
(Fig 7); the bound is calibrated to ensure adversary chains of equal
length but lower weight are distinguishable with probability >
1 - exp(-Θ(m)).

**Sanity check against our sim:** our Taktikos depth-k sims
(`adv_depth_optimization.py`, k≤80 measured + extrapolated to k=255)
give per-attempt reorg risk ≈ 3·10⁻¹¹ at k=255 under 1/3 adversary,
no delay. The paper's CP-violation bound is comparable in shape. We
inherit the bound without re-deriving it; the falsification experiment
is paper §5.6 (Fig 10/11) which shows the construction **does not work
under Praos flat** — i.e. the LDD gradient is load-bearing. Our cluster
is on that gradient.

### 2.4 Inclusion proofs

A light-client query is parameterised on a snapshot ordinal N (or a
range). The verifier wants to confirm "snapshot N is on the canonical
chain past the trust anchor". Two-step procedure:

1. **Verifier already has** an anchor `(anchorOrd, anchorTowerRoot)`
   from the genesis-anchor bootstrap (§4).
2. **Server returns** a Merkle inclusion proof:
   - The tower entry containing snapshot N (i.e. the highest level µ
     such that N lies between two level-µ tower entries),
   - Plus a chain of level-µ→level-(µ-1) descents tying N back through
     the levels to the anchor.

The "Merkle inclusion proof" reuses the existing MPT primitive: an MPT
keyed `(level, ordinal) → snapshotHash`. The proof is exactly an MPT
inclusion proof — same shape as the historical-proof service already
serves for state-proof verification (`HistoricalMptProofService`).

Per-snapshot subchainState (§2.2) makes the descent step trivial: from
any level-µ tower entry, the snapshot header itself contains
`subchainState[µ-1].tipHash` pointing at the previous level-(µ-1)
entry — no MPT lookup needed for the descent, only for the
anchoring at the top level.

### 2.5 Genesis anchor

The single load-bearing trust assumption a light client must accept:

```
(genesisHash, genesisVrfPubKeys[], genesisStakeDistribution)
```

This is hard-coded into the light-client binary (or distributed
alongside it, signed by a project key). It is **identical** to the
trust assumption a Tessellation-Nakamoto validator currently makes
when starting from scratch (cf. genesis JSON in `compose-runner.sh`).

The asymmetry is that today, a validator catches up via the chain-sync
protocol and re-derives the entire chain. A light client cannot
afford the full chain; the NIPoPoW tower is the compact replacement.

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
tower proof claiming "level-µ tower entry at ordinal N has hash H"
and checks the VRF eligibility of that entry. The check requires:
- The entry's slot-cert signature is valid under some validator's KES key.
- The validator was a member of the stake distribution at that ordinal.

**Without KES**, an adversary who *later* compromises a validator's
historical key can forge a snapshot dated to period j with arbitrarily
favourable VRF output, claim it as a level-µ superblock, and present a
counterfeit tower.

**With KES**, the period-j key was destroyed at the boundary of period
j+1. Once a tower entry is past the next-period boundary, it is
**permanently un-forgeable** by any party.

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
before KES.

It is acceptable, however, to **build the tower data structure and
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
   - Every entry's τ_µ must satisfy θ_µ^eff at its claimed `(g_µ, δ_S)`.
   - Adjacent entries' `subchainState[µ]` pointers must form a chain (each entry points at the previous).
   - The level-µ entries between two level-(µ+1) entries must be consistent with the level-(µ+1) entries' `subchainState[µ+1]`.

4. **Verify density.** Sanity-check that achieved per-level densities
   are within tolerance of `1/2^µ`. Gross density violations indicate
   either an adversarial tower or a buggy server. Tolerance pulled from
   `paper/main.tex` Tab 1: ≤ 13% per-level relative error at 10M slots.

5. **Verify eta rotation.** Each level-µ entry's VRF input includes
   an eta. The light client traces eta-rotation against the snapshots
   present in lower levels of the tower. Since eta_{j+1} is derived
   from VRF outputs in the first 2/3 of period j, the tower's level-0
   tip window plus higher-level entries together must contain enough
   VRF outputs to recompute each eta along the way.

6. **Cache anchor.** Store `(tipOrdinal, tipHash, towerRoot)` for
   subsequent incremental update queries.

### 4.2 Incremental update

On a later query (light client has cached anchor at ord N_old, wants to
catch up to ord N_new):

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
     levelMembership: [µ_0, µ_1, ...],  // levels at which this snapshot is a superblock (may be empty)
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
and is **not unique to NIPoPoW**. Mithril has the same problem (an
attacker controlling all peers can serve forged certs). The standard
mitigation — anchor diversity — applies to both.

### 4.5 Why not on-chain anchoring

An obvious alternative is to anchor tower roots in a parent chain
(e.g. periodically publish to Ethereum). We do not propose this for v1
because:

- It introduces a parent-chain trust dependency.
- It adds cost.
- It does not improve the security model — the cryptographic VRF + KES
  verification at the light client is already complete.

If we later want a sidechain-style application of NIPoPoW (cf. Kiayias-
Zindros 2020, PoW Sidechains), on-chain anchoring becomes part of that
sidechain's protocol. For light-client v1, peer-served towers suffice.

---

## §5 — Composition with the 4-phase finality model

The 4-phase model (`attestation-and-finality.md` §0.1) gives us the right
seams for the tower:

| Phase | What snapshots in this phase can do for the tower |
|---|---|
| **0 (PENDING)** | Cannot anchor a tower entry — rivals at the same ordinal expected, no canonical hash yet. |
| **1 (PROVISIONAL)** | Cannot anchor a tower entry — chain-quality contested. |
| **2 (SETTLED)** | Conditionally tower-eligible: snapshot is on the canonical bestTip ancestor, no chain-selection rule will touch it, but a sufficiently-deep Common-Prefix violation is still > 10⁻¹². **Not anchored at this point.** |
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

The shifted-exponential τ_µ computation and slot-cert publication of
`subchainState` are *production-time* concerns — they happen when the
snapshot is forged, regardless of phase. Headers carry tower-bits from
day one of any version that ships this. The tower is *anchored* (in the
sense that a light client may rely on it) only once entries are Phase 3.

This separation is important for backwards compatibility: a snapshot
emitted by a NIPoPoW-aware producer is consumable by a non-aware
verifier (the `subchainState` field is just bytes), and a snapshot
emitted by a non-aware producer is interpretable as having `µ = 0`
everywhere (no superblock claims). A clean velvet-fork-compatible
deployment, per Kiayias-Karantias-Zindros 2021 ("Velvet path").

### 5.3 Chain-quality observable composition

Our chain-quality observable (commit `866cd598`, `dag_nakamoto_chain_quality`
gauge ∈ {1, 2, 3}) gives operational evidence of the *number of
finality triggers* satisfied per ordinal. The tower's level-µ density
gives *cryptographic evidence* of how heavy each ordinal's accumulated
weight is. They're complementary:

- The gauge tells operators "this chain is suspiciously single-trigger" in real time.
- The tower tells external light clients "this chain is cumulatively heavy" post-archival.

The seams don't overlap and neither blocks the other.

### 5.4 Velvet fork path

Per Kiayias-Karantias-Zindros (2021, AFT '21, "The velvet path"), a
NIPoPoW deployment can be backwards-compatible — older nodes ignore
the `subchainState` field, newer nodes consume it. The security
analysis there shows graceful degradation: NIPoPoW security degrades
with the fraction of *participating* (interlinking) miners. For us
the "interlinking" cost is zero per snapshot (one extra hash + ~480
bytes header overhead), so the assumption is that **all nodes
upgrade**, and the velvet model is only relevant for the rollout
transition (a few weeks).

---

## §6 — Implementation outline (sizing, NOT a commitment)

The estimates below are best-effort, drawn from comparable modules in
the codebase. They assume KES is already in place (see §3.3) and that
historical stake-distribution snapshotting is also done.

### 6.1 Phase A — Tower production at the producer (~800 LOC)

| Component | Approx LOC | Where |
|---|---|---|
| `LevelThreshold` (shifted exp + slot-gap gating) | ~150 | `modules/node-shared/.../nakamoto/nipopow/LevelThreshold.scala` |
| Domain-separated VRF τ_µ derivation | ~80 | reuses `EligibilityChecker.vrfOutputAsRatio` |
| `SubchainState` data + codecs | ~120 | `modules/shared/.../schema/nakamoto/nipopow/SubchainState.scala` |
| `SubchainStateUpdater` (called per-snapshot, updates the vector from parent) | ~150 | new |
| `SuperLevelParams` (the L0–L9 parameter table from paper Tab 1) | ~60 | new |
| Slot-cert serialization changes (carry SubchainState in header) | ~120 | edits to slot-cert schema + scodec impls |
| Tests (per-level density at scale; parameter validation) | ~120 | `modules/node-shared/.../nakamoto/nipopow/SubchainStateSpec.scala` |

### 6.2 Phase B — Tower store + finalize sink (~600 LOC)

| Component | Approx LOC | Where |
|---|---|---|
| `TowerStore[F]` trait + `MptTowerStore[F]` impl | ~250 | `modules/node-shared/.../nakamoto/nipopow/TowerStore.scala` |
| Tower MPT keyed `(level, ordinal) → snapshotHash` | ~80 | reuses `MptStore` primitive |
| `TowerFinalizer` (the Phase-3 sink — appends entry on `T_depth2.advance`) | ~120 | wired into `SnapshotLeaderLoop.finalityMonitor` |
| `MptOverlay.pruneBelow` extension for tower entries (?) | ~50 | open: do we prune deepest levels or retain forever? |
| Tests | ~100 | `TowerFinalizerSpec.scala` |

### 6.3 Phase C — Tower proof builder + verifier (~700 LOC)

| Component | Approx LOC | Where |
|---|---|---|
| `TowerProofBuilder[F]` (serve `GET /nakamoto/tower`) | ~250 | `modules/dag-l0/.../http/routes/TowerRoutes.scala` |
| `TowerVerifier` (pure F[_]) | ~250 | `modules/node-shared/.../nakamoto/nipopow/TowerVerifier.scala` |
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
  multi-language, +4-6 weeks).

These are **estimates with significant uncertainty** — the security-
sensitive code is small (verifier is ~250 LOC), but parameter-tuning
and density-validation at production scale is the hard part and is
empirical.

---

## §7 — Timing recommendation

### 7.1 The three options

**Option A: Implement now, before KES + stake-weighted VRF.**

- Faster time-to-feature.
- Internal observability gain (tower densities provide cluster-wide chain-quality evidence beyond the gauge).
- Risk: built on placeholder uniform-stake VRF; KES not present → broken under adaptive corruption; would have to be re-validated after KES lands and possibly re-tuned.

**Option B: Research now, implement after KES.**

- Design parameters depend on stake-weighted VRF + KES; better to fix design while those primitives are fresh in someone's head.
- No code commitment now; this doc is the deliverable.
- No product blocker — we have no external light-client demand today.
- Implementation lands cleanly post-KES with the security model intact.

**Option C: Defer indefinitely; do Mithril when light clients demand it.**

- Avoid sunk research cost.
- Risk: when demand arrives, Mithril is multi-month and adds a heavy dependency (BLS pairings + committee protocol).
- Risk: design parameters drift as the protocol evolves; harder to revisit cold.

### 7.2 Recommendation: Option B

**Land this design doc now.** Defer code work until KES and
stake-weighted VRF are in place. Concretely:

1. ✅ Commit this design doc (today).
2. Reserve `subchainState` bytes in the slot-cert header schema — *velvet-fork-friendly*: add the bytes now, ignore on read, so we can populate them later without a protocol break.
3. After KES + stake-weighted VRF land (post-sharding P0.1 per `project_sharding_strategic_signals`), revisit this doc and either ship Phases A-D or defer further depending on demand at that time.
4. Open question (§8 below) on whether to compress the tower or anchor every Phase-3 snapshot remains parked here for the implementation revisit.

The expected value of having a **design lock** now is high; the
expected value of having **code** now is low (no consumer of the
tower in production today).

---

## §8 — Open questions for the user

The following items would benefit from explicit direction before the
implementation revisit:

1. **Tower compression**

   The paper publishes one tower entry per qualifying snapshot —
   straightforward append-only structure. An alternative is to publish
   only "burst-maximal" entries (the highest level reached per chain
   window). The first is more verbose (~5,100 headers per proof at
   1.43M chain); the second is smaller but introduces an extra rule
   ("highest level in window") that complicates verification.

   **Default if no input: append-only**, matching the paper. Decision
   affects ~30 LOC in `TowerFinalizer`.

2. **Historical stake distribution checkpointing**

   Tower entries are produced under the *current* stake distribution at
   the time of forging. A light client verifying a tower entry from
   year N needs to know who held what stake at year N. Today we have
   no mechanism to snapshot the historical stake distribution at
   tower-eligibility boundaries.

   Options:
   - **(a)** Snapshot the stake distribution at every k₂ boundary (heavy: 65536-ordinal frequency × full distribution).
   - **(b)** Snapshot at coarser intervals (e.g. eta-rotation period boundaries — R = 2550 currently).
   - **(c)** Defer to a sidecar / aggregator service.

   This is a **larger design decision** than NIPoPoW per se — it's the
   same question Mithril would have to answer for its lottery. Worth
   pulling forward into the stake-weighted-VRF design.

3. **Tower level count L**

   The paper uses L = 10 (L0–L9). For a 10M-slot validation that's
   plenty; for our ~2,500 ord/day cadence and an indefinite-lifetime
   chain, levels above L9 might be useful 5+ years out. Header overhead
   scales linearly: L=10 → 480 bytes; L=12 → 576 bytes; L=15 → 720 bytes.

   **Default if no input: L=10**, matching the paper's validated table.

4. **Genesis-anchor signing / distribution**

   Hard-coding the genesis anchor into the binary is the simplest
   option. Are we comfortable with that ops model, or do we want a
   signed-by-project-key external anchor file?

5. **Velvet-fork rollout vs hard-fork**

   The `subchainState` header reservation lets us velvet-fork in
   NIPoPoW. Hard-forking is also fine since we have no production
   chain yet (per `project_sharding_strategic_signals`). The velvet
   path is mostly useful if Tessellation goes live before NIPoPoW
   lands.

6. **Should the per-level (p_µ^max, σ_µ) parameter table from the paper
   be re-tuned for our specific LDD parameters?**

   The paper uses `ψ=0, γ=15, fA=0.5, fB=0.05`. We use `ψ=1, γ=15,
   fA=0.5, fB=0.05` — `ψ=1` differs. The shift is a *base-chain* ψ
   versus the *per-level* ψ in the paper's table (which is `ψ=1` for
   L1–L9). At small ψ differences, our sims suggest the per-level
   table holds, but the validation should be re-run on our cadence
   before final tuning. This is a half-day sim run, not a redesign.

---

## §9 — References

### Primary

- **`~/repos/research-nipopos-2026/`** (branch `nipopos-2026`) — the in-progress paper, simulation code, and reproducible figures. **The authoritative reference for this proposal.** Cited throughout above.
- `~/repos/research-nipopos-2026/paper/main.tex` — full LaTeX source.
- `~/repos/research-nipopos-2026/sims/RESULTS.md` — per-level density validation at 10M slots.
- `~/repos/research-nipopos-2026/paper/REFERENCES-RESEARCH.md` — full bibliography (Kiayias-Miller-Zindros 2017, Bünz et al. 2020, Chase-Karayannidis 2021, Karantias-Kiayias-Zindros 2021).

### Local

- `docs/nakamoto/attestation-and-finality.md` — 4-phase model + finality triggers. §5.4 reserves the Phase-3 light-client sink anticipated here.
- `docs/nakamoto/SYNC-PROTOCOL.md` — full-validator chain-sync; complementary to this proposal.
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala` — `vrfOutputAsRatio` is the input function for §2.1.
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/FinalityTrigger.scala` — `T_depth2` is the §5.1 hook.
- `~/.claude/skills/taktikos/SKILL.md` — Taktikos protocol primitives and adversarial sim results.
- `~/.claude/projects/.../memory/project_kes_port_constraints.md` — KES port constraints (§3.3).

### Foundational

- Kiayias, Miller, Zindros. *Non-Interactive Proofs of Proof-of-Work*. FC 2020 / eprint 2017/963.
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
