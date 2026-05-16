# Cross-shard stake-concentration mitigation — proposal

**Status:** research proposal. Decision input. No code commitment in this
document. Written 2026-05-15 against `feature/serde-typeclass-shim`
HEAD `c6528548`.
**Scope:** how to defend the cross-shard composition (gl0 aggregating
per-shard sub-snapshots via state-channel binaries) against the
stake-concentration attack derived in
[`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) §5.2.

This document formalises three candidate defences, compares them on the
load-bearing criteria, and recommends one to prototype first. Code
locations the recommended option would touch are sketched in §6; the
detailed implementation work is downstream.

Cross-references:
- [`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) (commit `45c97895`) —
  per-shard / cross-shard GKL trinity; §5.2 derives the
  `α_total > 1/(2S)` collapse boundary that motivates this proposal.
- [`docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md)
  (commit `be1c8250`) — Snowball cascade `(K=3, α_cascade=2, β=10)` and
  the 4-trigger composition. Option B in this document explicitly
  composes with that cascade.
- [`docs/nakamoto/NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md)
  (commit `bcb42140`) — per-shard NIPoPoW tower; cross-shard tower is
  out of scope here (no cross-shard tower in v1).
- [`docs/nakamoto/attestation-and-finality.md`](./attestation-and-finality.md)
  — 4-phase model + `T_count` / `T_weight` / `T_depth1` / `T_depth2`
  trigger stack. The recommendation reuses `T_count` semantics at the
  per-shard sub-snapshot boundary.
- [`modules/node-shared/.../statechannel/StateChannelValidator.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala)
  — the SC binary admission code path. §1.2 below dissects what it
  currently checks; §6 sketches the touch-points for the recommended
  option.
- [`modules/node-shared/.../managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala)
  — the gl0-side acceptance manager that buckets validated SC binaries
  into the next global snapshot.
- [`modules/shared/.../statechannel/StateChannelSnapshotBinary.scala`](../../modules/shared/src/main/scala/io/constellationnetwork/statechannel/StateChannelSnapshotBinary.scala)
  — the on-the-wire SC binary shape (`lastSnapshotHash`, `content`,
  `fee`). Option B grows this with an attestation-set field.
- [`modules/node-shared/.../nakamoto/StakeRegistry.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/StakeRegistry.scala)
  — global validator registry; used by Option A for per-epoch VRF
  sortition.
- `:project_sharding_strategic_signals` (memory file) — directive on
  combined `delegated stake + node collateral` as the stake-weight
  source. Both options A and B inherit this.
- `~/repos/research-nipopos-2026/sims/cross_shard.py` (branch
  `sim/integration`, commit `913c4a8`) + figures /
  `sims/data/cross_shard_t_count_fixed_30s_concentrated_in_shard_0_S4_slots1000.json` —
  empirical anchor for the §1 problem statement.

---

## §0 TL;DR

**Recommendation: Option B (per-shard attestation-set on SC binary
acceptance), tuned to `τ = ⅔` of the shard's active validator quorum,
composed with the existing Snowball cascade.** Option A
(VRF-sortition into shards) is too disruptive given Tessellation's
per-shard operator topology; Option C (slashing/governance) is
necessary as a deterrent regardless but cannot defend the cross-shard
CQ bound by itself. Option B reuses primitives the integrated stack
already has — `T_count`, `StakeRegistry`, the Snowball convergence —
and makes the structural defence cheap relative to the GKL bound it
needs to hold.

The one-paragraph rationale: under the GKL §5.2 derivation, an
adversary with global stake `α_total > 1/(2S)` can concentrate into a
single shard and exceed `α_local > 1/3` in that shard. Option B caps
the adversary's *effective influence over SC binary acceptance* at the
shard's quorum requirement: if `τ = ⅔`, an adversary needs `α_local
> 1/3` of the *shard's signing quorum* — not its block production
chance — to forge an SC binary gl0 will accept. The cross-shard CQ
bound becomes the union of per-shard quorum bounds, not the union of
per-shard production bounds; the two differ by exactly the per-shard
Snowball CP guarantee already proven in
[`AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md)
§§6.1 / 0.A.

---

## §1 Problem statement

### 1.1 The GKL §5.2 bound

[`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) §5.2 derives the
cross-shard CQ collapse threshold. For `S` equal shards with
`r_s_home = r/S` and a stake-concentrating adversary:

```
α_local = α_total / (1/S + α_total)
       = (α_total · S) / (1 + α_total · S)
```

This crosses `1/3` when `α_total > 1/(2S)`, i.e.

```
α_total > 1 / (2S)
```

Worked examples (cross-shard CQ collapse threshold, reproduced from
the GKL doc):

| S    | α_total threshold for `α_local > 1/3` | α_local at α_total = 1/3 |
|------|----------------------------------------|---------------------------|
| 3    | 1/6 ≈ 0.167                            | 1/2 (50% local)           |
| 5    | 1/10 = 0.10                            | 5/8 ≈ 0.625               |
| 10   | 1/20 = 0.05                            | 10/13 ≈ 0.769             |

**Naïve sharding makes the protocol strictly more adversary-sensitive
than single-chain.** At S=10 the single-chain 1/3 bound is replaced by
a 5% bound — the cross-shard sim's `cross_shard.py
--adversary-stake-concentration=concentrated_in_shard_0 --n-shards=4
--f-adv-global=0.33` configuration explicitly reproduces this:
shard 0's `per_shard_alpha_local = 1.0` (every block in shard 0 marked
dishonest), reducing its `per_shard_accept_rate` to **0.66 %** (1/152
binaries) under a `t_count` finality gate
(`sims/data/cross_shard_t_count_fixed_30s_concentrated_in_shard_0_S4_slots1000.json`).
Without a defence, the adversary owns shard 0's state entirely; gl0
accepts whatever it produces, because gl0 currently does not gate on
shard-internal attestation success.

### 1.2 The SC binary admission code-level gap

Inspecting the production admission path:

[`modules/node-shared/.../statechannel/StateChannelValidator.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala)
`validateAllowedSignatures` (lines `164-167`) chains three checks:

```
validateSignaturesWithSeedlist   -- at least one signature from l0Seedlist
  -> validateStateChannelAddress -- address present in allowance lists
  -> validateStateChannelAllowanceList -- ≥1 proof from per-address peer set
```

The signature gate is **`validateAtLeastOneSignatureInSeedlist`** —
literally one cryptographically valid signature from the L0 seedlist
suffices. There is no per-shard quorum check. The
[`StateChannelOutput`](../../modules/shared/src/main/scala/io/constellationnetwork/statechannel/StateChannelOutput.scala)
carries one
[`Signed[StateChannelSnapshotBinary]`](../../modules/shared/src/main/scala/io/constellationnetwork/statechannel/StateChannelSnapshotBinary.scala);
the binary itself is the shard's serialised snapshot bytes
(`content: Array[Byte]`) plus `lastSnapshotHash` and `fee`. The
producing metagraph signs this with its L0 operator key, gl0 verifies
the signature against the seedlist, and the binary is admitted into
`GlobalSnapshotStateChannelAcceptanceManager.accept` — which keys on
`(address, parent.lastSnapshotHash)` for first-sight registration but
does **no further attestation aggregation**.

The implicit "finality gate" today is gl0's own snapshot finality:
once gl0 accepts an SC binary into its global snapshot ordinal `N`,
the cross-shard view of that shard is canonical after gl0's own
`T_count` / `T_weight` / `T_depth1` fire at ordinal `N`. But the
*content* of the SC binary — what state the metagraph claims at this
snapshot — is admitted on **single-signature authority of the
metagraph's L0 operator**. An adversary controlling that operator key
(or running the metagraph's operator entirely) can submit any
cryptographically valid SC binary they like; gl0 cannot tell the
difference. Under the §1.1 concentration attack, the adversary's
target shard becomes precisely such an adversary-controlled metagraph,
and gl0 has no per-shard defence.

This is the load-bearing gap the cross-shard sim agent surfaced. The
gate must move from "one operator signature" to *some* per-shard
quorum check; §§2-4 enumerate the candidate designs.

### 1.3 Required properties for a defence

| Property            | Why it matters |
|---------------------|----------------|
| **Structural cap**  | Adversary's ability to forge SC binaries should be bounded by `α_local`, not by operator-key control. |
| **Bounded latency** | Defence must not strand SC binary acceptance behind a slow consensus round. Production target: comparable to today's "single signature" path. |
| **Composes with `T_count`/`T_weight`** | A separate machinery for SC binaries doubles the surface area and risks divergence between gl0 finality and per-shard finality. |
| **Tolerates partition** | Per-shard partition (small validator set, lost peers) must not block SC binary production indefinitely — `T_depth1` style fallback should still apply. |
| **No new crypto unless necessary** | KES is the only new primitive on the roadmap (`:project_kes_port_constraints`). BLS / threshold sigs are a bigger lift; avoid if a multi-sig set suffices. |

---

## §2 Option A — VRF-sortition of validators into shards

### 2.1 Design

Each epoch (cadence `E` snapshots, candidate values discussed below),
the protocol uses VRF — seeded by the global eta-period randomness
that already drives slot-leader election in
[`SnapshotLeaderLoop.scala`](../../modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala) —
to assign each registered validator to one or more shards. The per-
shard validator set at epoch `e` is therefore an approximately uniform
draw from the global pool:

```
shardAssignment(v, e) := VRF(sk_v, ηₑ ‖ "SHARD-ASSIGN") mod S
```

(Optionally multi-shard: assign to `m` shards by repeated rehashing.)

Each shard's block producer for a given slot is then the standard LDD
elected validator *restricted to that shard's assigned set*; SC
binaries the shard submits to gl0 are signed by the shard's currently
elected operator (i.e. by a validator the protocol — not the operator —
chose to seat in this shard this epoch).

The structural defence: an adversary with `α_total` stake spread
across all validators cannot self-select into a single shard. The
adversary's expected per-shard local fraction is `α_total · m/S` for
multi-shard assignment, or `α_total` for single-shard with `m=1` —
i.e. the global fraction, restoring the single-chain 1/3 bound at
each shard.

### 2.2 Pros

- **Structurally impossible to concentrate.** The defence is built
  into shard assignment, not into the runtime acceptance path.
- **No per-SC-binary extra cost.** Once assigned, an SC binary still
  carries one operator's signature. SC binary size is unchanged.
- **No new crypto.** Reuses the VRF that already drives slot-leader
  election plus the eta randomness that already rotates per
  `:project_taktikos_protocol`.

### 2.3 Cons

- **Operator infrastructure churn.** A validator suddenly assigned to
  shard `s` must spin up shard-`s` infrastructure — sync the shard's
  full state, register with the shard's L0 cluster, expose the shard's
  RPC, etc. The "two-tier stake" model from
  `:project_sharding_strategic_signals` (delegated stake + node
  collateral) further complicates the assignment unit: do we
  sortition **operators** (who run physical nodes) or **validators**
  (who post collateral)? An operator running multiple validators
  cannot trivially split its node into per-shard partitions.
- **Re-bootstrap cost per shard reassignment.** Each epoch boundary
  triggers a state-transfer wave proportional to `|assigned out| +
  |assigned in|`. Under our k₁ = 255 depth-1 finality, bootstrap is
  the well-known bottleneck (`:project_117_path_b_fork_recovery_deadlock`
  is the in-anger version). Doing this for every shard every epoch is
  a non-trivial operational hit.
- **Operator-key economic model breaks.** Today an operator picks
  which metagraphs to operate; they accept the operational cost of
  that metagraph. Under VRF-sortition, the protocol assigns work to
  operators who may not have agreed to operate that shard. The
  delegated-stake model becomes harder to reason about: a delegator
  staking to operator `O` does not know which shards `O` will be
  assigned to next epoch.
- **Grinding-resistance under stake-weighted assignment.** A naïve
  sortition that lets validators select via `VRF(sk_v, η)` lets an
  adversary grind on `sk_v` choice if any operator controls multiple
  registered keys. Mitigation requires KES forward-security and a
  registration window — additional protocol surface.

### 2.4 Key parameters

| Parameter | Recommendation | Tradeoff |
|---|---|---|
| **`E`** — epoch length in snapshots between reassignments | Tie to existing eta-rotation `R = 2550` (≈ 1 day at 7s snapshots) | Short `E` → less adversary-targeting opportunity, more churn. Long `E` → reduced churn, more time for adversary stake re-accumulation. |
| **`m`** — shards per validator | `m = 1` for minimal infra cost; `m ≥ 2` for redundancy against single-shard liveness gaps | `m > 1` softens churn but does not change the structural bound. |
| **Sortition unit** | **Open** — operator vs validator vs (operator, validator) tuple | The cleanest binding for the two-tier stake model is per-validator; the cleanest operational fit is per-operator. |

### 2.5 Verdict

Option A solves the §1.1 problem at the cost of a fundamental change
to Tessellation's operator topology. The "operators choose which
metagraphs to run" model is load-bearing for the current
metagraph-as-business-unit framing (`:reference_pacaswap_multimetagraph`).
Disrupting it is feasible but a multi-quarter workstream of its own,
and it interacts with KES rollout, the two-tier stake model, and the
metagraph-genesis flow in ways not yet enumerated.

**Status: candidate, but not recommended as first prototype.**

---

## §3 Option B — Per-shard attestation-set on SC binary admission

### 3.1 Design

Replace the single-operator signature on
[`StateChannelSnapshotBinary`](../../modules/shared/src/main/scala/io/constellationnetwork/statechannel/StateChannelSnapshotBinary.scala)
with a **multi-signature attestation set** from the shard's active
validator quorum. The wire-level change is bounded: extend the
binary's `proofs` field — currently a single
[`SignatureProof`](../../modules/shared/src/main/scala/io/constellationnetwork/security/signature/signature/SignatureProof.scala)
on the operator key — to a non-empty set of `SignatureProof`s from
the shard's validators. gl0 verifies that the set's effective
attestation weight clears a quorum threshold `τ` against the shard's
`StakeRegistry`.

Concretely:

```
Binary structure (B):
  lastSnapshotHash : Hash
  content          : Array[Byte]
  fee              : SnapshotFee

Wire envelope (S):
  Signed[B] with proofs : NonEmptySet[SignatureProof]

gl0 acceptance gate:
  isQuorate(S, τ) :=
    let signers = S.proofs.map(_.id.toPeerId)
    let validators = stakeRegistry.shardValidators(B.shard)
    let attesterStake = Σᵥ∈(signers ∩ validators) stakeRegistry.relativeStake(v)
    attesterStake ≥ τ * (Σᵥ∈validators stakeRegistry.relativeStake(v))
```

### 3.2 Composition with the Snowball cascade

The per-shard SC binary attestation set is **exactly the convergent
output of the shard's existing Snowball cascade**. Each per-shard
validator runs the cascade described in
[`AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md) §2
on the shard's sub-snapshot ordinals; on decision, the validator
emits a signed attestation. The shard's L0 operator collects these
attestations into the SC binary's `proofs` set and submits to gl0
once the set clears `τ`.

This is the §0.C "T_count subsumed by β" decision applied at the
sub-snapshot boundary. The Snowball cascade's empirical `(K=3,
α_cascade=2, β=10)` operating point gives `ε_snowball ≤ 3.7 × 10⁻⁴`
at the 10k-trial 95% Wilson upper bound for `f_adv ≤ 0.33` (per
[`AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md)
§0.A and the full GPU sweep at
`sims/data/avalanche_attestation_full_gpu_n10000.json`, commit
`d8f4639`). At τ = ⅔, the SC binary admission gate is:

```
ε_acceptance_per_shard_per_ordinal ≤ ε_snowball  +  ε_quorum(τ, α_local)
```

where `ε_quorum` is the probability the adversary can muster a
`τ`-fraction signing quorum despite holding only `α_local`. For
`τ = ⅔` and `α_local < 1/3`, `ε_quorum` is structurally zero (the
adversary cannot reach 2/3 with 1/3 stake). For `α_local > 1/3` —
the §1.1 concentration regime — `ε_quorum` becomes the dominant
term and the gate fails *correctly*: the adversary's SC binary is
rejected by gl0, not accepted.

The cross-shard CQ bound becomes:

```
ε_cross_CQ_per_ordinal ≤ Σₛ [ ε_snowball,s + ε_quorum,s(τ, α_local,s) ]
```

For `α_total ≤ 1/3` distributed uniformly (`α_local ≈ α_total`),
both terms are ε-small per shard. For `α_total ≤ 1/(2S)` even under
worst-case concentration, `α_local ≤ 1/3` and the same bound holds.
**The bound that GKL §5.2 said would collapse at `α_total > 1/(2S)`
is replaced by a quorum-gated bound that holds up to `α_total ≤ 1/3`
under any distribution.** This is the structural defence we need.

### 3.3 Quorum threshold τ

Recommendation: **τ = ⅔**, matching the
[`T_count`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/FinalityTrigger.scala)
trigger semantics from
[`attestation-and-finality.md`](./attestation-and-finality.md) §0.2.
SC binary admission becomes equivalent to "the per-shard
sub-snapshot is `T_count`-finalised at the shard's quorum".

This pulls the cross-shard composition into Regime A of
[`GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) §5.3 — the regime where
gl0 inherits per-shard CP via the aggregator wait — but at the
sub-snapshot boundary rather than the per-shard `T_depth1` boundary.
The latency cost is **~one Snowball convergence round per
sub-snapshot** (median 11-13 × 250 ms ≈ 3 s under the locked tick),
not the `k₁ × E[gap] ≈ 33 min` of the depth-fallback path.

### 3.4 Pros

- **No infrastructure churn.** Validators stay on their preferred
  shards; no re-bootstrap on epoch boundary. The operator topology is
  preserved.
- **Composes cleanly with existing primitives.** Reuses Snowball
  convergence + `T_count` semantics + `StakeRegistry` — no new
  crypto, no new consensus participants.
- **Detection is proactive.** Concentration becomes visible as
  rejection: gl0 sees a binary that *cannot* reach quorum and
  rejects, exposing the shard's compromised state to operators
  (slashing evidence — see §4) and to monitoring.
- **Partition behaviour matches existing model.** If the shard
  partitions and cannot reach quorum, the SC binary stalls — exactly
  the same liveness profile as `T_count` stalling at the per-shard
  level. `T_depth1` at the shard layer still applies as a structural
  fallback; the SC binary can still be admitted on `T_depth1` grounds
  via a per-shard structural-depth proof (open: see §7.2).

### 3.5 Cons

- **SC binary size grows.** The current single `SignatureProof`
  becomes a set of ~N/3 to N/2 proofs depending on quorum size.
  Under N=8 active validators per shard, `τ = ⅔` → 6 signatures
  → 6 × ~96 bytes ≈ 576 bytes of proofs. Acceptable; well within
  the current `maxBinarySizeInBytes` limit checked in
  [`StateChannelValidator.validateSnapshotSize`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala) (line 136).
- **gl0 verification cost.** Verifying 6 signatures per binary
  instead of 1. Still cheap relative to the snapshot-acceptance
  hot-path cost; we already verify on the order of dozens of
  signatures per global snapshot acceptance.
- **Stake-registry coupling.** gl0 must know the **shard's** active
  validator set, not just the global seedlist. Today,
  [`StakeRegistry`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/StakeRegistry.scala)
  is a global registry; adding a `shardValidators(shardId)` view is
  the required extension. Mechanically straightforward; the
  per-metagraph state already tracks
  [`activeDelegatedStakes` / `activeNodeCollaterals`](https://github.com/Constellation-Labs/tessellation/blob/main/modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala)
  per `:project_sharding_strategic_signals`.
- **Aggregator coordination cost on the shard.** The shard's L0
  operator becomes responsible for collecting the attestation set
  before submitting to gl0. This adds a per-sub-snapshot waiting
  step on the shard side — the same coordination Snowball already
  does for gl0's own finality, applied one level down.

### 3.6 Key parameters

| Parameter | Recommendation | Notes |
|---|---|---|
| **`τ`** — quorum threshold (fraction of shard active stake) | **`⅔`** | Matches `T_count` semantics; matches GKL §5.3 Regime A's binding bound. |
| **Sub-snapshot Snowball `(K, α_cascade, β)`** | Inherit from gl0 cascade: `(3, 2, 10)` | Re-sim per-shard if shard validator count differs materially from gl0 cluster size. |
| **Aggregator timeout** | Tie to `5 × slotDurationMs` finality-monitor tick | If quorum is not reached within timeout, the shard's L0 operator may submit a "no-decision" marker or fall back to a single-operator binary marked as `T_depth1`-track only. |
| **Shard validator-set epoch boundary** | Cohere with eta-rotation `R = 2550` snapshots | The shard's validator set is the active stake-weighted set at the snapshot the sub-snapshot is built from. |

---

## §4 Option C — Slashing / governance / out-of-band detection

### 4.1 Design

Leave SC binary admission as-is (single operator signature is enough)
and add a **post-hoc** divergence-detection layer:

1. Monitor for **state-channel divergence** — two SC binaries from
   the same metagraph claiming inconsistent state at the same
   per-shard ordinal, or a shard's monotonic state growth
   inconsistent with VRF-expected production rates.
2. Construct **on-chain slashing evidence**: a pair of equivocating
   signatures from the same operator key, with KES forward-secure
   metadata proving the operator could not have rotated keys to
   avoid liability.
3. **Slash** the offending operator's `nodeCollateral`. Optionally
   freeze the metagraph pending governance review.

This is the design implicit in the
[`KES port from Bifrost`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/seedlist/SeedlistEntry.scala) /
`:project_kes_port_constraints` workstream — KES forward-security is
the *required precondition* for any slashing path to function (a
slashed operator must not be able to repudiate evidence by claiming
the signing key was rotated between forge and detection).

### 4.2 Pros

- **Minimal protocol change.** No SC binary shape change, no shard
  reassignment, no new attestation aggregation.
- **Economic disincentive.** The operator's collateral is a
  meaningful stake — `:project_sharding_strategic_signals` is
  explicit that node collateral is one half of the stake-weight
  source. A slashed operator's metagraph loses its production
  weight.

### 4.3 Cons (why insufficient alone)

- **Reactive, not proactive.** The detection-to-slash window — even
  in the most aggressive design — is at minimum one cross-shard
  round-trip (~33 min at `k₁ × E[gap]` under `T_depth1`, or ~3 s
  under Snowball). During that window, the byzantine SC binary
  affects downstream users. For a metagraph holding currency
  balances, this means real funds can be moved on the strength of
  a divergent state claim before slashing fires.
- **Slashing-proof construction is open.** "Divergent state
  channel" is not formally defined today. Constructing succinct,
  on-chain-verifiable evidence of divergence is its own design
  workstream — likely requires per-shard NIPoPoW headers
  (`NIPOPOW-PROPOSAL.md` §2.1) to anchor the divergence claim, or
  fraud-proof style succinct evidence that a claimed sub-snapshot is
  inconsistent with the shard's prior state. This is comparable in
  complexity to Option B's full implementation.
- **KES is a hard prerequisite.** Without forward-secure signatures,
  a slashed operator can claim "that key was compromised after the
  forge", and the evidence is repudiable. KES port is multi-month;
  this places slashing well after Option B's implementation window.
- **Doesn't defend the cross-shard CQ bound directly.** It changes
  the adversary's *payoff* (slashed collateral) but not their
  *ability* to admit a byzantine binary. The GKL §5.2 bound is
  unchanged.

### 4.4 Status

**Necessary as a deterrent layer, regardless of which structural
defence (A or B) is chosen.** An adversary willing to lose collateral
can still exploit a structural-defence gap if there is one;
conversely, a structural defence with no economic backing risks a
permissionless adversary using throwaway keys. The two are
complementary, not alternative.

**Status: recommended as a follow-on workstream, sequenced after
Option B prototype and after KES port.**

---

## §5 Recommendation + rationale

### 5.1 Choice

**Prototype Option B first.** Sequence:

1. **B0** — extend `StateChannelSnapshotBinary` envelope to carry a
   multi-proof set + a shard-id tag. No verification change yet;
   binary still admits on single-signature semantics. (~2 weeks)
2. **B1** — extend
   [`StakeRegistry`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/StakeRegistry.scala)
   with `shardValidators(shardId): F[Set[PeerId]]`, populated from
   per-metagraph `activeDelegatedStakes` / `activeNodeCollaterals`.
   (~2 weeks)
3. **B2** — extend `StateChannelValidator.validateAllowedSignatures`
   to gate on `isQuorate(τ = ⅔)` against the shard validator set.
   Gate behind a config flag for staged rollout. (~3 weeks)
4. **B3** — wire the shard's L0 operator to collect Snowball-decided
   attestations and submit once `τ` is cleared. Reuse the existing
   `TipTracker` aggregation primitive. (~3 weeks)
5. **B4** — flip the config flag on a metagraph-by-metagraph basis;
   measure per-binary verification cost, SC binary size, end-to-end
   latency. (~2 weeks)

After B, sequence the KES port (`:project_kes_port_constraints`) and
Option C slashing on top.

### 5.2 Rationale

- **Composes with the integrated stack.** Option B reuses Snowball
  convergence, `T_count` quorum semantics, the `StakeRegistry`
  abstraction, and the `Signed[_]` multi-proof primitive. Nothing
  novel at the cryptographic layer.
- **Defends the GKL §5.2 bound.** The new bound is `α_total ≤ 1/3`
  under any distribution — restoring the single-chain bound for the
  cross-shard composition. This is the load-bearing safety property
  the GKL doc identified as missing.
- **No operator-topology disruption.** Operators continue to choose
  which metagraphs to operate; delegated-stake math is unchanged;
  the metagraph-as-business-unit framing is preserved.
- **Detection comes for free.** A shard that *cannot* reach quorum
  is visible at gl0 as a rejection event — slashing evidence for
  Option C is the natural output, not an additional construction.
- **Bounded engineering surface.** The total touch-set is ~5 files
  in `node-shared` + ~2 in `shared` + the per-shard aggregator wire
  on `dag-l0`. Smaller than Option A (which rewrites the
  metagraph-registration flow) and smaller than Option C (which
  requires KES + fraud-proof construction).

### 5.3 What we are explicitly **not** committing to

- An implementation. This is a design proposal; B0..B4 are sizing
  estimates, not a project plan.
- The exact aggregation primitive. Multi-`SignatureProof` set is the
  default; if shard validator counts grow beyond ~30 we may want to
  upgrade to BLS aggregate, but only with a separate proposal.
- The "per-shard `T_depth1` fallback" detail (§7.2 — open).
- The cross-shard NIPoPoW story. Out of scope; per-shard NIPoPoW
  remains as in [`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md).

---

## §6 Implementation skeleton (Option B)

This section sketches the code-level surface for Option B as a
reviewer aid. None of this is committed in this document.

### 6.1 Wire-level changes

[`modules/shared/src/main/scala/io/constellationnetwork/statechannel/StateChannelSnapshotBinary.scala`](../../modules/shared/src/main/scala/io/constellationnetwork/statechannel/StateChannelSnapshotBinary.scala)

```scala
// today
case class StateChannelSnapshotBinary(
  lastSnapshotHash: Hash,
  content: Array[Byte],
  fee: SnapshotFee
)
```

The wire shape is unchanged at the binary level; the multi-proof
support lives in the existing `Signed[_]` envelope (`proofs:
NonEmptySet[SignatureProof]`). The codec at
[`modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/StateChannelSnapshotBinaryCodec.scala`](../../modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/StateChannelSnapshotBinaryCodec.scala)
does not need to change.

What needs adding is a **shard-id tag** so gl0 can look up the
correct shard validator set. Today the shard is implicit in the
binary's `address` (the metagraph's address), so the lookup key is
`address → shardId → validatorSet`. The `address → shardId` map
already exists implicitly via per-metagraph
`stateChannelAllowanceLists`.

### 6.2 Validator-side change

[`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala)

Today `validateAllowedSignatures` (lines `164-167`) chains:

```scala
validateSignaturesWithSeedlist(stateChannelOutput.snapshotBinary)
  .andThen(_ => validateStateChannelAddress(stateChannelOutput.address))
  .andThen(_ => validateStateChannelAllowanceList(stateChannelOutput.address, stateChannelOutput.snapshotBinary))
```

Add a fourth step:

```scala
  .andThen(_ => validateShardQuorum(stateChannelOutput, stakeRegistry, τ))
```

where `validateShardQuorum` computes the attester-stake sum against
the shard's active validator set, gated by `τ = ⅔` of the shard's
total stake.

The constructor at `StateChannelValidator.make` (line 84) gains a
`stakeRegistry: StakeRegistry[F]` parameter; wired through
[`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/modules/SharedValidators.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/modules/SharedValidators.scala)
at construction.

### 6.3 Stake-registry extension

[`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/StakeRegistry.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/StakeRegistry.scala)

Add a shard-scoped view:

```scala
trait StakeRegistry[F[_]] {
  // existing global view
  def relativeStake(peerId: PeerId): F[Ratio]
  def allStakes: F[Map[PeerId, Ratio]]
  def validatorCount: F[Int]

  // new shard-scoped view
  def shardValidators(shard: Address): F[Set[PeerId]]
  def shardRelativeStake(shard: Address, peerId: PeerId): F[Ratio]
  def shardTotalStake(shard: Address): F[Ratio]
}
```

The `Address` here is the metagraph's address — i.e. the existing
shard identifier in the SC binary world.

### 6.4 Aggregator-side change (shard's L0 operator)

The shard's
[`BinaryPoster`](../../modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/services/BinaryPoster.scala)
moves from "post on operator signature" to "post when attestation set
clears τ":

1. The shard's `SnapshotLeaderLoop`-equivalent runs the Snowball
   cascade on each new sub-snapshot ordinal.
2. On decision, each validator broadcasts a signed attestation
   (existing `TipAttestation` gossip path).
3. The L0 operator collects attestations into a quorum set; when
   `Σ attesterStake ≥ τ × shardTotalStake`, the operator assembles
   the multi-proof `Signed[StateChannelSnapshotBinary]` and posts to
   gl0.

The `TipTracker.recordAttestation` primitive already handles
per-ordinal newer-wins aggregation; the quorum-extraction step is the
new logic.

### 6.5 Acceptance-manager change (gl0)

[`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala)

No structural change — the manager continues to first-sight-register
binaries keyed on `(address, parent.lastSnapshotHash)`. The
quorum check happens upstream in `StateChannelService.process` (line
`62`, `stateChannelValidator.validate`).

### 6.6 Config knobs

Following the
[`AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md) §2.5
pattern (numerator/denominator integer pairs for grinding-resistance):

```
NAKAMOTO_SHARD_QUORUM_NUM   = 2
NAKAMOTO_SHARD_QUORUM_DEN   = 3
NAKAMOTO_SHARD_QUORUM_GATE  = on | off  // staged rollout flag
NAKAMOTO_SHARD_QUORUM_TIMEOUT_TICKS = 5  // multiples of finality-monitor tick
```

Defaults sourced from a `ShardQuorumConfig.Default` matching the
`LddConfig.Default` pattern.

---

## §7 Open questions

These are flagged for the implementation workstream and the
Tier-2 sharding plan. This document does not attempt to close them.

### 7.1 Sortition unit (Option A residual)

Even if Option B is the recommended structural defence, **shard
membership** must be defined for `StakeRegistry.shardValidators`. Two
candidates:

- **Static, operator-elected.** Operators declare per-metagraph
  participation at registration; the shard validator set is the
  declared participants. Simple; matches today's "operators choose
  metagraphs" model. Concentration is possible if operators
  collude.
- **VRF-sortitioned per epoch.** Option A applied at the shard
  validator-set boundary only — operators run nodes for all
  metagraphs but only validate a VRF-assigned subset per epoch.
  Splits the difference between A and B.

A hybrid is plausible: operators declare *eligibility*, the protocol
samples *active set* via VRF, the active set runs Snowball + posts
the quorum. **Open.**

### 7.2 `T_depth1` fallback at the per-shard sub-snapshot boundary

If the shard partitions and cannot reach `τ` for an extended period,
the shard's state is stuck. The current `T_depth1` at gl0 handles
this *for gl0 itself*; for the per-shard sub-snapshot, an analogous
structural-depth fallback is needed. Options:

- **Operator-only fallback after timeout.** After `T` ticks without
  quorum, allow the operator to post a single-signature SC binary
  marked `T_depth1-only`; gl0 admits but does **not** count it
  toward cross-shard CQ. The shard state advances; downstream users
  treat it as "unconfirmed" until the partition heals.
- **Structural-depth proof.** The operator posts an SC binary plus
  a per-shard NIPoPoW header proving the sub-snapshot is depth-`k₁`
  deep in the shard's chain; gl0 admits on that proof in lieu of
  quorum. Cleaner but requires per-shard NIPoPoW first.

**Open. Recommended path: operator-only fallback for v1; structural-
depth proof later.**

### 7.3 Quorum threshold sensitivity

`τ = ⅔` matches `T_count`. But `T_count` thresholds are tuned to gl0
cluster sizes (currently ~3-8 active validators per
`:project_iter37_8node_validated`). At larger shard counts (~30+
validators per shard) the optimal `τ` may differ; we may want
`τ = 1/2 + ε` for liveness or `τ = 3/4` for stricter safety.

**Open. Needs cross-shard sim re-run at variable τ to characterise
the latency-vs-safety frontier.**

### 7.4 Aggregator-side timing model

Shard's L0 operator becomes the attestation-aggregator. If the
operator is byzantine (concentration scenario), it can withhold
honest attestations from the quorum set. The shard is then live but
producing only adversary-quorum binaries — gl0 rejects them, the
shard stalls.

This is the correct safety behaviour but raises a liveness question:
**how do honest validators in a byzantine-operator-controlled shard
escalate?** Options:

- A direct-to-gl0 attestation submission path for shard validators
  (i.e. validators can submit attestation sets without operator
  cooperation). Bypasses operator censorship at the cost of more
  wire traffic.
- A periodic "operator change" governance trigger if the shard
  stalls for an extended period.

**Open. Affects the Option C slashing design.**

### 7.5 Interaction with NIPoPoW per-shard tower

Per-shard NIPoPoW tower entries
([`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) §2) are produced at
the shard's sub-snapshot level. Under Option B, a tower entry is
admitted only if the underlying sub-snapshot's SC binary cleared the
quorum gate. This naturally lifts the per-shard tower's CP bound to
the same quorum-gated bound as the cross-shard CQ. The composition
is clean but worth formalising: a light client verifying a per-shard
tower entry inherits both `ε_snowball,s` and `ε_quorum,s` at the
tower-anchored ordinal.

**Open in GKL composition doc §6.5.**

### 7.6 Migration path

Pre-Option-B SC binaries are single-signed. Post-Option-B nodes must
accept both shapes during rollout (per
`:project_sharding_strategic_signals` "no backwards-compat constraint
for sharding" — but the metagraphs themselves are in production and
re-signing legacy binaries is not free). The staged-rollout flag
(`NAKAMOTO_SHARD_QUORUM_GATE`) handles the gl0 side; the per-shard
operator side needs analogous flagging.

**Open. Sequence with eventual era-3 hard-fork.**

---

## §8 Cross-references

| Doc | Relationship |
|---|---|
| [`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) §5.2 | Derives the `α_total > 1/(2S)` collapse boundary this proposal closes. |
| [`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) §5.3 | Cross-shard CP composition — Regime A is the regime Option B preserves at the sub-snapshot boundary. |
| [`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) §6.1 | Open question "stake-concentration mitigation" — this proposal is the answer. |
| [`docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md) §2 | Snowball cascade Option B reuses at the per-shard layer. |
| [`docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md) §0.A | Empirical `(K=3, α_cascade=2, β=10)` operating point Option B inherits. |
| [`docs/nakamoto/NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) §2 | Per-shard tower; §7.5 above sketches the composition. |
| [`docs/nakamoto/attestation-and-finality.md`](./attestation-and-finality.md) §0.2 | `T_count` quorum semantics Option B applies one level down (sub-snapshot). |
| [`docs/nakamoto/attestation-and-finality.md`](./attestation-and-finality.md) §0.4 G1 | The `pullFinalityGated` interface Option B extends to gate on shard-quorum. |
| `:project_sharding_strategic_signals` (memory) | Two-tier stake model (delegated + node collateral) Option B inherits via `StakeRegistry`. |
| `:project_kes_port_constraints` (memory) | KES port — prerequisite for Option C slashing follow-on. |
| `~/repos/research-nipopos-2026/sims/cross_shard.py` (branch `sim/integration`, commit `913c4a8`) | Empirical anchor — the simulator that surfaced the §1 gap. The `t_count` gate in the sim is the in-silico version of Option B's `τ`-quorum. |

---

*Research proposal. No code commitment. Implementation surface in §6
is reviewer-aid sizing; the project plan follows from the
recommendation in §5.*
