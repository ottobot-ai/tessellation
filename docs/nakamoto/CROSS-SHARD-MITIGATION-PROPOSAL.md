# Cross-shard stake-concentration mitigation — proposal

**Status:** research proposal. Decision input. No code commitment in this
document. Written 2026-05-15 against `feature/serde-typeclass-shim`
HEAD `c6528548`.
**Scope:** how to defend the cross-shard composition (gl0 aggregating
per-shard sub-snapshots via state-channel binaries) against the
stake-concentration attack derived in
[`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) §5.2.

**Revision history:**
- 2026-05-15 (initial): Recommended Option B (per-shard attestation-set
  on SC binaries, τ=⅔) as primary defence.
- 2026-05-15 (patch): User-directed flip from Option B (rejected —
  modifies metagraph consensus, out of scope) to **Option A
  (VRF-sortition of operator keys) + Option C (out-of-band slashing)
  combined defence**. Option B retained in §3 with REJECTED banner for
  historical record. §5 / §6 / §7 rewritten accordingly.

This document formalises three candidate defences, compares them on the
load-bearing criteria, and recommends the combined Option A + Option C
defence (Option B rejected by user directive 2026-05-15 — see §3 and
§5.2 for the rationale). Code locations the chosen options would touch
are sketched in §6; the detailed implementation work is downstream.

Cross-references:
- [`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) (commit `45c97895`) —
  per-shard / cross-shard GKL trinity; §5.2 derives the
  `α_total > 1/(2S)` collapse boundary that motivates this proposal.
- [`docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md)
  (commit `be1c8250`) — Snowball cascade `(K=3, α_cascade=2, β=10)` and
  the 4-trigger composition. Option B (rejected) had composed with that
  cascade; under the chosen Options A+C, gl0 still runs Snowball at the
  global layer but does not require shard-internal Snowball quorum on
  SC binary admission.
- [`docs/nakamoto/NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md)
  (commit `bcb42140`) — per-shard NIPoPoW tower; cross-shard tower is
  out of scope here (no cross-shard tower in v1).
- [`docs/nakamoto/attestation-and-finality.md`](./attestation-and-finality.md)
  — 4-phase model + `T_count` / `T_weight` / `T_depth1` / `T_depth2`
  trigger stack. Used at gl0 unchanged under Options A + C. (Rejected
  Option B had proposed extending `T_count` semantics to the per-shard
  sub-snapshot boundary.)
- [`modules/node-shared/.../statechannel/StateChannelValidator.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala)
  — the SC binary admission code path. §1.2 below dissects what it
  currently checks; §6 sketches the touch-points for the recommended
  option.
- [`modules/node-shared/.../managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala)
  — the gl0-side acceptance manager that buckets validated SC binaries
  into the next global snapshot.
- [`modules/shared/.../statechannel/StateChannelSnapshotBinary.scala`](../../modules/shared/src/main/scala/io/constellationnetwork/statechannel/StateChannelSnapshotBinary.scala)
  — the on-the-wire SC binary shape (`lastSnapshotHash`, `content`,
  `fee`). Unchanged under the chosen Options A + C; rejected Option B
  would have grown this with an attestation-set field.
- [`modules/node-shared/.../nakamoto/StakeRegistry.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/StakeRegistry.scala)
  — global validator registry; relevant if §7.1 picks a stake-weighted
  sortition unit.
- [`modules/node-shared/.../nakamoto/EligibilityChecker.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala)
  — canonical VRF + eta-seed pattern reused by Option A's
  `OperatorShardAssignment`.
- `:project_sharding_strategic_signals` (memory file) — directive on
  combined `delegated stake + node collateral` as the stake-weight
  source. Load-bearing on §7.1 sortition-unit decision under Option A.
- `~/repos/research-nipopos-2026/sims/cross_shard.py` (branch
  `sim/integration`, commit `913c4a8`) + figures /
  `sims/data/cross_shard_t_count_fixed_30s_concentrated_in_shard_0_S4_slots1000.json` —
  empirical anchor for the §1 problem statement.

---

## §0 TL;DR

**Combined mitigation: Option A (VRF-sortition of operator keys to
shards) as structural defence + Option C (out-of-band slashing /
governance) as economic deterrent. Option B (per-shard attestation-set
on SC binaries) is REJECTED — modifying how metagraphs construct their
submitted state-channel binaries is out of scope (user directive
2026-05-15).**

The one-paragraph rationale: under the GKL §5.2 derivation, an
adversary holding `α_total > 1/(2S)` global stake can concentrate
operator keys into one shard and drive `α_local > 1/3` there. **Option
A** blocks the structural concentration by deciding shard membership
of operator keys via VRF: the set of operator keys eligible to submit
SC binaries for a shard at epoch `e` is the deterministic VRF draw
`VRF(operator_pk, ηₑ) → shard_id`. An adversary controlling `N`
operator keys cannot place them all in one shard — the assignment is
the protocol's, not the operator's. The eligible-submitter set
rotates per epoch. **Option C** provides the economic deterrent: if a
byzantine operator slips through VRF-sortition at the margin and
submits divergent state, on-chain equivocation evidence triggers
slashing of the offending operator's node collateral. KES forward-
secure signatures are a hard prerequisite for slashing so evidence
cannot be repudiated by claiming the signing key was rotated after
the forge. **Why not B:** Option B would require the metagraph
operator to gather a quorum of shard validators to multi-sign the SC
binary *before* submitting to gl0. That changes the metagraph's
submission pipeline — a single-operator-signed binary becomes a
multi-sig artifact — which is precisely the metagraph-consensus
modification the user has declared out of scope: *"for metagraph
security, we leave it to them, if they submit a valid snapshot with
signatures we accept it. Modifying metagraph consensus is out of
scope."* The structural-vs-deterrent split (A + C) substitutes for
the rejected attestation-set approach (B) by moving the defence
entirely to gl0's admission and post-hoc enforcement layers.

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

### 2.4 Key parameters — DECIDED 2026-05-20

User-locked decisions for v1 (per the metagraph-throughput / scaling thread, anchored on "tens of thousands of operators, hundreds-to-thousands of metagraphs" target). State-management ergonomics on a per-operator node was the dominant deciding factor.

| Parameter | Value (v1) | Justification |
|---|---|---|
| **`m`** — shards per operator | **1** (one shard + global) | Operational: each operator runs gl0 (universal) + exactly one metagraph's state. Per-operator state cost is O(1) regardless of total metagraph count. The liveness variance concern at small N/S is handled by staggered rotation (below), not by multi-shard overlap. |
| **Stagger fraction per eta-boundary** | **1/4** | Each rotation only reassigns 1/4 of operators per shard; 75% continuity preserved across boundaries. Eth2 sync-committee precedent. Smooths bootstrap pressure 4× vs full rotation. |
| **Sortition unit** | **per-operator (PeerId)** | Simpler than per-validator; matches existing seedlist semantics. Delegators stake to operators without re-staking per-shard. |
| **`E`** — epoch length | **eta-rotation `R = 2550` snapshots (~6h at 7s)** | Reuses existing cadence. Adversary stake-rebalancing window matches the existing one. |
| **`S`** — shard count (v1) | **static, `S = M` (one shard per metagraph)** | v1 simplicity. Defer multi-metagraph-per-shard to v2 when needed. At target scale (10K-100K operators × 1K-3K metagraphs) gives N/S = 10-100 operators/shard — comfortably above the N/S ≥ 12 honest-majority safety floor. |
| **Pre-warm window** | **1 epoch advance notice** | Operator gets ~6h to sync the assigned shard's state before duty fires. |
| **Onboarding lag** | **earliest sortition = `join_epoch + 1 + n_warm`** | New operator syncs gl0 first, then gets advance notice for first shard duty. |
| **Emergency rotation** | **fallback if shard quorum drops <2/3 for K snapshots** | Liveness backstop. Pulls in spillover operators from adjacent shards. |
| **Slashed-operator handling** | **rotate out at next eta-boundary** | Mid-epoch re-sortition adds complexity; stake is already gone so the operator has nothing to attack with mid-epoch. |

**Configurability**: each parameter env-overridable so testnet can tune (e.g., `NAKAMOTO_SHARDS_PER_OPERATOR=1`, `NAKAMOTO_STAGGER_FRACTION=4`). Default to the values above.

### 2.5 Verdict

Option A solves the §1.1 problem by structural construction: an
adversary cannot place operator keys in the shard of its choosing,
because the assignment is VRF-determined. The cost is operator-
topology disruption — the "operators choose which metagraphs to run"
model is load-bearing for the current metagraph-as-business-unit
framing (`:reference_pacaswap_multimetagraph`) and Option A modifies
that framing at the *sortition unit* (whose keys are eligible to
submit binaries for which shard at which epoch). It interacts with
KES rollout, the two-tier stake model, and the metagraph-genesis flow
in ways §7 enumerates.

**Status: chosen (combined with Option C — see §0 and §5).** The
2026-05-15 user directive selected Option A as the structural defence
on the explicit grounds that modifying metagraph consensus (Option B)
is out of scope.

### 2.6 Operational considerations at scale

At target scale (10K-100K operators, 1K-3K metagraphs), per-epoch bandwidth is the dominant operational cost:

- **Per-rotation bootstrap**: with `stagger_fraction = 1/4` and N=10K, each eta-boundary triggers `N/4 = 2,500` operators-in-rotation. If each carries ~100MB of metagraph state, cluster-wide transfer ≈ 250GB per eta-boundary (~6h), or ~11MB/s sustained. Per-node sync is bounded at ~1Gbps → ~0.8s per operator, parallelizable.
- **Pre-warming budget**: an operator notified at `join_epoch + 1` has ~6h to download state before being on-duty. 100MB in 6h is trivial; this concern is academic unless state grows to GB-scale per metagraph.
- **gl0 stamping load**: aggregate snapshot includes one tip-hash per shard. At S=3000 shards, 32-byte hashes + metadata ≈ 100KB per gl0 snapshot. At 7s cadence ≈ 14KB/s — fine.
- **gl0 acceptance CPU**: one VRF-eligibility check per binary admission. At 3000 binaries per gl0 snapshot × ~10ms each = 30s of work per snapshot — exceeds the 7s cadence. Mitigation: batched VRF verification, cached committee assignments per `(operator, epoch)`. Address in implementation phase A2.

---

## §3 Option B: Per-shard attestation-set on SC binaries — **REJECTED**

### 3.0 Why rejected (user directive 2026-05-15)

Option B requires the metagraph's L0 operator to coordinate a quorum
of shard validators to multi-sign the SC binary **before** posting it
to gl0. That changes the metagraph's submission pipeline, turning a
single-operator-signed binary into a multi-sig artifact and adding a
shard-internal consensus round to the metagraph's path. Per user
directive: *"for metagraph security, we leave it to them, if they
submit a valid snapshot with signatures we accept it. Modifying
metagraph consensus is out of scope."* Option B is therefore rejected.
The remainder of this section is retained for historical record and
counterfactual reference; the analysis below was the prior
recommendation before the 2026-05-15 patch.

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

**Chosen, in combination with Option A.** Per the 2026-05-15 user
directive, Option C is the economic-deterrent half of the combined
defence. Option A blocks the structural concentration by VRF-assigning
operator keys to shards; Option C deters the residual marginal attacks
(adversary keys that happen to land in the target shard by VRF luck)
by slashing operator collateral on detected divergence. KES forward-
security is the hard prerequisite for evidence non-repudiation.

**Status: chosen. Sequenced after Option A baseline and after KES
port.**

---

## §5 Recommendation + rationale

### 5.1 Choice

**Option A (VRF-sortition of operator keys to shards) as structural
defence, combined with Option C (out-of-band slashing of node
collateral) as economic deterrent. Option B is REJECTED.**

The two chosen options play complementary roles:

- **Option A — structural defence.** The set of operator keys that
  can submit SC binaries for a given shard at epoch `e` is
  determined by VRF on `(operator_pk, ηₑ)`, not by operator choice.
  An adversary controlling `N` operator keys cannot concentrate them
  all into one shard because the assignment is deterministic and
  the protocol's — not the operator's. The eligible-submitter set
  rotates per epoch on the same eta-rotation cadence as gl0's
  slot-leader eligibility (see
  [`EligibilityChecker.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala)
  for the VRF pattern to reuse). At gl0, SC binary admission is
  gated by a VRF-eligibility check at
  [`StateChannelValidator.validateAllowedSignatures`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala)
  (currently lines `164-167`) — the operator key signing the binary
  must be VRF-eligible for the target shard at the current epoch.
- **Option C — economic deterrent.** If a byzantine operator slips
  through the VRF lottery (rare, but possible at the margin) and
  submits divergent state, the operator's node collateral is slashed
  via on-chain equivocation proof. KES forward-secure signatures
  (`:project_kes_port_constraints`) are the hard prerequisite — a
  slashed operator must not be able to repudiate evidence by claiming
  the signing key was rotated post-forge.

**Together: A blocks the structural concentration attack at the
admission boundary; C deters the residual marginal attacks by
imposing economic loss on detected divergence. The combination
substitutes for the rejected attestation-set approach (B), moving the
defence entirely to gl0's VRF-eligibility check and the post-hoc
slashing pipeline — no change to the metagraph's SC binary
construction.**

### 5.2 Why Option B is rejected

Option B would require the metagraph's L0 operator to coordinate a
quorum of shard validators to multi-sign the SC binary **before**
posting it to gl0. The wire artifact would mutate from a single-
operator-signed binary to a multi-sig artifact, and the metagraph's
submission pipeline would gain a shard-internal consensus round (the
operator must collect Snowball-decided attestations, sum stake against
τ, then post).

Per the user directive (2026-05-15): *"for metagraph security, we
leave it to them, if they submit a valid snapshot with signatures we
accept it. Modifying metagraph consensus is out of scope."* Option B
is therefore out of scope; the structural defence must live entirely
on the gl0 side. Option A satisfies that constraint (VRF-eligibility
is a gl0 admission check; the metagraph's submission pipeline is
unchanged — still a single operator signature).

### 5.3 Sequencing

1. **A0** — VRF-sortition design freeze: pick sortition unit
   (per-operator vs per-validator vs hybrid — see §7.1), epoch length
   `E` (see §7.3), and shard count `S` baseline. Closes via decision
   in §7.
2. **A1** — `OperatorShardAssignment` service in
   `node-shared/.../domain/sharding/` (location TBD pending the
   broader sharding skeleton). Reuses the VRF + eta pattern from
   [`EligibilityChecker.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala).
3. **A2** — `StateChannelValidator` gains a `validateShardEligibility`
   step at `validateAllowedSignatures` (lines `164-167`); behind a
   staged-rollout config flag.
4. **C0** — KES port (`:project_kes_port_constraints`) — hard
   prerequisite for C1.
5. **C1** — Equivocation detector at gl0: monitors for two SC binaries
   from same operator key at same shard slot with different state
   hashes; emits a slashing-evidence packet.
6. **C2** — Slashing pipeline: on-chain consumption of evidence
   packets; node-collateral burn / governance pause.

### 5.4 What we are explicitly **not** committing to

- An implementation. This is a design proposal; A0..C2 are sizing
  estimates, not a project plan.
- The exact slashing magnitude. Open — see §7.4.
- The cross-shard NIPoPoW story. Out of scope; per-shard NIPoPoW
  remains as in [`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md).
- Any change to metagraph consensus or the SC binary construction
  pipeline. Out of scope by user directive (see §5.2).

---

## §6 Implementation skeleton (Options A + C)

This section sketches the code-level surface for the chosen combined
defence as a reviewer aid. None of this is committed in this
document. Option B's implementation skeleton has been removed —
see §3 (Option B is rejected).

### 6.1 Option A — VRF-sortition of operator keys to shards

#### 6.1.1 New service: `OperatorShardAssignment`

Location: `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/sharding/`
(directory does not yet exist; created with the broader sharding
skeleton).

```scala
trait OperatorShardAssignment[F[_]] {
  /** VRF-derived assignment: returns the shard the operator key is
    * eligible to submit binaries for at epoch `e`. */
  def assignmentFor(operatorPk: PublicKey, epoch: EpochProgress): F[ShardId]

  /** Inverse view: returns the set of operator keys eligible to
    * submit binaries for `shard` at epoch `e`. */
  def eligibleOperators(shard: ShardId, epoch: EpochProgress): F[Set[PublicKey]]

  /** Predicate used by the gl0 validator. */
  def isEligible(operatorPk: PublicKey, shard: ShardId, epoch: EpochProgress): F[Boolean]
}
```

VRF input: `(operator_pk, ηₑ ‖ "SHARD-ASSIGN")`. ηₑ is the eta-seed
for epoch `e`, the same seed the gl0 LDD slot-leader election uses.
See [`EligibilityChecker.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala)
for the canonical VRF pattern to reuse. Output: a deterministic
shard index in `[0, S)`.

#### 6.1.2 Admission gate at `StateChannelValidator`

[`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala)

Today `validateAllowedSignatures` (lines `164-167`) chains:

```scala
validateSignaturesWithSeedlist(stateChannelOutput.snapshotBinary)
  .andThen(_ => validateStateChannelAddress(stateChannelOutput.address))
  .andThen(_ => validateStateChannelAllowanceList(stateChannelOutput.address, stateChannelOutput.snapshotBinary))
```

Add a fourth step:

```scala
  .andThen(_ => validateShardEligibility(stateChannelOutput, operatorShardAssignment, currentEpoch))
```

where `validateShardEligibility` extracts the operator's `PublicKey`
from the binary's signing proof, resolves the binary's target shard
from `stateChannelOutput.address`, and asks
`OperatorShardAssignment.isEligible` for the current epoch. Rejection
on `false`.

The constructor at `StateChannelValidator.make` (line 84) gains an
`operatorShardAssignment: OperatorShardAssignment[F]` parameter and
an `epochProgressR: F[EpochProgress]` reader, both wired through
[`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/modules/SharedValidators.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/modules/SharedValidators.scala).

#### 6.1.3 Epoch boundary

Epoch length `E` is open (see §7.3). Default candidate: tie to the
existing eta-rotation `R = 2550` snapshots (≈ 1 day at 7s cadence) to
avoid introducing a new rotation period. Short `E` → less time for
adversary stake re-accumulation; long `E` → less assignment churn.

#### 6.1.4 Address → shard mapping

The binary's target shard is resolved from
`stateChannelOutput.address` (the metagraph address). Open whether
this is a 1:1 (address ↔ shard) mapping or whether large metagraphs
span multiple shards. For v1 assume 1:1; revisit in the broader
sharding plan.

#### 6.1.5 Config knobs

```
NAKAMOTO_SHARD_VRF_EPOCH_LEN_SNAPSHOTS = 2550   // ηₑ rotation cadence
NAKAMOTO_SHARD_COUNT_S                  = 4     // baseline; see §1
NAKAMOTO_SHARD_VRF_ELIGIBILITY_GATE     = on|off  // staged rollout
```

### 6.2 Option C — slashing of node collateral on detected divergence

#### 6.2.1 Detection at gl0

The detector observes the SC binary stream entering
[`GlobalSnapshotStateChannelAcceptanceManager.accept`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala)
and flags any pair `(b₁, b₂)` such that:

- both are signed by the **same operator public key**, and
- both target the **same shard slot** (`address`, `parent.lastSnapshotHash`,
  or equivalent canonical shard-ordinal key), and
- they carry **different state hashes** (`content` digest or
  `lastSnapshotHash` of a downstream binary that anchors to them).

This is an equivocation event. Both binaries are retained as
evidence.

#### 6.2.2 Slashing-evidence packet

```scala
case class StateChannelEquivocationEvidence(
  operator: PublicKey,
  shard: ShardId,
  epoch: EpochProgress,
  binaryA: Signed[StateChannelSnapshotBinary],
  binaryB: Signed[StateChannelSnapshotBinary],
  kesProof: KesEvolutionProof  // proves both signatures predate any key rotation
)
```

Both signatures are verifiable against `operator`; the KES evolution
proof witnesses that the operator could not have rotated keys
between the two signings. KES is the hard prerequisite — see
`:project_kes_port_constraints`.

#### 6.2.3 Slashing pipeline

On evidence acceptance: the offending operator's node collateral is
slashed (magnitude TBD — see §7.4) and the operator is optionally
paused pending governance review. Slashed collateral may be burned
or redistributed to honest validators in the same shard. The
mechanics integrate with the existing `nodeCollateral` accounting in
per-metagraph state.

#### 6.2.4 Config knobs

```
NAKAMOTO_SLASHING_MAGNITUDE_PCT       = TBD   // see §7.4
NAKAMOTO_SLASHING_GATE                = on|off
NAKAMOTO_SLASHING_GOVERNANCE_PAUSE    = on|off
```

### 6.3 What was removed

The earlier draft sketched Option B implementation: shard-quorum
aggregation, multi-proof SC binary wire shape, per-shard
`StakeRegistry.shardValidators`, and shard-internal Snowball
attestation collection. All of that is **removed** — Option B was
rejected (see §3). The metagraph's SC binary submission pipeline is
unchanged under the chosen Options A + C; the defence lives entirely
on the gl0 admission and slashing layers.

---

## §7 Open questions

These are flagged for the implementation workstream and the
Tier-2 sharding plan. This document does not attempt to close them.

### 7.1 Sortition unit — **load-bearing under Options A + C**

Under the chosen design the VRF-sortition input determines what kind
of key is assigned to a shard. Per user directive (2026-05-15), the
default is **per-operator-key** sortition: each operator public key
draws its eligible shard from `VRF(operator_pk, ηₑ)`. But the two-
tier stake model from `:project_sharding_strategic_signals` — combined
**delegated stake + node collateral** — may push toward a finer-
grained unit. Three candidates:

- **Per-operator-key (default).** Sortition input is the operator's
  L0 signing public key. Simplest; matches the directive's plain
  reading. Concentration is structurally blocked at the eligible-
  submitter set; an operator with N keys cannot place them all into
  one shard.
- **Per-validator (delegated-stake-weighted).** Sortition input is a
  (validator, operator) tuple; eligibility is weighted by delegated
  stake. Aligns the two-tier model: delegators staking to operator
  `O` indirectly influence shard assignment.
- **Hybrid.** Operators declare eligibility at registration time; VRF
  picks the *active subset* of eligible operators per epoch per
  shard. Lets operators opt into shards they have infrastructure
  for, while preserving per-epoch VRF rotation.

**Needs decision before A0.** The two-tier-stake interaction is the
load-bearing constraint; see `:project_sharding_strategic_signals`.

### 7.2 `T_depth1` fallback at the per-shard sub-snapshot boundary

If a shard's elected operator key is offline or its VRF-eligible
set is degenerate (no live key) for an extended period, the shard's
state cannot advance. The current `T_depth1` at gl0 handles this
*for gl0 itself*; for the per-shard sub-snapshot, an analogous
structural fallback is needed. Options:

- **Wider VRF assignment after timeout.** After `T` epochs without a
  binary from any VRF-eligible operator, broaden eligibility to the
  next-epoch set (or to the global operator set). Live but with
  reduced concentration resistance until the partition heals.
- **Structural-depth proof.** The operator posts an SC binary plus
  a per-shard NIPoPoW header proving the sub-snapshot is depth-`k₁`
  deep in the shard's chain; gl0 admits on that proof in lieu of
  eligibility. Cleaner but requires per-shard NIPoPoW first.

**Open. Recommended path: wider VRF assignment for v1; structural-
depth proof later.**

### 7.3 VRF-sortition epoch length `E`

How long is each epoch over which a fixed VRF-shard-assignment holds?
Default candidate: tie to the eta-rotation `R = 2550` snapshots
(≈ 1 day at 7s cadence) so no new rotation cadence is introduced.
Tradeoff:

- **Short `E`.** Less time for an adversary to target a known shard
  assignment; less time for accumulated stake re-positioning. More
  assignment churn — operators bootstrap state more often.
- **Long `E`.** Less churn, simpler operator UX. More time for an
  adversary to plan around a specific epoch's assignments.

**Open. Needs cross-shard sim re-run at variable `E` to characterise
the safety-vs-operational-cost frontier. Replaces the earlier "optimal
τ" question, which is no longer relevant — τ was Option B's quorum
threshold; rejected.**

### 7.4 Combined-defence flow: byzantine operator that slips through VRF

Under Option A alone, an operator key that is VRF-eligible for a
shard at epoch `e` can submit any cryptographically valid SC binary
for that shard — including divergent state. The structural defence
does **not** prevent this; it only bounds *how many* keys can
concentrate. A byzantine operator landing in shard `s` is rare per
key but possible. The combined flow:

1. The byzantine operator submits two divergent SC binaries for the
   same shard slot (the equivocation that constitutes evidence).
2. gl0's equivocation detector (§6.2.1) observes both binaries.
3. The slashing-evidence packet (§6.2.2) is constructed and admitted
   on-chain.
4. The operator's node collateral is slashed (§6.2.3); future epochs'
   VRF-eligibility for that operator's key is unaffected by VRF but
   the operator no longer has collateral at stake, so the deterrent
   has bitten.

This is the residual-attack path Option C is designed to deter.

**Open subquestions:**
- **Escalation when only one divergent binary is observed (not yet
  two).** A byzantine operator might submit one divergent binary
  and never equivocate; gl0 has no on-chain evidence to slash. Cross-
  shard fraud-proof construction or external monitoring would be
  needed.
- **Slashing magnitude.** What fraction of the operator's node
  collateral is burned per evidenced equivocation? 100% (total burn,
  maximal deterrent) vs partial-slash with escalation. Open.
- **Governance escalation.** Should repeated equivocation or a
  cluster-detected shard stall trigger a governance pause on the
  operator's metagraph? Open; tied to `:project_sharding_strategic_signals`
  governance model.

### 7.5 Interaction with NIPoPoW per-shard tower

Per-shard NIPoPoW tower entries
([`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) §2) are produced at
the shard's sub-snapshot level. Under Options A + C, a tower entry is
admitted via the standard gl0 path, and the per-shard CP bound the
tower encodes inherits the VRF-eligibility check (Option A) and the
slashing-evidence guarantee (Option C). The light-client composition:
a verifier of a per-shard tower entry can assume the underlying SC
binary was admitted by a VRF-eligible operator at the entry's epoch,
and that any divergence would have surfaced slashing evidence at gl0.

**Open in GKL composition doc §6.5.**

### 7.6 Migration path

Under the chosen Options A + C, **no migration is needed for the
metagraph submission pipeline.** SC binaries remain single-operator-
signed; the wire shape of `StateChannelSnapshotBinary` is unchanged.
Migration is purely on the gl0 side and consists of two staged
rollouts:

- **`NAKAMOTO_SHARD_VRF_ELIGIBILITY_GATE`** — gl0 admission rejects
  binaries from operator keys not VRF-eligible for the target shard
  at the current epoch. Staged per-metagraph to allow operators to
  align infrastructure with assignments.
- **`NAKAMOTO_SLASHING_GATE`** — gl0 detects equivocations and admits
  slashing-evidence packets. Requires KES port (see §6.2). Staged
  after operators have rotated to KES signing keys.

Metagraphs in production continue to operate unchanged on their side;
the gl0-side gates flip on independently. **Open. Sequence after KES
port lands.**

---

## §8 Cross-references

| Doc | Relationship |
|---|---|
| [`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) §5.2 | Derives the `α_total > 1/(2S)` collapse boundary this proposal closes via Options A + C. |
| [`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) §5.3 | Cross-shard CP composition — the regime preserved by VRF-eligibility (Option A) and slashing-evidence enforcement (Option C) at the gl0 admission boundary. |
| [`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) §6.1 | Open question "stake-concentration mitigation" — this proposal is the answer. |
| [`docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md) §2 | Snowball cascade — used by gl0 for global finality. Under the chosen Options A + C it is **not** required at the per-shard layer (would have been under rejected Option B). |
| [`docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md) §0.A | Empirical `(K=3, α_cascade=2, β=10)` operating point at the global layer. |
| [`docs/nakamoto/NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) §2 | Per-shard tower; §7.5 above sketches the composition with VRF-eligibility + slashing. |
| [`docs/nakamoto/attestation-and-finality.md`](./attestation-and-finality.md) §0.2 | `T_count` quorum semantics at gl0 (unchanged). Option B had proposed extending this to the sub-snapshot boundary; rejected. |
| [`docs/nakamoto/attestation-and-finality.md`](./attestation-and-finality.md) §0.4 G1 | The `pullFinalityGated` interface; remains the gl0-side gate, no extension required under Options A + C. |
| [`modules/node-shared/.../nakamoto/EligibilityChecker.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala) | Canonical VRF + eta-seed pattern Option A's `OperatorShardAssignment` reuses. |
| `:project_sharding_strategic_signals` (memory) | Two-tier stake model (**delegated stake + node collateral**) — **load-bearing on §7.1 sortition-unit decision** (per-operator-key default vs delegated-stake-weighted). |
| `:project_kes_port_constraints` (memory) | KES port — **hard prerequisite** for Option C slashing evidence non-repudiation. |
| `~/repos/research-nipopos-2026/sims/cross_shard.py` (branch `sim/integration`, commit `913c4a8`) | Empirical anchor — the simulator that surfaced the §1 gap. Under Options A + C the sim's `t_count` gate is replaced by VRF-eligibility check (Option A) + post-hoc slashing (Option C); a sim re-run with the new gate is implied by §7.3 (epoch-length sweep). |

---

*Research proposal. No code commitment. Implementation surface in §6
is reviewer-aid sizing; the project plan follows from the
recommendation in §5. Revised 2026-05-15 per user directive — see
revision history at the top of the file.*
