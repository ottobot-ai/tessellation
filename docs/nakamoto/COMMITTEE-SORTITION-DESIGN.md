# Committee Sortition Primitive — Design

**Status:** draft, 2026-05-17
**Scope:** the per-metagraph committee sortition function (§4 cross-shard sharding, Option A from `project_cross_shard_cq_collapse_bound`). Slashing (Option C, #181-ish) and N-2 staggering (#180) are downstream of this doc and treated as out-of-scope here.

---

## 1. Goal

Each metagraph snapshot must be validated by a randomly-sampled **committee** of operator keys rather than by every gl0 operator. The sortition function must:

1. Be **deterministic** given public inputs `(eta, metagraph_id, snapshot_ord)` and the N-2 stake distribution — every honest node arrives at the same committee.
2. Be **locally-checkable**: an operator can determine "am I in this committee?" from their own VRF SK + public stake state, without coordinating with peers.
3. Produce a committee with **honest majority** under our standard adversarial model (≤ 1/3 stake adversarial) with parametric confidence ε.
4. Compose with existing primitives: VRF (§1.1, `EligibilityChecker`), KES (§1.2, per-operator-key), stake registry (N-2 frozen, per `consensus-epoch-staggering`).

The committee's role downstream: every committee member emits a KES-signed attestation on the metagraph's snapshot. Non-committee operators ignore the snapshot. Slashing (Option C, out of scope here) catches a committee member that emits contradictory attestations on competing snapshots.

---

## 2. Inputs

| Input | Source | Why |
|---|---|---|
| `eta` (32 B) | current epoch's eta seed, computed from 2/3-mark of N-1 per `consensus-epoch-staggering` | freshness — adversary can't pre-compute next committee |
| `metagraph_id` (32 B) | the address/id of the metagraph being validated | per-metagraph isolation |
| `snapshot_ord` (8 B) | the metagraph snapshot ordinal being voted on | committee rotates per snapshot |
| `stake_dist` | N-2 frozen StakeRegistry | adversary can't reshuffle stake mid-epoch to bias committee |
| `K_target` | metagraph-supplied parameter (with floor enforced by L0) | committee size target |

VRF message: `Blake2b-256(eta || metagraph_id || snapshot_ord || "committee")` — domain-separated from leader VRF so a leader VRF win doesn't leak committee eligibility for any metagraph.

---

## 3. Primitive options

### Option A — VRF-threshold per operator key (Algorand-style independent draws)

Each operator key `i` with stake fraction `σ_i` (against total active stake) computes one VRF and checks:

```
vrf_output_i  <  K_target · σ_i      ⇒  in committee
```

- **Local check.** No global view of other VRFs needed.
- **Stochastic committee size.** `E[|committee|] = K_target`, `Var ≤ K_target`.
- **Verifiability.** Verifier checks the same threshold against the published VRF proof.
- **Stake explosion.** If `σ_i · K_target > 1` the threshold saturates and the key is always in. Operators with very high single-key stake share lose the random-sampling property. Mitigated by per-operator-key sortition (per `consensus-epoch-staggering` §Sortition unit): an operator with stake fraction 0.4 across 4 keys gives each key σ = 0.1, restoring randomness for `K_target ≤ 10`.

### Option B — Top-K-by-VRF (sorted)

Every operator key publishes VRF output; sort by output; top `K_target` form the committee.

- **Deterministic size.** Always exactly K.
- **Requires global VRF gossip** before anyone can decide who's in — pushes the protocol toward a synchronous all-to-all round.
- **Verifier needs the full sorted list** to confirm membership — no local check.
- **Failure mode.** A single missing VRF gossip blocks the committee determination for that snapshot until timeout.

### Option C — Hybrid (VRF-threshold with size guarantee)

Run Option A; if `|committee| < K_min`, fall back to top-K of the VRF outputs to top-up.

- Solves Option A's low-tail variance (the snapshot-with-zero-committee case).
- Inherits Option B's gossip requirement, but only on the fallback path.
- Operational complexity: now there are two committee-determination paths, with subtle agreement requirements about when to fall back.

---

## 4. Axis-by-axis trade-offs

### 4.1 VRF-threshold (A) vs top-K (B) vs hybrid (C)

| | A — VRF-threshold | B — Top-K | C — Hybrid |
|---|---|---|---|
| Local check | ✅ | ❌ | partial |
| Deterministic size | ❌ | ✅ | mostly |
| Gossip requirement | none (just attestations) | full VRF round | conditional |
| Verifier complexity | low | high (sorted list) | medium |
| Algorand precedent | yes | no | no |
| Liveness under partition | best | worst | medium |
| Honest-majority math | clean Chernoff | clean (size fixed) | mixed |

**Recommendation: Option A.** The local-check property is load-bearing for our existing architecture — `EligibilityChecker.checkEligibility` is already a local check, the consensus loop has no concept of "wait for everyone's VRFs before deciding." Forcing top-K would require a new round, new timeout, new failure path. Option A drops in alongside the existing leader VRF.

Variance mitigation for A's "empty committee" tail: pick `K_target` large enough that `P[|committee| < K_min] < ε` (Chernoff §6).

### 4.2 Per-metagraph vs global committee

**Per-metagraph (recommended):**
- VRF message includes `metagraph_id`. Each metagraph gets its own committee per snapshot.
- Isolates failure: compromising one metagraph's committee doesn't affect others.
- Matches the failure-isolation rationale for sharding in the first place.

**Global:**
- One committee per snapshot ordinal, validates all metagraphs in that round.
- Smaller total committee count → lower coordination cost.
- But: a 1/3-adversarial committee compromises *all* metagraphs in that slot. Defeats the point of sharding.

**Recommendation: per-metagraph.** Matches the threat model that motivated sharding (§4 CQ collapse bound).

### 4.3 Fixed K vs stake-proportional K

**Fixed K (recommended for v1):**
- All metagraphs use the same `K_target` (e.g., 100).
- Simpler.
- Doesn't account for "high-value metagraph wants extra protection."

**Per-metagraph K, with floor:**
- Metagraph supplies its desired K via genesis/registration; L0 enforces `K ≥ K_floor`.
- Naturally lets higher-stake metagraphs buy more security.
- Adds a config knob and a validator (K_floor enforcement).

**Per-epoch census-derived K, v3 (Recommended end-state):**
- L0 computes `K = clamp(K_min, K_max, K_base · log2(1 + N_active / M_active))` at every N-2 epoch boundary, freezes it for the epoch.
- Inputs read from the N-2 frozen state:
  - `N_active`: stake-fraction-weighted active operator-key count from `StakeRegistry` (matches `consensus-epoch-staggering`'s denominator).
  - `M_active`: count of registered metagraphs that produced a binary in the prior `staleness_window` snapshots (read from `GlobalIncrementalSnapshot.lastCurrencySnapshots`).
- Defaults: `K_min = 50` (ε ≤ 6×10⁻², Chernoff §6), `K_max = 400` (ε ≤ 10⁻¹⁰), `K_base ≈ 40`.

Why this scales to thousands of operators × thousands of metagraphs:

| N (operators) | M (metagraphs) | K | Cluster work/snapshot |
|---|---|---|---|
| 100 | 1 | 50 (floor) | 50 attestations |
| 1,000 | 10 | 232 | 2.3K attestations |
| 10,000 | 100 | 270 | 27K attestations |
| 10,000 | 1,000 | 138 | 138K attestations |
| 10,000 | 10,000 | 50 (floor) | 500K attestations |

Cluster-wide work is `M · K = M · K_base · log2(N/M)` — sub-quadratic in operator count, linear in metagraph count. Naturally collapses to "give a single metagraph full protection" when M is small and "shard aggressively" when M ≫ N.

Don't make K vary inside an epoch — the N-2 staging rule from `consensus-epoch-staggering` (only one consensus variable changes per boundary) applies. K becomes the N-2 census's output, frozen for the epoch. Implicit "ordinal-varying" comes from the N-2 stake-snapshot rotation, not from a free `ord` parameter — keeps the threshold deterministic across observers.

**Recommendation: fixed K for v1, per-metagraph K with floor for v2, census-derived K for v3.** The threshold function already takes `K_target` as an input, so the v2/v3 upgrades are wiring changes against a stable primitive — no algorithm change. v1→v2 lets a high-value metagraph buy K up to `K_max` via its registration cert; v2→v3 removes the governance vector entirely by computing K from on-chain census.

### 4.4 Sortition unit (settled in `consensus-epoch-staggering`)

Per-operator-key, not per-operator. Each operator key gets its own VRF + stake fraction. This is already decided; the design here just lifts it intact.

### 4.5 Stake fraction denominator

Stake-fraction over **active** N-2 registered stake (matches `MinActiveQuorumFraction` denominator from `consensus-epoch-staggering`). Inactive keys (no recent attestations) are excluded from the denominator so an offline operator doesn't shrink the effective committee.

---

## 5. Recommended primitive

```
def isInCommittee(
  operatorKey: PeerId,
  vrfSk: Array[Byte],
  eta: Array[Byte],
  metagraphId: MetagraphId,
  snapshotOrd: Long,
  sigmaOperatorKey: Ratio,    // stake fraction over N-2 active stake
  kTarget: Int
): Option[(VrfProof, VrfOutput)] = {
  val message = Blake2b256(eta ++ metagraphId.bytes ++ snapshotOrd.toBigEndianBytes ++ "committee".getBytes)
  val proof = vrf.prove(vrfSk, message)
  val output = vrf.proofToHash(proof)
  val testValue = vrfOutputAsRatio(output)              // Ratio in [0, 1)
  val threshold = Ratio(kTarget) * sigmaOperatorKey     // K · σ_i; saturates at 1
  if (testValue < threshold) Some((proof, output)) else None
}
```

Verifier counterpart: `verifyCommitteeMembership(operatorVk, eta, metagraphId, snapshotOrd, sigmaOperatorKey, kTarget, proof)` — checks both VRF verification AND that `testValue < threshold`.

Domain separation: `"committee"` byte-suffix in the hash input makes the committee VRF independent from the leader VRF. A leader winning a slot reveals their leader-VRF output, but not their committee-VRF output for any future snapshot.

---

## 6. Honest-majority bound

Let `f_adv ≤ 1/3` be the adversarial stake fraction. Each operator key `i` enters the committee independently with `p_i = K · σ_i`. The committee is the set of selected keys; honest fraction in the committee is a Bernoulli sum.

Let `H = Σ_{i honest} Indicator(i selected)`, `A = Σ_{i adversarial} Indicator(i selected)`, `|committee| = H + A`.

`E[H] = K · (1 - f_adv) ≥ 2K/3`. `E[A] = K · f_adv ≤ K/3`.

Honest majority means `H > A`, i.e., `H > |committee|/2`. By Chernoff:

```
P[H ≤ |committee|/2]  ≤  exp(-K · D(1/2 ‖ 2/3))
                      ≈  exp(-K · 0.057)
```

| K | Honest-majority failure prob | Cadence |
|---|---|---|
| 50 | 6 × 10⁻² | per metagraph snapshot |
| 100 | 3 × 10⁻³ | per metagraph snapshot |
| 200 | 1 × 10⁻⁵ | per metagraph snapshot |
| 400 | 1 × 10⁻¹⁰ | per metagraph snapshot |

Per-snapshot ε must be amortized over snapshot rate × deployment lifetime. Production K should target ε ≤ 10⁻¹² per snapshot to stay below "1 failure per century" at ~7 s cadence. **K = 400** is the production starting point; lower for testnet.

Variance bound (empty-committee tail): `P[|committee| = 0] ≤ exp(-K · D(0 ‖ 1))` — negligible at K = 100+. The "low-tail" tail isn't a real concern at production K.

---

## 7. Implementation sketch

### Pure primitive (uncontroversial)

`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/CommitteeSortition.scala`

```scala
trait CommitteeSortition[F[_]] {
  def isInCommittee(
    vrfSk: Array[Byte],
    eta: Array[Byte],
    metagraphId: MetagraphId,
    snapshotOrd: Long,
    sigmaOperatorKey: Ratio,
    kTarget: Int
  ): F[Option[(Array[Byte], Array[Byte])]]                  // (proof, output)

  def verifyMembership(
    vrfVk: Array[Byte],
    eta: Array[Byte],
    metagraphId: MetagraphId,
    snapshotOrd: Long,
    sigmaOperatorKey: Ratio,
    kTarget: Int,
    proof: Array[Byte]
  ): F[Boolean]
}
```

Reuses `EligibilityChecker.vrfOutputAsRatio` and the `EcVrf25519` instance. Threshold is multiplicative (`K · σ_i`) rather than LDD-snowplow — no `Log1p` / `Exp` needed. Self-contained module + property tests + golden-vector determinism test land first, no consensus wiring.

### Integration point — and the architectural choice it forces

In the current codebase metagraph snapshots flow through gl0 as `StateChannelOutput` values; `NakamotoSyncDaemon.processMetagraphBinary: StateChannelOutput => F[Unit]` is the chokepoint. The global snapshot then records the binary's hash, and **every** gl0 operator attests to the global snapshot — there is no per-metagraph attestation today. So the design choice is *what we layer committee sortition on*:

| Option | What changes | Cost |
|---|---|---|
| **A — Pre-inclusion gate** | Only committee-attested SC binaries are eligible for inclusion in the next global snapshot. Non-committee operators wait for committee attestations to arrive before forwarding the binary into `processMetagraphBinary`. | New per-metagraph attestation gossip round; new aggregator that collects ≥ 2K/3 committee attestations before admitting the binary. |
| **B — Per-metagraph attestation split** | Decompose the global-snapshot attestation into a vector of per-metagraph attestations, each signed by that metagraph's committee. Global-snapshot finality requires per-metagraph quorum on each included binary. | Largest refactor: changes the attestation schema (`pb.TipAttestation`), the finality computation, and the global-snapshot proof structure. |
| **C — Parallel committee commitment** | Keep the existing global-snapshot attestation untouched. Add a parallel "committee attestation" envelope, KES-signed, that lets a light client verify "≥ 2K/3 of the committee attested this binary." Used by light clients and for slashing detection; doesn't change consensus. | Smallest blast radius — no consensus refactor, just a new attestation type. But doesn't actually shard the gl0 work — every gl0 operator still processes every binary. |

**Recommendation: A.** B is the cleanest end-state but the refactor cost is large and the upside doesn't compound until the operator set is big enough to need real sharding. C gives slashing infrastructure without sharding benefits. A delivers sharding (non-committee operators skip the binary) with bounded refactor surface. This expanded scope (Option A) is what subsequent impl slices (S1–S4 in §9) target.

### Slice-by-slice (Option A)

1. **S1 — `CommitteeSortition[F]` + property tests + golden vectors.** Pure module; no consensus wiring. Confirms VRF + threshold determinism + Chernoff sim against `K_target = 100` honest-majority bound.
2. **S2 — Protobuf bump.** New `pb.MetagraphAttestation { peer_id, metagraph_id, snapshot_ord, committee_vrf_proof, kes_signature }`. KES-signed end-to-end (reuses Slice 9 verify path). Aggregator stub gossips + collects but doesn't gate yet (warn-only).
3. **S3 — Pre-inclusion gate (load-bearing).** `processMetagraphBinary` waits for ≥ 2K/3 committee attestations before admitting the binary. Backed by a per-binary `Ref[F, Set[PeerId]]` with timeout.
4. **S4 — Slashing detection (Option C).** Two contradictory `MetagraphAttestation`s from the same committee key → emit a `SlashableEvidence` tx. Stake-burn logic is a separate follow-up doc.
5. **S5 — e2e validation.** K_target=8 on 8 gl0 cluster (degenerate — every operator always in every committee, sortition acts as pass-through). Validates wiring. Security validation needs ≥ 50 operators per metagraph; deferred to scale-test infra.

### What we DON'T touch

- L0 leader VRF — stays as-is (`EligibilityChecker`).
- KES — every operator still has KES; committee members use it to sign their `MetagraphAttestation` the same way they sign global-snapshot attestations today.
- Snapshot signing — unchanged; the metagraph operator (ml0) signs its own snapshot; the gl0 committee just gates inclusion.
- Slashing stake-burn — Option C detection lands in S4 but the burn-side ledger logic is a separate doc.

---

## 8. Open questions / out of scope

1. **N-2 stake snapshot pipeline.** This doc assumes `sigmaOperatorKey` is fetched against an N-2 frozen StakeRegistry. That snapshot pipeline is task #180 (NIPoPoW S0). Until it lands, an interim implementation can use live stake — broken under adaptive corruption but correct in shape, so the design migration is just swapping the StakeRegistry query.
2. **Slashing (Option C, follow-up).** A committee member that emits two contradictory attestations on competing snapshots is detectable from the two KES signatures. Slashing logic + stake-burn path is a separate design doc.
3. **Mid-life joiners (KES Slice 10, #179).** Operators registered mid-life sign with a KES offset. Committee sortition uses VRF + stake fraction, both of which are independent of KES offset, so no interaction. Receiver MUST still verify the operator was in the N-2 active set at this epoch — a brand-new joiner is excluded from the committee until N-2 epochs after their registration finalizes.
4. **K_target governance.** Fixed for v1. v2 would let each metagraph pick K, with L0-enforced `K ≥ K_floor`. Out of scope here.
5. **Algorand committee-vs-block-proposer split.** Algorand also uses VRF sortition to elect the proposer separately. We already have a leader VRF for that role (`EligibilityChecker`), so no new design needed.
6. **Per-snapshot vs per-epoch committee.** Per-snapshot in this doc. Per-epoch (one committee per K_committee_epoch eta periods) would reduce churn but increase adaptive-corruption risk. Defer.

---

## 9. Sequencing

1. **This doc** — committee sortition primitive.
2. **Slashing stake-burn design doc** — separate; detection lands in S4 of impl, but the ledger logic for stake reduction + slashable-window definition needs its own writeup.
3. **N-2 StakeRegistry pipeline** — #180 (NIPoPoW S0). Required for adaptive-corruption defense.
4. **Impl S1 — `CommitteeSortition[F]` primitive + tests.**
5. **Impl S2 — `pb.MetagraphAttestation` + gossip + aggregator stub (warn-only).**
6. **Impl S3 — pre-inclusion gate (load-bearing flip).**
7. **Impl S4 — slashing detection on conflicting committee attestations.**
8. **Impl S5 — e2e validation** at degenerate K=N. Larger-N security validation deferred to scale-test infra.

S2 must land warn-only before S3 flips load-bearing — same staging pattern that worked for KES Slice 5→9. Skipping the warn-only middle would break liveness the moment the pre-inclusion gate goes live because no operator has historic committee attestations to forward yet.

---

## References

- `project_cross_shard_cq_collapse_bound` — Option A (VRF-sortition of operator keys) + Option C (slashing).
- `project_consensus_epoch_staggering` — N-2 staggering rule, per-operator-key sortition unit, stake-fraction denominator.
- `project_sharding_strategic_signals` — two-tier stake model (delegated + node collateral) feeding into stake fractions.
- `EligibilityChecker.scala` — existing VRF + LDD-threshold for L0 leader election; the new committee VRF reuses `vrfOutputAsRatio` + `EcVrf25519`.
- `NIPOPOW-IMPLEMENTATION-PLAN.md` — N-2 StakeRegistry pipeline (Slice S0).
- Algorand Agreement (Gilad et al. 2017) — the per-operator-key VRF-threshold sortition primitive.
