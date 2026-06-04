# Attestation Timeliness Incentive Design — v2 (FUTURE)

**Status:** DESIGN ONLY — v2 / future work. No code exists. This is separate from committee sizing
(`COMMITTEE-SORTITION-DESIGN.md`) and the current participation-set refactor
(`EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md`). Do not implement until the epoch-anchored
participating set (§3–§4 of that doc) is e2e-validated.

**Date:** 2026-06-04

---

## 0. TL;DR

Use committee attestation timeliness as a validator-health signal that feeds **rewards** (always
positive, no slash) and, when the chain is genuinely failing to finalize, a **graduated inactivity
penalty** (bounded, recoverable). Slashing stays strictly for cryptographically-verifiable
equivocation — never for mere non-participation.

---

## 1. Motivation

Today a committee member that never sends a `MetagraphAttestation` for a binary pays no cost:

- It shrinks the **effective committee** below `K_target`, raising the slots-to-threshold latency
  for every binary that member was drawn for.
- It contributes nothing to liveness yet continues to be eligible for the next committee draw.
- The existing non-participation accumulator (`ShardNonParticipationCounter`, fieldId 24,
  `evaluateEpochBoundary`) was built and tested but has **zero production callers** — it measures
  nothing at runtime (see `EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md §2`).

Two goals:

(a) **Measure health from attestation behavior.** Record, per committee member per binary, when
their first valid attestation arrived (inclusion distance). Aggregate these measurements per epoch
into a promptness ratio that lives on-chain as part of the epoch participating set.

(b) **Align incentives.** Reward showing up promptly. Apply a graduated, recoverable inactivity
penalty only when the chain is provably failing to finalize. Never punish an honest-but-slow or
temporarily partitioned node beyond what the inactivity leak already calibrates.

This is modeled on **Ethereum 2**:
- Eth2 attestation-inclusion-distance rewards: validators earn proportionally more when their
  attestation is included in the next slot than in a later slot.
- Eth2 inactivity leak: a non-finalizing chain slowly burns offline validators' balances at a rate
  that accelerates the longer finality is stalled, stops the moment finality resumes.

Adapted here to Tessellation's committee gate model and snapshot cadence.

---

## 2. The Timeliness Signal

### 2.1 What the aggregator records today

`MetagraphAttestationAggregator` (node-shared) stores, per
`(metagraphAddress, parentHash, binaryHash)`, the **set of distinct peers** that have sent a valid
attestation (`record` is called once per peer after the triple verify — Ed25519 + KES + committee
VRF — in `MetagraphCommitteeGate.recordReceivedAttestation`). The gate polls
`thresholdReached(kTarget)` until `⌈2 K / 3⌉` attestations arrive.

What is **not** recorded today:

- The **slot** (or wall-clock time) at which each peer's attestation arrived.
- The **ordinal** of the global snapshot in which the binary was admitted.
- A per-peer per-binary **inclusion distance**.

### 2.2 Proposed extension — record arrival slot

Extend `MetagraphAttestationAggregator.record` to also accept the slot at which the attestation
arrives (read from `SlotClock[F].currentSlot` at the call site in the gate, which already has
`Clock[F]` in scope). Store:

```
Map[(Address, Hash, Hash), Map[PeerId, Slot]]
     (mg,     parent, binary)   peer  arrival-slot
```

The **inclusion distance** for peer `p` on binary `b` with committee open at slot `s_open` is:

```
d(p, b) = arrival_slot(p, b) - s_open
```

`s_open` is the slot at which the binary first entered the gate (i.e., the slot at which
`attestAndAdmit` is called). This value is recorded once per binary when the gate opens.

Inclusion distance = 0 means the attestation arrived in the same slot as the binary. Distance = 1
means the next slot. Values beyond `D_max` (a config param, suggested default: `K_target` slots,
roughly the expected latency to reach quorum) are capped at `D_max` for reward-weight purposes.

### 2.3 Promptness tiers

Three tiers, config-tunable:

| Tier | Condition | Reward weight |
|---|---|---|
| **Prompt** | `d ≤ D_prompt` (suggested: 2 slots) | `w_prompt` (e.g. 1.0) |
| **Late** | `D_prompt < d ≤ D_max` | `w_late` (e.g. 0.5) |
| **Absent** | no attestation before binary finalized | `w_absent` = 0.0 |

A committee member that is drawn but absent (no valid attestation within the gate timeout) receives
weight 0 — they get no committee reward for that binary. They do NOT get a negative reward (no
penalty here; that lives in §4 and only triggers under non-finality).

### 2.4 Aggregation across a period

At the eta-period boundary, the existing
`GlobalSnapshotAcceptanceManager.computeHistoricalStakeBoundaryDelta` hook writes
`HistoricalStakeSnapshot`. Extend this (or the parallel fieldId-25 slot described in
`EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md §3.3`) to also write, per peer, a
**promptness ratio** for the closed period:

```
promptnessRatio(p, period) =
  sum(w_tier(d(p, b)) for all binaries b in period where p was in committee)
  /
  count(binaries in period where p was in committee)
```

Integer-arithmetic implementation: multiply weights by a common denominator (e.g. 1000) so all
arithmetic stays in `Long`. Config-pinned, deterministic across all nodes at the boundary.

This promptness ratio per peer per period is the **timeliness signal** that feeds §3 (rewards) and
§5 (participating set).

---

## 3. Reward Weighting

### 3.1 Committee reward share today

The gl0 reward path calls `Rewards[F].distribute` (via `rewardsService.classicRewards.distribute`
or `DelegatedRewardsDistributor.distribute`) at each snapshot, receiving a `SortedSet[RewardTransaction]`.
Current "facilitators" are derived from the consensus-round participant set — not the committee gate.
There is no committee-tier reward.

### 3.2 Proposed integration point

The natural seam is the **delegated-stake reward distribution** in
`GlobalDelegatedRewardsDistributor` (dag-l0). It already:
- Iterates `activeDelegatedStakes` per peer.
- Reads per-peer stake fractions.
- Emits per-peer `RewardTransaction` lines.

Add a `promptnessRatio(peerId, closedPeriod)` lookup (backed by `StakeRegistry` / the new
promptness MPT field, mirroring `participationRatioAt`) and multiply the peer's base committee
reward share by their timeliness weight:

```
committeeReward(p) = baseCommitteeReward(p) × promptnessRatio(p, period)
```

The total committee reward pool is **not** reduced — the unearned portion (from absent members) is
either redistributed proportionally to prompt members or retained in the protocol reserve (decision
deferred; redistribution aligns incentives better but adds complexity).

**This is always non-negative.** No peer's reward can go below zero from timeliness alone.

### 3.3 Timing

Committee rewards are emitted at the **eta-period boundary** snapshot, not per-binary. This
amortizes the reward computation over the full period and aligns it with the promptness-ratio
aggregation boundary. It also means committee rewards are deterministic at verify time (all nodes
have the same promptness MPT state at the boundary ordinal).

---

## 4. Graduated Inactivity Penalty

### 4.1 The non-finality trigger — when the leak activates

The inactivity penalty is **only active when the chain is genuinely failing to finalize.** It does
NOT apply during normal operation.

Detection: `FinalityTrigger` (node-shared) is the existing finality observable. Non-finality is
detectable as: the gap between the current slot and the last ordinal qualified by ANY Phase 1→2
trigger (`T_weight`, `T_count`, `T_depth1`) exceeds a threshold `N_nonfinal` (suggested: 2 ×
`depth_k1`, so about 500 slots / ~8 minutes at default k₁=255). Concretely:

```
isNonFinalizing = currentSlot - slotOf(latestQualifiedOrdinal) > N_nonfinal
```

This observable already exists in spirit via `FinalityTrigger.latestQualifyingOrdinal`; a small
`FinalityHealthMonitor` service reading from the trigger and the slot clock exposes it.

### 4.2 Graduation — the leak rate

When `isNonFinalizing` is true, peers that have NOT sent a valid attestation for the most recent
`M_leak` consecutive slots (a config window; suggested default: 60 slots / 1 minute) accumulate
a **leak**:

```
leakRate(p) = baseStake(p) × leakFractionPerEpoch × inactiveEpochs(p)
```

Where:
- `leakFractionPerEpoch` is a config param (suggested: 0.001 = 0.1% per epoch). Small enough
  that an honest but partitioned node is not wiped out before the partition heals.
- `inactiveEpochs(p)` counts consecutive epochs without a valid attestation since non-finality
  started. It grows as the node stays offline, making the leak accelerate — exactly the Eth2
  inactivity leak behavior, where the chain self-heals when honest-majority are left after the
  offline nodes' stake decays below 2/3.
- The leak is **capped** at `maxLeakFraction` (suggested: 0.5 = 50% of stake) to prevent total
  destruction for nodes that are simply partitioned, not adversarial.

### 4.3 Recovery — leak stops immediately on resume

When `isNonFinalizing` becomes false (finality resumes), ALL active leaks stop. Similarly, a peer
that resumes sending valid attestations within the current `M_leak` window has their
`inactiveEpochs` counter reset to zero. The leaked stake is NOT recovered (it is consumed), but no
further leak accumulates.

This asymmetry (leak is permanent but stops on recovery) is intentional: it preserves honest nodes'
stake in the long run while still providing the mathematical property that a sufficiently long
non-finality event drains enough absent stake for the remaining honest majority to achieve finality
on its own.

### 4.4 Ledger effect

The inactivity leak applies the same stake-reduction effects as slashing
(`SLASHING-DESIGN.md §5`) but via a separate code path with a separate MPT key
`inactivityLeaks/<peer_id>/<period>`. It is NOT recorded as a `SlashableEvidence` transaction — it
is computed deterministically at the eta-period boundary alongside the promptness ratio, emitted as
an `AcceptanceMptStateChanges` delta. All honest nodes compute the same delta at the same boundary
ordinal (deterministic, reproducible offline, same safety bar as the participating set writes in
`EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md §3.5`).

---

## 5. Hard Non-Goal: Never Slash on Non-Participation

**This is the most important constraint. It is stated here unambiguously.**

Non-participation in committee attestation is a **liveness concern**, not a **safety violation**.
An honest node that is partitioned, overloaded, or catching up after a crash will miss attestation
windows through no fault that rises to the level of Byzantine behavior. Slashing that node would:

1. Punish honest nodes (safety footgun, Eth2 lesson learned the hard way).
2. Give adversaries a griefing vector: flood a target with traffic until it misses attestations,
   then slash it.

**The three-tier consequence model:**

| Behavior | Consequence | Mechanism |
|---|---|---|
| Present and prompt | Reward multiplier ≥ 1.0 | §3 promptness weight |
| Present but late | Reduced reward multiplier (0.5 ×) | §3 promptness tier |
| Absent (binary level) | Zero committee reward for that binary | §3 weight = 0 |
| Chronically absent, chain finalizing normally | Demotion from participating set (exclusion from committee eligibility in future epochs) | `EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md §3.5` |
| Chronically absent, chain NOT finalizing | Graduated inactivity penalty (bounded stake reduction) | §4 inactivity leak |
| Equivocation (two contradictory KES-signed attestations, same parent) | SLASH (100% stake, cooldown, bounty) | `SLASHING-DESIGN.md §4-§5` |

The first four rows are handled here. The last row is explicitly out of scope — it belongs to
`SLASHING-DESIGN.md` and depends on cryptographically verifiable `SlashableEvidence`.

A future reader seeing a non-participation event should resist any temptation to add a `SlashableEvidence`
path for it. Slashing requires a signature over two contradictory statements by the same key — mere
silence is not that.

---

## 6. Tie to #22: The Participating Set

The promptness ratio computed in §2.4 is the natural input to the participating-set demotion logic
in `EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md §3.5`. The relationship:

- `ShardNonParticipationSlasher.evaluateEpochBoundary` (generalized to gl0 scope) decides whether
  a peer's period-level binary-absence rate exceeds `ShardSlashingConfig.maxMissedPctPerEpoch`.
  This is the coarse binary-present/absent signal.
- The timeliness promptness ratio adds a **granularity layer** on top: a peer may be "present" (not
  demoted) yet consistently late, earning reduced rewards without reaching the demotion threshold.

Implementation sequencing (dependency order):

1. `EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md` slices 1–4 land first (participating set,
   demotion, re-promotion). This is load-bearing for correct finality weight.
2. Timeliness inclusion-distance tracking (§2) lands as an extension — additive to the boundary
   aggregation, no behavior change to gate logic.
3. Reward weighting (§3) integrates into `GlobalDelegatedRewardsDistributor` — changes the
   reward split but not consensus.
4. Inactivity leak (§4) is the highest-risk change (stake mutation); lands last with its own
   e2e validation phase.

Until step 4 is validated at target topology, the inactivity leak MUST be OFF by default
(`leakFractionPerEpoch = 0` in HOCON config).

---

## 7. Threat Model — Adversarial Gaming

### 7.1 Selective promptness gaming ("attest cheaply when convenient")

An adversary could try to maximize promptness rewards by only attesting when it is computationally
cheap — e.g., only for binaries whose parent hash is already known (avoiding the cost of verifying
an unknown binary). This is bounded by the existing gate protocol:

- The gate only records a valid attestation after the triple verify (Ed25519 + KES + committee
  VRF). An adversary cannot fabricate a valid attestation cheaply without the correct keys.
- Prompt attestation requires receiving the binary AND the eta quickly, which requires being a
  well-connected, in-sync participant. A node that selectively attests will still miss windows
  for binaries it doesn't receive in time, earning the `w_absent` weight for those — net reward
  will be lower than a consistently prompt node.
- The promptness tier boundary (`D_prompt`) can be tuned conservatively (e.g., 2 slots) so that
  only nodes with low latency and genuine participation benefit from the top tier.

### 7.2 Attestation withholding (strategic timing)

An adversary in the committee could delay releasing their attestation to force a specific quorum
outcome — e.g., release at exactly slot `D_prompt + 1` to be in the "late" tier rather than
disqualified, while allowing a competing binary to fail threshold first.

Mitigation:
- Committee VRF proofs are published alongside attestations. A node that holds a valid proof
  and releases it late cannot also claim not to have been in the committee — the proof commits
  to the binary hash.
- The gate timeout is fixed; a strategically late attestation that arrives after timeout is
  dropped regardless of tier — it doesn't count toward threshold at all.
- Grinding the promptness tier (trying to land in D_prompt + 1 vs D_prompt) yields negligible
  stake impact (0.5× vs 1.0× reward share on a small committee). The cost of being identified
  as consistently late (approaching the demotion threshold) dominates.

### 7.3 Non-finality leak grinding

An adversary could try to trigger the inactivity leak against competing nodes by strategically
forcing non-finality (withholding attestations from the quorum). But:

- The adversary controls at most `f_adv ≤ 1/3` of stake. Withholding enough attestations to
  prevent quorum (`> 1/3` of committee) requires controlling more than `1/3` of the committee
  draw — which is bounded by `f_adv` in expectation.
- If the adversary does prevent finality, THEY also accumulate inactivity leak (they are
  absent from the quorum they are preventing). The leak is symmetric.
- The leak cap (`maxLeakFraction = 50%`) means even a long non-finality event doesn't
  completely destroy the stake of a well-connected honest node.

### 7.4 Summary

The threat model is substantially more concerning at the slashing level (equivocation) than at the
timeliness/inactivity level. The reward weighting is a soft incentive, not a security primitive.
The inactivity leak is a liveness recovery mechanism borrowed from Eth2, where it has been
production-validated. Neither creates a new slashable surface.

---

## 8. Key References

**Existing code (do not modify until implementation begins):**
- `MetagraphAttestationAggregator.scala` — current tally state; `record` is the extension point
  for arrival-slot tracking.
- `MetagraphCommitteeGate.scala` — `recordReceivedAttestation` is the call site where
  `SlotClock[F].currentSlot` should be sampled and passed to `record`.
- `FinalityTrigger.scala` — `latestQualifyingOrdinal` per trigger; non-finality detection reads
  the max across `TWeight`, `TCount`, `TDepth1`.
- `SlotClock.scala` — `currentSlot` at 1Hz; the timing source for inclusion distance.
- `GlobalSnapshotAcceptanceManager.scala:954` — eta-period boundary hook; the integration point
  for promptness-ratio aggregation and inactivity-leak application.
- `GlobalDelegatedRewardsDistributor.scala` — reward emission; integration point for §3.
- `StakeRegistry.scala:338/:482` — `relativeStakeAt` pattern; replicate for
  `promptnessRatioAt(peerId, period)`.

**Design docs to align with:**
- `EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md` — load-bearing prerequisite; participating set,
  Slice-17 counters, `HistoricalStakeSnapshot` extension pattern.
- `SLASHING-DESIGN.md` — canonical equivocation slash path; the **only** path that results in a
  slash. This doc does NOT add to it.
- `COMMITTEE-SORTITION-DESIGN.md §8` — committee safety argument; slashing + N-2 staging
  together close the adaptive-corruption window.
- `attestation-and-finality.md` — finality model, trigger definitions, Phase 1→2 mechanics.

**External references:**
- Ethereum 2 attestation-inclusion-distance rewards (Beacon Chain spec `get_attestation_deltas`).
- Ethereum 2 inactivity leak (`process_inactivity_updates`, `INACTIVITY_SCORE_BIAS`,
  `INACTIVITY_PENALTY_QUOTIENT`).
- Cardano: has no inactivity leak or attestation timeliness reward; less applicable.
