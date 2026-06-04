# Committee Partition-Adaptivity via Participation-Set Rotation

**Status:** DESIGN NOTE (documentation only). Date: 2026-06-04.
**Thesis:** the partition-adaptivity that a "dynamic LDD-style committee expansion" would supply
is delivered, essentially for free, by the **participation-set rotation** work tracked as task #22.
No timer, no extra wire state, no per-round VRF dimension needed.

---

## 1. How committee membership works today

`CommitteeSortition.scala` implements Algorand-style per-operator VRF-threshold sortition. The
membership predicate is:

```
VRF(eta, metagraphAddress, parentHash)  <  threshold(kTarget, σ)
```

where `threshold` is defined at `CommitteeSortition.scala:139`:

```scala
def threshold(kTarget: Int, sigmaOperatorKey: Ratio): Ratio = {
  require(kTarget > 0, s"K_target must be positive, got $kTarget")
  val raw = Ratio(kTarget) * sigmaOperatorKey
  if (raw >= Ratio.One) Ratio.One else raw
}
```

`σ` (sigma, `sigmaOperatorKey`) is the per-operator stake fraction supplied by the call site.
Today that fraction is `committeeStake`, defined at `StakeRegistry.scala:66` and implemented at
`StakeRegistry.scala:172-176` (equalWeight path) and `:326-330` (stakeWeightedMpt path) as:

```scala
def committeeStake(peerId: PeerId): F[Ratio] =
  validatorsRef.get.map { validators =>
    if (validators.contains(peerId) && validators.nonEmpty) Ratio(1, validators.size)
    else Ratio.Zero
  }
```

The denominator is `|validatorsRef|` — the **full registered seedlist** — regardless of how many
validators are currently online. A node not in the seedlist gets `Ratio.Zero` and is never
admitted to any committee.

The `validatorsRef` is populated from the seedlist once at boot
(`GlobalSnapshotConsensus.scala:793`); it only grows or shrinks through explicit
`updateValidators` calls (currently boot-only). It does **not** track liveness.

Concretely: with 8 registered validators, σ = 1/8 for every peer, and
`threshold = kTarget * (1/8)`. With `kTarget = 200` that threshold is 25, meaning each operator
has an independent 25-in-256 chance of landing in the committee per binary.

---

## 2. The adaptivity argument: participating set makes σ self-correcting

`EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md §3.1` defines the participating set as a
proper subset of the validator set, computed at each eta boundary from participation counters
and written to the MPT under `HistoricalStakeSnapshot`. The key insight:

**Replace `|validators|` with `|participating|` in the `committeeStake` denominator.**

That is, after #22 lands, `committeeStake` reads:

```
σ_i = 1 / |participating_set_at(N-2 epoch boundary)|
```

instead of:

```
σ_i = 1 / |validatorsRef|  (full seedlist, boot-only, static)
```

### Worked example

Suppose 8 registered validators, 3 of which are partitioned / offline at the time the N-2
participating set was computed:

| State | Denominator | σ per live operator | threshold at kTarget=40 |
|---|---|---|---|
| Today (full seedlist) | 8 | 1/8 = 0.125 | 40 × 0.125 = **5.0** |
| After #22 (live 5) | 5 | 1/5 = 0.200 | 40 × 0.200 = **8.0** |

With kTarget = 40 and 5 live validators:

- Today's threshold is 5.0 out of [0, 256): expected committee = 40 × 5 (live) × 0.125 = **25** members — undershoots kTarget because 3 offline nodes are included in the denominator but contribute zero VRF proofs.
- After #22: threshold = 8.0; expected committee = 40 × 5 (live) × 0.200 = **40** — matches kTarget exactly, drawn entirely from the live set.

The self-rebalancing is algebraic and instantaneous. No timer fires, no round counter advances,
no extra gossip occurs. The cluster reconverges on the correct committee size simply by having
computed `σ` against the right denominator at the prior epoch boundary.

### Why this is safe (liveness and safety)

- **Liveness**: the committee draw covers the full live set. Every live validator has a positive
  chance of being selected, and the expected committee size remains `kTarget` regardless of how
  many validators are offline.
- **Safety (honest-majority)**: the Chernoff bound from `COMMITTEE-SORTITION-DESIGN.md §6`
  holds against the participating set. Offline validators that were excluded from the
  participating set contributed no VRF outputs and no attestations — they are absent from both
  numerator and denominator, so they neither help nor hurt the honest-majority guarantee.
- **Determinism (hard requirement)**: the participating set is epoch-anchored, computed
  identically on every node at the N-2 boundary from integer-arithmetic participation counters
  (see `EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md §3.3/§3.6`). The `committeeStake` read uses
  `participatingSetAt(period)` — the same MPT-backed, reproducible read as `relativeStakeAt`.
  Every node derives byte-identical σ values at the draw point, so sender and receiver agree on
  `threshold` without coordination — the core requirement that broke when `optimisticRelativeStake`
  drifted (#216).

---

## 3. Why the rejected "dynamic committee expansion" is worse

A dynamic committee expansion would enlarge the committee when attestations are slow to arrive —
e.g., after `T_wait` elapses with fewer than `kQuorum` attestations, expand the draw to include
more operators.

Three concrete costs, each paid at the worst possible time (partition or slow path):

1. **Expansion-WAIT latency on the critical path.** The expansion trigger fires only after
   `T_wait` of silence — that is, the stalled binaries wait the full timer before the expanded
   committee can even start forming. A static participation-set draw has no such cliff: if live
   operators are in the draw they attest immediately; no latency added.

2. **Round dimension on the committee VRF input.** For a verifier to reconstruct the threshold at
   round `r`, both `kTarget_r` (the expanded draw size) and the expansion eligibility rule must
   be deterministic in `r`. That forces every attestation to carry a `round` field, every
   verifier to re-derive `threshold(r)` using the expansion schedule, and the aggregator to
   track (binary, round) pairs. Contrast: the participation-set path adds no new wire field — σ
   is derived from the existing epoch-anchored participating-set read, which is already in the
   system for finality.

3. **Gossip saving is marginal.** The only upside of expansion — drawing from a larger pool
   during a partition — is that σ rises. But the participation-set path delivers the same σ
   increase without expansion by simply shrinking the denominator. The gossip volume is
   determined by how many attestations reach `kQuorum`, not by the draw size; a well-sized
   static draw against the live set produces the same traffic as expansion but without the timer.

In short: dynamic expansion gets the robustness after a delay and at the cost of protocol
complexity; the participation set gets the same robustness statically and for free.

---

## 4. Complementarity with the kDraw / kQuorum decoupling

The kDraw/kQuorum separation (the static throughput fix landing now) distinguishes:

- **kDraw** (`kTarget` in code) — the target number of operators drawn into the committee; sets
  the expected committee size and determines σ via `threshold = kDraw × σ`.
- **kQuorum** — the minimum attestation count required for a binary to be admitted; currently
  `ceil(2/3 × kTarget)` via `MetagraphAttestationAggregator.requiredCount` (`:83`).
- **participating set** (`#22`) — the population from which the draw is made; determines σ via
  `σ = 1 / |participating|`.

These three are orthogonal and compose without interference:

| Knob | Controls | Code surface |
|---|---|---|
| kDraw | expected committee size; admission wait time | `CommitteeSortition.threshold`, `MetagraphCommitteeGate.kTarget` |
| kQuorum | safety / throughput trade-off | `MetagraphAttestationAggregator.requiredCount` |
| participating set | adaptivity under partition; who is eligible | `StakeRegistry.committeeStake`, fed from `participatingSetAt` |

Raising kDraw increases the committee (and σ per-operator with a fixed participating set);
raising kQuorum requires more signatures before admission; shrinking the participating set raises σ
per live operator. None of these changes propagates to the other two surfaces — they read disjoint
state and compose at call sites only through the `threshold` expression.

---

## 5. Open questions and wiring for #22

### 5.1 How the participating set is determined

Per `EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md §3.2–3.3`:

- **Within-epoch measurement**: per-`(peerId, epoch)` participation counters (Slice 17,
  `ShardNonParticipationStateManager`) are wired from the election path
  (`recordSlotEligibility`) and the attestation path (the site currently calling the ad-hoc
  `markActive` at `TipTracker:197`).
- **Boundary computation**: at the eta boundary (`GlobalSnapshotAcceptanceManager.scala:954`,
  predicate `ord % R == R-1`) the participating set is computed from those counters via a
  generalized `ShardNonParticipationSlasher.evaluateEpochBoundary` predicate and folded into
  `HistoricalStakeSnapshot` (or a new `GlobalStateFieldId` entry, fieldId 25).
- **Retention**: three-period retention, same as the existing historical stake snapshots, so
  the N-2 lookback window is always populated.

### 5.2 How the participating set feeds `committeeStake`

Today `committeeStake` reads `validatorsRef.size` (static, boot-sourced). After #22, it should
read `participatingSetAt(currentEpoch - 2)` via `StakeRegistry.participatingSetAt` (the
new deterministic read added in `EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md §3.4`). The
implementation is a registry-side change: `committeeStake` calls `historicalStakeReader.lookup`
for the N-2 period and returns `Ratio(1, |participating_set|)` if the peer is a member, else
`Ratio.Zero`. Peers demoted from the participating set get `Ratio.Zero` — they are ineligible
for committee draw in that epoch.

### 5.3 Interaction with the eta / epoch boundary

The participation set is epoch-anchored to the **N-2 boundary** (matching the existing Cardano
mark/set/go pipeline for stake snapshots). The committee VRF input already carries `eta`, which
is computed from the 2/3-mark of N-1 (per `consensus-epoch-staggering`). Aligning the
participating-set read to N-2 means:

- Epoch N draws from the participating set finalized at boundary of N-2.
- The participating set for N-2 was computed from within-epoch counters for N-2, finalized at
  that boundary.
- A newly partitioned validator that misses most of epoch N-2 will be excluded from N's
  participating set — but inclusion in N-1 and N (before the next boundary) is unaffected.

The N-2 lag provides a two-epoch grace window: a transient partition that clears within one
epoch leaves the participating set unchanged for the epoch after it clears.

### 5.4 Consensus-determinism requirement

**The participating set must be cluster-identical at the draw point.** This is the same
hard requirement that broke `optimisticRelativeStake` (#216): if any node computes a different
`σ` at the draw point, sender and receiver disagree on `threshold(kTarget, σ)`, and the
committee VRF verify fails at the receiver.

The participating-set path satisfies this requirement by construction:
- It is computed at a deterministic ordinal (the eta-boundary predicate).
- All arithmetic is exact integer (participation counters, threshold config).
- It is written to the MPT and accessible via the same `historicalStakeReader.lookup` that
  `relativeStakeAt` uses — which is already tested for GSI↔MPT parity.

The ad-hoc `markActive` / `observedActive` path does **not** satisfy this requirement (it is
per-node, runtime, non-reproducible — see `EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md §1`).
The migration to `participatingSetAt` for `committeeStake` is therefore also a correctness fix,
not just an optimization.

---

## 6. Summary

| Property | Today (full seedlist σ) | After #22 (participating-set σ) |
|---|---|---|
| σ under partition (3/8 offline) | 1/8 (fixed; offline nodes dilute) | 1/5 (auto-corrects to live set) |
| Expected committee size | kDraw × 5 × (1/8) = 0.625 × kDraw | kDraw × 5 × (1/5) = kDraw |
| Wire overhead vs dynamic expansion | — | None (no round field, no timer) |
| Determinism | ❌ (static seedlist, correct; but tied to boot-only update) | ✅ (N-2 boundary, MPT-backed, parity-tested) |
| Interaction with kDraw/kQuorum split | orthogonal | orthogonal |

**Conclusion.** The participation set replaces the static `|validatorsRef|` denominator in
`committeeStake` with a deterministic, epoch-anchored, liveness-aware denominator. This delivers
automatic committee rebalancing under partition — the core value proposition of dynamic committee
expansion — at zero additional latency, zero additional wire state, and with stronger determinism
guarantees than the existing `optimisticRelativeStake` path.

---

## References

- `StakeRegistry.scala:66` — `committeeStake` docstring and signature
- `StakeRegistry.scala:172` / `:326` — `committeeStake` impls (equalWeight / stakeWeightedMpt); denominator is `validators.size`
- `CommitteeSortition.scala:139` — `threshold(kTarget, sigmaOperatorKey): Ratio`
- `CommitteeSortition.scala:220–239` — `make[F]` impl: `isInCommittee` and `verifyMembershipDetailed`
- `MetagraphAttestationAggregator.scala:83` — `requiredCount(kTarget)`: `ceil(2/3 × kTarget)`
- `EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md` — full participating-set + demotion design (task #22)
- `COMMITTEE-SORTITION-DESIGN.md §6` — Chernoff honest-majority bound derivation
- `COMMITTEE-SORTITION-DESIGN.md §4.5` — stake-fraction denominator rationale
- `project_216_committee_stake_drift_fix` (memory) — the #216 fix that proved live-set drift breaks the VRF threshold agree
