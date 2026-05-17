# §3 NIPoPoW — implementation slice plan

**Status**: SCHEDULED 2026-05-17 (post §1.2 KES Slice 9 land at `ddf183adc`).
**Companion to**: [NIPOPOW-PROPOSAL.md](./NIPOPOW-PROPOSAL.md) §1–§7 (design + phase sizing). This file slices the design into ordered, dependency-aware chunks with prereq gates and e2e milestones.

Estimate from the proposal §6.6: **~2,400 LOC, 6–10 engineer-weeks** in-node (Phases A–D). Add ~1–2 weeks for the N-2 staggering prereq (S0). Total: **8–12 weeks** to v1.

---

## Prereqs (must land first)

- ✅ §1.1 Stake-weighted VRF (`b0e3` series — combined delegated + collateral; validated 2026-05-16)
- ✅ §1.2 KES live wiring through Slice 9 (load-bearing flip @ `ddf183adc`); Slice 10 (#179 runtime registration) **can land in parallel** with NIPoPoW Slice S1+ — neither blocks the other since NIPoPoW's KES dependency is per-snapshot-sig verification, not registration mid-life
- ✅ Finality trigger stack (`b65` series — T_count, T_depth2, chain-quality)

---

## Slice S0 — N-2 epoch staggering for StakeRegistry (~1.5 weeks, prereq for ALL of NIPoPoW)

**Why first**: NIPoPoW level-µ trial density at level µ depends on the *signer-set-active-at-eta-boundary*, not "whoever happens to be in the GSI right now". Today `StakeRegistry.relativeStake` reads the latest GSI (see `modules/node-shared/.../nakamoto/StakeRegistry.scala:155-156` doc). NIPoPoW's verifier needs the same view as the producer; without N-2 staggering, the verifier can't deterministically reconstruct stake-at-snapshot.

**Tasks**:

| Slice | Component | LOC | File |
|---|---|---|---|
| S0.1 | `EpochStakeSnapshotter` — captures stake distribution per eta-period boundary | ~150 | new `modules/node-shared/.../nakamoto/EpochStakeSnapshotter.scala` |
| S0.2 | Extend `StakeRegistry` with `relativeStakeAt(peerId, etaPeriod)` | ~100 | edit existing |
| S0.3 | `GlobalSnapshotInfo` — add `historicalStakeSnapshots: Map[EtaPeriod, StakeDistribution]` (capped at N-2 retention) | ~80 | schema + MPT keying |
| S0.4 | Wire `EligibilityChecker` to read `relativeStakeAt(_, currentEtaPeriod - 2)` instead of latest | ~50 | edit |
| S0.5 | Backfill at genesis: stamp the genesis stake distribution as N-2/N-1/N for the first 3 periods | ~60 | edit `L0GenesisLoader` |
| S0.6 | Unit tests + property test (stake change at ord X is invisible to eligibility until ord X + 2 × eta-period) | ~200 | new |

**E2e milestone**: 8-node cluster with `NAKAMOTO_ETA_ROTATION_SNAPSHOTS=100`, mid-run delegate-stake change to op-7. Verify op-7's slot wins do NOT shift until after 2 full eta periods (~24 min wall clock at default cadence). This closes #177 cleanly because the demo's stake-change-impact assertion gets a deterministic deadline.

**Unblocks**: Slice S1+ NIPoPoW work.

---

## Slice S1 — Multi-level VRF trials (Phase A.1, ~3 days)

Per proposal §2.1, each slot fires `L = 10` independent trials per validator using domain-separated VRF outputs from the single ρ. The level-µ trial passes iff `H(ρ ‖ µ) < τ_µ`, where `τ_µ` shrinks geometrically.

- `LevelTrial` data + helper computing all L trials from one ρ in O(L · hash) (~120 LOC, `modules/node-shared/.../nakamoto/nipopow/LevelTrial.scala`)
- `LevelThreshold` parameter table from proposal §2.2 (`τ_µ = τ_0 · 2^(-µ)` with shifted-exp gating) (~150 LOC)
- `SuperLevelParams` — the L0–L9 parameter table (~60 LOC)
- Tests: L independent trials produce statistically independent passes; per-level density matches `f_0 · 2^(-µ)` over 10k slots (~80 LOC)

**No consensus impact**: level trials are computed offline from the existing ρ; they don't change which slot is "won" (level-0 is identical to today's VRF check).

---

## Slice S2 — SubchainState header field (Phase A.2, ~2 days)

- `SubchainState(levelCounts: Vector[Long])` carried in each snapshot's header (~120 LOC + codecs)
- `SubchainStateUpdater`: `updateFrom(parentState, levelTrialOutcomes) → newState` (~150 LOC)
- Slot-cert protobuf field for the L-vector (~100 LOC schema + scodec)
- Backward-compat: old snapshots with no subchain field decode to `Vector.fill(L)(0L)`; new code accepts both for one eta period after deploy
- Tests: 1k-snapshot replay produces identical level counts on every node (~100 LOC)

**Validation**: this should NEVER change finality — only adds a header observation. Run iter, verify cluster still finalizes at same cadence as pre-deploy.

---

## Slice S3 — TowerStore (Phase B, ~1 week)

- `TowerStore[F]` trait + `MptTowerStore` impl keyed `(level, ordinal) → snapshotHash` (~250 LOC)
- `TowerFinalizer` — Phase-3 sink that appends an entry on `T_depth2.advance` (~120 LOC, wired into `SnapshotLeaderLoop.finalityMonitor`)
- MPT overlay extension for tower-entry pruning under memory pressure (~50 LOC)
- Tests: tower length grows monotonically with finalized depth; pruning preserves the prefix needed for the most-recent N proofs (~100 LOC)

**Validation**: 8-node soak, observe `dag_nakamoto_tower_entries_total{level}` Prometheus gauge per level grows at expected `f_0 · 2^(-µ)` rate.

---

## Slice S4 — Tower proof builder + verifier (Phase C, ~1 week)

- `TowerProofBuilder[F]` serves `GET /nakamoto/tower` and `GET /nakamoto/tower/since/{N}` (~250 LOC, edit `dag-l0/.../routes/TowerRoutes.scala`)
- `TowerVerifier` (pure `F[_]`) — round-trips a serialized proof, validates: (a) all L trial outcomes per claimed-eligible slot; (b) density-relative-error gating; (c) eta-rotation reconstruction against `EtaCalculation` (~250 LOC)
- Density-violation detector (~50 LOC)
- Adversarial-tower tests: garbage levels, level-0 forged, density-padding attacks (~70 LOC)

**Validation**: build proof from gl0-0's tower, verify on gl0-7. Round-trip latency for a 10-eta-period proof ≤ 200ms.

---

## Slice S5 — Inclusion proofs + HTTP routes (Phase D, ~3 days)

- `GET /nakamoto/inclusion?ordinal={N}` — returns Merkle path from `(N, level=0)` up through finalized tower entries
- Prometheus: `dag_nakamoto_tower_density_relative_error{level}` for cluster-wide quality observability

---

## Slice S6 — E2e validation iter (~3 days, ongoing)

- 8-node cluster + bulk-tx load for 4+ hours
- Build NIPoPoW proof of last 2 eta periods from gl0-0
- Verify proof on gl0-7 + a fresh light-client harness (Scala-only, no JVM SDK yet — Phase E is separate)
- Confirm: proof size ≤ 50 KB, verification time ≤ 200ms, level-density relative error ≤ 5% per level

---

## Out of scope for §3 (deferred)

- **Phase E light-client SDK** (~1,500 LOC, multi-language) — separate work item, post-v1
- **On-chain anchoring** of the tower root in the metagraph header — proposal §4.5 says no (anchoring requires a re-org-safe checkpoint mechanism we don't have; tower lives in the HTTP layer for v1)
- **Velvet fork** path for legacy nodes that don't speak NIPoPoW — proposal §5.5 keeps this open; not needed for v1 since we're greenfield

---

## Risk register

1. **Density-tuning empirical gap** — proposal §6.4 flags this as the hardest non-cryptographic part. We should validate density behavior on the iter-stake-1-1 cluster BEFORE building proof routes (catch parameter mismatch early).
2. **N-2 staggering correctness** — if Slice S0 is wrong, all downstream NIPoPoW levels are silently broken because the verifier uses the wrong stake-at-period. Property test from S0.6 is load-bearing.
3. **KES forward-security assumption** — Slice 9 (load-bearing flip) is on by env flag default-off. Before NIPoPoW v1 ships, flip `NAKAMOTO_KES_ENFORCE=true` by default (separate ~1-day follow-up; gated on a clean iter with enforce=true).

---

## Critical-path lineup

```
S0 (N-2 staggering)   ├──→ S1 (L trials) → S2 (SubchainState header) → S3 (TowerStore) → S4 (Verifier) → S5 (HTTP) → S6 (e2e)
                      │
                      └──→ #177 KES stake-change demo unblocks (uses S0's deterministic stake-shift deadline)
```

The single thread is sequential; S0 is the keystone. Parallel opportunities:
- Slice S4 verifier can be designed/spec'd while S3 store is implemented (test-first works).
- Phase E light-client SDK can begin design in parallel with S4+ (but doesn't block v1).
