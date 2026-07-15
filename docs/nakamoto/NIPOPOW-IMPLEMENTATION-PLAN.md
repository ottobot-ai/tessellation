# §3 NIPoPoW — implementation slice plan

**Status**: PARTIAL IMPLEMENTATION; PRODUCTION ACTIVATION DARK (reviewed 2026-07-14).
**Companion to**: [NIPOPOW-PROPOSAL.md](./NIPOPOW-PROPOSAL.md) §1–§7 (design + phase sizing). This file slices the design into ordered, dependency-aware chunks with prereq gates and e2e milestones.

Estimate from the proposal §6.6: **~2,400 LOC, 6–10 engineer-weeks** in-node (Phases A–D). Add ~1–2 weeks for the N-2 staggering prereq (S0). Total: **8–12 weeks** to v1.

> **Production activation is dark (2026-07-14).** The HTTP routes remain mounted, but `GlobalSnapshotConsensus` keeps
> `nipopowProofProviderRef = None`, so proof and verification requests return 503. The tower finalizer and exact-walk catch-up coordinator
> are staged, tested components and are not attached to the live finality monitor. Activation is blocked on all of the following:
>
> 1. proof construction bound to an immutable, exact `(ordinal, hash)` target rather than mutable latest/head storage;
> 2. one all-or-none publication boundary for durable tower bytes, live indexed state, exact-hash cursor, and finalizer watermark, with
>    authenticated clean rebuild after crash, cancellation, hash failure, or density reorg;
> 3. serialized/idempotent finalizer publication under concurrent triggers, rather than separate read/append/watermark operations;
> 4. a bounded/paged catch-up algorithm whose total work is linear in the missing interval rather than rescanning the full
>    tip-to-cursor history for every processing chunk;
> 5. proof construction from one immutable exact `(ordinal, hash)` generation, including re-hashing the exact target rather than injecting
>    an independently read hash into a proof assembled from mutable current head/levels; and
> 6. ratified and enforced request, proof-size, verification-work, and memory limits. The S6 `50 KB` target is empirical acceptance data,
>    not a protocol bound on caller-supplied `k`.

---

## Prereqs (must land first)

This is a historical slice checklist, not current consensus authority. In particular, `T_count` is not a finality rail and `T_depth2`/`k2`
is retention/recovery policy only; neither can activate tower publication.

- ⚠ GL0 stake-weighted eligibility mechanics exist, but production safety is not closed: unbacked stake, fail-closed exact historical N-2
  availability, and exact-parent roster/key-pair resolution remain `ECO-02`/`CONS-03`/`CONS-05`/`CONS-06` gates. Execution-shard membership
  is a separate uniform public draw, not this stake-weighted leader rule.
- ⚠ Period-zero KES+VRF verification is load-bearing, but runtime registration activation, historical pair selection, secret provisioning/
  deletion, and implementation of O-12's ratified reorg/rejoin direction remain open under `CONS-06`. NIPoPoW activation cannot treat
  period-zero verification as a complete end-to-end runtime-key prerequisite.
- ⚠ Legacy trigger/telemetry scaffold (`b65` series) exists, but it is not the target finality stack. `T_count` is non-authoritative and
  must be removed/subsumed; `T_depth2` is local retention telemetry only. Target Phase 2 remains decided-attestation `T_weight` OR canonical
  `k1` depth, and the current optimistic accumulator still requires O-01's implementation, parameter, and proof gates.

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
| ~~S0.5~~ | ~~Backfill at genesis: stamp the genesis stake distribution as N-2/N-1/N for the first 3 periods~~ — **DROPPED 2026-05-20** by the historical implementation because `relativeStakeAt` falls through to current GSI during warmup. The current audit does not treat the assumptions that current GSI still equals genesis and no stake event can affect periods 0-2 as proved activation invariants; the reopened S0.6 coverage below must freeze the exact warmup behavior. | — | — |
| ~~S0.6~~ | ~~Unit tests + property test (stake change at ord X is invisible to eligibility until ord X + 2 × eta-period)~~ — **DROPPED 2026-05-20** alongside S0.5. The current audit does not accept fall-through plus boundary-write reasoning as proof. Reintroduce branch/reorg/restart property coverage before treating the N-2/N-1 schedule as an activation guarantee. | — | — |

**E2e milestone**: 8-node cluster with `NAKAMOTO_ETA_ROTATION_SNAPSHOTS=100`, mid-run delegate-stake change to op-7. Verify op-7's slot wins do NOT shift until after 2 full eta periods (~24 min wall clock at default cadence). This closes #177 cleanly because the demo's stake-change-impact assertion gets a deterministic deadline.

**Unblocks**: Slice S1+ NIPoPoW work.

---

## Slice S1 — Multi-level VRF trials (Phase A.1, ~3 days)  ✅ **LANDED `15f64d8fb`**

Per proposal §2.1, each slot the producer runs L-1 independent eligibility trials (one per super-level µ ∈ {1..L-1}, default L=10) all derived from the single existing VRF output `ρ_S` via domain-separated rehashing. **L0 stays untouched** — every snapshot is a level-0 hit by chain construction (the existing `EligibilityChecker` against `f(δ)` is the L0 production gate; the level-0 trial in NIPoPoW framework is implicit).

```
τ_µ(S) := Blake2b512(ρ_S ‖ "TEST-" ++ µ) / 2^512                          // domain-separated rehash
S is a level-µ superblock  ⟺  τ_µ(S) < θ_µ^eff(g_µ, δ_S)                  // pass condition
θ_µ(g_µ)   = p_µ^max · (1 − exp(−(g_µ − ψ_super) / σ_µ)) for g_µ ≥ ψ_super, else 0   // shifted-exp over base-block gap
θ_µ^eff    = θ_µ(g_µ) · min(1, δ_S / γ)                                   // L0 slot-gap gating
```

**Critical**: this is L independent trials, NOT Kiayias nested rarity. Per paper §6, independence forces the adversary to satisfy L independent constraints simultaneously; the PoW `τ < 2^(-µ)` analog has correct densities but no security gain.

Landed at `15f64d8fb`:
- `SuperLevelParams.scala` — L1-L9 (p_µ^max, σ_µ, target density) table from proposal §2.2.
- `LevelTrial.scala` — `LevelTrial(level, tau, effectiveThreshold, passed)`.
- `LevelTrialComputer.scala` — pure F-effecting computer (τ rehash + gating + threshold + runAll).
- 16/16 tests passing; uses Bifrost continued-fraction `Exp` (byte-deterministic).

**No consensus impact**: level trials are pure offline computation from the existing ρ; level-0 production unchanged.

---

## Slice S2 — SubchainState header field (Phase A.2, ~2 days)

Split into two phases against the "8gl0+4mg+4shards baseline that can't be broken" rule:

### Phase 2a ✅ **LANDED `5ec3737f3`** — pure data + updater

- `SubchainState(levelCounts: Vector[Long])` — per-super-level cumulative count, size-invariant via `require`.
- `SubchainStateUpdater.updateFrom(parent, trials)` — pure F-free transformation. Validates trial vector size + level ordering.
- 10/10 tests passing; no consensus path touched.

### Phase 2b — header wiring (consensus-impacting, deferred)

- Slot-cert protobuf field for the L-vector (~100 LOC schema + scodec; greenfield so no back-compat needed).
- `SnapshotLeaderLoop` populates the field via `SubchainStateUpdater` at production time.
- Verifier reads + validates the field on snapshot ingestion.
- Tests: 1k-snapshot replay produces identical level counts on every node (~100 LOC).

**Validation**: this should NEVER change finality — only adds a header observation. Run iter, verify cluster still finalizes at same cadence as pre-deploy.

---

## Slice S3 — TowerStore (Phase B, ~1 week)

- `TowerStore[F]` trait + `MptTowerStore` impl keyed `(level, ordinal) → snapshotHash` (~250 LOC)
- `TowerFinalizer` — local retention/proof-service component; staged and deliberately unwired pending the activation gates above. There is
  no protocol Phase 3, and `T_depth2`/`k2` cannot confer finality or select a branch. Before wiring, concurrent `finalizeThrough` calls must
  be serialized or made transactionally idempotent: the staged implementation derives work from separately read refs and publishes the
  tower append before a separate watermark update, so overlapping calls can duplicate/conflict or regress the watermark.
- `HistoricalCommitmentSmtStore.append` must publish its durable insert/commit and live `VersionedMptStorage` commit as one recoverable
  generation. Cancellation or hashing failure between those steps currently leaves durable and live views disagreeing until restart.
- MPT overlay extension for tower-entry pruning under memory pressure (~50 LOC)
- Tests: tower length grows monotonically with finalized depth; pruning preserves the prefix needed for the most-recent N proofs; concurrent
  finalizers cannot duplicate/conflict/regress; and injected cancellation/failure at every append publication boundary yields either the old
  or new complete generation, with live/durable parity before and after clean replay (~100 LOC)

**Validation**: 8-node soak, observe `dag_nakamoto_tower_entries_total{level}` Prometheus gauge per level grows at expected `f_0 · 2^(-µ)` rate.

---

## Slice S4 — Tower proof builder + verifier (Phase C, ~1 week)

- `TowerProofBuilder[F]` serves `GET /nakamoto/tower` and `GET /nakamoto/tower/since/{N}` (~250 LOC, edit `dag-l0/.../routes/TowerRoutes.scala`).
  It must pin one immutable exact-target generation, recompute/verify that target's hash, and derive the head and all levels from the same
  generation. The staged builder independently reads an ordinal snapshot, current head, and levels, then substitutes the separately read
  hash into the exact target without re-hashing; a same-height reorg can therefore construct a mixed-generation proof.
- Catch-up must page or persist traversal progress so `B`-sized runs over an `M`-snapshot gap perform `O(M)` total historical reads. The
  staged coordinator walks the full tip-to-cursor interval and only then takes `maxPerRun`, yielding `O(M^2/B)` reads across repeated runs.
- `TowerVerifier` (pure `F[_]`) — round-trips a serialized proof, validates: (a) all L trial outcomes per claimed-eligible slot; (b) density-relative-error gating; (c) eta-rotation reconstruction against `EtaCalculation` (~250 LOC)
- Density-violation detector (~50 LOC)
- Adversarial-tower tests: garbage levels, level-0 forged, density-padding attacks, same-height reorg during exact-target construction, and
  total traversal-work bounds across repeated catch-up chunks (~70 LOC)

**Validation**: build proof from gl0-0's tower, verify on gl0-7. Round-trip latency for a 10-eta-period proof ≤ 200ms.

---

## Slice S5 — Inclusion proofs + HTTP routes (Phase D)  ⚠ **CODE LANDED; PRODUCTION PUBLICATION DISABLED**

- `GET /nakamoto/nipopow/proof?fromOrd=N&k=K` — returns Circe-encoded `NipopowProof` from ordinal N with L0-suffix length K
- `GET /nakamoto/nipopow/proof/genesis?k=K` — convenience: full-chain proof from genesis
- `POST /nakamoto/nipopow/verify` — server-side verification offload (returns `{verified: bool, error?: ProofError}`). It currently shares
  the absent provider gate with proof construction and therefore returns 503 even though verification is pure and needs no local tower.
  Restore verifier-only availability through a separately constructed bounded verifier capability; do not enable proof generation with it.
- `NipopowProofProvider[F]` trait at `node-shared/.../nipopow/` wraps `(TowerProofBuilder, TowerVerifier)` with the `(genesisEta, etaRotationSnapshots, lddConfig)` bound at construction. The route seam is a `Ref[F, Option[NipopowProofProvider[F]]]`, but production intentionally leaves it empty until every activation gate at the top of this document is closed.
- Routes at `dag-l0/.../http/routes/NipopowRoutes.scala`. URL prefix `"/nakamoto"` added to `InternalUrlPrefixes`.
- `ProofError` JSON encoder routes through a stable `kind` discriminator (sealed-ADT, no string-matching).
- Current focused `NipopowRoutesSuite`: 13/13 passing. Historical aggregate counts are not activation evidence; the restart/reorg, target
  binding, and adversarial resource suites listed above remain mandatory.
- Prometheus `dag_nakamoto_tower_density_relative_error{level}` — DEFERRED to a follow-up; the metric infrastructure for the density check is wired in the dashboard revamp (`5338d6c59`), but the gauge sampler that feeds it isn't.

---

## Slice S6 — E2e validation iter (~3 days, ongoing)

- 8-node cluster + bulk-tx load for 4+ hours
- Build NIPoPoW proof of last 2 eta periods from gl0-0
- Verify proof on gl0-7 + a fresh light-client harness (Scala-only, no JVM SDK yet — Phase E is separate)

**Empirical acceptance criteria** (per paper §5.3, §5.4, §5.7):
- Proof size ≤ 50 KB
- Verification time ≤ 200ms
- Per-level density relative error ≤ 5% across all super-levels (matches paper Fig 5 "Superblock Density Validation" — target densities `f_0 · 2^(-µ)` validated within 1-13% per level at 10M slots)
- Honest cumulative weight outpaces a simulated burst adversary by ≥10× in fork-race ensemble (per paper §5.3 Fig 7)
- Settlement-layer behavior unchanged: cluster finalizes at same cadence as pre-S2-phase-2b deploy (no consensus regression)

---

## Out of scope for §3 (deferred)

- **Phase E light-client SDK** (~1,500 LOC, multi-language) — separate work item, post-v1
- **On-chain anchoring** of the tower root in the metagraph header — proposal §4.5 says no (anchoring requires a re-org-safe checkpoint mechanism we don't have; tower lives in the HTTP layer for v1)
- **Velvet fork** path for legacy nodes that don't speak NIPoPoW — proposal §5.5 keeps this open; not needed for v1 since we're greenfield

---

## Risk register

1. **Density-tuning empirical gap** — proposal §6.4 flags this as the hardest non-cryptographic part. We should validate density behavior on the iter-stake-1-1 cluster BEFORE building proof routes (catch parameter mismatch early).
2. **N-2 staggering correctness** — if Slice S0 is wrong, all downstream NIPoPoW levels are silently broken because the verifier uses the wrong stake-at-period. With S0.5/S0.6 dropped (see slice table), correctness is enforced by S0.4 boundary writes + StakeRegistry warmup fall-through — both deterministic. Re-introduce a property test if `StakeRegistry.relativeStakeAt` is ever refactored.

3. **GSI/MPT/stateProof triangle at genesis** (uncovered 2026-05-20): `mkFirstIncrementalSnapshot` computes ord=1 stateProof from PRE-augmentation GSI; `augmenter` then overlays delegated-stake + collateral records; `syncFromGlobalSnapshotInfo` writes augmented bytes to MPT. The stored ord=1 snapshot.stateProof is inconsistent with the persisted GSI + MPT. Compounding factor: ECDSA signatures on `DelegatedStakeRecord` are generated at JSON load time and are NOT deterministic, so `SortedSet[DelegatedStakeRecord]` ordering varies per node — `activeDelegatedStakes` MPT bytes diverge across the cluster. This is the genuine bug behind the slot=3 `delegStakes,mptRoot` reject. Fix is on the genesis-path side (augment-before-mkFirstIncrementalSnapshot), not the NIPoPoW slice plan.
4. **KES forward-security assumption** — Slice 9 made KES load-bearing unconditionally (no enforce flag; receivers drop any message that fails KES verify). NIPoPoW v1 can rely on KES authenticity end-to-end.
5. **Unbounded proof-service work** — no ratified `k`, proof-byte, verification-step, or memory limit currently turns the empirical S6 targets into fail-closed runtime bounds. The production provider stays absent until those bounds and adversarial resource tests land.

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
