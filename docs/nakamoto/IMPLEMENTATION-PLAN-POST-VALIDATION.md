# Implementation Plan — Post-Validation Roadmap (2026-05-16)

> **HISTORICAL, SUPERSEDED IMPLEMENTATION PLAN.** This roadmap describes an
> earlier branch, parameter set, finality model, and sequencing plan. It is not a
> current task list or authorization to restore removed fork-only paths. Retain it
> as planning history only; current work must be derived from source,
> [`../../AGENTS.md`](../../AGENTS.md), and accepted ADRs.

**Branch:** `feature/serde-typeclass-shim` (HEAD `a1f703d0` at time of writing).
**Status:** sequenced implementation plan after empirical validation closed.
Companion to [`NAKAMOTO-PLAN.md`](../../NAKAMOTO-PLAN.md) (in-flight workstream)
and [`NAKAMOTO-TODO.md`](../../NAKAMOTO-TODO.md) (backlog).

This document sequences the implementation work that lies ahead of
Tessellation-Nakamoto now that:

- the four-trigger finality stack is wired
  (`FinalityTrigger[F]` typeclass + `T_weight` / `T_count` / `T_depth1` /
  `T_depth2`, see `attestation-and-finality.md` §0);
- the analytical bounds composition is captured in
  [`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md);
- the integrated lifecycle sim (`~/repos/research-nipopos-2026/sims/integrated_lifecycle.py`)
  validates the bounds empirically — see
  [`GKL-EMPIRICAL-REPORT.md`](../../../research-nipopos-2026/docs/GKL-EMPIRICAL-REPORT.md);
- the Snowball calibration locked in **`(K=8, α=5, β=10)`** via the v2
  dual-mode GPU sweep
  ([`AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md)
  §0.A, commit `5ace3d36`);
- the NIPoPoW construction is design-locked
  ([`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md)) with re-tuned
  `(p_µ^max, σ_µ)` gated schedule
  ([`NIPOPOW-RETUNE-NOTES.md`](../../../research-nipopos-2026/docs/NIPOPOW-RETUNE-NOTES.md));
- the cross-shard mitigation is design-locked on Option A (VRF-sortition)
  + Option C (slashing)
  ([`CROSS-SHARD-MITIGATION-PROPOSAL.md`](./CROSS-SHARD-MITIGATION-PROPOSAL.md)).

Each phase below is **scoped to one concrete deliverable**, with
dependencies, files to touch, validation criteria, an empirical-validation
gate, and a conservative effort estimate (estimates are educated guesses,
not measured).

**Hard constraints across all phases:**

- `f_B = 0.05` in production. Untouched at the LDD level.
- Per
  [`feedback_epistemic_honesty`](../../../../../home/euler/.claude/projects/-home-euler-repos-tessellation-nakamoto/memory/feedback_epistemic_honesty.md):
  no phase is "validated" until its empirical-validation gate has fired
  and the evidence is listed. Default position is "not yet validated" +
  the planned gate.
- All effort estimates below are person-days, conservative; flagged as
  *estimated, not measured*. Validation gates that need a sim re-run are
  short (the integrated sim runs in seconds — 3.5 s on RTX 5090 per
  `GKL-EMPIRICAL-REPORT.md` §8); validation gates that need an e2e iter
  are long (the iter31 / iter32 / iter36 e2e ran in ~1 hour total).

---

## §0 Status

### §0.1 What has landed (referenced, not re-spec'd here)

| Workstream | Reference |
|---|---|
| **Finality trigger stack** — `FinalityTrigger[F]` typeclass, `T_count` (`7003be21`), `T_depth2` (`06455f98`), attestation skew (`422e1a6b`), chain-quality observable + HTTP route (`866cd598`), `MptOverlay.pruneBelow` (`173e6a7d`), `P-11b` self-exclusion (`95471c7f`), `P-11` `RebootstrapOrchestrator` (`01ebcca6`), `P-14` `parentSlot` wiring (`bec9de6b`). | `NAKAMOTO-PLAN.md` §5; `attestation-and-finality.md`. |
| **GKL composition doc** — analytical bounds for the integrated stack. | [`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) (commit `45c97895`). |
| **Avalanche-attestation proposal** — Snowball `(K=8, α=5, β=10, Δ=slot/2)` design-frozen, **implementation pending**. | [`AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md) (commits `2ba450bf`, `be1c8250`, `14dc839d`, `bb32f4b5`, `1212ed77`). |
| **NIPoPoW proposal** — L=10 domain-separated VRF trials per slot, re-tuned `(p_µ^max, σ_µ)` schedule, **implementation pending**. | [`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) (commits `bcb42140`, `2b697941`); [`NIPOPOW-RETUNE-NOTES.md`](../../../research-nipopos-2026/docs/NIPOPOW-RETUNE-NOTES.md). |
| **Cross-shard mitigation proposal** — Option A (VRF-sortition of operator keys) + Option C (slashing) chosen, **implementation pending**. | [`CROSS-SHARD-MITIGATION-PROPOSAL.md`](./CROSS-SHARD-MITIGATION-PROPOSAL.md) (commits `a1f703d0`, `8f6fe726`). |
| **TrustStorage / l0Trust removed** — dead code path purged. | Commits `f97dfb13`, `828d65a6`. |
| **Sim infrastructure** — GPU calibration sweep (RTX 5090, 7 min for full 1896-cell sweep, 19.6 min for dual-mode 6696-cell sweep); integrated lifecycle sim validates GKL bounds empirically in 3.5–3.6 s per config. | `~/repos/research-nipopos-2026/sims/`; `GKL-EMPIRICAL-REPORT.md`. |

### §0.2 What needs implementing — phase order

| Phase | Workstream | Critical-path successor of | Sim gate ready? | E2E gate ready? |
|---|---|---|---|---|
| **§1.1** | Stake-weighted VRF election | — | partial (uniform overlay sweep landed; stake-weighted needs distinct-stake sim) | yes (iter31/iter36 baseline) |
| **§1.2** | KES port from Bifrost | — | n/a (crypto, not consensus) | needs new KES key-evolution e2e |
| **§2** | Avalanche-attestation cascade (Snowball) | independent of §1.1, §1.2 | yes (`avalanche_attestation_full_gpu_n10000_v2.json`) | needs new convergence e2e |
| **§3** | NIPoPoW level-µ chains | §1.1 + §1.2 | yes (`nipopow_levels.py` with `PAPER_TABLE_1_GATED`) | needs new tower-self-consistency e2e |
| **§4.A** | Cross-shard Option A (VRF-sortition) | §1.1 | yes (`cross_shard.py` baseline) | needs new shard-rotation e2e |
| **§4.C** | Cross-shard Option C (slashing) | §1.2 + §4.A | partial (equivocation detector design only) | needs new slashing-evidence e2e |
| **§5** | Sharding proper | §4.A + §4.C in production | n/a (out of scope here) | n/a |

### §0.3 Gating

- **§1.1 and §1.2 are parallel tracks.** They are the prerequisite for
  every other phase. Both must land before §3 or §4 can be merged.
- **§2 is independent of §1.1 and §1.2.** Snowball uses uniform sampling
  (`AVALANCHE-ATTESTATION-PROPOSAL.md` §0.I) and the §2-internal
  primitives don't touch stake or KES. §2 may land in parallel with §1.
- **§3 depends on both §1.1 and §1.2.** Per-level eligibility weights by
  stake (so needs §1.1) and tower entries past the next eta-rotation
  boundary must be un-forgeable (so needs §1.2).
- **§4.A depends on §1.1.** VRF-sortition's safety bound depends on
  validators being weighted by stake; pre-§1.1 the threshold reduces to
  equal-weight which can be circumvented by sybil-spinning.
- **§4.C depends on §1.2.** Slashing-evidence non-repudiation depends on
  KES so a slashed operator cannot claim "the key was rotated post-forge".
- **§5 (sharding proper) is gated on both §4.A and §4.C being in
  production.** Per GKL §5.2 the cross-shard CQ bound collapses at
  `α_total > 1/(2S)` — i.e. **12.5% at S=4**. **Sharding is strictly
  less safe than single-chain unless A+C are in production.** This is
  not a stylistic preference but a derived safety property.

### §0.4 The empirical-validation process rule

**Future protocol parameter changes must run the empirical validation
BEFORE landing in proposals.** This is a process rule grounded in the
specific instance documented in
[`AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md)
§0.A: the original (Snowball-untested) recommendation `(K=3, α=2, β=10)`
held until the v2 dual-mode GPU sweep (commit `5ace3d36`) revealed K=3
to be at the per-color accumulator noise floor and flipped the
production target to `(K=8, α=5, β=10)`. The lesson is structural — the
production parameter set was wrong by a factor of 2.7× on the K
parameter, and only the empirical sweep surfaced it. Future parameter
selection MUST go through the same gate.

Concretely: any new threshold, cadence, depth, or weight knob proposed
in a Tessellation-Nakamoto doc MUST cite a sim payload (e.g.
`sims/data/<name>.json`) and Wilson-bound or 3σ-bound the proposed value
against empirical observation BEFORE the proposal is merged. The
proposal review checklist gains a "sim payload cited?" item.

---

## §1 Phase 0: Prerequisites (parallel tracks)

### §1.1 Stake-weighted VRF election

**Goal.** Replace the equal-weight `Ratio(1, N)` stake registry with
combined `delegatedStake + nodeCollateral` weighted-by-amount, sourced
from `GlobalSnapshotInfo.activeDelegatedStakes +
activeNodeCollaterals`. Per
[`project_sharding_strategic_signals`](../../../../../home/euler/.claude/projects/-home-euler-repos-tessellation-nakamoto/memory/project_sharding_strategic_signals.md):
two-tier stake (delegated + collateral) is the unit.

**Dependencies.** None (parallel to §1.2). Must complete before §3, §4.A.

**Spec source.** No standalone proposal doc. Sized in
[`NAKAMOTO-TODO.md`](../../NAKAMOTO-TODO.md) §"Stake-Proportional VRF".
The stub in
[`StakeRegistry.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/StakeRegistry.scala)
(line 134) names the entry point. Threshold formula
`threshold = 1 - (1 - f(δ))^relativeStake` is the
[`EligibilityChecker.threshold`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala)
existing math (line 32); only the `relativeStake` input changes.

**Files to touch.**

| File | Touch |
|---|---|
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/StakeRegistry.scala` | New `stakeWeighted` constructor (stubbed at line 134); reads `activeDelegatedStakes` + `activeNodeCollaterals` from `GlobalSnapshotInfo`. |
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala` | No structural change — `relativeStake: Ratio` is already the right shape. |
| `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala` | Switch `StakeRegistry` constructor wire-in from `equalWeight` to `stakeWeighted(snapshotInfoR)`. |
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/TipTracker.scala` | `T_weight` already reads `stakeRegistry.relativeStake(peerId)` (no change needed); ensure self-exclusion at line 60+ still holds under non-uniform stake (it does, by construction). |
| New unit + property tests in `modules/node-shared/src/test/`. |

**Tests required.**

- Unit: `StakeRegistry.stakeWeighted` returns `Ratio` matching
  `(delegatedStakeᵢ + nodeCollateralᵢ) / Σ(delegatedStakeⱼ + nodeCollateralⱼ)`
  for several stake-distribution fixtures.
- Property: total stake sums to `Ratio.One` (within tolerance for
  rounding of the BigInt math); `relativeStake(p) ∈ [Ratio.Zero,
  Ratio.One]` for all `p`.
- Property: `EligibilityChecker.threshold(stakeWeighted(p), δ, cfg)`
  for varied `δ` produces a monotone-non-decreasing-in-`δ` threshold
  (already proven for equal-weight; rerun under non-uniform).
- E2E: 8-node iter (≈ 70 min) with explicit non-uniform stake
  distribution (e.g. `0.05, 0.05, 0.05, 0.05, 0.15, 0.15, 0.25, 0.25`)
  — verify per-validator block-production rate ≈ stake fraction.

**Validation criteria.**

- Per-validator block-production rate ratio matches stake ratio within
  ±20% (10000-slot window); higher-stake validator demonstrably wins
  more slots.
- `T_weight` fires correctly under non-uniform stake (no NID
  regression from §0.F of `AVALANCHE-ATTESTATION-PROPOSAL.md` because
  P-11b self-exclusion is preserved structurally).
- No regression in finality-trigger validator semantics: `T_count`
  still uses unweighted distinct-peer counting (
  [`FinalityTrigger.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/FinalityTrigger.scala)
  line 313 — `count = 1` per peer; not stake-weighted).

**Estimated effort.** 5–8 person-days (*estimated, not measured*). Most
of the cost is the e2e validation + property-test coverage; the
StakeRegistry change itself is ≈ 1 day. Distinct-stake e2e harness
work + non-uniform stake snapshot bootstrap is the long pole.

**Risk areas.**

- The `MinActiveQuorumFraction` (
  [`StakeRegistry.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/StakeRegistry.scala)
  line 63) defaults to 1/2 of seedlist by **count**. Under
  stake-weighted finality this should probably also become stake-weighted
  (1/2 of stake, not 1/2 of validator count). Decide before §1.1
  merges.
- Stake distribution may rotate per eta period — the
  `stakeWeighted` constructor must be careful about which
  `GlobalSnapshotInfo` it reads (current ord vs eta-period boundary).
  Tied to §3 dependency: NIPoPoW light clients need the historical
  stake distribution per period, so a side-effect of §1.1 is the
  "historical stake-distribution snapshotting" question
  (`NIPOPOW-PROPOSAL.md` §3.3 — currently un-snapshotted).
- Sybil resistance is bounded by the on-chain stake registration cost;
  no protocol-layer defence against split-stake attacks beyond what
  the genesis allocation enforces. Out of scope for §1.1 itself but
  noted for the broader stake-model workstream.

**Empirical-validation gate.**

- Sim: `sims/adv_depth_expanded_parallel.py` re-run at distinct
  stake distributions (e.g. f_adv concentrated at the 50% stake
  fraction held by a single key). Confirm depth-k CP bound
  `risk(k=255, Δ=0) ≈ 3 × 10⁻¹¹` still extrapolates from
  10M trials — `FINDINGS_adv_depth_expanded.md` calibration.
- E2E: iter cycle with non-uniform stake at 3-node and 8-node
  clusters; per-validator block-rate ratio matches stake ratio.

---

### §1.2 KES port from Bifrost

**Goal.** Port Bifrost's KES (Key-Evolving Signatures, Bellare-Miner
1999) implementation into Tessellation-Nakamoto so that validators'
signing keys evolve forward each eta period. Required by §3 (NIPoPoW
retroactive non-forgeability) and §4.C (slashing-evidence
non-repudiation).

**Dependencies.** None (parallel to §1.1). Must complete before §3, §4.C.

**Spec source.** No standalone proposal doc. Constraints in
[`project_kes_port_constraints`](../../../../../home/euler/.claude/projects/-home-euler-repos-tessellation-nakamoto/memory/project_kes_port_constraints.md):

> When porting KES forward-secure signatures: MUST include Bifrost's
> read-once secret store (avoids multiple copies of evolved private-key
> bytes resident at once). Be very conservative modifying the scheme;
> improvements welcome only with high confidence and as separate
> proposal.

Bifrost reference: external repo (not in this tree).

**Files to touch.**

| File | Touch |
|---|---|
| `modules/keytool/src/main/scala/io/constellationnetwork/keytool/` | New `kes/` subdirectory with `KesKeyPair`, `KesPrivateKey`, `KesPublicKey`, `KesEvolutionProof` types. |
| `modules/shared/src/main/scala/io/constellationnetwork/security/signature/signature/SignatureProof.scala` | Add `kesEvolution: Option[KesEvolutionProof]` (or sibling type) for KES-signed payloads. |
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EpochState.scala` | KES period rotation hook — evolve key at eta-period boundary. |
| `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala` | Sign slot certificates with current-period KES key (existing flow signs with `KeyPair`; rewire to `KesKeyPair`). |
| Read-once secret store: new helper in `keytool` (Bifrost pattern). |
| New unit + property tests under `modules/keytool/src/test/`. |

**Tests required.**

- Unit: KES `(sign, verify)` round-trip per period; verify-against-old-period
  fails after evolution.
- Property: `kesPrivateKey.evolve(n + 1)` cannot reproduce signatures
  from any prior period `≤ n` (forward security property).
- Property: read-once secret store invariant — each evolved
  private-key byte slice is wiped after use, never copied. Cross-check
  with `jhsdb` / `jdeps` or a JVM-byte-tracking test harness.
- E2E: 8-node iter with explicit KES rotation at the eta-period
  boundary; verify slot-certs signed under period-`j` key still
  verify after rotation; verify the *private key* for period `j` is
  irrecoverable.

**Validation criteria.**

- `KesEvolutionProof` from period `j` to `j+k` for any `k ≥ 1`
  verifies as a chain of intermediate evolutions.
- Re-running the e2e cluster with a "compromised" key from period `j-3`
  fails to produce a valid slot-cert at period `j`.
- No regression in slot-cert verification latency (KES verify is
  comparable to Ed25519 verify; the cost is on the signing path,
  which is amortised over the eta period).

**Estimated effort.** 15–25 person-days (*estimated, not measured*).
KES port is the biggest single workstream in the post-validation
roadmap. Most of the cost: getting the read-once secret store right,
the test surface for forward-security, and the wire-format
compatibility with the existing signature pipeline. Bifrost's
implementation is well-trodden but in a different language; porting
to Scala 2.13 / cats-effect 3 is non-trivial.

**Risk areas.**

- Wire-format compatibility — KES signatures are larger than Ed25519;
  ensure they fit in the existing `SignatureProof` envelope or that
  the envelope grows compatibly.
- Period rotation timing — must be deterministic across the cluster;
  reuse the eta-rotation cadence (R = 2550 snapshots) so KES periods
  align with eta periods (one period per eta epoch).
- Cold-start key-generation cost — KES tree keys are large; pre-compute
  at genesis where possible.
- Read-once secret store interactions with the cats-effect `Sync[F]`
  semantics — careful to not let the JVM optimiser keep evolved key
  bytes alive past their nominal lifetime.

**Empirical-validation gate.**

- No sim equivalent — KES is a crypto primitive, not a consensus rule.
- E2E: explicit forward-security e2e (above) confirming period-`j` key
  irrecoverable post-rotation. This is a binary test, not a
  probabilistic sim.

---

## §2 Phase 1: Avalanche-attestation cascade (parallel to §1)

**Goal.** Convert
[`TipTracker.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/TipTracker.scala)'s
single-counter-per-attestation semantics into the Snowball cascade
described in
[`AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md)
§2 at production-locked `(K=8, α=5, β=10, Δ = slotDurationMs/2)`. Each
validator runs the per-(node, ord) cascade, decides at most one
preference per ordinal (decision §0.F emit-once), and emits its
attestation only on decision. Per-color accumulator persists across
flips (decision §0.K Snowball semantics).

**Dependencies.** Independent of §1.1, §1.2 (uniform peer sampling, no
KES requirement at the cascade layer). Production-deployable in
parallel; should land before §4.A so the cross-shard composition
inherits the converged-per-ordinal property.

**Spec source.**
[`AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md)
commit `2ba450bf` (locks decision §0.M, K=8/α=5/β=10) and earlier
commits in the design chain (`be1c8250`, `14dc839d`, `bb32f4b5`,
`1212ed77`). Empirical anchor:
`~/repos/research-nipopos-2026/sims/data/avalanche_attestation_full_gpu_n10000_v2.json`
(commit `5ace3d36`).

**Files to touch.**

| File | Touch |
|---|---|
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/TipTracker.scala` | New per-(node, ord) Snowball state: `preference: Hash`, `accum: Map[Hash, Int]`, `decided: Bool`, `initialized: Bool`. Snowball cascade loop (proposal §2.2). The `TipTracker` algebra itself unchanged at the receiver — only the *producer* changes (decision §3 composition). |
| `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala` | `processValidSnapshot` / `emitAttestation` / `emitTipAttestation` — emit-once gating moves here (proposal §3.1 table row). Remove §5.1 RE-ATTEST visibility ticker (proposal §3.1 — removed under decision §0.F). |
| `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala` | Remove the §5.1 re-attestation ticker (lines 489-510 per proposal §3.1). The `finalityMonitor` itself is untouched — trigger stack is unchanged at the wire level (proposal §3 invariant). |
| `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/RebootstrapOrchestrator.scala` | Demoted (decision §0.B): 30s tick removed; primitives stay; admin endpoint moves to `POST /admin/rebootstrap` (gated by admin auth). |
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/FinalityTrigger.scala` | No structural change. P-11b self-exclusion (commit `95471c7f`, line 60+ of TipTracker docstring) **rolled back** per proposal §3.1 — restore NID. |
| New sidecar wire protocol: `(queryId, ordinal)` query → `(queryId, ordinal, preference: Option[Hash], accum: Map[Hash, Int], decided: Bool)` response. Lands in the `p2p/` Go sidecar + Scala bridge. |
| Config knobs (proposal §2.4): `NAKAMOTO_AVALANCHE_K`, `_ALPHA`, `_BETA`, `_TICK_FRACTION_NUM`, `_TICK_FRACTION_DEN`. Pattern follows `NAKAMOTO_ATTESTATION_THRESHOLD` (TipTracker.scala line 115). |
| New unit + property tests in `modules/node-shared/src/test/`. |

**Tests required.**

- Unit: per-(node, ord) Snowball state machine — initial seed,
  α-majority increment, flip on `argmax(accum)` change, decision
  when `accum.top - accum.runnerUp ≥ β`.
- Property: under uniform honest peers, decided preference matches
  the majority of seed preferences in `O(β)` rounds.
- Property: under coordinated_lie adversary at f_adv ≤ 0.33, K=8, β=10,
  safety violations ≤ 3.7e-4 (Wilson upper at 10k trials, matches
  proposal §0.A).
- Property: symmetric rollback (decision §0.E) — on flip,
  `MptOverlay.rollbackTo(flipOrd - 1)` is invoked exactly once;
  TipTracker's `pendingRef` is reset to the post-flip hash.
- E2E: 8-node iter (≈ 70 min) — verify median Snowball convergence
  ≤ 4.25 s (proposal §2.4 large-N median 17 rounds × 250 ms).
  Verify zero observed safety violations across the full iter.
- E2E: small-cluster fallback iter (N=3, N=5) — verify
  `T_depth1`-only operation works (proposal §2.4 small-cluster row).

**Validation criteria.**

- `dag_nakamoto_snowball_decisions_total` Prometheus counter
  increments once per (validator, ord) across the iter — no
  re-emits (decision §0.F).
- `dag_nakamoto_snowball_rounds_p99` histogram p99 ≤ proposal §2.4
  envelope for cluster size N (e.g. p99 ≤ 5 s at N ≥ 16).
- Zero `chainStore.divergentRefuseCount` increments across the iter
  (P-11 re-bootstrap orchestrator's signal stays at zero —
  confirming the fork-recovery deadlock attractor is structurally
  gone, decision §0.B).
- No `RE-ATTEST` log entries (decision §0.F — removed).
- `T_weight` self-attestation is included again — verify NID by
  running two observer nodes on the same canonical chain past-hoc
  with the same gossip transcripts; both reach the same finality
  decision.

**Estimated effort.** 12–18 person-days (*estimated, not measured*).
The cascade itself is small (≈ 1 day for the state machine);
symmetric rollback wiring through `MptOverlay` is the long pole (3-5
days); the sidecar wire protocol is medium (3-5 days, mostly Go);
e2e validation is the gating step (multi-day iter cycles).

**Risk areas.**

- Symmetric rollback (decision §0.E) is load-bearing. The current
  `MptOverlay.MultiBranch` design (#56.11) handles branch
  checkout/discard but the Avalanche-flip path uses a different
  trigger pattern than the existing chain-selection-driven path.
  Worth proving via a focused integration test before the e2e iter.
- Sidecar query rate: at K=8 the per-tick load is 2.7× the K=3
  estimate (proposal §2.4 — `16k qps` cluster-wide at N=100). The
  sidecar should absorb this but the empirical envelope at large
  N is untested in our deployment.
- Tick-cadence determinism (decision §0.A): `Δ = slotDurationMs/2`
  computed via `Ratio[BigInt]`. The numerics package already exposes
  this — care needed to not slip a floating-point computation in
  anywhere along the cascade.

**Empirical-validation gate.**

- Sim already in hand: dual-mode GPU sweep
  (`avalanche_attestation_full_gpu_n10000_v2.json`, commit
  `5ace3d36`). Production parameters are calibrated against this.
  **No further calibration sim needed before merge** — but a
  post-merge re-sim should confirm production telemetry matches
  the sim's p50/p99 envelope.
- E2E: convergence iter at N=8 (proposal §2.4 row 3 — note residual
  coord_lie ≈ 0.51 % at K=3 fallback) and N=16+ (proposal §2.4 row
  4+).

---

## §3 Phase 2: NIPoPoW level-µ chains

**Goal.** Add `L = 10` independent VRF trials per slot under
domain-separated rehashing (`Blake2b512(ρ_S ‖ "TEST-" ++ µ)`); record
per-snapshot `subchainState : Vector[L][(slot, height, tipHash)]` so
verifiers can walk per-level subchains. Tower anchoring is gated to
Phase 3 (ARCHIVAL, `T_depth2`).

**Dependencies.** §1.1 (stake-weighted VRF — per-level trials weight
by stake) AND §1.2 (KES — tower entries past the next eta-rotation
boundary must be un-forgeable). Sequencing per
[`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) §3.3 (bootstrap order
non-negotiable):

> stake-weighted VRF first, then KES, then NIPoPoW.

**Spec source.**
[`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) commits `bcb42140`,
`2b697941`. Per-level density schedule from
[`NIPOPOW-RETUNE-NOTES.md`](../../../research-nipopos-2026/docs/NIPOPOW-RETUNE-NOTES.md)
(branch `sim/integration`, sim `sims/nipopow_levels.py`,
`PAPER_TABLE_1_GATED` constants — `(p_µ^max, σ_µ)` re-tuned for the
gated process).

**Files to touch.**

| File | Touch |
|---|---|
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala` | New `nipopowTrials(vrfOutput, L = 10): Vector[Ratio]` — one `Ratio` per level (line ≈ 108 — same `vrfOutputAsRatio` substrate, with domain-separation via `Blake2b512(ρ ‖ "TEST-" ++ µ)`). |
| `modules/shared/src/main/scala/io/constellationnetwork/schema/nakamoto/` | New `SubchainState` type — `Vector[L][(Slot, Long, Hash)]`, 48 bytes/level, 480 bytes total at L=10. Lives on every snapshot header. |
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/NakamotoProposer.scala` | Compute per-level trial result + populate `subchainState[µ]` (carry forward if not a hit; advance to `(sl, h_µ_parent + 1, hash(B))` if a hit). |
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/ChainSelection.scala` | **Unchanged.** Per proposal §2.4 cumulative weight is used only for proof-size amplification, NOT for chain selection (which stays on `maxvalid-tk`). |
| New per-level threshold constants (per `NIPOPOW-RETUNE-NOTES.md` "Before/after schedule" table) baked into a `NipopowConfig` companion of `LddConfig`. |
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/HistoricalMptProofService.scala` | New MPT keyed `(level, ordinal) → snapshotHash` for tower inclusion proofs (proposal §2.6). |
| New HTTP routes in `modules/dag-l0/.../routes/`: `GET /nakamoto/tower` and `GET /nakamoto/tower/since/{ord}` (proposal §4.1, §4.2). |
| New light-client side library (not in scope for this phase; tower production + verification at gl0 only). |
| New unit + property + integration tests under `modules/node-shared/src/test/` + `modules/dag-l0/src/test/`. |

**Tests required.**

- Unit: per-level trial — `EligibilityChecker.nipopowTrials(vrfOutput)`
  produces deterministic `Vector[Ratio]` per `(vrfOutput, L)`.
- Property: per-level density at `n = 10000` snapshots matches
  `NIPOPOW-RETUNE-NOTES.md` Table "Validation §primary criterion" —
  L1 within ±2%, L2..L9 within 4σ Bernoulli noise.
- Property: tower self-consistency — for each `µ ∈ 0..L-1`, the
  `subchainState[µ]` pointers form a valid chain back to the
  level-µ root.
- Property: domain-separation independence — Pearson correlation of
  `τ_µ` across levels ≤ 0.01 (per `NIPOPOW-RETUNE-NOTES.md` "Auxiliary
  criteria" — 0/36 off-diagonals exceed 4-SE at n=100k).
- Integration: synthesise a 10k-block tower offline, verify it via
  the light-client protocol (proposal §4.1) without access to the
  full chain.
- E2E: 10k-snapshot iter (≈ 70 min at 7s cadence) with tower-self-
  consistency reporter validating every snapshot. Zero failures.

**Validation criteria.**

- Per-level achieved density within ±2% of target at 100k snapshots
  (per `NIPOPOW-RETUNE-NOTES.md` "secondary criterion") OR within
  4σ at smaller samples.
- `subchainState` header overhead is **480 bytes/snapshot** at L=10
  (proposal §2.3 — 1.2 MB/day at 2,500 ord/day).
- `T_depth2` gates tower production: tower entries are emitted only
  for ordinals at `bestTipOrd - k₂ = 65536`. No tower entries
  produced for shallower ordinals (otherwise long-range adversaries
  can rewrite mid-chain).
- Chain selection still uses `maxvalid-tk` — tower weight does NOT
  feed `ChainSelection.compare`. Verify by running a controlled
  grinding-adversary sim (paper Fig 9 regime, recreatable from the
  `sims/run_grinding_ci.py` harness) and confirming settlement
  collapse threshold stays at ~30% (LDD-block-count) not ~10%
  (LDD-weighted, the broken path).

**Estimated effort.** 20–30 person-days (*estimated, not measured*).
The trials math is ≈ 2 days; `subchainState` header plumbing is ≈
3-5 days (header serialisation, MPT keyed prover, schema migration);
HTTP routes + light-client protocol is ≈ 5-8 days; e2e validation
including 100k-snapshot density confirmation is the long pole.

**Risk areas.**

- The re-tuned `(p_µ^max, σ_µ)` schedule in `NIPOPOW-RETUNE-NOTES.md`
  diverges from the canonical `NIPOPOW-PROPOSAL.md` Table at §2.2 —
  the proposal documents the un-gated paper values, the sim
  documents the gated values. **Production must use gated values**;
  the proposal's footnote at §2.2 should be amended in a follow-up
  doc commit. (The retune notes flag this; doc-update is a small
  follow-on, not part of this phase.)
- `T_depth1`-only nodes (small clusters, partition) cannot
  participate in tower production — tower entries are gated on
  `T_depth2` (proposal §4.3, §5.4). Verify that the light-client
  bootstrap path doesn't break under cluster-wide N < 16
  (the small-cluster Avalanche fallback regime per §2.4).
- Header size cost: 480 bytes/snapshot is ≈ 5% overhead at current
  snapshot sizes; verify no surprises in p2p bandwidth budgets.
- The cumulative-weight function `w(B)` for proof scoring (proposal
  §2.4) must NOT feed `ChainSelection.compare` — easy to wire wrong
  given how close the math is. Strong invariant test + Fig 9 regime
  re-verification gate before merge.

**Empirical-validation gate.**

- Sim: `nipopow_levels.py` already validates the per-level density
  match at n=10k / n=100k / fit-`n` (per `NIPOPOW-RETUNE-NOTES.md`
  "Validation" section). Production must reproduce these rates at a
  10k+ snapshot iter.
- Sim: grinding-adversary regression — `sims/run_grinding_ci.py` to
  confirm chain selection still on `maxvalid-tk`, not `maxvalid-weighted`.
- E2E: 10k-snapshot iter with tower-self-consistency reporter.

---

## §4 Phase 3: Cross-shard mitigation (Options A + C)

### §4.A Option A: VRF-sortition of operator keys to shards

**Goal.** New `OperatorShardAssignment[F]` service derives each
operator's shard eligibility per epoch via `VRF(operator_pk, ηₑ
‖ "SHARD-ASSIGN") mod S`. Gate SC binary admission at
[`StateChannelValidator.validateAllowedSignatures`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala)
(currently lines 164-167) by adding a `validateShardEligibility` step
that asks `OperatorShardAssignment.isEligible(operatorPk, shard,
currentEpoch)`.

**Dependencies.** §1.1 (stake-weighted VRF — Option A's safety bound
depends on the global stake being meaningfully weighted; under
equal-stake an adversary with N keys gets N independent shard draws
which the sortition-unit decision §7.1 must address).

**Spec source.**
[`CROSS-SHARD-MITIGATION-PROPOSAL.md`](./CROSS-SHARD-MITIGATION-PROPOSAL.md)
§2, §6.1 (Options A skeleton). Sortition unit decision in §7.1
(default per-operator-key, with hybrid alternatives — needs decision
before A0).

**Files to touch.**

| File | Touch |
|---|---|
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/sharding/` | **New directory.** `OperatorShardAssignment[F]` trait + production impl (proposal §6.1.1). |
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala` | Reuse the canonical VRF + eta-seed pattern (line 57+); add a `vrfShardDraw(operatorPk, etaSeed)` helper. |
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala` | Extend `validateAllowedSignatures` (line 164) with `validateShardEligibility`. Constructor at line 84 gains `operatorShardAssignment: OperatorShardAssignment[F]` + `epochProgressR: F[EpochProgress]`. |
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/modules/SharedValidators.scala` | Wire `operatorShardAssignment` + `epochProgressR` through. |
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelEventsProcessor.scala` | No structural change — admission flows through `StateChannelValidator`; rejected binaries fall out at the validator gate. |
| Config knobs (proposal §6.1.5): `NAKAMOTO_SHARD_VRF_EPOCH_LEN_SNAPSHOTS = 2550`, `NAKAMOTO_SHARD_COUNT_S = 4`, `NAKAMOTO_SHARD_VRF_ELIGIBILITY_GATE = on|off` (staged rollout). |
| New unit + property tests in `modules/node-shared/src/test/`. |

**Tests required.**

- Unit: `OperatorShardAssignment.assignmentFor` returns a
  deterministic shard for fixed `(operator_pk, ηₑ)`.
- Property: shard assignments are approximately uniform across the
  operator set at large operator count (`χ²` test against uniform).
- Property: adversary with `α_total = 1/3` stake spread over `N`
  operator keys can place at most `floor(N · α_total)` keys into
  any single shard (worst-case concentration); cross-shard CQ bound
  preserved at `α_local ≤ 1/3`.
- Integration: validator rejects an SC binary signed by a
  not-currently-eligible operator key (matching commit `8f6fe726`
  proposal §6.1.2).
- E2E: iter with explicit operator-key churn at epoch boundaries —
  validate sub-snapshots fail admission post-rotation if the
  operator's keys weren't VRF-rotated.

**Validation criteria.**

- Under `cross_shard.py` concentrated-in-shard-0 mode with
  `α_total = 0.33` at `S = 4`, **shard 0's accept rate is
  ≥ 0.9** (vs. 0.0008 in the no-mitigation baseline per
  `GKL-EMPIRICAL-REPORT.md` §5). The collapse goes away because the
  adversary cannot concentrate operator keys into shard 0.
- Per-shard validator-set distribution is uniform within `χ²` 95%
  CI at S=4 across 10 epochs.

**Estimated effort.** 12–18 person-days (*estimated, not measured*).
The `OperatorShardAssignment` service itself is ≈ 3 days; the
`StateChannelValidator` extension is ≈ 1-2 days; the wiring through
`SharedValidators` + per-epoch state management is ≈ 5 days;
e2e validation including operator-rotation harness is the long pole.

**Risk areas.**

- **Sortition unit decision is load-bearing** (proposal §7.1) —
  per-operator-key vs per-validator vs hybrid; the two-tier
  stake model from
  [`project_sharding_strategic_signals`](../../../../../home/euler/.claude/projects/-home-euler-repos-tessellation-nakamoto/memory/project_sharding_strategic_signals.md)
  pushes toward a finer-grained unit. **Needs decision before
  A0 starts.** Default per user directive 2026-05-15 is
  per-operator-key; the alternatives are documented in proposal
  §7.1.
- Operator infrastructure churn (proposal §2.3): a validator
  suddenly assigned to shard `s` must spin up shard-`s` infra.
  This is operationally expensive — the `T_depth1`-fallback path
  for shard liveness gaps (proposal §7.2) is the corresponding
  recovery mechanism; both must work cleanly together.
- Epoch length `E` (proposal §7.3): short `E` → less time for
  adversary stake re-accumulation, more churn; long `E` →
  opposite. Default to `R = 2550` snapshots (≈ 1 day at 7s
  cadence) to avoid a new rotation cadence; revisit after first
  production iter.
- Address → shard mapping (proposal §6.1.4): 1:1 default, but
  large metagraphs may need multi-shard. Revisit in §5 (sharding
  proper).

**Empirical-validation gate.**

- Sim: re-run `cross_shard.py` with the VRF-eligibility gate at
  `S = 4`, `α_total = 0.33`, `concentrated_in_shard_0` mode. **Shard
  0 accept rate must rise from 0.0008 (baseline collapse) to
  ≥ 0.9 (mitigated).** This is the load-bearing measurement.
- E2E: iter with operator-key rotation at epoch boundary;
  verify rejected binaries from not-eligible operators are
  visible in gl0 logs + metrics.

---

### §4.C Option C: slashing infrastructure

**Goal.** gl0 detects state-channel divergence (two SC binaries from
the same operator at same shard slot with different state hashes),
constructs `StateChannelEquivocationEvidence` packets, and slashes the
offending operator's `nodeCollateral`. KES (§1.2) is the hard
prerequisite to prevent evidence repudiation.

**Dependencies.** §1.2 (KES — `KesEvolutionProof` is part of the
evidence packet, proposal §6.2.2) AND §4.A (Option A landed first so
slashing is the *deterrent* for the residual marginal attacks, not the
primary defence).

**Spec source.**
[`CROSS-SHARD-MITIGATION-PROPOSAL.md`](./CROSS-SHARD-MITIGATION-PROPOSAL.md)
§4, §6.2.

**Files to touch.**

| File | Touch |
|---|---|
| `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala` | New equivocation detector observing the SC binary stream entering `accept` (proposal §6.2.1). |
| `modules/shared/src/main/scala/io/constellationnetwork/schema/statechannel/` | New `StateChannelEquivocationEvidence` type (proposal §6.2.2). |
| New slashing pipeline — proposal §6.2.3 sketches the surface; specific home directory `modules/node-shared/.../domain/slashing/` (does not exist yet). |
| Per-metagraph state — `activeNodeCollaterals` accounting in `GlobalSnapshotInfo` already exists ([`GlobalSnapshotInfo.scala`](../../modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala) line 154); wiring the slash deduction is bounded. |
| Config knobs (proposal §6.2.4): `NAKAMOTO_SLASHING_MAGNITUDE_PCT`, `NAKAMOTO_SLASHING_GATE`, `NAKAMOTO_SLASHING_GOVERNANCE_PAUSE`. Slashing magnitude itself is open (§7.4). |
| New unit + integration tests under `modules/node-shared/src/test/` + `modules/dag-l0/src/test/`. |

**Tests required.**

- Unit: equivocation detector flags `(b₁, b₂)` pairs sharing operator
  pubkey + shard slot key + different state hashes; ignores pairs
  with same state hash; ignores single-binary streams.
- Unit: `StateChannelEquivocationEvidence.verify` validates both
  signatures + KES evolution proof; rejects if either signature is
  invalid; rejects if KES evolution proof shows post-forge key
  rotation.
- Integration: end-to-end forge → detect → slash flow. Validate that
  the slashed operator's `nodeCollateral` is reduced by
  `NAKAMOTO_SLASHING_MAGNITUDE_PCT` × initial collateral.
- E2E: iter with a byzantine operator deliberately equivocating;
  verify the slash fires in the next eta period boundary +
  governance pause (if enabled) takes effect.

**Validation criteria.**

- Detected equivocation produces a single `StateChannelEquivocationEvidence`
  packet per (operator, shard, epoch) — no dup packets across the
  cluster.
- Slashed operator cannot replay the slashed evidence to slash again
  (replay protection — each evidence packet consumed once).
- KES evolution proof rejects evidence pairs where the second signature
  is from a key generation past the first (prevents the operator from
  rotating keys post-forge to repudiate evidence).
- Slashed `nodeCollateral` is correctly deducted from per-metagraph
  state (verify via `activeNodeCollaterals` view).

**Estimated effort.** 15–25 person-days (*estimated, not measured*).
Slashing pipeline is bigger than the equivocation detector — the
on-chain consumption of evidence packets, replay protection, dispute
mechanism, governance pause integration. Slashing magnitude decision
(proposal §7.4) is an open governance question that gates the
config-knob default but doesn't block the implementation itself.

**Risk areas.**

- **Slashing magnitude (proposal §7.4) is governance-bound** — 100%
  burn vs partial-slash with escalation is open. Default to a
  configurable value, but the production setting needs a governance
  decision, not a technical one.
- **Single-divergence detection (proposal §7.4)** — gl0 has no
  on-chain evidence to slash if the operator submits **only one**
  divergent binary (no equivocation pair). Cross-shard fraud-proof
  construction would be needed; out of scope here. Document the
  gap clearly so operators know what's enforced vs not.
- KES wire format must be settled before this phase starts (§1.2
  blocker).
- Governance pause integration — the operator's metagraph stays paused
  pending governance review. Need a defined unpause path or this
  becomes a soft DoS vector.

**Empirical-validation gate.**

- Sim: extend `cross_shard.py` or build a new
  `equivocation_detection.py` to model the byzantine operator forge
  + detect cycle. Validate that the slashing-evidence rate matches
  the residual-marginal-attack rate after Option A is in effect.
  (This sim does not yet exist; it's part of the §4.C scope.)
- E2E: iter with deliberate operator equivocation; verify the slash
  fires within one eta-rotation period and the operator's
  `nodeCollateral` drops.

---

## §5 Phase 4: Sharding proper (out of scope, mentioned)

**Goal.** Actual partitioning of state into independent shards (S > 1
production shards beyond gl0 itself acting as a single chain). This
is the **next major workstream** after §1.1, §1.2, §2, §3, §4.A, §4.C
land in production.

**Gating.** Per
[`GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) §5.2 the cross-shard CQ
bound collapses at `α_total > 1/(2S)` (12.5% at S=4, 5% at S=10).
**Without §4.A + §4.C in production, sharding is strictly less safe
than single-chain.** Therefore §5 is **strictly gated** on §4.A + §4.C
shipping.

**Out of scope for this document.** No file paths, no estimates, no
validation gate. The work is acknowledged here so the post-validation
roadmap is complete; the actual project plan for §5 follows from
deciding the open questions in
[`GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) §6, in particular
§6.4 (delayed-finality + cross-shard view consistency under partition)
and §6.5 (historical stake-distribution snapshotting).

---

## §6 Cross-cutting concerns

### §6.1 Schema bridge: Taktikos `block_id` ↔ cross-shard `sub_snapshot_id`

The Taktikos `block_id` (per-snapshot canonical hash in
[`NakamotoChainStore.scala`](../../modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoChainStore.scala))
and the cross-shard `sub_snapshot_id` (the per-shard sub-snapshot key
in
[`GlobalSnapshotStateChannelAcceptanceManager.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala))
are independently-derived hash types referring to overlapping concepts
(an L0 block ↔ a per-shard sub-snapshot ID). The cross-shard sim
(`cross_shard.py`) treats them as a single key for accounting purposes.
Worth standardising as part of §4.A: a canonical `SubSnapshotId` type
in `modules/shared/src/main/scala/io/constellationnetwork/schema/` that
both layers reference, with explicit conversion to/from existing
`Hash`-based keys. Small change (≈ 1-2 days), low risk; folds into the
§4.A scope.

### §6.2 Sim infrastructure as continuous validation

The integrated lifecycle sim (`sims/integrated_lifecycle.py`) runs in
3.5–3.6 s per config and the full `gkl_metrics.py` extraction in
seconds beyond that. **Wire it into CI** so every proposal change
re-runs the GKL bound verification. The sim payloads
(`sims/data/*.json`) become part of the proposal merge artefact.

Specific CI candidates:
- Per-shard CG bound (§1 of `GKL-EMPIRICAL-REPORT.md`).
- Per-shard CQ bound (§2 — caveats around integer-N rounding noted
  in the report; future runs at N ≥ 20 needed for clean f-stepping).
- Snowball CP at tip (§4) — quick smoke test that the production
  parameters still produce ε ≤ Wilson bound.
- Cross-shard CQ collapse (§5) — gated regression test for §4.A's
  mitigation effect.
- NIPoPoW per-level rarity (§6) — Bernoulli noise check for §3.

**N ≥ 20 needed for future precision sim runs.** The empirical report
flagged that at N=8 per shard, integer rounding hides f_adv=0.20 vs
f_adv=0.30 (both round to 2 byz per shard). Future sims targeting
finer-grained f_adv discrimination MUST bump N≥20 per shard.

### §6.3 Process rule: empirical validation before doc commitment

(Re-stated from §0.4.) Future protocol-parameter proposals MUST cite a
sim payload + Wilson/3σ bound before the proposal merges. The proposal
template (see
[`AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md)
§0.A for a canonical example) is the format. Apply to all future
parameter selections — `(K, α, β)`, `(p_µ^max, σ_µ)`, `(k₁, k₂)`,
sortition cadence `E`, slashing magnitude, etc.

---

## §7 Critical path + ETA

```
         ┌──────────────────────────────────────────────────────┐
         │ §1.1 Stake-weighted VRF   (5-8 person-days)          │──┐
         └──────────────────────────────────────────────────────┘  │
                            ↓                                       │
         ┌──────────────────────────────────────────────────────┐   │
         │ §1.2 KES port from Bifrost (15-25 person-days)       │──┤
         └──────────────────────────────────────────────────────┘   │
                  parallel to §1.1+§1.2:                            │
         ┌──────────────────────────────────────────────────────┐   │
         │ §2 Avalanche-attestation  (12-18 person-days)        │   │
         └──────────────────────────────────────────────────────┘   │
                            (§1.1 done)                             │
                            ↓                                       │
                                                                    ↓
         ┌──────────────────────────────────────────────────────┐
         │ §4.A Option A: VRF-sortition (12-18 person-days)     │──┐
         └──────────────────────────────────────────────────────┘  │
                            (§1.1+§1.2 done)                       │
                            ↓                                       │
         ┌──────────────────────────────────────────────────────┐   │
         │ §3 NIPoPoW level-µ chains  (20-30 person-days)       │   │
         └──────────────────────────────────────────────────────┘   │
                            (§1.2+§4.A done)                       │
                            ↓                                       │
         ┌──────────────────────────────────────────────────────┐   │
         │ §4.C Option C: slashing   (15-25 person-days)        │──┤
         └──────────────────────────────────────────────────────┘   │
                            ↓                                       │
                                                  (§4.A+§4.C done)  │
                            ┌─────────────────────────────────────┐ │
                            │ §5 Sharding proper (out of scope)   │ │
                            └─────────────────────────────────────┘ │
```

**Critical path:** §1.2 (KES port, 15-25 d) → §3 (NIPoPoW, 20-30 d) →
§4.C (slashing, 15-25 d) → §5 gate. Total critical path ≈ **50-80
person-days** end-to-end (= 10-16 person-weeks).

**Parallelism budget.** If two engineers can run §1.1 + §1.2 in
parallel (independent tracks), the §1 phase compresses to ≈ 15-25 d
calendar. If a third engineer runs §2 in parallel during §1, §2
lands at the same calendar boundary as §1. §3 and §4.A can also be
parallelised (§3 depends on §1.1 + §1.2; §4.A depends only on §1.1).

**Whole-roadmap total estimate (sequential):** **79-124 person-days**
(= 16-25 person-weeks, *estimated, not measured*). With moderate
parallelism (2 engineers), calendar time compresses to **8-15
person-weeks**.

---

## §8 Open questions per phase

| Phase | Largest open question |
|---|---|
| **§1.1** | Stake-weight denominator for `MinActiveQuorumFraction` — stake-fraction or count-fraction? Current default (count) becomes mismatched under heavy stake skew. |
| **§1.2** | Eta-period alignment — KES periods aligned with eta periods (R = 2550 snapshots) or independent cadence? Recommend aligned; verify. |
| **§2** | Sidecar query rate at scale — K=8 → 16k qps cluster-wide at N=100 per proposal §2.4. Production envelope at N=500+ untested. |
| **§3** | The re-tuned `(p_µ^max, σ_µ)` gated schedule in `NIPOPOW-RETUNE-NOTES.md` diverges from `NIPOPOW-PROPOSAL.md` §2.2 — production must use gated values; proposal needs a follow-up doc edit. (Sim is canonical; proposal is one revision behind.) |
| **§4.A** | Sortition unit (proposal §7.1) — per-operator-key (default) vs per-validator vs hybrid. Decision needed before A0 starts; affects two-tier stake interaction. |
| **§4.C** | Slashing magnitude (proposal §7.4) — 100% burn vs partial-slash. Governance-bound, not technical. |
| **§5** | Cross-shard view consistency under partition (`GKL-COMPOSITION.md` §6.4). Unproven; `MptOverlay.MultiBranch` is the mitigation substrate but the property is open. |

---

## §9 References

### Proposals (this repo)

| Doc | Commit |
|---|---|
| [`docs/GKL-COMPOSITION.md`](../GKL-COMPOSITION.md) | `45c97895` |
| [`docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md`](./AVALANCHE-ATTESTATION-PROPOSAL.md) | `2ba450bf` (latest), `be1c8250`, `14dc839d`, `bb32f4b5`, `1212ed77` (history) |
| [`docs/nakamoto/NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) | `bcb42140` (latest), `2b697941` (history) |
| [`docs/nakamoto/CROSS-SHARD-MITIGATION-PROPOSAL.md`](./CROSS-SHARD-MITIGATION-PROPOSAL.md) | `a1f703d0` (latest), `8f6fe726` (history) |
| [`docs/nakamoto/attestation-and-finality.md`](./attestation-and-finality.md) | living |

### Empirical anchors (research repo)

| Payload | Source |
|---|---|
| Snowball calibration v2 | `~/repos/research-nipopos-2026/sims/data/avalanche_attestation_full_gpu_n10000_v2.json` (commit `5ace3d36`) |
| Depth-k CP extrapolation | `~/repos/research-nipopos-2026/sims/FINDINGS_adv_depth_expanded.md` |
| Delay sensitivity | `~/repos/research-nipopos-2026/sims/adv_delay_sweep.py` |
| Integrated lifecycle sim | `~/repos/research-nipopos-2026/sims/integrated_lifecycle.py` + `GKL-EMPIRICAL-REPORT.md` |
| NIPoPoW level density schedule | `~/repos/research-nipopos-2026/sims/nipopow_levels.py` + `NIPOPOW-RETUNE-NOTES.md` |
| Cross-shard collapse | `~/repos/research-nipopos-2026/sims/cross_shard.py` + `sims/data/cross_shard_t_count_fixed_30s_concentrated_in_shard_0_S4_slots1000.json` |
| Taktikos paper (FC 2023) | `~/repos/research-nipopos-2026/paper/main.tex` |

### Code surfaces (referenced phase-by-phase above)

| File | Phase touch-point |
|---|---|
| [`modules/node-shared/.../nakamoto/StakeRegistry.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/StakeRegistry.scala) (line 134 `stakeWeighted` stub) | §1.1 |
| [`modules/node-shared/.../nakamoto/EligibilityChecker.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala) (line 32 threshold, line 57 checkEligibility, line 108 vrfOutputAsRatio) | §1.1, §3, §4.A |
| [`modules/node-shared/.../nakamoto/TipTracker.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/TipTracker.scala) (line 60+ self-exclusion, line 115 FinalityThreshold) | §2 |
| [`modules/node-shared/.../nakamoto/FinalityTrigger.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/FinalityTrigger.scala) (lines 177, 203, 230, 263 — trigger constructors) | §2 (P-11b rollback per proposal §3.1) |
| [`modules/dag-l0/.../nakamoto/SnapshotLeaderLoop.scala`](../../modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala) (lines 489-510 RE-ATTEST ticker — remove per proposal §3.1) | §2 |
| [`modules/dag-l0/.../nakamoto/NakamotoSyncDaemon.scala`](../../modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala) (`processValidSnapshot`, `emitAttestation`, `emitTipAttestation`) | §2 |
| [`modules/dag-l0/.../nakamoto/RebootstrapOrchestrator.scala`](../../modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/RebootstrapOrchestrator.scala) | §2 (demoted per decision §0.B) |
| [`modules/node-shared/.../statechannel/StateChannelValidator.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala) (line 84 `make`, line 164 `validateAllowedSignatures`, line 169 `validateSignaturesWithSeedlist`, line 173 `validateAtLeastOneSignatureInSeedlist`) | §4.A |
| [`modules/node-shared/.../managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala) | §4.C (equivocation detector) |
| [`modules/shared/.../schema/GlobalSnapshotInfo.scala`](../../modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala) (line 152-155: `activeDelegatedStakes`, `activeNodeCollaterals`) | §1.1, §4.C |
| `modules/node-shared/.../domain/sharding/` (new directory) | §4.A |
| `modules/keytool/.../kes/` (new directory) | §1.2 |

---

*Plan. No code commitment. Estimates are conservative educated
guesses, marked **estimated, not measured** throughout. Per
[`feedback_epistemic_honesty`](../../../../../home/euler/.claude/projects/-home-euler-repos-tessellation-nakamoto/memory/feedback_epistemic_honesty.md):
no phase is "validated" until the empirical-validation gate has fired
and the evidence is listed. Default position is "not yet validated" +
the planned gate. Process rule: future protocol-parameter changes
must run empirical validation BEFORE landing in proposals — see §0.4
and §6.3.*
