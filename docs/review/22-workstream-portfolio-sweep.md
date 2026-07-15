# 22 — Workstream Portfolio Sweep

> **HISTORICAL PRE-FIX PORTFOLIO.** This inventory was taken before universal
> GL0 replay and greenfield schema cleanup. Deleted authority-design names below
> are provenance recoverable from commit `725b25b`, not live dependencies.

> **Purpose.** A breadth-first catalog of **every** open, deferred, gated, or shelved
> workstream in the Tessellation-Nakamoto fork *other than* the three already grounded
> elsewhere, so a frontier-model (Fable) review can be aimed at the highest-value work
> across the **whole** portfolio — not just the active sharding track.
> **Method.** Doc header + `grep` for the feature's symbols in `modules/` (breadth over depth —
> no deep-dives). Every row cites a doc path, a `file:line`, or "NO CODE FOUND".
> **Verified against the working tree on 2026-07-07** (branch `feature/committee-state-diff`,
> HEAD `21933559c`). Memory was treated as *hypotheses* and spot-checked against source; the
> stale-memory corrections are in Appendix B.

---

## 0. Scope, method, and what is deliberately excluded

**Excluded (grounded in their own review docs — do NOT re-catalog here):**

| Workstream | Where it's grounded |
|---|---|
| Execution **sharding** (Track-1 / Track-3, committee re-exec, byte-diff adopt, finality band) | `docs/review/HANDOFF.md` + `10-reexec-byte-contract.md` … `14-packet-gaps-and-fallback.md`; `docs/nakamoto/TRACK1-TRACK3-IMPLEMENTATION-SPEC.md`, `SHARDING-PRODUCTION-READINESS-PLAN.md`, `ECONOMIC-TRUST-SHARDED-SECURITY-ARCHITECTURE.md`, `HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md`, `SHARD-CHECKPOINT-*`, `SHARD-SORTITION-WORKSTREAM-PLAN.md`, `SHARDABILITY-MAP.md`, `COMMITTEE-*`, `ROOTS-ONLY-SHARDING-ARCHITECTURE.md`, `EXECUTION-SHARDING-COMMITTEE-VERIFY-DESIGN.md`, `CROSS-SHARD-*`, `SHARDED-CURRENCY-MIRROR-*`, `ADOPT-COMMITTED-CURRENCY-DELTA.md`, `CURRENCY-APP-TOKEN-ENFORCEMENT.md` |
| **Serde** migration (Brotli-JSON → scodec, typeclass shim) | being grounded separately → `20-*` |
| **Hard-fork** / era migration | being grounded separately → `21-*`; docs `ERA-REGISTRY-DESIGN.md`, `NAKAMOTO-TODO.md §11` |

One sharding-internal item is flagged below only because it *crosses* into the portfolio risk
surface: **Track-2 (optimistic spendability gate) is NOT BUILT** while Track-3 makes the
`(k₂,k₁]` band revertable — a band reorg can revert an already-spent cross-shard output
(`HANDOFF.md §Track-2`, `grep kApprove|stageCheckpoint|spendabilityGate` = 0 hits). It stays in
the sharding review but is echoed in the shortlist because it is a genuine cross-cutting safety hole.

**Maturity legend:**
- **DESIGN-ONLY** — no implementing symbols found; pure doc.
- **PARTIALLY-BUILT** — some symbols exist; core path incomplete.
- **BUILT-BUT-GATED** — code exists but behind a default-off flag / not wired into the consensus path.
- **SHELVED** — code (or plan) exists but is abandoned / detection-only with no consequence path.
- **BUILT** — live in the production consensus path.
- **BUILT-ON-BRANCH** — complete on a sibling feature branch, **not merged** to the current branch.

---

## 1. Portfolio table

### 1a. Validator-state / crypto / slashing

| Workstream | Maturity | Evidence (doc + file:line / NO CODE) | Fable-grade? | Depends-on / sequencing |
|---|---|---|---|---|
| **Slashing consequence — equivocation & non-participation tiers** | **SHELVED (detection-only)** | `SLASHING-DESIGN.md` (draft). Validator exists: `node-shared/.../nakamoto/slashing/SlashableEvidenceValidator.scala:47`; equivocation evidence `.../slashing/ShardCheckpointEquivocationValidator.scala`; non-participation counters `.../managers/global/ShardNonParticipationStateManager.scala:151` (`ShardNonParticipationSlasher.evaluateEpochBoundary`) — **never called outside tests; no ledger sink credits/burns stake.** (Contrast: the **invalid-state-proof tier IS built** — see 1a next row.) | **Y** — Does a sharded committee retain honest-majority safety when 2 of 3 slashable fault classes have detection but no economic consequence? (ties to CQ-collapse bound α_total>1/(2S)) | Needs epoch-participating-set (below) as the demotion substrate; slashing-safety bar (`feedback_slashing_safety_bar`) |
| **Invalid-state-proof slash (fraud-proof tier)** | **BUILT** | `node-shared/.../nakamoto/slashing/InvalidStateProofSlashManager.scala`; durable slash → `Slashings` MPT partition **fieldId 34** (`shared/.../schema/mpt/GlobalStateKey.scala`); `.../sharding/WatchtowerFraudProofPool.scala`; `.../slashing/InvalidStateProofValidator.scala`. | N (built) | Part of sharding enforcement; the one slashing tier with a live consequence path |
| **Epoch-participating-set + demotion-as-exclusion** | **PARTIALLY-BUILT** (on branch `feature/epoch-participating-set`, Slices 1-2); shelf-ware on current branch | `EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md` ("DESIGN, no consensus code"). Counters/slasher exist (`ShardNonParticipationStateManager.scala:151`) but **zero production call sites**. Branch HEAD `f365f583c`. | Y — How to roll a static→dynamic validator roster into a live consensus without a finality gap during the swap? | Blocks committee-partition-adaptivity, timeliness incentive, sidecar-BLS |
| **Committee-partition adaptivity via participation set** | **DESIGN-ONLY** | `COMMITTEE-PARTITION-ADAPTIVITY-VIA-PARTICIPATION-SET.md` ("DESIGN NOTE, documentation only"). NO CODE — depends on task #22 participating-set rotation not on current branch. | N (mechanical once #22 lands) | Depends-on epoch-participating-set (#22) |
| **KES Wave-2 — on-chain rotation cert acceptance** | **PARTIALLY-BUILT** | `KES10-WAVE-2-DESIGN.md` ("design only"). KES **gossip path is authoritative** (Wave-1/Slice-9 DONE: `dag-l0/.../nakamoto/KesGossipVerification.scala:17` "made KES authoritative (no warn-only escape hatch)"). But `node-shared/.../kes/KesRegistrationCertAcceptanceManager.scala` is **not wired into GSAM**; verifiers still read the **genesis-frozen** `KesRegistry`. | Y — Is gossip-only KES authority (no on-chain rotation) a liability once long-lived shards need mid-life operator key rotation? | Sequencing: after epoch-participating-set; feeds BLS/committee cert v2 |
| **BLS aggregate signatures (snapshot/finality certs)** | **BUILT-ON-BRANCH** (`feature/bls-aggregate-sigs` S1–S5); **absent on current branch** | `BLS-AGGREGATE-SIGNATURE-DESIGN.md` + `BLS-PORT-MERGE-GUIDE.md`. On current branch `grep BlsSigner|AggregateSigned|ValidatorKeyRegistry` = 0 hits → **NO CODE HERE**. Branch HEAD `0fbbf24d4` (light-client serve/verify of gl0 finality cert). Blocked on BouncyCastle 1.85 stable. | Y — Does the unified rotatable `ValidatorKeyRegistry` (VRF+KES+BLS + PoP gate) hold its security under partial key-set rotation, and what's the merge-friction cost of vendoring a beta crypto lib? | Blocked on BC 1.85 stable; merge-conflict risk grows the longer it sits off-branch |
| **Sidecar BLS attestation aggregation** | **DESIGN-ONLY** | `SIDECAR-BLS-ATTESTATION-AGGREGATION-DESIGN.md` ("DRAFT"). NO CODE — no Go `blst` integration, no cross-lang KAT fixtures. Depends on #22 (participating-set) + #23 (BLS port). | Y — CPU-budget/QoS: does sidecar aggregation hold under Byzantine producer delay once BLS verify cost lands? | Depends-on BLS (#23) + epoch-participating-set (#22) |

### 1b. Finality / attestation / consensus core

| Workstream | Maturity | Evidence | Fable-grade? | Depends-on / sequencing |
|---|---|---|---|---|
| **Taktikos delay-cliff resistance (INV-FAULT-005)** | **NOT ENCODED** | `docs/review/01-invariants.md:341-355` — "As encoded: **nothing**. No adaptive-k, no finalization-pause-on-high-latency, no p99-propagation gate." LDD `fB=1/20` baseline (`application.conf:334`) guarantees adversary forward progress in long-δ regimes. | **Y (top pick)** — Past ~8s p99 propagation, per-attempt reorg risk is exponential and depth-k does NOT fix it; what adaptive discipline preserves safety under adversarial propagation inflation without killing liveness — and is it encodable given the LDD tail? | Standalone consensus-safety gap; Praos bounds don't transfer (Taktikos) |
| **NIPoPoW superblock / light-client proofs** | **PARTIALLY-BUILT** | `NIPOPOW-PROPOSAL.md` + `NIPOPOW-IMPLEMENTATION-PLAN.md`. Landed: `node-shared/.../nakamoto/nipopow/SuperLevelParams.scala:19` (S1), `SubchainState.scala:18` (S2a), `dag-l0/.../http/routes/NipopowRoutes.scala` (S5). **Deferred:** S2b header-wiring, S3 TowerStore, S4 TowerProofBuilder/Verifier, S6 e2e — currently **header-observation only, no proof tower**. | **Y (top pick)** — Can succinct light-client proofs be *sound* when the "difficulty certificate" is Taktikos' LDD slot-gap threshold (not PoW work), via level-µ independent trials? | Needs SMT-historical-proofs (gl0 store) for tower persistence |
| **Avalanche / Snowball attestation** | **PARTIALLY-BUILT** | `AVALANCHE-ATTESTATION-PROPOSAL.md` ("deferred behind stake-weighted VRF + KES"). `node-shared/.../nakamoto/SnowballAccumulator.scala:46,89` — per-color margin decision wired in TipTracker. **Full K-sample cascade (α-majority, flip-symmetric rollback) deferred (§2.2).** | Y — Does margin-only Snowball converge safely under split-honest adversaries, or does the missing K-sample cascade reintroduce the silence failure? (calibrated K=8/α=5/β=10, `project_avalanche_k8_snowball_recommendation`) | After stake-weighted VRF + KES |
| **Attestation-timeliness incentive** | **DESIGN-ONLY** | `ATTESTATION-TIMELINESS-INCENTIVE-DESIGN.md` ("DESIGN ONLY — v2/future. No code exists."). `MetagraphAttestationAggregator` has no arrival-slot / inclusion-distance / reward machinery. | **Y (top pick)** — Do graduated, bounded, recoverable inactivity penalties avoid the validator mass-exit cascade that burned Ethereum solo stakers when finality stalled? | Pairs with epoch-participating-set demotion |
| **Eta-rotation amortization** | **DESIGN-ONLY** | `ETA-ROTATION-AMORTIZATION-DESIGN.md` ("RFC/S0"). `EtaAccumulator` does **not exist**; verifier still does the O(R) epoch walk (`NakamotoSyncDaemon` ~1042-1072); producer fold `vrfAccRef` is in-memory only. (NB the eta-boundary *wedge* was separately fixed via bootstrapEta, `project_eta_boundary_wedge_cardano_fix`; the O(1) amortization is the unbuilt part.) | Y — If eta determinism fails and the verifier falls back to the branch-embedded eta, does trusting a snapshot's own eta break the light-client's chain-independent verification claim? | Interacts with NIPoPoW light-client soundness |
| **Finality-trigger stack** | **BUILT** | `attestation-and-finality.md`. `node-shared/.../nakamoto/FinalityTrigger.scala:56-59` (TWeight/TCount/TDepth1/TDepth2); wired `dag-l0/.../nakamoto/SnapshotLeaderLoop.scala:1050-1053`. | N (built) — residual: do 4 max-of triggers create ordering deps? | Done; low review priority |
| **Unified consensus engine (gl0 + metagraph, one seam)** | **DESIGN-ONLY** | `UNIFIED-CONSENSUS-ENGINE-DESIGN.md` ("DRAFT — hard-fork-scale for metagraphs"). Proposed algebra (`ChainLike`, `Eligibility`, `Membership`, `Finality`) does not exist. (`ChainSelection[F]` at `ChainSelection.scala:46` is the fork-choice rule, not the seam.) | **Y (top pick)** — Can one chain-based (solo-production + async-finality) engine serve metagraphs where finality is *enforced* not optional, without reviving the membership-agreement loop that killed BFT? | Gates metagraph-Nakamoto + two-level finality; `project_consensus_layering_decision` |
| **Fork-choice grindability (INV-FAULT-006)** | **PARTIALLY-EXPOSED** | `01-invariants.md:357-367`; `ChainSelection.scala:254-311` — VRF/hash tiebreak fires only on ordinal+slot tie (narrow). | N (narrow, "rare ≠ safe") | Keep the VRF tiebreaker (`feedback_vrf_tiebreaker_keep`) |

### 1c. Metagraph consensus / two-level finality

| Workstream | Maturity | Evidence | Fable-grade? | Depends-on / sequencing |
|---|---|---|---|---|
| **Metagraph (CL0/DL1) Nakamoto support** | **DESIGN / DEFERRED** | `NAKAMOTO-TODO.md #3` ("current impl is GL0-only"). Only **5** nakamoto refs in `modules/currency-l0/src/main` (all StateChannel/sender plumbing, not consensus) → metagraph consensus stays **BFT** by direction (`project_post_nipopow_phase_order`, "metagraphs stay BFT"). | Y (subsumed by unified-engine) | Depends-on unified consensus engine decision |
| **Two-level finality (GL0 fast + metagraph)** | **DEFERRED** | `NAKAMOTO-TODO.md #15, §7` ("GL0 only — metagraph deferred"). Two-tier model is an *architectural rule* (`project_two_tier_finality_model`) but the metagraph-local fast finality is unbuilt. | Y (part of unified-engine question) | After unified-engine |
| **Mempool reinsertion on finalize** | **NOT-BUILT** (may be partly stale) | `NAKAMOTO-TODO.md #9` — orphaned-fork events not recycled; `NakamotoChainStore.finalize` prunes but doesn't reinsert. | N | Verify against current chain-store before scheduling |

### 1d. Infra / research / ops

| Workstream | Maturity | Evidence | Fable-grade? | Depends-on / sequencing |
|---|---|---|---|---|
| **Mithril-over-SMT stake-threshold checkpoints** | **DESIGN-ONLY (research)** | `MITHRIL-OVER-SMT-CHECKPOINT-RESEARCH.md` ("research synthesis, not a committed design"). NO CODE — only "future Mithril" comments in `SnapshotLeaderLoop.scala`. | Y — Does an epoch-committed Taktikos committee already give the parameter-certification Mithril needed (the CVE was param, not crypto)? | Research; overlaps NIPoPoW / BLS |
| **SMT historical proofs (per-version commitments)** | **PARTIALLY-BUILT** | `SMT-HISTORICAL-PROOFS-DESIGN.md` ("research/design, no impl in doc"). `node-shared/.../nakamoto/GlobalChangeSetService.scala:8` imports `HistoricalCommitmentSmtStore` ("when the gl0 store is wired"); ~26 SMT symbols exist but **pending gl0-store wiring**. | Y — JMT-style every-key-provable-at-every-version vs cheaper roots-over-time index — which does NIPoPoW actually need? | Prereq for NIPoPoW tower persistence |
| **Local events service** | **BUILT-BUT-GATED** | `LOCAL-EVENTS-SERVICE-DESIGN.md` ("pending approval"). `node-shared/.../local_events/LocalEventsService.scala:38` exists, wired in `GlobalSnapshotConsensus.scala`, gated `nakamoto.local-events.enabled=false`. | N | Ops; flip when e2e stops polling |
| **Queue-boundedness audit** | **PARTIALLY-BUILT** | `QUEUE-BOUNDEDNESS-AUDIT.md` (trigger: OOM'd 4/5 gl0). The bounded ordinal-only `ChainSyncRequestQueue` was removed with its unsafe attestation-finality caller; **open Tier-1/2 findings** remain: GossipStream bridge, consensus output queues, `NakamotoSyncDaemon` pendingParentRef, and ShardBinaryBuffer accumulation on stalled peers. | Y — Cap *pending/unresolved* state globally, or TTL-per-use-case? (unbounded pending = liveness/DoS surface) | Reliability hardening; independent |
| **Metrics dashboard revamp** | **PARTIALLY-BUILT** | `METRICS-DASHBOARD-REVAMP.md` ("design + first slice"). ~283 metric emissions; Phase-2 panels (KES, Snowball, committee sortition, Tower density) + per-ordinal cardinality TODO. | N | Ops; watch cardinality vs Prom retention |
| **WAVE-2 HOCON config sweep** | **DESIGN-ONLY (plan not executed)** | `WAVE-2-HOCON-SWEEP-PLAN.md` ("Catalog + design draft. NO IMPLEMENTATION."). **9 `sys.env.get("NAKAMOTO_*")` remain** — e.g. `MetagraphCommitteeGate.scala` (×2 `NAKAMOTO_COMMITTEE_GATE_*`), `SlashableEvidenceValidator.scala` (`NAKAMOTO_SLASH_EVIDENCE_WINDOW`). **Violates the project HOCON rule** (`feedback_prefer_hocon_over_sysenv`). | N (but a live cluster-split risk) | Independent; each env read is a consensus-divergence footgun |
| **Era codec registry (hard-fork substrate)** | **PARTIALLY-BUILT** → **defer to 21-*** | `ERA-REGISTRY-DESIGN.md`. `EraCodecRegistry.eraForOrDie` tested (`shared/.../serde/SerdeShimSuite.scala:81`) but **not wired into production dispatch**; `HashSelect` still ad-hoc (`TessellationIOApp.scala:135-138`). | (see 21) | **Defer to hard-fork review 21-*** |
| **Unified state propagation** | **BUILT** | `UNIFIED-STATE-PROPAGATION.md` (2026-06-30 correction: built+wired). `ShardSubtreeProofService` + inclusion-proof transport. | N (sharding-adjacent, built) | Part of sharding enforcement |

### 1e. Architecture cleanup / deferred

| Workstream | Maturity | Evidence | Fable-grade? | Depends-on / sequencing |
|---|---|---|---|---|
| **Cell construct removal** | **SHELVED (plan only)** | `docs/CELL-REMOVAL-PLAN.md` + `docs/CELL-CONSTRUCT-ASSESSMENT.md`. `modules/kernel/.../Cell.scala` **still present**; `cellMonoid/PipeArrow/ΩList` = 13 refs (mostly vestigial + kernel test suites). Staged plan not executed. | N | Low priority; behavior-preserving deletion |
| **Crypto unification (hash algos + signing keys)** | **DESIGN-ONLY / deferred** | No doc; `project_crypto_unification_followup` (memory). "After Nakamoto stack lands." Related audits: `project_crypto_shortcut_audit_20260601`, `project_crypto_extension_brotli_hash_finding`. | Y — one hash + one key scheme across modules; sequences with the serde/era migration | After Nakamoto stack; couples to serde-20 / hardfork-21 |
| **RebootstrapOrchestrator (P-11 fork self-heal)** | **BUILT-BUT-GATED** | `dag-l0/.../nakamoto/RebootstrapOrchestrator.scala:69`; master-switch config `node-shared/.../config/types.scala:118` (default-OFF; flip stays gated per commit `605b8a496`). | N | Fork-recovery fallback; keep gated until deep-divergence path proven |
| **Content-addressed MPT (Ethereum-style)** | **DESIGN-ONLY (low priority)** | `NAKAMOTO-TODO.md #12`. Current `InMemoryMerklePatriciaProducer`; content-addressed trie is the "biggest refactor / best long-term" for MPT non-determinism (root cause of the fork storms, `project_nakamoto_fork_resolution_k2_freeze`). | Y — is the smtRoot-blind-compare fix a durable equilibrium, or does only content-addressing eliminate the non-determinism class? | Large refactor; sequences after sharding stabilizes |
| **Sync protocol (Nakamoto join flow)** | **BUILT** | `SYNC-PROTOCOL.md` (design sketch). `dag-l0/.../nakamoto/NakamotoSyncDaemon.scala` live. | N | Done |

---

## 2. The mainnet non-regression baseline (the v3.5.12 bar)

**The constraint.** This fork descends from `upstream = Constellation-Labs/tessellation`. The mainnet
line lives at tag **v3.5.12** (verified present locally: `git tag | grep 3.5.12`; note there is **no
local `main` branch** — the fork's mainline is `develop`, and `upstream/main` HEAD is `9594d6436`).
Mainnet supports **cross-metagraph / multi-metagraph** functionality exercised by an e2e suite;
execution sharding must **not regress it**.

**Current standard e2e topology** (`justfile:15` → `compose-runner.sh --num-gl0=3 --metagraphs=2`):
- **3 GL0** (Nakamoto), **2 independent metagraphs**, each with **1 ML0 + 1 CL1 + 1 DL1** (all BFT).
- Default **1 shard** (`NAKAMOTO_NUM_SHARDS=1`, non-sharded); memory "local standard 2mg/2shard" is the
  sharded-mode variant.

**Cross/multi-metagraph e2e coverage (the non-regression bar)** — orchestrated by
`docker/bin/compose-runner.sh` (test list at `:360` =
`currency,rewards,token-locks,allow-spends,spend,data-without-fee,data-with-fee,multi-metagraph`),
matrix at `.github/workflows/e2e-just-test.yml:111-123`:

| Test | Entry point | What it exercises (regression bar) |
|---|---|---|
| **multi-metagraph** (primary gate) | `.github/action_scripts/check_clusters/multi-metagraph.js` (`compose-runner.sh:1441`) | K≥2 metagraphs coexist without ID collision; **GL0 sees ALL K metagraph heads in `lastStateChannelSnapshotHashes`**; each ML0 advances past genesis. Budget 150s (single-shard) / 360s (sharded). |
| **allow-spends / spend** (cross-domain) | `.github/action_scripts/send_transactions/allow-spends-and-spend-transactions.js` (`:1485`) | Allow-spend created on one chain, verified in **DAG L0/L1 AND Currency L0/L1** and mirrored in **GL0's unified `activeAllowSpends[tokenId][address]`**; spend executes against it; expiry via pinned epoch; scenarios `{basic, invalidParent, invalidEpochProgress, invalidApprover}`. **This is the cross-domain swap infra sharding must preserve.** |
| **token-locks / token-lock-replacement** | `send_transactions/token-locks.js` (`:1473`); `delegated_staking/token-lock-replacement-edge-cases.js` (`:1360`) | Metagraph-scoped lock create/hold/expire; GL0 mirrors active locks; expiry uses **metagraph-pinned `globalSyncView.epochProgress`** (not GL0 live epoch). |
| **currency** | `send_transactions/currency.js` (`:1455`) | CL1 balance settles through CL1→CL0→GL0; GL0 `balances[address]` reflects last-adopted metagraph balance. |
| **rewards** | `rewards.js` (`:1464`) | Per-metagraph operator rewards aggregate into GL0 unified rewards. |
| **data-without-fee / data-with-fee** | `send_transactions/data-{without,with}-fee.js` (`:1513,:1522`) | DL1 data-app state propagates to GL0; with-fee variant is the historic cross-shard imbalance stressor (`project_data_with_fee_shard_imbalance_grind`). |
| **fork-recovery** | `--test=fork-recovery` (5 GL0), `test-fork-recovery.sh` | GL0 reorg must not drop/misorder metagraph state-channel binaries. |
| **delegated-staking** | `delegated_staking/delegated-staking.js` (`:1351`) | GL0-scoped stake in `activeDelegatedStakes` (orthogonal to metagraphs). |

**The bar, stated plainly:** every test above must stay green **and be byte-identical at
`numShards=1` vs `numShards=K`** (the determinism invariant, `SHARDING-PRODUCTION-READINESS-PLAN.md`).
The load-bearing cross-metagraph invariant is **GL0's unified allow-spend / token-lock / balance
registries seen by all metagraphs** — no shard may hold a partial view.

**Coverage gaps flagged** (upstream/roadmap features with **no dedicated e2e** here): cross-metagraph
state-proof verification (only implicit via `multi-metagraph.js`), light-client proofs across
metagraphs (deferred v2), cross-metagraph fraud proofs (deferred v2), multi-metagraph-per-shard
(v1 is 1:1 shard:metagraph). External multi-metagraph harnesses exist but are out-of-repo
(`reference_pacaswap_multimetagraph`, `reference_metakit_sdk_ts_inclusion_verifier`).

---

## 3. Shortlist — top 5 Fable-grade questions across the whole portfolio

Ranked by value = (frontier reasoning required) × (safety/economic blast radius) × (not already under review).

1. **Taktikos delay-cliff has zero mitigation code (INV-FAULT-005).**
   *`01-invariants.md:341`; LDD baseline `application.conf:334`.* Past ~8s p99 propagation the
   per-attempt reorg risk goes **exponential** (18% @ 8s, >60% @ 10s) and depth-k does **not** fix it;
   the LDD `fB=1/20` tail guarantees the adversary always makes forward progress. There is **no
   adaptive-k, no finalization-pause, no propagation gate anywhere**. *Fable:* design the minimal
   encodable defense (dynamic-k / pause-finalization / accept-unhealthy) that preserves safety under
   adversarial propagation inflation without collapsing liveness — and prove it survives the LDD tail
   where Praos common-prefix bounds don't transfer. **Highest value: a live, un-coded safety hole in
   the consensus substrate everything else rides on.**

2. **Two of three slashable fault classes are detection-only shelf-ware.**
   *`SLASHING-DESIGN.md`; validator `SlashableEvidenceValidator.scala:47`; non-participation slasher
   `ShardNonParticipationStateManager.scala:151` (no call sites); only the invalid-state-proof tier has
   a real sink, `InvalidStateProofSlashManager` → fieldId 34.* *Fable:* does the sharded committee model
   retain honest-majority safety when equivocation and non-participation are **observed but never
   punished**, given the cross-shard CQ-collapse bound (α_total > 1/(2S)) makes committees only as safe
   as their deterrent? What is the minimal consequence path (bond, demotion-as-exclusion, burn) that
   closes the gap without a mass-exit cascade?

3. **Can one chain-based engine serve metagraphs where finality is *enforced*, not optional?**
   *`UNIFIED-CONSENSUS-ENGINE-DESIGN.md` (design-only); `project_consensus_layering_decision`.* GL0 uses
   solo-production + async Nakamoto finality; metagraphs today are BFT (finality is a hard requirement,
   nodes must agree on membership before advancing). *Fable:* can the unified `Eligibility`/`Membership`
   seam give metagraphs Nakamoto-style liveness without reviving the membership-agreement loop that
   "killed BFT" — or is enforced-finality fundamentally incompatible with solo production at the
   metagraph layer? **Gates metagraph-Nakamoto (#3) and two-level finality (#15).**

4. **Is a NIPoPoW-style succinct proof sound when the difficulty certificate is an LDD slot-gap, not PoW work?**
   *`NIPOPOW-PROPOSAL.md`; landed `SuperLevelParams.scala:19`, `SubchainState.scala:18`, `NipopowRoutes.scala`;
   tower/verifier (S3/S4) deferred → header-observation only; `project_nipopow_level_mu_design`.* *Fable:*
   the level-µ construction uses **L independent domain-separated VRF rehashes vs per-level
   gap-conditioned thresholds** rather than nested PoW rarity — does that yield a sound, non-full-sync
   light-client proof over Taktikos, or does the pseudo-predictable VRF leak grinding room into the
   proof? Couples to eta-rotation determinism (does trusting a branch-embedded eta break chain-independent
   verification?).

5. **Graduated inactivity penalties vs the validator mass-exit cascade.**
   *`ATTESTATION-TIMELINESS-INCENTIVE-DESIGN.md` (design-only, no code) + `EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md`.*
   *Fable:* design the timeliness/inactivity incentive (arrival-slot, inclusion-distance, bounded
   recoverable penalty) so that a correlated liveness stall does **not** trigger the runaway leak that
   burned Ethereum solo stakers — while still deterring free-riding attestors. Pure-design, high-leverage
   incentive question that spans two shelved docs and interacts with #2 (demotion substrate).

*Cross-cutting runner-up (kept in the sharding review, flagged for the reviewer):* Track-3 makes the
`(k₂,k₁]` band revertable but Track-2's optimistic **spendability staging gate is NOT BUILT**
(`HANDOFF.md §Track-2`; `grep kApprove|stageCheckpoint|spendabilityGate` = 0) — so a band reorg can
revert an **already-spent** cross-shard output. What is the minimal defer-not-revert discipline that
keeps cross-shard spends safe under a revertable band without collapsing to synchronous k₂-wait latency?

---

## Appendix A — Branch map

| Branch | Represents | State (HEAD) |
|---|---|---|
| `feature/committee-state-diff` | **current** — sharding Track-1/3 (byte-diff adopt + finality band) | `21933559c` |
| `feature/bls-aggregate-sigs` | BLS workstream S1–S5 (unified key registry → detached aggregate cert → light-client serve/verify) | `0fbbf24d4`; **not merged** |
| `feature/epoch-participating-set` | Epoch-anchored participating-set + participation counters (Slices 1-2) + ml0 changeset-follow (#12) | `f365f583c`; **not merged** |
| `develop` | ottobot fork mainline — **predates Nakamoto** (HEAD = "VRF crypto primitives Phase 0", `ce19be98f`); stale relative to all Nakamoto feature branches | `ce19be98f` |
| `upstream/*` (Constellation-Labs) | mainnet/testnet/integrationnet + consensus-engine-rewrite; tag **v3.5.12** = the non-regression baseline | — |
| ~28 `worktree-agent-*` | ephemeral agent worktrees (incl. `abe9d8c297f13ef04` = Track-1/3 gate build) | — |

## Appendix B — Stale-memory corrections (verified this session)

- **"Slashing = detection-only / shelf-ware"** — *partly stale.* The **invalid-state-proof tier is now
  BUILT** with a durable ledger sink (`InvalidStateProofSlashManager`, `Slashings` fieldId 34,
  `WatchtowerFraudProofPool`). Equivocation + non-participation tiers remain detection-only.
- **"KES Slice 9 load-bearing flip pending"** — *stale.* KES is **authoritative on the gossip path**
  (`KesGossipVerification.scala:17` "made KES authoritative, no warn-only escape hatch"). The remaining
  unbuilt piece is **Wave-2 on-chain rotation-cert acceptance** into GSAM (verifiers still read the
  genesis-frozen `KesRegistry`).
- **"Stake-proportional VRF is equal-weight / stubbed"** (`NAKAMOTO-TODO.md #5`) — *stale.*
  `stakeWeightedMpt` + `relativeStakeAt` are **live** in the leader loop
  (`SnapshotLeaderLoop.scala:809,845`; `NodeStakeAggregator`).
- **`NAKAMOTO-TODO.md` items #6/#10 (proactive MPT rollback, fork/backfill recovery)** — *partly stale*;
  superseded by MptOverlay + the smtRoot-blind-compare storm fix (`project_nakamoto_fork_resolution_k2_freeze`).
  Re-verify each before scheduling.
- **BLS "design + proven dep"** — code actually **landed on `feature/bls-aggregate-sigs`** (S1–S5), not
  merged; absent from the current branch.
