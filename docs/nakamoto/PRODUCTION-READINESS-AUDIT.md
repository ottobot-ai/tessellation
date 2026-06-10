# Production-Readiness Audit — Nakamoto Workstream

**Date:** 2026-06-09. **Method:** six parallel code-audit agents (sharding lifecycle, docs-vs-code, consensus core, state-flow/followers, git history, config/harness) + direct verification of every consensus-critical claim. Every item below carries file:line evidence; items the audits disagreed on were re-verified by hand.

**Purpose:** the single source of truth for *what is production-grade vs. placeholder* in the Nakamoto fork. Update this document whenever a slice lands or a shortcut is introduced — a shortcut without an entry here is a process violation.

---

## 1. The trust model — as designed vs. as built

The intended model (EXECUTION-SHARDING-COMMITTEE-VERIFY-DESIGN.md §7, lines 206-210):

> Leader **produces via execution** → committee members **verify via their own re-execution** ("a member that computes differently simply doesn't sign") → non-committee gl0 **adopts via quorum trust**.

As built (verified 2026-06-09):

| Step | As built | Status |
|---|---|---|
| Leader produces via execution | `ShardCheckpointProducer:341-351` VRF lottery; `derivePerMgState` → real `deriveMetagraphRoot` (`ShardCheckpointWiring:147-156`); signature binds roots via `signingPreimage` (`ShardCheckpoint.scala:72-81`) | ✅ matches design |
| **Members verify via execution before signing** | **NOT IMPLEMENTED on the normal path.** Member receives fresh checkpoint → `evaluate` → no quorum + no depth → `PendingMoreAttestations` (`ShardCheckpointGl0AcceptanceManager:252-261`) → admissible (`NakamotoSyncDaemon:101-107`) → member adopts, counts signers, **emits its own attestation after crypto pre-checks only** (`NakamotoSyncDaemon:2218-2267`). Re-exec fires only on the depth-degraded path (`:245-251`) and verifyEmbedded sub-quorum path (`:296-305`). | ❌ **GAP-1, CRITICAL** |
| Non-committee adopts via quorum | `verifyEmbedded`: 4 per-signer pre-checks + `distinctSigners >= kQuorum` (`:285-295`); sub-quorum → deterministic re-exec failover (wired `GlobalSnapshotConsensus:486`) | ✅ matches design |
| numShards=1 fallback | every gl0 re-executes via `createContext` + stateProof gate; invalid binary **rejected** (`CurrencySnapshotContextFunctions:31-55`, GSCEP `~:736`) | ✅ real gate |

**Consequence of GAP-1:** quorum is formed from attestations that were never execution-backed (only the leader executed). A malicious/buggy leader + crypto-valid co-signers turns kQuorum into effectively 1-of-N for state correctness. This is the top soundness fix.

---

## 2. CRITICAL punch list (consensus soundness — block production)

| # | Gap | Evidence | Fix |
|---|---|---|---|
| 1 | **Members attest without re-execution** (normal path) | NakamotoSyncDaemon:101-107 (`PendingMoreAttestations => true`), :2218-2267 | Member-side mandatory re-exec before own attestation emit: re-run `deriveMetagraphRoot` over `includedSnapshots` at `gl0AnchorOrdinal`, byte-compare vs `perMetagraphMptRoots`, sign only on match; mismatch = slash evidence. The deterministic primitive already exists (`ShardCheckpointWiring.reExecDerivation`). |
| 2 | **VRF committee check is structural-only** (length ≥ 80 bytes, no crypto) | ShardCheckpointGl0AcceptanceManager:526-530 | Slice 13: real `CommitteeSortition.verifyMembership` against registered VRF-VK. Mitigated meanwhile by the seedlist-derived membership SET check (:338), but sortition unpredictability is not cryptographically enforced. |
| 3 | **Slashing is detection-only** | `SlashableEvidenceValidator` complete (9 steps, step 7 already-slashed = `neverSlashed` stub); equivocation validator built but **uncalled**; no consequence path (stake/eviction), no SlashableEvidence tx consumer | Wire the sink: evidence tx type → GSAM consume → stake effect. Both per-epoch-committee safety (SHARD-SORTITION plan) and EPOCH-PARTICIPATION demotion are gated on this. |
| 4 | **KES registry-absent carve-out** — missing registry entry ⇒ accept on Ed25519 alone | KesGossipVerification:58-65; ShardCheckpointGl0AcceptanceManager:502-506 | Close after Slice-10 registration is universal: registry-absent ⇒ reject (or bounded bootstrap window with alerting). |
| 5 | **KES in-memory SecureStore fallback** — no `CL_KES_SECURE_STORE_DIR` ⇒ in-memory keys + warn-only verification seams | GlobalSnapshotConsensus:827-834 | Production requires disk-backed store; make the in-memory path fail loudly outside dev. |
| 6 | **Prior-state for shard re-exec is EMPTY** (S3.1) — roots are prior-agnostic, not accumulated-state commitments | SHARD-SORTITION-WORKSTREAM-PLAN §3.1; COMMITTEE-SORTITION §8 Q2 | Decide + implement S3.1 (finalized prior at `gl0AnchorOrdinal`) — prerequisite for GAP-1's member verify to be a *true* state check and for fair slashing. |

## 3. HIGH punch list

| # | Gap | Evidence | Fix |
|---|---|---|---|
| 7 | **Consensus params still sys.env** (Wave-2 HOCON migration): `NAKAMOTO_ATTESTATION_THRESHOLD` (TipTracker:132), Snowball β/K/α (SnowballAccumulator:101-122), LDD quartet + `SLOTS_PER_EPOCH` (GlobalSnapshotConsensus:709-723), `GENESIS_TIME_MS` (:105), `OPTIMISTIC_MIN_FRACTION` (StakeRegistry:102), `SLASH_EVIDENCE_WINDOW`, rebootstrap knobs. **Worst:** `NAKAMOTO_SLOT_DURATION_MS` and `NAKAMOTO_ARCHIVAL_DEPTH` are read **at runtime** (SnapshotLeaderLoop:680, :869) — mid-run drift possible. | config/harness audit §1-2 | Migrate all to `nakamoto { ... }` typed config per CLAUDE.md rule; hoist runtime reads to startup. ~19 violations total. |
| 8 | **gl0 balance mirror staleness feeds consensus**: `SpendActionValidator` ← mirrored `si.balances` (GSAM:1674-1677); token-lock deltas ← `activeTokenLocks` (TokenLockStateManager:560/587); metagraph allow-spends (:1779). Per-field adopt keeps these last-VERIFIED but they LAG when gl0 can't reproduce balances (sharded). | ROOTS-ONLY §3.4 (updated 2026-06-09) | Roots-only reshape with the Slice-2 sequencing constraint: spend-actions go proof-carrying vs committed `R_M` **before/with** fold deletion. User direction: lag at most parent context — satisfied by proof-vs-fresh-root reads, not by mirror improvement. |
| 9 | **ml0 smtRoot verify is best-effort** — proof absent/fold fails ⇒ "adopting anyway" (mptRoot-only) | currency-l0 StateChannel:433-444 (task #20) | Make the SMT verify enforced once gl0 store wiring + retention (≥ k window) is confirmed on every node. |
| 10 | **RebootstrapOrchestrator default-OFF** — divergent-self-finalize deadlock has no automatic recovery | RebootstrapOrchestrator:99-103 | Validate in e2e (no spurious fires), then default-ON. |
| 11 | **Stake-concentration cross-shard bound** (α_total > 1/(2S) breaks per-shard honest majority) | GKL-COMPOSITION §5.2 | Chosen mitigations = sortition (landed) + slashing (item 3, pending). Bound must be revisited once committees rotate per-epoch; document the operating envelope (N, S, kDraw, kQuorum). |

## 4. MEDIUM (production-hardening)

- **maxvalid-tk comparator DUPLICATED** in ShardChainStore (`compareMaxvalidTk`, replicated because `ChainSelection.standardCompare` is private — ShardChainStore.scala:34-37). Consensus-critical algorithm in two places = drift risk. Extract one shared pure comparator; both call it. (ShardFinalityTriggers correctly REUSES the `FinalityTrigger[F]` typeclass — only the comparator was duplicated.)
- **Shard attestation quorum is not ancestry-transitive** (the GRANDPA half that wasn't carried over): an attestation for checkpoint C counts only toward C's hash, so a 1-slot fork at any ordinal can split quorum below kQuorum permanently even after maxvalid-tk converges (the 2026-06-10 genesis wedge's first domino). Fix: count an attestation toward the checkpoint AND its stored ancestors (pure function of attestation set + parentCheckpointHash ancestry; no timing, no re-emission — preserves the one-shot attestation rule adopted after the multi-emit fork bug). Pairs with GAP-1 (same emit/record seams). The ancestor-first embed selection (038c9b1d4) already heals adoption via the re-exec rail; this completes the fast path.
- **Empty-window / receipts-only checkpoints have no adoption progress marker** — ancestor-first selection skips them (none produced today); needs an adopted-checkpoint marker in GSI before T_alive/receipts-only checkpoints land.
- **SHARD CADENCE INVERSION (user design intent, 2026-06-10):** as built, the shard slot clock IS the gl0 ordinal (`slotForGl0Anchor(ord)=Slot(ord)`; produce fires once per canonical gl0 ord via the fan-out) — one lottery draw per shard per gl0 snapshot, LDD-stretched to ~1 checkpoint per 5-8 ords, i.e. shard chains run SLOWER than gl0. The intended model is INVERTED: shards on their own (finer, offset) slot clock with the domain-separated VRF (the `shardEta` half already exists), multiple finalized checkpoints committed per gl0 snapshot. Measured cost of the current shape: mg→gl0 state visibility 10-43 epochs (the allow-spend window race). Workstream = three coupled pieces: (1) decouple the shard slot clock (LDD re-tune at the faster rate; REQUIRES ancestry-transitive quorum first — faster slots = more committee forks = the genesis attestation-split failure mode), (2) the NEL batch embed below, (3) transitive-quorum verify + sidecar BLS (#30) to keep verification O(1)+hash-links and attestation traffic flat. Option C's "per gl0 ord" default + never-built T_burst hatch is superseded by this direction.
- **Catch-up is 1 checkpoint/shard/ordinal** (`shardCheckpoints: SortedMap[ShardId, ShardCheckpoint]`) — since production is also ~1/ordinal, a post-outage backlog drains only via slot gaps: bounded but STICKY lag. Batch catch-up = widen the embed to a per-shard chain (greenfield wire change), adopt parent→child with the anchor guard's tips THREADED between elements (sequential composition is the safety-critical detail; still a pure function of embedded list + prior GSI). **Wire type = evidence-carrying newtype, NOT a bare NEL** (user directive 2026-06-10): `ShardCheckpointChain` with a private constructor + validating factory (parent-linkage, strictly-increasing shardOrdinal, single shard, anchor ≤ bound) so a chain-hole / out-of-order batch / future anchor is a DECODE failure at the wire boundary, not a runtime branch — parse-don't-validate, same idiom as `Signed`/`Hashed`. The original chain-hole adoption was possible precisely because the bare map carries no linkage evidence. Verify via TRANSITIVE QUORUM — the quorum'd descendant's signatures bind the whole ancestry through `parentCheckpointHash`, so ancestors need hash-link checks, not K re-executions (cheaper AND stronger than per-ancestor re-exec; same endorsement argument as ancestor-first selection). Cap batch size (config) to bound proposal-validation cost. Sequence with the transitive-quorum/T_count work — shared mechanism.

- **Equivocation validator + ShardNonParticipationCounter built but unwired** (no callers; Slice 16/17 tallying + sink) — folds into item 3.
- **cl1 resync-to-canonical trust gap**: mptRoot recompute-match only; served GSI internals not fully re-verified (CurrencySnapshotProcessor:157-223).
- **changeset ring capacity vs finality window** unvalidated (`changeset-ring-depth=1024` HOCON; no sizing analysis vs ml0 catch-up).
- **`k` (confirmationDepthK) gl0/ml0 agreement not asserted** — config drift = split risk (currency-l0 StateChannel:75-77).
- **`NAKAMOTO_COMMITTEE_K_TARGET` is a dead knob** in compose (superseded by kDraw/kQuorum) — remove.
- **MetagraphCommitteeGate legacy deletion (S5)** of EXECUTION-SHARDING plan not executed.
- **Per-field adopt mismatch monitoring**: WARN exists; needs a metric + alert (mirrors slice-19 pattern).
- **Hard-coded catch-up constants** (NakamotoSyncDaemon:54-65) — no operational lever.
- **Proof-of-absence** not implemented in proof services (v1 membership only).

## 5. What is genuinely production-grade (verified)

- **Consensus arithmetic determinism**: all `Ratio`/BigInt, no Double, Hasher[F]-routed; VRF eligibility ENFORCED at gossip + validation; LDD exact.
- **Fork choice** (maxvalid-tk/bg + attestation orthogonal) and **finality triggers** (T_count/T_weight-Snowball/T_depth1/T_depth2) — enforced, monotone, validated at 8gl0 scale.
- **Catch-up admission gates** (envelope sig + stateProof rebuild) — enforced; the task-#9 fix.
- **numShards=1 path**: full re-execution gate, byte-identical regression bar, invalid snapshots rejected.
- **Follower adopt-verify family**: ml0 changeset adopt (mptRoot recompute-match), cl1/dl1 follow-slice (field-root equality vs locally-anchored finalized GSI), resyncToCanonical recovery — split-safe by construction.
- **LC-SMT light-client stack**: `LightClientSmt` (cross-lang KAT vs `@zk-kit/smt` proven), `BalanceMpt`, ShardProofRoutes/ShardSubtreeProofService wired (v1 membership proofs).
- **KES enforcement when registry present**; equivocation evidence crypto; slashing validator steps 1-6, 8-9.
- **gl0 currency mirror (per-field + expiry + syncView)**: never commits an unverified field; validated live (c8849188a).

## 6. Documentation status (condensed; full table in docs audit)

- **CURRENT / load-bearing**: attestation-and-finality.md, SYNC-PROTOCOL.md, HIERARCHICAL-SHARD-CHECKPOINTS (schema parts), COMMITTEE-SORTITION-DESIGN.md, TAKTIKOS notes.
- **DESIGN-AHEAD-OF-CODE (label them)**: EXECUTION-SHARDING-COMMITTEE-VERIFY (§7 member-verify contract NOT implemented — GAP-1), SLASHING-DESIGN (no consequence path), KES10-WAVE-2 (intake wired, MPT write + receiver-side registry missing), EPOCH-PARTICIPATION (machinery unwired), BLS designs (code on `feature/bls-aggregate-sigs` branch, mainline blocked on BC 1.85 stable).
- **SUPERSEDED markers needed**: HIERARCHICAL-SHARD-CHECKPOINTS §6/§12 (superseded by EXECUTION-SHARDING — header note missing); CODEBASE_MAP §kernel (Cell is vestigial — link CELL-CONSTRUCT-ASSESSMENT).
- **#259 family**: three docs give three diagnoses; the operative one is fold-vs-shard-tip divergence; ROOTS-ONLY (updated 2026-06-09 with consensus-critical reader rows + Slice-2 sequencing) is the fix-of-record.
- **MISSING specs for major code**: state-field registry (25+ GlobalStateFieldId), snapshot-leader-loop lifecycle, attestation aggregation/TipTracker, MPT MultiBranch overlay, stake registry (delegated+collateral, N-2 freeze).

## 7. Recommended order of work

1. **GAP-1 member verify-before-attest** (small, primitive exists) + S3.1 prior-state decision — restores the designed trust model.
2. **Roots-only Slice 2** with the reader-reshape sequencing (spend-action proof-carrying) — kills the mirror lag + the #259 family + chain-link flake structurally.
3. **Slashing consequence sink** (unblocks per-epoch committees + demotion; evidence machinery already built).
4. **Slice 13 real VRF sortition verify** + close KES carve-outs.
5. **Wave-2 HOCON migration** (mechanical; do alongside).
6. Doc supersession sweep + the missing-spec docs.
