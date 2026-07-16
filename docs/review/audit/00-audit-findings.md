# 00 — Audit Findings (Fable adversarial pass)

> **HISTORICAL PRE-FIX FINDINGS.** These findings are a snapshot of the
> 2026-07-07 tree, not proof of current behavior and not a design specification.
> Some defects and the code paths they cite have since been removed or changed.
> Reproduce and re-anchor a finding against current source before treating it as
> open. Current invariants are in [`../../../AGENTS.md`](../../../AGENTS.md),
> ADR-0016, and ADR-0017.

> **Current root-contract correction (2026-07-13):** every `SystemNamespace`
> active-address and expiry index is economic transition state and participates in
> `consensusMptRoot`; it is not a root-excluded sidecar. Field 32 is the only
> currently excluded stored partition. That is temporary containment:
> `globalSnapshotSyncView` is consumed by checkpoint replay without an exact
> signed/root-bound full-view witness, so `ECO-F32` is HIGH, CONFIRMED, and OPEN.
> This narrowly closes ECO-IDX-01/02, not ROOT-008/009 or full E9-01 projection.
> `WithdrawalTimeLimit` remains node-local while selecting rooted collateral-
> withdrawal expiry bytes, so ECO-IDX-03 is HIGH, CONFIRMED, and OPEN.

> **Status: Fleet complete (2026-07-07).** Eight adversarial Fable agents landed; every load-bearing
> `file:line` was re-grounded at source by the orchestrator before promotion. Branch
> `feature/committee-state-diff`, HEAD `21933559c`. **Board: 3 Critical · 13 High · 9 Medium · 2 Low ·
> 2 test-gaps** (+ a networking robustness cluster F4–F8 and 1 functional gap). Includes the
> audit-extension pass (serde / hardfork / networking). Headlines: **FINDING-S01** (flag-independent
> cross-shard double-spend via reorg) and **FINDING-F1** (dual-`Subscribe` race silently dropping ~half
> the shard checkpoints). Method: Fable sub-agents construct attacks; the orchestrator re-verifies at
> source before a claim becomes a finding; anything not re-verified is marked so.
>
> Extends — does not replace — the grounded packet (`docs/review/HANDOFF.md`, `01`–`23`). Cross-refs
> use those docs' anchors. Every `VERIFIED @ HEAD 21933559c` line was read by the orchestrator.

---

## 0. The resolved #1 question — where the enforcement teeth actually are

Three packet agents converged that gl0's happy path accepts on committee signature-quorum with no
re-execution. **Confirmed and sharpened against source:**

- Committee **attesters do NOT re-execute** before signing. `ShardCheckpointAttestationEmitter.emit`
  signs the checkpoint hash on a *became-best-tip* gate; the VRF is a membership proof, and the code's
  own scaladoc states "the threshold gate (which the producer passed) is **not re-asserted on the
  attestation path**" (`ShardCheckpointAttestationEmitter.scala:178-179`). *(The packet's `10-*` §3.3
  claim that this file "was not found" is wrong — it exists and confirms the hypothesis.)*
- gl0 **accepts on quorum-sig with no re-derivation**: `verifyEmbedded` →
  `if (distinctSigners >= kQuorum) ⇒ Accepted` (`ShardCheckpointGl0AcceptanceManager.scala:389-396`).
- **The synchronous per-node teeth are GAP-1** (binds each adopted field to the metagraph's OWN
  `stateProof`, `GlobalSnapshotAcceptanceManager.scala:1143-1193`) **+ the watchtower** (async,
  re-derives even on quorum — the closure is REAL in production, not a stub, see FINDING-002/W01).
  **Committee re-execution is NOT one of the teeth.**

So the token-model enforcement reduces to: **GAP-1 (bounded by the §4.2 Option-field hole + I-AUTH) +
watchtower (bounded by base-independence + the committee-exclusion gap)**. Every finding below attacks
one of those bounds.

---

## FINDING-001 — I-AUTH is a single signature; one Byzantine operator forges authoritative currency state for the whole hypergraph

- **Severity:** Critical
- **Category:** Security / Safety
- **Confidence:** High — the gate predicate, the env-map, and the adopt-without-re-derive anchor are all re-verified by the orchestrator.
- **Location (VERIFIED @ HEAD 21933559c):**
  - `StateChannelValidator.scala:187` — `case None => signedSC.validNec` (no allowance list ⇒ gate is a no-op).
  - `StateChannelValidator.scala:189-196` — `signedSC.proofs…find(peers.contains)`: **any single signer** in the allowance list passes; no threshold.
  - `StateChannelAllowanceLists.scala:21-25` — Dev/Testnet/**Integrationnet** all return `none` (allowance dimension absent → only a ≥1 seedlist signature remains); only Mainnet populates the map, at **1-of-3** per metagraph.
  - `GlobalSnapshotStateChannelEventsProcessor.scala:694-695` — `candidateBalances = authoritativeBalances.getOrElse(derivedBalances)`: gl0 **adopts** the metagraph-pushed balances, "NOT re-derived, in sharded mode" (its own comment); same for `authoritativeActiveAllowSpends`/`TokenLocks`/ref-maps.
- **Failure scenario:** On the `AdoptFromSignedFields` sharded path (`numShards>1`), a single Byzantine metagraph operator (holding one allowance-list/seedlist key) signs a `CurrencyIncrementalSnapshot` with attacker-chosen `authoritativeBalances` (or `authoritativeActiveAllowSpends`) and a byte-consistent `stateProof.balancesProof = hash(forgery)`. Every gate checks *internal consistency against the forged binary's own stateProof* — the committee re-execs the forgery (root matches), PIN-1 matches, GAP-1 binds to the forged proof, and the watchtower re-derives the forgery and sees no mismatch. gl0 adopts attacker-chosen currency state into its unified mirror, which is authoritative for cross-shard `SpendAction`/allow-spend/token-lock validation ⇒ value mint / cross-shard drain, undetected.
- **Drain vector narrowed (VERIFIED — G03):** the drain is via forging an **authoritative pushed field** (`authoritativeBalances`, or `authoritativeActiveAllowSpends` — the owner's active set), both adopted at `:694-695` siblings. It is **not** via replaying the cross-shard consume: the consume applies a **read-only** effective-balance overlay that never writes the `MgBalances` partition (`ConsumedAllowSpendStateManager.scala:46-64`: "The attested `MgBalances` partition is NEVER written … NO inflation"). So I-AUTH forgery of the *pushed* state is the mint; the consume marker is not itself an inflation surface.
- **Scope / why it's a precondition, not a live mainnet hole:** the adopt-without-re-derive path runs only at `numShards>1`. At the production default `numShards=1` gl0 re-derives, catching the forgery. So this is a **hard precondition for the sharding cutover** — shipping sharding without strengthening I-AUTH ships a trust *regression* vs mainnet re-validation, violating the non-regression floor.
- **Proposed direction:** replace the `≥1` predicate at `StateChannelValidator.validateStateChannelAllowanceList` with an **env-dependent operator-majority threshold** over the intersection of the binary's signers with the metagraph's registered operator key set (reuse `SignedValidator.validateSignedBySeedlistMajority`'s shape). Config via HOCON (`nakamoto.metagraph-source-authenticity`), threaded through `SharedValidators.make`; populate the Integrationnet allowance list. Route a sub-threshold snapshot to a slashable rejection reason. **This is the I-AUTH the packet flagged unbuilt; the stakeholder confirmed it a precondition.**

## FINDING-002 — Fraud-proof→slash is integrated for stake destruction but NOT for committee exclusion; two of three slash tiers are shelf-ware

- **Severity:** High (directly against the "fraud proofs must be part of the slashing mechanism" directive)
- **Category:** Security / Safety
- **Confidence:** High — closure-real and committee-exclusion-gap halves re-verified by the orchestrator; the gl0-stake-feedback half is agent-traced with line anchors (not independently re-walked end-to-end).
- **Location (VERIFIED @ HEAD 21933559c):**
  - **W01 — closure is REAL, not a stub:** `SharedServices.scala:321-351` wires `reExecuteDerivation = Some { … reExecDerivationWithDiff … }`; emitter built when `numShards>1 && watchtower-enabled` (`GlobalSnapshotConsensus.scala:1938-1950`, default on). The `:398-400` "fail-closed stub" comment is the `None`-fallback branch, not production.
  - **W03 — the gap:** committee draw pool = `seedlist minus metagraph-op` with **uniform σ=1/N** (`SharedServices.scala:303-307`, `ShardCheckpointWiring.scala:565`) — stake-independent; shard quorum is **count-based** `distinctSigners >= kQuorum` (`ShardCheckpointGl0AcceptanceManager.scala:389`). Neither reads the slash. The `Slashings` partition's only reader is the double-slash dedup (`InvalidStateProofSlashedReader.scala:88-93`); the per-operator cooldown gate is "a **future** … gate" (`:76`) — **unwired**. The manager scaladoc claiming the registry gates the active set (`InvalidStateProofSlashManager.scala:37`) is false at HEAD.
  - **W04/W05 — shelf-ware:** `ShardCheckpointEquivocationValidator`, `SlashableEvidenceValidator`, `ShardNonParticipationStateManager`/`Slasher` have **zero production construction sites** (agent grep; consistent with `22-*`/`14-*`). No detection emitter, ingress, pool, fold, or sink; the fieldId-24 counter is never written.
- **Failure scenario:** A committee member ships a wrong per-MG root, gets watchtower-slashed 100% (stake destroyed, gl0 leader-eligibility and gl0 attestation weight drop). But it is **redrawn into shard committees with identical probability** (uniform σ, slash not read) and its checkpoint signature still counts toward `kQuorum` — so a slashed Byzantine operator keeps attacking the exact surface (committees) the slash was meant to remove it from. Separately, equivocation and non-participation faults have **no consequence at all** (no detection even).
- **Proposed direction (epic):** (1) add a cooldown reader over the `Slashings` prefix and gate `committeeFor`'s `activeValidators`/filter (`ShardCheckpointWiring.scala:567-578`) to exclude `cooldownUntilEpoch > currentEpoch`; (2) wire the equivocation tier (evidence gossip topic + daemon handler mirroring `handleFraudProof` → pool → consensus field → accept-fold → `applySlash` with `SlashReason.{Checkpoint,Metagraph}Equivocation`); (3) wire non-participation (write the fieldId-24 counter on shard accept/attest paths; call `evaluateEpochBoundary` at the gl0 epoch boundary; route into `applyWatchtowerSlashes`).

## FINDING-003 — `lastMessages` is base-dependent inside the per-MG root, breaking the base-independence the watchtower relies on

- **Severity:** High
- **Category:** Safety (determinism / false-positive slashing)
- **Confidence:** High on the code-level base-dependence (crux re-verified); Medium on a guaranteed simultaneous two-node split (reachability of two honest nodes re-validating one past checkpoint at divergent live bases not fully traced).
- **Location (VERIFIED @ HEAD 21933559c):**
  - `GlobalSnapshotStateChannelEventsProcessor.scala:731` — `candidateLastMessages = authoritativeLastMessages.orElse(nextLastMessagesOpt)`. **Every sibling Option field** (`:704-705,706,725-730`) collapses to `None` via `committedProof.<field>.map(_ => next)` when the committed proof is `None` (base-independent); `lastMessages` **alone** falls back to `nextLastMessagesOpt` (a fold over the *live* prior `lastState.lastMessages`), so it can be `Some` when the committed proof is `None`.
  - `lastMessages` **is** in the PIN-1 root (`MgLastMessages`, fieldId 31; `GlobalStateKey.scala:332-341`).
  - The re-exec/watchtower rail reads the **live** base ignoring `executionBaseOrdinal` (`SharedServices.scala:327-331`, `363-367`) — the exact configuration `InvalidStateProofValidator` scaladoc prescribes *against* ("a watchtower that read its own live base would … false-slash an honest checkpoint whose base lags").
- **Failure scenario:** A metagraph sends its first `CurrencyMessage` at global ordinal P; `info.lastMessages` is `None` for ordinals `< P`. A watchtower re-validates a past checkpoint at ordinal `N < P`: a node whose live finalized tip is `≥ P` folds `lastState.lastMessages = Some` into the `MgLastMessages` leaf and computes a **different** per-MG root than the pinned-base attested (empty) root → raises a fraud proof against an **honest** committee → 100% slash of honest operators (and the slash write is itself in the consensus `mptRoot`). Conversely a Byzantine miss is possible. Couples to FINDING-002 (false slash) and FINDING-004/GAP-1 (this is the field GAP-1 also skips when empty).
- **Refinement (base-independence red-team — important; changes which rail is the live exposure):** the **watchtower fraud-proof rail is NOT the false-slash vector** it first appears. Its on-chain *verdict* re-derives at the **pinned** `executionBaseOrdinal` via `finalizedReaderAt` (base-identical to the producer), and the daemon pre-filters gossiped disputes by that pinned verdict — so a live-base emitter's spurious dispute is filtered, not slashed. The genuine live exposure of this base-dependence is the **sub-quorum `reExecPath`** (see **FINDING-B1**), which re-derives at the *live* base with **no** pinned backstop. The `lastMessages` base-dependence is real and load-bearing; it just bites through `reExecPath`, not the watchtower verdict.
- **Proposed direction:** collapse `lastMessages` to committed shape like its siblings — `authoritativeLastMessages.orElse(committedProof.lastMessages.map(_ => nextLastMessages))` — OR restore the pinned reader on **every** `liveReaderAt` site incl. `reExecPath` (honor `executionBaseOrdinal`, as the watchtower verdict already does). Add a determinism invariant/test: "any in-root field must collapse to the committed-proof shape." **General guardrail:** no in-root Option field may have a `lastState`-derived fallback that isn't gated on the committed proof.

## FINDING-004 — Attestation finality is 2/3 of *active* stake, not total → minority-partition fast-finalize → conflicting finalized history

- **Severity:** Medium-High
- **Category:** Safety (finality under partition)
- **Confidence:** High (denominator re-verified at source).
- **Location (VERIFIED @ HEAD 21933559c):** `TipTracker.scala:218` and `:258` — `stakeRegistry.optimisticRelativeStake(peerId)` ("active peers only"); threshold `isFinalized(_ >= FinalityThreshold)` (`:224-225`), `FinalityThreshold = 2/3` (`:131-136`).
- **Failure scenario:** Under a partition the active set shrinks, so 2/3-of-active can be a minority of total registered stake. A minority partition fast-finalizes its own branch via the attestation sink; on heal, each side's k₁ write-freeze floor (INV-SAFETY-001) refuses to reorg below its own finalized head → two partitions with conflicting finalized history → permanent fork needing rebootstrap. (This is the honest residual near the delay-cliff — a quorum *denominator* issue, distinct from the settled Taktikos synchrony assumption.)
- **Proposed direction:** require the attestation fast-path to weigh against **total registered stake** (or a floor fraction of it), so a minority-active partition cannot cross 2/3; OR gate the fast-finalize on an active-stake-vs-total-stake liveness check. Depth-k₁ remains the backstop, but the attestation sink must not be able to finalize a minority branch. *(Surface to the stakeholder — this touches the finality quorum; confirm it's implementation-correction, not a design change, before acting.)*

## FINDING-005 — Consensus-critical `FinalityThreshold` / `MaxAttestationSkewMs` are `sys.env` reads (cluster-split footgun)

- **Severity:** Medium
- **Category:** Safety (config divergence) / project-rule violation
- **Confidence:** High (re-verified at source).
- **Location (VERIFIED @ HEAD 21933559c):** `TipTracker.scala:131-136` — `FinalityThreshold` from `sys.env.get("NAKAMOTO_ATTESTATION_THRESHOLD")`; `:149-153` — `MaxAttestationSkewMs` from `sys.env.get("NAKAMOTO_MAX_ATTESTATION_SKEW_MS")`.
- **Failure scenario:** Two operators running different `NAKAMOTO_ATTESTATION_THRESHOLD` compute different `isFinalized` verdicts on identical attestation sets → divergent finality → fork. This is worse than the generic "9 sys.env reads remain" sweep item (`22-*`) because these gate *finality*. Violates CLAUDE.md's HOCON-over-sys.env rule for consensus-critical values.
- **Proposed direction:** migrate both to typed HOCON `nakamoto.*` fields with `${?ENV_VAR}` substitution in `application.conf`, threaded via `SharedConfig.nakamoto.*` — per the project pattern. Fold into the WAVE-2 HOCON sweep but prioritize these two.

---

# ADDITIONAL FINDINGS (Fable fleet — verified & promoted)

The eight-agent fleet has landed. Findings below are grouped by severity; combined with 001–005 the
full board is **2 Critical · 9 High · 8 Medium · 1 Low · 2 test-gaps**. The single most important new
result is **FINDING-S01** (a flag-independent cross-shard double-spend).

## FINDING-S01 — Cross-shard I-ONCE spent-set (fieldId 33) is wiped by every GSI-rebuild path → cross-shard double-spend + consensus-root fork
- **Severity:** **Critical** · **Category:** Safety (double-spend / inflation) + Consistency (root impurity)
- **Scope:** `numShards > 1`. **Flag containment:** NONE — independent of `band-density-reorg-enabled` and of `revertToOrdinal`; live in the always-reorgable `(k₁,head]` region and on plain restart.
- **Confidence:** High on mechanism (all four legs read at source); Medium on which manifestation dominates live.
- **Location (VERIFIED @ HEAD 21933559c):**
  - fieldId 33 `ConsumedAllowSpends` is **in the signed consensus root** — `GlobalStateKey.scala:291` + scaladoc `:283-289` ("`consensusRootEntries` keeps it … belongs in the consensus global `mptRoot`"), folded by `consensusMptRoot` (`GlobalSnapshotInfo.scala:388-390`).
  - `GlobalSnapshotInfo` has **no** field for 33 or 34 — full 18-field list `GlobalSnapshotInfo.scala:143-171` (the `activeAllowSpends` at `:149` is fieldId-7, the *owner* set, a different partition).
  - `syncFromGlobalSnapshotInfo` does `store.clear` (`GlobalStateConverter.scala:2134`) then `store.build` re-inserting only GSI-native partitions (`:2208`) → 33/34 never re-inserted.
  - The reorg self-heal calls it **ungated** (no `consensusMptRoot === signedRoot` re-check): `NakamotoSyncDaemon.scala:2136-2154` (`:2144`). Same ungated rebuild at reward-realign `:3366`, catch-up `:3499`, node restart `dag-l0/Main.scala:430/486/593`, download `Download.scala:523`.
  - Contrast: the **byte-faithful** catch-up preserves 33 verbatim and IS root-gated (`NakamotoSyncDaemon.scala:224-240`) — the reorg/restart paths were never migrated to it. The consume writes only the marker and never removes the allow-spend from the owner's active mirror (`AllowSpendConsumeHandler.scala:34-57`; gl0 must-not-mutate: `ConsumedAllowSpendStateManager.scala:46-79`).
- **Attack (no attacker needed for the wipe):** (1) cross-shard-consume an allow-spend at a finalized common-prefix ordinal → destination credited, fieldId-33 marker written, allow-spend **stays** in the owner's active set (owner is on another shard). (2) *Any* tip reorg in `(k₁,head]` — or a coordinated restart/download — rebuilds base via `:2144`, wiping the marker although step 1 is canonical history. (3) A second cross-shard spend re-consumes the same allow-spend: spent-set empty **and** allow-spend still active → both checks pass → **accepted** → the reservation pays out twice. Two manifestations: (a) root impurity → self-fork (certain when 33/34 ≠ ∅ at a rebuild); (b) double-spend when a quorum wipes together (what a common reorg causes) — commits cluster-wide before divergence is detectable.
- **Proposed direction (minimal — NOT Track-2):** route the reorg/restart/realign rebuilds through the existing byte-faithful `loadBytes` + `consensusMptRoot === signedRoot` gate so 33/34 survive verbatim; or add `consumedAllowSpends`/`slashings` fields to `GlobalSnapshotInfo`. **At minimum** gate these rebuilds on the root so they fail-closed (silent double-spend → detectable stall → Rebootstrap). This is the codebase's own `feedback_eliminate_globalsnapshotinfo` direction.

## FINDING-S02 — Watchtower `Slashings` ledger (fieldId 34) is wiped by the same GSI-rebuild paths → slash evasion
- **Severity:** High · **Category:** Safety (slashing integrity) · **Scope:** `numShards > 1` · **Confidence:** High (same verified mechanism as S01).
- **Location (VERIFIED @ HEAD 21933559c):** fieldId 34 `Slashings` consensus-root-load-bearing (`GlobalStateKey.scala:316` + scaladoc `:309-314`); absent from `GlobalSnapshotInfo` (`:143-171`); dropped by `syncFromGlobalSnapshotInfo` (`GlobalStateConverter.scala:2134-2208`), same ungated rebuild sites as S01.
- **Failure scenario:** a validator slashed via `InvalidStateProof` (a fieldId-34 record in the consensus root) has that record **erased** on any reorg/restart/catch-up that rebuilds from GSI → the slash is un-recorded on the rebuilt branch (evasion + retry surface) and the root diverges from nodes that retain it. Same fix as S01. Compounds FINDING-002 (the slash that *is* written is both un-consequential AND non-durable across a reorg).

## FINDING-E9-02 — Catch-up/realign Gate-2 rebuilds the state proof from the GSI alone → systematic rejection of every honest snapshot once fieldId-33/34 are non-empty (S01's liveness twin)
- **Severity:** High · **Category:** Liveness (systematic catch-up denial at the sharding cutover) · **Scope:** `numShards > 1` · **Confidence:** High (mechanism is a direct corollary of the verified S01 fact; end-to-end repro untested).
- **Location (VERIFIED @ HEAD 21933559c):** `verifyCatchUpSnapshot` Gate-2 uses `StateProofValidator.forGlobal[F](None)` (`NakamotoSyncDaemon.scala:185-187`) — "rebuild the state proof from the carried GSI (no producer ⇒ pure, no live-store access)" (`:183-184`) — and returns `RejectedStateProofMismatch` on any diff (`:190`). It is the only `forGlobal(None)` site in main. Consumers: reward-realign (`:3344→:3366`) and gossip catch-up (`:3445→:3499`). GSI has no fieldId-33/34 (`GlobalSnapshotInfo.scala:143-171`), both consensus-root-load-bearing (`GlobalStateKey.scala:283-291,309-316`).
- **Failure scenario:** at `numShards>1`, the first cross-shard consume or slash makes the signed `mptRoot` cover a non-empty 33/34 partition; Gate-2's GSI-only recompute can never reproduce it → **every honest snapshot thereafter is rejected**, so any node that falls behind can never deep-catch-up or reward-realign until the marker prunes. So the *gated* rebuild sites don't wipe (unlike S01) — they **wedge**. Worse, Gate-2's stated security argument ("the only state an attacker can install is bound to a validly-signed snapshot", `:151-154`) inverts — it rejects *honest* state — so operators are tempted to bypass the gate.
- **Proposed direction:** Gate-2 must compare against served signed bytes (`consensusMptRoot(bytes) === signed mptRoot`, the `seedMptByteFaithful` fast path `:224-240`), never a GSI re-encode; the GSI fallback (`:241-260`) is destructive-then-detect (clobbers the live MPT, *then* returns false) and must become fail-closed-before-write. Fold into EPIC-1.1 (this is why EPIC-1 must enumerate all rebuild sites, not just the ungated ones).

## FINDING-B1 — Sub-quorum `reExecPath` false-slashes an honest committee (100%) off a live-base re-derivation, with no pinned backstop and no empty-root filter
- **Severity:** High · **Reachability:** Medium (sub-quorum / degraded-liveness path only) · **Category:** Safety (honest-validator slashing + cluster split) · **Confidence:** High (crux read at source).
- **Location (VERIFIED @ HEAD 21933559c):** `ShardCheckpointGl0AcceptanceManager.scala:545-591`. `:556` calls `reExecuteDerivation(mg, snaps, gl0AnchorOrdinal, executionBaseOrdinal)` — but the closure uses `liveReaderAt`, which **ignores** `executionBaseOrdinal` and reads the node's live base (`SharedServices.scala:321-351`). On mismatch `:587` returns `RejectedReExecutionMismatch(firstReason, signers)` carrying the **entire committee** as the slash target. Unlike the watchtower path (`:422` filters `reDerived === Hash.empty`) it has **no empty-root guard**, and it does **not** pass through the pinned `InvalidStateProofValidator`. The comment `:552-555` asserts "byte-identical inputs ⇒ no-false-slashing" — unsound, because the live-base read + FINDING-003 base-dependence makes the inputs *not* identical across nodes at different live tips.
- **Failure scenario:** metagraph in its pre-first-message window; honest committee attests the empty-`lastMessages` root over `executionBaseOrdinal=B`. A gl0 node on the sub-quorum path with live tip `L>B` that has already mirrored the metagraph's first message re-derives a **non-empty** root → mismatch → slashes the **honest** committee 100%. Nodes at different live tips disagree on the slash → split.
- **Proposed direction:** `reExecPath` must re-derive at the pinned `executionBaseOrdinal` (swap `liveReaderAt` → `finalizedReaderAt`) and add the `Hash.empty` guard; better, route sub-quorum disputes through the same pinned `InvalidStateProofValidator` the watchtower verdict uses.

## FINDING-D02 — `fetchParent` is in-memory-only, making `compare` a function of node-local state → persistent cross-node split on deep forks
- **Severity:** High (precondition for flipping `band-density-reorg-enabled`) · **Category:** Safety/Liveness (fail-closed persistent split) · **Confidence:** High.
- **Location (VERIFIED @ HEAD 21933559c):** `NakamotoChainStore.scala:783-792` (`tipFor` reads `byHash` only, **no disk fallback** — unlike `getWithOrdinalFallback:477` / `walkBackTo:756`); young-chain prune drops non-canonical entries wholesale (`:714-719`).
- **Failure scenario:** two honest nodes holding different subsets of an alternate branch's lineage (gossip gaps, prune-timing skew, settled-marker skew) compute different MRCAs — one switches on density, the other gaps at depth > kLookback → `mrca=None` → `shouldSwitch` refuses (fail-closed). **Both behave "correctly"; the cluster stays split** until the gapped node backfills — and nothing on the fork-choice path requests it (refusal logged DEBUG-only).
- **Proposed direction:** give `tipFor` the disk fallback the other readers have; make an `mrca=None` refusal trigger a lineage backfill request (or at least WARN + metric). Blocks the band-flag flip.

## FINDING-D03 — `revertToOrdinal` (Track-3 S4 deep-revert executor) has zero production callers; a flipped band flag routes deep reorgs through the S01 GSI-rebuild wipe
- **Severity:** High (flag-flip precondition; inert-mechanism / FORK-006 pattern) · **Category:** Consistency · **Confidence:** High.
- **Location (VERIFIED @ HEAD 21933559c):** `grep '\.revertToOrdinal('` over `modules/*/src/main` returns **nothing** (only `MptOverlaySuite` + the trait/impl decl). Only its *disk reader* is wired (`GlobalSnapshotConsensus.scala:205/415`). The actual reorg executor is `NakamotoSyncDaemon.storeForkBranch:2100-2154`, which re-bases via `syncFromGlobalSnapshotInfo(:2144)` — the **S01 wipe path**. Flag default `false` (`application.conf:391`).
- **Consequence:** with the flag ON (as Track-3 intends), a density reorg moves bestTip below k₁ while **nothing reverts the overlay base** coherently — it falls into the S01 rebuild that drops 33/34. Exactly the reverted `86f390130` "inert mechanism shipped" pattern. **The flag must not be flipped until `revertToOrdinal` is wired AND S01 is fixed.** (Independently found by two agents.)

## FINDING-P01 — P is reconstructed from a node-local store whose walk-back silently truncates on any miss; join/rollback/downtime guarantee a miss → double-apply or divergence
- **Severity:** High · **Category:** Safety (quorum case) + Liveness (minority case) · **Confidence:** High on wiring; consensus post-refusal branch partially unverified.
- **Location (VERIFIED @ HEAD 21933559c):** `CurrencySnapshotCreator.scala:396-399` — `fetch(...) … case None => collected.pure[F]`: a pruned/missing snapshot is **indistinguishable from genesis**, no error/log. Store depth 50 (`application.conf:162`). `Rollback.scala:106-123` seeds one snapshot; `Download.scala:109-119` is forward-only, stores only the head.
- **Failure scenario:** node restarts via Rollback with head = a far snapshot; walk-back dies at the first disk miss → P window = 1 → P ∌ o for a still-in-U cross-shard ordinal o → the node re-applies o's SpendActions (double-credit) and refuses the majority artifact (`SnapshotDifferentThanExpected`). **Quorum variant:** a rolling restart / wiped-data redeploy (documented ops SOP) during a gl0-ingest lag makes a *majority* share the truncated window → o applied twice and **committed** cluster-wide; gl0 doesn't catch it (adopt path doesn't reproduce cross-shard spend effects, `GlobalSnapshotStateChannelEventsProcessor.scala:386-389`).
- **Proposed direction:** make the walk-back miss **loud** — fail-closed or backfill-from-peers to depth 50 on join/rollback, never silently genesis-equivalent.

## FINDING-P02 — gl0's Recreate fold and CL0's Download validation run P at window = 1 permanently (regression vs the pre-1a warm cache) → numShards=1 mirror freeze
- **Severity:** High · **Category:** Safety (mainnet-parity `numShards=1` non-regression bar) · **Confidence:** High on wiring; Medium on production trigger cadence.
- **Location (VERIFIED @ HEAD 21933559c):** the validator's `CurrencySnapshotCreator` is built with **no store** (`SharedServices.scala:213-220`, the `None` at `:216`) → window = 1 (`CurrencySnapshotCreator.scala:94-98,402-406`). Pre-1a this path had a warm 50-deep cache (`git show 8aa1de3dd^`, the deleted `globalSnapshotsAlreadyProcessed` `SignallingRef`). Consumers: gl0's legacy chain-link fold (`GlobalSnapshotStateChannelEventsProcessor.scala:349-363`, default `Recreate`) and ml0 join validation (`Download.scala:252-259`). The justifying comment `SharedServices.scala:204-205` ("numShards=1 ⇒ no fieldId-18 entry") is **unsound**: `metagraphSyncData` is populated by cross-**metagraph** SpendActions regardless of sharding (`MetagraphSyncManager.scala:270-272` filters `currencyId.isDefined`).
- **Failure scenario (numShards=1):** with view-lag between CL0 production and gl0 ingest, gl0 folds a metagraph binary at a recorded view where U still contains o while the committee's 50-window P already excludes it → recompute mismatch → `CannotCreateContext` → **permanent** rejection of that binary (all inputs pinned) → the gl0 mirror **freezes** for that metagraph, cascading into P03 loss.
- **Proposed direction:** restore a ≥50-deep (or in-band) window on the SharedServices/gl0 creator; fix the incorrect `metagraphSyncData=None` premise. **The DoD-floor finding that most needs an e2e repro.**

## FINDING-P03 — U silently drops the *newest* unapplied ordinals at the 100 cap → permanent, unflagged loss of cross-shard spends
- **Severity:** High · **Category:** Safety (permanent value loss) · **Confidence:** High (semantics verified).
- **Location (VERIFIED @ HEAD 21933559c):** `MetagraphSyncManager.scala:274-283` — `updated = currentOrdinals + newOrdinal; if (size > maxSize) updated.dropRight(size - maxSize)`. On a `SortedSet`, `dropRight` drops the **largest** elements → the ordinal being added *now* is discarded. Cap = 100 (`application.conf:252`); U only ever trimmed by GSP ingestion (`:221-222`), never re-added.
- **Failure scenario:** a metagraph M wedged (own consensus stalled, or binaries not ingested — e.g. the P02 freeze) while cross-shard spends into M keep being accepted at gl0. After 100 spend-carrying ordinals, every further ordinal o is added-then-dropped → never in any U snapshot → never applied, while the *source* already settled globally. No log, no metric. Recovery of M does not recover o.
- **Proposed direction:** drop-oldest-plus-alarm, or fail-closed at cap; the newest ordinal must never be the discarded one.

## FINDING-D04 — Shallow-vs-deep revert can diverge byte-wise (un-journaled base replacement; per-node deep-tier availability) — High once the band flag is wired
- **Severity:** High-when-wired (contained today by D03: no caller) · **Category:** Consistency · **Confidence:** High on the missing journal fence; Medium on a concrete seamless interleaving.
- **Location (VERIFIED @ HEAD 21933559c):** shallow replay requires every band ordinal in the journal (`MptOverlay.scala:1057-1058`); deep replay reads the exact-ordinal disk file (`:1094-1123`, fail-closed `RevertGapError`). Divergence 1: base-replacing ops (`syncFromGlobalSnapshotInfo`, `loadBytes`) neither write nor **fence** the undo journal → a later re-fold across a stale/fresh boundary yields a chimera state ≠ signed bytes; the contiguity gate checks *presence, not lineage-epoch*. Divergence 2: the deep-tier "contiguous to k₂" store is written only for hashes this node staged (`SnapshotLeaderLoop.scala:677-695`) → a catch-up-adopted node has holes → one node reads `Some(state)`, another gets `RevertGapError` for the same fork ordinal → split.
- **Proposed direction:** every base-replacing op must clear journal entries at-or-below its ordinal (a journal epoch fence); define a deep-tier-hole fallback before wiring the flag. Same "deep-fork cluster-uniformity sim" gate as D02/D03.

## FINDING-B2 — Leader-produce and follower-createContext validate fraud proofs against *different* bases (pinned vs live) → committee-controlled follower split
- **Severity:** Medium · **Category:** Safety (split) · **Confidence:** Medium (traced by the red-team; crux consistent with the verified `liveReaderAt`/`finalizedReaderAt` split, not independently end-to-end reproduced).
- **Location:** leader path uses the pinned `finalizedReaderAt`; follower `createContext` uses `liveReaderAt` (`SharedServices.scala:321-351` vs `:363-367`). A Byzantine committee can craft an attested root that is honest-over-live-base so the pinned produce upholds while the live createContext does not → followers split.
- **Proposed direction:** as B1 — both paths resolve at the pinned `executionBaseOrdinal`.

## FINDING-G01 — `lastMessages` injection for a message-free metagraph enables cross-metagraph address squatting + fee manipulation, no I-AUTH key required
- **Severity:** Medium · **Category:** Security (griefing / fee) · **Confidence:** High.
- **Location (VERIFIED @ HEAD 21933559c):** `StateChannelValidator.scala:52-62` (`getFeeAddresses` reads every metagraph's `info.lastMessages…values.map(_.address)`) → `:64-82` (`validateIfAddressAlreadyUsed` → `AddressAlreadyInUse` when another metagraph reuses an address). Because GAP-1 skips `lastMessages` when its stateProof is `None` (the message-free case), a quorum of lazy signers can inject owner/staking addresses the metagraph never signed — **no I-AUTH key needed**.
- **Effect:** injected addresses squat the cross-metagraph fee-address uniqueness map → other metagraphs' legitimate binaries rejected with `AddressAlreadyInUse`. No mint vector (owner/staking fields don't move balances); watchtower detects post-adopt but does not prevent.
- **Proposed direction:** subsumed by FINDING-001 threshold + closing the GAP-1 §4.2 Option-field hole for injected (non-empty) entries.

## FINDING-D01 — `densityCompare` is commutative on the MRCA-found path, but the `mrca=None` anchor is first-argument-biased and `shouldSwitch` runs two un-synchronized tine walks (TOCTOU)
- **Severity:** Medium · **Category:** Safety (commutativity / split) · **Confidence:** High on semantics; Medium on real-world race frequency (no e2e evidence).
- **Location (VERIFIED @ HEAD 21933559c):** `ChainSelection.scala:284-285` — when `mrca=None` the density anchor is `tineA.headOption` (the **first argument's** head), so the verdict is arg-order-dependent. `shouldSwitch` (`:164-165`) computes `compare` and `forkAncestorOrdinal` in **two separate** `buildTines` walks with no lock against concurrent `chainStore` mutation; if a parent arrives or an eviction lands between them, the switch executes on the asymmetric verdict. (The MRCA-found path is commutative — proven line-by-line and consistent with `ChainSelectionSuite.scala:119`.)
- **Proposed direction:** derive `(winner, mrcaOrdinal)` from a **single** `buildTines` result; the code already claims the two walks are "the SAME symmetric walk" (`:189-191`) — make that literally one execution.

## FINDING-P04 — P ⊉ U under a gl0-ingest stall > 50 CL0 snapshots → deterministic re-application every ~50 snapshots (admitted in-source, unmitigated)
- **Severity:** Medium · **Category:** Safety (deterministic double-apply) · **Confidence:** High on mechanism (documented in code); needs a sustained stall.
- **Location (VERIFIED @ HEAD 21933559c):** admission at `GlobalSnapshotOpsManager.scala:74-77` and `CurrencySnapshotCreator.scala:378-383`; window = 50; GSP emitted only when A non-empty (`CurrencySnapshotAcceptanceManager.scala:707-711`), so a processed-but-stuck ordinal's only in-chain record ages out at exactly 50 snapshots → o ∉ P again while o ∈ U → re-applied; all CL0 nodes agree (same window) → committed; gl0 doesn't re-derive economics. Repeats every ~50 snapshots for the stall.
- **Proposed direction:** emit a periodic GSP "keep-alive" for still-in-U processed ordinals older than window/2 so the in-chain record can't age out (fixes it in-band).

## FINDING-P05 — SpendActions for A are fetched by ordinal with no hash pin → fork-content sensitivity under Nakamoto gl0
- **Severity:** Medium · **Category:** Safety (proposal divergence) · **Confidence:** Medium.
- **Location (VERIFIED @ HEAD 21933559c):** `GlobalSnapshotOpsManager.scala:121-143` resolves each `o ∈ A` via `getGlobalSnapshotByOrdinal(o)`; only the **view ordinal's** hash is pinned (`CurrencySnapshotAcceptanceManager.scala:396-410`), not each fetched `o`. Under BFT gl0 ordinal→snapshot is unique; under Nakamoto gl0 `o ≤ view` is not necessarily `≤ finalized`, so two facilitators on different branches at `o` fold different SpendActions for the same A → artifact divergence.
- **Could not verify:** whether the view quorum is always at-or-below gl0 finality (if so, unreachable; no gate found). **Proposed direction:** pin each fetched ordinal by hash, or finality-gate the view.

## FINDING-P06 — Unhealable hash-file hole: recovery fallback writes ordinal-only and `write()` refuses to backfill the hash file → feeds P01's truncation
- **Severity:** Low-Medium (needs a race + a later restart) · **Category:** Safety · **Confidence:** High on mechanism.
- **Location (VERIFIED @ HEAD 21933559c):** `SnapshotStorage.scala:303-311` (recovery fallback is `writeUnderOrdinal` — "no hash file needed", skipped if a stale ordinal file exists); `SnapshotLocalFileSystemStorage.scala:47-49` (`write()` raises `UnableToPersistSnapshot` when the ordinal file pre-exists, so the retry loop never creates the hash file). In-process `hashCache` masks it; after restart, `get(hash)` disk-misses → the P walk-back truncates there for up to 50 snapshots.
- **Proposed direction:** allow `write()` to backfill a missing hash file even when the ordinal file exists; covered by making the P01 walk-back miss loud.

## FINDING-R01 — The follower `createContext` fraud-proof validator reads the LIVE base (not the pinned `executionBaseOrdinal`) over a base-dependent re-derivation → divergent slash → divergent `mptRoot` (verified form of B2)
- **Severity:** High · **Category:** Safety (consensus-root impurity — the 5-of-8 dominant class) · **Confidence:** High on the code asymmetry (verified at source); Medium on the exact fork manifestation (no numShards>1 run observed).
- **Blast radius:** whole-cluster at `numShards > 1` with `watchtower-enabled = true` (**default ON**, `application.conf:607`); inert at `numShards = 1` (`fraudProofs` always empty).
- **Location (VERIFIED @ HEAD 21933559c):** the follower `createContextInvalidStateProofValidator` is built with `liveReaderAt = (_: SnapshotOrdinal) => …GlobalStateReader.fromMptStore(storages.mptStore)` (`SharedServices.scala:363-367`) and the comment `:361-362` **explicitly asserts** "the re-derived root is base-independent" — the load-bearing wrong assumption. The gl0 produce/validate validator, by contrast, IS pinned (`GlobalSnapshotConsensus.scala:577-589`, `finalizedReaderAt`/`pinnedReaderAt(ord)`). The re-derivation is base-**dependent**: `deriveAdoptedCurrencyInfo` folds cumulative balances/active-sets onto the seed prior (`GlobalSnapshotStateChannelEventsProcessor.scala:483-512`), and `currencySnapshotMgRoot` is taken over that fold (`GlobalStateConverter.scala:1384-1389`). `fraudProofs` are threaded into `accept()` on every path (`GlobalSnapshotContextFunctions.scala:247-250`; re-validated `GlobalSnapshotAcceptanceManager.scala:2216-2249`).
- **Failure scenario:** gl0 (pinned) re-derives the dispute at `executionBaseOrdinal`, reproduces the attested root → **not upheld** → no slash → root R. A gl0/cl0/dl1/gl1 follower using `createContext` re-derives over its **live** base (ahead of `executionBaseOrdinal` for any historical checkpoint) → different root → `step7` **UPHELD** → slash written into fieldId-34 + stake maps → root R′ ≠ R → the follower cannot reproduce gl0's signed root (`StateProofMismatch`, the FORK-004/005 mirror-freeze shape). **This is B2, verified and upgraded to a root-impurity fork** — the divergent slash is *in* the compared root.
- **Compounding gaps (VERIFIED):** (a) `step7` treats a non-derivable MG (`reDerivePerMgRoot → Hash.empty`) as **UPHELD** (`InvalidStateProofValidator.scala:139-155`), whereas the node-local trigger deliberately filters `Hash.empty` as "can't check" (`ShardCheckpointGl0AcceptanceManager.scala:422`) — a follower that can't re-derive false-upholds. (b) No depth-k₁ window gate exists on the `accept()` fraud-proof loop despite the scaladoc claiming one — nothing bounds `executionBaseOrdinal` into the cluster-uniform finalized region.
- **Proposed direction:** all fraud-proof re-derivations (produce, createContext, sub-quorum reExecPath) must pin to the wire-carried `executionBaseOrdinal` (the reason it is signed into `ShardCheckpointSigPreimageV2`); a follower that cannot resolve that anchor must **fail-closed / skip the dispute**, never substitute a live base or `Hash.empty`-uphold. **This unifies with FINDING-003 / B1 / B2 into one epic: pin every per-MG re-derivation rail.**

## FINDING-R03 — Diff-base bounded-defer / fail-closed-drop are node-local reads with no `executionBaseOrdinal + k₁ ≤ N` floor → adopt-vs-defer can split at the finalization frontier
- **Severity:** Medium · **Category:** Safety-boundary / Liveness · **Confidence:** Medium (largely defended; the residual is a missing assertion).
- **Location (VERIFIED @ HEAD 21933559c):** `adoptShardCheckpoints` DEFERS the whole checkpoint when node-local `mptStore.lastPersistedOrdinal < cp.executionBaseOrdinal` (`GlobalSnapshotAcceptanceManager.scala:774,799-810`); `deriveAdoptedCurrencyState` FAIL-CLOSED DROPS an MG when `pinnedPriorInfoOf → None` (`:1096-1103`). Both change whether the currency advance lands in the compared `mptRoot`.
- **Why largely safe:** the k₁ margin closes the common case (a leader adopts only if finalized base ≥ `executionBaseOrdinal`, forcing `executionBaseOrdinal ≤ N − k₁`; an in-sync follower's finalized base ≈ N − k₁ also adopts). Defer/drop then only fires on genuinely lagging nodes, where defer→re-offer→self-heal is intended.
- **Residual (could NOT prove safe):** no explicit floor guarantees `executionBaseOrdinal ≤ N − k₁ − (finalized-tip jitter)`. A checkpoint whose `executionBaseOrdinal` sits at the finalization frontier + normal cross-node finalized-tip spread could split adopt-vs-defer at the same ordinal (the FORK-001 storm signature). **Direction:** add a monitored embed-time assertion `executionBaseOrdinal + k₁ ≤ N`.

## FINDING-R02 / ECO-F32 — field-32 is root-invisible but consumed by framework replay
- **Severity:** **High** · **Category:** Safety / deterministic execution · **Confidence:** High; source-confirmed and OPEN.
- **Location (current worktree, 2026-07-13):** field 32 is reconstructed into
  `CurrencySnapshotInfo.globalSnapshotSyncView` but excluded from the per-MG root and global
  `consensusMptRoot` (`GlobalStateConverter.scala:1362-1393,1691-1803`;
  `GlobalStateKey.scala:520-541`). Pinned peer backfill filters to `consensusRootEntries`, so it
  strips field 32 before persisting/reconstructing (`PinnedCurrencyInfoReader.scala:341-380`). GL0
  checkpoint replay then reads the prior `CurrencySnapshotInfo` and passes it to
  `processCurrencySnapshots` (`ShardCheckpointWiring.scala:263-314`). The signed incremental carries
  only `CurrencySnapshotStateProof.globalSnapshotSync` and accepted sync deltas, not the exact prior
  full-view preimage or explicit ML0 operator population (`currency.scala:52-61,96-114,222-240`).
- **Failure scenario:** node L has locally staged execution-base bytes containing a nonempty field-32
  view and reproduces the next currency state proof. Node B misses that base, accepts a peer map under
  the same signed GL0 root, strips field 32, reconstructs `Some(empty)`, and derives a different
  proof/root. L can sign while B refuses or disputes, preventing execution quorum or freezing that
  metagraph. Root exclusion prevents a direct global-root fork but does not make a consumed replay
  input reproducible.
- **Executable characterization (2026-07-16):** `ExecutionBasePinReExecutionSuite.scala:573-643`
  constructs those two readers at the same exact `GlobalSnapshotStateRef` using the production
  `PinnedByteBackfill` path. The full and stripped maps reproduce the same committed GL0 root, but
  local replay returns a currency root while the stripped/backfilled replay returns no result for the
  same signed binary. The suite also proves `None` and `Some(empty)` produce distinct
  `globalSnapshotSync` proof shapes. This confirms the failure mechanism; it does not close it.
- **Direction:** first bind an exact optional full-view replay witness and explicit ML0 operator
  population into the signed/root-bound framework artifact; preserve `None` versus `Some(empty)` and
  verify the witness against `CurrencySnapshotStateProof.globalSnapshotSync`. Missing material
  defers and cannot slash. Only then remove field 32 from every GL0 MPT/diff/load/reorg path while
  retaining it in ML0 `CurrencySnapshotInfo`.

## Note — cross-shard `emittedReceipts` are consensus-inert (dead accumulator / functional gap)
`consumeReceipts` folds receipts into `pendingCrossShardWrites` (`MetagraphSyncManager.scala:177-182`) but that `Ref` is **never read** by `acceptMetagraphSyncData` (`:102-130`); its only reader is the test inspector (`:288`). So `emittedReceipts` have no consensus effect today — the promised Slice-13 drain is unwired. This is a **functional gap** (cross-shard sync-data writes never land), not a fork (a never-read Ref can't diverge a root). Track as an unbuilt-mechanism item, not a safety finding.

*(This sweep also independently re-confirmed the fork-storm fix is intact in code: `validateArtifact` copies `stateProof.copy(smtRoot = None)` on both sides before `===`, `GlobalSnapshotConsensusFunctions.scala:286-295` — so TG-01 is a missing-**test** gap, not a code regression.)*

---

# NETWORKING (Go sidecar `p2p/` + gRPC boundary) — audit-extension, verified

The sidecar was not in the safety fleet's scope; this extension found two new consensus-load-bearing
bugs and corrected two stale packet claims. `numShards ≤ 1` is byte-identical (zero shard-topic joins,
`gossip.go:311`) — the risk is entirely in the `numShards = K` path.

## FINDING-F1 — Two `Subscribe` streams race-drain the single shared shard channel; the rumor bridge silently discards the shard checkpoints it wins
- **Severity:** Critical (`numShards > 1`; inert at ≤1) · **Category:** Safety (consensus-input loss) / wiring bug · **Confidence:** High on mechanism; Medium on the exact loss fraction (Go receiver-timing).
- **Location (VERIFIED @ HEAD 21933559c):** the JVM opens **two** `GossipStream.subscribe` streams — the rumor bridge (`GlobalSnapshotConsensus.scala:1252-1253` → `SidecarRumorBridge.receive`) and the daemon (`NakamotoSyncDaemon.scala:914`). `server.go:283` `Subscribe` drains the node-lifetime shared channels `ShardCheckpointMessages()`/`…AttestationMessages()` (`:300-301`), which return the **single** `shardCheckpointCh` (`gossip.go:75,:282,:690-691`). Go channel semantics deliver each message to exactly one of the two competing handlers. The bridge's stream keeps only rumors: `SidecarRumorBridge.scala:72` `.collect { case msg if msg.body.isRumor => … }` — everything else (shard checkpoints, attestations) is dropped, no log. The code's own comment states the hazard and wrongly assumes it away: `server.go:298-299` "Multiple concurrent Subscribe streams would race to drain these channels; in practice the JVM holds exactly one stream."
- **Failure scenario:** from boot (the genesis-checkpoint window), an order-half fraction of inbound shard checkpoints/attestations lands on the rumor bridge's stream and is silently discarded → shard quorum is harder to reach → the cluster leans on the pull recovery (F10) as an *unintended primary* delivery path. Universal topics (snapshots/attestations/rumors) are per-call subscriptions (fan-out), so they are unaffected — the loss is specific to the shared shard channels.
- **Proposed direction:** implement `SubscribeRequest.topics` filtering (the field exists on the wire, `sidecar.proto:404-407`; `server.go` ignores `req`) so the bridge subscribes rumor-only and the daemon takes the rest; or one shared demux that hands rumors off internally. Kills the race structurally. Must be landed **with** FINDING-F8 (bridge no-reconnect) — fixing either alone shifts the other's blast radius.

## FINDING-F2 — Watchtower fraud-proof transport is dead end-to-end; the enforcement tooth can't propagate
- **Severity:** High · **Category:** Safety (enforcement-teeth path; unbuilt slice presenting as built) · **Confidence:** High.
- **Location (VERIFIED @ HEAD 21933559c):** `grep -i fraud p2p/ --include=*.go` excluding generated `*.pb.go` → **zero** references: no fraud-proof topic in `gossip.go`, no relay, no `Subscribe` arm, no `PublishFraudProof` impl in `server.go` (it implements `PublishSnapshot/Attestation/Rumor/MetagraphBinary/MetagraphAttestation/AllowSpendBlock/DAGBlock/…` but not fraud-proof) → the call falls through to the `Unimplemented` embedding and returns `UNIMPLEMENTED`. The JVM emitter swallows it: `WatchtowerFraudProofEmitter.scala:110,125-128` `handleErrorWith { err => logger.warn(...) }` — one WARN, no retry, no outbox. The consumer arm `NakamotoSyncDaemon.scala:1037-1044` (`handleFraudProof` → pool → GSAM slash) is therefore **unreachable via the sidecar**.
- **Failure scenario:** a watchtower that detects an invalid state proof signs a fraud proof that **never leaves the node** — so it never reaches other nodes' `fraudProofPool`, is never embedded by a leader, and never slashes. This is the transport-layer complement to FINDING-002 (slash doesn't exclude) and B1/R01 (false-slash on live base): the watchtower tooth is broken at **propagation**, **consequence**, and **determinism** simultaneously. (Fraud proofs *do* re-validate when embedded in an artifact — R01 — but the detection→pool→leader hop that gets them embedded is exactly this dead gossip path.)
- **Proposed direction:** build the sidecar slice symmetric to shard-checkpoint (topic + relay + `Subscribe` arm + `PublishFraudProof` + **outbox durability** — a fraud proof must not be lossy); make the emitter fail loudly / outbox-retry, not warn-and-drop. Ties into EPIC-3.

## FINDING-F3 — ChainSync aggregate serve work remains unbounded across peer identities
- **Severity:** High · **Category:** Security (DoS) · **Confidence:** High.
- **Location (VERIFIED 2026-07-13; partially closed):** the undeployed range RPC/daemon is removed end-to-end. `handleIncoming` now rate-limits each libp2p peer before body reads and applies a 30-second stream deadline. Snapshot hashes, metagraph-binary hashes, and intersection points are capped at 64 in Go before JVM work; both hash streams are capped again in `ChainSyncServer`. The remaining defect is aggregate: peer identities have no protocol-pinned admission cost and there is no explicit global/per-peer in-flight stream or JVM-work budget, so many identities can still multiply the bounded unit of work.
- **Proposed direction:** add global and per-peer in-flight semaphores before allocation/JVM relay, rejection/occupancy metrics, and an admission identity/cost that cannot be reset cheaply by generating another libp2p identity.

## Robustness cluster (F4–F8, verified; details + tasks in `01-epics-and-tasks.md` EPIC-9)
- **F4 (High, G1):** inbound gossip crosses into the JVM through one `Queue.unbounded` (`GossipStream.scala:23`) with **no** gRPC flow-control — and (mechanism corrected vs the packet) the `onNext` offer is *non-blocking* fire-and-forget, so backpressure never reaches the wire; a slow consumer surfaces as unbounded **heap** growth. Fix = bounded queue + `request(n)`, landed together with F5.
- **F5 (Med-High, G2):** one serial `Subscribe` funnel (`server.go:304-463`) → head-of-line coupling across all 10 topic families; a JVM stall backs up every relay channel.
- **F6 (Med, G4, sharpened):** per-shard topics get zero peer-scoring (`gossip.go:1015-1025`), **and no `RegisterTopicValidator` exists anywhere**, so `InvalidMessageDeliveriesWeight:-99` is dead on *every* topic — the only live cross-peer penalties are IP-colocation + GRAFT. `Topic.SetScoreParams` is dynamic post-join (verified in libp2p-pubsub v0.15.0) — the fix is mechanical.
- **F7 (Med, G5):** the shard relay buffer is one fixed 256-slot channel shared across all shards (`gossip.go:282-283`), drop-on-full, no `numShards` scaling.
- **F8 (Med):** the rumor bridge never reconnects (`SidecarRumorBridge.scala:99-104` → `Stream.empty` on error/complete) — the first sidecar disconnect / mesh-recovery reset permanently kills inbound sidecar rumors on that node (the daemon, by contrast, reconnects, `NakamotoSyncDaemon.scala:1057-1067`). Interlocks with F1.

## Frontier note (F10) — packet-claim CORRECTED: pull-based shard chain-sync is LANDED, not design-only
Packet `23-*` §5 (and my launch prompt) said the pull recovery is design-only. It is **landed**:
`ShardCheckpointFetcher.scala` (`df5b7b0e0` T2 absence-pull, `d47a4403f` T1 orphan-pull), daemon wiring
`NakamotoSyncDaemon.scala:1175-1245`, serve routes `HttpApi.scala:305`. The pull rides authenticated
gl0→gl0 HTTP (bypasses the sidecar, immune to F1/F5/F7). So at HEAD the residual failure mode of
gossip loss is degraded **liveness** (shard progress leaning on pull), **not** the permanent divergence
the packet feared — three landed mechanisms break the drop→divergence chain (boot-grace, `pipeline-depth=1`,
`kQuorum=6/8`). Permanent divergence now requires *additionally* sustained pull failure or equivocating
attesters (whose slashing is detection-only). The real residual gap: no stability/boundedness **proof** of
the drop-vs-recover loop, and a pull **horizon** — `ShardChainStore` evicts below a keep-floor
(`ShardChainStore.scala:32,122,499`), so a node stalled past peers' retention 404s forever (no shard
deep-backfill). That proof + the horizon are the frontier task (EPIC-9 F-A).

---

# TEST-COVERAGE GAPS (feed the invariant→test matrix in `01-epics-and-tasks.md`)

## TG-01 — The June fork-storm fix (`smtRootBlind` consensus compare) had no direct regression test → **NOW CLOSED (authored, GREEN)**
- **Location (VERIFIED @ HEAD 21933559c):** `smtRootBlind` appears **only** in main (`GlobalSnapshotConsensusFunctions.scala:286-292`) — no test referenced the identifier. `smtRoot`-adjacent suites exist (`StateProofComparisonSuite`, …) but none asserted the consensus-`===` **blindness** that ended the storm. A regression re-opens ~97% content-validation forks.
- **STATUS — closed:** `SmtRootBlindValidateArtifactSuite` authored (worktree, untracked, no consensus code touched), **GREEN** both directions: an artifact differing only in `stateProof.smtRoot` passes `validateArtifact` (consensus-equal); one differing in `mptRoot` → `GlobalArtifactMismatch`. Deleting `smtRootBlind` (GSCF:286-292) flips it red — i.e. it pins the exact fork-storm regression. Ready to review + land.

## TG-02 — No `numShards=1 == numShards=K` byte-identity test existed → **base case NOW GREEN (authored); two adversarial legs remain**
- **Finding (was):** every "numShards=1 byte-identity" test compared *with-feature vs without-feature at numShards=1* — never 1-vs-K. The headline non-regression guard for the whole hard-fork did not exist.
- **STATUS — base case closed (GREEN):** `GlobalSnapshotAcceptanceManagerCrossShardCountByteIdentitySuite` (worktree, untracked) folds identical binaries through two GSAM instances — numShards=1 (raw events, real chain-link) vs K∈{2,4} (mock quorum-accepted `ShardCheckpoint`s, real `ShardAssignment` routing, only `verifyEmbedded` stubbed) → **byte-identical `stateProof.mptRoot`** under `MerklePatriciaFormat`, for genesis + chain-linked-child windows. Non-triviality pinned (currency state derived per MG; committee stub called per-checkpoint at K, never at 1).
- **⚠ TEST-HYGIENE FINDING (real, worth acting on):** the *existing* sharding suites (`GlobalSnapshotAcceptanceManagerShardingSuite` W3a, etc.) pin `GlobalStateProofSelector(Long.MaxValue) = LegacyFormat`, under which `stateProof.mptRoot` is **`None`** — so their "byte-identical mptRoot" assertions compare **`None == None`**, trivially green. Prior byte-identity coverage was **illusory**; TG-02 under `MinValue` (the production MPT regime) is the first real one. This is the PROC-16 "unvalidated instrument" class — flag it in the SDLC pass and re-pin those suites to the MPT era.
- **OPEN LEGS (still needed for the full DoD-2 gate):** (1) the `AdoptFromSignedFields` **2nd+ incremental** seam (needs the currency-l0 validator stack — not just genesis/opaque-child); (2) the **reorg + cross-shard-consume** path — that is precisely EPIC-1.4(a)'s S01 double-spend repro (**RED today**), and the reason TG-02's clean-fold GREEN does *not* contradict S01/P02: those diverge on paths this test doesn't exercise. Plus the 1-vs-2-shard e2e snapshot-hash diff.

---

## 9. Fleet complete — coverage ledger

All eight adversarial agents landed and their load-bearing claims were re-grounded at source. The
invariant→test matrix and the SDLC process catalog are folded into `01-epics-and-tasks.md` (DoD) and
`02-process-hardening.md` respectively.

| Investigation | Outcome |
|---|---|
| GAP-1 `lastMessages` fund/control effect | **G01** (address squatting, Med) promoted; drain vector via consume is **read-only** (G03) — narrowed FINDING-001. |
| Base-independence red-team | **B1** (reExecPath false-slash, High) + **B2** (produce/createContext base split, Med); refined FINDING-003 (watchtower rail is pinned-safe). |
| Spend-before-finality × band-revert | **S01** (Critical, flag-independent double-spend) + **S02** (High) + **S03**≡D03. The band-revert was *not* the hole; the GSI-rebuild wipe is. |
| Consensus-root purity sweep | **R01** (follower live-base fraud-proof verdict → root-impurity fork, High, verified — the sharp form of B2) + **R03** (no `executionBaseOrdinal+k₁≤N` floor, Med) + **R02/ECO-F32** (field-32 root-invisible replay input, High, confirmed, open) + inert-receipts functional gap. Re-confirmed `smtRootBlind` intact in code. |
| **Audit-extension: serde** | **E9-02** (catch-up Gate-2 GSI-only recompute → wedge, High, verified) + E9-04 (deleting the re-encode closes S01 *structurally*; EPIC-1.1 = slice-1 of GSI-elim; **strike EPIC-1.2**) + E9-03 (17 rebuild sites, not 6) + E9-05 (#23 closable now via `pinnedReaderAt` — the third base-pinning leg) + E9-01 (full `from(mpt, ordinal, era)` parity remains open). Current `GlobalStateConverter.scala:724-856,1265-1275,2920-2962` adds owner indices for `lastCurrencySnapshotsProofs` and `metagraphSyncData`; those are prerequisites, not a complete projection or independent every-field/absence-shape proof. → `01-epics-and-tasks.md` EPIC-9-SERDE. |
| **Audit-extension: hardfork** | H01 (cross-shard reachable via `gl0Local` — packet doc stale; verified) + H04 (roots-only deletes the state the teeth read → teeth blind; EPIC-4 prerequisite) + H02 (no activation-ordinal mechanism) + H03 (genesis-window + absence-proofs absent) + H05 (EPIC-1/2/3/4/8.1 confirmed as roots-only preconditions). → EPIC-9-HARDFORK. |
| **Audit-extension: networking** | **F1** (dual-`Subscribe` race drops shard checkpoints, Critical, verified) + **F2** (dead fraud-proof transport, High, verified) + F3 (ChainSync serve DoS, High) + F4–F8 robustness + F10 (pull recovery is LANDED — packet stale). → EPIC-9-NET. |
| densityCompare + k₂ deep-revert | **D01** (Med TOCTOU) + **D02** (High cross-node) + **D03** (High shelf-ware) + **D04** (High-when-wired). |
| P-window ⊇ U-window | **P01–P06** — inverted to a double-apply class; P02 hits the numShards=1 floor. |
| SDLC process forensics | 28 process findings, 8 root-cause classes, 11 guardrails → `02-process-hardening.md`. |
| Invariant→test coverage matrix | full matrix (35+ invariants) + top-8 gap ranking → `01-epics-and-tasks.md` DoD + TG-01/TG-02. |

---

## Cross-cutting observations (orchestrator synthesis so far)

1. **The teeth are GAP-1 + watchtower, and both bounds converge on the same weak spots.** I-AUTH
   (FINDING-001) undermines *both* (forge the source both bind to). `lastMessages` (FINDING-003) is the
   *single field* that is simultaneously base-dependent (breaks the watchtower) and GAP-1-skipped
   (breaks GAP-1). If one field had to be hardened first, it is `lastMessages`, and one gate,
   I-AUTH.
2. **"Built" ≠ "enforced end-to-end."** The watchtower closure is real (good) but its consequence
   doesn't reach the committee layer (FINDING-002); the slash is durable but never read by sortition.
   This is the packet's own "built the mechanism ≠ the invariant holds" pattern, now with anchors.
3. **The non-regression floor is the right frame for severity.** FINDING-001 and the adopt-without-
   re-derive design are safe at `numShards=1` and unsafe at `numShards=K` — i.e. the danger is exactly
   the *addition* sharding makes. Every Tier-0 finding is a sharding-cutover precondition, not a live
   mainnet regression — which is precisely why they gate the DoD. **Exception: P02 is a real
   `numShards=1` regression** (window 50 → 1 on the validator/gl0 creator), and **TG-02** shows no test
   would catch it — the floor is unprotected.
4. **One architectural seam explains four findings — and deleting it closes them at the type level.**
   S01 (nullifier wipe, ungated paths), S02 (slash-ledger wipe), **E9-02 (catch-up/realign wedge,
   *gated* paths)**, and the P02/P04 mirror freezes all trace to the same thing:
   **consensus-root-load-bearing state (fieldId 33/34) lives outside `GlobalSnapshotInfo`, so every path
   that rebuilds or re-derives state *from* the GSI either drops it (ungated) or rejects the honest
   snapshot that carries it (gated).** This is the 5-of-8-forks impurity class, now both a double-spend
   (S01) and a systematic liveness wedge (E9-02). The serde audit proved the fix is **structural**:
   eliminating the `syncFromGlobalSnapshotInfo` re-encode entirely (byte-faithful `loadBytes` +
   root-gate on *every* one of the **17** rebuild sites — not the 6 first listed) means no re-encode
   path can drop consensus-root state *by type-level absence*. So **EPIC-1.1 is the first slice of GSI
   elimination, not an independent fix, and EPIC-1.2 (add GSI fields) is struck** — it re-couples the
   root to GSI completeness, the opposite of the direction. This one change closes S01, S02, E9-02, and
   de-risks D03/D04.
5. **Live-base re-derivation of a base-dependent per-MG root is one defect on FOUR rails.** It is
   asserted-and-wrong in the code comments ("the re-derived root is base-independent",
   `SharedServices.scala:361-362` and `:327-331`). The four rails: **B1** (sub-quorum `reExecPath`
   acceptance), **R01/B2** (follower `createContext` fraud-proof verdict — the one that most clearly
   forks the root), **003** (`lastMessages` carry-forward), and **G01** (the squatting effect of the
   same field). The gl0 produce/validate verdict is the *only* pinned rail. Pinning **every** rail to
   the wire-carried `executionBaseOrdinal` (fail-closed when unresolvable, never `Hash.empty`-uphold)
   resolves all four — one epic (EPIC-4 in `01-epics-and-tasks.md`).
6. **Flag-gated landmines are real and interlocked.** D03 (revertToOrdinal unwired) + D02/D04 (deep-
   revert non-determinism) mean `band-density-reorg-enabled` cannot be flipped safely today, and if
   flipped it routes into S01. The decision-log gate on that flag needs its pass-conditions enumerated
   (done in `01-epics-and-tasks.md`).

### Verification ledger
Orchestrator-verified at source: 001, 002, 003, 004, 005, **S01 (all four legs)**, S02, B1, **R01
(the follower live-base wiring `SharedServices.scala:363-367` + the wrong "base-independent" comment
`:361-362`)**, G01, D03, P02, P03, plus the `revertToOrdinal`-no-caller and `smtRootBlind`-main-only
facts. Source-anchored by a careful agent with cruxes spot-checked (promote with stated confidence):
B2 (⊂ R01), D01, D02, D04, P01, P04, P05, P06, R02, R03.
