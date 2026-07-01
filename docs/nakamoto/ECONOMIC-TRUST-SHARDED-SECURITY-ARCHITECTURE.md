# Tessellation/Nakamoto — Shared-Security Sharding Architecture

**Status:** Design + critique (rev 2, incorporates stakeholder direction + Codex code review). STOP for review before implementation.
**Target:** general **multi-shard** shared security; **2-shard (2mg/2shard)** is the validation/test config, not the architectural ceiling. Greenfield — nothing ships until the full wiring is running and testable.
**Baseline:** branch `feature/committee-state-diff`, HEAD `86f390130`. Production-default `num-shards = 1` (legacy universal re-execution, byte-identical to mainnet, safe). Sharding (`num-shards>1`) is gated off (`application.conf:456`; `ShardCheckpointWiring` inactive at `numShards<=1`, `:425`).

> **CORRECTION (2026-06-30):** Much of the "status today / MISSING / stub / schema-only / logs-and-drops / unwired / dark" framing below is now STALE. As of commits `8ac7ce04f..0b7902d69` the watchtower fraud-proof (an ALWAYS-RUN `watchtowerReExec` does the per-MG re-derivation while `verifyEmbedded` admits on quorum; `FraudProofEnvelope`/`fraudProofs` is a real consensus artifact), the durable invalid-state-proof slash (`Slashings` fieldId 34, `InvalidStateProofEvidence`/`InvalidStateProofSlashedReader`), the cross-shard single-use spent-set (`ConsumedAllowSpends` fieldId 33, wired at `numShards>1`), PIN-1 component-addressable per-MG roots, real committee-VRF membership verify (`verifyVrf`; `verifyVrfStructural` is now only the registry-absent fallback), and the proof transport ARE BUILT — not "schemas and stubs." The checkpoint DOES carry the executed snapshots — as `ShardDerivedStateDelta.includedSnapshots` (not a missing top-level `Signed[CurrencyIncrementalSnapshot]` field). TWO model corrections also supersede the text below: **(1)** the authoritative-balance push is being REMOVED as the primary adoption path — the currency-app **token model is RE-EXECUTED and enforced** (`reExecRoot === stateProof`); only arbitrary state channels + the data-app sub-state stay authoritative. **(2)** "Revertible / usable at k1" was specced but NEVER implemented — the decided model is **DEFER-NOT-REVERT**: spendability is deferred until `approvals ≥ kApprove OR depth ≥ k1`; only the slash is wired, no state-revert exists. See CURRENCY-APP-TOKEN-ENFORCEMENT.md + SHARDING-PRODUCTION-READINESS-PLAN.md.

> **EM verdict (rev 2).** The settled model is Polkadot-style shared security and it is sound. The **central correctness mechanism is the stakeholder's watchtower fraud-proof**: the committee ships the byte-diff **and** the `Signed[CurrencyIncrementalSnapshot]` (which embeds the metagraph's own `stateProof`); any verifier holding the metagraph's prior state re-executes `accept()` and checks **execution output === the shipped stateProof**, and a *single honest verifier* can submit a fraud proof that slashes the committee. Trusters adopt the diff. This is fraud-proof + data-availability + approval-checking in one mechanism, and it is a well-established pattern (Polkadot approval-checking, optimistic-rollup fraud proofs, Lightning watchtowers). With it, **committee security no longer rests on a ⅔-honest committee** — it rests on "1-of-N honest verifier" — so the committee can stay small and predictable-per-epoch (the deliberate sortition design). ~~The wiring for all of this exists today only as **schemas and stubs** (`verifyEmbedded` admits on quorum with no re-execution; `FraudProofEnvelope` is schema-only; proof transport is noop; no cross-shard spent-set).~~ **[STALE — now BUILT (8ac7ce04f..0b7902d69):** the always-run `watchtowerReExec` does the re-derivation (`verifyEmbedded` admits on quorum, but the dispute path re-executes); `FraudProofEnvelope`/`fraudProofs` is a real consensus artifact; proof transport is wired; the `ConsumedAllowSpends` (33) spent-set is wired; the upheld dispute writes a durable slash (`Slashings` 34). **The remaining build is re-exec-primary** (remove the authoritative override) **+ defer-not-revert spendability** — see CURRENCY-APP-TOKEN-ENFORCEMENT.md + SHARDING-PRODUCTION-READINESS-PLAN.md.**]** The plan below was authored against the pre-build state.

---

## 1. Executive Summary

**The model.** Per metagraph, a **VRF-sortitioned shard committee** of gl0 validators re-executes the metagraph's currency `accept()` once at the *finalized* base and emits a `ShardCheckpoint` = byte-diff + per-MG root + committee signatures **+ [NEW] the `Signed[CurrencyIncrementalSnapshot]` it executed**. Every gl0 node applies-and-verifies the diff against the attested root. Metagraph logic reads global/cross-shard state **pinned** to a finalized `globalSyncView` anchor (never live). Correctness is guaranteed not by trusting the committee but by the **watchtower layer**: any node with the metagraph's prior state re-executes and disputes a wrong root within a challenge window.

**Trust boundary (refined per stakeholder).** Within a single metagraph there are two state classes with *different* guarantees:
- **Token / economic transitions** (transfers, fees, allow-spends, token-locks, balances, supply) — these are **protocol primitives a data-app cannot redefine**, so they are **RE-EXECUTABLE and verified** by gl0 + watchtowers. This is the hypergraph-guaranteed economic core.
- **Data-application custom state** (`getCalculatedState`, app records, data-with-fee app effects) — gl0 cannot run arbitrary metagraph code, so these are **adopted AUTHORITATIVELY** and *explicitly labeled* as metagraph-sourced. "The metagraph signed this," not "this is correct" — and that is acceptable *because it is the data layer, not the economic layer.*

**Authenticity (env-dependent).** A committee must not be able to launder an invalid metagraph snapshot. The metagraph snapshot's signature requirement is **environment-scoped**: **dev/testnet** (operator keys not registered) → count-based (≥1 allowance-list signature, today's behavior); **integrationnet/mainnet** (registered metagraph operator keys) → **a majority/threshold of the registered metagraph operator set**. A committee that pushes a snapshot not signed by a metagraph majority is then *detectable and slashable by a single honest verifier*.

**The 5 things that make the guarantee real (status today):**
1. **Watchtower fraud-proof + challenge window** — ~~`verifyEmbedded` admits on `kQuorum` signatures with **no re-execution**; `FraudProofEnvelope.scala` is **schema-only**; mismatch currently logs/drops. **MISSING — the core build.**~~ **[STALE — BUILT.** `verifyEmbedded` admits on quorum, but a separate ALWAYS-RUN `watchtowerReExec` (`ShardCheckpointGl0AcceptanceManager.scala`) re-derives every MG's per-MG root via the same PIN-1 closure; `FraudProofEnvelope`/`fraudProofs` is a real consensus artifact; an upheld dispute writes a durable slash (`Slashings` 34, `InvalidStateProofEvidence`). The residual is making re-exec the *primary adoption gate* (today the authoritative override masks it) — see CURRENCY-APP-TOKEN-ENFORCEMENT.md.**]**
2. **Checkpoint carries the executed snapshot** — ~~`ShardCheckpoint` carries diff + roots + sigs, **not** the `Signed[CurrencyIncrementalSnapshot]`. **ADD it**~~ **[STALE — BUILT.** the checkpoint carries the executed snapshots as `ShardDerivedStateDelta.includedSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]`; the watchtower/verdict re-derives the per-MG root from these. This is the DA mechanism, in place.**]**
3. **Env-dependent metagraph-majority signature** — gate lives at `StateChannelValidator.validateStateChannelAllowanceList` (`:167`); today ≥1 signature. **Make the threshold env-configurable.** *(Still TO BUILD — I-AUTH; accurate.)*
4. **Cross-shard single-use** — ~~only needed for cross-*metagraph* spend consumption (§5); no global spent-set today. **ADD conditionally.**~~ **[STALE — BUILT.** `ConsumedAllowSpends` fieldId 33 is wired (producer writes the spent-marker at the gl0 fold, consumer reads the absence-check) at `numShards>1`.**]**
5. **Pinned global-state everywhere** — held on the cl0 currency path (`forcedGlobalSyncView`); **violated** on the gl0 mirror-expiry path (R1, live bug) and the L1 data-app path (R4). **FIX both** (R4 = pin to the cl0 anchor, per stakeholder).

Committee unpredictability is **NOT** on this list — see §4.

---

## 2. Trust Model

### 2.1 The boundary (refined)
| State class | Authority | Mechanism |
|---|---|---|
| **Token/economic transitions** (transfer, fee, allow-spend, token-lock, balance, supply) | **HYPERGRAPH-GUARANTEED — RE-EXECUTED + verified** | committee re-executes `accept()` once; gl0 applies-and-verifies the per-MG root; **watchtowers re-execute and dispute** a wrong root |
| **Data-application custom state** (`getCalculatedState`, app records) | **METAGRAPH-AUTHORITATIVE (labeled)** | gl0 cannot run custom code; adopts the metagraph-signed value, labeled `authoritative*`; authenticity via the env-dependent metagraph-majority signature |
| **Delegated staking, node collateral, rewards, metagraphSync** | **HYPERGRAPH-GUARANTEED (global layer)** | universal gl0 fold, never sharded |

The split is sharper than "economic vs data": *even inside a metagraph*, the token primitives are re-executable (a data-app cannot change how fees/tokens behave), so the economic core is verified, and only the genuinely-custom data state is authoritative.

### 2.2 The watchtower correctness mechanism (the key addition)
The committee is the **producer**, not the **trusted oracle**. Adoption is optimistic; correctness is enforced by independent re-execution:
1. Committee re-executes `accept(finalizedBase, window.blocks, carriedGlobalSyncView)` once and ships `ShardCheckpoint{ diff, perMgRoots, committeeSignatures, Signed[CurrencyIncrementalSnapshot] }`.
2. A **truster** adopts the diff after the root-equality gate (cheap).
3. A **watchtower** — any non-committee gl0 node, or a metagraph operator watching the hypergraph — holds (or fetches) the metagraph's prior state, re-executes `accept()`, and checks **output === the snapshot's embedded `stateProof`**. On mismatch it submits a `FraudProofEnvelope` within a challenge window of K finalized ordinals.
4. gl0 verifies the dispute **deterministically** (it re-runs the now-available diff/inputs — no interactive bisection) and, if upheld, ~~**reverts the checkpoint and slashes the signing committee 100%**~~ **durably slashes the signing committee** (`Slashings` fieldId 34) (§6). **[CORRECTION (2026-06-30): the "revert the checkpoint" leg was specced but NEVER implemented — only the slash is wired. The decided model is DEFER-NOT-REVERT: economic effects are not spendable until `approvals ≥ kApprove OR depth ≥ k1`, so a pre-finality dispute is a clean reject and no state-revert is needed. See SHARDING-PRODUCTION-READINESS-PLAN.md §1/§4.]**

**Single-honest-verifier safety:** because the snapshot ships with the checkpoint (DA) and the metagraph's own `stateProof` is the spec, *one* honest watchtower is sufficient to catch and punish a fully-corrupt committee. This is why the α>1/(2S) committee floor (rev 1's concern) is *dissolved* rather than mitigated by sizing.

**Precedent (the stakeholder's question — yes, others do this):** Polkadot **approval-checking** (randomly-assigned validators re-execute the parachain PoV and raise disputes; backers ≠ approvers); **optimistic rollups** (anyone re-executes the posted batch and submits a fraud proof in the challenge window); **Lightning watchtowers** (third parties watch + punish). "Ship the data with the commitment so anyone can verify" is the **data-availability** discipline (Ethereum blobs, Celestia). The design is a faithful composition of these, with one Tessellation advantage: **gl0 is already a global total-order sequencer**, so the dispute adjudicator and the cross-shard conflict detector are the same existing layer.

### 2.3 Invariants (testable; stakeholder-approved)
- **I-COMMIT:** an honest gl0 node never commits metagraph economic state unless `applyDiff(finalizedBase, diff)` reproduces the attested per-MG root AND it composes into the signed global `mptRoot`. *(Transmission-consistency holds; correctness now backed by I-CORRECT below.)*
- **I-BASE:** the diff is computed and applied at the committee-**finalized** base, never `bestTip`. *(S1/S2.)*
- **I-PIN:** every global/cross-shard read inside `accept()` resolves against the **carried** `globalSyncView` anchor. *(Held on cl0 via `forcedGlobalSyncView`; VIOLATED on gl0 mirror-expiry — R1; on L1 data-app — R4. Both fixed in the DAG.)*
- **I-AUTH [NEW]:** gl0 commits a metagraph snapshot only if it carries the env-required metagraph-operator signature threshold (≥1 dev; majority of registered keys integrationnet/mainnet).
- **I-CORRECT [the core addition]:** an attested root that is not the honest output of `accept()` (output ≠ the snapshot's embedded `stateProof`) is **detectable and slashable by any single honest verifier** within the challenge window, given the checkpoint-carried snapshot (DA).
- **I-ONCE:** a cross-**metagraph** consumable object is consumable exactly once across shards — enforced by the global `consumedAllowSpends` spent-set, checked (absence) + written atomically at gl0's fold (§5).
- **I-REG:** with `num-shards=1`, `accept()` is byte-identical to pre-sharding HEAD (`GlobalSnapshotAcceptanceManager.scala:1854`).

---

## 3. Global-State Access (pinned, not banned)

The "ban global lookups" framing is retired. Metagraphs **read global state** — pinned to a finalized anchor. Stakeholder model, confirmed: **gl0 advances its tip fast (depth/attestation/phase-boundary finality); metagraphs work off the FINALIZED gl0 state and always trail it; the metagraph signals which gl0 view it had (`globalSyncView`), and gl0 is always ahead of that view.** A read pinned to a committed, finalized root yields a byte-identical witness for producer, committee, and every watchtower ⇒ pure function ⇒ split-safe (Polkadot PoV/witness, NEAR stateless validation). A *live* read does not ⇒ R1/R4 are determinism faults.

Anchor: `GlobalSyncView(ordinal, hash, epochProgress)` (`shared/.../currency/schema/globalSnapshotSync.scala`), pinned into every `CurrencyIncrementalSnapshot`; validator determinism via `forcedGlobalSyncView` + hash-equality (`CurrencySnapshotAcceptanceManager.scala:354-403`).

Allowed reads + status (no mainnet regression): `getLastSynchronized*` PRESERVED (thread the *carried* view so the pin is committed); cross-currency `SpendAction` ingress PRESERVED + pinned; own-MG history PRESERVED; **L1 `getLastGlobalSnapshot` → PIN to the cl0-committed `globalSyncView` (stakeholder decision: both L1 apps behave the same)** — R4; cross-shard reads carry an inclusion proof vs gl0's finalized subtree root (~~DESIGN; transport stubbed~~ **BUILT — proof transport wired; gl0-local cross-shard read functional/deterministic at `numShards>1`, `0b7902d69`**).

---

## 4. Sharded Execution + Security

### 4.1 Committee sortition — KEEP the current design (deliberate)
The committee is selected by a **deterministic PRF over public VRF keys, keyed on the metagraph snapshot's `parent_hash` (`lastSnapshotHash`), rotated each eta-epoch** (`CommitteeSortition.scala`; `ShardCheckpointWiring.committeeFor`). This is a **load-bearing prior decision** (see `committee-sortition-slot-id-decision`, 2026-05-17): parent-hash keying gives a tight slashing-identity algebra and uniform readability across heterogeneous (currency + custom) metagraph types; an LDD-style election here was tried and abandoned, and a Praos-style static threshold is explicitly not wanted. **Predictable-within-epoch + rotate-each-epoch is acceptable and intended.** Do NOT introduce intra-epoch re-draw or forced operator-to-shard assignment "for unpredictability" — with the watchtower layer, committee secrecy is not the security boundary.

What IS required (correctness of the primitive, not its unpredictability):
- **Real per-signer committee membership verification.** ~~`verifyVrfStructural` is a length≥80 placeholder; replace with real membership/VRF verification (the "Slice 13" work).~~ **[STALE — BUILT (025a58688):** real per-signer VRF verify (`verifyVrf` → `CommitteeSortition.verifyShardMembership`) is now wired; `verifyVrfStructural` survives only as the **registry-absent fallback** (a missing VK/eta is "I cannot check", not "wrong"). **Residual:** on integrationnet/mainnet, remove that carve-out once operator keys are universally registered.**]**

### 4.2 Why the α>1/(2S) floor dissolves (not "is mitigated by sizing")
rev 1 recommended `k-quorum_min = f(α,S)` near `K≈400`. **Withdrawn.** That sizing only matters in a world where the committee is the *sole* re-executor (honest-majority-committee assumption). The watchtower fraud-proof changes the trust assumption from "≥⅔ of this committee is honest" to "≥1 honest verifier exists for this shard AND inputs are available AND there is a challenge window." Therefore:
- **Committee size is a liveness/throughput parameter, not the safety parameter.** The current `k-draw=8 / k-quorum=6` is a reasonable starting point to validate; size is tuned for "enough signers to produce reliably + carry the metagraph-majority authenticity," not for honest-majority safety.
- **The real safety stack:** (1) watchtower fraud-proof + challenge window (§2.2); (2) data availability so the watchtower can get inputs (§6.3) — satisfied by the checkpoint carrying the snapshot, MVP; (3) wired invalidity-slashing so a caught lie is punished (§6); (4) the env-dependent metagraph-majority signature so the committee can't forge the *source*.
- **Residual to size deliberately:** value-at-risk per checkpoint ≤ committee-stake-at-risk × slash_fraction (so a caught-but-already-profited attack is unprofitable), and the **challenge-window length K** vs finalization latency. These are the genuine knobs — far smaller than K≈400.

### 4.3 Operating regime
Multi-shard general (`S` up to one-shard-per-metagraph); **validate on 2mg/2shard**. Committee `k-draw=8/k-quorum=6` as the starting point. Safety = watchtower + DA + slashing + metagraph-majority-sig, NOT committee size. Eta-epoch rotation (R≈2550 snapshots) for the committee draw — no intra-epoch churn.

---

## 5. Cross-Shard Economic Operations (incl. the stakeholder's I-ONCE question)

**Your question — is I-ONCE even needed, given allow-spends target an approver address?** Precise answer: targeting prevents the *wrong party* from consuming (only the named approver can), so a *different* metagraph cannot steal an allow-spend. I-ONCE addresses a *different* failure: the **same** consumer double-consuming when the allow-spend's active-status lives in a shard that does not witness the consumption.
- **Same-shard (allow-spend's currency == the consuming metagraph):** no cross-shard issue. The owner shard re-executes the consume and removes the allow-spend from its own accumulator (`SpendActionValidator.scala:185`) — single-use is local and already enforced. **I-ONCE not needed.**
- **Cross-metagraph (allow-spend on currency M, consumed by a spend processed in metagraph M′, a different shard):** M′ proves the allow-spend *exists* against M's last-finalized root (inclusion proof) and consumes it — but M's root keeps showing it active until M observes the consumption, so a stale-proof **double-consume** is possible. Your UTxO-hashref intuition is exactly the fix: a **global `consumedAllowSpends` spent-set** keyed by allow-spend hash, with consume requiring `include(M-root) ∧ absent(consumedAllowSpends)` and writing the spent-marker in the **same** gl0 snapshot (Cosmos `recvPacket` membership + receipt-absence).

**I-ONCE is IN for v1 (stakeholder: cross-metagraph consumption is supported).** A global `consumedAllowSpends` spent-set, keyed by allow-spend hash, is required before `num-shards>1`.

**Cross-shard consumption is ATOMIC at the gl0 layer — NOT async deliver-or-refund (stakeholder correction; it is stronger).** gl0 sees every shard checkpoint in one total-ordered global snapshot, so it settles a cross-shard consume *atomically at its fold*, with no escrow/refund/timeout:
- gl0 verifies the consume's inclusion proof (allow-spend X exists in owner shard A's last-finalized subtree root) **∧** that X is absent from `consumedAllowSpends` (I-ONCE) **∧** applies the token effect (the funds X **reserved/locked at creation** move source→destination — a protocol primitive gl0 can apply directly since balances are mirrored) **∧** writes X to `consumedAllowSpends` — all in the one snapshot.
- **Conflict-free with the owner shard by construction:** an allow-spend reserves its funds at creation, so A's ongoing checkpoints never touch the reserved amount; the consume just moves already-reserved funds at gl0's fold. A does not have to process the consume — its stale active-set entry is harmless because the spent-set is authoritative.
- **Same-snapshot double-consume:** two checkpoints consuming the same X in one snapshot → gl0 conflict-merge (deterministic VRF tiebreak, reject loser, slashable).
- **Cross-snapshot replay:** A's root keeps showing X active (A never learns of the consume), so a later re-consume's inclusion proof still passes — but the `consumedAllowSpends` absence-check fails. The spent-set is the single source of truth for consumption.

**Why this beats Cosmos/NEAR:** they are async deliver-or-timeout *because they have no shared sequencer* (each chain sees only its own state). gl0 IS the shared sequencer — it sees all checkpoints in one total-ordered snapshot — so cross-shard settlement is atomic and synchronous *at the gl0 layer* even though shards produce asynchronously. The only async aspect is that creation and consumption land in different snapshots; each consume is atomically settled when gl0 folds it. No 2PC blocking, no refund leg.

---

## 6. Enforcement (watchtower-driven slashing)

### 6.1 ~~Today (confirmed)~~ Original status — STALE
~~Zero production slashing call sites; mismatch logs/drops. `FraudProofEnvelope` schema-only. Design slashes equivocation but exempts invalidity. `SlashingDetector.scala` does not exist.~~ **[CORRECTION (2026-06-30): BUILT (8ac7ce04f..0b7902d69).** The invalid-state-proof slash is wired end-to-end: the always-run `watchtowerReExec` raises a real `FraudProofEnvelope`; an upheld dispute writes a **durable slash to the `Slashings` MPT partition (fieldId 34)** via `InvalidStateProofEvidence` / `InvalidStateProofSlashedReader`. Invalidity is now the primary slashable tier (no longer exempt). The residual is the *defer-not-revert spendability gate* and the *I-AUTH* source-authenticity signature (SHARDING-PRODUCTION-READINESS-PLAN.md §3-§5).**]**

### 6.2 Required
1. **`InvalidStateProof` = primary 100% tier**, triggered by an **upheld watchtower dispute** (output ≠ embedded `stateProof`, re-verified deterministically by gl0 from the checkpoint-carried snapshot). ICS ADR-005 discipline: slash on *independent re-verification*, never on a shard's bare attestation.
2. **Equivocation = secondary tier** (current design, KES-attributed for non-repudiation).
3. **Invalid-source-authenticity** (I-AUTH): a committee that ships a metagraph snapshot lacking the env-required operator-majority signature is slashable on a single honest report.
4. **Ledger effects wired:** stake `×(1−slash_fraction)` on `activeDelegatedStakes`+`activeNodeCollaterals`, `slashedRegistry` cooldown, double-slash guard, bounty `×0.05`, remainder burns; N-2 stake staging.

### 6.3 Hard dependencies
- **Data availability** — satisfied at MVP by the checkpoint carrying `Signed[CurrencyIncrementalSnapshot]` (the watchtower's inputs are in-band). Erasure-coded retrievability is the scale path; **land DA before roots-only**, since roots-only removes the implicit full-replication DA.
- **Challenge window K = k1** — a watchtower has k1 ordinals to re-execute the small diff + dispute; K < k2 ensures a dispute always lands before archive/irreversibility. Derived from the single confirmation-depth knob (R and k2 are already k1-derived). Bump to 2·k1 only if testing shows watchtowers need margin; never approach k2. **[CORRECTION (2026-06-30): the spendability rule is now DEFER-NOT-REVERT, not "effects usable at depth-k1 alone."** Effects are STAGED and released on `approvals ≥ kApprove OR depth ≥ k1` (fast watchtower-approval tier layered on the depth-k1 safety floor, max-of); a pre-finality dispute is a clean reject and **no state-revert path is built.** NB: the staging gate itself is still TO BUILD — today the fold applies effects immediately. See SHARDING-PRODUCTION-READINESS-PLAN.md §4.**]**
- **Value-at-risk ≤ committee-stake-at-risk × slash_fraction** — the real "committee sizing" parameter.

---

## 7. Where the Existing Design Is Inadequate (rev 2, blunt)

> **CORRECTION (2026-06-30): items 1, 2, 4, 5, 6 below are BUILT (8ac7ce04f..0b7902d69) and no longer inadequacies.** The live inadequacies are now item 3 (I-AUTH), the **authoritative-override removal so re-exec is the primary token-model gate** (CURRENCY-APP-TOKEN-ENFORCEMENT.md), and the **defer-not-revert spendability staging gate** (SHARDING-PRODUCTION-READINESS-PLAN.md §4).

1. ~~**Watchtower fraud-proof not built** — `verifyEmbedded` admits on quorum with no re-execution; checkpoint doesn't carry the snapshot; `FraudProofEnvelope` schema-only; mismatch logs/drops.~~ **[BUILT:** always-run `watchtowerReExec`; checkpoint carries the snapshots as `ShardDerivedStateDelta.includedSnapshots`; `FraudProofEnvelope` is a real artifact; upheld dispute → durable slash.**]**
2. ~~**Invalidity-slashing unwired + mis-tiered** (equivocation slashed, invalidity exempt; zero call sites).~~ **[BUILT:** invalid-state-proof is the primary slashable tier, written durably to `Slashings` (34).**]**
3. **Metagraph-source authenticity is a single signature** (`validateStateChannelAllowanceList`, ≥1 sig) — make env-dependent (majority on registered nets). *(Still accurate — I-AUTH TO BUILD.)*
4. ~~**Cross-metagraph single-use unenforced** (I-ONCE)~~ **[BUILT:** `ConsumedAllowSpends` (33) spent-set wired at `numShards>1`.**]**
5. ~~**`perMetagraphMptRoots` is a flat `hash((incRoot, infoRoot))`**, not component-addressable — cannot back membership/absence inclusion proofs until the S6 PIN-1 work.~~ **[BUILT:** PIN-1 component-addressable per-MG roots landed; inclusion/absence proofs are backed.**]**
6. ~~**Proof transport dark** — `ShardSubtreeProofClient.noop`/http-stub; `ShardProofRoutes` exists but is **not mounted** in `HttpApi`.~~ **[BUILT:** proof transport wired; cross-shard consume reads are functional/deterministic at `numShards>1` (`0b7902d69`).**]**
7. **Receipts are a skeleton** (`CrossShardReceipt`, `MetagraphSyncManager` pending accumulator) — not a produced/ordered/drained pipeline. *(Verify against current code; cross-shard settlement is now atomic at the gl0 fold — receipts may be obsolete rather than a gap.)*
8. **R1 live bug** (gl0 expires metagraph-scoped allow-spends/token-locks on its *live* epoch vs the metagraph's *pinned* `globalSyncView` epoch) + **R4** (L1 `getLastGlobalSnapshot` unpinned).
9. **NOT inadequate (rev 1 over-reach, withdrawn):** committee predictability and large committee floors — the watchtower layer makes committee secrecy and honest-majority-committee non-load-bearing.

---

## 8. Product Architecture
```
┌─ Metagraph (cl1, dl1, ml0) ──────────────────────────────────────────────────┐
│  accept() reads global/cross-shard state PINNED to finalized globalSyncView;   │
│  emits Signed[CurrencyIncrementalSnapshot] (embeds stateProof + the pin);       │
│  signed by ≥ env-threshold of registered metagraph operators (I-AUTH)           │
└──────────────────────────────────────────────────────────────────────────────┘
        │ signed snapshot                                  ▲ pinned reads
        ▼                                                  │
┌─ Shard committee (VRF-sortitioned, parent-hash-keyed, epoch-rotated) ─────────┐
│  re-exec accept() ONCE @ finalized base → ShardCheckpoint{ diff, perMgRoots,   │
│  committeeSignatures, [NEW] Signed[CurrencyIncrementalSnapshot] }               │
└──────────────────────────────────────────────────────────────────────────────┘
        │ checkpoint (diff + snapshot = DA)                ▲ FraudProofEnvelope
        ▼                                                  │ (output ≠ stateProof)
┌─ gl0 (global hypergraph: sequencer + adjudicator) ───────────────────────────┐
│  verifyEmbedded: per-signer pre-checks + I-AUTH; quorum ⇒ optimistic adopt      │
│  applyDiff(finalizedBase) → recompute root → gate ===attestedRoot → compose     │
│  WATCHTOWERS re-exec from carried snapshot → dispute → gl0 re-verifies          │
│  deterministically → durable slash (Slashings 34); conflict-merge; NO revert    │
│  universal fold: delegated-staking, node-collateral, rewards, metagraphSync     │
└──────────────────────────────────────────────────────────────────────────────┘
        │ finalized global snapshot (after challenge window K)
        ▼   light clients / cross-shard consumers (inclusion ∧ absence proofs)
```

---

## 9. Task DAG — Enable Full Multi-Shard Wiring (validated on 2mg/2shard)

Legend **S**(≤2d)/**M**(≤1w)/**L**(>1w); **T**=tactical, **W#**=wiring/security. Waves dependency-ordered; tasks within a wave parallel. Greenfield: nothing promotes to a shippable net until the whole stack is green on the 2-shard test.

> **CORRECTION (2026-06-30): much of Waves 2-3 is now LANDED (8ac7ce04f..0b7902d69).** Built: **W2a** (checkpoint carries snapshots via `ShardDerivedStateDelta.includedSnapshots`), **W2b** (PIN-1 component-addressable per-MG root), **W2c** (`watchtowerReExec` + real `FraudProofEnvelope` + verdict), **W2d** (real committee-VRF verify), **W3a** (durable `InvalidStateProof` slash → `Slashings` 34), **W3c** (proof transport wired), **W3d** (`ConsumedAllowSpends` 33 spent-set). The remaining critical-path work is re-framed by the two companion docs: **(1) re-exec as the PRIMARY token-model adoption gate** (remove the authoritative override — CURRENCY-APP-TOKEN-ENFORCEMENT.md), **(2) defer-not-revert spendability staging** + **I-AUTH** (SHARDING-PRODUCTION-READINESS-PLAN.md). W4 (bring-up + roots-only hard fork) still stands.

**Wave 0 — Tactical + red tests (now)**
- **T1**(M): R1 fix — scope-aware expiry: gl0 expires metagraph-scoped allow-spends/token-locks on the metagraph's **pinned `globalSyncView.epochProgress`**, not gl0's live epoch (`AllowSpendStateManager:167/203`, `TokenLockStateManager`; the `None` global scope keeps live epoch). De-wedges the live cluster; forward-compatible. **T2**(S): same for token-locks. **T3**(S): RED regression suite (live-vs-pinned over-prune). **T4**(S): CI guard — production config stays `num-shards=1` until the security stack is green. **T5**(S): RED tests for the target failure modes (quorum-signed invalid checkpoint adopted; cross-metagraph double-consume; unmounted proof route; missing absence proof; snapshot-without-majority-sig).

**Wave 1 — Pinned global-state complete + apply-diff terminal**
- **W1a**(M): thread the *carried* `globalSyncView` into `getLastSynchronized*`; **R4** — pin L1 `getLastGlobalSnapshot` to the cl0 anchor. **W1b**(M, after S2): delete contiguity gate; **W1c**(L): retire `pipeline-depth` after Taktikos adversarial sim (keep newness gate); **W1d**(M): UNROLL/cl1-emitter audit.

**Wave 2 — The safety core (watchtower fraud-proof + DA + provable roots)**
- **W2a**(L): **checkpoint carries `Signed[CurrencyIncrementalSnapshot]`** (DA in-band) + size/relay validation. **W2b**(L): **component-addressable per-MG root (PIN-1)** — replace flat `hash((incRoot,infoRoot))` with a Merkle commitment over real subtree roots; producer + follower-recompute + proof-service switched atomically. **W2c**(L, after W2a): **watchtower re-execution + `FraudProofEnvelope` producer/consumer + challenge window**; non-committee gl0 re-execs from the carried snapshot, checks output===stateProof, disputes; gl0 re-verifies deterministically. **W2d**(M): real committee membership/VRF verification (replace `verifyVrfStructural`) + remove KES carve-out on registered nets — *no* unpredictability changes.

**Wave 3 — Wired enforcement + authenticity + cross-shard (gates `num-shards>1`)**
- **W3a**(L, after W2c): **wire `InvalidStateProof` 100%-tier slashing** ledger effects; equivocation→secondary; N-2 staging. **W3b**(M): **env-dependent metagraph-majority signature** (I-AUTH) at `validateStateChannelAllowanceList` + slash on majority-less snapshot. **W3c**(L): **proof transport** — real `ShardSubtreeProofClient.http`, **mount `ShardProofRoutes` in `HttpApi`**, multi-shard `SpendActionValidator` (thread cluster `numShards`), `ProvenCurrencyStateReader`. **W3d**(L, *if cross-metagraph consumption in scope*): **I-ONCE** `consumedAllowSpends` partition + include∧absence proof + same-snapshot spent-marker; **W3e**(M, after W3d): gl0 conflict-merge + escrow/refund + receipts pipeline.

**Wave 4 — Bring up + validate the 2-shard network; then roots-only (separate hard fork)**
- **W4a**(M): **enable `num-shards>1`** on the 2mg/2shard e2e ONLY after W2c+W3a+W3b+(W3c)+(W3d/e if in scope) are green; full adversarial e2e (forge a root → caught+slashed; double-consume → rejected; majority-less snapshot → rejected). **W4b**(L): LC-SMT + ml0 balance proof. **W4c**(L)`[HARD FORK]`: roots-only fold deletion, ordinal-gated, after DA (W2a) is load-bearing.

**Critical path:** T1 → W1a → W2a → W2b → W2c → W3a → W4a. The watchtower core (W2a-c) + enforcement (W3a-b) is the indivisible "makes the guarantee real" unit; multi-shard validation (W4a) is gated on it.

---

## 10. Decisions / Open Questions (status)
**Resolved by stakeholder:** v1 = optimistic fraud-proof (no ZK yet). Data-app state = authoritative + labeled; token transitions = re-executed. Metagraph signature = env-dependent (count dev / majority registered). ~~Cross-shard = deliver-or-refund + gl0-sequencer~~ Cross-shard = **atomic gl0 settlement, no escrow/refund** (superseded by rev 3 below + §5). DA = yes, before roots-only. L1 reads (R4) = pin to cl0 anchor. Nothing ships until fully wired + tested (greenfield); multi-shard is the target, 2-shard the test.
**Resolved (rev 3, stakeholder):** Committee size = **keep `k-draw=8/k-quorum=6`** (safety is the watchtower, not committee honest-majority; K≈400 withdrawn). Cross-metagraph spend consumption = **IN v1** → I-ONCE required, **atomic gl0 settlement** (§5), no escrow/refund. **Challenge window K = k1** (single confirmation-depth knob; ~~effects usable at depth-k1~~ **spendability deferred until `approvals ≥ kApprove OR depth ≥ k1` — DEFER-NOT-REVERT, no state-revert built (2026-06-30 correction)**; K < k2 guarantees disputes land before archive). Target = **general multi-shard** (arbitrary S); 2mg/2shard is the test config.
**Shard-count limit (resolved):** NO hard limit in core code. `numShards` is cleanly parameterized — assignment `shardIdFor(addr)=hash(addr) mod numShards` (metagraphs *multiplexed* onto shards, not 1:1; `ShardAssignment.scala:60`), per-shard committee draw (`shardId` in the VRF preimage), `ShardId: NonNegInt` (documented 4..1000), only `require(numShards>0)` (`ShardAssignment.scala:55`). Arbitrary S works. The lone cap is the laptop e2e harness (`set-env.sh` errors at `--num-shards>5`) — a one-line raise, not a protocol constraint.
**Remaining (tuning, not blockers):** per-checkpoint value-at-risk cap (`slash_fraction × committee_stake ≥ max-extractable-per-checkpoint`).
