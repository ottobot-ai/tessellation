# Consensus Owner Decision Register - Rejected Advisory Draft

**Companion to:** [`CONSENSUS-OWNER-DECISIONS.md`](CONSENSUS-OWNER-DECISIONS.md)
**Original compilation:** 2026-07-14 at stale HEAD `5557ee084`

**AUDIT DISPOSITION: REJECTED. DO NOT RATIFY OR IMPLEMENT FROM THIS FILE.** An
adversarial source audit at HEAD `e26ad406e` found material factual errors,
unsupported readiness claims, reversed dependencies, and recommendations refuted
by the owner-supplied Taktikos papers. The stale draft is retained below only to
avoid destroying work. Review authority for O-15/O-16/O-17 is the audited source
packet for each gate:

- [O-15 Multi-Tine Frontier Owner Review](O15-MULTI-TINE-FRONTIER-OWNER-REVIEW.md)
- [O-16 Exact Phase-2 Consumer Lease](P6-FIN14-PHASE2-CONSUMER-LEASE.md#10-owner-review-required)
- [O-17 ROOT-008 GL0 Partition Grammar](ROOT-008-GL0-PARTITION-GRAMMAR.md#8-owner-review-required)

### Why this draft failed audit

1. **O-15 is not partly freeze-ready.** Taktikos Algorithm 1 initializes `C <-
   Cloc` and enumerates candidates against the current incumbent. Neither supplied
   paper defines a symmetric `max(post-MRCA suffix)` metric, an objective
   total-frontier selector, or the repository's lower-VRF/hash exact tie rule.
   O-15A through O-15G therefore remain open except for the already-ratified
   objective-result property. Sources and hashes are recorded in the O-15 packet
   at lines 94-112 and its review table at lines 202-215.
2. **Committee blind-signing is not the current root-recreation defect.** The live
   signing path calls `evaluateForSigning -> evaluateIntake -> reExecPath` and
   compares the claimed roots before producing `VerifiedShardCheckpoint`
   (`ShardCheckpointGl0AcceptanceManager.scala:312-350,629-708`); the emitter only
   accepts that capability (`ShardCheckpointAttestationEmitter.scala:47-53,128-224`).
   Remaining gaps include complete canonical diff/intent/result binding, ordinary
   adopter verification, and positive pre-inclusion watchtower coverage
   (`docs/adr/0017:182-194`).
3. **The economic invariant was stated incompletely.** The producer and every
   execution signer replay the exact ordered framework inputs at the exact Phase-2
   base and must match the canonical diff and root. A noncommittee adopter verifies
   the certificate/base/scope/continuity/diff, applies the scoped diff, and
   reproduces the root. Assigned watchtowers replay, and positive coverage precedes
   inclusion. Admission/DA signatures never satisfy execution quorum
   (`docs/adr/0017:66-135`; `AGENTS.md:39-60,78-81`).
4. **Certified diff adoption was not owner-rejected.** Rejected designs were
   universal noncommittee replay and authority/root-only adoption. The target is
   execution-certified canonical diff adoption plus adopter root reproduction
   (`docs/adr/0017:35-44,100-180`;
   `docs/review/10-reexec-byte-contract.md:3-11`).
5. **The dependency was reversed.** O-17 structural validity plus O-07/ECON-G
   semantic validity precede valid tines, then O-15 fork choice, exact-hash Phase 2,
   and O-16 consumer leases
   (`O15-MULTI-TINE-FRONTIER-OWNER-REVIEW.md:400-421`). O-17 parser/grammar work is
   not blocked on selecting an O-15 algorithm.
6. **O-16 readiness was overstated.** O-16A and O-16C are recommendations; O-16B
   and O-16E are conformance to locked L-19/L-23; O-16D and O-16F still require
   schema/era decisions. Issuance remains blocked by the full dependency inventory
   in `P6-FIN14-PHASE2-CONSUMER-LEASE.md:456-477`, and positive watchtower coverage
   is not live.
7. **O-17 is not “5/7 firm.”** R008-01 still needs the numeric-gap decision,
   R008-04 needs protocol budgets/growth rules, R008-05 waits on exact ECON-G
   identities, R008-06 needs the currency-scope rule, and R008-07 is a conformance
   gate (`ROOT-008-GL0-PARTITION-GRAMMAR.md:467-548`). ECO-F32 is HIGH, not
   CRITICAL.
8. **O-01 is not proven or freeze-ready.** `SnowballAccumulator` has no K-query/
   alpha sample cascade and is arrival-order sensitive
   (`SnowballAccumulator.scala:13-25,67-80`); the live loop also has a separate
   cumulative-weight sink (`SnapshotLeaderLoop.scala:1082-1103`). Finite simulation
   runs are calibration evidence, not a proof or a numeric `T_weight` freeze.
9. **Local defaults are not protocol law.** O-03's current HOCON penalty, cooldown,
   and window values are defaults, not rooted/ratified active-era parameters. The
   population, coverage, deadline, bond, bounty, and replay-budget contract remains
   open (`CONSENSUS-OWNER-DECISIONS.md:117-123`).
10. **The tower-store status was stale.** `MptTowerStore` and production wiring
    already exist (`MptTowerStore.scala:104-115,264-275`); durable restart,
    branch-aware reconstruction, and activation proof remain open.
11. **Positive stake is not a settled operator-membership predicate.** It permits
    stake splitting; the exact backing/bond predicate remains an owner decision
    (`CONSENSUS-OWNER-DECISIONS.md:214-237,279-294`).
12. **The claimed KES end-to-end run is not repository-verifiable.** No committed
    test or immutable log supports the stated 16-rotation/8-node/13,724-signature
    result. It must remain an unverified external claim.
13. **Severity labels were inflated.** FIN-14, SER-02, ECO-F32, and SMT-01 are HIGH
    in the audited ledger; ECO-04, ECO-05, ECO-18, and SHARD-C-009 are CRITICAL.
14. **The provenance claim is false.** The draft was compiled 39 commits behind
    the audited HEAD and cites out-of-repository `project_*`/`feedback_*` material
    as though every assertion were reproducible from committed source.
15. **O-05 describes the draw incorrectly.** Admission and execution are distinct
    draws sharing a parameter block; the missing construct is a separately typed
    custody/admission threshold that can never satisfy execution quorum.

<details>
<summary>Preserved stale advisory draft (not reviewed authority)</summary>

---

## What this is

For every open gate (O-01…O-17) this collects the answer **our own prior work already
supports** — from the sims, ADRs, design docs, code, and memory — separates it from the part
that remains a **genuine owner economic/governance judgment**, and flags any **live defect**
that motivates the gate. Every substantive claim carries a `file:line` citation so the owner
can verify at source rather than trust prose.

### The #1 rule this synthesis is held to

**CL1 framework-economic operations (allow-spend, spend, token-lock, transfer, framework fee,
balance, supply) MUST be re-executed** by the shard committee and checked by watchtowers.
"Roots-only / proof-carrying / attestation-only" adoption is correct **only for DL1
custom-data** (which GL0 cannot run) and is **never** a substitute for CL1 re-execution
(`docs/adr/0016`, `docs/adr/0017`). Every answer below carries a re-exec check; none weakens
this rule.

### Legends

**Readiness**
- 🟢 **Ready to freeze** — evidence settles it; owner ratifies as-is (or only a single binary product choice remains).
- 🟡 **Recommendation on record → ratify** — a complete drafted recommendation exists; owner reviews and ratifies. Implementation may be a separately scheduled gate.
- 🔴 **Open → needs work** — genuine owner economics/governance and/or unbuilt design/proof; not ratifiable from evidence alone.

**Evidence tags** used inline: `[LOCKED]` `[RECOMMENDED]` (on record) `[PARTIAL]` (partially implemented) `[GAP]` (defect/unbuilt) `[OPEN]` (no prior work / owner call).

---

## Summary

| Gate | Title | Ready | One-line answer our work supports |
|---|---|:--:|---|
| O-01 | Avalanche population & parameters | 🟢 | `(K=8, α=5, β=10, Δ=slot/2)`, N≥16 floor (else T_depth1), without-replacement sampling; sim-locked, ratify + lift K/α into typed config. |
| O-02 | Deep-history recovery beyond `k2` | 🟡 | Halt → fetch authenticated hash-addressed ancestry/state → re-verify the **same** `maxvalid-bg` decision → rebuild **before** mutation; atomic multi-sink recovery. Params still open. |
| O-03 | Watchtower params & availability fallback | 🔴 | Release rule + penalties locked (slash 1/1, bounty 1/20, cooldown 100, window=`k1`); every population/coverage/deadline/bond/replay-budget value is unset. |
| O-04 | ML0 response to a Phase-2 density reorg | 🟡 | Append-only audit → registered deterministic rebase/undo → else new ML0 epoch at last valid GL0 ref. Reconcile register-OPEN vs lifecycle-ratified. |
| O-05 | Intake threshold & censorship recovery | 🔴 | Intake↔execution separation locked (intake never satisfies `kQuorum`); all six intake parameters unset; no distinct custody-intake layer built. |
| O-06 | Global correction authorization | 🔴 | Forbidden case locked (no metagraph authority); vehicle = ordinal/hash-bound `ProtocolEra`; authorization **model** and the signed-correction **schema** undesigned. |
| O-07 | Economic operation grammar | 🔴 | Preserve manual-unlock + metagraph-source spend as authorized v4 features, re-specified with explicit authority; ~15 grammar rows + 12 RED/oracle vectors unbuilt. |
| O-08 | Tower proof contract | 🟡 | Proof contract designed + partly landed; blocked on making `smtRoot` deterministic & recipient-reproduced (L-22 precondition); size params stale. |
| O-09 | Pure opaque state-channel scope | 🟢 | Default: opaque/data-only lane **disabled unless owner explicitly retains**; if kept, availability-only, no economic write. Conditioned on the SER-02 fix. |
| O-10 | Portable shard-parent duty & slot bound | 🟡 | Ratify the drafted recommendation (exact `(ordinal,hash)` Phase-2 parent ref in the signed preimage; slot ≤ including-GL0-snapshot slot). Impl = `SHARD-C-009`. |
| O-11 | Permissionless GL0 operator roster | 🔴 | Frame + containment settled (registration≠membership; uniform `1/N` over N-2 pop ∩ active keys ∩ positive stake, fail-closed). The **economics** are undecided. |
| O-12 | Runtime KES secret deletion & N-2 reorg | 🟡 | Baseline drafted; one-way deletion at the eta-period evolution boundary **implemented + e2e-validated**; common-prefix probability, escrow y/n, rejoin still open. |
| O-13 | Durable global delivery sequence | 🟡 | Hash-linked per-destination sequence + rooted outbox + permanent nullifier + ML0 cursor; inbox-before-local-spend first rule. Motivated by live ECO-05 double-apply. |
| O-14 | Framework fee sequence & opaque-data binding | 🟡 | Reuse rooted `MgLastFeeTxRefs` (field 27) as strict per-source head + v1 rules; one clean choice: manifest-vs-bytes. Motivated by live ECO-18 fee replay. |
| O-15 | Multi-tine Taktikos/Genesis frontier | 🔴 | Property (L-24) + boundary (L-04) settled; the **selector (A)**, **frontier evidence (B)**, **proof (C)** genuinely open. **D** is ratifiable now; **E** open + flagged tension. |
| O-16 | Exact Phase-2 consumer lease (FIN-14) | 🔴 | All six sub-choices (A–F) recommended on record; zero constructs built; FIN-14 defect live; blocked on O-15/O-01. |
| O-17 | ROOT-008 GL0 partition grammar | 🔴 | 5/7 anchors carry firm recommendations; R008-04 (numeric budgets) and R008-06 (token-lock scope) genuinely unanswered; GROWTH-001 + field-32 parity open. |

**Freeze-now shortlist (evidence is sufficient):** O-01, O-09 (product yes/no + SER-02),
O-10, O-12, O-15-**D only**, and the O-16A–F / R008-01/02/03/05/07 recommendations as
*conditional* ratifications. Everything else needs either owner economics or design/proof work.

---

## O-01 — Avalanche population and exact parameters 🟢

**Freeze:** delayed canonical registry/weight snapshot, `K`, `alpha`, `beta`, `T_weight`,
sampling replacement rule, attestation lifetime, small-network failure mode; epoch pattern
stake/registry from `N-2`, eta from `N-1`.

**Answer (from our work):** Calibrated in the research repo `~/repos/research-nipopos-2026`.
- **`(K, α, β) = (8, 5, 10)`**, tick cadence **Δ = slot/2** — the smallest Snowball triple that
  zeroes safety violations across N∈{16,32,100,500,1000} at f_adv=0.33, per the 6696-cell ×
  10k-trial GPU sweep (`sims/avalanche_attestation_calibration_gpu.py`, data
  `sims/data/avalanche_attestation_full_gpu_n10000_v2.json`; write-up
  `docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md §0.A/§0.M`). Flipped up from `(3,2,10)`
  after K=3 was found at the Snowball noise floor. `[RECOMMENDED]`
- **Small-network failure mode:** K=8 is **infeasible at N≤8** (no perfectly-safe grid cell);
  recommendation holds for **N≥16**, and below that finality falls back to **T_depth1**
  (`AVALANCHE-ATTESTATION-PROPOSAL.md` lines 171-182). `[RECOMMENDED]`
- **Sampling replacement rule:** K peers sampled **uniformly without replacement** (Floyd
  K-subset in the sim; `AVALANCHE-ATTESTATION-PROPOSAL.md §2.2`). *Confirm against the
  `SnowballAccumulator` code, not only the sim.* `[RECOMMENDED]`
- **`T_weight` (finality quorum):** derived under Taktikos election in
  `sims/mithril_quorum_threshold.py` (5M-trial, `data/mithril_quorum_full_n5000000.json`).
  L-03 fixes Phase 2 = decided-attestation `T_weight` **or** canonical `k1` depth. `[RECOMMENDED]`
- **Attestation lifetime:** **emit-once** when the Snowball margin first clears β
  (`AVALANCHE-ATTESTATION-PROPOSAL.md §5.4`; design `ATTESTATION-TIMELINESS-INCENTIVE-DESIGN.md`).
  No dedicated lifetime sweep exists — this is a design decision informed by the suite. `[RECOMMENDED]`
- **Epoch pattern / registry snapshot:** stake/registry from `N-2`, eta from `N-1` for `N`,
  subject to the grinding suite (`sims/run_grinding_ci.py`, `grinding_results_ci.json`); aligns
  with the register's own O-11 lookup invariant and O-17/R008-02 self-authenticating field-20. `[RECOMMENDED]`

**Owner still decides:** (a) The config posture — `application.conf:340-343` already documents
`(K=8, α=5, β=10)` as the "GPU-sim-locked production point," but **only `snowball-beta` is a
HOCON knob (`:343`); K and α are code constants.** Ratifying O-01 means deciding whether to
lift K/α into typed `SharedConfig.nakamoto.*` (recommended, per the HOCON rule) or ratify them
as constants. (b) Whether the N≥16 floor + T_depth1 fallback is acceptable given the small
default test/prod clusters.

**Caveat:** the calibration is dated **2026-05-15 → 06-02**, i.e. ~6 weeks *before* the
committee-re-exec redesign (ADR-0016/0017). Re-confirm the population model (delayed canonical
registry vs the current seedlist — see O-11) before locking.

**Re-exec check:** OK. Avalanche is the optimistic finality rail only (L-03); it validates no
economics.

---

## O-02 — Deep-history recovery beyond local `k2` 🟡

**Freeze:** the authenticated archive/bootstrap protocol when a winning `maxvalid-bg` tine's
true common ancestor predates local rollback (`k2`) state.

**Answer (from our work):** The **semantics are specified consistently across three docs** and
governed by L-05 (`CONSENSUS-OWNER-DECISIONS.md:18` — objective comparison only, never guess or
choose socially):
- On `RecoveryRequired`, the node **halts production and Phase-2 serving**, fetches **exact
  hash-addressed ancestry + state from archival peers** (MPT-primary, exact-byte/root verified,
  **no peer-GSI installation authority**), **re-verifies the same `maxvalid-bg` decision**, and
  **rebuilds before any mutation**; corrupt/missing bytes **halt before mutation**; recovery
  derives eta/registry/tower state identical to a fresh bootstrap "by the same chain walk used
  by bootstrap" (`CONSENSUS-ARTIFACT-LIFECYCLE.md:769-774`;
  `GENESIS-DENSITY-PHASE2-REORG-AUDIT.md:64-133`). `[RECOMMENDED]`
- The **atomic multi-sink recovery inventory** is enumerated: MPT undo→MRCA via retained undo;
  requeue native + metagraph inputs exactly once; reverse checkpoint anchors + per-shard
  watermarks + binary confirmations; reverse pending delivery/nullifier; recompute
  eta/committee/tower/registry; durably publish the downstream replacement event
  (`CONSENSUS-ARTIFACT-LIFECYCLE.md:744-757`). `[RECOMMENDED]`

**Owner still decides:** the concrete **proof material**, the **minimum rollback/archive
service + DA/checkpoint/input/diff limits** (explicitly open,
`CONSENSUS-ARTIFACT-LIFECYCLE.md:887-888`), and a real **peer-diversity / anti-Sybil
archival-peer rule** (only "archival peers" + freshest-peer ranking exist today — no quorum or
diversity count). `[OPEN]`

**Status:** L-23's durable `RecoveryRequired` + idempotent finality-intent substrate is
**OPEN/PARTIAL** — built but **not wired** to the live FinalityGate/fork-choice/MPT
(`CONSENSUS-OWNER-DECISIONS.md:45-79`). The existing byte-faithful adopt primitive
(`project_gl0_deepcatchup_gsi_divergence`) covers the `>k1` gossip catch-up case, **not** the
`>k2` predates-retention case.

**Re-exec check:** OK — refold runs "the same validation/execution path as live production,"
and "no peer-GSI installation authority" explicitly forbids adopting un-re-executed state.

---

## O-03 — Watchtower parameters and availability fallback 🔴

**Freeze:** complement/sample size, minimum positive coverage, assignment anchor, deadline,
retry/redraw, bonds, replay budget, challenge lifetime, censorship fallback.

**Answer (from our work):** The **release rule is locked** — positive deterministic
noncommittee replay coverage precedes GL0-inclusion eligibility; a valid challenge triggers
exceptional bounded universal GL0 replay and the replay result (not the assertion) decides
rollback/slash (L-09, `CONSENSUS-OWNER-DECISIONS.md:22`; `docs/adr/0017:132-135`). **Penalty
parameters are set:** slash-fraction `1/1`, bounty-fraction `1/20`, cooldown `100` epochs
(`application.conf:565,571,576`); the **challenge lifetime is `k1`** — the GSAM keeps a
checkpoint's economic effects reversible until `k1` finalized ordinals after adoption
(`application.conf:296-302,552-555`). `[LOCKED]` / `[PARTIAL]`

**Owner still decides — no recorded values exist for any of these** (ADR-0017 explicitly defers
them, `:238-239,253-255`): watchtower complement/sample size; minimum positive-coverage
fraction; assignment anchor; deadline; retry/redraw; challenger **bond amount**; replay
budget/rate-limit; direct-fetch/censorship fallback. `[OPEN]`

**Status / re-exec flag:** the **shipped** watchtower is a *post-adoption, reversible-until-`k1`*
backstop — weaker than the **locked pre-inclusion coverage rule**. This is the ADR-0017
enforcement gap (committee signs on best-tip; GL0 adopts on quorum-sig; watchtower re-execs
after adoption). **Pro-guarantee: fix inside the re-exec model** by moving coverage before
inclusion — do not weaken the rule to match the code.

> Note: `SLASHING-DESIGN.md` and `COMMITTEE-SORTITION-DESIGN.md` are **historical/superseded**
> (stake-weighted admission; "not the current implementation"). Live values are
> `application.conf` + ADR-0016/0017.

---

## O-04 — ML0 response to a Phase-2 density reorg 🟡

**Freeze:** ML0's retained history + deterministic rewind/rebase/new-epoch contract when a
consumed GL0 Phase-2 ref is orphaned by a density reorg.

**Answer (from our work):** The three-way contract is drafted and consistent across four docs:
**(1)** retained history = append-only auditable ML0 history + rollback material through the
local horizon and every longer dependency (`k2` = capacity, not a validity floor); **(2)**
response order: append-only audit (preferred) → an app with a **registered deterministic
rebase/undo contract** appends a corrective snapshot → else a **new ML0 epoch at the last valid
GL0 ref**; **(3)** noninvertible external effects are integrator risk, not made reversible by a
new phase. Downstream follows exact hashes + explicit replacement events; orphaned inputs
requeue exactly once (`CONSENSUS-ARTIFACT-LIFECYCLE.md:759-767`;
`GENESIS-DENSITY-PHASE2-REORG-AUDIT.md:109-118`). `[RECOMMENDED]`

**Owner still decides:** formally freeze it and **reconcile a status tension** — the lifecycle
lists this as a *ratified* implementation-input (`CONSENSUS-ARTIFACT-LIFECYCLE.md:862-864`)
while the register keeps it **OPEN**. Also: the registration schema qualifying an app's
"deterministic rebase/undo contract," ML0's exact retained-history horizon and its
eviction→`RecoveryRequired` boundary, and the O-13 delivery-cursor-rewind interaction.

**Status:** the GL0 reorg gadget is **not yet one crash-consistent transaction**
(`GENESIS-DENSITY-PHASE2-REORG-AUDIT.md:24-36`); ML0's live `resyncToCanonical` is
ordinal-forward, not hash-bound Phase-2 replacement (`project_ml0_diff_adopt_design`). `[GAP]`

**Re-exec check:** OK — ML0 own-state rollback; refold re-runs the live execution path,
preserving committee+watchtower re-exec. Registered-rebase/proof-carried rollback is scoped to
currency-with-data (DL1), consistent with the rule.

---

## O-05 — Intake threshold and censorship recovery 🔴

**Freeze:** intake threshold, receipt/custody lifetime, queue ownership, durable-replication
requirement, redraw schedule, direct-fetch/censorship fallback.

**Answer (from our work):** The **separation is locked**: the binary-intake committee and the
execution committee are distinct draws; ML0 operators authenticate the binary but are not
committee members; intake receipts claim only authenticated source / checked
parent-ordinal-envelope / durable custody / availability and **never satisfy execution
`kQuorum`** (L-12/L-13, `CONSENSUS-OWNER-DECISIONS.md:25-26`). The L-13 "derive parent/ordinal
from authenticated state, not self-claim" duty is partially realized in
`MetagraphParentOrdinalResolver.scala:38-99`. `[LOCKED]` / `[PARTIAL]`

**Owner still decides — no recorded values for any of the six** freeze items. `[OPEN]`

**Status / config smell:** live code has **one shared committee draw** (`nakamoto.committee`,
`k-draw=8` / `k-quorum=6`, `application.conf:474-476`) doing per-metagraph admission; there is
**no distinct durable-custody intake layer** with receipts/replication. When the owner sets an
intake threshold it must be a **separate** value, or the "intake can't satisfy `kQuorum`"
invariant risks being collapsed in code.

**Re-exec check:** OK — L-12/L-13 explicitly bar intake receipts from counting toward execution
quorum.

---

## O-06 — Global correction authorization 🔴

**Freeze:** how a GL0 protocol correction of malformed metagraph state is authorized/activated,
plus the signed-correction schema. No metagraph-originated authority is an option.

**Answer (from our work):** The **forbidden case is locked** (L-18,
`CONSENSUS-OWNER-DECISIONS.md:32` — trust arrow is only `GL0 protocol → canonical GL0 state →
downstream rebase`; no ML0/CL1/DL1 or operator authority; a correction is a deterministic
root-covered GL0 transition, not a hidden producer override). The **activation vehicle has a
recorded direction**: an ordinal/hash-bound `ProtocolEra` boundary — deployed either as a
greenfield re-genesis cutover pre-launch (Shape A) or an ordinal-gated era boundary post-launch
(Shape B) (`21-workstream-hardfork.md:12-18,76-106`), with `EraCodecRegistry`'s validated
range-list as the disciplined seed (`ERA-REGISTRY-DESIGN.md:28-31`) and O-04 supplying the
downstream-rebase contract. `[LOCKED]` / `[RECOMMENDED]`

**Owner still decides:** the **authorization model itself** — hard-fork/era rule vs a future
canonical governance rule vs another deterministic GL0 mechanism (all three still open,
`:143-145`; no governance mechanism is designed anywhere); Shape A vs Shape B for a live-history
correction; and the **entire signed-correction schema** (signed domain, exact
`(metagraph, preRoot, version)` target, correction diff, post-root, reason, activation ordinal,
replay-protection field, audit trail) — **undesigned**; a prior attempt was deliberately
deleted (`21-workstream-hardfork.md:9-10`). `[OPEN]`

**Status:** `EraCodecRegistry` is **built but unwired** (greenfield `dev=0`, no production
dispatch); the unified `EraRegistry` is design-only (`ERA-REGISTRY-DESIGN.md:3,40`). `[GAP]`

**Re-exec check:** OK — pro-guarantee; L-18 bars metagraph-originated authority and consistent
with L-17's deletion of `authoritative*` overrides.

---

## O-07 — Economic operation grammar 🔴

**Freeze:** audit every v4.0.0 framework op; preserve ops with explicit deterministic
authority/conservation/ordering/replay; don't invent treasury/oracle authority; don't disable
working functionality just because its rule isn't restated; defective behavior gets a protocol
rule + RED/oracle vectors before enablement.

**Answer (from our work):** **Manual owner unlock and metagraph-source spend are intended,
authorized v4 features to preserve** — re-specified with explicit authority
(`V4-ECONOMIC-GRAMMAR-AUDIT.md`):
- **ECO-03** unsigned `TokenUnlock` — CONFIRMED intended (owner-signed unlock), authority
  **defective**; current `c610a0740` adds amount/currency/source equality
  (`TokenLockOpsManager.scala:161-173`) but that is not authorization (a Byzantine ML0 can copy
  public lock fields). Target = owner-signed `TokenUnlockIntent` derived from the canonical lock.
  `[PARTIAL]`/`[GAP]`
- **ECO-04** no-reference `SpendTransaction` — CONFIRMED intended metagraph-source spend
  (`SpendActionValidator.scala:258-261,319-328`); authority **gap**: GL0 doesn't bind the inner
  signer population/threshold to a canonical MG→ML0-operator registry. Target = explicit
  `MetagraphSpendIntent` with rooted registration + threshold + nullifier + settlement kernel
  (`V4-ECONOMIC-GRAMMAR-AUDIT.md:214-243`). `[PARTIAL]`/`[GAP]`
- `PricingUpdate` and protocol balance correction are real v4 mutators (correction routes to
  L-18/O-06, not metagraph authority).

**Owner still decides:** the exact authority predicate per op (esp. the canonical ML0-operator
registry/threshold/rotation for metagraph-source spend — couples to O-11), whether `PricingUpdate`
ships unchanged, and human classification after a **mechanical
constructor/codec/event/acceptance reachability inventory**. The gate is explicitly open until
that inventory + the **full RED/oracle corpus** land (~15 grammar rows + 12 vectors:
`ECON-AUTH-001/002`, `ECON-REPLAY-MGSPEND-001`, `ECON-RESERVE-001`, `ECON-LANE-001`, …
`V4-ECONOMIC-GRAMMAR-AUDIT.md:250-307`). `[OPEN]`

**Re-exec check:** OK, pro-guarantee — the audit *preserves* CL1 ops **with** committee
re-exec-before-sign; `ECON-LANE-001` forbids DL1 output synthesizing framework effects.

---

## O-08 — Tower proof contract 🟡

**Freeze:** snapshot-carried per-level state/pointers, trial computation, `N-2` registry / `N-1`
eta inputs, KES/VRF verification, SMT inclusion, proof comparison, size limits, cache
reconstruction, density-reorg rollback (L-22 locks eligibility but stages activation).

**Answer (from our work):** The contract is **designed end-to-end** and **partially
implemented**: per-level `subchainState` pointers (`NIPOPOW-PROPOSAL.md §2.3`; data landed
`NIPOPOW-IMPLEMENTATION-PLAN.md:66-71`), L independent domain-separated VRF trials (§2.1; S1
landed `:39-58`), MPT `(level,ordinal)` inclusion (§2.6), weight used **only** to compare proofs
never chain selection (§2.4), `m=50` / `k=k1` window (§2.5), KES-gated verification (§3-4). `N-2`
registry / `N-1` eta inputs match the register's O-11/O-17 constructs. `[RECOMMENDED]`/`[PARTIAL]`

**Owner still decides / blocker:** **L-22's activation precondition is unmet** — `smtRoot` is
currently *normalized out of consensus* (`smtRootBlind`) because it is path-dependent /
non-deterministic (`GlobalSnapshotConsensusFunctions.scala:281-294`), so the field a tower proof
relies on is **not** independently reproduced by every recipient today. Before activation:
make `smtRoot` deterministic **and** recipient-reproduced (or re-derive the tower commitment off
a non-consensus field), land the `MptTowerStore` (S3, restart/branch-aware reconstruction), and
**re-freeze the size params** — the proposal's `k=255`/`k2=65536` are stale vs the live
`k1=1024`, `k2=100·k1`, `R=3.1·k1`. `[GAP]`/`[OPEN]`

**Re-exec check:** OK — tower/NIPoPoW is light-client retroactive proof, explicitly not chain
selection and not economic validation (correctly proof-carrying for the serve path).

---

## O-09 — Pure opaque state-channel product scope 🟢

**Freeze:** whether a standalone opaque/data-only lane exists (product decision);
`FrameworkCurrency` + `FrameworkCurrencyWithData` are locked (L-16).

**Answer (from our work):** Default on record = **opaque/data-only lane is absent/disabled in
v1 unless the owner explicitly retains it**; if retained, authenticated inclusion/availability
only, **zero framework-economic write surface**, never selectable by decoder-probe
(`HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md:198-206`;
`CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md:290,745`; `docs/adr/0016:160`). `[RECOMMENDED]`

**Owner still decides:** the binary product choice — ship a standalone opaque lane in v1 or not.

**Condition:** enforcing L-16 requires closing **SER-02** (CONFIRMED, OPEN) — today the code
"decides framework-currency vs opaque DL1 by whichever JSON decoder succeeds"
(`CORRECTNESS-SECURITY-AUDIT-2026-07-11.md:204`); fix = one explicit signed lane/type envelope
so opaque bytes cannot be **decoder-promoted** into economics. Decoder-promotion *would* violate
the #1 rule, so this is the one guard that must land.

**Re-exec check:** OK — opaque/data-only is DL1-style authenticated carriage, barred from
economic writes (once SER-02 lands).

---

## O-10 — Portable shard-parent duty and slot bound 🟡

**Freeze:** the portable evidence validating a child checkpoint's parent-relative staircase
duty, plus the slot upper bound.

**Answer (from our work):** The register already carries the full drafted recommendation and it
is design-consistent (`CONSENSUS-OWNER-DECISIONS.md:189-204`;
`HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md:42-50`; `docs/adr/0017:61-64`): add an exact
`(ordinal, hash)` GL0 reference for the parent checkpoint to the child's **signed preimage**; a
verifier requires that GL0 snapshot Phase-2 under the hash-bound FinalityGate, extracts the
parent under the same shardId, hashes its canonical signing preimage to the child's
`parentCheckpointHash`, then uses its signed ordinal/slot for continuity + staircase duty; carry
the parent artifact/header or an authenticated inclusion proof (a bare claimed slot is
insufficient); single genesis sentinel; parent ref + bytes durably recoverable by a
gossip-missing node. **Slot bound:** the embedded checkpoint's signed slot ≤ the signed slot
certificate of the including GL0 snapshot. `[RECOMMENDED]`

**Owner still decides:** ratify the recommendation as written and schedule implementation.

**Status:** implementation is a **known RED gate** — the shard parent is resolved from a
receiver-local shard chain store (`SHARD-C-009`,
`ShardCheckpointGl0AcceptanceManager.scala:250-253`); the checkpoint binds an execution-base
ordinal + slot but **not** the exact Phase-2 `(ordinal,hash)` (`docs/adr/0017:196`;
`SHARD-CHECKPOINT-MONOTONICITY-DESIGN.md:57-62`); and **no slot ≤ GL0-snapshot-slot cap exists
anywhere** (`ShardCheckpointGl0AcceptanceManager.scala:232-233`). The target pattern already
exists on the ML0-binary side (`MetagraphParentOrdinalResolver`). `[GAP]`

**Re-exec check:** OK — anti-equivocation/validity, does not touch CL1 re-exec.

---

## O-11 — Permissionless GL0 operator roster 🔴

**Freeze:** the canonical rule turning a `PeerId` into an eligible GL0 operator — registration,
stake/collateral, minimum bond, activation/exit delay, slash/cooldown interaction, exact `N-2`
period-boundary root. Load-bearing for every uniform `1/N` admission/execution/watchtower draw.

**Answer (from our work) — the frame and containment are settled; the economics are not:**
- **Settled & enforceable:** registration ≠ membership (`:212`;
  `OPERATOR-CONSENSUS-KEY-REGISTRY.md:56-60`); uniform `1/N` over a delayed-canonical (`N-2`)
  population **intersected** with active KES+VRF pairs **and** positive stake, **fail-closed to
  "unavailable"** (never seedlist/key-only/live-peer) until a rooted roster exists
  (`HistoricalOperatorConsensusKeyRegistry.scala:117-204` — built and correct); a per-period
  boundary root retained (stake side already 4-period-retained,
  `GlobalSnapshotInfo.scala:169`). `[LOCKED]`/`[PARTIAL]`
- **Direction on record:** reuse v4 delegated-stake/node-collateral **event machinery** but
  **not its seedlist authority** (`feedback_new_tx_type_patterns`; the three v4 validators
  authorize via seedlist — `UpdateNodeParametersValidator.scala:74` etc.); genesis-first
  population injection (`feedback_stake_state_via_genesis`, HARD RULE); VRF-sortition + slashing
  as the anti-Sybil substitute for a stake-splitting ceiling. `[RECOMMENDED]`
- **The one quantitative safety result:** sharding amplifies adversary stake `α_local = α·S`;
  **`α_total > 1/(2S)` breaks per-shard honest-majority** (`project_cross_shard_cq_collapse_bound`;
  `sims/cross_shard.py:37-45`) — this is *why* a rooted minimum-bond/identity rule is mandatory,
  but it is not the numeric bond. `[RECOMMENDED]`

**Owner still decides (genuine economics — mostly UNDECIDED,** register `:281-294`): minimum
self-bond vs delegated/collateral backing + third-party collateralization; the quantitative
anti-stake-splitting bond; activation/exit/unbond/slash horizons (couples to O-03 challenge
lifetime + O-12 KES boundary); slash/cooldown timing + any liveness cap; derived-vs-explicit-event
membership (two candidate models on record, unadjudicated); the genesis population's exact
backing rule; and bounded registration/state-growth fees (overlaps O-17/R008-04). `[OPEN]`

**Status:** **no operator roster is rooted anywhere** in GSI/MPT; production still defines the
committee/validator population from the **local seedlist**
(`StakeRegistry.scala:32-45,68-85`; `SharedServices.scala:275,300-304`) — a forked-denominator,
Sybil-open authority that is exactly the `α_total>1/(2S)` exposure. `[GAP]`

**Re-exec check:** OK, pro-guarantee — the roster **defines who sits on the committee that
re-executes CL1**; it is the foundation of the re-exec model, never a substitute
(`OPERATOR-CONSENSUS-KEY-REGISTRY.md:238-241`). Closing the seedlist gap strengthens the
guarantee.

> Config nit (HOCON rule): `StakeRegistry.scala:102-107` reads
> `sys.env.get("NAKAMOTO_OPTIMISTIC_MIN_FRACTION")` directly — migrate to
> `SharedConfig.nakamoto.*`.

---

## O-12 — Runtime KES secret deletion and N-2 reorg boundary 🟡

**Freeze:** KES secret deletion point, quantified common-prefix failure probability, offline
escrow y/n, recovery/rejoin.

**Answer (from our work):** The baseline is drafted and on record
(`CONSENSUS-OWNER-DECISIONS.md:302-324`; `OPERATOR-CONSENSUS-KEY-REGISTRY.md:287-295`):
- **One-way secret deletion at each eta-period evolution boundary is already implemented,
  forward-secure, and e2e-validated** (`OperationalKeyMaker` + read-once `SecureStore.scala`
  with `SecureRandom` overwrite; `project_kes_live_wiring_validated`: 16 rotations / 8 nodes /
  13724 sigs / 0 invalid). `[PARTIAL-IMPL — mechanism live]`
- `N-2` prefix assumed common-prefix-stable at the ratified bound; a density reorg crossing an
  **erased-secret activation** → durable `RecoveryRequired` (stop signing, follow authenticated
  recovery, explicit realign/rejoin); operator cannot choose the branch; missing secret history
  is **not** slash evidence; **no** `k2` retention of old masters and **no** automatic secret
  rollback (that would weaken forward security). `[RECOMMENDED]`

**Owner still decides:** ratify the deletion point (currently the eta-period evolution
boundary); the **quantified common-prefix failure probability** (no number exists anywhere);
**offline escrow y/n** (recommendation leans no, unratified); the **recovery/rejoin procedure**
— which is design-only and **unwired** (as is `RecoveryRequired`, per L-23). `[OPEN]`/`[GAP]`

**Re-exec check:** OK — KES is signing-identity/forward-security, not CL1 economics; "missing
secret history cannot be slash evidence" aligns with `feedback_slashing_safety_bar`.

---

## O-13 — Durable global delivery sequence and settlement ordering 🟡

**Freeze:** replace `GlobalSnapshotsProcessed` + bounded-history reconstruction with a
hash-linked per-destination delivery sequence.

**Answer (from our work):** Adopt the PROPOSED design
(`CONSENSUS-OWNER-DECISIONS.md:326-361`): GL0 atomically appends canonical framework delivery
records + advances a **rooted outbox head** with settlement + **permanent authorization
nullifier**; ML0 stores an applied `(sequence, deliveryId)` cursor in `CurrencySnapshotInfo` +
state proof, executes a contiguous range from **one exact Phase-2 GL0 ref**, and emits a CAS
ack; pending records are individual rooted leaves; missing bytes **defer** (never authorize a
peer claim or skip an effect); **inbox-before-local-spend** is the recommended first ordering
rule. Delivery markers stay **framework-recomputed**, never DL1-supplied
(`CurrencySnapshotAcceptanceManager.scala:296,305-308`). `[RECOMMENDED]`

**Owner still decides:** the **full** total order among inbound-delivery / local-spend /
consume / cancel / expiry / refund (only the first rule is recommended); rooted byte/entry/pending
limits + backpressure + protocol fees ("local HOCON is not validity"); retention + authenticated
deep-recovery; the O-04 interaction after a density reorg orphans an applied ref; active
field-number allocation. `[OPEN]`

**Status:** motivated by a **live CRITICAL** — `GlobalSnapshotsProcessed` reconstructs over a
**50-snapshot window**; a pending ordinal outside it becomes eligible again and its adjustment
is **applied twice** (ECO-05, `CORRECTNESS-SECURITY-AUDIT-2026-07-11.md:146`). `[GAP]`

**Re-exec check:** OK — delivery/settlement is a GL0-owned framework kernel; nullifier + outbox
are GL0 state; markers framework-recomputed; cross-shard reads finality-first (Option A / L-10).

> `LOCAL-EVENTS-SERVICE-DESIGN.md` is a separate read-only observability stream — **not** this
> consensus delivery path; do not conflate.

---

## O-14 — Framework fee sequence and opaque-data binding 🟡

**Freeze:** reuse `MgLastFeeTxRefs` (field 27) as the strict per-source fee head + fee signing
contract; and the exact-bytes vs chunk-manifest choice.

**Answer (from our work):** Adopt the PROPOSED design
(`CONSENSUS-OWNER-DECISIONS.md:363-393`): field 27 is **already live and rooted**
(`GlobalStateKey.scala:258,331,390`) — use it as the strict per-MG/source head; a framework fee
signs network/genesis, era/lane, metagraph, source, dest, amount, exact parent ref, and an exact
opaque-data commitment; acceptance requires parent == rooted state, derives the successor, checks
available bytes **or a content-addressed chunk manifest without executing DL1 logic**, atomically
updates balances + head. v1 rules: 0-or-1 fee per custom item; any invalid fee rejects the whole
framework segment; outgoing fees reserve against pre-batch balance; no v1 expiry (one same-parent
sibling wins); resign-to-pay-again. **Recommended choice: content-addressed chunk-manifest root
only when every chunk is available before the execution signature, else exact signed-item
bytes.** `[RECOMMENDED]`/`[PARTIAL]`

**Owner still decides:** exact-bytes vs chunk-manifest; the interaction with E9's checkpoint-wide
reservation kernel for the outer `StateChannelSnapshotBinary.fee`; final field/codec allocation.
`[OPEN]`

**Status:** motivated by a **live CRITICAL** — a Byzantine ML0 can **reinclude a signed fee
until the source is drained** (ECO-18, `CORRECTNESS-SECURITY-AUDIT-2026-07-11.md:172`). `[GAP]`

**Re-exec check:** OK — the gate is explicit that GL0 verifies bytes/availability/authorization/
sequence/arithmetic/conservation and **never treats ML0 custom output as economic authority**.
⚠ Do **not** let the DL1 `data-with-fee` "authoritative-balances push" precedent
(`project_data_with_fee_shard_imbalance_grind`, on the now-superseded byte-diff/adopt
architecture) bleed into treating **framework** fees as authoritative-pushed — framework fees
are re-exec-verified.

---

## O-15 — Multi-tine Taktikos/Genesis frontier semantics 🔴

**Freeze:** A (total deterministic selector), B (frontier evidence + cutoff/bounded-diffusion +
late-reveal), C (security/liveness proof), D (exact `k1` short/deep metric), E (exact tie rule).
Full packet: [`O15-MULTI-TINE-FRONTIER-OWNER-REVIEW.md`](O15-MULTI-TINE-FRONTIER-OWNER-REVIEW.md).

**Answer (from our work):**
- **Settled:** the objective **property** (L-24 — same cutoff-complete frontier + params ⇒ same
  head, independent of enumeration/arrival/restart/incumbent) and the pairwise-rule **boundary**
  (L-04 — `maxvalid-tk` ≤ `k1`, `maxvalid-bg` > `k1`). `[LOCKED]`
- **A / B / C are genuinely OPEN** — not inherited from either cited paper. The current
  `selectBest` left-fold is **proven non-transitive** by retained RED witnesses (a strict Tk/Bg
  3-cycle, 3 permutations → 3 winners: `ChainSelectionSuite.scala:191-226`; store-path
  reproduction `NakamotoChainStoreSuite.scala:430-469`). A new total-frontier construction +
  security/liveness proof + portable frontier-evidence scheme (cutoff + bounded-diffusion +
  deterministic late-reveal) must be designed. `[OPEN]`
- **D is ratifiable now:** recommended `forkDepth = max post-MRCA suffix (excluding MRCA)`, Tk if
  ≤ `k1` else Bg — which **fixes a live code/spec mismatch**: `kLookback = k1+1`
  (`config/types.scala:177`) makes `depth = k1+1` still use Tk, contrary to L-04
  (`ChainSelection.scala:161-175`; RED `ChainSelectionSuite.scala:228-250`). `[RECOMMENDED]`
- **E is OPEN with a flagged tension:** the register keeps the current **lower-VRF-then-hash**
  tie rule *unratified* pending grinding/precomputation analysis (`:482-484`), while the standing
  memory instruction `feedback_vrf_tiebreaker_keep` is to **keep** it and not propose removal.
  Our sims do **not** quantify the specific VRF-tiebreaker grinding surface (the grinding suite
  measures the *rejected* weighted scheme). **Owner tension to resolve, not for the model to
  decide.** `[OPEN]`

**Owner still decides:** A (selector construction), B (evidence/cutoff params), C (the proof); ratify
D now; ratify-or-reject E after the missing grinding quantification.

**Re-exec check:** OK — GL0 fork choice only; a "valid tine" is snapshots that already passed
complete GL0 authentication; introduces no BFT (L-02).

---

## O-16 — Exact Phase-2 consumer lease and invalidation (FIN-14) 🔴

**Freeze:** ratify `CanonicalPhase2Lease` + the six sub-choices A–F. Packet:
[`P6-FIN14-PHASE2-CONSUMER-LEASE.md`](P6-FIN14-PHASE2-CONSUMER-LEASE.md).

**Answer (from our work) — all six recommended on record:**
- **A (invalidation granularity):** conservative **all-lease invalidation** on
  replacement/rollback via a monotone `CanonicalLineageRevision` (ABA-safe; no per-target
  dependency graph in V1). `[RECOMMENDED]`
- **B (purpose policy):** a **sealed exhaustive `Phase2UseScope`** (~18 cases, no
  Generic/Other/string), each with an explicit exact-ancestor-vs-current-P2-head + retention
  rule; confirms (not reopens) L-19. `[RECOMMENDED]`
- **C (attestation reuse):** retain **raw signed bytes only**, re-verify registry/VRF/KES/sig +
  exact anchor before reindexing; never copy a prior count/threshold. `[RECOMMENDED]`
- **D (historical age):** **no** receiver wall-clock/HOCON validity limit; any age bound must be
  branch-authenticated protocol data. `[RECOMMENDED]`
- **E (effect-journal conformance):** conform to locked **L-23** (idempotent scoped command +
  inverse/requeue; physical sink presence is never authority). `[RECOMMENDED]`
- **F (signed scope schema):** ratify an active-era binary/checkpoint shape binding the full
  `GlobalSnapshotStateRef` + registry/parameter era + purpose domain; the current ordinal/hash-only
  `GlobalSyncView` cannot be the final signed consumer scope. `[RECOMMENDED]`

**Owner still decides:** explicitly ratify all six (the packet recommends, does not decide).

**Status:** motivated by the live **FIN-14** defect (ordinal watermark transfers A's
qualification to unqualified B; the boolean adapter is live,
`GlobalSnapshotConsensus.scala:1409`); **zero** lease constructs are implemented; and activation
is **blocked on O-15 (selector) and O-01**. `[GAP]`/`[OPEN]`

**Re-exec check:** OK — the lease governs *which exact Phase-2 base* a consumer binds; the GL0
checkpoint-inclusion boundary still requires every signer to replay and validators to verify
committee/coverage + recompute the root. Strengthens, not substitutes, re-exec.

---

## O-17 — ROOT-008 GL0 partition grammar 🔴

**Freeze:** the seven partition-grammar anchors before schema activation. Packet:
[`ROOT-008-GL0-PARTITION-GRAMMAR.md`](ROOT-008-GL0-PARTITION-GRAMMAR.md).

**Answer (from our work) — 5/7 firm; 2 genuinely unanswered:**
- **R008-01 (retired IDs):** delete physical 3/6/21 from active GL0 + `fromInt`; delete 32 after
  ROOT-010 parity; keep numeric gaps (no renumbering); upstream-v4 disk only via offline typed
  import. `[RECOMMENDED]`
- **R008-02 (field-20 self-auth):** store `(EtaPeriod, HistoricalStakeSnapshot)` so the leaf
  reproduces its key. `[RECOMMENDED]`
- **R008-03 (field-23 self-auth):** store `(PeerId, KesRegistrationReference)` + separately prove
  the unique field-22 match. `[RECOMMENDED]`
- **R008-04 (resource/growth):** **UNANSWERED** — the numeric per-image/field/value/member
  budgets and the permanent nullifier/slash growth-or-compaction strategy must be owner-approved;
  `uint16`/JVM memory are not protocol bounds; needs an exact-once compaction/accumulator proof +
  `GROWTH-001`. `[OPEN]`
- **R008-05 (set-member identity):** canonical unsigned event/content-reference hashes for
  economic-event uniqueness; signed cert reference/domain for KES; freeze exact identity
  functions with ECON-G. `[RECOMMENDED]`
- **R008-06 (token-lock field-8/30 scope):** **UNANSWERED** — the owner must freeze the relation
  among `TokenLock.currencyId`, the global partition, and the network metagraph; the packet
  deliberately declines to pick (an economic-schema decision with ECON-G). `[OPEN]`
- **R008-07 (field-32 deletion gate):** confirm optional full-view witness parity, `None` vs
  `Some(empty)`, explicit ML0 population, and staged/backfill/restart/reorg behavior — a
  **conformance gate on locked L-15A**, not an option to retain an unrooted writable field.
  `[RECOMMENDED]`

**Owner still decides:** R008-04 numbers + growth strategy, R008-06 currency scope; confirm the
numeric-gaps-vs-renumber choice (must precede freeze); freeze R008-05 identity functions with
ECON-G.

**Status:** live defects — `fromInt` still **accepts** retired 3/6/21 and root-excluded 32
(`GlobalStateKey.scala:362-397`); field 32 is deliberately root-excluded, an "open
root-invisible-state defect" (`:311-335`); GROWTH-001 and ROOT-010 field-32 parity (ECO-F32) are
open. `[GAP]`/`[OPEN]`

**Re-exec check:** OK — ROOT-008 is a storage/structural-grammar layer that "does not prove
authorization, conservation, backing, replay, or transition"; the economic oracle (O-07/ECON-G)
stays separate. This is the "MPT-as-byte-source = representation, not re-exec removal"
distinction.

---

## Cross-cutting caveats (read before ratifying anything)

1. **The committee blind-sign enforcement gap is the umbrella issue.** ADR-0017 documents that,
   on the happy path, committee members sign on best-tip and GL0 adopts on quorum-signature
   **without re-execution** (only the producer re-execs); the watchtower is a *post-adoption*
   backstop. O-03's release rule (pre-inclusion positive coverage) and this fix are the same
   work. Several 🟡/🔴 readiness marks trace back here — the *design* is correct (committee
   re-exec is primary), the *implementation* has not closed the gate.

2. **Do not cite these superseded docs as live parameters:** `SLASHING-DESIGN.md` and
   `COMMITTEE-SORTITION-DESIGN.md` (stake-weighted admission — superseded by uniform `1/N`),
   `docs/review/10-reexec-byte-contract.md` and `docs/nakamoto/CURRENCY-APP-TOKEN-ENFORCEMENT.md`
   (byte-diff/adopt — owner-rejected 2026-07-11). The canonical model is committee
   re-exec-before-sign + watchtower backstop (ADR-0016/0017); live values are `application.conf`.

3. **The roots-only WRONG pattern.** Two memories (`project_trust_model_audit_roots_only_greenlight`,
   `project_roots_only_sharding_ii_decision`) and one fix-option in
   `project_adopt_gate_drops_allowspends_run26` advocate deleting the CL1 fold / roots-only
   substitution. **None of the answers above depends on them.** Roots are a storage commitment of
   committee-**re-executed** state, and proof-carrying is the DL1/light-client serve path only.

4. **Status tension to reconcile:** O-04 is a *ratified implementation-input* in
   `CONSENSUS-ARTIFACT-LIFECYCLE.md:862-864` but **OPEN** in the register. Pick one before it
   confuses an implementer.

5. **Dependency ordering.** O-16 and O-17 are hard-blocked on **O-15** (frontier selector);
   O-16 additionally on **O-01**. O-06's `ProtocolEra` vehicle and O-11's rooted `N-2` boundary
   share the same unbuilt substrate (a live `EraRegistry` / canonical-root ownership). O-08's
   activation depends on making `smtRoot` deterministic. Sequence accordingly.

6. **Live CRITICAL defects the owner should track alongside the gates:** ECO-04 (O-07 authority),
   ECO-05 (O-13 double-apply), ECO-18 (O-14 fee replay), FIN-14 (O-16), SER-02 (O-09
   decoder-promotion), SHARD-C-009 (O-10), retired-ID acceptance + field-32 root-exclusion (O-17),
   `smtRoot` non-determinism (O-08). Each is CONFIRMED and OPEN.

---

## Provenance

Compiled from: the register itself; the four companion packets (`O15-…`, `P6-FIN14-…`,
`ROOT-008-…`, `V4-ECONOMIC-GRAMMAR-AUDIT`); design docs under `docs/nakamoto/`; ADR-0016/0017;
`CONSENSUS-ARTIFACT-LIFECYCLE.md`, `CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md`,
`CONSENSUS-PROTOCOL-TEST-PLAN.md`, `CORRECTNESS-SECURITY-AUDIT-2026-07-11.md`; source code at
HEAD `5557ee084`; the simulation suite in `~/repos/research-nipopos-2026/sims`; and the project
memory. Every `file:line` is verifiable at that state; where prior work does not answer a gate,
it is marked a genuine owner decision rather than filled in.

</details>
