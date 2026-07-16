# Consensus Owner Decision Register — Answers & Ratified Directions (v2)

**Companion to:** [`CONSENSUS-OWNER-DECISIONS.md`](CONSENSUS-OWNER-DECISIONS.md)
**Revised:** 2026-07-15 · settled-answer source-audit baseline `ad13026d1`
**Supersedes:** the v1 draft that was rejected by adversarial audit at HEAD `e26ad406e`.
This revision reworks every gate against that audited source baseline, dispositions all
15 audit findings, and folds in the owner's refinement dialogue.

**Status:** Owner-ratified architecture and design direction. The owner has accepted the
recommendations and directions recorded here. Ratification does **not** assert that missing
protocol constants, schemas, reference models, RED vectors, or activation proofs already exist.
For O-15/O-16/O-17 the audited source packets remain the engineering and proof authority; this
document records which direction is settled and which executable freeze gates remain. `O-18` was
added after that ratification pass and is not answered by this document.
**Owner-question completeness:** `17/18` dispositioned. `O-01` through `O-17` are ratified;
`O-18` awaits an owner response in
[`O18-TRANSPORT-DA-BYTE-CONTRACT-OWNER-REVIEW.md`](O18-TRANSPORT-DA-BYTE-CONTRACT-OWNER-REVIEW.md).

---

## 0. Response to the audit (v1 → v2 disposition)

| # | Audit finding | Disposition in v2 |
|---|---|---|
| 1 | O-15 is not partly freeze-ready; the symmetric `max(post-MRCA suffix)` metric and the tie rule are not inherited from the papers. | **Accepted.** O-15 remains 🔴 across A–G, including D. The owner ratifies the single-common-anchor density/tower-evidence research direction, not an exact metric, tie, selector, or activation. |
| 2 | Committee blind-signing is not the current defect; the signing path re-executes and root-compares before `VerifiedShardCheckpoint`. | **Accepted.** Re-exec model corrected in §1 from `AGENTS.md:39-82`. The live gap is *pre-inclusion watchtower coverage* + complete adopter binding, not blind-sign. |
| 3 | The economic invariant was stated incompletely. | **Accepted.** §1 states the full chain: producer + every execution signer replay; adopter verifies cert + applies diff + **reproduces root**; watchtower backstop; admission never satisfies quorum. |
| 4 | Certified-diff adoption was **not** owner-rejected — it is the target; only ordinary-noncommittee universal CL1 replay and roots-only/authority adoption were rejected. Universal native GL1 execution was not rejected. | **Accepted.** v1 wrongly swept certified-diff adoption into "roots-only." The byte-identity obligation survives; the pre-fix APIs and authority claims preserved in `10-reexec-byte-contract.md` are historical evidence, not the current target contract. |
| 5 | Dependency reversed: O-17 structural + O-07 semantic → valid tines → O-15 → O-16. O-17 is not blocked on O-15. | **Accepted.** Corrected in the summary and cross-cutting §. |
| 6 | O-16 readiness overstated (A/C were recommendations; B/E are conformance; D/F need schema/era). | **Accepted.** The owner has since ratified A/C/F direction and B/E conformance; O-16 remains 🔴 because freshness, purpose inventory, schemas, and dependencies are not executable. |
| 7 | O-17 is not "5/7 firm." | **Accepted.** The owner has since ratified the packet directions, including R008-06; O-17 remains 🔴 until identity, resource, codec, and proof gates close. ECO-F32 = HIGH. |
| 8 | O-01 not proven/freeze-ready; `SnowballAccumulator` has no K/α sample cascade and is arrival-order sensitive; sims are calibration, not a proof or `T_weight` freeze. | **Accepted.** O-01 remains activation-blocked; values are **owner-ratified provisional calibration with sim references**, and the sim-vs-impl proof gap remains explicit. |
| 9 | Local HOCON defaults are not protocol law. | **Accepted.** O-03 and every parameter gate require derived, tested, frozen active-era parameters, never overridable local defaults. |
| 10 | `MptTowerStore` and wiring already exist. | **Accepted.** O-08 corrected: store exists; durable-restart / branch-aware / activation-proof remain open. |
| 11 | Positive stake is not a settled operator-membership predicate. | **Accepted.** O-11 stays 🔴; Cardano-style pledge/self-bond, proportional delegation slashing, and `bond >= extractable value` are ratified, while exact rooted predicates and values remain engineering. |
| 12 | The KES end-to-end run is not repository-verifiable. | **Accepted.** O-12 no longer asserts the 16-rotation/8-node/13,724-sig figure as fact; it is marked an unverified external claim. |
| 13 | Severity labels inflated. | **Accepted.** CRITICAL = ECO-04, ECO-05, ECO-18, SHARD-C-009. HIGH = FIN-14, SER-02, ECO-F32, SMT-01. |
| 14 | Provenance claim false (compiled behind HEAD; cited out-of-repo memory as reproducible). | **Accepted.** v2 verified at HEAD `ad13026d1`; committed-source citations are distinguished from out-of-repo memory context, which is labeled as such and not treated as reproducible. |
| 15 | O-05 draw described incorrectly. | **Accepted.** Admission and execution are distinct draws sharing a parameter block; the missing construct is a separately-typed custody/admission threshold that can never satisfy execution quorum. |

---

## 1. The re-execution model (corrected, from `AGENTS.md`, Economic Authority)

This is the invariant every answer is held to. Stated completely:

- **Every GL0 validator** independently executes and validates direct native
  GL1/DAG-token transitions against the exact global proposal parent. No shard
  checkpoint, execution certificate, or CL1 diff can authorize or bypass that
  path. The target additionally requires every GL0 validator to run the
  deterministic global conflict/nullifier/settlement kernel. E9 and the live
  `numShards <= 1` bypass make that a current gap, not a completed guarantee
  (`GlobalSnapshotAcceptanceManager.scala:2372-2384`).
- **Producer + every execution-committee signer** independently re-execute the exact ordered
  CL1 inputs at the same Phase-2 GL0 base; a signer signs **only** when its byte-identical
  canonical diff **and** resulting root match. Missing inputs ⇒ defer/no-sign
  (`AGENTS.md`, Economic Authority).
- **A noncommittee GL0 adopter** verifies the execution certificate, applies the
  namespace-bounded diff to the signed base, and **recomputes the root** — it does not blindly
  install a claimed root, and it does not re-run ordinary CL1 (`AGENTS.md`, Economic Authority). This is
  **certified-diff adoption with adopter root reproduction** — the target model, not a rejected
  one.
- **Watchtowers** are the noncommittee collusion backstop; **positive assigned replay coverage
  is required before GL0 inclusion**; its population/deadline/retry/bond/resource parameters are
  **open (O-03)** (`AGENTS.md`, Economic Authority).
- **ML0 / admission / DA** signatures authenticate input only and **can never satisfy execution
  quorum** (`AGENTS.md`, Economic Authority).
- **Forbidden:** `authoritative*`, `AdoptFromSignedFields`, and **roots-only economic adoption**
  (adopting a claimed root without recomputing it) (`AGENTS.md`, Economic Authority).

The distinction v1 missed: **roots-only adoption is forbidden; certified-diff adoption where the
adopter recomputes the root is the target.** Both involve a diff; only one skips the recompute.

## 2. Provisional-values policy (per owner instruction)

Where our sims calibrated a value, this doc records it as a **provisional number with a sim
reference**, explicitly marked *calibration evidence, not proof*, to be re-validated against the
actual implementation. This applies especially to O-01 (the shipped `SnowballAccumulator`
differs from the idealized sim) and O-15C (existing bounds were measured for the *current*
pairwise rule, not a new selector). Provisional numbers are a starting point to return to under
test, never a freeze.

## 3. Legends

**Readiness** — 🟢 owner direction and executable contract ready to freeze · 🟡 owner direction ratified, engineering freeze gates remain · 🔴 stop-the-line research/schema/parameter/proof gate.
**Evidence tags** — `[LOCKED]` `[OWNER-RATIFIED DIRECTION]` `[PARTIAL]` `[GAP]` `[OPEN ENGINEERING]`.
**Provenance** — citations to `path:line` are verifiable at HEAD `ad13026d1`; italic *(memory)* notes are out-of-repo context, not reproducible from committed source.

---

## Summary

| Gate | Title | Ready | One-line answer |
|---|---|:--:|---|
| O-01 | Avalanche parameters | 🟡 | Sampled K/α/β direction and provisional `(K=8,α=5,β=10,Δ=slot/2)`, N≥16 else T_depth1 are owner-ratified; implementation parity, `T_weight`, network constants, and proof remain open engineering. |
| O-02 | Deep-history recovery | 🟡 | Owner-ratified as authenticated chain-sync/rejoin from the objectively selected MRCA; atomic abandon/reconstruction and archive availability policy remain engineering. |
| O-03 | Watchtower parameters | 🔴 | Pre-inclusion positive replay and fixed-approval/redraw direction are ratified; population, coverage, deadline, bond, and replay-budget constants remain activation-blocking parameter work. |
| O-04 | ML0 density-reorg response | 🟡 | Owner-ratified rollback to the last GL0-embedded snapshot, with registered deterministic rebase or new epoch; schema and retained horizon remain engineering. |
| O-05 | Intake threshold | 🔴 | Separate typed custody/admission threshold is ratified and can never satisfy execution quorum; its schema and six protocol parameters remain open engineering. |
| O-06 | Global correction authorization | 🟡 | V1 uses binary/social `ProtocolEra` hard forks; a live Shape-B signed correction mechanism is deferred engineering, not a current owner gate. |
| O-07 | Economic operation grammar | 🟡 | The allowlisted service-metagraph data exception and four guardrails are ratified; the mechanical inventory, exact predicates, and RED/oracle corpus remain activation gates. |
| O-08 | Tower proof contract | 🟡 | Tower direction is ratified and partly staged (**`MptTowerStore` exists**); deterministic recipient reconstruction, reorg/restart behavior, proof, and size parameters remain open. |
| O-09 | Opaque state-channel scope | 🟢 | Retain the limited authenticated DA-only opaque lane for v4 functionality; it has zero framework-economic authority and requires an explicit signed lane, never decoder selection. |
| O-10 | Portable shard-parent duty | 🟡 | Exact Phase-2 `(ordinal,hash)` signed parent reference and containing-slot cap are ratified; implementation remains `SHARD-C-009` (CRITICAL). |
| O-11 | Operator roster | 🔴 | Cardano-style pledge/self-bond plus Cosmos-style proportional delegation slashing and `bond >= extractable value` are ratified; rooted schema and per-network bounds remain open engineering. |
| O-12 | KES secret deletion / N-2 reorg | 🟡 | No-`k2`-secret-retention and `RecoveryRequired`/rejoin baseline is ratified; common-prefix quantification and recovery implementation remain engineering. |
| O-13 | Durable delivery sequence | 🟡 | Hash-linked per-destination sequence, rooted outbox/nullifier/cursor, and inbox-before-local-spend are ratified; complete ordering, bounds, and field allocation remain engineering. |
| O-14 | Framework fee sequence | 🟡 | Field-27 strict head, v1 rules, and conditional manifest-versus-exact-bytes binding are ratified; codec and E9 reservation work remain. |
| O-15 | Multi-tine frontier | 🔴 | L-24/L-04 and the single-common-anchor density/tower-evidence-only direction are ratified; exact selector, boundary, metric, tie, evidence, overflow rule, and proof remain stop-the-line research. |
| O-16 | Phase-2 consumer lease | 🔴 | Conservative invalidation, raw-byte re-verification, L-19/L-23 conformance, and full signed exact-ref direction are ratified; freshness policy, purpose inventory, schema, and dependencies remain stop-the-line engineering. |
| O-17 | ROOT-008 partition grammar | 🔴 | Numeric gaps/offline import, self-authenticating fields, token-lock scope, and field-32 direction are ratified; identity functions, resource parameters, codecs, and proofs remain stop-the-line engineering. |
| O-18 | Transport and DA byte contract | 🔴 | **OWNER RESPONSE REQUIRED:** active-era maxima, migration scope, canonical bytes/compression, descriptor/chunk delivery, and the `512000`/`20 MiB` rule semantics are not ratified. Bounded helpers are unwired. |

---

## O-01 — Avalanche parameters 🟡

**Owner-ratified provisional calibration (not proof):** `(K=8, α=5, β=10, Δ=slot/2)`, floor **N≥16** else fall
back to T_depth1 — from `~/repos/research-nipopos-2026/sims/avalanche_attestation_calibration_gpu.py`
(6696-cell × 10k-trial sweep, data `sims/data/avalanche_attestation_full_gpu_n10000_v2.json`;
write-up `docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md §0.A/§0.M`). `T_weight` provisional from
`sims/mithril_quorum_threshold.py`. Sampling: uniform **without replacement**. Attestation
lifetime: **emit-once** at first β-clear. Epoch: registry `N-2`, eta `N-1`, grinding-checked
(`sims/run_grinding_ci.py`). `[OWNER-RATIFIED DIRECTION — PROVISIONAL PARAMS]`

**Open engineering/research gate (audit #8, must close before any freeze):** the shipped
`SnowballAccumulator.scala` has **no K-query/α-sample cascade** and is arrival-order
sensitive. The unsafe cumulative-weight finality sink has been removed; canonical depth-`k1` is
currently the only state-changing GL0 Phase-2 rail, while verified attestations remain telemetry.
The sim modeled an *idealized* Snowball the code does not yet implement, so the numbers cannot be
frozen until the accumulator implements the sampled cascade and is re-validated. Config posture:
only `snowball-beta` is HOCON
(`application.conf:343`); K/α are code constants. Derive, test, and bake every final value per
network as active-era protocol law.

The sampled K/α cascade and N<16 depth fallback are the ratified direction. Engineering must
implement the cascade, derive and freeze an executable `T_weight`, and rerun the reference,
grinding, convergence, and adversarial calibration before activation.

## O-02 — Deep-history recovery = chain-sync 🟡

**Owner-ratified direction:** a deep reorg whose true MRCA predates local `k2` retention is
**recovered by the same authenticated historical-sync the rejoin/long-offline path uses.** Once
objective fork choice selects the winning `maxvalid-bg` tine, the node conceptually rolls back to
the deep MRCA and **ChainSyncs up the winning branch to its tip** — exactly `SYNC-PROTOCOL.md:94-132`
("Historical Sync… once caught up (local tip ≥ network tip − 2), transition to Ready"), and the
O-15 packet agrees (`O15-…:73-75,404-405`). This **collapses O-02 into the sync protocol** plus
three additions: (1) **objective trigger** — recovery obtains inputs, never picks the winner
(L-05); (2) **atomic abandonment** of the orphaned branch's state + downstream events
(`CONSENSUS-ARTIFACT-LIFECYCLE.md:744-757`); (3) **authenticated reconstruction** vs the winning
tine's commitments, no social choice. `[OWNER-RATIFIED DIRECTION]`

**Open engineering gate:** specify atomic abandonment/reconstruction and minimum archive service.
A single peer may supply history or a NiPoPoW/tower proof, but proof verification against the
canonical commitment decides truth; peer diversity is availability hardening, not consensus
authority. **Re-exec:** OK — refold runs the live execution path; no peer-GSI install authority.

## O-03 — Watchtower parameters 🔴

**Owner-ratified direction (template = Polkadot approval-checking):** the shard model maps onto
Polkadot's **backing group (execution committee) + approval checkers (watchtowers)**: checkers
are **VRF-secretly self-selected** (the unpredictability the owner wants), any checker whose
re-exec disagrees raises a **dispute → escalate → slash**. Two thresholds, kept distinct:
- **Safety/veto:** *one* honest watchtower mismatch must trigger the fraud-proof/slash path.
- **Liveness/affirmative coverage:** how many must *affirm* before inclusion. A flat **1/3**
  (owner's proposal) is defensible, but Polkadot's **fixed `needed_approvals` + "no-show"
  redraw** degrades more gracefully than a fraction of the whole set being online. The
  fixed-approval plus no-show-redraw shape is the V1 direction; the exact approval count is not
  yet frozen.

**Stake-to-produce vs bond-to-verify/slash (owner's split):** reasonable and precedented
(Polkadot **fishermen**, Lightning watchtowers). Guard the **lazy-watchtower** failure: VRF
assignment (already planned) + the affirmation must *carry the re-exec result* + random audits.
Reward side already exists (`bounty-fraction=1/20`). `[LOCKED DIRECTION / OPEN PARAMETERS]`

**Open parameter/schema gate (not HOCON defaults, audit #9):** derive and freeze watchtower set
size, fixed affirmative approval count, assignment anchor, deadline, retry/redraw, challenger
bond, replay budget, and censorship fallback. No numeric value is implied here. **Re-exec:** OK
— pre-inclusion coverage is pro-guarantee.

## O-04 — ML0 density-reorg response 🟡

**Owner-ratified direction (standard L2-follows-L1):** the metagraph treats a GL0 density reorg as
a **rollback to the last currency snapshot embedded in the winning GL0 branch** — its canonical
state is only what the finalized GL0 chain embeds. This is the mainstream shared-security model:
- **Polkadot parachains** — a parachain block is only as final as its backing relay block; relay
  reorg → candidate orphaned → collator re-proposes on the new parent.
- **Ethereum rollups** — L2 state is a function of L1 data; L1 reorg → L2 re-derivation.
- **Cosmos/IBC** — avoids it by acting only on finalized state (the "Option A finality-first"
  already chosen for cross-shard).

Levers (both on record): **fast finality to shrink the reorg window**, and a **registered
deterministic rebase/undo** for replayable apps (else new ML0 epoch at last valid GL0 ref).
Non-invertible external effects are integrator risk — same as every L2. `[OWNER-RATIFIED DIRECTION]`

**Open engineering gate:** reconcile stale status labels mechanically, then define the
rebase/undo registration schema and ML0 retained-history horizon. **Re-exec:** OK — ML0
own-state rollback; refold re-runs the live execution path.

## O-05 — Intake threshold 🔴

**Owner-ratified layering (template = Polkadot pipeline):** the model is consistent
with the design. ML0 (currency-l0) = L2-specific BFT, no global responsibility, pushes
incrementals to GL0 (`AGENTS.md:55-57`). The three GL0 committees map cleanly to Polkadot:

| GL0 layer | Role | Polkadot analog |
|---|---|---|
| Intake committee | authenticate source, check parent/ordinal/envelope, durable custody + availability | availability distribution |
| Shard execution committee | re-execute CL1, diff+root, sign-on-match | backing group |
| Shard watchtowers | independent re-exec, dispute/slash | approval checkers |

**The correction (audit #15):** intake and execution are **distinct draws** but currently share
one parameter block (`nakamoto.committee`, k-draw/k-quorum). The missing construct is a
**separately-typed custody/admission threshold that can never satisfy execution `kQuorum`**
(L-12/L-13). `[LOCKED DIRECTION / OPEN PARAMETERS]`

**Open parameter/schema gate:** derive and freeze the custody threshold, receipt/custody
lifetime, queue ownership, durable-replication rule, redraw schedule, and censorship fallback.
**Re-exec:** OK.

## O-06 — Global correction authorization 🟡

**Locked V1 owner decision:** **binary/social governance through an ordinal/hash-bound
`ProtocolEra` hard fork now; an on-chain governance/adoption mechanism later.** Operators run
the release binary that encodes the correction/era rules; a formal on-chain governance/voting
layer is deferred. This is consistent with L-18 (trust flows *down* only;
metagraphs never force an economic view onto the hypergraph, `AGENTS.md:61-63`) and
`feedback_greenfield_no_wire_compat` *(memory)*. `[LOCKED V1 DIRECTION]`

**Sharpening:** "governance via binary" fixes *who authorizes*; a correction still needs a
deterministic **on-chain mechanism** so every node applies a byte-identical change — an exact
`(metagraph, preRoot→postRoot)` target, an **activation ordinal**, replay protection — i.e. the
ordinal/hash-bound `ProtocolEra` boundary. The abandoned configurable
`EraCodecRegistry` and legacy bridges are deleted; the landed
`ProtocolEraId.ScodecV1` is only a strict dark identity. The actual branch-bound
activation schedule and transition remain open engineering, not local HOCON or
an ordinal-range selector.

**Deferred engineering:** a signed-correction schema (target/diff/post-root/reason/activation-
ordinal/audit-trail) and live Shape-B correction implementation are not V1 prerequisites. A
future re-genesis import remains separate migration work. **Re-exec:** OK — pro-guarantee (no
metagraph-originated authority).

## O-07 — Economic operation grammar 🟡

**Owner-ratified resolution — the "oracle exception":** specialized **service metagraphs**
(e.g. a price metagraph) that publish data consumed by other services are a **network-defined
allowlisted exception**, not "invented oracle authority." This resolves the O-07 tension. The
four guardrails that keep it inside the #1 rule:
1. **Allowlist is network-defined/rooted** (genesis or era-governed), never self-claimed.
2. **Oracle output is authenticated-source *data*, not self-authorizing value** — it cannot mint
   or move framework balances on its own.
3. **Downstream economic consumption is still re-executed** — a consuming metagraph's CL1 uses
   the value as a *signed input* (finality-first, like a cross-shard read); the oracle is a
   witness, not an authority (`AGENTS.md:64-68` — DL1 data carriage "never extends to CL1
   economics").
4. **The oracle metagraph's own economics** are CL1-re-executed by its committee like any
   metagraph; only its *data-publish lane* is privileged (likely the v4 `PricingUpdate` op).

Separately, **manual owner unlock (ECO-03)** and **metagraph-source spend (ECO-04, CRITICAL)**
are intended, authorized v4 features to *preserve* with explicit authority (owner-signed
`TokenUnlockIntent`; `MetagraphSpendIntent` bound to a rooted registry/threshold/nullifier) —
not delete, not treasury/oracle-invent (`V4-ECONOMIC-GRAMMAR-AUDIT.md:104-249`).
`[OWNER-RATIFIED DIRECTION]`

**Open engineering gate:** complete the mechanical constructor/codec/event/acceptance inventory,
the RED/oracle corpus (`ECON-AUTH-*`, `ECON-LANE-001`, …), and exact per-op authority predicates
before enablement. **Re-exec:** OK — the oracle exception is a *data-input* exception, not an
economic-authority one; consumption is re-executed.

## O-08 — Tower proof contract 🟡

**Owner-ratified direction:** the proof contract is designed end-to-end (`NIPOPOW-PROPOSAL.md §2-4`) and
**partially landed as staged, production-dark components — including `MptTowerStore` (audit #10,
`MptTowerStore.scala:104-115,264-275`).** The production GL0 resource explicitly sets the proof
provider to `None`; the staged `TowerCatchupCoordinator`/`TowerFinalizer` are not attached to runtime
finality or proof-serving authority (`GlobalSnapshotConsensus.scala:1446-1456`). Per-level pointers,
L domain-separated VRF trials, MPT `(level,ordinal)`
inclusion, weight-for-proofs-only, KES-gated verification are on record; N-2 registry / N-1 eta
inputs match O-11/O-17. `[OWNER-RATIFIED DIRECTION / PARTIAL]`

**Blocker (L-22 activation):** the active era requires exact state-proof equality and rejects every
snapshot carrying `Some(smtRoot)`; the old `smtRootBlind` normalization is removed. Future
activation still requires one deterministic, branch-bound construction that every recipient
reproduces before acceptance, plus durable restart/reorg reconstruction and an activation proof.
The size parameters must also be re-frozen (proposal `k=255` is stale vs live `k1=1024`).
**Re-exec:** OK — light-client proof, not chain selection or economics. SMT-01 = HIGH.

## O-09 — Opaque state-channel scope 🟢

**Locked owner decision:** retain the limited opaque/data-only lane needed to preserve v4
state-channel functionality. It receives authenticated inclusion/availability semantics only,
has zero framework-economic write surface, and is never decoder-selected
(`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md:196-211`;
`docs/adr/0016-execution-sharding-reexecution-and-cross-shard-reads.md:158-170`). `[LOCKED]`

**Condition:** close **SER-02 (HIGH)** — today lane selection is by whichever JSON decoder
succeeds; fix = one explicit signed lane/type envelope so opaque bytes cannot be decoder-promoted
into economics (decoder-promotion *would* violate the #1 rule). The product decision is closed;
the explicit envelope and negative decoder-promotion tests remain engineering.

## O-10 — Portable shard-parent duty 🟡

**Owner-ratified direction** (`CONSENSUS-OWNER-DECISIONS.md:200-223`): put an
exact `(ordinal, hash)` Phase-2 parent reference in the child's **signed preimage**; verifier
requires that GL0 snapshot Phase-2, extracts the parent under the same shardId, hashes to the
child's `parentCheckpointHash`; carry the parent artifact/header or inclusion proof; slot ≤ the
including GL0 snapshot's slot cert; single genesis sentinel. `[OWNER-RATIFIED DIRECTION]`

**Status:** impl is `SHARD-C-009` (**CRITICAL**) — parent resolved from a receiver-local shard
store; the checkpoint binds an execution-base ordinal but **not** the exact Phase-2 `(ordinal,hash)`;
and **no slot cap exists**. Target pattern exists on the ML0-binary side already. Scheduling and
implementation are open engineering. **Re-exec:** OK.

## O-11 — Operator roster 🔴

**Settled frame:** registration ≠ membership; uniform 1/N over a delayed-canonical **N-2**
population **intersected** with active KES+VRF pairs, the rooted minimum self-stake/self-bond
predicate, and delegated backing; **fail-closed** to
"unavailable" (never seedlist/key-only) until a rooted roster exists
(`HistoricalOperatorConsensusKeyRegistry.scala:117-204`). `[LOCKED / PARTIAL]`

**Answer (owner-refined; Cardano + slashable bond):** Cardano registers pools with a **pledge**
(operator's own committed stake) + delegation on top, snapshots stake at epoch boundaries used
**N-2** — but **Cardano does not slash.** The owner wants Cardano's pledge **plus a slashable
bond** (this project *does* slash). The plan — **min self-stake to register KES/VRF + for
stake-snapshot inclusion; delegation allowed; operator min self-bond required for committee/
production eligibility** — is the Cosmos `min_self_delegation` + Cardano-pledge hybrid and is
sound. `[OWNER-RATIFIED DIRECTION]`

**Ratified load-bearing invariants (audit #11 — positive stake alone permits splitting):**
1. Delegated stake is slashed proportionally with the operator, Cosmos/Polkadot style, so
   delegation is security-additive rather than only liveness-additive.
2. **Bond ≥ extractable value in one fraud window** (ties to O-03/ADR-0017). The minimum bond
   is a safety margin, not merely Sybil resistance.

**Values & config:** no universal number (Eth 32 fixed; Cosmos `min_self_delegation` per-chain;
Polkadot dynamic-from-election; Cardano pledge+`a0`). The structure is ratified; derive and test
the numbers per network. Bake them as compiled per-network constants keyed by `AppEnvironment`
(like `k1`), not `${?ENV}`-overridable, then root the active parameters. The one HOCON nit:
`StakeRegistry.scala:102-107` reads `sys.env.get("NAKAMOTO_OPTIMISTIC_MIN_FRACTION")` — migrate
to `SharedConfig.nakamoto.*`. **Re-exec:** OK — the roster *defines the committee that
re-executes*; closing the live seedlist-defined-population gap (`SharedServices.scala:300-304`)
strengthens the guarantee (it is the `α_total>1/(2S)` exposure, `sims/cross_shard.py`).

## O-12 — KES secret deletion / N-2 reorg 🟡

**Owner-ratified baseline** (`OPERATOR-CONSENSUS-KEY-REGISTRY.md:287-295`): one-way secret
deletion at each **eta-period evolution** boundary (forward-secure `SecureStore` mechanism is
present in code); N-2 prefix assumed common-prefix-stable; a density reorg crossing an
erased-secret activation → durable `RecoveryRequired` + explicit rejoin; missing secret history
is **not** slash evidence; **no** `k2` master retention / automatic secret rollback.
`[OWNER-RATIFIED DIRECTION]`

**Provenance (audit #12):** the "16-rotation / 8-node / 13,724-sig" e2e figure is an *unverified
external claim* — no committed test/log reproduces it; treat as unverified. **Open engineering
gate:** quantify the common-prefix failure probability and implement the recovery/rejoin
procedure (`RecoveryRequired` remains unwired per L-23). Offline escrow economics are deferred
and non-load-bearing for V1; V1 safety cannot assume escrow or `k2` secret rollback. **Re-exec:** OK.

## O-13 — Durable delivery sequence 🟡

**Pending owner packet:**
[`O13-ALLOW-SPEND-TERMINAL-ORDER-OWNER-REVIEW.md`](O13-ALLOW-SPEND-TERMINAL-ORDER-OWNER-REVIEW.md).
Its candidate-universe, conflict, expiry, partial-consume, ML0 application, and
same-proposal recommendations require owner disposition; its exact source-locator
and native-versus-checkpoint ordering schema remains an explicit implementation
gate.

**Owner-ratified direction** (`CONSENSUS-OWNER-DECISIONS.md:350-386`): hash-linked
per-destination sequence, rooted outbox head + permanent nullifier, ML0 applied-`(sequence,
deliveryId)` cursor, contiguous execution from one exact Phase-2 ref, CAS ack, pending records as
rooted leaves, missing bytes **defer**; **inbox-before-local-spend** the first ordering rule;
delivery markers stay framework-recomputed. `[OWNER-RATIFIED DIRECTION]`

**Motivation:** ECO-05 (**CRITICAL**) — the 50-snapshot reconstruction window double-applies a
pending ordinal that falls outside it. **Open engineering/schema gate:** define the full total
order among the six op types, rooted limits/backpressure/fees, retention/deep-recovery, O-04
interaction, and field allocation. **Re-exec:** OK — GL0-owned framework kernel.

## O-14 — Framework fee sequence 🟡

**Owner-ratified direction:** reuse the already-rooted `MgLastFeeTxRefs` (field 27, `GlobalStateKey.scala:258`) as
the strict per-source head; fee signs network/era/lane/metagraph/source/dest/amount/parent-ref/
opaque-data-commitment; parent must equal rooted state; check availability **without executing
DL1**; atomic balance+head update; v1 rules on record (0-or-1 per item; invalid ⇒ reject whole
segment; reserve pre-batch; no v1 expiry; resign-to-pay-again). The ratified conditional binding is
**chunk-manifest root only when all chunks are available pre-signature, else exact bytes.**
`[OWNER-RATIFIED DIRECTION]`

**Motivation:** ECO-18 (**CRITICAL**) — a signed fee can be re-included until the source drains.
**Open engineering/schema gate:** encode the ratified conditional manifest/exact-bytes shape,
close the E9 outer-fee reservation interaction, and allocate the field/codec. **Re-exec:** OK — GL0 verifies authorization/arithmetic/conservation, never treats
ML0 custom output as authority. *Do not* let the DL1 `data-with-fee` "authoritative push"
precedent bleed into framework fees.

## O-15 — Multi-tine frontier 🔴

**Authority:** [`O15-MULTI-TINE-FRONTIER-OWNER-REVIEW.md`](O15-MULTI-TINE-FRONTIER-OWNER-REVIEW.md).
Owner-ratified: the objective-result **property** (L-24), the conceptual Tk/Bg **boundary**
(L-04), and the single-common-anchor density direction with the tower used only for portable
evidence. The observation protocol, exact selector, short/deep metric and equality, tie rule,
evidence schema, activation proof, and Byzantine-overflow rule remain stop-the-line research
(`O15-…:225-238`). Ratified direction is not an executable selector.

**What the primary papers actually specify:**

- Published Taktikos Algorithm 1 sets `C <- Cloc` and processes `C1..Cj` in
  sequence. A valid candidate that forks from the current incumbent by at most
  `k` replaces it only when the candidate is longer, or when lengths are equal
  and the candidate's head slot is strictly earlier. A deeper candidate and an
  exact length/slot tie retain the incumbent. The algorithm contains no VRF/hash
  final tie and does not define an incumbent-independent set argmax
  (published p. 6; prepublication Appendix A.1, pp. 22-23).
- Genesis Figure 7 also starts at `Cmax <- Cloc` and processes candidates in
  sequence. Its short-fork test is relative to that current incumbent; its deep
  condition uses the pair's most-recent common slot and a strict density win.
  Neither paper specifies a consensus candidate enumeration order or proves that
  this fold is a permutation-independent total-frontier function.
- Taktikos's prepublication bounds assume static stake/registration and a
  synchronous, time-homogeneous process; `FINIT` fixes a constant genesis nonce.
  Its Appendix A proposes the bounded-delay fold but does not prove L-24 or this
  project's dynamic `N-2` stake, `N-1` eta, and KES/VRF/registry-era composition.

**What the repository evidence proves:**

- The Scala comparator is a project variant, not a literal implementation of
  Algorithm 1. It uses a symmetric ancestry walk, a symmetric maximum-suffix
  depth, mixed Tk/Bg pairwise comparison, and a lower-VRF-then-hash final tie.
- Under a synthetic `k`/density configuration, the retained A/B/C fixture forms
  strict mixed Tk/Bg preferences whose left-fold result changes with permutation
  (`ChainSelectionSuite.scala:191-226`). The store fixture likewise finishes at
  different heads for three parent-before-child arrival schedules
  (`NakamotoChainStoreSuite.scala:280-372,426-469`).
- Those fixtures prove order-sensitive behavior in the current comparator/store
  control flow. They bypass complete VRF/KES/eta/era snapshot admission and do
  not prove that published Taktikos is broken, that the synthetic frontier is
  reachable under an active environment, or that the resulting divergence is
  persistent under subsequent valid block production.

**Engineering disposition:** the dedicated packet's O-15A through O-15G remain activation
gates. The source decision register groups the remaining work separately as A through F; those
labels are not a one-to-one subgate mapping. The owner-ratified research direction does not
authorize the current symmetric max-suffix rule, an unproved scalar ordering, the VRF/hash tie,
or a cutoff manifest. A precise construction, validator-backed traces, and a Taktikos/LDD
security and liveness argument are mandatory. The review packet is the engineering authority.

### O-15 owner-ratified research direction — tower-augmented evidence

This direction is not implementable without the packet's construction, proof, and simulations.
Ratification settles what engineering should attempt; it does not waive the activation gates.

**Why the pairwise relation cycles (worked example).** The Tk/Bg choice is
per-**pair**, keyed on *that pair's* fork depth from *that pair's* MRCA — not a
tine's absolute length. For a frontier `{A,B,C}` where A and B diverge at a recent
fork `m3` but both diverge from C at a deep fork `R`: `MRCA(A,B)=m3` (short → Tk)
while `MRCA(A,C)=MRCA(B,C)=R` (deep → Bg). The *same* tine A is therefore in a Tk
comparison (vs B) and a Bg comparison (vs C) at once; the comparison operator itself
changes per pair, so a strict `A>B`, `B>C`, `C>A` cycle is possible
(`ChainSelectionSuite.scala:191-226`). Note `k1` is the Tk/Bg boundary; `k2` is the
retention/recovery horizon, **not** a selection cutoff (L-05) — a fork deeper than
retained history goes to `RecoveryRequired`/O-02, not to a different rule.

**The anchoring requirement.** The cycle exists because A-vs-B is measured from `m3`
while A-vs-C is measured from `R` — *different anchors per pair*. Scoring every tine
from ONE common anchor (the frontier's deepest common ancestor `R`) gives a per-tine
scalar → total order → cycle gone. Worked, with block-count density from `R`:
`score(A)=5`, `score(B)=6`, `score(C)=3` → `B > A > C`, order-independent.

**Tower weight vs. extension count (grinding).** The μ-tower does supply a per-tine
scalar, but as a *decision* statistic it is a noisier estimator of the same quantity
density already measures (production rate ∝ stake-in-window):
- **Weight** `Σ 2^μ·n_μ` is tail-dominated (a μ=9 block ≈ 1024) → high variance →
  withholding/selection-grindable (publish the luckiest private chain). This is why
  the research relegates weight to the proof layer (`README.md:39-46`; NIPoPoW
  proposal §2.4 uses weight for proof comparison only, never chain selection).
- **Counting extensions** removes the `2^μ` amplification → lower variance →
  directionally less grindable. But **count-all-levels ≈ density** (`Σ n_μ ≈ n`;
  safe but redundant), and **count-only-high-μ ≈ as grindable as weight**. So the
  safe endpoint of "count instead of weight" is density itself; the tower adds no
  safer decision signal.

**Anchor choice (safest bet).** Anchoring on "the last common μ\*-superblock" is a
sound *instinct* (the anchor must be common), and it sits at-or-*deeper* than the
plain MRCA — the shared prefix cancels in the relative comparison, so it yields the
*same relative order* as anchoring at the plain MRCA. Its value is therefore
**mechanism** (jump to the anchor via the tower) and **succinct portable evidence**
(O-15E: verify "last common μ\*-superblock + density since it" with a log-size
proof), not a safer decision. Anchor-grinding is bounded: the shared prefix is
settled mutually-agreed history that cannot be ground; only late-reveal can move the
frontier MRCA (handled by O-15A reselection), and deeper anchors are less
manipulable.

**Owner-ratified direction (activation remains subject to O-15C proof + simulations):**
- Decision statistic = **density from a single common anchor** — not weight, not
  high-μ count.
- Anchor = the **deepest** common reference (frontier MRCA / most-conservative common
  superblock); recompute on late-reveal.
- Tower's role = **mechanism + O-15E evidence**, at most a low-variance secondary
  sanity check; **never** primary or override. The lower-VRF-then-hash tie remains only a
  RED/reference input pending O-15D grinding analysis; no exact tie is ratified.

**Warnings:** (1) a per-tine scalar is required to break the cycle, but a tiebreaker
*layer* under the current pairwise density does **not** help — the retained cycle is
strict density wins with no ties. (2) Do not let any tower statistic be primary or
override density. (3) Do not count only high-μ levels. (4) Do not use a shallow
anchor. (5) None of this is safe until re-validated: the density bound must be
re-proven for the anchored metric, and the withholding/grinding surface quantified
under it — `sims/weight_grinding_intuition.py`, `weight_grinding_intuition_v2.py`,
`nipopow_levels.py`, `adv_depth_*.py`, `run_grinding_ci.py`. The existing
`grinding_results_ci.json` measured the *rejected* weighted scheme, not this.

**Re-exec:** OK — GL0 fork choice only; introduces no BFT.

## O-16 — Phase-2 consumer lease 🔴

**Authority:** [`P6-FIN14-PHASE2-CONSUMER-LEASE.md`](P6-FIN14-PHASE2-CONSUMER-LEASE.md).
The owner ratifies: **O-16A** conservative all-lease invalidation; **O-16C** retaining raw signed
bytes and rerunning every registry/VRF/KES/signature/anchor check; **O-16B / O-16E** conformance
to locked L-19/L-23; and **O-16F** a signed full `GlobalSnapshotStateRef` plus registry/parameter
era and purpose domain. For **O-16D**, receiver-local wall clock/HOCON is forbidden; engineering
must define any operation-specific or branch-authenticated historical freshness rule through
ECON-G. The purpose inventory, concrete schema/codecs, and freshness rules remain stop-the-line
engineering. Issuance is also blocked by the full dependency inventory (`P6-…:456-477`),
O-15/O-01, and absent positive watchtower coverage. **Re-exec:** OK — the lease binds *which exact
base*; inclusion still requires replay + adopter root reproduction.

## O-17 — ROOT-008 partition grammar 🔴

**Authority:** [`ROOT-008-GL0-PARTITION-GRAMMAR.md`](ROOT-008-GL0-PARTITION-GRAMMAR.md).

**R008-06 — RESOLVED by owner decision.** A metagraph may issue token locks **only for its own
currency.** Confirmed against the schema (`TokenLock.currencyId: Option[CurrencyId]`,
`tokenLock.scala:88`; `CurrencyId` = the metagraph's address):
- **Field 8 `ActiveTokenLocks` (global partition):** `currencyId == None` (native/global token).
- **Field 30 `MgActiveTokenLocks` (metagraph partition):** `currencyId == Some(CurrencyId(that
  metagraph's address))` — must equal the owning metagraph; no cross-metagraph, no native in a
  metagraph partition.

This removes the parser's `None`-vs-surrounding-MG ambiguity (`ROOT-008-…:174,196,532-535`) without
transferring framework-economic authority to the metagraph. ROOT-008 proves the physical
currency-scope-to-partition shape; GL0's protocol-defined framework transition function and the
O-07/ECON-G gate still enforce authorization, conservation, backing, replay protection, and the
other economic semantics through committee replay and root-checked adoption. Only isolated opaque
DL1 custom semantics remain application-defined and subject to a malicious application's trust
boundary; custom output cannot synthesize or authorize a framework effect.
`[RESOLVED — freeze the currencyId × partition table with ECON-G]`

**Other owner-ratified anchors (audit #7):** R008-01 keeps numeric gaps and uses an offline typed
v4 import rather than live abandoned-schema decoders; R008-02 stores field-20 `(EtaPeriod,
HistoricalStakeSnapshot)`; R008-03 stores field-23 `(PeerId, KesRegistrationReference)`;
R008-07 deletes field 32 only after L-15A witness parity. R008-05's exact ECON-G identity
functions remain an engineering freeze gate.

**R008-04 (resource/growth) — owner asked for comparable templates.** Two *different* contracts:
- **Per-candidate work budget:** emulate **gas / weight / compute-unit** limits (Ethereum gas,
  Polkadot PoV+weight, Solana CUs) — bounded work per snapshot.
- **Permanent-state growth** (nullifiers field 33, slashes field 34): **do not cap** — a finite
  cap that halts an otherwise-valid chain is unacceptable (`ROOT-008-…:464`). Emulate a
  **cryptographic accumulator** — this is exactly how **Zcash** handles its ever-growing
  nullifier set (Merkle accumulator, O(log n) proofs, never a cap); that is what the packet's
  "authenticated compaction or accumulator + exact-once proof" means (`:519-521`). Ethereum is
  the *cautionary* example (unbounded state growth is its unsolved problem). The **numbers** remain
  network engineering (hardware × cadence); the owner-ratified **structure** is a gas-style work
  limit plus a non-halting accumulator/compaction contract with exact-once proof.
  `[OWNER-RATIFIED DIRECTION / OPEN PARAMETERS]`

**Re-exec:** OK — ROOT-008 is structural grammar only; the economic oracle (O-07/ECON-G) stays
separate (this is the "MPT-as-byte-source = representation, not re-exec removal" distinction).
ECO-F32 = HIGH.

## O-18 — Transport and DA byte contract 🔴

**Status:** **OWNER RESPONSE REQUIRED.** This answers document does not infer a disposition.
The focused packet is
[`O18-TRANSPORT-DA-BYTE-CONTRACT-OWNER-REVIEW.md`](O18-TRANSPORT-DA-BYTE-CONTRACT-OWNER-REVIEW.md).

The pending choices are `O18-01` active-era per-family canonical/transport maxima; `O18-02`
selected-source-chain migration scope; `O18-03` canonical byte identity and compression domain;
`O18-04` descriptor/chunk/fetch/retention proof; `O18-05` always-pull versus threshold delivery;
`O18-06` the `512000` state-channel size/fee rule; `O18-07` the `20 MiB` event-cutter rule; and
`O18-08` mandatory absolute decompression caps versus optional ratio limits.

The new bounded Brotli decoder and reserved ingress queue are unwired preparation, not a live
closure. O-18 cannot alter economic authority: every GL0 validator still executes direct native
`GL1 -> GL0`; ordinary sharded-CL1 adoption still requires replay-backed certificates, scoped diff
application, and root reproduction; transport and DA receipts satisfy neither threshold.

---

## Cross-cutting caveats

1. **Dependency direction (corrected, audit #5):** `O-17 structural validity + O-07/ECON-G
   semantic validity → complete GL0 validation → valid tines → O-15 fork choice → exact-hash
   Phase-2 (T_weight OR k1) → O-16 consumer lease` (`O15-…:421-443`). O-17 parser work is **not**
   blocked on O-15.
2. **Re-exec model (corrected, audit #2/#3/#4):** the committee re-executes and
   signs-on-match today; certified-diff adoption with adopter root reproduction is
   the **target**; only roots-only adoption is forbidden (§1). Live gaps include
   canonical diff/complete-root binding, exact pre-root/version and Phase-2-base
   CAS, network/genesis/era/parameter domains, complete adopter verification, and
   pre-inclusion positive watchtower coverage (O-03). Blind-signing is not the
   current signer defect.
3. **Severities (corrected, audit #13):** CRITICAL = ECO-04, ECO-05, ECO-18, SHARD-C-009. HIGH =
   FIN-14, SER-02, ECO-F32, SMT-01.
4. **Parameters are protocol law, not local knobs (audit #9):** derive, test, and freeze O-01,
   O-03, and O-11 values per network (like `k1`), then root them so all nodes provably agree.
   `${?ENV}`-overridable HOCON cannot define artifact validity.
5. **Provenance (audit #14):** source citations were audited at historical commit
   `ad13026d1`, not current HEAD; any moved line or changed implementation must be
   revalidated before use as activation evidence. *(memory)*-tagged context is
   out-of-repo and not reproducible from committed source.
6. **Superseded docs** (do not cite as live): `SLASHING-DESIGN.md`, `COMMITTEE-SORTITION-DESIGN.md`
   (stake-weighted), and the pre-fix APIs and authority claims in `10-reexec-byte-contract.md`.
   The byte-identity obligation documented there remains part of the certified-diff target, but the
   ratified target contract is producer/every-signer replay, replay-certified
   scoped-diff adoption with adopter root recomputation, and independent positive
   watchtower replay coverage before inclusion.
7. **Status reconciliation (O-04):** the owner direction is ratified; remaining rebase schema and
   retained-history work are engineering freeze gates in both the lifecycle and register.
8. **Open engineering/research freeze gates:** O-01 finality implementation/parameters/proof;
   O-03 watchtower parameters; O-05 custody schema/parameters; O-07 economic inventory/predicates/
   corpus; O-08 deterministic tower activation; O-11 roster schema/bounds; O-12 common-prefix
   quantification/rejoin; O-13 full ordering/bounds; O-14 codec/E9 interaction; **O-15
   A/B/C/D/E/F/G**; O-16 freshness/purpose/schema; and O-17 R008-04/05 parameters/identities and
   activation proofs. These are not unanswered owner choices and may not be filled by local
   configuration or an implementation shortcut. O-18 is different: its eight choices are newly
   surfaced and still await an owner response.

## Provenance

Compiled at HEAD `ad13026d1` from: the register; the four audited packets (`O15-…`, `P6-FIN14-…`,
`ROOT-008-…`, `V4-ECONOMIC-GRAMMAR-AUDIT`); `AGENTS.md`; `docs/nakamoto/` design docs; ADR-0016/0017;
`CONSENSUS-ARTIFACT-LIFECYCLE.md`, `CORRECTNESS-SECURITY-AUDIT-2026-07-11.md`; source at that HEAD;
and the sim suite in `~/repos/research-nipopos-2026/sims` (calibration evidence, marked
provisional). Comparable-network references (Polkadot approval-checking/parachains, Cardano
pledge, Cosmos self-delegation, Zcash nullifier accumulator, Ethereum gas/state-growth) are
architectural analogs, not citations to this repo.
