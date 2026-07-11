# 01 — Execution Roadmap (epics → tasks → DAG)

> **HISTORICAL PRE-FIX ROADMAP.** This plan was written against `21933559c`
> before universal GL0 replay replaced committee-root/diff adoption and before
> the 2026-07-11 audit expanded the production blockers. Its priority order,
> cutover gates, roots-only work, bounded-history replay proposal, finality patch,
> and `smtRootBlind` test are not active instructions. The current finding-owned
> sequence is
> [`../CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md`](../CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md).

**Branch** `feature/committee-state-diff` · **HEAD** `21933559c` · **Date** 2026-07-07
**Inputs:** `00-audit-findings.md` (2 Critical · 10 High · 8 Medium · 2 Low · 2 test-gaps), the
invariant→test coverage matrix, and the four confirmed stakeholder directives (fraud-proofs
fully-integrated-into-slashing; I-AUTH a precondition; test all invariants; break-GAP-1 — done).

**Owner model.** Fable (this pass) writes the findings, epics, tasks, and acceptance criteria.
Implementation is delegated per-task to the cheapest sufficient tier (Sonnet for mechanical/wiring,
Opus/Fable for the base-pinning + slashing-loop + sim work). Every task names a **verification
method**; no safety/liveness task is "done" until its verification artifact is green.

---

## 0. Definition of Done (the two hard gates)

Both are **merge/cutover gates**, not aspirations:

- **DoD-1 — Non-regression floor.** Must not regress Constellation-Labs mainnet **v3.5.12**
  cross-metagraph functionality. Concretely: the existing mainnet e2e cross-metagraph suite passes
  unchanged, AND the P02 window regression is closed (a `numShards=1` metagraph mirror must not freeze).
- **DoD-2 — Sharding byte-identity.** `numShards=1` and `numShards=K` must produce **byte-identical**
  `stateProof.mptRoot` for an identical event stream — enforced by a test that **does not exist today**
  (TG-02). This is the headline regression guard for the entire sharding hard-fork.

A change touching a consensus path is not mergeable unless it (a) keeps DoD-1/DoD-2 green and (b) adds
or updates the invariant test named in its task.

---

## 1. Epics (priority-ordered)

Tier legend: **T0** = sharding-cutover blocker (safety/fund-loss) · **T1** = cutover precondition ·
**T2** = hardening / other workstreams.

### EPIC-1 — Consensus-root durability: eliminate the GSI-rebuild wipe  ·  T0  ·  closes S01, S02; de-risks D03/D04; unblocks EPIC-3, EPIC-7
**Goal.** Consensus-root-load-bearing state (fieldId 33 `ConsumedAllowSpends`, 34 `Slashings`) must
survive **every** base-rebuild path, and no rebuild may install a base whose root ≠ the signed root.
This is the single highest-leverage epic — it closes the only Critical that needs no attacker (S01),
plus S02, and removes the landmine under the band flag (D03/D04).

| Task | Acceptance criteria (anchored on DoD) | Verify |
|---|---|---|
> **Corrected by the serde audit (E9-03/E9-04):** the site inventory is **17 invocations, not 6** (see
> EPIC-9-SERDE §9.0 for the full list); EPIC-1.1 is the **first slice of GSI elimination**, not an
> independent fix (do not plan the two separately — same sites get touched twice); and EPIC-1.2 is
> **STRUCK** (adding GSI fields re-couples the root to GSI completeness — the exact seam we're removing).

> **STATUS — worktree proof-of-fix ready for review (uncommitted, `worktree-agent-a9b2f7ef195c8974f`).**
> Two-layer fix: (1) `syncFromGlobalSnapshotInfo` now captures the fieldId-33/34 (`mptNativeConsensusFields`)
> bytes before `store.clear` and re-inserts them verbatim (runtime paths survive a reorg with the spent-set
> intact); (2) a new root-verified `syncFromGlobalSnapshotInfoVerified(info, ordinal, signedMptRoot): F[Boolean]`
> reconciles `{GSI ∪ preserved 33/34}` and `{GSI alone}` against the **signed root before any write**, and
> returns `false` (store untouched) when neither reproduces it — the fail-closed bar (also handles the
> cross-chain adopt case: stale local markers dropped when the target's 33/34 are empty). **All gl0 sites
> migrated** (reorg self-heal, catch-up GSI fallback = **E9-02 closed**, reward-realign, gossip catch-up →
> gate; cold-restart/peer-download/download-replay → fail-closed `raiseError`); fresh-genesis left (33/34
> structurally empty). **RED→GREEN:** new `GsiRebuildSpentSetSurvivalSuite` — RED wiped the marker + accepted
> the double-spend; GREEN (6 tests) the marker survives, `rebuiltRoot === signedRoot`, the re-consume is
> rejected; numShards=1 byte-identical (preservation is a provable no-op when 33/34 empty). ~90 adjacent
> rebuild-suite tests green. **Scope caveat (per E9-03):** this covers the **gl0** sites — the Critical
> double-spend + gl0 self-fork. The **7 follower sites** + `GlobalSnapshotTraverse`/`SnapshotDownloadStorage`
> (follow-verify *wedge*, not double-spend) remain = the rest of 9.1. **Preferred completion (E9-04):**
> boot/download should byte-faithful-**load** the persisted MPT bytes (which already contain 33/34), not just
> fail-closed — needs a small `producer.load(ordinal)` trait surface addition. e2e at numShards>1 not yet run.

| Task | Acceptance criteria (anchored on DoD) | Verify |
|---|---|---|
| **1.1** Route **all 17** rebuild sites (E9-03 inventory: reorg `NSD:2144`, realign `:3366`, catch-up `:3499`, deep-catch-up GSI fallback `:247`, restart `Main.scala:430/486/593`, download `Download.scala:523`, `GlobalSnapshotTraverse.scala:125`, `SnapshotDownloadStorage.scala:110`, + 7 follower sites) through byte-faithful `loadBytes` + `sidecarFreeMptRoot === signedRoot` (the `NSD:224-240` shape), **verify-before-write** (today's `:247` fallback is destructive-then-detect). **Delete** the follower GSI fallbacks the 3c-A byte route already covers. Each site gets a named (byte-source, gate) per the §9.0 design note — incl. the reorg-branch byte-availability problem (E9-04). | After any reorg/restart/catch-up, fieldId-33/34 byte-identical to pre-rebuild; rebuilt `sidecarFreeMptRoot === signed`; numShards=1 byte-identical (DoD-2). | Unit + e2e reorg |
| **1.2** ~~Add GSI fields~~ — **STRUCK (E9-03):** anti-aligned with GSI elimination; emergency-only. | — | — |
| **1.2′ (was E9-02)** Replace catch-up/realign **Gate-2** (`NSD:185-191`, `forGlobal(None)` GSI-only recompute) with a served-signed-bytes comparison; make the GSI fallback fail-closed-before-write. | (RED today) catch-up + reward-realign of an ordinal with non-empty fieldId-33 **succeeds** at numShards>1. | Weaver + catch-up e2e |
| **1.3** Make every rebuild site **fail-closed** on a post-rebuild root mismatch (stall → Rebootstrap), never silently proceed. | Injected root mismatch → node stalls + metric; no divergent-root commit. | Unit |
| **1.4** Regression tests: (a) cross-shard consume → reorg in `(k₁,head]` → re-consume **rejected** (RED today, the S01 double-spend repro); (b) restart preserves the nullifier; (c) an upheld slash survives a reorg; (d) catch-up with non-empty fieldId-33 succeeds (E9-02). | All green. | Weaver + e2e |

### EPIC-2 — I-AUTH: env-dependent operator-threshold source authenticity  ·  T1 (cutover precondition)  ·  closes 001, G01
**Goal.** The metagraph-source signature that GAP-1 binds every adopted field to must be a real trust
anchor (operator majority), not any-single-allow-listed-signer.

| Task | Acceptance criteria | Verify |
|---|---|---|
| **2.1** Replace the `≥1` predicate (`StateChannelValidator.scala:186-199`) with an env-dependent **operator-majority threshold** over the intersection of the binary's signers with the metagraph's registered operator key set; HOCON `nakamoto.metagraph-source-authenticity`, threaded via `SharedValidators.make` (no `sys.env`). | Binary with < threshold valid operator sigs → `NoSignerFromStateChannelAllowanceList`-class reject; ≥ threshold → accept. numShards=1 parity: single-signer nets unchanged (DoD-1). | Unit |
| **2.2** Populate the Integrationnet allow-list (`StateChannelAllowanceLists.scala:21-25`) so the registered-net gate is active there. | Integrationnet rejects an unlisted signer. | Unit |
| **2.3** Route a sub-threshold acceptance to a **slashable** rejection reason (feeds EPIC-3). | Sub-threshold binary produces a slash-eligible evidence record. | Unit |
| **2.4** Close the GAP-1 §4.2 Option-field hole for **injected non-empty** entries (the G01 squatting vector): an adopted field carrying entries whose stateProof is `None` must DROP, not adopt. | Checkpoint upserting `lastMessages`/`activeAllowSpends` while tip stateProof for that field is `None` → adoption drops (RED today). | Weaver |

### EPIC-3 — Close the fraud-proof → slash → exclusion loop; land the two shelf-ware tiers  ·  T0 (directive)  ·  closes 002; hardens S02; depends on EPIC-1
**Goal (verbatim directive).** Fraud proofs must be *fully integrated into slashing*, not signaling —
the loop verdict → stake-reduction → **feeds back into sortition/quorum** must close, and the
equivocation + non-participation tiers must get a real consequence sink.

| Task | Acceptance criteria | Verify |
|---|---|---|
| **3.1** Add a cooldown reader over the `Slashings` (fieldId-34) prefix and gate `committeeFor`'s `activeValidators` (`ShardCheckpointWiring.scala:567-578`) + the quorum denominator to exclude peers with `cooldownUntilEpoch > currentEpoch`. | A slashed peer with unexpired cooldown is **absent** from the committee draw and does not count toward `kQuorum`; quorum math shrinks accordingly. | Weaver |
| **3.2** Wire the equivocation tier end-to-end: evidence gossip topic + daemon handler (mirror `handleFraudProof` → pool → consensus field → accept-fold → `applySlash` with `SlashReason.{Checkpoint,Metagraph}Equivocation`). | Upheld equivocation evidence → `Slashings` write (mirror the W3a suite shape). | Weaver + e2e |
| **3.3** Wire non-participation: write the fieldId-24 counter on shard accept/attest paths; call `evaluateEpochBoundary` at the gl0 epoch boundary; route the produced `List[PeerId]` into `applyWatchtowerSlashes`. | Epoch-boundary non-participation list produces a stake/committee effect. | Weaver |
| **3.4** (depends on EPIC-1) Verify the slash is **durable across a reorg** (S02) — the exclusion must not be undone by a base rebuild. | Slashed-then-reorged peer stays excluded. | e2e |

### EPIC-4 — Pin every per-MG re-derivation rail to `executionBaseOrdinal`  ·  T0  ·  closes 003, B1, B2/R01, R02 (defense-in-depth); part of G01
**Goal.** One defect on four rails: live-base re-derivation of a base-**dependent** per-MG root. The
gl0 produce/validate verdict is the only pinned rail; the sub-quorum acceptance path, the follower
`createContext` fraud-proof verdict, and the `lastMessages` carry-forward all read live.

> **STATUS — worktree proof-of-fix ready for review (uncommitted, `worktree-agent-a4abb19c3b386ecae`).**
> Shared `pinnedPriorReaderAt` helper (fast-path when `executionBaseOrdinal == lastPersistedOrdinal`, else
> `pinnedReaderAt`, else **fail-closed**); both live rails repointed (4.1); `Hash.empty` → plain reject /
> new `CannotRederive` variant, no false slash (4.2); `lastMessages` collapsed to proof-shape (4.3);
> gl0 `finalizedReaderAt` deduped onto the same helper. RED→GREEN captured (validator false-slash on the
> `Hash.empty` sentinel; follower false-uphold; **balances** + `lastMessages` drift) via a new
> `ExecutionBasePinReExecutionSuite` (5) + extended suites; full compile + linter clean; numShards=1 byte-identical
> (all touched paths sharding-gated). **Confirms E9-05** (uses the `pinnedReaderAt` the in-source comment
> called nonexistent). **Residual (degraded-path only, self-healing, → Track-3 S2):** a follower whose
> retention has pruned a *deep* `executionBaseOrdinal` fails closed (`CannotRederive`) and falls to the
> byte-faithful adopt rail instead of reproducing the leader's slash — a verdict-uniformity/liveness
> residual, not a safety hole. Structural fix = the contiguous k₂-deep follower byte store (EPIC-4.6 below).

| Task | Acceptance criteria | Verify |
|---|---|---|
| **4.1** Swap `liveReaderAt → finalizedReaderAt`/pinned reader in the sub-quorum `reExecPath` closure (`SharedServices.scala:321-351`) and the follower `createContextInvalidStateProofValidator` (`:363-367`); delete the wrong "base-independent" comments (`:361-362`, `:327-331`). | Two nodes at different live tips re-derive **identical** per-MG roots for the same checkpoint (pinned base). Honest committee is never slashed. | Weaver forcing test |
| **4.2** A rail that cannot resolve `executionBaseOrdinal` must **fail-closed / skip the dispute** — never substitute a live base and never treat `Hash.empty` as UPHELD (`InvalidStateProofValidator.scala:139-155` vs the node-local `Hash.empty` filter `ShardCheckpointGl0AcceptanceManager.scala:422`). | Non-derivable MG → dispute skipped, no slash. | Weaver |
| **4.3** Collapse `lastMessages` to committed-proof shape like its siblings (`GlobalSnapshotStateChannelEventsProcessor.scala:731`), and add the determinism invariant "any in-root Option field must collapse to the committed-proof shape when the committed proof is `None`." | Same message-free window over prior-A (owner in `lastMessages`) vs prior-B (empty) → equal roots (RED today). | Weaver |
| **4.4** Add a depth-k₁ window gate on the `accept()` fraud-proof loop (`GlobalSnapshotAcceptanceManager.scala:2216-2249`) so `executionBaseOrdinal` is bounded into the cluster-uniform finalized region (R03 assertion `executionBaseOrdinal + k₁ ≤ N`). | Dispute with `executionBaseOrdinal` at the finalization frontier is deferred/rejected, not split. | Weaver |
| **4.5 (the third base-pinning leg — #23, surfaced by serde E9-05)** Repoint the `acceptMessages` head reads (`CurrencySnapshotAcceptanceManager.scala:304-317` `getCombined`/`lastUnsyncBalances`/`lastUnsyncLastCurrencySnapshots`) onto `PinnedCurrencyInfoReader.pinnedReaderAt(globalSyncView.ordinal)` (it **exists** — landed `eb9bf9a5e`; the in-source "not reconstructible" comment is stale). Needs the 9.2 full-map getters. **`lastMessages` fold inputs are the last live-base leg** — adopt≡re-exec is unprovable until 4.1 + 4.3 + 4.5 all land; then update `10-reexec-byte-contract.md §4.7` from "unverified assumption" to "enforced". Note: "MPT-backed" ≠ "pinned" — must use the *pinned* reader at the carried anchor. | (RED today) two nodes at different live heads fold the same **message-bearing** binary to identical `MgLastMessages` leaves. | Weaver forcing test (extend `CurrencySnapshotAcceptancePuritySuite` to message-bearing input) |
| **4.6 (residual from the 4.1 fix — Track-3 S2)** Contiguous k₂-deep follower byte store, so a follower can resolve a *deep* `executionBaseOrdinal` pinned base instead of failing closed (`CannotRederive`) and dropping to the byte-faithful adopt rail. Removes the verdict-uniformity residual the pin fix exposes (a retention-pruned follower can't reproduce the leader's slash). Today `PinnedCurrencyInfoReader` retention is `LogarithmicOrdinalCutoff` (deferred per its scaladoc `:48-55`). | A follower whose retention pruned the anchor still reproduces the leader's slash verdict (no `StateProofMismatch` drop). | Weaver + deep-anchor e2e |

### EPIC-5 — P/U window robustness (cross-shard spend accounting)  ·  T0/T1  ·  closes P01–P06; P02 is on the DoD-1 floor
**Goal.** `P ⊇ U` must hold across restart/rollback/download and under ingest stalls; U must not
silently lose the newest spends.

| Task | Acceptance criteria | Verify |
|---|---|---|
| **5.1 (P02, DoD-1)** Restore a ≥50-deep (or in-band) window on the validator/gl0 `CurrencySnapshotCreator` (`SharedServices.scala:213-220`); fix the unsound `metagraphSyncData=None@numShards=1` premise. | A `numShards=1` metagraph with cross-metagraph inbound spends does **not** freeze its gl0 mirror. | e2e (mainnet-parity) |
| **5.2 (P01/P06)** Make the P walk-back miss **loud**: fail-closed or backfill-from-peers to depth 50 on join/rollback (`CurrencySnapshotCreator.scala:396-399`, `Rollback.scala:106-123`); let `write()` backfill a missing hash file (`SnapshotLocalFileSystemStorage.scala:47-49`). | Restart-after-downtime reconstructs the full window or fails loudly; no silent double-apply. | Weaver + restart e2e |
| **5.3 (P03)** Replace U's `dropRight` at the 100-cap with drop-**oldest**-plus-alarm or fail-closed (`MetagraphSyncManager.scala:274-283`). | At cap, the newest ordinal is retained; a drop emits a metric. | Weaver |
| **5.4 (P04)** Emit a periodic GSP "keep-alive" for still-in-U processed ordinals older than window/2 so the in-chain record can't age out. | A > 50-snapshot ingest stall does **not** re-apply a spend. | Weaver + stall e2e |
| **5.5 (P05)** Pin each fetched `o ∈ A` by hash (`GlobalSnapshotOpsManager.scala:121-143`), or prove/enforce the view quorum ≤ gl0 finality. | Two facilitators on different branches at `o` fold identical SpendActions. | Weaver |

### EPIC-6 — Finality quorum + config hygiene  ·  T1  ·  closes 004, 005
| Task | Acceptance criteria | Verify |
|---|---|---|
| **6.1 (004 — surface to stakeholder first: touches the finality quorum)** Weigh the attestation fast-path against **total** registered stake (or a `MinActiveQuorumFraction` floor), so a minority-active partition cannot cross 2/3 (`TipTracker.scala:212-225`). | A minority partition (active ≪ total) cannot attestation-finalize; depth-k₁ remains the backstop. | Weaver partition sim |
| **6.2 (005)** Migrate `FinalityThreshold` + `MaxAttestationSkewMs` from `sys.env` to typed HOCON `nakamoto.*` with `${?ENV_VAR}` (`TipTracker.scala:131-136,149-153`); add config goldens. | No `sys.env.get("NAKAMOTO_*")` in `TipTracker`; golden pins the per-env value. | Unit |

### EPIC-7 — Band-revert flag readiness (deferred behind its sim)  ·  T2  ·  closes D01–D04; depends on EPIC-1
**Goal.** `band-density-reorg-enabled` cannot flip until deep reorgs have a coherent, cluster-uniform
base-revert. **Flag stays OFF until every task below is green** (the DEC-005/OPEN-002 gate).

| Task | Acceptance criteria | Verify |
|---|---|---|
| **7.1** Wire `revertToOrdinal` into the reorg executor (it has zero callers today, D03); route deep band reorgs through it, not `syncFromGlobalSnapshotInfo`. | A band reorg reverts the overlay base via `revertToOrdinal`, not the S01 wipe path. | Weaver |
| **7.2 (D04)** Add a journal epoch-fence to every base-replacing op; define a deep-tier-hole fallback (no unhandled `RevertGapError`). | Shallow-revert node and deep-revert node reach `sameBytes` + equal root for the same fork ordinal. | Weaver (pruned-vs-unpruned overlays) |
| **7.3 (D01)** Derive `(winner, mrcaOrdinal)` from a **single** `buildTines` walk in `shouldSwitch` (`ChainSelection.scala:164-165`). | Concurrent snapshot delivery during `shouldSwitch` produces zero switches on `mrca=None`-adjacent asymmetric verdicts. | Weaver race test |
| **7.4 (D02)** Give `tipFor` the disk fallback the other readers have; make `mrca=None` refusal trigger a lineage backfill + WARN/metric (`NakamotoChainStore.scala:783-792`). | Two nodes with different lineage subsets converge (no persistent split). | Deep-fork sim |
| **7.5** Write the **deep-fork cluster-uniformity sim** with the four enumerated pass-conditions (commutativity under partial knowledge; settled-skew adversarial placement; the `shouldSwitch` race; revert-path equivalence across shallow/deep/resync). Only then flip the flag. | All four pass-conditions green cluster-wide within bounded ordinals. | Sim + e2e |

### EPIC-8 — The invariant→test suite (the "test all invariants" directive + both DoD gates)  ·  T0 gate  ·  closes TG-01, TG-02, matrix gaps
**Goal.** Every invariant in the matrix has an asserting test; the two DoD gates are enforced by tests
that do not exist today. This epic runs **in parallel** with 1–7 and is the merge gate for all of them.

| Task | Acceptance criteria | Verify |
|---|---|---|
| **8.1 (TG-02, DoD-2)** Cross-count byte-identity test: fold an identical event stream through GSAM at `numShards=1` (raw) and `numShards=K` (mock quorum-accepted checkpoints carrying the same binaries) → identical `stateProof.mptRoot`; plus a 1-vs-2-shard e2e `.value.lastSnapshotHash` diff at a common ordinal. | Both green; wired into CI as the cutover gate. | Weaver + e2e |
| **8.2 (TG-01)** `smtRootBlind` regression test: two artifacts differing only in `stateProof.smtRoot` are consensus-`===`; differing in `mptRoot` are not. | Green; pins the June fork-storm fix. | Weaver |
| **8.3** The matrix's UNTESTED-but-built invariants get their closing tests: GAP-1 adversarial injection (INV-SAFETY-003 E-2), base-independence forcing test (E-3), P⊇U restart round-trip (E-6), k₂-revert cross-bound (E-5), attestation-partition (INV-FAULT-002), and the **committee-verify-before-attest RED test** (E-11) that fails until EPIC-2/4 land. | Each named invariant has an asserting suite; the RED tests document the current gap until their epic closes them. | Weaver |
| **8.4** Wire the `sys.env`/`Double`/`try-catch` lints named in the packet as CI checks so config-drift and IEEE-754-in-consensus regress loudly. | CI fails on a new `sys.env.get("NAKAMOTO_*")` or `Double` in a consensus path. | CI rule |

EPIC-9 was decomposed by a dedicated audit-extension pass (three read-only Fable agents, all findings
source-verified). It splits into three real sub-epics.

### EPIC-9-SERDE — Finish GSI elimination  ·  9.1 T1 (cutover-critical), rest T2  ·  closes E9-01…06; folds in S01/S02/E9-02
**Goal.** Remove the `GlobalSnapshotInfo` re-encode entirely so no path can drop/mis-derive
consensus-root state (S01/S02/E9-02 close at the type level). EPIC-1.1 is slice-1 of this epic.

| # | Task | Dep | Acceptance | Verify |
|---|---|---|---|---|
| **9.0** | Design note in `12-decision-log.md`: ratify EPIC-1.1 = slice-1; strike EPIC-1.2; per-site (byte-source, gate) decision over the **17-site inventory** incl. the reorg-branch byte-availability problem (E9-04: local signed store / peer byte-pull at `(ordinal,hash)` / wire-`ChangeSet` replay `GSC:294,796` / `revertToOrdinal`+re-fold). | — | Every one of 17 sites has a named source+gate; re-grep finds no unlisted site. | Doc review |
| **9.1** | Execute EPIC-1.1 over the full inventory + replace Gate-2 (= EPIC-1.1 + 1.2′). | 9.0 | EPIC-1.4 green + catch-up-with-nonempty-33 green. | Weaver + e2e |
| **9.2** | 3c-C getter fill: mirror the absent getters onto `GlobalStateReaderOps` (`getTxRef`, `getActiveAllowSpends`, `getTokenLockRef`, `getPriceRecord`, `getUpdateNodeParameters`, `getMetagraphSyncData`, `historicalStakeSnapshots` — last, per 259-coupling). | — | Per-getter parity test vs `info.<field>` incl. empty/genesis states. | Weaver |
| **9.3** | Close #23 = **EPIC-4.5** (joint-lands with 4.1/4.3). | 9.2 | (RED today) message-bearing binary folds identically across live heads. | Weaver |
| **9.4** | 3c-C read-site migration of the ~143 `info.<field>` hits, one file/commit; **triage each: consensus→pinned reader, serving→live OK** ("MPT-backed" ≠ "pinned"). | 9.2 | Per-file suite green + CI grep rule (EPIC-8.4 shape). | Weaver + CI |
| **9.5** | 3c-B `from(mptStore, ordinal, era)` projection + field parity suite. **Blocked fields (E9-01):** `lastCurrencySnapshots` Left-arm (no fieldId-3 writer), `priceState` + `historicalStakeSnapshots` (hashed keys, no carried key) — resolve by layout-normalization (root-changing flag-day) or obligation-shrink; `Some(empty-inner-set)` is a parity dimension. | 9.1,9.4 | `stateProof` byte-identical projection-vs-hand-built at every corpus ordinal (11 Option proof slots). | Weaver + TG-02-style compare |
| **9.6** | 3c-D on-disk flip: signed byte store authoritative on restart; dual-write one release; boot re-derive fallback. | 9.1 | Restart at numShards>1 with non-empty 33/34 comes back byte-identical. | Restart e2e |
| **9.7** | Delete `syncFromGlobalSnapshotInfo` (all overloads) + the GSI half of `/latest/combined`; then the ~132-file `SnapshotInfo[_]` threading. | 9.5,9.6 | grep = 0; full e2e green; S01/S02 closure now type-level. | `just test` |
| **9.8** | Kryo reachability scrub (30 min, independent). | — | One-paragraph note. | Config read |

### EPIC-9-HARDFORK — Roots-only readiness  ·  T2 (post-cutover), preconditions on EPICs 1-5/8.1  ·  closes H01…05
**Goal.** The ordinal-gated roots-only step (gl0 stops holding per-MG state) cannot be scheduled until
its own read-side + activation + prior-state-availability are built. **The `numShards>1` cutover
(Shape-A) does NOT need this epic; this is the *future* Shape-B fork.**

Key findings folded in: **H01** — cross-shard read IS reachable at numShards>1 via `gl0Local` (packet
`21-*` §2.2 stale; verified `GSAM:2470-2497`), so S8's read-side is a *design* task (replace `gl0Local`
with in-band proof-carrying), not wiring. **H04** — roots-only deletes the per-MG state the enforcement
teeth (watchtower / `reExecPath` / `createContext`) read; in-band DA covers window *inputs*, not the
seed prior at `executionBaseOrdinal` → teeth go blind exactly when the committee becomes sole state-holder.
**H02** — no activation-ordinal mechanism exists (must build on the `FieldsAddedOrdinals`/`Era` pattern,
**no `${?ENV}` on a consensus-flip ordinal** — the FINDING-005 footgun class). **H03** — genesis-window
fallback + proof-of-**absence** (v2, unimplemented) are both absent → universal rejection of new-MG spends.

| # | Task | Notes |
|---|---|---|
| HF.1 | Doc-truth pass: fix `21-*` §2.2/§4 + `ENDGAME-PLAN:193/211/239` for the W3c/`gl0Local` reality; refresh drifted anchors. | Mechanical |
| HF.2 | **DESIGN DECISION (blocks the rest):** (i) prior-state availability model for dispute rails (H04: watchtowers hold mirrors / k₂-windowed gl0 retention / state-witness-carrying disputes verified vs pinned `R_M`); (ii) Shape-A re-genesis vs Shape-B ordinal fork (F1 of the hardfork doc). | Record in decision log |
| HF.3 | Activation-ordinal machinery (H02) + `numShards` cluster-uniformity assertion. | Skip if HF.2 picks Shape-A |
| HF.4 | S7 read leg: `ProvenCurrencyStateReader` (verify vs own committed `R_M` pinned to S(N)) + **absence proofs** (H03, hard deliverable) + parity gate. | |
| HF.5 | Genesis-window fallback (H03): un-incremented MG spend passes via fallback, never `ProofUnavailable`. | RED test |
| HF.6 | Re-source the dispute rails under roots-only (H04 build; depends on EPIC-4). | |
| HF.7 | The atomic S8 flip (readers→proof-only + fold deletion + roots-only writes), all gated on HF.3's ordinal; per-flip-site numShards=1 byte-identity. | ONE indivisible change |

**Roots-only preconditions (checklist):** EPIC-1 (durability) · EPIC-2 (I-AUTH) · EPIC-3 (slash→exclusion) ·
EPIC-4 (pin rails, incl. 4.5) · EPIC-5 (P02) · EPIC-8.1 (byte-identity) · HF.2 decision recorded ·
HF.3 activation ordinal · HF.4 S7 leg + absence proofs · HF.5 genesis fallback · DA live before fold deletion.

### EPIC-9-NET — Networking hardening + the boundedness proof  ·  M1/M4 T1 (consensus-input), rest T2  ·  closes F1…F10
**Goal.** Close the two new consensus-load-bearing bugs (F1 shard-checkpoint loss, F2 dead fraud-proof
transport) and the DoS surface, then prove the drop-vs-recover loop is stable.

| # | Task | Closes |
|---|---|---|
| **M1** | Kill the dual-`Subscribe` shard race: implement `SubscribeRequest.topics` filtering (`server.go` ignores `req` today); bridge subscribes rumor-only, daemon the rest; metric on concurrent streams. | **F1 (Critical)** |
| **M2** | Rumor-bridge reconnect loop (mirror the daemon), landed **with** M1. | F8 |
| **M3** | ChainSync serve hardening: wire `RateLimitPerPeer`, enforce `MaxHashesPerRequest` + `maxPullRange` on all serve handlers **both hops**, replace `ChainSyncServer` `(start to end).toList` with lazy iteration. | F3 (High) |
| **M4** | Fraud-proof transport slice: sidecar topic + relay + `Subscribe` arm + `PublishFraudProof` + **outbox durability**; emitter retry-or-outbox, not warn-and-drop. | **F2 (High)** — ties to EPIC-3 |
| **M5** | Bound `GossipStream` queue + gRPC `request(n)` (F4/F5 interlock — one coherent change; per-topic streams natural after M1). | F4, F5 |
| **M6** | Shard topic `SetScoreParams` (dynamic, API-verified) + scale the shard relay buffer per-shard + broaden mesh-health check. | F6, F7 |
| **M7** | Backstops: outbox size cap, orphan-buffer TTL/cap, surface `sidecar_gossip_messages_dropped_total{shard_*}` in Grafana. | G6/G7 |
| **F-A** | **Frontier:** drop-vs-recover stability argument for shard-checkpoint delivery at numShards×numMetagraphs — recovery rate (pull: ~1 ord/5-9s/shard) vs drop×production; the divergence precondition set at HEAD (must include the attestation-equivocation hole — slashing detection-only); the **pull horizon** (`ShardChainStore` eviction vs a node stalled past retention — needs a shard deep-backfill?). | F10 |

Also: the remaining `sys.env`→HOCON sweep (`22-*`), reconcile BLS-on-unmerged-branch, and wire or
explicitly defer the inert `emittedReceipts` drain (functional gap, root-purity sweep).

---

## 2. Dependency DAG & critical path

```
                         ┌──────────────────────────── EPIC-8 (invariant+DoD test suite) ───────────────────────────┐
                         │  runs in parallel; is the MERGE GATE for every epic below                                │
                         └──────────────────────────────────────────────────────────────────────────────────────────┘

 EPIC-1 (S01/S02 root durability) ──┬──> EPIC-3 (slash→exclusion loop; needs durable slash)
   [CRITICAL PATH HEAD]             │
                                    └──> EPIC-7 (band flag; revert routes through the rebuild) ──> flip flag (deferred)

 EPIC-2 (I-AUTH threshold) ─────────┐
                                    ├──> SHARDING CUTOVER (numShards>1 enabled)
 EPIC-4 (pin every re-derive rail) ─┤        gated by DoD-1 + DoD-2 (EPIC-8.1)
                                    │
 EPIC-5 (P/U window; P02 on DoD-1) ─┘

 EPIC-6 (finality quorum + config) ── parallel, T1
 EPIC-9 (serde/hardfork/networking) ─ parallel, T2
```

**Critical path to the sharding cutover:**
`EPIC-1 → (EPIC-2 ∥ EPIC-4 ∥ EPIC-5) → EPIC-8.1 (byte-identity gate) → cutover.`

- **EPIC-1 is the head** — it is the only Critical reachable with no attacker (S01), and it de-risks
  EPIC-3 (slash durability) and EPIC-7 (revert routing). Start here.
- **EPIC-2, EPIC-4, EPIC-5 are the cutover preconditions** — the trust anchor (001), the base-pinning
  (003/B1/R01), and the non-regression floor (P02). They can proceed in parallel once EPIC-1's rebuild
  contract is fixed.
- **EPIC-8.1 is the gate** — no shard-count flip merges without the 1-vs-K byte-identity test.
- **EPIC-3 is the directive** and depends on EPIC-1 (a slash that a reorg erases isn't integrated).
- **EPIC-7 is deferred** — the band flag stays OFF; its sim (7.5) is the written pass-condition the
  decision log (DEC-005/OPEN-002) is missing.

**Sequencing note (avoid the historical "same-bug-refixed" trap):** EPIC-1 and EPIC-4 both stem from
the *one* seam "consensus-root-load-bearing state derived/rebuilt over a non-cluster-uniform base."
Enumerate the full membership of each class (all rebuild sites for EPIC-1; all `liveReaderAt` rails for
EPIC-4) **before** the first fix, so neither becomes an 8-commit field-by-field chain (PROC-18).

---

## 3. Invariant → test coverage (DoD-enforced summary)

The full matrix (35+ invariants across `01-invariants.md`, the economic-trust doc, the committee-diff
design, and the byte-contract residuals) is the closing-test backlog for EPIC-8. Top gaps, ranked
(impact × reachability × absence-of-compensating-control):

1. **Quorum-path state injection with no re-exec anywhere** (E-11 + E-2) — emitter signs without
   recompute, gl0 accepts on quorum-sig, GAP-1 skips `None` fields → arbitrary field injection.
   Closed by EPIC-2.4 + the EPIC-8.3 RED test.
2. **Per-MG root base-dependence via `lastMessages`** (E-3) — closed by EPIC-4.3.
3. **`smtRootBlind` unpinned by any test** (TG-01) — closed by EPIC-8.2.
4. **Slash-consequence loop open** (E-8/E-9) — closed by EPIC-3.
5. **Spendability staging / real watchtower detection unexercised** (INV-FAULT-001 prevention half) —
   `watchtowerReExec` is stub-only in every test; closed by an un-stubbed detection test (EPIC-8.3).
6. **No `numShards=1 == K` byte-identity bar** (TG-02) — closed by EPIC-8.1 (DoD-2).
7. **P ⊇ U on restart/deep-catch-up** (E-6) — closed by EPIC-5.2 + EPIC-8.3.
8. **Band-revert determinism across RAM-journal bounds + the cluster sim** (E-5) — closed by EPIC-7.2/7.5.

Coverage-status legend from the matrix (for the backlog): **TESTED** (assert the claim) / **PARTIAL**
(happy-path or weaker observable) / **UNTESTED** (built, no assertion) / **UNBUILT** (no enforcing
code). Every UNBUILT invariant (I-AUTH, spendability staging, committee-size safety bound, slash
exclusion) is owned by the epic that builds it; every UNTESTED invariant is an EPIC-8.3 task.

---

## 4. What needs a human / another deep pass before implementation
- **EPIC-6.1 (attestation denominator)** touches the finality quorum — confirm it's an
  implementation-correction, not a design change, before acting (flagged in FINDING-004).
- **EPIC-1.1 vs 1.2** (byte-faithful rebuild vs adding GSI fields) is an architecture choice; 1.1 aligns
  with `feedback_eliminate_globalsnapshotinfo` and is preferred, but the blast radius across the six
  rebuild sites warrants a design note first.
- **EPIC-7.5 pass-conditions** are the decision-log gap (DEC-005/OPEN-002) — they should be ratified in
  `12-decision-log.md` as the falsifiable flip criterion before the flag work starts.
