# Decision Log — Tessellation/Nakamoto Sharding Production-Readiness

> **HISTORICAL PRE-FIX DECISION LOG.** Authority/diff and finality decisions in
> this file are not current. Deleted document names are provenance recoverable
> from commit `725b25b`, not live design dependencies. Use `AGENTS.md`, ADR-0016,
> ADR-0017, and the 2026-07-11 correctness audit for current status.

> **Audience.** Fable (deep-reasoning review pass). **Purpose.** Separate what is
> **SETTLED** (with a grounded reason — do not re-litigate or "fix" it back to a
> known-broken state) from what is **genuinely OPEN** (Fable has license to redirect).
> Read `docs/review/HANDOFF.md` first for framing; this file does not duplicate it.
>
> **Grounding discipline.** Every "Why" cites a doc §, a commit, a `file:line`, or a named
> research result. Where a decision is asserted as settled but I could not find a recorded
> rationale, it is demoted to **Section C** (a risk, not a settled fact) — I did not invent
> reasoning to fill the gap. Verified against the working tree on 2026-07-07, branch
> `feature/committee-state-diff`, HEAD `21933559c`.

---

## Section A — SETTLED (do NOT re-litigate)

### DEC-001: k₂ = 100·k₁ (single derived accessor, not a free knob)
- **What:** The absolute retention/archival floor `k₂` (`keepDepthBehindFinalized`) is
  **derived** as `100 · k₁`, not independently configured. At mainnet `k₁ = 1024` ⇒
  `k₂ = 102 400 ≈ 8 days` at ~7 s snapshots.
- **Why:** k₁ is the single free confirmation-depth knob; every other depth (R = round(3.1·k₁),
  k₂, kLookback = k₁+1, sWindow ≈ R/3) is derived so the whole finality band stays in lockstep
  with one operator-tunable value (avoids cluster-split from divergent per-env defaults). k₂ is
  deliberately deep — "a long, slow, stable consensus history (≈8 days at k₁=1024 / 7 s
  snapshots)" — because it is the *only absolute floor*: below it, chain state is archived and
  irreversible, so it must sit far past any realistic liveness-degraded reorg. The "100" makes
  the archival horizon ≈ 8 days, comfortably beyond the challenge window (K = k₁) and any
  partition-recovery window.
- **Evidence:** `config/types.scala:161-166` (`keepDepthBehindFinalized(env) = 100L · confirmationDepthK`);
  `application.conf:308-314` (`confirmation-depth-k` mainnet 1024); TRACK1-TRACK3 spec §2
  ("k₂=100·k₁ (mainnet 102400 ≈8 days)"); §6 memory `project_nakamoto_k1_r_k2_params_eta_amortization`.
- **What breaks if reversed:** The 2026-06 forensic audit found the audit's own "k₂ = 10·k₁ / 2550"
  figure was **stale** (FORENSIC-AUDIT §"k2=3200"); an independently-configured or shallower k₂ is
  exactly what let the reverted `86f390130` set the archive marker to an *unreachable* depth
  (dev k₂=3200 never reached in any run) so **no persistence floor was armed anywhere**
  (FORENSIC-AUDIT:22). Decoupling k₂ from k₁ re-opens the "which horizon is actually armed?" class
  of bug.

### DEC-002: Roots-only sharding over gl0-holds-full-mirror
- **What:** gl0 commits **per-metagraph roots** (`{M → R_M}` into a gl0-level commitment), not the
  full per-MG balance map. Metagraph balances are served by the metagraph's own ml0 + an inclusion
  proof verified against gl0's committed root. gl0 does **not** re-derive metagraph balances by
  folding deltas.
- **Why:** gl0 **cannot** reconstruct full per-MG currency state by folding shard-tip-relative
  deltas against its own full-state prior — the two tips diverge at the first checkpoint and never
  reconcile (the `lastCurrencySnapshots`-stuck-at-genesis freeze). The freeze is *intrinsic* to
  "gl0 reconstructs full per-MG state," because sharded MGs reach gl0 only via committee-adopt and
  split-safety forbids node-local reconstruction of the missing genesis-bridge binary. Data-app +
  cross-shard custom state is not re-derivable by gl0 at all (it can't run arbitrary metagraph
  code). Roots-only deletes the reconstruction, so the blocker becomes structurally impossible.
- **Evidence:** ROOTS-ONLY-SHARDING-ARCHITECTURE.md §0 ("The freeze is intrinsic…") and §3.1;
  memory `project_roots_only_sharding_ii_decision`, `project_data_with_fee_shard_imbalance_grind`
  (gl0 CAN'T re-derive metagraph balances — data-app + cross-shard).
- **What breaks if reversed:** Re-introducing the full-mirror fold reinstates the gl0 currency-fold
  freeze (8gl0/4mg/4shard e2e blocker) and the shard-tip-vs-gl0-tip divergence that no
  node-shared-only fix can resolve.

### DEC-003: Role-split adoption — committee/watchtowers re-execute + attest; gl0 + everyone else adopt the byte-diff
- **What:** ml0 runs `accept()` (authoritative DATA at DL1). A VRF-sortitioned shard committee
  (`K_S`) + VRF-sampled watchtowers **re-execute the same `accept()`** over a pinned base and attest
  (this is the CL1 token-model enforcement). gl0 and everyone else **adopt the byte-diff**
  (`reconstructInfoFromDiff` over the pinned prior) — they do NOT re-execute metagraph logic.
- **Why:** This is the **Polkadot shared-security** model (backers/approvers split; relay chain
  applies-and-verifies, never re-executes shard logic) — the settled design-of-record. Making gl0
  re-execute *everything* is the full-mirror path DEC-002 rejects (it can't, and it froze). Safety
  rests on "≥1 honest watchtower re-executes and can fraud-prove," not on a ⅔-honest committee, so
  the committee can stay small. Keeping global-state access **pinned** (not banned) avoids a
  mainnet regression (metagraphs keep `getLastSynchronized*` / cross-currency reads, threaded
  through the *carried* `globalSyncView` anchor).
- **Evidence:** ECONOMIC-TRUST-SHARDED-SECURITY-ARCHITECTURE.md §1-§2.2, §8 diagram; TRACK1-TRACK3
  spec §1 ("CORRECTED role-split model … settled for weeks"), §5 decision 3; HANDOFF §0; memory
  `project_economic_trust_architecture`. Both code paths survive by design (HANDOFF §3):
  `deriveAdoptedCurrencyState` = byte-diff adopter (`GlobalSnapshotAcceptanceManager.scala:1006`),
  `reExecDerivationWithDiff` = committee/watchtower re-exec (`ShardCheckpointWiring.scala:210`).
- **What breaks if reversed:** gl0-re-exec-everything = the full-mirror freeze (DEC-002).
  Banning pinned global reads = a mainnet regression (metagraphs lose global-state access).
  Removing the watchtower layer re-instates the honest-⅔-committee assumption and the α>1/(2S)
  committee-sizing pressure the design deliberately dissolves (ECONOMIC-TRUST §4.2).

### DEC-004: REVERT of `86f390130` (the k2-freeze) — do NOT re-apply it as written
- **What:** Commit `86f390130` (split the write-admission freeze onto a new k₂
  `nakamotoArchivedOrdinalRef`) was **reverted** by `376d09fbc`. Track-3 restores a
  density-revertable band **without** re-introducing that commit's mechanism.
- **Why:** `86f390130` was **inert AND self-contradicting**. Inert: `archive()` fired 0 times on
  all 6 nodes for the whole run because k₂ (dev 3200) is unreachable at the run's tips (~650–778),
  so the store gate was never armed — "no persistence floor and no retention bound active below the
  tip for the entire run" (FORENSIC-AUDIT:22, :56). Self-contradicting: it moved the *store* freeze
  to k₂ but left `ChainSelection.shouldSwitch` **still k₁-gated**, so it refused the very
  `k₁ ≤ depth < k₂` density reorgs it was meant to enable (FORENSIC-AUDIT:9, :73). It also removed
  the reorg-safety floor for any sub-k₂ run. The real fork-storm root was a *pre-existing
  non-deterministic `smtRoot`* in the consensus content compare (`GlobalSnapshotConsensusFunctions:270`),
  fixed separately by `376d09fbc`'s P0 (smtRoot-blind compare), validated 2026-06-27 (0 stateProof
  rejects, 6/6 converged, fork_count 1).
- **Evidence:** `git show 376d09fbc` (revert commit body); FORENSIC-AUDIT-CHAIN-SELECTION-2026-06-26.md
  §9, :16, :22, :56, :73, :92; memory `project_nakamoto_fork_resolution_k2_freeze`. Track-3 is
  explicitly **attempt #2** (TRACK1-TRACK3 spec §4) and re-confirming `376d09fbc` holds on HEAD is a
  named precondition before enabling deep reorgs.
- **What breaks if reversed (i.e. re-applying `86f390130`):** No persistence floor armed anywhere
  (inert archive) + a switch-refusal that blocks the intended density reorgs (self-contradiction) —
  the exact state the revert cured. And it does **not** fix the storm (that was `smtRoot`
  non-determinism, a different bug).

### DEC-005: Band-density deep-reorg DEFAULT-OFF (`band-density-reorg-enabled = false`)
- **What:** The commutative true-MRCA density comparator + floor-move to the k₂ "settled" marker
  are **implemented but gated OFF by default**. `false` = legacy k₁-freeze fork choice
  (byte-identical to the post-`376d09fbc` baseline).
- **Why:** The flag is **consensus-critical + cluster-uniform** — every node must run the same value
  or a split forks the chain — and this is attempt #2 of the reverted `86f390130`. It stays OFF
  "until a deep-fork sim validates cluster-uniformity." The comparator's commutativity
  (`compare(A,B) == flip(compare(B,A))`) is a proof-by-construction (true-MRCA walk decided by
  ordinal not arg-position; symmetric MRCA-slot anchor; deterministic tiebreak) that has **not** been
  adversarially verified on a cluster (HANDOFF §5.3).
- **Evidence:** `application.conf:381-392` (flag + comment: "Keep OFF until a deep-fork sim validates
  cluster-uniformity — attempt #2 of the reverted 86f390130"); `config/types.scala:167-177`
  (`kLookback = k₁+1`, `sWindow ≈ R/3`); HANDOFF §4 board ("LANDED but GATED OFF"), §5.3; commit
  `14fb8b32e`.
- **What breaks if reversed (flipped ON prematurely):** If the true-MRCA walk or the tiebreak is
  asymmetric on any input, nodes on opposite band-branches never converge — a permanent fork. This
  is why the flip is gated on a sim, not a code review (OPEN-002).

### DEC-006: `rebootstrap-enabled` stays ON — the TRUE→FALSE flip is HARD-GATED, not done
- **What:** `RebootstrapOrchestrator` default is **TRUE/ON** and stays ON. The flip to OFF is
  documented as hard-gated; it was **not** flipped in the S5 slice.
- **Why:** A node that self-finalized a divergent branch and refuses canonical peers self-heals only
  via rebootstrap; with it OFF such a node **forks the global mptRoot forever** (the sharded
  data-app-fee reorg storm). The flip to OFF is only safe once the *replacement* recovery paths are
  proven: it is hard-gated on **both** the band-density deep-reorg path (S3) **and** the disk-backed
  k₂ revert-executor (S4) being e2e-green, **plus** a deep-fork cluster-uniformity sim. The prior
  session's claim that the default was OFF was a **factual error corrected this session** (S5,
  `605b8a496`): the live default was ON (`application.conf`, scaladoc, attestation-and-finality.md all
  wrongly said OFF). Post-flip the mechanism is retained as a manual last-resort escape hatch.
- **Evidence:** `application.conf:368-379` (default TRUE + the S5 gate comment: "The TRUE→FALSE flip
  is HARD-GATED on BOTH the density-past-k₁ deep-reorg path AND the disk-backed k₂ revert-executor
  (S4) being e2e-green, PLUS a deep-fork cluster-uniformity sim"); TRACK1-TRACK3 spec §4 (S5),
  §5; commit `605b8a496`; HANDOFF §4.
- **What breaks if reversed (flipped OFF now):** A divergent-self-finalized node loses its only
  recovery path and forks the global mptRoot permanently — with no validated replacement, since S3
  is gated OFF (DEC-005) and S4's deep revert is unverified on a cluster (OPEN / §5.6).

### DEC-007: Disk-backed k₂ revert (Cardano UTxO-HD analog) over a shallow revertible band
- **What:** Retention is **disk-backed to k₂**: the in-RAM undo journal (`undoJournalRef`) is bounded
  to a fraction of k for the fast near-tip path; a **deep** revert (to k₂) loads from disk
  (`deleteAbove(fork) + readState(fork) → rebuild → re-fold`). The `signedBytesStore` contiguous
  cutoff is raised to cover k₂ on disk. RAM window = a perf knob; **revert depth = k₂**.
- **Why:** This is an explicit **user direction that supersedes the verify agent's shallow-band
  recommendation** — the Cardano UTxO-HD analog (state on disk, bounded RAM working set). The heap
  blowup was the *in-memory* undo journal (k₂ ChangeSets in RAM ≈ 5 GB), **not** the base state
  (`MptStateStorage` is already disk-backed, per-ordinal `writeState`/`readState`, prunable). So the
  fix bounds RAM and pushes deep reverts to the existing disk machinery — keeping the full
  `(settled, finalized]` band density-revertable without a 5 GB RAM cost.
- **Evidence:** TRACK1-TRACK3 spec §4 (S2, "DISK-BACKED k2 revert (USER DIRECTION — supersedes the
  verify's shallow-band)") and §5 decision 4 ("disk-backed k2 revert (NOT shallow band) — Cardano
  UTxO-HD analog"); commits `2191510a6` (S2 disk-backed retention), `3e47d1904` (S4
  `MptOverlay.revertToOrdinal`: shallow-RAM `:486` / deep-disk `:1047`); HANDOFF §4, §6.
- **What breaks if reversed (shallow band instead):** A shallow revertible band cannot reach a
  liveness-degraded reorg deeper than the RAM window, so the network cannot fall back to
  density/length below that depth — re-creating the write-freeze deadlock class (P-11) that Track-3
  exists to cure.

### DEC-008: Consensus fractions are EXACT `Ratio`, never `Double`
- **What:** LDD `baseline`/`amplitude` (and other tunable fractions) are read as exact rationals
  from HOCON `"n/d"` strings straight into a `Ratio` ConfigReader — never through `Double`.
- **Why:** `0.05`-as-`Double` round-trips into a garbage denominator; `"1/20"` is exactly 1/20 on
  **every** node. These are consensus-critical (identical on every node or the cluster forks), so a
  float rounding divergence between operators is a fork.
- **Evidence:** `application.conf:326-337` ("EXACT fractions … never through Double (0.05-as-Double
  round-trips into a garbage denominator)"); `config/types.scala:180-188` (`GlobalLddConfig` fields
  `baseline: Ratio`, `amplitude: Ratio`); commit `2447aac0c` (exact-Ratio consensus config).
- **What breaks if reversed:** Different nodes compute different LDD election thresholds from the
  same config → divergent eligibility → cluster fork.

### DEC-009: No embedded value-defaults for consensus params (config is authoritative)
- **What:** Consensus-critical tunables have **no source-level default**. Required (or `Option`/`None`
  only). `archivalDepthK`/k₂ are REQUIRED (derived, not defaulted); `LddConfig.Default` was deleted.
- **Why:** Scattered/embedded defaults risk cluster-split — different operators silently running
  different values for consensus-critical parameters. Config is the single authoritative source;
  env override goes through HOCON `${?ENV_VAR}` substitution, not code reads (project-wide rule).
- **Evidence:** `application.conf:325` ("CONFIG IS AUTHORITATIVE — there is NO source-level default
  (LddConfig.Default removed 2026-06-30)"); commit `2447aac0c` ("Remove embedded value-defaults for
  consensus params (required, or Option-None only)"); CLAUDE.md "Config Conventions"; memory
  `feedback_prefer_hocon_over_sysenv`. (Note the *neutral fallback* `DefaultConfirmationDepthK = 32`
  at `types.scala:190-192` is a documented boot-safety fallback for a hand-trimmed config, not a
  consensus default — all four standard envs are always present in `application.conf`.)
- **What breaks if reversed:** Re-introducing embedded defaults re-opens the cluster-split risk
  (operators diverging on an un-set consensus value).

### DEC-010: VRF tiebreaker KEPT as the final chain-selection step
- **What:** The final, fully-deterministic tip tiebreak in `ChainSelection` is **lower VRF output
  wins** (then hash on an exact — cryptographically unreachable — VRF tie). It is NOT removed. There
  is no chain-growth-wait.
- **Why:** Standing project rule (`feedback_vrf_tiebreaker_keep`): keep the lower-VRF tiebreaker as
  the final step. S3 *strengthened* it to be commutative — `vrf<0 ? A : B` with a hash fallback on
  exact ties — which changes **no honest outcome** (VRF collisions don't occur) and makes the
  adversarial-equivocation case converge (`compare(A,B) == compare(B,A)`). It is a strict improvement
  over the pre-S3 arg-order-dependent `vrf <= 0 ? tipA : tipB`.
- **Evidence:** `ChainSelection.scala:299-308` (final tiebreak: lower VRF wins, then hash);
  scaladoc :27 (short-fork tiebreakers: ordinal → slot → VRF); memory `feedback_vrf_tiebreaker_keep`.
- **What breaks if reversed:** Removing the VRF tiebreak (or replacing with a chain-growth-wait)
  leaves equal-ordinal/equal-slot forks with no deterministic, commutative resolver → nodes pick
  different tips → non-converging fork.

### DEC-011: Committee slot-ID = `lastSnapshotHash` (parent hash), NOT snapshot ordinal
- **What:** The committee VRF is keyed on the metagraph snapshot's **parent hash**
  (`lastSnapshotHash`), rotated each eta-epoch — not on the snapshot ordinal.
- **Why:** Parent-hash keying gives a tight **slashing-identity algebra** — "same
  `(metagraph_address, parentHash)` + two different `binary_hash` from one operator" is exactly the
  equivocation surface — and **uniform readability** across heterogeneous (currency + custom)
  metagraph types. An LDD-style election here was tried and abandoned; a Praos-style static threshold
  is explicitly not wanted. Predictable-within-epoch + rotate-each-epoch is acceptable *because the
  watchtower layer* (not committee secrecy) is the security boundary.
- **Evidence:** `CommitteeSortition.scala:35-37` ("Slot identity = lastSnapshotHash … key the
  committee VRF on the metagraph snapshot's parent hash rather than its ordinal … exactly the
  slashable surface"), :22, :64-89 (VRF message over `(eta, metagraphAddress, parentHash)`);
  ECONOMIC-TRUST §4.1; memory `project_committee_sortition_slot_id_decision` (2026-05-17,
  "don't re-litigate").
- **What breaks if reversed (ordinal keying):** Loses the equivocation slashing-identity match
  (two different binaries at the same ordinal but different parents would not be a clean
  same-slot equivocation) and breaks uniform readability across metagraph types.

### DEC-012: Set-valued in-band P (`A = {o ∈ U : o ≤ view ∧ o ∉ P}`) over scalar-interval derivation
- **What:** The set of global-change ordinals to apply is derived as a **set-valued in-band P**
  (`P = ⋃ GlobalSnapshotsProcessed.ordinals` from the committed chain), replacing the node-local
  cached-scalar-interval derivation.
- **Why:** The scalar interval `(prior_view, cur]` was **REFUTED** (adversarial-verify Workflow
  `wf_f60d4bcf-80b`): it permanently drops cross-shard spends when `globalSyncView` runs ahead of
  the local head — an ordinal enters the unapplied set `U` *below* the advanced view, so the scalar
  interval misses it forever. The set-valued P (already in-band from retained CL0 history) catches
  it. Invariant to preserve: **P-window ⊇ U-window**.
- **Evidence:** TRACK1-TRACK3 spec §3 (1a RESOLVED, the REFUTED analysis with CSAM line anchors),
  §0/§7; commit `8aa1de3dd`; suite `CurrencySnapshotProcessedSetSuite`; HANDOFF §4, §6.
- **What breaks if reversed:** Permanent, silent cross-shard-spend loss whenever peer-sync advances
  the view ahead of the local head — plus a restart re-apply bug the set form also fixes.

---

## Section B — GENUINELY OPEN (Fable has license to redirect)

### OPEN-001: Committee-size / `K_S` decoupling vs the α_total > 1/(2S) CQ-collapse bound
- **Status:** open.
- **Why open:** The economic-trust design *asserts* the α>1/(2S) committee-sizing floor "dissolves"
  because safety rests on the watchtower fraud-proof (1-of-N honest), not on a ⅔-honest committee —
  so it keeps `k-draw=8 / k-quorum=6` and withdraws the earlier `K≈400` (ECONOMIC-TRUST §4.2). But
  the α_total > 1/(2S) result is about a *different* failure: an adversary concentrating **local**
  stake to push `α_local > 1/3` and break a shard's own consensus/CQ (CROSS-SHARD-MITIGATION-PROPOSAL
  §"α_total > 1/(2S)", :141-144). Whether the watchtower layer *fully* covers the CQ-collapse
  failure (vs only the committee-attestation failure) is not proven — the HANDOFF still lists it as
  an open question (§5.7). k-draw=8/k-quorum=6 is described as a "reasonable starting point to
  validate," i.e. not a derived safety value.
- **Input we want from Fable:** Does the watchtower fraud-proof + DA + slashing stack actually
  dominate the α_total > 1/(2S) collapse bound at `K_S = 8/6`, or is there a residual regime (e.g.
  a liveness/CQ break that produces no *attestable* fault for a watchtower to dispute) where the
  small committee is unsafe? If the latter, what is the minimum `K_S` (or the value-at-risk cap)?
- **Constraint:** Any answer must preserve the role-split + watchtower model (DEC-003) and the
  parent-hash-keyed, epoch-rotated, predictable-within-epoch sortition (DEC-011) — no intra-epoch
  re-draw or forced operator-to-shard assignment.

### OPEN-002: The band-density flag-flip criterion — what the deep-fork sim must show
- **Status:** open.
- **Why open:** DEC-005 keeps `band-density-reorg-enabled = false` "until a deep-fork sim validates
  cluster-uniformity," but the *acceptance criterion for that sim* is not pinned down. The
  comparator's commutativity is proof-by-construction, unverified adversarially (HANDOFF §5.3), and
  a precondition — re-confirming the `376d09fbc` smtRoot-determinism fix holds on HEAD — is a
  named Track-3 gate (TRACK1-TRACK3 §4) but is not itself a sim.
- **Input we want from Fable:** The precise pass condition for the deep-fork cluster-uniformity sim
  — e.g. an adversarial input family that would prove `densityCompare` is non-commutative (asymmetric
  MRCA walk or tiebreak), and the cluster-convergence metric (fork_count → 1, no oscillation) the
  sim must hit before the flag can go ON. Also: is a sim sufficient, or is an e2e deep-fork required?
- **Constraint:** The flag is cluster-uniform consensus-critical (a split value forks the chain);
  any criterion must be one that gates a *global* flip, and it must not weaken the post-`376d09fbc`
  baseline (which is the current OFF behavior).

### OPEN-003: #23 — I-PIN residual purity (a global-state-at-anchor reader)
- **Status:** open (Track-1 #13 landed *partial*).
- **Why open:** #13 made `accept()` read pinned global state on the re-exec/validator path
  (forcing-test `CurrencySnapshotAcceptancePuritySuite` GREEN, `d06e6fb78`). But the **message-path**
  global reads (`balances` / `lastCurrencySnapshots`) and the **ml0 producer** are still
  **head-sourced**, not anchored to the carried `globalSyncView`. The I-PIN invariant (every
  global/cross-shard read inside `accept()` resolves against the *carried* anchor) is therefore only
  partially enforced (ECONOMIC-TRUST I-PIN, R1/R4; HANDOFF §4 board "#23 PENDING").
- **Input we want from Fable:** The design for a **global-state-at-anchor reader** that pins the
  message-path balances/`lastCurrencySnapshots` reads and the ml0 producer read to the carried
  anchor — including whether the ml0 producer can be made to source from the pinned view without
  reopening a head-lag stall, and how to detect a residual live read (a forcing test).
- **Constraint:** Must keep global-state access *pinned, not banned* (no mainnet regression, DEC-003);
  a pinned read must yield a byte-identical witness for producer, committee, and every watchtower
  (pure function ⇒ split-safe).

### OPEN-004: Follower contiguous per-ordinal store for DEEP anchors
- **Status:** open (deferred).
- **Why open:** The disk-backed k₂ retention (DEC-007) works near-tip, but the **follower**
  contiguous per-ordinal store for *deep* anchors is deferred — deep-anchor pinned reads on the
  cl0/dl1 follower path are unsupported (HANDOFF §4 board "DEFERRED"; TRACK1-TRACK3 §3 2a built the
  reader on all 3 paths, but deep-anchor retention behind it is the gap).
- **Input we want from Fable:** Whether a follower must retain a contiguous per-ordinal store all the
  way to k₂ (disk cost) to serve deep pinned reads, or whether a bounded window + on-demand re-pull
  suffices — and the determinism argument for either (a deep pinned read must reproduce byte-identical
  state on every follower).
- **Constraint:** Must compose with the disk-backed k₂ revert (DEC-007) and the pinned-reader-on-all-
  3-paths decision (settled, TRACK1-TRACK3 §5 decision 1) — no head-fallback (the reader is hard-None
  by design).

### OPEN-005: `standardCompare` ordinal→slot→VRF pseudo-predictability exposure
- **Status:** open.
- **Why open:** The short-fork path uses `ordinal → slot → VRF`. In **Taktikos** any fork-choice
  tiebreaker deterministic from local state is pre-computable/grindable (pseudo-predictability — the
  substrate is Taktikos, NOT Praos; Praos bounds don't transfer). The VRF step only fires on an
  ordinal+slot tie (rare) — but "rare" is not proven to equal "safe" (HANDOFF §5.4; `taktikos` skill §4).
- **Input we want from Fable:** Whether the ordinal→slot ordering (before VRF) exposes a grindable
  advantage a Taktikos adversary can exploit to steer fork choice, and if so whether the VRF step
  should move earlier or the ordering be hardened. A concrete adversarial construction (or a proof
  that the tie-frequency makes it economically irrelevant) is the deliverable.
- **Constraint:** Must keep the VRF tiebreaker as the final, commutative, deterministic step
  (DEC-010) and stay cluster-uniform; any reordering must preserve `compare(A,B) == compare(B,A)`.

### OPEN-006: Implemented-but-UNVERIFIED (no cluster/adversarial confirmation)
- **Status:** open (unit-green, not cluster/adversarially verified).
- **Why open:** Several landed items are verified in **unit only**; the delete-of-authoritative-
  override is a real behavior change and the honest path now relies on ml0 byte-consistency
  (HANDOFF §4 "E2E cluster sanity pass — NOT RUN"). Specifically unverified on a cluster / under a
  Byzantine actor:
  1. **Adopt ≡ re-exec byte-equivalence under a Byzantine ml0** — is there any ml0-controlled input
     where the byte-diff and honest re-exec disagree and the adopter cannot detect it?
     (HANDOFF §5.1; `ShardCheckpointWiring.scala:210`, `GlobalSnapshotAcceptanceManager.scala:1006`.)
  2. **GAP-1 field-presence gate soundness** — the `stateProof.<field>Proof.isDefined` gate
     distinguishes genuine `None` from `reconstructInfoFromDiff`'s always-`Some(empty)`; is there a
     field where non-empty state yields no proof (or empty yields one)? (HANDOFF §5.2; `dc0dbe1c7`.)
  3. **k₂ deep-revert determinism cluster-wide** — does `deleteAbove + readState + re-fold` reproduce
     byte-identical state on every node, or can RAM-journal-vs-disk retention differences make the
     revert non-deterministic? (HANDOFF §5.6; `MptOverlay.scala:1047`, `2191510a6`.)
  4. **`densityCompare` commutativity adversarially** — proof-by-construction only (OPEN-002).
- **Input we want from Fable:** Prioritize which of these to break first, and construct the
  adversarial input (or argue the invariant holds). These are hypotheses to prove/break, not known
  defects.
- **Constraint:** Any fix must not re-introduce a reverted-known-broken state (esp. DEC-004 the
  k2-freeze, and DEC-002 the full-mirror fold).

---

## Section C — ASSERTED-but-UNJUSTIFIED (settled-in-tone, rationale NOT found — treat as risk)

### RISK-001: Production k₁ = 1024 (mainnet) has no recorded rationale for *that* value
- **What's asserted:** k₁ is the single free knob; mainnet runs `confirmation-depth-k = 1024`.
- **Why it's a risk:** The **recorded** rationale justifies ≈ 255–271, not 1024. The scaladoc says
  the "Default 255 chosen to approximate Cardano-equivalent 10⁻¹² common-prefix violation … k≈271,
  so 255 is a deliberately-conservative operating point" (`SnapshotLeaderLoop.scala:992-996`), and
  HANDOFF §8 repeats "k₁ default 255 … k≈271 for strict 10⁻¹²". But the **live** config is mainnet
  **1024** (`application.conf:309`) — ~4× the derived bound — and I found **no recorded reason for
  1024 specifically** (only the general "deeper = safer / raising k increases worst-case finality
  time" tradeoff at `SnapshotLeaderLoop.scala:998-1001`). So the production number in force is not
  the number the research result grounds, and the gap (271 → 1024) is unexplained. The k₂ = 100·k₁
  derivation (DEC-001) inherits this: k₂ ≈ 8 days is stated *at k₁=1024*, so the "8 days" figure
  rides on an unjustified k₁.
- **Why it's the dangerous one:** Every derived depth (R, k₂, kLookback, sWindow) scales off k₁, so
  an unexamined k₁ propagates into the entire finality band, the challenge window (K = k₁), and the
  ≈8-day archival horizon. And the docs are **internally inconsistent** (255 in prose/scaladoc vs
  1024 in config) — exactly the "losing the thread" the review is meant to catch. Recommend Fable
  confirm whether 1024 is intended (and why) or whether it should track the ~271 research bound.

*(No other Section-A decision was demoted here: DEC-001..012 each carry a grounded, cited "Why".
The k₁ *magnitude* is the sole settled-in-tone value whose recorded rationale does not match the
value actually in force.)*
