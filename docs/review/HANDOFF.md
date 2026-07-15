# Sharding Production-Readiness — Master Handoff

> **HISTORICAL PRE-FIX REVIEW PACKET.** This document is preserved as audit
> evidence for the 2026-07-07 tree; it is not current architecture, a migration
> plan, or a compatibility contract. In particular, pre-fix unchecked/roots-only
> byte-diff adoption, committee-as-authority, and authoritative-push descriptions
> are superseded. Replay-certified, scoped diff adoption remains the target.
> Revalidate every file/line claim against current source and use
> [`../../AGENTS.md`](../../AGENTS.md), ADR-0016, and ADR-0017 as the design of
> record.
> Names of deleted authority-design documents below are historical citations
> only. Their content remains recoverable from git history at commit `725b25b`;
> the paths are intentionally absent from the current tree.

> **Purpose.** Single source of truth for the Track-1 / Track-3 sharding-production-readiness
> workstream so any agent (Fable, Opus, Sonnet) or human can pick it up cold. Every claim
> here is grounded in a commit, a `file:line`, or a named design doc — not memory.
> **Verified against the working tree on 2026-07-07** (branch `feature/committee-state-diff`,
> HEAD `21933559c`). Where this doc says "verified", it means a command was run this session
> and its output is in the Evidence Appendix at the bottom.

---

## 0. The North Star (end goal)

Make **execution sharding** production-ready: cross-shard **L2 currency-app metagraph**
interactions, **sequenced by the global hypergraph (gl0)**, with the currency **token model
re-executed and enforced by the hypergraph** — not merely mirrored.

Concretely, the target end-state is:

- A **metagraph (ml0)** runs `accept()` and produces its currency incremental snapshot (the DATA is
  authoritative at DL1).
- The **shard committee** (`K_S` = a VRF-sortitioned subset of gl0 operators) **re-executes** the
  same `accept()`, **produces the byte-diff**, then **ships BOTH the byte-diff AND the currency
  incremental snapshot**. This is the enforcement of the CL1 token model (allow-spend / spend /
  token-lock / transfer / fee / balance / supply) by the hypergraph.
- **Watchtower nodes** (VRF-sampled) **re-execute to check** — they have the shipped currency
  incremental snapshot, re-run `accept()`, and raise a fraud proof (→ `InvalidStateProof` slash) on
  mismatch.
- **Everyone else just ADOPTS the byte-diff** (`reconstructInfoFromDiff`) and **checks the state
  proof** — which should **trivially match** precisely because they adopted the byte-diff. They do
  **not** re-execute metagraph logic.

> **User-stated intent, verbatim (2026-07-07), recorded so we don't drift:** *"the shard committee
> re-exec, produce the byte diff, then ship the byte diff and the currency incremental snapshot so
> the watchtower nodes can re-exec to check but everyone else can just adopt and check the state
> proof (which should trivially match because of the byte diff adoption)."* The as-built code
> diverges from this on one point — see §1.1 (committee-member re-exec-before-attest appears absent;
> gl0 accepts on quorum-sig). That gap is the top verification target, **not** a re-design license.

This is the **Polkadot shared-security** model: shards execute + attest; watchtowers police via
fraud proof; the relay chain (gl0) and everyone else adopt-and-verify the state proof, never
re-executing shard logic. Economic trust is **guaranteed by gl0**; metagraph data stays
authoritative at the metagraph. Full architecture:
`docs/nakamoto/ECONOMIC-TRUST-SHARDED-SECURITY-ARCHITECTURE.md` (36 KB, the load-bearing
design-of-record for the trust model).

The **consensus substrate underneath is Ouroboros Taktikos, NOT Praos** (LDD snowplow,
maxvalid-tk fork choice, pseudo-predictability). Praos common-prefix bounds do **not** transfer.
See the `taktikos` skill and `~/repos/research-nipopos-2026/docs/TAKTIKOS-NOTES.md`.

**Track map — what actually landed (`SHARDING-PRODUCTION-READINESS-PLAN.md` §1-4).** The plan has
**three** tracks:
- **Track-1 (enforcement/purity):** adopt-byte-diff ≡ re-exec-over-pinned-base. **LANDED this session.**
- **Track-2 (latency / optimistic finality — "defer-not-revert"):** watchtower approvals gate fast
  *spendability*, depth-k₁ backstops. **NOT BUILT** (grep `kApprove|stageCheckpoint|spendabilityGate`
  = 0 hits). Today effects apply **immediately at the fold** — there is **no spendability staging
  gate**. This is a real omission because Track-3 now makes the `(k₂,k₁]` band revertable, so a
  band reorg can revert an **already-spent** cross-shard output (see §5.8). Do not assume optimistic
  spendability exists.
- **Track-3 (finality band — kill k₁-absolute):** density-revertable `(k₂,k₁]` band. **LANDED this
  session (behind a default-off flag).**

Note: "optimistic finality" as a *settled decision* (§6) refers to the **attestation-2/3 fast path**
(`TipTracker`, built) for finalizing the *snapshot* — it does **not** mean Track-2's spendability
staging is built. Keep the two distinct.

---

## 1. The two invariants this workstream defends

Everything in Track-1 and Track-3 exists to protect two properties. State them precisely — a
reviewer's job is to try to break them.

- **Track-1 (enforcement soundness / adopt-purity):** the state that gl0-and-everyone ADOPT via
  the byte-diff must be **byte-identical** to the state a committee/watchtower gets by
  **re-executing** `accept()` over the same pinned base. If the two can diverge for an *honest*
  metagraph, honest nodes fork or fail-close; if they can be made to agree on a *dishonest*
  claim, the token model is not enforced. The whole point is: *adopt without re-exec is only
  safe if re-exec-over-pinned-base provably reproduces the attested root.*

- **Track-3 (finality band correctness):** `k₁` (`confirmation-depth-k`) is **operational
  finality**; it is **per-environment** — **mainnet 1024 / testnet+integrationnet 256 / dev 32**
  (`application.conf:308-314`, dev-overridable via `${?NAKAMOTO_CONFIRMATION_DEPTH}`). ⚠ The "255"
  in code comments/skill/prior docs is **STALE** (see §3.1) — do not cite it. `k₂`
  (`keepDepthBehindFinalized = 100·k₁`, mainnet = 102 400 ≈ 8 days) is the **only absolute
  floor**. The `(k₂, k₁]` band must be **density-revertable** (maxvalid-bg
  / Taktikos densest-chain), so a liveness-degraded network can fall back to length/density
  without a permanent write-freeze deadlock. The as-built code wrongly made `k₁` absolute
  (the P-11 deadlock); Track-3 restores the band **without** re-introducing the reverted
  `86f390130` k2-freeze (which was inert + self-contradicting — see `project_nakamoto_fork_resolution_k2_freeze`).

### 1.1 ⚠ The enforcement model AS BUILT ≠ as described — the #1 thing for Fable to verify

The role-split story says *"committee + watchtowers re-execute `accept()` and attest."* Verified
against source 2026-07-07 (byte-contract + invariants agents, confirmed by me), what is actually
built is subtler, and the difference is the whole security argument:

- **gl0's happy path does NOT re-execute.** `ShardCheckpointGl0AcceptanceManager.verifyEmbedded`:
  `if (distinctSigners >= kQuorum) ⇒ Accepted` via `Path.TCount` — **pure committee-signature
  quorum, no derivation** (`ShardCheckpointGl0AcceptanceManager.scala:386-396`). Re-execution
  (`reExecPath` / `Path.TDepth1`, `:545-587`) fires **only sub-quorum** (degraded liveness), and
  reads the **LIVE base** — it ignores the pinned `executionBaseOrdinal` (`SharedServices.scala:327-331`)
  on the claim that the per-MG root is base-independent under `AdoptFromSignedFields` (that claim is
  **unproven** — §5.9).
- **The single re-execution on the happy path is the PRODUCER's** — `ShardCheckpointProducer` /
  `ShardCheckpointWiring.reExecDerivationWithDiff` computes the root the committee then signs.
- **Whether committee ATTESTERS re-execute before signing is UNVERIFIED and appears ABSENT.**
  `ShardCheckpointAttestationEmitter` signs a checkpoint that *"became this node's best tip"* +
  VRF-leader eligibility; **no re-execution is visible before the signature**
  (`ShardCheckpointAttestationEmitter.scala`; and v1 acceptance *"does not cryptographically verify
  the gossiped attestation's VRF proof"*).

**Why this is the crux:** if attesters attest-to-best-tip without independently re-executing, a
Byzantine producer's false root is checked by *nobody* on the quorum path — the token model is
enforced only if a quorum of committee members **independently re-derived** before signing. **This
is the #1 question for Fable, to answer before any "safe" conclusion.** It is where three
independent agents converged: invariants `INV-SAFETY-007` ("re-exec-primary UNBUILT"), the
byte-contract ("quorum path does not re-execute"), and the gaps critic (I-AUTH single-sig, §4).

---

## 2. What landed this session (all on `feature/committee-state-diff`, NOT pushed)

14 commits, session-diff **117 files, +5250 / −2480, 94 Scala, 14 docs** (verified:
`git diff --stat 2447aac0c^..HEAD`). Final state **CI-green** (verified: `SBT_EXIT=0`,
`scalafmtCheckAll` clean, test suites `Failed 0, Errors 0` across the four verification runs,
previously-NPE-ing `ShardCheckpointProducer/FanOut` suites clean).

| Commit | Slice | What it does |
|---|---|---|
| `2447aac0c` | base | exact-`Ratio` consensus config + **remove value-defaults** (`archivalDepthK`/k₂ now REQUIRED, no embedded default) |
| `670cc8f56` | S1 | k₂-unify: single accessor `NakamotoConfig.keepDepthBehindFinalized`; `SettledOrdinalTracker`; GET `/global-snapshots/settled` |
| `8aa1de3dd` | 1a | **set-valued in-band P** — `P = ⋃ GlobalSnapshotsProcessed.ordinals` from the committed chain; replaces node-local cache (fixes permanent cross-shard-spend loss + a restart re-apply bug) |
| `06ad808c9` | S1.5 | **marker split** — `nakamotoSettledOrdinalRef` (k₂) distinct from `nakamotoFinalizedOrdinalRef` (k₁); store owns reset |
| `e2f266ff1` | 2a | **`PinnedCurrencyInfoReader`** — by-ordinal per-MG Info reader, 2-level verify (content-hash pin + mptRoot reproduction), hard-None no-HEAD-fallback; wired on all 3 paths |
| `db582dd42` | docs | corrected design docs + spec + 3 canonical diagrams |
| `14fb8b32e` | S3 | **floor-move behind default-off `band-density-reorg-enabled`**; **commutative `densityCompare`** (true-MRCA walk, symmetric MRCA-slot anchor, deterministic tiebreak) |
| `2191510a6` | S2 | **disk-backed k₂ retention** — RAM undo-journal bounded, `signedBytesRetentionDepth`→k₂; `MptOverlay.journalSizes` gauges |
| `d06e6fb78` | #13 | **I-PIN read-at-anchor** — `accept()` reads pinned global state on the re-exec/validator path (forcing-test `CurrencySnapshotAcceptancePuritySuite` GREEN) |
| `3e47d1904` | S4 | **`MptOverlay.revertToOrdinal`** (shallow-RAM / deep-disk) + `EtaStateManager.forgetUncommitted` |
| `605b8a496` | S5 | docs — corrected RebootstrapOrchestrator Default-OFF→ON factual error; flip stays gated |
| `eb9bf9a5e` | #15 | **execution-base-pin** — `executionBaseOrdinal` in `ShardCheckpointSigPreimageV2` + proto **field 11** (`diff_base_ordinal`); producer atomic stamp; adopter re-derives at the pinned ordinal |
| `dc0dbe1c7` | #18/3a | **remove the authoritative-override LAYERING** (see §3 correction); byte-diff-adopt primary; **GAP-1 re-grounded on `stateProof.<field>Proof.isDefined`** |
| `21933559c` | lint | scalafmt pass + `decodeArtifacts` `.handleError` (fixes a pre-existing σ=1 NPE, #24) |

---

## 3. ⚠ Precision correction (surfaced 2026-07-07 against source)

The prior session summary said commit `dc0dbe1c7` **"deletes both authoritative overrides
(`GSAM.deriveAdoptedCurrencyState` + `ShardCheckpointWiring.reExecDerivationWithDiff`)."**
**That phrasing is imprecise and would mislead a reviewer.** Verified against source:

- `GlobalSnapshotAcceptanceManager.deriveAdoptedCurrencyState` **still exists**
  (`GlobalSnapshotAcceptanceManager.scala:1006`).
- `ShardCheckpointWiring.reExecDerivationWithDiff` **still exists**
  (`ShardCheckpointWiring.scala:210`) and is still called from `SharedServices.scala:333,369`.

What `dc0dbe1c7` actually removed is the **authoritative-override *layering inside* those two
functions** — the adopter's `.copy(balances = authoritative.getOrElse(...), …)` and the
producer twin's 8 `auth*` feeders. Both functions now emit the plain reconstructed / re-exec
state directly. This is **correct and expected** for the role-split model: `reExecDerivationWithDiff`
*must* survive because it **is** the committee/watchtower re-exec (enforcement) path;
`deriveAdoptedCurrencyState` *must* survive because it **is** the byte-diff adopter. The commit
made the diff-adopt path *primary* by deleting the override that used to paper over base drift —
it did **not** delete either code path. (Commit body confirms: *"adopter … uses `nextInfoRaw`
directly; producer twin … emits plain `infoOf(next)`; deleted the 8 `auth*` feeders."*)

**This is exactly the class of "losing the thread" the review is meant to catch.** Any handoff
statement of the form "we deleted X" should be verified as "we deleted *behavior* X" vs "we
deleted *symbol* X" before a reviewer trusts it.

### 3.1 ⚠ Second correction: k₁ is **1024 (mainnet), not 255**

Verified against source 2026-07-07 (surfaced by the decision-log agent, confirmed by me):
`confirmation-depth-k` is a **per-environment HOCON block** — **mainnet 1024 / testnet 256 /
integrationnet 256 / dev 32** (`application.conf:308-314`), resolved once for the active env via
`NakamotoConfig.confirmationDepthK(env)` (`types.scala:153`). **Every "255" is stale** — it is an
old Scala-side fallback comment (e.g. `SnapshotLeaderLoop.scala:992` *"Default 255 chosen…"*, and
the `taktikos` skill's k-sweep tables) that the per-env HOCON migration superseded.

- The **value 1024 IS justified** (do not treat it as unexplained): the config records *"powers of
  two to avoid off-by-one surprises: mainnet keeps the deep 1024 … ≈8 days at k₁=1024 / 7s
  snapshots"* (`application.conf:303-304`, `types.scala:162`). The sims justify k ≥ ~271 for strict
  10⁻¹² CP-violation vs a 1/3 adversary; prod runs the **more conservative** power-of-two 1024.
- The **hazard is the stale "255"**, because k₁ is the master knob **every** derived depth scales
  from — `k₂ = 100·k₁` (mainnet 102 400), `R = round(3.1·k₁)`, `kLookback = k₁+1`, `sWindow`,
  the challenge window, and the "≈8-day" archival horizon. A reviewer or agent that reasons from
  255 will mis-size all of them. **FOLLOW-UP (not yet done):** scrub stale "255" from code
  comments + the `taktikos` skill k-sweep tables (rebuild them for k=1024/256/32) — comment-only,
  low-risk, but do it before Fable reads the skill.
- **Same drift class in LDD:** the `taktikos` skill (+ stale scaladoc) say `γ=15`, `ψ=1`; live
  config is **`γ=16`** — `ldd.scala:27-28` literally documents *"γ=15-here vs γ=16-in-config is
  exactly the class of bug this removal prevents"* (source `Default` removed; prod reads HOCON
  `nakamoto.ldd`); `ψ` scaladoc even stale-says `0` (live `1`). **Guardrail:** treat the `taktikos`
  skill's numeric tables as *possibly stale* — verify LDD + k against `application.conf` before quoting.

---

## 4. State of ALL work (the board)

| Item | State | Evidence / gate |
|---|---|---|
| Track-1: 1a set-valued P | **LANDED** | `8aa1de3dd`; `CurrencySnapshotProcessedSetSuite` |
| Track-1: 2a 3-path reader | **LANDED** | `e2f266ff1`; `PinnedCurrencyInfoReader.scala` |
| Track-1: #13 I-PIN read-at-anchor | **LANDED (partial — see #23)** | `d06e6fb78`; forcing-test GREEN |
| Track-1: #15 execution-base-pin | **LANDED** | `eb9bf9a5e`; proto field 11, `ShardCheckpointSigPreimageV2` |
| Track-1: #18/3a override-layering removal + GAP-1 | **LANDED** | `dc0dbe1c7`; `MultiBranchAdoptSuite` |
| Track-3: S1/S1.5 k₂-unify + marker split | **LANDED** | `670cc8f56`,`06ad808c9` |
| Track-3: S2 disk-backed k₂ retention | **LANDED (follower deep-anchor store DEFERRED)** | `2191510a6` |
| Track-3: S3 commutative densityCompare + floor-move | **LANDED but GATED OFF** | `14fb8b32e`; flag `band-density-reorg-enabled = false` (`application.conf:391`) |
| Track-3: S4 revert-executor | **LANDED** | `3e47d1904`; `MptOverlay.revertToOrdinal` |
| **#23 I-PIN residual purity** | **PENDING (open task)** | message-path global `balances`/`lastCurrencySnapshots` + ml0 producer still head-sourced → need a **global-state-at-anchor reader** |
| band-density reorg flag flip | **GATED — needs sim** | requires §5.6 deep-fork **cluster-uniformity sim** before `true` |
| `rebootstrap-enabled` config flip | **HARD-GATED — documented, not flipped** | S5 docs |
| follower contiguous per-ordinal store (deep anchors) | **DEFERRED** | near-tip works; deep anchors unsupported |
| **E2E cluster sanity pass** | **NOT RUN — user-run** | delete-override is a real behavior change; honest path now relies on ml0 authoritative-push byte-consistency, verified in **unit only** |
| Push branch / open PR | **NOT DONE** | local branch only; push requires user ask |
| **Committee re-exec-before-attest (enforcement teeth)** | **⚠ UNVERIFIED / APPEARS ABSENT** | §1.1 — happy path is quorum-sig; only the producer + sub-quorum re-execute; attester pre-sign re-exec not found |
| **Track-2 optimistic spendability staging** | **⚠ UNBUILT + was undisclosed** | §0 track-map; `kApprove`/`stageCheckpoint` grep = 0; effects apply at the fold, no staging gate |
| **I-AUTH metagraph-source authenticity** | **SINGLE-SIG (likely by design)** | `StateChannelValidator.scala:184-197` any-one-of-allowance-list; NOT a trust anchor — §5.1 rests entirely on committee honesty |
| **Band-revert as *shipped default behavior*** | **⚠ DISABLED** | `band-density-reorg-enabled=false`; INV-LIVE-002 "restore density-revertable band" is code-complete but OFF → shipped default still k₁-absolute write-freeze |
| **Slashing consequence path** | **SPLIT** | invalid-state-proof tier BUILT; equivocation accumulator = shelf-ware (no consequence); VaR cap un-enforced |

---

## 5. Open questions for a deep-reasoning pass (grounded, NOT asserted as bugs)

These are the highest-leverage things for Fable — each is anchored to real code/research, and
each is a *hypothesis to prove or break*, not a known defect:

1. **Adopt ≡ re-exec byte-equivalence under a Byzantine ml0.** #18 made byte-diff-adopt primary
   on the premise that `reExecDerivationWithDiff` over the pinned `executionBaseOrdinal` reproduces
   the attested root. Verified in unit (`ShardCommitteeReExecutionSuite`), **not** on a cluster.
   *Is there any ml0-controlled input where the byte-diff and the honest re-exec disagree, and the
   adopter cannot detect it?* Anchor: `ShardCheckpointWiring.scala:210`,
   `GlobalSnapshotAcceptanceManager.scala:1006`.

2. **GAP-1 field-presence gate soundness.** 3a gates each per-field check on
   `stateProof.<field>Proof.isDefined` to distinguish genuine `None` from `Some(empty)`
   (`reconstructInfoFromDiff` always yields `Some(empty)`). *Is there any field where a genuinely
   non-empty state produces no proof, or a genuinely empty state produces one — collapsing the
   distinction?* Anchor: `dc0dbe1c7`, `MultiBranchAdoptSuite`.

3. **`densityCompare` commutativity — proof by construction, unverified adversarially.** S3
   claims `compare(A,B) == flip(compare(B,A))` via true-MRCA walk (decided by ordinal, not arg
   position) + symmetric MRCA-slot anchor + deterministic tiebreak. *Construct an input where the
   MRCA walk or the tiebreak is asymmetric.* Anchor: `ChainSelection.scala`, `14fb8b32e`.

4. **Pseudo-predictability in `standardCompare`.** The short-fork path uses `ordinal → slot → VRF`;
   any fork-choice tiebreaker deterministic from local state is pre-computable/grindable in
   Taktikos. The VRF step only fires on an ordinal+slot tie (rare) — but *is "rare" the same as
   "safe"?* Anchor: `taktikos` skill §4, `ChainSelection.scala` `standardCompare`.

5. **Set-valued P-window ⊇ U-window invariant on ALL paths.** 1a reconstructs `P` from the
   committed chain. *Does `P ⊇ U` hold across restart, deep catch-up, and peer-sync-ahead paths,
   not just the happy path?* Anchor: `8aa1de3dd`, `CurrencySnapshotProcessedSetSuite`.

6. **k₂ deep-revert determinism.** S4's deep path does `deleteAbove(fork) + readState(fork) +
   re-fold`. *Does the re-fold reproduce byte-identical state across every node, or can disk
   retention differences (S2's bounded RAM journal vs disk) make the revert non-deterministic?*
   Anchor: `MptOverlay.scala:1047` (deep `revertToOrdinal`), `2191510a6`.

7. **Cross-shard CQ-collapse bound vs committee-size decoupling.** Prior result: honest-majority
   breaks when `α_total > 1/(2S)` (`project_cross_shard_cq_collapse_bound`). *Does the
   committee-size decoupling in the economic-trust design honor this bound at the chosen `K_S`?*
   Anchor: `ECONOMIC-TRUST-SHARDED-SECURITY-ARCHITECTURE.md`.

---

## 6. Already adversarially verified THIS session — do NOT re-derive

To spend a reviewer's budget on the frontier (§5), not re-litigation, note these were settled
this session via 3 adversarial Workflows + 1 gate-builder (run IDs in
`docs/nakamoto/TRACK1-TRACK3-IMPLEMENTATION-SPEC.md`):

- **1a** scalar interval derivation was **REFUTED** (permanently drops cross-shard spends when
  `globalSyncView` runs ahead of local head) → fixed with set-valued in-band P.
- **2a** reader existing only on the gl0 rail → build on **all 3** paths (settled user decision).
- **3a** delete-override **fail-closes 100% of honest sharded MGs** on `None`-vs-`Some(empty)` →
  gate on `stateProof.<field>Proof.isDefined` (the §3 correction is about *how* it landed).
- **Track-3 audit was STALE**: `k₂ = 100·k₁` is live (mainnet 102 400, **not** 2550);
  RebootstrapOrchestrator default was **ON** (docs wrongly said OFF); density was **never wired**
  (`ChainSelection.make` ran hardcoded 50/200). The reverted `86f390130` k2-freeze was inert +
  self-contradicting. The real 2026-06 fork storm root was **non-deterministic `smtRoot`**, fixed
  by `376d09fbc` (re-confirm live before enabling deep reorgs — it is a Track-3 precondition).

Settled user decisions (do not re-litigate): 3-path reader; optimistic-finalize + disk-backed
k₂ retention; role-split adoption; disk-backed k₂ revert (Cardano UTxO-HD analog); `k₂ = 100·k₁`
intended.

---

## 7. Consensus state-machine map (read control flow here, not summaries)

- **Fork choice / chain selection:** `modules/node-shared/.../domain/nakamoto/ChainSelection.scala`
  — `standardCompare` (short-fork, ordinal→slot→VRF), `densityCompare` (band, S3).
- **Depth-k finality + leader loop:** `modules/dag-l0/.../snapshot/nakamoto/SnapshotLeaderLoop.scala`
  (`ConfirmationDepthK` = k₁, per-env: mainnet 1024) and `NakamotoSyncDaemon.scala` (attestation, unconditional).
- **Attestation-2/3 tracking:** `modules/node-shared/.../domain/nakamoto/TipTracker.scala`
  (`FinalityThreshold = 2/3`). Attestation-finality and depth-k both run; first wins.
- **Chain store / floor gate:** `modules/dag-l0/.../snapshot/nakamoto/NakamotoChainStore.scala`
  (store-gate floor: k₁ when flag off, k₂ when `band-density-reorg-enabled`).
- **Settled marker:** `modules/node-shared/.../domain/nakamoto/SettledOrdinalTracker.scala`
  (k₂ monotone view over `nakamotoSettledOrdinalRef`).
- **Sharded adopt / re-exec:** `GlobalSnapshotAcceptanceManager.scala` (`deriveAdoptedCurrencyState`,
  adopter), `ShardCheckpointWiring.scala` (`reExecDerivationWithDiff`, committee/watchtower),
  `SharedServices.scala` (createContext wiring), `PinnedCurrencyInfoReader.scala` (by-ordinal
  pinned reads).
- **State overlay / revert:** `modules/node-shared/.../domain/nakamoto/overlay/MptOverlay.scala`
  (`revertToOrdinal`: trait:304, shallow:486, deep:1047).
- **Checkpoint schema:** `modules/shared/.../schema/sharding/ShardCheckpoint.scala`
  (`ShardCheckpointSigPreimageV2` carries `executionBaseOrdinal`), proto `sidecar.proto`
  (`diff_base_ordinal = 11`).
- **LDD eligibility:** `modules/shared/.../schema/nakamoto/ldd.scala` (ψ=1, **γ=16 live** / 15 in stale docs, fA=½, fB=1/20),
  `modules/node-shared/.../domain/nakamoto/EligibilityChecker.scala`.

---

## 8. Evidence Appendix (commands run 2026-07-07)

- `git rev-parse HEAD` → `21933559c…`; branch `feature/committee-state-diff`.
- `git diff --stat 2447aac0c^..HEAD` → `117 files changed, 5250 insertions(+), 2480 deletions(-)`;
  94 `.scala`, 14 `.md`.
- No local `main`; remotes: `james` (scasplte2, canonical), `otto` (ottobot-ai, fork),
  `upstream` (Constellation-Labs). Branch **not pushed** (no matching remote branch).
- `band-density-reorg-enabled = false` at `application.conf:391` (+ `${?NAKAMOTO_BAND_DENSITY_REORG_ENABLED}`).
- `keepDepthBehindFinalized = 100·confirmationDepthK` at `config/types.scala:165`.
- k₁ per-env block `mainnet:1024 / testnet:256 / integrationnet:256 / dev:32` at
  `application.conf:308-314` (NOT 255 — §3.1); resolver `types.scala:153`. Sims justify k≥~271 for
  strict 10⁻¹²; prod runs the more-conservative power-of-two 1024.
- `diff_base_ordinal = 11` in `sidecar.proto`; `ShardCheckpointSigPreimageV2` carries
  `executionBaseOrdinal` (`ShardCheckpoint.scala:135`).
- `deriveAdoptedCurrencyState` **defined** at `GlobalSnapshotAcceptanceManager.scala:1006`;
  `reExecDerivationWithDiff` **defined** at `ShardCheckpointWiring.scala:210` (both survive — §3).
- Working tree clean except 2 untracked: `SESSION-HANDOFF-2026-06-29.md`, `diagrams/archive/`.

---

*Maintainer note:* update §4 (the board) and §5 (open questions) after every substantive change,
same turn as the code. This file is the thing that survives context loss.
