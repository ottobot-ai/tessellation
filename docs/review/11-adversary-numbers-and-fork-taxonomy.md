# 11 — Measured Adversary Numbers & Known-Fork Taxonomy

> **HISTORICAL AUDIT INPUT, NOT THE ACTIVE SECURITY ARGUMENT.** The measured
> failures remain evidence, but later universal-replay, `numShards=1`, finality,
> P3, authoritative-field, and parameter conclusions may be superseded. The active
> lifecycle/roadmap requires re-deriving Taktikos/LDD + Avalanche + execution-shard
> bounds and treats one shard as the same replay-sign-diff-watchtower protocol, not
> a safe universal-replay bypass.

> **Purpose.** Companion reference for the Fable review (`FABLE-REVIEW-PROMPT.md`, `HANDOFF.md`).
> Part A gives the *measured* adversary numbers so parameter claims are grounded, not asserted.
> Part B is a taxonomy of how this system has *actually* forked — so the adversarial search in
> HANDOFF §5 targets real fault lines instead of hypothetical ones. Every number cites its
> sim/skill/doc source **with its caveat**; every commit hash was verified with `git log`/`git show`.
> **Where a memory/skill claim conflicts with git or current source, git/source wins and the
> conflict is flagged.** Consensus substrate is **Ouroboros Taktikos, not Praos** — Praos
> common-prefix formulas do not transfer.

---

## Provenance & cross-check status

- **`taktikos` skill** (loaded this session) — supplies the k-sweep and Δ-cliff cheat-sheets and the
  pseudo-predictability note. Reproduced below verbatim.
- **`~/repos/research-nipopos-2026`** — **REACHABLE.** Currently checked out on
  `sim/mithril-quorum-threshold`; the adversary sims live on branch **`sim/adv-7block-private`**,
  read here via `git show sim/adv-7block-private:sims/…` (no checkout, working tree undisturbed).
  `FINDINGS_adv_depth_expanded.md` was read from the working tree. Files confirmed present:
  `sims/FINDINGS_adv_7block_private.md`, `sims/FINDINGS_adv_depth_expanded.md`,
  `sims/adv_depth_optimization.py`, `sims/adv_depth_expanded_parallel.py`, `sims/adv_delay_sweep.py`,
  `sims/adv_7block_private.py`, `docs/TAKTIKOS-NOTES.md`. **The skill tables and the source sim
  findings agree** (skill's 0.91% @ k=31 = the 30k-trial run; the 10M run reports 0.948%, the small
  offset being parallel RNG streams, not bias — stated in the findings).
- **Config ground-truth** was re-read from current HEAD source (`application.conf`,
  `config/types.scala`, `SnapshotLeaderLoop.scala`) — see the drift note in A.0.

---

# PART A — MEASURED ADVERSARY NUMBERS

## A.0 Config ground-truth — and a stale-number correction

The finality-depth and LDD numbers the sims were built around have **drifted** from what current
HEAD ships. State the current values; do not quote the old ones.

| Param | Sim / skill / task-context value | **Current HEAD source value** | Source |
|---|---|---|---|
| k₁ confirmation depth | "255 default" (skill + this task's context line) | **mainnet 1024, testnet/integrationnet 256, dev 32** (env-overridable dev only); source fallback `DefaultConfirmationDepthK = 32` | `application.conf:308-314`; `config/types.scala:154,192` |
| R (active-slot window) | round(3.1·k₁) | round(3.1·k₁) — **confirmed** | `config/types.scala:160` |
| k₂ archival/deep-revert depth | 100·k₁ | **100·k₁ — confirmed** | `config/types.scala:166` |
| LDD ψ (offset) | 1 | 1 | `application.conf:332` |
| LDD γ (cutoff) | **15** (paper + all sims) | **16** ("gamma=16 ⇒ ~7 s median block") | `application.conf:330` |
| LDD fA (amplitude) | 1/2 | 1/2 (exact `Ratio`, parsed from `"1/2"`) | `application.conf:336` |
| LDD fB (baseline) | 1/20 = 0.05 | 1/20 (exact `Ratio`, parsed from `"1/20"`) | `application.conf:334` |

> **DRIFT FLAGS for Fable (spec-vs-code):**
> 1. **k₁ = 255 is stale.** The skill and this task's context both say "default 255"; the
>    `SnapshotLeaderLoop.scala:381` hardcoded-255 the skill cites **no longer exists** — k₁ is now
>    threaded per-env from HOCON `nakamoto.confirmation-depth-k`. Mainnet ships **1024**, which is
>    *far* beyond the ~290 the sims put at 10⁻¹² (≈ 4×10⁻¹⁵ extrapolated) — extremely conservative,
>    not a risk. Dev/tests run k₁=32 (≈ 0.9% per-attempt race — the sims' k=31 operating point).
> 2. **γ drifted 15 → 16.** Every sim below uses γ=15; production runs γ=16. Effect is small
>    (slightly longer ramp, ~7 s median block either way) but the tables are *not* re-measured at
>    γ=16 — treat the risk numbers as γ=15 and directionally valid, not exact for the shipped curve.
> 3. `LddConfig.Default` source-default was **removed 2026-06-30**; HOCON is now authoritative
>    (`application.conf:325`). fractions are exact `Ratio`, never `Double` (0.05-as-Double round-trips
>    to a garbage denominator and forks the cluster).

## A.1 Depth-vs-reorg-risk (k sweep)

Per-attempt reorg risk, **1/3 adversary, LDD ψ=1 γ=15 fA=0.5 fB=0.05, 7 s blocks (1 slot = 1 s),
no delay.** "Per-attempt" = one Nakamoto-style race `risk(k)=P(T_adv(k) ≤ T_hon(k))`; multiply by
attempts/window for an operational rate. Worst case = a *true* single 1/3-stake adversary with a
perfectly-coordinated private chain (pessimistic for realistic threat models).

**Skill cheat-sheet (built from `adv_depth_optimization.py`, k≤80, direct MC):**

| k | risk | 1 in N | Wall-clock @ 7 s | @ 29 s snapshot cadence |
|---|------|--------|------------------|-------------------------|
| 6 | 13.91% | 7 | 42 s | 2.9 min |
| 15 | 4.77% | 21 | 1.8 min | 7.3 min |
| 25 | 1.69% | 59 | 2.9 min | 12 min |
| 31 | 0.91% | 110 | 3.6 min | 15 min |
| 40 | 0.38% | 260 | 4.7 min | 19 min |
| 55 | 0.10% | 643 | 6.4 min | 27 min |
| 63 | ~0.05% (interp) | ~2000 | 7.4 min | 30 min |
| 79 | 0.01% | 10,000 | 9.2 min | 38 min |
| **255** | **extrapolated ~3·10⁻¹¹** | **~3·10¹⁰** | ~30 min | ~2 hr |
| ~271–290 (Cardano-equiv 10⁻¹²) | extrapolated | ∼10¹² | ~32–34 min | ~2.2 hr |

**Expanded 10M-trial run (`adv_depth_expanded_parallel.py`, k≤400, `FINDINGS_adv_depth_expanded.md`,
2026-04-22)** — anchors the extrapolation:

| k | wins/10M | risk | status |
|---|---|---|---|
| 31 | 94,746 | 9.475e-03 | measured (reproduces the 0.91% cheat-sheet row within RNG noise) |
| 55 | 9,340 | 9.340e-04 | measured |
| 79 | 961 | 9.610e-05 | measured |
| 100 | 141 | 1.410e-05 | measured |
| 120 | 22 | 2.200e-06 | low-count |
| 134 | 5 | 5.000e-07 | low-count (edge of measurable) |
| 150–400 | 0 | < 3.69e-7 | **sample floor** (Rule-of-3.7 upper bound; cannot distinguish from 0) |

- **Measured regime ends at k≈135.** Everything at/above k=150 (**including k=255**) is the
  3.69e-7 sample floor — the ~3·10⁻¹¹ figure for k=255 is an **EXTRAPOLATION, not a measurement**.
- Exponential-tail fit: **slope ≈ −0.040 ± 0.001 log₁₀(risk)/block** (≈ 1 decade per 24.5 blocks),
  extremely stable across fit windows [40,134]…[70,134].
- **k for strict 10⁻¹² (Cardano-equivalent CP-violation):** point estimate **k ≈ 277**;
  conservative-upper **≈ 284** (shallowest observed slope); **headroom-safe recommendation ≈ 290**.
  The pre-sim "k≈271" estimate is within 6 blocks of the point estimate. (Mainnet's 1024 is far
  past all of these.)
- **Why so much deeper than Praos:** the LDD **fB=0.05 flat tail** — once δ≥γ the adversary forges
  ~1.7% per slot *indefinitely* (`1-(1-0.05)^(1/3)`), so honest separation is only ~2× in median
  time-to-depth, not exponential. This is structural; it is why k lives in the hundreds.

## A.2 Delay cliff (Δ = network p99 propagation)

Measured at **fixed k=31** (`adv_delay_sweep.py`, 30k trials/Δ, `FINDINGS_adv_7block_private.md`):

| Δ | risk | note |
|---|------|------|
| 0–3 s | ~1% | baseline |
| 4 s | 1.5% | |
| **5 s** | **2.4%** | risk **doubles** (crosses 2%) |
| 6 s | 4.3% | |
| 7 s | ~5% (interp) | |
| **8 s** | **18%** | **cliff** — crosses 5% and 10% in one jump |
| 10 s | 60% | catastrophic (>50%) |
| 12 s+ | ~100% | |

- **Headroom budget at k=31: ~5 s p99 before risk doubles.** Crossings: 2% @ Δ≈5 s, 10% @ Δ≈8 s,
  catastrophic @ Δ≈10 s.
- The cliff sits where Δ approaches the honest inter-block gap (~9 s): honest blocks routinely fork
  each other, the honest chain halves its rate, the fB tail wins outright.
- **k does NOT buy delay resistance.** k buys risk-*floor* headroom; the cliff shape is structural
  to the fB=0.05 tail and applies at *any* k. During a network event where p99 degrades past ~7–8 s,
  even k=1024 is pushed into the cliff regime. Mitigations: pause finalization, dynamically escalate
  k, or accept the cluster is unhealthy. **This is a live safety lever for the Fable review** —
  no static k defends against a partial-synchrony blowout.
- **Caveat:** simplest possible delay model (a fork drops one of two colliding honest blocks; no
  multi-party gossip, no partial visibility, no asymmetric latency; adversary unaffected/private).

## A.3 LDD & pseudo-predictability (fork-choice)

- **LDD snowplow** `f(δ)`: dormant (δ<ψ, f=0) → ramp (ψ≤δ<γ, linear) → baseline (δ≥γ, f=fB).
  `threshold = 1−(1−f(δ))^α`; **low threshold = high difficulty** (inverse). With ψ=1 the slot
  immediately after a snapshot is always empty. **"Dormant" gates snapshot *proposal only*** —
  attestation (`NakamotoSyncDaemon.emitAttestation`) is unconditional, so a gap=0 node still
  validates/attests/gossips. Dormant ≠ offline.
- **maxvalid-tk fork choice:** longer chain wins; tie → **lower head slot** wins (proxy for having
  beaten a harder threshold). Taktikos-specific, **not** in Praos.
- **Pseudo-predictability:** VRF outputs are deterministic from `(sk, eta, slot)` — every staker
  pre-computes all future-slot VRFs. **Any fork-choice tiebreaker deterministic from local state is
  pre-computable and therefore grindable.** The Taktikos-pure rule is
  `length → head-slot → STALL and wait for future chain growth`, **not** `→ lowest VRF`.
- **Current exposure:** `ChainSelection.standardCompare` (short-fork path) uses
  `ordinal → slot → VRF`. The VRF step fires only when ordinal AND slot both tie (extremely rare),
  but it is a real pseudo-predictability leak. Flagged in `TAKTIKOS-NOTES.md:82`; HANDOFF §5.4 asks
  Fable to break it (selfish-mining / long-range under the fB=0.05 tail). Do **not** propose a
  VRF-nonce tiebreaker as a fix.

## A.4 Cross-shard CQ-collapse bound — `α_total > 1/(2S)`

**Derivation:** `docs/GKL-COMPOSITION.md` §5.2 (committed `45c97895`, "docs(gkl): analytical
composition of trigger stack + sharding", 2026-05-15). An adversary with global stake `α_total` can
**concentrate** all of it into one target shard. For `S` equal shards (`r_s_home = r/S`):

```
α_local = α_total / (r_s_home + α_total) = (α_total · S) / (1 + α_total · S)
α_local > 1/3   ⇔   α_total > 1/(2S)
```

| S | α_total threshold for α_local > 1/3 | α_local when α_total = 1/3 |
|---|---|---|
| 3 | 1/6 ≈ 0.167 | 1/2 (50% local) |
| 5 | 1/10 = 0.10 | 5/8 ≈ 0.625 |
| 10 | 1/20 = 0.05 | 10/13 ≈ 0.769 |

**What it means.** Naive sharding is **strictly more adversary-sensitive** than the single chain:
per-shard honest-majority breaks at a *global* stake fraction as low as 5% (S=10), well under the
33% single-chain bound. The targeted shard's per-shard CQ collapses even though `α_total ≤ 1/3`.

**Implication for committee-size / #shards.** Committee size must be **decoupled from S** and given
an *absolute* minimum backing (economic-trust decision 2026-06-28) so the per-shard floor does not
scale down with 1/(2S). Two lines of defence appear in the corpus:
1. **VRF-sortition of validators into shards + forced operator→shard assignment** — makes
   concentration structurally hard (memory: Option A chosen; empirical shard-0 collapse to 0.7%
   acceptance under α=0.33/S=4 in the `sim/integration` cross-shard sim).
2. **Watchtower fraud proof (economic-trust REV 2/3):** the argument that the α>1/(2S) *committee*
   floor **dissolves** if a single honest verifier + DA + challenge window can re-execute and slash —
   trust shifts from "≥⅔ committee honest" to "≥1 honest verifier". **This is contested and
   load-bearing** — the central economic-trust finding is that today the sharded path (numShards>1)
   is *"an optimistic rollup with the fraud proof amputated"* (committee is sole re-executor;
   followers verify diff-internal consistency, not that the root is the correct `accept()` output).
   **numShards=1 is the production default and is safe** (every gl0 re-executes). Fable should treat
   the fraud-proof-dissolves-the-bound claim as a claim to *break*, not a settled result
   (HANDOFF §5.7).

---

# PART B — KNOWN-FORK TAXONOMY

Eight real divergences this project hit. Each hash `git`-verified on `feature/committee-state-diff`.

### FORK-001: Non-deterministic `smtRoot` inside the consensus `===` (the storm)
- **Symptom:** honest gl0 nodes reject each other's freshly-won blocks; **97% (4094/4221) of
  ContentMismatch rejections carry the empty `diffs=[stateProof[]]` signature** (every enumerated
  field matches — only the un-printed smtRoot differs). Continuous fork storm: fork_count 15–26,
  up to 539 reorgs/run, finalized spread diverging. *The biggest one; a Track-3 precondition.*
- **Root cause:** `GlobalSnapshotConsensusFunctions:270` compares `recreatedArtifact === artifact`
  over the **full** stateProof *including* `smtRoot`. `attachSmtRoot` folds the locally-resolved
  `snapshot[N-k]` into a **path-/ancestor-resolvability-dependent** accumulating SMT (returns `None`
  when the ancestor is unresolvable), so honest nodes compute *different* smtRoots. A correct helper
  `StateProofComparison.equivalent` that excludes smtRoot already existed — consensus just didn't use
  it. **Pre-existing**, authored `27c99564c` (2026-05-29, "historical-commitment SMT anchored as
  smtRoot in gl0 state proof") — a month before the catch-up/k2 line of work.
- **Fix:** **`376d09fbc`** (2026-06-29) "kill fork storm at source — smtRoot-blind consensus compare
  + revert k2-freeze" (smtRoot-blind: copy stateProof with `smtRoot=None`, then `===`). Result:
  storm gone — **0 stateProof rejects** (was thousands by ord 126), 6/6 gl0 converged, fork_count 1.
- **Fault class:** **consensus-root impurity** — a path-dependent field inside the consensus
  equality. Violates the project rule *consensus root = pure fn of consensus-pinned state.*
- **Still-relevant risk:** **HIGH.** Sharding adds *more* path/observation-dependent roots (per-MG
  roots, PIN-1 component roots, the SMT/NIPoPoW tower, cross-shard receipts). The discipline "every
  non-consensus-pinned field leaves the root but stays stored" is the single most-recurring fix in
  this codebase (see FORK-004/003/005/007). HANDOFF §5.1/§5.2/§5.3 are all this class.
- *Provenance:* `docs/nakamoto/FORENSIC-AUDIT-CHAIN-SELECTION-2026-06-26.md` (13-agent audit, D5).

### FORK-002: Backfill BARE-vs-WRAPPED parse-format mismatch
- **Symptom:** a fallen-behind gl0 backfilling *evicted* history floods "could not parse snapshot"
  (659–1805/node); the walk-back cursor is **not advanced on `None`** → stalls forever; 3-of-6 gl0
  wedge → **3-3 finalized split** (spread 16→26, hashes diverge) → committee can't reach kQuorum=4
  → committee-gate timeout → e2e bringup aborts.
- **Root cause:** `ChainSyncServer` serves *in-memory* snapshots as the `{"snapshot","context"}`
  envelope but *disk/evicted* snapshots as the **bare** `Signed[GlobalIncrementalSnapshot]`;
  `BackfillDaemon.parseSnapshotWithContext` accepted **only** the envelope. Latent since April
  (`c71490634`); bites only when a node falls behind past k₁-bounded retention into evicted history.
  Plus a Latin-1 decode bug (`map(_.toChar)` corrupts bytes ≥0x80; serve side encodes UTF-8).
- **Fix:** **`f06287e7e`** (2026-06-19) "backfill parser tolerates bare disk-served snapshots"
  (envelope `.orElse` bare; UTF-8 decode). Validated: sails past the ord-198 eta rotation,
  spread=0, parseFail=0.
- **Fault class:** **parse/serialization format mismatch** (polymorphic serve encoding vs
  single-form parser) compounded by a **non-advancing-cursor livelock**.
- **Still-relevant risk:** **MEDIUM–HIGH.** Sharding multiplies serve/adopt byte formats (byte-diff,
  authoritative-push fields, MPT entries, inclusion proofs). Any serve path with two encodings and
  one parser recurs; the "don't advance the cursor on decode-None" trap is the livelock cousin of
  FORK-008.
- **Retirement (2026-07-13):** this incident remains provenance, but the undeployed
  `BackfillDaemon`/ordinal-range protocol was later removed because it lacked KES and complete
  registered KES/VRF/eta authority evidence. Hash-keyed recovery now re-enters the normal validator.

### FORK-003: Allow-spend epoch / currency-fold wedge (the canonical cross-shard violation)
- **Symptom:** gl0 (numShards=1 re-exec path) re-derives `activeAllowSpends = empty` ≠ the
  ml0-signed `Some(List(...))` → `CurrencySnapshotContextFunctions CannotCreateContext:
  SnapshotDifferentThanExpected` at m0 currency ord 450 → m0 fold frozen at 449 → spend actions
  never reach global `spendActions{}` → `DoubleUseAllowSpend` e2e fails. Earlier variant: a
  metagraph-scoped allow-spend is present for **exactly one ordinal, then vanishes** (ord 779 all 5
  nodes retain, ord 780 gl0-2/3 retain but gl0-0/1/4 drop — *same pass-through input, different
  output = non-determinism*, and the drop-proposal wins consensus).
- **Root cause:** metagraph-scoped economic state expired against **divergent epochs** — gl0 expires
  allow-spends on the **LIVE global epoch** (`AllowSpendStateManager.scala:167/203`,
  `filter(_.lastValidEpochProgress >= epochProgress)`) while ml0 uses its **PINNED
  `globalSyncView` epoch** (`CurrencySnapshotAcceptanceManager.scala:390`) → they disagree at the
  boundary. The general form: a metagraph reading/expiring economic state against *unpinned* live
  global state instead of its finalized `globalSyncView` anchor.
- **Fix:** T2 **R1-fix** — scope-aware allow-spend expiry on the metagraph's pinned `globalSyncView`
  epoch — landed in the sharded-security substrate **`8ac7ce04f`** (2026-06-29); **`d06e6fb78`**
  (2026-07-01) "I-PIN read-at-anchor" makes `accept()` read the pinned global state on the re-exec
  path. (The earlier authoritative-push `8b0743296` was a consistency band-aid; economic-trust REV 3
  reframes the correct fix as *pinned reads + re-exec*, not trust-the-metagraph.)
- **Fault class:** **consensus-root impurity via unpinned cross-shard/global-state read** — an
  observation-dependent input to a consensus fold. **THE canonical cross-shard determinism
  violation** (memory `project_economic_trust_architecture`).
- **Still-relevant risk:** **CRITICAL — this is the sharding endgame's central hazard.** Every
  cross-shard read must be pinned to a finalized anchor + proof-carrying (Polkadot PoV / NEAR
  receipts / stateless witnesses), or the fold is non-deterministic. Exactly HANDOFF §5.1
  (adopt≡re-exec byte-equivalence under a Byzantine ml0).

### FORK-004: `globalSnapshotSyncView` (field 32) consensus-root regression
- **Symptom:** ml0/cl1 resync `recomputed ≠ signed` (deterministic per ordinal); shard checkpoints
  `signers=1` (committee members compute different per-MG roots → no aggregation); shard buffer
  climb → data-with-fee fork.
- **Root cause, first failure:** `globalSnapshotSyncView`
  (`MgGlobalSnapshotSyncView`, fieldId 32) is observation-dependent. The ML0 producer validates it
  under its full facilitator population, while replay currently derives that population from the
  artifact proof subset (`CurrencySnapshotValidator.scala:56-70`;
  `GlobalSnapshotSyncValidator.scala:65-67`). Including the resulting per-peer map in the GL0/per-MG
  root therefore made honest roots diverge. Isolation revert **`9b416f736`** (2026-06-16) restored
  that old root inclusion and reproduced the live fork.
- **Historical containment:** **`dc2790dea`** + **`ffd5b3754`** (2026-06-17) excluded field 32 from
  both per-MG `infoSubFields` and global `consensusRootEntries`. That stopped the immediate root
  inclusion regression. *(The same-content hashes cited elsewhere as `f46bc7666`/`df19cef76` are
  cherry-picks; the on-branch hashes are authoritative.)*
- **Root cause, remaining failure (ECO-F32):** exclusion did **not** make the field dispensable.
  GL0 still writes, removes, and reconstructs it (`GlobalStateConverter.scala:1362-1393,1691-1725,1735-1803`),
  and shard execution seeds replay from that reconstructed prior (`ShardCheckpointWiring.scala:263-314`).
  A locally staged base retains a nonempty view, while root-verified peer backfill strips field 32
  (`PinnedCurrencyInfoReader.scala:341-380`) and reconstruction turns absence into `Some(empty)`.
  The incremental carries only `CurrencySnapshotStateProof.globalSnapshotSync` plus accepted
  `globalSnapshotSyncs`, not the full view preimage (`currency.scala:52-61,222-240`). Two nodes can
  therefore authenticate the same GL0 root and derive different replay proofs/signing outcomes.
- **Fault class:** **root-invisible consensus replay input** plus the historical
  **consensus-root impurity/regression-via-revert**. "Leave the root but stay stored" is not a safe
  invariant when replay consumes the stored value.
- **Status/fix:** **HIGH, CONFIRMED, OPEN.** First carry a signed/root-bound exact optional full-view
  replay witness at every required checkpoint-window boundary, bind the explicit ML0 operator
  population, preserve `None` versus `Some(empty)`, and verify the preimage against
  `CurrencySnapshotStateProof.globalSnapshotSync`. Missing material defers and cannot slash. Only
  then remove field 32 from every GL0 MPT/diff/load/reorg path while retaining it in ML0
  `CurrencySnapshotInfo`.

### FORK-005: data-with-fee — gl0 can't re-derive metagraph balances
- **Symptom:** a data-application fee credited on ml0 (`/currency/.../balance = 100`) **never
  mirrors to gl0** (`= 0`); `feeTxInGl0` 1800 s timeout; per-MG root MISMATCH `attested ≠ recomputed`
  → DROP → mg currency mirror frozen. (First mis-attributed to shard imbalance — both mgs hashing to
  the same shard → committee-gate timeout → orphan/shard-buffer fill; **REFUTED**, imbalance ≠ cause:
  mg0 mirrored fine-except-fee even on the overloaded shard.)
- **Root cause:** gl0's sharded `AdoptFromSignedFields` path re-derives balances but **cannot
  reproduce data-application-internal effects** (`info.balanceAdjustFunction` — the metagraph's
  custom `DataApplicationL0Service` code gl0 does not run) **+ cross-shard spends**
  (`updateCurrencyBalancesBySpendTransactions`, global-sourced) → `derived ≠ committed` → the
  per-field gate carries forward each node's **own** prior `lastState.balances` → `MgBalances`
  becomes a function of node history, not consensus-pinned. Replay fixes (`ae18839bc` fee,
  `671c434b1` locks) were **INERT** — the producer's own carry-forward emits an empty balance diff.
- **Fix:** **`95a19a35c`** (2026-06-22) — ml0 pushes `authoritativeBalances` on the signed
  incremental; gl0 verifies vs the metagraph-signed `balancesProof` (`= balances.hash`,
  selector-independent — reproducible byte-exact) + adopts, **fail-closed** drop on mismatch, no
  re-derive. Extended: **`8b0743296`** (active-allow-spends/active-token-locks), **`a564d8c6e`**
  (base-independent per-MG root: override reconstructed values with the authoritative ones), then
  **`073823862`** + **`3b8845d7c`** (the 5 cumulative ref-maps). Full 2mg/2shard e2e GREEN; fee
  mirrors in 44 s (was 1800 s timeout).
- **Fault class:** **consensus-root impurity via non-reproducible derivation** — gl0 cannot
  re-execute metagraph data-app / cross-shard logic, so the re-derive gate diverges. Fix restores
  mainnet's "trust-the-metagraph" invariant *for the sharded path*.
- **Still-relevant risk:** **CRITICAL.** This is precisely the *optimistic-rollup-with-amputated-
  fraud-proof* concern (economic-trust central finding). Every field gl0 can't re-derive is either
  authoritatively trusted (**unsafe** without a fraud proof) or must be provably re-executed by
  committee **+ watchtowers**. The authoritative-push that fixed the fork is itself flagged as a
  trust hole the watchtower re-exec must close (`8ac7ce04f`/`68246cffe`).

### FORK-006: the reverted `86f390130` k2-freeze (inert + self-contradicting)
- **Symptom:** intended to *allow* `k1<depth<k2` density reorgs by moving the write-freeze from k1
  to k2. Actual effect: **no persistence floor armed anywhere** — `archive()` fired **0×** on all 6
  nodes (dev k2=3200 unreachable at run tips ~650–778), `nakamotoArchivedOrdinalRef` stayed 0, the
  store gate inert (`REFUSED store=0` — a *vacuous* "0", the gate was never armed); **half-removed
  cement** — `store()` accepts a write that `shouldSwitch` then refuses to act on; served
  `finalized_ordinal` (gates CL0 SC-binary pruning) advanced on soft k1 confirm while the tip
  reorged **below** it. Amplified the FORK-001 storm's blast radius.
- **Root cause:** split finality into k1 (confirm) + k2 (archive = 100·k1) and moved
  `NakamotoChainStore.store()`'s write-freeze from the **reachable** k1 marker to the **unreachable**
  k2 marker — **while leaving `ChainSelection.shouldSwitch`'s `!currentIsFinalized` guard keyed to
  k1** (git-confirmed untouched across `86f390130^..HEAD`).
- **Fix:** **reverted inside `376d09fbc`** (2026-06-29, `git revert -n 86f390130`) — store freeze
  back on the k1 `nakamotoFinalizedOrdinalRef`; `archive()`/k2 gone; `ArchivalDepthK` migrated off
  `sys.env` to `100·confirmationDepthK`.
- **Fault class:** **inert / self-contradicting mechanism** — a "fix" whose gate is never armed in
  any realistic run and which contradicts an untouched sibling guard. A **process fault** as much as
  a code fault: a two-marker finality change shipped without a run that reaches the second marker.
- **Still-relevant risk:** **MEDIUM (recurs as a process risk).** Track-3 re-introduces a **k₁/k₂
  finality band** with a k₂ deep-revert; HANDOFF §5.6 flags k₂-deep-revert determinism (different
  RAM-journal/disk retention across nodes) as open. Lesson: never ship a finality-marker split
  without a test that actually reaches the deeper marker — and keep `shouldSwitch` aligned to
  whichever marker the freeze uses.

### FORK-007: allowSpends/tokenLocks dropped from the per-MG root (run-26 adopt-gate)
- **Symptom:** allow-spends phase fails forever ("No active allow spends found … in Global L0", 600
  attempts → exit 1); gl0's `lastCurrencySnapshots[mg]` has correct `balances` but
  `activeAllowSpends = 0` / `activeTokenLocks = 0` even *inside* the active window.
- **Root cause:** the `AdoptFromSignedFields` per-field gate commits gl0's **DERIVED** field only if
  `derivedProof.<f> === committedProof.<f>`, else carries the prior forward. gl0 re-derives **empty**
  `activeAllowSpends` (hash of the empty map, same value as empty token-locks) ≠ committed non-empty
  → falls back to the empty genesis prior; because `activeAllowSpends = prior |+| incoming`
  **accumulates**, once empty at the creation ordinal it stays empty forever. Sub-cause: **expiry
  over-prune** — gl0 expires with `syncEpoch = globalSyncView.epochProgress` while ml0 uses
  `lastGlobalSnapshotEpochProgress`; these diverge under the syncOffset=2 reorg-safety backstep.
  Structural: metagraph allow-spends live in the *global* fieldId-7 `ActiveAllowSpends` partition
  with `Some(mgAddr)` scope, **excluded from `infoSubFields`** → not consensus-pinned per-MG.
- **Fix:** converged into the authoritative-push family — **`8b0743296`** (2026-06-22, "gl0 adopts
  ml0's authoritative active-allow-spends + active-token-locks", extends `95a19a35c`) — same
  treatment as data-with-fee balances; the pinned-epoch expiry (R1, `8ac7ce04f`) closes the
  over-prune half. *(Note the memory conflict resolved by git: an early note said token-locks were
  "not pinned"; token-lock ref-fields `MgActiveTokenLocks`/`MgLastTokenLockRefs` **are** in
  `infoSubFields` — the load-bearing gap was the active-set re-derive + the expiry-epoch divergence,
  not the token-lock partition.)*
- **Fault class:** **consensus-root impurity via non-reproducible retention** + **partition-scope
  mismatch** (economic sub-state not covered by the per-MG root) + **epoch-pinning divergence**.
  Same family as FORK-003 / FORK-005.
- **Still-relevant risk:** **HIGH.** Same core as the cross-shard violation; any economic sub-state
  that is either outside the per-MG root or re-derived non-deterministically will recur at
  numShards>1 (and the cross-shard consume path adds a double-spend surface, I-ONCE).

### FORK-008: gl0 deep-catch-up livelock (D1/D2/D4/D6)
- **Symptom:** a >k-behind gl0 node's HEAD advances (via catch-up adopt) but FINALIZED stays pinned;
  Tier-3 catch-up **re-fires every gossip wave** — gl0-4 looped **342×/57 min frozen at 409**;
  gl0-3 logged 14 byte-faithful adopts of the **same ord=775** in 4 min (pure re-teleport); split
  cluster (nodes at 1081/1091/802/839/503).
- **Root cause (multiple):** **D1** stale-adopt-below-tip livelock — `byteFaithfulAdopt` stores a
  pulled ordinal ≤ bestTip as an **orphan** (ancestors not linked until the async `BackfillDaemon`
  connects them), so chain-selection keeps reporting the *old* connected tip; the Tier-3 gap-check
  read `bestTipOrdinal`, which never advances → re-teleport forever. **D2** `pullLatestMptEntries
  FromPeer` takes the **first responsive** peer, not the freshest (pulled ord 375 while peers held
  674–752). **D4** no monotonic-advance guard in `byteFaithfulAdopt` (can move the canonical tip
  backward). **D6** gl0-5 post-catch-up finalize wedge (adopt leaves `chainStore` without contiguous
  ancestors → depth-finalize walks to a `None` → OVERLAY-PRUNE drops finalized → permanent stall).
  The `ContentMismatch` handler had the same shape (re-adopt every forward gossip wave). *Per the
  forensic audit, D1/D2/D4/D6 were introduced or materially worsened by this line of work.*
- **Fix:** **`e310f6ccd`** (byte-faithful adopt — recompute `sidecarFreeMptRoot === signed mptRoot`
  *before* writing; `loadBytes` verbatim only on match), **`24e93ee10`** (Tier-3: add
  `lastCatchUpAdoptedOrdinal` max-monotone; gap-check against
  `max(bestTipOrdinal, lastCatchUpAdoptedOrdinal)`), **`61ede3a0e`** (ContentMismatch: gate full
  resync on `gap > confirmationDepthK`, not `>= CatchUpThreshold`; small-gap mismatch → normal fork
  → `storeForkBranch`). All 2026-06-25. Clean deep catch-up after: 1 Tier-3 / 1 adopt / 0 re-adopt.
- **Fault class:** **catch-up / restart livelock** — orphan-adopt freezes the gap-reference metric →
  re-teleport; monotonic-only guard against a stale adopt; first-not-freshest peer pick.
- **Retirement (2026-07-13):** the referenced `BackfillDaemon` no longer exists. Missing ancestry is
  fetched by hash and each child re-enters the normal parent-first validator; this paragraph remains
  the historical mechanism and incident record.
- **Still-relevant risk:** **HIGH.** Catch-up/restart paths are exactly where HANDOFF §5.5
  (set-valued P-window ⊇ U-window on *every* restart/catch-up path) and §5.6 (k₂ deep-revert
  determinism) live. Sharding adds per-shard mirror catch-up. Any adopt path that stores orphans
  without advancing the gap-reference metric, or picks a non-freshest peer on the consensus path
  (non-deterministic mptRoot input → fork), recurs.

---

## Fault-class heat map — ranked by likelihood to bite the multi-shard / multi-metagraph endgame

Ranked most→least likely to recur, with reasoning. **The single dominant class is consensus-root
impurity: 5 of the 8 forks are instances of it.**

| Rank | Fault class | Exemplar forks | Why it recurs in sharding | Severity if it recurs |
|---|---|---|---|---|
| **1** | **Consensus-root impurity / non-reproducible derivation** — a path-dependent or observation-dependent field inside the consensus `===`, OR a field gl0 cannot re-execute | **001 (smtRoot), 004 (syncView f32), 005 (data-with-fee), 007 (allowSpends), 003 (allow-spend epoch)** | Sharding adds new roots faster than any other change: per-MG roots, PIN-1 component roots, SMT/NIPoPoW tower, cross-shard receipts, authoritative-push fields. Each is a fresh chance to fold a non-consensus-pinned input. Directly = HANDOFF §5.1/§5.2/§5.3. | **Fork storm / silent mirror freeze.** Blast radius is the whole cluster. |
| **2** | **Cross-shard / unpinned global-state read** (a load-bearing sub-species of #1; the α>1/(2S) territory) | **003 (canonical), 005 (cross-shard spends), 007** | Goes **live instantly** at the numShards>1 cutover (cross-shard consume, I-ONCE double-spend surface). The economic-trust finding: sharded path = "optimistic rollup with the fraud proof amputated." Pinned reads + proof-carrying admission are designed but not fully landed. | **Fund loss / double-spend / undetectable Byzantine-committee forgery.** |
| **3** | **Catch-up / restart livelock & retention** (adopt paths, orphan stores, non-freshest peer, finality-marker retention) | **008 (deep-catch-up), 002 (backfill), 006 (k2 retention)** | Every restart, deep-fork, or lagging-node path must reconstruct state; sharding adds per-shard mirror catch-up and Track-3's k₂ deep-revert band. HANDOFF §5.5/§5.6 are open here. | **Node livelock / liveness stall / minority self-lockout** (usually not a global safety break, but hangs the cluster). |
| 4 | **Parse / serialization format mismatch** | 002 | Sharding multiplies serve/adopt byte formats (byte-diff, MPT entries, proofs, authoritative-push). | Localized wedge; usually caught fast at boot/e2e. |
| 5 | **Process / discipline faults** — inert-mechanism-shipped-untested (006), regression-via-isolation-revert (004) | 006, 004 | Not a code category but a *review* category: gates flipped without a written reachability criterion; reverts that silently un-fix determinism. HANDOFF §5 (workflows) asks Fable to hunt exactly these. | Re-opens an already-fixed fork; erodes trust in "validated." |

**One-line takeaway for the adversarial search:** if you have budget for one hypothesis, assume a
**non-consensus-pinned or non-re-executable field has leaked into a root** (class 1/2). That is where
five of eight real forks came from, and sharding manufactures new roots faster than anything else in
the codebase.
