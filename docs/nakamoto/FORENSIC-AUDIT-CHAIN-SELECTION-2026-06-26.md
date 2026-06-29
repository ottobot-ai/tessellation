# Forensic Audit: `feature/committee-state-diff` Chain-Selection / Catch-Up / k2-Freeze Line of Work

*Adversarial multi-agent forensic audit, 2026-06-26. 13 agents (3 git-forensics + 9 adversarial audits/claim-refutations + 1 synthesis). Commit hashes, authors (all OttoBot), dates, and the load-bearing diffs were independently re-verified against git before synthesis.*

---

## 1. DIRECT ANSWER — What this line of work did to the chain-selection rules

**It split the finality horizon into two markers and moved the write-admission freeze from the reachable one (k1) to an unreachable one (k2) — while leaving the actual chain-switch refusal pinned to k1. The result on this run is: no persistence floor armed anywhere, and a switch-refusal that still blocks the very reorgs the change was meant to enable.**

Before (pre-`86f390130`):
- `NakamotoChainStore.store()` refused a different-hash write at-or-below `nakamotoFinalizedOrdinalRef` (the **k1** confirm marker, advances continuously). Reorgs bounded to depth < k1.
- `ArchivalDepthK` was a hardcoded `sys.env`/65536 default.

After (`86f390130`):
- `store()` now refuses at-or-below a **new** `nakamotoArchivedOrdinalRef`, advanced **only** by `archive()`, called **only** by `T_depth2` at tip−k2.
- `ArchivalDepthK = 100 * confirmationDepthK` → dev k1=32 ⇒ **k2=3200** (testnet k1=256 ⇒ 25600).
- `finalize()` became a soft confirm (no freeze).

**NOT touched:** `ChainSelection.shouldSwitch`'s `candidateWins && !currentIsFinalized` guard still keys off **k1** (git-confirmed untouched across `86f390130^..HEAD`). The cement was half-removed: `store()` now *accepts* a write that `shouldSwitch` then *refuses to act on*.

**Net effect at this run's tip (~650–778, never near k2=3200):** `archive()` fired **0 times** on all 6 nodes; `nakamotoArchivedOrdinalRef` stayed `0`; the store gate is inert (`REFUSED store=0` cluster-wide). **No persistence floor and no retention bound active below the tip for the entire run.** Fork-safety rests entirely on density chain-selection — and `shouldSwitch` blocks the k1<depth<k2 density reorgs the commit intended to allow.

---

## 2. CONFIRMED DEFECTS — ranked by severity

| # | Severity | Defect | Attribution | Proof |
|---|----------|--------|-------------|-------|
| D1 | **CRITICAL** | **Stale-adopt-below-tip livelock.** Catch-up pulls an ordinal ≤ local bestTip; `store()` lands it in CASE 3 (stored, bestTip unchanged); `shouldSwitch` refuses to switch off a k1-finalized tip; `lastCatchUpAdoptedOrdinal=max(409,375)` never rises → Tier-3 re-fires forever. gl0-4 looped 342× for 57 min frozen at 409. | **INTERACTION — mine** (`e310f6ccd` unconditional store + `24e93ee10` monotonic-only guard + `86f390130` half-removed cement) | nodes/4 log 02:17–03:14; NakamotoChainStore.scala:369-389 |
| D2 | **HIGH** | **`pullLatestMptEntriesFromPeer` takes FIRST responsive peer, not freshest.** Unordered Set filtered only on Responsive; returns first success. gl0-4 pulled ord 375 while 5 peers held 674–752. Defeats the `24e93ee10` guard. | **MINE** (`e310f6ccd`) | NakamotoSyncDaemon.scala:287-307 |
| D3 | **HIGH** | **k2-freeze converts a bounded k1 floor into NO floor.** Served `finalized_ordinal` (gates CL0 SC-binary pruning) advances on soft k1 confirm while tip reorgs **below** it. reorgs/ord up to 0.76. | **INTERACTION — mine** (`86f390130`) | head−finalized = gl0-3 −44/−46 oscillating; gl0-2 reorg@736 vs Finalized=753 |
| D4 | **HIGH** | **No monotonic-advance guard in `byteFaithfulAdopt`.** Adopts pulled ordinal verbatim; sole gate is self-referential root-equality. Can move canonical tip backward. | **MINE** (`e310f6ccd`) | NakamotoSyncDaemon.scala:3404-3472 |
| D5 | **CRITICAL (storm driver)** | **Non-deterministic `smtRoot` in consensus root.** `GlobalSnapshotConsensusFunctions:270 recreatedArtifact === artifact` includes `smtRoot`; `attachSmtRoot` returns path-/ancestor-resolvability-dependent root → 97% (4094/4221) of ContentREJECTED are the empty `diffs=[stateProof[]]` signature. | **PRE-EXISTING** — `27c99564c5` (2026-05-29) | GSCF.scala:270; HistoricalCommitmentSmtStore.scala:121-125 |
| D6 | **HIGH** | **gl0-5 post-catch-up finalize wedge.** byte-faithful adopt leaves chainStore without contiguous ancestors; depth-finalize walks to 708, `chainStore.get` None, OVERLAY-PRUNE drops finalized → permanent stall. | **INTERACTION — mine** (`e310f6ccd` + `86f390130`) | gl0-5 log 03:11:35 |
| D7 | MEDIUM | **Unbounded in-memory `byHash` retention.** `keepDepthBehindFinalized=100*k1=k2` ⇒ keepFloor clamps to 0 ⇒ "pruned 0 orphan" every finalize; byHash 1→794. | **PRE-EXISTING** — `a5708ec8a` | types.scala:154 |

**MINE (introduced or materially worsened): D1, D2, D3, D4, D6. Pre-existing: D5, D7.**

---

## 3. THE FORK STORM — true root cause

**CREATOR: non-deterministic `smtRoot` (D5), pre-existing, `27c99564c5`, 2026-05-29.** 97% of ContentMismatch rejections are the empty `diffs=[stateProof[]]` signature — every enumerated field matches; only the un-enumerated `smtRoot` differs. Honest nodes reject each other's freshly-won blocks because `smtRoot` is path-dependent and inside the consensus `===` at GSCF:270. Violates the project rule *consensus root = pure fn of consensus-pinned state.*

**This line of work did NOT create the storm** (earliest commit 2026-06-24; smtRoot machinery 2026-05-29). **`8c55c6341`'s "killed the fork storm via LDD cutoff 16→30" is REFUTED** — HEAD runs cutoff=30 yet the storm reproduces; the cutoff is orthogonal to the driver.

**What the line of work DID do:** made the *consequences* worse without touching the cause — k2-freeze (D3) removed the bounded reorg floor; catch-up rewrites (D1/D2/D4/D6) converted a previous hard wedge into livelocks + a finalize stall. **Orthogonal to creation; materially amplifies blast radius.**

---

## 4. CLAIM SCORECARD

| Claim | Verdict | Deciding evidence |
|-------|---------|-------------------|
| "k2-freeze removed cement — 0 REFUSED store" | **PARTLY (vacuous)** | 0 is literally correct but `archive()` never fired (k2 unreachable) → gate never armed → can't distinguish "works" from "never exercised." |
| "Forks self-heal with k2-freeze + equal stake" | **FALSE** | Sole "pass" was a stalled-laggard gap-fill, not a fork reorg (isolated node built ZERO competing depth; hash identical before/after). Live cluster: 2 of 6 wedged. |
| "5 of 6 healthy; only gl0-4 wedged; not a fork" | **FALSE** | TWO wedged: gl0-4@409 (57-min livelock) AND gl0-5@693 (finalize None-walkback). Spread diverging 171→341. |
| "gl0-4 wedge = period-4 eta divergence; k2 exonerated" | **PARTLY** | k2 exoneration TRUE (gate dormant). Eta diagnosis FALSE: ords 407/408/409 same producer/period PASSED VRF; only 410 failed. Real persister = stale-adopt loop (D1). |
| "smtRoot non-determinism creates the storm" | **TRUE (mechanism), PRE-EXISTING** | GSCF:270 full-Eq includes smtRoot; 97% rejects are `stateProof[]`. Authored `27c99564c5`, not this line of work. |

---

## 5. CORRECTIVE PLAN

**P0 — Stop the storm at its source (D5). Highest leverage.** Make consensus content-validation smtRoot-blind, matching the existing-but-unused `StateProofComparison.equivalent` precedent. At `GlobalSnapshotConsensusFunctions:270`, normalize `stateProof.smtRoot = None` on both sides before `===` (or route through `equivalent`). Do NOT rely on `NAKAMOTO_LDD_CUTOFF=30` — this run proves it does not contain the storm. (Deeper follow-up: make smtRoot deterministic if it must be consensus-load-bearing.)

**P1 — Stop the catch-up livelocks (D1, D2, D4).**
- `byteFaithfulAdopt`: short-circuit (log + idle, no canonical write) when `pOrdinalL <= max(bestTipOrdinal, lastCatchUpAdoptedOrdinal)`. Never move the canonical tip backward.
- `pullLatestMptEntriesFromPeer`: pull from the **max-ordinal** responsive peer, not the first.
- Cooldown so a rejected-too-low adopt cannot immediately re-fire Tier-3.

**P2 — Reconcile the k1/k2 horizons (D3) — partial REVERT recommended.** `86f390130`'s intent (k1≤depth<k2 density reorgs) is sound but the impl is broken two ways: `shouldSwitch` still k1-gated (blocks the intended reorgs) and k2=3200/25600 is unreachable (no floor at all). Either make k2 reachable (`ArchivalDepthK = 10*k1` or a HOCON knob) **and** point `shouldSwitch` at the archived marker; **or REVERT `86f390130`** to the k1-gated freeze (reachable bounded floor) and pursue density reorgs as a separate validated change. **Revert is the safer default until P0/P1 land and a k2-reaching validation run exists.** Hard invariant: served `finalized_ordinal` ⊆ frozen state.

**P3 — gl0-5 finalize wedge (D6).** After byte-faithful adopt, populate chainStore with contiguous ancestors down to tip−k1, OR re-anchor the finalized marker to the adopted ordinal on a `chainStore.get` None.

**P4 — Decouple in-memory retention from archive depth (D7, pre-existing).** `keepDepthBehindFinalized = confirmationDepthK` (small multiple, ≥ 2·etaRotationSnapshots), not the k2 value.

**P5 — Observability.** Alert on `head < finalized_ordinal` per node and on `reorgs_total` rate.

---

## 6. WHAT REMAINS UNVERIFIED

1. **k2-freeze on its own terms is entirely untested** — no run reached k2 (3200 dev / 25600 testnet).
2. **The "old jar cemented" comparison** — asserted, never measured.
3. **The single ord-410 VRF failure on gl0-4** — its *trigger* (persister is the stale-adopt loop); genuine proof-verify failure vs threshold loss unresolved (logging can't distinguish).
4. **`Backfill: failed writing snapshot` errors on gl0-4** — observed, underlying write error not root-caused.
5. **Whether genuinely divergent tines reorg to a common chain** — never demonstrated; "does maxvalid-tk converge under the storm" is unanswered.
6. **`10763f37c` (cl1/dl1 adopt)** — in the line-of-work list, not analyzed; no evidence implicated, no evidence clean.

**Bottom line:** the line of work did not create the fork storm (pre-existing smtRoot non-determinism), but it made this run materially worse — two distinct catch-up livelocks/finalize stalls (D1, D6), removed the reorg-safety floor for any sub-k2 run (D3), and shipped a chain-selection change (`86f390130`) that is simultaneously inert (k2 unreachable) and self-contradicting (`shouldSwitch` still k1-gated). Four of seven confirmed defects are mine. The "self-heal / 5-of-6-healthy / no-cement" reassurances are not supported by the live cluster.
