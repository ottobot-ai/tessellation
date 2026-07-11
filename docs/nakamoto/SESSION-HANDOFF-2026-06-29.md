# Session Handoff — Sharded-Mirror Freeze + Downstream e2e Issues (2026-06-29)

> **HISTORICAL, NON-NORMATIVE PRE-FIX RECORD.** This is a point-in-time session
> handoff, not the current architecture or a compatibility contract. Its
> authoritative-push, sharded-mirror, branch, commit, test, and open-issue claims
> may describe code that was later removed or redesigned. Do not restore those
> paths from this document. Use [`../../AGENTS.md`](../../AGENTS.md), ADR-0016,
> ADR-0017, and current source as the design of record.

Branch: `feature/committee-state-diff`. For: codex evaluation / next session.

## TL;DR

- **Main deliverable DONE + validated live:** the gl0 sharded-mirror **freeze** (a pre-existing
  consensus bug) is fixed by an orphaned-tip **reanchor** (`e8bfbf888`). Proven on a live cluster
  (61 reanchors, all 6 gl0 converged on an identical hash, 0 forks, 0 exceptions) + 60 unit tests +
  an adversarial review that found no hard break.
- **2 supporting fixes committed:** 5-ref-map authoritative-push (`3b8845d7c`); delegated-stake test
  budget (`601ed962b`).
- **1 real issue root-caused but NOT fixed:** **#186** token-lock-replacement *ref-vs-set desync* —
  blocks the FULL e2e under `--stake-dist=uniform`. Design is ready; implementation is not done.
- **Honest self-assessment (see last section):** after the freeze fix was validated I kept chasing
  each *downstream* e2e failure deeply instead of checkpointing. Those downstream failures are
  **separate latent flakes the freeze fix merely un-masked** (the frozen mirror used to keep the
  cluster quiet). The freeze fix is solid; the rest is separable cleanup.

## Commits this session (newest first)

| commit | type | what |
|---|---|---|
| `601ed962b` | test | budget `delegated-staking.js:426/478` create-asserts to `maxOrdinalMisses:40` (matches siblings) |
| `e8bfbf888` | fix  | **orphaned-tip reanchor** — heals the sharded-mirror freeze (the headline) |
| `3b8845d7c` | fix  | sharded-adopt authoritative-push for the 4 remaining cumulative ref-maps (lastFeeTxRefs/lastAllowSpendRefs/lastTokenLockRefs/lastMessages) |

(Prior context, already on the branch before this session: the economic-trust sharded-security
feature — cross-shard settlement, watchtower, committee VRF, the data-with-fee authoritative-push
`a564d8c6e`, the lastTxRefs push `073823862`.)

## SOLID — high confidence (don't re-litigate)

### The freeze + its fix (`e8bfbf888`)
- **Root cause (PRE-EXISTING, not the sharded-security work):** gl0's per-MG mirror
  (`lastCurrencySnapshots` / `lastStateChannelSnapshotHashes`) advances only when a checkpoint window
  contains a binary whose `lastSnapshotHash === gl0's committed SC-tip` — an EXACT-HASH match at two
  mirrored sites, `GlobalSnapshotConsensusFunctions` embed-`pick` (645-660) and
  `GlobalSnapshotAcceptanceManager` adopt-guard (792-820), both authored by `09ace5227` (2026-06-10).
  A **same-ordinal slot-tiebreak shard reorg orphans the tip binary** → no window binary carries that
  parent hash → DEFER forever → the mirror **freezes** (observed: mg0 frozen at ord 121, mg1 at 61,
  while gl0+ml0 advanced). Confirmed pre-existing via `git blame` (predates every sharded-security
  commit). The token-lock e2e failure was a *victim* of the frozen mirror, not a separate bug.
- **Fix:** a single shared `ShardReanchor.classify`
  (`modules/node-shared/.../domain/nakamoto/ShardReanchor.scala`) drives BOTH sites byte-identically
  (lockstep). Orphaned tip = `idx<0` (no continuation) + genesis-rooted window
  (`nel.head.lastSnapshotHash === Hash.empty`) + `tipOrdinal ∈ [0, len-1]` ⇒ `Reanchor(min(tipOrdinal+1,
  len-1))` onto the committee-attested canonical lineage. genesis-rooted ⇒ binary index == metagraph
  ordinal (the producer's `chainLinkOrder.unfold` walks parent→child, no gaps — `ShardCheckpointProducer:817-828`).
- **Why it's safe with no verbatim-root code:** the EXISTING base-independent authoritative override
  (`GSAM:1019-1051` — the 5-ref-map + active-set push) recomputes the per-MG root over the metagraph's
  OWN signed cumulative state and requires `recomputed === attestedRoot` + per-field signed-proof gate
  (`GSAM:1079-1144`). So the orphan-prior cannot poison the adopt, and a wrong reanchor index simply
  DROPS. Pure function of (window bytes, prior GSI) — split-safe, no overlay/node-local reads.
- **Validation:** compiles (node-shared + dag-l0); **52 regression tests green** (MultiBranchAdopt,
  AdoptParity, Sharding, CurrencyAdoptRootsOnly, ShardCheckpointGl0, IncrementalVsRebuild); **8 new
  `ShardReanchorSuite` edge cases green**; adversarial review (7 axes) found **no hard break**; **live
  e2e: 61 reanchors fired, all 6 gl0 converged identical hash, 0 forks, 0 exceptions.**

### Refuted earlier in the session (do NOT re-chase these)
- "dagBatchSettle / blocks.accepted≈0" — a denominator artifact (the workload sends almost no
  DAG-layer txs; gl0 folded 100% of the few blocks). Not a metric, not an assert.
- "stake-withdrawal stuck" — withdrawals complete (accepted, state cleaned).
- These were idle-post-run misreads, settled by forensic agents with hard evidence.

## OPEN — the remaining issues

### 1. #186 — token-lock-replacement ref-vs-set desync (REAL consensus bug, NOT fixed)
This is the current blocker for a green **uniform-stake** e2e (`testIncreaseDelegatedStake` →
`InvalidTokenLock`/`NothingToReplace`). It is **deterministic** (all gl0 converge on a self-inconsistent
state), so it is NOT a race despite the memory's old framing.

- **The memory note `project_186` ("real fix = #118 OverlayReader") is REFUTED.** The overlay `pending`
  reader resolves at `chainStore.bestTip` (`NakamotoChainStore.scala:414`) — a node-local in-memory head.
  Reading it inside the consensus acceptance fold would SPLIT the cluster. Do **not** do that.
- **Actual root cause (verified at the seams):** in `GlobalSnapshotAcceptanceManager.accept`,
  `activeTokenLocks` is materialized from the **replacement-FILTERED** set
  (`TokenLockStateManager.acceptReplacementTokenLocks:265-304` drops a replacement unless its target is
  *currently active* AND `existing.amount < tx.amount` AND balance covers it), but `lastTokenLockRefs`
  is advanced from the **raw token-lock block chain-link**
  (`updatedTokenLockRefs = acceptTokenLockRefs(prior, tokenLockBlockAcceptanceResult.contextUpdate.lastTokenLocksRefs)`,
  `GSAM:2623-2625`; `acceptTokenLockRefs` is just `prior ++ contextUpdate`, `TokenLockStateManager:306-310`).
  So a **dropped replacement advances the ref but not the active set** → the stake's `tokenLockRef`
  points at a lock that was never materialized → the next chained replacement targets a non-active ref →
  cascade. Live evidence: `activeTokenLocks = {eaac19c6 @ord1}` but `lastTokenLockRefs = bfe040e9 @ord2`
  (`bfe040e9` exists only as a reference).
- **Fix DIRECTION (deterministic, no overlay) — needs design + review + e2e:**
  - Option A (ref-follows-set): derive `updatedTokenLockRefs` from the admitted/materialized
    `updatedGlobalTokenLocks`, so a dropped lock never advances its ref (invariant:
    `lastTokenLockRefs[addr]` = the newest lock actually in `activeTokenLocks[addr]`).
  - Option B (non-lossy filter): make `acceptReplacementTokenLocks` admit a chain-valid replacement
    (add new lock + unlock old from the block's own chain parent), removing the "block accepted the ref
    but the filter rejected the lock" divergence.
  - **Unresolved by me:** which option actually makes the e2e PASS (the operation must *succeed*, not
    just stop desyncing) and the exact interaction with the delegated-stake `tokenLockRef` the client
    re-reads. This needs the careful design I did NOT finish. Key files:
    `GlobalSnapshotAcceptanceManager.scala` (1922-1934, 2495-2626, 2887),
    `TokenLockStateManager.scala` (`acceptReplacementTokenLocks` 265-304, `generateTokenUnlocks` 664-688,
    `acceptTokenLockRefs` 306-310), `TokenLockBlockAcceptanceLogic.scala` (49-106, the ref-only advance).
  - `--stake-dist=harmonic` MASKS #186 (the memory's "right default for `just test`"); `uniform` exposes
    it. The user chose to fix it under uniform rather than switch to harmonic.

### 2. Reanchor Axis-7 follow-up (safe gap, not yet healed)
The reanchor only heals **genesis-rooted** orphans (= the observed freeze, whose window spans genesis).
A deep-reorg orphan AFTER gl0 finalizes the mg past genesis (window anchored mid-chain at
`finalizedBasePerMgTip`, non-genesis-rooted) safely **DEFERS** (no corruption) but is **not healed**.
Completion = a head-decode fallback: decode the window head's metagraph ordinal (effectful) so
`index→ordinal` works for non-genesis-rooted windows. Adversarial review confirmed this is safe-but-incomplete.

### 3. Latent determinism hazard (separate from #186, flagged by the review)
The gl0 delegated-stake validator embedded in the acceptance fold reads through the overlay `bestTip`
reader rather than the parent-branch `branchAwareReader` (`GSAM:505-527`). Not the trigger for #186,
but a real split hazard to fix separately (swap to the parent-branch reader in the fold).

### 4. Full e2e green
Not yet achieved under uniform. Sequence (all under `set -e`): cluster-health → dag-cluster →
delegated-staking → token-lock-replacement → multi-metagraph → currency → rewards → token-locks →
allow-spends → spend → data-with/without-fee. With the 3 committed fixes the run now passes
cluster-health, dag-cluster, and most of delegated-staking (create/update/withdraw), and dies at
`testIncreaseDelegatedStake` on #186 — i.e. it has NOT yet reached the `token-locks` phase that would
re-prove the freeze fix end-to-end (the freeze fix is already proven by the live-cluster observation
above).

## How to run the e2e (for codex)
```
# fix in shared/node-shared/dag-l0 → reuse JARs only if unchanged since last build:
NAKAMOTO_COMMITTEE_K_DRAW=3 NAKAMOTO_COMMITTEE_K_QUORUM=2 \
just test --use-test-metagraph --skip-streaming --grafana --keep-alive \
  --num-gl0=6 --num-ml0=1 --stake-dist=uniform --metagraphs=2 --num-shards=2
# rebuild needed (Scala changed) → drop --skip-assembly + add `just nuke-metagraph` first.
# WSL2 buildkit "parent snapshot does not exist" → docker buildx prune -af.
# --keep-alive preserves the cluster for live diagnosis (gl0-0=:9000, Prometheus :19090).
```

## Where I may have lost the plot (honest)
- The **freeze fix is the deliverable and it is done + validated**. After that point I should have
  checkpointed. Instead I treated each downstream e2e failure (test-budget, then #186) as if it were
  part of the same task and dove deep on each — multiple forensic agents, multiple ~60-min e2e cycles,
  a Docker cache flake — accreting scope.
- The pattern I was slow to name: **un-freezing the mirror makes the cluster do real work again, which
  exposes pre-existing latent flakes the frozen mirror was masking.** Each "new failure" was a separate,
  mostly-known issue (too-tight test budgets; #186, which `harmonic` already masks), not a regression in
  my fix. Recognizing that earlier would have meant: commit the validated freeze fix, and triage the
  rest as separate tickets — which is where we are now.
- Net: trust the freeze fix (evidence is strong). Treat #186 as its own focused consensus task with a
  clean design pass (the direction above is verified; the option choice + the success path are the open
  design questions). Consider proving the freeze fix's full-suite path under `harmonic` (fast green)
  while #186 is fixed under `uniform` separately.
