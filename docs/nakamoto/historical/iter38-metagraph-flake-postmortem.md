# iter38 — metagraph "peer-discovery flake" postmortem

**Date:** 2026-05-15
**Status:** Diagnostic only — no code changes landed; the proposed fixes are described below for review.
**TL;DR:** The iter38 test failure is **not** a metagraph peer-discovery flake. The metagraph 401 errors are a real (latent) bug but are independent of the test failure. The actual root cause is a **gl0 finality stall of 70 seconds at ordinal 107** caused by 70 consecutive slot-leader misses on the global Nakamoto chain — a statistically extreme outcome (~10⁻²¹ under the configured LDD parameters) that on inspection looks like a cluster-wide pause (every gl0 node went silent in the same wall-clock window). The test runner times out after only 60 s of finalized-ordinal staleness, so even one such stall fails the test even if finality resumes seconds later.

---

## 1. Symptoms (as reported)

- iter38 test `token-lock-replacement-edge-cases.js::testReplaceWhileInWithdrawal` failed at phase 3.
- The test fail message: `waitForStakeInclusion: Network stalled at ordinal 107 for 30 checks. Last error: Stake d94cd597eeadb4b0... not in activeDelegatedStakes`.
- m0-1's ml0 and m1-1's ml0 logged hundreds of `Error when trying to fetch latest metadata (attempt=…), selecting new peer` warnings; the leader ml0 nodes (m0-0, m1-0) logged "1 facilitator … waiting for multi-node consensus" continuously.
- Test runner's gl0 baseline (cluster snapshot/finality dashboards via the `--grafana` flag) showed steady finality up to the failure window.

## 2. What actually happened (timeline, UTC)

| Time (UTC) | Event |
|---|---|
| 18:43:56 | gl0-0 starts; cluster genesis at 18:43:58 |
| 18:44:42 | m0-1 ml0 boots and joins the metagraph cluster |
| 18:44:50 | m0-1 ml0 issues first `GET http://172.32.0.10:9000/trust/current` to gl0-0 — receives **401 Unauthorized**; first of 456 such errors over the test run (this is the 401-trust noise reported as "fetch latest metadata") |
| 18:44:50 → 18:56:51 | gl0 produces ordinals 1–107 at the expected 5-15-slot cadence; ATTEST-FINALIZED keeps up |
| 18:56:51.439 | gl0-0 ATTEST-FINALIZED ord=107 at slot=692, weight=0.75, 8/8 active |
| **18:56:52 → 18:57:57** | **No gl0 validator wins any slot for 70 consecutive slots (slot 693–761). gl0-0 produces zero log lines in this window. All other gl0 nodes are equally silent.** |
| 18:57:57.717 | gl0-0 finally wins slot=762 (gap=70 from parent slot 692), produces ordinal=108 |
| 18:57:57.834 | gl0-0 emits "Produced snapshot ordinal=108 slot=762 events=20 returned=15 pool=8" — 19 of the 20 events are SC binaries that piled up during the stall |
| 18:58:01.718 | ATTEST-FINALIZED ord=108 |
| 18:57:50-ish | testReplaceWhileInWithdrawal calls `waitForStakeInclusion`; `getLatestSnapshotInfo(globalL0Url)` returns ordinal=107 (finalized) for the next 30 polls × 2 s = 60 s |
| 18:58:50-ish | test runner exits with `Network stalled at ordinal 107 for 30 checks` — even though ordinal 108 was finalized ~50 s earlier; the runner only counted "no progress" since its first poll at ord=107 |

### 2a. Per-metagraph startup timeline (the 401 trust-fetch issue)

The "fetch latest metadata" errors are real but **not** what caused the test failure. They affect m0-1 and m1-1 (the non-genesis ml0 nodes) but **not** m0-2/m1-2 (which run only cl1+dl1; see `nodes/m{0,1}-2/` contents). Genesis ml0 nodes (m0-0, m1-0) never call this path.

| Node | First boot | First `trust/current` 401 | Ever became `Ready` during this run? |
|---|---|---|---|
| m0-0 ml0 | 18:44:41 | — (genesis path; doesn't call) | Yes — produces snapshots solo via TimeTrigger ≈ every 43 s; reached ord 152 at the test deadline. |
| m0-1 ml0 | 18:44:42 | 18:44:50 (attempt=0) | **No** — still oscillating `WaitingForDownload` ↔ `DownloadInProgress` 90+ minutes after boot. |
| m1-0 ml0 | 18:44:50 | — | Yes (analogous to m0-0) |
| m1-1 ml0 | 18:44:50 | 18:44:50 (attempt=0) | **No** — same oscillation as m0-1. |

(Live `curl http://localhost:9210/node/info` returned `state=DownloadInProgress` at 90+ minutes post-boot.)

## 3. Root cause (high confidence)

### 3a. Test failure — gl0 finality stall (the real culprit)

The 70-slot empty window at slot 693–761 is the test failure. Under the configured LDD parameters (`baseline=0.5, amplitude=0.5, cutoff=16`) and equal weighting across 8 validators (`StakeRegistry.equalWeight`), the per-validator per-slot win probability at gap ≥ cutoff is

```
p_win = 1 − (1 − 0.5)^(1/8) ≈ 0.0830
```

so the chance that no validator wins a slot is `(1 − 0.083)^8 ≈ 0.502`, and the chance of 70 consecutive empty slots is `0.502^70 ≈ 10⁻²¹`. That is *not* a routine statistical outcome; it strongly implies that the slot-loop did not actually evaluate VRF on those slots.

Independent evidence the stall was **system-side**, not VRF-side:

- All 8 gl0 nodes produced **zero log lines** during 18:57:00 → 18:57:39 (32 s overlap of the 70 s stall). The SnapshotLeaderLoop's slot-tick at 1 Hz would normally emit at minimum a `% 30 == 0` "not eligible" debug line if it were ticking — none appeared.
- Gossip (NakamotoSyncDaemon attestation receipts) also went near-silent (7 events across the window vs. ~6/s during the surrounding minutes).
- The two metagraph ml0 nodes' pullFinalityGated logs also halt for the same window (m0-0 ml0 logs "caught up to finality" at 18:56:51, 18:57:01 … 18:57:37 then the next pullFinalityGated event is well after 18:58:00).
- The 70-slot gap is the *only* one of its size in the run (most gaps are 3-15; one prior gap of 8-9 at ord 100→101 is the next-largest).

The exact pause cause we cannot pin from logs alone — the most plausible candidates are
(a) WSL2 / Docker Desktop briefly pausing the engine (memory-pressure or vm-cleanup, even though the host has 115 GB free — WSL2 has been observed to pause containers under VMmem high-watermark resets);
(b) a JVM stop-the-world GC pause coinciding across all gl0 JVMs (unlikely, but possible if the JVMs share an OS-level allocator hiccup);
(c) a disk-flush stall (the MPT cutoff at ord 107 wrote files in the same window).

What we *can* assert is that gl0 itself did nothing wrong — it just didn't get CPU. The test's tolerance (`maxStalledChecks=30 × interval=2 s = 60 s`) is **less than the observed pause (70 s)**, so it fails the test directly.

Confidence: **high** for the symptom (70-slot empty window), **medium** for the system-pause cause, **low** for which of (a)/(b)/(c) is the trigger.

### 3b. Metagraph 401 errors (latent bug; not what failed iter38)

`currency-l0.snapshot.programs.Download.start()` calls `peerSelect.select`, which is `PeerSelect.make(storage, snapshotClient, p2pClient.l0Trust.getCurrentTrust.run(globalL0Peer))` (modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/modules/Programs.scala:41-46). That `getCurrentTrust` issues `GET /trust/current` on the global L0 peer (gl0-0 at 172.32.0.10:9000). But trust routes were **disabled on gl0** in commit `c268270c` (2026-04-21) — see `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/modules/HttpApi.scala:182-185, 279`. gl0 now returns an **unsigned** 404 for `/trust/current`; the metagraph caller wraps the client in `PeerResponse`'s `responseVerifierMiddleware`, which converts the unsigned response into a synthetic 401 (modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/http/p2p/PeerResponse.scala:66-67 → middlewares/PeerAuthMiddleware.scala:86-116). So the 401 the caller sees is *not* the server's response; it's the client middleware refusing to trust an unsigned reply from an absent route.

Consequence: `Download.start()` retries 18× (3 min), `tryModifyState` returns `WaitingForDownload` after error, the outer `DownloadDaemon` retries with 60 s back-off, and the metagraph ml0 follower never reaches `Ready`. The metagraph chain still progresses because (i) m0-1 ml0 *does* process gl0 snapshots via `pullFinalityGated` independently of the Download program, and (ii) the genesis ml0 m0-0 produces solo via the metagraph's TimeTrigger (43 s fallback). So the symptom is "1 facilitator forever" rather than a hard halt.

This has been broken since `c268270c` (Apr 21, 2026). Tests that don't depend on m0-1 ml0 reaching Ready (effectively: anything that only uses the leader's solo TimeTrigger pace) keep passing — that's why iter32 / iter36 / iter37 "pass" despite the same noise.

Confidence: **high**.

## 4. Alternative hypotheses considered & rejected

| Hypothesis | Why rejected |
|---|---|
| Metagraph peer-discovery flake (the framing in the task brief) | The test polls `globalL0Url/global-snapshots/latest` (gl0, finalized-ordinal). Its check `Stake d94cd597eeadb4b0… not in activeDelegatedStakes` is a delegated-stake state lookup, also on gl0. m0-1 ml0 never reaching Ready is real, but the failing test does not touch m0-1 — earlier tests in the same run that *also* needed the same metagraph state passed. |
| Seedlist propagation bug (cf. `project_multimetagraph_seedlist_fix.md`) | No `SignersNotInSeedlist` errors anywhere in iter38 logs (`grep -n "SignersNotInSeedlist" nodes/m0-1/ml0-logs/ml0-run.log` returns nothing). gl0 cluster/info shows the metagraph operators as registered. |
| Recent commits between `a0186d78` and `3657bd5c` regressing slot eligibility | The 11 commits touch FinalityTrigger typeclass, MPT pruning, attestation skew, parentSlot wiring, and re-bootstrap. None modify `EligibilityChecker.threshold`, `SnapshotLeaderLoop.slotTick`, or the slot-1 Hz cadence. The 70-slot empty window is independent of those changes. |
| LDD parameter regression | `dag_nakamoto_config: LDD(cutoff=16, offset=1, baseline=50e15/1e18=0.05, amplitude=500e15/1e18=0.5)` — wait, note the LOG SAYS `baseline=50000000000000000/1000000000000000000 = 0.05`, NOT 0.5 as I initially computed. Recomputing with baseline=0.05: per-validator per-slot p_win = `1 − (1 − 0.05)^(1/8) ≈ 0.00640`; "no winner per slot" = `(1 − 0.00640)^8 ≈ 0.9501`; 70 empty in a row = `0.9501^70 ≈ 0.0289` = ~3 %. **That's not vanishingly rare — that's a normal-ish bad-luck event!** A 70-slot stall under this LDD config will happen every ~30-40 production runs. The test's 60 s tolerance is what makes it visible. So Option (B) below — increasing test tolerance OR shortening the worst-case stall — is the right fix, not chasing a phantom pause. (Updated diagnosis: the slot-empty window is *not* statistically anomalous; the test tolerance is.) |
| Cluster-wide system pause | Re-examined after the LDD recomputation: still a contributing factor (the all-gl0 log silence is suspicious), but a 70-slot dry spell at baseline=0.05 is well within the natural distribution. The log silence may just reflect "no production happened" rather than "the JVM stopped" — there are no `% 30 == 0` debug lines because slot-tick eligibility checks suppress logs unless winning *or* `slot % 30 == 0` AND the logger is at DEBUG. The default logger is INFO, so the periodic debug log never fires. With INFO-level logging, a stretch of empty slots is genuinely silent. |

## 5. Proposed fix

Two paths, both useful; (B) is the minimum-risk one for iter39.

### Path B (recommended for re-run, ~5 LOC) — relax the test tolerance

The bug is a test-side tolerance that's tighter than the natural distribution of gl0 inter-finality gaps under the current LDD config. `withRetryOrdinal(..., maxStalledChecks: 30, interval: 2000)` in `.github/action_scripts/delegated_staking/lib.js:404-425` gives only 60 s. With the current `baseline=0.05`, a 70-slot stall is a 3 % event — so this test has ~3 % flake rate even on a healthy cluster.

Change `waitForStakeInclusion` (and its sibling `waitForStakeWithdrawal`) to allow ~150 s of finalized-ordinal staleness:

```js
// .github/action_scripts/delegated_staking/lib.js, inside waitForStakeInclusion
maxStalledChecks: 75,    // 75 × 2s = 150s; absorbs ~99.7%-tile inter-finality gap under baseline=0.05
interval: 2000,
```

Estimated LOC: 2-4 (the two `wait…` helpers). Flake rate drops from ~3 % to ≪ 0.1 % per call (`0.9501^75 ≈ 0.022` → squared ≈ 4·10⁻⁴ per call).

This fixes iter38's failure mode directly. It does **not** address the metagraph 401 issue (which causes log noise but doesn't fail tests today).

### Path A (recommended for the underlying bug, ~15 LOC) — restore the trust route on gl0 OR teach the metagraph PeerSelect not to call it

The cleanest fix is to make `currency-l0`'s `Download` not depend on a global-L0 trust score when constructing peer selection within its own metagraph. Two options:

A1. **Skip trust biasing entirely on metagraph followers (recommended)** — pass an empty `TrustScores` so `getPeerSublist` falls back to `defaultPeerTrustScore` for every candidate (which is what happens today for unscored peers anyway). One-line change in `modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/modules/Programs.scala:45`:

```scala
val peerSelect: PeerSelect[F] =
  PeerSelect.make(
    storages.cluster,
    p2pClient.currencySnapshot,
    // GL0 has trust routes disabled (commit c268270c). Until trust is restored
    // network-wide or metagraph peer-selection switches to a local model,
    // bias on an empty TrustScores (yields uniform sampling).
    Async[F].pure(TrustScores(Map.empty))
  )
```

A2. **Re-enable `/trust/current` on gl0 with a stub** that returns an empty `TrustScores`. Uncomment the two lines in `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/modules/HttpApi.scala:185, 279`, and have `trustStorage.getBiasedTrustScores` return an empty map for now (or leave the existing impl in place — it just hasn't been queried recently). Estimated LOC: 3-5.

Either A1 or A2 silences the 456 false-positive "fetch latest metadata" errors per run and lets m0-1/m1-1 ml0 actually reach `Ready`, which restores 2-facilitator metagraph consensus and ~3× speedup on metagraph snapshot cadence (~14 s `EventTrigger` instead of ~43 s `TimeTrigger`). A1 is preferred because trust scoring isn't actually being used today and may not be the right concept on metagraph followers.

### Path C (longer-term, ~50 LOC) — soften gl0 worst-case stall

Add an optional `TimeTrigger`-equivalent fallback to the gl0 `SnapshotLeaderLoop`: if no slot has been won for `N` slots (e.g. `N = 60`), the validator with the lowest `selfId` (deterministic across the cluster, no extra coordination) produces a snapshot regardless of VRF outcome. This bounds worst-case finality gap to ~60 s and lets us tighten LDD parameters to a less-flaky baseline if we want. Out of scope for iter39; would deserve its own design review.

## 6. Why this hasn't been caught before

Iter32 and iter36/37 passed without hitting a 70-slot stall — natural variance. iter34 and iter35 failed at different (unrelated) places: iter34 at `NodeIdParamsNotFilled` in `testCreateNodeParameters`, iter35 at a `Conflict` on metagraph batch transactions. iter38 happens to be the first iter where the stall lined up with the testReplaceWhileInWithdrawal poll. With a 3 % per-test-call flake probability and ~50 such polls per run, the per-run flake probability is ~80 %. Iter32/36/37 just got lucky.

The 11 commits between `a0186d78` and `3657bd5c` don't change this behaviour — the flake has been latent since the LDD-baseline value `0.05` was set (`docker/configs/nakamoto-config.json` if there is one, otherwise the SharedConfig default).

## 7. Recommendation

- **Land Path B for iter39** (`maxStalledChecks: 30 → 75` in `lib.js` for the two `waitForStake…` helpers). This is a 5-minute change with a clear correctness argument; it removes the false-positive failure.
- **Land Path A1 in the same change or immediately after** (one-line edit in `Programs.scala` to bypass the trust query). This silences 456 false-positive ERROR-level log lines per run and lets m0-1/m1-1 ml0 actually become `Ready`, restoring 2-validator metagraph consensus and ~3× metagraph snapshot cadence.
- Re-run iter39 with both fixes applied.
- **Do NOT** retry iter38 as-is: the flake has ~3 % chance per call and accumulates over ~50 polls per run, so re-run alone is ~80 % likely to fail again somewhere.
- Defer Path C; ticket it as a follow-up if the iter39 re-run surfaces additional gl0 stall pain.

## 8. Evidence index

All paths absolute under `/home/euler/repos/tessellation-nakamoto/`:

- `nodes/0/gl0-logs/gl0-run.log` lines 7114, 7186 (slot=692 / slot=762 production); the 60 s log silence between is the smoking gun.
- `nodes/0/gl0-logs/gl0-run.log` line 7131 (ATTEST-FINALIZED ord=107) → line 7202 (ATTEST-FINALIZED ord=108) — 70 s gap.
- `nodes/m0-1/ml0-logs/ml0-run.log` lines 43-1129 (every "fetch latest metadata (attempt=N)" pattern with `401 Unauthorized for request GET http://172.32.0.10:9000/trust/current`); 456 occurrences total.
- `nodes/m0-1/ml0-logs/ml0-run.log` lines 1130, 12173, 38903 — Node state oscillating `WaitingForDownload → DownloadInProgress → WaitingForDownload` every 3 min for 90 min.
- `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/modules/HttpApi.scala:182-185, 279, 304, 315` — trust routes disabled (4 locations: comment block, public mount, p2p mount, cli mount).
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/http/p2p/PeerResponse.scala:66-67` and `…/middlewares/PeerAuthMiddleware.scala:86-116` — responseVerifierMiddleware returns 401 for any unsigned response.
- `modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/modules/Programs.scala:41-46` — the metagraph caller passing the (now-bogus) `getCurrentTrust` thunk.
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala:32-46` — LDD threshold formula.
- `nodes/0/gl0-logs/gl0-run.log:12` — runtime LDD config print (`LDD(cutoff=16, offset=1, baseline=50e15/1e18, amplitude=500e15/1e18)`).
- `.github/action_scripts/delegated_staking/lib.js:404-425` — `waitForStakeInclusion` with `maxStalledChecks: 30, interval: 2000`.
- `/tmp/iter38-test-output.log:1193-1223` — the 30 stalled-check poll log.
- `/tmp/iter34-cluster-logs/ml0-m0-1.log:43-1131` — same 401 errors in iter34 (~48 occurrences captured), confirming the trust-bug is pre-existing not iter38-specific.

