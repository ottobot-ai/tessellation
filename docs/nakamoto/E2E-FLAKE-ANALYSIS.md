# E2e Flake Analysis — 8gl0+4mg+4shards

Branch: `feature/serde-typeclass-shim` HEAD `46f3e2577`
Topology: 8 gl0 + 4 metagraphs (2 ml0 per mg) + K_target=4 committee-gate
Stake dist: harmonic (0.368, 0.184, 0.123, 0.092, 0.074, 0.061, 0.053, 0.046)
Reference runs:
- v25c PASS: `test-runs/iter-v25c-s0-validation.log` — 5263s end-to-end (commit `10abb0fa5`)
- g1 FAIL: `test-runs/iter-g1-full-e2e.log` — fails on TokenLock verifyInL0 (after ~95 min)
- g1-g4 FAIL: `test-runs/iter-g1-g4-full-e2e.log` — fails on Currency allow-spend TooFarLastValidEpochProgress
- g1-g4-v2 FAIL: `test-runs/iter-g1-g4-full-e2e-v2.log` — fails on Multi-metagraph K=4 visibility (commit `0e2931a2d`)

## Summary table

| Mode | Manifestation | Observed runs | Root-cause hypothesis | Proposed fix | Effort |
|------|---------------|---------------|----------------------|--------------|--------|
| 1 | `Content REJECTED slot=16/19 diffs=[stateProof[scHashes,balances,mptRoot]]` | v25c PASS (slot=3, slot=16); g1-g4-v2 FAIL (slot=16, slot=19) | **NOT a failure mode** — a controlled reorg trigger that self-heals via `NakamotoSyncDaemon.run`'s tentative-branch path. Same signature in PASS and FAIL. Symptom, not cause. | None — preserves designed behavior. Add log-level downgrade INFO → DEBUG once Mode 3 is fixed so the noise floor matches reality. | 5 min |
| 2 | `TooFarLastValidEpochProgress{epochProgress=518, currentEpochProgress=1}` on cl1 `POST /allow-spends` | g1-g4 FAIL only | cl1 `lastSnapshotStorage.get` returns `Some(cis)` but `cis.globalSyncView=None`, so `AllowSpendService.offer` falls back to `EpochProgress.MinValue=0` → `currentEpochProgress=1` (`EpochProgress` is `>= 1` postnatally). Affected currency snapshot was produced by ml0 before its cl0 first received a gl0 snapshot. **Downstream of Mode 3** (gl0 never advanced the metagraph past genesis → cl0 never received globalSyncView → first cis on cl1 has globalSyncView=None). | (a) Fix Mode 3 first; (b) defensive: use `lastGlobalSnapshotEpochProgress` from `lastNGlobalSnapshotStorage` (cl1 owns this) instead of mining it from the embedded cis. | 30 min for (b) once (a) lands |
| 3 | gl0 sees only 1/4 metagraphs in `lastCurrencySnapshots` within 150s | g1-g4-v2 FAIL (primary); g1-g4 FAIL indirectly | **MultiBranch overlay loses `ActiveAddressIndex` keyset entries at fork-replace.** Round-4 finalize accepts 3 of 4 metagraph genesis binaries; subsequent rounds reject every post-genesis incremental from the 3 missing metagraphs with `gl0.lastHash=000000000000` because `priorLastStateChannelSnapshotHashes` materialised from `mpt.get(activeAddressIndexKey)` returns a subset of the actual GSI keyset. The reverted fix `46d505c55` (`abb36f723`) addressed this exact symptom by unioning the GSI keyset; it was reverted without a replacement. | Reapply the GSI-keyset union from `46d505c55` for both `priorLastStateChannelSnapshotHashes` and `priorLastCurrencySnapshots` in `GlobalSnapshotAcceptanceManager.scala:912-967`. Add a regression test that runs Phase J MultiBranch with a deliberate sibling-branch eviction at the round that admits the index entry. | 2-4 hr including test |
| 4 | TokenLock `verifyInL0` returns "No active token locks found" for 600 attempts (~10 min) | g1 FAIL (primary); intermittent everywhere | Same Mode 3 — the test metagraph `DAG4GPGTxooK…` is one of the 4 mg, and on this run was a "missing" one. Its TokenLockBlock binaries land in cl0 / cl1 but the SC binary carrying the post-tokenlock GSI delta never makes it into gl0, so `gl0.activeTokenLocks` stays empty. The 600-attempt bump in commit `011663b64` papers over the symptom but doesn't fix the cause. | Fix Mode 3. Restore the 300-attempt bound after Mode 3 is fixed; if it still flakes, then there is a separate liveness issue worth its own analysis. | 0 (subsumed by Mode 3) |

**Net assessment:** modes 1-4 are not 4 independent flakes. Mode 1 is a non-failure (designed reorg path). Modes 2 + 4 are downstream symptoms of Mode 3. **There is essentially one bug**: the MPT `ActiveAddressIndex` sidecar's `mpt.get` query against the MultiBranch overlay returns an incomplete keyset, which causes gl0 to chain-link-reject every post-genesis binary from "lost" metagraphs.

The full v25c PASS does not falsify this story — the PASS exhibited the same `scHashes` reject (Mode 1) and self-healed via reorg. The difference between PASS and FAIL is *whether* the round-4 finalize happens to land on a branch where all 4 metagraph index entries survived the `branchesDropped=1` eviction. That's a 50/50 coin flip per-run, which matches the observed flake rate (v23 PASS, v24 FAIL, v25c PASS, current sequence FAIL/FAIL/FAIL).

---

## Mode 1: slot=16/19 scHashes/balances/mptRoot reject

### Log signature
```regex
❌ Content REJECTED: slot=\d+ diffs=\[stateProof\[(scHashes|balances|delegStakes|mptRoot)(,(scHashes|balances|delegStakes|mptRoot))*\]\]
```

### When it appears
Within the first 60 s of cluster bootup, on the receiver side of `NakamotoValidator.validate`. Always followed by `NakamotoSyncDaemon: 🔀 Fork at ordinal=N slot=M (gap=K). Storing as tentative branch (deferred validation)` and then `🔄 Reorg to fork at ordinal=N slot=M (denser chain). Validating via catch-up`.

### v25c PASS comparison
**Yes — the reject is present in the v25c PASS log at the same slot positions** (`test-runs/iter-v25c-s0-validation.log:1293,1486`). This is the strongest piece of evidence that Mode 1 is not a fault mode at all.

The PASS sequence:
1. `22:56:10.564` — `Content REJECTED: slot=3 diffs=[stateProof[delegStakes,mptRoot]]`
2. `22:56:10.568` — `Fork at ordinal=2 slot=3 (gap=1). Storing as tentative branch (deferred validation)`
3. `22:56:10.599` — `Reorg to fork at ordinal=2 slot=3 (denser chain). Validating via catch-up.`
4. `22:56:10.599` — `Production PAUSED: reorg-in-progress`
5. `22:56:10.612-22:56:10.614` — `[MptStore] Clearing store` → `[MPT] Full build from 29 entries (no cached trie)` → `Mempool reconciliation: all 4 events valid for new context`
6. `22:56:10.647-22:56:10.652` — `Production RESUMED`, attestations resume
7. Cluster proceeds normally → 5263 s total PASS.

The FAIL g1-g4-v2 sequence shows the **identical** mechanism at slots 16 and 19 (`test-runs/iter-g1-g4-full-e2e-v2.log:4384, 4462`); the cluster continues forward and reaches the Multi-metagraph K=4 test 13 min later. The Mode 1 reject is therefore neither the cause of the FAIL nor a flake signal.

### Per-node inspection
Across all 8 gl0 nodes' `gl0-run.log`, the rate of stateProof-diff rejects is **12 events total in 110 min** — single-digit per node. That density is consistent with the bootstrap window only.

### Root-cause hypothesis (not a bug)
Per `NakamotoSnapshotValidator.scala:176-227`, the diff-builder explicitly outputs `stateProof[…]` on a `GlobalArtifactMismatch` returned by `validateArtifact`. The mismatch path is **expected** during early-ordinal racey leadership where two leaders may build slightly different artifacts (different rewards distribution, different ordering on contested SC binaries), and one of the artifacts is what the local node attempts to validate. The reject triggers a reorg — that's the design (see the comment block at `NakamotoSnapshotValidator.scala:217-222`).

### Proposed fix
None. The downgrade I'd suggest is purely cosmetic — change the log level from `WARN` to `INFO` for the slot=3-window case (`< K * 8` slots from genesis), and INFO → DEBUG for the slot=16+ case, since they cause confusion in failure-mode triage by drawing the eye when the actual failure is elsewhere in the log. Cite: `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSnapshotValidator.scala:224`.

### Effort
5 min for the log-level downgrade. Optional.

---

## Mode 2: TooFarLastValidEpochProgress

### Log signature
```regex
data: \{\s*errors:\s*\[\s*\{\s*message: 'TooFarLastValidEpochProgress\{epochProgress=\d+,currentEpochProgress=\d+\}'
```

### When it appears
Currency workflow / allow-spend transactions posting against cl1 at `POST /allow-spends`. The cl1 returns HTTP 400 with the embedded error. In `iter-g1-g4-full-e2e.log:7477` the error message is `TooFarLastValidEpochProgress{epochProgress=518, currentEpochProgress=1}` — 70+ min into the test, the client's `lastValidEpochProgress=518` (correctly computed from the most recent gl0 epoch) is far above cl1's view of `currentEpochProgress=1`.

### v25c PASS comparison
**No** — the v25c PASS does not exhibit this; all currency-workflow steps proceed to completion (5263 s total).

### Per-node MPT log inspection
This message is emitted by cl1 (CurrencyL1), not gl0. The cl1 log was not preserved for this run, but the source path is unambiguous:

- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/AllowSpendService.scala:42-56` reads `lastGlobalEpochProgress` from `lastSnapshotStorage.get`:
  ```scala
  lastGlobalEpochProgress <- lastSnapshotStorage.get.map {
    case Some(snapshot) =>
      snapshot.signed.value match {
        case cis: CurrencyIncrementalSnapshot =>
          cis.globalSyncView.map(_.epochProgress).getOrElse(EpochProgress.MinValue)
        case gis: GlobalIncrementalSnapshot => gis.epochProgress
        case _ => EpochProgress.MinValue
      }
    case None => EpochProgress.MinValue
  }
  ```
- cl1 holds `lastSnapshotStorage` as a `LastSnapshotStorage[F, CurrencyIncrementalSnapshot, CurrencySnapshotInfo]` (`modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/modules/Storages.scala:57`). The `Some(cis)` branch is taken.
- If the latest cis on cl1 has `globalSyncView=None`, `lastGlobalEpochProgress = EpochProgress.MinValue = 0`. `validateEpochProgress` (`ContextualAllowSpendValidator.scala:101-114`) then rejects with `TooFarLastValidEpochProgress(518, 1)` because `lastValidEpochProgress` must be in `[currentEpochProgress + min, currentEpochProgress + max]` where `min,max` are roughly ±32.

### Root-cause hypothesis
The currency snapshot that ended up at the head of cl1's `lastSnapshotStorage` had no `globalSyncView` written by its producer (cl0). This happens when cl0's `CurrencySnapshotAcceptanceManager` (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManager.scala:317-349`) reaches the `lastSyncGlobalSnapshot` lookup but the underlying gl0-tracker (`lastNGlobalSnapshotStorage`) hasn't yet received a single gl0 snapshot for this metagraph.

The **upstream cause** is Mode 3: gl0 silently drops every post-genesis binary from this metagraph due to MPT `ActiveAddressIndex` sidecar loss. With gl0 not advancing the metagraph past `LastCurrencySnapshots[mg]=genesis`, the cl0 cannot bootstrap its `lastGlobalSnapshots` view past `MinValue`, so the cl0 produces cis with `globalSyncView=None`, which the cl1 then stores. The 70+ minute delay before this surfaces matches the `Currency workflow allow-spend` test being the first to push *fresh* allow-spends after the cluster had been running for hours — by which time cl1's reorg-blind `MaxAttempts` was already exhausted.

### Proposed fix
Two layers:
1. **Primary (root cause):** fix Mode 3. The cl1 will start receiving cis with `globalSyncView=Some(…)` and the validator will accept allow-spends.
2. **Secondary (defense in depth):** change `AllowSpendService.offer` (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/AllowSpendService.scala:44`) to also consult `lastNGlobalSnapshotStorage.getLatestSync` for the lastValidEpochProgress before falling back to MinValue. The cl1 has its own gl0 follower already (`globalL0Cluster`), so this is a wiring change, not a new dependency.

### File:line citations
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/AllowSpendService.scala:44-56` (cis-only fallback)
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/ContextualAllowSpendValidator.scala:101-114` (validator)
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManager.scala:339-349` (producer side)

### Effort
- Primary: subsumed by Mode 3.
- Secondary: 30 min wiring + 1 hr to write a regression test that spawns cl1 against a 0-snapshot gl0 stub.

---

## Mode 3: Multi-metagraph 1/4 visibility

### Log signature
```regex
\[SCAcceptance\] Chain-link rejection at ord=SnapshotOrdinal\{value=\d+\} for addr=DAG[A-Za-z0-9]+: gl0\.lastHash=000000000000 but binary\.lastSnapshotHash in \{[a-f0-9]+\}\. 1 returned, \d+ possible\.
```

The smoking gun is `gl0.lastHash=000000000000` (Hash.empty) appearing for a metagraph whose genesis binary was demonstrably accepted into a prior global snapshot.

### When it appears
At every gl0 round once a metagraph's post-genesis binary is gossiped. In `iter-g1-g4-full-e2e-v2.log`, the test reaches Multi-metagraph at 801 s and probes for 150 s; gl0 reports 1/4 metagraphs for all 30 attempts.

In the preserved gl0-0 log:
- gl0 round=4 (`19:23:12.835`) accepts `scSnapshots=3` (3 of the 4 metagraph genesis binaries) — `branchesDropped=1` (a sibling branch lost).
- gl0 round=8 (`19:23:32.359`) accepts `scSnapshots=1` (only one mg's first incremental).
- gl0 rounds 9-882: `scSnapshots=1` always (only `DAG2n5fYb4Y8…` continues to advance).
- Three other metagraphs (`DAG4uKoNMPeZ…`, `DAG7h4WBTFU9…`, `DAG43BYbWA6g…`) each generate 18,000+ `📦 Orphan-buffered … (parent not yet admitted)` events and never reach the committee gate.

### v25c PASS comparison
v25c PASS spent 0 s on Multi-metagraph (`test-runs/iter-v25c-s0-validation.log:2439`), meaning gl0 had already observed all 4 metagraphs in `lastCurrencySnapshots` by the time the test fired. Identical topology, identical CLI args, just a different commit (and possibly different round-4 branch survivor).

### Per-node inspection
gl0 logs across all 8 nodes show **0 chain-link rejections for `DAG2n5fYb4Y8…`** with `gl0.lastHash=000000000000` (always populated with a real prior). The 3 missing metagraphs see the empty-hash rejection across every node.

Per-metagraph admission tally (gl0-0):
- `DAG2n5fYb4Y8…`: 536 committee gate events, 49 attestations sent, 487 received, 210 admissions. **Healthy.**
- `DAG43BYbWA6g…`: 0 committee gate events. **Never reached the gate.**
- `DAG4uKoNMPeZ…`: 0 committee gate events. **Never reached the gate.**
- `DAG7h4WBTFU9…`: 0 committee gate events. **Never reached the gate.**

Per-metagraph orphan buffer entries (gl0-0):
- `DAG2n5fYb4Y8…`: 14,666
- `DAG43BYbWA6g…`: 18,395
- `DAG4uKoNMPeZ…`: 19,488
- `DAG7h4WBTFU9…`: 18,709

The orphan buffer is cap=256 (`MetagraphOrphanBuffer.scala:100`). With 18k entries per mg pouring in, the buffer is constantly evicting under FIFO; only 2-7 successful `drainChildren` events were logged across each node over the full 110-min run.

### Root-cause hypothesis (evidence trail)

The chain is:
1. ml0 (post-genesis) starts producing incremental binaries chained off its genesis binary hash `H_g`.
2. ml0 broadcasts to gl0 via sidecar gossip.
3. On the gl0 receiver, `makeMetagraphBinaryProcessor` (`NakamotoSyncDaemon.scala:1581-1654`) calls `resolveParent(mg, H_g)`:
   - First checks `orphanBuffer.lookupAdmittedOrd` (admission cache, in-memory). For the 3 missing mg, **no admission ever happened** → returns `None`.
   - Falls through to `MetagraphParentOrdinalResolver.resolve` (`MetagraphParentOrdinalResolver.scala:51-101`), which reads `lastStateChannelSnapshotHashes[mg]` via the overlay `GlobalStateReader`.
4. The reader returns `None` (case `None` at `MetagraphParentOrdinalResolver.scala:96`) → `processBytes` orphan-buffers the binary.
5. The orphan buffer is bounded at 256 entries; the binary may be FIFO-evicted within seconds.
6. Meanwhile, gl0's consensus pipeline calls `GlobalSnapshotStateChannelAcceptanceManager.accept` (`GlobalSnapshotStateChannelAcceptanceManager.scala:68`) with `priorLastStateChannelSnapshotHashes` materialized from MPT via `GlobalSnapshotAcceptanceManager.scala:918-933`:
   ```scala
   indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastStateChannelSnapshotHashes)
   addrSet <- mpt.get[SortedSet[Address]](indexKey).map(_.getOrElse(SortedSet.empty[Address]))
   keys = addrSet.toList.map(addr => GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastStateChannelSnapshotHashes))
   values <- mpt.getMany[Hash](keys)
   ```
7. If `addrSet` does NOT contain `mg` (the missing 3), then `priorLastStateChannelSnapshotHashes` does NOT include `mg`, and `acceptForAddress` (`GlobalSnapshotStateChannelAcceptanceManager.scala:87`) falls back to `Hash.empty=000000000000`:
   ```scala
   priorLastStateChannelSnapshotHashes.getOrElse(address, Hash.empty)
   ```
8. The binary's `lastSnapshotHash` (its genesis hash `H_g`) does not equal `Hash.empty`, so `onlyPossibleReferences` rejects it as chain-impossible: **chain-link rejection observed**.

**The crucial question: why is `addrSet` incomplete?**

It's because **MultiBranch overlay reads racing finalization can return a partially-populated index entry**. At gl0 round-4, `branchesDropped=1` (the gl0-0 log shows this). The branch that survived had `keysApplied=27`. The previous fix commit `46d505c55` ("fix(mpt): GSI fallback for empty MPT addrSet in GSAM priorLast{ScHashes,CurrencySnapshots} (#113)") documented the exact race:

> Under MultiBranch, a transient chain-walk race (e.g. a sibling-branch eviction dropping an ancestor that held the index sidecar's latest write) can return an empty `addrSet` for one accept() call even when the keyset is fully populated in `lastSnapshotContext`.

The fix was a defensive union of `lastSnapshotContext.lastStateChannelSnapshotHashes.keySet` into the materialized map; for any address missing from MPT, fall back to the GSI's hash. This fix was reverted in commit `abb36f723` on May 7. The revert message gives no rationale. The behavior the fix addressed is **what we are observing**.

The `ActiveAddressIndex` writes use a read-modify-write pattern (`GlobalStateConverter.scala:100-117`):
```scala
existing <- store.get[SortedSet[Address]](key).map(_.getOrElse(SortedSet.empty[Address]))
merged = (existing ++ added) -- removed
_ <- if (merged.isEmpty && existing.nonEmpty) store.remove(key)
     else if (merged.nonEmpty && merged != existing) store.insert[SortedSet[Address]](key, merged)
     else Async[F].unit
```
A RMW under MultiBranch on the *base* store could be stomped if a competing branch's write loses the merge — but the production code routes through `mpt` (the overlay), so the more likely failure is: branch B reads the prior `addrSet={mg1, mg2}` against base, adds mg3, writes back `{mg1, mg2, mg3}` into branch B. Branch A reads against base, adds mg4, writes back `{mg1, mg2, mg4}`. Round-4 finalizes branch A; mg3 is now missing from the canonical base. Subsequent rounds read `addrSet={mg1, mg2, mg4}` from MPT and never recover mg3.

### Proposed fix

**Reapply `46d505c55`** (or an equivalent) at `GlobalSnapshotAcceptanceManager.scala:912-933` and `:943-967`. The fix is:
1. Read `addrSet` from MPT (current behavior).
2. Union with `lastSnapshotContext.lastStateChannelSnapshotHashes.keySet` (GSI fallback).
3. For any address present in the GSI but missing from MPT (i.e., where `mpt.get(metagraph(addr, …))` returned None for an address that the GSI has), fall back to the GSI value.
4. Log at WARN when the GSI fallback fires (so we can observe how often the race actually happens).

Equivalent code for the `priorLastCurrencySnapshots` block must be reapplied at lines 943-967.

The revert rationale should be revisited; if there was a real reason to revert, the right answer may be a different fix (e.g., make the sidecar index write *transactional* with the per-address payload writes, so they can't partially survive a branch eviction). But until that's known, the union is the lowest-risk patch.

### File:line citations
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala:912-967` (the bug surface)
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala:87` (the `Hash.empty` default)
- `modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateConverter.scala:100-117` (the RMW pattern)
- Revert commit: `abb36f723` (revert of `46d505c55`)

### Effort
2-4 hours including:
- (45 min) reapply the union pattern at the two sites
- (90 min) write a regression test that exercises a MultiBranch overlay with two concurrent address-set writes and asserts the union recovers the right keyset
- (30 min) run the targeted MPT suite and the e2e at 4-mg to confirm

If the original revert had a legitimate reason that we don't yet know about, add another 4-8 hours for the deeper fix (transactional sidecar+payload). Investigate by reading the issue or PR that prompted the revert (`abb36f723` commit message has no body — needs git/email/PR search).

### Next investigation step
**Before landing the fix:** find the original justification for the revert in `abb36f723`. Options:
- `git log --all --grep "abb36f723"` for cross-references
- Search `~/.claude/projects/-home-euler-repos-tessellation-nakamoto/memory/*.md` for "46d505c5" or "GSI fallback"
- Check whether the revert was driven by a different test regression. If so, find that regression's test path and verify the new fix doesn't reintroduce it.

---

## Mode 4: Token-locks verifyInL0 600-attempt timeout

### Log signature
```regex
Global L0 verification attempt \d+ failed: No active token locks found for address DAG[A-Za-z0-9]+ in Global L0
```

### When it appears
The TokenLock workflow runs after Currency / Metagraph rewards (around 2872 s into the run in `iter-g1-full-e2e.log`). The test posts a token-lock to cl1 (`http://localhost:9300/token-locks`), confirms cl1 acceptance, confirms cl0 acceptance, then polls gl0 every second up to 600 times waiting for the token-lock to surface in gl0's `activeTokenLocks` index.

### v25c PASS comparison
v25c PASS completed `Token lock tests` in 1740 s (29 min) — well within budget.

### Per-node inspection
Cannot trace this from the failed-run gl0 logs because the gl0-0 logs in `nodes/` correspond to the *v2* run that failed at Multi-metagraph (i.e., never reached the TokenLock test). However, the symptom is identical to Mode 3's symptom: a metagraph's binaries chained off genesis cannot land in `gl0.activeTokenLocks` because the binaries themselves are chain-link-rejected at gl0.

### Root-cause hypothesis
Same as Mode 3. The TokenLock submission carries `currencyId=DAG4GPGTxooKvQqEKJSCgNCtyZcWZ4TBu4dsSEK5`. On this run, that mg was one of the "missing 3" — its post-genesis SC binaries never get past `priorLastStateChannelSnapshotHashes.getOrElse(addr, Hash.empty)` at gl0. The token-lock IS in the cl0 binary (which is why the previous-step "TokenLock transaction found in Currency L0" check passes), but the SC binary carrying that cl0 state never reaches gl0's MPT, so `gl0.activeTokenLocks` stays empty for this address.

The `011663b64` bump from 300 → 600 attempts (5 → 10 min) doesn't address the root cause — gl0 is permanently stuck for this metagraph for the rest of the run.

### Proposed fix
Subsumed by Mode 3. Once Mode 3 is fixed, the TokenLock binaries propagate normally and the test passes well under 300 attempts. Recommend reverting `011663b64` after Mode 3 lands (run 2-3 e2e iterations to confirm the previous 300-attempt budget is enough; if not, it would be evidence of a separate liveness issue that warrants its own analysis).

### File:line citations
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala:918-933` (priorLastStateChannelSnapshotHashes materialization — same bug)
- Token-lock test script (path inferred, not loaded for this analysis)

### Effort
0 (subsumed by Mode 3).

### Caveat (epistemic)
I am inferring Mode 4 is Mode 3 from the *symptom shape*, not from a fresh log of the g1 run. The gl0 logs in `nodes/0/gl0-logs/gl0-run.log` are from a *different* run that failed earlier. To confirm: re-run the TokenLock-only path with gl0 logs preserved, and inspect for `SCAcceptance Chain-link rejection … addr=DAG4GPGTxooK… gl0.lastHash=000000000000`. If found, confirmed same bug. If not, this needs its own analysis.

---

## Cross-cutting observations

### The 4 modes are 1 bug
Modes 2, 3, 4 are all surfaced by the same upstream defect: gl0's per-metagraph chain advancement gets stuck the moment the `ActiveAddressIndex` keyset for `LastStateChannelSnapshotHashes` loses a metagraph entry. Mode 1 is not a bug at all.

- **Mode 3** is the direct manifestation: gl0 chain-link-rejects every post-genesis binary from "lost" mg.
- **Mode 2** is the downstream effect when cl1 reports `currentEpochProgress=1` because cl0 never received a populated `globalSyncView` from a gl0-finalized snapshot covering its metagraph.
- **Mode 4** is another downstream effect when the test polls for a token-lock that's stranded in cl0 because the SC binary carrying it can't reach gl0's `activeTokenLocks`.

### The fix has the highest leverage in the codebase right now
Restoring (or replacing) the union from `46d505c55` is a ≤50-LOC change that addresses 3 of the 4 observed flake symptoms. It is also low-risk: under steady-state MultiBranch (or under Passthrough), the union is a no-op because the MPT keyset already matches the GSI keyset. The fix only fires in the precise race window that we observe; it's defensive by construction.

### v22→v23→v24→v25c sequence is a coin flip
Per memory, v22 first hit this at 8gl0+4mg+4shards, v23 PASS, v24 FAIL (VRF-random), v25c PASS, and current 3 runs all FAIL. The "VRF-random" hypothesis for v24 was likely correct — VRF leadership at gl0 round-4 determines *which* leader's proposal wins, which determines *which* of the racy branches wins finalization, which determines *which* metagraph entries survive in `ActiveAddressIndex`. Flake direction (1/4, 2/4, 3/4) is also non-deterministic per the same coin flip. We'd expect ~half of runs to be 4/4 (PASS) and ~half to be 1-3/4 (FAIL).

### Why the orphan buffer doesn't paper over this
The orphan buffer in `MetagraphOrphanBuffer.scala` is the correct response for "ml0 races ahead of gl0's GSI catch-up window". It assumes that, once gl0 admits binary B, the resolver wrapper can answer `parentOrdinalFor(mg, hash(B))` via the in-memory admission cache, and drained children of B can proceed.

But Mode 3 is **not** a GSI catch-up race — it's a **permanent** GSI dropout. Once `mg3` is missing from `addrSet`, no future round will add it back unless a write happens for `LastStateChannelSnapshotHashes[mg3]`, which can't happen because every incoming binary for `mg3` is chain-link-rejected before reaching the write. The orphan buffer fills up to 256, FIFO-evicts, and the bug is permanent.

### Why the v23/v25c PASSes worked
On those runs, the round-4 branch survivor happened to include all 4 metagraph entries in `ActiveAddressIndex` (most likely because the dropped sibling branch hadn't done its own write of the index yet, or had done a strictly-subset write that the survivor's write contains). It's a random win.

---

## Recommended fix sequence

### Priority 1 (highest leverage × lowest effort)
**Reapply the `46d505c55` GSI fallback in `GlobalSnapshotAcceptanceManager.scala:912-967`.** This fixes Modes 2, 3, and 4 simultaneously.

**Next investigation step before landing**: find the original revert rationale (`git show abb36f723` body, search for related issues/discussions). If the revert was driven by a different regression, characterize that regression and design the fix to avoid it. If no rationale is recoverable, apply the union as-is and run the full e2e at 8gl0+4mg+4shards three times to confirm zero flake. If the rationale was "this masks a deeper bug", the deeper bug is exactly what we are observing now, so the fix is still correct — but the test budget should also cover ChainSync replay.

**Validation cost**: 3× e2e runs at 8gl0+4mg+4shards ≈ 90 min each ≈ 4.5 h cluster time.

### Priority 2 (defense in depth)
**Wire `AllowSpendService.offer` (and the symmetric `TokenLockBlockService.offer` at `dag-l1/.../TokenLockBlockService.scala:54`) to consult `lastNGlobalSnapshotStorage.getLatestSync` as a fallback when `cis.globalSyncView=None`.** This decouples cl1's epoch-progress view from the cis embedding, so even a malformed cis (or any future bug producing `globalSyncView=None`) doesn't take down POST endpoints. 1 hour wiring + 1 hour regression test = 2 hours.

**Next investigation step**: confirm this doesn't introduce a different epoch-progress source-of-truth disagreement between cl1 nodes (e.g., one cl1 hits `cis.globalSyncView`, another hits `lastNGlobalSnapshotStorage`).

### Priority 3 (test budget restoration)
**Revert `011663b64` (back to 300 attempts)** once Modes 2-4 are fixed. Run 2 e2e iterations to confirm.

### Priority 4 (cosmetic)
**Downgrade the `Content REJECTED` log level from WARN to INFO** when followed by a reorg success within 5 s. Pure ergonomic improvement; helps triage. 5 min.

### Priority 5 (deeper hardening, separate workstream)
**Make the `ActiveAddressIndex` sidecar write transactional with the corresponding per-address payload writes** so they can't partially survive a branch eviction. This is the "right" fix for the underlying race; the Priority-1 union is a paper-over. Estimate 2-3 days. Out of scope for the current debug pass — schedule for after slashing + KES10 land if the union proves brittle.

---

## Open questions for the user

1. Is there a record (issue, PR, discussion) of why `46d505c55` was reverted on May 7?
2. The g1-full-e2e.log corresponds to a different run than the preserved `nodes/N/gl0-logs/gl0-run.log` (those are from g1-g4-v2). Are the cl1 / cl0 logs from the g1 run preserved anywhere we can inspect, to confirm Mode 2's hypothesis directly?
3. The fix should be straightforward, but **the underlying RMW race is real**. Are we comfortable with the union as a permanent workaround, or do we want the Priority-5 transactional-sidecar fix scheduled in the near term?
