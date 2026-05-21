# Mode 2 (cl1 globalSyncView stuck) — Root Cause Analysis

Branch `feature/serde-typeclass-shim` HEAD `0123411a4`
Topology 8 gl0 + 4 metagraphs (2 ml0 per mg) + K_target=4 committee-gate
Reference runs
- `test-runs/iter-flake-fix-v1.log` — Mode 2 manifests (post-fix-1, with Mode 3 fixed) at workflow Currency allow-spend
- `test-runs/iter-g1-g4-full-e2e.log` — earlier Mode 2 instance
- `test-runs/iter-v25c-s0-validation.log` — PASS reference (no Mode 2)

## Symptom

Exact log signature, observed in two failing runs:

```
data: { errors: [ { message: 'TooFarLastValidEpochProgress{epochProgress=516,currentEpochProgress=1}' } ] }
status: 400
POST http://localhost:9300/allow-spends
```

(`epochProgress=518,currentEpochProgress=1` in `iter-g1-g4-full-e2e.log:7477`; `epochProgress=516,currentEpochProgress=1` in `iter-flake-fix-v1.log:7604`.)

The number `currentEpochProgress=1` is invariant across both runs and is **not** the `EpochProgress.MinValue=0` floor. This rules out the framing "cl1's `cis.globalSyncView=None` → `getOrElse(EpochProgress.MinValue)`". The Mode 2 cis is **`Some(GlobalSyncView(_, _, EpochProgress(1)))`** — a stale view stuck at gl0 ord=1, not None.

The `=1` value is exactly `gl0.genesis.epochProgress.next` (`modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshot.scala:99`). gl0 ord=1 is the first incremental snapshot, with `epochProgress = EpochProgress.MinValue.next = EpochProgress(1)`.

This run had Mode 3 fixed (`85417689d`): `iter-flake-fix-v1.log:5345` reports `Multi-metagraph test PASSED (K=4)` at 1092s. gl0 had all 4 metagraphs visible. The TooFar happened at ~3986s during Currency allow-spend, ~48 min later. Mode 2 is NOT downstream of Mode 3 in this run — they are independent.

## Producer trail

### 1. Where `globalSyncView` is set on `CurrencyIncrementalSnapshot`

- `modules/shared/src/main/scala/io/constellationnetwork/currency/schema/currency.scala:239` — field declaration on `CurrencyIncrementalSnapshot`.
- `modules/shared/src/main/scala/io/constellationnetwork/currency/schema/currency.scala:343-345` — `CurrencySnapshot.mkGenesis` stamps `globalSyncView` from `latestGlobalSnapshot` at metagraph genesis time. `Genesis.create` (`modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/programs/Genesis.scala:133-135`) calls `l0Service.pullLatestSnapshot` and passes it to `mkGenesis(balances, dataApplicationPart, latestSnapshot.some)`. This is the bake-in site previously implicated in #217.
- `modules/shared/src/main/scala/io/constellationnetwork/currency/schema/currency.scala:369` — `mkFirstIncrementalSnapshot` (cl0 ord=1) propagates `genesis.globalSyncView` verbatim into ord=1's `globalSyncView` field.
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManager.scala:339-349` — `CurrencySnapshotAcceptanceManager.accept` computes `globalSyncView` for every cl0 ord >= 2:
  ```scala
  globalSyncView = forcedGlobalSyncView.getOrElse(
    maybeLastGlobalSyncView
      .filter(_.ordinal >= lastSyncGlobalSnapshot.ordinal)
      .getOrElse(
        GlobalSyncView(
          lastSyncGlobalSnapshot.ordinal,
          lastSyncGlobalSnapshot.hash,
          lastSyncGlobalSnapshot.epochProgress
        )
      )
  )
  ```
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotCreator.scala:287-296` — the `globalSyncView.some` write into the artifact is gated by `lastGlobalSnapshotToCheckFields < tessellation3MigrationStartingOrdinal`. In dev environment `tessellation3MigrationStartingOrdinal = 0` (`modules/node-shared/src/main/resources/application.conf:166-171`), so the gate is always false and `.globalSyncView.some` is always set. **Cl0 never emits `globalSyncView=None` in dev** — the Mode 2 cis carries `Some(GlobalSyncView(epochProgress=1))`.

### 2. How `lastSyncGlobalSnapshot` is selected (the stale-view pivot)

`CurrencySnapshotAcceptanceManager.accept:300-320` — priority chain for the GL0 sync point cl0 uses to stamp each new currency snapshot:

```scala
lastGlobalSnapshots <- lastNGlobalSnapshotStorage.getLastN

ordinalToFetchGlobalSnapshot <- forcedGlobalSyncView
  .map(_.ordinal)
  .fold(
    maybeSnapshotOrdinalSync                          // (A) peer GlobalSnapshotSync quorum, minus syncOffset=2
      .orElse(maybeLastGlobalSyncView.map(_.ordinal)) // (B) previous cl0 snapshot's view ordinal
      .filter(_ =!= SnapshotOrdinal.MinValue)
      .fold(fallbackOrdinal.pure[F])(_.pure[F])       // (C) lastUnsyncGlobalSnapshot.ordinal — cl0's actual gl0 head
  )(_.pure[F])

lastSyncGlobalSnapshot <- lastGlobalSnapshots.find(_.ordinal === ordinalToFetchGlobalSnapshot) match {
  case Some(value) => value.pure[F]
  case None        => globalSnapshotOps.getGlobalSnapshotWithRetry(...)
}
```

Three-way priority. `(B)` filters out `MinValue=0` but keeps `ord=1`. The bug is: once `(B)` is populated with the genesis-inherited `Some(GlobalSyncView(ord=1, _, epochProgress=1))`, the priority chain **prefers (B) over (C)** — it never advances to cl0's real gl0 head (`lastUnsyncGlobalSnapshot.ordinal`) unless `(A)` (peer quorum) lifts the floor first.

### 3. Where the gl0 → cl0 → cl1 propagation lives

- **gl0 → cl0**: `modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/StateChannel.scala:201-258` (`handleIncrementalSnapshot`) sets `storages.lastSyncGlobalSnapshot.set(snapshot, context)` and `sharedStorages.lastNGlobalSnapshot.set(...)` on every gl0 advancement. Line 226 also calls `sendGlobalSnapshotSyncConsensusEvent(snapshot)` which enqueues a self-`GlobalSnapshotSync` into the cl0 consensus engine.
- **GlobalSnapshotSync emission**: `StateChannel.scala:106-135` constructs `GlobalSnapshotSync(lastSentRef.ordinal, snapshot.ordinal, snapshot.hash, session)`, signs it, and enqueues as a consensus event. Each cl0 ml0 node fires its own self-sync per gl0 incremental observed.
- **Peer sync quorum on cl0**: `MessageValidationOpsManager.acceptGlobalSnapshotSyncs` (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/MessageValidationOpsManager.scala:115-152`) accepts validated peer syncs into `contextUpdate: SortedMap[PeerId, Signed[GlobalSnapshotSync]]`. `CurrencySnapshotAcceptanceManager:277-298` then derives `maybeSnapshotOrdinalSync` = mode of `peers.values.map(_.globalSnapshotOrdinal)` minus `syncOffset=2` (path `(A)`).
- **cl0 → cl1**: cl1 receives finalized gl0 snapshots via `GlobalSnapshotAlignment.pullFinalityGated`. Each finalized gl0 snapshot's `lastCurrencySnapshots.get(identifier)` carries the cl0-produced `CurrencyIncrementalSnapshot`. `CurrencySnapshotProcessor.processCurrencySnapshots` (`modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/domain/snapshot/programs/CurrencySnapshotProcessor.scala:224-322`) calls `processAlignment` which calls `lcss.set(snapshot, info)` — this is what populates cl1's `lastSnapshot` ref with the cis whose `globalSyncView` cl1's AllowSpendService then reads.

### 4. Why `globalSyncView.epochProgress` could be `1` after cluster has advanced

Two stable conditions for the stuck-at-ord=1 state:

**Condition A — peer `GlobalSnapshotSync` quorum never converges on a higher ordinal.** `maybeSnapshotOrdinalSync` requires `peers.values.map(_.globalSnapshotOrdinal).groupBy(identity).maxByOption` to return Some. With small per-metagraph cl0 quorum (2-3 ml0 nodes per mg in this topology), if peer syncs are racy, transient, or replaced before consensus folds them in, the mode of the peer map can be empty for several consecutive rounds, holding the path `(A)` at None.

**Condition B — the priority `(B) over (C)` clamp persists once seeded.** The seed `Some(GlobalSyncView(ord=1, epochProgress=1))` comes from genesis via `mkFirstIncrementalSnapshot`. The `globalSyncView` write at acceptance manager `:339-349` is a monotonic clamp:
```scala
maybeLastGlobalSyncView
  .filter(_.ordinal >= lastSyncGlobalSnapshot.ordinal)
  .getOrElse(GlobalSyncView(lastSyncGlobalSnapshot.ordinal, ...))
```
If the priority chain selected `ordinalToFetchGlobalSnapshot = 1` (via path `(B)`), then `lastSyncGlobalSnapshot.ordinal = 1`. The clamp condition `maybeLastGlobalSyncView.ordinal=1 >= lastSyncGlobalSnapshot.ordinal=1` is **true**, so it **reuses the same stale view** rather than constructing a fresh one. This is a strict fixed point — once stuck at ord=1, the producer cannot escape it without external help (peer-sync quorum on path `(A)`).

**The (B) preference is the root cause.** It exists for a reason — Cardano-style "monotonic last-known-finalized" semantics for the SC binary's globalSyncView referent. But the implementation conflates two different uses:
- *Forcing the snapshot to embed a finalized gl0 hash* (correct, makes the binary reorg-safe at gl0).
- *Choosing which gl0 epochProgress to expose to downstream consumers* (incorrect when path `(A)` is empty — the priority chain should prefer the current local gl0 head over the genesis-inherited view).

In Condition A, the producer's *actual* gl0 follower (`lastUnsyncGlobalSnapshot.ordinal` via path `(C)`) has advanced past ord=1 — but the priority chain never sees it because path `(B)` is selected first.

### Evidence summary

- Mode 2 hits at `currentEpochProgress=1` exactly. `EpochProgress.MinValue=0` and `EpochProgress(1) = MinValue.next`. The constant-`=1` symptom across two runs rules out random noise.
- Mode 3 was fixed in `iter-flake-fix-v1.log` (Multi-meta PASSED at 1092s); the failure is at 3986s. Mode 2 ≠ downstream-of-Mode-3 in this run.
- `iter-v25c-s0-validation.log` PASS does not exhibit `TooFar`; the difference is that v25c's peer GlobalSnapshotSync quorum landed early enough to lift `maybeSnapshotOrdinalSync` before the workflow probed allow-spends. This is a race; not a deterministic failure mode.

## Diagnosis (root cause hypothesis with evidence)

The cl0 (currency-l0) snapshot acceptance manager prefers `maybeLastGlobalSyncView.ordinal` (path B) over `lastUnsyncGlobalSnapshot.ordinal` (path C, the cl0's actual gl0 head) when peer `GlobalSnapshotSync` quorum (path A) is absent. The genesis-inherited `Some(GlobalSyncView(ord=1, epochProgress=1))` becomes a strict fixed point under the monotonic clamp at `:339-349`. The producer never advances its globalSyncView past gl0 ord=1 until peer quorum materializes — which can take several rounds in 2-3 node per-metagraph cl0 cohorts.

Downstream, cl1 reads `cis.globalSyncView.map(_.epochProgress)` (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/AllowSpendService.scala:48`) and surfaces `currentEpochProgress=1` to the AllowSpend validator. The validator rejects with `TooFarLastValidEpochProgress(516, 1)` because `516 > 1 + max=500` (`modules/node-shared/src/main/resources/application.conf:152`).

The bug is **producer-side stale-view**, not consumer-side missing-fallback. The fix #2 attempt (consumer-side fallback to `lastNGlobalSnapshotStorage`) couldn't have papered over this because cl1's `cis.globalSyncView` was `Some(epochProgress=1)`, not `None` — so the `getOrElse` branch wasn't taken and the fallback was never consulted.

(That re-frames the original `E2E-FLAKE-ANALYSIS.md` Mode 2 hypothesis: "cl1 emits cis with `globalSyncView=None`" is wrong. The cis has `Some(epochProgress=1)`, sourced from cl0's stuck-at-genesis priority chain. The consumer-side fix #2 would have papered over the `None` case but does nothing for `Some(epochProgress=1)` — even before its own currency-tx-batch regression, fix #2 could not have addressed the actually observed symptom.)

## Proposed fix (producer-side)

Two complementary changes at the priority chain in `CurrencySnapshotAcceptanceManager.accept`:

### Primary — reorder priority chain to prefer producer's actual gl0 head over stale prior view

`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManager.scala:308-315` currently:

```scala
ordinalToFetchGlobalSnapshot <- forcedGlobalSyncView
  .map(_.ordinal)
  .fold(
    maybeSnapshotOrdinalSync
      .orElse(maybeLastGlobalSyncView.map(_.ordinal))     // (B) — stale prior
      .filter(_ =!= SnapshotOrdinal.MinValue)
      .fold(fallbackOrdinal.pure[F])(_.pure[F])            // (C) — current head
  )(_.pure[F])
```

Replace with: peer-sync (A) takes priority as today, but when (A) is absent, prefer `(C) = max(fallbackOrdinal, maybeLastGlobalSyncView.ordinal)` over `(B) = maybeLastGlobalSyncView.ordinal` alone. That is:

```scala
ordinalToFetchGlobalSnapshot <- forcedGlobalSyncView
  .map(_.ordinal)
  .fold(
    maybeSnapshotOrdinalSync.fold(
      // No peer-sync quorum yet. Prefer producer's actual gl0 head over the stale
      // genesis-inherited view; keep the prior view as a lower bound to preserve
      // monotonicity if (rare) the local follower regressed.
      val priorOrdinal = maybeLastGlobalSyncView.map(_.ordinal).getOrElse(SnapshotOrdinal.MinValue)
      (if (fallbackOrdinal >= priorOrdinal) fallbackOrdinal else priorOrdinal).pure[F]
    )(_.pure[F])
  )(_.pure[F])
```

This addresses the root cause directly: once `lastUnsyncGlobalSnapshot.ordinal` (the cl0-local gl0 head, fed by `lastGlobalSnapshotStorage.getCombined` at `:244-248`) has advanced past ord=1, the producer immediately starts stamping cis with a fresh `GlobalSyncView` reflecting the current epoch progress.

### Secondary — break the monotonic-clamp fixed point at `:339-349`

After fix-1, the clamp `maybeLastGlobalSyncView.filter(_.ordinal >= lastSyncGlobalSnapshot.ordinal).getOrElse(...)` is still a strict inequality preserver, but with `lastSyncGlobalSnapshot.ordinal = fallbackOrdinal` it monotonically moves forward each round. The clamp's intent (preserve a finalized gl0 referent across reorgs) is still served. No change needed here.

### Out of scope for the producer-side fix

The behavior of the `GlobalSnapshotSync` quorum (path A) is correct as designed — under steady-state, peer-syncs would supply a higher-quality estimate than the local follower (committee consensus on the SC binary's referent ordinal). The bug is only that the producer **stalls at ord=1 when (A) is empty** rather than falling forward to (C). Reordering (B) vs (C) does not change (A)'s semantics.

## Why this fix won't have fix #2's blast radius

Fix #2 (commit `ea1613e48`, reverted in `0123411a4`) made two consumer-side changes:
1. **`AllowSpendService.offer`** — added `lastNGlobalSnapshotStorage` fallback when `cis.globalSyncView=None`. *This was the intended fix.* It's a read-only HTTP endpoint, low blast radius. Even if it had worked, however, it couldn't have addressed the observed `=1` symptom (which is `Some`, not `None`).
2. **`AllowSpendBlockService.accept`** — same fallback, but this is in the dag-l1 **block acceptance pipeline**, not the HTTP endpoint. It runs on every incoming AllowSpendBlock during dl1 consensus. Two consecutive runs (`iter-flake-fix-v2.log`, `iter-flake-fix-v3.log`) failed deterministically at currency-tx batch transfer with `Conflict{ordinal=1, existingHash=<X>, newHash=<Y>}` — `existingHash` stayed CONSTANT across retries (39c9e... in v3, c2d4c... in v2) while `newHash` varied. Per `project_flake_fix_2_regression`, the regression mechanism is the wiring change in `Services.scala` threading `lastNGlobalSnapshot` through to AllowSpend services possibly altered cl1's boot ordering OR the higher `lastGlobalEpochProgress` shifted some downstream computation that affects basic transaction-ref chaining at ord=1.

The producer-side fix proposed here:
- Touches **one file** (`CurrencySnapshotAcceptanceManager.scala`) at **one location** (`:308-315`), with **no constructor signature change**.
- Does **not** touch `Services.scala` wiring for either currency-l1 or dag-l1 — no boot-ordering risk, no constructor parameter shifts, no chain of trait-change propagation.
- Does **not** touch the block-acceptance pipeline. AllowSpendBlockService, TokenLockBlockService, and the per-block validators are untouched — the currency-tx batch transfer regression mechanism cannot recur.
- Affects **only the producer-side computation of one field** (`globalSyncView`) on cl0's emitted CurrencyIncrementalSnapshot. Downstream consumers (cl1, gl0 receiver-side acceptance manager via `forcedGlobalSyncView`, validator's `expected.globalSyncView`) all continue to receive a *better-quality* value: a `GlobalSyncView` reflecting the current gl0 head rather than the genesis-inherited ord=1.
- Is **backward-compatible with gl0's `Forced globalSyncView hash mismatch` check** (`CurrencySnapshotAcceptanceManager.scala:322-332`). gl0 verifies the embedded ordinal's hash matches its own local view at that ordinal. Under Mode 2's `fallbackOrdinal = lastUnsyncGlobalSnapshot.ordinal`, the chosen ordinal is from cl0's *finality-gated* gl0 follower (`StateChannel.pullFinalityGated`), so it is a finalized ordinal whose hash gl0 will agree on. No new `Forced globalSyncView hash mismatch` risk.

## Why fix #2 failed (postmortem)

The producer-side framing was missed entirely. The original `E2E-FLAKE-ANALYSIS.md` Mode-2 framing inferred `globalSyncView=None` from the `EpochProgress.MinValue` fallback path in `AllowSpendService.offer:48`, but never actually verified that hypothesis against cl1 logs. The `=1` constant in the error string is the load-bearing evidence — it disambiguates `Some(epochProgress=1)` from `None → MinValue=0`. The fix #2 attempt then tried to fix what it interpreted as the symptom (consumer falling back to MinValue) without confirming whether the cis actually had `None`.

The blast radius came from extending the fallback to `AllowSpendBlockService.accept` (the dag-l1 block acceptance pipeline). The Services.scala wiring change threaded `lastNGlobalSnapshotStorage` into the block service constructor — and at cl1 ord=1, the block service's `lastGlobalEpochProgress` becomes `lastNGlobalSnapshotStorage.get.signed.value.epochProgress` (i.e., gl0's current view) instead of `EpochProgress.MinValue`. Block acceptance at ord=1 then runs against a different `lastGlobalEpochProgress` than expected by the test client's batch — which (per `project_flake_fix_2_regression`) leaves `existingHash` for the conflict resolution constant while the test client's retries produce varied `newHash`es, producing the deterministic `Conflict{ordinal=1, ...}` pattern.

The lesson: changing `lastGlobalEpochProgress` in the block-acceptance pipeline has subtle downstream effects on transaction-ref chaining at the earliest ordinals. Any future fix in this area should leave the block-acceptance pipeline untouched and operate purely on the producer (cl0) side.

## Validation plan

### Targeted test (recommended before full e2e)

Add a regression test at `modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManagerSuite.scala` (create if absent) that exercises the priority chain with:
- Inputs: `lastArtifact.globalSyncView = Some(GlobalSyncView(ord=1, h1, EpochProgress(1)))`, `lastNGlobalSnapshotStorage` populated with gl0 ord=100, `globalSnapshotSyncsForAcceptance = List.empty` (no peer-sync quorum).
- Expected: produced cis carries `globalSyncView.epochProgress = <epochProgress at gl0 ord=100>`, not `EpochProgress(1)`.

Stub: `lastGlobalSnapshotStorage.getCombined` returns `Some((gl0Ord100Snapshot, gl0Ord100State))`, `lastNGlobalSnapshotStorage.getLastN` returns `List(gl0Ord100Snapshot)`, `getGlobalSnapshotByOrdinal(100)` returns `Some(gl0Ord100Snapshot)`.

This test would have failed under the current code; it must pass after the fix.

### Full e2e validation

Run 3 consecutive e2e iterations at 8gl0+4mg+4shards using the standing harmonic stake distribution. Acceptance criteria:
- Zero `TooFarLastValidEpochProgress` errors in any cl1's HTTP response log.
- Multi-metagraph K=4 PASS (regression-safe vs. fix #1).
- TokenLock expiration test PASS within 300-attempt budget after fix lands and `011663b64` (300→600 bump) is reverted (per `E2E-FLAKE-ANALYSIS.md` Priority 3).
- Currency-tx batch transfer at ord=1 — sanity-check no `Conflict{ordinal=1, ...}` regression. This is the failure mode that killed fix #2; verify the producer-side fix doesn't share the same trigger.

### Per-iteration diagnostic to capture

If a Mode 2 still hits, add a debug log at `CurrencySnapshotAcceptanceManager.scala:339-349` printing the inputs to the priority chain:

```scala
_ <- logger.debug(
  s"cl0 globalSyncView priority: forced=${forcedGlobalSyncView.show} " +
    s"peerSync=${maybeSnapshotOrdinalSync.show} prior=${maybeLastGlobalSyncView.map(_.ordinal).show} " +
    s"fallback=${fallbackOrdinal.show} chosen=${ordinalToFetchGlobalSnapshot.show}"
)
```

This makes the priority-chain trace observable; without it, the failure mode is invisible at INFO level.

## Open questions for the user

1. **Path (A) → (C) crossover** — could there be cases where path (A) `maybeSnapshotOrdinalSync` returns a *higher* ordinal than what cl0's actual gl0 follower has reached? If yes, the proposed fix is correct (keep A first). If no, then path A could be folded into max(A,C) for additional robustness. The current code suggests yes — peer-sync syncOffset=2 is a deliberate look-back, so peers can report ordinals cl0 has already processed plus a confidence-buffered backstep.
2. **Why is the `(B) over (C)` ordering present?** It looks structurally like a stale Cardano-style "monotonic last-known-finalized" pattern. Was this introduced for a specific safety property (e.g., resistance to cl0-local gl0 follower regression on reorg)? If so, the fix should preserve it as a lower bound via `max(fallbackOrdinal, priorOrdinal)` rather than removing it entirely. The proposed fix does this; flag if there's a stronger requirement.
3. **Should the fix gate on `tessellation3MigrationStartingOrdinal`?** In dev=0 every gate is open; in mainnet/testnet/integrationnet the gates are post-migration ordinals. The current priority chain at `:308-315` runs unconditionally. The fix should preserve that; no environment-specific gating is needed for the proposed reorder.
