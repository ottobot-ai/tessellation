# Committee-computed MPT diff → GL0 adopt-and-verify (currency state)

**Status:** DESIGN (2026-06-13). Replaces the `AdoptFromSignedFields` per-field re-derive in the sharded gl0 currency mirror. Greenfield (no wire compat per `[[feedback-greenfield-no-wire-compat]]`).

> **CORRECTION (2026-06-30):** this adopt-and-verify-at-the-**finalized-base** model (I3: *never bestTip*) is the design of record and is now being completed. NOTE on as-shipped status: the path that actually shipped at `numShards>1` kept `AdoptFromSignedFields` (it is the re-derive `deriveAdoptedCurrencyInfo`, which verifies vs `stateProof`) but then **overrode** the re-derived token fields with an ml0 *authoritative-balance push* — that override is being REMOVED so re-execution (`reExecRoot === stateProof`) becomes the **primary** token-model gate, exactly as this doc intends. The committee diff base is the FINALIZED base (`fromMptStore`), per §4/I3. See CURRENCY-APP-TOKEN-ENFORCEMENT.md + SHARDING-PRODUCTION-READINESS-PLAN.md.

## 1. The trust model (one line)

**Single executor, adopt-and-verify everywhere.** The shard committee (a gl0 subset) re-executes the metagraph's currency `accept()` **once**, anchored at the *finalized* base, and emits the resulting **MPT byte-diff** (change-set) in the checkpoint. Every other gl0 node **applies the diff** to its finalized base and verifies the resulting per-MG root against the committee-attested root. No node re-derives the state independently; no `createContext`; no per-field fallback. GL0 holds the full metagraph state (economic-security anchor); ML0 pushes, GL0 never reaches into ML0.

## 2. Why the current path is wrong (proven)

- `deriveMetagraphRoot` (the committee re-exec, `ShardCheckpointWiring:165`) runs `processCurrencySnapshots` with **empty prior** for split-safety. But `activeAllowSpends`/`activeTokenLocks` **accumulate across ordinals** — empty prior sees only the current window's blocks, never the allow-spend created in an earlier window. So the committee's derived state is window-only.
- The gl0-side `deriveAdoptedCurrencyInfo` per-field gate then compares window-derived vs the metagraph's full committed proof, mismatches, and **carries the empty genesis prior forward** — `activeAllowSpends` is empty forever (run-24/26 allow-spends failure). The "lag until roots-only" scaladoc was an agent rationalizing this, not an architecture decision.
- Mainnet has no bug here because its gl0 full-re-execs via `createContext` against the **live accumulated** state — correct but freezes under sharding (#259, the 31s global-lookup tax). We removed the freeze and lost correctness; this design restores correctness without the freeze.

## 3. Infra we reuse (already built — `domain/nakamoto/overlay/**`)

- `MptStore` holds the **finalized base** (on-disk); the overlay carries **forward** per-branch deltas (`MptOverlay`, `ChangeSet`). `bestTip = finalizedBase + branch delta`. Reorg drops the branch; the base is untouched.
- `GlobalStateReader.finalized` reads the base directly; followers are *compile-time-guarded* to finalized-only (`AcceptanceMpt:72-73,86`) — the trust rule is already enforced.
- `AcceptanceMptStateChanges` / `ChangeSet.withChanges(base, upserts, removals)` IS the byte-diff apply primitive.

## 4. The flow (correct by construction)

```
committee (gl0 node):
   base   = GlobalStateReader.finalized  (MG partition at the prior committee-finalized ordinal — deterministic, shared)
   state' = currencyAccept( base , window.blocks )      // metagraph's derivation, NO global-lookup validation; epoch from carried globalSyncView
   diff   = mptChangeSet( base , state' )                // AcceptanceMptStateChanges over the MG partition
   root'  = perMetagraphMptRoot(state')                  // == binary.stateProof (checked); this is the attested commitment
   emit ShardDerivedStateDelta { perMetagraphMptRoots = {mg -> root'}, perMetagraphStateDiff = {mg -> diff} }

gl0 followers (every node, incl. committee on validate):
   state' = ChangeSet.withChanges( finalizedBase(mg) , diff )
   require  perMetagraphMptRoot(state') === perMetagraphMptRoots[mg]    // quorum-attested root
   adopt    lastCurrencySnapshots[mg] = state'
```

- **Determinism / split-safety:** base is the *finalized* MG state (identical on every node); inputs are the signed window blocks; the derivation is `noGlobalSnapshotLookup` (epoch carried). ⇒ every committee member computes the identical `diff` and `root'`; every follower applies to the identical base ⇒ identical result. The empty-prior hack is no longer needed because the *finalized base* is itself deterministic.
- **No re-derive downstream:** followers `applyDiff`, not re-execute. The per-field gate is deleted.

## 5. Changes

1. **schema** `ShardDerivedStateDelta`: add `perMetagraphStateDiff: SortedMap[Address, ChangeSet]` (the MPT byte-diff). Keep `perMetagraphMptRoots`. `includedSnapshots` retained for the SC-tip/chain-link only (not currency derivation); revisit dropping later.
2. **committee** (`ShardCheckpointWiring` / `deriveMetagraphRoot`): anchor the derivation at the **finalized base** reader instead of empty prior; produce `(root', diff)`; assert `root' === binary.stateProof` (catch a bad input deterministically).
3. **gl0 adopt** (`GlobalSnapshotAcceptanceManager.deriveAdoptedCurrencyState`): replace `processCurrencySnapshots(AdoptFromSignedFields)` with `ChangeSet.withChanges(finalizedBase, diff)` + root verify.
4. **delete** `CurrencyAdoptionMode.AdoptFromSignedFields`, `deriveAdoptedCurrencyInfo`, `deriveNextCurrencyInfo`, the per-field gate. `Recreate` (numShards=1, = mainnet full re-exec) stays the default and is untouched.

## 6. Invariants (review checklist)
- I1: committee `diff` is a pure function of `(finalizedBase, window.blocks, carried epoch)` — no live-MPT, no global lookups, no wall-clock. (split-safety)
- I2: `applyDiff(base, diff)` root === attested root for every honest follower; mismatch ⇒ reject the checkpoint (never adopt unverified).
- I3: base is the prior **committee-finalized** MG state, present on every node (finalized ⇒ shared). Never bestTip.
- I4: deletion leaves no caller of the removed re-derive/gate (compile-time).
- I5: `numShards=1` path byte-identical to mainnet (Recreate untouched).

## 7. Open / to confirm during impl
- Exact `ChangeSet` shape carried on the wire (upserts+removals over `GlobalStateKey`) — codec must round-trip address-keyed keys (note the `GlobalStateKey` lossy-address-codec flagged in `259-FOLLOWER-TRUST-REDESIGN` slice-4; the diff carries MG-partition keys — verify they round-trip or restrict the wire-key shape).
- Whether the committee derives via the metagraph's `CurrencySnapshotAcceptanceManager.accept` or the gl0 `processCurrencySnapshots` with finalized prior — pick the one that reproduces `binary.stateProof` byte-exactly (I2 is the test).
