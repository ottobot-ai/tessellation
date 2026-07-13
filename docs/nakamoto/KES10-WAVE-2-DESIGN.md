# KES10 Wave 2 — Design Doc

**Status:** design only (no implementation)
**Branch reference:** `feature/serde-typeclass-shim` (Wave 1 + Path 1 merged)
**Date:** 2026-05-21
**Predecessors:** Wave 1 = S10 foundation (`KesRegistrationCert` schema, validator, acceptance manager, `KesRegistrationStateManager`, `MutableKesRegistry` + persist + S10-wiring-A (HTTP intake, gossip publish, mempool path))
**Successors:** S10 e2e ROTATE — operator submits cert → cert finalizes → sig-verify uses runtime VK

---

## 1. Summary

Wave 2 closes the *runtime KES master-VK registration* feedback loop. Today (Wave 1) the cert intake routes work (HTTP `POST /kes-registration` → mempool → gossip → mempool on peers), but the cert is **never written to MPT** (`GlobalSnapshotConsensusFunctions.createProposalArtifact` has no `KesRegistrationCertEvent` partition; `GlobalSnapshotAcceptanceManager.accept` has no `kesRegistrationCertAcceptanceManager` call), so `MutableKesRegistry.getKesVk` always falls through to the genesis-frozen `KesRegistry`. The receiver-side verifier paths (`KesGossipVerification.verifyAttestation` / `verifySnapshot` / `verifyAttestationByStep`) hold a `KesRegistry[F]`, not a `MutableKesRegistry[F]`, so even after a cert lands in MPT, sig-verify would still ignore it.

Wave 2 is split into two sub-slices that touch disjoint files. They can run **in parallel** (see §4).

- **Sub-slice A — S10-wiring-B**: Wire `KesRegistrationCertAcceptanceManager` + `KesRegistrationStateManager` into the GSAM accept pipeline so accepted certs are persisted to MPT.
- **Sub-slice B — S10-receiver-side**: Migrate the verifier paths from the genesis-frozen `KesRegistry` to `MutableKesRegistry.getKesVk(peerId, currentEpoch)` so runtime certs *take effect* at sig-verify time.

**Why load-bearing.** Without A, no cert reaches MPT and `MutableKesRegistry` is identity-over-`KesRegistry`. Without B, even with A, the verifier ignores the runtime cert. Either half alone produces zero observable behavior change; together they close S10 end-to-end and unlock the operator-rotation workflow that §1.2 epoch staggering depends on (see `project_consensus_epoch_staggering` memory).

---

## 2. Sub-slice A — S10-wiring-B (GSAM integration)

### 2.1 Files touched

Listed in dependency order (A.1 must compile before A.2, etc.).

| # | File | Approx lines | Change |
|---|---|---|---|
| A.1 | `modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateConverter.scala` | 54-93 | Add `kesRegistrationCerts: SortedMap[PeerId, SortedSet[KesRegistrationRecord]]` + `lastKesRegistrationRefs: SortedMap[PeerId, KesRegistrationReference]` + `removedKesRegistrationKeys: Set[PeerId]` + `removedLastKesRegistrationRefKeys: Set[PeerId]` fields to `StateChangesAccumulator` (with `SortedMap.empty` / `Set.empty` defaults to keep call sites compatible). |
| A.2 | `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/overlay/AcceptanceMptStateChanges.scala` | ~120-260 | (a) Build `kesRegistrationCertsEntries: Map[GlobalStateKey, SortedSet[KesRegistrationRecord]]` and `lastKesRegistrationRefsEntries: Map[GlobalStateKey, KesRegistrationReference]` via `GlobalStateKey.kesRegistrationCertsKey[F](peer)` / `lastKesRegistrationRefsKey[F](peer)`. (b) Add `mpt.insert[SortedSet[KesRegistrationRecord]](...)` + `mpt.insert[KesRegistrationReference](...)` calls in the writer body. (c) Add the removed-key paths to `toRemovalKeys`. **No ActiveAddressIndex sidecar** — the partition value carries `operatorPeerId` inside `event.value`, so prefix-scan + value-decode recovers the keyset (matches the `NodeCollateral` shape, NOT `Balances`). |
| A.3 | `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala` | (multiple, see §2.2) | The bulk of A — see detailed walkthrough below. |
| A.4 | `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala` | 308-340 + 355-375 | Pass new constructor arg `kesRegistrationCertAcceptanceManager` to `GlobalSnapshotAcceptanceManager.make`. Source it from `sharedServices` (added in A.5) or construct in-line (mirrors `updateNodeParametersAcceptanceManager` shape — single make call wrapping a validator). |
| A.5 | `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/modules/SharedServices.scala` | 200-260 + 295-313 | (a) Construct `kesRegistrationCertAcceptanceManager = KesRegistrationCertAcceptanceManager.make(validators.kesRegistrationCertValidator)`. (b) Add to the `SharedServices` GSAM call site (line ~234-271). (c) Expose on the `SharedServices` class (around line 312). Add `KesRegistrationCertValidator` to `SharedValidators` if not already present — check `sharedValidators` construction site. |
| A.6 | `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensusFunctions.scala` | 204-213 + 379-423 | (a) Add `kesRegEventsForAcceptance = events.collect { case e: KesRegistrationCertEvent => e }`. (b) Sort: `sortedKesRegEvents = kesRegEventsForAcceptance.toList.map(_.value).sorted(Signed.ordering(Order[KesRegistrationCert].toOrdering))` — `KesRegistrationCert` needs a derived `Order` (verify; if missing, derive in schema file in A.1 alongside the StateChangesAccumulator change). (c) Pass `sortedKesRegEvents` to the GSAM `accept(...)` call (new positional arg between `wncEvents` and `lastSnapshotContext`). (d) Add the `kes.accepted` / `kes.notAccepted` counters to the `Event.AcceptanceResults` ConsensusLog (line 437-453). |
| A.7 | `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensusFunctions.scala` | 113-141 (`validateArtifact`) | Extract `KesRegistrationCertEvent`s from a new artifact field (if added) or — preferred — leave the validateArtifact path alone and accept that accepted certs are observable only by their post-acceptance side effects (per-peer cert chains in MPT, per-peer last-refs in MPT). **Open question (Q1):** does the artifact need to surface the accepted KES certs as a SortedMap field on `GlobalIncrementalSnapshot`? See §5. |
| A.8 | `modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManagerSuite.scala` | n/a | New test cases (see §2.4). |

### 2.2 GSAM walkthrough (`A.3`)

The GSAM `accept` method is the heart of S10-wiring-B. Here is the slot-by-slot change plan.

#### 2.2.1 Constructor (lines ~175-235)

Add `kesRegistrationCertAcceptanceManager: KesRegistrationCertAcceptanceManager[F]` to `GlobalSnapshotAcceptanceManager.make`, threaded into the closure. Construct `kesRegistrationStateManager` analogously to `nodeCollateralStateManager` (line ~248):

```
val kesRegistrationStateManager = KesRegistrationStateManager.make[F](branchAwareReader)
```

This mirrors the `branchAwareReader` plumbing — all KES MPT reads observe the parent-branch view under MultiBranch, identical to the NodeCollateral pattern.

#### 2.2.2 `accept()` signature (lines ~129-173 + 782-820)

Insert a new positional parameter `kesRegEvents: List[Signed[KesRegistrationCert]]` between `wncEvents` and `lastSnapshotContext` in both the trait method signature (line 129) AND the impl (line 782). Update the 14-tuple return type to a **15-tuple** by appending a new element:

- New element: `kesRegistrationCertAcceptanceResult: KesRegistrationCertAcceptanceResult`

Concretely: the return type tuple at lines 149-172 and 802-819 gains:

```scala
KesRegistrationCertAcceptanceResult,  // 15th
BranchHandle[F, GlobalStateKey]       // unchanged trailing position
```

**Open question (Q2):** does the caller need the `KesRegistrationCertAcceptanceResult` directly, or is a `SortedMap[PeerId, KesRegistrationRecord]` (just the accepted) sufficient? See §5. The full result is preferred for observability + diagnostic surfacing (rejections).

#### 2.2.3 Acceptance pipeline body (the load-bearing change)

Insert a new pipeline step in `accept()`. Slot ordering matters — KES acceptance must run AFTER `acceptInitialData` (so we have the snapshot's epoch + ordinal in scope) and BEFORE `buildGlobalSnapshotInfo` (so the StateChangesAccumulator can pick up the delta). Recommended slot: immediately after `priorBalances`, around line 930.

```scala
// §1.2 Slice 10 (#179) — KES registration cert acceptance.
//
// Risk-5 directive (S10-persist review a52c18e76cedc90da): `lastRefs` and
// `lastEffectiveFromEpochs` MUST come from the SAME materialization pass. A
// desynchronized pair could let `MinValue` slip into the validator's
// monotonic-effective-epoch check against a populated `lastRef`, masking a
// real cert. Source both from `KesRegistrationStateManager` in ONE call
// (`materializeLastRefsAndEpochsFromMpt` — new joined materializer; see A.3.1
// below) over the branch-aware reader.
//
// Per Risk-5: source these from MPT-resolved current state, NOT from the
// snapshot being constructed (chicken-and-egg: the GSI being built doesn't
// yet contain this round's accepted KES certs).
kesRegMaterializedPair <- kesRegistrationStateManager.materializeLastRefsAndEpochsFromMpt
(priorKesRegLastRefs, priorKesRegLastEffectiveFromEpochs) = kesRegMaterializedPair

kesRegistrationCertAcceptanceResult <- kesRegistrationCertAcceptanceManager.accept(
  kesRegEvents,
  priorKesRegLastRefs,
  priorKesRegLastEffectiveFromEpochs,
  epochProgress,
  ordinal
)

// Per-peer cert chain materialization for the delta-merge step. Same
// branch-aware reader source so under MultiBranch we see the parent-branch's
// pending writes.
priorKesRegistrationCerts <- kesRegistrationStateManager.materializeActiveKesRegistrationCertsFromMpt

updatedKesRegistrationCerts = kesRegistrationStateManager.getUpdatedKesRegistrationCerts(
  kesRegistrationCertAcceptanceResult,
  priorKesRegistrationCerts
)

updatedKesRegistrationLastRefs <- kesRegistrationStateManager.getUpdatedLastRefs(
  kesRegistrationCertAcceptanceResult,
  priorKesRegLastRefs
)
```

#### 2.2.3.1 NEW joined materializer (`A.3.1`)

`KesRegistrationStateManager` currently exposes `materializeLastRefsFromMpt` (pointer-only) but does NOT join in `effectiveFromEpoch`. Per the Risk-5 directive, Wave 2 MUST add a single materializer that returns both maps from one prefix scan:

```scala
// in KesRegistrationStateManager trait (new method):
def materializeLastRefsAndEpochsFromMpt(
  implicit hasher: Hasher[F]
): F[(SortedMap[PeerId, KesRegistrationReference], SortedMap[PeerId, EpochProgress])]
```

**Implementation:** in `KesRegistrationStateManager.make`, walk `materializeActiveKesRegistrationCertsFromMpt` (existing prefix-scan) to recover per-peer chains; for each peer, look up the pointer (`LastKesRegistrationRefs[peer]`) and project the pointed-at record's `event.value.effectiveFromEpoch`. Both maps are built in ONE loop over the prefix-scan result — no second scan, no race window.

This closes the Risk-5 attack: two independent prefix scans (e.g. one for refs, one for epochs) could under MultiBranch land in two different branch states (a sibling branch eviction between the two scans could leave a peer present in one map and absent in the other). The joined materializer is atomic with respect to the underlying `GlobalStateReader[F]` snapshot.

**Cardinal rule (from Risk-5):** *never* materialize `lastRefs` and `lastEffectiveFromEpochs` independently. Both come from `materializeLastRefsAndEpochsFromMpt` or the design is broken.

#### 2.2.3.2 StateChangesAccumulator wiring (lines ~1474-1511)

Inside the `stateChangesAccumulator = StateChangesAccumulator(...)` constructor call, add:

```scala
kesRegistrationCerts = updatedKesRegistrationCerts,
lastKesRegistrationRefs = updatedKesRegistrationLastRefs,
removedKesRegistrationKeys = ...,  // peers whose chain became empty (rare/never)
removedLastKesRegistrationRefKeys = ...
```

Removed keys: a peer that has ever submitted a cert keeps a non-empty chain (cert acceptance is monotone — we never delete). The removed sets stay empty under normal operation, but include them for symmetry with NodeCollateral and for forward-compat if a future S11 introduces revocation. **Initial implementation:** pass `Set.empty[PeerId]` for both.

#### 2.2.3.3 Return tuple (lines ~1767-1784)

Add `kesRegistrationCertAcceptanceResult` to the yielded tuple (position 15, before `handle`).

### 2.3 Caller updates

`GlobalSnapshotConsensusFunctions.createProposalArtifact` (A.6) is the sole caller of `globalSnapshotAcceptanceManager.accept`. It needs:

1. **Event extraction** at line 204-213:
   ```scala
   val kesRegEventsForAcceptance = events.collect { case e: KesRegistrationCertEvent => e }
   ```

2. **Sort** at lines 348-357 (deterministic order):
   ```scala
   sortedKesRegEvents = kesRegEventsForAcceptance.toList
     .map(_.value)
     .sorted(Signed.ordering(Order[KesRegistrationCert].toOrdering))
   ```
   Requires `Order[KesRegistrationCert]` — derive in `KesRegistrationCert.scala` if absent. The natural sort key is `(operatorPeerId, ordinal)` to match the validator's per-peer chain-link ordering.

3. **Pass to accept** at lines 396-423. New positional arg `sortedKesRegEvents` between `sortedWncEvents` and `snapshotContext`.

4. **Unpack** the 15-tuple at lines 379-395. Add `kesRegistrationCertAcceptanceResult` between `nodeCollateralAcceptanceResult` and `scSnapshots` (or as the new last-before-`overlayHandle` element — pick whichever matches the GSAM `accept` return order).

5. **ConsensusLog** at lines 437-453: add `"kes.accepted" -> ...accepted.size.toString` and `"kes.rejected" -> ...notAccepted.size.toString`.

6. **`validateArtifact`** at lines 113-173: a leader's accepted KES certs must be reconstructible by followers. Since the artifact (GlobalIncrementalSnapshot) doesn't yet carry them, follower reconstruction depends on follower's own mempool containing the same `Signed[KesRegistrationCert]`s. **This matches the existing pattern** for other event types (e.g. UpdateNodeParameters comes from artifact, but DAG blocks come from `artifact.blocks`). **Open question (Q1):** add a `kesRegistrationCerts: Option[SortedMap[PeerId, Signed[KesRegistrationCert]]]` field to `GlobalIncrementalSnapshot` for full leader→follower determinism, or rely on mempool convergence? Default position: **add the artifact field** to make `validateArtifact` deterministic without requiring all peers to converge their mempool first.

### 2.4 Test expectations

#### Unit-level

- `GlobalSnapshotAcceptanceManagerSuite` — at least one test case: feed an accept() call a list with one valid `Signed[KesRegistrationCert]`, verify the returned 15-tuple's KES result has `accepted.size == 1`, AND verify the MPT post-state contains the new pointer + cert chain. Use the existing `branchAwareReader.get[KesRegistrationReference](lastKesRegistrationRefsKey(peer))` to assert.
- `KesRegistrationStateManagerSuite` (new) — covers `materializeLastRefsAndEpochsFromMpt` joined behavior. Critical test: two certs from the same peer at different ordinals; pointer points to the second; the joined materializer returns `(ref_to_second_cert, second_cert.effectiveFromEpoch)`. Negative case: pointer present but cert set missing the pointed ordinal → both maps drop the entry (mirrors `materializeFromMpt` tolerance).
- `GsamWritePathParitySuite` (#107) — the byte-parity contract. After Wave 2 lands, this suite gains coverage for the KES partitions: state with N runtime certs in MPT must produce a byte-identical `mptRoot` whether built via delta path or rebuild path. Add a fixture: cluster-state with 3 peers × 2 certs each, assert rebuild root === incremental root.

#### e2e-level

- `delegated_staking.test.js` / similar — operator submits KES rotation cert via `POST /kes-registration`, polls `GET /kes-registration/{peerId}/info` until `activeRecord` becomes non-empty. Without Wave 2 this polls forever (cert never lands in MPT).
- New e2e (`kes_rotation.test.js`): full flow — submit cert at epoch E with `effectiveFromEpoch = E + 2`, wait for finality, advance to epoch E + 3, assert subsequent attestations from this peer verify successfully against the runtime VK (proves both A and B work together; without B the verifier still uses genesis VK and verification would falsely succeed for OLD attestations or fail for NEW ones depending on the rotation direction).

---

## 3. Sub-slice B — S10-receiver-side (verifier consumption)

### 3.1 Lookup site identification

Three callers of `kesRegistry.getKesVk(peerId)` (genesis-only) exist:

| # | File | Line | Path |
|---|---|---|---|
| B.1 | `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/KesGossipVerification.scala` | 58 | `verifyAttestation` (TipAttestation gossip) |
| B.2 | same file | 143 | `verifyAttestationByStep` (committee/metagraph attestation gossip) |
| B.3 | same file | 199 | `verifySnapshot` (snapshot gossip) |

All three currently take `kesRegistry: KesRegistry[F]` (genesis-frozen). All three need to migrate to a lookup that:

1. Tries the runtime cert (via `MutableKesRegistry.getKesVk(peerId, currentEpoch)`).
2. Falls through to genesis if no active runtime cert exists.

`MutableKesRegistry.getKesVk` already implements that fall-through internally (lines 87-95 of `MutableKesRegistry.scala`), so the migration is **purely structural**: swap `KesRegistry[F]` → `MutableKesRegistry[F]` in the verifier signature + thread `currentEpoch` through.

### 3.2 Migration plan

Direct swap with two new positional params on each verify function:

```scala
def verifyAttestation[F[_]: Async: Metrics](
  // ...existing params...
  kesRegistry: MutableKesRegistry[F],   // <-- swapped
  currentEpoch: EpochProgress,          // <-- new
  // ...
): F[Boolean]
```

The internal call becomes:

```scala
kesRegistry.getKesVk(attesterId, currentEpoch).flatMap {
  case None => /* no-registry-entry carve-out, unchanged */
  case Some(entry) => /* verify with entry.vk and entry.offset, unchanged */
}
```

The accept/reject matrix is **byte-identical** to today — only the source of `entry` changes.

### 3.3 Threading `currentEpoch` through the callers

Two call sites consume the verifier:

| # | File | Line | How `currentEpoch` is obtained |
|---|---|---|---|
| B.4 | `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala` | 991 (`kesGate` in `processSnapshot`) | `signedSnapshot.value.epochProgress` from the incoming snapshot OR `lastGlobalSnapshotStorage.get.map(_.fold(EpochProgress.MinValue)(_._2.something))`. **Recommendation:** use the head snapshot's `epochProgress` from `lastGlobalSnapshotStorage` — this is the node's current finalized view, matches the validator/HTTP route pattern (`signedSnapshot.value.epochProgress` in `KesRegistrationCertRoutes.scala:84`). |
| B.5 | same file | 1339 (`verifyAttestation` in `handleAttestation`) | Same source — `lastGlobalSnapshotStorage.head` (or equivalent). |
| B.6 | `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala` | 897 (`committeeKesVerifier`) | Same: `lastGlobalSnapshotStorage` (already in scope as `lastGlobalSnapshotStorage`). |

**Concrete pattern:**

```scala
// In the verifier closure, lift currentEpoch resolution to F:
currentEpoch <- lastGlobalSnapshotStorage.head.map {
  case Some((signed, _)) => signed.value.epochProgress
  case None              => EpochProgress.MinValue  // pre-genesis or storage unavailable
}
ok <- KesGossipVerification.verifyAttestation[F](
  // ...existing args...
  kesRegistry = mutableKesRegistry,
  currentEpoch = currentEpoch,
  // ...
)
```

`EpochProgress.MinValue` fallback is safe: with `currentEpoch = 0`, `MutableKesRegistry.getKesVk` ALWAYS falls through to genesis (no cert has `effectiveFromEpoch <= 0`; validator's `NotForwardActivation` rejects retroactive activation), so verifier behavior collapses to Wave-1 behavior in the degenerate case.

### 3.4 Memoization decision

**Per Risk-5 review:** "attestation rate × peer count hot path."

**Hot path analysis:**
- TipAttestation verifies fire 1× per peer per snapshot → at 8 peers × ~7s/snapshot = ~1 verify/sec/peer.
- Committee-attestation (`verifyAttestationByStep`) fires 1× per metagraph-binary per committee-member → much rarer.
- `verifySnapshot` fires 1× per incoming snapshot per producer → ~1/sec.

Each verify does:
- `MutableKesRegistry.getKesVk(peerId, currentEpoch)` →
- `KesRegistrationStateManager.materializeFromMpt(peer)` →
  - 1 MPT point read on `LastKesRegistrationRefs[peer]`
  - 1 MPT point read on `KesRegistrationCerts[peer]` (returns `SortedSet`, in-memory `.find`)

Two MPT point reads per verify = 2 × (8 peers × 1/sec) = 16 reads/sec **per node**. Not trivially-free but not a hot path either. The MPT reader is in-memory under JsonHash; under MerklePatriciaFormat it's a tree walk but still O(log N) per read.

**Decision: NO per-block memoization in Wave 2.** Reasons:

1. **Reorg-aware behavior is part of the contract.** `MutableKesRegistry.getKesVk` goes through the branch-aware `GlobalStateReader[F]` (passed in at construction); under MultiBranch a memoization layer would either (a) miss reorgs and serve stale entries, or (b) require explicit cache-invalidation on every reorg, which is hard to get right.
2. **MPT reads are already cache-friendly.** `LastKesRegistrationRefs[peer]` is a tiny key; the underlying `MptStore` already has its own LRU. Adding a second-level cache in the verifier risks double-caching.
3. **Hot-path empirics today are gentle.** 8-node cluster at ~7s/snapshot is well below any reasonable threshold. If a future scale-up (e.g. 64 peers @ 1s slots) makes this load-bearing, add memoization THEN, scoped to a single `(peerId, currentEpoch)` tuple per `verify*` invocation chain — i.e. per-attestation-batch, NOT per-block.

**If memoization is requested in review:** the lightest-touch option is per-`verify*`-batch memoization. Each top-level call to `handleAttestation` / `processSnapshot` already loads one snapshot from gossip; wrap the inner verify in a `Ref[F, Map[PeerId, Option[KesRegistryEntry]]]` scoped to the batch (allocated on entry, discarded on exit). Zero cache-invalidation surface area, zero reorg-staleness exposure. Defer to follow-up unless a profiling pass shows the MPT reads as a bottleneck.

### 3.5 Test expectations

#### Unit-level

- **Superseded by the atomic operator-key design.** `KesGossipVerificationSuite` passes one already-resolved `OperatorConsensusKeys` pair;
  standalone `KesRegistry.make/empty` factories no longer exist. Runtime activation belongs to the exact candidate-parent
  `HistoricalOperatorConsensusKeyRegistry`, not a receiver-current mutable KES projection. Historical cases below are retained only as
  requirements for that branch-pinned resolver:
  - **Genesis-only**: no runtime cert in MPT → fall through to genesis VK (existing matrix unchanged).
  - **Runtime cert active**: runtime cert with `effectiveFromEpoch < currentEpoch` → verify uses runtime VK; genesis VK ignored.
  - **Runtime cert pending**: runtime cert with `effectiveFromEpoch > currentEpoch` → verify falls back to genesis (cert held as "pending").
  - **Rotation boundary**: assert the verifier picks up the new VK exactly at the snapshot whose `epochProgress == effectiveFromEpoch`, not one ordinal earlier.

#### e2e-level

See §2.4 (the `kes_rotation.test.js` e2e exercises both sub-slices end-to-end).

---

## 4. Inter-dependencies

**Sub-slices A and B are independent at the file-touch level.** A touches GSAM + GSCF + SharedServices + the writer/converter. B touches `KesGossipVerification` + the three caller sites in `NakamotoSyncDaemon` + `GlobalSnapshotConsensus`. Zero file overlap.

**At the data-flow level:** B *reads* what A *writes* (runtime cert → MPT → verifier). But:

- B compiles + tests cleanly without A: `MutableKesRegistry.getKesVk` works today (Wave 1) — it just always returns `None` from the pointer lookup and falls through to genesis. So a `kes_rotation.test.js` e2e with B-only would behave like Wave 1 (no rotation observable) but would not regress.
- A compiles + tests cleanly without B: certs land in MPT, are observable via `GET /kes-registration/{peerId}/info`, but the verifier path still uses genesis. No production behavior change, but `KesRegistrationStateManagerSuite` + GSAM tests pass.

**Recommended sequencing: PARALLEL.** Two impl agents can work simultaneously. The merge order is arbitrary; the e2e test that exercises both (kes_rotation) is the integration acceptance criterion.

**Caveats:**
- If A introduces a new field on `GlobalIncrementalSnapshot` (per Q1 in §5), agents must coordinate on the schema change before either lands. Recommend **lifting that schema decision into a pre-impl sync** (the design author + both impl agents agree on yes/no before either branch starts coding).
- The `validateArtifact` path inside A.6 — if Q1 lands "yes, add artifact field" — depends on `Signed[KesRegistrationCert]` carrying enough info for follower reconstruction. The acceptance manager already returns `KesRegistrationCertAcceptanceResult` (with `accepted: SortedMap[PeerId, KesRegistrationRecord]`); the record's `event` field carries the `Signed[KesRegistrationCert]` directly, so artifact-side roundtripping is straightforward.

---

## 5. Open questions

**Q1 — Artifact field on `GlobalIncrementalSnapshot`?**
Add `kesRegistrationCerts: Option[SortedMap[PeerId, Signed[KesRegistrationCert]]]` to `GlobalIncrementalSnapshot`?

- **Pros**: follower `validateArtifact` can deterministically reconstruct accepted certs without depending on mempool convergence. Matches the `activeDelegatedStakes` / `activeNodeCollaterals` pattern.
- **Cons**: schema change on a load-bearing artifact. Bumps the snapshot binary size by ~200 bytes/cert (most snapshots will have 0 certs, so the field stays `None` and the impact is the 1-byte tag).
- **Recommendation: YES.** Consistency with other create/withdraw event-derived artifact fields outweighs the schema-bump cost. Add as an `Option[SortedMap[...]]` so legacy snapshots decode cleanly (`None`).
- **Needs user input?** Probably not — this is a clear extension of an existing pattern, but flag it for review.

**Q2 — Surface full `KesRegistrationCertAcceptanceResult` to caller?**
The GSAM tuple grows from 14 to 15 elements. The 15th element could be:
- (a) Full `KesRegistrationCertAcceptanceResult` (accepted + notAccepted)
- (b) Just `SortedMap[PeerId, KesRegistrationRecord]` (accepted)
- (c) Nothing (drop the element; KES result is observable only via MPT side effect)

- **Recommendation: (a)**. Mirrors `delegatedStakeAcceptanceResult` / `nodeCollateralAcceptanceResult` — those carry both accepted + rejected so the caller can log rejections and surface them to /info routes. The GSCF `Event.AcceptanceResults` log line (§2.3 step 5) consumes `.accepted.size` and `.notAccepted.size`; only (a) provides both.

**Q3 — `KesRegistrationCertEvent` cutter / size limit?**
Other event types (`StateChannelEvent`, `DAGEvent`, `UpdateNodeParametersEvent`) have explicit cutters (`eventCutter`, `updateNodeParametersCutter`) to bound binary size. Should KES certs be subject to a similar cutter? A `KesRegistrationCert` is ~200 bytes; even at 1000 certs/snapshot the contribution is ~200 KB. Cluster has 8 peers; rotation cadence is "rare." **Recommendation:** NO cutter. Add a counter (`dag_nakamoto_kes_certs_per_snapshot`) to flag if growth becomes anomalous, and revisit if a per-snapshot peer-cert flood becomes possible. **Open**: leaves a denial-of-service vector — an attacker submitting many certs/snap could bloat the artifact. Mitigated by `KesRegistrationCertValidator.NonMonotonicOrdinal` (only `lastRef.ordinal.next` is accepted per peer per snapshot, so each peer contributes ≤ 1 cert per snapshot). Net throughput bound = `|active_peers| × 1 cert/snap × ~200B = ~1.6 KB/snap for 8 peers`. Safe.

**Q4 — `KesRegistrationCertValidator` already in `SharedValidators`?**
A.5 says "add to SharedValidators if not already present." Need to confirm. If `sharedValidators.kesRegistrationCertValidator` already exists, skip; if not, add it next to `updateNodeCollateralValidator`. Check `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/modules/SharedValidators.scala`.

**Q5 — Pruning?**
`KesRegistrationCerts[peer]` accumulates per-peer cert history monotonically. A long-lived cluster will see one operator's chain grow to N entries over N rotations. The MPT value `SortedSet[KesRegistrationRecord]` is read whole on every lookup. **Should we prune to "last K" (e.g. K=4 = 2 rotations of slop)?** Defer to a follow-up; Wave 2's contract is "accepted certs persist." Pruning is a §11 problem, not §10.

---

## 6. Test plan

### Regression catchers (must exist post-Wave-2)

| Sub-slice | Test | What it catches |
|---|---|---|
| A | `GlobalSnapshotAcceptanceManagerSuite` — "accepts valid KES cert and persists to MPT" | A.3 wiring broken (cert not actually written) |
| A | `GsamWritePathParitySuite` extension | A.2 writer-vs-rebuild byte mismatch (#107 contract) |
| A | `KesRegistrationStateManagerSuite` — joined materializer | Risk-5 directive violation (lastRefs vs lastEffectiveFromEpochs desync) |
| A | `GlobalSnapshotConsensusFunctionsSuite` — extracted KES events sorted correctly | A.6 event-extraction ordering bug |
| B | `KesGossipVerificationSuite` — runtime cert active overrides genesis | B.1/B.2/B.3 lookup migration broken |
| B | `KesGossipVerificationSuite` — pending runtime cert falls back to genesis | Risk-5 NonMonotonicEffectiveFromEpoch check bypass |
| A+B | e2e `kes_rotation.test.js` | End-to-end: cert submit → MPT land → verifier picks up at activation epoch |

### Performance baseline (post-Wave-2 sanity)

- Iter at 8gl0+4mg+4shards, target topology (per `project_iter_v23_full_pass_8gl0_4mg_4shards`):
  - Wave 2 adds 2 MPT point-reads + 1 MPT insert per snapshot per accepted KES cert. At rotation cadence "rare" (e.g. 1 cert/100 snapshots cluster-wide), overhead is negligible.
  - Watch `[ACCEPTANCE] MPT_SYNC_FP` log for KES partition presence at boundary writes.
  - Compare wall-clock e2e duration to v23 baseline (5533s); Wave 2 must not regress >5%.

### Failure-mode coverage

- **Risk-5 desync test:** stub `KesRegistrationStateManager` returning maps with mismatched key-sets for `lastRefs` vs `lastEffectiveFromEpochs`. Assert GSAM rejects the inconsistency (or — preferred — design out the failure mode by making the joined materializer the only public API).
- **Reorg-survival:** simulate MultiBranch with a sibling branch evicting a peer's pointer mid-snapshot. Verify the GSAM accept() call uses the parent-branch view (priorBalances pattern) and produces deterministic output across two nodes seeing the same eviction.
- **No-cert noop:** an accept() call with empty `kesRegEvents` produces a byte-identical MPT root to a call before Wave 2 (modulo the StateChangesAccumulator default fields being empty). Catches regressions where the new accumulator fields are mis-encoded as non-empty defaults.

---

## 7. Out of scope (for Wave 2)

- KES cert revocation / removal (S11 / future).
- KES cert "active" lookup at non-current epochs (e.g. historical state-proof verification for a past epoch). `MutableKesRegistry.getKesVk` already supports it via the `currentEpoch` arg, but no caller in Wave 2 uses it.
- Pruning per-peer cert chains.
- Cross-shard KES (metagraphs running their own KES schemes; Wave 2 is global-only).
- Slashing for KES-related misbehavior (S11+).

---

**Author:** Wave 2 design pass, 2026-05-21.
**Reviewer ask:** confirm Q1 (artifact field) and Q4 (validator in SharedValidators) before impl agents fork.
