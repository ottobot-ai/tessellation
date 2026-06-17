# Unroll the per-metagraph `CurrencySnapshotInfo` blob → per-entry MPT keys

**Status:** LANDED (2026-06-17). Prereq for `COMMITTEE-STATE-DIFF-ADOPTION-DESIGN.md`. Greenfield — no wire/on-disk compat (`[[feedback-greenfield-no-wire-compat]]`).

## Implemented (verified against code 2026-06-17, branch `feature/serde-typeclass-shim`)

The unroll **shipped on this branch** (the design doc itself first landed in `85be66a6a`). Ground-truth confirmation:

- **The 8 `Mg*` `GlobalStateFieldId`s** (`shared/.../schema/mpt/GlobalStateKey.scala:261-268`, `fromInt` map at :320-327):

  | field | case object | int |
  |---|---|---|
  | `balances` | `MgBalances` | 25 |
  | `lastTxRefs` | `MgLastTxRefs` | 26 |
  | `lastFeeTxRefs` | `MgLastFeeTxRefs` | 27 |
  | `lastAllowSpendRefs` | `MgLastAllowSpendRefs` | 28 |
  | `lastTokenLockRefs` | `MgLastTokenLockRefs` | 29 |
  | `activeTokenLocks` | `MgActiveTokenLocks` | 30 |
  | `lastMessages` | `MgLastMessages` | 31 |
  | `globalSnapshotSyncView` | `MgGlobalSnapshotSyncView` | 32 |

- **`infoSubFields` union site.** `val infoSubFields: Set[GlobalStateFieldId]` = exactly those 8 (`GlobalStateKey.scala:274-284`). It is consumed at the two load-bearing sites the design named:
  - producer: `GlobalStateConverter.currencySnapshotFieldRoots` → `rootForFields(GlobalStateFieldId.infoSubFields.contains)` (`GlobalStateConverter.scala:1327`);
  - follower / byte-rebuild: `GlobalSnapshotInfo.mptStateProofFromBytes` (`GlobalSnapshotInfo.scala:332-337`) computes `infoRoot = fieldRootFromBytes(UNION over FId.infoSubFields)` instead of the old `fieldId == LastCurrencySnapshotInfo` lookup. Both route through `currencySnapshotEntryBytes` (`GlobalStateConverter.scala:1285`), so producer↔follower byte-identity is by construction (invariant I2).
  - `lastCurrencySnapshotsProof = Some(CurrencySnapshotMptRoots(fieldRoot(LastIncrementalCurrencySnapshots), currencyInfoRoot))` (`GlobalSnapshotInfo.scala:353-355`) — stateProof shape unchanged (I3 holds), only the key-set under `infoRoot` changed.

- **Decision (b) confirmed verbatim:** `activeAllowSpends` stays in fieldId-7 (`ActiveAllowSpends`, `Some(mgId)`) — it is **NOT** emitted by `currencySnapshotEntryBytes` and there is **no** `MgActiveAllowSpends`. Explicit scaladoc at `GlobalStateConverter.scala:1188-1190`: "`activeAllowSpends` is NOT emitted here — it stays in the fieldId-7 `ActiveAllowSpends` partition … So `infoRoot` covers these 8 sub-fields; `activeAllowSpends` is committed separately via the fieldId-7 `activeAllowSpends` state-proof slot." Reconstruction reads it back from the fieldId-7 mg-scope prefix (`GlobalStateConverter.scala:1382-1393`). `SpendActionValidator`'s cross-shard read path is untouched, as the design intended.

- **Reconstruction** rebuilds `CurrencySnapshotInfo` per MG by prefix-scanning the `Mg*` partitions then re-reading fieldId-7 for `activeAllowSpends` (`GlobalStateConverter.scala` `getAllLastCurrencySnapshots` region, :1382-1393).

**Deviation flagged (doc-internal, not code):** §9 "Increment plan" step 1 says *"add the 9 `Mg*` `GlobalStateFieldId`s"* — the shipped count is **8** (no `MgActiveAllowSpends`), consistent with §3/§10-Q1 decision (b) and the table in §3. The "9" in step 1 is a stale leftover from before decision (b) was taken; the rest of the doc and the code agree on 8. No code change needed; treat §9 step 1's "9" as "8".

**Persistence read-compat note (greenfield):** per `[[feedback-greenfield-no-wire-compat]]` only on-disk state needs read-compat. The unroll changes the MPT key layout (new `Mg*` partitions replace the fieldId-6 blob) and was applied atomically cluster-wide — no dual-codec read path; pre-unroll on-disk `mpt_snapshot_info` dumps from older runs are not forward-read.

## 1. Motivation — SCALABILITY is the primary driver (not cleanup)

gl0 mirrors each metagraph's currency state as a **monolithic `CurrencySnapshotInfo` blob** at one MPT key per MG: `GlobalStateKey.metagraph(mgAddr, LastCurrencySnapshotInfo)` (fieldId 6). That blob is `O(N)` in the metagraph's account / allow-spend / token-lock count. Consequences that do **not** scale:

- **gl0 MPT churn is O(N) per ordinal.** Any change to *one* account rewrites the *entire* blob key, so the per-ordinal MPT write (and therefore the `infoRoot` recomputation and the overlay `ChangeSet`) is O(N) even for a 1-entry change.
- **The committee state-diff would carry O(N) per checkpoint.** Under `COMMITTEE-STATE-DIFF-ADOPTION-DESIGN.md` the committee ships the MPT byte-diff; with a monolithic blob that diff *is* the whole blob (the `LastCurrencySnapshotInfo` upsert in `AcceptanceMptStateChanges.applyStateChanges:124-131`). "Carry the diff" and "carry the large derived info" become the **same object**.
- Today every gl0 node also re-derives that O(N) blob per ordinal (the empty-prior re-exec, the run-24/26 allow-spends bug, `[[project-adopt-gate-drops-allowspends-run26]]`).

**The blob is a scalability dead-end.** Unrolling to per-entry keys makes every one of these `O(changed-entries)` — the Ethereum state model (one trie, diff = changed leaves; a denormalized per-contract blob is the anti-pattern). We change the data structure now, deliberately, rather than entrench the blob behind the committee-diff wire format. This scalability rationale — not tidiness — is why we deviate.

## 2. Current model (grounded)

- **Storage.** Per MG, `lastCurrencySnapshots` splits into two `metagraph`-namespaced keys: `LastIncrementalCurrencySnapshots` (fieldId 5, the `Signed[CurrencyIncrementalSnapshot]`) and `LastCurrencySnapshotInfo` (fieldId 6, the blob). Genesis adds `LastCurrencySnapshots` (fieldId 3, the `Signed[CurrencySnapshot]`, the `Left` branch). Encoder: `GlobalStateConverter.currencySnapshotEntryBytes:1167-1192`. Decoder/materialize: `getAllLastCurrencySnapshots:1427-1453` and `GlobalSnapshotAcceptanceManager:1623-1662`.
- **`CurrencySnapshotInfo` (9 fields)** (`shared/.../currency/schema/currency.scala:88-99`): `lastTxRefs`, `balances`, `lastMessages?`, `lastFeeTxRefs?`, `lastAllowSpendRefs?`, `activeAllowSpends?`, `globalSnapshotSyncView?`, `lastTokenLockRefs?`, `activeTokenLocks?` (`?` = `Option[SortedMap]`).
- **Per-MG unroll status today:** only `activeAllowSpends` is *also* unrolled — `hypergraph(ActiveAllowSpends, Some(mgId), holder)` (fieldId 7), keyed by metagraphId (`AcceptanceMptStateChanges:137-141`, accumulator `activeAllowSpends: SortedMap[Option[Address], …]`). The OTHER 8 fields live **only** in the fieldId-6 blob (the global `Balances`/`LastTxRefs`/… partitions are DAG-scoped, no metagraphId).
- **`infoRoot`.** `GlobalSnapshotStateProof.lastCurrencySnapshotsProof: Option[CurrencySnapshotMptRoots(incrementalRoot, infoRoot)]` (`GlobalSnapshotStateProof.scala:27-30,104`). `infoRoot = fieldRootFromBytes(entries where fieldId == LastCurrencySnapshotInfo)` (`GlobalStateConverter.currencySnapshotFieldRoots:1200-1214`, `fieldRootFromBytes:1146-1148`). It is in `StateProofComparison`'s `===` (only `smtRoot` excluded — `StateProofComparison.scala:36-40`) and is follow-verified by cl1/dl1 (`FollowVerifyCore.currencySnapshotsCheck:529-549`). **Load-bearing for cross-node verification.**
- **Read consumers (insulated).** Almost all read through `GlobalSnapshotInfo.lastCurrencySnapshots` (in-memory `Either` map) or `GlobalStateReaderOps.getCurrencySnapshotInfo`, using a narrow slice: `balances` (`AllowSpendService:38`, `TokenLockService:45`, `TransactionService:83`, `CurrencySnapshotConsensusFunctions:54`, `GSAM:1765`), `lastMessages` (`currencyMessage.scala:65-97`, `StateChannelValidator:52-62`, `SnapshotBinaryFeeCalculator:42`), `activeTokenLocks` (`TokenLockService:45`). **cl1 bootstrap** is the only whole-object consumer (`currency-l1/.../CurrencySnapshotProcessor.scala:423-430`, passes the full `CurrencySnapshotInfo` to `bootstrapFrom`). The GSCEP per-field adopt gate reads all 9 — **but it is being deleted** (§7).

## 3. The unroll schema

Replace the single fieldId-6 blob with **per-entry keys under the MG's own `MetagraphNamespace`**, one new `GlobalStateFieldId` per `CurrencySnapshotInfo` field. New key constructor (mirrors `hypergraph(field, contract, user)` but MG in the *network* slot, so it never touches the DAG-scoped partitions):

```
metagraphEntry(mgAddr, subField, userKey) =
  GlobalStateKey(MetagraphNamespace(mgAddr), subField, EmptyNamespace, <user>)
```

| CurrencySnapshotInfo field | new fieldId | user-namespace slot | value |
|---|---|---|---|
| `balances` | `MgBalances` | `AddressNamespace(account)` | `Balance` |
| `lastTxRefs` | `MgLastTxRefs` | `AddressNamespace(account)` | `TransactionReference` |
| `lastFeeTxRefs` | `MgLastFeeTxRefs` | `AddressNamespace(account)` | `TransactionReference` |
| `lastAllowSpendRefs` | `MgLastAllowSpendRefs` | `AddressNamespace(account)` | `AllowSpendReference` |
| `activeAllowSpends` | *(stays in fieldId-7 `ActiveAllowSpends` `Some(mgId)`)* | — | — |
| `lastTokenLockRefs` | `MgLastTokenLockRefs` | `AddressNamespace(account)` | `TokenLockReference` |
| `activeTokenLocks` | `MgActiveTokenLocks` | `AddressNamespace(holder)` | `SortedSet[Signed[TokenLock]]` |
| `lastMessages` | `MgLastMessages` | `HashNamespace(hash(MessageType))` | `Signed[CurrencyMessage]` |
| `globalSnapshotSyncView` | `MgGlobalSnapshotSyncView` | `HashNamespace(hash(PeerId))` | `Signed[GlobalSnapshotSync]` |

- `MgLastMessages` / `MgGlobalSnapshotSyncView` use `HashNamespace(hash(key))` for non-Address keys (same as `updateNodeParametersKey`/`priceStateKey:319-348`). Both are small/bounded (one per `MessageType`; one per cluster peer) — keying them per-entry is cheap and uniform.
- fieldId 5 (`LastIncrementalCurrencySnapshots`) and fieldId 3 (genesis `LastCurrencySnapshots`) are **unchanged** — only the fieldId-6 blob is unrolled.
- **fieldId-7 hypergraph `ActiveAllowSpends` metagraph-scope (`Some(mgId)`) — MIGRATE, don't silently drop (resolves Q1).** Review found a DIRECT reader: `SpendActionValidator.scala:363` does `GlobalStateKey.hypergraph(ActiveAllowSpends, targetMg, source)` for cross-shard allow-spend validation. So either (a) keep writing fieldId-7 for mg-scope AND migrate `SpendActionValidator` to read `MgActiveAllowSpends`, or (b) keep fieldId-7 mg-scope as gl0's cross-shard read index and DON'T move allow-spends into `MgActiveAllowSpends` (i.e. reconstruct `si.activeAllowSpends` FROM fieldId-7, leave that one field where it is). **Decision: (b)** — `activeAllowSpends` stays in fieldId-7 (already unrolled, already per-MG, already has its cross-shard reader); the unroll covers only the 8 blob-only fields. This avoids touching `SpendActionValidator` and the cross-shard path entirely. The DAG-scoped `ActiveAllowSpends` (`None`) is unaffected. So the new `Mg*` fieldIds are 8, not 9 (no `MgActiveAllowSpends`); reconstruction reads `activeAllowSpends` from the existing fieldId-7 mg-scope prefix.

## 4. `infoRoot` — same stateProof shape, root over the unrolled keys

`GlobalSnapshotStateProof` and `CurrencySnapshotMptRoots(incrementalRoot, infoRoot)` are **unchanged in shape**. Only the *set of keys* `infoRoot` covers changes: from "the one fieldId-6 key per MG" to "all `MgBalances|MgLastTxRefs|…|MgGlobalSnapshotSyncView` entries across all MGs".

- **Exact algorithm (BOTH producer and follower, identically — resolves review hazard "Option A vs B"):** `infoRoot = fieldRootFromBytes( UNION of all hex-keyed entries whose fieldId ∈ infoSubFields )`. ONE combined byte-map → ONE `MerklePatriciaTrie.makeParallelFromBytes` root (NOT 9 separate roots hashed together). Empty union ⇒ `Hash.empty` (the `fieldRootFromBytes` convention). Because the MPT root is a pure function of the (hex-key → bytes) set, insertion/iteration order is irrelevant (it's a trie) — confirmed determinism.
- `currencySnapshotFieldRoots:1200-1214`: change the producer recompute to the union-over-infoSubFields. Both producer and follower route through the SAME `currencySnapshotEntryBytes` encoder, so the root is byte-identical by construction (I2).
- `mptStateProofFromBytes` / `perFieldGrouping` (`GlobalSnapshotInfo.scala:336-416`): the fieldId-6 lookup (`fieldRoot(LastCurrencySnapshotInfo)`) becomes the union-over-infoSubFields. **This is the load-bearing site — if producer assembles `infoRoot` from raw MPT bytes grouped by fieldId but the follower recomputes from re-encoded reconstructed info, both must use the identical union rule.** Every site filtering `fieldId == LastCurrencySnapshotInfo` (GlobalSnapshotInfo.scala:277-279, 391-394; GlobalStateConverter.scala:1211-1212) must switch to the union.
- **The per-MG attested root in the committee checkpoint is the MPT root pair `CurrencySnapshotMptRoots(incrementalRoot, infoRoot)` — NOT `hash((addr, state))`.** This is the §5 Some/None fix: MPT roots are byte-level (Option-wrapper invisible). `deriveMetagraphRoot`'s `hash((metagraphAddress, state))` (GSCEP:843) is part of the re-exec path being DELETED (§7).
- No new stateProof field, no codec change (`GlobalSnapshotStateProofCodec` untouched). `StateProofComparison`, `FollowVerifyCore.currencySnapshotsCheck`, and `diffOptCurrencyRoots` keep working verbatim — they call `currencySnapshotFieldRoots`, which now returns the unrolled `infoRoot`.

## 5. Reconstruction (`CurrencySnapshotInfo` from per-entry keys)

`getAllLastCurrencySnapshots` and the GSAM materialization rebuild each MG's `CurrencySnapshotInfo` by prefix-scanning its sub-partitions (`hypergraphFieldPrefix` analogue for the metagraph namespace — add `metagraphFieldPrefix(mgAddr, subField)`), then assembling the case class. Genesis (`Left`, fieldId-3 present) still returns `Left(snap)` — its info comes from the genesis snapshot itself, so the unrolled fieldId-6 entries are redundant for `Left` (OPEN Q2: do we still emit them for `Left` to keep `infoRoot` covering genesis MGs, or skip — pick whichever keeps `infoRoot` byte-identical to the pre-unroll value for a genesis-only MG; the encoder must be symmetric on both sides regardless).

**Option-emptiness — THE load-bearing invariant (resolves review hazard #1).** The `?` fields reconstruct as `Some(scanned)`, empty scan ⇒ `Some(empty)`. This is safe ONLY because of the **post-tess3 always-`Some(_)` convention** (`applyAccumulatorToGSI:115-124`): cl1 (the metagraph) emits every optional field as `Some(_)` even when empty, so gl0's authoritative-derived info at a *touched* ordinal is already `Some(empty)`, and the reconstruction at an *untouched* ordinal also yields `Some(empty)` — identical. Why this matters beyond the MPT (the reviewer's prime suspect): `buildMerkleTreeAndProofs` hashes `(address, state)` into `LastCurrencySnapshotsProofs`, and `state` includes the reconstructed `CurrencySnapshotInfo` for *untouched* MGs carried forward each ordinal; Circe encodes `None` ≠ `Some(empty)`, so a reconstruction that flipped `None→Some(empty)` would churn an untouched MG's Merkle leaf. The post-tess3 convention makes flip impossible (it's always `Some`), so the leaf is stable and uniform cluster-wide.
- **MANDATORY test (the single assumption the whole design rests on):** assert that cl1's `CurrencySnapshotInfo` is always-`Some` for every optional field at every post-tess3 ordinal, and that `reconstruct(encode(info)) == info` byte-for-byte (incl. the `LastCurrencySnapshotsProofs` Merkle leaf `(addr, info).hash`). If cl1 can emit `None` post-tess3, we MUST add a per-MG presence-header key (a 7-bit Some/None mask, O(1)/MG) — fallback specified, not yet needed.
- **Genesis (`Left`, fieldId-3 present) (resolves Q2):** reconstruction returns `Left(snap)`; the info comes from the genesis snapshot's own `toCurrencySnapshotInfo` (which sets `lastMessages = None` etc. — see GSCEP:200-204). For `infoRoot` symmetry the encoder MUST emit the genesis MG's unrolled fieldId-6 entries identically on producer and follower (it already routes both through `currencySnapshotEntryBytes`, so symmetry is automatic — just don't special-case `Left` out of the encoder).
- Read-safe: every §2 consumer does `.getOrElse`/keyed lookup, never branches on `None` vs `Some(empty)` (Q3 spot-checked across the §2 list — confirm during impl step 3).

## 6. Write path — ALL writers of fieldId-6 (review completeness pass)

The blob is written in **five** places (encoder is shared by two; reviewer flagged `toAllStateKeyValuePairs` as missed). Every one stops writing the single fieldId-6 blob and instead emits the 8 unrolled per-entry keys (+ removals):

1. `GlobalStateConverter.currencySnapshotEntryBytes:1167-1192` — the shared bytes encoder (consumed by `toAllStateKeyValueBytes:863` AND `currencySnapshotFieldRoots:1205`). **Primary site.**
2. `GlobalStateConverter.toAllStateKeyValuePairs:458` — the JSON encoder (reviewer-flagged miss). Must mirror #1 or follow/peer JSON diverges from bytes.
3. `GlobalStateConverter.syncFromGlobalSnapshotInfo:1557` — bootstrap/resync/peer-download seed writer (used by ml0 `StateChannel.scala:250`, dl1, cl1 `CurrencySnapshotProcessor:408`). If this still writes the blob while live writes unroll, **bootstrap vs live cohorts split** — critical.
4. `GlobalStateConverter.syncFromStateChanges:1899` — the incremental delta writer.
5. `AcceptanceMptStateChanges.applyStateChanges:124-131` (overlay path) — the `AcceptanceMpt` writer.

A SINGLE shared helper (`infoEntryBytes(mgAddr, info): Map[GlobalStateKey, Array[Byte]]`) emitting the 8 per-entry keys is the one source of truth all five call — the only safe way to keep them in lockstep (mirrors how `currencySnapshotEntryBytes` is shared today).

**Removals require diffing prior-vs-new** (account dropped from `balances`, expired ref ⇒ delete its key) — extend the existing `removedAllowSpendKeys`/`removedTokenLockKeys` pattern (`AcceptanceMptStateChanges:77-98`) to the 8 fields. Prior info is materialized at accept start (GSAM:1623-1662). **Q4 resolved → derive-in-writer:** do NOT bloat `StateChangesAccumulator` with 8×(delta+removal) maps; instead the writer diffs `delta.lastCurrencySnapshots[mg]` (the new info, already carried) against the materialized prior per MG and emits per-entry upserts+removals. Keeps the accumulator shape stable and `applyAccumulatorToGSI` (the typed GSI mirror) unchanged — it still merges the whole-info `lastCurrencySnapshots` map; only the MPT serialization unrolls. Parity (`GsamWritePathParitySuite`/`RebuildVsProducerBytesAllPartitionsParitySuite`) extends to the 8 fields.

## 7. Committee-diff payoff + deletions (the §1 goal)

Once unrolled: the per-MG MPT `ChangeSet` is `O(changed entries)`. The committee ships it (`ShardCurrencyStateDiff` wire type + `ChangeSet.toWire/fromWire` — already built); gl0 applies `MerklePatriciaTrie.withChanges(finalizedBase, diff)` + verifies the recomputed `infoRoot` (+ incrementalRoot) `===` the committee-attested `CurrencySnapshotMptRoots`. Then **delete** `CurrencyAdoptionMode.AdoptFromSignedFields`, `deriveAdoptedCurrencyInfo`, `deriveNextCurrencyInfo`, the per-field gate (keep `Recreate` for `numShards=1`). Per `COMMITTEE-STATE-DIFF-ADOPTION-DESIGN.md`.

## 8. Invariants (review checklist)
- **I1 — read parity.** `reconstruct(unrolled) == blob` for every MG: a reconstructed `CurrencySnapshotInfo` is field-equal to what the blob held (modulo the documented `Some(empty)` vs `None` MPT-invisible nuance). Every §2 consumer sees identical values.
- **I2 — root parity (producer↔follower).** `infoRoot` computed by the producer over the unrolled write equals the follower's `currencySnapshotFieldRoots` recompute over the reconstructed-then-encoded state. Both go through `currencySnapshotEntryBytes`; the encoder is the single source of truth.
- **I3 — stateProof shape unchanged.** No `GlobalSnapshotStateProof` field added/removed/reordered; codec untouched; only the key-set under `infoRoot` changes. `StateProofComparison`/`StateProofValidator`/follow-verify unmodified in structure.
- **I4 — write parity.** GSI-apply (`applyAccumulatorToGSI`) and MPT-write (`syncFromStateChanges`) stay byte-equal per `GsamWritePathParitySuite`/`RebuildVsProducerBytesAllPartitionsParitySuite` (extend them to the unrolled fields).
- **I5 — removals complete.** No stale per-entry key survives a prior-vs-new deletion (else `infoRoot` diverges). Covered by a full-rebuild-vs-incremental parity test (`MptIncrementalVsFullSyncSuite` analogue).
- **I6 — `numShards=1` untouched** at the consensus-byte level beyond the deliberate (greenfield) layout change applied atomically cluster-wide.

## 9. Increment plan (each step compiles + has a parity test before the next)
1. **Schema:** add the 9 `Mg*` `GlobalStateFieldId`s + `metagraphEntry`/`metagraphFieldPrefix` key constructors + value codecs. (no behavior change yet)
2. **Encoder:** rewrite `currencySnapshotEntryBytes` to emit per-entry info keys; prove `infoRoot` for a sample info is stable producer-vs-follower (I2 unit test). This is the consensus-critical core — review hard.
3. **Decoder:** rewrite `getAllLastCurrencySnapshots` + GSAM materialization to reconstruct; round-trip test `reconstruct(encode(info)) == info` (I1).
4. **Write path:** per-entry upserts+removals in `applyStateChanges`/`syncFromStateChanges`; extend parity suites (I4/I5).
5. **Drop the blob + fieldId-7 mg-dup** (Q1); full e2e-shaped MPT parity (full-sync vs incremental).
6. **Committee diff + delete `AdoptFromSignedFields`/gate** (§7).
7. Adversarial review → gate.

## 10. Open questions — RESOLVED in design review (2026-06-13)
- **Q1 → RESOLVED (b):** keep `activeAllowSpends` in fieldId-7 (already per-MG unrolled, already read by `SpendActionValidator:363` cross-shard). Unroll covers the 8 blob-only fields; `infoRoot` covers 8; `activeAllowSpends` stays committed via the fieldId-7 `activeAllowSpends` stateProof field. `SpendActionValidator` untouched.
- **Q2 → RESOLVED:** genesis (`Left`) routes through the same encoder; symmetry automatic; reconstruction returns `Left(snap)` (info from the genesis snapshot, `toCurrencySnapshotInfo`).
- **Q3 → confirm in impl step 3** (spot-checked: §2 consumers use `.getOrElse`/keyed lookup; no `None`-branching).
- **Q4 → RESOLVED (derive-in-writer):** no new accumulator fields; the MPT writer diffs new-info-vs-materialized-prior per MG. `applyAccumulatorToGSI` unchanged.
- **Q5 → see §11.**

## 11. Scalability scope — what the unroll delivers NOW vs the lazy-reads follow-up

The unroll's PRIMARY target (the committee state-diff / gl0 MPT churn) is **O(changed entries)** — delivered. But the per-ordinal *materialization* (`getAllLastCurrencySnapshots` at GSAM:1623-1662, run for every MG every ordinal) regresses from O(MGs) point-reads to **O(total entries) prefix-scans**, and the in-memory `GlobalSnapshotInfo.lastCurrencySnapshots` map stays O(total) (gl0 is the full-state anchor — that footprint is by-design, not a target). At the e2e scale (2 tiny MGs) the materialization regression is invisible; at mainnet scale it is a real CPU cost.

**Decision:** ship the unroll now (it unblocks the small committee-diff — the actual sharded-e2e blocker — and is the data-structure change the user asked to do now). Track **lazy/keyed per-MG reads** (reconstruct only the MGs an ordinal touches, not the full map; the committee-diff adopt path skips reconstruction entirely since it applies the diff without re-deriving) as the explicit **mainnet-hardening follow-up** — it's a CPU optimization, NOT correctness, and is cleanly separable. The data-structure (the unroll) is what had to land now to avoid entrenching the blob behind the committee-diff wire format; the consumer-read optimization can follow without re-touching the schema. This split is recorded so the scalability story is honest: unroll = small diff ✓ now; lazy reads = no per-ordinal CPU regression, next.
