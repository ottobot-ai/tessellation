# Adopt the Committed Currency Delta

**Status:** DRAFT — 2026-06-05
**Goal:** eliminate gl0's currency-state **event-replay + fallback** (`deriveAdoptedCurrencyInfo`). The metagraph already computes its full currency-state change — tx balances, rewards, **owner-fee credit, unlock-expiry, cross-shard spend-actions**, all of it. Ship *that* as a typed delta; gl0 **applies it and verifies the resulting root against the committee-signed `stateProof`**. No re-derivation, no missed effects, no fallback.

**Supersedes** the replay-with-fallback half of [[project_roots_only_sharding_ii_decision]]; **is** the metagraph→gl0 mirror of [[project_ml0_diff_adopt_design]] (#12 / slices #14-19).

---

## 0. Why

`deriveAdoptedCurrencyInfo` (GSCEP:380-517) re-derives the per-MG `CurrencySnapshotInfo` by replaying the signed snapshot's events, then gates on `derivedRoot === committedStateProof`. A pure replay **cannot reproduce** effects that depend on metagraph-internal context (fee→owner credit, `unlockEpoch ≤ epoch` expiry, cross-shard spend-actions), so those snapshots **fail the gate → fallback**: gl0 carries the *prior* (stale) balances forward. gl0's currency view then drifts for any fee/lock/spend-active metagraph. Completing the replay is whack-a-mole (gl0 would forever mirror the metagraph's acceptance logic). The fix is to stop re-deriving and **adopt the metagraph's actual computed change**.

---

## 1. This is the proven gl0→ml0 changeset pattern, reversed

The gl0→ml0 follow pipeline (#12) already does exactly this in the other direction, and its **trust model is the load-bearing reusable idea**:

> **The delta is untrusted transport. Trust comes from recomputing the cryptographic root and matching it against the committee-signed value already in hand.**

In gl0→ml0: ml0 applies `StateChangesAccumulator` to its prior GSI and commits only if recomputed `mptRoot === snapshot.stateProof.mptRoot` (`GlobalStateConverter.adoptAndVerifyChangeSetDelta:291-329`, the `withTransaction` Commit/Rollback bracket). For **ml0→gl0**, the metagraph's signed **`CurrencySnapshotStateProof`** root plays the exact role gl0's `mptRoot` plays — and gl0's verify gate (GSCEP:497-516) already implements it.

Reusable verbatim: the scodec **codec pattern** (`StateChangesAccumulatorCodec` — hand-written shapeless-HList + `ImmutableCodec.fromScodecCodec` + sorted-set determinism + round-trip test), the per-field **diff/apply** algebra (`ConsumedFieldDelta.diff` / `(prior -- removals) ++ upserts`, `FollowVerifyCore:245-270`), and the **verify-then-adopt** gate.

---

## 2. The key simplification: inline-attached, **no ring / no transport**

The gl0→ml0 direction needed a per-ordinal ring + `/global-follow/changeset?since=` route + majority-peer fetch **because ml0 pulls from gl0**. The ml0→gl0 direction does **not**: gl0 *already receives* the metagraph's signed SC binary (the `CurrencyIncrementalSnapshot`) through the normal state-channel/checkpoint path. **Attach the delta to that snapshot** and gl0 reads it inline. So slices #15 (ring) and #16 (transport) of the gl0→ml0 work have **no analog here** — this is materially smaller than the forward pipeline.

---

## 3. Design — three moves

### 3.1 Producer (ml0): emit the delta at `accept`
`CurrencySnapshotAcceptanceManagerImpl.accept` (`managers/currency/CurrencySnapshotAcceptanceManager.scala:205-646`) already folds **every** effect and builds the full new `csi` (`:593-608`); the prior Info is in scope (`lastSnapshotContext.snapshotInfo`). Emit `delta = CurrencySnapshotInfoDelta.diff(prior, csi)` right there and thread it onto the acceptance result (`Models.scala:30-44`). Local, additive.

### 3.2 Wire: attach to `CurrencyIncrementalSnapshot`
Add `stateDelta: Option[CurrencySnapshotInfoDelta]` to `CurrencyIncrementalSnapshot` (`currency.scala:221-241`). It rides inside the serialized `content` of the SC binary automatically (`StateChannelSnapshotService.createBinary:101-113`), is covered by the metagraph's signature, and — because the binary is what the shard checkpoint's `includedSnapshots` carries — reaches gl0's adopt path unchanged for **both** shard and non-shard paths. (Alternative for the sharded path only: a `perMetagraphStateDelta` field on `ShardDerivedStateDelta` — the documented committee-signed extension point. Prefer the snapshot field; it's universal.)

### 3.3 Consumer (gl0): apply + verify (surgical swap)
In `deriveAdoptedCurrencyInfo` (GSCEP:380-517): if `snapshot.value.stateDelta` is present, `candidate = applyDelta(lastState, delta)` (reuse the `applyDelta` per-address helper already at GSCEP:413-420, generalized per field); **else** keep the current event-replay (backward-compat for pre-delta snapshots). Then the **existing verify-root gate (:497-516) and fallback stay verbatim**. With a real shipped delta the recomputed `stateProof === committed stateProof` always holds → the fallback goes dormant; gl0 maintains the correct full Info.

---

## 4. The delta type — `CurrencySnapshotInfoDelta`

`CurrencySnapshotInfo` (currency.scala:88-117) is 9 `SortedMap` fields; its `stateProof` (currency.scala:100-114) is a **per-field hash** (9 independent hashes). That's the clean fit: a per-field delta maps 1:1 onto the proof, and **applying a delta to `balances` only re-hashes `balances`** — the other 8 proof slots are untouched unless their field changed.

Net-new type (9 fields, each `upserts` + a `removed` key-set), but heavy pattern reuse from `ConsumedFieldDelta`:
- **5 fields structurally identical** to `ConsumedFieldDelta`: `balances`, `lastTxRefs`, `lastAllowSpendRefs`, `lastTokenLockRefs`, `activeTokenLocks` (same value types).
- **4 net-new**: `activeAllowSpends` (`SortedSet[Signed[AllowSpend]]`), `lastFeeTxRefs` (`Address→TxRef`), `lastMessages` (`MessageType→Signed[CurrencyMessage]`), `globalSnapshotSyncView` (`PeerId→Signed[GlobalSnapshotSync]`).
- Reuse `diff` (upserts/removed per map) + apply (`(prior -- removed) ++ upserts`) + the scodec/`ImmutableCodec`/round-trip pattern verbatim.

---

## 5. Trust model (no new assumption)

The shipped delta is **untrusted**. gl0 recomputes `applyDelta(prior, delta).stateProof(ordinal)` and adopts **only if** it `===` the metagraph's committed `stateProof` (which gl0 already holds in the signed binary). A wrong/tampered/wrong-base delta surfaces as a root mismatch → fallback, never advances unverified state. **Economic security is the committee** (it re-executes the full currency logic and attests the per-MG root); gl0 *materializes* that committee-attested state via the delta rather than re-deriving it. This is the correct division of labor and exactly the gl0→ml0 trust model.

---

## 6. Slice plan

- **S1** — `CurrencySnapshotInfoDelta` (9 fields) + `diff`/`apply` + scodec codec + `ImmutableCodec` + round-trip test. (Mirror `StateChangesAccumulatorCodec` + `ConsumedFieldDelta`.)
- **S2** — ml0 emits `diff(prior, csi)` at `accept`; thread through the acceptance result; add the `stateDelta` field to `CurrencyIncrementalSnapshot`.
- **S3** — gl0 `deriveAdoptedCurrencyInfo`: apply the attached delta when present (keep replay as backward-compat), verify-root gate unchanged.
- **S4** — e2e at 8gl0+4mg+4shards: assert the **fallback counter → 0** through the fee/token-lock/allow-spend/spend-action traffic; gl0's per-MG balances byte-match ml0's.

(No ring slice, no transport slice — §2.)

---

## 7. Reuse vs net-new

| Layer | Reuse (verbatim/pattern) | Net-new |
|---|---|---|
| Delta type | `ConsumedFieldDelta` diff/apply algebra; 5 of 9 fields identical | `CurrencySnapshotInfoDelta` (4 extra fields) |
| Codec | `StateChangesAccumulatorCodec` hand-written-scodec + `ImmutableCodec.fromScodecCodec` + round-trip pattern | the per-field `f1..f9` codecs |
| Transport | — (inline on the snapshot; no ring/route) | one optional field on `CurrencyIncrementalSnapshot` |
| Consumer | verify-root gate + fallback (GSCEP:497-516) verbatim; `applyDelta` helper (GSCEP:413-420) | apply-attached-delta branch (replaces the replay) |
| Trust | recompute-root-match-committee-signed (identical model) | `CurrencySnapshotStateProof` is the commitment (already exists) |

---

## 8. File:line swap points

| Concern | File:line |
|---|---|
| ml0 emission point (`accept`, full Info + prior in scope) | `node-shared/.../managers/currency/CurrencySnapshotAcceptanceManager.scala:593-608` |
| Acceptance result (thread the delta) | `managers/currency/Models.scala:30-44` |
| Snapshot field to add | `shared/.../currency/schema/currency.scala:221-241` |
| Serialize-to-binary (rides content) | `currency-l0/.../StateChannelSnapshotService.scala:101-113` |
| gl0 adopt swap (replay → apply-delta) | `node-shared/.../global/GlobalSnapshotStateChannelEventsProcessor.scala:402-494` |
| gl0 verify-root gate (keep) | same file `:497-516` |
| `CurrencySnapshotInfo` + per-field `stateProof` | `currency.scala:88-117`, `:100-114` |
| Reuse: `ConsumedFieldDelta.diff`/apply | `shared/.../schema/nakamoto/follow/FollowVerifyCore.scala:209-289` |
| Reuse: scodec codec pattern | `shared/.../serde/codecs/instances/StateChangesAccumulatorCodec.scala` |

---

## 9. Open decisions
1. **Attach site:** `CurrencyIncrementalSnapshot.stateDelta` (universal, recommended) vs `ShardDerivedStateDelta.perMetagraphStateDelta` (sharded-only, committee-signed). → snapshot field.
2. **Keep the event-replay** as a backward-compat path for pre-delta snapshots, or hard-require the delta (greenfield)? → keep it as fallback initially; it goes dormant once all metagraphs ship deltas.
3. **Data-app blob state:** same delta envelope can carry the data-app `CalculatedState` diff later (adopt, not validate — per the currency-vs-app split). Out of scope for S1-S4.

## 11. Read-dual: cross-shard reads via inclusion proofs

The delta-adopt is the **write** leg (metagraphs push state up to gl0). The **read** leg lets `ml0_A` read `ml0_B`'s committed state to facilitate cross-shard transactions — same architecture, reversed, **proxied through gl0** (which already holds B's state, so no ml0_A↔ml0_B channel exists). One per-address MPT commitment serves **both** TS light clients and other metagraphs.

**Mechanism (deterministic + trustless):**
1. **Commit provably** — the metagraph commits its balances in a **per-address MPT** (the `BalanceMpt` / `Hasher.forCanonicalJson` tree the light client already verifies), root in `CurrencySnapshotStateProof.balancesProof` (today a flat hash — promote it to the MPT root). gl0 holds it via the delta-adopt.
2. **gl0 serves inclusion proofs** over it — `ShardSubtreeProofService` (built but currently **unwired**); proof = `{metagraphB, addressX, valueV, mptProof}` against gl0's committed per-MG root.
3. **Proof-carrying cross-shard tx** — `ml0_A`'s tx carries `(V, gl0_ordinal_N, proof)`; every `ml0_A` validator verifies it against the gl0 snapshot N it **already follows** (same committee-signed root → deterministic), then reads `V` locally. This is [[project_cross_shard_message_passing_direction]].

**Scoping boundary:** an inclusion proof gives *visibility* ("at gl0 ord N, B had V"). A full **atomic** cross-shard transfer also needs B to authorize + A to consume — the **allow-spend → spend-action** handshake, whose primitives (`activeAllowSpends`, cross-shard `spend-actions`) the delta-adopt already carries. Read (visibility) + allow-spend/spend-action (atomic move) = full cross-shard tx.

**Additional slices (after S1-S4):**
- **S5** — promote `balancesProof` flat-hash → per-address `BalanceMpt` root; gl0 commits/holds it via the delta.
- **S6** — wire `ShardSubtreeProofService` → a `GET /…/balance/proof`-style route (already written, just dark).
- **S7** — cross-shard-dependency tx format `(value, gl0_ordinal, inclusionProof)` + admission-time verification against the follower's gl0 root (Scala mirror of `mptVerifier.ts`).

Net: **one per-address MPT commitment**, **one inclusion-proof verifier**, two consumers (light client + sibling metagraph). The write leg (delta-adopt) and read leg (inclusion proof) together are the complete cross-shard state architecture.

## 10. Related
[[project_ml0_diff_adopt_design]] · [[project_roots_only_sharding_ii_decision]] · [[project_cross_shard_message_passing_direction]] · `ROOTS-ONLY-SHARDING-ARCHITECTURE.md` · `UNIFIED-STATE-PROPAGATION.md`
