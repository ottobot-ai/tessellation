# The Re-Exec ↔ Byte-Diff Byte-Identity Contract (Track-1)

> **HISTORICAL, SUPERSEDED PRE-FIX CONTRACT.** The adopter/byte-diff architecture
> described below is evidence, not a compatibility requirement. The current
> target is committee replay-before-sign: the producer and every execution signer
> reproduce the exact framework result; a noncommittee GL0 node verifies the
> execution certificate, namespace/base/continuity, applies the certified diff,
> and recomputes the root; assigned watchtowers replay as the collusion backstop.
> A root or diff without replay-backed execution signatures is only a claim. This
> file remains solely as evidence of an earlier implementation/design state. See
> [`../../AGENTS.md`](../../AGENTS.md), ADR-0016, and ADR-0017.
> References below to deleted delta/adoption documents are historical evidence
> recoverable with `git show 725b25b:<path>`, not dangling design dependencies.

**Purpose.** Exact, evidence-grounded specification of the Track-1 safety claim so Fable can attack it.

> **THE CLAIM.** For a sharded metagraph, the `CurrencySnapshotInfo` that a *byte-diff adopter*
> (`gl0`/follower, no re-execution) commits is **byte-identical** to the one an honest *re-executor*
> (shard-committee member / watchtower, re-running `accept()`-derivation over the same pinned base)
> computes — and both equal the committee-attested per-MG root `perMetagraphMptRoots(mg)`.

All anchors are `file:line` against the working tree at branch `feature/committee-state-diff`
(HEAD `21933559c`). Every factual sentence carries an anchor; unverifiable statements are marked
`UNVERIFIED`.

Scope note: this contract governs the **per-MG currency state** (`ShardDerivedStateDelta.perMetagraphStateDiff` +
`perMetagraphMptRoots`). It does NOT govern the global `smtRoot` (NIPoPoW), which is path-dependent by
design and excluded from the consensus compare (§2.4).

---

## 1. The two paths, side by side

Both paths ultimately produce a per-MG `CurrencySnapshotInfo` (`nextInfo`) and its component-addressable
root, then compare that root to the committee-attested `perMetagraphMptRoots(mg)`. They differ only in
*how* `nextInfo` is obtained: the re-executor **re-runs the derivation**; the adopter **applies a wire
diff**.

### 1.0 Shared inputs (both paths)

| Input | Source | Anchor |
|---|---|---|
| `mg: Address` | checkpoint's `includedSnapshots` / `perMetagraphStateDiff` key | `ShardDerivedStateDelta.scala:87-94` |
| `binaries: NonEmptyList[Signed[SCSB]]` | `derivedStateDelta.includedSnapshots(mg)` (oldest-first on wire) | `ShardDerivedStateDelta.scala:90` |
| `gl0AnchorOrdinal` | `ShardCheckpoint.gl0AnchorOrdinal` — exec/fee-cutover CONTEXT, **not** the diff base | `ShardCheckpoint.scala:72`, `39-46` |
| `executionBaseOrdinal` | `ShardCheckpoint.executionBaseOrdinal` — the cluster-uniform pinned base `S(N)` | `ShardCheckpoint.scala:78`, `56-65` |
| `perMetagraphMptRoots(mg): Hash` | committee-attested claim (the thing verified against) | `ShardDerivedStateDelta.scala:88`, `64-66` |
| `perMetagraphStateDiff(mg): ShardCurrencyStateDiff` | committee's minimal byte-diff over `S(N)` | `ShardDerivedStateDelta.scala:89`, `67-71` |

### 1.1 The RE-EXECUTOR path — `reExecDerivationWithDiff`

Producer (`GlobalSnapshotConsensus.derivePerMgState`, `GlobalSnapshotConsensus.scala:1833-1843`), watchtower
(`GlobalSnapshotConsensus.scala:597-615`), and sub-quorum failover (`SharedServices.scala:321-351`) all
call `ShardCheckpointWiring.reExecDerivationWithDiff` (`ShardCheckpointWiring.scala:210-379`).

1. **Resolve the pinned base reader** at `executionBaseOrdinal` via the injected
   `priorReaderAt: SnapshotOrdinal => F[Option[GlobalStateReader[F]]]` (`ShardCheckpointWiring.scala:217`,
   `270`). `None` ⇒ **OMIT (defer)** — never derive over a wrong base (`ShardCheckpointWiring.scala:271-277`).
   - Producer/watchtower on gl0 rail: `finalizedReaderAt` = **fast-path live reader** when
     `executionBaseOrdinal == mptStore.lastPersistedOrdinal`, else `gl0PinnedReader.pinnedReaderAt(ord)` over the
     contiguous `signedBytesStore` (`GlobalSnapshotConsensus.scala:577-589`).
   - Sub-quorum failover + createContext re-derive on follower rail: `liveReaderAt` — **ignores the
     ordinal, always the live base** (`SharedServices.scala:327-331`, `363-367`) — justified by the claim
     that the *root* is base-independent (§4.7, `SharedServices.scala:323-326`).
2. **Two priors, deliberately different reads** (`ShardCheckpointWiring.scala:278-288`):
   - *derivation seed* = `priorState` = `getLastIncrementalCurrencySnapshot` (`Right`) / `getLastCurrencySnapshot`
     (`Left` genesis) / `None` (`ShardCheckpointWiring.scala:244-256`);
   - *diff prior* = `getCurrencySnapshotInfo` (gated on fieldId-5 incremental; `emptyInfo` at a
     not-yet-unrolled genesis) (`ShardCheckpointWiring.scala:284-286`).
3. **Derive** with `processor.processCurrencySnapshots(gl0AnchorOrdinal, empty balances, priorMap,
   SortedMap(mg -> binaries.reverse), noGlobalSnapshotLookup, AdoptFromSignedFields)`
   (`ShardCheckpointWiring.scala:297-305`). Window is reversed to NEWEST-FIRST (order contract,
   `ShardCheckpointWiring.scala:295-296`). `noGlobalSnapshotLookup` is a pure `_ => None` — **zero storage
   reads**, split-safe (`ShardCheckpointWiring.scala:226-227`).
4. `next` = the LAST resulting per-MG state (`ShardCheckpointWiring.scala:308-309`); `None` ⇒ OMIT
   (`ShardCheckpointWiring.scala:355-367`); crash ⇒ OMIT (`ShardCheckpointWiring.scala:370-376`).
5. **Output object** `Option[(Hash, ChangeSet)]`:
   - `root = GlobalStateConverter.currencySnapshotMgRoot(SortedMap(mg -> next))` (PIN-1)
     (`ShardCheckpointWiring.scala:346`);
   - `diff = ChangeSet.currencyInfoChangeSet(mg, priorInfo, nextInfo)` (PIN-2/3, minimal)
     (`ShardCheckpointWiring.scala:353`).
   Watchtower/sub-quorum discard the `ChangeSet` and keep only `_1` (the root) for a Hash-compare
   (`GlobalSnapshotConsensus.scala:614`, `SharedServices.scala:350`).

The **producer** stamps `executionBaseOrdinal = mptStore.lastPersistedOrdinal` atomically with its prior read
(`GlobalSnapshotConsensus.scala:1844-1846`) and writes `root`→`perMetagraphMptRoots`, `toWire(diff)`→
`perMetagraphStateDiff` into the checkpoint's `ShardDerivedStateDelta`.

Note: `reExecDerivationWithDiff` ≠ the older `reExecDerivation` (`ShardCheckpointWiring.scala:166-177`),
which seeds from an **empty** prior (base-independent, root-only, emits `hash((mg,state))` not the PIN-1
root) and is NOT the Track-1 path.

### 1.2 The ADOPTER path — `deriveAdoptedCurrencyState` + `reconstructInfoFromDiff`

Entry: `accept()` calls `deriveAdoptedCurrencyState` only when `adoptedScSnapshots.nonEmpty`
(`GlobalSnapshotAcceptanceManager.scala:2308-2317`); adopted per-MG state is right-biased-merged over the
chain-link `baseAcceptance` (`GlobalSnapshotAcceptanceManager.scala:2318-2323`).

Upstream, `adoptShardCheckpoints` (`GlobalSnapshotAcceptanceManager.scala:742-971`) runs the
**deterministic** `verifyEmbedded` adopt-verifier (`:791`) and, per adopted MG, records the tuple
`(diff, attestedRoot, cp.executionBaseOrdinal)` into `perMgDiffAndRoot` (`:859-868`). Bounded-defer: if a
checkpoint's `executionBaseOrdinal` is ahead of this node's `mptStore.lastPersistedOrdinal`, the WHOLE checkpoint
is deferred (`:774`, `:799-810`).

Then, per MG (`GlobalSnapshotAcceptanceManager.scala:1006-1219`):

1. **Build the result shell** via the SAME `processCurrencySnapshots(... AdoptFromSignedFields)` over the
   node-local `priorLastCurrencySnapshots` (`:1016-1037`). This shell yields `lastIncremental` (the last
   binary's signed incremental) — the fieldId-5 half of the root — and the fee `balanceUpdate`. Its *info*
   half is **discarded** and overridden below (`:1080-1081`, `:991-998`).
2. **Resolve the pinned diff prior** at `cp.executionBaseOrdinal` via
   `pinnedCurrencyInfoReader.readAtOrdinal(executionBaseOrdinal, mg)` (`:1074-1078`, `:1096`).
   - reader `None` (tests/currency-l0) ⇒ fall back to node-local `priorInfoOf` (`:1076`, `:1058-1063`);
   - reader `Some`, returns `None` ⇒ **FAIL-CLOSED DROP** this MG (`:1097-1103`);
   - reader `Some`, returns `Some(pinnedPrior)` ⇒ proceed (`:1104`).
3. `nextInfoRaw = ChangeSet.reconstructInfoFromDiff(mg, pinnedPrior, ChangeSet.fromWire(wireDiff))`
   (`:1093`, `:1106`). This is: rebuild `pinnedPrior`'s full unrolled entry map, apply `removals` then
   `upserts` (upsert-wins), reconstruct via `reconstructCurrencyInfoFrom` (`ChangeSet.scala:151-172`).
4. `nextState = Right((lastIncremental, nextInfo))` (`:1117-1118`).
5. **PIN-1 gate:** `recomputed = currencySnapshotMgRoot(SortedMap(mg -> nextState))`; require
   `recomputed === attestedRoot` (`:1123-1125`); mismatch ⇒ **DROP** the MG (`:1205-1216`).
6. **GAP-1 gate** (§3.2): bind each reconstructed field to the metagraph's OWN signed
   `lastIncremental.value.stateProof` (`:1126-1189`); any present-and-mismatched field ⇒ DROP (`:1192-1203`).
7. On all-pass, adopt `(mg -> nextState)` (`:1191`).

**Output object:** a `StateChannelAcceptanceResult` whose `calculatedCurrencyState[mg]` is the verified
`Right((lastIncremental, nextInfo))` (`GlobalSnapshotAcceptanceManager.scala:1082-1218`).

### 1.3 Pinned-base resolution — the 2-level pin (`PinnedCurrencyInfoReader`)

Both paths resolve `S(N)` through `PinnedCurrencyInfoReader` (`PinnedCurrencyInfoReader.scala`). The
adopter uses `readAtOrdinal`/the whole-global variant is `pinnedReaderAt`; the re-executor's
`finalizedReaderAt` uses `pinnedReaderAt` off the fast path.

`readAtOrdinal(ordinal, mg)` (`PinnedCurrencyInfoReader.scala:147-163`) and `pinnedReaderAt`
(`:165-177`) **self-resolve** the pin (no independently-carried hash — the pin IS the finalized snapshot
at `ordinal` on this node's chain, `:88-93`), then both funnel through `withVerifiedAnchorBytes`
(`:186-223`):

- **Level 1 — content-hash pin.** Resolve the finalized snapshot at `ordinal`; require
  `snap.hash === expectedGlobalSnapshotHash` (self-resolved = the snapshot's own hash) — else hard-reject
  (`:192-193`, `:219-222`). This ties the base to a specific snapshot on this node's canonical chain.
- **Level 2 — mptRoot reproduction.** Require `snap.stateProof.mptRoot` present (`:194-198`); read the
  version-retained state bytes at `ordinal` from `byteStore.readState(ordinal)` (`:200`); require
  `GlobalSnapshotInfo.consensusMptRoot(bytes) === snap.stateProof.mptRoot` (`:207-213`) — else
  hard-reject. This proves the retained bytes reproduce the committed root before any reconstruction runs.

`None` (a clean verify with no state, OR any reject) is indistinguishable by design and is NEVER a HEAD
fallback (`:31-41`, `:179-184`).

**Retention asymmetry (liveness, not safety — but see §4.6):**
- gl0 rail: `byteStore = signedBytesStore` (`<mptSnapshotInfoPath>_signed`, contiguous
  `ContiguousOrdinalCutoff`, depth k₂=100·k₁) — serves any anchor within k₂ (`:42-49`,
  `GlobalSnapshotConsensus.scala:569-573`, `:682-689`).
- follower/createContext rail: `byteStore = pinnedByteStore` (`cfg.mptSnapshotInfoPath`, **logarithmic**
  retention) — by-ordinal reads MISS unless on the log ladder ⇒ **hard-rejects most historical anchors**
  (`:49-55`, `SharedServices.scala:184`, `:187-188`).

---

## 2. Serialization and the compared root

### 2.1 What bytes are produced

Per-MG entry bytes come from ONE encoder, `GlobalStateConverter.currencySnapshotEntryBytes` →
`infoEntryBytes` (`GlobalStateConverter.scala:1196-1228`, `1285-1306`), which emits the fieldId-5 incremental
plus the 8 unrolled `Mg*` info sub-fields (one MPT entry per account/holder/messageType/peer). Values are
`ImmutableCodec.immutableBytes` (scodec/hand-rolled canonical), NOT Circe JSON, for the byte-diff path.
`activeAllowSpends` (fieldId-7) is encoded separately with `signedAllowSpendSetCodec` and unioned in by
`ChangeSet.fullInfoEntriesHex` (`ChangeSet.scala:119-136`).

Keys are hashed via `GlobalStateKey.toHex` (`ChangeSet.scala:133-134`). Because `toHex` HASHES the entry
key (lossy), each value carries its own typed `(key, value)` tuple so `reconstructCurrencyInfoFrom` recovers
the logical key by prefix scan (`GlobalStateConverter.scala:1183-1186`).

### 2.2 The compared root (PIN-1)

`currencySnapshotMgRoot(data)` (`GlobalStateConverter.scala:1384-1389`) =
`fieldRootFromBytes(currencySnapshotMgEntries(data))`:
- `currencySnapshotMgEntries` keeps ONLY `isCurrencyMgCommitmentField` entries: fieldId-5 incremental +
  the 8 `infoSubFields` `Mg*` partitions (`GlobalStateConverter.scala:1353-1363`, `1339-1340`).
  **fieldId-32 `MgGlobalSnapshotSyncView` is deliberately EXCLUDED** — observation-dependent, no per-field
  proof slot; folding it in re-froze adoption (`GlobalStateConverter.scala:1331-1340`).
- `fieldRootFromBytes` (`GlobalStateConverter.scala:1179-1181`) = `Hash.empty` on empty, else
  `MerklePatriciaTrie.makeParallelFromBytes(entries).rootHash` — a **real MPT root** (so it backs single-leaf
  inclusion proofs, `:1365-1372`). The empty→`Hash.empty` convention is determinism-load-bearing (`:1174-1177`).

This is a Hasher/MPT root over `ImmutableCodec` bytes — **NOT** `Hasher.hash` over a Circe case class. The
same helper backs all THREE PIN-1 sites (producer, adopter, proof service) so bytes are identical by
construction (`GlobalStateConverter.scala:1374-1379`).

**ECO-F32 is not closed by this exclusion.** `ShardCheckpointWiring` supplies the full prior
`CurrencySnapshotInfo` to framework replay, so replay consumes `globalSnapshotSyncView` even though
PIN-1 and the global `mptRoot` omit field 32. A locally staged base can retain a nonempty view while a
root-verified peer backfill strips it and reconstruction produces `Some(empty)`. The signed
incremental binds only `CurrencySnapshotStateProof.globalSnapshotSync` plus accepted sync deltas, not
the exact optional full-view preimage or explicit ML0 operator population. This is HIGH, CONFIRMED,
and OPEN: the signed/root-bound replay artifact must carry those exact inputs before field 32 is
removed from every GL0 MPT/diff/load/reorg path.

### 2.3 What "byte-identical" is actually checked

The comparison is a single `Hash` equality: adopter's `recomputed === attestedRoot`
(`GlobalSnapshotAcceptanceManager.scala:1125`); re-executor's `reDerived === claimed`
(`ShardCheckpointGl0AcceptanceManager.scala:558`, `:422-423`). The round-trip invariant that makes them
agree is `reconstructInfoFromDiff(mg, prior, currencyInfoChangeSet(mg, prior, next)) === next`
(`ChangeSet.scala:144-149`) — but this ONLY holds when the adopter's `prior` (`pinnedPrior`) is
byte-identical to the producer's `prior` (`priorInfo`). Diff-base-pin is the mechanism that enforces that.

### 2.4 The `smtRoot` / attachSmtRoot precedent (a DIFFERENT root — excluded)

The global `smtRoot` (NIPoPoW historical-commitment SMT) is attached to the SIGNED proof by `attachSmtRoot`
(`GlobalSnapshotAcceptanceManager.scala:573-602`, called at `:3295`). It is **path-dependent** — it folds
the locally-resolved snapshot[N−k] into a separately-maintained accumulating SMT, so honest nodes provably
cannot reproduce it in lockstep (`GlobalSnapshotConsensusFunctions.scala:279-285`,
`GlobalSnapshotAcceptanceManager.scala:558-571`). Commit **`376d09fbc`** ("fix(nakamoto): kill fork storm at
source — smtRoot-blind consensus compare + revert k2-freeze") resolved the resulting ~97% content-validation
fork storm by making the consensus re-derivation `===` compare **smtRoot-blind**
(`smtRootBlind`, `GlobalSnapshotConsensusFunctions.scala:286-295`) rather than by making the value
deterministic. `smtRoot` is NOT part of any per-MG currency root, so it does not affect adopt-vs-re-exec
per-MG byte-identity — but it is the canonical precedent that **a path-dependent value must not enter
a determinism-gated compare without an exact reproducible preimage**. The field-32 filter in §2.2
prevents a direct root fork, but §2.2's ECO-F32 gap proves that exclusion alone is insufficient when
framework replay still consumes the value.

---

## 3. The trust boundary

### 3.1 What is SIGNED and what the committee attests

The committee members sign `ShardCheckpoint.signingPreimage`, which returns
`ShardCheckpointSigPreimageV2` (`ShardCheckpoint.scala:91-102`) — the full envelope **minus
`committeeSignatures`** (`ShardCheckpoint.scala:28-30`, `115-125`, `134-145`). Fields signed (all
consensus-load-bearing, frozen-shape, derevo-magnolia Circe → `Hasher[F]`):

`shardId, parentCheckpointHash, shardOrdinal, gl0AnchorOrdinal, slot, derivedStateDelta, emittedReceipts,
epoch, executionBaseOrdinal` (`ShardCheckpoint.scala:135-145`).

Critically, `derivedStateDelta` (hence `perMetagraphMptRoots`, `perMetagraphStateDiff`, `includedSnapshots`)
AND `executionBaseOrdinal` are inside the signed preimage (`ShardCheckpoint.scala:98`, `101`). On the wire,
`executionBaseOrdinal` is proto field **11** `diff_base_ordinal` (`p2p/proto/sidecar.proto:212`); it is part of
the V2 sig preimage (comment, same line). V1 (`ShardCheckpointSigPreimage`, `ShardCheckpoint.scala:115-125`)
is retained only for a round-trip fixture (`:127-133`).

So a committee quorum attests: *"over base `executionBaseOrdinal`, applying `perMetagraphStateDiff` to metagraph
`mg` yields a per-MG state whose PIN-1 root is `perMetagraphMptRoots(mg)`, from binaries `includedSnapshots`."*

### 3.2 What a byte-diff adopter CAN independently check (without re-executing)

1. **PIN-1 root reproduction.** Apply the signed diff to its own pinned `S(N)`, recompute the PIN-1 root,
   require `=== attestedRoot` (`GlobalSnapshotAcceptanceManager.scala:1123-1125`). Ties the adopted value to
   the committee's signed claim AND to the metagraph's signed incremental (fieldId-5 half of the root).
2. **GAP-1 metagraph-signature binding.** Independently hash each reconstructed field and compare to the
   **metagraph's own** `lastIncremental.value.stateProof` (`GlobalSnapshotAcceptanceManager.scala:1143-1189`):
   - `balancesProof`, `lastTxRefsProof` — non-Option `Hash`, **UNCONDITIONAL** compare (`:1168-1169`);
   - `activeAllowSpends, activeTokenLocks, lastFeeTxRefsProof, lastAllowSpendRefsProof,
     lastTokenLockRefsProof, lastMessagesProof` — `Option[Hash]`, **compared only when
     `sp.<field>.isDefined`**, else SKIPPED (`:1170-1186`).
   This is the "verify-by-proof" Byzantine-producer check: gl0 adopts a value only if the metagraph itself
   signed it.
3. **2-level base pin.** The base `S(N)` it applies onto is itself content-hash-pinned + mptRoot-reproduced
   (§1.3).

### 3.3 What a byte-diff adopter CANNOT independently check

- **That the committee actually re-executed.** On the QUORUM happy path, `verifyEmbedded` accepts on
  **signatures only** — `distinctSigners >= kQuorum ⇒ Accepted`, NO re-exec, NO root recomputation
  (`ShardCheckpointGl0AcceptanceManager.scala:386-396`; `evaluate` T_count path identical, `:335-345`).
  Re-execution (`reExecPath`, comparing `reDerived === perMetagraphMptRoots`) fires ONLY on the SUB-QUORUM /
  depth-degraded path (`:397-407`, `:346-352`, `:545-587`). So on the common path the *enforcement* that
  "the attested root equals an honest re-exec" is delegated to (a) the attesting committee members
  themselves, (b) the watchtower dispute path, and (c) the GAP-1 metagraph-signature binding above — NOT to
  gl0's adopt gate.
- **`UNVERIFIED — do committee members re-execute before attesting?`** No `ShardCheckpointAttestationEmitter`
  source file was found on the searched path, and no re-exec/root-recompute was located in the emit path.
  If the attestation emitter signs a checkpoint that merely became its best tip WITHOUT recomputing the
  per-MG root, then the entire "committee re-execute + attest" enforcement on the quorum path reduces to the
  GAP-1 metagraph-signature binding (§3.2.2), and any field NOT covered by GAP-1 (Option fields with
  `sp.<field> = None`, §4.2) is guarded ONLY by PIN-1 root equality against an unre-executed attestation.
  **This is the single most important thing for Fable to pin down.**

---

## 4. The divergence surface (adopt ≠ re-exec)

Enumerated places the adopted `nextInfo` and an honest re-exec's `next` could differ. For each: whether
current code closes it, with anchor.

### 4.1 Map/set ordering nondeterminism — CLOSED
All wire maps are `SortedMap`/`SortedSet` (`ShardDerivedStateDelta.scala:37-40`, `86-94`, `60-62`); diff
keys are `Hex` (total order); the PIN-1 root is an MPT root (order-independent) built via
`makeParallelFromBytes` (`GlobalStateConverter.scala:1179-1181`); `committeeFor` folds in sorted PeerId order
into a `Set` (`ShardCheckpointWiring.scala:566-578`). `ChangeSet.upserts` is a plain `Map[Hex,Array[Byte]]`
but is only ever materialized into an MPT and never hashed as a map (`ChangeSet.scala:35-38`, `156-157`).
**Residual:** `ChangeSet` structural equality is by `Array[Byte]` reference, not value
(`ChangeSet.scala:32-33`) — harmless for the overlay but a trap for any code that compares ChangeSets.

### 4.2 `Some(empty)` vs `None` field presence — PARTIALLY CLOSED (the GAP-1 hole)
`reconstructCurrencyInfoFrom` ALWAYS lifts `.some` on all 6 Option info fields (a partition with zero
entries reconstructs `Some(empty)`, `GlobalStateConverter.scala:1473-1484`), whereas a live metagraph emits
genuine `None` (e.g. `lastFeeTxRefs` hardcoded `None`; `lastMessages` `None` when message-free,
`GlobalSnapshotAcceptanceManager.scala:1133-1139`). The PIN-1 root cannot distinguish them (both = zero
partition entries ⇒ same MPT root, `:1138-1139`), so `recomputed === attestedRoot` still holds. GAP-1
handles the divergence by SKIPPING any Option field whose `sp.<field>` is `None`
(`GlobalSnapshotAcceptanceManager.scala:1170-1186`).
**Residual attack (see §3.3):** because those fields are BOTH invisible to the PIN-1 root (when empty) AND
skipped by GAP-1 (when `sp.<field> = None`), the only guard on a NON-empty Option field is
`recomputed === attestedRoot` — i.e. an attestation the adopter did not re-execute. If a Byzantine producer
puts real entries into (say) `activeAllowSpends` while the metagraph's own `stateProof.activeAllowSpends`
is `None`, the diff→reconstruct yields `Some(nonEmpty)`, the recomputed root includes them, and adoption
turns entirely on whether kQuorum members signed that root honestly. `UNVERIFIED` whether
`stateProof.activeAllowSpends` can legitimately be `None` while entries exist for a live metagraph — Fable
should test.

### 4.3 Base drift — `executionBaseOrdinal` not pinned identically — CLOSED on gl0 rail, HAZARD on follower rail
The whole point of `executionBaseOrdinal` (`ShardCheckpoint.scala:56-65`): producer + every re-executor + every
adopter read `S(N)` at the SAME cluster-uniform ordinal, not each node's lagging `overlay.base`.
- Producer stamps `mptStore.lastPersistedOrdinal` atomically (`GlobalSnapshotConsensus.scala:1844-1846`).
- Adopter reads at `cp.executionBaseOrdinal` via `readAtOrdinal` (`GlobalSnapshotAcceptanceManager.scala:1074-1078`).
- Producer/watchtower read at `executionBaseOrdinal` via `finalizedReaderAt` (`GlobalSnapshotConsensus.scala:600`,
  `:1836`).
- Below k₁ optimistic finality the resolved snapshot is cluster-uniform (`PinnedCurrencyInfoReader.scala:93`).
**HAZARD:** the sub-quorum failover re-exec and the createContext re-derive read the **LIVE** base and
IGNORE `executionBaseOrdinal` (`SharedServices.scala:327-331`, `363-367`), justified by a claimed base-independence
of the *root* (§4.7). If that claim is false for any field, a follower's re-exec root diverges from the
producer's. **`UNVERIFIED` — is the per-MG root truly base-independent under `AdoptFromSignedFields`?** (§4.7.)

### 4.4 Expiry / prune epoch differences — MOSTLY MOVED OUT OF THE FOLD
The derivation runs `noGlobalSnapshotLookup` (pure `None`) so no node-local head/epoch enters
(`ShardCheckpointWiring.scala:226-227`, `169-176`). Balances/refs/active-sets in the adopted result come
from `AdoptFromSignedFields` (the metagraph's signed fields), not from a gl0-side expiry fold
(`GlobalSnapshotAcceptanceManager.scala:1109-1116`). So allow-spend/token-lock expiry is the metagraph's
responsibility, committed in its stateProof, and adopted — not recomputed. **Residual:** `gl0AnchorOrdinal`
IS threaded into `processCurrencySnapshots` as the derivation ordinal for the fee-required cutover
(`ShardCheckpointWiring.scala:175-176`, `299`); if fee-cutover logic reads it, producer and re-executor must
pass the identical `gl0AnchorOrdinal` — they do (it is signed, `ShardCheckpoint.scala:139`). Adopter passes
`ordinal` (the gl0 ord being built) not `gl0AnchorOrdinal` to its shell `processCurrencySnapshots`
(`GlobalSnapshotAcceptanceManager.scala:1017-1018`) — but the shell's info is discarded, so this only affects
`lastIncremental`, which is the signed binary content (ordinal-independent). **`UNVERIFIED` — confirm the
decoded `lastIncremental` bytes are independent of the `ordinal` arg.**

### 4.5 Floating / `Ratio` — LOW RISK, ONE SITE
`Ratio(1, n)` (`committeeStake σ`) enters ONLY committee sortition membership
(`ShardCheckpointWiring.scala:565`, `committeeFor`), not the per-MG state or root. `Balance` is integral.
No floating point in the diff/root path. **Residual:** committee-membership divergence (if `σ`/`kDraw`/`eta`
differ across nodes) would change WHO is drawn, and thus quorum, but not the per-MG bytes — a liveness/quorum
risk, not a byte-identity risk.

### 4.6 Follower base eviction — FAIL-CLOSED (liveness, not safety)
On the logarithmic follower rail, `readAtOrdinal(executionBaseOrdinal)` hard-rejects a deep anchor ⇒ adopter
FAIL-CLOSED DROPs the MG (`GlobalSnapshotAcceptanceManager.scala:1097-1103`,
`PinnedCurrencyInfoReader.scala:49-55`). Safe (never adopts wrong bytes) but a liveness wedge: a follower
that can never serve `executionBaseOrdinal` never advances that MG. `UNVERIFIED` whether producers always stamp a
`executionBaseOrdinal` a follower can serve (the follower log ladder vs the produce cadence).

### 4.7 The "root is base-independent" claim — THE LOAD-BEARING UNVERIFIED ASSUMPTION
`SharedServices.scala:323-326` asserts the sub-quorum re-exec root is base-independent ("authoritative-override
fields + sync-view excluded from the per-MG root"), justifying the live reader. Analysis: under
`AdoptFromSignedFields`, `deriveAdoptedCurrencyInfo` populates `next.info` from the metagraph's SIGNED fields
(cumulative balances/refs/active-sets), not by accumulating over the prior — so the *root* is a function of
the signed binary, base-independent; the *diff* is base-dependent (next−prior). This is internally
consistent with the PIN-1 root gate (adopter reproduces the same signed-field-derived `next.info` regardless
of base) — **but it is unproven in this codebase** and is precisely where a subtle
`AdoptFromSignedFields` fallback (a field the mode CANNOT source from the signed binary and instead carries
from the prior) would silently make the root base-dependent, splitting live-reader re-executors from
pinned-reader adopters. **Fable's highest-value target.**

### 4.8 Byzantine-ml0-controlled inputs — bounded by GAP-1, except §4.2
A Byzantine ml0 controls the SC-binary content (`includedSnapshots`) and its own signatures. The metagraph
CANNOT forge balances/refs that don't hash to its own `stateProof` (GAP-1, `:1168-1169` unconditional). It
CAN, per §4.2, potentially seed Option fields the stateProof leaves `None`. It also controls the fold inputs
that feed `deriveAdoptedCurrencyInfo`; the shell's `lastIncremental` is metagraph-signed, so the fieldId-5
half of the root is metagraph-authenticated. `UNVERIFIED` whether a Byzantine ml0 can craft a binary whose
`AdoptFromSignedFields` decode differs between the adopter's shell (seeded from node-local
`priorLastCurrencySnapshots`, `:1020`) and the producer's derivation (seeded from the pinned prior,
`ShardCheckpointWiring.scala:288`) — both feed `processCurrencySnapshots` but with different priors; only
`lastIncremental` (not the info) is taken from the adopter's shell, so this should be immaterial, but the
asymmetry deserves a direct test.

### 4.9 Design-doc vs source disagreements
- `ADOPT-COMMITTED-CURRENCY-DELTA.md:41-45` proposes attaching the delta to `CurrencyIncrementalSnapshot`
  (universal) and keeping event-replay as fallback. **Source diverges:** the delta rides
  `ShardDerivedStateDelta.perMetagraphStateDiff` (sharded-only, committee-signed), and the event-replay
  fallback is DELETED — the override was removed 2026-07-01
  (`GlobalSnapshotAcceptanceManager.scala:1107-1116`, `ShardCheckpointWiring.scala:312-325`). The doc is the
  earlier design; the source is the shipped `ShardDerivedStateDelta.perMetagraphStateDelta` alternative named
  in the doc's own §9.1.
- `ADOPT-COMMITTED-CURRENCY-DELTA.md:60-62` frames trust as "recompute root, adopt only if `===` committed
  stateProof." Source ADDS the GAP-1 per-field metagraph-signature binding on top of the root gate
  (`GlobalSnapshotAcceptanceManager.scala:1126-1189`) — stronger than the doc.
- `ADOPT-COMMITTED-CURRENCY-DELTA.md:8` (2026-06-30 correction) states the token model is RE-EXECUTED and
  enforced (`reExecRoot === stateProof`). Source: on the QUORUM path gl0 does NOT re-execute
  (`ShardCheckpointGl0AcceptanceManager.scala:386-396`); enforcement is signatures + GAP-1 + (degraded)
  re-exec + watchtower. The doc overstates per-node re-exec on the happy path.

---

## 5. Attack checklist for Fable

Concrete things to try to make **adopt ≠ re-exec** (or adopt a value no honest re-exec would):

1. **Option-field injection past GAP-1 (§4.2, §3.3).** Construct a checkpoint whose `perMetagraphStateDiff`
   upserts entries into an Option field (`activeAllowSpends`/`activeTokenLocks`/`lastMessages`) for an MG
   whose tip `stateProof.<field> = None`. Verify whether adoption succeeds on PIN-1 alone
   (`:1123-1125`) with GAP-1 skipping the field (`:1170-1186`) — and whether a quorum of lazy/colluding
   committee signers (who did not re-execute, §3.3) can therefore inject state the metagraph never signed.

2. **Prove/refute the base-independence of the per-MG root (§4.7).** Find any field `AdoptFromSignedFields`
   cannot source from the signed binary and instead carries from the prior. If one exists, a follower's
   live-reader re-exec root (`SharedServices.scala:327-331`) diverges from a pinned-reader adopter's — a
   cluster split with no fail-close.

3. **Diff-base skew across the two reader rails (§1.3, §4.3).** Force `executionBaseOrdinal` to an ordinal that
   the gl0 contiguous store serves but the follower logarithmic store does not (or vice-versa), and check
   whether one rail adopts while another FAIL-CLOSED drops (`:1097-1103`) — persistent per-MG state split
   between gl0 and followers.

4. **Stale-prior double-count.** Make the producer stamp `executionBaseOrdinal = N` but craft the window so an
   adopter whose pinned `S(N)` legitimately differs (reorg/trim at exactly N below k₁) reconstructs a
   different `next`. Confirm the PIN-1 mismatch DROP (`:1205-1216`) fires and there is NO path where the
   diff is applied twice (adopt + later re-adopt) — probe `ShardReanchor` re-anchor (`:845-847`) + the
   right-biased merge (`:2318-2323`).

5. **`Some(empty)` root aliasing to smuggle a removal.** Exploit that an empty partition and a `None` field
   hash identically (§4.2). Craft a diff whose `removals` empty a partition the metagraph still signs as
   `Some(nonEmpty)` in its stateProof; check whether GAP-1's `sp.<field>.isDefined` compare
   (`:1174-1186`) actually catches the mismatch or whether the reconstructed `Some(empty)` slips through.

6. **fieldId-32 sync-view leverage — CONFIRMED as ECO-F32.** Field 32 is excluded from PIN-1
   (`:1339-1340`) and the global consensus root (`GlobalStateKey.scala:520-541`) but remains
   diffed/reconstructed/stored. The downstream consumer is framework checkpoint replay:
   `ShardCheckpointWiring.priorState` reads the prior `CurrencySnapshotInfo` and passes it to
   `processCurrencySnapshots` (`ShardCheckpointWiring.scala:263-314`). Reproduce the concrete RED
   case: one node replays a locally staged nonempty view; another verifies a peer map under the same
   signed root, strips field 32, reconstructs `Some(empty)`, and derives a different proof/signing
   outcome. Closure requires the exact optional replay witness and explicit ML0 population described
   in §2.2; missing data defers and cannot slash.

7. **Wire-shape / preimage manipulation.** Since `executionBaseOrdinal` is in `ShardCheckpointSigPreimageV2`
   (`ShardCheckpoint.scala:135-145`) and proto field 11 (`sidecar.proto:212`), try a checkpoint where the
   proto `diff_base_ordinal` and the JSON preimage `executionBaseOrdinal` disagree (bridge parse skew), or where
   V1 vs V2 preimage is used — check the signature verify actually binds field 11.

8. **Committee-attestation-without-re-exec (§3.3, UNVERIFIED).** Determine whether
   `ShardCheckpointAttestationEmitter` recomputes the per-MG root before signing. If it signs a best-tip
   checkpoint without re-derivation, a Byzantine leader needs only kQuorum lazy attestations to have gl0
   adopt any root that survives GAP-1 — collapse attacks 1 and 2 into a live exploit.

9. **`gl0AnchorOrdinal` fee-cutover skew (§4.4).** Since the adopter's shell passes `ordinal` (not
   `gl0AnchorOrdinal`) to `processCurrencySnapshots` (`:1017-1018`) while the producer passes
   `gl0AnchorOrdinal` (`ShardCheckpointWiring.scala:299`), find any code path where the decoded
   `lastIncremental` bytes (fieldId-5 half of the root) depend on that ordinal argument — that would split
   the root between produce and adopt.

10. **Sub-quorum re-exec `Hash.empty` sentinel (§1.1).** The failover maps non-derivable MGs to `Hash.empty`
    (`SharedServices.scala:346-350`, `ShardCheckpointGl0AcceptanceManager.scala:422`). Try to force a real
    committed per-MG root to collide with, or be masked by, the `Hash.empty` sentinel (e.g. an empty-window
    checkpoint) so a mismatch is silently treated as a match/skip.

---

## Appendix: canonical anchor index

| Concern | Anchor |
|---|---|
| Re-exec producer path | `ShardCheckpointWiring.scala:210-379` |
| Adopter path | `GlobalSnapshotAcceptanceManager.scala:1006-1219` |
| Adopt-verify entry (`adoptShardCheckpoints`) | `GlobalSnapshotAcceptanceManager.scala:742-971` |
| PIN-1 root gate | `GlobalSnapshotAcceptanceManager.scala:1123-1125` |
| GAP-1 field gate | `GlobalSnapshotAcceptanceManager.scala:1143-1189` |
| Diff apply (`reconstructInfoFromDiff`) | `ChangeSet.scala:151-172` |
| Diff compute (`currencyInfoChangeSet`) | `ChangeSet.scala:101-113` |
| PIN-1 root (`currencySnapshotMgRoot`) | `GlobalStateConverter.scala:1384-1389` |
| field-32 temporary root exclusion / ECO-F32 | `GlobalStateConverter.scala:1339-1340`; `GlobalStateKey.scala:520-541` |
| 2-level pin | `PinnedCurrencyInfoReader.scala:186-223` |
| Signed preimage V2 | `ShardCheckpoint.scala:135-145` |
| `executionBaseOrdinal` semantics | `ShardCheckpoint.scala:56-65` |
| proto field 11 | `p2p/proto/sidecar.proto:212` |
| quorum-path signature-only accept | `ShardCheckpointGl0AcceptanceManager.scala:386-396` |
| sub-quorum re-exec compare | `ShardCheckpointGl0AcceptanceManager.scala:545-587` |
| smtRoot-blind consensus compare (`376d09fbc`) | `GlobalSnapshotConsensusFunctions.scala:286-295` |
| producer executionBaseOrdinal stamp | `GlobalSnapshotConsensus.scala:1844-1846` |
| gl0 pinned reader / finalizedReaderAt | `GlobalSnapshotConsensus.scala:569-589` |
| follower live-reader re-exec | `SharedServices.scala:327-331`, `363-367` |
