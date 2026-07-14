# MPT-as-Primary: Scodec Integration Plan

**Status:** Planning → Active → **PARTIAL (see "Status as of 2026-06-17" below)** — 3a DONE, 3c PARTIAL, 3b/3d/3e NOT STARTED.
**Branch:** `feature/serde-typeclass-shim` (current — the planned `feature/mpt-as-primary` was **never cut**; all 3a/3c work landed piecemeal on `feature/serde-typeclass-shim`)
**Target:** 65% accept() CPU reduction (Phase 0 profiling result) + enable light-client proofs at historical ordinals.

## Executive summary

The scodec codec library (`feature/serde-typeclass-shim`) provides canonical byte-exact encoding for every consensus type (506 tests, 6/6 parity against real Brotli-JSON snapshots, byte-offset lenses for O(1) field access).

Crucially, **most of the MPT-as-primary infrastructure already exists in tessellation** and was built with this milestone in mind (see `MptUndoJournal.scala:46-51` comment — "deferred until inclusion-proof features land that require reconstructing the trie root at a historical ordinal"). The integration is about wiring existing pieces, not greenfield construction.

This plan consolidates (a) the design direction validated by industry research, (b) the concrete integration points against existing tessellation code, and (c) the phased implementation.

## Status as of 2026-06-17 (reconciliation)

> This section was added 2026-06-17. The body below (from Apr 22) is the **original plan** and is preserved verbatim; treat it as the design intent, and this section as ground truth. Substantial MPT-as-primary work landed **piecemeal on `feature/serde-typeclass-shim`** — the planned `feature/mpt-as-primary` branch was **never cut** (`git branch -a` shows only `feature/serde-typeclass-shim` + its `otto` remote). All citations verified against code/git on that branch.

### Per-phase status

| Phase | Status | Evidence |
|---|---|---|
| **3a** scodec values in MptStore | **DONE** | `MptStore` get/insert/sync/update are all parameterized on `ImmutableCodec[V]` (scodec-backed `ByteVector`), **not** circe — `MptStore.scala:44-75`, encode via `ImmutableCodec[V].immutableBytes` at `:145-146`. Scaladoc `:36-37`: "Values are encoded via the canonical `ImmutableCodec[V]` typeclass (scodec-backed, byte-exact) … this replaces the earlier circe-based encoding." Typeclass: `serde/ImmutableCodec.scala:26-31`. Parity suite present: `shared/.../serde/JsonScodecParitySuite.scala`. **Note:** the serve/sync seam (`syncFromGlobalSnapshotInfo`, `currencySnapshotEntryBytes`) still mixes circe (`.asJson`) for some partitions inside `GlobalStateConverter` — the *store value codec* is scodec; the *global-state encoder* is not uniformly scodec. 3a as scoped (value layer) is done. |
| **3b** historical proof wiring | **NOT STARTED (as scoped)** | No `def proofAt(ordinal, key)` on `MptStore` (signatures at `MptStore.scala:44-75` — no `proofAt`). `MptUndoJournal.unapplyTo` still not consumed for proof-at-ordinal. The provers exist (pre-existing) but the per-ordinal proof API the plan specifies is unwired. (Light-client SMT proof work — `LightClientSmt.scala` — is a separate track, not this `MptStore.proofAt`.) |
| **3c** remove GSI materialization on accept | **PARTIAL** | At the **2026-06-17 historical checkpoint**: signed `mptRoot` derived from MPT overlay bytes, G1–G5 readers routed reads through the MPT, the then-live root-filter experiments had landed, and per-MG unroll had landed. The root-filter description is superseded by the 2026-07-13 correction below. REMAINS: accept still materialized the `GlobalSnapshotInfo` case class; serve/follow/persist were still GSI-blob-authoritative. See the **3c split** below. |
| **3d** snap-sync export/import | **NOT STARTED** | No `exportFlat`/`importFlat` on `MptStore` (`MptStore.scala:44-75`). Followers bootstrap by pulling the GSI blob + re-encoding it locally (`StateChannel.ensureMptInitialized` → `syncFromGlobalSnapshotInfo`), not via a dedicated snap-sync dump protocol. |
| **3e** benchmark + pruning tuning | **NOT STARTED** | No reproduction of the Phase 0 accept() CPU measurement under the new path on this branch; the 65% target is unverified. |

### Phase 3c split — LANDED vs REMAINS

**LANDED on `feature/serde-typeclass-shim`:**

- **Signed root is MPT-authoritative.** The producer path derives the signed `GlobalSnapshotStateProof.mptRoot` from the producer's **own overlay byte map** (`p.entries`), not from `info.allStateEntriesAsBytes`: `GlobalSnapshotInfo.scala:255-257` — `p.buildForOrdinal(ordinal).flatMap { case Right(_) => p.entries.flatMap(bytes => mptStateProofFromBytes[F](info, bytes)) }`. `info` is passed only for present-only per-field Option lifting (scaladoc `:248-254`), never as a byte source. Landed in `353dcabfb`.
- **G1–G5 MPT-primary readers.** Each adds a reader over the MPT store (typically `GlobalStateReader.fromMptStore(mptStore)`) and rewires a consensus read off the in-memory `info`:
  - **G1** `78f73bfd1`/`101d18467` — `NodeStakeAggregator` + `StakeRegistry` (`stakeWeightedMpt`), wired in `GlobalSnapshotConsensus.scala`.
  - **G2** `5dfe9bf3e`/`60d5f7b94` — boundary write reads `StakeDistribution` from the MPT (`NodeStakeAggregator` + `GlobalSnapshotAcceptanceManager`).
  - **G3** `a2edf677b`/`9a524bdb3` — `HistoricalStakeReader` (`historicalStakeSnapshots` from MPT), built against `GlobalStateReader.fromMptStore` (`GlobalSnapshotConsensus.scala:368,379`).
  - **G4** `0e2931a2d`/`fc79eec28` — `BlockAcceptanceContext.fromMpt` / `AllowSpendBlockAcceptanceContext.fromMpt` / `TokenLockBlockAcceptanceContext.fromMpt`, wired in `BlockAcceptanceCoordinatorManager.scala:75,93`.
  - **G5** `1eb828522`/`c34bc5ee0` — reward + node-parameters readers: `DelegatedStakeStateManager`, `NodeCollateralStateManager`, `UpdateNodeParametersStateReader`; `GlobalDelegatedRewardsDistributor` / `RewardsInfoCalculator` migrated.
  - (Each SHA pair is the same commit reachable under two refs — `git show --stat` is identical for both.)
- **Historical root-filter sequence, superseded by the 2026-07-13 complete-root correction.**
  - **Historical (2026-06):** `85be66a6a`/`353dcabfb` excluded the `03…`
    `SystemNamespace` active-address and expiry entries from the global `mptRoot` as
    “sidecars.” That classification was unsafe: those entries are authoritative inputs to
    economic materialization and expiry/refund transitions, so two maps could authenticate the
    same root and execute different state. The live root contract now retains every
    `SystemNamespace` entry; the old root-exclusion rule is not a supported compatibility mode.
  - **Historical (2026-06):** `f46bc7666` + `df19cef76` excluded observation-dependent
    `globalSnapshotSyncView` (`MgGlobalSnapshotSyncView`, field 32) from the per-MG `infoRoot` and
    global root, then `9b416f736` reverted both while isolating a fork storm. This records the
    commit sequence; it is not current behavior.
  - **Current worktree (2026-07-13):** `GlobalStateKey.consensusRootEntries` retains all
    `SystemNamespace` economic indexes and excludes only field 32;
    `GlobalSnapshotInfo.consensusMptRoot` is the shared global verification helper. Field-32
    exclusion is temporary containment, not a safety proof. GL0 checkpoint replay consumes the
    prior `CurrencySnapshotInfo.globalSnapshotSyncView`, but the signed incremental binds only its
    proof hash and accepted delta, not the exact full-view replay preimage or explicit ML0 operator
    population. `ECO-F32` is therefore HIGH, CONFIRMED, and OPEN. The fix order is: carry and verify
    the exact optional signed/root-bound replay witness (preserving `None` vs `Some(empty)`), then
    remove field 32 from every GL0 MPT/diff/load/reorg path while retaining it in ML0 state.
- **Per-MG unroll of `CurrencySnapshotInfo`.** The fieldId-6 blob is replaced by 8 per-entry `Mg*` partitions (fieldIds 25–32); see `docs/nakamoto/UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md` (now marked LANDED). Decision (b): `activeAllowSpends` deliberately stays in fieldId-7, **not** an `MgActiveAllowSpends`, so it is **not** in the per-MG `infoRoot` union (`GlobalStateConverter.scala:1188-1190`).

**REMAINS (the half-finished part — 3c END-STATE never reached):**

- **`accept()` still materializes the `GlobalSnapshotInfo` case class.** `GlobalSnapshotAcceptanceManager.buildGlobalSnapshotInfo` constructs `val baseInfo = GlobalSnapshotInfo(...)` (`GlobalSnapshotAcceptanceManager.scala:1220,1250`), called at `:2271`. The plan's "accept emits a delta, MPT is the state" was not reached — the GSI is still built and returned every ordinal.
- **Serve path still ships the GSI blob.** `GlobalL0Service.LatestSnapshotTuple = (Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)` (`GlobalL0Service.scala:41`); `pullLatestSnapshotFromPeer` → `l0GlobalSnapshotClient.getLatest` returns `(snapshot, state)` where `state` is the GSI (`:380-387`). The **gl0 serving side** responds with snapshot+state over `GlobalFollowRoutes.scala` / `StateChannelRoutes.scala` (`snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]`). MPT bytes are **not** served; the follower receives the GSI and rebuilds.
- **Historical 2026-06-17 follower behavior:** ml0 `StateChannel.ensureMptInitialized` re-encoded
  the GSI through `syncFromGlobalSnapshotInfo`, then compared
  `consensusMptRoot(entries) === signed mptRoot`; cl1 used its per-MG
  `getRootHashForOrdinal === signedRoot`. This paragraph records the state when the plan was
  reconciled, not the later signed-byte/bootstrap work. For global maps, the live helper includes
  every `SystemNamespace` economic index and filters only field 32.
- **Persistence is GSI-authoritative.** `StateChannel.persistGlobalSnapshot` writes `GlobalSnapshotWithState(snapshot.signed, state)` (`StateChannel.scala:175-178`) — the GSI blob is the persisted source of truth, not MPT bytes.
- **GlobalStateReader accessors incomplete + remaining read-sites unmigrated.** G1–G5 migrated specific consensus reads; the broad scatter-gather of `.info.<field>` read-sites is not fully retired. No `GlobalSnapshotInfo.from(mptStore, ordinal)` **derivation-only** helper exists (the `GlobalSnapshotInfo` companion at `GlobalSnapshotInfo.scala:213` has no `from(mptStore)`/`fromMpt`) — so the plan's "GSI becomes a derived view, hot paths skip it" end-state was **not** reached. `GlobalStateReader.fromMptStore` is a *parallel* reader used by G1–G5, **not** a flip that makes the GSI derived.

**Historical bug at the 2026-06-17 checkpoint (verified then):** the ml0
resync-to-canonical gate could derive
`consensusMptRoot(syncFromGlobalSnapshotInfo(servedInfo)) != signed mptRoot` because the served GSI
and signed overlay MPT were different encoders. Field 7 and the then-changing field-32 policy were
prime suspects. The durable lesson remains: recovery must verify and load the exact signed byte map,
not reconstruct a second candidate and treat equal-looking typed state as byte equality. Current
field-32 exclusion does not close `ECO-F32`: replay still needs an exact signed/root-bound full-view
witness before GL0 can remove that mirror safely.

### Greenfield note

Per `[[feedback-greenfield-no-wire-compat]]`, the REMAINS work needs **no network wire-compat** (the cluster upgrades atomically) — only on-disk state needs read-compat, and the MPT flat-state dumps are already the on-disk format. So "serve MPT bytes / rebuild-from-bytes / GSI-derived persistence" can be done as a clean cut, not a dual-codec migration.

---

## Design decision — validated by industry research

A background research agent surveyed how Ethereum (Geth PBSS, Erigon), Cosmos (IAVL), Polkadot (Substrate), and Verkle tries handle MPT state storage. Key findings:

**Geth PBSS migration (v1.13.0, Sept 2023)** solved the hash-indexed state-bloat problem at ~1TB scale. Path-indexed storage enabled natural in-place pruning, 30-50% disk reduction, at the cost of deep historical proofs. PBSS is now default for new Geth nodes.

**Erigon's flat-KV approach** — no MPT persisted at all, root computed on-demand — achieves 2TB archive vs Geth's 15TB, but **proof latency is spiky**, which conflicts with "proofs every couple of minutes to light clients".

**Cosmos IAVL post-mortems** (Band Protocol July 2020 OOM, `cosmos/iavl#256`) document versioned-tree pruning as a known footgun — `SaveVersion` perf degrades under pruning options.

**Substrate child tries**: Parity considered removing the feature as over-engineered. Single global trie with namespaced keys achieves the same proof granularity.

**Verkle tries**: not production-ready as of early 2026. Track but don't build on.

**Recommended architecture (from research)**: Path-indexed MPT with bounded-depth overlay (PBSS-style) + flat snap-sync side-table.

**Tessellation's actual starting position**: Erigon-style flat-state persistence with in-memory trie cache. `MptStateStorage.scala:18` is explicit: "Storage for MPT state only. Trie is rebuilt on load to save memory and disk space." The `mpt_snapshot_info/<ordinal>` files on disk are per-ordinal flat-KV dumps; MPT nodes are never persisted, they're rebuilt from flat state and cached (`MaxCacheSize = 50`). This means the research's "hash-indexed node storage bloats at 1TB" warning does NOT apply — there's no node storage to bloat. The hash-indexing in Tessellation is only at the top-level key (`Hex(GlobalStateKey)`), which is just key encoding, not node layout.

**Scaling concerns at 100GB are therefore different**: (1) flat-state file serialization at GB-scale (current `MptStateStorage.writeState` rewrites the whole map — move to delta-only persistence), (2) trie rebuild on cache miss (consider disk-backed node cache for longer proof windows), (3) proof cache for repeated queries. None block the MPT-as-primary milestone; all are straightforward tuning when state approaches 100GB.

## Tessellation's pre-existing infrastructure

Already built:

| Component | File | Capability |
|---|---|---|
| `MptStore[F, K]` | `schema/mpt/MptStore.scala` | Typed store on `GlobalStateKey`, get/insert/remove/update, batched `sync(updates, ordinal)`, `build(ordinal)`, savepoint/restore, `deleteAbove(ordinal)`, `withExclusiveLock`. |
| `FileSystemMerklePatriciaProducer` | `security/mpt/producer/` | Flat KV in `stateRef: Map[Hex, Array[Byte]]`, cached trie, pending inserts/removes tracked, disk persistence via `MptStateStorage`, per-ordinal root hash cache (`MaxCacheSize = 50`). |
| `MptUndoJournal[F]` | `node-shared/.../nakamoto/` | Per-ordinal undo records (inserted/removed/overwritten keys + old bytes), `unapplyTo(ancestorOrdinal)` implemented but not yet consumed, `pruneBelow(ordinal)` for finality pruning. |
| Proof provers | `security/mpt/prover/` | `MerklePatriciaSingleInclusionProver`, `MerklePatriciaBatchInclusionProver`, `MerklePatriciaRangeProver`, `MerklePatriciaPrefixProver` — all tested. |
| Scodec codec library | `serde/codecs/instances/` | Canonical byte-exact codecs for every consensus type. |
| Byte-offset field lenses | `serde/codecs/offset/` | O(1) field access for MPT-blob-store reads (Phase 2 of this plan). |

**Missing for MPT-as-primary**:

1. Flat-KV value format uses JSON via circe `Encoder`/`Decoder`. Needs to route through scodec.
2. `MptUndoJournal.unapplyTo` is never invoked — needs wiring into proof-at-historical-ordinal.
3. `GlobalSnapshotInfo` is still materialized in the accept path; needs to become a derived view.
4. No snap-sync export/import protocol for light-client bootstrap.
5. No systematic benchmark against Phase 0 baseline.

## Phase 3 integration (revised scope)

### 3a. Scodec values in the MptStore (1 week)

Swap JSON encoding for scodec at the value layer.

- Current: `MptStore.insert[V: Encoder]` / `get[V: Decoder]` uses circe via `JsonSerializer[F]`.
- Target: parameterize on `ImmutableCodec[V]` (our scodec typeclass). Or add a parallel `ScodecMptStore[F, K]` that coexists.
- Caveat: `MptUndoJournal` currently stores `old bytes` opaquely — those bytes become scodec-formatted; rollback continues to work because it just reinstates the old byte blob, format-agnostic.
- Parity against real data covered by `JsonScodecParitySuite` at the value-type level.

### 3b. Historical proof wiring (1 week)

New API:

```scala
trait MptStore[F[_], K] {
  ...
  def proofAt(ordinal: SnapshotOrdinal, key: K): F[Either[ProofError, Proof]]
}
```

Implementation options:

1. **Direct-read** (preferred if nodes for that root are still on disk): walk from the cached root hash at `ordinal`, no state mutation. Caches already exist; `build(ordinal)` reconstructs from persisted state.
2. **Savepoint + unapply + prove + restore**: use the existing `MptUndoJournal.unapplyTo` to temporarily roll the trie back, generate proof, restore. Slower but always works within the journal window.

Pick option 1 as primary, fall back to option 2 if the root isn't cached.

### 3c. Remove `GlobalSnapshotInfo` materialization on accept path (2-3 weeks)

> **HISTORICAL STATUS 2026-06-17: PARTIAL.** The *signed-root-from-MPT*, G1–G5 readers, then-live root-filter experiments, and per-MG unroll had landed on `feature/serde-typeclass-shim`. The root-filter claims are superseded by the 2026-07-13 complete-root correction near the top. At that checkpoint, accept still materialized the GSI case class; serve/follow/persist were still GSI-blob-authoritative; followers re-encoded the served GSI rather than rebuilding from served MPT bytes; and no `GlobalSnapshotInfo.from(mptStore, ordinal)` derivation-only flip existed. The bullets below are the **original plan**; they are NOT all done.

The "MPT is the state" change.

- `accept()` currently constructs a `GlobalSnapshotInfo` case class and computes a state proof over it. Target: `accept()` emits a delta `(Map[GlobalStateKey, Option[Value]])`, `MptStore.update(toUpsert, toRemove)`, `commit(ordinal)` → root hash.
- API/read callers that today read `.info.balances`, `.info.lastTxRefs`, etc. need to route through `MptStore.get`. This is the scatter-gather refactor.
- Preserve a `GlobalSnapshotInfo.from(mptStore, ordinal)` derivation helper for backward-compat read APIs that expect the old shape — but hot paths don't go through it.
- `GlobalSnapshotStateProof.mptRoot` becomes authoritative; legacy proof hashes populated transitively only where still needed.

This is the largest and riskiest piece. Propose splitting into sub-tasks per consumer subsystem (balance queries, last-tx-refs, delegated-stake views, etc.).

### 3d. Snap-sync export/import (1 week)

Light-client bootstrap.

- `MptStore.exportFlat(ordinal): F[SnapSyncDump]` — `stateRef` contents + root hash + signed commitment.
- `MptStore.importFlat(dump): F[Unit]` — bulk-load, rebuild trie, verify root.
- Wire a simple HTTP endpoint for peer fetch. Uses existing HTTP4s infrastructure.
- Wire format: scodec (natural — every value already has a codec). Or piggyback on the existing `mpt_snapshot_info/<ordinal>` files which are already the right shape (the parity discovery).

### 3e. Benchmark + pruning tuning (1 week)

- Reproduce Phase 0 accept() CPU measurement under the new path.
- Target: 65% CPU reduction.
- Tune `MaxCacheSize`, prune cadence, persist cadence. Finality window drives pruning depth.

## Total estimate: 5-6 weeks

Matches original estimate, but with concrete integration points rather than from-scratch construction.

## Deferred (revisit at large-state-scale)

- **Delta-only flat-state persistence**: current `MptStateStorage` rewrites the whole `Map[Hex, Array[Byte]]` per ordinal. Above ~1GB state this becomes I/O-bound. Replace with overlay-file-per-ordinal + periodic compaction. Straightforward refactor to the storage layer, not load-bearing for Phase 3.
- **Disk-backed trie node cache**: keep rebuilt MPT nodes on disk (RocksDB/MDBX) for longer proof-serving windows. In-memory `MaxCacheSize = 50` is the current ceiling; disk-backing extends it without memory pressure.
- **Verkle migration**. Track Ethereum's work; not production-ready as of early 2026.

Note: the "path-indexed migration" concern from Geth's PBSS transition does NOT apply to Tessellation because Tessellation does not persist MPT nodes. That problem class is sidestepped by the Erigon-style flat-only persistence.

## Non-goals (intentionally deferred beyond Phase 3)

- **Era registry wiring into the live read path** — Phase 4 follow-up.
- **Scodec snapshot envelope** — greenfield, no hardfork gating needed, but still a separate milestone after MPT-as-primary lands.

## Sequencing

```
3a (scodec values)  ──▶  3b (historical proofs)  ──▶  3c (remove GSI materialization)
                            │                            │
                            └──▶  3d (snap-sync)         └──▶  3e (benchmark + tune)
```

3a and 3b are prerequisites; 3c is the long pole; 3d and 3e can parallelize with 3c sub-tasks.

## Next concrete step

Start 3a: parameterize `MptStore` on a scodec value typeclass, migrate one value type (e.g. `Balance`) as a proof of concept, rerun the accept-path tests to confirm equivalence against the JSON-backed path.
