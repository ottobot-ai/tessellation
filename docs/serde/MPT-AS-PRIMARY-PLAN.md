# MPT-as-Primary: Scodec Integration Plan

**Status:** Planning → Active
**Branch:** `feature/serde-typeclass-shim` (current), then `feature/mpt-as-primary`
**Target:** 65% accept() CPU reduction (Phase 0 profiling result) + enable light-client proofs at historical ordinals.

## Executive summary

The scodec codec library (`feature/serde-typeclass-shim`) provides canonical byte-exact encoding for every consensus type (506 tests, 6/6 parity against real Brotli-JSON snapshots, byte-offset lenses for O(1) field access).

Crucially, **most of the MPT-as-primary infrastructure already exists in tessellation** and was built with this milestone in mind (see `MptUndoJournal.scala:46-51` comment — "deferred until inclusion-proof features land that require reconstructing the trie root at a historical ordinal"). The integration is about wiring existing pieces, not greenfield construction.

This plan consolidates (a) the design direction validated by industry research, (b) the concrete integration points against existing tessellation code, and (c) the phased implementation.

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
