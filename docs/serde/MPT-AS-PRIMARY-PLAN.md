# MPT-as-Primary: Scodec Integration Plan

**Status:** Planning → Active
**Branch:** `feature/serde-typeclass-shim` (current), then a new `feature/mpt-as-primary`
**Target:** 65% accept() CPU reduction (Phase 0 profiling result) by making the Merkle Patricia Trie the primary state store instead of a derived/recomputed proof.

## What we have

After 30+ commits on `feature/serde-typeclass-shim`, every consensus type on the write path has a canonical, hand-written, byte-exact scodec codec with round-trip tests:

- **Primitives:** Hash/ProofsHash, Address, Id, Hex, NonNegLong/PosLong/NonNegInt/PosInt shapes, Option/Either/List/Map/SortedMap/SortedSet/NonEmpty{List,Set}, String (UTF-8), UUID, ByteArray.
- **References (40-byte fixed):** TransactionReference, BlockReference, AllowSpendReference, TokenLockReference, DelegatedStakeReference, NodeCollateralReference, UpdateNodeParametersReference.
- **Sum-type ADTs:** PartitionNamespace (5), TokenId (2), SharedArtifact (6), UpdateDelegatedStake (2), UpdateNodeCollateral (2), MessageType (2), BalanceAdjustmentReason (3).
- **Records:** Transaction, Block, RewardTransaction, AllowSpend, TokenLock, UpdateNodeParameters family, Proof/MerkleTree, GlobalStateKey, DelegatedStake/NodeCollateral records, TokenPair/PricingUpdate/PriceRecord, CurrencyMessage, FeeTransaction, SpendTransaction, MetagraphSyncDataInfo, GlobalSnapshotSync/View, ActiveTip/DeprecatedTip/SnapshotTips, BlockAsActiveTip, AllowSpendBlock/TokenLockBlock, DataApplicationPart(V1), StateChannelSnapshotBinary, SlotCertificate + VRF trio.
- **Capstones:** GlobalSnapshotStateProof(V1), GlobalSnapshotInfo(V1), CurrencySnapshot/CurrencyIncrementalSnapshot(V1) + Info(V1) + StateProof(V1), GlobalSnapshot, GlobalIncrementalSnapshot(V1).

**503 shared tests** validate round-trip correctness. What they do NOT yet validate: that the scodec-encoded bytes, when decoded, match what real historical JSON-encoded data represents. That's parity.

## What we don't have (this plan's deliverables)

1. **Parity confidence.** We don't know if `scodec.decode(legacy JSON → circe decode → scodec encode)` equals the original Scala value across all the edge cases real data exercises. Only a round-trip through our own codecs — not through the historical encoding.

2. **Byte-offset field access.** Our codecs support it *structurally* (fixed-width layout where possible) but no API exposes field-at-offset reads today. All field access requires full decode.

3. **MPT-as-primary storage.** State today is recomputed from JSON on every accept(). Target: state lives in a content-addressed MPT keyed by `GlobalStateKey`, writes are incremental.

## Non-goals (intentionally deferred)

- **Era registry wiring into the live read path.** Phase 3 follow-up. The registry type exists; the codecs exist; flipping the read path is a separate branch when the shape of the new path stabilizes.
- **Scodec snapshot envelope.** Phase 4, post-MPT. Snapshots stay JSON-Brotli on disk; the *state* moves to MPT.
- **Hardfork ordinals.** User directive: greenfield, no governance. No ordinal gating for this work — the new code is the behavior.

## Test-data inventory (what we have to validate against)

```
nodes/{0..7}/gl0-data/
  snapshot/hash/**/*              — genesis full snapshots (Brotli JSON)
  incremental_snapshot/
    hash/{xxx}/{yyy}/{fullhash}    — ~760 snapshots per node (Brotli JSON)
    ordinal/0/{ordinal}            — ordinal → hash symlinks
  mpt_snapshot_info/{ordinal}      — MPT state-info persists every 100 ordinals
```

Real live data from an 8-node simulation. `GlobalSnapshot` (genesis), `GlobalIncrementalSnapshot` (hot path), and the MPT snapshot_info files all present.

## Phase 1 — Parity against real data (1 week, zero prod risk)

### Goal

Prove the scodec library is a faithful encoding of the same Scala types that circe-JSON produces from historical on-disk bytes.

### Work

1. **`JsonParityFixture`** test helper that:
   - Reads a file from `nodes/{n}/gl0-data/{path}`, decompresses Brotli, parses UTF-8 JSON.
   - Decodes via the existing circe `Decoder[GlobalIncrementalSnapshot]` (or whatever type the file represents).
   - Encodes that Scala value via our scodec codec.
   - Decodes the scodec bytes back.
   - Asserts the round-trip value equals the circe-decoded one.
2. **Batch fixture suite** that runs the above across a sampling of the 8-node simulation data — one genesis, a handful of incremental snapshots at different ordinals, the MPT info files.
3. **Fixtures committed** as test resources so CI can run without depending on the `nodes/` working tree. Pick ~5-10 representative files, not all 760; keep git size sane.
4. **Failures block merge.** Any type where scodec round-trip disagrees with circe round-trip is a codec bug we need to fix before Phase 2.

### Deliverable

`shared/test/JsonScodecParitySuite` — passes against real JSON bytes, catches any codec field-order or encoding mistake before the MPT work starts depending on correctness.

## Phase 2 — Byte-offset helpers (1-2 weeks, new API design)

### Goal

Expose field-at-offset reads for fixed-layout consensus types so persisted records can be peeked without full decode. The MPT write path and snapshot verification both benefit: e.g. "tell me the `stateRoot` of a snapshot on disk" should be a 33-byte fseek, not a 300KB brotli+JSON+circe+codec chain.

### Design sketch

```scala
/** A read-only lens into a byte-slice representation of T. */
trait FieldLens[T, F] {
  def offset: Long
  def length: Option[Long]    // None for variable-width; requires partial-decode to locate
  def read(bytes: ByteVector): Attempt[F]
}

object FieldLens {
  // Automatic for types composed entirely of fixed-width fields:
  def at[T, F](offset: Long, length: Long)(codec: Codec[F]): FieldLens[T, F] =
    new FieldLens[T, F] {
      val offset = offset
      val length = Some(length)
      def read(bytes: ByteVector) =
        codec.decode(bytes.drop(offset).take(length).bits).map(_.value)
    }
}
```

Per-type, hand-authored lenses for the high-value fixed-layout types:

```scala
object BlockReferenceLens {
  val height: FieldLens[BlockReference, Height]        = FieldLens.at(0, 8)(Codec[Height])
  val hash: FieldLens[BlockReference, ProofsHash]       = FieldLens.at(8, 32)(HashCodec.proofsCodec)
}

object GlobalSnapshotStateProofLens {
  // When mptRoot is present, the final 32 bytes of the last 33 are the root hash.
  def readMptRootIfPresent(bytes: ByteVector): Option[Hash] = ...
}
```

### Work

1. `FieldLens[T, F]` trait + helpers in `modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/offset/`.
2. Lens modules for: `BlockReference`, `TransactionReference`, `AllowSpendReference`, `TokenLockReference`, `MerkleRoot` — all 40-or-36 byte fully fixed.
3. `GlobalSnapshotStateProofLens` — partial-fixed: the three required hashes are at fixed offsets 0/32/64, the optional tail requires scanning 1-byte Option discriminators.
4. Tests that compare lens reads against full-decode reads for a set of samples — lens and full decode must agree.

### Deliverable

A family of `FieldLens[T, F]` types that give O(1) byte-slice access to the high-value fields. Used by the MPT layer in Phase 3 for "read just the state root from a snapshot on disk".

## Phase 3 — MPT-as-primary write path (4-6 weeks, the milestone)

### Goal

Make the Merkle Patricia Trie the *authoritative* state store instead of a recomputed proof derived from JSON. The 65% CPU that Phase 0 measured is in `stateProof()` computation — MPT-as-primary eliminates it by maintaining the MPT incrementally across accepts.

### Architecture

```
                           ┌─────────────────────────┐
                           │   GlobalStateKey        │
                           │  (typed ADT, scodec'd)  │
                           └───────────┬─────────────┘
                                       │ hash to 32 bytes
                                       ▼
                           ┌─────────────────────────┐
  accept(block) ──────────▶│   MptStore[F]            │◀──── snapshot-info
                           │  key: Hash               │       rollback reads
                           │  value: ByteVector       │
                           │  (scodec-encoded)        │
                           └───────────┬─────────────┘
                                       │
                                       ▼
                           ┌─────────────────────────┐
                           │  disk backend           │
                           │  (start: flat-files      │
                           │   indexed by key-hash)   │
                           └─────────────────────────┘
```

### Write path (what accept() does today → after)

**Today:**
1. Compute new state by folding the block over old state.
2. Serialize whole state to JSON, hash it into 16 partial proofs.
3. Brotli-compress and write the snapshot.
4. On next accept, re-read the whole state from JSON to fold the next block.

**Target:**
1. Compute new state as a *delta* — a list of `(GlobalStateKey, Option[Value])` operations.
2. Apply the delta to the MPT (insert/update/delete) — each op is O(log N) via MPT path update.
3. Get the MPT root hash. `GlobalSnapshotStateProof.mptRoot` = that hash.
4. Snapshot envelope still written as JSON-Brotli (no change to disk wire format yet), but its `stateProof` field now references the MPT root only.
5. On next accept, the MPT is already loaded — we never leave it.

### Work breakdown

1. **`MptStore[F]` trait** — `get(key: Hash): F[Option[ByteVector]]`, `put(key: Hash, value: ByteVector): F[Unit]`, `delete(key: Hash): F[Unit]`, `commit(): F[Hash]` (root), `rollback(): F[Unit]`.
2. **File-backed backend** — start with a flat-files-indexed-by-hash layout (mirrors the existing `incremental_snapshot/hash/{xxx}/{yyy}/{hash}` convention). LMDB/RocksDB upgrade later if needed.
3. **`GlobalStateMptOps`** — high-level wrapper that takes a `GlobalStateKey` (typed ADT), hashes it to an MPT key, and handles the codec for the value at that key. Uses `GlobalStateKey.toHex` for the current hex-derived key, but could flip to scodec-bytes-then-hash if preferred for canonicity.
4. **Accept-path integration** — refactor `GlobalSnapshotAcceptanceManager` / `accept()` to emit the `(key, value)` delta instead of computing proofs from scratch, then apply it to the `MptStore`.
5. **Rollback-aware** — the existing rollback machinery re-reads historical snapshot_info. After MPT-as-primary, rollback must reconstruct the MPT up to the target ordinal. Two options: (a) snapshot_info files *are* the MPT root + dirty pages at that ordinal, (b) replay deltas from a checkpoint. Pick (a) — it's simpler and matches the existing snapshot_info cadence (every 100 ordinals).
6. **Snapshot-info files become MPT-rooted** — the existing `mpt_snapshot_info/{ordinal}` files already exist in the sim data. This plan makes them authoritative rather than derived.
7. **Byte-offset field access** — MPT verification at scan time uses `FieldLens[GlobalSnapshotStateProof, Hash]` to read just the MPT root from a snapshot file on disk, without full decode.

### Deliverable

`feature/mpt-as-primary` branch that passes:
- Full existing test suite
- Parity suite (Phase 1)
- New MPT-store round-trip tests (put/get/commit/rollback)
- Accept-path performance test showing the expected CPU drop against a recorded benchmark

### Risks & mitigations

- **MPT state corruption during crash.** Mitigate: atomic commit via write-ahead log at the backend layer; MPT root only advances after the WAL is fsync'd.
- **Rollback correctness.** Mitigate: snapshot_info files at every-100-ordinal cadence are checkpoints; between checkpoints, we replay the scodec-encoded deltas captured in each snapshot.
- **Migration from existing JSON state.** Mitigate: on startup, if MPT is empty but snapshot_info exists, populate MPT from the JSON-derived state (one-time bootstrap; production never re-does this).

## Phase 4 — Scodec snapshot envelope (post-MPT, hardfork-optional for greenfield)

Not gated by this plan. When greenfield becomes non-greenfield, revisit.

## Sequencing

```
Phase 1 (parity)  ──▶  Phase 2 (byte-offset lenses)  ──▶  Phase 3 (MPT-as-primary)
  ~1 week                  ~1-2 weeks                        ~4-6 weeks
  (low risk)               (new API, reviewed)              (the milestone)
```

Phase 1 must land before Phase 3 depends on codec correctness. Phase 2 can land in parallel with Phase 3 planning since MPT uses lenses but doesn't block on them for the initial write path (full-decode works; lenses are the optimization).

## Next concrete step

Start Phase 1: `JsonScodecParitySuite` reading real data from `nodes/0/gl0-data/`, committed fixtures in `modules/shared/src/test/resources/serde/real/`.
