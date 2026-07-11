# Per-Metagraph Currency State MPT Layout

**Status:** implemented. This document describes storage layout only. Committee-diff adoption was abandoned; GL0 recreates CL1 transitions.

## Layout

The old monolithic `LastCurrencySnapshotInfo` value is unrolled into per-entry metagraph namespaces so changing one account does not rewrite
an O(N) blob.

| `CurrencySnapshotInfo` field | MPT field id |
|---|---|
| `balances` | `MgBalances` (25) |
| `lastTxRefs` | `MgLastTxRefs` (26) |
| `lastFeeTxRefs` | `MgLastFeeTxRefs` (27) |
| `lastAllowSpendRefs` | `MgLastAllowSpendRefs` (28) |
| `lastTokenLockRefs` | `MgLastTokenLockRefs` (29) |
| `activeTokenLocks` | `MgActiveTokenLocks` (30) |
| `lastMessages` | `MgLastMessages` (31) |
| `globalSnapshotSyncView` | `MgGlobalSnapshotSyncView` (32) |

Metagraph-scoped `activeAllowSpends` remains in field id 7 because the cross-shard validator reads that partition directly. Incremental
snapshots remain in field id 5 and genesis snapshots remain in field id 3.

`infoSubFields` is exactly the eight `Mg*` partitions above. The currency `infoRoot` is one MPT root over their union, not a hash of eight
independent roots. Producer, replay validator, byte-rebuild, bootstrap, and incremental-write paths must use the same key and value codecs.

## Invariants

1. Encoding and reconstructing a `CurrencySnapshotInfo` preserves every value and presence convention.
2. Full rebuild and incremental writes produce byte-identical entries and the same root.
3. Removed accounts, references, messages, and locks delete their old MPT keys.
4. `GlobalSnapshotStateProof` keeps the same logical currency-root fields; the committed key set is the unrolled layout.
5. A shard checkpoint carries replay inputs and a root claim only. GL0 computes this root from locally recreated CL1 state and never applies
   a committee-supplied MPT diff.

## Enforcement Sites

- `GlobalStateKey.scala`: field ids and metagraph key constructors.
- `GlobalStateConverter.scala`: canonical per-entry encoding, reconstruction, and currency root calculation.
- `GlobalSnapshotInfo.scala`: state-proof reconstruction from persisted bytes.
- `AcceptanceMptStateChanges.scala` and `MptStore` write paths: incremental upserts and removals.
- `ShardCheckpointWiring.scala`: pinned-base replay and per-metagraph root calculation.
