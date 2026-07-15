# Per-Metagraph Currency State MPT Layout

**Status:** implemented storage layout only. The current ordinary-adopter replay
path is a temporary regression, not the target authority model. Target sharded CL1
processing follows ADR-0017: producer and every execution signer replay, assigned
watchtowers replay, and an ordinary noncommittee GL0 validator applies the
replay-certified scoped diff and recomputes its root. This storage design does not
alter universal native GL1 execution. The universal global settlement kernel is
target E9 work and is not complete today.

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

The serialized/storeable layout has all eight `Mg*` field ids above, but
`infoSubFields` contains only the seven deterministic fields 25 through 31. The
currency `infoRoot` is one MPT root over the union of those seven fields, not a
hash of independent roots. Field 32 is currently excluded from both `infoRoot`
and the aggregate consensus root even though it is writable; that is the open
root-invisible-state defect, not approved mutable metadata. Producer, replay
validator, byte-rebuild, bootstrap, and incremental-write paths must use the same
key and value codecs.

## Invariants

1. Encoding and reconstructing a `CurrencySnapshotInfo` preserves every value and presence convention.
2. Full rebuild and incremental writes produce byte-identical entries and the same root.
3. Removed accounts, references, messages, and locks delete their old MPT keys.
4. `GlobalSnapshotStateProof` keeps the same logical currency-root fields; the committed key set is the unrolled layout.
5. The target shard checkpoint carries exact replay inputs, a canonical scoped
   diff, signed pre-root/version, and its result root. Producer and every execution
   signer independently reproduce the diff/root; positive assigned watchtower
   replay coverage is mandatory before GL0 inclusion. An ordinary noncommittee
   GL0 validator verifies the execution certificate, exact Phase-2 base,
   pre-root/version compare-and-set, scope, continuity, domains, and coverage,
   then applies the diff and recomputes the root. A claimed root is never installed
   directly. Current every-adopter CL1 recreation remains only until that complete
   adoption gate lands.

## Enforcement Sites

- `GlobalStateKey.scala`: field ids and metagraph key constructors.
- `GlobalStateConverter.scala`: canonical per-entry encoding, reconstruction, and currency root calculation.
- `GlobalSnapshotInfo.scala`: state-proof reconstruction from persisted bytes.
- `AcceptanceMptStateChanges.scala` and `MptStore` write paths: incremental upserts and removals.
- `ShardCheckpointWiring.scala`: pinned-base replay and per-metagraph root calculation.
