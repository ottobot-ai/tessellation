# Data-application block validity: all-or-nothing + TOCTOU (known limitation)

Surfaced while debugging a metagraph (ottochain) e2e; the root causes are framework-side and
affect every data-application metagraph. Recorded here for whoever picks up the proper fixes.

## The failure shape

A valid data update is silently dropped (never combined), with no rejection visible to the
client beyond an opaque confirmation timeout. It happens when the update is batched into a DL1
block alongside a *now-stale* update.

## Why

1. **All-or-nothing block acceptance.** `DataApplicationSnapshotAcceptanceManager` (node-shared,
   `…/snapshot/managers/currency/`) validates each block's transactions at ML0 via
   `validateDataTransactionsL0` → `service.validateData`. If **any** tx in the block is `Invalid`,
   the **entire block** is moved to `notAcceptedBlocks` — every valid tx in it is dropped too.

2. **TOCTOU between block formation and acceptance.** The DL1 data consensus (`currency-l1
   …/dataApplication/consensus/Engine.formBlock`) validates txs with **L1** against the cached
   `OnChain` *at formation time*. By the time ML0 re-validates the block (state has advanced via
   intervening snapshots), a tx that was valid can be stale (`*AlreadyExists`,
   `SequenceNumberMismatch`, non-monotonic) → it fails `validateData` → poisons the whole block.

3. **Stale copies linger because the mempool isn't cleared across nodes.** Data updates are NOT
   gossiped between mempools (`/data` POST is node-local; only consensus *proposals* cross nodes).
   A client submitting to all N DL1 nodes puts a copy in each local `dataTransactionsQueue`. With
   `data-consensus.peers-count < N-1`, a round includes only a subset of nodes, so the copies on
   non-participating nodes are never dequeued; once the update is applied via another node's block,
   those copies are stale and re-propose into later blocks (the poison source). Amplified under
   concurrent load.

## Mitigations available today (metagraph-side)

- Set `data-consensus.peers-count = N-1` so every round includes all DL1 nodes (no lingering copy).
- Keep the block-validity gate (`validateData`) STRUCTURAL only; defer stateful rejection to the
  combiner (which can reject gracefully without aborting the snapshot). Only works for checks the
  combiner re-does authoritatively.

## The real framework fixes (deferred)

- **Clear the mempool across nodes** when an update lands in an accepted snapshot, not just on the
  node whose block carried it (dedup by update hash against the accepted snapshot's contents).
- **Partial / graceful block acceptance** — accept the valid txs in a block and drop only the
  Invalid ones, instead of rejecting the whole block. The blocker: a `DataApplicationBlock` is a
  *signed* consensus artifact (L1 facilitators sign the whole block), so you can't drop txs from it
  without invalidating the signatures. Needs either a re-sign step or an acceptance model that
  validates/accepts at tx granularity below the signed envelope.
- Surface combine-time rejections to clients/indexers (today only `validateData` rejections do).
