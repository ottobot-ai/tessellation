# Currency-App Token Enforcement

> **HISTORICAL REGRESSION RECORD - NOT CANONICAL.** This document describes the
> universal-GL0 recreation direction introduced by `c610a0740`. The owner rejected
> that architecture on 2026-07-11. Do not implement its “every GL0 adopter
> recreates CL1” rule. The target is ADR-0017: producer and every execution signer
> replay before signing; ordinary noncommittee GL0 nodes require the distinct
> execution threshold, compare-and-set the signed pre-root, apply the canonical
> scoped diff, and recompute the root; assigned watchtowers replay as the collusion
> backstop, and mandatory positive assigned replay coverage is required before GL0
> inclusion. Removing ordinary noncommittee CL1 recreation never applies to
> native GL1: every GL0 validator continues to execute and validate direct
> DAG-token transitions. The universal global conflict/nullifier/settlement kernel
> is target E9 work, not a current guarantee. `authoritative*` and
> `AdoptFromSignedFields` remain deleted.

**Status:** superseded historical record. Current every-adopter CL1 recreation is
temporary containment until the complete certified-diff/base/domain/watchtower
adoption gate lands.

## Invariant

Historical rejected invariant: every framework-defined CL1 economic transition is
recreated by every GL0 node. The surviving invariant is narrower and stronger at
the signing boundary: no execution-validity signer signs without independent
replay, and no ordinary adopter installs a claimed root without threshold/base/
diff/root verification.

This covers transfers, rewards, fees, allow-spends, spends, token-locks/unlocks, balances, active sets, reference maps, replay/nullifier
state, and supply effects.

## Trust boundary

| State | GL0 guarantee |
|---|---|
| Framework CL1 economics (historical rejected rule) | Universal deterministic recreation and exact artifact/root comparison |
| Arbitrary DL1 application state | Authenticated commitment/DA carriage only; GL0 does not execute unknown application semantics |
| Arbitrary non-currency state-channel bytes | Authenticated carriage only; they cannot directly authorize a CL1 economic mutation |

The boundary is framework-defined versus application-defined. A data application cannot relabel token movement as custom state and bypass
CL1 execution.

## Historical rejected ordinary-adopter path

1. Decode the included currency snapshot and cryptographically validate its signatures.
2. Resolve its prior currency state at the checkpoint's signed `executionBaseOrdinal`.
3. Resolve the signed `globalSyncView` through finalized GL0 history and verify the referenced hash.
4. Recreate the proposal artifact through `CurrencySnapshotValidator` and `CurrencySnapshotContextFunctions.createContext`.
5. Require exact artifact equality and a valid rebuilt state proof.
6. Compute the per-metagraph root from the recreated state and require it to equal the checkpoint root.
7. Commit the recreated state, never a committee-carried replacement.

Under the rejected regression, the producer and committee run the same function
before proposing/signing, but every GL0 adopter runs it again. Target ordinary
noncommittee adoption instead verifies the replay certificate and canonical diff,
applies it at the exact base, and recomputes the root. Missing pinned data remains
a defer/reject, never a live-state fallback.

## Removed authority paths

- `GlobalSnapshotStateChannelEventsProcessor.deriveAdoptedCurrencyInfo` is removed.
- `CurrencyAdoptionMode` and `AdoptFromSignedFields` are removed; `processCurrencySnapshots` has one execution path.
- `GlobalSnapshotAcceptanceManager` no longer reconstructs and commits `CurrencySnapshotInfo` from
  `perMetagraphStateDiff`.
- `perMetagraphStateDiff`, `emittedReceipts`, and the eight `CurrencyIncrementalSnapshot.authoritative*` slots do not exist. They were
  fork-only, never deployed, and have no compatibility decoder.
- Committee quorum and depth paths both re-execute; `verifyEmbedded` has no signature-count execution bypass.
- Best-tip and stored-ancestor attestations are emitted only after root recreation.
- GL0 ignores the producer-carried snapshot context, rejects replay mismatches, and buffers missing-parent snapshots until ancestry can be
  replayed. Direct fork, reward-realignment, and peer-state catch-up installers are removed.
- Incoming KES verification and local KES signing both happen before `chainStore.store`. Public KES evidence is persisted by snapshot hash
  so disk-served ancestry does not bypass KES after in-memory eviction.

## Rewards and custom economics

When GL0 has no registered deterministic framework reward implementation, the recreated reward set is empty. A metagraph cannot mint by
supplying rewards that GL0 merely echoes. Supporting a reward policy requires registering equivalent deterministic code on every GL0 node
behind an activation/version boundary.

DL1 calculated state may remain commitment/DA-carried, but GL0 does not certify
its unknown semantics and it cannot directly alter CL1 balances or supply. A
future proof-verifier lane requires its own deterministic active-era verifier.

## Remaining production blockers

Universal recreation closes the authoritative-field, committee-diff, blind-sign, and quorum-adopt classes. It does not by itself prove all
economic code correct. The following remain separate blockers until fixed and tested:

- cross-shard consumed-marker rejection must prune the corresponding SpendAction;
- cross-shard expiry/refund settlement needs an atomic owner-shard acknowledgement design;
- fee replay protection and update-to-fee bijection are not supplied by signature validation alone;
- optimistic/depth finality and slashing-evidence authorization require independent repair;
- finality/slashing composition and economic conservation still require independent adversarial proofs beyond the replay boundary.

## Enforcement sites

- `modules/node-shared/.../snapshot/CurrencySnapshotCreator.scala`
- `modules/node-shared/.../snapshot/CurrencySnapshotValidator.scala`
- `modules/node-shared/.../snapshot/CurrencySnapshotContextFunctions.scala`
- `modules/node-shared/.../snapshot/managers/global/GlobalSnapshotStateChannelEventsProcessor.scala`
- `modules/node-shared/.../infrastructure/sharding/ShardCheckpointWiring.scala`
- `modules/node-shared/.../snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala`
- `modules/node-shared/.../snapshot/managers/global/ShardCheckpointGl0AcceptanceManager.scala`
- `modules/dag-l0/.../snapshot/nakamoto/NakamotoSyncDaemon.scala`

See ADR-0016, ADR-0017, and `HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md`.
