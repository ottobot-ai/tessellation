# 16. Execution sharding and finality-first cross-metagraph reads

Date: 2026-07-10

## Status

Accepted; corrected 2026-07-11 by owner clarification and ADR-0017.

**Implementation status: BLOCKED.** Commit `c610a0740` changed ordinary GL0
adoption from committee-produced byte diffs to universal currency recreation.
That is not this decision. The forward repair is specified in ADR-0017.

## Context

Tessellation has two submission paths and one canonical return path:

```text
client -> GL1 --------------------> GL0

client -> CL1 --+
                +-> ML0 ---------> GL0
client -> DL1 --+

canonical Phase-2 GL0 state -> GL1 / ML0 / CL1 / DL1
```

- GL0 is Global L0 / DAG L0 (`dag-l0`, `gl0.jar`). It runs global snapshot
  consensus, holds the canonical MPT, settles cross-metagraph effects, and owns
  global finality.
- GL1 is Global L1 / DAG L1 (`dag-l1`, `gl1.jar`). It submits native DAG-token
  blocks directly to GL0.
- ML0 / CL0 is Metagraph L0 / Currency L0 (`currency-l0`, `ml0.jar`). It runs
  metagraph snapshot consensus and submits state-channel binaries to GL0.
- CL1 is Currency L1 (`currency-l1`, `cl1.jar`). It submits framework-defined
  currency operations to ML0.
- DL1 is a data-application L1. In source it is a `CurrencyL1App` with an injected
  data-application service, not a separate formal `Layer` case.

The formal enum contains only `DagL0`, `DagL1`, `CurrencyL0`, and `CurrencyL1`
(`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/app/Layer.scala:3-7`).

ML0 consensus and the two GL0 committees are distinct:

1. ML0 operators agree on and sign the metagraph binary.
2. A per-metagraph admission committee of eligible GL0 operators authenticates
   and admits a binary.
3. A separately selected execution-shard committee of eligible GL0 operators
   accumulates and executes a checkpoint window.

Operating ML0 does not automatically make an operator a member of either GL0
committee.

## Decision

### 1. Sharding partitions execution work

Metagraph assignment is deterministic:

```text
shardId = unsignedBigEndian(SHA-256(metagraphAddress)) mod numShards
```

Execution-shard membership is the current public deterministic VK-hash draw over
eligible GL0 operators, not the abandoned stake-weighted secret-VRF design. The
registered VRF proof establishes key possession for the checkpoint slot; it does
not make membership secret. Producer duty rotates within a committee by the
deterministic staircase schedule.

The committee executes framework currency inputs and emits a canonical byte diff
and result root. Every execution signature means its signer independently
reproduced those exact outputs. Noncommittee GL0 nodes verify the execution
threshold and apply the diff with a root check. Watchtowers independently replay
as the collusion backstop. ADR-0017 defines this contract.

### 2. Layer topology does not change

Execution shards are internal GL0 infrastructure. They are not an additional
application layer and do not reroute applications:

```text
GL1 -> GL0
CL1/DL1 -> ML0 -> GL0
Phase-2 GL0 state -> every downstream follower
```

ML0 may retain BFT consensus for its small, well-connected validator set. Global
GL0 consensus remains Nakamoto/Taktikos/LDD. Do not introduce a global
proposal/vote/lock/QC/view-change protocol.

### 3. Framework currency and custom data have different guarantees

Currency and currency-with-data metagraphs share the framework currency schema.
Their economic portion is execution-sharded and re-executed before an execution
signature. Custom DL1 bytes are isolated commitment/availability payloads because
GL0 does not run arbitrary metagraph code. Custom application output cannot
authorize, synthesize, or overwrite balances, supply, locks, reservations,
nullifiers, fees, rewards, or framework references. An independently signed
framework fee/intent may explicitly bind the exact custom-data commitment; changing
that commitment rejects the bound framework intent atomically.

General opaque-only state-channel support is not required by this ADR. If retained,
it is authenticated data carriage only and has no framework-economic effect.

### 4. Cross-metagraph reads are Phase-2-first

Shard committees do not directly trust or settle with one another. A consuming
framework operation resolves its source authorization against an exact canonical
GL0 Phase-2 reference:

```text
GlobalSnapshotStateRef(ordinal, hash, parentHash, mptRoot)
```

Phase 0/1, a local best tip, an ordinal without its hash/root, a committee-only
root, a peer-selected response, and receiver wall clock are not valid read bases.
If the referenced value has not reached Phase 2, processing defers and retries.

Phase 2 is operational finality, not an absolute floor. A maxvalid-bg density
reorg can orphan a Phase-2 hash. Checkpoints and followers whose base is orphaned
must roll back and replay against the replacement Phase-2 branch. `k2` is retained
recovery capacity and never prevents an objectively denser valid branch from
winning; history older than local retention requires authenticated reconstruction
before comparison and mutation.

### 5. GL0 serializes cross-metagraph effects

A per-metagraph diff is confined to its registered framework namespace. It may
not directly mutate another metagraph or global replay/nullifier state.
Cross-metagraph authorizations and consumes are extracted from replayed framework
inputs, ordered by one canonical GL0 rule, checked against the Phase-2 owner state,
and committed with permanent semantic replay keys and exact conservation deltas.

The physical shard count cannot change authorization, ordering, conservation, or
replay semantics. The same input trace must have the same economic result at
`numShards=1` and `numShards=K`.

In the target economic protocol, `numShards=1` means one execution shard; it does
not disable the committee/diff/watchtower validity path. Current `numShards > 1`
semantic gates are implementation defects. A direct full-recreation path may
exist only in an explicitly unsafe development profile and cannot define different
validity or state bytes.

## Consequences

- CL1 does not re-execute canonical state flowing back from GL0. It verifies the
  exact Phase-2 reference and adopts/resyncs it.
- A checkpoint is not globally operational merely because it has execution
  signatures or shard depth. Its effects become downstream-referenceable only
  when the containing GL0 snapshot reaches Phase 2 and the owner-ratified
  watchtower release condition is satisfied.
- A tentative containing GL0 snapshot cannot advance the durable shard anchor.
  Phase-2 transition of that exact GL0 hash advances it.
- The current ordinal-only `FinalityGate` is insufficient for cross-metagraph
  reads because it cannot distinguish a replaced hash at the same ordinal.
- Direct shard-to-shard receipts are unnecessary for the first protocol version.
  GL0 canonical state and nullifiers are the rendezvous point.

## Required follow-up decisions

1. Whether pure opaque/data-only metagraphs remain a supported public lane.
2. The exact Phase-2 rollback contract exposed to ML0 and external integrators.
3. The canonical ordering between same-snapshot cross-metagraph consumes,
   cancellation, expiry, refunds, and local spends.

## References

- `docs/adr/0017-committee-reexecution-is-the-primary-economic-validity-gate.md`
- `docs/nakamoto/GENESIS-DENSITY-PHASE2-REORG-AUDIT.md`
- `docs/review/CONSENSUS-ARTIFACT-LIFECYCLE.md`
- `docs/review/CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md`
