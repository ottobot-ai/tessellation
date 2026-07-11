# Hierarchical Shard Checkpoints

**Status:** current design of record, rewritten 2026-07-10. ADR-0016 and ADR-0017 are normative on economic authority.

## 1. Purpose

Execution shards partition collection and pre-execution of metagraph work. They do not partition economic trust. A shard checkpoint is a
signed, replayable proposal that GL0 may include after validating its operator duties and locally recreating every included CL1 transition.

The production default is `numShards = 1`, which disables this subsystem. Enabling multiple shards is a cluster-uniform configuration
change.

## 2. Operators And Topology

Both sharding committees are selected from eligible GL0 operators:

- The metagraph-binary admission committee self-sortitions independently for each `(eta, metagraph, parentHash)` with a secret-key VRF.
- The execution-shard committee is a separate public deterministic VK-hash draw for `(eta, shardId, epoch, registeredVrfVk)`.

Both draws currently use uniform `1/N` operator weight, but admission uses the full eligible GL0 validator set while execution uses the
post-cooldown eligible pool. ML0 operators produce and sign metagraph binaries; they are not members of either GL0 committee by virtue of
operating ML0.

Data flow:

```text
CL1 economic blocks ----+
                       +--> ML0 binary --> GL0 admission --> shard buffer --> checkpoint --> GL0 snapshot
DL1 custom data blocks -+
```

## 3. Current Schema

`ShardCheckpoint` contains:

- `shardId`, `shardOrdinal`, and `parentCheckpointHash`;
- the GL0 anchor, slot, and eta period;
- a signed `executionBaseOrdinal` naming the finalized prior used for replay;
- per-metagraph root claims;
- the complete included signed state-channel binary window;
- committee member signatures.

It does not contain a framework-economic state diff, balance replacement, artifact delta, sync delta, or cross-shard receipt. Those
fork-only schemas were never deployed and have been removed.

The checkpoint signing preimage covers the complete current shape, including the execution base and included binaries. JSON, scodec, and
protobuf decoders reject missing current fields and invalid negative ordinals.

Three ordinal spaces are independent and must never be compared as if they were
one counter:

| Space | Advanced by | Owner |
|---|---|---|
| GL0 global ordinal | Global snapshot consensus | GL0 cluster |
| Metagraph currency ordinal | Currency snapshot production | One ML0 cohort |
| Shard checkpoint ordinal | A checkpoint over a window of metagraph binaries | Execution-shard committee |

A normal state can therefore be GL0 ordinal 462, currency ordinal 250, and shard
checkpoint ordinal 3. The checkpoint ordinal counts windows, not currency
snapshots.

## 4. Assignment And Rotation

Metagraph assignment is static in v1:

```text
unsignedBigEndian(SHA-256(metagraphAddress)) mod numShards
```

Every GL0 node computes the same result. ML0 does not select a shard.

Honest production redraws execution membership once per `(shardId, etaPeriod)` using the period of the checkpoint's finalized GL0 anchor,
and may cache it only after the period's slash-exclusion anchor is settled. The current adopter verifies the committee for the
wire-carried period but does not recompute that period from the anchor; SHARD-03 therefore leaves rotation grindable by a Byzantine
producer. An eta period is `R = round(3.1 * k1)` GL0 ordinals: 3174 on mainnet, 794 on testnet/integrationnet, and 99 on dev under current
defaults.

Within a committee, members are hash-ordered for each next shard ordinal. One rank owns each five-slot duty window by default, wrapping
over the committee. Genesis uses a 12x wider window while gossip converges. This staircase duty replaced the abandoned per-slot LDD shard
leader lottery.

## 5. Shard Chain And Selection Finality

Each shard maintains a chain keyed by checkpoint hash and parent hash. `ShardOrdinal.Root` is zero; the first emitted checkpoint is one.
Committee-count and shard-depth triggers choose when a replay-valid checkpoint is selectable for GL0 inclusion. Neither trigger proves
economic validity. A checkpoint must pass replay before it is stored, attested, counted, or returned as accepted.

## 6. Production

The scheduled GL0 operator:

1. reads the finalized per-metagraph base tips;
2. takes a deterministic, chain-linked window from the shard buffer;
3. captures one finalized `executionBaseOrdinal`;
4. recreates each CL1 transition against that exact prior and finalized GL0 references;
5. computes the per-metagraph MPT roots from the recreated state;
6. signs and publishes the checkpoint only if the base did not move during execution.

An unavailable base, missing finalized GL0 reference, empty window, duty mismatch, or replay failure produces no checkpoint.

## 7. GL0 Admission

Every GL0 adopter independently verifies:

- outer and committee Ed25519 signatures;
- KES evidence and registered keys;
- committee membership and registered VRF-key possession;
- shard ownership for every metagraph key;
- checkpoint chain linkage and current schema validity;
- exact CL1 replay at `executionBaseOrdinal`;
- equality between each recreated MPT root and the signed root claim.

Signature quorum and depth are selection inputs only. They never bypass replay. `Hash.empty` or unavailable pinned history is a fail-closed
defer/reject and is not slash evidence by itself.

**Open enforcement gap (SHARD-03):** committee membership is checked against `checkpoint.epoch`, but the adopter does not yet require
`checkpoint.epoch == executionShardEpoch(checkpoint.gl0AnchorOrdinal)`. Until that equality is enforced, the producer can choose among
resolvable public epoch draws even though honest producer policy uses the finalized anchor.

## 8. Cross-Shard Economics

Shards never settle directly with one another. A consuming CL1 operation is recreated at GL0 against owner state from a consensus-pinned,
finalized GL0 MPT view. A proof supplies an input; it never substitutes for executing the operation.

Allow-spend consumption records a permanent canonical GL0 nullifier. Pending settlement overlays remain only while the exact owner action
is still in the consensus pending set, and acknowledgements are generated from GL0 ordinals actually replayed by the owner.

The GL0 accept wiring constructs the cross-shard client from the finalized MPT base. State visible only on an unfinalized candidate branch
is absent to the consuming operation and must wait for a later finalized GL0 snapshot.

## 9. Recovery And Observability

Checkpoint pull is read-only. A fetched checkpoint re-enters the same signature, duty, assignment, and replay gates as gossip. Node-local
peer choice, retry timing, metrics, cooldowns, and best tips may affect availability but may not affect a root or validity decision.

Metrics are shard-labelled and bounded-cardinality. Partition monitors are diagnostic only.

## 10. Slashing

Invalid-state and equivocation evidence must be self-contained, cryptographically authenticated, deterministic, and folded into canonical
GL0 state before it has economic effect. Watchtower detection is defense in depth; later slashing is never permission to expose value from
an unexecuted checkpoint.

## 13. Operational Gate

Before `numShards > 1` is deployable, the repository must prove:

- universal GL0 replay on every admission/finality branch;
- finalized cross-shard reads;
- deterministic replay across independent nodes;
- lossless backpressure and restart recovery;
- valid, non-forgeable slash evidence with honest-node false-positive tests;
- full topology tests with Byzantine producers and committees at and beyond quorum.
