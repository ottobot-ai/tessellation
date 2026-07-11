# Shard Checkpoint Flow

**Status:** current flow. ADR-0016 and ADR-0017 are normative.

## Layer Topology

```text
native DAG client -> GL1 -------------------------------> GL0

economic client -> CL1 --+
                          +-> ML0 -> state-channel binary -> GL0 admission -> execution shard -> GL0
custom data client -> DL1-+

finalized GL0 state -> GL1, ML0, CL1, DL1
```

GL1 is the native DAG-token edge application and sends blocks directly to GL0. CL1 and DL1 send blocks to ML0. ML0 runs metagraph
snapshot consensus and submits the resulting signed state-channel binary to GL0. ML0 operators produce and sign that binary; they are not
the GL0 admission or execution committee by virtue of operating ML0.

## Binary Admission

For each `(eta, metagraphAddress, parentHash)`, eligible GL0 operators independently run the secret-key admission VRF. The draw is uniform
`1/N` today. A successful admission authenticates and buffers the binary for its statically assigned execution shard. It does not prove the
binary's framework-economic state transition.

## Execution Checkpoint

Execution membership is a separate public deterministic registered-VRF-key hash draw for `(eta, shardId, period)`. Members are eligible
GL0 operators. Honest producers derive membership once per finalized-anchor eta period; within the claimed period, hash-shuffled staircase
duty selects one producer window for each next shard ordinal. Current adopters do not bind the wire-carried period back to the anchor, so
the rotation is grindable until SHARD-03 is fixed.

The scheduled producer:

1. selects a chain-linked binary window from the shard buffer;
2. pins a finalized GL0 `executionBaseOrdinal`;
3. recreates every included CL1 transition against that exact prior and finalized GL0 references;
4. computes each metagraph MPT root;
5. signs the checkpoint containing the roots and all replay inputs.

Other committee signatures authenticate participation and availability. They never authorize a root. An attester must pass the same
checkpoint validation and recreation gate before vouching for the checkpoint.

## GL0 Inclusion

Every GL0 node verifies the envelope, registered keys, KES evidence, committee membership, shard assignment, ancestry, and exact schema.
It then independently recreates every included CL1 transition at the signed execution base and compares the local roots with the claims.
Only a replay-valid checkpoint may be stored, attested, selected, embedded, or used to update canonical state.

Committee quorum and shard depth choose among replay-valid checkpoints. Watchtower fraud proofs and slashing are defense in depth. Neither
is a substitute for validity-before-use.

DL1 custom application semantics are different: GL0 cannot execute arbitrary metagraph code, so DL1 remains proof-carried. Framework
economics inside the same ML0 binary remain universally executable and must pass GL0 recreation.

## Finality And Return

A checkpoint inherits the finality of the GL0 snapshot that embeds it. Downstream services follow finalized GL0 state. CL1 does not
override or independently reinterpret that canonical return state; it verifies the signed global snapshot and advances its local follower
view.

Cross-shard framework operations settle only at GL0. A consuming transition executes against an owner value from the finalized GL0 MPT
base and writes a permanent canonical nullifier. An owner value visible only on an unfinalized parent branch is unavailable until a later
finalized snapshot.

See `HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` for schema, rotation, recovery, and operational gates.
