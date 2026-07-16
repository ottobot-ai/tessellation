# Shard Checkpoint Flow

**Status:** current flow. ADR-0016 and ADR-0017 are normative.

## Layer Topology

```text
native DAG client -> GL1 -------------------------------> GL0

economic client -> CL1 --+
                          +-> ML0 -> state-channel binary -> GL0 admission -> execution shard -> GL0
custom data client -> DL1-+

exact canonical Phase-2 GL0 state -> GL1, ML0, CL1, DL1
```

GL1 is the native DAG-token edge application and sends blocks directly to GL0. CL1 and DL1 send blocks to ML0. ML0 runs metagraph
snapshot consensus and submits the resulting signed state-channel binary to GL0. ML0 operators produce and sign that binary; they are not
the GL0 admission or execution committee by virtue of operating ML0.

Every GL0 validator independently executes and validates the direct GL1/DAG-token
transition against the exact proposal parent. Execution-certificate/diff reuse is
limited to the sharded CL1 path and cannot authorize or bypass native validation.
The target also requires every GL0 validator to run the global
conflict/nullifier/settlement kernel; that kernel remains E9 planned work.

## Binary Admission

For each `(eta, metagraphAddress, parentHash)`, eligible GL0 operators independently run the secret-key admission VRF. The draw is uniform
`1/N` today. A successful admission authenticates and buffers the binary for its statically assigned execution shard. It does not prove the
binary's framework-economic state transition.

## Execution Checkpoint

Execution membership is a separate public deterministic registered-VRF-key hash draw for `(eta, shardId, period)`. Members are eligible
GL0 operators. Honest producers derive membership once per finalized-anchor eta period; within the claimed period, hash-shuffled staircase
duty selects one producer window for each next shard ordinal. Worktree adopters now require the wire period to equal
`floor(gl0AnchorOrdinal/R)` before committee lookup. This catches an inconsistent pair but still accepts a producer-chosen older ordinal
and its matching favorable committee. Exact proposal-parent Phase-2 anchor-hash/freshness evidence and a rooted/canonical R remain open, so
SHARD-03/SHARD-09 remain RED.

The scheduled producer:

1. selects a chain-linked binary window from the shard buffer;
2. signs an exact GL0 execution-state claim
   `(ordinal,hash,parentHash,mptRoot)` selected from the current Phase-2
   projection;
3. recreates every included CL1 transition against that exact prior and finalized GL0 references;
4. computes each metagraph MPT root;
5. signs the checkpoint containing the roots and all replay inputs.

The four-field reference closes ordinal-sibling substitution for replay state,
but it is not itself Phase-2 evidence. Hash-bound FinalityGate evidence,
freshness/purpose, historical eta/registry/committee resolution, and reorg lease
invalidation remain open.

Every execution-committee signature is a state-validity claim: its signer must independently recreate the exact checkpoint result before
signing. Admission/custody attestations are separately typed availability claims and can never satisfy execution `kQuorum`.

## GL0 Inclusion

The current temporary implementation makes every GL0 receiver recreate every included CL1 transition before storage/adoption. The target
keeps recreation at the producer, every execution signer, assigned watchtowers, and exceptional objective challenge adjudication. An
ordinary noncommittee GL0 adopter instead verifies the execution certificate, positive watchtower coverage, exact Phase-2 base,
pre-root/version compare-and-set, canonical namespace-confined diff, and recomputed post-root. It never installs a claimed root.

Distinct configured execution `kQuorum` is mandatory. Staircase duty and valid-chain fork choice select the checkpoint lineage; no shard
depth threshold can replace a missing replay signature or positive watchtower coverage. A checkpoint-derived economic effect is not
GL0-inclusion-eligible before the required positive watchtower replay coverage.

DL1 custom application semantics are different: GL0 cannot execute arbitrary metagraph code, so the custom portion remains committed and
data-available without becoming a framework write. Framework economics in `FrameworkCurrency` and `FrameworkCurrencyWithData` use the
shared schema and must pass the execution-committee/watchtower/global-kernel enforcement above.

## Finality And Return

A checkpoint inherits the phase of the exact GL0 snapshot that embeds it. Downstream operational services follow exact canonical Phase-2
GL0 state. Phase 2 remains density-reorgable. CL1 does not
override or independently reinterpret that canonical return state; it verifies the signed global snapshot and advances its local follower
view.

Cross-shard framework operations settle only at GL0. A consuming transition executes against an owner value from the exact Phase-2 GL0 MPT
base and writes a permanent canonical nullifier. An owner value visible only on an unfinalized parent branch is unavailable until a later
Phase-2 snapshot.

See `HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` for schema, rotation, recovery, and operational gates.
