# 17. Committee replay-before-sign is the CL1 execution gate

Date: 2026-07-10

## Status

Accepted; restored and clarified 2026-07-11 by owner decision.

**Implementation status: REGRESSED / BLOCKED.** The original blind-sign defect
must not be restored, but commit `c610a0740` overcorrected by deleting the
checkpoint byte diff and making ordinary GL0 nodes replay CL1. The required repair
is selective: keep replay-before-sign, restore a canonical diff, and make
noncommittee adoption apply and verify that diff.

## Non-negotiable invariant

**A node never adds a state-validity signature to a result it has not independently
re-executed.** For a shard checkpoint, the signature means:

> I executed the exact signed framework inputs at the exact signed Phase-2 base
> and reproduced this exact canonical byte diff and this exact result root.

Receipt, best-tip selection, committee membership, ancestor status, signature
count, shard depth, or producer reputation can never construct that meaning.

## Context

The original implementation violated the invariant:

- committee members signed a checkpoint hash on best-tip without replay;
- `kQuorum` let GL0 adopt without replay;
- only the producer replayed on the happy path;
- watchtower replay happened after adoption.

The first ADR revision correctly required every committee signer to replay. The
later universal-replay amendment conflated two unrelated concepts:

1. **forbidden authority overrides:** ML0-carried `authoritative*` cumulative
   fields and `AdoptFromSignedFields`, which could replace framework state; and
2. **required execution output:** a canonical committee-produced byte diff that
   every signer independently reproduces and every adopter root-checks.

The first must remain deleted. The second must be restored in a greenfield-only
schema without compatibility shims.

## Decision

### 1. Producer execution

The deterministic staircase producer:

1. resolves the checkpoint's exact canonical Phase-2 GL0 execution base;
2. collects a deterministic bounded window of complete signed ML0 binaries in
   per-metagraph parent order;
3. runs the current framework recreation path for every currency portion;
4. extracts globally serialized framework effects separately from per-MG writes;
5. emits a canonical namespace-bounded per-MG byte diff and a complete root that
   commits every diff-writable key;
6. signs the complete checkpoint preimage.

The preimage binds at least network, genesis, protocol era/parameter hash, shard,
execution epoch/roster, shard parent, shard ordinal, slot/duty, exact Phase-2 base
`(ordinal,hash,stateRoot)`, complete ordered inputs or their availability-bound
commitment, per-MG diffs, per-MG roots, and custom-data commitments.

### 2. Committee replay-before-sign

Every execution-committee member runs the same recreation at the same base and
compares both the canonical diff bytes and root:

```text
match                -> construct VerifiedShardCheckpoint -> sign once
missing base/input    -> defer; no signature; no slash
diff or root mismatch -> refuse; emit deterministic fraud evidence if available
```

The attestation API must accept a verified capability, not a naked checkpoint
hash. A raw `emit(hash, ...)` API leaves blind signing representable and is not an
acceptable enforcement boundary.

### 3. Execution certificate and shard chain

`kQuorum` distinct signatures from the checkpoint's anchored execution committee
form an execution certificate. It asserts independent execution, not BFT consensus.

The shard chain remains Nakamoto-style:

- deterministic staircase producer duty;
- hash-linked checkpoint ancestry;
- maxvalid-tk sibling/tine selection; and
- Phase-2 GL0 hard anchors.

There is no shard proposal/vote/lock/view-change state machine. Depth qualifies
neither validity nor an execution certificate. `kQuorum` distinct replay-backed
signatures are mandatory for economic diff adoption. Each shard has at most one
checkpoint whose exact containing GL0 snapshot has not reached Phase 2; that exact
Phase-2 anchor releases its successor. There is no checkpoint pipeline and no
shard-depth fallback.

### 4. Noncommittee GL0 adoption

An ordinary GL0 node does not call currency recreation for an execution-certified
checkpoint. It performs:

1. producer/duty/VRF/KES and checkpoint signature verification;
2. anchored committee derivation and distinct `kQuorum` verification;
3. exact Phase-2 base, parent, ordinal, per-MG continuation, and input-availability
   checks;
4. diff canonicality, namespace, field allowlist, key uniqueness, and size bounds;
5. signed per-MG pre-root/version equality with the proposal parent's current
   `Ml0FrameworkMirror` root/version (or an equally strong unchanged proof);
6. apply diff to the pinned base and recompute every committed root;
7. global ordering/conflict checks for extracted cross-metagraph effects;
8. atomic adoption or no mutation.

This is verified computation reuse, not an authoritative override. The claimed
root is never installed directly.

### 5. Watchtower replay

Deterministically selected noncommittee watchtowers independently execute the
same complete checkpoint. They detect a colluding execution threshold and submit a
challenge only when exact inputs/base are available and their reproduced diff/root
differs. An unavailable base is not evidence of fraud. The assertion is not itself
objective: the ratified adjudicator must independently compute the mismatch, with
exceptional bounded GL0 replay of the challenged checkpoint as the initial
recommendation. Happy-path ordinary adoption remains zero-replay.

Evidence identifies and can slash actual checkpoint signers. Detection, evidence
transport, adjudication, bonded-principal debit, committee exclusion, and reward
must be deterministic canonical state. Slashing never repairs value that was
allowed to leave before the challenge was resolved. The locked release rule is
positive deterministic noncommittee replay coverage before the checkpoint becomes
GL0-inclusion-eligible. No checkpoint-derived local or external economic derivative
is usable while coverage is missing; unrelated GL0 snapshots may continue.

### 6. Schema boundary

Restore only the greenfield execution result:

```text
ShardCurrencyStateDiff(
  upserts: SortedMap[CanonicalMptKey, CanonicalValueBytes],
  removals: SortedSet[CanonicalMptKey]
)

ShardDerivedStateDelta(
  perMetagraphMptRoots,
  perMetagraphStateDiff,
  includedSnapshots,
  customDataCommitments
)
```

Exact final fields depend on the payload-lane decision. Do not restore:

- any `authoritative*` currency snapshot field;
- `AdoptFromSignedFields`;
- `CrossShardReceipt` or direct shard-to-shard settlement;
- unproved per-field balance/artifact/sync replacement deltas;
- fork-only V1/V2 decoders, defaulted missing fields, or compatibility bridges.

Every key writable by the diff must be covered by the verified per-MG root. The
current root covers field 5 plus seven MG partitions 25-31, but excludes economic
active allow-spends field 7 and field 32. The target complete economic root must
add active allow-spends. Field 32 is ML0-owned replay state: ML0 retains the exact
optional `globalSnapshotSyncView` in `CurrencySnapshotInfo`, whose state proof
hashes it, but GL0 must not store it as an unrooted writable mirror.

That GL0 mirror cannot be removed first. The current currency incremental carries
only `CurrencySnapshotStateProof.globalSnapshotSync` plus the accepted
`globalSnapshotSyncs` delta, not the full view preimage. Before removal, the
checkpoint's signed/root-bound framework replay input must carry the exact optional
full-view witness for every required window boundary and the explicit ML0 operator
population used to validate sync entries. `None` and `Some(empty)` are distinct.
Every producer, execution signer, watchtower, and exceptional adjudicator verifies
the witness against the state-proof hash and replays under that same population;
missing or mismatched material defers and cannot slash. Only after this gate passes
is field 32 removed from every GL0 MPT, diff, load, and reorg path. It remains in
ML0 `CurrencySnapshotInfo`.

## Current source gap

- `ShardDerivedStateDelta.scala` currently carries roots and binaries but no diff.
- `ShardCheckpointGl0AcceptanceManager.verifyEmbedded` unconditionally replays.
- `GlobalSnapshotAcceptanceManager.deriveAdoptedCurrencyState` replays again.
- `ShardCheckpointAttestationEmitter.emit` now accepts only a sealed
  `VerifiedShardCheckpoint` minted after intake replay. Rejected/mismatching
  checkpoints cannot reach this signing API, and replay-valid under-quorum
  checkpoints can collect signatures. The capability still proves the current
  root-only recreation, not the target diff/intents/complete-root result.
- intake and `verifyEmbedded` now enforce distinct configured execution `kQuorum`;
  current universal replay remains the temporary economic backstop until canonical
  diff adoption and positive watchtower coverage land.
- the signed checkpoint binds an execution-base ordinal but not the exact Phase-2
  hash/root or network/genesis/era/parameter domain.
- field 32 remains writable, removable, and reconstructible in GL0. Pinned peer
  backfill strips it to the global root entry set, while a locally staged base can
  retain it. `ShardCheckpointWiring.reExecDerivationAtPinnedBase` consumes the
  resulting prior `CurrencySnapshotInfo`; reconstruction turns missing field 32
  into `Some(empty)`. The current binary's proof hash plus accepted delta cannot
  reconstruct a nonempty prior, and replay derives the ML0 facilitator population
  from the artifact proof subset instead of a separately bound full population.
  ECO-F32 is therefore HIGH and OPEN, not an unused-metadata cleanup.
- the current `numShards > 1` activation gate still makes the one-shard economic
  configuration bypass the target committee/diff/watchtower path.

These facts prove neither the old blind-sign path nor universal replay is the
target.

## Required tests

1. Honest producer and every signer reproduce exact diff and root.
2. Changed root, changed diff, extra/unrooted key, wrong base, and wrong input order
   prevent signing.
3. Missing history defers and never creates slash evidence.
4. Ordinary noncommittee adoption performs zero currency recreation calls and
   still rejects every malformed diff/root/certificate.
5. Shard depth cannot bypass the execution threshold.
6. Tentative GL0 inclusion cannot advance the shard hard anchor; Phase 2 can.
7. A colluding execution threshold is caught by an assigned watchtower before the
   owner-approved release boundary.
8. `numShards=1` and `numShards=K` produce identical economic writes and roots.
9. A node with locally staged field-32 bytes and a node that root-verifies a
   field-32-stripped backfill reproduce the identical framework result from the
   exact replay witness. Missing/wrong witness, wrong ML0 operator population, and
   `None`/`Some(empty)` substitution prevent signing and cannot create slash
   evidence.
10. After witness activation, every GL0 diff, peer/disk load, shallow/deep reorg,
    and reconstruction rejects or omits field 32, while ML0 state proof generation
    still commits the exact optional view.

## Locked follow-up decisions

1. Distinct configured execution `kQuorum` is mandatory; depth never substitutes.
2. A deterministic noncommittee complement/sample with minimum positive replay
   coverage is required before GL0 inclusion eligibility. Exact population,
   coverage threshold, deadline, and availability fallback remain parameter work,
   not an alternate release rule.
3. Every framework-economic field, including active allow-spends, is in the complete
   per-MG root and diff. Field 32 remains ML0-owned `CurrencySnapshotInfo` state,
   hash-bound through its state proof. The exact view preimage and explicit ML0
   operator population become signed/root-bound replay inputs before field 32 is
   removed from all GL0 MPT/diff/load/reorg paths; it is never writable through the
   checkpoint diff.
4. The checkpoint binds and retains the exact signed currency incrementals needed
   for replay. Custom application data uses content-addressed commitments/chunks.
   Retention lasts through the maximum challenge, Phase-2 recovery, and downstream
   acknowledgement horizon.
5. V1 has one outstanding checkpoint per shard, may batch multiple metagraphs and
   contiguous binaries per metagraph, and has no configurable pipeline or
   shard-depth qualification.
6. A bonded, assigned, rate/resource-bounded challenge triggers exceptional GL0
   replay of the exact retained inputs and base. The computed result controls
   rollback/slash; missing authenticated data defers and cannot slash.

## References

- `docs/adr/0016-execution-sharding-reexecution-and-cross-shard-reads.md`
- `docs/review/CONSENSUS-ARTIFACT-LIFECYCLE.md`
- `docs/review/CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md`
