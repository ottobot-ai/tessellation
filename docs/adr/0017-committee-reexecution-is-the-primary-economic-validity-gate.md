# 17. Universal GL0 re-execution is the CL1 economic-validity gate

Date: 2026-07-10

## Status

Accepted (amended 2026-07-10)

**Implementation safety status (2026-07-11): BLOCKED.** This ADR defines the
required authority boundary. Source audit confirms that the roots/diff/quorum
bypass is removed, but does not certify the global transition function itself.
Unsigned token unlocks and no-reference spends, bounded processed-history replay,
unbacked stake records, and unsafe finality still violate the broader economic
invariant. See `docs/review/CORRECTNESS-SECURITY-AUDIT-2026-07-11.md`.

Supersedes the earlier revision of this ADR that allowed non-committee GL0 nodes to adopt on committee signatures and a verified byte-diff.
That was not sufficient under the stated threat model: a colluding quorum can sign the same invalid computation, and a root or proof only
binds a claim; it does not establish that the framework transition was executed.

## Context

CL1 transfer, fee, reward, allow-spend, spend, token-lock, balance, reference, and supply effects are framework-defined. Every GL0 binary
contains the same Scala implementation needed to execute them. Before this decision, three shortcuts bypassed that implementation:

1. Currency snapshots carried cumulative `authoritative*` fields that could replace balances, active sets, and reference maps.
2. A shard committee byte-diff replaced the result produced by the GL0 currency processor.
3. `kQuorum` signatures allowed `verifyEmbedded` to return `Accepted` without re-execution; committee members also signed best-tip
   checkpoints and stored ancestors without first recomputing their roots.

Hash equality, a metagraph signature, committee agreement, and post-adoption slashing are not substitutes for executing CL1 before value is
usable.

## Decision

### 1. One CL1 transition function

`CurrencySnapshotValidator` artifact recreation, reached through `CurrencySnapshotContextFunctions.createContext`, is the only accepted
CL1 transition function. Producer, committee signer, global-snapshot producer, and every GL0 follower must run it over the checkpoint's full
included snapshot bytes.

Re-execution uses:

- the checkpoint's signed `executionBaseOrdinal` for the prior currency state;
- the currency snapshot's signed `globalSyncView`, resolved through a finalized ordinal lookup and checked by hash;
- deterministic framework code only.

An unavailable pinned input means defer/reject. It never means use a live head, use a local best tip, or trust a claimed cumulative field.

### 2. Signatures do not establish execution

Committee signatures retain their authentication, availability, and finality roles. Their count never establishes economic validity.

- A committee member re-executes before signing a received tip.
- Retroactive ancestor signing runs the same verifier.
- `evaluate` re-executes even when `T_count` qualifies.
- `verifyEmbedded` re-executes even when `distinctSigners >= kQuorum`.

A real mismatch is rejected and may form slash evidence. `Hash.empty` means cannot verify and is rejected without slashing.

### 3. No fork-only authority schema

This repository is a greenfield fork of upstream Tessellation v4.0.0. No fork-added authority schema has been deployed, so there is no
compatibility exception:

- `CurrencyIncrementalSnapshot.authoritative*` fields and codec slots do not exist;
- checkpoint state-diff, receipt, artifact-delta, balance-delta, and sync-delta fields do not exist;
- the checkpoint carries signed state-channel binaries, locally reproducible per-metagraph root claims, and a signed
  `executionBaseOrdinal`;
- decoders require the current fork schema rather than silently defaulting missing fork consensus fields.

### 4. DL1 remains proof-carried

This decision applies to framework CL1 economics. Arbitrary DL1 application state remains metagraph-defined because GL0 does not have its
code. A proof may authenticate that custom state, but DL1 state cannot authorize or override a CL1 economic transition.

### 5. GL0 recovery cannot install producer-carried state

A GL0 snapshot enters fork choice only after exact replay against its stored parent. The receiver ignores the producer-carried
`GlobalSnapshotInfo` and stores the context returned by local replay. A missing parent is fetched and buffered; a content mismatch is
rejected. Signature/root self-consistency, peer-served MPT bytes, and reward-only mismatch classification cannot authorize a canonical
state install. Recovery must obtain ancestry and replay each transition.

KES verification is a pre-storage condition. A producer must also obtain its KES signature before writing its own candidate to the chain
store; signing failure aborts the proposal and rolls back its state transaction.

## Consequences

- Every GL0 node pays the CL1 execution cost before adoption. Execution sharding may reduce proposal work and transport, but does not remove
  universal economic verification.
- A Byzantine committee at or beyond `kQuorum` cannot bypass recreation merely by colluding on signatures or a root claim. This does not
  prevent invalid state that the recreated global transition rules themselves accept.
- Determinism and pinned-input availability are now both safety and liveness requirements. Missing history fails closed and can halt the
  affected metagraph until the pinned input is recovered.
- Non-empty rewards are rejected when GL0 has no registered deterministic framework reward implementation; echoing the metagraph's claimed
  reward set is forbidden.
- Watchtower disputes remain defense in depth. They are not the pre-adoption validity gate.

## Enforcement sites

- `CurrencySnapshotValidator.scala`: signature verification plus exact artifact recreation; unregistered rewards default to empty.
- `GlobalSnapshotStateChannelEventsProcessor.scala`: every CL1 adoption mode calls `createContext`.
- `ShardCheckpointWiring.scala`: producer/verifier root derivation uses full recreation with pinned prior and global snapshot lookups.
- `GlobalSnapshotAcceptanceManager.scala`: adopted state is the recreation result.
- `ShardCheckpointGl0AcceptanceManager.scala`: `evaluate` and `verifyEmbedded` unconditionally re-execute.
- `NakamotoSyncDaemon.scala`: stored ancestors are verified before retroactive signing.
- `NakamotoSyncDaemon.scala`: parentless and replay-invalid snapshots never reach `chainStore.store`; producer-carried GSI/MPT recovery
  installers are removed.
- `SnapshotLeaderLoop.scala`: KES signing succeeds before a locally produced snapshot reaches `chainStore.store`.

## Out of scope

This ADR does not certify transition authorization/conservation, stake backing, replay protection, optimistic/depth finality, KES/VRF
fail-open behavior, cross-shard atomic settlement, or slashing-evidence authorization. Those are separate safety requirements and currently
block production.
