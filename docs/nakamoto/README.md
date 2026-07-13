# Nakamoto Design Index

The directory contains research, incident notes, superseded proposals, and active
design inputs from different commits. A filename in this directory is not, by
itself, a design decision.

## Normative order

Use these in order for new consensus work:

1. `../../AGENTS.md` - permanent layer/authority/finality invariants.
2. `../adr/0016-execution-sharding-reexecution-and-cross-shard-reads.md` - layer,
   shard, and Phase-2 cross-metagraph decision.
3. `../adr/0017-committee-reexecution-is-the-primary-economic-validity-gate.md` -
   replay-before-sign and noncommittee diff adoption.
4. `../review/CONSENSUS-ARTIFACT-LIFECYCLE.md` - full target lifecycle and open
   owner decisions.
5. `../review/CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md` - dependency-ordered work.
6. `../review/CONSENSUS-PROTOCOL-TEST-PLAN.md` - packet and release gates.
7. `../review/CONSENSUS-OWNER-DECISIONS.md` - locked and explicitly open owner
   protocol choices.

Code and tests still outrank documentation as evidence of current behavior. The
normative files explicitly separate current source facts from target design.

## Active design inputs

- `AVALANCHE-ATTESTATION-PROPOSAL.md` - research input for the optimistic Phase-2
  rail. Its K/alpha/beta cascade is not implemented today; the exact Phase-2
  threshold remains an owner decision.
- `GENESIS-DENSITY-PHASE2-REORG-AUDIT.md` - accepted P2-reorgable density and
  retention-only `k2` semantics.
- `HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` - target staircase checkpoint flow,
  replay-before-sign, canonical diff, watchtower, and GL0 phase integration.
- `COMMITTEE-SORTITION-DESIGN.md`, `ETA-ROTATION-AMORTIZATION-DESIGN.md`, and
  `SHARD-CHECKPOINT-*` notes - implementation evidence/inputs only; recheck every
  parameter and claim against the normative lifecycle and current source.

## Superseded or historical

- `attestation-and-finality.md` - historical trigger/runtime description. Its old
  ordinal-monotone and P2-irreversible claims are rejected.
- `UNIFIED-CONSENSUS-ENGINE-DESIGN.md` - superseded for ML0. ML0 may remain BFT;
  never use this draft to add BFT machinery to GL0.
- `CURRENCY-APP-TOKEN-ENFORCEMENT.md` - historical record of the rejected
  universal-GL0 recreation regression, not the target execution model.
- `IMPLEMENTATION-PLAN-POST-VALIDATION.md`, session handoffs, forensic reports,
  flake/RCA notes, and `historical/` - evidence and history, not current work order.
- `SHARD-CHECKPOINT-MONOTONICITY-DESIGN.md` and
  `SHARD-CHECKPOINT-CHAINSYNC-DESIGN.md` - forensic evidence only. Their
  configurable pipeline and shard-depth fallback language is retired.
- Roots-only, `AdoptFromSignedFields`, direct shard-receipt, stake-weighted secret
  execution-VRF, universal ordinary-GL0 CL1 replay, and GSI-authority directions
  are retired. Do not implement them from an older note.

## Required vocabulary

- GL0: Nakamoto/Taktikos/LDD global chain.
- ML0: metagraph consensus, BFT permitted.
- P2: exact-hash operational state, density-reorgable.
- `k2`: recommended retention/proof/automatic-rollback capacity only, never a
  phase, finality claim, pruning authority, or fork-choice floor. History older
  than local retention triggers authenticated `RecoveryRequired` reconstruction.
- Execution signature: signer independently reproduced exact diff/root.
- Admission/custody attestation: separately domain-labeled intake/availability
  claim; never state validity.
- `numShards=1`: one execution shard in economic mode, not a security bypass.

Any Phase-3 or immutable-`k2` language elsewhere in this directory is historical
and rejected, even if the surrounding document remains useful research input.

Any new document that changes these statements needs an owner-approved ADR and an
update to the lifecycle, roadmap, test plan, and this index in the same commit.
