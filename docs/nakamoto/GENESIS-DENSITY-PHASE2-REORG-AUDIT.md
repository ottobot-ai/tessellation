# Genesis Density Rule and Phase-2 Reorgability

**Original decision date:** 2026-06-10
**Owner correction:** 2026-07-12
**Status:** Target semantics accepted; runtime integration and parameter
qualification incomplete.

This document is subordinate to `../review/CONSENSUS-ARTIFACT-LIFECYCLE.md`,
`../review/CONSENSUS-OWNER-DECISIONS.md`, and `../../AGENTS.md`.

## Controlling invariant

An exact Phase-2 GL0 snapshot is operational and downstream-consumable, but it
remains replaceable by the valid chain selected under the ratified Taktikos/GKL
fork-choice rules. `k2 = 100 * k1` is recommended retained recovery/proof capacity
only. It is not a third protocol phase, an irreversible-finality claim, a pruning
authority, or an absolute fork-choice floor.

GL0 has no proposal/vote/lock/QC/view-change finality protocol. The optimistic
Phase-2 rail is the ratified Avalanche/Snowball decided-attestation mechanism;
the fallback is `k1` depth. Neither validates economics.

## Current source state

The implementation does not yet enforce this as one coherent gadget:

- `FinalityGate` remains an ordinal serving watermark rather than durable
  hash-bound phase state;
- the full K/alpha/beta sampling/cascade is absent;
- density recovery is disabled/incomplete and some store/watermark paths still
  behave as age-based floors;
- downstream monotone ordinal polling cannot represent same-ordinal replacement;
- Phase-2 transition callbacks, shard anchors, rollback journals, and outboxes are
  not one crash-consistent transaction;
- local retained history is not yet sufficient for authenticated reconstruction
  when the true common ancestor predates it.

The source currently derives `k2` from `k1`. Those values describe configured
storage capacity; they do not prove a probability bound or change fork choice.

## Fork-choice jurisdiction

```text
tip / short fork whose divergence is within k1
  -> valid-only maxvalid-tk

valid fork whose divergence is beyond k1
  -> valid-only maxvalid-bg density comparison from the true common ancestor

true common ancestor older than locally retained state
  -> RecoveryRequired: stop production/mutation, fetch authenticated exact
     history/state, reconstruct, compare objectively, then resume
```

P0/P1 candidates use `maxvalid-tk` in its proven jurisdiction. P1 enters P2 when
the exact current-canonical hash obtains portable decided-attestation evidence or
the `k1` fallback. P2 does not remove the snapshot from density comparison. An old
P2 hash becomes orphaned and a replacement exact hash becomes operational through
one explicit reorg transition.

Age alone never rejects a valid candidate. A node also never compares truncated
histories or asks an operator/peer which branch should win.

## Atomic Phase-2 replacement

The target `FinalityGate` emits a durable hash-bound transition:

```text
Phase2Reorg(
  oldOperationalRef,
  newOperationalRef,
  trueCommonAncestor,
  orphanedRange,
  adoptedRange,
  evidence
)
```

One crash-consistent transaction must:

1. verify exact candidate bytes and objective fork-choice evidence;
2. unwind the canonical MPT and every consensus-owned journal to the true common
   ancestor;
3. refold the winning branch through the same validation/execution path as live
   production;
4. replace exact P2 refs and optimistic/depth evidence;
5. recompute historical eta, registry, committee, tower, and parameter state;
6. reverse shard anchors and requeue each still-valid checkpoint input once;
7. reverse/reapply mempool, native, cross-metagraph settlement/nullifier, reward,
   slash, delivery, and acknowledgement state;
8. persist exact downstream/follower events in a retryable outbox.

The replacement may keep the same ordinal and change only the hash. No consumer
may infer it from an ordinal watermark.

## Retention and recovery

Retain authenticated snapshot bytes, ancestry, MPT undo data, optimistic
evidence, checkpoint inputs/diffs/signatures, watchtower evidence, DA chunks,
requeue identities, cross-metagraph state, and outbox events through the longest
applicable horizon. Recommended `k2` is one lower bound among challenge, DA,
downstream acknowledgement, and operational recovery requirements.

Local pruning is a service/capacity decision, not consensus truth. If a candidate
requires pruned history, the node enters `RecoveryRequired`; it does not refuse
the candidate because it is old. Recovery is MPT-primary and exact-byte verified,
with no peer-GSI installation authority.

## Downstream and shard behavior

- GL1/ML0/CL1/DL1 consume exact P2 refs and accept explicit replacement events.
- A shard checkpoint inherits operational status only from its exact containing
  GL0 P2 hash. There is no separate shard-depth or archival-finality ladder.
- A checkpoint whose execution base or containing hash is orphaned is invalidated;
  its still-valid ordered inputs requeue exactly once on the replacement base.
- Cross-metagraph effects unwind/refold atomically with the GL0 branch.
- External integrators choose their own confirmation-risk threshold. The protocol
  does not claim `k2` makes an irreversible external effect safe.

## Required tests

- comparator results are independent of argument and arrival order;
- valid short forks exercise `maxvalid-tk`; valid deep forks exercise
  `maxvalid-bg` from the true common ancestor;
- a denser fork older than local `k2` enters authenticated recovery and then wins;
- corrupt/missing recovery bytes halt before mutation;
- same-ordinal P2 replacement reaches every downstream consumer exactly once;
- restart at every transition write point returns the exact old or new state;
- shard anchor reversal and binary requeue are exact-once;
- cross-metagraph nullifier/settlement state unwinds and deterministically refolds;
- fresh bootstrap and reorg recovery derive identical eta/registry/tower state;
- pruning metadata never changes the winning valid chain.
