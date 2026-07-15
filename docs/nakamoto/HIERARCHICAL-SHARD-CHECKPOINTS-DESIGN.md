# Hierarchical Shard Checkpoints

**Status:** Target shard design; implementation currently regressed to ordinary
noncommittee GL0 recreation of sharded CL1 transitions. Every GL0 validator's
independent execution and validation of direct native GL1/DAG-token transitions
is permanent and not part of that regression. ADR-0016/0017 and
`../review/CONSENSUS-ARTIFACT-LIFECYCLE.md` are normative.

## 1. Purpose and topology

Execution shards partition accumulation and framework execution. They are internal
GL0 infrastructure, not another application layer:

```text
CL1 economic blocks ----+
                       +-> ML0 binary -> GL0 admission -> shard buffer
DL1 custom data blocks -+                                  |
                                                           v
                                  staircase checkpoint -> GL0 snapshot
```

The diagram scopes the shard path only. Native DAG-token blocks travel directly
`GL1 -> GL0` and every GL0 validator executes and validates them against the exact
proposal parent. The target also requires every GL0 validator to run the
deterministic global conflict/nullifier/settlement kernel; that E9 kernel is not
complete today. Neither responsibility is delegated to an execution shard.

ML0 operators run metagraph consensus and sign the binary. A GL0 admission
committee authenticates intake. A separate GL0 execution committee builds the
checkpoint. Membership in one group never implies membership in another.

In the target economic mode, `numShards=1` creates one execution shard and uses
the same protocol. Current code paths that treat one shard as disabled are gaps.

## 2. Assignment and committee rotation

V1 metagraph assignment is deterministic:

```text
unsignedBigEndian(SHA-256(metagraphAddress)) mod numShards
```

V1 `numShards` is immutable for the network/era. A local or ordinary parameter
change would remap metagraphs and fork state. Live resharding is future protocol
work requiring a hash-bound era transition, deterministic drain and state handoff,
old/new committee overlap rule, replay/nullifier continuity, Phase-2 replacement
behavior, and one atomic activation. `k2` cannot authorize resharding.

The execution committee is selected by the current public deterministic VK-hash
draw over the eligible GL0 set for a Phase-2-anchored `(eta, shardId, epoch)`.
This is deliberately not the abandoned stake-weighted secret-VRF draw. The
checkpoint VRF proves possession of the registered key for the slot.

Membership rotates at the eta/epoch boundary derived from the exact Phase-2 GL0
anchor. The checkpoint cannot self-claim a favorable wire epoch. Within one
committee, members are deterministically hash-ordered for the next shard ordinal.
One member owns each staircase duty window, then duty moves to the next rank and
wraps. The exact duty length and genesis multiplier are canonical parameters.

Current source parameters, pending derivation, protocol freeze, and security proof:

| Mechanism | Current cadence/rule |
|---|---|
| Binary-admission committee | New secret-VRF self-sortition for each `(eta, metagraph, parentHash)`; a new ML0 parent or eta changes the draw. |
| Execution membership | Fixed for one `(shardId, etaPeriod)`; eta period length `R = round(3.1*k1)` = 3174 mainnet, 794 test/integration, 99 dev. |
| Producer duty | Hash-shuffled anew for each next shard ordinal; each rank owns 5 slots, wrapping through the committee. |
| Genesis duty | Same order with a 12x window, currently 60 slots per rank while the mesh forms. |
| Draw/quorum | Current test/default target `kDraw=8`, `kQuorum=6`; quorum is a direct count, not an implicit `ceil(2*k/3)` formula. |
| Shard depth fallback | Removed. Distinct replay-backed `kQuorum` is mandatory; `retainedCheckpoints` is storage capacity only. |

`R` is currently derived with `Double`; the target parameter contract replaces it
with exact rational/integer arithmetic and binds every value in canonical state.

## 3. Two independent checkpoint axes

Execution validity:

```text
UnexecutedClaim
  -> ProducerExecuted
  -> SignerVerified (independently for each signer)
  -> ExecutionCertified (distinct kQuorum)
  -> Challenged/Invalid, if objective watchtower evidence succeeds
```

Chain/global position:

```text
ShardCandidate
  -> ShardPreferred
  -> EmbeddedTentative
  -> Operational at containing GL0 Phase 2
  -> locally retention-eligible only after all k2/challenge/DA/recovery/ack horizons

P2 -> Orphaned/Requeued on a later valid GL0 density reorg
```

Staircase parent/ordinal ordering and fork choice select a checkpoint candidate.
They do not establish execution and cannot bypass missing `kQuorum` replay
signatures. The exact containing GL0 Phase 2 supplies global operational status.

## 4. Producer accumulation and execution

V1 permits exactly one checkpoint per shard whose exact containing GL0 snapshot
has not reached Phase 2. That checkpoint may batch multiple metagraphs and one
bounded parent-contiguous ordered binary list per metagraph. Later inputs remain
buffered for its successor. Tentative embedding, ordinal equality, shard depth,
or a different same-ordinal hash never releases the successor; only the exact
checkpoint carried by the exact GL0 snapshot that reaches Phase 2 does. Restart,
replacement, and orphan requeue are owned by the durable FinalityGate transaction.

The scheduled producer:

1. resolves an exact canonical Phase-2 GL0 base `(ordinal,hash,parentHash,mptRoot)`;
2. derives the anchored execution roster/epoch and validates its staircase duty;
3. takes a deterministic bounded fair window from assigned metagraph queues while
   preserving each MG's exact parent order;
4. resolves every complete signed ML0 binary/custom-data commitment;
5. runs the shared framework kernel for every currency portion;
6. creates one canonical per-MG diff and complete root that covers every writable
   key;
7. extracts cross-metagraph framework intents without pre-writing global
   nullifier/inbox or another MG's partition;
8. signs and publishes the complete checkpoint.

Missing base, parent, body, DA material, schema, or deterministic result means no
checkpoint. There is no fallback to best tip, live store, peer-selected state, or
claimed ML0 cumulative fields.

## 5. Replay-before-sign

Every execution-committee member independently resolves the same base and bytes,
runs the same framework kernel, and compares accepted/rejected IDs, extracted
intents, canonical diff, and root:

```text
exact match         -> build VerifiedShardCheckpoint -> sign exact preimage once
unavailable input   -> defer; no signature; no slash
result mismatch     -> reject; retain objective comparison evidence
```

Best-tip selection, ancestor reception, signature count, and depth cannot produce
`VerifiedShardCheckpoint`. The attestation emitter must accept that capability,
not a naked hash. Retroactive ancestor signing follows the same replay rule.

The signed preimage binds network, genesis, era/parameters, shard, roster/epoch,
parent, ordinal, slot/duty, Phase-2 base, exact ordered inputs/DA commitments,
per-MG diffs/roots, extracted global intents, and custom commitments.

## 6. Ordinary GL0 adoption

An ordinary noncommittee GL0 node does not recreate ML0 currency snapshots. It:

1. verifies producer eligibility, KES/VRF, anchored roster, distinct execution
   signatures, and the mandatory threshold;
2. verifies shard parent/ordinal and every per-MG parent/continuation;
3. verifies the exact Phase-2 base and required bytes/commitments;
4. validates canonical diff ordering, uniqueness, bounds, namespace, and complete
   root coverage;
5. compare-and-sets each signed per-MG pre-root/version against the proposal
   parent's current `Ml0FrameworkMirror` root/version;
6. applies the diff to the exact base and recomputes each root;
7. submits extracted global intents to the universal GL0 conflict/settlement
   kernel;
8. stages all effects atomically in the containing GL0 branch.

The root is checked, never installed directly. The diff is reproduced committee
output, not an ML0 authoritative override.

The greenfield schema restores only `ShardCurrencyStateDiff` and
`perMetagraphStateDiff`. It does not restore `authoritative*`,
`AdoptFromSignedFields`, direct cross-shard receipts, unproved per-field
replacement deltas, or undeployed compatibility codecs.

## 7. GL0 phase integration

A checkpoint may be stored/selected and included in a tentative GL0 candidate
without becoming a durable shard anchor. When the exact containing GL0 hash enters
Phase 2:

- the checkpoint becomes operational;
- its hard anchor advances;
- downstream layers may reference its GL0 state;
- its binaries become operationally confirmed but remain retained/reversible.

If maxvalid-bg replaces that Phase-2 branch, the anchor reverses, the checkpoint
is orphaned, and each still-valid input requeues exactly once against the new
base. Local pruning waits for the maximum of recommended `k2`, challenge, DA,
recovery, and acknowledgement horizons. Pruning never creates a fork-choice
floor; an older candidate forces authenticated reconstruction before comparison.

## 8. Cross-metagraph economics

Shards do not settle directly. A source authorization must already exist in an
exact canonical Phase-2 GL0 state. Execution committees reproduce and sign the
framework consume intent, but their per-MG diff cannot decide a global conflict.
Every GL0 node runs one deterministic global order/nullifier/settlement kernel
over intents from all included checkpoints.

One-shot settlement writes authorization status, permanent nullifier, exact
balance/reservation deltas, and pending delivery atomically. ML0 acknowledgement
is hash-bound mirror progress only. Physical shard count cannot change the result.

## 9. Payload and data availability

O-09 and L-16 require three explicit signed lanes:

- `FrameworkCurrency`;
- `FrameworkCurrencyWithData`; and
- a limited standalone opaque/data-only lane.

The framework portion of either framework lane always follows replay/diff rules.
Custom bytes are a separate authenticated availability commitment and cannot
influence the framework result. The owner-retained standalone opaque/data-only
lane has authenticated custody/availability/ordering only, zero framework-economic
write surface, and no path to import claimed framework state. Decoder success can
never select or promote a lane.

Complete inputs remain available through the maximum of recommended `k2`, challenge,
watchtower, rollback, recovery, and downstream acknowledgement horizons. Missing
data prevents execution/signing/eligibility; network nonresponse alone is not
slash evidence.

## 10. Watchtowers and slashing

Deterministically assigned noncommittee watchtowers replay the complete checkpoint.
A challenge binds exact base, inputs, signed checkpoint, signer set, reproduced
diff/root, and mismatch. Only actual signers are liable. Missing history, transport
timeout, or an unresolvable base cannot slash. The watchtower assertion is not a
verdict; a ratified deterministic adjudicator computes the mismatch, initially by
exceptional bounded universal GL0 replay of the challenged exact checkpoint.

O-03/L-09 ratify positive assigned-watchtower replay coverage as a prerequisite
for GL0-inclusion eligibility; a post-release challenge window is not an
alternative. Engineering must derive and freeze the population, minimum coverage,
assignment anchor, deadline, redraw/retry, bond, replay-budget, and censorship
parameters. Blocking only withdraw/cross-MG is insufficient: invalid value must
not transfer locally, pay fees, stake, gain reward weight, mint, bridge, or compact
before release.

An uncovered checkpoint waits while unrelated GL0 and other-shard work proceeds.
Timeout/nonresponse cannot release value or slash.

## 11. Current implementation delta

Commit `c610a0740` removed the diff and made ordinary receivers/adopters replay:

- current `ShardDerivedStateDelta` has roots and binaries only;
- `ShardCheckpointGl0AcceptanceManager.verifyEmbedded` replays unconditionally;
- GSAM recreates adopted currency state again;
- the emitter still accepts a naked hash.

The repair must retain current full framework recreation for producer/signers and
watchtowers, restore only the canonical root-covered diff, make blind signing
unrepresentable, and replace ordinary replay with apply/root verification.

## 12. Required gates

- honest root/diff signs; tampered root/diff/base/input/intent never signs;
- missing history defers and never false-slashes;
- ordinary adoption records zero recreation calls;
- apply-diff produces exact root across independent nodes/platforms;
- depth cannot bypass the execution threshold;
- tentative inclusion cannot hard-anchor; P2 can; P2 reorg reverses/requeues;
- colluding threshold is caught under the owner-approved watchtower release rule;
- concurrent cross-shard consume yields one global winner;
- shard counts 1 and K yield byte-identical economic state.
