# Consensus Architecture and Artifact Lifecycle

**Status:** Target design baseline; ratified architecture and remaining parameter gates are explicit
**Source baseline:** `c610a0740c34833e563f8a94c2ab820186a75897`
**Security baseline:** `CORRECTNESS-SECURITY-AUDIT-2026-07-11.md`

Owner decisions are tracked in `CONSENSUS-OWNER-DECISIONS.md`.

This document is normative for implementation planning. It does not claim the
current code implements the target. When it conflicts with older fork-only notes,
the owner decisions recorded here and ADR-0016/0017 control.

## 1. Layer map and topology

| Name | Source/build identity | Responsibility |
|---|---|---|
| GL0 | Global L0, DAG L0, `dag-l0`, `gl0.jar` | Taktikos/LDD global snapshot chain, Avalanche/Snowball optimistic finality, depth fallback, canonical MPT, native DAG economics, framework settlement, and shard-checkpoint inclusion. |
| GL1 | Global L1, DAG L1, `dag-l1`, `gl1.jar` | Native DAG-token edge application; submits blocks directly to GL0. |
| ML0 / CL0 | Metagraph L0, Currency L0, `currency-l0`, `ml0.jar` | Metagraph snapshot consensus; aggregates CL1 framework currency and optional DL1 data into a state-channel binary submitted to GL0. |
| CL1 | Currency L1, `currency-l1`, `cl1.jar` | Framework-defined metagraph economic operations; submits blocks to ML0. |
| DL1 | Data application L1 | Custom metagraph logic injected into `CurrencyL1App`; submits data blocks to ML0. |
| ML1 | Conceptual name for CL1 plus DL1 | Not a distinct formal layer or core runtime. |

The formal application enum confirms only `DagL0`, `DagL1`, `CurrencyL0`, and
`CurrencyL1` (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/app/Layer.scala:3-7`).

```text
Native path:       client -> GL1 -> GL0

Metagraph path:    client -> CL1 --+
                                  +-> ML0 -> GL0 admission -> execution shard -> GL0 snapshot
                   client -> DL1 --+

Canonical return:  Phase-2 GL0 state -> GL1, ML0, CL1, DL1
```

Execution shards are internal GL0 infrastructure. They do not replace ML0 and do
not change either application path.

For economic operation, `numShards=1` means one execution shard using the same
replay-sign-diff-watchtower protocol as K shards. It is not an off switch. Current
`numShards > 1` gates are implementation gaps; an explicit unsafe development
profile may bypass sharding only if it cannot be confused with economic mode.

V1 shard count is immutable within a protocol era. Changing it remaps every
metagraph and is not a normal HOCON update. Future live resharding requires a
separate hash-bound era transition with queue drain, deterministic state and
checkpoint handoff, replay/nullifier continuity, committee overlap/activation,
rollback, and recovery rules.

## 2. Consensus boundaries

### 2.1 GL0

GL0 uses only chain-based Nakamoto/Taktikos/LDD production and fork choice. The
supervised active engine is `SnapshotLeaderLoop`
(`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala:81-92`).
Inherited inactive global round/event construction is still allocated and exposes
handlers/routes (`GlobalSnapshotConsensus.scala:847-890,1981-2128,2390-2397`). It
must be deleted so configuration or refactoring cannot reactivate a second global
consensus engine. ML0 retains its own existing consensus implementation.

Avalanche/Snowball is the optimistic Phase-2 finality mechanism. Nakamoto depth
`k1` is its fallback. Phase 2 remains reversible under Genesis-style
`maxvalid-bg` density selection. `k2` is a retention and recovery horizon, not a
fork-choice floor and not a second finality phase. Avalanche does not validate
economics or change fork choice.

The base chain admits only a fully authenticated and locally executed candidate:
network/genesis/era/parameters, exact parent and ordinal successor, slot and LDD
leader VRF eligibility, KES period/signature, eta derivation, body availability,
state transition, and state root all verify before storage can influence fork
choice or attestation. maxvalid-tk/maxvalid-bg compare valid chains only. The
Taktikos/LDD proof packet must cover equivocation, withholding/selfish production,
slot/clock bounds, eta grinding, wrong-root producers, and branch-sensitive stake;
Avalanche cannot repair an invalid base-chain transition.

### 2.2 ML0

ML0 may retain its existing consensus for a small, well-connected metagraph
validator set.
Its result authenticates the metagraph binary. ML0 finality is subordinate to GL0:
it cannot make framework value globally usable, select a GL0 fork, or change GL0
phase.

### 2.3 Execution shard

An execution shard is a chain-based checkpoint protocol over eligible GL0
operators:

- public deterministic VK-hash committee membership anchored to GL0 state;
- deterministic shuffled staircase producer duty;
- hash-linked checkpoint ancestry and maxvalid-tk sibling selection; checkpoint
  depth never substitutes for replay-backed execution quorum;
- execution signatures emitted only after local replay;
- hard anchor inherited from the containing GL0 snapshot at Phase 2.

Execution signatures certify reproduced computation and nothing else.

### 2.4 GL1

GL1 is the native DAG-token edge and submits blocks directly to GL0. Its local
block-formation consensus may remain the existing subordinate mechanism; it does
not set GL0 phase or make native value canonical. Every GL0 validator executes the
native block transition against the exact global proposal parent before inclusion.
Changing GL1's local consensus is separate product work and cannot introduce a
second GL0 consensus engine.

## 3. Non-negotiable authority rules

1. **Replay before state-validity signature.** A producer, execution-committee
   signer, watchtower fraud claimant, global optimistic attester, ML0
   state-validity signer, or GL1 state-validity signer must possess the exact
   inputs/base and locally run that layer's complete deterministic
   validation/reproduction before it signs or claims the result.
2. **Committee execution, noncommittee diff verification.** Producer and every
   execution signer replay CL1. Ordinary GL0 adopters verify the execution
   threshold, apply the canonical pinned-base diff, and recompute its root without
   rerunning currency recreation.
3. **Authority flows from GL0 downstream.** ML0-carried `authoritative*` state,
   `AdoptFromSignedFields`, direct claimed-root installation, and decoder-selected
   economic authority are forbidden. A protocol-defined GL0 correction is a
   separate globally validated transition and may repair a metagraph mirror; it
   cannot be originated or authorized by ML0/CL1/DL1.
4. **No blind-sign API.** A state attestation accepts a typed locally verified
   capability, not an arbitrary hash.
5. **Custom data is isolated.** Custom application output cannot construct,
   authorize, or overwrite framework economic state. A separate independently
   signed framework fee/intent may bind the exact custom commitment; mismatch
   rejects the bound intent rather than giving custom code economic authority.
6. **Global cross-metagraph coordination remains universal.** Every GL0 node runs
   the small deterministic global settlement/nullifier kernel over
   committee-extracted signed framework intents. This does not mean every GL0 node
   recreates every ML0 currency snapshot.
7. **Downstream is adopt/resync, not re-authority.** ML0/CL1/DL1/GL1 verify and
   follow exact canonical Phase-2 GL0 references. They do not override returned
   canonical state.

### 3.1 Economic enforcement map

| Operation/state class | Reproduction/validation before GL0 branch adoption | Ordinary GL0 work | Becomes downstream-usable |
|---|---|---|---|
| Native DAG transfer and fee from GL1 | Every GL0 node executes native block acceptance against the exact proposal parent. | Same execution result is included in the global MPT. | Exact containing GL0 P2. |
| CL1 local transfer, fee, balance, transaction/reference update | Producer and every execution signer run the framework kernel; assigned watchtowers replay. | Verify replay quorum; apply scoped canonical diff; recompute complete per-MG root. | Exact containing GL0 P2, subject to P2 rollback. |
| Allow-spend creation/reservation | Same committee replay and diff/root contract; owner signature/domain/amount/expiry validated by the framework kernel. | Apply per-MG authorization diff. If it is later used cross-MG, every GL0 global kernel verifies the exact P2 origin. | Creation at containing P2; cross-MG origin only from that exact canonical P2 ref. |
| Spend/allow-spend consume | Committee replay extracts the signed consume intent; local-only effects remain scoped. | Every GL0 node runs global conflict/order/nullifier/settlement for cross-MG consumes; one winner, one atomic nullifier/effect. | Containing P2 after required positive watchtower coverage. |
| Token lock/unlock | Committee framework replay validates owner/framework authority and exact arithmetic; unlock without authority rejects. | Apply scoped diff; any global/stake consequence runs through the global kernel. | Containing P2; additional interaction types stay disabled until their cross-MG state machine exists. |
| Rewards, mint/burn, supply changes | Committee may propose framework-derived output, but only explicitly registered bounded protocol rules are valid. | Every GL0 node verifies global reward/mint/burn/slash authority and conservation before canonical writes. | Containing P2 under its reorg risk; external consumers choose their own additional depth. |
| Stake/collateral/slash | Exact signed/evidence inputs and the global deterministic debit kernel. | Every GL0 node verifies evidence/authorization, debits real bonded principal once, and schedules anchored roster effect. | Canonical P2 state; eligibility changes at ratified boundary. |
| Settlement delivery acknowledgement | Committee replay validates ML0's hash-bound contiguous cursor against GL0 pending delivery. | Every GL0 node verifies acknowledgement changes metadata only; effective balances/nullifier remain unchanged. | Metadata at containing P2. |
| DL1 custom bytes | No framework execution claim. ML0 source authentication plus DA commitment only. | Verify inclusion/availability commitment; no economic write surface. | Custom availability at containing P2; semantic truth remains the metagraph's concern. |
| Canonical return to GL1/ML0/CL1/DL1 | No new execution signature or economic transition. | Downstream verifies exact P2 ref and adopts/resyncs. | Immediately under the explicit P2 reorg contract. |

For every CL1-derived row, “containing P2” is necessary but not sufficient until
the watchtower release decision is ratified. Under the conservative default, no
checkpoint-derived economic leaf or derivative capability is usable before
required positive replay coverage. This includes local transfers/fees/stake/reward
weight, not only withdrawal and cross-metagraph use.

Current code does not match all rows: it universally recreates checkpoint currency
on ordinary GL0 adoption, lacks the target diff, and disables the shard path at
`numShards=1`. The table is the target enforcement contract, not a current safety
claim.

## 4. Hash-bound GL0 finality gadget

### 4.1 Consensus phases and storage maturity

| State | Meaning | Allowed capabilities | Replacement rule |
|---|---|---|---|
| P0 `Pending` | Fully authenticated and executed branch candidate stored with exact parent and state. | Branch overlay, gossip, and valid-chain comparison. | Freely orphanable by chain selection. |
| P1 `Provisional` | Candidate is on the current `maxvalid-tk` canonical tine. | Optimistic sampling/attestation and speculative descendant construction. | Shallow chain selection may replace it. |
| P2 `Operational` | Exact canonical hash crossed `T_optimistic` or `T_depth1`. | Public operational serving, follower adoption, metagraph `globalSyncView`, shard execution base, cross-metagraph origin reference, and reversible spendability. | Remains replaceable by `maxvalid-bg` density selection. |
| `RetentionMature` | Exact canonical hash is beyond the node's configured local rollback/proof horizon (recommended `k2`) and every longer challenge, DA, acknowledgement, and dependency horizon. | Local compaction/archive policy only. It grants no extra consensus or economic authority. | A denser valid chain still wins; missing deep rollback data triggers verified recovery before mutation. |

Only P0, P1, and P2 are consensus phases. `k2` is deliberately not a phase or an
absolute common-prefix floor. Forks diverging deeper than `k1` are compared with
Genesis-style `maxvalid-bg`, including divergence deeper than `k2`. A node whose
local retention is insufficient must stop production, fetch and authenticate the
required ancestry/state, reproduce the objective comparison, and then unwind and
refold. Operator action may start recovery but may not select the winning branch.

P2 deliberately trades very low reorg probability for usable latency. All
in-protocol P2 consumers therefore implement the rollback contract. External
services choose their own confirmation depth and risk policy; the protocol does
not invent an irreversible threshold for them.

### 4.2 Phase-2 trigger algebra

```text
P0 -> P1: exact hash enters the maxvalid-tk canonical tine

P1 -> P2: T_optimistic(hash) OR T_depth1(hash)

T_optimistic:
  authenticated uniform samples update per-hash Snowball confidence
  -> alpha support persists for beta
  -> for one (network, genesis, era, ordinal, decision epoch), the node emits at
     most one decided attestation for a locally authenticated and executed exact
     snapshot hash
  -> distinct decided-attestation T_weight for that exact canonical hash is met

T_depth1:
  exact hash is k1 deep on the current canonical tine
```

`T_count` is removed/subsumed. Neither trigger changes fork choice or prevents a
later density replacement. “First wins” means the first valid trigger makes the
current exact canonical hash operational; it does not pin that ordinal forever.
An attester never signs a sampled peer hash merely because peers preferred it: it
first authenticates and executes the exact snapshot locally.

### 4.3 `FinalityGate` target

The current `FinalityGate` exposes only `finalizedOrdinal` and
`isServable(ordinal)`, while `fromRef` performs only an ordinal comparison
(`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/snapshot/finality/FinalityGate.scala:23-30,44-50`).
That is an ordinal watermark and cannot represent same-ordinal hash replacement.

The target is one durable exact-ref/branch-aware state machine:

```scala
trait FinalityGate[F[_]] {
  def submit(command: FinalityCommand): F[FinalityTransition]
  def statusOf(ref: GlobalSnapshotStateRef): F[SnapshotStatus]
  def canonicalPhase2: F[Option[GlobalSnapshotStateRef]]
  def requireCanonicalAtLeast(ref: GlobalSnapshotStateRef, phase: SnapshotPhase): F[Unit]
  def retentionStatus(ref: GlobalSnapshotStateRef): F[RetentionStatus]
  def events: Stream[F, FinalityEvent]
}

sealed trait FinalityEvent
final case class Advanced(ref: GlobalSnapshotStateRef, phase: SnapshotPhase) extends FinalityEvent
final case class Phase2Reorg(oldHead: GlobalSnapshotStateRef,
                             newHead: GlobalSnapshotStateRef,
                             commonAncestor: GlobalSnapshotStateRef) extends FinalityEvent
final case class RecoveryRequired(commonAncestor: Option[GlobalSnapshotStateRef],
                                  competingTip: GlobalSnapshotStateRef) extends FinalityEvent

sealed trait SnapshotStatus
final case class Canonical(phase: SnapshotPhase) extends SnapshotStatus
final case class Orphaned(was: SnapshotPhase,
                          replacement: Option[GlobalSnapshotStateRef]) extends SnapshotStatus

sealed trait FinalityCommand
final case class ObserveExecutedCandidate(candidate: AuthenticatedExecutedSnapshot)
    extends FinalityCommand
final case class SubmitOptimisticEvidence(evidence: OptimisticEvidence)
    extends FinalityCommand
final case class SubmitDepthEvidence(evidence: DepthEvidence) extends FinalityCommand
final case class SubmitDensityDecision(decision: VerifiedDensityDecision)
    extends FinalityCommand
```

The pure transition kernel owns canonical tip, the Phase-2 boundary, branch
evidence, orphan status, and retention metadata. The coordinator commits the
branch/MPT switch, rollback journal, phase transition, checkpoint anchor changes,
and downstream outbox atomically. Neither phase nor retention is inferred by
ordinal comparison across different hashes. `SettledOrdinalTracker` must be
renamed/replaced by an explicitly non-authoritative retention tracker and cannot
reject a valid density winner.

`submit` is the only state-changing consensus surface. HTTP and peer routes get a
read-only facade. Evidence validation is separate from the crash-consistent
coordinator that applies the resulting transition.

### 4.4 Capabilities governed by the gadget

`FinalityGate` is consulted by consensus and state transitions, not only HTTP:

- exact operational serving and same-ordinal replacement;
- metagraph `globalSyncView` validation;
- shard execution-base and committee-epoch derivation;
- checkpoint hard-anchor advancement;
- binary confirmation, requeue, and retention;
- cross-metagraph origin eligibility;
- follower adoption, rollback, and rebase;
- MPT fold/undo retention and verified deep recovery;
- operational APIs and downstream event delivery.

Inbound state-channel bytes may be staged while the GL0 head is ahead of P2.
Before execution, signing, or inclusion, every referenced base must resolve to an
exact canonical P2 hash/root. Staging is not execution or economic acceptance.

### 4.5 Portable operational and tower evidence

A permissionless downstream consumer cannot treat one node's boolean response as
proof. Historical P2 qualification has a domain-separated union:

```text
OperationalEvidence =
  OptimisticEvidence(
    exactSnapshotRef,
    sampleRegistryRef,
    distinct emit-once Snowball-decided attestations,
    T_weight result
  )
  | DepthEvidence(
      exactSnapshotRef,
      authenticated header/snapshot suffix,
      k1 and parameter hash
    )
```

The evidence proves why a hash crossed P2; current operational use additionally
requires proof that the hash is still on the density-selected chain. A later reorg
does not invalidate the historical signatures but removes their current-use
authority.

The NiPoPoW tower is the compact authenticated chain-comparison mechanism. Tower
eligibility is computed from branch-carried snapshot data, included in the signed
snapshot commitment, and independently reproduced before a GL0 node signs or
accepts the snapshot. The historical SMT root is consensus-load-bearing: artifact
comparison, follow, recovery, and proof verification all check the exact root.
Eligibility for epoch `N` uses the pinned stake distribution from `N-2`, eta from
`N-1`, and the exact registered KES/VRF identities for that history.

A peer supplying a tower proof can withhold freshness, but cannot forge a better
chain against the verifier's trusted genesis or cached authenticated commitment.
The verifier rejects an arbitrary root, an empty/vacuous proof, omitted SMT paths,
bad parent/tower pointers, wrong N-2 stake or N-1 eta, invalid KES/VRF signatures,
and proofs whose claimed tip/state does not match their commitment. Tower and SMT
stores are durable branch indexes, not local append-only truth; restart and density
reorg must reconstruct exactly the same canonical tower or halt before serving.
`k2` influences how much material is retained locally, never which valid tower or
chain wins.

## 5. Metagraph binary lifecycle

```text
Constructed at ML0
  -> ML0-consensus-authenticated
  -> intake-validated and durably stored by selected GL0 admission members
  -> admission-receipt threshold reached
  -> assigned to deterministic shard
  -> durably buffered in per-MG parent order
  -> included in an execution checkpoint window
  -> execution-certified and watchtower-covered checkpoint
  -> tentatively embedded in GL0
  -> operational when the exact containing GL0 hash reaches Phase 2
  -> downstream delivered/acknowledged
  -> locally prune-eligible only after every configured retention horizon closes
```

ML0 operators sign the binary under ML0 rules. Admission committee members are
GL0 operators selected separately for `(eta, metagraph, parentHash)`. Admission
members first validate the binary's ML0 source signatures against the exact
registry/allowlist epoch, network/genesis/lane/schema/resource bounds, authenticated
parent hash, and `incomingOrdinal = parentOrdinal + 1`; then they durably store the
exact bytes. Only that capability can produce a domain-separated admission/custody
receipt. A receipt means authenticated bounded intake and retrievability. It does
not claim CL1 execution, state validity, or a root, and can never count toward the
execution threshold.

Admission selection and receipt validation bind eta, metagraph, authenticated
parent hash, binary hash, metagraph ordinal, registry epoch, and custody horizon.
Initially the operator registry may be a network-configured per-metagraph
allowlist if every GL0 node derives identical bytes; a mutable node-local peerlist
is never consensus input. The future form is an exact delayed GL0 registry root.
The intake threshold, timeout/censorship fallback, and queue limits are protocol
parameters. A producer-supplied self-claimed ordinal or eta cannot select the
admission set.

A metagraph binary binds its exact ML0 parent and exact Phase-2 `globalSyncView`.
Ordinal-only or best-tip references are invalid. Missing parents/base defer; they
do not cause fallback to local live state.

The referenced P2 snapshot need not equal the latest P2 head; it may be an exact
still-canonical operational ancestor within the canonical protocol staleness and
retention bounds. `requireCanonicalAtLeast(ref,P2)` checks hash ancestry/phase, not
`ref == head`. If density later orphans that ancestor, every dependent binary and
checkpoint invalidates/rebases.

Within each metagraph segment, signed `globalSyncView` references are exact and
nondecreasing. Each binary's global reads execute against its signed historical P2
reference while the metagraph-local framework state threads from the segment's
signed pre-root through every binary in order. The checkpoint binds the complete
ordered input list and every reference. Epoch, expiry, authorization, and
cross-metagraph reads never use the receiver's live head.

## 6. Payload lanes

The v1 product target is:

| Lane | Framework treatment | Custom-data treatment |
|---|---|---|
| `FrameworkCurrency` | Decode strict known schema; execution committee replays; diff/root are certified. | None. |
| `FrameworkCurrencyWithData` | Same framework replay and diff/root rules. An independently signed framework fee/intent may explicitly bind the custom-data commitment; a mismatch rejects atomically. | Exact bytes or a content-addressed DA commitment are carried separately. Custom application output cannot synthesize, authorize, or overwrite framework economics. |

The lane tag, schema version, network/genesis, protocol era, metagraph registration,
and length/content commitment are signed. Decoder success is not a lane selector.
Unknown tags, trailing bytes, ambiguous encodings, and currency-shaped custom data
reject or stay custom according to the signed lane; they never gain authority.

Standalone `OpaqueStateChannel` is disabled by the proposed v1 scope unless the
owner explicitly retains pure data-only metagraphs. If retained, it provides only
authenticated ordering/availability and no economic state, genesis balance, or
upgrade path that imports claimed framework state.

## 7. Execution checkpoint lifecycle

Execution validity and chain position are separate axes.

### 7.1 Execution status

```text
UnexecutedClaim
  -> ProducerExecuted
  -> SignerLocallyVerified (per signer)
  -> ExecutionCertified (distinct kQuorum)
  -> ReplayCovered (required positive assigned-watchtower coverage)
  -> Challenged/Invalid, if objective watchtower evidence succeeds
```

### 7.2 Chain/global status

```text
ShardCandidate
  -> ShardPreferred by deterministic parent/duty selection
  -> ExecutionCertified
  -> ReplayCovered / GL0Eligible
  -> EmbeddedTentative in a GL0 candidate
  -> Operational when containing GL0 hash reaches P2
  -> RetentionMature when k2 and every longer retention horizon close

Any canonical status -> Orphaned/Requeued on shard or GL0 reorg
```

A checkpoint without the mandatory execution certificate is never diff-adoptable.
Shard age or descendants cannot substitute for one missing replay signature. A
fully execution-certified checkpoint that is not embedded in a P2 GL0 snapshot is
not globally operational. Positive assigned-watchtower replay coverage is required
before GL0 inclusion; a waiting checkpoint does not stall native GL0 work or other
shards.

### 7.3 Producer output

The scheduled staircase producer pins the required exact Phase-2 references and
deterministically cuts one fair, bounded checkpoint containing one or more
metagraph segments. Each segment contains one or more parent-contiguous
binaries. One checkpoint may therefore contain `MG-A=[100,101,102]`,
`MG-B=[1002]`, and `MG-C=[3,4,5]`. It emits:

- network, genesis, protocol era and parameter hash;
- shard, execution epoch/roster, parent, ordinal, slot/duty;
- exact Phase-2 refs `(ordinal,hash,parentHash,mptRoot)` used by the ordered binaries;
- exact ordered signed ML0 binaries required for framework replay, plus
  availability-bound content hashes/chunks for isolated custom payload bytes;
- for every touched MG, signed pre-root/version, complete ordered input segment,
  canonical byte diff, and complete post-root/version;
- extracted cross-metagraph framework intents;
- separate custom-data commitments;
- producer KES/VRF proof and signature.

V1 allows exactly one outstanding checkpoint per shard. A producer does not build
a child checkpoint until the current checkpoint's exact containing GL0 hash reaches
P2 or that checkpoint is explicitly orphaned/requeued. Batching occurs only inside
that checkpoint. There is no configurable checkpoint pipeline and no shard-depth
finality or inclusion qualifier. Parent/hash linkage and staircase sibling
selection still prevent ambiguous competing checkpoint order.

### 7.4 Signer behavior

Each execution signer independently resolves the same base/inputs and calls the
same deterministic framework kernel:

```text
exact diff and root match -> sign exact checkpoint once
input/base unavailable    -> defer; do not sign; do not slash producer
mismatch                  -> reject and retain objective comparison evidence
```

Retroactive ancestor signing obeys the identical rule. Replay results may be
cached by checkpoint hash after durable verification, but best-tip status cannot
construct the verified capability.

### 7.5 Ordinary adoption

Every noncommittee GL0 adopter verifies:

- producer eligibility, anchored committee roster, VRF/KES/signatures;
- distinct mandatory execution threshold;
- the separately domain-separated `PositiveReplayCoverage` certificate: its exact
  checkpoint hash, assignment anchor/roster, distinct assigned noncommittee
  signers, signatures, and protocol minimum threshold;
- shard ancestry and per-MG parent continuity;
- exact Phase-2 base and input availability;
- canonical sorted diff, no duplicate key, strict size/resource bounds;
- metagraph/framework key namespace and root coverage;
- for every touched MG, signed `preRoot` and version equal the proposal parent's
  current `Ml0FrameworkMirror` root/version, or an authenticated unchanged-version
  proof establishes equivalence;
- apply-diff result equals every signed post-root;
- extracted global framework intents are canonical and conflict-checked.

It does not rerun the ML0 currency snapshot creator. Any failure rejects the
checkpoint atomically.

The pre-root comparison is a compare-and-set invariant. A committee-valid diff
against an older Phase-2 base cannot overwrite an intervening per-MG mirror
transition. The separate `GlobalSettlementOverlay` does not waive this check.

### 7.6 Root coverage invariant

```text
every key writable by a checkpoint diff is committed by the verified root
```

The target complete framework root covers field 5, economic active allow-spends
field 7, and metagraph framework partitions 25-31. The current
`currencySnapshotMgRoot` excludes field 7 and therefore does not satisfy this
invariant. Field 32, `globalSnapshotSyncView`, remains in the exact signed ML0
binary as an execution locator but is not a writable GL0 canonical field, diff key,
or second authority. Current coverage is visible at
(`GlobalStateConverter.scala:1183-1185,1326-1355`;
`GlobalStateKey.scala:308-325`). A signed but unrooted writable field is forbidden.

### 7.7 Watchtowers

Watchtowers are selected outside the execution committee and replay the complete
checkpoint. Valid evidence binds exact base, inputs, checkpoint, signer set,
locally reproduced diff/root, and mismatch. Unavailable history is not fraud. A
watchtower's assertion alone is not objective evidence: adjudication must be a
ratified deterministic step/fraud proof, succinct proof, or exceptional bounded
universal GL0 replay of the challenged exact inputs. Happy-path ordinary adoption
still does not replay.

Watchtower selection is a deterministic noncommittee complement/sample with a
protocol minimum coverage. Every assigned watchtower replays the exact retained
inputs/base before signing positive coverage or reporting a mismatch. Positive
coverage is required before GL0 inclusion, so no checkpoint-derived local
transfer, fee, lock/stake/collateral, reward weight, mint/burn, cross-MG action,
withdrawal, bridge, or acknowledgement capability can escape before the backstop.
Unrelated GL0 snapshots continue while coverage waits.

`PositiveReplayCoverage` is a distinct exact-checkpoint-hash-bound artifact. Its
signature domain cannot decode or count as execution, admission, custody,
optimistic finality, KES production, or fraud evidence. The signing API accepts
only a non-forgeable local watchtower replay capability that binds the checkpoint,
assignment anchor, exact base/input commitment, reproduced diff/intents/root, and
watchtower identity. Receipt, availability, or a naked checkpoint hash cannot
construct that capability.

An authenticated assigned-watchtower mismatch report quarantines the checkpoint
pending objective adjudication even if the positive threshold has already been
met. Positive count never overrides a known mismatch. The report alone does not
slash or select a replacement state; the bounded objective replay/proof below
decides whether the checkpoint or claimant is faulty.

A mismatch assertion is assigned, bonded, rate-limited, and resource-bounded. In
the first implementation, exceptional bounded universal GL0 replay of the exact
challenged inputs/base decides the result. Missing data defers and cannot slash.
The deterministic replay result, never the assertion or a timeout, decides
rollback and signer penalties. A verified step or succinct proof may replace this
only after an independent specification and resource-bound tests.

## 8. GL0 snapshot composition

A GL0 producer composes, in canonical order:

1. native GL1 DAG-token transitions;
2. continuing execution-certified and release-eligible shard checkpoints;
3. deterministic global cross-metagraph intents extracted from those checkpoints;
4. protocol rewards/slashes/configuration transitions that pass the global kernel;
5. explicit GL0 protocol corrections active for this parent/era;
6. isolated custom-data commitments.

Native GL1/global coordination kernels are executed by every GL0 validator.
Per-MG framework state is adopted through execution-certified diff application.
These are complementary, not contradictory, uses of GL0 authority.

Tentative snapshot evaluation may stage a checkpoint, but cannot advance its hard
anchor or irreversibly prune its binaries. Phase-2 transition of the exact
containing GL0 hash makes it operational. `k2` plus every longer challenge,
availability, acknowledgement, and recovery horizon permits local pruning but does
not prevent a later density winner.

### 8.1 GL0 protocol correction lifecycle

GL0 retains protocol-level authority to repair a malformed metagraph framework
mirror. That authority is not a metagraph payload feature and cannot flow upward
from an ML0 signature, admission receipt, execution checkpoint, custom-data lane,
or local operator configuration.

```text
Declared by active GL0 protocol era
  -> Pending exact activation parent/hash
  -> Validated against target MG pre-root/version
  -> Deterministically executed by every GL0 snapshot validator
  -> Included in candidate GL0 state/root
  -> Operational when the exact containing GL0 hash reaches P2
  -> Reverted/reapplied if density selection replaces that branch
  -> Delivered downstream as a mandatory rebase input
```

A correction binds network/genesis/era, correction ID, target metagraph and
namespace, exact expected pre-root/version, canonical bounded write set or pure
transform, expected post-root/version, activation parent/hash, and reason code.
Every GL0 node independently verifies authorization under the active protocol
rule, executes the correction, checks conservation and the complete resulting
root, and records a permanent replay ID. A stale base, duplicate ID, wrong
namespace, locally configured correction, or metagraph-originated request rejects
atomically. ML0/CL1/DL1 consume the resulting P2 state and rebase; they do not
override or reinterpret the correction.

## 9. Cross-metagraph settlement

GL0 is the shared sequencer, authorization registry, nullifier store, settlement
kernel, and delivery outbox. Shards never directly settle with one another.

### 9.1 Canonical identity

Every signed framework intent binds network, genesis, protocol era, operation
type, source/owner metagraph, target domain, asset, amount/policy, nonce/sequence,
expiry, and exact authorization body. Identity excludes proof-container ordering.

```text
authorizationId = H(canonical signed body)
consumptionId   = H(canonical consume body)
replayKey       = type-specific one-shot ID or explicit sequence/nonce
```

### 9.2 State machine

```text
Authorization: Absent -> Active/Reserved -> Consumed/Settled
                                  +-------> Expired/Cancelled

Delivery:      Absent -> PendingDelivery -> Acknowledged
```

Consumption, permanent nullifier, balance/reservation deltas, and delivery append
are one atomic checked write set. Acknowledgement changes delivery metadata only;
it cannot create, duplicate, cancel, refund, or alter settled economics.

Canonical state separates two representations:

```text
Ml0FrameworkMirror
  = latest execution-certified per-MG framework state/diff adopted from ML0

GlobalSettlementOverlay
  = GL0-owned reservations, consumes, permanent nullifiers, pending balance
    adjustments, delivery sequence/cursor, expiry/cancel/refund status

EffectiveFrameworkState
  = exact checked composition(Ml0FrameworkMirror, GlobalSettlementOverlay)
```

The checkpoint's per-MG root commits its `Ml0FrameworkMirror` transition. The
universal GL0 kernel writes the separate settlement overlay; it does not silently
mutate the already-certified checkpoint root. All balance/authorization reads use
the effective composition. When ML0 later applies the inbox and acknowledges it,
GL0 may compact the mirrored/overlay representation only if effective state before
and after is byte-identical and permanent replay/status evidence remains.

### 9.3 Global conflict rule

Per-MG diffs cannot pre-accept competing cross-metagraph consumes. Execution
committees extract and sign canonical framework intents; every GL0 node executes a
small deterministic global merge against the proposal parent plus Phase-2 origin
proofs. One total order selects at most one winner for a one-shot authorization.
Losing intents have no economic effect and are reported deterministically.

The merge writes `GlobalSettlementOverlay`, not the execution-certified per-MG
mirror diff. A subsequent ML0 snapshot must apply mandatory inbox adjustments
before local CL1 spends; its execution committee replays the same order and its
new mirror root can absorb acknowledged adjustments without changing effective
balances.

The exact order of consumes, cancel, expiry, refund, inbound delivery, and local
spend is an owner-blocking protocol decision. Wall clock, ML0 progress, peer order,
shard arrival order, and map iteration are invalid tie breakers.

### 9.4 Phase-2 use and rollback

Only an authorization in canonical Phase-2 GL0 state can be a cross-metagraph
origin. A settlement becomes operational when its containing GL0 snapshot reaches
Phase 2. If density later orphans that hash, GL0 unwinds the settlement/nullifier
and every dependent downstream state, requeues still-valid intents, and follows
the replacement branch. No Phase-2 effect may be represented to integrators as
irreversible.

### 9.5 Concrete cross-shard allow-spend trace

```text
1. Owner on MG-A signs allow-spend for MG-B.
2. CL1-A -> ML0-A; ML0-A finalizes/signs its binary.
3. GL0 admission routes the binary to shard(A).
4. shard(A) producer + every execution signer replay it; ordinary GL0 applies
   the certified MG-A diff and checks its root.
5. Containing GL0 snapshot reaches P2. The authorization is now a legal exact
   origin ref; before P2 it is unusable.
6. MG-B creates a consume that names authorizationId and that P2 ref.
7. CL1-B -> ML0-B -> shard(B). Its execution committee replays and extracts the
   signed consume intent; its per-MG diff cannot write MG-A/global nullifiers.
8. GL0 composes all shard checkpoints and globally orders competing consumes.
   Every GL0 node verifies the P2 origin/current status and atomically writes the
   one winning settlement, permanent nullifier, and delivery entries.
9. The containing GL0 snapshot reaches P2. ML0-A/ML0-B follow the exact ref,
   apply their ordered inbox entries, and later acknowledge contiguous progress.
10. If either P2 hash is density-orphaned, the dependent authorization,
    consume, nullifier, delivery, acknowledgements, and local spends unwind/rebase
    according to the exact replacement event.
```

At no point does shard(B) read a live shard(A) tip or accept a direct receipt as
authority. GL0's canonical P2 state is the rendezvous point.

## 10. Phase-2 reorg contract

A density-band switch produces an exact event:

```text
Phase2Reorg(oldOperationalHead, newOperationalHead, MRCA, orphanedRange, adoptedRange)
```

The switch atomically:

- unwinds canonical MPT changes to the true MRCA using retained exact undo data;
- installs/replays the denser branch;
- replaces hash-bound phase and retention views consistently;
- reverses checkpoint anchors and binary confirmations, including atomically replacing every per-shard producer/recovery watermark when the
  replacement checkpoint has the same or a lower shard ordinal;
- restores/requeues orphaned native and metagraph inputs exactly once;
- reverses pending delivery/nullifier effects from orphaned snapshots;
- recomputes eta/committee anchors by the same chain walk used by bootstrap;
- durably publishes the downstream replacement event.

GL1, ML0, CL1, and DL1 follow exact hashes and retain rollback material through
their configured local horizon and every longer dependency. `k2` is the
recommended capacity target, not a protocol-validity minimum; a node whose local
history is shorter enters authenticated recovery sooner. Downstream layers resync
on this event.
ML0 preserves an auditable append-only history. A metagraph with a registered,
deterministic retained rebase/undo contract appends the mandatory rebase. Otherwise
it starts a new ML0 epoch at the last valid GL0 reference. Noninvertible external
effects are outside the reversible P2 guarantee and choose their own risk delay.

If the true common ancestor is older than locally retained undo/input state, the
node enters `RecoveryRequired`, stops production and P2 serving, fetches exact
hash-addressed ancestry and state from archival peers, verifies the same
`maxvalid-bg` decision, and rebuilds before mutation. It never truncates the
density window, compares asymmetric local histories, treats `k2` as a fork-choice
floor, or lets manual recovery select a branch.

## 11. Canonical serialization and protocol eras

The serde migration is not complete today. MPT values use scodec-backed codecs,
but ordinary signing/hashing still routes through Circe/JSON/Kryo selectors, and
`EraCodecRegistry` is not the production authority.

Target rules:

1. Greenfield genesis uses only `ScodecV1` from new-chain ordinal 0.
2. Every signed, hashed, MPT key/value, checkpoint, diff, finality evidence,
   recovery record, and migration manifest has one bounded canonical codec.
3. Decoders consume all bytes and reject trailing bytes, unknown tags, duplicate
   or unordered collections, non-minimal encodings, invalid refinements, and
   over-limit sizes.
4. JSON remains an API/debug representation, never a consensus preimage.
5. Fork-only undeployed compatibility formats are deleted, not retained.
6. Future public upgrades use one exact canonical hash-bound `ProtocolEra`
   schedule with exact predecessor, activation, state transform, density-reorg,
   stale-node, and live-object rules.
7. Consensus-object hashing and frozen MPT-key derivation are separately versioned
   so a serde migration cannot silently move keys.

## 12. Existing-network snapshot genesis

The future public-network migration is deterministic re-genesis, not a reason to
keep legacy consensus codecs in the new runtime:

```text
finalized upstream-network snapshot
  -> isolated verified exporter
  -> explicit state transform and conservation audit
  -> canonical ScodecV1 migration manifest
  -> new-chain ordinal-0 genesis
```

The manifest binds source network/genesis, exact source ordinal/hash/root/finality
evidence, imported and discarded namespaces, balances/supply/locks/reservations,
live authorizations/nullifiers, validator/metagraph registrations, new parameters,
replay-domain separation, and resulting genesis root/hash.

Source-network signatures cannot silently become valid new-network intents. Each
live authorization/order is either expired/dropped/refunded by the declared
transform or imported as inert migration state with an explicit new-chain claim
procedure; rehashing the old signature under the new domain is not authorization.

At least two independent reproduction paths must produce identical genesis bytes
and roots. Old-network parsing lives in an offline migration tool. The new chain
starts in its new network/genesis domain and does not accept old runtime messages.

## 13. MPT-primary state and GSI removal

`GlobalSnapshotInfo` is not part of the target authority or recovery model. The
work is complete only when production code has:

- typed MPT readers for every state partition;
- exact durable branch/diff/input/finality/nullifier/outbox records;
- no GSI-derived consensus write, recovery fallback, peer-state installation, or
  API authority;
- verify-before-mutate live and recovery paths;
- hash-addressed authenticated fetch when exact bytes are missing;
- an explicit halt rather than local state synthesis.

GSI may be used temporarily as a migration-test oracle, then is deleted from
production sources. This is not complete today.

## 14. Ratified decisions and remaining parameter gates

The following architecture decisions are implementation inputs, not open choices:

1. P2 is `T_optimistic OR T_depth1`; `T_optimistic` is distinct decided-attestation
   `T_weight`, and `T_count` is removed/subsumed.
2. Forks deeper than `k1` use `maxvalid-bg`. `k2` is retention/recovery guidance
   only and cannot reject a denser valid chain.
3. Tower eligibility and proofs use branch-bound state, N-2 stake, N-1 eta, exact
   KES/VRF identities, and a consensus-load-bearing historical SMT root.
4. Distinct execution `kQuorum` is mandatory. Every producer and signer replays;
   shard depth cannot substitute for execution.
5. Positive deterministic noncommittee watchtower coverage is required before GL0
   inclusion. A challenge is decided by bounded objective replay, never assertion.
6. Admission/custody receipts are separate non-validity types and never satisfy an
   execution threshold.
7. Per-MG diffs are namespace-confined. Every GL0 node runs the deterministic
   global conflict/nullifier/settlement kernel over signed extracted intents.
8. Every framework/economic field, including active allow-spends, is in the
   complete root. `globalSnapshotSyncView` remains signed inside the ML0 binary but
   is not writable GL0 state.
9. ML0 retains append-only history and emits a deterministic registered rebase, or
   starts a new epoch at the last valid GL0 ref when deterministic rebase is not
   available.
10. Currency and currency-with-data preserve the v4.0.0 framework capabilities.
    Framework bytes are replayed; custom bytes are isolated DA carriage.
11. Checkpoints retain exact signed currency incrementals required for replay and
    bind custom payload commitments through the longest challenge/recovery horizon.
12. P2 is reversible operational state. External service providers choose any
    additional confirmation depth appropriate to their risk.
13. One outstanding checkpoint per shard batches multiple contiguous binaries for
    multiple metagraphs. There is no checkpoint pipeline or shard-depth release.
14. Every touched MG uses exact parent mirror `preRoot/version` compare-and-set.
15. Per-binary exact P2 refs are nondecreasing; historical global reads use the
    signed ref while local state threads through the ordered MG segment.
16. GL0 may apply explicit protocol-defined metagraph corrections. A metagraph
    cannot originate or authorize a correction of GL0 canonical state.

These numeric/resource choices still gate their dependent runtime merge, but do
not block pure models, codecs, RED tests, or interface work:

- exact K/alpha/beta, `T_weight`, registry snapshot, churn, and small-network mode;
- admission draw/threshold, receipt lifetime, custody horizon, and censorship
  fallback;
- execution/watchtower draw sizes, `kQuorum`, coverage minimum, deadlines, bonds,
  replay resource caps, and penalty schedule;
- checkpoint/input/diff/DA limits and the minimum required rollback/archive service
  for recovery beyond local `k2` retention;
- global settlement total order for consume/cancel/expiry/refund/inbox/local spend;
- exact active-era authorization and activation form for a GL0 protocol correction;
- whether a pure opaque/data-only non-economic lane ships in v1;
- future public-network migration policy, which is intentionally deferred.
