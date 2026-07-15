# P6-FIN14-A Exact Phase-2 Consumer Lease

**Status:** Owner direction ratified; design/RED packet with activation-blocking schema and dependency gates
**Runtime authority:** None. This packet does not wire `FinalityGate`, issue a live
lease, qualify a snapshot, or make state usable.
**Finding:** `FIN-14`
**Primary gate:** `FOLLOW-008`

## 1. Purpose and boundary

This packet defines the local capability and commit protocol by which a GL0
consumer may use one exact canonical Phase-2 state without combining finality from
one branch with state from another branch. It does not define fork choice,
Avalanche/Snowball, `k1`, the MPT semantic grammar, or downstream rollback. Those
are prerequisites.

The invariant is:

> A Phase-2-derived mutation is linearized only while its exact
> `(ordinal, hash, parentHash, mptRoot)`, qualification, authenticated snapshot,
> semantic state, MPT image, and canonical lineage are still the same facts that
> were verified when the work began. A stale worker can retain bytes, but it cannot
> retain authority.

`CanonicalPhase2Lease` is a local, non-serializable capability. It is not portable
finality evidence, a signature preimage, an admission receipt, or a substitute for
independent verification by another node.

The lease does not lock or pin a GL0 branch. It creates no proposal, vote, lock,
QC, view change, BFT commit, or new finality rail. Density fork choice may replace
the branch at any time; the lease protocol only prevents stale consumers from
using the replaced state.

## 2. Confirmed current defects

### 2.1 An ordinal watermark transfers A's qualification to B

The current `FinalityGate` exposes only `finalizedOrdinal` and
`isServable(ordinal)`; its GL0 implementation performs `ordinal <= watermark`
(`FinalityGate.scala:23-30,44-50`). Both live Phase-2 rails update that watermark
monotonically (`SnapshotLeaderLoop.scala:1246-1253,1350-1353`).

Metagraph admission reads that watermark and the independently mutable current
best tip, then accepts a requested hash when it appears on the best-tip walk
(`GlobalSnapshotConsensus.scala:1395-1410`).
`Phase2CurrencyBinaryContext.verify` turns the resulting Boolean into a private
case-class value that contains only the metagraph ordinal and GL0 ordinal/hash
(`MetagraphParentOrdinalResolver.scala:35-58`). It does not bind the snapshot
parent/root, qualification evidence, released-core generation, MPT/semantic/anchor
readback, or a branch revision.

Concrete failure:

1. Branch A's hash at ordinal N reaches Phase 2, setting the watermark to N.
2. Density selection replaces A with branch B at or before N.
3. B's hash at N is on the new best-tip walk, but has neither decided-attestation
   `T_weight` nor `k1` depth.
4. The split read returns true and mints `Phase2CurrencyBinaryContext` for B.
5. A B-anchored binary can enter committee processing under a qualification that
   belongs only to A.

This is `FIN-14`; it is not fixed by making the two reads occur close together.
B must independently qualify.

### 2.2 A valid check can become stale during the committee wait

Even after replacing the false qualification adapter, the current binary path has
a check/use race:

1. `resolveContext` verifies Phase 2 once
   (`NakamotoSyncDaemon.scala:2829-2845`).
2. The path writes pending context and metagraph continuity caches before the
   committee decision (`NakamotoSyncDaemon.scala:2890-2919`).
3. `MetagraphCommitteeGate.attestAndAdmit` signs if selected and then polls the
   tally for as long as the configured timeout
   (`MetagraphCommitteeGate.scala:299-322,485-523`).
4. After the wait returns true, the caller does not recheck canonicality before
   `processMetagraphBinary`, shard-buffer insertion, or child draining
   (`NakamotoSyncDaemon.scala:2937-2951`).

The receiver has the same smaller race: it checks the cached context, derives eta,
then records an attestation in a separately mutable tally
(`NakamotoSyncDaemon.scala:2189-2198`). The tally key contains metagraph, parent,
binary, and sender, but no exact Phase-2 reference or lineage generation
(`MetagraphCommitteeGate.scala:539-550`).

A density replacement during any of these gaps leaves stale cache/tally/admission
state able to affect later work. Holding a finality lock for the committee timeout
is not an acceptable repair: an absent threshold or network partition would block
urgent fork-choice replacement and finality progress.

### 2.3 Dark finality types do not yet provide live authority

`FinalityCore` has useful nonactivating commitments:

- `CanonicalBranchRevision` is explicitly only a local future CAS guard
  (`FinalityCore.scala:19-28`).
- `CanonicalSelectionToken` binds selected tip, operational target, opaque
  fork-choice evidence, and lineage (`FinalityCore.scala:120-138`).
- the two permitted qualification rails are decided-attestation `T_weight` and
  canonical depth `k1` (`FinalityCore.scala:165-231`).
- `PreparedCoreTarget` and `ReleasedCoreReceipt` bind MPT publication, semantic
  state, and authenticated-anchor artifacts (`FinalityCore.scala:314-343,366-376`).

They are not live `FinalityGate` authority. In particular, the fork-choice evidence
is intentionally opaque until O-15's ratified direction is implemented and proved
(`FinalityCore.scala:120-125`), and production code outside the finality package
does not consume `FinalityCoreBatch`, `CanonicalSelectionToken`, or `ReleasedCore`.
Durable restart validation currently proves that semantic/anchor artifact bytes are
present by reading their pointers; it does not interpret either receipt or compare
the referenced external state (`FinalityDurableStore.scala:1357-1369,1393-1404`).

### 2.4 The ordinal facade also remains on serving/follower paths

GL0 startup installs `FinalityGate.fromRef` directly
(`dag-l0/Main.scala:197`). `SnapshotRoutes` exposes its ordinal watermark and
Boolean serving decision (`SnapshotRoutes.scala:58-79`). The Nakamoto
`FinalizedSnapshotReader` then reads a finalized ordinal and separately selects
ordinal-keyed checkpoint/MPT files, or checks `isServable` before a later file read
(`FinalizedSnapshotReader.scala:137-147,200-227,230-255`). These operations have no
same-hash/revision transaction.

Downstream bootstrap has the separate confirmed `BR-01` split-fetch defect. The
consumer lease does not replace BOOT exact-bundle verification, but serving and
follower adoption are lease scopes because an exact gate followed by an unscoped
ordinal/file read would reproduce the same check/use class.

## 3. Required capability model

### 3.1 Separate exact evidence from local authority

Three values must not be conflated:

| Value | Meaning | May be transported? | May directly mutate a consumer? |
|---|---|---:|---:|
| `GlobalSnapshotStateRef` | Claimed structural identity | Yes | No |
| `OperationalEvidence` plus current-chain evidence | Portable reason the exact hash qualified and remains selected | Yes | No; verify first |
| `CanonicalPhase2Lease` | Package-minted local authority after all evidence and state readbacks passed | No | Only through `commitIfCurrent` |

`GlobalSnapshotStateRef` is intentionally only a claim; its source type does not
authenticate the snapshot or MPT root (`GlobalSnapshotStateRef.scala:7-17`). A
Boolean, `Option`, ordinal watermark, peer response, decoded evidence wrapper, or
public case-class constructor cannot create the lease.

### 3.2 Proposed opaque shape and exhaustive purpose registry

The following is interface pseudocode, not a request to add these public case
classes now:

```scala
sealed trait Phase2UseScope
object Phase2UseScope {
  final case class BinaryAdmission(mg: Address, parent: Hash, binary: Hash)
      extends Phase2UseScope
  final case class BinaryConfirmationRequeue(mg: Address, binary: Hash)
      extends Phase2UseScope
  final case class ShardExecutionBase(shard: ShardId, checkpoint: Hash)
      extends Phase2UseScope
  final case class CheckpointInclusion(shard: ShardId, checkpoint: Hash)
      extends Phase2UseScope
  final case class CheckpointAnchorAdvancement(shard: ShardId, checkpoint: Hash)
      extends Phase2UseScope
  final case class AssignedWatchtowerReplay(shard: ShardId, checkpoint: Hash)
      extends Phase2UseScope
  final case class ChallengeAdjudication(challenge: Hash)
      extends Phase2UseScope
  final case class CrossMetagraphSettlement(operationId: Hash)
      extends Phase2UseScope
  final case class HistoricalEconomicRead(operationId: Hash)
      extends Phase2UseScope
  final case class OptimisticSamplerRegistryContext(round: Hash)
      extends Phase2UseScope
  final case class TowerEligibilityRegistryContext(trial: Hash)
      extends Phase2UseScope
  final case class TowerProofServing(requestId: Hash) extends Phase2UseScope
  final case class ProtocolCorrection(correctionId: Hash) extends Phase2UseScope
  final case class FollowerAdoption(layer: Layer) extends Phase2UseScope
  final case class ExactServing(requestId: Hash) extends Phase2UseScope
  final case class BootstrapBundleServing(requestId: Hash) extends Phase2UseScope
  final case class RetentionRecovery(operationId: Hash) extends Phase2UseScope
  final case class DownstreamEventDelivery(eventId: Hash) extends Phase2UseScope
}

final class CanonicalPhase2Lease private[finality] (
  val scope: Phase2UseScope,
  val target: GlobalSnapshotStateRef,
  val released: ReleasedCorePointer,
  val releaseGeneration: ReleaseGeneration,
  val lineageRevision: CanonicalLineageRevision,
  val observedSelectionRevision: CanonicalBranchRevision,
  val qualification: OperationalQualificationScope,
  val decisionEvidence: ImmutableArtifactPointer,
  val lineage: PathCommitment,
  val publication: MptActivePublication,
  val semanticReceipt: ScopedArtifactRef,
  val authenticatedAnchorReceipt: ScopedArtifactRef
)

sealed trait Phase2CommitResult[+A]
object Phase2CommitResult {
  final case class Committed[A](value: A) extends Phase2CommitResult[A]
  final case class Stale(reason: StaleLeaseReason) extends Phase2CommitResult[Nothing]
  final case class RecoveryRequired(reason: RecoveryReason)
      extends Phase2CommitResult[Nothing]
}
```

Required construction properties:

- The lease constructor and every current-use permit constructor are inaccessible
  outside the finality-owned package/runtime.
- The lease and permit have no Circe, Kryo, Scodec, protobuf, or Java serialization
  instance. Durable derivatives store their full exact scope, not a reconstructed
  lease object or opaque local lease ID.
- The scope is part of the lease. A binary-admission lease cannot be reused for an
  economic read, execution signature, checkpoint inclusion, follower adoption, or
  serving.
- The sealed registry is exhaustive for every authority-bearing consumer of exact
  Phase-2 state. There is no `Generic`, `Other`, string, or caller-selected purpose.
  Admission, binary confirmation/requeue, execution, checkpoint inclusion/anchor,
  assigned-watchtower replay, challenge adjudication, cross-metagraph settlement,
  optimistic-sampler registry context, tower-eligibility registry context/proof
  service, protocol correction, follower adoption, operational/bootstrap serving,
  retention/recovery, and event delivery
  have distinct cases and distinct ancestor/current-head and retention policies.
  `HistoricalEconomicRead` cannot authorize any of those effects. A new consumer
  is invalid until this registry, its policy, invalidation inventory, and RED tests
  are updated together.
- The finality coordinator's own branch selection, target qualification, core
  release, MPT fold/undo, and replacement transition are not lease consumers: they
  are the kernel that creates or invalidates lease authority. In particular, an
  optimistic-attestation target cannot use a Phase-2 lease to prove its own Phase-2
  qualification. Only its exact already-P2 historical registry/parameter context
  may use `OptimisticSamplerRegistryContext`.
- The lease carries the exact complete reference. Current `GlobalSyncView` has only
  ordinal, hash, and epoch progress (`globalSnapshotSync.scala:38-42`); the target
  signed schema and resolver must bind or resolve and verify the complete
  `GlobalSnapshotStateRef` before lease issuance.
- No method returns `Boolean` as authority. Typed failure distinguishes not
  canonical, not operational, missing evidence, unavailable image, stale
  generation, consumer conflict, and `RecoveryRequired`.

### 3.3 Lease acquisition

`acquireCanonicalPhase2(scope, requestedRef)` is a two-stage acquisition with two
short coordinator operations around immutable verification:

1. Under the coordinator serialization boundary, capture one immutable acquisition
   descriptor containing coordinator mode, both revisions, selected lineage and P2
   target, exact release generation, qualification/current-chain evidence pointers,
   readback identities, purpose policy version, and consumer sink revision. Release
   the boundary without minting a lease.
2. Outside every finality, MPT, chain-store, and consumer lock, load and verify the
   descriptor's content-addressed snapshot, lineage, evidence, MPT, semantic, and
   anchor artifacts. Peer fetch may supply missing bytes but cannot change any
   pointer or identity in the descriptor.
3. Re-enter the coordinator boundary briefly and compare-and-set the complete
   descriptor against current mode, revisions, lineage, target status, release and
   readback identities, purpose policy, and sink revision. Only an exact match may
   mint the lease. Any change returns stale/retry or `RecoveryRequired`.

The immutable verification in stage 2 must establish all of the following:

1. Coordinator mode is `Running`, not `RecoveryRequired`.
2. The exact target snapshot is authenticated and locally executed under its
   network, genesis, protocol era, parameter hash, parent, ordinal, slot, VRF/KES,
   body, state transition, and root rules.
3. The target is P2 on the currently selected canonical lineage. Ordinal equality
   is irrelevant.
4. One exact permitted qualification verifies: decided-attestation `T_weight` or
   canonical `k1` depth. A replacement hash independently qualifies; it cannot
   inherit the old hash's release generation or evidence.
5. The objective current-chain decision and complete lineage evidence verify under
   O-15. Historical qualification alone is insufficient after a reorg.
6. The released-core record and generation name the target and remain on the
   current immutable canonical release lineage. They need not equal the latest
   released head: a newer descendant release may preserve the older target. An
   audit record retained only as orphan history is not sufficient.
7. Authenticated snapshot-anchor, semantic-state, and MPT publication readbacks all
   name the same target and reproduce its complete `mptRoot`.
8. The complete active-era ROOT structural grammar and every applicable O-07/ECON-G
   economic authorization, conservation, backing, replay, and transition verifier
   accept the image/use. A self-consistent MPT manifest is insufficient.
9. The purpose-specific rule permits this exact canonical ancestor. Any protocol
   age/staleness rule is branch-authenticated consensus data, not local HOCON or
   wall clock.

No coordinator/finality lock spans stage-2 image verification or subsequent
network, replay, DA fetch, committee polling, or signature collection. Long work
runs against immutable, hash-addressed inputs selected by the lease. This final
acquisition CAS is the required answer to `FOLLOW-008C`; a single locked read before
expensive readbacks is insufficient.

## 4. Revision semantics

### 4.1 Two revisions have different jobs

One counter cannot preserve useful ancestor work across extension while also
detecting every replacement race. The finality coordinator therefore needs two
persisted monotone values:

| Revision | Advances when | Use |
|---|---|---|
| `CanonicalBranchRevision` | Every canonical selection mutation, including a pure descendant extension | Detect that the observed head changed and force a current-state check |
| `CanonicalLineageRevision` | Any rollback, sibling/deep replacement, recovery reconstruction, or other mutation that removes/substitutes a previously selected canonical hash | Invalidate every permit derived from the old lineage generation |

Neither revision is consensus evidence or transported as proof. Both are local
crash-consistent CAS inputs derived from the same verified finality transition.
They never reset on restart and cannot be restored from a peer claim.

### 4.2 Descendant extension

For a pure extension in which every previously selected hash remains canonical:

- `CanonicalBranchRevision` advances.
- `CanonicalLineageRevision` does not advance.
- A scope that permits an exact historical P2 ancestor may commit after
  `commitIfCurrent` proves the target remains canonical P2, the release/readbacks
  are unchanged, the target's released record remains on the canonical release
  lineage, and purpose-specific staleness still passes. Equality with the latest
  released pointer is not required.
- A scope requiring the latest operational head must fail stale and reacquire when
  the P2 head changes.

This avoids restarting a 30-second committee wait merely because a descendant was
added, without letting a consumer skip the final check.

### 4.3 Replacement, rollback, and ABA

For every replacement or rollback, including a replacement whose MRCA is above the
lease target:

- `CanonicalLineageRevision` advances before replacement effects become visible.
- every lease and current-use permit from the prior revision fails closed;
- no old cache, tally, threshold result, queue entry, checkpoint window, replay
  result, or signature capability can be committed under the new revision; and
- a still-canonical surviving ancestor may be reacquired under the new revision
  after its exact status/evidence/readbacks are checked again.

This conservative V1 rule deliberately invalidates more work than selective
orphan-only invalidation. It makes `A -> B -> A` safe: the returned A hash does not
revive a lease minted before B because the lineage revision is monotone.

If replacement evidence or required history is unavailable, the node enters
`RecoveryRequired`; it does not keep accepting under the last known revision.

Replacement linearization never waits for lease holders. The coordinator first
commits the new lineage revision and disables issuance/current-use for the old
core, then completes the verified core restoration/adoption and ordered
invalidation effects. A lease for the replacement is mintable only after its
released-core MPT, semantic, and anchor readbacks succeed. Crash at any boundary
resumes that durable transition or enters recovery; it cannot reopen the old
revision.

## 5. `commitIfCurrent` protocol

### 5.1 Linearization rule

Every authority-bearing consumer mutation uses a final short operation:

```text
long work outside finality lock
  -> build closed, deterministic consumer command
  -> commitIfCurrent(lease, expectedConsumerRevision, command)
       acquire finality/coordinator serialization boundary
       re-read coordinator mode and both revisions
       revalidate exact target status and purpose
       prove the exact released record remains on the canonical release lineage
       and compare its exact readback identities
       compare consumer sink revision
       append/commit the scoped command or return Stale/RecoveryRequired
       release serialization boundary
  -> execute only the ordered idempotent sink command
```

`commitIfCurrent` accepts a closed command or a package-owned bounded sink adapter,
not an arbitrary callback that may poll peers, wait for signatures, fetch data,
replay a checkpoint, or perform unbounded disk work. A current-use permit cannot
escape the lexical operation or be stored for later.

### 5.2 Independent sink stores

The repository's ratified L-23 model coordinates existing stores with an
idempotent finality intent/outbox, not one database transaction. Therefore a
consumer command must carry:

- exact target, release generation, lineage revision, scope, and command identity;
- expected and desired consumer-state digests/revisions;
- an idempotency key derived from the complete canonical command; and
- the inverse/requeue command for a lineage replacement.

The finality coordinator orders the consumer command with any later replacement.
A physical sink entry is never authority by presence alone: its generation is
checked at its next use. Thus a crash or delayed outbox worker cannot make an old
entry usable after a newer replacement command exists. Restart verifies the
coordinator and sink cursor before production/serving; a gap enters recovery.

An external-signature outbox checks the current generation/tombstone immediately
before network publication. Replacement reconciliation cancels every undelivered
old-generation publication. Bytes already published before the replacement remain
historical evidence only; every receiver independently checks current exact scope
before tally or use.

### 5.3 Required boundaries in the metagraph path

| Boundary | Required rule |
|---|---|
| Raw binary/attestation buffering | May retain bounded exact bytes without a lease. It is explicitly unauthenticated/inert. |
| Context and exact base resolution | Acquire a purpose-scoped lease; all eta, registry, KES/VRF, and historical reads use its target. |
| Local admission/custody signature | A reproduced/validated input may produce candidate signature bytes outside the finality lock. They are not recorded, counted, or published until `commitIfCurrent` durably accepts the exact outbox/tally command. The preimage binds the exact binary and full draw context. |
| Received attestation tally | Verify and record atomically under the same exact lease scope and lineage revision. |
| Committee threshold wait | Poll outside the finality lock. A tally namespace includes exact target, lineage revision, registry/parameter era, binary, and sender. |
| Threshold success | The threshold result is a scoped typed value, not `Boolean`; call `commitIfCurrent` before admission. |
| ML0 queue and admission cache | Insert only as the ordered admission command. Entries carry exact target/generation and cannot authorize a child after invalidation. |
| Shard buffer | Insert in the same commit or a causally ordered idempotent effect. Window selection rechecks every entry. |
| Execution replay/sign | Replay and candidate signing run outside the finality lock. `commitIfCurrent` must accept the exact replay result and candidate signature before it is recorded, counted, or published. A stale candidate is discarded. |
| GL0 checkpoint inclusion | The proposal application rechecks exact bases, current lineage, execution threshold, and separately domain-separated positive assigned-watchtower coverage. Producer and execution signers have replayed; assigned watchtowers replay independently. Each ordinary GL0 validator verifies committee/coverage identity, signatures, namespace, continuity, per-MG compare-and-set, and the certificate-bound extracted global intents, applies the namespace-confined diff, runs the deterministic global kernel, and recomputes the resulting root. It does not ordinarily recreate the currency transition. |
| Economic read/follower/API response | Read from the exact immutable image. Bind the returned exact ref/evidence and check current use at the response or mutation boundary. |

No committee or network wait occurs while a finality, MPT publication, chain-store,
or consumer sink lock is held.

## 6. Invalidation inventory

The replacement transition must account for every derivative, not only the object
that originally held the lease.

| Derivative | Current source evidence | Required replacement behavior |
|---|---|---|
| Pending binary context | Recorded before the wait at `NakamotoSyncDaemon.scala:2893-2895` | Remove the scoped context or mark stale; raw bytes may remain for re-resolution. |
| Metagraph continuity/admission cache | Written before admission at `NakamotoSyncDaemon.scala:2901-2919` | Split staged continuity from committed admission; key both by exact anchor and lineage revision. Old entries cannot resolve a child. |
| Deferred raw attestations | Buffered at `NakamotoSyncDaemon.scala:2182-2187` | Bytes may remain inert. Reverify under a newly acquired lease before any new-generation tally. |
| Verified attestation tally | Recorded at `MetagraphCommitteeGate.scala:539-550` | Namespace by exact target, lineage revision, binary, registry/parameter era, and sender. Never count across generations. |
| Threshold result | Boolean today at `MetagraphCommitteeGate.scala:485-523` | Replace with scoped evidence/result; old result fails `commitIfCurrent`. |
| Signature publication outbox | Current publisher is called inline at `MetagraphCommitteeGate.scala:451-462` | Durably enqueue only through scoped commit; suppress an undelivered old-generation command before send and never infer current authority from an earlier send. |
| Process/state-channel queue | Called after wait at `NakamotoSyncDaemon.scala:2941-2948` | Insert only through scoped commit; replacement requeues still-valid input exactly once or leaves it staged. |
| Orphan child drain | Runs after admission at `NakamotoSyncDaemon.scala:2949-2950` | Child resolution must use the new committed scoped parent, never an invalidated ordinal cache. |
| Shard binary buffer | Inserted at `NakamotoSyncDaemon.scala:2945-2947` | Tombstone/remove orphan-base entries, requeue once, and prevent stale checkpoint selection. |
| Checkpoint replay result/signature capability | Future execution path | Bind exact ordered inputs/base and lineage revision; invalidate before signing or inclusion. Historical signature bytes may remain evidence but cannot satisfy a current-generation certificate without full revalidation. |
| Assigned-watchtower selection/replay/coverage | Future execution path | Bind assignment, exact base/input/checkpoint, registry/parameter era, and lineage revision. Old replay bytes remain historical only and cannot satisfy current positive-coverage or challenge authority. |
| Challenge/adjudication work | Future execution path | Retain the exact allegation/evidence bytes, but reacquire the purpose-specific historical context and replay before any rollback/slash. Missing or stale context defers and cannot slash. |
| Cross-metagraph settlement command/nullifier capability | Future global kernel path | Bind owner authorization, exact signed historical read, proposal-parent CAS, and lineage revision. Replacement invalidates an uncommitted command; no sibling cache or surviving MG lends authority. |
| Optimistic sampler context | Future FinalityGate path | A lease may bind only the historical registry/parameter context used by sampling; it cannot qualify its own target or replace the optimistic decision transcript. Replacement invalidates cached context and any unpublished derivative. |
| Tower eligibility/proof capability | Future tower path | Namespace by exact target, branch evidence, trial/registry context, and lineage revision. Historical proof bytes remain verifiable history but cannot be relabeled current or servable after replacement. |
| Protocol-correction command | Future correction path | Bind exact target/precondition and correction artifact. Replacement invalidates an uncommitted correction; only a newly verified current GL0 protocol command may apply. |
| Checkpoint hard anchor/watermark | Finality effect kind exists at `FinalityEffects.scala:27-30` | Apply old/new/MRCA replacement, including same/lower shard ordinal, through ordered idempotent reconciliation. |
| Binary confirmation/requeue | Finality effect kind exists at `FinalityEffects.scala:28-30` | Reverse old confirmations and requeue exactly once before a dependent use. |
| Eta/committee caches | Finality effect kind exists at `FinalityEffects.scala:29-30` | Recompute from exact replacement ancestry; no live/current-state fallback. |
| MPT/semantic/economic read cache | Finality effects enumerate overlay and serving projections at `FinalityEffects.scala:19-22` | Namespace by exact ref/publication; never relabel bytes at an inherited ordinal. |
| Ordinal-keyed checkpoint/MPT serving files | Selected after an ordinal gate at `FinalizedSnapshotReader.scala:137-147,200-255` | Resolve an exact released ref first, then serve bytes proved against that same ref under a response-boundary recheck. Old files remain historical only. |
| Downstream events/followers | Finality effect kind exists at `FinalityEffects.scala:33` | Emit durable old/new/MRCA event in release order; follower cursor is exact hash, not ordinal. |

Retention or eviction is not invalidation authority. Missing bytes cause defer or
authenticated recovery; they do not permit a current-head fallback.

## 7. Exact evidence and readback dependencies

No live lease issuer may be implemented until all rows below have a verifier and a
negative test, not merely a schema.

| Dependency | Required proof/readback | Current status |
|---|---|---|
| Objective fork choice | Complete the O-15 frontier construction and its parameter-era, lineage, selected-result, evidence, and independent-verifier gates | Open; the direction is owner-ratified, but `ForkChoiceDecision` is an opaque pointer |
| Optimistic qualification | O-01 registry/weight snapshot, K/alpha/beta, emit-once attestations, `T_weight`, exact target | Open |
| Depth qualification | Exact authenticated canonical suffix, `k1` metric/equality, parameter hash, target position | Open with O-15 metric boundary |
| Authenticated executed snapshot | Exact signed bytes, network/genesis/era/parameters, parent/ordinal/slot, VRF/KES, body, transition, root | Live validation exists in pieces; no released-core capability |
| Complete exact reference | `(ordinal,hash,parentHash,mptRoot)` in one identity | Target type exists; current signed `GlobalSyncView` is incomplete |
| MPT image | Immutable complete physical bytes, reproduced root, active publication receipt | Dark partial; ROOT-002/003/008/011 remain open |
| Structural/economic state | Active-era ROOT field/key/value/identity/population receipt plus independently reproduced O-07/ECON-G authorization, conservation, backing, replay, and transition receipts | Open |
| Authenticated anchor | Snapshot-to-image/semantic binding and independent readback | Dark pointer/receipt only; no live verifier |
| Durable coordinator | Canonical versus orphaned released-core history, both revisions, crash-safe transition, objective restoration | Dark partial; no live release executor |
| Consumer sink | Expected digest/revision, idempotent command, readback receipt, inverse/requeue, restart verification | Not implemented for admission/shard consumers |
| Reorg delivery | Ordered old/new/MRCA plus orphaned/adopted paths, durable downstream acknowledgement | Open |

The lease interface may be coded dark under the ratified direction, but issuance remains
disabled until these dependencies close. In particular, wrapping the current
Boolean callback in the opaque class would preserve `FIN-14`.

## 8. FOLLOW-008 RED matrix

Every test uses authenticated complete snapshots once O-15/O-01 fixtures exist.
Until then, a pure reference model may establish expected state transitions, but
cannot close the runtime gate.

| ID | Schedule | Required assertion |
|---|---|---|
| FOLLOW-008A | A at N qualifies by `T_weight`; unqualified B replaces A at N. | B cannot acquire a lease or create any derivative. A's ordinal/generation/evidence never transfers. |
| FOLLOW-008B | Same as A, with A qualifying by `k1`. | Identical rejection; the two rails differ only in evidence verification. |
| FOLLOW-008C | Replace after the first short descriptor capture, during each unlocked snapshot/MPT/semantic/anchor readback, and immediately before the final CAS. | The final CAS returns stale/retry; no mixed lease is minted and no coordinator/finality lock spans image verification. |
| FOLLOW-008D | Replace after lease acquisition, during candidate signing, and before outbox/tally commit. | Candidate bytes may be discarded, but zero signature is recorded, counted, or published and no tally side effect occurs. |
| FOLLOW-008E | Replace between received-attestation verification and tally record. | Record CAS fails; count remains zero in the new generation. |
| FOLLOW-008F | Replace during every committee poll iteration, including immediately before threshold success. | No finality lock is held during the wait; threshold result cannot commit admission. |
| FOLLOW-008G | Replace after threshold success but before queue/cache/shard-buffer commit. | `commitIfCurrent` returns stale and all three sinks remain unchanged. |
| FOLLOW-008H | Crash after scoped admission intent but before each sink write; then replace and restart. | Recovery orders invalidation after/before the old command correctly; no stale entry is usable and requeue occurs at most once. |
| FOLLOW-008I | Pure descendant extensions occur throughout a wait. | Exact-ancestor scope may commit after revalidation; latest-head scope must reacquire. No restart loop is induced merely by extension. |
| FOLLOW-008J | Replacement occurs above a still-canonical target. | Old lineage lease fails. Reacquisition may succeed only after the target and all purpose/readback facts reverify under the new revision. |
| FOLLOW-008K | A is replaced by B and later becomes selected again. | Monotone lineage revision prevents ABA reuse of A's old lease, tally, threshold result, cache, or permit. |
| FOLLOW-008L | Keep raw binary and deferred attestation bytes across replacement. | Bytes can be re-resolved/reverified; no cached context, ordinal, sender count, or prior threshold is inherited. |
| FOLLOW-008M | One checkpoint window contains binaries at multiple nondecreasing exact P2 refs; one ref is orphaned. | Whole affected MG/checkpoint work defers/rebases according to its atomicity rule; no valid sibling entry lends authority to the orphaned ref. |
| FOLLOW-008N | Replace after replay but before execution signing, and after signing but before GL0 inclusion. | First schedule emits no signature. Second retains historical bytes only; inclusion rejects until the complete certificate/base is current and revalidated. |
| FOLLOW-008O | Retention evicts target bytes while lease work is in flight. | Commit defers or enters authenticated recovery; it never reads the mutable current base or treats eviction as orphaning evidence. |
| FOLLOW-008P | Concurrent replacement and 100 workers commit the same command. | Linearizable result is old-generation commits ordered before invalidation or stale failures after it; idempotency yields no duplicate queue, shard, economic, or requeue effect. |

Each schedule injects the transition before and after every read, evidence verify,
CAS, journal append, sink write, and readback. Assertions include zero unintended
admission receipt, state-validity/execution/optimistic signature, checkpoint
eligibility, economic read capability, follower release, or external serving.

## 9. Implementation sequence

1. Treat the ratified directions below as fixed and close their engineering freeze gates. Do not change live issuance.
2. Add a test-only reference model for branch/lineage revisions, lease acquisition,
   and `commitIfCurrent`; make FOLLOW-008A through FOLLOW-008P executable.
3. Close O-15/O-01 evidence verifiers and the exact P0/P1/P2 transition model.
4. Complete the released-core MPT/semantic/anchor transaction and durable effect
   executor; prove crash/restart/replacement ordering.
5. Add the dark opaque lease API with no production constructor and denylist
   Boolean/ordinal authority adapters.
6. Migrate one consumer at a time: read-only serving, metagraph admission,
   admission tally/cache, shard buffer, execution replay/signing, GL0 inclusion,
   downstream followers.
7. Delete `Phase2CurrencyBinaryContext.verify(Boolean)`, the watermark-plus-best-tip
   adapter, and every unscoped cache/tally key in the same activation change.
8. Activate only after FOLLOW-008, FIN-W, ROOT, recovery, and multi-process chaos
   gates pass on the complete path.

### 9.1 Packet ownership

The protocol test plan's packet map is authoritative:

- P6 owns the FinalityGate capability API, branch/lineage revisions, two-stage
  acquisition, `commitIfCurrent`, durable finality ordering, and the pure/live
  lease reference tests.
- P7 owns exact diff/root/checkpoint schemas and strict state-image interfaces. It
  supplies types and verifiers to P6/P8 but does not own the lease.
- P8 owns admission, tally/cache, shard buffer, execution-signing, watchtower,
  challenge, and checkpoint-inclusion adapters and their `FOLLOW-008` schedules.
- P10 owns follower, GSI, historical-economic-read, and downstream rebase adapters.
- P11 owns exact serving/bootstrap/recovery adapters and retention-loss schedules.

No packet may close `FOLLOW-008` alone. P6 integrates the complete matrix after the
consumer owners supply their disjoint adapters and tests.

## 10. Ratified choices and engineering freeze gates

The architecture already forbids ordinal inheritance and stale use. The owner has ratified the
directions below; engineering must encode the remaining purpose, freshness, schema, and dependency
gates before interface activation:

1. **O-16A - replacement invalidation granularity.** The conservative V1 rule is ratified:
   every rollback/replacement advances `CanonicalLineageRevision` and invalidates
   all old leases, even when a target survives below the MRCA. Reacquisition can
   then preserve valid work. Selective invalidation is more available but adds a
   per-target dependency graph to every sink.
2. **O-16B - exhaustive exact-ancestor versus latest-head purpose policy (locked
   L-19 semantics).** Freeze a policy for every sealed purpose above:
   binary historical reads/execution may use a still-canonical exact P2 ancestor;
   `/latest` serving requires the current P2 head. Engineering must classify watchtower,
   challenge, settlement, optimistic-context, tower, correction, follower, and
   event-delivery scopes. No generic case or caller silently chooses.
3. **O-16C - attestation reuse after a target-surviving replacement.** Retain
   raw signed bytes only, then rerunning registry/VRF/KES/signature and exact-anchor
   verification before indexing them into the new lineage revision. Never copy a
   prior count or threshold result.
4. **O-16D - historical-reference age rule.** Receiver-local wall-clock/HOCON validity limits are
   forbidden. Engineering must define any operation-specific or branch-authenticated protocol age
   bound through ECON-G and test it across reorg/era boundaries.
5. **O-16E - consumer-command durability boundary (locked L-23
   semantics).** Use
   the existing L-23 model:
   finality orders an idempotent scoped command plus inverse/requeue through its
   durable effect journal; physical sink presence alone is never authority. An
   all-stores database transaction is not required.
6. **O-16F - signed exact reference migration.** The active-era binary/checkpoint shape binds the
   full `GlobalSnapshotStateRef`, registry/parameter era, and purpose domain. The current
   ordinal/hash-only `GlobalSyncView` cannot be the final signed consumer scope; the concrete
   schema and codec remain engineering.

Items 2 and 5 are conformance checks, not requests to reopen L-19 or L-23. Items 1
and 3 choose the V1 invalidation/revalidation behavior. Items 4 and 6 affect
protocol validity/schema and must be frozen before activation.
