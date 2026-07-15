# Replay Before GL0 Tip Attestation

**Status:** implementation packet; restrictive containment and the lower
execution-receipt slice have landed in the current worktree. Raw local GL0
attestation emission and the receiver-local cumulative-weight finalization sink
are removed. GL0 proposal construction and follower recreation now return
issuer-reference-checked receipts within the trusted unmodified JVM, and the
receive path has no publicly constructible `Valid` success value. Those lower
receipts do not bind the final
decorated signed body, transport envelope, KES evidence, current preference, or
finality. The authenticated-executed capability, `storeValidated`, durable
lineage/projection journals, and real O-01 Avalanche/Snowball work remain open.
This containment does not make optimistic finality production-safe or active.

## 1. Purpose and consensus role

A GL0 operator may sign optimistic evidence for an exact snapshot only after
that operator has authenticated the snapshot and locally reproduced its complete
deterministic GL0 transition. A selected tip, a stored hash, successful decoding,
or another operator's signature is not such proof.

The current complete GL0 transition includes universal execution of native
GL1/DAG-token inputs. The target additionally requires the deterministic global
cross-metagraph settlement kernel, but that kernel is E9 work and the live
`numShards <= 1` path bypasses it. For a sharded CL1 checkpoint, the target
noncommittee GL0 transition is
execution-certificate and positive-watchtower-coverage verification, scoped
diff/root/compare-and-set adoption, and global-kernel execution; it is not
ordinary currency recreation. Current ordinary noncommittee replay of **sharded
CL1** framework transitions is a temporary backstop until that complete
certified-diff path lands. A receipt must never be interpreted either as
permission to stop universal native GL1/DAG execution by every GL0 node or as a
requirement to preserve ordinary noncommittee GL0 recreation of sharded CL1 after
the certified-diff gate is complete.

This is an input boundary for the Avalanche/Snowball optimistic Phase-2 rail. It
is not economic validation by signature count, and it does not add a global BFT
proposal, vote, lock, quorum certificate, commit certificate, or view change.
The intended Phase-2 predicate remains decided-attestation `T_weight` OR
canonical `k1` depth. Phase 2 remains density-reorgable. The current
`FinalityGate` documentation states the same no-BFT target and its current
ordinal-only limitation (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/snapshot/finality/FinalityGate.scala:9-17`).

The capability in this packet proves only what this node authenticated and
executed. It cannot:

- make an invalid snapshot valid;
- select the canonical tine;
- qualify its own target as Phase 2;
- replace the O-01 K/alpha/beta decision transcript;
- replace the O-15 objective frontier decision; or
- turn attestations into a global BFT certificate.

## 2. Current source facts

The received-snapshot path now preserves lower replay provenance, but it does not
yet preserve complete authenticated-executed provenance:

1. The protobuf envelope is bound to the signed hash, ordinal, parent, signer,
   slot certificate, VRF proof/key, eta, and VRF output
   (`NakamotoSyncDaemon.scala:94-165`, invoked at `:1332-1340`).
2. The registered operator pair and historical eta are resolved before
   eligibility (`NakamotoSyncDaemon.scala:1366-1410`).
3. KES is verified before replay and storage
   (`NakamotoSyncDaemon.scala:1412-1427`).
4. `GlobalSnapshotConsensusFunctions` allocates lower proposal/replay receipts
   only after the complete native-GL1-inclusive transition function returns. In
   the ordinary unmodified JVM path, consumption requires the private issuer
   identity of that exact functions instance
   (`GlobalSnapshotConsensusFunctions.scala:57-189`).
5. `NakamotoSnapshotValidator.validate` verifies VRF, Ed25519, slot-certificate
   lineage, and complete artifact recreation, consumes the lower replay receipt,
   and mints a private replay-valid result bound to the object-global validator
   issuer reference
   (`NakamotoSnapshotValidator.scala:28-72,177-341`).
6. `commitReplayValidated` delegates to the validator-owned issuer check before
   exposing snapshot/context to storage and canonical effects
   (`NakamotoSyncDaemon.scala:348-361,1529-1642`).

This is a trusted-process type/provenance guard, not a cryptographic boundary
against arbitrary in-process code. Reflection, `Unsafe`, an agent, or modified JVM
bytecode can inspect private fields/constructors and recover or forge issuer-bound
objects. Current adversarial tests prove only that public Scala construction and
reflected values carrying null or wrong issuer references invoke zero effects;
they do not prove resistance after issuer extraction. The positive outer-path test
does cover `NakamotoSnapshotValidator.validate` through
`consumeReplayValidated`, requiring the exact snapshot/context and exactly one
callback (`GlobalSnapshotExecutionReceiptSuite`, “full snapshot validation mints
one replay result carrying the exact snapshot and replayed context”).

This is not yet the target `AuthenticatedExecutedGlobalSnapshot`. The lower
producer receipt is consumed before certificate/eta decoration and signing, and
the received result does not carry the outer envelope or KES evidence that the
sole production caller checked immediately beforehand. No store, preference,
finality, or signing API accepts either lower receipt. The chain-store boundary
has typed outcomes and an internally serialized selected-tip transition, but no
execution-provenance type. `BecameSelected` is only the fresh selection signal and
still requires the exact branch+lineage canonical-effects CAS in RTA-006A. A raw
`Duplicate` must never authorize a projection: only future `storeValidated` may
insert a new receipt-backed validated entry after matching exact signed bytes,
proofs hash, hash era, and the locally replayed context, then remove the matching
physically separate recovery seed. It never upgrades a raw shared-map entry. The
live raw `store` still accepts caller-supplied metadata, and revisions are neither
durable nor signing authority. No current signing API consumes either result.

The pre-containment implementation had three independent authority defects:

- naked `emitAttestation`/`emitTipAttestation` APIs could sign caller-supplied
  fields;
- receipt of a valid snapshot invented an attestation under the producer's
  identity; and
- a five-second best-tip ticker re-signed storage-derived fields and a one-round
  cumulative-weight read could call `chainStore.finalize`.

Those paths are now historical. The source/API tripwire requires both naked
emitter names and every `publishAttestation` call in the GL0 leader/receive paths
to be absent, forbids the legacy `highestFinalizedOrdinal` API, forbids
producer-invented tracker mutation, and permits exactly one GL0
`NakamotoChainStore.finalizeSelectedAt` call
(`GlobalOptimisticFinalityContainmentSuite.scala`).

The sole state-changing GL0 snapshot Phase-2 sink is now canonical `k1` depth:
the monitor rereads the exact selected tip under the shared mutation barrier,
derives `selected.ordinal - k1` without consuming sticky trigger telemetry, and
uses hash + branch/lineage revision CAS to derive the target under the chain-store
lock. Local serve state precedes the public watermark; Phase-2 consumers run only
after it. This is in-process containment, not durable exact-hash finality.
`RTA-RED-019` exercises exact-hash depth progress with an empty attestation map
(`NakamotoChainStoreSuite.scala`). Verified remote Ed25519+KES attestations may
still enter `TipTracker`, but the leader loop labels them telemetry and gives
them no finalization sink.

The store also cannot prove replay provenance. `StoredSnapshot` has no validation
receipt (`NakamotoChainStore.scala:31-46`), and `store` accepts raw snapshot,
context, ordinal, slot, parent, and VRF output arguments
(`NakamotoChainStore.scala:230-244,490-503`). On startup it is seeded directly
from `snapshotStorage.head` without full replay
(`SnapshotLeaderLoop.scala:693-719`). A restored head must therefore be treated
as unattestable until exact readback validation and replay complete.

The shard signing boundary is the pattern to preserve, not authority for GL0:
`VerifiedShardCheckpoint` has a sealed capability whose implementation is private
to its replaying manager
(`ShardCheckpointGl0AcceptanceManager.scala:57-68,218-221,312-322`), and the
emitter accepts only that capability
(`ShardCheckpointAttestationEmitter.scala:47-53,128-154`). GL0 needs the same
non-forgeable shape with GL0-specific authentication, execution, and lineage
semantics.

## 3. Required invariants

### RTA-001 - Complete local authentication and execution

`AuthenticatedExecutedGlobalSnapshot` is minted only after all of the following
hold for the exact same snapshot bytes and exact retained parent:

- canonical body hash equals the transport hash;
- signed ordinal, parent, producer, slot, parent slot, eta, VRF proof, VRF key,
  and VRF output equal their authenticated counterparts;
- the producer's exact active-period KES+VRF pair resolves from the required
  historical registry view;
- VRF eligibility, Ed25519, KES, slot lineage, active-era shape, and parent
  continuity all verify;
- the complete deterministic GL0 transition is locally recreated;
- the recreated artifact, context, state proof, complete consensus root, and
  every consensus field equal the signed claim; and
- every embedded execution checkpoint has passed the complete GL0 artifact
  validation required by that transition.

Missing parent, history, registry state, inputs, or replay data means defer or
reject. It never mints a partial capability.

### RTA-002 - Exact identity is derived, never supplied to signing

The authenticated-executed capability binds at least:

```text
(ordinal, hash, parentHash, mptRoot, slot, producer,
 artifactPeriod, signedSnapshot, reproducedContext,
 validated registry/eta/parameter context)
```

The emitter derives the attestation hash, slot, ordinal, and KES period from the
capability. A caller cannot substitute any of them.

### RTA-003 - Execution validity and current preference are separate

Successful replay proves that a snapshot is a valid branch candidate. It does
not prove that it is the current selected tip. A second opaque capability,
`PreferredExecutedTip`, is minted only when the chain store atomically confirms
that the exact authenticated-executed hash is its current selected tip under the
current monotone `CanonicalBranchRevision` and `CanonicalLineageRevision`.

The capability also binds its issuing chain-store instance and the exact stored
representation: body hash, proofs hash, active hash logic, state reference,
branch revision, and lineage revision. A token from another store, another proof
variant, another hash era, or an earlier `A -> B -> A` generation is stale.

An alternate branch may retain historical replay evidence but cannot obtain a
current-preference capability.

### RTA-004 - Extension, reorg, and ABA safety

The two persisted monotone revisions from the P6 lease contract have different
jobs:

- `CanonicalBranchRevision` advances on every selected-head mutation, including a
  pure descendant extension. Any unpublished `PreferredExecutedTip` from the old
  branch revision fails because it no longer proves exact-tip preference.
- `CanonicalLineageRevision` advances only when a rollback, replacement,
  reconstruction, clearing, or other mutation removes or substitutes previously
  canonical state. It invalidates every active tally and authority from the prior
  lineage. A pure extension preserves this revision and preserves already-published
  exact-hash evidence for ancestors that remain canonical.

Neither revision resets. A hash returning to the selected position after
`A -> B -> A` does not revive the old capability; the node must reacquire current
preference under both new revisions and, for V1, rerun or revalidate the complete
exact replay receipt before another signature is authorized. This is the revision
split specified in
`docs/review/P6-FIN14-PHASE2-CONSUMER-LEASE.md:294-342`.

### RTA-005 - Restart is fail-closed

Capabilities are local, opaque, and non-serializable. No Circe, Scodec, Kryo,
Java-serialization, or protobuf codec exists for them. Restarted snapshot bytes,
an ordinal index, a cached root, or a prior Boolean validation result cannot
recreate one.

`snapshotStorage.head` may seed physically separate ancestry-recovery storage,
but that seed is explicitly unattestable and never inhabits validated chain
state. Full exact replay of the same bytes inserts a new receipt-backed validated
entry through `storeValidated`, then removes the matching recovery seed. It does
not upgrade a raw shared-map `Duplicate`. A same-hash retry can acquire projection
authority only when the existing entry was already validated with the exact
signed bytes, proofs hash, hash era, replayed context, and durable projection
journal identity.

A durable attestation journal may retain an exact signed command and publication
outcome, but those bytes are historical evidence/outbox state only. They do not
recreate either capability. After restart, retrying or signing again requires full
exact replay plus a fresh exact-tip `PreferredExecutedTip` under both current
revisions. Completing the tally for an already-`Published` command requires full
exact replay plus a fresh atomic proof that the target remains canonical under the
current lineage; descendant extension need not make the published ancestor inert.

### RTA-006 - Local production follows the same rule

The producer's call to `createProposalArtifact` is its complete local execution
(`GlobalSnapshotConsensusFunctions.scala:60-68,329-353`). Its opaque execution
receipt is carried through exact artifact decoration, Ed25519 signing, registered
KES signing, self-verification, and chain-store selection. Only the successful
exact result can become `AuthenticatedExecutedGlobalSnapshot`.

**Partial containment landed:** the store returns `Duplicate`,
`StoredAlternate`, `BecameSelected`, or typed rejection; receive no longer
reconstructs selection with a later unbound `bestTip` read. `BecameSelected`
alone is not canonical-effects authority. The current CAS tranche may serialize
a local projection while holding the selection lock and rechecking store
identity, selected body hash, and branch and lineage revisions. That closes the
stale `A`-after-`B` write race for those callbacks, but it is not multi-sink
atomic or complete representation identity: a partial callback failure leaves
production paused, while serving and other node activity are not globally
latched in `RecoveryRequired`. Network publication or signing must never run
under the selection lock. Durable crash recovery and atomic visibility still
require RTA-006A (`NakamotoChainStore.scala:249-264,634-657`; receiver use at
`NakamotoSyncDaemon.scala:1707-1742`; producer use at
`SnapshotLeaderLoop.scala:1700-1713`).

### RTA-006A - Canonical effects require exact branch-and-lineage CAS

Every canonical effect derived from a selected snapshot is bound to the same
`PreferredExecutedTip` identity:

```text
(storeInstance, stateRef, bodyHash, proofsHash, hashLogic,
 branchRevision, lineageRevision)
```

Immutable hash-addressed noncanonical MPT branch and accumulator staging is
receipt-gated but may precede selection so a retained alternate remains
replayable. Canonical effects include the selected pointer, MPT promotion/fold or
canonical reader pointer, `SnapshotStorage`/`LastN`/`LastGlobal` projection,
staged accumulator promotion, event/mempool removal or return, readiness, and
downstream serving state. A Boolean precheck followed by unlocked writes, a later
`bestTip` read, or ordinal equality cannot authorize any canonical effect.

The current selection-lock containment is permitted only as an intermediate
tranche:

1. the chain store holds its selection lock;
2. it compares store identity, selected body hash, and both revisions; it does
   **not** yet compare proofs hash, active hash logic, state reference/root, or a
   projection digest;
3. its package-owned callback receives the internally stored entry, but the type
   does not enforce a bounded effect set;
4. it returns `Applied` or `Stale`; and
5. any error after a partial projection leaves production paused and prevents
   that path from publishing, but a later successful projection can clear the
   shared reason; this is not a durable global recovery latch.

This intermediate form prevents a stale branch callback from starting after a
replacement, but holding a lock across fallible multi-sink I/O is not the
activation target and does not make those sinks crash-atomic.

The current callback-shaped API is package-owned but does not enforce "bounded
local projection" in its type. Before this containment can be relied on, replace
the arbitrary callback or add source/API tripwires forbidding signing, network
I/O, chain-store re-entry, and unrelated work under the selection lock, and add a
latched `RecoveryRequired` failure state.

The production target is a durable, idempotent, revision-tagged projection
protocol with two noncircular digests:

```text
stagedContentDigest = H(
  "gl0-projection-stage-v1" ||
  ordered(sinkId, key, codecEra, byteLength, H(exactStagedBytes))
)

planDigest = H(
  "gl0-projection-plan-v1" ||
  PreferredExecutedTipIdentity ||
  ordered(effectKind, sinkId, key, codecEra, byteLength, H(exactEffectBytes)) ||
  stagedContentDigest
)
```

The ordered effect manifest is complete: it covers the selected pointer, MPT
promotion/canonical reader pointer, projection stores, accumulator promotion,
event or mempool effects, readiness, and downstream serving state. Every effect
byte representation and ordering rule is active-era canonical; sink iteration or
map order is never implicit. Effects which are represented by a committed
canonical projection record, rather than copied into a separate sink during the
CAS, still appear in the manifest as exact typed actions. For every staged
effect, the `(sinkId, key, codecEra, byteLength, H(bytes))` tuple in the plan
manifest must be byte-identical to its tuple in the staged-content manifest.
Consequently `stagedContentDigest` is a deterministic committed submanifest of
`planDigest`, never a second caller-supplied assertion.

The protocol order is:

1. From the replay receipt and preferred identity, derive the complete canonical
   effect set and materialize its expensive bytes into hash-addressed,
   noncanonical staging **outside** the selection lock. This staging is inert and
   grants no preference, serving, signing, or projection authority.
2. Read back the exact staged bytes, form the ordered complete effect manifest,
   compute `stagedContentDigest`, then compute `planDigest` over that manifest plus
   the staged digest. Reject any effect-set/manifest/staging mismatch. Thus no
   digest is claimed before the bytes it commits exist.
3. A short prepare CAS rechecks the exact preferred identity and records one
   durable prepared intent containing that identity, `planDigest`,
   `stagedContentDigest`, and the staging object identities. Stale preference
   leaves only inert garbage-collectable staging.
4. A short final CAS rereads the intent and staging, recomputes both digests,
   rechecks exact selected hash, proof/hash-era identity, state reference/root,
   branch revision, lineage revision, and intent identity, then atomically
   publishes one canonical projection record and marks the intent committed.
5. Only readers following that committed canonical projection record observe the
   new projection. Restart deterministically finishes or rolls back the
   idempotent intent, or enters `RecoveryRequired`; it never infers success from
   a subset of updated sinks.

Current unversioned sinks do not satisfy that protocol merely because a
stage/prepare/final API exists. They must either read one atomically published
canonical projection record or be made idempotent projections of that committed
record. Signing, gossip, HTTP publication, and other network I/O are always
outside the chain-store lock and require their own durable command plus the
pre/post-publication checks in RTA-009.

### RTA-007 - Received flow never reconstructs authority from storage

The received flow carries the exact capability produced by its validation call
through storage and selection. `processValidSnapshot` must not discard it and
later rebuild signing authority from the protobuf envelope or `bestTip` fields.

### RTA-008 - No receiver-invented attestation

Receiving a valid producer-signed snapshot does not authorize this receiver to
invent optimistic evidence under the producer's identity. The removed
receiver-clock `recordAttestation(producerId, ...)` path is historical and must
remain absent; the source tripwire rejects that exact mutation pattern
(`GlobalOptimisticFinalityContainmentSuite.scala:47-70`).

Only a separately signed and KES-verified attestation received through
`handleAttestation` may enter the tracker under a remote operator identity
(`NakamotoSyncDaemon.scala:1781-1855`).

### RTA-009 - Publish success and a canonical-lineage CAS precede active weight

For one immutable attestation command:

1. acquire `PreferredExecutedTip`;
2. derive the exact attestation from it;
3. produce and locally verify Ed25519 and KES signatures;
4. durably persist the exact signed bytes and one idempotency key in a
   package-owned publication journal/outbox before network publication;
5. recheck exact selected hash plus both branch and lineage revisions immediately
   before publication;
6. publish the exact immutable bytes;
7. require `PublishResponse.ok == true` and durably mark those exact bytes
   `Published`;
8. call one atomic `recordPublishedIfCanonical(targetHash,
   expectedLineageRevision, replayReceipt, publishedEvidence)` operation. It
   verifies that the exact target remains canonical under the same lineage and
   records the exact-hash evidence in that lineage's tally in one transition. It
   deliberately does not require branch-revision equality: a descendant extension
   may advance the branch revision while preserving the target and its evidence.

The durable historical evidence/outbox and the current-lineage active tally are
different stores with different authority. A successful or ambiguous network
publication may leave durable historical evidence for an exact hash. It cannot
add current weight after that hash was orphaned or its lineage was replaced. The
canonical-lineage tally CAS, not the pre-publication check, is the local
linearization point for current Snowball weight.

In the pre-containment path, local recording preceded publication and the
response Boolean was discarded. That local emitter and its publication call are
now deleted, so no current GL0 attestation can acquire local weight through this
bug (`GlobalOptimisticFinalityContainmentSuite.scala:31-52`). The Go server still
returns ordinary `PublishResponse{Ok:false}` values rather than raising for every
failure (`p2p/internal/grpcserver/server.go:188-197`), so the future emitter must
implement the durable publication protocol below; `.void` is not a success test.

On exception, cancellation, `ok=false`, stale branch/lineage at the applicable
gate, signing failure, or local verification failure, no active local weight is
recorded. A known failure may
retain only its durable attempt state for audit/retry; it is not published
evidence and never enters the tally. Retry reuses the same logical decision,
idempotency key, and exact signed bytes; it does not create a second Snowball
decision.

An ambiguous publish result may cause duplicate network delivery. The journal
records it as publication-unknown, and receiver deduplication uses the exact
attestation identity. After process restart, journal bytes are inert: the node
must fully replay the exact snapshot. A retry also requires fresh exact-tip
preference under both revisions. An already-`Published` record instead requires a
fresh canonical-lineage check using the replay receipt; if the target is orphaned
or the lineage changed, the record remains historical/abandoned and produces zero
active weight. Persisted publication alone is never authority.

A head change before the pre-publication check invalidates the exact-tip
capability and suppresses publication. A pure descendant extension after that
check may race with network publication; `recordPublishedIfCanonical` may retain
the evidence only if the exact target remains canonical under the unchanged
lineage. A replacement after that check may also race with an unavoidable send,
but the post-publication CAS rejects active local weight. A replacement after a
successful active record advances the lineage and invalidates that old-lineage
tally through the same replacement transition. This does not
retroactively forge the historical exact-hash evidence; FinalityGate/reorg
processing marks the old hash orphaned, and other nodes scope it to that exact
hash and lineage. The future Snowball wire transcript must bind the query/round
or decision context needed to interpret that history.

### RTA-010 - No raw or compatibility signing path

The following main-source APIs are forbidden:

- `emitTipAttestation(hash, slot, ordinal, ...)`;
- `emitAttestation(pb.Snapshot, ...)` that derives signing authority from receipt;
- a capability factory accepting `Boolean`, `ValidationResult`, root, hash,
  signature count, `bestTip`, or storage presence as authority;
- a fallback from missing capability to raw fields;
- a public/test-only main-source constructor for either capability; and
- restoring a capability from disk or wire bytes.

The only public signing surface is structurally equivalent to:

```scala
trait GlobalOptimisticAttestationEmitter[F[_]] {
  def emit(preferred: PreferredExecutedTip): F[TipAttestationEmitResult]
}
```

### RTA-011 - No global BFT semantics

The capability and emitted artifact are optimistic Snowball evidence only. Their
types and signature domains must not be named or decoded as a global vote, lock,
QC, commit certificate, or view-change artifact. Signature count never validates
state and never changes fork choice. The state-changing optimistic sink must
eventually consume only the ratified decided-attestation `T_weight` transcript.

The former one-round cumulative-weight sink was unsafe and is now removed. It
must not return under another name. Remote attestation and `T_weight` objects in
the current tree are telemetry only; a future state-changing optimistic sink must
consume the ratified sampled, portable exact-hash decision evidence.

## 4. Capability boundaries

The recommended shape uses two sealed traits with private implementations and no
public companion constructor:

```scala
sealed trait AuthenticatedExecutedGlobalSnapshot {
  def stateRef: GlobalSnapshotStateRef
  def slot: Slot
  private[nakamoto] def signedSnapshot: Signed[GlobalIncrementalSnapshot]
  private[nakamoto] def context: GlobalSnapshotInfo
}

sealed trait PreferredExecutedTip {
  def executed: AuthenticatedExecutedGlobalSnapshot
  private[nakamoto] def storeInstance: StoreInstanceId
  private[nakamoto] def proofsHash: ProofsHash
  private[nakamoto] def hashLogic: HashLogic
  private[nakamoto] def branchRevision: CanonicalBranchRevision
  private[nakamoto] def lineageRevision: CanonicalLineageRevision
}
```

The exact names may follow local conventions, but the two authorities must not be
collapsed:

- the GL0 validation manager is the only authenticated-executed minter;
- the chain store is the only current-preference minter; and
- the optimistic emitter consumes only the latter.

The lower slice did not merely rename public `NakamotoSnapshotValidator.Valid`:
that type is removed. Lower consensus-function receipts use a per-instance issuer
reference; the outer replay-valid result uses the validator object's global issuer
reference. Exact implementation/issuer checks precede payload access or effects
in the ordinary unmodified JVM path. Tests currently close only public Scala
construction and reflected null/wrong-issuer cases. They do not close reflection
after issuer extraction, arbitrary in-process code, or a modified JVM. A positive
full validator-to-consumer test now proves exact payload delivery and one callback,
but not hostile-bytecode resistance. The slice deliberately does not claim the
target authenticated-executed boundary because the outer envelope/KES checks and
final signed producer body are not inside one minting operation.

The landed lower GL0 execution receipts avoid executing a state-mutating
transition twice. The existing generic
`ConsensusFunctions` methods may continue serving non-Nakamoto callers, but the
Nakamoto producer and validator paths must use receipt-returning methods whose
private implementation is allocated only after `createProposalArtifact` or
`validateArtifact` completes successfully.

## 5. Required flows

### 5.1 Received snapshot

```text
decode bytes
  -> exact envelope binding
  -> exact historical registry + eta resolution
  -> VRF + Ed25519 + KES + certificate/lineage checks
  -> exact-parent full GL0 recreation
  -> AuthenticatedExecutedGlobalSnapshot
  -> chainStore.storeValidated
       -> StoredAlternate: retain as valid branch, no optimistic emit
       -> ValidatedDuplicate: retry only an exact receipt/journal-bound projection
       -> BecameSelected(branchRevision,lineageRevision): acquire PreferredExecutedTip
  -> materialize inert projection staging outside the selection lock
  -> read back staging and compute stagedContentDigest + planDigest
  -> short prepare CAS records the exact revision-tagged projection intent
  -> short final CAS over exact branch + lineage makes projection visible
  -> Snowball decision path may emit using PreferredExecutedTip
```

Parent buffering remains inert. The only drain path re-enters the same validation
pipeline (`NakamotoSyncDaemon.scala:342-384`).

### 5.2 Local producer

```text
exact selected executed parent
  -> createProposalArtifact receipt
  -> add only deterministic Nakamoto certificate/eta fields
  -> slot-lineage validation
  -> Ed25519 + registered-period KES sign and self-verify
  -> AuthenticatedExecutedGlobalSnapshot
  -> chainStore.storeValidated
  -> only BecameSelected(branchRevision,lineageRevision) can acquire PreferredExecutedTip
  -> exact branch+lineage canonical-effects stage/readback/prepare/final protocol
  -> publish snapshot
  -> Snowball decision path may emit using PreferredExecutedTip
```

The current producer executes at `SnapshotLeaderLoop.scala:1493-1511`, validates
slot lineage and signs at `:1545-1604`, prepares exact-hash branch state and
stores/selects at `:1605-1659`, runs canonical effects through the current CAS at
`:1700-1713`, and publishes only afterward at `:1746-1765`.

### 5.3 Restart and recovery

```text
stored head bytes
  -> seedUnattestableForRecovery
  -> authenticate exact ancestry/parent/era inputs
  -> full replay and root comparison
  -> storeValidated inserts a new receipt-backed validated entry
  -> remove the matching physically separate recovery seed
  -> current branch/lineage check
  -> PreferredExecutedTip may be acquired only for the exact selected tip
  -> inspect durable attestation journal for the exact hash/decision
       -> pending/unknown: retry only identical bytes after fresh exact-tip preference
       -> published: replay receipt + fresh canonical-lineage check may restore tally
       -> orphaned/stale-lineage: retain historical status, zero active tally
```

Missing retained history enters defer/recovery. It cannot borrow the live head,
current MPT, prior process receipt, durable signed-command bytes, or an
ordinal-only finality watermark.

### 5.4 Descendant extension

```text
branch b, lineage l, selected A, preferred(A,b,l)
  -> objectively validated descendant C
  -> branch b+1, lineage l, selected C
  -> preferred(A,b,l) can no longer authorize publication
  -> already-published evidence for canonical ancestor A remains eligible in l
  -> acquire preferred(C,b+1,l)
```

### 5.5 Replacement

```text
branch b, lineage l, selected A, preferred(A,b,l)
  -> objectively validated replacement B
  -> advance branch to b+1 and lineage to l+1 before B is visible
  -> preferred(A,b,l) and every active tally in l fail
  -> restore/validate B exact state
  -> acquire preferred(B,b+1,l+1)
```

For `A -> B -> A`, returned A requires a new receipt/current-preference
capability under later branch and lineage revisions; the pre-B capability never
revives.

## 6. Re-attestation ticker and Snowball integration

The former five-second raw best-tip re-attestation ticker was not the target
optimistic protocol and is now removed. The owner direction recorded in
`docs/review/CONSENSUS-OWNER-DECISIONS-ANSWERS.md:108-114,125-127` is emit-once
at the first beta-clear, subject to O-01 implementation and proof gates.

Do not restore that ticker or add a raw-capability cache to recreate it. The
staged landing rules are:

1. the current containment exposes no local GL0 attestation signing path;
2. future emission requires both replay and exact-tip preference capabilities;
3. the ratified real sampled cascade, not a periodic best-tip observation, is the
   emission source;
4. canonical `k1` depth remains the sole live state-changing finality rail while
   optimistic activation is blocked;
5. verified remote attestations remain telemetry only; and
6. the legacy cumulative-weight sink cannot be relabeled `T_weight`.

## 7. File plan

### Phase A - RED boundary tests

- Add
  `modules/dag-l0/src/test/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/GlobalTipAttestationEmitterSuite.scala`.
- Extend `CatchUpVerificationSuite.scala`, `NakamotoChainStoreSuite.scala`, and
  `TipAttestationWireValidationSuite.scala` with the tests in section 8.
- Tests must fail before any production API change.

### Phase B - Opaque execution and authentication receipts

**Partial:** lower issuer-reference-checked proposal and replay receipts are live,
producer and validator paths consume them, and the public `Valid` result is removed. The
full authenticated-executed capability remains open: bind the exact decorated
signed bytes, proofs/hash era, envelope, KES/VRF/eta/registry evidence, exact
parent/context/state root, and successful lower receipt into one issuer-owned
object before any storage/preference/signing authority is granted.

- **Landed lower boundary:** `GlobalSnapshotConsensusFunctions.scala` has
  Scala-source-opaque, per-instance issuer-checked receipt-returning producer and
  validator methods without changing economic transition semantics. This assumes
  the validator JVM does not load hostile reflection/agent/modified bytecode.
- **Open authenticated boundary:** `NakamotoSnapshotValidator.scala` no longer
  exposes publicly constructible `Valid`, but the target sealed
  authenticated-executed capability must still place outer envelope, KES,
  registry, eta, parent, and replay gates inside one minting operation.
- **Landed lower boundary:** `NakamotoSyncDaemon.scala` carries the validator-owned
  replay result through `commitReplayValidated`; it does not reconstruct replay
  success from protobuf fields. The future store boundary must consume the full
  authenticated-executed capability, not this lower result.

### Phase C - Selected-tip revision and store outcomes

**Partial containment landed:** typed `StoreOutcome`, an internal mutation lock,
in-memory monotone branch/lineage revisions, and exact selected-tip finalization
CAS are live. Reset advances revisions in-process instead of resetting them.
These revisions restart at zero and persistence is not crash-atomic, so they
cannot authorize signing or satisfy Phase C.

- `NakamotoChainStore.scala`: add the persisted monotone branch and lineage
  revisions, validated storage,
  explicit unattestable recovery seed, typed `StoreOutcome`, validated-duplicate
  retry/reconciliation only for an already receipt-backed exact entry,
  preferred-tip acquisition/recheck, and the package-private short lineage
  transaction used by `recordPublishedIfCanonical`. Recovery is never a duplicate
  upgrade: exact replay inserts a new validated entry, then removes its matching
  physically separate seed. The lineage transaction owns the exact
  canonical-membership/lineage comparison and active-tally mutation as one
  linearization point; it never spans signing, persistence, or network I/O.
- Add a distinct revision-tagged canonical-projection journal, `planDigest`, and
  `stagedContentDigest` with the stage/readback/prepare/final order in RTA-006A.
  The current selection-lock callback is only temporary stale-write containment;
  its type does not bound the effect set and its clearable production pause is not
  a durable fail-stop. Before even temporary reliance, source/API tripwires must
  restrict it to bounded local effects and a partial error must enter a durable,
  later-success-proof `RecoveryRequired` latch. The activation target removes the
  arbitrary callback, stages expensive bytes outside the lock, and uses short
  prepare/final CAS operations for atomic canonical visibility. Signing and
  network I/O run under neither CAS.
- Increment branch revision on every selected-head mutation. Increment lineage
  revision only on rollback, replacement, removal, substitution, clearing, or
  recovery reconstruction. Bound
  retained replay receipts with the same lifecycle as their exact stored entry.
  A lineage-changing transition invalidates the prior lineage's active tally
  before the new lineage becomes observable; pure extension preserves tally
  entries for exact hashes that remain canonical.
- Do not use `getByOrdinal`, unordered map selection, or ordinal equality to mint
  current preference.

### Phase D - Capability-only emitter and evidence cleanup

- Add `GlobalOptimisticAttestationEmitter.scala` in the GL0 Nakamoto package.
- Add a package-owned durable `GlobalOptimisticAttestationJournal.scala` (exact
  name may follow the finality journal convention) under the L-23 effect-journal
  ownership boundary. It stores the idempotency key, exact signed bytes,
  publication outcome, exact hash, branch revision, and lineage revision; it is
  historical evidence/outbox state, never a capability factory.
- **Containment landed:** naked `emitAttestation`/`emitTipAttestation` signing
  APIs, all `publishAttestation` calls in the GL0 leader/receive paths, the
  periodic raw best-tip ticker, and receiver-invented producer evidence are
  deleted. The source/API tripwire is
  `GlobalOptimisticFinalityContainmentSuite.scala:31-52`.
- **Containment landed:** the legacy cumulative-weight finalization API and sink
  are deleted; canonical `k1` depth is the sole current `chainStore.finalizeSelectedAt`
  caller. This is a temporary safe restriction, not Phase D completion.
- Enforce durable pre-publish command persistence, immutable retry,
  `PublishResponse.ok`, durable publication outcome, post-publish
  canonical-lineage recheck, and the chain-store-owned lineage-scoped active-tally
  CAS. `TipTracker` exposes
  no public mutation from historical evidence; only that short transaction may
  install or invalidate current-lineage weight. It must not treat historical
  journal presence as current weight.
- Restart recovery must treat every journal command as inert until the exact
  snapshot is replayed. Retry requires fresh exact-tip preference under both
  revisions; an already-published command may regain tally weight only after a
  fresh atomic canonical-membership check under the current lineage. An orphaned
  or stale-lineage command is retained for audit/deduplication with zero active
  tally and is not re-signed.
- `SnapshotLeaderLoop.scala`: consume typed store outcomes; an alternate local
  proposal cannot update canonical storage or attest.
- `GlobalSnapshotConsensus.scala`: construct one shared emitter/capability service
  and inject it into the leader and receive paths. Current construction sites are
  `GlobalSnapshotConsensus.scala:762-800,1848-1853,2192-2215`.

### Phase E - Real optimistic rail

- Replace the re-attestation ticker and legacy latest-map sink with the ratified
  sampled K/alpha/beta implementation and portable exact-hash `T_weight`
  evidence.
- Bind fixed historical registry/weight/parameter context and the query or
  decision transcript.
- Keep the emitter capability-only. No compatibility overload is permitted.
- Activation remains blocked on O-01's implementation/parameter/proof gates and
  the relevant O-15 construction/proof and finality-lifecycle gates; the type
  boundary may land earlier because it only removes authority. Both O-item
  directions are owner-ratified.

## 8. Mandatory RED and race tests

### API and minting

| ID | Test |
|---|---|
| RTA-RED-001 | `AuthenticatedExecutedGlobalSnapshot` and `PreferredExecutedTip` cannot be directly constructed, copied, decoded, or obtained from a public companion. |
| RTA-RED-002 | The emitter exposes one capability-only `emit`; calls with hash/slot/ordinal or `pb.Snapshot` do not compile. |
| RTA-RED-003 | A fabricated `Accepted`/`Valid` result, signature count, stored hash, best tip, or root cannot mint either capability. |
| RTA-RED-004 | Every envelope, registry, eta, VRF, Ed25519, KES, parent, slot-lineage, active-era, replay, context, state-proof, and root failure yields zero store/sign/publish/tracker effects. |

### Storage, restart, and lineage

| ID | Test |
|---|---|
| RTA-RED-005 | A nonzero restored head is seeded for recovery but cannot attest before exact full replay. |
| RTA-RED-006 | Exact replay of a recovery seed inserts a new receipt-backed validated entry and then removes the separate seed. Raw `Duplicate`, different proofs/hash era/context, or partial validation grants no projection or preference authority. |
| RTA-RED-007 | A replay-valid alternate branch is stored but cannot mint `PreferredExecutedTip`. |
| RTA-RED-008 | Reorg between capability acquisition and final publication check produces no signature publication or local record. |
| RTA-RED-009 | `A -> B -> A` rejects the old A revision. Only newly reacquired, revalidated A authority can emit. |
| RTA-RED-010 | Pure extension advances branch revision and invalidates the old exact-tip capability without advancing lineage or deleting already-published evidence for a still-canonical ancestor. Clearing, rollback/removal, and `unsafe_clearFinality` advance lineage, invalidate old tally/capabilities, and neither revision resets. |
| RTA-RED-011 | Concurrent local production and better gossip selection returns `StoredAlternate` for the loser; it performs no canonical-head write and no attestation. |
| RTA-RED-011A | Preferred token for A is acquired, B replaces A, then A's canonical-effects path resumes. The exact branch+lineage CAS returns stale and no A projection becomes canonical. Any temporary-callback partial effect enters durable `RecoveryRequired`, blocks production/mutation/signing/serving, and cannot be cleared by a later successful callback; the current clearable production-pause reason does not satisfy this test. |
| RTA-RED-011B | Crash or cancellation occurs before/after every staged sink, readback/digest computation, projection-intent prepare, final CAS, and committed marker. Restart exposes the complete prior projection or the complete committed new projection, finishes/rolls back idempotently, or enters recovery; it never infers authority from split sinks. |

### Evidence and publication

| ID | Test |
|---|---|
| RTA-RED-012 | Receiving a valid snapshot does not create an attestation under its producer identity. Only a separately verified remote attestation does. |
| RTA-RED-013 | Ed25519/KES signing failure, local verification failure, cancellation, thrown publish, and `PublishResponse(ok=false)` leave no active-tally entry. Any durable attempt record is nonauthoritative and cannot decode as published evidence or current weight. |
| RTA-RED-014 | With no intervening lineage replacement, `ok=true` durably records exactly the immutable evidence that was published, then a post-publication canonical-hash/lineage CAS adds it once. A descendant extension after the final exact-tip precheck preserves that evidence if the target remains canonical. |
| RTA-RED-015 | Ambiguous publish followed by retry reuses one logical decision, idempotency key, and exact signed bytes; receiver and local deduplication prevent duplicate weight. |
| RTA-RED-015A | Replace after the pre-publication check and before/during/after publish but before the active-tally CAS. The exact command may be durable historical or publication-unknown evidence, but the new lineage receives zero active weight. |
| RTA-RED-015B | Crash before/after command persistence, network send, response, `Published` marking, post-publish check, and active-tally CAS. Restart cannot use journal bytes as authority: it fully replays, then reacquires exact-tip preference for retry or fresh canonical-lineage membership for an already-published tally. Replacement leaves history only and zero active weight. |
| RTA-RED-015C | Extend after the final exact-tip precheck at every publication boundary. The old capability cannot authorize a new send, but successfully published evidence is retained/recorded exactly once when its hash remains canonical and lineage is unchanged. |
| RTA-RED-016 | Hash, ordinal, slot, parent, root, producer, period, branch revision, or lineage revision substitution is impossible or rejected before signing. |
| RTA-RED-017 | Buffered children and catch-up snapshots re-enter the complete validation path; parent arrival alone cannot mint authority. |

### Protocol-role regression

| ID | Test |
|---|---|
| RTA-RED-018 | No new global proposal/vote/lock/QC/view-change type or transition exists; attestation emission cannot call finalization directly. |
| RTA-RED-019 | With optimistic activation dark, canonical `k1` depth continues to progress independently. |
| RTA-RED-020 | Legacy cumulative weight cannot satisfy the new decided `T_weight` input type. |

Use controlled `Deferred` barriers around selection, signing, journal persistence,
pre-publication branch/lineage check, network send, response, publication marking,
post-publication canonical-lineage check, and active-tally CAS so the races are
deterministic rather than timing-based. Run the same boundary matrix with process-restart fault
injection against the durable journal.

## 9. Completion and activation gates

The landed containment closes the immediate raw-signing, alternate-as-canonical,
stale depth-target, and legacy weight-to-finalization paths. It does not satisfy
this section's completion criteria: there is intentionally no local optimistic
emitter until the remaining opaque capability, durable revision/journal,
publication CAS, and sampled Snowball work lands.

The replay-before-sign slice is complete only when:

- every GL0 optimistic signing call graph terminates at the capability-only
  emitter;
- both local-producer and received-snapshot capabilities originate at complete
  local reproduction;
- restart, alternate branches, reorg, and ABA cannot reuse stale preference;
- pure descendant extension invalidates old exact-tip emission authority while
  preserving already-published evidence for still-canonical ancestors;
- receiver-invented evidence is gone;
- durable historical publication/outbox state is structurally separate from the
  current-lineage active tally;
- publication failure, ambiguity, crash, or post-check replacement cannot create
  local-only or stale-lineage active weight;
- every restart retry requires full exact replay plus fresh exact-tip preference,
  and every tally completion requires full exact replay plus fresh canonical-lineage
  membership, never only persisted command bytes;
- exact signed commands, publication outcomes, and active-tally effects are
  idempotent and crash-tested at every boundary;
- no raw or compatibility signing API remains; and
- all RTA RED tests pass.

That completion does **not** activate optimistic Phase 2. Activation additionally
requires the real O-01 sampled Snowball implementation, fixed historical
population/weight and parameter context, portable decided `T_weight` evidence,
objective valid-tine selection, exact-hash FinalityGate state, and reversible
replacement handling. Until then, the capability is a safety restriction and the
optimistic rail remains non-production or dark.
