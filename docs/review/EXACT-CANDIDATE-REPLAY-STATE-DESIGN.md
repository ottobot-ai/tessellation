# Exact Candidate Replay State Design

**Status:** DESIGN ONLY. This document defines a target authority boundary and
migration order. It is not a claim that the current runtime satisfies the
design. The current checkpoint, finality, replay-state, and restart paths remain
transitional.

**Scope:** GL0 candidate construction and validation, exact historical Phase-2
reads, execution-shard replay, ordinary checkpoint adoption, and restart
reconstruction.

**Dependencies:** The locked layer and economic-authority rules in `AGENTS.md`,
the artifact lifecycle in `CONSENSUS-ARTIFACT-LIFECYCLE.md`, the Phase-2 lease
contract in `P6-FIN14-PHASE2-CONSUMER-LEASE.md`, and the replay-before-tip
capabilities in `REPLAY-BEFORE-TIP-ATTESTATION-DESIGN.md`.

## 1. Locked execution split

This design does not change the network topology:

```text
native DAG path:   client -> GL1 -> GL0
metagraph path:    CL1/DL1 -> ML0 -> execution shard -> GL0
state return:      exact Phase-2 GL0 state -> GL1/ML0/CL1/DL1
```

The execution split is fixed:

1. **Native GL1 and the global kernel remain universal.** Every GL0 validator
   independently executes and validates all native DAG-token blocks and direct
   global protocol events. Every GL0 validator also runs the deterministic
   global conflict, nullifier, settlement, correction, and acknowledgement
   kernel.
2. **Sharded CL1 framework execution is replay-certified.** The checkpoint
   producer, every execution-committee signer, assigned watchtowers, and an
   exceptional adjudicator independently replay the exact ordered framework
   inputs at exact Phase-2 bases.
3. **An ordinary noncommittee GL0 validator does not recreate the CL1
   transition.** It verifies the execution certificate and positive watchtower
   coverage, exact base and continuity, namespace and compare-and-set
   invariants, applies the certified canonical diff, and independently
   recomputes the resulting root. It then runs the universal global kernel over
   certificate-bound extracted intents.
4. **DL1 custom bytes remain isolated data/availability carriage.** They cannot
   synthesize or authorize a framework-economic effect.
5. **`numShards = 1` means one execution shard.** It does not authorize a
   direct/unreplayed economic path.

The current global validator already reconstructs native/global inputs:
`GlobalSnapshotConsensusFunctions.scala:208-299` extracts DAG blocks and global
events at `:216-244`, recreates the candidate at `:261-280`, and compares the
exact artifact at `:282-299`. Native block and global protocol processing enter
GSAM at `GlobalSnapshotAcceptanceManager.scala:720-771,1889-1939`, and the
global nullifier engine runs at `:2372-2387`. These responsibilities must not be
removed when ordinary noncommittee GL0 recreation of **sharded CL1** framework
transitions is removed. That optimization never removes universal native
GL1/DAG-token execution from any GL0 node.

The live shard path is still transitional. `ShardDerivedStateDelta` carries
roots and signed inputs but no canonical byte diff
(`ShardDerivedStateDelta.scala:17-38`). `ShardCheckpoint` carries only an
ordinal execution base, not a complete exact Phase-2 reference
(`ShardCheckpoint.scala:14-25,49-77`). Consequently, every GL0 currently calls
`deriveAdoptedCurrencyState` and recreates adopted currency snapshots
(`GlobalSnapshotAcceptanceManager.scala:1034-1085,2083-2117`). That recreation
must remain until the complete diff-adoption gate lands, then remain available
only to the producer, execution signers, assigned watchtowers, and exceptional
adjudication. **Target rule:** every replay in those roles uses the exact retained
base and complete ordered inputs; missing data defers and cannot sign or slash.

## 2. Threats and invariants

The reader and capability boundary must withstand:

- same-ordinal competing forks;
- a malicious producer carrying a wrong root, base, context, or diff;
- a colluding execution quorum;
- stale or asymmetric best-tip, cache, live-store, and finality views;
- replay after a Phase-2 density replacement, including `A -> B -> A`;
- restart with only a persisted post-genesis head;
- retention loss deeper than the local rollback window;
- missing data during watchtower or slash adjudication;
- concurrent branch mutation while a candidate is executing; and
- resource-exhaustion references to old or oversized state.

The enforcing invariants are:

### RST-1 Exact parent identity

Consensus replay is scoped to one complete `GlobalSnapshotStateRef`:
`(ordinal, hash, parentHash, mptRoot)`. Ordinal, best tip, storage head, or a
caller-supplied context cannot substitute for that identity.

`GlobalSnapshotStateRef` is only a structural claim, not authority
(`GlobalSnapshotStateRef.scala:7-17`).

### RST-2 One immutable input image

Every read during one replay comes from one defensively owned immutable image.
The image independently reproduces `mptRoot`, passes the active physical grammar
and semantic/economic verifier, and cannot change during execution. Root
calculation and typed reads cannot observe different overlay generations.

### RST-3 Artifact, context, and state are one receipt

The signed snapshot, exact reference, locally reproduced `GlobalSnapshotInfo`,
semantic receipt, and MPT image are bound by one sealed
`AuthenticatedExecutedGlobalSnapshot`. Disk-supplied context is recovery data,
not execution evidence.

### RST-4 No state-validity signature without replay

The checkpoint producer and every execution signer obtain signing authority only
from a locally reproduced exact diff and root. Assigned watchtowers use the same
replay function and independently selected scope. Missing input means
defer/no-sign.

### RST-5 Exact historical Phase-2 reads

Every framework binary's global read resolves its signed complete reference to an
exact, still-canonical Phase-2 released core on the candidate parent's lineage.
Each binary in a metagraph segment may name a different nondecreasing Phase-2
reference. Receiver live head and ordinal caches are never inputs.

### RST-6 Missing data is not invalidity

Unavailable ancestry, image, witness, or context produces a typed defer or
`RecoveryRequired`. It cannot produce a state-validity signature, checkpoint
inclusion, false slash, head fallback, or an invalid-candidate verdict based only
on local retention.

### RST-7 Local retention has no fork-choice authority

`k2` is a recommended local retention and automatic rollback horizon only. It is
not finality, a common-prefix floor, a reference-validity bound, or permission
for an operator to select a branch.

### RST-8 Startup cannot manufacture execution authority

A fresh complete genesis may be installed through one issuer-private pinned
genesis capability. It is a distinct one-shot authority type: it is not an
`AuthenticatedExecutedGlobalSnapshot`, is not a `PreferredExecutedTip`, and has
no conversion to either. Every persisted post-genesis head is unattestable
recovery input until exact forward replay reproduces it. Persisted bytes never
enter validated ancestry, selected tip, fork choice, finality, serving, or
signing merely because startup found them.

### RST-9 Native GL1 validation cannot be bypassed by a shard certificate

A valid execution certificate or diff never authorizes a native DAG block,
global protocol event, or global settlement result. Every GL0 validator still
recreates those transitions.

## 3. Opaque capability model

The following shapes are pseudocode. Authority types are sealed, have concrete
implementations private and nested inside their sole issuer object, are not case
classes, expose no `copy`, and have no Circe, Kryo, Scodec, protobuf, Java, or
other serialization instance. A top-level `private[nakamoto]` concrete class is
not sufficient because unrelated code in that package could construct it.

```scala
sealed trait ReplayStateFailure

object FreshGenesisExecution {
  sealed abstract class PinnedCompleteGenesisExecution private (
    val stateRef: GlobalSnapshotStateRef
  )

  // Sole concrete implementation and allocation site remain private here.
  private final class IssuedPinnedCompleteGenesisExecution(...)
      extends PinnedCompleteGenesisExecution(...)
}

sealed trait ExactGlobalStateView[F[_]] {
  def ref: GlobalSnapshotStateRef
  private[nakamoto] def signed: Signed[GlobalIncrementalSnapshot]
  private[nakamoto] def context: GlobalSnapshotInfo
  private[nakamoto] def state: GlobalStateReader[F]
  private[nakamoto] def image: SortedMap[Hex, ByteVector]
  private[nakamoto] def semanticReceipt: ScopedArtifactRef
}

object ExactGlobalStateViewIssuer {
  private final class IssuedExactGlobalStateView[F[_]](...) extends ExactGlobalStateView[F]
}

sealed trait CandidateParentReplayView[F[_]] {
  def executed: AuthenticatedExecutedGlobalSnapshot
  private[nakamoto] def parentState: ExactGlobalStateView[F]
  private[nakamoto] def history: CandidateLineageReader[F]
}

object CandidateParentReplayViewIssuer {
  private final class IssuedCandidateParentReplayView[F[_]](...) extends CandidateParentReplayView[F]
}

sealed trait OperationalGlobalStateView[F[_]] {
  def lease: CanonicalPhase2Lease
  private[finality] def exact: ExactGlobalStateView[F]
}

object OperationalGlobalStateViewIssuer {
  private final class IssuedOperationalGlobalStateView[F[_]](...) extends OperationalGlobalStateView[F]
}

trait CandidateLineageReader[F[_]] {
  def artifactAt(
    ordinal: SnapshotOrdinal
  ): F[Either[ReplayStateFailure, ExactAncestorArtifact]]

  def operationalAt(
    scope: Phase2UseScope,
    ref: GlobalSnapshotStateRef
  ): F[Either[ReplayStateFailure, OperationalGlobalStateView[F]]]
}

trait CandidateParentReplayStateReader[F[_]] {
  def openValidated(
    parent: AuthenticatedExecutedGlobalSnapshot
  ): F[Either[ReplayStateFailure, CandidateParentReplayView[F]]]

  def openSelected(
    parent: PreferredExecutedTip
  ): F[Either[ReplayStateFailure, CandidateParentReplayView[F]]]
}
```

`openSelected` is producer-only: it additionally verifies the exact selected-tip
and branch/lineage revisions. Intake validation uses `openValidated`, because a
valid candidate may extend a retained, fully executed alternate parent before
objective fork choice selects it.

Opening a view performs these checks over the same immutable inputs:

1. Rehash the exact signed body under its active hash era.
2. Match hash, ordinal, signed parent, and state-proof MPT root to `ref`.
3. Resolve an exact branch image; an unknown branch is an error, not base.
4. Independently rebuild `ref.mptRoot`.
5. Validate the complete active-era physical grammar and economic semantics.
6. Match the context and semantic receipt minted by the parent's complete local
   execution.
7. Capture a candidate-parent-scoped ancestry resolver.

No overlay, finality, chain-store, or consumer lock remains held while replay,
DA fetch, signature production, or committee waits run. The immutable view is
the long-work input; final storage, signing, tally, or inclusion uses the
appropriate revision compare-and-set.

`operationalAt` additionally:

- proves the complete requested reference is on this candidate lineage;
- acquires the exact purpose-scoped `CanonicalPhase2Lease`;
- verifies current-chain and Phase-2 qualification evidence;
- verifies released-core image, semantic, and authenticated-anchor readbacks;
- returns a reader over that exact immutable image; and
- requires `commitIfCurrent` before any authority-bearing derivative is
  recorded, published, tallied, or applied.

A Boolean, `Option`, ordinal watermark, decoded state reference, or image-file
presence cannot mint any of these capabilities.

## 4. Exact source mapping

| Required input | Current source and evidence | Defect | Target source |
|---|---|---|---|
| Candidate artifact ancestry | `NakamotoChainStore.walkBackExact` is a bounded, exact hash-linked data walk (`NakamotoChainStore.scala:338-342,1018-1189`). | The walk returns artifact links only. It does not authenticate a context or MPT image, and callers must not restart one full walk for every ordinal read. | Keep a bounded, memoized exact artifact walk inside `CandidateLineageReader`; never promote it into a state/context capability. |
| Live tentative parent image | `MptOverlay.captureBranchImage` captures bytes (`MptOverlay.scala:286-292,953-960`). | It explicitly does not authenticate a branch; an unknown branch falls through to base (`:40-43,671-685,897-919`). `BranchEntry` stores only parent, changes, and ordinal (`:421,781-799`). | Add strict exact branch registration and capture keyed by parent and child `GlobalSnapshotStateRef`; unknown or mismatched branch fails. |
| Candidate context | `StoredSnapshot` carries a raw `GlobalSnapshotInfo` (`NakamotoChainStore.scala:33-42`), and raw `store` accepts caller metadata (`:234-246`). | Stored context can be selected without a sealed local execution receipt. | Context comes only from `AuthenticatedExecutedGlobalSnapshot`; restart must reproduce it. |
| P2/restart state image | `DurableMptImageStore` can prepare and read an image bound to full `GlobalSnapshotStateRef` (`DurableMptImageStore.scala:30-48,50-104,160-175,748-798`). | The class states that runtime integration is dark and semantic/finality authentication remains external. | Released finality core names the exact receipt, semantic receipt, and authenticated anchor; the reader verifies all three. |
| Transitional retained bytes | `MptStateStorage` filenames are only the ordinal (`MptStateStorage.scala:40-59`). | Cannot retain or select same-ordinal siblings and has no hash/root manifest. | Cache/offline-import input only; never authority. |
| Pre-release staged bytes | `pendingPostBytesRef` is hash-keyed but volatile (`GlobalSnapshotConsensus.scala:358-367`) and promoted/pruned by ordinal finality (`SnapshotLeaderLoop.scala:632-655`). | Mutable arrays, no durable receipt, and old same-height branch images are removed. | Optional inert optimization only; exact authority comes from an immutable branch image or durable released image. |
| Global P2 status | `FinalityGate` exposes only ordinal watermark methods (`FinalityGate.scala:23-30,44-50`). | Cannot express same-ordinal replacement, per-hash phase, or reorg invalidation (`:9-19`). | Exact FinalityGate/released-core lease over `GlobalSnapshotStateRef`. |
| Currency execution base | `PinnedCurrencyInfoReader.pinnedReaderAt` self-resolves a hash by ordinal (`PinnedCurrencyInfoReader.scala:265-303`). | Resolver can select a sibling; retained bytes are ordinal-keyed. | `OperationalGlobalStateView` opened from the full signed execution-base reference. |
| Historical currency global reads | Currency replay reads live global head and `LastN` state (`CurrencySnapshotAcceptanceManager.scala:300-324,362-395,446-474`) and ordinal caches (`GlobalSnapshotOpsManager.scala:29-55,117-134,152-162`). | Head/cache timing can change message validation and SpendActions. | Every read uses the binary's `operationalAt` view; no callback or cache bypass. |
| Cross-metagraph owner read | GSAM builds one ambient finalized-base reader (`GlobalSnapshotAcceptanceManager.scala:621-624`) and uses it in the cross-shard validator (`:2293-2302`). | It is not bound to the signed owner reference or candidate lineage. | `operationalAt(CrossMetagraphSettlement, signedOwnerRef)` for owner state; candidate parent state for nullifier/CAS writes. |
| Producer parent | Leader reads selected stored snapshot/context and supplies an exact artifact callback (`SnapshotLeaderLoop.scala:1442-1518`). | Exact artifact lookup does not bind the MPT image and context. | `openSelected(PreferredExecutedTip)`. |
| Receiver parent | Sync resolves the parent hash and supplies stored context plus exact artifact callback (`NakamotoSyncDaemon.scala:1455-1499`). | Same missing image/context receipt; raw storage remains authority. | `openValidated(AuthenticatedExecutedGlobalSnapshot)`. |
| GSAM parent reads | GSAM uses a mutable dynamic branch reader and `parentTip` (`GlobalSnapshotAcceptanceManager.scala:1685-1775`). | Reader identity is not an immutable exact-ref capability, and some managers still read raw context. | One immutable candidate replay session derived from `CandidateParentReplayView`. |

## 5. Candidate execution session

The reader is read-only. Candidate writes must be derived from the same frozen
parent image:

```scala
final class PreparedCandidateState private[nakamoto] (
  val parent: GlobalSnapshotStateRef,
  val childRoot: MptRoot
)

trait CandidateExecutionSession[F[_]] {
  def view: CandidateParentReplayView[F]
  def writer: AcceptanceMpt[F]
  def prepare: F[Either[ReplayStateFailure, PreparedCandidateState]]
}
```

The implementation may build an isolated in-memory MPT from the captured parent
image, accumulate a canonical change set, and derive the post-image once. It
must not read a mutable overlay generation after capture. Only complete envelope,
signature, KES/VRF, artifact-equality, transition, and root validation can bind a
`PreparedCandidateState` to an `AuthenticatedExecutedGlobalSnapshot` and install
it as a branch.

This removes the current ordering in which validation commits/rekeys an overlay
branch before it returns a sealed execution capability
(`NakamotoSnapshotValidator.scala:230-279`). It also prevents an unknown
`BranchId` from silently seeding a child from the finalized base.

## 6. Execution-shard capabilities

### 6.1 Replay-capable roles

The checkpoint producer, every execution signer, each assigned watchtower, and
an adjudicator use the same deterministic replay entry point:

```scala
final class ShardExecutionReplayView[F[_]] private[sharding] (
  val scope: Phase2UseScope,
  val executionBase: OperationalGlobalStateView[F],
  private[sharding] val history: CandidateLineageReader[F]
)
```

The view binds:

- checkpoint shard, hash, exact ordered signed input list, and active eras;
- complete exact Phase-2 execution base;
- touched-MG parent mirror `preRoot/version`;
- each binary's complete, nondecreasing historical Phase-2 reference;
- exact optional framework replay witnesses, including the field-32 replacement
  witness required by L-15A/O-17;
- explicit ML0 operator population used by witness validation; and
- one purpose and lineage revision.

Replay returns a canonical namespace-confined byte diff, complete post-root,
updated MG mirror version, and signed extracted global intents. The signature
preimage binds all of them. A signer cannot obtain a signature capability from
receipt, best-tip selection, signature count, or another member's result.

### 6.2 Ordinary noncommittee adopter

An ordinary GL0 validator receives no `ShardExecutionReplayView` and does not
call `CurrencySnapshotCreator` or `deriveAdoptedCurrencyState`. It must verify:

1. exact active execution committee and distinct replay signatures;
2. registered KES/VRF pairs and all signature preimages;
3. positive assigned-watchtower replay coverage;
4. exact canonical Phase-2 execution base and current lease scope;
5. metagraph-to-shard ownership and complete input ordering;
6. parent checkpoint, shard ordinal, and one-outstanding-checkpoint continuity;
7. touched-MG `preRoot/version` compare-and-set;
8. diff key grammar and strict MG namespace confinement;
9. exact diff application and recomputed post-root; and
10. complete extracted-intent commitment.

It then runs the global conflict/settlement kernel over those extracted intents.
That kernel alone can write global nullifiers, settlements, corrections, and
acknowledgements. A per-MG diff cannot write another MG or a global partition.

If any input is missing, the checkpoint waits while unrelated native GL0 work
and other shards progress. A certificate never converts missing data into
validity. Watchtower assertion alone never slashes; adjudication replays exact
inputs and missing data defers.

## 7. Genesis and restart

### 7.1 Fresh complete genesis

`PinnedCompleteGenesisExecution` is a one-shot exception, not a generic
constructor. It is distinct from, and non-convertible to, both ordinary
authenticated-execution and preferred-tip capabilities. Its issuer mints it only
when all of these match one configured network genesis identity:

- exact signed genesis bytes and canonical genesis hash;
- exact initial context and complete MPT image;
- reproduced state root and active semantic grammar;
- network, protocol era, and parameter identity; and
- complete committed genesis operator/KES/VRF registry.

Only `installGenesis(PinnedCompleteGenesisExecution)` may initialize an empty
validated chain store without a parent replay. `installGenesis` does not expose a
general receipt conversion, and the pinned origin cannot authorize an optimistic
attestation or masquerade as a post-genesis replay result.

**OPEN OWNER DECISION — GEN-PIN-01.** The authoritative commitment for the
complete configured network genesis identity is not frozen. In particular,
`L0GenesisProtocolParams` is currently documented as informational rather than a
consensus input (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/genesis/types.scala:54-77`). The owner must
choose the normative whole-manifest/parameter commitment and signing rule before
this capability can be production authority. Candidate directions include a
normative hash of the complete genesis manifest or a separately signed offline
manifest that binds the exact ordinal-1 artifact/state reference, network and
activation identity, complete live consensus parameters, and rooted operator
registry. The issuer must independently recompute and compare the configured
genesis; it may not bless arbitrary bytes merely because `Main` just wrote them.

### 7.2 Persisted heads are recovery-only

Two current startup paths read a persisted head, treat it as genesis/recovery
authority, and raw-select it through `chainStore.store`:

- `GlobalSnapshotConsensus.scala:1144-1178`, especially `:1153-1165`; and
- `SnapshotLeaderLoop.scala:693-719`, especially `:697-712`.

Neither path distinguishes deterministic fresh-genesis construction from a
post-genesis disk restore. Both violate the RED contract in
`RestartAuthorityRedSuite.scala`.

The replacement rule is:

1. Classify a persisted post-genesis head with
   `seedUnattestableForRecovery`.
2. Restore the newest exact authenticated released-core image, or the pinned
   complete genesis if no released core exists.
3. Fetch and authenticate exact descendant artifacts and ancestry.
4. Forward replay each descendant:
   - universally execute native GL1 and all global protocol transitions;
   - verify and apply certified CL1 diffs, then universally run the global
     kernel;
   - validate isolated DL1 commitment/availability carriage;
   - reproduce exact artifact, context, and state root.
5. Mint one sealed authenticated-executed receipt per successful child.
6. Enter validated storage and objective fork choice only through the validated
   store path.

Recovery storage is physically and logically separate from validated chain
state. A recovery seed cannot be returned by validated-parent, selected-tip,
eta, finality, fork-choice, Phase-2 serving, or signing APIs. Only exact forward
replay may remove the matching recovery seed and create a new ordinary
authenticated-execution receipt.

Persisted snapshot/context bytes, old v4 disk history, or a peer bundle may
supply recovery data. None is live authority without the same exact
authentication and replay. Historical format support belongs in an offline
typed import, not an active abandoned-schema decoder.

## 8. Retention, bounded work, and recovery

### 8.1 `k2`

The recommended `k2 = 100 * k1` controls local retention of exact artifacts,
images, undo data, proof data, and replay inputs. It does not:

- make a snapshot irreversible;
- reject a denser valid tine;
- validate an old reference;
- authorize a nearest-ordinal or current-head fallback; or
- let an operator choose the winner.

If an objective comparison or referenced exact state exceeds local retention,
the node enters `RecoveryRequired`, stops production, mutation, signing, and P2
serving, fetches exact hash-addressed history/state, verifies it, reconstructs
the ordinary comparison, and resumes only after readback succeeds. The current
overlay correctly refuses to trust ordinal-only bytes for deep recovery
(`MptOverlay.scala:1284-1324`).

### 8.2 Work bounds

Normal-path work is bounded by rooted active-era protocol parameters for:

- candidate bytes, events, checkpoints, MGs, binaries, and diff entries;
- physical key/value sizes and total image work;
- replay steps and witness/DA bytes; and
- signature, committee, and watchtower populations.

Local HOCON, wall clock, cache depth, or disk availability cannot decide
consensus validity. A protocol-invalid oversized artifact is rejected under the
same rooted parameter set by every node. A locally unavailable but
protocol-valid artifact enters fetch/recovery. Historical references are
deduplicated and resolved through a candidate-scoped exact path/index rather
than repeating an unbounded ordinal walk.

## 9. Migration order

1. **Contain restart authority.** Remove the raw startup selection path. Add
   pinned complete-genesis and recovery-only head tests.
2. **Seal execution authority.** Land
   `AuthenticatedExecutedGlobalSnapshot`, `PreferredExecutedTip`,
   prepared-replay results, and a validated-only chain-store entry point.
3. **Make branch state exact.** Add strict exact branch metadata/capture, one
   MPT mutation owner, immutable candidate execution sessions, and missing-branch
   failure instead of base fallback.
4. **Migrate GL0 producer and validator.** Replace
   `lastArtifact + lastContext + parentTip + ordinal callback` with
   `CandidateParentReplayView`. Preserve universal native/global execution.
5. **Activate exact released images dark.** Integrate
   `DurableMptImageStore`, semantic receipts, authenticated anchors, and
   released-core readback. Do not issue live leases until the finality gates
   below close.
6. **Upgrade signed schemas.** Replace incomplete `GlobalSyncView` and
   `executionBaseOrdinal` authority with complete `GlobalSnapshotStateRef`;
   add canonical diff, `preRoot/version`, post-root, extracted intents, complete
   inputs, and watchtower coverage.
7. **Purify shard replay.** Route producer, every execution signer,
   watchtowers, and adjudication through `ShardExecutionReplayView`; delete
   live-head, `LastN`, ordinal-cache, and Boolean P2 adapters from replay.
8. **Land ordinary diff adoption.** Verify/apply the certified diff and run the
   universal global kernel. Only then remove ordinary noncommittee GL0
   recreation of **sharded CL1** currency transitions from ordinary checkpoint
   adoption. Every GL0 node continues to execute native GL1/DAG-token blocks and
   direct global protocol events. Route one-shard operation through the same
   sharded-CL1 path.
9. **Land exact restart/reorg recovery.** Exact-forward replay from released
   core, hash-bound invalidation, `commitIfCurrent`, and ordered reorg effects.
10. **Delete transitional authority paths.** Remove ordinal-only phase checks,
    self-resolving pinned readers, raw chain-store authority, and ordinal caches
    in one activation change after all RED and chaos gates pass.

## 10. Mandatory RED and chaos matrix

| ID | Schedule | Required result |
|---|---|---|
| ECR-RED-001 | Construct, copy, decode, or deserialize an authority capability outside its package. | Does not compile or decode. |
| ECR-RED-002 | Two siblings have the same ordinal and different hashes/images. | Each candidate sees only its exact parent and history; no ordinal lookup crosses branches. |
| ECR-RED-003 | Open an unknown overlay branch. | Typed `MissingBranch`; never base state. |
| ECR-RED-004 | Mutate/reorg the base during immutable replay. | Replay remains internally immutable; final install/sign/commit CAS returns stale. |
| ECR-RED-005 | Restore an arbitrary persisted post-genesis head. | It remains unattestable and cannot select, produce, finalize, sign, or serve P2. |
| ECR-RED-006 | Install fresh genesis with wrong hash, root, context, registry, or local key pair. | Startup fails before chain authority exists. |
| ECR-RED-007 | Restart above the newest retained P2 image. | Exact forward replay reproduces every child before selection or production. |
| ECR-RED-008 | Poison `SnapshotStorage`, `LastN`, manager cache, or live head with a same-ordinal sibling. | Candidate and shard replay outputs are unchanged. |
| ECR-RED-009 | Replace P2 `A` with `B`, then select `A` again. | Old A lease, replay, signature, tally, cache, and command remain invalid; fresh acquisition is required. |
| ECR-RED-010 | Pure descendant extension occurs during historical-ancestor work. | Purpose policy rechecks exact ancestry; latest-head work reacquires. |
| ECR-RED-011 | Required state is absent inside or beyond local `k2`. | Defer/recovery, no fallback, invalidity, signature, or slash. |
| ECR-RED-012 | One MG window contains multiple binaries with different nondecreasing P2 references. | Each global read uses its own signed exact reference while MG-local state threads in order. |
| ECR-RED-013 | Execution signer receives a quorum-signed checkpoint it has not replayed. | It emits no state-validity signature. |
| ECR-RED-014 | Ordinary adopter receives a valid certificate/diff. | No sharded-CL1 currency recreation function is invoked; diff/root/CAS checks and the global kernel do run. Universal native GL1/DAG execution is unchanged. |
| ECR-RED-015 | A candidate contains an invalid native GL1 block plus a perfect shard certificate. | Every GL0 validator rejects the candidate through native replay. |
| ECR-RED-016 | Two certified shards consume the same authorization. | The universal global kernel deterministically records one winner and one permanent nullifier. |
| ECR-RED-017 | A diff writes another MG or a global partition, or has stale `preRoot/version`. | Reject before any canonical-state visibility; isolated staging is discarded. |
| ECR-RED-018 | `numShards = 1`. | Producer/signers/watchtower/certificate/diff/global-kernel path is exercised identically to a shard in K-shard mode. |
| ECR-RED-019 | Field-32 replacement witness changes `None` to `Some(empty)`, population, or one byte. | Producer, signer, watchtower, and adjudicator all reject/defer identically; ordinary adopter cannot interpret it as authority. |
| ECR-RED-020 | Watchtower or adjudicator lacks exact input/base data. | No guilty/innocent verdict and no slash; fetch/defer only. |
| ECR-RED-021 | Crash before/after image prepare, semantic receipt, anchor receipt, branch install, validated store, P2 publication, and reorg tombstone. | Restart exposes either the complete prior state or the complete new state, or enters recovery; never mixed authority. |
| ECR-RED-022 | A valid fork diverges deeper than retained `k2`. | Production stops, exact recovery completes, and ordinary objective fork choice runs; no manual winner or fail-open continuation. |

Chaos tests inject replacement, crash, cancellation, delayed peer data, sibling
arrival, and cache poisoning before and after every read, root rebuild, replay,
signature, tally, diff application, store, publication, and response-boundary
recheck. Assertions include zero unintended economic mutation, state-validity
signature, optimistic attestation, slash, checkpoint inclusion, or downstream
release.

## 11. Research and activation blockers

The API may be implemented dark, but live authority and schema activation remain
blocked by the following owner-recorded work.

### O-15: objective multi-tine selection

The single-common-anchor density/tower-evidence direction is ratified, but the
exact selector, cutoff-complete frontier, density metric/equality, tie rule,
evidence verifier, Byzantine-overflow rule, and proof are not frozen. Until
O-15 closes, a local incumbent/best-tip observation cannot prove current
canonical lineage or issue a live Phase-2 lease.

Authority: `CONSENSUS-OWNER-DECISIONS.md:421-528` and
`CONSENSUS-OWNER-DECISIONS-ANSWERS.md:377-494`.

### O-16: exact Phase-2 lease

Conservative replacement invalidation, raw-byte reverification, and a signed
full reference direction are ratified. The exhaustive purpose policy, concrete
signed scope/codec, branch-authenticated freshness rules, released-core
readbacks, and sink integration remain open. Wrapping the current Boolean or
ordinal watermark in an opaque type is forbidden.

Authority: `CONSENSUS-OWNER-DECISIONS.md:529-592` and
`CONSENSUS-OWNER-DECISIONS-ANSWERS.md:495-506`.

### O-17: complete root grammar

Physical partition ownership and the field-32 removal direction are ratified.
Resource parameters, identity functions, active codecs, complete structural and
economic semantic proofs, permanent-state growth, and exact optional
full-view-witness parity remain open. A self-consistent MPT root cannot mint an
execution or Phase-2 capability until these checks close.

Authority: `CONSENSUS-OWNER-DECISIONS.md:594-632` and
`CONSENSUS-OWNER-DECISIONS-ANSWERS.md:508-545`.

Additional engineering dependencies include one MPT mutation owner, sealed
authenticated-executed receipts, crash-consistent released-core publication,
exact reorg effects, the complete diff schema/verifier, and recovery readback.

No further owner choice is needed for the locked GL1/sharded-CL1 split, exact
hash/root requirement, replay-before-sign rule, missing-history recovery
semantics, or the non-convertible fresh-genesis versus recovery-only restored-head
distinction. Those are implementation invariants, not alternatives. The exact
normative manifest and complete live-parameter commitment that may mint the fresh
genesis capability remains the explicit `GEN-PIN-01` owner decision above.
