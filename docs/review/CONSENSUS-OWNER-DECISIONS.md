# Consensus Owner Decision Register

**Status:** Active. Owner directions in this register and
`CONSENSUS-OWNER-DECISIONS-ANSWERS.md` are ratified for `O-01` through `O-17`. Runtime packets may not silently invent a
missing parameter, schema, reference model, or proof needed to close an engineering freeze gate.
`O-18` was surfaced by the transport/resource audit and awaits an owner response; no implementation
may infer its byte, compression, chunking, retention, or migration choices. `O-19` was surfaced by
the upstream-v4 migration-coverage audit and also awaits an owner response; no implementation may
infer a field disposition, source-authentication exception, registry, epoch/eta mapping, or
conservation rule. `O-20` was surfaced by the field-34 slash-record audit and awaits an owner
response; no implementation may infer a generalized slash-record schema or activate a latent slash
reason. `O-21` was surfaced by the optimistic-finality evidence audit and awaits an owner response;
no implementation may infer that gossip counts or an opaque digest prove a completed sampled cascade.
`O-22` was surfaced by the fraud-proof activation audit and awaits an owner response; no implementation
may give a current-era nonempty proof consensus authority or infer a filter-and-continue adjudication rule.
`O-23` was surfaced by the ECO-06 principal-conservation audit and awaits an owner response; no
implementation may infer slash-time operator-wide liability, partial-lock semantics, or a release horizon.
**Owner-question completeness:** `17/23` dispositioned. `O-01` through `O-17` are ratified;
`O-18`, `O-19`, `O-20`, `O-21`, `O-22`, and `O-23` are pending.
**Updated:** 2026-07-16

This register uses project phases only where the owner has ratified them:
Phase 0 `Pending`, Phase 1 `Provisional`, and Phase 2 `Operational`. `k2` is a
retention/recovery recommendation, not a separate consensus-finality floor.

## Locked decisions

| ID | Decision |
|---|---|
| L-01 | Topology remains `GL1 -> GL0` and `CL1/DL1 -> ML0 -> GL0`, with exact Phase-2 GL0 state flowing back downstream. Every GL0 validator independently executes and validates the direct native GL1/DAG-token transition against the exact proposal parent. Sharded CL1 diff adoption never authorizes, replaces, or bypasses that native execution. |
| L-02 | GL0 consensus is Nakamoto/Taktikos/LDD chain consensus. No global proposal/vote/lock/QC/view-change BFT protocol is permitted. ML0 may retain BFT consensus for its small, well-connected metagraph validator set and remains subordinate to GL0. |
| L-03 | Avalanche/Snowball is only the optimistic GL0 Phase-2 finality rail. It does not validate economics and does not create a BFT lock or commit certificate. Phase 2 is reached by decided-attestation `T_weight` **or** canonical `k1` depth. `T_count` is removed or made non-authoritative. |
| L-04 | Within the `k1` comparison window, valid GL0 tines use the ratified Taktikos `maxvalid-tk` rule. Beyond `k1`, valid competing tines use the Ouroboros Genesis-family `maxvalid-bg` density rule from the true common ancestor. Phase-2 state remains density-reorgable. |
| L-05 | `k2 = 100 * k1` remains a recommended retention, proof-service, and automatic rollback horizon. It is not an absolute fork-choice, pruning, or finality floor. If the true common ancestor is older than locally retained state, a node must not guess or choose socially; it stops production, fetches authenticated history/state, and resumes only after objective chain comparison and verified reconstruction. |
| L-06 | Metagraphs and checkpoint execution reference only exact canonical Phase-2 `(ordinal, hash, parentHash, mptRoot)` GL0 state. Inbound binaries may stage while the live GL0 head is ahead, but execution and global reads never use an unqualified live head. |
| L-07 | Every execution-committee signer independently replays the exact ordered framework inputs at the exact signed Phase-2 base and signs only when its byte-identical diff, extracted intents, and resulting root match. Missing inputs mean defer/no-sign. |
| L-08 | Ordinary noncommittee GL0 nodes verify the distinct execution `kQuorum`, exact base, scope, continuity, diff, and resulting root, then adopt the diff without recreating the ordinary currency snapshot. Shard depth never substitutes for missing replay signatures. |
| L-09 | Deterministically assigned noncommittee watchtower replay provides the execution-threshold collusion backstop. Positive required replay coverage precedes GL0 inclusion eligibility. A valid challenge triggers exceptional bounded replay by every GL0 validator of the challenged sharded-CL1 checkpoint's exact retained base and ordered framework inputs; the replay result, not the assertion, decides rollback/slash. This exceptional path is distinct from ordinary sharded-CL1 certificate/diff adoption; native GL1 and global-kernel execution remain universal independently of a challenge. |
| L-10 | Target: per-metagraph diffs are namespace-confined and every GL0 node executes the small deterministic global conflict/nullifier/settlement kernel over committee-extracted signed intents. No shard or metagraph writes another metagraph's namespace or the GL0-owned global settlement namespace directly. E9 remains planned and the live `numShards <= 1` path still bypasses shard processing. |
| L-11 | One shard has at most one checkpoint whose exact containing GL0 snapshot has not reached Phase 2. That checkpoint may batch multiple metagraphs and a contiguous ordered list of binaries for each metagraph. Its successor is released only by the exact Phase-2 checkpoint hash, never tentative embedding or ordinal equality. Remove configurable checkpoint `pipelineDepth` and shard-depth validity fallback; staircase duty still selects the producer for the next checkpoint. |
| L-12 | The binary-intake committee and execution committee are distinct draws over eligible GL0 operators. ML0 operators authenticate the metagraph binary but are not committee members by virtue of running ML0. Intake receipts can claim only authenticated source, checked parent/ordinal/envelope, durable custody, and availability; they never satisfy execution quorum. |
| L-13 | Before emitting an intake/custody signature, a selected intake member verifies the exact binary's source signatures against the pinned metagraph operator registry/allowlist, derives parent and ordinal from authenticated state rather than a self-claim, applies deterministic envelope/resource checks, and stores the exact bytes durably. |
| L-14 | Current execution-shard membership remains the public deterministic VK-hash draw with staircase producer duty, not the abandoned stake-weighted secret-VRF/per-slot-LDD shard design. Admission remains a separately domain-separated per-input VRF self-sortition. Both population views are derived from delayed canonical GL0 state, never a local peerlist. |
| L-15 | Framework economic root coverage is complete: every writable economic/framework field, including active allow-spends, is rooted and diff-covered. No writable unrooted field is allowed. |
| L-15A | `globalSnapshotSyncView` remains ML0-owned `CurrencySnapshotInfo` state and is hash-committed by `CurrencySnapshotStateProof.globalSnapshotSync`; it is not GL0 economic MPT state. Before removing its current field-32 GL0 mirror, the signed/root-bound framework replay input must carry the exact optional full-view preimage for every required checkpoint-window boundary, preserving `None` versus `Some(empty)`, and the explicit ML0 operator population used to validate sync entries. Producer, every execution signer, watchtower, and exceptional adjudicator verify the preimage hash and use the same population. Missing or mismatched material means defer/no-sign and cannot slash. After that replay-witness gate passes, field 32 is removed from every GL0 MPT/diff/load/reorg path while remaining in ML0 `CurrencySnapshotInfo`. |
| L-16 | The supported metagraph lanes are explicit signed `FrameworkCurrency` and `FrameworkCurrencyWithData` envelopes carried by the state-channel binary. The GL0 execution committee replays the framework portion; ordinary noncommittee GL0 validators verify the execution and positive-replay-coverage certificates, apply the scoped diff, and recompute its root. Custom application bytes are isolated commitment/availability carriage and cannot synthesize framework effects. Decoder success is not a lane selector. |
| L-17 | `authoritative*`, `AdoptFromSignedFields`, direct claimed-root installation, and undeployed fork-only compatibility schemas remain deleted from metagraph-originated execution. A reproducible committee diff is not an authoritative override. |
| L-18 | GL0 retains protocol-level authority to correct malformed metagraph state, but the trust arrow is only `GL0 protocol -> canonical GL0 state -> downstream rebase`. No ML0/CL1/DL1 artifact or operator authority may force a GL0 correction. A correction must be a deterministic, root-covered GL0 transition with an exact target and precondition, not a hidden producer override. |
| L-19 | A checkpoint binds nondecreasing exact Phase-2 GL0 refs for every ordered metagraph binary. Each binary executes global reads against its signed historical ref while local metagraph state threads from the checkpoint parent/pre-root. Receiver live head is never an execution input. |
| L-20 | Every touched metagraph uses compare-and-set: its signed `preRoot` and version must equal the GL0 proposal parent's current framework-mirror root/version before composition. Otherwise the checkpoint defers/rebases; intervening mirror state is never overwritten. |
| L-21 | Greenfield runtime starts with one canonical `ScodecV1` era at new-chain ordinal 0. Existing-network migration work is deferred, while prior data already on disk remains readable according to an explicit historical-read contract. Undeployed fork schemas are not retained. |
| L-22 | Tower eligibility is a required protocol feature, but enabling it is multi-stage. Snapshot-carried trial/tower state and `smtRoot` must be independently reproduced and verified by every GL0 recipient, durable across restart, and branch-aware across density reorgs before proofs are treated as security evidence. Replacing `NotComputed` alone is forbidden. |
| L-23 | Exact-parent state uses an immutable captured parent generation plus compare-and-set at commit; a stale session retries or defers and never blocks GL0 finality indefinitely. Locally viable branch generations are retained only within bounded policy; history beyond that bound is accepted only through exact authenticated reconstruction, otherwise the node enters `RecoveryRequired`. Finality durability uses one idempotent intent journal coordinating the existing overlay, chain store, tracker, outbox, watermarks, projections, and other sinks; it does not require migrating every sink into one database transaction. No exact-parent guard activates until the verified base anchor, descendant-preserving fold, authenticated restart/reorg recovery, and recoverable finality-intent transition land as one coherent unit. |
| L-24 | GL0 fork choice over three or more valid tines is an objective total-frontier function: the same cutoff-complete published valid frontier and branch-authenticated parameters produce the same canonical head independently of candidate enumeration, gossip arrival schedule, restart, or prior local incumbent. A stateful pairwise incumbent tournament cannot authorize canonical selection or Phase 2. O-15's single-common-anchor density/tower-evidence research direction is ratified, but activation remains blocked on cutoff/bounded-diffusion and late-reveal semantics, an exact selector and `k1` metric/equality, objective tie, evidence/verifier, Byzantine-overflow rule, complete validator-backed convergence witnesses, and security/liveness proof. |

Implementation status for L-15A is **OPEN**. The current currency incremental
contains the `globalSnapshotSync` proof hash and accepted `globalSnapshotSyncs`
delta, not the exact full-view preimage, while GL0 still writes, reconstructs,
and strips field 32 on different storage paths (ECO-F32).

Implementation status for L-23 is **OPEN / PARTIAL**. The first dark durability
slice now contains ScodecV1 finality ADTs/codecs, canonical identities, structural
validators, and sealed mutation authority that permits only initialization, a
validated `Prepared` intent, or entry into absorbing `RecoveryRequired`. Its
checksummed coordinator/artifact/audit/outbox store uses exact compare-and-set,
durable readback, bounded restart validation, and fail-closed recovery. None of it
is wired to the live `FinalityGate`, fork choice, GL0 consensus, MPT publication,
followers, or serving. It therefore neither selects Phase 2 nor makes any economic
state usable. Exact per-hash P0/P1 state and the optimistic K/alpha/beta decision
cascade remain unimplemented here.

The local coordinator bootstrap now closes the raw-publication initialization bypass:
only a store-minted, expiring MPT lease can create and durably consume the initial
mutation through sealed durable-store implementations, exact full-publication
equality is required for an existing Running head, and a mismatch records the exact
observation in absorbing recovery. Its status is
`LocalPublicationBound`, not node readiness. This proves local durable equality only
while the MPT mutex is held; it does not prove semantic validity or canonical Phase-2
anchoring, and it does not yet own subsequent MPT publication transitions.

Activation remains blocked on authenticated finality-evidence/fork-choice
authorization, holding and rechecking the exact branch revision through durable
publication, and a coordinator-owned MPT-plus-semantic-plus-anchor transaction before
`CoreApplied` or `Released`. Objective restoration, an effect executor with
sink readback provenance and an explicit dependency DAG, `RetentionMature`
authority before pruning, bounded streaming validation for arbitrary-depth paths,
and a long-history audit-journal checkpoint/accumulator also do not exist. The
immutable MPT image store and pure branch lineage models remain nonactivating
prerequisites: they still lack FinalityGate-authenticated anchoring and a complete
active-era semantic verifier/session boundary. Live overlay, chain-store admission,
fold, and multi-sink publication defects remain open. L-23 does not close `SMT-02`,
`ROOT-002..005`, `E9-BRANCH`, or any live finality test gate. The schema represents
only an already-decided `T_weight` attestation or canonical depth-`k1` result, but
does not yet authenticate either. Phase 2 remains density-reorgable, `k2` remains
retention/recovery policy, and no global BFT proposal/vote/lock/QC path is permitted.

The live metagraph-admission adapter is also an activation blocker, not an interim
authority: it combines a monotone ordinal watermark with a separate current-best-tip
walk and can therefore mint `Phase2CurrencyBinaryContext` for a density replacement
that has not independently qualified. It must be deleted in favor of the exact
branch-revision authority above. The new strict MPT prefix/raw enumeration API is
only lossless parser plumbing; it does not close `ROOT-008` or mint that authority.
Its nibble-prefix comparison deliberately surfaces upper/lower case aliases, but
consensus use remains blocked until one whole-image pass rejects every
noncanonical, invalid, odd-length, or aliased physical key before partitioning.
This is a stop-the-line requirement, not normalization policy: MPT-07 reproduced
an infinite full build and incremental raw-map/root divergence for case aliases.
`ROOT-011` must reject the complete candidate before root construction or live
mutation and the builder must independently terminate with a typed collision error.

## Ratified direction with engineering freeze gates

The owner has ratified the direction recorded for `O-01` through `O-17` below. Those sections enumerate
the executable constants, schemas, reference models, RED vectors, and proofs still required
before activation. They do not reopen the locked architecture above, and incomplete engineering
cannot be filled by a local configuration value or caller-specific shortcut.

### O-01 Avalanche population and exact parameters

**Owner-ratified direction:** sampled K/alpha/beta optimistic finality over the delayed canonical
population, using stake/registry from epoch `N-2` and eta from `N-1` for epoch `N`, with depth-`k1`
fallback for undersized populations. The current values in the answers are provisional calibration.
Engineering must implement the sampled cascade, derive and freeze `T_weight` and every per-network
constant, then pass reference-model, convergence, grinding, and adversarial tests.

### O-02 Deep-history recovery beyond local `k2`

**Owner-ratified direction:** use authenticated historical chain-sync/rejoin from the objectively
selected true common ancestor; a peer supplies bytes/proofs but never chooses truth. Engineering
must specify the archive/bootstrap protocol when a winning
`maxvalid-bg` tine's true common ancestor predates local rollback state: required
proof material, state reconstruction, production halt/resume conditions, peer
diversity, and atomic recovery of MPT, mempool, checkpoints, downstream events,
tower state, and eta/registry history. The objective fork-choice result remains
mandatory; manual operation may start recovery but cannot choose the winning
tine.

### O-03 Watchtower parameters and availability fallback

**Owner-ratified direction:** VRF-assigned watchtowers, one honest mismatch triggers adjudication,
and fixed affirmative approvals with no-show redraw before inclusion. Engineering must derive and
freeze complement/sample size, minimum positive coverage, assignment anchor,
deadline, retry/redraw, bonds, replay budget, challenge lifetime, and censorship
fallback. The locked release rule is pre-inclusion positive coverage; these
parameters must preserve unrelated GL0 progress without letting an unchecked
checkpoint-derived economic effect become usable.

### O-04 ML0 response to a Phase-2 density reorg

**Owner-ratified direction:** roll back to the last ML0 snapshot embedded by the winning GL0 branch;
use a registered deterministic rebase/undo contract where available, otherwise begin a new ML0
epoch. Engineering must specify the exact retained history and deterministic rewind/rebase/new-epoch
contract. Append-only audit history is preferred; applications with a registered
deterministic rebase/undo contract may append a corrective snapshot. Otherwise a
new ML0 epoch begins at the last valid GL0 reference. Noninvertible external
effects are an integrator risk decision and are not made irreversible by a new
protocol phase.

### O-05 Intake threshold and censorship recovery

**Owner-ratified direction:** intake is a separately typed custody/admission threshold and can
never satisfy execution `kQuorum`. Engineering must freeze its threshold, receipt/custody lifetime, queue ownership, durable
replication requirement, redraw schedule, and direct-fetch/censorship fallback.
The threshold proves intake/availability only and remains unable to satisfy
execution `kQuorum`.

### O-06 Global correction authorization

**Locked V1 direction:** GL0 protocol corrections are authorized by binary/social governance and
activated through an ordinal/hash-bound `ProtocolEra` hard fork. No metagraph-originated authority
is an option. An on-chain governance/adoption mechanism and live Shape-B signed-correction schema
are deferred engineering, not V1 owner gates; when pursued they must bind the exact metagraph,
pre-root/version, diff, post-root, reason, activation ordinal, replay protection, downstream rebase,
and audit trail.

### O-07 Economic operation grammar

**Owner-ratified direction:** preserve v4 framework functionality with explicit deterministic
authority, including the rooted allowlisted service-metagraph data exception; service output is
data, never self-authorizing value, and every economic consumer still re-executes. Audit
Tessellation v4.0.0 behavior for every framework operation, then preserve
the functionality that has explicit deterministic authority, conservation,
ordering, and replay semantics. Do not invent treasury or oracle authority and
do not disable existing functionality merely because its rule has not yet been
restated. Do not invent treasury authority. Any defective upstream behavior receives an explicit protocol rule and
RED/oracle vectors before enablement.

`V4-ECONOMIC-GRAMMAR-AUDIT.md` records partial source evidence. It confirms
that manual owner unlock and metagraph-source spend are intended v4 features,
while proving that their inherited authority checks are insufficient. The gate
remains open until a mechanical constructor/codec/event/acceptance reachability
inventory and the full differential corpus are complete.

### O-08 Tower proof contract

**Owner-ratified direction:** enable tower eligibility only through recipient-reproducible,
branch-bound, KES/VRF-verified proofs; weight is proof selection only, never chain selection.
Engineering must freeze snapshot-carried per-level state/pointers, trial computation, historical
`N-2` registry and `N-1` eta inputs, KES/VRF verification, SMT inclusion path,
proof comparison, size limits, cache reconstruction, and density-reorg rollback.
A single GL0 peer may supply a proof, but it must verify from a trusted genesis or
cached canonical commitment; the peer can withhold freshness but cannot choose
proof truth.

### O-09 Pure opaque state-channel product scope

`FrameworkCurrency` and `FrameworkCurrencyWithData` are locked. The owner also retains the limited
standalone opaque/data-only lane needed for v4 functionality. It has no framework-economic write
surface, receives only authenticated inclusion/availability semantics, and uses an explicit signed
lane/type; decoder success can never select or promote it.

### O-10 Portable shard-parent duty and slot bound

Freeze the portable evidence used to validate a child checkpoint's parent-relative
staircase duty. The current worktree resolves the parent from a receiver-local shard
chain store, so a GL0 producer that saw shard gossip can accept an artifact that a
follower missing that gossip rejects. Parent availability may delay validation, but
prior receipt cannot be a validity input.

**Owner-ratified direction:** add an exact `(ordinal, hash)` reference to the GL0 snapshot that
carried the parent checkpoint to the child's signed preimage. A verifier requires that
exact GL0 snapshot to be Phase 2 under the hash-bound `FinalityGate`, extracts the
parent checkpoint under the same shard ID, hashes its canonical signing preimage to
the child's `parentCheckpointHash`, and only then uses its signed ordinal/slot for
continuity and staircase duty. The proof may carry the parent artifact/header or an
authenticated inclusion proof, but a bare claimed parent slot is insufficient.
Genesis uses one explicit sentinel rule. The exact Phase-2 parent reference and
checkpoint bytes must be durably recoverable by a node that missed shard gossip.

For the upper bound, require an embedded checkpoint's signed slot to be no greater
than the signed slot certificate of the GL0 snapshot that includes it. A checkpoint
slightly ahead because of clock skew simply remains pending for a later GL0 slot; no
receiver wall clock or configurable local epsilon enters artifact validity. This
prevents a committee member from anchoring a far-future valid staircase window and
halting honest successor production until wall time catches up.

### O-11 Permissionless GL0 operator roster

**Owner-ratified structure:** Cardano-style minimum self-stake/pledge and slashable self-bond,
delegation permitted and slashed proportionally with the operator, with
`bond >= extractable value in one fraud window`. Key registration proves ownership only and
cannot grant membership. Engineering must encode the canonical rule that turns a `PeerId` into
an eligible GL0 operator, including activation/exit, slash/cooldown, and exact period-boundary
root semantics, then derive per-network bounds.

This rule is load-bearing for every uniform `1/N` admission, execution, and
watchtower population. Using every positive-stake identity without a rooted
minimum-bond/identity rule permits stake splitting into many PeerIds; using the
seedlist or locally observed peers is asymmetric and forks committee denominators.
Eligibility in period `N` must intersect active paired keys with this authenticated
`N-2` roster and delayed stake state. Until the roster is rooted and recoverable,
historical committee resolution returns unavailable rather than substituting a
current or key-only population.

**Observed implementation gap.** `StakeRegistry` still defines the validator
population through a startup-updated local set and filters historical stake through
that current set; the MPT path can also fall back to live aggregate/equal weight
(`StakeRegistry.scala:32-45,68-85,425-438,480-503`). `SharedServices` supplies the
set from the receiver's seedlist (`SharedServices.scala:275,300-304`). GSI roots
stake amounts, paired-key histories/pointers, and immutable genesis keys, but no
operator roster (`GlobalSnapshotInfo.scala:145-179`). The isolated historical
resolver correctly keeps a roster separate from key registration and fails when
one is unavailable; production does not yet supply that rooted roster
(`HistoricalOperatorConsensusKeyRegistry.scala:37-63,126-140,201-231`). Its model
also requires positive stake after the supplied roster intersection
(`HistoricalOperatorConsensusKeyRegistry.scala:169-198`); positive stake is a necessary input
but cannot define the ratified self-bond/backing predicate or roster by itself. Neither
the seedlist, observed peers, a valid key record, nor positive stake is an interim
authority.

**Owner-ratified direction; schema not frozen - atomic historical population boundary.** At the exact
closing snapshot of period `P`, every GL0 producer and verifier would run one pure,
era-selected authorization rule over the same post-transition rooted state and
commit one canonical value whose sorted map keys are the exact authorized operator
population and whose values are their raw consensus stake/weight. The same boundary
value may carry `eta_P`, preserving the current one-read period cache. Thus one MPT
witness cannot mix one branch's roster with another branch's stake. Paired KES+VRF
registration remains a separate identity registry; final eligibility is still the
intersection of the boundary population and active paired records. This paragraph
does not freeze a Scala type, field number, codec, or authorization predicate.

**Required lookup invariant.** Regardless of the eventual encoding, a period-`N`
consumer resolves one exact candidate-parent `(ordinal, hash, parentHash, mptRoot)` capability:

- population and raw stake/weight from that branch's `N-2` boundary;
- the active atomic KES+VRF pair whose record is present in the same `N-2` prefix;
- eta evidence for `N` from that branch's `N-1` history; and
- the active period-`N` protocol parameters from the same branch.

The result is a single eligible population plus the derived KES step. A same-ordinal
sibling, wrong root, current seedlist/stake, receiver tip, live-peer set, or missing
history rejects or defers before draw, proof, signature, storage, acceptance, or
slash. Shard callers additionally bind this lookup to their exact Phase-2 GL0
anchor. Portable evidence carries MPT inclusion/activation witnesses; a peer may
supply those bytes but cannot select the root or population.

**Reusable upstream state, not upstream authority.** Tessellation v4.0.0 provides
signed chained node profiles and token-lock-backed delegated-stake/collateral
create, withdrawal, and pending-withdrawal records
(`v4.0.0:modules/shared/src/main/scala/io/constellationnetwork/schema/node.scala:146-189`,
`v4.0.0:modules/shared/src/main/scala/io/constellationnetwork/schema/delegatedStake.scala:80-159`,
`v4.0.0:modules/shared/src/main/scala/io/constellationnetwork/schema/nodeCollateral.scala:67-108`). These can supply
identity/profile and bonded-principal facts. Their validators authorize target
nodes through the seedlist
(`v4.0.0:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/node/UpdateNodeParametersValidator.scala:67-80`,
`v4.0.0:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/delegatedStake/UpdateDelegatedStakeValidator.scala:137-144`,
`v4.0.0:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nodeCollateral/UpdateNodeCollateralValidator.scala:163-170`), and v4 GSI contains no permissionless
operator roster. The event/state machinery may be reused; the seedlist decision may
not.

**Open engineering/parameter freeze gates before schema or runtime activation:**

- minimum self-bond versus total delegated/collateral backing, and whether a third
  party may collateralize an operator;
- the quantitative anti-stake-splitting rule for uniform `1/N` populations;
- activation, exit, unbond, and evidence/slash horizons, including whether pending
  withdrawals remain slashable through the last eligible artifact's challenge
  window;
- slash/cooldown timing and whether any liveness cap may keep a slashed operator in
  the population;
- whether lifecycle membership is purely derived from rooted profile/backing state
  or requires a separately authorized join/exit event;
- the independently authorized immutable genesis population and its backing rule;
  and
- bounded registration/state-growth fees or limits, without making registration a
  membership grant.

After those gates are specified, delivery order is: canonical codec/root ownership, immutable
genesis population, pure boundary derivation and undo/refold, exact-parent N-2/N-1
resolver, exact Phase-2 artifact references, all-consumer migration, then fixture
and adversarial qualification. O-11 remains activation-blocked until that sequence passes the
`KEYREG-013`, `PERM-*`, and `PARAM-001` gates.

### O-12 Runtime KES secret deletion and N-2 reorg boundary

**Owner-ratified baseline:** delete/evolve KES secret material at eta-period evolution; do not
retain old masters through `k2`; an objective reorg across erased activation enters
`RecoveryRequired` and explicit rejoin, and missing secret history is never slash evidence.
Engineering must quantify the common-prefix assumption under which an operator may delete/evolve
past KES secret material after a paired runtime registration becomes active.
Eligibility in period `N` reads the registration/roster/stake prefix at `N-2`,
but Phase-2 state remains density-reorgable. KES secret state is deliberately
one-way and must not be modeled as another MPT field that ordinary Phase-2
rollback can restore.

The baseline activation policy explicitly assumes the N-2
prefix is common-prefix stable at the ratified security bound; an objective
density reorg that nevertheless crosses an activation whose prior KES secret was
erased puts the operator in durable `RecoveryRequired`. It stops all consensus
signing, follows only authenticated objective chain recovery, and requires an
explicit operator realignment/rejoin procedure for a newly preregistered pair.
The operator cannot choose the winning branch and missing secret history cannot
be guilt or slash evidence.

Retaining old KES masters through the local `k2` horizon would allow automatic
secret rollback only by weakening the KES forward-security guarantee. That is
not an implicit retention policy. Engineering must encode the exact deletion point, quantify the
common-prefix failure probability, and implement recovery/rejoin consequences before runtime key
activation. Offline escrow economics are deferred and non-load-bearing for V1; V1 cannot assume
escrow for safety or automatic rollback.

### O-13 Durable global delivery sequence and settlement ordering

**Owner-ratified direction; schema not frozen.** Replace `GlobalSnapshotsProcessed` and every bounded
history reconstruction with a hash-linked sequence per destination metagraph.
GL0 atomically appends canonical framework delivery records and advances a rooted
outbox head with the settlement and permanent authorization nullifier. ML0 stores
an applied `(sequence, deliveryId)` cursor in `CurrencySnapshotInfo` and its state
proof, executes only a contiguous range from one exact Phase-2 GL0 reference, and
emits a compare-and-set acknowledgement. Acknowledgement changes representation
only: effective balances before and after compaction are byte-identical and the
permanent economic nullifier remains.

The owner-ratified granularity is a bounded canonical per-metagraph batch with
contiguous multi-entry acknowledgement. GL0 ordinals are not delivery sequence
numbers: they are sparse, and ML0 may discover an older still-pending delivery
after observing a higher GL0 ordinal. GL0 assigns its destination sequence at the
canonical append; receiver observation order cannot change or skip it. Pending records are
individual rooted leaves, not one growing field-18 ordinal set. Their framework
bytes remain available until acknowledged; missing bytes defer rather than
authorizing a peer claim or skipping an effect.

Open engineering/schema freeze gates before allocation:

- the exact total order among an already-canonical inbound delivery, local spend,
  consume, cancel, expiry, and refund after the ratified fixed first rule that the
  inbox applies before local spend; that first rule does not settle the other
  conflicts;
- rooted limits for record bytes, entries per batch/snapshot, pending entries,
  deterministic backpressure, and protocol fees; local HOCON is not validity;
- exact retention and authenticated deep-recovery behavior after local history is
  unavailable; and
- O-04's rewind/rebase/new-epoch outcome after ML0 applied a Phase-2 reference that
  a later density reorg orphans.

No active field number is assigned here. Greenfield active state uses the frozen
Scodec era from ordinal zero; upstream v4 data remains read-only import input and
cannot silently default a missing cursor into signable state.

The remaining terminal-order decisions are isolated for owner review in
[`O13-ALLOW-SPEND-TERMINAL-ORDER-OWNER-REVIEW.md`](O13-ALLOW-SPEND-TERMINAL-ORDER-OWNER-REVIEW.md).
That packet is not an executable rule: O13-A3 still requires an exact schema
freeze for semantic framework spend intents, authority sequencing, and
equivocation disposition before production terminal settlement may select a
winner. Checkpoint/binary packaging coordinates are evidence only.

### O-14 Framework fee sequence and opaque-data binding

**Owner-ratified direction; schema not frozen.** Reuse the already rooted per-metagraph
`MgLastFeeTxRefs` partition (field 27) as the strict head for each fee source. A
framework fee signs network/genesis, era/lane, metagraph, source, destination,
amount, exact parent reference, and an exact opaque-data commitment. Acceptance
requires the parent to equal rooted state, derives the successor reference, checks
available bytes or a content-addressed chunk manifest without executing DL1 logic,
and atomically updates balances plus the head. Signature proofs do not change the
semantic fee identity.

The owner-ratified v1 rules are:

- exactly zero or one framework fee per custom item; no unmatched item, orphan
  fee, duplicate mapping, or extra fee record is accepted;
- any invalid fee rejects the complete framework segment without partial debit or
  custom-item acceptance;
- outgoing fees reserve against the pre-batch source balance before any same-batch
  credits become spendable;
- a parent-valid fee has no protocol expiry in v1; only one same-parent sibling can
  win, and the others become stale; and
- a newly signed successor may intentionally pay again for identical data. A ban on
  repeated semantic data requires a separate explicit data nullifier.

The ratified conditional rule uses a content-addressed chunk-manifest root only when every exact
chunk is available before an execution signature; otherwise the fee binds exact signed-item
bytes. Engineering must encode the tagged shape and close the E9 reservation interaction. GL0 verifies bytes, availability,
authorization, sequence, arithmetic, and conservation; it never treats ML0 custom
execution output as economic authority. The outer `StateChannelSnapshotBinary.fee`
is a separate global-balance operation and still requires E9's checkpoint-wide
reservation kernel.

### O-15 Multi-tine Taktikos/Genesis frontier semantics

Engineering/proof packet: [O-15 Multi-Tine Frontier Owner Review](O15-MULTI-TINE-FRONTIER-OWNER-REVIEW.md).

**RATIFIED PROPERTY AND RESEARCH DIRECTION; ACTIVATION-BLOCKING PROTOCOL/PROOF GATE.** L-04 fixes the binary rule
boundary: `maxvalid-tk` within `k1` and Genesis-family `maxvalid-bg` beyond `k1`.
L-24 now fixes the multi-tine semantics: once two nodes have the same
cutoff-complete published valid frontier and branch-authenticated parameters, they must
produce the same canonical head independently of enumeration, prior arrival
order, restart, and prior incumbent. Interim heads may differ before their
published frontiers converge; a late valid reveal extends the frontier and
requires deterministic reselection. The owner-ratified research direction uses density measured
from one common anchor and reserves the tower for mechanism/portable evidence, never an override.
This does not yet define the concrete
objective selector or its portable declared-frontier evidence. No Nakamoto-only
artifact can prove that an adversary has no unrevealed private tine.

The primary sources do not supply a total frontier order. Taktikos section 3,
Algorithm 1 initializes `C <- Cloc`, enumerates `C1..Cj`, and replaces the
current incumbent only with a valid candidate that forks from that incumbent by
at most `k` and is longer, or equal-length with a lower head slot. A deeper
candidate and an exact tie leave the incumbent unchanged. Ouroboros Genesis
section 3.2.4, Figure 7 likewise initializes `Cmax <- Cloc` and enumerates
`C1..CM`; it applies strict longer-chain Condition A to a short pair and strict
pairwise-MRCA density Condition B to a deep pair. Neither paper specifies the
enumeration order, proves transitivity/permutation independence, or defines an
incumbent-independent argmax. Sources: Schutza et al., *Ouroboros Taktikos*,
section 3/Algorithm 1 ([DOI](https://doi.org/10.1007/978-981-99-8104-5_20),
owner-supplied published artifact `Taktikos - BlockSys_2023_paper_4526.pdf`,
SHA-256
`63d030d7df3e908985340dc86636f00a2e79e66841e051b83c4516127be1d7cf`,
p. 6; corroborating prepublication `fc-2023-v7.pdf`, Appendix A.1, pp. 22-23,
SHA-256
`9cdb2218db703195483a31ac4ff467b6a28cd76a6d64368e90b616c9c7ad1db6`).
Badertscher et al., *Ouroboros Genesis*, section 3.2.4/Figure 7
([accepted manuscript](https://www.pure.ed.ac.uk/ws/portalfiles/portal/76645278/Ouroboros_Genesis.pdf),
pp. 11-13).

For complete histories where the true MRCA is resolved, the current mixed
pairwise comparator is commutative but not transitive. A regression witness over
three structurally connected `ChainTip` tines has `A >tk B`, `B >bg C`, and
`C >bg A`; every deep edge is a strict density win, so the cycle does not depend
on the open VRF/hash tie rule. Three list permutations return three different
`selectBest` winners (`ChainSelectionSuite.scala:191-226`). The bare `ChainTip`
witness does not authenticate complete snapshots, VRF/KES eligibility, or
historical parameter eras. A store-boundary regression under a synthetic enabled
`k`/`s` configuration now signs the ordinal and parent linkage, feeds the same
frontier through three parent-before-child schedules, and leaves
`NakamotoChainStore` at best tips C, B, and A
(`NakamotoChainStoreSuite.scala:280-372,427-469`). This converts the production
store class/control-flow tournament from a source inference into a direct
store-path reproduction. It still invokes `store` directly with synthetic
caller-supplied slot/VRF metadata
and shared context and does not exercise a shipped environment configuration, so
a full validator-backed active-configuration admission witness remains mandatory.
No live
fork-choice source may mint `CanonicalSelectionToken`, authorize `FinalityGate`,
or release Phase-2 state from this tournament. The objective property is settled.

The dark `ForkChoiceDecision` schema does not close this gate. It carries only an
`ImmutableArtifactPointer`, deliberately without a Tk/Bg or transition-form tag
that the objective selector cannot yet prove. Validation closes equality between
one intent-scoped evidence locator and the selection token's pointer. It does not
decode or verify complete headers, tines, frontier membership/completeness,
parameter era, or the selected result (`FinalityCore.scala:120-138,353-364`;
`FinalityBaseCodecs.scala:96-119,178-179`;
`FinalityIntentValidator.scala:59-120,748-761`).

The following engineering/research freeze gates remain and must land coherently:

- **A - Selector:** define a total deterministic function over the complete valid
  frontier that resolves mixed short/deep cycles without an incumbent or hidden
  collection order. This is a new protocol construction, not a theorem inherited
  from either cited paper.
- **B - Frontier evidence:** define the bounded observation boundary and portable
  evidence binding the exact declared candidate manifest, ancestry, validity, and
  parameters supplied to the selector. Receiver-local arrival time, wall clock,
  peer visibility/count, first-N receipt, vote, quorum, certificate, or QC is not
  authority, and no Nakamoto artifact can prove that an adversary has not withheld
  another valid tine. The protocol must define an authenticated slot/era boundary
  plus explicit diffusion assumption and deterministic late-reveal reselection,
  or name another non-circular intake commitment.
- **C - Security and liveness:** prove or quantitatively bound chain quality,
  common prefix, grinding/withholding leverage, and convergence for the chosen
  cycle-resolution rule under the project's Taktikos/Genesis assumptions.
- **D - `k1` distance:** freeze the exact short/deep metric and the `== k1`
  boundary used for every pairwise fact supplied to the frontier selector. The
  current bounded walk, when it resolves a true MRCA over consecutive tines,
  returns maximum post-MRCA suffix length, but production passes
  `kLookback = k1 + 1` and chooses density only for `depth > kLookback`;
  therefore `depth = k1 + 1` still uses Tk, contrary to L-04's prose
  (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/types.scala:173-177`;
  `GlobalSnapshotConsensus.scala:979-994`; `ChainSelection.scala:161-175,187-240`;
  `ChainSelectionSuite.scala:228-250`).
- **E - Exact ties:** define and prove an objective tie result, or reject the current lower-VRF
  then hash rule; it is not in either cited algorithm
  and must be assessed for precomputation and grinding.
- **F - Lineage and overflow:** classify arbitrary-depth strict ancestry before
  any divergent-tine rule so a valid descendant cannot lose to its own prefix.
  Define objective dominance, expiry, authenticated compaction, or bounded-memory
  streaming for arbitrarily many valid equivocations; receiver-local eviction,
  first-N/hash truncation, `k2` caps, and adversarial fail-stop cannot decide the
  canonical result.

This gate does not reopen L-02 through L-05. GL0 remains Nakamoto/Taktikos/LDD
without global BFT machinery; Phase 2 remains `T_weight OR k1`, density-reorgable;
and `k2` remains retention/recovery policy only.

### O-16 Exact Phase-2 consumer lease and invalidation

Engineering packet:
[P6 FIN-14 Phase-2 Consumer Lease](P6-FIN14-PHASE2-CONSUMER-LEASE.md#10-ratified-choices-and-engineering-freeze-gates).

**OWNER-RATIFIED DIRECTION; SCHEMA/DEPENDENCIES NOT FROZEN.** `FIN-14` requires more than replacing the ordinal
watermark with an exact-ref query. A consumer can verify a valid exact P2 anchor,
wait for committee work, and then mutate after a density replacement. The proposed
local `CanonicalPhase2Lease` therefore binds the complete exact ref, released-core
generation, qualification and fork-choice evidence, MPT/semantic/anchor readbacks,
purpose, and a persisted lineage revision. It is package-minted,
non-serializable, non-portable, and usable only through a short
`commitIfCurrent`; no finality/MPT/chain lock is held across network, DA, replay,
signature, or committee waits. Portable evidence remains separately verified.

Acquisition uses two short coordinator operations around unlocked
immutable verification. The first captures a complete descriptor of revisions,
lineage, target, evidence/readback pointers, purpose policy, and sink revision. The
second compare-and-sets that same descriptor after snapshot, evidence, MPT,
semantic, and anchor verification. Holding a coordinator/finality lock across
complete-image verification is forbidden, while a single pre-verification read is
insufficient because it leaves the `FOLLOW-008C` race.

The ratified revision rule uses `CanonicalBranchRevision` for every selection
mutation and a separate monotone `CanonicalLineageRevision` for any
rollback/replacement/recovery that removes or substitutes a previously canonical
hash. Pure descendant extension may preserve an exact-ancestor use after a final
recheck. Every replacement invalidates all old-generation permits; even a target
that survives below the MRCA must be reacquired. This deliberately prevents ABA
reuse and avoids requiring a complete selective dependency graph in V1.

Engineering must encode and verify these ratified choices and remaining freeze gates:

- **O-16A - replacement invalidation:** every replacement invalidates all old leases; selective
  orphan-only invalidation is not V1;
- **O-16B - exhaustive purpose policy:** explicit, closed purpose cases for binary
  admission/confirmation/requeue, shard execution, checkpoint inclusion/anchor,
  assigned-watchtower replay,
  challenge adjudication, cross-metagraph settlement/economic reads, historical
  registry contexts used by optimistic sampling and tower eligibility, tower proof
  serving, protocol correction,
  follower adoption, operational/bootstrap serving, retention/recovery, and event
  delivery; for each, freeze
  still-canonical exact-ancestor versus current-P2-head behavior without reopening
  L-19's signed historical-reference rule;
- **O-16C - attestation reuse:** raw signed admission attestations may be reverified and reindexed
  after a target-surviving replacement; prior counts/thresholds never transfer;
- **O-16D - historical age:** local wall clock or HOCON cannot decide validity. Engineering must
  define any operation-specific or branch-authenticated age/freshness bound through ECON-G;
- **O-16E - effect-journal conformance:** conformance to L-23's already locked
  idempotent effect-journal boundary for
  admission/cache/tally/shard-buffer commit and inverse/requeue; and
- **O-16F - signed scope schema:** the active-era signed full
  `GlobalSnapshotStateRef`/registry/parameter-era/purpose shape replaces the current incomplete
  `GlobalSyncView` consumer scope; its concrete codec remains engineering.

The exact-ancestor rule and effect-journal strategy are conformance checks, not
open alternatives to L-19 or L-23.

The exact contract, current-source interleavings, invalidation inventory, and
`FOLLOW-008A` through `FOLLOW-008P` matrix are in the linked packet. No live lease issuer may land before O-16's
engineering freeze gates close and O-15, O-01, released-core readback, ROOT semantic/image gates, and
consumer effect ordering are independently verified. Wrapping the current Boolean
adapter in an opaque type is explicitly forbidden.

### O-17 ROOT-008 GL0 partition grammar

Engineering packet:
[ROOT-008 GL0 Partition Grammar](ROOT-008-GL0-PARTITION-GRAMMAR.md#8-ratified-anchors-and-engineering-freeze-gates).

**OWNER-RATIFIED DIRECTION; SCHEMA/PROOFS NOT FROZEN.** `ROOT-008` owns canonical physical placement, one
active-era value codec per field, logical identity/scope reproduction, bounded
decoding, and structural/index/population relations. It does not replace the P2
economic oracle or the O-07/ECON-G authorization, conservation, backing, replay,
and transition rules. Both gates must pass before a structurally valid image can
be used as canonical economic state.

Engineering must encode and verify these ratified anchors before schema activation:

- **O-17/R008-01 - retired ID lifecycle:** delete physical IDs 3, 6,
  and 21 from active GL0, deleting 32 after ROOT-010 witness parity, retaining
  numeric gaps, and handling upstream-v4 disk history only through an offline
  typed import rather than live fork-only compatibility decoders.
- **O-17/R008-02 - field-20 self-authentication:** store
  `(EtaPeriod, HistoricalStakeSnapshot)` so the leaf reproduces its physical key.
- **O-17/R008-03 - field-23 self-authentication:** store
  `(PeerId, KesRegistrationReference)` and separately proving the pointer's exact
  unique match in field 22.
- **O-17/R008-04 - resource and persistent-growth contract:** derive measured,
  versioned per-image/field/value/member budgets and a non-halting permanent
  nullifier/slash growth strategy. A finite state-size cap cannot silently make a
  valid chain stop once permanent nullifiers reach it; authenticated compaction or
  an accumulator requires its own exact-once proof and `GROWTH-001` test plan.
- **O-17/R008-05 - set-member identity:** derive and freeze canonical unsigned
  event/content-reference hashes for economic-event uniqueness and the signed
  domain/reference identity for KES records.
- **O-17/R008-06 - token-lock currency scope:** field 8 accepts only native/global
  `currencyId == None`; field 30 accepts only `Some(CurrencyId(owningMetagraph))`. The parser
  must reproduce and enforce this relationship.
- **O-17/R008-07 - field-32 deletion gate:** prove exact optional full-view
  witness parity, `None` versus `Some(empty)`, explicit ML0 population, and
  staged/backfill/restart/reorg behavior before deleting field 32 at every GL0
  boundary. This is a conformance gate on locked L-15A, not an option to retain an
  unrooted writable field.

The complete inventory, ratified directions, and generated-test contract are in the
linked packet.

### O-18 Transport and DA byte contract

Owner-review packet:
[O-18 Transport and DA Byte Contract Owner Review](O18-TRANSPORT-DA-BYTE-CONTRACT-OWNER-REVIEW.md).

**OWNER RESPONSE REQUIRED; LIVE WIRING FROZEN.** The current stack has incompatible application,
GossipSub, gRPC, ChainSync, callback, compression, and downstream-retention limits. The additive
bounded Brotli decoder and reserved ingress queue are unwired preparation only. They neither close
the transport finding nor change artifact validity.

The owner must disposition `O18-01` through `O18-08`: active-era per-family canonical and transport
maxima; exact-source-chain migration scope; canonical byte identity versus compression; descriptor,
chunk, fetch, and retention proof; always-pull versus threshold delivery; the fate and byte basis of
the existing `512000` state-channel rule; the fate and byte basis of the `20 MiB` event-cutter rule;
and mandatory absolute decompression caps versus optional ratio rules.

This is a resource/availability contract only. Every GL0 validator still executes and validates
every direct native `GL1 -> GL0` transition. For sharded CL1, producer/every execution signer replay,
watchtower replay, and ordinary noncommittee certificate/diff/root verification remain unchanged.
No transport or DA receipt can satisfy a state-validity threshold.

### O-19 Upstream-v4 snapshot migration policy

Owner-review packet:
[O-19 V4 Snapshot Migration Policy Owner Review](O19-V4-SNAPSHOT-MIGRATION-POLICY-OWNER-REVIEW.md).

**OWNER RESPONSE REQUIRED; MIGRATION AUTHORITY FROZEN.** Upstream v4 carries 17
`GlobalSnapshotInfo` field families. The post-MPT v4 state root omits
`updateNodeParameters` and `priceState`, so a selected signed tip plus a supplied
state object cannot authenticate those values. The current fresh-genesis loader
is not a migration transform: it has no exhaustive field plan, uses approximate
balance/stake/collateral fixture behavior, and cannot establish source-to-target
conservation or replay safety. Terminal v4 metagraph signatures are also
insufficient by themselves: the inner receiver derived facilitators from the
proofs while GL0 outer admission used seedlist/allowance intersections rather
than a portable metagraph quorum.

The owner must disposition `O19-01` through `O19-08`: old-domain live economic
state; per-metagraph exact-head continuation versus epoch restart and separate
opaque retention; epoch-progress mapping; replay versus reset for the two
post-MPT-unrooted source fields; rooted target operator/metagraph registries;
initial stake history/eta and fresh KES+VRF eligibility; treatment of malformed
release-selected source state; and the exact per-asset conservation policy.

Until those decisions are answered, every source field must remain explicit,
source signatures are evidence only, unrooted fields require replay or an
explicit reset, and no importer may silently drop/default/re-sign state, add a
stipend, or install target economic state.

### O-20 Field-34 slash record schema

Owner-review packet:
[O-20 Field-34 Slash Record Schema Owner Review](O20-SLASH-RECORD-SCHEMA-OWNER-REVIEW.md).

**OWNER RESPONSE REQUIRED; FIELD-34 SCODEC FREEZE BLOCKED.** The only production
`SlashedRegistryEntry` constructor is the invalid-state-proof ledger sink and it
hard-codes `SlashReason.InvalidStateProof`. The shared record nevertheless exposes
three future reason tags while requiring invalid-checkpoint-specific `shardId` and
`disputedCheckpointHash` fields. Its physical key is the same checkpoint-specific
triple, and its value still uses a hand-written JSON `ImmutableCodec`.

The owner must disposition `O20-01`: either freeze an invalid-state-proof-only V1
record and require future slash kinds to add variant-specific ADT payloads/keys, or
freeze one generalized multi-reason record now together with complete required-field,
identity, deduplication, evidence, effect, and invalid-combination rules for every
reason. The packet recommends the narrow V1 plus future variant-specific ADT, but
that recommendation has no authority until answered.

Until then, RED tests and dark codec experiments may proceed, but no implementation
may activate field-34 Scodec bytes, use sentinels/optional combinations to fill
undefined future contexts, or treat enum presence as a designed slash consequence.

### O-21 Optimistic decision evidence

Owner-review packet:
[O-21 Optimistic Decision Evidence Owner Review](O21-OPTIMISTIC-DECISION-EVIDENCE-OWNER-REVIEW.md).

**OWNER RESPONSE REQUIRED; OPTIMISTIC RAIL REMAINS DARK.** The owner has already
ratified `decided-attestation T_weight OR canonical k1 depth`, with no global BFT
lock/QC and no optimistic fork-choice or economic-validity authority. The missing
decision is whether the required sorted unique signed local decision statements
are sufficient alone or require additional authoritative complete sampling
transcripts or transcript commitments with mandatory retrieval.

The packet recommends exact signed local decision statements, with query
transcripts retained only as non-authoritative audit material. Every statement
must bind the exact state ref, domain/parameter identity, signer and historical
KES step, N-2 roster/stake/key view, N-1 eta, and exact cascade context. The
verifier derives historical `T_weight`; `T_count`, receiver-observed liveness,
wall clock, and current local state have no authority.

Until `O21-01` is answered and the O-01/O-11/O-12/O-15/O-16/O-18 engineering gates close,
`TipAttestation`, `SnowballAccumulator`, and `TWeightTrigger` remain telemetry,
and `DecidedAttestationEvidence` remains opaque and nonactivating.

### O-22 Fraud-proof activation and adjudication

Owner-review packet:
[O-22 Fraud-Proof Activation and Adjudication Owner Review](O22-FRAUD-PROOF-ACTIVATION-OWNER-REVIEW.md).

**OWNER RESPONSE REQUIRED; NONEMPTY ACTIVE-ERA PROOFS MUST BE CONTAINED.** The
current producer copies a node-local fraud pool into the globally signed artifact,
and GSAM can turn locally upheld evidence into rooted stake, bounty, cooldown, and
field-34 effects. The active-era shape validator does not reject this field, while
failed or unavailable adjudication is currently mapped to no slash and continued
acceptance. This is live consensus authority, not dark scaffolding.

The owner must disposition `O22-01` through `O22-03`: retain a mandatory-empty
current-era field versus remove it until an activating era; adopt atomic
reject/defer semantics instead of filtering failed proof adjudication; and freeze
the deterministic bounty claimant rule when later evidence proves additional
signers on an already-disputed checkpoint. The packet recommends removing the
unratified current-era field, whole-candidate reject/defer, and bounties funded only by each newly
proven signer's actual debit.

No answer activates slashing. Exact proposal-parent adjudication, per-signer
deduplication, structural-invalid signer accountability, rooted policy, principal
conservation, field-34 Scodec/key grammar, historical atomic KES+VRF context,
watchtower coverage, resource bounds, and density-reorg behavior remain mandatory
engineering gates.

### O-23 Slash liability and bond tranches

Owner-review packet:
[O-23 Slash Liability and Bond Tranche Owner Review](O23-SLASH-LIABILITY-OWNER-REVIEW.md).

**OWNER RESPONSE REQUIRED; ECO-06 IS NOT A PENDING-MAP PATCH.** The current
manager selects whatever active records point at a signer when the proof is
included. It therefore misses offense-time principal moved to pending and can
charge delegation created after the bad signature. Existing historical stake
state commits only an aggregate per operator and cannot identify the liable
locks across successor/replacement lineages.

The owner must disposition `O23-01` through `O23-06`: infraction-time stable
`BondId`/tranche liability versus slash-time operator-wide liability; full-only
InvalidStateProof V1 versus a new residual-lock design; the pending/release
horizon; slash-before-release same-candidate ordering; actual-debit bounty and
reward treatment; and density-reorg culpability. The packet recommends
infraction-time tranches, full-only V1, pending slashability through the complete
liability horizon, slash-before-release, bounty funded only by exact principal
debit with rewards separately burned, and revalidated culpability after a later
density reorg.

No O-23 answer activates the live path. O-20/O-22, exact historical Phase-2
context, per-signer adjudication, resource bounds, atomic accumulator integration,
serde, restart, and reorg gates remain mandatory.

## Change rule

Changing a locked answer or closing an engineering freeze gate requires one coherent change
that updates ADR-0016/0017 as applicable, the lifecycle, roadmap, test plan, this
register, canonical parameter/schema definitions, and associated RED/oracle
tests. No implementation commit may bury a protocol decision in HOCON or a
caller branch.
