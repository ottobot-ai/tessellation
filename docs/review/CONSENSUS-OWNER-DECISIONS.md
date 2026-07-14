# Consensus Owner Decision Register

**Status:** Active. Runtime packets may not silently choose an `OPEN` item.
**Updated:** 2026-07-13

This register uses project phases only where the owner has ratified them:
Phase 0 `Pending`, Phase 1 `Provisional`, and Phase 2 `Operational`. `k2` is a
retention/recovery recommendation, not a separate consensus-finality floor.

## Locked decisions

| ID | Decision |
|---|---|
| L-01 | Topology remains `GL1 -> GL0` and `CL1/DL1 -> ML0 -> GL0`, with exact Phase-2 GL0 state flowing back downstream. |
| L-02 | GL0 consensus is Nakamoto/Taktikos/LDD chain consensus. No global proposal/vote/lock/QC/view-change BFT protocol is permitted. ML0 may retain BFT consensus for its small, well-connected metagraph validator set and remains subordinate to GL0. |
| L-03 | Avalanche/Snowball is only the optimistic GL0 Phase-2 finality rail. It does not validate economics and does not create a BFT lock or commit certificate. Phase 2 is reached by decided-attestation `T_weight` **or** canonical `k1` depth. `T_count` is removed or made non-authoritative. |
| L-04 | Within the `k1` comparison window, valid GL0 tines use the ratified Taktikos `maxvalid-tk` rule. Beyond `k1`, valid competing tines use the Ouroboros Genesis-family `maxvalid-bg` density rule from the true common ancestor. Phase-2 state remains density-reorgable. |
| L-05 | `k2 = 100 * k1` remains a recommended retention, proof-service, and automatic rollback horizon. It is not an absolute fork-choice, pruning, or finality floor. If the true common ancestor is older than locally retained state, a node must not guess or choose socially; it stops production, fetches authenticated history/state, and resumes only after objective chain comparison and verified reconstruction. |
| L-06 | Metagraphs and checkpoint execution reference only exact canonical Phase-2 `(ordinal, hash, stateRoot)` GL0 state. Inbound binaries may stage while the live GL0 head is ahead, but execution and global reads never use an unqualified live head. |
| L-07 | Every execution-committee signer independently replays the exact ordered framework inputs at the exact signed Phase-2 base and signs only when its byte-identical diff, extracted intents, and resulting root match. Missing inputs mean defer/no-sign. |
| L-08 | Ordinary noncommittee GL0 nodes verify the distinct execution `kQuorum`, exact base, scope, continuity, diff, and resulting root, then adopt the diff without recreating the ordinary currency snapshot. Shard depth never substitutes for missing replay signatures. |
| L-09 | Deterministically assigned noncommittee watchtower replay provides the execution-threshold collusion backstop. Positive required replay coverage precedes GL0 inclusion eligibility. A valid challenge triggers exceptional bounded universal GL0 replay of the exact retained base and inputs; the replay result, not the assertion, decides rollback/slash. |
| L-10 | Per-metagraph diffs are namespace-confined. Every GL0 node executes the small deterministic global conflict/nullifier/settlement kernel over committee-extracted signed intents. No shard or metagraph writes another metagraph's namespace or the GL0-owned global settlement namespace directly. |
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

Implementation status for L-15A is **OPEN**. The current currency incremental
contains the `globalSnapshotSync` proof hash and accepted `globalSnapshotSyncs`
delta, not the exact full-view preimage, while GL0 still writes, reconstructs,
and strips field 32 on different storage paths (ECO-F32).

## Open decision gates

These are the remaining choices that cannot safely be hidden inside an
implementation packet. They do not reopen the locked architecture above.

### O-01 Avalanche population and exact parameters

Freeze the delayed canonical registry/weight snapshot, `K`, `alpha`, `beta`,
`T_weight`, sampling replacement rule, attestation lifetime, and small-network
failure mode. The intended epoch pattern is stake/registry from epoch `N-2` and
eta from epoch `N-1` for epoch `N`, subject to reference-model and grinding tests.

### O-02 Deep-history recovery beyond local `k2`

Specify the authenticated archive/bootstrap protocol when a winning
`maxvalid-bg` tine's true common ancestor predates local rollback state: required
proof material, state reconstruction, production halt/resume conditions, peer
diversity, and atomic recovery of MPT, mempool, checkpoints, downstream events,
tower state, and eta/registry history. The objective fork-choice result remains
mandatory; manual operation may start recovery but cannot choose the winning
tine.

### O-03 Watchtower parameters and availability fallback

Freeze complement/sample size, minimum positive coverage, assignment anchor,
deadline, retry/redraw, bonds, replay budget, challenge lifetime, and censorship
fallback. The locked release rule is pre-inclusion positive coverage; these
parameters must preserve unrelated GL0 progress without letting an unchecked
checkpoint-derived economic effect become usable.

### O-04 ML0 response to a Phase-2 density reorg

Specify the exact retained history and deterministic rewind/rebase/new-epoch
contract. Append-only audit history is preferred; applications with a registered
deterministic rebase/undo contract may append a corrective snapshot. Otherwise a
new ML0 epoch begins at the last valid GL0 reference. Noninvertible external
effects are an integrator risk decision and are not made irreversible by a new
protocol phase.

### O-05 Intake threshold and censorship recovery

Freeze intake threshold, receipt/custody lifetime, queue ownership, durable
replication requirement, redraw schedule, and direct-fetch/censorship fallback.
The threshold proves intake/availability only and remains unable to satisfy
execution `kQuorum`.

### O-06 Global correction authorization

Define how a GL0 protocol correction is authorized and activated in a
permissionless network: hard-fork/era rule, a future canonical governance rule,
or another deterministic GL0-level mechanism. Freeze its signed domain, exact
metagraph/pre-root/version target, correction diff, post-root, reason, activation
ordinal, replay protection, downstream rebase behavior, and audit trail. No
metagraph-originated authority is an option.

### O-07 Economic operation grammar

Audit Tessellation v4.0.0 behavior for every framework operation, then preserve
the functionality that has explicit deterministic authority, conservation,
ordering, and replay semantics. Do not invent treasury or oracle authority and
do not disable existing functionality merely because its rule has not yet been
restated. Any defective upstream behavior receives an explicit protocol rule and
RED/oracle vectors before enablement.

`V4-ECONOMIC-GRAMMAR-AUDIT.md` records partial source evidence. It confirms
that manual owner unlock and metagraph-source spend are intended v4 features,
while proving that their inherited authority checks are insufficient. The gate
remains open until a mechanical constructor/codec/event/acceptance reachability
inventory and the full differential corpus are complete.

### O-08 Tower proof contract

Freeze snapshot-carried per-level state/pointers, trial computation, historical
`N-2` registry and `N-1` eta inputs, KES/VRF verification, SMT inclusion path,
proof comparison, size limits, cache reconstruction, and density-reorg rollback.
A single GL0 peer may supply a proof, but it must verify from a trusted genesis or
cached canonical commitment; the peer can withhold freshness but cannot choose
proof truth.

### O-09 Pure opaque state-channel product scope

`FrameworkCurrency` and `FrameworkCurrencyWithData` are locked. Whether a future
standalone opaque/data-only lane exists remains a product decision. If retained,
it has no framework-economic write surface and receives only authenticated
inclusion/availability semantics.

### O-10 Portable shard-parent duty and slot bound

Freeze the portable evidence used to validate a child checkpoint's parent-relative
staircase duty. The current worktree resolves the parent from a receiver-local shard
chain store, so a GL0 producer that saw shard gossip can accept an artifact that a
follower missing that gossip rejects. Parent availability may delay validation, but
prior receipt cannot be a validity input.

**Recommendation:** add an exact `(ordinal, hash)` reference to the GL0 snapshot that
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

Freeze the canonical rule that turns a `PeerId` into an eligible GL0 operator:
required registration, stake and/or collateral, minimum bond, activation and exit
delay, slash/cooldown interaction, and the exact period-boundary root retained for
historical verification. Key registration proves ownership only and cannot grant
membership.

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
(`HistoricalOperatorConsensusKeyRegistry.scala:169-198`); that may be an
owner-ratified backing predicate, but cannot define the roster by itself. Neither
the seedlist, observed peers, a valid key record, nor positive stake is an interim
authority.

**PROPOSED, NOT RATIFIED - atomic historical population boundary.** At the exact
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
consumer resolves one exact candidate-parent `(ordinal, hash, stateRoot)` capability:

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

**Owner answers still required before schema or runtime work:**

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

After those answers, delivery order is: canonical codec/root ownership, immutable
genesis population, pure boundary derivation and undo/refold, exact-parent N-2/N-1
resolver, exact Phase-2 artifact references, all-consumer migration, then fixture
and adversarial qualification. O-11 remains open until that sequence passes the
`KEYREG-013`, `PERM-*`, and `PARAM-001` gates.

### O-12 Runtime KES secret deletion and N-2 reorg boundary

Freeze the common-prefix assumption under which an operator may delete/evolve
past KES secret material after a paired runtime registration becomes active.
Eligibility in period `N` reads the registration/roster/stake prefix at `N-2`,
but Phase-2 state remains density-reorgable. KES secret state is deliberately
one-way and must not be modeled as another MPT field that ordinary Phase-2
rollback can restore.

The recommended baseline is: the activation policy explicitly assumes the N-2
prefix is common-prefix stable at the ratified security bound; an objective
density reorg that nevertheless crosses an activation whose prior KES secret was
erased puts the operator in durable `RecoveryRequired`. It stops all consensus
signing, follows only authenticated objective chain recovery, and requires an
explicit operator realignment/rejoin procedure for a newly preregistered pair.
The operator cannot choose the winning branch and missing secret history cannot
be guilt or slash evidence.

Retaining old KES masters through the local `k2` horizon would allow automatic
secret rollback only by weakening the KES forward-security guarantee. That is
not an implicit retention policy. Ratify the exact deletion point, the quantified
common-prefix failure probability, whether any offline escrow is permitted, and
the recovery/rejoin availability consequences before runtime key activation.

### O-13 Durable global delivery sequence and settlement ordering

**PROPOSED, NOT RATIFIED.** Replace `GlobalSnapshotsProcessed` and every bounded
history reconstruction with a hash-linked sequence per destination metagraph.
GL0 atomically appends canonical framework delivery records and advances a rooted
outbox head with the settlement and permanent authorization nullifier. ML0 stores
an applied `(sequence, deliveryId)` cursor in `CurrencySnapshotInfo` and its state
proof, executes only a contiguous range from one exact Phase-2 GL0 reference, and
emits a compare-and-set acknowledgement. Acknowledgement changes representation
only: effective balances before and after compaction are byte-identical and the
permanent economic nullifier remains.

The recommended granularity is a bounded canonical per-metagraph batch with
contiguous multi-entry acknowledgement. GL0 ordinals are not delivery sequence
numbers: they are sparse, and ML0 may discover an older still-pending delivery
after observing a higher GL0 ordinal. GL0 assigns its destination sequence at the
canonical append; receiver observation order cannot change or skip it. Pending records are
individual rooted leaves, not one growing field-18 ordinal set. Their framework
bytes remain available until acknowledged; missing bytes defer rather than
authorizing a peer claim or skipping an effect.

Owner answers still required before schema allocation:

- the exact total order among an already-canonical inbound delivery, local spend,
  consume, cancel, expiry, and refund; inbox-before-local-spend is the recommended
  fixed first rule, but it does not settle the other conflicts;
- rooted limits for record bytes, entries per batch/snapshot, pending entries,
  deterministic backpressure, and protocol fees; local HOCON is not validity;
- exact retention and authenticated deep-recovery behavior after local history is
  unavailable; and
- O-04's rewind/rebase/new-epoch outcome after ML0 applied a Phase-2 reference that
  a later density reorg orphans.

No active field number is assigned here. Greenfield active state uses the frozen
Scodec era from ordinal zero; upstream v4 data remains read-only import input and
cannot silently default a missing cursor into signable state.

### O-14 Framework fee sequence and opaque-data binding

**PROPOSED, NOT RATIFIED.** Reuse the already rooted per-metagraph
`MgLastFeeTxRefs` partition (field 27) as the strict head for each fee source. A
framework fee signs network/genesis, era/lane, metagraph, source, destination,
amount, exact parent reference, and an exact opaque-data commitment. Acceptance
requires the parent to equal rooted state, derives the successor reference, checks
available bytes or a content-addressed chunk manifest without executing DL1 logic,
and atomically updates balances plus the head. Signature proofs do not change the
semantic fee identity.

Recommended v1 rules are:

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

The remaining owner choice is exact signed-item bytes versus a content-addressed
chunk-manifest root. The recommendation is the manifest only when every exact chunk
is available before an execution signature. GL0 verifies bytes, availability,
authorization, sequence, arithmetic, and conservation; it never treats ML0 custom
execution output as economic authority. The outer `StateChannelSnapshotBinary.fee`
is a separate global-balance operation and still requires E9's checkpoint-wide
reservation kernel.

## Change rule

Changing a locked answer or resolving an open gate requires one coherent change
that updates ADR-0016/0017 as applicable, the lifecycle, roadmap, test plan, this
register, canonical parameter/schema definitions, and associated RED/oracle
tests. No implementation commit may bury a protocol decision in HOCON or a
caller branch.
