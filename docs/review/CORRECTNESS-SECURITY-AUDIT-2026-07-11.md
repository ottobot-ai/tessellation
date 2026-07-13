# Correctness and Economic Security Audit - 2026-07-11

> **Design correction (2026-07-12):** the source findings remain audit evidence,
> but this report's original fixed-set vote-lock/quorum-certificate repair was
> rejected by the owner. GL0 remains Nakamoto/Taktikos/LDD. Phase 2 is reached by
> an exact-hash Avalanche-style decided attestation or canonical `k1` depth and is
> reversible under objective density chain selection. There is no global BFT
> proposal/vote/lock/QC/view-change protocol and no immutable finalized-hash floor.
> The owner register and roadmap are normative for repair direction.

## Scope and standard

This is a source audit of the greenfield Nakamoto fork relative to pre-audit commit
`725b25bdaf3e90d9d1f59f42875b3d948cfe239a` and upstream tag `v4.0.0`. Documents are
claims, not evidence. `CONFIRMED` means the cited code is sufficient to construct the
failure. `PLAUSIBLE` means the path exists but the final cross-platform or runtime
step was not reproduced.

The target architecture is stricter than upstream v4.0.0: GL0 is the sole canonical
framework-economic state machine. An ML0 signature, shard root, committee quorum,
DL1 artifact, receipt, or later slash cannot alone authorize a CL1 balance transition.

## 1. Executive verdict

**No: the intended economic invariant is not upheld today.** After the fixes in this
audit, every reachable checkpoint-adoption path structurally invokes the GL0 currency
recreation code before adopting a CL1 root. That closes the original roots-only and
quorum-only bypass. It does not establish economic correctness because the transition
function itself still accepts economic authority that GL0 cannot independently verify:

1. A finite replacement of a live backing lock can expire while its
   delegated-stake/collateral record survives, letting refunded capital be reused to
   create unbounded voting stake
   (`ContextualTokenLockValidator.scala:107-117,167-182`,
   `DelegatedStakeStateManager.scala:84-108`, `NodeCollateralStateManager.scala:94-125`).
2. The legacy optimistic rail treats latest attestations as finality without the
   ratified K/alpha/beta decided cascade and renormalizes a process-local observed-
   active half of stake to 100%. Two partitions can mark different valid economic
   histories operational (`StakeRegistry.scala:448-469`, `TipTracker.scala:193-201`).
3. ML0 can supply an unsigned `TokenUnlock` for another user's active lock; GL0 checks
   public fields but no owner signature or global condition, then refunds the lock
   (`artifact.scala:35-41`, `TokenLockOpsManager.scala:73-78,117-127,160-173`).

Consequently, universal execution is now a real path property, but **universal economic
enforcement is not**. The repository is not safe to deploy with economic value.

## 2. Verified architecture

| Name | Runtime/source identity | Actual relationship |
|---|---|---|
| GL0 | Global L0, DAG L0, `dag-l0`, `gl0.jar` | Global snapshot consensus, canonical MPT, framework-economic settlement, and finality. |
| GL1 | Global L1, DAG L1, `dag-l1`, `gl1.jar` | Native DAG-token edge app; submits blocks directly to GL0. |
| ML0 / CL0 | Metagraph/Currency L0, `currency-l0`, `ml0.jar` | Metagraph snapshot consensus; combines CL1 economics and DL1 data into a state-channel binary for GL0. |
| CL1 | Currency L1, `currency-l1`, `cl1.jar` | Framework metagraph economic blocks; submits to ML0. |
| DL1 | Data L1/custom app, `dl1.jar` | A `CurrencyL1App` with a data-application service; submits custom data to ML0. |
| ML1 | Concept only | Umbrella for CL1 and DL1; no formal runtime module. |

The formal enum has only `DagL0`, `DagL1`, `CurrencyL0`, and `CurrencyL1`
(`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/app/Layer.scala:3-7`).
DL1 injection is the `CurrencyL1App` data-service seam
(`modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/CurrencyL1App.scala:53-57`).

```text
native DAG:       client -> GL1 -> GL0
metagraph:        client -> CL1 --+
                                    +-> ML0 -> GL0
                  client -> DL1 --+
canonical return: finalized GL0 -> GL1 / ML0 / CL1 / DL1
```

ML0 operators sign metagraph binaries. They are not either GL0 committee. Binary
admission and execution-shard committees are separate draws over eligible GL0
operators. Admission uses secret VRF self-sortition per `(eta, metagraph, parentHash)`
(`CommitteeSortition.scala:19-31,61-81`). Execution membership is public deterministic
VK-hash selection per `(eta, shard, epoch, registeredVrfVk)`
(`CommitteeSortition.scala:203-234`). Both currently use uniform `1/N`, not stake
weight, but admission's denominator is the full GL0 validator set
(`StakeRegistry.scala:474-478`; `GlobalSnapshotConsensus.scala:1505-1509`) while
execution uses the post-slash eligible pool (`ShardCheckpointWiring.scala:596-619`).

Metagraph assignment is static:
`unsignedBigEndian(SHA-256(address)) mod numShards`
(`ShardAssignment.scala:54-70`). Honest producers derive execution membership from the
eta period of the finalized GL0 anchor (`SnapshotLeaderLoop.scala:187-194,943-960`), not
from every checkpoint. The worktree now recomputes the wire epoch from the signed anchor ordinal
before committee lookup, rejecting an inconsistent pair. It does not close grinding: selection accepts any
`gl0AnchorOrdinal <= currentOrdinal`, so a producer can choose an older ordinal and its matching favorable epoch.
The ordinal lacks exact proposal-parent Phase-2 hash/freshness proof and `R` is node-local configuration
(SHARD-03/SHARD-09). `R = round(3.1*k1)`: 3174/794/99 ordinals under mainnet,
test/integration, and dev defaults (`config/types.scala:161-174`;
`application.conf:308-313`). Within the fixed
committee, hash-ranked producer duty changes every five slots; the genesis window is
12 times wider (`ShardCheckpointProducer.scala:509-524`; `application.conf:539-540`). Advancing an ML0 parent hash
redraws the separate admission committee.

Cross-shard metagraphs do not directly trust each other. The consuming GL0 transition
reads owner state from the finalized base MPT (`GlobalSnapshotAcceptanceManager.scala:518-522,2203-2217`),
re-executes the framework operation, and writes a permanent type-specific nullifier
(`ConsumedAllowSpendStateManager.scala:179-221,245-274`;
`CrossShardMessageHandler.scala:118-156`; `GlobalSnapshotAcceptanceManager.scala:2868-2873`).
ML0 reads the finalized GL0 pending ordinals, fetches and replays the SpendActions,
applies them to its next currency snapshot, and emits `GlobalSnapshotsProcessed`
(`GlobalSnapshotOpsManager.scala:58-150`; `CurrencySnapshotAcceptanceManager.scala:478-513,576-615,672-676`).
GL0 then verifies that snapshot; CL1 later adopts finalized GL0 MPT state without
re-execution. Only cross-shard allow-spend consumption is implemented through this
generic seam today; other message types are future handlers
(`CrossShardMessageHandler.scala:61-64`; `GlobalSnapshotAcceptanceManager.scala:536-537`).

## 3. Findings

| ID | Sev | Class | Subsystem | Evidence | Defect and exploit | Verdict | Fix direction |
|---|---|---|---|---|---|---|---|
| FIN-01 | CRITICAL | DESIGN | optimistic finality | `TipTracker.scala:193-201`; `SnapshotLeaderLoop.scala:1128-1149` | Latest branch attestations are treated as finality weight without a K/alpha/beta decided cascade. Split observations can make different nodes mark different exact hashes operational. | CONFIRMED | Hash-bound portable decided-attestation evidence under the ratified Avalanche cascade; remove/subsume `T_count`; no global lock or QC. |
| FIN-02 | CRITICAL | DESIGN | optimistic finality | `StakeRegistry.scala:384,448-469` | Six equal validators cold-start in 3/3 partitions. Each half meets the 1/2 active floor, renormalizes itself to 100%, and finalizes a conflicting branch. | CONFIRMED | Never renormalize finality denominator to local observations; bind a fixed epoch set. |
| FIN-03 | CRITICAL | DESIGN | depth finality | `FinalityTrigger.scala:217-235`; `SnapshotLeaderLoop.scala:1182-1231` | A long-lived partition can grow `k1` successors and expose its local branch as Phase 2. Current consumers lack the exact-hash rollback contract needed when density selection later chooses the competing branch. | CONFIRMED | Keep canonical `k1` as the fallback Phase-2 trigger, make Phase 2 explicitly reversible, and atomically roll back/replay every derived capability on density replacement. |
| FIN-04 | CRITICAL | IMPLEMENTATION | fork choice | `ChainSelection.scala:142-155`; `application.conf:381-392` | The default clamp inconsistently protects only when current tip equals the locally finalized hash. It is neither a correct descendant rule nor the ratified density-reorg model. | CONFIRMED | Remove the absolute finalized-hash clamp; persist exact phase state and let objective maxvalid-tk/maxvalid-bg selection emit a durable replacement transaction. |
| FIN-05 | CRITICAL | IMPLEMENTATION | finality store | `NakamotoChainStore.scala:420-439,705-719` | Protected hash is chosen with unordered `byHash.values.find`. A surviving same-height alternate can make canonical replay look divergent and increment the reset trigger. | CONFIRMED | Store exact hash-bound phase identity explicitly and make replacement branch-aware; never recover phase truth through unordered lookup. |
| FIN-06 | CRITICAL | IMPLEMENTATION | restart/recovery | `dag-l0/Main.scala:118-152`; `RebootstrapOrchestrator.scala:60-80,203-235,243-287`; `NakamotoChainStore.scala:815-834` | Phase/finality refs restart empty; default rebootstrap erases them after three cumulative refuses. Restart/reset loses the state needed to reverse released capabilities correctly. | CONFIRMED | Durable atomic exact-hash phase state and rollback outbox; peer input cannot erase it, while objectively selected density replacement remains possible. |
| FIN-07 | CRITICAL | IMPLEMENTATION | finality atomicity | `SnapshotLeaderLoop.scala:1203-1231,1331-1347`; `NakamotoChainStore.scala:415-442` | Store finalization precedes MPT/outbox work and the external write-gate ref. A conflicting write at the just-finalized ordinal can enter during the gap. | CONFIRMED | One atomic durable store/MPT finality transaction read by every gate. |
| FIN-08 | CRITICAL | IMPLEMENTATION | self attestation | `NakamotoSyncDaemon.scala:2881-2929`; `SnapshotLeaderLoop.scala:1128-1149` | Self-attestation is recorded before KES/sign/publish; failure is swallowed and never retried. One node counts 3/4 locally while peers see 2/4. | CONFIRMED | Persist and count only a successfully signed attestation from an authenticated locally executed snapshot capability; retry through a durable outbox. |
| FIN-09 | HIGH | IMPLEMENTATION | attestation ordering | `NakamotoSyncDaemon.scala:1741-1755`; `TipTracker.scala:193-201` | Receipt of a snapshot invents a producer attestation stamped with receiver time. Delayed A can overwrite genuine later B on one receiver only. | CONFIRMED | No receiver-invented attestation; bind signer-originated evidence to the exact candidate and cascade parameters. |
| FIN-10 | CRITICAL | IMPLEMENTATION | stake cache | `NodeStakeAggregator.scala:135-155`; `GlobalSnapshotConsensus.scala:936-952` | Cache keys only by ordinal while reading a branch-sensitive tip. Same-height A->B reorg leaves different nodes weighting A or B. | CONFIRMED | Cache by `(ordinal,hash)` from a pinned reader. |
| FIN-11 | HIGH | IMPLEMENTATION | KES | pre-audit `KesGossipVerification.scala:58-65,199-206`; current `KesGossipVerification.scala:58-65,199-206` | A global attestation or snapshot with no registered KES key passed on Ed25519 alone, defeating forward-security during registry gaps. | CONFIRMED, FIXED | Missing registry entries now reject on all KES paths. |
| FIN-12 | CRITICAL | IMPLEMENTATION/PLAN | optimistic finality | `SnowballAccumulator.scala:13-25,118-151`; `SnowballAccumulatorSuite.scala:212-235`; `FinalityTrigger.scala:176-207`; `SnapshotLeaderLoop.scala:1271-1299` | The so-called Snowball accumulator moves a flipping peer's old color count instead of accumulating lifetime confidence, has no K/alpha cascade, and sticks to the first beta-margin crossing. With beta=2, node X can see A1,A2 first and stick A while node Y sees B1,B2 first and sticks B; both can later hold the same latest peer colors. The live state-changing sink ignores even this trigger and finalizes from the separate legacy 2/3 weight read. | CONFIRMED, REPRODUCED | Implement and independently model-test one authenticated exact-hash K/alpha/beta cascade with portable decisions, wire only its decided `T_weight` result as the optimistic P2 rail, and remove the cumulative-weight sink. |
| FIN-13 | HIGH | IMPLEMENTATION/DESIGN | ML0 P2 retention | `StateChannel.scala:753-775`; `StateChannelBinarySender.scala:163-166`; `BinaryTracker.scala:172-190,228-233` | GL0 hash G reaches reversible P2 and contains ML0 binary B. `confirm` deletes B once its confirmation ordinal is at or below the P2 watermark; the monotone GSI currency-ordinal GC also deletes older Pending inputs. A later density replacement of G cannot lower either watermark or requeue B, so ML0's next binary can reference history GL0 no longer has and the metagraph halts. | CONFIRMED, PARTIAL HARDENING IN WORKTREE | `None` now defers pruning, but retain inputs by exact containing hash through replacement/recovery/ack horizons; on P2 replacement atomically restore/requeue each orphaned binary once and make GSI GC branch-aware. |
| CONS-01 | CRITICAL | IMPLEMENTATION | GL0 LDD eligibility | pre-fix `NakamotoSyncDaemon.scala:1371-1373`; current `NakamotoSnapshotValidator.scala:44-70,157-180`; `CatchUpVerificationSuite.scala` test `slot lineage uses the retained parent` | A producer signs `slot=20,parentSlot=0` over an actual parent at slot 19. The receiver used the claimed gap 20 for the LDD threshold and never compared it with the retained parent, so a VRF proof that loses at gap 1 can pass and inject an ineligible chain block. | CONFIRMED, FOLLOWER FIXED IN WORKTREE; PRODUCER PRE-SIGN PARITY OPEN | Derive parent slot only from the exact retained parent certificate and require strict increase before eligibility/storage. The worktree follower gate does so; the leader must run the same pure gate before either Ed25519 or KES signing and local storage. |
| CONS-02 | CRITICAL | IMPLEMENTATION/DESIGN | eta/VRF | pre-fix `NakamotoSyncDaemon.scala:1375-1426`; strict range `NakamotoChainStore.scala:52-65,612-703`; `NakamotoSyncDaemon.scala:1392-1420,1463-1469`; `SnapshotLeaderLoop.scala:841-873`; exact API `EtaStateManager.scala:60-64,121-141,174-198`; exact GSAM parent `GlobalSnapshotAcceptanceManager.scala:1331-1353`; production wiring `SharedServices.scala:105-110,443`; `GlobalSnapshotConsensus.scala:480-502,697`; exact admission `GlobalSnapshotConsensus.scala:1435-1477`; residual ambient shard paths `SharedServices.scala:305-308`; `GlobalSnapshotConsensus.scala:1736-1797,1913-1931`; missing anchor fields `ShardCheckpoint.scala:68-77,95-104` | The original receiver accepted producer-embedded eta when N-1 history was missing, and shared paths accepted a partial prefix or bootstrap on absence. Those fallbacks are closed. `getEtaAt` now bypasses receiver-current MPT state and ambient caching; both production GSAMs pass the exact parent, and admission walks its exact Phase-2 anchor. Shard validity remains unsafe: a checkpoint signs only anchor ordinal/epoch, while committee draw, producer proof, and attester proof resolve ambient `getEta(period)`. After a same-height density reorg, node A can resolve sibling-A eta and node B sibling-B eta for identical checkpoint bytes, yielding different committee/proof verdicts and a shard halt or acceptance split. | CONFIRMED; GL0/ADMISSION EXACT-PARENT FIXED, SHARD ARTIFACT/WIRING OPEN | Add the exact canonical Phase-2 `(ordinal,hash,mptRoot)` to the signed checkpoint; use it for `getEtaAt` in committee selection, production, attestation, and verification. Missing/partial/wrong-branch history fetches or defers. Ratify a portable canonical result for a genuinely complete empty source period and the Taktikos/LDD nonce window rather than importing a Praos bound. |
| CONS-03 | CRITICAL | IMPLEMENTATION | GL0 delayed stake | pre-fix `SnapshotLeaderLoop.scala:809-812`; pre-fix `NakamotoSnapshotValidator.scala:114-128`; current `EtaCalculation.scala:35-47` | After the attacker rises from 1% at N-2 to 50% live stake, it ignores the honest producer helper and submits a VRF output between the two thresholds. Producers are specified to use N-2, but followers verified against live stake and accepted the otherwise ineligible block, defeating the delayed-stake chain-quality rule. A stake decrease caused the inverse honest-block rejection. | CONFIRMED, FIXED IN WORKTREE; HISTORICAL AVAILABILITY OPEN | Producer and follower now derive one `leaderStakeLookbackPeriod(parentOrdinal,R)` and call `relativeStakeAt`. Missing historical distribution still falls back to live/equal stake and remains a separate fail-closed parameter/history obligation. |
| CONS-04 | CRITICAL | IMPLEMENTATION | GL0 delayed-stake recovery | `StakeRegistry.scala:332-360,480-504` | At the same parent, node A retains N-2 distribution `attacker=1%` and rejects output x; restarted node B lacks that historical record, silently substitutes live MPT `attacker=50%`, and accepts x. If live state is also absent it substitutes 1/N. Identical signed bytes therefore have different validity based on cache/restart state. | CONFIRMED, OPEN | `relativeStakeAt` must return typed unavailable outside the explicitly bounded genesis warmup. Fetch and authenticate the exact hash-bound historical distribution before signing/validation; never substitute live/equal state for a missing mature period. |
| CONS-05 | CRITICAL | IMPLEMENTATION | GL0 VRF identity | pre-fix `NakamotoSnapshotValidator.scala:121-131,169-174`; current `ActiveOperatorConsensusKeys.scala:20-49`; `NakamotoSnapshotValidator.scala:44-63,139-155`; `LocalOperatorKeyPairGate.scala:118-197`; `L0GenesisLoader.scala:442-515` | The live GL0 receiver previously verified eligibility under the sender-carried VK, so an otherwise legitimate validator could grind disposable VRF keypairs for `(eta,slot)` and submit the winning unregistered key. Producer and receiver now accept only the unique atomic period-zero KES+VRF pair materialized from rooted signed genesis state; carried keys are comparison evidence. Runtime records deliberately fail this current-only API. | CONFIRMED; PERIOD-ZERO BYPASS FIXED IN WORKTREE, RUNTIME HISTORICAL ELIGIBILITY OPEN | Keep the period-zero gate. Complete E2K by constructing the exact-parent historical resolver in production, intersecting its N-2 active pair view with the separately rooted authorized N-2 roster/stake view, using N-1 eta, and migrating every VRF consumer. Registration alone never grants eligibility. |
| CONS-06 | CRITICAL | IMPLEMENTATION/PLAN | runtime KES/VRF identity | public record/history `KesRegistrationCert.scala:28-47,94-104`; acceptance/timing `KesRegistrationCertValidator.scala:84-141,191-255`; `KesRegistrationCertAcceptanceManager.scala:128-153`; rooted writes `GlobalSnapshotAcceptanceManager.scala:1741-1803,2843-2844`; isolated resolver `HistoricalOperatorConsensusKeyRegistry.scala:111-203,239-272,420-485`; current-only gate `ActiveOperatorConsensusKeys.scala:14-43`; local secrets `LocalOperatorKeyPairGate.scala:110-123,174-185`; `OperationalKeyMaker.scala:9-20,37-90` | Acceptance enforces one signed KES+VRF record, exact registration parent/sequence, and `signedEffectivePeriod >= inclusionPeriod+2`; rooted histories and an isolated N-2 resolver exist. This is the intended period-index lookback: period N uses the exact N-2 view and N-1 eta. The cert has no explicit network/genesis/era domain or containing-hash/root witness. Cross-operator key reuse rejects, but the same operator may reuse both keys or submit a successive complete record in which only one key changes because acceptance never compares the new pair with its prior pair. Atomicity proves both active keys came from one selected record; it does not decide whether either key must differ from the preceding record. Production also lacks the rooted authorized roster, exact artifact context, historical resolver wiring, a ratified same-owner reuse/rotation policy, and matching local secret lifecycle. If R1 activates in the model, the honest node still holds/derives R0 secrets and must stop signing; ignoring the mismatch emits universally rejected proofs/signatures. An N-2-crossing density reorg cannot restore an erased KES secret as ordinary state. Enough rotations or such a reorg halt honest production. | CONFIRMED; RECORD/PERSISTENCE/MODEL LANDED, PRODUCTION SECRET/ACTIVATION/REORG POLICY OPEN | Add explicit domain and containing-state witnesses; ratify same-owner reuse/rotation semantics. Atomically provision and store one VRF secret plus KES tree before registration; exact-parent eligibility selects the pair and compares both public keys. Root the roster, bind exact contexts, migrate all consumers, and ratify O-12 recovery. |
| ECO-01 | CRITICAL | IMPLEMENTATION | collateral/stake | pre-audit `UpdateNodeCollateralAcceptanceManager.scala:74-93`; current `UpdateNodeCollateralAcceptanceManager.scala:48-99`; `UpdateNodeCollateralValidator.scala:226-240`; `NodeStakeAggregator.scala:82-113` | Before this audit, many distinct same-round creates over one lock all read unchanged prior state and each added full stake. Fee variation made events distinct. | CONFIRMED, FIXED | Preserve the evolving source/ref/parent/node duplicate sets and the MPT-backed prior-record check as regression invariants. |
| ECO-02 | CRITICAL | DESIGN/IMPLEMENTATION | stake backing | `ContextualTokenLockValidator.scala:107-117,167-182`; `DelegatedStakeStateManager.scala:84-108`; `NodeCollateralStateManager.scala:94-125` | Replace an indefinite backing lock with a slightly larger finite lock; old principal refunds, replacement expires/refunds, but stake record remains and keeps voting/reward weight. Repeat across addresses for unbounded stake. | CONFIRMED | Every counted record must join to one live, indefinite, uniquely bound lock; replacement must preserve binding/conditions atomically. |
| ECO-03 | CRITICAL | DESIGN/IMPLEMENTATION | token unlock | `artifact.scala:35-41`; `TokenLockOpsManager.scala:43-59,73-78,117-128,160-173` | Byzantine ML0 supplies public fields of victim lock L as unsigned TokenUnlock; GL0 removes L and refunds victim early, defeating vesting/stake/escrow conditions. | CONFIRMED | GL0 derives expiry unlocks, or verifies an owner signature/framework condition. Never accept DL1 authority. |
| ECO-04 | CRITICAL | DESIGN | spend action | `artifact.scala:23-33`; `SpendActionValidator.scala:319-328,372-382` | No-ref SpendTransaction is unsigned. ML0 can transfer the producing MG treasury to an attacker; GL0 checks only source==MG and balance. | CONFIRMED | Explicitly define ML0 treasury authority or require a globally verifiable signed policy; disallow no-ref spend otherwise. |
| ECO-05 | CRITICAL | IMPLEMENTATION | replay/nullifier | `CurrencySnapshotCreator.scala:90-98,346-383`; `currency-l0/Services.scala:124-136`; `SharedServices.scala:211-223`; `GlobalSnapshotOpsManager.scala:58-77,99-112`; `application.conf:162` | ML0 reconstructs processed history at depth 50 while GL0 recreation defaults to 1. After acknowledgement falls out, malicious C3 matches GL0's replay and applies global spend g twice. | CONFIRMED | Durable canonical processed/nullifier set; no bounded-history correctness. |
| ECO-06 | CRITICAL | IMPLEMENTATION | slashing | `InvalidStateProofSlashManager.scala:96-172`; `GlobalSnapshotAcceptanceManager.scala:302-352,1892-1950,2595-2641` | A 100% slash removes stake metadata but not its token-lock principal. Replacement/expiry recovers principal; a watchtower bounty can be credited from a nominal, never-debited pool. | CONFIRMED | Debit/remove the backing lock and expiry index atomically; bounty <= actual debit. |
| SLASH-01 | CRITICAL | IMPLEMENTATION | watchtower evidence identity | pre-fix `InvalidStateProofValidator.scala:90-193`; `InvalidStateProofEvidence.scala:67-71`; `GlobalSnapshotAcceptanceManager.scala:1921-1933`; current `ShardCheckpointGl0AcceptanceManager.scala:155-162,330-341`; `InvalidStateProofValidator.scala:135-149` | An attacker constructed a wrong-root checkpoint whose fake committee records named honest PeerIds, signed only the challenger envelope, and obtained an upheld replay mismatch; GSAM derived slash targets directly from those unauthenticated IDs and applied a 100% slash. | CONFIRMED; ARBITRARY-ID FALSE SLASH FIXED IN WORKTREE, HISTORICAL PORTABILITY OPEN | Fraud adjudication now requires the exact distinct execution quorum plus Ed25519, registered KES, registered VRF possession, and deterministic committee membership before replay can uphold. Completion still requires exact checkpoint-parent rooted historical key/eta/roster lookup and typed retain/defer on unavailable history; frozen/current/local substitution cannot decide a slash. |
| ECO-07 | CRITICAL | IMPLEMENTATION | fee arithmetic | pre-audit `BalanceOpsManager.scala:77-101`; current `BalanceOpsManager.scala:77-105` | From balance 0, signed fees Max and Max-1 to two destinations wrapped the source to 3 while crediting nearly 2^64 units. | CONFIRMED, FIXED | BigInt accumulation plus range check; replay protection remains open. |
| ECO-08 | CRITICAL | DESIGN/IMPLEMENTATION | balance authority | removed `CurrencyBalanceAdjustments.scala:43-146` | At configured MG/ordinal, ML0 could append an extra BalanceAdjustment and mint it; validation required expected entries but did not reject extras. | CONFIRMED, FIXED | Variant, resource, loader, and mutation path removed; discriminator 0x04 unused. |
| ECO-09 | CRITICAL | IMPLEMENTATION | sharded replay prior | pre-audit `GlobalSnapshotAcceptanceManager.scala:2001-2008,2198-2210,3165-3276` | Parent incremental was paired with finalized-base balances/allow-spends. A lock/allow-spend consumed in G1 could be resurrected in child G2 and consumed again. | CONFIRMED, FIXED | One coherent checked-out parent reader for incremental, info, removals, and replay. |
| SHARD-01 | HIGH | IMPLEMENTATION | checkpoint adoption | pre-audit `GlobalSnapshotAcceptanceManager.scala:930-946,2348-2385`; current `GlobalSnapshotAcceptanceManager.scala:3080-3099` | Watermark/anchor advanced before suffix replay/root filtering. A rejected B was marked adopted and no longer offered, freezing the mirror. | CONFIRMED, PARTIAL FIX | Acknowledgement now follows successful replay/root match. It is still accept-path local state, not a finalized-GL0 event; move it to the finality hook. |
| SHARD-02 | HIGH | IMPLEMENTATION | reanchor | removed `ShardReanchor.scala:91-124`; current `ShardWindowContinuation.scala:11-44` | If GL0 committed sibling X and the shard chooses same-ordinal sibling Y then child Z, neither Y nor Z continues X. The unsafe ordinal jump is removed, but every window is now deferred and that MG halts because rollback/full replay is absent. | CONFIRMED, UNSAFE PATH FIXED; LIVENESS OPEN | Implement global rollback and full canonical replay before enabling sibling healing. |
| SHARD-03 | HIGH | IMPLEMENTATION | committee rotation | `GlobalSnapshotConsensusFunctions.scala:641`; `ShardCheckpoint.scala:68-80`; current `EtaCalculation.scala:35-46`; `ShardCheckpointGl0AcceptanceManager.scala:423-437` | A Byzantine producer chooses an older resolvable anchor ordinal E' whose public committee it controls, writes the matching `epoch=floor(E'/R)`, and supplies valid E' signatures. GL0 selection requires only `anchor<=currentOrdinal`; the worktree equality check rejects malformed inconsistent pairs but accepts this self-consistent stale pair. Replay can resolve historical state, so the attacker can monopolize/censor the shard and evade the intended current rotation/cooldown selection. | CONFIRMED, OPEN; NARROW HARDENING IN WORKTREE | Derive the only admissible anchor from exact proposal-parent Phase-2 `(ordinal,hash)` evidence with an explicit freshness/lineage rule, then derive epoch/eta/roster from it; bind R through a canonical parameter hash. |
| SHARD-04 | HIGH | IMPLEMENTATION | multi-MG atomicity | pre-fix `184f7863:GlobalSnapshotConsensusFunctions.scala:637-659,735-741`; current `ShardWindowContinuation.scala:51-105`; `GlobalSnapshotConsensus.scala:2066-2124` | Checkpoint `C={A continues,B deferred}` passed the old `exists` selector. GL0 applied A but embedded/Phase-2-anchored all of C, releasing a successor whose B lineage never entered global state. An outer `shardId` map-key mismatch similarly poisoned the wrong shard anchor. | CONFIRMED, FIXED IN WORKTREE | Require exact outer identity, no deferred MG, at least one continuation, and exact canonical accepted suffix for every continuing MG before proposal inclusion and again before the P2 hard anchor. |
| SHARD-05 | HIGH | IMPLEMENTATION | staircase duty | pre-fix `184f7863:ShardCheckpointGl0AcceptanceManager.scala:297-312`; current `ShardCheckpointGl0AcceptanceManager.scala:306-340`; `ShardCheckpointProducerDutyValidator.scala:12-95` | Any committee member could produce at `parent.slot+1`; honest members replay-signed because intake never checked producer duty, and lower-slot fork choice let the off-duty chain preempt the scheduled staircase producer and censor the shard. | CONFIRMED, PARTIAL FIX | Intake/sign/embed now check the retained producer, but the parent proof must become portable before this is consensus-safe; see SHARD-06. |
| SHARD-06 | CRITICAL | IMPLEMENTATION/DESIGN | embedded duty determinism | `ShardCheckpointWiring.scala:486-495`; `ShardCheckpointProducerDutyValidator.scala:31-67`; `ShardCheckpointGl0AcceptanceManagerSuite.scala` test `SHARD-C-009 remains RED` | Node A received parent P on shard gossip and resolves child C's duty; node B has the same GL0 proposal but not P in its local shard store. A accepts C while B rejects `parent unavailable`, so proposal validity depends on prior gossip and the global chain can split/halt. | CONFIRMED, OPEN | Child binds the exact Phase-2 GL0 ref carrying P; verifier extracts/authenticates P (or its header proof) from proposal-parent-bound evidence and derives hash/ordinal/slot without receiver-local history. |
| SHARD-07 | HIGH | IMPLEMENTATION | checkpoint slot validity | `ShardSlotLeader.scala:151-178`; `ShardCheckpointProducerDutyValidator.scala:75-89`; current `NakamotoSnapshotValidator.scala:44-70,157-180` | A committee member chooses a slot years ahead that maps to its staircase rank, gets replay signatures, and offers it for GL0 embedding. Without a containing-slot bound, successors cannot produce until wall time catches up. The worktree follower rejects `checkpoint.slot > containingGl0.slotCertificate.slot`, but the GL0 leader still constructs/signs locally before the same check. | CONFIRMED, FOLLOWER FIXED IN WORKTREE; PRODUCER PRE-SIGN OPEN | Run the same pure signed-slot-lineage gate before GL0 Ed25519/KES signing and storage; an ahead checkpoint remains pending. Never use receiver wall clock or local skew configuration as artifact validity. |
| SHARD-08 | HIGH | IMPLEMENTATION | Phase-2 recovery | `GlobalSnapshotConsensus.scala:2097-2115`; `ShardCheckpointChainStoreRecovery.scala:15-69`; `ShardCheckpointProducer.scala:354-385` | A follower that learned C only through GL0 recorded C as Phase 2 but `noteAnchor/finalize` no-op'd because C was absent from its shard store. With `bestTip=None` and adopted C ahead, its next duty failed closed forever. | CONFIRMED, FIXED IN WORKTREE | Reconstruct exact signed/store metadata from the already validated embedded C, ingest and prove its hash, then note adoption/anchor/finalize; duplicate ingest is idempotent. |
| SHARD-09 | CRITICAL | IMPLEMENTATION/DESIGN | committee parameters | `SharedServices.scala:286-291`; `EtaCalculation.scala:33-48`; `ShardCheckpointGl0AcceptanceManager.scala:420-433` | For the same checkpoint anchored at ordinal 99, node A configured with R=99 derives epoch 1 while node B with R=100 derives epoch 0. The same signatures/bytes can pass A and fail B before global artifact acceptance, splitting or halting GL0. The new epoch check makes this pre-existing parameter dependency explicit; it does not make local HOCON consensus-safe. | CONFIRMED, OPEN | Commit one canonical network/genesis/era parameter object/hash, make the checkpoint bind it, and halt before validation on local mismatch; local config cannot override artifact validity. |
| SHARD-10 | HIGH | IMPLEMENTATION | Phase-2 shard reorg | `GlobalSnapshotConsensus.scala:2064-2114`; `ShardCheckpointGl0AcceptanceManager.scala:274-290`; `ShardChainStore.scala:390-451`; `ShardCheckpointProducer.scala:355-386,428-454`; `NakamotoSyncDaemon.scala:2372-2376` | GL0 hash G-A reaches Phase 2 carrying shard checkpoint C-A at shard ordinal q. A density reorg makes G-B operational at the same GL0 height with sibling C-B at the same q. The callback anchors C-B in `ShardChainStore`, but `noteAdopted` updates only when `q > current`, so `lastAdoptedCheckpoint` and daemon recovery stay on C-A. The producer sees best tip C-B versus stale Phase-2 hash C-A, keeps `phase2AnchorNeedsRecovery=true`, and halts the shard; later receipts can re-anchor the orphaned C-A. | CONFIRMED, OPEN | The durable exact-hash `Phase2Reorg` transaction must atomically replace the shard checkpoint ref even at the same/lower ordinal, unwind/refold mirror and retention state, clear/requeue held inputs once, update daemon/producer views, and survive restart. Do not patch the isolated monotone `Ref`: only authenticated old/new/MRCA reorg evidence may move it backward or sideways. |
| SHARD-10 | CRITICAL | IMPLEMENTATION/DESIGN | producer-duty parameters | `ShardCheckpointWiring.scala:496-503`; `ShardSlotLeader.scala:150-177` | For the same signed child at slot 6 over parent slot 0 and ordered roster `[A,B]`, node X with local `staircaseDeltaSlots=5` schedules rank 1 (`B`), while node Y with delta 10 schedules rank 0 (`A`). The same execution-certified GL0 artifact can pass X and fail Y, splitting or halting global snapshot validity. | CONFIRMED, OPEN | Root one network/genesis/era proposal-parent parameter object/hash containing the staircase delta; any local mismatch halts before artifact validation. |
| ECO-10 | HIGH | IMPLEMENTATION | fee/address merge | `GlobalSnapshotStateChannelEventsProcessor.scala:215-232,299-355,440-508` | Two MGs concurrently claim same new fee address against one prior map; both pass, right-biased merge retains only one fee debit. | CONFIRMED | Globally sort MGs and thread address/balance accumulators. |
| ECO-11 | HIGH | IMPLEMENTATION | spend validation | `SpendActionValidator.scala:124-219,319-328,372-381`; `SpendTransactionBalanceManager.scala:43-99`; `GlobalSnapshotAcceptanceManager.scala:2505-2520` | Two no-ref spends of 60 from balance 100 validate independently; cumulative application underflows and throws, poisoning GL0 snapshot production. | CONFIRMED | Dry-run exact cumulative updater and reject offending artifact, not whole snapshot. |
| ECO-12 | HIGH | IMPLEMENTATION | balance reservation | `GlobalSnapshotAcceptanceManager.scala:1650-1719,2094-2105,2362-2371,2448-2458`; `CurrencySnapshotAcceptanceManager.scala:268-303,438-464,554-563` | GL1 transfer 60 and token lock 60 each validate against prior 100, then the sequential balance fold underflows and aborts the snapshot. The CL1 creator has the same validate-separately/apply-sequentially shape. | CONFIRMED | One deterministic reservation ledger across every economic class. |
| ECO-13 | HIGH | IMPLEMENTATION | rewards | pre-fix `GlobalDelegatedRewardsDistributor.scala:537-547`; current `GlobalDelegatedRewardsDistributor.scala:47-56,548-561`; `GlobalDelegatedRewardsDistributorSuite.scala:49-74` | Multiple matured withdrawals for one address were flattened to `.toMap`; one reward survived while all expired records were omitted from the next pending-withdrawal state. The current path checked-sums every reward per address before constructing transactions and fails the whole transition on overflow. | CONFIRMED, FIXED IN WORKTREE | Keep the checked aggregation load-bearing; the duplicate-address and overflow regressions must remain green. |
| ECO-14 | MEDIUM | IMPLEMENTATION | rewards | pre-fix `GlobalDelegatedRewardsDistributor.scala:289-305`; current `GlobalDelegatedRewardsDistributor.scala:58-69,303-308,339,363,428`; `GlobalDelegatedRewardsDistributorSuite.scala:74-104` | Replacement raised current stake 100->1000, but rewards used original event amount 100 forever. The shared weight helper now uses `record.amount`, performs intermediate arithmetic as `BigInt`, and rejects an aggregate outside `Amount` range before distribution. | CONFIRMED, FIXED IN WORKTREE | Keep the replacement and aggregate-overflow regression green across every reward-weight lane. |
| ECO-15 | HIGH | IMPLEMENTATION | CL1 determinism | `CurrencySnapshotAcceptanceManager.scala:310-335`; `CurrencyMessageValidator.scala:97-140` | Validator P's local GL0 head already contains competing fee address X and rejects; lagging Q accepts. `lastMessages` and state root diverge for the same pinned artifact. | CONFIRMED | Materialize registry from exact pinned `(ordinal,hash)` MPT. |
| ECO-16 | MEDIUM | DESIGN | oracle/inflation | `PricingUpdateValidator.scala:43-52,85-110`; `GlobalDelegatedRewardsDistributor.scala:219-275`; `application.conf:255-272`; `DelegatedRewardsConfigProvider.scala:137-142` | An allowed metagraph can submit zero/extreme price; GL0 verifies source/cooldown, not truth/range, and deterministically changes inflation up to the 6% cap. | CONFIRMED (dev/test/integration; mainnet activation disabled) | Globally specified bounded multi-source oracle or explicitly documented trusted authority. |
| ECO-17 | HIGH | IMPLEMENTATION | reward determinism | `GlobalDelegatedRewardsDistributor.scala:203-260` | Consensus reward uses `Double`, `Math.pow`, and `Math.exp`; an input near a rounding boundary may produce different integer Amount on heterogeneous JVM/CPU. | PLAUSIBLE | Deterministic fixed-point/decimal implementation and cross-platform vectors. |
| ECO-18 | CRITICAL | IMPLEMENTATION | fee replay | `modules/shared/src/main/scala/io/constellationnetwork/currency/dataApplication/package.scala:103-108`; `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/transaction/FeeTransactionValidator.scala:25-64`; `CurrencySnapshotValidator.scala:156-180`; `BalanceOpsManager.scala:57-105` | User signs fee F once for `dataUpdateRef=U`. A Byzantine ML0 re-includes the same signed F in later snapshots; GL0 validates only signature/source and applies the debit each time because no canonical consumed-set is checked. | CONFIRMED | Bind F to the carried data update and write a permanent canonical nullifier for `(source,dataUpdateRef)` or the signed fee hash before debiting. |
| SMT-01 | HIGH | IMPLEMENTATION | historical commitment | `GlobalSnapshotConsensusFunctions.scala:279-295`; `StateProofComparison.scala:42-48` | Follower explicitly removes `smtRoot` before equality. Producer can sign an arbitrary historical root; no cited finalize-time recomputation exists. | CONFIRMED | Make root reproducible/load-bearing or remove the field until it is. |
| NET-01 | HIGH | IMPLEMENTATION | GossipSub | `p2p/internal/gossip/gossip.go:160-214,793-819,1110-1158` (no `RegisterTopicValidator` call exists) | No topic validator is registered, so invalid-message penalty never fires. Unique junk earns first-delivery score, fills fixed relay queues, and causes honest snapshots/checkpoints to drop. | CONFIRMED | Structural+size validators in Go, application validation feedback/penalty, per-peer/topic quotas and fair queues. |
| NET-02 | HIGH | IMPLEMENTATION | GossipSub IDs | pre-audit `p2p/internal/gossip/gossip.go:205-214`; current `p2p/internal/gossip/gossip.go:205-213,370-375`; `p2p/proto/sidecar.proto:94-119`; `p2p/internal/gossip/message_id_test.go:5-21` | Message ID was `sha256(data)` without topic domain. Wire-identical L1 wrappers let an attacker prepublish valid payload bytes on the wrong topic, poisoning the seen cache so the correct topic is suppressed. | CONFIRMED, FIXED | Message ID now hashes `(topic,0x00,payload)` with cross-topic and same-topic regressions. Network/genesis domain remains NET-09. |
| NET-02A | HIGH | DESIGN | outbox retry | `p2p/internal/gossip/gossip.go:205-213,780-819`; `p2p/internal/outbox/outbox.go:65-90,153-190`; `StateChannelRoutes.scala:62-80` | Same-topic 30-second outbox republishes and five genesis retries retain the same content ID, so GossipSub suppresses them while its seen cache retains the first publication. A first publication lost after local acceptance is not repaired on the advertised cadence. | CONFIRMED | Make retry semantics bypass or outlive the seen cache without changing signed consensus bytes, and test loss recovery end to end. |
| NET-03 | HIGH | IMPLEMENTATION | sidecar gRPC/outbox | `p2p/internal/config/config.go:131-149`; `p2p/internal/grpcserver/server.go:141-166,306-367,440-500`; `p2p/internal/outbox/outbox.go:78-125` | The unauthenticated TCP gRPC endpoint (loopback by default) has no RPC rate/concurrency limit. Any process that can reach it can open competing shard Subscribe drains, create unbounded out-of-range shard topics, or fill uncapped outbox memory. | CONFIRMED | Unix socket/mTLS, one authenticated subscriber, shard bounds, message/concurrency limits, bounded durable outbox. |
| NET-04 | HIGH | IMPLEMENTATION | message sizing | `p2p/go.mod:8,11`; `p2p/internal/gossip/gossip.go:166-214`; `p2p/internal/grpcserver/server.go:148-161`; `SnapshotLeaderLoop.scala:1900-1952` | App config sets neither GossipSub nor gRPC message maxima, leaving the pinned libraries' 1 MiB pubsub and 4 MiB gRPC defaults. A valid aggregate snapshot can be stored and its events cleared before publication; oversize publication then fails and is swallowed, leaving a private branch/halt. | CONFIRMED | One protocol-wide maximum above worst valid envelope; reject proposal before local store if publish cannot accept it. |
| NET-05 | HIGH | IMPLEMENTATION | ChainSync | `p2p/internal/chainsync/protocol.go:32-38,92-132,222-241,602-612` | `RateLimitPerPeer=10` is unused and incoming reads have no deadline. A peer advertises a 16 MiB body then withholds it, pinning allocation/stream; cheap 0x03 requests proxy repeated JVM calls. | CONFIRMED | Token bucket before allocation, read/write deadlines, per-peer concurrency and range/span limits. |
| NET-06 | HIGH | IMPLEMENTATION | recovery/eclipse | `p2p/internal/chainsync/protocol.go:575-598`; `p2p/internal/gossip/gossip.go:1011-1040` | Recovery samples every connected peer uniformly with no validator/score filter; empty/malformed replies remain selectable. One peer subscribed to snapshot/attestation can mask seed reconnect while withholding other required topics. | CONFIRMED | Eligible/score-weighted peers, mark empty/malformed failed, quorum/intersection fetch, health check every required topic. |
| NET-07 | HIGH | IMPLEMENTATION | fraud-proof durability | `p2p/cmd/sidecar/main.go:298-305`; `p2p/internal/outbox/outbox.go:18-21,194-214`; `WatchtowerFraudProofEmitter.scala:153-167`; `GlobalSnapshotConsensusFunctions.scala:728-754` | The outbox is memory-only and TTL-pruned after one hour; the emitter stops after local RPC success. A wrong-root checkpoint filtered before GSAM produces no direct slash request, so crash/OOM or a long partition can lose the watchtower proof and let guilty signers escape slash. | CONFIRMED | Durable authenticated outbox; acknowledgement only from canonical inclusion/finality, never local enqueue. |
| NET-08 | MEDIUM | IMPLEMENTATION | peer exchange/scoring | `p2p/internal/gossip/gossip.go:1110-1132,1143-1169` | Positive topic score caps at 32 and app score is zero, but PX requires 100. No peer can qualify; dynamic shard topics have no topic score parameters. | CONFIRMED | Attainable calibrated threshold and score every critical dynamic topic. |
| NET-09 | MEDIUM | DESIGN | network isolation | `p2p/internal/gossip/gossip.go:416-475`; `p2p/internal/config/config.go:131-149`; `p2p/internal/chainsync/protocol.go:32-38` | Rendezvous, topics, and ChainSync protocol IDs contain no network/genesis fingerprint. Wrong-network/Sybil nodes can enter discovery and recovery selection. | CONFIRMED | Namespace and handshake every protocol with consensus-pinned network/genesis ID. |
| NET-10 | MEDIUM | IMPLEMENTATION | metrics HTTP | `p2p/cmd/sidecar/main.go:52,265-272`; `p2p/internal/metrics/metrics.go:207-224` | Default metrics server has no read/header/idle timeouts. Cluster-reachable slow clients consume descriptors/goroutines outside libp2p resource limits. | CONFIRMED | Bind management plane narrowly and configure HTTP timeouts/connection limits. |
| NET-11 | HIGH | IMPLEMENTATION | shard parent recovery | `NakamotoSyncDaemon.scala:2169-2207`; `ShardCheckpointFetcher.scala:54,61-69,112-117` | Each decodable checkpoint with a unique attacker-chosen missing parent starts a fetch fiber before membership/signature/replay validation. Cooldown is keyed only by that unique hash and its map never prunes, so unauthenticated gossip exhausts heap/fibers/sockets and can halt GL0. | CONFIRMED, PARTIAL FIX IN WORKTREE | Authenticate the producer signature before fetch. Then add bounded global/per-peer in-flight fetches, rate limits, expiring bounded dedup, and adversarial unique-hash flood tests; a compromised committee key must remain bounded. |
| NET-12 | HIGH | IMPLEMENTATION | shard ancestry recovery | pre-fix `NakamotoSyncDaemon.scala` missing-parent `Async.start(fetchByHash(...))` immediately followed by `evaluateForSigning`; current `NakamotoSyncDaemon.scala:62-66,201-285,2277-2342,2547-2569`; `ShardCheckpointProducerDutyValidator.scala:45-49`; `ShardCheckpointFetcher.scala:21-27`; `ShardCheckpointAdmissibilitySuite.scala:28-218` | Node receives valid child C without parent P. It detached the P fetch, immediately failed C's duty check as `parent unavailable`, and retained no C. P later stored, but C was not re-fed; GossipSub can suppress the same-content re-publication, so the shard stalls below execution quorum. If P lacked grandparent G, the same race repeated, so waiting one level was insufficient. | CONFIRMED, ORDERING FIXED IN WORKTREE; ADVERSARIAL EVICTION OPEN | The outer handler stays detached, but each pre-authorized ancestor is recovered synchronously inside it; walk depth, retained count, and bytes are bounded; unresolved children re-enter full validation after exact-parent storage. The one global FIFO still lets a compromised eligible key evict honest children; add per-shard/per-peer fairness, eviction metrics, and guaranteed ordinal recovery. Transport in-flight/rate/dedup bounds remain NET-11. |

## 4. Enforcement-gap map

Legend: `YES` means independent framework execution occurs before GL0 canonical
adoption. `WEAK` means code executes but does not prove authorization/conservation.
`N/A` means the layer does not own that transition.

| Operation | GL1 -> GL0 | CL1 -> ML0 -> GL0 | DL1/custom | Cross-shard | Before usable? |
|---|---|---|---|---|---|
| Native DAG transfer | GL0 block acceptance revalidates | N/A | N/A | N/A | YES, but cross-class reservation DoS remains (ECO-12). |
| CL1 transfer/balance/ref | N/A | `CurrencySnapshotValidator` rebuilds block events and framework artifact fields (`CurrencySnapshotValidator.scala:129-201`) | DL1 cannot override recreated framework fields | finalized GL0 mirror | YES structurally. |
| Fee transaction | N/A | Signature/source checks and balance fold rerun (`FeeTransactionValidator.scala:25-64`; `BalanceOpsManager.scala:57-105`) | DL1/ML0 supplies set | owner state in GL0 | NO replay safety: `dataUpdateRef` has no canonical consumed-set (ECO-18). |
| Reward/supply emission | GL0 reward code | Registered deterministic rewards or empty default | Custom reward claim cannot replace absent GL0 implementation | N/A | WEAK: withdrawal loss, wrong stake amount, floating math. |
| Allow-spend create/expire | GL0 handles native block | Blocks and expiry logic rerun | Artifact cannot replace create block | finalized owner state | YES structurally; cumulative reservation remains. |
| Spend with allow-spend ref | N/A | Exact signed allow-spend fields checked | DL1 only transports claim | finalized read + permanent nullifier | YES for implemented allow-spend path. |
| Spend without ref | N/A | Fields/balance checked | ML0 inclusion is effective authority | GL0 settles | NO independent authorization (ECO-04). |
| Token lock | GL0 native block path | CL1 block path rerun | DL1 cannot synthesize lock block | no generic handler yet | YES structurally; backing/replacement invariant broken. |
| Token unlock | N/A | Public artifact fields checked | Unsigned SharedArtifact accepted | no generic handler yet | NO authorization (ECO-03). |
| BalanceAdjustment | N/A | Removed | Removed | N/A | FIXED: no authority path remains. |
| `GlobalSnapshotsProcessed` | N/A | Supplied markers are filtered and the applied pinned ordinals regenerated (`CurrencySnapshotAcceptanceManager.scala:305-308,499-505`) | Cannot directly supply accepted marker | drives GL0 spend replay | WEAK: retained-history mismatch permits replay (ECO-05). |
| DL1 calculated/custom state | N/A | N/A | Proof/carried because GL0 lacks app code | no generic app handler | Intentionally not re-executed; must never authorize rows above. |
| Shard checkpoint claim | N/A | Full included binaries replayed | Custom state remains carried | GL0 total order | FIXED: root is comparison only; diff/receipt authority removed. |

Downstream CL1 does not and should not replay finalized GL0 state. It downloads the
signed MPT view, verifies the root, and forward-adopts the currency state
(`CurrencySnapshotProcessor.scala:569-600,629-665`). Validation belongs on the
submission path toward GL0; the finalized return path has one authority.

## 5. Determinism ledger

| Node-local input | Consensus effect | Evidence | Status |
|---|---|---|---|
| Observed-active validator set | Changes finality denominator/weight | `StakeRegistry.scala:384,448-469` | OPEN CRITICAL |
| Receiver wall clock and delivery order | Replaces latest attestation | `NakamotoSyncDaemon.scala:1741-1755`; `TipTracker.scala:193-201` | OPEN HIGH |
| Local KES/publish success after self-record | Changes only one node's quorum view | `NakamotoSyncDaemon.scala:2881-2929` | OPEN CRITICAL |
| Process restart | Rolls finality refs back to zero | `dag-l0/Main.scala:125-152` | OPEN CRITICAL |
| Unordered `Map.values.find` | Selects protected same-height hash | `NakamotoChainStore.scala:420-439` | OPEN CRITICAL |
| Best-tip branch + ordinal-only cache | Changes stake/eligibility | `NodeStakeAggregator.scala:135-155` | OPEN CRITICAL |
| Transport/signed `parentSlot` instead of exact retained parent slot | Inflates the LDD gap and makes an otherwise losing producer eligible | pre-fix `NakamotoSyncDaemon.scala:1371-1373`; current `NakamotoSnapshotValidator.scala:44-70` | FOLLOWER FIXED; producer pre-sign parity open. |
| Missing/partial or wrong-branch prior-period eta history | Changes the VRF message, committee population, proof verdict, and rooted eta boundary | Typed refusal and exact-parent `getEtaAt` now protect GL0 GSAM and admission (`EtaStateManager.scala:60-64,121-141,174-198`; `GlobalSnapshotAcceptanceManager.scala:1331-1353`; `GlobalSnapshotConsensus.scala:1435-1477`). Shard committee/producer/attester paths still use ambient eta because the checkpoint lacks an exact GL0 hash/root (`SharedServices.scala:305-308`; `GlobalSnapshotConsensus.scala:1736-1797,1913-1931`; `ShardCheckpoint.scala:68-77`). | GL0/ADMISSION FIXED; SHARD EXACT-BRANCH GAP OPEN CRITICAL. Bind exact Phase-2 `(ordinal,hash,mptRoot)` in the checkpoint and use `getEtaAt` at every shard consumer; distinguish a ratified complete-empty result from unavailable history. |
| Producer N-2 stake versus verifier live stake | Same VRF output passes one threshold and fails the other after stake changes | pre-fix `SnapshotLeaderLoop.scala:809-812`; pre-fix `NakamotoSnapshotValidator.scala:114-128` | FIXED IN WORKTREE with shared parent-ordinal lookback; missing historical-state fallback remains open. |
| Presence/absence of cached N-2 stake distribution | `relativeStakeAt` silently selects historical, live MPT/GSI, or equal weight for the same parent/artifact | `StakeRegistry.scala:332-360,480-504` | OPEN CRITICAL; mature-period absence must be typed unavailable and recover exact history. |
| Sender-chosen GL0 leader VRF VK | Lets one signed validator grind unlimited identities and choose an eligible proof | pre-fix `NakamotoSnapshotValidator.scala:118-131`; current `ActiveOperatorConsensusKeys.scala:20-43`, `NakamotoSnapshotValidator.scala:44-63` | PERIOD-ZERO FIXED; runtime exact-parent N-2 pair/roster/stake resolution remains open. |
| `NAKAMOTO_OPTIMISTIC_MIN_FRACTION` | Per-process finality threshold | `StakeRegistry.scala:102-107` | OPEN HIGH; must be typed cluster config or removed. |
| Node-local GL0 head during CL1 message validation | Changes accepted messages/state proof | `CurrencySnapshotAcceptanceManager.scala:310-335` | OPEN HIGH |
| `Math.pow/exp` platform result | May change reward integer/root | `GlobalDelegatedRewardsDistributor.scala:203-260` | PLAUSIBLE HIGH |
| Random ChainSync peer pick/failure cooldown | Changes recovery availability/order, not valid transition | `p2p/internal/chainsync/protocol.go:575-598` | Liveness risk; validation must remain order-independent. |
| Sidecar fixed-queue arrival order and same-topic retry seen-cache | Drops/suppresses honest messages | `p2p/internal/gossip/gossip.go:205-213,793-819`; `p2p/internal/outbox/outbox.go:153-190` | OPEN HIGH liveness; cross-topic poisoning is fixed. |
| Sharded finalized-base currency override | Mixed base/branch economic state | removed in this audit | FIXED |
| Reorgable best-tip execution epoch | Could split shard membership | `SnapshotLeaderLoop.scala:187-194,943-960` now uses finalized anchor | FIXED |
| Cross-shard branch-aware read | Could expose unfinalized owner state | `GlobalSnapshotAcceptanceManager.scala:518-522,2203-2217` uses base MPT | FIXED |
| Receiver-local shard parent store | Same embedded child passes duty on a node that saw parent gossip and fails on one that did not | `ShardCheckpointWiring.scala:486-495`; `ShardCheckpointProducerDutyValidator.scala:31-67` | OPEN CRITICAL; replace with portable proposal-parent-bound parent evidence. |
| Receiver-local time versus unbounded signed checkpoint slot | A far-future signed slot can become a valid staircase window and strand successors | `ShardSlotLeader.scala:151-178` | OPEN HIGH; upper-bound by the signed containing-GL0 slot, not receiver time. |
| Local eta-period length `R` | Same signed anchor ordinal derives different execution epochs, rosters, and validity | `SharedServices.scala:286-291`; `EtaCalculation.scala:33-48`; `ShardCheckpointGl0AcceptanceManager.scala:420-433` | OPEN CRITICAL; bind R through the canonical network/genesis/era parameter hash and halt on local mismatch. |
| Local `staircaseDeltaSlots` | Same signed checkpoint/parent/roster schedules a different producer | `ShardCheckpointWiring.scala:496-503`; `ShardSlotLeader.scala:150-177` | OPEN CRITICAL; derive delta from proposal-parent-bound canonical parameters and halt on local mismatch. |

## 6. Plan integrity

`NAKAMOTO-TODO.md:7-23,57-71,107-124` now labels the relevant work unsafe,
unverified, pending, or superseded. `NAKAMOTO-PLAN.md:3-12` is explicitly historical;
the rows below reject stale claims still present in its superseded body and record the
requested implementation checks, rather than treating that body as current design.

| Claim | Audit result |
|---|---|
| Avalanche `K/alpha/beta` optimistic finality is implemented | FALSE. Accumulator explicitly says the cascade is future work; K/alpha are unused. |
| `T_count/T_weight/T_depth1` max-of drives state | FALSE. T_count is diagnostic and Snowball T_weight telemetry is not the legacy state-changing sink. |
| Two-tier first-wins finality is safe | FALSE. Either unsafe rail can expose state; later depth cannot repair a conflicting release. |
| Hard-fork migration is needed | SCOPE MISMATCH. This is a greenfield fork; no deployed fork-only state exists. Keep only if migration from an actual upstream v4 network is a product requirement. |
| Mempool reinsertion status | NOT IMPLEMENTED. `reconcileMempool` evicts stale DAG events (`NakamotoSyncDaemon.scala:413-441`); it does not reinsert all valid orphaned events. |
| Stake-weighted VRF is production-safe | FALSE. Global leadership reads MPT stake, but ECO-02 permits unbacked stake (and pre-audit ECO-01 allowed same-round multiplication). Execution-shard membership is intentionally uniform public hash draw, not stake VRF. |
| Two-level GL0 + metagraph finality is built | MISLEADING. Shard checkpoint inclusion inherits unsafe GL0 finality; there is no independent proven metagraph finality certificate. |
| KES is load-bearing | PARTIAL. Received global attestations/snapshots now reject missing, empty, or invalid KES (`KesGossipVerification.scala:35-85,175-220`), but the local self-vote is recorded before KES signing/publish and survives either failure (FIN-08). |
| Sharding is complete | FALSE. Universal replay and finalized reads are present, but checkpoint acknowledgement is still pre-finality, sibling rollback/full replay is absent, and the processed-set replay defect remains. |
| `numShards=1` secures economics through committees | FALSE premise. Committees/watchtowers are inactive; universal GL0 execution is the only intended guard. The transition defects in this report remain active. |
| Historical `min(epsilon_snowball, epsilon_chain)` composition bounds first-wins | FALSE. For an OR finalizer, the weakest sufficient rail can fire first; risks do not compose by taking the minimum. |

## 7. What is actually correct

These are narrow invariants with an enforcement site, not a production-safety claim:

1. **Layer identity is unambiguous.** The formal enum contains exactly four runtime
   layers (`Layer.scala:3-7`); DL1 is a CurrencyL1 app injection
   (`CurrencyL1App.scala:53-57`).
2. **The ordinary global MPT root is replay-bound.** Followers reconstruct the global
   artifact and compare it with the proposal while retaining `mptRoot`
   (`GlobalSnapshotConsensusFunctions.scala:207-299`). Only `smtRoot` is normalized out.
3. **CL1 framework equality is exact after recreation.** The validator rebuilds
   framework events, invokes the shared creator, and rejects when the recreated artifact
   differs; only unavailable custom `dataApplication` state is copied from the claim
   (`CurrencySnapshotValidator.scala:129-201`).
4. **Absent framework reward code cannot echo an ML0 reward claim.** The fallback
   computes an empty reward set (`CurrencySnapshotValidator.scala:141-154`).
5. **Shard roots no longer replace framework state.** Checkpoints carry full binaries
   (`ShardCheckpoint.scala:14-21,46-55`); `verifyEmbedded` re-executes them and compares
   each recreated root (`ShardCheckpointGl0AcceptanceManager.scala:465-549`). The
   fork-only diff/receipt schemas are removed.
6. **Cross-shard allow-spend reads are finality-first.** The GL0-local proof client is
   built over the finalized base reader (`GlobalSnapshotAcceptanceManager.scala:518-522,2203-2217`).
7. **Cross-shard replay markers are canonical state.** Accepted consumes write permanent
   nullifiers through the global MPT (`ConsumedAllowSpendStateManager.scala:245-274`;
   `CrossShardMessageHandler.scala:139-156`; `GlobalSnapshotAcceptanceManager.scala:2868-2873`).
8. **Admission VRF identity is registry-bound.** A missing or wire-mismatched registered
   key is rejected by `MetagraphCommitteeGate`; sender-provided VK is not authority
   (`MetagraphCommitteeGate.scala:540-620`).
9. **Snapshot transport metadata is not trusted.** The daemon binds hash, ordinal,
   parent, producer, slot certificate, eta, VRF proof/key/output to the signed body
   before routing or storage (`NakamotoSyncDaemon.scala:90-160`).
10. **Fee arithmetic no longer wraps.** Fee deltas accumulate in `BigInt` and must fit
    `0..Long.MaxValue` before constructing balances (`BalanceOpsManager.scala:70-105`).
11. **One same-round token lock cannot multiply collateral through duplicate creates.**
    Acceptance now folds deterministic evolving source/ref/parent/node seen sets before
    adding records, and prior-record uniqueness is read from the MPT
    (`UpdateNodeCollateralAcceptanceManager.scala:48-99,137-159`;
    `UpdateNodeCollateralValidator.scala:226-240`). This does not prove live lock backing
    after replacement (ECO-02).
12. **KES and VRF are one current-view identity record.** The registry exposes KES/VRF
    only as projections of one `OperatorConsensusKeys` map and rejects malformed
    identities or cross-operator duplicate KES/VRF ownership
    (`OperatorConsensusKeyRegistry.scala:24-70,77-108`). This does not supply the
    delayed permissionless roster.
13. **Period-zero key material is rooted and restart-checked.** Genesis loading verifies
    the long-term signature over the complete KES+VRF record, materializes it from the
    authenticated MPT partition, and requires local/rooted equality
    (`L0GenesisLoader.scala:377-439,442-515`). This proves identity, not eligibility.
14. **An incomplete eta prefix is no longer accepted as complete, and GL0 GSAM
    eta is exact-parent bound.** The chain store returns a typed exact-walk result;
    mature `Incomplete`/empty ranges defer. `getEtaAt` bypasses receiver-current
    MPT state and ambient caching, and GSAM passes the exact parent
    (`NakamotoChainStore.scala:52-65,612-703`;
    `EtaStateManager.scala:60-64,121-141,174-198`;
    `GlobalSnapshotAcceptanceManager.scala:1331-1353`). The shard checkpoint's
    missing exact anchor and ambient shard eta paths remain open under CONS-02.
15. **The current tower verifier does not trust carried VRF output or pool size.**
    Every suffix and upper-level occurrence resolves the atomic period-zero pair,
    verifies the proof over the header's exact carried `eta || slot` bytes, and constant-time compares the
    derived output; a nonempty proof with an empty suffix rejects
    (`TowerVerifier.scala:125-147,199-258`). Sender `activePoolSize` is not read
    for eligibility, and otherwise-valid nonempty proofs fail closed with
    historical eligibility unavailable (`TowerVerifier.scala:344-350`). The
    carried eta is not proven canonical. This is not portable tower verification;
    historical membership/weight/eta witnesses
    and authenticated SMT inclusion remain open.

## 8. Removal audit

The working tree deletes 28 paths relative to pre-audit commit `725b25bd`. Every path
was checked against upstream tag `v4.0.0`; only five existed upstream. No production
runtime path outside the explicit authority/replacement groups below was removed.

| Removed group | Paths | Provenance and disposition |
|---|---:|---|
| Superseded authority/diff design documents | 11 | Fork-only. Deleted because they prescribe committee-root, state-diff, receipt, or roots-only authority contrary to ADR-0017. Historical audit/review records remain only behind non-normative banners. |
| `BalanceAdjustment` loader, mutator, resource, and tests | 5 | The only upstream-v4 removals. Intentional: ECO-08 proves the subsystem let a configured ML0 append unauthorized extra adjustments. Variant, resource, loader, service wiring, and tests were removed together. |
| `ShardReanchor` plus suite | 2 | Fork-only unsafe checkpoint relabeling. Replaced by `ShardWindowContinuation.scala` and its suite: only an exact parent continuation is accepted; otherwise the window defers and GSAM replays the selected suffix. |
| Shard non-participation schema/manager/suite | 3 | Fork-only abandoned schema. It never existed in v4 and is not retained as a greenfield compatibility field. |
| `CrossShardReceipt` plus receipt-consumer suite | 2 | Fork-only committee-receipt authority. Removed; implemented allow-spend interaction now uses finalized GL0 reads plus canonical nullifiers. |
| Currency-diff round-trip suite | 1 | Fork-only test of the removed authoritative diff wire shape; no live behavior remains to preserve. |
| Diff/base-pin and seed-MPT tests | 2 | `DiffBasePinReExecutionSuite` is replaced by the real pinned-base parity coverage in `ExecutionBasePinReExecutionSuite.scala`. `SeedMptByteFaithfulSuite` exercised a direct peer-byte state installer that was deliberately removed; the replacement is parent-first full replay with Valid-only commit (`CatchUpVerificationSuite.scala:236-355`; `NakamotoSyncDaemon.scala:257-286`). Integrated bad-root/no-MPT-write coverage remains a test gap. |
| Serialized fork-only GL0 fixtures | 2 | Obsolete 25-field fork snapshots lacking the now-required `fraudProofs` field. Their shard maps were empty, so they did not carry removed nested shard fields. Current schema round trips are covered by `GlobalSnapshotCodecsSuite` and `ShardingScodecCodecsSuite`. |

## 9. Required production gate

Do not enable economic value until every open CRITICAL/HIGH finding, including
FIN-01..FIN-10, FIN-12, ECO-02..ECO-06, ECO-10..ECO-15, ECO-17/18, SHARD-01..SHARD-10, SMT-01,
and every open HIGH network finding, is closed by adversarial tests.
The finality repair must center durable atomic exact-hash P0/P1/P2 state,
portable decided-attestation evidence or canonical `k1` depth qualification, and
objective density replacement with rollback. It must not add global BFT locks,
votes, quorum certificates, or an immutable descendant floor. The economic repair
must make every accepted balance change a checked,
authorized, replay-protected transition over one deterministic reservation accumulator.
The finding-owned implementation order and acceptance gates are maintained in
`CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md`.

## 10. Verification limits

- Post-change regression runs passed 2,565 Scala tests with zero failures:
  `shared/test` 809, `nodeShared/test` 1,478, `dagL0/test` 185,
  `dagL1/test` 56, `currencyL0/test` 13, and `currencyL1/test` 24.
  `p2p/go test ./...` and `p2p/go build ./...` also passed. This proves the
  exercised contracts did not regress; it does not close any open finding above.
- ECO-17 remains `PLAUSIBLE`: no heterogeneous JVM/CPU rounding-boundary vector was
  reproduced.
- Catch-up replay coverage is helper-level; an integrated `handleSnapshot`
  bad-root/no-MPT-write regression is still missing.
- Pre-audit citations refer to commit
  `725b25bdaf3e90d9d1f59f42875b3d948cfe239a`; removed-file line numbers are not current-tree paths.
- NET-04's 1 MiB/4 MiB limits were checked against the versions pinned at
  `p2p/go.mod:8,11`; the repository does not set those maxima itself.
