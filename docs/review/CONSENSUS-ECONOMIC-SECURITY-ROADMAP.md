# Consensus and Economic Security Roadmap

**Status:** Active implementation plan; owner directions are ratified and remaining engineering/research/parameter/proof gates block their named dependent tasks
**Source baseline:** `c610a0740c34833e563f8a94c2ab820186a75897`
**Audit baseline:** `CORRECTNESS-SECURITY-AUDIT-2026-07-11.md`
**Normative architecture:** `CONSENSUS-ARTIFACT-LIFECYCLE.md`
**Delegation/test contract:** `CONSENSUS-PROTOCOL-TEST-PLAN.md`
**Owner decision register:** `CONSENSUS-OWNER-DECISIONS.md`

`NAKAMOTO-PLAN.md` is the active sequence/status authority. This document is the
detailed security work breakdown and retains its pre-existing `E*.*` task IDs for
finding/test traceability; a bare epic number is therefore not a cross-document
identifier. Refer to an epic by document plus full name until E0 emits the
machine-readable ownership ledger.

This roadmap replaces the incorrect global-BFT plus ordinary-noncommittee
universal-CL1-replay plan. It does not discard the audit findings or universal
native GL1 execution, and it retains the target universal global-kernel requirement.
It assigns them to the owner-stated architecture:
chain-based GL0 finality, replay-before-sign execution committees, verified diff
adoption, watchtower collusion detection, Phase-2 cross-metagraph reads, and a
density-based recovery rule beyond `k1`. `k2` is a retention/recovery horizon,
not a separate finality phase or an absolute fork-choice floor.

No task is complete because it appears here. Completion requires the named RED
test, independent oracle, source re-audit, and closing commit.

## 1. Target architecture

```text
GL1 -------------------------------> GL0

CL1 --+
      +-> ML0 -> binary intake -> execution shard -> GL0 snapshot
DL1 --+

canonical Phase-2 GL0 state ------> GL1 / ML0 / CL1 / DL1
```

- GL0 global consensus is Nakamoto/Taktikos/LDD. Avalanche/Snowball supplies an
  optimistic Phase-2 trigger and depth `k1` supplies the fallback. There are no
  global BFT votes, locks, QCs, or view changes.
- ML0 may remain BFT. That local finality authenticates a metagraph binary but is
  subordinate to GL0.
- A shard producer and every execution-committee signer replay full framework
  currency at one exact Phase-2 GL0 base. A signature means exact diff/root
  reproduction.
- An ordinary noncommittee GL0 node verifies the execution threshold, applies the
  canonical namespace-bounded diff to its signed base, and recomputes the root. It
  does not rerun currency recreation.
- Noncommittee watchtowers replay as the collusion backstop.
- Every GL0 node still executes native GL1 transitions and the small deterministic
  global cross-metagraph ordering/nullifier/settlement kernel.
- Phase 2 is operational and downstream-consumable but density-reorgable. Valid
  forks within `k1` use `maxvalid-tk`; valid forks beyond `k1` use the ratified
  Ouroboros Genesis-family `maxvalid-bg` rule from the true common ancestor.
  `k2` recommends retained rollback/proof state; it never makes a branch win.

## 2. Current source reality

The plan begins from these source-proven gaps:

| Area | Current source fact | Consequence |
|---|---|---|
| Global engine | `SnapshotLeaderLoop` states that it replaces the old BFT rounds and names the exact-hash optimistic/depth gadget (`SnapshotLeaderLoop.scala:77-90`). | Do not design a new global BFT state machine. |
| Dormant GL0 BFT plumbing | Immediately before containment commit `f88d785e8`, `GlobalSnapshotConsensus` allocated an inherited `ConsensusEventLoop` and returned its handler without running `loop.run` (`f88d785e8^:GlobalSnapshotConsensus.scala:864-906,2384-2393`). At this packet's `40ea4e9da` baseline, that loop and live handler were already gone, but GL0 still allocated generic `ConsensusStorage` and `ConsensusRoutes` and returned disabled manager/handler compatibility objects through a generic `Consensus` result (`40ea4e9da:GlobalSnapshotConsensus.scala:746-757,825-833,2276-2284`). | The pre-containment loop was a dormant architectural hazard, not a confirmed remote heap DoS; this packet removes the remaining compatibility shell. The implementation now constructs no GL0 generic event loop, storage, manager, handler, routes, result, BFT state/schema, or BFT config subtree. `GlobalSnapshotConsensus.make` is a Nakamoto resource factory returning `Unit`; GL0 services expose no generic consensus API. `GlobalLegacyBftIngressDisabledSuite` checks the compiled Services API, parses the live GL0 config shape, checks construction-source boundaries, proves the retired GL0 state sources are absent, and proves CurrencyL0 retains its BFT engine/API. |
| GL0 sidecar-rumor consumption | At the baseline, `SidecarRumorBridge.receive` actively offered inbound rumors to the bounded shared queue (`SidecarRumorBridge.scala:60-105`), but Main never started `GossipDaemon.consumeRumors`. Commit `f88d785e8` removed GL0 BFT dispatch before enabling the consume-only Nakamoto branch. | The current worktree starts the generic consumer only after local state and chain seed, before ingress release. Runtime delivery, reconnect, cancellation, and durable missed-message recovery remain unproved. |
| Early generic-rumor window | `Services` allocates the bridge before Main bootstrap, but the receive effect waits on the await-only `ConsensusInputGate`; Main owns release and starts the consumer first. The sidecar now emits a mandatory typed `SubscribeStarted` only after the exact rumor subscription is locally acquired. | **Local ordering contained, integration open:** cold input cannot fill the rumor queue and Main cannot publish `Ready` before both current same-session lanes acknowledge. Delayed subscription can still miss ephemeral traffic; add end-to-end hostile-input, bounded-drain, reconnect, and durable-retry tests. |
| Early dedicated Nakamoto ingress | `NakamotoSyncDaemon`, proactive KES evolution, and rebootstrap await the same ingress gate. Main restores/root-checks state, awaits exact chain-seed success, starts sinks/listeners, activates both subscriptions, and performs its one-time `Ready` write under the serialized two-lane readiness lease. `ProductionGate` is initially paused and changes atomically with committed lane readiness. Exact gRPC acquisition errors propagate; quiet streams rely on HTTP/2 keepalive rather than payload-idle expiry. | **Subscription lifecycle contained; recovery open HIGH:** local acquisition is not peer reachability, catch-up, finality, or authenticated restart authority. The boot-mode/catch-up rule, authenticated disk-head recovery receipt, detached/bounded topic ownership, and hostile fresh/cold/rollback tests remain open. |
| Shard topic partitioning | With `numShards > 1`, the Go sidecar eagerly joins every GL0 node to both raw topic families for every configured shard and exposes one shared all-shards drain. There is no distinct GL0-wide certified-checkpoint/diff lane. | **Target load partition is absent:** raw checkpoint/attestation traffic is amplified through all GL0 nodes. E8 must bind raw subscriptions to current execution/watchtower assignments and carry execution-certified checkpoints on a separate universal adoption lane. The current `SubscribeStarted` truthfully reports the transitional profile; it does not legitimize it as target architecture. |
| FinalityGate | Only `finalizedOrdinal` and `isServable(ordinal)` exist, and `fromRef` compares ordinals (`FinalityGate.scala:23-30,44-50`). | It cannot own per-hash phases or represent same-ordinal hash replacement. |
| Premature metagraph Phase-2 capability | The current gate reads the monotone finalized ordinal and current best tip separately, then treats any requested hash found on that tip at an ordinal below the watermark as exact Phase 2 (`GlobalSnapshotConsensus.scala:1285-1300`; monotone projection update `SnapshotLeaderLoop.scala:1152-1155`). The minted capability retains only ordinal/hash and delegates authority to that Boolean (`MetagraphParentOrdinalResolver.scala:35-58`). | If density replaces Phase-2 branch A with branch B at or below A's watermark, B immediately inherits Phase 2 without its own decided `T_weight` evidence or `k1` depth. A B-anchored currency binary can enter committee admission during that window. Replace this path with a lease over exact branch revision plus authenticated/executed snapshot, historical qualification evidence, and exact MPT/semantic/anchor readback; never wrap the ordinal Boolean. |
| Avalanche | `SnowballAccumulator` is a sticky latest-attestation margin: a flip moves the old color count, K/alpha are unused, and first crossing is arrival-order sensitive (`SnowballAccumulator.scala:13-25,118-151`; reproduced at `SnowballAccumulatorSuite.scala:212-235`). | The optimistic protocol is not implemented and the current accumulator is not portable decision evidence. |
| Optimistic trigger containment | `T_weight` still reads the unsound transitional margin, but it is telemetry only (`FinalityTrigger.scala:176-208`). The leader loop explicitly keeps attestations and count/weight triggers non-authoritative; only an exact selected-tip `k1`-depth CAS reaches the Phase-2 sink (`SnapshotLeaderLoop.scala:1101-1236,1245-1284`). | The former cumulative-2/3 sink is removed. This is a safe dark cut, not implementation of the intended sampled optimistic gadget. |
| Invented `k2` floor | `SettledOrdinalTracker` and the density-band flag currently turn an ordinal projection into a branch-rewrite floor. | This does not implement the ratified retention-only `k2`; fork choice, retained recovery state, and service policy must be separated. |
| Density recovery | Density selection/revert code exists but is disabled by default, currently refuses sufficiently deep forks, and current sinks are not all reversible. | Genesis-family comparison and Phase-2 recovery are incomplete. |
| Multi-tine frontier | On complete, true-MRCA-resolved histories the mixed pairwise comparator is commutative but not transitive. Structurally connected bare `ChainTip` inputs form a strict-preference cycle `A >tk B`, `B >bg C`, `C >bg A`, so three permutations produce different `selectBest` winners without using the open tie rule (`ChainSelectionSuite.scala:191-226`). The analogous `NakamotoChainStore.store` tournament is now directly reproduced under a synthetic enabled `k`/`s` configuration: three parent-before-child schedules over one signed ordinal/parent-linked frontier leave best tips C, B, and A (`NakamotoChainStoreSuite.scala:280-372,427-469`; `ChainSelection.scala:119-159`; `NakamotoChainStore.scala:437-464`). The store-level RED supplies synthetic slot/VRF/context, bypasses full snapshot/VRF/KES/historical-era admission, and is not a shipped-environment configuration witness. | The papers specify incumbent folds, not a total frontier order. L-24 ratifies an objective result once nodes share the same cutoff-complete published valid frontier and branch-authenticated parameters. O-15 still requires cutoff/reveal semantics, cycle resolution, exact metric/tie, evidence/verifier, a complete validator-backed active-configuration admission witness, a corrected-store convergence witness, and a security/liveness proof. No live fork-choice path may authorize `FinalityGate` before those gates pass. |
| Fork-depth boundary | When the bounded walk resolves a true MRCA over consecutive tines it reports maximum post-MRCA suffix length. Production derives and passes `kLookback = k1 + 1`, while comparison selects density only when `forkDepth > kLookback` (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/types.scala:173-177`; `GlobalSnapshotConsensus.scala:979-994`; `ChainSelection.scala:161-175,187-240`; `ChainSelectionSuite.scala:228-250`). The depth-`k1+1` witness therefore picks sparse/longer by Tk under production wiring but dense/shorter when the locked prose boundary is applied. | O-15 must freeze the distance metric and exact equality boundary before changing live consensus; retain the witness as an activation blocker. |
| Checkpoint diff | `ShardDerivedStateDelta` currently carries roots and binaries, not a state diff (`ShardDerivedStateDelta.scala:17-31`). | Noncommittee verified diff adoption is unavailable. |
| Embedded execution threshold | Intake and embedded verification now require valid distinct execution signers at `kQuorum`, and shard depth no longer substitutes; ordinary noncommittee GL0 nodes still replay sharded CL1 transitions because the signed canonical diff/type boundary is absent. | Preserve the threshold while replacing caller-ordered signing with a typed replay capability and ordinary noncommittee replay with verified diff apply only after the complete gates pass. |
| Checkpoint execution-base identity | **Exact-state slice landed, authority remains open:** `ShardCheckpoint.executionBase` is now `GlobalSnapshotStateRef(ordinal,hash,parentHash,mptRoot)`, and all four fields are in the committee signing preimage (`GlobalSnapshotStateRef.scala:11-22`; `ShardCheckpoint.scala:61-76,94-104`). The pinned reader validates that exact snapshot identity, performs a fail-closed canonical hash recheck, and recomputes the root from retained bytes before exposing state; a miss or changed resolver has no live/head fallback (`PinnedCurrencyInfoReader.scala:268-298,321-413`). The producer captures a canonical Scodec identity of the complete retained base, re-reads and byte-compares it after replay, and only then signs; receiver/watchtower replay resolves the same signed reference (`ShardCheckpointProducer.scala:208-257,420-454,681-718`; `ShardCheckpointWiring.scala:156-190,209-230,346-388`). Focused regressions cover fail-closed same-ordinal wrong hash, parent, and root plus replacement/unavailability between resolver observations (`PinnedCurrencyInfoReaderSuite.scala:304-321,378-408`). | This closes ordinal-only sibling substitution for the retained execution prior; it does **not** authenticate Phase 2 or provide post-replay currentness. `GlobalSnapshotStateRef` is explicitly a claim, while live Phase-2 authority still comes from the ordinal watermark/current-best-branch composition above. The recheck must remain until an immutable exact lease plus mandatory `commitIfCurrent` immediately before signing/effect replaces it. Shard eta, historical registry/roster, committee selection, and parameters are not resolved from this exact base; freshness, purpose domain, lease/CAS, and reorg invalidation remain open. Network/genesis/era/parameter binding, pre-roots, diffs, extracted intents, replay witnesses, and custom-lane commitments also remain absent. Therefore FIN-14, O-16, and protocol-test-plan P4.4 remain open. |
| Candidate replay authority | **Dark exact-history and identity-composition slices present; replay authority remains open:** the earlier containment removed `LastN`/manager-cache authority from pinned and historical `SpendAction` reads (`CurrencySnapshotAcceptanceManager.scala:362-400,468-475`; `GlobalSnapshotOpsManager.scala:23-37,63-123`). A candidate-scoped structural session anchors the complete `GlobalSnapshotStateRef`, validates all four fields, performs a bounded exact-parent walk, batches/deduplicates targets, and caches each exact position's success, typed failure, raised error, or cancellation (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/overlay/ExactReplayHistory.scala:18-36,189-230,253-392,395-505`; `ExactReplayHistorySessionSuite.scala:144-337,339-510`). Its chain-store source requests exact `(hash,ordinal)`, runs a one-link exact walk, re-reads and content-rehashes the signed snapshot, rejects same-ordinal substitution, and deliberately ignores `StoredSnapshot.context` (`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoChainStoreExactReplayHistorySource.scala:16-20,29-61,65-146`; `NakamotoChainStoreExactReplayHistorySourceSuite.scala:129-239`). The live exact-base reader retains its fail-closed canonical recheck pending a post-replay lease CAS (`PinnedCurrencyInfoReader.scala:268-298,321-413`; `PinnedCurrencyInfoReaderSuite.scala:304-321`). A package-private model composes a dedicated replay-purpose `CanonicalPhase2Lease`, structural history, and matching image/semantic/F32/value identities; it exposes a narrow exact-target currency replay projection rather than a generic reader, derives `SpendAction`s from exact signed history, and preserves absent versus present-empty F32 (`CandidateCurrencyReplayView.scala:28-92,151-253`; `CandidateCurrencyReplayViewSuite.scala:203-294`). | The composition inputs are package-constructible model descriptors, not receipts derived by a verified byte parser. Equality does not prove whole-image semantics, field-32 preimages/population, or rooted-value derivation or namespace scope. There is no codec, live issuer, or live caller. The Phase-2 evidence/issuer and `commitIfCurrent`, signed complete `GlobalSyncView`, ROOT-008 strict parser and target codecs, ROOT-010 replay witness/removal, candidate pinning of message validation and every economic read, live migration, hash-era crossing, and density-reorg invalidation remain open. Currency message validation still reads ambient combined global state and field emission still has a `LastN` fallback (`CurrencySnapshotAcceptanceManager.scala:300-324,589-599`). Missing history must defer/enter authenticated recovery; these slices close neither FIN-14 nor E10. |
| Ordinary noncommittee sharded-CL1 replay regression | `verifyEmbedded` calls the replay path (`ShardCheckpointGl0AcceptanceManager.scala:297-312`) and GSAM recreates adopted currency state. | Ordinary GL0 nodes do the work the committee was meant to amortize. Native GL1 execution remains universal. The global kernel is target E9 work; current `numShards <= 1` bypasses it (`GlobalSnapshotAcceptanceManager.scala:2372-2384`). |
| Shard execution-sign boundary | `ShardCheckpointAttestationEmitter.emit` accepts only a sealed replay-minted `VerifiedShardCheckpoint`; intake rejection/mismatch cannot mint it (`ShardCheckpointAttestationEmitter.scala:50-53,102-108`; `ShardCheckpointGl0AcceptanceManager.scala:56-108`). | The current capability proves root-only recreation. Extend its result to the canonical diff/intents/complete root; do not weaken it when ordinary adoption stops replaying. |
| Global blind-sign type boundary | **Contained, not complete:** naked `emitAttestation`/`emitTipAttestation` APIs, receiver-invented producer evidence, periodic best-tip signing, and the cumulative-weight finality sink are removed. Canonical depth-`k1` is the sole live state-changing rail; verified remote attestations are telemetry only. Typed store outcomes, internal mutation serialization, in-memory branch/lineage revisions, exact selected-tip finalization CAS, and lower nonce-issued proposal/replay receipts have landed. Those lower receipts prove transition construction/recreation only; they do not bind the final decorated signed body, outer envelope/KES evidence, preference, or finality. | Before optimistic activation, add the full authenticated-executed and exact-tip preference capabilities, `storeValidated`, durable revisions and publish/effect journals, the sampled exact-hash cascade, and portable `T_weight` evidence. The lower receipts and in-memory CAS are containment, not crash-safe authority. |
| Operator consensus keys | One atomic public KES+VRF registry, rooted long-term-signed genesis pairs/runtime histories, an `inclusionPeriod + 2`/N-2 activation model, and exact-hash historical view adapter exist. This is a period-index lookback, not a claim of two fully elapsed durations after intra-period inclusion. Production still uses the period-zero-only `ActiveOperatorConsensusKeys`; the historical resolver and rooted roster are not production-wired. `StakeRegistry`/`SharedServices` still admit receiver-local seedlist/current-state population inputs. O-11 ratifies a branch-bound historical population/weight boundary plus Cardano-style pledge/self-stake, slashable self-bond, proportional delegation slashing, and `bond >= extractable value`; its exact type, field, codec, authorization predicate, and per-network values remain engineering gates. The runtime cert lacks explicit network/genesis/era domain and containing-hash/root witnesses, and acceptance permits same-owner full-pair reuse or a successive complete record in which only one key changes. Atomicity requires both active keys to come from one N-2 preregistered record; canonical duplicate/partial-rotation semantics and proofs remain engineering. Local rotation is also absent: VRF secrets derive from the long-term identity and `OperationalKeyMaker` holds one destructively evolving KES secret (`KesRegistrationCert.scala:94-104`; `KesRegistrationCertValidator.scala:198-209`; `KesRegistrationCertAcceptanceManager.scala:128-153`; `ActiveOperatorConsensusKeys.scala:14-43`; `HistoricalOperatorConsensusKeyRegistry.scala:111-203`; `LocalOperatorKeyPairGate.scala:110-123,174-185`; `OperationalKeyMaker.scala:9-20,37-90`). | Unregistered period-zero grinding is blocked, but runtime rotation needs a domain/witness schema, canonical duplicate/partial-rotation rules, atomic future KES+VRF secret provisioning, and exact-record selection in addition to public resolver wiring. Registration is not membership. Implement O-11's ratified structure and derive its schemas/parameters; implement O-12's ratified no-`k2`-secret-retention and `RecoveryRequired`/rejoin baseline with quantified common-prefix bounds. |
| Eta history | Live production/receipt reject incomplete or empty mature-period source ranges. `EtaStateManager.getEtaAt` bypasses receiver-current MPT state and ambient caching; both production GSAMs pass the exact parent, and admission walks the exact Phase-2 anchor (`EtaStateManager.scala:60-64,121-141,174-198`; `GlobalSnapshotAcceptanceManager.scala:1331-1353`; `SharedServices.scala:105-110,443`; `GlobalSnapshotConsensus.scala:475-524,700-717,1323-1365`). The checkpoint now signs an exact execution-state reference, but shard producer/attester eta, active keys, committee membership, and the wire-epoch check still use ambient callbacks, the period-zero registry, and locally supplied `etaRotationSnapshots` rather than a historical view proven from that reference (`ShardCheckpointProducer.scala:405-418,650-654`; `ShardCheckpointGl0AcceptanceManager.scala:461-485`; `SharedServices.scala:305-308`). | Make the signed reference an authenticated Phase-2 capability, then resolve eta, N-2 registry/roster/stake, committee, KES step, and canonical parameters from its exact historical branch at every producer, signer, verifier, and adjudicator. The field's presence alone cannot prevent sibling-local committee splits or false-slash asymmetry. A complete-empty source also needs one explicit portable rule distinct from unavailable history. |
| Watchtower adjudication residuals (`WT-002`) | Complete checkpoint replay now has typed `Reproduced`, `ProvenInvalidTransition`, and `Unavailable` outcomes and consumes the signed exact base (`InvalidStateProofValidator.scala:20-25,94-105,189-240`). However, `verifyExecutionCertificate` still invokes ordinary structural `preCheck`; an included/root keyset mismatch prevents the signature fold from yielding an authenticated signer quorum (`ShardCheckpointGl0AcceptanceManager.scala:335-345,461-528`). Evidence carries the full checkpoint inline with no frozen bounded/content-addressed retrieval contract (`InvalidStateProofEvidence.scala:18-29,59-71`). The slash guard returns already-slashed when **any** record matches `(shardId,checkpointHash)`, although records themselves are per signer (`InvalidStateProofSlashedReader.scala:20-37,70-93`; `InvalidStateProofSlashManager.scala:212-240`). Certificate validity can still depend on ambient committee/key/eta callbacks and local `etaRotationSnapshots` (`ShardCheckpointGl0AcceptanceManager.scala:461-485`). | Keep strict structural validation in ordinary acceptance, but expose a separate portable authenticated-quorum capability for adjudication. Freeze bounded evidence carriage/retrieval for the exact checkpoint, inputs, base bytes/proofs, and historical registry/eta/parameter witnesses; distinguish `Valid`, `Invalid`, and `HistoryUnavailable` before any slash; deduplicate each `(signer,shard,checkpoint)` independently; and forbid local configuration, current registry, receiver head, timeout, or missing-history fallback from proving a fault. Missing authenticated history defers and cannot slash. `WT-002`, `WT-002A`, and `WT-002B` remain open. |
| Tower verifier safety cut | Every suffix and level-chain occurrence resolves the current atomic period-zero pair, verifies the VRF proof over the header's exact carried concatenation of `eta` and `slot` bytes, compares its derived/carried output, and ignores sender `activePoolSize`. Snapshot, attestation, and tower consumers share the predecessor-indexed artifact-period rule, including the `R-1/R/R+1` boundary. Every otherwise-valid nonempty proof then returns historical eligibility unavailable (`EtaCalculation.scala:36-47`; `TowerVerifier.scala:125-147,199-258,344-360`). | Direct forged proof/output, claimed-pool authority, and the one-snapshot-early tower rotation bug are closed. The carried eta is only cryptographic self-consistency, not canonical historical randomness. Portable verification remains disabled until exact branch-historical registry/roster/stake/eta witnesses and authenticated tower/SMT inclusion exist. |
| Root and replay-witness coverage | Current per-MG root covers field 5 plus seven Mg partitions 25-31 but excludes economic active allow-spends field 7 and field 32 (`GlobalStateConverter.scala:1497-1506`; `GlobalStateKey.scala:311-332`). Field 32 nevertheless remains writable/reconstructed in GL0 (`GlobalStateConverter.scala:1362-1393,1691-1725,1735-1803`) and checkpoint replay consumes the resulting prior info (`ShardCheckpointWiring.scala:263-314`). A root-verified peer backfill strips it (`PinnedCurrencyInfoReader.scala:341-380`). The incremental contains the field hash and accepted delta, not the full preimage (`currency.scala:52-61,222-240`). The production-path characterization now proves that full local bytes and stripped backfill bytes share one signed GL0 state reference/root but produce replay-success versus replay-unavailable for the same signed currency binary; it also preserves the `None`/`Some(empty)` proof distinction (`ExecutionBasePinReExecutionSuite.scala:573-643`). | The characterization closes only the RED/reproduction half of SHARD-E-006. A restored diff needs a complete root/write-scope contract and a distinct exact replay-input commitment. First carry the exact optional full `globalSnapshotSyncView` witness at every required window boundary, preserve `None` versus `Some(empty)`, hash-match `CurrencySnapshotStateProof.globalSnapshotSync`, and bind the explicit ML0 operator population. Before live activation, current artifacts must stop projecting/signature-validating through lossy `CurrencyIncrementalSnapshotV1`, and the population must resolve against authenticated canonical per-MG operator state rather than the submitted proof subset. Then remove field 32 from all GL0 MPT/diff/load/reorg paths while retaining it in ML0 `CurrencySnapshotInfo`. Missing material defers and cannot slash. ECO-F32 remains HIGH and OPEN. |
| Physical MPT key grammar | Multiple rooted partitions still reconstruct logical maps from `entries.values`, embedded fields, or `headOption` without proving that each physical key equals the canonical key derived from the decoded value (MPT-01 through MPT-06). For MPT-07, current worktree physical preflight rejects uppercase, odd, invalid, aliased, duplicate encoded, and terminal-prefix sets before full/incremental construction and principal live mutations; public incremental producers sort inserts/removals; the builder has an independent nonshrinking guard; disk/wire decoders, durable capture, overlays, and replacement savepoints carry the same physical invariant. Key-only incremental validation avoids sorting/copying the complete economic image on each insert. | This is partial hardening, not ROOT-011 or ROOT-008 closure. Exact duplicate JSON object members are collapsed by the current map decoder; raw entry/key/value/aggregate limits need O-17/R008-04; pinned/network/disk/reorg integration and production-scale tests remain; compound mutation ownership is still ROOT-005; and canonical wrong-root bytes still need ROOT-009/BR-05 authenticated verify-before-install and crash-atomic publication. The exhaustive structural target remains `ROOT-008-GL0-PARTITION-GRAMMAR.md`; O-07/ECON-G separately owns economic validity, while field-32 witness/removal and `ROOT-008-F00..F34` still gate consumer migration. |
| Rooted System indices and recovery residuals | `consensusRootEntries` now retains every `SystemNamespace` entry and excludes only field 32; exact-key index reads distinguish absent, malformed, and present values. Indexed targets/bucket hashes fail closed, expiry bucket epochs must equal target-derived expiry, and the field-3/field-5 currency union rejects dual presence (`GlobalStateKey.scala:508-544`; `GlobalSnapshotInfo.scala:318-329,392-403`; `StrictMptRead.scala:18-37,66-127`; `GlobalStateReaderOps.scala:164-245`; expiry consumers `AllowSpendStateManager.scala:358-432`, `TokenLockStateManager.scala:383-454`, `NodeCollateralStateManager.scala:124-198`). This closes the original ECO-IDX-01/02 same-root System-sidecar and known exact-target defects. | Do not generalize the closure. `WithdrawalTimeLimit` is still local configuration that changes rooted collateral-withdrawal expiry bytes (ECO-IDX-03). A missing MultiBranch parent/ancestor still falls through to finalized base (SMT-02; mechanism confirmed, exploit sequence plausible) and needs a root-verified base anchor plus typed `ParentStateUnavailable`; GSI cannot heal it. Peer GSI is still unbound to proved bytes (BR-02); raw candidate bytes are installed before the caller compares the signed root and dependent stores are not published atomically (BR-05). Field 32 remains open (ECO-F32), and ROOT-008/009 remain open for key-aware prefix reconstruction and the full recovery matrix. |
| MPT persistence ordering | `MptStore` starts one detached persist fiber per sync/commit and advances its in-memory ordinal immediately. Each fiber later reads mutable producer state and writes directly in place (`MptStore.scala:165-174,345-408`; `FileSystemMerklePatriciaProducer.scala:335-343`; `MptStateStorage.scala:45-91`). Current worktree cutoff logic refuses to delete generations newer than the delayed caller ordinal, and its regression preserves N+1/N+2 under a stale N cleanup (`MptPersistenceFailureSuite.scala`). Producer-backed state-proof validation is also now read-only instead of advancing trie/root ordinal caches (`GlobalSnapshotInfo.scala:257-270`; `GlobalMptRootCompletenessSuite.scala`). | The bounded guards prevent one destructive prune path and one validator-side mutation, but a delayed persist for N can still label N+1 bytes as N, publish after transaction rollback, and report a false durable watermark. E9-BRANCH still needs one mutation owner, immutable transaction capture, post-Commit serialized atomic publication, digest/root readback, and a receipt-backed watermark before runtime activation. |
| Durable tower/SMT recovery | Current historical replay consumes one strict retained image, requires the exact contiguous eligible range `0..expectedEligible` including ordinal zero, preflights every `ordinal + k`, builds a fresh tree in isolation, and publishes it with one atomic swap. Malformation, gaps, failure, and cancellation preserve the prior tree (`HistoricalCommitmentSmtStore.scala:179-264`; `HistoricalCommitmentSmtStoreSuite.scala:293-337,349-559`). The old detached tower skip-ahead driver and startup provider publication are removed; a staged serialized exact-hash coordinator/finalizer exists but is deliberately unwired (`GlobalSnapshotConsensus.scala:1446-1456`; `TowerCatchupCoordinator.scala:20-36,216-273,315-398,400-513`; `TowerFinalizer.scala:33-74,156-235`). | `STOR-02` focused replay hardening is green, but disk/crash/reorg integration remains open. `STOR-03` is contained dark, not closed: coordinator state is in memory and each advance walks tip-to-cursor history (`TowerCatchupCoordinator.scala:20-23,235-238,275-310`), while the builder reads mutable storage head rather than a published exact target (`TowerProofBuilder.scala:92-165`). Durable rebuild, hard resource bounds, and atomic provider publication are absent. The provider remains `None` and proof routes remain unavailable/503 (`GlobalSnapshotConsensus.scala:1446-1456`; `NipopowRoutes.scala:82-87`; `NipopowRoutesSuite.scala:101-120,237-242`). |
| Retained `smtRoot` | The current worktree requires active-era `smtRoot=None` through one shared gate (`GlobalSnapshotActiveEraValidator.scala:10-30`) at production/validation (`GlobalSnapshotConsensus.scala:715-717`; `GlobalSnapshotConsensusFunctions.scala:282-299,353`), signed download (`Download.scala:368-381,607-622`), context/traverse (`GlobalSnapshotContextFunctions.scala:61-72`; `GlobalSnapshotTraverse.scala:107-116,183-194`), ML0 adoption (`StateChannel.scala:213-230,306-315,423-432`), generic storage (`SnapshotStorage.scala:100-125`), combined-checkpoint read (`FinalizedSnapshotReader.scala:145-169`), and direct serving (`SnapshotRoutes.scala:62-82`). State-proof comparison is exact (`StateProofComparison.scala:28-37,45-78`), historical-SMT production wiring is absent, and the schema field remains reserved (`GlobalSnapshotStateProof.scala:124-130`). | SMT-01 is contained by a fail-closed dark safety cut, not closed. Regressions cover the shape, comparator, live artifact validation, tail traversal, storage, combined reading, and routes (`GlobalSnapshotActiveEraValidatorSuite.scala:73-93`; `StateProofComparisonSuite.scala:69-72`; `SmtRootBlindValidateArtifactSuite.scala:62-103`; `GlobalSnapshotTraverseSuite.scala:652-671`; `SnapshotStorageSuite.scala:132-190`; `FinalizedSnapshotReaderMptEntriesAtSuite.scala:158-177`; `SnapshotRoutesActiveEraSuite.scala:150-168`). Future `Some` activation requires one exact branch-bound retained image and durable lifecycle reproduction before mutation/publication. |
| Follow-proof branch image | `ROOT-005A` captures one defensively owned branch-byte image and derives the complete-root follow proof from that image (`MptOverlay.scala:46-84,286-297,953-966`; `GlobalFollowProofService.scala:65-112`). Verification requires every consumed field, exact verifier-derived bounds, exact proved leaf values, and canonical-empty-root evidence for evidence-free proofs (`FollowVerifyCore.scala:585-683`; `MerklePatriciaRangeVerifier.scala:259-268`; regressions `GlobalFollowProofServiceSuite.scala:180-190,195-415`; `MptOverlaySuite.scala:852-904`; `MerklePatriciaRangeVerifierSuite.scala:87-123`). | The service states that it is unwired (`GlobalFollowProofService.scala:39-47`). `ROOT-005B` remains open because the image is not an authenticated exact-parent generation and direct base mutation is outside the overlay owner (`MptOverlay.scala:46-51,65-67,286-297`). Exact `(ordinal,hash,root)` binding and one base/overlay/proof/persistence/replacement owner must precede live service. |
| Serde | MPT values use scodec, but ordinary signing/hashing still has JSON/Kryo paths and the era registry is not the production authority. | Scodec migration is not complete. |
| Derived eta period | `NakamotoConfig.etaRotationSnapshots` uses `math.round(3.1d * k1)` (`config/types.scala:163-168`). | Replace consensus `Double` with an exact ratified integer/rational formula and cross-language vectors. |
| GSI | `GlobalSnapshotInfo` remains on production read, API, and recovery paths. | MPT-primary state/GSI deletion is not complete. |

Receipt qualifier: the lower “nonce-issued” receipts above are issuer-reference
checks inside the trusted unmodified validator JVM, not a cryptographic boundary
against arbitrary in-process code. Reflection after issuer extraction, `Unsafe`,
agents, or modified bytecode can forge their JVM representation. The tests cover
public Scala opacity, null/wrong issuer rejection, and one positive full
validator-to-consumer callback; they do not prove hostile-bytecode resistance.

## 3. Protocol invariants

| ID | Invariant | Enforcement boundary |
|---|---|---|
| SIG-1 | No state-validity signature is emitted without local reproduction of the exact signed result. | Typed verified capabilities at global attestation and shard execution-signature APIs. |
| FIN-1 | P0/P1/P2 belongs to exact `(ordinal,hash,parentHash,mptRoot)`, never an ordinal alone. | Hash-bound `FinalityGate` state and APIs. |
| FIN-2 | P2 is reached only by the ratified optimistic trigger or canonical `k1` depth fallback. | Pure finality transition kernel. |
| FIN-3 | A P2 hash may be orphaned by the valid chain selected under `maxvalid-tk` within `k1` or `maxvalid-bg` beyond `k1`; replacement emits one durable rollback event. Selection from three or more tines is an objective total-frontier function: the same cutoff-complete published valid frontier and branch-authenticated parameters produce the same head independently of collection order, arrival schedule, restart, or prior incumbent. | O-15 cutoff/reveal, selector, metric/tie, evidence/verifier, witness, and proof gates; true-MRCA/revert transaction; downstream outbox. |
| FIN-4 | `k2` affects retained rollback/proof availability only. A fork older than local retention causes verified history/state acquisition and a production halt until objective comparison/reconstruction succeeds; it is not refused because of age. | Retention policy, authenticated archive fetch, recovery coordinator, and production gate. |
| FIN-5 | Global optimistic attestations are for authenticated, locally executed snapshots and are never economic validation or BFT commits. | Snapshot validation plus Snowball emitter. |
| EXEC-1 | Producer and every execution signer reproduce exact input decisions, diff bytes, extracted intents, and complete root at the same P2 base. | Shared pure framework kernel and verified checkpoint capability. |
| EXEC-2 | Ordinary adopters never install a claimed root; they verify threshold/base/scope, apply the diff, and recompute the root. | Checkpoint acceptance and GSAM integration. |
| EXEC-3 | Every diff-writable key is committed by the verified root and confined to the assigned metagraph/framework namespace. | Frozen root/key schema plus bounded diff validator. |
| EXEC-4 | Shard depth/fork choice never silently substitutes for missing independent executions. | Separate chain-status and execution-status checks. |
| ECON-1 | Enabled framework operations have explicit authorization, exact checked arithmetic, conservation, and permanent semantic replay protection. | One reference/production kernel used by producer, signer, and watchtower. |
| ECON-2 | `authoritative*`, `AdoptFromSignedFields`, opaque/custom data, decoder probing, and ML0 claims cannot construct framework writes. A separately typed deterministic GL0 protocol correction may update metagraph state only from GL0 toward downstream. | Closed lane/schema boundary, kernel input ADT, and GL0 correction transition. |
| XMG-1 | Only an exact canonical P2 origin can authorize a cross-metagraph consume. | `FinalityGate.requireCanonicalAtLeast(ref,P2)`. |
| XMG-2 | Every GL0 node applies one deterministic conflict/nullifier/settlement kernel over extracted signed intents. | GL0 proposal and follower transition. |
| XMG-3 | Per-MG diffs cannot write another MG or global nullifier/inbox partitions. | Key-namespace validator. |
| XMG-4 | One-shot consume, settlement deltas, permanent nullifier, and delivery append are atomic; acknowledgement is metadata-only. | Canonical MPT transaction. |
| XMG-5 | Shard count changes scheduling only. The same trace at `numShards=1` and K has identical economic results. | Differential cluster gate. |
| XMG-6 | Execution-certified ML0 mirror state and GL0-owned pending settlement overlay are separate; every read uses their exact effective composition and acknowledgement cannot change it. | Typed MPT partitions, effective-state reader, pre/post-ack byte equality. |
| WT-1 | Watchtower evidence is exact-base/input reproducible, deterministic, signer-specific, and cannot slash on unavailable history. | Evidence verifier plus generic slash kernel. |
| SER-1 | Every active consensus object has one bounded canonical Scodec representation and a signed domain. | Genesis manifest and strict codecs. |
| ERA-1 | Greenfield starts `ScodecV1` at ordinal 0; future upgrades use an exact Phase-2 hash-bound era schedule. | Genesis and protocol-era state. |
| STATE-1 | Canonical MPT bytes plus exact branch/checkpoint/finality journals are authority; GSI/cache/peer choice never is. | Typed state APIs and production denylist. |
| STATE-2 | Every consensus-readable MPT entry proves `actualPhysicalKey == canonicalKey(decodedValue,scope)` before its value can enter a map, set, nullifier, weight, cooldown, price, or parameter view. | One strict physical-entry parser family used by live, replay, proof, and recovery paths. |
| STATE-3 | Root-invisible indices and caches are never accepted as transition inputs from memory, peer, disk, or reorg bytes. A required derivative is rebuilt deterministically from root-authenticated records or the transition stops. | Complete-root contract, exact-parent derived-index builder, and verified recovery loader. |
| STATE-4 | ML0-owned `globalSnapshotSyncView` is not writable GL0 state, but every framework re-execution receives its exact optional full-view preimage and explicit ML0 operator population from the signed/root-bound replay witness. The preimage hashes to `CurrencySnapshotStateProof.globalSnapshotSync`; `None` and `Some(empty)` never collapse. | Framework-lane/checkpoint witness codec, replay capability, and GL0 field-32 denylist after witness activation. |
| REC-1 | Restart, reorg, catch-up, and bootstrap reproduce exact bytes or stop before mutation. | Verify-before-write recovery. |
| DA-1 | All bytes required to execute, challenge, roll back, or serve remain authenticated and available through the maximum required horizon. | Content commitments, bounded fetch, signer/watchtower retention. |
| CFG-1 | Consensus parameters, committee derivation, resource limits, and era are genesis/finalized state, never receiver-local configuration. | Canonical parameter hash bound into artifacts. |
| MIG-1 | Existing-network migration is a deterministic audited snapshot-to-new-genesis transform, not a live legacy-codec mode. | Offline exporter/importer and migration manifest. |

### 3.1 Surgical repair boundary for `c610a0740`

Do not revert `c610a0740` wholesale. It touched 183 module files and combined the
unwanted change that makes every ordinary noncommittee GL0 node replay sharded
CL1 with unrelated safety and cleanup work. The implementation packet begins with
a file-by-file ownership diff and follows this boundary:

| Keep | Restore/rewrite | Never restore |
|---|---|---|
| Removal of ML0 `authoritative*` fields and `AdoptFromSignedFields`; current full framework recreation path; exact pinned-base/hash checks; replay-only ancestry validation; verify-before-store/KES work; authorization/conservation fixes that survive E2 oracle review; greenfield strict-decode direction. | Only canonical `ShardCurrencyStateDiff`/`perMetagraphStateDiff`; deterministic before/after diff helpers; complete root/write-set contract; producer root+diff output; typed replay-before-sign capability; role-split receive path; ordinary apply/root-check adoption; focused diff/parity tests. | `CrossShardReceipt`; direct shard-to-shard settlement; claimed cumulative balance/lock/artifact/sync deltas; peer/GSI state installation; V1/V2/default-missing compatibility schemas; blind best-tip signing; old `AdoptFromSignedFields` tests. |

Every retained `c610` change still goes through the current audit/oracle. “Keep”
means it is not part of the architectural rollback, not that it is automatically
correct.

### 3.2 Audit finding ownership

This preserves the prior roadmap's finding scope. E0.3 turns it into a
machine-readable one-task/one-test/one-closing-commit ledger; no row below is a
waiver.

| Findings | Owning epics | Required test families |
|---|---|---|
| FIN-01, FIN-02, FIN-03, FIN-12, FIN-13 | E3/E4/E10; the audit's BFT fix direction is rejected, so the owner-ratified Snowball/depth/density protocol and exact-hash downstream retention must close the exploits under their actual assumptions | FIN-M, FIN-D, FIN-S, FOLLOW, REC |
| FIN-04 through FIN-10 | E4, with branch-sensitive registry/cache work in E6 | FIN-D, FIN-W, FIN-S, SIG |
| FIN-11 (fixed regression) | E6/E14 | CRYPTO, SIG |
| ECO-02, ECO-03, ECO-04, ECO-06, ECO-18 | E2/E6/E9 | ECON-A/C/F/G/R, WT, XMG |
| ECO-05 | E2/E9/E10 | ECON-R, XMG, FOLLOW |
| ECO-10 through ECO-17, ECO-20 through ECO-25 | E2/E9 | ECON-D/C/O/B/F/G, XMG |
| ECO-IDX-01, ECO-IDX-02 (closed System-index defects) | Regression preservation in E1/E9/E11 | ROOT-007 System-label slice; do not infer ROOT-008/009 closure |
| ECO-IDX-03 | E1/E9/E10 | ROOT-007, ROOT-009, REC, parameter/era parity |
| ECO-F32 | E5/E7/E8/E9/E11 | ROOT-010, DIFF-003, SHARD-E-006, REC |
| BR-01 through BR-05 | E1/E10/E11/E12 | BOOT-001 through BOOT-005, REC-005, ROOT, FOLLOW |
| BR-06 | Cross-reference only: same defect and closure as ECO-F32 | ROOT-010, SHARD-E-006 |
| MPT-01 through MPT-06 | E1/E2/E6/E9/E11 | ROOT-008, XMG, PERM, WT, ECON-G, REC |
| MPT-07 | E1/E9/E11 | ROOT-011, BOOT-005, REC-005, RESOURCE |
| STOR-02, STOR-03 | E4/E11 | TOWER, ROOT, REC |
| SHARD-01 through SHARD-11 | E4/E6/E8/E10 | SHARD-C/E/S, FOLLOW, REC |
| SMT-01 through SMT-04 | E1/E9/E11 | ROOT, SER, REC |
| SER-01, SER-02, SER-03 | E1/E5/E13 | SER, ERA, LANE, DA, REC |
| NET-01, NET-02A, NET-03 through NET-10 | E12, with domain/size schemas in E1/E5 | NET, RESOURCE, DA, REC |
| ECO-01, ECO-07, ECO-08, ECO-09, ECO-20 through ECO-23, ECO-25, SMT-04, NET-02 (fixed regressions) | E2/E7/E9/E12/E14 as applicable | dedicated exploit regression plus differential/qualification suites |

MEDIUM ECO-14/ECO-16 and NET-08/09/10 remain included because they affect
consensus economics or the permissionless security boundary.

## 4. Ordered epics

### E0 - Architecture freeze and decision register

**Blocks:** every runtime task whose invariant, shared type, or protocol input is
not already locked. A disjoint packet implementing a locked decision may proceed
with its RED test and owned write set while unrelated decision gates remain open.

| Task | Exit evidence |
|---|---|
| E0.1 | Reconcile ADR-0016/0017 and every dependent packet to the owner-ratified lifecycle's layer, consensus, phase, signature, diff, lane, and rollback vocabulary. Repository search contains no global `LockedVoted`, QC, or view-change target lifecycle. |
| E0.2 | Preserve the completed O-01..O-17 owner dispositions in `CONSENSUS-OWNER-DECISIONS.md` and its answers companion. Close only the explicitly named engineering/research/schema/parameter/proof gates; no implementation may silently reopen or fill them with local policy. |
| E0.3 | Create a machine-readable finding ledger mapping every open CRITICAL/HIGH audit item to exactly one task, RED test, owner, and closing commit. CI rejects unowned entries. |
| E0.4 | Freeze exact Phase-0/1/2 capability matrix: serving, metagraph reference, shard base, spend/withdraw, follower adoption, challenge, rollback, archival service, and prune. |
| E0.4A | Phase 2 is reversible in-protocol operational state. External bridge/exchange/unbond providers choose and document their own risk threshold; the protocol does not invent an irreversible phase or claim that `k2` makes an external effect final. |
| E0.5 | Keep economic/public deployment fail-closed until the applicable E14 gates pass. E13 is required before an existing-network snapshot-to-new-genesis fork, not before greenfield development. Development networks may run only with an explicit unsafe/non-economic profile. |
| E0.6 | Produce the `c610a0740^..c610a0740` surgical repair manifest: every touched file classified keep/restore/rewrite/re-audit with test owner. No bulk revert or unrelated deletion is permitted. |

### E1 - Canonical ScodecV1, identities, parameters, and eras

**Depends on:** E0 schemas/vocabulary. Can run in parallel with E2/E3 models.

| Task | Exit evidence |
|---|---|
| E1.1 | Freeze a consensus manifest for snapshots, phase evidence, state-channel envelopes, framework intents, checkpoints, diffs, execution signatures, DA commitments, MPT keys/values, recovery records, and migration manifests. |
| E1.2 | Implement strict bounded scodec codecs. Reject trailing bytes, duplicates, unsorted collections, unknown tags, invalid refinements, non-minimal integers, and over-limit nesting/length. |
| E1.3 | Bind network, genesis, era, parameter hash, object domain/type, exact parent/base, and content commitment inside every signature/hash preimage. |
| E1.4 | Make `ScodecV1` the only new-chain consensus era at ordinal 0; delete undeployed fork compatibility bridges and consensus JSON/Kryo probing. JSON remains API/debug only. |
| E1.5 | Implement one finalized `ProtocolEra` schedule for post-genesis upgrades, including exact activation predecessor, branch/reorg behavior, state/key transform, live-object semantics, stale-node halt, and historical read-only decode. |
| E1.6 | Separate consensus-object hashing from frozen MPT-key derivation. Every intentional key migration has an explicit transform and root vector. |
| E1.7 | Canonical `ConsensusParameters` includes finality, eta, committee/shard, duty, resource, retention, DA, challenge, fee, and upgrade values. Derivations use exact integer/rational arithmetic (`R`, for example, uses a ratified `31/10` rule rather than `Double`). Local mismatch halts before signing/mutation. |
| E1.8 | Encode the owner-ratified V1 binary/social `ProtocolEra` hard-fork authority and exact activation delay for parameter/era/registration changes. A local admin/`ProductionGate` may make one node abstain but cannot change validity, phase, state, or another node. A live signed correction or network-halt mechanism is deferred engineering and has no V1 authority; any future form must be an explicit bounded canonical transition that cannot rewrite or waive validation and has deterministic expiry/resume. |
| E1.9 | Inventory every retained signed commitment. `smtRoot` and its tower-eligibility inputs are retained and must be independently reproduced and verified on produce/follow/restart/bootstrap. A decorative, producer-chosen, or follower-ignored root is forbidden. |
| E1.10 | The unwired configurable `EraCodecRegistry` and invalid legacy bridges are deleted, and a strict one-value `ProtocolEraId.ScodecV1` identity is landed. Replace `HasherSelector`/state-proof ordinal switches with one typed branch-bound protocol-era service. New-chain ordinal 0 selects only ScodecV1 for object bytes, hashes, signatures, state proofs, MPT nodes, and recovery records; no local boundary can change consensus. |
| E1.11 | Give every state-channel payload an explicit signed lane/type. Currency and currency-with-data carry exact framework Scodec bytes for replay plus a separately committed custom payload; decoder success never chooses authority. |
| E1.12 | Move upstream-v4 Kryo/Brotli-JSON types and probing into a read-only offline importer with frozen fixtures. The importer verifies the source snapshot/state and emits one ScodecV1 genesis manifest; active runtime stores cannot invoke legacy decoders. |
| E1.13 | Freeze full byte/hash/signature/root vectors for every composite consensus object and MPT node/value, not only round trips or primitive codecs. An independent implementation and negative corpus must reproduce them. |

The E1a cleanup is deliberately nonactivating. It deletes the unused
Kryo/JSON/Scodec range registry and unused plain-JSON/Kryo bridge typeclasses,
then freezes `ProtocolEraId.ScodecV1` as tag `0x01` with strict unknown-tag and
trailing-byte rejection. Live JSON/Kryo hashing, signing, state-proof selection,
disk probing, MPT hashing, and lane decoding are unchanged and remain guarded
against piecemeal Scodec activation. E1/SER-005/ERA-001 therefore remain open.

The E1b slice is also deliberately nonactivating. It freezes one explicit,
non-implicit ScodecV1 byte contract for the complete MPT commitment ADT: tags
`0/1/2`, bounded packed nibble paths with zero odd padding, 32-byte digests, and
sorted unique branches of at most 16 children. Exact leaf/branch/extension
goldens and strict negative tests pass 15/15. An inverse source guard passes 9/9,
freezes all 23 current JSON commitment-hash sites, and rejects ordinary production
references to the dark codec. That regex inventory is a syntactic fuse, not a
semantic non-reachability proof. Live MPT hashing, proof bytes, roots, aggregate
bounds, and runtime selection remain unchanged; E1/SER-005/SER-006 stay open.

### E2 - Deterministic conservative framework kernel

**Depends on:** E0 grammar; final codecs integrate after E1.

One pure kernel consumes an exact base state and canonical ordered framework
inputs, and returns:

```text
VerifiedFrameworkExecution(
  acceptedIds,
  rejectedIdsWithReason,
  canonicalPerMgDiff,
  completePostRoot,
  extractedGlobalIntents,
  resourceUnits
)
```

| Task | Exit evidence |
|---|---|
| E2.1 | Independent small reference interpreter defines separate rows for native/currency transfer and reference successor; balance writes; every fee lane; allow-spend create/consume/expiry/refund; referenced and metagraph-source spend; token-lock create/replacement/expiry/manual unlock; pricing; GL0 protocol correction including declared balance/supply adjustment; node parameters; rewards; stake/collateral create/withdraw/slash; currency owner/staking messages; global-sync acknowledgement; state-channel inclusion; mint/burn; and rejection semantics. Unsupported operations fail closed. |
| E2.2 | Fix unsigned unlock, no-reference spend authority, fee replay/binding, reward authority, and source/signature/domain checks. |
| E2.3 | Exact checked arithmetic and per-prefix conservation replace saturation/wraparound. Every fee/refund/mint/burn has a named source/sink and bound. |
| E2.4 | Permanent semantic IDs/nullifiers and explicit nonce/sequence rules replace bounded-history replay protection. |
| E2.5 | Same-batch operations thread one ordered state accumulator. Ordering never depends on wall clock, peer arrival, map/set iteration, shard arrival, local head, or exception behavior. |
| E2.6 | Meter deterministic execution, proof, MPT writes, cardinality, and output bytes. Over-budget batches have no partial effect. |
| E2.7 | Custom data and ML0-supplied cumulative fields have no constructor into the framework input/output ADTs. |
| E2.8 | Production/reference differential property tests compare decisions and exact writes after every prefix across supported JVM/OS/CPU targets. |
| E2.9 | Audit the complete Tessellation v4.0.0 framework grammar operation by operation and preserve functionality whose deterministic authority, conservation, ordering, and replay rules pass the oracle/RED suite. Mechanically inventory every economic ADT constructor, codec, route/event source, validator, acceptance branch, balance/supply writer, and configuration-driven issuance path, then human-classify its authority and lifecycle. Do not invent treasury/oracle concepts or regress an operation merely because its existing rule has not yet been restated; repair defective upstream rules explicitly. |

`V4-ECONOMIC-GRAMMAR-AUDIT.md` is the initial source-evidence packet for E2.1
and E2.9. It confirms that manual unlock, metagraph-source spend,
`PricingUpdate`, and protocol balance correction are real v4 capabilities, and
it distinguishes their required target authorities. It is explicitly partial:
until the mechanical reachability inventory and every RED/oracle row close,
E2.9 and O-07's engineering grammar/oracle gate remain open.

The landed E2.1 reference slice is test-only and nonactivating. It supports
exactly four typed operation IDs: zero-fee native transfer, zero-fee currency
transfer, zero-fee allow-spend creation, and zero-fee nonreplacement token-lock
creation. The allow-spend row binds the complete source-signed preimage, exact
domain/lane/per-source parent, and an explicit supplied epoch window; applies
checked `BigInt` arithmetic; reserves the amount without destination credit;
advances a distinct allow-spend reference chain; retains a permanent semantic
identity; and rejects invalid or replayed inputs without partial state. The
token-lock row applies the corresponding complete source/preimage,
domain/lane/parent, explicit epoch-policy, active-principal conservation,
separate-reference, and permanent-replay rules while sharing the ordered balance
ledger. A closed supported-ID set maps each ID to a positive constructor,
prevents those IDs from entering the unsupported sentinel path, and dynamically
fails closed every other manifest row. The four focused suites pass 41 tests.

A bounded test-only production adapter now exercises the real lower native
acceptance managers and currency ML0 wrappers for zero-fee transfer and
allow-spend creation plus single-operation zero-fee nonreplacement token-lock
creation. The production differential suite passes 17 tests. For token locks it
binds production signature/source ownership, exact native/currency scope,
canonical lane-specific genesis, parent, complete signed payload, and an
aggregate result containing exactly that one accepted block. It compares exact
exposed balances, successor reference, claimed-replacement and lane-specific
in-round context fields, and the complete address-keyed active-lock map. The
wrong-owner, lane, parent, genesis, fee, and replacement negatives preserve the
exact production rejection or explicit helper-boundary error. Raw reference
inputs are encapsulated by private adapter implementations and reach reference
execution only through internal projection methods. Existing transfer/
allow-spend mixed-outcome batches remain characterized; token locks have no
batch-parity claim.

This evidence does not implement or validate the production kernel, complete
GL0 acceptance path, token-lock batch differential, E2.8 cross-platform property corpus, canonical
Scodec/hash/signature bytes, MPT writes, complete-root calculation, or runtime
activation. Production batch insufficiency is `Awaiting` while the reference
row rejects, and live GL0 accepts an allow-spend outside the target reference
epoch window. Legacy payload signatures do not bind domain/lane; production
exposes no independent replay-ID or write-order evidence; batch allow-spend APIs
do not expose active-record deltas; single-result observers consume
caller-supplied results and prove no invocation provenance. Token-lock nonzero
fees and replacement remain helper-fail-closed because their target semantics
are not frozen. The differential explicitly records that live snapshot
acceptance accepts a lock rejected by the stricter reference contextual
minimum-duration rule. Token-lock expiry/refund and manual unlock remain open
and require owner review in
[`TOKEN-LOCK-EXPIRY-OWNER-REVIEW.md`](TOKEN-LOCK-EXPIRY-OWNER-REVIEW.md).
Allow-spend consume/expiry/refund remain blocked on O-13 terminal ordering, and
every other E2 operation row remains open.

### E3 - Pure finality and fork-choice model

**Depends on:** E0 phase decisions. Pure model can run in parallel with E1/E2.

| Task | Exit evidence |
|---|---|
| E3.1 | Specify exact Phase-0 `Pending`, Phase-1 `Provisional`, and Phase-2 `Operational` transitions over `(ordinal,hash,parentHash,mptRoot)` and the capability/rollback matrix. Phase-2 conflicts are reversible. `k2` is modeled only as retained-state availability. |
| E3.2 | Implement an independent K/alpha/beta Snowball/Snowman reference cascade with authenticated uniform sampling, ancestor preference, emit-once, stale/replay rejection, `N<K`, eclipse, and adaptive faults. |
| E3.3 | Specify the owner-approved predicate `P2 = decided-attestation T_weight OR canonical k1 depth`; remove/subsume `T_count`. No global BFT round, lock, or quorum certificate is introduced. |
| E3.4 | Model valid-tine `maxvalid-tk` selection within `k1`, Genesis-family `maxvalid-bg` selection beyond `k1`, true MRCA discovery, and the unavailable-local-history state. Pairwise symmetry is insufficient: retain the current three-tine non-transitivity/permutation counterexample, implement the ratified objective total-frontier property without silently inventing candidate order, and close O-15's cutoff/bounded-diffusion and late-reveal semantics, cycle-resolution selector, exact `k1` metric/equality, objective tie, portable evidence/verifier, validator/store witnesses, and security/liveness proof. No ordinal-age floor may override selection. |
| E3.5 | Re-derive or explicitly accept `k1`, eta, K/alpha/beta/weight thresholds, and the `k2` retention recommendation under the actual Taktikos/LDD assumptions. Do not import a Praos bound without proof. |
| E3.6 | Exhaustive small-network and stochastic large-network tests cover partitions, delay cliff, stale samples, equivocation, branch splits, restart, Phase-2 replacement, MRCA older than `k2`, authenticated reconstruction, and refusal to compare truncated asymmetric history. |
| E3.7 | Specify portable `OperationalEvidence`: optimistic decided-attestation set/`T_weight` proof or an authenticated `k1` suffix proves historical qualification, not current canonicality. A permissionless follower/light client also verifies trusted genesis/cached canonical commitment, chain score/ancestry, stale-orphan status, and live replacement tracking. A single peer may provide a self-verifying proof but cannot make an unproved suffix canonical. |
| E3.8 | Model the base Taktikos/LDD transition independently of finality: slot/ordinal/parent validity, VRF eligibility, eta derivation/grinding, equivocation/withholding/selfish production, valid-only maxvalid-tk/bg fork choice, and malicious producer wrong-root rejection. |
| E3.9 | Specify deep-history recovery when the true MRCA predates local `k2` retention. The node halts production, fetches authenticated ancestry/state/proofs, objectively compares valid tines, atomically reconstructs the winner, and then resumes. Manual operation may initiate recovery but cannot select the winner. |

### E4 - Hash-bound durable `FinalityGate`

**Depends on:** E1 artifact bytes, E3 model. Economic release integration also depends on E2; tower tasks E4.10-E4.13 depend on E6's historical identity/registry inputs.

| Task | Exit evidence |
|---|---|
| E4.1 | Replace ordinal refs with durable branch-aware finality state: canonical tip, exact Phase-2 refs/evidence, orphan map, and transition journal. Keep retention/service watermarks outside fork-choice truth. |
| E4.2 | Route all attestation/depth evidence through one pure transition coordinator; remove duplicated direct watermark writes. Delete or rename `SettledOrdinalTracker` so any remaining projection reports retention/service availability and cannot constrain fork choice. |
| E4.3 | Global optimistic attestation accepts only an authenticated locally executed snapshot capability. Implement real sampling/cascade; remove legacy re-attest/latest-vote shortcuts. |
| E4.4 | Atomically coordinate phase transition with canonical MPT branch, rollback journal, checkpoint anchor, binary confirmation, mempool state, and durable follower event outbox. |
| E4.5 | Enable and verify density recovery: unwind to the true MRCA, refold exact branch bytes, recompute eta/registry/finality/tower state, and apply `maxvalid-bg` beyond `k1`. If local rollback state is unavailable, enter the E3.9 authenticated recovery state instead of refusing the valid fork or comparing truncated histories. |
| E4.6 | Expose exact Phase-2 refs and trigger provenance. Operational APIs/events are hash-bound and explicitly reversible. Archival/retention APIs report what history/proofs are locally available without implying a distinct consensus-final phase. |
| E4.7 | Restart at every transition write point produces the exact old state or exact new state, never mixed boundaries. Automatic unsafe finality erasure is removed. |
| E4.8 | **IMPLEMENTED; QUALIFICATION PENDING:** delete dormant GL0 generic BFT event-loop construction, facilitator/round wiring, handler, storage, routes, result, BFT state/schema, BFT config, and global activation paths. The baseline queue was not remotely reachable because `Main` never started `GossipDaemon`; do not retain the retracted heap-DoS claim. GL0 now constructs only the Nakamoto runtime and its services expose no generic consensus object. CurrencyL0 still constructs `ConsensusEventLoop`, mounts its consensus routes, and retains the facility/proposal/signature state machine. The focused architecture suite and affected compile must remain green before closing qualification. |
| E4.8A | **CALLBACK/DRAIN COMPONENT CONTAINMENT LANDED; END-TO-END QUALIFICATION OPEN:** generic rumor consumption starts after local-state and chain-seed success and before the shared ingress gate opens. `GossipStream` now uses bounded item/encoded-byte callback storage, reserved manual gRPC credits, a terminal path outside the full data queue, and a generation fence. Eight dedicated family lanes are item/byte bounded and structurally owned by one subscription generation. Clean/error source termination seals and drains every accepted item before reconnect; offer-vs-seal is linearized and a replacement cannot overlap. Reproduced invalid Brotli, long-term-key/signature, and canonical KES-shape inputs reject without terminating the generation. Still prove downstream sinks, signature/collateral resource admission, exhaustive family fuzzing, resumable multi-sink progress, the cross-hop compressed/decompressed size contract, and durable recovery for process crash or messages missed before delayed subscription. |
| E4.8B | **PRE-STATE MUTATION AND LOCAL SUBSCRIPTION ORDERING CONTAINED; RECOVERY OPEN HIGH:** the three-phase gate rejects out-of-order/failed/cancelled successful seeding, keeps dedicated input/KES/rebootstrap inert, and makes pre-seed ChainSync unavailable. Exact role/profile/session/generation handshakes now gate `Ready`; a serialized readiness owner closes production before committed lane invalidation and opens it only for two current same-session lanes. This is local lifecycle evidence only. Define the boot-mode/catch-up rule and implement authenticated recovery-only disk-head seeding; hostile fresh/cold/rollback tests remain blockers. Full-snapshot rollback is deliberately disabled before cleanup, not implemented. Generation ownership closes detached worker lifetime, but not per-message failure isolation or atomic multi-sink effects. |
| E4.9 | Add durable `RecoveryRequired` handling when objective fork comparison or reconstruction lacks authenticated data: fail-stop signing/production/mutation, expose the missing range/evidence, fetch by exact commitment, and resume only after deterministic verification. Missing local history never becomes a social fork-choice vote. |
| E4.10 | Carry branch-bound tower trial state in signed snapshot certificates, including the per-level history needed to reproduce eligibility and pointers. Producer computes from the selected parent; every recipient independently recomputes it using the exact delayed registry/eta/KES inputs. |
| E4.11 | Preserve the current fail-closed active-era `smtRoot=None` rule and exact comparison (`GlobalSnapshotActiveEraValidator.scala:10-30`; `StateProofComparison.scala:28-37,45-78`; live wiring `GlobalSnapshotConsensus.scala:715-717,1446-1456`). Future activation may allow `Some` only after snapshot `N` commits a branch-bound verified eligible tuple and every lifecycle path derives the identical update/root before mutation. The schema field is retained; no root-blind compatibility path is allowed. |
| E4.12 | Preserve `STOR-02`'s strict one-image contiguous replay and isolated swap (`HistoricalCommitmentSmtStore.scala:179-264`). Replace the deliberately unwired `STOR-03` staging (`GlobalSnapshotConsensus.scala:1446-1456`; `TowerCatchupCoordinator.scala:20-36,216-273,275-398,400-513`) with durable branch-aware tower/SMT recovery. Proof construction must be bound to a published target/store generation rather than mutable storage head (`TowerProofBuilder.scala:92-165`), resource-bounded, and published atomically after recovery parity. |
| E4.13 | Complete portable tower proof construction and verification: SMT inclusion, hash/pointer ancestry, historical registry and eta transitions, KES/VRF signatures, canonical chain comparison, bounds, freshness, and adversarial single-peer withholding/eclipse tests. `TowerEligibility.NotComputed` is removed only when these gates pass. |

### E5 - Payload lanes, metagraph registration, and data availability

**Depends on:** O-09's ratified lane scope and E1 base codecs. Can run alongside E2/E3.

| Task | Exit evidence |
|---|---|
| E5.1 | Canonical registration fixes metagraph ID/owner, ML0 source proof/set, lane/schema, framework version, custom-data commitment, fee/resource policy, and delayed upgrades. |
| E5.2 | Implement signed `FrameworkCurrency` and `FrameworkCurrencyWithData` envelopes plus O-09's retained standalone opaque/data-only lane. The opaque lane carries authenticated custody/availability/ordering only and has zero framework-economic write surface. Decoder probing cannot select or promote a lane. |
| E5.3 | Currency-with-data commits framework execution and custom bytes separately. Custom application output cannot synthesize economics; an independently signed framework fee/intent may bind the exact custom commitment, and mismatch rejects atomically without replay. |
| E5.4 | Define one protocol hard envelope cap, deterministic chunk/content commitment, bounded authenticated fetch, erasure/reconstruction policy, and exact retention horizons. |
| E5.5 | Domain-separate ML0 source signatures, GL0 intake/custody receipts, execution signatures, optimistic attestations, and DA custody proofs. Only the execution signature claims CL1 recreation. |
| E5.6 | Data unavailable before checkpoint eligibility defers the checkpoint. Nonresponse alone is not slash evidence in an asynchronous network. |
| E5.7 | Implement the separate binary-intake protocol over eligible GL0 operators. Before a custody receipt, derive parent/ordinal from authenticated state, verify the exact ML0 source signatures against the delayed canonical per-MG registry/allowlist, enforce deterministic envelope/resource bounds, and durably store exact bytes. Freeze its distinct threshold/lifetime/equivocation/queue/redraw/censorship rules. Intake receipts never satisfy execution quorum. |

### E6 - Stake, KES/VRF, committee derivation, and slash kernel

**Depends on:** E1 identities/parameters; implementation of O-11's ratified roster
direction and closure of its schema/parameter/proof gates before population or
runtime eligibility; economic debit integrates with E2.

| Task | Exit evidence |
|---|---|
| E6.1 | Every active stake/committee/reward weight is joined to live bonded principal with activation, exit, unbond, and slash horizons. No locally observed-active denominator. |
| E6.2 | Complete the atomic preregistered operator-pair lifecycle: one long-term-signed `PeerId + KES master VK/offset + VRF VK` record, rooted early enough to be present in the exact N-2 canonical view before activation, and one atomically provisioned local future KES+VRF secret bundle selected only by the exact-parent active-record capability after public-key comparison. Add period-derived KES step, rotation/revocation, crash recovery, historical verification, and double-sign evidence. Missing/invalid/historically unavailable public or local material fails closed. Both active keys always come from the same selected record. Freeze complete-pair successor/reuse semantics in the canonical schema and RED vectors. O-12 is owner-ratified: now prove the N-2 prefix/common-prefix assumption and implement the settled secret-deletion and `RecoveryRequired` behavior for reorgs crossing erased KES material; do not treat secret rollback or k2 retention as ordinary state recovery. |
| E6.3 | Bind global leader eligibility and both GL0 committee draws to delayed canonical state. For period `N`, one exact-parent capability resolves the branch's atomic key/authorized-population/raw-weight intersection from `N-2`, eta from `N-1`, and active parameters for `N`; registration fixes keys but grants no eligibility. The owner-ratified O-11 candidate representation co-locates authorized population and raw weight in one period-boundary value so roster/stake cannot be mixed across branches; P1 must freeze its exact predicates, codec, root ownership, and network bounds before activation. Bind execution-shard membership/epoch to the exact Phase-2 anchor and close producer-chosen wire epoch grinding. |
| E6.4 | Preserve the public deterministic execution membership draw and shuffled staircase duty unless a new owner ADR changes it. Do not relabel it stake-weighted secret VRF. |
| E6.5 | Generic evidence IDs are canonical and replay-proof. The ratified verifier computes the verdict from exact evidence/inputs rather than trusting a claimant; a guilty verdict debits actual bonded principal once, caps reward, and changes eligibility only at the specified anchor. |
| E6.6 | False, stale, ambiguous, wrong-base, missing-input, split-view, forged-watchtower, or orphaned-branch evidence cannot slash an honest node or force unbounded replay. |
| E6.7 | State the execution-shard/watchtower adversary model quantitatively for S shards: committee capture and correlated draws, adaptive corruption/bribery after public membership, cross-shard chain-quality collapse (including the claimed alpha_total > 1/(2S) boundary), watchtower coverage, and value-at-risk versus bonded loss. Re-derive rather than inherit Polkadot/Praos bounds. |
| E6.8 | Define supported validator-set sizes and fail-closed/degraded modes. Never silently clamp/renormalize `kQuorum`, K, alpha, or weight denominators to locally observed N. Small dev networks use explicit canonical non-economic parameters or depth-only behavior; an impossible threshold halts visibly. |
| E6.9 | Prevent uniform-committee Sybil/key grinding with canonical operator identity, minimum live bonded principal, delayed registration/activation before eta is known, key-count rules, and quantitative stake-splitting/correlated admission-execution-watchtower analysis. |
| E6.10 | Reuse v4 signed node-profile and token-lock-backed delegated-stake/collateral event and withdrawal state only as deterministic identity/backing facts. Remove seedlist/local-peer membership authority after O-11's ratified replacement roster is implemented and rooted. Delivery order is owner rule -> canonical codec/root -> immutable genesis population -> pure boundary fold/undo -> exact N-2/N-1 resolver -> exact Phase-2 artifact refs -> all-consumer migration -> adversarial/restart qualification. A passing model with an injected roster does not close the production gate. |

### E7 - Canonical checkpoint diff and execution result

**Depends on:** E1, E2, E4 Phase-2 ref interface, E5 lanes, E6 committee identity.

| Task | Exit evidence |
|---|---|
| E7.1 | Restore only `ShardCurrencyStateDiff(upserts,removals)` and `perMetagraphStateDiff` in the greenfield schema. Keep `executionBase`, full signed inputs/commitments, and the exact framework replay witnesses required by STATE-4. |
| E7.2 | Do not restore metagraph-originated `authoritative*`, `AdoptFromSignedFields`, direct receipts, unproved per-field replacement deltas, or old fork V1/V2 compatibility codecs. A future GL0 protocol correction is a separate root-covered global transition under E9.11, never a checkpoint field. |
| E7.3 | Freeze complete per-MG root and key allowlist so every diff-writable key is root-covered. Keys are canonical, sorted, unique, bounded, and metagraph-scoped. Field 32 is never a GL0 diff key after STATE-4 activates. |
| E7.4 | Refactor the current currency recreation path to return the E2 execution result and deterministic before/after diff. Producer, signer, and watchtower share this exact function. |
| E7.5 | Checkpoint preimage binds exact Phase-2 base, full ordered inputs/DA commitments, diff, roots, extracted intents, shard parent/ordinal/duty/roster, parameters, and the exact optional field-32 replay preimages plus explicit ML0 operator populations. Each view preimage must hash to the corresponding `CurrencySnapshotStateProof.globalSnapshotSync`. |
| E7.6 | Apply-diff over the exact base independently reproduces post-root byte-for-byte. Missing base/input defers without mutation. At GL0 composition, every signed per-MG pre-root/version must compare-and-set against the proposal parent's current mirror root/version (or an equally strong unchanged-version proof). |
| E7.7 | Implement the locked mixed-`globalSyncView` window semantics: each binary uses its signed nondecreasing historical Phase-2 ref for global reads while local state threads from the signed parent/pre-root. No live-head fallback exists. |
| E7.8 | Close ECO-F32 in strict order: reproduce local-staged versus stripped-backfill replay divergence; add and verify the root-bound exact `globalSnapshotSyncView`/ML0-population witness with `None` distinct from `Some(empty)`; then delete field 32 from GL0 MPT writers, diffs, loaders, recovery, and reorg while retaining it in ML0 `CurrencySnapshotInfo`. |

### E8 - Staircase shard chain, replay-before-sign, and watchtowers

**Depends on:** E6/E7. Hard-anchor integration depends on E4.

| Task | Exit evidence |
|---|---|
| E8.1 | Deterministic bounded fair accumulation preserves per-MG parent order, continuation cursors, exact bytes, and no starvation across mixed lanes. |
| E8.1A | Enforce exactly one checkpoint per shard whose exact containing GL0 snapshot has not reached Phase 2. It batches multiple metagraphs and one contiguous ordered binary list per metagraph; remove configurable `pipelineDepth`, multi-checkpoint accumulation, and shard-depth validity/finality fallback. A second checkpoint cannot be produced, replay-signed, accepted, or embedded until signed evidence proves the first exact checkpoint hash is Phase-2-anchored or deterministically orphaned/requeued. Tentative embedding, ordinal-only watermarks, and receiver-local anchor state never release a successor. |
| E8.1B | Treat one multi-MG checkpoint as one atomic GL0 transition. The outer shard-map key must equal the signed shard ID; every MG window must continue or already equal proposal-parent state; every continuing suffix must be represented byte-exactly in the containing artifact. Any deferred/rejected/omitted segment rejects the whole checkpoint and its P2 hard anchor. |
| E8.2 | Producer duty remains shuffled staircase; enforce the identical parent-relative duty rule at intake, replay-signing, and embedded-artifact validation, then specify timeout/skip, fork choice, sibling recovery, and censorship fallback without BFT locks/views. Embedded validation resolves parent hash/ordinal/slot from portable proposal-parent-bound evidence, not receiver-local shard-gossip state. The producer remains the retained head signature; aggregation may append but never reorder it. |
| E8.2A | Require an embedded checkpoint's signed slot to be no greater than the signed slot certificate of its exact containing GL0 snapshot; an ahead checkpoint stays pending for a later GL0 slot. Monotonicity alone is insufficient: an arbitrary future slot can select an attacker's valid rank and halt the shard after anchoring. Receiver wall clock, receipt time, local skew configuration, and local best tip cannot decide artifact validity. |
| E8.3 | Replace naked-hash emitter with `VerifiedShardCheckpoint`. Producer and every execution signer replay and compare exact diff/root/intents before signing. |
| E8.4 | Separate structural storage, committee replay/sign, watchtower replay, and ordinary noncommittee behavior. Telemetry/tests prove ordinary receivers do not recreate currency. |
| E8.5 | Require distinct execution `kQuorum` replay signatures for every checkpoint. Staircase duty/parent ordering selects the next checkpoint candidate; no shard-depth threshold can make a subquorum checkpoint diff-adoptable. |
| E8.6 | Move shard hard-anchor advancement from tentative GL0 evaluation to exact containing-GL0 P2 transition. Before anchoring/finalizing, deterministically ingest the exact validated embedded checkpoint on nodes that missed shard gossip; duplicate ingestion is idempotent. Orphaned checkpoints requeue binaries exactly once. |
| E8.7 | Implement deterministic noncommittee watchtower assignment/replay/evidence under the locked release rule: required positive coverage makes a checkpoint GL0-inclusion-eligible, while unrelated GL0 snapshots continue without it. No checkpoint-derived local or cross-MG economic effect is released before this coverage. |
| E8.7A | Implement objective fraud adjudication: an assigned, bonded, age/rate/resource-bounded challenge makes all GL0 validators exceptionally replay the challenged sharded-CL1 checkpoint's exact retained framework inputs/base. The computed result, not the watchtower assertion, controls rollback/slash; missing authenticated data defers and cannot slash. Close the remaining `WT-002` design boundaries before activation: a portable authenticated execution-quorum capability separate from strict ordinary structural acceptance; bounded evidence carriage/retrieval for the checkpoint, inputs, base, and historical eligibility witnesses; typed `Valid` / `Invalid` / `HistoryUnavailable` resolution; per-signer `(peerId,shardId,checkpointHash)` slash deduplication; and zero slash predicates derived from local config, receiver-current registry/eta, wall clock, timeout, or peer response. This exceptional path does not replace universal native `GL1 -> GL0` execution. Happy-path sharded-CL1 adoption remains zero-replay. |
| E8.8 | Retain exact binaries/diffs/evidence through the maximum of recommended `k2`, challenge, DA, deep-recovery, and downstream acknowledgement horizons. Retention expiry never changes fork-choice validity. |
| E8.9 | Remove `numShards > 1` as a validity switch. Economic `numShards=1` runs one execution committee/diff/watchtower path; any direct test shortcut is explicitly unsafe and state-differential checked. |
| E8.10 | Freeze `numShards` for v1 at genesis/era. Reject local/live changes. Defer resharding until a separate hash-bound protocol-era transition specifies drain, deterministic reassignment/handoff, committee transition, replay/nullifier continuity, rollback, and recovery. |
| E8.11 | Specify eta/roster crossover for buffered, produced, partly signed, execution-certified, and tentatively embedded checkpoints. One exact anchor selects one roster; no receiver-local expiry or mixed old/new threshold. Activation delay and requeue rule preserve liveness across rotation/reorg. |
| E8.12 | Partition transport by authority and work: only current execution/watchtower assignees subscribe to raw per-shard checkpoint/replay lanes; every GL0 validator subscribes to a distinct certified-checkpoint/diff lane needed for ordinary noncommittee adoption. Rotation, reconnect, delayed activation, and `SubscribeStarted` profiles are exact-hash/era bound and cannot overlap or drop a required generation. |

Current E8.1A/E8.6 landing is deliberately partial. The exact-hash happy path is
present and buffer drain no longer suppresses held-byte rebroadcast, but held
bytes/anchors are volatile, Phase-2 replacement and rollback are not represented,
finality-to-anchor delivery is not a durable exactly-once transaction, and fair
bounded batching is absent. The rule currently constrains the honest producer
only: a checkpoint does not carry verifier-checkable evidence that its exact
parent reached Phase 2. A Byzantine committee can therefore replay-sign a
pipelined child; the fail-closed ancestor selector can then repeatedly surface an
under-certified parent and stall that shard. `REC-002`, `NET-002`, and the
Phase-2 reorg/malicious-pipeline cases in `SHARD-C-*` remain RED/required; this is
not a completion claim.

The current receive-side staircase patch rejects off-duty checkpoints at intake,
replay-signing, and embedded validation, but `SHARD-C-009` remains RED because the
embedded path resolves its parent from the receiver's local shard-gossip store.
Nodes with and without prior gossip can therefore disagree on the same GL0 artifact.
Parent duty context must be portable and bound to the proposal parent: at minimum,
canonical GL0 state needs a rooted per-shard
`ShardCheckpointRef(hash, ordinal, slot, epoch)`, or the proposal must carry an
equivalent inclusion proof against its exact parent-state root. The child must
compare-and-set that ref, and roster/eta resolution must use the same canonical
context. For stronger role accountability, a future schema should bind producer
identity or a producer-role domain explicitly: `committeeSignatures` is excluded
from the current preimage, so list reordering can change the nominal producer
label. This is not a demonstrated duty bypass after the current patch: only the
unique scheduled peer passes as head, an honest scheduled peer will not countersign
an off-duty-head artifact, and a scheduled Byzantine signer has already vouched for
the exact checkpoint bytes. The acceptance test rejects a complete execution quorum
whose retained head is off duty. Even after that, `SHARD-C-010` remains RED: the signed slot is checked for parent monotonicity
and mapped to a staircase rank, but has no canonical upper bound. An assigned member
can choose a far-future valid window and, if it obtains the mandatory replay
signatures, strand the shard behind that parent slot. A receiver-wall-clock check
would create asymmetric validity and is not an acceptable repair.

SHARD-11/SHARD-C-011 capture that duty validation currently consumes local HOCON `staircaseDeltaSlots`.
Identical artifacts can therefore select different scheduled peers under
configuration skew. The delta and its parameter hash must be rooted in the same
proposal-parent/genesis consensus context as the roster and eta before this is a
portable validity rule.

E8.3's signing API is also partially landed: only a sealed capability minted by
checkpoint intake replay can reach the shard attestation emitter, and ancestor
closure replays with the ancestor's own epoch membership before signing. The
checkpoint has no canonical diff/intents/complete-root result yet, so this does
not close E8.3, E8.4, or ordinary zero-replay adoption.

### E9 - Global checkpoint composition and cross-metagraph settlement

**Depends on:** E2, E4, E7/E8. Allow-spend v1 can be implemented before other interaction types.

| Task | Exit evidence |
|---|---|
| E9.1 | Ordinary GL0 checkpoint acceptance verifies duty/roster/execution certificate, the separately domain-separated positive replay coverage assignment/signatures/threshold, continuity, exact canonical Phase-2 base/origin refs, scope, and absence of a pending authenticated mismatch; compare-and-sets each signed pre-root/version against the proposal-parent mirror, applies diff, and recomputes root with zero ML0 recreation. |
| E9.2 | Canonically merge native GL1 writes, per-MG diffs, global framework intents, rewards/slashes/config changes, GL0 protocol corrections, and custom commitments. Conflicting keys or invalid roots reject atomically. |
| E9.2A | One typed balance ledger keyed by `(currency scope,address)` compiles transfers, framework/data fees, rewards, allow-spend reservation/release, token-lock reservation/unlock/replacement, mandatory GL0 SpendActions, and slash bounties. It reserves atomically in canonical operation order, rolls back a rejected multi-address operation, rejects projected values outside `[0,Long.MaxValue]`, and makes final application prove equality with the projected map. GL0 and ML0 use the same kernel; mandatory Phase-2 GL0 inbox work precedes conflicting ML0-local work. |
| E9.2B | Admission and application have identical operation semantics. Preserve the fixed no-destination-credit (ECO-20), one-shot replacement release (ECO-21), allow-spend terminal/reference (ECO-22), exact-lane (ECO-23), canonical cross-shard field-25 balance proof (ECO-25, fixed at `41c19903d`), and field-7 reconstruction (SMT-04) regressions. Complete exact post-class projected arithmetic (ECO-24). Checkpoint execution signatures bind checkpoint-wide outer-fee/global-intent reservation, not isolated per-MG roots. |
| E9.2C | Freeze and implement the complete-root/physical-key contract before checkpoint composition activates. Any active/expiry index read by a transition is canonical rooted state or is rebuilt inside the exact-parent session from strictly key-bound rooted records. Mg entries, nullifiers, stake/collateral, token locks, slash/cooldown state, price state, and parameters reject physical-key/value mismatch, duplicate logical identity, mixed scope, lossy `headOption`, and value-only reconstruction before producing a typed view. |
| E9.3 | Per-MG diff cannot write global nullifier/inbox or another MG. Every GL0 node runs one pure global intent conflict/settlement kernel. |
| E9.4 | Define signed authorization/consume identities and permanent replay keys. Network/genesis/era/type replay and proof-container malleability reject. |
| E9.4A | Replace inherited data-application fee replay semantics. A domain-separated fee consumes the rooted per-MG/source field-27 parent, binds exact available opaque bytes or a ratified chunk manifest, and atomically updates balance plus head. Exact fee/custom-item cardinality rejects extras, duplicates, and partial acceptance. |
| E9.5 | Implement one-shot allow-spend state machine: reserve, consume/cancel/expiry, exact refund/fee rules, permanent nullifier, and pending delivery in one transaction. |
| E9.6 | Define deterministic precedence for concurrent consumes, cancel/expiry, inbound delivery, local spend, and same-snapshot dependencies. One authorization has at most one winner across shards/branches. |
| E9.7 | ML0 inbox/cursor acknowledgement is hash-bound, contiguous, idempotent, and metadata-only. It cannot change effective balances or erase permanent replay state. |
| E9.7B | Replace bounded `GlobalSnapshotsProcessed` reconstruction with a per-MG hash-linked delivery head, rooted pending entries, durable ML0 applied cursor/state proof, and compare-and-set acknowledgement. GL0 append order assigns the destination sequence independently of sparse global ordinals; receiver observation order, restart, compaction, and deep recovery cannot skip or reapply it. |
| E9.7A | Separate `Ml0FrameworkMirror` from `GlobalSettlementOverlay`. Cross-MG global writes never invalidate the checkpoint's certified mirror root; all reads use exact checked effective composition. ML0 applies mandatory inbox before local spends and acknowledgement compacts representation with byte-identical effective state. |
| E9.8 | Add token-lock/transfer/other cross-MG types only after each has explicit authorization, conservation, timeout/refund, ordering, and acknowledgement rules. |
| E9.9 | Identical traces at shard counts 1, 2, and K have identical accepted IDs, economic leaves, roots, nullifiers, supply, and delivery state. |
| E9.10 | Bound permanent nullifier/authorization/inbox/evidence growth without reopening replay: protocol fees/rent, authenticated compaction/accumulator, or archived tombstone proof. Age/window eviction alone is forbidden. |
| E9.11 | Define the GL0 protocol correction transition for malformed metagraph state. It binds exact target MG/pre-root/version, deterministic correction diff, post-root, activation and replay domain, is executed/root-checked by GL0, and emits a mandatory downstream rebase. No ML0/CL1/DL1 signature or checkpoint field can authorize it. |

E9.4A and E9.7B are schema-gated by O-14 and O-13 respectively. Their directions
are owner-ratified; build order is pure reference oracle and RED vectors, active Scodec/domain
freeze, atomic MPT transition, all execution-signer/watchtower integration,
ordinary diff adoption, then restart/reorg/compaction and shard-count parity.
Neither slice may ship as a node-local cache, bounded history window, optional
field default, or ML0-authoritative override.

The narrow ECO-20 through ECO-23, ECO-25, SMT-04, and the ECO-IDX-01/02
System-index regressions are landed. ECO-25
closed at `41c19903d`: the GL0-local cross-shard balance path reads canonical field
25, preserves exact committed bytes, distinguishes absent from malformed storage,
and binds the embedded account. E9 remains planned because those repairs do not
supply the shared cross-class ledger, checkpoint diff adoption, global
conflict/nullifier kernel, exact destination projection (ECO-24), or the complete
root/key/recovery contract (ECO-IDX-03, ECO-F32, BR-02/05, ROOT-008/009/011,
and MPT-01 through MPT-07).

The landed root slice also adds `ActiveAddressIndex` ownership for
`LastCurrencySnapshotsProofs` and `MetagraphSyncData` on rebuild and incremental
write/replay (`GlobalStateConverter.scala:724-856,1265-1275,2920-2962`). That is a
prerequisite, not closure of E9-01: there is still no complete
`from(mpt, ordinal, era)` projection with independent parity across every field
and `Option` shape. In particular, do not treat the existing all-partitions
producer/rebuild corpus as independent coverage of every layout and absence case.
Optional `GlobalSnapshotStateProof` slot presence is now byte-canonical
(`GlobalSnapshotInfo.scala:302-388`), but `ROOT-010` must still preserve
`None` versus `Some(empty)` in the future semantic field-32 replay witness.

#### E9 immediate build lanes and join order

| Lane | Ordered work | May run in parallel with | Join gate |
|---|---|---|---|
| E9-STATE | (1) Preserve the landed System-index root/strict-target contract and root the active-era `WithdrawalTimeLimit`; the local environment fallback is forbidden for consensus. (2) Complete E9-01's full `from(mpt, ordinal, era)` projection and independent field/absence-shape parity; the two new owner indices are only prerequisites. (3) Before stripping field 32, land E7.8's exact optional full-view/ML0-population replay witness and prove local-staged/backfill parity; then remove field 32 from every GL0 write/diff/load/reorg boundary. (4) Land one strict `(physicalKey,rawValue) -> typedEntry` grammar and migrate Mg fields, consumed-allow-spend nullifiers, stake/collateral, token locks, slash/cooldown state, price state, and parameters without value-only reconstruction. (5) Bind every peer/disk/reorg projection to exact authenticated bytes and install through staged atomic recovery. (6) Make malformed durable tower/SMT keys fail the whole image into recovery. (7) qualify live/replay/signer/watchtower/adopter/restart/reorg parity. | Pure DLV/FEE/economic oracles and E7 checkpoint schema work against the frozen root/key interface. Runtime E9-GLOBAL and E9-BRANCH activation cannot proceed independently. | Preserve ECO-IDX-01/02 and the System-label slice of ROOT-007; close ECO-IDX-03, E9-01, ROOT-008/009/010, BR-02/05, SHARD-E-006, XMG-013, PERM-005, WT-010, ECON-G-002, REC-004/005, and zero consensus read of unrooted imported bytes. |
| E9-DLV | Implement O-13's ratified direction; close the remaining total-order, bounds, allocation, and codec gates; pure delivery oracle/RED traces; active cursor/delivery codecs and rooted leaves; GL0 append/ack kernel; ML0 cursor/inbox execution; restart/reorg/compaction. | E9-FEE and exact-base recovery design until shared schema integration. | XMG-004/005/005B/006/008 plus codec/root parity. |
| E9-FEE | Implement O-14's ratified direction; close its codec and E9 reservation gates; pure fee oracle/RED traces; domain-separated parented fee codec; field-27 atomic balance/head kernel; exact opaque-byte/manifest bijection; producer/signer/watchtower integration. | E9-DLV and DA retention work until shared balance/reservation integration. | ECON-F-002/003, ECON-REF-001, ECON-C-001, shard-count parity. |
| E9-GLOBAL | Checkpoint-wide ordering/reservation over outer binary fees, native writes, global intents, rewards, slashes, and protocol corrections; compare-and-set every per-MG mirror version; ordinary GL0 diff adoption. Preserve ECO-20 through ECO-23, ECO-25, and SMT-04 while closing ECO-24. | Pure DLV/FEE models and E7/E8 checkpoint format work; active integration waits for the E9-STATE root/key grammar. | ECO-10 residual closed; ECON-BAL-002/003, XMG-002/003/007/008/012/013, ROOT-006/007/008, and ECON-O-001. |
| E9-BRANCH | Verified MPT base-anchor identity, exact-parent sessions, descendant-preserving finalization, scratch candidate validation, crash-safe anchor persistence, and authenticated recovery. After the base anchor is verifiable, checkout and every point/prefix/root/raw-byte path return typed `ParentStateUnavailable(requestedParent,missingAncestor,reason)` before mutation when the exact chain cannot be resolved. | Pure economic models and schema review. | No consensus read can fall back from an unavailable hash to base or heal it from GSI; branch/restart/reorg model parity. |

`ROOT-005A` repairs the local follow-proof construction boundary: one immutable
branch image is filtered through the complete consensus-root entry set, and every
consumed field, verifier-derived bound, and leaf value is mandatory. Evidence-free
success is limited to the canonical empty trie root
(`MptOverlay.scala:46-84,286-297,953-966`;
`GlobalFollowProofService.scala:65-112`; `FollowVerifyCore.scala:585-683`;
`MerklePatriciaRangeVerifier.scala:259-268`; regressions
`GlobalFollowProofServiceSuite.scala:180-190,195-415`). This removes omission and
mixed-image acceptance from the focused proof primitive, but it does not activate
HTTP cross-shard evidence. The services remain unwired because `ROOT-005B` has not
bound that image to an authenticated exact `(ordinal,hash,root)` or unified all
base/overlay/persistence mutation under one owner. Decoder success, a self-claimed
root, or a locally current branch cannot substitute for those gates
(`MptOverlay.scala:46-51,65-67,286-297`;
`GlobalFollowProofService.scala:39-47`).

E9-BRANCH is one activation unit, not a sequence of independently enabled guards.
Today a successful fold deletes pending descendants, restart restores no
authenticated base identity, and the global finality sinks update the tip tracker,
chain store, and outbox before the overlay. Enabling unknown-branch rejection alone
therefore converts existing silent corruption into a normal-path halt after partial
external mutation. The verified anchor, descendant retention, exact session,
explicit `RecoveryRequired` state, and preflighted finality transaction must be
available before the fail-closed guard becomes active.

The first dark implementation slice is an immutable local MPT image store: copied
and sorted bytes, independent root rebuild, versioned deterministic encoding,
content digest, forced atomic image/manifest publication, verified readback,
generation CAS, lifetime OS directory ownership, bounded streaming decode, and
typed missing-artifact recovery. It has no runtime caller and closes none of
ROOT-002/003/004 by itself. Activation additionally requires an active-era
whole-image physical-key/value verifier inseparable from the root algorithm, exact
FinalityGate-authenticated
`(snapshotHash,parentHash,ordinal,mptRoot)` anchoring, and the durable multi-sink
finality intent. A caller-supplied era label or self-consistent manifest is not
authentication.

The accompanying pure exact-parent resolver proves only a canonically encoded,
ordinal-contiguous, cycle-free structural reference path to the supplied base. It rejects
unknown/missing ancestry, reserved/noncanonical identities, and a pending entry
that shadows the base hash. It is deliberately named `ResolvedParentLineage`, not
an execution session: no replay session exists until one atomic overlay read
captures the complete parent byte image plus the pending/base mutation generation.
The generation comparison must occur inside the eventual commit CAS; a separate
check followed by mutation is a TOCTOU bug.

The dark chain-store ancestry walk is bounded and resolves exact hash-addressed
bytes before consulting an ordinal fallback. A fallback sibling at the requested
ordinal is typed incomplete and can never substitute for the requested hash. The
walk currently hashes only when the live current-hasher logic agrees with the
ordinal-selected JSON/Kryo logic and otherwise returns `HashEraUnavailable`; this
avoids inventing a second identity inside one recovery helper. It is not the future
Scodec migration. Before another hash-codec era activates, snapshot identity must
migrate atomically across leader, sync, KES, overlay, gossip, storage, and recovery,
using a canonical era identifier stronger than coarse `HashLogic`. Separately,
live chain-store admission still accepts caller-supplied ordinal, parent, slot, and
VRF metadata alongside the signed snapshot; deriving and checking those fields at
admission remains an active-path hardening task.

The first dark L-23 finality-durability slice has landed for eventual coordination
of the existing sinks. Its ScodecV1 ADTs/codecs, canonical identities, structural
validators, sealed initialize/prepare/recovery kernel, and checksummed
coordinator/artifact/audit/outbox store provide exact compare-and-set, durable
readback, bounded restart validation, and absorbing `RecoveryRequired`. It is not
wired to `FinalityGate`,
fork choice, GL0 consensus, MPT publication, followers, or serving, and cannot
advance `CoreApplied` or `Released` through public authority. It also does not
implement exact per-hash P0/P1 state or the optimistic K/alpha/beta cascade.

That durable head now also commits a mandatory monotone
`CanonicalLineageRevision`; initialization starts it at zero and every currently
implemented mutation preserves it. No replacement/reconstruction mutator exists
yet, so nothing live advances it. A package-only dark O-16 race kernel separately
models exact-ancestor descriptor capture, exact released-core/qualification/current-
selection/readback identity comparison, a second branch+lineage CAS, and bounded
closed-command `commitIfCurrent`. Its non-case acquisition, verified-readback, and
`CanonicalPhase2Lease` capabilities have no codec and no live caller. Focused tests
cover exact-reference substitution, malformed/current lineage commitments,
descendant selection extension, target-surviving replacement, ABA, purpose/target
reuse, missing evidence/readbacks, and sink idempotence/collision. This is not
`FIN-14` closure: current-head `/latest` policy remains absent, portable O-15/O-01
evidence and full artifact/semantic authentication remain unimplemented, and no
bounded runtime owns the two short coordinator operations. Durable sink/effect/
inverse ordering, coordinator/chain-store coupling, verified replacement advancement,
all generation-scoped invalidation adapters, and every `FinalityGate`, admission,
checkpoint, follower, serving, tracker, and publication integration remain open.

A separate dark `ShardCheckpointOutboxStore` now exercises one local opaque-byte
custody slot: bounded caller-supplied ID/bytes, store-bound expected-head CAS,
exact idempotence, write/file-force/atomic-move/directory-force/readback ordering,
restart, and fail-closed corruption. It imports no checkpoint/wire codec and has
no consensus-ID derivation, validation, release, publication, or republish
operation. This is storage-mechanism evidence only. E4 canonical signed bytes,
exact Phase-2 currentness/revalidation, multi-checkpoint anchor/orphan lifecycle,
and `SHARD-C-008` still block the production pre-publication outbox.

The accompanying pure reference model now represents generic exact-hash
canonical replacement, true-MRCA orphan/adopt ranges, Phase-2 replacement, and
the `maxvalid-tk`/`maxvalid-bg` boundary. It explicitly accepts a preselected
winner and validates only which rule tag applies; it does not implement fork
choice (`FinalityReferenceModel.scala:7-11,97-100,217-313`). The strict-density
three-cycle over structurally connected comparator inputs proves why that
boundary cannot be mistaken for a selector; full validator-backed tine validity
remains an open integration gate (`ChainSelectionSuite.scala:191-226`). This is partial dark model work under
E3.4, not fork-choice authorization. `ForkChoiceDecision` likewise carries only
an opaque `ImmutableArtifactPointer`, deliberately without a Tk/Bg or
transition-form assertion; current validation checks equality of one
intent-scoped evidence locator with that token's pointer. No
decoded header/tine/frontier/parameter-era evidence payload or verifier exists
(`FinalityCore.scala:120-138,353-364`;
`FinalityBaseCodecs.scala:96-119,178-179`;
`FinalityIntentValidator.scala:59-120,748-761`).
The objective total-frontier semantics are ratified; cutoff/bounded-diffusion and
late-reveal semantics, cycle resolution, exact `k1` metric/equality, objective tie,
evidence/verifier, validator/store witnesses, and security/liveness proof remain open.

The current dark schema validates a monotone restoration plan and claimed receipt
shape after an unreleased target is *claimed* orphaned. It does not prove that the
target stayed unpublished or that an external MPT republish occurred.
`PriorUnchanged` names the exact CAS prior; `AppliedTargetReverted` names the exact
prior image at the next publication revision. `CoordinatorHead.publication` is a
separate committed effective cursor,
so the higher restored revision survives retirement/recovery and binds the next
prepare. Terminal state cannot substitute its plan or `ForkChoiceOrphanClaim`.
Tests reject revision rewind, wrong images, plan/claim substitution, and stale
publication context, and exercise restore -> retire -> next prepare. The orphan
claim still does not prove true MRCA or target exclusion. This does not execute or
durably persist restoration: the durable store rejects every restoration mutation,
the kernel exposes no restoration authority, and semantic/anchor readbacks remain
unverified. Raw caller-supplied coordinator initialization has been removed. The
runtime-dark bootstrap now acquires a store-minted, expiring capability after exact
journal/marker/image/legacy readback, holds the MPT publication mutex through finality
head initialization or recovery CAS, and requires full publication equality. Both
durable store interfaces are sealed against a substitute mutation-capturing
implementation. A mismatch retains the exact observed publication plus its
canonically derived digest
in typed absorbing recovery. The result is deliberately `LocalPublicationBound`, not
`Ready`: it proves neither active-era MPT semantics nor canonical Phase-2 anchoring,
and public `transitionActive` can still move MPT state after the lease. This closes
the raw-initializer bypass only; live activation still requires one coordinator-owned
MPT-plus-semantic-plus-anchor transaction
(`FinalityCore.scala:395-460`; `FinalityCoordinatorState.scala:68-76,99-136`;
`FinalityIntentValidator.scala:891-925,1083-1275,1277-1288,1364-1442,1461-1608`;
`FinalityCoordinatorKernel.scala:74-112`;
`FinalityCoordinatorBootstrap.scala:9-84`;
`DurableMptImageStore.scala:87-97,177-182,542-552`;
`FinalityDurableStore.scala:293-319,1209-1230,1357-1363,1652-1656`).

This remains an open, nonactivating prerequisite. Authenticated evidence and
fork-choice authorization, a branch-revision hold through publication,
MPT-plus-semantic-plus-anchor readback authority, objective restoration, a sink
executor with readback-proven receipts and an explicit dependency DAG,
`RetentionMature` pruning authority, streaming arbitrary-depth path validation,
and a long-history audit-journal checkpoint/accumulator are still absent. The
schema represents only an already-decided `T_weight` attestation or canonical
depth-`k1` result, but does not authenticate either. It cannot decide one,
legitimize the current cumulative-weight shortcut, make `k2` a finality floor, or
add proposal/vote/lock/QC semantics to GL0.

The lanes join before activation in this order: encode O-13/O-14's ratified
directions and freeze their remaining engineering parameters, schemas, and protocol
bounds; freeze the complete root, physical key grammar, and canonical
schemas/domains; land and verify the exact field-32 replay witness and explicit ML0
population, then remove field 32 from GL0; land E9-STATE strict parsers plus
root-invisible-load normalization;
land atomic DLV and FEE kernels over one exact-parent session; resolve the field-7
proof anchor; integrate checkpoint-wide global ordering; migrate every execution
signer and watchtower; enable ordinary certified-diff adoption; then run crash,
density-reorg, deep-recovery, and `numShards=1/2/K` qualification. Work from
different lanes may merge earlier only when its active behavior is unreachable;
there is no partial economic-security activation.

### E10 - Downstream exact-hash following and Phase-2 rollback

**Depends on:** E4 and E9; ML0 rollback choice from E0.

| Task | Exit evidence |
|---|---|
| E10.1 | GL1, ML0, CL1, and DL1 follow exact P2 `(ordinal,hash,parentHash,mptRoot)` and verify replacement events. Bare monotone ordinal polling is removed from consensus-bearing alignment. |
| E10.2 | A metagraph binary's `globalSyncView` and checkpoint execution context must be exact canonical P2 before replay/sign/inclusion and obey E7.7's equality or historical-context rule. Inbound staging may remain ahead. |
| E10.3 | On P2 density reorg, downstream rolls back/rebases by the ratified rule, invalidates orphan-base checkpoints/binaries, and re-follows without duplicate effects. |
| E10.4 | CL1 adopts canonical GL0 return state; it does not replay or override the downstream result. ML0 retains local BFT authority only over its own candidate history. |
| E10.5 | Operational APIs label Phase 2 as reversible and expose exact hash/evidence. Archival APIs expose retained history/proof availability without claiming a stronger protocol phase. External consumers choose and document their own risk boundary. |
| E10.6 | ML0 reorg handling follows O-04's owner-ratified rewind/rebase/new-epoch rule. Currency-with-data applications prove deterministic retained rollback/rebase; noninvertible external effects remain an explicit integrator risk decision. |
| E10.7 | FinalityGate exposes exact Phase-2 `(ordinal,hash,parentHash,mptRoot,evidence)`. Bootstrap transport binds the selected-era complete state proof and content-addressed state payload/projection proofs to that exact reference as one bundle. A cold follower verifies the whole bundle as one identity; it never combines a finalized ordinal from one response with latest/best-tip state from another. |
| E10.8 | Close FIN-14 with the owner-ratified O-16 contract. Every authority-bearing Phase-2 consumer uses the ratified two-stage exact evidence/readback acquisition, no lock across long work, short `commitIfCurrent`, and lineage-generation-scoped cache/tally/queue/shard-buffer/inclusion/follower invalidation. `FOLLOW-008A..P` must pass; descendant extension remains live, while replacement and ABA cannot reuse old authority. No live lease issuer activates until O-16's purpose, freshness, schema, dependency, and proof gates close. |

The signed checkpoint execution reference and retained-byte verification recorded in
section 2 are an exact-state prerequisite only. They do not complete E10.2 or
E10.8 and do not close FIN-14, O-16, or protocol-test-plan P4.4: the current
reference issuer still composes an ordinal watermark with the current best branch,
the shard registry/eta/committee context is not derived from the reference's exact
historical state, and purpose, freshness, lease/CAS, and density-reorg invalidation
remain unimplemented. The dark candidate replay-view identity model does not change
that status: no production verifier derives its semantic, field-32, or rooted-value
descriptors from the retained image, and no live consumer closes effects through
`commitIfCurrent`.

### E11 - MPT-primary state, GSI deletion, and exact recovery

**Depends on:** E1/E2 state schema, E4, E7-E10 artifact formats.

| Task | Exit evidence |
|---|---|
| E11.1 | Inventory every `GlobalSnapshotInfo`/`GlobalSnapshotWithState` read, write, API DTO, recovery, and follower dependency. Build typed MPT replacements by partition. |
| E11.2 | During migration tests only, compare typed MPT results to GSI for representable states and absent/empty cases. Then delete production GSI types and fallbacks; do not keep dual authority. |
| E11.3 | Persist exact branch bytes, checkpoint inputs/diffs, finality evidence/state, undo data, nullifiers, inbox/cursors, custom commitments, and outboxes. |
| E11.4 | Live and recovery paths share verify-before-mutate artifact application. Missing bytes use authenticated hash fetch or halt, never local reconstruction or peer-state installation. |
| E11.5 | Crash injection at every write boundary and long reorg/catch-up/bootstrap reproduce exact roots and capability state. Phase-2 data retains through every configured challenge, DA, downstream acknowledgement, and recovery dependency; production capacity is tested at recommended `k2`, while a node retaining less enters authenticated recovery sooner. Expiry never becomes validity or fork-choice truth. |
| E11.6 | Production-source denylist reports zero GSI authority/fallback references. APIs project from canonical MPT and hash-bound finality state. |
| E11.7 | Followers/restart/bootstrap verify every retained signed state commitment, including `smtRoot`, against reproduced canonical state; any commitment not made load-bearing under E1.9 is removed. |
| E11.8 | Peer, persisted-disk, reorg, catch-up, and bootstrap loaders accept only the complete rooted entry set, never preserve root-invisible indices as authority, rebuild any permitted derivative from strictly key-bound canonical records, and fail the whole image on malformed/noncanonical durable MPT, tower, or SMT keys. Clean replay and every recovery path produce byte-identical typed state, roots, and next-transition results. |
| E11.9 | Bootstrap validates the complete proof selected by the canonical era at the signed ordinal. A persisted byte image, peer projection/GSI, native MPT partitions, and any follower-specific slice are accepted only when derived from or proved against the same E10.7 bundle; `mptRoot` presence is not an era selector and a failed candidate never degrades to an unverified re-encode. |
| E11.10 | One staged installer verifies the complete bundle before mutation and atomically publishes the MPT image, exact snapshot anchor, typed projections, balances/references, follower cursors, and recovery markers. Mismatch, exception, cancellation, or crash preserves the prior complete generation or resumes one durable intent; no check-after-clear/load path remains. |

### E12 - Bounded permissionless transport and bootstrap

**Depends on:** contract can start after E0/E1/E5; semantic integration after E4/E8/E11.

| Task | Exit evidence |
|---|---|
| E12.1 | Freeze and enforce one canonical artifact-descriptor and chunk contract across every Go/JVM topic/RPC/HTTP queue, message, range, cardinality, decompression ratio, concurrent verification, and disk-retention boundary before expensive work. The current 20 MiB aggregate event cut (`dag-l0.conf:21-23`; `GlobalSnapshotEventCutter.scala:41-43`) conflicts with default 1 MiB GossipSub, default 4 MiB Go/JVM gRPC, 16 MiB ChainSync (`chainsync/protocol.go:32-38`), and the local 32 MiB callback limit; raising individual push limits is not closure. |
| E12.1A | **LANDED COMPONENT CONTAINMENT, NOT EPIC CLOSURE:** `GossipStream` callback demand/bytes/items and eight generation-owned dedicated worker lanes are bounded and focused-tested. End-to-end retained bytes remain unbounded because all eight dag-l0 downstream sinks, including `stateChannelOutput`, are still `Queue.unbounded` (`Queues.scala:25-32`). |
| E12.1B | Before native DAG/allow-spend/token-lock enqueue, verify the original outer signature, signer shape, and context-free structural bounds under per-peer/topic and global budgets. This is transport/resource admission only. It never authorizes economics or replaces every GL0 validator's mandatory execution and validation of the direct `GL1 -> GL0` transition at the exact proposal parent. The sharded replay optimization applies only to ordinary noncommittee adoption of replay-certified CL1 checkpoint diffs. |
| E12.1C | **PARTIAL:** reproduced invalid Brotli, off-curve/malformed long-term identity/signature, and short/noncanonical KES inputs are bounded per-message rejections. Complete per-family fuzzing remains open. Transport source error reconnects only after accepted work drains. Unexpected worker failure fail-stops the supervised daemon with production paused and must alert operators; it does not enter the reconnect loop. Worker/process cancellation cannot expose a permanently partial authority-bearing multi-sink effect, so use one atomic commit or persisted deterministic idempotent recovery. Missing retained eta ancestry becomes typed `RecoveryRequired`, not an undifferentiated worker failure. |
| E12.2 | Gossip canonical descriptors/content commitments, then use authenticated content-addressed bounded chunk fetch and durable outboxes for snapshots, binaries, checkpoint inputs/diffs, phase evidence, fraud evidence, and DA chunks. Descriptor, complete-content hash, length, chunk index/count/hash, compression domain, and retention/retry semantics are signed or commitment-bound. |
| E12.3 | Gossip drops, duplicate suppression, sidecar/JVM restart, partition, eclipse, flood, slow peers, and shard fan-in cannot permanently lose a required artifact or cause unbounded resource use. |
| E12.4 | Peer selection/cooldown/scoring affects transport only, never consensus roots, committee derivation, evidence truth, or finality. Multi-peer recovery verifies exact hashes before mutation. |
| E12.5 | Specify validator join/exit/offline recovery, maximum safe offline duration, weak-subjectivity/bootstrap checkpoint policy, set/era/parameter proof, and light-client verification. |

### E13 - Existing-network snapshot genesis

**Depends on:** E1 manifest/serde and E2/E11 target state schema. Can be developed offline in parallel after those freeze.

| Task | Exit evidence |
|---|---|
| E13.1 | Isolated exporter verifies exact upstream v4 finalized snapshot, source network/genesis/ordinal/hash/root, and source finality evidence. No legacy decoder enters the new runtime. |
| E13.2 | Explicit transform declares imported/dropped/converted state and preserves audited balances, supply, locks, reservations, live authorizations/nullifiers, registrations, and ownership according to policy. |
| E13.2A | Define every source-signed live object's fate. Old-domain allow-spends/orders/delegations cannot become new-domain executable intents merely by import; expire/refund, preserve inertly, or require explicit new-chain reauthorization. |
| E13.3 | Canonical ScodecV1 manifest commits source evidence, transform version, new network/genesis/parameters/operator keys, output root, and replay-domain separation. |
| E13.4 | Two independent implementations/reproduction paths produce byte-identical new ordinal-0 genesis and supply report. |
| E13.5 | Cutover rehearsal covers source freeze, export publication, operator/user verification, new-chain launch, rollback/cancel procedure, and old-message replay rejection. |

### E14 - Independent qualification and release

**Depends on:** every enabled feature epic.

| Gate | Required result |
|---|---|
| Q1 Architecture | No global BFT lifecycle or ordinary-noncommittee full CL1 recreation path; producer/signers/watchtowers still replay; native GL1 remains universally executed and the target global kernel is universally executed by release; no blind state signature API; owner decisions and parameters are recorded. |
| Q2 Finality | Reference/runtime differential, partitions/restarts/eclipses, decided-attestation/depth Phase-2 qualification, density replacement beyond `k1`, and authenticated recovery with MRCA older than local `k2` pass. No age-based floor changes the winning tine. |
| Q3 Economics | Reference/production decisions and exact writes match after every prefix; conservation/replay/authorization hold for every enabled operation. |
| Q4 Execution shards | Every signer replays; ordinary adopters do not; malformed diff/root/threshold/base rejects; watchtower collusion test satisfies release rule. |
| Q5 Cross-MG | Concurrent double-consume, cancel/expiry races, acknowledgement loss, P2 reorg, and shard-count differential yield one exact result. |
| Q6 Lanes/DA | Currency and currency-with-data progress together; custom application output cannot synthesize economics; explicit signed commitment-bound fees reject on mismatch/replay; withholding/recovery/retention are bounded. |
| Q7 State/recovery | GSI production denylist is zero; complete-root and strict physical-key grammars pass; peer/disk/reorg bytes cannot inject root-invisible transition inputs; crash/reorg/catch-up/bootstrap reproduce exact typed state and roots or halt before mutation. |
| Q8 Serde/era | Cross-language vectors, strict negative corpus, ordinal-0 ScodecV1, and test-only future era transition pass. |
| Q9 Permissionless | Join/exit/unbond/slash, weak-subjectivity bootstrap, eclipse/flood/resource, and light-client tests pass. |
| Q10 Migration | Snapshot-to-genesis reproduction and conservation report pass before a public fork is attempted. |

Release requires an independent team that did not author the closing packets to
re-audit the exact commit. No waiver closes an open CRITICAL/HIGH finding.

## 5. Dependency graph and parallel work waves

```text
E0 decisions/contracts
  |
  +--> E1 serde/era/parameters --------+
  +--> E2 economic kernel -------------+--> E7 diff/result --> E8 shard protocol --+
  +--> E3 finality model --> E4 gate --+-------------------------------------------+--> E9 GL0/XMG
  +--> E5 lanes/DA --------------------+-------------------------------------------+
  +--> E6 stake/KES/slash -------------+-------------------------------------------+
                                                                                     |
E1 + E2 + E11 target schema --> E13 migration tool                                   +--> E10 followers
                                                                                     +--> E11 state/recovery
E0/E1/E5 transport contract --> E12 scaffolding -------------------------------------+--> E12 integration

all enabled epics ---------------------------------------------------------------------> E14 qualification
```

| Wave | Parallel assignments | Merge gate |
|---|---|---|
| W0 | E0 architecture/decision register, RED exploit corpus, finding ledger, test harness | All O-01..O-17 directions remain recorded as owner-ratified; dependent engineering/research/schema/parameter gates and the capability matrix are explicit and assigned. |
| W1 | E1 base serde/era, E2 pure kernel, E3 pure finality model, E5 lane/DA contract, E6 stake/evidence model, E12 transport bounds | Independent models/vectors and frozen shared interfaces pass; no runtime switch. |
| W2A | E4 durable finality/tower core, E5 DA runtime, E6 runtime primitives; E7 pure diff/root/codecs only against frozen interfaces | Hash-bound Phase-2, density/deep-recovery, tower, and identity/lane interfaces freeze; pure diff vectors pass without premature GL0 integration. |
| W2B | E4 atomic integration hooks and exact Phase-2 reference/evidence API, then E7 checkpoint execution/result integration | Finality commands/events and the bootstrap-bundle identity contract are load-bearing before checkpoint or follower base/anchor code merges. |
| W3A | E8 staircase/replay/watchtower/adjudication | Committee/noncommittee execution counts, malformed diff, single-outstanding multi-MG batching, no shard-depth fallback, collusion, quarantine, and fraud-verifier tests pass. |
| W3B | E9 allow-spend-only global settlement after E8 interfaces pass | Exact-once, proposal-parent compare-and-set, mirror/overlay, and acknowledgement tests pass. |
| W4A | E10 downstream reorg, E11 module-by-module MPT/GSI removal plus transactional state installer, E12 exact-bundle semantic transport | `BOOT-001..005`, `REC-005`, full P2 reorg/restart/catch-up, and the GSI denylist pass before the target state schema freezes. |
| W4B | E13 offline migration against the frozen E11 target schema | Independent genesis transform/root/conservation vectors pass. |
| W5 | E9 remaining explicitly designed interaction types, permissionless/light-client integration | Every enabled type and public lifecycle has an oracle and fault test. |
| W6 | E14 independent qualification | All gates pass on the exact release commit. |

Safe early parallelism is model/schema/test work behind frozen interfaces. Shared
hotspots such as `GlobalSnapshotAcceptanceManager`, `GlobalSnapshotConsensus`,
`SnapshotLeaderLoop`, `FinalityGate`, checkpoint schemas, and MPT key derivation
have one integration owner per wave. Agents do not independently rewrite them.

## 6. Initial delegable packets

| Packet | Write scope | Starts | Exit artifact |
|---|---|---|---|
| P0 owner decisions/architecture guard | docs, finding ledger, source denylist test | now | Ratified decision record and CI guard against global BFT or ordinary-noncommittee sharded-CL1 replay drift, while preserving universal native GL1 execution and the target universal global-kernel requirement. |
| P1 Scodec manifest/negative corpus | shared serde/codec tests only | E0 vocabulary | Frozen vectors and strict decoder RED/GREEN corpus. |
| P2 economic reference interpreter | pure shared model/tests | E0 grammar | Prefix differential oracle covering all enabled operations. |
| PF finality reference model | pure model/checker/tests | E0 ratified finality contract | Phase/cascade/density counterexample report and vectors. |
| P4 Operator registry/stake/KES/evidence | shared schemas, historical resolver, crypto/evidence validators, and owned integration | P1 identity primitives; implement O-11's ratified roster schema/parameters/proofs before runtime eligibility | Rooted delayed population/key/stake binding, rotation, backing, false-slash, and exact-debit model. |
| P5 Lane/DA | signed envelope/registration/content commitment and bounded fetch | P0 ratified lane contract, P1 | Currency and currency-with-data contract plus withholding/recovery tests. |
| P6 Durable FinalityGate/tower | finality/store, tower/SMT, owned GL0 integration, and dark FIN-14 lease kernel under O-16's ratified direction; live issuance waits for its engineering gates | P1/PF; economic release hooks after P2 oracle | Crash-consistent hash-bound Phase 2, density/deep recovery, consensus-reproduced `smtRoot`, portable tower proofs, and owner-ratified exact-consumer authority. |
| P7 Diff/root/checkpoint | shared checkpoint schemas, pure diff/root model, complete-root ownership, field-32 witness, and strict physical-entry interface | P1/P2/P4/P5/P6 exact refs | Complete root/write-set proof, ROOT-008/010/011 interfaces, and canonical checkpoint vectors. |
| P8 Shard execution/watchtower | sharding runtime plus owned GSAM adapter | P7 | Replay-before-sign, positive assigned replay coverage, challenge adjudication, and zero-replay ordinary adoption. |
| PSIG Layer signing boundaries | GL0 optimistic, ML0 state-validity, and GL1 state-validity signing adapters; P8 retains shard execution/coverage ownership | P1 identity codecs plus each layer's frozen validator/oracle | Compile-negative signing APIs and runtime RED tests prove every state-validity signature is reachable only from that layer's locally reproduced result. |
| P9 Cross-metagraph settlement | pure global kernel plus owned GL0 integration | P2/P6/P8 and P7 strict-entry interface | Allow-spend exact-once across shards/reorg/restart. |
| P10 Followers/MPT/GSI | one module slice per agent behind P7 strict MPT and P6 finality APIs | P7 strict-entry interface; semantic owners P2/P4/P8/P9 | Exact-key reconstruction, exact-hash reorg tests, and module-specific GSI removal. |
| P11 Transport/recovery/bootstrap | sidecar/JVM transport, exact Phase-2 bundles, and transactional state installation | bundle contract after P6; state semantics after P7-P10 | Bounded transport, authenticated all-or-none recovery, and serving/bootstrap parity. |
| P12 Snapshot-genesis migration | isolated offline tooling/tests | P1/P2/frozen target schema | Reproducible upstream-snapshot-to-ScodecV1-genesis manifest/root/conservation report. |
| P13 Independent qualification | black-box/model/source audit only | closing candidate | Release verdict and artifact bundle. |

Every packet begins with a write-set manifest and named test IDs from the test
plan. A shared hotspot change is queued through its integration owner.

## 7. Explicit non-goals and retired directions

- No global partially synchronous BFT proposal/vote/lock/QC/view-change protocol.
- No universal full CL1 recreation by ordinary GL0 adopters.
- No blind committee signing for availability/chain agreement under an execution
  signature type.
- No ML0 `authoritative*` fields or `AdoptFromSignedFields`.
- No direct shard-to-shard economic receipts in v1.
- No stake-weighted secret-VRF claim for the current execution-shard draw.
- No fork-only compatibility schema for undeployed post-v4 work.
- No general opaque/data-only lane beyond O-09's locked authenticated DA-only carriage; it has no framework-economic authority.
- No GSI as consensus or recovery authority in the target.
- No claim that slashing after value exits is sufficient.
- No claim that Phase 2 is irreversible or that ordinal-only finality is adequate.
- No claim that `k2`, local pruning, or a retention watermark is an immutable
  consensus-finality floor.

## 8. Plan integrity audit

| Prior claim | Verdict at baseline | Roadmap disposition |
|---|---|---|
| GL0 Nakamoto loop replaced global BFT rounds | **Source-supported active path.** The worktree also removes the dormant inherited GL0 event loop and six BFT handlers; the focused regression is green. A dormant generic `Consensus` storage/routes compatibility shell remains. | E4.8 finishes compatibility/config/API deletion; ML0 BFT stays. |
| Avalanche optimistic finality implemented | **False.** Query cascade/alpha are absent; the named accumulator is an arrival-order-sensitive latest-color margin, and the state-changing path is separate legacy aggregation. | E3/E4 plus FIN-M/FIN-S. |
| Finality phases are implemented by `FinalityGate` | **False.** Gate is an ordinal serving facade; Phase 0/1/2 state is not hash-bound and tower/SMT evidence is not consensus-reproduced. | E3/E4. |
| Phase 2 cannot reorg | **Rejected owner model.** Phase 2 remains density-reorgable; current density code is default-off, imposes an age floor, and sinks are incomplete. | E3.4/E4.5/E10. |
| `k2` is an immutable finality floor | **Rejected owner model.** `k2` is recommended retention/recovery capacity only. | E3.9/E4.5/E11.5. |
| Tower/NiPoPoW eligibility is enabled | **False.** Active-era `smtRoot=None` and exact comparison are enforced by `GlobalSnapshotActiveEraValidator.scala:10-30` and `StateProofComparison.scala:28-37,45-78`; production leaves historical SMT and provider wiring absent (`GlobalSnapshotConsensus.scala:715-717,1446-1456`), and routes return 503 (`NipopowRoutes.scala:82-87`; `NipopowRoutesSuite.scala:101-120,237-242`). The verifier deliberately reports historical eligibility unavailable without exact branch-historical registry/roster/stake/eta witnesses (`TowerVerifier.scala:343-360`). | E1.9/E4.10-E4.13; preserve the dark cut until STOR-02/03, ROOT-005B, TOWER-001..007, and REC-004 pass. |
| Execution committee is stake-weighted secret VRF | **False for current execution shards.** Membership is public deterministic VK-hash with uniform weight; possession VRF is separate. | E6.3/E6.4. |
| Staircase shard duty exists | **Source-supported**, five-slot normal windows and widened genesis window. The worktree rejects a wire epoch inconsistent with the signed anchor ordinal, but exact hash-bound Phase-2 ancestry and canonical R remain open. | E6.3/E8.2. |
| Every execution signer replays | **Type-enforced for the current root-recreation checkpoint.** The emitter accepts only `VerifiedShardCheckpoint`; future diff/intents/complete-root parity and all signing surfaces still require closure. | E7/E8.3 plus SIG/SHARD-E tests. |
| Committee byte diff adoption is active | **False after `c610a0740`.** Diff was removed and ordinary noncommittee GL0 replay of sharded CL1 checkpoints was introduced. Universal GL0 execution of native GL1/DAG-token transitions is unchanged. | E7/E8/E9 selective forward repair. |
| Removing `authoritative*` is complete economic enforcement | **False.** Those overrides are correctly deleted, but authorization/conservation/replay defects remain and the useful diff was conflated with them. | E2 and E7.2. |
| `numShards=1` has equivalent security | **False today.** Committee/watchtower paths are gated off. | E8.9 and SHARD-C-005. |
| Cross-shard reads are finality-first | **Partly source-supported**, but current finality is ordinal-only/unsafe and global conflict semantics are incomplete. | E4/E9/E10. |
| Watchtower slashing closes collusion | **Unproven.** Replay/evidence scaffold exists; selection, false-slash, bonded debit, and release timing remain blockers. | E6/E8 and WT tests. |
| Scodec migration complete | **False.** MPT codecs exist; consensus hashing/signing and era dispatch still use legacy paths. | E1. |
| GSI removed | **False.** Production state/follower/API/recovery references remain. | E11. |
| Hard-fork migration no longer needed | **False product conclusion.** No live legacy mode is needed, but the requested source-snapshot-to-new-genesis tool does not exist. | E13. |
| Mempool reinsertion complete | **Not source-verified.** Historical TODO has no reliable closing evidence. | E4.5/E8.6/E10.3 and explicit requeue tests. |
| Two-level finality complete | **False.** ML0, shard execution/chain, and GL0 phases are not yet integrated under the exact lifecycle. | E4/E8/E10. |

Historical checkmarks in `NAKAMOTO-TODO.md` and `NAKAMOTO-PLAN.md` are not release
evidence. The current audit, this table, and the machine-readable E0 ledger control.

## 9. Consensus determinism ledger

Each item is forbidden from affecting accepted IDs, ordering, a diff/root,
committee/sample membership, phase, fork choice, slash verdict, or recovery result
unless converted to the named canonical input.

| Node-local/asymmetric input | Current exposure or risk | Required replacement |
|---|---|---|
| Wall clock / local genesis fallback | Slot/expiry/activation can diverge; config still describes a local-clock genesis fallback. | Genesis/finalized protocol time and exact successor/slot evidence only. |
| Signed/transport `parentSlot` not checked against the exact retained parent | A producer inflates the LDD gap and turns a losing VRF trial into an accepted leader win. | Derive the gap from the exact retained parent certificate and run the same pure check before producer signing and follower storage. |
| Missing, partial, or wrong-branch eta-source ancestry | Typed range checks defer on incomplete/empty history, and GL0 GSAM/admission use exact parents. `ShardCheckpoint` now signs an exact execution-state reference, but shard committee/producer/attester and fraud-certificate paths still obtain eta, registry, roster, and parameters from ambient callbacks/current configuration instead of proving them from that historical reference. Identical bytes can therefore still draw, verify, or slash differently across sibling-local views. | First authenticate the reference as exact Phase 2, then use its branch-historical resolver at every shard and evidence consumer; missing/partial history fetches/defers and cannot slash, while a genuinely complete empty interval follows one explicit canonical rule rather than an absence fallback. |
| Current/live stake substituted for N-2 stake | Producer and verifier use different LDD thresholds after stake changes; a newly enlarged attacker can pass only the live threshold. | One parent-ordinal-derived N-2 period and exact hash-bound historical distribution on both sign and verify paths; missing history defers. |
| Sender-carried GL0 leader VRF key | Period-zero producer/receiver paths now reject a replacement key, but runtime consumers still lack exact-parent N-2 resolution. | Resolve the atomic KES+VRF pair from the exact branch's N-2 view and intersect it with the delayed authorized roster/stake population; carried key only compares and missing/mismatch rejects. |
| Local best tip or pending branch used as a “finalized” read | Same operation can see different owner/base state. | Exact canonical P2 `(ordinal,hash,root)` for origins/bases; exact proposal parent for branch-local status. |
| Live mutable store versus pinned store | The checkpoint execution-prior slice now signs the exact state reference and exposes replay state only after retained bytes reproduce its committed root; producer stability re-read and receiver/watchtower replay share that path. Other candidate-scoped message, SpendAction, replay-witness, and exact Phase-2 authority inputs remain outside this closure. | Route every replay input through one exact-reference candidate session; missing bytes/history defer, never fall back to live state or become slash evidence. |
| Peer selection, cooldown, response order, timeout | Network success/order can change bytes or slash verdict. | Transport obtains exact hash-addressed bytes only; consensus defers on absence. |
| Locally observed active set / renormalized stake | Avalanche/finality/committee denominators diverge under partition. | Delayed canonical registry/stake state, modeled as epoch `N-2` registry and epoch `N-1` eta for epoch `N`, with exact anchor/hash. |
| Wire-carried checkpoint epoch | Worktree rejects an epoch inconsistent with the signed anchor ordinal, but an attacker can still choose an older admissible ordinal and its matching favorable committee; local R can also split validity. | Derive the anchor from exact proposal-parent hash-bound Phase-2 evidence/freshness, then recompute epoch/eta/roster using a canonical parameter hash. |
| Local eta-period length `R` | The same signed anchor ordinal maps to different execution epochs and rosters. | Canonical network/genesis/era parameter object/hash; local mismatch halts before validation. |
| Local `staircaseDeltaSlots` | The same signed checkpoint, parent, and roster schedule different producers. | Proposal-parent-bound canonical parameter object/hash; local mismatch halts before validation. |
| Local HOCON/env override | Validators can apply different k/era/shard/limit semantics. | Canonical parameter hash from genesis/finalized state; local mismatch halts. |
| `Double`/platform rounding | `R = round(3.1d*k1)` is not a canonical cross-language formula. | Exact bounded integer/rational derivation and golden vectors. |
| Map/set/hash iteration | Input order, diff bytes, identities, or roots can vary. | Sorted canonical collections and explicit total operation order. |
| Gossip/queue/shard arrival order | Competing operations/checkpoints can select different winners. | Parent-contiguous buffering plus canonical GL0 merge order. |
| Thread scheduling/chunking/parallel execution | Same batch can observe partial/interleaved state. | Pure ordered accumulator and atomic write set; parallelism only behind deterministic join. |
| JSON/Kryo/decoder probing | Multiple byte encodings or decoder choices change hashes/lanes. | One strict Scodec era and explicit signed lane/type. |
| GSI/local caches/reconstructed projections | Restart or cache history can invent a different state view. | Typed canonical MPT plus exact journals; cache is non-authoritative. |
| Missing-input sentinels such as `Hash.empty` | A “cannot derive” result can be confused with a real mismatch. | Typed `Verified` / `Mismatch` / `Unavailable`; unavailable defers and cannot slash. |
| Local watchtower/admission participation | Optional observers can change validity or release asymmetrically. | Canonical assignment and explicit threshold/deadline/release rule. |
| P2 ordinal without hash | Same-ordinal density replacement is invisible. | Hash-bound phase references and reorg events. |

The E0 ledger expands this table with every concrete source read and its owning
test. A code search alone is insufficient; callers must be traced to the signed
root/phase/slash sink.

## 10. Immediate next order

1. Propagate the locked decisions and remaining open gates into ADR-0016/0017,
   the artifact lifecycle, test plan, and architecture drift guard.
2. Close the applicable engineering/research/parameter/proof gates, including O-15 through O-17;
   runtime work must implement the ratified directions without inventing constants or schemas. L-23 already fixes the exact-parent concurrency, bounded
   retention/recovery, and cross-sink durability strategy, but its runtime gates
   remain open.
3. Run the named decisions/RED-ledger, serde-vector, economic-oracle,
   finality-model, stake/evidence-model, and transport-RED workstreams in
   parallel. Packet numbers and ownership are identical to the protocol test plan;
   that test plan remains the delegation contract.
4. Freeze the complete root/write set, GL0 correction shape, and checkpoint
   preimage in P7 after O-17's ratified structural directions and the dependent
   economic engineering gates are encoded and proved.
5. Implement P6 hash-bound Phase-2, density/deep recovery, and tower/SMT
   reproduction before any checkpoint hard-anchor or cross-metagraph integration.
6. Implement P8 replay-before-sign/diff adoption, then P9 allow-spend-only global
   settlement.
7. Land follower rollback and MPT/GSI removal slices, then recovery/transport.
8. Keep remaining interaction types disabled until their E9.8 contracts exist.
9. Perform E14 qualification before any economic/public testnet.
