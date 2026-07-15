# Consensus Protocol Test and Delegation Plan

**Status:** Required target plan; most tests do not exist yet
**Code baseline:** `c610a0740c34833e563f8a94c2ab820186a75897`
**Architecture:** `CONSENSUS-ARTIFACT-LIFECYCLE.md`
**Roadmap:** `CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md`

The current test suite is a regression baseline, not a proof of the target. Some
tests encode universal GL0 recreation, ordinal-only finality, legacy attestation
aggregation, or undeclared payload behavior. Such tests must first become RED
counterexamples or be rewritten against the ratified lifecycle.

## 1. Security-closing packet contract

Every packet that closes a consensus/economic finding delivers:

1. a ledger row with invariant/finding ID, baseline commit, owner, write scope,
   test IDs, status, and closing commit;
2. a reproducible RED exploit/counterexample on the named baseline;
3. an independent oracle: pure reference interpreter, model checker, golden vector,
   cross-language codec, or black-box state checker;
4. unit/property tests plus every applicable restart, reorg, partition, and
   multi-node fault scenario;
5. deterministic seeds and checked-in bounded fixtures;
6. a source re-audit of every sign/adopt/finalize/serve/recover path using the
   repaired primitive;
7. exact commands and artifact hashes in the handoff.

A changed expected root is not a conservation proof. A local counter test is not
an Avalanche/finality proof. A signature-count test is not replay-before-sign. A
watchtower log after value escaped is not an economic-security pass.

## 2. Shared contracts frozen before parallel runtime work

| Contract | Required artifact |
|---|---|
| Architecture vocabulary | Layer/committee map; P0/P1/P2 plus orthogonal retention maturity; separate execution and chain status; only the chain-based GL0 lifecycle. |
| Owner decisions | Ratified optimistic/depth algebra, execution threshold, positive watchtower release, root fields, ML0 reorg rule, one-checkpoint batching, lane scope, and explicit remaining numeric/resource gates. |
| Signature domains | Distinct ML0 source, admission, execution, positive watchtower replay coverage, optimistic, DA custody, KES producer, and fraud-evidence preimages with exact semantic claims. |
| Economic grammar | Closed framework operation/intents, authorization, order, conservation, rejection, replay IDs, and pure oracle interface. |
| Finality model | Hash-bound phase transition and density model; `k2` cannot become a fork-choice floor. |
| Checkpoint result | Exact Phase-2 base, input commitment, complete root/write scope, canonical diff, extracted global intents, exact optional framework replay witnesses (including field-32 view preimages and explicit ML0 operator populations), execution threshold, separately domain-separated positive replay coverage, and challenge material. |
| State/storage | Complete root ownership; strict physical-key/value grammar; nonauthoritative derived-index policy; finality journal, branch undo, checkpoint/diff/input retention, nullifier/inbox/outbox records, recovery normalization, and atomicity contract. |
| Serde/era | Scodec manifest, strict decoder rules, ordinal-0 genesis, future hash-bound era activation, and migration-manifest codec. |
| Parameters/resources | Canonical parameter hash and hard protocol caps; no consensus-affecting local override. |
| Transport/DA | Authenticated bounded publish/fetch/chunk/retention interfaces and recovery semantics. |

Central schemas, `FinalityGate`, `SnapshotLeaderLoop`,
`GlobalSnapshotAcceptanceManager`, `GlobalSnapshotConsensus`, MPT key derivation,
and checkpoint codecs have one integration owner per wave. Other agents work behind
frozen interfaces.

## 3. Work packets

| Packet | Primary ownership | Starts after | Exit artifact |
|---|---|---|---|
| P0 Architecture/decision/RED ledger | docs, drift guards, finding ledger | immediately | Ratified owner answers, forbidden-pattern guard, and every CRITICAL/HIGH assigned. |
| P1 Canonical serde/era | shared codecs/hash/signature schemas and tests | P0 vocabulary | Strict ScodecV1 corpus, cross-language vectors, genesis and future-era harness. |
| P2 Economic oracle/kernel | pure shared interpreter plus production adapter tests | P0 grammar | Prefix-by-prefix decision/write/conservation differential. |
| PF Finality model | pure phase/Avalanche/density model and checker | P0 ratified finality contract | Reference transcripts, counterexample report, and parameterized assumptions. |
| P4 Operator registry/stake/KES/evidence | immutable-genesis population cut; unified operator-key record; historical activation, roster, backing, committee, crypto, and evidence schemas/tests | P1 identity primitives; implement O-11's ratified roster schema/parameters/proofs before runtime eligibility; runtime branch lookup after P6 exact-ref interface; checkpoint migration after P7 exact refs | Rooted genesis pair/population binding, N-2 period-index rotation on an exact branch, cross-consumer key parity, backing, false-slash, exact-debit, and anchored-roster tests. Existing atomic registry/genesis commitment/resolver code is only a partial artifact. |
| P5 Lane/DA | envelope/registration/content commitment and bounded fetch | O-09 ratified lane scope, P1 | Currency, currency-with-data, and retained DA-only opaque-lane contracts plus withholding/recovery and decoder-promotion tests. |
| P6 Durable FinalityGate/tower | node-shared finality store, tower/SMT, owned GL0 integration, and a dark FIN-14 lease kernel under O-16's ratified direction | P1/PF; economic release hooks after P2 oracle; O-16 engineering gates before consumer API activation | Crash-consistent operational phase, load-bearing tower/SMT, arbitrary-depth density recovery, exact event outbox, and owner-ratified exact-consumer authority. |
| P7 Diff/root/checkpoint | shared checkpoint schema/diff/root model/codecs plus complete-root ownership, field-32 replay-witness contract, and strict physical-entry interface | P1/P2/P4/P5/P6 refs | ROOT-007 complete coverage, ROOT-008 strict key grammar, ROOT-010 exact replay witness/removal order, and exact apply-diff vectors. |
| P8 Shard execution/watchtower | sharding runtime plus owned GSAM adapter | P7 | Replay-before-sign, distinct positive assigned-watchtower coverage before inclusion, zero-replay ordinary adoption, and staircase/anchor/challenge tests. |
| PSIG Layer signing boundaries | GL0 optimistic, ML0 state-validity, and GL1 state-validity signing adapters; P8 retains shard execution/coverage ownership | P1 identity codecs plus each layer's frozen validator/oracle | Compile-negative signing APIs and runtime RED tests prove each signature is reachable only from that layer's locally reproduced result. |
| P9 Cross-metagraph settlement | global pure kernel plus owned GL0 integration; consumed-nullifier parser migration | P2/P6/P8 and P7 strict-entry interface | Allow-spend exact-once across shards, physical-nullifier binding, reorg, restart, and acknowledgement. |
| P10 Followers/MPT/GSI | one layer/module slice per agent; Mg, token-lock, stake/collateral, slash/cooldown, price/parameter strict-parser migrations | P7 strict-entry interface; semantic owners P2/P4/P8/P9 | Exact-key typed reconstruction, exact-hash rollback, and zero GSI/root-invisible authority or fallback. |
| P11 Transport/recovery/bootstrap | sidecar/JVM boundary, exact Phase-2 bootstrap bundle transport, root-invisible-load normalization, post-removal field-32 ingress denial, strict durable tower/SMT enumeration, and transactional exact-state installation | bundle contract after P6; state semantics after P7-P10 | BOOT-001..005, ROOT-009/010, and REC-004/005 recovery parity plus bounded adversarial transport and exact crash/catch-up/bootstrap. |
| P12 Snapshot-genesis migration | isolated offline tooling | P1/P2/target state schema | Reproducible upstream-snapshot to ScodecV1 genesis and conservation report. |
| P13 Independent qualification | black-box/model/source audit only | closing candidate | Release verdict for the exact commit. |

### P4 preregistration merge cuts

P4 is delivered as ordered cuts, not one broad registry merge. The current
worktree has an atomic paired-key registry, rooted long-term-signed genesis key
records, durable runtime records, an N-2 historical resolver, and a tested
exact-hash hot-chain view adapter. It does **not** yet construct that adapter as
production consensus authority, root a runtime operator roster, wire runtime
consumers, or complete checkpoint/tower/watchtower/fraud integration. The runtime
cert also lacks the target explicit network/genesis/era domain and containing
hash/root witness; current acceptance does not reject same-owner reuse or a
record in which only one key changes. Atomicity requires every activation to
select both keys from one preregistered record. Canonical duplicate and partial-
rotation semantics, codecs, and proofs remain engineering freeze gates. Runtime
records therefore remain ineligible. `StakeRegistry`/`SharedServices` still admit
receiver-local seedlist/current-state population inputs. O-11 ratifies the atomic
population-plus-raw-weight boundary and pledge/self-bond/delegation structure,
but its exact schema, authorization predicate, and per-network values remain open;
registration, positive stake, and injected test rosters remain insufficient.

| Cut | Deliverable | Starts after | RED/acceptance gate |
|---|---|---|---|
| P4.0 Atomic authority inventory (`PARTIAL`: atomic API and narrow textual tripwire landed; complete consumer manifest open) | One paired KES+VRF API; transport keys are evidence only; split mutable registries and direct-key consumers denied by inventory. Read-only KES/VRF projections may derive only from the atomic pair. The landed tripwire scans split factories, raw primitive references, and direct pair constructors only; it does not discover every higher-level sortition/eligibility/duty/proof/verifier call. | P1 identity bytes. | `KEYREG-001`, `KEYREG-005`, `KEYREG-006`, `KEYREG-011`; checked semantic inventory has zero unapproved production key/sortition call sites and every allowlisted path passes branch-historical vectors. |
| P4.1 Immutable-genesis safety cut (`PARTIAL`: pair root landed; population open) | Complete paired records plus a separately authorized immutable genesis operator/stake population committed by the canonical genesis identity; startup and local-secret checks fail closed. | P4.0. | `KEYREG-001`, `KEYREG-005`, `KEYREG-011`, `KEYREG-012`, genesis branch of `KEYREG-013`; mutated JSON, duplicate ownership, omitted record/population, and restart without witness fail before Ready. |
| P4.2 Permissionless roster contract (`OWNER DIRECTION RATIFIED / ENGINEERING GATES OPEN`) | Canonical Sybil-resistance/membership rule with the ratified pledge/self-bond, proportional delegation slashing, and `bond >= extractable value` structure plus activation, exit/unbond, slash/cooldown, immutable-genesis population, retention, and recovery semantics. Encode the ratified single boundary value whose sorted population keys and raw weights are authenticated together; freeze only after its exact predicate, values, codec, and proofs pass. Registration cannot create membership. | O-11 ratified direction; P1 codecs/root ownership and parameter derivation. | `KEYREG-013`, `PERM-*`, `PARAM-001`; self-signed registered outsider, key-only/stake-only/profile-only entry, live-peer/seedlist substitution, stake splitting, early unbond, mixed-branch roster/weight, and receiver-local population are RED. |
| P4.3 Exact historical activation (`PARTIAL MODEL`, runtime blocked) | Authenticated exact-parent `(ordinal,hash,parentHash,mptRoot)` view; period `N` uses the `N-2` pair/roster/stake view and `N-1` eta; paired activation/rotation unwinds and refolds with density reorgs. | P4.2 and P6 exact-parent branch interface. | `KEYREG-002..005`, `KEYREG-007`, `KEYREG-008`, `CRYPTO-001`; same ordinal with sibling hash, correct claimed hash with sibling registry/root bytes, live-head fallback, and missing history reject/defer before side effects. |
| P4.4 Exact Phase-2 artifact references | Currency binary and checkpoint base/anchor schemas bind exact `(ordinal,hash,parentHash,mptRoot)` Phase-2 evidence used for registry, eta, roster, and stake resolution. | P4.3; P6 phase proof; P7 schemas. | `KEYREG-006`, `KEYREG-007`, `KEYREG-010`, `SHARD-E-003A`, `SHARD-C-001B`, `FOLLOW-001`; same ordinal/wrong hash or root fails producer, signer, acceptance, reorg, and adjudication paths. |
| P4.5 Consumer and evidence migration | GL0 producer/verifier/sync, admission, execution membership/duty/producer/signer/acceptance, watchtower, optimistic sampler, tower, and slashing all use the same branch-historical resolver and derive the exact KES step. | P4.3/P4.4 and each consumer's typed schema. | `KEYREG-006`, `KEYREG-009`, `KEYREG-010`, `FIN-B-004`, `ADMIT-003`, `SHARD-S-003`, `TOWER-004/005`, subsystem reorg and false-slash suites. |
| P4.6 Fixture migration and release guard (`PARTIAL`: committee/leader/shard-sortition fixture migration and textual tripwire landed; complete semantic qualification open) | Every consensus fixture commits the complete genesis pair/population or registers the pair in the exact canonical N-2 view before use; arbitrary keys remain only in low-level crypto implementation tests. `CommitteeSortitionSuite`, `CommitteeShardSortitionSuite`, and `EligibilityCheckerSuite` now use loader-validated period-zero pairs, including registered wrong-key controls and repeated canonical-input statistical draws. | P4.5 APIs frozen; may migrate independent fixture families earlier. | `KEYREG-006`, `KEYREG-011`; generated-unregistered key fails before every draw/proof/sign/attest/store/adopt/slash side effect and the inventory artifact reports zero exceptions without an explicit primitive rationale. |

The hard dependency is O-11's owner-ratified structure -> P1 canonical
population/root codec and parameter/proof gates ->
P4.1 immutable genesis population closure -> P4.2 pure boundary fold plus
undo/refold -> P4.3 exact-parent N-2 population/key and N-1 eta capability -> P4.4
exact Phase-2 artifact refs -> P4.5 consumer/evidence migration -> P4.6 fixtures and
qualification. The paired-key portion of P4.1 may progress independently, but no
genesis or runtime population becomes authority by being inferred from its key
records, seedlist, positive stake, or a test-injected roster.

### 3.1 Lifecycle stage gates

Each stage is a merge gate, not a percentage-complete label. Its named tests must
pass against one exact commit and the independent checker must archive the
manifest, seed/fault schedule, outputs, and artifact hashes.

| Stage | Deliverable | Starts after | Acceptance gate |
|---|---|---|---|
| S0 Contracts and RED baseline | Frozen exact-ref, signature-domain, lane, diff/root, registry, resource, and correction schemas; reproduced current exploits. | Immediately. | Every shared type has one owner and version; every CRITICAL/HIGH finding has a RED test and packet. |
| S1 Pure reference models | Economic interpreter, P0/P1/P2 + density model, committee/tower model, canonical codecs, and operator-key activation/rotation model. | S0 contracts. | Production-independent model/golden/property suites are deterministic across seeds/platforms; unresolved numeric parameters are explicit inputs; every key consumer agrees on the same branch-historical active record. A model-supplied roster is not evidence that O-11 or its production root exists. |
| S2 Hash-bound GL0 phase/tower core | Durable `FinalityGate`, branch journal, authenticated exact-parent operator-key/roster view, verified `smtRoot`, tower eligibility/proofs, `RecoveryRequired`. | S1 finality/codec/crypto models; implemented O-11 roster schema/parameters/proofs for runtime eligibility. | Exact-hash P2 triggers, preregistered-key eligibility, same-ordinal wrong hash/root rejection, arbitrary-root/vacuous-proof rejection, restart equality, and density replacement beyond local `k2` all pass before any economic consumer uses P2. Genesis-only qualification additionally requires the separately rooted immutable genesis population. |
| S3 Binary intake and DA | Source validation, exact parent/ordinal resolution, durable custody, admission receipts, bounded fetch. | S0 domains/lanes; S1 identity codecs. | Receipt is emitted only after validation and storage, cannot satisfy execution, survives restart/reorg, and cannot be biased with a self-claimed ordinal/eta. |
| S4 One-checkpoint execution | One outstanding checkpoint per shard containing multiple ordered per-MG segments, complete canonical diff/root, exact framework replay witnesses, and replay-backed execution signatures. | S1 economic oracle; S2 P2 refs; S3 inputs. | Producer and every signer reproduce exact output from the same optional sync-view preimages and ML0 populations; `kQuorum-1` and every tamper reject; no second outstanding checkpoint can be built. |
| S5 Watchtower release | Deterministic noncommittee assignments, capability-backed domain-separated positive replay coverage, bounded challenge/adjudication. | S4 checkpoint. | No checkpoint is GL0-eligible without minimum positive coverage; an authenticated mismatch quarantines regardless of positive count; false positive/negative, unavailable-data, partition, flood, and restart suites pass. |
| S6 Ordinary GL0 adoption | Execution and positive-coverage certificate/diff/base/CAS/root verification and atomic namespace-confined application. | S4/S5. | Valid certificates and diff adopt with zero framework recreation; invalid assignment, duplicate/nonmember coverage, bad domain/signature/threshold, and every malformed/stale/cross-namespace diff leave state unchanged. |
| S7 Global settlement | Universal intent ordering, nullifier, settlement overlay, delivery/ack protocol. | S1 economic oracle; S2 P2 refs; S6 adoption. | Concurrent cross-shard consumes yield one deterministic winner and exact conservation at shard counts 1, 2, and K. |
| S8 Reorg/follower recovery | Atomic density unwind/refold, binary/checkpoint requeue, ML0 rebase/new epoch, downstream exact-hash replacement. | S2 and S6/S7 state paths. | Reorg at every depth and crash point converges byte-identically; missing data beyond local retention enters verified recovery rather than selecting by local truncation. |
| S9 GL0 protocol correction | Active-era correction grammar, deterministic execution, rooted state, downstream mandatory rebase. | S1 economic/root models; S2 phase events; S8 rebase. | Only active protocol rules can correct exact pre-state; metagraph/local authority, stale base, duplicate, conservation failure, and reorg mishandling reject. |
| S10 Permissionless qualification | Multi-node transport, eclipse/partition/restart/long-offline, resource and performance envelope, source re-audit. | S0-S9. | Independent agent reproduces every enabled suite on the release commit with no open unowned CRITICAL/HIGH finding. |

### 3.2 Required test modes

Every runtime stage supplies all applicable modes; omission requires a written
non-applicability proof in its manifest.

| Mode | Minimum artifact |
|---|---|
| Deterministic | Unit/golden-vector tests over exact bytes, hashes, roots, ordering, and transition results. |
| Property/model | Generated traces compared prefix-by-prefix with an independent pure model, with minimized counterexamples and fixed replay seeds. |
| Adversarial | RED exploit plus malformed, equivocation, replay, grinding, wrong-base/root, and resource-bound cases. |
| Integration | At least three independently configured processes exercising the real store, sidecar, restart, and exact-hash recovery paths. |
| Chaos | Named partition, eclipse, delay, duplication, drop, crash-at-write, disk-loss, and rejoin schedules with invariant checking after every event. |
| Performance | Worst-valid and one-unit-over CPU, heap, disk, network, proof-size, replay, and recovery benchmarks with hard protocol caps and regression budgets. |

## 4. Test catalog

### 4.1 Architecture and signature semantics

| Test ID | Required scenario and assertion |
|---|---|
| ARCH-001 | Production GL0 sources and target docs contain only the Nakamoto/Taktikos/LDD chain lifecycle plus the hash-bound optimistic/depth phase gadget. Any inherited alternate global round engine is absent or unreachable by construction. |
| ARCH-001A | GL0 startup constructs/supervises exactly `SnapshotLeaderLoop` and the hash-bound phase gadget. Inherited generic global event handlers/routes cannot be configured, triggered, or reached; ML0 startup remains green. |
| ARCH-002 | Topology integration proves GL1->GL0 and CL1/DL1->ML0->GL0, with exact P2 state returning downstream. Execution shards do not become an application layer. |
| ARCH-003 | ML0 source committee, GL0 admission committee, and GL0 execution committee are independently derived/verified and cannot substitute signatures. |
| SIG-001 | Every state-validity signing entry point requires a `LocallyExecuted`/`Verified*` capability. No public/runtime method accepts only arbitrary bytes/hash plus key. |
| SIG-002 | Receipt, best-tip change, signature count, shard depth, ancestor fetch, and restart cannot emit an execution or optimistic state attestation without exact local replay. |
| SIG-003 | ML0 source, admission, execution, positive watchtower replay coverage, optimistic, DA custody, producer KES, and evidence signatures reject cross-domain replay even over identical content hashes. |
| SIG-004 | Missing base/body/code/era defers signing and does not slash. Wrong reproduced output refuses signing and creates only the ratified objective evidence. |
| SIG-005 | GL0 producer/optimistic, shard execution, ML0 state-validity, and GL1 state-validity signing paths each prove local complete validation before signing. Admission/custody non-validity types cannot be decoded/count as any of them. |

### 4.2 Hash-bound GL0 finality and tower

| Test ID | Required scenario and assertion |
|---|---|
| FIN-M-001 | Independent model and runtime agree on every P0/P1/P2, `Orphaned`, `RetentionMature`, and `RecoveryRequired` transition for generated branch/evidence traces. Status lookup is by exact ref, not ordinal. Retention maturity never changes fork choice. |
| FIN-M-002 | Real K/alpha/beta cascade differential covers uniform sampling, ancestor preference, emit-once, stale/replayed/equivocating responses, `N<K`, churn rule, eclipse, and adaptive corruption. Delivery permutations with the same authenticated response set cannot produce opposing portable decisions. |
| FIN-M-003 | Decided-attestation `T_weight` and `k1` fallback are exercised independently and concurrently. First qualifying evidence advances only the exact current canonical hash. `T_count` has no independent release path. |
| FIN-M-004 | Small-network exhaustive exploration and large stochastic trials state the actual probabilistic safety/liveness bounds; no theorem from a different leader/finality construction is imported without an explicit transfer proof. |
| FIN-D-001 | Every binary comparison uses the ratified boundary: forks within `k1` use `maxvalid-tk`; forks deeper than `k1` use `maxvalid-bg`, including forks deeper than `k2`. Sparse loses to dense under the deep rule. Generated pairs agree on the exact MRCA, `k1` distance, density window, and tie result selected under O-15. |
| FIN-D-001A | Preserve the strict-preference comparator RED `A >tk B`, `B >bg C`, `C >bg A`, which does not depend on the current tie rule. Upgrade it to complete authenticated snapshots that pass VRF/KES/historical-era validation. Once every node has the same cutoff-complete published valid frontier and branch-authenticated parameters, every candidate permutation, prior incumbent, legal arrival schedule, restart, and gossip duplication must converge to the same head. Interim partial-frontier heads may differ; a late reveal triggers deterministic reselection. Independent implementations agree on the complete trace and result. |
| FIN-D-001B | Preserve the depth-`k1+1` boundary witness where sparse/longer wins Tk and dense/shorter wins Bg. O-15's engineering/proof gate must derive and freeze the exact post-MRCA distance metric and `== k1` boundary; production config, reference model, evidence verifier, and runtime comparator must all select the same rule. |
| FIN-D-002 | A P2 shallow/deep fork-choice replacement emits exact old/new/MRCA ranges, unwinds/refolds byte-identically, and can replace the hash at the same ordinal. |
| FIN-D-003 | `k2`, retention status, pruning metadata, and local archive availability cannot reject or bias an objectively denser valid chain. Nodes with full history switch automatically; nodes missing history enter `RecoveryRequired` before production/serving/mutation. |
| FIN-D-004 | Two productive partitions grow beyond `k2` and heal. Full-history nodes converge by the same density rule. Pruned nodes authenticate/fetch/rebuild and reach byte-identical state; manual recovery cannot choose the branch and asymmetric/truncated density comparison is forbidden. |
| FIN-B-001 | Base Taktikos/LDD model and runtime agree on slot/ordinal/parent validity, VRF eligibility, eta, valid-only fork choice, and wrong-root rejection under equivocation, withholding, selfish production, and eta/key grinding schedules. Slot gap is derived from the exact retained parent certificate: forged transport/signed `parentSlot`, equal/decreasing slot, and a leader-built artifact that fails this gate reject before Ed25519/KES signing, local storage, fork choice, or attestation. |
| FIN-B-002 | For period N>=2, eta is derived only from a proved-complete, exact-parent canonical source interval. Missing/truncated/pruned ancestry, a partial nonempty output list, producer-embedded eta, bootstrap substitution, reordering, duplicated/omitted rho, and locally different history availability defer/recover or reject identically; a genuinely complete empty interval has explicit portable evidence and one canonical result. |
| FIN-B-003 | Producer and every verifier derive the identical N-2 stake-distribution period from the exact child parent ordinal and canonical R. A VRF output between N-2 and live-stake thresholds, missing historical distribution, restart fallback, same-height branch replacement, slashing/join/exit boundary, and locally different live MPT state cannot change validity; unavailable history defers before signing or storage. |
| FIN-B-004 | GL0 leader eligibility verifies only under the producer's exact active-era canonical registered VRF VK. The immediate gate uses the unique frozen-genesis record; the runtime gate uses the candidate-parent historical record from `KEYREG-*`. A producer-supplied replacement key, many freshly ground keys for one slot, wrong/missing/duplicate registration, stale-era key, or valid proof under an unregistered key rejects before chain storage/fork choice/attestation; the registered honest key passes independently on every node. |
| FIN-W-001 | Crash before/after every finality journal, branch/MPT switch, shard-anchor, binary-confirmation, and outbox write yields the exact old transaction or exact new transaction. |
| FIN-W-002 | Restart reconstructs exact canonical P2 refs, branch evidence, and retention metadata without resetting to ordinal zero or accepting peer-supplied phase as authority. Automatic unsafe clear/rebootstrap cannot run. |
| FIN-W-003 | Objective abandonment before MPT publication proves the exact prior remained active. Abandonment after target publication restores the exact prior image at `targetRevision + 1`; revisions never rewind. Crash/cancellation at every boundary either resumes the same intent under a revalidated branch hold or enters recovery, and an abandoned target can never become `Released`. |
| FIN-S-001 | Operational APIs/followers serve only exact canonical P2 refs. An old hash at a still-servable ordinal rejects after replacement; retention/archive APIs never imply irreversibility. |
| FIN-S-001A | API/event schemas represent P2 as reversible operational state with exact hash, evidence kind, and replacement events across JSON/protobuf/sidecar round trips. No ordinal-only monotone `finalized` projection can authorize state. |
| FIN-S-002 | Global optimistic attestation is emitted only for a fully authenticated locally executed snapshot and binds exact body/parent/root/era/parameters. |
| FIN-S-003 | Independent ML0/GL1/light-client verifier accepts valid optimistic or depth historical qualification evidence only with the ratified anchor/current-chain proof, rejects wrong/stale/orphaned ref, registry, weight, signature, suffix, parameters, and a single eclipsing peer's valid private suffix, then follows a shallow or deep fork-choice replacement. |
| TOWER-000 | **Current dark safety gate:** every active-era producer emits `smtRoot=None`; live validation, signed download, context creation, traversal, ML0 adoption, generic snapshot storage, combined-checkpoint reading, and direct snapshot serving reject `Some`; complete comparison is exact (`GlobalSnapshotActiveEraValidator.scala:10-30`; `StateProofComparison.scala:28-37,45-78`; `GlobalSnapshotConsensus.scala:692-694`; `GlobalSnapshotConsensusFunctions.scala:282-299,353`; `Download.scala:368-381,607-622`; `GlobalSnapshotContextFunctions.scala:61-72`; `GlobalSnapshotTraverse.scala:107-116,183-194`; `StateChannel.scala:213-230,306-315,423-432`; `SnapshotStorage.scala:100-125`; `FinalizedSnapshotReader.scala:145-169`; `SnapshotRoutes.scala:62-82`). Historical SMT/tower wiring stays absent, the provider stays `None`, and proof routes return unavailable/503 (`GlobalSnapshotConsensus.scala:1510-1514`; `NipopowRoutes.scala:82-87`). Regressions include `SnapshotStorageSuite.scala:132-190` and `FinalizedSnapshotReaderMptEntriesAtSuite.scala:158-177`; the reserved schema field remains at `GlobalSnapshotStateProof.scala:124-130`. |
| TOWER-001 | **Future activation gate:** producer computes branch-carried tower eligibility and historical SMT update from one exact branch-bound retained image; every GL0 verifier independently reproduces both before snapshot acceptance/signing. Changing eligibility, path, leaf, or `smtRoot` rejects. |
| TOWER-002 | Future `Some(smtRoot)` activation occurs atomically across artifact equality, follow, signed download, context/traverse, ML0 adoption, catch-up, restart, reorg, bootstrap, and serving. Until that activation, every `Some` rejects; stripping/normalizing the field can never make unequal artifacts equal. |
| TOWER-003 | Empty/vacuous, truncated, duplicate, unordered, oversized, disconnected, missing-path, forged-tip, and no-progress proofs reject within hard resource bounds. Builder readiness is distinct from verifier availability: while local proof construction is dark/rebuilding, `GET /proof` returns typed unavailable but the independently configured pure `POST /verify` still verifies or rejects a supplied proof. Adversarial `since`, `k`, tower size, and concurrent requests cannot hold the catch-up mutation gate or allocate/read beyond the ratified bound. |
| TOWER-004 | N-2 stake, N-1 eta, active registry/KES/VRF key, period transition, parent/tower pointer, and signature are independently checked. Current-set substitution and uniform-probability approximation reject. |
| TOWER-005 | One honest peer's complete proof verifies from trusted genesis or cached authenticated commitment. A peer can withhold freshness but cannot forge a heavier chain, state inclusion, or current canonicality. |
| TOWER-006 | Tower/SMT stores survive crash/restart and every shallow/deep exact-hash fork-choice replacement. Rebuilt canonical roots/proofs equal clean replay byte-for-byte; append-only stale-branch state is never served. `TowerFinalizer` resumes the reconstructed de-duplication watermark and metrics-only level-0 count instead of resetting either ref. A controlled simultaneous `finalize(N)`/`finalize(N+1)` schedule must equal sequential oldest-first finalization: prepare and commit share one writer generation, stale gaps cannot append, and the watermark cannot regress. A catch-up interval `M` greater than 100 with chunk bound `B`, a missing retained snapshot, failure/cancellation at each ordinal, and restart between chunks advance one durable work cursor only through the last successfully appended exact `(ordinal,hash)`; each call reads/allocates at most `B+constant`, total walk work is `O(M)`, every suffix is retried, and clean-replay parity holds. Hold a ready tower for branch A while ordinal/head storage switches to same-ordinal branch B: proof building must return rebuild/unavailable, never a B header carrying A's injected hash. Every header is fetched by exact hash, rehashed, and bound to one immutable published target/store generation. |
| TOWER-007 | Proof comparison and `maxvalid-bg` agree on generated competing valid chains across era/eta boundaries and divergence beyond local `k2`; missing proof material enters recovery rather than defaulting to a local winner. |
| LIFE-001 | Capability matrix rejects P0/P1 use, permits only reversible P2 actions, and applies positive watchtower coverage to every checkpoint-derived economic capability. External service confirmation policy cannot change protocol validity or canonical state. |

Current worktree status: `TOWER-000` and focused exact-comparison regressions are
green (`GlobalSnapshotActiveEraValidatorSuite.scala:73-93`;
`StateProofComparisonSuite.scala:69-72`;
`SmtRootBlindValidateArtifactSuite.scala:62-103`). `STOR-03` is contained dark,
not closed: provider publication is explicitly disabled
(`GlobalSnapshotConsensus.scala:1510-1514`), while the staged serialized
exact-hash coordinator/finalizer is in-memory and walks tip-to-cursor history
(`TowerCatchupCoordinator.scala:20-36,216-273,275-398,400-513`;
`TowerFinalizer.scala:33-74,156-235`). `TOWER-003/006` still require durable
rebuild/cursor, hard bounds, and a builder bound to the published target rather
than mutable storage head (`TowerProofBuilder.scala:92-165`).

Current `SnowballAccumulatorSuite` and `FinalityTriggerSuite` are component tests,
not FIN-M proof. `SnowballAccumulatorSuite` now contains the concrete arrival-order
counterexample for the current sticky margin; it proves the implementation gap, not
the target cascade. Tests asserting ordinal-only monotone finality must become RED
fixtures for FIN-D-002/FIN-S-001.

The current `FinalityReferenceModel` is also component scaffolding, not FIN-D-001A:
it accepts an already-selected canonical tip and checks only the shallow/deep rule
tag before modeling replacement (`FinalityReferenceModel.scala:7-11,97-100,217-313`).
`ChainSelectionSuite` contains both the current tie-assisted regression and a
strict-density three-cycle. `NakamotoChainStoreSuite` now reproduces the
corresponding store-path defect under a synthetic enabled `k`/`s` configuration
with signed ordinal/parent linkage: three parent-before-child schedules over the
same frontier leave best tips C, B, and A
(`NakamotoChainStoreSuite.scala:280-372,427-469`). This closes the direct
store/control-flow RED, not FIN-D-001A: it calls `store` with synthetic
slot/VRF/context, does not pass full snapshot/VRF/KES/historical-era admission,
and does not exercise a shipped environment configuration.

The current dark `ForkChoiceDecision` is not FIN-D-001A evidence. It carries an
opaque `ImmutableArtifactPointer`, deliberately without a Tk/Bg or transition-form
assertion; the validator binds one intent-scoped evidence locator to the selection
token's pointer. It does not decode or verify the
complete header/tine frontier, its completeness boundary, or its active parameter
era (`FinalityCore.scala:120-138,353-364`;
`FinalityBaseCodecs.scala:96-119,178-179`;
`FinalityIntentValidator.scala:59-120,748-761`).
FIN-D-001A remains RED until O-15's ratified direction is implemented by closing
the cutoff/reveal semantics, cycle-resolution, exact metric/tie, canonical
evidence and independent-verifier engineering/research gates, plus a complete
validator-backed active-configuration admission witness, corrected-store
convergence under every required schedule/restart/duplication trace, and the
security/liveness argument.

The closed `PublicationRestoration` variants and current codec/validator tests
prove only part of FIN-W-003's data contract: the schema validates one exact plan
and claimed receipt shape, but does not prove that the target stayed unpublished
or that an external MPT republish occurred. The plan names either the unchanged
prior or the prior image at a consecutive revision. The committed effective
publication cursor survives restoration retirement and binds the next prepare.
The `ForkChoiceOrphanClaim` is structurally constrained but does not prove true
MRCA or target exclusion. The durable store rejects restoration mutations and
there is no live executor, branch hold, fault injection, semantic/anchor receipt
verifier, or public `RestoredAbandoned` authority, so FIN-W-003 and FIN-W-001 remain
open. The raw initialization route is now gone. `DurableMptImageStoreSuite` proves
that the store-minted publication lease verifies durable journal state, expires on
return/error/cancellation, rejects corrupt referenced images before callback, and
blocks concurrent publication transitions. `FinalityCoordinatorBootstrapSuite`
proves no-write failure on uninitialized/corrupt MPT, exact fresh/idempotent binding,
typed mismatch recovery, absorbing recovery preservation, no effect-outbox creation,
and MPT-mutex retention through the finality head write. Kernel/codec tests reject a
raw initializer, validate exact mismatch identity, and retain closed tag 12. These
tests close local durable bootstrap provenance only. Semantic/Phase-2 anchoring,
coordinator ownership of every later `transitionActive`, combined mismatch-recovery
crash/cancellation injection, and live wiring remain activation gates
(`FinalityCore.scala:395-460`;
`FinalityCoordinatorState.scala:68-76,99-136`;
`FinalityIntentValidator.scala:891-925,1083-1275,1277-1288,1364-1442,1461-1608`;
`FinalityCoordinatorKernel.scala:74-112`;
`FinalityCoordinatorBootstrap.scala:9-84`;
`DurableMptImageStore.scala:87-97,177-182,542-552`;
`DurableMptImageStoreSuite.scala:646-803`;
`FinalityCoordinatorBootstrapSuite.scala:58-192`;
`FinalityDurableStore.scala:293-319,1209-1230,1357-1363,1652-1656`).

Full positive `validateCoreBatch` fixtures now construct both
`ForkChoiceReplacement` and inherited `ForkChoiceRollbackToOperationalMrca` from
a valid prior batch/released core and validate the complete successor batch
(`FinalityIntentValidatorSuite.scala:388-444,896-1004`). This closes the
cross-field schema-constructibility test gap only. The fixtures deliberately do
not assert winner semantics; they do not authenticate the opaque fork-choice
evidence, prove the selected frontier, execute or durably apply either transition,
or make either transition activation-ready.

### 4.3 Serialization, protocol era, and cryptography

Operator-key delivery has separate immutable-genesis and runtime merge gates.
The atomic paired registry, rooted signed genesis key records, durable runtime
records, and N-2 resolver are partial implementation evidence. The
immutable-genesis gate closes only when the independent genesis eligibility
population is also rooted and every reachable consumer intersects it with the
paired records. The runtime gate remains blocked on implementation of O-11's
ratified roster direction, including its schema, parameter, and proof gates, plus
an authenticated exact-parent view, exact Phase-2 artifact references, and
consumer/reorg/evidence migration. A persisted runtime record or passing resolver
unit test is not runtime eligibility.

| Test ID | Required scenario and assertion |
|---|---|
| SER-001 | Every manifest type has exact checked-in ScodecV1 bytes/hash/signature vectors and round-trips valid refined values. |
| SER-002 | Decoder consumes all input and rejects trailing bytes, duplicates, unsorted maps/sets, unknown tags, non-minimal integers, invalid refinements, excessive depth/cardinality/length, and decompression bombs. |
| SER-003 | Independent Go or Rust implementation matches Scala for snapshot/checkpoint/diff/intents/evidence/MPT/migration bytes and hashes. |
| SER-004 | Consensus-object codec migration cannot silently alter frozen MPT key derivation. Intentional key transforms have before/after root vectors. |
| SER-005 | Every active hashing/signing call uses the domain/type-specific ScodecV1 preimage selected by one canonical era service. Repository/runtime guards reject JSON/Kryo `Hasher`, Circe-derived signing preimages, and local ordinal switches on consensus paths. |
| SER-006 | MPT leaf values and internal-node commitments have frozen ScodecV1 bytes and root vectors. JSON field/object ordering, printer settings, platform defaults, or runtime class registration cannot affect a root. |
| SER-007 | State-channel lane/type is signed before content. Currency and currency-with-data decode exactly one bounded framework schema; opaque/custom bytes that happen to decode as a framework object remain opaque and cannot construct an economic transition. |
| ERA-001 | Every environment starts the new chain with only ScodecV1 at ordinal 0. Kryo/JSON consensus encoding, format probing, and undeployed compatibility decoders are unreachable. |
| ERA-002 | No `ScodecV2` runtime schema or decoder exists in the greenfield release. A future ratified codec era must first add checked-in O-2 through O+2 vectors covering sibling branches across O, activation reorg, restart, catch-up, wrong-era/downgrade bytes, and a live authorization crossing O; this row does not authorize a speculative compatibility schema today. |
| ERA-003 | Only the exact canonical hash-bound era schedule selects an era. Local field/hash/state-proof ordinals cannot change consensus shape; a density reorg changes activation consistently and stale nodes halt. |
| ERA-004 | Frozen upstream-v4 Kryo and Brotli-JSON disk fixtures decode only through the offline read-only importer, reproduce the audited source state/root, and yield one deterministic ScodecV1 genesis manifest. Missing/corrupt/mixed-era input aborts import; active runtime rejects every legacy fixture. |
| CRYPTO-001 | Missing/wrong KES/VRF key, PoP, period, domain, roster, eta, or parameters rejects on every receive/store/sign/evidence path. |
| KEYREG-001 | Against the rooted immutable genesis pair/population intersection, a valid GL0 producer identity/KES signature plus a VRF proof under any sender-selected replacement VK rejects before signing, storage, fork choice, or attestation. Repeated fresh-key trials for one `(eta, slot)` never improve eligibility; only the unique preregistered pair of an authorized genesis operator can pass. |
| KEYREG-002 | The canonical registration preimage binds network/genesis/domain, `PeerId`, KES master VK, VRF VK, effective eta period, registration ordinal, and exact registration parent hash under the long-term identity signature. Mutation, cross-network/branch replay, wrong signer, non-canonical bytes, or partial-field substitution rejects. |
| KEYREG-003 | A candidate with `signedEffectivePeriod < inclusionPeriod + 2` rejects rather than being locally clamped. An accepted record activates only at its signed effective period and only when present in the exact N-2 branch view used for period N, with the pair fixed before N-1 eta is available. Boundary vectors include late-inclusion cases and prove the index-lookback rule without silently changing it to I+3; local wall clock, receipt time, current head, or sender claim never decides it. |
| KEYREG-004 | Rotation vectors cover the last candidate under the old record and first candidate under the new record at exact eta-period/parent boundaries. Premature new keys, stale old keys, gaps, overlapping claims, replay after rotation, and mixed-record KES/VRF selection reject deterministically under the atomic-pair rule. Freeze one canonical protocol-era rule for exact complete-pair duplicates and partial rotations, then require positive and negative vectors for that rule before runtime rotation activates. |
| KEYREG-005 | Duplicate claims at one operator/registration ordinal, cross-operator active VRF or KES ownership, conflicting activation claims, and ambiguous historical lookup fail closed. Input order, map/set order, arrival order, restart, and peer-local registry contents cannot choose a winner. |
| KEYREG-006 | One generated branch/era/operator matrix exercises GL0 snapshot leader validation, metagraph-binary admission self-sortition, execution-shard public VK-hash membership, staircase leader/duty identity and checkpoint possession, replay signing/acceptance, tower trials, slashing evidence, and any VRF-keyed optimistic/watchtower draw. Every consumer resolves the byte-identical active KES/VRF pair from the canonical API; no consumer accepts a direct transport/certificate VK or KES step as authority. A semantic source manifest inventories every VRF/KES draw, proof, verification, signing, duty, tower, and evidence entry point, including consumers that call higher-level abstractions without naming the raw primitive. The landed narrow textual guard remains a secondary tripwire, not evidence that this matrix is complete. |
| KEYREG-007 | Two branches at the same ordinal include, rotate, or orphan different records before and after activation. Candidate validation uses only an authenticated exact parent `(ordinal,hash,parentHash,mptRoot)` view. A sibling hash, the requested hash paired with registry/GSI bytes from the sibling root, an ordinal-only lookup, and receiver-current canonical state all reject/defer before side effects. A density reorg atomically unwinds/refolds active and pending records. |
| KEYREG-008 | Restart, pruning boundary, catch-up, and long-offline recovery reproduce the same active record and activation epoch from authenticated history. A stored ordinal with the wrong snapshot hash or state root, missing root witness, or unverifiable historical registry defers/enters recovery before signing, acceptance, or slashing; it never falls back to a live, genesis, peer-supplied, or most-recent key. |
| KEYREG-009 | A portable NiPoPoW/tower proof carries and verifies membership plus activation evidence for the exact operator record and roster root committed by the proved branch. A valid KES/VRF proof with a missing, wrong, pending, orphaned, current-set, same-ordinal sibling-hash, or wrong-root membership witness rejects; one honest peer's complete historical witness verifies independently. |
| KEYREG-010 | Cross-consumer property traces rotate keys while producing GL0 snapshots, admission receipts, execution membership/possession proofs, replay signatures, checkpoint acceptance, optimistic/watchtower assignments, tower hits, and fraud evidence. For each exact parent/base/anchor `(ordinal,hash,parentHash,mptRoot)`, all consumers either accept the same registered pair and derived KES step or all defer/reject. Replacing only the hash or root at the same ordinal rejects at producer, signer, adopter, reorg, and adjudication boundaries; no mixed old/new pair or consumer-specific registry can partially accept the artifact. |
| KEYREG-011 | Every consensus-path snapshot, admission, committee, execution signer, watchtower, optimistic-finality, tower, and slashing fixture first registers one paired KES+VRF identity in the exact canonical N-2 view used for eligibility, or commits the complete pair plus its independent authorized population entry in genesis for period-zero eligibility. A generated-but-unregistered key fails before draw/proof/sign/attest/store/adopt/slash side effects. Only isolated cryptographic primitive tests may exercise arbitrary unregistered pairs. |
| KEYREG-012 | The complete genesis KES+VRF registry and separately authorized immutable genesis operator/stake population are committed by the canonical genesis identity/root. Two nodes given otherwise identical genesis inputs but different key or population JSON cannot both enter Ready or validate descendants; mutation, omission, or reordering changes the committed identity and is caught before consensus. Restart without both committed witnesses fails closed. |
| KEYREG-013 | Registration alone never grants participation. Every leader, admission, execution, watchtower, optimistic-finality, and tower population is the exact intersection of active paired registrations and the branch-bound delayed canonical operator/stake roster. A valid self-signed pair outside that roster, a roster member without an active pair, a same-ordinal roster from a sibling hash/root, and any fallback to key-only/current/local membership all fail before draw or weight calculation. The immutable genesis population is the bounded safety cut; permissionless runtime admission cannot pass until O-11's ratified structure is encoded, rooted, parameterized, and proved. |
| KEYREG-014 | Before registration, the operator atomically provisions the VRF secret and KES tree for one pair, derives the exact public bytes, and binds the encrypted/durable bundle to the registration ID. At the N-2 activation boundary, only an exact-parent historical eligibility capability may select that local bundle. Missing one half, mismatched public bytes, old/new half mixing, current-head/time selection, crash between writes, duplicate bundle ID, and activation before durable provisioning all fail before VRF draw, proof, KES evolution, or signature. The previous active bundle remains selected until the exact boundary; duplicate/partial rotation follows the frozen KEYREG-004 protocol-era rule. |
| KEYREG-015 | Generated density traces cross a pending and active registration's N-2 lookup boundary before and after local KES erasure. Canonical public state may unwind/refold, but the KES secret never rolls backward. Under the owner-ratified O-12 rule, a reorg crossing erased material enters durable `RecoveryRequired`, emits no snapshot/admission/execution/finality/tower/evidence signature, cannot slash, and requires explicit operator realignment/rejoin. A configuration that silently retains old KES masters through k2, restores erased bytes, falls back to another local bundle, or lets an operator choose the branch fails the forward-security/recovery gate. |
| PARAM-001 | Independent implementations derive exact k1/k2/R/eta periods, committee thresholds, duty windows, challenge horizons, and resource limits from one parameter object. No `Double`, local env, overflow, or rounding-mode difference changes a result. |
| GOV-001 | Unauthorized, locally configured, immediate, stale-base, or insufficiently delayed parameter/era/metagraph updates reject. Authorized canonical activation and branch reorg follow the ratified rule. |
| HALT-001 | A local production pause changes only that node's willingness to propose. It cannot change validation/finality/state. If a protocol halt exists, unauthorized/unbounded/state-rewriting/validation-waiving/stale halt/resume rejects and deterministic expiry survives restart/partition. |

### 4.4 Framework economics

| Test ID | Required scenario and assertion |
|---|---|
| ECON-D-001 | Generated/adversarial traces make independent reference and production kernels agree on ordered accepted/rejected IDs, exact diff, extracted intents, and root after every prefix. |
| ECON-C-001 | For every currency/prefix, spendable + locked + reserved + explicit sinks equals declared supply adjusted only by named bounded mint/burn. |
| ECON-A-001 | Unsigned unlock, no-reference spend, wrong owner/source, malformed fee binding, unregistered reward, and ML0/custom claimed framework artifacts reject. |
| ECON-R-001 | Network/genesis/era/type replay, proof/signature reordering, duplicate proofs, restart, and long-history replay produce one semantic identity and one effect. |
| ECON-O-001 | Operation ordering is identical under randomized map/set construction, input arrival, chunking, parallel scheduling, JVM/OS/CPU, and shard count. |
| ECON-B-001 | Overflow, underflow, saturation attempt, max values, one-unit-over resource limits, exception paths, and malformed refinements reject with no partial writes. |
| ECON-F-001 | Fee debit/sink/refund and partial consume semantics are explicit and conserved. Cancel/expiry/consume priority matches the canonical total-order rule derived and frozen under O-13's remaining engineering gate. |
| ECON-G-001 | Every launch-enabled no-ref spend, unlock, reward/mint/burn, stake replacement, protocol correction, and expiry path matches the ratified authority grammar; every unspecified form rejects. |
| ECON-G-002 | Every rooted price and node-parameter entry retains its actual physical key, strictly decodes one bounded value, recomputes the canonical `TokenPair`/operator-ID key and scope from that value, and rejects mismatch, duplicate logical identity, wrong field/scope, malformed/trailing bytes, or conflicting same-batch updates before constructing the consensus view. Input order, restart, exact-hash reorg, and clean replay produce the identical selected price/parameter state and root. |
| ECON-F-002 | A framework fee/intent that binds custom data commits exact content/lane/domain once. Changed or replayed custom content rejects atomically; custom application output alone cannot synthesize a fee or other framework write. |
| ECON-F-003 | A framework data fee consumes the exact rooted per-MG/source fee parent and advances balance plus fee head atomically. Exact replay, same-parent sibling, gap/future parent, cross-MG/network/era/lane reuse, proof-container reorder, duplicate mapping, orphan fee, unmatched custom item, and unavailable committed bytes reject without either mutation. Restart and compaction preserve rejection from the head alone. |
| ECON-REF-001 | Every accepted native and currency transaction/reference successor consumes exactly its signed canonical parent and writes one rooted next reference. Wrong, stale, duplicate, skipped, cross-network/era, or same-batch-conflicting predecessors reject without balance or reference mutation. |
| ECON-BAL-001 | Every balance write is derived from an authorized operation or active-era GL0 correction, is checked arithmetically, conserved against named source/sink/supply entries, and changes the complete root. A metagraph/custom claimed balance field has no constructor into this transition. |
| ECON-BAL-002 | One shared ledger evaluates transfer, framework/data fee, reward, allow-spend, token-lock, SpendAction, and slash-bounty claims over `(currency scope,address)`. Transfer 60 plus lock 60 from balance 100, every cross-class pairing, multi-address rollback, duplicate IDs, self-destination, and max-boundary vectors deterministically accept one valid prefix or reject/defer an operation without exception or partial mutation. A referenced or no-ref credit of 1 to a `Long.MaxValue` destination rejects before application, and SpendAction validation uses the exact projected state after every earlier class rather than its own stale base. ML0 applies mandatory exact-P2 GL0 inbox claims before conflicting local CL1 claims. |
| ECON-BAL-003 | Allow-spend `A->B` never exposes B funds before consume; `A->B` followed by `B->C` from empty B rejects. Consume versus expiry has one terminal winner; missing, duplicate, overspend, same-address, and exact-last-valid-epoch cases return typed conserved results. GL0 rejects any nonnative/mixed block and ML0 rejects any native/foreign/mixed block even when metagraph-specific validation is disabled. A token-lock replacement release is consumed exactly once across unrelated blocks and all block permutations. GL0, ML0 producer/recreation, shard producer, every execution signer, and watchtower derive identical operation IDs, projected balances, diff, and root at shard counts 1, 2, and K. |
| CORR-001 | Active-era GL0 protocol correction at the exact expected MG pre-root/version deterministically yields the declared bounded diff/post-root on every validator and is fully committed by the global root. |
| CORR-001A | An active-era GL0 correction may intentionally adjust a balance or supply only when the artifact declares the exact source/sink or signed supply delta and every validator derives identical post-balance, post-supply, diff, and root. This positive protocol capability remains impossible to originate from ML0/CL1/DL1. |
| CORR-002 | ML0/CL1/DL1-originated, local-config, wrong-era/network/genesis/activation-parent, stale-base, duplicate-ID, cross-namespace, unrooted, nonconserving, and oversized corrections reject atomically. |
| CORR-003 | A density reorg reverts/reapplies a correction with its containing branch, preserves its replay identity, and emits one mandatory downstream rebase event. Restart at every correction write boundary returns exact old or new state. |

### 4.5 Checkpoint diff and replay-before-sign

| Test ID | Required scenario and assertion |
|---|---|
| DIFF-001 | Canonical before/after state yields one sorted unique diff. Encode/decode/apply on independent stores reproduces exact root. |
| DIFF-002 | Duplicate, unsorted, oversized, wrong-MG, wrong-field, global-partition, cross-MG, missing-root-coverage, and conflicting upsert/remove keys reject before mutation. |
| DIFF-003 | Every allowed writable key changes the verified root. Economic active allow-spends field 7 is covered. After ROOT-010 activates, `MgGlobalSnapshotSyncView`/field 32 is rejected from producer diff construction, signer verification, ordinary adoption, peer/disk load, and shallow/deep reorg. ML0 still retains the exact optional view in `CurrencySnapshotInfo`; its replay preimage is carried by the separate signed/root-bound witness, not a GL0 writable leaf. |
| SHARD-E-001 | Producer and each signer independently replay exact binaries at exact P2 base and match decisions, diff, intents, and root before signing. |
| SHARD-E-002 | Tampered root, diff, input order/content, base hash/root, epoch/roster, parent, duty, lane, DA commitment, or extracted intent prevents signature. |
| SHARD-E-003 | `kQuorum-1`, duplicate signers, nonmembers, wrong epoch, invalid KES/VRF, and depth-only qualification cannot make a diff adoptable under the mandatory-threshold rule. |
| SHARD-E-003A | A checkpoint that omits or changes network/genesis/era/parameter hash or exact Phase-2 base hash/root cannot be signed or adopted. Ordinal equality alone is insufficient. |
| DIFF-004 | A correctly certified diff whose signed pre-root/version predates an intervening proposal-parent metagraph mirror write rejects without mutation; the separate global settlement overlay neither causes a false rejection nor waives the mirror compare-and-set. |
| SHARD-E-004 | Ordinary noncommittee adoption instrumentation observes zero currency recreation calls while valid apply/root succeeds and every malformed artifact rejects. |
| SHARD-E-005 | Retroactive ancestor attestation uses the same replay capability. Cache/restart cannot convert an unverified checkpoint into a signable one. |
| SHARD-E-006 | Reproduce ECO-F32 with one node reading a locally staged nonempty field-32 base and another reading a peer-backfilled map that verifies the same signed GL0 root after field-32 stripping. The pre-fix paths derive different `CurrencySnapshotStateProof.globalSnapshotSync`/per-MG roots or signing outcomes. The fixed paths consume the same exact optional full-view witness and explicit ML0 operator population and reproduce byte-identically. Missing/wrong preimage, wrong population, hash mismatch, and `None`/`Some(empty)` substitution defer before signing and cannot create slash evidence. |
| SHARD-C-001 | Staircase duty rotation, timeout/skip, sibling selection, committee rotation, epoch-anchor binding, and recovery are deterministic and use only the specified checkpoint parent/duty state. |
| SHARD-C-001A | Producer and verifier derive the identical execution epoch from the checkpoint's signed anchor ordinal with one pure function. A wrong wire epoch rejects before committee lookup, signature verification, replay, storage, or signing. The same artifact under differing local R values must not produce different validity: R is canonical and parameter-hash bound, or local mismatch halts before artifact validation. |
| SHARD-C-001B | A producer cannot select an older anchor ordinal and its matching favorable epoch/committee merely because historical replay state is retained and `anchor <= containingOrdinal`. The proposal-parent exact Phase-2 `(ordinal,hash)` evidence and the freshness/lineage rule still to be derived and frozen determine the only admissible anchor before epoch/eta/roster lookup. Current acceptance of a self-consistent old pair is a passing RED characterization, not valid target behavior. |
| SHARD-C-002 | Tentative GL0 inclusion does not advance the shard's operational anchor. Exact containing P2 transition does; P2 reorg reverses it and requeues each binary once. |
| SHARD-C-002A | A node that never received a checkpoint on shard gossip but validates its containing GL0 artifact ingests the exact embedded checkpoint bytes before the P2 operational-anchor/retention update. After P2 it has the same parent, tip, and producer-duty context as a node that saw gossip and can produce/validate the successor; duplicate delivery is idempotent. |
| SHARD-C-003 | One checkpoint deterministically batches multiple MG segments such as `A=[100,101,102]`, `B=[1002]`, `C=[3,4,5]`. Cutting preserves each MG's authenticated parent/ordinal order, signed nondecreasing P2 refs, boundedness, fairness, and progress for currency and currency-with-data. |
| SHARD-C-003A | Exactly one checkpoint may be outstanding per shard. A second child/build, replay-sign, intake, and embedded-adoption attempt rejects until signed, branch-bound evidence proves the exact current checkpoint became P2-anchored or was orphaned/requeued. This holds for an honest producer and a Byzantine `kQuorum` committee after restart/reorg; no receiver-local anchor, configurable checkpoint pipeline, ordinal equality, or shard-depth release path can satisfy it. |
| SHARD-C-003B | GL0 adoption compare-and-sets every touched MG pre-root/version independently and atomically installs every resulting MG head. One stale segment rejects the whole checkpoint; omitted/duplicate/reordered MG segments cannot partially advance heads. |
| SHARD-C-003C | If the retained GL0-adopted checkpoint ref is ahead of or hash-incompatible with the local shard-chain tip, production enters exact checkpoint recovery. It cannot recreate an old ordinal, mint from a guessed parent, or advance from ordinal equality alone. |
| SHARD-C-003D | The outer shard-map key equals the checkpoint's signed `shardId`, every included MG window is `Continue` or `AlreadyAdopted`, at least one window continues, and every continuing suffix appears byte-exactly in the containing GL0 artifact. One deferred, stale, omitted, shortened, reordered, wrong-root, or independently decoded-but-byte-different segment rejects the whole checkpoint and prevents its P2 operational anchor; no partial MG application can release the next checkpoint. |
| SHARD-C-004 | Same input trace at `numShards=1`, 2, and K produces identical economic results even when sharded scheduling/order of arrival differs. |
| SHARD-C-005 | Economic `numShards=1` still executes producer/signer/watchtower roles and ordinary diff adoption. No `numShards > 1` validity, replay-protection, nullifier, slash, or finality gate changes semantics. |
| SHARD-C-006 | Local or unscheduled live `numShards` change halts/rejects before remapping. If a future reshard era is enabled, full drain/handoff/reorg/restart tests preserve every MG state, input, nullifier, and root. |
| SHARD-C-007 | Eta rotation with checkpoints in every lifecycle state selects exactly one anchored roster/threshold. Delayed signatures, restart, P2 reorg, and old/new members cannot create mixed quorum, false slash, loss, or duplicate inclusion. |
| SHARD-C-008 | Durable outbox retry republishes the same outstanding checkpoint after gossip loss/restart until it is P2-anchored or orphaned. Retry cannot mint a new shard ordinal, clear the held checkpoint, duplicate binaries, or exceed bounded backoff/storage. |
| SHARD-C-009 | Intake, replay-signing, and embedded-artifact validation derive the same producer from the retained head signature and enforce the same parent-relative shuffled-staircase duty before replay/store/sign/adoption. Embedded validation resolves parent hash/ordinal/slot from portable proposal-parent-bound evidence, never prior shard-gossip receipt or a receiver-local chain store. Off-duty, reordered-head, missing/wrong parent, noncontiguous ordinal, nonmonotone slot, wrong eta/roster, and a colluding off-duty `kQuorum` all reject; canonical on-duty input passes identically on nodes with and without prior shard gossip. |
| SHARD-C-010 | An embedded checkpoint's signed slot is no greater than the signed slot certificate of its exact containing GL0 snapshot. A checkpoint ahead of that bound remains pending; a committee member cannot choose a far-future slot that maps to its staircase rank, anchor it, and prevent honest production until wall time catches up. Receiver wall clock, arrival time, local head, and local skew configuration are not artifact-validity inputs. |
| SHARD-C-011 | Differentially validate the same signed checkpoint, exact parent, eta, and ordered roster under local `staircaseDeltaSlots=5` and `10`, choosing a slot gap that maps to different ranks. Both nodes must derive the delta from the same rooted proposal-parent/genesis parameter object/hash and return the same verdict; a local mismatch halts before committee lookup, replay, signature acceptance, storage, or GL0 artifact validation. |
| SHARD-C-012 | A density reorg replacing operational GL0 hash G-A with G-B at the same ordinal and replacing shard checkpoint C-A with sibling C-B at the same shard ordinal atomically replaces `lastAdoptedCheckpoint`, chain-store anchor/retention, producer held state, recovery watermark, MG frontiers, and downstream event. The producer extends C-B after crash/restart; no component fetches, re-anchors, republishes, or waits for orphaned C-A. A standalone sideways/backward `noteAdopted` call without authenticated old/new/MRCA reorg evidence rejects. |
| SHARD-S-001 | Model/sim sweeps S, N, kDraw, kQuorum, corrupt stake/operators, correlated public draws, adaptive corruption, and partition. Report committee/watchtower capture and cross-shard chain-quality failure rather than assuming the claimed alpha_total bound transfers. |
| SHARD-S-002 | For every supported N including N below kDraw/kQuorum/K, all nodes choose the same explicit mode. Impossible execution quorum does not auto-lower. Small-network Avalanche behavior is an explicit O-01 parameter/outcome; until frozen, an economic production profile that reaches that case cannot boot. |
| SHARD-S-003 | Stake splitting, multiple registered keys, activation after eta disclosure, admission/execution/watchtower draw correlation, and adaptive key grinding cannot multiply one economic operator's eligibility beyond the ratified bound. |

`SHARD-C-009` remains RED while embedded validation reads a receiver-local shard
store. Its oracle runs the same child artifact on fresh validators with and without
prior shard gossip and requires the same verdict from portable proposal-parent-
bound evidence; the current characterization test deliberately proves the two
local-availability verdicts diverge. Producer identity is currently inferred from
the first element of a signature list excluded from the signed preimage. Reordering
can relabel the nominal producer, but does not bypass duty because only the unique
scheduled signer is accepted as head and honest replay signers never sign an
off-duty-head artifact. Explicit signed producer-role binding remains an
accountability hardening item, not a confirmed post-patch duty bypass.
`SHARD-C-011` remains RED while production wiring injects local HOCON
`staircaseDeltaSlots` into receive-side duty validation. Its differential oracle
uses one checkpoint/parent/roster whose scheduled producer differs at delta 5 versus
10; delta must come from the same rooted proposal-parent/genesis parameter context
as roster and eta, and a local mismatch halts before validation.

### 4.6 Watchtower/slashing and release timing

| Test ID | Required scenario and assertion |
|---|---|
| WT-001 | A colluding execution threshold signs a wrong diff/root; assigned noncommittee watchtower replay detects it, and the ratified adjudicator independently computes the mismatch from exact retained inputs/base before rollback or slash. |
| WT-002 | Unavailable base/input, peer timeout, local crash, different but valid custom-data availability view, or stale checkpoint cannot slash an honest signer. |
| WT-003 | Evidence identifies only actual signers, is byte-deterministic, applies once, debits real bonded principal, caps reward, and changes the next eligible roster at the ratified anchor. |
| WT-004 | Partition/eclipsed watchtowers delay only the affected checkpoint. Before required positive coverage, it is not GL0-inclusion-eligible and cannot create any local/cross-MG economic derivative; unrelated GL0/shard work progresses. |
| WT-005 | Watchtower selection is deterministic from canonical state, excludes execution members as specified, provides the required coverage, and cannot be producer-chosen/grinded. |
| WT-006 | A later P2 reorg does not retroactively make an honest exact replay fraudulent. Evidence proves the base was eligible when signed and distinguishes branch orphaning from an execution mismatch; a real mismatch remains objectively verifiable on retained bytes. |
| WT-007 | Forged, duplicate, conflicting, unavailable-input, and flood challenges cannot false-slash or force unbounded universal replay. Assignment, challenger bond, rate/resource limits, and deterministic exceptional replay survive restart/partition. |
| WT-008 | Under the conservative rule, an execution-certified checkpoint without required positive watchtower coverage is not GL0-inclusion-eligible, while unrelated native/other-shard GL0 snapshots continue. Coverage makes the exact checkpoint eligible once without changing its bytes. |
| WT-008A | Positive coverage is signed only from a local replay capability and is bound to the exact checkpoint/assignment. Duplicate, execution-member, nonassigned, wrong-domain, bad-signature, stale-anchor, and subthreshold coverage cannot make it eligible. Any authenticated assigned mismatch report quarantines it despite an otherwise sufficient positive count until objective adjudication resolves the report. |
| WT-009 | Worst-valid checkpoint replay, maximum concurrent assigned challenges, and one-unit-over inputs respect deterministic CPU/heap/disk/network caps. Rate limiting cannot suppress required positive coverage or objective adjudication. |
| WT-010 | Every rooted slash and cooldown record proves its actual physical key equals the canonical signer/shard/disputed-checkpoint or operator/cooldown identity decoded from the value. Wrong-key placement, duplicate logical identity under two keys, signer/scope substitution, malformed value, and map-order collision reject before evidence, roster, reward, or eligibility effects. Live replay, restart, and density reorg cannot redirect, omit, duplicate, or escape a slash/cooldown. |

### 4.7 Cross-metagraph and cross-shard settlement

| Test ID | Required scenario and assertion |
|---|---|
| XMG-001 | Phase-0/1, best-tip, ordinal-only, wrong-hash, orphaned-P2, and self-claimed origins defer/reject. Exact canonical P2 origin succeeds. |
| XMG-001A | An older exact P2 ref that remains on the canonical branch and within canonical staleness/retention bounds succeeds; “latest head only” and receiver-local age policies cannot change validity. |
| XMG-002 | Same authorization is consumed concurrently by two metagraphs/shards/GL0 candidates. Canonical global ordering selects one winner and writes one permanent nullifier. |
| XMG-003 | Consume versus cancel/expiry/refund/local spend and inbound delivery versus local spend follow the canonical total order still to be completed under O-13's engineering gate after every input permutation. |
| XMG-004 | Settlement balances/reservation/nullifier/delivery append are atomic. Crash at every write point has all or none. |
| XMG-005 | ML0 acknowledgement loss, duplication, reordering, bounded-history eviction attempt, and restart cannot duplicate/erase/refund settlement. Pre/post-ack effective balances are identical. |
| XMG-005B | A per-MG hash-linked delivery sequence, rooted pending leaves, and durable ML0 applied cursor replace ordinal-history reconstruction. Sparse no-delivery GL0 ordinals and an older still-pending delivery discovered after a higher GL0 ordinal preserve their GL0-assigned contiguous sequence; gaps, siblings, stale acknowledgements, former depth limits, restart, bootstrap, and compaction cannot reapply or skip it. |
| XMG-005A | Checkpoint mirror root remains the signed replay result while GL0 global settlement writes a separate overlay. Effective-state reads equal the exact composition; inbox-before-local-spend and acknowledgement compaction preserve byte-identical effective balances. |
| XMG-006 | P2 density reorg unwinds source/consume/nullifier/delivery/dependent local spend consistently, requeues valid intent once, and replacement replay produces exact state. |
| XMG-007 | Per-MG diff cannot directly win a cross-MG conflict or write global nullifier/inbox. Every GL0 node's global kernel returns the same winner. |
| XMG-008 | Same trace at shard counts 1, 2, and K has identical accepted IDs, balances, locks, supply, nullifiers, inbox, acknowledgements, and MPT root. |
| XMG-009 | Token-lock/transfer/custom interaction types remain rejected/disabled until each has a registered type-specific oracle and the XMG suite. |
| XMG-010 | Each binary in a multi-MG checkpoint executes global reads against its signed exact nondecreasing P2 ref while local MG state threads through its segment. Receiver live head, peer response, arrival order, and other MG segment order cannot change the diff, intents, or root. |
| XMG-011 | A GL0 protocol correction to one MG cannot be synthesized by another MG's diff or settlement intent. Subsequent cross-MG reads see it only through the exact canonical P2 ref and reorg with that ref. |
| XMG-012 | A cross-shard no-ref balance read proves the exact owner metagraph field-25 `MgBalances` key used by same-shard reconstruction, decodes `(Address,Balance)`, and requires the embedded address to equal the requested key. Membership at 100 succeeds exactly as same-shard validation; authenticated absence means zero; field-5, wrong-address, wrong-type, malformed, and tampered proofs reject. Tests must build the canonical field-25 leaf and cannot synthesize the obsolete field-5 key. |
| XMG-013 | Every `ConsumedAllowSpend` leaf proves its actual physical nullifier key equals the canonical semantic consume identity decoded from the value before it enters the permanent spent set. Arbitrary-key placement, wrong metagraph/scope, two physical keys for one consume, one key carrying a different consume, duplicate/replay, malformed bytes, restart, compaction, and density reorg cannot omit, reclassify, or apply the consume twice. |
| GROWTH-001 | Long-running authorization/nullifier/inbox/evidence traces remain within the ratified storage/economic bound. Compaction preserves replay rejection and exact historical proof across restart/bootstrap. |

`XMG-012` is landed at `41c19903d` for the GL0-local field-25 path. It does not
close the HTTP field-7 proof path: the validator requests metagraph-scoped
`ActiveAllowSpends`, while the current per-MG proof entry set/root excludes field
7. Before committee execution uses that route, add adversarial vectors for the
ratified construction: either field-7 leaves are included in the signed complete
per-MG root, or the response proves them against the signed complete global root.
Missing value, wrong scope/source/key/path/root, stale/self-claimed checkpoint, and
cross-MG substitution reject before the allow-spend is usable.

### 4.8 Payload lanes and data availability

| Test ID | Required scenario and assertion |
|---|---|
| LANE-001 | Exact signed lane/schema/network/genesis/era vectors. Unknown/trailing/ambiguous payloads reject; decoder success cannot change lane. |
| LANE-002 | Currency-shaped custom bytes remain custom. Custom application output cannot synthesize framework writes. If an independently signed framework fee/intent binds the custom commitment, changing bytes rejects that bound intent atomically; otherwise only custom commitment/availability state changes. |
| LANE-003 | Currency-only and currency-with-data MGs progress together on one/many shards through checkpoint, P2, density reorg, retention maturity, and downstream adoption. |
| LANE-004 | O-09's retained pure opaque/data-only lane progresses through authenticated registration, custody, availability, ordering, P2 inclusion, density reorg, restart, and recovery with zero framework-economic writes. Currency-shaped bytes remain opaque; decoder success, ML0 output, admission receipts, and DA signatures cannot promote the lane or satisfy execution authority. |
| META-001 | Duplicate/colliding metagraph ID or namespace, wrong owner/ML0 set, unauthorized/immediate lane/schema/fee/DA upgrade, stale registration ref, and opaque-to-currency state import reject. Delayed authorized upgrade follows the exact state transform. |
| DA-001 | Missing chunk/body before execution/signing/eligibility defers. Reconstructed content matches commitment exactly before use. |
| DA-002 | Maximum valid and one-byte-oversize content/chunks, invalid erasure proof, decompression bomb, and cardinality overflow reject within deterministic resource bounds. |
| DA-003 | Signer/watchtower/serving retention survives partition, restart, P2 reorg, challenge, downstream retrieval, and every configured horizon. A production-capacity profile exercises recommended `k2`; a shorter local history enters authenticated recovery sooner. Pruning metadata never becomes a validity or fork-choice floor. |
| DA-004 | Domain-separated custody proof claims possession/retrievability only and cannot satisfy an execution-signature threshold. |
| ADMIT-001 | A selected member validates ML0 source signatures against the exact MG registry epoch, lane/schema/size, authenticated parent hash, and successor ordinal, then durably stores exact bytes before emitting a typed receipt. Reordering any step is a RED failure. |
| ADMIT-002 | Admission rejects subthreshold/duplicate/nonmember/wrong-eta/wrong-parent/wrong-source/oversize/equivocating envelopes, cannot count toward execution quorum, enqueues one valid parent-contiguous binary, and remains live under the ratified censorship fallback. |
| ADMIT-003 | Producer-supplied ordinal/eta/key grinding cannot select a favorable admission set. Selection derives from authenticated parent state; lifetime/redraw and exact registry are identical after restart/reorg. Mutable node-local peer state cannot change validity. |
| ADMIT-004 | Receipt and execution signature have disjoint codecs/domains/APIs. Cross-decoding, signature reuse, threshold mixing, and UI/metric misclassification reject even over identical binary/checkpoint hashes. |

### 4.9 Downstream, GSI deletion, and recovery

| Test ID | Required scenario and assertion |
|---|---|
| FOLLOW-001 | GL1/ML0/CL1/DL1 consume exact P2 refs. Same-ordinal hash replacement is detected and never mistaken for monotone progress. |
| FOLLOW-002 | ML0 handles orphaned `globalSyncView` by the ratified rewind/rebase rule; CL1/DL1 converge without independently overriding GL0. |
| FOLLOW-003 | Checkpoint windows based on orphaned P2 refs invalidate; inputs requeue exactly once; replacements replay/sign/adopt normally. |
| FOLLOW-004 | Mixed binary `globalSyncView` windows obey the ratified equality or nondecreasing historical-context rule. Expiry/epoch/authorization/cross-MG reads are identical on every signer and never use receiver live head. |
| FOLLOW-005 | Currency-with-data ML0 state whose P2 dependency is orphaned appends its registered deterministic rebase or starts a new epoch at the last valid ref. A noninvertible app must expose its external confirmation-risk policy explicitly. |
| FOLLOW-006 | A GL0 protocol correction is delivered once as a mandatory exact-hash rebase input to ML0/CL1/DL1; a correction reorg reverses/replaces it without accepting a metagraph-originated substitute. |
| FOLLOW-007 | A missing/error Phase-2 read prunes no ML0 binary. Explicit P2 observation is keyed by exact containing hash and retains confirmed and older Pending inputs through replacement/recovery/ack horizons; same-ordinal or deeper density replacement reverses monotone GSI watermarks and requeues each orphaned binary exactly once. |
| FOLLOW-008 | Umbrella gate: the complete `FOLLOW-008A` through `FOLLOW-008P` matrix in `P6-FIN14-PHASE2-CONSUMER-LEASE.md` passes through the pure model, live gate adapter, admission/cache/tally/shard-buffer path, execution signing, GL0 inclusion, restart, and multi-process chaos harness. A Boolean/ordinal adapter or test-only lease constructor reachable by production consumer code is a hard failure. |
| FOLLOW-008A | Let A qualify through decided `T_weight` at ordinal N, then density-replace it with unqualified B at N. B acquires no lease or derivative; A's ordinal, release generation, and evidence never transfer. |
| FOLLOW-008B | Repeat `FOLLOW-008A` with A qualified by canonical `k1` depth. The fallback rail changes evidence verification only and gives B no inherited authority. |
| FOLLOW-008C | Replace after the first short acquisition capture, during each unlocked snapshot/MPT/semantic/anchor readback, and immediately before the final descriptor CAS. The final CAS returns stale/retry and no mixed lease is minted; no finality lock spans image verification. |
| FOLLOW-008D | Replace after acquisition, during candidate signing, and before outbox/tally commit. Candidate bytes may be discarded, but zero signature is recorded, counted, or published and no tally side effect occurs. |
| FOLLOW-008E | Replace after received-attestation verification and immediately before tally record. The scoped record CAS fails; the new lineage count is unchanged. |
| FOLLOW-008F | Replace during every committee poll position, including immediately before threshold success. No finality/MPT/chain lock is held across polling, and the old scoped result cannot commit. |
| FOLLOW-008G | Replace after threshold success and before each admission queue, continuity cache, orphan drain, and shard-buffer write. `commitIfCurrent` leaves every sink unchanged. |
| FOLLOW-008H | Crash after the scoped admission intent and before/after each sink write, replace, then restart. Durable ordering makes every stale entry inert and requeues a still-valid input at most once. |
| FOLLOW-008I | Extend the same canonical tine throughout a wait. An exact-ancestor scope may commit only after revalidation; a latest-P2-head scope must reacquire. Descendant extension alone does not create an admission restart loop. |
| FOLLOW-008J | Replace above a lease target that remains below the MRCA. The old lineage lease still fails; reacquisition succeeds only after the target and every purpose/readback fact reverify under the new lineage revision. |
| FOLLOW-008K | Select A, replace it with B, then select A again. Monotone lineage revision prevents ABA reuse of A's old lease, tally, threshold, cache, replay, or permit. |
| FOLLOW-008L | Retain raw binary and deferred-attestation bytes across replacement. They remain inert until full re-resolution/reverification; no cached context, ordinal, sender count, or threshold transfers. |
| FOLLOW-008M | Build a multi-MG checkpoint window over nondecreasing exact P2 refs, then orphan one ref. The affected atomic unit defers/rebases; another valid binary or MG cannot lend it Phase-2 authority. |
| FOLLOW-008N | Replace after replay/before execution signing and after signing/before GL0 inclusion. The first emits no signature; the second retains historical bytes only and cannot satisfy current inclusion without full certificate/base revalidation. |
| FOLLOW-008O | Evict target bytes while leased work is in flight. Commit defers or enters authenticated recovery and never substitutes the mutable current base or interprets eviction as canonicality evidence. |
| FOLLOW-008P | Race replacement with 100 identical consumer commits. Linearization orders idempotent old-generation commits before invalidation or returns stale after it; no duplicate admission, shard, economic, serving, or requeue effect occurs. |
| MEMPOOL-001 | Global/shard P1 and P2 reorg reinserts each orphaned still-valid native/framework input once, never resurrects a conflicting/finalized input, and produces the same result across restart and arrival order. |
| GSI-001 | During migration tests, typed MPT readers equal GSI for every representable field and absent/empty shape. |
| GSI-002 | Native MPT-only finality, checkpoint, nullifier, inbox, evidence, and era records survive restart/catch-up without GSI projection. |
| GSI-003 | Production denylist reports zero `GlobalSnapshotInfo`, `GlobalSnapshotWithState`, `syncFromGlobalSnapshotInfo`, GSI rebuild, and peer-GSI install authority/fallback references. |
| BOOT-001 | With exact Phase-2 F, a valid best tip H>F, and same-ordinal sibling responses, cold ML0 and GL1 accept only one bundle whose FinalityGate-resolved `(ordinal,hash,parentHash,mptRoot,evidence)` and content-addressed payload identity all name F. Reordering, splitting responses across peers, or observing F's ordinal before fetching H never installs H or mutates any follower store. |
| BOOT-002 | Honest persisted MPT bytes for S combined with a stale, substituted, or sibling GSI/projection reject before every store. The accepted typed projection is deterministically derived from the bundle bytes or carries a complete proof against the same S root; ECO-F32's optional replay witness is bound to that same identity. |
| BOOT-003 | At active-era boundary-1, boundary, and boundary+1, bootstrap recomputes and compares the entire canonical selector-chosen proof. Legacy, MPT, missing, mixed, and `Some(mptRoot)` cross-era shapes cannot select their own validator or bypass a committed field. |
| BOOT-004 | Fresh ML0 and GL1 joins start with no preserved bytes while the Phase-2 state has nonempty native consumed-allow-spend and slashing partitions. ML0 receives complete exact bytes; GL1 receives only its explicitly enumerated, signed/proved consumed-field slice. Each reproduces its complete declared contract or remains unmutated in `RecoveryRequired`; neither degrades to GSI re-encoding. |
| BOOT-005 | Inject wrong root, truncated bytes, exception, cancellation, and process crash before and after every bootstrap install stage. MPT image, exact snapshot anchor, projections, balances, references, cursors, and recovery markers expose either the prior complete generation or the verified next generation, never a mixed state. |
| ROOT-001 | Every retained signed state commitment, including `smtRoot`, changes when its owned state changes and is verified on production, follow, restart, catch-up, and bootstrap. Any non-load-bearing commitment is absent from the schema. |
| ROOT-002 | The mutable MPT base is usable only through a root-verified `(snapshotHash, ordinal, mptRoot)` anchor restored from authenticated snapshot plus reproduced persisted bytes. Hash-only, ordinal-only, wrong-root, crash-window, reset, and peer-claimed anchors leave production in `RecoveryRequired`. |
| ROOT-003 | One exact-parent session owns all acceptance reads, replay, prefix/raw views, writes, root construction, and commit. Unknown/evicted parent, incomplete ancestry, concurrent finalize/evict/reset, and sibling substitution fail before callback/replay/write. Unknown finalization is no-mutation; known ancestor finalization retains canonical descendants. |
| ROOT-004 | Global finality preflights exact canonical overlay availability and performs the coordinated fold before exposing completion. At every injected failure, tip tracker, chain store, sidecar outbox, finalized watermarks/slices, signed-byte promotion, tower, local event publication, and overlay are either consistently advanced or explicitly recoverable from one durable intent. Restart with an already-folded base and restart with pending descendants both resume from an authenticated anchor without treating the canonical hash as unknown. |
| ROOT-005 | Persistence captures immutable state inside the exact-parent transaction and publishes generations monotonically. Force `persist(N)` to pause, publish `N+1`, then resume N: N never contains N+1 bytes, never deletes N+1, and no watermark reports either generation before forced atomic write plus digest/root readback. Inject cancellation and failure before/after temp write, rename, file/directory force, manifest CAS, and readback; readers observe only the prior or fully verified next generation. |
| ROOT-005A | **Focused primitive landed:** capture one immutable branch-byte image and derive its complete consensus-root image, root, proofs, and values without rereading mutable state (`MptOverlay.scala:46-84,286-297,953-966`; `GlobalFollowProofService.scala:65-112`). Every consumed field/proof/value is present, bounds equal verifier-derived bounds, leaf values match, and evidence-free success requires the canonical empty root (`FollowVerifyCore.scala:585-683`; `MerklePatriciaRangeVerifier.scala:259-268`; `GlobalFollowProofServiceSuite.scala:180-190,195-415`; `MptOverlaySuite.scala:852-904`; `MerklePatriciaRangeVerifierSuite.scala:87-123`). |
| ROOT-005B | **Activation gate open:** bind the captured image to an authenticated exact `(ordinal,hash,root)` inside the exact-parent transaction and make base writes, overlay writes/finalization, proof capture, persistence, reset, and replacement share one mutation owner/generation. Current code expressly lacks exact-parent authentication and direct-base serialization; `HistoricalMptProofService` ignores its ordinal, unknown branch IDs stop at and expose the base, and GlobalFollow stamps a caller ordinal on the captured image (`MptOverlay.scala:46-51,65-67,286-297,1531-1538`; `HistoricalMptProofService.scala:40-52`; `GlobalFollowProofService.scala:39-47,68-90`). RED tests request unknown, evicted, sibling, same-ordinal replacement, and deliberately wrong-ordinal branches while pausing capture across finalize/reset/replacement. Every case must return typed unavailable/mismatch or the one exact requested generation; a stale/base/mixed image cannot be served, signed, persisted, or labeled as another ordinal. |
| ROOT-006 | Field-7 active-allow-spend prefix scans decode every matched base and branch-upsert value in sorted key order or fail closed. Reconstruction requires a nonempty set with one embedded scope/source and proves that its recomputed canonical key equals the actual MPT key; wrong source, wrong scope, mixed/empty set, and undecodable bytes never disappear or reclassify. This gate is limited to field 7 and does not certify other value-only reconstruction paths. |
| ROOT-007 | Freeze complete root ownership before parser migration. For every `SystemNamespace` active/expiry index and every framework field, mutate only that state and prove either (a) it is canonical writable state whose change alters the signed complete root and bounded diff, or (b) it is a nonauthoritative derivative excluded from writes/ingress and rebuilt solely from root-authenticated records inside the exact-parent session. Two stores with identical rooted records but different root-invisible indices must produce identical typed views, expiry decisions, economic writes, and next roots or one must halt before mutation. No writable unrooted field remains. |
| ROOT-008 | One shared strict physical-entry reader implements the exhaustive ID 0-34 manifest in `ROOT-008-GL0-PARTITION-GRAMMAR.md`: it retains `(actualHexKey,rawBytes)`, consumes the entire bounded codec, requires canonical re-encoding, derives canonical key/scope, compares byte-exact identity, rejects duplicate logical identities before map/set construction, and runs complete address/pair/expiry/currency/KES/history structural and population relations. It does not prove economic authorization, conservation, backing, replay protection, or transition validity; O-07/ECON-G and the P2 oracle must independently pass. The landed transport-level suite must continue proving deterministic key order, immutable byte ownership/value equality, null/empty/malformed retention, nibble-prefix case-alias retention, prefix exclusion, prefix-only branch removal/upsert merge, and in-memory/filesystem parity; those cases alone do not pass this gate. Before partitioning, ROOT-011 rejects uppercase, odd-length, invalid-digit, nibble-equivalent aliases, and terminal-prefix collisions; ROOT-008 then enforces exact field lengths/slots. Generated `ROOT-008-F00..F34` negatives cover wrong key/scope, forbidden/unknown IDs, duplicate identities, mixed/empty sets, malformed/trailing/noncanonical bytes, over-limit images/partitions/values/collections, and every owned relational asymmetry. Canonical positive vectors exist only for active target IDs; denied IDs are rejection-only. No `entries.values`, right-biased `toMap`/`SortedMap`, filter/drop, normalization, or `headOption` path can silently choose consensus state. O-17's directions are ratified; its remaining exact identities, codecs, numeric resource limits, and proofs block schema activation. |
| ROOT-009 | Peer state-proof, persisted-disk, shallow/deep reorg, restart, catch-up, and bootstrap loaders first verify the complete root, discard every root-invisible imported index/cache, and rebuild permitted derivatives through ROOT-008 from the exact authenticated parent. For each malicious variation of active-address and all expiry indices, clean replay and every recovery path yield byte-identical typed state, transition decisions, diffs, and next roots. Missing rebuild inputs, physical-key mismatch, or duplicate logical identity enters `RecoveryRequired` before any store/finality/watermark mutation. |
| ROOT-010 | Field-32 migration is order-enforced. First, the signed/root-bound framework replay witness supplies the exact optional full `globalSnapshotSyncView` preimage for the checkpoint pre-state and every resulting snapshot boundary plus the explicit ML0 operator population used for sync validation. Each preimage byte-hashes to the corresponding `CurrencySnapshotStateProof.globalSnapshotSync`; `None` and `Some(empty)` have distinct vectors. The current hash-plus-accepted-delta binary alone fails the RED reconstruction case. Only after staged/backfilled/restart/reorg replay parity passes may GL0 delete field-32 keys. Thereafter all GL0 writers, diffs, loads, catch-up, compaction, and reorg paths contain zero field-32 entries and reject attempted ingress, while ML0 `CurrencySnapshotInfo.stateProof` continues to change with the exact optional view. |
| ROOT-011 | **Activation target, not current closure:** complete physical-key preflight occurs before trie construction and before any live-store mutation. Uppercase, odd-length, invalid-digit, duplicate-nibble-path, and terminal/prefix-colliding keys (including `aa03`/`AA03` and `aa`/`aa00`) fail with a typed error within a deadline; no normalization or last-write merge is allowed. Full build, incremental insert, remove, raw network/disk load, pinned backfill, restart, and reorg leave the prior bytes/root/anchors unchanged on rejection. The parallel builder independently detects a nonshrinking group and fails boundedly, so bypassing the boundary cannot hang a node. Canonical candidate images produce byte-identical roots across full and every insertion/removal order. Current worktree coverage is partial: exact duplicate JSON members collapse before validation, mutation ownership is not unified, raw resource bounds are not frozen, and pinned/network/disk/reorg plus production-scale integration remain required. |
| REC-001 | Corrupt/missing bytes at each retention boundary cause authenticated exact-hash fetch or halt before mutation, never local synthesis. |
| REC-002 | Crash/restart at checkpoint production/signing/store, diff apply, GL0 compose, P2 transition, retention update, tower/SMT update, density unwind/refold, correction, outbox, evidence, ack, and prune returns exact state. |
| REC-003 | Genesis sync, authenticated-anchor sync, long offline catch-up, sibling recovery, and multi-peer disagreement produce exact canonical bytes or fail closed. A common ancestor older than local `k2` data exercises verified fetch/rebuild. |
| REC-004 | Durable tower and historical-SMT enumeration rejects the entire image on a malformed, truncated, oversized, noncanonical, duplicate, out-of-range, or trailing-byte key, and rejects a negative replay lag or overflowing derived version before replay mutation; it never `flatMap`s/filter-drops an entry and continues with reduced history. Inject each defect plus failure/cancellation at every subsequent version-fold step across restart, compaction, shallow/deep density reorg, proof serving, and catch-up. Separately inject failure/cancellation after steady-state `durable.commit(N)` and before/inside live `versioned.commit(N)`: readers observe the complete prior or complete next generation only, later append cannot extend a stale tree, and recovery/replay plus the next append equal a clean uninterrupted build byte-for-byte. The node enters authenticated recovery before publishing a root, tower proof, finality phase, or eligibility result. |
| REC-005 | Peer, disk, reorg, catch-up, and cold-bootstrap raw-state loads use the same staged transactional installer as BOOT-005. In particular, no `clear`/`loadBytes`, snapshot/ref update, or recovery marker becomes visible before full bundle/proof verification and the durable all-or-none commit; failed verification and restart preserve the exact prior root and anchor. |

`REC-004` is partial in the current worktree. The total canonical key grammar,
one-time complete tower key/value validation with indexed hot reads/updates, and
historical strict-image validation are green in focused suites
(`DurableNipopowKey.scala:32-42,44-115`;
`MptTowerStore.scala:101-108,159-262`;
`MptTowerStoreSuite.scala:258-456`). Historical replay
requires the exact contiguous range `0..expectedEligible`, including ordinal
zero, preflights every `ordinal + k`, builds a fresh derived tree, and swaps it
only on success; gaps, malformation, hashing failure, and cancellation preserve
the prior complete tree (`HistoricalCommitmentSmtStore.scala:179-264`;
`HistoricalCommitmentSmtStoreSuite.scala:293-337,349-559`). The former detached
replay tests do not cover the steady-state split publication at
`HistoricalCommitmentSmtStore.scala:150-168`: durable KV publication precedes the
live versioned-tree commit and lacks a failure/cancellation all-or-none test. The
former detached
bounded skip-ahead loop and startup provider publication are removed from live
wiring, which now explicitly keeps the provider absent
(`GlobalSnapshotConsensus.scala:1510-1514`). The staged serialized exact-hash
replacement remains in-memory; every bounded run walks the full remaining
tip-to-cursor path before slicing, so repeated catch-up is `O(M^2/B)`
(`TowerCatchupCoordinator.scala:20-36,216-312,372-513`). Public finalizer calls
also lack one prepare/commit writer generation (`TowerFinalizer.scala:89-96,180-213`),
and proof construction reads by ordinal then injects a tower hash without exact
rehash while reading mutable head (`TowerProofBuilder.scala:96-103,148-166`).
Routes stay unavailable/503, including the otherwise independent pure verifier
(`NipopowRoutes.scala:82-87,154-168`;
`NipopowRoutesSuite.scala:101-120,237-242`). Durable-KV crash
atomicity, production boot/disk wiring, restart/compaction, exact-hash density
reorg, single-writer finalization, paged linear catch-up, target-bound bounded
proof construction, verifier/builder service separation, authenticated clean
rebuild, and atomic cursor/finalizer/store/provider publication all remain required.

### 4.10 Permissionless network and resources

| Test ID | Required scenario and assertion |
|---|---|
| NET-001 | Every topic/RPC/HTTP endpoint enforces hard message/range/cardinality/concurrency/rate/decompression bounds before expensive verification or storage. |
| NET-002 | Gossip buffer drop, duplicate suppression, first-publication loss, sidecar/JVM restart, durable outbox retry, and O(shards) fan-in remain bounded and eventually recover required exact artifacts. |
| NET-003 | Wrong network/genesis/topic/domain, unregistered identity, replayed proof, dynamic-topic abuse, and unauthenticated recovery bytes reject. |
| NET-004 | Eclipse/partition/malicious recovery peers cannot change roots/finality/evidence/committee state; scored multi-peer exact-hash recovery remains live under stated connectivity. |
| NET-005 | Missing-parent checkpoint recovery performs zero fetch/store/replay for an unauthenticated or nonmember producer. Valid and compromised-member unique-parent floods remain within canonical per-peer/global rate, in-flight, timeout, and bounded-expiring dedup limits; recovery of an honest missing parent still progresses. |
| NET-006 | A valid child received before its parent is retained under count and encoded-byte caps, ancestry recovery is bounded and authenticates every fetched ancestor before the next fetch, and the child re-enters full duty/signature/replay validation after the exact parent stores. Parent, grandparent, fetch-failure, depth-limit, duplicate, eviction, and same-content GossipSub-suppression cases cannot silently drop the only child receipt. |
| PERM-001 | Join/activation/exit/unbond/slash/rotation across partitions/restart preserves bonded backing, challenge horizon, and anchored sample/committee sets. |
| PERM-002 | Bootstrap policy states maximum safe offline time and validates the exact trusted genesis/cached anchor, density proof, set/era/parameter history, and current-chain evidence. Expired or incomparable anchors reject. |
| PERM-003 | For period `N`, producer, verifier, restart, reorg, and portable-proof paths derive byte-identical eligibility from one exact parent: authorized population/raw weight and active paired records from `N-2`, eta from `N-1`, and parameters for `N`. Same-ordinal sibling bytes, mixed roster/weight roots, local seedlist, current stake, observed peers, receiver head, and missing-history fallback reject or defer before any consensus side effect. |
| PERM-004 | After O-11's exact rooted backing predicates are derived and frozen, qualification vectors cover key-only, stake-only, profile-only, below-minimum, many-PeerId stake splitting, permitted and forbidden third-party delegation/collateralization, activation/exit boundaries, cooldown expiry, and unbond attempts through the last slash/challenge horizon. Reused v4 event/accounting behavior remains deterministic, while its seedlist authorization has zero consensus authority. |
| PERM-005 | Every delegated-stake, stake-withdrawal, node-collateral, and collateral-withdrawal record proves the actual physical owner/scope key equals the canonical identity embedded in the record before it affects backing, weight, rewards, exit, refund, or slashability. Wrong-owner/scope placement, duplicate logical record under two keys, mixed sets, malformed bytes, and expiry-index disagreement reject or rebuild from rooted records. Producer, verifier, restart, exact-hash reorg, and recovery derive byte-identical backing and eligible weight. |
| LIGHT-001 | Independent light client verifies tower/density comparison, set/era/parameter transitions, and MPT inclusion/absence from genesis or an allowed cached authenticated anchor, then follows a later denser replacement. |
| RESOURCE-001 | Worst-valid and one-unit-over snapshot/checkpoint/binary/diff/proof/state-growth inputs have deterministic cost and all-or-none effect. |

### 4.11 Snapshot-to-genesis migration

| Test ID | Required scenario and assertion |
|---|---|
| MIG-001 | Exporter verifies exact upstream network/genesis/finalized ordinal/hash/root/evidence and rejects nonfinal/wrong-network snapshots. |
| MIG-002 | Transform has an explicit disposition for every source namespace/field, including balances, supply, locks, reservations, live authorizations/nullifiers, registrations, and custom data. |
| MIG-003 | Independent implementations produce byte-identical ScodecV1 manifest, MPT, root, and new ordinal-0 genesis. |
| MIG-004 | Conservation report proves imported value plus declared conversions/burns equals source state; no unknown value appears/disappears. |
| MIG-005 | New network/genesis domain rejects old messages/signatures/replays while preserving intended addresses/ownership according to the ratified migration policy. |
| MIG-005A | Every source-signed live authorization/order/delegation follows its declared expire/refund/inert/reauthorize rule; no old-domain signature can create a new-chain spend. |
| MIG-006 | Full cutover rehearsal covers source freeze, export, public verification, launch, restart/bootstrap, abort procedure, and post-launch comparison. |

## 5. Assignment manifests and merge gates

Each agent gets a checked-in or generated manifest with:

```text
packet
baseline
writeSet
invariantIds
findingIds
testIds
dependencies
commands
seed/faultSchedule
artifactDirectory
integrationOwner
```

Default write sets are disjoint. Shared hotspot patches are reviewed and applied by
the wave integration owner. Model/RED agents do not modify runtime. Runtime authors
do not certify their own qualification packet.

| Test families | Default packet | Artifact directory |
|---|---|---|
| `ARCH-*`, decision ledger | P0 | `target/consensus-audit/P0/` |
| `SER-*`, `ERA-*`, codec part of `CRYPTO-*` | P1 | `target/consensus-audit/P1/` |
| `ECON-*` and economic part of `XMG-*` | P2/P9 | `target/consensus-audit/P2/`, `P9/` |
| `FIN-M-*`, `FIN-D-*` model | PF | `target/consensus-audit/PF/` |
| `SIG-*` | PSIG plus P6/P8 for their owned GL0-finality/shard paths | `target/consensus-audit/PSIG/`, `P6/`, `P8/` |
| `KEYREG-*`, stake/KES/evidence | P4 | `target/consensus-audit/P4/` |
| `WT-*` | P4/P8 | `target/consensus-audit/P4/`, `P8/` |
| `LANE-*`, `DA-*` | P5 | `target/consensus-audit/P5/` |
| `FIN-W-*`, `FIN-S-*`, `TOWER-*`, runtime cascade | P6 | `target/consensus-audit/P6/` |
| `DIFF-*`, checkpoint codec vectors | P7 | `target/consensus-audit/P7/` |
| `SHARD-*`, `ADMIT-*` | P8 | `target/consensus-audit/P8/` |
| `CORR-*` | P2/P6/P10 | `target/consensus-audit/CORR/` |
| `FOLLOW-001..007`, `GSI-*` | P10 by module, with P8/P9/P11 supplying their owned shard/settlement/recovery integration | `target/consensus-audit/P10/` plus owning packet directories |
| `FOLLOW-008A..P` | P6 lease kernel/integration; P8 admission/shard/watchtower/checkpoint adapters; P10 follower/economic-read adapters; P11 serving/recovery adapters | `target/consensus-audit/P6/`, `P8/`, `P10/`, `P11/` |
| `BOOT-*`, `REC-*`, `NET-*`, `PERM-*`, `LIGHT-*`, `RESOURCE-*` | P11 | `target/consensus-audit/P11/` |
| `MIG-*` | P12 | `target/consensus-audit/P12/` |
| all release gates | P13 | `target/consensus-audit/P13/` |

## 6. Execution waves

| Wave | Parallel work | Gate |
|---|---|---|
| W0 | P0 decisions, architecture guard, finding ledger, RED harness | All O-01 through O-17 owner directions are ratified; dependent work remains blocked wherever the register names an implementation, schema, parameter, research, RED-vector, or proof gate. |
| W1 | P1 serde, P2 oracle, PF finality model, P4.0 atomic inventory, P4.1 immutable-genesis population cut, P4 activation/stake/evidence model, P5 lane/DA contract, P11 transport RED corpus | Rooted genesis pair plus separately authorized immutable population, unified registration bytes, delayed-activation/reorg vectors, and shared contracts/models freeze; no runtime registration becomes eligible. |
| W2A | P6 FinalityGate/tower core, P5 DA/intake runtime, P4.2 O-11/rooted roster contract, P4.3 exact historical resolver, PSIG GL0/ML0/GL1 signing boundaries; P7 pure diff/root/codecs plus ROOT-007 root ownership and ROOT-010 field-32 witness contracts/RED vectors | Genesis consumers pass the immutable safety cut. Runtime work proceeds only after O-11's ratified structure has executable schemas, parameters, and proofs; then same-ordinal wrong hash/root, historical rotation, recovery, identity, phase, non-shard signing, complete-root ownership, and staged-versus-backfilled field-32 divergence are explicit gates. |
| W2B | P6 atomic integration/recovery hooks and exact Phase-2 bootstrap-bundle API, P4.4 exact Phase-2 key-context references, then P7 one-checkpoint execution/result integration including ROOT-010's exact replay witness | Phase/reorg events, exact refs, bundle payload identity, view-preimage hashes, and explicit ML0 populations are load-bearing before checkpoint or follower base/anchor merge; ordinal-only, latest-after-finalized, wrong-root, missing-witness, and proof-subset-population paths are RED. |
| W3A | P4.5/P4.6 shard/watchtower/tower/evidence consumer and fixture migration plus P8 shard protocol/watchtower/adjudication | Cross-consumer key parity/inventory, replay, one-outstanding batching, adoption, positive coverage, challenge, reorg, and false-slash gates pass. |
| W3B | ROOT-011 preflight first; then ROOT-008 registry/parser/RED development for final rooted fields in parallel with ROOT-010 witness work, followed by P9 XMG-013 nullifier and P10 PERM-005, WT-010, ECON-G-002 consumer migrations | The manifest interface may stabilize provisionally, but target activation/schema freeze waits for implementation of O-17's ratified structural directions, derived numeric bounds, field-32 deletion, and `ROOT-008-F00..F34`. Each disjoint migration passes its physical-key and relational RED corpus. |
| W4A | After ROOT-010 staged/backfill/restart/reorg witness parity is green, P10 removes and denies field 32 throughout GL0; then the final ROOT-008 manifest/consumers activate and P11 wires only a ROOT-011/008-verified exact Phase-2 bundle into the shared transactional installer before BOOT-001..005, ROOT-009/010, and REC-004/005 at shard counts 1, 2, and K | Cold join, P2 reorg, protocol correction rebase, zero field-32 GL0 ingress/storage, complete native payload/slice proof, root-invisible ingress stripping/rebuild, durable-key corruption, tower restart, and exact catch-up are all-or-none; only then does the target state schema/GSI denylist freeze. |
| W4B | P12 offline migration against frozen target schema | Independent transform/root/conservation gates pass. |
| W5 | Additional explicitly specified economic interaction types and permissionless/light-client integration | Every enabled type has its oracle and full fault suite. |
| W6 | P13 independent qualification | Exact candidate passes every applicable catalog test. |

## 7. Commands and artifacts

Focused Scala modules run first, then affected downstream modules. Full baseline:

```bash
sbt shared/test nodeShared/test dagL0/test dagL1/test currencyL0/test currencyL1/test
```

Sidecar baseline:

```bash
cd p2p
go test ./...
go test -race ./...
go build ./...
```

Cluster scenarios must cover at least shard counts 1, 2, and K with multiple
currency and currency-with-data metagraphs, named fault schedules, and exact state
comparison. Each run archives commit, manifest/parameter hashes, command,
environment/platform, seed, fault schedule, accepted/rejected IDs, snapshot refs,
phase events, checkpoint roots/diffs/signers, nullifiers/supply, memory/disk high
watermarks, and artifact hashes.

A log line containing `FINALIZED`, `Accepted`, `quorum`, or `fraud proof emitted`
is not a pass. The checker verifies exact capability transitions and forbidden
transitions did not occur.

## 8. Release decision

P13 has no authorship in P1-P12 closing changes. Release requires every enabled
catalog test, zero unowned/open CRITICAL/HIGH findings, exactly one active GL0
chain engine, zero blind state-signature APIs, zero universal ordinary-adopter CL1
replay, zero GSI authority/fallback, one active ScodecV1 genesis era, and a source
re-audit of every sign/adopt/phase/serve/recover path.
