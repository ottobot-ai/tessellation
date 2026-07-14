# Nakamoto Consensus — Status & Remaining Work

**Last updated:** 2026-07-14 (active epic decomposition)

---

> ### ⚠ 2026-07-10 status-sweep note — READ THIS
> This file was stale (last real edit 2026-04-05, at ~commit 62; the branch is now at 2575 commits). This sweep re-annotated status by cross-referencing **git log subjects + engineering notes**, NOT by re-reading each feature's code. Trust the flags accordingly:
> - **✅ done** — already validated, or a clear landing commit + prior confirmation.
> - **⚠ landed-per-git (unverified)** — a commit subject says it landed, but I did NOT re-verify the implementation at source this sweep. Confirm before relying.
> - **❓ unknown** — status genuinely unclear; needs a check.
> - **⏳ planned / pending** — no evidence it's done; still on the roadmap.
> - **♻ superseded** — replaced by newer design; the replacement is named.
>   Undeployed `authoritative*`, direct receipt, and compatibility schemas may be
>   deleted. The canonical execution-committee byte diff is target protocol, not
>   stale compatibility state.
>
> **Biggest change since 2026-04-05:** the project moved into execution sharding.
> The producer and every execution-committee signer independently recreate CL1
> and sign only an exact matching canonical byte diff/root. Ordinary noncommittee
> GL0 nodes verify the execution threshold, apply the pinned-base diff, and
> recompute the root; watchtowers replay as the collusion backstop. Commit
> `c610a0740` regressed this target to universal GL0 recreation and must be repaired
> selectively without restoring `authoritative*` fields. The failed shard design
> used secret stake-weighted VRF self-sortition and per-slot LDD leadership.
> Current shard v1 uses public deterministic VK-hash membership, uniform `1/N`
> over eligible GL0 operators, and hash-shuffled staircase producer duty.
>
> **2026-07-12 adversarial correction:** shard execution replay-before-sign now has
> a sealed `VerifiedShardCheckpoint` signing boundary, but the checkpoint still
> lacks the target canonical diff/intents/complete-root result and ordinary GL0
> adoption still replays. Economic safety is NOT complete. The framework transition function still accepts
> unsigned unlock/no-ref spend authority, bounded processed-history replay, and
> unbacked stake records. Optimistic/depth finality is unsafe and the claimed
> Avalanche cascade is absent. The source-cited status authority is
> `docs/review/CORRECTNESS-SECURITY-AUDIT-2026-07-11.md`; any checkmark below
> contradicted by that report is withdrawn.
>
> **Active implementation order:**
> `NAKAMOTO-PLAN.md`. The review roadmap, lifecycle, test plan, and owner register
> are source-audit and test inputs that E0 must reconcile before runtime work;
> conflicting older text is not an implementation instruction. The historical
> priority buckets and numbered items below are a component inventory, not the
> economic-deployment sequence.

---

## Active cross-shard build checklist

This is the status authority for the target protocol. Historical completion
tables below show component provenance only; they do not close an epic here.

- `[ ] PLANNED`: no target implementation claim.
- `[ ] PARTIAL`: useful source-proven scaffolding exists, but the invariant is
  not enforced end to end.
- `[ ] IN PROGRESS`: owned work has started but has not completed the close loop.
- `[x] COMPLETE`: RED test, implementation, required fault/integration tests,
  independent source audit, and closing commit are recorded.

There are currently **no complete target epics**. Full dependencies and exit
criteria are in `NAKAMOTO-PLAN.md`.

### Wave 0 - Freeze before dependent runtime edits

- [ ] **E0 IN PROGRESS - architecture and invariant freeze**
  - Reconcile ADR/review documents with `AGENTS.md`; remove target global
    BFT/QC/vote/lock/view-change language while preserving ML0-local BFT.
  - Lock exact-hash Phase 2, `maxvalid-tk` within `k1`, `maxvalid-bg` beyond
    `k1`, retention-only `k2`, single outstanding checkpoint, mandatory
    replay-backed `kQuorum`, positive watchtower coverage, and GL0-only protocol
    correction authority.
  - Preserve the ratified O-15 objective total-frontier property before fork
    choice can authorize `FinalityGate`: the same cutoff-complete published valid
    frontier and branch-authenticated parameters must produce the same head
    independently of candidate order, gossip arrival, restart, or prior incumbent.
    Close the still-open cutoff/bounded-diffusion and late-reveal semantics,
    cycle-resolution selector, frontier evidence and verifier, validator/store
    witnesses, security/liveness proof, exact-`k1` metric/equality boundary, and
    objective-tie design gates; do not hide any of them in a collection fold.
  - Assign every open CRITICAL/HIGH audit finding one owner, RED test, write set,
    dependency, and closing commit.
  - **Gate:** `ARCH-001..003`, `SIG-001..005`.

- [ ] **STOP-THE-LINE - complete rooted state and key-aware reads**
  - [x] **System-index root ownership (`ECO-IDX-01/02`):** the current worktree
    roots every `SystemNamespace` active-address/expiry entry, maintains owner
    indices for `LastCurrencySnapshotsProofs` and `MetagraphSyncData`, and makes
    known exact-key index/target reads fail closed. Expiry consumers require the
    bucket epoch to equal the decoded target's expiry, and the currency union
    rejects simultaneous legacy field-3 and incremental field-5 arms.
    `GlobalMptRootCompletenessSuite` covers the four System labels; strict
    read/RMW/replay suites cover malformed, absent-target, wrong-epoch bucket,
    and dual-arm cases. This closes the original
    same-root/different-System-index exploit, not `ROOT-008`, `ROOT-009`, or the
    full `from(mpt, ordinal, era)` projection.
  - [ ] **Fail closed on unavailable proposal-parent state:** MultiBranch point,
    prefix, root, and raw-byte reads currently stop at a missing requested branch
    or ancestor and compose against the finalized base. The fallback mechanism
    is confirmed; a reachable eviction/unknown-parent economic mismatch remains
    OPEN and PLAUSIBLE. First persist a root-verified base
    `(hash, ordinal, mptRoot)`, then make checkout/read return typed
    `ParentStateUnavailable` before mutation. Never heal the gap from GSI or the
    current finalized base. Gates: `SMT-02`, `SLASH-02`, `E9-BRANCH`.
  - [ ] **Root the expiry parameter (`ECO-IDX-03`):** the node-local
    `WithdrawalTimeLimit` currently changes rooted collateral-withdrawal expiry
    bytes for the same logical state. Resolve it from canonical active-era state
    and reject local/join mismatch.
  - Field 32 is still excluded while GL0 writes/reconstructs it and checkpoint
    replay consumes it; local staged and root-verified stripped-backfill bases can
    therefore derive different state proofs (`ECO-F32`, HIGH, confirmed).
    Stake/committee eligibility, tower activation, ordinary checkpoint diff
    adoption, and economic deployment must remain deployment-disabled until this
    entire checklist closes. This is a release
    gate, not a claim that every unsafe runtime path already has a hard-coded
    kill switch.
  - [ ] **1. Complete root and replay-witness ownership:** preserve the landed
    rooted System indices, exact expiry-epoch checks, and exclusive field-3/
    field-5 currency union; bind `WithdrawalTimeLimit` as active-era canonical
    rooted protocol state. The current local environment map changes rooted
    node-collateral-withdrawal expiry keys for the same GSI
    (`GlobalStateConverter.scala:822-847`; `dag-l0/Main.scala:108-114`;
    `MptFieldCoverageSuite.scala:325-377`). No local fallback may influence a
    consensus root.
    - [ ] **1A. Field-32 replay witness:** carry the exact optional full
      `globalSnapshotSyncView` preimage needed at every checkpoint window
      boundary, plus the explicit ML0 operator population used for sync
      validation, in the signed/root-bound framework replay input. Preserve
      `None` versus `Some(empty)` and verify the preimage against
      `CurrencySnapshotStateProof.globalSnapshotSync`; a missing/mismatched
      witness defers and cannot slash. The current binary's hash plus accepted
      sync delta is insufficient. Gates: `ECO-F32`, `ROOT-010`, `SHARD-E-006`.
    - [ ] **1B. Remove the GL0 mirror:** only after 1A is green, remove field 32
      from every GL0 MPT/diff/load/reorg path and reject it at those boundaries.
      Retain it in ML0 `CurrencySnapshotInfo` and its state proof. Gate:
      `DIFF-003`, `ROOT-010`.
    - The System-index slice of `ROOT-007` is landed; full `ROOT-007` remains
      open for every other writable field, including field 32 and the rooted
      protocol-parameter contract. Gate: `ECO-IDX-03`, `ROOT-007`.
    - [x] Optional `GlobalSnapshotStateProof` slot presence is derived only from
      authenticated byte partitions. `None` and `Some(empty)` GSI shapes produce
      one proof (`GlobalSnapshotInfo.scala:302-388`;
      `OptionShapeStateProofCanonicalizationSuite.scala:20-123`). This does not
      relax `ROOT-010`: its future field-32 replay witness must preserve the
      semantic distinction explicitly.
  - [ ] **2. Shared strict reader:** extend the strict point-read primitive landed
    in `41c19903d` to point, prefix, and raw enumeration. Return typed
    absent/present/malformed results with physical key/path and exact bytes;
    require decoded identity/scope to reproduce the physical key. Never omit a
    matched malformed entry. Gate: `ROOT-008`.
  - [ ] **3. Parallel consumer migrations:** after step 2, independently close
    `MPT-01` Mg* value-only reconstruction; `MPT-02` consumed-allow-spend
    physical nullifier keys; `MPT-03` stake/collateral scope and keys; `MPT-04`
    token-lock value/head selection; `MPT-05` slash/cooldown keys; and `MPT-06`
    price/parameter keys. The parser defects are confirmed; malicious ingress is
    PLAUSIBLE pending end-to-end RED reproduction. Gates: `XMG-013`, `PERM-005`,
    `WT-010`, `ECON-G-002`.
  - [ ] **4. Raw recovery and tower:** exact-snapshot/root-verify network, disk,
    and deep-reorg maps; strip non-consensus derived bytes and rebuild them only
    from rooted state (`ROOT-009`). `ECO-IDX-02`'s root-invisible System-index
    asymmetry is closed; `BR-02` peer-GSI binding, `BR-05` staged atomic install,
    and the broader recovery matrix remain open. After 1A/1B, field 32 is rejected
    from GL0 recovery rather than synthesized as an empty replay input. Make
    malformed tower/SMT durable keys reject the whole load instead of disappearing
    (`STOR-02`, `REC-004`).
  - [ ] **5. Integrated qualification:** prove identical GSAM decisions/root from
    identical rooted state under every sidecar mutation, then exercise restart,
    compaction, density reorg/recovery, and shard counts 1/2/K. Include expiry,
    stake/committee eligibility, slash/cooldown, checkpoint adoption, and supply.
  - **HTTP field-7 residual:** `41c19903d` fixes proof-envelope key/value/leaf
    binding and the local field-25 `MgBalances` path, but the served per-MG root
    still covers field 5 plus fields 25-31, not active allow-spends field 7.
    Root field 7 in the applicable complete commitment or prove it against the
    exact Phase-2 GL0 complete root before activating HTTP cross-shard reads.

### Wave 1 - Parallel models and shared contracts

- [ ] **E1 PARTIAL - hash-bound finality, density, and atomic reorg**
  - Replace ordinal-only phases with durable exact-hash/evidence state.
  - Implement/model the real K/alpha/beta optimistic cascade plus `k1` fallback.
  - **Partial dark model only:** `FinalityReferenceModel` accepts an
    already-selected winner, enforces the generic `maxvalid-tk`/`maxvalid-bg`
    boundary, and models MRCA orphan/adopt plus Phase-2 replacement. It does not
    select the winner or authorize live fork choice
    (`FinalityReferenceModel.scala:7-11,97-100,217-313`).
  - **OPEN frontier implementation/proof blocker:** a strict-preference three-cycle
    over structurally connected comparator inputs has `A >tk B`, `B >bg C`, and
    `C >bg A`, so list permutation changes the left-fold winner without relying on
    the open tie rule (`ChainSelectionSuite.scala:191-226`). A full snapshot/VRF/KES/era
    validation witness remains required. `selectBest` has no live caller; source
    inspection shows arrival-by-arrival `NakamotoChainStore.shouldSwitch` has the
    same incumbent-tournament risk, but a store-level legal-arrival reproduction
    remains open (`ChainSelection.scala:119-159`; `NakamotoChainStore.scala:437-464`).
    O-15 has ratified objective total-frontier semantics, but cutoff/bounded-diffusion
    and late-reveal semantics, cycle resolution, exact `k1` metric/equality,
    objective tie, evidence/verifier, validator/store witnesses, and the
    security/liveness proof remain open. Current `ForkChoiceDecision` carries one unscoped opaque artifact
    pointer; `FinalityCoreBatch.selectionEvidence` separately carries the
    intent-scoped locator and validation requires exact pointer equality. Neither
    asserts an unproved Tk/Bg/transition form;
    it contains no decoded complete header/tine/frontier/era proof
    (`FinalityCore.scala:120-138,353-364`;
    `FinalityBaseCodecs.scala:96-119,178-179`;
    `FinalityIntentValidator.scala:59-120,748-761`). Close those gates and
    FIN-D-001A before minting canonical-selection evidence.
  - **OPEN boundary blocker:** when the bounded live walk finds the true MRCA over
    consecutive tines it reports maximum post-MRCA suffix length, but production
    supplies `kLookback = k1 + 1` and density engages only
    at `depth > kLookback`. The landed witness proves depth `k1 + 1` remains Tk.
    Freeze O-15's exact metric/equality rule and close FIN-D-001B before changing
    live consensus
    (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/types.scala:173-177`;
    `GlobalSnapshotConsensus.scala:1074-1088`; `ChainSelection.scala:161-175,187-240`;
    `ChainSelectionSuite.scala:228-250`).
  - **Partial dark restoration only:** the schema validates a closed
    `PublicationRestoration` plan and claimed receipt shape; it does not prove that
    the target stayed unpublished or that an external republish occurred. The plan
    names either the exact unchanged CAS prior or the exact prior image at
    `targetRevision + 1`. The committed
    coordinator publication cursor survives retirement/recovery and binds the next
    prepare; rewind, wrong-image, substituted-plan, substituted-claim, and stale
    publication contexts reject. `ForkChoiceOrphanClaim` is not true-MRCA or
    target-exclusion proof. The durable store rejects restoration mutations, and no
    live executor, branch hold, or `RestoredAbandoned` authority exists. Coordinator
    initialization still accepts a raw publication cursor; replace it with a
    package-owned exact-MPT-readback capability before activation
    (`FinalityCore.scala:395-460`; `FinalityCoordinatorState.scala:68-76,99-136`;
    `FinalityIntentValidator.scala:891-925,1083-1275,1277-1288,1443-1590`;
    `FinalityCoordinatorKernel.scala:72-154`;
    `FinalityDurableStore.scala:1209-1230,1357-1363,1652-1656`).
  - Remove absolute `k1`/`k2` refusal; recover authenticated history before a
    comparison that crosses local retention.
  - Atomically unwind/refold MPT, phases, shard anchors, binary tracking, tower
    caches, and downstream events.
  - **Gate:** `FIN-M-*`, revised `FIN-D-*`, `FIN-W-*`, `FIN-S-*`, `REC-*`.

- [ ] **E2 PARTIAL - historical N-2 stake/registry and N-1 eta evidence**
  - Runtime eligibility additionally depends on every stop-the-line
    complete-root step; a root-invisible roster/stake/index is not authority.
  - Replace receiver-current filtering with branch-bound historical roots.
  - Root the permissionless GL0 operator roster and its Sybil-resistance rule;
    key registration alone is not eligibility. Every period-N draw resolves the
    exact candidate branch's N-2 atomic key/roster/stake view, intersects those
    sets, and uses only that branch's N-1 eta evidence or returns unavailable.
  - Bind admission, execution, watchtower, optimistic, and tower derivations to
    exact historical evidence across restart/reorg/rotation.
  - Prove slash/cooldown activation, key splitting, and mixed-roster quorum do
    not change eligibility nondeterministically.
  - **Gate:** `CRYPTO-001`, `PARAM-001`, `PERM-*`, `SHARD-C-007`, `SHARD-S-*`.

- [ ] **E2K PARTIAL - canonical preregistered operator keys for every VRF use**
  - One canonical active-era record binds `PeerId`, KES master VK, VRF VK,
    effective eta period, registration ordinal, and exact registration parent hash
    under the operator's long-term identity signature. No sender-carried VK is
    an authority source.
  - **Landed frozen-genesis cut:** GL0 snapshot validation and the currently
    migrated frozen-registry consumers compare carried/configured keys with one
    atomic period-zero pair. Complete long-term-signed genesis KES+VRF records
    are committed in canonical MPT state, and restart materializes and compares
    that rooted identity. This is not runtime registration or permissionless
    eligibility: the independently authorized genesis operator/stake population
    is not yet a rooted consensus input.
  - **Landed runtime state path; authority still blocked:** GSAM validates and
    roots unified record histories/pointers in GL0 state. Acceptance requires
    `signedEffectivePeriod >= inclusionPeriod + 2`, and the isolated resolver
    activates at the signed period only when the record is in the exact N-2
    branch prefix. This is the ratified period-index lookback; it does not claim
    two full period durations elapsed after intra-period inclusion. Production
    consumers do not use that resolver yet.
    Pending, early, stale, orphaned, absent-witness, policy-ambiguous same-owner
    reuse/one-key-change, and other ambiguous cases must fail closed and
    unwind/refold on a density reorg.
  - Route GL0 snapshot leadership, metagraph-binary admission self-sortition,
    execution-shard VK-hash membership, staircase leader/duty identity and
    checkpoint possession, tower trials, any VRF-keyed optimistic/watchtower
    draw, and every future sortition through the same lookup. The KES/VRF pair
    for one operator/era/branch must be byte-identical at every consumer.
  - Carry exact historical registry membership/activation evidence in portable
    NiPoPoW proofs; proof-to-supplied-key equality alone never proves eligibility.
  - Require every consensus-path committee, watchtower, admission, finality,
    tower, evidence, and snapshot fixture to preregister the paired KES+VRF
    identity in the exact canonical N-2 view before use, or commit it in genesis
    for period-zero eligibility. Unregistered generated keypairs are permitted
    only in isolated cryptographic primitive tests. `CommitteeSortitionSuite`,
    `CommitteeShardSortitionSuite`, and `EligibilityCheckerSuite` now use
    loader-validated period-zero paired identities; wrong-key cases use another
    registered identity and repeated statistical trials vary canonical draw inputs.
  - **Current implementation boundary (2026-07-13):** the atomic pair, rooted
    genesis commitment, runtime registration history, and N-2 historical
    resolver exist. The exact-hash hot-chain view adapter also exists, but
    production does not construct it as the runtime authority. Existing live
    key consumers are deliberately frozen-genesis or fail closed. Runtime
    rotations remain disabled because the permissionless operator roster is not
    rooted and shard checkpoints do not bind an exact Phase-2 GL0
    `(ordinal, hash, mptRoot)` authority context. A separate blocker is local
    secret lifecycle: live VRF secrets derive from the long-term identity and
    `OperationalKeyMaker` exposes one destructively evolving KES key. There is no
    scheduled atomic future KES+VRF secret bundle or exact-record selector, so a
    resolver swap alone cannot activate rotation. The runtime certificate also
    lacks an explicit network/genesis/era domain and containing snapshot/root
    witness. Acceptance rejects cross-operator key collisions but does not reject
    one operator reusing both keys or submitting a complete pair record in which
    only one key changed. Atomic activation requires selecting both keys from one
    preregistered record; whether an unchanged KES or VRF key is permitted across
    successive complete records remains an owner decision.
  - **Runtime secret/reorg gate:** atomically provision and durably store the
    future VRF secret plus fresh KES tree, register their public pair under the
    exact N-2 historical-view preregistration rule before eligibility, and
    select that bundle only from an
    exact-parent historical eligibility capability after comparing both public
    keys. Ratify O-12: activation assumes the N-2 registration/roster prefix is
    common-prefix stable. KES erasure cannot be rolled back like Phase-2 state; a
    density reorg crossing an erased activation boundary enters
    `RecoveryRequired`/operator realignment. Retaining old KES masters through
    k2 weakens forward security and is not an implicit fallback.
  - **Strict eta-source completeness and GL0 exact-parent binding landed:** live
    snapshot leader/receiver, `EtaStateManager`, both GSAM boundary-writer
    construction sites, and the admission-anchor callback require a typed
    proved-complete, nonempty N-1 range for N>=2. `getEtaAt(period,parentHash)`
    bypasses receiver-current MPT state and the ambient walk cache; GSAM passes
    its exact `BranchId`, and admission walks the exact Phase-2 anchor. A partial
    or empty range defers; producer-carried/bootstrap eta is not substituted.
    **Residual CRITICAL gap:** shard checkpoints do not bind an exact GL0 anchor
    hash/root, so SharedServices committee resolution and shard
    producer/attester proof eta still call ambient `getEta(period)`. The same
    checkpoint can therefore draw/verify under different eta after a sibling
    density reorg. Add the exact Phase-2 `(ordinal,hash,mptRoot)` to the signed
    checkpoint and remove ambient eta from every shard validity path. A proved-
    complete empty source also needs one ratified canonical liveness result
    distinct from unavailable history.
  - **Current fixture status:** the currently inventoried operative snapshot,
    admission, committee, execution, tower, and slashing fixtures use the
    canonical committed-genesis fixture. Remaining direct
    `OperatorConsensusKeys` construction is confined to registry algebra or
    explicit negative/runtime-unavailable cases. A static repository guard now
    rejects new split-registry factories, unreviewed raw VRF production
    consumers, and new/direct fixture constructors. This guard does not inventory
    every higher-level sortition/eligibility/duty/verifier call, so extend it with
    a complete semantic consumer manifest. `KEYREG-011` remains open
    until qualification proves every allowlisted consumer's historical semantics
    and every generated-unregistered-key no-side-effect vector; the textual
    allowlist is a review tripwire, not that proof.
  - **Portable evidence remains open:** for every suffix and upper-level tower
    occurrence, the verifier now resolves the current atomic period-zero pair,
    verifies the VRF proof over the header's exact carried `eta || slot` bytes,
    compares the derived output
    in constant time, rejects empty/nonempty malformed shapes, and never treats
    sender `activePoolSize` as stake. It then fails closed with historical
    eligibility unavailable because branch-bound roster/stake/eta witnesses do
    not exist. The unverifiable ordinal-range archival backfill path is removed;
    any replacement must carry registered KES/VRF/eta evidence and re-enter the
    normal parent-first snapshot validator. Slashing cannot decide against
    unavailable exact offence-parent history.
  - **Gate:** `KEYREG-001..015`, `FIN-B-004`, `CRYPTO-001`, `TOWER-004/005`,
    `SHARD-S-003`, `ADMIT-003`, cross-consumer and restart/reorg vectors.

- [ ] **S1 PARTIAL - canonical ScodecV1, identity, era, and parameters**
  - Freeze one bounded representation/signature domain for every active artifact.
  - Replace the unwired era registry and JSON/Kryo `HasherSelector` plus legacy
    state-proof switches with one canonical hash-bound protocol-era service.
    Ordinal zero is ScodecV1 for bytes, hashes, signatures, state proofs, MPT
    nodes/values, and recovery records in every environment.
  - Replace JSON state-channel content and decoder-success classification with an
    explicit signed framework-currency / framework-currency-with-data lane.
  - Delete undeployed fork-only compatibility paths; isolate historical disk/
    upstream-v4 Kryo/Brotli-JSON import reading from the new-chain runtime. The
    read-only importer emits a verified ScodecV1 genesis manifest.
  - Freeze composite byte/hash/signature/root vectors and strict negative vectors;
    round-trip-only codec tests do not close activation.
  - Bind network/genesis/era/parameters and exact parent/base into artifacts.
  - **Gate:** `SER-*`, `ERA-*`, `PARAM-001`, `CRYPTO-001`.

- [ ] **S2 PARTIAL - deterministic framework oracle and kernel**
  - Close authorization, conservation/checked arithmetic, replay, ordering,
    backing, and resource-limit findings for every enabled economic operation.
  - Differential-check decisions and exact writes after every input prefix.
  - **Gate:** `ECON-D/C/A/R/O/B/F/G-*`.

- [ ] **S3 PARTIAL - signed payload lanes and data availability**
  - Make currency and currency-with-data explicit; decoder success and custom
    output can never construct framework writes.
  - Retain exact replay bytes and isolated custom commitments/chunks through the
    longest execution/challenge/rollback/recovery horizon.
  - **Gate:** `LANE-*`, `DA-*`, `META-001`.

- [ ] **S4 PARTIAL - adversarial transport/resource/recovery harness**
  - Bound every network/decompression/range/concurrency path before expensive
    verification or storage.
  - Add durable outbox, exact-hash multi-peer recovery, and reproducible fault/
    resource fixtures.
  - **Gate:** `NET-*`, `RESOURCE-001`, `REC-*`.

### Wave 2 - Finality evidence and checkpoint contracts

- [ ] **E3 SCAFFOLD ONLY - signed tower state, consensus SMT, portable proof**
  - Activation depends on every stop-the-line complete-root step, including
    strict durable tower/SMT key decoding and reorg recovery.
  - Put per-level trial state and last-hit links in branch-bound signed snapshots;
    every receiver independently reproduces them.
  - Verify consensus SMT transitions and historical eligibility on produce,
    follow, restart, and bootstrap.
  - Make tower/SMT caches durable and branch-aware under density reorg.
  - Implement a bounded proof and independent comparator over SMT, ancestry,
    KES/VRF, historical registry/eta/parameters, and freshness evidence.
  - Keep `TowerEligibility.NotComputed` until all checks are load-bearing.
  - **Gate:** `ROOT-001`, `LIGHT-001`, `SER-*`, `CRYPTO-001`, proof/reorg vectors.

- [ ] **E4 PLANNED - replayable checkpoint schema and complete root**
  - Schema work depends on complete-root steps 1-3; ordinary diff adoption
    remains blocked until integrated step 5 closes.
  - Bind exact Phase-2 base, network/genesis/era/parameters, shard/roster/parent/
    duty, and complete ordered signed inputs.
  - Bind per-MG `preRoot/preVersion`, canonical byte diff,
    `postRoot/postVersion`, decisions, global intents, and lane/DA commitments.
  - Root every economic write and reject cross-MG/global writes from per-MG diffs.
  - Preserve multiple contiguous binaries for multiple MGs in one checkpoint.
  - **Gate:** `DIFF-001..004`, `SHARD-E-002/003A`, `SER-*`, `LANE-*`, `DA-*`.

### Wave 3 - Shard execution, admission, and watchtowers

- [ ] **E5 PARTIAL - replay before every execution signature**
  - Require a typed locally reproduced execution result at the signing API.
  - Producer and every signer reproduce decisions/diff/intents/root at the exact
    base; missing data/base/era means defer/no-sign.
  - Enforce distinct eligible `kQuorum`; no receipt/depth/best-tip substitute.
  - Retain universal adopter replay until E9 closes.
  - **Current landing:** the shard attestation emitter accepts only a sealed
    replay-minted `VerifiedShardCheckpoint`; rejected/mismatching intake cannot
    store/count/sign, replay-valid under-quorum intake can collect signatures, and
    ancestor duties are checked against each ancestor's own epoch. This proves the
    current root-replay signing boundary, not future diff/intents parity.
  - **Gate:** `SIG-*`, `SHARD-E-001..005`, `ECON-*` differential suite.

- [ ] **E6 IN PROGRESS - single-outstanding multi-MG batching**
  - Remove `pipelineDepth` and shard-depth inclusion qualification.
  - Permit one checkpoint awaiting exact containing-GL0 Phase 2, with multiple
    bounded contiguous binaries for multiple MGs; retain/rebroadcast exact held
    bytes. Tentative embedding and ordinal equality cannot release a successor.
  - Buffer later inputs for the next checkpoint and hard-anchor only at exact
    containing Phase 2.
  - **Current residuals:** held bytes and the Phase-2 anchor are process-local;
    single-outstanding is enforced only by producer policy, not by signed parent
    Phase-2 evidence at replay/sign/adoption, so a Byzantine committee can still
    pipeline a child and stall ancestor-first selection;
    restart can recreate ordinal 1, Phase-2 replacement/rollback cannot update
    the max-monotone anchor, callback delivery is not transactional/exactly-once,
    and batch size/fairness are unbounded. This epic remains open.
  - Treat every multi-MG checkpoint as one atomic transition: exact outer shard
    identity, no deferred segment, and exact accepted suffixes before its P2 anchor.
  - Enforce the retained producer's staircase duty during intake, signing, and
    embedded-artifact validation. Embedded validation still needs portable
    proposal-parent-bound parent context instead of a receiver-local shard-store
    lookup. A canonical signed-evidence upper slot bound is also required; parent
    monotonicity alone admits a far-future-slot shard halt.
  - A node learning a checkpoint only through GL0 must durably ingest its exact bytes
    before the P2 anchor/finalize update so it can validate and build the successor.
  - **Gate:** `SHARD-C-001..005`, `SHARD-C-007..010`, `REC-002`, and `NET-002`,
    including durable loss/rebroadcast, eta rotation, restart, and P2 reorg.

- [ ] **E7 PARTIAL - authenticated admission/custody before receipt signature**
  - Verify ML0 source signatures against the pinned MG operator registry before
    an admission member signs.
  - Validate domain/size/lane/authenticated parent/ordinal; derive eta from
    authenticated state, not a producer claim.
  - Durably store exact bytes, then sign a domain-separated custody receipt that
    cannot count as execution validity.
  - **Gate:** `ADMIT-001/002`, `SIG-003/004`, `DA-*`, `NET-*`.

- [ ] **E8 SCAFFOLD ONLY - positive watchtower coverage before GL0 inclusion**
  - Deterministically select a noncommittee complement/sample from historical
    eligible state and require minimum positive replay coverage.
  - Sign coverage only from a local replay capability in its own exact-checkpoint
    domain; every adopter verifies assignment, distinct signers, signatures, and
    threshold. Any authenticated assigned mismatch quarantines pending objective
    adjudication regardless of positive count.
  - No checkpoint-derived economic capability is usable before coverage;
    unrelated GL0 snapshots continue while it waits.
  - **Gate:** `WT-001/002/004/005/006/008/008A`.

### Wave 4 - Global adoption and cross-metagraph settlement

- [ ] **E9 PLANNED - verified GL0 diff adoption and global settlement kernel**
  - Verify quorum/base/continuity/pre-root/input/coverage/diff scope, apply the
    canonical diff, and recompute post-roots. Never install a claimed root.
  - Require exact canonical Phase-2 origin for every signed historical read,
    nondecreasing per-MG refs, and proposal-parent pre-root/version CAS before
    replay/sign/inclusion. Receiver live head and self-claimed roots never enter.
  - Replace ordinary universal CL1 replay only after malformed-artifact gates pass.
  - Run one deterministic GL0 conflict/nullifier/settlement kernel over signed
    intents and atomically compose per-MG mirrors with GL0-owned overlays.
  - Replace bounded `GlobalSnapshotsProcessed` reconstruction with a rooted,
    hash-linked per-MG delivery sequence, durable ML0 applied cursor/state proof,
    and compare-and-set metadata-only acknowledgement (O-13). No ordinal/history
    window may restore delivery eligibility.
  - Replace replayable data-application fees with a domain-separated exact-parent
    fee intent that atomically advances rooted field 27 and balances, and binds
    exact available opaque bytes/manifest without GL0 executing DL1 (O-14).
  - Serialize outer state-channel binary fees and every other global-balance write
    in the checkpoint-wide GL0 reservation kernel; per-MG roots alone cannot
    resolve a payer shared across metagraphs.
  - Use one typed balance ledger keyed by `(currency scope,address)` across
    transfers, fees, rewards, allow-spends, token locks, mandatory GL0
    SpendActions, and slash bounties. Atomic multi-address claims either reserve
    completely or leave no state; final application must equal the projected map.
    ML0 applies exact Phase-2 GL0 inbox work before conflicting local CL1 work.
  - Preserve the closed narrow regressions: allow-spend admission never credits
    destination (ECO-20), a token-lock replacement release is consumed by one exact
    operation at the exact candidate epoch (ECO-21), allow-spend settlement has one
    terminal winner and typed one-shot references (ECO-22), lane identity rejects
    whole malformed blocks (ECO-23), and field-7 reconstruction fails closed on
    key/value mismatch (SMT-04).
  - Keep the remaining work explicit: one exact post-class projected ledger must
    reject destination overflow before application (ECO-24). ECO-25 is fixed in
    `41c19903d`: local cross-shard no-ref reads use strict exact-byte field-25
    `MgBalances`, bind the embedded account, and reject malformed committed
    bytes. This does not close the HTTP field-7 proof/root residual above.
    Execution signatures bind the final checkpoint-wide outer-fee/global-intent
    result, not isolated MG roots.
  - Require identical economics at shard counts 1, 2, and K.
  - **Gate:** `DIFF-*`, `SHARD-E-003/004`, `SHARD-C-004/005`,
    `XMG-001..005B`, `XMG-007/008/010/012/013`, `ECON-F-002/003`,
    `ECON-REF-001`, `ECON-BAL-002/003`, `ECON-G-002`, `ROOT-006..009`,
    `PERM-005`, `WT-008/008A/010`, `ECO-IDX-03`, and `MPT-01..06`. Preserve the
    closed `ECO-IDX-01/02` regression corpus.

- [ ] **E10 PARTIAL - downstream exact-hash rebase and historical recovery**
  - E9 owns exact-origin/CAS validity before the security cutover. Carry the same
    exact Phase-2 refs through downstream delivery, rollback, and recovery.
  - [ ] **Bootstrap/recovery stop-line (`BR-01..BR-05`), in dependency order:**
    - [ ] P6 exposes exact FinalityGate Phase-2
      `(ordinal,hash,parentHash,mptRoot,evidence)`; P11 transport binds the canonical
      selected-era full proof and content-addressed state payload/projection proofs
      to that reference as one bundle. No caller may combine a finalized ordinal
      with a separate latest/best-tip fetch.
    - [ ] P1/P10 validate the complete ordinal-selected proof and bind every GSI/
      typed projection to the same bundle bytes. `mptRoot` presence does not choose
      the era; persisted bytes do not authenticate a separately supplied GSI.
    - [ ] P7/P10/P11 supply complete exact ML0 bytes, including native partitions,
      and only an explicitly enumerated, signed/proved GL1 consumed-field slice. A
      failed root/proof candidate enters `RecoveryRequired`; it never degrades to
      plain GSI re-encode.
    - [ ] P11 stages and verifies the entire bundle, then atomically installs MPT,
      exact snapshot anchor, projections, balances/references, cursors, and markers.
      Mismatch, cancellation, exception, and crash preserve the prior generation.
    - [ ] Close only after `BOOT-001..005` and `REC-005` pass for cold join, disk,
      peer, catch-up, and density-reorg recovery. The current narrow rebuilt-root
      guards are PARTIAL and do not close any BR finding.
  - `BR-06` is cross-referenced to open `ECO-F32`/`ROOT-010`; do not duplicate it
    or treat a root-only check as its replay-witness closure.
  - Bind every MPT base to a root-verified `(snapshotHash, ordinal, mptRoot)` and
    acquire one exact-parent session for all reads, replay, writes, and commit.
    Unknown/evicted hashes and incomplete ancestry enter typed recovery; they
    never fall through to base. Finalizing an unknown branch cannot mutate base
    or markers, and finalizing an ancestor retains canonical descendants.
  - **Owner decision locked (L-23):** an exact-parent session captures one
    immutable generation and commits through generation CAS/retry; viable branch
    generations have bounded local retention with authenticated reconstruction or
    `RecoveryRequired` beyond it; one durable idempotent finality-intent journal
    coordinates the existing sinks. This settles the strategy, not the
    `ROOT-002..005` implementation gates.
  - Activate those branch guards only with authenticated restart binding,
    descendant retention, explicit `RecoveryRequired`, and finality-sink
    preflight/coordination. Unknown rejection alone halts the current normal path
    after the tip tracker, chain store, and outbox may already have advanced.
  - The dark image store now supplies immutable copied/sorted images, independent
    root rebuild, forced atomic publication, digest/readback, monotone generation
    CAS, lifetime OS directory ownership, bounded streaming decode, and typed
    missing-artifact recovery. Before activation add an active-era whole-image
    physical-key/value verifier inseparable from the root algorithm,
    FinalityGate-authenticated exact snapshot anchoring, immutable capture inside
    the session, and one durable multi-sink finality intent. A supplied era label
    or self-consistent manifest is not authentication. The image store alone closes
    none of `ROOT-002..004`.
  - The dark structural resolver returns `ResolvedParentLineage`, not an execution
    session. Mint the latter only while atomically copying the complete exact-parent
    bytes and the whole overlay mutation generation; compare that generation inside
    the commit CAS. Pending rekey/discard/replacement with an unchanged durable base
    must stale the capture.
  - The dark chain-store walk is bounded and exact-hash first; ordinal fallback can
    only establish absence or return a typed same-ordinal sibling mismatch. It
    fails closed with `HashEraUnavailable` where current and ordinal-selected
    JSON/Kryo logic differ. Before any Scodec/hash-era activation, land one atomic
    canonical-identity migration across every producer, verifier, KES, overlay,
    gossip, storage, and recovery path, using a discriminator stronger than the
    current coarse `HashLogic`.
  - [ ] Harden live chain-store admission so ordinal, parent hash, slot, and VRF
    metadata are derived from and checked against the authenticated signed snapshot;
    the dark ancestry walk is detection/recovery infrastructure, not that boundary.
  - [ ] **L-23 PARTIAL - finish the dark durable finality-intent boundary and wire it
    only after its authorization gates close.** ScodecV1 ADTs/codecs, canonical
    identities, structural validators, and a sealed kernel that can initialize,
    persist a validated `Prepared` intent, or enter absorbing `RecoveryRequired`
    have landed. The checksummed coordinator/audit/outbox store performs exact
    compare-and-set, durable readback, bounded restart validation, and fail-closed
    recovery. It is dark: no live `FinalityGate`, fork choice, snapshot consensus,
    MPT publisher, follower, or service path calls it, so L-23 remains open.
    Exact per-hash P0/P1 state and the optimistic K/alpha/beta decision cascade are
    not implemented by this durability slice.
  - Before activation, add authenticated finality-evidence/fork-choice authority and
    hold/recheck the exact branch revision through publication; package-own the
    MPT-plus-semantic-plus-anchor readback capability that may advance
    `CoreApplied`/`Released`; prove objective restoration; and implement the effect
    executor with sink readback provenance, dependency DAG, and `RetentionMature`
    pruning authority. Replace materialized arbitrary-depth path validation with a
    bounded streaming verifier and add a long-history audit-journal checkpoint/
    accumulator before the bounded startup walk can become an availability limit.
    The schema represents only an already-decided `T_weight`
    attestation or canonical depth-`k1` result, but does not yet authenticate either;
    it must never legitimize today's cumulative-weight shortcut or introduce a global
    BFT vote/lock/QC path.
  - Density replacement reverses anchors, mirrors, settlement/nullifiers, and
    delivery before exact replacement re-follow/rebase.
  - Missing historical data fetches authenticated bytes or enters
    `RecoveryRequired`; it never falls back to receiver live head.
  - **Gate:** `BOOT-001..005`, `XMG-006`, `FOLLOW-001..005`, `REC-*` including
    `REC-004/005`, `MEMPOOL-001`, `ROOT-002..005`, `ROOT-009`, `STOR-02`,
    `GROWTH-001`.

- [ ] **E11 SCAFFOLD ONLY - exceptional replay, adjudication, and slashing**
  - Accept only assigned, bonded, rate/resource-limited exact-data challenges.
  - Bounded universal GL0 replay, not the assertion, decides mismatch, rollback/
    quarantine, signer debit, and reward.
  - Missing data defers/no-slash; later base orphaning is not execution fraud;
    evidence is branch-aware, deterministic, and exact-once.
  - **Gate:** `WT-001..007`, `CRYPTO-001`, `REC-*`, flood/resource tests.

### Wave 5 - Rebase, correction, and activation

- [ ] **E12 PARTIAL - downstream exact-hash rebase and return path**
  - GL1/ML0/CL1/DL1 consume exact canonical Phase-2 GL0 state; CL1 does not
    independently replay or override it on return.
  - Density replacement rolls back anchors/windows and requeues orphaned inputs
    once; ML0 performs deterministic rebase/rewind or begins a new epoch.
  - Remove GSI/peer/cache authority only after typed MPT parity and recovery.
  - **Gate:** `FOLLOW-001..005`, `MEMPOOL-001`, `GSI-*`, `REC-*`.

- [ ] **E13 PLANNED - protocol-level GL0 metagraph correction**
  - Define a root-covered GL0/active-era artifact binding target MG, lineage/
    pre-root/version, deterministic correction, activation, and post-root/version.
  - Every GL0 validator verifies/applies it; ML0/CL1/DL1 can only rebase from the
    result and can never originate equivalent authority.
  - Preserve future upstream-v4 snapshot-genesis import without undeployed
    fork-only runtime schemas.
  - **Gate:** `CORR-001..003`, `XMG-011`, `FOLLOW-006`, `DIFF-*`,
    `ECON-BAL-001`, conservation/reorg tests.

- [ ] **E14 PLANNED - staged activation and independent qualification**
  - Keep economic sharding fail-closed until every launch dependency closes.
  - Run model/component, multi-process, partition/restart/reorg, Byzantine
    committee/watchtower, cross-shard multi-MG, long-run permissionless, and
    independent candidate-audit stages.
  - Cover currency and currency-with-data, shards 1/2/K, rotations, mixed refs,
    censorship, colluding quorum, eclipse, deep recovery, and root/supply/
    nullifier equality.
  - Archive exact candidate, parameters, seed/fault schedule, commands, artifacts,
    and independent verdict.

### Delegation rule

E1, E2, E2K, S1, S2, S3, and S4 can run in parallel after E0 freezes shared
vocabulary. E3/E4 then proceed in parallel against frozen interfaces. E6/E7 may
run in parallel after E4; E8 follows replay-capable E5. Inside E9, the delivery,
data-fee, checkpoint-global-ordering, and exact-branch lanes may build in parallel
only behind frozen interfaces; they join before any economic activation. E9 is
the security cutover. E10/E11 integrate after it; E12/E13 follow the global state
contracts; E14 qualifies the exact integrated candidate.

Every delegated packet records `baseline`, `writeSet`, invariant/finding/test
IDs, dependencies, commands, artifact directory, and integration owner. Shared
schema/finality/GSAM/MPT files have one integration owner. The implementer cannot
be the independent closer.

---

## ✅ Completed (Phases 0-7 + extras) — from the original sweep, unchanged

| Feature | Commit | Tests |
|---------|--------|-------|
| VRF crypto (ECVRF-ED25519-SHA512-TAI) | PR #4 merged | 43 tests |
| Slot clock + LDD snowplow eligibility | PR #5 | 20+ tests |
| StakeRegistry, EpochState, SlotCertificate | PR #6 | 30+ tests |
| Go libp2p sidecar (GossipSub, mDNS, gRPC) | — | Manual |
| NakamotoProposer + snapshot production | — | — |
| Attestation + depth trigger scaffolding | Built, but the real Avalanche cascade and hash-bound P0/P1/P2 gate are absent | See active lifecycle/roadmap |
| Fork choice / ChainSelection (Bifrost-style density) | 92cab6e1 | — |
| ParentChildTree + reorg support | 92cab6e1 | — |
| Chain-derived eta (replaces accumulator) | 9b1ede57 | — |
| MPT stateProof determinism (full-rebuild workaround) | b7489986 | — |
| Self-healing incremental MPT (detect + resync on fork) | 21cec6de | — |
| ~~MptUndoJournal (per-ordinal delta tracking)~~ — REMOVED in #56.10 (`5ecc0772`, 2026-05-06); replaced by `MptOverlay` branch checkout/discard. | 289bcece (intro); `5ecc0772` (removal) | — |
| Content validation enforcement | 289bcece | — |
| NakamotoSnapshotValidator (4-stage: VRF+sig+cert+content) | — | — |
| /latest/info endpoint for validators | 5a09af8f | — |
| RunNakamotoValidator (join running cluster) | 5a09af8f | 4-node test |
| ProductionGate (pausable leader election) | bc18ffba | — |
| Better-gossip-received gate trigger | 3bf8db7c | — |
| Cold restart recovery (resume from disk) | 60ff0af4 | 3-node test |
| Replay-only ancestry catch-up | Current tree | Fresh validation required; direct peer-state installation removed |
| Optimistic attestation scaffold | Built, but legacy aggregation is not the real K/alpha/beta cascade and phase state is ordinal-only | See audit FIN-01/FIN-02 plus active lifecycle |

**Test cluster validated (as of original sweep):** 3-node genesis + 1 validator joining mid-chain, cold restart, single-node restart, reorgs, fork convergence, MPT determinism, attestation finality.

---

## 🧩 Execution-Sharding & Global Economic Enforcement (dominant workstream since 2026-04)

> The target model: the producer and every execution signer run the same full
> currency recreation over exact signed ML0 bytes at an exact Phase-2 GL0 base.
> They sign the reproduced canonical diff and complete root. Ordinary GL0 adopters
> apply and root-check that diff without recreation. Noncommittee watchtowers
> replay against collusion. Every GL0 node still executes native GL1 transitions
> and the small global cross-metagraph ordering/nullifier/settlement kernel. Custom
> DL1 bytes remain isolated commitment/availability data and cannot affect the
> framework result. Authorization and conservation blockers remain open.

- ⚠ **Sharded-security substrate** — pinned `globalSyncView`, PIN-1 component-addressable per-MG MPT roots, I-ONCE spent-set, watchtower fraud-proof scaffold (`8ac7ce04f`).
- ⚠ **Sharding wiring present, epoch binding partial** — real committee key-possession verification + W3c sharded-validator activation
  are live. The worktree now derives `checkpoint.epoch = floor(gl0AnchorOrdinal/R)` with the same pure producer/verifier function and
  rejects an inconsistent pair before committee lookup. Grinding remains: a producer can choose an older admissible anchor ordinal and its
  matching favorable epoch because selection currently checks only `gl0AnchorOrdinal <= currentOrdinal`. The anchor is not an exact
  proposal-parent hash-bound Phase-2 proof and R is still node-local configuration; SHARD-03/SHARD-09 remain open.
- ⚠ **Shard committee draw** — public deterministic VK-hash selection per `(shard, eta period)`, uniform `1/N` over eligible GL0 operators. This deliberately replaced the failed secret stake-weighted VRF design; the attached VRF proves registered-key possession, not hidden membership.
- 🔴 **`numShards=1` security bypass** — current `numShards > 1` gates make the
  committee/watchtower path inert at the default. Target economic semantics treat
  one as one execution shard using the same replay/diff/watchtower protocol; shard
  count cannot select a different validity or replay-protection function.
- 🔴 **Committee diff/adoption regression** — current every-adopter recreation from
  `includedSnapshots` is not the target. Restore only a canonical root-covered
  `ShardCurrencyStateDiff`; every signer replays before signing and ordinary GL0
  nodes apply/root-check. Keep `authoritative*`, `AdoptFromSignedFields`, and
  direct cross-shard receipt schemas deleted.
- ⚠ **Watchtower InvalidStateProof slash — durable ledger** (Slashings fieldId 34) (`ed8928b81`); durably slash a full-quorum colluding committee on one honest fraud proof (`68246cffe`, W3a). **Wired-vs-shelfware being verified in handoff.**
- ⚠ **Slash-cooldown committee exclusion** — the cooldown reader is wired, but the loop is not closed: the worktree wire-epoch check still
  depends on an unrooted local R and ordinal-only anchor (SHARD-03/SHARD-09), while ECO-06 leaves bonded principal undebited.
- ⚠ **Cross-shard framework reads = Option A (finality-first), exact-hash gate
  incomplete** — the GL0 accept path reads through the current finalized MPT base,
  so a value present only on a tentative parent is not used by that path. The live
  `FinalityGate` is ordinal-only, however; exact Phase-2 hash identity, density
  replacement, and downstream rollback are not implemented. Do not treat the
  current read source as proof of the target cross-shard contract.
- ⚠ **Atomic cross-shard allow-spend settlement (I-ONCE)** via generic cross-shard-message seam (`f368e064e`); W3c cross-shard proof-path effective-balance overlay (`4081ef0d4`).
- ⚠ **Sharded-currency-mirror store-fidelity stack** — pinned finalized reads, by-ordinal `PinnedCurrencyInfoReader`, and root-verified exact-ordinal hole backfill remain. Adopted-state staging and direct recovery installers are removed; peer bytes never authorize consensus adoption.
- ✅ **ML0 authoritative override fields removed** — fork-only `authoritative*`
  schema/codec slots and `deriveAdoptedCurrencyInfo` remain deleted. ⚠ This does
  not justify deleting the committee's reproducible canonical byte diff; that diff
  must be restored under ADR-0017 without any claimed-state replacement path.
- ⚠ **Shard-checkpoint chain-sync (pull-based recovery)** (`df5b7b0e0`, `d47a4403f`); FINALIZED-anchored shard checkpoints (`eb15e19c0`).
- ⚠ **ML0 consensus integration** — ML0 may remain BFT for its small,
  well-connected validator set, with the GL0 Phase-2 chain as global truth. Any
  prior plan to force global-style Nakamoto consensus onto ML0 is not the current
  target and requires a separate product decision.
- ⚠ **Go↔JVM gossip transport for shard-checkpoint + fraud-proof legs** (`72f39d652`, F1/F2/F8); sidecar carries l1-block topics + self-delivery (`b0ca67908`).
- 🔴 **RECURRING WEDGE / current e2e operational frontier (not the security dependency head) — metagraph committee-gate parent-ordinal resolution.** `MetagraphCommitteeGate` / `MetagraphParentOrdinalResolver` / `MetagraphOrphanBuffer`; resolver→None on GSI-tip-lag → orphan re-buffer loop. Prior fix "walk-finalized-not-bestTip" (`40d546761`) recurred; blocks the 2mg/2shard token-lock e2e. **Must be fixed WITHIN the re-exec model, after the active roadmap's security ordering is respected.**
- ⚠ **CL1 replay-before-sign is type-gated; target result/adoption is missing** —
  the emitter now accepts only a sealed capability minted after `evaluate` replay,
  and the ancestor path independently replays before signing. The replayed object
  is still the current root-only checkpoint, however; it does not bind/reproduce
  the target canonical diff, intents, or complete root. `verifyEmbedded` and GSAM
  still replay on every GL0 node. Add noncommittee diff apply; watchtower replay
  remains the collusion backstop. Before removing universal
  replay, embedded adoption must also enforce distinct `kQuorum` and the signed
  checkpoint must bind exact Phase-2 hash/root plus network/genesis/era/parameters;
  neither holds today.
- 🔴 **Global replay-before-attest type boundary** — `emitTipAttestation` accepts
  naked tip hash/slot/ordinal inputs and the best-tip ticker calls it directly.
  Replace it with an authenticated-locally-executed snapshot capability as part of
  the real FinalityGate/Snowball work.

---

## 🔧 Implementation Still Needed (original items, re-annotated 2026-07-10)

The priority headings in this inherited list are historical. Use the active
consensus-economic roadmap for dependencies and release gates.

### High Priority — Before Testnet

1. ⏳ **Existing-network snapshot genesis** — preserve no live legacy dispatcher or
   pre-genesis Kryo/JSON/GSI dual mode in the new runtime. Build an isolated
   upstream-v4 finalized-snapshot exporter and deterministic audited transform to
   a new-network ScodecV1 ordinal-0 genesis. Future post-genesis upgrades use the
   finalized hash/ordinal-bound `ProtocolEra` mechanism.

2. ✅ ~~**Disable BFT Daemons in Nakamoto Mode**~~ (af15c077, bde17768, 5ec6ac46) — unchanged.

3. ♻⚠ **Metagraph consensus integration** — original framing ("CL0 needs the same
   VRF+LDD+attestation 1:1") is superseded. ML0 may retain BFT consensus; execution
   shards use staircase Nakamoto checkpoint chains; exact Phase-2 GL0 state is the
   shared truth. Re-audit current ML0 rotation/anchor code against that split.

4. ⚠ **Gossip Layer: Replace Tier 1/2 with Sidecar** — substantial progress: sidecar now carries Nakamoto + l1-block + shard-checkpoint + fraud-proof topics; Go↔JVM transport hardened (`72f39d652`, `461a34830`, `ad3f01092`). **Not confirmed:** ALL gossip through sidecar; Kademlia DHT peer discovery (still seedlist/mDNS?). Keep open items.

5. ⚠ **Stake-Proportional VRF (global Nakamoto leadership only)** — delegated-stake state landed and is read from MPT, but source verification of the global leader threshold remains open. This item does **not** apply to execution-shard membership: shard v1 is intentionally public, identity-uniform `1/N`, and not stake weighted.

### Medium Priority — Testnet Hardening

6. ♻ ~~**Proactive MPT Rollback on Fork Switch**~~ — superseded by `MptOverlay` (#56); MultiBranch overlay default since `e3538d9b`; revert-executor `MptOverlay.revertToOrdinal` (`3e47d1904`). Self-healing remains fallback.

7. ⚠ **Production Abandonment on Better Gossip** — ProductionGate + better-gossip trigger exist; finality-trigger stack landed. **Unverified:** threshold-based abandonment fully wired (ChainSelection.compare incoming vs in-progress at SnapshotLeaderLoop checkpoints).

8. ⚠ **Genesis Time Discovery for Validators** — genesis time/eta moved to typed HOCON (`d7a4212d5`). **Unverified:** peer-API discovery (`/cluster/genesis-time` or slot-cert in `/latest`) vs still config-supplied. Note: eta bootstrap for periods 0&1 now genesis-derivable (`45066b4b0`).

9. ⏳ **Mempool Reinsertion on Finalize** — no clear landing commit found. Round-cancellation logging landed (`948d2b2e1`) but event recycling on orphan/finalize appears unimplemented. Keep.

10. ⚠ **Partition Recovery (Fork Recovery)** — the GL0 design changed after the reported runs: direct peer-state/GSI installs are removed. A receiver buffers a missing-parent snapshot, fetches ancestry, and exact-replays every transition through the ordinary validator. Shard-checkpoint pull recovery remains, but the combined replay-only flow needs fresh partition/restart e2e validation. Confirm per-sub-item:
    - **10a.** Sidecar GossipSub mesh re-establishment after partition — ❓ verify (sidecar reconnection/mesh re-graft).
    - **10b.** Missing-parent ancestry request + buffered replay — ⚠ confirm recursive completion and prove no peer-carried context/state can be installed.
    - **10c.** ProductionGate stale-fork detection / pause-if-behind — ⚠ confirm.

11. 🔴 **Hash-bound P0/P1/P2 FinalityGate** — the full Avalanche cascade is absent,
    `T_count` is diagnostic, the fast path still uses legacy aggregation, phase
    refs are ordinal-only/volatile, and density-band recovery is disabled. Target:
    P0 pending; P1 maxvalid-tk provisional; P2 operational via the ratified real
    Snowball optimistic trigger OR `k1` depth fallback. Phase 2 remains
    maxvalid-bg density-reorgable and emits exact-hash downstream rollback events.
    `k2` is retained recovery/proof capacity, not a protocol phase or finality
    floor. No global BFT certificate/vote/lock layer.

### Low Priority — Post-Testnet

12. ⚠ **Content-Addressed MPT (Ethereum-style)** — MPT-as-primary substrate substantially built (signed-byte store, `MptStore.loadBytes`, by-ordinal readers, serve+rebuild `082ed24e7`). **Unverified:** full content-addressed trie-node KV store keyed by hash. Storage-representation migration, NOT re-execution.

13. ⏳ **Superblock Proofs (NIPoPoW-style)** — level-µ design landed (notes: one VRF, L domain-separated rehashes); hexary-MPT absence-proof WIP (`690b0f259`, task #286). Implementation deferred; light-client DEMO sequenced post-slashing/KES.

14. ⚠ **Global attestation committees** — still open. Shard-checkpoint membership is a public deterministic VK-hash draw plus a possession VRF, not secret VRF self-sortition; do not cite it as implementation of this global-attestation item.

15. 🔴 **Layered finality (GL0 + ML0 + shard)** — ML0 BFT finality authenticates
    its binary; execution signatures certify independent CL1 replay; staircase
    duty and parent/fork choice select a checkpoint candidate, but only distinct
    replay-backed `kQuorum` makes it execution-certified. The exact containing GL0
    Phase-2 hash makes effects operational and remains density-reorgable. `k2`
    changes retention/recovery capacity only. These statuses cannot substitute for
    one another.

---

## 📊 Scope Doc vs Implementation (re-annotated 2026-07-10)

| Scope Doc Section | Status |
|-------------------|--------|
| §1 Problem Statement | ✅ Understood |
| §2 LDD Heartbeat | ✅ Implemented (fA=0.5, fB=0.05; shard-tuned variants exist, e.g. γ=45 per-slot) |
| §3 Epoch Progress | ⚠ Now used in acceptance (`metagraphPinnedEpochProgresses`, epochProgress); eta bootstrap landed (`45066b4b0`). Confirm full §3 semantics. |
| §4 What Changes | ✅ Removals identified; BFT daemons disabled in Nakamoto mode |
| §5 VRF Key Derivation | ✅ Implemented (SHA-512 domain separation) |
| §6 Attestation & Finality | 🔴 Implemented rails are unsafe; claimed Avalanche/max-of protocol is absent |
| §7 Two-Level Finality | 🔴 Not established; metagraph inclusion inherits unsafe GL0 finality |
| §8 Mempool Reinsertion | ⏳ Not implemented (unchanged) |
| §9 Network Layer (sidecar) | ⚠ Expanded (l1-block/shard-checkpoint/fraud-proof topics); full replacement + DHT open |
| §10 Staking Model | ❓ Delegated-stake state landed; VRF stake-weighting unverified |
| §11 Migration Strategy | ⏳ Offline finalized-snapshot -> new ScodecV1 genesis required for the planned future network fork; no live legacy runtime mode |
| §12 Superblocks | ⏳ Design landed; implementation deferred |
| §13 Phases | 🔴 Historical “complete” claim rejected: finality is ordinal-only/volatile, the real cascade and density rollback are absent, and `k2` was incorrectly treated as a finality phase |
| §14 Open Questions | Owner register now covers optimistic/current-canonical evidence, deep authenticated recovery, single-outstanding checkpoint handling, execution quorum, watchtower release/adjudication, pre-root CAS, mixed refs, admission, economic grammar, ML0 reorg, root fields, and lane scope |

---

## 🔐 Security workstream (KES / slashing) — added 2026-07-10

- ⚠ **KES historical cluster exercise, not runtime-key completion** — the
  2026-05-16 eight-node rotation run predates the atomic operator-pair contract.
  The current worktree roots atomic KES+VRF genesis/runtime records and models
  delayed historical activation, but production consumers intentionally reject
  runtime records until E2K supplies the rooted authorized roster, exact-parent
  resolver, and exact artifact anchors.
- ⚠ **Slashing = detection-only accumulator** — landed but SHELF-WARE; planned refactor to epoch-anchored participating-set + demotion-as-exclusion (design-only).
- ⚠ **Watchtower fraud-proof / InvalidStateProof slash** — intended noncommittee
  collusion backstop for execution-certified diff adoption. Selection, challenge
  timing versus full economic release, independently computed adjudication, bonded
  debit, DoS bounds, and false-slash resistance remain release blockers.
- ⏳ **BLS / aggregate-sig** — feasibility PROVEN (BC 1.85 KAT byte-match); unified rotatable ValidatorKeyRegistry + PoP, target = snapshot certs. Not started in prod.

---

## 🧪 Test Infrastructure

- Current standard e2e: `just test --skip-streaming --grafana --num-shards=2` (2mg/2shard). See `docs/nakamoto/E2E-CLUSTER-TOPOLOGY.md`. (⚠ `--shards` is a dead no-op; use `--num-shards`.)
- Original harness (still present): `nakamoto-test/` — `demo.sh`, `docker-compose*.yml` (3 genesis + 1 validator + Go sidecars), monitoring (Prometheus 5s scrape + Grafana), `test-validator.sh`.
- Prometheus cluster-wide at `localhost:19090`; correlate with node logs on any failure.
- Unit tests: 647+ at original sweep; many more since (sharding, cross-shard, watchtower, catch-up, store-fidelity suites).
