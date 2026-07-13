# Nakamoto — Active Work Plan

**Historical branch at time of writing:** `feature/serde-typeclass-shim`
**Historical content date:** 2026-05-16
**Active architecture/build sequence:** 2026-07-12

> **ACTIVE PLAN.** Execute the sequenced epics below. The source-cited security
> baseline remains `docs/review/CORRECTNESS-SECURITY-AUDIT-2026-07-11.md`.
> `docs/review/CONSENSUS-ARTIFACT-LIFECYCLE.md` is the normative architecture only
> after conflicting text is reconciled through E0. The older May roadmap is
> retained below as historical context and is not an implementation order.
>
> **Committee correction.** Both committees are GL0-operator committees, not ML0 committees. Per-binary admission uses a real secret-key VRF keyed by `(eta, metagraph, parentHash)` but currently weights operators uniformly (`1/N`). Execution-shard membership is a separate public deterministic VK-hash draw keyed by `(eta, shard, epoch)`, also uniform `1/N`, followed by hash-shuffled staircase duty. The abandoned design was secret, stake-weighted shard membership with per-slot LDD leadership. Historical roadmap text below is not the live shard design.

Companion to `NAKAMOTO-TODO.md`. The older `docs/nakamoto/IMPLEMENTATION-PLAN-POST-VALIDATION.md` is historical and must not be read as the current shard design.

## Active objective

Safely enable framework-economic interactions among metagraphs assigned to
different execution shards, with every effect mediated by the selected GL0
snapshot chain:

```text
CL1/DL1 -> ML0 signed binary -> GL0 admission/custody -> execution shard
        -> replay-certified checkpoint -> positive watchtower coverage
        -> GL0 diff adoption + global conflict/nullifier/settlement kernel
        -> exact Phase-2 downstream state and reorg/rebase notifications
```

GL0 consensus remains Nakamoto/Taktikos/LDD. Avalanche/Snowball is only the
optimistic exact-hash Phase-2 rail. Do not introduce global proposal/vote/lock,
QC, or view-change machinery. ML0 may retain BFT consensus.

`k1` separates short-chain `maxvalid-tk` selection from long-range
Genesis-style `maxvalid-bg` density selection. `k2` is a retention, recovery,
and external-risk recommendation; it is not an absolute fork-choice floor.
A fork older than locally retained state requires authenticated recovery and
replay before switching. It must not be silently rejected, locally scored over
truncated history, or resolved by an operator choosing the winning branch.

## Completion loop

Every task, including documentation and schema changes, follows the same loop:

1. **Freeze:** name the invariant, signed preimage/state ownership, adversary,
   dependencies, and exact source baseline.
2. **RED:** add an exploit regression or independent model/oracle that fails on
   the baseline for the intended reason.
3. **Implement:** make the smallest owned change behind typed boundaries. Do not
   weaken current universal replay until its replay-certified replacement and
   adoption checks are executable.
4. **Verify:** run focused unit/property tests, independent differential/model
   checks, restart/reorg tests, then multi-node tests at `numShards=1`, `2`, and
   `K`.
5. **Audit:** a reviewer who did not implement the task traces every signing,
   state mutation, rollback, and recovery path and confirms the RED test cannot
   be bypassed.
6. **Close:** record source lines, test IDs/commands/artifacts, closing commit,
   residual assumptions, and update status only then.

No epic is complete merely because a type, route, store, or happy-path component
exists. Missing-data behavior must be defer/halt before signing or mutation.

## Sequenced epics

Statuses below describe the target protocol, not historical component landings.
`PARTIAL` means useful code exists but the stated invariant is not enforced end
to end.

### E0 - Architecture and invariant freeze (`IN PROGRESS`, blocks dependent runtime changes)

- Reconcile ADR-0016/0017, lifecycle, roadmap, test plan, and owner decisions to
  the layer map and authority direction in `AGENTS.md`.
- Lock the exact-hash Phase-0/1/2 lifecycle, `k1` density boundary, retention-only
  `k2`, single-outstanding shard checkpoint, mandatory replay-backed `kQuorum`,
  positive watchtower eligibility, and protocol-level GL0 correction semantics.
- Freeze typed meanings for ML0 source signature, admission/custody receipt,
  execution signature, watchtower coverage/evidence, global optimistic
  attestation, and downstream Phase-2 reference. No type may substitute for
  another.
- Create a one-owner/one-RED-test/one-closing-commit ledger for every open
  CRITICAL/HIGH audit finding and a write-set manifest for delegated work.
- Gate: `ARCH-001` through `ARCH-003`, `SIG-001` through `SIG-005`; repository
  search rejects a target global BFT lifecycle.

### E1 - Hash-bound finality, density selection, and atomic reorg (`PARTIAL`)

**Depends on:** E0. **Can run with:** E2 and supporting tracks S1-S3.

- Replace ordinal watermarks with durable exact `(ordinal, hash, parent,
  stateRoot, evidence)` phase state and replacement events.
- Implement/validate the real K/alpha/beta Avalanche/Snowball cascade as the
  optimistic Phase-2 rail and `k1` depth as its Nakamoto fallback. Neither rail
  validates economics.
- Use `maxvalid-tk` for short forks and valid-only `maxvalid-bg` density selection
  beyond `k1`. Separate storage retention from fork-choice eligibility; `k2`
  never makes a less-dense chain valid or final by fiat.
- Add one crash-consistent reorg transaction covering chain head, MPT branch,
  Phase-2 refs, checkpoint anchors, binary confirmation/requeue, tower caches,
  and downstream outbox. Missing history triggers authenticated recovery before
  comparison/mutation.
- Gates: `FIN-M-*`, `FIN-D-*` revised for retention-only `k2`, `FIN-W-*`,
  `FIN-S-*`, `FOLLOW-001`, `REC-001`/`REC-002`.

### E2 - Historical validator, stake, eta, and key evidence (`PARTIAL`)

**Depends on:** E0 and canonical identity primitives from S1.

- Derive eta-period-N eligibility from the exact candidate branch: the atomic
  key/authorized-roster/stake view from N-2, eta evidence from N-1, and the
  active parameters for N. The KES step is derived from N and the N-2-resolved
  pair's registered offset. Receiver-current validator state is never substituted.
- Bind admission, execution, watchtower, optimistic sampling, tower trials, and
  evidence verification to their exact historical registry/parameter roots.
- Make slash/cooldown activation branch- and eta-period-bound; prevent key splitting,
  period grinding, mixed-roster quorum, and replay across reorg/restart.
- Gates: `CRYPTO-001`, `PARAM-001`, `PERM-*`, `SHARD-C-007`, `SHARD-S-002`/
  `SHARD-S-003`, historical-roster adversarial vectors.

### E2K - Canonical preregistered operator keys (`PARTIAL`, runtime eligibility blocked)

**Depends on:** E0 and S1 identity/codecs. The immutable-genesis safety cut can
finish independently. Runtime activation depends on E1's authenticated exact-
parent branch view and on resolution of O-11; checkpoint consumers additionally
depend on E4's exact Phase-2 reference schema.

**Invariant:** every KES signature, VRF proof, or registered-VK draw resolves one
atomic `(PeerId, KES master VK/offset, VRF VK)` pair from the artifact's exact
historical branch context. Wire-carried keys and KES steps are comparison evidence
only. Registration proves key ownership and never grants operator eligibility.

**Current landing is partial, not runtime completion:**

- The split mutable KES/VRF authorities have been replaced by one atomic paired-
  key registry with read-only projections. The complete long-term-signed genesis
  pair records are committed in the genesis-derived MPT state, and startup/restart
  compares local material with that rooted commitment.
- Runtime registration records, durable pending/history state, cross-operator
  duplicate-key rejection, and an `inclusionPeriod + 2` index-delay historical
  activation resolver exist. This implements the N-2 period-index model; it does
  not imply two full period durations after intra-period inclusion. The resolver
  can intersect active pairs with a separately supplied delayed roster and stake
  view in focused tests.
- The runtime certificate still lacks an explicit network/genesis/era domain and
  a containing snapshot/state-root/registry-root witness. Acceptance does not
  compare the new pair with the same operator's prior pair, so full-pair reuse
  and a complete record in which only the KES or only the VRF key changes remain
  accepted. Atomicity means one record selects both active keys; it does not
  itself require both byte strings to change. The permitted reuse/rotation
  policy is an open owner decision and blocks runtime activation.
- An exact-hash hot-chain view adapter now exists, distinguishes same-ordinal
  siblings by requested hash, and rejects mismatched, malformed, or missing
  results without disk, ordinal, head, or current-state fallback.
  Production does not yet construct and supply that adapter to the resolver, nor
  supply a canonical runtime roster. Existing migrated consumers therefore
  accept the committed period-zero pair only and fail closed on runtime records.
  This is intentional containment, not runtime rotation support.
- Production has no scheduled local secret-bundle lifecycle. GL0 and shard VRF
  secrets are derived from the long-term identity, while `OperationalKeyMaker`
  loads and destructively evolves one named KES secret. No exact registration ID
  atomically selects a future VRF secret and fresh KES tree. A historical resolver
  swap alone would make a newly active public pair unusable by its honest operator.
- The genesis key commitment is not by itself an eligibility roster. Until the
  separately authorized immutable genesis operator/stake population is also a
  canonical rooted input, the genesis-only path is a development safety cut, not
  permissionless membership. O-11 remains unresolved and blocks runtime admission
  of new operators even though their key-registration bytes can be persisted.
- The live GL0 leader/receiver, `EtaStateManager`, both GSAM boundary/follower
  construction sites, and admission-anchor callback accept only a typed proved-
  complete, nonempty N-1 VRF-output interval for N>=2. Partial/empty history
  defers, with no producer-carried/bootstrap substitution. GL0 exact-parent
  binding is now explicit: `getEtaAt(period,parentHash)` bypasses receiver-current
  MPT state and ambient memoization, GSAM supplies its `parentTip`, and admission
  walks the exact Phase-2 anchor. Exact binding remains CRITICAL/open only for the
  shard path: the signed checkpoint lacks an exact GL0 hash/root, while committee
  resolution and shard producer/attester proof eta still use one-argument ambient
  `getEta(period)`. Add a signed exact Phase-2 `(ordinal,hash,mptRoot)` and route
  every shard validity consumer through `getEtaAt`. The complete-empty result also
  remains a protocol/liveness decision and cannot reuse unavailable history.
- Currently inventoried operative consensus fixtures have moved to the canonical
  committed-genesis fixture. Direct pair construction remains only in registry
  algebra and explicit negative/runtime-unavailable cases. A static source-
  inventory guard now fails on new split-registry factories, unreviewed raw VRF
  consumers, or changed direct-constructor locations/counts. It does not discover
  every higher-level sortition, eligibility, duty, proof, or verifier call. This textual
  allowlist does not prove an allowlisted consumer uses the correct historical
  branch, so K7 still needs cross-consumer qualification.
- Tower verification now resolves the current atomic period-zero pair for every
  header occurrence, cryptographically binds each proof to the header's exact
  carried `eta || slot`
  and its carried output, rejects malformed/empty nonempty-proof shapes, and
  ignores sender `activePoolSize`. Every otherwise-valid nonempty proof still
  fails closed because exact branch-historical roster/stake/eta eligibility is
  unavailable. Backfill lacks the full registered KES/VRF/eta context, and
  slashing lacks production exact-offence-parent historical resolution.

Deliver E2K in the following order; a later cut cannot bypass an earlier gate:

1. **K0 - Complete the immutable-genesis safety cut (pair root landed;
   population open).** The complete unique paired-key set, long-term signatures,
   local secret/public check, duplicate rejection, and rooted restart
   materialization are implemented. Root the separately authorized immutable
   genesis operator/stake population and route every currently reachable KES/VRF
   consumer through the pair/population intersection before this cut can close.
   Gates: `KEYREG-001`, `KEYREG-005`, `KEYREG-011`, `KEYREG-012`, and the genesis
   case of `KEYREG-013`.
2. **K1 - Freeze O-11 and its state ownership.** Specify the permissionless GL0
   operator/Sybil-resistance rule, including bond/stake requirements, activation,
   exit, slash/cooldown, and the exact period-boundary roster root. Define its
   canonical codec, MPT ownership, undo/refold, retention, and recovery contract.
   A seedlist, live peers, positive stake alone, or a valid key registration is
   never a substitute. Gate: owner-ratified O-11 plus `KEYREG-013`, `PERM-*`, and
   `PARAM-001` RED vectors.
3. **K2 - Make the branch-historical view load-bearing.** Resolve period `N` from
   the authenticated candidate-parent `(ordinal, hash, stateRoot)`, using the
   exact `N-2` paired-key/roster/stake view and `N-1` eta evidence. A sibling with
   the same ordinal, a view whose hash is correct but whose decoded registry/root
   belongs to another branch, receiver-current state, or missing history must
   reject/defer before draw, proof, signing, storage, acceptance, or slash. Replace
   every list-returning eta fallback with a typed complete/incomplete exact-parent
   interval; a nonempty partial prefix is never eta authority, and a proved-
   complete empty interval follows one separately ratified canonical rule. Gates:
   `KEYREG-002..005`, `KEYREG-007`, `KEYREG-008`, and
   `CRYPTO-001`.
4. **K3 - Provision secrets, ratify O-12, and activate runtime rotation.** Before
   submitting a record, atomically provision the future VRF secret and fresh KES
   tree, durably bind them to the registration ID, and verify both public keys.
   Wire the existing durable record/activation model to K1/K2. Require
   `signedEffectivePeriod >= inclusionPeriod + 2` and presence in the exact N-2
   canonical view before activation. This is an index lookback, not an inferred
   `I+3` elapsed-duration rule. Paired
   KES/VRF public and local-secret selection is atomic. The exact-parent
   eligibility capability selects the record before the local bundle is opened;
   time/current-head selection is forbidden. Ratify the N-2 common-prefix
   stability, secret-deletion point, and recovery/rejoin rule in O-12. A reorg
   crossing an erased KES activation enters `RecoveryRequired`; it does not roll
   the KES secret back as ordinary Phase-2 state. Retaining old masters through
   k2 is an explicit forward-security tradeoff, not a hidden fallback. Gates:
   `KEYREG-003..005`, `KEYREG-007`, `KEYREG-008`, `KEYREG-010`, `KEYREG-014`, and
   `KEYREG-015`.
5. **K4 - Bind exact Phase-2 references.** Currency binaries and shard checkpoints
   bind every execution base/anchor needed for key, eta, roster, and stake lookup
   as exact `(ordinal, hash, stateRoot)` Phase-2 evidence. Proposal, replay signing,
   GL0 acceptance, reorg, and fraud adjudication use those references rather than
   a receiver head or ordinal. A same-ordinal wrong hash or root is a mandatory RED
   rejection. Gates: `KEYREG-006`, `KEYREG-007`, `KEYREG-010`, `SHARD-E-003A`,
   `SHARD-C-001B`, and `FOLLOW-001`.
6. **K5 - Migrate every consumer.** In order: GL0 leader production/snapshot
   validation/catch-up; metagraph admission; execution membership and staircase
   producer duty; replay signer and checkpoint acceptance; selected watchtower
   assignment/coverage; optimistic-finality sampling; NiPoPoW/tower production and
   verification; slashing/fraud evidence. Each consumer uses the same exact-parent
   API and cannot accept a transport key or a consumer-local population. Gates:
   `KEYREG-006`, `KEYREG-009`, `KEYREG-010`, `FIN-B-004`, `ADMIT-003`,
   `SHARD-S-003`, `TOWER-004/005`, and the corresponding subsystem suite.
7. **K6 - Close reorg, recovery, and evidence semantics.** Orphaned registrations,
   rotations, assignments, checkpoints, attestations, and tower hits unwind and
   refold atomically, but erased KES secrets do not. Historical evidence resolves
   the pair and KES step at the exact signed branch; unavailable authenticated
   history or an N-2-crossing secret mismatch enters the ratified recovery state
   and can never slash. Gates: `KEYREG-007..010`, `KEYREG-015`,
   `REC-001`/`REC-002`, and false-slash vectors.
8. **K7 - Qualify migrated fixtures and retain the inventory tripwire.** The
   currently inventoried operative fixtures commit a complete genesis pair or use
   a rooted runtime-unavailable negative case. Arbitrary keys remain legal only in
   isolated crypto-primitive tests. The repository guard inventories split
   registry factories, raw production VRF consumers, and direct pair construction,
   and fails on an unreviewed change. Extend it to a complete semantic manifest of
   higher-level sortition/eligibility/duty/proof/verifier entry points.
   `CommitteeSortitionSuite`, `CommitteeShardSortitionSuite`, and
   `EligibilityCheckerSuite` now use loader-validated period-zero paired identities;
   wrong-key cases use another registered identity and statistical coverage varies
   canonical draw inputs instead of minting disposable keys. Complete the semantic
   generated-branch/consumer matrix and generated-unregistered-key no-side-effect
   vectors; an allowlisted path is not automatically correct. Gate: `KEYREG-006`,
   `KEYREG-011`, and a zero-unapproved-call-site inventory artifact.

### E3 - Portable NiPoPoW tower (`SCAFFOLD ONLY`)

**Depends on:** E1, E2, E2K, and S1. **Must precede:** permissionless proof claims.

- Put branch-bound, per-level trial state and last-hit links in the signed
  snapshot certificate. Producer computes it; every recipient independently
  reproduces it before accepting or attesting to the snapshot.
- Commit the exact eligible historical tuple in the consensus SMT and verify the
  parent-to-child SMT transition on produce, follow, restart, and bootstrap.
- Replace volatile/append-only assumptions with durable branch-aware caches that
  unwind/refold with density reorgs.
- Define a bounded portable proof carrying authenticated SMT paths, ancestry,
  KES/VRF/tower evidence, historical registry/eta/parameters, and freshness/
  current-chain evidence. Implement an independent proof verifier/comparator.
- Do not flip `TowerEligibility.NotComputed` until producer/verifier parity and
  root verification are load-bearing.
- Gates: `ROOT-001`, `SER-*`, `CRYPTO-001`, `LIGHT-001`, restart/reorg proof
  vectors, one-peer forgery/withholding/truncation tests.

### E4 - Replayable checkpoint schema and complete root (`PLANNED`)

**Depends on:** E0, E1 exact Phase-2 refs, E2/E2K identities, S1-S3.

- Define one strict Scodec checkpoint preimage binding network/genesis/era/
  parameters, shard/epoch/roster, parent/ordinal/duty, exact Phase-2 base hash and
  root, and one ordered bounded input list per metagraph.
- Each touched metagraph binds `preRoot/preVersion`, complete canonical byte diff,
  `postRoot/postVersion`, execution decisions, extracted signed global intents,
  and lane/DA commitments. Every writable economic field is rooted.
- Keep multiple contiguous binaries for multiple metagraphs in one checkpoint.
  Resolve each metagraph head independently by compare-and-set at GL0 embedding.
- Per-MG diffs are namespace-confined and cannot write another metagraph or the
  GL0 global conflict/nullifier/settlement partitions.
- Gates: `DIFF-001` through `DIFF-004`, `SHARD-E-002`/`003A`, `SER-*`,
  `LANE-*`, `DA-*`.

### E5 - Committee replay before every execution signature (`PARTIAL`)

**Depends on:** E4 and the deterministic framework kernel S2.

- Producer and every execution-committee signer replay the exact ordered inputs
  at the signed Phase-2 base and reproduce decisions, diff, intents, and post-root
  byte-for-byte.
- Replace naked-hash signing APIs with a non-forgeable `VerifiedExecution`
  capability. Missing base/body/era/data means defer/no-sign.
- Require distinct eligible `kQuorum` signatures. Shard depth, receipt count,
  best-tip, and ancestor position never substitute for replay signatures.
- Keep universal adopter replay until E9 diff adoption passes its complete gates.
- Current landing: shard checkpoint signatures require a sealed
  `VerifiedShardCheckpoint` minted by intake replay, including for under-quorum
  ancestor closure across committee rotation. This capability currently proves
  the root-only recreation result; E4 must extend it to the exact diff/intents/
  complete-root result before E5 can close.
- Gates: `SIG-*`, `SHARD-E-001` through `SHARD-E-005`, economic differential
  tests from S2.

### E6 - Single-outstanding checkpoint batching (`IN PROGRESS`)

**Depends on:** E4/E5. **Can run with:** E7.

- Remove configurable `pipelineDepth` and any shard-depth inclusion qualifier.
  A shard has at most one checkpoint whose exact containing GL0 snapshot has not
  reached Phase 2. Tentative embedding and ordinal equality do not release its
  successor.
- Batch multiple metagraphs and multiple parent-contiguous binaries per
  metagraph inside that checkpoint with deterministic byte limits and fairness.
- While one checkpoint is outstanding, retain/rebroadcast its exact bytes and
  accumulate later binaries for the next checkpoint. Advance the hard shard
  anchor only when the exact containing GL0 snapshot reaches Phase 2.
- Current partial landing removes `pipelineDepth` and shard-depth qualification,
  uses the exact checkpoint hash for happy-path Phase-2 release, and continues
  held-byte rebroadcast after the input buffer drains. It is not complete: the
  one-outstanding rule is producer policy only; the signed artifact and embedded
  verifier do not yet prove that the exact parent reached Phase 2, so a Byzantine
  committee can still pipeline a child and stall ancestor-first inclusion. The
  held outbox and Phase-2 anchor are volatile, same-ordinal/reverse Phase-2 reorg
  is not represented, callback delivery is not transactional/exactly-once, and
  batching still lacks deterministic byte bounds/fair continuation cursors.
- One multi-MG checkpoint is atomic at proposal validation and P2 anchoring: its
  outer shard identity and every accepted suffix must match exactly; one deferred
  or rejected MG prevents the whole anchor.
- Enforce the same retained-producer staircase duty at intake, countersigning, and
  embedded-artifact validation. Embedded validation must use portable
  proposal-parent-bound parent context, not prior shard-gossip receipt. Add a
  canonical signed-evidence upper slot bound; parent monotonicity alone permits a
  far-future-slot halt.
- Nodes that first learn a checkpoint through GL0 ingest its exact bytes before P2
  anchor/finalize handling; prior shard gossip is not required to continue the chain.
- Gates: `SHARD-C-001` through `SHARD-C-005`, `SHARD-C-007` through `010`, `REC-002`,
  and `NET-002`, including durable loss/rebroadcast, eta rotation, restart,
  tentative inclusion, Phase-2 reorg, and multi-MG batching.

### E7 - Static-auth admission and custody before signature (`PARTIAL`)

**Depends on:** E0, E2/E2K, and S1/S3. **Independent of execution validity.**

- Before an admission member signs, verify the exact ML0 source signatures
  against the branch/epoch-pinned metagraph operator allowlist or registry,
  envelope domain/size/lane, authenticated parent, and `ordinal = parent + 1`.
- Derive the secret per-binary admission draw from authenticated parent state,
  not a producer-claimed ordinal/eta. Durably retain the exact committed bytes
  through the required execution/challenge/recovery horizon.
- Sign a domain-separated admission/custody receipt that cannot decode or count
  as execution validity. Specify a deterministic censorship/offline fallback.
- Gates: `ADMIT-001`/`ADMIT-002`, `SIG-003`/`SIG-004`, `DA-*`, `NET-*`.

### E8 - Positive pre-inclusion watchtower coverage (`SCAFFOLD ONLY`)

**Depends on:** E2/E2K, E4-E6, and S2.

- Deterministically select a noncommittee complement/sample with minimum
  coverage from the same branch-bound historical eligibility state.
- Every selected watchtower independently replays exact inputs/base and records
  a positive match or objective mismatch. A checkpoint is not GL0-inclusion-
  eligible until the required positive coverage exists; unrelated GL0 work
  continues while it waits.
- Positive coverage is a separate exact-checkpoint-hash signature domain minted
  only from a non-forgeable local watchtower replay capability. Every adopter
  verifies assignment, distinct noncommittee identities, signatures, and minimum
  threshold. An authenticated assigned mismatch quarantines the checkpoint
  pending objective adjudication even if positive count is already sufficient.
- No checkpoint-derived transfer, fee, lock, stake/collateral, reward, mint/burn,
  cross-MG effect, withdrawal, bridge effect, or acknowledgement becomes usable
  before coverage.
- Gates: `WT-001`, `WT-002`, `WT-004` through `WT-006`, `WT-008`/`008A`.

### E9 - Ordinary GL0 diff adoption and global settlement kernel (`PLANNED`)

**Depends on:** E1, E4, E5, E8, and S2. **Security cutover point.**

- Ordinary noncommittee GL0 nodes verify identities/quorum, exact base, parent/
  ordinal, pre-root/version, input commitments, the positive-coverage certificate
  and absence of a pending authenticated mismatch, diff namespace/canonicality,
  then apply the diff and recompute every post-root.
- Resolve every binary's signed origin to exact canonical Phase-2 historical
  state before replay/sign/inclusion, require nondecreasing refs within each MG
  segment, and never use receiver live head, peer-local state, wall clock, or a
  self-claimed root. Compare-and-set every touched MG against proposal-parent
  mirror root/version before composition.
- Only after those checks pass, replace ordinary universal CL1 recreation with
  zero-recreation diff adoption. Never install a claimed root.
- Every GL0 node runs the small deterministic global ordering/conflict/nullifier/
  settlement kernel over committee-extracted signed intents. Apply mirror diffs
  and GL0-owned settlement overlay atomically without letting either overwrite
  the other.
- `numShards=1`, `2`, and `K` use the identical transition function.
- Gates: `SHARD-E-003`/`004`, `DIFF-*`, `XMG-001` through `XMG-005A`,
  `XMG-007`/`008`/`010`, `SHARD-C-004`/`005`, `WT-008`/`008A`, and
  conservation/replay tests from S2.

### E10 - Downstream exact-hash rebase and historical-read recovery (`PARTIAL`)

**Depends on:** E1 and E9. E9 owns the exact-origin/CAS validity checks required
for the security cutover; E10 carries those exact refs through downstream
delivery, rollback, and recovery.

- Downstream APIs and durable events carry exact Phase-2 `(ordinal,hash,root)`
  identities and historical proof material, never a monotone ordinal watermark.
- Density replacement reverses dependent checkpoint anchors, mirrors, settlement,
  nullifiers, and deliveries, then re-follows/rebases from the exact replacement.
- Historical-view recovery verifies the same exact-origin and nondecreasing-ref
  rules used by E9; missing local data fetches authenticated content or enters
  `RecoveryRequired`, never receiver-live-head fallback.
- Gates: `XMG-006`, `FOLLOW-001` through `FOLLOW-005`, `REC-*`, `MEMPOOL-001`,
  and `GROWTH-001`.

### E11 - Exceptional challenge replay and sound slashing (`SCAFFOLD ONLY`)

**Depends on:** E2, E4-E10.

- An assigned, bonded, rate-limited challenge names exact retained inputs/base,
  checkpoint, signers, and reproduced mismatch. An assertion alone never rolls
  back or slashes.
- First version performs bounded exceptional universal GL0 replay; its result
  decides mismatch, rollback/quarantine, signer-specific debit, and reward.
  Missing data defers and cannot slash.
- Evidence is deterministic, permanent/exact-once, branch-aware, and distinguishes
  an honest replay on a later-orphaned Phase-2 base from execution fraud.
- Gates: `WT-001` through `WT-007`, `CRYPTO-001`, `REC-*`, resource/flood tests.

### E12 - Downstream exact-hash rebase and return path (`PARTIAL`)

**Depends on:** E1, E6, E9-E11.

- GL1/ML0/CL1/DL1 adopt exact canonical Phase-2 GL0 state. CL1 does not replay or
  override it on the return path.
- A Phase-2 density replacement sends exact old/new/MRCA data. Shard anchors and
  checkpoint windows roll back; orphaned binaries requeue once; ML0 uses its
  registered deterministic rewind/rebase contract or starts a new ML0 epoch.
- Noninvertible external effects wait for the operator/integrator's declared risk
  horizon; protocol state remains reversible according to the GL0 chain.
- Remove GSI/peer/local cache authority only after typed MPT equivalents pass
  parity and restart/catch-up tests.
- Gates: `FOLLOW-001` through `FOLLOW-005`, `MEMPOOL-001`, `GSI-*`, `REC-*`.

### E13 - Protocol-level GL0 correction (`PLANNED`)

**Depends on:** E1, E9, E10, E12, and S2.

- Define a globally deterministic, root-covered correction artifact owned by the
  GL0 protocol/active era. It binds target metagraph, expected lineage and
  pre-root/version, correction diff/reason/activation, and post-root/version.
- Every GL0 validator independently verifies/applies it; downstream layers rebase
  from canonical GL0. No ML0/CL1/DL1 signature, claimed field, or opaque payload
  can authorize the correction.
- Preserve historical read ability needed by the future upstream-v4 snapshot-
  genesis importer without retaining undeployed fork-only runtime schemas.
- Gates: `CORR-001` through `CORR-003`, `XMG-011`, `FOLLOW-006`, `DIFF-*`,
  `ECON-BAL-001`, conservation, and restart/reorg vectors.

### E14 - Staged activation and independent qualification (`PLANNED`)

**Depends on:** every launch-enabled epic and supporting track.

- Keep economic sharding fail-closed until all predecessor gates pass. An unsafe
  development profile must be explicit and cannot share production parameters.
- Activate in stages: model/component; deterministic multi-process; partition/
  restart/reorg; adversarial committee/watchtower; multi-MG cross-shard; long-run
  permissionless testnet; exact-candidate independent audit.
- Exercise currency-only and currency-with-data metagraphs, shard counts 1/2/K,
  eta/committee rotations, mixed historical refs, admission censorship, colluding
  `kQuorum`, eclipsed watchtowers, density replacements, and recovery beyond local
  retention.
- No release based on aggregate test counts. Archive seed/fault schedule,
  parameter/era hashes, exact refs/roots/diffs/signers/nullifiers/supply, restart
  boundaries, and independent source-audit verdict for the candidate commit.

## Parallel supporting tracks

These are consensus dependencies, not optional cleanup:

| Track | Work | Earliest parallel start | Blocks |
|---|---|---|---|
| S1 canonical identity/serde/era | One bounded ScodecV1 representation and domain-separated preimage for every active artifact; exact parameter/era registry; delete undeployed compatibility paths. | After E0 vocabulary | E2, E2K, E3-E5, E7, E13 |
| S2 deterministic framework oracle/kernel | Authorization, checked arithmetic, conservation, semantic replay protection, ordered execution, resource bounds, independent prefix oracle. | After E0 economic grammar | E4/E5/E8-E11/E13 |
| S3 lane and DA contract | Explicit currency and currency-with-data lanes; isolated custom commitment; exact input/chunk retention; no decoder-based dispatch. | After E0 lane decision + S1 primitives | E4/E7/E8/E11 |
| S4 transport/resource/recovery harness | Bounded gossip/RPC/HTTP, durable outboxes, exact-hash multi-peer recovery, fuzz/fault harness. | RED tests can start after E0 | E1/E3/E7/E11/E14 |

Work may be delegated in parallel only with disjoint write sets and frozen shared
types. Model/RED authors do not approve their own runtime implementation. Shared
hotspots (`FinalityGate`, checkpoint schema/codecs, GSAM, MPT transaction code,
and consensus parameters) have one integration owner and merge in dependency
order.

## Historical roadmap (2026-05-16, superseded)

The implementation work that remains after the finality-trigger stack + GKL composition doc + empirical sim validation lives in **`docs/nakamoto/IMPLEMENTATION-PLAN-POST-VALIDATION.md`**. Phases:

1. **§1.1 Stake-weighted VRF** (parallel track, 5-8 d) — combined `delegatedStake + nodeCollateral` weighting; foundation for §3, §4.A.
2. **§1.2 KES port from Bifrost** (parallel track, 15-25 d) — forward-secure signatures; prerequisite for §3, §4.C.
3. **§2 Avalanche-attestation cascade** (independent, 12-18 d) — Snowball `(K=8, α=5, β=10, Δ=slot/2)`; production parameters empirically validated.
4. **§3 NIPoPoW level-µ chains** (needs §1.1+§1.2, 20-30 d) — historical proposal for `L = 10` domain-separated VRF trials per slot and `T_depth2` anchoring. Target tower eligibility/proofs are P2 hash-bound; `k2` is only recommended local proof/rollback retention.
5. **§4.A Cross-shard Option A** (superseded mechanism) — proposed secret VRF assignment of operator keys to shards. Target v1 uses public VK-hash execution membership, committee replay-before-sign, and noncommittee diff/root verification.
6. **§4.C Cross-shard Option C** (needs §1.2+§4.A, 15-25 d) — slashing of `nodeCollateral` on detected equivocation; KES-anchored evidence non-repudiation.
7. **§5 Sharding proper** — historical scope statement. Current code gates execution sharding behind `numShards > 1`; target economic mode treats one as one shard and uses the same committee/diff/watchtower protocol at every shard count.

Critical path: §1.2 → §3 → §4.C ≈ 50-80 person-days. Whole-roadmap sequential: 79-124 person-days. Process rule introduced: empirical validation must precede doc commitment (§0.4 / §6.3 of the companion).

---

## Current milestone (2026-05-14) — MPT overlay e2e validation

**Status: iter31 full e2e PASSED** (`feature/serde-typeclass-shim` @ `f51252ef`).

- 8 gl0 + 2 metagraphs, 4212s runtime, EXIT=0
- All 11 test phases green (delegated-staking → token-lock-replacement → multi-metagraph K=2 → currency → rewards → token-locks → allow-spends → spend-transactions → data-tx-no-fee → data-tx-with-fee)
- Per-gl0 finality (preserved at `/tmp/iter31-cluster-logs/`): 488–500 **ATTEST-FINALIZED**, **0 DEPTH-FINALIZED**, 0–3 fork branches per node, all attestations `weight=0.75, 8/8 active`. Cluster is healthy; depth-k fallback never engaged.

**Open follow-ups** (memory: `project_iter31_overlay_full_e2e_pass.md`):
- dl1/cl1 `pullFinalityGated` tick=10s vs gl0 finalization ~6s/ord → follower-side download lag is the actual mechanism behind iter26's `TooFarLastValidEpochProgress`. Worked around with `allow-spends.max-epoch-progress` 200→500; structural fix is faster pull / parallel batch / direct gl0 epoch read.
- Reorg re-attestation gap: chainSelection bestTip changes don't trigger fresh `processValidSnapshot` → no Polkadot-style re-attest to new canonical. Not failing tests but a correctness gap.
- Reproducibility: iter31 is one pass; iter32 currently running for second confirmation.
- Pending memory items #118 (OverlayReader rewire of 5 gl0 HTTP read sites), #119 (n1 fork-recovery deadlock re-bootstrap), #120 (2-of-2 fragility under VRF droughts).

---

## Goal

Get **Nakamoto GL0 + new gossip** production-ready, then run a **metagraph end-to-end test** (CL0 + DL1) against it via `just` infra.

Hard fork migration is **deferred** — network can be force-forked. Stake-weighted VRF is **deferred** — equal weight for now. CL0 keeps **BFT consensus** but rides the **new sidecar gossip transport**.

---

## Workstream (in order)

### 1. Sidecar gossip — full migration  *(✅ code complete, runtime validation pending)*
Ported event gossip and BFT consensus channels onto the Go libp2p sidecar via a single generic `Rumor` topic. Added Kademlia DHT for decentralized peer discovery. Runtime validation tracked in task #8.

**What landed:**
- Sidecar `/tessellation/rumors/1.0.0` GossipSub topic + `PublishRumor` gRPC RPC
- `SidecarRumorBridge`: outbound `publishFn` (wired into `Gossip.setSidecarPublishFn`) and inbound `receive` daemon (parses `Signed[RumorRaw]` JSON, recomputes hash, offers to `rumorQueue`)
- Wired into `GlobalSnapshotConsensus` startup after `SidecarClient` allocation
- `GossipDaemon.make` accepts `nakamotoMode: Boolean` — when true, skips legacy peer/common round runners (only `consumeRumors` runs)
- Kademlia DHT in server mode in the Go sidecar; rendezvous-based discovery loop (`tessellation-nakamoto`); seedlist becomes bootstrap nodes
- All four Scala modules compile; Go sidecar builds

**Why this design wins:** because BFT consensus rumors and Tessellation events both flow through `Gossip.spread → rumorQueue → consumeRumors → RumorHandler.run`, the bridge plugs in at `Gossip.spread` (outbound) and `rumorQueue` (inbound). CL0 BFT messages get sidecar transport for free with zero CL0-side changes.

### 2. Genesis time as config param  *(✅ done)*
Centralized into a single `nakamotoGenesisTimeMs: Long` val on `GlobalSnapshotConsensus`. Resolved once at process start, env override preserved (`NAKAMOTO_GENESIS_TIME_MS`), default falls back to system time for single-node dev. Per-cluster contract documented in scaladoc. Chain-derived genesis time deferred.

### 3. Production abandonment on better gossip  *(✅ done)*
Three checkpoints in `SnapshotLeaderLoop`: (1) slot-tick gate (already existed), (2) **new pre-sign gate** between `createProposalArtifact` and signing — when closed, `chainStore.store` is skipped via `gateOpenPreSign` flag, propagating through downstream `whenA(stored)` gates, (3) pre-publish gate (already existed). Also fixed a small bug: `eventMempool.clearIncluded` was unconditional and would wrongly clear events when production was abandoned — now also gated on `stored`.

### 4. MptUndoJournal.unapplyTo wired on reorg  *(SUPERSEDED by MptOverlay — #56.10 removed the journal, 2026-05-06)*
**Status update 2026-05-12:** `MptUndoJournal` was removed in commits `5ecc0772` (G+F: delete) / `caa3559e` (D: GSAM accept() migrated through writer algebra). Fork-switch under the production-default `OverlayMode.MultiBranch` (since #56.11 / `e3538d9b`) is handled via `MptOverlay` branch checkout/discard, not journal replay. Per-branch pending writes live in the overlay's ChangeSets; commit/discard happens at finality. The "branch-aware proofs" deferral below is also resolved (`proofAtBranch` via `overlay.buildRoot` + stateless prover, #56.7 / `3bed33e7`). The historical-context paragraphs below are preserved for reference but reflect the pre-#56.10 architecture.

**Historical context (pre-#56.10):** self-healing already handles reorgs. Commit `21cec6de` wired the fallback: when MPT detects divergence post-reorg, it triggers a full rebuild from the canonical chain. The journal would be the fast path (O(reorg_depth) undo) vs self-healing's slow path (O(state_size) full rebuild). Both are correct; the journal is a performance optimization.

**Why the wire-up isn't clean today:** the reorg in `NakamotoChainStore.store` is detected AFTER the incoming fork's MPT mutations have already been applied earlier in the snapshot acceptance pipeline (validation walks `GlobalSnapshotAcceptanceManager` → `mptStore` → `wrapApply`). To call `unapplyTo` correctly we'd need to reorder the validation path: detect reorg BEFORE MPT mutation, compute common ancestor across forks, unapplyTo, then let the new fork apply forward. Half-day of work, well-defined, but speculative until we see self-healing be a bottleneck.

**What stays in place meanwhile:** journal recording (`wrapApply`) still runs — deltas accumulate, just no consumer. Cheap and harmless to keep maintained.

**Why we WILL need the journal eventually (not just for performance):**
- **Inclusion proofs for leaves at historical ordinals.** Producing a Merkle proof that a particular state leaf was present at ordinal N requires reconstructing the trie root *as it existed at N*. With a content-addressed MPT this is trivial (root pointer per ordinal). With our current mutable in-memory MPT, the journal is the only mechanism that lets us walk backwards from "now" to ordinal N's state without replaying the entire chain. Expected use cases: light-client proofs, fraud proofs, cross-metagraph state attestations, NIPoPoW witness generation.
- This is a **functional requirement**, not just an optimization — the journal becomes load-bearing for any feature that needs "state at past ordinal X" proofs.

**Revisit triggers (in order of likelihood):**
1. Metagraph end-to-end testing (#7) reveals self-healing rebuild is a bottleneck on reorgs at test cluster scale → wire `unapplyTo` for performance
2. Inclusion-proof feature work begins → wire `unapplyTo` for correctness on the read side, plus add an `applyTo(ordinal)` API to walk forward from a checkpoint
3. Content-addressed MPT migration → the whole problem dissolves; journal becomes unnecessary

### 5. Parametrize finality (historical implementation record; target semantics supersede it)

> The original completion claim below is withdrawn. In particular, `T_count` is
> not a finality rail and `T_depth2`/`k2` is not Phase 3 or an immutable floor.
> The active E1/E3 work above replaces these semantics while retaining useful
> source provenance.
Env-var knobs with sensible defaults — `NAKAMOTO_ATTESTATION_THRESHOLD` (default 2/3, in `TipTracker.FinalityThreshold`, shared by `T_weight` and `T_count`), `NAKAMOTO_CONFIRMATION_DEPTH` (default **255**, in `SnapshotLeaderLoop.ConfirmationDepthK` / `T_depth1`), `NAKAMOTO_ARCHIVAL_DEPTH` (default **65536** = 2¹⁶, in `SnapshotLeaderLoop.ArchivalDepthK` / `T_depth2`), `NAKAMOTO_OPTIMISTIC_MIN_FRACTION` (default 0.5, in `StakeRegistry.MinActiveQuorumFraction`), `NAKAMOTO_MAX_ATTESTATION_SKEW_MS` (default 60_000, in `TipTracker.MaxAttestationSkewMs`). All gates always run; whichever fires first finalizes. No mode switch.

**k measures snapshots, not slots.** With LDD targeting ~15% slot fill, slots run ~6× sparser than snapshots, but the depth gate is purely an ordinal-distance check: `tip.ordinal - snapshotOrdinal > k`. The original k=31 choice (2026-04-08) was based on a measured-sim table topping out at k≤80 with k=6 too high a fork rate against a 1/3 adversary; k=31 gave ~0.91% per-attempt. Subsequent expanded sims (`adv_depth_expanded_parallel.py`, 10M trials, k≤400) extrapolate the fB=0.05-tail slope to ~10⁻¹² at k≈271–290. Default raised to **k=255** as a conservative operating point approximating Cardano-equivalent CP-violation; attestation finality remains the hot path (seconds), so this only affects worst-case finality time during degraded operation.

**Two slot/ordinal-units bugs were fixed in this round** (2026-04-08), discovered while validating the wire-up: `lastFinalizedOrdinal` was being read off `tipTracker.lastFinalized`'s **slot** value, and `finalizeAtSlot` was being computed as `tip.slot - k` (mixing slot- and ordinal-units). The first bug had silently disabled the depth gate end-to-end since it landed — in our 720s e2e test we observed 224 ATTEST-FINALIZED entries and **zero** DEPTH-FINALIZED entries. Both gates now read their inputs from the chain store, which is the authoritative ordinal source.

**Finality-trigger stack refactor (2026-05-15).** The two inline gates (depth-k₁ + attestation-2/3) were extracted into a `FinalityTrigger[F]` typeclass (`modules/node-shared/.../nakamoto/FinalityTrigger.scala`) so each trigger is a monotone-Ref-backed observable that `SnapshotLeaderLoop.finalityMonitor` evaluates and advances on each tick. With the refactor:

- **`T_count`** added (commit `7003be21`) — 1-validator-1-vote canonical-hash-filtered count finality, self-excluded (#133), denominator = `StakeRegistry.validatorCount` (full seedlist). Reuses `TipTracker.FinalityThreshold` so it ties with `T_weight` under equal stake and is strictly stronger evidence once stake-weighted VRF lands.
- **`T_depth2`** added historically (commit `06455f98`) as a claimed Phase-2-to-Phase-3 archival gate. That interpretation is rejected: `k2` is retention/recovery capacity only, and pruning must never become fork-choice truth. The associated `MptOverlay.pruneBelow` provenance remains relevant to the replacement recovery design.
- **`T_weight`** got self-exclusion via #133 (commit `95471c7f`) — `TipTracker.highestFinalizedOrdinal` now takes a `selfId: PeerId` parameter and drops the self-entry before the canonical-hash filter. Partial mitigation of #119 fork-recovery deadlock.
- **Re-bootstrap reset machinery landed** (commit `01ebcca6`, task #141) — `RebootstrapOrchestrator` observes sustained `chainStore.divergentRefuseCount`, then resets TipTracker/Overlay/finality state. The typed-HOCON setting is live-default `true`. Reset is not itself recovery: completion now depends on ordinary verified ancestry replay because direct peer-context/state installers were removed. Fresh end-to-end validation is required.
- **`attestedAt` skew bound** (commit `422e1a6b`) — receive-side defense-in-depth for `T_count`. Drops attestations outside ±`NAKAMOTO_MAX_ATTESTATION_SKEW_MS` of `Clock[F].realTime`; counter `dag_nakamoto_attestations_rejected_skew_total`. Tightenable post-Chronos.
- **Chain-quality observable** (commit `866cd598`, task #138) — `FinalityTrigger.triggersFor(ord)` lookup answers "which triggers qualified ord N?" at both finalize sites (gauge `dag_nakamoto_chain_quality` ∈ {1, 2, 3}; per-kind counters) and via HTTP route `GET /global-snapshots/{ord}/finality-triggers`. Pure observability — never feeds back into consensus.
- **`SlotCertificate.parentSlot` wiring** (commit `bec9de6b`) — `NakamotoProposer` was passing `parentSlot = Slot.MinValue` (TODO placeholder); now threaded through correctly so verifier-side `slotGap = cert.slot - cert.parentSlot` reconstruction matches the producer's LDD lottery threshold.

See the supersession notice in `docs/nakamoto/attestation-and-finality.md` and
`docs/review/CONSENSUS-ARTIFACT-LIFECYCLE.md` for the target three-phase
P0/P1/P2 contract. The historical four-phase trigger record below is not target
semantics.

### 6. Close SC binary finality loop (CL0-side)  *(✅ done)*
**Bug:** original `pruneConfirmed` dropped a binary on first sight in any GL0 snapshot — if that snapshot was later orphaned in a Nakamoto reorg, the binary was permanently lost.

**Fix landed:**
- **Data model:** `BinaryTracker.pruneFinalizedBelow(SnapshotOrdinal)` only prunes ConfirmedBinary entries whose `proof.globalOrdinal <= lastFinalizedGlobalOrdinal`. `StateChannelBinarySender.confirm` gained an optional `lastFinalizedGlobalOrdinal: Option[SnapshotOrdinal]` parameter defaulting to the snapshot's own ordinal (BFT-preserving).
- **GL0 endpoint:** new `GET /global-snapshots/latest/finalized-ordinal` route on `SnapshotRoutes`. In Nakamoto mode, dag-l0 wires it to a `Ref[F, Long]` that `SnapshotLeaderLoop` updates after every successful `chainStore.finalize` call (depth-k or attestation-2/3, whichever fires first). In BFT mode the route defaults to head ordinal — semantically correct since BFT snapshots are immediately final.
- **CL0 caller:** `StateChannel.scala:172` now calls `services.globalL0.pullLatestFinalizedOrdinal` (best-effort, falls back to legacy snapshot-own-ordinal on error) and passes the result into `stateChannelBinarySender.confirm`.

**Validated live:** in the metagraph e2e (#7 below), `GET /global-snapshots/latest/finalized-ordinal` returns a real value (`{"value":159}`) at end-of-test, proving the route is reachable, the Ref is being updated, and CL0 is consuming it.

### 7. Metagraph end-to-end via `just`  *(✅ done)*
Updated `just test` to launch CL0 + DL1 against a Nakamoto GL0 cluster (`--use-test-metagraph --num-gl0=3 --nakamoto-gl0`). Currency e2e test suite (DAG transfers + L0 token transfers + double-spend prevention for both) ran to completion in **622s** test time / **720s** total against 3-node Nakamoto GL0 + sidecars + 3 GL1 + 2 ML0 + 3 CL1 + 3 DL1. This is historical evidence only; it does not validate the target replay-before-sign/diff-adoption path, Phase-2 rollback, or exact recovery.

**Also validated:**
- Sidecar gossip migration end-to-end: BFT consensus rumors and Tessellation events both flow through Go libp2p GossipSub (no legacy HTTP gossip) and CL0 BFT consensus still reaches finality.
- DHT-only peer discovery: Go sidecar `-disable-mdns` flag forces all peer discovery through Kademlia, validating multi-host readiness (mDNS cannot cross subnets).

---

## Decisions locked in this session

| Topic | Decision |
|---|---|
| CL0 consensus | Keep BFT, ride new sidecar gossip |
| Stake-weighted VRF | Deferred — equal weight `1/N` |
| Hard fork migration | Deferred — force-fork the network |
| Genesis time discovery | Config param baked into binary |
| Mempool reinsertion (DAG txs) | Not pursuing GL0 reinsertion |
| SC binary reorg recovery | CL0-side: wait for GL0 finality before pruning |
| Configurable finality | Parametrize knobs only — no mode switch |

---

## Out of scope this round
- Metagraph (CL0/DL1) Nakamoto consensus port — staying BFT
- Hard fork migration / dual-mode dispatch
- Stake-proportional VRF
- Content-addressed MPT
- NIPoPoW superblocks
- VRF-sortitioned attestation committees
- Two-level finality
