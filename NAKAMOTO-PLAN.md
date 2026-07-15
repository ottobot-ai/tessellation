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

**Owner-decision status:** `O-01` through `O-17` are `17/17`
dispositioned in `docs/review/CONSENSUS-OWNER-DECISIONS-ANSWERS.md`. Every O-item
dependency below means implementation of its ratified direction and closure of
its explicitly listed engineering, research, schema, parameter, or proof gates;
none means that an owner response is pending.

## Active objective

Safely enable framework-economic interactions among metagraphs assigned to
different execution shards, with every effect mediated by the selected GL0
snapshot chain:

```text
native client -> GL1 -> universal GL0 native execution/validation

CL1 framework lane -> ML0 signed currency envelope -> GL0 admission
        -> execution shard replay-certified checkpoint
        -> positive watchtower coverage -> GL0 diff adoption
        -> global conflict/nullifier/settlement kernel

DL1 custom lane -> ML0 signed data commitment/DA payload
        -> GL0 admission + authenticated custody/availability/ordering only

exact Phase-2 GL0 state -> downstream adoption + reorg/rebase notifications
```

The sharded replay optimization applies only to ordinary noncommittee processing
of CL1 framework transitions carried through ML0 checkpoints. It never applies to
the direct GL1 path: every GL0 validator independently executes and validates
native DAG-token transitions against the exact proposal parent. In the target,
every GL0 validator also executes the deterministic global
conflict/nullifier/settlement kernel; that kernel is E9 planned work, not a current
claim. A `FrameworkCurrencyWithData` envelope follows both lanes: only its
framework portion is replayed, while its isolated custom commitment remains DA
carriage.

GL0 consensus remains Nakamoto/Taktikos/LDD. Avalanche/Snowball is only the
optimistic exact-hash Phase-2 rail. Do not introduce global proposal/vote/lock,
QC, or view-change machinery. ML0 may retain BFT consensus.

`k1` separates short-chain `maxvalid-tk` selection from long-range
Genesis-style `maxvalid-bg` density selection. `k2` is a retention, recovery,
and external-risk recommendation; it is not an absolute fork-choice floor.
A fork older than locally retained state requires authenticated recovery and
replay before switching. It must not be silently rejected, locally scored over
truncated history, or resolved by an operator choosing the winning branch.

That binary short/deep rule does not yet define selection over a frontier of
three or more fully validated tines. The primary Taktikos and Genesis algorithms
specify stateful incumbent folds; they do not specify or prove an
incumbent-independent total frontier order. Structurally connected bare
`ChainTip` inputs form a strict-preference three-cycle without using the current
tie rule, but they are not authenticated snapshots passing VRF/KES/era validation
(`ChainSelectionSuite.scala:191-226`). O-15 ratifies objective
total-frontier semantics: the same cutoff-complete published valid frontier and
branch-authenticated parameters must produce the same head independently of
candidate order, arrival schedule, restart, or prior incumbent. The exact
cutoff/bounded-diffusion and late-reveal semantics, cycle-resolution selector,
portable frontier evidence and verifier, validator/store witnesses, security and
liveness proof, `k1` distance/equality boundary, and objective tie rule remain open
before live fork choice can authorize
`FinalityGate`. This is a Nakamoto fork-choice question; it does not permit
global BFT voting, locks, QCs, or view changes.

## Completion loop

Every task, including documentation and schema changes, follows the same loop:

1. **Freeze:** name the invariant, signed preimage/state ownership, adversary,
   dependencies, and exact source baseline.
2. **RED:** add an exploit regression or independent model/oracle that fails on
   the baseline for the intended reason.
3. **Implement:** make the smallest owned change behind typed boundaries. Do not
   weaken the temporary ordinary-GL0 replay of sharded CL1 checkpoints until its
   replay-certified diff replacement and adoption checks are executable. This
   does not apply to native GL1/DAG-token transitions, which every GL0 node must
   continue to execute.
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
- Implement O-15's ratified objective total-frontier property and close its
  cutoff/reveal, cycle-resolution selector, exact metric/tie, evidence/verifier,
  validator/store witness, and security/liveness proof gates. Do not turn a
  non-transitive mixed comparator into consensus through list/map/set iteration,
  gossip arrival order, or a prior incumbent.
- Freeze typed meanings for ML0 source signature, admission/custody receipt,
  execution signature, watchtower coverage/evidence, global optimistic
  attestation, and downstream Phase-2 reference. No type may substitute for
  another.
- Create a one-owner/one-RED-test/one-closing-commit ledger for every open
  CRITICAL/HIGH audit finding and a write-set manifest for delegated work.
- Gate: `ARCH-001` through `ARCH-003`, `SIG-001` through `SIG-005`; repository
  search rejects a target global BFT lifecycle.

### Stop-the-line complete-root program (`OPEN`, blocks economic activation)

The original `ECO-IDX-01/02` defect is closed in the current worktree:
`GlobalStateKey.consensusRootEntries` retains every `SystemNamespace`
active-address and expiry entry, `consensusMptRoot` hashes that exact set, and
known index/target reads fail closed on malformed bytes, absent indexed targets,
currency addresses carrying both field-3 and field-5 union arms, or an expiry
bucket whose epoch disagrees with the target-derived expiry
(`GlobalStateKey.scala:508-544`;
`GlobalSnapshotInfo.scala:318-329,392-403`; `StrictMptRead.scala:18-37,66-127`;
`GlobalStateReaderOps.scala:164-245`; `AllowSpendStateManager.scala:358-432`;
`TokenLockStateManager.scala:383-454`; `NodeCollateralStateManager.scala:124-198`).
The canonical index projection and incremental writer now include owner indices
for `LastCurrencySnapshotsProofs` and `MetagraphSyncData`
(`GlobalStateConverter.scala:724-856,2920-2974`). This closes only the former
same-root/System-index asymmetry; it does not close the shared prefix grammar,
full MPT-to-GSI projection, authenticated bootstrap/activation binding, or
transactional installation.

Exact-target failure is not exact-parent failure. MultiBranch reads still stop
when the requested branch or an ancestor is absent from `pendingRef` and compose
the surviving delta with the mutable finalized base
(`MptOverlay.scala:772-847,1255-1283`). The fallback mechanism is confirmed; a
reachable unknown/evicted
parent with divergent base producing an economic mismatch remains OPEN and
PLAUSIBLE. After landing a root-verified persisted base anchor, checkout and
every point/prefix/root/raw-byte read must return a typed
`ParentStateUnavailable` before reads or writes. A GSI, strict target decoder, or
finalized-base fallback must never heal missing proposal-parent state.

One new rooted-state determinism defect remains: the node-local
`delegatedStaking.withdrawalTimeLimit` determines the rooted node-collateral-
withdrawal expiry key. Two nodes can rebuild the same logical GSI into different
roots (`GlobalStateConverter.scala:822-847`; `dag-l0/Main.scala:108-114`;
`MptFieldCoverageSuite.scala:325-377`; `ECO-IDX-03`, HIGH, OPEN). The active-era
value must be canonical rooted protocol state, not a local fallback.
Separately, field 32 is excluded from that root but remains a writable GL0 mirror
and a confirmed input to framework checkpoint replay. A local staged base and a
root-verified backfilled base can therefore recreate different currency state
proofs at the same signed root (`ECO-F32`, HIGH).
Until the following sequence closes, runtime stake eligibility, tower
activation, ordinary checkpoint diff adoption, and every economic deployment
must remain deployment-disabled. Nonactivating schema, model, and RED-test work
may continue; this is a release gate, not a claim that every unsafe runtime path
already has a hard-coded kill switch.

1. **Finish complete root and replay-witness ownership.** Preserve the landed
   `ECO-IDX-01/02` invariant: every System index is rooted and every indexed
   exact-key target fails closed, expiry buckets bind the target-derived epoch,
   and field-3/field-5 currency union arms are mutually exclusive. Root the
   active-era `WithdrawalTimeLimit` and
   use that value for production, replay, rebuild, recovery, and join validation
   (`ECO-IDX-03`). Do not generalize this exact-key closure to `ROOT-008` prefix
   reconstruction or to `ROOT-009` recovery atomicity. Field 32 is ML0-owned
   replay state, not a GL0 economic leaf, but it cannot simply be stripped: the
   `CurrencySnapshotStateProof.globalSnapshotSync` plus the accepted sync delta,
   not the full preimage. First bind an exact optional full-view witness for every
   checkpoint window boundary, including the explicit ML0 operator population,
   into the signed/root-bound framework replay input; preserve `None` versus
   `Some(empty)` and verify the full view against the state-proof hash. Missing or
   mismatched material defers and cannot slash. Only after that gate passes,
   delete field 32 from every GL0 MPT, diff, load, and reorg path while retaining
   it in ML0 `CurrencySnapshotInfo`. Gates: `ECO-IDX-03`, `ECO-F32`, `ROOT-007`,
   `ROOT-010`, `SHARD-E-006`.
2. **Land one strict key-aware reader.** Point, prefix, and raw reads return
   typed absent/present/malformed results with the physical MPT key/path and
   exact immutable copied value bytes. Decoding never drops an entry; reconstruction
   recomputes the expected key from the decoded identity/scope and compares it
   with the physical key before returning a value. The strict point-read
   primitive landed in `41c19903d`. Deterministic, defensively copied physical
   prefix/raw enumeration is now present at the shared store, overlay, and reader
   boundaries, including retained null/empty/malformed values, nibble-equivalent
   case aliases, and prefix-only branch-view merge tests. This foundation is
   deliberately nonauthoritative: exact-parent capture, root/snapshot/Phase-2
   binding, whole-image canonical physical-key preflight, bounded per-partition
   decoding, physical-key reproduction, scope checks, and duplicate-logical-
   identity rejection remain open. Case-distinct aliases also reproduce an
   infinite full build and mutation-history-dependent incremental roots
   (`MPT-07`); complete candidate preflight and bounded builder collision failure
   are required before any raw load or parser migration. Gates: `ROOT-008`,
   `ROOT-011`.
   A partial worktree slice now enforces generic physical grammar across full and
   public incremental producers, typed-key materialization, stateful mutation,
   disk/wire maps, overlays, and durable images; it also canonically orders public
   incremental operations and restores the in-memory savepoint on replacement
   build failure. ROOT-011 remains open for duplicate-member-preserving bounded
   transport and integration/scale tests. `ROOT-005A` now captures one immutable
   branch image and requires complete follow-proof field/range/value coverage,
   with evidence-free success limited to the canonical empty trie root. Its
   focused tests do not activate the unwired proof services. Exact generation is
   still absent: `HistoricalMptProofService` accepts but ignores `ordinal`, an
   unknown branch resolves to the mutable base, and GlobalFollow stamps the caller
   ordinal on the captured image. `ROOT-005B` remains open for exact authenticated
   `(ordinal,hash,root)` binding, typed rejection of unknown/evicted branches, and
   one owner for base, overlay, proof capture, persistence, and replacement mutations;
   `ROOT-009`/`BR-05` still own authenticated crash-atomic installation
   (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/overlay/MptOverlay.scala:46-84,286-297,953-966`;
   `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/overlay/MptOverlay.scala:1531-1538`;
   `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/HistoricalMptProofService.scala:40-52`;
   `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/GlobalFollowProofService.scala:39-47,65-112`;
   `modules/shared/src/main/scala/io/constellationnetwork/schema/nakamoto/follow/FollowVerifyCore.scala:585-683`;
   `modules/shared/src/main/scala/io/constellationnetwork/security/mpt/verifier/MerklePatriciaRangeVerifier.scala:259-268`;
   regressions `modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/GlobalFollowProofServiceSuite.scala:180-190,195-415`,
   `modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/overlay/MptOverlaySuite.scala:852-904`, and
   `modules/shared/src/test/scala/io/constellationnetwork/security/mpt/MerklePatriciaRangeVerifierSuite.scala:87-123`).
   The exhaustive target field manifest, physical shapes, codecs, identity/scope
   rules, structural/index/population relations, root ownership, and remaining
   engineering freeze gates
   are in `docs/review/ROOT-008-GL0-PARTITION-GRAMMAR.md`. `ROOT-008` parser and
   RED work for final rooted fields may proceed alongside `ROOT-010`, but the
   target manifest has no GL0 field-32 lane and cannot activate until witness
   parity permits field-32 deletion. Numeric resource limits, retired-ID policy,
   field-20/23 self-authentication, set identities, and token-lock currency scope
   must implement the ratified O-17/R008-01..07 directions and pass their codec,
   resource, and proof gates before schema freeze. ROOT-008 proves
   physical placement, codec/identity, and structural/population relations; it
   composes with, but does not replace, O-07/ECON-G economic authorization,
   conservation, backing, replay protection, and transition validity.
3. **Migrate key-blind consumers in parallel after step 2.** Close the six
   source-confirmed parser families: `MPT-01` Mg* value-only reconstruction,
   `MPT-02` consumed-allow-spend physical nullifier keys, `MPT-03` stake and
   collateral scope/keys, `MPT-04` token-lock value/head selection, `MPT-05`
   slash/cooldown keys, and `MPT-06` price/parameter keys. Their parser defects
   are confirmed; end-to-end malicious ingress remains PLAUSIBLE until the RED
   integration vectors reproduce it. Gates: `XMG-013`, `PERM-005`, `WT-010`,
   `ECON-G-002`.
4. **Harden raw recovery and tower state.** Network sync, persisted restore, and
   deep-reorg loads accept only an exact snapshot-bound complete root. Any
   non-consensus derived bytes are stripped and deterministically rebuilt from
   rooted state, never preserved as peer/disk authority (`ROOT-009`). The former
   root-invisible System-index variation is closed by `ECO-IDX-02`; this step
   remains open for exact bundle/projection binding (`BR-02`), preflighted atomic
   installation (`BR-05`), and every non-System recovery input. After the
   replay-witness migration, field 32 is rejected from
   every GL0 load rather than converted to a synthetic empty replay input.
   Malformed tower/SMT durable keys fail the whole load instead of
   disappearing during decode (`STOR-02`, `REC-004`). The strict total decoder,
   one-time validated tower index, and strict historical replay are green in
   focused worktree tests. Replay consumes one retained image, requires exact
   contiguous `0..expectedEligible` coverage including ordinal zero, preflights
   `ordinal + k`, and atomically swaps an isolated completed tree; malformed,
   gapped, failed, or cancelled replay preserves the prior tree (`STOR-02`;
   `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/DurableNipopowKey.scala:32-42,44-115`;
   `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/MptTowerStore.scala:101-108,159-262`;
   `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/HistoricalCommitmentSmtStore.scala:179-264`;
   `modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/MptTowerStoreSuite.scala:258-456`;
   `modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/HistoricalCommitmentSmtStoreSuite.scala:293-337,349-559`). The
   steady-state append path is not covered by that atomic-swap claim: it publishes
   `durable.insert/commit` before `versioned.commit`, without an all-or-none
   failure/cancellation boundary. A failure after durable publication can leave a
   retained leaf absent from the live tree, and a later append can extend that
   stale tree until restart replay derives another root
   (`HistoricalCommitmentSmtStore.scala:150-168`). Add the `REC-004` RED injection
   between those effects and require the next append plus restart replay to match
   clean replay before activation. The
   detached skip-ahead tower driver and startup provider publication are removed.
   Its staged serialized exact-hash coordinator/finalizer is deliberately
   unwired and in-memory. It still has three correctness/resource defects: public
   finalizer prepare/commit is not one single-writer generation, every bounded
   catch-up run walks the full remaining interval before slicing (`O(M^2/B)` over
   a gap), and the builder reads ordinal state then injects a tower hash without
   rehashing or binding one immutable exact target. Provider remains `None`, so
   proof routes remain unavailable/503; this also unnecessarily disables the pure
   verifier route (`STOR-03`, contained dark, not closed;
   `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala:1446-1456`;
   `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/TowerCatchupCoordinator.scala:216-312,372-513`;
   `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/TowerFinalizer.scala:89-96,180-213`;
   `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/TowerProofBuilder.scala:96-103,148-166`;
   `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/http/routes/NipopowRoutes.scala:82-87,154-168`).
   Durable-KV crash atomicity, production boot/disk wiring, durable rebuild and
   cursor, restart/compaction, exact-hash density reorg, one-writer finalization,
   paged linear catch-up, target-bound exact-hash proof construction, resource
   bounds, verifier/builder service separation, and atomic provider publication
   remain open. RED tests must force simultaneous `finalize(N/N+1)`, measure total
   and per-call walk work for `M >> B`, switch same-ordinal branches during proof
   build, and keep `POST /verify` usable while local building is unavailable.
   `SMT-01` is separately contained by active-era `smtRoot=None`, exact proof
   comparison, and fail-closed producer/validator/download/context/traverse/ML0/
   route gates. The schema field remains; future activation must reproduce it
   from an exact branch-bound durable image at every lifecycle boundary
   (`modules/shared/src/main/scala/io/constellationnetwork/validator/GlobalSnapshotActiveEraValidator.scala:10-30`;
   `modules/shared/src/main/scala/io/constellationnetwork/validator/StateProofComparison.scala:28-37,45-78`;
   `modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotStateProof.scala:124-130`;
   `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensusFunctions.scala:282-299,353`;
   `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala:715-717`;
   `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/domain/snapshot/programs/Download.scala:368-381,607-622`;
   `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/GlobalSnapshotContextFunctions.scala:61-72`;
   `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotTraverse.scala:107-116,183-194`;
   `modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/StateChannel.scala:213-230,306-315,423-432`;
   `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/storage/SnapshotStorage.scala:100-125`;
   `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/snapshot/finality/FinalizedSnapshotReader.scala:145-169`;
   `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/http/routes/SnapshotRoutes.scala:62-82`).
5. **Qualify the integrated state machine.** Run GSAM differential tests from
   identical rooted bytes with every sidecar omission/substitution, then
   restart, compaction, density unwind/refold, authenticated recovery, and
   `numShards=1`, `2`, and `K` parity. Root, decisions, expiry, stake/committee
   eligibility, slash/cooldown, checkpoint adoption, and supply must match
   exactly before any blocked activation gate can close.

The HTTP subtree-proof path has an additional field-7 residual. Commit
`41c19903d` binds the structured proof key, raw value, terminal-leaf digest, and
canonical field-25 `MgBalances` reads, but the served per-MG checkpoint root is
built from field 5 plus fields 25-31 and excludes field-7 active allow-spends.
The route therefore cannot yet provide a checkpoint-rooted field-7 membership
proof. Root field 7 in the applicable complete commitment, or prove it against
the exact Phase-2 GL0 complete root; do not activate the HTTP cross-shard read
path on the current root shape.

### E1 - Hash-bound finality, density selection, and atomic reorg (`PARTIAL`)

**Depends on:** E0. **Can run with:** E2 and supporting tracks S1-S3.

- Replace ordinal watermarks with durable exact `(ordinal, hash, parentHash,
  mptRoot, evidence)` phase state and replacement events.
- FinalityGate exposes the exact Phase-2 identity/evidence. Bootstrap transport
  binds the selected-era complete proof and content-addressed state payload/
  projections to that reference as one bundle. Never combine a finalized ordinal
  observation with a separate latest/best-tip state fetch.
- Implement/validate the real K/alpha/beta Avalanche/Snowball cascade as the
  optimistic Phase-2 rail and `k1` depth as its Nakamoto fallback. Neither rail
  validates economics.
- **Current containment (2026-07-15):** raw local GL0 attestation emitters,
  receiver-invented producer evidence, periodic best-tip re-attestation, and the
  one-round cumulative-weight finalization sink are removed. Verified remote
  attestations and unfinished trigger/accumulator objects are telemetry only;
  canonical `k1` depth is the sole live GL0 snapshot
  `NakamotoChainStore.finalizeSelectedAt` rail. The
  source/API tripwire is
  `GlobalOptimisticFinalityContainmentSuite.scala`, and
  `RTA-RED-019` proves exact-hash depth progress with no attestations
  (`NakamotoChainStoreSuite.scala`). Typed store outcomes, internal mutation
  serialization, in-memory branch/lineage revisions, and exact selected-tip
  finalization CAS have also landed. Lower instance-issued proposal/replay
  receipts now prove completion of the native-GL1-inclusive deterministic
  transition and reject null/wrong-issuer results before payload access/effects in
  the trusted unmodified JVM. Reflection after issuer extraction or hostile
  in-process bytecode is not defended. They are not storage or signing authority: the producer
  unwraps its lower receipt before certificate/eta decoration and signing, and the
  receiver's lower result does not bind the outer envelope/KES evidence. This is a
  temporary safety restriction, not E1 completion: the full
  `AuthenticatedExecutedGlobalSnapshot`, `storeValidated`, exact-tip preference
  capability, durable revisions and publication/effect journals, the sampled
  exact-hash decision transcript, and exact-hash `FinalityGate` activation remain
  open.
- Use `maxvalid-tk` for short forks and valid-only `maxvalid-bg` density selection
  beyond `k1`. Separate storage retention from fork-choice eligibility; `k2`
  never makes a less-dense chain valid or final by fiat.
- Treat the present fork-choice reference model as partial dark scaffolding only.
  It accepts a preselected winner, checks the generic shallow/deep rule tag, and
  models exact MRCA replacement; it does not compute the winner
  (`FinalityReferenceModel.scala:7-11,97-100,217-313`). The strict three-tine
  comparator counterexample blocks canonical-selection authority until O-15's
  algorithm/evidence/proof gate and FIN-D-001A close. The production store
  class/control-flow risk is now a completed store-boundary RED under a synthetic
  enabled `k`/`s` configuration: `NakamotoChainStoreSuite` constructs
  snapshots whose signatures bind ordinal and parent hash, inserts one strict
  frontier under three parent-before-child schedules, and observes final best
  tips C, B, and A (`NakamotoChainStoreSuite.scala:280-372,427-469`;
  `NakamotoChainStore.scala:437-464`). The test calls
  `NakamotoChainStore.store` directly with synthetic caller-supplied slot/VRF
  metadata and shared test context; it does not pass normal snapshot, VRF/KES,
  historical eta/registry, or era admission, and it is not a shipped-environment
  configuration witness. A complete validator-backed, active-configuration
  admission witness and corrected-store convergence test remain required before
  FIN-D-001A can close.
  `ForkChoiceDecision` currently carries one unscoped opaque artifact pointer;
  the batch separately supplies an intent-scoped locator which must equal it. It
  deliberately asserts no unproved Tk/Bg/transition form;
  no decoded complete header/tine/frontier/parameter-era evidence payload or
  verifier exists (`FinalityCore.scala:120-138,353-364`;
  `FinalityBaseCodecs.scala:96-119,178-179`;
  `FinalityIntentValidator.scala:59-120,748-761`).
- Full positive successor fixtures now construct and completely validate both
  replacement transition shapes from a valid released predecessor. This proves
  cross-field schema constructibility only, not winner semantics, authenticated
  fork-choice evidence, durable execution, or activation readiness
  (`FinalityIntentValidatorSuite.scala:388-444,896-1004`).
- Resolve the source-proven `k1` boundary mismatch before activation. When the
  bounded walk finds the true MRCA over consecutive tines, it reports maximum
  post-MRCA suffix length, while production passes `k1 + 1` and
  density requires `depth > kLookback`; FIN-D-001B records the resulting divergent
  Tk/Bg winner at depth `k1 + 1`
  (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/types.scala:173-177`;
  `GlobalSnapshotConsensus.scala:979-994`; `ChainSelection.scala:161-175,187-240`;
  `ChainSelectionSuite.scala:228-250`).
- Preserve the current dark monotone-restoration data contract. The schema validates
  one exact plan and claimed receipt shape; it does not prove that the target stayed
  unpublished or that an external MPT republish occurred. `PriorUnchanged` names the
  exact CAS prior, while `AppliedTargetReverted` names the exact prior image at the
  next publication revision. `CoordinatorHead.publication` independently retains that effective CAS
  cursor through retirement, recovery, release, and the next prepare. The terminal
  receipt cannot substitute its plan or orphan claim. The claim is not proof of a
  true MRCA or target exclusion. This is codec/validator modeling only: the durable
  store rejects restoration mutations and no live executor, branch hold, or release
  capability exists. The raw initialization bypass is now closed in the runtime-dark
  store boundary: a store-minted, lease-scoped capability verifies the initialized
  publication journal, marker, residual legacy agreement, and referenced image while
  holding the MPT publication mutex; coordinator initialization consumes and durably
  installs its mutation before returning. Both durable store interfaces are sealed,
  so package code cannot substitute a capturing implementation. Existing Running
  state requires complete publication equality, and mismatch appends an exact typed `PublicationMismatch`
  recovery record. `LocalPublicationBound` proves only that local durable equality
  during the lease. It does not authenticate Phase-2 canonicality, validate MPT entry
  semantics, or prevent the still-public `transitionActive` from changing MPT state
  after the lease. Activation therefore still requires one coordinator-owned
  MPT-plus-semantic-plus-anchor transaction and live wiring (`FinalityCore.scala:395-460`;
  `FinalityCoordinatorState.scala:68-76,99-136`;
  `FinalityIntentValidator.scala:891-925,1083-1275,1277-1288,1364-1442,1461-1608`;
  `FinalityCoordinatorKernel.scala:74-112`;
  `FinalityCoordinatorBootstrap.scala:9-84`;
  `DurableMptImageStore.scala:87-97,177-182,542-552`;
  `FinalityDurableStore.scala:293-319,1209-1230,1357-1363,1652-1656`).
- Add one crash-consistent reorg transaction covering chain head, MPT branch,
  Phase-2 refs, checkpoint anchors, binary confirmation/requeue, tower caches,
  and downstream outbox. Missing history triggers authenticated recovery before
  comparison/mutation.
- Gates: `FIN-M-*`, `FIN-D-*` revised for retention-only `k2`, `FIN-W-*`,
  `FIN-S-*`, `BOOT-001`, `FOLLOW-001`, `REC-001`/`REC-002`.

### E2 - Historical validator, stake, eta, and key evidence (`PARTIAL`)

**Depends on:** E0, complete-root steps 1-2, and canonical identity primitives
from S1. Runtime stake/committee eligibility also depends on steps 3-5.

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
parent branch view and implementation of O-11's ratified roster direction,
including its schema, parameter, and proof gates; checkpoint consumers
additionally depend on E4's exact Phase-2 reference schema.

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
  itself require both byte strings to change. The atomic-pair direction is
  owner-ratified; the explicit active-era validator for complete-pair reuse and
  one-key-change rotations remains schema/RED engineering and must fail closed
  until frozen.
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
  permissionless membership. O-11's pledge/self-bond, proportional delegation
  slashing, and `bond >= extractable value` structure is owner-ratified, but its
  exact rooted predicates, codec, and network bounds still block runtime admission
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
  unavailable. The unverifiable ordinal-range archival backfill path has been
  removed; any replacement must use full registered KES/VRF/eta evidence and the
  normal parent-first snapshot validator. Slashing still lacks production exact-
  offence-parent historical resolution.

Deliver E2K in the following order; a later cut cannot bypass an earlier gate:

1. **K0 - Complete the immutable-genesis safety cut (pair root landed;
   population open).** The complete unique paired-key set, long-term signatures,
   local secret/public check, duplicate rejection, and rooted restart
   materialization are implemented. Root the separately authorized immutable
   genesis operator/stake population and route every currently reachable KES/VRF
   consumer through the pair/population intersection before this cut can close.
   Gates: `KEYREG-001`, `KEYREG-005`, `KEYREG-011`, `KEYREG-012`, and the genesis
   case of `KEYREG-013`.
2. **K1 - Implement owner-ratified O-11 and freeze its state ownership.** Specify the permissionless GL0
   operator/Sybil-resistance rule, including bond/stake requirements, activation,
   exit, slash/cooldown, and the exact period-boundary roster root. Define its
   canonical codec, MPT ownership, undo/refold, retention, and recovery contract.
   A seedlist, live peers, positive stake alone, or a valid key registration is
   never a substitute. Gate: owner-ratified O-11 plus `KEYREG-013`, `PERM-*`, and
   `PARAM-001` RED vectors.
3. **K2 - Make the branch-historical view load-bearing.** Resolve period `N` from
   the authenticated candidate-parent `(ordinal, hash, parentHash, mptRoot)`, using the
   exact `N-2` paired-key/roster/stake view and `N-1` eta evidence. A sibling with
   the same ordinal, a view whose hash is correct but whose decoded registry/root
   belongs to another branch, receiver-current state, or missing history must
   reject/defer before draw, proof, signing, storage, acceptance, or slash. Replace
   every list-returning eta fallback with a typed complete/incomplete exact-parent
   interval; a nonempty partial prefix is never eta authority, and a proved-
   complete empty interval follows one explicitly encoded, branch-authenticated
   canonical rule rather than an unavailable-history fallback. Gates:
   `KEYREG-002..005`, `KEYREG-007`, `KEYREG-008`, and
   `CRYPTO-001`.
4. **K3 - Provision secrets, implement O-12's ratified baseline, and activate runtime rotation.** Before
   submitting a record, atomically provision the future VRF secret and fresh KES
   tree, durably bind them to the registration ID, and verify both public keys.
   Wire the existing durable record/activation model to K1/K2. Require
   `signedEffectivePeriod >= inclusionPeriod + 2` and presence in the exact N-2
   canonical view before activation. This is an index lookback, not an inferred
   `I+3` elapsed-duration rule. Paired
   KES/VRF public and local-secret selection is atomic. The exact-parent
   eligibility capability selects the record before the local bundle is opened;
   time/current-head selection is forbidden. Quantify the N-2 common-prefix
   stability and implement O-12's ratified secret-deletion and recovery/rejoin
   baseline. A reorg
   crossing an erased KES activation enters `RecoveryRequired`; it does not roll
   the KES secret back as ordinary Phase-2 state. Retaining old masters through
   k2 is an explicit forward-security tradeoff, not a hidden fallback. Gates:
   `KEYREG-003..005`, `KEYREG-007`, `KEYREG-008`, `KEYREG-010`, `KEYREG-014`, and
   `KEYREG-015`.
5. **K4 - Bind exact Phase-2 references.** Currency binaries and shard checkpoints
   bind every execution base/anchor needed for key, eta, roster, and stake lookup
   as exact `(ordinal, hash, parentHash, mptRoot)` Phase-2 evidence. Proposal, replay signing,
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

**Depends on:** E1, E2, E2K, S1, and all complete-root steps. **Must precede:**
permissionless proof claims.

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
- Preserve the active-era dark cut while those gates are open: `smtRoot=None`
  at every producer/validator/download/context/traverse/ML0/serving boundary,
  exact state-proof comparison, no production historical-store/coordinator
  wiring, provider `None`, and unavailable/503 proof routes. This contains
  `SMT-01`/`STOR-03`; it does not remove the schema field or close activation
  (`modules/shared/src/main/scala/io/constellationnetwork/validator/GlobalSnapshotActiveEraValidator.scala:10-30`;
  `modules/shared/src/main/scala/io/constellationnetwork/validator/StateProofComparison.scala:28-37,45-78`;
  `modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotStateProof.scala:124-130`;
  `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/storage/SnapshotStorage.scala:100-125`;
  `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/snapshot/finality/FinalizedSnapshotReader.scala:145-169`;
  `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala:715-717,1446-1456`;
  `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/http/routes/NipopowRoutes.scala:82-87`).
- Do not flip `TowerEligibility.NotComputed` until producer/verifier parity and
  root verification are load-bearing.
- Gates: `ROOT-001`, `ROOT-005B`, `STOR-02/03`, `TOWER-001..007`, `REC-004`,
  `SER-*`, `CRYPTO-001`, `LIGHT-001`, restart/reorg proof vectors, and one-peer
  forgery/withholding/truncation tests.

### E4 - Replayable checkpoint schema and complete root (`PLANNED`)

**Depends on:** E0, E1 exact Phase-2 refs, E2/E2K identities, S1-S3, and
complete-root steps 1-3. Diff adoption remains blocked through step 5.

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
- Keep ordinary-GL0 replay of sharded CL1 checkpoints until E9 diff adoption
  passes its complete gates. Universal GL0 execution of native GL1/DAG-token
  transitions is permanent and is not part of this replacement.
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

**Confirmed replay-authority blocker:** the historical-snapshot callback is not
the sole input today. Currency replay can prefer ordinal-only `LastN` snapshots,
reuse ordinal-keyed SpendAction/cache entries, and read the node's ambient
combined global state for message inputs. A same-ordinal sibling can therefore
change candidate validity even when the caller supplies an exact-parent lookup.
E9 must first provide one candidate-parent-scoped artifact resolver and an exact
hash/root-bound historical state reader, route every replay read through them,
and enter `RecoveryRequired` on unavailable retained data. Local `k2` may bound
work; it never makes an older reference invalid or changes fork choice.

- Ordinary noncommittee GL0 nodes verify identities/quorum, exact base, parent/
  ordinal, pre-root/version, input commitments, the positive-coverage certificate
  and absence of a pending authenticated mismatch, diff namespace/canonicality,
  then apply the diff and recompute every post-root.
- Resolve every binary's signed origin to exact canonical Phase-2 historical
  state before replay/sign/inclusion, require nondecreasing refs within each MG
  segment, and never use receiver live head, peer-local state, wall clock, or a
  self-claimed root. Compare-and-set every touched MG against proposal-parent
  mirror root/version before composition.
- Only after those checks pass, replace ordinary noncommittee GL0 recreation of
  sharded CL1 transitions with zero-recreation diff adoption. Never install a
  claimed root. Producer/signers/watchtowers still replay; native GL1 remains
  universally executed by GL0, and this epic makes the target global kernel
  universally executed.
- Every GL0 node runs the small deterministic global ordering/conflict/nullifier/
  settlement kernel over committee-extracted signed intents. Apply mirror diffs
  and GL0-owned settlement overlay atomically without letting either overwrite
  the other.
- Replace bounded `GlobalSnapshotsProcessed` history with the O-13 per-MG
  hash-linked delivery sequence, rooted pending leaves, durable ML0 cursor/state
  proof, and compare-and-set metadata-only acknowledgement.
- Replace replayable custom-data fees with O-14's domain-separated exact-parent
  intent over rooted field 27, exact fee/item bijection, available content
  commitment, and atomic balance/head update. Serialize the outer binary fee and
  every other global-balance writer in the same checkpoint-wide reservation
  kernel.
- The reservation kernel is one typed ledger over `(currency scope,address)` for
  transfers, framework/data fees, rewards, allow-spends, token locks, mandatory
  GL0 SpendActions, and slash bounties. It atomically rolls back a failed
  multi-address claim, rejects projected values outside the balance domain, and
  requires final application to equal the projected map. Exact Phase-2 GL0 inbox
  claims precede conflicting ML0-local claims.
- Preserve the closed narrow regressions: an allow-spend never credits destination
  before consume (ECO-20), a token-lock replacement release belongs to one exact
  consuming operation at the exact candidate epoch (ECO-21), allow-spend references
  have one terminal winner and typed one-shot settlement (ECO-22), lane identity
  rejects the whole malformed block (ECO-23), and field-7 reconstruction fails
  closed on key/value mismatch (SMT-04).
- Complete the shared compiler before cutover: exact post-class projected arithmetic
  rejects destination overflow before application (ECO-24). ECO-25 is fixed in
  `41c19903d`: local cross-shard no-ref reads use strict exact-byte field-25
  `MgBalances` entries, bind the embedded account, and fail closed on malformed
  committed bytes. This does not close the HTTP field-7 proof/root residual in
  the stop-the-line program. Shard execution signatures cover the final
  checkpoint-wide outer-fee/global-intent reservation result.
- `numShards=1`, `2`, and `K` use the identical transition function.
- Gates: `SHARD-E-003`/`004`, `DIFF-*`, `XMG-001` through `XMG-005B`,
  `XMG-007`/`008`/`010`/`012`/`013`, `ECON-F-002`/`003`, `ECON-REF-001`,
  `ECON-BAL-002`/`003`, `ECON-G-002`, `ROOT-006` through `ROOT-009`, `ROOT-011`,
  `PERM-005`, `SHARD-C-004`/`005`, `WT-008`/`008A`/`010`, and
  conservation/replay tests from S2. Preserve the closed `ECO-IDX-01/02`
  regressions; `ECO-IDX-03` and `MPT-01..07` remain mandatory stop-the-line
  findings, not optional hardening.

### E10 - Downstream exact-hash rebase and historical-read recovery (`PARTIAL`)

**Depends on:** E1 and E9. E9 owns the exact-origin/CAS validity checks required
for the security cutover; E10 carries those exact refs through downstream
delivery, rollback, and recovery.

- Downstream APIs and durable events carry exact Phase-2
  `(ordinal,hash,parentHash,mptRoot)`
  identities and historical proof material, never a monotone ordinal watermark.
- Cold ML0/GL1 bootstrap consumes one bundle anchored to the exact Phase-2
  identity/evidence resolved by FinalityGate. The selected-era full proof,
  complete ML0 byte image, GL1's explicitly enumerated signed/proved consumed-
  field slice, and every typed projection all bind the same `(ordinal,hash,root)`;
  persisted bytes cannot authenticate a separate peer GSI.
- Validate the complete bundle into staging before any mutation, then publish the
  MPT image, snapshot anchor, balances/references, projections, cursors, and
  recovery markers through one durable transaction. Verification failure,
  cancellation, or crash preserves the previous complete generation.
- Density replacement reverses dependent checkpoint anchors, mirrors, settlement,
  nullifiers, and deliveries, then re-follows/rebases from the exact replacement.
- Historical-view recovery verifies the same exact-origin and nondecreasing-ref
  rules used by E9; missing local data fetches authenticated content or enters
  `RecoveryRequired`, never receiver-live-head fallback.
- Bind the mutable MPT base to a root-verified `(ordinal,hash,mptRoot)` and use one
  exact-parent session for all acceptance reads/replay/writes/root/commit. Folding
  a finalized prefix retains canonical descendants. Restart restores the anchor
  before production, and global finality stages overlay, chain, tracker, outbox,
  watermarks, and projections through one recoverable transition.
- The owner-ratified session strategy is immutable parent-state capture plus a
  generation compare-and-set at commit; a stale session retries or defers instead
  of holding a lock across replay. Locally viable branch generations are retained
  only within bounded policy; older state requires authenticated reconstruction or
  `RecoveryRequired`. Cross-sink durability uses one idempotent finality-intent
  journal over the existing stores rather than an all-sinks database rewrite.
- Exact-parent/unknown-branch rejection is activated only with that complete
  anchor, descendant-retention, recovery, and finality-orchestration slice; a
  narrow guard alone reaches normal restart/finality paths after partial external
  mutation and is not mergeable.
- The dark image store now has immutable copied/sorted MPT images, independent root
  rebuild, forced atomic image/manifest publication, digest/readback, monotone
  generation CAS, lifetime OS directory ownership, bounded streaming decode, and
  typed missing-artifact recovery. Runtime activation still requires an active-era
  whole-image physical-key/value verifier bound to the actual root algorithm,
  FinalityGate-authenticated exact snapshot identity, immutable in-session capture,
  and one recoverable finality intent spanning every sink. A caller-supplied era
  label and self-consistent manifest are insufficient. This primitive alone is not
  ROOT-002/003/004 completion.
- Keep structural ancestry separate from an execution session. The dark
  `ResolvedParentLineage` may prove exact reference continuity, but the session is
  minted only by atomically copying the complete parent byte image and the whole
  overlay mutation generation. The same atomic commit compares that generation;
  no check-then-use generation test is accepted as CAS.
- The dark bounded chain-store ancestry walk resolves an exact hash before any
  ordinal fallback and rejects a same-ordinal sibling. It uses the live canonical
  hasher only when its coarse JSON/Kryo logic agrees with the ordinal-selected
  logic; a boundary returns typed `HashEraUnavailable`. Before Scodec or another
  hash-codec era activates, replace that coarse discriminator and migrate snapshot
  identity atomically across leader, sync, KES, overlay, gossip, storage, and
  recovery. Chain-store admission must also derive ordinal, parent, slot, and VRF
  metadata from the authenticated signed snapshot instead of trusting parallel
  caller arguments.
- The first dark L-23 durability slice originally landed without changing the
  then-live finality rails. The later containment described above disabled the
  unsafe optimistic sink independently of this still-dark store. ScodecV1
  ADTs/codecs, canonical identities, structural validators,
  and sealed mutation authority permit only initialization, a validated `Prepared`
  intent, or entry into absorbing `RecoveryRequired`. A checksummed coordinator,
  audit, artifact, and outbox store provides exact compare-and-set, durable
  readback, bounded restart validation, and fail-closed recovery. It remains
  unwired from `FinalityGate`, fork choice, GL0 consensus, MPT publication,
  followers, and serving; this is a partial nonactivating prerequisite, not L-23
  completion. It does not yet implement exact per-hash P0/P1 tracking or the
  optimistic K/alpha/beta decision cascade.
- P6-FIN14-A now has an owner-ratified, nonactivating exact-consumer direction in
  `docs/review/P6-FIN14-PHASE2-CONSUMER-LEASE.md`. It separates portable evidence
  from a package-minted local lease, uses two short acquisition operations around
  unlocked immutable verification, distinguishes descendant extension from
  lineage replacement, and requires a short `commitIfCurrent` plus generation-
  scoped invalidation for admission caches, attestations/tallies, queues, shard
  buffers, replay/signing, inclusion, reads, and followers. Its purpose registry
  explicitly covers admission, execution, watchtower/challenge, settlement,
  optimistic/tower contexts, correction, followers, serving, and event delivery;
  no generic economic-read scope can authorize another effect. No finality/MPT/
  chain lock may span image verification, committee polling, or replay. P6 owns
  the kernel, with P8/P10/P11 owning their consumer adapters and P7 supplying exact
  checkpoint/state interfaces. O-16A..F engineering/schema gates, O-15/O-01, exact released-
  core readback, ROOT semantic/image gates, and consumer effect ordering block any
  live issuer. Wrapping the current watermark/best-tip Boolean in an opaque type
  does not close FIN-14.
- Activation requires an authenticated evidence/fork-choice capability and an exact
  branch-revision hold through durable publication; MPT, semantic-state, and anchor
  compare-and-set readback before `CoreApplied`/`Released`; objective restoration;
  a package-owned effect executor whose receipts prove sink readback and enforce an
  explicit dependency DAG plus `RetentionMature` before pruning; bounded streaming
  validation of arbitrary-depth paths; and a long-history audit-journal checkpoint
  or accumulator. No public caller may mint those state transitions from structural
  receipts. The only modeled qualification rails are an already-decided `T_weight`
  attestation or canonical depth-`k1`, but the dark store does not authenticate
  either. Phase 2 remains density-reorgable, `k2` remains retention/recovery policy,
  and no proposal, vote, lock, certificate, or BFT decision path is introduced.
- The current direct rebuilt-root guards do not close `BR-01` through `BR-05`:
  exact Phase-2 selection, state/projection binding, proof-era validation,
  complete fresh-join payloads, and transactional installation remain open.
  `BR-06` is the existing `ECO-F32` witness defect, not a separate closure.
- Gates: `BOOT-001` through `BOOT-005`, `XMG-006`, `FOLLOW-001` through
  `FOLLOW-007`, `FOLLOW-008A` through `FOLLOW-008P`, `REC-*` including
  `REC-004`/`REC-005`, `MEMPOOL-001`,
  `ROOT-002` through `ROOT-005`, including `ROOT-005A/005B`, `ROOT-009`,
  `STOR-02`, `STOR-03`, and `GROWTH-001`.

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
- A Phase-2 shallow/deep fork-choice replacement sends exact old/new/MRCA data. Shard anchors and
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
| S1 canonical identity/serde/era | Replace the currently unwired Scodec era scaffold and live JSON/Kryo hashing/proofs with one hash-bound ScodecV1 ordinal-0 service; freeze composite vectors and MPT node/value bytes; isolate upstream-v4 Brotli/Kryo in a read-only importer. | After E0 vocabulary | E2, E2K, E3-E5, E7, E13 |
| S2 deterministic framework oracle/kernel | Authorization, checked arithmetic, conservation, semantic replay protection, ordered execution, resource bounds, independent prefix oracle. | After E0 economic grammar | E4/E5/E8-E11/E13 |
| S3 lane and DA contract | Explicit currency and currency-with-data lanes; isolated custom commitment; exact input/chunk retention; no decoder-based dispatch. | After E0 lane decision + S1 primitives | E4/E7/E8/E11 |
| S4 transport/resource/recovery harness | Bounded gossip/RPC/HTTP, durable outboxes, exact-hash multi-peer recovery, fuzz/fault harness. | RED tests can start after E0 | E1/E3/E7/E11/E14 |

Work may be delegated in parallel only with disjoint write sets and frozen shared
types. Model/RED authors do not approve their own runtime implementation. Shared
hotspots (`FinalityGate`, checkpoint schema/codecs, GSAM, MPT transaction code,
and consensus parameters) have one integration owner and merge in dependency
order.

## Historical roadmap (2026-05-16, superseded)

All attestation-finality runtime descriptions below are point-in-time evidence,
not current behavior. In particular, the logged `ATTEST-FINALIZED` path and raw
re-attestation follow-up were removed by the 2026-07-14 containment. Current GL0
finalization is depth-`k1` only until the replay-gated sampled exact-hash
`T_weight` rail is implemented and activated.

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

## Historical milestone (2026-05-14) — MPT overlay e2e validation

**Status: iter31 full e2e PASSED** (`feature/serde-typeclass-shim` @ `f51252ef`).

- 8 gl0 + 2 metagraphs, 4212s runtime, EXIT=0
- All 11 test phases green (delegated-staking → token-lock-replacement → multi-metagraph K=2 → currency → rewards → token-locks → allow-spends → spend-transactions → data-tx-no-fee → data-tx-with-fee)
- Per-gl0 finality (preserved at `/tmp/iter31-cluster-logs/`): 488–500 **ATTEST-FINALIZED**, **0 DEPTH-FINALIZED**, 0–3 fork branches per node, all attestations `weight=0.75, 8/8 active`. Cluster is healthy; depth-k fallback never engaged.

**Open follow-ups** (memory: `project_iter31_overlay_full_e2e_pass.md`):
- dl1/cl1 `pullFinalityGated` tick=10s vs gl0 finalization ~6s/ord → follower-side download lag is the actual mechanism behind iter26's `TooFarLastValidEpochProgress`. Worked around with `allow-spends.max-epoch-progress` 200→500; structural fix is faster pull / parallel batch / direct gl0 epoch read.
- Historical reorg re-attestation gap: this raw best-tip re-attestation design is
  not the target and is now removed. Future emission is driven by the ratified
  replay-gated sampled Snowball protocol, not by restoring this ticker.
- Reproducibility: iter31 is one pass; iter32 currently running for second confirmation.
- Pending memory items #118 (OverlayReader rewire of 5 gl0 HTTP read sites), #119 (n1 fork-recovery deadlock re-bootstrap), #120 (2-of-2 fragility under VRF droughts).

---

## Goal

Get **Nakamoto GL0 + new gossip** production-ready, then run a **metagraph end-to-end test** (CL0 + DL1) against it via `just` infra.

Hard fork migration is **deferred** — network can be force-forked. Stake-weighted VRF is **deferred** — equal weight for now. CL0 keeps **BFT consensus** but rides the **new sidecar gossip transport**.

---

## Workstream (in order)

### 1. Sidecar gossip — full migration  *(⚠ focused containment/wiring tests green; runtime qualification pending)*
Ported Tessellation event and generic rumor transport onto the Go libp2p sidecar
via one `Rumor` topic. ML0 may carry its own BFT messages over that transport; GL0
must not register the legacy BFT rumor families. Added Kademlia DHT for
decentralized peer discovery. Runtime validation is tracked in task #8.

**2026-07-15 correction:** at the audited baseline, GL0 constructed `gossipDaemon` but never called
`startAsInitialValidator` or `startAsRegularValidator`. The inbound bridge fed the
bounded rumor queue, but no `consumeRumors` fiber validated or dispatched its
generic/event handlers. The dormant GL0 BFT command queue was therefore not
remotely reachable; it was a latent hazard that would have become reachable if the
consumer were started without first removing the six BFT handlers. Commit
`f88d785e8` orders that containment correctly and preserves ML0 BFT.

The current worktree contains the direct pre-state mutation window with a
three-phase capability rather than a message quarantine. Main alone signals
local-state completion; `SnapshotLeaderLoop` alone publishes chain-seed
success/failure; both sidecar subscriptions await the ingress release. Main awaits
seed success, starts the generic consumer and all event/collateral daemons, and
binds all Main HTTP listeners before its current handoff. Pre-seed ChainSync
returns gRPC `UNAVAILABLE`; the gate rejects pre-seed success/release and records
failed/cancelled seed results even before local-state readiness.

The current worktree also closes the local subscription-order defect. The sidecar
requires explicit rumor/Nakamoto roles and an exact active topic profile, acquires
every requested local subscription before sending a mandatory first
`SubscribeStarted`, and binds that acknowledgement to a process session plus a
monotonic stream generation. Main now performs exact chain seed -> sinks/listeners
-> subscription activation -> both same-session acknowledgements -> `Ready` last.
The readiness owner installs an initially closed `ProductionGate`, pauses before a
current lane is invalidated, and resumes only after both current lanes are
installed. Main's one-time `Ready` write runs under the same serialized lease.
Message silence is valid; HTTP/2 keepalive and real stream termination, not a
payload-idle timer, drive reconnect. An acknowledgement proves only local drainer
registration, never mesh reachability, chain freshness, restart authority, or
economic validity.

Do not call E4.8A/B complete yet. The boot-mode/catch-up rule and authenticated
cold-restart authority are still open: a root-consistent disk head is not yet a
signature/KES/VRF/ancestry/native-replay recovery receipt. Delayed subscription can
still lose ephemeral traffic, and detached handler fibers can survive reconnect or
cancellation. Native DAG/allow-spend/token-lock topic handlers also feed unbounded
queues without pre-enqueue inner-signature validation; this is a post-bootstrap
resource/signing DoS and the next transport tranche. Target shard subscription
partitioning and a distinct GL0-wide certified-checkpoint adoption lane also remain
open; the current sidecar truthfully acknowledges its transitional all-shards
profile when sharding is active.

**What landed:**
- Sidecar `/tessellation/rumors/1.0.0` GossipSub topic + `PublishRumor` gRPC RPC
- `SidecarRumorBridge`: outbound `publishFn` (wired into `Gossip.setSidecarPublishFn`) and inbound `receive` daemon (parses `Signed[RumorRaw]` JSON, recomputes hash, offers to `rumorQueue`)
- Allocated during `GlobalSnapshotConsensus` construction, but inbound subscription
  effects await `ConsensusInputGate`; Main releases them only after local state,
  chain seed, generic consumer, and event sinks are ready
- Mandatory typed `SubscribeStarted` handshakes for both roles; exact active topic
  profiles, sidecar session/generation replay checks, serialized production fencing,
  and `Ready` publication only under the current two-lane lease
- `GossipDaemon.make` accepts `nakamotoMode: Boolean` — when its start method is
  invoked with `nakamotoMode=true`, it skips legacy peer/common round runners and
  runs only `consumeRumors`. Main starts it after chain seed and before sidecar
  ingress release.
- Kademlia DHT in server mode in the Go sidecar; rendezvous-based discovery loop (`tessellation-nakamoto`); seedlist becomes bootstrap nodes
- All four Scala modules compile; Go sidecar builds

**Intended transport flow:** Tessellation rumors follow
`Gossip.spread -> rumorQueue -> consumeRumors -> RumorHandler.run`; the bridge
plugs in at `Gossip.spread` outbound and `rumorQueue` inbound. ML0 may use this
transport for its own BFT messages. GL0 must not register or dispatch the legacy
BFT rumor families.

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
Historical env-var knobs include `NAKAMOTO_ATTESTATION_THRESHOLD`, `NAKAMOTO_CONFIRMATION_DEPTH`, `NAKAMOTO_ARCHIVAL_DEPTH`, `NAKAMOTO_OPTIMISTIC_MIN_FRACTION`, and `NAKAMOTO_MAX_ATTESTATION_SKEW_MS`. In the current containment tree, the unfinished `T_weight`, `T_count`, and `T_depth2` calculators may run for telemetry, but only canonical `T_depth1`/`k1` can advance GL0 Phase 2. The target later adds the sampled decided-attestation `T_weight` rail after its activation gates pass; it does not restore `T_count` or give `k2` finality authority.

**k measures snapshots, not slots.** With LDD targeting ~15% slot fill, slots run ~6× sparser than snapshots, but the live depth gate is purely an ordinal-distance check: `tip.ordinal - snapshotOrdinal >= k`. The original k=31 choice (2026-04-08) was based on a measured-sim table topping out at k≤80 with k=6 too high a fork rate against a 1/3 adversary; k=31 gave ~0.91% per-attempt. Subsequent expanded sims (`adv_depth_expanded_parallel.py`, 10M trials, k≤400) extrapolate the fB=0.05-tail slope to ~10⁻¹² at k≈271–290. Default raised to **k=255** as a conservative operating point approximating Cardano-equivalent CP-violation. Depth-`k1` is currently the sole live state-changing GL0 rail; the target sampled exact-hash optimistic rail is not active.

**Two slot/ordinal-units bugs were fixed in this round** (2026-04-08), discovered while validating the wire-up: `lastFinalizedOrdinal` was being read off `tipTracker.lastFinalized`'s **slot** value, and `finalizeAtSlot` was being computed as `tip.slot - k` (mixing slot- and ordinal-units). The first bug had silently disabled the depth gate end-to-end since it landed — in our 720s e2e test we observed 224 ATTEST-FINALIZED entries and **zero** DEPTH-FINALIZED entries. The surviving depth gate reads its inputs from the chain store, which is the authoritative ordinal source; the unsafe attestation-weight sink described by that historical run is removed.

**Finality-trigger stack refactor (2026-05-15).** The two inline gates (depth-k₁ + attestation-2/3) were extracted into a `FinalityTrigger[F]` typeclass (`modules/node-shared/.../nakamoto/FinalityTrigger.scala`) so each trigger is a monotone-Ref-backed observable that `SnapshotLeaderLoop.finalityMonitor` evaluates and advances on each tick. With the refactor:

- **`T_count`** added (commit `7003be21`) — 1-validator-1-vote canonical-hash-filtered count finality, self-excluded (#133), denominator = `StakeRegistry.validatorCount` (full seedlist). Reuses `TipTracker.FinalityThreshold` so it ties with `T_weight` under equal stake and is strictly stronger evidence once stake-weighted VRF lands.
- **`T_depth2`** added historically (commit `06455f98`) as a claimed Phase-2-to-Phase-3 archival gate. That interpretation is rejected: `k2` is retention/recovery capacity only, and pruning must never become fork-choice truth. The associated `MptOverlay.pruneBelow` provenance remains relevant to the replacement recovery design.
- **Historical `T_weight` mitigation** (commit `95471c7f`) added self-exclusion to the former `TipTracker.highestFinalizedOrdinal` cumulative query. That query and its state-changing sink are now removed; this history does not describe the target sampled exact-hash `T_weight`.
- **Re-bootstrap reset machinery landed** (commit `01ebcca6`, task #141) — `RebootstrapOrchestrator` observes sustained `chainStore.divergentRefuseCount`, then resets TipTracker/Overlay/finality state. The reset now pauses before waiting for and retains the shared snapshot mutation barrier through the complete reset; cancellation or failure fail-stops instead of reopening concurrent canonical mutation. The typed-HOCON setting remains live-default `true`. This is still not objective recovery: a peer-driven refuse count is only a heuristic, revisions/journals are not durable, served projections are not atomically quarantined, and completion depends on ordinary verified ancestry replay. Replace it with authenticated objective recovery before activation.
- **`attestedAt` skew bound** (commit `422e1a6b`) — receive-side defense-in-depth for `T_count`. Drops attestations outside ±`NAKAMOTO_MAX_ATTESTATION_SKEW_MS` of `Clock[F].realTime`; counter `dag_nakamoto_attestations_rejected_skew_total`. Tightenable post-Chronos.
- **Chain-quality observable** (commit `866cd598`, task #138) — `FinalityTrigger.triggersFor(ord)` lookup answers "which triggers qualified ord N?" at the depth-`k1` finalize site (gauge `dag_nakamoto_chain_quality` ∈ {1, 2, 3}; per-kind counters) and via HTTP route `GET /global-snapshots/{ord}/finality-triggers`. Pure observability — never feeds back into consensus.
- **`SlotCertificate.parentSlot` wiring** (commit `bec9de6b`) — `NakamotoProposer` was passing `parentSlot = Slot.MinValue` (TODO placeholder); now threaded through correctly so verifier-side `slotGap = cert.slot - cert.parentSlot` reconstruction matches the producer's LDD lottery threshold.

See the supersession notice in `docs/nakamoto/attestation-and-finality.md` and
`docs/review/CONSENSUS-ARTIFACT-LIFECYCLE.md` for the target three-phase
P0/P1/P2 contract. The historical four-phase trigger record below is not target
semantics.

### 6. Close SC binary finality loop (CL0-side)  *(✅ done)*
**Bug:** original `pruneConfirmed` dropped a binary on first sight in any GL0 snapshot — if that snapshot was later orphaned in a Nakamoto reorg, the binary was permanently lost.

**Fix landed:**
- **Data model:** `BinaryTracker.pruneFinalizedBelow(SnapshotOrdinal)` only prunes ConfirmedBinary entries whose `proof.globalOrdinal <= lastFinalizedGlobalOrdinal`. `StateChannelBinarySender.confirm` gained an optional `lastFinalizedGlobalOrdinal: Option[SnapshotOrdinal]` parameter defaulting to the snapshot's own ordinal (BFT-preserving).
- **GL0 endpoint:** new `GET /global-snapshots/latest/finalized-ordinal` route on `SnapshotRoutes`. In Nakamoto mode, dag-l0 wires it to a `Ref[F, Long]` that `SnapshotLeaderLoop` currently updates only after a successful exact selected-tip `finalizeSelectedAt` receipt from canonical `k1` depth. The target exact-hash `FinalityGate` must replace this ordinal-only surface before the optimistic rail activates. In BFT mode the route defaults to head ordinal because that separate consensus engine commits its snapshots immediately.
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
