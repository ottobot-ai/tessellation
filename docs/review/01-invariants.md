# Tessellation-Nakamoto — Invariant Catalog (CLAIM-SET for adversarial review)

> **Purpose.** A pre-seeded catalog of the consensus invariants the Track-1 / Track-3 sharding
> workstream is supposed to uphold, each anchored to `file:line` in the *current* working tree
> (branch `feature/committee-state-diff`, HEAD `21933559c`, verified 2026-07-07) and rated by
> confidence. This is the CLAIM-SET Fable will attack. Where the code does not clearly enforce a
> stated property it is rated **Unclear** or **Likely broken** — *not* "Holds" to be agreeable.
> Read `docs/review/HANDOFF.md` for the framing; this file does not duplicate it.
>
> **Ground-truth notes established while writing this (correct these before trusting summaries):**
> - **k₁ is NOT 255.** The `taktikos` skill and several scaladoc comments
>   (`SnapshotLeaderLoop.scala:992-994`) say "default 255". That is **stale**. The live value is
>   threaded from HOCON per-env `nakamoto.confirmation-depth-k`
>   (`application.conf:308-316`): **mainnet 1024, testnet/integrationnet 256, dev 32**; the
>   code-level neutral fallback is **32** (`config/types.scala:192`). `ConfirmationDepthK` at
>   `SnapshotLeaderLoop.scala:1003` is the run-parameter, not the literal 255. All k₁ claims below
>   use the HOCON value.
> - **k₂ = 100·k₁** is derived, not defaulted (`config/types.scala:165-166`) — mainnet ≈ 102 400.
> - The scaladoc at `SnapshotLeaderLoop.scala:993` also says LDD `ψ=0`; the live config is
>   `offset ψ=1` (`application.conf:332`, `ldd.scala:32`). Another stale comment.

---

## SECTION A — SAFETY (what must NEVER happen)

### INV-SAFETY-001: No node rewrites its own finalized/settled history (write-freeze floor)
- **Statement:** an honest node must never store a *different-hash* snapshot at an ordinal at or
  below its finality floor (k₁ `nakamotoFinalizedOrdinalRef` with the band flag OFF; k₂
  `nakamotoSettledOrdinalRef` with it ON). Fork choice must likewise never switch away below that
  floor.
- **Upheld by:** store-gate `NakamotoChainStore.scala:415-443` (`floorRef.get`; `ordinal <= floorLong`
  + different existing hash ⇒ `REFUSED store` → `.as(false)`, else `tryStore`);
  `ChainSelection.shouldSwitch` `ChainSelection.scala:142-170` (flag-OFF: refuse if `current` is the
  k₁ finalized head, `:145-155`; flag-ON: refuse if `forkAncestorOrdinal < settled`, `:156-170`).
- **Fault model:** a fork-storm / equivocating producer trying to overwrite an already-finalized
  ordinal; a partitioned minority chain arriving via sync.
- **Confidence:** **Likely holds** (per-node monotonicity), with a real caveat.
- **Reasoning:** the store-gate makes a single node's finalized prefix immutable, and the P-11
  divergent-refuse trip-wire (`NakamotoChainStore.scala:422-438`) fires exactly when the canonical
  chain tries to overwrite a locally-frozen divergent fork. But that trip-wire *is the evidence
  that two nodes CAN finalize different hashes at the same ordinal* — the node then needs
  `RebootstrapOrchestrator` to escape. So this is per-node safety, not a global "no two conflicting
  snapshots finalize" guarantee; the latter held only after the 2026-06 fork-storm root cause
  (non-deterministic `smtRoot`, fixed by `376d09fbc`) was removed, and the Track-1/3 spec lists
  "re-confirm `376d09fbc` holds on HEAD" as an *open precondition* (`TRACK1-TRACK3-...SPEC.md:40,59`).
  Attack surface: any residual determinism gap in the consensus root re-opens same-ordinal
  divergence below the floor.

### INV-SAFETY-002: No two globally-conflicting snapshots finalize (cross-node common prefix)
- **Statement:** two honest nodes must never finalize different hashes at the same ordinal.
- **Upheld by:** emergent from INV-SAFETY-001 (per-node floor) + a *deterministic consensus root*
  (INV-SAFETY-005) + fork choice (`ChainSelection.compare`) being a total, commutative function
  (`ChainSelection.scala:299-311` `deterministicTip` totality).
- **Fault model:** ≤1/3 Byzantine stake producing equivocating tips; network partition; a
  non-deterministic root that makes honest nodes disagree on identical data.
- **Confidence:** **Likely holds** post-`376d09fbc`, **conditional on that fix holding on HEAD.**
- **Reasoning:** this is a *probabilistic* Nakamoto property, not an absolute one — it holds with
  overwhelming probability past k₁ and is guaranteed only past the k₂ absolute floor. The historical
  failure mode (June 2026 storm) was NOT chain-selection but a non-deterministic `smtRoot` in the
  consensus `===` making honest nodes fork on identical data — i.e. INV-SAFETY-005 was violated and
  this fell with it. Flagged as an open re-confirmation item in the handoff. Attack the root
  determinism, not the fork-choice arithmetic.

### INV-SAFETY-003: Adopt-byte-diff ≡ re-exec-over-pinned-base, byte-identical (Track-1 T1)
- **Statement:** the currency `Info` that gl0-and-everyone ADOPT via `reconstructInfoFromDiff` must
  be byte-identical to what a committee/watchtower gets by re-executing `accept()` over the same
  pinned base; an attested per-MG root that a Byzantine producer fabricated must be rejected.
- **Upheld by:** byteDiff adopter `GlobalSnapshotAcceptanceManager.deriveAdoptedCurrencyState`
  (`:1006`); apply-and-verify gate `recomputed === attestedRoot`
  (`GlobalSnapshotAcceptanceManager.scala:1123-1125`, component-addressable per-MG root via
  `GlobalStateConverter.currencySnapshotMgRoot`); re-exec twin
  `ShardCheckpointWiring.reExecDerivationWithDiff` (`:210`); reconstruction
  `ChangeSet.reconstructInfoFromDiff` (`:151`); execution-base pin resolved at the cluster-uniform
  `executionBaseOrdinal` via `pinnedPriorInfoOf` (`GlobalSnapshotAcceptanceManager.scala:1094-1104`).
- **Fault model:** a Byzantine ml0/committee shipping a byte-diff whose reconstructed root does not
  match an honest re-execution; per-node base drift (`overlay.base` lagging) making honest nodes
  reconstruct different `next`.
- **Confidence:** **Likely holds** — unit-verified only, NOT cluster-verified.
- **Reasoning:** the execution-base-pin (`eb9bf9a5e`) is the load-bearing move: every honest node reads
  the per-MG prior `S(N)` at the *same* pinned `executionBaseOrdinal` rather than its own lagging base,
  so applying the diff reconstructs the byte-identical `infoOf(next)` (comment
  `GlobalSnapshotAcceptanceManager.scala:1107-1116`). The root-equality gate then rejects any
  fabricated root. Residual risk (handoff §5.1): any ml0-controlled input where the byte-diff and an
  honest re-exec disagree AND `recomputed === attestedRoot` still passes would break this — verified
  in `ShardCommitteeReExecutionSuite`/`CurrencySnapshotAcceptancePuritySuite` unit only. Also depends
  on the re-exec twin `reExecDerivationWithDiff` pinning to the *same* ordinal atomically; the twin
  survives (§3 of the handoff corrects the "we deleted it" phrasing) — attack the atomicity of the
  `executionBaseOrdinal` capture vs the diff-prior read.

### INV-SAFETY-004: gl0 never adopts a currency advance over the wrong prior (fail-closed pin)
- **Statement:** if the pinned prior at `executionBaseOrdinal` is unreadable (evicted below retention, or
  the pinned snapshot is on a fork), the MG's currency advance must be DROPPED, never adopted over a
  substitute prior.
- **Upheld by:** `pinnedPriorInfoOf` returns `None` ⇒ `GlobalSnapshotAcceptanceManager.scala:1096-1103`
  logs `FAIL-CLOSED DROP` and yields `none`; the reader itself is hard-None with no HEAD fallback
  (`PinnedCurrencyInfoReader`, handoff §2 commit `e2f266ff1`).
- **Fault model:** an adopter whose retention window has evicted the pinned ordinal; a committee that
  pins a `executionBaseOrdinal` on a minority fork.
- **Confidence:** **Holds** (as written) — but note the self-heal is a *liveness* cost.
- **Reasoning:** the code path clearly drops rather than substitutes. The failure mode this converts
  is safety→liveness: the MG stalls until the anchor is served and the leader re-offers. Distinct
  reader wiring exists on all 3 paths (handoff §2 `2a`), but the "follower contiguous per-ordinal
  store for deep anchors" is DEFERRED (handoff §4) — so on the follower/deep-catch-up path a deep
  `executionBaseOrdinal` is *unsupported*, which fails closed (safe) but can wedge liveness. Attack: can a
  committee choose a `executionBaseOrdinal` deep enough that honest followers permanently fail-closed?

### INV-SAFETY-005: Consensus root is a pure function of consensus-pinned state (field-32 lesson)
- **Statement:** every byte that can change deterministic consensus execution must be committed by
  the signed root or supplied through an equally exact signed/root-bound replay witness. An
  observation-dependent value cannot be both root-invisible and consumed by replay. Identical
  committed inputs must produce byte-identical roots and decisions.
- **Enforced portion:** `GlobalStateKey.consensusRootEntries` retains every `SystemNamespace`
  active-address and expiry index because they drive economic materialization and expiry/refund
  transitions; `GlobalSnapshotInfo.consensusMptRoot` hashes that complete economic entry set. The
  only stored partition currently filtered is `MgGlobalSnapshotSyncView` (field 32) —
  `GlobalStateKey.scala:505-541`; `GlobalSnapshotInfo.scala:319-330,386-397`.
- **Open violation (`ECO-F32`):** GL0 checkpoint replay reads the prior
  `CurrencySnapshotInfo.globalSnapshotSyncView`, while pinned peer backfill strips field 32 and
  reconstruction can turn its absence into `Some(empty)`. The signed currency incremental carries
  the resulting proof hash and accepted delta, not the exact full-view preimage or explicit ML0
  operator population needed to reproduce it — `PinnedCurrencyInfoReader.scala:146-156,341-380`;
  `ShardCheckpointWiring.scala:263-314`; `GlobalStateConverter.scala:1691-1803`;
  `currency.scala:52-61,96-114,222-240`.
- **Fault model:** node L replays from a locally staged nonempty field-32 view while node B
  root-verifies a backfill under the same signed GL0 root, strips field 32, and reconstructs an empty
  view. L can reproduce and sign the next currency proof while B derives a different result and
  refuses or disputes it, preventing execution quorum or freezing that metagraph. A root filter has
  contained the old direct global-root fork but has not made the replay input reproducible.
- **Confidence:** **VIOLATED — HIGH, CONFIRMED, OPEN.** Carry an exact optional full-view witness and
  explicit ML0 operator population in the signed/root-bound framework replay artifact; preserve
  `None` versus `Some(empty)` and verify the view against
  `CurrencySnapshotStateProof.globalSnapshotSync`. Missing material defers and cannot slash. Only
  then remove field 32 from every GL0 MPT/diff/load/reorg path while retaining it in ML0 state.

### INV-SAFETY-006: The committee signature binds the whole claim (ShardCheckpointSigPreimageV2)
- **Statement:** a relayer/committee must not be able to alter the diff base, the state delta, the
  cross-shard receipts, the slot, or the chain-link without invalidating the committee signatures.
- **Upheld by:** `ShardCheckpoint.signingPreimage` → `ShardCheckpointSigPreimageV2`
  (`ShardCheckpoint.scala:91-102,135-145`) binds `shardId, parentCheckpointHash, shardOrdinal,
  gl0AnchorOrdinal, slot, derivedStateDelta, emittedReceipts, epoch, executionBaseOrdinal`; only
  `committeeSignatures` is excluded from the signed bytes (`:107-113` frozen-shape discipline);
  `executionBaseOrdinal` is proto field 11 (handoff §2 `#15`).
- **Fault model:** a relayer mutating the byte-diff, retargeting the diff base, or forging receipts;
  a wire-format drift silently changing the signed bytes.
- **Confidence:** **Holds.**
- **Reasoning:** the preimage is a pure projection of the envelope minus the signature field, and the
  greenfield hard-cutover to V2 means the whole cluster hashes the same field set (no dual-codec).
  The `executionBaseOrdinal` — the base every re-executor pins to — is now inside the signed bytes, so a
  committee cannot claim one base and diff over another. Residual: this proves *authenticity of the
  claim*, not *correctness* — correctness rests on INV-SAFETY-003 (root gate) + the watchtower layer
  (INV-FAULT-001). Attack the field *set*: is any consensus-load-bearing input NOT in the preimage
  (e.g. `committeeSignatures` count/threshold, `includedSnapshots` DA payload)?

### INV-SAFETY-007: No un-enforced cross-shard token op finalizes (I-ONCE + re-exec)
- **Statement:** a cross-metagraph allow-spend must be consumable exactly once (no double-consume /
  replay), and the token-model transition must be *enforced* (re-executed), not merely adopted.
- **Upheld by:** `ConsumedAllowSpends` spent-set (fieldId 33), `include(M-root) ∧ absent(spent-set)`
  + same-snapshot spent-marker write — `GlobalSnapshotAcceptanceManager.scala:523-529`,
  `AllowSpendConsumeHandler.scala:32`, `ConsumedAllowSpendStateManager.scala:55-153`; gated
  `numShards > 1` (`GlobalSnapshotAcceptanceManager.scala:525`).
- **Fault model:** the same consumer double-consuming when the allow-spend's active-status lives in a
  shard that never witnesses the consumption; a stale-inclusion-proof replay.
- **Confidence:** **Unclear** — the spent-set is built, but the enforcement/spendability story is
  partially UNBUILT.
- **Reasoning:** I-ONCE (the spent-set) is wired and the settlement is atomic at gl0's fold, which is
  sound *for double-spend*. BUT the economic-trust doc's own corrections (2026-06-30) say: (a) the
  authoritative-override *removal* making re-exec the *primary* token-model gate is "the remaining
  build" (`ECONOMIC-TRUST-...md:24,117,180`); (b) the **defer-not-revert spendability staging gate is
  still TO BUILD** — "today the fold applies effects immediately" (`ECONOMIC-TRUST-...md:127`); (c)
  I-AUTH env-threshold metagraph-operator signature is TO BUILD (`:26,138`). So "no *un-enforced*
  cross-shard token op finalizes" is only true once re-exec-primary + staging land; today a
  cross-shard effect can finalize and be spendable before the challenge window, with only a *slash*
  (no state-revert) as recourse. Also everything here is INERT at the production default `numShards=1`
  (`application.conf:485`), so it is untested in the shipping config. Attack: a quorum-signed invalid
  checkpoint adopted before any watchtower disputes, with effects already spent.

### INV-SAFETY-008: The settled (k₂) marker is monotone and never aliases the k₁ floor
- **Statement:** the k₂ "settled" marker must advance monotonically, be a distinct `Ref` from the k₁
  production floor, and only be written by the `T_depth2` archival sink.
- **Upheld by:** `SettledOrdinalTracker.markSettled` internally monotone
  (`SettledOrdinalTracker.scala:57-58`); distinct-ref invariant documented + only reset by the store
  in lock-step (`:44-54`, `NakamotoChainStore.scala:210-220`); sole writer is the T_depth2 sink
  (`SnapshotLeaderLoop.scala:1510-1514,1560`).
- **Fault model:** a mis-wired route or aliasing that lets a k₂-valued write land on the k₁
  production floor (would stall block production) or vice-versa.
- **Confidence:** **Holds.**
- **Reasoning:** `markSettled` ignores any non-strictly-greater value even under a mis-wired caller,
  and the class doc + store comments make the distinct-ref requirement a construction invariant. The
  route surface is read-only (`settledOrdinal`). Low attack surface; the only sharp edge is that
  under the band flag ON the store *shares* this ref for the fork-choice floor, so the reset path
  (`unsafe_clearFinality`) must stay in lock-step with k₁ — verify the two refs reset together.

---

## SECTION B — LIVENESS (what must EVENTUALLY happen, under what synchrony)

### INV-LIVE-001: Attestation-2/3 fast path OR depth-k₁ finalizes (max-of, first wins)
- **Statement:** under partial synchrony a snapshot must eventually finalize via EITHER ≥2/3
  attestation weight (fast, healthy net) OR depth-k₁ confirmation (fallback, degraded net) — whichever
  fires first.
- **Upheld by:** two independent finalize sinks in `SnapshotLeaderLoop.finalityMonitor`: depth sink
  driven by `tDepth1.latestQualifyingOrdinal` (`:1177-1194`, calls `chainStore.finalize`); attestation
  sink driven by `tipTracker.highestFinalizedOrdinal(FinalityThreshold=2/3, …)` (`:1303-1322`, gated
  `!depthFinalized`); both advance `nakamotoFinalizedOrdinalRef` monotonically (`:1217-1220,1333-1336`).
  Threshold `TipTracker.FinalityThreshold = 2/3` (`TipTracker.scala:131-136`).
- **Fault model:** attestations stall (partition, <2/3 active) → depth-k₁ must still finalize; healthy
  net → attestation must finalize in seconds without waiting k₁.
- **Confidence:** **Holds**, with two nuances worth flagging.
- **Reasoning:** the max-of is realized as *two separate sinks* that each advance the same monotone
  finalized ref, not a single combined trigger. Nuance 1: **T_count is diagnostic-only** — despite
  the doc/`FinalityTrigger` describing it as a co-equal Phase-1→2 trigger, its branch only logs and
  "does not drive state" (`SnapshotLeaderLoop.scala:1396-1432`); finalization is weight-OR-depth. Not
  a defect (weight+depth cover liveness) but the "T_count finalizes" claim is false as-encoded.
  Nuance 2: the attestation sink uses the *legacy weight-sum* `highestFinalizedOrdinal`, not the
  Snowball `tWeight` trigger that the docs call primary (`:1291-1311`) — Snowball advances the Ref for
  observability but the state-advancing finalize is the legacy 2/3 weight-sum. Attack: a case where
  neither weight-sum nor depth advances (e.g. attestations all filtered to zero weight by the
  canonical-hash filter while the node is on a fork, `:1376-1385`).

### INV-LIVE-002: The (k₂, k₁] band is density-revertable (no permanent write-freeze deadlock)
- **Statement:** a liveness-degraded network must be able to fall back to length/density in the
  `(settled, finalized]` band instead of dead-locking on a k₁-absolute write-freeze (the P-11
  deadlock the reverted `86f390130` caused).
- **Upheld by:** the machinery EXISTS — `ChainSelection` band path (`:156-170` k₂ floor, `:266-297`
  commutative `densityCompare`), store-gate keyed on `nakamotoSettledOrdinalRef` when the flag is ON
  (`NakamotoChainStore.scala:409-415`), disk-backed k₂ revert (`MptOverlay.revertToOrdinal`, handoff
  §2 `S4`). BUT it is **gated OFF by default**: `band-density-reorg-enabled = false`
  (`application.conf:391`, `ChainSelection.scala:116`, `NakamotoChainStore.scala:259`).
- **Fault model:** a liveness-degraded cluster whose k₁ finalized head froze on a minority branch and
  cannot reorg to the denser majority branch.
- **Confidence:** **Likely broken as-shipped** (the invariant is NOT in force by default).
- **Reasoning:** with the flag OFF — the production default — `shouldSwitch` uses the legacy k₁
  hash-exact clamp (`ChainSelection.scala:145-155`) and the store freezes at k₁
  (`NakamotoChainStore.scala:415`), i.e. the very k₁-absolute behavior this invariant says must not
  hold. The band-revert code is present but explicitly held OFF pending a deep-fork cluster-uniformity
  sim (handoff §4, "band-density reorg flag flip — GATED — needs sim"). So the *stated* invariant is a
  design target, not the running behavior. Two things to attack even when the flag is ON: (a) is
  `densityCompare` actually commutative — construct an MRCA-walk or tiebreak asymmetry (handoff §5.3);
  (b) is the disk-backed deep revert byte-deterministic across nodes with different RAM-journal bounds
  (handoff §5.6)? Until both are proven the flag stays off and this invariant does not hold in prod.

### INV-LIVE-003: Dormant ≠ offline (eligibility gates proposal only; attestation unconditional)
- **Statement:** a node that is LDD-dormant (gap < ψ) for its own proposal must still fully
  participate — validate, gossip, and ATTEST — so degraded proposal eligibility never removes a node
  from finality quorum.
- **Upheld by:** proposal is gated (`SnapshotLeaderLoop.checkEligibility` → `None` ⇒ skip `onSlotWon`,
  `:840-900`; threshold zero when `gap < offset`, `EligibilityChecker.scala:34`); attestation is
  UNCONDITIONAL — `emitTipAttestation` records + signs + publishes with no eligibility check
  (`NakamotoSyncDaemon.scala:3235-3320`), and the finality-monitor re-attest ticker fires on every
  bestTip change regardless of gap (`SnapshotLeaderLoop.scala:1117-1139`).
- **Fault model:** ψ=1 makes the slot immediately after every snapshot dormant; a naive design would
  drop those nodes from the attestation quorum and stall finality.
- **Confidence:** **Holds.**
- **Reasoning:** there is no eligibility predicate on any attestation-emit path — the asymmetry is
  explicit and matches the taktikos skill's "dormant gates proposal only". The 2/3 quorum is therefore
  computed over all active attesters, not just currently-eligible proposers. Low risk. Adjacent
  concern (not this invariant): the attestation *weight denominator* is optimistic/active stake
  (`TipTracker.scala:212-222`), which is a fault-model issue tracked under INV-FAULT-002.

### INV-LIVE-004: Block production continues regardless of finality status
- **Statement:** if finality (both attestation and depth) stalls, builders must keep extending the
  longest chain so the network does not halt.
- **Upheld by:** `TipTracker` scaladoc contract "Production continues regardless of finality status"
  (`TipTracker.scala:22`); the leader loop's slot tick and `onSlotWon` are independent of the finality
  monitor stream (`SnapshotLeaderLoop.scala:1567` merges `slotTick` with `finalityMonitor`);
  production floor is k₁ `nakamotoFinalizedOrdinalRef`, not the frozen head.
- **Fault model:** attestation quorum lost during a partition; the chain must still grow so depth-k₁
  can eventually finalize.
- **Confidence:** **Holds.**
- **Reasoning:** finalization and production are separate stream branches; nothing blocks `onSlotWon`
  on a finality event. This is what makes depth-k₁ a viable fallback (the chain has to keep growing to
  reach depth k₁). The one place production *can* stall is the write-freeze floor
  (INV-SAFETY-001) — by design, and recoverable via `RebootstrapOrchestrator`.

---

## SECTION C — FAULT-MODEL assumptions: AS ENCODED vs AS STATED

### INV-FAULT-001: Per-shard safety = 1-of-N honest watchtower (NOT ⅔-honest committee)
- **Statement (as stated):** committee security does not rest on a ⅔-honest committee; a *single*
  honest watchtower re-executing `accept()` from the checkpoint-carried snapshot can detect and slash
  a fully-corrupt committee within the challenge window (K = k₁).
- **As encoded:** the watchtower re-exec + fraud-proof + durable slash ARE built —
  `watchtowerReExec`, `FraudProofEnvelope`, `InvalidStateProofEvidence` → `Slashings` fieldId 34
  (`ECONOMIC-TRUST-...md:24,117`, `config/types.scala:240-273`); checkpoint carries the DA payload as
  `ShardDerivedStateDelta.includedSnapshots`. BUT: re-exec is not yet the *primary* adoption gate (the
  authoritative override for data-app state still exists), the **defer-not-revert spendability staging
  gate is NOT built** (effects apply immediately, `ECONOMIC-TRUST-...md:127`), and this is all gated
  `numShards > 1` — production ships `numShards = 1` (`application.conf:485`).
- **Confidence:** **Unclear** (design sound; enforcement path partially unbuilt + untested at
  `numShards>1`).
- **Reasoning:** the trust *reduction* (⅔-committee → 1-of-N verifier) is architecturally sound and
  the detection+slash half is wired, but "caught" ≠ "prevented" until spendability is deferred behind
  the challenge window. Today a corrupt committee's effect can finalize and be spent, with a slash as
  the only recourse and NO state-revert (the "revert the checkpoint" leg was specced but never
  implemented, `ECONOMIC-TRUST-...md:50`). So the *safety* of an economic op still leans on the
  optimistic window in a way the code does not yet close. Attack: value-extracted-before-slash > 
  committee-stake-at-risk × slash_fraction (the doc's own residual sizing knob, `:128`).

### INV-FAULT-002: ≤1/3 global Byzantine stake for attestation finality
- **Statement (as stated):** attestation finality requires ≥2/3 stake; ≤1/3 Byzantine cannot forge it.
- **As encoded:** `TipTracker.FinalityThreshold = 2/3` (`TipTracker.scala:131`), but the weight sum
  uses `stakeRegistry.optimisticRelativeStake(peerId)` — **active peers only**
  (`TipTracker.scala:212-222`, `:258`). So the threshold is 2/3 of the *observed-active* stake, not
  2/3 of total registered stake.
- **Confidence:** **Unclear** — the denominator is not the total-stake denominator the "≤1/3 global"
  framing implies.
- **Reasoning:** under a partition the active set shrinks, so 2/3-of-active can be a minority of total
  stake — a minority partition could fast-finalize its own branch via the attestation sink. The
  backstop is that (a) depth-k₁ is the real safety net and (b) the store-gate floor
  (INV-SAFETY-001) refuses to rewrite it later. Whether "minority partition fast-finalizes then the
  majority overwrites" is safe depends entirely on the floor + reorg rules. Attack: a 34%-stake
  attacker who can suppress enough honest attesters (mark them inactive) to become 2/3 of the
  active set, fast-finalize, then the honest majority cannot reorg below the floor. Note also the
  attestation skew gate (`±60s`, `TipTracker.scala:149-153`, `MaxAttestationSkewMs`) and that
  `FinalityThreshold` is a `sys.env` read (`TipTracker.scala:132`) — a per-operator override here is a
  cluster-split risk (violates the project's HOCON-over-sys.env rule).

### INV-FAULT-003: Cross-shard honest-majority collapses when α_total > 1/(2S)
- **Statement (as stated):** the prior CQ-collapse result (`α_total > 1/(2S)` breaks honest-majority
  across S shards) is *dissolved* by the watchtower layer, so committee size is a
  liveness/throughput knob, not the safety parameter (`k-draw=8 / k-quorum=6`).
- **As encoded:** there is **no committee-size safety bound in code** — `numShards` only requires
  `> 0` (`ShardAssignment.scala:55`), assignment is `hash(addr) mod numShards`
  (`ECONOMIC-TRUST-...md:204`), and `k-draw/k-quorum` are config, not derived from `α,S`. The safety
  rests on the watchtower stack (INV-FAULT-001), which is partially unbuilt.
- **Confidence:** **Unclear.**
- **Reasoning:** the doc *withdrew* the `K≈400` sizing on the premise that the watchtower dissolves the
  bound — but that premise is only as strong as INV-FAULT-001, whose enforcement path (re-exec-primary
  + spendability-defer) is not fully built. If the watchtower layer is incomplete, the α>1/(2S) bound
  is NOT actually dissolved and `k-quorum=6` is under-sized. Handoff §5.7 explicitly lists this as an
  open question. Attack: at `numShards>1` with a small committee and the staging gate unbuilt, does a
  cross-shard collapse become exploitable before a watchtower disputes?

### INV-FAULT-004: Partial synchrony (bounded Δ) for depth-k₁ probabilistic finality
- **Statement (as stated):** depth-k₁ gives ≈10⁻¹¹–10⁻¹² common-prefix-violation vs a 1/3 adversary
  under bounded network delay.
- **As encoded:** k₁ is the depth gate (`SnapshotLeaderLoop.scala:1171-1194`), sized per-env
  (`application.conf:308-316`, mainnet 1024). The security *number* is an extrapolation from Taktikos
  MC sims (taktikos skill; k≤80 measured, 255→~3·10⁻¹¹ extrapolated, k≈290 for strict 10⁻¹²), NOT a
  measurement at the shipped depth.
- **Confidence:** **Likely holds** (for the finality *number*), assuming the synchrony bound.
- **Reasoning:** the depth gate is correctly wired and mainnet 1024 is far past the k≈290 needed for
  10⁻¹². The invariant is only as good as the delay assumption — see INV-FAULT-005. The extrapolation
  is honest in the skill; the risk is the LDD `fB=1/20` tail making the true slope shallower than
  extrapolated, but 1024 has large headroom over 290.

### INV-FAULT-005: Taktikos delay-cliff at Δ≈8s — NOT encoded (operational assumption only)
- **Statement (as stated):** past ~7-8s p99 propagation the per-attempt reorg risk enters an
  exponential cliff (18% at Δ=8s, >60% at Δ=10s) that k depth does NOT fix — k buys risk-floor
  headroom, not delay-cliff resistance.
- **As encoded:** **nothing.** There is no adaptive-k, no finalization-pause-on-high-latency, and no
  p99-propagation gate anywhere in the leader loop or sync daemon. The LDD `fB=1/20` baseline
  (`application.conf:334`) guarantees the adversary always makes forward progress in long-δ regimes;
  the cliff is structural to that tail.
- **Confidence:** **Likely broken** as a *code* invariant — it is a pure operational assumption.
- **Reasoning:** the system assumes p99 propagation stays well under ~7s and has no mechanism to
  detect or respond when it doesn't; during a network event even k₁=1024 gets pushed into the cliff
  regime (taktikos skill "Delay cliff"). The options named in the skill (pause finalization,
  dynamically escalate k, accept-unhealthy) are none of them implemented. Attack: an adversary who can
  inflate honest propagation past 8s (eclipse, gossip flooding) turns depth-k₁ from 10⁻¹¹ into
  double-digit-percent reorg risk without violating the ≤1/3 stake bound.

### INV-FAULT-006: Fork-choice tiebreakers are not grindable (pseudo-predictability)
- **Statement (as stated):** VRF outputs are deterministic from `(sk, eta, slot)` and pre-computable,
  so any fork-choice tiebreaker deterministic from local state is grindable; the Taktikos-pure rule is
  `length → head-slot → stall-and-wait`, NOT `→ lowest VRF`.
- **As encoded:** `standardCompare` (short-fork) uses `ordinal → slot → deterministicTip`, where
  `deterministicTip` is `lower-VRF → lower-hash` (`ChainSelection.scala:254-311`). The VRF/hash
  tiebreak only fires when ordinal AND slot both tie — rare but present. `densityCompare` (long fork)
  ends in the same `deterministicTip`.
- **Confidence:** **Unclear** — the exposure is narrow (ordinal+slot tie) but real; "rare" ≠ "safe".
- **Reasoning:** the taktikos skill flags this as a known minor exposure (skill §4; `TAKTIKOS-NOTES.md:82`).
  A tiebreaker deterministic from local state (VRF output, then hash) is in principle grindable — an
  equivocating same-slot producer can pick the tine whose VRF/hash wins. The mitigating facts: it only
  triggers on an exact ordinal+slot collision, and the final hash tiebreak makes `compare` total and
  commutative (needed for cluster-uniform convergence, `ChainSelection.scala:299-303`). The
  Taktikos-pure alternative (stall for future chain growth) is not implemented. Attack: can a producer
  who controls two same-slot equivocations at a tie ordinal steer honest nodes onto the branch it
  prefers by grinding the VRF/hash tiebreak? Low severity, but it is a deterministic-from-local-state
  tiebreak, which is exactly the class the protocol says to avoid.

---

## Cross-cutting: invariants enforced ONLY on the happy path (restart / catch-up / error)

Flagged per the review's "does it hold on restart/catch-up, not just happy path?" mandate:

- **Deep catch-up bypasses full validation.** `verifyCatchUpSnapshot` runs only two parent-free gates
  (envelope signature + `stateProof`-vs-GSI consistency) — the full `NakamotoSnapshotValidator`
  (VRF + slot-cert + majority-hash) does NOT run on the reset-to-network-tip path
  (`NakamotoSyncDaemon.scala:3323-3335` scaladoc). So INV-SAFETY-001/002's proposer-eligibility and
  majority-hash checks are RELAXED during catch-up. This is the historical home of the D1/D2/D4/D6
  catch-up defects (memory: `project_gl0_deepcatchup_gsi_divergence`).
- **Driver-B self-heal ADOPTS a producer's signed state for one ordinal** on a `selfHealable`
  ContentMismatch (`NakamotoSyncDaemon.scala:~3340+`), re-aligning the base rather than re-deriving —
  a deliberate trust-the-signed-producer step that widens the adopt surface under fork churn.
- **Set-valued P ⊇ U window** (Track-1 1a) is reconstructed from the committed chain; the handoff
  explicitly flags "does P ⊇ U hold across restart, deep catch-up, and peer-sync-ahead, not just the
  happy path?" as OPEN (handoff §5.5). The invariant `P-window ⊇ U-window` is described as a *latent
  bug today* if violated (`TRACK1-TRACK3-...SPEC.md:31`).
- **Follower deep-anchor reads are DEFERRED** — INV-SAFETY-004's pinned reader fails closed for deep
  `executionBaseOrdinal` on the follower path (handoff §4), converting a deep-anchor safety case into a
  liveness stall that is untested.
- **Band-revert + deep k₂ revert determinism is unproven** — INV-LIVE-002's disk-backed
  `revertToOrdinal` re-fold must be byte-identical across nodes with different RAM-journal bounds;
  re-verifying this is listed as "the FIRST S4 TDD gate" and is NOT yet done at cluster scale
  (`TRACK1-TRACK3-...SPEC.md:46,59`).

---

## Summary — invariants rated below "Holds" (the attack targets)

1. **INV-LIVE-002 (band revertable) — Likely broken as-shipped.** The k₁-absolute write-freeze is the
   live default; band-revert code exists but is gated OFF (`band-density-reorg-enabled=false`) pending
   an unpassed cluster-uniformity sim. The stated invariant is a target, not running behavior.
2. **INV-FAULT-005 (Taktikos delay-cliff) — Likely broken.** Zero code survives the Δ≈8s cliff — no
   adaptive-k, no finalization pause. Pure operational assumption; k depth does not help.
3. **INV-SAFETY-007 (no un-enforced cross-shard op) — Unclear.** Spent-set is built, but re-exec-primary
   and the defer-not-revert spendability staging gate are UNBUILT (effects apply immediately); slash is
   the only recourse, no state-revert. All inert at the shipped `numShards=1`.
4. **INV-FAULT-001 (1-of-N watchtower safety) — Unclear.** Detection+slash wired; prevention
   (defer-spendability + re-exec-primary) not — "caught" ≠ "prevented" before the challenge window.
5. **INV-FAULT-003 (α>1/(2S) dissolved) — Unclear.** No committee-size safety bound in code; the
   "dissolved" claim inherits INV-FAULT-001's incompleteness. `k-quorum=6` may be under-sized.
6. **INV-FAULT-002 (≤1/3 global Byzantine) — Unclear.** Attestation threshold is 2/3 of *optimistic
   active* stake, not total; a minority partition (or an attester-suppressing attacker) could
   fast-finalize its branch.
7. **INV-FAULT-006 (no grindable tiebreaker) — Unclear.** `standardCompare` ends in a VRF→hash
   tiebreak deterministic from local state (the class Taktikos says to avoid); narrow trigger
   (ordinal+slot tie) but not "safe" by construction.
8. **INV-SAFETY-002/003 (common prefix / adopt≡re-exec) — Likely holds, conditionally.** Both hinge on
   root determinism (`376d09fbc`) and execution-base-pin, verified in UNIT ONLY; re-confirm on a cluster.
   Also **k₁ ≠ 255** (stale doc) — live mainnet 1024.
