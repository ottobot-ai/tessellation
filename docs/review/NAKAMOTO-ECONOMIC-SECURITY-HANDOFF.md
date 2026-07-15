# Nakamoto Execution-Sharding & Economic-Security — Architecture Handoff (evidence map)

> **HISTORICAL PRE-FIX AUDIT EVIDENCE, NOT A DESIGN SPECIFICATION.** This packet
> captures claims and defects at the cited 2026-07-09 tree. Subsequent changes may
> have removed the named wire fields, state-diff adoption paths, or enforcement
> gaps, so every finding must be rechecked against current source before use. Its
> descriptions never override [`../../AGENTS.md`](../../AGENTS.md), ADR-0016, or
> ADR-0017, and do not create backward-compatibility requirements in this
> greenfield fork.
>
> **Current enforcement correction (2026-07-15):** the historical blind-sign
> defect described below is closed in the live tree. `evaluateForSigning` runs
> mandatory pinned-base replay and can mint `VerifiedShardCheckpoint` only after
> the claimed per-metagraph roots match; `ShardCheckpointAttestationEmitter`
> accepts only that capability. Embedded adoption also replays today. The live
> gaps are the target canonical root-covered checkpoint byte diff, complete
> exact-hash base/CAS binding, and separately domain-separated positive assigned
> watchtower coverage before GL0 inclusion. Until those land, ordinary GL0 nodes
> still recreate sharded CL1 transitions as a transitional backstop. Any removal
> of universal replay means only that ordinary noncommittee sharded-CL1
> recreation. Every GL0 node must continue executing and validating native
> GL1/DAG-token transitions and the global conflict/settlement kernel. Do not
> restore `authoritative*`, roots-only economic adoption, or
> `AdoptFromSignedFields`. See current ADR-0017 and the consensus lifecycle.
>
> **Current root-contract correction (2026-07-13):** every
> `SystemNamespace` active-address and expiry index participates in
> `consensusMptRoot`; only field 32 is filtered. Field-32 exclusion is temporary
> containment, not economic closure: framework replay consumes the prior view,
> so an exact signed/root-bound optional replay witness and explicit ML0 operator
> population must land before GL0 removes the mirror (`ECO-F32`, HIGH, OPEN).
> The original `ECO-IDX-01/02` same-root System-index defects are closed narrowly:
> root selection is enforced at `GlobalStateKey.scala:508-544` and
> `GlobalSnapshotInfo.scala:318-329,392-403`; typed malformed/absent/inconsistent
> failures are defined at `StrictMptRead.scala:18-37,66-127`; exact indexed targets
> fail closed, expiry buckets must match the target-derived epoch, and the
> currency union rejects simultaneous legacy field-3 and incremental field-5
> arms at `GlobalStateReaderOps.scala:164-245`,
> `AllowSpendStateManager.scala:358-432`, `TokenLockStateManager.scala:383-454`,
> and `NodeCollateralStateManager.scala:124-198`. Owner indices for
> `LastCurrencySnapshotsProofs` and `MetagraphSyncData` are maintained at
> `GlobalStateConverter.scala:724-856,1265-1275,2920-2962`.
> This is not full E9-01/ROOT-008/ROOT-009 closure. The complete
> `from(mpt, ordinal, era)` projection is still absent, prefix parsers remain open,
> and bootstrap still has unbound-GSI and nontransactional-install findings
> (`BR-02`, `BR-05`). The node-local `WithdrawalTimeLimit` also changes rooted
> collateral-withdrawal expiry bytes (`GlobalStateConverter.scala:822-847`;
> `dag-l0/Main.scala:108-114`; `MptFieldCoverageSuite.scala:325-377`), so
> `ECO-IDX-03` is HIGH, CONFIRMED, and OPEN.
> A separate exact-parent residual is also open: MultiBranch reads treat an
> absent requested branch or ancestor as a reason to compose against the mutable
> finalized base (`MptOverlay.scala:772-847,1255-1283`). The mechanism is
> confirmed; a reachable unknown/evicted-parent economic mismatch remains
> PLAUSIBLE. A root-verified persisted base anchor must precede typed
> `ParentStateUnavailable` failure across checkout and every point/prefix/root/raw
> read. Neither GSI nor finalized base may heal missing proposal-parent state.
> Optional global proof slots are now selected from authenticated byte-partition
> presence alone (`GlobalSnapshotInfo.scala:302-388`); this byte-canonical proof
> shape must not be confused with ROOT-010's future semantic field-32 witness,
> where `None` and `Some(empty)` remain distinct replay inputs.

**Purpose.** Give an external reviewer (Codex) a self-contained, line-anchored map of the execution-sharding + economic-security architecture so they can double-check our mental model *against the code* before we spend more cycles. Every non-trivial claim below carries a `file:line`. The canonical design decisions are **`docs/adr/0016`** (execution-sharding re-exec + cross-shard) and **`docs/adr/0017`** (committee re-execution is the primary economic-validity gate — the fix for the §4 defect); this doc is the *evidence* behind them.

**Repo state.** Branch `feature/committee-state-diff`, HEAD `5557ee084` (2026-07-09). All paths are under `modules/`.

**Verification legend.**
- **✅ VERIFIED** — I (the author) read the exact cited lines this session.
- **◆ AGENT** — gathered by a sub-agent that re-read the lines; where I spot-checked, it says **✅ spot-checked**.
- **[INFERRED]** — an inference from surrounding code, not a direct statement in it. Treat as a hypothesis to confirm.

**Current summary (rechecked 2026-07-15).** Metagraph processing is sharded, but every GL0 node still executes native GL1/DAG-token movement and the global conflict/settlement kernel. On the sharded CL1 path, the producer derives the checkpoint root and every execution signer must independently reproduce the processor's accepted output before `VerifiedShardCheckpoint` can reach the signing API. The current embedded-adoption path also invokes that replay on each GL0 node. That universal **sharded-CL1** recreation is additional transitional enforcement, not a production-safety proof or the target cost model: the current checkpoint has no canonical namespace-confined byte diff, complete exact Phase-2 base, mandatory all-input-consumed commitment, or pre-inclusion positive assigned-watchtower coverage certificate. Explicit signed framework/custom-data lane isolation is also a target, not live enforcement; current payload semantics remain decoder-selected. Cross-shard reads remain finality-first. See §4 for the current enforcement boundary and remaining gaps.

---

## 1. Layer map — GL0 / GL1 / CL1 / DL1

Two distinct apps at the **global** layer (◆ AGENT):
- `node-shared/.../app/Layer.scala:4-5` — `case object DagL0` / `case object DagL1`.
- **dag-l0 / GL0** = global snapshot producer + verifier. Entry: `dag-l0/.../Main.scala:55-61` (`layer = DagL0`). Produces `GlobalIncrementalSnapshot` at `dag-l0/.../GlobalSnapshotConsensusFunctions.scala:881`; verifies at `:207` (`validateArtifact`). Determinism contract "leader and every follower independently call `createProposalArtifact` from the same inputs" — `:61-62`.
- **dag-l1 / GL1** = global DAG-token edge app. Entry: `dag-l1/.../Main.scala:46-52` (`layer = DagL1Layer`). Packages DAG txs into blocks (`dag-l1/.../consensus/block/RoundData.scala:59-74`, `formBlock`), submits to gl0 (`dag-l1/.../Main.scala:196-199`, `sidecarClient.publishDAGBlock`).

Metagraph layer (**L2**, execution-sharded): **CL1** = currency framework economic ops (allow-spend / spend / token-lock / transfer / fee / balance / supply); **DL1** = custom per-metagraph data-app logic; **ml0** aggregates them into a metagraph snapshot.

---

## 2. The re-execution model (the three cases)

### 2a. Global DAG token (GL1) — re-executed by EVERY gl0 node ◆ AGENT
Inbound dag-l1 block → gl0 `l1Output` queue (`dag-l0/.../modules/Services.scala:264-266`) → wrapped as `DAGEvent` (`GlobalSnapshotEventsPublisherDaemon.scala:59-61`; `GlobalSnapshotEvent.scala:29`) → cut into `blocksForAcceptance` (`GlobalSnapshotConsensusFunctions.scala:397,509-511`) → **re-executed** in `GlobalSnapshotAcceptanceManager.acceptBlocks` (`GlobalSnapshotAcceptanceManager.scala:626`) → `BlockAcceptanceManager.acceptBlock` (`BlockAcceptanceManager.scala:48-49`) → **balance debit/credit** in `BlockAcceptanceLogic.scala:144-146` (minus amount, minus fee, plus amount), folded into global balances at `GlobalSnapshotAcceptanceManager.scala:2102` and written to the snapshot at `:2987`. So gl0 re-executes DAG transactions rather than trusting dag-l1's acceptance.

### 2b. Metagraph CL1 — producer and every execution signer replay before signing (current) ✅ RECHECKED

The producer and verifier share `ShardCheckpointWiring.reExecDerivationAtPinnedBase`, which resolves an ordinal-selected base, invokes `processCurrencySnapshots`, and derives the per-metagraph root (`ShardCheckpointWiring.scala:233-245,278-315,317-355`). Production verification wires that closure at `SharedServices.scala:309-354`. This does **not** yet prove consumption of the complete committed input window: recreation may return an accepted prefix after a later input fails, while the wiring's count/ordinal checks are advisory and root derivation continues (`GlobalSnapshotStateChannelEventsProcessor.scala:433-436,493-498`; `ShardCheckpointWiring.scala:316-355`; `ShardCheckpointProducer.scala:662-693`). Mandatory all-input-consumed or deterministic per-input-disposition binding is open.

The former best-tip blind-sign path is archived historical evidence, not current behavior. Intake calls `evaluateForSigning`; it runs `evaluateIntake -> verifyForIntake -> reExecPath`, and only `Accepted` or `PendingMoreAttestations` can mint the private `VerifiedImpl` capability (`ShardCheckpointGl0AcceptanceManager.scala:302-322,339-371`). `reExecPath` compares every locally reproduced root with the checkpoint claim and rejects unavailable or mismatching replay (`:630-706`). `ShardCheckpointAttestationEmitter.emit` accepts only `VerifiedShardCheckpoint` (`ShardCheckpointAttestationEmitter.scala:47-53,128-154`). In the receive path, only the successful capability branch calls naked-checkpoint `ingestValidated`, then requires the recovered hash to equal the capability hash before continuing (`NakamotoSyncDaemon.scala:2276-2298`). The storage API is not itself capability-typed and remains an API-hardening gap. The signer reuses that capability for the received checkpoint or calls `evaluateForSigning` again for each stored ancestor; the emitter receives only the resulting capability (`:2360-2395`).

This proves that signing is gated on reproducing the processor output at the current API boundary. It does **not** yet prove complete-window replay, an exact hash/root-bound base, or that the transition function has complete authorization, conservation, replay protection, or root-input coverage. Those gaps can stall honest signing or falsely classify an honest historical committee as divergent. The target canonical diff and positive assigned-watchtower coverage discussed in §4 are also absent.

`ShardCommitteeReExecutionSuite.scala:169-198` asserts **producer-root == independent verifier replay root** for one real currency binary. `ShardCheckpointGl0AcceptanceManagerSuite.scala:1122-1164` asserts that a reproduced root differing from the checkpoint claim deterministically yields `RejectedReExecutionMismatch`. Neither test closes the accepted-prefix or exact-base gaps above.

### 2c. Metagraph DL1 — target commitment/DA lane; explicit isolation is NOT live

GL0 cannot execute arbitrary data-app code. The locked target gives an explicit
signed opaque lane authenticated custody, availability, and ordering only; GL0
does not certify custom semantic correctness. The target
`FrameworkCurrencyWithData` lane replays its framework-economic portion as CL1
while isolated custom bytes have zero economic authority. **That lane typing is
not implemented today.** The live `StateChannelSnapshotBinary` is untagged
(`StateChannelSnapshotBinary.scala:17-20`), and GL0 selects currency semantics by
whether payload bytes successfully JSON-decode
(`GlobalSnapshotStateChannelEventsProcessor.scala:371-393,400-409`). Decoder
success must not remain a lane selector. Any ML0-origin framework-state authority
is forbidden; a future custom semantic-proof lane requires a separately
registered deterministic active-era verifier.

---

## 3. Shard checkpoint structure ◆ AGENT

The current `ShardCheckpoint` contains `shardId`, `parentCheckpointHash`,
`shardOrdinal`, `gl0AnchorOrdinal`, `slot`, `derivedStateDelta`,
`committeeSignatures`, `epoch`, and `executionBaseOrdinal`
(`ShardCheckpoint.scala:68-77`). The signing preimage includes every field except
`committeeSignatures` (`:85-105`), so the root claims and ordered inputs are
consensus-load-bearing.

The current `ShardDerivedStateDelta` contains only
`perMetagraphMptRoots` and the committed ordered `includedSnapshots`
(`ShardDerivedStateDelta.scala:17-38`). It does **not** contain the target
canonical byte diff, and the current replay path does not prove that every
committed input was consumed. `ShardCheckpoint` also carries only ordinal base fields,
not a complete exact Phase-2 `(ordinal, hash, root)` reference
(`ShardCheckpoint.scala:14-25,49-77`). Any older description in this packet of
`perMetagraphStateDiff`, `ShardCurrencyStateDiff`, token-lock deltas, artifacts,
or sync deltas is archived fork-only schema history and creates no compatibility
requirement.

---

## 4. ⭐ The GL0 adoption gate — blind-sign closed; diff/coverage target still open ✅ RECHECKED

The historical quorum-only happy path is no longer live:

- **Signing requires local replay.** `evaluateForSigning` can mint only the private replay-backed `VerifiedImpl`, and the emitter has no naked checkpoint/hash signing overload (`ShardCheckpointGl0AcceptanceManager.scala:218-221,302-322`; `ShardCheckpointAttestationEmitter.scala:47-53`).
- **Quorum is mandatory but not sufficient for embedded adoption.** `verifyForAdoption` first verifies the execution certificate and then unconditionally calls `reExecPath`; the locally reproduced roots decide acceptance (`ShardCheckpointGl0AcceptanceManager.scala:353-371,630-706`).
- **Current ordinary GL0 adoption still recreates sharded CL1.** GSAM calls `deriveAdoptedCurrencyState -> processCurrencySnapshots` for the included checkpoint inputs (`GlobalSnapshotAcceptanceManager.scala:1034-1085,2083-2117`). This is additional transitional enforcement, not proof of production safety and not the target execution-sharding cost model. The base is ordinal-only, complete input consumption is not enforced, and a density reorg can make honest historical execution diverge from the verifier's later same-ordinal base.
- **The target canonical diff is absent.** The current delta has only roots and ordered inputs, and the checkpoint has no exact-hash Phase-2 base (`ShardDerivedStateDelta.scala:17-38`; `ShardCheckpoint.scala:14-25,49-77`). Noncommittee GL0 nodes therefore cannot yet verify/apply a namespace-confined canonical diff and recompute its root without recreating the CL1 transition.
- **Positive assigned-watchtower coverage is absent.** `watchtowerReExec` exists, but the receive path starts it after a checkpoint becomes the local shard best tip (`NakamotoSyncDaemon.scala:2302-2314`). There is no separately domain-separated positive-coverage capability/certificate and no pre-inclusion coverage threshold. This remains the execution-threshold collusion backstop gap; later slashing cannot make an already usable invalid derivative safe.

**Current verdict:** signing and current adoption invoke replay/root comparison, but neither path yet proves the complete committed window or binds the exact historical `(ordinal, hash, root)` base. A replay mismatch is therefore not yet sound slash evidence. The remaining target work is exact base/input/root and all-input-consumed coverage, canonical diff construction and verification, pre-inclusion positive assigned-watchtower coverage, and zero-recreation ordinary noncommittee adoption. Removing universal replay in that final step means only ordinary noncommittee replay of **sharded CL1** framework transitions. Universal native GL1/DAG execution and the global kernel remain mandatory on every GL0 node.

---

## 5. Watchtower / InvalidStateProof slash — ACTIVE & consensus-load-bearing (numShards>1) ◆ AGENT (✅ spot-checked)

**Verdict: not shelf-ware** — wired end-to-end into the live `NakamotoSyncDaemon` and the GSAM fold; **gated on `numShards > 1`** (inert at the `num-shards = 1` default, `application.conf:500`).

- Evidence type defined: `shared/.../slashing/InvalidStateProofEvidence.scala:59`; envelope `shared/.../sharding/FraudProofEnvelope.scala`; slash record `SlashReason.InvalidStateProof` (`InvalidStateProofSlashManager.scala:204,231`).
- Deterministic on-chain verdict re-derives the root and upholds only on affirmative divergence: `InvalidStateProofValidator.scala:173-200`.
- Emitter and validator are constructed behind the shard/watchtower gates at `GlobalSnapshotConsensus.scala:1883-1926` and injected into the daemon with the shared fraud-proof pool at `:2341-2353`.
- Live gossip subscribes the `fraud-proof` topic (`SidecarClient.scala:123-126,157-170`); dispatch is `NakamotoSyncDaemon.scala:1048-1055`; inbound dispute decode, deterministic validation, and pool offer are `:2545-2609`, especially `:2583-2589`. The leader embeds the same pool contents into the `fraudProofs` consensus field (`GlobalSnapshotConsensusFunctions.scala:754,816-817,899-903`), and GSAM re-validates upheld evidence into slash requests (`GlobalSnapshotAcceptanceManager.scala:1983-2036`).
- **Consensus-load-bearing sink (✅ spot-checked):** `applyWatchtowerSlashes` (`GlobalSnapshotAcceptanceManager.scala:385-445`) calls `InvalidStateProofSlashManager.applySlash` (`InvalidStateProofSlashManager.scala:96`) and the accepted slash records are inserted with **`mpt.insert[SlashedRegistryEntry](…)` at `GlobalSnapshotAcceptanceManager.scala:2960`** (Slashings partition, fieldId 34), so they feed the MPT root.
- Two slash paths, one sink: (A) GL0's embedded-adoption replay can return `RejectedReExecutionMismatch` -> `WatchtowerSlashRequest(submitter=None)`, 100% burn (`GlobalSnapshotAcceptanceManager.scala:996-1022`); (B) a gossiped watchtower fraud proof is embedded and re-adjudicated before `submitter=Some` yields the configured bounty. Config: `slash-fraction = "1/1"`, `bounty-fraction = "1/20"`, `cooldown-epochs = 100`, and `watchtower-enabled = true`. Explicitly unresolvable replay data follows the non-slashing sentinel path, but that is not a soundness proof: ordinal-only base resolution and field-32-stripped backfill can yield a real divergent root and falsely enter the slash branch (`PinnedCurrencyInfoReader.scala:277-298,361-380`; `ShardCheckpointGl0AcceptanceManager.scala:647-693`). Exact base binding and complete authenticated replay bytes are required before mismatch may slash.
- [INFERRED / agent] Stale scaladoc at `NakamotoSyncDaemon.scala:2535-2538` still calls evidence submission "remaining wiring"; the live pool offer at `:2583-2589` plus the pool→embed→apply chain above supersedes it.

---

## 6. Cross-shard reads = Option A (finality-first) ✅ VERIFIED (this session)

The gl0 accept path reads cross-shard values off gl0's own consensus-pinned **finalized** mirror, not a peer/committee attestation:
- `GlobalSnapshotAcceptanceManager.scala:2293-2305`: production defaults to `crossShardSpendProofClient.getOrElse(ShardSubtreeProofClient.gl0Local[F](finalizedBaseReader))`; the local client reads the consensus-finalized base, not the tentative candidate branch.
- `ShardSubtreeProofClient.gl0Local` (`ShardSubtreeProofClient.scala:104-201`) reads off gl0's finalized MPT; scaladoc warns a peer-fetch here "would FORK the cluster."
- The committee-to-committee P2P path (`ShardSubtreeProofClient.http`, `:216-373`) still anchors every proof at gl0's **last-finalized** checkpoint (`verifyProof`, `ShardSubtreeProofService.scala:267-304`), fail-closed with no anchor.
- The consuming CL1 op is still re-executed by the validator (`SpendActionValidator.scala:369-385,447-538` cross-shard read paths). Not-yet-finalized ⇒ absent ⇒ op rejected + retried. See ADR-0016.

---

## 7. Replay-base fidelity — current precursor work, not canonical diff adoption ◆ AGENT

These mechanisms help current universal sharded-CL1 replay reproduce the same
per-metagraph root. They do not constitute the absent canonical diff protocol:

- **Replay-base pin** — producer and verifier resolve the signed ordinal base through a version-retained pinned reader, never a receiver live head (`ShardCheckpointWiring.scala:233-245,278-315`; production verifier wiring `SharedServices.scala:309-354`). An unresolvable base defers/fails closed.
- **Replay-first staging** — proposal/validation stages captured signed post-state bytes under the replayed artifact hash (`GlobalSnapshotConsensusFunctions.scala:954-968`); the receiver rekeys stripped-hash staging to the exact signed snapshot hash only after full content validation (`NakamotoSnapshotValidator.scala:260-277`); the finalization sink promotes only the exact finalized hash and otherwise skips (`SnapshotLeaderLoop.scala:638-654`). This is retained replay data, not authoritative diff adoption.
- **Read-time backfill** — on a pinned-read miss, fetch peer bytes, **strip to `consensusRootEntries`**, verify `consensusMptRoot === committed stateProof.mptRoot`, persist+serve only on match else fail-closed: `PinnedCurrencyInfoReader.scala:360,369,370-371,374-377,381,386`. Wired gl0-only: `GlobalSnapshotConsensus.scala:577-595,706`. This proves the imported global entry set, but it also strips field 32; without the `ECO-F32` replay witness, a nonempty locally staged prior and a stripped backfill can still recreate different currency proofs.

---

## 8. Metagraph parent-lag bridge — old permanent-wedge claim is no longer current

The earlier `resolveParent`/`parentOrdinalFor` path and its cited line numbers no
longer exist. Do not continue to report that historical orphan loop as the
current 2mg/2shard e2e blocker without a new reproduction.

The live framework-currency path now separates two inputs:

- ML0 continuity ordinal is decoded deterministically from the signed binary,
  while eta, keys, KES period, and global reads come from its signed exact GL0
  `globalSyncView`; the resolver requires that signed `(ordinal, hash)` and
  provides no content-decoder fallback to a receiver head
  (`MetagraphParentOrdinalResolver.scala:18-59,61-115`). The live verifier is
  still transitional: it combines an ordinal watermark with a best-tip ancestry
  walk and explicitly does **not** mint the target exact Phase-2 lease/CAS
  (`GlobalSnapshotConsensus.scala:1367-1382`). A concurrent replacement remains
  an open authority race until that exact gate lands.
- To bridge GL0's delayed ML0-tip projection, the daemon first checks the
  deterministic admitted-parent cache, otherwise applies the metagraph-parent
  identity guard, and then reruns that transitional signed-anchor/watermark/
  ancestry predicate on every use (`NakamotoSyncDaemon.scala:2687-2715`). It
  records the binary's deterministic value-hash-to-ordinal continuity entry at
  resolution (`:2770-2789`), runs `attestAndAdmit`, and processes/drains children
  only on admission (`:2811-2820`).

This removes the source basis for the old claim of an unavoidable permanent
re-buffer loop. It is not an end-to-end proof: regression coverage must still
exercise several monotonically increasing binaries per metagraph, GL0 projection
lag, out-of-order delivery, local gate timeout, and a same-ordinal Phase-2 hash
replacement. Cache residence is continuity state only and must never preserve
authorization across the target exact-Phase-2 lease recheck.

---

## 9. Key config (`node-shared/.../resources/application.conf`)
`k-draw = 8`, `k-quorum = 6` (`:484-486`); `num-shards = 1` **default — sharding/committee/watchtower path inert here** (`:500`); `watchtower-enabled = true` (`:622`); `slash-fraction = "1/1"`, `bounty-fraction = "1/20"`, `cooldown-epochs = 100` (`:628,634,639`).

---

## 10. ⭐ Open questions for external review (ranked)

1. **Q1 — Committee blind-signing (§4) — CLOSED at the live API boundary, replay soundness still OPEN.** `VerifiedShardCheckpoint` is minted only after `reExecPath`, and the emitter accepts only that capability. Preserve compile-negative coverage for every sign/attest entry point. Activation still requires exact base binding, mandatory complete-input consumption/disposition, authenticated field-32 replay material, the canonical diff verifier, and pre-inclusion positive assigned-watchtower coverage. Until then a replay-backed signature or mismatch is not automatically sound.
2. **Q2 — Complete CL1 economic grammar remains open.** The current shared replay calls `processCurrencySnapshots` against a pinned prior and root-compares the output. Audit and repair the shared transition function's authorization, conservation, replay protection, complete root-input coverage, and deterministic failure semantics; replaying a defective function does not make it economically correct.
3. **Q3 — numShards=1 default (§5,§9).** The entire committee/watchtower/cross-shard economic-security apparatus is inert at the production default `num-shards = 1`. Is single-shard the intended launch posture, and if so what provides metagraph economic security there?
4. **Q4 — Replay-input completeness.** Pinned-base replay is live, but `ECO-F32` remains open: the optional full framework replay view and explicit ML0 operator population are not completely bound by the current root/input schema. Missing material must defer and cannot produce a signature or slash.
5. **Q5 — Parent-lag regression proof (§8).** The historical permanent-wedge claim is superseded by the live signed-anchor/hash-ancestry resolver/cache containment, but exact Phase-2 lease authority is still open. Prove the replacement under multi-binary lag, timeout, out-of-order, and Phase-2-replacement schedules; do not reintroduce receiver-local economic authority.

---

## 11. Audit targets — where else might a signature/adoption skip the verification it implies?

The historical §4 blind-sign defect is closed at the current shard signing API, but it remains a useful regression pattern. Sweep for the same class of bug: **a node signing or attesting to state it did not itself reproduce, or an adopter treating a certificate as sufficient without performing the validation its role requires.**

1. **Global-snapshot Nakamoto attestation.** Current containment removes local GL0 optimistic signing rather than treating replay as sufficient provenance. Future emission must consume both a sealed `AuthenticatedExecutedGlobalSnapshot` and an exact current `PreferredExecutedTip`; best-tip, storage, or a public validation result cannot reach signing.
2. **Canonical diff and positive coverage.** Confirm that future ordinary noncommittee adoption cannot activate until the checkpoint binds the exact base, complete inputs, canonical namespace-confined diff, extracted global intents, execution threshold, and separately domain-separated positive assigned-watchtower coverage. Signature count or post-adoption fraud detection is insufficient.
3. **ml0 diff-adopt follow** (`project_ml0_diff_adopt_design`): ml0 adopts GSI + verifies `mptRoot === signed` instead of re-running createContext. Is root-equality sufficient, or does it trust fields not covered by that root?
4. **gl1 stake-root follow** (`project_gl1_historicalstake_carryforward_blocker`): inclusion-proof follow of gl0's stake root — verify the proof is checked against a *finalized* root, not accepted on assertion.
5. **Cross-shard `http` proof consumer (§6):** verifies against gl0's finalized checkpoint — confirm no path trusts the peer's *self-claimed* checkpoint/root.
6. **Historical paths labelled "authoritative."** Treat ML0-origin balance or framework-state authority as forbidden. Framework economics must pass CL1 replay/certificate/diff/root/global-kernel enforcement. Only protocol/global GL0 correction authority may flow downward; dropped sub-state cannot carry a stale or empty prior.
7. **Attestation crypto completeness.** Current `preCheck` verifies committee membership, Ed25519, KES, and registered-key VRF possession for every carried signature (`ShardCheckpointGl0AcceptanceManager.scala:419-436`). Remaining work is exact historical active-key/roster resolution and exact Phase-2 anchor/freshness binding; a receiver-current registry or ordinal-only base cannot authorize a signer.
8. **DL1 commitment/DA carriage (§2c).** Confirm opaque custom bytes cannot affect framework state and that GL0 claims only authenticated custody, availability, and ordering. No custom semantic-proof claim exists unless a separately registered deterministic verifier is active for that era.

The grep pattern: an `emit` / `sign` / `attest` / `adopt` / `Accepted` gated on *receipt / best-tip / signature-count* rather than on *this node re-computing the thing it's vouching for.*

## Appendix — primary files
- `ShardCheckpointGl0AcceptanceManager.scala` — gl0 adopt gate + watchtower re-exec (§4,§5).
- `ShardCheckpointProducer.scala` / `ShardCheckpointWiring.scala` — producer/verifier replay and root derivation (§2b,§7); canonical diff remains absent.
- `GlobalSnapshotStateChannelEventsProcessor.scala` — current framework-currency replay (`processCurrencySnapshots`, §2b).
- `GlobalSnapshotAcceptanceManager.scala` — global accept, cross-shard wiring, slash fold (§2a,§5,§6).
- `ShardSubtreeProofClient.scala` / `ShardSubtreeProofService.scala` — cross-shard reads (§6).
- `InvalidStateProofValidator.scala` / `InvalidStateProofSlashManager.scala` / `WatchtowerFraudProofEmitter.scala` — slash path (§5).
- `MetagraphParentOrdinalResolver.scala` / `MetagraphOrphanBuffer.scala` / `MetagraphCommitteeGate.scala` — committee-gate wedge (§8).
- `PinnedCurrencyInfoReader.scala` — store-fidelity backfill (§7).

*Evidence gathered by 6 parallel sub-agents (line-exact, re-read) + author verification of the load-bearing claims (§4 enforcement gap, §8 resolver, §2b/§5 spot-checks, §6 cross-shard). [INFERRED] tags mark the non-direct claims to confirm.*
