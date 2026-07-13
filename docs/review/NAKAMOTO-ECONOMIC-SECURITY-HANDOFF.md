# Nakamoto Execution-Sharding & Economic-Security — Architecture Handoff (evidence map)

> **HISTORICAL PRE-FIX AUDIT EVIDENCE, NOT A DESIGN SPECIFICATION.** This packet
> captures claims and defects at the cited 2026-07-09 tree. Subsequent changes may
> have removed the named wire fields, state-diff adoption paths, or enforcement
> gaps, so every finding must be rechecked against current source before use. Its
> descriptions never override [`../../AGENTS.md`](../../AGENTS.md), ADR-0016, or
> ADR-0017, and do not create backward-compatibility requirements in this
> greenfield fork.
>
> **Current target correction:** the accepted repair is committee
> replay-before-sign, a canonical root-covered checkpoint byte diff,
> noncommittee GL0 apply/root verification, and noncommittee watchtower replay.
> Commit `c610a0740` removed the old blind-sign/quorum-adopt path but also removed
> the useful diff and regressed to universal GL0 currency recreation. Do not
> restore `authoritative*`/`AdoptFromSignedFields`, and do not treat universal
> recreation as the target. See the current ADR-0017 and consensus lifecycle.

**Purpose.** Give an external reviewer (Codex) a self-contained, line-anchored map of the execution-sharding + economic-security architecture so they can double-check our mental model *against the code* before we spend more cycles. Every non-trivial claim below carries a `file:line`. The canonical design decisions are **`docs/adr/0016`** (execution-sharding re-exec + cross-shard) and **`docs/adr/0017`** (committee re-execution is the primary economic-validity gate — the fix for the §4 defect); this doc is the *evidence* behind them.

**Repo state.** Branch `feature/committee-state-diff`, HEAD `5557ee084` (2026-07-09). All paths are under `modules/`.

**Verification legend.**
- **✅ VERIFIED** — I (the author) read the exact cited lines this session.
- **◆ AGENT** — gathered by a sub-agent that re-read the lines; where I spot-checked, it says **✅ spot-checked**.
- **[INFERRED]** — an inference from surrounding code, not a direct statement in it. Treat as a hypothesis to confirm.

**The one-paragraph summary.** Metagraph *processing* is sharded; the global layer is universal. Two things re-execute economic state: (1) **every gl0 node** re-executes global DAG-token movement (dag-l1 blocks), and (2) on the shard-committee path, **only the producer re-executes on the happy path** — other committee members currently **sign on best-tip without re-executing** (verified defect), and gl0 adopts on **quorum signature alone**, so the guarantee reduces to producer-honesty + post-hoc watchtower slashing. The sub-quorum and watchtower re-executions ARE real (base-pinned), but neither gates the happy path. **ADR-0017 (Accepted) is the fix:** committee re-execution becomes the primary gate — a member signs only its own re-executed root and fraud-proofs on mismatch. Custom data-L1 (DL1) logic is proof-carried. Cross-shard reads are finality-first (Option A), verified. See §4, §10, and the audit targets in §11.

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

### 2b. Metagraph CL1 — re-executed by the PRODUCER; committee members currently do NOT (defect → ADR-0017) ◆ AGENT (✅ spot-checked)

> **⛔ Verified defect (this session).** The text below describes how the *producer* re-executes. **Other committee members do not re-execute before signing** — `ShardCheckpointAttestationEmitter` signs on became-best-tip (chain selection) over the checkpoint *hash*, with no re-exec (grep-confirmed empty; the receive handler `NakamotoSyncDaemon.scala:2933-2953` only hash-verifies the signature). So on the happy path exactly one node — the producer — re-executed before adoption. **ADR-0017** makes each committee member re-execute and sign only its own verified root. See §4.
Call chain: `ShardCheckpointProducer.produce` → `assembleDelta` → injected `derivePerMgState` = `ShardCheckpointWiring.reExecDerivationWithDiff` (`ShardCheckpointWiring.scala:268`) → **`processor.processCurrencySnapshots(gl0AnchorOrdinal, …, AdoptFromSignedFields)`** (`ShardCheckpointWiring.scala:355-363`, ✅ spot-checked) → per-MG root `GlobalStateConverter.currencySnapshotMgRoot` (`:404`) + byteDiff `ChangeSet.currencyInfoChangeSet` (`:411`). The re-executed economic state is `CurrencySnapshotInfo` carrying `lastTxRefs` / `balances` / `lastFeeTxRefs` / `activeAllowSpends` / `activeTokenLocks` (`shared/.../currency.scala:89-99`).

**Precise semantics of the re-execution mode — READ THIS (✅ VERIFIED `GlobalSnapshotStateChannelEventsProcessor.scala:76-91`).** There are two modes:
- **`Recreate`** (default; numShards=1 and the `deriveMetagraphRoot` primitive): "re-derive the full state via `createContext` (proposal-artifact recreate + byte-equality)" (`:78-81`).
- **`AdoptFromSignedFields`** (the sharded committee path): "DERIVE the per-MG state by **replaying the adopted, committee-attested signed binary's OWN already-accepted events** (blocks, GIVEN rewards, token-locks, allow-spends, messages) onto the prior Info, then **VERIFY the derived root against the binary's committed `stateProof`** (economic-security gate; fall back to prior balances on mismatch…)" (`:82-91`).

So the committee path **re-applies the metagraph's own already-accepted events and verifies the resulting root against the signed stateProof** — it is not a from-scratch re-validation of each economic op's admissibility. Whether that satisfies "CL1 ops MUST be re-executed" in the full sense is **Open Question Q2 (§10)**.

Test `ShardCommitteeReExecutionSuite` asserts the load-bearing property **producer-root == verifier-root** (`…/nakamoto/ShardCommitteeReExecutionSuite.scala:52-53,275,278`) and that tampering the attested root yields `RejectedReExecutionMismatch` (`:361,373`).

### 2c. Metagraph DL1 — proof-carried (model per ADR-0016; **not separately code-verified this sweep**)
GL0 cannot hold arbitrary data-app code, so DL1 state is adopted-and-verified against a proof/root rather than re-executed. The closest verified code is the data-with-fee **balance** adopt path (gl0 adopts ml0's authoritative balances, verified vs `balancesProof`) — commit `95a19a35c` — which is economic-adjacent, not the pure DL1 data logic. Flagging as a model claim to confirm.

---

## 3. Shard checkpoint structure ◆ AGENT

`ShardCheckpoint` (`shared/.../sharding/ShardCheckpoint.scala:68-79`): `shardId`, `parentCheckpointHash`, `shardOrdinal`, `gl0AnchorOrdinal`, `slot`, **`derivedStateDelta`**, `emittedReceipts: List[CrossShardReceipt]`, `committeeSignatures: NonEmptyList[CommitteeMemberSignature]`, `epoch`, `executionBaseOrdinal`. The whole `derivedStateDelta` is inside the committee sig preimage (`:98`; `ShardDerivedStateDelta.scala:33-34`) → consensus-load-bearing.

`ShardDerivedStateDelta` (`ShardDerivedStateDelta.scala:87-94`): **`perMetagraphMptRoots: Map[Address,Hash]`** (the committee-attested commitment), **`perMetagraphStateDiff: Map[Address,ShardCurrencyStateDiff]`** ("the OUTPUT of the committee's re-execution", `:68`), `includedSnapshots`, `tokenLockBalancesDelta`, `perMetagraphArtifacts`, `perMetagraphSyncDataDelta`. `ShardCurrencyStateDiff` = `{upserts: Map[Hex,Hex], removals: Set[Hex]}` (`:37-40`) — the per-MG byte-diff over gl0's finalized base that every gl0 node applies and checks against `perMetagraphMptRoots`.

---

## 4. ⭐ The gl0 adoption gate — THE ENFORCEMENT GAP ✅ VERIFIED

This is the most important section for review. **On the happy path, gl0 adopts a shard checkpoint into the global snapshot on committee quorum signature alone — no re-execution gates finality.**

- **Happy path = signature count vs `kQuorum`, NO re-exec** — `ShardCheckpointGl0AcceptanceManager.scala:388-396`: `distinctSigners = checkpoint.committeeSignatures…toSet.size; if (distinctSigners >= kQuorum) … Accepted`. The preceding `preCheck` verifies only per-signer crypto (committee membership + Ed25519 + KES + committee-VRF), not state.
- **The code says so itself** — `:156-158` (✅ VERIFIED): *"`verifyEmbedded` admits on `kQuorum` distinct committee signatures WITHOUT re-execution (the common path) — so a corrupt committee that reaches quorum can attest a WRONG root and have it adopted."*
- **Re-exec exists only on the sub-quorum failover** — `:397-406`: `else { … reExecPath(checkpoint) }`, reachable only when `distinctSigners < kQuorum` (degraded liveness).
- **The finalize site adopts on `Accepted`** — `GlobalSnapshotAcceptanceManager.scala:794-795` (`verifyEmbedded(cp).flatMap { case Accepted => …}` adopts `derivedStateDelta.includedSnapshots`).
- **Watchtower re-exec runs, but AFTER adoption, fire-and-forget** — `watchtowerReExec` (`ShardCheckpointGl0AcceptanceManager.scala:411-441`) re-executes unconditionally, but it is invoked only from `NakamotoSyncDaemon.scala:2784-2787` (✅ VERIFIED): `Async[F].whenA(becameBestTip) { … Async[F].start(emitter.emit(checkpoint)).void }` — post-adoption, detached fiber, produces a `FraudProofEnvelope` for *later* slashing.

**Verdict (mine, from source):** re-execution-before-finalization is **ABSENT** on the common path. The economic guarantee currently rests on *detect-and-slash-after-adoption* (§5), not *prevent-at-finalization*. Config default `k-quorum = 6`, `k-draw = 8` (`application.conf:484-486`).

**And the committee itself doesn't re-execute before signing (verified this session — the root cause).** `ShardCheckpointAttestationEmitter` signs when a checkpoint becomes best-tip; no re-exec. So the quorum is chain-agreement, not economic re-validation — only the producer re-executed on the happy path. The sub-quorum `reExecPath` and `watchtowerReExec` DO run a real base-pinned re-exec (production wires `reExecuteDerivation = Some { reExecDerivationWithDiff }` at `SharedServices.scala:321`; the `noReExecDerivation` "stub" is only the tests/legacy `None` default) — but neither gates the happy path. Git trace: emitter built as a best-tip liveness seam (`6c7746aec`), quorum-adopt (`8a9d54836`), throughput tuning (`396ca81b0`), hole documented + watchtower bolted on post-hoc (`8ac7ce04f`). **Remediation: ADR-0017** — committee re-execution as the primary gate; determinism becomes liveness-critical.

**Stale-doc note (◆ AGENT):** `GlobalSnapshotAcceptanceManager.scala:711` still describes the quorum as `ceil(2·kS/3)`; the live gate is the decoupled `kQuorum` (`ShardCheckpointGl0AcceptanceManager.scala:389`). Cosmetic, but worth fixing.

---

## 5. Watchtower / InvalidStateProof slash — ACTIVE & consensus-load-bearing (numShards>1) ◆ AGENT (✅ spot-checked)

**Verdict: not shelf-ware** — wired end-to-end into the live `NakamotoSyncDaemon` and the GSAM fold; **gated on `numShards > 1`** (inert at the `num-shards = 1` default, `application.conf:500`).

- Evidence type defined: `shared/.../slashing/InvalidStateProofEvidence.scala:59`; envelope `shared/.../sharding/FraudProofEnvelope.scala`; slash record `SlashReason.InvalidStateProof` (`InvalidStateProofSlashManager.scala:204,231`).
- Deterministic on-chain verdict re-derives the root and upholds on divergence: `InvalidStateProofValidator.scala:144-161`.
- Emitter constructed (gated numShards>1 + `watchtower-enabled`): `GlobalSnapshotConsensus.scala:1960-1974`; daemon supervised with emitter/validator/pool: `:2283-2386` (`:2370-2383`).
- Live gossip subscribes `fraud-proof` topic (`SidecarClient.scala:124,158,168`); dispatch `NakamotoSyncDaemon.scala:1117-1123`; inbound dispute → validate → pool `:3024-3030`; leader embeds into the `fraudProofs` consensus field `GlobalSnapshotConsensusFunctions.scala:774,832,915`; GSAM re-validates → slash request `GlobalSnapshotAcceptanceManager.scala:2268-2288`.
- **Consensus-load-bearing sink (✅ spot-checked):** `applyWatchtowerSlashes` (`:293`) → `InvalidStateProofSlashManager.applySlash` (`:96`) → **`mpt.insert[SlashedRegistryEntry](…)` at `GlobalSnapshotAcceptanceManager.scala:3183-3184`** (Slashings partition, fieldId 34) — lands in the mptRoot byte source. Comment `:3175` confirms the write is empty-and-thus-byte-identical only on the no-slash path.
- Two slash paths, one sink: (A) gl0 self-detected **sub-quorum** re-exec mismatch → `WatchtowerSlashRequest(submitter=None)`, 100% burn; (B) **watchtower-on-quorum** → gossiped fraud proof → pool → embed → GSAM re-validate → `submitter=Some`, 5% bounty. Config: `slash-fraction = "1/1"`, `bounty-fraction = "1/20"`, `cooldown-epochs = 100` (`application.conf:628,634,639`), `watchtower-enabled = true` (`:622`).
- [INFERRED / agent] Stale scaladoc at `NakamotoSyncDaemon.scala:2976-2978` calls the on-chain slash "remaining wiring"; the code at `:3030` + the pool→embed→apply chain supersedes it.

---

## 6. Cross-shard reads = Option A (finality-first) ✅ VERIFIED (this session)

The gl0 accept path reads cross-shard values off gl0's own consensus-pinned **finalized** mirror, not a peer/committee attestation:
- `GlobalSnapshotAcceptanceManager.scala:2541-2542`: `crossShardSpendProofClient.getOrElse(ShardSubtreeProofClient.gl0Local[F](branchAwareReader))` — `branchAwareReader` is the accept-`parentTip`-bound finalized reader.
- `ShardSubtreeProofClient.gl0Local` (`ShardSubtreeProofClient.scala:104-201`) reads off gl0's finalized MPT; scaladoc warns a peer-fetch here "would FORK the cluster."
- The committee-to-committee P2P path (`ShardSubtreeProofClient.http`, `:216-373`) still anchors every proof at gl0's **last-finalized** checkpoint (`verifyProof`, `ShardSubtreeProofService.scala:267-304`), fail-closed with no anchor.
- The consuming CL1 op is still re-executed by the validator (`SpendActionValidator.scala:388-479` cross-shard read paths). Not-yet-finalized ⇒ absent ⇒ op rejected + retried. See ADR-0016.

---

## 7. Store-fidelity mirror — how gl0 carries + adopts the committee's re-executed state ◆ AGENT

Three fixes make the sharded-currency-mirror byte-faithful so adopters reconstruct the committee's per-MG root:
- **Diff-base-pin** — producer diffs against a version-retained pinned prior, never the live store: `ShardCheckpointWiring.scala:196-218` ("deliberately NO live-store fast path"), execution-base = signed store's latest `:234-235`, producer reads pinned prior `:328-335` (fail-closed if unresolvable).
- **Stage-on-adopt** — adopted root-verified bytes staged into the signed byte store (closes fail-close "holes"): `SnapshotLeaderLoop.scala:189` (`stageAdoptedPostBytes`), finalize-sink promote `:717-733` (`case None => unit` = the "Absent ⇒ skip"), 3 adopt sites `NakamotoSyncDaemon.scala:2249,3515,3675`.
- **Read-time backfill** — on a pinned-read miss, fetch peer bytes, **strip to `consensusRootEntries`**, verify `sidecarFreeMptRoot === committed stateProof.mptRoot`, persist+serve only on match else fail-closed: `PinnedCurrencyInfoReader.scala:360,369,370-371,374-377,381,386`. Wired gl0-only: `GlobalSnapshotConsensus.scala:577-595,706`.

---

## 8. Current frontier — committee-gate parent-ordinal wedge ✅ VERIFIED (resolver) + memory correction

The 2mg/2shard token-lock e2e is blocked here (not on cross-shard, not on the store-fidelity stack).
- Resolve site: `NakamotoSyncDaemon.scala:3126-3129` (`resolveParent` → orphan-buffer admitted-ord cache, else `parentOrdinalFor`), gating admission before `attestAndAdmit` (`:3163`).
- Resolver: `MetagraphParentOrdinalResolver.resolveFromBinary` (✅ VERIFIED `:105-139`) — **identity guard** requires `lastStateChannelSnapshotHash(mg) === parentHash` (`:111-112`); on mismatch or pre-bootstrap → `None` fail-closed (`:127-138`).
- The wedge (◆ AGENT, scaladoc `MetagraphOrphanBuffer.scala:24-27`): gl0 sees binary ord=N whose parent points to ord=(N−1) that gl0 never recorded → fail-closes → chain permanently stuck; drained children re-query the resolver, still `None`, **re-buffer loop** (`MetagraphOrphanBuffer.scala:35-38`; explicit "orphan re-buffer loop with gl0 trailing ml0" comment `NakamotoSyncDaemon.scala:3203-3205`). Active recovery: `stuckDetectionStream` (`:1168-1207`).

**⚠ Note (✅ VERIFIED).** The fix `40d546761` derives the parent ordinal as **`N−1` from the incoming binary's own content** — a pure, cross-node-deterministic function (`MetagraphParentOrdinalResolver.scala:60-91,116` + rationale `:16-45`) — specifically to *avoid* per-peer node-local state (the old `nakamotoFinalizedOrdinalRef`/GSI-partition reads that were per-peer asymmetric). The surviving identity guard still reads the **best-tip** overlay (`pendingReader`), not the finalized base. (One of our compacted index lines had mislabeled this "walk-finalized-not-bestTip" — corrected; the detailed note was already accurate.)

---

## 9. Key config (`node-shared/.../resources/application.conf`)
`k-draw = 8`, `k-quorum = 6` (`:484-486`); `num-shards = 1` **default — sharding/committee/watchtower path inert here** (`:500`); `watchtower-enabled = true` (`:622`); `slash-fraction = "1/1"`, `bounty-fraction = "1/20"`, `cooldown-epochs = 100` (`:628,634,639`).

---

## 10. ⭐ Open questions for external review (ranked)

1. **Q1 — Committee blind-signing (§4) — decided (ADR-0017); verify the fix.** Root cause: committee members sign on best-tip without re-executing, and gl0 adopts on quorum sig — only the producer re-executes on the happy path. The accepted fix (ADR-0017) makes committee re-execution the primary gate (sign-only-on-match + fraud-proof-on-mismatch), demoting the watchtower to a backstop. **What Codex should check:** (a) the fix scope is complete — the attestation emit path *and any other sign/attest site*; (b) determinism is airtight enough to be liveness-safe (empty-prior boundary Q4, base-pin) so honest committees don't stall; (c) the `Hash.empty` "can't-derive" defer path cannot deadlock quorum. **This is the #1 issue.**
2. **Q2 — What "re-execution" means for CL1 (§2b).** The committee path is `AdoptFromSignedFields`: replay the metagraph's *already-accepted* signed events + verify root vs `stateProof`, not a from-scratch re-validation of each op's admissibility. Does this satisfy "CL1 economic ops MUST be re-executed," or is full economic re-validation required somewhere on the path? (Note the gl0 accept path *does* run `SpendActionValidator` for spend actions — §6 — so the answer may be "layered.")
3. **Q3 — numShards=1 default (§5,§9).** The entire committee/watchtower/cross-shard economic-security apparatus is inert at the production default `num-shards = 1`. Is single-shard the intended launch posture, and if so what provides metagraph economic security there?
4. **Q4 — Watchtower empty-prior boundary (◆ AGENT).** `deriveMetagraphRoot` runs with `priorLastCurrencySnapshots = empty` for cross-node determinism (`GlobalSnapshotStateChannelEventsProcessor.scala:130-144`); for incremental-only windows over a non-empty prior this yields a "deterministic but prior-agnostic root" — a documented boundary/follow-up. Does this weaken watchtower detection for non-genesis-rooted windows?
5. **Q5 — Committee-gate wedge (§8).** The current e2e blocker. Fix must stay within the re-exec model.

---

## 11. Audit targets — where else might a signature/adoption skip the verification it implies?

The §4 committee blind-sign defect was a *documented* hole that sat unfixed for weeks. Sweep for the same class of bug — **a node signing / attesting / adopting state it did not itself re-compute.** Each target below with the question to answer:

1. **Global-snapshot Nakamoto attestation.** `NakamotoSyncDaemon.emitAttestation` / `emitTipAttestation` — does a gl0 node re-derive/verify the global snapshot before attesting, or attest on best-tip (the global analog of the shard bug)? The global consensus *does* re-derive the artifact (`GlobalSnapshotConsensusFunctions.scala:61-62`, "leader and every follower independently call `createProposalArtifact`"), so this is likely OK — but **confirm the Nakamoto attestation overlay is gated on that re-derivation, not merely on best-tip.**
2. **Sub-quorum `reExecPath` reachability.** It's real, but only fires when `distinctSigners < kQuorum` (`kDraw=8, kQuorum=6`). Confirm how often the happy path is taken vs the failover — the failover must not be effectively dead code.
3. **ml0 diff-adopt follow** (`project_ml0_diff_adopt_design`): ml0 adopts GSI + verifies `mptRoot === signed` instead of re-running createContext. Is root-equality sufficient, or does it trust fields not covered by that root?
4. **gl1 stake-root follow** (`project_gl1_historicalstake_carryforward_blocker`): inclusion-proof follow of gl0's stake root — verify the proof is checked against a *finalized* root, not accepted on assertion.
5. **Cross-shard `http` proof consumer (§6):** verifies against gl0's finalized checkpoint — confirm no path trusts the peer's *self-claimed* checkpoint/root.
6. **"Adopt authoritative X" paths** (data-with-fee balances `95a19a35c`; the adopt-gate that drops allowSpends/tokenLocks, `project_adopt_gate_drops_allowspends_run26`): confirm every adopted authoritative value is verified against a committed proof/root, and dropped sub-state can't carry a stale/empty prior.
7. **Attestation crypto completeness** (`NakamotoSyncDaemon.scala:2944-2945` verifies Ed25519 over the checkpoint hash): confirm the KES leg and committee-VRF membership are verified where the design claims them. Note the emitter scaladoc says v1 acceptance does **not** cryptographically verify the gossiped attestation's VRF proof — is that intended, and does it let a non-committee peer inflate quorum?
8. **DL1 proof-carrying (§2c):** not code-verified this sweep — confirm DL1 data-app state is adopted-and-verified against a proof, not trusted.

The grep pattern: an `emit` / `sign` / `attest` / `adopt` / `Accepted` gated on *receipt / best-tip / signature-count* rather than on *this node re-computing the thing it's vouching for.*

## Appendix — primary files
- `ShardCheckpointGl0AcceptanceManager.scala` — gl0 adopt gate + watchtower re-exec (§4,§5).
- `ShardCheckpointProducer.scala` / `ShardCheckpointWiring.scala` — committee re-exec + byteDiff (§2b,§7).
- `GlobalSnapshotStateChannelEventsProcessor.scala` — `processCurrencySnapshots` / adoption modes (§2b).
- `GlobalSnapshotAcceptanceManager.scala` — global accept, cross-shard wiring, slash fold (§2a,§5,§6).
- `ShardSubtreeProofClient.scala` / `ShardSubtreeProofService.scala` — cross-shard reads (§6).
- `InvalidStateProofValidator.scala` / `InvalidStateProofSlashManager.scala` / `WatchtowerFraudProofEmitter.scala` — slash path (§5).
- `MetagraphParentOrdinalResolver.scala` / `MetagraphOrphanBuffer.scala` / `MetagraphCommitteeGate.scala` — committee-gate wedge (§8).
- `PinnedCurrencyInfoReader.scala` — store-fidelity backfill (§7).

*Evidence gathered by 6 parallel sub-agents (line-exact, re-read) + author verification of the load-bearing claims (§4 enforcement gap, §8 resolver, §2b/§5 spot-checks, §6 cross-shard). [INFERRED] tags mark the non-direct claims to confirm.*
