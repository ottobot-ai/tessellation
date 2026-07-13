# Unified Chain-Based Consensus Engine — Design (DRAFT for review)

**Status:** SUPERSEDED FOR ML0. Do not implement this draft as the current target.
**Date:** 2026-06-11.
**Owner clarification 2026-07-11:** ML0 may remain BFT for small, well-connected
metagraph networks. GL0 remains Nakamoto/Taktikos/LDD, and execution-shard
checkpoint chains remain staircase/Nakamoto. A future ML0 chain-engine migration
requires a separate owner decision and must not leak BFT or this draft's generic
engine assumptions into GL0. Current architecture is in
`../review/CONSENSUS-ARTIFACT-LIFECYCLE.md`.
**Companion evidence:** the 2026-06-11 e2e campaign post-mortems (runs `bimn7o09f`, `bmnnfnao7`) and the upstream comparison (mainnet BFT 1,859 LOC / testnet 12,666 / this branch 6,245 in the consensus engine).

---

## 1. Motivation — the disease, not the symptoms

Every metagraph-layer consensus failure in the 2026-06 campaign was an instance of one problem:

> **PBFT-style rounds require continuous agreement on WHO IS PARTICIPATING before any progress can be made.**

The boot-race fork (self-appended facilitator base), the racing candidacy pointers (exact-key registration match), the stall-without-eviction, the `minViableQuorum` floor that cannot exclude a dead peer at N=2, the frozen-entropy leader election that re-elects an absent leader forever, the admitted-but-still-joining facilitator that wedges every round at `progress=1/2` — all of these are membership-agreement machinery failing, not artifact-agreement machinery failing. The artifact logic (snapshot creation, validation, state transition) has been correct throughout.

The upstream history quantifies the cost: mainnet's modest BFT is 1.9k LOC and survives by retrying; testnet grew to 12.7k LOC of health machinery (stall detection, view changes, vote locks, quality tiers, readmission probation) chasing the same liveness edges for months; this branch carries 6.2k. **Chain-based consensus does not have the problem**: there is no facilitator set, a solo producer extends legally, joiners follow until eligible, and disagreement is resolved *after the fact* by fork choice instead of *before the fact* by membership agreement.

The architectural fact that makes this safe for metagraphs: **gl0 is already the finality gadget for everything.** A metagraph snapshot only counts once a committee-attested shard checkpoint embedding it is inside a *finalized* gl0 snapshot (Slice-14 machinery, live today). ml0-internal BFT instant-finality is redundant security purchased with the most fragile code in the repo.

## 2. Design principle

> **One engine, many policies. Layers differ in consensus *properties*, never in consensus *codebases*.**
>
> The interface of consensus is an algebra; implementations are instances.

## 3. The algebra

Six small typeclasses over a hash-linked chain of artifacts. These are the *interface of consensus*; everything else (the engine loop, storage, gossip) is generic machinery written once against them.

```scala
/** Hash-linked artifacts. Already satisfied by GlobalIncrementalSnapshot / CurrencyIncrementalSnapshot. */
trait ChainLike[A] {
  def parentRef(a: A): Hash
  def ordinal(a: A): SnapshotOrdinal
}

/** WHO may extend the chain at a slot — the only layer-specific LIVENESS policy.
  * P is the eligibility proof carried in the artifact header.
  */
trait Eligibility[F[_], A, S, P] {
  /** Local: produce a proof if self is eligible to extend `parent` at `slot`. */
  def tryProve(slot: Slot, parent: A, state: S): F[Option[P]]
  /** Universal: verify a producer's claimed eligibility. Pure in (slot, parent-state, producer, proof). */
  def verify(slot: Slot, parent: A, state: S, producer: PeerId, proof: P): F[Boolean]
}
// LAW (soundness):     tryProve = Some(p)  ⟹  verify(self, p) = true
// LAW (determinism):   verify is a function — identical inputs give identical answers on all honest nodes
// LAW (non-emptiness): over any window of W slots, some registered validator is eligible (liveness floor)

/** Fork choice — a total preorder on observed chains. */
trait ChainSelection[F[_], A] {
  def compare(a: ChainSegment[A], b: ChainSegment[A]): F[Comparison]
}
// LAW (totality + transitivity)
// LAW (extension monotonicity): a chain never loses to its own proper prefix

/** WHAT is irreversible — a monotone map from (tip, evidence) to a finalized ancestor. */
trait Finality[F[_], A, E] {
  def finalizedPrefix(tip: A, evidence: E): F[Option[SnapshotOrdinal]]
}
// LAW (monotone):  finalized ordinal never decreases as evidence accumulates
// LAW (prefix):    the finalized ordinal is an ancestor of some observed tip
// LAW (safety):    two honest nodes never finalize conflicting artifacts at the same ordinal

/** WHO is a validator — a registry derived ONLY from finalized chain state. */
trait Membership[F[_], S] {
  def validatorsAt(state: S, epoch: EpochProgress): F[SortedSet[PeerId]]
}
// LAW (agreement):         identical finalized state ⟹ identical validator set on all honest nodes
// LAW (epoch granularity): the set is constant within an epoch; changes take effect at boundary E+1
// LAW (closure):           membership changes ONLY via transactions in finalized artifacts
//                          (register / exit / demotion-by-recorded-inactivity) — never via local
//                          health observation, timeouts, or gossip-time judgments

/** Co-signing observed tips — the evidence stream feeding Finality. */
trait Attestation[F[_], A] {
  def attest(tip: A): F[Signed[TipAttestation]]
  def record(att: Signed[TipAttestation]): F[Unit]
}
```

**The membership closure law is the heart of this design.** Every 2026-06 failure traces to membership decisions made outside finalized state: self-appended bases (local), candidacy registration matching (local racing pointers), stall eviction (local timing), quality tiers (local observation). Under the closure law there is *nothing to disagree about*: you are in the registry at epoch E or you are not, and the registry is on the chain.

### 3.1 The engine (written once)

```
loop every slot s:
  parent   ← ChainSelection-best tip
  proof    ← Eligibility.tryProve(s, parent, state)
  if Some: artifact ← ConsensusFunctions.createProposalArtifact(parent, mempool…)   // EXISTING seam
           sign, persist, broadcast

on receive(artifact):
  Eligibility.verify ∧ ConsensusFunctions.validateArtifact                          // EXISTING seam
  store; tip ← ChainSelection; if tip changed: Attestation.attest(tip)
  Finality.finalizedPrefix → advance finalized pointer → prune / anchor / emit
```

This *is* `SnapshotLeaderLoop` + `NakamotoSyncDaemon`, generalized. gl0 already runs it; the work is extracting the seam and instantiating it for ml0.

## 4. Instantiations

| Policy | **gl0** (10⁵–10⁶ nodes, open) | **ml0** (≤ few hundred, registered) | **gl1 / cl1 / dl1** |
|---|---|---|---|
| `Eligibility` | VRF stake lottery + LDD snowplow (Taktikos — **exists**) | VRF lottery with **uniform weights** over the registry, eta inherited from gl0 (§5.1) — or deterministic rotation (§7 Q1) | none — no chain; keep the batch-and-co-sign block engine |
| `ChainSelection` | maxvalid-tk blended with attestations (**exists**) | longest → lower head slot → anchor-compatibility (a chain consistent with the last gl0-anchored checkpoint beats one that isn't) | n/a |
| `Finality` | attestation-2/3 ∥ depth-k₁, k₂ absolute floor (**exists**) | **gl0 anchor**: ordinal N is final iff a committee-attested shard checkpoint covering N is embedded in a *finalized* gl0 snapshot (Slice-14 — **exists**); soft-confirmation = small local depth for UX | inclusion of the block in the parent L0's chain |
| `Membership` | stake registry + epoch staggering, HistoricalStakeSnapshot (**exists**) | **on-chain validator registry**: `RegisterValidator` / `ExitValidator` txs following the node-collateral/delegated-staking pattern (schema → manager → acceptance → MPT partition → validator → route); inactivity demotion via the epoch-participating-set design (EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md) | n/a |
| `Attestation` | KES-signed TipAttestations over sidecar (**exists**) | the **shard-checkpoint committee signatures** (Slice-14) double as the attestation/evidence stream — no new crypto | n/a |

Note how many cells say **exists**. The unified engine is mostly a *deletion* project: the only genuinely new construction is the ml0 registry tx family and the ml0 `Eligibility`/`Finality` instances, both of which follow established patterns.

## 5. ml0 specifics

### 5.1 Eligibility — DECIDED direction (owner, 2026-06-11): scheduled rank-staircase ("LDD over an ordered set")

The owner's requirement: the producer should be **known beforehand** at the metagraph level (predictable cadence, minimal fork rate, fast client finality), while still inheriting fresh randomness from gl0. Private/unpredictable election buys anti-targeting in OPEN networks of anonymous stakers; a metagraph registry is public, so privacy buys nothing — only **unbiasability of the order** matters, and eta-from-gl0 provides it.

**The scheme** (a known-good shape — Ouroboros-BFT's deterministic schedule, expressed as an LDD variant):

  - Per epoch/interval, derive the priority permutation `π = shuffle(registry(epoch), eta_gl0(epoch), intervalIndex)`.
  - **Rank staircase — deterministic duty, NOT a lottery (owner, 2026-06-11)**: rank-r's production window opens at `r·δ` after the parent snapshot, measured on the producer's LOCAL clock, and within its window the validator produces with **probability 1**. There is no threshold function, no ramp, no coin flip at the metagraph level — LDD contributes only its gap-clock structure (windows keyed to time-since-parent) and the eta-shuffled ordering, not a probability curve. Clock skew can open overlapping windows on different nodes; that is fine — overlaps are resolved by `ChainSelection`, not prevented by protocol. Rank-0 emits immediately; if silent, rank-1 emits at δ, rank-2 at 2δ, …
  - **Policy, not validity**: "I waited my turn" is unprovable (time is local), so the ladder is a PRODUCTION policy. Validity = signed by a registry member for the interval, correct chain link. The schedule is enforced by `ChainSelection`: at equal length, prefer the lower-rank producer (and prefer the attested tip — see 5.2). A withholding rank-0 can displace rank-1's block by at most depth-1, and loses outright once the fast rail attests rank-1's block.

The pure uniform-weight lottery (previous recommendation) remains the fallback alternative; it shares more gl0 code but gives up the known-producer property. The staircase's single new parameter δ is benign in a chain-based engine: mis-tuning causes a shallow fork, never a wedge — unlike BFT timeouts, which gate progress.

Solo extension remains the N=1 normal case and the N>1 degraded case (all higher ranks silent ⟹ your window opens). Liveness requires only ONE live registry member.

### 5.2 Finality — two rails, mirroring gl0; the client-latency win lives here

> **ml0 finality = countersign-2/3 (fast rail) ∥ gl0-anchor (slow rail, unconditional)**

- **Fast rail**: validators countersign observed tips (the `Attestation` instance — same TipAttestation machinery as gl0). A snapshot with ≥2/3-of-registry countersignatures is final for metagraph clients ~1 gossip RTT after production (~1–2s). CRITICAL INVARIANT: **production never waits for countersignatures** — votes are evidence for finality, never a precondition for progress; the moment production depends on votes, a round (and its wedge class) has been rebuilt.
- **Slow rail**: ordinal N is final unconditionally once a committee-attested shard checkpoint covering N is embedded in a *finalized* gl0 snapshot (Slice-14 — exists). If the fast rail starves (mass validator outage), the chain still grows (staircase) and the anchor still finalizes.
- The two rails compose with 5.1: a known producer means there is almost never a competing tip to split attestations — the fast rail stays crisp (the genesis-fork seeding bug was precisely attestation-splitting between competing tips).
- A snapshot carries **one producer signature**; followers verify registry membership + chain link, and read quorum confidence from countersignatures (fast) or the checkpoint committee (anchored). Existing rule preserved: state advancement finality-gated, SC-binary confirmation not.

### 5.3 Joining, leaving, misbehaving — all on-chain

- **Join:** operator follows the chain (download — exists), submits `RegisterValidator` (collateral pattern), is in the registry at epoch E+1, starts winning slots. No candidacy gossip, no observation keys, no admission certificates. The 2026-06-11 `OutcomeAdmission` work is the conceptual ancestor: "membership changes only through finalized outcomes" — here the finalized outcome IS the chain.
- **Leave:** `ExitValidator` tx, or
- **Demotion:** remains a design item. It requires a new, explicitly specified consensus-carried participation record and deterministic epoch-boundary exclusion rule. No field-24 accumulator or production demotion sink exists today.
- **Equivocation:** two signed artifacts for the same slot = slashing evidence carried in a tx (slashing safety bar memory: cryptographically verifiable evidence, byte-equivalent outcomes).

### 5.4 What gets deleted (ml0/BFT engine path)

`ConsensusEventLoop`/`ConsensusFSM`/`ConsensusRoundRunner`, `ConsensusStateCreator/Updater/Advancer/Remover` (phase machine), `StallDetector`, `AbandonmentTracker`, `PeerQualityTracker`, `TrailingCommonAncestorFilter`, `FacilitatorSelector`, `EvictionVoteTracker`, declarations (`Facility`/`Proposal`/`Signature`/withdrawals), `EventTriggerGuard` + Time/Event triggers (replaced by the slot clock), observation keys & peer-registration plumbing, the Candidates/Facilitators/Eligible outcome fields. **≈5–6k LOC**, comprising essentially every component that failed in the 2026-06 campaign.

### 5.5 What survives (the seam the owner wants kept)

- `ConsensusFunctions` (`createProposalArtifact`, `validateArtifact`, context functions) — the artifact algebra, untouched.
- Snapshot creators (`CurrencySnapshotCreator`, acceptance managers, the whole GSAM/state pipeline).
- The typed `Key/Artifact/Context` parameterization (the `Outcome` type is replaced by `(tip, finalizedPtr, registryView)` — a simpler algebra).
- Event mempool, gossip/sidecar transport, storages, the trigger daemon (becomes the ml0 slot clock).
- All Slice-14 checkpoint/committee machinery (it *gains* a role: ml0's finality evidence).
- L1 block engines (swap/token-lock/StateChannel batch-and-co-sign) — small, observable since `948d2b2e1`, candidates for a later "co-sign primitive" unification (separate doc, not this fork).

### 5.6 Additional design considerations (owner-reviewed 2026-06-11)

1. **δ (rank window) is metagraph-configurable, consensus-visible.** Default 5 slots. Lives in metagraph genesis / on-chain config, NOT per-node HOCON — policy skew cannot fork (fork choice absorbs it) but agreed δ keeps cadence clean.
2. **Event-driven production with a max-gap heartbeat.** The metagraph `epochProgress` is a load-bearing clock (allow-spend/token-lock windows starve when it crawls — the entire 2026-06 window-failure family). Rank-0 produces on events; an empty heartbeat snapshot every T_max keeps the clock honest on quiet metagraphs.
3. **Timestamp discipline.** Artifact timestamps: monotone vs parent, ≤ now + ε (skew bound, explicit). Validity bounds only — local time NEVER appears in fork choice; rank is the deterministic tiebreaker.
4. **Censorship bound.** A tx censored by f colluding validators waits ≤ ~(f+1) intervals for an honest rank. This rotation property is the answer to "one signer per snapshot" at metagraph trust scale.
5. **Equivocation accountability — start reputational, bond opt-in.** Two signed snapshots for one (parent, window) = byte-verifiable slashing evidence (safety bar: evidence-carrying tx, deterministic outcome). v1: record + demote via the epoch-participating-set (its missing consequence sink). Registry schema reserves an optional collateral/bond field for bonded metagraphs. OPEN: owner call on default.
6. **Anchor cadence bounds client risk.** Soft-state reorg window ≈ checkpoint cadence + gl0 finality lag. Checkpoint cadence = metagraph config; expose `anchoredOrdinal` on the ml0 API (mirror of gl0's settled/k₂ field) so clients pick tip / countersigned / anchored.
7. **Epoch-aware countersignatures.** Fast-rail denominator = registry at the attestation's epoch; attestations carry epoch and verify against registry-at-epoch (registry churn at boundaries cannot double-count or strand evidence).
8. **Genesis edge.** Window 0 anchors on the genesis timestamp; initial registry ships in metagraph genesis (balance-CSV pattern).
9. **Observability is part of the engine.** Ship with: expected-producer gauge, rank-window countdown, countersign coverage, anchor lag, per-interval production source (rank). Every 2026-06 failure hid in unlogged state; the engine must not be able to fail silently.

### 5.7 Current shard-checkpoint duty runs on the shared slot grid

One genesis-anchored wall-clock slot grid drives production. GL0 snapshot production uses the Taktikos/LDD lottery. Execution-shard
checkpoint production does not: a public deterministic committee draw plus `ShardSlotLeader.dutyOrder` assigns one member to each
staircase window. `slotDuration` is configuration, not a consensus constant.

**Historical defect corrected by the current implementation.** The earlier implementation mapped the shard-local "slot" to the **gl0 anchor ordinal**
(`GlobalSnapshotConsensus.scala`: "the gl0 anchor ordinal IS the shard-local slot index"), and the producer is
triggered once per anchor. That was a determinism shortcut from the sharding slices
needs no wall-clock trust), NOT a discussed design decision — and it inverted the intended cadence: the shard lottery
gets one draw per global snapshot (~6.5 slot-durations observed mean inter-snapshot time) while gl0 draws every slot,
so shards tick ~6.5× SLOWER than the layer they feed. Run-10's "Gap A" (51 anchor-draws ≈ 3.3 min without a shard
leader, consuming half a 6.5-min allow-spend budget) is this inversion, not a fat lottery tail.

**REV 2 (owner, 2026-06-12): shard production = SHUFFLED STAIRCASE, not a lottery.** Runs 13–14 showed that at
per-slot draws a small committee forks at GENESIS (everyone instantly eligible at unbounded gap) and siblings under
any quorum lag — at ANY LDD density (a ψ=5/γ=45 retune still forested; run 14 had ZERO quorum-Accepted receipts).
Only a unique-producer-per-window schedule avoids it. Polkadot reaches the same split: the relay chain runs a
lottery (BABE) over the open validator set while parachains author via **Aura round-robin** over their small
registered collator sets — and relay cadence ≥ parachain cadence, because the parent chain is the inclusion clock.
Ours mirrors both: gl0 keeps Taktikos; the shard committee is hash-sorted per `(shardEta, shardOrdinal)`
(`ShardSlotLeader.dutyOrder`), rank r proposes for `staircase-delta-slots` (default 5) slots starting one slot after
the parent's wire slot, wrapping modulo committee size (liveness = ONE live member; censorship bounded by rotation).
Duty is intended to be a pure function of proposal-parent-bound data (parent ref + child slot + anchored eta/roster). The current receiver
derives the parent from its local shard store, so embedded-artifact validity remains asymmetric until `SHARD-C-009` supplies portable parent
evidence. Shard chain growth stays structurally ≤ gl0 growth (the one-outstanding-checkpoint rule is the throughput governor); the
staircase's job is mint LATENCY — rank-0 produces within ~1 slot of
the lane opening. Client-facing finality speed lives in the ml0 countersign rail (5.2), not in shard cadence.

**Current mechanics:**
  - The `ShardCheckpoint` envelope carries its production **`slot`** explicitly. Validation currently checks parent monotonicity,
    deterministic staircase duty, and a registered-key possession proof over `(shardEta, slot)`. It does **not** yet enforce a canonical
    upper bound; `SHARD-C-010` requires the exact containing GL0 snapshot's signed slot certificate so receiver wall clock never decides
    artifact validity.
  - `gl0AnchorOrdinal` REMAINS on the envelope as chain-link data (epoch/eta resolution, adoption anchoring) — it is
    no longer the lottery clock.
  - The producer runs from the slot tick. `slotGap` = slots since the parent checkpoint's wire slot and indexes the staircase window
    (`(slotGap − 1) / δ mod K`), not an LDD threshold.
  - Every GL0 node has the slot clock. Production permits exactly one outstanding checkpoint per shard; only the exact containing GL0
    snapshot reaching Phase 2 releases its successor. The current honest-producer gate is partial because the checkpoint does not yet carry
    portable verifier-checkable Phase-2 parent-anchor evidence.

**Safety:** committee membership and duty restrict who may propose, but neither signatures nor depth authorize economic state. Every GL0
adopter must recreate each included CL1 transition at the signed finalized execution base before storing, attesting, selecting, or embedding
the checkpoint. Watchtower slashing is defense in depth.

### 5.8 High-traffic metagraphs — spreading levers (owner-reviewed 2026-06-12)

The structural fact that sorts all options: **a metagraph's binary chain is sequential** (parent-hash chain), so a
single hot metagraph cannot be striped across shards — its lever is *batch size per fold*, never shard-parallelism.
Shard-parallelism spreads *many* metagraphs across lanes, not one metagraph across many.

Our embed lane is variable-width: each shard folds ≤ 1 checkpoint per gl0 ord, but the checkpoint's per-MG **window**
(`includedSnapshots: mg → NonEmptyList[binary]`) is an arbitrary-length contiguous chain segment. (Contrast Polkadot:
its lane unit — a core — carries a *fixed-size* parachain block, so hot chains must rent MORE cores; ours widens.)
Levers, in activation order — each conditional on a pressure we do not yet have:

1. **NOW**: per-slot lottery (§5.7) + remove the sender batch ceiling (`StateChannelBinarySender` RetryMode ships the
   full contiguous backlog, not 64/tick) so windows actually absorb bursts. The shard layer is then a bulk-service
   queue: throughput = window size × fold rate, stable for any arrival rate below wire bandwidth.
2. **IF a window cap is ever introduced** (wire size / committee verify cost): deterministic fair packing —
   round-robin water-fill across MGs in address order until the cap (a pure function of the buffer ⇒ byte-verifiable
   by re-exec; no persistent deficit state). Protects light MGs co-located with a hot one. Until a cap exists there
   is nothing to ration.
3. **IF MG-count ≫ shards or hot/light co-location hurts**: epoch-keyed load-aware assignment —
   `shardIdFor(address, epoch)` computed identically by all nodes from finalized per-MG traffic counts (mirror
   ordinal deltas), greedy heaviest-first packing; hot MG gets an isolated shard. Today `ShardAssignment.shardIdFor`
   is static `SHA-256(address) mod numShards` — this lever is a real workstream (every `shardIdFor` call site becomes
   epoch-aware + boundary handoff). Migration-safe: a moved MG's first window on its new shard chains off gl0's SC
   tip (the existing unseeded-MG fallback).
4. **IF both a window cap AND a hot MG**: fold k consecutive same-shard checkpoints (parent-linked, order preserved)
   in one gl0 ord — the Polkadot elastic-scaling analog. Escape hatch only; meaningless while windows are uncapped.
5. **IF adversarial congestion**: collateral-weighted bandwidth shares (stake-weighted QoS). Economic policy, owner
   call, not a correctness mechanism.

## 6. Migration plan (hard fork LAST, per standing phase order)

| Phase | Content | Risk gate |
|---|---|---|
| 0 (**done**, `6895195cb`) | Interim solo-producer mode: candidate admission off; followers join inert (production gate). e2e green path restored. | focused e2e |
| 1 | Extract the algebra typeclasses; re-express the **gl0** Nakamoto path as the first instance (no behavior change — pure refactor, equivalence = byte-identical snapshots on replay corpus). | replay + full e2e |
| 2 | ml0 instance behind config (`metagraph-consensus = chain`), N=1: must be observationally equivalent to today's solo producer (cadence, snapshot contents). Registry tx family lands here (collateral pattern). | A/B e2e |
| 3 | N≥2 ml0: lottery + follower production; kill the wedge class for good. The 2-node e2e topology becomes a *real* 2-validator test for the first time. | multi-node e2e + fault injection (kill a node mid-run) |
| 4 | Delete the BFT engine path (§5.4); dl1/cl1's ml0-follow already consumes snapshots, unaffected. | full suite |
| 5 | Hard fork: wire/genesis cleanups, registry in metagraph genesis (balance-CSV pattern per the stake-via-genesis rule). | — |

## 7. Open questions for owner review

1. ~~ml0 eligibility: lottery vs rotation~~ **RESOLVED (owner, 2026-06-11): scheduled rank-staircase with eta-from-gl0 ordering (§5.1) + countersign fast rail (§5.2).** Remaining tunable: δ (per-rank window; straw-man δ = p99 mg gossip latency × 2 ≈ 2–4s) and the per-interval reshuffle granularity (per-snapshot vs per-epoch — per-snapshot ordering uses `intervalIndex = parent ordinal`, giving every snapshot a fresh ladder).
2. **Soft-confirmation depth at ml0** for cl1/dl1 reads before the anchor lands (d=1? d=2? tip?).
3. **Registry bootstrap**: initial validator set in the metagraph genesis file (extends the balance-CSV pattern) — confirm.
4. **Demotion coupling**: adopt the epoch-participating-set design as the inactivity sink at ml0 in phase 2 or defer to its own workstream?
5. **Eta inheritance from gl0** (§5.1): accept the coupling (metagraph slots depend on gl0 finality cadence) or give ml0 an independent eta fold?
6. **gl0's dormant BFT path** (non-nakamoto mode in dag-l0): delete in phase 4 alongside, or keep for upstream-compat experiments?
