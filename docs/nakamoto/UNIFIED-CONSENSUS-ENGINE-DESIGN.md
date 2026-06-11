# Unified Chain-Based Consensus Engine — Design (DRAFT for review)

**Status:** DRAFT — owner review required before any surgery (hard-fork-scale for metagraphs).
**Date:** 2026-06-11.
**Supersedes:** the 2026-05-20 "metagraphs stay BFT" direction, with owner approval pending.
**Companion evidence:** `PRODUCTION-READINESS-AUDIT.md`, the 2026-06-11 e2e campaign post-mortems (runs `bimn7o09f`, `bmnnfnao7`), and the upstream comparison (mainnet BFT 1,859 LOC / testnet 12,666 / this branch 6,245 in the consensus engine).

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

### 5.1 Eligibility — recommendation: uniform-weight Taktikos, eta from gl0

Reuse the gl0 lottery code wholesale with `stake(v) = 1/|registry|` and **eta derived from the finalized gl0 eta for the epoch** (the metagraph follows gl0 anyway; inheriting entropy removes the entire eta-computation subsystem from ml0 and makes grinding require attacking gl0). LDD parameters tuned for small N (higher fA so slots rarely go empty; ψ/γ as at gl0).

Properties: solo extension is the *normal* case at N=1 and the degraded case at N>1 (a node down ⟹ other nodes win the next slots — liveness with zero membership machinery). Forks between two simultaneous winners are routine and shallow; `ChainSelection` + the gl0 anchor settle them. There is no leader to be absent; there is no round to wedge.

The alternative — deterministic rotation `leader(s) = registry[(s + offset) mod n]` with timeout fallback — gives a predictable cadence but reintroduces a timing parameter ("how long do I wait for the scheduled producer?") which is a mini-StallDetector. Listed as open question Q1, but the lottery is recommended *because it shares 100% of the gl0 code path*.

### 5.2 Finality and the follower trust model

- A cl1/dl1/harness reader treats ml0 ordinal N as **final** when the gl0-anchored checkpoint covers it (queryable today via the combined-checkpoint serving path). This is the same trust boundary the trust-model audit greenlit: gl0 finality + committee attestation.
- **Soft confirmation** for low-latency UX: tip-minus-1 (or a small depth d) — readers that act on soft state accept reorg risk bounded by the anchor cadence (seconds). Existing memory rule applies: state advancement finality-gated, SC-binary confirmation not.
- A snapshot carries **one producer signature** (registry member, eligibility-proved). Followers verify: signature by a registry member + eligibility proof + chain link. Quorum confidence comes from the checkpoint committee, not from per-snapshot multi-sig.

### 5.3 Joining, leaving, misbehaving — all on-chain

- **Join:** operator follows the chain (download — exists), submits `RegisterValidator` (collateral pattern), is in the registry at epoch E+1, starts winning slots. No candidacy gossip, no observation keys, no admission certificates. The 2026-06-11 `OutcomeAdmission` work is the conceptual ancestor: "membership changes only through finalized outcomes" — here the finalized outcome IS the chain.
- **Leave:** `ExitValidator` tx, or
- **Demotion:** the epoch-participating-set accumulator (designed, fieldId 24) records non-participation *on chain*; exclusion at the epoch boundary is computed identically by everyone. This finally gives that design its consequence sink at ml0.
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

1. **ml0 eligibility: uniform-VRF lottery (recommended, §5.1) vs deterministic rotation?** Lottery = maximal code reuse, no timing parameters; rotation = predictable cadence but needs a fallback timeout.
2. **Soft-confirmation depth at ml0** for cl1/dl1 reads before the anchor lands (d=1? d=2? tip?).
3. **Registry bootstrap**: initial validator set in the metagraph genesis file (extends the balance-CSV pattern) — confirm.
4. **Demotion coupling**: adopt the epoch-participating-set design as the inactivity sink at ml0 in phase 2 or defer to its own workstream?
5. **Eta inheritance from gl0** (§5.1): accept the coupling (metagraph slots depend on gl0 finality cadence) or give ml0 an independent eta fold?
6. **gl0's dormant BFT path** (non-nakamoto mode in dag-l0): delete in phase 4 alongside, or keep for upstream-compat experiments?
