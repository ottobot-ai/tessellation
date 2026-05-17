# Slashing — Design

**Status:** draft, 2026-05-17
**Scope:** the ledger-side deterrent that makes per-epoch committees safe. Detection in S4 of the committee-sortition sequencing; this doc is the burn-side ledger logic referenced by `COMMITTEE-SORTITION-DESIGN.md` §8 Q#2.

Greenfield rollout — no production migration constraint. Defaults can be aggressive.

---

## 1. Goal

A committee member that signs two contradictory `MetagraphAttestation`s on the same `(metagraph_address, snapshot_ord)` with different `binary_hash` is provably equivocating. The chain accepts a `SlashableEvidence` transaction proving this and applies:

1. **Stake reduction** — the offender's combined (delegated + collateral) stake drops by `slash_fraction`.
2. **Eviction** — operator key is removed from the active registry for `cooldown_epochs`.
3. **Bounty** — `bounty_fraction` of the slashed amount goes to the submitter; remainder burns.

Per-epoch committees become safe to adopt (`COMMITTEE-SORTITION-DESIGN.md` §8 Q#6) once equivocation is this expensive — the adversary's adaptive-corruption window closes because corrupting a key only lets them sign once before slashing destroys the stake.

---

## 2. Threat model

The adversary controls some subset of operator keys with total stake fraction `f_adv ≤ 1/3` and wants to either:

- (a) get two competing binaries committee-attested at the same metagraph snapshot ord (creates ambiguity about what was finalized), or
- (b) selectively attest one binary while privately holding back evidence for a fork attack.

Both require at least one committee key to sign two distinct binaries at the same `(metagraph_address, snapshot_ord)`. That signature pair is the slashable evidence.

Honest committee members never produce contradictory signatures. The KES (`§1.2`) ensures the secret key is bound to a specific period; once an honest operator signs binary A at period p, the only way to sign binary B at the same `(metagraph_address, snapshot_ord)` is to deliberately re-sign — i.e., adversarial choice, not a glitch.

---

## 3. Detection (S4 of committee-sortition sequencing)

Observers maintain a window of recent `MetagraphAttestation`s. When a second attestation arrives with:

- same `peer_id`
- same `metagraph_address`
- same `snapshot_ord`
- different `binary_hash`

…it's evidence. Both attestations are KES- and committee-VRF-verified before observing peers submit a `SlashableEvidence` tx.

**Where the detector lives:** JVM-side observer in `node-shared/domain/nakamoto/SlashingDetector.scala`. Reads from the same `MetagraphAttestationAggregator` that S3's pre-inclusion gate uses — single source of truth for inbound attestations. Detector emits a `SlashableEvidence` candidate to the local mempool; the first-to-include operator earns the bounty.

**Race avoidance:** two operators may both detect the same equivocation and submit duplicate evidence transactions. The duplicate-detection rule is "first SlashableEvidence for `(slashed_peer_id, metagraph_address, snapshot_ord, binary_hash_a, binary_hash_b)` wins." Subsequent duplicates fail validation as "already slashed."

---

## 4. Evidence transaction

JVM-side schema (`modules/shared/src/main/scala/io/constellationnetwork/schema/slashing/SlashableEvidence.scala`):

```scala
final case class SlashableEvidence(
  evidenceA: Signed[MetagraphAttestation],   // first conflicting attestation
  evidenceB: Signed[MetagraphAttestation],   // second conflicting attestation
  submitterId: PeerId,                       // bounty recipient
  bountySignature: HashSignature             // submitter signs the evidence pair
)
```

This is **NOT** a sidecar gossip message — it's a regular L0 transaction included in the gl0 global snapshot. The sidecar gossip carries `MetagraphAttestation`s; the JVM aggregates and detects equivocation; the JVM constructs and submits a `SlashableEvidence` through the same mempool that other L0 txs use.

### 4.1 Validator (gl0 acceptance)

`SlashableEvidenceValidator.validate(evidence)` returns `F[Either[SlashingRejection, SlashableEvidence]]`:

1. **Identity match.** `evidenceA.peerId == evidenceB.peerId`.
2. **Subject match.** `evidenceA.metagraphAddress == evidenceB.metagraphAddress`.
3. **Ord match.** `evidenceA.snapshotOrd == evidenceB.snapshotOrd`.
4. **Distinct binaries.** `evidenceA.binaryHash != evidenceB.binaryHash` (otherwise it's just a duplicate retransmission).
5. **Both KES-signed by the same master VK.** Reuses `KesRegistry.verify` (Slice 9 path).
6. **Both committee VRF proofs verify** under the accused operator's published VRF VK and the N-2 stake fraction at the relevant eta period. Reuses `CommitteeSortition.verifyMembership`.
7. **Not already slashed.** Lookup MPT key `slashings/<peer_id>/<metagraph_address>/<snapshot_ord>` — if present, reject.
8. **Within evidence window.** Current epoch ≤ `event_epoch + evidence_window`.
9. **Submitter signature.** `bountySignature` verifies over `Blake2b256(evidenceA.bytes ‖ evidenceB.bytes ‖ submitterId.bytes)` under `submitterId`'s long-term key.

Validation steps 5-6 are the expensive ones (3 KES verifies + 2 VRF verifies); cache hot.

---

## 5. Ledger effects

When `SlashableEvidence` is accepted in a global snapshot, GSAM applies the following effects atomically (same accept path as other L0 transactions):

| Field | Change |
|---|---|
| `activeDelegatedStakes[address][peer_id]` | × `(1 - slash_fraction)` for every record on the peer; reduces both numerator and denominator of stake-weighted VRF |
| `activeNodeCollaterals[address][peer_id]` | × `(1 - slash_fraction)` similarly |
| `slashedRegistry[peer_id]` | new entry: `{ event_ord, cooldown_until_epoch, evidence_digest }` |
| `slashings/<peer_id>/<metagraph_address>/<snapshot_ord>` (MPT key) | set to `evidence_digest` to block double-slashing |
| `balances[submitter_address]` | += `slashed_amount × bounty_fraction` |
| (burn) | `slashed_amount × (1 - bounty_fraction)` is removed from circulating supply — no minter, destroyed |

After the cooldown epoch, the peer's slashed registry entry is pruned and they may re-register stake. Their old delegated stake is gone; they bring fresh stake.

### 5.1 Effect on consensus

The peer's stake fraction `σ_i` drops; their committee-VRF threshold drops proportionally. They may still be elected leader for L0 slots (separate path — leader VRF doesn't read the slashed-registry), but the committee gate makes their metagraph attestations unable to count toward any binary's quorum until they re-stake.

The `slashedRegistry` is read by `StakeRegistry.markActive` to immediately mark the peer inactive — they cannot accidentally contribute to `MinActiveQuorumFraction` while slashed.

---

## 6. Defaults (greenfield, aggressive)

| Parameter | Default | Env override | Rationale |
|---|---|---|---|
| `slash_fraction` | `1.0` (100%) | `NAKAMOTO_SLASH_FRACTION` | Full economic deterrent. Single equivocation event = total loss. Per-epoch committee adoption requires this hardness. |
| `bounty_fraction` | `0.05` (5%) | `NAKAMOTO_SLASH_BOUNTY_FRACTION` | Small incentive to submit evidence; not large enough that a self-attacker can recover through self-submission (since 95% burns). |
| `cooldown_epochs` | `100` | `NAKAMOTO_SLASH_COOLDOWN_EPOCHS` | At eta-period ≈ snapshot cadence × `R` (default ~hours), 100 epochs is ~100 hours-equivalent. Long enough that a returning slashed peer must build trust fresh. |
| `evidence_window` | `100` epochs | `NAKAMOTO_SLASH_EVIDENCE_WINDOW` | Matches cooldown — beyond this, the stake has rolled over and submitting old evidence accomplishes nothing but bookkeeping. |
| `max_evidence_pending` | `1024` per epoch | `NAKAMOTO_SLASH_MAX_PENDING` | Anti-DoS cap on pending evidence; legitimate equivocation events are rare. |

Production-grade values are downstream of public testnet experience.

---

## 7. Why these defaults compose cleanly with the rest of the stack

- **KES Slice 9 (load-bearing)**: a re-used KES sig is what makes the second attestation forgeable evidence. The KES design's "secret key burns forward at the end of each period" is what closes the window — an honest operator who deletes their old SK can never re-sign. (Detection works regardless; the deterrent is to prevent the adversary from CONFIDENTLY producing the second sig in the first place.)
- **N-2 stake staging (#180)**: σ at evidence time is the N-2 frozen value, not the live value. This avoids the "adversary front-runs by reducing their own stake before evidence lands" attack — by the time the evidence is included, the relevant σ is already cemented.
- **Per-epoch committee adoption**: with slashing live, an adversary corrupting a key during the epoch faces 100% loss if they equivocate. The cost of a successful equivocation attack scales linearly with the stake they had to acquire to be in the committee in the first place. This is the load-bearing economic argument for the per-epoch shift.

---

## 8. Sequencing within committee-sortition impl

This doc covers what S4 of `COMMITTEE-SORTITION-DESIGN.md` §9 produces. The full slice sequence becomes:

| Slice | Adds | Status |
|---|---|---|
| S1 | `CommitteeSortition[F]` primitive + property tests | ✅ landed |
| S2 | `pb.MetagraphAttestation` + sidecar gossip + JVM bindings | ✅ landed |
| S2.5 | `MetagraphAttestationAggregator[F]` (verify + tally; warn-only) | next |
| S3 | Pre-inclusion gate (load-bearing) — `processMetagraphBinary` waits for ≥2K/3 | next-next |
| S4a | `SlashableEvidence` schema + validator + tests | this doc |
| S4b | `SlashingDetector[F]` + bounty path | this doc |
| S4c | GSAM accept-time ledger effects (stake reduction + cooldown + burn) | this doc |
| S5 | Per-epoch committee shift (VRF msg uses `eta_period` not `snapshot_ord`) | this doc (gated by S4) |
| S6 | e2e validation at degenerate K=N | final |

S4 is structured as three sub-slices (schema, detector, accept) so each is independently reviewable.

---

## 9. Out of scope here

- **Mithril-style threshold sigs over slashing certificates** — future light-client work, not consensus.
- **Cross-chain slashing for delegators** — if a delegator's chosen operator is slashed, the delegated stake reduction is captured in §5's `activeDelegatedStakes` write. No separate cross-chain mechanism needed.
- **Soft-forks for protocol disagreement** — only equivocation on the same VRF input is slashable. An operator that fails to attest, or attests "wrongly" against a hash that some peers don't have, is not slashed (that's just a liveness or fork-resolution issue).
- **Anti-grinding (selective censorship of evidence)** — once a node sees evidence, they include it in the next block they're leader for. Censoring requires controlling the leader VRF, which is a separate, larger attack.

---

## References

- `COMMITTEE-SORTITION-DESIGN.md` §8 Q#2 — original Option C reference.
- `project_cross_shard_cq_collapse_bound` — slashing as Option C of cross-shard mitigation.
- KES Slice 9 (#174) — KES verification path reused by step 5 of evidence validation.
- Cardano slashing literature — Cardano has no slashing; we diverge here. Closer reference is Ethereum's slashing-for-equivocation in Casper FFG / Beacon Chain.
