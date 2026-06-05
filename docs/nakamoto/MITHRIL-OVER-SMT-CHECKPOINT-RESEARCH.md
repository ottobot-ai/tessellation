# Mithril-over-SMT Checkpoint Certificates for Taktikos — Research Report

**Date:** 2026-06-02. **Status:** research synthesis (not a committed design).
**Method:** deep-research harness — 5 search angles, 21 primary sources fetched, 104
claims extracted, top 25 adversarially verified (3-vote, 2/3-refute to kill); 23
confirmed, 2 refuted. Grounded in the Taktikos notes, `docs/nakamoto` NIPOPOW/SMT/GKL
docs, and `~/repos/research-nipopos-2026`.

## Question

Is a Mithril-style aggregate-signature certificate layer over an SMT-committed
NIPoPoW level-µ structure sound and worthwhile for light-client / succinct
verification in Tessellation's **Ouroboros-Taktikos** setting (NOT Praos), and how
should it be constructed?

## Verdict

**Yes — with one decisive caveat.** A Mithril-style aggregate-signature certificate
over an SMT-committed state root is a **sound, well-precedented** design for succinct
light-client verification, but **only** if:

1. It is treated as a **STATE-commitment attestation** ("a stake quorum of this
   epoch's committee signed this root"), **NOT** as a proof of chain-validity or
   per-block VRF rarity; and
2. It is paired with an explicit **long-range / weak-subjectivity defense supplied by
   the Taktikos protocol itself** — because the certificate layer provably does **not**
   supply it.

Mithril is direct, deployed precedent that the pattern works. But **every proven
soundness result in this space rests on assumptions that do NOT transfer to Taktikos
for free** (see the Taktikos gap, below).

## Key findings (confirmed)

### F1 — Sound as a state-commitment attestation, not a chain-validity proof (high)
Mithril signs a Merkle root (leaves = hashed stake-distribution tuples); the client
**locally recomputes the root** and verifies the quorum signature, then fast-bootstraps
by hopping certified checkpoints and **resumes normal per-block validation** after
reaching the latest checkpoint. The cert proves "a stake quorum signed this
commitment," not chain validity. Two structural rules: the signer-set commitment (AVK
Merkle tree) and the signed-message commitment (state root) MUST be **distinct trees**.
*Sources: mithril.network certification docs; IACR 2021/916.*

### F2 — The cert does NOT establish canonical chain selection or long-range safety (high)
PoS light clients are **inherently weakly subjective** and need a recent trusted state
root out-of-band. A chain of historical checkpoint certs is exactly the structure most
exposed to long-range corruption of OLD stake. **Key-evolving signatures alone do not
stop stake-bleeding** ("cannot be prevented by key-evolving cryptographic
techniques"). The cert layer must anchor to a Taktikos-supplied long-range defense.
*Sources: Stake-Bleeding IACR 2018/248; ethereum.org weak-subjectivity.*

### F3 — LOAD-BEARING LESSON: the Mithril CVE was about WHAT was bound, not the crypto (high)
The Mithril forgery CVE (**GHSA-724h-fpm5-4qvr**, Feb 2025) was **not** a BLS break —
the aggregate key and multisig verified correctly. The gap: protocol parameters
(`phi_f`, `k`, `m`) were **not certified** by the certificate chain, so a single
adversarial *registered* signer could manipulate `phi_f/k/m` to win enough lotteries
to forge a quorum. The fix (Distribution 2506) **added certification of protocol
parameters + epochs** — additive binding, not a crypto patch.
**→ The signed message MUST commit to the full quorum/eligibility parameter set + epoch
index + committee/stake-distribution identity + SMT root *together*. Binding only the
root repeats Mithril's exact mistake.**
*Source: GHSA-724h-fpm5-4qvr.*

### F4 — BLS PoP + signer-set-via-registration validated, with two hard constraints (high)
(1) Use a **second PoP element / Merkle-committed verification keys (AVK root)** to
defend rogue-key attacks; membership via log-size Merkle paths. (2) **Aggregation
compresses N signatures to 1 but NEVER the signer set** — Mithril itself does not use
raw aggregation for the quorum because individual signatures must still verify the
per-signer eligibility mapping. Eligibility is the stake-weighted `phi(w)=1-(1-f)^w`
lottery — **the same function as Tessellation's `1-(1-f)^s` rarity** — and a valid cert
needs **k-of-m winning lottery indices** (a stake quorum over unique winning indices,
k=445/m=2728 in production), not all signers. Confirms the team's own
"per-(ordinal,id,level) dense storage is a non-starter."
*Sources: IACR 2021/916; docs.rs/mithril-stem.*

### F5 — For VRF-private leadership (closest to Taktikos), use the Agrawal/Neu/Tas/Zindros handover (high)
Naive committee-handover signatures do NOT transfer: next-epoch leaders aren't knowable
in advance, so epoch *j* cannot sign off epoch *j+1*'s leader keys. The correct fix:
handover signatures sign the **next-epoch randomness + a Merkle-Segment tree of the
CURRENT stake distribution**; **per-leader VRF rarity is verified separately at
CHALLENGE TIME** (open the VRF proof + stake Merkle path against the revealed
threshold). The cert alone is insufficient — per-leader VRF rarity must still be
checked. *Sources: IACR 2022/1642 = arXiv 2209.08673.*

### F6 — PoPoS sublinear bootstrapping is INTERACTIVE; a detached cert is a different, stronger-to-prove position (high)
PoPoS's O(log N) soundness rests on **existential honesty** (verifier connected to ≥1
honest prover) via an interactive bisection game — fundamentally NOT a non-interactive
detached proof. When all provers are adversarial, the succinct client can be convinced
of an incorrect commitment. **A detached BLS certificate cannot offload soundness onto
interactivity** — it must carry its own cryptographic soundness via a per-checkpoint
**honest-stake threshold (≈ m/2)** that holds non-interactively.
*Sources: arXiv 2209.08673; IACR 2022/1642.*

### F7 — All proven results assume corruption models Taktikos doesn't get for free (high)
Mithril is proven only under **STATIC corruption with a fixed stake distribution** (the
authors concede dynamic/long-range corruption of an old distribution "directly violates
our model's assumptions"). PoPoS requires a **delayed honest majority over two epochs +
a slowly-adaptive adversary** (corruption takes effect two epochs later) + key-evolving
signatures. These are **protocol-layer inputs the cert depends on, not theorems it
proves.** *Sources: IACR 2021/916 §6.5; IACR 2022/1642.*

### F8 — THE TAKTIKOS FRONTIER: Praos-family long-range proofs do NOT transfer (high)
Praos/Genesis long-range defenses (moving checkpoints in `maxvalid-mc`; Genesis
`maxvalid-bg` local-density rule, adopt deeper fork with more blocks in `s = k/(4f)`
slots after the fork point) are proven in the **Praos model with a uniform active-slot
coefficient f** and the bound `alpha*(1-f)^(Δ+1) >= (1+ε)/2`. **Taktikos's LDD-snowplow
NON-uniform election + maxvalid-tk break the uniform-f assumption** underlying both the
α bound and the `s=k/(4f)` density window — so neither soundness proof transfers without
re-derivation. `ChainSelection.scala` already implements a `maxvalid-bg` density rule,
so the *mechanism* is design-grounded — but the *guarantee* is unproven for Taktikos.
*Sources: Genesis IACR 2018/378; Stake-Bleeding IACR 2018/248.*

## Recommended construction (if pursued)

- **Certify the SMT root at SPARSE EPOCH BOUNDARIES** (not dense low-µ per-block).
- **Bind into the signed message** (per the Mithril CVE): SMT root **+** full
  quorum/eligibility parameter set **+** epoch index **+** committee/stake-distribution
  identity **+** next-epoch randomness **+** Merkle-Segment stake commitment.
- **PoP-defended BLS**, signer sets as **registration-index participation bitmaps**
  (aggregation compresses sigs, never the signer set).
- **Defer per-leader VRF-rarity verification to challenge time** (open VRF proof + stake
  Merkle path); the cert does not prove rarity.
- **Distinct trees** for signer-set commitment (AVK) vs signed message (state root).
- **Anchor to a Taktikos long-range defense** (the cert does not provide one).

## Refuted (did NOT survive verification)

- *"Context-sensitive transactions completely neutralize stake-bleeding"* — 1-2 refuted.
- *"Weak-subjectivity checkpoints act as revert limits / finality anchors"* — 0-3 refuted.

## Open questions (the real work before building)

1. **Threshold re-derivation:** the correct per-checkpoint honest-stake-majority
   threshold (Taktikos analog of Mithril's `k=m*phi(1/2+a)` / PoPoS's `m/2`) once the
   LDD-snowplow NON-uniform election replaces uniform f — needs derivation against
   Taktikos's actual win distribution + the L-independent-trials rarity, validated vs
   the `adv-7block-private` sims.
   **RESOLVED (sim `sim/mithril-quorum-threshold` @ 626c4fa in research-nipopos-2026, 2026-06-02):**
   the LDD non-uniformity does **not** enter the quorum. gl0 finality is **ALL-active
   stake-weighted 2/3** (`TipTracker` — every validator attests; no LDD/committee gate on *who*
   attests), i.e. standard BFT (safe+live iff α<1/3); and the `K·σ` committee sortition is
   *uniform* (Layer A reproduced §6's `exp(−0.057K)`, MC≈exact-binomial — model validated). So **no
   Taktikos-specific re-derivation is needed.** For a *sampled-committee* checkpoint cert of size m,
   the safe quorum is ordinary sampled-quorum analysis: **m scales with ε** (α=1/3 needs m≈800 for
   ε=1e-12; k/m≈0.45–0.5), **distribution-independent**, sitting just above PoPoS `m/2` and well
   below Mithril's `2/3`. LDD correction ≤1.6%, operatively 0 for gl0. ⇒ the genuinely-open item is
   now #2 (the long-range anchor), NOT the threshold.
2. **Long-range anchor:** a detached offline cert can't use PoPoS's interactive escape
   hatch — what does Taktikos supply? (a) lean on the existing `maxvalid-bg` density rule
   in `ChainSelection.scala` (soundness unproven for Taktikos), (b) moving-checkpoint
   depth-k bounds, (c) external objective anchor. Which is sound, at what cadence?
3. **Bound-message schema:** the minimal complete field set to avoid the GHSA class of
   forgery, and how it interacts with the existing `AggregateSigned(hash, signers, sig)`
   shape + `ValidatorKeyRegistry` / KES rotation lifecycle.
4. **Signer-set scale:** registration-index bitmaps at sparse epoch checkpoints — the
   cross-over where dense low-level superblock attestation becomes prohibitive; does the
   L-independent-trials level-µ structure admit sparse-checkpoint-only certification that
   still supports succinct NIPoPoW verification?

## Caveats

- **Taktikos gap (central):** NO source proves any of these constructions secure in the
  Taktikos model. Every quantitative guarantee cited (Mithril Thm 2 quorum, Genesis α
  bound, `maxvalid-bg` window, PoPoS common-prefix k) needs independent re-derivation
  against Taktikos's election distribution before being relied on. Our L-independent-
  trials level-µ variant further departs from the Kiayias nested-rarity model that
  off-the-shelf NIPoPoW compression proofs assume — so superblock-compression soundness
  is also unestablished here.
- **Source quality:** strong — every load-bearing claim anchors to a primary peer-
  reviewed paper, official protocol doc, reference implementation, or the security
  advisory. No load-bearing claim rests on blog/forum-only evidence.
- **Not verified:** the `adv-7block-private` sim quantitative outputs were referenced as
  grounding but not re-examined in this pass.

## Primary sources

- Mithril: [IACR 2021/916](https://eprint.iacr.org/2021/916.pdf) · [certification docs](https://mithril.network/doc/mithril/advanced/mithril-certification/cardano-stake-distribution/) · [mithril-stem](https://docs.rs/mithril-stem) · [CVE GHSA-724h-fpm5-4qvr](https://github.com/input-output-hk/mithril/security/advisories/GHSA-724h-fpm5-4qvr)
- PoPoS / superlight: [IACR 2022/1642](https://eprint.iacr.org/2022/1642) = [arXiv 2209.08673](https://arxiv.org/pdf/2209.08673)
- Stake-Bleeding: [IACR 2018/248](https://eprint.iacr.org/2018/248.pdf)
- Ouroboros Genesis: [IACR 2018/378](https://eprint.iacr.org/2018/378.pdf)
- BLS multisig / rogue-key: [Boneh-Drijvers-Neven](https://crypto.stanford.edu/~dabo/pubs/papers/BLSmultisig.html) · [pkreg](https://rist.tech.cornell.edu/papers/pkreg.pdf) · [eth2book signatures](https://eth2book.info/latest/part2/building_blocks/signatures/)
- Weak subjectivity: [ethereum.org](https://ethereum.org/developers/docs/consensus-mechanisms/pos/weak-subjectivity/)

## Implementation reality (verified against the code, 2026-06-02)

A read of the landed NIPoPoW code corrects and sharpens this design:

- **The tower is IMPLEMENTED** (S1–S5 under `modules/node-shared/.../domain/nakamoto/nipopow/`:
  `LevelTrialComputer`, `SuperLevelParams` (L=10, super-levels 1–9), `TowerFinalizer`,
  `TowerProofBuilder`, `TowerVerifier`). The `NIPOPOW-PROPOSAL.md` "research-only" header is
  **stale** — `NIPOPOW-IMPLEMENTATION-PLAN.md` tracks the real landed state.
- **Tower vs cert = COMPOSE, not subsume.** Tower = *objective, committee-free* chain-validity/
  weight proof (`TowerVerifier` re-derives the VRF rarity per header — no quorum). Mithril cert =
  *committee-trust* constant-size state-commitment attestation. Orthogonal on the validity-vs-state
  axis.
- **The SMT is the composition seam — and it already exists.** `smtRoot` is a field on
  `GlobalSnapshotStateProof` (NOT field-19 — field-19 is `SystemIndex`), populated by a separate
  `HistoricalCommitmentSmtStore`, and is **trusted today via the snapshot's own signature** (all
  gl0 nodes derive a byte-identical `smtRoot`). The SMT leaf is
  `PerOrdinalCommitment(hypergraphRoot, incrementalSnapshotHash, towerEligibility)`. So one
  objective `SmtVerifier` inclusion proof against a trusted `smtRoot` already reveals BOTH the state
  root AND (the slot for) the tower eligibility of any finalized ordinal.
- **Concrete wiring gap:** `PerOrdinalCommitment.towerEligibility` is currently hard-wired to
  `NotComputed` at `GlobalSnapshotAcceptanceManager.scala:~416`. Flipping it to the real
  tower-sourced max-passed-level is "a pure value change, no tuple-shape churn." Doing so makes the
  SMT leaf actually carry tower state — completing the seam.

**Revised role of a Mithril-style cert here:** it does NOT need to re-commit the tower or invent a
new root. It anchors the **already-existing `smtRoot`** for committee-trusting fast clients (today
that root is only signature-trusted by the producer); the tower remains the committee-free validity
proof feeding one field of each SMT leaf. The "what to sign" set from the CVE lesson becomes:
`smtRoot + params + epoch + committee identity` — over the root that already exists.
