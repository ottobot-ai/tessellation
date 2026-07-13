# Review Portfolio — where to point Fable (and Opus/Sonnet after)

> **HISTORICAL PRE-FIX REVIEW INDEX.** This portfolio indexes the 2026-07-07
> review packet and its then-current workstreams. It is not the current backlog,
> design of record, or a compatibility contract; linked claims and priorities may
> now be closed or obsolete. Recheck current source and follow
> [`../../AGENTS.md`](../../AGENTS.md),
> `CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md`,
> `CONSENSUS-ARTIFACT-LIFECYCLE.md`, and
> `CONSENSUS-PROTOCOL-TEST-PLAN.md`, with open/locked choices in
> `CONSENSUS-OWNER-DECISIONS.md`. ADR-0016/0017 remain architecture inputs,
> not proof that the implementation satisfies those contracts.

> **This is the entry point.** It ranks the frontier-reasoning questions across **all** open
> workstreams so limited Fable (`claude-fable-5`) access is spent on the highest-value work — not
> one task. Read order: **this file → `HANDOFF.md` (sharding deep-dive) → the numbered docs it
> cites.** Every workstream row links to a grounded companion doc; every claim there is
> `file:line`-anchored to HEAD `21933559c` on `feature/committee-state-diff`.
>
> Built 2026-07-07 by six grounding agents + three portfolio agents, each verified against source
> (they independently re-caught the same doc/code drift — k₁ 255→**1024**, LDD γ 15→**16** — which
> is exactly why we ground *before* spending Fable).

---

## 0. The end goal + the hard constraint

**Goal:** production-ready **multi-shard, multi-metagraph consensus** — cross-shard L2
currency-app metagraph interactions sequenced by gl0, with the currency token model **re-executed
and enforced by the hypergraph** (role-split: committee re-execs + produces byte-diff; watchtowers
re-exec to check; everyone else adopts + verifies the state proof). Full model: `HANDOFF §0`.

**Hard constraint (non-negotiable):** must **NOT regress Constellation-Labs/tessellation mainnet
`v3.5.12`**, which supports cross-**metagraph** functionality today. The bar is the e2e list in
`docker/bin/compose-runner.sh:360` — `currency, rewards, token-locks, allow-spends, spend,
data-without-fee, data-with-fee, multi-metagraph` — which must stay green **at `numShards=1` AND at
`numShards=K`**. Load-bearing invariant: gl0's unified allow-spend / token-lock / balance registries
are visible to all metagraphs, **byte-identical `numShards=1` vs `K`**. Sharding may *add* features;
it may **lose none**. This is the floor of the Definition-of-Done (§4).

---

## 1. The workstream portfolio (maturity map)

| Workstream | Maturity | Grounded in | Fable-grade Q? |
|---|---|---|---|
| **Execution sharding** (Track-1/3) | LANDED, CI-green, gated + unverified-on-cluster | `HANDOFF.md`, `01`,`10`,`11`,`13`,`14` | **YES — top 3** |
| **Serde migration** (MPT-primary / kill GSI) | ~40% (3c-A done; GSI-elimination not started) | `20-workstream-serde-migration.md` | Partial (2 real Qs) |
| **Hardfork** (economic-trust cutover) | numShards>1 cutover built; ordinal-gated fork design-only; **no `protocolVersion`** | `21-workstream-hardfork.md` | 1 (activation semantics) |
| **Networking layer** (Go sidecar `p2p/` + REST) | grounded: sidecar in-repo (Go/libp2p ~3.9k LOC); gaps = unbounded JVM inbound funnel, missing REST/ChainSync rate-limits, O(numShards) gossip fan-in | `23-workstream-networking-layer.md` | **YES — 1 frontier Q + mechanical hardening** |
| **Taktikos** (protocol, k-values, LDD, VRF-tiebreaker, delay-cliff, pseudo-predictability) | **SETTLED research** — sims run, params chosen | `taktikos` skill, `01`, `11` | **No — do NOT re-litigate (§2 ⛔ list)** |
| **Slashing consequence** | invalid-state-proof tier BUILT; equivocation + non-participation = detection-only, no sink | `22-*`, `12-decision-log.md` | No — wiring is an impl-TODO, not a design Q |
| **Unified consensus engine** | design-only; direction DECIDED (chain-based everywhere) | `22-*` | No — direction settled; impl-TODO |
| **NIPoPoW light-client** | partial; approach SETTLED | `22-*` | No — settled |
| **Attestation-timeliness / epoch-demotion incentive** | design-only / partial | `22-*` | No — design-stage, not Fable |
| **BLS aggregate sigs** | S1–S5 built **only on `feature/bls-aggregate-sigs`** (0 symbols here) | `22-*` | Later (BC 1.85) |
| **KES Wave-2** (on-chain rotation) | Slice-9 done; Wave-2 pending | `22-*` | Mechanical-ish |
| ~20 more (committee-partition-adaptivity, Mithril-over-SMT, cell-removal, crypto-unification, WAVE-2-HOCON-sweep w/ **9 `sys.env.get` still live**, …) | design-only / shelved / partial | `22-workstream-portfolio-sweep.md` | mostly No |

Full catalog (≈30 rows, each with evidence or "NO CODE FOUND"): `22-workstream-portfolio-sweep.md`.

---

## 2. The ranked Fable-grade questions (spend budget top-down)

Ranked by: genuine frontier reasoning × safety/liveness load × bearing on the active goal + the
non-regression bar. **If Fable's budget runs out, everything above the line it reached is done.**

### Tier 0 — the sharded-enforcement safety cluster (protects the non-regression guarantee)
These decide whether sharding ships a trust model **≥** mainnet's re-validation. If any fails,
sharding *regresses* economic trust — a direct violation of the hard constraint.

1. **Enforcement teeth — does the re-execution enforcement actually fire before adopt/spend?**
   Three independent agents converged here (byte-contract, invariants `INV-SAFETY-007`, hardfork
   `F3`). gl0's happy path accepts on committee **signature quorum with NO re-exec**
   (`ShardCheckpointGl0AcceptanceManager.scala:386-396`); only the producer re-execs; committee
   *attesters* sign on best-tip + VRF eligibility with **no visible re-exec**; the sub-quorum
   re-exec reads the **live** base. So the teeth depend on **(a)** committee members re-executing
   before attesting, and **(b)** the watchtower re-exec→`InvalidStateProof`-slash loop being closed
   **and timely** (slash tier is built; the re-exec *trigger* + timing vs finality is unverified).
   *Prove the enforcement catches a Byzantine producer before a false root is adopted/spent, or show
   it can't.* → `HANDOFF §1.1`, `10-reexec-byte-contract.md`.

2. **adopt ≡ re-exec byte-equivalence under a Byzantine ml0.** The Track-1 correctness core. Two
   open sub-claims from the byte contract: **(a)** "per-MG root is base-independent under
   `AdoptFromSignedFields`" is **unproven** yet load-bearing (the live-base re-exec vs pinned-base
   adopt split); **(b)** the **GAP-1 Option-field hole** — reconstruction always lifts `Some(empty)`
   and GAP-1 *skips* any Option field whose stateProof is `None`, so a non-empty unsigned Option
   field is guarded only by a possibly-un-re-executed root. *Construct a divergence the adopter
   can't detect.* → `10-*` attack checklist, `HANDOFF §5.1-5.2`.

3. **Spend-before-finality ↔ band-revert double-spend.** Track-2 (optimistic spendability staging)
   is **UNBUILT** (effects apply at the fold, no gate) while Track-3 now makes the `(k₂,k₁]` band
   **revertable**. *Can a band reorg revert an already-spent cross-shard output → double-spend /
   clawback?* This is a **new** hole this session's work opened. → `HANDOFF §0 track-map, §5.8`.

### Tier 1 — implementation-correctness of the settled design (the real frontier)
4. **Consensus-root purity — the dominant fault class (5 of 8 historical forks).** *Does any
   non-consensus-pinned or non-re-executable field leak into a root on the sharding produce/adopt
   paths?* THE search prior (`11-*`): field-32, smtRoot non-determinism, data-with-fee, allowSpends
   all fell through it. Not questioning the consensus design — verifying the code's roots are pure
   functions of consensus-pinned state. → `11-*`, `HANDOFF §5.2`.
5. **Serde determinism (== sharding #23).** Is seal-time `postBytes` sufficient that `loadBytes`
   reproduces the signed root under **every** reorg/MultiBranch shape? and is `from(mptStore,
   ordinal)` byte-faithful on Option-lifting / empty-vs-absent (the "version disease" class)?
   **Closing serde-3c-B closes sharding #23** (message-path head-sourcing). → `20-*`.
6. **Networking-layer robustness (NEW — user-raised).** Sidecar is in-repo (`p2p/`, Go/libp2p).
   **The one frontier question:** *is shard checkpoint/attestation gossip provably bounded AND live
   at numShards×numMetagraphs scale?* Sidecar shard channels are **bounded, drop-on-full**
   (`gossip.go:282-283`, sized by `ShardCheckpointBufferSize`), fan-in is **O(numShards)**, and
   GossipSub dedups so re-gossip can't heal a drop → dropped-checkpoint → missed quorum → shard
   divergence at K>1 (the **non-regression-relevant** failure mode). Everything else is **mechanical
   hardening** (Opus/Sonnet impl-TODOs): unbounded JVM inbound funnel (`GossipStream.scala:23`), no
   ChainSync serve rate-limit / unbounded range (`protocol.go:37`), zero REST rate-limit/pagination
   (`/mpt-entries` unpaginated, `SnapshotRoutes.scala:156`).
   ⚠ *Verifier note (Opus): `23-*` overstates two specifics — the buffer is per-family config-sized
   (not one 256-slot shared buffer), and an active-pull recovery IS wired for #259 metagraph-binaries
   (`sidecar.pb.go:2244`); the open part is whether it covers dropped shard-checkpoints.* → `23-*`.

### Tier 2 — foundational / genuinely-open design
7. **Hardfork activation semantics.** Given greenfield / no-wire-compat, does roots-only need an
   ordinal-gated activation, or is it a pure re-genesis cutover? A genuinely UNRESOLVED design
   decision (`ERA-REGISTRY-DESIGN.md` Q1) — decides whether the Era apparatus is load-bearing or
   vestigial. → `21-*` F1.
8. **`densityCompare` commutativity (implementation).** Is the *code's* compare actually commutative
   under adversarial fork shapes (behind default-off `band-density-reorg-enabled`)? A non-commutative
   compare forks — implementation correctness of the *settled* fork-choice rule, not a question about
   the rule. → `HANDOFF §5.3`.

### ⛔ SETTLED — NOT for Fable (closed research / decided direction; do not re-litigate)
Fable must **not** re-open these. Where one carries an implementation TODO, that is Opus/Sonnet
*build* work — never a frontier-reasoning question:
- **Taktikos** as the protocol; **k₁/k₂/R** values; **LDD** ψ/γ/fA/fB; the **VRF tiebreaker** (kept
  by explicit decision, `feedback_vrf_tiebreaker_keep`) and its pseudo-predictability. Reference: the
  `taktikos` skill.
- **NIPoPoW** soundness/approach; the **economic-security model** (Polkadot shared-security) + the
  **α_total > 1/(2S)** cross-shard collapse bound (Option A+C chosen).
- **Unified-consensus-engine** direction (chain-based everywhere — decided); **that slashing is
  needed** (decided — wire it). Both have impl-TODOs; neither is a Fable question.

### ⚠ Known-but-unaddressed — NOT settled, NOT Fable (team operational call)
- The **delay-cliff** (`Δ` p99 > ~7–8s ⇒ exponential reorg risk; deeper k doesn't help) is a
  documented *consequence* of the settled LDD design, currently **unmitigated** (`INV-FAULT-005` —
  no adaptive-k, no finalization-pause) with **no recorded accept-vs-mitigate decision**. Whether to
  add an operational mitigation is a **team ops decision**, distinct from re-litigating Taktikos —
  parked here for visibility, not handed to Fable.

---

## 3. Cross-workstream shared roots (reasoning about one = reasoning about several)

- **#1 (enforcement teeth) = hardfork F3.** The cutover is only a security *upgrade* if the
  enforcement fires; same question, two workstreams.
- **#5 serde-3c-B = sharding #23.** GSI-still-materialized *is* why the message-path stays
  head-sourced. One fix, both closed.
- **Slashing consequence is #1's deterrent — but it's an impl-TODO, not a Fable question.**
  Enforcement (#1) has no teeth if detection has no consequence; *wiring* the consequence is
  Opus/Sonnet build work (the settled design already says to do it).
- **The dominant fault class is consensus-root impurity** (5 of 8 historical forks, `11-*`) — that
  IS Tier-1 #4; it unifies #1, #2, and #5. A non-consensus-pinned or non-re-executable field leaking
  into a root is *the* way this system forks.

---

## 4. Definition-of-Done (anchored on the non-regression bar)

Sharding is "done" when **all** hold:
1. **Non-regression:** `compose-runner.sh:360` e2e list green at `numShards=1` **and** `numShards=K`;
   registries byte-identical across K (the v3.5.12 bar). *This is the floor — nothing ships red here.*
2. **Enforcement teeth verified** (Tier-0 #1): committee/watchtower re-exec catches a Byzantine
   producer before adopt/spend, demonstrated on a cluster, not just unit.
3. **adopt ≡ re-exec proven** (Tier-0 #2): no undetectable divergence; base-independence proven or removed.
4. **No spend-before-finality double-spend** (Tier-0 #3): either Track-2 staging built, or a proof
   the band-revert cannot revert a spent cross-shard output.
5. **Band-flip criterion met:** the §5.6 deep-fork cluster-uniformity sim passes before
   `band-density-reorg-enabled` flips true.
6. **#23 closed** (or explicitly accepted): message-path global reads pinned-at-anchor.
7. **Slashing loop closed** for the invalid-state class (detect → slash → deter), timely vs finality.

---

## 5. Fallback tiering (when Fable access ends)

| Work | Tier | Handoff artifact |
|---|---|---|
| Novel safety/liveness arguments, economic attacks, delay-cliff defense design, enforcement-teeth proof/break | **Fable** | a `FINDING` (see below) + a *costed, ranked* remediation plan — **Fable writes findings, NOT code** |
| Integration, multi-file synthesis, adversarial verification of a specific finding, reconciling agent claims vs source | **Opus** | picks up a `FINDING @ HEAD <sha>` with a `Verifier note`; also runs the Phase-0 model-confirmation gate for Fable |
| Implement a `Status: Proposed` fix, write tests from `03-test-strategy.md`, re-run a Phase-2 audit on one module, mechanical serde/kryo scrub | **Sonnet** | the `FINDING` + a one-line entry point |

**Findings format** (so a cheaper model can act cold): the `FINDING-NNN` template in
`FABLE-REVIEW-PROMPT.md §4`, extended with `VERIFIED @ HEAD <sha>` and a `Verifier note`. Cheap
per-finding re-run: re-audit only the cited `file:line` + its test after a fix lands.

**Driving loop (minimize Fable round-trips):** Fable states its Phase-0 model of the role-split →
**Opus verifies it against source** (not the user) → Fable works the Tier list top-down, one
`FINDING` per issue, stopping at each safety/liveness path rather than patching. The user decides
which Tier-0 finding to remediate first.

---

## 6. Packet index

`PORTFOLIO.md` (this) · `HANDOFF.md` (sharding master) · `FABLE-REVIEW-PROMPT.md` (kickoff) ·
`01-invariants.md` · `10-reexec-byte-contract.md` · `11-adversary-numbers-and-fork-taxonomy.md` ·
`12-decision-log.md` · `13-e2e-observability-contract.md` · `14-packet-gaps-and-fallback.md` ·
`20-workstream-serde-migration.md` · `21-workstream-hardfork.md` ·
`22-workstream-portfolio-sweep.md`.

*Recommended still-to-build (cheap, Opus-doable, before Fable): a golden cross-shard trace (one
token op end-to-end through re-exec→checkpoint→adopt→cross-shard consume) — it's the spine that
makes Tier-0 #1/#2 attackable rather than reconstructed. Flagged, not yet written.*
