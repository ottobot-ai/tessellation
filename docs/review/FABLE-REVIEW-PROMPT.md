# Fable Review Prompt — Tessellation-Nakamoto (portfolio-driven)

> **HISTORICAL PROMPT - DO NOT RUN AS CURRENT INSTRUCTIONS.** Its design inputs
> include deleted authority/diff documents and a pre-fix finality model. Use the
> 2026-07-11 correctness audit and current ADRs for any new review.

> **How to use.** Hand this whole file to a Fable agent (model `claude-fable-5`) pointed at the
> repo root, `feature/committee-state-diff`. The value is in reading **actual control flow and
> state transitions**, not summaries.
>
> **READ ORDER: `docs/review/PORTFOLIO.md` FIRST, then `HANDOFF.md`, then the numbered docs they
> cite.** `PORTFOLIO.md §2` is a **ranked, tiered list of frontier questions across ALL
> workstreams** (sharding-enforcement, serde, hardfork, networking, …) — **work it
> top-down**; if budget runs out, everything above the line you reached is done. This is NOT a
> single-task review.
>
> **You write FINDINGS, not code.** Produce the `FINDING` records (§4) + a costed, ranked
> remediation plan; cheaper models (Opus/Sonnet) implement — see `PORTFOLIO.md §5` for the tiering
> and the handoff-artifact format. Do not re-derive what `HANDOFF §6` / `12-decision-log.md` mark
> as settled; spend budget on the open frontier.
>
> **Priority-1, before any "safe" conclusion** (`PORTFOLIO.md §2` Tier-0 #1): the enforcement
> teeth — does committee/watchtower re-execution actually catch a Byzantine producer before a false
> root is adopted/spent? Three independent agents flagged the gl0 happy path as accept-on-quorum-sig
> with no re-exec (`HANDOFF §1.1`). Answer this first.
>
> **Hard constraint on everything:** must not regress mainnet `v3.5.12` cross-metagraph
> functionality (`PORTFOLIO.md §0`, §4 Definition-of-Done).

---

## 1. Context

You are reviewing a from-scratch redesign of **Tessellation-Nakamoto**'s sharded-consensus and
finality mechanism. This is a permissionless, economically-incentivized DAG ledger (Constellation
Network node software). Code you review runs live and adversarial with no instant cross-node
patch. **Treat correctness bugs here as categorically more expensive than in application code** —
an error means a chain split, a silent liveness stall, a censorship vector, or fund loss via
un-enforced cross-shard token operations.

- **Repo:** this repository (Scala 2.13 / Java 21). Branch `feature/committee-state-diff`, HEAD `21933559c`.
- **Current mechanism (one line):** hierarchical DAG — L1 blocks → currency-L0 snapshots → global-L0
  (gl0) global snapshots — with a Nakamoto/Taktikos finality overlay.
- **Target mechanism (one line):** **execution sharding** where a VRF-sortitioned gl0 committee +
  watchtowers **re-execute** each metagraph's `accept()` and attest, while gl0-and-everyone else
  **adopt the byte-diff** without re-executing (Polkadot shared-security).
- **Fault model:** honest-majority per shard; up to 1/3 Byzantine on the global chain; partial
  synchrony; adaptive adversary; cross-shard collapse bound `α_total > 1/(2S)` (see HANDOFF §5.7).
  Consensus substrate is **Ouroboros Taktikos, NOT Praos** — do not apply Praos common-prefix
  formulas (see the `taktikos` skill and `~/repos/research-nipopos-2026/docs/TAKTIKOS-NOTES.md`).
- **Design docs (the spec lives in the repo):** `docs/review/HANDOFF.md` (start here);
  `docs/nakamoto/TRACK1-TRACK3-IMPLEMENTATION-SPEC.md` (design-of-record);
  `docs/nakamoto/ECONOMIC-TRUST-SHARDED-SECURITY-ARCHITECTURE.md` (trust model).

**Stack conventions to enforce, not just describe** (flag violations even when unrelated to your
main finding — cheap signal): Cats Effect 3, fs2, tagless-final, derevo derivation, Weaver +
ScalaCheck tests. No `try`/`catch` — use `EitherT`/`OptionT`/`MonadError`. `Resource` for
acquire/release. No unguarded blocking in fibers. **Consensus fractions are exact `Ratio`, never
`Double`. No embedded value-defaults for consensus params — required, or `Option`-`None` only,
threaded from HOCON `SharedConfig.nakamoto.<field>` (never `sys.env.get`).**

## 2. Mandate, in priority order

1. **Understand before touching.** Build and write down a model of the role-split adoption
   mechanism (HANDOFF §0) and the k₁/k₂ finality band (HANDOFF §1) before proposing any change.
2. **Never trade safety or liveness for elegance or performance.** If a change is cleaner but you
   can't prove an invariant still holds, flag it — don't proceed.
3. **Assume Byzantine, rational, AND simply-broken participants.** Especially a **Byzantine ml0**
   (the metagraph whose `accept()` output the committee re-executes and everyone adopts).
4. **Analysis over diffs, early.** In Phases 0–2 your output is a *finding*, not a patch. Do not
   "fix as you go" on anything safety/liveness-relevant — this code cannot be merged autonomously.

## 3. Where you are most effective — spend budget here

**High-leverage — these MIRROR `PORTFOLIO.md §2`; work that ranked list top-down. All are
IMPLEMENTATION correctness of the *settled* design, never the design itself:**
- **Enforcement teeth** (Tier-0 #1, `HANDOFF §1.1`) — does committee/watchtower re-execution
  actually catch a Byzantine producer before a false root is adopted/spent? gl0's happy path accepts
  on quorum-sig with **no re-exec**. **Answer this FIRST**, before any "safe" conclusion.
- **Adopt ≡ re-exec byte-equivalence under a Byzantine ml0** (Tier-0 #2, `HANDOFF §5.1-5.2`) — can a
  crafted input make the adopted byte-diff and the honest re-exec over the pinned `executionBaseOrdinal`
  diverge undetectably? Incl. the GAP-1 Option-field hole and the unproven "root is base-independent".
- **Spend-before-finality × band-revert** (Tier-0 #3) — can a `(k₂,k₁]` band reorg revert an
  already-spent cross-shard output (Track-2 spendability-staging is unbuilt)?
- **Consensus-root purity** (Tier-1 #4) — does any non-consensus-pinned / non-re-executable field
  leak into a root on the sharding paths? The 5-of-8-forks fault class (`11-*`).
- **Serde determinism** (Tier-1 #5, == #23) — does `loadBytes` / `from(mptStore)` reproduce the
  signed root under **every** reorg/MultiBranch shape?
- **Networking bounded + live** (Tier-1 #6) — is shard checkpoint/attestation gossip provably
  bounded AND live at numShards×numMetagraphs (drop-on-full → missed quorum → shard divergence)?
- **Hardfork activation semantics** (Tier-2 #7) · **`densityCompare` commutativity** (Tier-2 #8,
  impl of the settled rule) · **P-window ⊇ U-window on all restart/catch-up paths** (`§5.5`) ·
  **k₂ deep-revert determinism** (`§5.6`).
- **Concurrency correctness under Cats Effect:** fiber leak/cancellation, masked/uncancelable regions
  hiding starvation, effects smuggled into pure-looking code, races in the settled/finalized `Ref`
  markers and the MPT overlay revert path.
- **Spec-vs-implementation drift:** is every claimed invariant enforced on *every* path
  (error/timeout/retry/restart)?

**⛔ Do NOT re-open the consensus DESIGN.** Taktikos, k-values, LDD params, the VRF tiebreaker + its
pseudo-predictability, the delay-cliff, NIPoPoW, and the economic-security model + `α>1/(2S)` bound
are **settled research** (`PORTFOLIO.md §2` ⛔ list). **No grinding / long-range / selfish-mining
analysis** — that is decided, not your target. Your frontier is *does the code enforce it*.

**Lower-leverage (do fast or defer to a cheaper model):** mechanical refactors, derevo
boilerplate, unit-test scaffolding once invariants are known, style/format, prose once the finding
already exists. If you're deep in this list while §5 items are open, stop and reprioritize.

## 4. Working protocol — files under `docs/review/`

Each phase produces a file. The files ARE the deliverable (they survive after your session).

- **Phase 0 — `docs/review/00-project-map.md`.** Map the sharded-consensus state machine using the
  anchors in HANDOFF §7: entry points, message/checkpoint types, where finality commits, where
  re-exec vs adopt diverge, and where the OLD (pre-sharding / k₁-absolute) mechanism still lingers.
  **Stop and state your model of the role-split back before Phase 1** — a wrong mental model
  (e.g. re-reading it as "gl0 re-executes everything", or "we deleted `reExecDerivationWithDiff`")
  poisons everything downstream. Note HANDOFF §3: both override functions still *exist*; only the
  override *layering* was removed.
- **Phase 1 — `docs/review/01-invariants.md`.** Enumerate the safety invariants (Track-1
  adopt≡re-exec byte-equivalence; no two conflicting global snapshots finalize) and liveness
  invariants (k₁ optimistic finality; k₂ density-revertable band under degraded liveness), plus the
  fault-model assumptions **as actually encoded in code** vs as stated. For each: cite the code
  path(s) that uphold it and rate `Holds / Likely holds / Unclear / Likely broken` with reasoning.
- **Phase 2 — `docs/review/02-findings.md`.** One record per finding:
  ```
  ### FINDING-[NNN]
  - Severity: Critical | High | Medium | Low
  - Category: Safety | Liveness | Security | Correctness | Performance
  - Location: path:line
  - Description:
  - Attack/failure scenario:
  - Proposed direction (not necessarily a full fix):
  - Confidence:
  - Status: Open
  ```
- **Phase 3 — `docs/review/03-test-strategy.md`.** Concrete Weaver + ScalaCheck adversarial
  generators, and simulation/chaos scenarios (partitions, delayed/duplicated/reordered/dropped
  gossip, Byzantine-ml0, Byzantine-committee-minority, deep-fork past k₁). Call out whether any
  single invariant (adopt≡re-exec, or densityCompare commutativity) is load-bearing enough to
  warrant a lightweight formal spec (TLA+) over tests alone. Cross-reference the existing sims in
  `~/repos/research-nipopos-2026` and the `taktikos` skill's k-sweep tables rather than reinventing.
- **Continuous — `docs/review/HANDOFF.md`.** APPEND your findings to the existing board (§4) and
  open-questions (§5) — do not overwrite the master handoff; extend it. Keep a
  ready-to-paste "next session" prompt at the bottom of `02-findings.md`.

## 5. Also assess (the user asked specifically)

Beyond code: **our development process and workflows.** Where did the implementing agents "lose the
thread"? HANDOFF §3 is one concrete instance (imprecise "deleted X" claim). Look for others:
claims in `docs/nakamoto/*` or memory that don't match source; gates that were set default-off
without a written flip-criterion; deferred items (#23, follower deep-anchor store) that are
load-bearing for a claimed guarantee. Recommend concretely which review/verification workflows
should change and which decisions need a human or another deep-reasoning pass.

## 6. Rules of engagement

- Operate within this repo (and read-only against `~/repos/research-nipopos-2026` for sims). If a
  referenced doc is missing, say so — don't infer intent.
- **Never silently patch a safety/liveness path.** Produce a `FINDING` with `Status: Open` and
  stop. You cannot merge autonomously and shouldn't try.
- Every safety/liveness claim traces to code you actually read or an assumption you explicitly
  flag — no "should be fine."
- Prefer to break the claims in HANDOFF §5 over confirming them. A confirmed-safe with a real
  adversarial argument is worth more than a "looks fine."

## 7. Kickoff

Read `docs/review/HANDOFF.md`, then start Phase 0. Do not proceed to Phase 1 until
`docs/review/00-project-map.md` exists and your model of the **role-split adoption mechanism** has
been stated back for confirmation.
