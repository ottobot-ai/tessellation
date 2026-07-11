# Packet Gaps & Fallback Scaffolding — an independent completeness critique

> **HISTORICAL PRE-FIX REVIEW.** This critique describes the 2026-07-07
> authority/diff design. Deleted document names below are git-history evidence,
> not current files or requirements. Do not use it as current architecture.

> **Role of this file.** Independent, skeptical review of the Fable-review packet
> (`docs/review/HANDOFF.md`, `docs/review/FABLE-REVIEW-PROMPT.md`) *before* limited Fable
> budget is spent. It does **not** validate the packet — it hunts for what a frontier
> reviewer will need and the packet does not yet give, and it designs the Opus/Sonnet
> fallback for when Fable access ends. Every gap is proven by pointing at what is absent,
> with a `file:line` / doc / commit / grep for the load-bearing claims.
> **Verified against the working tree 2026-07-07**, branch `feature/committee-state-diff`,
> HEAD `21933559c`. Grep commands that back the "absent"/"unbuilt" claims are inline.
> This file only writes itself — it touches no source, config, or other doc.

---

## SECTION A — PACKET GAPS

The packet is unusually good at *provenance discipline* (§3's "deleted behavior vs deleted
symbol" correction is exactly right, and the Evidence Appendix is real). The gaps below are
therefore not sloppiness — they are **scope holes and missing anchors** that will make Fable
either (a) reason on a false premise, or (b) burn budget rediscovering context. Rated by how
much each **blocks Fable's usefulness**: `BLOCKER` (Fable reasons wrong or wastes a session
without it) → `HIGH` → `MEDIUM` → `LOW`.

### A1 — No CURRENT end-to-end cross-shard trace; the only walkthrough is STALE `[BLOCKER]`

The packet's North Star (`HANDOFF.md:12-38`) states the target *shape* but there is **no single
document that traces one cross-shard token op through the actual code**: ml0 `accept()` →
committee re-exec → `ShardCheckpoint` → gl0 adopt-and-verify → cross-shard consume (`I-ONCE`) →
finality. `HANDOFF.md §7` (lines 205-227) is a *flat list of file anchors*, not a trace — it
tells a reviewer where `standardCompare` lives but not how a `SpendAction` created in shard A is
consumed in shard B, which is the whole point of the workstream.

The one file that *is* an end-to-end trace — `docs/nakamoto/SHARD-CHECKPOINT-FLOW-WALKTHROUGH.md`
— is **materially stale and describes the pre-role-split architecture**. It documents the
"current Slice-13 wiring" as: *"re-feeds `includedSnapshots` through the legacy
`processStateChannelEvents` pipeline … and **discards** the committee's … `perMetagraphMptRoots`"*
(that doc's §0). That is precisely the world **before** `dc0dbe1c7` made byte-diff-adopt primary.
The FABLE prompt itself warns that a wrong mental model "poisons everything downstream"
(`FABLE-REVIEW-PROMPT.md:84-86`) — yet a reviewer skimming `docs/nakamoto/` for "flow" will land
on this doc and adopt exactly the wrong model. The packet neither points to it nor warns it is
stale.

**Why it blocks Fable:** the central Track-1 safety claim (`HANDOFF §5.1`, adopt ≡ re-exec) can
only be attacked if the reviewer holds the *real* data path in their head. Without a current
trace they must reconstruct it from ~10 files across three modules in Phase 0 — that is the most
expensive possible use of frontier budget.

**Close before Fable:** write `docs/review/00-project-map.md` yourself (it's a Phase-0
deliverable anyway — Opus can do it, see Section C) as a *narrated* trace with `file:line` at each
hop, and add a one-line "STALE — pre-role-split, do not use for the data path" banner-pointer in
the packet for `SHARD-CHECKPOINT-FLOW-WALKTHROUGH.md`. Surface the three current diagrams
(`docs/nakamoto/diagrams/{1-shard-to-hypergraph,2-metagraph-to-shard,3-combined-resolution-and-finality}.{dot,png}`,
dated Jul 1) explicitly in the packet — they are referenced only in a *commit description* in the
board (`HANDOFF.md:77`), never handed to the reviewer.

### A2 — Track-2 (optimistic finality / spendability staging) is UNBUILT *and* absent from the packet `[BLOCKER]`

The workstream has **three** conceptual tracks, not two. `SHARDING-PRODUCTION-READINESS-PLAN.md`
§0/§4 defines **Track 2 (latency): optimistic finality — watchtower approvals give fast
spendability, depth-k₁ backstops**, whose "one real build" is the **spendability staging gate**
(`plan §4.3`: *"stage a checkpoint's economic effects … release on `approvalCount ≥ kApprove OR
depth ≥ k1`"*). `ECONOMIC-TRUST-SHARDED-SECURITY-ARCHITECTURE.md §6.3` confirms the decided model
is **DEFER-NOT-REVERT** and states plainly: *"the staging gate itself is still TO BUILD — today
the fold applies effects immediately."*

It is still unbuilt on HEAD:

```
$ grep -rIn "kApprove\|approvalCount\|spendabilityGate\|stageCheckpoint\|defer-not-revert" modules/*/src
   → (zero hits in source)
```

The packet **never mentions Track-2 at all** — not as done, not as deferred, not as out of scope.
The only occurrences of "optimistic" in the packet are `HANDOFF.md:199` ("optimistic-finalize", a
retention decision) and `FABLE-REVIEW-PROMPT.md:89` ("k₁ optimistic finality") — neither is the
Track-2 workstream. So the packet's own North Star ("cross-shard token op **at latency not gated
by the full k₁ challenge window**", the explicit goal in `TRACK1-TRACK3-IMPLEMENTATION-SPEC.md:13`)
has **no landed mechanism and no acknowledgement that the mechanism is missing.** Today effects
apply immediately at the gl0 fold — which means the honest cross-shard path either (a) is spendable
before any challenge window, contradicting the whole watchtower/defer-not-revert safety story, or
(b) is not accelerated at all, defeating the latency goal. That fork is *unresolved and invisible*
in the packet.

**Why it blocks Fable:** a frontier reviewer asked to verify "adopt ≡ re-exec is safe" (§5.1) and
"the economic model holds" will implicitly assume the challenge-window/staging discipline exists,
because every design doc describes it. It does not. Fable may certify a safety argument whose
premise (effects are not spendable until `approvals ≥ kApprove OR depth ≥ k1`) is not in the code.

**Close before Fable:** add a "Track-2 — NOT STARTED" row to the board (`HANDOFF §4`) and one line
to §0 stating that the landed work is Track-1 + Track-3 only, the spendability staging gate is
unbuilt, and **effects currently apply at the fold** — so any Byzantine-committee reasoning must
treat the challenge window as *not yet enforced on the spend path*. This is a two-line honesty fix
that changes what Fable is allowed to assume.

### A3 — I-AUTH (metagraph-source authenticity) is UNBUILT and the §5.1 question rests on it `[BLOCKER]`

The single-honest-verifier safety argument — the reason committee size is claimed *not* to be the
safety parameter (`ECONOMIC-TRUST §4.2`) — requires that a committee **cannot forge the source
snapshot**. That is invariant **I-AUTH** (`ECONOMIC-TRUST §2.3`, §6.2 item 3, §7 item 3): the
metagraph snapshot must carry an *env-dependent operator-majority signature* (≥1 sig dev; majority
of registered keys on integrationnet/mainnet). Every one of those sections marks it **"still TO
BUILD."** Confirmed on HEAD — the gate is still a single-signature check:

```
$ grep -rIn "validateStateChannelAllowanceList\|operatorMajority\|metagraphOperatorThreshold" modules/*/src/main
   → StateChannelValidator.scala:167,182  (only; no env threshold / majority logic)
```

The packet does **not mention I-AUTH anywhere** (`grep -i "I-AUTH\|source authenticity" docs/review/*`
→ zero hits). Yet `HANDOFF §5.1` / `FABLE-REVIEW-PROMPT §3` make "adopt ≡ re-exec under a
**Byzantine ml0**" the #1 frontier question. The dangerous case is a Byzantine ml0 *colluding with
its committee*: if the committee can sign a source snapshot the metagraph operators never signed,
watchtowers re-execute the *forged* input, get `reExecRoot === stateProof` (both derived from the
forgery), and detect nothing. Without I-AUTH, "watchtowers catch a lying committee" has a hole the
packet does not disclose.

**Why it blocks Fable:** Fable is most valuable proving/breaking exactly this. Handing it §5.1
without telling it the source-authenticity gate is a single signature invites a **false "safe"
verdict** — the worst outcome for a safety review.

**Close before Fable:** add I-AUTH to the board as "NOT BUILT — single sig today" and re-word §5.1
to name the collusion case explicitly: *"assume a Byzantine ml0 whose committee co-signs; the
source-authenticity threshold (I-AUTH) is not yet implemented — does adopt ≡ re-exec still catch a
forged source, or is I-AUTH load-bearing for §5.1?"* Let Fable rule on whether I-AUTH is a
precondition for the Track-1 claim (it very likely is).

### A4 — Committee/watchtower sortition + attestation mechanics have NO anchors in the packet `[HIGH]`

The North Star's mechanism is "a shard committee (`K_S`, VRF-sortitioned) **plus watchtowers**
(VRF-sampled) re-execute and attest" (`HANDOFF.md:22-25`). But `HANDOFF §7` (the "read control
flow here" map) lists fork-choice, finality, adopt/re-exec and overlay files and **omits every
sortition/attestation/enforcement file**:

- `CommitteeSortition.*` — the committee draw and `verifyShardMembership` (test exists:
  `CommitteeSortitionSuite.scala`; design: `COMMITTEE-SORTITION-DESIGN.md`).
- `WatchtowerFraudProofEmitter` / `WatchtowerFraudProofPool` — the watchtower dispute producers
  (confirmed present as compiled classes under `modules/dag-l0/.../infrastructure/sharding/`).
- The **watchtower sampling + "no-show → escalate" coverage rule**, which
  `SHARDING-PRODUCTION-READINESS-PLAN.md §1` calls **"load-bearing (the price of no-revert)"** —
  i.e. if VRF sampling ever yields zero honest re-execs for a checkpoint within k₁, the depth-k₁
  leg cannot catch the fraud. `grep -rIn "no-show\|noShow\|escalate\|watchtowerSample" modules/*/src/main`
  returns nothing → the coverage guarantee appears **unimplemented**, and the packet does not flag it.
- Intra-epoch watchtower rotation vs predictable-within-epoch committee (`CURRENCY-APP-TOKEN-ENFORCEMENT.md §3`).

`HANDOFF §5.7` asks Fable to check the cross-shard collapse bound (`α_total > 1/(2S)`) "vs
committee-size decoupling" but hands it **no file** — the reviewer cannot check the bound against
`K_S` without the sortition code. This is a HIGH gap: a whole security-relevant subsystem is named
in §0 and unmapped in §7.

**Close before Fable:** extend `HANDOFF §7` with a "Committee / watchtower / enforcement" block
citing `CommitteeSortition.*`, `WatchtowerFraudProof*`, `ShardCheckpointWiring.committeeFor`,
`InvalidStateProofValidator*`, and explicitly state the coverage/no-show rule's build status
(looks **unbuilt** — verify).

### A5 — Slashing consequence path is split, and the economic-deterrent sizing is absent `[HIGH]`

Detection exists; the packet says nothing about whether **punishment** does. Reality (verified) is
*two-tiered and asymmetric*, which is exactly the kind of nuance a reviewer must be told:

- **Invalid-state-proof tier — BUILT.** `InvalidStateProofSlashManager.applySlash` reduces stake
  ×(1−`slashFraction`) + cooldown, written durably to `Slashings` fieldId 34, applied at
  `numShards>1` (`GlobalSnapshotAcceptanceManager.scala:319,457-460,934`;
  `InvalidStateProofSlashManager.scala:32,77,93`; production `slashFraction = 1/1`,
  `config/types.scala:248,267`).
- **Equivocation + non-participation tier — STILL DETECTION-ONLY (shelf-ware).**
  `PRODUCTION-READINESS-AUDIT.md §2 item 3` and §4: *"equivocation validator built but **uncalled**
  … no consequence path"*; `ShardCheckpointEquivocationValidator` / `ShardNonParticipationCounter`
  have zero non-test call sites.
- **VaR cap — NOT ENFORCED.** The economic bound that makes DEFER-NOT-REVERT *rational-safe*
  (value-at-risk per checkpoint ≤ committee-stake × `slashFraction`, `ECONOMIC-TRUST §6.3`,
  `plan §6`) has **no enforcement in code** (`grep -rIn "valueAtRisk\|VaR cap" modules/*/src/main`
  → nothing; only the `slashFraction` knob exists). `kApprove` is likewise unsized (A2).

The packet mentions slashing **nowhere**. Economic attacks (grief a committee into a slash;
front-run spendability before the challenge window; profit-then-slash where profit > stake) are
squarely Fable's frontier strength — and the packet gives zero anchors and no statement of *what
is a real deterrent vs. a detected-but-unpunished event*.

**Close before Fable:** add a "Slashing / enforcement" board block distinguishing the built
invalid-state tier from the unbuilt equivocation tier and the un-enforced VaR cap, and add an
open-question: *"is the profit-from-a-caught-fraud bounded below the slashable stake at the current
`slashFraction`, with no VaR cap and no staging gate?"*

### A6 — No Definition-of-Done for sharding; the sole e2e gate hides a known blocker `[HIGH]`

There is no testable "done" bar. The board (`HANDOFF §4`) tracks *landed/pending* per slice but
never states what **"production-ready execution sharding" passes as an acceptance test**. The
acceptance criteria are scattered and un-consolidated: `plan §4` ("happy-path spendability in
minutes; pre-finality dispute = clean reject; cross-shard consume forcing test"), `ECONOMIC-TRUST
§9 W4a` ("forge a root → caught+slashed; double-consume → rejected; majority-less snapshot →
rejected"). `grep -rIln "definition of done\|acceptance criteria" docs/` finds no DoD doc.

The one concrete gate — "**E2E cluster sanity pass — NOT RUN**" (`HANDOFF §4`) — does not disclose
that a **known, root-caused-but-unfixed blocker** may prevent it: the untracked
`docs/nakamoto/SESSION-HANDOFF-2026-06-29.md` records **#186 token-lock-replacement ref-vs-set
desync "blocks the FULL e2e under `--stake-dist=uniform`."** A reviewer (or the next agent) reading
"NOT RUN" will assume "just run it"; the packet omits that it may not pass.

**Close before Fable (or hand to Opus):** write a one-screen DoD checklist — the union of the three
scattered gates above, each as a named test/e2e scenario with pass/fail — and add the #186 caveat
to the e2e board row. This is the artifact that lets a *Sonnet* agent later confirm "sharding is
done" without judgement calls (Section C).

### A7 — Invariant → test coverage matrix is missing; gated flags lack written flip-criteria `[MEDIUM]`

The design corpus defines many invariants — `ECONOMIC-TRUST §2.3` (I-COMMIT, I-BASE, I-PIN,
I-AUTH, I-CORRECT, I-ONCE, I-REG), `COMMITTEE-STATE-DIFF-ADOPTION-DESIGN.md §6` (I1–I5),
`plan §2` (three determinism invariants). The packet cites suites for *some* Track-1/3 items
(`CurrencySnapshotProcessedSetSuite`, `PinnedCurrencyInfoReader`, `MultiBranchAdoptSuite`,
`ShardCommitteeReExecutionSuite`) but **never states which invariants have no test.** From
Sections A2/A3/A5 the uncovered-and-load-bearing set includes at least: **I-AUTH** (unbuilt →
untestable), **watchtower coverage / no-show → escalate** (unbuilt), **VaR cap** (unbuilt),
**equivocation-slash consequence** (shelf-ware). "An invariant with no test" is one of the things
the task brief asks to surface — here the *most* safety-relevant ones are exactly the untested ones.

Separately, the `band-density-reorg-enabled = false` flag (`application.conf:391`) is gated on "a
§5.6 deep-fork **cluster-uniformity sim**" (`HANDOFF §4`), but **no sim spec is written down** — no
doc defines the scenario, pass threshold, or where to run it (`grep "cluster-uniformity" docs/`
only hits the packet and the audit referencing it, never a spec). A gate defaulted off with an
unwritten flip-criterion is exactly the anti-pattern `FABLE-REVIEW-PROMPT §5` says to hunt.

**Close before Fable:** add a small invariant→(test | UNBUILT | UNTESTED) table to the packet, and
either write the deep-fork sim spec or mark the flip-criterion "SPEC NEEDED" so it is not mistaken
for "just run the sim."

### A8 — Citation-integrity friction in the packet's own cross-refs `[LOW]`

`FABLE-REVIEW-PROMPT.md` cites "HANDOFF §5.6" (line 64) and "§5.7" (lines 27, 65), but `HANDOFF §5`
is a **flat numbered list 1–7** with no subsections — a reviewer will spend a beat mapping "§5.6" →
"item 6." Minor, but the packet's credibility rests on precise citation (per its own §3 lesson);
normalize these to "§5 item 6/7." Same for "HANDOFF §5.7" appearing as both a fault-model cite and
an open-question cite.

---

## SECTION B — SCOPE BOUNDARY SHARPNESS

The packet *tries* to draw the boundary (`HANDOFF §6` "already verified — do NOT re-derive"; §5
"open frontier"; `FABLE-REVIEW-PROMPT §3` high vs low leverage). That structure is good. Where it
will still **leak budget**:

1. **The "settled — do not re-litigate" list mixes two kinds of settled.** `HANDOFF §6` lumps
   *user product decisions* (3-path reader, role-split, `k₂ = 100·k₁`) with *adversarial
   conclusions* (1a scalar refuted, k2-freeze inert). A frontier reviewer respects the second kind
   (they're provable) but may reasonably want to *challenge* the first (e.g. "is `k-draw=8/k-quorum=6`
   actually safe at the collapse bound?" is both a settled user decision *and* the §5.7 open
   question). The boundary between "settled by fiat" and "settled by proof" is blurred — Fable
   won't know which it's allowed to reopen. **Sharpen:** split §6 into "settled by proof (do not
   re-derive)" vs "settled by product choice (may pressure-test the *consequences*, not the choice)."

2. **"Out of scope" is never stated positively.** The packet says what's open (§5) and what's
   settled (§6) but never lists what is **deliberately excluded from this review** — e.g. Track-2
   spendability (A2), BLS aggregation (`BLS-AGGREGATE-SIGNATURE-DESIGN.md`, gated on BC 1.85),
   roots-only hard fork (`W4c`), the KES/era machinery. Without an explicit exclusion list Fable may
   wander into `docs/nakamoto/`'s 55 files (many with "CORRECTION (2026-06-30)" banners signalling
   churn) and spend a session on a subsystem no one wanted reviewed.

3. **The low-leverage list is advisory, not enforced.** `FABLE-REVIEW-PROMPT §3` says "if you're
   deep in [refactors] while §5 items are open, stop and reprioritize" — good instinct, but there is
   no *budget cap per phase*. Fable's scarcity is the whole premise; the packet should state an
   explicit budget allocation (e.g. "Phase 0 ≤ X, spend the rest on §5.1/§5.2/§5.4") so the model
   self-limits Phase-0 mapping (the most tempting time-sink, and the one A1 makes worse).

4. **Where Fable will waste budget, concretely:** (a) reconstructing the data path in Phase 0
   because A1; (b) re-deriving the watchtower/slashing model from scratch because A4/A5 give no
   anchors; (c) potentially "proving" §5.1 safe on the false I-AUTH premise (A3) — the most
   expensive waste of all, because it produces a *confident wrong answer*.

**Net:** the boundary is ~70% crisp. Close A1–A5 and split §6, and it becomes crisp enough that
Fable spends its budget on the four genuinely-frontier items (§5.1 adopt≡re-exec, §5.2 GAP-1, §5.3
densityCompare commutativity, §5.4 Taktikos grinding).

---

## SECTION C — FALLBACK SCAFFOLDING (Fable → Opus → Sonnet tiering)

**Premise:** Fable produces *findings and novel arguments*; it does not merge. When Fable access
ends, Opus and Sonnet must carry each finding to a landed, tested fix **cold** — so the handoff
artifact between tiers is as important as the tier assignment.

### C.1 Work-tiering table

| Activity | Tier | Why this tier |
|---|---|---|
| Break/prove adopt ≡ re-exec under Byzantine ml0 (+ whether I-AUTH is load-bearing) — §5.1 | **Fable** | Novel safety argument over an adversary that controls source *and* committee; the packet's #1 question |
| GAP-1 field-presence soundness — is `stateProof.<field>Proof.isDefined` a true `None`/`Some(empty)` separator on *every* field? §5.2 | **Fable** | Per-field adversarial case analysis; a single counterexample flips a Critical |
| `densityCompare` commutativity — construct an asymmetric MRCA/tiebreak input, §5.3 | **Fable** | Break-a-proof-by-construction; small, deep, high-value |
| Taktikos pseudo-predictability / selfish-mining / long-range under LDD `fB=0.05`, §5.4 | **Fable** | Requires the Taktikos (not Praos) bound intuition; genuine research reasoning |
| Economic attacks: profit-from-caught-fraud vs stake, grief-slash, front-run-spendability (needs A2/A5 context) | **Fable** | Game-theoretic; the model's soundness rests on VaR ≤ stake × slashFraction, currently un-enforced |
| Build the current end-to-end cross-shard trace (`00-project-map.md`) | **Opus** | Multi-file synthesis across 3 modules; no novel reasoning, but needs judgement to be *correct* |
| Adversarially verify ONE specific Fable finding against source before it's actioned | **Opus** | The §3 "deleted X" class of error — a relayed claim must be re-grounded in code, not trusted |
| Wire the invariant→test coverage matrix (A7) + the DoD checklist (A6) | **Opus** | Reads across the corpus + code; needs to decide what "covered" means |
| Extend `HANDOFF §7` with the committee/watchtower/slashing anchors (A4/A5) | **Opus** | Locate + verify each anchor lives where claimed |
| Implement a `Status: Proposed` fix once the direction is settled | **Sonnet** | Mechanical once the finding names the file, the invariant, and the fix shape |
| Write Weaver + ScalaCheck generators from a stated test strategy (`03-test-strategy.md`) | **Sonnet** | Scaffolding from a spec; no design decisions |
| Re-run a Phase-2 audit on ONE module after a fix lands; diff findings | **Sonnet** | Bounded, repeatable, single-module scope |
| Re-confirm a determinism invariant (e.g. `376d09fbc` smtRoot-blind still holds on HEAD) | **Sonnet** | Grep + read + assert; the packet already names it a Track-3 precondition |

**Rule of thumb:** if the task's output is *a new argument*, Fable. If it's *synthesis or
verification of an existing claim across files*, Opus. If it's *execution of a decided change*,
Sonnet. Never send a safety/liveness **decision** to Sonnet, and never spend Fable on synthesis
Opus can do.

### C.2 The handoff artifact (what a Fable finding must contain for Opus/Sonnet to act cold)

The packet already prescribes the Phase-2 `FINDING` record (`FABLE-REVIEW-PROMPT §4`). Extend it
with the fields an *acting agent* needs but a *finding author* often omits — this is the contract:

```
### FINDING-[NNN]
- Severity / Category / Confidence          # as today
- Invariant-at-risk: <I-name or "adopt≡re-exec"/"densityCompare-commutativity">
- Location(s): path:line  (VERIFIED @ HEAD <sha> on <date>)   # not "around line X"
- Reproduction: exact test/grep/command that exhibits it, OR "argument-only (no repro)"
- Attack/failure scenario: <precondition → step → observable divergence>
- Proposed direction: <file(s) to change> + <the invariant the change must restore>
- Blast radius: <which of numShards=1 regression / honest path / adversarial path>
- Status: Open | Proposed(<owner-tier>) | Verified | Landed
- Verifier note: <Opus fills this — "re-grounded in source: <what was confirmed/refuted>">
```

The two load-bearing additions are **`Location … VERIFIED @ HEAD sha`** (kills the stale-line-number
class — see §3 and Section D) and **`Verifier note`** (forces the Opus adversarial-verification hop
to be *recorded*, so a Sonnet implementer sees "confirmed in source," not a relayed claim). A
finding without a `Reproduction` line or an explicit "argument-only" is not actionable cold —
bounce it back.

### C.3 Cheap phase re-run after a fix lands

The phase files (`00`–`03`) are the durable deliverable, so re-running a phase must be a *diff*, not
a redo:

1. **Per-finding gate, not per-phase rerun.** Each `FINDING` names its `Invariant-at-risk` and a
   `Reproduction`. After a fix, a **Sonnet** agent runs only that finding's reproduction + the named
   suite (`just test --test=<group>`, per the repo's targeted-test rule) and flips `Status → Landed`
   with the passing evidence appended — no full Phase-2 re-audit.
2. **Module-scoped Phase-2 replay.** If a fix touches a whole file (e.g. `ChainSelection.scala`), a
   **Sonnet** agent re-runs Phase-2 *for that one module* against the new HEAD and diffs its finding
   set vs the prior `02-findings.md` — new/resolved/unchanged. Bounded and repeatable.
3. **Precondition re-confirm as a standing check.** The Track-3 preconditions (`376d09fbc` smtRoot
   determinism holds; disk-backed deep-revert determinism) are named in
   `TRACK1-TRACK3-IMPLEMENTATION-SPEC.md §7` — encode each as a one-line Sonnet "grep+assert @ HEAD"
   so any branch rebase cheaply re-verifies them before enabling deep reorgs.
4. **Board is the join point.** Every tier writes back to `HANDOFF §4/§5` (the packet already
   mandates append-only, `FABLE-REVIEW-PROMPT §4`). That is what lets Fable → Opus → Sonnet hand off
   without a live conversation: the board + the FINDING record + the Verifier note are the whole
   protocol.

---

## SECTION D — PROCESS RISKS

The concrete instance the team cited — a summary claiming two functions were "deleted" when only
their **override-layering** was removed (both `GlobalSnapshotAcceptanceManager.deriveAdoptedCurrencyState`
and `ShardCheckpointWiring.reExecDerivationWithDiff` still exist; `HANDOFF §3`) — is not a one-off.
It is one visible symptom of three systemic causes, all in evidence in this very corpus:

**Cause 1 — Memory/doc staleness read as ground truth.** `MEMORY.md` is *over its size limit and
only partially loaded* (the system-reminder warns it truncates), and nearly every design doc carries
a "CORRECTION (2026-06-30)" banner overwriting a prior "MISSING/stub/dark" status
(`ECONOMIC-TRUST`, `EXECUTION-SHARDING-COMMITTEE-VERIFY`, `SHARD-SORTITION-WORKSTREAM-PLAN`,
`PRODUCTION-READINESS-AUDIT`, `HIERARCHICAL-SHARD-CHECKPOINTS`, `COMMITTEE-STATE-DIFF-ADOPTION`).
An agent that cites the *body* of any of these without reading past the banner gets the wrong status
— exactly how `SHARD-CHECKPOINT-FLOW-WALKTHROUGH.md` (A1) still describes the pre-role-split world.

**Cause 2 — "deleted X" / "built X" as symbol-vs-behavior conflation.** `HANDOFF §3` diagnoses this
precisely for "deleted." The dual failure is "**built** X": the corpus says the watchtower/slash/
committee-VRF stack is "BUILT" (true — files exist), which is easy to over-read as "the guarantee is
complete," while I-AUTH, the spendability gate, the VaR cap, and equivocation-slash consequence are
*not* built (A2/A3/A5). "Built the mechanism" ≠ "the invariant holds end-to-end."

**Cause 3 — Relayed agent claims trusted without re-grounding in source.** The packet's own §6 says
conclusions came from "3 adversarial Workflows + 1 gate-builder" and instructs "do NOT re-run them."
That is correct for *budget*, but it means downstream agents inherit those conclusions **without a
source re-check** — the same mechanism that let "deleted both overrides" propagate until someone
finally read `:1006`/`:210`.

### Guardrails (concrete, adoptable this session)

1. **"Deleted/built" is a banned bare verb in handoffs.** Every such claim must read "deleted/built
   *behavior* X — *symbol* Y at `path:line` still exists/does not" — i.e. name the symbol *and* its
   line *and* whether it survives. `HANDOFF §3` did this after the fact; make it a pre-commit rule
   for any status line. (Cheapest, highest-yield guardrail — it directly kills the cited failure.)

2. **Every `file:line` in a handoff carries `@ HEAD <sha>`; a claim without it is "unverified."**
   Line numbers drift (`TRACK1-TRACK3-SPEC §3` already notes "+2 drift", "~+144 in SnapshotLeaderLoop").
   A citation that names the sha it was verified at is falsifiable in one `git show`; one that doesn't
   is a rumor. The FINDING template (C.2) enforces this downstream.

3. **Status doc must lead with a machine-checkable "as of" line, and stale docs get a pointer, not a
   rewrite.** Instead of another buried "CORRECTION" banner, a superseded doc gets a top-line
   `SUPERSEDED-FOR <topic> BY <doc> @ <date>` so an agent knows to stop reading before absorbing the
   stale body. Apply immediately to `SHARD-CHECKPOINT-FLOW-WALKTHROUGH.md`.

4. **Every default-off gate names its flip-criterion as a *written, testable* artifact.** The
   `band-density-reorg-enabled` flag is gated on an unwritten sim (A7). Rule: a flag may not be
   committed default-off without a `docs/…` line stating the exact scenario + pass threshold that
   flips it. "Needs a sim" is not a flip-criterion; a sim spec is.

5. **Relayed conclusions are re-grounded once at the point of *action*, and the re-grounding is
   recorded.** Do not re-run the whole workflow (budget), but before any agent *acts* on a relayed
   finding, an Opus verifier confirms the one load-bearing `file:line` still says what the finding
   claims and writes the `Verifier note` (C.2). The §3 error survived precisely because no one did
   this hop before the summary propagated.

**Which decisions still need a human or a deep-reasoning pass** (per the task's ask): (i) whether
Track-2 spendability staging is *in scope for production sharding at all*, or the honest cross-shard
path ships at full-k₁ latency for v1 (A2) — a product call; (ii) whether I-AUTH is a *precondition*
for the Track-1 adopt≡re-exec claim (A3) — hand to Fable to rule on, then a human to schedule;
(iii) the VaR-cap / `kApprove` sizing (A5) — economic analysis, Fable-then-human.

---

## Appendix — grep/verify commands run for this critique (2026-07-07, HEAD `21933559c`)

- `grep -rIn "kApprove\|approvalCount\|spendabilityGate\|stageCheckpoint" modules/*/src` → **0 hits**
  (Track-2 spendability gate unbuilt — A2).
- `grep -rIn "operatorMajority\|metagraphOperatorThreshold" modules/*/src/main` → **0**;
  `validateStateChannelAllowanceList` only at `StateChannelValidator.scala:167,182` (I-AUTH unbuilt — A3).
- `grep -rIn "valueAtRisk\|VaR cap" modules/*/src/main` → **0**; only `slashFraction` knob exists
  (`config/types.scala:248,267`) (VaR cap un-enforced — A5).
- Watchtower/slash BUILT: `WatchtowerFraudProofEmitter`, `WatchtowerFraudProofPool`,
  `InvalidStateProofSlashManager.scala`, `Slashings` fieldId 34 wiring at
  `GlobalSnapshotAcceptanceManager.scala:319,457-460,934` — present, but **absent from `HANDOFF §7`** (A4/A5).
- `grep -i "I-AUTH\|source authenticity\|track-2\|kApprove\|watchtower approval" docs/review/*` → **0**
  (packet omits Track-2 and I-AUTH — A2/A3).
- `grep -rIln "definition of done\|acceptance criteria" docs/` → no DoD doc (A6).
- `SHARD-CHECKPOINT-FLOW-WALKTHROUGH.md §0` describes the discarded-`perMetagraphMptRoots` /
  legacy-`processStateChannelEvents` path = pre-`dc0dbe1c7` architecture (stale — A1).
- Untracked `docs/nakamoto/SESSION-HANDOFF-2026-06-29.md` records #186 blocks the full e2e under
  `--stake-dist=uniform` (undisclosed e2e blocker — A6).
