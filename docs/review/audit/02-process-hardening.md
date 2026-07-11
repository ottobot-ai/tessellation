# 02 — SDLC Critique & Process Hardening

**Branch** `feature/committee-state-diff` · **HEAD** `21933559c` · **Date** 2026-07-07
**Basis.** Transcript forensics across 8 development sessions (2026-06-02 → 2026-07-07), the embedded
incident catalog, the commit history, and the memory record. 28 process findings (`PROC-01…28`), each
transcript- or commit-anchored. This is the brutally-honest pass the stakeholder asked for: the
engineering is strong and the bugs are hard, but a **repeating set of process failures** cost days of
firefighting and shipped at least one inert consensus mechanism.

**The one-sentence verdict.** The dominant failure is not bad code — it is **declaring victory before
the evidence supports it** (premature-validation, 9 of 28 findings, highest damage), compounded by
**stale constants propagating through docs, scaladoc, the skill, and agent memory** (doc-drift, 6
findings) and **trusting relayed sub-agent claims without re-grounding in source** (3 findings, one of
which aimed the #1 safety question at a file that exists).

---

## 1. The failure classes (ranked by frequency × damage)

### Class 1 — Premature validation (9 findings; the expensive one)
"Validated / fixed / fork-safe" declared before the test contained the phenomenon the claim is about.
The signature sub-modes, each with a real instance:

- **Wrong-scenario validation.** "Self-heal VALIDATED with equal stake" told to the user and written to
  memory (06-27 01:30) — refuted 2h later by a 13-agent audit: the test was a laggard catching up, not
  two tines reorging; the validation scenario "never contained the phenomenon (a fork) the claim was
  about" (PROC-11).
- **Commit-before-the-decisive-test.** The k₂-freeze mechanism (`86f390130`) was committed *before* it
  ever fired; the validation run never reached k₂=3200 so `archive()` armed **zero times** on all 6
  nodes, and "0 REFUSED" (a never-armed gate's silence) was read as success. On-branch 3 days, then
  reverted (PROC-12).
- **Absence-of-signal as proof; oscillating verdicts.** k₂-freeze was "exonerated" 43 minutes before
  being declared inert+self-contradicting — two opposite confident verdicts to the user inside an hour
  (PROC-14).
- **Unvalidated instruments.** The fork-safety monitor keyed on a snapshot field that is `null` (the
  API has no top-level `.hash`), so it would have reported "converged" regardless; caught only by luck
  (PROC-16). Every "all-converged" claim before that discovery rests on an instrument later shown
  capable of false-convergence.
- **Contaminated test bed.** The original "forked as fuck" alarm was manufactured by the operator's own
  node reset/rejoin into a perturbed cluster; a separate "restarted all 6 gl0 simultaneously → lost
  quorum" was self-inflicted. Perturbations weren't logged or segregated from observation windows, so
  fix-vs-churn attribution needed a forensic audit (PROC-17).
- **Systemic tell:** the project's own embedded incident catalog marks **7 of 8** incidents
  `recurredLater=true`, with one bug "mis-diagnosed ~6 times" (PROC-20).

### Class 2 — Doc / constant drift (6 findings; long half-life)
A consensus constant is retuned in code and never swept through the docs that reason from it.

- **k₁ 255 → 1024** (retuned 06-05) is still `255` in **≥8 design docs** at HEAD; caught only at
  review-packet time, 32 days later (PROC-01). "k₁ is the master knob every derived depth scales
  from — a reviewer who reasons from 255 mis-sizes all of them."
- **γ 15 → 16 / ψ 0 → 1**: existed in three places (HOCON, source `Default`, a hardcoded sim constant);
  **every reorg-risk sim was run at γ=15** and never re-run after the curve changed — the packet had to
  annotate its own risk numbers "γ=15, directionally valid, not exact for the shipped curve" (PROC-02).
- **Born-stale docs and missed purges:** an audit doc first committed 5 days *after* the k₂ retune
  still cited the old `k₂=10·k₁`; a dedicated "purge stale k₂" commit missed it (PROC-03).
- **Agent memory as a drift vector:** MEMORY.md still said "k₁ prod 255" and "cutoff NOW 30" after both
  were changed — the review session had to fix drift its own memory propagated (PROC-05).

### Class 3 — Relayed claims trusted without re-grounding (3 findings + a systemic cause)
- **The costliest:** the review packet shipped "No `ShardCheckpointAttestationEmitter` source file was
  found … the single most important thing for Fable to pin down" — **the file exists**, the parent had
  already listed it, quoted it with line numbers, and edited its comment. A sub-agent's false absence
  claim aimed the #1 safety question at a non-problem (PROC-08). *(This audit re-verified the file first
  thing and the conclusion held — but the process shipped a self-contradiction.)*
- A relayed "BouncyCastle has zero BLS support" nearly flipped the BLS architecture decision; corrected
  only by user pushback + a firsthand check (PROC-09).
- **Institutionalized:** the packet's own §6 says the conclusions came from prior workflows and "do NOT
  re-run them" — correct for budget, but it means downstream agents inherit conclusions without a source
  re-check (PROC-10). Absence claims from isolated-worktree sub-agents are structurally unreliable
  (a sub-agent honestly reporting "the doc does not exist in this worktree" is the same failure shape).

### Classes 4–8 (lower frequency, still real)
- **Same-bug-refixed / scope-churn:** the authoritative-push structural cause ("gl0 can't re-derive
  cumulative per-MG state") was fixed **field-by-field across 8 commits over 10 days**; the exhaustive
  field inventory happened at the 7th (PROC-18). A named-late defect class (None-vs-`Some(empty)`) was
  re-encountered ≥3 weeks apart before being catalogued (PROC-19).
- **Regression-via-revert / lost work:** an isolation revert put an observation-dependent field back
  into the consensus root on a false suspicion (PROC-21); a validated 150-LOC fix was **unreachable from
  any ref for 18 days** while its bug class stormed (PROC-22); BLS S1–S5 lives only on an unmerged
  branch (0 symbols on current) yet was tracked as progress.
- **Lose-the-thread / rule-not-consulted:** the role-split was re-litigated *twice* ("how is this news
  to you, weeks"); VRF-tiebreaker removal was proposed against a 53-day-old indexed memory rule that
  said don't; both caught by **user** pushback (PROC-25).
- **Gate-without-criterion / risk-mislabeled:** two default-off safety flips are gated on **one
  unwritten sim** with no pinned pass-condition (PROC-23); the Taktikos delay-cliff flipped from
  "high-priority design gap" to "accepted operational property" in **7 minutes with no decision record**
  (PROC-24) — the same framing conflict the stakeholder resolved with me this session.
- **Symbol-vs-behavior conflation:** "deleted both overrides" propagated through a task summary, a
  commit headline, and a compaction summary until someone finally read the file — **both functions
  still exist**; only the override *layering* was removed (PROC-07). Compaction summaries are the
  highest-risk carrier because they harden imprecise language into "fact."

---

## 2. Guardrails (concrete, enforceable, mapped to the classes)

Ordered by leverage. Each is a mechanical check, not an exhortation.

1. **"Validated" requires a fired mechanism (Class 1).** A claim "X validated" must cite (a) the
   log/metric line proving X's code path **executed ≥1 time** in the test (for a store-gate: `archive()`
   fire-count > 0), and (b) that the test **contained the phenomenon** the claim quantifies over (a
   fork, not a laggard). Absence-of-signal ("0 REFUSED") is admissible only with proof the gate armed.
   Enforce as a commit-message DoD: any consensus `fix`/`feat` names the observable that flipped.

2. **Instrument-first (Class 1).** Before any convergence/health claim, run the monitor against a
   KNOWN-divergent and a KNOWN-converged fixture (the `.hash`-null monitor fails this in 30s). Log every
   operator perturbation (restart/rejoin/hot-swap) with timestamps into the run record; observation
   windows overlapping a perturbation are **inadmissible** for fix-attribution.

3. **Constants live once; a change sweeps its blast radius (Class 2).** Any change to a consensus
   constant (k₁, γ, ψ, k₂ formula, quorum sizes) requires, in the same commit, a `grep -rn` for the OLD
   literal across `docs/ modules/ .claude/skills/` + the memory dir, with the hit-list pasted into the
   commit message (deferred items allowed, but visible). Adopt the `ldd.scala` precedent: delete
   source-level defaults, HOCON authoritative. Sims quoted in a safety argument record their exact param
   tuple; a param change stamps dependent results "stale."

4. **Memory freshness is same-turn (Class 2).** A commit that changes a value present in MEMORY.md
   obliges a same-turn memory edit. (The 06-27 self-heal entry was corrected within 2h *because* it was
   used; the cutoff=30 entry shows what happens when it isn't.)

5. **Absence claims are quarantined (Class 3).** A sub-agent claim "X does not exist / was not found" is
   recorded as "X not found BY MY SEARCH FROM cwd=… via `<cmd>`" and the parent re-runs one `rg`/`find`
   from repo root before it enters any doc, summary, or decision. At doc-assembly time, grep the
   assembled docs for `not found|does not exist|absent` and re-check each hit. Capability claims
   ("library has zero support") get a firsthand check before relaying.

6. **Symbol-vs-behavior language precision (Class 8).** "deleted/built" is a banned bare verb in commit
   headlines, task summaries, and any compaction-bound text — require "deleted the override *lines* in
   `fn` (fn retained)" / "built the mechanism (invariant NOT yet enforced end-to-end)". Audit compaction
   summaries for these verbs on resume; a `git show` re-ground is the model.

7. **Name the class at the second occurrence (Class 4).** When a fix "extends" a prior fix, stop and
   enumerate the **full membership** of the bug class before the third fix ("which OTHER fields are
   non-re-derivable?" would have collapsed 8 commits into 2). A `recurredLater` flag already exists in
   the incident log — make a 2nd recurrence trigger a mandatory inventory task. *(This is baked into the
   roadmap: EPIC-1 and EPIC-4 each carry an "enumerate the full class first" sequencing note.)*

8. **Reverts carry a re-land ledger (Class 5).** Every isolation `revert` lists each reverted change
   with a disposition deadline; end-of-session handoffs diff "commits believed landed" vs `git log HEAD`
   (would have caught the 18-day-lost fix and the BLS-on-unmerged-branch immediately).

9. **No default-off flip without a written pass-condition (Class 7).** A config gate may not merge until
   its flip criterion is a **falsifiable sentence** in the decision log (DEC-005 shows the form; OPEN-002
   shows the current gap). Every gate and every isolation revert ships with a regression signal ("forks
   recurring at 16 ⇒ the smtRoot fix isn't holding" is the gold standard).

10. **"Accepted risk" is a decision, not a label (Class 7).** Writable only with a decision-log ID +
    date + owner; otherwise it stays an open risk. The delay-cliff must be either `DEC-nnn`'d or kept as
    an open gap — not both within 7 minutes. *(Now settled: the stakeholder confirmed delay-cliff is
    inherent to Nakamoto and k₁/k₂ handle finality within the synchrony assumption — record that as a
    decision, with the attestation-active-stake denominator, FINDING-004, as the honest residual.)*

11. **Pre-proposal memory/decision check (Class 6).** Before recommending removal/addition of any
    consensus mechanism, grep the memory index + decision log for the mechanism's name (the VRF rule was
    one grep away for 53 days). A checklist line in the design-doc template: "memory/decision-log hits
    for each mechanism touched: <list or 'none'>".

---

## 3. What this audit did differently (evidence the guardrails work)

This audit was run as **find → source-verify → promote**, with redundant agents on the load-bearing
questions, and it caught agent overstatements *before* they became findings — the mechanism the process
above lacked:

- The base-independence question was run by two independent agents; their disagreement (one said the
  watchtower verdict is uniformly pinned-safe; the other found the follower `createContext` rail reads
  live) was resolved by **reading `SharedServices.scala:353-390` directly** — yielding the corrected,
  verified FINDING-R01 instead of a confident-but-partial claim.
- A GAP-1 agent's `activeAllowSpends` drain vector was **blocked at source** (the consume overlay is
  read-only), narrowing FINDING-001 rather than overstating it.
- An agent cited a wrong file path for the base-independence anchor; the load-bearing asymmetry was
  re-located and verified before promotion.
- Every "absence" claim (revertToOrdinal-no-caller, smtRootBlind-untested, no-1-vs-K-test) was
  re-grounded with a fresh grep from repo root — directly applying guardrail #5 to avoid a repeat of
  PROC-08.

The cost was real (8 agents + orchestrator re-verification), but it is the right cost for code where "a
correctness bug means a chain split or fund loss." The recommendation is to make this loop —
**redundant find, adversarial verify, orchestrator re-ground before any claim is durable** — the
standard for consensus-path work, and to wire guardrails #1–#6 as commit/CI mechanics so the cheaper,
non-redundant path can't silently ship a premature "validated."

---

## 4. Top-3 process changes to adopt now
1. **Guardrail #1 (fired-mechanism DoD)** — kills the highest-damage class (premature-validation) at
   the commit boundary. Nothing merges as "validated" without the executed-path evidence.
2. **Guardrail #3 (constants-live-once + blast-radius sweep)** — kills the longest-half-life class
   (doc/constant drift), which silently mis-informs every downstream reviewer and sim.
3. **Guardrail #5 (absence-claim quarantine)** — the cheapest high-value check; a single re-grep from
   repo root would have prevented the packet's #1-safety-question self-contradiction.
