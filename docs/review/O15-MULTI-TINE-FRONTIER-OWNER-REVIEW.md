# O-15 Multi-Tine Frontier Owner Review

**Status:** Owner review required. The objective result property is ratified;
the selector, declared-frontier protocol, divergent-tine distance metric,
evidence verifier, tie rule, resource-exhaustion rule, and proof are not ratified
or implemented.

**Runtime authority:** None. This packet does not authorize the current pairwise
fold, change GL0 fork choice, issue finality evidence, or make a snapshot Phase 2.

**Primary gates:** `FIN-D-001`, `FIN-D-001A`, `FIN-D-001B`,
`FIN-B-001..004`, `ECON-G-001..002`

## 1. Purpose

O-15 answers one narrow consensus question:

> When a GL0 node knows three or more fully valid competing tines, how does it
> choose one canonical head without letting candidate iteration order, gossip
> arrival order, restart order, or the node's previous incumbent choose the
> result?

The binary short-fork/deep-fork rule is already settled:

- use Taktikos `maxvalid-tk` within `k1`; and
- use the Ouroboros Genesis-family `maxvalid-bg` density comparison beyond
  `k1`, from the true most-recent common ancestor.

That rule tells us how the current implementation compares two tines. It does
not produce a total, order-independent choice over a frontier containing three
or more tines. The live pairwise relation can cycle.

O-15 is part of GL0's Nakamoto fork choice. It is not Avalanche/Snowball, a
committee protocol, ML0 BFT, or economic execution. It must not add a GL0 vote,
lock, quorum certificate, view change, or BFT commit.

## 2. Terminology

| Term | Meaning in this packet |
|---|---|
| **Tine** | One valid hash-linked GL0 chain ending at a candidate head. A tine includes the authenticated ancestry and active historical parameters needed to validate every compared header. |
| **Valid tine** | A tine whose snapshots passed complete GL0 authentication and deterministic state validation before entering fork choice. For an execution checkpoint this includes certificate verification, scoped diff application, and root reproduction; it does not mean every noncommittee GL0 node replays CL1. Fork choice never repairs or legitimizes an invalid snapshot. |
| **Head** | The newest snapshot on one tine. |
| **MRCA** | The true most-recent common ancestor of two tines. The short/deep decision and density window are measured from this common branch point, not from a truncated local walk. |
| **`k1`** | The Nakamoto confirmation-depth parameter and the boundary between short-fork Tk comparison and deep-fork Bg density comparison. It is also the depth fallback for Phase 2, but those are separate uses of the same parameter. |
| **`maxvalid-tk`** | The short-fork rule: prefer the longer valid tine; for equal length the current implementation prefers the lower head slot and then applies an implementation-specific tie rule. |
| **`maxvalid-bg`** | The deep-fork rule: compare the number of blocks in the configured density window after the true MRCA; the denser valid tine wins. |
| **Frontier** | The set of currently relevant valid competing tine heads plus the authenticated ancestry and parameter views needed to compare them. It is a set, not a list whose order has consensus meaning. |
| **Published frontier** | The exact declared manifest made available under the protocol's still-open slot/era boundary and diffusion assumption. It cannot include an adversary's unrevealed private tine. |
| **Cutoff-complete** | Complete only with respect to one exact declared manifest under a still-open authenticated slot/era boundary and explicit diffusion assumption, not proof that no private or selectively withheld tine exists. Local arrival time, wall clock, peer count, or a vote/quorum/QC cannot define this property. |
| **Selector** | A pure deterministic function from one verified frontier and its branch-authenticated parameters to one canonical head. |
| **Late reveal** | A valid tine disclosed after an earlier frontier cutoff. It extends a later frontier and triggers deterministic reselection; the previous result is not an immutable lock. |
| **Phase 2** | Reversible operational finality for one exact GL0 hash, reached by decided-attestation `T_weight` or canonical `k1` depth. A later valid density winner can replace it. |
| **`k2`** | Recommended local retention, proof-service, and automatic rollback capacity. It is not a fork-choice input, finality phase, or absolute reorg floor. |

## 3. Settled architecture

The following points are not reopened by O-15:

1. GL0 remains Nakamoto/Taktikos/LDD chain consensus. There is no global BFT
   proposal, vote, lock, QC, or view-change path.
2. Only fully authenticated and locally executed snapshots are candidates for
   fork choice or optimistic attestation.
3. The binary comparison boundary is Tk within `k1` and Bg beyond `k1`.
4. Once two nodes possess the same cutoff-complete published valid frontier and
   branch-authenticated parameters, they must select the same head independently
   of enumeration, arrival history, restart, or prior incumbent.
5. A late valid reveal extends the frontier and causes deterministic reselection.
   No earlier result creates a BFT-style lock.
6. Phase 2 remains density-reorgable. Avalanche/Snowball only supplies the
   optimistic exact-hash Phase-2 trigger; it does not choose the GL0 chain.
7. `k2` affects retained recovery capacity only. A node missing the true-MRCA
   history enters `RecoveryRequired`, authenticates and reconstructs the required
   history, and then applies ordinary objective fork choice. It does not choose a
   winner locally or refuse a winner merely because the fork is old.

These properties are recorded as L-02 through L-05 and L-24 in
`CONSENSUS-OWNER-DECISIONS.md:13-18,38` and as the GL0 lifecycle in
`CONSENSUS-ARTIFACT-LIFECYCLE.md:53-76,165-186`.

## 4. Why the current implementation is unsafe

### 4.1 The cited papers do not provide a total frontier order

Taktikos section 3, Algorithm 1 starts from the local chain `Cloc`, enumerates
candidate chains, and conditionally replaces that incumbent. Ouroboros Genesis
section 3.2.4, Figure 7 likewise starts from `Cloc`, enumerates candidates, and
applies its strict short/deep pairwise conditions. Neither cited algorithm defines
an incumbent-independent argmax over a set, specifies a consensus enumeration
order, or proves that the mixed relation is transitive or permutation-independent.

This is not a claim that either paper is BFT or that its two-chain rule is unused.
It means this project's stronger L-24 total-frontier requirement needs a new,
explicit construction and proof rather than an assumption that it was inherited.

Sources: Schutza et al., *Ouroboros Taktikos*, section 3/Algorithm 1
([DOI](https://doi.org/10.1007/978-981-99-8104-5_20), owner-supplied published
artifact `Taktikos - BlockSys_2023_paper_4526.pdf`, SHA-256
`63d030d7df3e908985340dc86636f00a2e79e66841e051b83c4516127be1d7cf`,
p. 6). The byte-distinct prepublication `fc-2023-v7.pdf` contains the same
algorithm in Appendix A.1, pp. 22-23, SHA-256
`9cdb2218db703195483a31ac4ff467b6a28cd76a6d64368e90b616c9c7ad1db6`.
Badertscher et al., *Ouroboros Genesis*, section 3.2.4/Figure 7
([accepted manuscript](https://www.pure.ed.ac.uk/ws/portalfiles/portal/76645278/Ouroboros_Genesis.pdf),
pp. 11-13).

### 4.2 The pairwise rule is not a frontier selector

`ChainSelection.selectBest` starts with the first list element and folds
`compare` across the remaining elements
(`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/ChainSelection.scala:119-129`).
The result is safe only if the pairwise preference relation supplies the required
associative/transitive behavior. It does not.

The retained regression constructs three structurally connected tines with
strict preferences:

```text
A beats B under Tk
B beats C under Bg density
C beats A under Bg density
```

Every deep edge is a strict density win, so this counterexample does not rely on
the current VRF/hash equality tie. Three permutations of the same frontier yield
three different winners
(`modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/ChainSelectionSuite.scala:191-226`).

Pairwise commutativity therefore is insufficient. `compare(A, B)` returning the
same value as `compare(B, A)` does not make a left fold permutation-independent.

### 4.3 The production store performs the same incumbent tournament

For an alternate branch, `NakamotoChainStore.store` compares the arriving tine to
the current best tip and immediately replaces or retains that incumbent
(`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoChainStore.scala:437-471`).

The store regression signs ordinal and parent linkage, inserts one frontier in
three parent-before-child schedules, and ends at three different best tips
(`modules/dag-l0/src/test/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoChainStoreSuite.scala:280-372,426-469`).
That test deliberately bypasses the complete snapshot validator and uses a
synthetic enabled `k`/`s` configuration. It proves the store control-flow defect,
but a validator-backed active-configuration witness is still required.

Concrete failure:

1. Honest nodes X, Y, and Z eventually store the same valid tines A, B, and C.
2. Gossip schedules present those tines in different orders.
3. Each node runs the incumbent tournament and ends at a different head.
4. Each head can feed different descendants, Phase-2 evidence, MPT branches, and
   downstream state.
5. Arrival order has become consensus authority, producing a persistent fork even
   though the nodes know the same frontier.

### 4.4 The `k1` equality boundary is wired inconsistently

The current MRCA walk reports the maximum post-MRCA suffix length and selects Bg
only when `forkDepth > kLookback`
(`ChainSelection.scala:161-175,187-240`). Production defines
`kLookback = k1 + 1`
(`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/types.scala:173-177`)
and wires that value into the selector
(`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala:1074-1088`).

Consequently, a fork with maximum post-MRCA suffix length `k1 + 1` still uses Tk,
although the locked prose says a fork beyond `k1` uses Bg. The retained witness
has a sparse longer tine that wins Tk and a shorter denser tine that wins Bg
(`ChainSelectionSuite.scala:228-250`). This is a real winner difference, not a
terminology-only mismatch.

### 4.5 Local finality/retention markers still bias live fork choice

`ChainSelection.shouldSwitch` currently has two flag-selected paths. With the
density-reorg flag off, it refuses a candidate when the current tip equals the
node's locally finalized hash. With the flag on, it refuses a switch whose MRCA is
below the locally settled ordinal and treats an unresolved MRCA as a refusal
(`ChainSelection.scala:131-159`). The repository default keeps the flag off
(`modules/node-shared/src/main/resources/application.conf:372-380`).

Neither path implements the target protocol. The first makes a local Phase-2
observation a write freeze; the second makes `k2` retention state a fork-choice
floor. Nodes with different retained history or local marker state can therefore
refuse different candidates. The final O-15 API must exclude both values from the
selector. Missing comparison history yields authenticated recovery, not a local
winner or refusal.

### 4.6 The current portable type is only an opaque placeholder

`ForkChoiceDecision` contains only one immutable artifact pointer and explicitly
leaves its evidence opaque until O-15 is closed
(`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/snapshot/finality/FinalityCore.scala:120-138`).
Its codec merely serializes that pointer
(`FinalityBaseCodecs.scala:96-119,178-179`). The validator checks pointer kind,
scope equality, selected refs, and lineage commitments; it does not decode or
verify a frontier, its cutoff, headers, ancestry, active parameter era, or the
selected result
(`FinalityIntentValidator.scala:59-120,748-761`).

The dark type is useful plumbing. It is not fork-choice evidence and cannot
authorize live `FinalityGate` transitions.

## 5. Exact decision under owner review

O-15 contains seven related decisions. They must land as one coherent protocol and
proof; selecting only the easy rows does not close the gate.

| ID | Owner decision | Recommendation for this review | Current status |
|---|---|---|---|
| O-15A | Observation boundary and late reveal | Define an authenticated slot/era-derived observation boundary under an explicit Taktikos diffusion assumption. Local arrival time, wall clock, peer count, first-N receipt, vote, quorum, certificate, or QC is not authority. Evidence binds one exact declared manifest; it cannot prove that every valid/private tine was disclosed. Every later valid reveal enters a subsequent frontier and causes deterministic reselection. | Fully open; no concrete boundary or evidence rule yet |
| O-15B | Multi-tine selector | Require one pure objective total-frontier function over a verified set. Reject incumbent folds, receiver-local iteration order, and hidden map/set order. Do not activate any concrete cycle rule until it has an independent reference model and security/liveness argument. | Property ratified; algorithm open |
| O-15C | Lineage classification, short/deep metric, and equality | Classify exact ancestry first: identical tines are equal and a strict descendant beats its strict prefix. For genuinely divergent tines, freeze what distance is compared to `k1` and the equality boundary. The cited algorithms measure a candidate relative to the current incumbent; the candidate symmetric `max(post-MRCA suffix lengths)` metric is a new project rule and its paper bounds do not automatically transfer. | Open research; no metric is ready for ratification |
| O-15D | Exact ties | Do not treat the current lower-VRF-then-hash rule as ratified. Retain it only as a RED/reference input until its grinding, equivocation, and precomputation consequences are quantified or a different objective rule is proposed. Incumbent or argument order is forbidden. | Open; no supportable exact choice in current evidence |
| O-15E | Portable evidence and verifier | Define a canonical, resource-bounded manifest for the declared frontier, complete compared ancestry, boundary context, exact immutable snapshot/body/state/era validation inputs, and selected result. The verifier independently reconstructs validity, MRCA facts, measurements used by the selector, and the final selection. Local validation receipts are cache hints only, never portable authority. A decoded pointer or peer assertion is never enough. | Recommended shape; exact schema/limits open |
| O-15F | Activation proof | Require the full validator-backed witness, order/restart/late-reveal convergence, partition/withholding simulations, chain-quality/common-prefix analysis, and corrected-store tests before this selector may mint canonical-selection authority. | Mandatory; open |
| O-15G | Byzantine frontier overflow | Define objective dominance, expiry, authenticated compaction, or bounded-memory streaming for arbitrarily many individually valid equivocations. No receiver-local eviction, first-N cap, hash truncation, or `k2` retention limit may change frontier membership or the selected result; fail-stop cannot be the normal overflow rule. | Open; no non-halting resource rule yet |

### 5.1 Lineage must be classified before divergent-tine distance

The comparison domain has three structurally different cases:

```text
same head                    -> equal
A is a strict prefix of B    -> B wins
B is a strict prefix of A    -> A wins
otherwise                    -> apply the ratified divergent-tine rule
```

The current code enforces the descendant rule only for an immediate child
(`ChainSelection.scala:161-175`). A multi-block descendant enters the generic
density path and can lose to its own ancestor on a density tie. A concrete RED
vector uses `kLookback = 2`, `sWindow = 2`, ancestor G at slot 0, and descendants
D1/D2/D3 at slots 10/11/12. The density window counts only G on each side; a lower
VRF/hash on G then selects G over D3 (`ChainSelection.scala:255-299`). The final
design must classify arbitrary-depth ancestry before applying a divergent-tine
rule.

For genuinely divergent tines, the candidate symmetric metric is:

```text
suffixA = number of blocks after MRCA on tine A
suffixB = number of blocks after MRCA on tine B
candidateForkDepth = max(suffixA, suffixB)
```

This candidate would make a symmetric total-frontier construction easier to
specify, but it is not the incumbent-relative distance defined by Taktikos
Algorithm 1 or Ouroboros Genesis Figure 7. Adopting it changes which rule fires
for some pairs and therefore requires a security/liveness argument for this
project's complete frontier construction. The existing `k1 + 1` witness proves
that production and the locked prose differ; it does not prove which replacement
metric inherits the papers' bounds. Missing ancestry produces `RecoveryRequired`,
never a truncated metric or local winner.

### 5.2 What O-15B still does not specify

The repository has no defensible formula for resolving the strict mixed Tk/Bg
cycle. The following implementation shortcuts are not acceptable substitutes:

| Shortcut | Consequence | Verdict |
|---|---|---|
| Current arrival-by-arrival incumbent tournament | Honest nodes with the same frontier can retain different heads. | Reject |
| Sort candidate hashes, then run the same non-transitive fold | Makes the arbitrary sort key decide cycles and may add a grinding surface; it does not establish the intended security or liveness properties. | Reject without a new proof |
| Use map/set iteration order | Runtime representation becomes consensus input. | Reject |
| Keep the prior incumbent on a cycle | Restart history and arrival order remain authority. | Reject |
| Fail-stop whenever a cycle appears | Prevents an immediate fork but lets an adversary halt canonical progress by publishing a cyclic frontier. | Reject as the default protocol |
| Add a GL0 vote/QC to choose among the tines | Reintroduces the global partially synchronous BFT architecture explicitly forbidden by L-02. | Forbidden |
| Define a new objective total-frontier construction and prove it | Can satisfy L-24 if it preserves the Tk/Bg security assumptions and converges under the declared frontier protocol. | Required design class; exact construction still open |

The owner should not be asked to approve a named selector until its complete
formula, adversary model, and reference traces exist. The present recommendation
is therefore to keep O-15B open and stop live finality authorization at this gate.

## 6. Invariants the final design must enforce

| Invariant | Required enforcement point |
|---|---|
| **VALID-ONLY**: invalid snapshots never enter the frontier. | Complete GL0 snapshot validator before chain-store frontier admission. |
| **SET-NOT-LIST**: candidate order has no semantic meaning. | Frontier type and selector API accept a canonical verified set/manifest, not a caller-ordered list. |
| **OBJECTIVE-RESULT**: identical verified frontier and parameters produce one identical head. | Pure selector plus independent verifier; property tests cover every permutation and prior incumbent. |
| **AUTHENTICATED-HISTORY**: every MRCA, slot, ordinal, VRF/KES identity, eta, and active parameter is branch-authenticated. | Frontier evidence verifier; receiver-current registry/config and sender metadata are not authority. |
| **EXTENSION-MONOTONICITY**: a strict valid descendant cannot lose to its own strict prefix. | Lineage classifier before any genuinely divergent Tk/Bg measurement. |
| **EXACT-BOUNDARY**: every implementation uses the same post-MRCA metric and `<= k1`/`> k1` split. | Configuration derivation, reference model, evidence verifier, live selector, and fixed boundary vectors. |
| **LATE-RESELECT**: a later valid reveal cannot be ignored because an earlier head was selected or reached Phase 2. | Frontier epoch transition and `FinalityGate` replacement path. |
| **NO-PRIVATE-COMPLETENESS-CLAIM**: evidence does not claim to prove absence of a withheld tine. | Evidence schema and security statement. |
| **RETENTION-NONAUTHORITY**: `k2`, pruning state, and local archive availability cannot make a branch win or lose. | Selector input type excludes retention metadata; unavailable history yields recovery. |
| **BOUNDED-WITHOUT-LOCAL-EVICTION**: Byzantine equivocation cannot make receiver-local capacity or arrival order decide membership or the winner. | Objective compaction/expiry or bounded-memory streaming rule plus adversarial overflow tests. |
| **PHASE-SEPARATION**: fork choice chooses the canonical tine; `T_weight OR k1` qualifies exact hashes on that tine. | Hash-bound `FinalityGate`; Avalanche evidence is not a chain score. |
| **ATOMIC-REPLACEMENT**: a selected density winner replaces branch, MPT, phase refs, checkpoint anchors, and downstream events as one recoverable operation. | Finality coordinator and durable reorg transaction after O-15 selection verifies. |

There is no current source line that enforces all of these invariants. O-15 is an
activation blocker precisely because `ChainSelection.selectBest` and
`NakamotoChainStore.store` violate `SET-NOT-LIST` and `OBJECTIVE-RESULT`.

## 7. Required code impact after ratification

No code change should begin with "make `selectBest` sort the list." The coherent
implementation surface is:

1. Replace the public pairwise-authority path with a frontier-owned API. Pairwise
   Tk/Bg measurements may be verified inputs to the new selector, but a fold of
   those facts cannot itself mint authority.
2. Change `NakamotoChainStore` from arrival-by-arrival canonical mutation to
   validated frontier admission followed by objective reselection. Alternate
   branches remain stored for late reveal and density recovery.
3. Add arbitrary-depth prefix classification, then freeze and correct the
   divergent-tine `k1` metric in configuration, runtime comparison, reference
   model, and evidence vectors together.
4. Replace opaque `ForkChoiceDecision` evidence with a canonical bounded schema
   only after O-15E is ratified. Keep `CanonicalBranchRevision` a local CAS guard,
   not portable proof.
5. Permit `FinalityGate` to consume only an independently verified fork-choice
   capability. The raw chain-store result cannot directly authorize P1/P2 or a
   downstream lease.
6. Remove the legacy `k1`/`k2` branch-rewrite floors from fork choice. Retention
   and missing-history handling stay in recovery/storage policy.
7. Integrate deep replacement with exact-MRCA unwind/refold and the durable
   finality transaction. No sink may observe a new canonical hash with old branch
   state.

O-15 does not change ML0 consensus, execution-shard checkpoint selection,
execution replay, watchtower rules, or economic transition functions.

## 8. Test and proof plan

The minimum close evidence is:

1. **Reference selector:** a small independent implementation consumes the
   canonical frontier manifest and produces the same result and trace as runtime.
2. **Permutation property:** every permutation, duplicate gossip delivery, prior
   incumbent, legal parent-before-child schedule, restart order, and map/set
   representation over one verified frontier selects the same head.
3. **Strict-cycle regression:** retain the existing A/B/C strict Tk/Bg cycle and
   upgrade it to complete snapshots passing GL0 signature, VRF, KES, eta,
   historical registry/stake, parameter-era, execution, and root validation.
4. **Corrected-store witness:** the three existing store schedules and randomized
   schedules converge after the same frontier is known.
5. **Boundary vectors:** test depths `k1 - 1`, `k1`, `k1 + 1`, unequal suffix
   lengths, ancestor/descendant, same-height siblings, and true-MRCA absence.
   Include the sparse three-block descendant RED above. Runtime, evidence
   verifier, and reference model must agree.
6. **Cutoff/late-reveal model:** exercise on-time publication, delayed honest
   gossip, withholding through one cutoff, later disclosure, partition/heal, and
   repeated reselection. Converged frontiers must converge without a lock.
7. **Tie/grinding model:** quantify the ratified tie rule under producer
   equivocation, eligible-slot precomputation, many privately produced candidates,
   and exact VRF collision fixtures.
8. **Overflow model:** one eligible producer emits unbounded distinct valid
   same-parent/same-slot candidates and many producers fill the frontier. Every
   node either streams or applies the same proved objective compaction/expiry;
   first-N, local eviction, hash truncation, `k2` caps, and adversarial fail-stop
   are rejected.
9. **Recovery vectors:** forks deeper than local `k2` cause authenticated recovery,
   complete objective comparison, and byte-identical reconstructed state. They do
   not cause local refusal or operator-selected realignment.
10. **Phase-2 replacement:** a P2 hash can be orphaned at the same ordinal or across
   an MRCA range; all state and downstream events unwind/refold atomically.
11. **Adversarial analysis:** document the chain-quality, common-prefix,
    withholding/grinding leverage, convergence, and liveness assumptions for the
    exact new selector. Bounds from Praos or a BFT protocol cannot be imported
    without showing that they transfer to Taktikos/LDD plus the selected
    Genesis-family rule.

These are the repository gates `FIN-D-001`, `FIN-D-001A`, `FIN-D-001B`,
`FIN-D-002..004`, `FIN-B-001..004`, `FIN-W-001..003`, and `FIN-S-001..003` in
`CONSENSUS-PROTOCOL-TEST-PLAN.md:169-185`. The existing component and synthetic
store tests are RED evidence, not closure
(`CONSENSUS-PROTOCOL-TEST-PLAN.md:195-226`).

## 9. Owner review checklist

No O-15 algorithmic decision is ready for ratification. L-24's objective-result
property remains ratified; the owner should review whether this packet frames the
open questions correctly before protocol research continues:

1. What authenticated slot/era boundary and exact Taktikos diffusion assumption
   define one declared frontier manifest without using local time, peer count, or
   global voting/quorum machinery?
2. What exact event moves a late valid reveal into the next selection frontier?
3. Does the owner confirm strict extension monotonicity for arbitrary-depth valid
   descendants before any divergent-tine metric is applied?
4. For genuinely divergent tines, should research retain an explicitly
   incumbent-relative construction, investigate the symmetric maximum-suffix
   candidate as a new project rule, or evaluate another precisely specified
   metric? No choice is recommended without its proof.
5. What objective total-frontier function resolves the proven strict Tk/Bg cycle?
6. What exact tie rule is acceptable after grinding and equivocation analysis?
7. What objective non-halting rule bounds an equivocator-created frontier without
   local eviction, first-N selection, hash truncation, or `k2` authority?
8. What canonical evidence manifest and resource limits let another node replay
   the whole decision from immutable authenticated inputs?
9. What security/liveness argument supports the selector under Taktikos/LDD's
   actual leader process and the project's Genesis-family density rule?

Until all nine are closed, the safe owner verdict is:

> Keep O-15 open. Preserve the objective total-frontier requirement and the RED
> witnesses. Do not let the current pairwise tournament, an arbitrary sorted fold,
> or an opaque `ForkChoiceDecision` authorize `FinalityGate` or Phase 2.

## 10. Relationship to other owner gates

- O-01 defines Avalanche/Snowball population and parameters after fork choice has
  selected an exact valid hash. It cannot resolve O-15.
- O-02 defines authenticated recovery when the true MRCA predates local retained
  state. Recovery obtains the inputs O-15 needs; it cannot choose the winner.
- O-16 defines how a downstream consumer holds and rechecks authority to one exact
  Phase-2 hash. It depends on O-15 producing verified canonical selection.
- O-17 defines structural validity for the released GL0 state image and partition
  grammar. O-07/ECON-G plus the complete GL0 validator prove economic transition
  semantics. O-15 can select only tines that passed both gates.

The dependency is therefore:

```text
O-17 structural validity + O-07/ECON-G semantic validity
  -> complete GL0 validation of exact candidate images
  -> valid GL0 tines
  -> O-15 objective fork choice
  -> exact-hash Phase-2 qualification (`T_weight OR k1`)
  -> release/read back the already validated exact O-17 image
  -> O-16 scoped downstream consumer lease
```
