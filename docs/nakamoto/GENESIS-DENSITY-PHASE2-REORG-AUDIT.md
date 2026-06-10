# Genesis Density Rule & Phase-2 Reorgability — Audit + Design

**Date:** 2026-06-10. **Status:** AUDIT COMPLETE — design decisions below await owner sign-off before implementation.
**Owner-stated intended model (canonical going forward):** maxvalid-tk governs Phase 1→2 (k₁ operational finality via T_count/T_weight/T_depth1). A k₁-finalized snapshot **remains reorg-able under the Ouroboros-Genesis maxvalid-bg density rule** — the rarely-exercised deep-fork recovery — until it is k₂ deep (Phase 3 / SETTLED). **k₂ is the only absolute floor.**

## 1. As-built reality: k₁ is the absolute floor (the shortcut)

Four enforcement sites make k₁-finalized irrevocable today:

| Site | Evidence | Floor |
|---|---|---|
| Fork choice | `ChainSelection.shouldSwitch` — `candidateWins && !currentIsFinalized` vs `tipTracker.lastFinalized` (ChainSelection.scala:101-112; :69-73 "refuse reverting below a finalized head") | k₁ (first trigger) |
| Store gate | `NakamotoChainStore.store` refuses different-hash writes at-or-below `nakamotoFinalizedOrdinalRef` (NakamotoChainStore.scala:384-410; the a4ef837fb finality-safety invariant) | k₁ |
| Production | leader skips slot wins ≤ finalized (SnapshotLeaderLoop.scala:1534-1546) | k₁ |
| Recovery | `RebootstrapOrchestrator` = manual full-reset escape from the deadlock the missing rule creates (P-11 divergent-self-finalize) | — |

The **density comparator is fully implemented** (`densityCompare`, ChainSelection.scala:197-216, kLookback=50, sWindow=200) and correctly routed for forks deeper than kLookback (:114-131) — but `shouldSwitch`'s k₁ clamp makes it dead for any branch that would actually need it. The Genesis machinery exists; its jurisdiction was revoked.

**The P-11 deadlock is the scar tissue:** a node that depth-finalizes a branch the majority abandons refuses both chains forever; `RebootstrapOrchestrator` (default-OFF, full state wipe) is the bandaid. Under the intended model that node *recovers via density* — automatically, deterministically, no operator.

## 2. The conflicting record (as predicted)

1. **`attestation-and-finality.md` §0.4 codifies the shortcut as design**: "density rule operationally bounded to Phase 0/1… once Phase 2, no chain-selection rule can touch it." This sentence must be rewritten; it is the losing side of the prior discussion.
2. **Two competing k₂ notions coexist**: (a) `k₂ = 10·k₁` derived in NakamotoConfig (0940a10ea — "keep-depth-behind-finalized — phase-3 archive / tower checkpoint"; dev 320, prod 2550) and (b) `ArchivalDepthK` from `NAKAMOTO_ARCHIVAL_DEPTH` default **65536** read at runtime in SnapshotLeaderLoop:869 (also a sys.env rule violation, already in the audit). These must be unified: **one k₂, derived, HOCON.**
3. **No runtime phase state exists** — phases live in docs/comments only. The k₁ floor is `lastFinalizedOrdinal`; there is no k₂/Phase-3 marker wired to anything consensus-bearing (T_depth2's only sink is overlay pruning).
4. Audit-agent disagreements resolved: density **is** reachable code but **net-unreachable** past k₁ (the clamp wins); chain retention (`keepDepthBehindFinalized`) is **already k₂=10·k₁** post-0940a10ea (an earlier claim that it's k₁ was stale).

## 3. What Phase-2 revert needs that is missing (retention inventory)

Verified state-layer gaps (with corrected costs at k₂ = 10·k₁ = 2550 prod / 320 dev — NOT the legacy 65536):

| Component | Today | Needed | Cost (prod, k₂=2550) |
|---|---|---|---|
| Chain store `byHash` | keep-depth already = k₂ (0940a10ea) | ✓ nothing | — |
| MPT overlay **undo journal** | pruned at the k₁/operational tick (SnapshotLeaderLoop:1306-1316 `pruneBelow(operationalPruneQualifying)`) | prune at k₂ only (the :1333-1348 archival sink already exists — delete the operational prune or re-key it) | ~2550 × ~50KB ≈ **130MB** |
| TipTracker attestations | pruned immediately at finalize (:1038-1039, :1166-1167) | retain to k₂ (weight re-evaluation after revert) | ~MBs |
| Changeset/accumulator ring | 256 | ≥ k₂ if followers re-follow post-revert via deltas; OR rely on full-GSI resync fallback (exists) | 0 if resync-fallback accepted |
| Overlay pending branches | cleared at fold (Phase 1→2) | NOT needed — revert uses undo journal + re-fold, not retained branches | — |

**Verdict:** with the undo-journal prune re-keyed to k₂ and attestation retention extended, a deterministic Phase-2 revert (unfold via `applyUndoAt` ord-by-ord back to the fork point, then re-fold the denser branch) is mechanically executable. ~150MB steady-state memory cost at prod parameters. The 7GB figure circulated earlier was computed against the legacy 65536 and is wrong.

## 4. The design (proposed precise semantics)

- **Fork choice / adoption floor → k₂.** `shouldSwitch` refuses revert below the **k₂-settled** ordinal; in the (k₂-settled, k₁-finalized] band, ONLY the density rule may justify a switch (tk tiebreakers stay confined to sub-k₁; a Phase-2 switch demands the Genesis bar: denser-after-fork-point within sWindow). Store gate likewise re-keys to k₂.
- **Production floor stays at LOCAL k₁.** A recovering node must *accept* a denser foreign branch below its k₁; it never needs to *produce* there. Keeping production ≥ local-k₁ avoids re-opening the divergent-production churn the a4ef837fb invariant fixed.
- **k₁ remains "operational finality"** — what followers consume, what tests wait on, what the UX advertises (reorg probability past k₁ ≈ 10⁻¹¹/attempt per the sims). Phase-2 revert is the ε-event handler, not a normal path.
- **Followers: tolerate-revert, not k₂-gating.** ml0/cl1/dl1 already own the machinery for "my upstream moved": `FollowResyncNeeded` → `resyncToCanonical`, NotNext adopt-forward, full-GSI fallback. A Phase-2 gl0 reorg looks to a follower exactly like the resync cases shipped this month. Do NOT delay follower consumption to k₂ (would add minutes-hours of latency for a 10⁻¹¹ event).
- **Shard checkpoints: phases by embedding, not a parallel ladder.** A checkpoint's absolute finality is inherited from the gl0 ordinal that embeds it (its binaries' effects revert/replay with the gl0 branch — adoption is already a pure function of embedded bytes + prior, so replay-on-reorg is deterministic by construction). k1Shard stays the shard-layer operational marker. No k2Shard gate (an audit agent proposed deferring checkpoint adoption to shard-k₂; rejected — it would add the exact latency the cadence-inversion workstream is removing, for no safety the embedding doesn't already provide).
- **P-11 retirement path:** once density-past-k₁ lands, the divergent-self-finalize deadlock self-heals (the node's branch loses on density); `RebootstrapOrchestrator` demotes to a true last-resort manual tool.
- **Unify k₂**: single derived `k₂ = 10·k₁` in NakamotoConfig; delete the 65536 `NAKAMOTO_ARCHIVAL_DEPTH` runtime read; T_depth2/archival keys on it.

## 5. Slice plan (post-sign-off; sequenced after the e2e campaign)

1. **k₂ unification** (config; deletes a sys.env violation) + runtime k₂-settled marker (`markSettled` at the T_depth2 sink) + expose in finality-triggers route.
2. **Retention re-key**: undo-journal prune k₁→k₂; attestation prune k₁→k₂; bound checks + metrics for the new windows.
3. **Floor move**: `shouldSwitch`/store-gate re-key to k₂-settled, density-only adjudication in the band (tk never reverts past k₁); production floor unchanged at local k₁. Unit suite: synthetic deep-fork fixtures (denser branch wins in band; sparser refused; sub-k₂ refused absolutely).
4. **Revert executor**: ord-by-ord `applyUndoAt` unwind + re-fold + follower-notification (the existing finalized-ordinal event stream regressing is the signal followers already handle).
5. **P-11 demotion + docs**: rewrite attestation-and-finality.md §0.4 (Phase 2 = operationally final, density-revertable; Phase 3 = absolute), update TAKTIKOS notes, retire the Rebootstrap default-OFF caveat from the audit CRITICAL list.
6. **Adversarial sim**: extend the GPU fork sims with the band-reorg scenario to validate the density bar parameters (sWindow vs eta-period interplay; sWindow=200 < R must hold so density windows don't straddle eta rotations — check).

## 6. Owner decisions — ALL RESOLVED (2026-06-10)

1. **Follower surfaces stay k₁-keyed; EXPOSE `settled` (k₂) as an additional API field.** Followers consume operational finality + resync on the ε-event (the existing `resyncToCanonical`/NotNext machinery). The finality-triggers route + snapshot info surfaces gain a `settledOrdinal` (k₂) field so integrators (exchanges, light clients) can opt into the absolute marker. SNAPSHOT_FINALIZED events stay k₁.
2. **Production floor stays at local k₁.** A recovering node accepts the denser branch; it never produces below its own finality.
3. **k₂ = 10·k₁ is THE common-prefix parameter** (derived, HOCON; the legacy `NAKAMOTO_ARCHIVAL_DEPTH=65536` env read is deleted in slice 1). 10⁻¹² fork-race ≈ k 271–290 per the sims; 10·k₁ = 2550 prod is ~9× deeper.
4. **Eta on band-reorg: RECOMPUTE, by the bootstrap-equivalence principle.** A density-reorging node is a bootstrap peer joining from a deeper fork point — recovery reuses the SAME eta chain-walk a fresh peer runs (EtaStateManager), re-deriving boundary writes while traversing up the new branch. No parallel revert logic. The 2/3R staggering (eta(N) sources the first 2/3 of period N−1, hence ≥ R/3 ≈ k₁ deep at consumption) already guarantees agreement for ALL sub-k₁ operation — eta divergence is possible only inside the ε band-event, so freeze-at-k₂ (rejected) would tax every epoch's anchoring to defend a path the staggering already confines.

## 7. Derived chain-selection parameters (replaces the stale fixed values)

- **kLookback = k₁ + 1** (was fixed 50). The +1 is load-bearing: any fork that can contest a DEPTH-finalized snapshot (fork point ≥ k₁ deep) lands in density jurisdiction by construction; tk only ever adjudicates strictly-sub-k₁ forks where nothing depth-finalized is contested.
- **sWindow ≈ R/3 ≈ k₁** (was fixed 200; derive, don't hardcode). Keeps the density window inside one eta-stabilization span so the compared branches share a randomness lineage; the exact constant is validated by the sim slice (§5.6).
- Both derive in NakamotoConfig next to R and k₂ — k₁ remains the single free knob.

## 8. The optimistic-finalization interim lock (analyzed; acceptable)

Scenario: a node attestation-finalizes branch X at shallow depth d < k₁; the majority builds Y from a fork point in (d, k₁). The node refuses Y via the tk floor (its finalized marker) while density is not yet reachable (fork shallower than kLookback) — an INTERIM lock. It resolves by aging, with no deadlock, because: (1) the store gate (re-keyed to k₂) keeps STORING Y's branch as fork candidates — today's k₁ store-gate is what makes P-11 permanent, refusing to even retain the evidence; (2) chain retention reaches k₂ (0940a10ea); (3) when the fork point ages past kLookback = k₁+1, density jurisdiction activates and the node switches if Y is denser. Worst case ≈ k₁ ordinals (~30 min prod) of producing harmlessly-ignored snapshots on X. The chain-selection semantics preclude a soft lock; they impose only a bounded recovery delay on the ε path.
