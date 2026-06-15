# Sharded Currency Mirror — Data-Flow & Version Model

**Status:** MODEL (2026-06-13). The authoritative mental model for how a metagraph's currency state
mirrors into gl0 under sharding. Written because a string of "bugs" (run-24→27) turned out to be **one
disease**: different actors reading a *different version* of the same state, *S(N)*, at a *different time*.
Pairs with the apply-diff contract in [`COMMITTEE-STATE-DIFF-ADOPTION-DESIGN.md`](./COMMITTEE-STATE-DIFF-ADOPTION-DESIGN.md)
(invariants I1–I5). Read that for the trust model; read this for *who holds/reads/writes which version, when*.

> **The disease, in one sentence.** Every sharded-mirror failure this campaign has been an instance of:
> *actor A computed against version V₁ of an MG's state while actor B applied/verified against version V₂ ≠ V₁.*
> The fix is never "make B read X"; it is "anchor **every** reference of S(N) to the **same** version."

---

## 1. The three ordinal spaces (never conflate them)

| Space | What advances it | Who owns it | Typical magnitudes (e2e) |
|-------|------------------|-------------|--------------------------|
| **gl0 global ordinal** | gl0 Nakamoto consensus finalizing a global snapshot | gl0 cluster | hundreds |
| **metagraph currency ordinal** | the metagraph (ml0) producing a currency snapshot | one metagraph's ml0 cohort | hundreds, faster cadence (~43s) |
| **shard checkpoint ordinal** | the shard committee producing a checkpoint that folds a *window* of MG binaries | the per-shard committee (a gl0 subset) | small (a checkpoint covers MANY currency ordinals) |

A single shard checkpoint window can carry dozens of currency ordinals. "gl0 is at ord 462, the metagraph
at currency ord 250, shard checkpoint ord 3" is normal, not a bug.

---

## 2. Storage layers & the versions of S(N)

State lives in gl0's `MptOverlay` over an on-disk `MptStore`. The MG's `CurrencySnapshotInfo` is the unrolled
`Mg*` partitions (fieldId 25–32) + the fieldId-5 incremental + fieldId-7 activeAllowSpends.

| Version name | Reader | Definition | Deterministic across nodes? | Reorg-safe? |
|--------------|--------|------------|------------------------------|-------------|
| **finalized base** | `GlobalStateReader.fromMptStore(overlay.base)` | on-disk store; advances only on `finalizeBranch` (depth-k confirmation) | **yes** (finalized ⇒ shared) | **yes** (reorg drops branches, base untouched) |
| **branch / adopted** | `GlobalStateReader.dynamic(branchTipRef.get)` | `base + canonical-branch ChangeSet delta`; the state being extended at the *parent* global ordinal | yes, *given the canonical branch* (all nodes extend the same parent) | no — an in-memory branch can be dropped by reorg |
| **node-local pending / best-tip** | `GlobalStateReader.pending` | `base + this node's best-tip overlay` (un-adopted snapshots, local fold of MG binaries) | **NO** — node-local | no |

`bestTip = finalizedBase + branch delta` (MptOverlay doc). Ordering: `finalized base ≤ branch/adopted ≤ pending`.
The gap between *base* and *branch* is exactly the un-finalized window (depth-k of global ordinals).

**Shard-layer tip** (separate store, `ShardChainStore`): `bestTip` is the maxvalid-tk-selected shard checkpoint;
`perMgTip(mg) = hash(bestTip.window(mg).last)` — i.e. the window anchor tracks the **adopted/produced shard tip**,
which sits at the *branch/adopted* level, **ahead of the finalized base** by the finalization lag.

---

## 3. Actors — who executes what, and when

```
 ml0 (metagraph cohort)                shard committee (gl0 subset)            every gl0 node
 ─────────────────────                 ───────────────────────────            ──────────────
 produce currency snapshot   ──SC──►   buffer MG binaries                     finalize global snapshots
 expire locks @ raw synced    binary   produce checkpoint (leader, per slot): (Nakamoto consensus, depth-k)
   epoch; advertise           gossip     window = perMgTip → tip                 │
   globalSyncView (monotone)            re-exec MG accept() over PRIOR  ◄────────┤ reads PRIOR of S(N)
 send genesis-FULL ONCE                  ⇒ (per-MG root, byte-diff)              │
   (un-retried) ──────────────────────► committee attests (T_count_shard)      embed qualifying checkpoint
                                                                                 (window must extend gl0 SC tip)
                                                                                apply diff over PRIOR ⇒ verify root
                                                                                commit MG state to branch ⇒ finalize
```

- **ml0** owns metagraph consensus + produces the genesis-FULL. It is the ONLY durable holder of its early
  binaries until they propagate. A solo / under-replicated cohort (`candidates=0`) ⇒ single point of loss.
- **Shard committee producer** (leader gl0 node, per slot, off the accept path): re-execs each MG's `accept()`
  over a PRIOR of S(N) and emits `(perMetagraphMptRoots, perMetagraphStateDiff)`. **Reads a PRIOR of S(N).**
- **Every gl0 node** (acceptor): embeds a quorum'd checkpoint, **applies** the diff over a PRIOR of S(N),
  verifies the per-MG root === attested, commits, and later finalizes. **Reads a PRIOR of S(N) + the WINDOW anchor.**

---

## 4. The apply-diff version-alignment invariant (the crux)

For the committee→gl0 apply-and-verify to succeed, **three references to "the PRIOR S(N)" must be the SAME version**:

1. **Window anchor** — `perMgTip(mg)`: where the producer's window of binaries *starts*.
2. **Producer diff-prior** — the base the producer computes `diff = changeSet(prior, next)` against.
3. **gl0 apply-prior** — the base every gl0 node computes `reconstruct(prior, diff)` against.

`reconstruct(applyPrior, changeSet(diffPrior, next)) === next` **iff** `applyPrior === diffPrior` on *every* field
(even ones the diff doesn't touch — a minimal `upserts=1` diff still mismatches if the priors differ on an
untouched field; this is the diagnostic signature we keep seeing). And the diff itself is only meaningful if the
window's blocks chain *from* that same prior. ⇒ all three must anchor to one version.

**I3 chooses that version: the finalized base** — "present on every node (finalized ⇒ shared), never bestTip."
Rationale: only the finalized base is both deterministic *and* reorg-safe. A branch/bestTip prior can be dropped
by reorg, leaving a diff computed against a state that no longer exists.

**Consequence (the design's load-bearing requirement):** the window must *also* anchor at the finalized base —
i.e. cover `base → latest`, idempotently re-including any adopted-but-not-yet-finalized binaries each round, until
they fold into base. Progress is then gated on **finalization**, not mere adoption. This is the price of reorg-safety.

---

## 5. Known divergence points (every bug, mapped to the model)

| Run | Symptom | Which reference diverged | Status |
|-----|---------|--------------------------|--------|
| 24/26 | allow-spends/token-locks empty in gl0 forever | producer diff-prior = **empty** (window-only) vs gl0 committed full state | fixed by the apply-diff redesign (delete per-field gate) |
| 26→27 | ~890 ADOPT-VERIFY/node; all MGs frozen | producer diff-prior = **pending/best-tip** (node-local fold) vs gl0 apply = finalized | fixed: producer reader `pendingReader → fromMptStore` + `pipelineDepth=1` |
| 27 | metagraph never seeds; shard ord stuck at genesis | genesis-FULL never reached gl0 (one-shot, gl0 not finalizing yet); solo cohort ⇒ no peer holds it | partial: harness gl0-readiness gate (finalizedOrd≥2) — closes the gl0 side; metagraph-side (#28/#36) still open |
| 27b/c | one MG (DAG5L1ez) ADOPT-VERIFY mismatch | window anchor = **bestTip**, producer diff-prior = **base** — split when bestTip>base ⇒ producer folds the bestTip-anchored window onto a lagging base ⇒ wrong `next` | fixed: **contiguity gate** in `reExecDerivationWithDiff` — OMIT (defer) when the window doesn't chain from base (ordinal gap) |
| 27d/e | shard chain frozen at genesis; `embed-none` forever; SC-tip (`b31d16cf`, **binary-ord3**) ≠ adoptedShardOrd (**checkpoint-ord1**) | **two ordinal spaces** + a genesis **adopt-vs-derive race**: a producer that hasn't yet observed its own ord-1 adopt has `priorOpt=None` ⇒ OMITs the MG ⇒ a **partial checkpoint** that **wins maxvalid-tk** (lower slot) ⇒ regresses `perMgTip` to genesis ⇒ the **awaiting-embed gate** (`pipelineDepth=1`) freezes re-production permanently. Three correct-in-isolation guards deadlock. | fixed: **defer-the-whole-checkpoint-on-OMIT** (`ShardCheckpointProducer.assembleDelta` → `Option`) — never mint a partial; defer until *all* active MGs derive, so the caught-up producer's COMPLETE checkpoint wins |
| (latent) | over-prune: gl0 mirror has fewer active locks than metagraph | ml0 expires @ raw synced epoch but advertises monotone `globalSyncView`; the producer re-execs at the advertised (higher) epoch | latent; captured *inside* the diff so it does NOT cause an apply mismatch, but corrupts the mirrored set |

**Reading this table is the point of the model:** these "different" bugs share one disease — *unaligned versions of S(N)*.
**Sharp edge surfaced run-27d (add to §1):** "SC-binary ordinal" (the per-MG `lastStateChannelSnapshotHashes` tip, set to the *last binary* of an adopted window) and "shard-checkpoint ordinal" (`adoptedShardOrd`) are DIFFERENT counters. One shard checkpoint can batch N metagraph binaries, so the SC tip can be N ahead of the checkpoint ordinal — this is normal, not a fork. Conflating them sent me chasing a phantom "tip past genesis."

---

## 6. Standardized debugging workflow (follow this; don't tail-chase)

When the sharded mirror misbehaves:

1. **Reproduce + pull BOTH metrics and logs** (HARD SOP — Prometheus heap/gc/consensus AND node logs; never logs
   alone). Keep the cluster up (`--keep-alive`).
2. **Localize to one ordinal space** (§1). Is the gap in gl0-global, metagraph-currency, or shard-checkpoint
   ordinals? Don't conflate ("gl0 ord 462 ≠ currency ord 250 ≠ shard ord 3" is normal).
3. **Localize to one actor + one storage version** (§2–3). Who computed the divergent bytes, against which
   version (finalized base / branch / pending)?
4. **Classify the failure**:
   - root *mismatch* with `diff(upserts=k)` small ⇒ a **PRIOR** divergence on an untouched field ⇒ a §4
     version-alignment break. Identify *which* of the three references (window anchor / diff-prior / apply-prior)
     is on the wrong version.
   - `embed-none matched=false` ⇒ gl0's **SC tip is off the canonical lineage** (genesis fork / orphan), not an
     apply problem — look at seeding/propagation (§5 run-27), not the diff.
   - state present in metagraph but absent/empty in gl0 mirror, NO mismatch logged ⇒ **over-prune** (epoch
     divergence) or a seeding gap — the diff faithfully carried a wrong `next`.
5. **Read the seam yourself** (HARD rule — never relay an agent's claim on a consensus seam). Confirm the
   divergent version against the code, not a hypothesis.
6. **Before changing anything, check the interaction**: a fix that moves ONE of the three §4 references must move
   the OTHER TWO to the same version, or it just relocates the split. (This is what almost bit DAG5L1ez.)
7. **Fix → rebuild only the affected jar** (the reader/producer lives in dag-l0 ⇒ gl0.jar only; config is bundled
   but consumed only by gl0) → re-run → **probe ~20min in** for the decisive signal (adoptedShardOrd advancing;
   ADOPT-VERIFY count; embed-none) before waiting the full hour.
8. **Update THIS doc + the §5 table** with the new divergence point and which version was wrong.

---

## 7. Open items this model surfaces (prioritized)

1. **DAG5L1ez (run-27b):** ~~align all three §4 references at the finalized base~~ — **FIXED (run-27c) by a
   contiguity gate**, the simpler realization of the same goal. Rather than restructure the window to re-anchor at
   base, the producer (`ShardCheckpointWiring.reExecDerivationWithDiff`) now **OMITs an MG whose window does not
   chain from the finalized base** — detected by ordinal: a base-contiguous window advances `base.ordinal` by
   exactly `windowSize`; a gap means `bestTip > base`. This *enforces* the §4 alignment by only producing in the
   aligned case (window-anchor == diff-prior == base; gl0's branch==base there too, so apply-prior coincides — **no
   gl0 change, no window restructuring**). The deferred MG self-heals when its in-flight checkpoints finalize (base
   catches up), then produces `base→latest` in one burst. This is the design doc's skipped I2 *input check* (§5
   line 43, "assert root' === binary.stateProof"), implemented as the cheaper ordinal-gap test. Tradeoff: an active
   MG's mirror bursts at the finalization cadence rather than continuously — acceptable as long as the burst lands
   inside the test's verify timeout. Note: my run-27 producer-reader fix (`pending → base`) is what *introduced*
   this facet — it removed non-determinism (890→92) but left the window anchored at bestTip; the gate closes that.
2. **Seeding (#28/#36):** the harness gl0-readiness gate is necessary but not sufficient — the metagraph-side
   one-shot genesis-FULL + solo cohort still loses binaries. Harden with a per-metagraph verify-and-retry in the
   harness, and/or durable genesis-FULL re-emission + cohort admission in production.
3. **Over-prune:** unify ml0's expiry epoch with the advertised `globalSyncView` (entangled; see
   `MODE2-GLOBAL-SYNC-VIEW-RCA.md` — a prior consumer-side fix caused a `Conflict{ordinal=1}` regression).
