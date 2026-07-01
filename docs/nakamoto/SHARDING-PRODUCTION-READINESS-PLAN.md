# Sharding Production-Readiness Plan (design of record)

**Status:** APPROVED direction (user, 2026-06-30). Drives the numShards>1 production-readiness work.
**Companion:** [CURRENCY-APP-TOKEN-ENFORCEMENT.md](./CURRENCY-APP-TOKEN-ENFORCEMENT.md) (the layer model).
**Rule:** build the full model before the next sharded e2e — no deferring to "later" (user directive).

---

## 0. The goal

Cross-shard interactions between L2 currency apps, sequenced by the global hypergraph (gl0), with the
**framework token model hypergraph-enforced by RE-EXECUTION** (not trusted-to-metagraph), at a latency that
isn't gated by the full k1 challenge window.

Two tracks, sharing the watchtower:
- **Track 1 (correctness):** make re-execution the primary adoption path; delete the authoritative override.
- **Track 2 (latency):** optimistic finality — watchtower approvals give fast spendability, depth-k1 backstops.

---

## 1. Decisions (settled — do not re-litigate)

- **Re-exec is primary** for the currency token model; `authoritative*` override is REMOVED (kept only for
  arbitrary non-currency state channels + the data-app sub-state). [Track 1]
- **Fast tier = M-of-N watchtower approvals**, layered ON TOP of the **1-of-N fraud-proof + depth-k1 safety
  floor** (never removed). max-of composition. The floor preserves "committee size doesn't matter for safety";
  the fast tier is a *spendability* accelerator whose early-release safety rests on VRF-sampled `kApprove`.
- **Defer, don't revert.** Spendability is DEFERRED until `approvals ≥ kApprove OR depth ≥ k1`. A pre-finality
  dispute is a clean reject. **No state-revert is built.** The append-only corrective checkpoint is a deferred
  Phase-3 backstop, gated on a VaR analysis showing it's needed.
- **Coverage is load-bearing** (the price of no-revert): VRF watchtower sampling MUST include **no-show →
  escalate** so every checkpoint gets ≥1 honest re-exec within k1. Otherwise the depth-k1 leg can't catch a
  fraud that no honest watchtower happened to sample.
- **Cross-shard invariant:** a cross-shard consume may only reference SOURCE state that is already final
  (gate the consume on the *source* checkpoint's finality, not just the consuming one).

---

## 2. Determinism invariants (any violation = cluster split)

1. **No node-local state gates consensus state.** Approval/attestation counts that gate spendability MUST be
   carried in-band and re-counted deterministically in the fold (the `verifyEmbedded` pattern), never read from
   a node-local `FinalityTrigger`/`TipTracker`/`bestTip` (the GSAM:771-774 / Issue-#3 hazard class).
2. **numShards=1 stays byte-identical** to the pre-sharding path at every step (regression gate).
3. **Pure function of in-band bytes + finalized state** for every adopt/gate decision.

---

## 3. Track 1 — re-exec primary (root cause + fix)

> **REFINEMENT (verified 2026-06-30 — supersedes the half-measure below).** Investigation (3 deep traces)
> resolved the approach: **run the ONE shared `accept()`, don't reimplement it.** `CurrencySnapshotAcceptanceManager.accept`
> has zero data-app calls (data-app is a separate `Option`-injected manager; gl0 already passes `None`), and the
> data-app never writes the `balances` map — so token balances are 100% framework-derivable and the *same* `accept()`
> ml0 runs can run on the gl0/committee/watchtower rail (data-app stubbed via the existing `None` seam; committed
> rewards/fees/artifacts passed as constants). The blocker that forced the `deriveAdoptedCurrencyInfo` replay +
> authoritative-push was **not** the data-app and **not** the 31s stall (a `noGlobalSnapshotLookup` stub × retry-on-None)
> — it was that `accept()` is **not yet a pure function of `(prior, blocks, recorded globalSyncView)`**: it reads the
> node's finality-gated head (`lastGlobalSnapshotStorage.getCombined`, CSAM:290) for message-acceptance + spend-action
> balances, plus a node-local `globalSnapshotsAlreadyProcessed` cache, and self-mutates the MptStore. Those aren't
> recorded, so no verifier reproduces ml0's result → divergence → the retreat to authoritative-push.
>
> **So Track-1 step 0 is `I-PIN`: make `accept()` pure.** Reorder so `globalSyncView` is selected first, then read ALL
> global state at the recorded anchor (not the per-node head); make `globalSnapshotsAlreadyProcessed` derivation-pure;
> resolve the MptStore re-entrancy. The anchor is selected at gl0's **Phase-1 watermark** (optimistic-final, low-reorg,
> ~10s–min latency — only gl0 producers sit at the bleeding edge) and pinned by **hash** (CSAM:376-385), so verifiers
> reproduce-or-reject deterministically. **Graceful degradation:** a rare Phase-1 reorg → recorded hash no longer
> canonical → *reject* (not raise) → the metagraph re-derives against the new canonical view (one snapshot redone);
> Nakamoto depth (Phase 2/3) is the eventual-consistency floor. Verified orthogonal to the MODE2 selection bug.
>
> **Then:** base-pin (below) for the prior → call the shared `accept()` on the gl0 rail with the real finalized-chain
> `getGlobalSnapshotByOrdinal` (not `noGlobalSnapshotLookup`) → per-field `result.stateProof === artifact.stateProof`
> gate → **delete `deriveAdoptedCurrencyInfo` + both authoritative overrides**. Data-app metagraphs: token model
> re-executed + enforced; data-app `calculatedState` stays authoritative; own-token mint/reward/fee is sovereign but
> bounded (can't forge DAG / other-MG value — those go through the enforced global fold / cross-shard settlement).
> Determinism gate for I-PIN: a forcing test where two invocations with DIFFERENT local heads but the SAME recorded
> `globalSyncView` produce byte-identical `stateProof`.

**Root cause (verified):** the authoritative override masks a **base-ordinal mismatch**. Both committee
producer (`ShardCheckpointWiring.reExecDerivationWithDiff`, `priorStateReader = fromMptStore(mptStore)`) and
gl0 adopter (`priorInfoOf` via `baseCurrencyInfoReader = fromMptStore(mptStore) = overlay.base`) read the
finalized base — but **each reads its own `overlay.base` at its own latest-finalized ordinal**. The committee's
diff is a minimal delta keyed to *its* base ordinal; when gl0 lagged on that MG, the delta lands on the wrong
prior → `recomputed ≠ attestedRoot` → MG dropped → base never advances → self-perpetuating "S(N)-lags"
deadlock. Pure timing/catch-up; readers are identical by construction.

**Fix (minimal):**
1. **Pin the diff base** — carry `diffBaseOrdinal` (the cluster-uniform depth-k-finalized gl0 ordinal the
   producer's S2 window/diff is anchored on; computed today at `GlobalSnapshotConsensus:1700-1703`) in a
   `ShardCheckpointSigPreimageV2` bump (greenfield: no wire-compat).
2. **Both sides read the per-MG prior at that pinned ordinal** (reuse `getGlobalSnapshotByOrdinal` /
   version-retained finalized reads), not "latest `overlay.base`."
3. **Bounded defer** if gl0 hasn't finalized to `diffBaseOrdinal` (guaranteed reachable — base is depth-k
   *behind* the tip; self-heals, unlike the unpinned deadlock).
4. **Delete the override** (`GlobalSnapshotAcceptanceManager:1064-1073` + producer twin
   `ShardCheckpointWiring:351-360`); `reconstructInfoFromDiff` over the pinned base is primary; **GAP-1 per-field
   signed-proof gate stays** as the Byzantine-producer check.
5. **Re-ground ShardReanchor's drop check** on the pinned base (the override currently backstops a wrong
   reanchor; with a pinned base, the base-dependent recompute does the same job — verify it still drops).
6. **Fix stale scaladocs** (`ShardCheckpointWiring:186-191` falsely claims best-tip base).

**Gate:** numShards=1 byte-identical; numShards>1 `reExecRoot === stateProof` with the override gone; the
sharding/adopt-parity/reanchor regression suites green.

---

## 4. Track 2 — optimistic finality (defer-not-revert)

Reuse the existing two-tier `FinalityTrigger` (`ShardFinalityTriggers` already composes committee-attestation
OR shard-depth, max-of). Add:

1. **Watchtower approvals** — flip `watchtowerReExec`'s discarded match signal into a signed approval; carry on
   the `fraudProofs` dedicated-field / pool / byte-exact-recreate rails as a new in-band `approvals` set;
   **re-count deterministically in the fold** (invariant #1).
2. **VRF-sampled per-MG watchtowers + intra-epoch rotation + no-show→escalate** (primitives: `EcVrf25519`).
   Replaces every-node-every-MG. Unpredictability is wanted here (distinct from the predictable committee).
3. **Spendability gate (the one real build)** — stage a checkpoint's economic effects (cross-shard markers +
   adopted balances) and release on `approvalCount ≥ kApprove OR depth ≥ k1`. Today effects apply immediately
   at the fold; this adds a staging layer keyed by checkpoint. Cross-shard consumes gate on the SOURCE
   checkpoint's release.
4. **Size `kApprove`** against the VaR cap.

**Gate:** happy-path spendability in minutes; depth-k1 backstop; pre-finality dispute = clean reject; the
cross-shard consume forcing-function test passes (source-final → consume → destination spendable).

---

## 5. Phase 3 — residual safety

- **I-AUTH** env-dependent operator-majority signature (today ≥1 sig; intended).
- **VaR cap** enforcement (per-checkpoint value-at-risk ≤ committee-stake × slash_fraction).
- **Append-only corrective checkpoint** — ONLY if VaR analysis shows the no-revert residual is unacceptable.
- Then the full 2-shard adversarial e2e with the complete model.

---

## 6. Open parameters (to size, not blockers)

- `kApprove` (approval threshold for fast spendability) vs VaR.
- Watchtower sample size + escalation schedule (coverage guarantee).
- VaR cap value.
