# #259 — Metagraph verifier divergence on `historicalStakeSnapshots` (fix options)

**Status:** root cause narrowed; fix is a consensus-design decision (awaiting direction).
**Date:** 2026-05-27

## Confirmed mechanism (evidence-backed)

When a metagraph node (ml0/cl1/dl1, all BFT) replays a gl0 global snapshot to track
`globalSyncView`, it runs `GlobalSnapshotAcceptanceManager.accept` and recomputes gl0's
stateProof. At an eta-rotation boundary (`ord % R == R-1`, R=100) the accept path writes
`historicalStakeSnapshots[currentPeriod] = HistoricalStakeSnapshot(snapshotFromMpt, eta)`
(`GlobalSnapshotAcceptanceManager.scala:796-843`). The metagraph's computed entry **diverges
from gl0's** → the ord-99 stateProof check fails → MPT rollback → the entry never persists →
the field stays empty → every subsequent ordinal mismatches (gl0 has it, metagraph has empty) →
~197 `recoverFromOrphan` recoveries/metagraph, every ~5 ords, throttling the metagraph's
global-epoch tracking to ~0.2x. That throttle is what fails `validateTokenLockExpiration`
(metagraph epoch never reaches `unlockEpoch`).

**Not the cause (ruled out):** (a) the R config asymmetry alone — `etaRotationSnapshots` IS
threaded to the metagraph GSAM (`SharedServices.scala:301`), boundary fires at ord 99; (b) a
wiring gap — `etaForPeriod` IS wired too (`SharedServices.scala:305`); (c) #258 LocalEventsService
(red herring — stream healthy, 0 drops); (d) the carry-forward (correct, GSAM:834-841). The
divergence is in the **content**: `NodeStakeAggregator.snapshotFromMpt` (stake) or
`sharedEtaForPeriod` (eta, the Path-1 `e9756d9e6` addition, backed by an EtaStateManager
chain-walk fallback). **Leading hypothesis (unconfirmed): the eta** — gl0 derives it from its own
VRF-output chain; the metagraph's chain-walk fallback may lack gl0's VRF outputs. Pinning
stake-vs-eta needs instrumentation (a byte-level compare at ord 99).

## Key constraint

`historicalStakeSnapshots` is a **user-field partition committed in gl0's aggregate `mptRoot`**.
The metagraph's computed root must equal gl0's *claimed* root, which includes this field. So the
metagraph **cannot unilaterally exclude it** from its root (that guarantees a mismatch). It must
either obtain gl0's value, tolerate the diff, or have the field excluded on **both** sides.

## Options

1. **Adopt gl0's value (recommended).** When the metagraph verifies a gl0 snapshot, take
   `historicalStakeSnapshots` from gl0's claimed stateProof/GSI instead of recomputing it from
   stake+eta. The metagraph's root then matches by construction. Rationale: the metagraph is a
   *follower* that doesn't consume this field (it's gl0 leader-election state); gl0's 8 nodes
   verify it among themselves (zero mismatches), so adopting gl0's authoritative value loses no
   real guarantee. Lowest risk; no need to reconstruct gl0's stake distribution or VRF chain.
   *Caveat:* must be scoped to verifier-replay only — gl0 producers still compute it normally.

2. **Fix the recompute.** Make the metagraph's `snapshotFromMpt`+`sharedEtaForPeriod` reproduce
   gl0's value byte-identically. Requires the metagraph to track gl0's stake distribution AND VRF
   chain (for eta). Most "correct" but the heaviest, and may be infeasible if the metagraph
   genuinely lacks gl0's VRF outputs. Needs the stake-vs-eta instrumentation first.

3. **Scoped tolerance.** Metagraph tolerates a `historicalStakeSnapshots`-only mismatch. Cheap but
   **goes against the hardening direction** (#25/#26 deliberately removed mptRoot/per-field
   tolerances). Least preferred.

4. **Exclude globally (gl0 + metagraph).** Drop `historicalStakeSnapshots` from the committed
   `mptRoot` on *all* nodes (the ActiveAddressIndex `353dcabfb` pattern, but for a user field).
   Only viable if gl0 does **not** need this field committed in the snapshot root (e.g. for
   light-client proofs / N-2 staggering integrity). Bigger blast radius; needs that confirmation.

## Recommendation

**Option 1 (adopt gl0's value), scoped to the verifier-replay path.** It directly removes the
divergence, matches the follower-trust model, and avoids both the hardening-regression of (3) and
the reconstruction cost/feasibility risk of (2). If you prefer (2), the prerequisite is the
stake-vs-eta instrumentation to confirm which component diverges (and whether the metagraph can
even get the eta inputs).

## Validation plan (once a fix lands)

Re-run `just test --num-gl0=8 --metagraphs=4 --num-shards=4 --grafana --skip-streaming` with the
metagraph + gl1 R fixes (already staged) + the chosen fix. Expect: metagraph `StateProofMismatch`
→ ~0, epoch tracking ~1x, `validateTokenLockExpiration` passes. The R fixes alone already restored
token-lock *propagation* (`test-runs/numshards4-Rfix-v1.log`).

## Related

- [[project-s1-s6-validated-259-blocker]] (memory) — full saga + the 3 prior root-cause refinements.
- ActiveAddressIndex consensus-root exclusion `353dcabfb` (worktree, parked) — the Option-4 precedent.
- clearPending `c2b2f07d9`, global-argmax `9de1913c7` — parked, unmerged.
