# Shard sub-chain ← gl0 phase-2 finality: hard anchor design

**Status:** DRAFT (run-24 diagnosis). **Owner seam:** `ShardChainStore` fork choice + `ShardCheckpointGl0AcceptanceManager` anchor + `NakamotoSyncDaemon` anchor drive.

## 1. Problem, in the fork-string formalism

The shard-checkpoint sub-chain (one per shard, Nakamoto/Taktikos `maxvalid-tk`) is **manufacturing forkable characteristic strings from purely honest behavior**:

- **Rank rotation** (shuffled staircase) hands duty to the next rank every `staircase-delta-slots`; if the prior rank's checkpoint hasn't reached the cluster, the next rank mints a competing checkpoint at the same depth → a `1` in the characteristic string.
- **ml0 binary re-emission** — the metagraph's BFT-final *state* is unique, but it re-emits the snapshot as a second `StateChannelSnapshotBinary` (same state, different hash); producers that captured different reps build divergent windows → reinforces the fork.

Run-24 (shard 1): `w = 0 0 0 0 1 1 1 1 1 1` — uniquely-honest through depth 4, then a fork at **every** subsequent depth. See `forkstring-run24.png`.

The two tines:
- **α (gl0-finalized):** … d4 → C7A (d7). gl0's global consensus embedded this into a **phase-2 finalized** global snapshot. SC tip = `a240a5fe`.
- **β (local bestTip):** … d4 → C7B → … → C10 (d10). Longer, so local `maxvalid-tk` picks it.

**The bug:** in a healthy Nakamoto chain, honest slots are uniquely-led (`w≈0ⁿ`), so the longest tine *is* the finalized one. Here the protocol forks from honest behavior, and **nothing collapses the tines back onto gl0's finality** — so the finalized vertex (C7A) ends up off the winning tine. gl0's SC tip `a240a5fe` is then orphaned: every live binary chains off β, none off `a240a5fe`, so gl0 fail-closes and freezes; the producer stalls `awaiting-embed`. Mutual deadlock.

## 2. Why the existing anchor doesn't bite

The fork choice is already correct: `ShardChainStore.compareAnchoredMaxvalid` puts **anchor-compatibility FIRST** — a tine through the gl0-finalized vertex beats any tine that isn't, before length. `noteAnchor` re-runs fork choice under a new anchor (the heal). This is exactly the finality gate.

It never fires for the loaded shard because the **anchor signal is dead**:

- `noteAnchor` ANCHOR-REORG fired only for `ShardId(0)` (empty shard), **never `ShardId(1)`** (run-24 logs).
- gl0 finalized **C7A (α)**, but the local chain store followed **β** and **never stored C7A**. `noteAnchor` refuses a not-yet-stored hash (`ShardChainStore.scala:354` — "never replace a known anchor with a not-yet-stored hash"), so the anchor stays on a shared ancestor both tines contain → no discrimination → plain `maxvalid-tk` → β wins forever.

So: **the finality gate can't bite because the node doesn't have the finalized block.**

## 3. Design — gl0 phase-2 finality as a HARD anchor

Principle (matches how the metagraph already subordinates to gl0 finality): **the shard sub-chain's canonical tine is whatever gl0 has phase-2 finalized; the p2p sub-chain may run ahead optimistically, but on any divergence it re-bases on the finalized vertex.**

Four changes:

### S1 — fetch-on-finalize (the load-bearing fix)
When the gl0-finalized anchor (`lastAdoptedAnchor(shard)`) advances to a checkpoint the local store does **not** have, **pull it by hash + walk its ancestry** via `ShardCheckpointFetcher` (already built), store the ancestry, then `noteAnchor`. Now `noteAnchor` succeeds → `compareAnchoredMaxvalid` reorgs the local bestTip onto the finalized tine (α) → producer builds the next checkpoint on C7A → its window chains off `a240a5fe` → gl0 adopts → unblocked. This converts the orphan-pull I added (run-22) into a *finality-driven* reorg instead of a best-effort heal.

### S2 — anchor tracks gl0's LATEST finalized, every finalization
Ensure `noteAdopted → lastAdoptedAnchor → noteAnchor` advances on **every** gl0 finalization (today it effectively moved once for shard 1). Verify the daemon's `deps.acceptanceManager` is the SAME instance `GlobalSnapshotAcceptanceManager` calls `noteAdopted` on (the #44 split risk — `ShardCheckpointWiring` scaladoc notes "TWO `GlobalSnapshotAcceptanceManager.make`"). If split, unify so the adopt watermark and the daemon's anchor read share one `Ref`.

### S3 — producer + pipeline gate read the anchored tine
Already true (`produce` reads `chainStore.bestTip`); once S1/S2 land, bestTip *is* the finalized tine, so production follows finality with no further change. Confirm the `awaiting-embed` pipeline gate uses the anchored adopted watermark.

### S4 — reduce the fork RATE: canonicalize ml0 re-emissions
At the shard buffer intake (`ShardBinaryBuffer.bufferBinary` / `bufferReceivedBinaryForShard`), key by **ml0 ordinal** and keep one deterministic representative (lowest binary hash) — since the metagraph state is BFT-final, re-emits are equivalent. Fewer `1`-slots in the characteristic string ⇒ fewer forks for S1 to heal. Defensive, not load-bearing.

### Not doing
- A new committee agreement round on the anchor — **gl0 phase-2 finality already IS the unanimous agreement**; reuse it (this is the answer to "committee-agreed common ancestor"). The optimistic-2/3-agreed-latest layer is deferred unless latency demands it.
- Tolerant overlap-match in gl0 embed-selection (earlier "fix A") — papers over the gl0 side without collapsing the layer-2 fork; superseded by S1.

## 4. Invariant after the fix
For every shard, on every node: `localBestTip` is a descendant of (or equal to) gl0's latest phase-2-finalized checkpoint for that shard. A divergent longer tine can exist transiently but is abandoned the moment gl0 finalizes — the fork-string is collapsed by finality, not by chain length.
