# Handoff #4: #113 — Last blocker is L0-token reverse on gl0

## What we know (with evidence)

Iter 3 (`/tmp/e2e-iter-3.log`, gl0/gl1/ml0/cl1 logs preserved at `/tmp/iter3-logs/`) shows the cleanest run yet:

- ✅ DAG cluster, delegated_staking, token_lock_replacement, **fork-recovery** all pass.
- ✅ **All DAG transactions pass** — single forward 15s, single reverse 25s, batch 100 forward 20s, batch 100 reverse 20s.
- ✅ **L0-token forward** settles in 5s (cl1's balance updated correctly: 99990/100010).
- ❌ **L0-token reverse fails** at 180s polling timeout. cl1 ACCEPTS the reverse tx (its `lastTxRef` advances to ord=1, hash=f6b4d2cb85e1) but cl1's *balance map* never updates from the post-forward values.

Diagnostic from iter 3:

```
[DIAG] post-failure L0-token state:
  origin(DAG87hragrbz..) cl1LastRef={ord=1,hash=f6b4d2cb85e1..}    # accepted in cl1
  dest(DAG0d6yzQqBZ..)   cl1LastRef={ord=1,hash=9a176816616c..}    # forward-tx hash, expected
  gl0MetagraphOrd=14 (was 14 before submit)                          # frozen
```

`gl0MetagraphOrd` is the metagraph snapshot ordinal that gl0 reports in `/global-snapshots/latest/info` → `lastCurrencySnapshots[<metagraphAddr>]` → snapshot ordinal. It was 14 *before* the reverse submit (despite the forward having succeeded), and stayed 14 throughout the 180s polling window.

This means: **gl0's GSI dropped the metagraph from `lastCurrencySnapshots` after the forward completed**, and isn't picking up subsequent ml0 binaries. cl1 reads gl0's GSI for its balance source-of-truth → balance frozen → test fails.

Under Passthrough, this scenario settles in 20s (Agent #1 confirmed via A/B comparison). Under MultiBranch, reproducible failure across multiple iterations.

## Suspect: priorLastCurrencySnapshots single-key materialization

`GlobalSnapshotAcceptanceManager.scala:838-862` — the prior currency-snapshots map is built by reading ONE sidecar index key:

```scala
indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastCurrencySnapshots)
addrSet <- mpt.get[SortedSet[Address]](indexKey).map(_.getOrElse(SortedSet.empty[Address]))
addrList = addrSet.toList   // EMPTY if mpt.get returned None
// ... per-address lookups gated on addrList ...
```

If `mpt.get` returns `None` at any ord under MultiBranch (e.g., overlay chain walk fails, eviction race, `pendingRef` empty after finalize-on-commit and base hasn't propagated the index write yet), `addrSet` is empty → no per-address lookups → `priorLastCurrencySnapshots = SortedMap.empty`. Then `processStateChannelEvents` builds the new GSI's `lastCurrencySnapshots` from THIS ord's events only. If no metagraph events this ord → metagraph **vanishes** from the map. Next ord's `priors` are this ord's `lastCurrencySnapshots` which is empty → vanishing persists.

Agent #1 added a defensive GSI fallback (`46d505c5`, since reverted) and saw 0 WARN hits — but in their test conditions, gl0MetagraphOrd advanced normally. In iter 3 (different timing/conditions) it freezes. The agent's evidence may have been gathered when conditions didn't trigger the empty-addrSet race.

## What to do (sharp focus)

### Step 1 — Add targeted WARN logging on the metagraph DROP

In `GlobalSnapshotAcceptanceManager.scala` (the post-`processStateChannelEvents` path that builds the GSI), add a check: if **previous-ord lastCurrencySnapshots had a metagraph that the new lastCurrencySnapshots doesn't**, log at WARN level with `[#113-DROP]` tag:

```scala
val priorSet = priorLastCurrencySnapshots.keySet
val nextSet = nextLastCurrencySnapshots.keySet  // whatever the post-processing variable is named
val dropped = priorSet -- nextSet
if (dropped.nonEmpty) logger.warn(s"[#113-DROP] ord=${ordinal.show} metagraphs disappeared: ${dropped.mkString(",")} priorSize=${priorSet.size} nextSize=${nextSet.size}")
```

Pick the right variables — read the source. The point is: trigger when the metagraph is in priors but NOT in the new GSI's map. That's the moment gl0 "loses" the metagraph entry.

ALSO add a log just before/after the `mpt.get[SortedSet[Address]](indexKey)` call:

```scala
addrSet <- mpt.get[SortedSet[Address]](indexKey).map(_.getOrElse(SortedSet.empty[Address]))
_ <- logger.warn(s"[#113-IDX] ord=${ordinal.show} indexKey size=${addrSet.size}").whenA(addrSet.isEmpty)
```

So we can correlate: did the index return empty at the same ord the drop happened?

### Step 2 — Build the JARs (forces clean rebuild of gl0.jar AND cl1.jar AND ml0.jar)

```bash
rm -rf metagraph/project_template/modules/{l0,l1,data_l1,shared_data}/target
just clean-data && just clean-configs
just test --skip-streaming --grafana 2>&1 | tee /tmp/e2e-iter-instrument.log
```

### Step 3 — When test fails (it will, at L0-token reverse), grep for evidence

```bash
grep "#113-DROP" nodes/0/gl0-logs/gl0-run.log nodes/1/gl0-logs/gl0-run.log nodes/2/gl0-logs/gl0-run.log
grep "#113-IDX"  nodes/0/gl0-logs/gl0-run.log
```

Three possible outcomes:
1. **`#113-DROP` fires before the L0-token reverse window**: the metagraph was dropped at a specific gl0 ord. Find that ord, look at what happened at that ord (state-channel events count? gossip events?). Does `#113-IDX` fire at the same ord?
2. **`#113-IDX` fires (empty addrSet) but `#113-DROP` doesn't fire concurrently**: the addrSet returned empty but the drop logic is wrong, or the per-address fallback pulls it back. Investigate.
3. **Neither fires**: the drop happens via a different path. Look at the full gl0 acceptance log around when gl0MetagraphOrd freezes.

### Step 4 — Implement the actual fix

Based on the evidence:
- If `#113-IDX` is the cause: the priorLastCurrencySnapshots reads need to NOT depend solely on a single index key. Either source the keyset via prefix scan (mirroring how other partitions do it post-tasks #40/#41/#42), or fall back to GSI keyset when MPT addrSet is empty (similar to agent #1's `46d505c5` but actually triggered).
- If the drop happens via a different mechanism: fix that.

### Step 5 — Validate

Re-run e2e once. Capture: phase outcomes, `#113-DROP`/`#113-IDX` log counts (should be 0), gl0MetagraphOrd advancement (should reach 30+), L0-token reverse settle time. Hand back.

## Hard rules

- **Don't break what works.** Iter 3 shows DAG layer is solid. Don't touch overlay logic; don't touch the metagraph fix `72272b8a`; don't touch the validator orphan-discard `88ee5e52` or the finalize-on-commit / dual-tip eviction `43e44b6f`.
- **`OverlayMode.productionDefault = MultiBranch(4)` MUST stay.** Don't toggle.
- **No `git push`, no PR.** Local commits on `feature/serde-typeclass-shim`.
- **Don't pass `--nakamoto-gl0`** (removed). Use `--skip-streaming --grafana`.
- **`just clean-data && just clean-configs`** between runs (NEVER `just clean`).
- **Verify fix lands in deployed gl0.jar** — extract the file from `docker/jars/gl0.jar` and grep for the `[#113-DROP]` string before re-running. Stale-JAR trap.
- **Epistemic honesty.** Don't claim "validated" without evidence. Default to "I don't know".
- **Don't create speculative .md files.**
- **Preserve nodes/<n>/<layer>-logs/** — copy to `/tmp/iter-N-logs/` before clean-data if you want forensics.

## Hand-back format (≤300 words)

1. Diagnostic-log evidence: did `#113-DROP` fire? At which ord? Did `#113-IDX` fire? Did they correlate?
2. Confirmed root cause in one sentence with file:line.
3. Fix applied: commit hash + file:line summary.
4. e2e validation: did L0-token reverse settle? Time? gl0MetagraphOrd at end? Total runtime?
5. Anything you tried that didn't work.

If 2 e2e iterations don't isolate the cause, hand back early with the strongest hypothesis. Don't loop indefinitely.

Working dir: `/home/euler/repos/tessellation-nakamoto`. Branch: `feature/serde-typeclass-shim`. Latest commit: `72272b8a` (cherry-picked metagraph fix). Iter 3 logs preserved at `/tmp/iter3-logs/` if you need ground-truth from the failure scenario without re-running.
