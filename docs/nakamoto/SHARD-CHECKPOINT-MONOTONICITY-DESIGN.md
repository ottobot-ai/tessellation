# Shard-Checkpoint Monotonicity & Idempotent Production — Design Proposal

**Status:** draft for critic review. No code commitment. Written 2026-06-12 against
`feature/serde-typeclass-shim` at HEAD `09af4447e` (the task #44 unification commit).
Authored after the run-19 focused e2e (`5gl0 / 2shard / 2mg`) root-caused the next
shard-checkpoint blocker (task #45).

**Thesis (one line):** *Production became deterministic (the shuffled staircase, run-14);
the shard chain data-model stayed probabilistic (a fork-DAG + `maxvalid-tk`, copied from
gl0). #45 lives in that mismatch. The fix is to align the model with the staircase — make
the shard chain **monotonic, single-successor, and idempotently produced**, with a
gl0-**anchor-reorg** as the one sanctioned exception that may replace a losing branch.*

**Scope:** the gl0-side shard-checkpoint **production** loop (`ShardCheckpointProducer`,
`ShardCheckpointFanOut`) and **chain store** (`ShardChainStore`) at `numShards > 1`.
Out of scope: the acceptance/verify path (`ShardCheckpointGl0AcceptanceManager`,
unchanged identity contract), committee sortition, the metagraph (cl1/dl1/ml0) layers,
and anything at `numShards = 1` (the regression bar — this whole path is inert there).

Cross-references (read in conjunction):

- [`HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md`](./HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md) — the current checkpoint schema, operator duties, and universal GL0 replay contract.
- [`../adr/0017-committee-reexecution-is-the-primary-economic-validity-gate.md`](../adr/0017-committee-reexecution-is-the-primary-economic-validity-gate.md) — committee quorum never authorizes framework-economic state.
- [`SHARD-CHECKPOINT-FLOW-WALKTHROUGH.md`](./SHARD-CHECKPOINT-FLOW-WALKTHROUGH.md) — the produce → gossip → attest → adopt → embed pipeline.
- [`attestation-and-finality.md`](./attestation-and-finality.md) — `kQuorum` / `tCountShard` / the Phase-0/1/2 finality model.
- Memory: `[[per-slot-rewire-runs12-13]]` (runs 12–19, the staircase + anchor-compat + demux + boot-grace + #44 chain; the run-19 forensics that produced this doc), `[[taktikos]]` (LDD / `maxvalid-tk` are clock-denominated), `[[feedback-greenfield-no-wire-compat]]`, `[[feedback-prefer-hocon-over-sysenv]]`.

---

## §1 Problem statement — what run-19 proved

run-19 (`5gl0 / 2shard / 2mg`, HEAD `09af4447e`) booted fully healthy: GL0 5/5 Ready,
DAG-L1 3/3, both metagraphs producing locally (ml0 ordinals climbing 61/68). Task #44
was **validated** — `adoptedShardOrd` advanced `0 → 2` (was dead-stuck on the duplicate
registry), gl0 saw 2/2 metagraphs in `lastStateChannelSnapshotHashes`, and the run-18
ord-3 *spam* is gone (producers now correctly `produce-skip reason=awaiting-embed`,
reading the live watermark). But making the pipeline gate actually function **unmasked
the next blocker**: the shard chain freezes at `adoptedShardOrd=2` and never advances.

### §1.1 The forensic chain (single-node anchor churn)

The shard-checkpoint quorum is **starved by checkpoint-identity churn**, not by a
classic cross-node sibling forest:

1. `ShardCheckpointFanOut.run` drives each producer **once per gl0 ordinal**, passing
   `producedOrd` = the **live gl0 tip** straight in as `gl0AnchorOrdinal`
   (`ShardCheckpointFanOut.scala:70-78,138`).
2. The checkpoint embeds **`gl0AnchorOrdinal` and the per-tick `slot`** in its hashed
   identity (`ShardCheckpointProducer.scala:414-415`). There is **no "already minted
   shardOrd N" guard** anywhere in `produce` / `produceInner`.
3. When the on-duty node is stuck on a `shardOrdinal` (its mint never reaches `kQuorum`
   → never becomes the adopted tip → `nextShardOrdinal` stays `N`), it **re-mints
   shardOrd=N with a fresh `gl0Anchor`+`slot` every gl0 ordinal** — a **distinct hash
   each tick**.
4. **Evidence:** gl0-4 emitted **34 distinct `gl0Anchor` values for the single
   `shard1/ord3`** (`gl0Anchor` 93 → 116 over ~3.5 min); the other four nodes emitted ≤ 1
   each. The ~5 committee attestations smear across the 34 re-anchored variants, so **no
   single hash ever reaches `kQuorum=4`** → `evaluate=PendingMoreAttestations` forever,
   `becameBestTip=false` 65–67× vs `true` 0–7× per node, **no shardOrd anywhere ever
   reached `signers≥4`**, `adoptedShardOrd` frozen at 2.

### §1.2 Why the fork-choice can't save it

`compareAnchoredMaxvalid` (`ShardChainStore.scala:574`) ranks **anchor-ancestry first**
(#42), then `compareMaxvalidTk` (`:589`): higher `shardOrdinal` → **earlier slot** →
lower VRF output. Two failure interactions:

- The **anchor is frozen** at the adopted shardOrd 2, so anchor-compat cannot break ties
  among the shardOrd-3 variants (they all share the same anchored shardOrd-2 ancestor).
- Among the variants it falls to **earliest-slot-wins**. Each re-mint has a *higher*
  slot, so a peer that locked onto an early variant keeps it (`becameBestTip=false` for
  the later arrivals) — but **different peers locked onto different early variants** by
  gossip race, and each attests **its own** best tip. Attestations split; quorum never
  concentrates. (This is the run-13 split-quorum failure mode, one level deeper: the
  staircase fixed *who produces*, not *how many distinct checkpoints one producer emits
  for one ordinal*.)

### §1.3 The model mismatch (the actual root cause)

Shards **do** hash-chain (`parentCheckpointHash`, `shardOrdinal = parent.next`,
connected-only `bestTip`, orphan buffering). What they **do not** enforce is
**monotonicity / single-successor**:

- The producer is **not idempotent** per ordinal — it re-mints freely.
- The store is a **fork-DAG** (`byHash` retains every sibling) resolved by `maxvalid-tk`,
  rather than rejecting a second checkpoint at the same `(shardId, shardOrdinal)`.

This is a near-verbatim copy of gl0's `NakamotoChainStore` + `ChainSelection`
(`ShardChainStore.scala:28,34-36` say so). On **gl0** that is correct: a VRF lottery
means many independent producers legitimately fork, and `maxvalid-tk` is the Nakamoto
resolution rule. But the **shuffled staircase** (`78aa215f4`, run-14) made shard
production a **single deterministic producer per `(shardEta, shardOrdinal)` window** —
and the data model was never tightened to match. With one honest producer per window
there is **no legitimate reason for two checkpoints at the same shardOrdinal**, so the
fork-DAG's permissiveness is now pure downside: it is exactly what lets the churn (and
duty rotation across a stalled ordinal) spray siblings that `maxvalid-tk` cannot collapse
before attestations smear across them.

> **Production became deterministic; the chain model stayed probabilistic. #45 is that gap.**

---

## §2 Thesis & invariants

Align the shard chain with the staircase. Three invariants the design adds:

- **(I1) Idempotent production.** A node mints `(shardId, shardOrdinal)` **at most once**.
  After minting, it **holds** the exact bytes (pinned `gl0AnchorOrdinal` + `slot`) and
  **re-publishes the same checkpoint** (cadence-gated, for loss recovery) until that
  ordinal is adopted or a sanctioned anchor-reorg supersedes it. It does **not** re-mint
  with a fresh anchor.
- **(I2) Monotone single-successor canonical chain.** For a given `(shardId,
  shardOrdinal)` the store recognises **one** canonical checkpoint at a time. A second,
  *different* checkpoint at an ordinal this node already holds canonically is **not**
  allowed to silently win on a `maxvalid-tk` slot tiebreak.
- **(I3) Anchor-reorg is the one sanctioned replacement.** The *only* event that may
  replace a held/canonical checkpoint at an ordinal is gl0 **adopting a different hash**
  at that shardOrdinal (`noteAnchor`) — i.e. the cluster, via `kQuorum`, committed a
  different lineage. Then the local node re-aligns to the anchored lineage (the #42
  healer, now wired to the live Ref by #44) and resumes producing children of it.

I1 removes the *cause* (churn). I2 makes it a store-enforced invariant (defense in depth:
even a buggy/byzantine re-mint cannot churn the canonical identity). I3 preserves the one
genuinely-needed replacement path so liveness/safety under real cluster disagreement is
unchanged.

### §2.1 Why pinning the anchor is safe (determinism)

The checkpoint's `gl0AnchorOrdinal` is **loosely coupled** by design
(`ShardCheckpointProducer.scala:162`: "gl0 accepts at this ord **or any later**"). So a
checkpoint minted with `gl0Anchor = A` still embeds at any gl0 ord `≥ A`; pinning `A` at
first-mint and never bumping it is valid. Determinism is preserved because:

- The anchor is **wire-carried** on the signed envelope; producer and every verifier read
  the *same* `gl0AnchorOrdinal` for the re-exec derivation
  (`derivePerMgState(mg, includedChain, gl0AnchorOrdinal)`), so the fee-cutover and the
  per-MG MPT root stay **byte-identical** regardless of how stale `A` is relative to the
  embedding ord (the #261 contract is untouched — same closure, same inputs).
- `slot` is likewise pinned at first-mint, so the `maxvalid-tk` slot tiebreak and the
  shard-leader-VRF check key on a stable value.

The only thing pinning *changes* is that the checkpoint stops mutating its own hash every
tick — which is the entire point.

---

## §3 The fix — two tiers

### §3.1 Tier 1 — producer idempotence + cadence-gated re-publish (the cause)

In `ShardCheckpointProducer`, add a per-shard **mint memo**:
`Ref[F, Option[HeldCheckpoint]]` where `HeldCheckpoint = (ShardOrdinal,
Signed[ShardCheckpoint], lastPublishedTick)` (one in flight at a time is sufficient —
the bounded pipeline already caps unadopted depth; a small `Map[ShardOrdinal, …]` is the
generalisation if we ever lift `pipelineDepth > 1` to mean multiple distinct held
ordinals).

`produce` becomes:

1. Resolve `bestTip` / `adoptedOrd` as today; compute `nextShardOrdinal`.
2. **If the memo holds a checkpoint for `nextShardOrdinal`** and it has not been
   superseded by an anchor-reorg (I3): **re-publish the held bytes** if the loss-recovery
   cadence gate has elapsed (mirror the run-12 §5.8 sender cadence-gate — unsent every
   tick is irrelevant here since there's exactly one; re-publish on a ~quorum-RTT cadence,
   e.g. every Kth tick), else no-op. **Never rebuild.**
3. **Else** (no held checkpoint for this ordinal): run the existing staircase/on-duty /
   chain-link path, mint **once**, store it in the memo with the **pinned** `gl0Anchor` +
   `slot`, self-store into the chain store, and publish.
4. **Clear the memo** so the next ordinal can be minted, on either trigger:
   - **(a) adoption** — `adoptedOrd.value >= held.shardOrdinal`. Observable **today**: the
     producer already takes `lastAdoptedOrd` (`ShardCheckpointProducer.scala:286`, wired at
     `GlobalSnapshotConsensus.scala:1459`). No new plumbing.
   - **(b) anchor-reorg** — gl0 adopted a *different* hash at the held ordinal. The producer
     has **no anchor channel today** (the signal lives in the daemon + `ShardChainStore.noteAnchor`).
     Two closable options: (i) thread `lastAdoptedAnchor(shardId)` — which **already exists**
     on the acceptance manager (`ShardCheckpointGl0AcceptanceManager.scala:142,220`) — as a new
     producer param at the same wiring site; or (ii, preferred) detect *tip-moved-from-under-held*
     by comparing `held.parentHash` against `chainStore.bestTip.hash` (the store's `noteAnchor`
     already re-homed the tip), avoiding duplicating ancestry logic in the producer.

This alone collapses the 34 variants to 1 and lets attestations concentrate.

### §3.2 Tier 2 — store-level monotonicity invariant (defense in depth)

Enforce I2 **in the `store` bestTip-recompute fold ONLY** (`ShardChainStore.scala:290-301`),
**not** in the shared `compareAnchoredMaxvalid` comparator. The fold seeds with the
established `resolvedBest` and replaces only on `compare > 0`, so making it *sticky on the
established incumbent* for a same-ordinal non-anchored sibling is deterministic. The net
rule: **once a checkpoint is canonical at ordinal N, only the gl0-anchored checkpoint at N
may replace it** (longer-chain extension and the I3 anchor carve-out are unaffected).
Siblings are still *stored* (so a later anchor-reorg can re-home without a re-pull) but
cannot win the tip by a slot/VRF tiebreak alone.

> **DETERMINISM CONSTRAINT (verified 2026-06-12 — do NOT put the guard in the comparator).**
> `compareAnchoredMaxvalid` is shared with `noteAnchor`, which does
> `state.connected.toList.reduceOption((x,y) => if (compare(x,y) >= 0) x else y)`
> (`ShardChainStore.scala:360-362`) over a `Set[Hash]` (`:188`). If the guard makes the
> comparator return `0` on a same-ordinal tie, `noteAnchor` resolves it by **Set iteration
> order** → a different tip on different nodes → a #261 split. The comparator must keep its
> **total deterministic order** (slot → VRF). I2 stickiness therefore belongs only where
> there *is* an established incumbent: the `store` fold. `noteAnchor`'s heal is left to run
> the full deterministic `compareAnchoredMaxvalid` unchanged. See §9.

> **Note (the run-19 case is "both-anchored").** The contested siblings all chain off the
> *adopted* ord-2, so with `anchorHash = Some(ord2)` `ancestryContains(variant, ord2)` is
> true for **every** variant → all anchored → `compareAnchoredMaxvalid` falls straight to
> the slot tiebreak. The guard must fire for the **both-anchored same-ordinal** case, not
> just "neither anchored".

> Tier 1 fixes the **observed** #45 (single-node churn → one variant per ordinal). Tier 2
> is defense-in-depth for the **cross-node** sibling case (duty rotation across a stalled
> ordinal) — but see §9: fold-stickiness makes the *local* choice arrival-order-dependent,
> which is itself a cross-node hazard, so Tier 2 needs its own design pass. **Recommend:
> ship Tier 1 first, re-validate on the gate, then design Tier 2 only if cross-node
> rotation siblings actually manifest** (Tier 1 + the existing #42 anchor-heal may suffice).

---

## §4 Determinism, safety & liveness analysis

- **Determinism / #261 split-safety:** unchanged. The verify/adopt path
  (`ShardCheckpointGl0AcceptanceManager.verifyEmbedded`) reads the wire-carried
  `gl0AnchorOrdinal` / `epoch` and re-derives byte-identically; pinning the anchor only
  *reduces* the set of hashes in flight. No node adopts a checkpoint another would reject.
- **Liveness:** improved. With one stable hash per ordinal, `kQuorum` attestations
  concentrate, `tCountShard` fires, gl0 adopts, the watermark advances, the pipeline gate
  opens, the next ordinal mints. The boot-window adoptions (shardOrd 1–2) already prove
  the path works when identities are stable; Tier 1 makes *every* ordinal behave like the
  boot window.
- **Censorship / safety under a stuck producer:** the staircase **rotation** is the
  censorship bound — if the on-duty node is offline/withholding, duty rotates after δ
  slots and the next rank mints. That is unchanged; I1 only stops *one* node from
  *re-minting the same ordinal*, it does not stop *rotation* from giving a different node
  the window. Cross-node siblings from rotation are exactly the case Tier 2's
  anchor-reorg carve-out (I3) and the #42 healer resolve.
- **Anchor-reorg correctness:** when gl0 adopts hash `H'` at ordinal N that differs from
  the locally-held `H`, `noteAnchor(H')` fires; the held memo for N is cleared/superseded,
  the chain store re-homes onto the anchored lineage, and production resumes from N+1 off
  `H'`. This is the #42 path that #44 reconnected to the live Ref — it should now actually
  fire (it fired 0× in run-19 precisely because nothing reached quorum to *be* an anchor).

---

## §5 Open decisions (for review before coding)

1. **Tier 2 hardness — store rejection vs producer-only.** Recommend the
   **store-level invariant** (I2) *with* the explicit I3 anchor-reorg carve-out, not
   producer-only idempotence. Producer-only leaves the fork-DAG able to flip on a slot
   tiebreak if any path ever re-inserts a sibling; the store invariant is the durable
   guarantee. Risk to manage: do **not** over-reject and break the legitimate anchor-reorg
   replacement — the carve-out must be exact.
2. **Held-memo cardinality.** Use a **single `Ref[F, Option[HeldCheckpoint]]`**, not a
   `Map[ShardOrdinal, Held]` (corrected post-review). The producer's own pipeline gate
   (`ShardCheckpointProducer.scala:321-333`) blocks minting once `unadoptedDepth >=
   pipelineDepth`, and `nextShardOrdinal = bestTip.shardOrdinal.next` is single-valued, so
   the current loop never has two *distinct held* ordinals it would mint in one tick — a
   `Map` adds eviction complexity for unreachable generality. Revisit only if `pipelineDepth`
   semantics change to "mint N ahead of the tip without waiting for self-store".
3. **Re-publish cadence.** What cadence balances loss-recovery against gossip load? The
   run-12 §5.8 precedent re-sends a full prefix every ~6th tick (~30 s ≈ confirmation
   RTT). Recommend a single typed HOCON knob
   (`nakamoto.sharding.checkpoint.republishEveryTicks` or seconds) — **no `sys.env`**
   (`[[feedback-prefer-hocon-over-sysenv]]`).
4. **Anchor staleness bound.** Is an unbounded-stale pinned `gl0Anchor` ever a problem
   (e.g. fee-cutover semantics far behind the embedding ord)? Believed safe by loose
   coupling, but the doc should state an explicit bound or argue none is needed. If a
   bound is wanted, a *re-mint on staleness* must itself be idempotent (new ordinal-scoped
   identity, not a silent churn).
5. **Interaction with `slotGap` / staircase duty.** Pinning `slot` at first-mint means a
   held checkpoint's `slot` no longer tracks wall-clock. Confirm the receiver-side
   `slotGapFor` / `dutyOrder` math reads the **wire** slot (it does — design §5.7,
   owner-corrected) so a held checkpoint's duty window is computed identically everywhere.

---

## §6 Implementation sketch (non-binding — the sub-agent proposal refines this)

- `ShardCheckpointProducer.make`: add `heldRef: Ref[F, Map[ShardOrdinal,
  HeldCheckpoint]]`; branch `produce` on memo hit (re-publish, cadence-gated) vs miss
  (mint-once, pin, store-in-memo). Clear memo entries `≤ adoptedOrd` and on anchor-reorg.
- `ShardCheckpointFanOut.run`: unchanged call shape; the producer internally decides
  mint-vs-republish. (The `producedOrd` it passes is still the live gl0 tip — that's the
  *embedding* target; the producer pins its own anchor on first mint and ignores later
  `producedOrd` values for an already-held ordinal.)
- `ShardChainStore.insert`: add the I2 monotonicity guard in the bestTip recomputation
  fold (sibling-at-canonical-ordinal cannot replace unless anchored). `noteAnchor` already
  exists and re-homes; verify it clears any superseded state cleanly.
- HOCON: one `republish` cadence knob under `nakamoto.sharding.checkpoint`.

## §7 Test plan

- **Unit (`ShardCheckpointProducerSuite`):** drive `produce` repeatedly with an
  *advancing* `producedOrd` and a *non-advancing* `bestTip`/`adoptedOrd`; assert exactly
  **one** distinct checkpoint hash is emitted for the stuck ordinal, and that re-publish
  is cadence-gated. Assert a watermark advance clears the memo and the next ordinal mints.
- **Unit (`ShardChainStoreSuite`):** insert N distinct siblings at one ordinal; assert the
  canonical tip does **not** flip on slot/VRF tiebreak once one is canonical; assert a
  `noteAnchor` of a *different* sibling **does** re-home (I3).
- **e2e gate:** re-run `5gl0 / 2shard / 2mg` (`just clean-data` first; `set -o pipefail`);
  assert `signers≥4` is reached for shardOrd ≥ 3, `adoptedShardOrd` climbs past 2, gl0 sees
  2/2 metagraphs *within* the multi-metagraph 150 s budget, and `currency`/`allow-spends`/
  `spend` proceed. If first-embed is still > 150 s purely from cold-start staircase
  latency, raise the check budget for `numShards > 1` (separate, test-only follow-up).

## §8 Non-goals

- No change to committee sortition, `kQuorum`/`kDraw`, the verify/adopt determinism
  contract, or the metagraph layers.
- No wire-format compatibility ceremony (`[[feedback-greenfield-no-wire-compat]]`) — the
  checkpoint envelope shape is unchanged; this is purely *when/whether* the producer mints
  and *which* sibling the store calls canonical.
- Does **not** address any residual multi-metagraph **timeout** tightness — that's a
  test-harness budget question, tracked separately.

---

## §9 Review addendum (architect sub-agent + code-verified, 2026-06-12)

A Plan sub-agent proposed a file-by-file implementation and critiqued this doc; the
load-bearing claims were re-verified against the code (not relayed). Net decisions:

- **Tier 1 is the primary, well-specified fix and resolves the *observed* #45**
  (single-node anchor churn — run-19's 34 variants from one node). Low risk, deterministic,
  no comparator change. **Ship it first and re-validate on the `5gl0/2shard/2mg` gate.**
- **Memo-clear wiring (verified):** the *adoption* clear is observable today via
  `lastAdoptedOrd`; the *anchor-reorg* clear is **not** observable by the producer today and
  needs new wiring — either thread `lastAdoptedAnchor(shardId)` (confirmed to exist,
  `ShardCheckpointGl0AcceptanceManager.scala:142,220`) or detect tip-moved-from-under-held.
  §3.1 step 4 updated.
- **Tier 2 determinism hazard (verified, critical):** the I2 guard must NOT live in the
  shared `compareAnchoredMaxvalid` — `noteAnchor` reduces it over a `Set[Hash]`
  (`:188`,`:360-362`) and a `0`-tie would resolve by iteration order = a #261 split. Guard
  belongs in the `store` fold (sticky on the established incumbent) only. §3.2 updated.
- **Tier 2 is under-designed for the cross-node case (new finding).** Even fold-stickiness
  makes the *local* canonical pick **arrival-order-dependent** (whichever sibling a node
  receives first becomes its incumbent), which is itself cross-node nondeterministic during
  a gossip race. So Tier 2 does *not* cleanly solve the duty-rotation sibling case; for that
  case the existing `maxvalid-tk` is deterministic but prefers the earliest-slot sibling even
  when it is the unavailable one, and the real resolver is the #42 anchor-heal once gl0
  reaches quorum on one. **Conclusion: Tier 2 is a separate design question, not a same-PR
  add-on.** Implement Tier 1, re-run the gate; only design Tier 2 if cross-node rotation
  siblings actually manifest after the churn is gone.
- **Single `Ref`, not a `Map`** for the held memo (§5 decision 2, corrected): the produce
  loop is single-valued under the pipeline gate.
- **Pinning `slot` is safe (verified):** the staircase `dutyOrder` keys on
  `(shardEta, shardOrdinal)` only — not slot (`ShardSlotLeader.scala:203-213`) — so a pinned
  slot cannot perturb duty; it only fixes the structural `membershipProof` and the
  `maxvalid-tk` tiebreak, both of which want a stable value.
- **Held-window staleness (liveness, open):** a long-held checkpoint pins its
  `includedSnapshots`; if the metagraph re-emits a superseding chain while held, the held
  window can be rejected at embed. Not a determinism bug (everyone re-derives identically).
  Mitigation: also drop the memo if the buffer's chain-link tip diverges, or accept that
  adoption/anchor-reorg eventually re-homes it. Tracked as an open item for Tier 1 testing.
- **Boot grace:** the memo is dormant during a boot-grace window — `produce` isn't called
  then (the FanOut call is gated in `SnapshotLeaderLoop`), so a checkpoint cannot be held
  across one. Benign; noted for completeness.
