# Nakamoto — Active Work Plan

**Branch:** `feature/nakamoto-stake-registry`
**Last updated:** 2026-04-08

Companion to `NAKAMOTO-TODO.md` (full backlog). This file tracks the in-flight workstream toward metagraph end-to-end on Nakamoto GL0.

---

## Goal

Get **Nakamoto GL0 + new gossip** production-ready, then run a **metagraph end-to-end test** (CL0 + DL1) against it via `just` infra.

Hard fork migration is **deferred** — network can be force-forked. Stake-weighted VRF is **deferred** — equal weight for now. CL0 keeps **BFT consensus** but rides the **new sidecar gossip transport**.

---

## Workstream (in order)

### 1. Sidecar gossip — full migration  *(✅ code complete, runtime validation pending)*
Ported event gossip and BFT consensus channels onto the Go libp2p sidecar via a single generic `Rumor` topic. Added Kademlia DHT for decentralized peer discovery. Runtime validation tracked in task #8.

**What landed:**
- Sidecar `/tessellation/rumors/1.0.0` GossipSub topic + `PublishRumor` gRPC RPC
- `SidecarRumorBridge`: outbound `publishFn` (wired into `Gossip.setSidecarPublishFn`) and inbound `receive` daemon (parses `Signed[RumorRaw]` JSON, recomputes hash, offers to `rumorQueue`)
- Wired into `GlobalSnapshotConsensus` startup after `SidecarClient` allocation
- `GossipDaemon.make` accepts `nakamotoMode: Boolean` — when true, skips legacy peer/common round runners (only `consumeRumors` runs)
- Kademlia DHT in server mode in the Go sidecar; rendezvous-based discovery loop (`tessellation-nakamoto`); seedlist becomes bootstrap nodes
- All four Scala modules compile; Go sidecar builds

**Why this design wins:** because BFT consensus rumors and Tessellation events both flow through `Gossip.spread → rumorQueue → consumeRumors → RumorHandler.run`, the bridge plugs in at `Gossip.spread` (outbound) and `rumorQueue` (inbound). CL0 BFT messages get sidecar transport for free with zero CL0-side changes.

### 2. Genesis time as config param  *(✅ done)*
Centralized into a single `nakamotoGenesisTimeMs: Long` val on `GlobalSnapshotConsensus`. Resolved once at process start, env override preserved (`NAKAMOTO_GENESIS_TIME_MS`), default falls back to system time for single-node dev. Per-cluster contract documented in scaladoc. Chain-derived genesis time deferred.

### 3. Production abandonment on better gossip  *(✅ done)*
Three checkpoints in `SnapshotLeaderLoop`: (1) slot-tick gate (already existed), (2) **new pre-sign gate** between `createProposalArtifact` and signing — when closed, `chainStore.store` is skipped via `gateOpenPreSign` flag, propagating through downstream `whenA(stored)` gates, (3) pre-publish gate (already existed). Also fixed a small bug: `eventMempool.clearIncluded` was unconditional and would wrongly clear events when production was abandoned — now also gated on `stored`.

### 4. MptUndoJournal.unapplyTo wired on reorg  *(deferred — see notes below)*
**Why deferred (not a correctness gap right now):** self-healing already handles reorgs. Commit `21cec6de` wired the fallback: when MPT detects divergence post-reorg, it triggers a full rebuild from the canonical chain. The journal would be the fast path (O(reorg_depth) undo) vs self-healing's slow path (O(state_size) full rebuild). Both are correct; the journal is a performance optimization.

**Why the wire-up isn't clean today:** the reorg in `NakamotoChainStore.store` is detected AFTER the incoming fork's MPT mutations have already been applied earlier in the snapshot acceptance pipeline (validation walks `GlobalSnapshotAcceptanceManager` → `mptStore` → `wrapApply`). To call `unapplyTo` correctly we'd need to reorder the validation path: detect reorg BEFORE MPT mutation, compute common ancestor across forks, unapplyTo, then let the new fork apply forward. Half-day of work, well-defined, but speculative until we see self-healing be a bottleneck.

**What stays in place meanwhile:** journal recording (`wrapApply`) still runs — deltas accumulate, just no consumer. Cheap and harmless to keep maintained.

**Why we WILL need the journal eventually (not just for performance):**
- **Inclusion proofs for leaves at historical ordinals.** Producing a Merkle proof that a particular state leaf was present at ordinal N requires reconstructing the trie root *as it existed at N*. With a content-addressed MPT this is trivial (root pointer per ordinal). With our current mutable in-memory MPT, the journal is the only mechanism that lets us walk backwards from "now" to ordinal N's state without replaying the entire chain. Expected use cases: light-client proofs, fraud proofs, cross-metagraph state attestations, NIPoPoW witness generation.
- This is a **functional requirement**, not just an optimization — the journal becomes load-bearing for any feature that needs "state at past ordinal X" proofs.

**Revisit triggers (in order of likelihood):**
1. Metagraph end-to-end testing (#7) reveals self-healing rebuild is a bottleneck on reorgs at test cluster scale → wire `unapplyTo` for performance
2. Inclusion-proof feature work begins → wire `unapplyTo` for correctness on the read side, plus add an `applyTo(ordinal)` API to walk forward from a checkpoint
3. Content-addressed MPT migration → the whole problem dissolves; journal becomes unnecessary

### 5. Parametrize finality (no mode switch)  *(✅ done)*
Three env-var knobs with sensible defaults — `NAKAMOTO_ATTESTATION_THRESHOLD` (default 2/3, in `TipTracker.FinalityThreshold`), `NAKAMOTO_CONFIRMATION_DEPTH` (default 6, in `SnapshotLeaderLoop.ConfirmationDepthK`), `NAKAMOTO_OPTIMISTIC_MIN_FRACTION` (default 0.5, in `StakeRegistry.MinActiveQuorumFraction`). Both gates always run; whichever fires first finalizes. No mode switch.

### 6. Close SC binary finality loop (CL0-side)  *(✅ data model done, wire-up deferred to #7)*
**Bug:** original `pruneConfirmed` dropped a binary on first sight in any GL0 snapshot — if that snapshot was later orphaned in a Nakamoto reorg, the binary was permanently lost.

**Fix landed (data model):** `BinaryTracker.pruneFinalizedBelow(SnapshotOrdinal)` only prunes ConfirmedBinary entries whose `proof.globalOrdinal <= lastFinalizedGlobalOrdinal`. `StateChannelBinarySender.confirm` gained an optional `lastFinalizedGlobalOrdinal: Option[SnapshotOrdinal]` parameter defaulting to the snapshot's own ordinal (BFT-preserving). Tests + main compile.

**Wire-up remaining (part of #7):** GL0-side endpoint exposing the actual finalized ordinal needs to land, and the CL0 caller in `StateChannel.scala:172` must pass it through. Without this, metagraphs running against Nakamoto GL0 will still drop binaries on reorg — that's why this lives inside the metagraph end-to-end milestone now.

### 7. Metagraph end-to-end via `just`  *(milestone — next focus)*
Update `just` and docker infra to launch CL0 + DL1 against the Nakamoto GL0. Run an existing metagraph end-to-end test. Fix what breaks. Includes the SC binary finality wire-up from #6 above.

---

## Decisions locked in this session

| Topic | Decision |
|---|---|
| CL0 consensus | Keep BFT, ride new sidecar gossip |
| Stake-weighted VRF | Deferred — equal weight `1/N` |
| Hard fork migration | Deferred — force-fork the network |
| Genesis time discovery | Config param baked into binary |
| Mempool reinsertion (DAG txs) | Not pursuing GL0 reinsertion |
| SC binary reorg recovery | CL0-side: wait for GL0 finality before pruning |
| Configurable finality | Parametrize knobs only — no mode switch |

---

## Out of scope this round
- Metagraph (CL0/DL1) Nakamoto consensus port — staying BFT
- Hard fork migration / dual-mode dispatch
- Stake-proportional VRF
- Content-addressed MPT
- NIPoPoW superblocks
- VRF-sortitioned attestation committees
- Two-level finality
