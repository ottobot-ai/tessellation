# Nakamoto — Active Work Plan

**Branch:** `feature/nakamoto-stake-registry`
**Last updated:** 2026-04-08 (milestone closed: metagraph e2e on Nakamoto GL0 passing)

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

### 4. MptUndoJournal.unapplyTo wired on reorg  *(SUPERSEDED by MptOverlay — #56.10 removed the journal, 2026-05-06)*
**Status update 2026-05-12:** `MptUndoJournal` was removed in commits `5ecc0772` (G+F: delete) / `caa3559e` (D: GSAM accept() migrated through writer algebra). Fork-switch under the production-default `OverlayMode.MultiBranch` (since #56.11 / `e3538d9b`) is handled via `MptOverlay` branch checkout/discard, not journal replay. Per-branch pending writes live in the overlay's ChangeSets; commit/discard happens at finality. The "branch-aware proofs" deferral below is also resolved (`proofAtBranch` via `overlay.buildRoot` + stateless prover, #56.7 / `3bed33e7`). The historical-context paragraphs below are preserved for reference but reflect the pre-#56.10 architecture.

**Historical context (pre-#56.10):** self-healing already handles reorgs. Commit `21cec6de` wired the fallback: when MPT detects divergence post-reorg, it triggers a full rebuild from the canonical chain. The journal would be the fast path (O(reorg_depth) undo) vs self-healing's slow path (O(state_size) full rebuild). Both are correct; the journal is a performance optimization.

**Why the wire-up isn't clean today:** the reorg in `NakamotoChainStore.store` is detected AFTER the incoming fork's MPT mutations have already been applied earlier in the snapshot acceptance pipeline (validation walks `GlobalSnapshotAcceptanceManager` → `mptStore` → `wrapApply`). To call `unapplyTo` correctly we'd need to reorder the validation path: detect reorg BEFORE MPT mutation, compute common ancestor across forks, unapplyTo, then let the new fork apply forward. Half-day of work, well-defined, but speculative until we see self-healing be a bottleneck.

**What stays in place meanwhile:** journal recording (`wrapApply`) still runs — deltas accumulate, just no consumer. Cheap and harmless to keep maintained.

**Why we WILL need the journal eventually (not just for performance):**
- **Inclusion proofs for leaves at historical ordinals.** Producing a Merkle proof that a particular state leaf was present at ordinal N requires reconstructing the trie root *as it existed at N*. With a content-addressed MPT this is trivial (root pointer per ordinal). With our current mutable in-memory MPT, the journal is the only mechanism that lets us walk backwards from "now" to ordinal N's state without replaying the entire chain. Expected use cases: light-client proofs, fraud proofs, cross-metagraph state attestations, NIPoPoW witness generation.
- This is a **functional requirement**, not just an optimization — the journal becomes load-bearing for any feature that needs "state at past ordinal X" proofs.

**Revisit triggers (in order of likelihood):**
1. Metagraph end-to-end testing (#7) reveals self-healing rebuild is a bottleneck on reorgs at test cluster scale → wire `unapplyTo` for performance
2. Inclusion-proof feature work begins → wire `unapplyTo` for correctness on the read side, plus add an `applyTo(ordinal)` API to walk forward from a checkpoint
3. Content-addressed MPT migration → the whole problem dissolves; journal becomes unnecessary

### 5. Parametrize finality (no mode switch)  *(✅ done — updated 2026-05-08 to k=255 after expanded sims)*
Three env-var knobs with sensible defaults — `NAKAMOTO_ATTESTATION_THRESHOLD` (default 2/3, in `TipTracker.FinalityThreshold`), `NAKAMOTO_CONFIRMATION_DEPTH` (default **255**, in `SnapshotLeaderLoop.ConfirmationDepthK`), `NAKAMOTO_OPTIMISTIC_MIN_FRACTION` (default 0.5, in `StakeRegistry.MinActiveQuorumFraction`). Both gates always run; whichever fires first finalizes. No mode switch.

**k measures snapshots, not slots.** With LDD targeting ~15% slot fill, slots run ~6× sparser than snapshots, but the depth gate is purely an ordinal-distance check: `tip.ordinal - snapshotOrdinal > k`. The original k=31 choice (2026-04-08) was based on a measured-sim table topping out at k≤80 with k=6 too high a fork rate against a 1/3 adversary; k=31 gave ~0.91% per-attempt. Subsequent expanded sims (`adv_depth_expanded_parallel.py`, 10M trials, k≤400) extrapolate the fB=0.05-tail slope to ~10⁻¹² at k≈271–290. Default raised to **k=255** as a conservative operating point approximating Cardano-equivalent CP-violation; attestation finality remains the hot path (seconds), so this only affects worst-case finality time during degraded operation.

**Two slot/ordinal-units bugs were fixed in this round** (2026-04-08), discovered while validating the wire-up: `lastFinalizedOrdinal` was being read off `tipTracker.lastFinalized`'s **slot** value, and `finalizeAtSlot` was being computed as `tip.slot - k` (mixing slot- and ordinal-units). The first bug had silently disabled the depth gate end-to-end since it landed — in our 720s e2e test we observed 224 ATTEST-FINALIZED entries and **zero** DEPTH-FINALIZED entries. Both gates now read their inputs from the chain store, which is the authoritative ordinal source.

### 6. Close SC binary finality loop (CL0-side)  *(✅ done)*
**Bug:** original `pruneConfirmed` dropped a binary on first sight in any GL0 snapshot — if that snapshot was later orphaned in a Nakamoto reorg, the binary was permanently lost.

**Fix landed:**
- **Data model:** `BinaryTracker.pruneFinalizedBelow(SnapshotOrdinal)` only prunes ConfirmedBinary entries whose `proof.globalOrdinal <= lastFinalizedGlobalOrdinal`. `StateChannelBinarySender.confirm` gained an optional `lastFinalizedGlobalOrdinal: Option[SnapshotOrdinal]` parameter defaulting to the snapshot's own ordinal (BFT-preserving).
- **GL0 endpoint:** new `GET /global-snapshots/latest/finalized-ordinal` route on `SnapshotRoutes`. In Nakamoto mode, dag-l0 wires it to a `Ref[F, Long]` that `SnapshotLeaderLoop` updates after every successful `chainStore.finalize` call (depth-k or attestation-2/3, whichever fires first). In BFT mode the route defaults to head ordinal — semantically correct since BFT snapshots are immediately final.
- **CL0 caller:** `StateChannel.scala:172` now calls `services.globalL0.pullLatestFinalizedOrdinal` (best-effort, falls back to legacy snapshot-own-ordinal on error) and passes the result into `stateChannelBinarySender.confirm`.

**Validated live:** in the metagraph e2e (#7 below), `GET /global-snapshots/latest/finalized-ordinal` returns a real value (`{"value":159}`) at end-of-test, proving the route is reachable, the Ref is being updated, and CL0 is consuming it.

### 7. Metagraph end-to-end via `just`  *(✅ done)*
Updated `just test` to launch CL0 + DL1 against a Nakamoto GL0 cluster (`--use-test-metagraph --num-gl0=3 --nakamoto-gl0`). Currency e2e test suite (DAG transfers + L0 token transfers + double-spend prevention for both) runs to completion in **622s** test time / **720s** total against 3-node Nakamoto GL0 + sidecars + 3 GL1 + 2 ML0 + 3 CL1 + 3 DL1.

**Also validated:**
- Sidecar gossip migration end-to-end: BFT consensus rumors and Tessellation events both flow through Go libp2p GossipSub (no legacy HTTP gossip) and CL0 BFT consensus still reaches finality.
- DHT-only peer discovery: Go sidecar `-disable-mdns` flag forces all peer discovery through Kademlia, validating multi-host readiness (mDNS cannot cross subnets).

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
