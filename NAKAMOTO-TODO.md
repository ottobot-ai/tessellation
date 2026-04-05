# Nakamoto Consensus — Status & Remaining Work

**Branch:** `feature/nakamoto-stake-registry` (62 commits, 71 files, ~9,800 lines)  
**Last updated:** 2026-04-05

---

## ✅ Completed (Phases 0-7 + extras)

| Feature | Commit | Tests |
|---------|--------|-------|
| VRF crypto (ECVRF-ED25519-SHA512-TAI) | PR #4 merged | 43 tests |
| Slot clock + LDD snowplow eligibility | PR #5 | 20+ tests |
| StakeRegistry, EpochState, SlotCertificate | PR #6 | 30+ tests |
| Go libp2p sidecar (GossipSub, mDNS, gRPC) | — | Manual |
| NakamotoProposer + snapshot production | — | — |
| Attestation + dual finality (attest + depth k=6) | — | — |
| Fork choice / ChainSelection (Bifrost-style density) | 92cab6e1 | — |
| ParentChildTree + reorg support | 92cab6e1 | — |
| Chain-derived eta (replaces accumulator) | 9b1ede57 | — |
| MPT stateProof determinism (full-rebuild workaround) | b7489986 | — |
| Self-healing incremental MPT (detect + resync on fork) | 21cec6de | — |
| MptUndoJournal (per-ordinal delta tracking) | 289bcece | — |
| Content validation enforcement | 289bcece | — |
| NakamotoSnapshotValidator (4-stage: VRF+sig+cert+content) | — | — |
| /latest/info endpoint for validators | 5a09af8f | — |
| RunNakamotoValidator (join running cluster) | 5a09af8f | 4-node test |
| ProductionGate (pausable leader election) | bc18ffba | — |
| Better-gossip-received gate trigger | 3bf8db7c | — |
| Cold restart recovery (resume from disk) | 60ff0af4 | 3-node test |
| Single node catch-up (gossip-based) | 60ff0af4 | Tested |
| Optimistic attestation finality (dynamic cluster) | 35e4b8a4 | 3/4 seedlist test |

**Test cluster validated:** 3-node genesis + 1 validator joining mid-chain, cold restart, single-node restart, reorgs, fork convergence, MPT determinism, attestation finality.

---

## 🔧 Implementation Still Needed

### High Priority — Before Testnet

1. **Hard Fork Migration Mechanism**
   - Dual-mode dispatch in ConsensusManager (BFT below fork ordinal, Nakamoto above)
   - Load existing chain state (balances, state channels, metagraph snapshots) at fork point
   - Genesis time derivation from fork ordinal + slot duration
   - `epochProgress` continuation: `forkEpochProgress + floor((currentSlot - forkSlot) / 60)`
   - This is the biggest remaining piece — all current testing uses fresh genesis

2. **Disable BFT Daemons in Nakamoto Mode**
   - ConsensusStateAdvancer, FacilitatorSelector, StallDetector, AbandonmentTracker
   - ViewChangeManager, 5-phase round machinery
   - Must not run concurrently with Nakamoto production
   - Should be clean — check nodeState or config flag

3. **Metagraph (CL0/DL1) Nakamoto Support**
   - Current implementation is GL0-only
   - CL0 needs same VRF+LDD+attestation pattern (1:1 component reuse per scope doc)
   - DL1 (data metagraphs) — same pattern
   - Per-metagraph GossipSub topics: `/tessellation/cl0/<metagraph-id>/snapshots`

4. **Gossip Layer: Replace Tier 1/2 with Sidecar**
   - Current: sidecar handles Nakamoto topics only, old gossip still runs for everything else
   - Target: all gossip through sidecar (events, blocks, peer discovery)
   - Need: Event gossip via GossipSub, Kademlia DHT for peer discovery (replace seedlist-only)
   - Sidecar currently uses mDNS (local only) — needs bootstrap nodes for multi-host

5. **Stake-Proportional VRF**
   - Currently equal-weight (`1/N`) — every seedlist peer has same probability
   - Target: `threshold = 1 - (1 - f(δ))^relativeStake`
   - Read from `GlobalSnapshotInfo.activeDelegatedStakes` + `activeNodeCollaterals`
   - StakeRegistry.stakeWeighted already stubbed

### Medium Priority — Testnet Hardening

6. **Proactive MPT Rollback on Fork Switch**
   - MptUndoJournal exists and records deltas, but `unapplyTo` not yet wired in NakamotoSyncDaemon
   - Currently relies on self-healing (detect divergence → full resync)
   - Need: on reorg in chainStore, call `journal.unapplyTo(commonAncestorOrdinal)` before accepting new fork
   - Required for testnet scale (100MB state, 80k trie entries — full resync too expensive)

7. **Production Abandonment on Better Gossip**
   - ProductionGate infrastructure exists but threshold-based abandonment not wired
   - Need: ChainSelection.compare incoming vs in-progress, abandon if incoming wins
   - SnapshotLeaderLoop checks Deferred at pre-sign and pre-publish checkpoints

8. **Genesis Time Discovery for Validators**
   - Currently requires `NAKAMOTO_GENESIS_TIME_MS` env var
   - Should derive from peer API (e.g., slot certificate + slot duration in /latest response)
   - Or persist to shared state accessible via `/cluster/genesis-time` endpoint

9. **Mempool Reinsertion on Finalize**
   - Events from orphaned fork branches need recycling back to mempool
   - State-dependent revalidation against finalized state
   - `claimedBy` tracking for pending vs orphaned events
   - NakamotoChainStore.finalize currently prunes chains but doesn't recycle events

10. **Configurable Finality Mode**
    - Small clusters (≤3): depth-only (attestation can't reach 2/3 with 2 nodes)
    - Medium clusters: optimistic attestation (current implementation)
    - Large clusters: VRF-sortitioned attestation committees (future)
    - Config flag: `nakamoto.finality = { mode: attestation | depth | auto }`

### Low Priority — Post-Testnet

11. **Content-Addressed MPT (Ethereum-style)**
    - Current: mutable in-memory `InMemoryMerklePatriciaProducer` with stateRef
    - Target: content-addressed trie nodes in KV store keyed by hash
    - Fork switching = change root pointer, structural sharing, GC unreachable
    - Eliminates all MPT non-determinism issues permanently
    - Biggest refactor but best long-term solution

12. **Superblock Proofs (NIPoPoW-style)**
    - LDD's slot-gap threshold serves as difficulty certificate
    - Enables logarithmic light client proofs (~5,100 headers vs 1.43M full chain)
    - Research track from dilf4s paper — deferred

13. **VRF-Sortitioned Attestation Committees**
    - Full-set attestation doesn't scale past ~100 nodes
    - VRF-based committee selection for attestation duty
    - Smaller, rotating committees with same security guarantees

14. **Two-Level Finality (GL0 + Metagraph)**
    - Metagraph-local fast finality + GL0 hard finality
    - Cross-metagraph operations wait for GL0
    - Metagraph snapshot resubmission on GL0 orphaning

---

## 📊 Scope Doc vs Implementation

| Scope Doc Section | Status |
|-------------------|--------|
| §1 Problem Statement | ✅ Understood |
| §2 LDD Heartbeat | ✅ Implemented (fA=0.5, fB=0.05, γ=15) |
| §3 Epoch Progress | ⚠️ Not implemented (using slot numbers directly) |
| §4 What Changes | ✅ All removals identified, not yet disabled |
| §5 VRF Key Derivation | ✅ Implemented (SHA-512 domain separation) |
| §6 Attestation & Finality | ✅ Implemented + optimistic dynamic sizing |
| §7 Two-Level Finality | ❌ GL0 only — metagraph deferred |
| §8 Mempool Reinsertion | ❌ Not implemented |
| §9 Network Layer (sidecar) | ⚠️ Partial — Nakamoto topics only, old gossip still runs |
| §10 Staking Model | ⚠️ Equal-weight only |
| §11 Migration Strategy | ❌ Not implemented — biggest remaining piece |
| §12 Superblocks | ❌ Deferred research track |
| §13 Phases | ✅ Phases 0-7 complete |
| §14 Open Questions | Partially addressed (Q2 solved by optimistic finality) |

---

## 🧪 Test Infrastructure

- `nakamoto-test/docker-compose.yml` — 3 genesis + 1 validator, Go sidecars, seedlist
- `nakamoto-test/test-validator.sh` — automated 4-node join test
- Manual test scripts for cold restart, single-node restart, reorg scenarios
- 647+ unit tests passing (VRF, LDD, ChainSelection, Proposer, etc.)
