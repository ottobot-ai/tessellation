# Nakamoto Consensus — Status & Remaining Work

**Branch:** `feature/committee-state-diff` (2575 commits on branch; HEAD `5557ee084`)
**Last updated:** 2026-07-10 (status sweep) — previously 2026-04-05

---

> ### ⚠ 2026-07-10 status-sweep note — READ THIS
> This file was stale (last real edit 2026-04-05, at ~commit 62; the branch is now at 2575 commits). This sweep re-annotated status by cross-referencing **git log subjects + engineering notes**, NOT by re-reading each feature's code. Trust the flags accordingly:
> - **✅ done** — already validated, or a clear landing commit + prior confirmation.
> - **⚠ landed-per-git (unverified)** — a commit subject says it landed, but I did NOT re-verify the implementation at source this sweep. Confirm before relying.
> - **❓ unknown** — status genuinely unclear; needs a check.
> - **⏳ planned / pending** — no evidence it's done; still on the roadmap.
> - **♻ superseded** — replaced by newer design; the replacement is named. **Nothing has been deleted.**
>
> **Biggest change since 2026-04-05:** the project moved into **execution-sharding + committee-based metagraph economic security** — the sharded-currency-mirror / committee-re-execution / watchtower-slash / cross-shard workstream. It dominates the last ~2000 commits and was entirely absent from this TODO. It now has its own section below (**🧩 Execution-Sharding & Committee Security**). Canonical architecture: **`docs/adr/0016-execution-sharding-reexecution-and-cross-shard-reads.md`**. Ground-truth for "what's actually wired" is being captured in a review handoff under `docs/review/`.

---

## ✅ Completed (Phases 0-7 + extras) — from the original sweep, unchanged

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
| ~~MptUndoJournal (per-ordinal delta tracking)~~ — REMOVED in #56.10 (`5ecc0772`, 2026-05-06); replaced by `MptOverlay` branch checkout/discard. | 289bcece (intro); `5ecc0772` (removal) | — |
| Content validation enforcement | 289bcece | — |
| NakamotoSnapshotValidator (4-stage: VRF+sig+cert+content) | — | — |
| /latest/info endpoint for validators | 5a09af8f | — |
| RunNakamotoValidator (join running cluster) | 5a09af8f | 4-node test |
| ProductionGate (pausable leader election) | bc18ffba | — |
| Better-gossip-received gate trigger | 3bf8db7c | — |
| Cold restart recovery (resume from disk) | 60ff0af4 | 3-node test |
| Single node catch-up (gossip-based) | 60ff0af4 | Tested |
| Optimistic attestation finality (dynamic cluster) | 35e4b8a4 | 3/4 seedlist test |

**Test cluster validated (as of original sweep):** 3-node genesis + 1 validator joining mid-chain, cold restart, single-node restart, reorgs, fork convergence, MPT determinism, attestation finality.

---

## 🧩 Execution-Sharding & Committee Security (dominant workstream since 2026-04)

> The model: **metagraph *processing* is sharded** (global layer stays universal). A VRF-sortitioned **shard committee** RE-EXECUTES metagraph currency-L1 (CL1) economic ops, produces the re-executed state (byteDiff) + shard checkpoint, and ships both. **Watchtowers** re-exec-check → `InvalidStateProof` slash. Other gl0 nodes adopt + verify the stateProof. Custom data-L1 (DL1) logic is proof-carried (GL0 can't run it). See `docs/adr/0016`. **All ⚠ statuses below are "landed per git-log subject, wiring/enforcement being verified in the `docs/review/` handoff" unless noted.**

- ⚠ **Sharded-security substrate** — pinned `globalSyncView`, PIN-1 component-addressable per-MG MPT roots, I-ONCE spent-set, watchtower fraud-proof scaffold (`8ac7ce04f`).
- ⚠ **Sharding wiring complete** — real committee-VRF verify + W3c sharded-validator activation (`025a58688`).
- ⚠ **Committee sortition** — VRF over operator keys; slot-ID = lastSnapshotHash (`CommitteeSortition.scala`; decision recorded).
- ⚠ **Committee re-execution → shard checkpoint + byteDiff** (`ShardCheckpointProducer.scala`, `ShardCommitteeReExecutionSuite`). **← verify in handoff.**
- ⚠ **Watchtower InvalidStateProof slash — durable ledger** (Slashings fieldId 34) (`ed8928b81`); durably slash a full-quorum colluding committee on one honest fraud proof (`68246cffe`, W3a). **Wired-vs-shelfware being verified in handoff.**
- ⚠ **Slash-cooldown committee exclusion** — FINDING-002 loop closed (`893ed5351`, EPIC-3.1/3.4).
- ✅ **Cross-shard framework reads = Option A (finality-first)** — VERIFIED at source 2026-07-10: gl0 accept reads cross-shard values off gl0's consensus-pinned FINALIZED mirror (`gl0Local`, `GlobalSnapshotAcceptanceManager` ~L2542; `0b7902d69`). Consuming CL1 op still re-executed. Locked in `docs/adr/0016`.
- ⚠ **Atomic cross-shard allow-spend settlement (I-ONCE)** via generic cross-shard-message seam (`f368e064e`); W3c cross-shard proof-path effective-balance overlay (`4081ef0d4`).
- ⚠ **Sharded-currency-mirror store-fidelity stack** — diff-base-pin never reads live store (`a6d7d045b`, `eb9bf9a5e`), stage-adopted-bytes into signed store (`4b3ac1cac`), read-time peer backfill for holes (`5557ee084`), by-ordinal `PinnedCurrencyInfoReader` (`e2f266ff1`).
- ⚠ **gl0 adopts ml0's authoritative balances/allow-spends/token-locks** (verified vs proof) — data-with-fee (`95a19a35c`, `8b0743296`, `671c434b1`).
- ⚠ **Shard-checkpoint chain-sync (pull-based recovery)** (`df5b7b0e0`, `d47a4403f`); FINALIZED-anchored shard checkpoints (`eb15e19c0`).
- ⚠ **ml0 unified chain-based consensus engine** — rotation + solo-extension + gl0-anchor under one seam (`add65b620` design; decision recorded).
- ⚠ **Go↔JVM gossip transport for shard-checkpoint + fraud-proof legs** (`72f39d652`, F1/F2/F8); sidecar carries l1-block topics + self-delivery (`b0ca67908`).
- 🔴 **RECURRING WEDGE / current frontier — metagraph committee-gate parent-ordinal resolution.** `MetagraphCommitteeGate` / `MetagraphParentOrdinalResolver` / `MetagraphOrphanBuffer`; resolver→None on GSI-tip-lag → orphan re-buffer loop. Prior fix "walk-finalized-not-bestTip" (`40d546761`) recurred; blocks the 2mg/2shard token-lock e2e. **Must be fixed WITHIN the re-exec model.**
- ❓ **ENFORCEMENT GAP (open, top question) — re-exec-before-finalization on the gl0 happy path.** Notes indicate gl0's happy path accepts a shard checkpoint on committee **quorum signature** with re-exec-before-final apparently ABSENT (`ShardCheckpointGl0AcceptanceManager`, ~L386-396). The watchtower re-exec-check is the intended backstop; whether it gates finality is being verified in the handoff. **This is the #1 thing for external review.**

---

## 🔧 Implementation Still Needed (original items, re-annotated 2026-07-10)

### High Priority — Before Testnet

1. ⏳ **Hard Fork Migration Mechanism** — no evidence of landing; still the biggest pre-testnet piece. Notes confirm hard-fork is sequenced LAST. Keep.
   - Dual-mode dispatch in ConsensusManager (BFT below fork ordinal, Nakamoto above)
   - Load existing chain state (balances, state channels, metagraph snapshots) at fork point
   - Genesis time derivation from fork ordinal + slot duration
   - `epochProgress` continuation: `forkEpochProgress + floor((currentSlot - forkSlot) / 60)`

2. ✅ ~~**Disable BFT Daemons in Nakamoto Mode**~~ (af15c077, bde17768, 5ec6ac46) — unchanged.

3. ♻⚠ **Metagraph (CL0/DL1) Nakamoto Support** — original framing ("CL0 needs same VRF+LDD+attestation 1:1") **superseded** by the execution-sharding + committee model above. Metagraph consensus (ml0 rotation, shard checkpoints, cl1/dl1 adopt) is substantially built per git-log; confirm scope vs the original 1:1 intent. Per-metagraph GossipSub topics exist (sidecar). **← see Execution-Sharding section.**

4. ⚠ **Gossip Layer: Replace Tier 1/2 with Sidecar** — substantial progress: sidecar now carries Nakamoto + l1-block + shard-checkpoint + fraud-proof topics; Go↔JVM transport hardened (`72f39d652`, `461a34830`, `ad3f01092`). **Not confirmed:** ALL gossip through sidecar; Kademlia DHT peer discovery (still seedlist/mDNS?). Keep open items.

5. ❓ **Stake-Proportional VRF** — delegated-stake state landed and read from MPT (`b39356521` withdrawal reads active stakes; delegated-staking e2e). **Unverified:** whether the VRF eligibility threshold is actually stake-weighted (`1-(1-f)^relativeStake`) vs still equal-weight. NEEDS SOURCE CHECK.

### Medium Priority — Testnet Hardening

6. ♻ ~~**Proactive MPT Rollback on Fork Switch**~~ — superseded by `MptOverlay` (#56); MultiBranch overlay default since `e3538d9b`; revert-executor `MptOverlay.revertToOrdinal` (`3e47d1904`). Self-healing remains fallback.

7. ⚠ **Production Abandonment on Better Gossip** — ProductionGate + better-gossip trigger exist; finality-trigger stack landed. **Unverified:** threshold-based abandonment fully wired (ChainSelection.compare incoming vs in-progress at SnapshotLeaderLoop checkpoints).

8. ⚠ **Genesis Time Discovery for Validators** — genesis time/eta moved to typed HOCON (`d7a4212d5`). **Unverified:** peer-API discovery (`/cluster/genesis-time` or slot-cert in `/latest`) vs still config-supplied. Note: eta bootstrap for periods 0&1 now genesis-derivable (`45066b4b0`).

9. ⏳ **Mempool Reinsertion on Finalize** — no clear landing commit found. Round-cancellation logging landed (`948d2b2e1`) but event recycling on orphan/finalize appears unimplemented. Keep.

10. ⚠ **Partition Recovery (Fork Recovery)** — heavily worked and reported PASSING in recent 2mg/2shard runs: deep-catch-up byte-faithful adopt (`e310f6ccd`), Tier-3 catch-up livelock fixes (`24e93ee10`, `61ede3a0e`), shard chain-sync pull recovery + adopted-stall trigger (`df5b7b0e0`, `d47a4403f`). Confirm per-sub-item:
    - **10a.** Sidecar GossipSub mesh re-establishment after partition — ❓ verify (sidecar reconnection/mesh re-graft).
    - **10b.** Backfill trigger on parent-not-found — ⚠ NakamotoSyncDaemon backfill exists; confirm auto-trigger.
    - **10c.** ProductionGate stale-fork detection / pause-if-behind — ⚠ confirm.

11. ⚠ **Configurable Finality Mode** — finality-trigger stack (T_count + T_depth) + typed HOCON consensus params landed (`d7a4212d5`). **Unverified:** the explicit `nakamoto.finality = {mode: attestation|depth|auto}` selector.

### Low Priority — Post-Testnet

12. ⚠ **Content-Addressed MPT (Ethereum-style)** — MPT-as-primary substrate substantially built (signed-byte store, `MptStore.loadBytes`, by-ordinal readers, serve+rebuild `082ed24e7`). **Unverified:** full content-addressed trie-node KV store keyed by hash. Storage-representation migration, NOT re-execution.

13. ⏳ **Superblock Proofs (NIPoPoW-style)** — level-µ design landed (notes: one VRF, L domain-separated rehashes); hexary-MPT absence-proof WIP (`690b0f259`, task #286). Implementation deferred; light-client DEMO sequenced post-slashing/KES.

14. ⚠ **VRF-Sortitioned Attestation Committees** — VRF-sortitioned committees ARE implemented for **shard-checkpoint** duty (CommitteeSortition, real committee-VRF verify `025a58688`). **Open:** applying the same to **global attestation** duty (the original item's scope).

15. ⚠ **Two-Level Finality (GL0 + Metagraph)** — substantially built: metagraph shard-checkpoints with gl0-embed finality (`b3edc4eef` bounded pipeline holds until gl0 embed; `eb15e19c0` FINALIZED-anchored). Cross-shard waits for gl0 finality (ADR-0016 Option A). **Unverified:** metagraph snapshot resubmission on GL0 orphaning.

---

## 📊 Scope Doc vs Implementation (re-annotated 2026-07-10)

| Scope Doc Section | Status |
|-------------------|--------|
| §1 Problem Statement | ✅ Understood |
| §2 LDD Heartbeat | ✅ Implemented (fA=0.5, fB=0.05; shard-tuned variants exist, e.g. γ=45 per-slot) |
| §3 Epoch Progress | ⚠ Now used in acceptance (`metagraphPinnedEpochProgresses`, epochProgress); eta bootstrap landed (`45066b4b0`). Confirm full §3 semantics. |
| §4 What Changes | ✅ Removals identified; BFT daemons disabled in Nakamoto mode |
| §5 VRF Key Derivation | ✅ Implemented (SHA-512 domain separation) |
| §6 Attestation & Finality | ✅ Implemented + optimistic dynamic sizing + finality-trigger stack |
| §7 Two-Level Finality | ⚠ Substantially built via sharding/committee + gl0-embed (was ❌ GL0-only) |
| §8 Mempool Reinsertion | ⏳ Not implemented (unchanged) |
| §9 Network Layer (sidecar) | ⚠ Expanded (l1-block/shard-checkpoint/fraud-proof topics); full replacement + DHT open |
| §10 Staking Model | ❓ Delegated-stake state landed; VRF stake-weighting unverified |
| §11 Migration Strategy | ⏳ Not implemented — still the biggest remaining piece |
| §12 Superblocks | ⏳ Design landed; implementation deferred |
| §13 Phases | ✅ Phases 0-7 complete; sharding/committee is the post-phase workstream |
| §14 Open Questions | Partially addressed; enforcement-gap (re-exec-before-final) is the open #1 |

---

## 🔐 Security workstream (KES / slashing) — added 2026-07-10

- ⚠ **KES live wiring** — VALIDATED 2026-05-16 (8 nodes rotated period 0→1: bootstrap→registration→KesRegistry→protobuf→disk SecureStore→eta-aligned). **Pending:** Slice 9 (load-bearing flip + cert v2).
- ⚠ **Slashing = detection-only accumulator** — landed but SHELF-WARE; planned refactor to epoch-anchored participating-set + demotion-as-exclusion (design-only).
- ⚠ **Watchtower fraud-proof / InvalidStateProof slash** — see Execution-Sharding section (durable ledger landed; wiring being verified).
- ⏳ **BLS / aggregate-sig** — feasibility PROVEN (BC 1.85 KAT byte-match); unified rotatable ValidatorKeyRegistry + PoP, target = snapshot certs. Not started in prod.

---

## 🧪 Test Infrastructure

- Current standard e2e: `just test --skip-streaming --grafana --num-shards=2` (2mg/2shard). See `docs/nakamoto/E2E-CLUSTER-TOPOLOGY.md`. (⚠ `--shards` is a dead no-op; use `--num-shards`.)
- Original harness (still present): `nakamoto-test/` — `demo.sh`, `docker-compose*.yml` (3 genesis + 1 validator + Go sidecars), monitoring (Prometheus 5s scrape + Grafana), `test-validator.sh`.
- Prometheus cluster-wide at `localhost:19090`; correlate with node logs on any failure.
- Unit tests: 647+ at original sweep; many more since (sharding, cross-shard, watchtower, catch-up, store-fidelity suites).
