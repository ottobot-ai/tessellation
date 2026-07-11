# Nakamoto Consensus — Status & Remaining Work

**Last updated:** 2026-07-11 (source audit)

---

> ### ⚠ 2026-07-10 status-sweep note — READ THIS
> This file was stale (last real edit 2026-04-05, at ~commit 62; the branch is now at 2575 commits). This sweep re-annotated status by cross-referencing **git log subjects + engineering notes**, NOT by re-reading each feature's code. Trust the flags accordingly:
> - **✅ done** — already validated, or a clear landing commit + prior confirmation.
> - **⚠ landed-per-git (unverified)** — a commit subject says it landed, but I did NOT re-verify the implementation at source this sweep. Confirm before relying.
> - **❓ unknown** — status genuinely unclear; needs a check.
> - **⏳ planned / pending** — no evidence it's done; still on the roadmap.
> - **♻ superseded** — replaced by newer design; the replacement is named. Fork-only authority/diff/receipt schemas may be deleted because this greenfield fork has never deployed them.
>
> **Biggest change since 2026-04-05:** the project moved into execution sharding, but ADR-0017 rejects committee authority over framework economics. Proposal and attestation duties may be sharded; **CL1 economic validity is universal** because every GL0 node recreates CL1 before adoption. The failed shard design used secret stake-weighted VRF self-sortition and per-slot LDD leadership. Current shard v1 intentionally uses public deterministic VK-hash membership, uniform `1/N` over eligible GL0 operators, and hash-shuffled staircase producer duty. See **🧩 Execution-Sharding & Universal Economic Verification** below and the canonical `docs/adr/0016-*` / `docs/adr/0017-*` decisions.
>
> **2026-07-11 adversarial correction:** structural CL1 replay is now wired, but
> economic safety is NOT complete. The global transition function still accepts
> unsigned unlock/no-ref spend authority, bounded processed-history replay, and
> unbacked stake records. Optimistic/depth finality is unsafe and the claimed
> Avalanche cascade is absent. The source-cited status authority is
> `docs/review/CORRECTNESS-SECURITY-AUDIT-2026-07-11.md`; any checkmark below
> contradicted by that report is withdrawn.
>
> **Active implementation order:**
> `docs/review/CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md`. The historical priority
> buckets and numbered items below are a status inventory, not the sequence for
> economic deployment. Finality, authorization, conservation, exact-once
> inter-metagraph settlement, recovery, and network gates in the active roadmap
> take precedence over the older testnet labels.

---

## ✅ Completed (Phases 0-7 + extras) — from the original sweep, unchanged

| Feature | Commit | Tests |
|---------|--------|-------|
| VRF crypto (ECVRF-ED25519-SHA512-TAI) | PR #4 merged | 43 tests |
| Slot clock + LDD snowplow eligibility | PR #5 | 20+ tests |
| StakeRegistry, EpochState, SlotCertificate | PR #6 | 30+ tests |
| Go libp2p sidecar (GossipSub, mDNS, gRPC) | — | Manual |
| NakamotoProposer + snapshot production | — | — |
| Attestation + depth rails | Built but unsafe; not Avalanche and not a common-prefix proof | See 2026-07-11 audit FIN-01..FIN-12 |
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
| Replay-only ancestry catch-up | Current tree | Fresh validation required; direct peer-state installation removed |
| Optimistic attestation rail | Built, but local active-set renormalization and unlocked latest votes can split finality | See audit FIN-01/FIN-02 |

**Test cluster validated (as of original sweep):** 3-node genesis + 1 validator joining mid-chain, cold restart, single-node restart, reorgs, fork convergence, MPT determinism, attestation finality.

---

## 🧩 Execution-Sharding & Universal Economic Verification (dominant workstream since 2026-04)

> The target model: metagraph proposal/processing duties are sharded, but economic verification remains universal. The shard committee pre-executes CL1 and attests a checkpoint root; the producer, every signer, and every GL0 adopter independently run the same full currency recreation over the included snapshot bytes. Abandoned checkpoint diff/receipt schemas are removed. Watchtowers are defense-in-depth slashing, not the validity gate. Custom DL1 logic remains proof-carried because GL0 cannot run it. Structural replay is implemented; the transition-level authorization and conservation blockers in the 2026-07-11 audit remain open.

- ⚠ **Sharded-security substrate** — pinned `globalSyncView`, PIN-1 component-addressable per-MG MPT roots, I-ONCE spent-set, watchtower fraud-proof scaffold (`8ac7ce04f`).
- ⚠ **Sharding wiring present, epoch binding incomplete** — real committee key-possession verification + W3c sharded-validator activation
  are live, but the adopter trusts the checkpoint's wire-carried execution epoch instead of deriving it from the signed finalized anchor.
  A producer can grind resolvable public epochs for a favorable committee (audit SHARD-03).
- ⚠ **Shard committee draw** — public deterministic VK-hash selection per `(shard, eta period)`, uniform `1/N` over eligible GL0 operators. This deliberately replaced the failed secret stake-weighted VRF design; the attached VRF proves registered-key possession, not hidden membership.
- ⚠ **Committee pre-execution -> checkpoint root; universal GL0 recreation** — every adopter recreates from `includedSnapshots`; abandoned diff and receipt fields have been removed. This is a path invariant, not an economic-safety certification; see ECO-02..ECO-06.
- ⚠ **Watchtower InvalidStateProof slash — durable ledger** (Slashings fieldId 34) (`ed8928b81`); durably slash a full-quorum colluding committee on one honest fraud proof (`68246cffe`, W3a). **Wired-vs-shelfware being verified in handoff.**
- ⚠ **Slash-cooldown committee exclusion** — the cooldown reader is wired, but the loop is not closed: SHARD-03 permits wire-epoch grinding around the intended selection and ECO-06 leaves bonded principal undebited.
- ✅ **Cross-shard framework reads = Option A (finality-first)** — the GL0 accept path wires `gl0Local` over the finalized MPT base;
  an owner value present only on an unfinalized parent branch is not usable by a consuming shard.
- ⚠ **Atomic cross-shard allow-spend settlement (I-ONCE)** via generic cross-shard-message seam (`f368e064e`); W3c cross-shard proof-path effective-balance overlay (`4081ef0d4`).
- ⚠ **Sharded-currency-mirror store-fidelity stack** — pinned finalized reads, by-ordinal `PinnedCurrencyInfoReader`, and root-verified exact-ordinal hole backfill remain. Adopted-state staging and direct recovery installers are removed; peer bytes never authorize consensus adoption.
- ✅ **Authoritative CL1 adoption removed** — the fork-only `authoritative*` schema/codec slots, `deriveAdoptedCurrencyInfo`, and committee-diff state replacement are removed; producer, signer, and every GL0 adopter run full currency recreation.
- ⚠ **Shard-checkpoint chain-sync (pull-based recovery)** (`df5b7b0e0`, `d47a4403f`); FINALIZED-anchored shard checkpoints (`eb15e19c0`).
- ⚠ **ml0 unified chain-based consensus engine** — rotation + solo-extension + gl0-anchor under one seam (`add65b620` design; decision recorded).
- ⚠ **Go↔JVM gossip transport for shard-checkpoint + fraud-proof legs** (`72f39d652`, F1/F2/F8); sidecar carries l1-block topics + self-delivery (`b0ca67908`).
- 🔴 **RECURRING WEDGE / current e2e operational frontier (not the security dependency head) — metagraph committee-gate parent-ordinal resolution.** `MetagraphCommitteeGate` / `MetagraphParentOrdinalResolver` / `MetagraphOrphanBuffer`; resolver→None on GSI-tip-lag → orphan re-buffer loop. Prior fix "walk-finalized-not-bestTip" (`40d546761`) recurred; blocks the 2mg/2shard token-lock e2e. **Must be fixed WITHIN the re-exec model, after the active roadmap's security ordering is respected.**
- ⚠ **CL1 re-exec before sign/adopt** — `evaluate` and `verifyEmbedded` re-execute regardless of quorum/depth; ancestor attestations are
  gated by the same verifier. Watchtower checking is defense in depth, not the economic-validity gate.

---

## 🔧 Implementation Still Needed (original items, re-annotated 2026-07-10)

The priority headings in this inherited list are historical. Use the active
consensus-economic roadmap for dependencies and release gates.

### High Priority — Before Testnet

1. ♻ **Hard Fork Migration Mechanism** — superseded for this greenfield fork. No fork-only schema has been deployed. Re-open only if migration from an actually deployed upstream-v4 network becomes a product requirement.
   - Dual-mode dispatch in ConsensusManager (BFT below fork ordinal, Nakamoto above)
   - Load existing chain state (balances, state channels, metagraph snapshots) at fork point
   - Genesis time derivation from fork ordinal + slot duration
   - `epochProgress` continuation: `forkEpochProgress + floor((currentSlot - forkSlot) / 60)`

2. ✅ ~~**Disable BFT Daemons in Nakamoto Mode**~~ (af15c077, bde17768, 5ec6ac46) — unchanged.

3. ♻⚠ **Metagraph (CL0/DL1) Nakamoto Support** — original framing ("CL0 needs same VRF+LDD+attestation 1:1") **superseded** by the execution-sharding + committee model above. Metagraph consensus (ml0 rotation, shard checkpoints, cl1/dl1 adopt) is substantially built per git-log; confirm scope vs the original 1:1 intent. Per-metagraph GossipSub topics exist (sidecar). **← see Execution-Sharding section.**

4. ⚠ **Gossip Layer: Replace Tier 1/2 with Sidecar** — substantial progress: sidecar now carries Nakamoto + l1-block + shard-checkpoint + fraud-proof topics; Go↔JVM transport hardened (`72f39d652`, `461a34830`, `ad3f01092`). **Not confirmed:** ALL gossip through sidecar; Kademlia DHT peer discovery (still seedlist/mDNS?). Keep open items.

5. ⚠ **Stake-Proportional VRF (global Nakamoto leadership only)** — delegated-stake state landed and is read from MPT, but source verification of the global leader threshold remains open. This item does **not** apply to execution-shard membership: shard v1 is intentionally public, identity-uniform `1/N`, and not stake weighted.

### Medium Priority — Testnet Hardening

6. ♻ ~~**Proactive MPT Rollback on Fork Switch**~~ — superseded by `MptOverlay` (#56); MultiBranch overlay default since `e3538d9b`; revert-executor `MptOverlay.revertToOrdinal` (`3e47d1904`). Self-healing remains fallback.

7. ⚠ **Production Abandonment on Better Gossip** — ProductionGate + better-gossip trigger exist; finality-trigger stack landed. **Unverified:** threshold-based abandonment fully wired (ChainSelection.compare incoming vs in-progress at SnapshotLeaderLoop checkpoints).

8. ⚠ **Genesis Time Discovery for Validators** — genesis time/eta moved to typed HOCON (`d7a4212d5`). **Unverified:** peer-API discovery (`/cluster/genesis-time` or slot-cert in `/latest`) vs still config-supplied. Note: eta bootstrap for periods 0&1 now genesis-derivable (`45066b4b0`).

9. ⏳ **Mempool Reinsertion on Finalize** — no clear landing commit found. Round-cancellation logging landed (`948d2b2e1`) but event recycling on orphan/finalize appears unimplemented. Keep.

10. ⚠ **Partition Recovery (Fork Recovery)** — the GL0 design changed after the reported runs: direct peer-state/GSI installs are removed. A receiver buffers a missing-parent snapshot, fetches ancestry, and exact-replays every transition through the ordinary validator. Shard-checkpoint pull recovery remains, but the combined replay-only flow needs fresh partition/restart e2e validation. Confirm per-sub-item:
    - **10a.** Sidecar GossipSub mesh re-establishment after partition — ❓ verify (sidecar reconnection/mesh re-graft).
    - **10b.** Missing-parent ancestry request + buffered replay — ⚠ confirm recursive completion and prove no peer-carried context/state can be installed.
    - **10c.** ProductionGate stale-fork detection / pause-if-behind — ⚠ confirm.

11. 🔴 **Finality redesign** — no mode is currently safe for irreversible economic release. The full Avalanche cascade is absent, T_count is diagnostic, latest-vote weight lacks locking/intersection, depth-k splits under partition, and finality is not durable. See audit FIN-01..FIN-12.

### Low Priority — Post-Testnet

12. ⚠ **Content-Addressed MPT (Ethereum-style)** — MPT-as-primary substrate substantially built (signed-byte store, `MptStore.loadBytes`, by-ordinal readers, serve+rebuild `082ed24e7`). **Unverified:** full content-addressed trie-node KV store keyed by hash. Storage-representation migration, NOT re-execution.

13. ⏳ **Superblock Proofs (NIPoPoW-style)** — level-µ design landed (notes: one VRF, L domain-separated rehashes); hexary-MPT absence-proof WIP (`690b0f259`, task #286). Implementation deferred; light-client DEMO sequenced post-slashing/KES.

14. ⚠ **Global attestation committees** — still open. Shard-checkpoint membership is a public deterministic VK-hash draw plus a possession VRF, not secret VRF self-sortition; do not cite it as implementation of this global-attestation item.

15. 🔴 **Two-Level Finality (GL0 + Metagraph)** — not established. A shard checkpoint inherits GL0 inclusion, but GL0 finality itself is unsafe and there is no independent proved metagraph certificate. Cross-shard reads are pinned to GL0's finalized base, which does not repair a conflicting-finality split.

---

## 📊 Scope Doc vs Implementation (re-annotated 2026-07-10)

| Scope Doc Section | Status |
|-------------------|--------|
| §1 Problem Statement | ✅ Understood |
| §2 LDD Heartbeat | ✅ Implemented (fA=0.5, fB=0.05; shard-tuned variants exist, e.g. γ=45 per-slot) |
| §3 Epoch Progress | ⚠ Now used in acceptance (`metagraphPinnedEpochProgresses`, epochProgress); eta bootstrap landed (`45066b4b0`). Confirm full §3 semantics. |
| §4 What Changes | ✅ Removals identified; BFT daemons disabled in Nakamoto mode |
| §5 VRF Key Derivation | ✅ Implemented (SHA-512 domain separation) |
| §6 Attestation & Finality | 🔴 Implemented rails are unsafe; claimed Avalanche/max-of protocol is absent |
| §7 Two-Level Finality | 🔴 Not established; metagraph inclusion inherits unsafe GL0 finality |
| §8 Mempool Reinsertion | ⏳ Not implemented (unchanged) |
| §9 Network Layer (sidecar) | ⚠ Expanded (l1-block/shard-checkpoint/fraud-proof topics); full replacement + DHT open |
| §10 Staking Model | ❓ Delegated-stake state landed; VRF stake-weighting unverified |
| §11 Migration Strategy | ♻ Superseded by greenfield scope unless an actual deployed-v4 migration is required |
| §12 Superblocks | ⏳ Design landed; implementation deferred |
| §13 Phases | ✅ Phases 0-7 complete; sharding/committee is the post-phase workstream |
| §14 Open Questions | Universal CL1 recreation is implemented; remaining blockers include finalized cross-shard reads, conservation/replay, finality, and slashing authorization |

---

## 🔐 Security workstream (KES / slashing) — added 2026-07-10

- ⚠ **KES live wiring** — VALIDATED 2026-05-16 (8 nodes rotated period 0→1: bootstrap→registration→KesRegistry→protobuf→disk SecureStore→eta-aligned). **Pending:** Slice 9 (load-bearing flip + cert v2).
- ⚠ **Slashing = detection-only accumulator** — landed but SHELF-WARE; planned refactor to epoch-anchored participating-set + demotion-as-exclusion (design-only).
- ⚠ **Watchtower fraud-proof / InvalidStateProof slash** — defense in depth only; universal GL0 recreation is the CL1 validity gate. Durable-ledger and authorization details still require independent verification.
- ⏳ **BLS / aggregate-sig** — feasibility PROVEN (BC 1.85 KAT byte-match); unified rotatable ValidatorKeyRegistry + PoP, target = snapshot certs. Not started in prod.

---

## 🧪 Test Infrastructure

- Current standard e2e: `just test --skip-streaming --grafana --num-shards=2` (2mg/2shard). See `docs/nakamoto/E2E-CLUSTER-TOPOLOGY.md`. (⚠ `--shards` is a dead no-op; use `--num-shards`.)
- Original harness (still present): `nakamoto-test/` — `demo.sh`, `docker-compose*.yml` (3 genesis + 1 validator + Go sidecars), monitoring (Prometheus 5s scrape + Grafana), `test-validator.sh`.
- Prometheus cluster-wide at `localhost:19090`; correlate with node logs on any failure.
- Unit tests: 647+ at original sweep; many more since (sharding, cross-shard, watchtower, catch-up, store-fidelity suites).
