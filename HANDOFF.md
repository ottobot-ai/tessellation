# Nakamoto Consensus Refactor — Agent Handoff

**For the next agent picking this up** (human or LLM, OpenClaw or otherwise).

**Last updated:** 2026-04-08 by OttoBot
**Branch:** `feature/nakamoto-stake-registry` on `otto` remote (ottobot-ai/tessellation)
**Repo root:** `/home/euler/repos/tessellation-nakamoto/`
**Head commit:** `46a8495e` (as of this writing)

---

## 1. What This Is

We are replacing Tessellation's BFT 5-phase consensus (EventTrigger, TimeTrigger, Facilities → Proposals → Signatures → Finished, StallDetector, view changes) with a **Nakamoto-style VRF lottery over a slot clock**, using the **LDD "snowplow" eligibility curve** from the dilf4s research paper.

The pitch, in one paragraph: every node runs a wall-clock slot clock (1s slots). At each slot, each node evaluates a VRF with its own key. If the VRF output is below an eligibility threshold (which grows with slot gap from the last finalized snapshot, via LDD), that node produces a snapshot **solo**. It broadcasts via GossipSub. Peers validate the VRF proof, sign a tip attestation, and gossip it back. Once 2/3+1 attestation weight accumulates on a tip, it's "soft finalized." Chain depth k gives hard finality. No rounds. No facilitator selection. No view changes.

**Design doc (read this first):** `/home/euler/.openclaw/workspace/nakamoto-tessellation-scope.md` — 379 lines, comprehensive scope written 2026-04-02.

**Research paper (why LDD, not Praos):** `/home/euler/repos/dilf4s/paper/main.pdf` + `/home/euler/repos/dilf4s/paper/main.tex` — the LDD snowplow parameters (fA, fB, γ, ψ), fork rate analysis, comparison vs Praos. We confirmed A/B that LDD gives ~13% fill rate at 1s slots vs Praos's ~5.5% at the same security level.

**Formal proposal (LaTeX, 24 pages):** `/home/euler/.openclaw/workspace/nakamoto-tessellation-proposal.pdf` — the write-up for external/stakeholder review.

---

## 2. Current State (2026-04-08)

### What works
- **3-node and 8-node genesis clusters** — full convergence, single canonical chain, 2/3 attestation finality
- **4th validator joining mid-chain** via `run-nakamoto-validator` CLI subcommand (downloads latest snapshot via HTTP, syncs MPT, joins VRF production)
- **Cold restart recovery** — detect existing snapshots on disk, resume from latest ordinal
- **Fork convergence + reorgs** via Bifrost-style ChainSelection (chain length → stake weight → slot recency → hash tiebreak)
- **Chain-derived eta** for VRF randomness (replaced an earlier SharedEpochState accumulator that caused partition at rotation boundaries — see commit `a932fe41`)
- **MPT stateProof determinism** via full-rebuild workaround (commit `b7489986`) + self-healing on fork switch (`21cec6de`) + undo journal per-ordinal (`289bcece`)
- **Nakamoto-specific daemons** — BFT download daemon, event gossip daemon, and BFT P2P routes are disabled in Nakamoto mode via `isNakamotoMode` flag (`af15c077`, `bde17768`)
- **Prometheus metrics + Grafana dashboard** (`e6eff016`) — see `nakamoto-test/grafana/dashboards/nakamoto-consensus.json`
- **647+ unit tests passing**

### Test cluster
```
cd nakamoto-test
bash demo.sh              # full demo: 3 GL0 + 3 L1 + Grafana/Prometheus
bash demo.sh stop         # tear down
# Variants:
NODES=8 WITH_L1=false bash demo.sh
WITH_L1=false WITH_MON=false bash demo.sh
```

Grafana: http://localhost:3000 (admin/admin, or anonymous).
See `nakamoto-test/README.md` for the full operator guide.

### What does NOT work yet / is blocked

1. **Hard fork migration from BFT → Nakamoto is NOT implemented.** All current testing uses fresh genesis. The biggest remaining piece. We need a dual-mode ConsensusManager that runs BFT below the fork ordinal and Nakamoto above, plus logic to load existing chain state (balances, state channels, metagraph snapshots) as Nakamoto's genesis input.
2. **Metagraph (CL0/DL1) Nakamoto support is not wired** — current code is GL0-only.
3. **Gossip layer is only partially migrated.** The sidecar handles Nakamoto snapshot topics; old tessellation gossip still runs for events. Long-term target: everything through the Go sidecar.
4. **Stake is equal-weight** (`1/N`). Target is stake-proportional: `threshold = 1 - (1 - f(δ))^relativeStake`. `StakeRegistry.stakeWeighted` is stubbed but unused.
5. **MPT rollback on fork switch uses full resync**, not the `MptUndoJournal.unapplyTo` path. The journal records deltas but the unapply is not yet wired in `NakamotoSyncDaemon`.
6. **Production abandonment on better-gossip** — `ProductionGate` exists but threshold-based abandonment during in-progress production is not wired at pre-sign / pre-publish checkpoints.
7. **Mempool reinsertion on finalize** — orphaned-fork events are not recycled back to the mempool.
8. **Services.make still constructs the full BFT consensus object** even in Nakamoto mode. It's never triggered (no daemons, no routes) so it's harmless but ugly. Deferred refactor.

**Full living list:** `NAKAMOTO-TODO.md` at the repo root. Treat that as the master backlog and update it as you go.

---

## 3. Where to Start (Next Priorities)

In rough order, in case you pick this up cold:

1. **Hard fork loader** (`NAKAMOTO-TODO.md` item #1 in "High Priority").
   - Needs to read a specific BFT snapshot ordinal as the seed state for Nakamoto.
   - `epochProgress` continuation: `forkEpochProgress + floor((currentSlot - forkSlot) / 60)`.
   - Genesis time derivation: `forkWallClock + slotDuration * 0`.
   - All active balances, state channels, metagraph info must be copied from `GlobalSnapshotInfo` at the fork ordinal.
   - The new VRF lottery must use a fresh eta derived from the fork-point snapshot hash.

2. **Metagraph (CL0/DL1) support.**
   - Mirror the GL0 pattern for currency-l0 and dag-l1 (the "metagraph" layers).
   - Per-metagraph GossipSub topics: `/tessellation/cl0/<metagraph-id>/snapshots`.
   - Most domain code in `modules/node-shared/.../domain/nakamoto/` should be reusable 1:1.

3. **Stake-proportional VRF** (`StakeRegistry.stakeWeighted`).
   - Read from `GlobalSnapshotInfo.activeDelegatedStakes` and `activeNodeCollaterals`.
   - Update threshold formula: `threshold = 1 - (1 - f(δ))^relativeStake`.

4. **Wire `MptUndoJournal.unapplyTo` into `NakamotoSyncDaemon`** on reorg.
   - Required for testnet scale — full resync is too expensive at 100MB state / 80k trie entries.

5. **Proactive production abandonment** via `ProductionGate` at pre-sign / pre-publish.

6. **Mempool reinsertion on finalize** — orphaned branch events recycled with state-dependent revalidation.

---

## 4. Code Map

### Scala code (the core)
All under `modules/` in the repo. Key new/changed paths:

**Nakamoto domain** (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/`):
- `SlotClock.scala` — wall-clock → slot number
- `EligibilityChecker.scala` — LDD snowplow curve; VRF-vs-threshold check
- `NakamotoProposer.scala` — orchestrates: slot win → build snapshot → sign → publish
- `StakeRegistry.scala` — seedlist-backed stake registry (stub for stake-weighted VRF)
- `EpochState.scala` — epoch boundaries, eta rotation (currently `SlotCertificate`-driven)
- `EtaCalculation.scala` — eta derived from canonical chain, NOT accumulator (see commits `a932fe41`, `9b1ede57`)
- `ChainSelection.scala` — Bifrost-style fork choice (length → stake → recency → hash)
- `ParentChildTree.scala` — in-memory tree of tips and ancestry
- `TipTracker.scala` — accumulates attestations per tip; computes finality weight
- `ProductionGate.scala` — pausable leader election (for abandonment)
- `MptUndoJournal.scala` — records per-ordinal MPT deltas for rollback
- `EventSourcedMptState.scala` — experimental Bifrost-style event-sourced MPT (not yet wired)

**Nakamoto infrastructure** (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/consensus/nakamoto/`):
- `SidecarClient.scala` — gRPC client to the Go libp2p sidecar
- `GossipStream.scala` — HTTP bridge (port 50052) alternate transport
- `NakamotoTriggerDaemon.scala` — periodic slot-tick driver

**dag-l0 Nakamoto glue** (`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/`):
- `SnapshotLeaderLoop.scala` — the main production loop
- `NakamotoSyncDaemon.scala` — receive path: validate + store + attest + accumulate
- `NakamotoSnapshotValidator.scala` — 4-stage validation (VRF + signature + slot cert + content)
- `NakamotoChainStore.scala` — on-disk chain storage, finalize, prune
- `NakamotoMetrics.scala` — Prometheus metrics (threaded + polling)

**VRF crypto** (`modules/shared/src/main/scala/io/constellationnetwork/security/vrf/`):
- `EcVrf25519.scala` — ECVRF-ED25519-SHA512-TAI, sourced from dilf4s
- `VrfKeyDeriver.scala` — derives Ed25519 VRF key from existing secp256k1 node key via `SHA-512("tessellation-vrf-v1" ++ rawKeyBytes).take(32)`

**Protobuf schemas for Nakamoto** (`modules/shared/src/main/scala/io/constellationnetwork/schema/nakamoto/`):
- `attestation.scala`, `chain.scala`, `ldd.scala`, `slot.scala`

**CLI entry points** (`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/cli/method.scala`):
- `RunNakamoto` — new genesis
- `RunNakamotoValidator` — join running cluster

**Main wiring** (`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/Main.scala`):
- Line ~138: `isNakamotoMode` flag computed from method
- Line ~163: conditional `Daemons.startNakamoto` vs `Daemons.start`
- Line ~451: `case m: RunNakamoto =>` — full Nakamoto boot path, cold restart detection, SnapshotLeaderLoop wiring
- Line ~595: `case m: RunNakamotoValidator =>` — validator boot path (HTTP download from peer)

### Go sidecar
All under `p2p/`:
- `p2p/go.mod` — module `github.com/scasplte2/tessellation/p2p`, Go 1.25.7
- `p2p/proto/sidecar.proto` — gRPC schema
- `p2p/cmd/` — main binary
- `p2p/internal/` — libp2p/GossipSub wiring
- `p2p/Dockerfile` — build image
- Built static binary lives at `nakamoto-test/sidecar-linux` (38MB, statically linked, for the docker-compose builds)

The sidecar exposes:
- gRPC on `:50051`
- HTTP bridge on `:50052`
- Topics like `/tessellation/gl0/snapshots`

### Test infrastructure
All under `nakamoto-test/`:
- `demo.sh` — one-command full demo
- `docker-compose.yml` — 3 GL0 + 3 sidecars + validator profile (node-3)
- `docker-compose-8node.yml` — 8-node variant
- `docker-compose-l1.yml` — 3 DAG-L1 nodes over Nakamoto GL0
- `docker-compose-monitoring.yml` — Prometheus (5s scrape) + Grafana
- `Dockerfile.jvm` — dag-l0 JVM image
- `Dockerfile.l1` — dag-l1 JVM image
- `Dockerfile.sidecar` — Go sidecar image
- `genesis.csv`, `genesis-8.csv` — genesis account balances
- `seedlist.csv`, `seedlist-8.csv` — authorized peer IDs (these drive StakeRegistry today)
- `keys/` — pre-generated key material for local nodes
- `prometheus.yml` — scrape config for all nodes
- `grafana/dashboards/nakamoto-consensus.json` — pre-provisioned dashboard
- `start.sh`, `run-nakamoto.sh`, `join-l1.sh`, `test-validator.sh`, `gen-cluster.sh`, `compare-ldd-praos.sh` — various helpers

---

## 5. Build & Run

### Prerequisites
- **Java 21** via SDKMAN: `source ~/.sdkman/bin/sdkman-init.sh` (Temurin)
- **sbt** (1.x)
- **Docker + Docker Compose** (demo uses Compose v2 `docker compose`)
- **Go 1.25+** (only if rebuilding the sidecar binary)
- ~16GB free RAM for `sbt stage` — the build can SIGKILL under memory pressure if containers are running

### Build the JVM
```bash
source ~/.sdkman/bin/sdkman-init.sh
sbt -J-Xmx4g "dagL0/clean" "dagL0/stage"
# Also for L1 demo:
sbt -J-Xmx4g "dagL1/stage"
```

**Gotchas** (learned the hard way):
- Always `dagL0/clean` before `dagL0/stage` — there's an sbt-native-packager bug where re-stages leave stale files.
- `nodeShared/clean` after proto changes — stale ScalaPB generated code.
- Stop containers before building if you're memory-constrained.
- `docker compose build --no-cache` when JARs change.

### Build the Go sidecar
```bash
cd p2p
go build -o ../nakamoto-test/sidecar-linux ./cmd/sidecar
```

### Run tests
```bash
sbt "nodeShared/test"     # most Nakamoto domain tests
sbt "dagL0/test"          # SnapshotLeaderLoop etc
sbt "test"                # full suite (647+ tests)
```

### Local demo
See §2 above or `nakamoto-test/README.md`.

---

## 6. Key Architecture Decisions (and why)

These are locked in. Don't revisit without good reason.

- **Pure attestation, no BFT rounds.** VRF winner produces solo → peers validate + sign → TipTracker accumulates → 2/3+1 finalizes. This is GRANDPA-style async finality, not Tendermint-style rounds.
- **VRF Ed25519 keys derived from secp256k1.** `SHA-512("tessellation-vrf-v1" ++ rawKeyBytes).take(32)`. Nodes don't need to generate/store a second keypair.
- **LDD over Praos.** Confirmed by A/B test (`nakamoto-test/compare-ldd-praos.sh`): LDD gives ~13% fill rate at 1s slots vs Praos ~5.5% at the same security. Params: `fA=0.5, fB=0.05, γ=15, ψ=1`. Recommended tuning from parameter sweep: `fA=0.4, γ=12` for ~8s cadence and <1.2% fork rate at 100 nodes.
- **Chain-derived eta, NOT accumulator.** An earlier design used a `SharedEpochState` accumulator but caused divergence at rotation boundaries (commit `a932fe41` has the post-mortem in the message body). Now eta is deterministically derived from the canonical chain via Blake2b.
- **Go libp2p sidecar for gossip.** Mirrors Ethereum's execution/consensus client split. JVM ↔ Go via gRPC (50051) and HTTP bridge (50052). GossipSub mesh, mDNS for local discovery, Noise transport.
- **Fork choice is Bifrost-style 4-tier**, NOT density-based. Chain length → stake weight → slot recency → hash tiebreak.
- **All nodes produce at every slot they win.** No leader/follower distinction. Slot gap is global (measured from last finalized).
- **Each node signs genesis with its own key.** Genesis hash excludes signatures, so it's identical across nodes.
- **Dual finality.** Attestation weight ≥ 2/3+1 (soft) OR chain depth > k=6 (hard).
- **Equal-weight staking initially.** Stake-proportional deferred but planned.
- **Content validation must be ENFORCED** (James's explicit directive). Current code enforces, but there were periods where it was advisory only.
- **Kryo registration is NOT needed for new Nakamoto types.** Kryo is a fallthrough for older formats. New types live in the new encoding path.

---

## 7. Known Gotchas & Bug Notes

- **MPT stateProof determinism.** The in-memory MPT is currently rebuilt fully per-snapshot to guarantee deterministic roots (commit `b7489986`). This is a workaround. The long-term fix is a content-addressed trie (Ethereum-style) — see `EventSourcedMptState.scala` for the start of that work. Not yet wired in.
- **VRF proof vs output in gossip.** Gossip carries the VRF *proof* but `chainStore.store` expects the VRF *output*. `NakamotoSyncDaemon` has a helper `vrfOutputFromProof` that calls `EcVrf25519.vrfProofToHash` (commit `2bb40db5`). If you see mismatches in the chain store after gossip-path snapshots, check this.
- **Genesis time discovery for validators.** Currently requires `NAKAMOTO_GENESIS_TIME_MS` env var. Should ideally come from a peer's `/latest/info` endpoint. See `5a09af8f`.
- **PeerId encoding.** There was a subtle bug where the Go sidecar and JVM had different PeerId serializations that caused attestation misrouting. Fixed during cluster convergence work (~Apr 5). If you see attestations being assigned to wrong peers, check this code path.
- **sbt + Docker memory pressure.** `sbt stage` gets SIGKILL if Docker containers are eating RAM. Stop containers first.
- **gRPC health.** The sidecar can lose mesh membership briefly. `NakamotoTriggerDaemon` has retry logic. Check sidecar logs (`docker compose logs sidecar-0`) for "Connected to peer" lines if nodes aren't seeing each other's gossip.

---

## 8. Memory, Skills & References for the Next Agent

> If you're an agent running inside OpenClaw under the `main` persona (OttoBot), you'll already have most of this in context. If you're running somewhere else (Claude Code, Cursor, Aider, raw Codex, a human, etc.), read these files directly.

### Repo-local docs
- `/home/euler/repos/tessellation-nakamoto/NAKAMOTO-TODO.md` — **master backlog.** Update as you close items.
- `/home/euler/repos/tessellation-nakamoto/HANDOFF.md` — this file.
- `/home/euler/repos/tessellation-nakamoto/README.md` — upstream tessellation README.
- `/home/euler/repos/tessellation-nakamoto/CLAUDE.md` — upstream repo notes for LLMs.
- `/home/euler/repos/tessellation-nakamoto/CONTRIBUTING.md` — contribution guidelines.
- `/home/euler/repos/tessellation-nakamoto/nakamoto-test/README.md` — demo / operator guide.
- `/home/euler/repos/tessellation-nakamoto/docs/consensus/README.md` — the old BFT consensus deep-dive. Read this to understand what you're *replacing*.
- `/home/euler/repos/tessellation-nakamoto/docs/mpt/` — MPT architecture, proof system, api reference, integration notes, data structures. Read `architecture.md` and `proof-system.md` before touching the MPT.
- `/home/euler/repos/tessellation-nakamoto/docs/nakamoto/SYNC-PROTOCOL.md` — validator sync protocol (used by `RunNakamotoValidator`).

### External docs (workspace-level)
- `/home/euler/.openclaw/workspace/nakamoto-tessellation-scope.md` — **the design document (379 lines, v2). READ THIS FIRST.**
- `/home/euler/.openclaw/workspace/nakamoto-tessellation-scope-v1.md` — v1, deeper (1361 lines). Useful for historical context on decisions that changed.
- `/home/euler/.openclaw/workspace/nakamoto-tessellation-proposal.tex` + `.pdf` — formal 24-page writeup for stakeholders.
- `/home/euler/.openclaw/workspace/nakamoto-architecture.png` — architecture diagram.

### Research references
- `/home/euler/repos/dilf4s/paper/main.tex` + `main.pdf` — **the LDD snowplow paper.** Read this to understand the eligibility curve math, fork rate analysis, and parameter choices. Figures in `dilf4s/paper/figures/`.
- `/home/euler/repos/dilf4s/paper/REFERENCES-RESEARCH.md` — bibliography.
- `/home/euler/repos/dilf4s/` — dilf4s is the **reference implementation** of LDD-based Nakamoto consensus in Scala. Our `EcVrf25519`, LDD curve, and chain selection ideas come from here. Look at `dilf4s/core/` for inspiration.
- `/home/euler/repos/Bifrost/` — another reference implementation (Topl's Bifrost), the source of the `ParentChildTree` + 4-tier ChainSelection pattern. Look at `Bifrost/blockchain/` and `Bifrost/consensus/`.

### Key research concepts to know
- **LDD snowplow curve:** eligibility grows with slot gap. Bounded above (fA) and below (fB). The shape avoids both empty droughts and burst-induced fork storms.
- **VRF self-selection:** no coordination, no committee — each node locally computes `VRF(key, eta, slot)` and checks against threshold.
- **Eta rotation:** the "randomness beacon" used by VRF. Must be unpredictable far enough ahead to prevent grinding, and must be agreed on by all nodes. We derive from chain hash.
- **Dual finality:** GRANDPA-style async finality (attestation) + Nakamoto depth confirmation (PoW-style). Belt and suspenders.
- **Superblocks / NIPoPoW:** deferred research track. LDD's slot-gap threshold can serve as a difficulty certificate, enabling log-size light client proofs. Don't implement until testnet is stable.

### Upstream tessellation docs
- Upstream repo: https://github.com/Constellation-Labs/tessellation (remote `upstream`)
- Constellation Network docs: https://docs.constellationnetwork.io/
- If you need to understand how the existing BFT consensus works before modifying/disabling it, start with `docs/consensus/README.md` in this repo.

### OpenClaw-specific (only relevant if running as an OpenClaw agent)
These are OttoBot's internal references. Skip if you're not OpenClaw:
- `/home/euler/.openclaw/workspace/skills/tessellation-cluster/SKILL.md` — local cluster runbook
- `/home/euler/.openclaw/workspace/skills/ottochain/SKILL.md` — deploy / operate skill (OttoChain is the downstream consumer of this work)
- `/home/euler/.openclaw/workspace/skills/aider-scala/SKILL.md` — which models work for Scala edits, which don't. **Important**: local models are unreliable for complex Scala refactors. Use Aider with `ollama_chat/glm-4.7-flash` for small edits, Opus/Sonnet for anything architectural.
- `/home/euler/.openclaw/workspace/MEMORY.md` — OttoBot's long-term memory. Contains decisions, bug lore, and things like "Kryo registration is NOT needed for new types" that are easy to get wrong.
- `/home/euler/.openclaw/workspace/memory/` — daily notes, search for "nakamoto" or "tessellation".

---

## 9. Git & Remote Setup

```bash
# Current branch
git checkout feature/nakamoto-stake-registry

# Remotes (already configured)
otto     https://github.com/ottobot-ai/tessellation.git    # work branch lives here
james    https://github.com/scasplte2/tessellation.git     # James's personal fork
upstream https://github.com/Constellation-Labs/tessellation.git  # canonical tessellation

# Sync
git fetch --all
git log --oneline feature/nakamoto-stake-registry ^upstream/develop | wc -l   # commits ahead
```

**The work branch is ~70+ commits ahead of upstream/develop.** Don't rebase casually — several commits have important standalone fixes (e.g., `a932fe41` fork-aware eta, `b7489986` MPT determinism).

### Open PRs
- **PR #4** (MERGED) — VRF crypto
- **PR #5** — slot clock + LDD
- **PR #6** — StakeRegistry, EpochState, SlotCertificate
- (Others may be opened — check `gh pr list --repo ottobot-ai/tessellation`)

---

## 10. Working Style Notes (from previous agent)

Some rules of engagement that worked well:

- **Diagnose before changing.** "Think → diagnose → one fix." Multiple blind attempts wastes cycles.
- **Verify before claiming.** Don't say "the cluster is finalizing" without checking `/latest/info` and the Grafana attestation weight panel. Don't say "this compiles" without running `sbt compile`.
- **Repo first, server second.** Any fix must land in the repo (PR or commit), not as an ad-hoc change on a running node.
- **Stay on the blocking issue.** When production is broken, don't clean up unrelated code. Ship the fix, then refactor.
- **Each commit should be understandable alone.** This branch's git log is a story of the debugging journey — keep that quality.
- **`trash > rm`** for anything that could matter later.
- **When in doubt about upstream tessellation behavior**, read the Scala source, not docs. The docs lag.

### Things that burned time (don't repeat)
- Chasing non-determinism in the MPT stateProof by re-running clusters 20 times. Root cause was insertion-order dependence. Fix: full rebuild per snapshot (`b7489986`).
- Trying to reuse the old `SharedEpochState` accumulator for eta. Caused partition at rotation boundary (~1165 slots). Fix: chain-derived eta (`a932fe41`, `9b1ede57`).
- Running `sbt stage` with containers up. OOM kill every time. Fix: stop containers first.
- Assuming the sidecar's PeerId encoding matched the JVM's. It didn't. Fix: explicit encoding convention, test on cluster.
- Assuming new Nakamoto types needed Kryo registration. They don't — Kryo is the old-format fallthrough.

---

## 11. Contact & Escalation

- **Project lead:** James Aman (scasplte2, james@constellationnetwork.io)
- **Upstream team:** Constellation Labs (GitHub: Constellation-Labs)
- **LDD paper author / reference implementation:** dilf4s (in `/home/euler/repos/dilf4s`)

If you are an OpenClaw agent: message James on Telegram (chat id `7910600397`) for blocking questions, or via email for anything requiring deliberation. Cross-check with the `main` (OttoBot) agent session if you need history context.

If you are NOT an OpenClaw agent: open an issue in the `ottobot-ai/tessellation` repo or ask James directly.

---

## 12. "I am reading this, where do I go right now"

1. Read `nakamoto-tessellation-scope.md` (the design doc) — 30 min.
2. Read `NAKAMOTO-TODO.md` at repo root — 10 min.
3. `cd nakamoto-test && bash demo.sh` — watch a live 3-node cluster finalize, look at the Grafana dashboard. 15 min.
4. Read `SnapshotLeaderLoop.scala` and `NakamotoSyncDaemon.scala` — 30 min. This is the heart of the new consensus.
5. Read `Main.scala` lines 138–700 — understand how Nakamoto mode is wired in. 15 min.
6. Pick the highest-priority open TODO, write a short design note inline in `NAKAMOTO-TODO.md`, and start.

Good luck. This is a fun one.

— OttoBot 🦦, 2026-04-08
