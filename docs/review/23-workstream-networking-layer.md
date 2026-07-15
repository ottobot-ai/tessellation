# 23 — Workstream: Networking Layer (Go sidecar gossip + REST routes)

> Grounded 2026-07-07 against HEAD `21933559c` on `feature/committee-state-diff`. Every claim is
> `file:line`-anchored. Companion to `PORTFOLIO.md §2 Tier-1 #6`.
>
> **Scope (what the team asked):** *how could we improve the networking layer — either via the Go
> sidecar for gl0 nodes, or via the REST routes for other consumers?* This doc grounds the **current
> implementation**, surfaces **robustness / boundedness / DoS / consumer-API** gaps, and ranks
> improvements. It is about **implementation quality**, not architecture.
>
> ⛔ **Settled, not re-litigated here** (per the task guardrail): *that a Go sidecar exists and owns
> gl0 P2P gossip*; *libp2p/GossipSub as the transport*; *BLS aggregation as the chosen aggregation
> approach*; *Taktikos/consensus design*. Where one of these carries an **unbuilt implementation**
> slice, that appears below as build-work — never as a design question.

---

## 0. Where the sidecar code lives (located)

**In-repo, not external, not a submodule.** The Go sidecar is a self-contained Go module at
`/home/euler/repos/tessellation-nakamoto/p2p/` — module path `github.com/scasplte2/tessellation/p2p`
(`p2p/go.mod:1`). ~3,888 LOC across 8 Go files:

| File | LOC | Responsibility |
|---|---|---|
| `p2p/cmd/sidecar/main.go` | 403 | entrypoint, flags, key mgmt, DHT bootstrap, outbox republisher |
| `p2p/internal/gossip/gossip.go` | 1070 | libp2p host, GossipSub topics, peer-scoring, mesh-health, per-shard topics |
| `p2p/internal/grpcserver/server.go` | 579 | the JVM-facing gRPC service (publish/subscribe/confirm) |
| `p2p/internal/chainsync/protocol.go` | 621 | libp2p request/response for active parent/range fetch |
| `p2p/internal/outbox/outbox.go` | 241 | in-memory durable-publish ledger (#196) |
| `p2p/internal/httpbridge/bridge.go` | 200 | opt-in HTTP debug/fallback (off by default) |
| `p2p/internal/config/config.go` | 190 | config + `NAKAMOTO_NUM_SHARDS` env read |
| `p2p/internal/metrics/metrics.go` | 214 | Prometheus counters/gauges |

**Deps** (`p2p/go.mod`): `go-libp2p v0.48.0`, `go-libp2p-pubsub v0.15.0`, `go-libp2p-kad-dht v0.39.0`,
`grpc v1.80.0`, `protobuf v1.36.11`, `prometheus/client_golang v1.23.2`; Go 1.25.

**How it is deployed / referenced:** built locally from `p2p/Dockerfile` by
`docker/bin/compose-runner.sh:404` (`docker build -t nakamoto-sidecar:test -f p2p/Dockerfile p2p/`),
layered in per-node via `docker/docker-compose.nakamoto-sidecar.yaml`. **One sidecar container per gl0
node** (`sidecar${SUFFIX}` next to `gl0${SUFFIX}`). Identity is an Ed25519 key **deterministically
derived from each node's ECDSA key** (`compose-runner.sh:619-629`, `main.go:74-108` `-derive-from-ecdsa`),
so the libp2p peer-id is stable and can be embedded in the seedlist as
`/dns4/sidecar-N/tcp/9500/p2p/<peer-id>` (`compose-runner.sh:637-641`). mDNS is disabled in-cluster
(`-disable-mdns`), forcing DHT-only discovery. Prometheus scrapes each sidecar at `:9501`
(`compose-runner.sh:1242-1247`).

**Sidecar-specific ops lore** (already learned the hard way): memory grows with uptime (gossip
dedup/cache sets); `docker-compose.nakamoto-sidecar.yaml:22-40` pins `mem_limit == memswap_limit ==
1024m` + `GOMEMLIMIT=900MiB` so the runtime GCs before the cgroup OOMs (the 2026-06-10 swap-thrash
that dropped attestation latency to the depth-k rail).

---

## 1. CURRENT SHAPE — sidecar owns gossip, JVM owns consensus + REST

### 1.1 Ownership split

**Sidecar owns (per gl0 node):** the entire libp2p/GossipSub P2P plane — host, peer discovery
(Kademlia DHT + seedlist + rendezvous, `gossip.go:391-445`), 8 universal GossipSub topics + 2
per-shard topic families, peer-scoring / mesh-shedding (`gossip.go:1015-1070`), the durable-publish
outbox (#196), and the ChainSync request/response protocol (`/nakamoto/chainsync/1.0.0`). The sidecar
treats **all payloads as opaque bytes** — it never deserializes a snapshot/block/checkpoint; the JVM
owns all serialization, signing, and validation (`sidecar.proto:64-75`, and the invariant restated in
`SIDECAR-BLS-ATTESTATION-AGGREGATION-DESIGN.md:20` — *"the sidecar makes the JVM verify less and
bigger; it NEVER verifies instead"*).

**JVM (Scala/http4s) owns:** consensus (Taktikos FSM), all crypto/verification, snapshot storage, the
MPT, and **all consumer-facing REST** (§3b). It talks to its own sidecar over localhost gRPC.

### 1.2 The boundary interface (protobuf/gRPC)

Defined in `p2p/proto/sidecar.proto`. Three gRPC services:

- **`SidecarService`** (`sidecar.proto:332-397`) — JVM → sidecar, on `127.0.0.1:50051`
  (`compose-runner`/`main.go:47`). 11 `Publish*` unary RPCs (snapshot, attestation, rumor,
  metagraph-binary, metagraph-attestation, allow-spend/dag/token-lock block, **shard-checkpoint**,
  **shard-checkpoint-attestation**, **fraud-proof**), `ConfirmFinalized` (outbox ack), a
  **server-streaming `Subscribe`** that pushes every received `GossipMessage` to the JVM, plus
  `PeerCount` / `Health`.
- **`ChainSyncOutbound`** — JVM → sidecar → peer: `FetchSnapshots`, `FindIntersection`,
  `GetPeerTip`, and `FetchMetagraphBinaries` (#259 active recovery).
- **`ChainSyncInbound`** — sidecar → JVM on `gl0:50053` (`main.go:50`): when a *peer* asks this
  node for data, the sidecar calls back into the JVM to serve it (`ServeSnapshots`,
  `ServeChainPoints`, `ServeMetagraphBinaries`).

The gossip envelope is `GossipMessage` — a `oneof body` with **11 arms** (`sidecar.proto:313-327`),
one per message type. The **flow-control / backpressure fields** on the wire:
`PeerCountResponse.mesh_*` per-topic mesh sizes including `mesh_shard_checkpoint_attestations`
(`sidecar.proto:411-423`); `ConfirmFinalizedRequest.message_ids` (the JVM's Phase-3-finality ack that
lets the sidecar drop outbox entries, `sidecar.proto:428-435`); `SubscribeRequest.topics`
(`sidecar.proto:404-407`, currently ignored — the JVM gets all types). The sidecar↔JVM gRPC uses
keepalive (`server.go:73-86`: enforce 20s, server ping 30s) so partitions are detected.

### 1.3 Data flow (prose)

**Publish (this node won a slot / produced a binary):** JVM `SidecarClient` → gRPC `Publish*` →
`server.go` `proto.Marshal` → `gossip.go` `topic.Publish` → GossipSub mesh → peer sidecars. For the
durable topics (metagraph-binary, allow-spend/dag/token-lock block, shard-checkpoint[-attestation]) the
publish is *also* recorded in the **outbox** (`server.go:141-268`) and re-gossiped every 30s
(`main.go:322-403`) until the JVM's Phase-3 finality hook calls `ConfirmFinalized`.

**Receive:** peer sidecar → GossipSub → local `subscribeAndRelay` goroutine per topic
(`gossip.go:709-735`) → per-topic buffered Go channel → the **single** `Subscribe` select-loop
(`server.go:304-463`) `proto.Unmarshal`s and `stream.Send`s each as a `GossipMessage` → JVM
`GossipStream.subscribe` (`GossipStream.scala:19`) turns the gRPC stream into an `fs2.Stream` → the JVM
demuxes by `oneof` arm: rumors → `SidecarRumorBridge` → bounded `rumorQueue`; snapshots →
`NakamotoSyncDaemon` bounded intake queue; shard checkpoints → `NakamotoSyncDaemon.handleShardCheckpoint`.

**Active recovery (gap):** `NakamotoSyncDaemon` stuck-detection (`NakamotoSyncDaemon.scala:64-65`, 20s
tick) → gRPC `ChainSyncOutbound.Fetch*` → sidecar opens a libp2p stream to a random peer
(`protocol.go:247-300`) → that peer's sidecar calls its JVM `ChainSyncInbound.Serve*` → bytes stream
back and are **re-fed through the normal admission path** (never trust-on-fetch, `sidecar.proto:504-508`).

---

## 2. KNOWN ISSUES / ROBUSTNESS GAPS

The canonical prior audit is `docs/nakamoto/QUEUE-BOUNDEDNESS-AUDIT.md` (2026-06-10). **It is now
partly stale — several TIER-1/2 items have since been fixed** (verified against HEAD): `rumorQueue` is
`Queue.bounded[Hashed[RumorRaw]](65536)` + tryOffer-drop (`SharedQueues.scala:27,31`,
`SidecarRumorBridge.scala:86`, `ConsensusRoutes.scala:52`); the snapshot intake queue is now
`Queue.bounded[pb.Snapshot](1024)` + tryOffer-drop (`NakamotoSyncDaemon.scala:842,889`);
`ShardBinaryBuffer` now **evicts-finalized-first then rejects-new** rather than the audit's "no
evict-on-finalize" (`ShardBinaryBuffer.scala:171,188-201`). The gaps that **remain**:

**G0 — HEAD constructs the GL0 rumor consumer but never starts it.**
`Main.scala` at HEAD `:259-325` assembles generic, legacy-consensus, and event
handlers and calls `GossipDaemon.make`, but never invokes either start method.
`SidecarRumorBridge.receive` still feeds the bounded queue
(`SidecarRumorBridge.scala:60-105`), while `consumeRumors` is reachable only from
those start methods (`GossipDaemon.scala:28-32,56-70,89-116`). Thus the legacy BFT
queue was not a live remote DoS; all GL0 generic/event rumor handlers were inert
instead. The worktree removes the six GL0 BFT handlers/queue, composes only the
generic and event handlers (`Main.scala:242-258`), then starts the consume-only
Nakamoto daemon after bootstrap and immediately before `Ready` on both startup
paths (`Main.scala:347-350,647-650`). The focused six-family/no-queue/source
regression is green and confirms ML0 BFT remains
(`GlobalLegacyBftIngressDisabledSuite.scala:26-82`; 8/8); the broader final
receipt/BFT containment selection is 35/35. Treat runtime queue
drain, validation, handler delivery, reconnect, and supervision/release as the
remaining integration qualification; delete the dormant generic `Consensus`
storage/routes compatibility shell separately.

**G0A — HIGH: dedicated Nakamoto topics start before GL0 bootstrap.**
`Main` allocates `Services` and consensus before entering rollback/genesis/restart
bootstrap (`Main.scala:196-225,331-350,647-650`; `Services.scala:257-306`).
`GlobalSnapshotConsensus` supervises `NakamotoSyncDaemon` during that allocation
(`GlobalSnapshotConsensus.scala:2192-2276`), and its snapshot worker immediately
invokes validation against chain, roster, and economic services
(`NakamotoSyncDaemon.scala:845-910,1243-1271`). An early peer snapshot can race
cold state. Gate all dedicated-topic processing on an explicit bootstrap-ready
capability or hold exact bounded input inert until readiness; test hostile early
delivery and failed/restarted bootstrap.

**G0B — MEDIUM: the generic bridge also precedes its consumer.**
`SidecarRumorBridge.receive` starts during consensus construction
(`GlobalSnapshotConsensus.scala:1121-1129`), but the new consumer starts only
after bootstrap (`Main.scala:347-350,647-650`). The bounded rumor queue can still
fill and drop during that interval. Move bridge activation behind readiness or
specify and test bounded cold-start quarantine/release semantics.

**G1 — the inbound gossip firehose funnels into ONE unbounded queue (the open TIER-1 audit item).**
`GossipStream.scala:23` is still `Queue.unbounded[F, Option[GossipMessage]]`, and the gRPC `onNext`
callback does a blocking `queue.offer(Some(value))` (`GossipStream.scala:40`) — so it **never applies
`request(n)` flow-control back to the sidecar**. Every topic (snapshots, attestations, rumors,
metagraph binaries, three block types, shard checkpoints/attestations, fraud proofs) crosses the
boundary through this single unbounded queue *before* demux. Downstream queues are bounded, so this is
usually drained fast — but under a CPU-saturated verify (the load-156 scenario in
`SIDECAR-BLS-ATTESTATION-AGGREGATION-DESIGN.md:12`) this is the accumulation point. `QUEUE-BOUNDEDNESS-
AUDIT.md:18` flags exactly this (*"bound + gRPC flow-control"*).

**G2 — one serial `Subscribe` stream ⇒ head-of-line coupling across all topics.** The JVM holds
exactly one `Subscribe` stream; `server.go:304-463` is a single `select` loop that `stream.Send`s every
type. gRPC stream-level flow-control means one slow message type (or a slow JVM consumer of it) blocks
`stream.Send`, which stalls the *whole* loop, so **every** topic's relay channel backs up and starts
dropping together (`gossip.go:727-731` loud-drop). The per-topic Go buffers (`config.go:151-167`) soften
this but the final funnel is serial. `server.go:296-299` explicitly notes "the JVM holds exactly one
stream."

**G3 — ChainSync request bounds are landed; global/concurrent admission remains open (DoS surface).**
`handleIncoming` now applies a sliding one-minute, 10-request limit per libp2p peer before reading the
request body and installs a 30-second stream deadline. The Go serve handlers reject more than 64
snapshot hashes, metagraph-binary hashes, or intersection points before connecting to the JVM; the
JVM independently enforces the 64-item bound on both hash-based streaming handlers. The obsolete
unbounded `[start,end]` protocol was removed instead of capped. Remaining exposure is aggregate
Sybil amplification and the absence of explicit global/per-peer in-flight stream and JVM-work
budgets; the 16 MiB frame bound and per-peer request rate do not prove bounded aggregate work.

**G4 — per-shard topics are absent from peer-scoring.** `buildPeerScoreParams` builds
`TopicScoreParams` for exactly the **8 universal topics** (`gossip.go:1016-1025`); the per-shard
`shard-checkpoint-<id>` / `shard-checkpoint-attestation-<id>` families (joined at `gossip.go:561-594`)
get **default (zero) topic scoring**. So `InvalidMessageDeliveriesWeight = -99` and the colocation/
behaviour penalties do **not** apply to shard-checkpoint gossip — a peer flooding a shard topic with
junk is not penalized on that topic. This is the consensus-load-bearing sharding path (checkpoints +
attestations + fraud proofs) running with the weakest Sybil protection.

**G5 — the per-shard relay buffer is a single fixed 256-slot channel shared across ALL shards.**
`gossip.go:282-283` allocates `shardCheckpointCh` / `shardCheckpointAttCh` at
`cfg.ShardCheckpointBufferSize` = **256** (`config.go:167`), and *every* joined shard's relay goroutine
fans into that **one** shared channel (`gossip.go:604-638`). The buffer **does not scale with
numShards** — 256 total, not 256/shard. When it fills, checkpoints are dropped (`gossip.go:629-634`
loud-drop) and recovery falls to the 30s outbox republish (or the still-design-only chainsync pull, §5).

**G6 — the snapshot orphan buffer is unbounded, no TTL.** `NakamotoSyncDaemon.scala:733` is
`Ref.of[F, Map[Hash, List[pb.Snapshot]]](Map.empty)` — keyed by missing-parent-hash, drained only when
the parent arrives. A parent that never arrives (stalled peer / abandoned branch) accumulates
`pb.Snapshot`s forever. Mitigated in practice by the #259 stuck-detection tick (20s cadence, 60s
refetch cooldown, `NakamotoSyncDaemon.scala:64-65`) which actively pulls the missing parent — but there
is no TTL/size cap as a backstop. (`QUEUE-BOUNDEDNESS-AUDIT.md:22` TIER-2 #3.)

**G7 — the outbox has a TTL but no size cap; in-memory only.** `outbox.go` prunes entries older than
`ttl` (1h, `outbox.go:198-215`) but has **no size cap** — it is bounded only by (distinct-payload
publish rate × 1h). Its own scaladoc flags "no exponential backoff, no persistence" as follow-ups
(`outbox.go:19-21`); a sidecar crash loses all in-flight durable messages (the JVM resubmits on
reconnect, which is the intended fallback). Separately, the mesh-health degraded-check only inspects
snapshot + attestation + `connectedPeers` (`gossip.go:929`) — metagraph-binary, block, and shard topic
meshes don't trigger the seedlist-reconnect recovery.

**G8 — `MeshMessageDeliveries` scoring is disabled.** `MeshMessageDeliveriesWeight: 0` "disabled until
we have rate telemetry" (`gossip.go:1054`). Intentional and documented, but it means the mesh cannot
penalize a peer that *joins* a topic mesh and then withholds deliveries (a lazy/eclipse vector) — the
one score component that catches "in the mesh but not forwarding."

---

## 3. IMPROVEMENT OPPORTUNITIES (ranked)

### 3a. Sidecar / gl0 side

| # | Opportunity | Tag | Value | Anchor |
|---|---|---|---|---|
| **S1** | Bound `GossipStream`'s queue and drive gRPC `request(n)` flow-control back to the sidecar; and/or split the single `Subscribe` into per-topic streams to kill head-of-line coupling. Closes the last open TIER-1 audit item + G2. | impl robustness | **HIGH** | G1/G2; `GossipStream.scala:23`, `server.go:304-463` |
| **S2** | **Partial landed 2026-07-13:** obsolete range/backfill RPC removed; 64-item inbound/outbound caps, per-peer sliding-window rate limit, and stream deadline enforced. Add explicit global/per-peer in-flight stream and JVM-work budgets with rejection metrics to close aggregate/Sybil amplification. | impl robustness | **HIGH** | G3; `protocol.go` ChainSync serve handlers |
| **S3** | Add the per-shard topic families to `buildPeerScoreParams`, and scale the shard relay buffer per-shard (or per-`(shard×mg)`) instead of one shared 256. Directly on the sharding critical path. | impl robustness | **MED** | G4/G5; `gossip.go:282,1016` |
| **S4** | Build the **sidecar BLS committee-attestation aggregation** slices (Phase 4/5 of `SIDECAR-BLS-ATTESTATION-AGGREGATION-DESIGN.md`) — collapse the committee-gate's O(N) per-attestation verify to O(1). *The aggregation approach is settled (BLS); the sidecar-side slices are unbuilt.* Gated on BouncyCastle 1.85. Efficiency win at scale, not a robustness fix. | new capability | **MED** (gated) | design §6 Phases 4-6; `gossip.go` publish sites, `server.go` |
| **S5** | Backstop the unbounded state: TTL/size-cap on `pendingParentRef`; size-cap (+ optional persistence) on the outbox. | impl robustness | **LOW** | G6/G7; `NakamotoSyncDaemon.scala:733`, `outbox.go:59` |
| **S6** | Broaden the mesh-health degraded-check to all topic families; enable `MeshMessageDeliveries` once per-topic rate telemetry exists. | impl robustness | **LOW** | G7/G8; `gossip.go:929,1054` |

### 3b. REST / consumer side

The consumer surface is large (~28 `*Routes.scala` files) and generally healthy, but two structural
gaps stand out. The **only** per-route middleware anywhere is `Timeout`, on exactly three route groups
(`SnapshotRoutes.scala:67`, `GossipRoutes.scala:35`, `StateChannelRoutes.scala:88`). **No rate-limiting,
no response-size cap, no server-side pagination limit exists on any public route.**

| # | Opportunity | Tag | Value | Anchor |
|---|---|---|---|---|
| **R1** | Add throttle / rate-limit middleware + response-size caps to the public routes. The exposure: `/latest/combined/mpt-entries` serves the **entire** MPT byte-map in one response with no pagination (`SnapshotRoutes.scala:156`); `/{ordinal}?full=true` (`:188`) and the `since`-driven follow routes let a caller name unbounded work. Today only `Timeout` protects them. | impl robustness | **HIGH** | R-gaps; `SnapshotRoutes.scala:67,156,188` |
| **R2** | Complete the **light-client / roots-only proof** surface. Present today: `/{address}/balance/proof` (MPT inclusion proof, `WalletRoutes.scala:27`), `POST /shard/{shardId}/proof` (GlobalStateKey inclusion under a metagraph subtree, `ShardProofRoutes.scala:84`), and NIPoPoW proofs (`NipopowRoutes.scala:100,125,137`). Missing for external verifiers (metakit-sdk / @zk-kit): **proof-of-absence** (`ShardProofRoutes` 404s on absent key; `GL0CurrencyBalanceRoutes.scala:59` returns balance 0 with no proof), **batch proof** endpoints, and streaming of large proofs. | new capability | **MED** | `WalletRoutes.scala:27`, `ShardProofRoutes.scala:84`, `NipopowRoutes.scala:100` |
| **R3** | Cap the `since`-driven diff window on `/global-follow/slice?since=` (`GlobalFollowRoutes.scala:117`) and `/global-follow/changeset?since=` (`:127`) — today the caller controls the range; the ring already falls back to a full slice, so a bounded window is a safe cap. The changeset route is already a good citizen (scodec binary octet-stream, `GlobalFollowRoutes.scala:80`). | impl robustness | **MED** | `GlobalFollowRoutes.scala:104-127` |
| **R4** | Offer a long-poll / SSE / stream "latest-finalized" subscribe so consumers stop hot-polling `/latest*`. The primitives exist (`GossipRoutes` already streams via fs2, `GossipRoutes.scala:84`; the changeset ring already emits per-ordinal deltas). | new capability | **LOW** | `SnapshotRoutes.scala:100`, `GlobalFollowRoutes.scala:127` |

---

## 4. BEARING ON SHARDING (the non-regression bar)

The networking layer **does** carry the multi-shard/multi-metagraph traffic: gossip envelope arms
9/10/11 are `ShardCheckpointWire`, `ShardCheckpointAttestationWire`, `FraudProofEnvelopeWire`
(`sidecar.proto:323-325`), and the sidecar has dedicated per-shard topics + relays.

**The boundedness risk that scales with #shards × #metagraphs is real and specific:**

1. At the audited source baseline, **every gl0 subscribes to every shard's topics**
   because the sidecar *eagerly* joins shards `0..M-1` at startup (`gossip.go:311-328`,
   `config.go:47-59`, driven by `NAKAMOTO_NUM_SHARDS`). The design's load-shedding payoff
   (`HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md §6.4:736-738` — *"non-members don't subscribe … shed the
   network load"*) is **not realized yet**. Gossip fan-in therefore scales O(numShards) on every node.
   This transport subscription is not execution membership or a requirement that
   ordinary noncommittee GL0 nodes replay sharded CL1. Execution membership is the
   separate public deterministic VK-hash subset; every GL0 validator still executes
   the direct native GL1 path and global settlement kernel.
2. That fan-in lands in a **single fixed 256-slot shared relay channel** (G5) — it does **not** grow
   with M. Per-envelope committee-verify cost is `K_S × 3` crypto (`design §11.1:1197-1200`), the CPU
   load the BLS aggregation (S4) targets.
3. **Non-regression is protected at the boundary:** at `numShards ≤ 1` the sidecar performs **zero**
   shard-topic joins and is byte-identical to pre-sharding (`gossip.go:311` guard, `config.go:56-58`).
   So the risk lives **entirely in the `numShards = K` path** — sharding *adds* the load without a
   proportional buffer, it does not regress the K=1 path.

---

## 5. THE IMPLEMENTATION-CORRECTNESS QUESTION WORTH FRONTIER REASONING

**Is the shard checkpoint/attestation gossip provably BOUNDED *and* LIVE under adversarial load at
`numShards × numMetagraphs` scale?**

This is a genuine liveness+boundedness proof, not mechanical hardening, and it sits on the **active
sharding critical path** (the non-regression bar in `PORTFOLIO.md §0`). The pieces interact
adversarially:

- Delivery is **drop-on-full** at two hops — the shared 256 relay channel (G5, `gossip.go:629`) and the
  serial `Subscribe` funnel (G2) — so under fan-in pressure checkpoints are *lost*, not backpressured.
- Recovery of a lost checkpoint **cannot** come from re-gossip: GossipSub dedups identical bytes by
  content-hash `msgid` (`gossip.go:203-206`), so a producer re-publishing the same checkpoint is
  dropped as a duplicate until the seen-cache TTL — the **run-20 5m40s hole**
  (`SHARD-CHECKPOINT-CHAINSYNC-DESIGN.md §1:44-52`). The designed fix — a **pull-based** shard
  chain-sync — is **design-only** (that doc is "draft for critic review, no code commitment"), so today
  the only recovery is the 30s outbox republish (which, being the *same bytes*, hits the same dedup for
  peers that never saw the first forward).
- The failure mode is documented and severe: a missed checkpoint → shard quorum missed → the
  `T_depth1` depth-fallback adopts a **node-local** genesis → **permanent divergence**
  (`SHARD-CHECKPOINT-CHAINSYNC-DESIGN.md §1:50-52`).

The question to escalate: **can an adversary (or merely numShards × numMetagraphs honest fan-in) drive
the shared relay buffer full often enough that checkpoints drop faster than recovery replaces them,
stalling shard quorum?** That is a stability argument about the drop-and-recover loop — the kind of
bound (buffer sizing vs fan-in rate vs recovery latency) that wants frontier reasoning, and it is the
one networking question with safety/liveness teeth on the shard path.

Everything else in §2/§3 (G1, G3, R1, R2) is **mechanical hardening** — worth doing, but Opus/Sonnet
build-work, not a frontier question. **S4 (BLS aggregation) and R2 (light-client completeness) are
settled-approach capabilities** with unbuilt slices, not open design questions.

---

## 6. Anchor index

Sidecar: `p2p/proto/sidecar.proto`, `p2p/internal/gossip/gossip.go`,
`p2p/internal/grpcserver/server.go`, `p2p/internal/chainsync/protocol.go`,
`p2p/internal/outbox/outbox.go`, `p2p/internal/config/config.go`, `p2p/cmd/sidecar/main.go`,
`docker/docker-compose.nakamoto-sidecar.yaml`, `docker/bin/compose-runner.sh:401-641`.
JVM boundary: `…/consensus/nakamoto/GossipStream.scala`, `SidecarClient.scala`,
`SidecarRumorBridge.scala`; `…/modules/SharedQueues.scala`;
`…/dag-l0/…/nakamoto/NakamotoSyncDaemon.scala`; `…/sharding/ShardBinaryBuffer.scala`;
`…/nakamoto/MetagraphOrphanBuffer.scala`; `…/gossip/GossipDaemon.scala`.
REST: `…/http/routes/SnapshotRoutes.scala`, `WalletRoutes.scala`, `GlobalFollowRoutes.scala`;
`…/dag-l0/http/routes/{ShardProofRoutes,NipopowRoutes,GL0CurrencyBalanceRoutes,FinalityTriggersRoutes,ShardCheckpointRoutes}.scala`.
Docs: `QUEUE-BOUNDEDNESS-AUDIT.md`, `SIDECAR-BLS-ATTESTATION-AGGREGATION-DESIGN.md`,
`SHARD-CHECKPOINT-CHAINSYNC-DESIGN.md`, `SYNC-PROTOCOL.md`, `HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md`.
