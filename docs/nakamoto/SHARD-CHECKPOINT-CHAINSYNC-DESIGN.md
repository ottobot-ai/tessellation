# Shard-Checkpoint Chain-Sync (pull-based recovery) — Design Proposal

> **HISTORICAL FORENSIC DESIGN, NOT THE CURRENT VALIDITY RULE.** The missing-parent
> recovery evidence remains useful, but the `T_depth1` shard fallback and
> `numShards=1` bypass described below are retired. Pulling bytes never makes a
> checkpoint valid: every execution signature requires replay, distinct
> `kQuorum` is mandatory, positive watchtower coverage precedes GL0 inclusion,
> and one outstanding checkpoint is released only by its exact containing GL0
> Phase-2 hash. Use ADR-0016/0017 and the consensus lifecycle for implementation.

**Status:** draft for critic review. No code commitment. Written 2026-06-12 against
`feature/serde-typeclass-shim` (HEAD `beaef8fa8` — the intake-demux-completion commit,
on top of Tier-1 idempotence `c995c088a` and #44 `09af4447e`). Authored after run-20
root-caused the shard stall to a boot-window checkpoint loss that gossip alone cannot
recover.

**Thesis (one line):** *The shard checkpoint chain has the global chain's fork-DAG and
gossip, but not its **recovery**. A missed checkpoint cannot be healed by re-gossip
(GossipSub dedups identical bytes), so it waits minutes for the seen-cache TTL — long
enough that cluster quorum is missed and the `T_depth1` depth-fallback adopts divergent
local segments. Give shards a **pull-based chain-sync** mirroring the global
`ChainSyncManager`: detect a gap, pull the missing checkpoint from a peer, re-feed it
through the normal accept path.*

**Scope:** the gl0-side shard-checkpoint recovery path at `numShards > 1`: gap detection
in `NakamotoSyncDaemon.handleShardCheckpoint` + a periodic absence check, a pull client,
and a serve handler reading `ShardChainStore`. Out of scope: the producer
(`ShardCheckpointProducer`, Tier 1 — unchanged), the fork choice (`compareAnchoredMaxvalid`,
unchanged), and anything at `numShards = 1` (inert).

Cross-references:

- [`SHARD-CHECKPOINT-MONOTONICITY-DESIGN.md`](./SHARD-CHECKPOINT-MONOTONICITY-DESIGN.md) — Tier 1 (idempotent production); §9 predicted the cross-node case this doc addresses.
- [`HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md`](./HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md) — the v1 checkpoint architecture.
- Global ChainSync (the thing we mirror): `ChainSyncManager` (puller, `requestMissing`) and `ChainSyncServer` (`serveSnapshots`/`serveMetagraphBinaries`). The former ordinal-only `ChainSyncRequestQueue` was deleted with the unsafe receiver-local attestation-finality sink that owned its only caller.
- `ShardChainStore` — the `byParent` orphan buffer (the gap-detection primitive already exists).
- Memory: `[[per-slot-rewire-runs12-13]]` (runs 18–20; the run-20 forensics), `[[feedback-prefer-hocon-over-sysenv]]`, `[[feedback-greenfield-no-wire-compat]]`.

---

## §1 Problem — what run-20 proved

run-20 (`5gl0/2shard/2mg`, Tier 1 active): churn gone, **first quorum ever**
(`shardId=1 shardOrd=2 distinctSigners=4`). But progress stalled — shard0 genesis
**forked** and never adopted, gl0 stuck at 1/2 metagraphs. Root cause (live forensics on
gl0-4):

| Time | Event |
|---|---|
| 17:43:39 | node Ready |
| 17:45:01.187 | sidecar **receives** gl0-1's shard0 genesis (instant from producer) |
| 17:45:46 | JVM processes its first shard checkpoint (a shard1 one) |
| **17:50:41** | sidecar **re-delivers** the SAME genesis → JVM finally processes it (`becameBestTip=false`) |

The genesis arrived at the sidecar at 17:45:01 but the JVM didn't process it until
**17:50:41** — a 5m40s hole — and only because GossipSub re-gossiped it after the
seen-cache TTL expired. In that window shard0 genesis never reached cluster quorum, so
the `T_depth1` depth-fallback adopted a **node-local** genesis on each node → divergent
anchors → permanent split.

### §1.1 Why push-based recovery is structurally incapable

The obvious "fix" — have the producer re-gossip its held checkpoint — is exactly what
**Tier 1 already does** (re-publish every `republishEveryTicks`). It does **not** recover
a missed checkpoint, because:

- Tier 1 re-publishes the **same bytes** (that is the whole point — stable identity so
  attestations concentrate). Same bytes ⇒ **same content-hash `msgid`**.
- GossipSub **dedups by `msgid`** at the sidecar's seen-cache. Every re-publish to a node
  that missed the first forward is dropped as a duplicate — until the seen-cache TTL
  expires (the observed ~5 min).

So the gossip layer actively **suppresses** the only push-recovery signal we have. This
is precisely why the global chain never trusted gossip alone and built `ChainSyncManager`
to **pull**. Shards inherited the fork-DAG but not the puller.

> A mempool is **not** the missing piece — `ShardBinaryBuffer` already accumulates raw
> binaries per shard. The missing piece is **chain-sync**: a way to pull a missing
> *checkpoint* segment.

---

## §2 The design — mirror the global ChainSync for shard checkpoints

Three parts, each mirroring an existing global-chain mechanism:

### §2.1 Gap detection (two triggers)

- **(T1) Orphan-by-hash (out-of-order).** `ShardChainStore` already buffers a checkpoint
  whose `parentCheckpointHash` is absent as an orphan in `byParent` (it is stored but not
  `connected`; when the parent lands, connectivity cascades). Today nothing *fetches* the
  missing parent. New: when `handleShardCheckpoint` stores a checkpoint whose parent is
  absent, signal the puller to fetch `(shardId, parentCheckpointHash)`. This is the exact
  analogue of the global daemon signalling `ChainSyncManager.requestMissing(parentHash)`.
- **(T2) Absence-by-ordinal (the boot/genesis case).** A *missed genesis* is **not** an
  orphan — genesis has `parent = Hash.empty`, and a node that missed it simply has an
  empty store, no gap to "connect". So T1 cannot catch it. New: a periodic per-shard
  check — "I have no checkpoint at `adoptedOrd + 1` (or my tip is below a peer's adopted
  watermark) and have been stuck for > `stuckMs`" → pull `(shardId, ordinal = myTip+1 ..
  peerTip)`. This mirrors the global pending-parent `requestMissing` and stuck-detection
  recovery pattern. The producer's existing `awaiting-embed` / frozen-watermark state
  is the stuck signal.

### §2.2 Pull client

`ChainSyncManager`-style: `fetchShardCheckpoints(shardId, by: Hash | OrdinalRange):
F[List[Signed[ShardCheckpoint]]]`, background fiber, **deduped** by in-flight key
(mirror `requestMissing`'s `inflightRef`), best-effort (on any error log + return empty;
the next tick retries). Fetched checkpoints are **re-fed through the normal
`handleShardCheckpoint` accept path** (evaluate → store → attest), so a pulled checkpoint
is validated identically to a gossiped one and cascades connectivity to waiting orphans —
exactly how `ChainSyncManager` re-feeds fetched snapshots through `handleSnapshot`.

### §2.3 Serve handler

A peer serves shard checkpoints from its **local `ShardChainStore`** by hash or ordinal
range, **read-only** (a peer fetch never mutates the server's admission/adopt state —
correction-A invariant from the global `serveMetagraphBinaries`). Returns the
`Signed[ShardCheckpoint]` wire bytes the puller re-feeds.

---

## §3 Transport — two options (decision needed)

The global ChainSync pulls via the **sidecar's `ChainSyncOutbound` gRPC → libp2p stream**
to a peer (NAT-traversal + peer-discovery via the sidecar). Two ways to carry the shard
pull:

- **Option A — extend the ChainSync libp2p protocol.** Add `FetchShardCheckpoints`
  request/response to the `.proto`, handle it in the Go sidecar (open a libp2p stream to a
  peer's sidecar, which asks that peer's JVM to serve), mirror `fetchMetagraphBinaries` /
  `serveMetagraphBinaries`. **Pro:** consistent with the global path; reuses NAT/discovery.
  **Con:** cross-language (protobuf + Go sidecar change).
- **Option B — JVM-direct HTTP.** Add an HTTP route (`GET
  /shard-checkpoints/{shardId}/by-hash/{h}` and `…/by-ordinal/{n}`) served from
  `ShardChainStore`, and a JVM HTTP-client puller using the peer IP:port from
  `clusterStorage`. **Pro:** JVM-only, no Go/protobuf change, smaller blast radius; the
  shard committee is gl0-internal and HTTP-reachable in-cluster. **Con:** a second pull
  transport (not reusing ChainSync); relies on direct gl0↔gl0 HTTP (fine in-cluster, but
  not NAT-traversed like libp2p).

**Recommendation: Option B (JVM HTTP) for the first cut** — it removes the cross-language
dependency, ships entirely in Scala, and the shard committee is always a set of gl0 peers
with known in-cluster addresses (the same peers the e2e and existing routes already reach
over HTTP). If a future deployment needs NAT traversal between gl0 operators, migrate to
Option A. (To be confirmed in review — see §6.1.)

---

## §4 Determinism, safety & interaction

- **Determinism / #261:** unchanged. A pulled checkpoint re-enters the SAME
  `handleShardCheckpoint` → `evaluate` (committee re-exec) → `store` path as a gossiped
  one; nothing about admission or the adopt decision changes. Pull only changes *whether*
  a node has the bytes, not *how* it judges them.
- **Serve is read-only:** a fetch never mutates the server's state (the global
  `serveMetagraphBinaries` correction-A invariant). No amplification of one node's view
  onto another.
- **Interaction with Tier 1:** complementary. Tier 1 stabilises the checkpoint *identity*
  (so attestations concentrate); chain-sync ensures every node *has* that identity quickly.
  Together: producer mints once + holds (Tier 1) → peers that missed it **pull** it in
  seconds (chain-sync) → quorum forms → adopt. The dedup that defeats Tier-1 re-gossip is
  irrelevant to a pull (request/response, not broadcast).
- **Interaction with the depth-fallback (`T_depth1`):** with reliable pull, genesis/each
  ordinal reaches all nodes fast → quorum forms → the depth-fallback **rarely fires**.
  The fallback's *local* adoption on quorum-miss (the divergence amplifier) is a **separate
  latent issue** (it should be deterministic or quorum-gated at low ordinals); chain-sync
  removes its trigger but does not fix it. Track separately.
- **Bounded work:** dedup the in-flight set (per `(shardId, hash)` / `(shardId, ordinal)`),
  rate-limit the absence-tick per shard, cap the pulled range. Pull is best-effort; a
  failed pull just retries next tick.

---

## §5 The push side stays (don't remove gossip)

Gossip remains the *fast* path (first delivery, low latency). Chain-sync is the *recovery*
path for what gossip drops. This mirrors the global chain exactly: gossip + ChainSync, not
one or the other. We are **not** removing the sidecar GossipSub checkpoint topics.

Optional cheap complement (not a substitute): shortening the sidecar GossipSub seen-cache
TTL would make Tier-1 re-publishes recover faster — but it is a band-aid (re-gossip
amplification, no delivery guarantee) and is orthogonal to this design. Mentioned only so
review can decide whether to also tune it.

---

## §6 Open decisions (for review before coding)

1. **Transport A vs B (§3).** Recommend B (JVM HTTP). Confirm gl0↔gl0 in-cluster HTTP is
   always reachable for the committee set (it is for the e2e; verify no deployment assumes
   sidecar-only connectivity between gl0 operators).
2. **Absence-tick trigger precision (T2).** What is the exact "stuck" predicate — `myTip+1
   absent for stuckMs`, or `peer adoptedOrd > my adoptedOrd + slack`? The former needs no
   peer-state query; the latter needs a "peer shard tip" read (a new lightweight route).
   Recommend the former (local-only trigger) for the first cut; pull `myTip+1` speculatively
   from a random committee peer.
3. **Peer selection.** Random committee member, round-robin, or the checkpoint's signer?
   Recommend a random committee peer (the committee is who has it), with fallback rotation
   on empty/error.
4. **HOCON knobs** (per `[[feedback-prefer-hocon-over-sysenv]]`): `stuckMs`,
   `maxPullRange`, `absenceTickInterval`, `pullDedupTtl`. Typed fields under
   `nakamoto.sharding.checkpoint`, env-overridable in `application.conf`.
5. **Genesis specifically.** The boot window is the worst case (no tip yet). Confirm T2
   fires from an empty store (tip = Genesis, `adoptedOrd = 0`) so a node that missed
   genesis pulls ordinal 1 without waiting for an orphan it will never get.

---

## §7 Implementation sketch (non-binding — the sub-agent refines)

- **Serve (Option B):** `ShardCheckpointRoutes` — `GET /shard-checkpoints/{shardId}/by-hash/{hash}`
  and `…/by-ordinal/{n}`, reading `shardAcceptanceDeps.registry(shardId).chainStore`
  (`getByHash` / a new `getByOrdinal` walking from bestTip). Register in `HttpApi`.
- **Pull client:** `ShardCheckpointSync.make` mirroring `ChainSyncManager` — `inflightRef`,
  `fetchByHash(shardId, hash)` / `fetchByOrdinal(shardId, n)` via the HTTP peer client,
  re-feed each result through `handleShardCheckpoint`.
- **T1 trigger:** in `handleShardCheckpoint`, after `store`, if the parent is absent
  (`getByHash(parentCheckpointHash)` is `None` and `parentCheckpointHash =!= Hash.empty`),
  `shardCheckpointSync.fetchByHash(shardId, parentCheckpointHash)`.
- **T2 trigger:** a `fs2.Stream.fixedRate(absenceTickInterval)` per active shard — if
  `bestTip.shardOrdinal < adoptedOrd + 1`-style stuck (or no checkpoint at `tip+1` for
  `stuckMs`), `fetchByOrdinal(shardId, tip+1)`.
- **Wiring:** `shardAcceptanceDeps.registry` gives the per-shard `chainStore`; the
  committee peer set is `shardAcceptanceDeps.committeeMembership(shardId, epoch)` →
  `clusterStorage` for addresses.

## §8 Test plan

- **Unit (serve):** store N checkpoints, assert `by-hash` / `by-ordinal` return the exact
  envelopes; absent → 404/empty; serve does not mutate store state.
- **Unit (pull/re-feed):** a stub peer client returns a checkpoint; assert the puller
  re-feeds it through the accept path and the orphan cascade connects a waiting descendant;
  assert in-flight dedup (two requests for the same hash → one fetch).
- **Unit (T2 absence):** empty store + stuck → asserts a `fetchByOrdinal(_, 1)` fires
  (the genesis-miss case).
- **e2e gate:** re-run `5gl0/2shard/2mg` (`just clean-data`, `pipefail`); assert a node
  that misses genesis **pulls** it within seconds (not minutes), shard0 reaches quorum,
  `adoptedShardOrd` climbs on BOTH shards, gl0 reaches 2/2 within the multi-metagraph
  budget, `currency`/`allow-spends`/`spend` proceed.

## §9.5 Review addendum (architect sub-agent + code-verified, 2026-06-12)

A Plan sub-agent proposed a file-by-file impl and critiqued this doc; the load-bearing
claims were re-verified against the code. Net decisions:

- **Transport = Option B (JVM HTTP) CONFIRMED feasible.** The primitives exist:
  `PeerResponse` (`PeerResponse.scala`) + `L0GlobalSnapshotClient` (a worked by-ordinal
  peer GET) + `ClusterStorage.getPeer` + `Peer.toP2PContext`. Framing correction (F1): the
  global puller's transport is the sidecar libp2p gRPC, NOT HTTP — so Option B mirrors the
  ChainSync **logic** (dedup, re-feed, read-only serve), not its transport. State that
  explicitly; "mirror the global ChainSync" applies to the logic only.
- **SERVE THE WIRE BYTES, NOT `Signed[ShardCheckpoint]` JSON (key refinement — eliminates
  the only real correctness risk).** `handleShardCheckpoint` consumes
  `pb.ShardCheckpointWire` (verified, `NakamotoSyncDaemon.scala:2141`), and the gossip path
  already proves the wire round-trip is hash-stable (run-20 reached `distinctSigners=4`,
  which requires every node to compute the same hash from the gossiped wire). So the serve
  route returns `signedShardCheckpointToWire(stored).toByteArray` (protobuf bytes) and the
  puller feeds those bytes (`ShardCheckpointWire.parseFrom`) straight into
  `handleShardCheckpoint` — byte-identical to gossip, so the pulled checkpoint gets the SAME
  hash. This sidesteps the agent's uncertainty #3 (a `Signed→JSON→Signed` round-trip that
  could fork the hash) entirely. §2.2/§7 should serve wire bytes, not `Signed` JSON.
- **`getByOrdinal` ALREADY EXISTS** (`ShardChainStore.scala:123`/`:507`, walks canonical
  chain from `bestTip`, `None` for future/orphan ordinals). §7's "a new `getByOrdinal`" is
  wrong — it's existing, and it serves canonical-only (a safety plus: a puller never gets a
  peer's orphan as canonical).
- **The re-feed closure is owned by the DAEMON, not `ShardCheckpointSync`** (F3).
  `handleShardCheckpoint` is `private` and needs `shardAcceptanceDeps`/`emitter`/`selfId`/
  `logger` — all in scope only inside `NakamotoSyncDaemon.run`. So thread
  `ShardCheckpointSync` INTO the daemon (new optional param next to `shardAcceptanceDeps`),
  and let the daemon wire T1/T2 + the re-feed. §7's "ShardCheckpointSync.make … re-feed"
  layering is inverted.
- **Serve route = p2p, peer-authenticated, on `p2pApp`/`p2pPort`** (F5) — shard checkpoints
  are pre-finality consensus artifacts; mount in `HttpApi.p2pRoutes` behind
  `requestVerifierMiddleware` (NOT a public route), reached on `p2pPort` (what
  `Peer.toP2PContext` selects). Registry reaches the route via the `depsRef.set`-from-
  consensus pattern (mirror `nipopowProofProviderRef`), avoiding an `HttpApi.make` signature
  change.
- **Genesis peer-resolution (F4): for v1 use `clusterStorage.getResponsivePeers` (any peer)
  rather than `committeeMembership`** — at an empty store the puller has no checkpoint to
  read `epoch` from for the committee draw, and the serve is read-only/safe regardless of
  who answers. The T2 predicate must special-case `bestTip == None` ⇒ pull ordinal 1
  (the §7 "`bestTip.shardOrdinal < …`" predicate NPEs on the empty-store genesis case).
- **Add a by-ordinal-RANGE serve endpoint** (F6) capped at `maxPullRange`, so a deep gap
  (missed ords 1..N) recovers in one round-trip instead of N orphan-triggered ones.
- **T2 is a NEW `fs2.Stream.fixedRate` stream** modeled on the #259 metagraph
  `stuckDetectionStream` (`NakamotoSyncDaemon.scala:890`) with a per-shard cooldown Ref
  (F8) — there is no existing per-shard tick to reuse, and the producer's awaiting-embed
  state lives producer-side, not in the daemon.
- **HOCON knobs** (verified pattern): `stuckMs`, `maxPullRange`, `absenceTickIntervalMs`,
  `pullDedupCooldownMs` as defaulted `ShardCheckpointConfig` fields + `${?ENV}` keys.

## §9 Non-goals

- Not removing gossip (it's the fast path).
- Not changing the producer (Tier 1), the fork choice, or the verify/adopt determinism.
- Not fixing the `T_depth1` depth-fallback's local-adoption (separate; chain-sync removes
  its trigger).
- No wire-format compat ceremony (`[[feedback-greenfield-no-wire-compat]]`).
