# Queue / Buffer Boundedness Audit

**Date:** 2026-06-10. **Trigger:** the gossip `rumorQueue` was `Queue.unbounded` and OOM'd 4/5 gl0 nodes under the spend/double-spend workload (456M retained circe `Json$JNumber` = 7.3 GB; 80× heap asymmetry leader-vs-follower). This audits every other queue/buffer for the same bug class.

## The bug class (what to look for)
A queue is dangerous only when **three** factors combine: (1) holds LARGE payloads (parsed circe.Json, `Signed[Snapshot]`, big `Array[Byte]`), (2) fed by a FIREHOSE/peer-receive producer, (3) drained by a SLOW or SERIAL consumer. Small-payload queues (ADTs, ordinals) or fast/immediate consumers are low-risk even when unbounded — bounding them blindly risks deadlock for no memory benefit. **Do not mass-bound.**

## FIXED this session
| Queue | Was | Now |
|---|---|---|
| `SharedQueues.rumorQueue` | `Queue.unbounded[Hashed[RumorRaw]]` (circe.Json) | `Queue.bounded(65536)` + `parEvalMap(N)` validation in GossipDaemon + **tryOffer-drop** at the two peer-receive offer sites (SidecarRumorBridge, ConsensusRoutes/push-rumor) so the bound cannot migrate upstream; supervised blocking offer kept for local `Gossip.spread`. Legacy pull-gossip modes may replay a drop, but Nakamoto mode does not run those rounds: inbound overload remains lossy until durable retry/outbox recovery lands. |

Prior-fixed (context): `NakamotoChainStore.byHash` (keepDepth=k₁=255, prune-on-finalize), sidecar 384m→1g+GOMEMLIMIT, combined-checkpoint retention 2→64+boot-rescan, committee-VRF memoization, gl0 heap 7168→12288m.

## Open findings — ranked, with the bug-class test applied

### TIER 1 — same class, fix next (large payload OR firehose + serial)
1. **`GossipStream` sidecar bridge queue** — `Queue.unbounded[Either[Throwable, Option[GossipMessage]]]` (`GossipStream.scala:45-68`). The truthful `SubscribeStarted` lifecycle and exact error propagation do not bound data callbacks. This is directly upstream of rumor and dedicated-topic dispatch, so a network or reachable-sidecar firehose can migrate bounded Go pressure into JVM heap. **Fix: bounded item+encoded-byte queue, manual gRPC `request(n)` flow control, and a termination/error control path that cannot wait behind a full data queue.**
2. **dag-l0 consensus output queues** — 8× `Queue.unbounded` (dag-l0 `Queues.scala:25-32`) holding `Signed[Block]`, `StateChannelOutput`, allow-spend/token-lock/stake/collateral/KES blocks. This is now a confirmed peer firehose for three native GL1 families: dedicated gossip handlers deserialize and detached-enqueue DAG/allow-spend/token-lock blocks without inner signature/admission validation (`NakamotoSyncDaemon.scala:1002-1013,1883-1963`; `Services.scala:241-259`). The serial publisher re-signs each event with the GL0 key before bounded-mempool rejection (`GlobalSnapshotEventsPublisherDaemon.scala:55-115,120-134`). **Fix: authenticate before enqueue; bound by bytes/items with per-peer/topic quotas; drain through supervised bounded workers. Universal GL0 execution remains mandatory after admission.** Same queue pattern in dag-l1 (×6) and currency-l1 (×2) needs separate ingress analysis.

### DORMANT HAZARD + SEPARATE CONSUMER DEFECT

The GL0 `ConsensusEventLoop` command queue was **not** an active peer-driven heap
DoS at HEAD. Although Nakamoto construction allocated an unbounded undrained queue
and registered six legacy handlers, `Main` only constructed `gossipDaemon` and
never called either start method (`Main.scala` at HEAD `:259-325`). No
`consumeRumors` fiber invoked those handlers. Starting that consumer alone would
have activated the hazard, so removal must precede consumer startup.

The source-proven baseline defect was a missing consumer for the **bounded**
shared rumor queue. `SidecarRumorBridge.receive` offered inbound rumors
(`SidecarRumorBridge.scala:60-105`), but no daemon validated/dispatched them; the
queue could fill and later input would drop. Exact workflow severity needs an
integration test because dedicated Nakamoto topics have separate consumers. The
worktree removes GL0's six BFT handlers/unbounded loop and adds a three-phase
`ConsensusInputGate`. Both sidecar receive effects remain inert until root-checked
local bootstrap and chain seed succeed and the generic consumer, event/collateral
daemons, and HTTP listeners are running. Main then activates both streams, requires
typed exact role/topic/session/generation acknowledgements from one sidecar process,
and publishes `Ready` under the serialized readiness lease. Production starts
fenced and pauses before committed lane invalidation. ChainSync returns
`UNAVAILABLE` before seed success. This closes the direct cold queue-fill/mutation
and local subscription-ordering windows. It does not authenticate a restored disk
head or establish catch-up, mesh reachability, finality, or economic validity.
Bounded delivery/drain ownership, durable recovery for traffic missed by delayed
subscription, and the cold-restart policy remain open. The dormant generic `Consensus` storage/routes
compatibility shell is cleanup, not an active queue path. Dedicated-topic detached
workers and the newly confirmed native-block unbounded ingress are separate HIGH
findings, not closed by the startup gate.

### TIER 2 — unbounded Ref accumulators with a fragile prune trigger
3. **`NakamotoSyncDaemon` pendingParentRef** — `Ref[Map[Hash, List[pb.Snapshot]]]` keyed by missing-parent-hash, drained only on parent receipt. If a parent never arrives (stalled peer / orphan chain), it accumulates `pb.Snapshot` forever. **Fix: TTL (age-out) + size cap.**
4. **`ShardBinaryBuffer`** — `binary-buffer-cap=4096` per shard, REJECT-NEW at cap, **no evict-on-finalize** (scaladoc admits "later slice"). Holds `Array[Byte]` (not Json), so it's a byte-array footprint not the JNumber leak — but pins up to 4096×numShards binaries until restart. **Fix: prune-on-finalize.**

### TIER 3 — bounded-but-serial (back up only under slow I/O), monitor not fix-now
5. `SnapshotStorage` offload/cutoff queues — `Queue.unbounded[SnapshotOrdinal]` (small payload, serial file-I/O consumer). Low memory risk (ordinals); could lag on slow disk. **Fix optional: bound 512.**

### OK (verified bounded / pruned / small)
EventMempool (cap+clear-on-finalize), MptOverlay pending branches (cap+evict-on-commit, watermark), MetagraphOrphanBuffer (FIFO cap 256), EtaStateManager walkCache (watermark), GossipRoundRunner (bounded), ClickHouse sinks (bounded), all fs2 Topics (bounded per-subscriber), StateChangesAccumulator staging (cap 2048 + finalize-prune), SnapshotStorage tentative map (depth+reorg-window bounded).

## Cross-cutting recommendation
Add a `*_queue_size` Prometheus gauge to every retained queue (only the consensus command queue has one today). The rumor leak was invisible for the entire campaign because nothing measured queue depth. This folds into task #25 (liveness/lag metrics + grafana row). A single "queue depths" grafana row turns this whole class of bug from a post-mortem heap-histogram into a live panel.
