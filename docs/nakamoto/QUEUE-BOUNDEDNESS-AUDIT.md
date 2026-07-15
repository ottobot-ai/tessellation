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
1. **`GossipStream` sidecar bridge queue** — `Queue.unbounded[Option[GossipMessage]]` (GossipStream.scala:23). The layer directly upstream of rumorQueue; protobuf bytes (smaller than parsed Json, but the same inbound firehose). The downstream tryOffer-drop prevents migration into the bounded rumor queue, but HEAD had no `GossipDaemon` consumer for that queue; the current worktree adds it. The upstream stream still needs a bound with backpressure to the gRPC `request(n)` observer. **Fix: bound + gRPC flow-control.**
2. **dag-l0 consensus output queues** — 8× `Queue.unbounded` (dag-l0 Queues.scala:25-32) holding `Signed[Block]`, `StateChannelOutput`, allow-spend/token-lock/stake/collateral/KES blocks — LARGE payloads, serial merge consumer in `GlobalSnapshotEventsPublisherDaemon`. Producer is per-consensus-round (not a firehose), so lower-rate than gossip, but under multi-mg/multi-shard churn the serial merge can lag. **Fix: bound each to ~depth-k (256-512); consider parEvalMap if the publisher I/O is the bottleneck.** Same pattern in dag-l1 (×6) and currency-l1 (×2).

### DORMANT HAZARD + SEPARATE CONSUMER DEFECT

The GL0 `ConsensusEventLoop` command queue was **not** an active peer-driven heap
DoS at HEAD. Although Nakamoto construction allocated an unbounded undrained queue
and registered six legacy handlers, `Main` only constructed `gossipDaemon` and
never called either start method (`Main.scala` at HEAD `:259-325`). No
`consumeRumors` fiber invoked those handlers. Starting that consumer alone would
have activated the hazard, so removal must precede consumer startup.

The source-proven active defect was instead a missing consumer for the **bounded**
shared rumor queue. `SidecarRumorBridge.receive` offered inbound rumors
(`SidecarRumorBridge.scala:60-105`), but no daemon validated/dispatched them; the
queue could fill and later input would drop. Exact workflow severity needs an
integration test because dedicated Nakamoto topics have separate consumers. The
worktree uses the safe order: remove GL0's six BFT handlers/unbounded loop, then
start `gossipDaemon.startAsInitialValidator`, whose Nakamoto branch runs only
`consumeRumors`. GL0 composes only generic/event handlers (`Main.scala:242-258`)
and starts the consumer after bootstrap, immediately before `Ready`, on both
startup paths (`Main.scala:347-350,647-650`; `GossipDaemon.scala:56-70,89-116`).
The focused six-family/no-queue/source regression is green and confirms ML0's
active BFT loop remains (`GlobalLegacyBftIngressDisabledSuite.scala:26-82`;
8/8); the broader final receipt/BFT containment selection is 35/35. This reduces
the missing-consumer defect from the node's full lifetime to a cold-start window,
but does not close it: `SidecarRumorBridge.receive` starts during consensus
construction (`GlobalSnapshotConsensus.scala:1121-1129`) before `Main` starts the
consumer after bootstrap (`Main.scala:347-350,647-650`). The bounded queue can
still fill and drop during that interval. Keep runtime delivery/drain/reconnect,
cold-start ordering, and supervision/release qualification open; the dormant
generic `Consensus` storage/routes compatibility shell is cleanup, not an active
queue path. Separately, the dedicated-topic `NakamotoSyncDaemon` also starts
before bootstrap (`GlobalSnapshotConsensus.scala:2192-2276`), a HIGH cold-state
processing race rather than a queue-boundedness finding.

### TIER 2 — unbounded Ref accumulators with a fragile prune trigger
3. **`NakamotoSyncDaemon` pendingParentRef** — `Ref[Map[Hash, List[pb.Snapshot]]]` keyed by missing-parent-hash, drained only on parent receipt. If a parent never arrives (stalled peer / orphan chain), it accumulates `pb.Snapshot` forever. **Fix: TTL (age-out) + size cap.**
4. **`ShardBinaryBuffer`** — `binary-buffer-cap=4096` per shard, REJECT-NEW at cap, **no evict-on-finalize** (scaladoc admits "later slice"). Holds `Array[Byte]` (not Json), so it's a byte-array footprint not the JNumber leak — but pins up to 4096×numShards binaries until restart. **Fix: prune-on-finalize.**

### TIER 3 — bounded-but-serial (back up only under slow I/O), monitor not fix-now
5. `SnapshotStorage` offload/cutoff queues — `Queue.unbounded[SnapshotOrdinal]` (small payload, serial file-I/O consumer). Low memory risk (ordinals); could lag on slow disk. **Fix optional: bound 512.**

### OK (verified bounded / pruned / small)
EventMempool (cap+clear-on-finalize), MptOverlay pending branches (cap+evict-on-commit, watermark), MetagraphOrphanBuffer (FIFO cap 256), EtaStateManager walkCache (watermark), GossipRoundRunner (bounded), ClickHouse sinks (bounded), all fs2 Topics (bounded per-subscriber), StateChangesAccumulator staging (cap 2048 + finalize-prune), SnapshotStorage tentative map (depth+reorg-window bounded).

## Cross-cutting recommendation
Add a `*_queue_size` Prometheus gauge to every retained queue (only the consensus command queue has one today). The rumor leak was invisible for the entire campaign because nothing measured queue depth. This folds into task #25 (liveness/lag metrics + grafana row). A single "queue depths" grafana row turns this whole class of bug from a post-mortem heap-histogram into a live panel.
