# Queue / Buffer Boundedness Audit

**Date:** 2026-06-10. **Updated:** 2026-07-15. **Trigger:** the gossip `rumorQueue` was `Queue.unbounded` and OOM'd 4/5 gl0 nodes under the spend/double-spend workload (456M retained circe `Json$JNumber` = 7.3 GB; 80× heap asymmetry leader-vs-follower). This audits every other queue/buffer for the same bug class.

## The bug class (what to look for)
A queue is dangerous only when **three** factors combine: (1) holds LARGE payloads (parsed circe.Json, `Signed[Snapshot]`, big `Array[Byte]`), (2) fed by a FIREHOSE/peer-receive producer, (3) drained by a SLOW or SERIAL consumer. Small-payload queues (ADTs, ordinals) or fast/immediate consumers are low-risk even when unbounded — bounding them blindly risks deadlock for no memory benefit. **Do not mass-bound.**

## FIXED / CONTAINED this session
| Queue | Was | Now |
|---|---|---|
| `SharedQueues.rumorQueue` | `Queue.unbounded[Hashed[RumorRaw]]` (circe.Json) | `Queue.bounded(65536)` + `parEvalMap(N)` validation in GossipDaemon + **tryOffer-drop** at the two peer-receive offer sites (SidecarRumorBridge, ConsensusRoutes/push-rumor) so the bound cannot migrate upstream; supervised blocking offer kept for local `Gossip.spread`. Legacy pull-gossip modes may replay a drop, but Nakamoto mode does not run those rounds: inbound overload remains lossy until durable retry/outbox recovery lands. |
| `GossipStream` JVM callback bridge | `Queue.unbounded[Either[Throwable, Option[GossipMessage]]]` let a reachable sidecar move network pressure into JVM heap. | One subscription generation now has a bounded item/encoded-byte data queue, reserved manual gRPC credits, a terminal path that bypasses a full data queue, and a callback-generation fence (`GossipStream.scala:37-50,90-270`). Flood, oversize, prompt-terminal, cancellation, and stale-callback cases are covered (`GossipStreamFlowControlSuite.scala:135-290`). This bounds only the callback bridge, not the complete transport. |
| Dedicated Nakamoto topic workers | Per-message detached work had no generation owner or family byte budget. | Eight item/byte-bounded family lanes use nonblocking admission and structured generation ownership. Source completion/error seals admission, drains every accepted queued/in-flight item, then reconnects without overlapping the old generation; unexpected worker failure and owner cancellation still stop the generation. Item/byte/oversize, offer-vs-seal, clean/error drain, concurrency, cancellation, replacement, and sibling-worker cases are covered. This does not bound downstream `Queues.scala` sinks, provide process-crash durability, or make multi-sink effects resumable. |
| Malformed shared decoders/crypto | Invalid Brotli, off-curve/malformed long-term keys/signatures, and structurally decoded short KES fields could escape a handler and terminate all topic lanes. | Brotli decompression/JSON/domain decode now completes `F` with `Left`; tip IDs require 64 raw bytes; long-term parsing/verification is total at tip/metagraph boundaries; KES wire fields have exact canonical lengths/counts/no trailing bytes and `OperationalKeyMaker.verify` returns false for malformed values. Exact exploit regressions cover all three KES gossip rails and metagraph admission. |

Prior-fixed (context): `NakamotoChainStore.byHash` (keepDepth=k₁=255, prune-on-finalize), sidecar 384m→1g+GOMEMLIMIT, combined-checkpoint retention 2→64+boot-rescan, committee-VRF memoization, gl0 heap 7168→12288m.

## UNWIRED preparation — not a live closure

| Primitive | Proven behavior | What remains open |
|---|---|---|
| `JsonBrotliBinarySerializer.deserializeBounded` | Rejects compressed input before opening Brotli and streams output only through an absolute `max + 1` sentinel (`JsonBrotliBinarySerializer.scala:71-103,138-143`). Boundary, high-ratio, malformed, truncated, and invalid-limit tests exist. | Legacy `deserialize` still fully materializes decompressed output (`:123-136`). No live untrusted message family calls the bounded API, and O-18 has not ratified per-family limits. |
| `ReservedIngressQueue` | Atomically reserves both item and retained-byte capacity and holds that reservation through successful, failed, or canceled processing (`ReservedIngressQueue.scala:10-18,38-119`). | No dag-l0 sink uses it. The eight live queues remain `Queue.unbounded`; no end-to-end owner yet accounts for decoded object graphs, mempool/outbox copies, partial chunks, or multi-sink effects. |

These primitives may be reviewed and tested independently, but they cannot be
listed as fixed/contained until every reachable producer and bypass is wired to
an owner-ratified limit and the reservation remains held through the full retained
lifetime.

## Open findings — ranked, with the bug-class test applied

### TIER 1 — same class, fix next (large payload OR firehose + serial)
1. **dag-l0 downstream consensus/state-channel queues remain HIGH and unbounded** — all 8 sinks in dag-l0 `Queues.scala:25-32` are still `Queue.unbounded`, including `stateChannelOutput`, native DAG, allow-spend, and token-lock. The new native worker lane bounds work only until its handler deserializes and offers into those sinks (`NakamotoSyncDaemon.scala:1063-1077,1227-1254,2118-2198`). The three native handlers still perform no outer `Signed[...]` signature/structural admission check before enqueue. The additive `ReservedIngressQueue` is not wired to any of them. Three additional escape paths prevent a queue-only patch from closing the finding:
   - inbound `Signed[GlobalSnapshotEvent]` rumors go directly to `eventMempool.add` (`Main.scala:248-258`), bypassing the four native/state-channel queues and any future reservation attached only there;
   - after committee admission, `processMetagraphBinary` erases `StateChannelService.process`'s validation `Left` with `.void` (`Services.scala:225-240`), and the caller then proceeds to the shard buffer (`NakamotoSyncDaemon.scala:3154-3163`); and
   - the serial publisher merges all eight queues and performs sign, mempool add, and gossip in one `evalMap` (`GlobalSnapshotEventsPublisherDaemon.scala:55-135`), while `Daemon.spawn` only supervises and discards the fiber handle (`Daemon.scala:17-21`). An uncaught publisher error therefore has no local log/restart/join policy and leaves every input queue without this consumer.

   **Fix:** verify the original outer signature and context-free structure before native enqueue, then enforce downstream item/byte and per-peer/topic budgets whose reservation is held through signing, mempool/outbox ownership transfer, or rejection. The check is resource admission only; it never authorizes state and never substitutes for every GL0 validator's mandatory execution and validation of direct `GL1 -> GL0` transitions against the exact proposal parent. Route direct event rumors through an equivalent bounded owner, make state-channel rejection stop shard buffering, and give publisher failure a visible fail-stop/restart policy plus resumable effects. The state-channel HTTP/body path needs the same pre-materialization and end-to-end bound. Same queue patterns in dag-l1 (x6) and currency-l1 (x2) need separate ingress analysis.
2. **Exhaustive malformed-input isolation and multi-sink restart atomicity remain open** — the confirmed Brotli, long-term-key/signature, and KES exception paths are now typed per-message rejects. Source completion/error now seals and drains accepted work before reconnect. Worker failure or outer daemon/process cancellation stops work immediately; worker failure is deliberately re-raised so the supervised daemon dies with production paused, but explicit operator alert/restart policy is not wired. The audit has not yet proved every decoder/validator total, and drain does not prove that a handler which already mutated sink A can resume sink B after failure/restart. Missing retained eta ancestry also still reaches this fail-stop instead of a typed `RecoveryRequired` defer. **Fix:** fuzz every message family into a typed `Applied | Rejected | Retryable` outcome, reserve fail-stop for infrastructure/invariant faults, alert operators, type eta-history recovery, and persist idempotent per-hash progress or atomic commits for every authority-bearing multi-sink path.
3. **No single transport envelope/decompression contract** — GL0 currently permits a 20 MiB aggregate event cut (`dag-l0.conf:21-23`; `GlobalSnapshotEventCutter.scala:41-43`), while the sidecar leaves GossipSub at its 1 MiB default (`gossip.go:160-214` has no `WithMaxMessageSize`), both Go and JVM gRPC construction retain the 4 MiB receive default (`grpcserver/server.go:163-184`; `SidecarClient.scala:197-210`), and ChainSync caps a message at 16 MiB (`chainsync/protocol.go:32-38`). `GossipStream`'s local 32 MiB envelope limit cannot raise any smaller hop. The legacy Brotli path still materializes the complete expansion before any decoded-size check; the new streaming bounded decoder is unwired and has no ratified family limits. The theoretically schema-valid v4 set has no finite payload maximum because types such as `StateChannelSnapshotBinary.content` are unconstrained byte arrays. **Fix/freeze:** disposition O-18, freeze active-era canonical-uncompressed, compressed-envelope, structural, chunk, aggregate-fetch, concurrency, and retention bounds by family, gossip the ratified bounded descriptor, and fetch exact content-addressed artifacts in authenticated bounded chunks. A future hard-fork migration inventories the exact selected source chain plus explicit headroom rather than making greenfield decode unbounded. Bounded streaming decompression must stop at `limit + 1`; compression is a transport transform and cannot silently choose a signature, hash, fee, or economic-validity result.

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
The callback bridge and dedicated family workers are now item/byte bounded and
generation-owned, but downstream sink bounds, malformed-message isolation,
multi-sink cancellation atomicity, and durable recovery for traffic missed by
delayed subscription remain open. The cold-restart policy also remains open. The dormant generic `Consensus` storage/routes
compatibility shell is cleanup, not an active queue path. Dedicated-topic detached
workers have been replaced by structured generation-owned lanes; the still-unbounded
native/state-channel sinks and absent pre-enqueue outer-signature admission remain
separate HIGH findings, not closed by the startup gate or worker bounds.

### TIER 2 — unbounded Ref accumulators with a fragile prune trigger
4. **`NakamotoSyncDaemon` pendingParentRef** — `Ref[Map[Hash, List[pb.Snapshot]]]` keyed by missing-parent-hash, drained only on parent receipt. If a parent never arrives (stalled peer / orphan chain), it accumulates `pb.Snapshot` forever. **Fix: TTL (age-out) + size cap.**
5. **`ShardBinaryBuffer`** — `binary-buffer-cap=4096` per shard, REJECT-NEW at cap, **no evict-on-finalize** (scaladoc admits "later slice"). Holds `Array[Byte]` (not Json), so it's a byte-array footprint not the JNumber leak — but pins up to 4096×numShards binaries until restart. **Fix: prune-on-finalize.**

### TIER 3 — bounded-but-serial (back up only under slow I/O), monitor not fix-now
6. `SnapshotStorage` offload/cutoff queues — `Queue.unbounded[SnapshotOrdinal]` (small payload, serial file-I/O consumer). Low memory risk (ordinals); could lag on slow disk. **Fix optional: bound 512.**

### OK (verified bounded / pruned / small)
EventMempool (cap+clear-on-finalize), MptOverlay pending branches (cap+evict-on-commit, watermark), MetagraphOrphanBuffer (FIFO cap 256), EtaStateManager walkCache (watermark), GossipRoundRunner (bounded), ClickHouse sinks (bounded), all fs2 Topics (bounded per-subscriber), StateChangesAccumulator staging (cap 2048 + finalize-prune), SnapshotStorage tentative map (depth+reorg-window bounded).

## Cross-cutting recommendation
Add a `*_queue_size` Prometheus gauge to every retained queue (only the consensus command queue has one today). The rumor leak was invisible for the entire campaign because nothing measured queue depth. This folds into task #25 (liveness/lag metrics + grafana row). A single "queue depths" grafana row turns this whole class of bug from a post-mortem heap-histogram into a live panel.
