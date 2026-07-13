# LocalEventsService — gl0 Reactive Event Stream

**Status:** Design — pending user approval before implementation.
**Author:** design agent (research-only pass).
**Scope:** add a gRPC service to gl0 that publishes consensus-relevant events
to local subscribers. Immediate consumer: JS e2e tests (replace polling with
reactive `awaitChainEvent`). Future consumer: operator GUI / Stargazer.

---

## 1. Goal + Scope

### Immediate (this design's deliverable)

Replace ~300-attempt polling loops in `.github/action_scripts/send_transactions/*.js`
with reactive subscriptions. Concrete wins:

- `validateTokenLockExpiration` wakes the instant gl0 emits the expiry, not
  on the next 1-2s poll tick.
- Test wall-clock drops by the polling-interval slack, currently 1-2s per
  observation × dozens of observations per run.
- Test flake under cluster slowdown (chain-link drain at 8gl0+4mg+4shards,
  per `EPOCH_PROGRESS_BUFFER_EXPIRATION_TEST = 200` comment) goes away
  because the observer doesn't have a deadline race against the prober.

### Long-term (extensibility, no impl now)

Same gRPC surface powers an operator GUI / Stargazer dashboard. We design
the wire schema and lifecycle to absorb new event types and remote callers
without breaking the e2e clients.

### Out of scope

- Snapshot replay from chainStore older than the in-memory window (YAGNI v1).
- Remote (non-localhost) gRPC — designed for, not impl'd.
- Cross-node event aggregation (each gl0 only publishes its own observed
  events; a multi-node GUI subscribes to all gl0s independently).

---

## 2. Existing gRPC Patterns We're Reusing

| Concern | Existing reference |
|---|---|
| protobuf source layout | `p2p/proto/sidecar.proto` |
| scalapb codegen | `modules/node-shared/.../proto/sidecar/*` (generated) |
| Server bootstrap | `GlobalSnapshotConsensus.scala:1206-1240` (`io.grpc.ServerBuilder.forPort(50053)...`) |
| Resource lifecycle (start + graceful shutdown w/ 5s grace) | same block, `Resource.make(...)(srv => srv.shutdown(); srv.awaitTermination(5, SECONDS))` |
| Dispatcher pattern for async-to-gRPC bridging | `chainSyncDispatcher <- Dispatcher.sequential[F]` then passed to `ChainSyncServer.make` |
| ServerStreamingObserver pattern | `ChainSyncServer.scala` (`serveSnapshots`, `serveMetagraphBinaries`) |
| FS2 `Topic` for fan-out | `ClusterStorage.scala:31` (`topic <- Topic[F, Ior[Peer, Peer]]`) and `NodeStorage.scala:28` |
| HOCON-typed config (env-var fallback) | `application.conf:281-302` `nakamoto { ... }`, mirror class `NakamotoConfig` in `config/types.scala:85` |

**Net new code is small:** one .proto file, one server impl (~250 LOC),
one publisher module (FS2 `Topic` wrapper), config plumbing, JS client helper.

---

## 3. Port, Bind, and Security

### Recommendation: **separate port 50054**, separate `io.grpc.Server`.

Rationale:

1. **Trust boundary differs.** 50053 (ChainSyncInbound) is called by the
   in-process libp2p sidecar — a trusted local sibling. The events service
   will be called by external test runners and (later) by operator-GUI
   processes on possibly different hosts. We don't want to add an
   externally-callable RPC to the side of a service the sidecar already
   uses; reasoning about ACL/mTLS becomes per-service.

2. **Lifecycle independence.** If a misbehaving e2e subscriber wedges a
   stream, we can restart 50054 without bouncing ChainSyncInbound (which
   would freeze the libp2p data plane).

3. **One-line cost.** Adding a second `ServerBuilder.forPort(...)` and a
   second `Resource.make` in `GlobalSnapshotConsensus.scala` is ~30 LOC.

4. **Port allocation.** 50054 is unclaimed in the repo. (50050-50052 are
   convention free; 50053 is taken; 50054 is the natural next slot.)

### Bind

- **Default: `127.0.0.1:50054`** via `ServerBuilder.forAddress(InetSocketAddress.createUnresolved("127.0.0.1", 50054))`.
- **Override knob:** `nakamoto.local-events.bind-address` (HOCON). E2e in
  Docker uses `0.0.0.0` so the host can reach the container; production
  defaults to `127.0.0.1`.

### Security

- **v1:** localhost only. Trust boundary = "anyone with a UNIX shell on
  the node host can read consensus events". This matches the existing
  127.0.0.1 service surface (metrics, HTTP debug endpoints).
- **Roadmap (not impl):** mTLS via `SslContextBuilder` (grpc-netty
  natively supports it) + a per-operator API token in metadata. Document
  in section 12, not in v1 schema.

---

## 4. Protobuf Schema — `local_events.proto`

### Design choices

- **Single `EventEnvelope` with `oneof`.** Easier to serve one stream per
  subscriber, easier to add event types non-breakingly (proto3 unknown-
  field tolerance + new oneof variants). Cost: clients pattern-match the
  oneof; cheap in TS/Scala/Go.
- **Filter via repeated `EventFilter`** at subscribe time. Pre-emit
  filtering reduces wire chatter for narrow tests. Filter language is
  intentionally weak (address-set + event-kind set + ord-range).
- **Bidi `Watch` deferred (YAGNI v1).** No runtime filter mutation;
  reconnect + resubscribe is fine for tests.
- **Replay deferred (YAGNI v1).** `replay_from_ord` field reserved in
  schema; server returns `UNIMPLEMENTED` when set. See §8.
- **Versioning.** Single `wire_version = 1` field on envelope; bump on
  breaking change. Add new event variants without bumping (oneof is
  forward-compat).
- **Sequence numbers** per stream let clients detect server-side drops
  (`seq_gap`).

```proto
syntax = "proto3";

package nakamoto.local_events;

option java_package = "io.constellationnetwork.node.shared.infrastructure.local_events.proto";

// ─── Service ────────────────────────────────────────────────────────

service LocalEvents {
  // Server-streaming subscription. Client opens once; server pushes
  // EventEnvelopes until the client closes the stream or the server
  // shuts down. Backpressure flows via gRPC HTTP/2 windows into the
  // FS2 source (see §6).
  rpc Subscribe(SubscribeRequest) returns (stream EventEnvelope);

  // Health probe. Returns immediately; used by clients to verify the
  // service is up before subscribing.
  rpc Health(HealthRequest) returns (HealthResponse);
}

// ─── Subscribe request ──────────────────────────────────────────────

message SubscribeRequest {
  // Conjunctive filter set. Empty = receive ALL events. Each filter
  // narrows further. Same-kind filters OR (address-set union); cross-
  // kind filters AND (kind set ∩ address set ∩ ord range).
  repeated EventFilter filters = 1;

  // Reserved for replay. v1: server returns UNIMPLEMENTED if set.
  // v2 may source from chainStore. See §8.
  optional int64 replay_from_ord = 2;

  // Client tag — logged server-side for debugging slow subscribers.
  // E.g. "e2e/token-locks/expiration".
  string client_tag = 3;
}

message EventFilter {
  // Restrict to these kinds. Empty = all kinds.
  repeated EventKind kinds = 1;
  // Restrict to events touching at least one of these addresses (DAG
  // base58). Applies only to address-bearing events; non-address events
  // are unaffected.
  repeated string addresses = 2;
  // Restrict to events touching one of these metagraph addresses.
  // Applies only to metagraph events.
  repeated string metagraph_addresses = 3;
  // Inclusive ordinal range. -1 sentinel = unbounded.
  int64 min_ordinal = 4;
  int64 max_ordinal = 5;
}

enum EventKind {
  EVENT_KIND_UNSPECIFIED = 0;
  // Immediate (§5)
  SNAPSHOT_FINALIZED = 1;
  BALANCE_CHANGE = 2;
  TOKEN_LOCK_STATE_CHANGE = 3;
  ALLOW_SPEND_STATE_CHANGE = 4;
  TRANSACTION_ACCEPTED = 5;
  METAGRAPH_SNAPSHOT_ACCEPTED = 6;
  METAGRAPH_BALANCE_CHANGE = 7;
  // Future (§1 future-extensibility)
  KES_ROTATED = 8;
  ETA_ROTATED = 9;
  SNOWBALL_DECISION = 10;
  CHAIN_QUALITY_CHANGE = 11;
  VALIDATOR_PARTICIPATION = 12;
  SLASHING_EVENT = 13;
}

// ─── Envelope ───────────────────────────────────────────────────────

message EventEnvelope {
  // Bumped on breaking schema change. v1.
  uint32 wire_version = 1;

  // Monotone per-server-process sequence number. Lets clients detect
  // server-internal drops (FS2 fast-publisher / slow-subscriber prune).
  uint64 seq = 2;

  // Server wall-clock at publish time (millis since epoch). NOT a
  // consensus timestamp.
  int64 published_at_millis = 3;

  // The ordinal this event is associated with. For SnapshotFinalized
  // this IS the finalized ordinal. For BalanceChange / TokenLock /
  // AllowSpend / TransactionAccepted, it's the gl0 ordinal during whose
  // accept() the change occurred. For metagraph events, it's the gl0
  // aggregate ordinal that folded the metagraph snapshot in.
  int64 ordinal = 4;

  oneof event {
    SnapshotFinalized snapshot_finalized = 10;
    BalanceChange balance_change = 11;
    TokenLockStateChange token_lock_state_change = 12;
    AllowSpendStateChange allow_spend_state_change = 13;
    TransactionAccepted transaction_accepted = 14;
    MetagraphSnapshotAccepted metagraph_snapshot_accepted = 15;
    MetagraphBalanceChange metagraph_balance_change = 16;
    // Future variants — reserved field numbers. Adding any of these
    // does NOT bump wire_version. Old clients see UNKNOWN_EVENT (proto3
    // tolerates unknown oneof variants).
    KesRotated kes_rotated = 20;
    EtaRotated eta_rotated = 21;
    SnowballDecision snowball_decision = 22;
    ChainQualityChange chain_quality_change = 23;
    ValidatorParticipation validator_participation = 24;
    SlashingEvent slashing_event = 25;
  }
}

// ─── Immediate event variants ───────────────────────────────────────

message SnapshotFinalized {
  int64 finalized_ordinal = 1;  // == EventEnvelope.ordinal; explicit for clarity
  bytes snapshot_hash = 2;      // 32 bytes
  int64 slot = 3;
  // Which Phase-1→2 trigger(s) qualified this finalization. Bitmask of
  // {1=t_weight, 2=t_count, 4=t_depth1, 8=t_depth2}.
  uint32 trigger_mask = 4;
}

message BalanceChange {
  string address = 1;     // DAG base58
  uint64 old_balance = 2;
  uint64 new_balance = 3;
  // Optional cause hint. "block" / "reward" / "tokenlock_credit" /
  // "tokenlock_refund" / "allowspend_consume" / "metagraph_sc". Free-
  // form so we can refine without schema churn.
  string cause = 4;
}

message TokenLockStateChange {
  enum Transition {
    UNSPECIFIED = 0;
    CREATED = 1;
    EXPIRED = 2;       // unlockEpoch passed; activeTokenLocks evicted
    WITHDRAWN = 3;     // explicit unlock tx accepted
  }
  string address = 1;
  bytes token_lock_ref = 2;        // hash of the Signed[TokenLock]
  Transition transition = 3;
  uint64 amount = 4;
  // unlockEpoch if known (CREATED + EXPIRED carry it; WITHDRAWN may
  // omit if pre-expiry). 0 sentinel for omitted.
  uint64 unlock_epoch = 5;
}

message AllowSpendStateChange {
  enum Transition {
    UNSPECIFIED = 0;
    CREATED = 1;
    CONSUMED = 2;      // SpendAction applied
    EXPIRED = 3;       // epoch deadline passed
  }
  string address = 1;
  bytes allow_spend_ref = 2;
  Transition transition = 3;
  uint64 amount = 4;
  string destination = 5;          // empty if not applicable
  uint64 expiry_epoch = 6;
}

message TransactionAccepted {
  bytes tx_hash = 1;
  string source = 2;
  string destination = 3;
  uint64 amount = 4;
  uint64 fee = 5;
}

message MetagraphSnapshotAccepted {
  string metagraph_address = 1;
  int64 metagraph_ordinal = 2;
  bytes metagraph_snapshot_hash = 3;
  // The gl0 ordinal during whose accept() this currency snapshot was
  // folded. (Same as EventEnvelope.ordinal but duplicated for clarity.)
  int64 gl0_ordinal = 4;
}

message MetagraphBalanceChange {
  string metagraph_address = 1;
  string address = 2;
  uint64 old_balance = 3;
  uint64 new_balance = 4;
  string cause = 5;
}

// ─── Future event variants (schema-only; not emitted in v1) ─────────

message KesRotated {
  uint32 from_period = 1;
  uint32 to_period = 2;
  int64 at_finalized_ordinal = 3;
}

message EtaRotated {
  uint32 from_period = 1;
  uint32 to_period = 2;
  bytes new_eta = 3;
}

message SnowballDecision {
  bytes snapshot_hash = 1;
  bool accepted = 2;
  uint32 confidence = 3;
}

message ChainQualityChange {
  double cq = 1;
  int64 finalized_ordinal = 2;
  uint32 trigger_mask = 3;
}

message ValidatorParticipation {
  string peer_id = 1;
  uint32 eta_period = 2;
  uint64 wins = 3;
  uint64 attestations = 4;
}

message SlashingEvent {
  string offender_peer_id = 1;
  string offense_kind = 2;
  bytes evidence_hash = 3;
  uint64 slashed_amount = 4;
}

// ─── Health ─────────────────────────────────────────────────────────

message HealthRequest {}

message HealthResponse {
  bool healthy = 1;
  int64 subscriber_count = 2;
  uint64 last_published_seq = 3;
  uint64 dropped_to_slow_subscribers_total = 4;
}
```

---

## 5. Server-Side Hookpoints

Verified file:line from a read-only pass. Where the table says "no hook
exists yet — propose," there is no current emission site; the design
proposes the cleanest one.

| Event | Hook | Strategy |
|---|---|---|
| `SnapshotFinalized` | `modules/dag-l0/.../SnapshotLeaderLoop.scala:808` (`chainStore.finalize(canonicalHash, finalizeAtOrdinal)` inside `finalityMonitor`'s depth-finalize arm) | Publish AFTER `chainStore.finalize` returns. Same `for { ... }` block already calls `nakamotoFinalizedOrdinalRef.update(...)` at L831 and logs `DEPTH-FINALIZED`. Add `publisher.publish(SnapshotFinalized(...))` next to it. Use `triggerMask` from `FinalityTrigger.triggersFor(...)` at L854-856 (already computed for `emitChainQuality`). |
| `BalanceChange` | `modules/node-shared/.../GlobalSnapshotAcceptanceManager.scala` at the `BuildGlobalSnapshotInfoResult` site (`L700+`) where `priorBalances ++ contextUpdate.balances` produces the new balance map (refs at L931, L1110, L1447) | Diff `priorBalances` vs `newBalances` after `acceptAllowSpendAndTokenLockBlocks` + reward acceptance fold. The diff list goes into the GSAM return tuple as a new field, or — cleaner — the publisher is passed in as a constructor dependency and `accept()` calls `publisher.publishBalanceChanges(...)` once per accept. Trade-off: GSAM is shared with currency-l0; we DO want the publisher in currency-l0 too, so threading it through `make` is acceptable. Alternative: emit from the dag-l0 caller after `accept()` returns, using the returned `GlobalSnapshotInfo` diff'd against the prior context — less code in the shared module but loses the `cause` tag fidelity (block vs reward vs refund). **Recommend: constructor injection of a `LocalEventsPublisher[F]` typeclass; default `LocalEventsPublisher.noop` for tests/cl0.** |
| `TokenLockStateChange` (CREATED) | `GlobalSnapshotAcceptanceManager.scala:876` `tokenLockStateManager.acceptReplacementTokenLocks(...)` returns `acceptedGlobalTokenLocks` | Publish `CREATED` for each new lock. Same publisher injection. |
| `TokenLockStateChange` (EXPIRED) | `TokenLockStateManager.scala:299` and `:383` (`findExpiredGlobalTokenLocksViaIndexFromMpt(previousEpochProgress, epochProgress).flatMap { expiredGlobalTokenLocks => ... }`) | Two emission sites because the expiry walk is invoked twice per accept (one for the "new active set" build, one for the "balance restoration" pass — see comments at L302-305 and L401). Recommend: hoist the expired-set computation up to the GSAM caller once, then thread it both places + publish from the single site. (Refactor cost is ~30 LOC.) |
| `TokenLockStateChange` (WITHDRAWN) | `TokenLockStateManager.scala` token-unlock-tx path — search for `tokenUnlock` accept handler (verify in impl; may be a separate manager call) | Same constructor-injected publisher. |
| `AllowSpendStateChange` (CREATED) | `GlobalSnapshotAcceptanceManager.scala:875` `acceptedGlobalAllowSpends = allowSpendBlockAcceptanceResult.accepted.flatMap(...)` | Publish on the accepted list. |
| `AllowSpendStateChange` (CONSUMED) | `SpendActionValidator` / `SpendTransactionBalanceManager.scala` — when a `SpendAction` consumes an `AllowSpend` | Publish at the consume site. (Verify exact line during impl; module is `managers/global/SpendTransactionBalanceManager.scala`.) |
| `AllowSpendStateChange` (EXPIRED) | `AllowSpendStateManager.scala` epoch-driven expiry pass — analogous to `TokenLockStateManager.findExpiredGlobalTokenLocksViaIndexFromMpt` | Same hoisting pattern as TokenLock expiry. |
| `TransactionAccepted` | `BlockAcceptanceCoordinatorManager.scala` / `BlockAcceptanceManager` accept loop — the place that returns `BlockAcceptanceResult.accepted` (visible to GSAM at L875-area as `blockResult`) | After the block result is assembled but before stateProof is built. Iterate `blockResult.accepted.flatMap(_.value.transactions.toList)`. Cheap; no new hook needed in `BlockAcceptanceManager` — emit from GSAM. |
| `MetagraphSnapshotAccepted` | `GlobalSnapshotStateChannelEventsProcessor.scala` — the `process` method that returns `SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]` (visible at GSAM L809) | After the processor returns, iterate the SortedMap and publish one event per `(metagraphAddress, snapshot)` pair. The metagraph ordinal lives in the binary's parsed contents; deserialize once and reuse the cached value (the processor already does this). |
| `MetagraphBalanceChange` | Same processor — after fold of currency-snapshot balances into `currencyAcceptanceBalanceUpdate` (refs at GSAM L1041, L1110) | Diff the merged update vs `lastSnapshotContext.lastCurrencySnapshots.../balances`. Publisher accepts a list of `(metagraphAddr, addr, old, new)`. |

**Cross-cutting design:** wire a single `LocalEventsPublisher[F]` typeclass
into the GSAM constructor. All hook sites call methods on it. The `noop`
default is used everywhere except the dag-l0 production wiring, which
constructs the real one. This avoids sprinkling FS2-Topic references
through the shared module.

```scala
trait LocalEventsPublisher[F[_]] {
  def publishSnapshotFinalized(e: SnapshotFinalized): F[Unit]
  def publishBalanceChanges(ordinal: SnapshotOrdinal, changes: List[BalanceChange]): F[Unit]
  def publishTokenLockChanges(ordinal: SnapshotOrdinal, changes: List[TokenLockStateChange]): F[Unit]
  def publishAllowSpendChanges(ordinal: SnapshotOrdinal, changes: List[AllowSpendStateChange]): F[Unit]
  def publishTransactionsAccepted(ordinal: SnapshotOrdinal, txs: List[TransactionAccepted]): F[Unit]
  def publishMetagraphEvents(ordinal: SnapshotOrdinal, snapshots: List[MetagraphSnapshotAccepted], balances: List[MetagraphBalanceChange]): F[Unit]
}

object LocalEventsPublisher {
  def noop[F[_]: Applicative]: LocalEventsPublisher[F] = ...  // returns F.unit
}
```

---

## 6. FS2 Wiring — `Topic` vs `Channel`

**Decision: `fs2.concurrent.Topic[F, EventEnvelope]` for the in-process fanout.**

Rationale:

- **Existing codebase precedent.** `ClusterStorage.scala:31` and
  `NodeStorage.scala:28` both use `Topic` for the same pattern
  (one publisher, many subscribers, fan-out). Stay consistent.
- **`Channel` is for one-producer-one-consumer with order guarantees.**
  Not what we want; multiple gRPC subscribers each want their own
  filtered view.
- **`Topic.subscribe(maxQueued)`** gives each subscriber its own
  bounded buffer; the publisher fan-outs into all of them and a slow
  subscriber doesn't block fast ones (it gets dropped per `Topic`
  semantics — see §7).

**Architecture:**

```
[publisher hooks]              [LocalEventsPublisher impl]
  ├─ GSAM.accept     ─────►    ┐
  ├─ finality monitor ─────►   ├──► Topic[F, EventEnvelope]
  └─ ... (future)    ─────►    ┘
                                    │
                                    │ Topic.subscribe(maxQueued=1024)
                                    ▼
                          [per-subscriber FS2 stream]
                                    │ .filter(envelope => matchesFilter)
                                    ▼
                          [gRPC ServerCallStreamObserver]
                                    │ onNext via Dispatcher
                                    ▼
                          [HTTP/2 flow-controlled wire]
```

**Sequence numbers + drop counters.** The publisher assigns a monotone
`seq` via a `Ref[F, Long]` before publishing. The per-subscriber stream
inserts gap-detection metadata (server-side drops surface as missing
`seq`s the client can see and reconnect on). The publisher exposes a
`droppedToSlowSubscribers` counter (read by `Health` RPC + Prometheus
metric `dag_local_events_dropped_total`).

---

## 7. Slow-Consumer + Reconnection Semantics

### Slow-consumer policy

`Topic.subscribe(maxQueued)` drops the SLOWEST subscriber's overflow,
not the publisher. We honor this: a slow gRPC subscriber sees gaps in
`seq` and a `dropped_to_slow_subscribers_total` increment in Health.

**Rationale:** the only alternative — block the publisher — would block
GSAM's `accept()`, which would stall consensus. Hard no.

**`maxQueued = 1024`** chosen so: at ~7s per finalized ordinal and
worst-case ~8 events per ord, a subscriber has ~15 minutes of
buffering before drops. Adjustable via `nakamoto.local-events.max-queued-per-subscriber`.

### gRPC backpressure interaction

`io.grpc.stub.ServerCallStreamObserver` exposes `isReady()` and
`setOnReadyHandler(...)`. The bridge from FS2 to gRPC uses these:

- When `isReady() == false`, the per-subscriber FS2 stream parks
  (FS2 Queue backpressure). gRPC's HTTP/2 window holds the wire.
- When `setOnReadyHandler` fires, FS2 wakes and resumes draining.

This means: gRPC backpressure → FS2 stream park → FS2 `Topic` subscriber
queue fills → if it crosses `maxQueued`, drop (with metric). The
publisher never blocks regardless.

### Reconnection

**v1: client reconnects + re-subscribes. No resume token. Accept gaps.**

Tests can detect gaps via `seq` jumps and either retry the event they
were waiting for or fail loudly. For tests this is fine because every
test-relevant state has a "current state" HTTP probe to fall back on.

Operator GUI can use the same protocol; gaps just mean "I missed a
fraction of a second, refresh from the REST endpoint."

**v2 (deferred): `replay_from_seq` for short-window resumption from the
in-memory ring buffer.** See §13 open questions.

---

## 8. Replay / Catch-up

### v1: not supported.

If `SubscribeRequest.replay_from_ord` is set, server returns gRPC
`UNIMPLEMENTED`. Clients use REST `/snapshots/latest/combined` for the
"current state at subscribe time" then start streaming. The combined
endpoint gives them all `activeTokenLocks`, `balances` etc. at a known
ordinal; subsequent events are deltas from that point.

This is enough for every immediate test-side use case.

### v2 candidates (YAGNI now)

1. **Short ring buffer (last N events in memory).** Cheap; ~1MB to hold
   ~1000 events. Gives second-scale catch-up. Sufficient for GUI
   refresh-after-disconnect.
2. **Long replay from chainStore.** Walk finalized snapshots from
   `replay_from_ord` to current; reconstruct each event by re-diffing
   `GlobalSnapshotInfo`s. Expensive: each event requires snapshot
   load + diff. Only viable for offline analysis tools, not live UI.

**Recommend documenting (1) as a v2 follow-up; (2) as "not on the
roadmap unless an explicit consumer materializes."**

---

## 9. HOCON Config Schema

### Keys (in `modules/node-shared/src/main/resources/application.conf`, under existing `nakamoto { ... }` block at L281)

```hocon
nakamoto {
  # ... existing eta-rotation-snapshots, keep-depth-behind-finalized ...

  local-events {
    # Master switch. Default OFF in production. Set to true in e2e via
    # HOCON override in dag-l0.conf or by an env-var-substituted fallback
    # (production deploys flip via deploy script HOCON).
    enabled = false
    enabled = ${?NAKAMOTO_LOCAL_EVENTS_ENABLED}

    # Bind address. Default loopback. Docker e2e flips to 0.0.0.0 to
    # accept connections from the host.
    bind-address = "127.0.0.1"
    bind-address = ${?NAKAMOTO_LOCAL_EVENTS_BIND}

    port = 50054
    port = ${?NAKAMOTO_LOCAL_EVENTS_PORT}

    # Per-subscriber queue depth. Drop-oldest if a subscriber falls
    # behind. ~15 min buffer at 8 events/ord × 7s/ord. See §7.
    max-queued-per-subscriber = 1024
    max-queued-per-subscriber = ${?NAKAMOTO_LOCAL_EVENTS_MAX_QUEUED}

    # Per-process publisher-side total queue. Backstop only; should
    # never be reached under normal load. If reached, oldest events
    # drop with a metric. 4096 = ~30s of full-tilt event volume.
    publisher-buffer-size = 4096
    publisher-buffer-size = ${?NAKAMOTO_LOCAL_EVENTS_BUFFER}

    # Shutdown grace period. Matches the existing ChainSyncInbound
    # 5-second grace at GlobalSnapshotConsensus.scala:1235.
    shutdown-grace-seconds = 5
  }
}
```

### Typed case class (extend `NakamotoConfig` in `config/types.scala:85`)

```scala
case class LocalEventsConfig(
  enabled: Boolean,
  bindAddress: String,
  port: PortNumber,                     // refined Int 1..65535
  maxQueuedPerSubscriber: PosInt,
  publisherBufferSize: PosInt,
  shutdownGraceSeconds: PosInt
)

case class NakamotoConfig(
  etaRotationSnapshots: PosLong,
  keepDepthBehindFinalized: PosLong,
  localEvents: LocalEventsConfig         // NEW
)
```

`PortNumber` is a refined newtype; if not present in shared module,
introduce alongside `PosInt` import (cheap, ~5 LOC).

---

## 10. Test-Client Helper Shape (TS/JS)

### Goal

Replace polling loops with `await awaitChainEvent(filter, predicate, opts)`.
Return as soon as the matching event arrives; reject on timeout.

### Sketch (TypeScript flavor; JS works the same)

```typescript
// modules: @grpc/grpc-js, generated stubs from local_events.proto.
// Lives in .github/action_scripts/shared/localEventsClient.ts (or .js).

type EventEnvelope = /* generated proto type */;

interface LocalEventsClient {
  // Open a long-running subscription. Returns an AsyncIterable.
  subscribe(filters: SubscribeRequest['filters'], clientTag: string): AsyncIterable<EventEnvelope>;

  // Convenience: await the first event matching `predicate`. Times out
  // after `opts.timeoutMs`. Reconnects on transient gRPC errors up to
  // `opts.maxRetries`.
  awaitChainEvent<T extends EventEnvelope>(
    filters: SubscribeRequest['filters'],
    predicate: (e: EventEnvelope) => e is T,
    opts: { timeoutMs: number; clientTag: string; maxRetries?: number }
  ): Promise<T>;

  close(): Promise<void>;
}

export function createLocalEventsClient(host: string, port: number = 50054): LocalEventsClient {
  /* @grpc/grpc-js client construction */
}

// Usage in token-locks.js:

const client = createLocalEventsClient('127.0.0.1', 50054);
const event = await client.awaitChainEvent(
  [{ kinds: ['TOKEN_LOCK_STATE_CHANGE'], addresses: [address] }],
  (e): e is /* TokenLockStateChange envelope */ =>
    e.event?.tokenLockStateChange?.transition === 'EXPIRED' &&
    e.event.tokenLockStateChange.tokenLockRef === hash,
  { timeoutMs: 5 * 60 * 1000, clientTag: 'token-locks/expiration' }
);
// event is now typed; balance reverted by this ord. Verify with one
// REST call OR trust the eventEnvelope.ordinal and stop polling.
```

### Convenience helpers (built on `awaitChainEvent`)

- `awaitBalance(address, predicate, opts)` — wraps `BalanceChange`.
- `awaitSnapshotFinalized(ordPredicate, opts)` — wraps `SnapshotFinalized`.
- `awaitTokenLockExpired(address, ref, opts)`.
- `awaitAllowSpendConsumed(address, ref, opts)`.

These live alongside `shared.js`. Generation strategy: hand-write the
TS once + check in; `@grpc/grpc-js` doesn't need build-time codegen
beyond `grpc_tools_node_protoc`.

### Fallback

Each helper accepts an `opts.fallbackPoller?: () => Promise<T>` that
fires on event-channel failure. Lets the test still pass if the gRPC
service is down (e.g., during the rollout while only some gl0s have
the feature). Default: no fallback, fail hard so we catch regressions.

---

## 11. Migration Plan — Polling Sites

### Survey (verified counts from grep)

| File | LOC | Polling loops (approx) | Migration priority |
|---|---|---|---|
| `token-locks.js` | 478 | 5 (`verifyTokenLockInSnapshot` × 2, `verifyBalanceChange`, `verifyTokenLockExpiration`, `verifyTriggerTokenUnlock`) | **High** — biggest win; the 200-epoch buffer comments at L19-47 document the pain. |
| `allow-spends-and-spend-transactions.js` | 2244 | 4+ (`verifyBalanceChange`, `verifyBalanceAfterSpend` × multiple variants) | High |
| `currency.js` | 771 | 2 (`batchTransaction` retry, `batchMetagraphTransaction` retry — both `maxAttempts = 3` SDK-error retries, less of a wall-clock target but still benefits from finality events) | Medium |
| `data-with-fee.js` | 237 | 1-2 verify loops | Medium |
| `data-without-fee.js` | 135 | 1 verify loop | Low |
| `bulk-submit-test.js` | 318 | 0-1 (mostly submission, not verification) | Low |

**Approximate LOC change:** ~400 LOC replaced with ~120 LOC of helper
calls + ~250 LOC new helper library. Net: -30 LOC and -tens of minutes
of e2e wall-clock per run.

### Sequencing

1. **Land schema + server (no consumers).** Validate via a smoke test
   that uses `Health` + a 30-second subscription. Lowest risk.
2. **Migrate `token-locks.js`** — most pain, isolated scope.
3. **Migrate `allow-spends-and-spend-transactions.js`** — share helpers
   with #2.
4. **Migrate `currency.js`** + the rest opportunistically.

Each PR is independently revertible. Tests keep working before+after
each migration (the gRPC service is additive, never replaces a REST
endpoint).

### Compatibility window

Keep the polling fallback in `awaitChainEvent` defaulted-off but wirable.
Rollout: enable globally → run e2e → after a clean week, remove polling
paths entirely.

---

## 12. Operator-GUI Future Hooks

(Bullets only — detailed design out of scope.)

- **Same gRPC server, same `LocalEvents` service.** GUI subscribes
  via gRPC-Web through an Envoy/grpcwebproxy hop (Stargazer is a
  browser extension; can't speak HTTP/2 directly to gRPC).
- **mTLS / API token** added in metadata interceptor; cert source =
  node's existing operator-key trust chain.
- **Bind address** flips to a non-loopback interface; reverse-proxy
  in front handles TLS + auth.
- **Future event variants (§4) emitted from natural sites:**
  - `KesRotated`: `SnapshotLeaderLoop.scala:1188` already does the
    `operationalKeyMaker.evolveTo(...)` rotation logic; publish next
    to the existing metric increment at L1194.
  - `EtaRotated`: at the eta-period boundary write in GSAM (the
    `etaForPeriod` path).
  - `SnowballDecision`: future Snowball cascade implementation site.
  - `ChainQualityChange`: piggyback on `emitChainQuality[F]` at
    `SnapshotLeaderLoop.scala:856`.
  - `ValidatorParticipation`: at eta-period end, derive from
    `NodeStakeAggregator` historical snapshots.
  - `SlashingEvent`: when slashing manager publishes a slash tx.

---

## 13. Open Questions for User Approval

These are the questions worth your call before we cut code. Ranked by
how load-bearing they are:

### Q1. **Publisher injection: GSAM constructor or post-accept emission?**

The cleanest source of `BalanceChange` / `TokenLockStateChange` /
`TransactionAccepted` fidelity is inside GSAM (where the diff is fresh
and the cause is known). But that means threading a
`LocalEventsPublisher[F]` through the GSAM `make` constructor — which
adds a parameter that currency-l0 also has to wire up (with `noop`).

**Alternative:** emit from the dag-l0 caller AFTER `accept()` returns,
diffing the returned `GlobalSnapshotInfo` vs the prior one. Pro: no
GSAM API change. Con: loses `cause` fidelity (we just see "balance
went from X to Y," not "because reward" vs "because tokenlock refund");
we'd have to re-derive cause from the surrounding context, which is
brittle.

Recommend: constructor injection. Costs one parameter; gains accurate
`cause` semantics that the GUI and slashing-evidence consumers will
want long-term.

### Q2. **TokenLock / AllowSpend expiry: refactor the double-walk or duplicate the emission?**

`TokenLockStateManager.findExpiredGlobalTokenLocksViaIndexFromMpt` is
called twice per accept (L299 + L383). Both call sites would emit
events. Either:

- **(a)** Compute once in GSAM, pass the set into both manager calls,
  emit once. ~30 LOC refactor.
- **(b)** Emit at both sites with idempotent dedup downstream. Brittle,
  rejected.
- **(c)** Walk once at the GSAM level (separate from manager), emit, then
  let the managers walk again as today. Cleanest but does extra MPT
  work. ~10 LOC.

Recommend (a) — small refactor, no perf cost.

### Q3. **Replay scope for v1 — really YAGNI?**

Tests can use `/snapshots/latest/combined` at subscribe time to get
current state, then react to deltas. That's sufficient for every
identified polling site. But it means tests have a small race window
between "REST snapshot" and "first delta arrives" where they could
miss an event whose `ordinal` is between the two. Mitigation: the
subscribe call returns a `start_ordinal` so the client can RPC
`/snapshots/at-ordinal/N` and bridge the gap deterministically.

Is that mitigation worth a half-day of impl, or do we live with the
gap (which manifests as a one-time false-negative that the test's
existing timeout will re-try past)?

Recommend: skip mitigation for v1. Add the gap-handling only if a
specific test flakes on it.

### Other minor questions (lower priority)

- **Naming.** "LocalEvents" vs "GlobalEvents" vs "ChainEvents" — happy
  to bikeshed.
- **Filter language.** Worth adding `event_kind_exclude` for "give me
  everything except SnapshotFinalized" use cases? (Adds 2 lines to
  proto + matcher.)
- **Metrics.** Publish per-event-kind counters
  (`dag_local_events_published_total{kind="..."}`)? Costs a Prometheus
  label cardinality; bounded since the enum is small.

---

## Appendix A — Read-Only Verification Notes

The following hookpoint claims were verified directly in source during
this design pass:

- `GlobalSnapshotConsensus.scala:1206-1240` — port 50053, ServerBuilder,
  Resource.make pattern. Used as the template for the new 50054 server.
- `SnapshotLeaderLoop.scala:808` — `chainStore.finalize(canonicalHash, finalizeAtOrdinal)`
  is the canonical "snapshot has been finalized" call.
  `nakamotoFinalizedOrdinalRef.update` at L831 is the high-water mark.
- `SnapshotLeaderLoop.scala:854-856` — `FinalityTrigger.triggersFor(...)`
  + `emitChainQuality[F]` already compute the trigger mask we need for
  `SnapshotFinalized.trigger_mask`.
- `GlobalSnapshotAcceptanceManager.scala:128-173` — accept method
  signature; returns a 15-tuple. New publisher events emit as a side
  effect; no tuple expansion needed.
- `GlobalSnapshotAcceptanceManager.scala:875-879` — accepted allow-spend
  + token-lock derivation point.
- `GlobalSnapshotAcceptanceManager.scala:931` — `priorBalances ++ initialData.blockResult.contextUpdate.balances`
  is the canonical merge site; balance diff happens just past this.
- `TokenLockStateManager.scala:299, 383, 402-403, 411` — expiry
  computation; `expiredLocks.filter(_.unlockEpoch.exists(_ < epochProgress))`.
- `ClusterStorage.scala:31` + `NodeStorage.scala:28` — existing FS2
  `Topic` precedent for fanout.
- `application.conf:281-302` — existing `nakamoto { ... }` HOCON block
  to extend.

Hookpoints listed as "verify in impl" (e.g., AllowSpend CONSUMED,
TokenLock WITHDRAWN) need a final pass against the actual control-flow
during the impl PR — they live in modules adjacent to the verified
sites but the exact emit line wasn't grep-able without reading more
than a design-pass warrants.
