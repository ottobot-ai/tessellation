# Nakamoto Sync Protocol — Design Sketch

## Problem

The current validator join flow is built around BFT consensus:

```
Initial → ReadyToJoin → (HTTP P2P join) → WaitingForDownload →
  (DownloadDaemon polls peers via HTTP) → DownloadInProgress →
    (ConsensusEventLoop.initFromDownload) → Observing →
      (observe N rounds) → WaitingForReady → Ready
```

Every step assumes:
- Snapshots are served via authenticated HTTP P2P endpoints (port 9001)
- Download requires enrollment in the consensus facilitator pool
- Observing means "watch BFT rounds happen" before participating
- Ready means "enrolled in consensus, eligible for facilitator selection"

In Nakamoto, none of this applies. There are no rounds to observe. Snapshots
arrive via GossipSub. You're "ready" when you have the chain tip and can
evaluate VRF.

## New Node State Machine

```
                    ┌─────────────────────────────────────┐
                    │          NAKAMOTO NODE FSM           │
                    └─────────────────────────────────────┘

  ┌──────────┐    sidecar      ┌──────────────┐   caught up    ┌───────┐
  │ Starting │──connected──▶│ Syncing      │──to chain tip──▶│ Ready │
  └──────────┘               └──────────────┘                 └───────┘
       │                          │    ▲                          │
       │ genesis                  │    │ fork/reorg               │ leave
       │ node                     ▼    │                          ▼
       │                     ┌──────────────┐               ┌─────────┐
       └─────genesis─────────▶│ Ready       │               │ Offline │
                              └──────────────┘               └─────────┘
```

### States

| State | Meaning | Entry condition |
|-------|---------|-----------------|
| **Starting** | Booting, connecting to sidecar | Process start |
| **Syncing** | Receiving snapshots, building state | Sidecar connected, peers found |
| **Ready** | Chain tip known, VRF active, producing/attesting | Caught up to network tip |
| **Offline** | Graceful shutdown | Leave requested |

### Transitions (vs current)

| Current BFT | Nakamoto | Why |
|-------------|----------|-----|
| `ReadyToJoin` → HTTP join handshake | Subscribe to GossipSub topics | No enrollment needed |
| `WaitingForDownload` → poll peer HTTP | Sidecar `Subscribe` stream starts delivering | Push, not pull |
| `DownloadInProgress` → fetch ordinals | Process received snapshots, request gaps | Hybrid push+pull |
| `Observing` → watch rounds | *(eliminated)* | No rounds to observe |
| `WaitingForReady` → wait N rounds | Tip within 2 ordinals of network | Clock-based, not round-based |
| `Ready` → BFT facilitator | VRF eligible + attesting | Self-selected, not assigned |

## Data Flow

### 1. Snapshot Distribution (Producer → Network)

```
 Slot winner (JVM)                    Go Sidecar                 Network
 ─────────────────                    ──────────                 ───────
 createProposalArtifact
   → sign snapshot
   → store in SnapshotStorage         
   → gRPC PublishSnapshot ──────────▶ GossipSub.Publish ────────▶ mesh peers
                                      topic: /nakamoto/snapshots
```

No HTTP serving needed. The sidecar IS the distribution layer.

### 2. Snapshot Reception (Network → Validator)

```
 Network                  Go Sidecar                    Validator (JVM)
 ───────                  ──────────                    ──────────────
 GossipSub message ──────▶ validate topic              
                           deduplicate (SeenHashCache)
                           relay to mesh peers
                           gRPC Subscribe stream ──────▶ SnapshotReceiver
                                                          → verify VRF proof
                                                          → verify contents
                                                          → store in SnapshotStorage
                                                          → update TipTracker
                                                          → emit TipAttestation
```

### 3. Historical Sync (Catching Up)

New node joins mid-chain. Needs ordinals 0..N to build state.

**Option A: Sidecar-mediated sync request**
```
 New node sidecar ──── RequestSync(fromOrdinal) ────▶ peer sidecar
                  ◀─── stream of Snapshot messages ───
```

Add a new sidecar RPC `SyncSnapshots` — request a range, peer streams them
from its local storage. The Go sidecar makes an outbound gRPC call to the
peer's sidecar (or a dedicated sync protocol on libp2p).

**Option B: Bitswap-style content-addressed fetch**
```
 New node: "I need snapshot with hash X" → IWANT on GossipSub
 Peer with hash X: responds with full snapshot
```

Reuse GossipSub's lazy pull (IHAVE/IWANT) but for historical snapshots.
Simpler, but less efficient for large ranges.

**Option C: Hybrid — snapshot archive endpoint**
```
 New node JVM ──── HTTP GET /snapshots/{ordinal} ────▶ any peer's public API
```

Keep the existing public API snapshot serving (port 9000) for historical
sync. Only live consensus traffic moves to sidecar. This is the pragmatic
first step — already works since SnapshotStorage.prepend makes snapshots
queryable.

**Recommendation: Start with Option C for sync, sidecar for live.**

The public API already serves snapshots. A `NakamotoSyncDaemon` replaces
`DownloadDaemon` — fetches historical snapshots via HTTP from any peer's
public API, while simultaneously subscribing to live snapshots via sidecar.
Once caught up (local tip ordinal ≥ network tip - 2), transition to Ready.

## Sidecar Protocol Extensions

```protobuf
// Add to SidecarService:

// Request historical snapshots from peers.
// Sidecar asks connected peers for snapshots in [from_ordinal, to_ordinal].
rpc SyncSnapshots(SyncRequest) returns (stream Snapshot);

// Report current chain tip (so sidecar can tell peers our height).
rpc ReportChainTip(ChainTipReport) returns (ChainTipResponse);

message SyncRequest {
  int64 from_ordinal = 1;
  int64 to_ordinal = 2;       // 0 = up to peer's tip
  int32 max_batch_size = 3;    // max snapshots per response chunk
}

message ChainTipReport {
  int64 ordinal = 1;
  bytes hash = 2;
  int64 slot = 3;
}

message ChainTipResponse {
  bool ok = 1;
}
```

The sidecar maintains a lightweight ordinal→hash index for sync requests.
When a peer asks for ordinals 5-100, the sidecar queries its local JVM
(via a callback or shared state) for those snapshots and streams them.

## JVM Components

### New: `NakamotoSyncDaemon` (replaces `DownloadDaemon`)

```scala
trait NakamotoSyncDaemon[F[_]] extends Daemon[F]

object NakamotoSyncDaemon {
  def make[F[_]: Async](
    sidecarClient: SidecarClientAlgebra[F],
    snapshotStorage: SnapshotStorage[F, ...],
    nodeStorage: NodeStorage[F],
    snapshotValidator: SnapshotValidator[F],  // VRF + content validation
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F]
  ): NakamotoSyncDaemon[F]
}
```

**Responsibilities:**
1. Subscribe to sidecar gossip stream (live snapshots + attestations)
2. On received snapshot: validate VRF → validate contents → prepend to storage
3. On received attestation: record in TipTracker
4. Track network tip from gossip (highest valid ordinal seen)
5. If local ordinal < network tip - gap threshold:
   - Request historical snapshots from peers (via sidecar SyncSnapshots or HTTP)
   - Process sequentially, building state
6. When caught up: transition node to Ready
7. Continuously: emit own TipAttestations for valid snapshots

### New: `SnapshotValidator` (extracted from consensus)

```scala
trait SnapshotValidator[F[_]] {
  /** Validate a received snapshot's VRF proof, slot assignment, and contents. */
  def validate(
    snapshot: Signed[GlobalIncrementalSnapshot],
    certificate: SlotCertificate,
    parentSnapshot: Signed[GlobalIncrementalSnapshot]
  ): F[Either[ValidationError, ValidatedSnapshot]]
}
```

Reuses `ConsensusFunctions.validateArtifact` internally but adds:
- VRF proof verification (EcVrf25519.vrfVerify)
- LDD threshold check (was the producer eligible at that slot gap?)
- Slot monotonicity (slot > parent slot)
- Producer identity verification (VRF public key → PeerId mapping)

### Modified: `Daemons.scala`

```scala
// Current:
DownloadDaemon.make(storages.node, programs.download, ...),

// Nakamoto mode:
if (nakamotoEnabled)
  NakamotoSyncDaemon.make(sidecarClient, snapshotStorage, nodeStorage, ...)
else
  DownloadDaemon.make(storages.node, programs.download, ...)
```

### Modified: `Main.scala` validator flow

```scala
// Current RunValidator:
gossipDaemon.startAsRegularValidator >>              // HTTP P2P gossip
  storages.node.tryModifyState(Initial, ReadyToJoin) >>
  programs.joining.joinOneOf(peerPool)               // HTTP join handshake

// Nakamoto RunValidator:
sidecarClient.connect >>                             // gRPC to sidecar
  storages.node.tryModifyState(Initial, Syncing) >>  // new state
  // NakamotoSyncDaemon handles the rest:
  //   Syncing → (caught up) → Ready
  // No explicit join — subscribing to GossipSub IS joining
```

## Node Lifecycle (Nakamoto)

### Genesis Node
```
1. Create genesis snapshot (same as today)
2. Store in SnapshotStorage
3. Connect to sidecar
4. → Ready immediately (you ARE the chain)
5. Start SnapshotLeaderLoop (VRF slot production)
6. Wait for peers to subscribe
```

### Validator Joining
```
1. Connect to sidecar (needs seedlist for initial peers)
2. Sidecar connects to peer sidecars via libp2p
3. → Syncing state
4. NakamotoSyncDaemon subscribes to gossip:
   a. Receives live snapshots → queues them
   b. Requests historical snapshots from genesis to tip
   c. Processes historical snapshots in order (building state)
   d. Once caught up, processes queued live snapshots
5. Local tip within 2 ordinals of network tip → Ready
6. Start SnapshotLeaderLoop (VRF slot production)
7. Start emitting TipAttestations for received snapshots
```

### Fork Recovery
```
1. Detect fork via TipTracker (competing tips with attestations)
2. ChainSelection picks winner (most attestations → longest → earliest slot)
3. If our tip loses:
   a. Request winning chain snapshots from peers
   b. Replay from fork point (using SnapshotStorage branching - future work)
   c. For now: nuke-and-replay from last finalized ordinal
4. Resume producing on winning chain
```

## What We Keep vs Replace

| Component | Keep | Replace | Why |
|-----------|------|---------|-----|
| `SnapshotStorage` | ✅ | | Linear chain, prepend, head — works |
| `EventMempool` | ✅ | | Events still accumulate the same way |
| `ConsensusFunctions` | ✅ | | createProposalArtifact is pure |
| `EventGossipDaemon` | ✅ | | Tier 2 event mesh unchanged |
| `GlobalSnapshotEventsPublisher` | ✅ | | Events → mempool → mesh unchanged |
| `DownloadDaemon` | | ✅ `NakamotoSyncDaemon` | Push not pull |
| `ConsensusEventLoop` | | ✅ `SnapshotLeaderLoop` | No rounds |
| `FacilitatorSelector` | | ✅ VRF eligibility | Self-selection |
| `NodeStateDaemon` (partial) | ⚠️ | ⚠️ Simplified | Fewer states |
| `P2P join/leave` | | ✅ GossipSub sub/unsub | No enrollment |
| `ConsensusClient` (HTTP) | | ✅ `SidecarClient` (gRPC) | New transport |
| `GossipDaemon` (rumor) | ⚠️ | ⚠️ Attestations only | No BFT rumors |
| `StallDetector` | | ✅ LDD handles liveness | Built into threshold |
| `ViewChangeManager` | | ✅ *(removed)* | No views |

## Implementation Order

### Phase 7a: NakamotoSyncDaemon + SnapshotValidator
- Subscribe to sidecar gossip stream
- Validate + store received snapshots
- Track network tip
- Transition to Ready when caught up
- **Test: 3-node cluster, all reach Ready**

### Phase 7b: Attestation loop
- Validators emit TipAttestations for valid snapshots
- TipTracker accumulates across network
- Finality monitor marks finalized snapshots
- **Test: finalization at 2/3+1 threshold**

### Phase 7c: Historical sync
- SyncSnapshots sidecar RPC (or HTTP fallback)
- Late-joining node catches up from genesis
- **Test: start node after 100 snapshots, verify it catches up**

### Phase 7d: Fork handling
- ChainSelection picks winning tip
- Request missing snapshots from peers
- Nuke-and-replay from finalized ordinal
- **Test: network partition → heal → convergence**

## Open Questions

1. **Sidecar discovery**: Currently uses seedlist for initial peers. Should
   sidecar also bootstrap from the JVM's cluster peer list? Or fully
   independent peer discovery via Kademlia DHT?

2. **Snapshot serialization**: GossipSub carries opaque bytes in `payload`.
   Use Kryo (tessellation native) or JSON (interoperable)? Kryo is smaller
   but requires matching JVM deserializer.

3. **Attestation persistence**: Store attestations on disk or memory only?
   Need them for fork-choice but they're ephemeral once finalized.

4. **Sidecar failure**: If sidecar crashes, JVM should pause production and
   enter a degraded state until sidecar reconnects. gRPC health checks
   handle this, but need explicit state transition.

5. **Mixed-mode cluster**: Can BFT and Nakamoto nodes coexist during
   migration? Probably not — hard fork at activation ordinal means everyone
   switches simultaneously.
