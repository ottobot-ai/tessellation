# Event Mempool + Fork Recovery: Branch Integration & Testing Guide

## Branch Overview

| Branch | Purpose | Status |
|--------|---------|--------|
| `scas/event-mempool-fork-recovery` | Event mempool, gossipsub-style event propagation, fork recovery detection | Compiles, 224 tests pass. Fork recovery **not yet wired** in Main.scala |
| `fix/expand-local-cluster-cap` | Expand local Docker cluster from 3 to 9 nodes | Complete, 1 commit (Docker scripts only) |

## Combining the Branches

The `fix/expand-local-cluster-cap` branch modifies only Docker shell scripts (no Scala), so there are **zero merge conflicts** with the event mempool work.

### Option A: Cherry-pick (recommended for testing)

```bash
git checkout scas/event-mempool-fork-recovery
git cherry-pick e6e7036aa   # feat: expand local cluster cap from 3 to 9 nodes
```

### Option B: Merge both into a test branch

```bash
git checkout develop
git checkout -b test/mempool-fork-recovery-cluster
git merge scas/event-mempool-fork-recovery
git merge fix/expand-local-cluster-cap
```

## What's Wired vs. What's Not

### Wired (active in current code)

- **EventMempool**: Hash-based event storage with FIFO fairness and state-key conflict detection
- **EventGossipDaemon**: Gossipsub-inspired mesh overlay with eager push, lazy pull (IHave/IWANT), graft sync
- **EventGossipRoutes**: P2P HTTP endpoints under `/events/` for gossip protocol
- **MempoolRoutes**: Debug HTTP endpoints under `/mempool/` for observability
- **Facility declaration**: Uses `eventHashes: Set[Hash]` instead of old `upperBound: Bound`
- **Consensus integration**: `ConsensusRoundRunner` pulls events from mempool via hash intersection

### NOT Wired (defaults to None — needs activation)

The fork recovery system is implemented but all three optional parameters in `EventGossipDaemon.make()` are left at their defaults (`None`):

```scala
// In dag-l0 Main.scala, line ~131:
EventGossipDaemon
  .make[IO, GlobalSnapshotEvent, GlobalStateKey](
    services.consensus.eventMempool,
    storages.cluster,
    sharedResources.client,
    sharedServices.session
    // These default to None:
    // getLocalChainTip = ???,
    // maybeForkRecoveryDetector = ???,
    // onForkDetected = ???
  )
```

To activate fork recovery, wire these in `Main.scala`:

```scala
import io.constellationnetwork.node.shared.infrastructure.gossip.event._

// 1. getLocalChainTip: reads current ordinal + hash from global snapshot storage
val getLocalChainTip: F[Option[ChainTip]] = for {
  maybeSnapshot <- storages.globalSnapshot.head
} yield maybeSnapshot.map(s => ChainTip(s.ordinal, s.hash))

// 2. ForkRecoveryDetector: compares local vs. peer chain tips
val forkDetector = ForkRecoveryDetector.make(meshState, getLocalOrdinal)

// 3. onForkDetected: trigger node restart/rejoin when fork detected
val onFork: ForkRecoveryInfo => F[Unit] = info =>
  logger.warn(s"Fork detected, lag=${info.lag}") >>
    storages.node.tryModifyState(NodeState.Ready, NodeState.WaitingForDownload)
```

**Recommendation**: Test the event mempool + gossip system first (without fork recovery). Fork recovery activation can be a follow-up once the base gossip layer is validated.

## Running an 8-Node Local Cluster

### Prerequisites

1. Docker and Docker Compose installed
2. `just` command-line tool installed
3. Branches combined (see above)

### Build & Launch

```bash
# 1. Build assembly JARs (takes several minutes)
SBT_OPTS="-Xss4m -Xmx12g" sbt dagL0/assembly dagL1/assembly

# 2. Start 8-node cluster
just up --num-gl0=8

# Or with tests:
just test --num-gl0=8 --skip-assembly
```

### Observability Endpoints

Each node exposes HTTP on sequential ports. For an 8-node GL0 cluster:

| Node | API Port | P2P Port |
|------|----------|----------|
| 0    | 9000     | 9001     |
| 1    | 9010     | 9011     |
| 2    | 9020     | 9021     |
| ...  | ...      | ...      |
| 7    | 9070     | 9071     |

**Key debug endpoints per node:**

```bash
# Cluster membership
curl http://localhost:9000/cluster/info

# Current snapshot ordinal
curl http://localhost:9000/global-snapshots/latest

# Mempool state (new)
curl http://localhost:9000/mempool/status

# Event gossip mesh info (new)
curl http://localhost:9000/events/mesh

# Node state
curl http://localhost:9000/node/state
```

### What to Validate

#### Phase 1: Basic Gossip Health

1. **All nodes join cluster**: Check `/cluster/info` on node 0 shows 8 peers
2. **Mesh formation**: Check `/events/mesh` — each node should have 4-6 mesh peers (default mesh degree = 6)
3. **Event propagation**: Submit a transaction to one node, verify it appears in all nodes' mempools
4. **Consensus progress**: Watch `/global-snapshots/latest` — ordinal should increment on all nodes

#### Phase 2: Fault Tolerance

1. **Kill a node**: `docker stop` one node, verify consensus continues on remaining 7
2. **Restart the node**: Verify it rejoins, receives gossip, and catches up
3. **Network partition**: Use `docker network disconnect` to isolate 2-3 nodes, then reconnect — verify they resync

#### Phase 3: Fork Recovery (once wired)

1. **Simulate fork**: Isolate a node, let it fall behind by >5 ordinals
2. **Reconnect**: Verify `ForkRecoveryDetector` logs fork divergence warning
3. **Recovery**: Verify node transitions to `WaitingForDownload` and resyncs from majority chain

### Quick Health Check Script

```bash
#!/bin/bash
echo "=== Cluster Health ==="
for i in $(seq 0 7); do
  port=$((9000 + i * 10))
  state=$(curl -s http://localhost:$port/node/state 2>/dev/null | jq -r '.state // "UNREACHABLE"')
  ordinal=$(curl -s http://localhost:$port/global-snapshots/latest 2>/dev/null | jq -r '.value.ordinal // "N/A"')
  mesh=$(curl -s http://localhost:$port/events/mesh 2>/dev/null | jq -r '.meshSize // "N/A"')
  printf "Node %d (:%d) — state=%-20s ordinal=%-8s mesh=%s\n" $i $port "$state" "$ordinal" "$mesh"
done
```

### Teardown

```bash
just down
```

## Readiness Assessment

| Capability | Ready? | Notes |
|------------|--------|-------|
| Event mempool | Yes | Hash-based storage, FIFO fairness, conflict detection |
| Gossipsub gossip | Yes | Mesh overlay, eager push, lazy pull, graft sync |
| Consensus integration | Yes | Facility uses event hashes, round runner pulls from mempool |
| Fork recovery detection | Implemented, not wired | Needs Main.scala wiring (see above) |
| Fork recovery action | Implemented, not wired | `RecoveryPeerHint` + `AbandonmentTracker` integration ready |
| 8-node Docker cluster | Yes (after cherry-pick) | `--num-gl0=8` flag from expand-local-cluster-cap |
| Unit tests | 224 passing | Fork recovery unit tests not yet written |

**Bottom line**: The event mempool + gossip system is ready for cluster testing. Fork recovery can be wired and tested as a second step once the base layer is validated.
