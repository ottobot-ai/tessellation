# Nakamoto Consensus — Local Test Cluster

Run a local Nakamoto consensus cluster with optional DAG-L1 layer and Grafana dashboards.

## Quick Start

```bash
# Full demo: 3 GL0 nodes + 3 L1 nodes + Grafana/Prometheus
cd nakamoto-test
bash demo.sh

# GL0 only (no L1, no monitoring)
WITH_L1=false WITH_MON=false bash demo.sh

# 8-node GL0 cluster with monitoring
NODES=8 WITH_L1=false bash demo.sh

# Tear down
bash demo.sh stop
```

## What You'll See

### Grafana Dashboard (http://localhost:3000)

Login: admin/admin (or anonymous — no login required)

The **Nakamoto Consensus** dashboard shows:
- **Chain Overview**: best ordinal, finalized ordinal, current slot, fill rate
- **Snapshots/min**: production rate + gossip receive rate per node
- **Slot Gap Distribution**: histogram of gaps between consecutive snapshots
- **Attestation Weight**: finality convergence toward 1.0 (2/3 threshold)
- **Per-node ordinals**: confirm all nodes track the same chain

### Expected Behavior

After genesis (~60s warmup), you should see:
- **~15-20% fill rate** (LDD snowplow curve, fA=0.5, γ=15)
- **Attestation weight → 1.0** within a few snapshots (3/3 = 1.0)
- **ATTEST-FINALIZED** log lines as 2/3+ weight is reached
- **DEPTH-FINALIZED** when chain grows past k=6 confirmation depth
- **All 3 nodes** producing snapshots (no leader — VRF self-election)

### Chain Progression

```bash
# Watch production in real time
docker compose logs -f node-0 2>&1 | grep -E 'WON slot|FINALIZED|Chain extended'

# Check a node's latest state
curl -s http://localhost:9000/node/info | jq .

# Check cluster peers
curl -s http://localhost:9000/cluster/info | jq '.[].state'
```

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   node-0    │    │   node-1    │    │   node-2    │
│  (dag-l0)   │    │  (dag-l0)   │    │  (dag-l0)   │
│  :9000      │    │  :9010      │    │  :9020      │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ sidecar-0   │◄──►│ sidecar-1   │◄──►│ sidecar-2   │
│ (Go libp2p) │    │ (Go libp2p) │    │ (Go libp2p) │
│ GossipSub   │    │ GossipSub   │    │ GossipSub   │
└─────────────┘    └─────────────┘    └─────────────┘

JVM nodes produce snapshots via VRF lottery.
Go sidecars handle GossipSub mesh (mDNS discovery, Noise transport).
Communication: gRPC (50051) + HTTP bridge (50052).
```

With L1 enabled:
```
┌──────────┐  ┌──────────┐  ┌──────────┐
│ l1-node-0│  │ l1-node-1│  │ l1-node-2│
│  :9100   │  │  :9110   │  │  :9120   │
└────┬─────┘  └────┬─────┘  └────┬─────┘
     │              │              │
     └──────────────┼──────────────┘
                    │ L0 peer link
                    ▼
               ┌─────────┐
               │ node-0   │
               │ GL0:9000 │
               └─────────┘
```

L1 nodes run standard BFT consensus and submit block data to GL0. This tests the full stack: Nakamoto GL0 producing snapshots that include L1 block references.

## Adding a 4th Validator Mid-Chain

```bash
# Start node-3 as a validator that syncs from the existing chain
docker compose --profile validator up -d sidecar-3 node-3
```

Node-3 uses `run-nakamoto-validator` — it downloads the latest snapshot from node-0, syncs MPT state, and joins VRF production.

## Monitoring Stack

```bash
# Start monitoring separately
docker compose -f docker-compose-monitoring.yml up -d
```

| Service    | URL                    | Notes                          |
|-----------|------------------------|--------------------------------|
| Grafana    | http://localhost:3000  | Pre-provisioned dashboard      |
| Prometheus | http://localhost:9090  | 5s scrape interval, all nodes  |

### Key Metrics

| Metric | Description |
|--------|-------------|
| `dag_nakamoto_ordinal` | Current best chain ordinal |
| `dag_nakamoto_slot` | Current slot number |
| `dag_nakamoto_fill_rate` | Fraction of slots with snapshots |
| `dag_nakamoto_finalized_ordinal` | Last finalized ordinal |
| `dag_nakamoto_snapshots_produced_total` | Total snapshots this node produced |
| `dag_nakamoto_snapshots_received_total` | Total snapshots received via gossip |
| `dag_nakamoto_attestation_weight` | Current best-tip attestation weight |
| `dag_nakamoto_slot_gap` | Distribution of slot gaps |

## LDD Parameters

Tune via environment variables in docker-compose:

| Variable | Default | Description |
|----------|---------|-------------|
| `NAKAMOTO_LDD_AMPLITUDE` | 0.5 | fA — max probability at gap=1 |
| `NAKAMOTO_LDD_BASELINE` | 0.05 | fB — min probability floor |
| `NAKAMOTO_LDD_CUTOFF` | 15 | γ — gap where curve flattens |
| `NAKAMOTO_LDD_OFFSET` | 1 | ψ — snowplow offset |
| `NAKAMOTO_SLOTS_PER_EPOCH` | 60 | Slots per epoch |
| `NAKAMOTO_ETA_ROTATION_SLOTS` | 600 | Eta randomness rotation interval |

**Recommended tuning** (from parameter sweep): `fA=0.4, γ=12` gives ~8s cadence, <1.2% fork rate at 100 nodes.

## Troubleshooting

**sbt stage gets SIGKILL**: Stop containers first (`docker compose down`), then build. JVM + Docker compete for memory.

**Nodes stuck at "Initial"**: Check genesis time. If it's in the past by >60s at boot, nodes may have missed the window. Use `start.sh` which sets genesis 45s ahead.

**Sidecar can't find peers**: mDNS needs ~10s. Check `docker compose logs sidecar-0` for "Connected to peer" lines.

**Slot numbers but no snapshots**: VRF eligibility depends on stake registration. Verify `seedlist.csv` includes all node peer IDs.

## Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | 3-node GL0 cluster |
| `docker-compose-8node.yml` | 8-node GL0 cluster |
| `docker-compose-l1.yml` | 3-node DAG-L1 overlay |
| `docker-compose-monitoring.yml` | Prometheus + Grafana |
| `demo.sh` | One-command full demo |
| `start.sh` | Build + start GL0 only |
| `join-l1.sh` | Join L1 nodes to cluster |
| `test-validator.sh` | Add 4th node mid-chain |
| `genesis.csv` | Genesis account balances |
| `seedlist.csv` | Authorized peer IDs |
| `prometheus.yml` | Scrape config |
| `grafana/` | Dashboard JSON + provisioning |
