# Nakamoto Consensus — Local Test Cluster

Run a local Nakamoto consensus cluster with optional DAG-L1 layer and Grafana
dashboards via the `just test` harness. The standalone demo scripts that used
to live here (`demo.sh`, `gen-cluster.sh`, hand-rolled `docker-compose*.yml`,
etc.) have been removed — `just test` is now the single entry point.

## Quick Start

```bash
# 8-node Nakamoto GL0 + 3 GL1 + metagraph + Grafana/Prometheus
just test --num-gl0=8 --use-test-metagraph --grafana

# Skip the assembly step and reuse cached JARs (much faster on iteration)
just test --skip-assembly --num-gl0=8 --grafana

# 3-node GL0 variant
just test --num-gl0=3 --grafana

# Tear down
just down
```

Cluster env defaults (LDD parameters, slot duration, sidecar wiring) live in
[`docker/docker-compose.nakamoto-overlay.yaml`](../docker/docker-compose.nakamoto-overlay.yaml).
Production defaults are in `LddConfig.Default` and `SnapshotLeaderLoop`.

## What You'll See

### Grafana Dashboard (http://localhost:3000)

Login: admin/admin (or anonymous — no login required)

The **Nakamoto Consensus** dashboard shows:
- **Chain Overview**: best ordinal, finalized ordinal, current slot, fill rate
- **Snapshots/min**: production rate + gossip receive rate per node
- **Slot Gap Distribution**: histogram of gaps between consecutive snapshots
- **Attestation Weight**: finality convergence toward 1.0 (2/3 threshold)
- **Per-node ordinals**: confirm all nodes track the same chain

Dashboards are mounted read-only (`allowUiUpdates: false`); UI edits won't
persist across runs. Edit `grafana/dashboards/nakamoto-consensus.json` directly.

### Expected Behavior

After genesis (~25s warmup), with the test-overlay defaults you should see:
- **~12% fill rate** at ψ=1/γ=16/fA=0.5/fB=0.05 with 1000ms slots (prod-aligned)
- **Attestation weight → 1.0** within a few snapshots (2/3+ threshold)
- **ATTEST-FINALIZED** log lines as 2/3+ weight is reached
- **DEPTH-FINALIZED** when chain grows past the confirmation depth (k=31)
- **All nodes** producing snapshots (no leader — VRF self-election)

### Chain Progression

```bash
# Watch production in real time
docker logs -f gl0-0 2>&1 | grep -E 'WON slot|FINALIZED|Chain extended'

# Check a node's latest state
curl -s http://localhost:9000/node/info | jq .

# Check cluster peers
curl -s http://localhost:9000/cluster/info | jq '.[].state'
```

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   gl0-0     │    │   gl0-1     │    │   gl0-2     │
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

With L1 enabled (default in `just test`):

```
┌──────────┐  ┌──────────┐  ┌──────────┐
│ gl1-0    │  │ gl1-1    │  │ gl1-2    │
│  :9100   │  │  :9110   │  │  :9120   │
└────┬─────┘  └────┬─────┘  └────┬─────┘
     │              │              │
     └──────────────┼──────────────┘
                    │ L0 peer link
                    ▼
               ┌─────────┐
               │  gl0-0   │
               │ GL0:9000 │
               └─────────┘
```

GL1 nodes run BFT consensus and submit block data to GL0. This tests the full
stack: Nakamoto GL0 producing snapshots that include GL1 block references.

## Monitoring Stack

`just test --grafana` brings up Prometheus + Grafana + the image-renderer
sidecar automatically. URLs:

| Service    | URL                    | Notes                          |
|------------|------------------------|--------------------------------|
| Grafana    | http://localhost:3000  | Pre-provisioned dashboard      |
| Prometheus | http://localhost:9090  | 5s scrape, all nodes           |

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

## LDD and Slot Parameters

Tune via env vars on the `just test` command line; defaults below are the
test-cluster overlay values, which run faster than production for shorter
e2e cycles.

| Variable | Test default | Prod default | Description |
|----------|--------------|--------------|-------------|
| `NAKAMOTO_LDD_AMPLITUDE` | 0.5 | 0.5 | fA — max probability at gap=1 |
| `NAKAMOTO_LDD_BASELINE`  | 0.05 | 0.05 | fB — min probability floor |
| `NAKAMOTO_LDD_CUTOFF`    | 16  | 15  | γ — gap where curve flattens |
| `NAKAMOTO_LDD_OFFSET`    | 1   | 1   | ψ — snowplow offset |
| `NAKAMOTO_SLOT_DURATION_MS` | 1000 | 1000 | Slot tick interval |
| `NAKAMOTO_SLOTS_PER_EPOCH`  | 60  | 60  | Slots per epoch |
| `NAKAMOTO_ETA_ROTATION_SLOTS` | 600 | 600 | Eta randomness rotation |

LDD parameters now match production defaults (within rounding — γ=16 vs 15)
so the test cluster exercises the same eligibility curve as mainnet. Slot
duration stays at 1000ms — halving it also halves wall-clock per epoch
(slots-per-epoch is fixed at 60), which broke `WithdrawalTimeLimit` timing
in token-lock-replacement edge cases. Chain growth is wall-clock bound, so
faster slots wouldn't have helped throughput anyway.

## Troubleshooting

**Stale JAR after a fix**: SBT incremental build skips JAR re-assembly when
only resources change. Run `just nuke` (or omit `--skip-assembly`) to force a
rebuild. See the auto-memory entry on the JAR cache for the gotcha.

**Nodes stuck at "Initial"**: Genesis time is set per-run by
`compose-runner.sh`. If a node misses the window, look at gl0-0 logs for the
genesis timestamp and confirm wall-clock alignment across the host.

**Slot numbers but no snapshots**: VRF eligibility depends on stake
registration. Verify the seedlist (now generated under `nodes/`) includes
all node peer IDs.

**Sidecar can't find peers**: GossipSub mesh formation needs ~10s.
`docker logs sidecar-0 | grep "Connected to peer"` should show N-1 peers.

## Files

| Path | Purpose |
|------|---------|
| `grafana/dashboards/nakamoto-consensus.json` | Pre-provisioned consensus dashboard |
| `grafana/provisioning/` | Datasource + dashboard wiring |
