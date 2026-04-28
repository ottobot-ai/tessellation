# Nakamoto Consensus — Grafana Assets

This directory holds the Grafana dashboard JSON and provisioning used by the
`just test --grafana` flow. The dashboards are mounted into the bigset's
Grafana container by `docker/bin/compose-runner.sh`.

The standalone demo scripts and hand-rolled compose files that previously
lived here (`demo.sh`, `gen-cluster.sh`, `docker-compose*.yml`, etc.) have
been removed — `just test` is now the single entry point for running a
local Nakamoto cluster.

## Running a local cluster

```bash
just test --nakamoto-gl0 --num-gl0=8 --grafana    # 8-node Nakamoto + Grafana
just test --nakamoto-gl0 --num-gl0=3 --grafana    # 3-node variant
just down                                          # tear down
```

LDD and slot-duration env defaults for the test cluster live in
[`docker/docker-compose.nakamoto-overlay.yaml`](../docker/docker-compose.nakamoto-overlay.yaml).
Production defaults are in `LddConfig.Default` and `SnapshotLeaderLoop`.

## Dashboards

`grafana/dashboards/nakamoto-consensus.json` is the main consensus dashboard
(per-node ordinal, slot fill, attestation weight, finality lag, etc.).
`provisioning/` wires Grafana to the Prometheus datasource the bigset spins
up automatically when `--grafana` is passed.

Dashboards are mounted read-only (`allowUiUpdates: false`); UI edits will
not persist across runs. Edit the JSON file directly.
