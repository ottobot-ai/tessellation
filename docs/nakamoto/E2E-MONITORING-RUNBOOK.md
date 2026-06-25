# E2E Monitoring Runbook (for humans and triage agents)

**SOP (hard rule):** any cluster failure/stall/leak investigation pulls **Prometheus metrics AND node logs, correlated by time window** — never logs alone. (Precedent: the "157GB leak" that was actually a finalization stall — only metrics revealed it.)

Verified live against the 5gl0/2mg/2shard stack on 2026-06-09. Adjust container names/ports for other topologies (`gl0-<i>` API port = `9000 + 10·i`; metagraph k port band shifts −1000 per k: m0=92xx/93xx/94xx, m1=82xx/83xx/84xx).

> **Topology/wiring reference:** this runbook is monitoring-focused. For the docker-network layout, IP/port bands, the `just→compose-runner→entrypoint` chain, and how gl0/gl1/ml0/cl1/dl1 + sidecars interconnect, see **[E2E-CLUSTER-TOPOLOGY.md](./E2E-CLUSTER-TOPOLOGY.md)**.

## Endpoints

| What | Where |
|---|---|
| Prometheus | `http://localhost:19090` (API: `/api/v1/query?query=...`) |
| Grafana | `http://localhost:3000` (nakamoto dashboard) |
| Scrape targets | `gl0-<i>:90<i>0/metrics` (all gl0s) + `sidecar-<i>:9501/metrics` |
| gl0 HTTP (host) | `localhost:90<i>0` — `/node/info`, `/global-snapshots/latest/combined` (finalized; prefer over `/latest/info` which 503s when head>finalized) |
| ml0/cl1/dl1 HTTP (host) | m0: `localhost:9200/9300/9400` (+10 per node); m1: `localhost:8200/8300/8400` |
| Test driver log | `/tmp/e2e-run.log` (or the path given at launch) |
| Node logs | `docker logs <name> 2>&1` — names: `gl0-0..4`, `gl1-0..2`, `ml0-m<k>-<n>`, `cl1-m<k>-<n>`, `dl1-m<k>-<n>`, `sidecar-0..4` |

## Health checks (metrics first)

Query helper: `curl -s 'http://localhost:19090/api/v1/query?query=<PROMQL>'`

1. **Consensus advancing?** `dag_nakamoto_finalized_ordinal` and `dag_nakamoto_ordinal` per instance — all nodes within ~3 ordinals of each other and increasing; `dag_nakamoto_slot` increasing. Stall = finalized flat while slot climbs.
2. **Finality healthy?** `rate(dag_nakamoto_finalized_total[5m])` > 0; `dag_nakamoto_finality_latency_ms_sum / _count` stable; `dag_nakamoto_attestation_weight` near committee size.
3. **Fork churn?** `dag_nakamoto_reorgs_total`, `dag_nakamoto_forks_stored_total`, `dag_nakamoto_fork_count` — small and flat after startup. Sudden growth = chain-selection trouble.
4. **Shards/metagraphs live?** `dag_nakamoto_shard_chain_height{shard_id=...}` increasing per shard; `dag_nakamoto_shard_checkpoint_total`, `dag_nakamoto_shard_committee_attestation_total`, `dag_nakamoto_committee_admit_total` increasing. A shard height flat at 1 = the genesis-seeding flake.
5. **KES sanity:** `dag_nakamoto_kes_snapshots_verified_total` / `kes_attestations_verified_total` climbing, no `*_invalid` counterparts climbing.
6. **JVM:** standard `jvm_memory_bytes_used{area="heap"}`, GC time, thread counts per instance — heap sawtooth normal; monotonic climb + finalized stall = investigate as STALL first, leak second.

## The current known flake (genesis seeding) — targeted checks

- **Metric signature:** one mg's shard height stuck at 1 while the other advances; `shard_committee_attestation_total` flat for that shard.
- **HTTP signature:** `curl -s localhost:9000/global-snapshots/latest/combined | jq '.[1].lastCurrencySnapshots | keys'` missing one metagraph id for >2 min after cl1s are Ready.
- **Log signatures (grep across all gl0s):** `MetagraphCommitteeGate` + `parentHash mismatch`; `verifyEmbedded sub-quorum`; `ShardCheckpoint rx ... signers=1`; `untracked shard; dropping`.
- Correlate: which gl0 produced the genesis checkpoint (`🧩` producer logs), which members logged rx for it, whether attestation rx lines (`ShardCheckpointAttestation`) ever appear for its hash.

## Triage flow for agents

1. Read this file. 2. Pull the metric panel above (one query batch, note timestamps of anomaly onset). 3. Grep the implicated nodes' logs in that window only. 4. Report: metric evidence + log evidence + the correlated timeline; never a conclusion from logs alone. 5. Don't restart/kill containers — report findings; the orchestrator decides.

## Test-driver progress (cheap)

`tail -50 /tmp/e2e-run.log` — workflow banners (`---- Start/End test... ----`), `assert...: Success at ordinal N`, miss/stall counters (`miss X/40`, `stalled X/75` are retry budgets, not failures until exhausted). Final verdict: PASS/FAIL summary at end of file.
