# 13 — E2E Observability Contract

> **Purpose.** This doc is grounding material for the Phase-3 test-strategy pass
> (`docs/review/03-test-strategy.md`, see `docs/review/FABLE-REVIEW-PROMPT.md` §4). Phase-3 asks
> for chaos/adversarial scenarios that "build on infra we already have rather than reinvent." This
> file **is** that infra inventory for the E2E/observability side: exactly how to run the local
> cluster, exactly what Prometheus/Grafana already expose, and the one fork-detection trick that is
> easy to get backwards. Every claim below cites a file, line, or recipe name in this repo — if a
> name can't be verified it's marked `UNVERIFIED`. Read `docs/nakamoto/E2E-CLUSTER-TOPOLOGY.md` and
> `docs/nakamoto/E2E-MONITORING-RUNBOOK.md` for the full topology/monitoring reference; this doc is
> the condensed, change-set-specific companion.

---

## 1. How to run the standard local e2e

### 1.1 The standard command

The canonical local run (`docs/nakamoto/E2E-CLUSTER-TOPOLOGY.md` §8, cross-referenced from
`E2E-MONITORING-RUNBOOK.md` §"Topology/wiring reference"):

```bash
RUN_TIMEOUT_SECONDS=14400 just test --skip-streaming \
  --num-gl0=6 --metagraphs=2 --num-shards=2 \
  --stake-dist=harmonic --keep-alive --grafana
```

`just test` expands to `bash docker/bin/compose-runner.sh --use-test-metagraph --num-gl0=3
--metagraphs=2 {{ extra_args }}` (`justfile:13-15`) — the `--num-gl0=6 --metagraphs=2
--num-shards=2` above **override** the recipe's own `--num-gl0=3` default via `{{ extra_args }}`,
which is how the topology ceiling is dialed in per-run rather than by adding new `just` recipes.

**2mg/2shard is the local ceiling; 4mg/4shard OOMs/thrashes** — `docs/nakamoto/E2E-CLUSTER-TOPOLOGY.md:238`.
Metagraph nodes are capped at 9 by the string-concat port/IP allocation
(`docs/nakamoto/E2E-CLUSTER-TOPOLOGY.md:258`, `set-env.sh:742`); `--metagraphs ≤ 5` and
`--num-shards ≤ 5` are hard fail-fast caps in `set-env.sh:361,371`.

### 1.2 Mandatory / load-bearing flags

| Flag | Effect | Ground |
|---|---|---|
| `--skip-streaming` | **Mandatory** unless testing the indexer — needs a `GITHUB_TOKEN` you don't have locally. Skips the postgres+indexer stage. | `set-env.sh:244-245` (`SKIP_STREAMING`); `docker/bin/compose-runner.sh:1099-1210` gates the snapshot-streaming stage on it; `E2E-CLUSTER-TOPOLOGY.md:232-233` |
| `--grafana` | Starts Prometheus + Grafana + the image-renderer trio so a human can watch the dashboard live. | `set-env.sh:257-258` (`ENABLE_GRAFANA`); `docker/bin/compose-runner.sh:1214-1284` |
| `--num-shards=N` | **Hierarchical shard count M.** `1` (default) keeps the shard path fully inert — byte-identical to pre-sharding. `>1` activates per-shard `ShardCheckpoint` production + gl0 aggregation. | `set-env.sh:328-336` |
| `--shards=K` | **DEAD NO-OP.** Exports `NAKAMOTO_COMMITTEE_K_TARGET`, which nothing in HOCON/Scala reads (`set-env.sh:316-323,630`; decoupled from committee sizing in `396ca81b0`, 2026-06-04). Passing it changes nothing about the actual committee. Size the committee via `--num-gl0` (auto-derives `K_DRAW=N`, `K_QUORUM=⌈2N/3⌉`) or explicit `NAKAMOTO_COMMITTEE_K_{DRAW,QUORUM}` — `E2E-CLUSTER-TOPOLOGY.md:208,212-218`. |
| `--num-gl0=N` | Hypergraph validator count; also auto-derives committee `K_DRAW`/`K_QUORUM` unless overridden. Cap `≤16` (`set-env.sh:365`). | `set-env.sh:211` |
| `--stake-dist=harmonic` | Tier-1 stake-weighted genesis; local default is harmonic at N≥2 gl0 nodes. | `set-env.sh:407-482`; `E2E-CLUSTER-TOPOLOGY.md:210` |
| `--keep-alive` | Leaves the cluster running after the JS test suite finishes (pass/fail) instead of tearing down. | `compose-runner.sh:36` comment, gates `cleanup_end` |

### 1.3 Clean-between-runs discipline

- **`just clean-data && just clean-configs`** — the correct reset between runs. `clean-data`
  blanket-wipes `nodes/` + `docker/nodes/` (including root-owned KES files) via a throwaway root
  container (`justfile:42-49`); `clean-configs` runs `docker/bin/clean-configs.sh` (`justfile:51-52`).
- **NEVER `just clean`** for a routine reset — it also runs `bash sbt clean` on both the
  tessellation build *and* the metagraph template, plus `clean-docker` (`justfile:54-58`). That
  forces a full cold rebuild (`just nuke`'s own comment: "next `just test` does a full cold
  rebuild (~5-10 min)", `justfile:61-63`) — far more than a state reset between test iterations
  needs.
- **After changing `dag-l1`/`dag-l0`/`node-shared`/`shared`/`sdk`: run `just nuke-metagraph`** before
  the next `just test`. Reason (`justfile:82-98`): the metagraph JARs (`ml0`/`cl1`/`dl1`) **bundle**
  the published `tessellation-sdk` from `~/.ivy2/local` — changes to those modules do not
  re-bundle into the metagraph binaries without blasting that cache + the deploy JARs +
  `project_template` assembly targets. This is a materially different, cheaper operation than
  `just nuke` (leaves the tessellation build itself intact).
- **`just down`** (alias for `clean-docker`) tears down containers/network/volumes but **preserves
  `nodes/` logs** (`E2E-CLUSTER-TOPOLOGY.md:27`). **Do not run `just clean-data` after a failed
  run** until logs are pulled from `nodes/<i>/gl0-logs/…`, `nodes/m<k>-<i>/{ml0,cl1,dl1}-logs/…`
  (`E2E-CLUSTER-TOPOLOGY.md:250`) — `clean-data` is the step that actually deletes them.

---

## 2. The observability surface

### 2.1 Endpoints

| What | Where | Ground |
|---|---|---|
| Prometheus | `http://localhost:19090` (container-internal port stays `9090`; **not** the host port) | `compose-runner.sh:1265-1268` (`PROMETHEUS_HOST_PORT` default `19090`); `E2E-MONITORING-RUNBOOK.md:13` |
| Grafana | `http://localhost:3000` (admin/admin; anonymous Viewer also enabled) | `compose-runner.sh:1270-1275` |
| Nakamoto dashboard | `nakamoto-test/grafana/dashboards/nakamoto-consensus.json`, mounted read-only at `/var/lib/grafana/dashboards` | `compose-runner.sh:1277`; `docs/nakamoto/METRICS-DASHBOARD-REVAMP.md:15-16` |
| Scrape targets | `gl0-<i>:<9000+i*10>/metrics` (all gl0s) + `sidecar-<i>:9501/metrics` (all sidecars) | Prometheus config generated inline at `compose-runner.sh:1219-1245` into `nodes/prometheus.yml`; live copy confirms exactly these two `job_name` blocks |
| Prometheus retention | Not configured in this repo — runs `prom/prometheus:latest` with no `--storage.tsdb.retention.time` flag (`compose-runner.sh:1266-1268`), so it's the upstream image default (15d, per `METRICS-DASHBOARD-REVAMP.md:91-93`). `UNVERIFIED` against the actual `prom/prometheus:latest` default at time of use — check `docker inspect prometheus` if it matters. |

### 2.2 Metric catalog (verified emitters, grep of `Metrics[F].{updateGauge,incrementCounter,recordTime,recordDistribution}` across `modules/*/src/main/scala`)

**Chain progression (gauges, sampled every 5s by `NakamotoMetrics.run`, `modules/dag-l0/.../nakamoto/NakamotoMetrics.scala:37-58`):**

| Metric | Meaning | Source |
|---|---|---|
| `dag_nakamoto_ordinal` | **HEAD tip ordinal — NOT finalized.** See the trap in §4. | `SnapshotLeaderLoop.scala:1975` (on produce), `NakamotoSyncDaemon.scala:1951` (on catch-up adopt) |
| `dag_nakamoto_finalized_ordinal` | Attestation/depth-k finalized ordinal — the health signal | `NakamotoSyncDaemon`/`SnapshotLeaderLoop` gauges (grep-confirmed live this session, exact line not re-verified here) |
| `dag_nakamoto_slot` | Current wall-clock slot | leader loop (grep-confirmed live this session, exact line not re-verified here) |
| `dag_nakamoto_chain_length` | Snapshots held in the chain store | `NakamotoMetrics.scala:54` |
| `dag_nakamoto_fork_count` | **Active fork branches** — the fork-count health signal | `NakamotoMetrics.scala:41,55` |
| `dag_nakamoto_fill_rate` | snapshots / slots produced | `NakamotoMetrics.scala:48,56` |
| `dag_nakamoto_attestation_weight` | Best-tip attestation weight (2/3 threshold tracking) | `NakamotoMetrics.scala:50,57` |

**Rejection / catch-up counters (`dag_nakamoto_catchup_rejected{reason}` — THE stateProof-mismatch signal):**

`NakamotoSyncDaemon.scala:3453-3464,3581-3582,3641-3642` emit a single counter
`dag_nakamoto_catchup_rejected` labelled by `reason`, with four buckets actually wired:
`invalid_signature`, `state_proof_mismatch`, `byte_faithful_root_mismatch`,
`stale_pull_at_or_below_tip`. **`reason="state_proof_mismatch"` is the metric behind "0 stateProof
rejects"** in the healthy-cluster description below — it fires when a gossiped
`GlobalSnapshotInfo` does not match the snapshot's committed `stateProof`
(`NakamotoSyncDaemon.scala:3386-3392` for the log-only reward-sum-realign path — this arm logs and
falls back to normal fork handling without incrementing a counter,
`NakamotoSyncDaemon.scala:3459-3464` for the counted catch-up-adopt path). Separately,
`dag_nakamoto_snapshots_rejected` (bare counter, no label) increments on any
`NakamotoSnapshotValidator.Invalid` verdict at ordinary (non-catch-up) validation time
(`NakamotoSyncDaemon.scala:1765`).

**Sharding / checkpoint (Track-1-relevant — see §5):**

| Metric | Meaning | Source |
|---|---|---|
| `dag_nakamoto_shard_checkpoint_total{shard_id, path}` | Accepted checkpoint, `path ∈ {t_count, t_depth1}` | `ShardMetrics.scala:38` |
| `dag_nakamoto_shard_checkpoint_rejected_total{shard_id, reason}` | Rejected checkpoint. `reason` buckets: `pre_check_committee`, `pre_check_ed25519`, `pre_check_kes`, `pre_check_vrf`, `execution_quorum`, **`re_exec_mismatch`** (local recreation differs from the committee-signed root), `other` | `ShardMetrics.scala` |
| `dag_nakamoto_shard_committee_attestation_total{shard_id}` | Committee attestation received | `ShardMetrics.scala` (`CommitteeAttestationTotal`) |
| `dag_nakamoto_shard_chain_height{shard_id}` / `dag_nakamoto_shard_chain_finalized_ordinal{shard_id}` | Per-shard chain progress gauges | `ShardMetrics.scala` |
| `dag_nakamoto_committee_admit_total{outcome}` | `accepted` / `timeout` | `docs/nakamoto/METRICS-DASHBOARD-REVAMP.md:131-132`; panel query at `nakamoto-test/grafana/dashboards/nakamoto-consensus.json:614` |
| `dag_nakamoto_committee_sortition_self_outcome_total{outcome}` | `in_committee` / `not_in_committee` / `unknown_parent` | panel query at `nakamoto-test/grafana/dashboards/nakamoto-consensus.json:603` |

**Other counters/gauges present (non-exhaustive, full grep list on request):** `dag_nakamoto_reorgs`,
`dag_nakamoto_forks_stored`, `dag_nakamoto_orphan_buffer_size`, `dag_nakamoto_catchups`,
`dag_nakamoto_finality_latency_ms` (+ `_sum`/`_count` per Prometheus histogram convention),
`dag_nakamoto_finality_triggers_fired_t_{count,depth1,weight}_total`, `dag_nakamoto_chain_quality`,
`dag_nakamoto_kes_{period_rotations,attestations_*,snapshots_*}_total`,
`dag_nakamoto_nipopow_level_trial_{pass_,}total`, `dag_nakamoto_stake_aggregator_mpt_scan_ms`.
All confirmed live via `grep -rhoE '(updateGauge|incrementCounter|recordTime|recordDistribution)\("dag_[a-z0-9_]+"' modules/*/src/main/scala` this session.

**Sidecar (Go, separate metrics endpoint):** `sidecar_gossip_mesh_peers`,
`sidecar_gossip_messages_{published,received}_total`, `sidecar_connected_peers_total`,
`sidecar_dht_routing_table_size` — `docs/nakamoto/METRICS-DASHBOARD-REVAMP.md:37`.

**JVM heap/GC/thread metrics.** Bound once at registry construction via Micrometer binders —
`ClassLoaderMetrics`, `JvmGcMetrics`, `JvmHeapPressureMetrics`, `JvmInfoMetrics`,
`JvmMemoryMetrics`, `JvmThreadMetrics`, `SystemDiskSpaceMetrics`, `FileDescriptorMetrics`,
`ProcessorMetrics`, `UptimeMetrics`, `LogbackMetrics`
(`modules/node-shared/.../infrastructure/metrics/Metrics.scala:127-139`). These emit Micrometer's
standard Prometheus-convention names (`jvm_memory_used_bytes`, `jvm_gc_pause_seconds`,
`jvm_threads_live_threads`, `process_cpu_usage`, etc.) — **not custom Nakamoto emitters, so the
exact name strings are `UNVERIFIED` here; confirm with one `curl localhost:19090/api/v1/label/__name__/values` on first use.** There is no dedicated dashboard panel for heap/GC in
`nakamoto-consensus.json` (panel list checked this session has none) — heap/GC is a raw-PromQL /
Grafana-Explore check, per the hard SOP below, not a pre-built panel.

### 2.3 The hard SOP

**Never point-in-time grep one node's logs.** Pull Prometheus across **all N nodes** with
`rate()`/`query_range`, find the outlier, *then* correlate node logs in that time window
(`E2E-MONITORING-RUNBOOK.md:1-3,21-30`; this is a hard rule, not a suggestion — precedent: a
"157GB leak" that was actually a finalization stall, only visible cluster-wide in metrics).
Query helper: `curl -s 'http://localhost:19090/api/v1/query?query=<PROMQL>'`
(`E2E-MONITORING-RUNBOOK.md:23`). Concretely:

```bash
# finalized ordinal per node — should be increasing and within ~3 of each other
curl -s 'http://localhost:19090/api/v1/query?query=dag_nakamoto_finalized_ordinal' | jq .

# fork count — should be flat/small after startup
curl -s 'http://localhost:19090/api/v1/query?query=dag_nakamoto_fork_count' | jq .

# stateProof rejects — should be all-zero
curl -s 'http://localhost:19090/api/v1/query?query=increase(dag_nakamoto_catchup_rejected{reason="state_proof_mismatch"}[10m])' | jq .

# heap trend — sawtooth is normal, monotonic climb + finalized-flat is a STALL, not a leak, until proven otherwise
curl -s 'http://localhost:19090/api/v1/query_range?query=jvm_memory_used_bytes{area="heap"}&start=...&end=...&step=15s' | jq .
```

Triage flow for agents (`E2E-MONITORING-RUNBOOK.md:39-41`): read the runbook → pull the metric
panel across all nodes, note the anomaly's onset timestamp → grep only the implicated nodes' logs
in that window → report metric evidence + log evidence + timeline, never logs alone → do not
restart/kill containers, report and let the orchestrator decide.

---

## 3. The fork-detection method (get this exactly right)

**The trap:** the snapshot HTTP API has no top-level `.hash` field. `GET /global-snapshots/latest`
and `GET /global-snapshots/{ordinal}` both return a `Signed[GlobalIncrementalSnapshot]` —
i.e. `{ value: {...}, proofs: [...] }` — and the snapshot's own hash is not a stored field on
`value`; it's derived. **Same ordinal number across two nodes does NOT mean same chain.**

**The correct check is `.value.lastSnapshotHash`** — the parent-link hash embedded in the
snapshot body — compared **at a common ordinal** across nodes. This is not a proposed method; it
is the literal, already-running implementation in the e2e harness:
`.github/action_scripts/check_clusters/shared.js:61-101`, function `validateOrdinalsAndSnapshots`,
invoked from `compose-runner.sh:1341,1441` (`node check_clusters/multi-metagraph.js`) as part of
every standard `just test` run:

```js
// .github/action_scripts/check_clusters/shared.js:61-101 (abridged)
const ordinals = (await Promise.all(urls.map(u => fetchData(`${u}/latest`))))
  .map(_ => _.value.ordinal);
const lowestOrdinal = Math.min(...ordinals);
// ... assert max-min spread <= 3 ...

const snapshotResponses = await Promise.all(urls.map(u => fetchData(`${u}/${lowestOrdinal}`)));
const snapshots = snapshotResponses.map(_ => _.value.lastSnapshotHash);
const areSnapshotsTheSame = snapshots.every(s => s === snapshots[0]);
if (!areSnapshotsTheSame) {
  throw Error(`Snapshots are different between nodes: ${JSON.stringify(snapshots)}`);
}
```

Route grounding for the two calls the harness makes:
- `GET /global-snapshots/latest` → `Root / "latest"`, returns `Signed[S]` from head (or finalized,
  in Nakamoto mode) — `modules/node-shared/.../http/routes/SnapshotRoutes.scala:100-112`.
- `GET /global-snapshots/{ordinal}` → `Root / SnapshotOrdinalVar(ordinal)`, returns `Signed[S]` at
  that ordinal if servable — `SnapshotRoutes.scala:188-210`.
- There **is** a dedicated `GET /global-snapshots/{ordinal}/hash` route
  (`SnapshotRoutes.scala:212-224`) that computes and returns the snapshot's own hash directly — an
  alternative if you specifically need "is snapshot N byte-identical," as opposed to "did these
  nodes extend the same N-1 parent." The harness uses the cheaper single-fetch `lastSnapshotHash`
  form because it's already pulling the full snapshot body for the ordinal-spread check.

**Manual equivalent for ad-hoc debugging** (mirrors the harness exactly):

```bash
# assume nodes at :9000, :9010, :9020 (gl0-0..2)
for p in 9000 9010 9020; do
  ord=$(curl -s localhost:$p/global-snapshots/latest | jq .value.ordinal)
  echo "gl0 on port $p: ordinal=$ord"
done
# pick the lowest common ordinal N, then:
for p in 9000 9010 9020; do
  curl -s localhost:$p/global-snapshots/$N | jq -r .value.lastSnapshotHash
done
# all lines must be identical, or the nodes disagree on the chain at N despite matching heights
```

---

## 4. What healthy vs forked looks like

**Healthy:**
- `dag_nakamoto_finalized_ordinal` tracks `dag_nakamoto_slot` — climbing, not flat
  (`E2E-MONITORING-RUNBOOK.md:25`).
- `dag_nakamoto_fork_count` small and flat after startup — sudden growth means chain-selection
  trouble (`E2E-MONITORING-RUNBOOK.md:27`; `NakamotoMetrics.scala:24,41,55`).
- `increase(dag_nakamoto_catchup_rejected{reason="state_proof_mismatch"}[window])` == 0 across all
  gl0 nodes.
- N-of-N gl0 nodes converged: `.value.lastSnapshotHash` identical across all nodes at the lowest
  common ordinal (§3) — this is what `validateOrdinalsAndSnapshots` asserts every run, and what
  `assertClusterSize` (`shared.js:136-147`) pairs with a cluster-size check.
- Ordinal spread across nodes ≤ 3 (`shared.js:75-79` — the harness's own hard-coded threshold).

**Forked:**
- `dag_nakamoto_fork_count` climbing and not settling.
- `dag_nakamoto_catchup_rejected{reason="state_proof_mismatch"}` incrementing — a peer's gossiped
  `GlobalSnapshotInfo` disagrees with its own committed `stateProof`
  (`NakamotoSyncDaemon.scala:3459-3464`).
- `.value.lastSnapshotHash` **disagreeing** at a common ordinal across nodes — same height,
  different chain (the exact failure `validateOrdinalsAndSnapshots` throws on,
  `shared.js:88-95`).
- (Historical precedent, not this change set) `smtRoot` non-determinism producing per-node root
  divergence at the same ordinal despite identical underlying data — the real root cause behind a
  prior fork storm, per project memory; the P0 fix there was exactly a byte-level compare instead
  of a semantic one, the same principle as §3.

**The known trap:** `dag_nakamoto_ordinal` is the **HEAD tip**, not the finalized height — in
Nakamoto mode head can run ahead of finalized by design (attestation/depth-k lag). Judging cluster
health off `dag_nakamoto_ordinal` alone will read a perfectly healthy, just-not-yet-finalized
cluster as diverged. Always judge health off **`dag_nakamoto_finalized_ordinal`**
(`E2E-CLUSTER-TOPOLOGY.md:248-249`; also reflected in the route split between `GET
/latest/ordinal` (head, tentative) and `GET /latest/finalized-ordinal`
(`SnapshotRoutes.scala:69-83`), and in the "Best Ordinal" vs "Finalized Ordinal" panel pair in
`nakamoto-consensus.json`).

---

## 5. Validation checklist for THIS change set (`feature/committee-state-diff`, HEAD `21933559c`)

Per `docs/review/HANDOFF.md` §2/§4: this branch made **byte-diff-adopt primary** (commit
`dc0dbe1c7` removed the authoritative-override *layering* inside `deriveAdoptedCurrencyState`
(`GlobalSnapshotAcceptanceManager.scala:1006`) and `reExecDerivationWithDiff`
(`ShardCheckpointWiring.scala:210`) — both functions still exist and are still wired from
`SharedServices.scala:333,369`; what changed is that the adopter no longer papers over base drift
with an authoritative override). It also landed the S3 `band-density-reorg-enabled` flag
**default-OFF** (`application.conf:391-392`) and the S4 `MptOverlay.revertToOrdinal` deep-revert
executor (`MptOverlay.scala:1047`). The e2e cluster sanity pass for this branch is listed as
**NOT RUN — user-run** in the handoff (§4 row "E2E cluster sanity pass").

### 5.1 Delete-override behavior change (relies on ml0 authoritative-push byte-consistency)

This is the highest-leverage thing to watch, because it's a live behavior change (not gated behind
a flag) and the honest path now depends on `reExecDerivationWithDiff`'s re-exec over the pinned
`executionBaseOrdinal` reproducing exactly what the byte-diff adopter reconstructs — verified in unit
only (`ShardCommitteeReExecutionSuite`, per HANDOFF §5.1), not on a cluster.

**Watch, for the standard 2mg/2shard run:**

1. `dag_nakamoto_shard_checkpoint_rejected_total{reason="re_exec_mismatch"}` — must stay at **0**
   across the run. Any increment means a committee/watchtower's re-execution of a metagraph's
   `accept()` over the pinned base produced a different root than what the checkpoint's
   committee-signed root claims — i.e. the exact failure mode HANDOFF §5.1 asks a reviewer to try
   to construct.
2. `dag_nakamoto_shard_checkpoint_total{path="t_count"}` / `{path="t_depth1"}` climbing per shard,
   per metagraph, roughly in step with `dag_nakamoto_shard_chain_height{shard_id}` — confirms
   checkpoints are actually being accepted (not silently starved) with sharding active
   (`--num-shards=2`).
3. Per §3/§4: gl0-side `.value.lastSnapshotHash` convergence across all gl0 nodes at a common
   ordinal, run continuously (or at minimum, at the run's final ordinal) — this is the
   cluster-visible symptom if a per-shard adopt/re-exec divergence propagates into gl0's own
   global-snapshot chain.
4. `dag_nakamoto_catchup_rejected{reason="state_proof_mismatch"}` == 0 on gl0 — a secondary signal;
   if the shard-level re-exec and the global-level stateProof reconciliation ever disagree, this is
   where it would also surface.
5. `GET /global-snapshots/latest/combined` on each gl0 (`SnapshotRoutes.scala:135-144`) →
   `jq '.[1].lastCurrencySnapshots | keys'` — confirm both metagraph IDs present and their entries
   advancing, per the existing genesis-seeding-flake check in `E2E-MONITORING-RUNBOOK.md:34-35`
   (a stuck shard height at 1 for one metagraph is the known flake, orthogonal to this change set,
   but worth ruling out before attributing a stall to the byte-diff-adopt change).

### 5.2 Band-density flag (`band-density-reorg-enabled`) — default OFF, expect byte-identical behavior

`application.conf:391` sets `band-density-reorg-enabled = false` with `${?NAKAMOTO_BAND_DENSITY_REORG_ENABLED}`
override (`application.conf:392`). With the flag off, fork choice and the chain-store floor gate
stay on the pre-existing k₁-absolute path (per HANDOFF §1, §7's "Chain store / floor gate" entry:
"store-gate floor: k₁ when flag off, k₂ when `band-density-reorg-enabled`"). **The standard e2e run
does not set `NAKAMOTO_BAND_DENSITY_REORG_ENABLED`, so this run should be byte-identical to
pre-`14fb8b32e`/`2191510a6` behavior.**

**Watch to confirm no regression (flag OFF, the actual run this checklist targets):**

- `dag_nakamoto_fork_count` and `dag_nakamoto_reorgs` should look the same as any pre-this-branch
  baseline run at the same topology — this branch should not have changed the *shape* of fork
  activity when the S3 gate is off, only the gated-off code paths' presence.
- No `MptOverlay.revertToOrdinal` deep-path activity is expected: the deep-revert executor (S4,
  `MptOverlay.scala:1047`) is invoked from the reorg-replace arms that only fire past the (still
  k₁-gated) floor; confirm via log grep for `revertToOrdinal` in gl0 logs — should not appear at
  all in a flag-off run: a short local e2e runs as **Dev** (`CL_APP_ENV=dev`), so k₁ = **32**
  (`application.conf:311`; no `NAKAMOTO_CONFIRMATION_DEPTH` override — `docker-compose.nakamoto-overlay.yaml:34`;
  NOT the stale "255" / mainnet-1024, see HANDOFF §3.1) — a short local run won't reach a depth-k₁
  reorg regardless of the flag.
- `dag_nakamoto_shard_checkpoint_total`/`_rejected_total` behavior should be unaffected by this
  flag (S3 is a global fork-choice/floor concern, not a per-shard-checkpoint concern) — useful as a
  control: if shard-checkpoint metrics regress on this branch, band-density is not the suspect.

**If the flag were flipped on (`NAKAMOTO_BAND_DENSITY_REORG_ENABLED=true`) — NOT part of the
standard run, but documented here since a reviewer may want to probe it:**

- This is explicitly **HARD-GATED** per HANDOFF §4: "requires §5.6 deep-fork **cluster-uniformity
  sim** before `true`" — do not flip it in a casual e2e run without that precondition understood.
- If probed anyway, watch `dag_nakamoto_reorgs` for **deep** reorgs (past k₁, up to the k₂ floor)
  and `MptOverlay.revertToOrdinal`'s shallow-RAM vs deep-disk path selection (log grep
  `revertToOrdinal` — the deep path logs "cannot reach fork ordinal ... via the in-memory undo
  journal", `MptOverlay.scala:87-98`), plus whether `densityCompare`
  (`ChainSelection.scala:276`, S3's commutative comparator) ever produces asymmetric results across
  nodes (HANDOFF §5.3's open question) — which would show up as **persistent** (non-converging)
  `.value.lastSnapshotHash` disagreement per §3, not a transient one.
- `dag_nakamoto_shard_checkpoint_rejected_total{reason="re_exec_mismatch"}` is unrelated to this
  flag and should be checked independently of it (§5.1).

### 5.3 Quick-reference: what NOT to conflate

| Symptom | Likely cause | NOT this change set |
|---|---|---|
| `dag_nakamoto_shard_checkpoint_rejected_total{reason="re_exec_mismatch"}` > 0 | §5.1 (byte-diff-adopt ≡ re-exec breaking) — **this is the change set's own top risk** | — |
| `dag_nakamoto_fork_count` climbing with band-density flag OFF | Something else — this branch shouldn't touch flag-off fork-choice shape | correct — investigate as pre-existing (e.g. `smtRoot` non-determinism precedent) |
| One metagraph's shard height stuck at 1 | Genesis-seeding flake (pre-existing, orthogonal) | correct, per `E2E-MONITORING-RUNBOOK.md:32-37` |
| `dag_nakamoto_ordinal` flat while `dag_nakamoto_finalized_ordinal` climbs | Normal Nakamoto head-ahead-of-finalized behavior, not a stall | correct — see §4 trap |

---

## Cross-references

- `docs/nakamoto/E2E-CLUSTER-TOPOLOGY.md` — canonical infra (network, ports, layer wiring, run SOP).
- `docs/nakamoto/E2E-MONITORING-RUNBOOK.md` — monitoring-focused companion to the topology doc.
- `docs/nakamoto/METRICS-DASHBOARD-REVAMP.md` — metric inventory + dashboard design rationale.
- `docs/review/HANDOFF.md` — the ground truth for this change set's state and open questions.
- `docs/review/FABLE-REVIEW-PROMPT.md` — the review campaign this doc feeds Phase 3 of.
- `.github/action_scripts/check_clusters/shared.js` — the live fork-detection implementation (§3).
