# E2E Cluster Topology — how the layers wire over Docker, and how to debug them

**Status:** canonical reference (kept in lockstep with the infra code). Last verified against
code 2026-06-25.

This is the single source of truth for **how the hypergraph (gl0/gl1) and metagraph
(ml0/cl1/dl1) layers interact over the Docker network**, what the orchestration entrypoint
does, and how to drive + debug a local e2e run. Every fact below cites the file that owns it —
if you change one of those files, update the cited line here too.

> If a statement here disagrees with the code, **the code wins** — fix this doc. The whole
> point is that a new contributor (or agent) can read *this one file* instead of re-deriving
> the topology from `compose-runner.sh`.

---

## 1. Entry points (just → compose-runner → entrypoint)

Everything is orchestrated by **`docker/bin/compose-runner.sh`** (1541 lines), invoked by `just`
recipes (`justfile`). There is no other entrypoint.

| Recipe | Expands to | Purpose |
|---|---|---|
| `just test [args]` | `compose-runner.sh --use-test-metagraph --num-gl0=3 --metagraphs=2 [args]` (`justfile:13-15`) | Build + bring up cluster + run JS e2e suite. **Defaults: 3 gl0 + 2 metagraphs + test metagraph.** |
| `just up [args]` | `compose-runner.sh --up [args]` (`justfile:20-22`) | Bring up cluster, **no tests**, leave it running. |
| `just build [args]` | `compose-runner.sh --build [args]` (`justfile:29-31`) | Assemble + build images only, start nothing. |
| `just down` | `tessellation-docker-cleanup.sh` (`justfile:25-37`) | Tear down containers/volumes/network. **Preserves `nodes/` logs.** |
| `just nuke` / `just nuke-metagraph` / `just clean-data` | cache/state wipes (`justfile:64-99,42-49`) | `nuke-metagraph` is required after changes to dag-l1/dag-l0/node-shared/shared/sdk that the metagraph JARs bundle. |

**`compose-runner.sh` flow** (in order):

1. **`source docker/bin/set-env.sh "$@"`** — arg parse, env defaults, topology caps, stake-dist
   resolution, committee sizing.
2. **Watchdog + teardown trap** — a background watchdog SIGTERMs the run after
   `RUN_TIMEOUT_SECONDS` (default **10800s / 3h**, `compose-runner.sh:322`); the EXIT trap
   `cleanup_end` auto-tears-down unless `--keep-alive`/`--up` (always preserves `nodes/` logs).
3. **Assembly + images** — `assembly.sh` → `docker/jars/*.jar`; `docker build` the
   `constellationnetwork/tessellation:test` image (`docker/Dockerfile`) **and** the
   `nakamoto-sidecar:test` image (`p2p/Dockerfile`) (`compose-runner.sh:393-404`).
4. **Per-node config** — `node-key-env-setup.sh` (keys) → `docker-env-setup.sh` (writes each
   node's `.env`: IPs, ports, peer IDs, join targets).
5. **Tier-1 genesis** (only when `NAKAMOTO_STAKE_DISTRIBUTION` is set) — `tools.jar
   generate-genesis` produces a stake-weighted `l0-genesis.json` + per-operator KES SK files,
   copied to each gl0 node (`compose-runner.sh:445-525`).
6. **Network create** — `docker network create … tessellation_common` (`compose-runner.sh:565`,
   see §2).
7. **Seed compose files** into `nodes/$i` (hg) and `nodes/m${k}-${i}` (metagraph).
8. **Seedlists** — derive per-node sidecar identity keys, build `NAKAMOTO_SIDECAR_SEEDLIST`
   (libp2p multiaddrs) + `NAKAMOTO_JVM_SEEDLIST` (5-field CSV: `peerId,ip,p2pPort,alias,bias`).
9. **Start order** (§6): gl0 (pulls sidecar) → wait gl0 finalizing → gl1 → metagraphs (grind
   pre-pass → ml0 genesis → ml0 → cl1/dl1) → per-metagraph tx-senders → snapshot-streaming →
   Prometheus/Grafana.
10. **Run JS e2e** from `.github/action_scripts` (`compose-runner.sh:1337+`).

Inside each container, **`docker/entrypoint.sh`** selects the run command from `CL_DOCKER_ID` +
`NAKAMOTO_MODE` (§5).

---

## 2. The Docker network (there is exactly one)

- **`tessellation_common`** — a single `bridge` network, created by `compose-runner.sh:565-569`
  and declared `external: true` in every compose file (`docker-compose.test.yaml:39-42`,
  `docker-compose.metagraph-test.yaml:24-27`, `docker-compose.nakamoto-sidecar.yaml:52-53`).
- **Subnet:** `${NET_BASE}.0.0/16` — default **`172.32.0.0/16`** (`NET_PREFIX` default
  `"172.32.0"`, `set-env.sh:30`; `NET_BASE="172.32"`, `docker-env-setup.sh:12`).
- **Dynamic IP pool** is pinned to the **upper half** `${NET_BASE}.128.0/17`
  (`compose-runner.sh:566-568`) so Docker's auto-allocated containers (sidecars,
  prometheus/grafana/tx-sender) can never collide with the **static lower-half** assignments
  below.

### IP allocation (static, lower half)

| Layer | IP | Source |
|---|---|---|
| gl0-`i` | `172.32.0.(10+i)` (`GL0_IP_BASE=10`) | `set-env.sh:695`, `docker-env-setup.sh:174` |
| gl1-`i` | `172.32.0.(30+i)` (`GL1_IP_BASE=30`) | `set-env.sh:696`, `docker-env-setup.sh:175` |
| snapshot-streaming | `172.32.0.60` (postgres) / `.61` (streaming) | `set-env.sh:703` (`SS_IP_FLOOR=60`) |
| ml0-m`k`-`i` | `172.32.(k+1).(30+i)` | `docker-env-setup.sh:218`, `docker-compose.metagraph-test.yaml:10` |
| cl1-m`k`-`i` | `172.32.(k+1).(40+i)` | `docker-compose.metagraph-test.yaml:16` |
| dl1-m`k`-`i` | `172.32.(k+1).(50+i)` | `docker-compose.metagraph-test.yaml:22` |

Hypergraph (gl0/gl1) uses **arithmetic** IP/port allocation, so it scales past 9 nodes
(ceiling `NAKAMOTO_MAX_HG_NODES`, default 20; hard cap `--num-gl0 ≤ 16`, `set-env.sh:365`).
Each **metagraph gets its own `/24`** (`172.32.(k+1).*`) inside the shared `/16`, so K
metagraphs never collide. Metagraph layers still use **single-digit string-concat** allocation
(cap 9; practically ~3 nodes). `--metagraphs ≤ 5`, `--num-shards ≤ 5` (`set-env.sh:361-372`).

---

## 3. Ports (host = external, container = internal)

Port prefixes (`set-env.sh:52-56`): `DAG_L0=90`, `DAG_L1=91`, `ML0=92`, `CL1=93`, `DL1=94`.

| Layer | Internal (container) | External (host) | Source |
|---|---|---|---|
| gl0-`i` | `9000+i*10` {public, +1 p2p, +2 cli} | **same** (ext==int) | `docker-env-setup.sh:170,198-200` |
| gl1-`i` | `9100+i*10` (legacy band; `gl1-0:9100` alias + BFT join) | `9600+i*10` (`GL1_EXT_PORT_BASE`) | `set-env.sh:698-699`, `docker-env-setup.sh:171-172,182-184` |
| ml0-m`k`-`i` | `${PFX}${i}{0,1,2}` | same | `docker-env-setup.sh:262-288` |
| cl1 / dl1 | `93…/94…` (per-k shifted) | same | `docker-env-setup.sh:263-288` |
| LocalEvents gRPC (gl0) | `50054` | `50054+i*10` | `docker-compose.nakamoto-overlay.yaml:104-109`, `docker-env-setup.sh:191` |
| Prometheus | `9090` | **`19090`** (`PROMETHEUS_HOST_PORT`) | `compose-runner.sh:1265-1268` |
| Grafana | `3000` | `3000` | `compose-runner.sh:1270-1275` |

**Per-metagraph port shift:** each metagraph's prefix is shifted **−10 per `k`** to avoid host
collisions: m0 = `92/93/94`, m1 = `82/83/84`, m2 = `72/73/74` (`docker-env-setup.sh:226-228`).

**Host URLs a test/debugger hits** (`set-env.sh:758-762`): gl0 `:9000`, gl1 `:9600`,
ml0(m0) `:9200`, cl1(m0) `:9300`, dl1(m0) `:9400`. For metagraph `k>0` subtract `k*10` from the
prefix (m1 ml0 = `:8200`, etc.).

### gl0 ↔ sidecar gRPC + libp2p (the bit that surprises people)

There are **two** gRPC endpoints between gl0 and its sidecar, plus the sidecar's libp2p port:

- **sidecar listens `:50051`** — gl0 *publishes* snapshots/attestations/rumors to it
  (`SIDECAR_HOST=sidecar-${suffix}`, `SIDECAR_GRPC_PORT=50051`,
  `docker-compose.nakamoto-overlay.yaml:100-103`; sidecar `-grpc 0.0.0.0:50051`,
  `docker-compose.nakamoto-sidecar.yaml:25-26`).
- **gl0 listens `:50053`** — the sidecar *delivers received gossip back* to gl0
  (sidecar `-jvm-grpc gl0-${suffix}:50053`, `docker-compose.nakamoto-sidecar.yaml:27-28`).
- **sidecar libp2p `tcp/9500`** — peer-to-peer GossipSub mesh; dials
  `NAKAMOTO_SIDECAR_SEEDLIST` (`/dns4/sidecar-j/tcp/9500/p2p/<id>`), `-disable-mdns`
  (`docker-compose.nakamoto-sidecar.yaml:29-35`, built in `compose-runner.sh:634-641`).
- **sidecar metrics `:9501`** — scraped by Prometheus (`compose-runner.sh:1246`).

---

## 4. Topology diagram

See **`docs/nakamoto/e2e-cluster-topology.dot`** (rendered `e2e-cluster-topology.png`) for the
container/network/port wiring. It shows the `tessellation_common` network, the static IP bands,
and every gRPC/libp2p/HTTP edge described in §3 and §5.

> Note: `docs/nakamoto-architecture.dot` is a **within-a-single-gl0-node** component diagram
> (consensus FSM, storage, state pipeline) — NOT the cluster topology. Use this doc's diagram
> for deployment/network questions.

---

## 5. Layer interaction (who talks to whom, and how they boot)

`entrypoint.sh` picks the run command by `CL_DOCKER_ID` (+ `NAKAMOTO_MODE`):

- **gl0 — hypergraph L0, Nakamoto.** `run-nakamoto <genesis>` iff `CL_DOCKER_ID=gl0` **and**
  `NAKAMOTO_MODE=true` (set only by `docker-compose.nakamoto-overlay.yaml:10`)
  (`entrypoint.sh:129-142`). **No BFT join** — `CL_DOCKER_GL0_JOIN=false` is forced
  (`compose-runner.sh:673`); validators discover each other via the JVM seedlist + the sidecar
  mesh. Bootstrap mode auto-detects: `--rollback-hash` | local data on disk | genesis CSV/JSON.
  Tier-1 stake-weighted genesis is selected by `CL_GENESIS_CONTAINER_PATH` pointing at
  `genesis.json`.
- **sidecar — Go libp2p, one per gl0** (compose profile `l0`, started with gl0 via `depends_on`,
  `docker-compose.nakamoto-overlay.yaml:110-112`). GossipSub transport for the gl0 mesh. Reads
  `NAKAMOTO_NUM_SHARDS` to eager-join every shard's checkpoint/attestation topic at startup
  (`docker-compose.nakamoto-sidecar.yaml:41-49`).
- **gl1 — hypergraph L1, BFT.** `run-validator`, BFT-joins hg node 0 (gl0-0). Forwards
  DAG/AllowSpend/TokenLock blocks to gl0 **via the sidecar** (#196/#197) using the same
  `SIDECAR_HOST` (`docker-compose.nakamoto-overlay.yaml:113-120`). Recomputes the
  `historicalStakeSnapshots` state-proof, so it needs the same `k₁=32` as gl0 (both run
  `CL_APP_ENV=dev`, resolved from `application.conf`).
- **ml0 — metagraph L0, BFT.** `run-genesis` on the genesis node (m`k`-0, from `ml0.jar
  create-genesis <genesis.csv>`, `entrypoint.sh:198-209`) / `run-validator` on followers. Uses
  the shared gl0 as its **global L0** (`CL_GLOBAL_L0_PEER_*` in `docker-env-setup.sh:33-43`).
  Emits `CurrencySnapshot` state-channel binaries to gl0.
- **cl1 — currency L1, BFT** / **dl1 — data L1, BFT.** Both dial their own metagraph's ml0
  (`CL_L0_PEER_HTTP_HOST=ml0-m${k}-0`, `docker-env-setup.sh:291-293`). Per-metagraph.

**All metagraphs share one gl0.** gl0 accepts a metagraph's state-channel binary only if a
signer is in the seedlist with alias **`metagraph-op`** (`compose-runner.sh:175-191`); that
alias is filtered out of the `StakeRegistry` (`Main.scala`/`GlobalSnapshotConsensus.scala`) so
VRF stake stays over hypergraph validators only.

---

## 6. Startup sequence + readiness gates

`compose-runner.sh` brings layers up in dependency order, gating each on the previous being
*functional* (not merely *present*):

1. **gl0** (`--profile l0`, pulls sidecar) — all gl0 started together (`:679-689`).
2. Wait until gl0 **cluster formed** (`/cluster/info` ≥ 1, `:694-716`).
3. **gl1** (`--profile l1`, `:720-738`).
4. Wait until gl0 is **finalizing** (`latest/ordinal ≥ 2`, `:753-781`) — *not just formed*. A
   metagraph's one-shot genesis-full `CurrencySnapshot` is lost if gl0's head isn't available
   when it lands (run-27 seeding-race fix).
5. **Metagraphs** (`:783-1086`): optional **grind pre-pass** (when `num-shards>1 &&
   metagraphs>1`, grinds each genesis key until `shardIdFor(addr)==k mod M` for even shard
   spread, then refreshes gl0 seedlist + cohort join ids once) → ml0 genesis → ml0 → wait ml0
   ready → cl1/dl1 → per-metagraph background tx-sender.
6. **snapshot-streaming** (postgres + indexer, unless `--skip-streaming`, `:1099-1210`).
7. **Prometheus + Grafana** (only with `--grafana`, `:1214-1284`).

---

## 7. Key env → HOCON knobs

Test overrides live in `docker-compose.nakamoto-overlay.yaml`; HOCON reads them via `${?VAR}`
in `modules/node-shared/src/main/resources/application.conf` (project rule: **never**
`sys.env.get` in code — see `feedback_prefer_hocon_over_sysenv`).

| Env | Overlay default | Meaning |
|---|---|---|
| `NAKAMOTO_MODE` | `true` | Selects `run-nakamoto` in entrypoint. |
| `NAKAMOTO_GENESIS_TIME_MS` | per-run | Coordinated genesis slot-0 time (90s in the future). |
| `NAKAMOTO_LDD_{CUTOFF,OFFSET,BASELINE,AMPLITUDE}` | **30** / 1 / 0.05 / 0.5 | LDD snowplow leader-election curve. `cutoff=30` active since `8c55c6341` (2026-06-24). |
| `NAKAMOTO_SLOTS_PER_EPOCH` / `NAKAMOTO_SLOT_DURATION_MS` | 60 / 1000 | Epoch + slot timing. |
| `NAKAMOTO_NUM_SHARDS` | 1 | Hierarchical shard count M (`--num-shards`). 1 = inert. |
| `NAKAMOTO_COMMITTEE_K_DRAW` / `_K_QUORUM` | 8 / 6 | Committee draw size / admit quorum (`application.conf:449-454`). **These are the live committee knobs.** `set-env.sh:398-402` auto-derives `K_DRAW=N`, `K_QUORUM=⌈2N/3⌉` from `--num-gl0` unless you set them explicitly. |
| `NAKAMOTO_LOCAL_EVENTS_{ENABLED,BIND,PORT}` | true / 0.0.0.0 / 50054 | Reactive gRPC stream the JS tests subscribe to. |
| `NAKAMOTO_STAKE_DISTRIBUTION` | via `--stake-dist` (default `harmonic` at N≥2) | Tier-1 stake-weighted genesis. |

> ⚠ **Dead knob — `--shards` / `NAKAMOTO_COMMITTEE_K_TARGET`.** `--shards=K` exports
> `NAKAMOTO_COMMITTEE_K_TARGET` (`set-env.sh:316-320`) and the overlay forwards it
> (`docker-compose.nakamoto-overlay.yaml:84`), but **nothing reads it**: there is no
> `${?NAKAMOTO_COMMITTEE_K_TARGET}` substitution and no Scala read. Committee draw/quorum were
> decoupled from the old single `committeeKTarget` in `396ca81b0` (2026-06-04); the live
> knobs are `K_DRAW`/`K_QUORUM` above. **Passing `--shards` has no effect.** Size the committee
> with `--num-gl0` (auto) or explicit `NAKAMOTO_COMMITTEE_K_{DRAW,QUORUM}`.

---

## 8. Run + debug SOP

**Standard local e2e** (see `feedback_e2e_run_command`):

```bash
RUN_TIMEOUT_SECONDS=14400 just test --skip-streaming \
  --num-gl0=6 --metagraphs=2 --num-shards=2 \
  --stake-dist=harmonic --keep-alive --grafana
```

- `--skip-streaming` is **mandatory** unless you're testing the indexer (it needs a
  `GITHUB_TOKEN`).
- `--num-shards=N` (hierarchical M) is **not** `--shards=K` — and `--shards` is a no-op (§7).
- After changing `dag-l1`/`dag-l0`/`node-shared`/`shared`/`sdk`, run `just nuke-metagraph` then
  one full `just test`; thereafter `--skip-assembly` reuses JARs.
- `--keep-alive` leaves the cluster up; **logs always survive** under `nodes/<n>/<layer>-logs/`.
- Local topology ceiling is ~`2mg/2shard`; `4mg/4shard` OOMs/thrashes locally.

**Debugging (HARD SOP — `feedback_cluster_analysis_prom_sop`):** always pull **Prometheus** AND
correlate node logs — never logs alone.

- Prometheus is at **`localhost:19090`** (NOT 9090 — 9090 is container-internal). Confirm with
  `docker port prometheus` first.
- Key series: `dag_nakamoto_finalized_ordinal` (vs `…_slot`), `…_fork_count`,
  `…_orphan_buffer_size`, `…_reorgs_total`, `…_shard_checkpoint_rejected_total`. (Names are
  Micrometer-emitted at runtime; verify on first use.)
- **HEAD≠finalized trap:** `dag_nakamoto_ordinal` is the tip, not the finalized height — judge
  health by *finalized* spread across nodes.
- Logs: `nodes/<i>/gl0-logs/gl0-run.log`, `nodes/<i>/gl1-logs/…`, `nodes/m<k>-<i>/{ml0,cl1,dl1}-logs/…`.

---

## 9. Caps / limits (fail-fast in `set-env.sh`)

- `--num-gl0 ≤ 16` (`:365`); `--metagraphs ≤ 5` (`:361`); `--num-shards ≤ 5` (`:369`).
- Hypergraph IP-safe ceiling = 20 (`NAKAMOTO_MAX_HG_NODES`, derived `:711-725`).
- Metagraph nodes capped at 9 (string-concat allocation, `:742`).

---

## 10. Cross-references

- Monitoring/metrics detail: `docs/nakamoto/E2E-MONITORING-RUNBOOK.md`.
- LocalEvents gRPC contract: `docs/nakamoto/LOCAL-EVENTS-SERVICE-DESIGN.md`.
- Sharding model: `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` and ADR-0016/ADR-0017.
- Dev commands: `docker/README.md`. Codebase map: `docs/CODEBASE_MAP.md` (Build & Deploy).
- Within-node consensus architecture: `docs/nakamoto-architecture.dot`.
