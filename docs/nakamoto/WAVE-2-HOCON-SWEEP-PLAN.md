# Wave 2 — Full HOCON Sweep Plan

**Status:** Catalog + design draft. NO IMPLEMENTATION.
**Owner:** TBD (impl agent).
**Date drafted:** 2026-05-21.
**Predecessor:** Wave 1 (Path 1) migrated `NAKAMOTO_ETA_ROTATION_SNAPSHOTS` and
`NAKAMOTO_KEEP_DEPTH_BEHIND_FINALIZED` to HOCON (`nakamoto.eta-rotation-snapshots`,
`nakamoto.keep-depth-behind-finalized`). This plan sweeps the remaining `NAKAMOTO_*`
env-reads in production Scala.

## 1. Summary

| Metric | Value |
| --- | --- |
| Production `sys.env.get("NAKAMOTO_…")` call sites remaining | **24** |
| Distinct env-var names remaining | **23** (one — `NAKAMOTO_CONFIRMATION_DEPTH` — read at two sites) |
| Already migrated (Wave 1) | `NAKAMOTO_ETA_ROTATION_SNAPSHOTS`, `NAKAMOTO_KEEP_DEPTH_BEHIND_FINALIZED` |
| Non-`NAKAMOTO_*` env-reads in same files (kept for context) | 4 (`CL_KES_SECURE_STORE_DIR`, `CL_L0_TOKEN_IDENTIFIER`, `CL_FOLLOWER_ID`, `CL_VERSION_HASH`/`CL_METAGRAPH_VERSION_HASH`/`CL_JAR_HASH`, `SIDECAR_GRPC_PORT`, `TESSELLATION_DATA_DIR`) — **out of scope** for this wave (already follow the `CL_*`/deploy-tooling pattern, not the Nakamoto consensus surface). |

The harness also exports `NAKAMOTO_STAKE_DISTRIBUTION` and `NAKAMOTO_GENESIS_SEED`
from `docker/bin/set-env.sh`, but neither name is read by any production Scala
file — both flow through other mechanisms (stake distribution lands in
per-cluster `genesis.csv`; seed feeds awk in set-env.sh). Out of scope.

The migration goal is total elimination of `sys.env.get("NAKAMOTO_…")` from
production code. Tests may keep `sys.env` if they want to honor an env override
in a fixture, but the production path goes through `SharedConfig.nakamoto`
exclusively.

## 2. Per-env-var catalog

Legend for **Criticality**:

- **CRITICAL** — all nodes in the cluster MUST agree on the value for safety
  (changing it across operators splits consensus). Slight ops-script-level
  override may be permitted but the cluster operator owns the constant.
- **OPS-ONLY** — local-node knob (timeout, memory cap, ticker cadence, log
  threshold). Differing values across nodes is acceptable; this only tunes the
  local observation envelope.
- **MIXED** — boundary case (e.g. attestation skew bound: too-loose at one node
  affects only what *that* node finalizes via `T_count`; not safety-critical
  but anomalously-loose values cause divergence).

| # | Env var | Default | File:line | What it controls | Criticality | Proposed HOCON key |
|---|---|---|---|---|---|---|
| 1 | `NAKAMOTO_GENESIS_TIME_MS` | `System.currentTimeMillis()` | `dag-l0/.../snapshot/GlobalSnapshotConsensus.scala:104` | Unix-epoch ms at which slot 0 starts. Per-cluster constant; nodes that disagree never see overlapping VRF eligibility. | **CRITICAL** | `nakamoto.genesis.time-ms` |
| 2 | `NAKAMOTO_GENESIS_ETA` | `"tessellation-nakamoto-genesis"` (literal bytes) | `dag-l0/.../snapshot/GlobalSnapshotConsensus.scala:408` | Per-cluster genesis-eta seed bytes used by the (legacy) `NakamotoTriggerState`. The newer `nakamotoGenesisEta` (line 115) ignores this env and hard-codes a Blake2b digest of `"tessellation-nakamoto-genesis-eta-v1"` — see Open Question #4. | **CRITICAL** if used | `nakamoto.genesis.eta-seed` |
| 3 | `NAKAMOTO_LDD_CUTOFF` | `LddConfig.Default.lddCutoff` = `15` | `dag-l0/.../snapshot/GlobalSnapshotConsensus.scala:498` | LDD snowplow γ — slot count at which the ramp reaches amplitude. | **CRITICAL** | `nakamoto.ldd.cutoff` |
| 4 | `NAKAMOTO_LDD_OFFSET` | `LddConfig.Default.offset` = `1` | `dag-l0/.../snapshot/GlobalSnapshotConsensus.scala:499` | LDD snowplow ψ — minimum gap before any eligibility. | **CRITICAL** | `nakamoto.ldd.offset` |
| 5 | `NAKAMOTO_LDD_BASELINE` | `LddConfig.Default.baselineDifficulty` = `Ratio(1,20)` | `dag-l0/.../snapshot/GlobalSnapshotConsensus.scala:501` | LDD snowplow fB — difficulty in recovery region (δ ≥ γ). Parsed Double, locked into `Ratio` at boot. | **CRITICAL** | `nakamoto.ldd.baseline-difficulty` |
| 6 | `NAKAMOTO_LDD_AMPLITUDE` | `LddConfig.Default.amplitude` = `Ratio(1,2)` | `dag-l0/.../snapshot/GlobalSnapshotConsensus.scala:506` | LDD snowplow fA — peak difficulty at ramp top. Parsed Double, locked into `Ratio` at boot. | **CRITICAL** | `nakamoto.ldd.amplitude` |
| 7 | `NAKAMOTO_SLOTS_PER_EPOCH` | `60L` | `dag-l0/.../snapshot/GlobalSnapshotConsensus.scala:512` | Slots per Cardano-equivalent epoch boundary. Logged & passed to leader loop. | **CRITICAL** | `nakamoto.slots-per-epoch` |
| 8 | `NAKAMOTO_COMMITTEE_K_TARGET` | `math.max(1, validatorPeers.size)` (= active gl0 count, dynamic) | `dag-l0/.../snapshot/GlobalSnapshotConsensus.scala:846` | Committee-sortition target size K for the metagraph-binary attestation gate. **Dynamic default** — cannot be cluster-static. | **CRITICAL** when overridden; default is degenerate K=N | `nakamoto.committee.k-target` (Option, default `None` → fall through to `math.max(1, active)`) |
| 9 | `NAKAMOTO_SLOT_DURATION_MS` | `1000L` | `dag-l0/.../snapshot/nakamoto/SnapshotLeaderLoop.scala:455` | Slot tick duration; tied to wall-clock & slot-derivation math. Cluster nodes MUST agree. | **CRITICAL** | `nakamoto.slot-duration-ms` |
| 10 | `NAKAMOTO_CONFIRMATION_DEPTH` | `255L` | `dag-l0/.../snapshot/nakamoto/SnapshotLeaderLoop.scala:619` | k₁ — operational confirmation depth (Phase 2 → Phase 3 boundary; `T_depth1` trigger). | **CRITICAL** | `nakamoto.finality.confirmation-depth` |
| 10b | `NAKAMOTO_CONFIRMATION_DEPTH` | `255L` (dup) | `dag-l0/.../snapshot/nakamoto/NakamotoSyncDaemon.scala:59` | Same constant — Tier 2/Tier 3 catch-up boundary. Same default. | **CRITICAL** | `nakamoto.finality.confirmation-depth` (single source) |
| 11 | `NAKAMOTO_ARCHIVAL_DEPTH` | `65536L` | `dag-l0/.../snapshot/nakamoto/SnapshotLeaderLoop.scala:637` | k₂ — archival depth (Phase 3; `T_depth2` trigger). | **CRITICAL** | `nakamoto.finality.archival-depth` |
| 12 | `NAKAMOTO_SNOWBALL_BETA` | `10` | `node-shared/.../domain/nakamoto/SnowballAccumulator.scala:101` | Snowball β — leader-minus-runner-up margin to decide. Cluster-aligned per AVALANCHE-ATTESTATION-PROPOSAL.md §3.4. | **CRITICAL** | `nakamoto.snowball.beta` |
| 13 | `NAKAMOTO_SNOWBALL_K` | `8` | `node-shared/.../domain/nakamoto/SnowballAccumulator.scala:112` | Snowball K — peer-sample size. Cluster-aligned (memory's K=8/α=5/β=10 recommendation). | **CRITICAL** | `nakamoto.snowball.k` |
| 14 | `NAKAMOTO_SNOWBALL_ALPHA` | `5` | `node-shared/.../domain/nakamoto/SnowballAccumulator.scala:122` | Snowball α — per-round majority recruitment threshold. Cluster-aligned. | **CRITICAL** | `nakamoto.snowball.alpha` |
| 15 | `NAKAMOTO_ATTESTATION_THRESHOLD` | `Ratio(2,3)` | `node-shared/.../domain/nakamoto/TipTracker.scala:131-136` | BFT-classic attestation finality threshold. Parsed Double, locked into `Ratio`. Used by `T_weight`, `T_count`, fallback-evidence sites. | **CRITICAL** | `nakamoto.finality.attestation-threshold` |
| 16 | `NAKAMOTO_MAX_ATTESTATION_SKEW_MS` | `60000L` | `node-shared/.../domain/nakamoto/TipTracker.scala:149-153` | Clock-skew bound for accepting `MetagraphAttestation.attestedAt` against local wall-clock. | **MIXED** — local-only enforcement, but very-loose values can let a malicious peer poison `T_count`. | `nakamoto.finality.max-attestation-skew-ms` |
| 17 | `NAKAMOTO_COMMITTEE_GATE_TIMEOUT_MS` | `30000L` | `node-shared/.../domain/nakamoto/MetagraphCommitteeGate.scala:108` | Sender-path wait for committee threshold before dropping binary. | **OPS-ONLY** | `nakamoto.committee.gate-timeout-ms` |
| 18 | `NAKAMOTO_COMMITTEE_GATE_POLL_INTERVAL_MS` | `250L` | `node-shared/.../domain/nakamoto/MetagraphCommitteeGate.scala:114` | Sender-path poll interval for threshold check. | **OPS-ONLY** | `nakamoto.committee.gate-poll-interval-ms` |
| 19 | `NAKAMOTO_ORPHAN_BUFFER_CAP` | `256` | `node-shared/.../domain/nakamoto/MetagraphOrphanBuffer.scala:101` | In-memory cap on orphan-buffered binaries (memory pressure knob). | **OPS-ONLY** | `nakamoto.orphan-buffer.cap` |
| 20 | `NAKAMOTO_RECENT_ADMIT_CAP` | `1024` | `node-shared/.../domain/nakamoto/MetagraphOrphanBuffer.scala:108` | In-memory cap on recent-admission cache. | **OPS-ONLY** | `nakamoto.orphan-buffer.recent-admit-cap` |
| 21 | `NAKAMOTO_SLASH_EVIDENCE_WINDOW` | `100L` | `node-shared/.../domain/nakamoto/slashing/SlashableEvidenceValidator.scala:79` | Slashing evidence-window in epochs; matches `cooldown_epochs` per SLASHING-DESIGN.md §6. | **CRITICAL** — evidence acceptance window must agree network-wide for slashing determinism. | `nakamoto.slashing.evidence-window-epochs` |
| 22 | `NAKAMOTO_OPTIMISTIC_MIN_FRACTION` | `Ratio(1,2)` | `node-shared/.../domain/nakamoto/StakeRegistry.scala:104-107` | Minimum stake-fraction of seedlist that must be observed-active before optimistic finality kicks in. Parsed Double, locked into `Ratio`. | **MIXED** — local quorum gate, but operators must align or some nodes optimistic-finalize earlier. | `nakamoto.stake.optimistic-min-fraction` |
| 23 | `NAKAMOTO_REBOOTSTRAP_REFUSE_THRESHOLD` | `3L` | `dag-l0/.../snapshot/nakamoto/RebootstrapOrchestrator.scala:71-75` | Sustained divergent-refuse count over the observation window required to trigger reset. | **OPS-ONLY** (default-OFF master switch ships disabled). | `nakamoto.rebootstrap.refuse-threshold` |
| 24 | `NAKAMOTO_REBOOTSTRAP_COOLDOWN_MS` | `300000L` (5 min) | `dag-l0/.../snapshot/nakamoto/RebootstrapOrchestrator.scala:81-85` | Cooldown after rebootstrap before another can fire. | **OPS-ONLY** | `nakamoto.rebootstrap.cooldown-ms` |
| 25 | `NAKAMOTO_REBOOTSTRAP_TICK_MS` | `30000L` | `dag-l0/.../snapshot/nakamoto/RebootstrapOrchestrator.scala:90-94` | Ticker cadence for divergent-refuse observation. | **OPS-ONLY** | `nakamoto.rebootstrap.tick-ms` |
| 26 | `NAKAMOTO_REBOOTSTRAP_ENABLED` | `false` | `dag-l0/.../snapshot/nakamoto/RebootstrapOrchestrator.scala:99-103` | Master switch for the rebootstrap orchestrator. Default-OFF in production. | **OPS-ONLY** | `nakamoto.rebootstrap.enabled` |

(Numbering: 1-26 with #10/#10b sharing key. Distinct env-var names = 23, distinct
call sites = 24.)

### 2.1 Non-NAKAMOTO sys.env reads in the same files (not migrated by this wave)

These exist alongside the NAKAMOTO_* reads but are out of scope (different
ownership domain — `CL_*` is the long-standing Tessellation deploy contract).

| File:line | Env var | Note |
|---|---|---|
| `dag-l0/.../GlobalSnapshotConsensus.scala:619,625` | `CL_KES_SECURE_STORE_DIR` | KES disk-backed secure store path; already filesystem-tied, separate workstream. |
| `node-shared/.../app/TessellationIOApp.scala:177` | `CL_L0_TOKEN_IDENTIFIER` | Long-standing deploy-tooling contract. |
| `node-shared/.../app/TessellationIOApp.scala:184,188` | `CL_VERSION_HASH`, `CL_METAGRAPH_VERSION_HASH` | Build-time hash overrides; deploy-only. |
| `node-shared/.../app/TessellationIOApp.scala:378` | `CL_JAR_HASH` | Build-time hash override. |
| `node-shared/.../fork/ExitOnFork.scala:18,29` | `CL_FOLLOWER_ID` and an arbitrary flag arg | Debug-only fork-exit machinery. |
| `dag-l0/.../modules/Services.scala:161` | `SIDECAR_GRPC_PORT` | Sidecar wiring; already plumbed via `Services.make` constructor. |
| `dag-l1/.../Main.scala:118` | `SIDECAR_GRPC_PORT` | Same as above. |
| `dag-l0/.../snapshot/GlobalSnapshotConsensus.scala:1269` | `TESSELLATION_DATA_DIR` | Filesystem path; deploy-only. |

## 3. Domain groupings

### 3.1 Consensus finality (CRITICAL)
- `NAKAMOTO_CONFIRMATION_DEPTH` (k₁, 255) — read at 2 sites; collapse to one.
- `NAKAMOTO_ARCHIVAL_DEPTH` (k₂, 65536).
- `NAKAMOTO_ATTESTATION_THRESHOLD` (2/3).
- `NAKAMOTO_MAX_ATTESTATION_SKEW_MS` (60000).

### 3.2 Snowball / Avalanche cascade (CRITICAL)
- `NAKAMOTO_SNOWBALL_K` (8).
- `NAKAMOTO_SNOWBALL_ALPHA` (5).
- `NAKAMOTO_SNOWBALL_BETA` (10).

### 3.3 Slot / timing (CRITICAL)
- `NAKAMOTO_GENESIS_TIME_MS` (no static default).
- `NAKAMOTO_SLOT_DURATION_MS` (1000).
- `NAKAMOTO_SLOTS_PER_EPOCH` (60).

### 3.4 LDD snowplow (CRITICAL)
- `NAKAMOTO_LDD_CUTOFF` (15).
- `NAKAMOTO_LDD_OFFSET` (1).
- `NAKAMOTO_LDD_BASELINE` (1/20 Ratio).
- `NAKAMOTO_LDD_AMPLITUDE` (1/2 Ratio).

### 3.5 Eta / period (DONE — Wave 1)
- `NAKAMOTO_ETA_ROTATION_SNAPSHOTS` (2550) → `nakamoto.eta-rotation-snapshots` ✅
- `NAKAMOTO_KEEP_DEPTH_BEHIND_FINALIZED` (255) → `nakamoto.keep-depth-behind-finalized` ✅

### 3.6 Committee sortition (mostly CRITICAL)
- `NAKAMOTO_COMMITTEE_K_TARGET` (default = active count — must remain dynamic; expose as `Option`).
- `NAKAMOTO_COMMITTEE_GATE_TIMEOUT_MS` (30000) — OPS-ONLY.
- `NAKAMOTO_COMMITTEE_GATE_POLL_INTERVAL_MS` (250) — OPS-ONLY.

### 3.7 Stake registry (MIXED)
- `NAKAMOTO_OPTIMISTIC_MIN_FRACTION` (1/2).

### 3.8 Genesis / bootstrap (CRITICAL)
- `NAKAMOTO_GENESIS_TIME_MS` — also under 3.3.
- `NAKAMOTO_GENESIS_ETA` — see Open Question #4 (suspect unused code path).

### 3.9 Slashing (CRITICAL)
- `NAKAMOTO_SLASH_EVIDENCE_WINDOW` (100 epochs).

### 3.10 Orphan buffer (OPS-ONLY)
- `NAKAMOTO_ORPHAN_BUFFER_CAP` (256).
- `NAKAMOTO_RECENT_ADMIT_CAP` (1024).

### 3.11 Rebootstrap orchestrator (OPS-ONLY)
- `NAKAMOTO_REBOOTSTRAP_REFUSE_THRESHOLD` (3).
- `NAKAMOTO_REBOOTSTRAP_COOLDOWN_MS` (300000).
- `NAKAMOTO_REBOOTSTRAP_TICK_MS` (30000).
- `NAKAMOTO_REBOOTSTRAP_ENABLED` (false).

## 4. Typed config schema diff

Proposed structure (additions in **bold**; existing Wave 1 fields kept). All
sub-cases stay under `NakamotoConfig` in
`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/types.scala`.

```scala
final case class NakamotoConfig(
  // Wave 1 — already landed.
  etaRotationSnapshots: PosLong,
  keepDepthBehindFinalized: PosLong,

  // Wave 2 — new sub-cases below.
  genesis: NakamotoGenesisConfig,            // §4.1
  slot: NakamotoSlotConfig,                  // §4.2
  ldd: LddConfig,                            // §4.3 reuses existing schema/nakamoto/LddConfig
  snowball: NakamotoSnowballConfig,          // §4.4
  finality: NakamotoFinalityConfig,          // §4.5
  committee: NakamotoCommitteeConfig,        // §4.6
  stake: NakamotoStakeConfig,                // §4.7
  slashing: NakamotoSlashingConfig,          // §4.8
  orphanBuffer: NakamotoOrphanBufferConfig,  // §4.9
  rebootstrap: NakamotoRebootstrapConfig     // §4.10
)
```

### 4.1 `NakamotoGenesisConfig`

```scala
final case class NakamotoGenesisConfig(
  // Optional: when absent the launching JVM falls back to `System.currentTimeMillis()`.
  // Multi-node clusters MUST set this in HOCON or via the env-var substitution.
  timeMs: Option[Long],
  // Bytes used to seed the legacy `NakamotoTriggerState.initial`. See Open Question #4 —
  // production codepath ignores this in favor of the hard-coded Blake2b seed; we mirror the
  // existing env-var-or-default behavior for ops parity but the impl agent should decide
  // whether to delete this field if the codepath proves dead.
  etaSeed: NonEmptyString
)
```

### 4.2 `NakamotoSlotConfig`

```scala
final case class NakamotoSlotConfig(
  durationMs: PosLong,         // default 1000
  slotsPerEpoch: PosLong       // default 60
)
```

### 4.3 `LddConfig` (existing schema)

The schema already exists at `modules/shared/src/main/scala/io/constellationnetwork/schema/nakamoto/ldd.scala`.
Wire it directly under `NakamotoConfig.ldd`. The existing `LddConfig.fromDoubles` constructor
covers the Double→`Ratio` conversion the current env-read path does inline.

PureConfig reader: ConfigSource handles `Int` + `Double` natively; we need a
custom `ConfigReader[Ratio]` that delegates to `fromDoubles` (or a hand-rolled
`baselineDifficultyDouble`/`amplitudeDouble` HOCON field + post-construct
conversion). Recommend hand-rolled Double fields + post-construct conversion in
`SharedConfigReader → SharedConfig` so the constraint "store as Ratio in
production code" stays close to the existing pattern (LddConfig.Default).

### 4.4 `NakamotoSnowballConfig`

```scala
final case class NakamotoSnowballConfig(
  k: PosInt,      // default 8
  alpha: PosInt,  // default 5
  beta: PosInt    // default 10
)
```

### 4.5 `NakamotoFinalityConfig`

```scala
final case class NakamotoFinalityConfig(
  confirmationDepth: PosLong,         // default 255
  archivalDepth: PosLong,             // default 65536
  attestationThresholdDouble: Double, // default 2.0/3.0; locked to Ratio at boot
  maxAttestationSkewMs: PosLong       // default 60000
) {
  def attestationThreshold: Ratio =
    Ratio(attestationThresholdDouble, 18)
}
```

### 4.6 `NakamotoCommitteeConfig`

```scala
final case class NakamotoCommitteeConfig(
  // None → fall through to math.max(1, validatorPeers.size) at construction time.
  // Production cluster sortition sets this explicitly (e.g. K=4 on N=8).
  kTarget: Option[PosInt],
  gateTimeoutMs: PosLong,        // default 30000
  gatePollIntervalMs: PosLong    // default 250
)
```

### 4.7 `NakamotoStakeConfig`

```scala
final case class NakamotoStakeConfig(
  // Stored as Double in HOCON; locked to Ratio at first read.
  optimisticMinFractionDouble: Double  // default 0.5
) {
  def optimisticMinFraction: Ratio =
    Ratio(optimisticMinFractionDouble, 18)
}
```

### 4.8 `NakamotoSlashingConfig`

```scala
final case class NakamotoSlashingConfig(
  evidenceWindowEpochs: PosLong  // default 100
)
```

### 4.9 `NakamotoOrphanBufferConfig`

```scala
final case class NakamotoOrphanBufferConfig(
  cap: PosInt,             // default 256
  recentAdmitCap: PosInt   // default 1024
)
```

### 4.10 `NakamotoRebootstrapConfig`

```scala
final case class NakamotoRebootstrapConfig(
  enabled: Boolean,         // default false
  refuseThreshold: PosLong, // default 3
  cooldownMs: PosLong,      // default 300000
  tickMs: PosLong           // default 30000
)
```

### 4.11 `application.conf` additions

Extending the existing `nakamoto { ... }` block, mirroring the Wave 1 env-var-substitution
pattern (`${?NAKAMOTO_…}`):

```hocon
nakamoto {
  # Wave 1 — already present.
  eta-rotation-snapshots = 2550
  eta-rotation-snapshots = ${?NAKAMOTO_ETA_ROTATION_SNAPSHOTS}
  keep-depth-behind-finalized = 255
  keep-depth-behind-finalized = ${?NAKAMOTO_KEEP_DEPTH_BEHIND_FINALIZED}

  genesis {
    # `time-ms` left unset by default; deploy tooling sets the env var per cluster.
    time-ms = ${?NAKAMOTO_GENESIS_TIME_MS}
    eta-seed = "tessellation-nakamoto-genesis"
    eta-seed = ${?NAKAMOTO_GENESIS_ETA}
  }

  slot {
    duration-ms = 1000
    duration-ms = ${?NAKAMOTO_SLOT_DURATION_MS}
    slots-per-epoch = 60
    slots-per-epoch = ${?NAKAMOTO_SLOTS_PER_EPOCH}
  }

  ldd {
    cutoff = 15
    cutoff = ${?NAKAMOTO_LDD_CUTOFF}
    offset = 1
    offset = ${?NAKAMOTO_LDD_OFFSET}
    baseline-difficulty = 0.05
    baseline-difficulty = ${?NAKAMOTO_LDD_BASELINE}
    amplitude = 0.5
    amplitude = ${?NAKAMOTO_LDD_AMPLITUDE}
  }

  snowball {
    k = 8
    k = ${?NAKAMOTO_SNOWBALL_K}
    alpha = 5
    alpha = ${?NAKAMOTO_SNOWBALL_ALPHA}
    beta = 10
    beta = ${?NAKAMOTO_SNOWBALL_BETA}
  }

  finality {
    confirmation-depth = 255
    confirmation-depth = ${?NAKAMOTO_CONFIRMATION_DEPTH}
    archival-depth = 65536
    archival-depth = ${?NAKAMOTO_ARCHIVAL_DEPTH}
    attestation-threshold = 0.6666666666666667
    attestation-threshold = ${?NAKAMOTO_ATTESTATION_THRESHOLD}
    max-attestation-skew-ms = 60000
    max-attestation-skew-ms = ${?NAKAMOTO_MAX_ATTESTATION_SKEW_MS}
  }

  committee {
    # k-target left unset → math.max(1, validatorPeers.size) at construction.
    k-target = ${?NAKAMOTO_COMMITTEE_K_TARGET}
    gate-timeout-ms = 30000
    gate-timeout-ms = ${?NAKAMOTO_COMMITTEE_GATE_TIMEOUT_MS}
    gate-poll-interval-ms = 250
    gate-poll-interval-ms = ${?NAKAMOTO_COMMITTEE_GATE_POLL_INTERVAL_MS}
  }

  stake {
    optimistic-min-fraction = 0.5
    optimistic-min-fraction = ${?NAKAMOTO_OPTIMISTIC_MIN_FRACTION}
  }

  slashing {
    evidence-window-epochs = 100
    evidence-window-epochs = ${?NAKAMOTO_SLASH_EVIDENCE_WINDOW}
  }

  orphan-buffer {
    cap = 256
    cap = ${?NAKAMOTO_ORPHAN_BUFFER_CAP}
    recent-admit-cap = 1024
    recent-admit-cap = ${?NAKAMOTO_RECENT_ADMIT_CAP}
  }

  rebootstrap {
    enabled = false
    enabled = ${?NAKAMOTO_REBOOTSTRAP_ENABLED}
    refuse-threshold = 3
    refuse-threshold = ${?NAKAMOTO_REBOOTSTRAP_REFUSE_THRESHOLD}
    cooldown-ms = 300000
    cooldown-ms = ${?NAKAMOTO_REBOOTSTRAP_COOLDOWN_MS}
    tick-ms = 30000
    tick-ms = ${?NAKAMOTO_REBOOTSTRAP_TICK_MS}
  }
}
```

## 5. Coupling constraints + startup validations

The Path 1 saga surfaced one explicit numeric coupling
(`keep-depth-behind-finalized ≥ 2 × eta-rotation-snapshots`). Wave 2 has several
analogous constraints. Recommend a single `NakamotoConfig.validate: Either[String, NakamotoConfig]`
called once at boot in `SharedConfigReader.toShared` (or wherever the reader currently lives) so
the cluster crashes loud on misconfig instead of silently producing safety
violations.

### 5.1 Snowball K/α/β constraint (CRITICAL)

Per AVALANCHE-ATTESTATION-PROPOSAL.md §0.A / §2.4: `α ≥ ⌈5K/8⌉` and `β ≥ K + α` is
the empirical floor. The K=8/α=5/β=10 default already satisfies both. Validation:

```scala
val kRequired = math.ceil(snowball.k.value * 5.0 / 8.0).toInt
require(snowball.alpha.value >= kRequired,
  s"snowball.alpha=${snowball.alpha} < ⌈5K/8⌉=${kRequired} — safety violation under split-honest adversary")
```

(`β ≥ K` is less rigid but recommended; spec mentions K=3/β=10 as a counterexample,
where K is too small for any β.)

### 5.2 Finality-depth ordering (CRITICAL)

`archival-depth ≥ confirmation-depth`. The defaults (`k₁=255`, `k₂=65536`)
satisfy this with comfortable headroom but a misconfig that inverts them lets
`T_depth2` fire before `T_depth1`, breaking the Phase-2→Phase-3 invariant in
`docs/nakamoto/attestation-and-finality.md` §0.3.

```scala
require(finality.archivalDepth.value >= finality.confirmationDepth.value,
  s"finality.archival-depth=${finality.archivalDepth} < confirmation-depth=${finality.confirmationDepth}")
```

### 5.3 `keep-depth-behind-finalized` vs `confirmation-depth` (Wave 1 coupling)

The Wave 1 doc already notes that
`keep-depth-behind-finalized ≥ confirmation-depth` is the "in-memory walkback
covers the depth-k window" guarantee. With Wave 1 + Wave 2 both present:

```scala
require(keepDepthBehindFinalized.value >= finality.confirmationDepth.value,
  s"keep-depth-behind-finalized=${keepDepthBehindFinalized} < finality.confirmation-depth=${finality.confirmationDepth}")
```

Note this is a softer constraint than the historical
`2 × eta-rotation-snapshots` recommendation — Path 1's disk fallback means
`keep-depth-behind-finalized < eta-rotation-snapshots` is now a perf knob, not
correctness. So the only HARD constraint is `keep-depth-behind-finalized ≥
confirmation-depth` (so depth-k finalization walkbacks stay in memory).

### 5.4 Attestation threshold range (CRITICAL)

`0 < attestation-threshold < 1`. Less than ½ trivially breaks BFT safety;
greater than 1 is incoherent.

```scala
require(finality.attestationThreshold > Ratio(0, 1) && finality.attestationThreshold < Ratio(1, 1),
  s"finality.attestation-threshold=${finality.attestationThreshold} outside (0, 1)")
require(finality.attestationThreshold >= Ratio(1, 2),
  s"finality.attestation-threshold=${finality.attestationThreshold} < 1/2 — BFT safety violation")
```

### 5.5 LDD constraints (CRITICAL)

Per `schema/nakamoto/ldd.scala`: `ψ ≤ γ` (offset ≤ cutoff); `0 ≤ fB ≤ fA ≤ 1`
(baseline ≤ amplitude). The default 1/20 ≤ 1/2 ≤ 1 and 1 ≤ 15 hold.

```scala
require(ldd.offset <= ldd.cutoff,
  s"ldd.offset=${ldd.offset} > ldd.cutoff=${ldd.cutoff}")
require(ldd.baselineDifficulty <= ldd.amplitude,
  s"ldd.baseline-difficulty=${ldd.baselineDifficulty} > ldd.amplitude=${ldd.amplitude}")
require(ldd.amplitude <= Ratio(1, 1) && ldd.baselineDifficulty >= Ratio(0, 1),
  "ldd amplitude/baseline outside [0, 1]")
```

### 5.6 Slot & timing constraints (OPS-ONLY)

`slot.duration-ms > 0` (PosLong covers this); `slots-per-epoch > 0` (PosLong covers this).
No cross-field constraint within Wave 2 — the genesis-time vs slot-duration coupling
is handled by the genesis-protocol invariant (all nodes get the same
`genesis.time-ms` from the same source).

### 5.7 Orphan buffer constraints (OPS-ONLY)

`recent-admit-cap ≥ cap` (admission cache persists longer than orphan-buffer entries
per the comment in `MetagraphOrphanBuffer.scala:103-105`). The defaults
(1024 ≥ 256) satisfy this; warn rather than `require` since misconfig is
non-fatal.

### 5.8 Rebootstrap constraints (OPS-ONLY)

`cooldown-ms ≥ tick-ms` (cooldown must outlast at least one ticker pass).
`refuse-threshold ≥ 1`. Defaults (300000 ≥ 30000, 3) satisfy.

### 5.9 Committee constraints (CRITICAL)

`k-target.forall(_ ≤ active-validator-count)` — but `active-validator-count` is
dynamic at runtime, so this is enforced at the construction site in
`GlobalSnapshotConsensus.scala` rather than at boot. The deploy harness already
performs this check (`docker/bin/set-env.sh:495-501`); recommend mirroring in
the Scala-side validator at the GSC construction site (current code does
`math.max(1, active)`; the impl agent should add `math.min(kTarget, active)` to
cap).

## 6. Migration sequencing

The impl agent should land Wave 2 in a sequence that minimizes intra-wave merge
conflicts and gives each phase its own validation lap (just-test e2e).

### Phase A — Wiring scaffold (1 PR, no semantic change)

1. Add **all** new sub-case classes (§§4.1-4.10) to `types.scala` under
   `NakamotoConfig`.
2. Extend `application.conf` with the full `nakamoto { ... }` block from §4.11.
3. Wire `SharedConfigReader → SharedConfig` to populate the new sub-cases.
4. Add `NakamotoConfig.validate` (§5) and call it once at boot.
5. Leave the `sys.env.get(...)` call sites unchanged (each one will pull its
   value from `sharedCfg.nakamoto.<…>` in subsequent phases).
6. Add a `KnobsRoundTripSuite`-style test that loads `application.conf`,
   asserts defaults match the existing companion-object defaults (e.g.
   `LddConfig.Default`, `SnowballAccumulator.K`, …) — locks the migration to
   no-default-drift.

This phase is byte-equivalent to current behavior — it only widens the typed
config surface.

### Phase B — Domain group migrations (one PR per sub-section)

Each PR replaces a single sub-case's `sys.env.get(...)` reads with the typed
field, in this recommended order. The order minimizes file-overlap conflicts:

1. **Snowball** (`§3.2`): 3 reads in one file (`SnowballAccumulator.scala`).
   Single-PR, minimal blast radius. Companions become deprecated; pass the
   typed config to `SnowballAccumulator.make`.
2. **Orphan buffer** (`§3.10`): 2 reads in one file (`MetagraphOrphanBuffer.scala`).
   Easy second.
3. **Slashing** (`§3.9`): 1 read in `SlashableEvidenceValidator.scala`. Trivial.
4. **Rebootstrap** (`§3.11`): 4 reads in one file (`RebootstrapOrchestrator.scala`).
5. **Committee** (`§3.6`): 1 read in `GlobalSnapshotConsensus.scala` for
   `kTarget` + 2 in `MetagraphCommitteeGate.scala` for timeouts. Same companion
   as snowball — companion-object defaults need careful handling since
   `MetagraphCommitteeGate.make` callers (line 974-975 of GSC) pass the
   companion `DefaultGateTimeoutMs` / `DefaultPollIntervalMs`. Swap the callers
   to read from `sharedCfg.nakamoto.committee.*`.
6. **Stake** (`§3.7`): 1 read in `StakeRegistry.scala`. The companion `Ratio`
   is read by both registry constructors (`equalWeight`, stake-weighted); swap
   both callers to take it as a parameter from `SharedConfig`.
7. **Finality** (`§3.1`): 4 reads. `NAKAMOTO_CONFIRMATION_DEPTH` lives in 2
   files (`SnapshotLeaderLoop.scala` line 619 + `NakamotoSyncDaemon.scala` line
   59) — **collapse to one read** in the GSC `make` Resource so both
   downstream sites get the same value. `NAKAMOTO_ATTESTATION_THRESHOLD` +
   `NAKAMOTO_MAX_ATTESTATION_SKEW_MS` in `TipTracker.scala` — same pattern:
   swap the `TipTracker.make` signature to take both as parameters; companion
   defaults move into `SharedConfig`.
8. **LDD** (`§3.4`): 4 reads. Localized to one block in `GlobalSnapshotConsensus.scala:495-511`.
   Replace the 4 `sys.env.get` reads with a direct `sharedCfg.nakamoto.ldd`
   resolution. Custom `ConfigReader[Ratio]` for `baseline-difficulty` +
   `amplitude` (or keep them as Double in HOCON and convert at the
   `NakamotoConfig → LddConfig` boundary).
9. **Slot/timing** (`§3.3`): 2 reads (slot duration, slots-per-epoch) in 2
   files (`SnapshotLeaderLoop.scala:455`, `GlobalSnapshotConsensus.scala:512`).
   Same pattern.
10. **Genesis** (`§3.8`): 2 reads. `NAKAMOTO_GENESIS_TIME_MS` is the
    semantically-loadbearing one — has `System.currentTimeMillis()` fallback
    that ONLY works for single-node dev launches; production must set it. Add
    a startup-time WARN if the value is None and the cluster has more than one
    node (PeerId set size > 1). `NAKAMOTO_GENESIS_ETA` see Open Question #4 —
    impl agent may delete the codepath entirely.

Total Wave 2 PR count: 10 phase-B PRs + 1 phase-A scaffold = 11 PRs. Each is
small (≤200 LOC) and independently testable.

### Phase C — Cleanup (1 PR)

- Delete the `sys.env.get(...)` companion-object readers if any survived as
  deprecated shims (none should; the per-domain PRs each remove their reads).
- Update any docstring that mentions `Override via NAKAMOTO_…` to also mention
  the HOCON key (matches the Wave 1 docstring pattern in `application.conf:283-303`).
- Delete `NakamotoConfig` (Wave 1 case-class) commented mentions of "Wave 2 of
  the sys.env-to-HOCON sweep handles the rest" — replaced by a stable docstring
  that says "all Nakamoto consensus knobs live here".

## 7. Open questions for user

1. **`NAKAMOTO_COMMITTEE_K_TARGET` default**: the env-default is dynamic
   (`math.max(1, validatorPeers.size)` = degenerate K=N). The proposed HOCON
   key is `Option[PosInt]` with `None` meaning "fall through to runtime". Is
   that semantics what you want, or should the HOCON have a hard default (e.g.
   `0` = sentinel for "use active count")?

2. **LDD `Ratio` representation**: the env-vars are typed `Double`, locked to
   `Ratio(d, 18)` at parse time so the threshold computation is exact. Options
   for HOCON: (a) keep as `Double` in HOCON, convert at the
   `NakamotoConfig → LddConfig` boundary (matches current env-read pattern,
   recommended); (b) accept HOCON `"1/20"`-style fractional strings and parse
   to `Ratio` directly (more accurate but new syntax). Recommend (a) unless
   you have appetite for a custom `ConfigReader[Ratio]`.

3. **`NAKAMOTO_ATTESTATION_THRESHOLD`** is currently Double-typed
   (`Ratio(_, 18)`) just like LDD. Same question as #2 — proposed default
   `0.6666666666666667` (15-decimal `2/3` approximation). Tolerable
   given the `Ratio(_, 18)` rounding, but if you'd prefer exact `Ratio(2, 3)`,
   we'd need the fractional-string ConfigReader. Recommend Double-with-rounding
   to match the env-var-equivalent path. **Note**: the Wave-1 companion
   constraint that `Ratio(2, 3) == Ratio(0.6666666666666667, 18)` should be
   verified by the impl agent before landing (a quick `Ratio.eqv` test).

4. **`NAKAMOTO_GENESIS_ETA` codepath aliveness**: there are TWO genesis-eta
   functions in `GlobalSnapshotConsensus.scala` — `nakamotoGenesisEta` (line
   115, hard-coded Blake2b-256 of a fixed string, used by `EtaStateManager`)
   and the legacy `NakamotoTriggerState.initial` path at line 408 (the env-read
   site). Inspect downstream usage of `NakamotoTriggerState` — if the
   `genesisEta` field is no longer load-bearing for any production path, the
   env-var migration is a no-op and the field can be deleted entirely instead
   of migrated. The impl agent should grep `NakamotoTriggerState` references
   before adding `nakamoto.genesis.eta-seed` to HOCON.

5. **`NAKAMOTO_GENESIS_TIME_MS` mandatoriness**: currently the env-var fallback
   is `System.currentTimeMillis()`, which silently allows multi-node clusters
   to misconfigure (each node picks its own genesis time → permanent slot
   drift). Should the migration:
   (a) keep the silent fallback for backwards compat (recommended for now);
   (b) require the value when cluster size > 1 (boot-time check) and crash
   loud otherwise;
   (c) derive from the genesis snapshot (the comment at line 100 mentions this
   as future work — out of scope for this wave).
   Recommend (a) + add a startup-time WARN as described in §6 Phase B step 10.

6. **`NAKAMOTO_MAX_ATTESTATION_SKEW_MS` & `NAKAMOTO_OPTIMISTIC_MIN_FRACTION`
   criticality**: I've marked both as MIXED. If you regard them as fully
   per-operator OPS-ONLY (i.e. cluster operators can choose), nothing changes
   in the migration; if you regard them as CRITICAL (cluster-aligned), the
   impl agent should add a gossip-aware drift check at boot (read a peer's
   value via /metrics or a config-introspection endpoint and WARN on
   divergence). Recommend MIXED + no boot-time gossip check.

7. **Companion-object default deletion**: the per-class companion objects
   (`SnowballAccumulator.K`, `TipTracker.FinalityThreshold`,
   `MetagraphCommitteeGate.DefaultGateTimeoutMs`, …) currently expose the env-read
   defaults as `val`s. After migration, should they:
   (a) survive as `val`s that read from a thread-local config holder (sketchy);
   (b) be deleted, with every caller plumbed through `SharedConfig` parameter
   (cleaner, more invasive);
   (c) survive as **pure compile-time constants** that match `application.conf`
   defaults (so test fixtures keep working without a `SharedConfig` in scope)?
   Recommend (c) — the companions become "matches the default in
   application.conf; production reads from `SharedConfig`". Tests can keep
   using `SnowballAccumulator.K` directly without standing up a `SharedConfig`.

8. **Pure-test sites that still read `sys.env`**: in scope to also flip the
   2 references in `KesGossipVerificationSuite.scala` / `NakamotoChainStoreSuite.scala`
   over to the typed-config path? Recommend leaving them with literal Long values
   (already the case for `etaRotationSnapshots = 2550L`) — tests should never
   depend on `sys.env`.

---

End of plan.
