# Metrics + Grafana Dashboard Revamp

Status: design + first implementation slice (2026-05-20)
Branch: `feature/serde-typeclass-shim` (worktree `agent-a1642a692b7aed9e9`)

## Goal

1. Support per-ordinal plotting + cross-node per-ordinal comparison on the same
   panel.
2. Modernize the overview dashboard so recent feature work (KES rotation,
   committee sortition, NIPoPoW level-µ, finality triggers, etc.) is observable.
3. Keep cardinality bounded — never let one snapshot ordinal pin a series alive
   forever.

The existing dashboard
(`nakamoto-test/grafana/dashboards/nakamoto-consensus.json`) is panel-rich for
the **time-indexed** view (ordinal vs slot, fill %, lag, gossip, etc.) but
**has no per-ordinal drill-down**. The 10 features the user called out are
mostly already emitted as either time-series or single counters — the gap is
in (a) per-ordinal labelling and (b) panel coverage of the new emitters.

## Phase 1 — Audit

### Metric inventory (Nakamoto-relevant subset)

Catalogued by grepping `Metrics[F]\.(updateGauge|incrementCounter|recordTime|recordDistribution)` across `modules/` and extracting the `dag_*` literal keys.

| Bucket | Emitters | Existing labels |
|---|---|---|
| **Chain progression** | `dag_nakamoto_ordinal`, `dag_nakamoto_finalized_ordinal`, `dag_nakamoto_slot`, `dag_nakamoto_chain_length`, `dag_nakamoto_chain_quality`, `dag_nakamoto_fork_count`, `dag_nakamoto_attestation_weight`, `dag_nakamoto_archival_ordinal`, `dag_nakamoto_snowball_highest_decided_ordinal` | `{node}` (added by Prometheus scrape config) |
| **Counters per node** | `dag_nakamoto_slots_won`, `dag_nakamoto_slots_trialed_total`, `dag_nakamoto_snapshots_produced`, `dag_nakamoto_snapshots_received`, `dag_nakamoto_snapshots_rejected`, `dag_nakamoto_finalized`, `dag_nakamoto_catchups`, `dag_nakamoto_archival_finalized`, `dag_nakamoto_reorgs`, `dag_nakamoto_forks_stored`, `dag_nakamoto_production_abandoned_total`, `dag_nakamoto_rebootstrap_initiated_total`, `dag_nakamoto_snowball_decisions_total`, `dag_nakamoto_attestations_rejected_skew_total` | `{node}` |
| **Finality triggers** | `dag_nakamoto_finality_triggers_fired_t_count_total`, `dag_nakamoto_finality_triggers_fired_t_weight_total`, `dag_nakamoto_finality_triggers_fired_t_depth1_total` | `{node}` |
| **KES** | `dag_nakamoto_kes_period_rotations_total`, `dag_nakamoto_kes_current_period`, `dag_nakamoto_kes_attestations_{signed,verified,sign_failed,invalid,no_sig,no_registry_entry,decode_failed}_total`, `dag_nakamoto_kes_snapshots_{signed,verified,...}_total`, `dag_nakamoto_kes_mg_attestations_*_total` | `{node}`; no period label on counters |
| **Distributions** | `dag_nakamoto_finality_latency_ms`, `dag_nakamoto_production_duration_ms`, `dag_nakamoto_slot_gap`, `dag_nakamoto_events_{drained,accepted,returned}` | `{node}` |
| **Labelled counter (only one today)** | `dag_nakamoto_slots_won_by_period` | `{node, eta_period, phase}` — task #74/75 reference pattern |
| **Snapshot acceptance** | `dag_*_snapshot_*` (cumulative + incremental token economics) | `{node}` |
| **Sidecar** | `sidecar_gossip_mesh_peers`, `sidecar_gossip_messages_{published,received}_total`, `sidecar_connected_peers_total`, `sidecar_dht_routing_table_size` | `{instance, topic}` |
| **HTTP** | `dag_http_request_count_total` | `{node, route, status}` |

### Gap analysis (vs. the 10 features the user enumerated)

| # | Feature | Has metrics? | Has dashboard panel? | Gap |
|---|---|---|---|---|
| 1 | KES rotation events + verify-fail counters | YES (`dag_nakamoto_kes_period_rotations_total`, plus 7 verify counters) | NO | Add dashboard panels |
| 2 | Avalanche cascade + Snowball confidence | PARTIAL (`dag_nakamoto_snowball_decisions_total`, `dag_nakamoto_snowball_highest_decided_ordinal`) | NO | Add dashboard; consider per-ordinal leader/runner-up margin gauge |
| 3 | Committee sortition wins per (operator, parent-snap, mg) | NO | NO | **Emit** + dashboard |
| 4 | NIPoPoW level-µ trial pass-rates per super-level | NO (computed in `TowerFinalizer.finalizeFromParts`, not emitted) | NO | **Emit** + dashboard |
| 5 | Tower density (rel-error per super-level vs §5.3 5% bound) | NO | NO | **Emit** + dashboard (deferred — TowerVerifier path) |
| 6 | T_count + T_depth2 + attestation-3/3 finality triggers | YES (all three fire counters, plus chain-quality gauge) | NO | Add panels |
| 7 | Chain-quality observable (1/2/3-of-3) | YES (`dag_nakamoto_chain_quality`) | NO | Add panel (overlay against fire counts) |
| 8 | Per-validator VRF stake-weighted threshold + eligibility outcomes | PARTIAL (`dag_nakamoto_slots_won`/`dag_nakamoto_slots_trialed_total` per node; no threshold gauge) | YES (Wins by Eta Period) | Add per-node threshold gauge (deferred — needs EligibilityChecker plumbing) |
| 9 | Genesis stateProof / MPT consistency | NO | NO | **Emit** boot-time gauge (deferred — would need test wiring) |
| 10 | NodeStakeAggregator MPT prefix-scan latency | NO | NO | **Emit** histogram |

## Phase 2 — Design

### Per-ordinal cardinality strategy

**Approach (A) — labelled metrics with retention.** A `snapshot_ordinal` Prometheus label on **a select subset of point-in-time gauges** (specifically:
finality-trigger fires, level-µ trial outcomes, committee gate admit
results). Time-series metrics (`dag_nakamoto_ordinal`, rates, lag,
production duration) stay un-ordinaled — they're already plotted on a
time axis and adding ordinal would only inflate cardinality.

Why (A) over (B) JSON datasource and (C) exemplars:

- (B) — a custom `/per-ordinal/{N}` JSON datasource queryable from Grafana
  would expose finer per-ordinal context (e.g., per-validator
  attestation timing for a single ordinal) without polluting Prometheus
  cardinality. **Strictly more powerful but out of scope** for this
  revamp — needs a new HTTP route, new Grafana datasource provisioning,
  and a way to expose ordinal-keyed in-memory state from `TowerStore` /
  `SnowballAccumulator` / `MetagraphAttestationAggregator`. Worth doing
  in a follow-up if cross-node per-ordinal comparison becomes a daily
  workflow.
- (C) — Prometheus **exemplars** (one sample value attached to a
  histogram bucket as a trace-id-tagged exemplar) are great for a
  histogram-of-latencies that exposes the per-ordinal trace, but the
  Tessellation stack has no OpenTelemetry trace bus today and most
  Nakamoto gauges aren't histograms. Exemplars are an enhancement for a
  trace-bus-having stack; do not adopt now.

### Cardinality retention pattern

A naive `Counter` keyed on `snapshot_ordinal` keeps every label-set alive
forever in the registry — at 1 Hz × 10000 ordinals/3h that's a
million-series memory leak. To bound this we **only emit per-ordinal-labelled
metrics at the snapshot-finalize site, not the snapshot-produce/-receive
site**, and the dashboard exposes a `last-N-ordinals` template variable
that filters at query time. The underlying Prometheus series count is bounded
by the Prometheus storage retention (`--storage.tsdb.retention.time`,
default 15d) — and downstream we MAY add a Prometheus `metric_relabel_configs`
rule to drop the `snapshot_ordinal` label after age-N if needed.

### Grafana dashboard structure

1. **Overview panel set** (the existing `Nakamoto Consensus` dashboard, expanded with new sections for the missed features).
2. **Per-feature drill-down panels** — a second dashboard
   (`Nakamoto Per-Ordinal`) with a `$snapshot_ordinal` template variable
   sourced from `label_values(dag_nakamoto_finality_triggers_fired_t_count_per_ordinal_total, snapshot_ordinal)`.
   Cross-node per-ordinal comparison comes for free because the existing
   `node` label discriminates per-validator state at a fixed
   `snapshot_ordinal`.
3. **Snapshot ordinal selector template variable.** Single-pick or
   multi-pick; pinned to the per-ordinal dashboard only.

This revamp **adds** the missing overview-panel coverage in iteration 1 and
**adds** a per-ordinal subset of metrics for the highest-value features.
The per-ordinal dashboard itself is a follow-up that builds on iteration 1's
metric emission.

## Phase 3 — Implementation scope

### Landed in this iteration

1. **`TowerFinalizer` emits per-super-level pass + per-ordinal counts.**
   New emitters at `TowerFinalizer.finalize`:
   - `dag_nakamoto_nipopow_level_trial_pass_total{level}` — counts per-level
     passes
   - `dag_nakamoto_nipopow_level_trial_total{level}` — counts per-level
     attempts (denominator for pass-rate)
   - `dag_nakamoto_nipopow_levels_passed` — gauge of passes per finalize
     (cluster-wide rate-of-pass observable)

2. **`NodeStakeAggregator.aggregateFromMpt` records latency.** New emitter
   at `NodeStakeAggregator.make`:
   - `dag_nakamoto_stake_aggregator_mpt_scan_ms` — distribution of prefix-scan wall-time

3. **`MetagraphCommitteeGate.attestAndAdmit` records sortition admit outcome.**
   New emitters at `MetagraphCommitteeGate.attestAndAdmit`:
   - `dag_nakamoto_committee_admit_total{outcome}` — `accepted` / `timeout`
   - `dag_nakamoto_committee_sortition_self_outcome_total{outcome}` — `in_committee` / `not_in_committee` / `unknown_parent`

4. **Per-ordinal finality-trigger fires.** Add an `snapshot_ordinal` label to
   the existing `_t_count_total`, `_t_depth1_total`, `_t_weight_total` counters
   in the form of three new `*_per_ordinal_total` counters that are tagged with
   `snapshot_ordinal`. **The original counters (no `snapshot_ordinal` label)
   stay** — existing alerts continue to reference them.

5. **Dashboard overhaul.** `nakamoto-test/grafana/dashboards/nakamoto-consensus.json`
   gets six new sections:
   - KES Rotation & Verification
   - Avalanche / Snowball
   - Committee Sortition & Gate
   - NIPoPoW Level-µ Trials
   - Finality Triggers (with chain-quality overlay)
   - Stake Aggregator Latency

### Deferred (follow-ups)

- Tower density gauge vs §5.3 5% bound — requires plumbing
  `TowerVerifier.densityCheck` results into a daemon that periodically
  emits.
- Per-validator VRF threshold gauge — requires
  `EligibilityChecker.threshold` to be observable per-slot. Cardinality
  bound: per-node × per-slot = unbounded on long runs. Better as an
  ad-hoc HTTP endpoint with sampling.
- Genesis stateProof consistency gauge — boot-time only; the existing
  `L0GenesisStateProofConsistencySuite` test gives stronger guarantees
  than a runtime gauge would.
- The full Per-Ordinal dashboard (with `$snapshot_ordinal` template
  variable) — builds on this iteration's emission. The new label is
  already wired so the follow-up is a JSON-only change.
- A custom `/per-ordinal/{N}` HTTP route with JSON datasource (option B).
  Highest-value follow-up if cross-node per-ordinal forensics becomes
  routine.

## Validation strategy

- `sbt 'shared/Test/compile; nodeShared/Test/compile; dagL0/Test/compile'` —
  must compile cleanly.
- `jq empty docker/grafana/.../*.json && jq empty nakamoto-test/.../*.json`
  — dashboards must parse.
- No e2e in this iteration.

## Risk register

- **Cardinality blow-up risk:** Bounded by emitting per-ordinal counters
  only at finalize (T_count + T_depth1 + T_weight fire-points — each
  ordinal fires each kind at most once). Total per-ordinal series count
  ≤ `3 (kinds) × cluster-size` per ordinal. Prometheus retention bounds
  the series lifetime.
- **Existing-alert risk:** All NEW emitters use NEW metric names. No
  rename, no label change to existing counters. Existing alerts and
  panels continue to work.
- **JAR-cache risk:** All emitter changes ship in JAR-compiled code paths
  (no resource-only edits). `just nuke` is not required.
