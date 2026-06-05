# Epoch-Anchored Participating Set + Participation Ratio + Demotion — Design

**Status:** DESIGN (no consensus code written). Date: 2026-06-02.
**Motivation:** replace the ad-hoc runtime "active" set with a deterministic, epoch-anchored
**participating set**, add a **participation ratio**, and a **demotion** mechanism (sustained poor
behavior / slashing) — fixing both live-finality determinism and offline cert verifiability, and
delivering the first half of the long-deferred slashing **consequence** path.

## 1. The problem (verified in code)

Today "active" is an in-memory `Ref[Set[PeerId]]` (`StakeRegistry.scala:116`):
- `markActive(peerId)` adds a seedlist validator — called from **exactly one** site, `TipTracker:197`,
  on attestation receipt.
- `markInactive(peerId)` removes — **never called anywhere** (dead code).
- ⇒ "active" = *validators observed attesting ≥once since this node booted*; **only grows, never
  shrinks, resets on restart, per-node**. The doc comments ("attested *recently*", "`markInactive`
  on timeout/disconnect") describe semantics that **do not exist**.

`TipTracker.attestationWeight` finalizes at ≥2/3 of `optimisticRelativeStake`, which is computed
against this set. Consequences: (a) it **saturates** toward the full seedlist in steady state (so
the "optimistic" optimization mostly bites only during warmup); (b) a once-seen-then-offline peer
stays in the denominator forever; (c) it is **non-deterministic across nodes and non-reproducible
offline** — so the live finality basis isn't cleanly deterministic, and the S5 light-client cert
verify cannot reproduce it (it uses an interim `relativeStakeAt` = 2/3-of-total stand-in).

## 2. State of slashing (verified) — detection-only, no consequence

| Piece | Exists? | Wired? |
|---|---|---|
| Equivocation evidence + validator (Family A `SlashableEvidence`; Family B shard-checkpoint) | ✅ types+validators+tests, in HEAD | ❌ no consumer; no L0-tx/event ADT admits them |
| Non-participation accumulator (Slice 17): `ShardNonParticipationCounter`, `ShardNonParticipationStateManager` (record* + `materializeAllForEpoch`), MPT partition **fieldId 24**, `ShardNonParticipationSlasher.evaluateEpochBoundary → List[PeerId]`, `ShardSlashingConfig` | ✅ built+tested, in HEAD | ❌ **zero production call sites** (counters never written, slash-list never read) |
| Consequence: stake penalty / eviction / validator-set or participating-set mutation / `slashedRegistry` | ❌ does not exist | — |

`updateValidators` is called **once at boot** (`GlobalSnapshotConsensus.scala:793`), sourced from the
**seedlist** (minus `metagraph-op` aliases); validator *membership* is static post-genesis (only
stake *weight* evolves). `SLASHING-DESIGN §8` / `HIERARCHICAL-SHARD-CHECKPOINTS §10` mark the
consequence (§5 ledger-effect) as the deferred half. **This refactor builds that missing sink.**

## 3. Design

### 3.1 Core idea — demotion = exclusion from the epoch participating set, NOT seedlist mutation

Do **not** mutate the seedlist-sourced validator set (that path is unimplemented and membership is a
seedlist gate). **Forward-constraint (user, 2026-06-02):** the seedlist gate is *temporary* —
seedlist validators are slated for removal, kept only to bootstrap genesis of the *next* chain
iteration; validator membership will become stake/on-chain-driven. So read membership through the
**validator-set abstraction**, never the seedlist directly, so this design survives seedlist removal.
Instead, per eta-period, record a **participating set** ⊆ validator set. "Demoted" =
*in the validator set but excluded from this epoch's participating set*. Consensus weight, finality
quorum, and (optionally) committee eligibility read the **participating set**, not the raw seedlist.
This makes demotion epoch-anchored, deterministic, reproducible, and reversible (re-promotion) —
without touching the boot-only `updateValidators`.

### 3.2 Measure within an epoch — extend Slice 17

Per-`(peerId, epoch)` participation counters (generalize Slice-17's per-shard counters by dropping
the `shardId` dimension for the gl0-global set): slots-eligible-as-leader vs missed, attestation
windows total vs missed, checkpoints/tips seen. **The integration gap is invocation** — the Slice-17
`record*` writers have no callers. Wire them from the same paths that drive consensus today: the
election/eligibility path (`recordSlotEligibility`) and the attestation/tip-accept path (the site
that currently calls the ad-hoc `markActive`, `TipTracker:197`).

### 3.3 Record at the eta boundary — extend the `HistoricalStakeSnapshot` pattern

The exact template exists: `GlobalSnapshotAcceptanceManager.computeHistoricalStakeBoundaryDelta`
(`:954`, boundary predicate `ord % R == R-1`, `R = etaRotationSnapshots` default 2550) builds
`HistoricalStakeSnapshot{stakes, eta}` with **last-3-period retention** and emits an MPT delta via
`AcceptanceMptStateChanges` (fieldId 20). Two options, lowest-friction first:
1. **Fold into `HistoricalStakeSnapshot`** → `{stakes, eta, participating}` (a `Set[PeerId]` or a
   `SortedMap[PeerId, ParticipationRatio]`), computed in the *same* boundary function from the §3.2
   counters via a generalized `ShardNonParticipationSlasher.shouldSlash` predicate. The boundary
   hook, retention, MPT-delta plumbing, and GSI↔MPT parity test already exist.
2. Or a new `GlobalStateFieldId` (next free int = **25**) + `participatingSetKey[F](period)` mirroring
   `historicalStakeSnapshotsKey`, written in the same boundary branch.

The **participation ratio** is integer-arithmetic from the counters (e.g. `1 - missed/total`),
config-thresholded (`ShardSlashingConfig.maxMissedPctPerEpoch`, `minDenominatorPerEpoch`) — fully
deterministic.

### 3.4 Read deterministically — extend the `relativeStakeAt` pattern

Add `participatingSetAt(period)` / `participationRatioAt(peerId, period)` to `StakeRegistry`, backed
by `HistoricalStakeReader.lookup(period)` (mirroring `relativeStakeAt`, `StakeRegistry.scala:338/482`).
This is the deterministic, MPT-backed, **offline-reproducible** read.

### 3.5 Demotion over a series of epochs

- **Decision** (exists, generalize): `ShardNonParticipationSlasher.evaluateEpochBoundary(closedEpoch)
  → List[PeerId]` — deterministic, sorted, integer-arithmetic, config-thresholded. Generalize to the
  gl0-global set. Demote on **sustained** non-participation across K consecutive epochs (hysteresis,
  to survive transient partitions), and immediately on **accepted equivocation evidence** (Family A/B).
- **Effect** (the missing sink): the boundary writes the demoted peers *out* of the next period's
  participating set (§3.3). **Re-promotion** on sustained participation (symmetric hysteresis).
- **One consequence sink for both inputs:** non-participation (Slice 17) *and* accepted equivocation
  evidence feed the same epoch-boundary participating-set update. This is the first half of
  `SLASHING-DESIGN §5`; the stake-reduction / bounty / burn effects compose on top later.

### 3.6 Consumers swap to the participating set

- **Live finality:** `TipTracker` reads the epoch participating set / `participationRatioAt` instead
  of the ad-hoc `activeRef`; `markActive`/`markInactive` and the `activeRef` are deleted.
- **S5 cert verify:** `LightClientCertVerifier` swaps its interim `relativeStakeAt` quorum re-check to
  `participatingSetAt`/`participationRatioAt` — now the cert verifies against the *same* deterministic
  basis the network finalized on (closing the S5 interim-vs-canonical gap). Verify already takes
  `stakeRegistry` as input, so this is a registry-side change.
- **Committee sortition** (optional): may read participation to weight/gate.

## 4. Extend vs net-new

- **Extend (already in HEAD):** the eta-boundary hook + retention + MPT-delta plumbing + parity test;
  `HistoricalStakeSnapshot` + `HistoricalStakeReader` + `relativeStakeAt`; Slice-17 counter schema +
  state manager + `evaluateEpochBoundary` + `ShardSlashingConfig`; the `GlobalStateFieldId` pattern.
- **Net-new:** invocation of the participation counters from the gl0 election/attestation paths; the
  gl0-global (vs shard-scoped) counter; the persisted per-period participating/demoted set; the
  `participatingSetAt`/`participationRatioAt` reads; **the consequence sink** that excludes demoted
  peers from consensus weight (shared with slashing).

## 5. Slicing (incremental, each independently testable)

1. **Record** the epoch participating set at the boundary (extend `HistoricalStakeSnapshot`) + the
   deterministic `participatingSetAt` read. (No behavior change; observability.)
2. **Wire** the Slice-17 participation counters from the election + attestation paths.
3. **Swap** `TipTracker` finality + the S5 cert verify to read the participating set; delete the
   ad-hoc `activeRef`/`markActive`/`markInactive`.
4. **Demote:** generalize `evaluateEpochBoundary`; exclusion effect at the boundary; hysteresis +
   re-promotion.
5. **Slashing consequence:** feed accepted equivocation evidence into the same sink; then the §5
   stake-reduction/bounty/burn effects.

## 6. Safety / open questions

- **Honest-node protection:** demotion must require *sustained* (K-epoch) non-participation with a
  minimum denominator, so a transient partition or a slow node isn't demoted — `ShardSlashingConfig`
  already has `minDenominatorPerEpoch`; add the K-epoch hysteresis. Re-promotion symmetric.
- **Determinism (hard requirement):** all counters + thresholds integer-arithmetic, config-pinned;
  the participating set is computed identically on every node at the boundary (GSI↔MPT parity test
  extended). This is what makes it reproducible offline and consensus-safe.
- **Consensus-criticality:** the participating set gates *who can finalize* — same safety bar as
  slashing (evidence-based, deterministic, adapt standard designs; see `feedback_slashing_safety_bar`).
- **Liveness vs the old optimistic behavior:** the old `optimisticRelativeStake` let a warmed-up
  cluster finalize with fewer online peers; the participating set must preserve a participation-ratio
  gate (analogous to `MinActiveQuorumFraction`) so finality doesn't stall when the participating set
  is small — but now measured per-epoch and deterministically.
- **Bootstrap:** genesis / first eta-periods need a defined participating set (mirror the
  `relativeStakeAt` genesis fallback: periods ≤ 1 → full genesis validator set).
- Staging is consensus-critical and must be e2e-validated at target topology before relying on it.

## 7. Key references
- `StakeRegistry.scala` (`:116` activeRef, `:146` updateValidators, `:149` markActive, `:155` dead
  markInactive, `:338/:482` relativeStakeAt)
- `TipTracker.scala:197` (sole markActive call), `:207` attestationWeight, `:131` FinalityThreshold
- `GlobalSnapshotConsensus.scala:790-793` (boot-only validator-set source = seedlist)
- `GlobalSnapshotAcceptanceManager.scala:954-1019` (eta-boundary recording hook)
- `AcceptanceMptStateChanges.scala` (boundary MPT write)
- `ShardNonParticipationStateManager.scala` (Slice-17 counters + Slasher — unwired)
- `StakeDistribution.scala` (`HistoricalStakeSnapshot`, `EtaPeriod`, `EpochStakeSnapshotter`)
- `GlobalStateKey.scala:249/:368` (fieldId 24 non-participation), `:280` (fieldId 20 historical stake)
- slashing: `slashing/{SlashableEvidence,ShardCheckpointEquivocation}Validator.scala`;
  `docs/nakamoto/SLASHING-DESIGN.md §5/§8`; `HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md §10`
- consumer to swap: `LightClientCertVerifier.scala` (S5 interim quorum basis)
