package io.constellationnetwork.node.shared.config

import cats.data.NonEmptySet
import cats.syntax.option._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}

import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.node.shared.domain.statechannel.FeeCalculatorConfig
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.node.{NodeState, RewardFraction}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.transaction.TransactionAmount
import io.constellationnetwork.schema.{NonNegFraction, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash

import com.comcast.ip4s.{Host, Port}
import eu.timepit.refined.types.numeric._
import fs2.io.file.Path

object types {

  case class FieldsAddedOrdinals(
    tessellation3Migration: Map[AppEnvironment, SnapshotOrdinal],
    tessellation301Migration: Map[AppEnvironment, SnapshotOrdinal],
    checkSyncGlobalSnapshotField: Map[AppEnvironment, SnapshotOrdinal],
    metagraphSyncData: Map[AppEnvironment, SnapshotOrdinal],
    updatedLastSyncGlobalOrder: Map[AppEnvironment, SnapshotOrdinal],
    updatedLastSyncGlobalFromPeersInConsensus: Map[AppEnvironment, SnapshotOrdinal],
    updatingCombineFunctionSpendActions: Map[AppEnvironment, SnapshotOrdinal],
    fixingAllowSpendExpiration: Map[AppEnvironment, SnapshotOrdinal],
    fixingAllowSpendAndTokenLockValidation: Map[AppEnvironment, SnapshotOrdinal],
    setSumFix: Map[AppEnvironment, SnapshotOrdinal]
  )

  /** Typed predicate over per-environment migration gates. Resolves a `FieldsAddedOrdinals` field for the active `AppEnvironment` once at
    * construction, then collapses repeated `ordinal < <gate>StartingOrdinal` checks into named methods.
    */
  final case class Era(
    private val tessellation3: SnapshotOrdinal,
    private val tessellation301: SnapshotOrdinal,
    private val metagraphSync: SnapshotOrdinal
  ) {
    def atOrAfterTess3(ordinal: SnapshotOrdinal): Boolean = ordinal.value.value >= tessellation3.value.value
    def beforeTess3(ordinal: SnapshotOrdinal): Boolean = ordinal.value.value < tessellation3.value.value
    def atOrAfterTess301(ordinal: SnapshotOrdinal): Boolean = ordinal.value.value >= tessellation301.value.value
    def atOrAfterMetagraphSync(ordinal: SnapshotOrdinal): Boolean = ordinal.value.value >= metagraphSync.value.value

    /** Some(value) at-or-after tessellation3, None before — the shape behind the 12 nullable post-tess3 fields in the GSI.
      */
    def postTess3[A](ordinal: SnapshotOrdinal)(value: => A): Option[A] =
      if (atOrAfterTess3(ordinal)) Some(value) else None
    def postTess301[A](ordinal: SnapshotOrdinal)(value: => A): Option[A] =
      if (atOrAfterTess301(ordinal)) Some(value) else None
    def postMetagraphSync[A](ordinal: SnapshotOrdinal)(value: => A): Option[A] =
      if (atOrAfterMetagraphSync(ordinal)) Some(value) else None
  }

  object Era {
    def fromConfig(env: AppEnvironment, fieldsAddedOrdinals: FieldsAddedOrdinals): Era = Era(
      tessellation3 = fieldsAddedOrdinals.tessellation3Migration.getOrElse(env, SnapshotOrdinal.MinValue),
      tessellation301 = fieldsAddedOrdinals.tessellation301Migration.getOrElse(env, SnapshotOrdinal.MinValue),
      metagraphSync = fieldsAddedOrdinals.metagraphSyncData.getOrElse(env, SnapshotOrdinal.MinValue)
    )
  }

  case class MetagraphsSyncConfig(
    maxUnappliedGlobalChangeOrdinals: PosInt
  )

  /** Nakamoto parameter family. `confirmationDepthK` (k1) is the single free security knob; the related eta period and recommended local
    * retention capacity are derived from it rather than loaded independently, removing the previous misconfiguration where
    * `eta-rotation-snapshots` held k₂'s value (10·k₁) and `keep-depth-behind-finalized` held k₁'s.
    *
    *   - k1 = `confirmationDepthK(env)` — canonical-depth fallback for reversible exact-hash Phase 2. Loaded from HOCON per environment
    *     (`nakamoto.confirmation-depth-k` is a `{ mainnet, testnet, integrationnet, dev }` block, mirroring `last-kryo-hash-ordinal`) and
    *     resolved ONCE for the active `AppEnvironment` at the use site — mainnet 1024, test/integration nets 256, dev 32 (with the
    *     `${?NAKAMOTO_CONFIRMATION_DEPTH}` override applied to the dev value only). REUSED by the §3 NIPoPoW historical-commitment SMT as
    *     its finalized cutoff (`smtRoot(N)` commits ordinals i ≤ N − k₁), and by `SnapshotLeaderLoop` / `NakamotoSyncDaemon`.
    *   - R = `etaRotationSnapshots(env)` = round(3.1·k₁) — eta-rotation period. Ouroboros: the eta nonce uses the first 2/3 of the period's
    *     VRF rho values, so the last 1/3 = R/3 must be ≥ k₁ (those inputs FINALIZED before use) ⇒ R ≥ 3·k₁; the .1 over 3 is the worst-case
    *     finalization margin (the 2/3-mark must FINALIZE before the boundary; finalization lags production by ≤ k₁) ⇒ (R/3−k₁)=0.033·k₁.
    *   - k2 = `keepDepthBehindFinalized(env)` = 100*k1 — recommended local retention, proof-service, and automatic-rollback capacity. It is
    *     not a finality phase, validity threshold, or fork-choice floor
    *
    * Both derived depths are env-parameterized methods (NOT vals) so they resolve from the SAME per-env k₁ as the active environment;
    * resolve the env from the wrapping `SharedConfig.environment` and pass it once (see the call sites in `GlobalSnapshotConsensus` /
    * `CurrencyL0App` / `SharedServices`).
    *
    * Other `NAKAMOTO_*` tunables (the LDD knobs, slot duration) are likewise typed under `nakamoto.*` and resolved via
    * `SharedConfig.nakamoto`, not `sys.env`.
    */
  case class NakamotoConfig(
    // Confirmation depth k₁ — the single loaded consensus-depth knob, now PER-ENVIRONMENT (`nakamoto.confirmation-depth-k` block:
    // mainnet 1024 / testnet 256 / integrationnet 256 / dev 32, dev overridable via `${?NAKAMOTO_CONFIRMATION_DEPTH}`). Resolve for the
    // active env via the `confirmationDepthK(env)` accessor below — R and k₂ DERIVE from the resolved value (see the `def`s in the body).
    confirmationDepthKByEnv: Map[AppEnvironment, PosLong],
    // Slot duration in ms — the consensus time unit (§5.7: a PARAMETER, not a constant; 1000 prod / 500 fast-test).
    // Drives the GL0 SnapshotLeaderLoop slot tick and execution-shard staircase duty windows. Migrated from the
    // `NAKAMOTO_SLOT_DURATION_MS` sys.env read in SnapshotLeaderLoop (project rule: HOCON over scattered env reads).
    slotDurationMs: PosLong,
    // gl0 LDD snowplow — consensus-critical; read from HOCON as EXACT rationals (`baseline`/`amplitude` = `"n/d"`),
    // mapped 1:1 to `LddConfig` at the wiring site. No Double, no source-level default (config is authoritative).
    ldd: GlobalLddConfig,
    // Coordinated genesis time (epoch ms); 0 = unset -> local-clock fallback with WARN (solo dev only).
    genesisTimeMs: NonNegLong,
    genesisEtaSeed: String,
    // Snowball decision margin (beta) for the attestation accumulator.
    snowballBeta: PosInt,
    // #259 active-recovery: caps on the metagraph orphan buffer + recent-admission cache. Migrated from the
    // `NAKAMOTO_ORPHAN_BUFFER_CAP` / `NAKAMOTO_RECENT_ADMIT_CAP` env reads to typed HOCON (project rule: no scattered
    // sys.env). `recentAdmitCap` is kept proportionally larger (4×) — see `MetagraphOrphanBuffer.DefaultAdmissionsCap`.
    orphanBufferCap: PosInt,
    recentAdmitCap: PosInt,
    // Fork-recovery master switch for `RebootstrapOrchestrator`: a node that self-finalized a divergent branch and now
    // refuses canonical peers self-heals (pause production → reset chain store / overlay / tip-tracker → re-seed from
    // gossip). Migrated from the `NAKAMOTO_REBOOTSTRAP_ENABLED` env read to typed HOCON (project rule: no scattered
    // sys.env). Default TRUE: with it OFF a locked-out node forks the global mptRoot indefinitely (the sharded
    // data-app-fee reorg storm — gl0-4 self-finalized 857 while peers held 819-822). The refuse-counter trigger is
    // conservative (K sustained different-hash writes at-or-below finalized; no false-positive scenario for the gate).
    rebootstrapEnabled: Boolean,
    // TRANSITIONAL CONFIG-FLAG (`nakamoto.band-density-reorg-enabled`). false (default) = legacy k1-freeze fork choice: the
    // `NakamotoChainStore` store-gate + `ChainSelection.shouldSwitch` both key off the k₁ finalized marker (byte-identical to
    // the post-`376d09fbc` baseline — the 2026-06-27 storm backstop). true = re-key BOTH to the k₂ "settled" marker so forks
    // in the `(settled, finalized]` band are density-revertable (maxvalid-bg), AND enable the commutative true-MRCA density
    // comparator. Consensus-critical + cluster-uniform: EVERY node must run the same value (a split would fork the chain), so
    // this is a single global switch (NOT per-env). Neither branch is the locked target: objective maxvalid-bg must remain
    // available for P2 density reorgs, and unavailable retained history must enter RecoveryRequired rather than use a k1/k2 floor.
    bandDensityReorgEnabled: Boolean,
    // ml0 gl0-follow changeset transport (task #12). `changesetRingDepth` bounds the gl0 producer's SERVED ring of
    // recent finalized per-ordinal accumulators (`GlobalChangeSetService` / `SnapshotLeaderLoop.ringInsertTrimmed`) —
    // a follower more than this many finalized ordinals behind falls back to a heavy full-GSI resync, so widening it
    // cuts the "baseOrdinal=None" resync class at the cost of memory (each accumulator can be sizeable; ~1024 × the
    // per-ordinal changeset). `stagingAccumulatorsCap` bounds the producer's hash-keyed STAGING map of accumulators
    // awaiting finalization (`GlobalSnapshotConsensusFunctions`); the steady-state bound is the finalized-watermark
    // prune, so this size cap is only a backstop for a burst of never-finalizing forks between two finalize ticks and
    // is kept comfortably above `changesetRingDepth` (2×). Pure transport memory bounds — NOT consensus parameters.
    changesetRingDepth: PosInt,
    stagingAccumulatorsCap: PosInt,
    // Signed-byte-store read-time BACKFILL (2026-07-09) — heals HOLES in gl0's contiguous signed byte store
    // (`mpt_snapshot_info_signed`) at read time: when a pinned execution-base read misses locally but the snapshot at that ordinal
    // IS locally finalized, pull the signed byte map from up to `pinnedBackfillMaxPeers` peers (per-try `pinnedBackfillPerPeerTimeout`
    // so a hung peer can't stall the accept fold), verify `sidecarFreeMptRoot === the LOCAL committed stateProof.mptRoot`, and stage.
    // Transport tunables ONLY — the verification is unconditional and a total fetch failure stays fail-closed (defer), so these are
    // NOT consensus parameters. `pinned-backfill-max-peers = 0` disables the transport (kill switch).
    pinnedBackfillMaxPeers: NonNegInt = NonNegInt.unsafeFrom(3),
    pinnedBackfillPerPeerTimeout: FiniteDuration = 5.seconds,
    commitmentSmt: CommitmentSmtConfig,
    localEvents: LocalEventsConfig,
    committee: CommitteeConfig,
    sharding: ShardingConfig,
    invaliditySlashing: InvalidStateProofSlashingConfig
  ) {
    // k₁ for the active environment. Resolved once at the use site from `SharedConfig.environment`. Falls back to the dev value
    // (`NakamotoConfig.DefaultConfirmationDepthK`, 32) for any env absent from the HOCON block — same neutral-default convention as
    // `Era.fromConfig` (which uses `SnapshotOrdinal.MinValue`). All four standard envs are always present in `application.conf`, so the
    // fallback only fires under a hand-trimmed config; 32 keeps a node finalizing rather than failing at boot.
    def confirmationDepthK(env: AppEnvironment): PosLong =
      confirmationDepthKByEnv.getOrElse(env, NakamotoConfig.DefaultConfirmationDepthK)
    // R = 3.1·k₁ (Ouroboros: eta uses the first 2/3 of the period; last 1/3 = R/3 ≥ k₁ so those inputs FINALIZE before use ⇒ R ≥ 3·k₁).
    // The .1 over 3 (was .03) is the worst-case finalization margin: the 2/3-mark must FINALIZE before the boundary consumes the eta,
    // and the depth fallback can lag production by k1 even when optimistic Phase 2 does not ⇒ margin (R/3 - k1) = 0.033*k1.
    // Derived from the per-env k₁, NOT loaded — keeps the eta-rotation period in lockstep with the confirmation depth.
    def etaRotationSnapshots(env: AppEnvironment): PosLong =
      PosLong.unsafeFrom(math.round(3.1d * confirmationDepthK(env).value))
    // k2 = 100*k1 recommended local retention/proof/recovery capacity. It does not create Phase 3 or an
    // absolute fork-choice floor. Derived from per-environment k1 and threaded through legacy `archivalDepthK`
    // parameter names until those Phase-3-era symbols are removed.
    def keepDepthBehindFinalized(env: AppEnvironment): PosLong =
      PosLong.unsafeFrom(100L * confirmationDepthK(env).value)
    // Track-3 S3 fork-choice lookback = k₁ + 1. Forks shallower than this are resolved by the tip
    // tiebreak (maxvalid-tk, longest-chain); deeper forks switch to the density rule (maxvalid-bg). At
    // k1 + 1 the density rule engages after the shallow-fork window. Derived from per-environment k1 —
    // replaces the hardcoded `ChainSelection.DefaultKLookback` (50) at the production wiring.
    def kLookback(env: AppEnvironment): Long = confirmationDepthK(env).value + 1L
    // Track-3 S3 density window = round(R / 3), R = etaRotationSnapshots (= round(3.1·k₁)), so
    // sWindow ≈ 1.033·k₁ slots. The density comparison counts blocks within this many slots of the true
    // fork point — one confirmation-depth's worth of chain growth, matching maxvalid-bg's s-parameter.
    // Derived from the per-env R, NOT loaded — replaces the hardcoded `ChainSelection.DefaultSWindow` (200).
    def sWindow(env: AppEnvironment): Long = math.round(etaRotationSnapshots(env).value.toDouble / 3.0d)
  }

  /** HOCON shape for the gl0 LDD snowplow (see [[NakamotoConfig.ldd]]). `baseline` (fB) and `amplitude` (fA) are EXACT rationals read
    * straight from `"n/d"` strings via the `Ratio` ConfigReader — no `Double`, no embedded defaults (config is authoritative).
    */
  case class GlobalLddConfig(
    cutoff: Int,
    offset: Int,
    baseline: Ratio,
    amplitude: Ratio
  )

  object NakamotoConfig {
    // Neutral fallback for `confirmationDepthK(env)` when an environment is missing from the HOCON block — the dev default (32).
    val DefaultConfirmationDepthK: PosLong = PosLong.unsafeFrom(32L)

    // The `confirmationDepthKByEnv` field reads from the HOCON key `confirmation-depth-k` (the per-env block), NOT the
    // default kebab-cased `confirmation-depth-k-by-env`. Use an explicit `ProductHint` field override;
    // every OTHER field falls through to pureconfig's default `CamelCase` → `KebabCase` so they keep their existing keys.
    implicit val configHint: _root_.pureconfig.generic.ProductHint[NakamotoConfig] =
      _root_.pureconfig.generic.ProductHint[NakamotoConfig](_root_.pureconfig.ConfigFieldMapping {
        case "confirmationDepthKByEnv" => "confirmation-depth-k"
        case other => _root_.pureconfig.ConfigFieldMapping(_root_.pureconfig.CamelCase, _root_.pureconfig.KebabCase)(other)
      })
  }

  /** Committee draw/quorum decouple — the two cluster-uniform knobs shared by the per-metagraph admission committee and the execution-shard
    * committee. Both are GL0-operator committees, but admission uses a true secret-key VRF per binary while shard membership uses a public,
    * enumerable VK-hash draw per `(shard, eta period)`. Both currently use uniform `1/N` weight.
    *
    *   - `kDraw` — committee DRAW target. Used in `CommitteeSortition.threshold(kDraw, σ) = min(kDraw·σ, 1)`, so the expected committee
    *     size is `≈ kDraw·σ·N`. With uniform σ = 1/N this is `≈ kDraw`; setting `kDraw = N` makes `kDraw·σ = 1` saturate ⇒ committee =
    *     everyone.
    *   - `kQuorum` — admit quorum. The number of distinct committee attestations the metagraph gate waits for
    *     (`MetagraphAttestationAggregator.thresholdReached`) and the mandatory shard execution-certificate count
    *     (`ShardFinalityTriggers.tCountShard`). GL0 also re-executes every included CL1 transition; the count alone is never sufficient.
    *
    * '''Invariant (validated fail-fast at config load via [[validated]]): `0 < kQuorum <= kDraw`.''' Decoupling the two fixes the
    * throughput lag where expected-committee == admit-quorum: a binomial committee draw around `kDraw` left ~36% of binaries with a
    * committee SMALLER than the quorum, which could never reach it → committee-gate timeout → re-buffer churn → gl0 admits metagraph
    * binaries slower than ml0 produces. For liveness you want `kDraw` large enough that `P(|committee| ≥ kQuorum) ≈ 1` — e.g. `kDraw ≥
    * ~1.5·kQuorum`, or `kDraw = N` (committee = everyone, threshold saturates at 1). Testnet default `kDraw = 8, kQuorum = 6` (N = 8):
    * `8·(1/8) = 1` saturates ⇒ committee = all 8 ⇒ `P(8 ≥ 6) = 1`; admit at 2/3 of N.
    */
  case class CommitteeConfig(kDraw: Int, kQuorum: Int) {

    /** Fail-fast invariant check, run once at startup wiring. Returns `this` on success; raises `IllegalArgumentException` with a clear,
      * operator-actionable message on `kQuorum <= 0`, `kDraw <= 0`, or `kQuorum > kDraw` (the cluster-split footgun: a quorum larger than
      * the draw can never be met by a committee the draw produces).
      */
    def validated: CommitteeConfig = {
      require(kDraw > 0, s"nakamoto.committee.k-draw must be positive, got $kDraw")
      require(kQuorum > 0, s"nakamoto.committee.k-quorum must be positive, got $kQuorum")
      require(
        kQuorum <= kDraw,
        s"nakamoto.committee invariant violated: k-quorum ($kQuorum) must be <= k-draw ($kDraw) — " +
          s"a quorum larger than the draw target can never be reached by the drawn committee (would stall metagraph/shard admission)"
      )
      this
    }
  }

  /** WATCHTOWER invalid-state-proof slashing — the 100% `InvalidStateProof` tier (`docs/nakamoto/SLASHING-DESIGN.md` §6;
    * `docs/nakamoto/WATCHTOWER-FRAUD-PROOF-DESIGN.md`). This is the evidence-based total-loss tier for a committee that signed a checkpoint
    * with a wrong per-MG derivation.
    *
    *   - `watchtowerEnabled`: master switch for the watchtower approval-check (the per-checkpoint re-execution + fraud-proof gossip).
    *     Default `true` at `numShards > 1`; INERT at `numShards = 1` (no committee checkpoints exist). Turning it off disables fraud-proof
    *     emission (the dispute consumer + slash still run if an envelope arrives, but no node produces one).
    *   - `slashFraction`: exact `Ratio` — fraction of the offender's combined (delegated + collateral) stake destroyed. Production `"1/1"`
    *     (total loss — the `InvalidStateProof` tier is the maximum severity; a single proven wrong derivation = total loss). A value `< 1`
    *     reduces delegated stake proportionally and still fully removes collateral (collateral has no partial-amount slot).
    *   - `bountyFraction`: exact `Ratio` — fraction of the slashed pool credited to the fraud-proof submitter; the remainder burns.
    *     Production `"1/20"` (5%) — enough to incentivise running a watchtower, not enough for a self-attacker to recover via
    *     self-submission (95% burns).
    *   - `cooldownEpochs`: epochs the slashed operator is excluded from the active set (cannot rejoin a committee / contribute to quorum).
    *     Default `100`, matching the equivocation cooldown in `SLASHING-DESIGN.md` §6.
    *   - `fraudProofPublishAttempts` / `fraudProofPublishRetryDelay`: bounded retry for the watchtower emitter's fraud-proof publish over
    *     the LOCAL sidecar gRPC hop (EPIC-9-NET M4 "retry-or-outbox"). A fraud proof is slashing evidence — a single warn-and-drop on a
    *     transient sidecar restart silently disarmed the tooth. Once the RPC lands, the sidecar's durable outbox owns network delivery.
    *     Node-local QoS knobs (NOT consensus-critical — divergent values cannot split the cluster).
    *
    * '''Current challenge-window gap.''' Live GSAM limits disputes by `confirmationDepthK`, but target k1 only makes an exact hash Phase-2
    * operational; it does not make checkpoint economics irreversible or prevent a later density reorg. Positive deterministic watchtower
    * replay coverage is required before checkpoint-derived value is inclusion-eligible. Later fraud evidence remains a collusion backstop,
    * and any retained resource bound must defer without slash when exact inputs/base are unavailable rather than treating k1 as a safety
    * floor.
    */
  case class InvalidStateProofSlashingConfig(
    watchtowerEnabled: Boolean,
    // slashFraction / bountyFraction are EXACT `Ratio` (HOCON `"n/d"` → Ratio via ConfigReader, never Double): the slash reduces
    // stake by these fractions and the result seeds the global mptRoot, so the arithmetic MUST be exact + byte-identical cluster-wide.
    slashFraction: Ratio,
    bountyFraction: Ratio,
    cooldownEpochs: Long,
    // Defaults keep the many existing construction sites source-compatible; production values come from `application.conf`.
    fraudProofPublishAttempts: Int = 3,
    fraudProofPublishRetryDelay: FiniteDuration = 2.seconds
  )

  /** §3 NIPoPoW historical-commitment SMT tunables. The tree is unbounded; `versionRootRetention` bounds only how many recent historical
    * ROOTS stay queryable for past-ordinal inclusion proofs (separate from the `confirmationDepthK` finalized lag). Must be >= 1.
    */
  case class CommitmentSmtConfig(
    versionRootRetention: PosInt
  )

  /** Hierarchical-shard-checkpoints v1 typed config shape (see `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §4.2). Defaults
    * collapse to the degenerate single-shard case (`numShards = 1`) so this block is a no-op until other slices consume it.
    *
    *   - `numShards`: cluster-wide static shard count. Metagraph → shard is deterministic via `Hasher.hash(metagraphAddress) mod
    *     numShards`. Default `1` ⇒ every metagraph maps to shard 0.
    *   - `retention`: bounded in-memory checkpoint history. It is not a finality or checkpoint-qualification threshold.
    *   - `checkpoint`: emission cadence + burst cap for shard checkpoints (Option C per `SHARD-CHECKPOINT-GRANULARITY.md`).
    *
    * NOTE: the shard committee DRAW target + ADMIT quorum are NOT here — they are the cluster-wide [[CommitteeConfig]] (`kDraw` /
    * `kQuorum`), shared with the per-metagraph committee gate, threaded into the shard wiring from `nakamoto.committee`. (Previously this
    * block carried a single overloaded `committeeKTarget` that conflated draw and quorum.)
    */
  case class ShardingConfig(
    numShards: Int,
    retention: ShardCheckpointRetentionConfig,
    checkpoint: ShardCheckpointConfig
  )

  /** Bounded shard-chain storage. Checkpoint retention never qualifies an execution checkpoint and is not a fork-choice rule. */
  case class ShardCheckpointRetentionConfig(retainedCheckpoints: Long)

  case class ShardCheckpointConfig(
    binaryBufferCap: Int,
    /** Shuffled-staircase proposal window width in slots (design §5.7 rev 2, owner 2026-06-12; default 5). Per shard ordinal the committee
      * is hash-sorted under the epoch eta; rank r proposes for this many slots, wrapping modulo committee size. Replaces the per-slot LDD
      * lottery (run-13/14: genesis forks + same-ord sibling lineages split attestations below kQuorum at ANY density).
      */
    staircaseDeltaSlots: Int = 5,
    /** Boot grace (run-17): no shard duty until Ready for this many slots — late-booting ranks must drain intake before taking (especially
      * genesis) duty, or they seed rival lineages while blind.
      */
    bootGraceSlots: Int = 30,
    /** Loss-recovery re-publish cadence (task #45, run-19). A node mints each (shardId, shardOrdinal) at most ONCE and HOLDS it; until that
      * ordinal is adopted or the tip moves out from under it (anchor-reorg), it re-gossips the SAME bytes every this-many produce ticks
      * instead of re-minting with a fresh gl0Anchor+slot. Re-minting churned the checkpoint hash every tick (run-19: 34 variants for one
      * ordinal) and split attestations below kQuorum. ~6 ticks ≈ confirmation RTT (mirrors the run-12 §5.8 sender cadence-gate).
      */
    republishEveryTicks: Int = 6,
    /** Chain-sync recovery (run-20, task #A). A shard checkpoint missed at boot / dropped by gossip cannot be healed by re-gossip —
      * GossipSub dedups Tier-1's identical re-publish bytes by msgid until the seen-cache TTL expires (minutes). So a node PULLS the
      * missing checkpoint from a peer over the p2p port (served read-only from the peer's ShardChainStore). `stuckMs` = how long the
      * per-shard tip may stall before the absence tick pulls `tip+1` (an EMPTY store ⇒ pull ordinal 1, the genesis-miss case);
      * `absenceTickIntervalMs` = the absence-detection cadence; `pullDedupCooldownMs` suppresses re-requesting the same `(shard, key)`.
      */
    stuckMs: Long = 5000,
    absenceTickIntervalMs: Long = 2000,
    pullDedupCooldownMs: Long = 30000
  )

  /** Configuration for the gl0-embedded `LocalEvents` reactive event stream (see `docs/nakamoto/LOCAL-EVENTS-SERVICE-DESIGN.md`). Drives
    * the gRPC server that publishes consensus events to local subscribers (e2e tests, operator GUI).
    *
    *   - `enabled`: master switch. Default OFF in production; e2e flips to true via HOCON or `${?NAKAMOTO_LOCAL_EVENTS_ENABLED}` env
    *     substitution. When false the gRPC server is not started and `LocalEventsPublisher.noop` is wired into GSAM.
    *   - `bindAddress`: default loopback. Docker e2e flips to `0.0.0.0` so the host reaches the container.
    *   - `port`: defaults to 50054 (distinct from `ChainSyncInbound`'s 50053).
    *   - `maxQueuedPerSubscriber`: per-subscriber queue depth before drop-oldest; ~15 min buffer at 8 events/ord × 7s/ord.
    *   - `publisherBufferSize`: process-wide FS2 `Topic` backstop. Should never be reached under normal load.
    *   - `shutdownGraceSeconds`: shutdown grace period matching the existing `ChainSyncInbound` 5-second grace.
    */
  case class LocalEventsConfig(
    enabled: Boolean,
    bindAddress: String,
    port: PosInt,
    maxQueuedPerSubscriber: PosInt,
    publisherBufferSize: PosInt,
    shutdownGraceSeconds: PosInt
  )

  case class SharedConfigReader(
    gossip: GossipConfig,
    leavingDelay: FiniteDuration,
    stateAfterJoining: NodeState,
    collateral: Option[CollateralConfig],
    snapshot: SharedSnapshotConfig,
    feeConfigs: Map[AppEnvironment, Map[SnapshotOrdinal, FeeCalculatorConfig]],
    forkInfoStorage: ForkInfoStorageConfig,
    priorityPeerIds: Map[AppEnvironment, NonEmptySet[PeerId]],
    lastKryoHashOrdinal: Map[AppEnvironment, SnapshotOrdinal],
    lastLegacyStateProofOrdinal: Map[AppEnvironment, SnapshotOrdinal],
    incrementalDelegatedStakingStartingOrdinal: Map[AppEnvironment, SnapshotOrdinal],
    addresses: AddressesConfig,
    allowSpends: AllowSpendsConfig,
    tokenLocks: TokenLocksConfig,
    lastGlobalSnapshotsSync: LastGlobalSnapshotsSyncConfig,
    validationErrorStorage: ValidationErrorStorageConfig,
    delegatedStaking: DelegatedStakingConfig,
    fieldsAddedOrdinals: FieldsAddedOrdinals,
    metagraphsSync: MetagraphsSyncConfig,
    priceOracle: Map[AppEnvironment, PriceOracleConfig],
    snapshotBinarySenderTimeouts: SnapshotBinarySenderTimeoutsConfig,
    clickHouseConfig: ClickHouseAppConfig,
    nakamoto: NakamotoConfig
  )

  case class SharedConfig(
    environment: AppEnvironment,
    gossip: GossipConfig,
    http: HttpConfig,
    leavingDelay: FiniteDuration,
    stateAfterJoining: NodeState,
    collateral: CollateralConfig,
    priorityPeerIds: Option[NonEmptySet[PeerId]],
    snapshotSize: SnapshotSizeConfig,
    feeConfigs: SortedMap[SnapshotOrdinal, FeeCalculatorConfig],
    forkInfoStorage: ForkInfoStorageConfig,
    lastKryoHashOrdinal: Map[AppEnvironment, SnapshotOrdinal],
    lastLegacyStateProofOrdinal: Map[AppEnvironment, SnapshotOrdinal],
    incrementalDelegatedStakingStartingOrdinal: Map[AppEnvironment, SnapshotOrdinal],
    addresses: AddressesConfig,
    allowSpends: AllowSpendsConfig,
    tokenLocks: TokenLocksConfig,
    lastGlobalSnapshotsSync: LastGlobalSnapshotsSyncConfig,
    validationErrorStorage: ValidationErrorStorageConfig,
    delegatedStaking: DelegatedStakingConfig,
    fieldsAddedOrdinals: FieldsAddedOrdinals,
    metagraphsSync: MetagraphsSyncConfig,
    priceOracle: PriceOracleConfig,
    snapshotBinarySenderTimeouts: SnapshotBinarySenderTimeoutsConfig,
    snapshotTimeoutsConfig: SnapshotTimeoutsConfig,
    clickHouseConfig: ClickHouseAppConfig,
    mptSnapshotInfoPath: Path,
    nakamoto: NakamotoConfig
  )

  case class SharedSnapshotConfig(
    size: SnapshotSizeConfig,
    timeouts: SnapshotTimeoutsConfig,
    mptSnapshotInfoPath: Path
  )

  case class SnapshotSizeConfig(
    singleSignatureSizeInBytes: PosLong,
    maxStateChannelSnapshotBinarySizeInBytes: PosLong
  )

  case class RumorStorageConfig(
    peerRumorsCapacity: PosLong,
    activeCommonRumorsCapacity: NonNegLong,
    seenCommonRumorsCapacity: NonNegLong
  )

  case class GossipDaemonConfig(
    peerRound: GossipRoundConfig,
    commonRound: GossipRoundConfig
  )

  case class GossipRoundConfig(
    fanout: PosInt,
    interval: FiniteDuration,
    maxConcurrentRounds: PosInt,
    maxOrdinalsPerRequest: Option[PosInt] = None
  )

  case class GossipTimeoutsConfig(
    routes: FiniteDuration,
    client: FiniteDuration
  )

  case class GossipConfig(
    storage: RumorStorageConfig,
    daemon: GossipDaemonConfig,
    timeouts: GossipTimeoutsConfig
  )

  case class ConsensusConfig(
    timeTriggerInterval: FiniteDuration,
    declarationTimeout: FiniteDuration,
    declarationRangeLimit: NonNegLong,
    lockDuration: FiniteDuration,
    eventCutter: EventCutterConfig,
    maxFacilitatorCount: Option[PosInt] = None,
    reStallTimeout: Option[FiniteDuration] = None,
    noProgressTimeout: Option[FiniteDuration] = None,
    maxStallCycles: Int = 3,
    maxRoundDuration: Option[FiniteDuration] = None,
    removalPenaltyRounds: Int = 3,
    facilitiesTimeoutMultiplier: Double = 0.75,
    proposalsTimeoutMultiplier: Double = 1.5,
    signaturesTimeoutMultiplier: Double = 0.75,
    maxConsecutiveAbandonments: Int = 5,
    monitorSummaryInterval: FiniteDuration = FiniteDuration(10, "s"),
    peerScoreLogInterval: FiniteDuration = FiniteDuration(60, "s"),
    qualityDecayThreshold: Int = 100,
    eventTriggerThreshold: Int = 1,
    eventTriggerCooldown: FiniteDuration = FiniteDuration(5, "s"),
    eventGossipHeartbeatInterval: FiniteDuration = FiniteDuration(10, "s"),
    eventGossipPullInterval: FiniteDuration = FiniteDuration(20, "s"),
    forkLagThreshold: Long = 10,
    /** INTERIM kill-switch (2026-06-11, default OFF): when false, rounds neither propose registered peers as candidates nor fold approved
      * candidates into the facilitator base — the cohort runs a SOLO producer with correct, non-forking followers (the production gate
      * keeps unadmitted nodes inert). Multi-facilitator admission re-enables with the unified chain-based engine
      * (docs/nakamoto/UNIFIED-CONSENSUS-ENGINE-DESIGN.md); the BFT round machinery cannot survive an admitted-but-absent facilitator
      * (2-facilitator rounds wedge at progress=1/2 with no working eviction — run bmnnfnao7).
      */
    candidateAdmissionEnabled: Boolean = false
  ) {

    /** Deterministic hash of consensus-critical config values.
      *
      * All nodes in a consensus round MUST have the same config to produce the same results. This hash is included in Facility declarations
      * so that config divergence is detected immediately during the CollectingFacilities phase, rather than causing mysterious forks
      * downstream.
      *
      * '''Consensus-critical fields''' (included in hash):
      *   - `maxFacilitatorCount`: determines eligible facilitator list size and rendezvous hashing
      *   - `maxStallCycles`: affects when rounds are abandoned (triggers recovery)
      *   - `removalPenaltyRounds`: affects facilitator eligibility after eviction
      *
      * '''Non-critical fields''' (excluded — affect timing/performance, not deterministic outcomes):
      *   - `timeTriggerInterval`, `declarationTimeout`, `lockDuration`, `reStallTimeout`, `noProgressTimeout`: timing only
      *   - `facilitiesTimeoutMultiplier`, `proposalsTimeoutMultiplier`, `signaturesTimeoutMultiplier`: timing multipliers only
      *   - `maxRoundDuration`: safety net, not consensus logic
      *   - `declarationRangeLimit`, `eventCutter`: event filtering, not consensus decisions
      *   - `qualityDecayThreshold`: local peer quality tracking, no consensus effect
      *
      * IMPORTANT: When adding new fields to ConsensusConfig, evaluate whether they affect consensus determinism. If the field changes what
      * peers decide (facilitator selection, quorum logic, voting thresholds), add it to the hash string below. If it only affects timing or
      * performance, exclude it.
      */
    lazy val deterministicConfigHash: Hash = {
      val configString =
        s"maxFacilitatorCount=${maxFacilitatorCount.map(_.value)}," +
          s"maxStallCycles=$maxStallCycles," +
          s"removalPenaltyRounds=$removalPenaltyRounds"
      Hash.fromBytes(configString.getBytes("UTF-8"))
    }
  }

  case class EventCutterConfig(
    maxBinarySizeBytes: PosInt,
    maxUpdateNodeParametersSize: PosInt
  )

  case class SnapshotBinarySenderTimeoutsConfig(
    routes: FiniteDuration,
    client: FiniteDuration
  )

  case class SnapshotTimeoutsConfig(
    routes: FiniteDuration,
    client: FiniteDuration
  )

  case class RouteRateLimiterConfig(
    public: FiniteDuration,
    peerToPeer: FiniteDuration
  )

  object RouteRateLimiterConfig {
    def empty(): RouteRateLimiterConfig =
      RouteRateLimiterConfig(
        0.second,
        0.second
      )
  }
  case class ClickHouseAppConfig(
    maxRetries: Int,
    maxQueueSize: Int,
    retryBaseDelay: FiniteDuration,
    batchSize: Int,
    flushInterval: FiniteDuration,
    retentionPeriodInDays: Int,
    errorPauseDuration: FiniteDuration,
    host: Option[String],
    user: Option[String],
    password: Option[String],
    logsTableName: Option[String],
    metricsTableName: Option[String],
    port: Option[Int],
    database: Option[String]
  )

  case class SnapshotConfig(
    consensus: ConsensusConfig,
    inMemoryCapacity: NonNegLong,
    snapshotPath: Path,
    snapshotInfoPath: Path,
    incrementalTmpSnapshotPath: Path,
    incrementalPersistedSnapshotPath: Path,
    calculatedStatePath: Path,
    globalSnapshotsWithStatePath: Path,
    globalSnapshotsWithStateDeltasPath: Path,
    maxGlobalSnapshotsWithStateStored: PosLong,
    maxGlobalSnapshotsWithStateDeltasStored: PosLong,
    combinedSnapshotCheckpointPath: Path
  )

  case class HttpClientConfig(
    timeout: FiniteDuration,
    idleTimeInPool: FiniteDuration
  )

  case class HttpServerConfig(
    host: Host,
    port: Port,
    shutdownTimeout: FiniteDuration
  )

  case class HttpConfig(
    externalIp: Host,
    client: HttpClientConfig,
    publicHttp: HttpServerConfig,
    p2pHttp: HttpServerConfig,
    cliHttp: HttpServerConfig
  )

  case class CollateralConfig(
    amount: Amount
  )

  case class DelegatedStakingConfig(
    minRewardFraction: RewardFraction,
    maxRewardFraction: RewardFraction,
    maxMetadataFieldsChars: PosInt,
    maxTokenLocksPerAddress: PosInt,
    minTokenLockAmount: PosLong,
    withdrawalTimeLimit: Map[AppEnvironment, EpochProgress]
  )

  case class EmissionConfigEntry(
    epochsPerYear: PosLong,
    asOfEpoch: EpochProgress,
    iTarget: NonNegFraction,
    iInitial: NonNegFraction,
    lambda: NonNegFraction,
    iImpact: NonNegFraction,
    totalSupply: Amount,
    dagPrices: Map[EpochProgress, NonNegFraction],
    epochsPerMonth: NonNegLong
  )

  case class ProgramsDistributionConfig(
    weights: Map[Address, NonNegFraction],
    validatorsWeight: NonNegFraction,
    delegatorsWeight: NonNegFraction
  )

  case class OneTimeReward(epoch: EpochProgress, address: Address, amount: TransactionAmount)

  sealed trait RewardsConfig

  case class ClassicRewardsConfig(
    programs: EpochProgress => ProgramsDistributionConfig,
    rewardsPerEpoch: Map[EpochProgress, Amount],
    oneTimeRewards: List[OneTimeReward]
  ) extends RewardsConfig

  case class DelegatedRewardsConfig(
    flatInflationRate: NonNegFraction,
    emissionConfig: Map[AppEnvironment, EpochProgress => EmissionConfigEntry],
    percentDistribution: Map[AppEnvironment, EpochProgress => ProgramsDistributionConfig],
    oneTimeRewards: Map[AppEnvironment, List[OneTimeReward]],
    priceOracleEpoch: Map[AppEnvironment, EpochProgress]
  ) extends RewardsConfig

  case class PeerDiscoveryDelay(
    checkPeersAttemptDelay: FiniteDuration,
    checkPeersMaxDelay: FiniteDuration,
    additionalDiscoveryDelay: FiniteDuration,
    minPeers: PosInt
  )

  case class ForkInfoStorageConfig(
    maxSize: PosInt
  )

  case class AddressesConfig(locked: Set[Address])

  case class MinMax(min: NonNegLong, max: NonNegLong)

  case class AllowSpendsConfig(lastValidEpochProgress: MinMax)

  case class TokenLocksConfig(minEpochProgressesToLock: NonNegLong)

  case class LastGlobalSnapshotsSyncConfig(syncOffset: NonNegLong, maxLastGlobalSnapshotsInMemory: PosInt)

  case class ValidationErrorStorageConfig(maxSize: PosInt)

  case class PriceOracleConfig(
    allowedMetagraphIds: Option[List[Address]],
    minEpochsBetweenUpdates: NonNegLong
  )

  object PriceOracleConfig {
    val default = PriceOracleConfig(List.empty.some, NonNegLong.MaxValue)
  }
}
