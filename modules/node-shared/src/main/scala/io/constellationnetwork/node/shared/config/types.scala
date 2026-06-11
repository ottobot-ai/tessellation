package io.constellationnetwork.node.shared.config

import cats.data.NonEmptySet
import cats.syntax.option._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}

import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.node.shared.domain.statechannel.FeeCalculatorConfig
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

  /** Consensus-parameter family. `confirmationDepthK` (k₁) is the SINGLE free config knob; the two related depths are DERIVED from it
    * (settled by Ouroboros + our fork-race sims) rather than loaded independently — which removes the previous mis-configuration where
    * `eta-rotation-snapshots` held k₂'s value (10·k₁) and `keep-depth-behind-finalized` held k₁'s.
    *
    *   - k₁ = `confirmationDepthK(env)` — confirmation depth (fork-race statistical finality). Loaded from HOCON PER-ENVIRONMENT
    *     (`nakamoto.confirmation-depth-k` is a `{ mainnet, testnet, integrationnet, dev }` block, mirroring `last-kryo-hash-ordinal`) and
    *     resolved ONCE for the active `AppEnvironment` at the use site — mainnet 1024, test/integration nets 255, dev 33 (with the
    *     `${?NAKAMOTO_CONFIRMATION_DEPTH}` override applied to the dev value only). REUSED by the §3 NIPoPoW historical-commitment SMT as
    *     its finalized cutoff (`smtRoot(N)` commits ordinals i ≤ N − k₁), and by `SnapshotLeaderLoop` / `NakamotoSyncDaemon`.
    *   - R = `etaRotationSnapshots(env)` = round(3.1·k₁) — eta-rotation period. Ouroboros: the eta nonce uses the first 2/3 of the period's
    *     VRF rho values, so the last 1/3 = R/3 must be ≥ k₁ (those inputs FINALIZED before use) ⇒ R ≥ 3·k₁; the .1 over 3 is the worst-case
    *     finalization margin (the 2/3-mark must FINALIZE before the boundary; finalization lags production by ≤ k₁) ⇒ (R/3−k₁)=0.033·k₁.
    *   - k₂ = `keepDepthBehindFinalized(env)` = 100·k₁ — historical-archive / phase-3 retention depth (the tower's moving checkpoint).
    *
    * Both derived depths are env-parameterized methods (NOT vals) so they resolve from the SAME per-env k₁ as the active environment;
    * resolve the env from the wrapping `SharedConfig.environment` and pass it once (see the call sites in `GlobalSnapshotConsensus` /
    * `CurrencyL0App` / `SharedServices`).
    *
    * Other `NAKAMOTO_*` env vars (LDD knobs, slots-per-epoch, etc.) are NOT migrated here — Wave 2 of the sys.env-to-HOCON sweep handles
    * the rest of the namespace in one pass.
    */
  case class NakamotoConfig(
    // Confirmation depth k₁ — the single loaded consensus-depth knob, now PER-ENVIRONMENT (`nakamoto.confirmation-depth-k` block:
    // mainnet 1024 / testnet 255 / integrationnet 255 / dev 33, dev overridable via `${?NAKAMOTO_CONFIRMATION_DEPTH}`). Resolve for the
    // active env via the `confirmationDepthK(env)` accessor below — R and k₂ DERIVE from the resolved value (see the `def`s in the body).
    confirmationDepthKByEnv: Map[AppEnvironment, PosLong],
    // #259 active-recovery: caps on the metagraph orphan buffer + recent-admission cache. Migrated from the
    // `NAKAMOTO_ORPHAN_BUFFER_CAP` / `NAKAMOTO_RECENT_ADMIT_CAP` env reads to typed HOCON (project rule: no scattered
    // sys.env). `recentAdmitCap` is kept proportionally larger (4×) — see `MetagraphOrphanBuffer.DefaultAdmissionsCap`.
    orphanBufferCap: PosInt,
    recentAdmitCap: PosInt,
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
    commitmentSmt: CommitmentSmtConfig,
    localEvents: LocalEventsConfig,
    committee: CommitteeConfig,
    sharding: ShardingConfig
  ) {
    // k₁ for the active environment. Resolved once at the use site from `SharedConfig.environment`. Falls back to the dev value
    // (`NakamotoConfig.DefaultConfirmationDepthK`, 32) for any env absent from the HOCON block — same neutral-default convention as
    // `Era.fromConfig` (which uses `SnapshotOrdinal.MinValue`). All four standard envs are always present in `application.conf`, so the
    // fallback only fires under a hand-trimmed config; 32 keeps a node finalizing rather than failing at boot.
    def confirmationDepthK(env: AppEnvironment): PosLong =
      confirmationDepthKByEnv.getOrElse(env, NakamotoConfig.DefaultConfirmationDepthK)
    // R = 3.1·k₁ (Ouroboros: eta uses the first 2/3 of the period; last 1/3 = R/3 ≥ k₁ so those inputs FINALIZE before use ⇒ R ≥ 3·k₁).
    // The .1 over 3 (was .03) is the worst-case finalization margin: the 2/3-mark must FINALIZE before the boundary consumes the eta,
    // and finalization can lag production by k₁ (depth-k, no optimistic finality) ⇒ margin (R/3 − k₁) = 0.033·k₁ (#31 eta-amortization).
    // Derived from the per-env k₁, NOT loaded — keeps the eta-rotation period in lockstep with the confirmation depth.
    def etaRotationSnapshots(env: AppEnvironment): PosLong =
      PosLong.unsafeFrom(math.round(3.1d * confirmationDepthK(env).value))
    // k₂ = 100·k₁ — historical-archive / phase-3 retention (tower moving checkpoint). Deep deliberately: prod keeps a long, slow,
    // stable consensus history (≈8 days at k₁=1024 / 7s snapshots). Derived from the per-env k₁, NOT loaded.
    def keepDepthBehindFinalized(env: AppEnvironment): PosLong =
      PosLong.unsafeFrom(100L * confirmationDepthK(env).value)
  }

  object NakamotoConfig {
    // Neutral fallback for `confirmationDepthK(env)` when an environment is missing from the HOCON block — the dev default (32).
    val DefaultConfirmationDepthK: PosLong = PosLong.unsafeFrom(32L)

    // The `confirmationDepthKByEnv` field reads from the HOCON key `confirmation-depth-k` (the per-env block), NOT the
    // default kebab-cased `confirmation-depth-k-by-env`. Same `ProductHint` field-override technique as [[ShardFinalityConfig]];
    // every OTHER field falls through to pureconfig's default `CamelCase` → `KebabCase` so they keep their existing keys.
    implicit val configHint: _root_.pureconfig.generic.ProductHint[NakamotoConfig] =
      _root_.pureconfig.generic.ProductHint[NakamotoConfig](_root_.pureconfig.ConfigFieldMapping {
        case "confirmationDepthKByEnv" => "confirmation-depth-k"
        case other => _root_.pureconfig.ConfigFieldMapping(_root_.pureconfig.CamelCase, _root_.pureconfig.KebabCase)(other)
      })
  }

  /** Committee draw/quorum decouple — the two cluster-uniform knobs that size the per-metagraph committee gate AND (reused) the per-shard
    * committee. Both MUST be byte-identical on every node: `kDraw` keys the VRF/VK-seeded DRAW so the elected committee is the SAME
    * sender↔receiver and node↔node, and `kQuorum` is the admit count every node waits for, so they all admit at the same threshold.
    *
    *   - `kDraw` — committee DRAW target. Used in `CommitteeSortition.threshold(kDraw, σ) = min(kDraw·σ, 1)`, so the expected committee
    *     size is `≈ kDraw·σ·N`. With uniform σ = 1/N this is `≈ kDraw`; setting `kDraw = N` makes `kDraw·σ = 1` saturate ⇒ committee =
    *     everyone.
    *   - `kQuorum` — admit quorum. The number of distinct committee attestations the metagraph gate waits for
    *     (`MetagraphAttestationAggregator.thresholdReached`) and the per-shard acceptance count (`ShardCheckpointGl0AcceptanceManager` /
    *     `ShardFinalityTriggers.tCountShard`). This is the count DIRECTLY (no further 2/3 multiplier).
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
    *   - `finality`: per-shard FinalityTrigger params (`k1Shard` is the depth-finality fallback in the shard's own mini-chain — smaller
    *     than gl0 k₁=255 because shard ords are sparser).
    *   - `checkpoint`: emission cadence + burst cap for shard checkpoints (Option C per `SHARD-CHECKPOINT-GRANULARITY.md`).
    *
    * NOTE: the shard committee DRAW target + ADMIT quorum are NOT here — they are the cluster-wide [[CommitteeConfig]] (`kDraw` /
    * `kQuorum`), shared with the per-metagraph committee gate, threaded into the shard wiring from `nakamoto.committee`. (Previously this
    * block carried a single overloaded `committeeKTarget` that conflated draw and quorum.)
    */
  case class ShardingConfig(
    numShards: Int,
    finality: ShardFinalityConfig,
    checkpoint: ShardCheckpointConfig,
    observability: ShardObservabilityConfig,
    slashing: ShardSlashingConfig
  )

  /** `k1Shard` maps to HOCON key `k1-shard` (the digit binds tight to the preceding letter — same convention as `k₁` in the design doc). A
    * `ProductHint` is supplied in [[ShardFinalityConfig]]'s companion so pureconfig's default `CamelCase` → `KebabCase` doesn't split the
    * field into the unwanted `k-1-shard`.
    */
  case class ShardFinalityConfig(k1Shard: Long)

  object ShardFinalityConfig {
    implicit val configHint: _root_.pureconfig.generic.ProductHint[ShardFinalityConfig] =
      _root_.pureconfig.generic.ProductHint[ShardFinalityConfig](
        _root_.pureconfig.ConfigFieldMapping(Map("k1Shard" -> "k1-shard"))
      )
  }

  case class ShardCheckpointConfig(tAliveMs: Long, tBurst: Int, binaryBufferCap: Int)

  /** Slice 19 observability tunables (see `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §13 row 19 + §9.4).
    *
    *   - `tPartitionHardMs`: if a shard goes longer than this without ANY `T_count_shard` attestation reaching threshold (only the
    *     `T_depth1_shard` fallback fires), the gl0 leader logs a `SHARD-PARTITION-SUSPECT` WARN and increments
    *     `dag_nakamoto_shard_partition_hard_total{shard_id}`. Operator intervention is expected; per design-doc §9.4, v1 does not perform
    *     automatic rotation. Default `600000` ms = 10 minutes (≥ `5 × t-alive-ms` in any realistic deploy).
    */
  case class ShardObservabilityConfig(tPartitionHardMs: Long)

  /** Slice 17 (`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §10.3) — per-epoch non-participation slashing.
    *
    *   - `maxMissedPctPerEpoch`: percentage threshold (0..100) — a committee member whose `missedSlotsAsLeader / totalSlotsAsLeader` or
    *     `missedAttestationWindows / totalCheckpointsReceived` exceeds this fraction in the just-closed epoch is added to the slash list at
    *     the epoch boundary. Default `33` ⇒ slash when more than ~one-third of duties are missed.
    *   - `minDenominatorPerEpoch`: minimum number of duties (slot-leader elections OR checkpoints received) that must have occurred before
    *     the rate is evaluated for that obligation. Without this floor, a peer elected leader once and missing that single slot would
    *     register 100% missed and be slashed — a single sample is noise, not evidence of non-participation. Default `5` ⇒ ignore rates when
    *     fewer than 5 duties were attempted; matches the slashing-safety bar (evidence + determinism + standard patterns —
    *     `feedback_slashing_safety_bar`) which favours false-negatives over false-positives.
    */
  case class ShardSlashingConfig(maxMissedPctPerEpoch: Int, minDenominatorPerEpoch: Long)

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
