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

  /** Path 1 (heap-leak workstream): the two consensus-critical Nakamoto knobs that previously read directly from `sys.env`
    * (`NAKAMOTO_ETA_ROTATION_SNAPSHOTS` and `NAKAMOTO_KEEP_DEPTH_BEHIND_FINALIZED`) routed through HOCON. The HOCON values keep the env-var
    * fallback for ops continuity (the application.conf entries use `${?NAKAMOTO_...}` substitution), so existing deploy scripts that set
    * the env vars continue to work — but the read in production code goes through this typed struct.
    *
    * Defaults match the pre-migration env-var defaults exactly: `eta-rotation-snapshots = 2550` (10·k₁); `keep-depth-behind-finalized =
    * 255` (k₁).
    *
    * Other `NAKAMOTO_*` env vars (LDD knobs, slots-per-epoch, etc.) are NOT migrated here — Wave 2 of the sys.env-to-HOCON sweep handles
    * the rest of the namespace in one pass.
    */
  case class NakamotoConfig(
    etaRotationSnapshots: PosLong,
    keepDepthBehindFinalized: PosLong,
    // Confirmation depth k₁ — REUSED by the §3 NIPoPoW historical-commitment SMT as its finalized cutoff (`smtRoot(N)` commits
    // ordinals i ≤ N − k). Mirrors the existing `NAKAMOTO_CONFIRMATION_DEPTH` env default (255) via the HOCON `${?...}` substitution
    // so the gl0 SMT wiring, `SnapshotLeaderLoop.ConfirmationDepthK`, and `NakamotoSyncDaemon` all agree.
    confirmationDepthK: PosLong,
    // #259 active-recovery: caps on the metagraph orphan buffer + recent-admission cache. Migrated from the
    // `NAKAMOTO_ORPHAN_BUFFER_CAP` / `NAKAMOTO_RECENT_ADMIT_CAP` env reads to typed HOCON (project rule: no scattered
    // sys.env). `recentAdmitCap` is kept proportionally larger (4×) — see `MetagraphOrphanBuffer.DefaultAdmissionsCap`.
    orphanBufferCap: PosInt,
    recentAdmitCap: PosInt,
    commitmentSmt: CommitmentSmtConfig,
    localEvents: LocalEventsConfig,
    sharding: ShardingConfig
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
    *   - `committeeKTarget`: target committee size per shard per epoch (the `K_target` of `COMMITTEE-SORTITION-DESIGN.md` §5). v1: fixed
    *     cluster-wide.
    *   - `finality`: per-shard FinalityTrigger params (`k1Shard` is the depth-finality fallback in the shard's own mini-chain — smaller
    *     than gl0 k₁=255 because shard ords are sparser).
    *   - `checkpoint`: emission cadence + burst cap for shard checkpoints (Option C per `SHARD-CHECKPOINT-GRANULARITY.md`).
    */
  case class ShardingConfig(
    numShards: Int,
    committeeKTarget: Int,
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
    forkLagThreshold: Long = 10
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
