package io.constellationnetwork.node.shared.modules

import java.security.KeyPair

import cats.Parallel
import cats.data.NonEmptySet
import cats.effect.Async
import cats.effect.std.Supervisor
import cats.syntax.all._

import io.constellationnetwork.domain.allowance_list.AllowanceListEntry
import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.config.DefaultDelegatedRewardsConfigProvider
import io.constellationnetwork.node.shared.config.types.{CollateralConfig, SharedConfig}
import io.constellationnetwork.node.shared.domain.cluster.services.{Cluster, Session}
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceManager
import io.constellationnetwork.node.shared.domain.gossip.Gossip
import io.constellationnetwork.node.shared.domain.healthcheck.LocalHealthcheck
import io.constellationnetwork.node.shared.domain.node.UpdateNodeParametersAcceptanceManager
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralAcceptanceManager
import io.constellationnetwork.node.shared.domain.priceOracle.PriceStateUpdater
import io.constellationnetwork.node.shared.domain.statechannel.FeeCalculator
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockAcceptanceManager
import io.constellationnetwork.node.shared.http.p2p.clients.NodeClient
import io.constellationnetwork.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.cluster.services.Cluster
import io.constellationnetwork.node.shared.infrastructure.gossip.{Gossip => GossipImpl}
import io.constellationnetwork.node.shared.infrastructure.healthcheck.LocalHealthcheck
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.node.RestartService
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
import io.constellationnetwork.node.shared.infrastructure.snapshot._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency.CurrencySnapshotAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  GlobalSnapshotAcceptanceManager,
  GlobalSnapshotStateChannelAcceptanceManager,
  GlobalSnapshotStateChannelEventsProcessor
}
import io.constellationnetwork.node.shared.logger.LoggerBundle
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.generation.Generation
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.{Hasher, HasherSelector, SecurityProvider}

import fs2.concurrent.SignallingRef

object SharedServices {

  /** Path 1 (heap-leak workstream): the default genesis eta seed used as a 32-byte fall-through whenever the §3 NIPoPoW boundary writer
    * cannot derive eta from the chain (period ≤ 1, no chain-store, empty chain walk). Mirrors the per-cluster constant the dag-l0
    * `GlobalSnapshotConsensus.nakamotoGenesisEta` helper computes — both call sites produce byte-identical bytes so the MPT entry is
    * deterministic across the leader (gl0 with chain-store walk) and the follower-path SharedServices GSAM (no-op walk → genesisEta).
    *
    * Constructed lazily (not via `val`) because the Blake2b digest is a stateful instance; building one per call avoids accidental cross-
    * call mutation.
    */
  def DefaultNakamotoGenesisEta: Array[Byte] = {
    val seed = "tessellation-nakamoto-genesis-eta-v1".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val digest = new org.bouncycastle.crypto.digests.Blake2bDigest(256)
    digest.update(seed, 0, seed.length)
    val out = new Array[Byte](32)
    digest.doFinal(out, 0)
    out
  }

  /** Path 1 (heap-leak workstream): default no-op chain walk for [[EtaStateManager]]. Layers without a chain store pass this (returns an
    * empty list for every source-period query); the manager falls through to `genesisEta` on the cache miss, matching the pre-Path-1
    * `Hash.empty` semantics in spirit but with a deterministic non-zero value (so the MPT entry is informative rather than a sentinel
    * `Hash.empty`).
    */
  def noopEtaChainWalk[F[_]: Async]: Long => F[List[(Long, Array[Byte])]] =
    (_: Long) => Async[F].pure(List.empty[(Long, Array[Byte])])

  /** Path 1 (heap-leak workstream): hex-encode a 32-byte eta into the [[Hash]] shape the [[GlobalSnapshotAcceptanceManager]] boundary
    * writer stores in `HistoricalStakeSnapshot.eta`. Mirrors the inverse decode in
    * [[io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager.make]] (`entry.eta.value.grouped(2)…`); the round-trip is
    * byte-identical so the MPT entries are deterministic across nodes.
    *
    * Duplicate of `GlobalSnapshotConsensus.etaBytesToHash` (in dag-l0) — the two helpers exist to avoid a node-shared → dag-l0 dependency
    * inversion; both produce identical hex bytes for any 32-byte input.
    */
  def etaBytesToHash(bytes: Array[Byte]): Hash =
    Hash(bytes.map(b => f"$b%02x").mkString)

  /** Build the `EtaPeriod => F[Hash]` callback used by [[GlobalSnapshotAcceptanceManager]] from an
    * [[io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager]]. Routes the manager's `Array[Byte]` output through
    * [[etaBytesToHash]] under `HasherSelector.withCurrent` (so the MPT cache lookup uses the same hasher the boundary writer commits with).
    */
  def etaForPeriodCallback[F[_]: Async: HasherSelector](
    mgr: io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager[F]
  ): io.constellationnetwork.schema.nakamoto.EtaPeriod => F[Hash] =
    (period: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
      HasherSelector[F].withCurrent { implicit hasher =>
        mgr.getEta(period.value).map(etaBytesToHash)
      }

  def make[F[_]: Async: Parallel: HasherSelector: SecurityProvider: Metrics: Supervisor: JsonSerializer: KryoSerializer, A <: CliMethod](
    cfg: SharedConfig,
    nodeId: PeerId,
    generation: Generation,
    keyPair: KeyPair,
    storages: SharedStorages[F],
    queues: SharedQueues[F],
    session: Session[F],
    nodeClient: NodeClient[F],
    validators: SharedValidators[F],
    seedlist: Option[Set[SeedlistEntry]],
    restartSignal: SignallingRef[F, Option[A]],
    versionHash: Hash,
    metagraphVersionHash: Hash,
    jarHash: Hash,
    collateral: CollateralConfig,
    stateChannelAllowanceLists: Option[Map[Address, NonEmptySet[PeerId]]],
    environment: AppEnvironment,
    txHasher: Hasher[F],
    allowanceList: Option[Set[AllowanceListEntry]],
    metagraphId: Option[Address],
    loggerBundle: LoggerBundle[F],
    // Path 1 (heap-leak workstream): genesis eta + chain-walk fallback for the [[EtaStateManager]]
    // that backs the GSAM boundary-write callback. Default = a constant Blake2b-domain-string genesis
    // and a no-op chain walk; layers that don't run a Nakamoto chain store (cl0, cl1, dl1, gl1) get
    // `genesisEta`-bytes written at every boundary (period N reads `EtaStateManager.getEta(N)` which
    // hits MPT first, then falls through to chain walk; empty walk → `genesisEta` per `EtaCalculation`
    // convention). gl0 callers MAY override the chain-walk to thread `NakamotoChainStore.vrfOutputsForPeriod`
    // through a Ref (the chain store is built later in the Resource graph, so callers pass a closure that
    // reads from a `Ref[Option[NakamotoChainStoreAlgebra[F]]]`). See `GlobalSnapshotConsensus.make` for the
    // dag-l0 GSAM construction which also uses the same `EtaStateManager.make` shape.
    nakamotoGenesisEta: Array[Byte] = SharedServices.DefaultNakamotoGenesisEta,
    nakamotoEtaChainWalkFallback: Option[Long => F[List[(Long, Array[Byte])]]] = None
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    currencyStateProofSelector: CurrencyStateProofSelector
  ): F[SharedServices[F, A]] =
    for {
      restartService <- RestartService.make(restartSignal, storages.cluster)

      cluster = Cluster
        .make[F](
          cfg.leavingDelay,
          cfg.http,
          nodeId,
          keyPair,
          storages.cluster,
          storages.session,
          storages.node,
          seedlist,
          restartService,
          versionHash,
          metagraphVersionHash,
          jarHash,
          environment,
          allowanceList,
          metagraphId
        )

      localHealthcheck <- LocalHealthcheck.make[F](nodeClient, storages.cluster)
      gossip <- HasherSelector[F].withCurrent(implicit hasher => GossipImpl.make[F](queues.rumor, nodeId, generation, keyPair))
      currencySnapshotAcceptanceManager <- CurrencySnapshotAcceptanceManager.make(
        cfg.fieldsAddedOrdinals,
        cfg.environment,
        cfg.lastGlobalSnapshotsSync,
        BlockAcceptanceManager.make[F](validators.currencyBlockValidator, txHasher),
        TokenLockBlockAcceptanceManager.make[F](validators.tokenLockBlockValidator),
        AllowSpendBlockAcceptanceManager.make[F](validators.allowSpendBlockValidator),
        collateral.amount,
        validators.currencyMessageValidator,
        validators.feeTransactionValidator,
        validators.globalSnapshotSyncValidator,
        storages.lastNGlobalSnapshot,
        storages.lastGlobalSnapshot
      )

      currencyEventsCutter = CurrencyEventsCutter.make[F](None)

      currencySnapshotValidator = CurrencySnapshotValidator.make[F](
        cfg.fieldsAddedOrdinals.tessellation3Migration.getOrElse(cfg.environment, SnapshotOrdinal.MinValue),
        CurrencySnapshotCreator.make[F](
          cfg.fieldsAddedOrdinals.tessellation3Migration.getOrElse(cfg.environment, SnapshotOrdinal.MinValue),
          currencySnapshotAcceptanceManager,
          None,
          cfg.snapshotSize,
          currencyEventsCutter,
          storages.currencySnapshotEventValidationError
        ),
        validators.signedValidator,
        None,
        None
      )
      currencySnapshotContextFns = CurrencySnapshotContextFunctions.make(
        currencySnapshotValidator
      )
      feeCalculator = FeeCalculator.make(cfg.feeConfigs)
      globalSnapshotStateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager.make(stateChannelAllowanceLists)
      updateNodeParametersAcceptanceManager = UpdateNodeParametersAcceptanceManager.make(validators.updateNodeParametersValidator)
      updateDelegatedStakeAcceptanceManager = UpdateDelegatedStakeAcceptanceManager.make(
        validators.updateDelegatedStakeValidator
      )
      updateNodeCollateralAcceptanceManager = UpdateNodeCollateralAcceptanceManager.make(
        validators.updateNodeCollateralValidator
      )
      priceStateUpdater = PriceStateUpdater.make[F](
        cfg.environment,
        DefaultDelegatedRewardsConfigProvider,
        io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore(storages.mptStore)
      )
      // Path 1 (heap-leak workstream): construct the [[EtaStateManager]] backing the GSAM `etaForPeriod` callback.
      //
      // The manager wraps an MPT-cache point read (`HistoricalStakeReader.lookup(period)`) + a caller-supplied chain-walk fallback.
      // On the SharedServices side both the MPT cache and the chain walk are layer-agnostic — gl0 still gets a richer chain walk via
      // the `GlobalSnapshotConsensus` GSAM (which has access to `NakamotoChainStore.vrfOutputsForPeriod`). Layers without a chain
      // store (cl0, cl1, dl1, gl1) use the default no-op walk; their boundary write at `ord % R == R - 1` lands the
      // `EtaCalculation.computeEta(genesisEta, period, [])` fallback for periods ≤ 1, and the period-N writeover lazily falls back to
      // `genesisEta` for higher periods until/unless a custom walk is wired.
      //
      // Wiring through a fresh MPT-only `GlobalStateReader.fromMptStore(storages.mptStore)` matches the existing
      // `HistoricalStakeReader` usage in `GlobalSnapshotConsensus.make` — both producer and reader observe the same per-key bytes that
      // `AcceptanceMptStateChanges.applyStateChanges` writes inside `accept()`.
      sharedHistoricalStakeReader = io.constellationnetwork.node.shared.domain.nakamoto.HistoricalStakeReader
        .make[F](io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore[F](storages.mptStore))
      etaStateManager <- io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager
        .make[F](
          genesisEta = nakamotoGenesisEta,
          historicalStakeReader = sharedHistoricalStakeReader,
          chainWalkFallback = nakamotoEtaChainWalkFallback.getOrElse(SharedServices.noopEtaChainWalk[F])
        )
      sharedEtaForPeriod = SharedServices.etaForPeriodCallback[F](etaStateManager)
      // Hierarchical-shard-checkpoints v1 — ACCEPTANCE-side production wiring (priority 1). Gated on
      // `cfg.nakamoto.sharding.numShards > 1`. At the production default `numShards = 1` this returns
      // `None` (constructs nothing) and the GSAM call below passes `None` for all three sharding params —
      // byte-identical to the pre-wiring call (the regression bar). See `ShardCheckpointWiring` scaladoc.
      //
      // The SharedServices GSAM is the verify/follower path (cl0/cl1/dl1/gl1 + gl0 follower). It has no
      // genesis-loaded KesRegistry in scope (that lives at the gl0 layer — see GlobalSnapshotConsensus),
      // so we pass `KesRegistry.empty`: the acceptance manager's registry-absent carve-out accepts on the
      // Ed25519 signature strength alone. The active-validator set is the seedlist minus `metagraph-op`
      // aliases (mirrors `GlobalSnapshotConsensus`'s `validatorPeers` derivation), falling back to `{nodeId}`.
      // S3 committee re-execution: the SAME `GlobalSnapshotStateChannelEventsProcessor` this (verify/follower) GSAM
      // uses is built once and shared by the shard verifier's `reExecuteDerivation`, so the verifier re-runs the
      // IDENTICAL currency derivation a producer used — the byte-identity contract that prevents false-slashing
      // (see `GlobalSnapshotStateChannelEventsProcessor.deriveMetagraphRoot`).
      shardScEventsProcessor = GlobalSnapshotStateChannelEventsProcessor
        .make[F](
          validators.stateChannelValidator,
          globalSnapshotStateChannelManager,
          currencySnapshotContextFns,
          feeCalculator,
          io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore(storages.mptStore)
        )
      shardAcceptanceDeps <- ShardCheckpointWiring.acceptanceDeps[F](
        cfg = cfg.nakamoto.sharding,
        selfPeerId = nodeId,
        kesRegistry = io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry.empty[F],
        // Slice S1: the verify/follower path has no genesis-loaded VRF registry in scope (it lives at the gl0
        // layer — see GlobalSnapshotConsensus). Pass `VrfRegistry.empty`; the registry is unconsumed in S1
        // (full-set committee membership), so empty here is a no-op exactly as `KesRegistry.empty` is.
        vrfRegistry = io.constellationnetwork.node.shared.domain.nakamoto.VrfRegistry.empty[F],
        activeValidators = Async[F].pure(
          seedlist
            .map(_.collect { case e if !e.alias.exists(_.value.value == "metagraph-op") => e.peerId })
            .getOrElse(Set(nodeId))
        ),
        // S3: real committee re-execution closure (replaces the `noReExecDerivation` fail-closed stub). Same shared
        // processor as GSAM so producer↔verifier roots are byte-identical.
        reExecuteDerivation = Some(
          ShardCheckpointWiring.reExecDerivation[F](shardScEventsProcessor)(Async[F], HasherSelector[F].getCurrent)
        )
      )(Async[F], HasherSelector[F].getCurrent, implicitly[SecurityProvider[F]], implicitly[Metrics[F]])
      globalSnapshotAcceptanceManager <- GlobalSnapshotAcceptanceManager.make(
        cfg.fieldsAddedOrdinals,
        cfg.metagraphsSync,
        cfg.environment,
        BlockAcceptanceManager.make[F](validators.blockValidator, txHasher),
        AllowSpendBlockAcceptanceManager.make[F](validators.allowSpendBlockValidator),
        TokenLockBlockAcceptanceManager.make[F](validators.tokenLockBlockValidator),
        shardScEventsProcessor,
        updateNodeParametersAcceptanceManager,
        updateDelegatedStakeAcceptanceManager,
        updateNodeCollateralAcceptanceManager,
        validators.spendActionValidator,
        validators.pricingUpdateValidator,
        priceStateUpdater,
        collateral.amount,
        cfg.delegatedStaking.withdrawalTimeLimit.getOrElse(cfg.environment, EpochProgress.MinValue),
        storages.mptOverlay,
        loggerBundle,
        maintainNodeCollateralWithdrawalExpiryIndex = true,
        // §3 NIPoPoW S0.4: must match the producer's `NAKAMOTO_ETA_ROTATION_SNAPSHOTS`. Read at GSAM construction so the
        // boundary-write check (`ord % R == R - 1`) inside accept() is deterministic across all nodes.
        // Path 1 (heap-leak workstream): moved from `sys.env.get("NAKAMOTO_ETA_ROTATION_SNAPSHOTS")` to HOCON
        // `nakamoto.eta-rotation-snapshots` (which still honors `${?NAKAMOTO_ETA_ROTATION_SNAPSHOTS}` substitution in
        // `application.conf` so ops scripts keep working). Matches the migration already landed in
        // `GlobalSnapshotConsensus.make` — the two GSAM construction sites now read the same typed config field.
        etaRotationSnapshots = cfg.nakamoto.etaRotationSnapshots.value,
        // Path 1 (heap-leak workstream): wire the eta callback so the boundary-write at `ord % R == R - 1` lands a
        // real computed eta in the `HistoricalStakeSnapshot` MPT entry instead of `Hash.empty`. The callback is backed
        // by an [[EtaStateManager]] (MPT cache + caller-supplied chain-walk fallback) constructed above.
        etaForPeriod = Some(sharedEtaForPeriod),
        // Hierarchical-shard-checkpoints v1 acceptance-side deps. `None` at `numShards = 1` (regression bar);
        // `Some(...)` activates the shard-checkpoint admission path inside `accept()`. The same
        // `ShardCheckpointWiring.acceptanceDeps` result feeds both GSAM construction sites so they stay consistent.
        shardingConfig = shardAcceptanceDeps.map(_.shardingConfig),
        shardCheckpointAcceptanceManager = shardAcceptanceDeps.map(_.acceptanceManager),
        shardAssignment = shardAcceptanceDeps.map(_.shardAssignment)
      )
      globalSnapshotContextFns = GlobalSnapshotContextFunctions.make(
        globalSnapshotAcceptanceManager,
        updateDelegatedStakeAcceptanceManager,
        cfg.delegatedStaking.withdrawalTimeLimit.getOrElse(cfg.environment, EpochProgress.MinValue),
        cfg.fieldsAddedOrdinals.tessellation3Migration.getOrElse(cfg.environment, SnapshotOrdinal.MinValue),
        cfg.fieldsAddedOrdinals.setSumFix.getOrElse(cfg.environment, SnapshotOrdinal.MinValue),
        storages.mptStore,
        cfg.incrementalDelegatedStakingStartingOrdinal.getOrElse(cfg.environment, SnapshotOrdinal.MinValue),
        storages.mptOverlay
      )
    } yield
      new SharedServices[F, A](
        localHealthcheck = localHealthcheck,
        cluster = cluster,
        session = session,
        gossip = gossip,
        globalSnapshotContextFns = globalSnapshotContextFns,
        currencySnapshotContextFns = currencySnapshotContextFns,
        currencySnapshotAcceptanceManager = currencySnapshotAcceptanceManager,
        currencyEventsCutter = currencyEventsCutter,
        restart = restartService,
        updateNodeParametersAcceptanceManager = updateNodeParametersAcceptanceManager,
        updateDelegatedStakeAcceptanceManager = updateDelegatedStakeAcceptanceManager,
        updateNodeCollateralAcceptanceManager = updateNodeCollateralAcceptanceManager,
        priceStateUpdater = priceStateUpdater
      ) {}
}

sealed abstract class SharedServices[F[_], A <: CliMethod] private (
  val localHealthcheck: LocalHealthcheck[F],
  val cluster: Cluster[F],
  val session: Session[F],
  val gossip: Gossip[F],
  val globalSnapshotContextFns: GlobalSnapshotContextFunctions[F],
  val currencySnapshotContextFns: CurrencySnapshotContextFunctions[F],
  val currencySnapshotAcceptanceManager: CurrencySnapshotAcceptanceManager[F],
  val currencyEventsCutter: CurrencyEventsCutter[F],
  val restart: RestartService[F, A],
  val updateNodeParametersAcceptanceManager: UpdateNodeParametersAcceptanceManager[F],
  val updateDelegatedStakeAcceptanceManager: UpdateDelegatedStakeAcceptanceManager[F],
  val updateNodeCollateralAcceptanceManager: UpdateNodeCollateralAcceptanceManager[F],
  val priceStateUpdater: PriceStateUpdater[F]
)
