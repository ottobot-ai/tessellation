package io.constellationnetwork.dag.l0.infrastructure.snapshot

import java.security.KeyPair

import cats.Parallel
import cats.data.NonEmptySet
import cats.effect.kernel.{Async, Fiber, Ref}
import cats.effect.std.{Queue, Random, Supervisor}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.dag.l0.config.types.AppConfig
import io.constellationnetwork.dag.l0.domain.snapshot.programs.{
  GlobalSnapshotEventCutter,
  SnapshotBinaryFeeCalculator,
  UpdateNodeParametersCutter
}
import io.constellationnetwork.dag.l0.infrastructure.rewards.RewardsService
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.schema.{GlobalConsensusKind, GlobalConsensusOutcome}
import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.json.{JsonBrotliBinarySerializer, JsonSerializer}
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.config.DefaultDelegatedRewardsConfigProvider
import io.constellationnetwork.node.shared.config.types.SharedConfig
import io.constellationnetwork.node.shared.domain.cluster.services.Session
import io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage
import io.constellationnetwork.node.shared.domain.consensus.ConsensusFunctions
import io.constellationnetwork.node.shared.domain.gossip.Gossip
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.rewards.Rewards
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage, SnapshotStorage}
import io.constellationnetwork.node.shared.domain.statechannel.{FeeCalculator, FeeCalculatorConfig}
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.consensus._
import io.constellationnetwork.node.shared.infrastructure.consensus.engine.{ConsensusCommand, ConsensusEventLoop, _}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.NakamotoTriggerDaemon
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.NakamotoTriggerDaemon.NakamotoTriggerState
import io.constellationnetwork.node.shared.infrastructure.consensus.state._
import io.constellationnetwork.node.shared.infrastructure.gossip.RumorHandler
import io.constellationnetwork.node.shared.infrastructure.gossip.event.EventGossipClient
import io.constellationnetwork.node.shared.infrastructure.mempool.EventMempool
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.node.RestartService
import io.constellationnetwork.node.shared.infrastructure.snapshot._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  GlobalSnapshotAcceptanceManager,
  GlobalSnapshotStateChannelAcceptanceManager,
  GlobalSnapshotStateChannelEventsProcessor
}
import io.constellationnetwork.node.shared.logger.LoggerBundle
import io.constellationnetwork.node.shared.modules.{SharedServices, SharedValidators}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.gossip.RumorRaw
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._

import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.Json
import org.http4s.client.Client

/** Factory for creating the Global L0 consensus engine.
  *
  * Wires together all components and starts the consensus background stream. Returns a Consensus instance with handler (for gossip),
  * manager (external API), storage (state queries), and routes (HTTP endpoints).
  *
  * When NAKAMOTO_ENABLED env var is set, uses VRF slot-based leader election via NakamotoTriggerDaemon instead of the traditional
  * EventTrigger + TimeTrigger system. The BFT round machinery (Facility → Proposal → Signature → Finished) remains unchanged.
  *
  * @see
  *   ConsensusEventLoop for FSM and command processing
  * @see
  *   NakamotoTriggerDaemon for VRF slot clock implementation
  */
object GlobalSnapshotConsensus {

  /** Check if Nakamoto mode is enabled via environment variable */
  val nakamotoEnabled: Boolean = sys.env.contains("NAKAMOTO_ENABLED")

  def make[F[_]: Async: Parallel: Random: JsonSerializer: HasherSelector: SecurityProvider: Metrics, R <: CliMethod](
    sharedCfg: SharedConfig,
    gossip: Gossip[F],
    selfId: PeerId,
    keyPair: KeyPair,
    seedlist: Option[Set[SeedlistEntry]],
    collateral: Amount,
    clusterStorage: ClusterStorage[F],
    nodeStorage: NodeStorage[F],
    globalSnapshotStorage: SnapshotStorage[F, GlobalSnapshotArtifact, GlobalSnapshotContext],
    validators: SharedValidators[F],
    sharedServices: SharedServices[F, R],
    appConfig: AppConfig,
    stateChannelPullDelay: NonNegLong,
    stateChannelPurgeDelay: NonNegLong,
    stateChannelAllowanceLists: Option[Map[Address, NonEmptySet[PeerId]]],
    feeConfigs: SortedMap[SnapshotOrdinal, FeeCalculatorConfig],
    client: Client[F],
    session: Session[F],
    rewardsService: RewardsService[F],
    txHasher: Hasher[F],
    restartService: RestartService[F, R],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    mptStore: MptStore[F, GlobalStateKey],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    eventGossipClient: EventGossipClient[F, GlobalSnapshotEvent],
    loggerBundle: LoggerBundle[F],
    rumorQueue: Queue[F, Hashed[RumorRaw]]
  )(implicit supervisor: Supervisor[F], globalStateProofSelector: GlobalStateProofSelector): F[GlobalSnapshotConsensus[F]] =
    for {
      globalStateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager
        .make[F](stateChannelAllowanceLists, pullDelay = stateChannelPullDelay, purgeDelay = stateChannelPurgeDelay)

      feeCalculator = FeeCalculator.make(feeConfigs)

      snapshotAcceptanceManager =
        GlobalSnapshotAcceptanceManager.make(
          sharedCfg.fieldsAddedOrdinals,
          sharedCfg.metagraphsSync,
          sharedCfg.environment,
          BlockAcceptanceManager.make[F](validators.blockValidator, txHasher),
          AllowSpendBlockAcceptanceManager.make[F](validators.allowSpendBlockValidator),
          TokenLockBlockAcceptanceManager.make[F](validators.tokenLockBlockValidator),
          GlobalSnapshotStateChannelEventsProcessor.make[F](
            validators.stateChannelValidator,
            globalStateChannelManager,
            sharedServices.currencySnapshotContextFns,
            feeCalculator,
            mptStore
          ),
          sharedServices.updateNodeParametersAcceptanceManager,
          sharedServices.updateDelegatedStakeAcceptanceManager,
          sharedServices.updateNodeCollateralAcceptanceManager,
          validators.spendActionValidator,
          validators.pricingUpdateValidator,
          sharedServices.priceStateUpdater,
          collateral,
          sharedCfg.delegatedStaking.withdrawalTimeLimit
            .getOrElse(sharedCfg.environment, EpochProgress.MinValue),
          mptStore,
          loggerBundle
        )

      consensusStorage <- ConsensusStorage.make[
        F,
        GlobalSnapshotEvent,
        GlobalSnapshotKey,
        GlobalSnapshotArtifact,
        GlobalSnapshotContext,
        GlobalSnapshotStatus,
        GlobalConsensusOutcome,
        GlobalConsensusKind
      ](appConfig.snapshot.consensus)

      consensusFunctions =
        GlobalSnapshotConsensusFunctions.make[F](
          snapshotAcceptanceManager,
          collateral,
          rewardsService,
          GlobalSnapshotEventCutter.make(
            appConfig.snapshot.consensus.eventCutter.maxBinarySizeBytes,
            SnapshotBinaryFeeCalculator.make(appConfig.shared.feeConfigs, mptStore)
          ),
          UpdateNodeParametersCutter.make(appConfig.snapshot.consensus.eventCutter.maxUpdateNodeParametersSize),
          appConfig.environment,
          DefaultDelegatedRewardsConfigProvider,
          sharedCfg.fieldsAddedOrdinals.tessellation3Migration
            .getOrElse(sharedCfg.environment, SnapshotOrdinal.MinValue),
          sharedCfg.fieldsAddedOrdinals.setSumFix
            .getOrElse(sharedCfg.environment, SnapshotOrdinal.MinValue),
          sharedCfg.incrementalDelegatedStakingStartingOrdinal
            .getOrElse(sharedCfg.environment, SnapshotOrdinal.MinValue),
          mptStore
        )

      stateAdvancer =
        GlobalSnapshotConsensusStateAdvancer.make(
          appConfig.snapshot.consensus,
          keyPair,
          consensusStorage,
          globalSnapshotStorage,
          consensusFunctions,
          gossip,
          restartService,
          nodeStorage,
          appConfig.shared.leavingDelay,
          lastNGlobalSnapshotStorage,
          lastGlobalSnapshotStorage,
          getGlobalSnapshotByOrdinal,
          clusterStorage,
          eventMempool,
          eventGossipClient,
          loggerBundle,
          mptStore
        )

      facilitatorSelector = FacilitatorSelector.make(
        appConfig.snapshot.consensus.maxFacilitatorCount.map(_.value)
      )

      peerQualityTracker <- PeerQualityTracker.make[F]

      tcaFilter = TrailingCommonAncestorFilter.make[F]

      // In Nakamoto mode, create state ref for VRF trigger daemon
      nakamotoStateRef <-
        if (nakamotoEnabled) {
          // Genesis time defaults to now if not provided via env
          val genesisTimeMs = sys.env.get("NAKAMOTO_GENESIS_TIME_MS").flatMap(_.toLongOption).getOrElse(System.currentTimeMillis())
          // Genesis eta (randomness seed) - use a default or env-provided value
          val genesisEta = sys.env.get("NAKAMOTO_GENESIS_ETA").map(_.getBytes).getOrElse("tessellation-nakamoto-genesis".getBytes)
          Ref.of[F, NakamotoTriggerState](NakamotoTriggerState.initial(genesisTimeMs, genesisEta)).map(Some(_))
        } else {
          Async[F].pure(None)
        }

      stateCreator =
        GlobalSnapshotConsensusStateCreator.make(
          consensusFunctions,
          consensusStorage,
          gossip,
          selfId,
          seedlist,
          facilitatorSelector,
          appConfig.snapshot.consensus.deterministicConfigHash,
          peerQualityTracker,
          tcaFilter,
          eventMempool,
          nakamotoStateRef
        )

      stateRemover =
        GlobalSnapshotConsensusStateRemover.make(
          consensusStorage,
          gossip
        )

      consensusOps = GlobalSnapshotConsensusOps.make

      stateUpdater =
        ConsensusStateUpdater.make(
          stateAdvancer,
          consensusStorage,
          consensusOps
        )

      consensusClient = ConsensusClient.make[F, GlobalSnapshotKey, GlobalConsensusOutcome](client, session)

      directPushFn = ConsensusDirectSender.makeDirectPushFn(clusterStorage, consensusClient)
      _ <- gossip.setDirectPushFn(directPushFn)

      loop <-
        ConsensusEventLoop.build[
          F,
          GlobalSnapshotEvent,
          GlobalSnapshotKey,
          GlobalSnapshotArtifact,
          GlobalSnapshotContext,
          GlobalSnapshotStatus,
          GlobalConsensusOutcome,
          GlobalConsensusKind
        ](
          selfId,
          consensusStorage,
          stateCreator,
          stateUpdater,
          stateAdvancer,
          stateRemover,
          consensusOps,
          nodeStorage,
          clusterStorage,
          consensusFunctions,
          consensusClient,
          appConfig.snapshot.consensus,
          facilitatorSelector,
          peerQualityTracker,
          nakamotoMode = nakamotoEnabled
        )

      handler = GlobalConsensusHandler.make(loop.queue)

      routes = new ConsensusRoutes[
        F,
        GlobalSnapshotKey,
        GlobalSnapshotArtifact,
        GlobalSnapshotContext,
        GlobalSnapshotStatus,
        GlobalConsensusOutcome,
        GlobalConsensusKind
      ](consensusStorage, rumorQueue)

      // In Nakamoto mode, triggerEvent is a no-op (slot clock handles all triggering)
      triggerEvent = if (nakamotoEnabled) Async[F].unit else loop.queue.offer(ConsensusCommand.FacilitateByEvent)

      // Only start BFT consensus loop in non-Nakamoto mode
      _ <- if (nakamotoEnabled) Async[F].unit else supervisor.supervise(loop.run.compile.drain)

      // In Nakamoto mode, start either the pure attestation loop or BFT trigger daemon
      _ <-
        if (nakamotoEnabled) {
          val lddConfig = {
            val default = io.constellationnetwork.schema.nakamoto.LddConfig.Default
            io.constellationnetwork.schema.nakamoto.LddConfig(
              lddCutoff = sys.env.get("NAKAMOTO_LDD_CUTOFF").flatMap(_.toIntOption).getOrElse(default.lddCutoff),
              offset = sys.env.get("NAKAMOTO_LDD_OFFSET").flatMap(_.toIntOption).getOrElse(default.offset),
              baselineDifficulty = sys.env.get("NAKAMOTO_LDD_BASELINE").flatMap(_.toDoubleOption).getOrElse(default.baselineDifficulty),
              amplitude = sys.env.get("NAKAMOTO_LDD_AMPLITUDE").flatMap(_.toDoubleOption).getOrElse(default.amplitude)
            )
          }
          val slotsPerEpoch = sys.env.get("NAKAMOTO_SLOTS_PER_EPOCH").flatMap(_.toLongOption).getOrElse(60L)
          val etaRotationSlots = sys.env.get("NAKAMOTO_ETA_ROTATION_SLOTS").flatMap(_.toLongOption).getOrElse(600L)
          val usePureAttestation = sys.env.contains("NAKAMOTO_PURE")

          if (usePureAttestation) {
            // Pure attestation mode: SnapshotLeaderLoop bypasses BFT rounds entirely
            val pureGenesisTimeMs = sys.env.get("NAKAMOTO_GENESIS_TIME_MS").flatMap(_.toLongOption).getOrElse(0L)
            for {
              nakLogger <- org.typelevel.log4cats.slf4j.Slf4jLogger.getLoggerFromName[F]("NakamotoConsensus").pure[F]
              _ <- nakLogger.info(
                s"🔧 Nakamoto config: LDD(cutoff=${lddConfig.lddCutoff}, offset=${lddConfig.offset}, baseline=${lddConfig.baselineDifficulty}, amplitude=${lddConfig.amplitude}), etaRotation=${etaRotationSlots}s, slotsPerEpoch=${slotsPerEpoch}, genesisTime=${pureGenesisTimeMs}"
              )
              stakeRegistry <- io.constellationnetwork.node.shared.domain.nakamoto.StakeRegistry.equalWeight[F]
              _ <- stakeRegistry.updateValidators(seedlist.map(_.map(_.peerId)).getOrElse(Set(selfId)))
              tipTracker <- io.constellationnetwork.node.shared.domain.nakamoto.TipTracker.make[F](stakeRegistry)
              chainStore <- {
                val chainSelection = io.constellationnetwork.node.shared.domain.nakamoto.ChainSelection.make[F](tipTracker)
                io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore
                  .make[F](globalSnapshotStorage, chainSelection, tipTracker)
              }
              lastKnownSlotRef <- cats.effect.kernel.Ref.of[F, Option[Long]](None)
              // Shared epoch state: VRF outputs from ALL sources accumulate here for eta rotation
              genesisEta = {
                val rawPrivKey: Array[Byte] = keyPair.getPrivate match {
                  case ecKey: java.security.interfaces.ECPrivateKey =>
                    val bytes = ecKey.getS.toByteArray
                    if (bytes.length > 32) bytes.drop(bytes.length - 32)
                    else if (bytes.length < 32) Array.fill(32 - bytes.length)(0.toByte) ++ bytes
                    else bytes
                  case other => other.getEncoded.takeRight(32)
                }
                io.constellationnetwork.security.vrf.VrfKeyDeriver.deriveVrfSeed(rawPrivKey).take(32)
              }
              epochStateRef <- cats.effect.kernel.Ref
                .of[F, io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SharedEpochState](
                  io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SharedEpochState.initial(genesisEta)
                )
              sidecarConfig = io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient.SidecarConfig(
                host = sys.env.getOrElse("SIDECAR_HOST", "127.0.0.1"),
                grpcPort = sys.env.get("SIDECAR_GRPC_PORT").flatMap(_.toIntOption).getOrElse(50051)
              )
              allocatedPair <- io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
                .makeResource[F](sidecarConfig)
                .allocated
              sidecarClient = allocatedPair._1
              _ <- supervisor.supervise(
                io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SnapshotLeaderLoop
                  .run[F](
                    consensusFns = consensusFunctions,
                    snapshotStorage = globalSnapshotStorage,
                    chainStore = chainStore,
                    lastGlobalSnapshotStorage = lastGlobalSnapshotStorage,
                    lastNGlobalSnapshotStorage = lastNGlobalSnapshotStorage,
                    eventMempool = eventMempool,
                    sidecarClient = sidecarClient,
                    tipTracker = tipTracker,
                    stakeRegistry = stakeRegistry,
                    nodeStorage = nodeStorage,
                    keyPair = keyPair,
                    selfId = selfId,
                    lddConfig = lddConfig,
                    slotsPerEpoch = slotsPerEpoch,
                    etaRotationSlots = etaRotationSlots,
                    lastKnownSlotRef = lastKnownSlotRef,
                    epochStateRef = epochStateRef,
                    genesisTimeMs = pureGenesisTimeMs
                  )
                  .compile
                  .drain
              )
              // Start NakamotoSyncDaemon: receives snapshots + attestations from gossip
              _ <- supervisor.supervise(
                io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoSyncDaemon
                  .run[F](
                    channel = sidecarClient.channel,
                    chainStore = chainStore,
                    nodeStorage = nodeStorage,
                    tipTracker = tipTracker,
                    stakeRegistry = stakeRegistry,
                    sidecarClient = sidecarClient,
                    selfId = selfId,
                    lddConfig = lddConfig,
                    lastKnownSlotRef = lastKnownSlotRef,
                    epochStateRef = epochStateRef,
                    etaRotationSlots = etaRotationSlots,
                    consensusFns = consensusFunctions,
                    snapshotStorage = globalSnapshotStorage
                  )
                  .compile
                  .drain
              )
            } yield ()
          } else {
            // BFT trigger mode: VRF triggers fed into existing round system
            nakamotoStateRef match {
              case Some(stateRef) =>
                supervisor
                  .supervise(
                    NakamotoTriggerDaemon
                      .run[F](
                        consensusQueue = loop.queue,
                        stateRef = stateRef,
                        keyPair = keyPair,
                        selfId = selfId,
                        lddConfig = lddConfig,
                        slotsPerEpoch = slotsPerEpoch
                      )
                      .compile
                      .drain
                  )
                  .void
              case _ => Async[F].unit
            }
          }
        } else Async[F].unit
      consensus = new Consensus(
        handler,
        consensusStorage,
        loop.manager,
        routes,
        consensusFunctions,
        Some(loop.healthRef),
        Some(triggerEvent)
      )
    } yield consensus
}
