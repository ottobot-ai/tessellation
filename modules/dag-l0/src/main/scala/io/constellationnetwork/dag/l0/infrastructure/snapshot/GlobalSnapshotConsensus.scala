package io.constellationnetwork.dag.l0.infrastructure.snapshot

import java.security.KeyPair

import cats.Parallel
import cats.data.NonEmptySet
import cats.effect.kernel._
import cats.effect.std._
import cats.effect.syntax.all._
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

import eu.timepit.refined.auto._
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

  // GL0 is Nakamoto-only. No env var check needed — the run-nakamoto CLI command
  // is the single source of truth. Tunables come from NAKAMOTO_* env vars below.

  /** Genesis time of the Nakamoto chain — Unix epoch milliseconds at which slot 0 starts.
    *
    * This is a per-cluster constant: every node in the same Nakamoto cluster MUST agree on the same value, otherwise their slot clocks
    * drift and they will never produce overlapping VRF eligibility windows.
    *
    * Resolved once at process start, in this single place, and threaded as an already-resolved `Long` to every downstream component.
    * Override via `NAKAMOTO_GENESIS_TIME_MS` at launch time (the standard cluster-launch knob, set by the deploy tooling alongside the
    * genesis snapshot). If unset, falls back to `System.currentTimeMillis()` — only suitable for single-node dev launches; multi-node
    * clusters MUST set the env var.
    *
    * Future work: derive from the genesis snapshot itself so validators discover it from the chain instead of needing the env var. See task
    * #2 / NAKAMOTO-PLAN.md.
    */
  val nakamotoGenesisTimeMs: Long =
    sys.env.get("NAKAMOTO_GENESIS_TIME_MS").flatMap(_.toLongOption).getOrElse(System.currentTimeMillis())

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
    // #56.6: branch-aware overlay over `mptStore`. Plumbed straight through to SnapshotLeaderLoop
    // so the finality call sites (depth-k + attestation-2/3) can notify the overlay when a branch
    // becomes canonical at an ordinal. Currently a passthrough impl by default — finalize is a
    // no-op until accept() migrates to the overlay (#56.10).
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    // #56.10 Phase I (multi-tip in #115): setter on `SharedStorages` for the overlay's eviction
    // `bestTipsFn`. Invoked once the chain store has been constructed below so the overlay can walk
    // back through pending branches without dropping the canonical chain under eviction pressure
    // (#56.9). Returns the FULL set of fork tips, not just bestTip — protects canonical chain
    // ancestors during fork-recovery (#115) when the local-fork chain is bestTip.
    setBestTipsFn: F[Set[io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId]] => F[Unit],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    eventGossipClient: EventGossipClient[F, GlobalSnapshotEvent],
    loggerBundle: LoggerBundle[F],
    rumorQueue: Queue[F, Hashed[RumorRaw]],
    // Updated by SnapshotLeaderLoop after every chainStore.finalize call. Read by HttpApi
    // to expose /global-snapshots/latest/finalized-ordinal so CL0 can gate state-channel
    // -binary pruning on actual finality. Seeded with SnapshotOrdinal.MinIncrementalValue
    // (ordinal 1 = genesis); grows monotonically as finality advances.
    nakamotoFinalizedOrdinalRef: Ref[F, SnapshotOrdinal],
    // Observability seam for the chain-quality HTTP route (#138). Populated by SnapshotLeaderLoop
    // once the four FinalityTriggers are constructed; read by `FinalityTriggersRoutes` to answer
    // "which triggers qualified ord N?" without owning trigger references. Empty until the
    // leader-loop fiber has started — the route handles `None` as a 503.
    finalityTriggerViewRef: Ref[F, Option[io.constellationnetwork.node.shared.domain.nakamoto.FinalityTriggerView[F]]],
    // Invoked by NakamotoSyncDaemon when a metagraph-binary arrives via gossip.
    // Routes the binary through the same pipeline as the HTTP endpoint (stateChannelService.process).
    processMetagraphBinary: io.constellationnetwork.statechannel.StateChannelOutput => F[Unit],
    // Created in Services.make (hoisted so HTTP routes and stateChannelService can also publish).
    sidecarClient: io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient.SidecarClientAlgebra[F],
    // §1.2 Slice 3c: (peerId → KES master VK) registry loaded from L0 genesis. Empty for the CSV-
    // genesis bootstrap path (no per-operator KES VKs registered). Read by Slice 5/6 verification
    // on every incoming attestation/snapshot to confirm the sender's KES signature against the
    // genesis-registered VK without trusting the sender to ship its own VK in-band.
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F]
  )(
    implicit supervisor: Supervisor[F],
    globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): Resource[F, GlobalSnapshotConsensus[F]] =
    for {
      globalStateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager
        .make[F](stateChannelAllowanceLists, pullDelay = stateChannelPullDelay, purgeDelay = stateChannelPurgeDelay)
        .toResource

      feeCalculator = FeeCalculator.make(feeConfigs)

      // Wrap getGlobalSnapshotByOrdinal with a chainStore fallback for Nakamoto mode.
      // snapshotStorage loses ordinal index files during fork switches; chainStore has the
      // full canonical chain. The Ref breaks the ordering dependency (chainStore is created later).
      chainStoreForLookupRef <- cats.effect.kernel.Ref
        .of[F, Option[
          io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore.NakamotoChainStoreAlgebra[F]
        ]](None)
        .toResource
      getGlobalSnapshotByOrdinalWithFallback: (SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]) = {
        (ordinal: SnapshotOrdinal) =>
          getGlobalSnapshotByOrdinal(ordinal).flatMap {
            case some @ Some(_) => Async[F].pure(some: Option[Hashed[GlobalIncrementalSnapshot]])
            case None           =>
              // snapshotStorage failed (stale cache or missing ordinal file from reorg race).
              // Fall back to chainStore's in-memory store — direct ordinal scan, no disk.
              chainStoreForLookupRef.get.flatMap {
                case Some(cs) =>
                  cs.getByOrdinal(ordinal.value.value).flatMap {
                    case Some(stored) =>
                      HasherSelector[F].withCurrent(implicit h => stored.signedSnapshot.toHashed[F].map(_.some))
                    case None => Async[F].pure(None: Option[Hashed[GlobalIncrementalSnapshot]])
                  }
                case None => Async[F].pure(None: Option[Hashed[GlobalIncrementalSnapshot]])
              }
          }
      }

      snapshotAcceptanceManager <- GlobalSnapshotAcceptanceManager
        .make[F](
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
            io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore(mptStore)
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
          mptOverlay,
          loggerBundle
        )
        .toResource

      consensusStorage <- ConsensusStorage
        .make[
          F,
          GlobalSnapshotEvent,
          GlobalSnapshotKey,
          GlobalSnapshotArtifact,
          GlobalSnapshotContext,
          GlobalSnapshotStatus,
          GlobalConsensusOutcome,
          GlobalConsensusKind
        ](appConfig.snapshot.consensus)
        .toResource

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
          mptStore,
          mptOverlay
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
          getGlobalSnapshotByOrdinalWithFallback,
          clusterStorage,
          eventMempool,
          eventGossipClient,
          loggerBundle,
          mptStore
        )

      facilitatorSelector = FacilitatorSelector.make(
        appConfig.snapshot.consensus.maxFacilitatorCount.map(_.value)
      )

      peerQualityTracker <- PeerQualityTracker.make[F].toResource

      tcaFilter = TrailingCommonAncestorFilter.make[F]

      // In Nakamoto mode, create state ref for VRF trigger daemon
      nakamotoStateRef <- {
        val genesisEta = sys.env.get("NAKAMOTO_GENESIS_ETA").map(_.getBytes).getOrElse("tessellation-nakamoto-genesis".getBytes)
        Ref.of[F, NakamotoTriggerState](NakamotoTriggerState.initial(nakamotoGenesisTimeMs, genesisEta)).map(Some(_))
      }.toResource

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
      _ <- gossip.setDirectPushFn(directPushFn).toResource

      loop <-
        ConsensusEventLoop
          .build[
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
            nakamotoMode = true
          )
          .toResource

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

      // Nakamoto GL0: no BFT consensus trigger or loop — slot clock handles production
      triggerEvent = Async[F].unit

      // Nakamoto LDD + VRF config. Env vars are typed Double for backwards-compatible config; we lock
      // them into Ratio at boot via `Ratio.apply(double, prec)` so the threshold computation is exact
      // and reproducible across all JVMs/CPUs.
      lddConfig = {
        val default = io.constellationnetwork.schema.nakamoto.LddConfig.Default
        io.constellationnetwork.schema.nakamoto.LddConfig(
          lddCutoff = sys.env.get("NAKAMOTO_LDD_CUTOFF").flatMap(_.toIntOption).getOrElse(default.lddCutoff),
          offset = sys.env.get("NAKAMOTO_LDD_OFFSET").flatMap(_.toIntOption).getOrElse(default.offset),
          baselineDifficulty = sys.env
            .get("NAKAMOTO_LDD_BASELINE")
            .flatMap(_.toDoubleOption)
            .map(io.constellationnetwork.numerics.Ratio(_, io.constellationnetwork.schema.nakamoto.LddConfig.DoubleParsePrecision))
            .getOrElse(default.baselineDifficulty),
          amplitude = sys.env
            .get("NAKAMOTO_LDD_AMPLITUDE")
            .flatMap(_.toDoubleOption)
            .map(io.constellationnetwork.numerics.Ratio(_, io.constellationnetwork.schema.nakamoto.LddConfig.DoubleParsePrecision))
            .getOrElse(default.amplitude)
        )
      }
      slotsPerEpoch = sys.env.get("NAKAMOTO_SLOTS_PER_EPOCH").flatMap(_.toLongOption).getOrElse(60L)
      // R = 2550 = 10·k₁ (matches Cardano R/k ratio). Rotation is keyed on **ordinal**, not slot —
      // slots are LDD-paced and lumpy; ordinals are 1:1 with snapshots and give a stable R that
      // satisfies the Praos R ≥ 3·k₁ stability bound. See `docs/nakamoto/attestation-and-finality.md` §1.
      etaRotationSnapshots = sys.env.get("NAKAMOTO_ETA_ROTATION_SNAPSHOTS").flatMap(_.toLongOption).getOrElse(2550L)

      // Start the Nakamoto SnapshotLeaderLoop + sidecar bridge.
      //
      // This block runs in the outer Resource context so the long-lived resources it creates
      // (ChainSyncRequestQueue drainer, Dispatcher for the ChainSync gRPC server, the gRPC
      // server itself) are released when the app Resource tree tears down, instead of being
      // escaped via `.allocated` and leaked across test restarts / shutdown.
      _ <- {

        val pureGenesisTimeMs = nakamotoGenesisTimeMs
        for {
          nakLogger <- org.typelevel.log4cats.slf4j.Slf4jLogger.getLoggerFromName[F]("NakamotoConsensus").pure[F].toResource
          _ <- nakLogger
            .info(
              s"🔧 Nakamoto config: LDD(cutoff=${lddConfig.lddCutoff}, offset=${lddConfig.offset}, baseline=${lddConfig.baselineDifficulty}, amplitude=${lddConfig.amplitude}), etaRotation=${etaRotationSnapshots} snapshots, slotsPerEpoch=${slotsPerEpoch}, genesisTime=${pureGenesisTimeMs}"
            )
            .toResource
          // §1.1: switch from equal-weight to stake-weighted VRF election. `snapshotInfoR` re-reads
          // the latest GSI on every call so per-validator stake reflects the most recent
          // activeDelegatedStakes + activeNodeCollaterals. Boot path (no GSI yet) falls back to 1/N
          // inside the constructor; updateValidators / markActive / markInactive semantics are
          // unchanged from equalWeight.
          stakeRegistry <- io.constellationnetwork.node.shared.domain.nakamoto.StakeRegistry
            .stakeWeighted[F](lastGlobalSnapshotStorage.getCombined.map(_.map(_._2)))
            .toResource
          // Filter out entries marked with alias="metagraph-op". They live in the seedlist
          // so state-channel binary signature validation accepts them as known signers,
          // but they must not count as Nakamoto validators (would dilute 1/N VRF stake).
          validatorPeers = seedlist
            .map(_.collect { case e if !e.alias.exists(_.value.value == "metagraph-op") => e.peerId })
            .getOrElse(Set(selfId))
          _ <- stakeRegistry.updateValidators(validatorPeers).toResource
          tipTracker <- io.constellationnetwork.node.shared.domain.nakamoto.TipTracker.make[F](stakeRegistry).toResource
          // §1.2 Slice 3c: bootstrap an OperationalKeyMaker. Two paths:
          //   - Disk-backed (production / e2e harness): if `CL_KES_SECURE_STORE_DIR` is set, open a
          //     [[io.constellationnetwork.security.kes.SecureStore.disk]] at that directory and
          //     `OperationalKeyMaker.make` against the pre-staged `kes-sk.bin` written by the
          //     genesis generator (Slice 3b). Its master VK matches the genesis-registered VK by
          //     construction, so Slice 5 verification works end-to-end out of the box.
          //   - Fallback (dev / unit-test): no env var → in-memory store + fresh bootstrap with a
          //     SecureRandom seed. The master VK is per-JVM-run and will NOT match anything in
          //     `kesRegistry`, so Slice 5 receivers will count it as "no registry entry" — fine for
          //     local dev (warn-only) but not for cross-node verification.
          kesSecureStore <- sys.env.get("CL_KES_SECURE_STORE_DIR") match {
            case Some(dir) =>
              io.constellationnetwork.security.kes.SecureStore.disk[F](java.nio.file.Paths.get(dir)).toResource
            case None =>
              io.constellationnetwork.security.kes.SecureStore.inMemory[F].toResource
          }
          operationalKeyMaker <- sys.env.get("CL_KES_SECURE_STORE_DIR") match {
            case Some(_) =>
              // Disk path: SK was pre-staged at `<dir>/kes-sk.bin` by the genesis generator
              // (Slice 3b). Just open; do not regenerate.
              io.constellationnetwork.security.kes.OperationalKeyMaker
                .make[F](
                  secureStore = kesSecureStore,
                  keyName = "kes-sk.bin",
                  etaPeriodLength = etaRotationSnapshots.toLong
                )
            case None =>
              // Fallback: in-memory + fresh bootstrap with SecureRandom seed.
              val seed = new Array[Byte](32)
              new java.security.SecureRandom().nextBytes(seed)
              io.constellationnetwork.security.kes.OperationalKeyMaker
                .bootstrap[F](
                  secureStore = kesSecureStore,
                  keyName = "gl0-operational-kes.key",
                  seed = seed,
                  etaPeriodLength = etaRotationSnapshots.toLong
                )
          }
          // §1.2 Slice 2: bind the KES master VK (period-0 root of the super × sub tree) to the
          // operator's long-term Ed25519 key via a SHA512withECDSA signature over kesVk.value.
          // The pair (kesVk, kesVkSigByLongTerm) is the registration certificate: it lets any
          // other validator confirm that the KES VK they see in genesis (or in a future runtime
          // registration tx) was issued by this operator without anyone needing access to the
          // KES SK. KES forward security is preserved because the long-term key signs the VK,
          // not the SK — compromise of the long-term key still cannot forge KES sigs for
          // past periods (those required the corresponding SK leaf that has been erased).
          kesRegistration <- operationalKeyMaker.currentPublicKey.flatMap { kesVk =>
            io.constellationnetwork.security.signature.Signing
              .signData[F](kesVk.value)(keyPair.getPrivate)
              .map(regSig => (kesVk, regSig))
          }.toResource
          _ <- {
            val (kesVk, regSig) = kesRegistration
            val vkHex = kesVk.value.take(16).map("%02x".format(_)).mkString
            val regHex = regSig.take(16).map("%02x".format(_)).mkString
            nakLogger.info(
              s"🔐 KES bootstrap: period=0 height=${io.constellationnetwork.security.kes.OperationalKeyMaker.DefaultHeight} vk=$vkHex... step=${kesVk.step}"
            ) >>
              nakLogger.info(
                s"🔐 KES-REG: master-vk-sig=$regHex... (SHA512withECDSA over kesVk by operator long-term key, ${regSig.length}B)"
              ) >>
              // §1.2 Slice 3c: log the genesis-loaded KesRegistry size. Empty means no peers were
              // registered at genesis (CSV-genesis bootstrap, or JSON without the kesRegistrations
              // field) — Slice 5 verification will count incoming KES sigs against the empty
              // registry as "no entry" and warn-only-skip.
              kesRegistry.list.flatMap { regs =>
                nakLogger.info(s"📋 KES registry loaded: ${regs.size} peer(s) registered")
              }
          }.toResource

          // Numerics for VRF eligibility threshold. Bifrost prod precision: log1p=8, exp=38, maxIter=10000.
          // We use prec=8 / prec=38 to match Bifrost exactly. The Lentz iteration runs in exact `Ratio`
          // arithmetic so two honest nodes (different JVMs/CPUs/JIT-tiers) compute byte-identical
          // thresholds — closes the IEEE 754 nondeterminism risk.
          log1p <- io.constellationnetwork.numerics.interpreters.Log1pInterpreter.make[F](maxIterations = 10000, precision = 8).toResource
          exp <- io.constellationnetwork.numerics.interpreters.ExpInterpreter.make[F](maxIterations = 10000, precision = 38).toResource
          eligibilityChecker = io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker.make[F](log1p, exp)
          // ChainSelection needs fetchParent — but chainStore needs ChainSelection.
          // Break the cycle: create chainStore first with a lazy fetchParent that
          // uses chainStore.tipFor once it's available.
          chainStoreRef <- cats.effect.kernel.Ref
            .of[F, Option[
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore.NakamotoChainStoreAlgebra[F]
            ]](None)
            .toResource
          fetchParent = (tip: io.constellationnetwork.schema.nakamoto.ChainTip) =>
            chainStoreRef.get.flatMap {
              case Some(cs) => cs.tipFor(tip.parentHash)
              case None     => cats.Applicative[F].pure(None: Option[io.constellationnetwork.schema.nakamoto.ChainTip])
            }
          chainSelection = io.constellationnetwork.node.shared.domain.nakamoto.ChainSelection.make[F](tipTracker, fetchParent)
          chainStore <- io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore
            .make[F](globalSnapshotStorage, chainSelection, tipTracker, nakamotoFinalizedOrdinalRef)
            .toResource
          _ <- chainStoreRef.set(Some(chainStore)).toResource
          _ <- chainStoreForLookupRef.set(Some(chainStore)).toResource
          // #56.10 Phase I (multi-tip in #115): wire the overlay's eviction `bestTipsFn` to
          // `chainStore.allTips` so ancestor protection covers EVERY viable chain head — the
          // canonical bestTip AND any tentative-branch heads being followed during fork-recovery.
          // Without this, when validator commits canonical-N before chain reorgs to it, canonical-N
          // is committed but isn't yet bestTip; subsequent eviction (with lastCommittedRef pointing
          // elsewhere) drops canonical-N's branch, breaking the parent walk for canonical-N+1.
          // Using `allTips` (every leaf in `byHash`) instead of just `bestTip` keeps the canonical
          // chain alive through fork-recovery's commit-before-reorg window. The overlay captured a
          // Ref-backed closure at SharedStorages.make time; setting the ref here completes the
          // binding.
          _ <- setBestTipsFn(
            chainStore.allTips.map(
              _.map(io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(_))
            )
          ).toResource
          // #56.10 Phase H: startup base-consistency guard. Runs in the boot Resource chain
          // (after SharedStorages.make's mptStore and the chainStore just constructed above)
          // and before any request-serving — this resource block is awaited before the HTTP
          // servers start in Main.scala. A no-op for fresh nodes; fires only when crash
          // recovery left MPT base ahead of finalized.
          _ <- io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.MptBaseConsistency
            .assertBaseConsistentOrPrune[F](mptStore, chainStore.lastFinalizedOrdinal)(implicitly[Async[F]], nakLogger)
            .toResource
          // Seed chain store with the current head snapshot so gossip children can find their parent
          _ <- globalSnapshotStorage.head.flatMap {
            case Some((headSigned, headCtx)) =>
              HasherSelector[F].withCurrent { implicit hasher =>
                headSigned.toHashed[F].flatMap { hashed =>
                  nakLogger.info(
                    s"🌱 Seeding chain store with genesis: ordinal=${hashed.ordinal} hash=${hashed.hash.value.take(16)} " +
                      s"lastSnapshotHash=${hashed.lastSnapshotHash.value.take(16)}"
                  ) >>
                    chainStore
                      .store(
                        headSigned,
                        headCtx,
                        hashed.ordinal.value.value,
                        0L, // slot unknown for genesis
                        hashed.lastSnapshotHash,
                        Array.empty // no VRF output for genesis
                      )
                      .void
                }
              }
            case None =>
              Async[F].unit
          }.toResource
          lastKnownSlotRef <- cats.effect.kernel.Ref.of[F, Option[Long]](None).toResource
          // Shared epoch state: VRF outputs from ALL sources accumulate here for eta rotation
          genesisEta = {
            // Genesis eta must be identical across all nodes — derive from a fixed domain string
            // (In production, derive from genesis snapshot hash. For now, use a deterministic constant.)
            val genesisEtaSeed = "tessellation-nakamoto-genesis-eta-v1".getBytes(java.nio.charset.StandardCharsets.UTF_8)
            val genesisEtaDigest = new org.bouncycastle.crypto.digests.Blake2bDigest(256)
            genesisEtaDigest.update(genesisEtaSeed, 0, genesisEtaSeed.length)
            val genesisEtaBytes = new Array[Byte](32)
            genesisEtaDigest.doFinal(genesisEtaBytes, 0)
            genesisEtaBytes
          }
          epochStateRef <- cats.effect.kernel.Ref
            .of[F, io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SharedEpochState](
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SharedEpochState.initial(genesisEta)
            )
            .toResource
          // Shared semaphore: serialize snapshot production and gossip processing
          // so each operation sees correct parent state (Bifrost uses same pattern)
          snapshotSemaphore <- cats.effect.std.Semaphore[F](1).toResource
          productionGate <- io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate.make[F].toResource
          // Shared ChainSyncManager reference. Populated by NakamotoSyncDaemon after it constructs
          // its internal manager; read-through by the ChainSyncRequestQueue worker below so the
          // reactive walkback path piggybacks on existing hash-keyed dedup + fetch. No-op until bound.
          sharedChainSyncManagerRef <- cats.effect.kernel.Ref
            .of[F, Option[
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.ChainSyncManager.ChainSyncManagerAlgebra[F]
            ]](None)
            .toResource
          // Reactive ChainSync trigger for the finality walkback path (see
          // ChainSyncRequestQueue docs for the full rationale). Bound directly into the outer
          // Resource — the drainer fiber is torn down cleanly on app shutdown.
          chainSyncRequestQueue <- {
            val workerFn = io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.ChainSyncRequestQueue
              .walkbackWorker[F] _
            val reactiveWorker: Long => F[Unit] = ord =>
              sharedChainSyncManagerRef.get.flatMap {
                case Some(csm) => workerFn(sidecarClient.channel, csm).apply(ord)
                case None      =>
                  // Pre-binding window before NakamotoSyncDaemon publishes its ChainSyncManager.
                  // Brief in practice; the next 5s finality-monitor tick re-offers if the fork persists.
                  cats.Applicative[F].unit
              }
            io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.ChainSyncRequestQueue
              .make[F](reactiveWorker)
          }
          // sidecarClient is now created in Services.make and passed in as a parameter so that
          // the HTTP state-channel route can publish metagraph binaries via the same gRPC channel.
          // Wire rumor gossip onto the sidecar transport. Outbound: every rumor passing through
          // Gossip.spread is forwarded to the libp2p sidecar via PublishRumor. Inbound: rumors
          // received from the GossipSub mesh are deserialized back to Hashed[RumorRaw] and offered
          // to rumorQueue, where the existing GossipDaemon.consumeRumors pipeline validates and
          // dispatches them via the registered RumorHandlers — meaning BFT consensus messages,
          // Tessellation events, and any other rumor type ride sidecar transport for free.
          _ <- gossip
            .setSidecarPublishFn(
              io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarRumorBridge
                .publishFn[F](sidecarClient)
            )
            .toResource
          _ <- io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarRumorBridge
            .receive[F](sidecarClient.channel, rumorQueue)
            .toResource
          _ <- supervisor
            .supervise(
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
                  eligibilityChecker = eligibilityChecker,
                  slotsPerEpoch = slotsPerEpoch,
                  etaRotationSnapshots = etaRotationSnapshots,
                  lastKnownSlotRef = lastKnownSlotRef,
                  epochStateRef = epochStateRef,
                  genesisTimeMs = pureGenesisTimeMs,
                  snapshotSemaphore = snapshotSemaphore,
                  productionGate = productionGate,
                  mptStore = mptStore,
                  mptOverlay = mptOverlay,
                  nakamotoFinalizedOrdinalRef = nakamotoFinalizedOrdinalRef,
                  chainSyncRequestQueue = chainSyncRequestQueue,
                  finalityTriggerViewRef = finalityTriggerViewRef,
                  // §1.2 Slice 5/6: parallel-sign attestations + snapshots with KES.
                  operationalKeyMaker = operationalKeyMaker
                )
                .compile
                .drain
            )
            .toResource
          // Start Nakamoto metrics publisher (periodic chain-state gauges)
          _ <- supervisor
            .supervise(
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoMetrics
                .run[F](
                  chainStore = chainStore,
                  tipTracker = tipTracker,
                  genesisTimeMs = pureGenesisTimeMs
                )
                .compile
                .drain
            )
            .toResource
          // §1.2 Slice 8: KES period evolution aligned with eta rotation cadence.
          //
          // Sender-side sign-time evolution (Slice 5/6) auto-evolves the key on every signAt call,
          // so forward security is preserved *whenever the validator signs*. But a quiet validator
          // (no VRF wins) holds stale period-N bytes until its next sign — the exposure window is
          // 1/relativeStake snapshots in expectation. This fiber tightens that window to a single
          // poll interval by proactively burning past periods at each eta boundary.
          //
          // Period derivation matches `EtaCalculation.rotationPeriod(ordinal, etaRotationSnapshots)`
          // (snapshot-indexed, not slot-indexed — see attestation-and-finality.md §1). Aligns with
          // [[project_consensus_epoch_staggering]]'s "KES periods aligned with eta cadence" rule.
          //
          // Skipped entirely on the in-memory fallback path (no CL_KES_SECURE_STORE_DIR) — the
          // evolution would write to nowhere persistent and just waste cycles. Disk path is the
          // only one where the proactive evolution materially shrinks the exposure window.
          lastEvolvedKesPeriodRef <- Ref.of[F, Int](0).toResource
          _ <- supervisor
            .supervise(
              fs2.Stream
                .awakeEvery[F](scala.concurrent.duration.DurationInt(2).seconds)
                .evalMap { _ =>
                  for {
                    finalizedOrd <- nakamotoFinalizedOrdinalRef.get
                    currentPeriod = io.constellationnetwork.node.shared.domain.nakamoto.EtaCalculation
                      .rotationPeriod(finalizedOrd.value.value, etaRotationSnapshots.toLong)
                      .toInt
                    lastEvolved <- lastEvolvedKesPeriodRef.get
                    _ <-
                      if (currentPeriod > lastEvolved)
                        operationalKeyMaker.evolveTo(currentPeriod).flatMap {
                          case Right(_) =>
                            lastEvolvedKesPeriodRef.set(currentPeriod) >>
                              nakLogger.info(
                                s"🔐 KES period rotated: $lastEvolved → $currentPeriod (finalizedOrd=${finalizedOrd.value.value}, etaRotationSnapshots=$etaRotationSnapshots)"
                              ) >>
                              Metrics[F].incrementCounter("dag_nakamoto_kes_period_rotations_total") >>
                              Metrics[F].updateGauge("dag_nakamoto_kes_current_period", currentPeriod.toLong)
                          case Left(err) =>
                            nakLogger.warn(s"⚠️ KES evolveTo($currentPeriod) failed: $err")
                        }
                      else cats.Applicative[F].unit
                  } yield ()
                }
                .compile
                .drain
            )
            .toResource
          // Start ChainSyncInbound gRPC server — serves local chain data to peers
          // via the sidecar. The sidecar calls this server when peers request
          // snapshots or chain points for the ChainSync protocol.
          chainSyncDispatcher <- Dispatcher.sequential[F]
          chainSyncServer = io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.ChainSyncServer
            .make[F](chainStore, globalSnapshotStorage, chainSyncDispatcher)(
              implicitly,
              implicitly,
              scala.concurrent.ExecutionContext.global
            )
          // Server lifecycle: start it on Resource acquire, shut it down cleanly on release so
          // the listen socket is returned to the OS instead of being held by a leaked Java server.
          // `shutdown()` requests graceful close; we give it a short grace period and then drop on
          // timeout — we don't want a hung RPC from a misbehaving peer to block app teardown.
          _ <- Resource.make(
            Async[F].blocking {
              val grpcServer = io.grpc.ServerBuilder
                .forPort(50053)
                .addService(
                  io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar.ChainSyncInboundGrpc
                    .bindService(chainSyncServer, scala.concurrent.ExecutionContext.global)
                )
                .build()
              grpcServer.start()
              grpcServer
            }
          )(srv =>
            Async[F].blocking {
              srv.shutdown()
              srv.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
              ()
            }
              .handleError(_ => ())
          )
          _ <- nakLogger.info("🔗 ChainSyncInbound gRPC server started on port 50053").toResource

          // Start NakamotoSyncDaemon: receives snapshots + attestations from gossip
          _ <- supervisor
            .supervise(
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoSyncDaemon
                .run[F](
                  channel = sidecarClient.channel,
                  chainStore = chainStore,
                  nodeStorage = nodeStorage,
                  tipTracker = tipTracker,
                  stakeRegistry = stakeRegistry,
                  sidecarClient = sidecarClient,
                  selfId = selfId,
                  keyPair = keyPair,
                  lddConfig = lddConfig,
                  eligibilityChecker = eligibilityChecker,
                  lastKnownSlotRef = lastKnownSlotRef,
                  epochStateRef = epochStateRef,
                  etaRotationSnapshots = etaRotationSnapshots,
                  consensusFns = consensusFunctions,
                  snapshotStorage = globalSnapshotStorage,
                  lastGlobalSnapshotStorage = lastGlobalSnapshotStorage,
                  lastNGlobalSnapshotStorage = lastNGlobalSnapshotStorage,
                  snapshotSemaphore = snapshotSemaphore,
                  productionGate = productionGate,
                  mptStore = mptStore,
                  mptOverlay = mptOverlay,
                  eventMempool = eventMempool,
                  dataDir = java.nio.file.Paths.get(sys.env.getOrElse("TESSELLATION_DATA_DIR", "/tessellation/data")),
                  processMetagraphBinary = processMetagraphBinary,
                  sharedChainSyncManagerRef = sharedChainSyncManagerRef,
                  // §1.2 Slice 5/6: sign attestations + warn-only verify incoming KES sigs.
                  operationalKeyMaker = operationalKeyMaker,
                  kesRegistry = kesRegistry
                )
                .compile
                .drain
            )
            .toResource

          // P-11 (#141): re-bootstrap orchestrator. Periodic ticker that detects when this
          // node has self-finalized a divergent fork and got locked out of canonical recovery
          // (the "fork-recovery deadlock"). Gated by `NAKAMOTO_REBOOTSTRAP_ENABLED` (default
          // FALSE) — production deployments turn it on per-node once iter e2e validates no
          // spurious fires. Disabled: emits a single INFO at startup and otherwise no-op.
          _ <- supervisor
            .supervise(
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.RebootstrapOrchestrator
                .run[F](
                  chainStore = chainStore,
                  tipTracker = tipTracker,
                  mptOverlay = mptOverlay,
                  productionGate = productionGate
                )
                .compile
                .drain
            )
            .toResource

          // Check for persisted backfill cursor from a previous session (crash recovery).
          // If found, resume backfill with production paused until it completes.
          backfillDataDir = java.nio.file.Paths.get(sys.env.getOrElse("TESSELLATION_DATA_DIR", "/tessellation/data"))
          existingCursor <- io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.BackfillDaemon
            .loadCursor[F](backfillDataDir)
            .toResource
          _ <- (existingCursor match {
            case Some(cursor) =>
              nakLogger.info(
                s"🔄 Resuming backfill from crash: ordinal ${cursor.currentOrdinal} → ${cursor.targetOrdinal} " +
                  s"(started at ${cursor.startedAtOrdinal})"
              ) >>
                supervisor
                  .supervise(
                    io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.BackfillDaemon
                      .run[F](cursor, sidecarClient.channel, globalSnapshotStorage, productionGate, backfillDataDir)
                      .handleErrorWith(e => nakLogger.warn(s"Backfill daemon failed on resume: ${e.getMessage}"))
                  )
                  .void
            case None =>
              Async[F].unit
          }).toResource
        } yield ()
      }
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
