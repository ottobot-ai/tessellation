package io.constellationnetwork.dag.l0.modules

import java.security.KeyPair

import cats.Parallel
import cats.data.NonEmptySet
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.{Random, Supervisor}
import cats.effect.syntax.all._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._

import io.constellationnetwork.dag.l0.config.types.AppConfig
import io.constellationnetwork.dag.l0.domain.cell.L0Cell
import io.constellationnetwork.dag.l0.domain.statechannel.StateChannelService
import io.constellationnetwork.dag.l0.infrastructure.mempool.GlobalEventMempool
import io.constellationnetwork.dag.l0.infrastructure.rewards._
import io.constellationnetwork.dag.l0.infrastructure.snapshot._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event.GlobalSnapshotEvent
import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.config.DefaultDelegatedRewardsConfigProvider
import io.constellationnetwork.node.shared.config.types.SharedConfig
import io.constellationnetwork.node.shared.domain.cluster.services.{Cluster, Session}
import io.constellationnetwork.node.shared.domain.collateral.Collateral
import io.constellationnetwork.node.shared.domain.gossip.Gossip
import io.constellationnetwork.node.shared.domain.healthcheck.LocalHealthcheck
import io.constellationnetwork.node.shared.domain.rewards.Rewards
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityGate
import io.constellationnetwork.node.shared.domain.snapshot.services.AddressService
import io.constellationnetwork.node.shared.infrastructure.collateral.MptStoreCollateral
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
import io.constellationnetwork.node.shared.infrastructure.delegatedStake.{RewardsInfoCalculator, RewardsInfoStorage}
import io.constellationnetwork.node.shared.infrastructure.gossip.event.{EventGossipClient, RecoveryPeerHint}
import io.constellationnetwork.node.shared.infrastructure.mempool.EventMempool
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.node.RestartService
import io.constellationnetwork.node.shared.infrastructure.snapshot.services.AddressService
import io.constellationnetwork.node.shared.logger.LoggerBundle
import io.constellationnetwork.node.shared.modules.{SharedServices, SharedStorages, SharedValidators}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.{Hasher, HasherSelector, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelOutput

import org.http4s.client.Client
import org.typelevel.log4cats.slf4j.Slf4jLogger

object Services {

  def make[F[
    _
  ]: Async: FinalityGate: Parallel: Random: KryoSerializer: JsonSerializer: HasherSelector: SecurityProvider: Metrics: Supervisor, R <: CliMethod](
    sharedCfg: SharedConfig,
    sharedServices: SharedServices[F, R],
    sharedStorages: SharedStorages[F],
    queues: Queues[F],
    storages: Storages[F],
    validators: SharedValidators[F],
    client: Client[F],
    session: Session[F],
    seedlist: Option[Set[SeedlistEntry]],
    stateChannelAllowanceLists: Option[Map[Address, NonEmptySet[PeerId]]],
    selfId: PeerId,
    keyPair: KeyPair,
    cfg: AppConfig,
    txHasher: Hasher[F],
    loggerBundle: LoggerBundle[F],
    nakamotoFinalizedOrdinalRef: Ref[F, SnapshotOrdinal],
    finalityTriggerViewRef: Ref[F, Option[io.constellationnetwork.node.shared.domain.nakamoto.FinalityTriggerView[F]]],
    // §1.2 Slice 3c: KesRegistry loaded from L0 genesis (or empty for CSV-genesis). Threaded
    // through to GlobalSnapshotConsensus.make.
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): Resource[F, Services[F, R]] =
    for {
      classicRewards <- Rewards
        .make[F](
          cfg.rewards,
          ProgramsDistributor.make,
          FacilitatorDistributor.make
        )
        .pure[F]
        .toResource

      delegatorRewards <- HasherSelector[F].withCurrent { implicit hasher =>
        GlobalDelegatedRewardsDistributor
          .make[F](
            cfg.environment,
            DefaultDelegatedRewardsConfigProvider.getConfig()
          )
          .pure[F]
      }.toResource

      rewardsInfoCalculator = RewardsInfoCalculator.make(delegatorRewards)

      rewardsInfoStorage <- RewardsInfoStorage.make.toResource

      rewardsService = RewardsService(
        classicRewards,
        delegatorRewards,
        rewardsInfoCalculator,
        rewardsInfoStorage
      )

      eventMempoolService <- HasherSelector[F].withCurrent { implicit hasher =>
        GlobalEventMempool.make[F](
          GlobalEventMempool.defaultConfig
        )
      }.toResource

      eventGossipClient = EventGossipClient.make[F, GlobalSnapshotEvent](client, session)

      // Sidecar client for libp2p GossipSub. Created here (rather than inside consensus)
      // so stateChannelService and HTTP routes can also publish — specifically, the
      // HTTP state-channel endpoint broadcasts received metagraph binaries to peer GL0s
      // to work around CL0's single-push-per-binary retry cap starvation.
      //
      // Held as a Resource so the gRPC channel is shut down cleanly on app teardown
      // instead of being escaped via `.allocated` (prior behaviour leaked the channel).
      sidecarConfig = SidecarClient.SidecarConfig(
        host = sys.env.getOrElse("SIDECAR_HOST", "127.0.0.1"),
        grpcPort = sys.env.get("SIDECAR_GRPC_PORT").flatMap(_.toIntOption).getOrElse(50051)
      )
      sidecarClient <- SidecarClient.makeResource[F](sidecarConfig)

      // stateChannelService must exist before consensus so that the Nakamoto
      // gossip daemon can route metagraph-binary gossip messages through the
      // same acceptance pipeline that the HTTP POST endpoint uses.
      stateChannelService = StateChannelService
        .make[F](
          L0Cell.mkL0Cell(
            queues.l1Output,
            queues.stateChannelOutput,
            queues.updateNodeParametersOutput,
            queues.delegatedStakeOutput,
            queues.nodeCollateralOutput
          ),
          validators.stateChannelValidator,
          sharedStorages.mptStore
        )

      // Callback for the Nakamoto gossip daemon to process incoming metagraph
      // binaries. Mirrors the HTTP route in StateChannelRoutes: wrap as
      // StateChannelOutput, fetch head context, delegate to
      // stateChannelService.process. Errors are logged and swallowed — gossip
      // is fire-and-forget, no sender to reply to. DO NOT finality-gate this
      // path: in Nakamoto mode head is ~always ahead of finalized during
      // active production, so gating here would reject every incoming binary.
      // The validator handles the race via forcedGlobalSyncView at snapshot
      // production time.
      processMetagraphBinary = (output: StateChannelOutput) =>
        storages.globalSnapshot.head.flatMap {
          case Some((snapshot, info)) =>
            HasherSelector[F].withCurrent { implicit hasher =>
              stateChannelService.process(output, (snapshot, info))
            }.void
          case None => Async[F].unit
        }

      // (#196) Sink for inbound AllowSpendBlock gossip — same queue
      // GlobalSnapshotEventsPublisherDaemon drains into the event mempool. The
      // HTTP route that previously did this offer (AllowSpendBlockRoutes) was
      // removed in the same change; this is now the sole entry point.
      enqueueAllowSpendBlock = (signed: io.constellationnetwork.security.signature.Signed[
        io.constellationnetwork.schema.swap.AllowSpendBlock
      ]) => queues.l1AllowSpendOutput.offer(signed)

      consensus <- HasherSelector[F].withCurrent { implicit hs =>
        GlobalSnapshotConsensus
          .make[F, R](
            sharedCfg,
            sharedServices.gossip,
            selfId,
            keyPair,
            seedlist,
            cfg.collateral.amount,
            storages.cluster,
            storages.node,
            storages.globalSnapshot,
            validators,
            sharedServices,
            cfg,
            stateChannelPullDelay = cfg.stateChannel.pullDelay,
            stateChannelPurgeDelay = cfg.stateChannel.purgeDelay,
            stateChannelAllowanceLists,
            feeConfigs = cfg.shared.feeConfigs,
            client,
            session,
            rewardsService,
            txHasher,
            sharedServices.restart,
            sharedStorages.lastNGlobalSnapshot,
            sharedStorages.lastGlobalSnapshot,
            storages.globalSnapshot.getHashed,
            sharedStorages.mptStore,
            sharedStorages.mptOverlay,
            sharedStorages.setBestTipsFn,
            eventMempoolService,
            eventGossipClient,
            loggerBundle,
            queues.rumor,
            nakamotoFinalizedOrdinalRef,
            finalityTriggerViewRef,
            processMetagraphBinary,
            enqueueAllowSpendBlock,
            sidecarClient,
            kesRegistry
          )
      }
      addressService = AddressService.make[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo](
        cfg.shared.addresses,
        storages.globalSnapshot,
        Some(sharedStorages.mptStore)
      )
      collateralService = MptStoreCollateral.make[F](cfg.collateral, sharedStorages.mptStore)
      recoveryPeerHintService <- RecoveryPeerHint.make[F].toResource
    } yield
      new Services[F, R](
        localHealthcheck = sharedServices.localHealthcheck,
        cluster = sharedServices.cluster,
        session = sharedServices.session,
        gossip = sharedServices.gossip,
        consensus = consensus,
        address = addressService,
        collateral = collateralService,
        stateChannel = stateChannelService,
        restart = sharedServices.restart,
        rewards = rewardsService,
        recoveryPeerHint = recoveryPeerHintService,
        eventMempool = eventMempoolService,
        sidecarClient = sidecarClient,
        finalityTriggerViewRef = finalityTriggerViewRef
      ) {}
}

sealed abstract class Services[F[_], R <: CliMethod] private (
  val localHealthcheck: LocalHealthcheck[F],
  val cluster: Cluster[F],
  val session: Session[F],
  val gossip: Gossip[F],
  val consensus: GlobalSnapshotConsensus[F],
  val address: AddressService[F, GlobalIncrementalSnapshot],
  val collateral: Collateral[F],
  val stateChannel: StateChannelService[F],
  val restart: RestartService[F, R],
  val rewards: RewardsService[F],
  val recoveryPeerHint: RecoveryPeerHint[F],
  val eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
  val sidecarClient: SidecarClient.SidecarClientAlgebra[F],
  // Observability seam for /global-snapshots/{ord}/finality-triggers (#138). Set once by
  // SnapshotLeaderLoop after trigger construction; read by FinalityTriggersRoutes. The
  // route returns 503 while the Ref is empty (pre-startup window).
  val finalityTriggerViewRef: Ref[F, Option[io.constellationnetwork.node.shared.domain.nakamoto.FinalityTriggerView[F]]]
)
