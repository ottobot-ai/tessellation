package io.constellationnetwork.dag.l1

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.BuildInfo
import io.constellationnetwork.dag.l1.cli.method._
import io.constellationnetwork.dag.l1.config.types._
import io.constellationnetwork.dag.l1.domain.snapshot.programs.DAGSnapshotProcessor
import io.constellationnetwork.dag.l1.http.p2p.P2PClient
import io.constellationnetwork.dag.l1.infrastructure.block.rumor.handler.blockRumorHandler
import io.constellationnetwork.dag.l1.infrastructure.swap.rumor.handler.allowSpendBlockRumorHandler
import io.constellationnetwork.dag.l1.infrastructure.tokenlock.rumor.handler.tokenLockBlockRumorHandler
import io.constellationnetwork.dag.l1.modules._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.ext.kryo._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.app._
import io.constellationnetwork.node.shared.app.{DagL1 => DagL1Layer}
import io.constellationnetwork.node.shared.ext.pureconfig._
import io.constellationnetwork.node.shared.infrastructure.DagL1
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
import io.constellationnetwork.node.shared.infrastructure.gossip.{GossipDaemon, RumorHandlers}
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.LastNGlobalSnapshotStorage
import io.constellationnetwork.node.shared.resources.MkHttpServer
import io.constellationnetwork.node.shared.resources.MkHttpServer.ServerName
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.cluster.ClusterId
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.node.NodeState.SessionStarted
import io.constellationnetwork.schema.semver.TessellationVersion
import io.constellationnetwork.schema.tokenLock.TokenLockLimitsConfig
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.shared.{SharedKryoRegistrationIdRange, sharedKryoRegistrar}

import com.monovore.decline.Opts
import eu.timepit.refined.auto._
import eu.timepit.refined.boolean.Or
import eu.timepit.refined.pureconfig._
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._

object Main
    extends TessellationIOApp[Run](
      "dag-l1",
      "DAG L1 node",
      ClusterId("17e78993-37ea-4539-a4f3-039068ea1e92"),
      version = TessellationVersion.unsafeFrom(BuildInfo.version),
      layer = DagL1Layer
    ) {

  val opts: Opts[Run] = cli.method.opts(DagL1)

  protected val configFiles: List[String] = List("dag-l1.conf")

  type KryoRegistrationIdRange = DagL1KryoRegistrationIdRange Or SharedKryoRegistrationIdRange

  val kryoRegistrar: Map[Class[_], KryoRegistrationId[KryoRegistrationIdRange]] =
    dagL1KryoRegistrar.union(sharedKryoRegistrar)

  def run(method: Run, nodeShared: NodeShared[IO, Run]): Resource[IO, Unit] = {
    import nodeShared._

    for {
      cfgR <- loadConfigAs[AppConfigReader].asResource
      implicit0(logger: SelfAwareStructuredLogger[IO]) = Slf4jLogger.getLoggerFromName[IO](this.getClass.getName)
      cfg = method.appConfig(cfgR, sharedConfig)
      implicit0(withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit) =
        io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.some(
          cfg.shared.delegatedStaking.withdrawalTimeLimit
            .getOrElse(cfg.environment, io.constellationnetwork.schema.epoch.EpochProgress.MinValue)
        )

      queues <- Queues.make[IO](sharedQueues).asResource
      validators = hasherSelector.withCurrent { implicit hasher =>
        Validators
          .make[IO, GlobalSnapshotStateProof, GlobalIncrementalSnapshot, GlobalSnapshotInfo](
            cfg.shared,
            seedlist,
            cfg.transactionLimit,
            None,
            Hasher.forKryo[IO],
            None
          )
      }
      storages <- hasherSelector.withCurrent { implicit hasher =>
        Storages
          .make[IO, GlobalSnapshotStateProof, GlobalIncrementalSnapshot, GlobalSnapshotInfo](
            sharedStorages,
            method.l0Peer,
            validators.transactionContextual,
            validators.allowSpendContextual,
            validators.tokenLockContextual
          )
          .asResource
      }
      p2pClient = P2PClient.make[IO](
        sharedConfig,
        sharedP2PClient,
        sharedResources.client,
        currencyPathPrefix = "dag"
      )
      maybeMajorityPeerIds <- getMajorityPeerIds[IO](
        nodeShared.prioritySeedlist,
        cfg.priorityPeerIds,
        cfg.environment
      ).asResource

      // Sidecar gRPC client for libp2p GossipSub (#196). Replaces the
      // single-peer HTTP POST path from Swap.sendBlockToL0 with a durable
      // outbox publish. Held as a Resource so the gRPC channel is shut
      // down cleanly on app teardown.
      sidecarClient <- SidecarClient.makeResource[IO](
        SidecarClient.SidecarConfig(
          host = sys.env.getOrElse("SIDECAR_HOST", "127.0.0.1"),
          grpcPort = sys.env.get("SIDECAR_GRPC_PORT").flatMap(_.toIntOption).getOrElse(50051)
        )
      )
      services = Services.make[IO, GlobalSnapshotStateProof, GlobalIncrementalSnapshot, GlobalSnapshotInfo, Run](
        storages,
        storages.lastSnapshot,
        storages.l0Cluster,
        validators,
        sharedServices,
        p2pClient,
        cfg,
        maybeMajorityPeerIds,
        Hasher.forKryo[IO],
        sharedStorages
      )

      snapshotProcessor = DAGSnapshotProcessor.make(
        storages.address,
        storages.block,
        sharedStorages.lastGlobalSnapshot,
        sharedStorages.lastNGlobalSnapshot,
        storages.transaction,
        storages.allowSpend,
        storages.tokenLock,
        sharedServices.globalSnapshotContextFns,
        Hasher.forKryo[IO],
        services.globalL0.pullGlobalSnapshot,
        services.globalL0,
        storages.globalL0Alignment,
        sharedStorages.mptStore
      )
      programs = Programs.make(sharedPrograms, p2pClient, storages, snapshotProcessor)

      rumorHandler = RumorHandlers
        .make[IO](storages.cluster, services.localHealthcheck, sharedStorages.forkInfo)
        .handlers <+>
        blockRumorHandler[IO](queues.peerBlock) <+>
        allowSpendBlockRumorHandler[IO](queues.allowSpendBlocks) <+>
        tokenLockBlockRumorHandler[IO](queues.tokenLocksBlocks)

      _ <- Daemons
        .start(storages, services)
        .asResource

      api = HttpApi
        .make[IO, GlobalSnapshotStateProof, GlobalIncrementalSnapshot, GlobalSnapshotInfo, Run](
          storages,
          queues,
          keyPair.getPrivate,
          services,
          programs,
          nodeShared.nodeId,
          TessellationVersion.unsafeFrom(BuildInfo.version),
          cfg.http,
          Hasher.forKryo[IO],
          validators,
          TokenLockLimitsConfig(
            sharedConfig.delegatedStaking.maxTokenLocksPerAddress,
            sharedConfig.delegatedStaking.minTokenLockAmount
          ),
          sharedConfig
        )
      _ <- MkHttpServer[IO].newEmber(ServerName("public"), cfg.http.publicHttp, api.publicApp)
      _ <- MkHttpServer[IO].newEmber(ServerName("p2p"), cfg.http.p2pHttp, api.p2pApp)
      _ <- MkHttpServer[IO].newEmber(ServerName("cli"), cfg.http.cliHttp, api.cliApp)

      // (#196 follow-up) dl1 → gl0 send-block hop: publish through the sidecar
      // durable outbox instead of the prior single-peer HTTP POST lottery.
      // Serialization mirrors the AllowSpendBlock path — JsonSerializer[F]
      // produces the canonical JSON+Brotli bytes that gl0 receivers
      // round-trip on the same typeclass. Errors logged and swallowed:
      // gossip is fire-and-forget, the outbox TTL is the safety net.
      sendDAGBlockToL0Fn = (signed: io.constellationnetwork.security.signature.Signed[io.constellationnetwork.schema.Block]) =>
        JsonSerializer[IO]
          .serialize(signed)
          .flatMap(sidecarClient.publishDAGBlock)
          .flatMap { resp =>
            if (resp.ok) IO.unit
            else logger.warn(s"Sidecar publishDAGBlock returned ok=false: ${resp.error}")
          }
          .handleErrorWith(e => logger.warn(e)("Error publishing DAGBlock to sidecar"))

      sendTokenLockBlockToL0Fn = (signed: io.constellationnetwork.security.signature.Signed[
        io.constellationnetwork.schema.tokenLock.TokenLockBlock
      ]) =>
        JsonSerializer[IO]
          .serialize(signed)
          .flatMap(sidecarClient.publishTokenLockBlock)
          .flatMap { resp =>
            if (resp.ok) IO.unit
            else logger.warn(s"Sidecar publishTokenLockBlock returned ok=false: ${resp.error}")
          }
          .handleErrorWith(e => logger.warn(e)("Error publishing TokenLockBlock to sidecar"))

      // (#196) AllowSpendBlock dl1 → gl0 send hop: lift the (still-current) #196
      // sidecar-publish path into a lambda so `dag.l1.Swap` can be parameterised the
      // same way as `dag.l1.StateChannel` and `dag.l1.TokenLock`. The cl1 → cl0
      // call site supplies an HTTP-POST lambda instead (cl0 has no sidecar).
      sendAllowSpendBlockToL0Fn = (signed: io.constellationnetwork.security.signature.Signed[
        io.constellationnetwork.schema.swap.AllowSpendBlock
      ]) =>
        JsonSerializer[IO]
          .serialize(signed)
          .flatMap(sidecarClient.publishAllowSpendBlock)
          .flatMap { resp =>
            if (resp.ok) IO.unit
            else logger.warn(s"Sidecar publishAllowSpendBlock returned ok=false: ${resp.error}")
          }
          .handleErrorWith(e => logger.warn(e)("Error publishing AllowSpendBlock to sidecar"))

      stateChannel <- StateChannel
        .make[IO, GlobalSnapshotStateProof, GlobalIncrementalSnapshot, GlobalSnapshotInfo, Run](
          cfg,
          keyPair,
          p2pClient,
          programs,
          queues,
          nodeId,
          services,
          storages,
          validators,
          Hasher.forKryo[IO],
          sendDAGBlockToL0Fn
        )
        .asResource

      alignment = GlobalSnapshotAlignment
        .make[IO, GlobalSnapshotStateProof, GlobalIncrementalSnapshot, GlobalSnapshotInfo, Run](
          services,
          programs,
          storages,
          sharedStorages
        )

      swapRuntime = hasherSelector.withCurrent { implicit hasher =>
        Swap.run(
          cfg.swap,
          storages.cluster,
          storages.lastSnapshot,
          storages.node,
          sendAllowSpendBlockToL0Fn,
          p2pClient.swapConsensusClient,
          services,
          storages.allowSpend,
          storages.allowSpendBlock,
          queues,
          validators.allowSpend,
          keyPair,
          nodeId,
          storages.globalL0Alignment
        )
      }

      tokenLockRuntime = hasherSelector.withCurrent { implicit hasher =>
        TokenLock.run(
          cfg.tokenLock,
          storages.cluster,
          storages.lastSnapshot,
          storages.node,
          sendTokenLockBlockToL0Fn,
          p2pClient.tokenLockConsensusClient,
          services,
          storages.tokenLock,
          storages.tokenLockBlock,
          queues,
          validators.tokenLock,
          keyPair,
          nodeId,
          storages.globalL0Alignment
        )
      }

      gossipDaemon = GossipDaemon.make[IO](
        storages.rumor,
        queues.rumor,
        storages.cluster,
        p2pClient.gossip,
        rumorHandler,
        validators.rumorValidator,
        services.localHealthcheck,
        nodeId,
        generation,
        cfg.gossip.daemon,
        services.collateral
      )

      _ <- alignment.performGlobalSnapshotProcessingUntilCaughtUp().asResource

      _ <- {
        method match {
          case cfg: RunInitialValidator =>
            gossipDaemon.startAsInitialValidator >>
              programs.l0PeerDiscovery.discoverFrom(cfg.l0Peer) >>
              storages.node.tryModifyState(NodeState.Initial, NodeState.ReadyToJoin) >>
              services.cluster.createSession >>
              services.session.createSession >>
              storages.node.tryModifyState(SessionStarted, NodeState.Ready) >>
              services.restart.setClusterLeaveRestartMethod(
                RunValidator(
                  cfg.keyStore,
                  cfg.alias,
                  cfg.password,
                  cfg.environment,
                  cfg.httpConfig,
                  cfg.l0Peer,
                  cfg.seedlistPath,
                  cfg.collateralAmount,
                  cfg.prioritySeedlistPath,
                  cfg.allowanceListPath
                )
              ) >>
              services.restart.setNodeForkedRestartMethod(
                RunValidatorWithJoinAttempt(
                  cfg.keyStore,
                  cfg.alias,
                  cfg.password,
                  cfg.environment,
                  cfg.httpConfig,
                  cfg.l0Peer,
                  cfg.seedlistPath,
                  cfg.collateralAmount,
                  cfg.prioritySeedlistPath,
                  _,
                  cfg.allowanceListPath
                )
              )

          case cfg: RunValidator =>
            gossipDaemon.startAsRegularValidator >>
              programs.l0PeerDiscovery.discoverFrom(cfg.l0Peer) >>
              storages.node.tryModifyState(NodeState.Initial, NodeState.ReadyToJoin) >>
              services.restart.setNodeForkedRestartMethod(
                RunValidatorWithJoinAttempt(
                  cfg.keyStore,
                  cfg.alias,
                  cfg.password,
                  cfg.environment,
                  cfg.httpConfig,
                  cfg.l0Peer,
                  cfg.seedlistPath,
                  cfg.collateralAmount,
                  cfg.prioritySeedlistPath,
                  _,
                  cfg.allowanceListPath
                )
              )

          case cfg: RunValidatorWithJoinAttempt =>
            gossipDaemon.startAsRegularValidator >>
              programs.l0PeerDiscovery.discoverFrom(cfg.l0Peer) >>
              storages.node.tryModifyState(NodeState.Initial, NodeState.ReadyToJoin) >>
              programs.joining.joinOneOf(cfg.majorityForkPeerIds) >>
              services.restart.setClusterLeaveRestartMethod(
                RunValidator(
                  cfg.keyStore,
                  cfg.alias,
                  cfg.password,
                  cfg.environment,
                  cfg.httpConfig,
                  cfg.l0Peer,
                  cfg.seedlistPath,
                  cfg.collateralAmount,
                  cfg.prioritySeedlistPath,
                  cfg.allowanceListPath
                )
              ) >>
              services.restart.setNodeForkedRestartMethod(
                RunValidatorWithJoinAttempt(
                  cfg.keyStore,
                  cfg.alias,
                  cfg.password,
                  cfg.environment,
                  cfg.httpConfig,
                  cfg.l0Peer,
                  cfg.seedlistPath,
                  cfg.collateralAmount,
                  cfg.prioritySeedlistPath,
                  _,
                  cfg.allowanceListPath
                )
              )
        }
      }.asResource
      _ <- stateChannel.runtime
        .merge(alignment.runtime())
        .merge(swapRuntime)
        .merge(tokenLockRuntime)
        .compile
        .drain
        .handleErrorWith { error =>
          logger.error(error)("An error occured during state channel runtime") >> error.raiseError[IO, Unit]
        }
        .asResource
    } yield ()
  }
}
