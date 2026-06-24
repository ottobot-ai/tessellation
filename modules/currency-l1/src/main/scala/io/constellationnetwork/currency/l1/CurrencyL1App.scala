package io.constellationnetwork.currency.l1

import cats.effect.kernel.Ref
import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.currency.dataApplication.{BaseDataApplicationL1Service, L1NodeContext}
import io.constellationnetwork.currency.l1.cli.method
import io.constellationnetwork.currency.l1.cli.method._
import io.constellationnetwork.currency.l1.domain.snapshot.programs.CurrencySnapshotProcessor
import io.constellationnetwork.currency.l1.http.p2p.P2PClient
import io.constellationnetwork.currency.l1.modules._
import io.constellationnetwork.currency.l1.node.L1NodeContext
import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.dag.l1._
import io.constellationnetwork.dag.l1.config.types._
import io.constellationnetwork.dag.l1.domain.transaction.{CustomContextualTransactionValidator, TransactionFeeEstimator}
import io.constellationnetwork.dag.l1.http.p2p.{P2PClient => DAGP2PClient}
import io.constellationnetwork.dag.l1.infrastructure.block.rumor.handler.blockRumorHandler
import io.constellationnetwork.dag.l1.infrastructure.swap.rumor.handler.allowSpendBlockRumorHandler
import io.constellationnetwork.dag.l1.infrastructure.tokenlock.rumor.handler.tokenLockBlockRumorHandler
import io.constellationnetwork.dag.l1.modules.{Daemons => DAGL1Daemons, Queues => DAGL1Queues, Validators => DAGL1Validators}
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.ext.kryo.{KryoRegistrationId, MapRegistrationId}
import io.constellationnetwork.json.{JsonBrotliBinarySerializer, JsonSerializer}
import io.constellationnetwork.node.shared.app.{CurrencyL1 => CurrencyL1Layer, _}
import io.constellationnetwork.node.shared.ext.pureconfig._
import io.constellationnetwork.node.shared.infrastructure.gossip.{GossipDaemon, RumorHandlers}
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.LastNGlobalSnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.{CurrencyL1, DataL1}
import io.constellationnetwork.node.shared.resources.MkHttpServer
import io.constellationnetwork.node.shared.resources.MkHttpServer.ServerName
import io.constellationnetwork.node.shared.{NodeSharedOrSharedRegistrationIdRange, nodeSharedKryoRegistrar}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.cluster.ClusterId
import io.constellationnetwork.schema.nakamoto.follow.ConsumedFieldState
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.node.NodeState.SessionStarted
import io.constellationnetwork.schema.semver.{MetagraphVersion, TessellationVersion}
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.schema.tokenLock.TokenLockLimitsConfig
import io.constellationnetwork.security.{Hasher, HasherSelector}

import com.monovore.decline.Opts
import eu.timepit.refined.auto._
import eu.timepit.refined.boolean.Or
import eu.timepit.refined.pureconfig._
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._

trait OverridableL1 extends TessellationIOApp[Run] {
  def dataApplication: Option[Resource[IO, BaseDataApplicationL1Service[IO]]] = None
  def transactionValidator: Option[CustomContextualTransactionValidator] = None
  def transactionFeeEstimator: Option[TransactionFeeEstimator[IO]] = None
  def setTokenLockLimits: Option[TokenLockLimitsConfig] = None
}

abstract class CurrencyL1App(
  name: String,
  header: String,
  clusterId: ClusterId,
  tessellationVersion: TessellationVersion,
  metagraphVersion: MetagraphVersion
) extends TessellationIOApp[Run](
      name,
      header,
      clusterId,
      layer = CurrencyL1Layer,
      version = tessellationVersion,
      metagraphVersion = metagraphVersion
    )
    with OverridableL1 {

  val opts: Opts[Run] =
    dataApplication match {
      case Some(_) => method.opts(DataL1)
      case None    => method.opts(CurrencyL1)
    }

  protected val configFiles: List[String] = List("currency-l1.conf", "dag-l1.conf")

  type KryoRegistrationIdRange = NodeSharedOrSharedRegistrationIdRange Or DagL1KryoRegistrationIdRange

  val kryoRegistrar: Map[Class[_], KryoRegistrationId[KryoRegistrationIdRange]] =
    nodeSharedKryoRegistrar.union(dagL1KryoRegistrar)

  def run(method: Run, nodeShared: NodeShared[IO, Run]): Resource[IO, Unit] = {
    import nodeShared._

    for {
      cfgR <- loadConfigAs[AppConfigReader].asResource
      cfg = method.appConfig(cfgR, sharedConfig)
      implicit0(withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit) =
        io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.some(
          sharedConfig.delegatedStaking.withdrawalTimeLimit
            .getOrElse(sharedConfig.environment, io.constellationnetwork.schema.epoch.EpochProgress.MinValue)
        )

      dagL1Queues <- DAGL1Queues.make[IO](sharedQueues).asResource
      queues <- Queues.make[IO](dagL1Queues).asResource
      txHasher = Hasher.forKryo[IO]
      validators = hasherSelector.withCurrent { implicit hasher =>
        DAGL1Validators
          .make[IO, CurrencySnapshotStateProof, CurrencyIncrementalSnapshot, CurrencySnapshotInfo](
            cfg.shared,
            seedlist,
            cfg.transactionLimit,
            transactionValidator,
            txHasher,
            CurrencyId(method.identifier).some
          )
      }
      storages <- hasherSelector.withCurrent { implicit hasher =>
        Storages
          .make[IO, CurrencySnapshotStateProof, CurrencyIncrementalSnapshot, CurrencySnapshotInfo](
            sharedStorages,
            method.l0Peer,
            method.globalL0Peer,
            method.identifier,
            validators.transactionContextual,
            validators.allowSpendContextual,
            validators.tokenLockContextual
          )
      }.asResource
      dagP2PClient = DAGP2PClient
        .make[IO](sharedConfig, sharedP2PClient, sharedResources.client, currencyPathPrefix = "currency")
      p2pClient = P2PClient.make[IO](
        dagP2PClient,
        sharedResources.client
      )
      maybeMajorityPeerIds <- getMajorityPeerIds[IO](
        nodeShared.prioritySeedlist,
        cfg.priorityPeerIds,
        cfg.environment
      ).asResource
      dataApplicationService <- dataApplication.sequence.adaptError {
        case error =>
          new RuntimeException(
            s"Data application initialization failed: ${error.getMessage}. ",
            error
          )
      }
      services = Services
        .make[IO, Run](
          storages,
          sharedStorages.lastGlobalSnapshot,
          storages.globalL0Cluster,
          validators,
          sharedServices,
          p2pClient,
          cfg,
          dataApplicationService,
          transactionFeeEstimator,
          maybeMajorityPeerIds,
          Hasher.forKryo[IO],
          sharedStorages
        )
      // CUTOVER (mirrors gl1's dag-l1 Main): the follower's verified-mirror Ref `(lastVerifiedTip, ConsumedFieldState)`, created
      // ONCE here and threaded into the processor so it persists across follow ticks. `None` cold start ⇒ first tick fetches the
      // FULL slice; thereafter the #287 incremental diff since the held tip, with full-fetch fallback on any verify miss / reset.
      followMirrorRef <- Ref.of[IO, Option[(SnapshotOrdinal, ConsumedFieldState)]](none).asResource
      snapshotProcessor = CurrencySnapshotProcessor.make(
        method.identifier,
        storages.address,
        storages.block,
        sharedStorages.lastGlobalSnapshot,
        sharedStorages.lastNGlobalSnapshot,
        storages.lastSnapshot,
        storages.transaction,
        cfg.transactionLimit,
        sharedConfig.allowSpends,
        sharedConfig.tokenLocks,
        Hasher.forKryo[IO],
        storages.allowSpend,
        storages.tokenLock,
        services.globalL0.pullGlobalSnapshot,
        services.globalL0,
        storages.globalL0Alignment,
        sharedStorages.mptStore,
        followMirrorRef
      )
      programs = Programs
        .make[IO, CurrencySnapshotStateProof, CurrencyIncrementalSnapshot, CurrencySnapshotInfo, Run](
          sharedPrograms,
          p2pClient,
          storages,
          snapshotProcessor
        )

      rumorHandler = RumorHandlers
        .make[IO](storages.cluster, services.localHealthcheck, sharedStorages.forkInfo)
        .handlers <+>
        blockRumorHandler[IO](queues.peerBlock) <+>
        allowSpendBlockRumorHandler[IO](queues.allowSpendBlocks) <+>
        tokenLockBlockRumorHandler[IO](queues.tokenLocksBlocks)

      _ <- DAGL1Daemons
        .start(storages, services)
        .asResource

      implicit0(nodeContext: L1NodeContext[IO]) = L1NodeContext
        .make[IO](sharedStorages.lastGlobalSnapshot, storages.lastSnapshot, storages.identifier)

      api = HttpApi
        .make[IO, CurrencySnapshotStateProof, CurrencyIncrementalSnapshot, CurrencySnapshotInfo, Run](
          services.dataApplication,
          storages,
          sharedStorages,
          queues,
          keyPair.getPrivate,
          services,
          programs,
          nodeShared.nodeId,
          tessellationVersion,
          cfg.http,
          metagraphVersion.some,
          txHasher,
          validators,
          setTokenLockLimits,
          sharedConfig
        )
      _ <- MkHttpServer[IO].newEmber(ServerName("public"), cfg.http.publicHttp, api.publicApp)
      _ <- MkHttpServer[IO].newEmber(ServerName("p2p"), cfg.http.p2pHttp, api.p2pApp)
      _ <- MkHttpServer[IO].newEmber(ServerName("cli"), cfg.http.cliHttp, api.cliApp)

      // (#196 follow-up) cl1 → cl0 send-block hop: the upstream `StateChannel`
      // + `TokenLock` were refactored to take a per-call-site lambda so the
      // dl1 → gl0 hop can publish via the gl0 sidecar's durable outbox
      // (replaces the brittle single-peer HTTP POST lottery). cl0 has no
      // sidecar wiring, so the cl1 → cl0 path keeps the previous semantics:
      // pick a random gl0-cl0 alignment peer with collateral, HTTP-POST to
      // it, log + retry on failure. Behaviourally identical to the pre-#196
      // implementation. If/when cl0 grows a sidecar we can lift these
      // lambdas to publish through it instead.
      sendDAGBlockToL0Fn = (signed: io.constellationnetwork.security.signature.Signed[io.constellationnetwork.schema.Block]) =>
        storages.l0Cluster.getPeers
          .map(_.toNonEmptyList.toList)
          .flatMap(_.filterA(p => services.collateral.hasCollateral(p.id)))
          .flatMap(peers => cats.effect.std.Random[IO].shuffleList(peers))
          .map(_.headOption)
          .flatMap {
            case Some(l0Peer) =>
              dagP2PClient.l0BlockOutputClient
                .sendL1Output(signed)(l0Peer)
                .ifM(IO.unit, logger.warn("Sending DAG block to cl0 failed."))
            case None => logger.warn("No available cl0 peer")
          }
          .handleErrorWith(err => logger.error(err)("Error sending DAG block to cl0"))

      sendTokenLockBlockToL0Fn = (signed: io.constellationnetwork.security.signature.Signed[
        io.constellationnetwork.schema.tokenLock.TokenLockBlock
      ]) =>
        storages.l0Cluster.getPeers
          .map(_.toNonEmptyList.toList)
          .flatMap(_.filterA(p => services.collateral.hasCollateral(p.id)))
          .flatMap(peers => cats.effect.std.Random[IO].shuffleList(peers))
          .map(_.headOption)
          .flatMap {
            case Some(l0Peer) =>
              dagP2PClient.l0BlockOutputClient
                .sendTokenLockBlock(signed)(l0Peer)
                .handleErrorWith(e => logger.error(e)("Error when sending token-lock block to cl0").as(false))
                .ifM(IO.unit, logger.warn("Sending token-lock block to cl0 failed"))
            case None => logger.warn("No available cl0 peer")
          }

      sendAllowSpendBlockToL0Fn = (signed: io.constellationnetwork.security.signature.Signed[
        io.constellationnetwork.schema.swap.AllowSpendBlock
      ]) =>
        storages.l0Cluster.getPeers
          .map(_.toNonEmptyList.toList)
          .flatMap(_.filterA(p => services.collateral.hasCollateral(p.id)))
          .flatMap(peers => cats.effect.std.Random[IO].shuffleList(peers))
          .map(_.headOption)
          .flatMap {
            case Some(l0Peer) =>
              dagP2PClient.l0BlockOutputClient
                .sendAllowSpendBlock(signed)(l0Peer)
                .handleErrorWith(e => logger.error(e)("Error when sending allow-spend block to cl0").as(false))
                .ifM(IO.unit, logger.warn("Sending allow-spend block to cl0 failed"))
            case None => logger.warn("No available cl0 peer")
          }

      stateChannel <- StateChannel
        .make[IO, CurrencySnapshotStateProof, CurrencyIncrementalSnapshot, CurrencySnapshotInfo, Run](
          cfg,
          keyPair,
          dagP2PClient,
          programs,
          dagL1Queues,
          nodeId,
          services,
          storages,
          validators,
          txHasher,
          sendDAGBlockToL0Fn
        )
        .asResource

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

      alignment = GlobalSnapshotAlignment
        .make[IO, CurrencySnapshotStateProof, CurrencyIncrementalSnapshot, CurrencySnapshotInfo, Run](
          services,
          programs,
          storages,
          sharedStorages
        )

      _ <- alignment.performGlobalSnapshotProcessingUntilCaughtUp().asResource

      _ <- {
        method match {
          case cfg: RunInitialValidator =>
            storages.identifier.setInitial(cfg.identifier) >>
              gossipDaemon.startAsInitialValidator >>
              programs.l0PeerDiscovery.discoverFromWithRetry(cfg.l0Peer) >>
              programs.globalL0PeerDiscovery.discoverFromWithRetry(cfg.globalL0Peer) >>
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
                  cfg.globalL0Peer,
                  cfg.identifier,
                  cfg.seedlistPath,
                  cfg.collateralAmount,
                  cfg.prioritySeedlistPath,
                  cfg.allowanceListPath
                )
              )

          case cfg: RunValidator =>
            storages.identifier.setInitial(cfg.identifier) >>
              gossipDaemon.startAsRegularValidator >>
              programs.l0PeerDiscovery.discoverFromWithRetry(cfg.l0Peer) >>
              programs.globalL0PeerDiscovery.discoverFromWithRetry(cfg.globalL0Peer) >>
              storages.node.tryModifyState(NodeState.Initial, NodeState.ReadyToJoin)

          case cfg: RunValidatorWithJoinAttempt =>
            storages.identifier.setInitial(cfg.identifier) >>
              gossipDaemon.startAsRegularValidator >>
              programs.l0PeerDiscovery.discoverFromWithRetry(cfg.l0Peer) >>
              programs.globalL0PeerDiscovery.discoverFromWithRetry(cfg.globalL0Peer) >>
              storages.node.tryModifyState(NodeState.Initial, NodeState.ReadyToJoin) >>
              programs.joining.joinOneOf(cfg.majorityForkPeerIds)
        }
      }.asResource
      _ <- hasherSelector.withCurrent { implicit hasher =>
        services.dataApplication.map { da =>
          DataApplication
            .run(
              cfg.dataConsensus,
              storages.cluster,
              storages.l0Cluster,
              sharedStorages.lastGlobalSnapshot,
              storages.lastSnapshot,
              storages.node,
              p2pClient.l0BlockOutputClient,
              p2pClient.consensusClient,
              services,
              queues,
              da,
              keyPair,
              nodeId
            )
            .merge(alignment.runtime())
            .compile
            .drain
            .handleErrorWith { error =>
              logger.error(error)("An error occured during state channel runtime") >> error.raiseError[IO, Unit]
            }
        }.getOrElse {
          Swap
            .run[IO, CurrencySnapshotStateProof, CurrencyIncrementalSnapshot, CurrencySnapshotInfo, Run](
              cfg.swap,
              storages.cluster,
              sharedStorages.lastGlobalSnapshot,
              storages.node,
              sendAllowSpendBlockToL0Fn,
              p2pClient.swapConsensusClient,
              services,
              storages.allowSpend,
              storages.allowSpendBlock,
              dagL1Queues,
              validators.allowSpend,
              keyPair,
              nodeId,
              storages.globalL0Alignment
            )
            .merge {
              TokenLock.run[IO, CurrencySnapshotStateProof, CurrencyIncrementalSnapshot, CurrencySnapshotInfo, Run](
                cfg.tokenLock,
                storages.cluster,
                sharedStorages.lastGlobalSnapshot,
                storages.node,
                sendTokenLockBlockToL0Fn,
                p2pClient.tokenLockConsensusClient,
                services,
                storages.tokenLock,
                storages.tokenLockBlock,
                dagL1Queues,
                validators.tokenLock,
                keyPair,
                nodeId,
                storages.globalL0Alignment
              )
            }
            .merge(stateChannel.runtime)
            .merge(alignment.runtime())
            .compile
            .drain
        }.asResource
      }
    } yield ()
  }
}
