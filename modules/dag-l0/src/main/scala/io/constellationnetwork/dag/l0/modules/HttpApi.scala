package io.constellationnetwork.dag.l0.modules

import java.security.PrivateKey

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.dag.l0.domain.cell.{L0Cell, L0CellInput}
import io.constellationnetwork.dag.l0.domain.delegatedStake.DelegatedStakeOutput
import io.constellationnetwork.dag.l0.domain.nodeCollateral.NodeCollateralOutput
import io.constellationnetwork.dag.l0.http.routes._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotKey
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event.GlobalSnapshotEvent
import io.constellationnetwork.dag.l0.infrastructure.snapshot.schema.GlobalConsensusOutcome
import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.env.AppEnvironment._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.config.types.{HttpConfig, RouteRateLimiterConfig, SharedConfig}
import io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertValidator
import io.constellationnetwork.node.shared.domain.snapshot.finality.{FinalityGate, FinalizedSnapshotReader}
import io.constellationnetwork.node.shared.http.p2p.middlewares.{MetricsMiddleware, PeerAuthMiddleware, `X-Id-Middleware`}
import io.constellationnetwork.node.shared.http.routes._
import io.constellationnetwork.node.shared.infrastructure.gossip.event.ChainTip
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.CombinedSnapshotCheckpointFileSystemStorage
import io.constellationnetwork.node.shared.modules.SharedValidators
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.TessellationVersion
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{HasherSelector, SecurityProvider}

import eu.timepit.refined.auto._
import org.http4s.implicits.http4sKleisliResponseSyntaxOptionT
import org.http4s.server.middleware.{CORS, RequestLogger, ResponseLogger}
import org.http4s.{HttpApp, HttpRoutes}

object HttpApi {

  def make[F[_]: Async: FinalityGate: SecurityProvider: HasherSelector: JsonSerializer: Metrics, R <: CliMethod](
    storages: Storages[F],
    queues: Queues[F],
    services: Services[F, R],
    programs: Programs[F],
    privateKey: PrivateKey,
    environment: AppEnvironment,
    selfId: PeerId,
    nodeVersion: TessellationVersion,
    httpCfg: HttpConfig,
    sharedValidators: SharedValidators[F],
    delegatedStakingWithdrawalTimeLimit: EpochProgress,
    sharedConfig: SharedConfig,
    combinedSnapshotCheckpointFileSystemStorage: CombinedSnapshotCheckpointFileSystemStorage[
      F,
      GlobalIncrementalSnapshot,
      GlobalSnapshotInfo
    ],
    // §1.2 Slice 10 (#179): L0 seedlist threaded into the KES registration validator built inside HttpApi.
    // Mirrors the seedlist that `SharedValidators` already received — the validator uses it to gate
    // who is authorized to submit a runtime KES registration cert.
    l0Seedlist: Option[Set[SeedlistEntry]],
    getLocalChainTip: Option[F[Option[ChainTip]]] = None,
    maybeMarkSeen: Option[Hash => F[Unit]] = None
  ): F[HttpApi[F, R]] = {
    // GL0 runs Nakamoto consensus — head may run ahead of the attestation-finalized ordinal held in `FinalityGate`. The
    // `FinalizedSnapshotReader.nakamoto` variant serves `/latest/combined` and its kin from the on-disk checkpoint at-or-below
    // finalized, so tentative (pre-finality) state never leaves the node via HTTP — that channel is reserved for sidecar gossip.
    val finalizedReader = FinalizedSnapshotReader.nakamoto[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo](
      FinalityGate[F],
      combinedSnapshotCheckpointFileSystemStorage
    )
    SnapshotRoutes
      .make[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo](
        storages.globalSnapshot,
        storages.fullGlobalSnapshot.some,
        "/global-snapshots",
        storages.node,
        HasherSelector[F],
        sharedConfig.snapshotTimeoutsConfig,
        finalizedReader
      )
      .map { snapshotRoutes =>
        new HttpApi[F, R](
          storages,
          queues,
          services,
          programs,
          privateKey,
          environment,
          selfId,
          nodeVersion,
          httpCfg,
          sharedValidators,
          delegatedStakingWithdrawalTimeLimit,
          sharedConfig,
          snapshotRoutes,
          l0Seedlist,
          getLocalChainTip,
          maybeMarkSeen
        ) {}
      }
  }
}

sealed abstract class HttpApi[
  F[_]: Async: FinalityGate: SecurityProvider: HasherSelector: JsonSerializer: Metrics,
  R <: CliMethod
] private (
  storages: Storages[F],
  queues: Queues[F],
  services: Services[F, R],
  programs: Programs[F],
  privateKey: PrivateKey,
  environment: AppEnvironment,
  selfId: PeerId,
  nodeVersion: TessellationVersion,
  httpCfg: HttpConfig,
  sharedValidators: SharedValidators[F],
  delegatedStakingWithdrawalTimeLimit: EpochProgress,
  sharedConfig: SharedConfig,
  snapshotRoutes: SnapshotRoutes[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
  l0Seedlist: Option[Set[SeedlistEntry]],
  getLocalChainTip: Option[F[Option[ChainTip]]] = None,
  maybeMarkSeen: Option[Hash => F[Unit]] = None
) {

  private val mkNodeParametersCell = (params: Signed[UpdateNodeParameters]) =>
    L0Cell
      .mkL0Cell(
        queues.l1Output,
        queues.stateChannelOutput,
        queues.updateNodeParametersOutput,
        queues.delegatedStakeOutput,
        queues.nodeCollateralOutput,
        queues.kesRegistrationCertOutput
      )
      .apply(L0CellInput.HandleUpdateNodeParameters(params))

  private val mkDelegatedStakesCell = (data: DelegatedStakeOutput) =>
    L0Cell
      .mkL0Cell(
        queues.l1Output,
        queues.stateChannelOutput,
        queues.updateNodeParametersOutput,
        queues.delegatedStakeOutput,
        queues.nodeCollateralOutput,
        queues.kesRegistrationCertOutput
      )
      .apply(L0CellInput.HandleDelegatedStake(data))

  private val mkNodeCollateralCell = (data: NodeCollateralOutput) =>
    L0Cell
      .mkL0Cell(
        queues.l1Output,
        queues.stateChannelOutput,
        queues.updateNodeParametersOutput,
        queues.delegatedStakeOutput,
        queues.nodeCollateralOutput,
        queues.kesRegistrationCertOutput
      )
      .apply(L0CellInput.HandleNodeCollateral(data))

  // §1.2 Slice 10 (#179): Mirrors `mkNodeCollateralCell` — wraps the validated `Signed[KesRegistrationCert]`
  // in an `L0Cell` whose algebra offers the cert into `queues.kesRegistrationCertOutput`. The events
  // publisher daemon drains the queue and lifts each cert to a `KesRegistrationCertEvent` for the mempool,
  // which the GSAM accept-pipeline (wave 2) picks up and applies to `services.mutableKesRegistry`.
  private val mkKesRegistrationCertCell = (cert: Signed[KesRegistrationCert]) =>
    L0Cell
      .mkL0Cell(
        queues.l1Output,
        queues.stateChannelOutput,
        queues.updateNodeParametersOutput,
        queues.delegatedStakeOutput,
        queues.nodeCollateralOutput,
        queues.kesRegistrationCertOutput
      )
      .apply(L0CellInput.HandleKesRegistrationCert(cert))

  private val clusterRoutes =
    HasherSelector[F].withCurrent { implicit hasher =>
      ClusterRoutes[F](programs.joining, programs.peerDiscovery, storages.cluster, services.cluster, services.collateral)
    }
  private val nodeRoutes = NodeRoutes[F](storages.node, storages.session, storages.cluster, nodeVersion, httpCfg, selfId)

  private val registrationRoutes = RegistrationRoutes[F](services.cluster)
  private val gossipRoutes = GossipRoutes[F](storages.rumor, services.gossip, sharedConfig.gossip.timeouts)
  private val eventGossipRoutes = EventGossipRoutes.make[F, GlobalSnapshotEvent, GlobalStateKey](
    services.eventMempool,
    getLocalChainTip,
    maybeMarkSeen
  )
  private val stateChannelRoutes =
    HasherSelector[F].withCurrent { implicit hasher =>
      StateChannelRoutes[F](
        services.stateChannel,
        storages.globalSnapshot,
        sharedConfig.snapshotBinarySenderTimeouts,
        services.sidecarClient
      )
    }
  private val nodeParametersRoutes = HasherSelector[F].withCurrent { implicit hasher =>
    NodeParametersRoutes[F](
      mkNodeParametersCell,
      storages.globalSnapshot,
      storages.node,
      services.cluster,
      sharedValidators.updateNodeParametersValidator
    )
  }
  private val delegatedStakesRoutes =
    HasherSelector[F].withCurrent { implicit hasher =>
      DelegatedStakesRoutes[F](
        mkDelegatedStakesCell,
        sharedValidators.updateDelegatedStakeValidator,
        storages.globalSnapshot,
        storages.node,
        delegatedStakingWithdrawalTimeLimit,
        services.rewards.rewardsInfoStorage,
        services.pendingReader
      )
    }
  private val nodeCollateralsRoutes = HasherSelector[F].withCurrent { implicit hasher =>
    NodeCollateralRoutes[F](
      mkNodeCollateralCell,
      sharedValidators.updateNodeCollateralValidator,
      storages.globalSnapshot,
      storages.node,
      delegatedStakingWithdrawalTimeLimit,
      services.pendingReader
    )
  }

  // §1.2 Slice 10 (#179): Runtime KES master-VK registration intake. The route validates the cert (signature,
  // monotonic ordinal, chain-link parent, forward activation, well-formed VK) and on success offers it into
  // the cell that publishes a `KesRegistrationCertEvent` for the mempool. Reads/writes the shared
  // `services.mutableKesRegistry` so `lastReference` matches what the validator + GSAM accept-pipeline see.
  private val kesRegistrationCertRoutes = HasherSelector[F].withCurrent { implicit hasher =>
    val validator = KesRegistrationCertValidator.make[F](sharedValidators.signedValidator, l0Seedlist)
    val onAccepted: Signed[KesRegistrationCert] => F[Unit] = cert => mkKesRegistrationCertCell(cert).run().void
    KesRegistrationCertRoutes[F](
      onAccepted,
      validator,
      storages.globalSnapshot,
      services.mutableKesRegistry
    )
  }

  private val tokenLockRoutes = GL0TokenLockRoutes(storages.globalSnapshot, services.pendingReader)

  // Chain-quality / finality-triggers observable (#138). Reads the FinalityTriggerView Ref
  // populated by SnapshotLeaderLoop after trigger construction. Pure observability — no
  // consensus semantics change.
  private val finalityTriggersRoutes = FinalityTriggersRoutes[F](services.finalityTriggerViewRef)

  // §3 NIPoPoW S5 — light-client proof + verify routes. Reads the NipopowProofProvider Ref
  // populated by GlobalSnapshotConsensus.make once the tower store and snapshot storage are
  // wired. Pure observability — never feeds back into consensus.
  private val nipopowRoutes = NipopowRoutes[F](services.nipopowProofProviderRef)

  // Axis 2 Slice 3a (gl1 inclusion-proof follow) — serves `GET /global-follow/slice/latest` so a gl1
  // `GlobalFollowClient` can fetch the latest-finalized consumed-field slice. Reaches gl0 over the
  // PUBLIC HTTP port: gl1 resolves gl0 as an `L0Peer` whose `port = publicPort` (see
  // `L0Peer.toP2PContext` / `fromPeer`), the SAME PeerResponse path `L0GlobalSnapshotClient` uses to
  // pull global snapshots — so the route is a `PublicRoutes` mounted in `openRoutes` alongside
  // `nipopowRoutes`. Reads the service Ref populated by GlobalSnapshotConsensus.make (the slice producer
  // over the SAME finalized GSI source `getCombined` serves; the slice service carries the finalized
  // ordinal itself). Pure observability — never feeds back into consensus.
  private val globalFollowRoutes =
    GlobalFollowRoutes[F](services.globalFollowSliceServiceRef)

  private val walletRoutes = WalletRoutes[F, GlobalIncrementalSnapshot]("/dag", services.address)
  private val consensusInfoRoutes =
    HasherSelector[F].withCurrent { implicit hasher =>
      new ConsensusInfoRoutes[F, GlobalSnapshotKey, GlobalConsensusOutcome](
        services.cluster,
        services.consensus.storage,
        selfId,
        services.consensus.healthRef
      )
    }
  private val consensusRoutes = services.consensus.routes.p2pRoutes

  private val debugRoutes = DebugRoutes[F](
    storages.cluster,
    services.consensus,
    services.gossip,
    services.session
  ).publicRoutes

  private val metricRoutes = MetricRoutes[F]().publicRoutes
  private val targetRoutes =
    HasherSelector[F].withCurrent { implicit hasher =>
      TargetRoutes[F](services.cluster).publicRoutes
    }

  private val openRoutes: HttpRoutes[F] =
    CORS.policy.withAllowOriginAll.withAllowHeadersAll.withAllowCredentials(false).apply {
      MetricsMiddleware[F]()(implicitly[Async[F]], implicitly[Metrics[F]]) {
        PeerAuthMiddleware
          .responseSignerMiddleware(privateKey, storages.session, selfId) {
            `X-Id-Middleware`.responseMiddleware(selfId) {
              (if (Seq(Dev, Integrationnet, Testnet).contains(environment)) debugRoutes else HttpRoutes.empty) <+>
                metricRoutes <+>
                targetRoutes <+>
                stateChannelRoutes.publicRoutes <+>
                clusterRoutes.publicRoutes <+>
                snapshotRoutes.publicRoutes <+>
                finalityTriggersRoutes.publicRoutes <+>
                nipopowRoutes.publicRoutes <+>
                globalFollowRoutes.publicRoutes <+>
                walletRoutes.publicRoutes <+>
                nodeRoutes.publicRoutes <+>
                consensusInfoRoutes.publicRoutes <+>
                tokenLockRoutes.publicRoutes <+>
                nodeParametersRoutes.publicRoutes <+>
                delegatedStakesRoutes.publicRoutes <+>
                nodeCollateralsRoutes.publicRoutes <+>
                kesRegistrationCertRoutes.publicRoutes
            }
          }
      }
    }

  /** BFT P2P routes (gossip, event gossip, consensus) are excluded — GL0 is Nakamoto-only. */
  private val bftP2pRoutes: HttpRoutes[F] = HttpRoutes.empty

  private val p2pRoutes: HttpRoutes[F] =
    MetricsMiddleware[F]()(implicitly[Async[F]], implicitly[Metrics[F]]) {
      PeerAuthMiddleware.responseSignerMiddleware(privateKey, storages.session, selfId)(
        registrationRoutes.p2pPublicRoutes <+>
          clusterRoutes.p2pPublicRoutes <+>
          PeerAuthMiddleware.requestVerifierMiddleware(
            PeerAuthMiddleware.requestTokenVerifierMiddleware(services.session)(
              PeerAuthMiddleware.requestCollateralVerifierMiddleware(services.collateral)(
                clusterRoutes.p2pRoutes <+>
                  nodeRoutes.p2pRoutes <+>
                  snapshotRoutes.p2pRoutes <+>
                  bftP2pRoutes
              )
            )
          )
      )
    }

  private val cliRoutes: HttpRoutes[F] =
    clusterRoutes.cliRoutes

  private val loggers: HttpApp[F] => HttpApp[F] = { http: HttpApp[F] =>
    RequestLogger.httpApp(logHeaders = true, logBody = false)(http)
  }.andThen { http: HttpApp[F] =>
    ResponseLogger.httpApp(logHeaders = true, logBody = false)(http)
  }

  val publicApp: HttpApp[F] = loggers(openRoutes.orNotFound)
  val p2pApp: HttpApp[F] = loggers(p2pRoutes.orNotFound)
  val cliApp: HttpApp[F] = loggers(cliRoutes.orNotFound)

}
