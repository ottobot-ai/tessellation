package io.constellationnetwork.dag.l0

import cats.effect._
import cats.syntax.all._

import io.constellationnetwork.BuildInfo
import io.constellationnetwork.dag.l0.StoragesInitializer.initializeStorages
import io.constellationnetwork.dag.l0.cli.method._
import io.constellationnetwork.dag.l0.config.types._
import io.constellationnetwork.dag.l0.domain.snapshot.ForkRecoveryService
import io.constellationnetwork.dag.l0.http.p2p.P2PClient
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event.GlobalSnapshotEvent
import io.constellationnetwork.dag.l0.infrastructure.snapshot.schema.{Finished, GlobalConsensusOutcome}
import io.constellationnetwork.dag.l0.modules._
import io.constellationnetwork.ext.cats.effect._
import io.constellationnetwork.ext.kryo._
import io.constellationnetwork.node.shared.app.{DagL0, NodeShared, TessellationIOApp}
import io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager.EtaSourceRange
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityGate
import io.constellationnetwork.node.shared.ext.pureconfig._
import io.constellationnetwork.node.shared.infrastructure.consensus.state._
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.EventTrigger
import io.constellationnetwork.node.shared.infrastructure.genesis.{GenesisFS => GenesisLoader, L0GenesisLoader}
import io.constellationnetwork.node.shared.infrastructure.gossip.event._
import io.constellationnetwork.node.shared.infrastructure.gossip.{GossipDaemon, RumorHandlers}
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.GlobalSnapshotLocalFileSystemStorage
import io.constellationnetwork.node.shared.resources.MkHttpServer
import io.constellationnetwork.node.shared.resources.MkHttpServer.ServerName
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.cluster.{ClusterId, ClusterSessionToken, SessionToken}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.generation.Generation
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.peer.{Peer, Responsive}
import io.constellationnetwork.schema.semver.TessellationVersion
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed

import com.monovore.decline.Opts
import eu.timepit.refined.auto._
import eu.timepit.refined.pureconfig._
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._

private[l0] object GenesisOperatorKeyAnchor {
  def requireJson(path: Option[fs2.io.file.Path]): Either[IllegalArgumentException, fs2.io.file.Path] =
    path
      .filter(_.extName == ".json")
      .toRight(
        new IllegalArgumentException(
          s"Nakamoto GL0 requires the canonical L0 genesis operator-key anchor as a JSON genesis argument for every startup mode " +
            s"(including cold restart and rollback). The JSON supplies local startup material, but the signed MPT root is authoritative; received " +
            path.fold("no genesis argument")(_.toString)
        )
      )
}

object Main
    extends TessellationIOApp[RunNakamoto](
      name = "dag-l0",
      header = "Tessellation Node",
      version = TessellationVersion.unsafeFrom(BuildInfo.version),
      clusterId = ClusterId("6d7f1d6a-213a-4148-9d45-d7200f555ecf"),
      layer = DagL0
    ) {

  val opts: Opts[RunNakamoto] = cli.method.opts

  protected val configFiles: List[String] = List("dag-l0.conf")

  type KryoRegistrationIdRange = DagL0KryoRegistrationIdRange

  val kryoRegistrar: Map[Class[_], KryoRegistrationId[KryoRegistrationIdRange]] =
    dagL0KryoRegistrar

  /** Load the complete signed genesis operator-key material before constructing any GL0 service. Every bootstrap mode, including cold
    * restart and rollback, requires the JSON material; startup later proves exact equality with the root-authenticated field-24 identity.
    * CSV and absent-genesis fallbacks fail here rather than starting a node whose consensus consumers have empty or asymmetric identities.
    */
  override protected def nakamotoOperatorKeyRegistries(method: RunNakamoto)(
    implicit jsonSerializer: io.constellationnetwork.json.JsonSerializer[IO],
    securityProvider: io.constellationnetwork.security.SecurityProvider[IO]
  ): Resource[IO, io.constellationnetwork.node.shared.domain.nakamoto.OperatorConsensusKeyRegistry[IO]] =
    GenesisOperatorKeyAnchor.requireJson(method.genesisPath) match {
      case Right(gPath) =>
        GenesisLoader
          .make[IO, GlobalSnapshot]
          .loadL0Genesis(gPath)
          .flatMap(L0GenesisLoader.buildOperatorKeyRegistry[IO])
          .asResource
      case Left(error) =>
        IO.raiseError[io.constellationnetwork.node.shared.domain.nakamoto.OperatorConsensusKeyRegistry[IO]](error).toResource
    }

  def run(method: RunNakamoto, nodeShared: NodeShared[IO, RunNakamoto]): Resource[IO, Unit] = {
    import nodeShared._

    for {
      cfgR <- loadConfigAs[AppConfigReader].asResource
      implicit0(logger: SelfAwareStructuredLogger[IO]) = Slf4jLogger.getLoggerFromName[IO](this.getClass.getName)
      cfg = method.appConfig(cfgR, sharedConfig)
      // WithdrawalTimeLimit — per-environment constant supplied as implicit context to every MPT write path
      // (`syncFromGlobalSnapshotInfo`, `toAllStateKeyValueBytes`, `mptStateProof`) so that both accept and
      // rebuild paths produce the same mptRoot. Matches the `StateProofSelector` implicit pattern.
      implicit0(withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit) =
        io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.some(
          cfg.shared.delegatedStaking.withdrawalTimeLimit.getOrElse(cfg.environment, EpochProgress.MinValue)
        )
      queues <- Queues.make[IO](sharedQueues).asResource

      p2pClient = P2PClient.make[IO](sharedP2PClient, sharedResources.client, sharedServices.session, sharedConfig.snapshotTimeoutsConfig)
      // Nakamoto finalized-ordinal tracker. Updated by SnapshotLeaderLoop after every
      // chainStore.finalize call (depth-k or attestation-2/3). Read by the HTTP routes
      // exposing /global-snapshots/latest/finalized-ordinal so CL0 can gate state-channel
      // -binary pruning on actual finality. Stays at 0 in BFT mode (HttpApi only consults
      // it when nakamoto mode is on).
      //
      // Seed with MinValue (= ord 0, pre-genesis sentinel) so finality-gated endpoints
      // return NotFound until the leader loop actually finalizes the first ord. Previous
      // seed of MinIncrementalValue (= ord 1) was the root of #215: it claimed ord=1
      // was finalized before any attestation ran, so /latest/combined/stream served
      // whatever ord=1 the local peer happened to hold — which under 8gl0 fork races was
      // a transient hash later reorged away. ml0 bootstrapped against that dead hash, and
      // every CL0 binary it built referenced a `globalSyncView` gl0 ord=1 hash that no
      // longer matched canonical → `Forced globalSyncView hash mismatch` at GSAM →
      // metagraph chain stuck at genesis. By seeding MinValue we make ml0 wait for the
      // first ACTUAL finality before bootstrapping; once SnapshotLeaderLoop advances
      // the ref past MinValue, ml0 picks up the canonical finalized snapshot.
      // Constructed BEFORE Storages so SnapshotStorage.setHeadForRecovery can enforce
      // the finality-safety guard (refuses different-hash overwrites at-or-below finalized).
      nakamotoFinalizedOrdinalRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue).asResource
      // Chain-quality observable seam (#138). Set by SnapshotLeaderLoop after trigger
      // construction; read by the new /global-snapshots/{ord}/finality-triggers HTTP
      // route. Lives at the same lifetime as nakamotoFinalizedOrdinalRef — created here
      // so both Services and HttpApi can share it.
      finalityTriggerViewRef <- Ref
        .of[IO, Option[io.constellationnetwork.node.shared.domain.nakamoto.FinalityTriggerView[IO]]](None)
        .asResource
      // Transitional legacy k2 watermark shared by the tracker, HTTP telemetry, and current
      // chain-store floor wiring. It stays distinct from the current k1 ref to avoid aliasing, but
      // target k2 is local retention/proof/recovery capacity only and has no Phase-3/fork-choice role.
      nakamotoSettledOrdinalRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue).asResource
      settledOrdinalTracker = io.constellationnetwork.node.shared.domain.nakamoto.SettledOrdinalTracker
        .makeFromRef[IO](nakamotoSettledOrdinalRef)
      // §3 NIPoPoW S5 — light-client proof + verify HTTP routes. Populated inside
      // GlobalSnapshotConsensus.make once the tower store and snapshot storage are wired.
      // Shared between Services and HttpApi the same way as `finalityTriggerViewRef`.
      nipopowProofProviderRef <- Ref
        .of[IO, Option[io.constellationnetwork.node.shared.domain.nakamoto.nipopow.NipopowProofProvider[IO]]](None)
        .asResource
      // Axis 2 (gl1 inclusion-proof follow) — observability seam for the GlobalFollowRoutes
      // `GET /global-follow/slice/latest` endpoint. Populated inside GlobalSnapshotConsensus.make once
      // the finalized GSI source (`lastNGlobalSnapshotStorage`) is wired; the slice service reads the
      // latest-finalized `(ordinal, GSI)` itself. Shared between Services and HttpApi exactly like
      // `nipopowProofProviderRef`; the route returns 503 while it is still `None`.
      globalFollowSliceServiceRef <- Ref
        .of[IO, Option[io.constellationnetwork.node.shared.domain.nakamoto.GlobalFollowSliceService[IO]]](None)
        .asResource
      // Task #12 (ml0 adopt) — observability seam for the GlobalFollowRoutes
      // `GET /global-follow/changeset?since=<ord>` endpoint. Populated inside GlobalSnapshotConsensus.make once
      // the finalized accumulator ring (`recentFinalizedAccumulatorsRef`) is wired; the changeset service reads
      // that ring. Shared between Services and HttpApi exactly like `globalFollowSliceServiceRef`; the route
      // returns 503 while it is still `None`.
      globalChangeSetServiceRef <- Ref
        .of[IO, Option[io.constellationnetwork.node.shared.domain.nakamoto.GlobalChangeSetService[IO]]](None)
        .asResource
      // Slice 10/11 (cross-shard read transport) — backing seam for `ShardProofRoutes`
      // (`POST /shard/{shardId}/proof`). Populated inside GlobalSnapshotConsensus.make once the per-shard
      // chain stores + MPT proof service are wired (only on the sharding-active path; stays `None` at
      // numShards=1, so the route serves 503). Shared between Services and HttpApi exactly like
      // `globalFollowSliceServiceRef`; it IS the Ref the route reads.
      shardProofServiceRef <- Ref
        .of[IO, Option[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProofService[IO]]](None)
        .asResource
      storages <- Storages
        .make[IO](
          sharedStorages,
          sharedConfig,
          nodeShared.seedlist,
          cfg.snapshot,
          cfg.incremental,
          sharedConfig.environment,
          hashSelect,
          nakamotoFinalizedOrdinalRef.some
        )
        .asResource

      // Transitional ordinal-only P2 HTTP release gate. Target FinalityGate is keyed by
      // exact `(ordinal, hash)`, carries P0/P1/P2, and emits density-reorg rollback/re-follow.
      // Constructed once here so current route gating is at least uniform.
      implicit0(finalityGate: FinalityGate[IO]) = FinalityGate.fromRef[IO](nakamotoFinalizedOrdinalRef)

      services <- Services
        .make[IO, RunNakamoto](
          sharedConfig,
          sharedServices,
          sharedStorages,
          queues,
          storages,
          nodeShared.sharedValidators,
          sharedResources.client,
          sharedServices.session,
          nodeShared.seedlist,
          method.stateChannelAllowanceLists,
          nodeShared.nodeId,
          keyPair,
          cfg,
          Hasher.forKryo[IO],
          nodeShared.loggerBundle,
          nakamotoFinalizedOrdinalRef,
          nakamotoSettledOrdinalRef,
          finalityTriggerViewRef,
          settledOrdinalTracker,
          nipopowProofProviderRef,
          globalFollowSliceServiceRef,
          globalChangeSetServiceRef,
          shardProofServiceRef,
          // Split-safety (#261, eta axis): install the leader's chain-walk into the follower / `createContext`
          // GSAM's deferred committee-eta resolver (the Ref created in `TessellationIOApp.make`, exposed on
          // `NodeShared`). `GlobalSnapshotConsensus.make` invokes this once the chain store exists so the
          // follower `EtaStateManager.getEtaAt(P, parentHash)` byte-matches the leader's exact branch for every period P.
          setFollowerEtaChainWalk = (walk: (Long, Option[Hash]) => IO[EtaSourceRange]) => nakamotoFollowerEtaChainWalkRef.set(Some(walk))
        )

      programs = Programs.make[IO, RunNakamoto](
        sharedPrograms,
        storages,
        services,
        keyPair,
        cfg,
        cfg.incremental.lastFullGlobalSnapshotOrdinal.getOrElse(cfg.environment, SnapshotOrdinal.MinValue),
        p2pClient,
        sharedServices.globalSnapshotContextFns,
        storages.globalSnapshot,
        sharedStorages.lastNGlobalSnapshot,
        sharedStorages.lastGlobalSnapshot,
        sharedStorages.mptStore
      )

      // Inbound event rumor handler: receives Signed[GlobalSnapshotEvent] rumors
      // from other GL0 nodes via the sidecar GossipSub transport and adds them
      // to the local event mempool. This is how events submitted to gl0-1 reach
      // gl0-0's mempool for inclusion in the next snapshot.
      eventRumorHandler = {
        import io.constellationnetwork.dag.l0.infrastructure.snapshot.event.GlobalSnapshotEvent
        import io.constellationnetwork.node.shared.infrastructure.gossip.RumorHandler
        import io.constellationnetwork.node.shared.infrastructure.gossip.ExcludeSelfOrigin
        RumorHandler.fromPeerRumorConsumer[IO, Signed[GlobalSnapshotEvent]](ExcludeSelfOrigin) { rumor =>
          services.eventMempool.add(rumor.content).void
        }
      }

      rumorHandler = RumorHandlers
        .make[IO](storages.cluster, services.localHealthcheck, sharedStorages.forkInfo)
        .handlers <+>
        services.consensus.handler <+>
        eventRumorHandler

      forkRecoveryService = ForkRecoveryService.make[IO](
        storages.node,
        sharedStorages.lastGlobalSnapshot,
        services.recoveryPeerHint
      )

      // Nakamoto GL0: no legacy EventGossipDaemon (P2P gossip is via libp2p sidecar)
      eventGossipDaemon = EventGossipDaemon.noop[IO, GlobalSnapshotEvent, GlobalStateKey]

      _ <- Daemons
        .startNakamoto(
          storages,
          services,
          queues,
          sharedServices.gossip,
          nodeId,
          keyPair,
          cfg
        )
        .asResource

      api <- Resource.eval(
        HttpApi.make[IO, RunNakamoto](
          storages,
          queues,
          services,
          programs,
          keyPair.getPrivate,
          sharedConfig.environment,
          nodeShared.nodeId,
          TessellationVersion.unsafeFrom(BuildInfo.version),
          cfg.http,
          sharedValidators,
          cfg.shared.delegatedStaking.withdrawalTimeLimit
            .getOrElse(sharedConfig.environment, EpochProgress.MinValue),
          cfg.shared,
          storages.combinedGlobalSnapshotCheckpointStorage,
          nodeShared.seedlist,
          getLocalChainTip = Some(forkRecoveryService.getLocalChainTip),
          maybeMarkSeen = Some(eventGossipDaemon.markSeen)
        )
      )

      _ <- MkHttpServer[IO].newEmber(ServerName("public"), cfg.http.publicHttp, api.publicApp)
      _ <- MkHttpServer[IO].newEmber(ServerName("p2p"), cfg.http.p2pHttp, api.p2pApp)
      _ <- MkHttpServer[IO].newEmber(ServerName("cli"), cfg.http.cliHttp, api.cliApp)

      gossipDaemon = GossipDaemon.make[IO](
        storages.rumor,
        queues.rumor,
        storages.cluster,
        p2pClient.gossip,
        rumorHandler,
        nodeShared.sharedValidators.rumorValidator,
        services.localHealthcheck,
        nodeId,
        generation,
        sharedConfig.gossip.daemon,
        services.collateral,
        nakamotoMode = true
      )

      // Unified Nakamoto bootstrap: auto-detect the right startup path from the flags
      // provided on the CLI. See cli/method.scala for precedence docs.
      //
      // Precedence:
      //   1. --rollback-hash  → load a specific snapshot (from disk or peer) as trust anchor
      //   2. Local data on disk → cold restart from the latest ordinal
      //   3. --nakamoto-peer  → fail closed until ancestry replay or an audited finality certificate is implemented
      //   4. --genesis-csv    → fresh start from a genesis CSV
      //   5. None             → startup error
      _ <- ({
        import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.GlobalSnapshotInfoLocalFileSystemStorage

        if (method.rollbackHash.isDefined) {
          // === PATH 1: ROLLBACK — load a specific snapshot by hash ===
          val hash = method.rollbackHash.get
          logger.info(s"Rollback mode: loading snapshot hash=${hash.value.take(16)}...") >>
            storages.node.tryModifyState(
              NodeState.Initial,
              NodeState.RollbackInProgress,
              NodeState.RollbackDone
            ) {
              programs.rollbackLoader.load(hash, programs.download).flatMap {
                case (snapshotInfo, snapshot) =>
                  for {
                    hashedSnapshot <- hasherSelector.withCurrent(implicit hasher => snapshot.toHashed[IO])
                    _ <- services.consensus.manager.startFacilitatingAfterRollback(
                      snapshot.ordinal,
                      GlobalConsensusOutcome(
                        snapshot.ordinal,
                        Facilitators(List(nodeId)),
                        RemovedFacilitators.empty,
                        WithdrawnFacilitators.empty,
                        EligibleFacilitators.empty,
                        Finished(snapshot, snapshotInfo, EventTrigger, Candidates.empty, Hash.empty, hashedSnapshot.hash)
                      )
                    )
                  } yield ()
              }
            } >>
            services.cluster.createSession >>
            services.session.createSession >>
            storages.node.setNodeState(NodeState.Ready)

        } else {
          storages.node.tryModifyState(
            NodeState.Initial,
            NodeState.LoadingGenesis,
            NodeState.GenesisReady
          ) {
            // Check for local snapshot data on disk (cold restart detection)
            GlobalSnapshotInfoLocalFileSystemStorage.make[IO](cfg.snapshot.snapshotInfoPath).flatMap { infoStorage =>
              infoStorage.listStoredOrdinals.flatMap { ordinalStream =>
                ordinalStream.compile.toList.flatMap { storedOrdinals =>
                  if (storedOrdinals.nonEmpty) {
                    // === PATH 2: COLD RESTART — resume from latest ordinal on disk ===
                    val latestOrdinal = storedOrdinals.max
                    logger.info(
                      s"Cold restart detected: found ${storedOrdinals.size} snapshots on disk, latest ordinal=$latestOrdinal"
                    ) >>
                      (storages.globalSnapshot.get(latestOrdinal), infoStorage.read(latestOrdinal)).flatMapN {
                        case (Some(latestSnapshot), Some(latestInfo)) =>
                          hasherSelector.withCurrent { implicit hasher =>
                            for {
                              hashedSnapshot <- latestSnapshot.toHashed[IO]
                              _ <- storages.globalSnapshot.setHeadForRecovery(latestSnapshot, latestInfo)
                              _ <- sharedStorages.lastGlobalSnapshot.setForRecovery(hashedSnapshot, latestInfo)
                              _ <- sharedStorages.lastNGlobalSnapshot.setForRecovery(hashedSnapshot, latestInfo)
                              // FINDING-S01 fail-closed cold-restart gate. The persisted `GlobalSnapshotInfo` has NO field for the
                              // MPT-native consensus partitions (`ConsumedAllowSpends` 33 / `Slashings` 34), and the in-memory MPT store
                              // is empty at boot (SharedStorages seeds an empty producer), so a plain from-GSI rebuild would silently
                              // resurface WITHOUT the cross-shard spent-set — re-opening consumed allow-spends for a double-spend and
                              // committing a base whose root diverges from the last snapshot's SIGNED `stateProof.mptRoot`.
                              // Seed order: (1) `syncFromPersistedMptVerified` — byte-faithful reload of the node's OWN persisted MPT at
                              // `latestOrdinal` (carries 33/34 verbatim, so it reproduces the signed root by construction — a node with
                              // a non-empty spent-set restarts WITHOUT re-bootstrapping); (2) `syncFromGlobalSnapshotInfoVerified` —
                              // rebuilds ONLY when a from-GSI candidate reproduces the signed root (always at `numShards = 1` / empty
                              // spent-set — the normal restart); (3) fail closed.
                              persistedAdopted <- sharedStorages.mptStore
                                .syncFromPersistedMptVerified(latestOrdinal, latestSnapshot.value.stateProof.mptRoot)
                              _ <- logger
                                .info(
                                  s"Cold restart: adopted OWN persisted MPT bytes at ordinal=$latestOrdinal " +
                                    s"(root == signed stateProof.mptRoot; ConsumedAllowSpends/Slashings preserved verbatim)"
                                )
                                .whenA(persistedAdopted)
                              mptAdopted <-
                                if (persistedAdopted) IO.pure(true)
                                else
                                  sharedStorages.mptStore
                                    .syncFromGlobalSnapshotInfoVerified(latestInfo, latestOrdinal, latestSnapshot.value.stateProof.mptRoot)(
                                      globalStateProofSelector,
                                      withdrawalTimeLimit
                                    )
                              _ <- IO
                                .raiseError[Unit](
                                  new RuntimeException(
                                    s"Cold restart at ordinal=$latestOrdinal: the persisted GlobalSnapshotInfo cannot reproduce the " +
                                      s"snapshot's SIGNED stateProof.mptRoot — its MPT-native ConsumedAllowSpends/Slashings partitions are " +
                                      s"not carried by the GSI. FAILING CLOSED rather than booting with a wiped cross-shard spent-set. " +
                                      s"Restore this node's own root-verified persisted bytes, or recover through authenticated ancestry " +
                                      s"fetch and global replay (or a separately audited finality certificate)."
                                  )
                                )
                                .whenA(!mptAdopted)
                              rootedGenesisOperatorKeys <- L0GenesisLoader
                                .materializeRootedGenesisOperatorKeys[IO](sharedStorages.mptStore)
                              localGenesisData <- GenesisOperatorKeyAnchor.requireJson(method.genesisPath) match {
                                case Right(path) => GenesisLoader.make[IO, GlobalSnapshot].loadL0Genesis(path)
                                case Left(error) =>
                                  IO.raiseError[io.constellationnetwork.node.shared.domain.genesis.types.L0GenesisData](error)
                              }
                              _ <- L0GenesisLoader.requireGenesisDataMatchesRooted[IO](
                                localGenesisData,
                                rootedGenesisOperatorKeys
                              )
                              _ <- IO
                                .raiseError[Unit](
                                  new RuntimeException(
                                    s"Cold restart at ordinal=$latestOrdinal: persisted GlobalSnapshotInfo genesis operator keys do not " +
                                      "equal the root-authenticated field-24 partition. Refusing to construct the next snapshot from " +
                                      "unrooted or stale identity state."
                                  )
                                )
                                .unlessA(latestInfo.genesisOperatorKeys === rootedGenesisOperatorKeys)
                              _ <- L0GenesisLoader.requireLocalRegistryMatchesRooted[IO](
                                sharedServices.operatorKeyRegistry,
                                rootedGenesisOperatorKeys
                              )
                              _ <- services.consensus.manager
                                .startFacilitatingAfterRollback(
                                  latestSnapshot.ordinal,
                                  GlobalConsensusOutcome(
                                    latestSnapshot.ordinal,
                                    Facilitators(List(nodeId)),
                                    RemovedFacilitators.empty,
                                    WithdrawnFacilitators.empty,
                                    EligibleFacilitators.empty,
                                    Finished(latestSnapshot, latestInfo, EventTrigger, Candidates.empty, Hash.empty, hashedSnapshot.hash)
                                  )
                                )
                              _ <- logger.info(s"Recovered from disk at ordinal=$latestOrdinal")
                            } yield ()
                          }
                        case _ =>
                          IO.raiseError(
                            new RuntimeException(
                              s"Cold restart: snapshot info for ordinal=$latestOrdinal exists but snapshot or info data missing"
                            )
                          )
                      }

                  } else if (method.peerToJoin.isDefined) {
                    // === PATH 3: PEER DOWNLOAD DISABLED — a peer tip is not an economic-validity proof ===
                    val peerUrl = method.peerToJoin.get
                    IO.raiseError(
                      new RuntimeException(
                        s"Cannot bootstrap from --nakamoto-peer $peerUrl: single-peer latest-state adoption is disabled. " +
                          "Startup must fetch authenticated ancestry and globally replay every transition before installing economic state, " +
                          "or verify a separately audited finality certificate. Use this node's own persisted root-verified state or genesis " +
                          "until one of those bootstrap protocols is implemented."
                      )
                    )

                  } else if (method.genesisPath.isDefined) {
                    // === PATH 4: FRESH GENESIS — start from genesis CSV OR Tier-1 l0-genesis.json ===
                    val gPath = method.genesisPath.get

                    // Tier-1 test-vector dispatch: extension-driven, opt-in. The CSV path is the
                    // canonical default; the JSON branch loads an `L0GenesisData` payload (balances +
                    // delegated-stake records + node-collateral records + protocol params) and
                    // augments the in-memory `GlobalSnapshotInfo` AFTER `toGlobalSnapshotInfo` per
                    // Option (ii) in `IMPLEMENTATION-PLAN-POST-VALIDATION.md` §1.1 — the on-disk
                    // `Signed[GlobalSnapshot]` stays V1.
                    val genesisLoader = GenesisLoader.make[IO, GlobalSnapshot]

                    // (balanceMap, gsiAugmenter): one for the GlobalSnapshot.mkGenesis call, one for
                    // overlaying delegated-stake + collateral entries onto the runtime GSI.
                    val loadStep: IO[(Map[Address, balance.Balance], GlobalSnapshotInfo => IO[GlobalSnapshotInfo])] =
                      if (gPath.extName == ".json") {
                        genesisLoader.loadL0Genesis(gPath).flatMap { data =>
                          val balanceMap = data.initialBalanceMap
                          IO.raiseError[(Map[Address, balance.Balance], GlobalSnapshotInfo => IO[GlobalSnapshotInfo])](
                            new RuntimeException(
                              s"L0 genesis JSON at $gPath has no operators — refusing to start with empty operator set."
                            )
                          ).whenA(data.operators.isEmpty) >>
                            logger.info(
                              s"Loaded L0 genesis JSON: ${data.operators.size} operators, " +
                                s"${data.delegatedStakes.size} delegated-stakes, " +
                                s"${data.nodeCollaterals.size} node-collaterals, " +
                                s"${data.initialBalances.size} initial balances"
                            ) >> IO.pure(
                              (
                                balanceMap,
                                (base: GlobalSnapshotInfo) =>
                                  hasherSelector.withCurrent { implicit hasher =>
                                    L0GenesisLoader.augmentSnapshotInfo[IO](base, data)
                                  }
                              )
                            )
                        }
                      } else {
                        genesisLoader.loadBalances(gPath).flatMap { accounts =>
                          IO.raiseError[(Map[Address, balance.Balance], GlobalSnapshotInfo => IO[GlobalSnapshotInfo])](
                            new RuntimeException(
                              s"Genesis CSV at $gPath loaded 0 balances — file may be empty or unreadable " +
                                s"(check CL_GENESIS_FILE mount). Refusing to start with empty genesis state."
                            )
                          ).whenA(accounts.isEmpty) >>
                            logger.info(s"Loaded ${accounts.size} genesis balances from $gPath") >> IO.pure(
                              (
                                accounts.map(a => (a.address, a.balance)).toMap,
                                (base: GlobalSnapshotInfo) => IO.pure(base)
                              )
                            )
                        }
                      }

                    loadStep.flatMap {
                      case (balanceMap, augmenter) =>
                        val genesis = GlobalSnapshot.mkGenesis(balanceMap, method.startingEpochProgress)
                        hasherSelector.withCurrent { implicit hasher =>
                          Signed.forAsyncHasher[IO, GlobalSnapshot](genesis, keyPair).flatMap(_.toHashed[IO])
                        }.flatMap { hashedGenesis =>
                          GlobalSnapshotLocalFileSystemStorage.make[IO](cfg.snapshot.snapshotPath).flatMap {
                            fullGlobalSnapshotLocalFileSystemStorage =>
                              hasherSelector.withCurrent { implicit hasher =>
                                fullGlobalSnapshotLocalFileSystemStorage.write(hashedGenesis.signed) >> {
                                  // Order matters: apply the Tier-1 augmenter BEFORE building the
                                  // ord=1 incremental snapshot. The augmenter overlays
                                  // `activeDelegatedStakes`, `activeNodeCollaterals`, and the immutable
                                  // long-term-signed genesis KES+VRF identities (JSON path);
                                  // CSV path is identity. We pass the post-augmentation GSI to
                                  // `mkFirstIncrementalSnapshot` so the stateProof in the snapshot
                                  // matches the GSI + MPT we are about to persist. The CSV path
                                  // is unaffected (augmenter == identity → same bytes as before).
                                  val baseGsi = hashedGenesis.info.toGlobalSnapshotInfo
                                  augmenter(baseGsi).flatMap { globalSnapshotInfo =>
                                    L0GenesisLoader
                                      .requireLocalRegistryMatchesRooted[IO](
                                        sharedServices.operatorKeyRegistry,
                                        globalSnapshotInfo.genesisOperatorKeys
                                      ) >> GlobalSnapshot.mkFirstIncrementalSnapshot[IO](hashedGenesis, globalSnapshotInfo).flatMap {
                                      firstIncrementalSnapshot =>
                                        Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](firstIncrementalSnapshot, keyPair).flatMap {
                                          signedFirstIncrementalSnapshot =>
                                            for {
                                              hashedSnapshot <- signedFirstIncrementalSnapshot.toHashed[IO]
                                              _ <- initializeStorages[IO](
                                                storages.globalSnapshot,
                                                sharedStorages.lastNGlobalSnapshot,
                                                sharedStorages.lastGlobalSnapshot,
                                                programs.download,
                                                hashedSnapshot,
                                                globalSnapshotInfo
                                              )
                                              mptAdopted <- sharedStorages.mptStore
                                                .syncFromGlobalSnapshotInfoVerified(
                                                  globalSnapshotInfo,
                                                  hashedSnapshot.ordinal,
                                                  hashedSnapshot.signed.value.stateProof.mptRoot
                                                )(
                                                  globalStateProofSelector,
                                                  withdrawalTimeLimit
                                                )
                                              _ <- IO
                                                .raiseError[Unit](
                                                  new RuntimeException(
                                                    "Fresh genesis MPT rebuild did not reproduce the signed first-incremental mptRoot"
                                                  )
                                                )
                                                .unlessA(mptAdopted)
                                              rootedGenesisOperatorKeys <- L0GenesisLoader
                                                .materializeRootedGenesisOperatorKeys[IO](sharedStorages.mptStore)
                                              _ <- IO
                                                .raiseError[Unit](
                                                  new RuntimeException(
                                                    "Fresh genesis field-24 operator identity partition differs from the GSI used to sign " +
                                                      "the first incremental snapshot"
                                                  )
                                                )
                                                .unlessA(rootedGenesisOperatorKeys === globalSnapshotInfo.genesisOperatorKeys)
                                              _ <- services.consensus.manager
                                                .startFacilitatingAfterRollback(
                                                  signedFirstIncrementalSnapshot.ordinal,
                                                  GlobalConsensusOutcome(
                                                    signedFirstIncrementalSnapshot.ordinal,
                                                    Facilitators(List(nodeId)),
                                                    RemovedFacilitators.empty,
                                                    WithdrawnFacilitators.empty,
                                                    EligibleFacilitators.empty,
                                                    Finished(
                                                      signedFirstIncrementalSnapshot,
                                                      globalSnapshotInfo,
                                                      EventTrigger,
                                                      Candidates.empty,
                                                      Hash.empty,
                                                      hashedSnapshot.hash
                                                    )
                                                  )
                                                )
                                            } yield ()
                                        }
                                    }
                                  }
                                }
                              }
                          }
                        }
                    }

                  } else {
                    // === PATH 5: ERROR — no bootstrap source ===
                    IO.raiseError(
                      new RuntimeException(
                        "Cannot start: no local snapshot data, --genesis-csv, or --rollback-hash was provided. " +
                          "Single-peer latest-state bootstrap via --nakamoto-peer is disabled until ancestry is globally replayed " +
                          "or a separately audited finality certificate is available."
                      )
                    )
                  }
                }
              }
            }
          } >>
            services.cluster.createSession >>
            services.session.createSession >>
            // Populate ClusterStorage from the seedlist so /cluster/info returns the
            // full validator set. In Nakamoto there's no BFT join handshake, so peers
            // never register via addPeer — we seed them from the seedlist at startup.
            (nodeShared.seedlist match {
              case Some(entries) =>
                storages.cluster.getToken.flatMap { maybeToken =>
                  val token = maybeToken.getOrElse(ClusterSessionToken(Generation(1L)))
                  // Drop metagraph-op-marked entries: they're in the seedlist only so gl0
                  // accepts state-channel binary signatures, but they aren't gl0 cluster
                  // members. Including them in /cluster/info caused metagraph cl1/dl1 to
                  // pick them as "global L0 peers" and fail to fetch global snapshots.
                  entries.toList
                    .filter(_.peerId =!= nodeId)
                    .filterNot(_.alias.exists(_.value.value == "metagraph-op"))
                    .traverse_ { entry =>
                      entry.connectionInfo match {
                        case Some(connInfo) =>
                          val publicPort = com.comcast.ip4s.Port
                            .fromInt(connInfo.p2pPort.value - 1)
                            .getOrElse(connInfo.p2pPort)
                          val sessionGen = eu.timepit.refined.types.numeric.PosLong.unsafeFrom(System.currentTimeMillis())
                          val peer = Peer(
                            id = entry.peerId,
                            ip = com.comcast.ip4s.Host.fromString(connInfo.ipAddress.toString).get,
                            publicPort = publicPort,
                            p2pPort = connInfo.p2pPort,
                            clusterSession = token,
                            session = SessionToken(Generation(sessionGen)),
                            state = NodeState.Ready,
                            responsiveness = Responsive,
                            jar = Hash.empty
                          )
                          storages.cluster.addPeer(peer).void
                        case None =>
                          // 1-field seedlist entry (peerId only) — can't add to ClusterStorage
                          // without connection info. The peer will be absent from /cluster/info.
                          IO.unit
                      }
                    }
                }
              case None => IO.unit
            }) >>
            storages.node.setNodeState(NodeState.Ready)
        }
      }).asResource
    } yield ()
  }
}
