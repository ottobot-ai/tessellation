package io.constellationnetwork.dag.l0.modules

import java.security.KeyPair

import cats.effect.Async
import cats.effect.std.Supervisor
import cats.syntax.functor._
import cats.syntax.traverse._

import io.constellationnetwork.dag.l0.config.types.AppConfig
import io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotEventsPublisherDaemon
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.domain.Daemon
import io.constellationnetwork.node.shared.domain.gossip.Gossip
import io.constellationnetwork.node.shared.infrastructure.cluster.daemon.NodeStateDaemon
import io.constellationnetwork.node.shared.infrastructure.collateral.daemon.CollateralDaemon
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.{HasherSelector, SecurityProvider}

object Daemons {

  /** Start GL0 Nakamoto daemons.
    *
    *   - No DownloadDaemon (Nakamoto nodes sync via GossipSub sidecar, not tessellation peer download)
    *   - No BFT EventGossipDaemon (events are disseminated via `Gossip.spread` → sidecar GossipSub)
    *   - EventsPublisher still runs (events accumulate in mempool for snapshot inclusion) and spreads new events as rumors through the
    *     sidecar so all GL0 nodes share the same mempool view
    *   - No BFT consensus trigger (Nakamoto uses VRF slot-based production)
    */
  def startNakamoto[F[_]: Async: Supervisor: HasherSelector: SecurityProvider, R <: CliMethod](
    storages: Storages[F],
    services: Services[F, R],
    queues: Queues[F],
    gossip: Gossip[F],
    nodeId: PeerId,
    keyPair: KeyPair,
    cfg: AppConfig
  ): F[Unit] =
    List[Daemon[F]](
      NodeStateDaemon.make(storages.node, services.gossip),
      GlobalSnapshotEventsPublisherDaemon
        .make(
          queues.stateChannelOutput,
          queues.l1Output,
          queues.l1AllowSpendOutput,
          queues.l1TokenLockOutput,
          queues.updateNodeParametersOutput,
          queues.delegatedStakeOutput,
          queues.nodeCollateralOutput,
          queues.kesRegistrationCertOutput,
          keyPair,
          services.eventMempool,
          gossip,
          triggerEventConsensus = None, // No BFT consensus to trigger
          services.consensus.storage.getLastConsensusOutcome.map(_.fold(0)(_.facilitators.value.size)),
          cfg.snapshot.consensus
        ),
      CollateralDaemon.make(services.collateral, storages.globalSnapshot, storages.cluster)
    ).traverse(_.start).void

}
