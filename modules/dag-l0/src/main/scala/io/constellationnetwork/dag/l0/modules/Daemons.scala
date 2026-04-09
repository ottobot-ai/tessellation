package io.constellationnetwork.dag.l0.modules

import java.security.KeyPair

import cats.effect.Async
import cats.effect.std.Supervisor
import cats.syntax.functor._
import cats.syntax.traverse._

import io.constellationnetwork.dag.l0.config.types.AppConfig
import io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotEventsPublisherDaemon
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event.GlobalSnapshotEvent
import io.constellationnetwork.dag.l0.infrastructure.trust.TrustStorageUpdater
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.domain.Daemon
import io.constellationnetwork.node.shared.infrastructure.cluster.daemon.NodeStateDaemon
import io.constellationnetwork.node.shared.infrastructure.collateral.daemon.CollateralDaemon
import io.constellationnetwork.node.shared.infrastructure.gossip.event.EventGossipDaemon
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.{HasherSelector, SecurityProvider}

object Daemons {

  /** Start GL0 Nakamoto daemons.
    *
    *   - No DownloadDaemon (Nakamoto nodes sync via GossipSub sidecar, not tessellation peer download)
    *   - No EventGossipDaemon.start (P2P event gossip replaced by libp2p GossipSub)
    *   - EventsPublisher still runs (events accumulate in mempool for snapshot inclusion) but uses a no-op gossip daemon so publish calls
    *     are silent
    *   - No consensus trigger (Nakamoto uses VRF slot-based production, not event-triggered BFT rounds)
    */
  def startNakamoto[F[_]: Async: Supervisor: HasherSelector: SecurityProvider, R <: CliMethod](
    storages: Storages[F],
    services: Services[F, R],
    queues: Queues[F],
    nodeId: PeerId,
    keyPair: KeyPair,
    cfg: AppConfig
  ): F[Unit] = {
    val noopGossip = EventGossipDaemon.noop[F, GlobalSnapshotEvent, GlobalStateKey]

    List[Daemon[F]](
      NodeStateDaemon.make(storages.node, services.gossip),
      Daemon.periodic(storages.trust.updateTrustWithBiases(nodeId), cfg.trust.daemon.interval),
      GlobalSnapshotEventsPublisherDaemon
        .make(
          queues.stateChannelOutput,
          queues.l1Output,
          queues.l1AllowSpendOutput,
          queues.l1TokenLockOutput,
          queues.updateNodeParametersOutput,
          queues.delegatedStakeOutput,
          queues.nodeCollateralOutput,
          keyPair,
          services.eventMempool,
          noopGossip,
          triggerEventConsensus = None, // No BFT consensus to trigger
          services.consensus.storage.getLastConsensusOutcome.map(_.fold(0)(_.facilitators.value.size)),
          cfg.snapshot.consensus
        ),
      CollateralDaemon.make(services.collateral, storages.globalSnapshot, storages.cluster),
      TrustStorageUpdater.daemon(services.trustStorageUpdater)
    ).traverse(_.start).void
  }

}
