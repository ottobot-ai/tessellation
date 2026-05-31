package io.constellationnetwork.node.shared.domain.cluster.programs

import cats.data.NonEmptySet
import cats.effect.std.Random
import cats.effect.{Sync, Temporal}
import cats.syntax.all._
import cats.{Applicative, MonadThrow}

import scala.collection.immutable.SortedSet
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import io.constellationnetwork.node.shared.domain.cluster.programs.L0PeerDiscovery.L0PeerDiscoveryError
import io.constellationnetwork.node.shared.domain.cluster.storage.L0ClusterStorage
import io.constellationnetwork.node.shared.http.p2p.clients.L0ClusterClient
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.peer.{L0Peer, P2PContext, PeerId}

import org.typelevel.log4cats.slf4j.Slf4jLogger

object L0PeerDiscovery {

  def make[F[_]: Sync: Random](
    l0ClusterClient: L0ClusterClient[F],
    l0ClusterStorage: L0ClusterStorage[F]
  ): L0PeerDiscovery[F] =
    new L0PeerDiscovery[F](l0ClusterClient, l0ClusterStorage) {}

  case object L0PeerDiscoveryError extends NoStackTrace {
    override def getMessage: String = s"Error during L0 peer discovery!"
  }

  /** #290: startup L0-discovery retry budget. `discoverFrom` is one-shot and raises on any transient connection error (NoRouteToHost /
    * connection-refused / timeout). At scale (16gl0 + N metagraphs boot storm) a single transient route hiccup when a later metagraph's 3rd
    * L1 node first reaches its L0 peer would otherwise kill startup permanently — the node never advances past `NodeState.Initial`, and the
    * runtime 10s-retry discovery stream never starts because Resource init failed first. Exponential backoff from
    * `StartupDiscoveryBaseDelay` over `StartupDiscoveryMaxRetries` retries (2,4,8,…,256s ≈ 8.5 min total) comfortably outlasts a boot-storm
    * route-readiness window. Named constants (not `sys.env`, per the project HOCON rule); promote to typed config if they ever need to be
    * operator-tunable.
    */
  val StartupDiscoveryMaxRetries: Int = 8
  val StartupDiscoveryBaseDelay: FiniteDuration = 2.seconds

  /** #290: the whole bounded-backoff retry, run HERE (companion, context-bound to [[Temporal]] ONLY) rather than inside the
    * [[L0PeerDiscovery]] class. The class carries `Sync[F]` and [[L0PeerDiscovery.discoverFromWithRetry]] adds `Temporal[F]`; `Sync` and
    * `Temporal` both extend `MonadError`/`Applicative`, so `retryingOnSomeErrors` (which summons `MonadError`/`Sleep`) and
    * `RetryPolicies.*` (which summon `Applicative`/`Apply`) are AMBIGUOUS anywhere both instances are in scope. In this companion method
    * `Temporal[F]` is the SOLE effect instance in scope, so every summon resolves unambiguously. `onError` is taken as a plain `(Throwable,
    * RetryDetails) => F[Unit]` value built by the caller (e.g. a `logger.warn` on the already-constructed logger), so it carries no
    * instance-summoning into this scope.
    */
  private[programs] def withStartupRetry[F[_]: Temporal](
    maxRetries: Int,
    baseDelay: FiniteDuration,
    onError: (Throwable, retry.RetryDetails) => F[Unit]
  )(op: F[Unit]): F[Unit] = {
    import retry._

    retryingOnSomeErrors(
      policy = RetryPolicies.exponentialBackoff[F](baseDelay).join(RetryPolicies.limitRetries[F](maxRetries)),
      isWorthRetrying = (_: Throwable) => Temporal[F].pure(true),
      onError = onError
    )(op)
  }
}

sealed abstract class L0PeerDiscovery[F[_]: Sync: Random] private (
  l0ClusterClient: L0ClusterClient[F],
  l0ClusterStorage: L0ClusterStorage[F]
) {

  val logger = Slf4jLogger.getLogger[F]

  def discover(lastFacilitators: NonEmptySet[PeerId]): F[Unit] =
    l0ClusterStorage.getPeers
      .map(_.map(_.id).intersect(lastFacilitators))
      .map(_.toList)
      .flatMap(Random[F].shuffleList)
      .flatMap(_.headOption.flatTraverse(l0ClusterStorage.getPeer))
      .flatMap {
        case Some(facilitatorPeer) =>
          getPeersFrom(facilitatorPeer)
            .flatMap(l0ClusterStorage.setPeers)
        case None =>
          logger.warn("No known peers found among last facilitators, falling back to random peer") >>
            l0ClusterStorage.getPeers
              .map(_.toNonEmptyList.toList)
              .flatMap(Random[F].shuffleList)
              .flatMap(peers => tryDiscoverFromPeers(peers.take(2)))
      }
      .handleErrorWith { error =>
        logger.warn(error)(s"An error occurred during L0 peer discovery")
      }

  private def tryDiscoverFromPeers(peers: List[L0Peer]): F[Unit] =
    peers match {
      case peer :: rest =>
        getPeersFrom(peer)
          .map(_.toSortedSet)
          .flatMap(l0ClusterStorage.addPeers)
          .handleErrorWith { error =>
            logger.warn(error)(s"Failed to discover L0 peers from ${peer.id}, ${rest.size} attempts remaining") >>
              tryDiscoverFromPeers(rest)
          }
      case Nil =>
        logger.warn("All L0 peer discovery attempts exhausted")
    }

  private def getPeersFrom(peer: P2PContext): F[NonEmptySet[L0Peer]] =
    l0ClusterClient.getPeers
      .run(peer)
      .map(_.filter(p => NodeState.ready.contains(p.state)))
      .map(_.map(L0Peer.fromPeerInfo))
      .flatMap { s =>
        NonEmptySet
          .fromSet(SortedSet.from(s))
          .fold {
            (new Throwable("Unexpected state - no peers found but at least one should be available")).raiseError[F, NonEmptySet[L0Peer]]
          }(Applicative[F].pure)
      }

  def discoverFrom(peer: P2PContext): F[Unit] =
    getPeersFrom(peer)
      .map(_.toSortedSet)
      .flatMap(l0ClusterStorage.addPeers)
      .handleErrorWith { e =>
        logger.error(e)("Error during L0 peer discovery!") >>
          MonadThrow[F].raiseError[Unit](L0PeerDiscoveryError)
      }

  /** #290: [[discoverFrom]] wrapped in bounded exponential-backoff retry, for the one-shot STARTUP join sequence (gl1 `Main` + metagraph
    * `CurrencyL1App`). The bare `discoverFrom` raises on any transient connection error and, at the startup call sites, that error is
    * terminal (Resource init aborts; the runtime retry stream never starts). Retrying here lets a transient boot-storm route hiccup
    * self-heal so the node still reaches `Ready`. The steady-state `GlobalSnapshotAlignment.l0PeerDiscovery` stream keeps its own
    * independent 10s retry; this only hardens the cold-start gap. Mirrors the proven `GlobalSnapshotAlignment.withRetry` shape; needs
    * [[Temporal]] for the inter-retry sleep.
    */
  def discoverFromWithRetry(
    peer: P2PContext,
    maxRetries: Int = L0PeerDiscovery.StartupDiscoveryMaxRetries,
    baseDelay: FiniteDuration = L0PeerDiscovery.StartupDiscoveryBaseDelay
  )(implicit T: Temporal[F]): F[Unit] =
    // All retry-instance summoning happens inside the Temporal-only companion `withStartupRetry`; here we only pass an op +
    // an onError closure (a `logger.warn` on the already-built logger), so the class's `Sync[F]` never clashes with `T`.
    L0PeerDiscovery.withStartupRetry[F](
      maxRetries,
      baseDelay,
      (err: Throwable, details: retry.RetryDetails) =>
        logger.warn(err)(s"Startup L0 peer discovery failed (attempt ${details.retriesSoFar + 1}/${maxRetries + 1}); retrying")
    )(discoverFrom(peer))
}
