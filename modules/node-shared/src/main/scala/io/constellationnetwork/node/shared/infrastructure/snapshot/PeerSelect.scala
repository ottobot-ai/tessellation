package io.constellationnetwork.node.shared.infrastructure.snapshot

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.std.Random
import cats.effect.syntax.concurrent._
import cats.syntax.applicative._
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.list._

import scala.util.control.NoStackTrace

import io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage
import io.constellationnetwork.node.shared.domain.snapshot.PeerSelect
import io.constellationnetwork.node.shared.http.p2p.clients.SnapshotClient
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.node.NodeState.Ready
import io.constellationnetwork.schema.peer.Peer.toP2PContext
import io.constellationnetwork.schema.peer.{L0Peer, Peer}
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo}
import io.constellationnetwork.security.hash.Hash

import derevo.cats.show
import derevo.circe.magnolia.encoder
import derevo.derive
import io.circe.syntax.EncoderOps
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Peer selection for snapshot download.
  *
  * Selects a single L0 peer whose snapshot ordinal is in the majority cohort and whose snapshot hash for that ordinal is in the majority
  * cohort.
  *
  * Selection process:
  *   1. Filter ready peers and uniformly sample up to `maxSampleSize` candidates. 2. Query each candidate's latest snapshot ordinal; the
  *      most popular ordinal becomes `majorityOrdinal`. 3. Query each candidate's snapshot hash at `majorityOrdinal`; the largest
  *      hash-equivalence class wins. 4. Pick one peer uniformly at random from the winning class.
  */
object PeerSelect {
  val peerSelectLoggerName = "PeerSelectLogger"

  @derive(encoder, show)
  case class FilteredPeerDetails(
    initialPeers: NonEmptyList[Peer],
    latestOrdinals: NonEmptyList[SnapshotOrdinal],
    ordinalDistribution: List[(SnapshotOrdinal, NonEmptyList[Peer])],
    majorityOrdinal: SnapshotOrdinal,
    hashDistribution: List[(Hash, NonEmptyList[Peer])],
    peerCandidates: NonEmptyList[Peer],
    selectedPeer: L0Peer
  )

  val maxConcurrentPeerInquiries = 10
  val maxSampleSize: Int = 20

  case object NoPeersToSelect extends NoStackTrace
  case object NoHashes extends NoStackTrace

  def make[F[_]: Async: Random, S <: Snapshot, SI <: SnapshotInfo[_]](
    storage: ClusterStorage[F],
    snapshotClient: SnapshotClient[F, S, SI]
  ): PeerSelect[F] = new PeerSelect[F] {

    val logger = Slf4jLogger.getLoggerFromName[F](peerSelectLoggerName)

    def select: F[L0Peer] = getFilteredPeerDetails
      .flatTap(details => logger.debug(details.asJson.noSpaces))
      .map(_.selectedPeer)

    def getFilteredPeerDetails: F[FilteredPeerDetails] = for {
      peers <- storage.getResponsivePeers
        .map(_.filter(_.state === Ready))
        .flatMap(getPeerSublist)
        .flatMap { peerSublist =>
          MonadThrow[F].fromOption(peerSublist.toNel, NoPeersToSelect)
        }
      peerOrdinals <- peers.parTraverseN(maxConcurrentPeerInquiries) { peer =>
        snapshotClient.getLatestOrdinal(peer).map((peer, _))
      }
      latestOrdinals = peerOrdinals.map { case (_, ordinal) => ordinal }
      ordinalDistribution = peerOrdinals.groupMap { case (_, ordinal) => ordinal } { case (peer, _) => peer }
      (majorityOrdinal, _) = latestOrdinals.groupBy(identity).maxBy { case (_, ordinals) => ordinals.size }
      peerDistribution <- peers
        .parTraverseN(maxConcurrentPeerInquiries)(getSnapshotHashByPeer(_, majorityOrdinal))
        .flatMap { maybePeerSnapshotHashes =>
          MonadThrow[F].fromOption(
            maybePeerSnapshotHashes.toList.flatten.toNel,
            NoHashes
          )
        }
        .map(_.groupMap { case (_, hash) => hash } { case (peer, _) => peer })
      peerCandidates = peerDistribution.values.maxBy(_.length)
      selectedPeer <- Random[F].elementOf(peerCandidates.toList).map(L0Peer.fromPeer)
    } yield
      FilteredPeerDetails(
        peers,
        latestOrdinals,
        ordinalDistribution.toList,
        majorityOrdinal,
        peerDistribution.toList,
        peerCandidates,
        selectedPeer
      )

    /** Uniform-random sample (without replacement) of up to `maxSampleSize` peers. */
    def getPeerSublist(peers: Set[Peer]): F[List[Peer]] = {
      val peerList = peers.toList
      if (peerList.size <= maxSampleSize) peerList.pure[F]
      else Random[F].shuffleList(peerList).map(_.take(maxSampleSize))
    }

    def getSnapshotHashByPeer(peer: Peer, ordinal: SnapshotOrdinal): F[Option[(Peer, Hash)]] =
      snapshotClient.getHash(ordinal).run(peer).map(_.map((peer, _)))
  }
}
