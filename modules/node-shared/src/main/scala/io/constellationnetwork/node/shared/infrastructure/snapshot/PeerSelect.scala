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
import io.constellationnetwork.node.shared.domain.snapshot.{PeerSelect, PeerSelection}
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
  * Selects a single L0 peer from a unique largest ordinal cohort and then a unique largest hash cohort at that ordinal. These are local
  * plurality observations, not authenticated consensus or Phase-2 evidence. Callers must authenticate downloaded state before mutation.
  *
  * Selection process:
  *   1. Filter ready peers and uniformly sample up to `maxSampleSize` candidates. 2. Query each candidate's latest snapshot ordinal; the
  *      unique largest ordinal cohort wins. 3. Query only that cohort's members for the snapshot hash at the selected ordinal; the unique
  *      largest hash-equivalence class wins. 4. Pick one peer uniformly at random from the winning hash cohort.
  */
object PeerSelect {
  val peerSelectLoggerName = "PeerSelectLogger"

  @derive(encoder, show)
  case class FilteredPeerDetails(
    initialPeers: NonEmptyList[Peer],
    latestOrdinals: NonEmptyList[SnapshotOrdinal],
    ordinalDistribution: List[(SnapshotOrdinal, NonEmptyList[Peer])],
    selectedOrdinal: SnapshotOrdinal,
    hashDistribution: List[(Hash, NonEmptyList[Peer])],
    selectedHash: Hash,
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

    def select: F[PeerSelection] = getFilteredPeerDetails
      .flatTap(details => logger.debug(details.asJson.noSpaces))
      .map(details => PeerSelection(details.selectedPeer, details.selectedOrdinal, details.selectedHash))

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
      ordinalSelection <- MonadThrow[F].fromEither(uniqueLargestCohort(peerOrdinals))
      (selectedOrdinal, ordinalCohort) = ordinalSelection
      peerSnapshotHashes <- ordinalCohort
        .parTraverseN(maxConcurrentPeerInquiries)(getSnapshotHashByPeer(_, selectedOrdinal))
        .flatMap { maybePeerSnapshotHashes =>
          MonadThrow[F].fromOption(
            maybePeerSnapshotHashes.toList.flatten.toNel,
            NoHashes
          )
        }
      peerDistribution = peerSnapshotHashes.groupMap { case (_, hash) => hash } { case (peer, _) => peer }
      hashSelection <- MonadThrow[F].fromEither(uniqueLargestCohort(peerSnapshotHashes))
      (selectedHash, peerCandidates) = hashSelection
      selectedPeer <- Random[F].elementOf(peerCandidates.toList).map(L0Peer.fromPeer)
    } yield
      FilteredPeerDetails(
        peers,
        latestOrdinals,
        ordinalDistribution.toList,
        selectedOrdinal,
        peerDistribution.toList,
        selectedHash,
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

  /** Return the only largest value cohort, rejecting ties instead of making map iteration order authoritative. */
  private[snapshot] def uniqueLargestCohort[A, B](
    observations: NonEmptyList[(A, B)]
  ): Either[AmbiguousPeerCohort.type, (B, NonEmptyList[A])] = {
    val cohorts = observations.toList.groupMap { case (_, value) => value } { case (item, _) => item }.toList.map {
      case (value, items) => value -> NonEmptyList.fromListUnsafe(items)
    }
    val largestSize = cohorts.iterator.map(_._2.length).max
    cohorts.filter(_._2.length === largestSize) match {
      case winner :: Nil => Right(winner)
      case _             => Left(AmbiguousPeerCohort)
    }
  }

  case object AmbiguousPeerCohort extends NoStackTrace
}
