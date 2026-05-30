package io.constellationnetwork.node.shared.domain.snapshot.services

import java.lang.Math.ceil

import cats.data._
import cats.effect.Async
import cats.effect.syntax.concurrent._
import cats.syntax.all._
import cats.{Applicative, Parallel, Show}

import scala.collection.immutable.SortedSet
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NoStackTrace

import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.ext.collection.FoldableOps.pickMajority
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.cluster.storage.L0ClusterStorage
import io.constellationnetwork.node.shared.domain.snapshot.Validator.isNextSnapshot
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.http.p2p.PeerResponse
import io.constellationnetwork.node.shared.http.p2p.clients.{GlobalFollowClient, L0GlobalSnapshotClient}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.follow.GlobalFollowSliceResponse
import io.constellationnetwork.schema.peer.{L0Peer, PeerId}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.CompatCodecs._
import io.constellationnetwork.validator.StateProofValidator

import eu.timepit.refined.auto.autoUnwrap
import eu.timepit.refined.types.numeric.PosLong
import io.circe.Json
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait GlobalL0Service[F[_]] {
  type LatestSnapshotTuple = (Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)
  def pullLatestSnapshot: F[LatestSnapshotTuple]
  def pullLatestSnapshotFromRandomPeer: F[LatestSnapshotTuple]
  def pullGlobalSnapshots: F[Either[LatestSnapshotTuple, List[Hashed[GlobalIncrementalSnapshot]]]]
  def pullGlobalSnapshots(ordinal: SnapshotOrdinal): F[Either[LatestSnapshotTuple, List[Hashed[GlobalIncrementalSnapshot]]]]
  def pullGlobalSnapshot(ordinal: SnapshotOrdinal): F[Option[Hashed[GlobalIncrementalSnapshot]]]
  def pullGlobalSnapshot(hash: Hash): F[Option[Hashed[GlobalIncrementalSnapshot]]]

  /** Highest GL0 snapshot ordinal that has reached finality.
    *
    * In BFT GL0 mode this equals the head ordinal (every snapshot is immediately final). In Nakamoto GL0 mode this lags the head: it's the
    * ordinal up to which attestation-2/3 OR depth-k confirmation has completed. Used by CL0 to gate state-channel-binary pruning on actual
    * finality so Nakamoto reorgs cannot silently drop binaries.
    *
    * Returns None if the remote GL0 has no snapshots yet (pre-genesis) or the request fails.
    */
  def pullLatestFinalizedOrdinal: F[Option[SnapshotOrdinal]]

  /** Pull GL0 snapshots in the range (lastOrdinal, finalizedOrdinal], i.e. only depth-k-finalized snapshots above our local head.
    *
    * Followers (gl1/cl1/dl1/ml0) MUST consume only finalized snapshots — by construction, GL0 reorgs can only happen within the depth-k
    * window, so finalized snapshots are immutable from the follower's perspective and `replaceByRefs` / `recoverFromOrphan` paths become
    * unreachable on the happy path. See #122.
    *
    * Returns an empty list if `finalizedOrdinal <= lastOrdinal` (nothing finalized beyond local head yet). Returns at most
    * `singlePullLimit` snapshots when configured.
    */
  def pullFinalizedGlobalSnapshots(
    lastOrdinal: SnapshotOrdinal,
    finalizedOrdinal: SnapshotOrdinal
  ): F[List[Hashed[GlobalIncrementalSnapshot]]]

  /** Fetch the latest-finalized consumed-field slice from a random GL0 peer (`GET /global-follow/slice/latest`).
    *
    * The gl1 own-slice follow path (Axis 2 — see `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`) uses this to obtain gl0's claimed
    * Address-keyed slice (`balances`, `lastTxRefs`, `lastAllowSpendRefs`, `lastTokenLockRefs`) at the latest finalized global ordinal,
    * which a follower then recompute-matches against the matching signed snapshot's `stateProof.<field>Proof` roots
    * ([[io.constellationnetwork.node.shared.domain.nakamoto.GlobalFollowMirrorVerifier]]). The slice carries its OWN ordinal, so the
    * verifier anchors against the snapshot AT that ordinal — never a cross-ordinal mismatch.
    *
    * Returns `None` when no [[GlobalFollowClient]] is wired (the cl0/cl1 callers that don't follow this path) or the peer has no finalized
    * ordinal yet / the request fails. The caller (`DAGSnapshotProcessor.applyGlobalSnapshotFn`) treats `None` as "do not advance, retry
    * next tick" — there is no `StateProofMismatch`-style recovery storm.
    */
  def getLatestFollowSlice: F[Option[GlobalFollowSliceResponse]]

  /** The #287 incremental fetch: `GET /global-follow/slice?since=<ordinalLong>`. Returns the change-set a follower holding the
    * consumed-field state at `since` needs to reach gl0's latest finalized ordinal (`baseOrdinal = Some(since)`), or — if `since` fell out
    * of gl0's projection ring — the full from-empty slice (`baseOrdinal = None`). Same `None` / no-recovery-storm semantics as
    * [[getLatestFollowSlice]]. The follower applies the diff only when `baseOrdinal` matches the tip it holds and falls back to a full
    * fetch otherwise, so a wrong base cannot advance the mirror.
    */
  def getFollowSliceSince(since: SnapshotOrdinal): F[Option[GlobalFollowSliceResponse]]
}

object GlobalL0Service {
  case object NoMajorityPeers extends Exception("No majority peers found in storage") with NoStackTrace
  case object NoPeersWithMajorityHash extends Exception("No peers returned snapshot with hash in the majority") with NoStackTrace
  case object NoMajoritySnapshotData extends Exception("Unable to determine latest snapshot data for majority") with NoStackTrace
  case object NoPeerAlignedWithMajority extends Exception("No peer available that is aligned with majority") with NoStackTrace

  def make[
    F[_]: Async: Parallel: SecurityProvider: HasherSelector: JsonSerializer
  ](
    l0GlobalSnapshotClient: L0GlobalSnapshotClient[F],
    globalL0ClusterStorage: L0ClusterStorage[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    singlePullLimit: Option[PosLong],
    maybeMajorityPeerIdSet: Option[NonEmptySet[PeerId]],
    mptStore: MptStore[F, GlobalStateKey],
    // gl1 own-slice follow transport (Axis 2). Optional so the cl0/cl1 callers that don't follow this
    // path keep their existing wiring; gl1's Main wires `Some(GlobalFollowClient.make(client))`.
    maybeGlobalFollowClient: Option[GlobalFollowClient[F]] = None
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): GlobalL0Service[F] =
    new GlobalL0Service[F] {

      private val numConcurrentQueries = 10
      private val logger = Slf4jLogger.getLoggerFromName[F](this.getClass.getName)
      private val maybeMajorityPeerIds = maybeMajorityPeerIdSet.map(_.toNonEmptyList)
      private val ordinalRange = 0L to 20L
      private val validator = StateProofValidator.forGlobal(Some(mptStore.underlying))

      implicit val hashShow: Show[Hash] = Hash.shortShow
      implicit val peerIdShow: Show[PeerId] = PeerId.shortShow

      private val noSnapshots = List.empty[Hashed[GlobalIncrementalSnapshot]]

      def pullLatestSnapshot: F[LatestSnapshotTuple] =
        maybeMajorityPeerIds.fold(pullLatestSnapshotFromRandomPeer)(pullLatestSnapshotWithMajorityHash)

      def pullLatestSnapshotFromRandomPeer: F[LatestSnapshotTuple] =
        globalL0ClusterStorage.getRandomPeer >>= pullLatestSnapshotFromPeer

      def pullGlobalSnapshot(hash: Hash): F[Option[Hashed[GlobalIncrementalSnapshot]]] =
        pullGlobalSnapshot(l0GlobalSnapshotClient.get(hash)).handleErrorWith { e =>
          logger
            .warn(e)(s"Failure pulling single snapshot with hash=${hash.show}")
            .as(none)
        }

      def pullGlobalSnapshot(ordinal: SnapshotOrdinal): F[Option[Hashed[GlobalIncrementalSnapshot]]] =
        pullGlobalSnapshot(l0GlobalSnapshotClient.get(ordinal)).handleErrorWith { e =>
          logger
            .warn(e)(s"Failure pulling single snapshot with ordinal=${ordinal.show}")
            .as(none)
        }

      def pullLatestFinalizedOrdinal: F[Option[SnapshotOrdinal]] =
        globalL0ClusterStorage.getRandomPeer.flatMap { peer =>
          l0GlobalSnapshotClient.getLatestFinalizedOrdinal.run(peer).map(_.some)
        }.handleErrorWith { e =>
          logger.warn(e)(s"Failure pulling latest finalized ordinal").as(none)
        }

      def getLatestFollowSlice: F[Option[GlobalFollowSliceResponse]] =
        maybeGlobalFollowClient match {
          case None                     => none[GlobalFollowSliceResponse].pure[F]
          case Some(globalFollowClient) =>
            // gl0 serves the slice on the PUBLIC port — the same port the L0Peer already targets — so we resolve a random
            // L0 cluster peer and run the PeerResponse against it, exactly like `pullLatestFinalizedOrdinal` above
            // (`L0Peer <: P2PContext`). Errors (peer down, 503 before any finalized ordinal) → None so the caller idles
            // and retries next tick (no recovery storm).
            globalL0ClusterStorage.getRandomPeer.flatMap { peer =>
              globalFollowClient.getLatestSlice.run(peer).map(_.some)
            }.handleErrorWith { e =>
              logger.warn(e)(s"Failure pulling latest follow slice").as(none)
            }
        }

      def getFollowSliceSince(since: SnapshotOrdinal): F[Option[GlobalFollowSliceResponse]] =
        maybeGlobalFollowClient match {
          case None                     => none[GlobalFollowSliceResponse].pure[F]
          case Some(globalFollowClient) =>
            // #287 incremental fetch — same peer-resolution + error-to-None handling as `getLatestFollowSlice`.
            globalL0ClusterStorage.getRandomPeer.flatMap { peer =>
              globalFollowClient.getSliceSince(since).run(peer).map(_.some)
            }.handleErrorWith { e =>
              logger.warn(e)(s"Failure pulling follow slice since=${since.show}").as(none)
            }
        }

      def pullGlobalSnapshots: F[Either[LatestSnapshotTuple, List[Hashed[GlobalIncrementalSnapshot]]]] =
        maybeMajorityPeerIds.fold(pullGlobalSnapshotsFromRandomPeer)(pullGlobalSnapshotsFromMajority)

      private def pullGlobalSnapshotsFromMajority(
        majorityPeerIds: NonEmptyList[PeerId]
      ): F[Either[LatestSnapshotTuple, List[Hashed[GlobalIncrementalSnapshot]]]] =
        lastGlobalSnapshotStorage.getOrdinal.flatMap {
          _.fold {
            pullLatestSnapshotWithMajorityHash(majorityPeerIds).map(_.asLeft[List[Hashed[GlobalIncrementalSnapshot]]])
          }(pullGlobalSnapshotsFromMajorityAtOrdinal(majorityPeerIds, _))
        }.handleErrorWith { e =>
          logger
            .warn(e)(s"Failure pulling global snapshots from majority")
            .as(noSnapshots.asRight[LatestSnapshotTuple])
        }

      private def pullGlobalSnapshotsFromMajorityAtOrdinal(
        majorityPeerIds: NonEmptyList[PeerId],
        ordinal: SnapshotOrdinal
      ): F[Either[LatestSnapshotTuple, List[Hashed[GlobalIncrementalSnapshot]]]] =
        for {
          msd <- getMajoritySnapshotData(majorityPeerIds, ordinal.next)
          l0Peers <- globalL0ClusterStorage.getPeers
          pulled <- (ordinal < msd.ordinal)
            .pure[F]
            .ifM(
              pullVerifiedSnapshots(ordinal.next, msd, l0Peers),
              noSnapshots.pure[F]
            )
        } yield pulled.asRight[LatestSnapshotTuple]

      private def pullLatestSnapshotWithMajorityHash(
        majorityPeerIds: NonEmptyList[PeerId]
      ): F[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)] = {
        type Agg = SortedSet[L0Peer]
        type Result = LatestSnapshotTuple
        for {
          peers <- globalL0ClusterStorage.getPeers.map(nes => Random.shuffle(nes.toSortedSet))

          _ <- logger.info(s"Pulling latest snapshot using ${peers.size} peers")
          _ <- logger.trace(s"${peers.map(_.id.show)}")

          majorityPeers <- getL0Peers(majorityPeerIds)
          cachedMajorityOrdinal <- getMajorityOrdinal(majorityPeers)
          result <- peers.tailRecM[F, Result] { l0Peers =>
            l0Peers.headOption.fold {
              NoPeersWithMajorityHash.raiseError[F, Either[Agg, Result]]
            } { l0Peer =>
              pullLatestSnapshotFromPeer(l0Peer).flatMap { latest =>
                Applicative[F].ifF(verifyLatestSnapshot(latest, majorityPeers, cachedMajorityOrdinal))(
                  latest.asRight[Agg],
                  l0Peers.tail.asLeft[Result]
                )
              }.handleErrorWith { err =>
                logger
                  .warn(err)(s"Error pulling latest snapshot from peer ${l0Peer.show}")
                  .as(l0Peers.tail.asLeft[Result])
              }
            }
          }
        } yield result
      }

      def pullGlobalSnapshots(ordinal: SnapshotOrdinal): F[Either[LatestSnapshotTuple, List[Hashed[GlobalIncrementalSnapshot]]]] =
        maybeMajorityPeerIds.fold(pullGlobalSnapshotsFromRandomPeerAtOrdinal(ordinal))(pullGlobalSnapshotsFromMajorityAtOrdinal(_, ordinal))

      def pullFinalizedGlobalSnapshots(
        lastOrdinal: SnapshotOrdinal,
        finalizedOrdinal: SnapshotOrdinal
      ): F[List[Hashed[GlobalIncrementalSnapshot]]] =
        if (finalizedOrdinal <= lastOrdinal)
          // Nothing finalized beyond our head yet — follower must idle until finality
          // advances. Logged at DEBUG (this is the steady-state when the follower is
          // caught up to the finality watermark).
          logger.debug(s"No finalized snapshots beyond local head: finalized=${finalizedOrdinal.show} <= last=${lastOrdinal.show}") >>
            noSnapshots.pure[F]
        else
          // Pull the standard batch starting from lastOrdinal, then clamp to finalizedOrdinal.
          // The post-filter is safe because pullGlobalSnapshots(ordinal) already verifies hash
          // chaining via pullVerifiedSnapshots (majority mode) or peer fetch (random mode);
          // we just drop any snapshots that exceed the finality watermark before returning.
          pullGlobalSnapshots(lastOrdinal).flatMap {
            case Left(_) =>
              // Bootstrap path — caller's lastOrdinal had no stored state. The follower's
              // initial-snapshot path runs separately (download bootstrap), so we don't
              // surface a tuple here; return empty so the caller idles. This is a transient
              // state during startup.
              logger
                .info(s"pullFinalizedGlobalSnapshots: bootstrap tuple returned for last=${lastOrdinal.show}, returning empty")
                .as(noSnapshots)
            case Right(snapshots) =>
              val finalized = snapshots.filter(_.ordinal <= finalizedOrdinal)
              val dropped = snapshots.size - finalized.size
              logger
                .info(
                  s"pullFinalizedGlobalSnapshots: kept ${finalized.size} finalized, dropped $dropped unfinalized" +
                    s" (last=${lastOrdinal.show}, finalized=${finalizedOrdinal.show})"
                )
                .whenA(dropped > 0)
                .as(finalized)
          }

      private def verifyLatestSnapshot(
        snapshotTuple: LatestSnapshotTuple,
        majorityPeers: NonEmptyList[L0Peer],
        cachedMajorityOrdinal: Option[SnapshotOrdinal]
      ): F[Boolean] = {
        val (snapshot, _) = snapshotTuple
        // Skip state proof validation during pullLatestSnapshot - it will be validated
        // later when processing the snapshot via createContext. This avoids expensive
        // full MPT syncs during initial download when validating snapshots from multiple peers.
        // The majority hash/ordinal validations ensure we're getting a valid snapshot from
        // the network, and the full state proof validation happens during processing.
        List(
          majorityOrdinalValidation(snapshot, cachedMajorityOrdinal),
          majorityHashValidation(snapshot, majorityPeers)
        ).forallM(identity)
      }

      private def stateProofValidation(snapshot: Hashed[GlobalIncrementalSnapshot], info: GlobalSnapshotInfo)(
        implicit hasher: Hasher[F]
      ): F[Boolean] =
        // Sync the MPT store to match the snapshot's state (typed scodec — no JSON blob
        // intermediate), then validate. Validator uses the stateful producer which requires
        // the trie to be built at the correct ordinal.
        mptStore.syncFromGlobalSnapshotInfo(info, snapshot.ordinal) >>
          validator
            .validate(snapshot, info)
            .flatTap(v => logger.debug(s"Failed StateProofValidation: $v").whenA(v.isInvalid))
            .map(_.isValid)

      private def majorityOrdinalValidation(
        snapshot: Hashed[GlobalIncrementalSnapshot],
        maybeMajorityOrdinal: Option[SnapshotOrdinal]
      ): F[Boolean] = {
        val isInRange = maybeMajorityOrdinal.exists(o => ordinalRange.contains(o.value - snapshot.ordinal.value))

        logger
          .debug(s"Majority ordinal ${maybeMajorityOrdinal.show} missing or not in $ordinalRange, ${snapshot.ordinal.show}")
          .unlessA(isInRange)
          .as(isInRange)
      }

      private def majorityHashValidation(snapshot: Hashed[GlobalIncrementalSnapshot], majorityPeers: NonEmptyList[L0Peer]): F[Boolean] =
        getMajorityHash(majorityPeers, snapshot.ordinal).flatMap { maybeMajorityHash =>
          val isMajority = maybeMajorityHash.exists(_ === snapshot.hash)

          logger
            .debug(s"Majority/Snapshot hash mismatch: ${maybeMajorityHash.show}, ${snapshot.hash.show}")
            .unlessA(isMajority)
            .as(isMajority)
        }

      private def pullLatestSnapshotFromPeer(l0Peer: L0Peer): F[LatestSnapshotTuple] =
        l0GlobalSnapshotClient.getLatest(l0Peer).flatMap {
          case (snapshot, state) =>
            HasherSelector[F].withCurrent { implicit hasher =>
              snapshot.toHashedWithSignatureCheck
            }
              .flatMap(_.liftTo[F])
              .map((_, state))
        }

      private def pullGlobalSnapshot(
        peerResponse: PeerResponse.PeerResponse[F, Signed[GlobalIncrementalSnapshot]]
      ): F[Option[Hashed[GlobalIncrementalSnapshot]]] =
        globalL0ClusterStorage.getRandomPeer.flatMap { l0Peer =>
          peerResponse(l0Peer)
            .flatMap(snapshot => HasherSelector[F].withCurrent(implicit hasher => snapshot.toHashedWithSignatureCheck).flatMap(_.liftTo[F]))
            .map(_.some)
        }

      private def pullGlobalSnapshotsFromRandomPeer: F[Either[LatestSnapshotTuple, List[Hashed[GlobalIncrementalSnapshot]]]] =
        lastGlobalSnapshotStorage.getOrdinal.flatMap {
          _.fold {
            pullLatestSnapshotFromRandomPeer.map(_.asLeft[List[Hashed[GlobalIncrementalSnapshot]]])
          }(pullGlobalSnapshotsFromRandomPeerAtOrdinal)
        }.handleErrorWith { e =>
          logger
            .warn(e)("Failure pulling global snapshots from random peer")
            .as(noSnapshots.asRight[LatestSnapshotTuple])
        }

      private def pullGlobalSnapshotsFromRandomPeerAtOrdinal(
        startingOrdinal: SnapshotOrdinal
      ): F[Either[LatestSnapshotTuple, List[Hashed[GlobalIncrementalSnapshot]]]] =
        for {
          l0Peer <- globalL0ClusterStorage.getRandomPeer
          latestOrdinal <- l0GlobalSnapshotClient.getLatestOrdinal.run(l0Peer)
          nextOrdinal = startingOrdinal.next
          lastOrdinal = calculateLastOrdinal(nextOrdinal, latestOrdinal)
          pulled <- pullSnapshots(l0Peer, nextOrdinal, lastOrdinal)
        } yield pulled.toList.asRight[LatestSnapshotTuple]

      private def calculateLastOrdinal(nextOrdinal: SnapshotOrdinal, latestOrdinal: SnapshotOrdinal): SnapshotOrdinal =
        SnapshotOrdinal.unsafeApply(
          latestOrdinal.value.value
            .min(
              singlePullLimit
                .map(nextOrdinal.value.value + _.value)
                .getOrElse(latestOrdinal.value.value)
            )
        )

      private def pullSnapshots(
        l0Peer: L0Peer,
        nextOrdinal: SnapshotOrdinal,
        lastOrdinal: SnapshotOrdinal
      )(implicit hasherSelector: HasherSelector[F]): F[Chain[Hashed[GlobalIncrementalSnapshot]]] = {
        val ordinals = LazyList
          .range(nextOrdinal.value.value, lastOrdinal.value.value + 1)
          .map(SnapshotOrdinal.unsafeApply)

        type Success = Hashed[GlobalIncrementalSnapshot]
        type Result = Chain[Success]
        type Agg = (LazyList[SnapshotOrdinal], Result)
        (ordinals, Chain.empty[Success]).tailRecM[F, Result] {
          case (ordinal #:: nextOrdinals, snapshots) =>
            l0GlobalSnapshotClient
              .get(ordinal)(l0Peer)
              .flatMap(snapshot => hasherSelector.withCurrent(implicit hasher => snapshot.toHashedWithSignatureCheck).flatMap(_.liftTo[F]))
              .map(s => (nextOrdinals, snapshots :+ s).asLeft[Result])
              .handleErrorWith { e =>
                logger
                  .warn(e)(s"Failure pulling snapshot with ordinal=${ordinal.show}")
                  .as(snapshots.asRight[Agg])
              }

          case (_, snapshots) => snapshots.asRight[Agg].pure[F]
        }
      }

      private case class MajoritySnapshotData(ordinal: SnapshotOrdinal, hash: Hash)

      private def getMajoritySnapshotData(majorityPeerIds: NonEmptyList[PeerId], nextOrdinal: SnapshotOrdinal): F[MajoritySnapshotData] = {
        val result = for {
          peers <- OptionT.liftF {
            getL0Peers(majorityPeerIds).handleErrorWith { e =>
              logger.error(e)(s"Failed to get L0 peers for majorityPeerIds: ${majorityPeerIds.toList}") >>
                Async[F].raiseError(e)
            }
          }

          majorityOrdinal <- OptionT {
            getMajorityOrdinal(peers).flatTap {
              case Some(_) => Async[F].unit
              case None =>
                logger.warn(s"No majority ordinal found among ${peers.size} peers. This will cause the operation to fail.")
            }
          }

          lastOrdinal = calculateLastOrdinal(nextOrdinal, majorityOrdinal)

          majorityHash <- OptionT {
            getMajorityHash(peers, lastOrdinal).flatTap {
              case Some(_) => Async[F].unit
              case None =>
                logger.warn(
                  s"No majority hash found among ${peers.size} peers for ordinal: $lastOrdinal. This will cause the operation to fail."
                )
            }
          }

        } yield MajoritySnapshotData(lastOrdinal, majorityHash)

        result.getOrElseF {
          logger.error(
            s"Failed to get majority snapshot data. Either majority ordinal or majority hash consensus could not be achieved among peers: ${majorityPeerIds.toList}"
          ) >>
            NoMajoritySnapshotData.raiseError[F, MajoritySnapshotData]
        }
      }

      private val peerQueryRetries = 3

      private def withPeerRetry[A](peer: L0Peer, fa: F[A], remaining: Int = peerQueryRetries): F[A] =
        fa.handleErrorWith { err =>
          if (remaining <= 1)
            logger.warn(err)(s"Peer ${peer.show} unreachable after $peerQueryRetries attempts, removing from storage") >>
              globalL0ClusterStorage.removePeer(peer.id) >>
              Async[F].raiseError(err)
          else
            Async[F].sleep(200.millis) >> withPeerRetry(peer, fa, remaining - 1)
        }

      private def getMajorityHash(peers: NonEmptyList[L0Peer], ordinal: SnapshotOrdinal): F[Option[Hash]] =
        peers.toList
          .parTraverseN(numConcurrentQueries) { p =>
            withPeerRetry(p, l0GlobalSnapshotClient.getHash(ordinal).run(p))
              .handleErrorWith(_ => none[Hash].pure[F])
          }
          .map(_.flatten)
          .flatTap(hashes => logger.debug(s"Majority Hashes ${hashes.map(_.show)}"))
          .map(selectMajorityHash)

      private def pullVerifiedSnapshots(
        nextOrdinal: SnapshotOrdinal,
        msd: MajoritySnapshotData,
        peerSet: NonEmptySet[L0Peer]
      ): F[List[Hashed[GlobalIncrementalSnapshot]]] = {
        type Agg = SortedSet[L0Peer]
        type Result = List[Hashed[GlobalIncrementalSnapshot]]
        Random
          .shuffle(peerSet.toSortedSet)
          .tailRecM[F, Result] { peers =>
            peers.headOption.fold {
              NoPeerAlignedWithMajority.raiseError[F, Either[Agg, Result]]
            } { peer =>
              for {
                sc <- pullSnapshots(peer, nextOrdinal, msd.ordinal)
                verified <- verifySnapshotChain(sc, msd, peer)
              } yield Either.cond(verified, sc.toList, peers.tail)
            }
          }
      }

      private def verifySnapshotChain(
        chain: Chain[Hashed[GlobalIncrementalSnapshot]],
        msd: MajoritySnapshotData,
        peer: L0Peer
      ): F[Boolean] =
        chain.toList match {
          case Nil =>
            logger.warn(s"No snapshots to verify from ${peer.show}").as(false)
          case _ if !chain.lastOption.exists(_.hash === msd.hash) =>
            logger.warn(s"Last snapshot hash from ${peer.show} does not match majority").as(false)
          case ss if !ss.zip(ss.tail).forall { case (a, b) => isNextSnapshot[GlobalIncrementalSnapshot](a, b) } =>
            logger.warn(s"Pulled snapshots from ${peer.show} do not form a chain").as(false)
          case _ =>
            logger.debug(s"Verified snapshot chain from ${peer.show}").as(true)
        }

      private def getL0Peers(peerIds: NonEmptyList[PeerId]): F[NonEmptyList[L0Peer]] =
        OptionT(
          peerIds.traverse(globalL0ClusterStorage.getPeer).map(_.toList.flatten.toNel)
        ).getOrRaise(NoMajorityPeers)

      private def getMajorityOrdinal(peers: NonEmptyList[L0Peer]): F[Option[SnapshotOrdinal]] =
        peers.toList
          .parTraverseN(numConcurrentQueries) { p =>
            withPeerRetry(p, l0GlobalSnapshotClient.getLatestOrdinal.run(p))
              .map(_.some)
              .handleErrorWith(_ => none[SnapshotOrdinal].pure[F])
          }
          .map(_.flatten)
          .map(pickMajority[List, SnapshotOrdinal])

      private def selectMajorityHash(hashes: List[Hash]): Option[Hash] =
        maybeMajorityPeerIds
          .map(ids => ceil(0.5 * (1 + ids.size)).toInt)
          .flatMap(min => pickMajority(hashes).filter(h => hashes.count(_ === h) >= min))
    }
}
