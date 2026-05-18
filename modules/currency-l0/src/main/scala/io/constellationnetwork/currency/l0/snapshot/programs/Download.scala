package io.constellationnetwork.currency.l0.snapshot.programs

import cats.Applicative
import cats.effect.std.Random
import cats.effect.{Async, Ref}
import cats.syntax.all.none
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.option._
import cats.syntax.order._
import cats.syntax.semigroup._
import cats.syntax.show._

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import io.constellationnetwork.currency.dataApplication.{BaseDataApplicationL0Service, DataCalculatedState, L0NodeContext}
import io.constellationnetwork.currency.l0.domain.snapshot.storages.CurrencySnapshotCleanupStorage
import io.constellationnetwork.currency.l0.http.p2p.P2PClient
import io.constellationnetwork.currency.l0.snapshot.CurrencySnapshotConsensus
import io.constellationnetwork.currency.schema.CurrencyStateKey
import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage
import io.constellationnetwork.node.shared.domain.collateral.LatestBalances
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.programs.Download
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.domain.snapshot.{PeerSelect, Validator}
import io.constellationnetwork.node.shared.infrastructure.mempool.EventMempool
import io.constellationnetwork.node.shared.infrastructure.snapshot.CurrencySnapshotContextFunctions
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.{CombinedSnapshotCheckpointFileSystemStorage, IdentifierStorage}
import io.constellationnetwork.node.shared.snapshot.currency.CurrencySnapshotEvent
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.peer.Peer
import io.constellationnetwork.schema.snapshot.SnapshotMetadata
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher, HasherSelector}

import eu.timepit.refined.cats._
import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.RetryPolicies._
import retry._

object Download {
  def make[F[_]: Async: Random](
    p2pClient: P2PClient[F],
    clusterStorage: ClusterStorage[F],
    currencySnapshotContextFns: CurrencySnapshotContextFunctions[F],
    nodeStorage: NodeStorage[F],
    consensus: CurrencySnapshotConsensus[F],
    peerSelect: PeerSelect[F],
    identifierStorage: IdentifierStorage[F],
    maybeDataApplication: Option[BaseDataApplicationL0Service[F]],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    snapshotStorage: SnapshotStorage[F, CurrencyIncrementalSnapshot, CurrencySnapshotInfo] with LatestBalances[F],
    currencySnapshotCleanupStorage: CurrencySnapshotCleanupStorage[F],
    combinedSnapshotCheckpointFileSystemStorage: CombinedSnapshotCheckpointFileSystemStorage[
      F,
      CurrencyIncrementalSnapshot,
      CurrencySnapshotInfo
    ],
    eventMempool: EventMempool[F, CurrencySnapshotEvent, CurrencyStateKey]
  )(implicit l0NodeContext: L0NodeContext[F]): Download[F, CurrencyIncrementalSnapshot] = new Download[F, CurrencyIncrementalSnapshot] {

    val logger = Slf4jLogger.getLogger[F]

    val observationOffset = NonNegLong(4L)
    val fetchSnapshotDelayBetweenTrials = 10.seconds

    type DownloadResult = (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)
    type ObservationLimit = SnapshotOrdinal

    // Currency L0 recovery uses the same fetch-and-observe path as the initial download,
    // but switches the head-installation step to setHeadForRecovery instead of prepend.
    //
    // Rationale: a prior failed-and-recovered InitializeFromDownload may have already
    // prepended a partial-observation snapshot (e.g. ordinal=2 from a chain that needed
    // this node for quorum) into snapshotStorage. The fresh recovery cycle will observe
    // a later tip (e.g. ordinal=14). prepend's `isNextSnapshot` check would reject the
    // non-sequential jump 2 → 14 and raise; the daemon's retry would then abort because
    // the FSM is no longer in WaitingForDownload. setHeadForRecovery bypasses the
    // sequential-link requirement (mirrors dag-l0's lastNGlobalSnapshotStorage.setForRecovery
    // in its recoveryDownload path) so the second download cycle can complete and the
    // node can finally reach Ready. The setHead approach is safe here because the snapshot
    // was content-validated during observe() (createContext succeeded for every fetched
    // ordinal).
    def recoveryDownload(implicit hasherSelector: HasherSelector[F]): F[Unit] =
      downloadInternal(useRecoveryHead = true)

    def download(implicit hasherSelector: HasherSelector[F]): F[Unit] =
      downloadInternal(useRecoveryHead = false)

    private def downloadInternal(useRecoveryHead: Boolean)(implicit hasherSelector: HasherSelector[F]): F[Unit] = {
      implicit val hasher = hasherSelector.getCurrent

      nodeStorage
        .tryModifyState(NodeState.WaitingForDownload, NodeState.DownloadInProgress, NodeState.WaitingForObserving)(start)
        .flatMap(observe)
        .flatMap { result =>
          val ((snapshot, context), observationLimit) = result

          val installHead: F[Unit] =
            if (useRecoveryHead)
              logger.info(s"[Download] Recovery head install (setHeadForRecovery) at ordinal ${snapshot.ordinal.show}") >>
                snapshotStorage.setHeadForRecovery(snapshot, context)
            else
              snapshotStorage.prepend(snapshot, context).flatMap { prepended =>
                if (!prepended)
                  (new Exception(s"Failed to prepend currency snapshot ordinal=${snapshot.ordinal} to storage")).raiseError[F, Unit]
                else
                  Applicative[F].unit
              }

          logger.info(s"[Download] Cleanup for snapshots greater than ${snapshot.ordinal}") >>
            currencySnapshotCleanupStorage.cleanupAbove(snapshot.ordinal) >>
            combinedSnapshotCheckpointFileSystemStorage.deleteAbove(snapshot.ordinal) >>
            eventMempool.clear >>
            logger.info("[Download] Cleared event mempool for recovery") >>
            installHead >>
            fetchAndSetCalculatedState(snapshot) >>
            identifierStorage.get.flatMap { currencyAddress =>
              consensus.manager
                .startFacilitatingAfterDownload(observationLimit, snapshot, CurrencySnapshotContext(currencyAddress, context))
            }
        }
    }

    private def fetchAndSetCalculatedState(snapshot: Signed[CurrencyIncrementalSnapshot])(implicit hasher: Hasher[F]): F[Unit] =
      maybeDataApplication.map { da =>
        implicit val d = da.calculatedStateDecoder

        val retryPolicy = RetryPolicies.limitRetries[F](3).join(RetryPolicies.exponentialBackoff(2.seconds))

        retryingOnAllErrors[(SnapshotOrdinal, DataCalculatedState)](
          policy = retryPolicy,
          onError = (err: Throwable, retryDetails: RetryDetails) =>
            logger.warn(err)(s"Error fetching calculated state (attempt=${retryDetails.retriesSoFar}), selecting new peer")
        ) {
          clusterStorage.getResponsivePeers
            .map(NodeState.ready)
            .map(_.toList)
            .flatMap(Random[F].shuffleList)
            .flatMap {
              case Nil =>
                (new Exception(s"No peers to fetch off-chain state from")).raiseError[F, (SnapshotOrdinal, DataCalculatedState)]
              case peer :: _ => p2pClient.dataApplication.getCalculatedState.run(peer)
            }
            .flatTap {
              case (_, calculatedState) =>
                da.hashCalculatedState(calculatedState).flatMap { calculatedStateHash =>
                  (new Exception(s"Downloaded calculated state does not match the proof stored in snapshot")
                    .raiseError[F, Unit])
                    .unlessA(snapshot.dataApplication.map(_.calculatedStateProof) === calculatedStateHash.some)
                }
            }
        }.flatMap { case (ordinal, calculatedState) => da.setCalculatedState(ordinal, calculatedState) }.void
      }.getOrElse(Applicative[F].unit)

    def start: F[DownloadResult] = {
      // Retry with 10s constant delay, up to 18 attempts (3 min). The genesis node's
      // combined checkpoint file may not be available for ~60-90s after cluster start
      // (genesis initialization + first consensus round + disk write).
      val retryPolicy = constantDelay[F](fetchSnapshotDelayBetweenTrials).join(RetryPolicies.limitRetries(18))
      retryingOnAllErrors[(Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)](
        policy = retryPolicy,
        onError = (err: Throwable, retryDetails: RetryDetails) =>
          logger.error(err)(s"Error when trying to fetch latest metadata (attempt=${retryDetails.retriesSoFar}), selecting new peer")
      ) {
        peerSelect.select.flatMap {
          p2pClient.currencySnapshot.getLatest.run(_)
        }
      }
    }

    def observe(result: DownloadResult)(implicit hasher: Hasher[F]): F[(DownloadResult, ObservationLimit)] = {
      val (lastSnapshot, _) = result

      val observationLimit = SnapshotOrdinal(lastSnapshot.ordinal.value |+| observationOffset)

      // Ref holds the effective observation limit — starts at the target
      // observationLimit, but gets rewritten to the actual last-observed
      // ordinal if observation stalls.
      Ref.of[F, ObservationLimit](observationLimit).flatMap { effectiveLimitRef =>
        def go(result: DownloadResult): F[DownloadResult] = {
          val (lastSnapshot, _) = result

          if (lastSnapshot.ordinal === observationLimit) {
            result.pure[F]
          } else
            fetchNextSnapshot(result)
              .flatMap(go)
              .handleErrorWith {
                case CannotFetchSnapshot | InvalidChain =>
                  // Chain has stalled — the next ordinal never appeared after exhausting
                  // retries (300s). This happens in small clusters (2-3 nodes) where this
                  // downloading node is needed for BFT quorum: the genesis node produced
                  // snapshots solo until this node joined the cluster, but now consensus
                  // requires both nodes and can't advance until this download completes.
                  // Accept the current state and proceed — the node will catch up via
                  // normal consensus once it reaches Ready. Register the actual
                  // last-observed ordinal so consensus starts from where we are, not
                  // from the original target (which would cause InitializeFromDownload
                  // to mismatch artifact/context).
                  logger.warn(
                    s"[Download] Observation stalled at ordinal ${lastSnapshot.ordinal.show} " +
                      s"(target was ${observationLimit.show}). Chain likely needs this node for quorum. " +
                      s"Proceeding with partial observation."
                  ) >> effectiveLimitRef.set(lastSnapshot.ordinal) >> result.pure[F]
                case other => other.raiseError[F, DownloadResult]
              }
        }

        // Observe first, then register consensus at the EFFECTIVE limit.
        // If observation stalls, registerForConsensus is called with the
        // actual last-observed ordinal — avoiding InitializeFromDownload
        // artifact/context mismatch when consensus tries to init at a
        // target the downloading node never reached.
        go(result).flatMap { r =>
          effectiveLimitRef.get.flatMap { effectiveLimit =>
            consensus.manager.registerForConsensus(effectiveLimit).as((r, effectiveLimit))
          }
        }
      }
    }

    def fetchNextSnapshot(result: DownloadResult)(implicit hasher: Hasher[F]): F[DownloadResult] = {
      def retryPolicy = constantDelay(fetchSnapshotDelayBetweenTrials).join(limitRetries(30))

      def isWorthRetrying(err: Throwable): F[Boolean] = err match {
        case CannotFetchSnapshot | InvalidChain => true.pure[F]
        case _                                  => false.pure[F]
      }

      retryingOnSomeErrors(retryPolicy, isWorthRetrying, retry.noop[F, Throwable]) {
        val (lastSnapshot, lastContext) = result

        fetchSnapshot(none, lastSnapshot.ordinal.next).flatMap { snapshot =>
          lastSnapshot.toHashed[F].flatMap { hashed =>
            Applicative[F].unlessA {
              Validator.isNextSnapshot(hashed, snapshot.value)
            }(InvalidChain.raiseError[F, Unit])
          } >>
            identifierStorage.get
              .flatMap(currencyAddress =>
                currencySnapshotContextFns
                  .createContext(
                    CurrencySnapshotContext(currencyAddress, lastContext),
                    lastSnapshot,
                    snapshot,
                    getGlobalSnapshotByOrdinal
                  )
                  .handleErrorWith(_ => InvalidChain.raiseError[F, CurrencySnapshotContext])
              )
              .map(c => (snapshot, c.snapshotInfo))
        }
      }
    }

    def fetchSnapshot(hash: Option[Hash], ordinal: SnapshotOrdinal)(implicit hasher: Hasher[F]): F[Signed[CurrencyIncrementalSnapshot]] =
      clusterStorage.getResponsivePeers
        .map(NodeState.ready)
        .map(_.toList)
        .flatMap(Random[F].shuffleList)
        .flatTap { _ =>
          logger.info(s"Download currency snapshot hash=${hash.show}, ordinal=${ordinal.show}")
        }
        .flatMap { peers =>
          type Success = Signed[CurrencyIncrementalSnapshot]
          type Result = Option[Success]
          type Agg = (List[Peer], Result)

          (peers, none[Success]).tailRecM[F, Result] {
            case (Nil, snapshot) => snapshot.asRight[Agg].pure[F]
            case (peer :: tail, _) =>
              p2pClient.currencySnapshot
                .get(ordinal)
                .run(peer)
                .flatMap(_.toHashed[F])
                .map(_.some)
                .handleError(_ => none[Hashed[CurrencyIncrementalSnapshot]])
                .map {
                  case Some(snapshot) if hash.forall(_ === snapshot.hash) => snapshot.signed.some.asRight[Agg]
                  case _                                                  => (tail, none[Success]).asLeft[Result]
                }
          }
        }
        .flatMap {
          case Some(snapshot) => snapshot.pure[F]
          case _              => CannotFetchSnapshot.raiseError[F, Signed[CurrencyIncrementalSnapshot]]
        }

  }

  case object CannotFetchSnapshot extends NoStackTrace

  case object InvalidChain extends NoStackTrace
}
