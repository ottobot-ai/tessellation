package io.constellationnetwork.node.shared.infrastructure.snapshot.daemon

import cats.effect.Async
import cats.effect.std.{Semaphore, Supervisor}
import cats.syntax.applicativeError._
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.domain.Daemon
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.PeerDiscoveryDelay
import io.constellationnetwork.node.shared.domain.snapshot.programs.Download
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.snapshot.Snapshot
import io.constellationnetwork.security.HasherSelector

import org.typelevel.log4cats.slf4j.Slf4jLogger

trait DownloadDaemon[F[_]] extends Daemon[F] {}

object DownloadDaemon {

  def make[F[_]: Async, S <: Snapshot](
    nodeStorage: NodeStorage[F],
    download: Download[F, S],
    peerDiscoveryDelay: PeerDiscoveryDelay[F],
    hasherSelector: HasherSelector[F]
  )(
    implicit S: Supervisor[F]
  ): DownloadDaemon[F] = new DownloadDaemon[F] {

    private val logger = Slf4jLogger.getLoggerFromClass[F](DownloadDaemon.getClass)

    def start: F[Unit] =
      Semaphore[F](1).flatMap { downloadLock =>
        S.supervise(watchForDownload(downloadLock)).void
      }

    private def watchForDownload(downloadLock: Semaphore[F]): F[Unit] =
      nodeStorage.nodeStates
        .filter(_ === NodeState.WaitingForDownload)
        .evalTap { _ =>
          downloadLock.tryAcquire.flatMap {
            case true =>
              Async[F].guaranteeCase(
                (peerDiscoveryDelay.waitForPeers >> download.download(hasherSelector)).handleErrorWith { err =>
                  logger.error(err)(
                    "Download failed, stream kept alive. " +
                      "Node remains in WaitingForDownload — will retry after 10s backoff."
                  ) >> Async[F].sleep(10.seconds)
                }
              )(_ => downloadLock.release)
            case false =>
              logger.debug("Download already in progress, skipping duplicate trigger")
          }
        }
        .compile
        .drain
  }
}
