package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Controls whether the SnapshotLeaderLoop should attempt VRF election + production.
  *
  * Multiple subsystems can pause/resume production. The gate is open (production allowed) only when ALL registered pause reasons are
  * cleared. This prevents:
  *   - Producing during reorg (stale tip)
  *   - Producing during initial sync (validator catching up)
  *   - Producing during MPT self-healing resync
  *   - Producing when a better gossip snapshot would beat our in-progress work
  */
trait ProductionGate[F[_]] {

  /** Pause production with a reason tag. Idempotent — pausing with same reason twice is a no-op. */
  def pause(reason: String): F[Unit]

  /** Resume production by clearing a reason. Open iff no reasons remain. */
  def resume(reason: String): F[Unit]

  /** True when production is allowed (no active pause reasons). */
  def isOpen: F[Boolean]

  /** Current set of active pause reasons (for diagnostics). */
  def pauseReasons: F[Set[String]]
}

object ProductionGate {

  /** Well-known pause reasons. Use these constants instead of raw strings. */
  val ReorgInProgress: String = "reorg-in-progress"
  val InitialSync: String = "initial-sync"
  val MptResync: String = "mpt-resync"
  val BetterGossipReceived: String = "better-gossip-received"
  val ChainBackfill: String = "chain-backfill"
  val InboundSubscriptionsUnavailable: String = "inbound-subscriptions-unavailable"

  def make[F[_]: Async]: F[ProductionGate[F]] =
    Ref.of[F, Set[String]](Set.empty).map { reasonsRef =>
      new ProductionGate[F] {
        private val logger = Slf4jLogger.getLoggerFromName[F]("ProductionGate")

        def pause(reason: String): F[Unit] =
          reasonsRef.modify { reasons =>
            val updated = reasons + reason
            (updated, reasons.contains(reason))
          }.flatMap { wasAlreadyPaused =>
            Async[F].unlessA(wasAlreadyPaused)(
              logger.info(s"⏸️ Production PAUSED: $reason")
            )
          }

        def resume(reason: String): F[Unit] =
          reasonsRef.modify { reasons =>
            val updated = reasons - reason
            (updated, (reasons.contains(reason), updated.isEmpty))
          }.flatMap {
            case (wasActive, nowOpen) =>
              Async[F].whenA(wasActive)(
                if (nowOpen)
                  logger.info(s"▶️ Production RESUMED (cleared: $reason, gate OPEN)")
                else
                  reasonsRef.get.flatMap(remaining =>
                    logger.info(s"⏸️ Production still paused (cleared: $reason, remaining: ${remaining.mkString(", ")})")
                  )
              )
          }

        def isOpen: F[Boolean] = reasonsRef.get.map(_.isEmpty)

        def pauseReasons: F[Set[String]] = reasonsRef.get
      }
    }
}
