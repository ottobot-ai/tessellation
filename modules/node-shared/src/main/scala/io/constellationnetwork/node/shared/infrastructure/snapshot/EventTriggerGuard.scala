package io.constellationnetwork.node.shared.infrastructure.snapshot

import cats.effect.{Async, Clock, Ref}
import cats.syntax.all._

import scala.concurrent.duration.FiniteDuration

import io.constellationnetwork.node.shared.infrastructure.mempool.EventMempool

import org.typelevel.log4cats.SelfAwareStructuredLogger

/** Shared guard logic for event-driven consensus triggering.
  *
  * Checks pass when:
  *   1. `triggerEventConsensus` is available (consensus wired) 2. Last round had >= 2 facilitators (not solo genesis) 3. Mempool has >=
  *      `threshold` pending events (batch efficiency) 4. Cooldown elapsed since last trigger (prevent rapid-fire)
  *
  * The solo-genesis guard uses `lastFacilitatorCount` rather than peer count because `getResponsivePeers` excludes self, which would
  * incorrectly block event triggers in 2-node clusters. The facilitator count reflects actual consensus participation: solo genesis has 1,
  * multi-node has 2+.
  *
  * Events always enter the mempool and get gossiped regardless of whether EventTrigger fires. TimeTrigger (43s) picks them up as a
  * fallback.
  */
object EventTriggerGuard {

  def apply[F[_]: Async, E, K](
    eventMempool: EventMempool[F, E, K],
    triggerEventConsensus: Option[F[Unit]],
    getLastFacilitatorCount: F[Int],
    lastTriggerRef: Ref[F, Long],
    logger: SelfAwareStructuredLogger[F],
    threshold: Int,
    cooldown: FiniteDuration,
    allowSoloEventTrigger: Boolean = false
  ): F[Unit] =
    triggerEventConsensus match {
      case None => Async[F].unit
      case Some(trigger) =>
        for {
          lastFacCount <- getLastFacilitatorCount
          _ <-
            // The solo-genesis guard exists so a lone genesis node doesn't race ahead of validators that
            // are about to join. In interim solo-producer mode (candidate admission disabled) nobody ever
            // joins production, so the guard only inflicts the 43s TimeTrigger floor on the cohort — which
            // made the gl0 currency mirror lag past allow-spend validity windows (run bo1rv812m). Callers
            // pass allowSoloEventTrigger = !candidateAdmissionEnabled.
            if (lastFacCount > 0 && lastFacCount < 2 && !allowSoloEventTrigger)
              logger.debug(
                s"EventTrigger skipped: last round had $lastFacCount facilitator(s), waiting for multi-node consensus"
              )
            else
              eventMempool.size.flatMap { mempoolSize =>
                if (mempoolSize < threshold)
                  Async[F].unit
                else
                  Clock[F].monotonic.flatMap { now =>
                    val nowMs = now.toMillis
                    lastTriggerRef.modify { lastMs =>
                      val elapsed = nowMs - lastMs
                      if (elapsed >= cooldown.toMillis)
                        (nowMs, true)
                      else
                        (lastMs, false)
                    }.flatMap {
                      case true =>
                        logger.info(
                          s"EventTrigger fired: lastFacilitators=$lastFacCount, pending=$mempoolSize, " +
                            s"threshold=$threshold, cooldown=${cooldown.toSeconds}s"
                        ) >> trigger
                      case false =>
                        Async[F].unit
                    }
                  }
              }
        } yield ()
    }
}
