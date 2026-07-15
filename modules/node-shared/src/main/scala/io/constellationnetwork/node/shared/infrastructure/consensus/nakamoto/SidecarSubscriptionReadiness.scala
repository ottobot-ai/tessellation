package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import cats.effect.kernel.Async
import cats.effect.std.Semaphore
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{ProductionGate => SnapshotProductionGate}

import fs2.Stream
import fs2.concurrent.SignallingRef

/** Process-local lifecycle state for the two sidecar Subscribe streams.
  *
  * An active acknowledgement proves only that the current sidecar process acquired the requested local subscriptions. It says nothing about
  * peers, mesh reachability, message freshness, chain catch-up, consensus validity, or economic validity.
  *
  * Lifecycle effects are the local linearization boundary. A remote partition may occur before gRPC reports it; once the callback's
  * serialized `release` commits, production is paused before the lane is invalidated. Instantaneous connectivity is not a validity input.
  */
trait SidecarSubscriptionReadiness[F[_]] {
  import SidecarSubscriptionReadiness._

  /** Starts a new local stream attempt and immediately invalidates the prior acknowledgement for this lane. */
  def begin(lane: Lane): F[Attempt]

  /** Installs an acknowledgement only when `attempt` is still the current attempt for its lane. */
  def acknowledge(attempt: Attempt, acknowledgement: Acknowledgement): F[Unit]

  /** Clears the lane only when `attempt` is still current. A stale stream teardown is a no-op. */
  def release(attempt: Attempt): F[Unit]

  def current: F[Status]

  /** Emits the current status first and then every subsequent lifecycle change. */
  def changes: Stream[F, Status]

  /** Waits until both lanes are acknowledged by the same sidecar process. */
  def awaitBoth: F[Ready]

  /** Waits for both lanes, rechecks them under the lifecycle mutex, and runs `effect` while that readiness lease remains current. A
    * concurrent disconnect is serialized after this lease and pauses production before invalidating its lane. If readiness changed before
    * acquisition, this retries.
    */
  def awaitBothAndRun[A](effect: Ready => F[A]): F[A]
}

object SidecarSubscriptionReadiness {

  sealed trait Lane extends Product with Serializable
  object Lane {
    case object RumorBridge extends Lane
    case object NakamotoSync extends Lane
  }

  final case class Attempt private[nakamoto] (lane: Lane, localGeneration: Long)

  final case class Acknowledgement(sidecarSessionId: String, streamGeneration: Long)

  final case class Ready(
    sidecarSessionId: String,
    rumorGeneration: Long,
    nakamotoGeneration: Long
  )

  final case class Status(
    rumor: Option[Acknowledgement],
    nakamoto: Option[Acknowledgement]
  ) {
    def ready: Option[Ready] =
      (rumor, nakamoto).flatMapN { (r, n) =>
        Option.when(r.sidecarSessionId === n.sidecarSessionId && r.streamGeneration =!= n.streamGeneration)(
          Ready(r.sidecarSessionId, r.streamGeneration, n.streamGeneration)
        )
      }
  }

  final case class StaleAttempt(attempt: Attempt)
      extends IllegalStateException(
        s"Sidecar subscription acknowledgement arrived for stale ${attempt.lane} attempt ${attempt.localGeneration}"
      )

  final case class InvalidAcknowledgement(detail: String)
      extends IllegalArgumentException(s"Invalid sidecar subscription acknowledgement: $detail")

  final case class ReplayedGeneration(lane: Lane, previous: Acknowledgement, received: Acknowledgement)
      extends IllegalArgumentException(
        s"Non-increasing sidecar stream generation for $lane in session ${received.sidecarSessionId}: " +
          s"previous=${previous.streamGeneration}, received=${received.streamGeneration}"
      )

  final case class DuplicateCurrentGeneration(acknowledgement: Acknowledgement)
      extends IllegalArgumentException(
        s"Both sidecar subscription lanes reused stream generation ${acknowledgement.streamGeneration} " +
          s"in session ${acknowledgement.sidecarSessionId}"
      )

  private final case class LaneState(
    localGeneration: Long,
    active: Boolean,
    acknowledgement: Option[Acknowledgement],
    lastAcknowledgement: Option[Acknowledgement]
  )

  private final case class State(
    nextLocalGeneration: Long,
    rumor: LaneState,
    nakamoto: LaneState
  ) {
    def lane(lane: Lane): LaneState = lane match {
      case Lane.RumorBridge  => rumor
      case Lane.NakamotoSync => nakamoto
    }

    def updateLane(lane: Lane, value: LaneState): State = lane match {
      case Lane.RumorBridge  => copy(rumor = value)
      case Lane.NakamotoSync => copy(nakamoto = value)
    }

    def status: Status = Status(rumor.acknowledgement, nakamoto.acknowledgement)
  }

  private object State {
    val empty: State = State(
      0L,
      LaneState(0L, active = false, None, None),
      LaneState(0L, active = false, None, None)
    )
  }

  /** Constructs the sole lifecycle owner for `productionGate` and installs the initial fail-closed pause before returning. Callers must not
    * release ingress before this effect completes.
    */
  def make[F[_]: Async](productionGate: SnapshotProductionGate[F]): F[SidecarSubscriptionReadiness[F]] =
    for {
      _ <- productionGate.pause(SnapshotProductionGate.InboundSubscriptionsUnavailable)
      mutex <- Semaphore[F](1L)
      stateRef <- SignallingRef.of[F, State](State.empty)
    } yield
      new SidecarSubscriptionReadiness[F] {
        private def serialized[A](effect: F[A]): F[A] =
          mutex.permit.use(_ => Async[F].uncancelable(_ => effect))

        private def reconcileFence(state: State): F[Unit] =
          state.status.ready.fold(
            productionGate.pause(SnapshotProductionGate.InboundSubscriptionsUnavailable)
          )(_ => productionGate.resume(SnapshotProductionGate.InboundSubscriptionsUnavailable))

        def begin(lane: Lane): F[Attempt] =
          serialized {
            productionGate.pause(SnapshotProductionGate.InboundSubscriptionsUnavailable) >> stateRef.modify { state =>
              val next = state.nextLocalGeneration + 1L
              val attempt = Attempt(lane, next)
              val previous = state.lane(lane)
              val updated = state
                .copy(nextLocalGeneration = next)
                .updateLane(lane, LaneState(next, active = true, None, previous.lastAcknowledgement))
              (updated, attempt)
            }
          }

        def acknowledge(attempt: Attempt, acknowledgement: Acknowledgement): F[Unit] =
          Async[F]
            .raiseError[Unit](InvalidAcknowledgement("sidecar session id was empty"))
            .whenA(acknowledgement.sidecarSessionId.trim.isEmpty) >>
            Async[F]
              .raiseError[Unit](InvalidAcknowledgement("stream generation must be positive"))
              .whenA(acknowledgement.streamGeneration <= 0L) >>
            serialized {
              stateRef.get.flatMap { state =>
                val laneState = state.lane(attempt.lane)
                val otherAcknowledgement = attempt.lane match {
                  case Lane.RumorBridge  => state.nakamoto.acknowledgement
                  case Lane.NakamotoSync => state.rumor.acknowledgement
                }

                if (laneState.localGeneration =!= attempt.localGeneration || !laneState.active)
                  Async[F].raiseError[Unit](StaleAttempt(attempt))
                else
                  laneState.lastAcknowledgement match {
                    case Some(previous)
                        if previous.sidecarSessionId === acknowledgement.sidecarSessionId &&
                          acknowledgement.streamGeneration <= previous.streamGeneration =>
                      Async[F].raiseError[Unit](ReplayedGeneration(attempt.lane, previous, acknowledgement))
                    case _
                        if otherAcknowledgement.exists(other =>
                          other.sidecarSessionId === acknowledgement.sidecarSessionId &&
                            other.streamGeneration === acknowledgement.streamGeneration
                        ) =>
                      Async[F].raiseError[Unit](DuplicateCurrentGeneration(acknowledgement))
                    case _ =>
                      val updated = state.updateLane(
                        attempt.lane,
                        laneState.copy(
                          acknowledgement = acknowledgement.some,
                          lastAcknowledgement = acknowledgement.some
                        )
                      )
                      stateRef.set(updated) >> reconcileFence(updated)
                  }
              }
            }

        def release(attempt: Attempt): F[Unit] =
          serialized {
            stateRef.get.flatMap { state =>
              val laneState = state.lane(attempt.lane)
              if (laneState.localGeneration === attempt.localGeneration && laneState.active) {
                val updated = state.updateLane(attempt.lane, laneState.copy(active = false, acknowledgement = None))
                productionGate.pause(SnapshotProductionGate.InboundSubscriptionsUnavailable) >> stateRef.set(updated)
              } else
                // A stale finalizer never clears, pauses, or overwrites the replacement registration.
                reconcileFence(state)
            }
          }

        def current: F[Status] = stateRef.get.map(_.status)

        def changes: Stream[F, Status] = stateRef.discrete.map(_.status)

        def awaitBoth: F[Ready] = changes.map(_.ready).unNone.head.compile.lastOrError

        def awaitBothAndRun[A](effect: Ready => F[A]): F[A] = {
          def loop: F[A] =
            awaitBoth >> serialized(stateRef.get.flatMap(_.status.ready.traverse(effect))).flatMap(
              _.fold(loop)(_.pure[F])
            )

          loop
        }
      }
}
