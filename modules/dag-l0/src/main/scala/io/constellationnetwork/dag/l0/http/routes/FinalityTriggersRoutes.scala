package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{FinalityTrigger, FinalityTriggerView}
import io.constellationnetwork.node.shared.ext.http4s.SnapshotOrdinalVar
import io.constellationnetwork.routes.internal._

import eu.timepit.refined.auto._
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl

/** HTTP route for the chain-quality / finality-triggers observable (task #138).
  *
  * `GET /global-snapshots/{ord}/finality-triggers` answers "which finality triggers qualified the given ordinal?" without taking on
  * leader-loop internals. The trigger list is owned by
  * [[io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SnapshotLeaderLoop]], which publishes a [[FinalityTriggerView]]
  * through a `Ref` at startup. This route reads that Ref each request — lock-free, no caching.
  *
  * Returns 503 while the Ref is empty (the leader-loop hasn't initialized yet — startup window).
  *
  * Pure observability — never feeds back into consensus.
  */
final case class FinalityTriggersRoutes[F[_]: Async](
  viewRef: Ref[F, Option[FinalityTriggerView[F]]]
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  protected val prefixPath: InternalUrlPrefix = "/global-snapshots"

  protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / SnapshotOrdinalVar(ordinal) / "finality-triggers" =>
      viewRef.get.flatMap {
        case None =>
          // Leader loop hasn't initialized the view yet. Same 503 semantics as the
          // SnapshotRoutes pre-finality endpoints — caller retries.
          ServiceUnavailable()
        case Some(view) =>
          view.triggersFor(ordinal).flatMap { kinds =>
            val phase12 = Set[FinalityTrigger.Kind](
              FinalityTrigger.Kind.TWeight,
              FinalityTrigger.Kind.TCount,
              FinalityTrigger.Kind.TDepth1
            )
            val payload = FinalityTriggersRoutes.FinalityTriggersPayload(
              ordinal = ordinal.value.value,
              triggers = kinds.toList.map(_.name).sorted,
              satisfied_count = kinds.size,
              phases = FinalityTriggersRoutes.PhasesPayload(
                phase_1_to_2_count = kinds.intersect(phase12).size,
                phase_2_to_3 = kinds.contains(FinalityTrigger.Kind.TDepth2)
              )
            )
            Ok(payload.asJson)
          }
      }
  }
}

object FinalityTriggersRoutes {

  /** Response payload sub-object. Mirrors the design in task #138:
    *   - `phase_1_to_2_count` — count of qualifying Phase 1→2 triggers (`t_weight`, `t_count`, `t_depth1`); range 0..3.
    *   - `phase_2_to_3` — boolean: did `t_depth2` fire for this ordinal?
    */
  final case class PhasesPayload(
    phase_1_to_2_count: Int,
    phase_2_to_3: Boolean
  )

  object PhasesPayload {
    implicit val encoder: Encoder[PhasesPayload] = deriveEncoder
  }

  /** Top-level response payload:
    *   - `ordinal` — the requested ordinal.
    *   - `triggers` — alphabetical list of qualifying trigger names (subset of the four kinds).
    *   - `satisfied_count` — `triggers.size`, total across both phases (0..4).
    *   - `phases` — see [[PhasesPayload]].
    */
  final case class FinalityTriggersPayload(
    ordinal: Long,
    triggers: List[String],
    satisfied_count: Int,
    phases: PhasesPayload
  )

  object FinalityTriggersPayload {
    implicit val encoder: Encoder[FinalityTriggersPayload] = deriveEncoder
  }
}
