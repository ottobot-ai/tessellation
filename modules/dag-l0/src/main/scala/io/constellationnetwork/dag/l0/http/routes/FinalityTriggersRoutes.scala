package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{FinalityTrigger, FinalityTriggerView}
import io.constellationnetwork.node.shared.ext.http4s.SnapshotOrdinalVar
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema.SnapshotOrdinal

import eu.timepit.refined.auto._
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl

/** HTTP route for the chain-quality / finality-triggers observable (task #138).
  *
  * `GET /global-snapshots/{ord}/finality-triggers` reports current trigger calculators without taking on leader-loop internals. The trigger
  * list is owned by [[io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SnapshotLeaderLoop]], which publishes a
  * [[FinalityTriggerView]] through a `Ref` at startup. This route reads that Ref each request — lock-free, no caching.
  *
  * Returns 503 while the Ref is empty (the leader-loop hasn't initialized yet — startup window).
  *
  * Pure observability — never feeds back into consensus.
  */
final case class FinalityTriggersRoutes[F[_]: Async](
  viewRef: Ref[F, Option[FinalityTriggerView[F]]],
  // Legacy-named local k2 retention telemetry for `/settled`. It is not a phase, validity
  // threshold, or fork-choice floor; target P2 remains exact-hash and density-reorgable.
  settledOrdinal: F[SnapshotOrdinal],
  k2Depth: Long
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  protected val prefixPath: InternalUrlPrefix = "/global-snapshots"

  protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    // Legacy local-retention watermark. Read-only and outside GlobalSnapshotInfo/stateProof.
    case GET -> Root / "settled" =>
      settledOrdinal.flatMap { ord =>
        Ok(
          FinalityTriggersRoutes
            .SettledPayload(settled_ordinal = ord.value.value, k2_depth = k2Depth)
            .asJson
        )
      }

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

  /** Transitional response payload for `GET /global-snapshots/settled`:
    *   - `settled_ordinal` — legacy JSON name for the current local k2 watermark; it does not identify consensus-settled state
    *   - `k2_depth` — recommended local retention/proof/recovery capacity k2 = 100*k1
    *
    * G3: RESPONSE-JSON ONLY — this is NOT part of `GlobalSnapshotInfo` / the stateProof consensus root (the field-32 syncView regression
    * class); it is pure observability derived from a node-local marker.
    */
  final case class SettledPayload(
    settled_ordinal: Long,
    k2_depth: Long
  )

  object SettledPayload {
    implicit val encoder: Encoder[SettledPayload] = deriveEncoder
  }

  /** Transitional response payload sub-object:
    *   - `phase_1_to_2_count` includes legacy `t_count`; target P2 uses only `t_weight` or `t_depth1`
    *   - `phase_2_to_3` is a stale JSON name for whether the local `t_depth2` calculator crossed the ordinal; no Phase 3 exists
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
