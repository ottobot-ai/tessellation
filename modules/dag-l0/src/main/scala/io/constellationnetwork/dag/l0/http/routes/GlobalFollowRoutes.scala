package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.GlobalFollowSliceService
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.follow.GlobalFollowSliceResponse

import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.syntax._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.dsl.impl.QueryParamDecoderMatcher
import org.http4s.{HttpRoutes, Response}

/** HTTP route serving the latest-finalized consumed-field slice for the gl1 own-slice follow path
  * (`docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`, the TRANSPORT half; the prover is [[GlobalFollowSliceService]]).
  *
  * '''Endpoints''':
  *   - `GET /global-follow/slice/latest` — serve the full consumed-field slice at the latest-finalized global ordinal as a
  *     [[GlobalFollowSliceResponse]] (the slice is a [[io.constellationnetwork.schema.nakamoto.follow.ConsumedFieldDelta]] of all-upserts,
  *     Address-keyed, `baseOrdinal = None` — see [[GlobalFollowSliceService.latestSlice]]). A gl1 follower applies it to its (empty or
  *     held) slice and recompute-matches each field root against the matching signed snapshot's `stateProof.<field>Proof`. The bootstrap /
  *     fallback transport.
  *     - 200 [[GlobalFollowSliceResponse]] JSON on success.
  *     - 503 while the service `Ref` is still empty (pre-startup) OR no global ordinal has finalized yet (no GSI to project).
  *   - `GET /global-follow/slice?since=<ordinalLong>` — the #287 "send diffs" transport: serve the INCREMENTAL change-set a follower
  *     holding the consumed-field state at ordinal `since` needs to reach the latest finalized ordinal (see
  *     [[GlobalFollowSliceService.sliceSince]]). `baseOrdinal = Some(since)` when a diff (or no-change) is served, `None` when `since` fell
  *     out of the ring and the full slice is returned instead. Same 503 gates as `/slice/latest`.
  *
  * '''Why the LATEST-finalized anchor''' (locked decision 1, "Transfer model"): both endpoints anchor verification at the latest-finalized
  * `GlobalSnapshotInfo` ordinal (the SAME source the `getCombined` snapshot endpoint serves). The `MptOverlay` cannot serve a
  * value-accurate slice at a non-tip ordinal, so the `since` diff is computed from gl0's retained recent-projection ring, not by reading
  * the overlay at a historical ordinal. The diff is a pure transport optimization (#287); a follower that cannot use it falls back to the
  * full slice.
  *
  * '''Ordinal carried by the service''' (own-slice rework, 2026-05-28): the service reads the latest-finalized `(ordinal, GSI)` itself and
  * returns the ordinal alongside the slice, so the route no longer needs an external finalized-anchor resolver.
  *
  * '''Mirrors''' [[io.constellationnetwork.node.shared.http.routes.nakamoto.NipopowRoutes]] / `ShardProofRoutes` (#138): pure
  * observability, reads a `Ref` populated by the consensus startup once the slice service is wired. Never feeds back into consensus.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`): fresh route for the follow transport. No compat ceremony.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`): no `sys.env.get` — the route takes typed dependencies via its
  * constructor; production wiring threads them from consensus state, not env reads.
  */
final case class GlobalFollowRoutes[F[_]: Async](
  serviceRef: Ref[F, Option[GlobalFollowSliceService[F]]]
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  // `?since=<ordinalLong>` for the #287 diff endpoint. A raw Long (the wire ordinal); refinement to a valid
  // SnapshotOrdinal happens at the handler via `SnapshotOrdinal.unsafeApply`.
  private object SinceOrdinalParam extends QueryParamDecoderMatcher[Long]("since")

  protected val prefixPath: InternalUrlPrefix = "/global-follow"

  /** Run the inner handler with the slice service if present; respond 503 otherwise. Mirrors `ShardProofRoutes.withService` /
    * `NipopowRoutes.withProvider` so the startup-ordering gate is consistent across routes.
    */
  private def withService(f: GlobalFollowSliceService[F] => F[Response[F]]): F[Response[F]] =
    serviceRef.get.flatMap {
      case None          => ServiceUnavailable(Json.obj("message" -> Json.fromString("global follow slice service not yet initialized")))
      case Some(service) => f(service)
    }

  protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "slice" / "latest" =>
      withService { service =>
        service.latestSlice.flatMap {
          case None =>
            // No global ordinal has finalized yet (cold start) — there is no slice to serve. 503 so the
            // follower retries, consistent with the service-not-ready 503 above.
            ServiceUnavailable(Json.obj("message" -> Json.fromString("no finalized global ordinal yet")))
          case Some((ordinal, slice)) =>
            // Bootstrap / fallback transport: full from-empty slice, baseOrdinal = None.
            Ok(GlobalFollowSliceResponse(ordinal, slice, none).asJson)
        }
      }

    case GET -> Root / "slice" :? SinceOrdinalParam(since) =>
      // #287 "send diffs": serve the incremental change-set vs the projection the follower already holds at
      // `since` (or the full slice if `since` fell out of the ring). 503 gate identical to /slice/latest.
      withService { service =>
        service.sliceSince(SnapshotOrdinal.unsafeApply(since)).flatMap {
          case None           => ServiceUnavailable(Json.obj("message" -> Json.fromString("no finalized global ordinal yet")))
          case Some(response) => Ok(response.asJson)
        }
      }
  }
}

object GlobalFollowRoutes {

  /** Convenience constructor wrapping a service in a `Ref[F, Option[_]]`. Mirrors `ShardProofRoutes.make` — the startup ordering populates
    * the `Ref` once the GSI source (and hence the slice service) is wired.
    */
  def make[F[_]: Async](service: GlobalFollowSliceService[F]): F[GlobalFollowRoutes[F]] =
    Ref.of[F, Option[GlobalFollowSliceService[F]]](service.some).map(GlobalFollowRoutes(_))

  /** Convenience constructor for a pre-startup empty `Ref`. The wiring code populates it later via `routes.serviceRef.set(Some(service))`,
    * exactly like `ShardProofRoutes.empty`.
    */
  def empty[F[_]: Async]: F[GlobalFollowRoutes[F]] =
    Ref.of[F, Option[GlobalFollowSliceService[F]]](None).map(GlobalFollowRoutes(_))
}
