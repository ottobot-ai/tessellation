package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{GlobalChangeSetService, GlobalFollowSliceService}
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.follow.{GlobalChangeSetResponse, GlobalFollowSliceResponse}
import io.constellationnetwork.serde.codecs.instances.GlobalChangeSetResponseCodec.{codec => globalChangeSetResponseCodec}

import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.syntax._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.dsl.impl.QueryParamDecoderMatcher
import org.http4s.{EntityEncoder, HttpRoutes, Response}

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
  *   - `GET /global-follow/changeset?since=<ordinalLong>` — the currency-l0 (ml0) ADOPT transport (task #12): serve the contiguous
  *     per-ordinal typed [[io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator]] deltas a FULL-state follower
  *     holding the GSI at `since` needs to reach the latest finalized ordinal (see [[GlobalChangeSetService.changeSetSince]]). Unlike the
  *     gl1 5-field slice this serves the EXACT typed delta gl0 applied (incl. the system expiry-index changes a GSI-diff misses), so ml0
  *     adopts each delta sequentially and recompute-matches the resulting MPT root against THAT ordinal's signed `mptRoot` — no
  *     re-execution. The body is a FULLY-SCODEC octet-stream ([[GlobalChangeSetResponse]] encoded via
  *     [[io.constellationnetwork.serde.codecs.instances.GlobalChangeSetResponseCodec]], which REUSES the canonical
  *     [[io.constellationnetwork.serde.codecs.instances.StateChangesAccumulatorCodec]] for the accumulator binary — never Circe). The 503
  *     gate is identical to `/slice/latest`: 503 while the changeset service `Ref` is empty (pre-startup) OR no ordinal has finalized yet
  *     (the served ring is empty).
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
  serviceRef: Ref[F, Option[GlobalFollowSliceService[F]]],
  changeSetServiceRef: Ref[F, Option[GlobalChangeSetService[F]]]
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  // `?since=<ordinalLong>` for the #287 diff endpoint. A raw Long (the wire ordinal); refinement to a valid
  // SnapshotOrdinal happens at the handler via `SnapshotOrdinal.unsafeApply`.
  private object SinceOrdinalParam extends QueryParamDecoderMatcher[Long]("since")

  protected val prefixPath: InternalUrlPrefix = "/global-follow"

  // Octet-stream body for the ml0 changeset transport: the scodec-encoded `GlobalChangeSetResponse` bytes,
  // served via http4s' built-in `Array[Byte]` entity encoder (application/octet-stream) — NO Circe touches the
  // accumulator binary. The client (`GlobalFollowClient.getChangeSetSince`) decodes the same bytes with the same
  // scodec codec. `contramap` over the built-in `Array[Byte]` encoder so the wire is pure scodec end-to-end.
  private implicit val changeSetResponseEntityEncoder: EntityEncoder[F, GlobalChangeSetResponse] =
    EntityEncoder.byteArrayEncoder[F].contramap[GlobalChangeSetResponse] { response =>
      globalChangeSetResponseCodec.encode(response).require.toByteArray
    }

  /** Run the inner handler with the slice service if present; respond 503 otherwise. Mirrors `ShardProofRoutes.withService` /
    * `NipopowRoutes.withProvider` so the startup-ordering gate is consistent across routes.
    */
  private def withService(f: GlobalFollowSliceService[F] => F[Response[F]]): F[Response[F]] =
    serviceRef.get.flatMap {
      case None          => ServiceUnavailable(Json.obj("message" -> Json.fromString("global follow slice service not yet initialized")))
      case Some(service) => f(service)
    }

  /** Run the inner handler with the changeset service if present; respond 503 otherwise — same startup-ordering gate as [[withService]],
    * mirroring it exactly so the changeset endpoint's pre-wiring behaviour is consistent with the slice endpoints.
    */
  private def withChangeSetService(f: GlobalChangeSetService[F] => F[Response[F]]): F[Response[F]] =
    changeSetServiceRef.get.flatMap {
      case None          => ServiceUnavailable(Json.obj("message" -> Json.fromString("global changeset service not yet initialized")))
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

    case GET -> Root / "changeset" :? SinceOrdinalParam(since) =>
      // Task #12 ml0 adopt: serve the contiguous per-ordinal typed deltas the full-state follower needs to reach
      // the latest finalized ordinal from the state it holds at `since` (or `baseOrdinal = None` for a full-GSI
      // fallback when `since` fell out of the served ring). 503 gate identical to /slice. Body is the FULLY-SCODEC
      // octet-stream `GlobalChangeSetResponse` (accumulator binary is scodec, never Circe).
      withChangeSetService { service =>
        service.changeSetSince(SnapshotOrdinal.unsafeApply(since)).flatMap {
          case None           => ServiceUnavailable(Json.obj("message" -> Json.fromString("no finalized global ordinal yet")))
          case Some(response) => Ok(response)
        }
      }
  }
}

object GlobalFollowRoutes {

  /** Convenience constructor wrapping both services in `Ref[F, Option[_]]`s. Mirrors `ShardProofRoutes.make` — the startup ordering
    * populates the `Ref`s once the GSI source (and hence the slice + changeset services) is wired.
    */
  def make[F[_]: Async](
    service: GlobalFollowSliceService[F],
    changeSetService: GlobalChangeSetService[F]
  ): F[GlobalFollowRoutes[F]] =
    (
      Ref.of[F, Option[GlobalFollowSliceService[F]]](service.some),
      Ref.of[F, Option[GlobalChangeSetService[F]]](changeSetService.some)
    ).mapN(GlobalFollowRoutes(_, _))

  /** Convenience constructor for pre-startup empty `Ref`s. The wiring code populates them later via `routes.serviceRef.set(Some(service))`
    * / `routes.changeSetServiceRef.set(Some(changeSetService))`, exactly like `ShardProofRoutes.empty`.
    */
  def empty[F[_]: Async]: F[GlobalFollowRoutes[F]] =
    (
      Ref.of[F, Option[GlobalFollowSliceService[F]]](None),
      Ref.of[F, Option[GlobalChangeSetService[F]]](None)
    ).mapN(GlobalFollowRoutes(_, _))
}
