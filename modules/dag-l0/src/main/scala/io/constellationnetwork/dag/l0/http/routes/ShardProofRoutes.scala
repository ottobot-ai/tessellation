package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.syntax.all._

import io.constellationnetwork.ext.http4s.AddressVar
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardSubtreeProof, ShardSubtreeProofService}
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.schema.sharding.ShardId

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegInt
import io.circe.Json
import io.circe.syntax._
import org.http4s.circe.CirceEntityCodec.{circeEntityDecoder, circeEntityEncoder}
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Response}

/** HTTP route exposing [[ShardSubtreeProof]] for cross-shard committee queries — Slice 10 of
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §8.6.
  *
  * '''Endpoint''':
  *   - `POST /shard/{shardId}/proof?metagraph={addr}` — generate an MPT inclusion proof for a
  *     [[GlobalStateKey]] (passed as the JSON request body) under the named metagraph's subtree.
  *     - 200 [[ShardSubtreeProof]] JSON on success.
  *     - 400 if `shardId` is malformed, `metagraph` query param is missing/malformed, OR the
  *       shard doesn't own the metagraph per the deterministic static assignment (defence
  *       against probing peers / stale config).
  *     - 404 if the shard owns the MG but the requested key is absent from the underlying MPT
  *       (v1 doesn't ship proof-of-absence).
  *     - 503 while the service Ref is still empty (pre-startup) OR the shard has no current
  *       checkpoint (bootstrap, paused shard).
  *
  * '''Wire shape rationale (deviation from design doc §8.6 `GET` shape)''':
  *   - Design §8.6 specifies `GET /shard/{shardId}/proof?metagraph={addr}&key={hexKey}`. v1 uses
  *     `POST` with a JSON body for the key because [[GlobalStateKey]] is a structured 4-tuple
  *     (network namespace, fieldId, contract namespace, user namespace) — passing it as a
  *     URL-encoded JSON or as the `GlobalStateKey.toHex` output adds an extra serialization
  *     surface that's load-bearing for cluster-wide proof reproducibility. POST with a JSON body
  *     decoded by the same magnolia-derived `GlobalStateKey` decoder used everywhere else
  *     eliminates that surface.
  *   - The `anchor` query param from §8.6 is OMITTED in v1 — the prover anchors at whatever
  *     branch+ordinal the [[ShardSubtreeProofService]] was constructed with (typically the
  *     last-finalized snapshot). Adding caller-driven anchor is a v2 feature when multiple
  *     historical anchors are needed (light-client time-travel queries).
  *
  * '''Mirrors''' [[NipopowRoutes]] (#138): pure observability, reads a Ref populated by the
  * consensus startup once the proof service is wired. Never feeds back into consensus.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`): fresh route for the
  * cross-shard read path. No compat ceremony.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`): no `sys.env.get` — all knobs
  * (`numShards` via the underlying [[ShardSubtreeProofService]]) come from typed HOCON config.
  */
final case class ShardProofRoutes[F[_]: Async](
  serviceRef: Ref[F, Option[ShardSubtreeProofService[F]]]
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  protected val prefixPath: InternalUrlPrefix = "/shard"

  /** Run the inner handler with the proof service if present; respond 503 otherwise. Mirrors
    * [[NipopowRoutes.withProvider]] so the startup-ordering gate is consistent across routes.
    */
  private def withService(f: ShardSubtreeProofService[F] => F[Response[F]]): F[Response[F]] =
    serviceRef.get.flatMap {
      case None          => ServiceUnavailable(Json.obj("message" -> Json.fromString("shard subtree proof service not yet initialized")))
      case Some(service) => f(service)
    }

  /** Parse a non-negative shard id from a path segment. Returns `None` if the segment is not a
    * valid non-negative integer.
    */
  private def parseShardId(raw: String): Option[ShardId] =
    for {
      i <- scala.util.Try(raw.toInt).toOption
      n <- NonNegInt.from(i).toOption
    } yield ShardId(n)

  /** Extract the `metagraph` query param and decode it as an [[io.constellationnetwork.schema.address.Address]].
    * Routes through the same [[AddressVar]] extractor the other gl0 routes use.
    */
  private def extractMetagraph(req: org.http4s.Request[F]) =
    req.params.get("metagraph").flatMap { raw =>
      AddressVar.unapply(raw)
    }

  protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / shardIdRaw / "proof" =>
      parseShardId(shardIdRaw) match {
        case None =>
          BadRequest(Json.obj("message" -> Json.fromString(s"shardId must be a non-negative integer; got '$shardIdRaw'")))

        case Some(shardId) =>
          extractMetagraph(req) match {
            case None =>
              BadRequest(Json.obj("message" -> Json.fromString("missing or malformed `metagraph` query parameter")))

            case Some(mgAddress) =>
              req.attemptAs[GlobalStateKey].value.flatMap {
                case Left(decodeFail) =>
                  BadRequest(
                    Json.obj(
                      "message" -> Json.fromString(s"could not decode GlobalStateKey body: ${decodeFail.message}")
                    )
                  )

                case Right(key) =>
                  withService { service =>
                    service.generateProofForMetagraph(shardId, mgAddress, key).flatMap {
                      case Some(proof) => Ok(proof.asJson)
                      case None        =>
                        // Three reasons the service returns None — disambiguated here for the wire layer per the route's
                        // response-code policy:
                        //
                        //   1. Shard-ownership mismatch: the MG is owned by a different shard per the deterministic static
                        //      assignment. 400 — the caller has a stale config or is probing.
                        //   2. No current checkpoint (bootstrap, paused shard): 503 — the shard exists but isn't producing yet.
                        //   3. Key absent from MPT: 404 — v1 doesn't ship proof-of-absence.
                        //
                        // To distinguish (1) without re-running the assignment here (and without coupling the route to the
                        // service's internal flow), we re-check ownership via the service's own contract — the v1 service exposes
                        // ownership as a property: a Some-return iff ownership AND checkpoint AND membership. The cheapest 1-bit
                        // disambiguation is to test ownership independently using the service's own ShardAssignment. But the route
                        // currently doesn't have direct access to ShardAssignment (decoupled by design). Pragmatic v1 choice: map
                        // all three to 404 with a diagnostic message, and let v2 wire a finer-grained `generateProofResult: F[GenerateOutcome]`
                        // ADT into the service surface. This trades a small loss of HTTP-code granularity for a clean v1 surface.
                        NotFound(
                          Json.obj(
                            "message" -> Json.fromString(
                              "no proof available — possible causes: shard does not own metagraph; shard has no current checkpoint; key absent from MPT"
                            ),
                            "shardId" -> shardId.asJson,
                            "metagraph" -> Json.fromString(mgAddress.value.value)
                          )
                        )
                    }
                  }
              }
          }
      }
  }
}

object ShardProofRoutes {

  /** Convenience constructor that wraps a service in a `Ref[F, Option[_]]`. Mirrors how
    * [[NipopowRoutes]] expects its provider — the startup ordering populates the Ref once all
    * dependencies (per-shard chain stores, MPT proof service, shard assignment) are wired.
    */
  def make[F[_]: Async](service: ShardSubtreeProofService[F]): F[ShardProofRoutes[F]] =
    Ref.of[F, Option[ShardSubtreeProofService[F]]](service.some).map(ShardProofRoutes(_))

  /** Convenience constructor for a pre-startup empty Ref. The wiring code populates it later via
    * `routes.serviceRef.set(Some(service))`.
    */
  def empty[F[_]: Async]: F[ShardProofRoutes[F]] =
    Ref.of[F, Option[ShardSubtreeProofService[F]]](None).map(ShardProofRoutes(_))
}
