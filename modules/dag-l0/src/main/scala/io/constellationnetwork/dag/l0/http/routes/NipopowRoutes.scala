package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.nipopow.{NipopowProofProvider, ProofError, TowerProof}
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema.SnapshotOrdinal

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.Json
import io.circe.syntax._
import org.http4s.circe.CirceEntityCodec.{circeEntityDecoder, circeEntityEncoder}
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Response}

/** §3 NIPoPoW S5 — HTTP routes exposing [[TowerProof]] for light clients.
  *
  * '''Endpoints''':
  *   - `GET /nakamoto/nipopow/proof?fromOrd=N[&k=K]` — build a proof anchored at ordinal `N` with L0-suffix length `k`.
  *     - 200 `NipopowProof` on success.
  *     - 400 if `fromOrd` is malformed, negative, or strictly greater than the local chain head.
  *     - 404 if the proof's `level0Suffix` is empty (no finalized snapshots in range — tower has no entries).
  *     - 503 while the provider Ref is still empty (pre-startup).
  *   - `GET /nakamoto/nipopow/proof/genesis[?k=K]` — convenience for cold-start light clients; equivalent to the main route with `fromOrd =
  *     SnapshotOrdinal.MinValue`.
  *   - `POST /nakamoto/nipopow/verify` — server-side verifier offload. Accepts a Circe-encoded [[TowerProof]] body; returns 200 `{
  *     verified: true }` or 400 `{ verified: false, error: <ProofError JSON> }` on first-failure.
  *
  * '''Defaults''':
  *   - `k` defaults to [[TowerProof.DefaultSuffixLength]] (= 5), the local L0 proof-suffix length. It is unrelated to GL0 k1/k2 finality
  *     semantics. Callers may override it via query string.
  *
  * Mirrors `FinalityTriggersRoutes` (#138): pure observability, reads a Ref populated by the consensus startup once the tower store +
  * snapshot storage + numerics interpreters are wired. Never feeds back into consensus.
  */
final case class NipopowRoutes[F[_]: Async](
  providerRef: Ref[F, Option[NipopowProofProvider[F]]]
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  protected val prefixPath: InternalUrlPrefix = "/nakamoto"

  /** Parse a non-negative `Long` query parameter, returning `None` on absence and `Left` on malformed input. */
  private def optionalLongParam(req: org.http4s.Request[F], key: String): Either[String, Option[Long]] =
    req.params.get(key) match {
      case None => Right(None)
      case Some(raw) =>
        raw.toLongOption match {
          case Some(v) if v >= 0L => Right(Some(v))
          case Some(_)            => Left(s"$key must be non-negative")
          case None               => Left(s"$key is not a valid integer")
        }
    }

  /** Parse `fromOrd` and `k` from query params; defaults `k` to [[TowerProof.DefaultSuffixLength]] when absent. Returns the canonical
    * `(since, k)` tuple or a human-readable error on malformed input.
    */
  private def parseProofParams(req: org.http4s.Request[F]): Either[String, (SnapshotOrdinal, Int)] =
    for {
      fromOrdRaw <- optionalLongParam(req, "fromOrd")
      kRaw <- optionalLongParam(req, "k")
      since <- fromOrdRaw match {
        case None => Right(SnapshotOrdinal.MinValue)
        case Some(v) =>
          NonNegLong.from(v) match {
            case Right(nn) => Right(SnapshotOrdinal(nn))
            case Left(err) => Left(s"fromOrd: $err")
          }
      }
      k <- kRaw match {
        case None => Right(TowerProof.DefaultSuffixLength)
        case Some(v) =>
          if (v < 1L) Left("k must be ≥ 1")
          else if (v > Int.MaxValue.toLong) Left("k overflows Int range")
          else Right(v.toInt)
      }
    } yield (since, k)

  /** Build a proof using the provider when present, returning 503 otherwise. */
  private def withProvider[A](f: NipopowProofProvider[F] => F[Response[F]]): F[Response[F]] =
    providerRef.get.flatMap {
      case None           => ServiceUnavailable(Json.obj("message" -> Json.fromString("Nipopow provider not yet initialized")))
      case Some(provider) => f(provider)
    }

  /** Build a proof anchored at `since` with suffix length `k`. Returns 200 on success, 404 when the proof has no L0 suffix (tower empty —
    * no finalized headers in range).
    */
  private def buildAndRespond(provider: NipopowProofProvider[F], since: SnapshotOrdinal, k: Int): F[Response[F]] =
    provider.build(since, k).flatMap { proof =>
      if (proof.level0Suffix.isEmpty) NotFound(Json.obj("message" -> Json.fromString("no finalized headers in requested range")))
      else Ok(proof.asJson)
    }

  protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ GET -> Root / "nipopow" / "proof" =>
      parseProofParams(req) match {
        case Left(err) => BadRequest(Json.obj("message" -> Json.fromString(err)))
        case Right((since, k)) =>
          withProvider { provider =>
            provider.build(since, k).flatMap { proof =>
              // 400 when `since` is strictly ahead of tip — the proof builder returns an empty proof
              // in that case (tip < since means no headers fall in range), but the user-supplied
              // anchor is unsatisfiable. Distinguishable from 404 (server is past `since` but no
              // tower entries are present yet) by `proof.tipOrdinal`.
              if (since.value.value > proof.tipOrdinal.value.value)
                BadRequest(
                  Json.obj(
                    "message" -> Json.fromString("fromOrd is ahead of chain head"),
                    "fromOrd" -> since.asJson,
                    "head" -> proof.tipOrdinal.asJson
                  )
                )
              else if (proof.level0Suffix.isEmpty)
                NotFound(Json.obj("message" -> Json.fromString("no finalized headers in requested range")))
              else Ok(proof.asJson)
            }
          }
      }

    case req @ GET -> Root / "nipopow" / "proof" / "genesis" =>
      optionalLongParam(req, "k") match {
        case Left(err) => BadRequest(Json.obj("message" -> Json.fromString(err)))
        case Right(kRaw) =>
          val k = kRaw.fold(TowerProof.DefaultSuffixLength) { v =>
            if (v < 1L) TowerProof.DefaultSuffixLength
            else if (v > Int.MaxValue.toLong) Int.MaxValue
            else v.toInt
          }
          withProvider(provider => buildAndRespond(provider, SnapshotOrdinal.MinValue, k))
      }

    case req @ POST -> Root / "nipopow" / "verify" =>
      withProvider { provider =>
        req
          .attemptAs[TowerProof]
          .value
          .flatMap {
            case Left(decodeFail) =>
              BadRequest(
                Json.obj(
                  "verified" -> Json.False,
                  "message" -> Json.fromString(s"could not decode TowerProof body: ${decodeFail.message}")
                )
              )
            case Right(proof) =>
              provider.verify(proof).flatMap {
                case Right(()) =>
                  Ok(Json.obj("verified" -> Json.True))
                case Left(err) =>
                  BadRequest(
                    Json.obj(
                      "verified" -> Json.False,
                      "error" -> (err: ProofError).asJson
                    )
                  )
              }
          }
      }
  }
}
