package io.constellationnetwork.dag.l0.http.routes

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.{GlobalChangeSetService, GlobalFollowSliceService}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.nakamoto.follow.GlobalChangeSetResponse
import io.constellationnetwork.serde.codecs.instances.GlobalChangeSetResponseCodec.{codec => globalChangeSetResponseCodec}

import eu.timepit.refined.types.numeric.NonNegLong
import org.http4s.Method._
import org.http4s._
import org.http4s.client.dsl.io._
import org.http4s.syntax.literals._
import scodec.bits.BitVector
import suite.HttpSuite

/** Route + service round-trip tests (test (b) of task #12 slice 3) for the ml0 changeset endpoint
  * (`GET /global-follow/changeset?since=<ord>`) served by [[GlobalFollowRoutes]].
  *
  * Builds a REAL [[GlobalChangeSetService]] over a populated ordinal-keyed accumulator ring (the same `SortedMap` shape
  * `recentFinalizedAccumulatorsRef` carries in production), wires it into the route, issues the HTTP GET, then decodes the FULLY-SCODEC
  * octet-stream body with the canonical [[GlobalChangeSetResponse]] codec and asserts the served `baseOrdinal` + `deltas` match what
  * `changeSetSince` computes from the ring. Also covers the two 503 gates (empty service Ref pre-startup, empty ring cold-start) so the
  * changeset endpoint's gate matches the slice endpoint's.
  */
object GlobalChangeSetRoutesSuite extends HttpSuite {

  override type Res = Unit
  override def sharedResource: cats.effect.Resource[IO, Unit] = cats.effect.Resource.unit[IO]

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  private val addr: Address = Address.fromBytes("global-changeset-routes-suite-seed".getBytes("UTF-8"))

  /** A small populated accumulator so the per-ordinal delta carries real (scodec) bytes over the wire. */
  private def acc(seed: Long): StateChangesAccumulator =
    StateChangesAccumulator(balances = SortedMap(addr -> Balance(NonNegLong.unsafeFrom(1000L + seed))))

  /** A populated served ring covering ordinals 8..10 — the shape `recentFinalizedAccumulatorsRef` holds after the leader loop has promoted
    * three finalized accumulators.
    */
  private val populatedRing: SortedMap[SnapshotOrdinal, StateChangesAccumulator] =
    SortedMap(ord(8L) -> acc(8L), ord(9L) -> acc(9L), ord(10L) -> acc(10L))

  /** Build the routes wired to a changeset service over `ring` (None ⇒ empty service Ref, exercising the pre-startup 503). The slice service
    * Ref is left empty — this suite only drives the changeset endpoint.
    */
  private def mkRoutes(ring: Option[SortedMap[SnapshotOrdinal, StateChangesAccumulator]]): IO[HttpRoutes[IO]] =
    for {
      sliceRef <- Ref.of[IO, Option[GlobalFollowSliceService[IO]]](None)
      changeSetRef <- Ref.of[IO, Option[GlobalChangeSetService[IO]]](
        ring.map(r => GlobalChangeSetService.make[IO](IO.pure(r)))
      )
    } yield GlobalFollowRoutes[IO](sliceRef, changeSetRef).publicRoutes

  /** Run the request, assert 200, decode the octet-stream body with the scodec codec. */
  private def fetchDecoded(routes: HttpRoutes[IO], req: Request[IO]): IO[Either[String, GlobalChangeSetResponse]] =
    routes.run(req).value.flatMap {
      case None => IO.pure("route not found".asLeft)
      case Some(resp) if resp.status =!= Status.Ok =>
        IO.pure(s"unexpected status ${resp.status}".asLeft)
      case Some(resp) =>
        resp.as[Array[Byte]].map { bytes =>
          globalChangeSetResponseCodec
            .decodeValue(BitVector(bytes))
            .toEither
            .leftMap(_.messageWithContext)
        }
    }

  test("GET /global-follow/changeset?since=N returns 503 when service is not yet initialized") {
    val req = GET(uri"/global-follow/changeset".withQueryParam("since", 8L))
    for {
      routes <- mkRoutes(None)
      r <- expectHttpStatus(routes, req)(Status.ServiceUnavailable)
    } yield r
  }

  test("GET /global-follow/changeset?since=N returns 503 when the served ring is empty (cold start)") {
    val req = GET(uri"/global-follow/changeset".withQueryParam("since", 8L))
    for {
      routes <- mkRoutes(SortedMap.empty[SnapshotOrdinal, StateChangesAccumulator].some)
      r <- expectHttpStatus(routes, req)(Status.ServiceUnavailable)
    } yield r
  }

  test("GET /global-follow/changeset?since=8 serves the contiguous deltas (9,10) with baseOrdinal Some(8)") {
    val req = GET(uri"/global-follow/changeset".withQueryParam("since", 8L))
    for {
      routes <- mkRoutes(populatedRing.some)
      decoded <- fetchDecoded(routes, req)
    } yield
      decoded match {
        case Left(err) => failure(s"decode failed: $err")
        case Right(response) =>
          expect.same(response.latestOrdinal, ord(10L)) &&
          expect.same(response.baseOrdinal, ord(8L).some) &&
          expect.same(response.deltas, List(ord(9L) -> acc(9L), ord(10L) -> acc(10L)))
      }
  }

  test("GET /global-follow/changeset?since=10 serves a no-op (empty deltas, baseOrdinal Some(10)) at the tip") {
    val req = GET(uri"/global-follow/changeset".withQueryParam("since", 10L))
    for {
      routes <- mkRoutes(populatedRing.some)
      decoded <- fetchDecoded(routes, req)
    } yield
      decoded match {
        case Left(err) => failure(s"decode failed: $err")
        case Right(response) =>
          expect.same(response.latestOrdinal, ord(10L)) &&
          expect.same(response.baseOrdinal, ord(10L).some) &&
          expect.same(response.deltas, Nil)
      }
  }

  test("GET /global-follow/changeset?since=2 (older than ring) serves baseOrdinal None for a full-GSI fallback") {
    // `since + 1` (= 3) was evicted from the ring (lowest retained ordinal is 8) ⇒ a gap ⇒ baseOrdinal None.
    val req = GET(uri"/global-follow/changeset".withQueryParam("since", 2L))
    for {
      routes <- mkRoutes(populatedRing.some)
      decoded <- fetchDecoded(routes, req)
    } yield
      decoded match {
        case Left(err) => failure(s"decode failed: $err")
        case Right(response) =>
          expect.same(response.latestOrdinal, ord(10L)) &&
          expect.same(response.baseOrdinal, none[SnapshotOrdinal]) &&
          expect.same(response.deltas, Nil)
      }
  }
}
