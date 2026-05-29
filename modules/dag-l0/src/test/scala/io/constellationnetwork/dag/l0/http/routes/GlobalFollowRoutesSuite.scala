package io.constellationnetwork.dag.l0.http.routes

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.GlobalFollowSliceService
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.nakamoto.follow.{ConsumedFieldDelta, GlobalFollowSliceResponse}
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong
import org.http4s.Method._
import org.http4s._
import org.http4s.client.dsl.io._
import org.http4s.syntax.literals._
import suite.HttpSuite

/** Route-level tests for [[GlobalFollowRoutes]] (the gl0-side TRANSPORT of the gl1 own-slice follow path,
  * `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`).
  *
  * Brings up a stub [[GlobalFollowSliceService]] (no real GSI source) so we exercise the route's response shape, the 503 startup/cold-start
  * gates, and the [[GlobalFollowSliceResponse]] JSON codec in isolation — the real slice production is covered by
  * `GlobalFollowSliceServiceSuite`.
  *
  * '''Coverage'''
  *   - 503 when the service `Ref` is empty (pre-startup).
  *   - 503 when the service is present but no global ordinal has finalized yet (`latestSlice` yields `None`).
  *   - 200 with the correct [[GlobalFollowSliceResponse]] body (the service's `(ordinal, slice)`) on success.
  */
object GlobalFollowRoutesSuite extends HttpSuite {

  override type Res = Unit
  override def sharedResource: cats.effect.Resource[IO, Unit] = cats.effect.Resource.unit[IO]

  // ===========================================================================
  // Fixtures
  // ===========================================================================

  private val anchorOrdinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(7L))

  private def addr(seed: Int): Address = Address.fromBytes(s"global-follow-routes-suite-seed-$seed".getBytes("UTF-8"))

  /** Canned Address-keyed slice — one Balances entry, one LastTxRefs entry. The route doesn't inspect the slice's contents; it just wraps
    * it in the response envelope, so any well-formed [[ConsumedFieldDelta]] exercises the codec.
    */
  private val cannedSlice: ConsumedFieldDelta =
    ConsumedFieldDelta.empty.copy(
      balances = SortedMap(addr(1) -> Balance(NonNegLong(1000L))),
      lastTxRefs = SortedMap(addr(2) -> TransactionReference(TransactionOrdinal(NonNegLong(3L)), Hash("ab" * 32)))
    )

  /** Service stub: `latestSlice` returns the canned `(ordinal, slice)` or `None` (cold start) per the fixture. */
  private def stubService(latest: Option[(SnapshotOrdinal, ConsumedFieldDelta)]): GlobalFollowSliceService[IO] =
    new GlobalFollowSliceService[IO] {
      def latestSlice: IO[Option[(SnapshotOrdinal, ConsumedFieldDelta)]] = IO.pure(latest)
    }

  /** Build the routes wired to a service `Ref`. `service = None` exercises the 503 startup path; a present service whose `latestSlice` is
    * `None` exercises the no-finalized-ordinal 503 path.
    */
  private def mkRoutes(service: Option[GlobalFollowSliceService[IO]]): IO[HttpRoutes[IO]] =
    Ref.of[IO, Option[GlobalFollowSliceService[IO]]](service).map(GlobalFollowRoutes[IO](_).publicRoutes)

  // ===========================================================================
  // Tests
  // ===========================================================================

  test("GET /global-follow/slice/latest returns 503 when service is not yet initialized") {
    val req = GET(uri"/global-follow/slice/latest")
    for {
      routes <- mkRoutes(None)
      r <- expectHttpStatus(routes, req)(Status.ServiceUnavailable)
    } yield r
  }

  test("GET /global-follow/slice/latest returns 503 when no global ordinal has finalized yet") {
    val req = GET(uri"/global-follow/slice/latest")
    for {
      routes <- mkRoutes(stubService(none).some)
      r <- expectHttpStatus(routes, req)(Status.ServiceUnavailable)
    } yield r
  }

  test("GET /global-follow/slice/latest returns 200 with the service's (ordinal, slice)") {
    val req = GET(uri"/global-follow/slice/latest")
    val expected = GlobalFollowSliceResponse(anchorOrdinal, cannedSlice)
    for {
      routes <- mkRoutes(stubService((anchorOrdinal -> cannedSlice).some).some)
      r <- expectHttpBodyAndStatus(routes, req)(expected, Status.Ok)
    } yield r
  }
}
