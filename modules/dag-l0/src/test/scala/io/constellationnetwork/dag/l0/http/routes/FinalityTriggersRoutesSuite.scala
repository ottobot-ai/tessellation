package io.constellationnetwork.dag.l0.http.routes

import cats.effect.IO
import cats.effect.kernel.Ref

import io.constellationnetwork.node.shared.domain.nakamoto.{FinalityTrigger, FinalityTriggerView}
import io.constellationnetwork.schema.SnapshotOrdinal

import io.circe.Json
import io.circe.syntax._
import org.http4s.Method.GET
import org.http4s._
import org.http4s.client.dsl.io._
import org.http4s.syntax.literals._
import suite.HttpSuite

/** Route-level tests for `GET /global-snapshots/{ord}/finality-triggers` (#138).
  *
  * Spins up a `FinalityTriggerView` against an in-memory Ref so we can drive exactly which kinds qualify which ordinals, then asserts the
  * JSON shape and content. Pure observability — no consensus components touched.
  */
object FinalityTriggersRoutesSuite extends HttpSuite {

  override type Res = Unit
  override def sharedResource: cats.effect.Resource[IO, Unit] = cats.effect.Resource.unit[IO]

  /** Stub `FinalityTriggerView` that returns the given set verbatim for the requested ordinal. */
  private def viewReturning(kindsByOrd: Map[Long, Set[FinalityTrigger.Kind]]): FinalityTriggerView[IO] =
    new FinalityTriggerView[IO] {
      def triggersFor(ord: SnapshotOrdinal): IO[Set[FinalityTrigger.Kind]] =
        IO.pure(kindsByOrd.getOrElse(ord.value.value, Set.empty))
    }

  private def mkRoutes(view: Option[FinalityTriggerView[IO]]) =
    Ref.of[IO, Option[FinalityTriggerView[IO]]](view).map(FinalityTriggersRoutes[IO](_).publicRoutes)

  test("returns 503 when the view ref is empty (pre-startup)") {
    val req = GET(uri"/global-snapshots/100/finality-triggers")
    for {
      routes <- mkRoutes(None)
      result <- expectHttpStatus(routes, req)(Status.ServiceUnavailable)
    } yield result
  }

  test("returns the expected JSON shape for an ordinal qualified by all three Phase 1→2 triggers") {
    val view = viewReturning(
      Map(
        100L -> Set[FinalityTrigger.Kind](
          FinalityTrigger.Kind.TWeight,
          FinalityTrigger.Kind.TCount,
          FinalityTrigger.Kind.TDepth1
        )
      )
    )
    val req = GET(uri"/global-snapshots/100/finality-triggers")
    val expected = Json.obj(
      "ordinal" -> 100L.asJson,
      // Alphabetical order: t_count, t_depth1, t_weight.
      "triggers" -> List("t_count", "t_depth1", "t_weight").asJson,
      "satisfied_count" -> 3.asJson,
      "phases" -> Json.obj(
        "phase_1_to_2_count" -> 3.asJson,
        "phase_2_to_3" -> false.asJson
      )
    )
    for {
      routes <- mkRoutes(Some(view))
      result <- expectHttpBodyAndStatus(routes, req)(expected, Status.Ok)
    } yield result
  }

  test("returns satisfied_count=1 when only depth-only finalized") {
    val view = viewReturning(Map(50L -> Set[FinalityTrigger.Kind](FinalityTrigger.Kind.TDepth1)))
    val req = GET(uri"/global-snapshots/50/finality-triggers")
    val expected = Json.obj(
      "ordinal" -> 50L.asJson,
      "triggers" -> List("t_depth1").asJson,
      "satisfied_count" -> 1.asJson,
      "phases" -> Json.obj(
        "phase_1_to_2_count" -> 1.asJson,
        "phase_2_to_3" -> false.asJson
      )
    )
    for {
      routes <- mkRoutes(Some(view))
      result <- expectHttpBodyAndStatus(routes, req)(expected, Status.Ok)
    } yield result
  }

  test("flags phase_2_to_3=true when T_depth2 qualifies the ordinal") {
    val view = viewReturning(
      Map(
        10L -> Set[FinalityTrigger.Kind](
          FinalityTrigger.Kind.TWeight,
          FinalityTrigger.Kind.TCount,
          FinalityTrigger.Kind.TDepth1,
          FinalityTrigger.Kind.TDepth2
        )
      )
    )
    val req = GET(uri"/global-snapshots/10/finality-triggers")
    val expected = Json.obj(
      "ordinal" -> 10L.asJson,
      "triggers" -> List("t_count", "t_depth1", "t_depth2", "t_weight").asJson,
      "satisfied_count" -> 4.asJson,
      "phases" -> Json.obj(
        "phase_1_to_2_count" -> 3.asJson,
        "phase_2_to_3" -> true.asJson
      )
    )
    for {
      routes <- mkRoutes(Some(view))
      result <- expectHttpBodyAndStatus(routes, req)(expected, Status.Ok)
    } yield result
  }

  test("returns satisfied_count=0 with empty triggers list when nothing qualifies the ordinal") {
    val view = viewReturning(Map.empty)
    val req = GET(uri"/global-snapshots/999/finality-triggers")
    val expected = Json.obj(
      "ordinal" -> 999L.asJson,
      "triggers" -> List.empty[String].asJson,
      "satisfied_count" -> 0.asJson,
      "phases" -> Json.obj(
        "phase_1_to_2_count" -> 0.asJson,
        "phase_2_to_3" -> false.asJson
      )
    )
    for {
      routes <- mkRoutes(Some(view))
      result <- expectHttpBodyAndStatus(routes, req)(expected, Status.Ok)
    } yield result
  }
}
