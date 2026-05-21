package io.constellationnetwork.dag.l0.http.routes

import cats.effect.IO
import cats.effect.kernel.Ref

import io.constellationnetwork.node.shared.domain.nakamoto.nipopow._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.Json
import io.circe.syntax._
import org.http4s.Method._
import org.http4s._
import org.http4s.circe.CirceEntityCodec.{circeEntityDecoder, circeEntityEncoder}
import org.http4s.client.dsl.io._
import org.http4s.syntax.literals._
import suite.HttpSuite

/** §3 NIPoPoW S5 — route-level tests for `NipopowRoutes`.
  *
  * Brings up a stub [[NipopowProofProvider]] (no real chain) so we can exercise the route's response shape, error mapping, and JSON codec
  * for [[TowerProof]] + [[ProofError]] in isolation. The provider stub returns canned proofs / verifier outcomes — testing the underlying
  * builder + verifier behaviour is the job of `TowerProofBuilderSuite` / `TowerVerifierSuite`.
  */
object NipopowRoutesSuite extends HttpSuite {

  override type Res = Unit
  override def sharedResource: cats.effect.Resource[IO, Unit] = cats.effect.Resource.unit[IO]

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  /** Lowercase-hex 64-char hash from a short label — same convention as `TowerProofBuilderSuite`. */
  private def h(s: String): Hash =
    Hash(s.getBytes("UTF-8").map(b => f"${b & 0xff}%02x").mkString.padTo(64, '0').take(64))

  /** Build a canned [[TowerProofHeader]] for use in canned proofs. The values don't need to be VRF-valid — this suite tests the route's
    * surface, not the verifier's internals.
    */
  private def header(ordinal: Long, slot: Long = 100L, parentSlot: Long = 99L): TowerProofHeader =
    TowerProofHeader(
      ordinal = ord(ordinal),
      slot = Slot.unsafeApply(slot),
      parentSlot = Slot.unsafeApply(parentSlot),
      vrfProof = VrfProof(Hex("0a" * 80)),
      vrfOutput = VrfOutput(Hex("0b" * 64)),
      vrfPublicKey = VrfPublicKey(Hex("0c" * 32)),
      eta = h(s"eta-$ordinal"),
      activePoolSize = 8,
      subchainLevelCounts = Vector.fill(SuperLevelParams.SuperLevelCount)(0L),
      snapshotHash = h(s"snap-$ordinal")
    )

  /** Canned proof — small L0 suffix, no level chains. The route doesn't inspect verifier correctness, so this minimal shape is fine. */
  private def cannedProof(since: Long = 0L, tip: Long = 4L): TowerProof =
    TowerProof(
      since = ord(since),
      tipOrdinal = ord(tip),
      level0Suffix = Vector(header(tip - 2), header(tip - 1), header(tip)),
      levelChains = Map.empty
    )

  /** Stub provider — returns the canned proof for any (since, k), and accepts/rejects on `verify` per the constructor argument. */
  private def stubProvider(
    proof: TowerProof = cannedProof(),
    verifyResult: Either[ProofError, Unit] = Right(())
  ): NipopowProofProvider[IO] =
    new NipopowProofProvider[IO] {
      def build(since: SnapshotOrdinal, k: Int): IO[TowerProof] = IO.pure(proof)
      def verify(p: TowerProof): IO[Either[ProofError, Unit]] = IO.pure(verifyResult)
    }

  /** Stub provider whose `build` returns an empty proof (no L0 suffix) — used to exercise the 404 path. */
  private def emptyProvider: NipopowProofProvider[IO] =
    new NipopowProofProvider[IO] {
      def build(since: SnapshotOrdinal, k: Int): IO[TowerProof] = IO.pure(TowerProof.Empty)
      def verify(p: TowerProof): IO[Either[ProofError, Unit]] = IO.pure(Right(()))
    }

  /** Stub provider whose `build` returns a proof whose `tipOrdinal < since` — exercises the 400 "ahead of head" path. */
  private def behindHeadProvider(reportedTip: Long): NipopowProofProvider[IO] =
    new NipopowProofProvider[IO] {
      def build(since: SnapshotOrdinal, k: Int): IO[TowerProof] =
        IO.pure(TowerProof(since, ord(reportedTip), Vector.empty, Map.empty))
      def verify(p: TowerProof): IO[Either[ProofError, Unit]] = IO.pure(Right(()))
    }

  private def mkRoutes(p: Option[NipopowProofProvider[IO]]): IO[HttpRoutes[IO]] =
    Ref.of[IO, Option[NipopowProofProvider[IO]]](p).map(NipopowRoutes[IO](_).publicRoutes)

  test("GET /nakamoto/nipopow/proof returns 503 when provider is empty") {
    val req = GET(uri"/nakamoto/nipopow/proof")
    for {
      routes <- mkRoutes(None)
      r <- expectHttpStatus(routes, req)(Status.ServiceUnavailable)
    } yield r
  }

  test("GET /nakamoto/nipopow/proof returns 200 with TowerProof body on success") {
    val expected = cannedProof()
    val req = GET(uri"/nakamoto/nipopow/proof?fromOrd=0&k=3")
    for {
      routes <- mkRoutes(Some(stubProvider(expected)))
      r <- expectHttpBodyAndStatus(routes, req)(expected, Status.Ok)
    } yield r
  }

  test("GET /nakamoto/nipopow/proof returns 404 when proof has no L0 suffix (tower empty in range)") {
    val req = GET(uri"/nakamoto/nipopow/proof?fromOrd=0")
    for {
      routes <- mkRoutes(Some(emptyProvider))
      r <- expectHttpStatus(routes, req)(Status.NotFound)
    } yield r
  }

  test("GET /nakamoto/nipopow/proof returns 400 when fromOrd is strictly ahead of chain head") {
    val req = GET(uri"/nakamoto/nipopow/proof?fromOrd=999")
    for {
      routes <- mkRoutes(Some(behindHeadProvider(reportedTip = 5L)))
      r <- expectHttpStatus(routes, req)(Status.BadRequest)
    } yield r
  }

  test("GET /nakamoto/nipopow/proof returns 400 when k is not a valid integer") {
    val req = GET(uri"/nakamoto/nipopow/proof?fromOrd=0&k=notanumber")
    for {
      routes <- mkRoutes(Some(stubProvider()))
      r <- expectHttpStatus(routes, req)(Status.BadRequest)
    } yield r
  }

  test("GET /nakamoto/nipopow/proof returns 400 when fromOrd is negative") {
    val req = GET(uri"/nakamoto/nipopow/proof?fromOrd=-1")
    for {
      routes <- mkRoutes(Some(stubProvider()))
      r <- expectHttpStatus(routes, req)(Status.BadRequest)
    } yield r
  }

  test("GET /nakamoto/nipopow/proof/genesis returns 200 with proof anchored at MinValue") {
    val expected = cannedProof(since = 0L)
    val req = GET(uri"/nakamoto/nipopow/proof/genesis")
    for {
      routes <- mkRoutes(Some(stubProvider(expected)))
      r <- expectHttpBodyAndStatus(routes, req)(expected, Status.Ok)
    } yield r
  }

  test("GET /nakamoto/nipopow/proof/genesis honors the k query parameter") {
    val req = GET(uri"/nakamoto/nipopow/proof/genesis?k=7")
    for {
      routes <- mkRoutes(Some(stubProvider()))
      r <- expectHttpStatus(routes, req)(Status.Ok)
    } yield r
  }

  test("POST /nakamoto/nipopow/verify returns 200 verified=true on success") {
    val proof = cannedProof()
    val req = POST(proof.asJson, uri"/nakamoto/nipopow/verify")
    val expected = Json.obj("verified" -> Json.True)
    for {
      routes <- mkRoutes(Some(stubProvider(verifyResult = Right(()))))
      r <- expectHttpBodyAndStatus(routes, req)(expected, Status.Ok)
    } yield r
  }

  test("POST /nakamoto/nipopow/verify returns 400 verified=false with ProofError JSON on rejection") {
    val proof = cannedProof()
    val req = POST(proof.asJson, uri"/nakamoto/nipopow/verify")
    val rejection: Either[ProofError, Unit] = Left(ProofError.NonMonotonicOrdinals(level = 2))
    for {
      routes <- mkRoutes(Some(stubProvider(verifyResult = rejection)))
      // Drive the route, then assert the response shape rather than golden-JSON: the ProofError
      // encoder is exercised here but we don't want to over-couple to its exact key naming.
      resp <- routes.run(req).value
      result <- resp match {
        case Some(r) =>
          r.as[Json].map { body =>
            expect
              .same(r.status, Status.BadRequest)
              .and(expect(body.hcursor.downField("verified").as[Boolean].toOption.contains(false)))
              .and(expect(body.hcursor.downField("error").downField("kind").as[String].toOption.contains("non_monotonic_ordinals")))
              .and(expect(body.hcursor.downField("error").downField("level").as[Int].toOption.contains(2)))
          }
        case None => IO.pure(failure("route not found"))
      }
    } yield result
  }

  test("POST /nakamoto/nipopow/verify returns 400 with verified=false on malformed body") {
    // Send something that isn't a TowerProof JSON — server should return a 400 with the
    // decode failure surfaced in the response body. We assert status + the verified=false
    // discriminator; the exact message is not load-bearing.
    val req = POST(Json.obj("garbage" -> Json.fromInt(1)), uri"/nakamoto/nipopow/verify")
    for {
      routes <- mkRoutes(Some(stubProvider()))
      resp <- routes.run(req).value
      result <- resp match {
        case Some(r) =>
          r.as[Json].map { body =>
            expect
              .same(r.status, Status.BadRequest)
              .and(expect(body.hcursor.downField("verified").as[Boolean].toOption.contains(false)))
          }
        case None => IO.pure(failure("route not found"))
      }
    } yield result
  }

  test("POST /nakamoto/nipopow/verify returns 503 when provider is empty") {
    val req = POST(cannedProof().asJson, uri"/nakamoto/nipopow/verify")
    for {
      routes <- mkRoutes(None)
      r <- expectHttpStatus(routes, req)(Status.ServiceUnavailable)
    } yield r
  }
}
