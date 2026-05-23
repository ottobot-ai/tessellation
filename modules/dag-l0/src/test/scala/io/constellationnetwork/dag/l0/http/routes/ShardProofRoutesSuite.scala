package io.constellationnetwork.dag.l0.http.routes

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardSubtreeProof, ShardSubtreeProofService}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.prover.attestation.MerklePatriciaInclusionProof
import io.constellationnetwork.security.signature.Signed

import io.circe.Json
import io.circe.syntax._
import org.http4s.Method._
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.client.dsl.io._
import org.http4s.syntax.literals._
import suite.HttpSuite

/** Slice 10 — route-level tests for [[ShardProofRoutes]].
  *
  * Brings up a stub [[ShardSubtreeProofService]] (no real chain or MPT) so we can exercise the route's response shape, error mapping, and
  * JSON codec for [[ShardSubtreeProof]] in isolation. The service stub returns canned proofs / `None` — testing the underlying proof
  * generation + verification is the job of `ShardSubtreeProofServiceSuite`.
  *
  * '''Coverage'''
  *   - 503 when the service Ref is empty (pre-startup).
  *   - 400 when shardId is malformed.
  *   - 400 when the `metagraph` query parameter is missing.
  *   - 400 when the body is not a valid `GlobalStateKey`.
  *   - 200 with a [[ShardSubtreeProof]] body on success.
  *   - 404 when the service returns None.
  */
object ShardProofRoutesSuite extends HttpSuite {

  override type Res = Unit
  override def sharedResource: cats.effect.Resource[IO, Unit] = cats.effect.Resource.unit[IO]

  // ===========================================================================
  // Fixtures
  // ===========================================================================

  /** Deterministic Address from a label — same convention as `ShardSubtreeProofServiceSuite`. */
  private def addr(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))

  /** Canned key — Balances partition for a specific user. The route doesn't inspect the key's structure; the JSON-encoded form is decoded
    * by the route and passed to the service stub.
    */
  private val cannedKey: GlobalStateKey =
    GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr("user-routes-suite"))

  /** Canned [[ShardSubtreeProof]] — used as the service stub's positive return value. The route encodes it as JSON and returns 200. Field
    * values don't need to verify; the route just streams them as-is.
    */
  private val cannedProof: ShardSubtreeProof =
    ShardSubtreeProof(
      shardCheckpointHash = Hash("aa" * 32),
      metagraphAddress = addr("mg-routes-suite"),
      perMgMptRoot = Hash("bb" * 32),
      key = cannedKey,
      value = Some(Hex("cc" * 16)),
      mptProof = MerklePatriciaInclusionProof(Hex("dd" * 16), List.empty)
    )

  /** Service stub that returns `proof` for any generate call, and a configurable verify result. */
  private def stubService(
    proof: Option[ShardSubtreeProof] = cannedProof.some,
    verifyResult: Boolean = true
  ): ShardSubtreeProofService[IO] =
    new ShardSubtreeProofService[IO] {
      def generateProofForMetagraph(
        shardId: ShardId,
        metagraphAddress: Address,
        key: GlobalStateKey
      ): IO[Option[ShardSubtreeProof]] = IO.pure(proof)

      def verifyProof(
        shardCheckpoint: Signed[io.constellationnetwork.schema.sharding.ShardCheckpoint],
        proof: ShardSubtreeProof
      ): IO[Boolean] = IO.pure(verifyResult)
    }

  /** Build the routes wired to a service `Ref`. `s = None` exercises the 503 startup-not-ready path; `s = Some(stub)` exercises the happy +
    * 404 paths.
    */
  private def mkRoutes(s: Option[ShardSubtreeProofService[IO]]): IO[HttpRoutes[IO]] =
    Ref.of[IO, Option[ShardSubtreeProofService[IO]]](s).map(ShardProofRoutes[IO](_).publicRoutes)

  // A valid metagraph address parseable by `AddressVar` — checksummed DAG address from the wallet
  // tests. Hard-coded so the URL parser succeeds; the route then forwards to the stubbed service
  // which accepts any address.
  private val validMetagraph: String = "DAG6Yp9hSWZD4TFiNJ7HmrPRWPYjwgVxVD89uvY8"

  // ===========================================================================
  // Tests
  // ===========================================================================

  test("POST /shard/0/proof returns 503 when service is not yet initialized") {
    val req = POST(cannedKey.asJson, uri"/shard/0/proof?metagraph=DAG6Yp9hSWZD4TFiNJ7HmrPRWPYjwgVxVD89uvY8")
    for {
      routes <- mkRoutes(None)
      r <- expectHttpStatus(routes, req)(Status.ServiceUnavailable)
    } yield r
  }

  test("POST /shard/{bad}/proof returns 400 when shardId is not a non-negative integer") {
    val req = POST(cannedKey.asJson, uri"/shard/abc/proof?metagraph=DAG6Yp9hSWZD4TFiNJ7HmrPRWPYjwgVxVD89uvY8")
    for {
      routes <- mkRoutes(Some(stubService()))
      r <- expectHttpStatus(routes, req)(Status.BadRequest)
    } yield r
  }

  test("POST /shard/0/proof returns 400 when metagraph query parameter is missing") {
    val req = POST(cannedKey.asJson, uri"/shard/0/proof")
    for {
      routes <- mkRoutes(Some(stubService()))
      r <- expectHttpStatus(routes, req)(Status.BadRequest)
    } yield r
  }

  test("POST /shard/0/proof returns 400 when body is not a valid GlobalStateKey JSON") {
    val req = POST(Json.obj("garbage" -> Json.fromInt(1)), uri"/shard/0/proof?metagraph=DAG6Yp9hSWZD4TFiNJ7HmrPRWPYjwgVxVD89uvY8")
    for {
      routes <- mkRoutes(Some(stubService()))
      r <- expectHttpStatus(routes, req)(Status.BadRequest)
    } yield r
  }

  test("POST /shard/0/proof returns 200 with ShardSubtreeProof JSON on success") {
    val req = POST(cannedKey.asJson, Uri.unsafeFromString(s"/shard/0/proof?metagraph=$validMetagraph"))
    for {
      routes <- mkRoutes(Some(stubService(cannedProof.some)))
      r <- expectHttpBodyAndStatus(routes, req)(cannedProof, Status.Ok)
    } yield r
  }

  test("POST /shard/0/proof returns 404 when service returns None (no proof available)") {
    val req = POST(cannedKey.asJson, Uri.unsafeFromString(s"/shard/0/proof?metagraph=$validMetagraph"))
    for {
      routes <- mkRoutes(Some(stubService(None)))
      r <- expectHttpStatus(routes, req)(Status.NotFound)
    } yield r
  }
}
