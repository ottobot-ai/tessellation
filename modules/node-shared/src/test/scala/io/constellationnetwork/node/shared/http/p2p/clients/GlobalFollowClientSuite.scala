package io.constellationnetwork.node.shared.http.p2p.clients

import cats.data.NonEmptySet
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateFieldId
import io.constellationnetwork.schema.nakamoto.follow.{ConsumedFieldDelta, GlobalFollowSliceResponse}
import io.constellationnetwork.schema.peer.{P2PContext, PeerId}
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import com.comcast.ip4s.IpLiteralSyntax
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import org.http4s.client.Client
import org.http4s.{Request, Response, Uri}
import weaver.MutableIOSuite

/** Slice 2b — codec + request-shape tests for [[GlobalFollowClient]] (the gl1-side fetch half of the own-slice follow transport,
  * `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`).
  *
  * There is no in-memory `PeerResponse` server+client round-trip harness in the codebase to mirror (the `PeerResponse` GET path wraps the
  * client in `PeerAuthMiddleware.responseVerifierMiddleware`, which cryptographically verifies a signed response — a full round-trip would
  * need a matching response-signing server). Per the slice's test bar, we therefore cover:
  *   - the [[GlobalFollowSliceResponse]] Circe codec round-trips (encode → decode == identity), incl. a non-empty [[ConsumedFieldDelta]];
  *   - the client issues the correct request: `GET /global-follow/slice/latest` against the supplied peer.
  *
  * The request-shape test captures the outgoing request inside a stub inner [[Client]]. `responseVerifierMiddleware` calls `client.run(req)`
  * (passing the request through unchanged) before it attempts to verify the response, so the capture sees the exact URI the client built;
  * the subsequent signature verification fails (the stub response is unsigned), which we discard via `.attempt` — only the captured URI is
  * asserted.
  */
object GlobalFollowClientSuite extends MutableIOSuite {

  override type Res = SecurityProvider[IO]
  override def sharedResource: Resource[IO, SecurityProvider[IO]] = SecurityProvider.forAsync[IO]

  private val peer: P2PContext =
    P2PContext(host"127.0.0.1", port"9000", PeerId(Hex("aa" * 64)))

  private def addr(seed: Int): Address = Address.fromBytes(s"global-follow-client-suite-seed-$seed".getBytes("UTF-8"))

  /** A dummy `Signed[TokenLock]` — the signature is not verified anywhere in this codec round-trip; only its JSON encode/decode identity is
    * exercised, so a fixed placeholder proof is sufficient (mirrors the dummy-proof pattern used across the dag-l0 suites).
    */
  private def signedTokenLock(seed: Int): Signed[TokenLock] =
    Signed(
      TokenLock(
        source = addr(seed),
        amount = TokenLockAmount(PosLong(500L)),
        fee = TokenLockFee(NonNegLong(1L)),
        parent = TokenLockReference(TokenLockOrdinal(NonNegLong(1L)), Hash("cd" * 32)),
        currencyId = none,
        unlockEpoch = EpochProgress(NonNegLong(1000L)).some,
        replaceTokenLockRef = none
      ),
      NonEmptySet.one(SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70))))
    )

  private val sampleResponse: GlobalFollowSliceResponse =
    GlobalFollowSliceResponse(
      ordinal = SnapshotOrdinal(NonNegLong(42L)),
      slice = ConsumedFieldDelta(
        balances = SortedMap[Address, Balance](addr(1) -> Balance(NonNegLong(1000L)), addr(2) -> Balance(NonNegLong(2000L))),
        lastTxRefs = SortedMap[Address, TransactionReference](addr(3) -> TransactionReference(TransactionOrdinal(NonNegLong(5L)), Hash("ab" * 32))),
        lastAllowSpendRefs = SortedMap.empty,
        lastTokenLockRefs = SortedMap.empty,
        // A non-empty `activeTokenLocks` so the 5th consumed field round-trips through the Circe codec too.
        activeTokenLocks = SortedMap[Address, SortedSet[Signed[TokenLock]]](addr(6) -> SortedSet(signedTokenLock(6))),
        removals = SortedMap[GlobalStateFieldId, Set[Address]](
          GlobalStateFieldId.LastAllowSpendRefs -> Set(addr(4), addr(5))
        )
      )
    )

  test("GlobalFollowSliceResponse round-trips through its Circe codec (incl. a non-empty slice)") { _ =>
    import io.circe.syntax._

    val decoded = sampleResponse.asJson.as[GlobalFollowSliceResponse]
    IO.pure(
      expect(decoded == Right(sampleResponse)) &&
        // the encoded envelope has the contract field names
        expect(sampleResponse.asJson.hcursor.keys.map(_.toList).contains(List("ordinal", "slice")))
    )
  }

  test("GlobalFollowSliceResponse round-trips when the slice is empty") { _ =>
    import io.circe.syntax._

    val r = GlobalFollowSliceResponse(SnapshotOrdinal(NonNegLong(0L)), ConsumedFieldDelta.empty)
    IO.pure(expect(r.asJson.as[GlobalFollowSliceResponse] == Right(r)))
  }

  test("getLatestSlice issues GET /global-follow/slice/latest against the peer") { implicit sp =>
    for {
      captured <- Ref.of[IO, Option[Uri]](none[Uri])
      // Inner client records the outgoing request URI, then returns an (unsigned) 200. The peer-auth
      // response verifier the PeerResponse GET wraps around this runs AFTER this `run`, so the capture
      // is taken regardless of the verification outcome.
      innerClient = Client[IO] { (req: Request[IO]) =>
        Resource.eval(captured.set(req.uri.some)).as(Response[IO]())
      }
      client = GlobalFollowClient.make[IO](innerClient)
      _ <- client.getLatestSlice.run(peer).attempt
      uriOpt <- captured.get
    } yield uriOpt match {
      case Some(uri) =>
        expect(uri.path.renderString == "/global-follow/slice/latest") &&
          expect(uri.host.map(_.value).contains("127.0.0.1")) &&
          expect(uri.port.contains(9000))
      case None => failure("client did not issue any request")
    }
  }
}
