package io.constellationnetwork.node.shared.http.p2p.clients

import cats.effect.Async

import io.constellationnetwork.node.shared.domain.cluster.services.Session
import io.constellationnetwork.node.shared.http.p2p.PeerResponse
import io.constellationnetwork.node.shared.http.p2p.PeerResponse.PeerResponse
import io.constellationnetwork.schema.nakamoto.follow.GlobalFollowSliceResponse
import io.constellationnetwork.security.SecurityProvider

import org.http4s.client.Client

/** gl1-side fetch client for the gl0 → gl1 own-slice follow path — Slice 2b of `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md` (the
  * TRANSPORT half). Consumer-side counterpart of [[io.constellationnetwork.dag.l0.http.routes.GlobalFollowRoutes]]; the verify+apply wiring
  * into `GlobalSnapshotAlignment` / the gl1 follow loop is Slice 3 (this client is NOT wired anywhere yet).
  *
  * A REAL fetch (unlike the cross-shard [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProofClient]] whose
  * production impl is still a `noop` stub). Follows the [[PeerResponse]]/[[Client]] pattern of [[SnapshotClient]] /
  * [[L0GlobalSnapshotClient]]: a GET against a peer resolved at call time, response decoded via the standard circe `EntityDecoder` and
  * verified by the peer-auth response middleware [[PeerResponse]] wraps in.
  *
  * Fetches ONLY the latest-finalized full slice (design point 1, "Transfer model" — the overlay can't serve a value-accurate slice at a
  * non-tip historical ordinal, so there is no per-ordinal historical-diff fetch; gl1 recompute-matches the latest finalized state).
  */
trait GlobalFollowClient[F[_]] {

  /** Fetch the latest-finalized consumed-field slice from a peer: `GET /global-follow/slice/latest`. The peer (a
    * [[io.constellationnetwork.schema.peer.P2PContext]]) is supplied when the returned [[PeerResponse]] Kleisli is run, exactly like the
    * other p2p clients.
    */
  def getLatestSlice: PeerResponse[F, GlobalFollowSliceResponse]
}

object GlobalFollowClient {

  def make[F[_]: Async: SecurityProvider](
    client: Client[F],
    maybeSession: Option[Session[F]] = None
  ): GlobalFollowClient[F] =
    new GlobalFollowClient[F] {

      def getLatestSlice: PeerResponse[F, GlobalFollowSliceResponse] = {
        import org.http4s.circe.CirceEntityCodec.circeEntityDecoder

        PeerResponse[F, GlobalFollowSliceResponse]("global-follow/slice/latest")(client, maybeSession)
      }
    }
}
