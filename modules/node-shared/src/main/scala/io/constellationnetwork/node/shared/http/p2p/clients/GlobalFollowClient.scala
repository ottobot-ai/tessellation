package io.constellationnetwork.node.shared.http.p2p.clients

import cats.effect.Async

import io.constellationnetwork.node.shared.domain.cluster.services.Session
import io.constellationnetwork.node.shared.http.p2p.PeerResponse
import io.constellationnetwork.node.shared.http.p2p.PeerResponse.PeerResponse
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.follow.{GlobalChangeSetResponse, GlobalFollowSliceResponse}
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.serde.codecs.instances.GlobalChangeSetResponseCodec.{codec => globalChangeSetResponseCodec}

import org.http4s.client.Client
import org.http4s.{EntityDecoder, Uri}
import scodec.bits.BitVector

/** gl1-side fetch client for the gl0 → gl1 own-slice follow path — Slice 2b of `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md` (the
  * TRANSPORT half). Consumer-side counterpart of [[io.constellationnetwork.dag.l0.http.routes.GlobalFollowRoutes]]; the verify+apply wiring
  * into `GlobalSnapshotAlignment` / the gl1 follow loop is Slice 3 (this client is NOT wired anywhere yet).
  *
  * A REAL fetch (unlike the cross-shard [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProofClient]] whose
  * production impl is still a `noop` stub). Follows the [[PeerResponse]]/[[Client]] pattern of [[SnapshotClient]] /
  * [[L0GlobalSnapshotClient]]: a GET against a peer resolved at call time, response decoded via the standard circe `EntityDecoder` and
  * verified by the peer-auth response middleware [[PeerResponse]] wraps in.
  *
  * Fetches the latest-finalized full slice (the bootstrap / fallback path) and — for the #287 "send diffs" optimization — the incremental
  * change-set since a follower's last-verified tip. The overlay still can't serve a value-accurate slice at a non-tip historical ordinal;
  * the `since` diff is computed by gl0 from its retained recent-projection ring and verified by the follower against the LATEST finalized
  * snapshot, so it never depends on per-ordinal historical replay.
  */
trait GlobalFollowClient[F[_]] {

  /** Fetch the latest-finalized consumed-field slice from a peer: `GET /global-follow/slice/latest`. The peer (a
    * [[io.constellationnetwork.schema.peer.P2PContext]]) is supplied when the returned [[PeerResponse]] Kleisli is run, exactly like the
    * other p2p clients. The response always has `baseOrdinal = None` (full from-empty slice).
    */
  def getLatestSlice: PeerResponse[F, GlobalFollowSliceResponse]

  /** Fetch the #287 incremental slice vs `since` from a peer: `GET /global-follow/slice?since=<ordinalLong>`. The response carries
    * `baseOrdinal = Some(since)` when gl0 served a diff (or no-change) the follower must apply on the state it holds at `since`, or
    * `baseOrdinal = None` when `since` fell out of gl0's ring and it returned the full from-empty slice instead.
    */
  def getSliceSince(since: SnapshotOrdinal): PeerResponse[F, GlobalFollowSliceResponse]

  /** Fetch the currency-l0 (ml0) ADOPT changeset vs `since` from a peer: `GET /global-follow/changeset?since=<ordinalLong>` (task #12). The
    * response carries the contiguous per-ordinal typed [[io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator]]
    * deltas a FULL-state follower holding the GSI at `since` applies sequentially to reach gl0's latest finalized ordinal (`baseOrdinal =
    * Some(since)`), or `baseOrdinal = None` when `since` fell out of gl0's ring (the follower full-GSI-adopts instead). The wire body is a
    * FULLY-SCODEC octet-stream decoded via the canonical [[io.constellationnetwork.serde.codecs.instances.GlobalChangeSetResponseCodec]] —
    * the accumulator binary is scodec, never Circe.
    */
  def getChangeSetSince(since: SnapshotOrdinal): PeerResponse[F, GlobalChangeSetResponse]
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

      def getSliceSince(since: SnapshotOrdinal): PeerResponse[F, GlobalFollowSliceResponse] = {
        import org.http4s.circe.CirceEntityCodec.circeEntityDecoder

        // Build the query param via `withQueryParam` (NOT a raw `?` in the path string — `addPath` would
        // percent-encode it and the server's `:? SinceOrdinalParam` matcher would never fire), mirroring
        // `L0GlobalSnapshotClient.getFull`'s `uri.addPath(...).withQueryParam(...)`.
        PeerResponse[F, GlobalFollowSliceResponse]((uri: Uri) =>
          uri.addPath("global-follow/slice").withQueryParam("since", since.value.value)
        )(client, maybeSession)
      }

      def getChangeSetSince(since: SnapshotOrdinal): PeerResponse[F, GlobalChangeSetResponse] = {
        // FULLY-SCODEC octet-stream: decode the raw response bytes (http4s built-in `Array[Byte]` decoder,
        // application/octet-stream) with the canonical `GlobalChangeSetResponse` scodec codec — NO Circe. Same
        // `withQueryParam` request-building rule as `getSliceSince` so the server's `:? SinceOrdinalParam`
        // matcher fires.
        implicit val changeSetResponseDecoder: EntityDecoder[F, GlobalChangeSetResponse] =
          EntityDecoder.byteArrayDecoder[F].flatMapR { bytes =>
            globalChangeSetResponseCodec.decodeValue(BitVector(bytes)) match {
              case scodec.Attempt.Successful(response) => cats.data.EitherT.rightT[F, org.http4s.DecodeFailure](response)
              case scodec.Attempt.Failure(err) =>
                cats.data.EitherT.leftT[F, GlobalChangeSetResponse](
                  org.http4s.InvalidMessageBodyFailure(s"GlobalChangeSetResponse scodec decode failed: ${err.messageWithContext}")
                )
            }
          }

        PeerResponse[F, GlobalChangeSetResponse]((uri: Uri) =>
          uri.addPath("global-follow/changeset").withQueryParam("since", since.value.value)
        )(client, maybeSession)
      }
    }
}
