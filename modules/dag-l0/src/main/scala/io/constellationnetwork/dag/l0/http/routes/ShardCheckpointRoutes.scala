package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema.sharding.{ShardId, ShardOrdinal}
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegInt
import io.circe.Json
import io.circe.syntax._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Response}

/** Read-only serve side of the shard-checkpoint chain-sync (run-20, task #A — see `docs/nakamoto/SHARD-CHECKPOINT-CHAINSYNC-DESIGN.md`). A
  * peer that missed a checkpoint (boot window / gossip drop) PULLS it from a node tracking that shard, instead of waiting minutes for
  * GossipSub re-gossip (which dedups Tier-1's identical re-publish bytes). This is the shard analogue of the global
  * `ChainSyncServer.serveSnapshots`/`serveMetagraphBinaries`.
  *
  * '''Endpoints''' (served on the public app — response-signed so the puller's `responseVerifierMiddleware` authenticates the source; the
  * checkpoint is self-authenticating regardless, via its committee signatures + the puller's `evaluate` re-check on re-feed):
  *   - `GET /shard-checkpoints/{shardId}/by-hash/{hash}` — the `Signed[ShardCheckpoint]` with this canonical hash (T1 orphan recovery).
  *   - `GET /shard-checkpoints/{shardId}/by-ordinal/{n}` — the CANONICAL checkpoint at ordinal `n` (T2 absence / genesis-miss recovery).
  *     `getByOrdinal` walks the canonical chain from `bestTip`, so this never serves an orphan/non-canonical sibling.
  *
  * '''Read-only invariant''' (mirrors `ChainSyncServer.serveMetagraphBinaries`'s "correction A"): both handlers call only `getByHash` /
  * `getByOrdinal`, which are pure `Ref.get` reads. A peer fetch never mutates this node's admission/adopt state.
  *
  * '''Responses''': 200 `Signed[ShardCheckpoint]` JSON; 400 malformed `shardId`/`ordinal`; 404 shard-not-tracked or checkpoint-absent; 503
  * if sharding is inactive (`numShards = 1`, `deps = None`).
  *
  * '''Greenfield / HOCON''' (`[[feedback-greenfield-no-wire-compat]]`, `[[feedback-prefer-hocon-over-sysenv]]`): fresh route; the deps come
  * from the typed `ShardCheckpointWiring.acceptanceDeps`, no `sys.env`.
  */
final case class ShardCheckpointRoutes[F[_]: Async](
  deps: Option[ShardCheckpointWiring.AcceptanceDeps[F]]
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  protected val prefixPath: InternalUrlPrefix = "/shard-checkpoints"

  private def parseShardId(raw: String): Option[ShardId] =
    for {
      i <- scala.util.Try(raw.toInt).toOption
      n <- NonNegInt.from(i).toOption
    } yield ShardId(n)

  /** Resolve the per-shard chain store, gating on sharding being active (503) and the shard being tracked (404). */
  private def withChainStore(shardId: ShardId)(f: ShardChainStore[F] => F[Response[F]]): F[Response[F]] =
    deps match {
      case None =>
        ServiceUnavailable(Json.obj("message" -> Json.fromString("sharding inactive (numShards=1)")))
      case Some(d) =>
        d.registry.get(shardId).map(_.chainStore) match {
          case None     => NotFound(Json.obj("message" -> Json.fromString(s"shard ${shardId.value.value} not tracked locally")))
          case Some(cs) => f(cs)
        }
    }

  protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / shardIdRaw / "by-hash" / hashRaw =>
      parseShardId(shardIdRaw) match {
        case None => BadRequest(Json.obj("message" -> Json.fromString(s"shardId must be a non-negative integer; got '$shardIdRaw'")))
        case Some(shardId) =>
          withChainStore(shardId) { cs =>
            cs.getByHash(Hash(hashRaw)).flatMap {
              case Some(hashed) => Ok(hashed.signed.asJson)
              case None         => NotFound(Json.obj("message" -> Json.fromString(s"no checkpoint with hash $hashRaw")))
            }
          }
      }

    case GET -> Root / shardIdRaw / "by-ordinal" / ordRaw =>
      parseShardId(shardIdRaw) match {
        case None => BadRequest(Json.obj("message" -> Json.fromString(s"shardId must be a non-negative integer; got '$shardIdRaw'")))
        case Some(shardId) =>
          scala.util.Try(ordRaw.toLong).toOption match {
            case None => BadRequest(Json.obj("message" -> Json.fromString(s"ordinal must be a long; got '$ordRaw'")))
            case Some(ordL) =>
              withChainStore(shardId) { cs =>
                cs.getByOrdinal(ShardOrdinal(ordL)).flatMap {
                  case Some(hashed) => Ok(hashed.signed.asJson)
                  case None         => NotFound(Json.obj("message" -> Json.fromString(s"no canonical checkpoint at ordinal $ordL")))
                }
              }
          }
      }
  }
}
