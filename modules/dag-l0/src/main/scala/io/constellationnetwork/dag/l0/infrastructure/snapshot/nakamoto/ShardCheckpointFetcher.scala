package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage
import io.constellationnetwork.node.shared.http.p2p.PeerResponse
import io.constellationnetwork.schema.peer.{P2PContext, Peer}
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId, ShardOrdinal}
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed

import org.http4s.Uri
import org.http4s.client.Client
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Pull-based recovery for shard checkpoints (run-20, task #A) — the shard analogue of the global `ChainSyncManager`.
  *
  * '''Why this exists.''' Shard checkpoints are gossip-only. A checkpoint missed at boot (or dropped by gossip) CANNOT be healed by
  * re-gossip: Tier-1 re-publishes the SAME bytes (stable identity so attestations concentrate), and GossipSub dedups identical bytes by
  * `msgid` at the sidecar's seen-cache until its TTL expires — the multi-minute hole run-20 root-caused. So a node that is missing a
  * checkpoint must PULL it. This fetcher issues an authenticated gl0→gl0 HTTP GET (over the p2p port, the same transport the global
  * snapshot client uses) against a peer's read-only serve route, returning the `Signed[ShardCheckpoint]`. The caller (the daemon) converts
  * it to the wire form and re-feeds it through the IDENTICAL `handleShardCheckpoint` accept path a gossiped checkpoint takes, so a pulled
  * checkpoint is judged byte-identically.
  *
  * '''Best-effort + deduped.''' Every fetch is best-effort: any error logs at debug and returns `None` (the next absence/orphan tick
  * retries). A per-`(shard, key)` cooldown (`pullDedupCooldownMs`) suppresses re-requesting the same checkpoint inside the window, mirroring
  * `ChainSyncManager.requestMissing`'s `inflightRef`. Peer selection is a time-rotated pick over `getResponsivePeers` (the committee is a
  * subset of responsive gl0 peers; the serve is read-only, so any peer that holds the checkpoint is a valid source).
  */
trait ShardCheckpointFetcher[F[_]] {

  /** Pull the canonical checkpoint at `(shardId, ordinal)` from a peer. Used by the absence tick (T2) — including the genesis-miss case
    * (an empty store pulls ordinal 1). `None` ⇒ suppressed by cooldown, no peer, or the pull failed.
    */
  def fetchByOrdinal(shardId: ShardId, ordinal: ShardOrdinal): F[Option[Signed[ShardCheckpoint]]]

  /** Pull the checkpoint with this canonical hash from a peer. Used by the orphan trigger (T1) — a received checkpoint whose
    * `parentCheckpointHash` is absent locally pulls the missing parent.
    */
  def fetchByHash(shardId: ShardId, hash: Hash): F[Option[Signed[ShardCheckpoint]]]
}

object ShardCheckpointFetcher {

  def make[F[_]: Async: SecurityProvider](
    client: Client[F],
    clusterStorage: ClusterStorage[F],
    pullDedupCooldownMs: Long
  ): F[ShardCheckpointFetcher[F]] =
    Ref.of[F, Map[String, Long]](Map.empty).map { cooldownRef =>
      val logger = Slf4jLogger.getLoggerFromName[F]("ShardCheckpointFetcher")

      // JSON over the authenticated p2p GET — the same `circeEntityCodec` path the global snapshot client uses. The daemon converts the
      // returned `Signed[ShardCheckpoint]` to the wire form before re-feeding, so the accept-path hash matches the gossip path.
      import org.http4s.circe.CirceEntityCodec.circeEntityDecoder

      def fetch(shardId: ShardId, path: String, dedupKey: String): F[Option[Signed[ShardCheckpoint]]] =
        Async[F].realTime.map(_.toMillis).flatMap { now =>
          cooldownRef.modify { m =>
            m.get(dedupKey) match {
              case Some(t) if now - t < pullDedupCooldownMs => (m, true) // within cooldown — suppress
              case _                                        => (m.updated(dedupKey, now), false)
            }
          }.flatMap { suppressed =>
            if (suppressed) Async[F].pure(none[Signed[ShardCheckpoint]])
            else
              clusterStorage.getResponsivePeers.flatMap { peers =>
                val peerList = peers.toList
                if (peerList.isEmpty)
                  logger
                    .debug(s"🧩 shard-pull: no responsive peers (shard=${shardId.value.value}, key=$dedupKey)")
                    .as(none[Signed[ShardCheckpoint]])
                else {
                  val peer: Peer = peerList((now % peerList.size.toLong).toInt)
                  // Hit the peer's PUBLIC port: `openRoutes` is response-signed (so `responseVerifierMiddleware`
                  // verifies the peer authored it) but requires no request auth — unlike the p2p port, which
                  // `Peer.toP2PContext` selects and which would reject an un-tokened request. The checkpoint is
                  // self-authenticating anyway (committee sigs + the `evaluate` re-check on re-feed).
                  val ctx = P2PContext(peer.ip, peer.publicPort, peer.id)
                  PeerResponse[F, Signed[ShardCheckpoint]]((uri: Uri) => uri.addPath(path))(client)
                    .run(ctx)
                    .flatMap { signed =>
                      logger
                        .info(
                          s"🧩 shard-pull OK: shard=${shardId.value.value} key=$dedupKey from=${peer.id.value.value.take(8)} " +
                            s"shardOrd=${signed.value.shardOrdinal.value}"
                        )
                        .as(signed.some)
                    }
                    .handleErrorWith { e =>
                      logger
                        .debug(s"🧩 shard-pull failed: shard=${shardId.value.value} key=$dedupKey: ${e.getMessage}")
                        .as(none[Signed[ShardCheckpoint]])
                    }
                }
              }
          }
        }

      new ShardCheckpointFetcher[F] {
        def fetchByOrdinal(shardId: ShardId, ordinal: ShardOrdinal): F[Option[Signed[ShardCheckpoint]]] =
          fetch(
            shardId,
            s"shard-checkpoints/${shardId.value.value}/by-ordinal/${ordinal.value}",
            s"${shardId.value.value}-ord-${ordinal.value}"
          )

        def fetchByHash(shardId: ShardId, hash: Hash): F[Option[Signed[ShardCheckpoint]]] =
          fetch(
            shardId,
            s"shard-checkpoints/${shardId.value.value}/by-hash/${hash.value}",
            s"${shardId.value.value}-hash-${hash.value}"
          )
      }
    }
}
