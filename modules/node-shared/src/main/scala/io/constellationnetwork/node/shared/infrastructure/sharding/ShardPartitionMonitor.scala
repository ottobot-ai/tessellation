package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.sharding.ShardId

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** "Hard-partition" observability monitor — Slice 19 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §13 row 19 + §9.4.
  *
  * Tracks the last time each shard fired a `T_count_shard` (full quorum) checkpoint acceptance. If a shard goes longer than
  * `tPartitionHardMs` without ANY `T_count` fire — meaning every accepted checkpoint in that window took the `T_depth1_shard` fallback path
  * — the gl0 leader logs a `SHARD-PARTITION-SUSPECT` WARN and increments `dag_nakamoto_shard_partition_hard_total{shard_id}`. Operator
  * intervention is expected; per design-doc §9.4 v1 does not attempt automatic emergency rotation. v2 may.
  *
  * '''Why a stand-alone service.''' Pulled out of `GlobalSnapshotConsensus` so the partition-detection logic is unit-testable in isolation
  * without spinning up the gl0 leader's full state. Wiring into `GlobalSnapshotConsensus` is then a one-line `notifyTCount` /
  * `notifyTDepth1` call per checkpoint admission outcome (Slice 13+'s GSAM rewire wires this).
  *
  * '''Suppression after WARN.''' Once a shard fires the partition-hard WARN, the monitor records the WARN's timestamp as the new "last
  * T_count" baseline so the next WARN waits another full `tPartitionHardMs` (rather than firing every gl0 tick while the partition
  * persists). The WARN-flood prevention is the same pattern as the consensus-stall WARN in
  * `[[io.constellationnetwork.node.shared.infrastructure.consensus.engine.StallDetector]]`.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`):
  *   - Fresh service; no compat ceremony.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`):
  *   - `tPartitionHardMs` is a constructor parameter; callers wire from `cfg.nakamoto.sharding.observability.tPartitionHardMs`. No
  *     `sys.env.get` anywhere.
  */
trait ShardPartitionMonitor[F[_]] {

  /** Record that shard `shardId` accepted a checkpoint via the `T_count_shard` (full quorum) path. Resets the per-shard "last T_count fire"
    * timestamp to "now". After this call, the next WARN for this shard waits at least `tPartitionHardMs` ms.
    */
  def notifyTCount(shardId: ShardId): F[Unit]

  /** Record that shard `shardId` accepted a checkpoint via the `T_depth1_shard` fallback path. Does NOT update the per-shard "last T_count
    * fire" timestamp — the only signal that the shard is healthy is the T_count path. Depth-fallback only is precisely the condition we
    * want to detect on.
    */
  def notifyTDepth1(shardId: ShardId): F[Unit]

  /** Tick the monitor against the per-shard "last T_count fire" timestamps. For each tracked shard, if `(now - lastTCount) >
    * tPartitionHardMs`, the monitor:
    *   - logs a `SHARD-PARTITION-SUSPECT` WARN
    *   - increments `dag_nakamoto_shard_partition_hard_total{shard_id}`
    *   - resets the per-shard timestamp to `now` so the next WARN waits another full `tPartitionHardMs`
    *
    * Callers wire this from the gl0 leader's per-ord loop (Slice 13+'s GSAM rewire). Tests can drive it directly with a `clock` callback
    * that returns the desired test "now" — see the suite.
    *
    * Returns the set of shard-ids that fired a WARN this tick. Diagnostic; production callers don't branch on it.
    */
  def check: F[Set[ShardId]]

  /** Diagnostic — read the per-shard "last T_count fire timestamp" map (millis since epoch). Production callers SHOULD NOT use this for
    * control flow; use [[check]] which applies the suppression semantics consistently.
    */
  def state: F[Map[ShardId, Long]]
}

object ShardPartitionMonitor {

  /** Construct a partition monitor.
    *
    * @param tPartitionHardMs
    *   threshold in milliseconds. If no `T_count_shard` fire has been recorded for `> tPartitionHardMs` ms, the next [[check]] for that
    *   shard fires the WARN + counter increment. Wire from `sharedConfig.nakamoto.sharding.observability.tPartitionHardMs`.
    * @param clock
    *   `F[Long]` returning the current epoch-millis time. Production wiring passes `Async[F].realTime.map(_.toMillis)`; tests pass a
    *   Ref-backed stub so they can step time deterministically. We don't bind to `Async[F].realTime` here so a Sync-only constraint plus a
    *   test clock is sufficient — and the monitor compiles in any `F` that supports `Ref`.
    */
  def make[F[_]: Async: Metrics](
    tPartitionHardMs: Long,
    clock: F[Long]
  ): F[ShardPartitionMonitor[F]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ShardPartitionMonitor")

    Ref.of[F, Map[ShardId, Long]](Map.empty).map { lastTCountRef =>
      new ShardPartitionMonitor[F] {

        def notifyTCount(shardId: ShardId): F[Unit] =
          clock.flatMap { now =>
            lastTCountRef.update(_.updated(shardId, now))
          }

        def notifyTDepth1(shardId: ShardId): F[Unit] =
          // First-ever fallback for a shard we've never seen a T_count from: seed the timestamp at "now" so the partition-hard
          // window starts counting from the first observed activity rather than from epoch=0 (which would WARN immediately on
          // bootstrap). Subsequent fallbacks leave the timestamp untouched — a depth-fallback is NOT evidence of T_count health.
          clock.flatMap { now =>
            lastTCountRef.update { current =>
              if (current.contains(shardId)) current
              else current.updated(shardId, now)
            }
          }

        def check: F[Set[ShardId]] =
          clock.flatMap { now =>
            lastTCountRef.modify { current =>
              // Identify shards whose "last T_count fire" is stale enough to WARN. Reset their timestamps to `now` so the next
              // WARN waits another full `tPartitionHardMs`. Shards that just fired T_count get carried forward unchanged.
              val (stale, fresh) = current.partition { case (_, ts) => now - ts > tPartitionHardMs }
              val staleIds: Set[ShardId] = stale.keySet
              val nextMap: Map[ShardId, Long] = fresh ++ staleIds.map(_ -> now).toMap
              (nextMap, staleIds)
            }.flatMap { staleIds =>
              if (staleIds.isEmpty) Async[F].pure(Set.empty[ShardId])
              else
                staleIds.toList.traverse_ { sid =>
                  logger.warn(
                    s"SHARD-PARTITION-SUSPECT: shardId=$sid in T_depth1-only mode for > ${tPartitionHardMs}ms — " +
                      s"operator intervention may be required"
                  ) >> ShardMetrics.incPartitionHard[F](sid)
                }.as(staleIds)
            }
          }

        def state: F[Map[ShardId, Long]] = lastTCountRef.get
      }
    }
  }
}
