package io.constellationnetwork.node.shared.modules

import cats.effect.Concurrent
import cats.effect.std.Queue
import cats.syntax.functor._

import io.constellationnetwork.schema.gossip.RumorRaw
import io.constellationnetwork.security.Hashed

object SharedQueues {

  /** Hard cap on buffered, not-yet-validated gossip rumors. 2026-06-10 (run b6zx4w20b): the queue was `unbounded`, and under the
    * spend/double-spend gossip firehose the single-fiber `GossipDaemon.consumeRumors` drain fell behind — each queued `Hashed[RumorRaw]`
    * pins a `PeerRumorRaw.content: io.circe.Json` tree, so the backlog grew to 456M `Json$JNumber` instances (7.3 GB) on every NON-LEADER
    * gl0 node (the leader produces rather than receives, so it stayed at 90 MB — the 80x heap asymmetry), driving 4/5 nodes into G1
    * death-thrash. Bounding turns catastrophic unbounded growth into backpressure (the `.offer` sites — ConsensusRoutes,
    * SidecarRumorBridge, Gossip.spread — block until the parallelized consumer drains). Gossip is round-replayed, so a momentarily-full
    * queue is self-healing. Sized so the worst-case pinned-Json footprint stays well under the gl0 heap (16384 × ~30 KB ≈ 0.5 GB). Paired
    * with the `parEvalMap` validation fan-out in GossipDaemon.
    */
  val DefaultRumorQueueCap: Int = 16384

  def make[F[_]: Concurrent](rumorQueueCap: Int = DefaultRumorQueueCap): F[SharedQueues[F]] =
    for {
      rumorQueue <- Queue.bounded[F, Hashed[RumorRaw]](rumorQueueCap)
    } yield
      new SharedQueues[F] {
        val rumor = rumorQueue
      }
}

sealed abstract class SharedQueues[F[_]] private {
  val rumor: Queue[F, Hashed[RumorRaw]]
}
