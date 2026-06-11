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
    * queue is self-healing. Sized so the worst-case pinned-Json footprint stays well under the gl0 heap. Paired with the `parEvalMap`
    * validation fan-out in GossipDaemon.
    *
    * 2026-06-10 (run bul74s6xx) RAISED 16384 → 65536: the fix eliminated the OOM (heaps held at ~1.5 GB of the 12 GB cap under load), but
    * 16384 was small enough that a transient host-CPU-contention spike — which starves the Ed25519 consumer — let the queue fill and DROP
    * ~10k inbound rumors (the tryOffer-drop path); one dropped rumor was an allow-spend that then propagated only slowly via gossip-round
    * replay. With heap headroom proven, size the cap to ABSORB such bursts without dropping: 65536 × ~30 KB ≈ 2 GB worst case, well under
    * the 12 GB heap and 6× the observed burst. Drops now signal genuine sustained overload, not a transient spike.
    */
  val DefaultRumorQueueCap: Int = 65536

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
