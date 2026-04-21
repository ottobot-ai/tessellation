package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.std.{Queue, Supervisor}
import cats.effect.syntax.all._
import cats.effect.{Async, Ref, Resource}
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.security.hash.Hash

import fs2.Stream
import io.grpc.ManagedChannel
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** A reactive, back-pressured request channel for ChainSync.
  *
  * ==Why this exists==
  *
  * `ChainSyncManager.requestMissing(hash)` already fires hash-keyed fetches whenever gossip delivers a snapshot whose parent is unknown.
  * The call site has the parent hash (from the child's payload), so a direct fire-and-forget `Async[F].start` with a `Ref[Set[Hash]]` dedup
  * works fine there.
  *
  * The *finality* path is different. When `SnapshotLeaderLoop` tries to walk back to an attested ordinal and finds no hash on its local
  * canonical chain, it has only the ordinal number — no hash, no peer id. It just logs a warning and moves on. That leaves the node on a
  * fork it can't finalize out of until the periodic ChainSync pass happens to notice. We observed ~48s stalls in that state.
  *
  * This queue gives that site (and any future one like it) a cheap, idempotent way to say "someone should go get the chain around ordinal
  * N" without knowing anything about transports or peers. One drainer fiber services the queue, running a pluggable `worker` that
  * translates an ordinal request into concrete network action.
  *
  * ==Narrow by design==
  *
  *   - One request type: `Long` (ordinal). No priorities, no ranges, no per-peer routing. If you reach for those, see "When to extend".
  *   - One shared worker. All ordinals flow through the same `worker` function. No job-type switching.
  *   - Dedup is "currently-pending" (`Ref[Set[Long]]`), not LRU/TTL. A noisy producer firing the same ordinal repeatedly collapses into one
  *     in-flight request; once the worker completes, the next offer is accepted. Good enough for a 5s-tick WARN site. If you need to
  *     suppress re-requests for longer (e.g. "don't retry this ordinal for 30s even after failure"), layer TTL on top — don't widen the
  *     queue.
  *   - Bounded capacity, drop-on-full. A hot producer should not be able to DOS the drainer or balloon memory. Drops are logged so they
  *     show up in diagnostics; the periodic backstop (existing BackfillDaemon / ChainSync sweeps) catches what's dropped.
  *
  * ==Cats Effect primitives in play==
  *
  *   - `cats.effect.std.Queue.bounded` — the transport.
  *   - `Ref[F, Set[Long]]` — in-flight dedup.
  *   - `cats.effect.std.Supervisor` — structured fork of the drainer. `Resource`-scoped so the fiber goes away with the queue.
  *   - `fs2.Stream.fromQueueUnterminated` + `evalMap` — drain loop. `parEvalMap(n)` would raise concurrency if one worker becomes a
  *     bottleneck; for now `evalMap` (serial) is plenty since the worker is network-bound and one request usually triggers a cascade.
  *
  * ==When to extend==
  *
  * Reach for a richer abstraction only once two or more of these become true:
  *
  *   - '''Multiple job types sharing a worker budget.''' E.g. walk-back requests compete with opportunistic backfill for bandwidth and one
  *     should preempt the other. Replace `Long` with a sealed `Job` ADT; swap `Queue.bounded` for a priority structure
  *     (`cats.effect.std.PQueue` if on recent versions, or a `Ref[F, SortedSet[Job]]` polled via `Supervisor`).
  *   - '''Per-job timeouts / retries with backoff.''' Wrap the worker with `retry` policies (`cats-retry`) and compose via `Resource`.
  *   - '''Fan-out to multiple workers with per-peer routing.''' Introduce a `Queue[F, Job]` per peer and a scheduler that chooses which
  *     peer to dispatch to based on liveness. This is a proper ChainSync scheduler; expect ~300–500 LOC.
  *   - '''Observability requirements (lag, throughput metrics).''' Add `Metrics[F]` hooks in the drainer — straightforward but bloats the
  *     trait. Fine to inline for now; factor out when a second queue type needs the same instrumentation.
  *
  * Until then, any of those concerns can be layered on top of this queue as decorators (e.g. a `PriorityChainSyncQueue` that wraps this one
  * and routes into it). Generalize when you have a second concrete use, not before.
  */
trait ChainSyncRequestQueue[F[_]] {

  /** Request that ChainSync attempt to resolve the chain around `ordinal`. Idempotent while a request for the same ordinal is in flight.
    * Returns immediately — work happens on the drainer fiber.
    */
  def request(ordinal: Long): F[Unit]
}

object ChainSyncRequestQueue {

  /** Build a queue with a pluggable worker. The worker is responsible for one ordinal request — typically translating it into network
    * action (e.g. `FindIntersection` + `FetchSnapshots`, or `GetPeerTip` + hash-keyed `ChainSyncManager.requestMissing`).
    *
    * Errors from the worker are logged and swallowed — a failing worker should not take down the drainer fiber. If a request needs to
    * signal completion back to the caller, the current shape doesn't support that (intentionally — this is fire-and-forget). Use a
    * `Deferred`-per-request side-channel if a follow-up ever needs it.
    *
    * @param worker
    *   invoked with the ordinal for each dequeued request; completion is awaited before the next item is pulled.
    * @param capacity
    *   queue capacity. 32 handles bursts of fork-finality warnings across the confirmation window without growing unbounded; full queue →
    *   oldest-semantics drop at the producer.
    */
  def make[F[_]: Async](
    worker: Long => F[Unit],
    capacity: Int = 32
  ): Resource[F, ChainSyncRequestQueue[F]] =
    Supervisor[F](await = false).evalMap { supervisor =>
      for {
        queue <- Queue.bounded[F, Long](capacity)
        inflight <- Ref.of[F, Set[Long]](Set.empty)
        logger = Slf4jLogger.getLoggerFromName[F]("ChainSyncRequestQueue")

        // The drainer. Runs serially on Supervisor-managed fiber; when the Resource closes the fiber is cancelled cleanly.
        // `evalMap` (not `parEvalMap`) — serial processing is fine because the worker is I/O bound and most completions cascade
        // into the existing hash-keyed ChainSyncManager dedup, so adding worker concurrency here doesn't obviously help.
        _ <- supervisor.supervise(
          Stream
            .fromQueueUnterminated(queue)
            .evalMap { ord =>
              worker(ord)
                .handleErrorWith(e => logger.warn(e)(s"ChainSyncRequestQueue worker failed for ordinal=$ord"))
                // Always remove from the in-flight set — failures should not permanently block re-requests for the same ordinal.
                .guarantee(inflight.update(_ - ord))
            }
            .compile
            .drain
        )
      } yield
        new ChainSyncRequestQueue[F] {
          def request(ordinal: Long): F[Unit] =
            // Atomic "mark in-flight if not already" — the returned boolean tells us whether to enqueue. This deduplicates concurrent
            // callers firing the same ordinal; only the first one through modifies the set and goes on to offer.
            inflight.modify { s =>
              if (s.contains(ordinal)) (s, false)
              else (s + ordinal, true)
            }.flatMap {
              case false => Async[F].unit
              case true  =>
                // tryOffer — never block the producer. The producer's call site (e.g. the 5s finality-monitor tick) is in the hot path.
                // A full queue means the drainer is already busy; periodic backstop sync will handle it on the next tick.
                queue.tryOffer(ordinal).flatMap {
                  case true  => Async[F].unit
                  case false =>
                    // Back out of the in-flight set so a future offer can succeed once the drainer drains down.
                    inflight.update(_ - ordinal) >>
                      logger.debug(s"ChainSyncRequestQueue full (capacity=$capacity), dropping request for ordinal=$ordinal")
                }
            }
        }
    }

  /** No-op instance. Use for tests and for layers that don't have a sidecar wired up. */
  def noop[F[_]: Async]: ChainSyncRequestQueue[F] = new ChainSyncRequestQueue[F] {
    def request(ordinal: Long): F[Unit] = Async[F].unit
  }

  /** Default worker for the finality-walkback case.
    *
    * Strategy: ask a peer for its tip via `GetPeerTip`. If the peer's tip is at-or-past the ordinal we're asking about, hand the peer's tip
    * hash to `ChainSyncManager.requestMissing` — which triggers the existing hash-keyed fetch. When the tip snapshot arrives at
    * `NakamotoSyncDaemon.handleSnapshot` it will see its parent missing and cascade-fetch the gap via the same mechanism. So one ordinal
    * request → one tip fetch → gap filled via existing machinery. No new protocol needed.
    *
    * Caveats this worker does *not* address (leave for later / richer scheduler):
    *   - No peer selection. Uses whichever peer the sidecar returns from `GetPeerTip`. If peers disagree, we pick whoever answers first.
    *   - No bound on how large a gap to attempt. If peer tip is 10_000 ahead, this still triggers. In practice `NakamotoSyncDaemon` has its
    *     own tiering (Tier 1/2/3) that caps walk-back depth and falls back to `BackfillDaemon`.
    *   - No retry/backoff. If the peer is unresponsive, the worker fails; the next 5s finality-monitor tick re-offers.
    *
    * @param channel
    *   gRPC channel to the local sidecar (same one `ChainSyncManager` uses).
    * @param chainSyncManager
    *   existing hash-keyed fetcher; we piggyback on its dedup and fetch machinery.
    */
  def walkbackWorker[F[_]: Async](
    channel: ManagedChannel,
    chainSyncManager: ChainSyncManager.ChainSyncManagerAlgebra[F]
  ): Long => F[Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ChainSyncRequestQueue.worker")
    val stub = pb.ChainSyncOutboundGrpc.blockingStub(channel)

    (ordinal: Long) =>
      Async[F]
        .blocking(stub.getPeerTip(pb.GetPeerTipRequest()))
        .flatMap { tip =>
          val peerOrdinal = tip.ordinal
          if (peerOrdinal < ordinal) {
            // Peer is behind us — nothing to pull. The gap is upstream or we're already ahead; let BackfillDaemon / periodic sync handle it.
            logger.debug(s"walkback worker: peer tip ordinal=$peerOrdinal < requested=$ordinal; skipping")
          } else {
            // Wire format is 64-byte ASCII hex (Hash.value is a hex string). Every other decode
            // site in this codebase passes StandardCharsets.UTF_8 explicitly; match that so a
            // non-UTF-8 file.encoding on the JVM can never silently corrupt the hash.
            val peerTipHash = Hash(new String(tip.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
            logger.info(
              s"walkback worker: requesting peer tip hash=${peerTipHash.value.take(12)} ordinal=$peerOrdinal for requested ordinal=$ordinal"
            ) >> chainSyncManager.requestMissing(peerTipHash)
          }
        }
  }
}
