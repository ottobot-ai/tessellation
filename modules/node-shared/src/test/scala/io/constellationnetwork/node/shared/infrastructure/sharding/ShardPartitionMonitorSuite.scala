package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.effect.{IO, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.metrics.{CountingMetrics, Metrics}
import io.constellationnetwork.schema.sharding.ShardId

import weaver.SimpleIOSuite

/** Tests for [[ShardPartitionMonitor]] — Slice 19 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §13 row 19 + §9.4.
  *
  * '''Required coverage''' (per slice spec):
  *   1. WARN + counter fires when a shard goes longer than `tPartitionHardMs` in T_depth1-only mode (no `T_count` fire)
  *   1. WARN + counter does NOT fire on a healthy shard (regular `T_count` fires keep the timestamp fresh)
  *   1. Suppression — once a WARN fires for shard `s`, subsequent ticks within `tPartitionHardMs` do not re-fire
  *   1. `notifyTDepth1` on a brand-new shard seeds the timestamp at `now` (so we don't WARN immediately on bootstrap)
  *
  * '''Test clock''': all tests use a `Ref[IO, Long]`-backed clock so we can step time deterministically. Production wiring passes
  * `Async[IO].realTime.map(_.toMillis)`.
  */
object ShardPartitionMonitorSuite extends SimpleIOSuite {

  // Convenience: build a clock+monitor pair backed by a Ref so tests can step time.
  private def mkMonitor(
    tPartitionHardMs: Long
  ): IO[(Ref[IO, Long], Ref[IO, CountingMetrics.State], ShardPartitionMonitor[IO])] =
    for {
      clockRef <- Ref.of[IO, Long](0L)
      pair <- CountingMetrics.makeWithState
      (stateRef, m) = pair
      implicit0(metrics: Metrics[IO]) = m
      monitor <- ShardPartitionMonitor.make[IO](tPartitionHardMs, clockRef.get)
    } yield (clockRef, stateRef, monitor)

  private val s0: ShardId = ShardId.unsafeApply(0)
  private val s1: ShardId = ShardId.unsafeApply(1)

  // ============================================================================
  // Test 1: WARN + counter fires when shard exceeds threshold
  // ============================================================================

  test("partition-hard fires: T_depth1-only mode for > tPartitionHardMs → WARN + partition_hard_total counter") {
    for {
      (clock, stateRef, monitor) <- mkMonitor(tPartitionHardMs = 100L)

      // Bootstrap: shard fires one T_count at t=0, then nothing else but T_depth1 fallbacks.
      _ <- monitor.notifyTCount(s0)
      _ <- clock.set(50L) // 50ms in — not yet stale.
      tick1 <- monitor.check
      stateMid <- stateRef.get

      _ <- clock.set(250L) // 250ms in — well past 100ms threshold.
      tick2 <- monitor.check
      stateAfter <- stateRef.get
    } yield
      expect(tick1.isEmpty) &&
        expect(stateMid.counters.getOrElse(ShardMetrics.PartitionHardTotal.value, 0) == 0) &&
        expect.same(Set(s0), tick2) &&
        expect(stateAfter.counters.getOrElse(ShardMetrics.PartitionHardTotal.value, 0) == 1)
  }

  // ============================================================================
  // Test 2: Healthy shard does not fire
  // ============================================================================

  test("partition-hard does NOT fire on healthy shard: regular T_count keeps timestamp fresh") {
    for {
      (clock, stateRef, monitor) <- mkMonitor(tPartitionHardMs = 100L)

      _ <- monitor.notifyTCount(s0)
      _ <- clock.set(50L)
      _ <- monitor.notifyTCount(s0) // refresh BEFORE threshold elapses
      _ <- clock.set(120L) // 70ms since last T_count — still under 100ms threshold
      tick <- monitor.check
      state <- stateRef.get
    } yield
      expect(tick.isEmpty) &&
        expect(state.counters.getOrElse(ShardMetrics.PartitionHardTotal.value, 0) == 0)
  }

  // ============================================================================
  // Test 3: Suppression after fire
  // ============================================================================

  test("suppression: after a WARN fires, the next WARN waits another full tPartitionHardMs") {
    for {
      (clock, stateRef, monitor) <- mkMonitor(tPartitionHardMs = 100L)

      _ <- monitor.notifyTCount(s0)
      _ <- clock.set(200L) // first WARN
      tick1 <- monitor.check

      _ <- clock.set(250L) // 50ms after first WARN — should be suppressed
      tick2 <- monitor.check

      _ <- clock.set(400L) // 200ms after first WARN — should fire again
      tick3 <- monitor.check

      state <- stateRef.get
    } yield
      expect.same(Set(s0), tick1) &&
        expect(tick2.isEmpty) &&
        expect.same(Set(s0), tick3) &&
        expect(state.counters.getOrElse(ShardMetrics.PartitionHardTotal.value, 0) == 2)
  }

  // ============================================================================
  // Test 4: Multi-shard isolation
  // ============================================================================

  test("multi-shard: only stale shards fire; healthy shard untouched") {
    for {
      (clock, stateRef, monitor) <- mkMonitor(tPartitionHardMs = 100L)

      _ <- monitor.notifyTCount(s0)
      _ <- monitor.notifyTCount(s1)

      _ <- clock.set(150L) // s1 refreshes well after s0's last fire
      _ <- monitor.notifyTCount(s1) // s1 stays healthy

      _ <- clock.set(200L) // s0 stale (200ms since last T_count); s1 fresh (50ms since last)
      tick <- monitor.check
      state <- stateRef.get
    } yield
      expect.same(Set(s0), tick) &&
        expect(state.counters.getOrElse(ShardMetrics.PartitionHardTotal.value, 0) == 1)
  }

  // ============================================================================
  // Test 5: notifyTDepth1 on first-ever activity seeds the timestamp
  // ============================================================================

  test("notifyTDepth1 on unseen shard seeds timestamp — no immediate WARN on bootstrap") {
    for {
      (clock, stateRef, monitor) <- mkMonitor(tPartitionHardMs = 100L)

      _ <- clock.set(1000L) // bootstrap at non-zero time
      _ <- monitor.notifyTDepth1(s0) // first activity is a depth fallback
      _ <- clock.set(1050L) // 50ms in — under threshold
      tick1 <- monitor.check

      _ <- clock.set(1200L) // 200ms after seed — past threshold
      tick2 <- monitor.check

      state <- stateRef.get
    } yield
      expect(tick1.isEmpty) &&
        expect.same(Set(s0), tick2) &&
        expect(state.counters.getOrElse(ShardMetrics.PartitionHardTotal.value, 0) == 1)
  }

  // ============================================================================
  // Test 6: notifyTDepth1 does NOT reset existing T_count timestamp
  // ============================================================================

  test("notifyTDepth1 on previously-T_count'd shard does NOT refresh the staleness clock") {
    for {
      (clock, stateRef, monitor) <- mkMonitor(tPartitionHardMs = 100L)

      _ <- monitor.notifyTCount(s0) // at t=0
      _ <- clock.set(50L)
      _ <- monitor.notifyTDepth1(s0) // depth-fallback is NOT health evidence
      _ <- clock.set(150L) // 150ms since last T_count → stale
      tick <- monitor.check
      state <- stateRef.get
    } yield
      expect.same(Set(s0), tick) &&
        expect(state.counters.getOrElse(ShardMetrics.PartitionHardTotal.value, 0) == 1)
  }
}
