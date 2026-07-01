package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import weaver.SimpleIOSuite

/** Track-3 S1 unit tests for [[SettledOrdinalTracker]] and its coupling to the `T_depth2` archival sink.
  *
  * The settled marker must: (1) read `SnapshotOrdinal.MinValue` at cold start; (2) be MONOTONE — a lower/equal `markSettled` is a no-op;
  * (3) equal `bestTip − k₂` once `bestTip > k₂` (the value the `T_depth2` sink feeds it); and (4) advance ONLY through the sink value — a
  * spurious lower write cannot regress it. G1 (enforced structurally by the trait, not tested here): it is its OWN ref, never the k₁
  * `nakamotoFinalizedOrdinalRef`.
  */
object SettledOrdinalTrackerSuite extends SimpleIOSuite {

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal.unsafeApply(n)
  private def pid(name: String): PeerId = PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))
  private def hash(s: String): Hash = Hash(s.padTo(64, '0'))

  // A minimal `ConsensusState` — only `bestTipOrdinal` matters for `TDepth2Trigger`.
  private def state(bestOrd: Long): FinalityTrigger.ConsensusState[IO] =
    FinalityTrigger.ConsensusState[IO](
      selfId = pid("self"),
      bestTipOrdinal = ord(bestOrd),
      bestTipHash = hash("tip"),
      canonicalHashAt = (_: Long) => IO.pure(None)
    )

  test("cold start: settledOrdinal is MinValue") {
    for {
      tracker <- SettledOrdinalTracker.make[IO]
      settled <- tracker.settledOrdinal
    } yield expect.same(SnapshotOrdinal.MinValue, settled)
  }

  test("markSettled advances to a strictly greater ordinal") {
    for {
      tracker <- SettledOrdinalTracker.make[IO]
      _ <- tracker.markSettled(ord(100L))
      a <- tracker.settledOrdinal
      _ <- tracker.markSettled(ord(250L))
      b <- tracker.settledOrdinal
    } yield expect.same(ord(100L), a) && expect.same(ord(250L), b)
  }

  test("markSettled is monotone: a lower or equal ordinal is a no-op") {
    for {
      tracker <- SettledOrdinalTracker.make[IO]
      _ <- tracker.markSettled(ord(500L))
      _ <- tracker.markSettled(ord(499L)) // regress attempt — ignored
      _ <- tracker.markSettled(ord(500L)) // equal — ignored
      settled <- tracker.settledOrdinal
    } yield expect.same(ord(500L), settled)
  }

  test("settled marker equals bestTip − k₂ once bestTip > k₂ (fed from the T_depth2 sink)") {
    // Dev k₂ = 100·k₁ = 3200. bestTip 10000 > k₂ ⇒ qualifying = 10000 − 3200 = 6800.
    val k2 = 3200L
    for {
      tracker <- SettledOrdinalTracker.make[IO]
      tDepth2 <- TDepth2Trigger.make[IO](k2)
      qualifying <- tDepth2.evaluateAndAdvance(state(10000L))
      _ <- tracker.markSettled(qualifying) // the sink write
      settled <- tracker.settledOrdinal
    } yield expect.same(ord(6800L), qualifying) && expect.same(ord(6800L), settled)
  }

  test("advances only through the T_depth2 sink value; a spurious lower write cannot regress it") {
    val k2 = 3200L
    for {
      tracker <- SettledOrdinalTracker.make[IO]
      tDepth2 <- TDepth2Trigger.make[IO](k2)
      // The sink advances as bestTip grows: 6800 then 7800.
      q1 <- tDepth2.evaluateAndAdvance(state(10000L))
      _ <- tracker.markSettled(q1)
      q2 <- tDepth2.evaluateAndAdvance(state(11000L))
      _ <- tracker.markSettled(q2)
      afterAdvance <- tracker.settledOrdinal
      // A spurious non-sink write below the marker is ignored (monotone).
      _ <- tracker.markSettled(ord(1L))
      afterSpurious <- tracker.settledOrdinal
    } yield
      expect.same(ord(6800L), q1) &&
        expect.same(ord(7800L), q2) &&
        expect.same(ord(7800L), afterAdvance) &&
        expect.same(ord(7800L), afterSpurious)
  }

  test("below k₂ the sink yields MinValue, so the marker stays at cold start") {
    val k2 = 3200L
    for {
      tracker <- SettledOrdinalTracker.make[IO]
      tDepth2 <- TDepth2Trigger.make[IO](k2)
      qualifying <- tDepth2.evaluateAndAdvance(state(100L)) // bestTip 100 < k₂
      _ <- tracker.markSettled(qualifying)
      settled <- tracker.settledOrdinal
    } yield expect.same(SnapshotOrdinal.MinValue, qualifying) && expect.same(SnapshotOrdinal.MinValue, settled)
  }
}
