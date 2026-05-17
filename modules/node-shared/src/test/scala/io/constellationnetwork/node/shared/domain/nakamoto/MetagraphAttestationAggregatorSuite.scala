package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO
import cats.syntax.all._

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import weaver.SimpleIOSuite

/** Tests for [[MetagraphAttestationAggregator]] — the per-metagraph committee attestation tally
  * (Slice S2.5).
  *
  * The aggregator is verification-agnostic by contract (callers pre-verify); these tests assert
  * the bookkeeping: idempotent record, per-binary isolation, threshold semantics that match
  * `TipTracker.FinalityThreshold`, and `pruneBelow` correctness.
  */
object MetagraphAttestationAggregatorSuite extends SimpleIOSuite {

  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  private def addr(tag: String): Address =
    Address.fromBytes(tag.getBytes("UTF-8"))

  private def hash(tag: String): Hash =
    Hash((tag + "0" * 64).take(64))

  // ============ requiredCount semantics ============

  test("requiredCount uses ceil(2/3 · K) — matches TipTracker.FinalityThreshold parity") {
    // Default threshold = 2/3. K=3 → ceil(2) = 2; K=6 → ceil(4) = 4; K=7 → ceil(14/3) = 5;
    // K=100 → ceil(200/3) = 67; K=400 → ceil(800/3) = 267.
    IO.pure {
      expect(MetagraphAttestationAggregator.requiredCount(3) == 2)
        .and(expect(MetagraphAttestationAggregator.requiredCount(6) == 4))
        .and(expect(MetagraphAttestationAggregator.requiredCount(7) == 5))
        .and(expect(MetagraphAttestationAggregator.requiredCount(100) == 67))
        .and(expect(MetagraphAttestationAggregator.requiredCount(400) == 267))
    }
  }

  test("requiredCount rejects kTarget=0") {
    IO.delay {
      val caught =
        try { MetagraphAttestationAggregator.requiredCount(0); false }
        catch { case _: IllegalArgumentException => true }
      expect(caught)
    }
  }

  // ============ record + idempotency ============

  test("record returns post-record count; same peer re-recorded is idempotent") {
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      c1 <- agg.record(addr("mg-A"), 10L, hash("bin-1"), pid("peer-1"))
      c2 <- agg.record(addr("mg-A"), 10L, hash("bin-1"), pid("peer-2"))
      c3 <- agg.record(addr("mg-A"), 10L, hash("bin-1"), pid("peer-1")) // duplicate
    } yield expect(c1 == 1).and(expect(c2 == 2)).and(expect(c3 == 2))
  }

  // ============ per-binary isolation ============

  test("different (metagraph, ord, hash) keys tally independently") {
    val a = addr("mg-A")
    val b = addr("mg-B")
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- agg.record(a, 10L, hash("bin-1"), pid("p1"))
      _ <- agg.record(a, 10L, hash("bin-1"), pid("p2"))
      _ <- agg.record(a, 10L, hash("bin-2"), pid("p1")) // same metagraph + ord, different hash
      _ <- agg.record(b, 10L, hash("bin-1"), pid("p1")) // different metagraph
      _ <- agg.record(a, 11L, hash("bin-1"), pid("p1")) // different ord
      cA10b1 <- agg.countFor(a, 10L, hash("bin-1"))
      cA10b2 <- agg.countFor(a, 10L, hash("bin-2"))
      cB10b1 <- agg.countFor(b, 10L, hash("bin-1"))
      cA11b1 <- agg.countFor(a, 11L, hash("bin-1"))
    } yield
      expect(cA10b1 == 2)
        .and(expect(cA10b2 == 1))
        .and(expect(cB10b1 == 1))
        .and(expect(cA11b1 == 1))
  }

  // ============ thresholdReached ============

  test("thresholdReached fires only at ceil(2K/3) attestations") {
    val a = addr("mg-A")
    val ord = 100L
    val h = hash("bin")
    val k = 6 // ceil(2·6/3) = 4
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- (1 to 3).toList.traverse_(i => agg.record(a, ord, h, pid(s"p$i")))
      below <- agg.thresholdReached(a, ord, h, k)
      _ <- agg.record(a, ord, h, pid("p4"))
      atThreshold <- agg.thresholdReached(a, ord, h, k)
    } yield expect(!below).and(expect(atThreshold))
  }

  test("thresholdReached false when binary unknown") {
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      reached <- agg.thresholdReached(addr("mg-A"), 1L, hash("none"), 6)
    } yield expect(!reached)
  }

  // ============ pruneBelow ============

  test("pruneBelow drops snapshots strictly below keepFromOrd, leaves others") {
    val a = addr("mg-A")
    val b = addr("mg-B")
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- agg.record(a, 5L, hash("bin-old"), pid("p1"))
      _ <- agg.record(a, 10L, hash("bin-new"), pid("p1"))
      _ <- agg.record(b, 5L, hash("bin-other"), pid("p1")) // different metagraph, untouched
      _ <- agg.pruneBelow(a, 10L)
      cAOld <- agg.countFor(a, 5L, hash("bin-old"))
      cANew <- agg.countFor(a, 10L, hash("bin-new"))
      cBOld <- agg.countFor(b, 5L, hash("bin-other"))
    } yield
      expect(cAOld == 0)
        .and(expect(cANew == 1))
        .and(expect(cBOld == 1))
  }

  test("pruneBelow that empties a metagraph drops the outer-map entry") {
    val a = addr("mg-A")
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- agg.record(a, 5L, hash("bin-1"), pid("p1"))
      sizeBefore <- agg.size
      _ <- agg.pruneBelow(a, 10L)
      sizeAfter <- agg.size
    } yield expect(sizeBefore == 1).and(expect(sizeAfter == 0))
  }

  test("pruneBelow on absent metagraph is a no-op") {
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- agg.pruneBelow(addr("mg-absent"), 100L)
      n <- agg.size
    } yield expect(n == 0)
  }

  // ============ size ============

  test("size counts distinct (metagraph, ord, hash) keys, not attestations") {
    val a = addr("mg-A")
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- agg.record(a, 1L, hash("bin-1"), pid("p1"))
      _ <- agg.record(a, 1L, hash("bin-1"), pid("p2")) // same key, more peers
      _ <- agg.record(a, 2L, hash("bin-2"), pid("p1")) // new key
      n <- agg.size
    } yield expect(n == 2)
  }
}
