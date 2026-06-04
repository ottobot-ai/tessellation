package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO
import cats.syntax.all._

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import weaver.SimpleIOSuite

/** Tests for [[MetagraphAttestationAggregator]] — the per-metagraph committee attestation tally (Slice S2.5).
  *
  * The aggregator is verification-agnostic by contract (callers pre-verify); these tests assert the bookkeeping: idempotent record,
  * per-binary isolation, threshold semantics that match `TipTracker.FinalityThreshold`, and `pruneParents` correctness.
  */
object MetagraphAttestationAggregatorSuite extends SimpleIOSuite {

  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  private def addr(tag: String): Address =
    Address.fromBytes(tag.getBytes("UTF-8"))

  private def parent(tag: String): Hash =
    Hash.fromBytes(tag.getBytes("UTF-8"))

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
      c1 <- agg.record(addr("mg-A"), parent("p1"), hash("bin-1"), pid("peer-1"))
      c2 <- agg.record(addr("mg-A"), parent("p1"), hash("bin-1"), pid("peer-2"))
      c3 <- agg.record(addr("mg-A"), parent("p1"), hash("bin-1"), pid("peer-1")) // duplicate
    } yield expect(c1 == 1).and(expect(c2 == 2)).and(expect(c3 == 2))
  }

  // ============ per-binary isolation ============

  test("different (metagraph, parent, hash) keys tally independently") {
    val a = addr("mg-A")
    val b = addr("mg-B")
    val p1 = parent("p1")
    val p2 = parent("p2")
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- agg.record(a, p1, hash("bin-1"), pid("p1"))
      _ <- agg.record(a, p1, hash("bin-1"), pid("p2"))
      _ <- agg.record(a, p1, hash("bin-2"), pid("p1")) // same metagraph + parent, different binary (equivocation surface)
      _ <- agg.record(b, p1, hash("bin-1"), pid("p1")) // different metagraph
      _ <- agg.record(a, p2, hash("bin-1"), pid("p1")) // different parent
      cAp1b1 <- agg.countFor(a, p1, hash("bin-1"))
      cAp1b2 <- agg.countFor(a, p1, hash("bin-2"))
      cBp1b1 <- agg.countFor(b, p1, hash("bin-1"))
      cAp2b1 <- agg.countFor(a, p2, hash("bin-1"))
    } yield
      expect(cAp1b1 == 2)
        .and(expect(cAp1b2 == 1))
        .and(expect(cBp1b1 == 1))
        .and(expect(cAp2b1 == 1))
  }

  // ============ thresholdReached ============

  // Draw/quorum decouple: `thresholdReached(requiredQuorum)` now compares the distinct-attester count against `requiredQuorum` DIRECTLY
  // (the cluster-uniform `nakamoto.committee.kQuorum`), NOT `ceil(2/3 · K)` of some draw target.
  test("thresholdReached fires exactly at requiredQuorum attestations (direct count)") {
    val a = addr("mg-A")
    val p = parent("p-threshold")
    val h = hash("bin")
    val requiredQuorum = 4 // wait for 4 distinct attesters, directly
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- (1 to 3).toList.traverse_(i => agg.record(a, p, h, pid(s"p$i")))
      below <- agg.thresholdReached(a, p, h, requiredQuorum)
      _ <- agg.record(a, p, h, pid("p4"))
      atThreshold <- agg.thresholdReached(a, p, h, requiredQuorum)
    } yield expect(!below).and(expect(atThreshold))
  }

  test("thresholdReached rejects requiredQuorum = 0") {
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      caught <- IO.delay {
        try { agg.thresholdReached(addr("mg-A"), parent("p"), hash("bin"), 0); false }
        catch { case _: IllegalArgumentException => true }
      }
    } yield expect(caught)
  }

  test("thresholdReached false when binary unknown") {
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      reached <- agg.thresholdReached(addr("mg-A"), parent("unknown"), hash("none"), 6)
    } yield expect(!reached)
  }

  // ============ pruneParents ============

  test("pruneParents drops entries for finalized parents, leaves others") {
    val a = addr("mg-A")
    val b = addr("mg-B")
    val pOld = parent("p-old")
    val pNew = parent("p-new")
    val pOther = parent("p-other")
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- agg.record(a, pOld, hash("bin-old"), pid("p1"))
      _ <- agg.record(a, pNew, hash("bin-new"), pid("p1"))
      _ <- agg.record(b, pOther, hash("bin-other"), pid("p1")) // different metagraph, untouched
      _ <- agg.pruneParents(a, Set(pOld))
      cAOld <- agg.countFor(a, pOld, hash("bin-old"))
      cANew <- agg.countFor(a, pNew, hash("bin-new"))
      cBOther <- agg.countFor(b, pOther, hash("bin-other"))
    } yield
      expect(cAOld == 0)
        .and(expect(cANew == 1))
        .and(expect(cBOther == 1))
  }

  test("pruneParents that empties a metagraph drops the outer-map entry") {
    val a = addr("mg-A")
    val p = parent("p-only")
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- agg.record(a, p, hash("bin-1"), pid("p1"))
      sizeBefore <- agg.size
      _ <- agg.pruneParents(a, Set(p))
      sizeAfter <- agg.size
    } yield expect(sizeBefore == 1).and(expect(sizeAfter == 0))
  }

  test("pruneParents on absent metagraph is a no-op") {
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- agg.pruneParents(addr("mg-absent"), Set(parent("p")))
      n <- agg.size
    } yield expect(n == 0)
  }

  test("pruneParents with empty set is a no-op") {
    val a = addr("mg-A")
    val p = parent("p")
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- agg.record(a, p, hash("bin-1"), pid("p1"))
      _ <- agg.pruneParents(a, Set.empty)
      n <- agg.countFor(a, p, hash("bin-1"))
    } yield expect(n == 1)
  }

  // ============ size ============

  test("size counts distinct (metagraph, parent, hash) keys, not attestations") {
    val a = addr("mg-A")
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      _ <- agg.record(a, parent("p1"), hash("bin-1"), pid("p1"))
      _ <- agg.record(a, parent("p1"), hash("bin-1"), pid("p2")) // same key, more peers
      _ <- agg.record(a, parent("p2"), hash("bin-2"), pid("p1")) // new key
      n <- agg.size
    } yield expect(n == 2)
  }
}
