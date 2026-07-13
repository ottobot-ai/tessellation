package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.metrics.NoOpMetrics
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import weaver.SimpleIOSuite

/** Behavioral tests for the transitional latest-attestation margin implemented by [[SnowballAccumulator]].
  *
  * These tests do not establish Snowball/Avalanche safety. The implementation has no K-peer query loop or alpha-majority cascade, and its
  * sticky first-crossing decision is arrival-order sensitive. Calibration of a separate K/alpha/beta model therefore does not transfer to
  * this code.
  */
object SnowballAccumulatorSuite extends SimpleIOSuite {

  implicit private val metrics: io.constellationnetwork.node.shared.infrastructure.metrics.Metrics[IO] = NoOpMetrics.make

  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  private def hash(s: String): Hash = Hash(s.padTo(64, '0'))

  // ============================================================
  // Executable transitional rule: current leader_count - runner_up_count >= beta
  // ============================================================

  test("decision rule: 0 peers — no decision") {
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      decided <- acc.decidedAt(50L)
    } yield expect.same(None, decided)
  }

  test("decision rule: peer count below β — no decision") {
    val tipH = hash("H")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      _ <- (1 to 9).toList.traverse_(i => acc.recordAttestation(pid(s"peer-$i"), 50L, tipH))
      decided <- acc.decidedAt(50L)
      counts <- acc.accumAt(50L)
    } yield
      // 9 peers attest H → leader_count = 9, runner_up = 0, margin 9 < β=10 → undecided.
      expect.same(None, decided) &&
        expect.same(9, counts.getOrElse(tipH, 0))
  }

  test("decision rule: peer count clears β margin against zero runner-up — decided") {
    val tipH = hash("H")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      _ <- (1 to 10).toList.traverse_(i => acc.recordAttestation(pid(s"peer-$i"), 50L, tipH))
      decided <- acc.decidedAt(50L)
      counts <- acc.accumAt(50L)
    } yield
      // 10 peers attest H → leader_count = 10, runner_up = 0, margin 10 ≥ β=10 → decided H.
      expect.same(Some(tipH), decided) &&
        expect.same(10, counts.getOrElse(tipH, 0))
  }

  test("decision rule: margin (not absolute count) is what gates the decision") {
    // 9 peers on hash A, 1 peer on hash B → leader = 9, runner_up = 1, margin = 8 < β=10 → undecided.
    val tipA = hash("A")
    val tipB = hash("B")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      _ <- (1 to 9).toList.traverse_(i => acc.recordAttestation(pid(s"a-$i"), 50L, tipA))
      _ <- acc.recordAttestation(pid("b-1"), 50L, tipB)
      decided <- acc.decidedAt(50L)
    } yield expect.same(None, decided)
  }

  test("decision rule: leader needs β margin OVER runner-up, not absolute β count") {
    // 15 peers on A, 4 peers on B → leader = 15, runner_up = 4, margin = 11 ≥ β=10 → decided A.
    val tipA = hash("A")
    val tipB = hash("B")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      _ <- (1 to 15).toList.traverse_(i => acc.recordAttestation(pid(s"a-$i"), 50L, tipA))
      _ <- (1 to 4).toList.traverse_(i => acc.recordAttestation(pid(s"b-$i"), 50L, tipB))
      decided <- acc.decidedAt(50L)
    } yield expect.same(Some(tipA), decided)
  }

  // ============================================================
  // Current latest-attestation counting behavior (not Snowball lifetime confidence)
  // ============================================================

  test("current counts retain contributions from different peers on competing hashes") {
    // Five distinct peers currently point at A and five different peers currently point at B.
    // Both current-count entries remain present; this says nothing about lifetime confidence.
    val tipA = hash("A")
    val tipB = hash("B")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 100) // β=100 so neither decides; we just check counts
      _ <- (1 to 5).toList.traverse_(i => acc.recordAttestation(pid(s"a-$i"), 50L, tipA))
      _ <- (1 to 5).toList.traverse_(i => acc.recordAttestation(pid(s"b-$i"), 50L, tipB))
      counts <- acc.accumAt(50L)
    } yield
      // Both sets of current peer contributions are represented.
      expect.same(5, counts.getOrElse(tipA, 0)) &&
        expect.same(5, counts.getOrElse(tipB, 0))
  }

  test("peer changing hash moves its current contribution and preserves other peers") {
    // A peer's change from A to B moves that peer's current contribution. Other peers currently
    // pointing at A remain counted.
    val tipA = hash("A")
    val tipB = hash("B")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 100)
      _ <- (1 to 5).toList.traverse_(i => acc.recordAttestation(pid(s"a-$i"), 50L, tipA))
      // peer-a-1 flips to B at the same ordinal.
      _ <- acc.recordAttestation(pid("a-1"), 50L, tipB)
      counts <- acc.accumAt(50L)
    } yield
      // a-1 moved from A to B; A retains its other 4 contributors; B gains 1.
      expect.same(4, counts.getOrElse(tipA, 0)) &&
        expect.same(1, counts.getOrElse(tipB, 0))
  }

  test("transitional first-crossing decision is sticky") {
    // This is executable behavior, not a proof that irrevocability is safe: after A first crosses
    // the margin, later contradictory current attestations cannot unset it.
    val tipA = hash("A")
    val tipB = hash("B")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      _ <- (1 to 10).toList.traverse_(i => acc.recordAttestation(pid(s"a-$i"), 50L, tipA))
      decidedAfterA <- acc.decidedAt(50L)
      // Try to flip the decision with 100 peers on B.
      _ <- (1 to 100).toList.traverse_(i => acc.recordAttestation(pid(s"b-$i"), 50L, tipB))
      decidedAfterB <- acc.decidedAt(50L)
    } yield
      // Sticky: still decided on A despite the later B wave.
      expect.same(Some(tipA), decidedAfterA) &&
        expect.same(Some(tipA), decidedAfterB)
  }

  test("per-peer at-most-once: same peer re-attesting same hash does not double-count") {
    // A single peer's repeated re-attestation of the same hash at the same ordinal does not stack.
    val tipH = hash("H")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      _ <- (1 to 5).toList.traverse_(_ => acc.recordAttestation(pid("peer-1"), 50L, tipH))
      counts <- acc.accumAt(50L)
    } yield expect.same(1, counts.getOrElse(tipH, 0))
  }

  // ============================================================
  // Repeatability and arrival-order safety gap
  // ============================================================

  test("same ordered transcript produces the same result on two fresh accumulators") {
    // This proves ordinary deterministic replay of one ordered input stream. It does not prove
    // observer-independent consensus because honest observers can receive attestations in
    // different orders.
    val tipH = hash("H")
    val transcript: List[(PeerId, Long, Hash)] =
      (1 to 12).toList.map(i => (pid(s"peer-$i"), 50L, tipH)) ++
        (1 to 5).toList.map(i => (pid(s"peer-$i"), 60L, tipH))
    for {
      accA <- SnowballAccumulator.make[IO](beta = 10)
      accB <- SnowballAccumulator.make[IO](beta = 10)
      _ <- transcript.traverse_ { case (p, o, h) => accA.recordAttestation(p, o, h) }
      _ <- transcript.traverse_ { case (p, o, h) => accB.recordAttestation(p, o, h) }
      decA50 <- accA.decidedAt(50L)
      decB50 <- accB.decidedAt(50L)
      decA60 <- accA.decidedAt(60L)
      decB60 <- accB.decidedAt(60L)
      countsA <- accA.accumAt(50L)
      countsB <- accB.accumAt(50L)
    } yield
      // Identical ordered inputs produce identical local results.
      expect.same(decA50, decB50) &&
        expect.same(decA60, decB60) &&
        expect.same(countsA, countsB) &&
        // Sanity: at ord 50 (12 peers ≥ β=10) decided; at ord 60 (only 5 peers < β=10) not decided.
        expect.same(Some(tipH), decA50) &&
        expect.same(None, decA60)
  }

  test("terminal current counts are permutation-invariant when each peer appears once") {
    // Current counts are a commutative sum for this restricted transcript. Sticky decisions are
    // not permutation-invariant, so equal terminal counts are diagnostic only and cannot serve as
    // portable finality evidence.
    val tipA = hash("A")
    val tipB = hash("B")
    val baseTranscript: List[(PeerId, Long, Hash)] =
      (1 to 10).toList.map(i => (pid(s"peer-$i"), 50L, tipA)) ++
        (1 to 3).toList.map(i => (pid(s"peer-${i + 100}"), 50L, tipB))
    val permuted: List[(PeerId, Long, Hash)] = baseTranscript.reverse
    for {
      accA <- SnowballAccumulator.make[IO](beta = 10)
      accB <- SnowballAccumulator.make[IO](beta = 10)
      _ <- baseTranscript.traverse_ { case (p, o, h) => accA.recordAttestation(p, o, h) }
      _ <- permuted.traverse_ { case (p, o, h) => accB.recordAttestation(p, o, h) }
      countsA <- accA.accumAt(50L)
      countsB <- accB.accumAt(50L)
    } yield
      // Terminal counts match even though the general decision rule is arrival-order sensitive.
      expect.same(countsA, countsB) &&
        // Sanity: both terminal states show the correct totals.
        expect.same(10, countsA.getOrElse(tipA, 0)) &&
        expect.same(3, countsA.getOrElse(tipB, 0))
  }

  test("same eventual current-attestation map can retain opposing decisions under different arrival orders") {
    val tipA = hash("A")
    val tipB = hash("B")
    val aFirst = List(
      (pid("a-1"), 50L, tipA),
      (pid("a-2"), 50L, tipA),
      (pid("b-1"), 50L, tipB),
      (pid("b-2"), 50L, tipB)
    )
    val bFirst = aFirst.reverse

    for {
      accA <- SnowballAccumulator.make[IO](beta = 2)
      accB <- SnowballAccumulator.make[IO](beta = 2)
      _ <- aFirst.traverse_ { case (p, o, h) => accA.recordAttestation(p, o, h) }
      _ <- bFirst.traverse_ { case (p, o, h) => accB.recordAttestation(p, o, h) }
      countsA <- accA.accumAt(50L)
      countsB <- accB.accumAt(50L)
      decidedA <- accA.decidedAt(50L)
      decidedB <- accB.decidedAt(50L)
    } yield
      expect.same(countsA, countsB) &&
        expect.same(Some(tipA), decidedA) &&
        expect.same(Some(tipB), decidedB)
  }

  // ============================================================
  // Illustrative beta-margin example; K and alpha are not exercised
  // ============================================================

  test("current-count margin reaches beta after two additional A attestations") {
    // This exercises only the local beta-margin arithmetic. It cannot be compared to a
    // K/alpha/beta cascade simulation because this implementation performs no K-peer sampling or
    // alpha-majority rounds.
    //
    // Setup: cluster of 12 validators (8 honest, 4 Byzantine — f_adv ≈ 0.33). All 8 honest peers
    // attest hash A; 4 Byzantines split — 2 attest hash B, 2 attest hash A (lying to drain).
    // Expected: A is the canonical leader (10 attestations) vs B (2 attestations); margin = 8 < 10,
    // so undecided yet. Add one more honest peer and the margin becomes 9 ≠ 10 still undecided.
    // Add ANOTHER honest peer → margin 10 ≥ β=10 → the local current-count rule sticks to A.
    val tipA = hash("A")
    val tipB = hash("B")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      // 8 honest attest A.
      _ <- (1 to 8).toList.traverse_(i => acc.recordAttestation(pid(s"honest-$i"), 50L, tipA))
      // 2 Byzantines attest B (split-honest pattern).
      _ <- (1 to 2).toList.traverse_(i => acc.recordAttestation(pid(s"byz-b-$i"), 50L, tipB))
      // 2 Byzantines attest A (coordinated-lie pattern: pretending to be honest to drain confidence).
      _ <- (1 to 2).toList.traverse_(i => acc.recordAttestation(pid(s"byz-a-$i"), 50L, tipA))
      midState <- acc.decidedAt(50L) // 10 on A, 2 on B → margin 8 < β=10 → undecided
      // Two more honest votes for A push us over: 12 on A, 2 on B → margin 10 ≥ β=10 → decided A.
      _ <- acc.recordAttestation(pid("honest-9"), 50L, tipA)
      _ <- acc.recordAttestation(pid("honest-10"), 50L, tipA)
      finalState <- acc.decidedAt(50L)
    } yield
      // The transitional current-count rule crosses its beta margin on A.
      expect.same(None, midState) &&
        expect.same(Some(tipA), finalState)
  }

  // ============================================================
  // Canonical-chain filtering of transitional sticky decisions
  // ============================================================

  test("highestDecidedOnCanonical: returns highest decided ord whose decided hash matches canonical") {
    // Decide ord 50 on hash H50, ord 60 on hash H60. Canonical chain agrees on both. Should
    // return 60 (the highest matching).
    val h50 = hash("h50")
    val h60 = hash("h60")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      _ <- (1 to 10).toList.traverse_(i => acc.recordAttestation(pid(s"p-$i"), 50L, h50))
      _ <- (1 to 10).toList.traverse_(i => acc.recordAttestation(pid(s"q-$i"), 60L, h60))
      result <- acc.highestDecidedOnCanonical(o => IO.pure(if (o == 50L) Some(h50) else if (o == 60L) Some(h60) else None))
    } yield expect.same(Some(60L), result)
  }

  test("highestDecidedOnCanonical: returns next-highest matching ord when a higher decision is off-canonical") {
    // Decide ord 60 on hash X (peers built up evidence for X), but canonical chain at ord 60 is Y.
    // Decide ord 50 on hash H50, canonical also H50. Should return 50 (skipping the off-canonical
    // ord 60).
    val h50 = hash("h50")
    val h60off = hash("h60-off-canonical")
    val h60canon = hash("h60-canonical")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      _ <- (1 to 10).toList.traverse_(i => acc.recordAttestation(pid(s"p-$i"), 50L, h50))
      _ <- (1 to 10).toList.traverse_(i => acc.recordAttestation(pid(s"q-$i"), 60L, h60off))
      result <- acc.highestDecidedOnCanonical(o => IO.pure(if (o == 50L) Some(h50) else if (o == 60L) Some(h60canon) else None))
    } yield expect.same(Some(50L), result)
  }

  test("highestDecidedOnCanonical: returns None when no decision matches canonical") {
    val hForked = hash("forked")
    val hCanon = hash("canonical")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      _ <- (1 to 10).toList.traverse_(i => acc.recordAttestation(pid(s"p-$i"), 50L, hForked))
      result <- acc.highestDecidedOnCanonical(_ => IO.pure(Some(hCanon)))
    } yield expect.same(None, result)
  }

  // ============================================================
  // Pruning + reset
  // ============================================================

  test("pruneBelow: drops state strictly below floor") {
    val h50 = hash("h50")
    val h60 = hash("h60")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      _ <- (1 to 10).toList.traverse_(i => acc.recordAttestation(pid(s"p-$i"), 50L, h50))
      _ <- (1 to 10).toList.traverse_(i => acc.recordAttestation(pid(s"q-$i"), 60L, h60))
      _ <- acc.pruneBelow(55L) // keep >= 55, drop ord 50
      dec50 <- acc.decidedAt(50L)
      dec60 <- acc.decidedAt(60L)
      counts50 <- acc.accumAt(50L)
      counts60 <- acc.accumAt(60L)
    } yield
      expect.same(None, dec50) &&
        expect.same(Some(h60), dec60) &&
        expect(counts50.isEmpty) &&
        expect.same(10, counts60.getOrElse(h60, 0))
  }

  test("unsafe_reset: wipes all accumulator state") {
    val h50 = hash("h50")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      _ <- (1 to 10).toList.traverse_(i => acc.recordAttestation(pid(s"p-$i"), 50L, h50))
      _ <- acc.unsafe_reset
      dec <- acc.decidedAt(50L)
      counts <- acc.accumAt(50L)
    } yield expect.same(None, dec) && expect(counts.isEmpty)
  }

  test("unsafe_reset: post-reset accumulator is reusable") {
    val h = hash("h")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 10)
      _ <- (1 to 10).toList.traverse_(i => acc.recordAttestation(pid(s"p-$i"), 50L, h))
      _ <- acc.unsafe_reset
      _ <- (1 to 10).toList.traverse_(i => acc.recordAttestation(pid(s"q-$i"), 100L, h))
      dec <- acc.decidedAt(100L)
    } yield expect.same(Some(h), dec)
  }
}
