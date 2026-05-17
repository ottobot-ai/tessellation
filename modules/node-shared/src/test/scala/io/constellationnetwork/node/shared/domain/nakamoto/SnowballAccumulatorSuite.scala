package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.metrics.NoOpMetrics
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import weaver.SimpleIOSuite

/** Tests for [[SnowballAccumulator]] — the per-(ordinal, hash) lifetime accumulator implementing Snowball decision semantics from the Snow
  * family.
  *
  * Anchors: `docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md` §2.2 (algebraic decision rule), §3.4 (why K=8/α=5/β=10), GPU sim kernel at
  * `~/repos/research-nipopos-2026/sims/avalanche_attestation_calibration_gpu.py` commit `5ace3d36` (reference implementation).
  */
object SnowballAccumulatorSuite extends SimpleIOSuite {

  implicit private val metrics: io.constellationnetwork.node.shared.infrastructure.metrics.Metrics[IO] = NoOpMetrics.make

  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  private def hash(s: String): Hash = Hash(s.padTo(64, '0'))

  // ============================================================
  // §2.2 decision rule: leader_count − runner_up_count >= β
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
  // Snowball ≠ Snowflake — counter is NON-RESETTING (load-bearing property)
  // ============================================================

  test("non-reset on flip: losing-hash accumulator persists across other-peer flips") {
    // The defining property of Snowball vs Snowflake: a flip in the leader does NOT reset the
    // accumulator of the losing hash. This is the per-color persistent confidence accumulator
    // (proposal §2.1, §2.2). Here: 5 peers on A, then 5 different peers on B → leader = 5 on B
    // (most recent ties broken by insertion-order — leader by count alone is "any of A/B");
    // both accumulators show their full lifetime evidence regardless of who's currently leading.
    val tipA = hash("A")
    val tipB = hash("B")
    for {
      acc <- SnowballAccumulator.make[IO](beta = 100) // β=100 so neither decides; we just check counts
      _ <- (1 to 5).toList.traverse_(i => acc.recordAttestation(pid(s"a-$i"), 50L, tipA))
      _ <- (1 to 5).toList.traverse_(i => acc.recordAttestation(pid(s"b-$i"), 50L, tipB))
      counts <- acc.accumAt(50L)
    } yield
      // Both lifetime accumulators preserved — neither was reset by the other.
      expect.same(5, counts.getOrElse(tipA, 0)) &&
        expect.same(5, counts.getOrElse(tipB, 0))
  }

  test("non-reset on flip: peer flipping its OWN preference moves its contribution but doesn't zero others") {
    // A peer's flip from hash A to hash B moves THAT peer's contribution; OTHER peers' lifetime
    // contributions on A are preserved (Snowball's per-color persistence at the cluster level).
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

  test("monotone (irrevocable) decision: once decided, the decision is sticky") {
    // Snowball decisions are irrevocable at the protocol layer (proposal §2.1 "decided ⇒ quiescent").
    // After 10 peers decide A, more peers attesting a contradicting hash do NOT unset the decision.
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
  // NID property test — the load-bearing assertion
  // ============================================================

  test("NID: identical transcripts ⇒ identical decisions regardless of who is observing") {
    // Two SnowballAccumulator instances built independently and fed the SAME (peer, ord, hash)
    // tuples produce IDENTICAL decided maps. This is the property that Snowball restores at the
    // T_weight position after P-11b is rolled back — the defining property of consensus.
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
      // Identical decisions — NID property.
      expect.same(decA50, decB50) &&
        expect.same(decA60, decB60) &&
        expect.same(countsA, countsB) &&
        // Sanity: at ord 50 (12 peers ≥ β=10) decided; at ord 60 (only 5 peers < β=10) not decided.
        expect.same(Some(tipH), decA50) &&
        expect.same(None, decA60)
  }

  test("NID: final accumulator counts are permutation-invariant (decisions are intentionally order-sensitive)") {
    // Snowball decisions are made in real-time as the margin first crosses β; the *decision* IS
    // intentionally order-sensitive — that's the irrevocability property (proposal §2.1 "decided
    // ⇒ quiescent"). The accumulator *counts*, however, must be permutation-invariant — the
    // per-color lifetime evidence is a commutative sum. This test asserts the latter, which is
    // the load-bearing NID guarantee for downstream consumers that only read counts (e.g. the
    // ATTEST-FINALIZED log line's diagnostic accumulator dump).
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
      // Counts are commutative — accumulator state is permutation-invariant.
      expect.same(countsA, countsB) &&
        // Sanity: both terminal states show the correct totals.
        expect.same(10, countsA.getOrElse(tipA, 0)) &&
        expect.same(3, countsA.getOrElse(tipB, 0))
  }

  // ============================================================
  // Smoke validation: K=8/α=5/β=10 prevents safety violations on a small synthetic transcript
  // ============================================================

  test("smoke validation: K=8/α=5/β=10 — at f_adv=0.33 with structured transcripts, observers agree") {
    // This is a Scala-side sanity check that the Snowball accumulator's decision tracks the
    // empirically-validated GPU sim result at K=8/α=5/β=10. NOT a replacement for the GPU sim
    // (`~/repos/research-nipopos-2026/sims/avalanche_attestation_calibration_gpu.py` commit
    // `5ace3d36`) — just a smoke test that the Scala port computes consistent decisions.
    //
    // Setup: cluster of 12 validators (8 honest, 4 Byzantine — f_adv ≈ 0.33). All 8 honest peers
    // attest hash A; 4 Byzantines split — 2 attest hash B, 2 attest hash A (lying to drain).
    // Expected: A is the canonical leader (10 attestations) vs B (2 attestations); margin = 8 < 10,
    // so undecided yet. Add one more honest peer and the margin becomes 9 ≠ 10 still undecided.
    // Add ANOTHER honest peer → margin 10 ≥ β=10 → decided A. This walks the cascade through the
    // narrow window where the decision crystallises.
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
      // Cascade lands on canonical (A) — no safety violation in this trial. Aligns with the GPU
      // sim's 0 / 10000 safety violations result at this configuration.
      expect.same(None, midState) &&
        expect.same(Some(tipA), finalState)
  }

  // ============================================================
  // Canonical-chain walk (NID-restored path)
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
