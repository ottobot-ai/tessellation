package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.node.shared.infrastructure.metrics.NoOpMetrics
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.nakamoto.TipAttestation
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object TipTrackerSuite extends SimpleIOSuite {

  // No-op Metrics typeclass instance — tests don't assert on counters so a noop suffices.
  // The skew-rejection counter (`dag_nakamoto_attestations_rejected_skew_total`) still
  // flows through this instance; the dedicated skew tests below assert behavior, not counts.
  implicit private val metrics: io.constellationnetwork.node.shared.infrastructure.metrics.Metrics[IO] = NoOpMetrics.make

  // Helper to create PeerId from a name
  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  // Helper to create a slot
  private def slot(n: Long): Slot = Slot(NonNegLong.unsafeFrom(n))

  // Helper to create a hash
  private def hash(s: String): Hash = Hash(s.padTo(64, '0'))

  // Helper to create an attestation. `attestedAt` is wall-clock epoch ms in production
  // (sourced from `Clock[F].realTime`); in tests we pass a Slot for terseness and
  // unpack to its raw Long value, which keeps the monotonic-newer-wins ordering the
  // test cases assume without coupling the test fixtures to wall-clock minutiae.
  private def att(tipHash: Hash, tipSlot: Slot, tipOrdinal: Long, attestedAt: Slot): TipAttestation =
    TipAttestation(tipHash, tipSlot, tipOrdinal, attestedAt.value.value)

  // Setup StakeRegistry with N equal-weight validators
  private def setupRegistry(validators: Set[PeerId]): IO[StakeRegistry[IO]] =
    for {
      registry <- StakeRegistry.equalWeight[IO]
      _ <- registry.updateValidators(validators)
    } yield registry

  // Setup TipTracker with a registry
  private def setupTracker(validators: Set[PeerId]): IO[(TipTracker[IO], StakeRegistry[IO])] =
    for {
      registry <- setupRegistry(validators)
      tracker <- TipTracker.make[IO](registry)
    } yield (tracker, registry)

  // Helper that passes the attestation's own `attestedAt` as `now` so the skew gate
  // (TipTracker.MaxAttestationSkewMs) is trivially satisfied (skew=0). Existing tests
  // assert behavior independent of wall-clock skew; the dedicated skew tests at the
  // bottom of this file exercise the gate explicitly by passing custom `now` values.
  private def record(tracker: TipTracker[IO], peerId: PeerId, attestation: TipAttestation): IO[Unit] =
    tracker.recordAttestation(peerId, attestation, attestation.attestedAt)

  test("empty tracker has no heaviest tip") {
    for {
      (tracker, _) <- setupTracker(Set.empty)
      heaviest <- tracker.heaviestTip
    } yield expect.same(None, heaviest)
  }

  test("single attestation becomes heaviest tip") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val slotA = slot(10)

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- record(tracker, peer1, att(tipA, slotA, 100L, slot(11)))
      heaviest <- tracker.heaviestTip
    } yield
      expect(heaviest.isDefined) &&
        expect.same(tipA, heaviest.get._1) &&
        expect.same(slotA, heaviest.get._2) &&
        expect.same(Ratio.One, heaviest.get._3)
  }

  test("local isFinalized predicate renormalizes three active of four validators to weight one") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val peer4 = pid("peer4")
    val tipA = hash("tipA")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3, peer4))
      // Current behavior: once the local activity gate opens, the denominator is the receiver's
      // observed-active set. This test records that behavior; it does not establish a portable
      // Phase-2 decision because another receiver can observe a different active set.
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer3, att(tipA, slot(10), 100L, slot(11)))
      isFinalized <- tracker.isFinalized(tipA)
      weight <- tracker.attestationWeight(tipA)
    } yield
      expect(isFinalized) &&
        expect.same(Ratio.One, weight)
  }

  test("newer attestation supersedes older from same peer") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      weightABefore <- tracker.attestationWeight(tipA)
      // Newer attestation (attestedAt=12 > 11)
      _ <- record(tracker, peer1, att(tipB, slot(12), 102L, slot(12)))
      weightAAfter <- tracker.attestationWeight(tipA)
      weightB <- tracker.attestationWeight(tipB)
    } yield
      expect.same(Ratio.One, weightABefore) &&
        expect.same(Ratio.Zero, weightAAfter) &&
        expect.same(Ratio.One, weightB)
  }

  test("local isFinalized predicate rejects both sides of an equal two-versus-two split") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val peer4 = pid("peer4")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3, peer4))
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer3, att(tipB, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer4, att(tipB, slot(10), 100L, slot(11)))
      isFinalizedA <- tracker.isFinalized(tipA)
      isFinalizedB <- tracker.isFinalized(tipB)
      weightA <- tracker.attestationWeight(tipA)
      weightB <- tracker.attestationWeight(tipB)
    } yield
      expect(!isFinalizedA) &&
        expect(!isFinalizedB) &&
        expect.same(Ratio(1, 2), weightA) &&
        expect.same(Ratio(1, 2), weightB)
  }

  test("attestationWeight returns 0 for unknown tip") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val unknownTip = hash("unknown")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      weight <- tracker.attestationWeight(unknownTip)
    } yield expect.same(Ratio.Zero, weight)
  }

  test("markFinalized updates lastFinalized") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      before <- tracker.lastFinalized
      _ <- tracker.markFinalized(tipA, slot(10))
      after <- tracker.lastFinalized
    } yield
      expect.same(None, before) &&
        expect.same(Some((tipA, slot(10))), after)
  }

  test("pruneBelow removes old attestations") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val tipOld = hash("tipOld")
    val tipNew = hash("tipNew")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2))
      _ <- record(tracker, peer1, att(tipOld, slot(5), 50L, slot(6)))
      _ <- record(tracker, peer2, att(tipNew, slot(10), 100L, slot(11)))
      beforePrune <- tracker.allAttestations
      _ <- tracker.pruneBelow(slot(10))
      afterPrune <- tracker.allAttestations
    } yield
      expect.same(2, beforePrune.size) &&
        expect.same(1, afterPrune.size) &&
        expect(afterPrune.contains(peer2)) &&
        expect(!afterPrune.contains(peer1))
  }

  test("heaviestTip local query returns the tip with most observed weight") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3))
      // 2 peers on A, 1 peer on B
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer3, att(tipB, slot(10), 100L, slot(11)))
      heaviest <- tracker.heaviestTip
    } yield
      expect(heaviest.isDefined) &&
        expect.same(tipA, heaviest.get._1) &&
        expect.same(Ratio(2, 3), heaviest.get._3)
  }

  test("attestation from non-validator has zero local attestation weight") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val nonValidator = pid("nonValidator")
    val tipA = hash("tipA")

    for {
      // Only peer1 and peer2 are validators
      (tracker, _) <- setupTracker(Set(peer1, peer2))
      // nonValidator attests but has 0 stake
      _ <- record(tracker, nonValidator, att(tipA, slot(10), 100L, slot(11)))
      weight <- tracker.attestationWeight(tipA)
      isFinalized <- tracker.isFinalized(tipA)
    } yield
      expect.same(Ratio.Zero, weight) &&
        expect(!isFinalized)
  }

  test("local active-set weighting turns two active of three validators into weight one") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipA = hash("tipA")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3))
      // The current receiver-local active denominator turns two attestations into weight one.
      // This is a documented target violation, not evidence for a network-wide threshold.
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipA, slot(10), 100L, slot(11)))
      weight <- tracker.attestationWeight(tipA)
      isFinalized <- tracker.isFinalized(tipA)
    } yield
      expect.same(Ratio.One, weight) &&
        expect(isFinalized)
  }

  test("markFinalized stores the supplied local frontier tuple") {
    // This tests only the mutable marker API. It does not prove ancestor finality, establish an
    // exact-hash Phase-2 decision, or make the marker irreversible under a density reorg.
    val peer1 = pid("peer1")
    val tipFinal = hash("tipFinal")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- tracker.markFinalized(tipFinal, slot(100))
      lastFin <- tracker.lastFinalized
    } yield
      // The supplied tuple is returned unchanged.
      expect.same(Some((tipFinal, slot(100))), lastFin)
  }

  test("older attestation does not supersede newer one from same peer") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      // First: newer attestation
      _ <- record(tracker, peer1, att(tipB, slot(12), 102L, slot(15)))
      // Then try to record older attestation
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      weightA <- tracker.attestationWeight(tipA)
      weightB <- tracker.attestationWeight(tipB)
    } yield
      // tipB should still have weight since its attestation was newer
      expect.same(Ratio.Zero, weightA) &&
        expect.same(Ratio.One, weightB)
  }

  test("older attestation cannot mutate the sibling accumulator after latest-map rejection") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- record(tracker, peer1, att(tipB, slot(10), 100L, slot(15)))
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      accum <- tracker.snowballAccumAt(100L)
    } yield
      expect.same(Some(1), accum.get(tipB)) &&
        expect(!accum.contains(tipA))
  }

  test("allAttestations returns all current attestations") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2))
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipB, slot(12), 102L, slot(13)))
      all <- tracker.allAttestations
    } yield
      expect.same(2, all.size) &&
        expect(all.get(peer1).exists(_.tipHash == tipA)) &&
        expect(all.get(peer2).exists(_.tipHash == tipB))
  }

  // -- Skew gate (#140) --
  //
  // `TipTracker.MaxAttestationSkewMs` defends against peers (badly-skewed or malicious)
  // submitting attestations whose claimed `attestedAt` is more than ±60s away from our
  // local wall-clock when `recordAttestation` runs. Skewed attestations are dropped
  // silently (counter `dag_nakamoto_attestations_rejected_skew_total` increments via
  // the no-op Metrics here; production wires the real registry). The constant is read
  // from `NAKAMOTO_MAX_ATTESTATION_SKEW_MS` at JVM start — tests use whatever the
  // process saw at boot. With the default 60_000ms the boundary numbers below sit
  // safely inside / outside that window.

  test("recordAttestation drops attestation with attestedAt > now + 60s") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val nowMs = 1_700_000_000_000L
    // Claim 60_001ms in the future — just past the +60s ceiling.
    val skewedFuture = TipAttestation(tipA, slot(10), 100L, nowMs + 60_001L)

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- tracker.recordAttestation(peer1, skewedFuture, nowMs)
      all <- tracker.allAttestations
      weight <- tracker.attestationWeight(tipA)
    } yield
      // Attestation rejected → no entry recorded, zero weight.
      expect(all.isEmpty) &&
        expect.same(Ratio.Zero, weight)
  }

  test("recordAttestation drops attestation with attestedAt < now - 60s") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val nowMs = 1_700_000_000_000L
    // Claim 60_001ms in the past — just past the -60s floor.
    val skewedPast = TipAttestation(tipA, slot(10), 100L, nowMs - 60_001L)

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- tracker.recordAttestation(peer1, skewedPast, nowMs)
      all <- tracker.allAttestations
      weight <- tracker.attestationWeight(tipA)
    } yield
      expect(all.isEmpty) &&
        expect.same(Ratio.Zero, weight)
  }

  test("recordAttestation accepts attestation within ±60s window") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val tipA = hash("tipA")
    val nowMs = 1_700_000_000_000L
    // Two attestations near the boundary on each side — both must be accepted.
    val nearFuture = TipAttestation(tipA, slot(10), 100L, nowMs + 59_999L)
    val nearPast = TipAttestation(tipA, slot(10), 100L, nowMs - 59_999L)

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2))
      _ <- tracker.recordAttestation(peer1, nearFuture, nowMs)
      _ <- tracker.recordAttestation(peer2, nearPast, nowMs)
      all <- tracker.allAttestations
      weight <- tracker.attestationWeight(tipA)
    } yield
      // Both inside the window → both recorded → full attestation weight on tipA.
      expect.same(2, all.size) &&
        expect(all.contains(peer1)) &&
        expect(all.contains(peer2)) &&
        expect.same(Ratio.One, weight)
  }

  // ============================================================
  // P-11 (task #141): unsafe_reset for re-bootstrap recovery
  // ============================================================

  test("unsafe_reset: clears attestations map") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val tipA = hash("tipA")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2))
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipA, slot(10), 100L, slot(11)))
      beforeReset <- tracker.allAttestations
      _ <- tracker.unsafe_reset
      afterReset <- tracker.allAttestations
    } yield
      expect.same(2, beforeReset.size) &&
        expect(afterReset.isEmpty)
  }

  test("unsafe_reset: clears lastFinalized marker") {
    val peer1 = pid("peer1")
    val tipFinal = hash("tipFinal")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- tracker.markFinalized(tipFinal, slot(100))
      beforeReset <- tracker.lastFinalized
      _ <- tracker.unsafe_reset
      afterReset <- tracker.lastFinalized
    } yield
      expect.same(Some((tipFinal, slot(100))), beforeReset) &&
        expect.same(None, afterReset)
  }

  test("unsafe_reset: post-reset is reusable — new attestations accumulate weight normally") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipOld = hash("tipOld")
    val tipNew = hash("tipNew")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3))
      // Build up a local marker and attestation state that reset must clear.
      _ <- record(tracker, peer1, att(tipOld, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipOld, slot(10), 100L, slot(11)))
      _ <- tracker.markFinalized(tipOld, slot(10))
      // Reset (simulating re-bootstrap orchestrator firing).
      _ <- tracker.unsafe_reset
      // Post-reset: receive fresh attestations.
      _ <- record(tracker, peer1, att(tipNew, slot(20), 200L, slot(21)))
      _ <- record(tracker, peer2, att(tipNew, slot(20), 200L, slot(21)))
      newWeight <- tracker.attestationWeight(tipNew)
      oldWeight <- tracker.attestationWeight(tipOld)
      isNewFinalized <- tracker.isFinalized(tipNew)
    } yield
      // Old current weight is cleared and the tracker accepts new local observations.
      expect.same(Ratio.Zero, oldWeight) &&
        expect(newWeight >= Ratio(2, 3)) &&
        expect(isNewFinalized)
  }
}
