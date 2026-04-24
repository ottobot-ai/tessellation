package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.schema.nakamoto.TipAttestation
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object TipTrackerSuite extends SimpleIOSuite {

  // Helper to create PeerId from a name
  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  // Helper to create a slot
  private def slot(n: Long): Slot = Slot(NonNegLong.unsafeFrom(n))

  // Helper to create a hash
  private def hash(s: String): Hash = Hash(s.padTo(64, '0'))

  // Helper to create an attestation
  private def att(tipHash: Hash, tipSlot: Slot, tipOrdinal: Long, attestedAt: Slot): TipAttestation =
    TipAttestation(tipHash, tipSlot, tipOrdinal, attestedAt)

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
      _ <- tracker.recordAttestation(peer1, att(tipA, slotA, 100L, slot(11)))
      heaviest <- tracker.heaviestTip
    } yield
      expect(heaviest.isDefined) &&
        expect.same(tipA, heaviest.get._1) &&
        expect.same(slotA, heaviest.get._2) &&
        expect(Math.abs(heaviest.get._3 - 1.0) < 0.0001)
  }

  test("majority attestation reaches finality (3 of 4 peers attest same tip → >2/3 weight)") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val peer4 = pid("peer4")
    val tipA = hash("tipA")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3, peer4))
      // 3 of 4 peers attest. Active fraction = 3/4 = 75% ≥ MinActiveQuorumFraction (50%),
      // so optimistic weighting kicks in: weight is computed against the active set (3 peers),
      // and all 3 attested the same tip → weight = 1.0. Finality threshold (2/3) is met.
      _ <- tracker.recordAttestation(peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipA, slot(10), 100L, slot(11)))
      _ <- tracker.recordAttestation(peer3, att(tipA, slot(10), 100L, slot(11)))
      isFinalized <- tracker.isFinalized(tipA)
      weight <- tracker.attestationWeight(tipA)
    } yield
      expect(isFinalized) &&
        expect(Math.abs(weight - 1.0) < 0.0001)
  }

  test("newer attestation supersedes older from same peer") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- tracker.recordAttestation(peer1, att(tipA, slot(10), 100L, slot(11)))
      weightABefore <- tracker.attestationWeight(tipA)
      // Newer attestation (attestedAt=12 > 11)
      _ <- tracker.recordAttestation(peer1, att(tipB, slot(12), 102L, slot(12)))
      weightAAfter <- tracker.attestationWeight(tipA)
      weightB <- tracker.attestationWeight(tipB)
    } yield
      expect(Math.abs(weightABefore - 1.0) < 0.0001) &&
        expect(Math.abs(weightAAfter - 0.0) < 0.0001) &&
        expect(Math.abs(weightB - 1.0) < 0.0001)
  }

  test("split attestations (2 peers on tip A, 2 on tip B → neither finalized with equal weight)") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val peer4 = pid("peer4")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3, peer4))
      _ <- tracker.recordAttestation(peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipA, slot(10), 100L, slot(11)))
      _ <- tracker.recordAttestation(peer3, att(tipB, slot(10), 100L, slot(11)))
      _ <- tracker.recordAttestation(peer4, att(tipB, slot(10), 100L, slot(11)))
      isFinalizedA <- tracker.isFinalized(tipA)
      isFinalizedB <- tracker.isFinalized(tipB)
      weightA <- tracker.attestationWeight(tipA)
      weightB <- tracker.attestationWeight(tipB)
    } yield
      expect(!isFinalizedA) &&
        expect(!isFinalizedB) &&
        expect(Math.abs(weightA - 0.5) < 0.0001) &&
        expect(Math.abs(weightB - 0.5) < 0.0001)
  }

  test("attestationWeight returns 0 for unknown tip") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val unknownTip = hash("unknown")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- tracker.recordAttestation(peer1, att(tipA, slot(10), 100L, slot(11)))
      weight <- tracker.attestationWeight(unknownTip)
    } yield expect.same(0.0, weight)
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
      _ <- tracker.recordAttestation(peer1, att(tipOld, slot(5), 50L, slot(6)))
      _ <- tracker.recordAttestation(peer2, att(tipNew, slot(10), 100L, slot(11)))
      beforePrune <- tracker.allAttestations
      _ <- tracker.pruneBelow(slot(10))
      afterPrune <- tracker.allAttestations
    } yield
      expect.same(2, beforePrune.size) &&
        expect.same(1, afterPrune.size) &&
        expect(afterPrune.contains(peer2)) &&
        expect(!afterPrune.contains(peer1))
  }

  test("fork choice: heaviestTip returns tip with most weight") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3))
      // 2 peers on A, 1 peer on B
      _ <- tracker.recordAttestation(peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipA, slot(10), 100L, slot(11)))
      _ <- tracker.recordAttestation(peer3, att(tipB, slot(10), 100L, slot(11)))
      heaviest <- tracker.heaviestTip
    } yield
      expect(heaviest.isDefined) &&
        expect.same(tipA, heaviest.get._1) &&
        expect(Math.abs(heaviest.get._3 - 2.0 / 3.0) < 0.0001)
  }

  test("attestation from non-validator (zero stake) doesn't count toward finality") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val nonValidator = pid("nonValidator")
    val tipA = hash("tipA")

    for {
      // Only peer1 and peer2 are validators
      (tracker, _) <- setupTracker(Set(peer1, peer2))
      // nonValidator attests but has 0 stake
      _ <- tracker.recordAttestation(nonValidator, att(tipA, slot(10), 100L, slot(11)))
      weight <- tracker.attestationWeight(tipA)
      isFinalized <- tracker.isFinalized(tipA)
    } yield
      expect.same(0.0, weight) &&
        expect(!isFinalized)
  }

  test("2/3+1 threshold: exactly 2 of 3 validators is 0.667 (borderline, should finalize)") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipA = hash("tipA")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3))
      // 2 of 3 peers attest. Active fraction = 2/3 ≈ 66.7% ≥ MinActiveQuorumFraction (50%),
      // so optimistic weighting is active: both attesters voted the same tip, weight = 1.0
      // against the 2-peer active set, which trivially exceeds the 2/3 finality threshold.
      _ <- tracker.recordAttestation(peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipA, slot(10), 100L, slot(11)))
      weight <- tracker.attestationWeight(tipA)
      isFinalized <- tracker.isFinalized(tipA)
    } yield
      expect(Math.abs(weight - 1.0) < 0.0001) &&
        expect(isFinalized)
  }

  test("chain finalization: when tip at ordinal 100 finalizes, all ancestors are implicitly finalized") {
    // This test verifies the conceptual model: we don't need to explicitly track
    // ancestor finalization because any tip that finalizes implies all its ancestors
    // are finalized. The markFinalized/lastFinalized tracks the frontier.
    val peer1 = pid("peer1")
    val tipFinal = hash("tipFinal")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- tracker.markFinalized(tipFinal, slot(100))
      lastFin <- tracker.lastFinalized
    } yield
      // When we finalize tip at slot 100, ordinals 1-99 are implicitly finalized
      // because a snapshot at slot 100 must have all previous snapshots in its chain
      expect.same(Some((tipFinal, slot(100))), lastFin)
  }

  test("older attestation does not supersede newer one from same peer") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      // First: newer attestation
      _ <- tracker.recordAttestation(peer1, att(tipB, slot(12), 102L, slot(15)))
      // Then try to record older attestation
      _ <- tracker.recordAttestation(peer1, att(tipA, slot(10), 100L, slot(11)))
      weightA <- tracker.attestationWeight(tipA)
      weightB <- tracker.attestationWeight(tipB)
    } yield
      // tipB should still have weight since its attestation was newer
      expect.same(0.0, weightA) &&
        expect(Math.abs(weightB - 1.0) < 0.0001)
  }

  test("allAttestations returns all current attestations") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2))
      _ <- tracker.recordAttestation(peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipB, slot(12), 102L, slot(13)))
      all <- tracker.allAttestations
    } yield
      expect.same(2, all.size) &&
        expect(all.get(peer1).exists(_.tipHash == tipA)) &&
        expect(all.get(peer2).exists(_.tipHash == tipB))
  }

  test("highestFinalizedOrdinal skips attestations not on our canonical chain (fork scenario)") {
    // Simulates 3-node cluster where gl0-2 forked. gl0-0/gl0-1 share chain A;
    // gl0-2 is on chain B. All three attest their local tip at ordinal 100 with
    // different hashes. For a gl0-0 caller (canonical chain = A), only gl0-0
    // and gl0-1's attestations count → 2/3 weight = threshold met, finalize
    // ordinal 100 with hash A. For a gl0-2 caller (canonical chain = B), only
    // its own attestation counts → 1/3 < 2/3, no finality on B. This prevents
    // the hash-agnostic bug that let both forks "finalize" in parallel.
    val gl0_0 = pid("gl0-0")
    val gl0_1 = pid("gl0-1")
    val gl0_2 = pid("gl0-2")
    val hashA100 = hash("chainA_ord100")
    val hashB100 = hash("chainB_ord100")

    for {
      (tracker, _) <- setupTracker(Set(gl0_0, gl0_1, gl0_2))
      _ <- tracker.recordAttestation(gl0_0, att(hashA100, slot(200), 100L, slot(201)))
      _ <- tracker.recordAttestation(gl0_1, att(hashA100, slot(200), 100L, slot(201)))
      _ <- tracker.recordAttestation(gl0_2, att(hashB100, slot(205), 100L, slot(206)))

      // gl0-0's view: canonical chain A → ordinal 100 canonical hash = hashA100
      fromChainA <- tracker.highestFinalizedOrdinal(
        2.0 / 3.0,
        ord => IO.pure(if (ord == 100L) Some(hashA100) else None)
      )

      // gl0-2's view: canonical chain B → ordinal 100 canonical hash = hashB100
      fromChainB <- tracker.highestFinalizedOrdinal(
        2.0 / 3.0,
        ord => IO.pure(if (ord == 100L) Some(hashB100) else None)
      )
    } yield
      // Chain A finalizes (gl0-0 + gl0-1 = 2/3 stake agree on A's ord 100)
      expect(fromChainA.isDefined) &&
        expect.same(100L, fromChainA.get._1) &&
        expect(Math.abs(fromChainA.get._2 - 2.0 / 3.0) < 0.0001) &&
        // Chain B does NOT finalize (only gl0-2 attests, 1/3 < 2/3)
        expect.same(None, fromChainB)
  }

  test("highestFinalizedOrdinal: GRANDPA ancestor rule still works (all agree chain)") {
    // No fork: all three peers attest different ordinals on the same chain.
    // Finality should pick the highest ordinal where cumulative weight ≥ 2/3.
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val h50 = hash("ord50")
    val h60 = hash("ord60")
    val h70 = hash("ord70")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3))
      _ <- tracker.recordAttestation(peer1, att(h70, slot(140), 70L, slot(141)))
      _ <- tracker.recordAttestation(peer2, att(h60, slot(120), 60L, slot(121)))
      _ <- tracker.recordAttestation(peer3, att(h50, slot(100), 50L, slot(101)))

      result <- tracker.highestFinalizedOrdinal(
        2.0 / 3.0,
        ord =>
          IO.pure(ord match {
            case 70L => Some(h70)
            case 60L => Some(h60)
            case 50L => Some(h50)
            case _   => None
          })
      )
    } yield
      // peer1 alone at ord 70 = 1/3 (below threshold),
      // peer1 + peer2 cumulative at ord 60 = 2/3 (at threshold — finalize ord 60)
      expect(result.isDefined) &&
        expect.same(60L, result.get._1)
  }
}
