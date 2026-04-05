package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.schema.nakamoto.slot.{Slot, VrfOutput}
import io.constellationnetwork.schema.nakamoto.{ChainTip, TipAttestation}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object ChainSelectionSuite extends SimpleIOSuite {

  // Helper to create PeerId from a name
  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  // Helper to create a slot
  private def slot(n: Long): Slot = Slot(NonNegLong.unsafeFrom(n))

  // Helper to create a hash
  private def hash(s: String): Hash = Hash(s.padTo(64, '0'))

  // Helper to create VRF output from a byte value (fills 64 bytes)
  private def vrfOutput(byte: Byte): VrfOutput =
    VrfOutput.fromBytes(Array.fill(64)(byte))

  // Helper to create a chain tip
  private def tip(hashStr: String, slotNum: Long, ordinal: Long, parentHashStr: String, vrfByte: Byte): ChainTip =
    ChainTip(hash(hashStr), slot(slotNum), ordinal, hash(parentHashStr), vrfOutput(vrfByte))

  // Helper to create an attestation
  private def att(tipHash: Hash, tipSlot: Slot, tipOrdinal: Long, attestedAt: Slot): TipAttestation =
    TipAttestation(tipHash, tipSlot, tipOrdinal, attestedAt)

  // Setup StakeRegistry with N equal-weight validators
  private def setupRegistry(validators: Set[PeerId]): IO[StakeRegistry[IO]] =
    for {
      registry <- StakeRegistry.equalWeight[IO]
      _ <- registry.updateValidators(validators)
    } yield registry

  // Setup ChainSelection with TipTracker and StakeRegistry
  private def setupChainSelection(validators: Set[PeerId]): IO[(ChainSelection[IO], TipTracker[IO], StakeRegistry[IO])] =
    for {
      registry <- setupRegistry(validators)
      tracker <- TipTracker.make[IO](registry)
      // fetchParent always returns None — tests focus on tip comparison, not ancestry traversal
      chainSelection = ChainSelection.make[IO](tracker, _ => IO.pure(None))
    } yield (chainSelection, tracker, registry)

  test("prefers tip with higher attestation weight") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipA = tip("tipA", 10, 100, "parent", 0x50)
    val tipB = tip("tipB", 10, 100, "parent", 0x50)

    for {
      (chainSelection, tracker, _) <- setupChainSelection(Set(peer1, peer2, peer3))
      // 2 peers attest to tipA, 1 to tipB
      _ <- tracker.recordAttestation(peer1, att(tipA.hash, tipA.slot, tipA.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipA.hash, tipA.slot, tipA.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer3, att(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      result <- chainSelection.compare(tipA, tipB)
    } yield expect.same(tipA.hash, result.hash)
  }

  test("equal weight, prefers higher ordinal (longer chain)") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    // Same weight (1 attestation each), tipA has higher ordinal
    val tipA = tip("tipA", 10, 101, "parent", 0x50)
    val tipB = tip("tipB", 10, 100, "parent", 0x50)

    for {
      (chainSelection, tracker, _) <- setupChainSelection(Set(peer1, peer2))
      _ <- tracker.recordAttestation(peer1, att(tipA.hash, tipA.slot, tipA.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      result <- chainSelection.compare(tipA, tipB)
    } yield expect.same(tipA.hash, result.hash)
  }

  test("equal weight and ordinal, prefers earlier slot") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    // Same weight, same ordinal, tipA has earlier slot
    val tipA = tip("tipA", 9, 100, "parent", 0x50)
    val tipB = tip("tipB", 10, 100, "parent", 0x50)

    for {
      (chainSelection, tracker, _) <- setupChainSelection(Set(peer1, peer2))
      _ <- tracker.recordAttestation(peer1, att(tipA.hash, tipA.slot, tipA.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      result <- chainSelection.compare(tipA, tipB)
    } yield expect.same(tipA.hash, result.hash)
  }

  test("all equal except VRF output, prefers lower VRF output") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    // Same weight, ordinal, slot - tipA has lower VRF output
    val tipA = ChainTip(hash("tipA"), slot(10), 100L, hash("parent"), vrfOutput(0x10))
    val tipB = ChainTip(hash("tipB"), slot(10), 100L, hash("parent"), vrfOutput(0x50))

    for {
      (chainSelection, tracker, _) <- setupChainSelection(Set(peer1, peer2))
      _ <- tracker.recordAttestation(peer1, att(tipA.hash, tipA.slot, tipA.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      result <- chainSelection.compare(tipA, tipB)
    } yield expect.same(tipA.hash, result.hash)
  }

  test("selectBest returns None for empty list") {
    for {
      (chainSelection, _, _) <- setupChainSelection(Set.empty)
      result <- chainSelection.selectBest(List.empty)
    } yield expect.same(None, result)
  }

  test("selectBest returns single candidate") {
    val tipA = tip("tipA", 10, 100, "parent", 0x50)

    for {
      (chainSelection, _, _) <- setupChainSelection(Set.empty)
      result <- chainSelection.selectBest(List(tipA))
    } yield
      expect(result.isDefined) &&
        expect.same(tipA.hash, result.get.hash)
  }

  test("selectBest picks best from 3 candidates") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    // tipB has most attestations
    val tipA = tip("tipA", 10, 100, "parent", 0x50)
    val tipB = tip("tipB", 10, 100, "parent", 0x50)
    val tipC = tip("tipC", 10, 100, "parent", 0x50)

    for {
      (chainSelection, tracker, _) <- setupChainSelection(Set(peer1, peer2, peer3))
      _ <- tracker.recordAttestation(peer1, att(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer3, att(tipA.hash, tipA.slot, tipA.ordinal, slot(11)))
      result <- chainSelection.selectBest(List(tipA, tipB, tipC))
    } yield
      expect(result.isDefined) &&
        expect.same(tipB.hash, result.get.hash)
  }

  test("shouldSwitch returns true when candidate is better") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val current = tip("current", 10, 100, "parent", 0x50)
    val candidate = tip("candidate", 10, 100, "parent", 0x50)

    for {
      (chainSelection, tracker, _) <- setupChainSelection(Set(peer1, peer2, peer3))
      // Candidate has more attestations
      _ <- tracker.recordAttestation(peer1, att(candidate.hash, candidate.slot, candidate.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(candidate.hash, candidate.slot, candidate.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer3, att(current.hash, current.slot, current.ordinal, slot(11)))
      result <- chainSelection.shouldSwitch(current, candidate)
    } yield expect(result)
  }

  test("shouldSwitch returns false when current is better") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val current = tip("current", 10, 100, "parent", 0x50)
    val candidate = tip("candidate", 10, 100, "parent", 0x50)

    for {
      (chainSelection, tracker, _) <- setupChainSelection(Set(peer1, peer2, peer3))
      // Current has more attestations
      _ <- tracker.recordAttestation(peer1, att(current.hash, current.slot, current.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(current.hash, current.slot, current.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer3, att(candidate.hash, candidate.slot, candidate.ordinal, slot(11)))
      result <- chainSelection.shouldSwitch(current, candidate)
    } yield expect(!result)
  }

  test("shouldSwitch returns false when current tip is finalized (even if candidate has more weight)") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val peer4 = pid("peer4")
    val current = tip("current", 10, 100, "parent", 0x50)
    val candidate = tip("candidate", 11, 101, "current", 0x50)

    for {
      (chainSelection, tracker, _) <- setupChainSelection(Set(peer1, peer2, peer3, peer4))
      // Mark current as finalized
      _ <- tracker.markFinalized(current.hash, current.slot)
      // Candidate has more attestations (3 vs 1)
      _ <- tracker.recordAttestation(peer1, att(candidate.hash, candidate.slot, candidate.ordinal, slot(12)))
      _ <- tracker.recordAttestation(peer2, att(candidate.hash, candidate.slot, candidate.ordinal, slot(12)))
      _ <- tracker.recordAttestation(peer3, att(candidate.hash, candidate.slot, candidate.ordinal, slot(12)))
      _ <- tracker.recordAttestation(peer4, att(current.hash, current.slot, current.ordinal, slot(11)))
      result <- chainSelection.shouldSwitch(current, candidate)
    } yield expect(!result)
  }

  test("attestation weight dominates over all tiebreakers (tip with less ordinal but more attestations wins)") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    // tipA: higher ordinal (longer chain), earlier slot, lower VRF - but LESS attestations
    val tipA = ChainTip(hash("tipA"), slot(9), 105L, hash("parent"), vrfOutput(0x10))
    // tipB: lower ordinal, later slot, higher VRF - but MORE attestations
    val tipB = ChainTip(hash("tipB"), slot(10), 100L, hash("parent"), vrfOutput(0x50))

    for {
      (chainSelection, tracker, _) <- setupChainSelection(Set(peer1, peer2, peer3))
      // tipB has 2/3 attestations, tipA has 1/3
      _ <- tracker.recordAttestation(peer1, att(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer3, att(tipA.hash, tipA.slot, tipA.ordinal, slot(11)))
      result <- chainSelection.compare(tipA, tipB)
    } yield expect.same(tipB.hash, result.hash)
  }

  test("identical tips: shouldSwitch returns false") {
    val peer1 = pid("peer1")
    val tipA = tip("tipA", 10, 100, "parent", 0x50)

    for {
      (chainSelection, tracker, _) <- setupChainSelection(Set(peer1))
      _ <- tracker.recordAttestation(peer1, att(tipA.hash, tipA.slot, tipA.ordinal, slot(11)))
      // Comparing tip with itself
      result <- chainSelection.shouldSwitch(tipA, tipA)
    } yield expect(!result)
  }

  test("VRF comparison handles different byte values correctly (unsigned comparison)") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    // 0xFF as unsigned byte is 255, which is > 0x01 as unsigned
    // But as signed byte, 0xFF is -1 which is < 0x01
    // We want unsigned comparison, so tipA (0x01) should win
    val tipA = ChainTip(hash("tipA"), slot(10), 100L, hash("parent"), vrfOutput(0x01))
    val tipB = ChainTip(hash("tipB"), slot(10), 100L, hash("parent"), vrfOutput(0xff.toByte))

    for {
      (chainSelection, tracker, _) <- setupChainSelection(Set(peer1, peer2))
      _ <- tracker.recordAttestation(peer1, att(tipA.hash, tipA.slot, tipA.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      result <- chainSelection.compare(tipA, tipB)
    } yield expect.same(tipA.hash, result.hash)
  }

  test("selectBest with all tips having zero attestations uses tiebreakers") {
    // No attestations recorded, so all tips have 0 weight
    // Should fall through to tiebreakers: ordinal → slot → VRF
    val tipA = ChainTip(hash("tipA"), slot(10), 100L, hash("parent"), vrfOutput(0x50))
    val tipB = ChainTip(hash("tipB"), slot(10), 101L, hash("parent"), vrfOutput(0x50)) // Higher ordinal
    val tipC = ChainTip(hash("tipC"), slot(10), 99L, hash("parent"), vrfOutput(0x50))

    for {
      (chainSelection, _, _) <- setupChainSelection(Set.empty)
      result <- chainSelection.selectBest(List(tipA, tipB, tipC))
    } yield
      expect(result.isDefined) &&
        expect.same(tipB.hash, result.get.hash)
  }

  test("compare is consistent: compare(A, B) and compare(B, A) should select same winner") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val tipA = tip("tipA", 10, 101, "parent", 0x50)
    val tipB = tip("tipB", 10, 100, "parent", 0x50)

    for {
      (chainSelection, tracker, _) <- setupChainSelection(Set(peer1, peer2))
      _ <- tracker.recordAttestation(peer1, att(tipA.hash, tipA.slot, tipA.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer2, att(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      resultAB <- chainSelection.compare(tipA, tipB)
      resultBA <- chainSelection.compare(tipB, tipA)
    } yield expect.same(resultAB.hash, resultBA.hash)
  }
}
