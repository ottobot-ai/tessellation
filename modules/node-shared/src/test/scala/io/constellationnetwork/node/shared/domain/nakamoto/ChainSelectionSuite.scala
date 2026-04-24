package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.schema.nakamoto.slot.{Slot, VrfOutput}
import io.constellationnetwork.schema.nakamoto.{ChainTip, TipAttestation}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

/** Tests for the structural-only fork choice. After the BABE/GRANDPA-style split (attestations only drive finality, never fork choice),
  * `compare` is a deterministic function of block headers — ordinal/slot/VRF tiebreaks for short forks, density-in-window for long forks.
  * Attestations are still threaded through `shouldSwitch`'s "don't revert below the finalized head" guard, but never tip selection.
  */
object ChainSelectionSuite extends SimpleIOSuite {

  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  private def slot(n: Long): Slot = Slot(NonNegLong.unsafeFrom(n))

  private def hash(s: String): Hash = Hash(s.padTo(64, '0'))

  private def vrfOutput(byte: Byte): VrfOutput =
    VrfOutput.fromBytes(Array.fill(64)(byte))

  private def tip(hashStr: String, slotNum: Long, ordinal: Long, parentHashStr: String, vrfByte: Byte): ChainTip =
    ChainTip(hash(hashStr), slot(slotNum), ordinal, hash(parentHashStr), vrfOutput(vrfByte))

  private def setupRegistry(validators: Set[PeerId]): IO[StakeRegistry[IO]] =
    for {
      registry <- StakeRegistry.equalWeight[IO]
      _ <- registry.updateValidators(validators)
    } yield registry

  /** ChainSelection with no parent fetcher — short-fork tests don't need ancestor traversal because their tips have either matching parents
    * or one is the immediate parent of the other.
    */
  private def setupChainSelection(validators: Set[PeerId] = Set.empty): IO[(ChainSelection[IO], TipTracker[IO])] =
    for {
      registry <- setupRegistry(validators)
      tracker <- TipTracker.make[IO](registry)
      chainSelection = ChainSelection.make[IO](tracker, _ => IO.pure(None))
    } yield (chainSelection, tracker)

  test("compare: identical tips return same") {
    val tipA = tip("tipA", 10, 100, "parent", 0x50)
    for {
      (chainSelection, _) <- setupChainSelection()
      result <- chainSelection.compare(tipA, tipA)
    } yield expect.same(tipA.hash, result.hash)
  }

  test("compare: prefers higher ordinal (longer chain) — Praos / maxvalid-tk") {
    // Same parent, different tips, A has higher ordinal → A wins by length
    val tipA = tip("tipA", 11, 101, "parent", 0x50)
    val tipB = tip("tipB", 10, 100, "parent", 0x50)
    for {
      (chainSelection, _) <- setupChainSelection()
      result <- chainSelection.compare(tipA, tipB)
    } yield expect.same(tipA.hash, result.hash)
  }

  test("compare: equal ordinal, prefers earlier slot — slot-recency tiebreak") {
    val tipA = tip("tipA", 9, 100, "parent", 0x50)
    val tipB = tip("tipB", 10, 100, "parent", 0x50)
    for {
      (chainSelection, _) <- setupChainSelection()
      result <- chainSelection.compare(tipA, tipB)
    } yield expect.same(tipA.hash, result.hash)
  }

  test("compare: all equal except VRF, prefers lower VRF — final tiebreak") {
    val tipA = ChainTip(hash("tipA"), slot(10), 100L, hash("parent"), vrfOutput(0x10))
    val tipB = ChainTip(hash("tipB"), slot(10), 100L, hash("parent"), vrfOutput(0x50))
    for {
      (chainSelection, _) <- setupChainSelection()
      result <- chainSelection.compare(tipA, tipB)
    } yield expect.same(tipA.hash, result.hash)
  }

  test("compare: ignores attestation weight — even a 100% attested losing-by-ordinal tip stays losing") {
    // Pre-fix this would have been picked by the attestation path. Post-fix, structural rule alone wins:
    // tipA has higher ordinal so tipA wins regardless of who attested what.
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipA = tip("tipA", 10, 105, "parent", 0x10) // longer chain (105 > 100)
    val tipB = tip("tipB", 10, 100, "parent", 0x50)
    for {
      (chainSelection, tracker) <- setupChainSelection(Set(peer1, peer2, peer3))
      // All three peers attest tipB — under blended fork choice, tipB would have won. Now it doesn't.
      _ <- tracker.recordAttestation(peer1, TipAttestation(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer2, TipAttestation(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      _ <- tracker.recordAttestation(peer3, TipAttestation(tipB.hash, tipB.slot, tipB.ordinal, slot(11)))
      result <- chainSelection.compare(tipA, tipB)
    } yield expect.same(tipA.hash, result.hash)
  }

  test("compare: VRF tiebreak treats bytes as unsigned (0x01 < 0xFF)") {
    val tipA = ChainTip(hash("tipA"), slot(10), 100L, hash("parent"), vrfOutput(0x01))
    val tipB = ChainTip(hash("tipB"), slot(10), 100L, hash("parent"), vrfOutput(0xff.toByte))
    for {
      (chainSelection, _) <- setupChainSelection()
      result <- chainSelection.compare(tipA, tipB)
    } yield expect.same(tipA.hash, result.hash)
  }

  test("compare is consistent: compare(A, B) == compare(B, A)") {
    val tipA = tip("tipA", 10, 101, "parent", 0x50)
    val tipB = tip("tipB", 10, 100, "parent", 0x50)
    for {
      (chainSelection, _) <- setupChainSelection()
      resultAB <- chainSelection.compare(tipA, tipB)
      resultBA <- chainSelection.compare(tipB, tipA)
    } yield expect.same(resultAB.hash, resultBA.hash)
  }

  test("selectBest: empty list returns None") {
    for {
      (chainSelection, _) <- setupChainSelection()
      result <- chainSelection.selectBest(List.empty)
    } yield expect.same(None, result)
  }

  test("selectBest: single candidate returns it") {
    val tipA = tip("tipA", 10, 100, "parent", 0x50)
    for {
      (chainSelection, _) <- setupChainSelection()
      result <- chainSelection.selectBest(List(tipA))
    } yield
      expect(result.isDefined) &&
        expect.same(tipA.hash, result.get.hash)
  }

  test("selectBest: picks highest-ordinal among 3 candidates (no attestations)") {
    val tipA = ChainTip(hash("tipA"), slot(10), 100L, hash("parent"), vrfOutput(0x50))
    val tipB = ChainTip(hash("tipB"), slot(10), 101L, hash("parent"), vrfOutput(0x50)) // longest
    val tipC = ChainTip(hash("tipC"), slot(10), 99L, hash("parent"), vrfOutput(0x50))
    for {
      (chainSelection, _) <- setupChainSelection()
      result <- chainSelection.selectBest(List(tipA, tipB, tipC))
    } yield
      expect(result.isDefined) &&
        expect.same(tipB.hash, result.get.hash)
  }

  test("shouldSwitch: returns true when candidate has higher ordinal (structural)") {
    val current = tip("current", 10, 100, "parent", 0x50)
    val candidate = tip("candidate", 11, 101, "current", 0x50)
    for {
      (chainSelection, _) <- setupChainSelection()
      result <- chainSelection.shouldSwitch(current, candidate)
    } yield expect(result)
  }

  test("shouldSwitch: returns false when current already wins structurally") {
    val current = tip("current", 10, 101, "parent", 0x50)
    val candidate = tip("candidate", 10, 100, "parent", 0x50)
    for {
      (chainSelection, _) <- setupChainSelection()
      result <- chainSelection.shouldSwitch(current, candidate)
    } yield expect(!result)
  }

  test("shouldSwitch: identical tips returns false") {
    val tipA = tip("tipA", 10, 100, "parent", 0x50)
    for {
      (chainSelection, _) <- setupChainSelection()
      result <- chainSelection.shouldSwitch(tipA, tipA)
    } yield expect(!result)
  }

  test("shouldSwitch: refuses to switch when current tip is finalized — finalized-head lock") {
    // Even if the candidate is structurally better, finalized state wins.
    // This is the only place attestations (via lastFinalized) influence chain selection.
    val current = tip("current", 10, 100, "parent", 0x50)
    val candidate = tip("candidate", 11, 101, "current", 0x50)
    for {
      (chainSelection, tracker) <- setupChainSelection()
      _ <- tracker.markFinalized(current.hash, current.slot)
      result <- chainSelection.shouldSwitch(current, candidate)
    } yield expect(!result)
  }
}
