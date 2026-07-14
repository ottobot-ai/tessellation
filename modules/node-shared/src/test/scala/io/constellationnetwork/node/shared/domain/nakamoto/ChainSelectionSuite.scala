package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.node.shared.infrastructure.metrics.NoOpMetrics
import io.constellationnetwork.schema.nakamoto.slot.{Slot, VrfOutput}
import io.constellationnetwork.schema.nakamoto.{ChainTip, TipAttestation}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

/** Tests for structural Nakamoto/Taktikos fork choice. `compare` is a deterministic function of block headers: ordinal/slot/VRF tie-breaks
  * for short forks and density-in-window for long forks. Attestations do not create a GRANDPA/BFT lock or choose the tip. Current
  * `shouldSwitch` finality guards are transitional target violations because reversible Phase 2 and retention-only k2 cannot floor density.
  */
object ChainSelectionSuite extends SimpleIOSuite {

  // No-op Metrics typeclass instance — required by `TipTracker.make` since the
  // skew-rejection counter (`dag_nakamoto_attestations_rejected_skew_total`) is
  // emitted there. This suite never exercises the skew path so the noop is enough.
  implicit private val metrics: io.constellationnetwork.node.shared.infrastructure.metrics.Metrics[IO] = NoOpMetrics.make

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
      // Pass `now == attestedAt` so the skew gate (TipTracker.MaxAttestationSkewMs) trivially passes.
      _ <- tracker.recordAttestation(peer1, TipAttestation(tipB.hash, tipB.slot, tipB.ordinal, slot(11).value.value), slot(11).value.value)
      _ <- tracker.recordAttestation(peer2, TipAttestation(tipB.hash, tipB.slot, tipB.ordinal, slot(11).value.value), slot(11).value.value)
      _ <- tracker.recordAttestation(peer3, TipAttestation(tipB.hash, tipB.slot, tipB.ordinal, slot(11).value.value), slot(11).value.value)
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

  test("regression witness: mixed tk/bg selectBest is permutation-sensitive across three connected candidate tines") {
    // Pairwise commutativity is not enough for a canonical frontier. With kLookback=2 and sWindow=5:
    //   A <tk B (B is longer), B <tk C (C is longer), but C <bg A (equal density, lower A VRF wins).
    // The resulting cycle makes the left-fold in selectBest depend on candidate arrival order. This test
    // intentionally records the activation blocker; no portable canonical-selection evidence may be minted
    // from selectBest until the protocol defines and proves a deterministic frontier rule.
    val g = tip("cycle-g", 0L, 0L, "cycle-root", 0x40)
    val a = tip("cycle-a", 1L, 1L, "cycle-g", 0x01)
    val b1 = tip("cycle-b1", 2L, 1L, "cycle-g", 0x20)
    val b = tip("cycle-b", 4L, 2L, "cycle-b1", 0x30)
    val c1 = tip("cycle-c1", 6L, 2L, "cycle-b1", 0x31)
    val c = tip("cycle-c", 10L, 3L, "cycle-c1", 0x50)
    val graph = List(g, a, b1, b, c1, c)

    for {
      cs <- graphSelection(graph, kLookback = 2L, sWindow = 5L)
      ab <- cs.compare(a, b)
      bc <- cs.compare(b, c)
      ca <- cs.compare(c, a)
      abc <- cs.selectBest(List(a, b, c))
      bca <- cs.selectBest(List(b, c, a))
      cab <- cs.selectBest(List(c, a, b))
    } yield
      expect.all(
        ab.hash == b.hash,
        bc.hash == c.hash,
        ca.hash == a.hash,
        abc.exists(_.hash == c.hash),
        bca.exists(_.hash == a.hash),
        cab.exists(_.hash == b.hash)
      )
  }

  test("activation blocker: strict tk/bg preferences form a permutation-sensitive three-cycle") {
    // This cycle does not depend on the current VRF/hash equality tiebreak. With k=3 and s=10:
    // A beats B by maxvalid-tk length, B beats C by strictly greater MRCA-window density,
    // and C beats A by strictly greater MRCA-window density.
    val g = tip("strict-cycle-g", 0L, 0L, "strict-cycle-root", 0x40)
    val x1 = tip("strict-cycle-x1", 1L, 1L, "strict-cycle-g", 0x10)
    val x2 = tip("strict-cycle-x2", 2L, 2L, "strict-cycle-x1", 0x11)
    val a1 = tip("strict-cycle-a1", 20L, 3L, "strict-cycle-x2", 0x20)
    val a2 = tip("strict-cycle-a2", 30L, 4L, "strict-cycle-a1", 0x21)
    val a = tip("strict-cycle-a", 40L, 5L, "strict-cycle-a2", 0x22)
    val b1 = tip("strict-cycle-b1", 3L, 3L, "strict-cycle-x2", 0x30)
    val b = tip("strict-cycle-b", 4L, 4L, "strict-cycle-b1", 0x31)
    val c1 = tip("strict-cycle-c1", 1L, 1L, "strict-cycle-g", 0x50)
    val c2 = tip("strict-cycle-c2", 5L, 2L, "strict-cycle-c1", 0x51)
    val c3 = tip("strict-cycle-c3", 9L, 3L, "strict-cycle-c2", 0x52)
    val c = tip("strict-cycle-c", 20L, 4L, "strict-cycle-c3", 0x53)
    val graph = List(g, x1, x2, a1, a2, a, b1, b, c1, c2, c3, c)

    for {
      cs <- graphSelection(graph, kLookback = 3L, sWindow = 10L)
      ab <- cs.compare(a, b)
      bc <- cs.compare(b, c)
      ca <- cs.compare(c, a)
      abc <- cs.selectBest(List(a, b, c))
      bca <- cs.selectBest(List(b, c, a))
      cab <- cs.selectBest(List(c, a, b))
    } yield
      expect.all(
        ab.hash == a.hash,
        bc.hash == b.hash,
        ca.hash == c.hash,
        abc.exists(_.hash == c.hash),
        bca.exists(_.hash == a.hash),
        cab.exists(_.hash == b.hash)
      )
  }

  test("activation blocker: production k1-plus-one lookback keeps a depth k1-plus-one fork on maxvalid-tk") {
    // The walk's forkDepth is the maximum post-MRCA suffix length. For k1=2 this fork has depth 3.
    // The sparse three-block tine wins maxvalid-tk by length; the dense two-block tine wins maxvalid-bg.
    // Production derives kLookback=k1+1 and compare uses bg only when depth > kLookback, so it returns
    // the sparse tine at depth k1+1. Passing kLookback=k1 yields the ratified "beyond k1" density result.
    val g = tip("boundary-g", 0L, 0L, "boundary-root", 0x40)
    val sparse1 = tip("boundary-sparse-1", 10L, 1L, "boundary-g", 0x20)
    val sparse2 = tip("boundary-sparse-2", 20L, 2L, "boundary-sparse-1", 0x21)
    val sparse3 = tip("boundary-sparse-3", 30L, 3L, "boundary-sparse-2", 0x22)
    val dense1 = tip("boundary-dense-1", 1L, 1L, "boundary-g", 0x30)
    val dense2 = tip("boundary-dense-2", 2L, 2L, "boundary-dense-1", 0x31)
    val graph = List(g, sparse1, sparse2, sparse3, dense1, dense2)

    for {
      productionBoundary <- graphSelection(graph, kLookback = 3L, sWindow = 5L)
      ratifiedBoundary <- graphSelection(graph, kLookback = 2L, sWindow = 5L)
      productionWinner <- productionBoundary.compare(sparse3, dense2)
      ratifiedWinner <- ratifiedBoundary.compare(sparse3, dense2)
    } yield
      expect.all(
        productionWinner.hash == sparse3.hash,
        ratifiedWinner.hash == dense2.hash
      )
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

  // ============================================================
  // Track-3 S3 — band-density deep-reorg (flag ON): the density comparator is COMMUTATIVE (true-MRCA
  // anchored) and shouldSwitch's revert floor is the k₂ "settled" ordinal (not the k₁ finalized head).
  // ============================================================
  //
  // A band fork off a common ancestor `gAnc` (ordinal 100). Branch A is 4 blocks in a tight slot window
  // (DENSE); branch B is 2 blocks spread across a wide slot range (SPARSE). Fork depth (4) exceeds the
  // small kLookback (2), so the density rule (maxvalid-bg) — not the tip tiebreak — decides.
  private val gAnc = tip("Gband", 100, 100, "rootband", 0x30)
  private val a1 = tip("a1", 101, 101, "Gband", 0x10)
  private val a2 = tip("a2", 102, 102, "a1", 0x11)
  private val a3 = tip("a3", 103, 103, "a2", 0x12)
  private val a4 = tip("a4", 104, 104, "a3", 0x13)
  private val b1 = tip("b1", 130, 101, "Gband", 0x20)
  private val b2 = tip("b2", 160, 102, "b1", 0x21)
  private val bandGraph: List[ChainTip] = List(gAnc, a1, a2, a3, a4, b1, b2)

  /** ChainSelection over an in-memory chain graph with the band-density flag ON (true-MRCA walk to `maxAncestorDepth`, k₂ settled floor).
    * `fetchParent` resolves parents from the graph by hash.
    */
  private def graphSelection(
    graph: List[ChainTip],
    settled: Long = 0L,
    kLookback: Long = 2L,
    sWindow: Long = 100L,
    maxAncestorDepth: Long = 1000L,
    bandDensityReorgEnabled: Boolean = true
  ): IO[ChainSelection[IO]] = {
    val byHash = graph.map(t => t.hash -> t).toMap
    for {
      registry <- setupRegistry(Set.empty)
      tracker <- TipTracker.make[IO](registry)
    } yield
      ChainSelection.make[IO](
        tracker,
        (t: ChainTip) => IO.pure(byHash.get(t.parentHash)),
        kLookback = kLookback,
        sWindow = sWindow,
        maxAncestorDepth = maxAncestorDepth,
        settledOrdinalReader = Some(IO.pure(settled)),
        bandDensityReorgEnabled = bandDensityReorgEnabled
      )
  }

  test("S3 densityCompare is COMMUTATIVE on a band fork: compare(A,B) == compare(B,A)") {
    // The pre-S3 comparator truncated the ancestor walk at kLookback (BEFORE the true fork point for a
    // band fork) and anchored the density window on the FIRST argument's truncation head, so
    // compare(A,B) ≠ compare(B,A) — nodes on opposite band branches never converged. With the true-MRCA
    // (gAnc) anchor the result is identical in both directions (cluster-uniformity).
    for {
      cs <- graphSelection(bandGraph)
      ab <- cs.compare(a4, b2)
      ba <- cs.compare(b2, a4)
    } yield expect.same(ab.hash, ba.hash)
  }

  test("S3 densityCompare: the DENSER branch wins in the band (maxvalid-bg)") {
    // Branch A packs 5 blocks (gAnc + a1..a4) into the sWindow from the fork point; branch B only 3
    // (gAnc + b1, b2). Denser wins — regardless of argument order.
    for {
      cs <- graphSelection(bandGraph)
      winnerAB <- cs.compare(a4, b2)
      winnerBA <- cs.compare(b2, a4)
    } yield expect.same(a4.hash, winnerAB.hash) && expect.same(a4.hash, winnerBA.hash)
  }

  test("S3 shouldSwitch: switches onto the denser branch when the fork point is ABOVE the settled floor") {
    // settled = 50 < fork point (gAnc, ordinal 100): the reorg reverts only b1/b2 (ords 101,102), both
    // above settled ⇒ the k₂ floor allows it and the denser candidate a4 wins.
    for {
      cs <- graphSelection(bandGraph, settled = 50L)
      switch <- cs.shouldSwitch(b2, a4)
    } yield expect(switch)
  }

  test("S3 shouldSwitch: allows the switch when settled == the fork-point ordinal (fork point is preserved)") {
    // settled = 100 == gAnc.ordinal (the MRCA, common to BOTH branches so it is NOT reverted; the deepest
    // reverted block is b1 at ord 101 > settled). Refuse iff MRCA < settled ⇒ 100 < 100 is false ⇒ allow.
    // This boundary is only correct because the walk found the TRUE MRCA (ord 100) — a kLookback-truncated
    // walk would report no ancestor and wrongly refuse.
    for {
      cs <- graphSelection(bandGraph, settled = 100L)
      switch <- cs.shouldSwitch(b2, a4)
    } yield expect(switch)
  }

  test("S3 shouldSwitch: REFUSES the switch when it would revert a block at/below the settled (k₂) floor") {
    // settled = 101 == b1.ordinal (the deepest block the reorg would revert). MRCA gAnc(100) < settled(101)
    // ⇒ the switch reverts settled history ⇒ refuse. This is the k₂ absolute floor doing its job.
    for {
      cs <- graphSelection(bandGraph, settled = 101L)
      switch <- cs.shouldSwitch(b2, a4)
    } yield expect(!switch)
  }
}
