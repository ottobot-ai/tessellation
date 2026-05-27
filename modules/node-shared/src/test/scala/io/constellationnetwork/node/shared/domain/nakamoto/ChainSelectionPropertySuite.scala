package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.infrastructure.metrics.NoOpMetrics
import io.constellationnetwork.schema.nakamoto.ChainTip
import io.constellationnetwork.schema.nakamoto.slot.{Slot, VrfOutput}
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong
import org.scalacheck.Gen
import weaver.SimpleIOSuite
import weaver.scalacheck.{CheckConfig, Checkers}

/** Property + determinism tests for the structural fork-choice comparator and the global-argmax selection path.
  *
  * Two coverage gaps this suite closes (audit, May 2026):
  *   - `standardCompare` was only exercised on a handful of hand-picked tips. The total-order properties a fork-choice rule MUST satisfy
  *     (antisymmetry across the full ordinal → slot → VRF fall-through, 3-way transitivity, field-only determinism) had no property
  *     coverage.
  *   - `selectBest` (the global argmax now driving `NakamotoChainStore.store`) had no permutation-stability test — the very property the
  *     prior pairwise `shouldSwitch` path lacked. If the comparator is a true total order, the argmax is invariant under input order.
  *   - `densityCompare` (long-fork ≥ kLookback path) had an antisymmetry hole: the window origin was taken from one tine's head only, so
  *     `compare(A,B)` and `compare(B,A)` could disagree when `buildTines` exits via the depth-bound branch (heads not yet converged).
  *
  * '''Total order without ties.''' Every generated tip is given a UNIQUE VRF output (index-encoded), so even when two tips collide on
  * ordinal and slot the VRF tiebreak is strict. This is the regime fork choice actually operates in (VRF outputs are 64-byte hashes —
  * collisions are cryptographically negligible) and it makes the comparator a strict total order, so antisymmetry / transitivity / argmax
  * stability all hold with no residual-tie carve-outs.
  */
object ChainSelectionPropertySuite extends SimpleIOSuite with Checkers {

  implicit private val metrics: io.constellationnetwork.node.shared.infrastructure.metrics.Metrics[IO] = NoOpMetrics.make

  // The permutation test fans out to n! orderings per sample; 40 samples is ample to exercise the comparator
  // fall-through legs without an unnecessarily long suite.
  override def checkConfig: CheckConfig = CheckConfig.default.copy(minimumSuccessful = 40)

  // A single shared parent for the short-fork regime. With a no-op `fetchParent`, every pair of distinct
  // tips sharing this parent routes through `buildTines` → (exceedsK = false) → `standardCompare`, so these
  // generators exercise the maxvalid-tk comparator without needing ancestor traversal.
  private val commonParent: Hash = Hash("parent".padTo(64, '0'))

  private def slot(n: Long): Slot = Slot(NonNegLong.unsafeFrom(n))

  // Unique-per-index 64-byte VRF (big-endian index in the low 8 bytes). Distinct indices ⇒ distinct unsigned
  // BigInt ⇒ strict VRF tiebreak, so no two generated tips are comparator-equal.
  private def vrfFromIndex(i: Long): VrfOutput = {
    val bytes = Array.fill[Byte](64)(0)
    var v = i
    var pos = 63
    while (pos >= 56) {
      bytes(pos) = (v & 0xffL).toByte
      v >>>= 8
      pos -= 1
    }
    VrfOutput.fromBytes(bytes)
  }

  private def tipAt(index: Long, ordinal: Long, slotNum: Long): ChainTip =
    ChainTip(
      hash = Hash(s"tip$index".padTo(64, '0')),
      slot = slot(slotNum),
      ordinal = ordinal,
      parentHash = commonParent,
      vrfOutput = vrfFromIndex(index)
    )

  private def setupChainSelection: IO[ChainSelection[IO]] =
    for {
      registry <- StakeRegistry.equalWeight[IO]
      tracker <- TipTracker.make[IO](registry)
      cs = ChainSelection.make[IO](tracker, _ => IO.pure(None))
    } yield cs

  // ---- generators -----------------------------------------------------------------------------------

  // Small ordinal/slot ranges so the generator frequently produces equal-ordinal and equal-slot collisions,
  // forcing the comparator down the slot and VRF fall-through legs. Index is the unique identity (hash + VRF).
  private def genTip(index: Long): Gen[ChainTip] =
    for {
      ordinal <- Gen.choose(100L, 104L)
      slotNum <- Gen.choose(10L, 14L)
    } yield tipAt(index, ordinal, slotNum)

  private val genPair: Gen[(ChainTip, ChainTip)] =
    for {
      a <- genTip(1L)
      b <- genTip(2L)
    } yield (a, b)

  private val genTriple: Gen[(ChainTip, ChainTip, ChainTip)] =
    for {
      a <- genTip(1L)
      b <- genTip(2L)
      c <- genTip(3L)
    } yield (a, b, c)

  // A list of 2..6 distinct tips (distinct indices ⇒ distinct hashes + VRFs).
  private val genTipList: Gen[List[ChainTip]] =
    for {
      n <- Gen.choose(2, 6)
      tips <- Gen.sequence[List[ChainTip], ChainTip]((1L to n.toLong).map(i => genTip(i)))
    } yield tips

  // ---- standardCompare: antisymmetry across the full fall-through ------------------------------------

  test("standardCompare antisymmetry (random pairs, full ordinal→slot→VRF fall-through): compare(a,b) == compare(b,a)") {
    forall(genPair) {
      case (a, b) =>
        setupChainSelection.flatMap { cs =>
          for {
            ab <- cs.compare(a, b)
            ba <- cs.compare(b, a)
          } yield expect.same(ab.hash, ba.hash)
        }
    }
  }

  test("standardCompare antisymmetry — equal ordinal, equal slot, different VRF (terminal VRF leg)") {
    // Force every pair onto the VRF tiebreak: identical ordinal + slot, distinct VRF (distinct index).
    val gen = Gen.choose(100L, 104L).flatMap { ord =>
      Gen.choose(10L, 14L).map { s =>
        (tipAt(1L, ord, s), tipAt(2L, ord, s))
      }
    }
    forall(gen) {
      case (a, b) =>
        setupChainSelection.flatMap { cs =>
          for {
            ab <- cs.compare(a, b)
            ba <- cs.compare(b, a)
          } yield expect.same(ab.hash, ba.hash)
        }
    }
  }

  test("standardCompare antisymmetry — equal ordinal, different slot (slot leg)") {
    // Construct distinct slots without a `suchThat` filter (which discards too many inputs): sB = sA + delta
    // with delta ≥ 1 guarantees sB ≠ sA, so every sample routes through the slot leg.
    val gen = for {
      ord <- Gen.choose(100L, 104L)
      sA <- Gen.choose(10L, 14L)
      delta <- Gen.choose(1L, 5L)
    } yield (tipAt(1L, ord, sA), tipAt(2L, ord, sA + delta))
    forall(gen) {
      case (a, b) =>
        setupChainSelection.flatMap { cs =>
          for {
            ab <- cs.compare(a, b)
            ba <- cs.compare(b, a)
          } yield expect.same(ab.hash, ba.hash)
        }
    }
  }

  // ---- standardCompare: 3-way transitivity -----------------------------------------------------------

  test("standardCompare transitivity: preferred(a,b)==a && preferred(b,c)==b ⇒ preferred(a,c)==a") {
    forall(genTriple) {
      case (a, b, c) =>
        setupChainSelection.flatMap { cs =>
          for {
            ab <- cs.compare(a, b).map(_.hash)
            bc <- cs.compare(b, c).map(_.hash)
            ac <- cs.compare(a, c).map(_.hash)
          } yield
            if (ab === a.hash && bc === b.hash) expect.same(a.hash, ac)
            else expect(true) // antecedent false — vacuously satisfied
        }
    }
  }

  // ---- standardCompare: determinism from fields only -------------------------------------------------

  test("compare is deterministic — same inputs yield same winner across repeated calls") {
    forall(genPair) {
      case (a, b) =>
        setupChainSelection.flatMap { cs =>
          for {
            r1 <- cs.compare(a, b).map(_.hash)
            r2 <- cs.compare(a, b).map(_.hash)
            r3 <- cs.compare(a, b).map(_.hash)
          } yield expect.same(r1, r2) && expect.same(r2, r3)
        }
    }
  }

  test("compare depends only on (ordinal, slot, VRF) — relabeling the hash does not change which fields win") {
    // Two tips identical in ordinal/slot/VRF to (a, b) but with different hash labels must produce a winner
    // whose ordinal/slot/VRF equals the original winner's. (Hash is the identity returned, never a tiebreak input.)
    val gen = for {
      ordA <- Gen.choose(100L, 104L)
      slotA <- Gen.choose(10L, 14L)
      ordB <- Gen.choose(100L, 104L)
      slotB <- Gen.choose(10L, 14L)
    } yield (ordA, slotA, ordB, slotB)
    forall(gen) {
      case (ordA, slotA, ordB, slotB) =>
        setupChainSelection.flatMap { cs =>
          // indices 1/2 vs 7/8 give different hashes AND different VRFs; to isolate the "hash doesn't matter"
          // claim we keep the VRF the same across the relabeling by reusing the same index for VRF identity.
          val a = ChainTip(Hash("a".padTo(64, '0')), slot(slotA), ordA, commonParent, vrfFromIndex(1L))
          val b = ChainTip(Hash("b".padTo(64, '0')), slot(slotB), ordB, commonParent, vrfFromIndex(2L))
          val a2 = ChainTip(Hash("aa".padTo(64, '0')), slot(slotA), ordA, commonParent, vrfFromIndex(1L))
          val b2 = ChainTip(Hash("bb".padTo(64, '0')), slot(slotB), ordB, commonParent, vrfFromIndex(2L))
          for {
            w <- cs.compare(a, b)
            w2 <- cs.compare(a2, b2)
          } yield
            // winner field-tuple must match even though hashes differ
            expect.same((w.ordinal, w.slot, w.vrfOutput), (w2.ordinal, w2.slot, w2.vrfOutput))
        }
    }
  }

  // ---- selectBest: argmax stability (permutation invariance) -----------------------------------------

  test("selectBest argmax stability — same head regardless of insertion/arrival order (all permutations)") {
    forall(genTipList) { tips =>
      setupChainSelection.flatMap { cs =>
        // Distinct indices guarantee distinct hashes; dedupe defensively in case a generator repeats.
        val distinct = tips.distinctBy(_.hash)
        cs.selectBest(distinct).flatMap { reference =>
          distinct.permutations.toList.traverse(cs.selectBest).map { results =>
            val refHash = reference.map(_.hash)
            expect(results.forall(_.map(_.hash) === refHash))
          }
        }
      }
    }
  }

  test("selectBest equals the pairwise-max — argmax winner beats every other candidate") {
    forall(genTipList) { tips =>
      setupChainSelection.flatMap { cs =>
        val distinct = tips.distinctBy(_.hash)
        cs.selectBest(distinct).flatMap {
          case None => IO.pure(expect(distinct.isEmpty))
          case Some(best) =>
            distinct.traverse(t => cs.compare(best, t).map(_.hash === best.hash)).map { winsAll =>
              expect(winsAll.forall(identity))
            }
        }
      }
    }
  }

  // ---- densityCompare: antisymmetry at fork-depth ≥ kLookback ----------------------------------------

  // The long-fork (density) path is reached when `buildTines` exits via the depth bound BEFORE the tines
  // converge. We force that with kLookback = 0 and a map-backed `fetchParent`: at depth 0 the heads differ
  // and `0 > 0` is false, so each tine walks back one hop (depth 1), then `1 > 0` is true ⇒ density path with
  // tineA = [parentA, tipA], tineB = [parentB, tipB]. The two tine heads (parentA, parentB) are DISTINCT
  // snapshots (no common ancestor in the built tines) and carry DIFFERENT slots — exactly the configuration
  // where an asymmetric window origin (one tine's head) produces compare(A,B) ≠ compare(B,A).
  //
  // Slots are chosen (sWindow = 10) so the PRIOR A-biased origin disagreed across argument order — verified by
  // brute force over the slot grid:
  //   - origin = parentA.slot = 0  ⇒ window [.., 10]: densityA([0,11]) = 1, densityB([1,10]) = 2 ⇒ tipB wins.
  //   - origin = parentB.slot = 1  ⇒ window [.., 11]: density(B-tine [1,10]) = 2, density(A-tine [0,11]) = 2 ⇒
  //     tie ⇒ VRF picks tipA. ⇒ OLD compare(A,B)=B but compare(B,A)=A (antisymmetry violation).
  // The symmetric origin min(parentA.slot, parentB.slot) = 0 is used for BOTH orderings, so both pick tipB.
  private val parentA = ChainTip(Hash("pA".padTo(64, '0')), slot(0L), 98L, Hash("gpA".padTo(64, '0')), vrfFromIndex(10L))
  private val parentB = ChainTip(Hash("pB".padTo(64, '0')), slot(1L), 98L, Hash("gpB".padTo(64, '0')), vrfFromIndex(11L))
  private val tipA = ChainTip(Hash("tA".padTo(64, '0')), slot(11L), 99L, parentA.hash, vrfFromIndex(12L))
  private val tipB = ChainTip(Hash("tB".padTo(64, '0')), slot(10L), 99L, parentB.hash, vrfFromIndex(13L))

  private val densityFetchParent: ChainTip => IO[Option[ChainTip]] = {
    val byHash: SortedMap[String, ChainTip] = SortedMap(
      parentA.hash.value -> parentA,
      parentB.hash.value -> parentB,
      tipA.hash.value -> tipA,
      tipB.hash.value -> tipB
    )
    (t: ChainTip) => IO.pure(byHash.get(t.parentHash.value))
  }

  private def setupDensitySelection: IO[ChainSelection[IO]] =
    for {
      registry <- StakeRegistry.equalWeight[IO]
      tracker <- TipTracker.make[IO](registry)
      // kLookback = 0 forces the density path after one walk-back hop; sWindow = 10 is the window where the
      // prior A-biased origin disagreed across argument order (see fixture comment above).
      cs = ChainSelection.make[IO](tracker, densityFetchParent, kLookback = 0L, sWindow = 10L)
    } yield cs

  test("densityCompare antisymmetry at fork-depth ≥ kLookback: compare(A,B) == compare(B,A)") {
    setupDensitySelection.flatMap { cs =>
      for {
        ab <- cs.compare(tipA, tipB)
        ba <- cs.compare(tipB, tipA)
      } yield expect.same(ab.hash, ba.hash)
    }
  }

  test("densityCompare is reached (sanity): the denser equal-ordinal long fork wins deterministically (tipB)") {
    // Guards against the test silently exercising the short-fork path instead of density. Equal ordinal +
    // kLookback = 0 means the only paths that can pick a winner are density (then VRF). With the symmetric
    // origin min(0,1)=0 and window [..,10]: tipB's tine has 2 in-window blocks vs tipA's 1, so tipB wins by
    // density on BOTH orderings (asserted above) — and specifically it is tipB, not the A-biased result.
    setupDensitySelection.flatMap { cs =>
      cs.compare(tipA, tipB).map(w => expect.same(tipB.hash, w.hash))
    }
  }
}
