package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.Show
import cats.effect.IO
import cats.syntax.all._

import io.constellationnetwork.security.hex.Hex

import org.scalacheck.{Arbitrary, Gen}
import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers

/** Property suite for ChangeSet (#56.1). Verifies merge semantics and the apply-equivalence law:
  *
  * `applyDelta(applyDelta(base, a), b) == applyDelta(base, a.merge(b))`
  *
  * where `applyDelta(m, ch) = (m -- ch.removals) ++ ch.upserts`. This law is the contract that lets the overlay accumulate per-branch
  * deltas via merge and then materialize once via `MerklePatriciaTrie.withChanges`.
  */
object ChangeSetSuite extends SimpleIOSuite with Checkers {

  // Equality on Array[Byte] is reference-based; lift to Seq[Byte] for structural compare.
  private def normalize(cs: ChangeSet): (Map[Hex, Seq[Byte]], Set[Hex]) =
    (cs.upserts.view.mapValues(_.toSeq).toMap, cs.removals)

  private val hexKeyGen: Gen[Hex] =
    Gen.listOfN(8, Gen.oneOf("0123456789abcdef".toList)).map(cs => Hex(cs.mkString))

  private val valueBytesGen: Gen[Array[Byte]] =
    Gen.chooseNum(1, 8).flatMap(n => Gen.listOfN(n, Gen.chooseNum(Byte.MinValue, Byte.MaxValue)).map(_.toArray))

  implicit val arbChangeSet: Arbitrary[ChangeSet] = Arbitrary(
    for {
      nUp <- Gen.chooseNum(0, 5)
      nRm <- Gen.chooseNum(0, 5)
      upKeys <- Gen.listOfN(nUp, hexKeyGen).map(_.distinct)
      upVals <- Gen.listOfN(upKeys.size, valueBytesGen)
      rmKeys <- Gen.listOfN(nRm, hexKeyGen).map(_.toSet)
    } yield ChangeSet(upKeys.zip(upVals).toMap, rmKeys)
  )

  implicit val showChangeSet: Show[ChangeSet] =
    Show.show(cs => s"ChangeSet(upserts=${cs.upserts.size}, removals=${cs.removals.size})")

  pureTest("empty ChangeSet has no mutations") {
    expect(ChangeSet.empty.isEmpty) &&
    expect.same(0, ChangeSet.empty.size) &&
    expect.same(Map.empty[Hex, Array[Byte]], ChangeSet.empty.upserts) &&
    expect.same(Set.empty[Hex], ChangeSet.empty.removals)
  }

  test("merge with empty is identity (left)") {
    forall { (a: ChangeSet) =>
      expect.same(normalize(a), normalize(ChangeSet.empty.merge(a)))
    }
  }

  test("merge with empty is identity (right)") {
    forall { (a: ChangeSet) =>
      expect.same(normalize(a), normalize(a.merge(ChangeSet.empty)))
    }
  }

  test("merge result has no overlap between upserts and removals") {
    forall { (a: ChangeSet, b: ChangeSet) =>
      val merged = a.merge(b)
      val overlap = merged.upserts.keySet.intersect(merged.removals)
      expect(overlap.isEmpty)
    }
  }

  pureTest("merge: other.removals cancel matching upserts in this") {
    val k = Hex("ab")
    val a = ChangeSet(upserts = Map(k -> Array[Byte](1, 2, 3)), removals = Set.empty)
    val b = ChangeSet(upserts = Map.empty, removals = Set(k))
    val merged = a.merge(b)
    expect(!merged.upserts.contains(k)) &&
    expect(merged.removals.contains(k))
  }

  pureTest("merge: other.upserts cancel matching removals in this") {
    val k = Hex("cd")
    val newVal = Array[Byte](9, 9, 9)
    val a = ChangeSet(upserts = Map.empty, removals = Set(k))
    val b = ChangeSet(upserts = Map(k -> newVal), removals = Set.empty)
    val merged = a.merge(b)
    expect(!merged.removals.contains(k)) &&
    expect.same(newVal.toSeq, merged.upserts(k).toSeq)
  }

  pureTest("merge: same upsert key in both — other wins") {
    val k = Hex("ef")
    val v1 = Array[Byte](1)
    val v2 = Array[Byte](2)
    val a = ChangeSet(Map(k -> v1), Set.empty)
    val b = ChangeSet(Map(k -> v2), Set.empty)
    val merged = a.merge(b)
    expect.same(v2.toSeq, merged.upserts(k).toSeq)
  }

  test("PROPERTY: merge is associative") {
    forall { (a: ChangeSet, b: ChangeSet, c: ChangeSet) =>
      val left = a.merge(b).merge(c)
      val right = a.merge(b.merge(c))
      expect.same(normalize(left), normalize(right))
    }
  }

  test("PROPERTY: applyDelta(applyDelta(base, a), b) == applyDelta(base, a.merge(b))") {
    val baseGen: Gen[Map[Hex, Seq[Byte]]] = for {
      n <- Gen.chooseNum(0, 8)
      keys <- Gen.listOfN(n, hexKeyGen).map(_.distinct)
      vals <- Gen.listOfN(keys.size, valueBytesGen.map(_.toSeq))
    } yield keys.zip(vals).toMap

    def applyDelta(base: Map[Hex, Seq[Byte]], cs: ChangeSet): Map[Hex, Seq[Byte]] =
      (base -- cs.removals) ++ cs.upserts.view.mapValues(_.toSeq).toMap

    val combined: Gen[(Map[Hex, Seq[Byte]], ChangeSet, ChangeSet)] =
      for {
        base <- baseGen
        a <- arbChangeSet.arbitrary
        b <- arbChangeSet.arbitrary
      } yield (base, a, b)

    implicit val showCombined: Show[(Map[Hex, Seq[Byte]], ChangeSet, ChangeSet)] =
      Show.show { case (b, a1, a2) => s"(base=${b.size}, a=${a1.size}, b=${a2.size})" }

    forall(combined) {
      case (base, a, b) =>
        val sequential = applyDelta(applyDelta(base, a), b)
        val merged = applyDelta(base, a.merge(b))
        expect.same(sequential, merged)
    }
  }
}
