package io.constellationnetwork.security.mpt

import cats.Show
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import org.scalacheck.Gen
import weaver.MutableIOSuite
import weaver.scalacheck.Checkers

/** Property suite for `MerklePatriciaTrie.withChanges` (#56.3 in the MPT branch refactor).
  *
  * Core invariant: for any `base` trie and any `(upserts, removes)` delta,
  * `base.withChanges(upserts, removes)` must produce a trie whose root hash equals
  * `MerklePatriciaTrie.makeParallelFromBytes(applyDelta(baseEntries, upserts, removes))`.
  *
  * If this property holds, the persistent-trie diff is consensus-equivalent to a fresh
  * full build, which is the gate for using `withChanges` as the `viewAt` materialization
  * primitive in the branch overlay (#56.4).
  */
object MerklePatriciaTrieWithChangesSuite extends MutableIOSuite with Checkers {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] = for {
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (j, h, sp)

  // Weaver's `forall` requires `Show` for the generated type to print failing samples.
  // Byte arrays don't have a useful `Show`, so summarize sizes instead.
  implicit val showScene: Show[(Map[Hex, Array[Byte]], Map[Hex, Array[Byte]], Set[Hex])] =
    Show.show { case (initial, upserts, removes) =>
      s"Scene(initial=${initial.size}, upserts=${upserts.size}, removes=${removes.size})"
    }

  implicit val showRemovesScene: Show[(Map[Hex, Array[Byte]], Set[Hex])] =
    Show.show { case (initial, removes) => s"RemovesScene(initial=${initial.size}, removes=${removes.size})" }

  implicit val showUpsertsScene: Show[(Map[Hex, Array[Byte]], Map[Hex, Array[Byte]])] =
    Show.show { case (initial, upserts) => s"UpsertsScene(initial=${initial.size}, upserts=${upserts.size})" }

  private val hexKeyGen: Gen[Hex] =
    Gen.listOfN(64, Gen.oneOf("0123456789abcdef".toList)).map(cs => Hex(cs.mkString))

  private val valueBytesGen: Gen[Array[Byte]] =
    Gen.chooseNum(1, 32).flatMap(n => Gen.listOfN(n, Gen.chooseNum(Byte.MinValue, Byte.MaxValue)).map(_.toArray))

  /** Initial entries + delta (upserts may overlap removes — withChanges must apply removes first). */
  private val sceneGen: Gen[(Map[Hex, Array[Byte]], Map[Hex, Array[Byte]], Set[Hex])] =
    for {
      nInit <- Gen.chooseNum(0, 12)
      nUp <- Gen.chooseNum(0, 8)
      nRm <- Gen.chooseNum(0, 6)
      initKeys <- Gen.listOfN(nInit, hexKeyGen).map(_.distinct)
      initVals <- Gen.listOfN(initKeys.size, valueBytesGen)
      newKeys <- Gen.listOfN(nUp, hexKeyGen).map(_.distinct)
      newVals <- Gen.listOfN(newKeys.size, valueBytesGen)
      // removes can hit existing keys, new keys, or unknown keys
      rmFromInit <- Gen.someOf(initKeys).map(_.toSet)
      rmExtra <- Gen.listOfN(nRm, hexKeyGen).map(_.toSet)
    } yield {
      val initial = initKeys.zip(initVals).toMap
      val upserts = newKeys.zip(newVals).toMap
      val removes = rmFromInit ++ rmExtra
      (initial, upserts, removes)
    }

  private def applyDelta(
    base: Map[Hex, Array[Byte]],
    upserts: Map[Hex, Array[Byte]],
    removes: Set[Hex]
  ): Map[Hex, Array[Byte]] =
    (base -- removes) ++ upserts

  test("empty delta is identity (rootHash unchanged)") { case (j, h, _) =>
    implicit val js: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    val initial = Map(Hex("ab" * 32) -> "v1".getBytes("UTF-8"))
    for {
      trie <- MerklePatriciaTrie.makeParallelFromBytes[IO](initial)
      result <- trie.withChanges[IO](Map.empty, Set.empty)
    } yield expect.same(trie.rootHash, result.rootHash)
  }

  test("empty delta on empty trie (no entries) is identity") { case (j, h, _) =>
    implicit val js: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    for {
      empty <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map.empty)
      result <- empty.withChanges[IO](Map.empty, Set.empty)
    } yield expect.same(empty.rootHash, result.rootHash)
  }

  test("single insert into empty trie matches fresh build") { case (j, h, _) =>
    implicit val js: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    val key = Hex("ab" * 32)
    val value = "hello".getBytes("UTF-8")
    for {
      empty <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map.empty)
      result <- empty.withChanges[IO](Map(key -> value), Set.empty)
      expected <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map(key -> value))
    } yield expect.same(expected.rootHash, result.rootHash)
  }

  test("remove non-existent key on empty trie yields the empty trie") { case (j, h, _) =>
    implicit val js: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    for {
      empty <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map.empty)
      result <- empty.withChanges[IO](Map.empty, Set(Hex("ff" * 32)))
    } yield expect.same(empty.rootHash, result.rootHash)
  }

  test("remove the only key reduces trie to empty equivalent") { case (j, h, _) =>
    implicit val js: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    val key = Hex("12" * 32)
    val value = "x".getBytes("UTF-8")
    for {
      single <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map(key -> value))
      result <- single.withChanges[IO](Map.empty, Set(key))
      expected <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map.empty)
    } yield expect.same(expected.rootHash, result.rootHash)
  }

  test("remove-then-upsert in same delta yields the new value") { case (j, h, _) =>
    implicit val js: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    val key = Hex("ab" * 32)
    val oldVal = "old".getBytes("UTF-8")
    val newVal = "new-and-different".getBytes("UTF-8")
    for {
      base <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map(key -> oldVal))
      result <- base.withChanges[IO](Map(key -> newVal), Set(key))
      expected <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map(key -> newVal))
    } yield expect.same(expected.rootHash, result.rootHash)
  }

  test("prefix-collision keys: insertion produces extension+branch, root matches fresh build") {
    case (j, h, _) =>
      implicit val js: JsonSerializer[IO] = j
      implicit val hh: Hasher[IO] = h
      // Two keys sharing a 32-nibble prefix, diverging at nibble 32 ("0" vs "f").
      val sharedPrefix = "ab" * 16
      val k1 = Hex(sharedPrefix + "0" + "00" * 15 + "0")
      val k2 = Hex(sharedPrefix + "f" + "00" * 15 + "0")
      val v1 = "alpha".getBytes("UTF-8")
      val v2 = "beta".getBytes("UTF-8")
      for {
        empty <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map.empty)
        result <- empty.withChanges[IO](Map(k1 -> v1, k2 -> v2), Set.empty)
        expected <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map(k1 -> v1, k2 -> v2))
      } yield expect.same(expected.rootHash, result.rootHash)
  }

  test("branch-to-extension collapse: remove one of two prefix-collision keys") { case (j, h, _) =>
    implicit val js: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    val sharedPrefix = "ab" * 16
    val k1 = Hex(sharedPrefix + "0" + "00" * 15 + "0")
    val k2 = Hex(sharedPrefix + "f" + "00" * 15 + "0")
    val v1 = "alpha".getBytes("UTF-8")
    val v2 = "beta".getBytes("UTF-8")
    for {
      both <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map(k1 -> v1, k2 -> v2))
      result <- both.withChanges[IO](Map.empty, Set(k2))
      expected <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map(k1 -> v1))
    } yield expect.same(expected.rootHash, result.rootHash)
  }

  test("update-existing-key changes the root and matches fresh rebuild") { case (j, h, _) =>
    implicit val js: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    val key = Hex("ab" * 32)
    val v1 = "v1".getBytes("UTF-8")
    val v2 = "v2-different-content".getBytes("UTF-8")
    for {
      base <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map(key -> v1))
      result <- base.withChanges[IO](Map(key -> v2), Set.empty)
      expected <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map(key -> v2))
      _ <- IO(())
    } yield expect.same(expected.rootHash, result.rootHash) &&
      expect(base.rootHash =!= result.rootHash)
  }

  test("PROPERTY: withChanges == makeParallelFromBytes(applyDelta(base, delta))") { case (j, h, _) =>
    implicit val js: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    forall(sceneGen) { case (initial, upserts, removes) =>
      for {
        base <- MerklePatriciaTrie.makeParallelFromBytes[IO](initial)
        result <- base.withChanges[IO](upserts, removes)
        expected <- MerklePatriciaTrie.makeParallelFromBytes[IO](applyDelta(initial, upserts, removes))
      } yield expect.same(expected.rootHash, result.rootHash)
    }
  }

  test("PROPERTY: applying removes-only matches fresh rebuild without those keys") { case (j, h, _) =>
    implicit val js: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    val gen = for {
      n <- Gen.chooseNum(1, 12)
      keys <- Gen.listOfN(n, hexKeyGen).map(_.distinct)
      values <- Gen.listOfN(keys.size, valueBytesGen)
      rms <- Gen.someOf(keys).map(_.toSet)
    } yield (keys.zip(values).toMap, rms)

    forall(gen) { case (initial, removes) =>
      for {
        base <- MerklePatriciaTrie.makeParallelFromBytes[IO](initial)
        result <- base.withChanges[IO](Map.empty, removes)
        expected <- MerklePatriciaTrie.makeParallelFromBytes[IO](initial -- removes)
      } yield expect.same(expected.rootHash, result.rootHash)
    }
  }

  test("PROPERTY: applying upserts-only matches fresh rebuild with merged map") { case (j, h, _) =>
    implicit val js: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    val gen = for {
      n0 <- Gen.chooseNum(0, 8)
      n1 <- Gen.chooseNum(0, 8)
      ks0 <- Gen.listOfN(n0, hexKeyGen).map(_.distinct)
      vs0 <- Gen.listOfN(ks0.size, valueBytesGen)
      ks1 <- Gen.listOfN(n1, hexKeyGen).map(_.distinct)
      vs1 <- Gen.listOfN(ks1.size, valueBytesGen)
    } yield (ks0.zip(vs0).toMap, ks1.zip(vs1).toMap)

    forall(gen) { case (initial, upserts) =>
      for {
        base <- MerklePatriciaTrie.makeParallelFromBytes[IO](initial)
        result <- base.withChanges[IO](upserts, Set.empty)
        expected <- MerklePatriciaTrie.makeParallelFromBytes[IO](initial ++ upserts)
      } yield expect.same(expected.rootHash, result.rootHash)
    }
  }
}
