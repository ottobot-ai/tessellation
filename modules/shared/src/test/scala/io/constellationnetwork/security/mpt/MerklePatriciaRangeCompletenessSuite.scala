package io.constellationnetwork.security.mpt

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security._
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.prover.MerklePatriciaRangeProver
import io.constellationnetwork.security.mpt.verifier.MerklePatriciaRangeVerifier
import io.constellationnetwork.shared.sharedKryoRegistrar

import weaver.MutableIOSuite

/** Range-completeness (set-soundness) tests for the #286 strengthening of [[MerklePatriciaRangeVerifier]].
  *
  * The point of these tests is the OMISSION case: a range proof from which an in-range leaf has been dropped must now be REJECTED (it
  * passed before the strengthening). The previously-valid complete proofs must still verify.
  */
object MerklePatriciaRangeCompletenessSuite extends MutableIOSuite {

  type Res = (HasherSelector[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    KryoSerializer.forAsync[IO](sharedKryoRegistrar).flatMap { implicit kryo =>
      JsonSerializer.forAsync[IO].asResource.map { implicit json =>
        (
          HasherSelector.forSync[IO](
            Hasher.forJson[IO],
            Hasher.forKryo[IO],
            hashSelect = new HashSelect { def select(ordinal: SnapshotOrdinal): HashLogic = KryoHash }
          ),
          json
        )
      }
    }

  private def k(prefix: String): Hex = Hex(prefix.padTo(64, '0'))

  // Keys diverging at the first nibble ⇒ root is a single Branch with one child per key.
  private val allKeys = List("10", "20", "30", "40", "90").map(k)

  private def buildTrie(implicit hasher: Hasher[IO]): IO[MerklePatriciaTrie] =
    for {
      entries <- allKeys.traverse(key => hasher.hash(s"value_${key.value}").map(_ => key -> s"value_${key.value}"))
      trie <- MerklePatriciaTrie.make(entries.toMap)
    } yield trie

  test("complete range proof verifies (additive strengthening keeps valid proofs valid)") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val lo = k("20")
      val hi = k("40") // in-range keys: 20, 30, 40
      for {
        trie <- buildTrie
        prover = MerklePatriciaRangeProver.make[IO](trie)
        proof <- prover.attestRange(lo, hi).flatMap(IO.fromEither)
        verifier = MerklePatriciaRangeVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmRange(proof)
      } yield
        expect.all(
          result.isRight,
          proof.inclusionProofs.length == 3
        )
    }
  }

  test("range proof with an in-range leaf OMITTED is rejected (the soundness fix)") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val lo = k("20")
      val hi = k("40")
      for {
        trie <- buildTrie
        prover = MerklePatriciaRangeProver.make[IO](trie)
        proof <- prover.attestRange(lo, hi).flatMap(IO.fromEither)
        // Drop the middle in-range key (30...). Ordering + boundary checks still pass — only the
        // completeness check should catch this.
        omittedKey = k("30")
        forged = proof.copy(inclusionProofs = proof.inclusionProofs.filterNot(_.path == omittedKey))
        verifier = MerklePatriciaRangeVerifier.make[IO](trie.rootNode.digest)
        completeResult <- verifier.confirmRange(proof)
        forgedResult <- verifier.confirmRange(forged)
      } yield
        expect.all(
          completeResult.isRight,
          forged.inclusionProofs.length == 2,
          forgedResult.isLeft
        )
    }
  }

  test("range proof with the first in-range leaf omitted is rejected") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val lo = k("20")
      val hi = k("40")
      for {
        trie <- buildTrie
        prover = MerklePatriciaRangeProver.make[IO](trie)
        proof <- prover.attestRange(lo, hi).flatMap(IO.fromEither)
        forged = proof.copy(inclusionProofs = proof.inclusionProofs.filterNot(_.path == k("20")))
        verifier = MerklePatriciaRangeVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmRange(forged)
      } yield expect(result.isLeft)
    }
  }

  test("range proof with the last in-range leaf omitted is rejected") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val lo = k("20")
      val hi = k("40")
      for {
        trie <- buildTrie
        prover = MerklePatriciaRangeProver.make[IO](trie)
        proof <- prover.attestRange(lo, hi).flatMap(IO.fromEither)
        forged = proof.copy(inclusionProofs = proof.inclusionProofs.filterNot(_.path == k("40")))
        verifier = MerklePatriciaRangeVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmRange(forged)
      } yield expect(result.isLeft)
    }
  }

  test("genuinely-empty range with boundaries verifies (no leaf in gap)") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      // Range strictly between 40 and 90 — no keys present, both boundaries exist.
      val lo = k("50")
      val hi = k("80")
      for {
        trie <- buildTrie
        prover = MerklePatriciaRangeProver.make[IO](trie)
        proof <- prover.attestRange(lo, hi).flatMap(IO.fromEither)
        verifier = MerklePatriciaRangeVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmRange(proof)
      } yield
        expect.all(
          result.isRight,
          proof.inclusionProofs.isEmpty
        )
    }
  }

  test("full-span range over all keys verifies and returns all keys") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val lo = k("00")
      val hi = k("ff")
      for {
        trie <- buildTrie
        prover = MerklePatriciaRangeProver.make[IO](trie)
        proof <- prover.attestRange(lo, hi).flatMap(IO.fromEither)
        verifier = MerklePatriciaRangeVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmRange(proof)
      } yield
        expect.all(
          result.isRight,
          proof.inclusionProofs.length == allKeys.length
        )
    }
  }

  test("full-span range with an interior key omitted is rejected") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val lo = k("00")
      val hi = k("ff")
      for {
        trie <- buildTrie
        prover = MerklePatriciaRangeProver.make[IO](trie)
        proof <- prover.attestRange(lo, hi).flatMap(IO.fromEither)
        forged = proof.copy(inclusionProofs = proof.inclusionProofs.filterNot(_.path == k("30")))
        verifier = MerklePatriciaRangeVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmRange(forged)
      } yield expect(result.isLeft)
    }
  }

  // Keys sharing a deep common prefix so omission must be caught at a NON-root branch (recursive descent).
  test("omission under a deep shared prefix (non-root branch) is rejected; complete still verifies") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      // All share "abcd"; diverge at nibble 5. Root = Extension("abcd") -> Branch{0,1,2,9}.
      val deepKeys = List("abcd0", "abcd1", "abcd2", "abcd9").map(k)
      val lo = k("abcd0")
      val hi = k("abcd2") // in-range: abcd0, abcd1, abcd2 ; abcd9 is the right boundary
      for {
        entries <- deepKeys.traverse(key => hasher.hash(s"v_${key.value}").map(_ => key -> s"v_${key.value}"))
        trie <- MerklePatriciaTrie.make(entries.toMap)
        prover = MerklePatriciaRangeProver.make[IO](trie)
        proof <- prover.attestRange(lo, hi).flatMap(IO.fromEither)
        verifier = MerklePatriciaRangeVerifier.make[IO](trie.rootNode.digest)
        completeResult <- verifier.confirmRange(proof)
        forged = proof.copy(inclusionProofs = proof.inclusionProofs.filterNot(_.path == k("abcd1")))
        forgedResult <- verifier.confirmRange(forged)
      } yield
        expect.all(
          completeResult.isRight,
          proof.inclusionProofs.length == 3,
          forgedResult.isLeft
        )
    }
  }
}
