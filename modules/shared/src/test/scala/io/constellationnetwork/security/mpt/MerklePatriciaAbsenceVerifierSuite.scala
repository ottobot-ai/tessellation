package io.constellationnetwork.security.mpt

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security._
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.prover.attestation.{AbsenceTermination, MerklePatriciaAbsenceProof}
import io.constellationnetwork.security.mpt.prover.{MerklePatriciaAbsenceProver, MerklePatriciaSingleInclusionProver}
import io.constellationnetwork.security.mpt.verifier.{MerklePatriciaAbsenceVerifier, MerklePatriciaInclusionVerifier}
import io.constellationnetwork.shared.sharedKryoRegistrar

import weaver.MutableIOSuite

object MerklePatriciaAbsenceVerifierSuite extends MutableIOSuite {

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

  // Fixed-length (64-nibble) keys with controlled prefixes so we can exercise each termination case deterministically.
  private def k(prefix: String): Hex = Hex(prefix.padTo(64, '0'))

  // Trie shape: root Extension("a") -> Branch{ 0 -> leaf, 1 -> leaf } (both leaves' remaining = "0"*62)
  private val keyA = k("a0") // a,0,0,...,0
  private val keyB = k("a1") // a,1,0,...,0

  private def buildTrie(implicit hasher: Hasher[IO]): IO[MerklePatriciaTrie] =
    for {
      entries <- List(keyA, keyB).traverse(key => hasher.hash(s"value_${key.value}").map(_ => key -> s"value_${key.value}"))
      trie <- MerklePatriciaTrie.make(entries.toMap)
    } yield trie

  test("absent key via empty branch slot verifies as absent") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val absent = k("a2") // descends Extension("a"), then branch slot 2 is empty
      for {
        trie <- buildTrie
        prover = MerklePatriciaAbsenceProver.make[IO](trie)
        proof <- prover.attestAbsence(absent).flatMap(IO.fromEither)
        verifier = MerklePatriciaAbsenceVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmAbsence(proof)
      } yield
        expect.all(
          result.isRight,
          proof.termination match {
            case AbsenceTermination.BranchEmptySlot(_) => true
            case _                                     => false
          }
        )
    }
  }

  test("absent key via extension divergence verifies as absent") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val absent = k("b0") // diverges from root Extension("a") at first nibble
      for {
        trie <- buildTrie
        prover = MerklePatriciaAbsenceProver.make[IO](trie)
        proof <- prover.attestAbsence(absent).flatMap(IO.fromEither)
        verifier = MerklePatriciaAbsenceVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmAbsence(proof)
      } yield
        expect.all(
          result.isRight,
          proof.termination == AbsenceTermination.ExtensionDivergence
        )
    }
  }

  test("absent key via leaf mismatch verifies as absent") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val absent = Hex("a0".padTo(63, '0') + "1") // follows path to leaf at slot 0, but last nibble differs
      for {
        trie <- buildTrie
        prover = MerklePatriciaAbsenceProver.make[IO](trie)
        proof <- prover.attestAbsence(absent).flatMap(IO.fromEither)
        verifier = MerklePatriciaAbsenceVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmAbsence(proof)
      } yield
        expect.all(
          result.isRight,
          proof.termination == AbsenceTermination.LeafMismatch
        )
    }
  }

  test("absent key in an empty trie verifies as absent (empty root branch)") { implicit res =>
    implicit val (hs, js) = res
    hs.withCurrent { implicit hasher =>
      val absent = k("a0")
      for {
        // Empty trie = empty Branch root.
        empty <- MerklePatriciaTrie.makeParallelFromBytes[IO](Map.empty[Hex, Array[Byte]])
        prover = MerklePatriciaAbsenceProver.make[IO](empty)
        proof <- prover.attestAbsence(absent).flatMap(IO.fromEither)
        verifier = MerklePatriciaAbsenceVerifier.make[IO](empty.rootNode.digest)
        result <- verifier.confirmAbsence(proof)
      } yield expect(result.isRight)
    }
  }

  test("present key via inclusion proof verifies (sanity — inclusion path intact)") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      for {
        trie <- buildTrie
        prover = MerklePatriciaSingleInclusionProver.make[IO](trie)
        proof <- prover.attestPath(keyA).flatMap(IO.fromEither)
        verifier = MerklePatriciaInclusionVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirm(proof)
      } yield expect(result.isRight)
    }
  }

  test("prover refuses to prove absence of a present key") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      for {
        trie <- buildTrie
        prover = MerklePatriciaAbsenceProver.make[IO](trie)
        result <- prover.attestAbsence(keyA)
      } yield expect(result.isLeft)
    }
  }

  test("tampered termination tag fails verification") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val absent = k("a2") // genuinely a BranchEmptySlot termination
      for {
        trie <- buildTrie
        prover = MerklePatriciaAbsenceProver.make[IO](trie)
        proof <- prover.attestAbsence(absent).flatMap(IO.fromEither)
        // Lie about the termination kind.
        tampered = proof.copy(termination = AbsenceTermination.LeafMismatch)
        verifier = MerklePatriciaAbsenceVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmAbsence(tampered)
      } yield expect(result.isLeft)
    }
  }

  test("tampered missing-nibble in BranchEmptySlot fails verification") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val absent = k("a2")
      for {
        trie <- buildTrie
        prover = MerklePatriciaAbsenceProver.make[IO](trie)
        proof <- prover.attestAbsence(absent).flatMap(IO.fromEither)
        tampered = proof.copy(termination = AbsenceTermination.BranchEmptySlot(Nibble.unsafe(7: Byte)))
        verifier = MerklePatriciaAbsenceVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmAbsence(tampered)
      } yield expect(result.isLeft)
    }
  }

  test("tampered witness chain fails verification") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val absent = k("a2")
      for {
        trie <- buildTrie
        prover = MerklePatriciaAbsenceProver.make[IO](trie)
        proof <- prover.attestAbsence(absent).flatMap(IO.fromEither)
        // Drop the terminal commitment — breaks the authenticated chain.
        tampered = proof.copy(witness = proof.witness.drop(1))
        verifier = MerklePatriciaAbsenceVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmAbsence(tampered)
      } yield expect(result.isLeft)
    }
  }

  test("absence proof against a wrong root fails verification") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      val absent = k("a2")
      for {
        trie <- buildTrie
        prover = MerklePatriciaAbsenceProver.make[IO](trie)
        proof <- prover.attestAbsence(absent).flatMap(IO.fromEither)
        wrongRoot <- hasher.hash("wrong_root")
        verifier = MerklePatriciaAbsenceVerifier.make[IO](wrongRoot)
        result <- verifier.confirmAbsence(proof)
      } yield expect(result.isLeft)
    }
  }

  test("fabricated absence proof for a present key (reusing its inclusion witness) is rejected") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      for {
        trie <- buildTrie
        inclProver = MerklePatriciaSingleInclusionProver.make[IO](trie)
        inclProof <- inclProver.attestPath(keyA).flatMap(IO.fromEither)
        // Wrap the present key's inclusion witness as a bogus "absence" proof claiming LeafMismatch.
        fake = MerklePatriciaAbsenceProof(keyA, inclProof.witness, AbsenceTermination.LeafMismatch)
        verifier = MerklePatriciaAbsenceVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmAbsence(fake)
      } yield expect(result.isLeft)
    }
  }

  test("absence proof Circe round-trips") { implicit res =>
    implicit val (hs, _) = res
    hs.withCurrent { implicit hasher =>
      import io.circe.syntax._
      val absent = k("a2")
      for {
        trie <- buildTrie
        prover = MerklePatriciaAbsenceProver.make[IO](trie)
        proof <- prover.attestAbsence(absent).flatMap(IO.fromEither)
        json = proof.asJson
        decoded <- IO.fromEither(json.as[MerklePatriciaAbsenceProof])
        verifier = MerklePatriciaAbsenceVerifier.make[IO](trie.rootNode.digest)
        result <- verifier.confirmAbsence(decoded)
      } yield
        expect.all(
          decoded.path == proof.path,
          decoded.termination == proof.termination,
          result.isRight
        )
    }
  }
}
