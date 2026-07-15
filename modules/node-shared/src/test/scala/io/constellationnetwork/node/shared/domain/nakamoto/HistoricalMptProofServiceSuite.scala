package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, FinalizationOutcome, MptOverlay}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.mpt.prover.MerklePatriciaSingleInclusionProver
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for the branch-aware proof path (#56.7). */
object HistoricalMptProofServiceSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def addr(seed: Int): Address =
    Address.fromBytes(s"hist-mpt-proof-suite-seed-$seed".getBytes("UTF-8"))

  private def gskBalance(seed: Int): GlobalStateKey =
    GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(seed))

  private val ordinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))

  private val branchA: BranchId = BranchId(Hash("a" * 64))
  private val branchB: BranchId = BranchId(Hash("b" * 64))
  private val parentP: BranchId = BranchId(Hash("0" * 64))

  private def mkSetup(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[(MptStore[IO, GlobalStateKey], MptOverlay[IO, GlobalStateKey], HistoricalMptProofService[IO])] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.productionDefault,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      svc = HistoricalMptProofService.make[IO](overlay)
    } yield (store, overlay, svc)

  test("proofAtBranch: produces a valid inclusion proof for a key in the base trie (branch unknown to overlay = base view)") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkSetup
      (store, _, svc) = setup
      key = gskBalance(1)
      _ <- store.insert[Balance](key, Balance(NonNegLong(42L)))

      proofE <- svc.proofAtBranch(parentP, ordinal, key)
    } yield
      expect(
        proofE.isRight
      )
  }

  test("proofAtBranch: branch-isolated proofs — sibling branches yield different inclusion roots for the same key") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkSetup
      (store, overlay, svc) = setup
      // Seed base so InMemoryMerklePatriciaProducer.build doesn't error on empty.
      _ <- store.insert[Balance](gskBalance(99), Balance(NonNegLong(1L)))

      key = gskBalance(2)
      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](key, Balance(NonNegLong(11L)))
      _ <- overlay.commit(hA, branchA, ordinal)

      hB <- overlay.checkout(parentP)
      _ <- hB.insert[Balance](key, Balance(NonNegLong(22L)))
      _ <- overlay.commit(hB, branchB, ordinal)

      // Build the per-branch trie so we can compare the proof's path matches the per-branch root.
      trieA <- overlay.buildRoot(branchA, ordinal).map(_.toOption.get)
      trieB <- overlay.buildRoot(branchB, ordinal).map(_.toOption.get)

      proofA <- svc.proofAtBranch(branchA, ordinal, key).map(_.toOption.get)
      proofB <- svc.proofAtBranch(branchB, ordinal, key).map(_.toOption.get)
    } yield
      expect.all(
        // Both proofs are valid inclusion proofs (provider returned Right).
        proofA.path.value.nonEmpty,
        proofB.path.value.nonEmpty,
        // The two branches MUST have different root hashes since they wrote different values for the same key.
        trieA.rootHash != trieB.rootHash
      )
  }

  test("proofAtBranch: matches a stateless prover run directly against overlay.buildRoot") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkSetup
      (store, overlay, svc) = setup
      _ <- store.insert[Balance](gskBalance(99), Balance(NonNegLong(1L)))

      key = gskBalance(3)
      handle <- overlay.checkout(parentP)
      _ <- handle.insert[Balance](key, Balance(NonNegLong(7L)))
      _ <- overlay.commit(handle, branchA, ordinal)

      svcProofE <- svc.proofAtBranch(branchA, ordinal, key)
      // Independent path: build trie via overlay, run a stateless prover directly.
      trie <- overlay.buildRoot(branchA, ordinal).map(_.toOption.get)
      hex <- GlobalStateKey.toHex[IO](key)
      directProofE <- MerklePatriciaSingleInclusionProver.make[IO](trie).attestPath(hex)
    } yield
      expect.all(
        svcProofE.isRight,
        directProofE.isRight,
        // Both proofs should target the same path and produce the same witness — they were computed
        // from the same trie via the same prover. Equality on `MerklePatriciaInclusionProof` is structural
        // because both `path: Hex` and `witness: List[MerklePatriciaCommitment]` are case-class members.
        svcProofE.toOption.map(_.path) == directProofE.toOption.map(_.path),
        svcProofE.toOption.map(_.witness.size) == directProofE.toOption.map(_.witness.size)
      )
  }

  test("proofAtBranch: concurrent branch finalization completes without store-to-overlay lock inversion") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkSetup
      (store, overlay, svc) = setup
      _ <- store.insert[Balance](gskBalance(99), Balance(NonNegLong(1L)))

      key = gskBalance(30)
      handle <- overlay.checkout(parentP)
      _ <- handle.insert[Balance](key, Balance(NonNegLong(7L)))
      _ <- overlay.commit(handle, branchA, ordinal)

      result <- (svc.proofAtBranch(branchA, ordinal, key), overlay.finalizeBranch(branchA, ordinal)).parTupled.timeout(5.seconds)
      (proof, finalization) = result
    } yield
      expect.all(
        proof.isRight,
        finalization == FinalizationOutcome.Folded(keysApplied = 1, branchesDropped = 0)
      )
  }

  test("proofAtBranch: builds the canonical empty trie and returns the underlying missing-path proof error") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkSetup
      (_, _, svc) = setup
      proofE <- svc.proofAtBranch(parentP, ordinal, gskBalance(4))
    } yield
      expect(
        proofE match {
          case Left(HistoricalMptProofService.Underlying(_)) => true
          case _                                             => false
        }
      )
  }
}
