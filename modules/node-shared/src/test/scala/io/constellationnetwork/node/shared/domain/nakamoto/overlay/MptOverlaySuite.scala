package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.{IO, Resource}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Correctness-equivalence tests for `MptOverlay.passthrough` (#56.2 milestone).
  *
  * Goal: prove the passthrough impl behaves identically to using `MptStore` directly. If a write goes through `BranchHandle`, a subsequent
  * read through `MptOverlay.get` returns the same result as reading `MptStore.get`. `commit` is a no-op against state but does record
  * topology in `parentChildTree`. `finalizeBranch` is always `NoOp`.
  */
object MptOverlaySuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def mkStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
    } yield store

  // Deterministic distinct addresses for trie key diversity. `Address.fromBytes` always produces a valid
  // refined DAG address, so any seed is safe.
  private def addr(seed: Int): Address =
    Address.fromBytes(s"mpt-overlay-suite-seed-$seed".getBytes("UTF-8"))

  private val ordinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))

  // The branch arg is ignored by passthrough — use distinct ids in tests to make that explicit.
  private val branchA: BranchId = BranchId(Hash("a" * 64))
  private val branchB: BranchId = BranchId(Hash("b" * 64))
  private val branchC: BranchId = BranchId(Hash("c" * 64))

  test("passthrough: writes via BranchHandle land in the underlying MptStore") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      handle <- overlay.checkout(branchA)
      key = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(0))
      _ <- handle.insert[Balance](key, Balance(NonNegLong(42L)))

      directRead <- store.get[Balance](key)
      overlayRead <- overlay.get[Balance](branchA, key)
    } yield expect.all(
      directRead.contains(Balance(NonNegLong(42L))),
      overlayRead.contains(Balance(NonNegLong(42L)))
    )
  }

  test("passthrough: reads ignore the BranchId — same value visible across all branches") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      key = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(2))
      _ <- store.insert[Balance](key, Balance(NonNegLong(7L)))

      readA <- overlay.get[Balance](branchA, key)
      readB <- overlay.get[Balance](branchB, key)
      readC <- overlay.get[Balance](branchC, key)
    } yield expect.all(
      readA.contains(Balance(NonNegLong(7L))),
      readB.contains(Balance(NonNegLong(7L))),
      readC.contains(Balance(NonNegLong(7L)))
    )
  }

  test("passthrough: BranchHandle.update applies removes and upserts (delegates to MptStore.update)") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      keyKeep = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(3))
      keyDrop = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(4))
      keyAdd = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(5))

      _ <- store.insert[Balance](
        Map[GlobalStateKey, Balance](
          keyKeep -> Balance(NonNegLong(1L)),
          keyDrop -> Balance(NonNegLong(2L))
        )
      )

      handle <- overlay.checkout(branchA)
      _ <- handle.update[Balance](Map(keyAdd -> Balance(NonNegLong(3L))), Set(keyDrop))

      keepRead <- overlay.get[Balance](branchA, keyKeep)
      dropRead <- overlay.get[Balance](branchA, keyDrop)
      addRead <- overlay.get[Balance](branchA, keyAdd)
    } yield expect.all(
      keepRead.contains(Balance(NonNegLong(1L))),
      dropRead.isEmpty,
      addRead.contains(Balance(NonNegLong(3L)))
    )
  }

  test("passthrough: buildRoot returns the same trie as MptStore.build at the same ordinal") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      key = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(6))
      _ <- store.insert[Balance](key, Balance(NonNegLong(99L)))

      directBuild <- store.build(ordinal)
      overlayBuild <- overlay.buildRoot(branchA, ordinal)
    } yield expect.all(
      directBuild.isRight,
      overlayBuild.isRight,
      directBuild.toOption.map(_.rootHash) == overlayBuild.toOption.map(_.rootHash)
    )
  }

  test("passthrough: commit associates childTip→parent in ParentChildTree") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      handle <- overlay.checkout(branchA)
      _ <- overlay.commit(handle, branchB, ordinal)

      parent <- pcTree.parentOf(branchB.value)
    } yield expect.same(Some(branchA.value), parent)
  }

  test("passthrough: commit with childTip == parent skips self-association (no self-loop in ParentChildTree)") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      handle <- overlay.checkout(branchA)
      _ <- overlay.commit(handle, branchA, ordinal)

      // No association recorded for branchA, so parentOf returns None.
      parent <- pcTree.parentOf(branchA.value)
    } yield expect(parent.isEmpty)
  }

  test("passthrough: finalizeBranch always returns NoOp regardless of branch or ordinal") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      r1 <- overlay.finalizeBranch(branchA, ordinal)
      r2 <- overlay.finalizeBranch(branchB, SnapshotOrdinal(NonNegLong(99L)))
      // Repeated calls at the same (ordinal, hash) are idempotent — still NoOp.
      r3 <- overlay.finalizeBranch(branchA, ordinal)
    } yield expect.all(
      r1 == FinalizationOutcome.NoOp,
      r2 == FinalizationOutcome.NoOp,
      r3 == FinalizationOutcome.NoOp
    )
  }

  test("passthrough: getAllForPrefix sees writes from any branch — no isolation") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      // Writes via branchA's handle are visible via reads keyed by branchB.
      handle <- overlay.checkout(branchA)
      key1 = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(10))
      key2 = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(11))
      _ <- handle.insert[Balance](key1, Balance(NonNegLong(1L)))
      _ <- handle.insert[Balance](key2, Balance(NonNegLong(2L)))

      hex1 <- GlobalStateKey.toHex[IO](key1)
      hex2 <- GlobalStateKey.toHex[IO](key2)
      // Empty Hex prefix scans the whole trie — both entries should be visible to branchB.
      allFromB <- overlay.getAllForPrefix[Balance](branchB, io.constellationnetwork.security.hex.Hex(""))
    } yield expect.all(
      allFromB.contains(hex1),
      allFromB.contains(hex2)
    )
  }

  test("MptOverlay.make: enabled=false routes to passthrough (correctness-equivalent)") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](enabled = false, store, pcTree)

      key = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(20))
      _ <- store.insert[Balance](key, Balance(NonNegLong(5L)))
      r <- overlay.get[Balance](branchA, key)
    } yield expect(r.contains(Balance(NonNegLong(5L))))
  }

  test("MptOverlay.make: enabled=true also routes to passthrough until #56.4") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](enabled = true, store, pcTree)

      key = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(21))
      _ <- store.insert[Balance](key, Balance(NonNegLong(8L)))
      r <- overlay.get[Balance](branchA, key)
    } yield expect(r.contains(Balance(NonNegLong(8L))))
  }
}
