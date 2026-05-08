package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Phase A acceptance gate (#56.10). Asserts the `AcceptanceMpt[F]` algebra preserves byte-identical reads against `MptStore` when wired
  * over `OverlayMode.Passthrough` (i.e. the algebra introduces no semantic drift), and that reads via the algebra observe the writer's
  * branch view through `MptOverlay.get`/`getAllForPrefix` rather than the raw underlying store.
  */
object AcceptanceMptSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def mkStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
    } yield store

  private def addr(seed: Int): Address =
    Address.fromBytes(s"acceptance-mpt-suite-seed-$seed".getBytes("UTF-8"))

  private def gskBalance(seed: Int): GlobalStateKey =
    GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(seed))

  private val ordinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))
  private val parentP: BranchId = BranchId(Hash("0" * 64))
  private val childTip: BranchId = BranchId(Hash("c" * 64))

  test("passthrough algebra: get returns the underlying MptStore value when no local writes") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)
      key = gskBalance(0)
      _ <- store.insert[Balance](key, Balance(NonNegLong(7L)))

      handle <- overlay.checkout(parentP)
      mpt = AcceptanceMpt.fromOverlay[IO](overlay, parentP, handle)

      direct <- store.get[Balance](key)
      viaAlgebra <- mpt.get[Balance](key)
    } yield
      expect.all(
        direct.contains(Balance(NonNegLong(7L))),
        viaAlgebra.contains(Balance(NonNegLong(7L))),
        direct == viaAlgebra
      )
  }

  test("passthrough algebra: writes flow through to MptStore on commit") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      handle <- overlay.checkout(parentP)
      mpt = AcceptanceMpt.fromOverlay[IO](overlay, parentP, handle)

      key = gskBalance(1)
      _ <- mpt.insert[Balance](key, Balance(NonNegLong(99L)))
      _ <- overlay.commit(handle, childTip, ordinal)

      // In passthrough, writes go straight to MptStore — observable directly post-commit.
      direct <- store.get[Balance](key)
    } yield expect(direct.contains(Balance(NonNegLong(99L))))
  }

  test("getMany: per-key reads match individual mptStore.getMany result on the parent view") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      keyA = gskBalance(2)
      keyB = gskBalance(3)
      keyMissing = gskBalance(4)

      _ <- store.insert[Balance](keyA, Balance(NonNegLong(11L)))
      _ <- store.insert[Balance](keyB, Balance(NonNegLong(22L)))

      handle <- overlay.checkout(parentP)
      mpt = AcceptanceMpt.fromOverlay[IO](overlay, parentP, handle)

      viaAlgebra <- mpt.getMany[Balance](List(keyA, keyB, keyMissing))
      viaStore <- store.getMany[Balance](List(keyA, keyB, keyMissing))
    } yield
      expect.all(
        viaAlgebra == viaStore,
        viaAlgebra.size == 2,
        viaAlgebra.get(keyA).contains(Balance(NonNegLong(11L))),
        viaAlgebra.get(keyB).contains(Balance(NonNegLong(22L))),
        !viaAlgebra.contains(keyMissing)
      )
  }

  test("multi-branch algebra: a writer's local upsert is observed at the same branch tip after commit") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.productionDefault,
        underlying = store,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = Set.empty[BranchId].pure[IO]
      )

      handle <- overlay.checkout(parentP)
      mpt = AcceptanceMpt.fromOverlay[IO](overlay, parentP, handle)

      key = gskBalance(5)
      _ <- mpt.insert[Balance](key, Balance(NonNegLong(31L)))
      _ <- overlay.commit(handle, childTip, ordinal)

      atChild <- overlay.get[Balance](childTip, key)
      atParent <- overlay.get[Balance](parentP, key)
      atBase <- store.get[Balance](key)
    } yield
      expect.all(
        atChild.contains(Balance(NonNegLong(31L))),
        // Sibling/parent view: the child's writes are NOT visible.
        atParent.isEmpty,
        // Base store: not yet folded forward (commit only registers; finalize folds).
        atBase.isEmpty
      )
  }

  test("multi-branch algebra: writes are isolated between sibling branches") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.productionDefault,
        underlying = store,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = Set.empty[BranchId].pure[IO]
      )

      key = gskBalance(6)

      // Sibling A
      handleA <- overlay.checkout(parentP)
      mptA = AcceptanceMpt.fromOverlay[IO](overlay, parentP, handleA)
      _ <- mptA.insert[Balance](key, Balance(NonNegLong(41L)))
      siblingA = BranchId(Hash("a" * 64))
      _ <- overlay.commit(handleA, siblingA, ordinal)

      // Sibling B (also a child of parentP)
      handleB <- overlay.checkout(parentP)
      mptB = AcceptanceMpt.fromOverlay[IO](overlay, parentP, handleB)
      _ <- mptB.insert[Balance](key, Balance(NonNegLong(52L)))
      siblingB = BranchId(Hash("b" * 64))
      _ <- overlay.commit(handleB, siblingB, ordinal)

      readA <- overlay.get[Balance](siblingA, key)
      readB <- overlay.get[Balance](siblingB, key)
      readBase <- store.get[Balance](key)
    } yield
      expect.all(
        readA.contains(Balance(NonNegLong(41L))),
        readB.contains(Balance(NonNegLong(52L))),
        readBase.isEmpty
      )
  }
}
