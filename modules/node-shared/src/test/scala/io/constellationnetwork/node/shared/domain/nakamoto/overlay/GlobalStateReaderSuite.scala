package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay.OverlayMode
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

/** Tests for `GlobalStateReader.pending` and `GlobalStateReader.finalized` — the #117/#118 Phase 2 read-path migration entry points used by
  * gl0 HTTP routes and read-path services to pick up the chain's pending overlay state without staleness from `finalizeBranch.foldIntoBase`
  * lag.
  *
  *   - `pending` falls through to base when no overlay write exists for the key.
  *   - `pending` returns overlay write when one exists at the configured `bestTipBranchF`.
  *   - `finalized` ignores overlay entirely — reads only what the base `MptStore` holds.
  */
object GlobalStateReaderSuite extends MutableIOSuite {

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

  private def mkOverlay(
    store: MptStore[IO, GlobalStateKey]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptOverlay[IO, GlobalStateKey]] =
    for {
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        underlying = store,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
    } yield overlay

  private def addr(seed: Int): Address =
    Address.fromBytes(s"global-state-reader-suite-seed-$seed".getBytes("UTF-8"))

  private def gskBalance(seed: Int): GlobalStateKey =
    GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(seed))

  private val ordinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))
  private val tipA: BranchId = BranchId(Hash("a" * 64))

  test("pending: falls through to base when no overlay write exists for the key") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      overlay <- mkOverlay(store)
      // Seed base directly — no overlay write at any branch.
      key = gskBalance(0)
      _ <- store.insert[Balance](key, Balance(NonNegLong(42L)))
      // bestTipFn returns Some(tipA) but tipA isn't in pendingRef, so multi-branch falls
      // through to base.
      bestTipFnRef <- Ref.of[IO, IO[Option[BranchId]]](IO.pure(Some(tipA)))
      reader = GlobalStateReader.pending[IO](overlay, bestTipFnRef.get.flatten)
      read <- reader.get[Balance](key)
    } yield expect(read.contains(Balance(NonNegLong(42L))))
  }

  test("pending: returns overlay write when one exists at the configured bestTip") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      overlay <- mkOverlay(store)
      // Seed base with one value, then commit a different value to the overlay at tipA.
      key = gskBalance(1)
      _ <- store.insert[Balance](key, Balance(NonNegLong(7L)))
      // Check out at parent=base, write to tipA, commit.
      handle <- overlay.checkout(BranchId.base)
      _ <- handle.insert[Balance](key, Balance(NonNegLong(999L)))
      _ <- overlay.commit(handle, tipA, ordinal)
      // bestTipFn returns Some(tipA) — reader should see the overlay's pending value, not base.
      bestTipFnRef <- Ref.of[IO, IO[Option[BranchId]]](IO.pure(Some(tipA)))
      reader = GlobalStateReader.pending[IO](overlay, bestTipFnRef.get.flatten)
      read <- reader.get[Balance](key)
    } yield expect(read.contains(Balance(NonNegLong(999L))))
  }

  test("pending: when bestTipFn is None, reads resolve to BranchId.base → base store") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      overlay <- mkOverlay(store)
      // Seed base, commit overlay at tipA — but bestTipFn returns None so reads ignore tipA.
      key = gskBalance(2)
      _ <- store.insert[Balance](key, Balance(NonNegLong(11L)))
      handle <- overlay.checkout(BranchId.base)
      _ <- handle.insert[Balance](key, Balance(NonNegLong(999L)))
      _ <- overlay.commit(handle, tipA, ordinal)
      // bestTipFn = None → reader uses BranchId.base, which isn't in pendingRef → falls to base.
      reader = GlobalStateReader.pending[IO](overlay, IO.pure(Option.empty[BranchId]))
      read <- reader.get[Balance](key)
    } yield expect(read.contains(Balance(NonNegLong(11L))))
  }

  test("finalized: ignores overlay entirely — reads only base") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      overlay <- mkOverlay(store)
      // Seed base AND overlay with different values. `finalized` reader takes only the store; the
      // overlay is constructed but not threaded into the reader.
      key = gskBalance(3)
      _ <- store.insert[Balance](key, Balance(NonNegLong(5L)))
      handle <- overlay.checkout(BranchId.base)
      _ <- handle.insert[Balance](key, Balance(NonNegLong(999L)))
      _ <- overlay.commit(handle, tipA, ordinal)
      reader = GlobalStateReader.finalized[IO](store)
      read <- reader.get[Balance](key)
    } yield expect(read.contains(Balance(NonNegLong(5L))))
  }
}
