package io.constellationnetwork.node.shared.domain.block.processing

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockAcceptanceContext
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockAcceptanceContext
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore, WithdrawalTimeLimit}
import io.constellationnetwork.schema.swap.AllowSpendReference
import io.constellationnetwork.schema.tokenLock.TokenLockReference
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** §G4: spec for the MPT-backed BlockAcceptanceContext factories.
  *
  * Property under test: after `GlobalSnapshotInfo` is sync'd into an MPT store, point reads via [[BlockAcceptanceContext.fromMpt]] (and the
  * AllowSpend / TokenLock siblings) return the SAME values that the legacy GSI-map factory would have returned. Plus the boundary case —
  * missing keys return None — which downstream consumers fall through to `empty` / `initialTxRef` on.
  *
  * Why this matters: §G4 swaps the block-acceptance read path from `lastSnapshotContext.balances` (full map closure) to per-address MPT
  * point reads. The migration is byte-equivalent only if GSI → MPT key derivation and value codec round-trips agree at the leaf level, for
  * every field the acceptance path touches. These tests pin that agreement at the codec/key level so the GSI-to-MPT migration can't
  * silently drift.
  */
object BlockAcceptanceContextMptSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimitCtx: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  // ---- helpers --------------------------------------------------------------

  private def addr(tag: String): Address =
    Address.fromBytes(tag.getBytes("UTF-8"))

  // The MPT immutable codec for Hash enforces a 64-hex-char (32-byte) wire form. Padding the
  // test hashes to that width is just keeps the codec happy; the test isn't asserting anything
  // about cryptographic structure.
  private def hashOf(seed: String): Hash =
    Hash(seed.padTo(64, '0'))

  private def mkStore(
    info: GlobalSnapshotInfo
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- store.syncFromGlobalSnapshotInfo(info, SnapshotOrdinal(NonNegLong(1L)))
    } yield store

  // ---- BlockAcceptanceContext.fromMpt --------------------------------------

  test("BlockAcceptanceContext.fromMpt — getBalance returns the MPT-stored value for a known address") { res =>
    implicit val (h, _, js) = res
    val a = addr("alice")
    val b = addr("bob")
    val info = GlobalSnapshotInfo.empty.copy(
      balances = SortedMap(a -> Balance(NonNegLong(1_000_000L)), b -> Balance(NonNegLong(42L)))
    )
    for {
      store <- mkStore(info)
      reader = GlobalStateReader.fromMptStore[IO](store)
      ctx = BlockAcceptanceContext.fromMpt[IO](
        reader,
        parentUsages = Map.empty,
        collateral = Amount.empty,
        initialTxRef = TransactionReference.empty
      )
      gotA <- ctx.getBalance(a)
      gotB <- ctx.getBalance(b)
    } yield expect.same(Some(Balance(NonNegLong(1_000_000L))), gotA) && expect.same(Some(Balance(NonNegLong(42L))), gotB)
  }

  test("BlockAcceptanceContext.fromMpt — getBalance returns None for an unknown address (GSI parity for missing key)") { res =>
    implicit val (h, _, js) = res
    val a = addr("alice")
    val ghost = addr("ghost")
    val info = GlobalSnapshotInfo.empty.copy(
      balances = SortedMap(a -> Balance(NonNegLong(1L)))
    )
    for {
      store <- mkStore(info)
      reader = GlobalStateReader.fromMptStore[IO](store)
      ctx = BlockAcceptanceContext.fromMpt[IO](
        reader,
        parentUsages = Map.empty,
        collateral = Amount.empty,
        initialTxRef = TransactionReference.empty
      )
      got <- ctx.getBalance(ghost)
    } yield expect.same(None, got)
  }

  test("BlockAcceptanceContext.fromMpt — getLastTxRef returns the MPT-stored value for a known address") { res =>
    implicit val (h, _, js) = res
    val a = addr("alice")
    val ref = TransactionReference.empty.copy(hash = hashOf("aabbcc"))
    val info = GlobalSnapshotInfo.empty.copy(
      lastTxRefs = SortedMap(a -> ref)
    )
    for {
      store <- mkStore(info)
      reader = GlobalStateReader.fromMptStore[IO](store)
      ctx = BlockAcceptanceContext.fromMpt[IO](
        reader,
        parentUsages = Map.empty,
        collateral = Amount.empty,
        initialTxRef = TransactionReference.empty
      )
      got <- ctx.getLastTxRef(a)
    } yield expect.same(Some(ref), got)
  }

  test("BlockAcceptanceContext.fromMpt — getLastTxRef returns None for an unknown address") { res =>
    implicit val (h, _, js) = res
    val ghost = addr("ghost")
    val info = GlobalSnapshotInfo.empty
    for {
      store <- mkStore(info)
      reader = GlobalStateReader.fromMptStore[IO](store)
      ctx = BlockAcceptanceContext.fromMpt[IO](
        reader,
        parentUsages = Map.empty,
        collateral = Amount.empty,
        initialTxRef = TransactionReference.empty
      )
      got <- ctx.getLastTxRef(ghost)
    } yield expect.same(None, got)
  }

  // ---- AllowSpendBlockAcceptanceContext.fromMpt -----------------------------

  test("AllowSpendBlockAcceptanceContext.fromMpt — getBalance matches MPT-stored value") { res =>
    implicit val (h, _, js) = res
    val a = addr("alice")
    val info = GlobalSnapshotInfo.empty.copy(
      balances = SortedMap(a -> Balance(NonNegLong(7_000L)))
    )
    for {
      store <- mkStore(info)
      reader = GlobalStateReader.fromMptStore[IO](store)
      ctx = AllowSpendBlockAcceptanceContext.fromMpt[IO](
        reader,
        collateral = Amount.empty,
        initialTxRef = AllowSpendReference.empty
      )
      got <- ctx.getBalance(a)
    } yield expect.same(Some(Balance(NonNegLong(7_000L))), got)
  }

  test("AllowSpendBlockAcceptanceContext.fromMpt — getLastTxRef matches MPT-stored value") { res =>
    implicit val (h, _, js) = res
    val a = addr("alice")
    val ref = AllowSpendReference.empty.copy(hash = hashOf("deadbeef"))
    val info = GlobalSnapshotInfo.empty.copy(
      lastAllowSpendRefs = Some(SortedMap(a -> ref))
    )
    for {
      store <- mkStore(info)
      reader = GlobalStateReader.fromMptStore[IO](store)
      ctx = AllowSpendBlockAcceptanceContext.fromMpt[IO](
        reader,
        collateral = Amount.empty,
        initialTxRef = AllowSpendReference.empty
      )
      got <- ctx.getLastTxRef(a)
      missing <- ctx.getLastTxRef(addr("nobody"))
    } yield expect.same(Some(ref), got) && expect.same(None, missing)
  }

  // ---- TokenLockBlockAcceptanceContext.fromMpt ------------------------------

  test("TokenLockBlockAcceptanceContext.fromMpt — getLastTxRef matches MPT-stored value") { res =>
    implicit val (h, _, js) = res
    val a = addr("alice")
    val ref = TokenLockReference.empty.copy(hash = hashOf("cafe1234"))
    val info = GlobalSnapshotInfo.empty.copy(
      lastTokenLockRefs = Some(SortedMap(a -> ref))
    )
    for {
      store <- mkStore(info)
      reader = GlobalStateReader.fromMptStore[IO](store)
      ctx = TokenLockBlockAcceptanceContext.fromMpt[IO](
        reader,
        collateral = Amount.empty,
        initialTxRef = TokenLockReference.empty,
        toBeReplacedHashedTokenLocks = Nil
      )
      got <- ctx.getLastTxRef(a)
      missing <- ctx.getLastTxRef(addr("nobody"))
    } yield expect.same(Some(ref), got) && expect.same(None, missing)
  }

  // ---- GSI-parity round-trip ------------------------------------------------

  test("GSI parity — every (balance, lastTxRef) entry in GSI round-trips through MPT point reads") { res =>
    implicit val (h, _, js) = res
    // A non-trivial spread of addresses & values. The test asserts MPT point reads agree with
    // the GSI map for every key, AND that an address NOT in GSI returns None from the MPT —
    // matching the static-map's `.get(k) → None` semantics that the legacy `fromStaticData`
    // factory relied on.
    val entries = (0 until 8).toList.map { i =>
      val a = addr(s"acct-$i")
      val bal = Balance(NonNegLong.unsafeFrom((i + 1) * 100L))
      val ref = TransactionReference.empty.copy(hash = hashOf(f"${i}%02x"))
      (a, bal, ref)
    }
    val info = GlobalSnapshotInfo.empty.copy(
      balances = SortedMap.from(entries.map { case (a, b, _) => (a, b) }),
      lastTxRefs = SortedMap.from(entries.map { case (a, _, r) => (a, r) })
    )
    val ghost = addr("ghost-acct")
    for {
      store <- mkStore(info)
      reader = GlobalStateReader.fromMptStore[IO](store)
      ctx = BlockAcceptanceContext.fromMpt[IO](
        reader,
        parentUsages = Map.empty,
        collateral = Amount.empty,
        initialTxRef = TransactionReference.empty
      )
      balResults <- entries.traverse { case (a, _, _) => ctx.getBalance(a) }
      refResults <- entries.traverse { case (a, _, _) => ctx.getLastTxRef(a) }
      ghostBal <- ctx.getBalance(ghost)
      ghostRef <- ctx.getLastTxRef(ghost)
    } yield
      expect.same(entries.map { case (_, b, _) => Some(b) }, balResults) &&
        expect.same(entries.map { case (_, _, r) => Some(r) }, refResults) &&
        expect.same(None, ghostBal) &&
        expect.same(None, ghostRef)
  }

}
