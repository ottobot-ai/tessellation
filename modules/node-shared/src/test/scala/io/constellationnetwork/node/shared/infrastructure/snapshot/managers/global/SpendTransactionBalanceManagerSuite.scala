package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendTransaction
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.swap.SwapAmount
import io.constellationnetwork.security._
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

object SpendTransactionBalanceManagerSuite extends MutableIOSuite {
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def mkMptStore(prior: SortedMap[Address, Balance])(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      _ <- prior.toList.traverse_ {
        case (addr, bal) =>
          mptStore.insert[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr), bal)
      }
    } yield mptStore

  test("source pays destination using MPT-resident balances when delta map is empty") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      source = kp1.getPublic.toAddress
      dest = kp2.getPublic.toAddress

      priorInMpt = SortedMap[Address, Balance](
        source -> Balance(NonNegLong(1000)),
        dest -> Balance(NonNegLong(50))
      )
      spendTx = SpendTransaction(
        source = source,
        destination = dest,
        amount = SwapAmount(PosLong(100)),
        currencyId = None,
        allowSpendRef = None
      )

      mptStore <- mkMptStore(priorInMpt)
      mgr = SpendTransactionBalanceManager.make[IO](GlobalStateReader.fromMptStore(mptStore))

      result <- mgr.updateGlobalBalancesBySpendTransactions(SortedMap.empty, SortedMap.empty, List(spendTx))
      delta = result.map(_._2).getOrElse(SortedMap.empty[Address, Balance])
    } yield
      expect.all(
        result.isRight,
        delta(source) == Balance(NonNegLong(900)),
        delta(dest) == Balance(NonNegLong(150))
      )
  }

  test("delta shadows MPT — in-ordinal balance is what's used") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      source = kp1.getPublic.toAddress
      dest = kp2.getPublic.toAddress

      // MPT says source had 0; delta says 500 (already credited this ordinal)
      priorInMpt = SortedMap[Address, Balance](source -> Balance(NonNegLong(0)))
      currentDelta = SortedMap[Address, Balance](source -> Balance(NonNegLong(500)))
      spendTx = SpendTransaction(
        source = source,
        destination = dest,
        amount = SwapAmount(PosLong(100)),
        currencyId = None,
        allowSpendRef = None
      )

      mptStore <- mkMptStore(priorInMpt)
      mgr = SpendTransactionBalanceManager.make[IO](GlobalStateReader.fromMptStore(mptStore))

      result <- mgr.updateGlobalBalancesBySpendTransactions(currentDelta, SortedMap.empty, List(spendTx))
    } yield
      expect.all(
        result.isRight,
        result.map(_._2).getOrElse(SortedMap.empty[Address, Balance])(source) == Balance(NonNegLong(400))
      )
  }

  test("source missing from delta and MPT: balance arithmetic error (insufficient funds)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      source = kp1.getPublic.toAddress
      dest = kp2.getPublic.toAddress

      spendTx = SpendTransaction(
        source = source,
        destination = dest,
        amount = SwapAmount(PosLong(100)),
        currencyId = None,
        allowSpendRef = None
      )

      mptStore <- mkMptStore(SortedMap.empty)
      mgr = SpendTransactionBalanceManager.make[IO](GlobalStateReader.fromMptStore(mptStore))

      result <- mgr.updateGlobalBalancesBySpendTransactions(SortedMap.empty, SortedMap.empty, List(spendTx))
    } yield expect(result.isLeft)
  }

  test("empty spend transactions yields the input deltas unchanged") { res =>
    implicit val (h, sp, js) = res
    for {
      mptStore <- mkMptStore(SortedMap.empty)
      mgr = SpendTransactionBalanceManager.make[IO](GlobalStateReader.fromMptStore(mptStore))

      result <- mgr.updateGlobalBalancesBySpendTransactions(SortedMap.empty, SortedMap.empty, List.empty)
    } yield
      expect.all(
        result.isRight,
        result.map(_._1).getOrElse(SortedMap.empty[Address, Balance]).isEmpty,
        result.map(_._2).getOrElse(SortedMap.empty[Address, Balance]).isEmpty
      )
  }
}
