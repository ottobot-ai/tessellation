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
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

object SpendTransactionBalanceManagerSuite extends MutableIOSuite {
  import SpendTransactionBalanceManager._

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

  test("a no-reference self-transfer proves funds without changing state or deltas") { res =>
    implicit val (h, sp, js) = res
    for {
      source <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      spendTx = SpendTransaction(
        source = source,
        destination = source,
        amount = SwapAmount(PosLong(60L)),
        currencyId = None,
        allowSpendRef = None
      )
      mptStore <- mkMptStore(SortedMap(source -> Balance(NonNegLong(100L))))
      mgr = SpendTransactionBalanceManager.make[IO](GlobalStateReader.fromMptStore(mptStore))
      accepted <- mgr.updateGlobalBalancesBySpendTransactions(SortedMap.empty, SortedMap.empty, List(spendTx))
      insufficientStore <- mkMptStore(SortedMap(source -> Balance(NonNegLong(59L))))
      insufficientMgr = SpendTransactionBalanceManager.make[IO](GlobalStateReader.fromMptStore(insufficientStore))
      insufficient <- insufficientMgr.updateGlobalBalancesBySpendTransactions(SortedMap.empty, SortedMap.empty, List(spendTx))
    } yield
      expect.all(
        accepted.contains((SortedMap.empty[Address, Balance], SortedMap.empty[Address, Balance])),
        insufficient == Left(BalanceArithmeticFailure(source, io.constellationnetwork.schema.balance.AmountUnderflow))
      )
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

  test("referenced spend fails closed when the exact allow-spend is missing") { res =>
    implicit val (h, sp, js) = res
    for {
      source <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      destination <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      missingRef = Hash("ab" * 32)
      spendTx = SpendTransaction(
        source = source,
        destination = destination,
        amount = SwapAmount(PosLong(10L)),
        currencyId = None,
        allowSpendRef = missingRef.some
      )
      mptStore <- mkMptStore(SortedMap(source -> Balance(NonNegLong(100L))))
      mgr = SpendTransactionBalanceManager.make[IO](GlobalStateReader.fromMptStore(mptStore))
      result <- mgr.updateGlobalBalancesBySpendTransactions(SortedMap.empty, SortedMap.empty, List(spendTx))
    } yield expect(result == Left(ReferencedAllowSpendNotFound(source, missingRef)))
  }

  test("referenced spend fails with a typed error when amount exceeds its reservation") { res =>
    implicit val (h, sp, js) = res
    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destination <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      source = sourceKey.getPublic.toAddress
      signedAllowSpend <- Signed.forAsyncHasher(
        AllowSpend(
          source,
          destination,
          None,
          SwapAmount(PosLong(50L)),
          AllowSpendFee(NonNegLong(0L)),
          AllowSpendReference.empty,
          EpochProgress(NonNegLong(20L)),
          List(destination)
        ),
        sourceKey
      )
      hashedAllowSpend <- signedAllowSpend.toHashed
      spendTx = SpendTransaction(
        source = source,
        destination = destination,
        amount = SwapAmount(PosLong(51L)),
        currencyId = None,
        allowSpendRef = hashedAllowSpend.hash.some
      )
      mptStore <- mkMptStore(SortedMap.empty)
      mgr = SpendTransactionBalanceManager.make[IO](GlobalStateReader.fromMptStore(mptStore))
      result <- mgr.updateGlobalBalancesBySpendTransactions(
        SortedMap.empty,
        SortedMap(source -> List(hashedAllowSpend)),
        List(spendTx)
      )
    } yield
      expect(
        result == Left(
          SpendAmountExceedsAllowSpend(hashedAllowSpend.hash, signedAllowSpend.amount, spendTx.amount)
        )
      )
  }

  test("referenced spend releases exactly the reserved amount between source and destination") { res =>
    implicit val (h, sp, js) = res
    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destination <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      source = sourceKey.getPublic.toAddress
      signedAllowSpend <- Signed.forAsyncHasher(
        AllowSpend(
          source,
          destination,
          None,
          SwapAmount(PosLong(100L)),
          AllowSpendFee(NonNegLong(0L)),
          AllowSpendReference.empty,
          EpochProgress(NonNegLong(20L)),
          List(destination)
        ),
        sourceKey
      )
      hashedAllowSpend <- signedAllowSpend.toHashed
      spendTx = SpendTransaction(
        source = source,
        destination = destination,
        amount = SwapAmount(PosLong(60L)),
        currencyId = None,
        allowSpendRef = hashedAllowSpend.hash.some
      )
      prior = SortedMap(source -> Balance(NonNegLong(100L)), destination -> Balance(NonNegLong(10L)))
      mptStore <- mkMptStore(prior)
      mgr = SpendTransactionBalanceManager.make[IO](GlobalStateReader.fromMptStore(mptStore))
      result <- mgr.updateGlobalBalancesBySpendTransactions(
        SortedMap.empty,
        SortedMap(source -> List(hashedAllowSpend)),
        List(spendTx)
      )
      updated = result.toOption.map(_._1).getOrElse(SortedMap.empty[Address, Balance])
      released =
        BigInt(updated(source).value.value - prior(source).value.value) +
          BigInt(updated(destination).value.value - prior(destination).value.value)
    } yield
      expect.all(
        result.isRight,
        updated(source) == Balance(NonNegLong(140L)),
        updated(destination) == Balance(NonNegLong(70L)),
        released == BigInt(100L)
      )
  }

  test("referenced spend accumulates both credits when allowance source equals destination") { res =>
    implicit val (h, sp, js) = res
    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      signedAllowSpend <- Signed.forAsyncHasher(
        AllowSpend(
          source,
          source,
          None,
          SwapAmount(PosLong(100L)),
          AllowSpendFee(NonNegLong(7L)),
          AllowSpendReference.empty,
          EpochProgress(NonNegLong(20L)),
          List(source)
        ),
        sourceKey
      )
      hashedAllowSpend <- signedAllowSpend.toHashed
      spendTx = SpendTransaction(
        source = source,
        destination = source,
        amount = SwapAmount(PosLong(60L)),
        currencyId = None,
        allowSpendRef = hashedAllowSpend.hash.some
      )
      mptStore <- mkMptStore(SortedMap(source -> Balance(NonNegLong(13L))))
      mgr = SpendTransactionBalanceManager.make[IO](GlobalStateReader.fromMptStore(mptStore))
      result <- mgr.updateGlobalBalancesBySpendTransactions(
        SortedMap.empty,
        SortedMap(source -> List(hashedAllowSpend)),
        List(spendTx)
      )
    } yield
      expect.all(
        result.toOption.exists(_._1.get(source).contains(Balance(NonNegLong(113L)))),
        result.toOption.exists(_._2.get(source).contains(Balance(NonNegLong(113L))))
      )
  }

  test("referenced authorization cannot release twice through the balance applier") { res =>
    implicit val (h, sp, js) = res
    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destination <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      source = sourceKey.getPublic.toAddress
      signedAllowSpend <- Signed.forAsyncHasher(
        AllowSpend(
          source,
          destination,
          None,
          SwapAmount(PosLong(100L)),
          AllowSpendFee(NonNegLong(0L)),
          AllowSpendReference.empty,
          EpochProgress(NonNegLong(20L)),
          List(destination)
        ),
        sourceKey
      )
      hashedAllowSpend <- signedAllowSpend.toHashed
      spendTx = SpendTransaction(
        source = source,
        destination = destination,
        amount = SwapAmount(PosLong(60L)),
        currencyId = None,
        allowSpendRef = hashedAllowSpend.hash.some
      )
      mptStore <- mkMptStore(SortedMap.empty)
      mgr = SpendTransactionBalanceManager.make[IO](GlobalStateReader.fromMptStore(mptStore))
      result <- mgr.updateGlobalBalancesBySpendTransactions(
        SortedMap.empty,
        SortedMap(source -> List(hashedAllowSpend)),
        List(spendTx, spendTx)
      )
    } yield expect(result == Left(ReferencedAllowSpendAlreadyConsumed(source, hashedAllowSpend.hash)))
  }

  test("referenced spend accepts the Long bound and reports destination overflow deterministically") { res =>
    implicit val (h, sp, js) = res
    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destination <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      source = sourceKey.getPublic.toAddress
      maxSwap = SwapAmount(PosLong.unsafeFrom(Long.MaxValue))
      signedAllowSpend <- Signed.forAsyncHasher(
        AllowSpend(
          source,
          destination,
          None,
          maxSwap,
          AllowSpendFee(NonNegLong(0L)),
          AllowSpendReference.empty,
          EpochProgress(NonNegLong(20L)),
          List(destination)
        ),
        sourceKey
      )
      hashedAllowSpend <- signedAllowSpend.toHashed
      spendTx = SpendTransaction(
        allowSpendRef = hashedAllowSpend.hash.some,
        currencyId = None,
        amount = maxSwap,
        source = source,
        destination = destination
      )
      mptStore <- mkMptStore(SortedMap.empty)
      mgr = SpendTransactionBalanceManager.make[IO](GlobalStateReader.fromMptStore(mptStore))
      accepted <- mgr.updateGlobalBalancesBySpendTransactions(
        SortedMap.empty,
        SortedMap(source -> List(hashedAllowSpend)),
        List(spendTx)
      )
      overflow <- mgr.updateGlobalBalancesBySpendTransactions(
        SortedMap(destination -> Balance(NonNegLong(1L))),
        SortedMap(source -> List(hashedAllowSpend)),
        List(spendTx)
      )
    } yield
      expect.all(
        accepted.toOption.exists(_._1(destination) == Balance(NonNegLong.unsafeFrom(Long.MaxValue))),
        overflow == Left(BalanceArithmeticFailure(destination, io.constellationnetwork.schema.balance.AmountOverflow))
      )
  }
}
