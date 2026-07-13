package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import cats.effect.{IO, Resource}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.dataApplication.FeeTransaction
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.transaction.FeeTransactionValidator
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.{Signed, SignedValidator}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object BalanceOpsManagerSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      securityProvider <- SecurityProvider.forAsync[IO]
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    } yield (Hasher.forJson[IO], securityProvider)

  test("two maximum fees cannot wrap an insufficient source balance into a positive balance") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      source <- KeyPairGenerator.makeKeyPair[IO]
      destination1 <- KeyPairGenerator.makeKeyPair[IO]
      destination2 <- KeyPairGenerator.makeKeyPair[IO]
      tx1 <- Signed.forAsyncHasher(
        FeeTransaction(
          source.getPublic.toAddress,
          destination1.getPublic.toAddress,
          Amount(NonNegLong.unsafeFrom(Long.MaxValue)),
          Hash.empty
        ),
        source
      )
      tx2 <- Signed.forAsyncHasher(
        FeeTransaction(
          source.getPublic.toAddress,
          destination2.getPublic.toAddress,
          Amount(NonNegLong.unsafeFrom(Long.MaxValue)),
          Hash.empty
        ),
        source
      )
      manager = BalanceOpsManager.make[IO](FeeTransactionValidator.make[IO](SignedValidator.make[IO]))
      result <- manager
        .acceptFeeTxs(
          SortedMap(source.getPublic.toAddress -> Balance.empty),
          Some(SortedSet(tx1, tx2))
        )
        .attempt
    } yield expect(result.left.exists(_.isInstanceOf[ArithmeticException]))
  }

  test("two maximum fees cannot wrap a destination balance past Long.MaxValue") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      source1 <- KeyPairGenerator.makeKeyPair[IO]
      source2 <- KeyPairGenerator.makeKeyPair[IO]
      destination <- KeyPairGenerator.makeKeyPair[IO]
      tx1 <- Signed.forAsyncHasher(
        FeeTransaction(
          source1.getPublic.toAddress,
          destination.getPublic.toAddress,
          Amount(NonNegLong.unsafeFrom(Long.MaxValue)),
          Hash.empty
        ),
        source1
      )
      tx2 <- Signed.forAsyncHasher(
        FeeTransaction(
          source2.getPublic.toAddress,
          destination.getPublic.toAddress,
          Amount(NonNegLong.unsafeFrom(Long.MaxValue)),
          Hash.empty
        ),
        source2
      )
      manager = BalanceOpsManager.make[IO](FeeTransactionValidator.make[IO](SignedValidator.make[IO]))
      funded = Balance(NonNegLong.unsafeFrom(Long.MaxValue))
      result <- manager
        .acceptFeeTxs(
          SortedMap(
            source1.getPublic.toAddress -> funded,
            source2.getPublic.toAddress -> funded,
            destination.getPublic.toAddress -> Balance(NonNegLong.unsafeFrom(3L))
          ),
          Some(SortedSet(tx1, tx2))
        )
        .attempt
    } yield expect(result.left.exists(_.isInstanceOf[ArithmeticException]))
  }

  test("ECO-18 / ECON-F-003 remains RED: a successor snapshot reaccepts and reapplies the exact same signed fee") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      source <- KeyPairGenerator.makeKeyPair[IO]
      destination <- KeyPairGenerator.makeKeyPair[IO]
      fee <- Signed.forAsyncHasher(
        FeeTransaction(
          source.getPublic.toAddress,
          destination.getPublic.toAddress,
          Amount(NonNegLong.unsafeFrom(10L)),
          Hash("a" * 64)
        ),
        source
      )
      manager = BalanceOpsManager.make[IO](FeeTransactionValidator.make[IO](SignedValidator.make[IO]))
      feeSet = Some(SortedSet(fee))
      initialBalances = SortedMap(
        source.getPublic.toAddress -> Balance(NonNegLong.unsafeFrom(100L)),
        destination.getPublic.toAddress -> Balance.empty
      )

      _ <- manager.validateFeeTxs(feeSet)
      (firstSnapshotBalances, firstAccepted) <- manager.acceptFeeTxs(initialBalances, feeSet)

      _ <- manager.validateFeeTxs(feeSet)
      (successorSnapshotBalances, secondAccepted) <- manager.acceptFeeTxs(firstSnapshotBalances, feeSet)
    } yield
      expect.all(
        firstAccepted == feeSet,
        secondAccepted == feeSet,
        firstSnapshotBalances(source.getPublic.toAddress) == Balance(NonNegLong.unsafeFrom(90L)),
        firstSnapshotBalances(destination.getPublic.toAddress) == Balance(NonNegLong.unsafeFrom(10L)),
        successorSnapshotBalances(source.getPublic.toAddress) == Balance(NonNegLong.unsafeFrom(80L)),
        successorSnapshotBalances(destination.getPublic.toAddress) == Balance(NonNegLong.unsafeFrom(20L))
      )
  }
}
