package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import java.security.KeyPair
import java.util.UUID

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.{CurrencySnapshotContext, CurrencySnapshotInfo}
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.AddressesConfig
import io.constellationnetwork.node.shared.domain.swap.AllowSpendValidator.AllowSpendAlreadyExpired
import io.constellationnetwork.node.shared.domain.swap.block._
import io.constellationnetwork.node.shared.domain.swap.{AllowSpendChainValidator, AllowSpendValidator => SignedAllowSpendValidator}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency.AllowSpendOpsManager.AllowSpendSettlementError
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendTransaction
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.{Signed, SignedValidator}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

object AllowSpendOpsManagerSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      securityProvider <- SecurityProvider.forAsync[IO]
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    } yield (Hasher.forJson[IO], securityProvider)

  private def balance(value: Long): Balance = Balance(NonNegLong.unsafeFrom(value))
  private def amount(value: Long): SwapAmount = SwapAmount(PosLong.unsafeFrom(value))
  private def fee(value: Long): AllowSpendFee = AllowSpendFee(NonNegLong.unsafeFrom(value))
  private def epoch(value: Long): EpochProgress = EpochProgress(NonNegLong.unsafeFrom(value))

  private def makeAllowSpend(
    sourceKeyPair: KeyPair,
    destination: Address,
    amountValue: Long,
    lastValidEpoch: Long,
    currencyId: Option[CurrencyId] = none,
    feeValue: Long = 0L
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[AllowSpend]] =
    Signed.forAsyncHasher(
      AllowSpend(
        source = sourceKeyPair.getPublic.toAddress,
        destination = destination,
        currencyId = currencyId,
        amount = amount(amountValue),
        fee = fee(feeValue),
        parent = AllowSpendReference.empty,
        lastValidEpochProgress = epoch(lastValidEpoch),
        approvers = List(destination)
      ),
      sourceKeyPair
    )

  private def total(balances: SortedMap[Address, Balance]): BigInt =
    balances.valuesIterator.foldLeft(BigInt(0))((acc, value) => acc + BigInt(value.value.value))

  private def makeSignedBlock(
    allowSpend: Signed[AllowSpend],
    id: Long
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[AllowSpendBlock]] =
    for {
      signer1 <- KeyPairGenerator.makeKeyPair[IO]
      signer2 <- KeyPairGenerator.makeKeyPair[IO]
      signer3 <- KeyPairGenerator.makeKeyPair[IO]
      value = AllowSpendBlock(RoundId(new UUID(0L, id)), NonEmptySet.one(allowSpend))
      signed1 <- Signed.forAsyncHasher(value, signer1)
      signed2 <- Signed.forAsyncHasher(value, signer2)
      signed3 <- Signed.forAsyncHasher(value, signer3)
    } yield signed1.addProof(signed2.proofs.head).addProof(signed3.proofs.head)

  test("ML0 admission rejects an incoming allow-spend at the exact expired epoch") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      metagraphKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = metagraphKeyPair.getPublic.toAddress
      source = sourceKeyPair.getPublic.toAddress
      destination = destinationKeyPair.getPublic.toAddress
      initialRef <- AllowSpendReference.emptyCurrency[IO](metagraphId)
      expired <- Signed.forAsyncHasher(
        AllowSpend(
          source = source,
          destination = destination,
          currencyId = CurrencyId(metagraphId).some,
          amount = amount(60L),
          fee = fee(0L),
          parent = initialRef,
          lastValidEpochProgress = epoch(10L),
          approvers = List(destination)
        ),
        sourceKeyPair
      )
      block <- makeSignedBlock(expired, 1L)
      signedValidator = SignedValidator.make[IO]
      transactionValidator = SignedAllowSpendValidator.make[IO](AddressesConfig(Set.empty), signedValidator)
      blockValidator = AllowSpendBlockValidator.make[IO](signedValidator, AllowSpendChainValidator.make[IO], transactionValidator)
      acceptanceManager = AllowSpendBlockAcceptanceManager.make[IO](blockValidator)
      manager = new BlockAcceptanceOpsManager[IO](null, null, acceptanceManager, Amount.empty)
      snapshotInfo = CurrencySnapshotInfo(
        SortedMap.empty,
        SortedMap(source -> balance(100L)),
        none,
        none,
        none,
        none,
        none,
        none,
        none
      )
      result <- manager.acceptAllowSpendBlocks(
        List(block),
        CurrencySnapshotContext(metagraphId, snapshotInfo),
        SnapshotOrdinal.MinValue,
        initialRef,
        shouldPerformMetagraphSpecificValidations = false,
        lastSyncGlobalSnapshotEpochProgress = epoch(10L)
      )
    } yield expect.all(
      result.accepted.isEmpty,
      result.notAccepted.exists {
        case (`block`, ValidationFailed(reasons)) =>
          reasons.exists {
            case InvalidAllowSpend(_, AllowSpendAlreadyExpired) => true
            case _                                              => false
          }
        case _ => false
      }
    )
  }

  test("consumption and expiration are mutually exclusive terminal releases") { res =>
    implicit val (hasher, securityProvider) = res
    val manager = AllowSpendOpsManager.make[IO]

    for {
      consumedSourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      expiredSourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      consumedSource = consumedSourceKeyPair.getPublic.toAddress
      expiredSource = expiredSourceKeyPair.getPublic.toAddress
      destination = destinationKeyPair.getPublic.toAddress
      consumed <- makeAllowSpend(consumedSourceKeyPair, destination, amountValue = 60L, lastValidEpoch = 9L)
      expired <- makeAllowSpend(expiredSourceKeyPair, destination, amountValue = 60L, lastValidEpoch = 9L)
      consumedHashed <- consumed.toHashed
      active = SortedMap(
        consumedSource -> SortedSet(consumed),
        expiredSource -> SortedSet(expired)
      )
      spend = SpendTransaction(consumedHashed.hash.some, none, amount(40L), consumedSource, destination)
      expiredForRefund <- manager.filterExpiredAllowSpends(active, epoch(10L), List(spend))
      afterExpiryEither <- manager.updateCurrencyBalancesByAllowSpends(
        epoch(10L),
        SortedMap(consumedSource -> balance(40L), expiredSource -> balance(40L), destination -> Balance.empty),
        SortedMap.empty,
        active,
        List(spend)
      )
      afterExpiry <- IO.fromEither(afterExpiryEither.leftMap(new AssertionError(_)))
      afterSettlement <- IO.fromEither(
        manager
          .updateCurrencyBalancesBySpendTransactions(
            afterExpiry,
            SortedMap(consumedSource -> List(consumedHashed)),
            List(spend)
          )
          .leftMap(error => new AssertionError(error.message))
      )
    } yield expect.all(
      expiredForRefund.get(consumedSource).forall(_.isEmpty),
      expiredForRefund.get(expiredSource).contains(SortedSet(expired)),
      afterSettlement.get(consumedSource).contains(balance(60L)),
      afterSettlement.get(expiredSource).contains(balance(100L)),
      afterSettlement.get(destination).contains(balance(40L)),
      total(afterSettlement) == 200L
    )
  }

  test("an unrelated no-reference spend cannot suppress expiration") { res =>
    implicit val (hasher, securityProvider) = res
    val manager = AllowSpendOpsManager.make[IO]

    for {
      reservedSourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      directSourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      reservedSource = reservedSourceKeyPair.getPublic.toAddress
      directSource = directSourceKeyPair.getPublic.toAddress
      destination = destinationKeyPair.getPublic.toAddress
      expired <- makeAllowSpend(reservedSourceKeyPair, destination, amountValue = 60L, lastValidEpoch = 9L)
      active = SortedMap(reservedSource -> SortedSet(expired))
      directSpend = SpendTransaction(none, none, amount(10L), directSource, destination)
      expiredForRefund <- manager.filterExpiredAllowSpends(active, epoch(10L), List(directSpend))
      afterExpiryEither <- manager.updateCurrencyBalancesByAllowSpends(
        epoch(10L),
        SortedMap(reservedSource -> balance(40L), directSource -> balance(50L), destination -> Balance.empty),
        SortedMap.empty,
        active,
        List(directSpend)
      )
      afterExpiry <- IO.fromEither(afterExpiryEither.leftMap(new AssertionError(_)))
      afterSettlement <- IO.fromEither(
        manager
          .updateCurrencyBalancesBySpendTransactions(afterExpiry, SortedMap.empty, List(directSpend))
          .leftMap(error => new AssertionError(error.message))
      )
    } yield expect.all(
      expiredForRefund.get(reservedSource).contains(SortedSet(expired)),
      afterSettlement.get(reservedSource).contains(balance(100L)),
      afterSettlement.get(directSource).contains(balance(40L)),
      afterSettlement.get(destination).contains(balance(10L)),
      total(afterSettlement) == 150L
    )
  }

  test("a no-reference self-transfer proves funds without burning its balance") { res =>
    implicit val securityProvider: SecurityProvider[IO] = res._2
    val manager = AllowSpendOpsManager.make[IO]

    for {
      source <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      spend = SpendTransaction(none, none, amount(60L), source, source)
      initial = SortedMap(source -> balance(100L))
      accepted = manager.updateCurrencyBalancesBySpendTransactions(initial, SortedMap.empty, List(spend))
      insufficient = manager.updateCurrencyBalancesBySpendTransactions(
        SortedMap(source -> balance(59L)),
        SortedMap.empty,
        List(spend)
      )
    } yield expect.all(
      accepted.contains(initial),
      insufficient.left.exists {
        case AllowSpendSettlementError.BalanceArithmetic(_) => true
        case _                                               => false
      }
    )
  }

  test("Some(missing) fails instead of becoming an ordinary balance spend") { res =>
    implicit val securityProvider: SecurityProvider[IO] = res._2
    val manager = AllowSpendOpsManager.make[IO]

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      destination = destinationKeyPair.getPublic.toAddress
      missingRef = Hash("ab" * 32)
      spend = SpendTransaction(missingRef.some, none, amount(10L), source, destination)
      result = manager.updateCurrencyBalancesBySpendTransactions(
        SortedMap(source -> balance(100L), destination -> Balance.empty),
        SortedMap.empty,
        List(spend)
      )
    } yield expect(result.left.exists {
      case AllowSpendSettlementError.MissingAllowSpendReference(`source`, `missingRef`) => true
      case _                                                                            => false
    })
  }

  test("a spend above the referenced allowance fails instead of using a zero remainder") { res =>
    implicit val (hasher, securityProvider) = res
    val manager = AllowSpendOpsManager.make[IO]

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      destination = destinationKeyPair.getPublic.toAddress
      allowSpend <- makeAllowSpend(sourceKeyPair, destination, amountValue = 60L, lastValidEpoch = 20L)
      hashed <- allowSpend.toHashed
      spend = SpendTransaction(hashed.hash.some, none, amount(61L), source, destination)
      result = manager.updateCurrencyBalancesBySpendTransactions(
        SortedMap(source -> balance(40L), destination -> Balance.empty),
        SortedMap(source -> List(hashed)),
        List(spend)
      )
    } yield expect(result.left.exists {
      case AllowSpendSettlementError.SpendAmountExceedsAllowSpend(ref, allowed, attempted) =>
        ref === hashed.hash && allowed === amount(60L) && attempted === amount(61L)
      case _ => false
    })
  }

  test("a referenced authorization is retired after one settlement in a batched global inbox") { res =>
    implicit val (hasher, securityProvider) = res
    val manager = AllowSpendOpsManager.make[IO]

    for {
      metagraphKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = CurrencyId(metagraphKeyPair.getPublic.toAddress)
      source = sourceKeyPair.getPublic.toAddress
      destination = destinationKeyPair.getPublic.toAddress
      allowSpend <- makeAllowSpend(
        sourceKeyPair,
        destination,
        amountValue = 100L,
        lastValidEpoch = 20L,
        currencyId = metagraphId.some
      )
      hashed <- allowSpend.toHashed
      firstOrdinalSpend = SpendTransaction(hashed.hash.some, metagraphId.some, amount(60L), source, destination)
      secondOrdinalReplay = SpendTransaction(hashed.hash.some, metagraphId.some, amount(60L), source, destination)
      result = manager.updateCurrencyBalancesBySpendTransactions(
        SortedMap(source -> Balance.empty, destination -> Balance.empty),
        SortedMap(source -> List(hashed)),
        List(firstOrdinalSpend, secondOrdinalReplay)
      )
    } yield expect(result.left.exists {
      case AllowSpendSettlementError.ReferencedAllowSpendAlreadyConsumed(`source`, ref) => ref === hashed.hash
      case _                                                                             => false
    })
  }

  test("same-address referenced settlement accumulates amount and remainder without erasing value") { res =>
    implicit val (hasher, securityProvider) = res
    val manager = AllowSpendOpsManager.make[IO]

    for {
      metagraphKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = CurrencyId(metagraphKeyPair.getPublic.toAddress)
      source = sourceKeyPair.getPublic.toAddress
      allowSpend <- makeAllowSpend(
        sourceKeyPair,
        source,
        amountValue = 100L,
        lastValidEpoch = 20L,
        currencyId = metagraphId.some,
        feeValue = 7L
      )
      hashed <- allowSpend.toHashed
      spend = SpendTransaction(hashed.hash.some, metagraphId.some, amount(60L), source, source)
      result = manager.updateCurrencyBalancesBySpendTransactions(
        SortedMap(source -> balance(13L)),
        SortedMap(source -> List(hashed)),
        List(spend)
      )
    } yield expect.all(
      result.exists(_.get(source).contains(balance(113L))),
      result.exists(total(_) == 113L)
    )
  }

  test("last-valid equality remains active and conserves the reserved principal when consumed") { res =>
    implicit val (hasher, securityProvider) = res
    val manager = AllowSpendOpsManager.make[IO]

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      destination = destinationKeyPair.getPublic.toAddress
      boundary <- makeAllowSpend(sourceKeyPair, destination, amountValue = 60L, lastValidEpoch = 10L)
      boundaryHashed <- boundary.toHashed
      active = SortedMap(source -> SortedSet(boundary))
      spend = SpendTransaction(boundaryHashed.hash.some, none, amount(40L), source, destination)
      expired <- manager.filterExpiredAllowSpends(active, epoch(10L), List(spend))
      updatedActive <- manager.acceptCurrencyAllowSpends(epoch(10L), SortedMap.empty, active, List(spend))
      afterSettlement <- IO.fromEither(
        manager
          .updateCurrencyBalancesBySpendTransactions(
            SortedMap(source -> balance(40L), destination -> Balance.empty),
            SortedMap(source -> List(boundaryHashed)),
            List(spend)
          )
          .leftMap(error => new AssertionError(error.message))
      )
    } yield expect.all(
      expired.isEmpty,
      updatedActive.get(source).forall(_.isEmpty),
      afterSettlement.get(source).contains(balance(60L)),
      afterSettlement.get(destination).contains(balance(40L)),
      total(afterSettlement) == 100L
    )
  }
}
