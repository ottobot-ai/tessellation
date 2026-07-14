package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{AllowSpendExpiration, SharedArtifact, SpendTransaction}
import io.constellationnetwork.schema.balance.{Amount, Balance, BalanceArithmeticError}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.syntax.sortedCollection.sortedSetSyntax

import eu.timepit.refined.types.all.NonNegLong

class AllowSpendOpsManager[F[_]: Async] {
  import AllowSpendOpsManager.AllowSpendSettlementError

  def acceptCurrencyAllowSpends(
    epochProgress: EpochProgress,
    incomingCurrencyAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    existentCurrencyAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    allAcceptedSpendTxns: List[SpendTransaction]
  )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[Signed[AllowSpend]]]] = {
    val allAcceptedSpendTxnsAllowSpendsRefs =
      allAcceptedSpendTxns
        .flatMap(_.allowSpendRef)

    for {
      expiredAllowSpends <- filterExpiredAllowSpends(
        existentCurrencyAllowSpends,
        epochProgress,
        allAcceptedSpendTxns
      )

      unexpiredAllowSpends = (incomingCurrencyAllowSpends |+| expiredAllowSpends).foldLeft(existentCurrencyAllowSpends) {
        case (acc, (address, allowSpends)) =>
          val lastAddressAllowSpends = acc.getOrElse(address, SortedSet.empty[Signed[AllowSpend]])
          val unexpired = (lastAddressAllowSpends ++ allowSpends).filter(_.value.lastValidEpochProgress >= epochProgress)
          acc + (address -> unexpired)
      }

      result <- unexpiredAllowSpends.toList.foldLeftM(unexpiredAllowSpends) {
        case (acc, (address, allowSpends)) =>
          allowSpends.toList.traverse(_.toHashed).map { hashedAllowSpends =>
            val validAllowSpends = hashedAllowSpends
              .filterNot(h => allAcceptedSpendTxnsAllowSpendsRefs.contains(h.hash))
              .map(_.signed)
              .to(SortedSet)

            acc + (address -> validAllowSpends)
          }
      }
    } yield result
  }

  def filterExpiredAllowSpends(
    activeCurrencyAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    epochProgress: EpochProgress,
    metagraphIdSpendTransactions: List[SpendTransaction]
  )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[Signed[AllowSpend]]]] = {
    val consumedAllowSpendRefs = metagraphIdSpendTransactions.flatMap(_.allowSpendRef).toSet

    activeCurrencyAllowSpends.toList.traverse {
      case (address, allowSpends) =>
        allowSpends.toList.traverse { allowSpend =>
          allowSpend.toHashed.map { hashedAllowSpend =>
            val isExpired = allowSpend.value.lastValidEpochProgress < epochProgress
            val isNotConsumed = !consumedAllowSpendRefs.contains(hashedAllowSpend.hash)
            Option.when(isExpired && isNotConsumed)(allowSpend)
          }
        }.map { expiredList =>
          val expiredForAddress = expiredList.flatten.to(SortedSet)
          Option.when(expiredForAddress.nonEmpty)(address -> expiredForAddress)
        }
    }.map(_.flatten.to(SortedMap))
  }

  def updateCurrencyBalancesByAllowSpends(
    epochProgress: EpochProgress,
    currentBalances: SortedMap[Address, Balance],
    incomingCurrencyAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    lastActiveCurrencyAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    metagraphIdSpendTransactions: List[SpendTransaction]
  )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, SortedMap[Address, Balance]]] =
    for {
      expiredCurrencyAllowSpends <- filterExpiredAllowSpends(
        lastActiveCurrencyAllowSpends,
        epochProgress,
        metagraphIdSpendTransactions
      )

      result = (incomingCurrencyAllowSpends |+| expiredCurrencyAllowSpends)
        .foldLeft[Either[BalanceArithmeticError, SortedMap[Address, Balance]]](Right(currentBalances)) {
          case (accEither, (address, allowSpends)) =>
            for {
              acc <- accEither
              initialBalance = acc.getOrElse(address, Balance.empty)
              unexpiredBalance <- {
                val unexpired = allowSpends.filter(_.value.lastValidEpochProgress >= epochProgress)

                unexpired.foldLeft[Either[BalanceArithmeticError, Balance]](Right(initialBalance)) {
                  (currentBalanceEither, signedAllowSpend) =>
                    val allowSpend = signedAllowSpend.value
                    for {
                      currentBalance <- currentBalanceEither
                      balanceAfterAmount <- currentBalance.minus(SwapAmount.toAmount(allowSpend.amount))
                      balanceAfterFee <- balanceAfterAmount.minus(AllowSpendFee.toAmount(allowSpend.fee))
                    } yield balanceAfterFee
                }
              }
              expiredBalance <- {
                val expired = allowSpends.filter(_.value.lastValidEpochProgress < epochProgress)
                expired.foldLeft[Either[BalanceArithmeticError, Balance]](Right(unexpiredBalance)) {
                  (currentBalanceEither, signedAllowSpend) =>
                    val allowSpend = signedAllowSpend.value
                    for {
                      currentBalance <- currentBalanceEither
                      balanceAfterExpiredAmount <- currentBalance.plus(SwapAmount.toAmount(allowSpend.amount))
                    } yield balanceAfterExpiredAmount
                }
              }
              updatedAcc = acc.updated(address, expiredBalance)
            } yield updatedAcc
        }
    } yield result

  def updateCurrencyBalancesBySpendTransactions(
    currentBalances: SortedMap[Address, Balance],
    allActiveCurrencyAllowSpends: SortedMap[Address, List[io.constellationnetwork.security.Hashed[AllowSpend]]],
    metagraphIdSpendTransactions: List[SpendTransaction]
  ): Either[AllowSpendSettlementError, SortedMap[Address, Balance]] = {
    type AllowSpendIndex = SortedMap[Address, Map[Hash, io.constellationnetwork.security.Hashed[AllowSpend]]]
    type ConsumedReference = (Address, Hash)
    type SettlementState = (SortedMap[Address, Balance], AllowSpendIndex, Set[ConsumedReference])

    val initialAllowSpendIndex: AllowSpendIndex =
      allActiveCurrencyAllowSpends.view.mapValues(_.iterator.map(hashed => hashed.hash -> hashed).toMap).to(SortedMap)

    metagraphIdSpendTransactions
      .foldLeft[Either[AllowSpendSettlementError, SettlementState]](
        Right((currentBalances, initialAllowSpendIndex, Set.empty[ConsumedReference]))
      ) { (txnAccEither, spendTransaction) =>
        for {
          state <- txnAccEither
          (txnAcc, remainingAllowSpends, consumedAllowSpendRefs) = state
          destinationAddress = spendTransaction.destination
          sourceAddress = spendTransaction.source

          spendTransactionAmount = SwapAmount.toAmount(spendTransaction.amount)
          currentDestinationBalance = txnAcc.getOrElse(destinationAddress, Balance.empty)

          updatedState <- spendTransaction.allowSpendRef match {
            case Some(allowSpendRef) =>
              val consumedReference = sourceAddress -> allowSpendRef
              remainingAllowSpends.getOrElse(sourceAddress, Map.empty).get(allowSpendRef) match {
                case None if consumedAllowSpendRefs.contains(consumedReference) =>
                  AllowSpendSettlementError
                    .ReferencedAllowSpendAlreadyConsumed(sourceAddress, allowSpendRef)
                    .asLeft[SettlementState]

                case None =>
                  AllowSpendSettlementError
                    .MissingAllowSpendReference(sourceAddress, allowSpendRef)
                    .asLeft[SettlementState]

                case Some(allowSpend) =>
                  val sourceAllowSpendAddress = allowSpend.source
                  val balanceToReturnToAddress = BigInt(allowSpend.amount.value.value) - BigInt(spendTransactionAmount.value.value)

                  if (balanceToReturnToAddress < 0)
                    AllowSpendSettlementError
                      .SpendAmountExceedsAllowSpend(allowSpendRef, allowSpend.amount, spendTransaction.amount)
                      .asLeft[SettlementState]
                  else
                    for {
                      updatedDestinationBalance <- currentDestinationBalance
                        .plus(spendTransactionAmount)
                        .leftMap(AllowSpendSettlementError.BalanceArithmetic)
                      balancesAfterDestination = txnAcc.updated(destinationAddress, updatedDestinationBalance)
                      currentSourceBalance = balancesAfterDestination.getOrElse(sourceAllowSpendAddress, Balance.empty)
                      updatedSourceBalance <- currentSourceBalance
                        .plus(Amount(NonNegLong.unsafeFrom(balanceToReturnToAddress.longValue)))
                        .leftMap(AllowSpendSettlementError.BalanceArithmetic)
                      updatedBalances = balancesAfterDestination.updated(sourceAllowSpendAddress, updatedSourceBalance)
                      remainingForSource = remainingAllowSpends.getOrElse(sourceAddress, Map.empty) - allowSpendRef
                      updatedAllowSpendIndex =
                        if (remainingForSource.nonEmpty) remainingAllowSpends.updated(sourceAddress, remainingForSource)
                        else remainingAllowSpends - sourceAddress
                    } yield (updatedBalances, updatedAllowSpendIndex, consumedAllowSpendRefs + consumedReference)
              }

            case None =>
              val currentSourceBalance = txnAcc.getOrElse(sourceAddress, Balance.empty)

              if (sourceAddress === destinationAddress)
                currentSourceBalance
                  .minus(spendTransactionAmount)
                  .leftMap(AllowSpendSettlementError.BalanceArithmetic)
                  .as((txnAcc, remainingAllowSpends, consumedAllowSpendRefs))
              else
                for {
                  updatedDestinationBalance <- currentDestinationBalance
                    .plus(spendTransactionAmount)
                    .leftMap(AllowSpendSettlementError.BalanceArithmetic)
                  updatedSourceBalance <- currentSourceBalance
                    .minus(spendTransactionAmount)
                    .leftMap(AllowSpendSettlementError.BalanceArithmetic)
                } yield
                  (
                    txnAcc
                      .updated(destinationAddress, updatedDestinationBalance)
                      .updated(sourceAddress, updatedSourceBalance),
                    remainingAllowSpends,
                    consumedAllowSpendRefs
                  )
          }
        } yield updatedState
      }
      .map(_._1)
  }

  def emitAllowSpendsExpired(
    addressToSet: SortedMap[Address, SortedSet[Signed[AllowSpend]]]
  )(implicit hasher: Hasher[F]): F[SortedSet[SharedArtifact]] =
    addressToSet.values.flatten.toList
      .traverse(_.toHashed)
      .map(_.map(hashed => AllowSpendExpiration(hashed.hash): SharedArtifact).toSortedSet)
}

object AllowSpendOpsManager {
  sealed trait AllowSpendSettlementError extends Product with Serializable {
    def message: String
  }

  object AllowSpendSettlementError {
    final case class BalanceArithmetic(error: BalanceArithmeticError) extends AllowSpendSettlementError {
      val message: String = s"Balance arithmetic error: $error"
    }

    final case class MissingAllowSpendReference(source: Address, allowSpendRef: Hash) extends AllowSpendSettlementError {
      val message: String = s"Allow-spend reference $allowSpendRef is missing for source $source"
    }

    final case class ReferencedAllowSpendAlreadyConsumed(source: Address, allowSpendRef: Hash) extends AllowSpendSettlementError {
      val message: String = s"Allow-spend reference $allowSpendRef was consumed more than once for source $source"
    }

    final case class SpendAmountExceedsAllowSpend(
      allowSpendRef: Hash,
      allowSpendAmount: SwapAmount,
      spendAmount: SwapAmount
    ) extends AllowSpendSettlementError {
      val message: String =
        s"Spend amount ${spendAmount.value.value} exceeds allow-spend amount ${allowSpendAmount.value.value} for reference $allowSpendRef"
    }
  }

  def make[F[_]: Async]: AllowSpendOpsManager[F] =
    new AllowSpendOpsManager[F]
}
