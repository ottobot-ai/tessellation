package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendTransaction
import io.constellationnetwork.schema.balance.{Amount, Balance, BalanceArithmeticError}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.{Hashed, Hasher}

import eu.timepit.refined.types.numeric.NonNegLong

trait SpendTransactionBalanceManager[F[_]] {

  /** Legacy (`shouldUseMptStore = false`): `currentBalances` is the full balance map threaded through the ordinal's pipeline; missing
    * sources/destinations default to `Balance.empty`.
    *
    * MPT (`shouldUseMptStore = true`): `currentBalances` is the delta of addresses already touched this ordinal; missing entries fall back
    * to `mptStore.getBalance`.
    */
  def updateGlobalBalancesBySpendTransactions(
    currentBalances: SortedMap[Address, Balance],
    allGlobalAllowSpends: SortedMap[Address, List[Hashed[AllowSpend]]],
    globalSpendTransactions: List[SpendTransaction]
  )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]]
}

object SpendTransactionBalanceManager {

  def make[F[_]: Async](
    mptStore: Option[MptStore[F, GlobalStateKey]] = None,
    shouldUseMptStore: Boolean = false
  ): SpendTransactionBalanceManager[F] = new SpendTransactionBalanceManager[F] {

    def updateGlobalBalancesBySpendTransactions(
      currentBalances: SortedMap[Address, Balance],
      allGlobalAllowSpends: SortedMap[Address, List[Hashed[AllowSpend]]],
      globalSpendTransactions: List[SpendTransaction]
    )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]] =
      globalSpendTransactions.foldM[F, Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]](
        Right((currentBalances, SortedMap.empty[Address, Balance]))
      ) {
        case (Left(err), _) =>
          (Left(err): Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]).pure[F]
        case (Right((balances, balancesDelta)), spendTransaction) =>
          val destinationAddress = spendTransaction.destination
          val sourceAddress = spendTransaction.source
          val addressAllowSpends = allGlobalAllowSpends.getOrElse(sourceAddress, List.empty)
          val spendTransactionAmount = SwapAmount.toAmount(spendTransaction.amount)

          readBalance(destinationAddress, balances).flatMap { currentDestinationBalance =>
            spendTransaction.allowSpendRef.flatMap(ref => addressAllowSpends.find(_.hash === ref)) match {
              case Some(allowSpend) =>
                val sourceAllowSpendAddress = allowSpend.source
                val balanceToReturnToAddress = allowSpend.amount.value.value - spendTransactionAmount.value.value

                readBalance(sourceAllowSpendAddress, balances).map { currentSourceBalance =>
                  for {
                    updatedDestinationBalance <- currentDestinationBalance.plus(spendTransactionAmount)
                    updatedSourceBalance <- currentSourceBalance.plus(
                      Amount(NonNegLong.from(balanceToReturnToAddress).getOrElse(NonNegLong.MinValue))
                    )
                  } yield
                    (
                      balances
                        .updated(destinationAddress, updatedDestinationBalance)
                        .updated(sourceAllowSpendAddress, updatedSourceBalance),
                      balancesDelta
                        .updated(destinationAddress, updatedDestinationBalance)
                        .updated(sourceAllowSpendAddress, updatedSourceBalance)
                    )
                }

              case None =>
                readBalance(sourceAddress, balances).map { currentSourceBalance =>
                  for {
                    updatedDestinationBalance <- currentDestinationBalance.plus(spendTransactionAmount)
                    updatedSourceBalance <- currentSourceBalance.minus(spendTransactionAmount)
                  } yield
                    (
                      balances
                        .updated(destinationAddress, updatedDestinationBalance)
                        .updated(sourceAddress, updatedSourceBalance),
                      balancesDelta
                        .updated(destinationAddress, updatedDestinationBalance)
                        .updated(sourceAddress, updatedSourceBalance)
                    )
                }
            }
          }
      }

    private def readBalance(
      address: Address,
      deltas: SortedMap[Address, Balance]
    )(implicit hasher: Hasher[F]): F[Balance] =
      deltas.get(address) match {
        case Some(b) => b.pure[F]
        case None =>
          if (shouldUseMptStore) mptStore.fold(Balance.empty.pure[F])(_.getBalance(address).map(_.getOrElse(Balance.empty)))
          else Balance.empty.pure[F]
      }
  }
}
