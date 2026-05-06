package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendTransaction
import io.constellationnetwork.schema.balance.{Amount, Balance, BalanceArithmeticError}
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.{Hashed, Hasher}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.addressSetImmutableCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.types.numeric.NonNegLong

trait SpendTransactionBalanceManager[F[_]] {

  /** `currentBalances` is the in-ordinal balance delta — addresses already touched this ordinal. Sources/destinations missing from the
    * delta are read from `mptStore.getBalance`, falling back to `Balance.empty` if absent.
    */
  def updateGlobalBalancesBySpendTransactions(
    currentBalances: SortedMap[Address, Balance],
    allGlobalAllowSpends: SortedMap[Address, List[Hashed[AllowSpend]]],
    globalSpendTransactions: List[SpendTransaction]
  )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]]

  /** Materialize the full `address → Balance` view via the `ActiveAddressIndex` sidecar. The Balance value type doesn't carry the address,
    * so we recover the keyset from the sidecar partition and `getMany` each entry. Used by GSAM to source the prior-ordinal `balances` map
    * without consulting `lastSnapshotContext`.
    */
  def materializeAllBalancesFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, Balance]]
}

object SpendTransactionBalanceManager {

  def make[F[_]: Async](reader: GlobalStateReader[F]): SpendTransactionBalanceManager[F] =
    new SpendTransactionBalanceManager[F] {

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

      private def readBalance(address: Address, deltas: SortedMap[Address, Balance]): F[Balance] =
        deltas.get(address) match {
          case Some(b) => b.pure[F]
          case None =>
            reader.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, address)).map(_.getOrElse(Balance.empty))
        }

      def materializeAllBalancesFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, Balance]] =
        for {
          indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.Balances)
          addrSet <- reader.get[SortedSet[Address]](indexKey).map(_.getOrElse(SortedSet.empty[Address]))
          addrList = addrSet.toList
          keys = addrList.map(addr => GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr))
          values <- reader.getMany[Balance](keys)
        } yield
          SortedMap.from(addrList.flatMap { addr =>
            val key = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr)
            values.get(key).map(addr -> _)
          })
    }
}
