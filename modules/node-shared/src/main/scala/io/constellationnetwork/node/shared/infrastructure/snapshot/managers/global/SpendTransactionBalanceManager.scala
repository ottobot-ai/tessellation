package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendTransaction
import io.constellationnetwork.schema.balance.{Amount, Balance, BalanceArithmeticError}
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, StrictMptRead}
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.hash.Hash
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
  )(implicit hasher: Hasher[F]): F[
    Either[SpendTransactionBalanceManager.SpendTransactionBalanceError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]
  ]

  /** Materialize the full `address → Balance` view via the rooted `ActiveAddressIndex`. The Balance value type doesn't carry the address,
    * so the index supplies the keyset and every indexed target must be present and decodable in the same authenticated view. Used by GSAM
    * to source the prior-ordinal `balances` map without consulting `lastSnapshotContext`.
    */
  def materializeAllBalancesFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, Balance]]
}

object SpendTransactionBalanceManager {

  sealed trait SpendTransactionBalanceError extends Product with Serializable
  final case class BalanceArithmeticFailure(address: Address, error: BalanceArithmeticError) extends SpendTransactionBalanceError
  final case class ReferencedAllowSpendNotFound(source: Address, allowSpendRef: Hash) extends SpendTransactionBalanceError
  final case class ReferencedAllowSpendAlreadyConsumed(source: Address, allowSpendRef: Hash) extends SpendTransactionBalanceError
  final case class SpendAmountExceedsAllowSpend(
    allowSpendRef: Hash,
    allowed: SwapAmount,
    attempted: SwapAmount
  ) extends SpendTransactionBalanceError

  def make[F[_]: Async](reader: GlobalStateReader[F]): SpendTransactionBalanceManager[F] =
    new SpendTransactionBalanceManager[F] {

      def updateGlobalBalancesBySpendTransactions(
        currentBalances: SortedMap[Address, Balance],
        allGlobalAllowSpends: SortedMap[Address, List[Hashed[AllowSpend]]],
        globalSpendTransactions: List[SpendTransaction]
      )(implicit hasher: Hasher[F]): F[
        Either[SpendTransactionBalanceError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]
      ] = {
        type AllowSpendIndex = SortedMap[Address, Map[Hash, Hashed[AllowSpend]]]
        type ConsumedReference = (Address, Hash)
        type SettlementState =
          (SortedMap[Address, Balance], SortedMap[Address, Balance], AllowSpendIndex, Set[ConsumedReference])

        val initialAllowSpendIndex: AllowSpendIndex =
          allGlobalAllowSpends.view.mapValues(_.iterator.map(hashed => hashed.hash -> hashed).toMap).to(SortedMap)

        globalSpendTransactions
          .foldM[F, Either[SpendTransactionBalanceError, SettlementState]](
            Right((currentBalances, SortedMap.empty[Address, Balance], initialAllowSpendIndex, Set.empty[ConsumedReference]))
          ) {
            case (Left(err), _) =>
              (Left(err): Either[SpendTransactionBalanceError, SettlementState]).pure[F]
            case (Right((balances, balancesDelta, remainingAllowSpends, consumedAllowSpendRefs)), spendTransaction) =>
              val destinationAddress = spendTransaction.destination
              val sourceAddress = spendTransaction.source
              val spendTransactionAmount = SwapAmount.toAmount(spendTransaction.amount)

              spendTransaction.allowSpendRef match {
                case Some(allowSpendRef) =>
                  val consumedReference = sourceAddress -> allowSpendRef
                  remainingAllowSpends.getOrElse(sourceAddress, Map.empty).get(allowSpendRef) match {
                    case None if consumedAllowSpendRefs.contains(consumedReference) =>
                      (Left(ReferencedAllowSpendAlreadyConsumed(sourceAddress, allowSpendRef)): Either[
                        SpendTransactionBalanceError,
                        SettlementState
                      ]).pure[F]

                    case None =>
                      (Left(ReferencedAllowSpendNotFound(sourceAddress, allowSpendRef)): Either[
                        SpendTransactionBalanceError,
                        SettlementState
                      ]).pure[F]

                    case Some(allowSpend) =>
                      NonNegLong.from(allowSpend.amount.value.value - spendTransactionAmount.value.value) match {
                        case Left(_) =>
                          (Left(SpendAmountExceedsAllowSpend(allowSpendRef, allowSpend.amount, spendTransaction.amount)): Either[
                            SpendTransactionBalanceError,
                            SettlementState
                          ]).pure[F]

                        case Right(balanceToReturnToAddress) =>
                          val sourceAllowSpendAddress = allowSpend.source
                          readBalance(destinationAddress, balances).flatMap { currentDestinationBalance =>
                            currentDestinationBalance.plus(spendTransactionAmount) match {
                              case Left(error) =>
                                (Left(BalanceArithmeticFailure(destinationAddress, error)): Either[
                                  SpendTransactionBalanceError,
                                  SettlementState
                                ]).pure[F]

                              case Right(updatedDestinationBalance) =>
                                val balancesAfterDestination = balances.updated(destinationAddress, updatedDestinationBalance)

                                readBalance(sourceAllowSpendAddress, balancesAfterDestination).map { currentSourceBalance =>
                                  currentSourceBalance
                                    .plus(Amount(balanceToReturnToAddress))
                                    .leftMap(BalanceArithmeticFailure(sourceAllowSpendAddress, _))
                                    .map { updatedSourceBalance =>
                                      val remainingForSource = remainingAllowSpends.getOrElse(sourceAddress, Map.empty) - allowSpendRef
                                      val updatedAllowSpendIndex =
                                        if (remainingForSource.nonEmpty) remainingAllowSpends.updated(sourceAddress, remainingForSource)
                                        else remainingAllowSpends - sourceAddress

                                      (
                                        balancesAfterDestination.updated(sourceAllowSpendAddress, updatedSourceBalance),
                                        balancesDelta
                                          .updated(destinationAddress, updatedDestinationBalance)
                                          .updated(sourceAllowSpendAddress, updatedSourceBalance),
                                        updatedAllowSpendIndex,
                                        consumedAllowSpendRefs + consumedReference
                                      )
                                    }
                                }
                            }
                          }
                      }
                  }

                case None =>
                  if (sourceAddress === destinationAddress)
                    readBalance(sourceAddress, balances).map { currentSourceBalance =>
                      currentSourceBalance
                        .minus(spendTransactionAmount)
                        .leftMap(BalanceArithmeticFailure(sourceAddress, _))
                        .as((balances, balancesDelta, remainingAllowSpends, consumedAllowSpendRefs))
                    }
                  else
                    (readBalance(destinationAddress, balances), readBalance(sourceAddress, balances)).mapN {
                      (currentDestinationBalance, currentSourceBalance) =>
                        for {
                          updatedDestinationBalance <- currentDestinationBalance
                            .plus(spendTransactionAmount)
                            .leftMap(BalanceArithmeticFailure(destinationAddress, _))
                          updatedSourceBalance <- currentSourceBalance
                            .minus(spendTransactionAmount)
                            .leftMap(BalanceArithmeticFailure(sourceAddress, _))
                        } yield
                          (
                            balances
                              .updated(destinationAddress, updatedDestinationBalance)
                              .updated(sourceAddress, updatedSourceBalance),
                            balancesDelta
                              .updated(destinationAddress, updatedDestinationBalance)
                              .updated(sourceAddress, updatedSourceBalance),
                            remainingAllowSpends,
                            consumedAllowSpendRefs
                          )
                    }
              }
          }
          .map(_.map { case (balances, deltas, _, _) => (balances, deltas) })
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
          indexHex <- GlobalStateKey.toHex[F](indexKey)
          addrSet <- StrictMptRead.valueOrElseF(
            reader.getStrict[SortedSet[Address]](indexKey),
            SortedSet.empty[Address],
            "materialize Balances ActiveAddressIndex",
            indexHex
          )
          entries <- addrSet.toList.traverse { addr =>
            val targetKey = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr)
            for {
              targetHex <- GlobalStateKey.toHex[F](targetKey)
              value <- StrictMptRead.requirePresentF(
                reader.getStrict[Balance](targetKey),
                s"materialize Balances indexed target(address=$addr)",
                targetHex
              )
            } yield addr -> value
          }
        } yield SortedMap.from(entries)
    }
}
