package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendTransaction
import io.constellationnetwork.schema.balance.{Amount, Balance, BalanceArithmeticError}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.syntax.sortedCollection.sortedSetSyntax

/** Result of allow spend acceptance containing full state, deltas, and removed keys */
case class AllowSpendAcceptanceResult(
  fullState: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
  deltas: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
  removedKeys: Set[(Option[Address], Address)] = Set.empty
)

trait AllowSpendStateManager[F[_]] {
  def acceptAllowSpends(
    epochProgress: EpochProgress,
    activeAllowSpendsFromCurrencySnapshots: SortedMap[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    allAcceptedSpendTxns: List[SpendTransaction]
  )(implicit hasher: Hasher[F]): F[AllowSpendAcceptanceResult]

  def acceptAllowSpendRefs(
    lastAllowSpendRefs: SortedMap[Address, AllowSpendReference],
    lastAllowSpendContextUpdate: Map[Address, AllowSpendReference]
  ): SortedMap[Address, AllowSpendReference]

  def filterExpiredAllowSpends(
    allowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    epochProgress: EpochProgress
  ): SortedMap[Address, SortedSet[Signed[AllowSpend]]]

  def updateGlobalBalancesByAllowSpends(
    epochProgress: EpochProgress,
    currentBalances: SortedMap[Address, Balance],
    globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
  )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]]
}

object AllowSpendStateManager {

  def make[F[_]: Async](
    mptStore: Option[MptStore[F, GlobalStateKey]] = None,
    shouldUseMptStore: Boolean = false
  ): AllowSpendStateManager[F] = new AllowSpendStateManager[F] {

    def acceptAllowSpends(
      epochProgress: EpochProgress,
      activeAllowSpendsFromCurrencySnapshots: SortedMap[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
      lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      allAcceptedSpendTxns: List[SpendTransaction]
    )(implicit hasher: Hasher[F]): F[AllowSpendAcceptanceResult] = {
      val allAcceptedSpendTxnsAllowSpendsRefs =
        allAcceptedSpendTxns
          .flatMap(_.allowSpendRef)

      val lastActiveGlobalAllowSpends = lastActiveAllowSpends.getOrElse(None, SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])
      val expiredGlobalAllowSpends = filterExpiredAllowSpends(lastActiveGlobalAllowSpends, epochProgress)

      val unexpiredGlobalAllowSpends = (globalAllowSpends |+| expiredGlobalAllowSpends).foldLeft(lastActiveGlobalAllowSpends) {
        case (acc, (address, allowSpends)) =>
          val lastAddressAllowSpends = acc.getOrElse(address, SortedSet.empty[Signed[AllowSpend]])
          val unexpired = (lastAddressAllowSpends ++ allowSpends).filter(_.lastValidEpochProgress >= epochProgress)
          acc + (address -> unexpired)
      }

      val unexpiredGlobalWithoutSpendTransactionsF =
        unexpiredGlobalAllowSpends.toList.foldLeftM(unexpiredGlobalAllowSpends) {
          case (acc, (address, allowSpends)) =>
            allowSpends.toList.traverse(_.toHashed).map { hashedAllowSpends =>
              val validAllowSpends = hashedAllowSpends
                .filterNot(h => allAcceptedSpendTxnsAllowSpendsRefs.contains(h.hash))
                .map(_.signed)
                .to(SortedSet)

              acc + (address -> validAllowSpends)
            }
        }

      def processMetagraphAllowSpends(
        metagraphId: Address,
        metagraphAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
        accAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        accDeltas: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      ): F[
        (
          SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
          SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
        )
      ] = {
        val lastActiveMetagraphAllowSpends =
          accAllowSpends.getOrElse(metagraphId.some, SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])

        metagraphAllowSpends.toList.traverse {
          case (address, addressAllowSpends) =>
            val lastAddressAllowSpends = lastActiveMetagraphAllowSpends.getOrElse(address, SortedSet.empty[Signed[AllowSpend]])

            val unexpired = (lastAddressAllowSpends ++ addressAllowSpends)
              .filter(_.lastValidEpochProgress >= epochProgress)

            val unexpiredWithoutSpendTransactions = unexpired.toList
              .traverse(_.toHashed)
              .map { hashedAllowSpends =>
                hashedAllowSpends.filterNot(h => allAcceptedSpendTxnsAllowSpendsRefs.contains(h.hash))
              }
              .map(_.map(_.signed).toSortedSet)

            unexpiredWithoutSpendTransactions.map { validAllowSpends =>
              val hasChanged = lastAddressAllowSpends != validAllowSpends
              (address, validAllowSpends, hasChanged)
            }
        }.map { updatedMetagraphAllowSpends =>
          val fullStateMap = SortedMap(updatedMetagraphAllowSpends.map { case (addr, spends, _) => addr -> spends }: _*)
          // Filter out empty sets - those are removals tracked separately in removedKeys
          val deltasMap = SortedMap(updatedMetagraphAllowSpends.collect {
            case (addr, spends, true) if spends.nonEmpty => addr -> spends
          }: _*)

          val updatedFullState = accAllowSpends + (metagraphId.some -> fullStateMap)
          val updatedDeltas = if (deltasMap.nonEmpty) {
            accDeltas + (metagraphId.some -> deltasMap)
          } else {
            accDeltas
          }

          (updatedFullState, updatedDeltas)
        }
      }

      // Process metagraph allow spends and track deltas
      val processedMetagraphsF = activeAllowSpendsFromCurrencySnapshots.toList
        .foldLeft((lastActiveAllowSpends, SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]).pure[F]) {
          case (accF, (metagraphId, metagraphAllowSpends)) =>
            for {
              (accFullState, accDeltas) <- accF
              (updatedFullState, updatedDeltas) <- processMetagraphAllowSpends(metagraphId, metagraphAllowSpends, accFullState, accDeltas)
            } yield (updatedFullState, updatedDeltas)
        }

      for {
        (updatedCurrencyAllowSpends, currencyDeltas) <- processedMetagraphsF
        validGlobalAllowSpends <- unexpiredGlobalWithoutSpendTransactionsF
      } yield {
        // Compute global deltas by comparing with previous state
        // Filter out empty sets - those are removals tracked separately in removedKeys
        val globalDeltas: SortedMap[Address, SortedSet[Signed[AllowSpend]]] =
          validGlobalAllowSpends.filter {
            case (address, allowSpends) =>
              allowSpends.nonEmpty && !lastActiveGlobalAllowSpends.get(address).contains(allowSpends)
          }

        val fullState = if (validGlobalAllowSpends.nonEmpty) {
          updatedCurrencyAllowSpends + (None -> validGlobalAllowSpends)
        } else {
          updatedCurrencyAllowSpends
        }

        val deltas = if (globalDeltas.nonEmpty) {
          currencyDeltas + (None -> globalDeltas)
        } else {
          currencyDeltas
        }

        // Compute removed keys: addresses that had AllowSpends but now have empty or missing sets
        val removedKeys: Set[(Option[Address], Address)] = lastActiveAllowSpends.flatMap {
          case (metagraphIdOpt, innerMap) =>
            innerMap.collect {
              case (address, spends)
                  if spends.nonEmpty &&
                    !fullState.get(metagraphIdOpt).flatMap(_.get(address)).exists(_.nonEmpty) =>
                (metagraphIdOpt, address)
            }
        }.toSet

        AllowSpendAcceptanceResult(fullState, deltas, removedKeys)
      }
    }

    def acceptAllowSpendRefs(
      lastAllowSpendRefs: SortedMap[Address, AllowSpendReference],
      lastAllowSpendContextUpdate: Map[Address, AllowSpendReference]
    ): SortedMap[Address, AllowSpendReference] =
      lastAllowSpendRefs ++ lastAllowSpendContextUpdate

    def filterExpiredAllowSpends(
      allowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
      epochProgress: EpochProgress
    ): SortedMap[Address, SortedSet[Signed[AllowSpend]]] =
      allowSpends.view.mapValues(_.filter(_.lastValidEpochProgress < epochProgress)).to(SortedMap)

    def updateGlobalBalancesByAllowSpends(
      epochProgress: EpochProgress,
      currentBalances: SortedMap[Address, Balance],
      globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
      lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
    )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]] = {
      val lastActiveGlobalAllowSpends = lastActiveAllowSpends.getOrElse(None, SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])
      val expiredGlobalAllowSpends = filterExpiredAllowSpends(lastActiveGlobalAllowSpends, epochProgress)

      (globalAllowSpends |+| expiredGlobalAllowSpends).toList
        .foldM[F, Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]](
          Right((currentBalances, SortedMap.empty[Address, Balance]))
        ) {
          case (Left(err), _) =>
            (Left(err): Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]).pure[F]
          case (Right((balances, balancesDelta)), (address, allowSpends)) =>
            readBalance(address, balances).map { initialBalance =>
              val unexpiredBalance: Either[BalanceArithmeticError, Balance] = {
                val unexpired = allowSpends.filter(_.lastValidEpochProgress >= epochProgress)
                unexpired.foldLeft[Either[BalanceArithmeticError, Balance]](Right(initialBalance)) { (currentBalanceEither, allowSpend) =>
                  for {
                    currentBalance <- currentBalanceEither
                    balanceAfterAmount <- currentBalance.minus(SwapAmount.toAmount(allowSpend.amount))
                    balanceAfterFee <- balanceAfterAmount.minus(AllowSpendFee.toAmount(allowSpend.fee))
                  } yield balanceAfterFee
                }
              }

              for {
                unexpired <- unexpiredBalance
                expired <- {
                  val expiredSet = allowSpends.filter(_.lastValidEpochProgress < epochProgress)
                  expiredSet.foldLeft[Either[BalanceArithmeticError, Balance]](Right(unexpired)) { (currentBalanceEither, allowSpend) =>
                    for {
                      currentBalance <- currentBalanceEither
                      balanceAfterExpiredAmount <- currentBalance.plus(SwapAmount.toAmount(allowSpend.amount))
                    } yield balanceAfterExpiredAmount
                  }
                }
              } yield
                (
                  balances.updated(address, expired),
                  balancesDelta.updated(address, expired)
                )
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
