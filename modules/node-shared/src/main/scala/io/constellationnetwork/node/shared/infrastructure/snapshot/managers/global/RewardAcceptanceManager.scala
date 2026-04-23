package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.transaction.RewardTransaction
import io.constellationnetwork.security.Hasher

trait RewardAcceptanceManager[F[_]] {

  /** Under the legacy (`shouldUseMptStore = false`) contract, `balances` is the *full* balance map threaded through the ordinal's balance
    * pipeline; a missing recipient is treated as `Balance.empty`.
    *
    * Under the MPT path (`shouldUseMptStore = true`), `balances` is the *delta* map (addresses already touched this ordinal); a missing
    * recipient falls back to `mptStore.getBalance`.
    */
  def acceptRewardTxs(
    balances: SortedMap[Address, Balance],
    txs: SortedSet[RewardTransaction]
  )(implicit hasher: Hasher[F]): F[(SortedMap[Address, Balance], SortedSet[RewardTransaction], SortedMap[Address, Balance])]
}

object RewardAcceptanceManager {

  def make[F[_]: Async](
    mptStore: Option[MptStore[F, GlobalStateKey]] = None,
    shouldUseMptStore: Boolean = false
  ): RewardAcceptanceManager[F] = new RewardAcceptanceManager[F] {

    def acceptRewardTxs(
      balances: SortedMap[Address, Balance],
      txs: SortedSet[RewardTransaction]
    )(implicit hasher: Hasher[F]): F[(SortedMap[Address, Balance], SortedSet[RewardTransaction], SortedMap[Address, Balance])] =
      txs.toList.foldM(
        (balances, SortedSet.empty[RewardTransaction], SortedMap.empty[Address, Balance])
      ) {
        case ((updatedBalances, acceptedTxs, balanceDeltas), tx) =>
          readCurrentBalance(tx.destination, updatedBalances).map { current =>
            current
              .plus(tx.amount)
              .map { newBalance =>
                (
                  updatedBalances.updated(tx.destination, newBalance),
                  acceptedTxs + tx,
                  balanceDeltas.updated(tx.destination, newBalance)
                )
              }
              .getOrElse((updatedBalances, acceptedTxs, balanceDeltas))
          }
      }

    private def readCurrentBalance(
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
