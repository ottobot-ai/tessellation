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

  /** `balanceDeltas` is the in-ordinal balance map for addresses already touched this ordinal. A reward recipient missing from
    * `balanceDeltas` is read from `mptStore.getBalance`; missing in MPT means `Balance.empty`.
    */
  def acceptRewardTxs(
    balanceDeltas: SortedMap[Address, Balance],
    txs: SortedSet[RewardTransaction]
  )(implicit hasher: Hasher[F]): F[(SortedMap[Address, Balance], SortedSet[RewardTransaction], SortedMap[Address, Balance])]
}

object RewardAcceptanceManager {

  def make[F[_]: Async](mptStore: MptStore[F, GlobalStateKey]): RewardAcceptanceManager[F] = new RewardAcceptanceManager[F] {

    def acceptRewardTxs(
      balanceDeltas: SortedMap[Address, Balance],
      txs: SortedSet[RewardTransaction]
    )(implicit hasher: Hasher[F]): F[(SortedMap[Address, Balance], SortedSet[RewardTransaction], SortedMap[Address, Balance])] =
      txs.toList.foldM(
        (balanceDeltas, SortedSet.empty[RewardTransaction], SortedMap.empty[Address, Balance])
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
    ): F[Balance] =
      deltas.get(address) match {
        case Some(b) => b.pure[F]
        case None    => mptStore.getBalance(address).map(_.getOrElse(Balance.empty))
      }
  }
}
