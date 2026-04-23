package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.transaction.{Transaction, TransactionReference}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.syntax.sortedCollection._

trait TransactionReferenceManager[F[_]] {
  def acceptTransactionRefs(
    lastTxRefs: SortedMap[Address, TransactionReference],
    lastTxRefsContextUpdate: Map[Address, TransactionReference],
    acceptedTransactions: SortedSet[Signed[Transaction]]
  ): F[SortedMap[Address, TransactionReference]]
}

object TransactionReferenceManager {

  def make[F[_]: Async](
    mptStore: MptStore[F, GlobalStateKey],
    shouldUseMptStore: Boolean
  ): TransactionReferenceManager[F] = new TransactionReferenceManager[F] {

    private def useGlobalSnapshotInfo(
      lastTxRefs: SortedMap[Address, TransactionReference],
      lastTxRefsContextUpdate: Map[Address, TransactionReference],
      acceptedTransactions: SortedSet[Signed[Transaction]]
    ): F[SortedMap[Address, TransactionReference]] = {
      val updatedKeys = lastTxRefs.keySet ++ lastTxRefsContextUpdate.keySet
      val newDestinationAddresses = acceptedTransactions.map(_.destination) -- updatedKeys
      val newDestinationAddressesRefs = newDestinationAddresses.toList.map(_ -> TransactionReference.empty)
      (lastTxRefsContextUpdate ++ newDestinationAddressesRefs).toSortedMap.pure[F]
    }

    private def useMptStore(
      lastTxRefsContextUpdate: Map[Address, TransactionReference],
      acceptedTransactions: SortedSet[Signed[Transaction]]
    ): F[SortedMap[Address, TransactionReference]] = {
      val destinationsNeedingLookup =
        (acceptedTransactions.map(_.destination) -- lastTxRefsContextUpdate.keySet).toList
      destinationsNeedingLookup.traverseFilter { addr =>
        mptStore.getTxRef(addr).map {
          case None => (addr -> TransactionReference.empty).some
          case _    => none
        }
      }
        .map(newDestRefs => (lastTxRefsContextUpdate ++ newDestRefs).toSortedMap)
    }

    def acceptTransactionRefs(
      lastTxRefs: SortedMap[Address, TransactionReference],
      lastTxRefsContextUpdate: Map[Address, TransactionReference],
      acceptedTransactions: SortedSet[Signed[Transaction]]
    ): F[SortedMap[Address, TransactionReference]] =
      if (shouldUseMptStore) useMptStore(lastTxRefsContextUpdate, acceptedTransactions)
      else useGlobalSnapshotInfo(lastTxRefs, lastTxRefsContextUpdate, acceptedTransactions)
  }
}
