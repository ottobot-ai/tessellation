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
    lastTxRefsContextUpdate: Map[Address, TransactionReference],
    acceptedTransactions: SortedSet[Signed[Transaction]]
  ): F[SortedMap[Address, TransactionReference]]
}

object TransactionReferenceManager {

  def make[F[_]: Async](mptStore: MptStore[F, GlobalStateKey]): TransactionReferenceManager[F] = new TransactionReferenceManager[F] {

    def acceptTransactionRefs(
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
  }
}
