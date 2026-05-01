package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.transaction.{Transaction, TransactionReference}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.addressSetImmutableCodec
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => transactionReferenceImmutableCodec}
import io.constellationnetwork.syntax.sortedCollection._

trait TransactionReferenceManager[F[_]] {
  def acceptTransactionRefs(
    lastTxRefsContextUpdate: Map[Address, TransactionReference],
    acceptedTransactions: SortedSet[Signed[Transaction]]
  ): F[SortedMap[Address, TransactionReference]]

  /** Materialize the full `address → TransactionReference` view via the `ActiveAddressIndex` sidecar. The reference value type doesn't
    * carry the address, so we recover the keyset from the sidecar partition and `getMany` each entry. Used by GSAM to source the
    * prior-ordinal `lastTxRefs` map without consulting `lastSnapshotContext`.
    */
  def materializeLastTxRefsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, TransactionReference]]
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

    def materializeLastTxRefsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, TransactionReference]] =
      for {
        indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastTxRefs)
        addrSet <- mptStore.get[SortedSet[Address]](indexKey).map(_.getOrElse(SortedSet.empty[Address]))
        addrList = addrSet.toList
        keys = addrList.map(addr => GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, addr))
        values <- mptStore.getMany[TransactionReference](keys)
      } yield
        SortedMap.from(addrList.flatMap { addr =>
          val key = GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, addr)
          values.get(key).map(addr -> _)
        })
  }
}
