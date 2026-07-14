package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, StrictMptRead}
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

  /** Materialize the full `address → TransactionReference` view via the rooted `ActiveAddressIndex`. The reference value type doesn't carry
    * the address, so the index supplies the keyset and every indexed target must be present and decodable in the same authenticated view.
    * Used by GSAM to source the prior-ordinal `lastTxRefs` map without consulting `lastSnapshotContext`.
    */
  def materializeLastTxRefsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, TransactionReference]]
}

object TransactionReferenceManager {

  def make[F[_]: Async](reader: GlobalStateReader[F]): TransactionReferenceManager[F] = new TransactionReferenceManager[F] {

    def acceptTransactionRefs(
      lastTxRefsContextUpdate: Map[Address, TransactionReference],
      acceptedTransactions: SortedSet[Signed[Transaction]]
    ): F[SortedMap[Address, TransactionReference]] = {
      val destinationsNeedingLookup =
        (acceptedTransactions.map(_.destination) -- lastTxRefsContextUpdate.keySet).toList
      destinationsNeedingLookup.traverseFilter { addr =>
        reader.get[TransactionReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, addr)).map {
          case None => (addr -> TransactionReference.empty).some
          case _    => none
        }
      }
        .map(newDestRefs => (lastTxRefsContextUpdate ++ newDestRefs).toSortedMap)
    }

    def materializeLastTxRefsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, TransactionReference]] =
      for {
        indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastTxRefs)
        indexHex <- GlobalStateKey.toHex[F](indexKey)
        addrSet <- StrictMptRead.valueOrElseF(
          reader.getStrict[SortedSet[Address]](indexKey),
          SortedSet.empty[Address],
          "materialize LastTxRefs ActiveAddressIndex",
          indexHex
        )
        entries <- addrSet.toList.traverse { addr =>
          val targetKey = GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, addr)
          for {
            targetHex <- GlobalStateKey.toHex[F](targetKey)
            value <- StrictMptRead.requirePresentF(
              reader.getStrict[TransactionReference](targetKey),
              s"materialize LastTxRefs indexed target(address=$addr)",
              targetHex
            )
          } yield addr -> value
        }
      } yield SortedMap.from(entries)
  }
}
