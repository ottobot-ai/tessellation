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

import org.typelevel.log4cats.slf4j.Slf4jLogger

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

    private val logger = Slf4jLogger.getLoggerFromClass[F](TransactionReferenceManager.getClass)

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

    /** Diagnostic: run the MPT path purely for instrumentation and compare against the legacy path. Logs every divergence (addresses where
      * the two paths disagree on whether to add an empty-ref) but always returns the legacy result so production behavior is unchanged.
      * Drop this once we understand why MPT.LastTxRefs has gaps relative to GSI.lastTxRefs.
      */
    private def runDiagAndUseLegacy(
      lastTxRefs: SortedMap[Address, TransactionReference],
      lastTxRefsContextUpdate: Map[Address, TransactionReference],
      acceptedTransactions: SortedSet[Signed[Transaction]]
    ): F[SortedMap[Address, TransactionReference]] = {
      val destinations = acceptedTransactions.map(_.destination)
      for {
        legacyResult <- useGlobalSnapshotInfo(lastTxRefs, lastTxRefsContextUpdate, acceptedTransactions)
        mptResult <- useMptStore(lastTxRefsContextUpdate, acceptedTransactions)
        // Per-destination MPT lookup for fine-grained divergence reporting.
        perDestLookup <- destinations.toList.traverse(addr => mptStore.getTxRef(addr).map(addr -> _))
        legacyOnly = legacyResult.keySet -- mptResult.keySet
        mptOnly = mptResult.keySet -- legacyResult.keySet
        sharedDisagreement = legacyResult.keySet.intersect(mptResult.keySet).filter(k => legacyResult.get(k) != mptResult.get(k))
        _ <-
          if (legacyOnly.nonEmpty || mptOnly.nonEmpty || sharedDisagreement.nonEmpty) {
            val mptHas = perDestLookup.collect { case (a, Some(_)) => a.value.value.take(12) }.mkString(",")
            val mptMissing = perDestLookup.collect { case (a, None) => a.value.value.take(12) }.mkString(",")
            val gsiHas = lastTxRefs.keySet.toList.map(_.value.value.take(12)).mkString(",")
            logger.warn(
              s"[TX_REF_DIVERGE] dests=${destinations.size} ctxUpd=${lastTxRefsContextUpdate.size} " +
                s"gsi.lastTxRefs.size=${lastTxRefs.size} gsi.lastTxRefs.keys=[$gsiHas] " +
                s"legacyOnly=${legacyOnly.size} mptOnly=${mptOnly.size} sharedDisagreement=${sharedDisagreement.size} " +
                s"perDest.mptHas=[$mptHas] perDest.mptMissing=[$mptMissing] " +
                s"legacy.keys=${legacyResult.keySet.map(_.value.value.take(12))} " +
                s"mpt.keys=${mptResult.keySet.map(_.value.value.take(12))}"
            )
          } else
            logger.debug(s"[TX_REF_AGREE] dests=${destinations.size} ctxUpd=${lastTxRefsContextUpdate.size} keys=${legacyResult.size}")
      } yield legacyResult
    }

    def acceptTransactionRefs(
      lastTxRefs: SortedMap[Address, TransactionReference],
      lastTxRefsContextUpdate: Map[Address, TransactionReference],
      acceptedTransactions: SortedSet[Signed[Transaction]]
    ): F[SortedMap[Address, TransactionReference]] =
      if (shouldUseMptStore) runDiagAndUseLegacy(lastTxRefs, lastTxRefsContextUpdate, acceptedTransactions)
      else useGlobalSnapshotInfo(lastTxRefs, lastTxRefsContextUpdate, acceptedTransactions)
  }
}
