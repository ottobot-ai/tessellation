package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.Parallel
import cats.effect.kernel.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{GlobalSnapshotsProcessed, SpendAction, SpendTransaction}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo

import monocle.syntax.all._

case class MetagraphSyncAcceptanceResult(
  fullState: SortedMap[Address, MetagraphSyncDataInfo],
  deltas: SortedMap[Address, MetagraphSyncDataInfo]
)

trait MetagraphSyncManager[F[_]] {
  def acceptMetagraphSyncData(
    existingData: SortedMap[Address, MetagraphSyncDataInfo],
    incomingCurrencySnapshots: SortedMap[Address, List[CurrencySnapshotWithState]],
    globalSnapshotsProcessed: Map[Address, List[GlobalSnapshotsProcessed]],
    acceptedSpendActions: Map[Address, List[SpendAction]],
    currentGlobalOrdinal: SnapshotOrdinal,
    currentGlobalEpochProgress: EpochProgress
  ): F[MetagraphSyncAcceptanceResult]
}

object MetagraphSyncManager {

  private[global] def pendingAfterAcknowledgements(
    current: SortedSet[SnapshotOrdinal],
    processed: Iterable[GlobalSnapshotsProcessed]
  ): SortedSet[SnapshotOrdinal] =
    current -- processed.iterator.flatMap(_.ordinals).toSet

  /** This is a consensus acknowledgement queue, not a cache. Never discard an unacknowledged ordinal: absence is the proof used to retire a
    * consumed-allow-spend balance overlay.
    */
  private[global] def appendPendingOrdinal(
    current: SortedSet[SnapshotOrdinal],
    next: SnapshotOrdinal
  ): SortedSet[SnapshotOrdinal] = current + next

  def make[F[_]: Async: Parallel]: F[MetagraphSyncManager[F]] =
    Async[F].pure {
      new MetagraphSyncManager[F] {

        def acceptMetagraphSyncData(
          existingData: SortedMap[Address, MetagraphSyncDataInfo],
          incomingCurrencySnapshots: SortedMap[Address, List[CurrencySnapshotWithState]],
          globalSnapshotsProcessed: Map[Address, List[GlobalSnapshotsProcessed]],
          acceptedSpendActions: Map[Address, List[SpendAction]],
          currentGlobalOrdinal: SnapshotOrdinal,
          currentGlobalEpochProgress: EpochProgress
        ): F[MetagraphSyncAcceptanceResult] =
          for {
            (updatedFromSnapshots, snapshotDeltas) <- updateFromCurrencySnapshots(
              existingData,
              incomingCurrencySnapshots,
              globalSnapshotsProcessed,
              currentGlobalOrdinal,
              currentGlobalEpochProgress
            )

            (updatedFromSpendActions, spendActionDeltas) <- updateFromSpendActions(
              updatedFromSnapshots,
              acceptedSpendActions,
              currentGlobalOrdinal
            )

          } yield {
            val mergedDeltas = snapshotDeltas ++ spendActionDeltas
            MetagraphSyncAcceptanceResult(updatedFromSpendActions, mergedDeltas)
          }

        private def updateFromCurrencySnapshots(
          existingData: SortedMap[Address, MetagraphSyncDataInfo],
          incomingCurrencySnapshots: SortedMap[Address, List[CurrencySnapshotWithState]],
          globalSnapshotsProcessed: Map[Address, List[GlobalSnapshotsProcessed]],
          currentOrdinal: SnapshotOrdinal,
          currentEpochProgress: EpochProgress
        ): F[(SortedMap[Address, MetagraphSyncDataInfo], SortedMap[Address, MetagraphSyncDataInfo])] =
          incomingCurrencySnapshots.toList.parTraverse {
            case (address, _) =>
              val currentInfo = existingData.getOrElse(address, MetagraphSyncDataInfo.empty)
              val metagraphGlobalSnapshotsProcessed =
                globalSnapshotsProcessed.getOrElse(address, List.empty)
              val updatedUnappliedGlobalChangeOrdinals =
                pendingAfterAcknowledgements(currentInfo.unappliedGlobalChangeOrdinals, metagraphGlobalSnapshotsProcessed)

              val updatedInfo = currentInfo
                .focus(_.globalOrdinalLastAcceptedOn)
                .replace(currentOrdinal)
                .focus(_.globalEpochProgressLastAcceptedOn)
                .replace(currentEpochProgress)
                .focus(_.unappliedGlobalChangeOrdinals)
                .replace(updatedUnappliedGlobalChangeOrdinals)

              val hasChanged = currentInfo != updatedInfo
              (address, updatedInfo, hasChanged).pure[F]
          }.map { updatedEntries =>
            val updatedMap = SortedMap.from(updatedEntries.map { case (addr, info, _) => addr -> info })
            val deltasMap = SortedMap.from(updatedEntries.collect { case (addr, info, true) => addr -> info })
            (existingData ++ updatedMap, deltasMap)
          }

        private def updateFromSpendActions(
          currentData: SortedMap[Address, MetagraphSyncDataInfo],
          spendActions: Map[Address, List[SpendAction]],
          currentOrdinal: SnapshotOrdinal
        ): F[(SortedMap[Address, MetagraphSyncDataInfo], SortedMap[Address, MetagraphSyncDataInfo])] = {
          val allCurrencySpendTransactions = extractCurrencySpendTransactions(spendActions)

          val transactionsByMetagraph = allCurrencySpendTransactions.groupBy(_.currencyId.get.value)

          transactionsByMetagraph.toList.foldM((currentData, SortedMap.empty[Address, MetagraphSyncDataInfo])) {
            case ((acc, deltas), (metagraphId, _)) =>
              val currentInfo = acc.getOrElse(metagraphId, MetagraphSyncDataInfo.empty)

              val updatedUnappliedGlobalChangeOrdinals =
                appendPendingOrdinal(currentInfo.unappliedGlobalChangeOrdinals, currentOrdinal)

              val updatedInfo = currentInfo
                .focus(_.unappliedGlobalChangeOrdinals)
                .replace(updatedUnappliedGlobalChangeOrdinals)

              val hasChanged = currentInfo != updatedInfo
              val newAcc = acc.updated(metagraphId, updatedInfo)
              val newDeltas = if (hasChanged) deltas.updated(metagraphId, updatedInfo) else deltas

              (newAcc, newDeltas).pure[F]
          }
        }

        private def extractCurrencySpendTransactions(spendActions: Map[Address, List[SpendAction]]): List[SpendTransaction] =
          spendActions.values.flatten
            .flatMap(_.spendTransactions.toList)
            .filter(_.currencyId.isDefined)
            .toList

      }
    }
}
