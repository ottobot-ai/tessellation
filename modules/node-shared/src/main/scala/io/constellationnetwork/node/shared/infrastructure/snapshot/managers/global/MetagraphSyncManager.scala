package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.Parallel
import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.config.types.MetagraphsSyncConfig
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{GlobalSnapshotsProcessed, SpendAction, SpendTransaction}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.sharding.CrossShardReceipt
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash

import monocle.syntax.all._
import org.typelevel.log4cats.slf4j.Slf4jLogger

case class MetagraphSyncAcceptanceResult(
  fullState: SortedMap[Address, MetagraphSyncDataInfo],
  deltas: SortedMap[Address, MetagraphSyncDataInfo]
)

trait MetagraphSyncManager[F[_]] {
  def acceptMetagraphSyncData(
    lastSnapshotContext: GlobalSnapshotInfo,
    incomingCurrencySnapshots: SortedMap[Address, List[CurrencySnapshotWithState]],
    globalSnapshotsProcessed: Map[Address, List[GlobalSnapshotsProcessed]],
    acceptedSpendActions: Map[Address, List[SpendAction]],
    currentGlobalOrdinal: SnapshotOrdinal,
    currentGlobalEpochProgress: EpochProgress
  ): F[MetagraphSyncAcceptanceResult]

  /** Apply cross-shard receipts to local `MetagraphSyncData` state — Slice 12 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md`
    * §8.4.
    *
    * Called by the gl0 leader when including a shard checkpoint; receipts come from the checkpoint's `derivedStateDelta` via the envelope's
    * `emittedReceipts` field. For each `CrossShardReceipt.MetagraphSyncDataWrite(targetMg, increment)` the cumulative pending state for
    * `targetMg` is folded with `increment`. Slice 13 (the GSAM `accept()` refactor) drains this accumulator at snapshot acceptance time and
    * persists into the canonical MPT-backed state.
    *
    * '''Idempotence.''' Applying the same receipt twice is a no-op. Tracked via an internal `seen-receipt` set keyed on the `Hasher[F]` of
    * the receipt's canonical JSON encoding; a recurring receipt (operator replay, gossip duplication) is silently dropped after the first
    * successful apply. Per `[[feedback-use-hasher-no-manual-serialize]]`: no hand-rolled Blake2b / byte-concat — the receipt's
    * derevo-derived Circe `Encoder` instance is the canonical wire-byte source, hashed via the project-wide `Hasher[F]`.
    *
    * '''Unknown target MG.''' If `targetMetagraph` has no prior entry in the pending accumulator we initialise from
    * `MetagraphSyncDataInfo.empty` and fold the increment — same shape as the existing `updateFromSpendActions` path which calls
    * `.getOrElse(metagraphId, MetagraphSyncDataInfo.empty)` before merging.
    *
    * '''Empty input.''' `consumeReceipts(Nil)` short-circuits without touching either internal Ref; this is the common-case branch when a
    * shard checkpoint emits no cross-shard writes.
    */
  def consumeReceipts(receipts: List[CrossShardReceipt])(implicit hasher: Hasher[F]): F[Unit]
}

object MetagraphSyncManager {

  /** Internal handle bundling the manager with its Refs.
    *
    * Surfaced by [[makeWithInspector]] so tests can verify `consumeReceipts` side-effects (cumulative pending state + seen-set membership)
    * without going through the not-yet-wired Slice 13 GSAM drain path. Production callers stick with [[make]], which discards the inspector
    * handle.
    */
  final case class Built[F[_]](
    manager: MetagraphSyncManager[F],
    pendingCrossShardWrites: F[SortedMap[Address, MetagraphSyncDataInfo]],
    seenReceiptHashes: F[Set[Hash]]
  )

  /** Construct a `MetagraphSyncManager`.
    *
    * Returns `F[MetagraphSyncManager[F]]` (not `MetagraphSyncManager[F]`) because the cross-shard receipt consumer needs an internal `Ref`
    * for (a) the cumulative per-MG pending sync-data accumulator and (b) the idempotence `seen-receipt` set. The Ref allocations are
    * effectful — the change ripples to a single caller (GSAM wiring at `GlobalSnapshotAcceptanceManager.scala:264`) which already runs
    * inside an `F` for-comprehension.
    */
  def make[F[_]: Async: Parallel](
    metagraphsSyncConfig: MetagraphsSyncConfig
  ): F[MetagraphSyncManager[F]] =
    makeWithInspector[F](metagraphsSyncConfig).map(_.manager)

  /** Test-leaning sibling of [[make]] that additionally returns reads of the internal Refs. Production code MUST use [[make]]; the
    * inspector handle is unstable and exists only so the unit suite can assert state-after-consume cleanly.
    */
  def makeWithInspector[F[_]: Async: Parallel](
    metagraphsSyncConfig: MetagraphsSyncConfig
  ): F[Built[F]] = {
    val logger = Slf4jLogger.getLoggerFromName[F](this.getClass.getName)

    (
      Ref.of[F, SortedMap[Address, MetagraphSyncDataInfo]](SortedMap.empty),
      Ref.of[F, Set[Hash]](Set.empty)
    ).mapN { (pendingCrossShardWrites, seenReceiptHashes) =>
      val manager: MetagraphSyncManager[F] = new MetagraphSyncManager[F] {

        def acceptMetagraphSyncData(
          lastSnapshotContext: GlobalSnapshotInfo,
          incomingCurrencySnapshots: SortedMap[Address, List[CurrencySnapshotWithState]],
          globalSnapshotsProcessed: Map[Address, List[GlobalSnapshotsProcessed]],
          acceptedSpendActions: Map[Address, List[SpendAction]],
          currentGlobalOrdinal: SnapshotOrdinal,
          currentGlobalEpochProgress: EpochProgress
        ): F[MetagraphSyncAcceptanceResult] =
          lastSnapshotContext.metagraphSyncData.map { existingData =>
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
          }.getOrElse(MetagraphSyncAcceptanceResult(SortedMap.empty, SortedMap.empty).pure[F])

        def consumeReceipts(receipts: List[CrossShardReceipt])(implicit hasher: Hasher[F]): F[Unit] =
          // Empty input is a no-op fast-path — avoids the Ref hit and the per-receipt hash compute. Required by the slice-12
          // `Empty receipt list` test contract and matches how the production caller (gl0 leader, Slice 13 GSAM rewire) will
          // pass an empty list when a shard checkpoint emits no cross-shard writes (the common case).
          if (receipts.isEmpty) Async[F].unit
          else receipts.traverse_(applyOneReceipt)

        /** Apply a single receipt under the idempotence gate.
          *
          * Order: (1) hash the receipt under the project-wide `Hasher[F]`; (2) atomic CAS the seen-set; (3) on first sight, fold the
          * increment into the pending accumulator. The seen-set check fires BEFORE the accumulator write so concurrent duplicates can't
          * both append. Sequential `consumeReceipts` calls from a single gl0 leader serialise naturally; the atomicity matters only if a
          * future caller fans receipts in parallel.
          */
        private def applyOneReceipt(receipt: CrossShardReceipt)(implicit hasher: Hasher[F]): F[Unit] =
          receipt match {
            case write: CrossShardReceipt.MetagraphSyncDataWrite =>
              Hasher[F].hash(receipt).flatMap { receiptHash =>
                seenReceiptHashes.modify { seen =>
                  if (seen.contains(receiptHash)) (seen, false)
                  else (seen + receiptHash, true)
                }.flatMap { firstSight =>
                  if (!firstSight)
                    logger.debug(
                      s"consumeReceipts: drop duplicate receipt hash=${receiptHash.value.take(16)}... " +
                        s"targetMg=${write.targetMetagraph} sourceMg=${write.sourceMetagraph}"
                    )
                  else
                    foldIncrementIntoPending(write.targetMetagraph, write.increment) *>
                      logger.debug(
                        s"consumeReceipts: applied receipt hash=${receiptHash.value.take(16)}... " +
                          s"targetMg=${write.targetMetagraph} sourceMg=${write.sourceMetagraph}"
                      )
                }
              }
          }

        /** Fold an increment into the pending accumulator for the given target metagraph.
          *
          * Merge semantics mirror the existing `updateFromSpendActions` shape (line 103-129 in the pre-Slice-12 file): take the monotone
          * max of `globalOrdinalLastAcceptedOn` / `globalEpochProgressLastAcceptedOn` (so a later-ordered receipt cannot roll the watermark
          * back) and union `unappliedGlobalChangeOrdinals` with size-trimming under
          * `metagraphsSyncConfig.maxUnappliedGlobalChangeOrdinals`. An unknown target MG starts from `MetagraphSyncDataInfo.empty` — same
          * pattern the in-tree `updateFromSpendActions` uses at line 114.
          */
        private def foldIncrementIntoPending(target: Address, increment: MetagraphSyncDataInfo): F[Unit] =
          pendingCrossShardWrites.update { pending =>
            val prior = pending.getOrElse(target, MetagraphSyncDataInfo.empty)
            val merged = mergeSyncDataInfo(prior, increment)
            pending.updated(target, merged)
          }

        /** Pure merge of two `MetagraphSyncDataInfo`s. Monotone for both scalar fields, union-with-trim for the ordinal set. */
        private def mergeSyncDataInfo(prior: MetagraphSyncDataInfo, increment: MetagraphSyncDataInfo): MetagraphSyncDataInfo = {
          val mergedOrdinals =
            trimUnappliedOrdinalsSet(prior.unappliedGlobalChangeOrdinals ++ increment.unappliedGlobalChangeOrdinals)
          val mergedOrdinal =
            if (increment.globalOrdinalLastAcceptedOn.value.value > prior.globalOrdinalLastAcceptedOn.value.value)
              increment.globalOrdinalLastAcceptedOn
            else prior.globalOrdinalLastAcceptedOn
          val mergedEpoch =
            if (increment.globalEpochProgressLastAcceptedOn.value.value > prior.globalEpochProgressLastAcceptedOn.value.value)
              increment.globalEpochProgressLastAcceptedOn
            else prior.globalEpochProgressLastAcceptedOn
          MetagraphSyncDataInfo(
            globalOrdinalLastAcceptedOn = mergedOrdinal,
            globalEpochProgressLastAcceptedOn = mergedEpoch,
            unappliedGlobalChangeOrdinals = mergedOrdinals
          )
        }

        private def trimUnappliedOrdinalsSet(ordinals: SortedSet[SnapshotOrdinal]): SortedSet[SnapshotOrdinal] = {
          val maxSize = metagraphsSyncConfig.maxUnappliedGlobalChangeOrdinals.value
          if (ordinals.size <= maxSize) ordinals
          else ordinals.dropRight(ordinals.size - maxSize)
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
                globalSnapshotsProcessed.getOrElse(address, List.empty).flatMap(_.ordinals).toSet
              val updatedUnappliedGlobalChangeOrdinals =
                currentInfo.unappliedGlobalChangeOrdinals.diff(metagraphGlobalSnapshotsProcessed)

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
                trimUnappliedOrdinals(currentInfo.unappliedGlobalChangeOrdinals, currentOrdinal)

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

        private def trimUnappliedOrdinals(
          currentOrdinals: SortedSet[SnapshotOrdinal],
          newOrdinal: SnapshotOrdinal
        ): SortedSet[SnapshotOrdinal] = {
          val maxSize = metagraphsSyncConfig.maxUnappliedGlobalChangeOrdinals.value
          val updated = currentOrdinals + newOrdinal

          if (updated.size <= maxSize) updated
          else updated.dropRight(updated.size - maxSize)
        }
      }

      Built[F](
        manager = manager,
        pendingCrossShardWrites = pendingCrossShardWrites.get,
        seenReceiptHashes = seenReceiptHashes.get
      )
    }
  }
}
