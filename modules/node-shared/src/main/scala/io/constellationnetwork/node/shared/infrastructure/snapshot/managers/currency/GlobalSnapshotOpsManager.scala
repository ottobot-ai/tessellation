package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
import io.constellationnetwork.node.shared.config.types.LastGlobalSnapshotsSyncConfig
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{GlobalSnapshotsProcessed, SharedArtifact, SpendAction}
import io.constellationnetwork.security.Hashed

import fs2.concurrent.SignallingRef

class GlobalSnapshotOpsManager[F[_]: Async: Parallel](
  lastGlobalSnapshotsSyncConfig: LastGlobalSnapshotsSyncConfig,
  lastGlobalSnapshotsCached: SignallingRef[F, Map[SnapshotOrdinal, Hashed[GlobalIncrementalSnapshot]]]
) {

  /** Resolve one required historical snapshot through the caller-owned resolver exactly once.
    *
    * The method retains its existing name until the candidate-scoped replay-view API replaces the ordinal-only callback. It performs no
    * retry and never consults `lastGlobalSnapshotsCached`: manager-owned retry timing and an ordinal-keyed local cache cannot choose replay
    * inputs. The supplied callback remains an ordinal-only authority boundary until the next replay-view slice.
    */
  def getGlobalSnapshotWithRetry(
    ordinal: SnapshotOrdinal,
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  ): F[Hashed[GlobalIncrementalSnapshot]] =
    Async[F].defer(getGlobalSnapshotByOrdinal(ordinal)).flatMap {
      case Some(snapshot) => snapshot.pure[F]
      case None =>
        new RuntimeException(s"Required global snapshot not found for ordinal $ordinal")
          .raiseError[F, Hashed[GlobalIncrementalSnapshot]]
    }

  /** Select which cross-shard global-snapshot ordinals to apply for this metagraph and return their combined `SpendAction`s.
    *
    * The applied set is `A = { o ∈ U : o ≤ view ∧ o ∉ P }` where:
    *   - `U` = `syncDataInfo.unappliedGlobalChangeOrdinals` (the gl0-maintained set of global ordinals carrying not-yet-acked cross-shard
    *     changes for this metagraph),
    *   - `view` = `globalSnapshotViewOrdinal` (the pinned/recorded global sync view), and
    *   - `P` = `alreadyProcessedGlobalOrdinals`, the set of global ordinals this metagraph has ALREADY emitted in prior currency snapshots'
    *     `GlobalSnapshotsProcessed` artifacts (blocker-1a: reconstructed IN-BAND from the retained CL0 chain by the caller — see
    *     [[GlobalSnapshotOpsManager.reconstructProcessedGlobalOrdinals]] — replacing the former node-local
    *     `globalSnapshotsAlreadyProcessed` mutable cache).
    *
    * `P` is required because gl0 trims `U` (`MetagraphSyncManager.updateFromCurrencySnapshots`:
    * `U.diff(GlobalSnapshotsProcessed.ordinals)`) only once it has ingested the CL0 snapshot that emitted `GlobalSnapshotsProcessed(A)`.
    * Between emission and that trim propagating back into the producer's `U`, several currency snapshots can be produced against the same
    * stale `U`; without `P` they would re-apply the same `SpendAction`s (double-deduction / snapshot-diff mismatch).
    *
    * DETERMINISM INVARIANT (`P`-window ⊇ `U`-window): `A` depends only on `P ∩ U`, so as long as the reconstructed `P` covers every ordinal
    * in `U ∩ (≤ view)` that was already processed, `A` is identical across nodes regardless of how much extra CL0 history any node retains.
    * If the CL0 snapshot that emitted a still-in-`U` processed-ordinal has been evicted from `P`'s reconstruction window, that ordinal
    * re-applies — see the retention analysis in the blocker-1a report.
    *
    * The second element of the result (`= A`) is what the caller wraps in a `GlobalSnapshotsProcessed` artifact for the snapshot being
    * built.
    */
  def getLastGlobalSnapshotsSpendActions(
    globalSnapshotViewOrdinal: SnapshotOrdinal,
    lastGlobalSnapshots: List[Hashed[GlobalIncrementalSnapshot]],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    currencyId: Address,
    metagraphSyncData: Option[SortedMap[Address, snapshot.MetagraphSyncDataInfo]],
    alreadyProcessedGlobalOrdinals: SortedSet[SnapshotOrdinal]
  ): F[(SortedMap[Address, List[SpendAction]], SortedSet[SnapshotOrdinal])] = {
    val emptySpendActions = SortedMap.empty[Address, List[SpendAction]]
    val emptyProcessedGlobalSnapshots = SortedSet.empty[SnapshotOrdinal]

    metagraphSyncData match {
      case None => (emptySpendActions, emptyProcessedGlobalSnapshots).pure[F]
      case Some(metagraphSyncData) =>
        metagraphSyncData.get(currencyId) match {
          case None               => (emptySpendActions, emptyProcessedGlobalSnapshots).pure[F]
          case Some(syncDataInfo) =>
            // A = { o ∈ U : o ≤ view ∧ o ∉ P }. P (`alreadyProcessedGlobalOrdinals`) is reconstructed in-band from the retained CL0
            // chain by the caller; this replaces the former node-local `globalSnapshotsAlreadyProcessed` cache read.
            val unappliedGlobalOrdinalsToProcess: SortedSet[SnapshotOrdinal] =
              syncDataInfo.unappliedGlobalChangeOrdinals
                .filter(o => o <= globalSnapshotViewOrdinal && !alreadyProcessedGlobalOrdinals.contains(o))

            if (unappliedGlobalOrdinalsToProcess.isEmpty)
              (emptySpendActions, emptyProcessedGlobalSnapshots).pure[F]
            else {
              // Compatibility input only. An ordinal-keyed LastN entry is not candidate-lineage evidence and must never outrank the
              // caller-owned resolver. The next replay-view slice will remove this parameter when it replaces the ordinal-only callback.
              val _ = lastGlobalSnapshots

              processUnappliedOrdinals(
                unappliedGlobalOrdinalsToProcess,
                getGlobalSnapshotByOrdinal
              ).flatMap(spendActions => (spendActions, unappliedGlobalOrdinalsToProcess).pure[F])
            }
        }
    }
  }

  private def processUnappliedOrdinals(
    unappliedOrdinals: SortedSet[SnapshotOrdinal],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  ): F[SortedMap[Address, List[SpendAction]]] =
    // `unappliedOrdinals` is a SortedSet, so each required ordinal is attempted once. Capturing each outcome prevents one miss/error from
    // cancelling the remaining bounded lookups; only after all attempts finish do we fail closed at the first ordinal in deterministic order.
    unappliedOrdinals.toList.parTraverse { ordinal =>
      Async[F].defer(getGlobalSnapshotByOrdinal(ordinal)).attempt.map(ordinal -> _)
    }
      .flatMap(
        _.traverse {
          case (ordinal, Right(Some(snapshot))) =>
            (ordinal -> snapshot.spendActions.getOrElse(SortedMap.empty[Address, List[SpendAction]])).pure[F]
          case (ordinal, Right(None)) =>
            new RuntimeException(s"Required global snapshot not found for ordinal $ordinal")
              .raiseError[F, (SnapshotOrdinal, SortedMap[Address, List[SpendAction]])]
          case (ordinal, Left(error)) =>
            new RuntimeException(s"Failed to resolve required global snapshot at ordinal $ordinal", error)
              .raiseError[F, (SnapshotOrdinal, SortedMap[Address, List[SpendAction]])]
        }
      )
      .map(combineSpendActions)

  /** Every ordinal returned in `GlobalSnapshotsProcessed` must have every one of its actions replayed. Merge in ordinal order and
    * concatenate producer collisions explicitly; right-biased `Map.++` would acknowledge an earlier ordinal while silently dropping its
    * actions.
    */
  private[currency] def combineSpendActions(
    spendActionsByOrdinal: List[(SnapshotOrdinal, SortedMap[Address, List[SpendAction]])]
  ): SortedMap[Address, List[SpendAction]] =
    spendActionsByOrdinal.sortBy(_._1).foldLeft(SortedMap.empty[Address, List[SpendAction]]) {
      case (combined, (_, actionsByProducer)) =>
        actionsByProducer.foldLeft(combined) {
          case (acc, (producer, actions)) =>
            acc.updated(producer, acc.getOrElse(producer, List.empty).appendedAll(actions))
        }
    }

  def updateGlobalSnapshotCache(
    snapshot: Hashed[GlobalIncrementalSnapshot]
  ): F[Unit] =
    for {
      _ <- lastGlobalSnapshotsCached.update { current =>
        val updated = current.updated(snapshot.ordinal, snapshot)
        updated.toSeq
          .sortBy(_._1.value.value)
          .takeRight(lastGlobalSnapshotsSyncConfig.maxLastGlobalSnapshotsInMemory.value)
          .toMap
      }
    } yield ()
}

object GlobalSnapshotOpsManager {
  def make[F[_]: Async: Parallel](
    lastGlobalSnapshotsSyncConfig: LastGlobalSnapshotsSyncConfig,
    lastGlobalSnapshotsCached: SignallingRef[F, Map[SnapshotOrdinal, Hashed[GlobalIncrementalSnapshot]]]
  ): GlobalSnapshotOpsManager[F] =
    new GlobalSnapshotOpsManager[F](
      lastGlobalSnapshotsSyncConfig,
      lastGlobalSnapshotsCached
    )

  /** In-band reconstruction of `P` (blocker-1a): the union of every `GlobalSnapshotsProcessed.ordinals` carried in the `artifacts` of the
    * given retained CL0 currency snapshots. This is a PURE fold over consensus-pinned chain data — it replaces the former node-local
    * `globalSnapshotsAlreadyProcessed` mutable mirror. Callers supply a bounded window of the metagraph's own recent currency snapshots
    * (walked back from the pinned prior); the window must cover the live `unappliedGlobalChangeOrdinals` (`U`) span (`P`-window ⊇
    * `U`-window), otherwise an already-processed ordinal that is still in `U` re-applies.
    */
  def reconstructProcessedGlobalOrdinals(snapshots: Iterable[CurrencyIncrementalSnapshot]): SortedSet[SnapshotOrdinal] =
    snapshots.foldLeft(SortedSet.empty[SnapshotOrdinal]) { (acc, snap) =>
      acc ++ snap.artifacts
        .getOrElse(SortedSet.empty[SharedArtifact])
        .toList
        .collect { case gsp: GlobalSnapshotsProcessed => gsp.ordinals }
        .flatten
    }
}
