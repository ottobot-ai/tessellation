package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.effect.{Async, Ref}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash

/** Persistent ordered store of finalized super-level hits per `docs/nakamoto/NIPOPOW-PROPOSAL.md` §2.3 + S3 plan slice.
  *
  * '''Append-only at finalization.''' Entries are added by a Phase-3 sink (`T_depth2.advance`); the tower never moves backwards outside an
  * explicit prune call. Concurrent appends from the same node are serialized externally (single producer at the finality sink).
  *
  * '''Per-level read access.''' `entriesAtLevel(µ, since)` returns the ordered subsequence of level-µ hits at or after `since`, used by the
  * proof builder to construct skeletons.
  *
  * '''Latest-at-level''' returns the most recent level-µ entry, used by the producer to compute `g_µ` (base-block gap) for the next
  * snapshot's trial.
  *
  * '''Pruning.''' `pruneBelow(keepFrom)` drops entries strictly below `keepFrom` at every level. Memory-pressure-driven; safe to call from
  * any thread (idempotent). Must NOT prune below the oldest ordinal needed by an in-flight proof — caller's responsibility.
  */
trait TowerStore[F[_]] {

  def appendAtFinality(
    ordinal: SnapshotOrdinal,
    snapshotHash: Hash,
    passes: Vector[LevelTrial]
  ): F[Unit]

  def entriesAtLevel(level: Int, since: SnapshotOrdinal): F[List[TowerEntry]]

  def latestAt(level: Int): F[Option[TowerEntry]]

  def cumulativeCount(level: Int): F[Long]

  def pruneBelow(keepFrom: SnapshotOrdinal): F[Unit]
}

object TowerStore {

  /** Internal state: `level → (ordinal → snapshotHash)`. SortedMap on the inner so per-level iteration is in ordinal order. */
  private type State = SortedMap[Int, SortedMap[SnapshotOrdinal, Hash]]

  private val EmptyState: State = SortedMap.empty[Int, SortedMap[SnapshotOrdinal, Hash]]

  /** In-memory `Ref`-backed implementation. Deterministic, test-friendly. The production `MptTowerStore` (deferred) will share this
    * interface, persisting to a new MPT partition.
    */
  def inMemory[F[_]: Async]: F[TowerStore[F]] =
    Ref.of[F, State](EmptyState).map { ref =>
      new TowerStore[F] {

        def appendAtFinality(
          ordinal: SnapshotOrdinal,
          snapshotHash: Hash,
          passes: Vector[LevelTrial]
        ): F[Unit] =
          ref.update { state =>
            passes.foldLeft(state) {
              case (acc, trial) if trial.passed =>
                val inner = acc.getOrElse(trial.level, SortedMap.empty[SnapshotOrdinal, Hash])
                acc.updated(trial.level, inner.updated(ordinal, snapshotHash))
              case (acc, _) => acc
            }
          }

        def entriesAtLevel(level: Int, since: SnapshotOrdinal): F[List[TowerEntry]] =
          ref.get.map { state =>
            state
              .getOrElse(level, SortedMap.empty[SnapshotOrdinal, Hash])
              .iteratorFrom(since)
              .map { case (ord, hash) => TowerEntry(level, ord, hash) }
              .toList
          }

        def latestAt(level: Int): F[Option[TowerEntry]] =
          ref.get.map { state =>
            state.get(level).flatMap(_.lastOption).map { case (ord, hash) => TowerEntry(level, ord, hash) }
          }

        def cumulativeCount(level: Int): F[Long] =
          ref.get.map(_.getOrElse(level, SortedMap.empty[SnapshotOrdinal, Hash]).size.toLong)

        def pruneBelow(keepFrom: SnapshotOrdinal): F[Unit] =
          ref.update(_.view.mapValues(_.dropWhile { case (ord, _) => ord < keepFrom }).to(SortedMap))
      }
    }
}
