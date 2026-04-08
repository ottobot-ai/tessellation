package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Per-ordinal undo record for reversing state changes on the flat stateRef. */
case class UndoEntry(
  ordinal: Long,
  snapshotHash: Hash,
  parentHash: Hash,
  inserted: Set[Hex], // keys new at this ordinal
  removed: Map[Hex, Array[Byte]], // keys deleted + their old bytes
  overwritten: Map[Hex, Array[Byte]] // keys modified + their old bytes
)

/** Fork-aware undo journal for MPT state.
  *
  * Records what changed at each ordinal on the flat Map[Hex, Array[Byte]] inside InMemoryMerklePatriciaProducer. On fork switch, walks
  * journal backwards to common ancestor, undoing mutations, then lets the new fork's pipeline apply forward normally.
  *
  * Cost: O(delta_size) per ordinal for recording. O(delta_size × fork_depth) for a fork switch. Memory: one UndoEntry per unfinalized
  * ordinal.
  */
trait MptUndoJournal[F[_]] {

  /** Wrap a state-mutating action (typically syncFromStateChanges) with journal recording. Snapshots producer.entries before, runs the
    * action, then diffs to build the undo entry.
    */
  def wrapApply(
    ordinal: Long,
    snapshotHash: Hash,
    parentHash: Hash
  )(apply: F[Unit]): F[Unit]

  /** Unapply journal entries from current tip back to (not including) the target ancestor ordinal. Returns count of entries unapplied.
    *
    * NOT YET CONSUMED. Recording (via `wrapApply`) is wired, but no caller invokes `unapplyTo` today. Reorgs are handled by the
    * self-healing MPT path (full rebuild from canonical chain) — slower but correct. The journal is a performance optimization deferred
    * until: (a) self-healing rebuilds become a bottleneck during testing, or (b) inclusion-proof features land that require reconstructing
    * the trie root at a historical ordinal — which is a functional requirement, not just performance.
    *
    * To wire this, the snapshot acceptance pipeline needs to detect reorgs BEFORE applying the incoming fork's MPT mutations, compute the
    * common ancestor ordinal between the canonical chain and the incoming fork, call `unapplyTo` to roll the MPT back to that ancestor,
    * then let the new fork apply forward via `wrapApply`. See task #4 in NAKAMOTO-PLAN.md.
    */
  def unapplyTo(ancestorOrdinal: Long): F[Int]

  /** Prune entries at or below ordinal (call after finalization). */
  def pruneBelow(ordinal: Long): F[Unit]

  /** Current journal tip ordinal. */
  def currentTipOrdinal: F[Option[Long]]

  /** Snapshot hash at a given ordinal. */
  def hashAtOrdinal(ordinal: Long): F[Option[Hash]]

  /** Parent hash at a given ordinal. */
  def parentHashAtOrdinal(ordinal: Long): F[Option[Hash]]

  /** Number of journal entries (for diagnostics). */
  def size: F[Int]
}

object MptUndoJournal {

  private case class JournalState(
    entries: Map[Long, UndoEntry],
    tipOrdinal: Option[Long]
  )

  def make[F[_]: Async](
    mptStore: MptStore[F, GlobalStateKey]
  ): F[MptUndoJournal[F]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("MptUndoJournal")

    Ref.of[F, JournalState](JournalState(Map.empty, None)).map { stateRef =>
      new MptUndoJournal[F] {

        private val producer = mptStore.underlying

        override def wrapApply(
          ordinal: Long,
          snapshotHash: Hash,
          parentHash: Hash
        )(apply: F[Unit]): F[Unit] =
          for {
            // 1. Snapshot the entire stateRef before mutation
            before <- producer.entries

            // 2. Run the actual sync
            _ <- apply

            // 3. Snapshot after and diff
            after <- producer.entries

            // Classify changes:
            // - inserted: in after but not in before
            // - removed:  in before but not in after
            // - overwritten: in both but bytes differ
            inserted = after.keySet -- before.keySet
            removedKeys = before.keySet -- after.keySet
            removed = removedKeys.map(k => k -> before(k)).toMap
            overwritten = before.keySet
              .intersect(after.keySet)
              .flatMap { k =>
                val oldBytes = before(k)
                val newBytes = after(k)
                if (!java.util.Arrays.equals(oldBytes, newBytes)) Some(k -> oldBytes)
                else None
              }
              .toMap

            entry = UndoEntry(ordinal, snapshotHash, parentHash, inserted, removed, overwritten)

            _ <- stateRef.update(s =>
              s.copy(
                entries = s.entries + (ordinal -> entry),
                tipOrdinal = Some(ordinal)
              )
            )

            _ <- logger.info(
              s"[UndoJournal] ordinal=$ordinal: " +
                s"${inserted.size} inserted, ${removed.size} removed, ${overwritten.size} overwritten " +
                s"(journal=${stateRef.toString})"
            )
          } yield ()

        override def unapplyTo(ancestorOrdinal: Long): F[Int] =
          for {
            journalState <- stateRef.get

            ordinalsToUnapply = journalState.entries.keys.toList
              .sorted(Ordering[Long].reverse)
              .takeWhile(_ > ancestorOrdinal)

            _ <- logger.info(
              s"[UndoJournal] Rolling back ${ordinalsToUnapply.size} ordinals " +
                s"(${ordinalsToUnapply.headOption.getOrElse("?")} → " +
                s"${ordinalsToUnapply.lastOption.getOrElse("?")}), target=$ancestorOrdinal"
            )

            _ <- ordinalsToUnapply.traverse_ { ord =>
              journalState.entries.get(ord) match {
                case Some(entry) => unapplyEntry(entry)
                case None =>
                  logger.warn(s"[UndoJournal] Missing entry for ordinal=$ord")
              }
            }

            _ <- stateRef.update { s =>
              val remaining = s.entries -- ordinalsToUnapply.toSet
              s.copy(
                entries = remaining,
                tipOrdinal = if (remaining.isEmpty) None else Some(ancestorOrdinal)
              )
            }

            // Rebuild trie from corrected stateRef
            _ <- mptStore.build(SnapshotOrdinal.unsafeApply(ancestorOrdinal)).void

            _ <- logger.info(
              s"[UndoJournal] Rolled back ${ordinalsToUnapply.size} ordinals, " +
                s"MPT now at ancestor=$ancestorOrdinal"
            )
          } yield ordinalsToUnapply.size

        override def pruneBelow(ordinal: Long): F[Unit] =
          stateRef.update(s => s.copy(entries = s.entries.filter(_._1 > ordinal))) >>
            logger.debug(s"[UndoJournal] Pruned entries ≤ ordinal=$ordinal")

        override def currentTipOrdinal: F[Option[Long]] =
          stateRef.get.map(_.tipOrdinal)

        override def hashAtOrdinal(ordinal: Long): F[Option[Hash]] =
          stateRef.get.map(_.entries.get(ordinal).map(_.snapshotHash))

        override def parentHashAtOrdinal(ordinal: Long): F[Option[Hash]] =
          stateRef.get.map(_.entries.get(ordinal).map(_.parentHash))

        override def size: F[Int] =
          stateRef.get.map(_.entries.size)

        private def unapplyEntry(entry: UndoEntry): F[Unit] =
          for {
            // 1. Remove newly-inserted keys
            _ <- producer.remove(entry.inserted.toList).void.whenA(entry.inserted.nonEmpty)
            // 2. Restore overwritten keys
            _ <- producer.insertBytes(entry.overwritten).void.whenA(entry.overwritten.nonEmpty)
            // 3. Re-insert removed keys
            _ <- producer.insertBytes(entry.removed).void.whenA(entry.removed.nonEmpty)

            _ <- logger.debug(
              s"[UndoJournal] Unapplied ordinal=${entry.ordinal}: " +
                s"-${entry.inserted.size} new, ~${entry.overwritten.size} restored, +${entry.removed.size} re-inserted"
            )
          } yield ()
      }
    }
  }
}
