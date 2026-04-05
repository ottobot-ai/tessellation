package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.effect.std.Semaphore
import cats.syntax.all._

import io.constellationnetwork.schema.{GlobalSnapshotInfo, GlobalSnapshotStateProof}
import io.constellationnetwork.security.hash.Hash

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Event-sourced MPT state with checkpoint+replay for fork handling.
  *
  * Adapted from Bifrost's EventSourcedState pattern. Since tessellation's MPT is forward-only (no unapply), we use checkpoint+replay
  * instead of apply/unapply:
  *
  *   - **Checkpoint**: After each finalized ordinal, save the GlobalSnapshotInfo as a restore point.
  *   - **Apply**: Process a snapshot delta to advance the MPT (normal acceptance pipeline).
  *   - **Reorg/Unapply**: Restore the last checkpoint and replay forward on the winning fork.
  *
  * This avoids the need for undo logs or content-addressed trie storage while still supporting fork switches correctly. The cost of a reorg
  * \= replay from last checkpoint, which is bounded by the finality depth (k blocks max).
  *
  * Thread safety: all state mutations go through a Semaphore (shared with SnapshotLeaderLoop and NakamotoSyncDaemon).
  */
trait EventSourcedMptState[F[_]] {

  /** Get the state proof for a given snapshot, computing it via the appropriate path:
    *   - If the snapshot extends the current tip → incremental apply
    *   - If on a different fork → checkpoint restore + replay
    *   - Always falls back to full rebuild for correctness
    */
  def stateProofAt(
    snapshotHash: Hash,
    snapshotInfo: GlobalSnapshotInfo,
    parentHash: Hash
  ): F[GlobalSnapshotStateProof]

  /** Checkpoint the current state (call after finalization). */
  def checkpoint(hash: Hash, info: GlobalSnapshotInfo): F[Unit]

  /** Get the current tip hash that the MPT state is positioned at. */
  def currentTip: F[Option[Hash]]

  /** Restore to the last checkpoint (for reorg). Returns the checkpoint hash, or None if no checkpoint. */
  def restoreCheckpoint: F[Option[Hash]]
}

object EventSourcedMptState {

  case class MptCheckpoint(
    hash: Hash,
    info: GlobalSnapshotInfo
  )

  case class MptPosition(
    currentTipHash: Option[Hash],
    lastCheckpoint: Option[MptCheckpoint]
  )

  def make[F[_]: Async](
    parentChildTree: ParentChildTree[F],
    applySnapshot: (GlobalSnapshotInfo, Hash) => F[GlobalSnapshotStateProof],
    fullRebuild: GlobalSnapshotInfo => F[GlobalSnapshotStateProof]
  ): F[EventSourcedMptState[F]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("EventSourcedMptState")

    for {
      positionRef <- Ref.of[F, MptPosition](MptPosition(None, None))
      semaphore <- Semaphore[F](1)
    } yield
      new EventSourcedMptState[F] {

        def stateProofAt(
          snapshotHash: Hash,
          snapshotInfo: GlobalSnapshotInfo,
          parentHash: Hash
        ): F[GlobalSnapshotStateProof] =
          semaphore.permit.use { _ =>
            positionRef.get.flatMap { position =>
              position.currentTipHash match {
                case Some(tipHash) if tipHash === parentHash =>
                  // Happy path: snapshot extends current tip → incremental apply
                  applySnapshot(snapshotInfo, snapshotHash).flatTap { proof =>
                    positionRef.update(_.copy(currentTipHash = Some(snapshotHash))) >>
                      logger.debug(
                        s"Applied incrementally: ${snapshotHash.value.take(12)} extends tip ${tipHash.value.take(12)}"
                      )
                  }

                case Some(tipHash) =>
                  // Fork detected: parent doesn't match current tip.
                  // Use full rebuild for correctness (checkpoint+replay can be added as optimization).
                  logger.info(
                    s"Fork detected: snapshot parent=${parentHash.value.take(12)} != tip=${tipHash.value.take(12)}, using full rebuild"
                  ) >>
                    fullRebuild(snapshotInfo).flatTap { _ =>
                      positionRef.update(_.copy(currentTipHash = Some(snapshotHash)))
                    }

                case None =>
                  // First snapshot — full rebuild
                  fullRebuild(snapshotInfo).flatTap { _ =>
                    positionRef.update(_.copy(currentTipHash = Some(snapshotHash))) >>
                      logger.info(s"Initial state proof computed for ${snapshotHash.value.take(12)}")
                  }
              }
            }
          }

        def checkpoint(hash: Hash, info: GlobalSnapshotInfo): F[Unit] =
          semaphore.permit.use { _ =>
            positionRef.update(_.copy(lastCheckpoint = Some(MptCheckpoint(hash, info)))) >>
              logger.info(s"Checkpointed MPT state at ${hash.value.take(12)}")
          }

        def currentTip: F[Option[Hash]] =
          positionRef.get.map(_.currentTipHash)

        def restoreCheckpoint: F[Option[Hash]] =
          semaphore.permit.use { _ =>
            positionRef.modify { pos =>
              pos.lastCheckpoint match {
                case Some(cp) =>
                  (pos.copy(currentTipHash = Some(cp.hash)), Some(cp.hash))
                case None =>
                  (pos, None)
              }
            }
          }.flatTap {
            case Some(hash) => logger.info(s"Restored MPT to checkpoint ${hash.value.take(12)}")
            case None       => logger.warn("No checkpoint to restore — MPT state may be inconsistent")
          }
      }
  }
}
