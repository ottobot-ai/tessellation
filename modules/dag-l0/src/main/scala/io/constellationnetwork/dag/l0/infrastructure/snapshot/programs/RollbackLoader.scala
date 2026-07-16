package io.constellationnetwork.dag.l0.infrastructure.snapshot.programs

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.dag.l0.config.types.GlobalSnapshotConfig
import io.constellationnetwork.dag.l0.domain.snapshot.storages.SnapshotDownloadStorage
import io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotTraverse
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.snapshot.programs.Download
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage, SnapshotStorage}
import io.constellationnetwork.node.shared.infrastructure.snapshot.GlobalSnapshotContextFunctions
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed

import org.typelevel.log4cats.slf4j.Slf4jLogger

object RollbackLoader {

  def make[F[_]: Async: Parallel: KryoSerializer: JsonSerializer: HasherSelector](
    snapshotConfig: GlobalSnapshotConfig,
    incrementalGlobalSnapshotLocalFileSystemStorage: SnapshotLocalFileSystemStorage[F, GlobalIncrementalSnapshot],
    snapshotInfoLocalFileSystemStorage: SnapshotInfoLocalFileSystemStorage[F, GlobalSnapshotStateProof, GlobalSnapshotInfo],
    snapshotStorage: SnapshotDownloadStorage[F],
    snapshotContextFunctions: GlobalSnapshotContextFunctions[F],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    globalSnapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    combinedSnapshotCheckpointFileSystemStorage: CombinedSnapshotCheckpointFileSystemStorage[
      F,
      GlobalIncrementalSnapshot,
      GlobalSnapshotInfo
    ],
    mptStore: MptStore[F, GlobalStateKey]
  ): RollbackLoader[F] =
    new RollbackLoader[F](
      snapshotConfig,
      incrementalGlobalSnapshotLocalFileSystemStorage,
      snapshotStorage: SnapshotDownloadStorage[F],
      snapshotContextFunctions,
      snapshotInfoLocalFileSystemStorage,
      getGlobalSnapshotByOrdinal,
      globalSnapshotStorage,
      lastNGlobalSnapshotStorage,
      lastGlobalSnapshotStorage,
      combinedSnapshotCheckpointFileSystemStorage,
      mptStore
    ) {}
}

sealed abstract class RollbackLoader[F[_]: Async: Parallel: KryoSerializer: JsonSerializer: HasherSelector] private (
  snapshotConfig: GlobalSnapshotConfig,
  incrementalGlobalSnapshotLocalFileSystemStorage: SnapshotLocalFileSystemStorage[F, GlobalIncrementalSnapshot],
  snapshotStorage: SnapshotDownloadStorage[F],
  snapshotContextFunctions: GlobalSnapshotContextFunctions[F],
  snapshotInfoLocalFileSystemStorage: SnapshotInfoLocalFileSystemStorage[F, GlobalSnapshotStateProof, GlobalSnapshotInfo],
  getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
  globalSnapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
  lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
  lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
  combinedSnapshotCheckpointFileSystemStorage: CombinedSnapshotCheckpointFileSystemStorage[
    F,
    GlobalIncrementalSnapshot,
    GlobalSnapshotInfo
  ],
  mptStore: MptStore[F, GlobalStateKey]
) {

  private val logger = Slf4jLogger.getLogger[F]

  def load(
    rollbackHash: Hash,
    download: Download[F, GlobalIncrementalSnapshot]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): F[(GlobalSnapshotInfo, Signed[GlobalIncrementalSnapshot])] =
    GlobalSnapshotLocalFileSystemStorage.make[F](snapshotConfig.snapshotPath).flatMap { fullGlobalSnapshotLocalFileSystemStorage =>
      fullGlobalSnapshotLocalFileSystemStorage
        .read(rollbackHash)
        .flatMap {
          case None =>
            logger.info("Attempt to treat rollback hash as pointer to incremental global snapshot") >> {
              val snapshotTraverse = GlobalSnapshotTraverse
                .make[F](
                  incrementalGlobalSnapshotLocalFileSystemStorage.read(_),
                  fullGlobalSnapshotLocalFileSystemStorage.read(_),
                  snapshotInfoLocalFileSystemStorage.read(_),
                  snapshotContextFunctions,
                  rollbackHash,
                  getGlobalSnapshotByOrdinal,
                  globalSnapshotStorage,
                  lastNGlobalSnapshotStorage,
                  lastGlobalSnapshotStorage,
                  download,
                  mptStore
                )
              snapshotTraverse.loadChain()
            }
          case Some(_) =>
            Async[F].raiseError[(GlobalSnapshotInfo, Signed[GlobalIncrementalSnapshot])](
              new IllegalArgumentException(
                "Rollback to a full global snapshot is unsupported: the persisted V1 full snapshot does not carry the rooted " +
                  "operator-key, delegated-stake, and collateral augmentation required to reproduce the canonical first incremental. " +
                  "Use an incremental snapshot hash. A future full-snapshot rollback must bind and verify the exact genesis manifest " +
                  "before deriving or installing state. No rollback cleanup has been performed."
              )
            )
        }
        .flatTap {
          case (_, lastInc) =>
            logger.info(s"[Rollback] Cleanup for snapshots greater than ${lastInc.ordinal}") >>
              snapshotStorage.cleanupAbove(lastInc.ordinal) >>
              combinedSnapshotCheckpointFileSystemStorage.deleteAbove(lastInc.ordinal)
        }
    }
}
