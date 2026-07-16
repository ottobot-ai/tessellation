package io.constellationnetwork.dag.l0.modules

import cats.Parallel
import cats.effect.Async
import cats.effect.std.Supervisor
import cats.syntax.all._

import io.constellationnetwork.dag.l0.config.types.{GlobalSnapshotConfig, IncrementalConfig}
import io.constellationnetwork.dag.l0.domain.snapshot.storages.SnapshotDownloadStorage
import io.constellationnetwork.dag.l0.infrastructure.snapshot.SnapshotDownloadStorage
import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.config.types.SharedConfig
import io.constellationnetwork.node.shared.domain.cluster.storage.{ClusterStorage, SessionStorage}
import io.constellationnetwork.node.shared.domain.collateral.LatestBalances
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.gossip.RumorStorage
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage._
import io.constellationnetwork.node.shared.modules.SharedStorages
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security.{HashSelect, HasherSelector}

import fs2.io.file.Files

object Storages {

  def make[F[+_]: Async: Parallel: KryoSerializer: JsonSerializer: HasherSelector: Supervisor: Files](
    sharedStorages: SharedStorages[F],
    sharedConfig: SharedConfig,
    seedlist: Option[Set[SeedlistEntry]],
    snapshotConfig: GlobalSnapshotConfig,
    incrementalConfig: IncrementalConfig,
    environment: AppEnvironment,
    hashSelect: HashSelect,
    // Optional finalized-ordinal ref. When Some, SnapshotStorage.setHeadForRecovery
    // refuses different-hash overwrites at-or-below finalized (raw-layer safety net
    // complementing NakamotoChainStore.store's guard). Nakamoto GL0 passes Some(ref);
    // other consumers leave None so BFT semantics are unchanged.
    nakamotoFinalizedOrdinalRef: Option[cats.effect.kernel.Ref[F, SnapshotOrdinal]] = None
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): F[Storages[F]] =
    for {
      incrementalGlobalSnapshotTmpLocalFileSystemStorage <- GlobalIncrementalSnapshotLocalFileSystemStorage.make[F](
        snapshotConfig.incrementalTmpSnapshotPath
      )
      incrementalGlobalSnapshotPersistedLocalFileSystemStorage <- GlobalIncrementalSnapshotLocalFileSystemStorage.make[F](
        snapshotConfig.incrementalPersistedSnapshotPath
      )
      fullGlobalSnapshotLocalFileSystemStorage <- GlobalSnapshotLocalFileSystemStorage.make[F](
        snapshotConfig.snapshotPath
      )
      incrementalGlobalSnapshotInfoLocalFileSystemStorage <- GlobalSnapshotInfoLocalFileSystemStorage.make[F](
        snapshotConfig.snapshotInfoPath
      )
      incrementalKryoGlobalSnapshotInfoLocalFileSystemStorage <- GlobalSnapshotInfoKryoLocalFileSystemStorage.make[F](
        snapshotConfig.snapshotInfoPath
      )

      combinedGlobalSnapshotCheckpointStorage <- CombinedSnapshotCheckpointFileSystemStorage
        .make[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo](
          snapshotConfig.combinedSnapshotCheckpointPath
        )
      globalSnapshotStorage <- SnapshotStorage.make[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo](
        incrementalGlobalSnapshotPersistedLocalFileSystemStorage,
        incrementalGlobalSnapshotInfoLocalFileSystemStorage,
        snapshotConfig.inMemoryCapacity,
        incrementalConfig.lastFullGlobalSnapshotOrdinal.getOrElse(environment, SnapshotOrdinal.MinValue),
        HasherSelector[F],
        combinedGlobalSnapshotCheckpointStorage,
        nakamotoFinalizedOrdinalRef
      )
      snapshotDownloadStorage = SnapshotDownloadStorage
        .make[F](
          incrementalGlobalSnapshotTmpLocalFileSystemStorage,
          incrementalGlobalSnapshotPersistedLocalFileSystemStorage,
          fullGlobalSnapshotLocalFileSystemStorage,
          incrementalGlobalSnapshotInfoLocalFileSystemStorage,
          incrementalKryoGlobalSnapshotInfoLocalFileSystemStorage,
          combinedGlobalSnapshotCheckpointStorage,
          hashSelect,
          sharedStorages.mptStore
        )
    } yield
      new Storages[F](
        cluster = sharedStorages.cluster,
        node = sharedStorages.node,
        session = sharedStorages.session,
        rumor = sharedStorages.rumor,
        globalSnapshot = globalSnapshotStorage,
        fullGlobalSnapshot = fullGlobalSnapshotLocalFileSystemStorage,
        incrementalGlobalSnapshotLocalFileSystemStorage = incrementalGlobalSnapshotPersistedLocalFileSystemStorage,
        snapshotDownload = snapshotDownloadStorage,
        globalSnapshotInfoLocalFileSystemStorage = incrementalGlobalSnapshotInfoLocalFileSystemStorage,
        globalSnapshotInfoLocalFileSystemKryoStorage = incrementalKryoGlobalSnapshotInfoLocalFileSystemStorage,
        combinedGlobalSnapshotCheckpointStorage = combinedGlobalSnapshotCheckpointStorage,
        mptStore = sharedStorages.mptStore
      ) {}
}

sealed abstract class Storages[F[_]] private (
  val cluster: ClusterStorage[F],
  val node: NodeStorage[F],
  val session: SessionStorage[F],
  val rumor: RumorStorage[F],
  val globalSnapshot: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] with LatestBalances[F],
  val fullGlobalSnapshot: SnapshotLocalFileSystemStorage[F, GlobalSnapshot],
  val incrementalGlobalSnapshotLocalFileSystemStorage: SnapshotLocalFileSystemStorage[F, GlobalIncrementalSnapshot],
  val snapshotDownload: SnapshotDownloadStorage[F],
  val globalSnapshotInfoLocalFileSystemStorage: SnapshotInfoLocalFileSystemStorage[F, GlobalSnapshotStateProof, GlobalSnapshotInfo],
  val globalSnapshotInfoLocalFileSystemKryoStorage: SnapshotInfoLocalFileSystemStorage[F, GlobalSnapshotStateProof, GlobalSnapshotInfoV2],
  val combinedGlobalSnapshotCheckpointStorage: CombinedSnapshotCheckpointFileSystemStorage[
    F,
    GlobalIncrementalSnapshot,
    GlobalSnapshotInfo
  ],
  val mptStore: MptStore[F, GlobalStateKey]
)
