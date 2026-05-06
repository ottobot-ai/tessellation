package io.constellationnetwork.node.shared.modules

import cats.Parallel
import cats.effect.kernel.Async
import cats.syntax.flatMap._
import cats.syntax.functor._

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.SharedConfig
import io.constellationnetwork.node.shared.domain.block.processing.BlockRejectionReason
import io.constellationnetwork.node.shared.domain.cluster.storage.{ClusterStorage, SessionStorage}
import io.constellationnetwork.node.shared.domain.collateral.LatestBalances
import io.constellationnetwork.node.shared.domain.fork.ForkInfoStorage
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage}
import io.constellationnetwork.node.shared.infrastructure.cluster.storage.{ClusterStorage, SessionStorage}
import io.constellationnetwork.node.shared.infrastructure.consensus.{CurrencySnapshotEventValidationErrorStorage, ValidationErrorStorage}
import io.constellationnetwork.node.shared.infrastructure.fork.ForkInfoStorage
import io.constellationnetwork.node.shared.infrastructure.gossip.RumorStorage
import io.constellationnetwork.node.shared.infrastructure.node.NodeStorage
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage}
import io.constellationnetwork.node.shared.snapshot.currency.CurrencySnapshotEvent
import io.constellationnetwork.schema.cluster.ClusterId
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.mpt.producer.FileSystemMerklePatriciaProducer

object SharedStorages {

  def make[F[_]: Parallel: JsonSerializer: Async: Hasher](
    clusterId: ClusterId,
    cfg: SharedConfig
  ): F[SharedStorages[F]] =
    for {
      clusterStorage <- ClusterStorage.make[F](clusterId)
      nodeStorage <- NodeStorage.make[F]
      sessionStorage <- SessionStorage.make[F]
      rumorStorage <- RumorStorage.make[F](cfg.gossip.storage)
      forkInfoStorage <- ForkInfoStorage.make[F](cfg.forkInfoStorage)
      currencySnapshotEventValidationErrorStorage <- CurrencySnapshotEventValidationErrorStorage.make(cfg.validationErrorStorage.maxSize)
      lastNGlobalSnapshotStorage <- LastNGlobalSnapshotStorage.make[F](cfg.lastGlobalSnapshotsSync)
      lastGlobalSnapshotStorage <- LastSnapshotStorage.make[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]
      mptProducer <- FileSystemMerklePatriciaProducer.make[F](cfg.mptSnapshotInfoPath)
      mptStore <- MptStore.make[F, GlobalStateKey](
        mptProducer,
        GlobalStateKey.toHex[F]
      )
      // MptOverlay (#56.6 wiring): default flag = false → passthrough impl, correctness-equivalent to direct
      // MptStore use. Lives here so production sites that need branch-aware reads / finality folding can
      // pull it from `SharedStorages` alongside the underlying store. The `ParentChildTree` is the same
      // tree used by consensus topology — passing it through means topology associations made during
      // `commit` / `finalizeBranch` stay in sync with the chain-store's view.
      mptOverlayParentChildTree <- ParentChildTree.make[F]
      mptOverlay <- MptOverlay.make[F, GlobalStateKey](
        enabled = false,
        underlying = mptStore,
        pcTree = mptOverlayParentChildTree,
        toHex = GlobalStateKey.toHex[F]
      )
    } yield
      new SharedStorages[F](
        cluster = clusterStorage,
        node = nodeStorage,
        session = sessionStorage,
        rumor = rumorStorage,
        forkInfo = forkInfoStorage,
        currencySnapshotEventValidationError = currencySnapshotEventValidationErrorStorage,
        lastNGlobalSnapshot = lastNGlobalSnapshotStorage,
        lastGlobalSnapshot = lastGlobalSnapshotStorage,
        mptStore = mptStore,
        mptOverlay = mptOverlay
      ) {}
}

sealed abstract class SharedStorages[F[_]] private (
  val cluster: ClusterStorage[F],
  val node: NodeStorage[F],
  val session: SessionStorage[F],
  val rumor: RumorStorage[F],
  val forkInfo: ForkInfoStorage[F],
  val currencySnapshotEventValidationError: ValidationErrorStorage[F, CurrencySnapshotEvent, BlockRejectionReason],
  val lastNGlobalSnapshot: LastNGlobalSnapshotStorage[F],
  val lastGlobalSnapshot: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] with LatestBalances[F],
  val mptStore: MptStore[F, GlobalStateKey],
  val mptOverlay: MptOverlay[F, GlobalStateKey]
)
