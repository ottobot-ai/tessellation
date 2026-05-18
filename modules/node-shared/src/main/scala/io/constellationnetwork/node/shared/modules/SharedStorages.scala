package io.constellationnetwork.node.shared.modules

import cats.Parallel
import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.SharedConfig
import io.constellationnetwork.node.shared.domain.block.processing.BlockRejectionReason
import io.constellationnetwork.node.shared.domain.cluster.storage.{ClusterStorage, SessionStorage}
import io.constellationnetwork.node.shared.domain.collateral.LatestBalances
import io.constellationnetwork.node.shared.domain.fork.ForkInfoStorage
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, MptOverlay}
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
      // Lazy-bound `bestTipsFn` for overlay eviction (#56.10 Phase I, refined #113, multi-tip in #115).
      //
      // Default = `lastGlobalSnapshotStorage.get.map(_.toSet.map(h => BranchId(h.hash)))` — every layer
      // (gl0/gl1/cl1/dl1/ml0) maintains `lastGlobalSnapshot`, and the canonical hash of the
      // most-recently-accepted snapshot is exactly what the overlay's eviction needs as the
      // ancestor-walk tip. Followers don't have multi-tip state (no fork-recovery), so the singleton
      // set is correct here. Without this, follower layers had no ancestor protection under
      // MultiBranch and saw recoverable SPM under stress (gl1 ~50, cl1/dl1 ~28 each per full e2e).
      //
      // dag-l0 overrides via `setBestTipsFn(chainStore.allTips...)` once `NakamotoChainStore` is
      // built, since the chain store has every viable chain head — the canonical bestTip AND every
      // tentative-branch head being followed during fork-recovery. Returning the full set (not just
      // bestTip) is the #115 fix: when validator commits canonical-N before chain reorgs to it,
      // canonical-N isn't bestTip but IS in allTips, so its ancestors are protected from eviction.
      bestTipsFnRef <- Ref.of[F, F[Set[BranchId]]](
        lastGlobalSnapshotStorage.get.map(_.map(hashed => BranchId(hashed.hash)).toSet)
      )
      // `bestTipFn` for `GlobalStateReader.pending` (#117/#118 Phase 2). The single canonical
      // bestTip — under MultiBranch the chain's pending writes live under this branch, so HTTP /
      // service reads via the `pending` reader walk this branch first and fall through to base on
      // miss. Default reads the last accepted global snapshot's hash, which is correct for layers
      // that don't have a `NakamotoChainStore` (followers never construct the `pending` reader, so
      // they never hit this default through that path either — it's only here so the default is
      // sensible if some code path constructs `pending` on a follower in error). dag-l0 overrides
      // via `setBestTipFn(chainStore.bestTip...)` once `NakamotoChainStore` is built, so the
      // reader sees the chain's true canonical tip across reorgs (not just the most-recently
      // accepted snapshot, which may not be on the canonical chain during fork recovery).
      bestTipFnRef <- Ref.of[F, F[Option[BranchId]]](
        lastGlobalSnapshotStorage.get.map(_.map(hashed => BranchId(hashed.hash)))
      )
      mptOverlay <- MptOverlay.make[F, GlobalStateKey](
        // Phase J landed two of three prerequisites for MultiBranch:
        //   1. ✅ `overlay.commit(handle, BranchId(snapshotHash), ordinal)` is now called by the proposer
        //      (`GlobalSnapshotConsensusFunctions` after `globalSnapshot` is hashed) and the follower
        //      (`GlobalSnapshotContextFunctions` after state-proof verification passes), each using the
        //      resulting artifact's hash as the real `childTip` — no more self-loop in `pendingRef`.
        //   2. ✅ `accept()` derives the proof's global `mptRoot` from `overlay.allEntriesAsBytesWithHandle`
        //      (post-write byte view including handle's pending writes) via `mptStateProofFromBytes`,
        //      not `producer.getRootHashForOrdinal` — correct under MultiBranch where the producer has
        //      no per-ordinal commit until `finalizeBranch.foldIntoBase`.
        //   3. ❌ Per-manager prior reads route through a branch-aware reader (`GlobalStateReader.dynamic`
        //      bound to a `Ref[BranchId]` set at accept() top), AND the 3 direct `mptStore.getAllX` calls
        //      in GSAM accept() now use the branch-aware `mpt` (AcceptanceMpt). Both compile clean and
        //      the parity gate (#107) + GSAM unit suite (67 tests) pass under both modes.
        //
        // PENDING for MultiBranch flip: e2e #3 (2026-05-06 evening) still surfaces follower mptRoot
        // mismatches at low ordinals. gl1 (Global L1 follower) computes mptRoot=0b45d762698c at ord=3
        // (with `gsi.currSnapshots=1`, correctly carrying ord-2's currency snapshots forward), while
        // gl0's leader-claimed mptRoot=56ad37cf069a corresponds to `gsi.currSnapshots=0`. Both nodes
        // run identical accept() with the same inputs, so the divergence implies one of the per-call
        // overlay paths is missing the parent-branch's pending writes despite the reader fix. Likely
        // suspects: (a) leader runs createProposalArtifact AND createContext per ord — second commit
        // overwrites first, but if the second runs against a stale `branchTipRef.get` we'd see the
        // observed pattern; (b) the createContext path on the leader may diverge from the follower's
        // path despite identical algebra. Needs more investigation before re-flipping.
        mode = MptOverlay.OverlayMode.productionDefault,
        underlying = mptStore,
        pcTree = mptOverlayParentChildTree,
        toHex = GlobalStateKey.toHex[F],
        bestTipsFn = bestTipsFnRef.get.flatten
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
        mptOverlay = mptOverlay,
        setBestTipsFn = bestTipsFnRef.set,
        bestTipFn = bestTipFnRef.get.flatten,
        setBestTipFn = bestTipFnRef.set
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
  val mptOverlay: MptOverlay[F, GlobalStateKey],
  // Setter for the overlay's eviction `bestTipsFn` (#56.10 Phase I, multi-tip in #115). Called
  // from the dag-l0 wiring layer once the chain store is constructed; layers without a chain
  // store never call this and the overlay sees the lastGlobalSnapshot-derived singleton set
  // from the default in `make`.
  val setBestTipsFn: F[Set[BranchId]] => F[Unit],
  // Reader-tip for `GlobalStateReader.pending` (#117/#118 Phase 2). Reads the chain's canonical
  // best tip — used by gl0 HTTP / read paths to pick up the chain's pending writes that haven't
  // been folded into base yet (the overlay walks pending → base on miss). Mirrors the
  // `setBestTipsFn` pattern: dag-l0 overrides via `setBestTipFn(chainStore.bestTip...)` once
  // `NakamotoChainStore` is built. The default reads `lastGlobalSnapshot.get.hash`.
  val bestTipFn: F[Option[BranchId]],
  val setBestTipFn: F[Option[BranchId]] => F[Unit]
)
