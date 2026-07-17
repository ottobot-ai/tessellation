package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.Parallel
import cats.data.NonEmptySetImpl.catsDataInstancesForNonEmptySet
import cats.effect.{Async, Ref}
import cats.syntax.all._

import scala.collection.immutable.SortedSet

import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.economics.StakeBackingValidator
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{ActiveTokenLockMptReader, GlobalStateReader}
import io.constellationnetwork.node.shared.domain.swap.block._
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed

final case class BackingAwareTokenLockAcceptanceResult(
  result: TokenLockBlockAcceptanceResult,
  parentBackingState: Option[StakeBackingValidator.ValidatedBackingState]
)

trait BlockAcceptanceCoordinatorManager[F[_]] {
  def acceptBlocks(
    blocksForAcceptance: List[Signed[Block]],
    lastSnapshotContext: GlobalSnapshotInfo,
    lastActiveTips: SortedSet[ActiveTip],
    lastDeprecatedTips: SortedSet[DeprecatedTip],
    ordinal: SnapshotOrdinal
  )(implicit hasher: Hasher[F]): F[BlockAcceptanceResult]

  def acceptAllowSpendBlocks(
    blocksForAcceptance: List[Signed[AllowSpendBlock]],
    lastSnapshotContext: GlobalSnapshotInfo,
    snapshotOrdinal: SnapshotOrdinal,
    epochProgress: EpochProgress
  )(implicit hasher: Hasher[F]): F[AllowSpendBlockAcceptanceResult]

  def acceptTokenLockBlocks(
    blocksForAcceptance: List[Signed[TokenLockBlock]],
    lastSnapshotContext: GlobalSnapshotInfo,
    snapshotOrdinal: SnapshotOrdinal,
    epochProgress: EpochProgress
  )(implicit hasher: Hasher[F]): F[TokenLockBlockAcceptanceResult]

  def acceptTokenLockBlocksWithBackingState(
    blocksForAcceptance: List[Signed[TokenLockBlock]],
    lastSnapshotContext: GlobalSnapshotInfo,
    snapshotOrdinal: SnapshotOrdinal,
    epochProgress: EpochProgress,
    parentBackingState: Option[StakeBackingValidator.ValidatedBackingState]
  )(implicit hasher: Hasher[F]): F[BackingAwareTokenLockAcceptanceResult]
}

object BlockAcceptanceCoordinatorManager {

  def make[F[_]: Async: Parallel](
    blockAcceptanceManager: BlockAcceptanceManager[F],
    allowSpendBlockAcceptanceManager: AllowSpendBlockAcceptanceManager[F],
    tokenLockBlockAcceptanceManager: TokenLockBlockAcceptanceManager[F],
    tipUsageManager: TipUsageManager[F],
    collateral: Amount,
    reader: GlobalStateReader[F]
  ): BlockAcceptanceCoordinatorManager[F] = new BlockAcceptanceCoordinatorManager[F] {

    def acceptBlocks(
      blocksForAcceptance: List[Signed[Block]],
      lastSnapshotContext: GlobalSnapshotInfo,
      lastActiveTips: SortedSet[ActiveTip],
      lastDeprecatedTips: SortedSet[DeprecatedTip],
      ordinal: SnapshotOrdinal
    )(implicit hasher: Hasher[F]): F[BlockAcceptanceResult] = {
      val tipUsages = tipUsageManager.getTipsUsages(lastActiveTips, lastDeprecatedTips)
      // §G4: balances + lastTxRefs sourced from the branch-aware MPT reader instead of the
      // GSI map snapshot. The reader is scoped to the consensus parent branch (chain bestTip
      // under MultiBranch) — same view `lastSnapshotContext` was derived from in §G1.
      // `tipUsages` remains in-memory (TipUsageManager, not MPT). `lastSnapshotContext`
      // stays on the trait surface for §G5/G6 (downstream managers still consume it).
      val context = BlockAcceptanceContext.fromMpt[F](
        reader,
        tipUsages,
        collateral,
        TransactionReference.empty
      )

      blockAcceptanceManager.acceptBlocksIteratively(blocksForAcceptance, context, ordinal)
    }

    def acceptAllowSpendBlocks(
      blocksForAcceptance: List[Signed[AllowSpendBlock]],
      lastSnapshotContext: GlobalSnapshotInfo,
      snapshotOrdinal: SnapshotOrdinal,
      epochProgress: EpochProgress
    )(implicit hasher: Hasher[F]): F[AllowSpendBlockAcceptanceResult] = {
      val (nativeBlocks, invalidLaneBlocks) = blocksForAcceptance.sorted.partition(_.value.transactions.forall(_.currencyId.isEmpty))
      // §G4: balances + lastAllowSpendRefs sourced from the branch-aware MPT reader.
      val context = AllowSpendBlockAcceptanceContext.fromMpt[F](
        reader,
        collateral,
        AllowSpendReference.empty
      )
      allowSpendBlockAcceptanceManager
        .acceptBlocksIteratively(
          nativeBlocks,
          context,
          snapshotOrdinal,
          shouldPerformMetagraphSpecificValidations = true,
          epochProgress.some
        )
        .map { result =>
          result.copy(
            notAccepted = result.notAccepted ++ invalidLaneBlocks.map(_ -> InvalidGlobalAllowSpendLane)
          )
        }
    }

    private def acceptTokenLockBlocksWithParent(
      blocksForAcceptance: List[Signed[TokenLockBlock]],
      lastSnapshotContext: GlobalSnapshotInfo,
      snapshotOrdinal: SnapshotOrdinal,
      epochProgress: EpochProgress,
      suppliedParentBackingState: Option[StakeBackingValidator.ValidatedBackingState]
    )(implicit hasher: Hasher[F]): F[BackingAwareTokenLockAcceptanceResult] = {
      val (nativeBlocks, invalidLaneBlocks) = blocksForAcceptance.sorted.partition(_.tokenLocks.forall(_.currencyId.isEmpty))
      val replacementTxs = nativeBlocks.flatMap(_.value.tokenLocks.toList).filter(_.replaceTokenLockRef.nonEmpty)
      val replacementRefHashes = replacementTxs.flatMap(_.replaceTokenLockRef).toSet
      val replacementSources = replacementTxs.map(_.source).distinct.sorted

      for {
        backingStateCache <- Ref.of[F, Option[StakeBackingValidator.ValidatedBackingState]](suppliedParentBackingState)
        toBeReplacedHashedTokenLocks <- replacementSources.flatTraverse { source =>
          ActiveTokenLockMptReader
            .readNative(reader, source)
            .map(_.fold(List.empty[Signed[TokenLock]])(_.toList))
            .flatMap(_.traverse(_.toHashed))
        }.map(_.filter(lock => replacementRefHashes.contains(lock.hash)).sortBy(_.hash))

        // §G4: balances + lastTokenLockRefs sourced from the branch-aware MPT reader.
        // `toBeReplacedHashedTokenLocks` already resolves via the same `reader` above.
        loadParentBackingState = backingStateCache.get.flatMap {
          case Some(state) => state.pure[F]
          case None =>
            StakeBackingValidator.validateParent(reader).flatTap(state => backingStateCache.set(state.some))
        }
        context = TokenLockBlockAcceptanceContext.fromMptWithBackingLookup[F](
          reader,
          collateral,
          TokenLockReference.empty,
          toBeReplacedHashedTokenLocks,
          epochProgress,
          ref => loadParentBackingState.map(_.replacementRequirements.get(ref))
        )
        res <- tokenLockBlockAcceptanceManager.acceptBlocksIteratively(
          nativeBlocks,
          context,
          snapshotOrdinal,
          shouldPerformMetagraphSpecificValidations = true,
          epochProgress.some
        )
        capturedBackingState <- backingStateCache.get
      } yield
        BackingAwareTokenLockAcceptanceResult(
          res.copy(
            notAccepted = res.notAccepted ++ invalidLaneBlocks.map(_ -> InvalidGlobalTokenLockLane)
          ),
          capturedBackingState
        )
    }

    def acceptTokenLockBlocks(
      blocksForAcceptance: List[Signed[TokenLockBlock]],
      lastSnapshotContext: GlobalSnapshotInfo,
      snapshotOrdinal: SnapshotOrdinal,
      epochProgress: EpochProgress
    )(implicit hasher: Hasher[F]): F[TokenLockBlockAcceptanceResult] =
      acceptTokenLockBlocksWithParent(
        blocksForAcceptance,
        lastSnapshotContext,
        snapshotOrdinal,
        epochProgress,
        None
      ).map(_.result)

    override def acceptTokenLockBlocksWithBackingState(
      blocksForAcceptance: List[Signed[TokenLockBlock]],
      lastSnapshotContext: GlobalSnapshotInfo,
      snapshotOrdinal: SnapshotOrdinal,
      epochProgress: EpochProgress,
      parentBackingState: Option[StakeBackingValidator.ValidatedBackingState]
    )(implicit hasher: Hasher[F]): F[BackingAwareTokenLockAcceptanceResult] =
      acceptTokenLockBlocksWithParent(
        blocksForAcceptance,
        lastSnapshotContext,
        snapshotOrdinal,
        epochProgress,
        parentBackingState
      )
  }
}
