package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.swap.block.{
  AllowSpendBlockAcceptanceContext,
  AllowSpendBlockAcceptanceManager,
  AllowSpendBlockAcceptanceResult
}
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock.{TokenLockBlock, TokenLockReference}
import io.constellationnetwork.schema.transaction.{Transaction, TransactionReference}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.types.all.NonNegLong

class BlockAcceptanceOpsManager[F[_]: Async: Parallel](
  blockAcceptanceManager: BlockAcceptanceManager[F],
  tokenLockBlockAcceptanceManager: TokenLockBlockAcceptanceManager[F],
  allowSpendBlockAcceptanceManager: AllowSpendBlockAcceptanceManager[F],
  collateral: Amount
) {

  def acceptBlocks(
    blocksForAcceptance: List[Signed[Block]],
    lastSnapshotContext: CurrencySnapshotContext,
    snapshotOrdinal: SnapshotOrdinal,
    lastActiveTips: SortedSet[ActiveTip],
    lastDeprecatedTips: SortedSet[DeprecatedTip],
    initialTxRef: TransactionReference,
    shouldPerformMetagraphSpecificValidations: Boolean
  )(implicit hasher: Hasher[F]): F[BlockAcceptanceResult] = {
    val tipUsages = getTipsUsages(lastActiveTips, lastDeprecatedTips)
    val context = BlockAcceptanceContext.fromStaticData(
      lastSnapshotContext.snapshotInfo.balances,
      lastSnapshotContext.snapshotInfo.lastTxRefs,
      tipUsages,
      collateral,
      initialTxRef
    )

    blockAcceptanceManager.acceptBlocksIteratively(
      blocksForAcceptance,
      context,
      snapshotOrdinal,
      shouldPerformMetagraphSpecificValidations
    )
  }

  def acceptTokenLockBlocks(
    tokenLockBlocksForAcceptance: List[Signed[TokenLockBlock]],
    lastSnapshotContext: CurrencySnapshotContext,
    snapshotOrdinal: SnapshotOrdinal,
    initialTxRef: TokenLockReference,
    shouldPerformMetagraphSpecificValidations: Boolean,
    lastSyncGlobalSnapshotEpochProgress: EpochProgress
  )(implicit hasher: Hasher[F]): F[TokenLockBlockAcceptanceResult] = {
    val expectedCurrencyId = CurrencyId(lastSnapshotContext.address)
    val (validLaneBlocks, invalidLaneBlocks) = tokenLockBlocksForAcceptance.sorted.partition(
      BlockAcceptanceOpsManager.isValidMetagraphTokenLockBlock(_, expectedCurrencyId)
    )
    val context = TokenLockBlockAcceptanceContext.fromStaticData(
      lastSnapshotContext.snapshotInfo.balances,
      lastSnapshotContext.snapshotInfo.lastTokenLockRefs.getOrElse(SortedMap.empty),
      collateral,
      initialTxRef,
      List.empty,
      lastSyncGlobalSnapshotEpochProgress
    )

    tokenLockBlockAcceptanceManager
      .acceptBlocksIteratively(
        validLaneBlocks,
        context,
        snapshotOrdinal,
        shouldPerformMetagraphSpecificValidations,
        lastSyncGlobalSnapshotEpochProgress.some
      )
      .map { result =>
        result.copy(
          notAccepted = result.notAccepted ++ invalidLaneBlocks.map(_ -> InvalidMetagraphTokenLockLane(expectedCurrencyId))
        )
      }
  }

  def acceptAllowSpendBlocks(
    blocksForAcceptance: List[Signed[AllowSpendBlock]],
    lastSnapshotContext: CurrencySnapshotContext,
    snapshotOrdinal: SnapshotOrdinal,
    initialTxRef: AllowSpendReference,
    shouldPerformMetagraphSpecificValidations: Boolean,
    lastUnsyncGlobalSnapshotOrdinal: SnapshotOrdinal,
    fixingAllowSpendAndTokenLockValidation: SnapshotOrdinal,
    lastSyncGlobalSnapshotEpochProgress: EpochProgress
  )(implicit hasher: Hasher[F]): F[AllowSpendBlockAcceptanceResult] = {
    val context = AllowSpendBlockAcceptanceContext.fromStaticData(
      lastSnapshotContext.snapshotInfo.balances,
      lastSnapshotContext.snapshotInfo.lastAllowSpendRefs.getOrElse(Map.empty),
      collateral,
      initialTxRef
    )

    val maybeEpochProgress =
      if (lastUnsyncGlobalSnapshotOrdinal > fixingAllowSpendAndTokenLockValidation)
        lastSyncGlobalSnapshotEpochProgress.some
      else
        none

    allowSpendBlockAcceptanceManager.acceptBlocksIteratively(
      blocksForAcceptance,
      context,
      snapshotOrdinal,
      shouldPerformMetagraphSpecificValidations,
      maybeEpochProgress
    )
  }

  def acceptTransactionRefs(
    lastTxRefs: SortedMap[Address, TransactionReference],
    lastTxRefsContextUpdate: Map[Address, TransactionReference],
    acceptedTransactions: SortedSet[Signed[Transaction]],
    initialTxRef: TransactionReference
  ): SortedMap[Address, TransactionReference] = {
    val updatedRefs = lastTxRefs ++ lastTxRefsContextUpdate
    val newDestinationAddresses = acceptedTransactions.map(_.destination) -- updatedRefs.keySet
    // [#113] For currency snapshots, the empty parent reference is `emptyCurrency(metagraphAddress)` — not
    // `TransactionReference.empty (0, 0x000)`. cl1's TransactionStorage uses `emptyCurrency` for fresh wallets;
    // populating destinations with DAG-style empty here causes `ParentHashNotEqLastTxHash` rejection at ml0
    // when a fresh recipient subsequently sends. `initialTxRef` is `emptyCurrency(metagraphAddress)` per
    // CurrencySnapshotAcceptanceManager line 184, the same value used by `getInitialTxRef` fallback.
    updatedRefs ++ newDestinationAddresses.toList.map(_ -> initialTxRef)
  }

  def acceptTokenLockRefs(
    lastTxRefs: SortedMap[Address, TokenLockReference],
    lastTxRefsContextUpdate: Map[Address, TokenLockReference]
  ): SortedMap[Address, TokenLockReference] =
    lastTxRefs ++ lastTxRefsContextUpdate

  def acceptAllowSpendRefs(
    lastAllowSpendRefs: SortedMap[Address, AllowSpendReference],
    lastAllowSpendContextUpdate: Map[Address, AllowSpendReference]
  ): SortedMap[Address, AllowSpendReference] =
    lastAllowSpendRefs ++ lastAllowSpendContextUpdate

  private def getTipsUsages(
    lastActive: Set[ActiveTip],
    lastDeprecated: Set[DeprecatedTip]
  ): Map[BlockReference, NonNegLong] = {
    val activeTipsUsages = lastActive.map(at => (at.block, at.usageCount)).toMap
    val deprecatedTipsUsages = lastDeprecated.map(dt => (dt.block, deprecationThreshold)).toMap

    activeTipsUsages ++ deprecatedTipsUsages
  }
}

object BlockAcceptanceOpsManager {
  private[currency] def isValidMetagraphTokenLockBlock(
    block: Signed[TokenLockBlock],
    expectedCurrencyId: CurrencyId
  ): Boolean =
    block.tokenLocks.forall { tokenLock =>
      tokenLock.currencyId.contains(expectedCurrencyId) && tokenLock.replaceTokenLockRef.isEmpty
    }

  def make[F[_]: Async: Parallel](
    blockAcceptanceManager: BlockAcceptanceManager[F],
    tokenLockBlockAcceptanceManager: TokenLockBlockAcceptanceManager[F],
    allowSpendBlockAcceptanceManager: AllowSpendBlockAcceptanceManager[F],
    collateral: Amount
  ): BlockAcceptanceOpsManager[F] =
    new BlockAcceptanceOpsManager[F](
      blockAcceptanceManager,
      tokenLockBlockAcceptanceManager,
      allowSpendBlockAcceptanceManager,
      collateral
    )
}
