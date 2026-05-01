package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import java.lang.management.ManagementFactory

import cats.data.NonEmptyList
import cats.data.NonEmptySetImpl.catsDataInstancesForNonEmptySet
import cats.effect.{Async, Sync}
import cats.syntax.all._
import cats.{MonadThrow, Parallel}

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.util.control.NoStackTrace

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.merkletree.Proof
import io.constellationnetwork.merkletree.syntax._
import io.constellationnetwork.node.shared.config.types.{FieldsAddedOrdinals, MetagraphsSyncConfig}
import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.delegatedStake.{
  UpdateDelegatedStakeAcceptanceManager,
  UpdateDelegatedStakeAcceptanceResult
}
import io.constellationnetwork.node.shared.domain.node.UpdateNodeParametersAcceptanceManager
import io.constellationnetwork.node.shared.domain.nodeCollateral.{
  UpdateNodeCollateralAcceptanceManager,
  UpdateNodeCollateralAcceptanceResult
}
import io.constellationnetwork.node.shared.domain.priceOracle.PricingUpdateValidator.PricingUpdateValidationError
import io.constellationnetwork.node.shared.domain.priceOracle.{PriceStateUpdater, PricingUpdateValidator}
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator.SpendActionValidationError
import io.constellationnetwork.node.shared.domain.swap.block.{AllowSpendBlockAcceptanceManager, AllowSpendBlockAcceptanceResult}
import io.constellationnetwork.node.shared.domain.tokenlock.block.{TokenLockBlockAcceptanceManager, TokenLockBlockAcceptanceResult}
import io.constellationnetwork.node.shared.infrastructure.snapshot._
import io.constellationnetwork.node.shared.logger.LoggerBundle
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact._
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal, UpdateNodeCollateral}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.priceOracle.{PriceRecord, TokenPair}
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.producer.StatefulMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.CompatCodecs._
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary, StateChannelValidationType}
import io.constellationnetwork.syntax.sortedCollection.{sortedMapSyntax, sortedSetSyntax}

import fs2.Stream
import io.circe.Json
import io.circe.disjunctionCodecs._

case class PartitionedRecords[A, B](
  existing: SortedMap[Address, A],
  unexpired: SortedMap[Address, B],
  expired: SortedMap[Address, B]
)

case class MerkleTreeResult(
  tree: Option[io.constellationnetwork.merkletree.MerkleTree],
  proofs: SortedMap[Address, Proof]
)

case class ArtifactValidationResult(
  acceptedSpendActions: Map[Address, List[SpendAction]],
  // Each metagraph can emit multiple SpendActions in one snapshot; if more than one is
  // rejected, we need to see ALL of them, not just the last-processed. Previously the
  // type was `(SpendAction, List[Error])` and a `.toMap` in the validator silently
  // dropped shadowed entries (bug surfaced in DoubleUseAllowSpend e2e).
  rejectedSpendActions: Map[Address, List[(SpendAction, List[SpendActionValidationError])]],
  acceptedPricingUpdates: List[PricingUpdate],
  rejectedPricingUpdates: List[(PricingUpdate, List[PricingUpdateValidationError])]
)

/** Result of cleaning state maps - includes cleaned maps and removed keys for MPT sync */
case class CleanedStateMapsResult(
  cleanedAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
  cleanedTokenLockBalances: SortedMap[Address, SortedMap[Address, Balance]],
  cleanedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
  cleanedCreateDelegatedStakes: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
  cleanedWithdrawDelegatedStakes: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
  cleanedCreateNodeCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]],
  cleanedWithdrawNodeCollaterals: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]],
  // Removed keys for MPT incremental sync
  removedDelegatedStakeKeys: Set[Address],
  removedDelegatedStakeWithdrawalKeys: Set[Address],
  removedNodeCollateralKeys: Set[Address],
  removedNodeCollateralWithdrawalKeys: Set[Address]
)

/** Orchestrates acceptance of all event types into a new global snapshot.
  *
  * This is the main pipeline that processes blocks, state channel events, delegated stakes, node collaterals, allow spends, token locks,
  * rewards, and spend transactions to produce the next `GlobalSnapshotInfo` and `GlobalSnapshotStateProof`.
  *
  * '''Determinism contract''': All input lists MUST be in canonical (sorted) order. The pipeline uses `foldLeft`/`foldLeftM` with stateful
  * accumulation in several places, making the output dependent on input ordering. Internal intermediate collections use `SortedMap` to
  * prevent iteration-order divergence across peers.
  *
  * '''MptStore side effects''': Calls `mptStore.syncFromStateChanges` at the end, mutating the shared Merkle Patricia Trie. On validation
  * failure, the caller must restore from a savepoint to prevent partial state from leaking to future rounds.
  */
trait GlobalSnapshotAcceptanceManager[F[_]] {
  def accept(
    ordinal: SnapshotOrdinal,
    epochProgress: EpochProgress,
    previousEpochProgress: EpochProgress,
    blocksForAcceptance: List[Signed[Block]],
    allowSpendBlocksForAcceptance: List[Signed[AllowSpendBlock]],
    tokenLockBlocksForAcceptance: List[Signed[TokenLockBlock]],
    scEvents: List[StateChannelOutput],
    unpEvents: List[Signed[UpdateNodeParameters]],
    cdsEvents: List[Signed[UpdateDelegatedStake.Create]],
    wdsEvents: List[Signed[UpdateDelegatedStake.Withdraw]],
    cncEvents: List[Signed[UpdateNodeCollateral.Create]],
    wncEvents: List[Signed[UpdateNodeCollateral.Withdraw]],
    lastSnapshotContext: GlobalSnapshotInfo,
    lastActiveTips: SortedSet[ActiveTip],
    lastDeprecatedTips: SortedSet[DeprecatedTip],
    calculateRewardsFn: RewardsInput => F[DelegatedRewardsResult],
    validationType: StateChannelValidationType,
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  ): F[
    (
      BlockAcceptanceResult,
      AllowSpendBlockAcceptanceResult,
      TokenLockBlockAcceptanceResult,
      UpdateDelegatedStakeAcceptanceResult,
      UpdateNodeCollateralAcceptanceResult,
      SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
      Set[StateChannelOutput],
      SortedSet[RewardTransaction],
      GlobalSnapshotInfo,
      GlobalSnapshotStateProof,
      Map[Address, List[SpendAction]],
      SortedMap[Id, Signed[UpdateNodeParameters]],
      SortedSet[SharedArtifact],
      SortedMap[PeerId, Map[Address, Amount]]
    )
  ]
}

object GlobalSnapshotAcceptanceManager {

  private case object InvalidMerkleTree extends NoStackTrace

  def make[F[_]: Async: Parallel: HasherSelector: SecurityProvider: JsonSerializer](
    fieldsAddedOrdinals: FieldsAddedOrdinals,
    metagraphsSyncConfig: MetagraphsSyncConfig,
    environment: AppEnvironment,
    blockAcceptanceManager: BlockAcceptanceManager[F],
    allowSpendBlockAcceptanceManager: AllowSpendBlockAcceptanceManager[F],
    tokenLockBlockAcceptanceManager: TokenLockBlockAcceptanceManager[F],
    stateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[F],
    updateNodeParametersAcceptanceManager: UpdateNodeParametersAcceptanceManager[F],
    updateDelegatedStakeAcceptanceManager: UpdateDelegatedStakeAcceptanceManager[F],
    updateNodeCollateralAcceptanceManager: UpdateNodeCollateralAcceptanceManager[F],
    spendActionValidator: SpendActionValidator[F],
    pricingUpdateValidator: PricingUpdateValidator[F],
    priceStateUpdater: PriceStateUpdater[F],
    collateral: Amount,
    withdrawalTimeLimit: EpochProgress,
    mptStore: MptStore[F, GlobalStateKey],
    loggerBundle: LoggerBundle[F],
    undoJournal: Option[io.constellationnetwork.node.shared.domain.nakamoto.MptUndoJournal[F]] = None,
    // When true, accept() emits adds/removes for the node-collateral-withdrawal expiry index. Requires the same
    // `Some(withdrawalTimeLimit)` to also be passed to every `syncFromGlobalSnapshotInfo` / `toAllStateKeyValueBytes`
    // in the node's production paths — otherwise rebuild-path and delta-path mptRoots diverge. Default false until
    // that threading lands.
    maintainNodeCollateralWithdrawalExpiryIndex: Boolean = false
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector
  ): GlobalSnapshotAcceptanceManager[F] = {
    // Establish the WithdrawalTimeLimit implicit from the explicit constructor param so that the rebuild paths
    // (`syncFromGlobalSnapshotInfo`, `stateProofBuilder` → `mptStateProof`) see the same limit the accept path uses
    // to compute expiry-index buckets. Gated by the feature flag until the threading below lands everywhere.
    implicit val withdrawalTimeLimitCtx: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
      if (maintainNodeCollateralWithdrawalExpiryIndex)
        io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.some(withdrawalTimeLimit)
      else
        io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none
    val artifactEmissionManager = ArtifactEmissionManager.make[F]()
    val tipUsageManager = TipUsageManager.make[F]()
    val metagraphSyncManager = MetagraphSyncManager.make[F](metagraphsSyncConfig)
    val rewardAcceptanceManager = RewardAcceptanceManager.make[F](mptStore)
    val allowSpendStateManager = AllowSpendStateManager.make[F](mptStore)
    val tokenLockStateManager = TokenLockStateManager.make[F](mptStore)
    val spendTransactionBalanceManager = SpendTransactionBalanceManager.make[F](mptStore)
    val delegatedStakeStateManager = DelegatedStakeStateManager.make[F](mptStore)
    val nodeCollateralStateManager = NodeCollateralStateManager.make[F](mptStore)
    val transactionReferenceManager = TransactionReferenceManager.make[F](mptStore)

    val blockAcceptanceCoordinatorManager = BlockAcceptanceCoordinatorManager.make[F](
      blockAcceptanceManager,
      allowSpendBlockAcceptanceManager,
      tokenLockBlockAcceptanceManager,
      tipUsageManager,
      collateral,
      mptStore
    )

    new GlobalSnapshotAcceptanceManager[F] {
      private val builder = GlobalSnapshotInfo.stateProofBuilder(Some(mptStore.underlying))

      case class InitialData(
        blockResult: BlockAcceptanceResult,
        delegatedResult: UpdateDelegatedStakeAcceptanceResult,
        nodeParamsResult: SortedMap[Id, Signed[UpdateNodeParameters]],
        existingStakes: PartitionedRecords[SortedSet[DelegatedStakeRecord], SortedSet[PendingDelegatedStakeWithdrawal]]
      )

      private def acceptInitialData(
        ordinal: SnapshotOrdinal,
        epochProgress: EpochProgress,
        blocksForAcceptance: List[Signed[Block]],
        cdsEvents: List[Signed[UpdateDelegatedStake.Create]],
        wdsEvents: List[Signed[UpdateDelegatedStake.Withdraw]],
        unpEvents: List[Signed[UpdateNodeParameters]],
        lastSnapshotContext: GlobalSnapshotInfo,
        lastActiveTips: SortedSet[ActiveTip],
        lastDeprecatedTips: SortedSet[DeprecatedTip],
        acceptedGlobalTokenLocks: List[Signed[TokenLock]]
      )(
        implicit hasher: Hasher[F]
      ): F[InitialData] =
        for {
          blockResult <- blockAcceptanceCoordinatorManager.acceptBlocks(
            blocksForAcceptance,
            lastSnapshotContext,
            lastActiveTips,
            lastDeprecatedTips,
            ordinal
          )

          unexpiredStakes <- delegatedStakeStateManager
            .processExistingDelegatedStakes(
              lastSnapshotContext,
              epochProgress,
              acceptedGlobalTokenLocks,
              withdrawalTimeLimit
            )

          delegatedResult <- updateDelegatedStakeAcceptanceManager.accept(
            cdsEvents,
            wdsEvents,
            lastSnapshotContext,
            epochProgress,
            ordinal,
            acceptedGlobalTokenLocks
          )

          nodeParamsResult <- updateNodeParametersAcceptanceManager
            .acceptUpdateNodeParameters(unpEvents, lastSnapshotContext)
            .map(acceptanceResult =>
              SortedMap.from(
                acceptanceResult.accepted.flatMap(signed => signed.proofs.toList.map(proof => (proof.id, signed)))
              )
            )
        } yield
          InitialData(
            blockResult,
            delegatedResult,
            nodeParamsResult,
            unexpiredStakes
          )

      private def acceptNodeCollateral(
        ordinal: SnapshotOrdinal,
        epochProgress: EpochProgress,
        cncEvents: List[Signed[UpdateNodeCollateral.Create]],
        wncEvents: List[Signed[UpdateNodeCollateral.Withdraw]],
        lastSnapshotContext: GlobalSnapshotInfo,
        delegatedStakeAcceptanceResult: UpdateDelegatedStakeAcceptanceResult
      ): F[UpdateNodeCollateralAcceptanceResult] =
        updateNodeCollateralAcceptanceManager.accept(
          cncEvents,
          wncEvents,
          lastSnapshotContext,
          epochProgress,
          ordinal,
          delegatedStakeAcceptanceResult
        )

      private def processStateChannelEvents(
        ordinal: SnapshotOrdinal,
        lastSnapshotContext: GlobalSnapshotInfo,
        updatedGlobalBalances: SortedMap[Address, Balance],
        scEvents: List[StateChannelOutput],
        validationType: StateChannelValidationType,
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[F]): F[StateChannelAcceptanceResult] =
        stateChannelEventsProcessor.process(
          ordinal,
          lastSnapshotContext.copy(balances = updatedGlobalBalances),
          scEvents,
          validationType,
          getGlobalSnapshotByOrdinal
        )

      private def calculateRewards(
        ordinal: SnapshotOrdinal,
        epochProgress: EpochProgress,
        tessellation3MigrationStartingOrdinal: SnapshotOrdinal,
        acceptedTransactions: SortedSet[Signed[Transaction]],
        delegatedStakeAcceptanceResult: UpdateDelegatedStakeAcceptanceResult,
        unexpiredStakes: PartitionedRecords[SortedSet[DelegatedStakeRecord], SortedSet[PendingDelegatedStakeWithdrawal]],
        calculateRewardsFn: RewardsInput => F[DelegatedRewardsResult]
      ): F[DelegatedRewardsResult] = {
        val unexpiredCreateDelegatedStakes = unexpiredStakes.existing
        val unexpiredWithdrawalsDelegatedStaking = unexpiredStakes.unexpired
        val expiredWithdrawalsDelegatedStaking = unexpiredStakes.expired

        if (ordinal.value < tessellation3MigrationStartingOrdinal.value) {
          calculateRewardsFn(ClassicRewardsInput(acceptedTransactions))
        } else {
          calculateRewardsFn(
            DelegateRewardsInput(
              delegatedStakeAcceptanceResult,
              PartitionedStakeUpdates(
                unexpiredCreateDelegatedStakes,
                unexpiredWithdrawalsDelegatedStaking,
                expiredWithdrawalsDelegatedStaking
              ),
              epochProgress
            )
          )
        }
      }

      /** Diff `before` vs `after` per-address; each delta record contributes to the expiry index at epoch `createdAt +
        * withdrawalTimeLimit`. Hashes the record's `event` for the index key's `hash` component.
        */
      private def computeNodeCollateralWithdrawalExpiryIndexDelta(
        before: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]],
        after: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]],
        limit: EpochProgress
      )(implicit hasher: Hasher[F]): F[SystemIndexDelta[NodeCollateralWithdrawalExpiryKey]] = {
        val pairAddresses: Set[Address] = before.keySet ++ after.keySet

        def hashEntries(
          addr: Address,
          withdrawals: SortedSet[PendingNodeCollateralWithdrawal]
        ): F[List[(EpochProgress, NodeCollateralWithdrawalExpiryKey)]] =
          withdrawals.toList.traverse { w =>
            w.event.toHashed.map(h => (w.createdAt |+| limit, NodeCollateralWithdrawalExpiryKey(addr, h.hash)))
          }

        type Entries = List[(EpochProgress, NodeCollateralWithdrawalExpiryKey)]
        val empty: (Entries, Entries) = (List.empty, List.empty)

        pairAddresses.toList
          .foldLeftM[F, (Entries, Entries)](empty) {
            case ((accAdds, accRemoves), addr) =>
              val oldSet = before.getOrElse(addr, SortedSet.empty[PendingNodeCollateralWithdrawal])
              val newSet = after.getOrElse(addr, SortedSet.empty[PendingNodeCollateralWithdrawal])
              val added = newSet.diff(oldSet)
              val removed = oldSet.diff(newSet)
              for {
                addedEntries <- hashEntries(addr, added)
                removedEntries <- hashEntries(addr, removed)
              } yield (accAdds ++ addedEntries, accRemoves ++ removedEntries)
          }
          .map {
            case (adds, removes) =>
              val addsMap: SortedMap[EpochProgress, Set[NodeCollateralWithdrawalExpiryKey]] =
                adds.groupMap(_._1)(_._2).view.mapValues(_.toSet).to(SortedMap)
              val removesMap: SortedMap[EpochProgress, Set[NodeCollateralWithdrawalExpiryKey]] =
                removes.groupMap(_._1)(_._2).view.mapValues(_.toSet).to(SortedMap)
              SystemIndexDelta.EpochBucket[NodeCollateralWithdrawalExpiryKey](addsMap, removesMap)
          }
      }

      /** Validates spend actions and pricing updates, returning accepted and rejected results.
        *
        * Always uses sequential execution to guarantee deterministic error ordering across all peers. Previously used conditional
        * parallelism (parMapN when size > 100) which could produce different error orderings depending on thread scheduling.
        */
      private def validateArtifacts(
        epochProgress: EpochProgress,
        spendActions: Map[Address, List[SpendAction]],
        pricingUpdates: Map[Address, List[PricingUpdate]],
        lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        currencyBalances: Map[Option[Address], SortedMap[Address, Balance]],
        globalBalances: Map[Option[Address], SortedMap[Address, Balance]],
        lastSnapshotContext: GlobalSnapshotInfo
      ): F[ArtifactValidationResult] =
        for {
          (acceptedSpend, rejectedSpend) <- spendActionValidator.validateReturningAcceptedAndRejected(
            spendActions,
            lastActiveAllowSpends,
            currencyBalances ++ globalBalances
          )
          (acceptedPricing, rejectedPricing) <- pricingUpdateValidator.validateReturningAcceptedAndRejected(
            pricingUpdates,
            lastSnapshotContext,
            epochProgress
          )
        } yield ArtifactValidationResult(acceptedSpend, rejectedSpend, acceptedPricing, rejectedPricing)

      /** Accepts allow-spend and token-lock blocks sequentially to ensure deterministic results.
        *
        * Uses sequential execution to guarantee identical acceptance results across all peers. AllowSpend and TokenLock acceptance are
        * independent (no shared state), but sequential execution avoids any risk of non-deterministic error ordering from parallel
        * scheduling.
        */
      private def acceptAllowSpendAndTokenLockBlocks(
        ordinal: SnapshotOrdinal,
        epochProgress: EpochProgress,
        allowSpendBlocksForAcceptance: List[Signed[AllowSpendBlock]],
        tokenLockBlocksForAcceptance: List[Signed[TokenLockBlock]],
        lastSnapshotContext: GlobalSnapshotInfo,
        fixingAllowSpendAndTokenLockValidation: SnapshotOrdinal
      )(implicit hasher: Hasher[F]): F[(AllowSpendBlockAcceptanceResult, TokenLockBlockAcceptanceResult)] =
        for {
          allowSpend <- blockAcceptanceCoordinatorManager.acceptAllowSpendBlocks(
            allowSpendBlocksForAcceptance,
            lastSnapshotContext,
            ordinal,
            fixingAllowSpendAndTokenLockValidation,
            epochProgress
          )
          tokenLock <- blockAcceptanceCoordinatorManager.acceptTokenLockBlocks(
            tokenLockBlocksForAcceptance,
            lastSnapshotContext,
            ordinal,
            fixingAllowSpendAndTokenLockValidation,
            epochProgress
          )
        } yield (allowSpend, tokenLock)

      private def buildMerkleTreeAndProofs(
        ordinal: SnapshotOrdinal,
        updatedLastCurrencySnapshots: SortedMap[Address, Either[
          Signed[CurrencySnapshot],
          (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)
        ]]
      )(implicit hasher: Hasher[F]): F[MerkleTreeResult] = {

        import io.constellationnetwork.merkletree.syntax._
        val batchSize = 256

        hasher.getLogic(ordinal) match {

          case JsonHash =>
            for {
              maybeTree <- updatedLastCurrencySnapshots.merkleTree[F]

              proofs <- maybeTree match {
                case Some(tree) =>
                  updatedLastCurrencySnapshots.toList
                    .grouped(batchSize)
                    .toList
                    .traverse { batch =>
                      Async[F].cede *> batch.traverse {
                        case (address, state) =>
                          (address, state).hash
                            .flatMap(tree.findPath[F])
                            .flatMap(MonadThrow[F].fromOption(_, InvalidMerkleTree))
                            .map(address -> _)
                      }
                    }
                    .flatMap(results => Async[F].cede.as(SortedMap.from(results.flatten)))

                case None =>
                  Async[F].pure(SortedMap.empty[Address, Proof])
              }
            } yield MerkleTreeResult(maybeTree, proofs)

          case KryoHash =>
            val converted: SortedMap[Address, Either[
              Signed[CurrencySnapshot],
              (Signed[CurrencyIncrementalSnapshotV1], CurrencySnapshotInfoV1)
            ]] =
              updatedLastCurrencySnapshots.map {
                case (address, Left(snapshot)) => (address, Left(snapshot))
                case (address, Right((Signed(incrementalSnapshot, proofs), info))) =>
                  address ->
                    Right(
                      (
                        Signed(CurrencyIncrementalSnapshotV1.fromCurrencyIncrementalSnapshot(incrementalSnapshot), proofs),
                        CurrencySnapshotInfoV1.fromCurrencySnapshotInfo(info)
                      )
                    )
              }

            for {
              maybeTree <- converted.merkleTree[F]

              proofs <- maybeTree match {
                case Some(tree) =>
                  converted.toList
                    .grouped(batchSize)
                    .toList
                    .traverse { batch =>
                      Async[F].cede *> batch.traverse {
                        case (address, state) =>
                          (address, state).hash
                            .flatMap(tree.findPath[F])
                            .flatMap(MonadThrow[F].fromOption(_, InvalidMerkleTree))
                            .map(address -> _)
                      }
                    }
                    .flatMap(results => Async[F].cede.as(SortedMap.from(results.flatten)))

                case None =>
                  Async[F].pure(SortedMap.empty[Address, Proof])
              }
            } yield MerkleTreeResult(maybeTree, proofs)
        }
      }

      /** Cleans empty entries from state maps and computes removed keys in a single pass.
        *
        * Prior state is passed as keysets-of-non-empty-addresses (`prior*Keys`), not as full maps. This is all that is needed to compute
        * removals (`addresses that had non-empty sets in previous state and are now empty/missing`) and tightens the dependency: the caller
        * need not hand over the entire prior partition — only the addresses that had records.
        */
      private def cleanStateMaps(
        updatedAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        updatedTokenLockBalances: SortedMap[Address, SortedMap[Address, Balance]],
        updatedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        updatedCreateDelegatedStakes: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
        updatedWithdrawDelegatedStakes: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
        updatedCreateNodeCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]],
        updatedWithdrawNodeCollaterals: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]],
        // Prior non-empty keysets — only data needed to compute removals.
        priorDelegatedStakeKeys: Set[Address],
        priorDelegatedStakeWithdrawalKeys: Set[Address],
        priorNodeCollateralKeys: Set[Address],
        priorNodeCollateralWithdrawalKeys: Set[Address]
      ): CleanedStateMapsResult = {
        val cleanedAllowSpends = updatedAllowSpends.map {
          case (outerKey, innerMap) =>
            val cleanedInnerMap = innerMap.filter { case (_, allowSpendSet) => allowSpendSet.nonEmpty }
            (outerKey, cleanedInnerMap)
        }.filter { case (_, innerMap) => innerMap.nonEmpty }

        val cleanedTokenLockBalances = updatedTokenLockBalances.filter { case (_, tokenLockBalances) => tokenLockBalances.nonEmpty }
        val cleanedGlobalTokenLocks = updatedGlobalTokenLocks.filter { case (_, tokenLocks) => tokenLocks.nonEmpty }
        val cleanedCreateDelegatedStakes = updatedCreateDelegatedStakes.filter { case (_, records) => records.nonEmpty }
        val cleanedWithdrawDelegatedStakes = updatedWithdrawDelegatedStakes.filter { case (_, records) => records.nonEmpty }
        val cleanedCreateNodeCollaterals = updatedCreateNodeCollaterals.filter { case (_, records) => records.nonEmpty }
        val cleanedWithdrawNodeCollaterals = updatedWithdrawNodeCollaterals.filter { case (_, records) => records.nonEmpty }

        // Removed keys: addresses that had non-empty sets in previous state but no longer do.
        val removedDelegatedStakeKeys = priorDelegatedStakeKeys -- cleanedCreateDelegatedStakes.keySet
        val removedDelegatedStakeWithdrawalKeys = priorDelegatedStakeWithdrawalKeys -- cleanedWithdrawDelegatedStakes.keySet
        val removedNodeCollateralKeys = priorNodeCollateralKeys -- cleanedCreateNodeCollaterals.keySet
        val removedNodeCollateralWithdrawalKeys = priorNodeCollateralWithdrawalKeys -- cleanedWithdrawNodeCollaterals.keySet

        CleanedStateMapsResult(
          cleanedAllowSpends,
          cleanedTokenLockBalances,
          cleanedGlobalTokenLocks,
          cleanedCreateDelegatedStakes,
          cleanedWithdrawDelegatedStakes,
          cleanedCreateNodeCollaterals,
          cleanedWithdrawNodeCollaterals,
          removedDelegatedStakeKeys,
          removedDelegatedStakeWithdrawalKeys,
          removedNodeCollateralKeys,
          removedNodeCollateralWithdrawalKeys
        )
      }

      private def buildGlobalSnapshotInfo(
        ordinal: SnapshotOrdinal,
        tessellation3MigrationStartingOrdinal: SnapshotOrdinal,
        tessellation301MigrationStartingOrdinal: SnapshotOrdinal,
        metagraphSyncDataStartingOrdinal: SnapshotOrdinal,
        lastSnapshotContext: GlobalSnapshotInfo,
        acceptanceResult: BlockAcceptanceResult,
        updatedLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
        transactionsRefs: SortedMap[Address, TransactionReference],
        updatedBalancesBySpendTransactions: SortedMap[Address, Balance],
        updatedLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        updatedLastCurrencySnapshotProofs: SortedMap[Address, Proof],
        updatedAllowSpendsCleaned: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        updatedGlobalTokenLocksCleaned: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        updatedTokenLockBalancesCleaned: SortedMap[Address, SortedMap[Address, Balance]],
        updatedAllowSpendRefs: SortedMap[Address, AllowSpendReference],
        updatedTokenLockRefs: SortedMap[Address, TokenLockReference],
        updatedUpdateNodeParameters: SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)],
        updatedCreateDelegatedStakesCleaned: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
        updatedWithdrawDelegatedStakesCleaned: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
        updatedCreateNodeCollateralsCleaned: SortedMap[Address, SortedSet[NodeCollateralRecord]],
        updatedWithdrawNodeCollateralsCleaned: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]],
        updatedPriceState: SortedMap[TokenPair, PriceRecord],
        updatedAcceptedMetagraphSyncData: SortedMap[Address, MetagraphSyncDataInfo]
      ): GlobalSnapshotInfo =
        GlobalSnapshotInfo(
          updatedLastStateChannelSnapshotHashes,
          if (ordinal < tessellation3MigrationStartingOrdinal)
            lastSnapshotContext.lastTxRefs ++ acceptanceResult.contextUpdate.lastTxRefs
          else transactionsRefs,
          updatedBalancesBySpendTransactions,
          updatedLastCurrencySnapshots,
          updatedLastCurrencySnapshotProofs,
          if (ordinal < tessellation3MigrationStartingOrdinal) none else updatedAllowSpendsCleaned.some,
          if (ordinal < tessellation3MigrationStartingOrdinal) none else updatedGlobalTokenLocksCleaned.some,
          if (ordinal < tessellation3MigrationStartingOrdinal) none else updatedTokenLockBalancesCleaned.some,
          if (ordinal < tessellation3MigrationStartingOrdinal) none else updatedAllowSpendRefs.some,
          if (ordinal < tessellation3MigrationStartingOrdinal) none else updatedTokenLockRefs.some,
          if (ordinal < tessellation3MigrationStartingOrdinal) none else updatedUpdateNodeParameters.some,
          if (ordinal < tessellation3MigrationStartingOrdinal) none else updatedCreateDelegatedStakesCleaned.some,
          if (ordinal < tessellation3MigrationStartingOrdinal) none else updatedWithdrawDelegatedStakesCleaned.some,
          if (ordinal < tessellation3MigrationStartingOrdinal) none else updatedCreateNodeCollateralsCleaned.some,
          if (ordinal < tessellation3MigrationStartingOrdinal) none else updatedWithdrawNodeCollateralsCleaned.some,
          if (ordinal < tessellation301MigrationStartingOrdinal) none else updatedPriceState.some,
          if (ordinal < metagraphSyncDataStartingOrdinal) none else updatedAcceptedMetagraphSyncData.some
        )

      def accept(
        ordinal: SnapshotOrdinal,
        epochProgress: EpochProgress,
        previousEpochProgress: EpochProgress,
        blocksForAcceptance: List[Signed[Block]],
        allowSpendBlocksForAcceptance: List[Signed[AllowSpendBlock]],
        tokenLockBlocksForAcceptance: List[Signed[TokenLockBlock]],
        scEvents: List[StateChannelOutput],
        unpEvents: List[Signed[UpdateNodeParameters]],
        cdsEvents: List[Signed[UpdateDelegatedStake.Create]],
        wdsEvents: List[Signed[UpdateDelegatedStake.Withdraw]],
        cncEvents: List[Signed[UpdateNodeCollateral.Create]],
        wncEvents: List[Signed[UpdateNodeCollateral.Withdraw]],
        lastSnapshotContext: GlobalSnapshotInfo,
        lastActiveTips: SortedSet[ActiveTip],
        lastDeprecatedTips: SortedSet[DeprecatedTip],
        calculateRewardsFn: RewardsInput => F[DelegatedRewardsResult],
        validationType: StateChannelValidationType,
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      ): F[
        (
          BlockAcceptanceResult,
          AllowSpendBlockAcceptanceResult,
          TokenLockBlockAcceptanceResult,
          UpdateDelegatedStakeAcceptanceResult,
          UpdateNodeCollateralAcceptanceResult,
          SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
          Set[StateChannelOutput],
          SortedSet[RewardTransaction],
          GlobalSnapshotInfo,
          GlobalSnapshotStateProof,
          Map[Address, List[SpendAction]],
          SortedMap[Id, Signed[UpdateNodeParameters]],
          SortedSet[SharedArtifact],
          SortedMap[PeerId, Map[Address, Amount]]
        )
      ] = {
        implicit val hasher: Hasher[F] = HasherSelector[F].getForOrdinal(ordinal)

        val tessellation3MigrationStartingOrdinal = fieldsAddedOrdinals.tessellation3Migration
          .getOrElse(environment, SnapshotOrdinal.MinValue)

        val tessellation301MigrationStartingOrdinal = fieldsAddedOrdinals.tessellation301Migration
          .getOrElse(environment, SnapshotOrdinal.MinValue)

        val metagraphSyncDataStartingOrdinal = fieldsAddedOrdinals.metagraphSyncData
          .getOrElse(environment, SnapshotOrdinal.MinValue)

        val fixingAllowSpendAndTokenLockValidation = fieldsAddedOrdinals.fixingAllowSpendAndTokenLockValidation
          .getOrElse(environment, SnapshotOrdinal.MinValue)

        loggerBundle.app.withOrdinal(ordinal) {
          for {
            _ <- loggerBundle.app.info(
              s"[ACCEPTANCE] ordinal=$ordinal epoch=${epochProgress.show} ENTER " +
                s"blocks=${blocksForAcceptance.size} allowSpend=${allowSpendBlocksForAcceptance.size} " +
                s"tokenLock=${tokenLockBlocksForAcceptance.size} sc=${scEvents.size} unp=${unpEvents.size} " +
                s"cds=${cdsEvents.size} wds=${wdsEvents.size} cnc=${cncEvents.size} wnc=${wncEvents.size}"
            )

            (allowSpendBlockAcceptanceResult, tokenLockBlockAcceptanceResult) <-
              acceptAllowSpendAndTokenLockBlocks(
                ordinal,
                epochProgress,
                allowSpendBlocksForAcceptance,
                tokenLockBlocksForAcceptance,
                lastSnapshotContext,
                fixingAllowSpendAndTokenLockValidation
              )

            acceptedGlobalAllowSpends = allowSpendBlockAcceptanceResult.accepted.flatMap(_.value.transactions.toList)
            acceptedGlobalTokenLocks <- tokenLockStateManager.acceptReplacementTokenLocks(
              tokenLockBlockAcceptanceResult.accepted.flatMap(_.value.tokenLocks.toList),
              lastSnapshotContext
            )

            initialData <-
              acceptInitialData(
                ordinal,
                epochProgress,
                blocksForAcceptance,
                cdsEvents,
                wdsEvents,
                unpEvents,
                lastSnapshotContext,
                lastActiveTips,
                lastDeprecatedTips,
                acceptedGlobalTokenLocks
              )

            nodeCollateralAcceptanceResult <- acceptNodeCollateral(
              ordinal,
              epochProgress,
              cncEvents,
              wncEvents,
              lastSnapshotContext,
              initialData.delegatedResult
            )

            updatedUpdateNodeParameters = lastSnapshotContext.updateNodeParameters.getOrElse(
              SortedMap.empty[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)]
            ) ++ initialData.nodeParamsResult.view.mapValues(unp => (unp, ordinal))

            acceptedTransactions = initialData.blockResult.accepted.flatMap {
              case (block, _) => block.value.transactions.toSortedSet
            }.toSortedSet

            // Source prior-ordinal `balances` from the MPT instead of `lastSnapshotContext.balances`. The
            // ActiveAddressIndex sidecar (maintained on both delta and bootstrap paths) carries the keyset; we
            // `getMany` the values. Read happens before `syncFromStateChanges` so the MPT still reflects the
            // prior ordinal's state. Used here for the early `updatedGlobalBalances` and below for the final
            // GSI's `balances` field.
            priorBalances <- spendTransactionBalanceManager.materializeAllBalancesFromMpt

            updatedGlobalBalances = priorBalances ++ initialData.blockResult.contextUpdate.balances

            StateChannelAcceptanceResult(
              scSnapshots,
              currencySnapshots,
              returnedSCEvents,
              currencyAcceptanceBalanceUpdate,
              incomingCurrencySnapshots
            ) <- processStateChannelEvents(
              ordinal,
              lastSnapshotContext,
              updatedGlobalBalances,
              scEvents,
              validationType,
              getGlobalSnapshotByOrdinal
            )

            transactionsRefsDeltas <- transactionReferenceManager.acceptTransactionRefs(
              initialData.blockResult.contextUpdate.lastTxRefs,
              acceptedTransactions
            )

            // Source prior-ordinal `lastTxRefs` from the MPT instead of `lastSnapshotContext.lastTxRefs`. The
            // ActiveAddressIndex sidecar (maintained on both delta and bootstrap paths) carries the keyset; we
            // `getMany` the values. Read happens before `syncFromStateChanges` so the MPT still reflects the
            // prior ordinal's state.
            priorLastTxRefs <- transactionReferenceManager.materializeLastTxRefsFromMpt

            // Use SortedMap to guarantee deterministic iteration order for downstream processing.
            // Previously used unordered Map which could cause different validation ordering per node.
            currencyBalances = currencySnapshots.toList.map {
              case (_, Left(_))              => SortedMap.empty[Option[Address], SortedMap[Address, Balance]]
              case (address, Right((_, si))) => SortedMap(address.some -> si.balances)
            }.foldLeft(SortedMap.empty[Option[Address], SortedMap[Address, Balance]])(_ ++ _)

            sharedArtifacts = incomingCurrencySnapshots.toList.map {
              case (address, snapshots) =>
                val artifacts: List[SharedArtifact] = snapshots.flatMap {
                  case Left(_)       => Nil
                  case Right((s, _)) => s.artifacts.getOrElse(SortedSet.empty[SharedArtifact]).toList
                }
                SortedMap(address -> artifacts)
            }.foldLeft(SortedMap.empty[Address, List[SharedArtifact]])(_ |+| _).view

            sCSnapshotHashes <- scSnapshots.toList.traverse {
              case (address, nel) => nel.last.toHashed.map(address -> _.hash)
            }.map(_.toSortedMap)

            DelegatedRewardsResult(
              delegatorRewardsMap,
              updatedCreateDelegatedStakes,
              updatedWithdrawDelegatedStakes,
              nodeOperatorRewards,
              reservedAddressRewards,
              withdrawalRewardTxs,
              _
            ) <- calculateRewards(
              ordinal,
              epochProgress,
              tessellation3MigrationStartingOrdinal,
              acceptedTransactions,
              initialData.delegatedResult,
              initialData.existingStakes,
              calculateRewardsFn
            )

            _ <- loggerBundle.app.info(
              s"[ACCEPTANCE] ordinal=$ordinal REWARDS: " +
                s"nodeOperator=${nodeOperatorRewards.size} reserved=${reservedAddressRewards.size} " +
                s"withdrawal=${withdrawalRewardTxs.size} delegatorRewards=${delegatorRewardsMap.size} " +
                s"updatedDelegStakes=${updatedCreateDelegatedStakes.size} updatedDelegWithdrawals=${updatedWithdrawDelegatedStakes.size}"
            )

            (updatedBalancesByRewards, acceptedRewardTxs, rewardBalancesDelta) <- rewardAcceptanceManager.acceptRewardTxs(
              initialData.blockResult.contextUpdate.balances.toSortedMap ++ currencyAcceptanceBalanceUpdate,
              withdrawalRewardTxs ++ nodeOperatorRewards ++ reservedAddressRewards
            )

            globalBalances = SortedMap(none[Address] -> updatedBalancesByRewards)

            // Use SortedMap for deterministic validation ordering across all peers.
            spendActions = sharedArtifacts
              .mapValues(_.collect { case sa: SpendAction => sa })
              .filter { case (_, actions) => actions.nonEmpty }
              .toSortedMap

            pricingUpdates = sharedArtifacts
              .mapValues(_.collect { case pu: PricingUpdate => pu })
              .filter { case (_, updates) => updates.nonEmpty }
              .toSortedMap

            globalSnapshotsProcessed = sharedArtifacts.view
              .mapValues(_.collect { case pu: GlobalSnapshotsProcessed => pu })
              .filter { case (_, updates) => updates.nonEmpty }
              .toSortedMap

            lastActiveAllowSpends <- allowSpendStateManager.materializeActiveAllowSpendsFromMpt

            ArtifactValidationResult(
              acceptedSpendActions,
              rejectedSpendActions,
              acceptedPricingUpdates,
              rejectedPricingUpdates
            ) <- validateArtifacts(
              epochProgress,
              spendActions,
              pricingUpdates,
              lastActiveAllowSpends,
              currencyBalances,
              globalBalances,
              lastSnapshotContext
            )
            acceptedSpendActionsMessage = s"[CONSENSUS:PROPOSAL] [ORDINAL=$ordinal] Accepted spend actions: ${acceptedSpendActions.show}"
            rejectedSpendActionMessage = s"[CONSENSUS:PROPOSAL] [ORDINAL=$ordinal] Rejected spend actions: ${rejectedSpendActions.show}"
            acceptedPricingUpdatesMessage =
              s"[CONSENSUS:PROPOSAL] [ORDINAL=$ordinal] Accepted pricing updates: ${acceptedPricingUpdates.show}"
            rejectedPricingUpdatesMessage =
              s"[CONSENSUS:PROPOSAL] [ORDINAL=$ordinal] Rejected pricing updates: ${rejectedPricingUpdates.show}"

            _ <- loggerBundle.app.info(acceptedSpendActionsMessage)
            _ <- loggerBundle.app.info(rejectedSpendActionMessage)
            _ <- loggerBundle.app.info(acceptedPricingUpdatesMessage)
            _ <- loggerBundle.app.info(rejectedPricingUpdatesMessage)

            updatedLastStateChannelSnapshotHashes = lastSnapshotContext.lastStateChannelSnapshotHashes ++ sCSnapshotHashes
            updatedLastCurrencySnapshots = lastSnapshotContext.lastCurrencySnapshots ++ currencySnapshots

            activeAllowSpendsFromCurrencySnapshots = currencySnapshots
              .mapFilter(_.toOption.flatMap { case (_, info) => info.activeAllowSpends })

            globalAllowSpends = acceptedGlobalAllowSpends
              .groupBy(_.value.source)
              .view
              .mapValues(SortedSet.from(_))
              .to(SortedMap)

            globalTokenLocks = acceptedGlobalTokenLocks
              .groupBy(_.value.source)
              .view
              .mapValues(SortedSet.from(_))
              .to(SortedMap)

            allAcceptedSpendTxns = acceptedSpendActions.toSortedMap.values.flatten
              .flatMap(spendAction => spendAction.spendTransactions.toList)
              .toList

            // Same materialized view as `lastActiveAllowSpends` above — `lastSnapshotContext` is immutable,
            // so a single MPT read covers both consumers (validateArtifacts and acceptAllowSpends).
            globalActiveAllowSpends = lastActiveAllowSpends
            globalActiveTokenLocks <- tokenLockStateManager.materializeActiveTokenLocksFromMpt

            // Build the hash-keyed lookup from the MPT — same source as `acceptReplacementTokenLocks` so the
            // two reads can't disagree. Scoped to the addresses actually involved in this acceptance round
            // (replacement TX sources + expired-withdrawal stakers); avoids a full address-set scan.
            // Fixes the chain-sync replay stall observed on gl0-7 where MPT and GSI views of activeTokenLocks
            // diverged and `generateTokenUnlocks` failed lookups that `acceptReplacementTokenLocks` had passed.
            tokenLockLookupAddresses = acceptedGlobalTokenLocks.map(_.value.source).toSet ++
              initialData.existingStakes.expired.keySet
            globalActiveTokenLocksByRef <- tokenLockStateManager.buildActiveTokenLocksByRefFromMpt(
              tokenLockLookupAddresses
            )

            globalLastAllowSpendRefs <- allowSpendStateManager.materializeLastAllowSpendRefsFromMpt
            globalLastTokenLockRefs <- tokenLockStateManager.materializeLastTokenLockRefsFromMpt

            allowSpendAcceptanceResult <- allowSpendStateManager.acceptAllowSpends(
              epochProgress,
              previousEpochProgress,
              activeAllowSpendsFromCurrencySnapshots,
              globalAllowSpends,
              globalActiveAllowSpends,
              allAcceptedSpendTxns
            )
            updatedAllowSpends = allowSpendAcceptanceResult.fullState
            allowSpendsDeltas = allowSpendAcceptanceResult.deltas
            removedAllowSpendKeys = allowSpendAcceptanceResult.removedKeys
            allowSpendExpiryIndexDelta = allowSpendAcceptanceResult.expiryIndexDelta

            updatedAllowSpendRefs = allowSpendStateManager.acceptAllowSpendRefs(
              globalLastAllowSpendRefs,
              allowSpendBlockAcceptanceResult.contextUpdate.lastTxRefs
            )

            allowSpendBalancesResult <- allowSpendStateManager.updateGlobalBalancesByAllowSpends(
              epochProgress,
              previousEpochProgress,
              updatedBalancesByRewards,
              globalAllowSpends,
              globalActiveAllowSpends
            )
            (updatedBalancesByAllowSpends, updatedBalancesByAllowSpendsDeltas) <- Async[F].fromEither(
              allowSpendBalancesResult.leftMap(ex =>
                new RuntimeException(s"Balance arithmetic error updating balances by allow spends: $ex")
              )
            )

            unexpiredNodeCollateralsRaw <- nodeCollateralStateManager.acceptNodeCollaterals(
              lastSnapshotContext,
              epochProgress,
              previousEpochProgress,
              withdrawalTimeLimit
            )
            (unexpiredCreate, unexpiredWithdraw, _) = unexpiredNodeCollateralsRaw

            updatedCreateNodeCollaterals <- nodeCollateralStateManager.getUpdatedCreateNodeCollaterals(
              nodeCollateralAcceptanceResult,
              unexpiredCreate
            )

            updatedWithdrawNodeCollaterals <- nodeCollateralStateManager.getUpdatedWithdrawNodeCollaterals(
              nodeCollateralAcceptanceResult,
              unexpiredWithdraw,
              lastSnapshotContext
            )

            generatedTokenUnlocks <- tokenLockStateManager
              .generateTokenUnlocks(
                initialData.existingStakes.expired,
                acceptedGlobalTokenLocks,
                globalActiveTokenLocksByRef
              )
              .leftMap(error => new RuntimeException(s"Error generating token unlocks: $error"))
              .liftTo[F]

            tokenLockAcceptanceResult <- tokenLockStateManager.acceptTokenLocks(
              epochProgress,
              previousEpochProgress,
              globalTokenLocks,
              globalActiveTokenLocks,
              generatedTokenUnlocks
            )
            updatedGlobalTokenLocks = tokenLockAcceptanceResult.fullState
            tokenLocksDeltas = tokenLockAcceptanceResult.deltas
            removedTokenLockKeys = tokenLockAcceptanceResult.removedKeys
            tokenLockExpiryIndexDelta = tokenLockAcceptanceResult.expiryIndexDelta

            updatedTokenLockRefs = tokenLockStateManager.acceptTokenLockRefs(
              globalLastTokenLockRefs,
              tokenLockBlockAcceptanceResult.contextUpdate.lastTokenLocksRefs
            )

            priorTokenLockBalances <- tokenLockStateManager.materializeTokenLockBalancesFromMpt
            TokenLockBalanceResult(updatedTokenLockBalances, tokenLockBalancesDeltas, removedTokenLockBalanceKeys) = tokenLockStateManager
              .updateTokenLockBalances(
                currencySnapshots,
                priorTokenLockBalances.some
              )

            tokenLockBalancesResult <- tokenLockStateManager.updateGlobalBalancesByTokenLocks(
              epochProgress,
              previousEpochProgress,
              updatedBalancesByAllowSpends,
              globalTokenLocks,
              generatedTokenUnlocks
            )
            (updatedBalancesByTokenLocks, updatedBalancesByTokenLocksDeltas) <- Async[F].fromEither(
              tokenLockBalancesResult.leftMap(err =>
                new RuntimeException(s"Balance arithmetic error updating balances by token locks: $err")
              )
            )

            lastActiveGlobalAllowSpends = globalActiveAllowSpends.getOrElse(None, SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])

            combined = (globalAllowSpends |+| lastActiveGlobalAllowSpends).toList
            allGlobalAllowSpends <-
              if (combined.isEmpty) {
                Async[F].pure(SortedMap.empty[Address, List[Hashed[AllowSpend]]])
              } else {
                Stream
                  .emits(combined)
                  .covary[F]
                  .chunkN(50)
                  .parEvalMap(10) { chunk =>
                    Async[F].cede *> chunk.toList.traverse {
                      case (address, allowSpends) =>
                        val allowSpendsList = allowSpends.toList
                        if (allowSpendsList.isEmpty) {
                          Async[F].pure((address, List.empty[Hashed[AllowSpend]]))
                        } else {
                          Stream
                            .emits(allowSpendsList)
                            .covary[F]
                            .chunkN(20)
                            .parEvalMap(5) { innerChunk =>
                              Async[F].cede *> innerChunk.toList.traverse { (allowSpend: Signed[AllowSpend]) =>
                                allowSpend.toHashed: F[Hashed[AllowSpend]]
                              } <* Async[F].cede
                            }
                            .compile
                            .toList
                            .map { (lists: List[List[Hashed[AllowSpend]]]) =>
                              (address, lists.flatten)
                            }
                        }
                    }: F[List[(Address, List[Hashed[AllowSpend]])]]
                  }
                  .compile
                  .toList
                  .flatMap { (lists: List[List[(Address, List[Hashed[AllowSpend]])]]) =>
                    Async[F].cede.as(lists.flatten.toSortedMap)
                  }
              }

            globalSpendTransactions = acceptedSpendActions.toSortedMap.flatMap {
              case (_, spendActions) =>
                spendActions
                  .flatMap(_.spendTransactions.toList)
                  .filter(_.currencyId.isEmpty)
            }.toList

            spendTxBalancesResult <- spendTransactionBalanceManager.updateGlobalBalancesBySpendTransactions(
              updatedBalancesByTokenLocks,
              allGlobalAllowSpends,
              globalSpendTransactions
            )
            (updatedBalancesBySpendTransactions, updatedBalancesBySpendTransactionsDeltas) <- Async[F].fromEither(
              spendTxBalancesResult.leftMap(err =>
                new RuntimeException(s"Balance arithmetic error updating balances by spend transactions: $err")
              )
            )

            MerkleTreeResult(_, updatedLastCurrencySnapshotProofs) <- buildMerkleTreeAndProofs(
              ordinal,
              updatedLastCurrencySnapshots
            )

            // Clean state maps and compute removed keys in a single pass.
            // For removed-key computation, only the prior non-empty keysets are needed; the manager materialize
            // helpers prefix-scan MPT and recover Address keys from each record's `event.value.source`.
            priorDelegatedStakeKeys <- delegatedStakeStateManager.materializeActiveDelegatedStakeAddressesFromMpt
            priorDelegatedStakeWithdrawalKeys <- delegatedStakeStateManager.materializeDelegatedStakeWithdrawalAddressesFromMpt
            priorNodeCollateralKeys <- nodeCollateralStateManager.materializeActiveNodeCollateralAddressesFromMpt
            priorNodeCollateralWithdrawalKeys <- nodeCollateralStateManager.materializeNodeCollateralWithdrawalAddressesFromMpt
            cleanedMapsResult = cleanStateMaps(
              updatedAllowSpends,
              updatedTokenLockBalances,
              updatedGlobalTokenLocks,
              updatedCreateDelegatedStakes,
              updatedWithdrawDelegatedStakes,
              updatedCreateNodeCollaterals,
              updatedWithdrawNodeCollaterals,
              priorDelegatedStakeKeys,
              priorDelegatedStakeWithdrawalKeys,
              priorNodeCollateralKeys,
              priorNodeCollateralWithdrawalKeys
            )

            updatedAllowSpendsCleaned = cleanedMapsResult.cleanedAllowSpends
            updatedTokenLockBalancesCleaned = cleanedMapsResult.cleanedTokenLockBalances
            updatedGlobalTokenLocksCleaned = cleanedMapsResult.cleanedGlobalTokenLocks
            updatedCreateDelegatedStakesCleaned = cleanedMapsResult.cleanedCreateDelegatedStakes
            updatedWithdrawDelegatedStakesCleaned = cleanedMapsResult.cleanedWithdrawDelegatedStakes
            updatedCreateNodeCollateralsCleaned = cleanedMapsResult.cleanedCreateNodeCollaterals
            updatedWithdrawNodeCollateralsCleaned = cleanedMapsResult.cleanedWithdrawNodeCollaterals
            removedDelegatedStakeKeys = cleanedMapsResult.removedDelegatedStakeKeys
            removedDelegatedStakeWithdrawalKeys = cleanedMapsResult.removedDelegatedStakeWithdrawalKeys
            removedNodeCollateralKeys = cleanedMapsResult.removedNodeCollateralKeys
            removedNodeCollateralWithdrawalKeys = cleanedMapsResult.removedNodeCollateralWithdrawalKeys

            priorPriceState <- priceStateUpdater.materializePriceStateFromMpt
            priceStateDeltas <- priceStateUpdater.updatePriceState(
              priorPriceState,
              acceptedPricingUpdates,
              epochProgress
            )
            updatedPriceState = priorPriceState ++ priceStateDeltas

            MetagraphSyncAcceptanceResult(updatedAcceptedMetagraphSyncData, metagraphSyncDataDeltas) <- metagraphSyncManager
              .acceptMetagraphSyncData(
                lastSnapshotContext,
                incomingCurrencySnapshots,
                globalSnapshotsProcessed,
                acceptedSpendActions,
                ordinal,
                epochProgress
              )

            gsi = buildGlobalSnapshotInfo(
              ordinal,
              tessellation3MigrationStartingOrdinal,
              tessellation301MigrationStartingOrdinal,
              metagraphSyncDataStartingOrdinal,
              lastSnapshotContext,
              initialData.blockResult,
              updatedLastStateChannelSnapshotHashes,
              (priorLastTxRefs ++ transactionsRefsDeltas).toSortedMap,
              priorBalances ++ updatedBalancesBySpendTransactions,
              updatedLastCurrencySnapshots,
              updatedLastCurrencySnapshotProofs,
              updatedAllowSpendsCleaned,
              updatedGlobalTokenLocksCleaned,
              updatedTokenLockBalancesCleaned,
              updatedAllowSpendRefs,
              updatedTokenLockRefs,
              updatedUpdateNodeParameters,
              updatedCreateDelegatedStakesCleaned,
              updatedWithdrawDelegatedStakesCleaned,
              updatedCreateNodeCollateralsCleaned,
              updatedWithdrawNodeCollateralsCleaned,
              updatedPriceState,
              updatedAcceptedMetagraphSyncData
            )

            balanceChanges: SortedMap[Address, Balance] =
              initialData.blockResult.contextUpdate.balances.toSortedMap ++
                currencyAcceptanceBalanceUpdate.toSortedMap ++
                rewardBalancesDelta ++
                updatedBalancesByAllowSpendsDeltas ++
                updatedBalancesByTokenLocksDeltas ++
                updatedBalancesBySpendTransactionsDeltas

            currencySnapshotsDeltas = incomingCurrencySnapshots.collect {
              case (address, snapshots) if snapshots.nonEmpty => address -> snapshots.last
            }

            updateNodeParametersDelta = initialData.nodeParamsResult.view
              .mapValues(unp => (unp, ordinal))
              .to(SortedMap)

            nodeCollateralWithdrawalExpiryIndexDelta <-
              if (maintainNodeCollateralWithdrawalExpiryIndex)
                nodeCollateralStateManager.materializeNodeCollateralWithdrawalsFromMpt.flatMap { priorWithdrawals =>
                  computeNodeCollateralWithdrawalExpiryIndexDelta(
                    priorWithdrawals,
                    updatedWithdrawNodeCollateralsCleaned,
                    withdrawalTimeLimit
                  )
                }
              else
                SystemIndexDelta.empty[NodeCollateralWithdrawalExpiryKey].pure[F]

            stateChangesAccumulator = StateChangesAccumulator(
              lastStateChannelSnapshotHashes = sCSnapshotHashes.toSortedMap,
              lastTxRefs = transactionsRefsDeltas,
              balances = balanceChanges,
              lastCurrencySnapshots = currencySnapshotsDeltas,
              // Sync ALL proofs, not just deltas: when ANY currency snapshot changes,
              // the Merkle tree changes and ALL proof paths are recomputed.
              // Previously only delta proofs were synced, leaving stale proof bytes
              // in the MPT for unchanged metagraphs, causing StateProof mismatches.
              lastCurrencySnapshotsProofs = updatedLastCurrencySnapshotProofs,
              activeAllowSpends = allowSpendsDeltas,
              activeTokenLocks = tokenLocksDeltas,
              tokenLockBalances = tokenLockBalancesDeltas,
              lastAllowSpendRefs = allowSpendBlockAcceptanceResult.contextUpdate.lastTxRefs.toSortedMap,
              lastTokenLockRefs = tokenLockBlockAcceptanceResult.contextUpdate.lastTokenLocksRefs.toSortedMap,
              activeDelegatedStakes = updatedCreateDelegatedStakesCleaned,
              delegatedStakesWithdrawals = updatedWithdrawDelegatedStakesCleaned,
              activeNodeCollaterals = updatedCreateNodeCollateralsCleaned,
              nodeCollateralWithdrawals = updatedWithdrawNodeCollateralsCleaned,
              metagraphSyncData = metagraphSyncDataDeltas,
              updateNodeParameters = updateNodeParametersDelta,
              priceState = priceStateDeltas,
              allowSpendExpiryIndex = allowSpendExpiryIndexDelta,
              tokenLockExpiryIndex = tokenLockExpiryIndexDelta,
              nodeCollateralWithdrawalExpiryIndex = nodeCollateralWithdrawalExpiryIndexDelta,
              removedAllowSpendKeys = removedAllowSpendKeys,
              removedTokenLockKeys = removedTokenLockKeys,
              removedTokenLockBalanceKeys = removedTokenLockBalanceKeys,
              removedDelegatedStakeKeys = removedDelegatedStakeKeys,
              removedDelegatedStakeWithdrawalKeys = removedDelegatedStakeWithdrawalKeys,
              removedNodeCollateralKeys = removedNodeCollateralKeys,
              removedNodeCollateralWithdrawalKeys = removedNodeCollateralWithdrawalKeys
            )

            _ <- loggerBundle.app.info(
              s"[ACCEPTANCE] ordinal=$ordinal MPT_SYNC: " +
                s"balanceChanges=${balanceChanges.size} " +
                s"scHashChanges=${sCSnapshotHashes.size} " +
                s"txRefChanges=${transactionsRefsDeltas.size} " +
                s"currSnapshotChanges=${currencySnapshotsDeltas.size} " +
                s"delegStakeAddrs=${updatedCreateDelegatedStakesCleaned.size} " +
                s"delegWithdrawAddrs=${updatedWithdrawDelegatedStakesCleaned.size} " +
                s"nodeCollAddrs=${updatedCreateNodeCollateralsCleaned.size} " +
                s"nodeCollWithdrawAddrs=${updatedWithdrawNodeCollateralsCleaned.size} " +
                s"removedKeys(ds=${removedDelegatedStakeKeys.size},dsw=${removedDelegatedStakeWithdrawalKeys.size}," +
                s"nc=${removedNodeCollateralKeys.size},ncw=${removedNodeCollateralWithdrawalKeys.size})"
            )

            // Temporary instrumentation for ml0/gl0 mptRoot divergence (task #18). Per-field content
            // fingerprints of the accumulator deltas; identical fingerprints between two nodes mean
            // identical input to the MPT writer for that field. Compare gl0-0 vs ml0-0 logs at the
            // first diverging ordinal to localize which field carries the gl0/ml0 disagreement.
            _ <- loggerBundle.app.info(
              s"[ACCEPTANCE] ordinal=$ordinal MPT_SYNC_FP: " +
                s"balances=[${stateChangesAccumulator.balances.size},${"%08x".format(stateChangesAccumulator.balances.toString.hashCode)}] " +
                s"scHashes=[${stateChangesAccumulator.lastStateChannelSnapshotHashes.size},${"%08x"
                    .format(stateChangesAccumulator.lastStateChannelSnapshotHashes.toString.hashCode)}] " +
                s"txRefs=[${stateChangesAccumulator.lastTxRefs.size},${"%08x".format(stateChangesAccumulator.lastTxRefs.toString.hashCode)}] " +
                s"currSnapshots=[${stateChangesAccumulator.lastCurrencySnapshots.size},${"%08x"
                    .format(stateChangesAccumulator.lastCurrencySnapshots.toString.hashCode)}] " +
                s"currProofs=[${stateChangesAccumulator.lastCurrencySnapshotsProofs.size},${"%08x"
                    .format(stateChangesAccumulator.lastCurrencySnapshotsProofs.toString.hashCode)}] " +
                s"allowSpends=[${stateChangesAccumulator.activeAllowSpends.size},${"%08x"
                    .format(stateChangesAccumulator.activeAllowSpends.toString.hashCode)}] " +
                s"tokenLocks=[${stateChangesAccumulator.activeTokenLocks.size},${"%08x"
                    .format(stateChangesAccumulator.activeTokenLocks.toString.hashCode)}] " +
                s"tokenLockBal=[${stateChangesAccumulator.tokenLockBalances.size},${"%08x"
                    .format(stateChangesAccumulator.tokenLockBalances.toString.hashCode)}] " +
                s"asRefs=[${stateChangesAccumulator.lastAllowSpendRefs.size},${"%08x"
                    .format(stateChangesAccumulator.lastAllowSpendRefs.toString.hashCode)}] " +
                s"tlRefs=[${stateChangesAccumulator.lastTokenLockRefs.size},${"%08x"
                    .format(stateChangesAccumulator.lastTokenLockRefs.toString.hashCode)}] " +
                s"delegStakes=[${stateChangesAccumulator.activeDelegatedStakes.size},${"%08x"
                    .format(stateChangesAccumulator.activeDelegatedStakes.toString.hashCode)}] " +
                s"delegWithdrawals=[${stateChangesAccumulator.delegatedStakesWithdrawals.size},${"%08x"
                    .format(stateChangesAccumulator.delegatedStakesWithdrawals.toString.hashCode)}] " +
                s"nodeColl=[${stateChangesAccumulator.activeNodeCollaterals.size},${"%08x"
                    .format(stateChangesAccumulator.activeNodeCollaterals.toString.hashCode)}] " +
                s"nodeCollWithdrawals=[${stateChangesAccumulator.nodeCollateralWithdrawals.size},${"%08x"
                    .format(stateChangesAccumulator.nodeCollateralWithdrawals.toString.hashCode)}] " +
                s"metagraphSync=[${stateChangesAccumulator.metagraphSyncData.size},${"%08x"
                    .format(stateChangesAccumulator.metagraphSyncData.toString.hashCode)}] " +
                s"updateNodeParams=[${stateChangesAccumulator.updateNodeParameters.size},${"%08x"
                    .format(stateChangesAccumulator.updateNodeParameters.toString.hashCode)}] " +
                s"priceState=[${stateChangesAccumulator.priceState.size},${"%08x"
                    .format(stateChangesAccumulator.priceState.toString.hashCode)}] " +
                s"asExpiry=${"%08x".format(stateChangesAccumulator.allowSpendExpiryIndex.toString.hashCode)} " +
                s"tlExpiry=${"%08x".format(stateChangesAccumulator.tokenLockExpiryIndex.toString.hashCode)} " +
                s"ncwExpiry=${"%08x".format(stateChangesAccumulator.nodeCollateralWithdrawalExpiryIndex.toString.hashCode)} " +
                s"removed(as=${stateChangesAccumulator.removedAllowSpendKeys.size}/${"%08x"
                    .format(stateChangesAccumulator.removedAllowSpendKeys.toString.hashCode)}," +
                s"tl=${stateChangesAccumulator.removedTokenLockKeys.size}/${"%08x"
                    .format(stateChangesAccumulator.removedTokenLockKeys.toString.hashCode)}," +
                s"tlb=${stateChangesAccumulator.removedTokenLockBalanceKeys.size}/${"%08x"
                    .format(stateChangesAccumulator.removedTokenLockBalanceKeys.toString.hashCode)}," +
                s"ds=${stateChangesAccumulator.removedDelegatedStakeKeys.size}/${"%08x"
                    .format(stateChangesAccumulator.removedDelegatedStakeKeys.toString.hashCode)}," +
                s"dsw=${stateChangesAccumulator.removedDelegatedStakeWithdrawalKeys.size}/${"%08x"
                    .format(stateChangesAccumulator.removedDelegatedStakeWithdrawalKeys.toString.hashCode)}," +
                s"nc=${stateChangesAccumulator.removedNodeCollateralKeys.size}/${"%08x"
                    .format(stateChangesAccumulator.removedNodeCollateralKeys.toString.hashCode)}," +
                s"ncw=${stateChangesAccumulator.removedNodeCollateralWithdrawalKeys.size}/${"%08x"
                    .format(stateChangesAccumulator.removedNodeCollateralWithdrawalKeys.toString.hashCode)})"
            )

            // === MPT Sync with undo journal ===
            // Before applying deltas, detect fork switches: if the journal tip doesn't
            // match ordinal-1 (the parent), the MPT has state from a different fork.
            // Reset MPT to the parent state first, then apply deltas incrementally.
            _ <- undoJournal match {
              case Some(journal) =>
                journal.currentTipOrdinal.flatMap {
                  case Some(tipOrd) if tipOrd != ordinal.value.value - 1 =>
                    // Fork switch: journal tip at ordinal $tipOrd but parent should be ordinal-1.
                    // Roll back journal to clear stale fork entries, then reset MPT to parent state.
                    loggerBundle.app.info(
                      s"[ACCEPTANCE] ordinal=$ordinal fork switch detected: journalTip=$tipOrd, expected=${ordinal.value.value - 1}. " +
                        s"Resetting MPT to parent state before applying deltas."
                    ) >>
                      journal.unapplyTo(0) >>
                      mptStore.syncFromGlobalSnapshotInfo(lastSnapshotContext, SnapshotOrdinal.unsafeApply(ordinal.value.value - 1))
                  case _ =>
                    Async[F].unit // aligned, no rollback needed
                }
              case None =>
                Async[F].unit
            }
            // Snapshot the pre-sync MPT bytes. Used below for independent cross-check: apply the
            // accumulator's delta to this map via the `toAccumulatorHexDelta` helper (a separate
            // encoder+merge path from `syncFromStateChanges`) and build an MPT via the Parallel
            // producer. If both paths agree on the post-state root, the writer is validated
            // without needing `GlobalSnapshotInfo` as an intermediate.
            preSyncBytes <- mptStore.allEntriesAsBytes
            // Temporary instrumentation (task #18): fingerprint of starting MPT bytes. Combined with
            // MPT_SYNC_FP above, gives us the two writer inputs (prev state + delta) for gl0/ml0 diff.
            // Map[Hex, Array[Byte]] needs sort + content-aware hash since Map order is non-deterministic
            // and Array.toString is identity-based.
            _ <- loggerBundle.app.info {
              val sorted = preSyncBytes.toSeq.sortBy(_._1.toString)
              val fp = sorted.map { case (k, v) => (k.toString, java.util.Arrays.hashCode(v)) }.toString.hashCode
              s"[ACCEPTANCE] ordinal=$ordinal MPT_SYNC_PRE: " +
                s"entries=${preSyncBytes.size} hash=${"%08x".format(fp)}"
            }
            syncAction = mptStore.syncFromStateChanges(stateChangesAccumulator, ordinal)
            _ <- undoJournal match {
              case Some(journal) =>
                journal.wrapApply(
                  ordinal.value.value,
                  io.constellationnetwork.security.hash.Hash(ordinal.value.value.toString),
                  io.constellationnetwork.security.hash.Hash.empty
                )(syncAction)
              case None =>
                syncAction
            }
            // Temporary instrumentation (task #18): fingerprint the END gsi components used by
            // builder.buildProof. If MPT_SYNC_PRE + MPT_SYNC_FP match across nodes but mptRoot
            // still diverges, the difference must be here.
            _ <- loggerBundle.app.info(
              s"[ACCEPTANCE] ordinal=$ordinal GSI_END_FP: " +
                s"balances=[${gsi.balances.size},${"%08x".format(gsi.balances.toString.hashCode)}] " +
                s"scHashes=[${gsi.lastStateChannelSnapshotHashes.size},${"%08x"
                    .format(gsi.lastStateChannelSnapshotHashes.toString.hashCode)}] " +
                s"txRefs=[${gsi.lastTxRefs.size},${"%08x".format(gsi.lastTxRefs.toString.hashCode)}] " +
                s"currSnapshots=[${gsi.lastCurrencySnapshots.size},${"%08x".format(gsi.lastCurrencySnapshots.toString.hashCode)}] " +
                s"currProofs=[${gsi.lastCurrencySnapshotsProofs.size},${"%08x"
                    .format(gsi.lastCurrencySnapshotsProofs.toString.hashCode)}] " +
                s"allowSpends=[${gsi.activeAllowSpends.fold(0)(_.size)},${"%08x".format(gsi.activeAllowSpends.toString.hashCode)}] " +
                s"tokenLocks=[${gsi.activeTokenLocks.fold(0)(_.size)},${"%08x".format(gsi.activeTokenLocks.toString.hashCode)}] " +
                s"tokenLockBal=[${gsi.tokenLockBalances.fold(0)(_.size)},${"%08x".format(gsi.tokenLockBalances.toString.hashCode)}] " +
                s"asRefs=[${gsi.lastAllowSpendRefs.fold(0)(_.size)},${"%08x".format(gsi.lastAllowSpendRefs.toString.hashCode)}] " +
                s"tlRefs=[${gsi.lastTokenLockRefs.fold(0)(_.size)},${"%08x".format(gsi.lastTokenLockRefs.toString.hashCode)}] " +
                s"delegStakes=[${gsi.activeDelegatedStakes.fold(0)(_.size)},${"%08x".format(gsi.activeDelegatedStakes.toString.hashCode)}] " +
                s"delegWithdrawals=[${gsi.delegatedStakesWithdrawals.fold(0)(_.size)},${"%08x"
                    .format(gsi.delegatedStakesWithdrawals.toString.hashCode)}] " +
                s"nodeColl=[${gsi.activeNodeCollaterals.fold(0)(_.size)},${"%08x".format(gsi.activeNodeCollaterals.toString.hashCode)}] " +
                s"nodeCollWithdrawals=[${gsi.nodeCollateralWithdrawals.fold(0)(_.size)},${"%08x"
                    .format(gsi.nodeCollateralWithdrawals.toString.hashCode)}] " +
                s"metagraphSync=[${gsi.metagraphSyncData.fold(0)(_.size)},${"%08x".format(gsi.metagraphSyncData.toString.hashCode)}] " +
                s"updateNodeParams=[${gsi.updateNodeParameters.fold(0)(_.size)},${"%08x".format(gsi.updateNodeParameters.toString.hashCode)}] " +
                s"priceState=[${gsi.priceState.fold(0)(_.size)},${"%08x".format(gsi.priceState.toString.hashCode)}]"
            )
            incrementalProof <- builder.buildProof(gsi, ordinal)

            // Verify incremental (FileSystem producer incremental-insert) against independent
            // replay (Parallel producer batch build over `prev ⊖ removes ⊕ upserts`). Only
            // applies when MPT is the active proof format; LegacyFormat proofs carry no mptRoot.
            isMptFormat = globalStateProofSelector.select(ordinal) == MerklePatriciaFormat
            incrementalRoot = incrementalProof.mptRoot.map(_.show).getOrElse("none")

            stateProof <-
              if (!isMptFormat) {
                loggerBundle.app
                  .info(
                    s"[ACCEPTANCE] ordinal=$ordinal stateProof: format=LEGACY (MPT check skipped) " +
                      s"gsi.balances=${gsi.balances.size} gsi.currSnapshots=${gsi.lastCurrencySnapshots.size}"
                  )
                  .as(incrementalProof)
              } else {
                for {
                  // Independent byte derivation: take pre-sync bytes, apply the accumulator's
                  // upserts + removes via `toAccumulatorHexDelta` (scodec per-field encoding,
                  // no `GlobalSnapshotInfo` involved). This is the MPT-as-primary verify path.
                  deltaPair <- io.constellationnetwork.schema.mpt.GlobalStateConverter
                    .toAccumulatorHexDelta[F](stateChangesAccumulator, preSyncBytes)
                  (deltaUpserts, deltaRemoves) = deltaPair
                  expectedBytes = (preSyncBytes -- deltaRemoves) ++ deltaUpserts
                  verifyTrie <- io.constellationnetwork.security.mpt.MerklePatriciaTrie
                    .makeParallelFromBytes[F](expectedBytes)
                  verifyRoot = verifyTrie.rootHash.value.show
                  result <-
                    if (incrementalRoot == verifyRoot) {
                      loggerBundle.app
                        .info(
                          s"[ACCEPTANCE] ordinal=$ordinal stateProof: mptRoot=${incrementalRoot.take(12)} " +
                            s"mptConsistency=MATCH " +
                            s"gsi.balances=${gsi.balances.size} gsi.currSnapshots=${gsi.lastCurrencySnapshots.size}"
                        )
                        .as(incrementalProof)
                    } else {
                      // Writer divergence — `syncFromStateChanges` produced bytes that don't
                      // match the expected `prev ⊖ removes ⊕ upserts` replay. This is a writer
                      // bug; fail acceptance so it surfaces in tests instead of silently healing.
                      loggerBundle.app.error(
                        s"[ACCEPTANCE] ordinal=$ordinal stateProof: mptConsistency=DIVERGED " +
                          s"incremental=${incrementalRoot.take(12)} replay=${verifyRoot.take(12)} " +
                          s"entries=${expectedBytes.size} " +
                          s"deltaUpserts=${deltaUpserts.size} deltaRemoves=${deltaRemoves.size} " +
                          s"gsi.balances=${gsi.balances.size} gsi.currSnapshots=${gsi.lastCurrencySnapshots.size}"
                      ) >>
                        Async[F].raiseError[GlobalSnapshotStateProof](
                          new RuntimeException(
                            s"MPT writer divergence at ordinal $ordinal: incremental=$incrementalRoot replay=$verifyRoot"
                          )
                        )
                    }
                } yield result
              }

            expiredAllowSpends = allowSpendStateManager.filterExpiredAllowSpends(
              lastActiveGlobalAllowSpends,
              epochProgress
            )
            expiredTokenLocks <- tokenLockStateManager.findExpiredGlobalTokenLocksViaIndexFromMpt(
              previousEpochProgress,
              epochProgress
            )

            artifactsFromExpired <- artifactEmissionManager.emitAllExpiredArtifacts(
              expiredAllowSpends,
              expiredTokenLocks
            )

            allowSpendsExpiredEvents = artifactsFromExpired.collect { case a: AllowSpendExpiration => a }
            tokenUnlocksEvents = artifactsFromExpired.collect { case t: TokenUnlock => t }

            generatedTokenUnlockArtifacts = SortedSet.from[SharedArtifact](
              generatedTokenUnlocks.view.values.flatten
                .filterNot(x =>
                  tokenUnlocksEvents.exists {
                    case t: TokenUnlock => t.tokenLockRef == x.tokenLockRef
                    case _              => false
                  }
                )
            )
          } yield
            (
              initialData.blockResult,
              allowSpendBlockAcceptanceResult,
              tokenLockBlockAcceptanceResult,
              initialData.delegatedResult,
              nodeCollateralAcceptanceResult,
              scSnapshots,
              returnedSCEvents,
              acceptedRewardTxs,
              gsi,
              stateProof,
              acceptedSpendActions,
              updatedUpdateNodeParameters.view.mapValues(_._1).toSortedMap,
              (allowSpendsExpiredEvents ++ tokenUnlocksEvents ++ generatedTokenUnlockArtifacts).toSortedSet,
              delegatorRewardsMap
            )
        }
      }
    }
  }
}
