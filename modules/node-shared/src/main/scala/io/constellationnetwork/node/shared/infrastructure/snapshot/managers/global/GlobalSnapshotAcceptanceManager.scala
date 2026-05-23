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
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.delegatedStake.{
  UpdateDelegatedStakeAcceptanceManager,
  UpdateDelegatedStakeAcceptanceResult
}
import io.constellationnetwork.node.shared.domain.nakamoto.overlay._
import io.constellationnetwork.node.shared.domain.nakamoto.{NodeStakeAggregator, ShardAssignment}
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
import io.constellationnetwork.node.shared.infrastructure.local_events.LocalEventsPublisher
import io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events.{
  AllowSpendStateChange => PbAllowSpendStateChange,
  BalanceChange => PbBalanceChange,
  MetagraphBalanceChange => PbMetagraphBalanceChange,
  MetagraphSnapshotAccepted => PbMetagraphSnapshotAccepted,
  TokenLockStateChange => PbTokenLockStateChange,
  TransactionAccepted => PbTransactionAccepted
}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
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
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, HistoricalStakeSnapshot}
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal, UpdateNodeCollateral}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.priceOracle.{PriceRecord, TokenPair}
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId}
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.producer.StatefulMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.CompatCodecs._
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs.currencySnapshotInfoImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutable}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
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
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    parentTip: BranchId,
    // Slice 13 (hierarchical-shard-checkpoints v1, §13 row 13). When non-empty AND the manager is wired with a
    // `shardCheckpointAcceptanceManager` AND `shardingConfig.numShards > 1`, the new shard-checkpoint admission
    // path runs: each checkpoint is verified, its `derivedStateDelta.includedSnapshots` are folded into the SC
    // event stream so the standard `processStateChannelEvents` consumes the shard-committee-attested per-MG
    // binaries, and `emittedReceipts` are drained into [[MetagraphSyncManager.consumeReceipts]] for the cross-MG
    // sync-data write effect (§8.4). Default `SortedMap.empty` preserves byte-identical behavior at
    // `numShards = 1` (today's production default) — the new branch never fires until shard wiring is enabled at
    // the gl0 consensus layer (a later slice).
    shardCheckpoints: SortedMap[ShardId, ShardCheckpoint] = SortedMap.empty
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
      SortedMap[PeerId, Map[Address, Amount]],
      // BranchHandle returned uncommitted (#56.10 Phase J). Caller hashes the resulting artifact
      // and runs `overlay.commit(handle, BranchId(snapshotHash), ordinal)` once the snapshot is
      // sealed — that's the only point at which `childTip` (the just-produced snapshot's hash)
      // is known. Committing inside `accept()` with `parentTip` as childTip would self-loop
      // pendingRef under MultiBranch.
      BranchHandle[F, GlobalStateKey]
    )
  ]
}

object GlobalSnapshotAcceptanceManager {

  private case object InvalidMerkleTree extends NoStackTrace

  def make[F[_]: Async: Parallel: HasherSelector: SecurityProvider: JsonSerializer: Metrics](
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
    overlay: MptOverlay[F, GlobalStateKey],
    loggerBundle: LoggerBundle[F],
    // When true, accept() emits adds/removes for the node-collateral-withdrawal expiry index. Requires the same
    // `Some(withdrawalTimeLimit)` to also be passed to every `syncFromGlobalSnapshotInfo` / `toAllStateKeyValueBytes`
    // in the node's production paths — otherwise rebuild-path and delta-path mptRoots diverge. Default false until
    // that threading lands.
    maintainNodeCollateralWithdrawalExpiryIndex: Boolean = false,
    // §3 NIPoPoW S0.4: number of snapshots per eta-rotation period. At every boundary ordinal (`ord % R == R - 1`)
    // accept() captures `NodeStakeAggregator.snapshotFromMpt` (the §G2 MPT-primary path; byte-equivalent to the previous
    // `EpochStakeSnapshotter.snapshot(builtInfo)` GSI walk) into `historicalStakeSnapshots[currentPeriod]` and prunes
    // entries older than `currentPeriod - 3` (algorithm reads N-2; the extra slot is a reorg-grace). Must match the
    // producer's `NAKAMOTO_ETA_ROTATION_SNAPSHOTS` for cross-node determinism.
    etaRotationSnapshots: Long = 2550L,
    // Path 1 (heap-leak workstream): callback that returns eta_period for the just-closed eta-period at
    // the boundary write. Eta_period is determined at the 2/3-mark of period (period-1) and used by slot
    // leaders DURING period; by the time the boundary ordinal of period is reached, eta_period has been
    // "the" eta for the whole window. This boundary write captures that deterministic value alongside
    // stake_period so future reads (`EtaStateManager.getEta(period)`) hit the MPT cache instead of walking
    // the chainStore (which Fix B's k₁-bounded retention can have evicted). Defaults to `None` —
    // suitable for tests / pre-wire-up call sites where boundary writes can fall through to
    // `Hash.empty`; production overrides at `GlobalSnapshotConsensus` construction with a
    // chainStore-backed walk.
    etaForPeriod: Option[EtaPeriod => F[Hash]] = None,
    // [[LocalEventsPublisher]] gates emission of consensus events into the gl0-embedded gRPC stream
    // (`docs/nakamoto/LOCAL-EVENTS-SERVICE-DESIGN.md`). Production wiring at `GlobalSnapshotConsensus` /
    // `SharedServices` constructs a `Topic`-backed publisher when `nakamoto.local-events.enabled = true`;
    // currency-l0 and tests pass `LocalEventsPublisher.noop`. The Q2 user override (hoist the double-walk)
    // is implemented inside `accept()`: TokenLock/AllowSpend expired sets are computed once and feed both
    // the manager call sites and the publisher's `EXPIRED` emissions.
    //
    // `Option` rather than a defaulted value because Scala can't resolve the `Applicative[F]` instance for
    // `LocalEventsPublisher.noop[F]` at the def's default-parameter site (the context-bound implicits are
    // bound on the outer method, not on default-arg expressions). `None` collapses to `noop` inside the
    // body where the implicits are in scope.
    localEventsPublisher: Option[LocalEventsPublisher[F]] = None,
    // Slice 13 (hierarchical-shard-checkpoints v1, §13 row 13). Sharding admission dependencies. ALL three
    // default to `None` so existing call sites (`SharedServices.scala:234`, currency-l0, tests) keep their
    // current behavior: `numShards = 1` single-shard mode means the new branch never fires and `accept()` is
    // byte-identical to the pre-Slice-13 code path (the regression bar).
    //
    // Activation requires (1) `shardingConfig.numShards > 1`, (2) `shardCheckpointAcceptanceManager.isDefined`,
    // (3) a non-empty `accept(..., shardCheckpoints)` argument. Production wiring will populate all three at
    // `GlobalSnapshotConsensus` construction once a later slice surfaces the wired shard pipeline.
    //
    // `shardAssignment` is currently carried through for forward-compat / future expansion (e.g., a future
    // slice that uses gl0-side reverse-lookup of "is this MG in shard `s`" before applying a delta). At
    // Slice 13 it's referenced once below to keep the unused-warning silent.
    shardingConfig: Option[ShardingConfig] = None,
    shardCheckpointAcceptanceManager: Option[ShardCheckpointGl0AcceptanceManager[F]] = None,
    shardAssignment: Option[ShardAssignment[F]] = None
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector
  ): F[GlobalSnapshotAcceptanceManager[F]] = {
    // Reserved for forward-compat — see scaladoc on the parameter. Forces the `Option`-typed field to be
    // referenced exactly once so the Scala -Wunused warning doesn't fire when `None` is in play.
    val _unusedShardAssignment = shardAssignment
    val _ = _unusedShardAssignment
    val publisher: LocalEventsPublisher[F] = localEventsPublisher.getOrElse(LocalEventsPublisher.noop[F])
    // Establish the WithdrawalTimeLimit implicit from the explicit constructor param so that the rebuild paths
    // (`syncFromGlobalSnapshotInfo`, `stateProofBuilder` → `mptStateProof`) see the same limit the accept path uses
    // to compute expiry-index buckets. Gated by the feature flag until the threading below lands everywhere.
    implicit val withdrawalTimeLimitCtx: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
      if (maintainNodeCollateralWithdrawalExpiryIndex)
        io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.some(withdrawalTimeLimit)
      else
        io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none
    // `mptStore` is the underlying base — used by the LegacyFormat path's `builder.buildProof`
    // (which reads from the producer attached to the base store), and as the source-of-truth for
    // verify-replay's pre-state byte snapshot. Per-manager prior-state reads are routed through
    // a dynamic branch-aware reader (`branchTipRef` set at accept() top) so under MultiBranch a
    // child sees its parent branch's pending writes (#56.10 Phase J).
    val mptStore: MptStore[F, GlobalStateKey] = overlay.base
    // Slice 12: `MetagraphSyncManager.make` is now effectful (allocates internal `Ref`s for the cross-shard receipt consumer's
    // pending accumulator + seen-set). Threaded into the `mapN` alongside the existing `Ref.of`/`Semaphore.apply` allocations
    // so the construction stays a single `F` action with no nested `flatMap` ceremony.
    (
      cats.effect.Ref.of[F, BranchId](BranchId.base),
      cats.effect.std.Semaphore[F](1),
      MetagraphSyncManager.make[F](metagraphsSyncConfig)
    ).mapN { (branchTipRef, acceptMutex, metagraphSyncManager) =>
      val branchAwareReader = io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.dynamic[F](
        overlay,
        branchTipRef.get
      )
      val artifactEmissionManager = ArtifactEmissionManager.make[F]()
      val tipUsageManager = TipUsageManager.make[F]()
      val rewardAcceptanceManager = RewardAcceptanceManager.make[F](branchAwareReader)
      val allowSpendStateManager = AllowSpendStateManager.make[F](branchAwareReader)
      val tokenLockStateManager = TokenLockStateManager.make[F](branchAwareReader)
      val spendTransactionBalanceManager = SpendTransactionBalanceManager.make[F](branchAwareReader)
      val delegatedStakeStateManager = DelegatedStakeStateManager.make[F](branchAwareReader)
      val nodeCollateralStateManager = NodeCollateralStateManager.make[F](branchAwareReader)
      val transactionReferenceManager = TransactionReferenceManager.make[F](branchAwareReader)
      // §G2 — MPT-primary stake aggregator. Mirrors the per-state-manager pattern (each routed via the branch-aware reader so
      // boundary-write reads under MultiBranch see pending parent-branch writes). Used by `computeHistoricalStakeBoundaryDelta`
      // to materialize the §3 NIPoPoW S0.4 boundary `StakeDistribution` from MPT prefix-scans rather than walking the in-memory
      // `baseInfo.activeDelegatedStakes / activeNodeCollaterals` maps; the two paths are byte-equivalent when MPT and GSI are in
      // sync (which they are inside accept() since the GSI is built from the same accepted records the MPT writer then syncs).
      val stakeAggregator: NodeStakeAggregator[F] = NodeStakeAggregator.make[F](branchAwareReader)
      val blockAcceptanceCoordinatorManager = BlockAcceptanceCoordinatorManager.make[F](
        blockAcceptanceManager,
        allowSpendBlockAcceptanceManager,
        tokenLockBlockAcceptanceManager,
        tipUsageManager,
        collateral,
        branchAwareReader
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
          currentBalances: SortedMap[Address, Balance],
          priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
          priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
            CurrencySnapshot
          ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
          scEvents: List[StateChannelOutput],
          validationType: StateChannelValidationType,
          getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
        )(implicit hasher: Hasher[F]): F[StateChannelAcceptanceResult] =
          stateChannelEventsProcessor.process(
            ordinal,
            currentBalances,
            priorLastStateChannelSnapshotHashes,
            priorLastCurrencySnapshots,
            scEvents,
            validationType,
            getGlobalSnapshotByOrdinal
          )

        /** Slice 13 (hierarchical-shard-checkpoints v1, §13 row 13, §7 admission flow). Pre-process the per-shard checkpoint envelopes
          * supplied by the gl0 leader (`shardCheckpoints` parameter on `accept()`) before the standard SC-event pipeline runs.
          *
          * For each (shardId, checkpoint) tuple:
          *
          *   1. Run [[ShardCheckpointGl0AcceptanceManager.evaluate]] — pre-checks (signatures, committee membership, KES, VRF structural) +
          *      finality-trigger phase (`T_count_shard` fast-path / `T_depth1_shard` re-exec degraded-path). Result branches:
          *      - `Accepted`: harvest the checkpoint's contributions (per-MG SC binaries + cross-shard receipts).
          *      - `PendingMoreAttestations`: skip this checkpoint for this ord; the gl0 leader will retry next ord (§7.2 `gl0AnchorOrdinal`
          *        loose coupling permits a checkpoint to ride into N, N+1, …).
          *      - `Rejected` / `RejectedReExecutionMismatch`: log + drop. Slashing emission for the re-exec mismatch case (§10.2) is Slice
          *        16/17 territory; Slice 13 surfaces the rejection but does not yet emit evidence.
          *
          *   1. Convert each accepted shard checkpoint's `derivedStateDelta.includedSnapshots` into [[StateChannelOutput]] entries and
          *      append to the raw `scEvents` list. Rationale: the shard committee has already chain-link-validated the per-MG binaries;
          *      gl0's standard `processStateChannelEvents` will accept them through the same predicates the raw events go through, so
          *      double-application is impossible (the chain-link check inside the processor naturally deduplicates). At `numShards = 1`
          *      with empty `shardCheckpoints` this branch is a no-op — the regression bar.
          *
          *   1. Drain the union of `emittedReceipts` from accepted checkpoints into [[MetagraphSyncManager.consumeReceipts]]. The cross-MG
          *      `MetagraphSyncDataWrite` effect (§8.4) is folded into the manager's pending accumulator; the cumulative state is read later
          *      by `acceptMetagraphSyncData` (which sees the union of "raw scEvents-produced sync updates" AND "shard-committee-attested
          *      cross-shard receipts"). Idempotent per the consumer's seen-set gate.
          *
          * '''Returns''' the supplemented event list (existing `scEvents` ++ shard-derived `StateChannelOutput`s for accepted shards). The
          * cross-shard receipt drain is a side-effect on the [[MetagraphSyncManager]] instance — no value flows back through this method's
          * return type.
          *
          * '''Gating contract''': this method is only invoked when `shardingActive(shardCheckpoints)` returns `true`. The gate check lives
          * at the single call site below to keep the no-op fast-path obvious.
          */
        private def processShardCheckpoints(
          ordinal: SnapshotOrdinal,
          shardCheckpoints: SortedMap[ShardId, ShardCheckpoint],
          rawScEvents: List[StateChannelOutput],
          checkpointManager: ShardCheckpointGl0AcceptanceManager[F]
        )(implicit hasher: Hasher[F]): F[List[StateChannelOutput]] =
          shardCheckpoints.toList
            .foldM((List.empty[StateChannelOutput], List.empty[io.constellationnetwork.schema.sharding.CrossShardReceipt])) {
              case ((acceptedBinariesAcc, receiptsAcc), (shardId, cp)) =>
                checkpointManager.evaluate(cp).flatMap {
                  case ShardCheckpointAcceptResult.Accepted =>
                    val newBinaries: List[StateChannelOutput] =
                      cp.derivedStateDelta.includedSnapshots.toList.flatMap {
                        case (mgAddr, nel) => nel.toList.map(b => StateChannelOutput(mgAddr, b))
                      }
                    val newReceipts: List[io.constellationnetwork.schema.sharding.CrossShardReceipt] = cp.emittedReceipts
                    loggerBundle.app
                      .info(
                        s"[ACCEPTANCE/SHARDING] ordinal=$ordinal shardId=$shardId shardOrd=${cp.shardOrdinal.value} " +
                          s"ACCEPTED binaries=${newBinaries.size} receipts=${newReceipts.size}"
                      )
                      .as((acceptedBinariesAcc ++ newBinaries, receiptsAcc ++ newReceipts))

                  case ShardCheckpointAcceptResult.PendingMoreAttestations =>
                    loggerBundle.app
                      .info(
                        s"[ACCEPTANCE/SHARDING] ordinal=$ordinal shardId=$shardId shardOrd=${cp.shardOrdinal.value} " +
                          s"PENDING — will retry next gl0 ord"
                      )
                      .as((acceptedBinariesAcc, receiptsAcc))

                  case ShardCheckpointAcceptResult.Rejected(reason) =>
                    // Logged at info (not warn) because a Rejected checkpoint can be a legitimate operator-level transient
                    // (e.g., a shard not yet in the local finalityTriggers map during bootstrap). Slashing for malicious
                    // rejections is Slice 16/17 territory; Slice 13 surfaces the rejection but takes no slashing action.
                    loggerBundle.app
                      .info(
                        s"[ACCEPTANCE/SHARDING] ordinal=$ordinal shardId=$shardId shardOrd=${cp.shardOrdinal.value} " +
                          s"REJECTED reason=$reason"
                      )
                      .as((acceptedBinariesAcc, receiptsAcc))

                  case ShardCheckpointAcceptResult.RejectedReExecutionMismatch(reason, slashSigners) =>
                    // Wrong-derivation result. Slashing evidence emission lands in Slice 16/17 (signer list surfaced here
                    // for the future hook). Slice 13 logs the rejection at warn so operators can spot the deviation.
                    loggerBundle.app
                      .warn(
                        s"[ACCEPTANCE/SHARDING] ordinal=$ordinal shardId=$shardId shardOrd=${cp.shardOrdinal.value} " +
                          s"REJECTED-REEXEC-MISMATCH reason=$reason slashSigners=${slashSigners.size}"
                      )
                      .as((acceptedBinariesAcc, receiptsAcc))
                }
            }
            .flatMap {
              case (acceptedBinaries, receipts) =>
                // Side-effect: drain the union of cross-shard receipts into the shared MetagraphSyncManager accumulator.
                // The manager's internal seen-set gate makes the apply idempotent — duplicate receipts (operator replay,
                // gossip duplication, or repeat-evaluation in a re-acceptance turn) are silently dropped after first sight.
                metagraphSyncManager.consumeReceipts(receipts).as(rawScEvents ++ acceptedBinaries)
            }

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
          * removals (`addresses that had non-empty sets in previous state and are now empty/missing`) and tightens the dependency: the
          * caller need not hand over the entire prior partition — only the addresses that had records.
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

        /** §3 NIPoPoW S0.4 boundary computation, shared by `buildGlobalSnapshotInfo` (GSI field) and the `StateChangesAccumulator` (MPT
          * delta). Both must stay byte-identical: GSI consumers read the accepted snapshot directly, while the accumulator drives the MPT
          * writes that back the proof.
          *
          * Returns:
          *   - `adds`: the boundary-period entry, empty on non-boundary ordinals.
          *   - `removes`: period keys evicted by retention (`currentPeriod - 3`), empty on non-boundary ordinals.
          *   - `next`: the post-accept `historicalStakeSnapshots` map (`baseInfo.historicalStakeSnapshots` on non-boundary,
          *     `(pruned.updated(currentPeriod, newSnapshot))` on boundary).
          *
          * Deterministic on `(ordinal, baseInfo)` — both producers and verifiers run this and produce the same bytes, which is the
          * MPT/Brotli state-proof parity contract.
          */
        private def computeHistoricalStakeBoundaryDelta(
          ordinal: SnapshotOrdinal,
          baseInfo: GlobalSnapshotInfo
        )(
          implicit hasher: Hasher[F]
        ): F[(SortedMap[EtaPeriod, HistoricalStakeSnapshot], Set[EtaPeriod], SortedMap[EtaPeriod, HistoricalStakeSnapshot])] = {
          val ordValue = ordinal.value.value
          if (etaRotationSnapshots > 0L && ordValue % etaRotationSnapshots == etaRotationSnapshots - 1L) {
            val currentPeriod = EtaPeriod(ordValue / etaRotationSnapshots)
            // §G2 — read the boundary `StakeDistribution` from the MPT via `NodeStakeAggregator` rather than from
            // `baseInfo.{activeDelegatedStakes, activeNodeCollaterals}`. The two are byte-equivalent here (MPT was just synced
            // from the same accepted records that built the GSI), and routing via MPT removes the redundant in-memory mirror —
            // closing a class of #218-style cross-node drift bugs where two nodes' GSI iteration order produced divergent bytes.
            //
            // Path 1 (heap-leak workstream): also resolve eta_currentPeriod via `etaForPeriod` and pack it into the
            // `HistoricalStakeSnapshot` boundary entry. Eta is deterministic from the canonical chain at this
            // point (derived from period (currentPeriod-1)'s first 2/3 VRF outputs, fully knowable before
            // currentPeriod even starts), so all honest verifiers produce byte-equivalent boundary entries.
            val etaF: F[Hash] = etaForPeriod.map(_(currentPeriod)).getOrElse(Async[F].pure(Hash.empty))
            (NodeStakeAggregator.snapshotFromMpt[F](stakeAggregator), etaF).mapN { (newStakeSnapshot, eta) =>
              val newSnapshot = HistoricalStakeSnapshot(newStakeSnapshot, eta)
              val retentionMinPeriod = currentPeriod.value - 3L
              val priorKeys = baseInfo.historicalStakeSnapshots.keySet
              val pruned = baseInfo.historicalStakeSnapshots.filter(_._1.value >= retentionMinPeriod)
              val next = pruned.updated(currentPeriod, newSnapshot)
              // Adds: include the boundary write *and* any retained-prior entry whose value `next` newly
              // exposes — in practice only `currentPeriod` is added because retained priors equal their
              // pre-boundary values, but writing them is idempotent and stays consistent if retention rules
              // ever evolve. Keep the minimal form to match `buildGlobalSnapshotInfo`'s `updated` semantics.
              val adds: SortedMap[EtaPeriod, HistoricalStakeSnapshot] =
                SortedMap[EtaPeriod, HistoricalStakeSnapshot](currentPeriod -> newSnapshot)
              // Removes: prior keys that survived in `baseInfo.historicalStakeSnapshots` but are below the
              // retention floor. The producer's MPT writer prunes them; the verifier rebuilds from `next` so
              // it doesn't see them — without the explicit removal the producer's MPT keeps a stale entry
              // and parity (#107) breaks at the next boundary.
              val removes: Set[EtaPeriod] = priorKeys.filter(_.value < retentionMinPeriod).toSet
              (adds, removes, next)
            }
          } else {
            Async[F].pure(
              (
                SortedMap.empty[EtaPeriod, HistoricalStakeSnapshot],
                Set.empty[EtaPeriod],
                baseInfo.historicalStakeSnapshots
              )
            )
          }
        }

        /** Build result carrying the post-accept GSI together with the §3 NIPoPoW boundary delta. Both pieces are derived from the same
          * `baseInfo`; returning them together keeps GSI field set and MPT delta in lockstep (parity contract for the MPT/Brotli state
          * proof).
          */
        private case class BuildGlobalSnapshotInfoResult(
          gsi: GlobalSnapshotInfo,
          historicalStakeAdds: SortedMap[EtaPeriod, HistoricalStakeSnapshot],
          historicalStakeRemoves: Set[EtaPeriod]
        )

        private def buildGlobalSnapshotInfo(
          ordinal: SnapshotOrdinal,
          era: Era,
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
        )(
          implicit hasher: Hasher[F]
        ): F[BuildGlobalSnapshotInfoResult] = {
          val baseInfo = GlobalSnapshotInfo(
            updatedLastStateChannelSnapshotHashes,
            if (era.beforeTess3(ordinal))
              lastSnapshotContext.lastTxRefs ++ acceptanceResult.contextUpdate.lastTxRefs
            else transactionsRefs,
            updatedBalancesBySpendTransactions,
            updatedLastCurrencySnapshots,
            updatedLastCurrencySnapshotProofs,
            era.postTess3(ordinal)(updatedAllowSpendsCleaned),
            era.postTess3(ordinal)(updatedGlobalTokenLocksCleaned),
            era.postTess3(ordinal)(updatedTokenLockBalancesCleaned),
            era.postTess3(ordinal)(updatedAllowSpendRefs),
            era.postTess3(ordinal)(updatedTokenLockRefs),
            era.postTess3(ordinal)(updatedUpdateNodeParameters),
            era.postTess3(ordinal)(updatedCreateDelegatedStakesCleaned),
            era.postTess3(ordinal)(updatedWithdrawDelegatedStakesCleaned),
            era.postTess3(ordinal)(updatedCreateNodeCollateralsCleaned),
            era.postTess3(ordinal)(updatedWithdrawNodeCollateralsCleaned),
            era.postTess301(ordinal)(updatedPriceState),
            era.postMetagraphSync(ordinal)(updatedAcceptedMetagraphSyncData),
            // §3 NIPoPoW S0.4: passthrough initially; the boundary write below overwrites if this ordinal
            // closes an eta-period (`ord % R == R - 1`).
            lastSnapshotContext.historicalStakeSnapshots
          )
          // §3 NIPoPoW S0.4 — Cardano-style mark/set/go boundary write.
          //
          // At every closing ordinal of period N, capture the just-built `activeDelegatedStakes + activeNodeCollaterals`
          // into `historicalStakeSnapshots[N]`. Slot-leader eligibility in period N+2 reads `relativeStakeAt(_, N)` —
          // the 2-period gap gives finality time for this snapshot to lock in before consensus depends on it.
          //
          // Retention: keep the last 4 periods (`[currentPeriod - 3, currentPeriod]`). The algorithm reads N-2; the extra
          // slot is a reorg grace.
          //
          // The boundary delta is computed in `computeHistoricalStakeBoundaryDelta` so the same logic feeds both this
          // GSI field set and the `StateChangesAccumulator.historicalStakeSnapshots` / `removedHistoricalStakeSnapshotKeys`
          // delta — keeping them in lockstep is the MPT parity contract.
          //
          // §G2 — `computeHistoricalStakeBoundaryDelta` is now `F[]` (reads the boundary `StakeDistribution` from MPT via
          // `NodeStakeAggregator.snapshotFromMpt`). `buildGlobalSnapshotInfo` lifts into `F[]` here.
          computeHistoricalStakeBoundaryDelta(ordinal, baseInfo).map {
            case (adds, removes, nextHistorical) =>
              BuildGlobalSnapshotInfoResult(
                gsi = baseInfo.copy(historicalStakeSnapshots = nextHistorical),
                historicalStakeAdds = adds,
                historicalStakeRemoves = removes
              )
          }
        }

        /** Bundle GSAM-derived diffs into typed proto events and route them to the publisher.
          *
          * All emissions are best-effort: this method's errors are swallowed by `attempt.void` at the call site, so a publisher fault never
          * blocks consensus.
          */
        private def emitLocalEvents(
          ordinal: SnapshotOrdinal,
          priorBalances: SortedMap[Address, Balance],
          postBalances: SortedMap[Address, Balance],
          acceptedGlobalAllowSpends: List[Signed[AllowSpend]],
          acceptedGlobalTokenLocks: List[Signed[TokenLock]],
          expiredAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
          expiredTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
          tokenUnlocks: Map[Address, List[TokenUnlock]],
          acceptedTransactions: SortedSet[Signed[Transaction]],
          scSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
          currencyAcceptanceBalanceUpdate: SortedMap[Address, Balance],
          lastSnapshotContext: GlobalSnapshotInfo
        )(implicit hasher: Hasher[F]): F[Unit] = {
          // BalanceChange — diff post-vs-prior, emit one envelope per delta. The cause is left
          // free-form `"accept"` here; finer granularity (block / reward / tokenlock / allowspend /
          // spend) would require threading more context in.
          val balanceChanges: List[PbBalanceChange] = {
            val keys = (priorBalances.keySet ++ postBalances.keySet).toList
            keys.flatMap { addr =>
              val o = priorBalances.get(addr).map(_.value.value).getOrElse(0L)
              val n = postBalances.get(addr).map(_.value.value).getOrElse(0L)
              if (o == n) Nil
              else List(PbBalanceChange(address = addr.value.value, oldBalance = o, newBalance = n, cause = "accept"))
            }
          }

          // TokenLockStateChange.CREATED for accepted; .EXPIRED for the hoisted expired set.
          val tlCreated: F[List[PbTokenLockStateChange]] =
            acceptedGlobalTokenLocks.traverse { signed =>
              signed.toHashed.map { h =>
                PbTokenLockStateChange(
                  address = signed.value.source.value.value,
                  tokenLockRef = com.google.protobuf.ByteString.copyFrom(h.hash.value.getBytes),
                  transition = PbTokenLockStateChange.Transition.CREATED,
                  amount = signed.value.amount.value.value,
                  unlockEpoch = signed.value.unlockEpoch.map(_.value.value).getOrElse(0L)
                )
              }
            }
          val tlExpired: F[List[PbTokenLockStateChange]] =
            expiredTokenLocks.toList.flatMap { case (addr, set) => set.toList.map(addr -> _) }.traverse {
              case (addr, signed) =>
                signed.toHashed.map { h =>
                  PbTokenLockStateChange(
                    address = addr.value.value,
                    tokenLockRef = com.google.protobuf.ByteString.copyFrom(h.hash.value.getBytes),
                    transition = PbTokenLockStateChange.Transition.EXPIRED,
                    amount = signed.value.amount.value.value,
                    unlockEpoch = signed.value.unlockEpoch.map(_.value.value).getOrElse(0L)
                  )
                }
            }
          // TokenLockStateChange.WITHDRAWN from generated unlocks (one TokenUnlock per ref).
          val tlWithdrawn: List[PbTokenLockStateChange] =
            tokenUnlocks.toList.flatMap {
              case (addr, unlocks) =>
                unlocks.map { u =>
                  PbTokenLockStateChange(
                    address = addr.value.value,
                    tokenLockRef = com.google.protobuf.ByteString.copyFrom(u.tokenLockRef.value.getBytes),
                    transition = PbTokenLockStateChange.Transition.WITHDRAWN,
                    amount = u.amount.value.value,
                    unlockEpoch = 0L
                  )
                }
            }

          // AllowSpendStateChange.CREATED for accepted; .EXPIRED for the hoisted expired set.
          // TODO(LocalEvents v2): AllowSpendStateChange.CONSUMED is not emitted yet.
          // Adding it requires hooking into SpendActionValidator / SpendTransactionBalanceManager
          // where consumption is finalized. Proto reserves CONSUMED = 2 (oneof tag in
          // AllowSpendStateChange.Transition) so addition is additive. See
          // docs/nakamoto/LOCAL-EVENTS-SERVICE-DESIGN.md §11.
          val asCreated: F[List[PbAllowSpendStateChange]] =
            acceptedGlobalAllowSpends.traverse { signed =>
              signed.toHashed.map { h =>
                PbAllowSpendStateChange(
                  address = signed.value.source.value.value,
                  allowSpendRef = com.google.protobuf.ByteString.copyFrom(h.hash.value.getBytes),
                  transition = PbAllowSpendStateChange.Transition.CREATED,
                  amount = signed.value.amount.value.value,
                  destination = signed.value.destination.value.value,
                  expiryEpoch = signed.value.lastValidEpochProgress.value.value
                )
              }
            }
          val asExpired: F[List[PbAllowSpendStateChange]] =
            expiredAllowSpends.toList.flatMap { case (addr, set) => set.toList.map(addr -> _) }.traverse {
              case (addr, signed) =>
                signed.toHashed.map { h =>
                  PbAllowSpendStateChange(
                    address = addr.value.value,
                    allowSpendRef = com.google.protobuf.ByteString.copyFrom(h.hash.value.getBytes),
                    transition = PbAllowSpendStateChange.Transition.EXPIRED,
                    amount = signed.value.amount.value.value,
                    destination = signed.value.destination.value.value,
                    expiryEpoch = signed.value.lastValidEpochProgress.value.value
                  )
                }
            }

          // TransactionAccepted for every accepted DAG tx this ordinal.
          val txs: F[List[PbTransactionAccepted]] =
            acceptedTransactions.toList.traverse { signed =>
              signed.toHashed.map { h =>
                PbTransactionAccepted(
                  txHash = com.google.protobuf.ByteString.copyFrom(h.hash.value.getBytes),
                  source = signed.value.source.value.value,
                  destination = signed.value.destination.value.value,
                  amount = signed.value.amount.value.value,
                  fee = signed.value.fee.value.value
                )
              }
            }

          // MetagraphSnapshotAccepted for every (mgAddr, snapshot) pair in scSnapshots.
          val mgSnapshots: F[List[PbMetagraphSnapshotAccepted]] =
            scSnapshots.toList.flatMap {
              case (mgAddr, nel) => nel.toList.map(mgAddr -> _)
            }.traverse {
              case (mgAddr, signed) =>
                signed.toHashed.map { h =>
                  PbMetagraphSnapshotAccepted(
                    metagraphAddress = mgAddr.value.value,
                    metagraphOrdinal = 0L, // ordinal not cheap to decode here; left at 0 until needed
                    metagraphSnapshotHash = com.google.protobuf.ByteString.copyFrom(h.hash.value.getBytes),
                    gl0Ordinal = ordinal.value.value
                  )
                }
            }

          // MetagraphBalanceChange — derive from `currencyAcceptanceBalanceUpdate`. The update is
          // DAG-side balance changes triggered by SC events; the metagraph context is captured by
          // the snapshot event above. Until we thread per-metagraph balance diffs, leave the
          // metagraphAddress empty and let the test client filter by address.
          val mgBalances: List[PbMetagraphBalanceChange] =
            currencyAcceptanceBalanceUpdate.toList.flatMap {
              case (addr, newBal) =>
                val oldBal = lastSnapshotContext.balances.getOrElse(addr, io.constellationnetwork.schema.balance.Balance.empty)
                if (oldBal == newBal) Nil
                else
                  List(
                    PbMetagraphBalanceChange(
                      metagraphAddress = "",
                      address = addr.value.value,
                      oldBalance = oldBal.value.value,
                      newBalance = newBal.value.value,
                      cause = "currency_accept"
                    )
                  )
            }

          for {
            _ <- publisher.publishBalanceChanges(ordinal, balanceChanges)
            tlc <- tlCreated
            tle <- tlExpired
            _ <- publisher.publishTokenLockChanges(ordinal, tlc ++ tle ++ tlWithdrawn)
            asc <- asCreated
            ase <- asExpired
            _ <- publisher.publishAllowSpendChanges(ordinal, asc ++ ase)
            tx <- txs
            _ <- publisher.publishTransactionsAccepted(ordinal, tx)
            mgs <- mgSnapshots
            _ <- publisher.publishMetagraphEvents(ordinal, mgs, mgBalances)
          } yield ()
        }

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
          getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
          parentTip: BranchId,
          shardCheckpoints: SortedMap[ShardId, ShardCheckpoint] = SortedMap.empty
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
            SortedMap[PeerId, Map[Address, Amount]],
            BranchHandle[F, GlobalStateKey]
          )
        ] = {
          implicit val hasher: Hasher[F] = HasherSelector[F].getForOrdinal(ordinal)

          val era = Era.fromConfig(environment, fieldsAddedOrdinals)

          val tessellation3MigrationStartingOrdinal = fieldsAddedOrdinals.tessellation3Migration
            .getOrElse(environment, SnapshotOrdinal.MinValue)

          val fixingAllowSpendAndTokenLockValidation = fieldsAddedOrdinals.fixingAllowSpendAndTokenLockValidation
            .getOrElse(environment, SnapshotOrdinal.MinValue)

          // Phase J: serialize accept() across the GSAM instance with `acceptMutex`. The dynamic
          // branch-aware reader closes over `branchTipRef.get`; without this lock, a concurrent
          // accept() (e.g. the consensus FSM running validateArtifact while SnapshotLeaderLoop
          // runs createProposalArtifact) could overwrite `branchTipRef` mid-read and route the
          // first call's manager reads to the second call's parentTip view. The previous
          // assumption that `mptStore.withTransaction` serialized acceptance was wrong — that
          // bracket only protects the producer's savepoint, not the overlay's branch-Ref state.
          // Under Passthrough this lock is also safe (no contention since accept was already
          // serialized via `snapshotSemaphore` on dag-l0).
          loggerBundle.app.withOrdinal(ordinal) {
            acceptMutex.permit.use { _ =>
              for {
                _ <- loggerBundle.app.info(
                  s"[ACCEPTANCE] ordinal=$ordinal epoch=${epochProgress.show} ENTER " +
                    s"blocks=${blocksForAcceptance.size} allowSpend=${allowSpendBlocksForAcceptance.size} " +
                    s"tokenLock=${tokenLockBlocksForAcceptance.size} sc=${scEvents.size} unp=${unpEvents.size} " +
                    s"cds=${cdsEvents.size} wds=${wdsEvents.size} cnc=${cncEvents.size} wnc=${wncEvents.size}"
                )

                // Branch-aware checkout against the parent's tip. Reads through `mpt` route through
                // `overlay.get(parentTip, _)`, so priors observe the parent-branch view (under
                // `OverlayMode.Passthrough` this collapses to the underlying base). The legacy
                // `journal.unapplyTo(ord-1)` fork-switch reverse-apply is gone: branch-scoped reads
                // make rollback unnecessary — a competing proposal at the same slot checks out
                // against its own parent and accumulates writes in its own handle, never observing
                // sibling branches' deltas. (#70's gl0 leader-vs-validator at same ordinal disappears
                // by construction.)
                handle <- overlay.checkout(parentTip)
                mpt = AcceptanceMpt.fromOverlay[F](overlay, parentTip, handle)
                // Phase J: bind the per-call `parentTip` into the dynamic reader's Ref. The Ref
                // is shared across accept() calls but `acceptMutex` (above) ensures only one
                // accept() reads/writes it at a time.
                _ <- branchTipRef.set(parentTip)

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
                _dagLayerTokenLocks = tokenLockBlockAcceptanceResult.accepted.flatMap(_.value.tokenLocks.toList)
                _ <- loggerBundle.app.info {
                  val sigs = _dagLayerTokenLocks
                    .map(tl =>
                      s"src=${tl.value.source.value.value.takeRight(8)}/amt=${tl.value.amount.value.value}/lastRef=${tl.value.parent.ordinal.value.value}"
                    )
                    .mkString(",")
                  s"[Q2/gl0-DAG-extract] ordinal=$ordinal dagLayerTokenLocks=${_dagLayerTokenLocks.size} sigs=[$sigs]"
                }
                acceptedGlobalTokenLocks <- tokenLockStateManager.acceptReplacementTokenLocks(
                  _dagLayerTokenLocks,
                  lastSnapshotContext
                )

                // ─── Q2 hoist: compute expired sets ONCE per accept() ──────────────────────────
                // Both the TokenLockStateManager and the AllowSpendStateManager internally call
                // `findExpired...ViaIndexFromMpt(previousEpochProgress, epochProgress)` from at least
                // two sites each (`accept*FromMpt` + `updateGlobalBalancesBy*FromMpt`). The walk traverses
                // an MPT epoch-index range per accept; hoisting the result up here means a SINGLE walk
                // per accept(), then we thread the precomputed expired sets through the new
                // `*WithExpired` variants. Same byte-equivalent outputs as before; just one MPT walk
                // instead of four. The publisher also reads this single source for EXPIRED events.
                expiredTokenLocksHoisted <- tokenLockStateManager.findExpiredGlobalTokenLocksViaIndexFromMpt(
                  previousEpochProgress,
                  epochProgress
                )
                expiredAllowSpendsHoisted <- allowSpendStateManager.findExpiredGlobalAllowSpendsViaIndexFromMpt(
                  previousEpochProgress,
                  epochProgress
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

                // Source prior-ordinal `updateNodeParameters` from the MPT instead of `lastSnapshotContext.updateNodeParameters`.
                // The MPT key is a hash of the `Id`, but the signed value carries the signer's `Id` in `proofs.head.id`,
                // which by GSAM convention matches the map's keying `Id`. No sidecar needed; prefix-scan + value-decode.
                // Phase J: route through `mpt` (branch-aware) so prior reads see the parent branch's pending writes
                // under MultiBranch — `mptStore` is base-only and would miss any not-yet-folded entries.
                priorUpdateNodeParameters <- {
                  import io.constellationnetwork.security.signature.Signed
                  for {
                    prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.UpdateNodeParameters)
                    entries <- mpt.getAllForPrefix[(Signed[UpdateNodeParameters], SnapshotOrdinal)](prefix)
                  } yield SortedMap.from(entries.values.map { case (signed, ord) => signed.proofs.head.id -> (signed, ord) })
                }

                updatedUpdateNodeParameters = priorUpdateNodeParameters ++
                  initialData.nodeParamsResult.view.mapValues(unp => (unp, ordinal))

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

                // Source prior `lastStateChannelSnapshotHashes` from the MPT instead of `lastSnapshotContext`. The
                // `LastStateChannelSnapshotHashes` partition is metagraph-keyed and the value (`Hash`) doesn't carry the
                // address; the `ActiveAddressIndex` sidecar tracks the keyset, and `getMany` preserves the original
                // `MetagraphNamespace(addr)` for pattern-match recovery. Read here (before
                // `processStateChannelEvents`) so the StateChannelAcceptanceManager can be GSI-free.
                // Phase J: route through `mpt` (branch-aware) — `mptStore` is base-only.
                //
                // #113 fallback: under MultiBranch, a transient chain-walk race (e.g. a sibling-branch eviction
                // dropping an ancestor that held the index sidecar's latest write) can return an empty `addrSet`
                // for one accept() call even when the keyset is fully populated in `lastSnapshotContext`. We union
                // the GSI keyset in defensively so the materialized priors stay byte-equivalent across nodes.
                // Under Passthrough this union is a no-op (MPT addrSet always equals GSI keyset). Under
                // steady-state MultiBranch it's also a no-op for the same reason — the union only adds keys when
                // MPT is transiently behind. Logged at warn when the union actually grows the keyset, so the
                // diagnostic is visible without flooding logs in steady state.
                priorLastStateChannelSnapshotHashes <- {
                  import io.constellationnetwork.schema.mpt.PartitionNamespace.MetagraphNamespace
                  for {
                    indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastStateChannelSnapshotHashes)
                    mptAddrSet <- mpt.get[SortedSet[Address]](indexKey).map(_.getOrElse(SortedSet.empty[Address]))
                    gsiAddrSet = lastSnapshotContext.lastStateChannelSnapshotHashes.keySet.to(SortedSet)
                    addrSet = mptAddrSet ++ gsiAddrSet
                    _ <-
                      if (addrSet.size > mptAddrSet.size)
                        loggerBundle.app.warn(
                          s"#113 priorLastStateChannelSnapshotHashes: MPT addrSet=${mptAddrSet.size} GSI keyset=${gsiAddrSet.size} " +
                            s"union=${addrSet.size} ord=$ordinal — using GSI fallback for ${addrSet.size - mptAddrSet.size} addr(s)"
                        )
                      else Async[F].unit
                    keys = addrSet.toList.map(addr => GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastStateChannelSnapshotHashes))
                    values <- mpt.getMany[Hash](keys)
                    mptResult = SortedMap.from(values.toList.flatMap {
                      case (key, h) =>
                        key.networkNamespace match {
                          case MetagraphNamespace(addr) => List(addr -> h)
                          case _                        => Nil
                        }
                    })
                    // For addresses where MPT had no value (e.g., the entry lives only in lastSnapshotContext
                    // because the index race lost the write), fall back to the GSI's hash. Under steady-state
                    // this branch never fires: every key in addrSet has an MPT value.
                    result = lastSnapshotContext.lastStateChannelSnapshotHashes.foldLeft(mptResult) {
                      case (acc, (addr, h)) => if (acc.contains(addr)) acc else acc.updated(addr, h)
                    }
                  } yield result
                }

                // Source prior `lastCurrencySnapshots` from the MPT instead of `lastSnapshotContext`. The
                // `Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]` value
                // spans 3 metagraph-keyed partitions; we read the keyset from `LastCurrencySnapshots`'s
                // `ActiveAddressIndex` sidecar (which marks any address with either mode), then per-address try Left
                // before falling back to the Right pair. Read here (before `processStateChannelEvents`) so the
                // StateChannelEventsProcessor can be GSI-free — `getFeeAddresses` and the `initialState` lookup
                // both consume this materialized map.
                // Phase J: route through `mpt` (branch-aware) — `mptStore` is base-only.
                //
                // #113 fallback: same defensive union pattern as priorLastStateChannelSnapshotHashes. When the
                // MPT chain walk returns an empty `addrSet` due to a transient MultiBranch race, we union the
                // GSI keyset and per-address fall back to the GSI value so the metagraph entry doesn't drop out
                // of the prior map. Without this, `currencySnapshotsDeltas` for the subsequent ord would only
                // see new SC events, and the `processStateChannelEvents` validation would treat the metagraph
                // as having no prior history — slowing or breaking the state-channel pipeline that cl1 listens
                // on for balance updates (root cause of L0-token reverse balance non-settlement).
                priorLastCurrencySnapshots <- {
                  for {
                    indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastCurrencySnapshots)
                    mptAddrSet <- mpt.get[SortedSet[Address]](indexKey).map(_.getOrElse(SortedSet.empty[Address]))
                    gsiAddrSet = lastSnapshotContext.lastCurrencySnapshots.keySet.to(SortedSet)
                    addrSet = mptAddrSet ++ gsiAddrSet
                    _ <-
                      if (addrSet.size > mptAddrSet.size)
                        loggerBundle.app.warn(
                          s"#113 priorLastCurrencySnapshots: MPT addrSet=${mptAddrSet.size} GSI keyset=${gsiAddrSet.size} " +
                            s"union=${addrSet.size} ord=$ordinal — using GSI fallback for ${addrSet.size - mptAddrSet.size} addr(s)"
                        )
                      else Async[F].unit
                    addrList = addrSet.toList
                    leftKeys = addrList.map(addr => GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastCurrencySnapshots))
                    incKeys = addrList.map(addr => GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastIncrementalCurrencySnapshots))
                    infoKeys = addrList.map(addr => GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastCurrencySnapshotInfo))
                    lefts <- mpt.getMany[Signed[CurrencySnapshot]](leftKeys)
                    incs <- mpt.getMany[Signed[CurrencyIncrementalSnapshot]](incKeys)
                    infos <- mpt.getMany[CurrencySnapshotInfo](infoKeys)
                    mptResult = SortedMap.from(addrList.flatMap { addr =>
                      val leftKey = GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastCurrencySnapshots)
                      val incKey = GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastIncrementalCurrencySnapshots)
                      val infoKey = GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastCurrencySnapshotInfo)
                      lefts.get(leftKey) match {
                        case Some(snap) => List(addr -> Left(snap))
                        case None =>
                          (incs.get(incKey), infos.get(infoKey)) match {
                            case (Some(inc), Some(info)) => List(addr -> Right((inc, info)))
                            case _                       => Nil
                          }
                      }
                    })
                    // Per-address fallback: if MPT has no value but the GSI does, use the GSI's value.
                    // Steady-state this loop is a no-op (every addrSet member has an MPT value).
                    result = lastSnapshotContext.lastCurrencySnapshots.foldLeft(mptResult) {
                      case (acc, (addr, v)) => if (acc.contains(addr)) acc else acc.updated(addr, v)
                    }
                  } yield result
                }

                // Slice 13 (hierarchical-shard-checkpoints v1, §13 row 13). Gate: the new shard-checkpoint admission path
                // only fires when (a) the manager was wired with `shardingConfig.numShards > 1` AND
                // `shardCheckpointAcceptanceManager.isDefined`, AND (b) the caller actually supplied a non-empty
                // `shardCheckpoints` map for this ord. All three conditions must hold; otherwise `effectiveScEvents`
                // collapses to the raw input `scEvents` and the rest of `accept()` runs byte-identically to today
                // (the regression bar — see make()'s shardingConfig scaladoc).
                //
                // The shard-derived `StateChannelOutput`s are APPENDED (not replacement) to handle the mixed-bootstrap
                // case: a shard not yet wired with a committee can still feed binaries via the legacy raw event path
                // for the same gl0 ord; the standard `processStateChannelEvents` chain-link check naturally
                // deduplicates per-MG.
                effectiveScEvents <-
                  (shardingConfig, shardCheckpointAcceptanceManager) match {
                    case (Some(cfg), Some(scMgr)) if cfg.numShards > 1 && shardCheckpoints.nonEmpty =>
                      processShardCheckpoints(ordinal, shardCheckpoints, scEvents, scMgr)
                    case _ => Async[F].pure(scEvents)
                  }

                StateChannelAcceptanceResult(
                  scSnapshots,
                  currencySnapshots,
                  returnedSCEvents,
                  currencyAcceptanceBalanceUpdate,
                  incomingCurrencySnapshots
                ) <- processStateChannelEvents(
                  ordinal,
                  updatedGlobalBalances,
                  priorLastStateChannelSnapshotHashes,
                  priorLastCurrencySnapshots,
                  effectiveScEvents,
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

                // `updatedBalancesByRewards` carries only addresses touched by this ordinal's blocks/rewards. The
                // SpendActionValidator consumes this via `getOrElse(addr, Balance.empty)` and treats absence as zero,
                // so a metagraph self-spend (`spendTransactionB` with no allowSpendRef) whose DAG balance was funded
                // in a PRIOR ordinal would always reject as `NotEnoughCurrencyIdBalance{balance: 0}`. Merge with
                // `priorBalances` (full prior-ordinal map from MPT, materialized at line 747) so the validator
                // sees the cumulative state. Mirrors the GSI-store merge at line 1150.
                globalBalances = SortedMap(none[Address] -> (priorBalances ++ updatedBalancesByRewards))

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
                acceptedSpendActionsMessage =
                  s"[CONSENSUS:PROPOSAL] [ORDINAL=$ordinal] Accepted spend actions: ${acceptedSpendActions.show}"
                rejectedSpendActionMessage = s"[CONSENSUS:PROPOSAL] [ORDINAL=$ordinal] Rejected spend actions: ${rejectedSpendActions.show}"
                acceptedPricingUpdatesMessage =
                  s"[CONSENSUS:PROPOSAL] [ORDINAL=$ordinal] Accepted pricing updates: ${acceptedPricingUpdates.show}"
                rejectedPricingUpdatesMessage =
                  s"[CONSENSUS:PROPOSAL] [ORDINAL=$ordinal] Rejected pricing updates: ${rejectedPricingUpdates.show}"

                _ <- loggerBundle.app.info(acceptedSpendActionsMessage)
                _ <- loggerBundle.app.info(rejectedSpendActionMessage)
                _ <- loggerBundle.app.info(acceptedPricingUpdatesMessage)
                _ <- loggerBundle.app.info(rejectedPricingUpdatesMessage)

                updatedLastStateChannelSnapshotHashes = priorLastStateChannelSnapshotHashes ++ sCSnapshotHashes
                updatedLastCurrencySnapshots = priorLastCurrencySnapshots ++ currencySnapshots

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

                allowSpendAcceptanceResult <- allowSpendStateManager.acceptAllowSpendsWithExpired(
                  epochProgress,
                  activeAllowSpendsFromCurrencySnapshots,
                  globalAllowSpends,
                  globalActiveAllowSpends,
                  allAcceptedSpendTxns,
                  expiredAllowSpendsHoisted
                )
                updatedAllowSpends = allowSpendAcceptanceResult.fullState
                allowSpendsDeltas = allowSpendAcceptanceResult.deltas
                removedAllowSpendKeys = allowSpendAcceptanceResult.removedKeys
                allowSpendExpiryIndexDelta = allowSpendAcceptanceResult.expiryIndexDelta

                updatedAllowSpendRefs = allowSpendStateManager.acceptAllowSpendRefs(
                  globalLastAllowSpendRefs,
                  allowSpendBlockAcceptanceResult.contextUpdate.lastTxRefs
                )

                allowSpendBalancesResult <- allowSpendStateManager.updateGlobalBalancesByAllowSpendsWithExpired(
                  epochProgress,
                  updatedBalancesByRewards,
                  globalAllowSpends,
                  expiredAllowSpendsHoisted
                )
                (updatedBalancesByAllowSpends, updatedBalancesByAllowSpendsDeltas) <- Async[F].fromEither(
                  allowSpendBalancesResult
                    .leftMap(ex => new RuntimeException(s"Balance arithmetic error updating balances by allow spends: $ex"))
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

                tokenLockAcceptanceResult <- tokenLockStateManager.acceptTokenLocksWithExpired(
                  epochProgress,
                  globalTokenLocks,
                  globalActiveTokenLocks,
                  generatedTokenUnlocks,
                  expiredTokenLocksHoisted
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
                TokenLockBalanceResult(updatedTokenLockBalances, tokenLockBalancesDeltas, removedTokenLockBalanceKeys) =
                  tokenLockStateManager
                    .updateTokenLockBalances(
                      currencySnapshots,
                      priorTokenLockBalances.some
                    )
                // #259 instrumentation (Q2/gl0-currency-extract): currency-snapshot-side token-lock balance flow.
                // updateTokenLockBalances pulls token-lock balances from `currencySnapshots` (which contain
                // currency-l0's incremental snapshots). If this delta is empty when test creates a metagraph lock,
                // it means the currency snapshot fed in here did NOT carry the lock. Compare with [Q2/cl0-snapshot]
                // logs on the same metagraph to see if cl0 included it but gl0 lost it.
                _ <- loggerBundle.app.info {
                  val perMg = currencySnapshots.toList.map {
                    case (mgAddr, Right((signed, _))) =>
                      val locks = signed.value.tokenLockBlocks.map(_.toList).getOrElse(List.empty).flatMap(_.tokenLocks.toList)
                      val sigs =
                        locks.map(tl => s"src=${tl.value.source.value.value.takeRight(8)}/amt=${tl.value.amount.value.value}").mkString(",")
                      s"${mgAddr.value.value.takeRight(8)}:ord=${signed.ordinal.value}/locks=${locks.size}[$sigs]"
                    case (mgAddr, Left(_)) => s"${mgAddr.value.value.takeRight(8)}:full-snap"
                  }.mkString(" ")
                  val deltaSigs = tokenLockBalancesDeltas.toList.flatMap {
                    case (mgAddr, perAddr) =>
                      perAddr.toList.map {
                        case (a, b) => s"${mgAddr.value.value.takeRight(8)}->${a.value.value.takeRight(8)}=${b.value.value}"
                      }
                  }.mkString(",")
                  s"[Q2/gl0-currency-extract] ordinal=$ordinal currSnapshots={$perMg} tlbDeltaCount=${tokenLockBalancesDeltas.size} tlbDelta=[$deltaSigs]"
                }

                tokenLockBalancesResult <- tokenLockStateManager.updateGlobalBalancesByTokenLocksWithExpired(
                  epochProgress,
                  updatedBalancesByAllowSpends,
                  globalTokenLocks,
                  generatedTokenUnlocks,
                  expiredTokenLocksHoisted
                )
                (updatedBalancesByTokenLocks, updatedBalancesByTokenLocksDeltas) <- Async[F].fromEither(
                  tokenLockBalancesResult
                    .leftMap(err => new RuntimeException(s"Balance arithmetic error updating balances by token locks: $err"))
                )

                lastActiveGlobalAllowSpends = globalActiveAllowSpends.getOrElse(
                  None,
                  SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]]
                )

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
                  spendTxBalancesResult
                    .leftMap(err => new RuntimeException(s"Balance arithmetic error updating balances by spend transactions: $err"))
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

                gsiResult <- buildGlobalSnapshotInfo(
                  ordinal,
                  era,
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
                gsi = gsiResult.gsi

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
                  // §3 NIPoPoW S0.4: empty on non-boundary ordinals, the boundary entry/eviction set otherwise.
                  // `buildGlobalSnapshotInfo` runs the same `computeHistoricalStakeBoundaryDelta` to set
                  // `gsi.historicalStakeSnapshots` — GSI reads and MPT writes stay byte-equal.
                  historicalStakeSnapshots = gsiResult.historicalStakeAdds,
                  removedHistoricalStakeSnapshotKeys = gsiResult.historicalStakeRemoves,
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

                // Per-entry balance delta dump for #70 dl1/gl0 mptRoot.balances divergence diagnosis.
                // SortedMap iteration is deterministic; Balance.toString is a Long — both content-stable
                // across nodes. Each entry: <addr-last8>=<balance>. Uncapped (#257 instrumentation): the
                // 32-cap was masking divergences whose tell lay beyond the head sample. Verify-side dumps
                // its full computed map (uncapped) on mismatch — both sides need full coverage to cross-diff.
                _ <- loggerBundle.app.info {
                  val entries = stateChangesAccumulator.balances.toSeq.map {
                    case (addr, bal) => s"${addr.value.value.takeRight(8)}=${bal.value.value}"
                  }
                  s"[ACCEPTANCE] ordinal=$ordinal MPT_SYNC_FP_BAL_DELTA: [${entries.mkString(",")}]"
                }
                // #257 instrumentation: per-entry activeDelegatedStakes delta dump. Verify-side flagged this
                // partition as a diverging field (cl1/dl1 vs gl0). Render each entry as
                // <addr-last8>=<recordCount>:<hash> so per-address divergence localizes without dumping the
                // full DelegatedStakeRecord (which contains nested Signed structures). Uncapped — sample size
                // is bounded by the active stake set.
                _ <- loggerBundle.app.info {
                  val entries = stateChangesAccumulator.activeDelegatedStakes.toSeq.map {
                    case (addr, records) =>
                      val rh = "%08x".format(records.toString.hashCode)
                      s"${addr.value.value.takeRight(8)}=${records.size}:$rh"
                  }
                  s"[ACCEPTANCE] ordinal=$ordinal MPT_SYNC_FP_DS_DELTA: [${entries.mkString(",")}]"
                }
                _ <- loggerBundle.app.info {
                  val pipelineEntries = List(
                    "blocks" -> initialData.blockResult.contextUpdate.balances.toSortedMap,
                    "currAccept" -> currencyAcceptanceBalanceUpdate.toSortedMap,
                    "rewards" -> rewardBalancesDelta,
                    "allowSpends" -> updatedBalancesByAllowSpendsDeltas,
                    "tokenLocks" -> updatedBalancesByTokenLocksDeltas,
                    "spendTxs" -> updatedBalancesBySpendTransactionsDeltas
                  )
                  val parts = pipelineEntries.map {
                    case (label, m) =>
                      val rendered = m.toSeq.map { case (addr, bal) => s"${addr.value.value.takeRight(8)}=${bal.value.value}" }
                        .mkString(",")
                      s"$label=[${m.size},${"%08x".format(m.toString.hashCode)}]<$rendered>"
                  }
                  s"[ACCEPTANCE] ordinal=$ordinal MPT_SYNC_FP_BAL_PIPELINE: ${parts.mkString(" ")}"
                }

                // === MPT writes via the overlay algebra (#56.10 Phase D) ===
                // Snapshot the pre-sync MPT bytes for the verify-replay cross-check below: apply the
                // accumulator's delta to this map via the `toAccumulatorHexDelta` helper (a separate
                // encoder+merge path from the writer-algebra) and build an MPT via the Parallel
                // producer. If both paths agree on the post-state root, the writer is validated
                // without needing `GlobalSnapshotInfo` as an intermediate. Read at the parent-branch
                // view so MultiBranch sees pending state from ancestor branches (under Passthrough this
                // degenerates to `overlay.base.allEntriesAsBytes` since BranchId is ignored).
                preSyncBytes <- overlay.allEntriesAsBytes(parentTip)
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
                // Route the accumulator's deltas through the writer algebra: `mpt.insert / mpt.remove`
                // accumulate in the branch handle (Passthrough → straight to base; MultiBranch →
                // per-branch ChangeSet). Field/insert order, sidecar maintenance and removal-key
                // derivation mirror legacy `mptStore.syncFromStateChanges` byte-for-byte —
                // `GsamWritePathParitySuite` (#107) is the regression contract.
                _ <- AcceptanceMptStateChanges.applyStateChanges[F](mpt, stateChangesAccumulator)
                // Pull the post-write byte view from the overlay (Phase J). Under Passthrough this
                // collapses to `overlay.base.allEntriesAsBytes` (writes already committed inline);
                // under MultiBranch it composes parent-chain pending entries with this handle's
                // uncommitted accumulator. This is the byte-source for both the proof's mptRoot and
                // the verify-replay below — keeping them on the same source means a writer-bug
                // produces a `MATCH` (proof aligned with bytes) while a delta-replay-bug produces
                // `DIVERGED`. The handle is NOT committed here — `accept()` returns it to the caller
                // which knows the resulting snapshot's hash and runs `overlay.commit(handle,
                // BranchId(snapshotHash), ordinal)` once the artifact is sealed. Committing earlier
                // with `parentTip` as childTip would self-loop `pendingRef[parentTip]` under
                // MultiBranch (every accept overwrites it with `BranchEntry(parent = parentTip,...)`).
                postBytes <- overlay.allEntriesAsBytesWithHandle(handle, ordinal)
                // Temporary instrumentation (task #18): fingerprint the END gsi components used by
                // proof construction. If MPT_SYNC_PRE + MPT_SYNC_FP match across nodes but mptRoot
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
                // Phase J: format-conditional proof construction. MerklePatriciaFormat derives the
                // global mptRoot (and per-field roots) from `postBytes` via `mptStateProofFromBytes`,
                // which is correct under both Passthrough (postBytes == overlay.base bytes) and
                // MultiBranch (postBytes == base ⊕ parent-chain ⊕ handle accumulator). LegacyFormat
                // keeps the per-field-hash path unchanged via `builder.buildProof` since legacy
                // proofs don't carry mptRoot.
                isMptFormat = globalStateProofSelector.select(ordinal) == MerklePatriciaFormat
                incrementalProof <-
                  if (isMptFormat)
                    GlobalSnapshotInfo.mptStateProofFromBytes[F](gsi, postBytes)
                  else
                    builder.buildProof(gsi, ordinal)
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

                artifactsFromExpired <- artifactEmissionManager.emitAllExpiredArtifacts(
                  expiredAllowSpends,
                  expiredTokenLocksHoisted
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

                // ─── LocalEvents publisher emissions ─────────────────────────────────────
                // Fire-and-forget: spawn the emit onto a separate fiber so a slow/wedged subscriber
                // CANNOT semantically-block consensus. FS2 `Topic.publish1` returns
                // `F[Either[Topic.Closed, Unit]]` whose acquire suspends on the per-subscriber
                // bounded queue's `Queue.offer` — if a subscriber stops draining, the publishing
                // fiber blocks. Routing `emitLocalEvents` through `Async[F].start(...).void`
                // decouples consensus completion from event emission entirely.
                //
                // Per-event ordering inside `emitLocalEvents` is preserved by its internal serial
                // `>>` composition (publisher methods chain via `for`). Outer ordering across the
                // accept call vs. its emission is NOT load-bearing: subscribers receive events
                // tagged with `ordinal`, so observers reorder by `ordinal` if needed.
                //
                // `.attempt.void` retained as a defense-in-depth for any synchronous exception
                // before the publisher hits the fiber-blocking boundary (e.g. allocation failure
                // in the encoder).
                _ <- Async[F]
                  .start(
                    emitLocalEvents(
                      ordinal,
                      priorBalances = priorBalances,
                      postBalances = gsi.balances,
                      acceptedGlobalAllowSpends = acceptedGlobalAllowSpends,
                      acceptedGlobalTokenLocks = acceptedGlobalTokenLocks,
                      expiredAllowSpends = expiredAllowSpendsHoisted,
                      expiredTokenLocks = expiredTokenLocksHoisted,
                      tokenUnlocks = generatedTokenUnlocks,
                      acceptedTransactions = acceptedTransactions,
                      scSnapshots = scSnapshots,
                      currencyAcceptanceBalanceUpdate = currencyAcceptanceBalanceUpdate,
                      lastSnapshotContext = lastSnapshotContext
                    ).attempt.void
                  )
                  .void
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
                  delegatorRewardsMap,
                  handle
                )
            }
          }
        }
      }
    }
  }
}
