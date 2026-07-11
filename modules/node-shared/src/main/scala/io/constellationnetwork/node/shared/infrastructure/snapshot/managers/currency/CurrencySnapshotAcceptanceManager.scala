package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import cats.Parallel
import cats.data.{NonEmptySet, OptionT}
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.dataApplication.FeeTransaction
import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.currency.schema.globalSnapshotSync.{GlobalSnapshotSync, GlobalSyncView}
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.{FieldsAddedOrdinals, LastGlobalSnapshotsSyncConfig}
import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.PinnedCurrencyInfoReader
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage}
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.transaction.FeeTransactionValidator
import io.constellationnetwork.node.shared.infrastructure.snapshot.{CurrencyMessageValidator, GlobalSnapshotSyncValidator}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{GlobalSnapshotsProcessed, SharedArtifact, TokenUnlock}
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.currencyMessage.CurrencyMessage
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.{RewardTransaction, Transaction, TransactionReference}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.security.{Hashed, Hasher}
import io.constellationnetwork.syntax.sortedCollection.{sortedMapSyntax, sortedSetSyntax}

import eu.timepit.refined.auto.autoUnwrap
import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait CurrencySnapshotAcceptanceManager[F[_]] {
  def accept(
    blocksForAcceptance: List[Signed[Block]],
    tokenLockBlocksForAcceptance: List[Signed[TokenLockBlock]],
    allowSpendBlocksForAcceptance: List[Signed[AllowSpendBlock]],
    messagesForAcceptance: List[Signed[CurrencyMessage]],
    feeTransactionsForAcceptance: Option[SortedSet[Signed[FeeTransaction]]],
    globalSnapshotSyncsForAcceptance: List[Signed[GlobalSnapshotSync]],
    sharedArtifactsForAcceptance: SortedSet[SharedArtifact],
    lastSnapshotContext: CurrencySnapshotContext,
    snapshotOrdinal: SnapshotOrdinal,
    epochProgress: EpochProgress,
    lastActiveTips: SortedSet[ActiveTip],
    lastDeprecatedTips: SortedSet[DeprecatedTip],
    calculateRewardsFn: SortedSet[Signed[Transaction]] => F[SortedSet[RewardTransaction]],
    facilitators: Set[PeerId],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    lastGlobalSyncView: Option[GlobalSyncView],
    shouldPerformMetagraphSpecificValidations: Boolean,
    lastArtifactProofs: NonEmptySet[SignatureProof],
    // blocker-1a — set-valued in-band P. The union of `GlobalSnapshotsProcessed.ordinals` over the metagraph's retained CL0 chain,
    // reconstructed by the caller (see `GlobalSnapshotOpsManager.reconstructProcessedGlobalOrdinals`). Replaces the former node-local
    // `globalSnapshotsAlreadyProcessed` mutable cache. Gates which cross-shard `SpendAction`s are (re-)applied:
    // `A = { o ∈ U : o ≤ view ∧ o ∉ P }`. Because `P` is now a pure input, `accept` no longer depends on manager-instance-local state.
    alreadyProcessedGlobalOrdinals: SortedSet[SnapshotOrdinal],
    // Validator-only execution pin. The submitted view selects an ordinal, but its
    // hash and epoch must match finalized GL0 before any transition is recreated.
    // Producers pass None; GL0 validators supply the view committed by the binary.
    pinnedGlobalSyncView: Option[GlobalSyncView] = None
  )(implicit hasher: Hasher[F]): F[CurrencySnapshotAcceptanceResult]

  def acceptRewardTxs(
    baseBalances: SortedMap[Address, Balance],
    newUpdatedBalance: Map[Address, Balance],
    rewards: SortedSet[RewardTransaction]
  ): F[(SortedMap[Address, Balance], SortedSet[RewardTransaction])]
}

object CurrencySnapshotAcceptanceManager {

  private[currency] def filterFrameworkGeneratedArtifacts(
    supplied: SortedSet[SharedArtifact]
  ): SortedSet[SharedArtifact] = supplied.filter {
    case _: GlobalSnapshotsProcessed => false
    case _                           => true
  }

  /** Select the GL0 ordinal the CL0 producer will sync to when stamping the next CL0 snapshot.
    *
    * Priority (producer path, when `pinnedGlobalSyncView=None`): (A) peer `GlobalSnapshotSync` quorum (`maybeSnapshotOrdinalSync`) — when
    * present, it is the consensus-derived sync point and wins outright (committee-of-peers can legitimately be ahead of the local follower
    * at small CL0 cohort sizes). (B) prior CL0 snapshot's `globalSyncView.ordinal` (`maybeLastGlobalSyncView`) — a monotonic lower bound
    * that protects against local-follower regression on reorg. (C) the producer's actual GL0 head (`fallbackOrdinal =
    * lastUnsyncGlobalSnapshot.ordinal`) under finality, this is always reachable and advances monotonically.
    *
    * Bug fixed by this helper (see docs/nakamoto/MODE2-GLOBAL-SYNC-VIEW-RCA.md): The previous priority chain preferred (B) over (C)
    * outright via `.orElse`, which created a strict fixed point at the genesis-inherited `GlobalSyncView(ord=1, epochProgress=1)`. Once
    * seeded by `mkFirstIncrementalSnapshot`, the producer could not escape it without peer-sync quorum (A) materializing — which is racy at
    * small ml0 cohorts. Symptom: cl1 reports `currentEpochProgress=1` indefinitely; AllowSpend rejects with
    * `TooFarLastValidEpochProgress{epochProgress=<N>, currentEpochProgress=1}`.
    *
    * Fix: when (A) is None, take `max((B), (C))` rather than `(B).orElse((C))`. This preserves (B)'s lower-bound semantics for the rare
    * local-regression case while letting the producer escape genesis once `lastUnsyncGlobalSnapshot.ordinal` advances.
    *
    * `MinValue` (ord=0) on path (B) is treated as "no prior view"; pre-fix logic already filtered this out and the helper retains that to
    * avoid using the `Ord.MinValue` sentinel as a real ordinal.
    */
  private[currency] def selectOrdinalToFetchGlobalSnapshot(
    pinnedGlobalSyncView: Option[GlobalSyncView],
    maybeSnapshotOrdinalSync: Option[SnapshotOrdinal],
    maybeLastGlobalSyncView: Option[GlobalSyncView],
    fallbackOrdinal: SnapshotOrdinal
  ): SnapshotOrdinal =
    pinnedGlobalSyncView.map(_.ordinal) match {
      case Some(pinned) => pinned
      case None =>
        maybeSnapshotOrdinalSync match {
          case Some(peerSync) => peerSync
          case None =>
            val priorOrdinal =
              maybeLastGlobalSyncView.map(_.ordinal).filter(_ =!= SnapshotOrdinal.MinValue)
            priorOrdinal match {
              case Some(prior) => if (prior >= fallbackOrdinal) prior else fallbackOrdinal
              case None        => fallbackOrdinal
            }
        }
    }

  def make[F[_]: Async: Parallel: JsonSerializer](
    fieldsAddedOrdinals: FieldsAddedOrdinals,
    environment: AppEnvironment,
    lastGlobalSnapshotsSyncConfig: LastGlobalSnapshotsSyncConfig,
    blockAcceptanceManager: BlockAcceptanceManager[F],
    tokenLockBlockAcceptanceManager: TokenLockBlockAcceptanceManager[F],
    allowSpendBlockAcceptanceManager: AllowSpendBlockAcceptanceManager[F],
    collateral: Amount,
    messageValidator: CurrencyMessageValidator[F],
    feeTransactionValidator: FeeTransactionValidator[F],
    globalSnapshotSyncValidator: GlobalSnapshotSyncValidator[F],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    // Track-1 I-PIN (Step-1 read-at-anchor). The version-retained, BY-ORDINAL per-metagraph pinned reader (blocker-2a). On the
    // VALIDATOR / re-exec path (`pinnedGlobalSyncView` set) `accept` reads the metagraph's gl0-committed cross-shard sync data at the
    // RECORDED `globalSyncView` through this reader instead of the non-deterministic node-local head. `None` (tests, or a rail with no
    // per-ordinal global byte store such as the currency-l0 producer) ⇒ the producer path keeps its head read (byte-identical to today)
    // and the validator path folds no cross-shard sync data (deterministic). Prod wires it in `SharedServices.make` over the same
    // `mpt_snapshot_info` byte store that backs the GSAM reader.
    pinnedCurrencyInfoReader: Option[PinnedCurrencyInfoReader[F]] = None
  )(
    implicit currencyStateProofSelector: CurrencyStateProofSelector
  ): F[CurrencySnapshotAcceptanceManager[F]] =
    for {

      // Holds a cache of the most recent GlobalIncrementalSnapshots by their SnapshotOrdinal.
      // Used to avoid redundant network calls and repeated deserialization of global snapshots
      // when multiple currency snapshots are being processed concurrently or in sequence.
      lastGlobalSnapshotsCached <- SignallingRef.of[F, Map[SnapshotOrdinal, Hashed[GlobalIncrementalSnapshot]]](Map.empty)

      // NOTE (blocker-1a): the former node-local `globalSnapshotsAlreadyProcessed` cache (which tracked, per metagraph, which global
      // snapshot ordinals had already been processed so their SpendActions were not re-applied) has been removed. That set (`P`) is now
      // reconstructed IN-BAND from the retained CL0 chain (`⋃ GlobalSnapshotsProcessed.ordinals`) and passed into `accept` as
      // `alreadyProcessedGlobalOrdinals`. This eliminates a mutable, manager-instance-local, node-dependent input to consensus acceptance.

      // Initialize operational components
      blockOps = BlockAcceptanceOpsManager.make[F](
        blockAcceptanceManager,
        tokenLockBlockAcceptanceManager,
        allowSpendBlockAcceptanceManager,
        collateral
      )

      messageOps = MessageValidationOpsManager.make[F](
        messageValidator,
        globalSnapshotSyncValidator
      )

      globalSnapshotOps = GlobalSnapshotOpsManager.make[F](
        lastGlobalSnapshotsSyncConfig,
        lastGlobalSnapshotsCached
      )

      allowSpendOps = AllowSpendOpsManager.make[F]
      tokenLockOps = TokenLockOpsManager.make[F]
      balanceOps = BalanceOpsManager.make[F](feeTransactionValidator)

    } yield
      new CurrencySnapshotAcceptanceManagerImpl[F](
        fieldsAddedOrdinals,
        environment,
        lastGlobalSnapshotsSyncConfig,
        lastNGlobalSnapshotStorage,
        lastGlobalSnapshotStorage,
        blockOps,
        messageOps,
        globalSnapshotOps,
        allowSpendOps,
        tokenLockOps,
        balanceOps,
        pinnedCurrencyInfoReader
      )
}

/** Main implementation with parallelized operations for improved performance
  */
private class CurrencySnapshotAcceptanceManagerImpl[F[_]: Async: Parallel: JsonSerializer](
  fieldsAddedOrdinals: FieldsAddedOrdinals,
  environment: AppEnvironment,
  lastGlobalSnapshotsSyncConfig: LastGlobalSnapshotsSyncConfig,
  lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
  lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
  blockOps: BlockAcceptanceOpsManager[F],
  messageOps: MessageValidationOpsManager[F],
  globalSnapshotOps: GlobalSnapshotOpsManager[F],
  allowSpendOps: AllowSpendOpsManager[F],
  tokenLockOps: TokenLockOpsManager[F],
  balanceOps: BalanceOpsManager[F],
  pinnedCurrencyInfoReader: Option[PinnedCurrencyInfoReader[F]]
)(implicit currencyStateProofSelector: CurrencyStateProofSelector)
    extends CurrencySnapshotAcceptanceManager[F] {

  // [OVERPRUNE-DIAG] (2026-06-13, REMOVE after e2e): only used by the epoch-divergence diagnostic in `accept`.
  private val logger = Slf4jLogger.getLoggerFromName[F]("CurrencySnapshotAcceptanceManager")

  def accept(
    blocksForAcceptance: List[Signed[Block]],
    tokenLockBlocksForAcceptance: List[Signed[TokenLockBlock]],
    allowSpendBlocksForAcceptance: List[Signed[AllowSpendBlock]],
    messagesForAcceptance: List[Signed[CurrencyMessage]],
    feeTransactionsForAcceptance: Option[SortedSet[Signed[FeeTransaction]]],
    globalSnapshotSyncsForAcceptance: List[Signed[GlobalSnapshotSync]],
    sharedArtifactsForAcceptance: SortedSet[SharedArtifact],
    lastSnapshotContext: CurrencySnapshotContext,
    snapshotOrdinal: SnapshotOrdinal,
    epochProgress: EpochProgress,
    lastActiveTips: SortedSet[ActiveTip],
    lastDeprecatedTips: SortedSet[DeprecatedTip],
    calculateRewardsFn: SortedSet[Signed[Transaction]] => F[SortedSet[RewardTransaction]],
    facilitators: Set[PeerId],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    maybeLastGlobalSyncView: Option[GlobalSyncView],
    shouldPerformMetagraphSpecificValidations: Boolean,
    lastArtifactProofs: NonEmptySet[SignatureProof],
    alreadyProcessedGlobalOrdinals: SortedSet[SnapshotOrdinal],
    pinnedGlobalSyncView: Option[GlobalSyncView] = None
  )(implicit hasher: Hasher[F]): F[CurrencySnapshotAcceptanceResult] = for {
    initialTxRef <- TransactionReference.emptyCurrency(lastSnapshotContext.address)
    tokenLockInitialTxRef <- TokenLockReference.emptyCurrency(lastSnapshotContext.address)
    initialAllowSpendRef <- AllowSpendReference.emptyCurrency(lastSnapshotContext.address)
    metagraphId = lastSnapshotContext.address

    checkSyncGlobalSnapshotField = fieldsAddedOrdinals.checkSyncGlobalSnapshotField
      .getOrElse(environment, SnapshotOrdinal.MinValue)
    tessellation3MigrationStartingOrdinal = fieldsAddedOrdinals.tessellation3Migration
      .getOrElse(environment, SnapshotOrdinal.MinValue)
    metagraphSyncDataStartingOrdinal = fieldsAddedOrdinals.metagraphSyncData
      .getOrElse(environment, SnapshotOrdinal.MinValue)
    updatedLastSyncGlobalOrder = fieldsAddedOrdinals.updatedLastSyncGlobalOrder
      .getOrElse(environment, SnapshotOrdinal.MinValue)
    updatedLastSyncGlobalFromPeersInConsensus = fieldsAddedOrdinals.updatedLastSyncGlobalFromPeersInConsensus
      .getOrElse(environment, SnapshotOrdinal.MinValue)
    fixingAllowSpendExpiration = fieldsAddedOrdinals.fixingAllowSpendExpiration
      .getOrElse(environment, SnapshotOrdinal.MinValue)
    fixingAllowSpendAndTokenLockValidation = fieldsAddedOrdinals.fixingAllowSpendAndTokenLockValidation
      .getOrElse(environment, SnapshotOrdinal.MinValue)

    acceptanceBlocksResult <- blockOps.acceptBlocks(
      blocksForAcceptance,
      lastSnapshotContext,
      snapshotOrdinal,
      lastActiveTips,
      lastDeprecatedTips,
      initialTxRef,
      shouldPerformMetagraphSpecificValidations
    )

    acceptedTransactions = acceptanceBlocksResult.accepted.flatMap {
      case (block, _) =>
        block.value.transactions.toSortedSet
    }.toSortedSet

    transactionsRefs = blockOps.acceptTransactionRefs(
      lastSnapshotContext.snapshotInfo.lastTxRefs,
      acceptanceBlocksResult.contextUpdate.lastTxRefs,
      acceptedTransactions,
      initialTxRef
    )

    rewards <- calculateRewardsFn(acceptedTransactions)

    (updatedBalancesByRewards, acceptedRewardTxs) <- acceptRewardTxs(
      lastSnapshotContext.snapshotInfo.balances,
      acceptanceBlocksResult.contextUpdate.balances,
      rewards
    )

    _ <- balanceOps.validateFeeTxs(feeTransactionsForAcceptance)

    (updatedBalancesByFeeTransactions, acceptedFeeTxs) <- balanceOps.acceptFeeTxs(
      updatedBalancesByRewards,
      feeTransactionsForAcceptance
    )

    // GlobalSnapshotsProcessed is a framework acknowledgement, not DL1-provided application data. Recompute it below from the exact
    // consensus-pinned GL0 ordinals this acceptance actually folds. Accepting a supplied value verbatim would let a producer acknowledge an
    // ordinal without applying its SpendActions, causing GL0 to retire the cross-shard balance overlay against stale raw currency state.
    acceptedSharedArtifacts = CurrencySnapshotAcceptanceManager.filterFrameworkGeneratedArtifacts(sharedArtifactsForAcceptance)
    maybeUnsyncLastGlobalSnapshot <- lastGlobalSnapshotStorage.getCombined

    (lastUnsyncGlobalSnapshot, lastUnsyncGlobalSnapshotInfo) <- OptionT
      .fromOption(maybeUnsyncLastGlobalSnapshot)
      .getOrRaise(new IllegalStateException("Could not get the last global snapshot info"))

    // NOTE (Track-1 I-PIN Step-1 scope): `lastUnsyncBalances` / `lastUnsyncLastCurrencySnapshots` feed ONLY currency-message validation
    // (`acceptMessages`) and are the GLOBAL cross-metagraph maps (balances of every address; every metagraph's fee/message addresses for the
    // cross-metagraph address-uniqueness check) — NOT reconstructible from `pinnedCurrencyInfoReader`'s per-metagraph `CurrencySnapshotInfo`.
    // They are kept on the head read in this slice; a message-bearing snapshot's `lastMessages` field therefore remains head-influenced on
    // the validator path (a residual purity gap, out of scope here — the forcing test is message-free). `lastUnsyncMetagraphSyncData` is used
    // ONLY on the PRODUCER path below (`pinnedGlobalSyncView` empty); the validator path reads the metagraph's sync data at the pinned anchor.
    lastUnsyncBalances = lastUnsyncGlobalSnapshotInfo.balances
    lastUnsyncLastCurrencySnapshots = lastUnsyncGlobalSnapshotInfo.lastCurrencySnapshots
    lastUnsyncMetagraphSyncData = lastUnsyncGlobalSnapshotInfo.metagraphSyncData

    parallelResults <- (
      messageOps.acceptMessages(
        lastSnapshotContext.snapshotInfo.lastMessages,
        messagesForAcceptance,
        lastSnapshotContext.address,
        snapshotOrdinal,
        lastUnsyncBalances,
        lastUnsyncLastCurrencySnapshots,
        shouldPerformMetagraphSpecificValidations
      ),
      messageOps.acceptGlobalSnapshotSyncs(
        lastSnapshotContext.snapshotInfo.globalSnapshotSyncView,
        globalSnapshotSyncsForAcceptance,
        lastSnapshotContext.address,
        facilitators
      )
    ).parMapN((messages, syncs) => (messages, syncs))

    (messagesAcceptanceResult, globalSnapshotSyncAcceptanceResult) = parallelResults

    fallbackOrdinal = lastUnsyncGlobalSnapshot.ordinal

    lastPeersParticipatedOnConsensus = lastArtifactProofs.map(_.id.toPeerId)
    peersToGetSnapshotOrdinalSync =
      if (lastUnsyncGlobalSnapshot.ordinal > updatedLastSyncGlobalFromPeersInConsensus) {
        globalSnapshotSyncAcceptanceResult.contextUpdate.filter {
          case (peerId, _) =>
            lastPeersParticipatedOnConsensus.contains(peerId)
        }
      } else {
        globalSnapshotSyncAcceptanceResult.contextUpdate
      }

    maybeSnapshotOrdinalSync = peersToGetSnapshotOrdinalSync.values
      .map(_.globalSnapshotOrdinal)
      .groupBy(identity)
      .maxByOption {
        case (ordinal, occurrences) =>
          if (lastUnsyncGlobalSnapshot.ordinal > updatedLastSyncGlobalOrder) {
            (occurrences.size, ordinal.value.value)
          } else {
            (occurrences.size, -ordinal.value.value)
          }
      }
      .flatMap { case (ordinal, _) => SnapshotOrdinal(ordinal.value - lastGlobalSnapshotsSyncConfig.syncOffset) }

    lastGlobalSnapshots <- lastNGlobalSnapshotStorage.getLastN

    // Validator path: re-run acceptance against the exact GL0 input committed by the
    // submitted snapshot. The ordinal is only a lookup key; finalized hash and epoch
    // equality below are mandatory execution-input checks.
    //
    // Producer path: see CurrencySnapshotAcceptanceManager.selectOrdinalToFetchGlobalSnapshot
    // for the priority semantics (peer-sync > max(prior-view, local-head)). Mode-2 bug fix
    // documented in docs/nakamoto/MODE2-GLOBAL-SYNC-VIEW-RCA.md.
    ordinalToFetchGlobalSnapshot = CurrencySnapshotAcceptanceManager.selectOrdinalToFetchGlobalSnapshot(
      pinnedGlobalSyncView,
      maybeSnapshotOrdinalSync,
      maybeLastGlobalSyncView,
      fallbackOrdinal
    )

    lastSyncGlobalSnapshot <- lastGlobalSnapshots.find(_.ordinal === ordinalToFetchGlobalSnapshot) match {
      case Some(value) => value.pure[F]
      case None        => globalSnapshotOps.getGlobalSnapshotWithRetry(ordinalToFetchGlobalSnapshot, getGlobalSnapshotByOrdinal)
    }

    // The committed view is only an execution-input locator. The located snapshot must exactly match finalized GL0 history; submitted hash
    // and epoch values are never copied into recreated state.
    _ <- pinnedGlobalSyncView.traverse_ { pinned =>
      Async[F].raiseWhen(
        lastSyncGlobalSnapshot.hash =!= pinned.hash || lastSyncGlobalSnapshot.epochProgress =!= pinned.epochProgress
      )(
        new IllegalArgumentException(
          s"Pinned globalSyncView does not match finalized GL0 history at ordinal ${pinned.ordinal.show}: " +
            s"pinnedHash=${pinned.hash.show} finalizedHash=${lastSyncGlobalSnapshot.hash.show} " +
            s"pinnedEpoch=${pinned.epochProgress.show} finalizedEpoch=${lastSyncGlobalSnapshot.epochProgress.show}"
        )
      )
    }

    // Track-1 I-PIN (Step-1): `updateGlobalSnapshotCache` REMOVED from the consensus path — populating a manager-instance-local cache
    // from acceptance made `accept()` self-mutating (an impurity). The last-resort cache read in `getGlobalSnapshotWithRetry` still works;
    // it simply no longer depends on a side effect of a prior `accept()`.

    lastGlobalSnapshotEpochProgress = lastSyncGlobalSnapshot.epochProgress
    lastGlobalSnapshotOrdinal = lastSyncGlobalSnapshot.ordinal

    finalizedFetchedView = GlobalSyncView(
      lastSyncGlobalSnapshot.ordinal,
      lastSyncGlobalSnapshot.hash,
      lastSyncGlobalSnapshot.epochProgress
    )
    globalSyncView = pinnedGlobalSyncView.fold(
      maybeLastGlobalSyncView
        .filter(_.ordinal >= lastSyncGlobalSnapshot.ordinal)
        .getOrElse(finalizedFetchedView)
    )(_ => finalizedFetchedView)

    // [OVERPRUNE-DIAG/ml0] (2026-06-13, REMOVE after e2e): the smoking gun for run-26 divergence #1. ml0 EXPIRES allow-spends/
    // token-locks with `lastGlobalSnapshotEpochProgress` (the synced snapshot's epoch at `ordinalToFetch`), but ADVERTISES
    // `globalSyncView.epochProgress` (monotonic-clamped, can be the higher prior view when peer-sync backstepped below it). gl0's
    // mirror reads the advertised epoch → over-prunes when these diverge. This fires ONLY on divergence; absence ⇒ #1 didn't fire.
    _ <- Async[F].whenA(globalSyncView.epochProgress =!= lastGlobalSnapshotEpochProgress)(
      logger.info(
        s"[OVERPRUNE-DIAG/ml0] mg=${metagraphId.show.take(10)} cl0Ord=${snapshotOrdinal.show} EPOCH-DIVERGENCE " +
          s"expiryEpoch=${lastGlobalSnapshotEpochProgress.value.value} advertisedEpoch=${globalSyncView.epochProgress.value.value} " +
          s"syncedOrd=${lastSyncGlobalSnapshot.ordinal.show} advertisedOrd=${globalSyncView.ordinal.show} " +
          s"peerSync=${maybeSnapshotOrdinalSync.map(_.show).getOrElse("none")} " +
          s"prior=${maybeLastGlobalSyncView.map(_.ordinal.show).getOrElse("none")} fallback=${fallbackOrdinal.show}"
      )
    )

    blockAcceptanceResults <- (
      blockOps.acceptTokenLockBlocks(
        tokenLockBlocksForAcceptance,
        lastSnapshotContext,
        snapshotOrdinal,
        tokenLockInitialTxRef,
        shouldPerformMetagraphSpecificValidations,
        // Track-1 I-PIN (Step-1): feature-activation gate keyed off the RECORDED `globalSyncView`, not the node-local head, so the
        // producer and every re-executing validator apply the identical validation rules (producer==validator determinism).
        globalSyncView.ordinal,
        fixingAllowSpendAndTokenLockValidation,
        lastGlobalSnapshotEpochProgress
      ),
      blockOps.acceptAllowSpendBlocks(
        allowSpendBlocksForAcceptance,
        lastSnapshotContext,
        snapshotOrdinal,
        initialAllowSpendRef,
        shouldPerformMetagraphSpecificValidations,
        // Track-1 I-PIN (Step-1): feature-activation gate keyed off the RECORDED `globalSyncView` (see acceptTokenLockBlocks above).
        globalSyncView.ordinal,
        fixingAllowSpendAndTokenLockValidation,
        lastGlobalSnapshotEpochProgress
      )
    ).parMapN((tokenLock, allowSpend) => (tokenLock, allowSpend))

    (acceptanceTokenLockBlocksResult, allowSpendBlockAcceptanceResult) = blockAcceptanceResults

    lastAllowSpendsRefs = lastSnapshotContext.snapshotInfo.lastAllowSpendRefs.getOrElse(SortedMap.empty[Address, AllowSpendReference])

    updatedAllowSpendRefs = blockOps.acceptAllowSpendRefs(
      lastAllowSpendsRefs,
      allowSpendBlockAcceptanceResult.contextUpdate.lastTxRefs
    )

    tokenLockRefs = blockOps.acceptTokenLockRefs(
      lastSnapshotContext.snapshotInfo.lastTokenLockRefs.getOrElse(SortedMap.empty[Address, TokenLockReference]),
      acceptanceTokenLockBlocksResult.contextUpdate.lastTokenLocksRefs
    )

    // Track-1 I-PIN (Step-1) READ-AT-ANCHOR: the cross-shard sync data (`U` = the metagraph's unapplied global-change ordinals) that gates
    // which SpendActions this snapshot folds MUST be read at the RECORDED `globalSyncView`, never the node-local head. Otherwise two
    // validators whose heads advertise different `metagraphSyncData` (one having advanced past the pinned view) fold different cross-shard
    // state and compute divergent `balancesProof` — exactly the forcing-test impurity. Rules:
    //   - PRODUCER path (`pinnedGlobalSyncView` empty): keep the head read. The producer DEFINES the view it records, and the currency-l0
    //     producer has no per-ordinal global byte store to re-read from ⇒ byte-identical to pre-I-PIN, preserving cross-shard on produce.
    //   - VALIDATOR / re-exec path (`pinnedGlobalSyncView` set): read the metagraph's fieldId-18 sync data at the pinned anchor through
    //     `pinnedCurrencyInfoReader` (hard-reject/`None` on hash-pin miss or retention eviction — NEVER a head fallback). A `None` reader
    //     (tests, or a rail with no global byte store) folds no cross-shard sync data — deterministic and head-independent.
    anchorMetagraphSyncData <- pinnedGlobalSyncView match {
      case None => lastUnsyncMetagraphSyncData.pure[F]
      case Some(_) =>
        pinnedCurrencyInfoReader match {
          case Some(reader) =>
            reader
              .readMetagraphSyncDataAt(globalSyncView.ordinal, globalSyncView.hash, metagraphId)
              .map(_.map(info => SortedMap(metagraphId -> info)))
          case None => none[SortedMap[Address, MetagraphSyncDataInfo]].pure[F]
        }
    }

    (globalSnapshotsSpendActions, globalSnapshotsProcessed) <- globalSnapshotOps.getLastGlobalSnapshotsSpendActions(
      globalSyncView.ordinal,
      lastGlobalSnapshots,
      getGlobalSnapshotByOrdinal,
      metagraphId,
      anchorMetagraphSyncData,
      alreadyProcessedGlobalOrdinals
    )

    metagraphIdSpendTransactions = globalSnapshotsSpendActions.flatMap {
      case (_, spendActions) =>
        spendActions
          .flatMap(_.spendTransactions.toList)
          .filter(_.currencyId.exists(_.value == metagraphId))
    }.toList

    incomingTokenLocks = acceptanceTokenLockBlocksResult.accepted.flatMap { tokenLockBlock =>
      tokenLockBlock.value.tokenLocks.toSortedSet
    }.toSortedSet

    activeTokenLocks = lastSnapshotContext.snapshotInfo.activeTokenLocks.getOrElse(SortedMap.empty[Address, SortedSet[Signed[TokenLock]]])

    tokenLocksByRef <-
      (incomingTokenLocks.toList ++ activeTokenLocks.values.flatten)
        .traverse(tokenLock => tokenLock.toHashed.map(hashed => hashed.hash -> tokenLock.value))
        .map(_.toMap)

    tokenUnlocks = acceptedSharedArtifacts.collect {
      case tokenUnlock: TokenUnlock => tokenUnlock
    }

    expiredTokenLocksHashes <-
      (incomingTokenLocks.toList ++ activeTokenLocks.values.flatten)
        .filter(_.unlockEpoch.exists(_ < lastGlobalSnapshotEpochProgress))
        .traverse(_.toHashed)
        .map(_.map(_.hash))

    acceptedTokenUnlocks = tokenLockOps.acceptTokenUnlocks(
      expiredTokenLocksHashes,
      tokenUnlocks,
      tokenLocksByRef
    )

    acceptedTokenLocks = incomingTokenLocks
      .filter(itl => itl.unlockEpoch.forall(_ >= lastGlobalSnapshotEpochProgress))
      .groupBy(_.source)
      .toSortedMap

    (updatedActiveTokenLocks, expiredTokenLocks) <- tokenLockOps.acceptTokenLocks(
      lastGlobalSnapshotEpochProgress,
      acceptedTokenLocks,
      activeTokenLocks,
      acceptedTokenUnlocks
    )

    updatedBalancesByTokenLocks = tokenLockOps.updateBalancesByTokenLocks(
      lastGlobalSnapshotEpochProgress,
      updatedBalancesByFeeTransactions,
      acceptedTokenLocks,
      activeTokenLocks,
      acceptedTokenUnlocks
    ) match {
      case Right(balances) => balances
      case Left(error)     => throw new RuntimeException(s"Balance arithmetic error updating balances by token locks: $error")
    }

    acceptedCurrencyAllowSpends = allowSpendBlockAcceptanceResult.accepted.flatMap(_.value.transactions.toList)
    incomingCurrencyAllowSpends = acceptedCurrencyAllowSpends
      .groupBy(_.value.source)
      .view
      .mapValues(SortedSet.from(_))
      .to(SortedMap)

    lastActiveAllowSpends = lastSnapshotContext.snapshotInfo.activeAllowSpends.getOrElse(
      SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]]
    )

    updatedAllowSpends <- allowSpendOps.acceptCurrencyAllowSpends(
      lastGlobalSnapshotEpochProgress,
      incomingCurrencyAllowSpends,
      lastActiveAllowSpends,
      metagraphIdSpendTransactions,
      // Track-1 I-PIN (Step-1): allow-spend expiration feature gate keyed off the RECORDED `globalSyncView`, not the node-local head.
      globalSyncView.ordinal,
      fixingAllowSpendExpiration
    )

    updatedBalancesByAllowSpends <- allowSpendOps
      .updateCurrencyBalancesByAllowSpends(
        lastGlobalSnapshotEpochProgress,
        updatedBalancesByTokenLocks,
        incomingCurrencyAllowSpends,
        lastActiveAllowSpends,
        metagraphIdSpendTransactions,
        // Track-1 I-PIN (Step-1): allow-spend expiration feature gate keyed off the RECORDED `globalSyncView` (see above).
        globalSyncView.ordinal,
        fixingAllowSpendExpiration
      )
      .flatMap {
        case Right(balances) => balances.pure[F]
        case Left(error) =>
          new RuntimeException(s"Balance arithmetic error updating balances by allow spends: $error")
            .raiseError[F, SortedMap[Address, Balance]]
      }

    allActiveCurrencyAllowSpends <- (incomingCurrencyAllowSpends |+| lastActiveAllowSpends).toList.traverse {
      case (address, allowSpends) =>
        allowSpends.toList.traverse(_.toHashed).map(hashedAllowSpends => address -> hashedAllowSpends)
    }.map(_.toSortedMap)

    updatedBalancesBySpendTransactions = allowSpendOps.updateCurrencyBalancesBySpendTransactions(
      updatedBalancesByAllowSpends,
      allActiveCurrencyAllowSpends,
      metagraphIdSpendTransactions
    ) match {
      case Right(balances) => balances
      case Left(error)     => throw new RuntimeException(s"Balance arithmetic error updating balances by spend transactions: $error")
    }

    updatedAllowSpendsCleaned = updatedAllowSpends.filter { case (_, allowSpends) => allowSpends.nonEmpty }
    updatedActiveTokenLocksCleaned = updatedActiveTokenLocks.filter { case (_, tokenLocks) => tokenLocks.nonEmpty }

    // Track-1 I-PIN (Step-1) BLOCKER-1b: the optional-CSI-field emission gate keyed off the RECORDED `globalSyncView`, not the node-local
    // head — so which fields a snapshot emits at a feature boundary (fields-added-ordinals in application.conf) is a pure function of the
    // recorded view and identical across nodes with divergent heads. On the validator/re-exec path `globalSyncView.ordinal` == the fetched
    // `lastGlobalSnapshotOrdinal` (both are the pinned ordinal), so the else-branches (which already read the anchor-derived
    // `lastGlobalSnapshotOrdinal`) stay head-independent. See `CurrencySnapshotFieldEmissionBoundarySuite` for the per-boundary identity proof.
    snapshotOrdinalToCheckFields =
      if (globalSyncView.ordinal > metagraphSyncDataStartingOrdinal) {
        globalSyncView.ordinal
      } else if (lastGlobalSnapshotOrdinal <= checkSyncGlobalSnapshotField) {
        lastGlobalSnapshotOrdinal
      } else {
        val fallbackOrdinal = lastGlobalSnapshots.lastOption
          .map(_.ordinal)
          .getOrElse(maybeLastGlobalSyncView.map(_.ordinal).getOrElse(SnapshotOrdinal.MinValue))
        if (lastGlobalSnapshotOrdinal === SnapshotOrdinal.MinValue) fallbackOrdinal
        else lastGlobalSnapshotOrdinal
      }

    csi = CurrencySnapshotInfo(
      if (snapshotOrdinalToCheckFields < tessellation3MigrationStartingOrdinal)
        lastSnapshotContext.snapshotInfo.lastTxRefs ++ acceptanceBlocksResult.contextUpdate.lastTxRefs
      else transactionsRefs,
      updatedBalancesBySpendTransactions,
      Option.when(messagesAcceptanceResult.contextUpdate.nonEmpty)(messagesAcceptanceResult.contextUpdate),
      None,
      if (snapshotOrdinalToCheckFields < tessellation3MigrationStartingOrdinal) none else updatedAllowSpendRefs.some,
      if (snapshotOrdinalToCheckFields < tessellation3MigrationStartingOrdinal) none else updatedAllowSpendsCleaned.some,
      if (snapshotOrdinalToCheckFields < tessellation3MigrationStartingOrdinal) none
      else globalSnapshotSyncAcceptanceResult.contextUpdate.some,
      if (snapshotOrdinalToCheckFields < tessellation3MigrationStartingOrdinal) none else tokenLockRefs.some,
      if (snapshotOrdinalToCheckFields < tessellation3MigrationStartingOrdinal) none else updatedActiveTokenLocksCleaned.some
    )

    stateProof <- csi.stateProof(snapshotOrdinal)

    events <- (
      allowSpendOps
        .filterExpiredAllowSpends(
          lastActiveAllowSpends,
          lastGlobalSnapshotEpochProgress,
          metagraphIdSpendTransactions,
          // Track-1 I-PIN (Step-1): allow-spend expiration feature gate keyed off the RECORDED `globalSyncView`, not the node-local head.
          globalSyncView.ordinal,
          fixingAllowSpendExpiration
        )
        .flatMap(allowSpendOps.emitAllowSpendsExpired),
      tokenLockOps.emitTokenUnlocks(acceptedTokenUnlocks, expiredTokenLocks)
    ).parMapN((allowSpendEvents, tokenLockEvents) => (allowSpendEvents, tokenLockEvents))

    (allowSpendsExpiredEvents, tokenUnlocksEvents) = events

    maybeGlobalSnapshotProcessedEvent: SortedSet[SharedArtifact] =
      if (globalSnapshotsProcessed.nonEmpty)
        SortedSet(GlobalSnapshotsProcessed(globalSnapshotsProcessed))
      else
        SortedSet.empty[SharedArtifact]

  } yield
    CurrencySnapshotAcceptanceResult(
      acceptanceBlocksResult,
      acceptanceTokenLockBlocksResult,
      allowSpendBlockAcceptanceResult,
      messagesAcceptanceResult,
      globalSnapshotSyncAcceptanceResult,
      acceptedRewardTxs,
      acceptedSharedArtifacts ++ allowSpendsExpiredEvents ++ tokenUnlocksEvents ++ maybeGlobalSnapshotProcessedEvent,
      acceptedFeeTxs,
      csi,
      stateProof,
      globalSyncView,
      lastGlobalSnapshotOrdinal,
      snapshotOrdinalToCheckFields
    )

  def acceptRewardTxs(
    baseBalances: SortedMap[Address, Balance],
    newUpdatedBalance: Map[Address, Balance],
    rewards: SortedSet[RewardTransaction]
  ): F[(SortedMap[Address, Balance], SortedSet[RewardTransaction])] =
    balanceOps.acceptRewardTxs(baseBalances, newUpdatedBalance, rewards)
}
