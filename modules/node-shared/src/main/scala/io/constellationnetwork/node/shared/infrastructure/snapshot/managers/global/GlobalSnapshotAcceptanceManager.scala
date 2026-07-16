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
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertValidator.RegistrationEvaluationContext
import io.constellationnetwork.node.shared.domain.nakamoto.kes._
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps._
import io.constellationnetwork.node.shared.domain.nakamoto.overlay._
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProofClient
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashManager.SlashedRegistryEntry
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.{InvalidStateProofSlashManager, InvalidStateProofSlashedReader}
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
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.kes.KesRegistrationStateManager
import io.constellationnetwork.node.shared.logger.LoggerBundle
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact._
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationRecord, KesRegistrationReference}
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
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.{PhysicalTrieKeyValidator, StatefulMerklePatriciaProducer}
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
    // At numShards > 1, checkpoint windows are admitted only after `verifyEmbedded` globally re-executes every included CL1 chain and
    // matches every claimed per-MG root. The windows then run through the same full state-channel validation and currency transition again
    // inside this acceptance pass. Committee diffs, receipts, artifacts, and signature counts cannot supply economic state.
    shardCheckpoints: SortedMap[ShardId, ShardCheckpoint] = SortedMap.empty,
    // Downstream replay of a finalized GL0 artifact cannot reconstruct period-N eta without GL0's VRF ancestry. At an eta boundary, use the
    // signed artifact's consensus-pinned eta so the follower recreates the producer's HistoricalStakeSnapshot bytes. GL0 production leaves
    // this None and derives eta locally. This is finalized GL0 metadata flowing downstream, not a CL1 economic-state override.
    pinnedBoundaryEta: Option[Hash] = None,
    // WATCHTOWER fraud-proof CONSENSUS ARTIFACT (W3a). The canonical `SortedSet` of UPHELD fraud proofs the gl0 leader embedded in the
    // produced snapshot's `fraudProofs` field, threaded back here on EVERY path (leader-produce, follower-`createContext`,
    // peer-`validateArtifact`) so the slash is folded identically. Each entry is re-validated via the `invalidStateProofValidator`
    // (deterministic recompute); on UPHELD a `WatchtowerSlashRequest(submitter = Some(challengerAddress))` is surfaced into the SAME
    // `applyWatchtowerSlashes` fold the self-detected re-exec mismatch feeds (durable slash + bounty to the challenger + `Slashings` MPT
    // write). The set is `(shardId, disputedCheckpointHash)`-ordered so the same wrong checkpoint appears at most once. Default
    // `SortedSet.empty` (every test/cl0/dl1 call site) ⇒ no slash ⇒ byte-identical; ALWAYS empty at `numShards = 1` ⇒ the regression bar holds.
    fraudProofs: SortedSet[io.constellationnetwork.schema.slashing.InvalidStateProofEvidence] = SortedSet.empty,
    // Unified operator-key registrations are ordinary consensus events. They are evaluated against the exact parent branch and inclusion
    // eta period; only accepted certs are embedded and rooted. A missing verifier rejects every candidate rather than accepting unverified
    // key authority.
    kesRegistrationCerts: List[Signed[KesRegistrationCert]] = Nil
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
      BranchHandle[F, GlobalStateKey],
      // Task #12 slice 2b — the typed per-ordinal global-state delta gl0 just applied. Built
      // mid-comprehension, consumed internally for the MPT writes, and ALSO returned so the gl0
      // producer (`GlobalSnapshotConsensusFunctions`) can stage it hash-keyed and `SnapshotLeaderLoop`
      // can promote the FINALIZED ordinal's accumulator into the bounded changeset ring the ml0
      // adopt-and-verify follow path serves. Followers (`GlobalSnapshotContextFunctions`) ignore it.
      // Pure read-out — does NOT change snapshot content / finality / mptRoot.
      StateChangesAccumulator,
      KesRegistrationCertAcceptanceResult
    )
  ]
}

object GlobalSnapshotAcceptanceManager {

  /** Project the frozen atomic genesis registry into the permanent ownership set. The registry is read once as paired records; KES and VRF
    * projections are never joined here and therefore cannot drift into independent authority surfaces. A map key that disagrees with its
    * embedded operator identity is startup corruption and fails closed. This helper does not manufacture runtime registration certificates;
    * genesis remains a distinct canonical anchor.
    */
  def anchoredConsensusKeyClaimsFromRegistry[F[_]: Async](
    registry: OperatorConsensusKeyRegistry[F]
  ): F[List[RegisteredConsensusKeyClaim]] =
    registry.list.flatMap { entries =>
      val mismatchedOperators = entries.toList.collect {
        case (stateOperator, keys) if stateOperator =!= keys.operatorPeerId =>
          s"$stateOperator->${keys.operatorPeerId}"
      }.sorted

      if (mismatchedOperators.nonEmpty)
        Async[F].raiseError(
          new IllegalStateException(
            s"Malformed atomic genesis consensus-key registry: operator mismatches=${mismatchedOperators.mkString(",")}"
          )
        )
      else {
        val claims = entries.toList.sortBy(_._1).map {
          case (peerId, keys) =>
            RegisteredConsensusKeyClaim(
              peerId,
              Hex.fromBytes(keys.kes.vk.value),
              Hex.fromBytes(keys.vrfPublicKey.toBytes)
            )
        }
        Async[F]
          .fromEither(
            RegisteredConsensusKeyOwnership
              .fromState(claims, SortedMap.empty[PeerId, SortedSet[KesRegistrationRecord]])
              .leftMap(errors => new IllegalStateException(s"Invalid paired genesis KES+VRF ownership: ${errors.toList.mkString(", ")}"))
          )
          .as(claims)
      }
    }

  private final case class PendingCheckpointAdoption(
    shardId: ShardId,
    shardOrdinal: io.constellationnetwork.schema.sharding.ShardOrdinal,
    checkpointHash: Hash,
    replayedMetagraphs: Set[Address]
  )

  private[global] def pruneRejectedSpendActions(
    accepted: SortedMap[Address, List[SpendAction]],
    rejectedAllowSpendRefs: Set[Hash]
  ): SortedMap[Address, List[SpendAction]] =
    accepted.flatMap {
      case (producer, actions) =>
        val retained = actions.flatMap { action =>
          NonEmptyList
            .fromList(action.spendTransactions.toList.filterNot(tx => tx.allowSpendRef.exists(rejectedAllowSpendRefs.contains)))
            .map(SpendAction(_))
        }
        Option.when(retained.nonEmpty)(producer -> retained)
    }

  private[global] def pruneSpendActionsAtPendingCapacity(
    accepted: SortedMap[Address, List[SpendAction]],
    pendingGlobalChangeOrdinals: Map[Address, SortedSet[SnapshotOrdinal]],
    currentOrdinal: SnapshotOrdinal,
    maxPendingPerMetagraph: Int
  ): SortedMap[Address, List[SpendAction]] =
    accepted.flatMap {
      case (producer, actions) =>
        val retained = actions.flatMap { action =>
          NonEmptyList
            .fromList(action.spendTransactions.toList.filter { transaction =>
              transaction.currencyId.forall { currencyId =>
                val pending = pendingGlobalChangeOrdinals.getOrElse(currencyId.value, SortedSet.empty[SnapshotOrdinal])
                pending.contains(currentOrdinal) || pending.size < maxPendingPerMetagraph
              }
            })
            .map(SpendAction(_))
        }
        Option.when(retained.nonEmpty)(producer -> retained)
    }

  private[global] def filterNativeAllowSpendsForEpoch(
    activeAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    epochProgress: EpochProgress
  ): SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]] =
    activeAllowSpends.get(None).fold(activeAllowSpends) { nativeAllowSpends =>
      activeAllowSpends.updated(
        None,
        nativeAllowSpends.view.mapValues(_.filter(_.lastValidEpochProgress >= epochProgress)).to(SortedMap)
      )
    }

  private[global] def nativeConsumedAllowSpendRefs(
    settledSpendActions: SortedMap[Address, List[SpendAction]]
  ): SortedSet[Hash] =
    SortedSet.from(
      settledSpendActions.valuesIterator
        .flatMap(_.iterator)
        .flatMap(_.spendTransactions.toList.iterator)
        .filter(_.currencyId.isEmpty)
        .flatMap(_.allowSpendRef)
    )

  private[global] final case class NativeAllowSpendTerminalOverlap(refs: SortedSet[Hash]) extends NoStackTrace

  private[global] def ensureNativeAllowSpendTerminalDisjointness(
    consumedRefs: SortedSet[Hash],
    expiredRefs: SortedSet[Hash]
  ): Either[NativeAllowSpendTerminalOverlap, Unit] = {
    val overlap = consumedRefs.intersect(expiredRefs)
    Either.cond(overlap.isEmpty, (), NativeAllowSpendTerminalOverlap(overlap))
  }

  private case object InvalidMerkleTree extends NoStackTrace

  /** One upheld invalid-state-proof dispute surfaced from `adoptShardCheckpoints` — the durable-slash request the accept path applies. The
    * reachable producer is the GL0-deterministic replay mismatch (`ShardCheckpointAcceptResult.RejectedReExecutionMismatch`), which carries
    * no fraud-proof submitter ⇒ `submitter = None` ⇒ the full slashed amount BURNS (no bounty recipient). The `submitter` slot is `Option`
    * so the watchtower-quorum dispute path (an `InvalidStateProofEvidence` carrying `fraudProof.submitterId`), when later wired into
    * accept, credits the bounty by passing `Some(submitterAddress)`.
    *
    * @param shardId
    *   the shard whose committee signed the wrong derivation (double-slash key half + registry-entry field).
    * @param disputedCheckpointHash
    *   `Hasher(cp.signingPreimage)` — the disputed checkpoint's canonical identity (double-slash key half + evidence digest).
    * @param slashSigners
    *   the committee signers to slash (every signer attested the wrong derivation; §10.2), deduplicated by the helper.
    * @param submitter
    *   the bounty recipient if this dispute arrived via a watchtower fraud proof; `None` for the gl0 self-detected re-exec path (burn all).
    */
  final case class WatchtowerSlashRequest(
    shardId: ShardId,
    disputedCheckpointHash: Hash,
    slashSigners: List[PeerId],
    submitter: Option[Address]
  )

  /** The deterministic outcome of folding every [[WatchtowerSlashRequest]] for one ordinal — the post-slash (pre-clean) stake maps that
    * replace the accept path's `updatedCreateDelegatedStakes` / `updatedCreateNodeCollaterals`, the per-operator audit records to durably
    * write into the `Slashings` MPT partition, the bounty credits to fold into balances (empty when no submitter), and the burned total.
    */
  final case class WatchtowerSlashApplication(
    slashedDelegatedStakes: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    slashedNodeCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]],
    registryEntries: List[SlashedRegistryEntry],
    bountyBalanceDelta: SortedMap[Address, Balance],
    totalBurned: Long
  )

  /** Apply every upheld invalid-state-proof dispute for one ordinal as a PURE, deterministic fold (consensus-state — every honest node
    * computes the byte-identical post-state). Requests are processed in `(shardId, disputedCheckpointHash)` sort order; each
    * [[InvalidStateProofSlashManager.applySlash]] is pure and its post-slash maps seed the next request's prior, so the accumulated maps +
    * registry entries + burn are order-deterministic. Bounty credit: when a request carries a `submitter`, `bountyAmount` is credited to
    * that address via `creditBalance` over its balance after every earlier economic transition in this ordinal (`postEconomicBalances` +
    * any earlier credit this fold); otherwise the whole slashed pool burns. `disputedCheckpointHash` doubles as the registry entry's
    * `evidenceDigest` (it is the canonical identity of the disputed checkpoint).
    *
    * Returns the prior maps verbatim + empty deltas when `requests` is empty — the no-op fast path that keeps `numShards = 1` (where no
    * request is ever produced) byte-identical.
    */
  def applyWatchtowerSlashes(
    requests: List[WatchtowerSlashRequest],
    priorDelegatedStakes: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    priorNodeCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]],
    postEconomicBalances: SortedMap[Address, Balance],
    eventOrdinal: SnapshotOrdinal,
    currentEpoch: EpochProgress,
    config: InvalidStateProofSlashingConfig
  ): WatchtowerSlashApplication =
    if (requests.isEmpty)
      WatchtowerSlashApplication(priorDelegatedStakes, priorNodeCollaterals, Nil, SortedMap.empty[Address, Balance], 0L)
    else {
      // A checkpoint is slashable once per `(shardId, checkpointHash)`, even when local replay and one or more carried fraud proofs surface
      // the same dispute in this ordinal. Coalesce before the economic fold so input list order cannot decide which duplicate receives a
      // bounty. Signers are unioned canonically. A self-detected request keeps the existing burn-all policy; otherwise the lowest ordered
      // authenticated submitter receives the single bounty.
      val canonicalRequests = requests
        .groupBy(r => (r.shardId.value.value, r.disputedCheckpointHash.value))
        .toList
        .sortBy(_._1)
        .map {
          case (_, duplicates) =>
            val representative = duplicates.head
            representative.copy(
              slashSigners = duplicates.flatMap(_.slashSigners).distinct.sortBy(_.value.value),
              submitter =
                if (duplicates.exists(_.submitter.isEmpty)) None
                else duplicates.flatMap(_.submitter).sorted.headOption
            )
        }

      canonicalRequests
        .foldLeft(
          WatchtowerSlashApplication(priorDelegatedStakes, priorNodeCollaterals, Nil, SortedMap.empty[Address, Balance], 0L)
        ) { (acc, req) =>
          val res = InvalidStateProofSlashManager.applySlash(
            slashTargets = req.slashSigners.toSet,
            priorDelegatedStakes = acc.slashedDelegatedStakes,
            priorNodeCollaterals = acc.slashedNodeCollaterals,
            eventOrdinal = eventOrdinal,
            currentEpoch = currentEpoch,
            shardId = req.shardId,
            disputedCheckpointHash = req.disputedCheckpointHash,
            evidenceDigest = req.disputedCheckpointHash,
            slashFraction = config.slashFraction,
            bountyFraction = config.bountyFraction,
            cooldownEpochs = config.cooldownEpochs
          )
          // Bounty credit ONLY when a submitter is present (watchtower-quorum path). The re-exec path passes `None` ⇒ no credit ⇒ the
          // `bountyAmount` portion also burns (it is never returned to any balance). Read the running balance from the post-economic map
          // folded with any earlier credit this same fold so two requests crediting the same submitter accumulate.
          val nextBountyDelta: SortedMap[Address, Balance] = req.submitter match {
            case Some(addr) if res.bountyAmount > 0L =>
              val current = acc.bountyBalanceDelta.getOrElse(addr, postEconomicBalances.getOrElse(addr, Balance.empty))
              acc.bountyBalanceDelta.updated(addr, InvalidStateProofSlashManager.creditBalance(current, res.bountyAmount))
            case _ => acc.bountyBalanceDelta
          }
          // Burn = the slashed pool minus whatever was actually credited as bounty. With no submitter the credit is 0 ⇒ burn = total.
          val creditedThisStep: Long = req.submitter match {
            case Some(_) => res.bountyAmount
            case None    => 0L
          }
          WatchtowerSlashApplication(
            slashedDelegatedStakes = res.slashedDelegatedStakes,
            slashedNodeCollaterals = res.slashedNodeCollaterals,
            registryEntries = acc.registryEntries ++ res.newRegistryEntries,
            bountyBalanceDelta = nextBountyDelta,
            totalBurned = acc.totalBurned + (res.totalSlashedAmount - creditedThisStep)
          )
        }
    }

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
    // §3 NIPoPoW S0.4: number of snapshots per eta-rotation period. At every boundary ordinal (`ord % R == R - 1`)
    // accept() captures `NodeStakeAggregator.snapshotFromMpt` (the §G2 MPT-primary path; byte-equivalent to the previous
    // `EpochStakeSnapshotter.snapshot(builtInfo)` GSI walk) into `historicalStakeSnapshots[currentPeriod]` and prunes
    // entries older than `currentPeriod - 3` (algorithm reads N-2; the extra slot is a reorg-grace). REQUIRED (no source-level
    // default): must match the producer's R = round(3.1·k₁) (`nakamoto.confirmation-depth-k` derived) for cross-node determinism —
    // a stale literal would silently diverge. Prod + the gl0 path thread the derived R; test callers pass an explicit R fixture.
    etaRotationSnapshots: Long,
    // Path 1 (heap-leak workstream): callback that returns eta_period for the just-closed eta-period at
    // the boundary write. Eta_period is determined at the 2/3-mark of period (period-1) and used by slot
    // leaders DURING period; by the time the boundary ordinal of period is reached, eta_period has been
    // "the" eta for the whole window. This boundary write captures that deterministic value alongside
    // stake_period so future reads (`EtaStateManager.getEta(period)`) hit the MPT cache instead of walking
    // the chainStore (which Fix B's k₁-bounded retention can have evicted). Defaults to `None` —
    // suitable for tests / pre-wire-up call sites where boundary writes can fall through to
    // `Hash.empty`; production overrides at `GlobalSnapshotConsensus` construction with a
    // chainStore-backed walk.
    etaForPeriod: Option[(EtaPeriod, BranchId) => F[Hash]] = None,
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
    // `GlobalSnapshotConsensus` construction once the single-outstanding checkpoint path is wired.
    //
    // `shardAssignment` is the deterministic, cluster-wide static metagraph → shard map (Hasher-based; identical
    // on every node). At `numShards > 1` it is consulted in `accept()` to PARTITION the raw `scEvents`: a metagraph
    // address that maps to a shard flows only through the checkpoint path. Target inclusion requires replay-backed
    // execution quorum plus positive watchtower coverage; a committee signature count alone is insufficient. It is
    // EXCLUDED from the base `processStateChannelEvents` chain-link call. Because the static assignment is total,
    // in practice this excludes every metagraph SC event at `numShards > 1`; the base path then handles only any
    // genuinely non-sharded events (none under the current total assignment). This removes the double-path, the
    // per-node pending-checkpoint divergence window, and the fee-payer balanceUpdate overlap. At `numShards = 1`
    // (or `shardAssignment = None`) NO filter is applied — byte-identical to today.
    shardingConfig: Option[ShardingConfig] = None,
    shardCheckpointAcceptanceManager: Option[ShardCheckpointGl0AcceptanceManager[F]] = None,
    shardAssignment: Option[ShardAssignment[F]] = None,
    // W3c ACTIVATION — the CROSS-SHARD read client for the sharded `SpendActionValidator`. When sharding is active
    // (`shardingConfig.numShards > 1 ∧ shardAssignment.isDefined`), accept() builds a PER-ACCEPT cross-shard-capable
    // `SpendActionValidator` (the `SpendActionValidator.make(proofClient, shardAssignment, overlay)` overload) whose
    // W3c `CrossShardEffectiveBalanceOverlay` is BOUND to THIS accept's already-materialized consensus context
    // (`consumedSpentSetForValidation` + `metagraphPinnedEpochProgresses` + `epochProgress`) via
    // `ConsumedAllowSpendStateManager.effectiveCurrencyBalances` — so a cross-shard no-`allowSpendRef` self-spend of a
    // PHANTOM expiry-refund is REJECTED on the cross-shard path exactly as on the same-shard path. DETERMINISM: the
    // overlay reuses the deterministic+saturating `effectiveCurrencyBalances` over the gl0-finalized (cluster-uniform)
    // spent-set, and the per-accept context is materialized identically on every node (gated `numShards > 1`); the
    // validator constructor is a pure closure factory (no Ref/F), so binding it per-accept is split-safe.
    //
    // `None` (the default) ⇒ accept() supplies the DETERMINISTIC `ShardSubtreeProofClient.gl0Local` read source, built from this
    // accept's consensus-pinned `branchAwareReader` (the SAME accept-`parentTip`-bound reader every per-manager prior-state read
    // uses). gl0 is the GLOBAL mirror — it holds the finalized state of every shard's metagraphs — so the cross-shard
    // `AllowSpend`/`Balance` the validator needs is read DIRECTLY off gl0's own finalized state: every gl0 node reads the
    // byte-identical value for the same key at the same ordinal ⇒ the cross-shard spend-action result feeds the consensus mptRoot
    // identically (no fork). This REPLACES the prior fail-closed `noop` default (which rejected every cross-shard spend for retry).
    // A live `ShardSubtreeProofClient.http` here would STILL be out of scope — a peer fetch is node-local (network/cooldown/
    // peer-pick) and would fork — but the gl0-local read needs no peer because gl0 already holds all shards' state. An explicit
    // `Some(client)` (e.g. a test mock) overrides the gl0-local default. The W3c forcing function (the OVERLAY) wraps the read on
    // top of whichever client is used. At `numShards = 1` (or `shardAssignment = None`) the injected unsharded
    // `spendActionValidator` is used verbatim ⇒ the cross-shard branch is unreachable, the gl0-local client is never constructed
    // ⇒ byte-identical to today.
    crossShardSpendProofClient: Option[ShardSubtreeProofClient[F]] = None,
    // Staged future-era NIPoPoW historical-commitment SMT store coupled with its confirmation-depth cutoff k `(store, k)`.
    // Current GL0 production wiring MUST pass `None`: the active-era snapshot contract requires `stateProof.smtRoot = None`, and
    // GlobalSnapshotActiveEraValidator rejects a populated slot before persistence or adoption. Retaining this dependency keeps the
    // unfinished tower implementation reviewable; wiring `Some` is not an activation mechanism and would produce an invalid artifact.
    historicalCommitmentSmt: Option[
      (io.constellationnetwork.node.shared.domain.nakamoto.nipopow.HistoricalCommitmentSmtStore[F], Long)
    ] = None,
    // WATCHTOWER invalid-state-proof slashing config (slashing part 3): slashFraction / bountyFraction / cooldownEpochs / watchtowerEnabled
    // — typed HOCON, NOT a `sys.env` read (project rule). Threaded into the `applyWatchtowerSlashes` fold at the upheld-dispute sink. Default
    // mirrors `application.conf`'s `nakamoto.invalidity-slashing` (100% tier, 5% bounty, 100-epoch cooldown, watchtower ON) so the durable
    // slash applies at `numShards > 1`. REQUIRED (no source-level default): slashFraction/bountyFraction feed the post-slash stake
    // maps committed into the global mptRoot, so they must be the operator-configured, cluster-uniform values — never a silent
    // literal. Prod threads `SharedConfig.nakamoto.invaliditySlashing` (100% tier, 5% bounty, 100-epoch cooldown, watchtower ON)
    // at BOTH GSAM construction sites; test callers pass an explicit fixture.
    // NOTE: `watchtowerEnabled = false` makes the sink inert (no slash + no `Slashings` write), keeping the mptRoot pre-slash. The slash is
    // ONLY reachable at `numShards > 1` regardless (the adopt path that surfaces the request never runs at `numShards = 1`), so the
    // `numShards = 1` byte-identical regression bar is independent of this config.
    invaliditySlashingConfig: InvalidStateProofSlashingConfig,
    // WATCHTOWER fraud-proof DETERMINISTIC dispute verdict (W3a). When `Some`, every fraud-proof artifact carried in `accept(fraudProofs=…)`
    // is re-validated here via the SAME `InvalidStateProofValidator` the daemon uses (recomputes the honest per-MG root from the disputed
    // checkpoint's OWN signed bytes; UPHELD iff attested ≠ honest — never trusts the challenger). On UPHELD a `WatchtowerSlashRequest` with
    // `submitter = Some(challengerAddress)` is surfaced into the SAME `applyWatchtowerSlashes` fold the self-detected re-exec path feeds, so
    // the leader/follower/peer reach a BYTE-IDENTICAL slash (the validator is pure given its inputs + the exact proposal-parent reader
    // supplied inside `accept`). The production wiring (`GlobalSnapshotConsensus.make`) passes the validator built with the PIN-1
    // `watchtowerReDerive` closure; authoritative validation replaces its staging reader with an
    // `InvalidStateProofSlashedReader.fromGlobalStateReader(mpt)` bound to `parentTip`, so an already-slashed checkpoint yields
    // `AlreadySlashed` ⇒ NOT upheld ⇒ no double slash. `None` (cl0/dl1/tests passing no validator) ⇒ carried fraud proofs are ignored ⇒ no slash. The slash is
    // additionally gated by `watchtowerEnabled` and is ONLY reachable at `numShards > 1` (no fraud proofs exist at `numShards = 1`), so the
    // `numShards = 1` byte-identical regression bar is preserved independently of this validator.
    invalidStateProofValidator: Option[
      io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[F]
    ] = None,
    // The validator consumes the ordinal-selected hasher, so it is constructed per accept call. `None` is fail-closed: all registration
    // candidates are rejected and returned to the event pool.
    kesRegistrationAcceptanceManagerForHasher: Option[Hasher[F] => KesRegistrationCertAcceptanceManager[F]] = None,
    // Paired genesis KES+VRF claims remain permanent key owners. Runtime histories are added from the exact parent branch before each
    // acceptance pass; malformed or colliding ownership state aborts acceptance.
    anchoredConsensusKeyClaims: List[RegisteredConsensusKeyClaim] = Nil
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector
  ): F[GlobalSnapshotAcceptanceManager[F]] = {
    val publisher: LocalEventsPublisher[F] = localEventsPublisher.getOrElse(LocalEventsPublisher.noop[F])
    // Establish the WithdrawalTimeLimit implicit from the explicit constructor param so that the rebuild paths
    // (`syncFromGlobalSnapshotInfo`, `stateProofBuilder` -> `mptStateProof`) see the same limit the accept path uses
    // to compute the rooted expiry-index buckets. This is consensus behavior, not a construction-site feature flag.
    implicit val withdrawalTimeLimitCtx: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
      io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.some(withdrawalTimeLimit)
    // `mptStore` is the underlying base — used by the LegacyFormat path's `builder.buildProof`
    // (which reads from the producer attached to the base store), and as the source-of-truth for
    // verify-replay's pre-state byte snapshot. Per-manager prior-state reads are routed through
    // a dynamic branch-aware reader (`branchTipRef` set at accept() top) so under MultiBranch a
    // child sees its parent branch's pending writes (#56.10 Phase J).
    val mptStore: MptStore[F, GlobalStateKey] = overlay.base
    // Construct the consensus sync manager alongside the branch tip and acceptance mutex in one effect.
    val validateAnchoredClaims = Async[F]
      .fromEither(
        RegisteredConsensusKeyOwnership
          .fromState(anchoredConsensusKeyClaims, SortedMap.empty[PeerId, SortedSet[KesRegistrationRecord]])
          .leftMap(errors => new IllegalStateException(s"Invalid anchored KES+VRF ownership: ${errors.toList.mkString(", ")}"))
      )
      .void

    (
      cats.effect.Ref.of[F, BranchId](BranchId.base),
      cats.effect.std.Semaphore[F](1),
      MetagraphSyncManager.make[F],
      validateAnchoredClaims
    ).mapN { (branchTipRef, acceptMutex, metagraphSyncManager, _) =>
      val branchAwareReader = io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.dynamic[F](
        overlay,
        branchTipRef.get
      )
      // Cross-shard framework inputs are finality-first. This reader never sees pending branch deltas; only `finalizeBranch` folds state
      // into `overlay.base`, so an owner authorization is unavailable to another shard until GL0 finalizes it.
      val finalizedBaseReader =
        io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore[F](mptStore)
      val artifactEmissionManager = ArtifactEmissionManager.make[F]()
      val tipUsageManager = TipUsageManager.make[F]()
      val rewardAcceptanceManager = RewardAcceptanceManager.make[F](branchAwareReader)
      val allowSpendStateManager = AllowSpendStateManager.make[F](branchAwareReader)
      // ATOMIC CROSS-SHARD ALLOW-SPEND SETTLEMENT (I-ONCE, Option A). Reads/writes the `ConsumedAllowSpends` spent-set (fieldId 33) via the
      // SAME branch-aware reader so the spent-set view is consistent with every other per-manager prior-state read. Only consulted at
      // `numShards > 1 ∧ shardAssignment.isDefined`; at `numShards = 1` the classification yields no cross-shard consume ⇒ the spent-set
      // stays empty ⇒ the reservation-adjustment overlay is the identity ⇒ the mptRoot is byte-identical to the pre-change path.
      val consumedAllowSpendStateManager = ConsumedAllowSpendStateManager.make[F](branchAwareReader)
      val kesRegistrationStateManager = KesRegistrationStateManager.make[F](branchAwareReader)
      // Generic cross-shard-message nullifier seam (thin): one handler per cross-shard message TYPE, each owning its own distinct nullifier
      // partition. Instance 1 = allow-spend consume over `ConsumedAllowSpends` (fieldId 33). A future type (cross-shard token-lock,
      // transfer, data-app message) adds another handler here over its own fieldId; the generic `CrossShardMessageEngine` writes them all
      // through one uniform `mpt.insert` path. The allow-spend STATE EFFECT (the read-side effective-currency-balance overlay) is applied
      // separately at the validator/route read sites, NOT in this list.
      val crossShardMessageHandlers: List[CrossShardMessageHandler[F]] =
        List(AllowSpendConsumeHandler[F](consumedAllowSpendStateManager))
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

        /** Staged future-era NIPoPoW computation. If the optional store is wired, this method populates `proof.smtRoot`; that output is
          * deliberately invalid in the current active era and is rejected by GlobalSnapshotActiveEraValidator. This code is retained for
          * owner review, not as a live activation path.
          *
          * Deterministic + producer/verifier-symmetric:
          *   - eligible ordinal `j = ordinal − k`; below the genesis/warmup window (`ordinal ≤ k`, or `j` not yet a finalized snapshot)
          *     there is nothing to commit ⇒ leave `smtRoot = None` (the same value on every node).
          *   - read snapshot `j` from the finalized chain (`getGlobalSnapshotByOrdinal`); derive `PerOrdinalCommitment(hypergraphRoot =
          *     snap.stateProof.mptRoot.getOrElse(empty), incrementalSnapshotHash = snap.hash, towerEligibility = NotComputed)`. All three
          *     are deterministic functions of the immutable snapshot at `j`, so leader and every gl0 peer derive byte-identical
          *     commitments. (towerEligibility is reserved but not yet tower-sourced — see PerOrdinalCommitment / report STAGING.)
          *   - fold it into the store under version `ordinal` and read the resulting cutoff-root. `appendAtFinality` is idempotent, so
          *     re-running accept(ordinal) (validateArtifact after produce, or a re-proposal) reproduces the same root.
          *   - if snapshot `j` is not retrievable (bootstrapped node lacking ancestors < its join ordinal), leave `smtRoot = None` — that
          *     node simply doesn't anchor smtRoot until it has accumulated the ancestor; it is NOT a wrong root. See report "bootstrap" for
          *     the sync-vs-skip options.
          */
        private def attachSmtRoot(
          ordinal: SnapshotOrdinal,
          proof: GlobalSnapshotStateProof,
          getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
        ): F[GlobalSnapshotStateProof] =
          historicalCommitmentSmt match {
            case None => proof.pure[F]
            case Some((store, confirmationDepthK)) =>
              val ordValue = ordinal.value.value
              if (ordValue < confirmationDepthK) proof.pure[F]
              else {
                val eligibleOrdinal =
                  SnapshotOrdinal(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(ordValue - confirmationDepthK))
                getGlobalSnapshotByOrdinal(eligibleOrdinal).flatMap {
                  case None =>
                    // Eligible ancestor not on disk (bootstrap / pruned). Deterministic fallback: no smtRoot this ordinal.
                    proof.pure[F]
                  case Some(eligibleSnapshot) =>
                    val commitment =
                      io.constellationnetwork.node.shared.domain.nakamoto.nipopow.PerOrdinalCommitment(
                        hypergraphRoot = eligibleSnapshot.signed.value.stateProof.mptRoot.getOrElse(Hash.empty),
                        incrementalSnapshotHash = eligibleSnapshot.hash,
                        towerEligibility = io.constellationnetwork.node.shared.domain.nakamoto.nipopow.TowerEligibility.NotComputed
                      )
                    store
                      .appendAtFinality(ordinal, eligibleOrdinal, commitment)
                      .map(smtRoot => proof.copy(smtRoot = Some(smtRoot.value)))
                }
              }
          }

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

        /** Verify checkpoint envelopes and select chain-continuous binary windows for global recreation. `verifyEmbedded` always
          * re-executes all included CL1 snapshots and compares their roots. Carried diffs and receipts are ignored. This method is
          * unreachable when `numShards == 1`.
          */
        private def adoptShardCheckpoints(
          ordinal: SnapshotOrdinal,
          shardCheckpoints: SortedMap[ShardId, ShardCheckpoint],
          checkpointManager: ShardCheckpointGl0AcceptanceManager[F],
          // CHAIN-HOLE GUARD (2026-06-10): gl0's per-MG SC tips from the PRIOR GlobalSnapshotInfo. A window may only be
          // adopted if its head chains off this tip (Hash.empty for an unseeded MG) — adopting a child checkpoint whose
          // parent was never adopted silently skips the genesis window, advances the SC tip past it, and permanently
          // wedges the MG (the seeding flake's root cause). Pure function of (embedded checkpoint, prior GSI) — every
          // node reaches the same adopt/defer decision.
          priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash]
        )(implicit hasher: Hasher[F]): F[
          (
            SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
            // The claimed roots remain comparison values only. GL0 recreates every accepted window and rejects any root mismatch.
            SortedMap[Address, Hash],
            // WATCHTOWER durable-slash requests (slashing part 3): one per upheld invalid-state-proof dispute surfaced this ordinal — the
            // GL0-deterministic replay mismatch (`RejectedReExecutionMismatch`). Consumed by the accept path's pure
            // `applyWatchtowerSlashes` fold (post-slash stake maps + `Slashings` MPT records + burn). Empty on the common all-accept path
            // and ALWAYS empty at `numShards = 1` (this method never runs there) ⇒ the regression bar is preserved.
            List[WatchtowerSlashRequest],
            // Local pipeline/fork-choice acknowledgements. They are applied only after the selected MG suffixes have passed the
            // independent GSAM replay and root comparison later in this accept pass.
            List[PendingCheckpointAdoption]
          )
        ] =
          shardCheckpoints.toList
            .foldM(
              (
                SortedMap.empty[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
                SortedMap.empty[Address, Hash],
                List.empty[WatchtowerSlashRequest],
                List.empty[PendingCheckpointAdoption]
              )
            ) {
              case ((adoptedAcc, rootsAcc, slashAcc, pendingAdoptions), (shardId, cp)) =>
                // DETERMINISTIC adopt-verifier — NOT intake `evaluate`. `verifyEmbedded` decides from the checkpoint bytes, the committee
                // membership for `(shardId, epoch)`, the configured execution quorum, and deterministic replay, so the leader (produce),
                // the follower (`createContext`), and every gl0 peer (`validateArtifact`) reach a byte-identical adopt decision + committed
                // state. Intake evaluation may return PendingMoreAttestations below quorum; embedded verification must reject it.
                if (shardId =!= cp.shardId)
                  loggerBundle.app
                    .warn(
                      s"[ACCEPTANCE/SHARDING] ordinal=$ordinal mapShard=$shardId checkpointShard=${cp.shardId} " +
                        s"shardOrd=${cp.shardOrdinal.value} REJECTED outer shard key mismatch"
                    )
                    .as((adoptedAcc, rootsAcc, slashAcc, pendingAdoptions))
                else
                  checkpointManager.verifyEmbedded(cp).flatMap {
                    case ShardCheckpointAcceptResult.Accepted =>
                      // ADOPT — no `onlyPossibleReferences` chain-link unfold, but with the CHAIN-HOLE GUARD: each per-MG
                      // window must chain off gl0's recorded SC tip (`Hash.empty` for an unseeded MG). The committee
                      // chain-link-validated these binaries against the SHARD chain; the guard ensures gl0 consumes that
                      // chain IN ORDER (no child window adopted while its parent window was never adopted — the genesis
                      // wedge). A non-continuous window is DEFERRED loudly; the leader re-offers the missing ancestor
                      // checkpoint at a later ord (ancestor-first selection in GlobalSnapshotConsensusFunctions).
                      // TRIM-AWARE anchoring (2026-06-10): all checkpoint windows partition the SAME linear ml0
                      // binary chain, so after a shard-chain reorg gl0's recorded tip (an orphaned branch's window
                      // tail) still lies ON that linear chain — the canonical window containing its continuation
                      // OVERLAPS the already-adopted prefix rather than chaining exactly off it. Requiring exact
                      // head==tip (the first guard shape) turned that overlap into a permanent adoption stall (run
                      // bml994k4d, shard 1 frozen 30+ min while its chain advanced). Instead: find the binary INSIDE
                      // the window whose lastSnapshotHash === tip (binaries carry their parent hash — no
                      // recomputation) and adopt the suffix from there. Pure function of (window, prior GSI) —
                      // split-safe. No continuation present at all ⇒ defer (true chain hole, the leader re-offers an
                      // older ancestor).
                      val tipFor: Address => Hash = mg => priorLastStateChannelSnapshotHashes.getOrElse(mg, Hash.empty)
                      // Classify via the same helper used by embed selection. Continue selects an exact suffix off GL0's committed
                      // tip; AlreadyAdopted is a no-op; every sibling or missing-parent lineage is deferred. Reconstructing a sibling
                      // by ordinal is invalid without authoritative prior state, and committee-carried state is never authoritative.
                      cp.derivedStateDelta.includedSnapshots.toList.traverse {
                        case (mg, nel) =>
                          ShardWindowContinuation.windowTipHashF(nel).map { windowTipHash =>
                            def suffixFrom(idx: Int): Either[
                              (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]),
                              (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], Int)
                            ] =
                              NonEmptyList
                                .fromList(nel.toList.drop(idx))
                                .map(suffix =>
                                  Right((mg, suffix, idx)): Either[
                                    (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]),
                                    (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], Int)
                                  ]
                                )
                                .getOrElse(Left((mg, nel)))
                            val decision: Either[
                              Address,
                              Either[
                                (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]),
                                (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], Int)
                              ]
                            ] =
                              ShardWindowContinuation.classify(
                                nel,
                                windowTipHash,
                                tipFor(mg)
                              ) match {
                                case ShardWindowContinuation.Continue(idx)  => Right(suffixFrom(idx))
                                case ShardWindowContinuation.AlreadyAdopted => Left(mg)
                                case ShardWindowContinuation.Defer          => Right(Left((mg, nel)))
                              }
                            decision
                          }
                      }.flatMap { trimResults =>
                        val alreadyAdopted = trimResults.collect { case Left(mg) => mg }
                        val deferred = trimResults.collect { case Right(Left(d)) => d }
                        val adopted = trimResults.collect { case Right(Right(r)) => r }
                        val chainContinuous: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]] =
                          SortedMap.from(adopted.map { case (mg, suffix, _) => mg -> suffix })(Address.OrderingInstance)
                        // A carried root is only a claim to compare with GL0's recreated result. State diffs are ignored.
                        val newRoots: SortedMap[Address, Hash] =
                          SortedMap.from(chainContinuous.keys.toList.flatMap { mg =>
                            cp.derivedStateDelta.perMetagraphMptRoots.get(mg).map(mg -> _)
                          })(Address.OrderingInstance)
                        alreadyAdopted.traverse_ { mg =>
                          loggerBundle.app.info(
                            s"[ACCEPTANCE/SHARDING] ordinal=$ordinal shardId=$shardId shardOrd=${cp.shardOrdinal.value} " +
                              s"ALREADY-ADOPTED mg=$mg gl0Tip=${tipFor(mg).value.take(12)} — window tail === gl0's SC tip " +
                              s"(fully adopted); nothing to adopt for this MG"
                          )
                        } >>
                          deferred.traverse_ {
                            case (mg, nel) =>
                              loggerBundle.app.warn(
                                s"[ACCEPTANCE/SHARDING] ordinal=$ordinal shardId=$shardId shardOrd=${cp.shardOrdinal.value} " +
                                  s"DEFER-ANCHOR mg=$mg windowHeadParent=${nel.head.value.lastSnapshotHash.value.take(12)} " +
                                  s"gl0Tip=${tipFor(mg).value.take(12)} " +
                                  s"— no continuation of gl0's SC tip anywhere in the window (missing ancestor checkpoint); deferring"
                              )
                          } >>
                          adopted.traverse_ {
                            case (mg, _, trimmedCount) =>
                              Async[F].whenA(trimmedCount > 0) {
                                loggerBundle.app.info(
                                  s"[ACCEPTANCE/SHARDING] ordinal=$ordinal shardId=$shardId shardOrd=${cp.shardOrdinal.value} " +
                                    s"TRIM-ANCHOR mg=$mg trimmed=$trimmedCount already-adopted prefix binaries (window overlap)"
                                )
                              }
                          } >>
                          loggerBundle.app
                            .info(
                              s"[ACCEPTANCE/SHARDING] ordinal=$ordinal shardId=$shardId shardOrd=${cp.shardOrdinal.value} " +
                                s"SELECTED-FOR-REPLAY mgs=${chainContinuous.size} binaries=${chainContinuous.values.map(_.size).sum} " +
                                s"deferredMgs=${deferred.size} alreadyAdoptedMgs=${alreadyAdopted.size}"
                            ) >>
                          Hasher[F]
                            .hash(cp.signingPreimage)
                            .map { cpHash =>
                              val nextPending =
                                if (deferred.isEmpty)
                                  pendingAdoptions :+ PendingCheckpointAdoption(
                                    shardId,
                                    cp.shardOrdinal,
                                    cpHash,
                                    chainContinuous.keySet
                                  )
                                else pendingAdoptions
                              (adoptedAcc ++ chainContinuous, rootsAcc ++ newRoots, slashAcc, nextPending)
                            }
                      } // close trimResults flatMap

                    case ShardCheckpointAcceptResult.PendingMoreAttestations =>
                      loggerBundle.app
                        .info(
                          s"[ACCEPTANCE/SHARDING] ordinal=$ordinal shardId=$shardId shardOrd=${cp.shardOrdinal.value} " +
                            s"PENDING — will retry next gl0 ord"
                        )
                        .as((adoptedAcc, rootsAcc, slashAcc, pendingAdoptions))

                    case ShardCheckpointAcceptResult.Rejected(reason) =>
                      // Logged at info (not warn) because a Rejected checkpoint can be a legitimate operator-level transient, such as an
                      // unavailable pinned replay base. Slashing for malicious rejections is Slice 16/17 territory; this slice surfaces
                      // the rejection but takes no slashing action.
                      loggerBundle.app
                        .info(
                          s"[ACCEPTANCE/SHARDING] ordinal=$ordinal shardId=$shardId shardOrd=${cp.shardOrdinal.value} " +
                            s"REJECTED reason=$reason"
                        )
                        .as((adoptedAcc, rootsAcc, slashAcc, pendingAdoptions))

                    case ShardCheckpointAcceptResult.RejectedReExecutionMismatch(reason, slashSigners) =>
                      // Wrong-derivation result from unconditional GL0 replay. The committee signers deviated from determinism and are the
                      // 100% `InvalidStateProof` slash targets (§10.2). The deterministic LEDGER EFFECT is
                      // `InvalidStateProofSlashManager.applySlash` (stake reduction ×(1−slashFraction) + cooldown registry entry +
                      // bounty/burn), the SAME sink the WATCHTOWER quorum-path dispute feeds via an on-chain `InvalidStateProofEvidence`.
                      //
                      // DURABLE WIRING (slashing part 3): surface a `WatchtowerSlashRequest` here — the canonical disputed-checkpoint hash
                      // (`Hasher(cp.signingPreimage)`) + the slash-target signers. The accept path's pure `applyWatchtowerSlashes` fold then
                      // (a) reduces/removes the targets' `activeDelegatedStakes` / `activeNodeCollaterals` BEFORE `cleanStateMaps` (so the
                      // existing removal-key derivation + GSI + accumulator stay consistent), and (c) writes each `SlashedRegistryEntry` into
                      // the `Slashings` MPT partition (fieldId 34) — both the writer-algebra view AND the #107 verify-replay set — after
                      // `applyStateChanges`. This path carries NO fraud-proof submitter (gl0 self-detected via re-exec), so the bounty has no
                      // recipient ⇒ the whole slashed pool burns (`submitter = None`). The decision is reached identically by leader/follower/
                      // peer (deterministic re-exec inside `accept()`), so the durable write is consensus-safe.
                      val slashTargets = slashSigners.distinct
                      Hasher[F].hash(cp.signingPreimage).flatMap { cpHash =>
                        loggerBundle.app
                          .warn(
                            s"[ACCEPTANCE/SHARDING] ordinal=$ordinal shardId=$shardId shardOrd=${cp.shardOrdinal.value} " +
                              s"REJECTED-REEXEC-MISMATCH reason=$reason slashSigners=${slashTargets.size} " +
                              s"checkpoint=${cpHash.value.take(12)} (InvalidStateProof 100% tier — durable slash + Slashings MPT write queued)"
                          )
                          .as(
                            (
                              adoptedAcc,
                              rootsAcc,
                              slashAcc :+ WatchtowerSlashRequest(shardId, cpHash, slashTargets, submitter = None),
                              pendingAdoptions
                            )
                          )
                      }
                  }
            }
            .map {
              case (adopted, roots, slashRequests, pendingAdoptions) =>
                (adopted, roots, slashRequests, pendingAdoptions)
            }

        /** Recreate every adopted CL1 snapshot with the global currency transition function. Checkpoint diffs are deliberately absent from
          * this interface: no committee-carried state can replace the result of global execution.
          */
        private def deriveAdoptedCurrencyState(
          ordinal: SnapshotOrdinal,
          currentBalances: SortedMap[Address, Balance],
          priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
            CurrencySnapshot
          ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
          adoptedScSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
          attestedRoots: SortedMap[Address, Hash],
          getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
        )(implicit hasher: Hasher[F]): F[StateChannelAcceptanceResult] =
          stateChannelEventsProcessor
            .processCurrencySnapshotsWithCompleteConsumption(
              ordinal,
              currentBalances,
              priorLastCurrencySnapshots,
              // The checkpoint carries canonical OLDEST-FIRST windows. The structured boundary performs the legacy order conversion and
              // separately proves that recreation returned every exact Signed input in that same order. Root equality over an accepted
              // prefix is insufficient: the checkpoint signature covers the whole window.
              adoptedScSnapshots,
              getGlobalSnapshotByOrdinal
            )
            .flatMap { replay =>
              // Incomplete, reordered, substituted, and unexpected output is structurally unavailable to checkpoint adoption.
              val completelyAccepted = replay.completeResults
              // The accepted map is the output of full CurrencySnapshotValidator recreation. Checkpoints carry execution inputs and root
              // claims only; they cannot replace balances, references, active sets, or any other economic state.
              val unverifiedResult =
                stateChannelEventsProcessor
                  .assembleAcceptanceResult(completelyAccepted, priorLastCurrencySnapshots, Set.empty[StateChannelOutput])

              adoptedScSnapshots.keys.toList.traverse { mg =>
                (unverifiedResult.calculatedCurrencyState.get(mg), attestedRoots.get(mg)) match {
                  case (Some(state), Some(attestedRoot)) if completelyAccepted.contains(mg) =>
                    GlobalStateConverter
                      .currencySnapshotMgRoot[F](SortedMap(mg -> state))
                      .map(recreatedRoot => Option.when(recreatedRoot === attestedRoot)(mg))
                  case _ => none[Address].pure[F]
                }
              }.map { verified =>
                val verifiedMgs = verified.flatten.toSet
                stateChannelEventsProcessor.assembleAcceptanceResult(
                  // Filter positively rather than subtracting rejected checkpoint keys: an unexpected processor output is never an
                  // independently authorized checkpoint input and therefore cannot enter the accepted result.
                  completelyAccepted.filter { case (mg, _) => verifiedMgs.contains(mg) },
                  priorLastCurrencySnapshots,
                  Set.empty[StateChannelOutput]
                )
              }
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
          lastSnapshotContext: GlobalSnapshotInfo,
          // W3c ACTIVATION: the spend-action validator for THIS accept. At `numShards > 1 ∧ shardAssignment.isDefined`
          // this is a per-accept CROSS-SHARD-capable validator whose effective-balance overlay is bound to this accept's
          // context (built at the call site); otherwise it is the injected unsharded `spendActionValidator` (byte-identical).
          spendValidatorForAccept: SpendActionValidator[F]
        ): F[ArtifactValidationResult] =
          for {
            (acceptedSpend, rejectedSpend) <- spendValidatorForAccept.validateReturningAcceptedAndRejected(
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
          lastSnapshotContext: GlobalSnapshotInfo
        )(implicit hasher: Hasher[F]): F[(AllowSpendBlockAcceptanceResult, TokenLockBlockAcceptanceResult)] =
          for {
            allowSpend <- blockAcceptanceCoordinatorManager.acceptAllowSpendBlocks(
              allowSpendBlocksForAcceptance,
              lastSnapshotContext,
              ordinal,
              epochProgress
            )
            tokenLock <- blockAcceptanceCoordinatorManager.acceptTokenLockBlocks(
              tokenLockBlocksForAcceptance,
              lastSnapshotContext,
              ordinal,
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
          baseInfo: GlobalSnapshotInfo,
          parentTip: BranchId,
          pinnedBoundaryEta: Option[Hash]
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
            // Path 1 (heap-leak workstream): the gl0-producer resolves eta_currentPeriod via `etaForPeriod` and packs it into
            // the `HistoricalStakeSnapshot` boundary entry. Eta is deterministic from the canonical chain at this point
            // (derived from period (currentPeriod-1)'s first 2/3 VRF outputs, fully knowable before currentPeriod even starts).
            //
            val etaF: F[Hash] =
              pinnedBoundaryEta.fold(etaForPeriod.map(_(currentPeriod, parentTip)).getOrElse(Async[F].pure(Hash.empty)))(Async[F].pure)
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
          updatedAcceptedMetagraphSyncData: SortedMap[Address, MetagraphSyncDataInfo],
          updatedKesRegistrationCerts: SortedMap[PeerId, SortedSet[KesRegistrationRecord]],
          updatedLastKesRegistrationRefs: SortedMap[PeerId, KesRegistrationReference],
          parentTip: BranchId,
          pinnedBoundaryEta: Option[Hash]
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
            lastSnapshotContext.historicalStakeSnapshots,
            updatedKesRegistrationCerts,
            updatedLastKesRegistrationRefs,
            lastSnapshotContext.genesisOperatorKeys
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
          computeHistoricalStakeBoundaryDelta(ordinal, baseInfo, parentTip, pinnedBoundaryEta).map {
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
          shardCheckpoints: SortedMap[ShardId, ShardCheckpoint] = SortedMap.empty,
          pinnedBoundaryEta: Option[Hash] = None,
          // WATCHTOWER fraud-proof artifact (W3a) — see the trait scaladoc. Threaded identically on every path; folded into the slash sink.
          fraudProofs: SortedSet[io.constellationnetwork.schema.slashing.InvalidStateProofEvidence] = SortedSet.empty,
          kesRegistrationCerts: List[Signed[KesRegistrationCert]] = Nil
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
            BranchHandle[F, GlobalStateKey],
            // Task #12 slice 2b — see the trait return type. The typed per-ordinal delta, returned for the
            // producer's changeset-ring staging. Additive; followers ignore it.
            StateChangesAccumulator,
            KesRegistrationCertAcceptanceResult
          )
        ] = {
          implicit val hasher: Hasher[F] = HasherSelector[F].getForOrdinal(ordinal)

          val era = Era.fromConfig(environment, fieldsAddedOrdinals)

          val tessellation3MigrationStartingOrdinal = fieldsAddedOrdinals.tessellation3Migration
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
                // The slash-ledger verdict is consensus-load-bearing. Bind it immutably to this proposal's exact parent branch; the
                // validator's construction-time reader remains available only for non-authoritative daemon staging.
                proposalParentSlashedReader = InvalidStateProofSlashedReader.fromGlobalStateReader[F](mpt)
                // Phase J: bind the per-call `parentTip` into the dynamic reader's Ref. The Ref
                // is shared across accept() calls but `acceptMutex` (above) ensures only one
                // accept() reads/writes it at a time.
                _ <- branchTipRef.set(parentTip)

                // Resolve the unified operator-key registry from this exact candidate-parent branch. Complete histories are load-bearing:
                // permanent ownership prevents a former holder's KES or VRF key from being reassigned after rotation. Pointer/history
                // disagreement is corrupt consensus state and aborts construction rather than weakening ownership to an empty view.
                priorKesRegistrationCerts <- kesRegistrationStateManager.materializeActiveKesRegistrationCertsFromMpt
                priorLastKesRegistrationRefs <- kesRegistrationStateManager.materializeLastRefsFromMpt
                priorLatestKesRegistrations <- kesRegistrationStateManager.materializeAllFromMpt
                _ <-
                  if (priorKesRegistrationCerts.keySet === priorLastKesRegistrationRefs.keySet)
                    Async[F].unit
                  else
                    Async[F].raiseError[Unit](
                      new IllegalStateException(
                        s"Corrupt KES+VRF registration parent state at ${parentTip.value}: " +
                          s"historyPeers=${priorKesRegistrationCerts.keySet.mkString(",")} " +
                          s"referencePeers=${priorLastKesRegistrationRefs.keySet.mkString(",")}"
                      )
                    )
                lastEffectiveFromPeriods = priorLatestKesRegistrations.view
                  .mapValues(_.event.value.effectiveFromPeriod)
                  .to(SortedMap)
                registeredKeyOwnership <- Async[F].fromEither(
                  RegisteredConsensusKeyOwnership
                    .fromState(anchoredConsensusKeyClaims, priorKesRegistrationCerts)
                    .leftMap(errors =>
                      new IllegalStateException(
                        s"Invalid KES+VRF ownership at parent ${parentTip.value}: ${errors.toList.mkString(", ")}"
                      )
                    )
                )
                kesRegistrationAcceptanceManager = kesRegistrationAcceptanceManagerForHasher
                  .fold(
                    KesRegistrationCertAcceptanceManager.make[F](
                      io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertValidator.rejectAll[F]
                    )
                  )(_(hasher))
                kesRegistrationAcceptanceResult <- kesRegistrationAcceptanceManager.accept(
                  kesRegistrationCerts.sorted,
                  priorLastKesRegistrationRefs,
                  lastEffectiveFromPeriods,
                  registeredKeyOwnership,
                  RegistrationEvaluationContext(
                    candidateParentHash = parentTip.value,
                    inclusionPeriod = EtaPeriod(EtaCalculation.rotationPeriod(ordinal.value.value, etaRotationSnapshots))
                  ),
                  ordinal
                )
                updatedKesRegistrationCerts = kesRegistrationStateManager.getUpdatedKesRegistrationCerts(
                  kesRegistrationAcceptanceResult,
                  priorKesRegistrationCerts
                )
                updatedLastKesRegistrationRefs <- kesRegistrationStateManager.getUpdatedLastRefs(
                  kesRegistrationAcceptanceResult,
                  priorLastKesRegistrationRefs
                )
                acceptedKesRegistrationPeers = kesRegistrationAcceptanceResult.accepted.keySet
                kesRegistrationCertDeltas = updatedKesRegistrationCerts.filter {
                  case (peerId, _) =>
                    acceptedKesRegistrationPeers.contains(peerId)
                }
                lastKesRegistrationRefDeltas = updatedLastKesRegistrationRefs.filter {
                  case (peerId, _) =>
                    acceptedKesRegistrationPeers.contains(peerId)
                }

                (allowSpendBlockAcceptanceResult, tokenLockBlockAcceptanceResult) <-
                  acceptAllowSpendAndTokenLockBlocks(
                    ordinal,
                    epochProgress,
                    allowSpendBlocksForAcceptance,
                    tokenLockBlocksForAcceptance,
                    lastSnapshotContext
                  )

                acceptedGlobalAllowSpends = allowSpendBlockAcceptanceResult.accepted.flatMap(_.value.transactions.toList)
                _dagLayerTokenLocks = tokenLockBlockAcceptanceResult.accepted
                  .flatMap(_.value.tokenLocks.toList)
                  .sortBy(tx => (tx.source, tx.ordinal, tx))
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
                _ <- Async[F].raiseUnless(acceptedGlobalTokenLocks === _dagLayerTokenLocks)(
                  new IllegalStateException(
                    s"Token-lock admission/state parity violation: admitted=${_dagLayerTokenLocks.size} stateAccepted=${acceptedGlobalTokenLocks.size}"
                  )
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
                // which by GSAM convention matches the map's keying `Id`. No reverse index is needed; prefix-scan + value-decode.
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

                // Source prior-ordinal `balances` from the MPT instead of `lastSnapshotContext.balances`. The rooted
                // ActiveAddressIndex (maintained on both delta and bootstrap paths) supplies the keyset; every indexed
                // target is required in the same view. Read happens before `syncFromStateChanges` so the MPT still reflects the
                // prior ordinal's state. Used here for the early `updatedGlobalBalances` and below for the final
                // GSI's `balances` field.
                priorBalances <- spendTransactionBalanceManager.materializeAllBalancesFromMpt

                updatedGlobalBalances = priorBalances ++ initialData.blockResult.contextUpdate.balances

                // Source prior `lastStateChannelSnapshotHashes` from the MPT instead of `lastSnapshotContext`. The
                // `LastStateChannelSnapshotHashes` partition is metagraph-keyed and the value (`Hash`) doesn't carry the
                // address; the rooted `ActiveAddressIndex` supplies the keyset, and every indexed target must resolve
                // in the same authenticated branch view. Read here (before
                // `processStateChannelEvents`) so the StateChannelAcceptanceManager can be GSI-free.
                // Phase J: route through `mpt` (branch-aware) — `mptStore` is base-only.
                //
                // The rooted index and every indexed target must resolve in the same branch-aware MPT view. A gap is authenticated
                // corrupt state (or an unavailable branch) and fails/defer; `lastSnapshotContext` is not an alternate authority.
                priorLastStateChannelSnapshotHashes <- mpt.materializeLastStateChannelSnapshotHashes
                // Source prior `lastCurrencySnapshots` from the MPT instead of `lastSnapshotContext`. The
                // `Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]` value
                // spans 3 metagraph-keyed partitions; we read the keyset from `LastCurrencySnapshots`'s
                // rooted `ActiveAddressIndex` (which marks any address with either mode), then per-address try Left
                // before falling back to the Right pair. Read here (before `processStateChannelEvents`) so the
                // StateChannelEventsProcessor can be GSI-free — `getFeeAddresses` and the `initialState` lookup
                // both consume this materialized map.
                // Phase J: route through `mpt` (branch-aware) — `mptStore` is base-only.
                //
                // Currency Left/Right targets obey the same rooted branch-view contract. Both arms decode strictly; neither-arm is an
                // error. GSI values cannot heal an index/target gap.
                priorLastCurrencySnapshots <- mpt.materializeLastCurrencySnapshots
                // Axis 1a (#259 token-lock stall fix). Sharding gate: the shard-checkpoint ADOPT path only fires when
                // (a) the manager was wired with `shardingConfig.numShards > 1` AND `shardCheckpointAcceptanceManager.isDefined`,
                // AND (b) the caller actually supplied a non-empty `shardCheckpoints` map for this ord. All three conditions
                // must hold; otherwise `adoptedScSnapshots` is empty, the standard chain-link path runs on the raw `scEvents`
                // verbatim, and the rest of `accept()` is byte-identical to today (the regression bar — see make()'s
                // shardingConfig scaladoc). Every adopted CL1 snapshot is recreated below.
                adoptedResult <-
                  (shardingConfig, shardCheckpointAcceptanceManager) match {
                    case (Some(cfg), Some(scMgr)) if cfg.numShards > 1 && shardCheckpoints.nonEmpty =>
                      adoptShardCheckpoints(
                        ordinal,
                        shardCheckpoints,
                        scMgr,
                        priorLastStateChannelSnapshotHashes
                      )
                    case _ =>
                      Async[F].pure(
                        (
                          SortedMap.empty[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
                          SortedMap.empty[Address, Hash],
                          List.empty[WatchtowerSlashRequest],
                          List.empty[PendingCheckpointAdoption]
                        )
                      )
                  }
                adoptedScSnapshots = adoptedResult._1
                adoptedRoots = adoptedResult._2

                // WATCHTOWER fraud-proof CONSENSUS ARTIFACT → durable slash (W3a). Re-validate EVERY carried fraud proof here via the SAME
                // deterministic `InvalidStateProofValidator` the daemon uses (recomputes the honest per-MG root from the disputed checkpoint's
                // OWN signed bytes; UPHELD iff attested ≠ honest — never trusts the challenger's claimed roots), and for each UPHELD dispute
                // surface a `WatchtowerSlashRequest(submitter = Some(challengerAddress))`. Because the validator is a pure function of the
                // evidence + the exact proposal-parent slash ledger, the leader/follower/peer reach a BYTE-IDENTICAL
                // verdict and thus the byte-identical slash. The honest-committee floor is enforced INSIDE the validator (`DisputeNotUpheld` on
                // a frivolous/forged proof ⇒ skipped here). The double-slash guard is the validator's step-7 exact-parent `slashedReader`
                // PLUS `applyWatchtowerSlashes`'s per-`(shardId, checkpointHash)` coalescing within the fold.
                // `submitterId.toAddress` is the deterministic recover-public-key→address of the challenger (the bounty recipient).
                // BYTE-IDENTITY GATE: explicitly require `numShards > 1` (the SAME gate `adoptShardCheckpoints` uses) so that even a forged
                // `fraudProofs` artifact injected at `numShards = 1` is a hard no-op — no committees exist there, so no honest dispute is
                // possible, and the slash must never touch the mptRoot. In production `fraudProofs` is also always empty at `numShards = 1`
                // (the producer's pool is `noop`); this gate is defense-in-depth so the regression bar holds unconditionally.
                fraudProofSlashRequests <-
                  (shardingConfig, invalidStateProofValidator, invaliditySlashingConfig.watchtowerEnabled) match {
                    case (Some(cfg), Some(validator), true) if cfg.numShards > 1 && fraudProofs.nonEmpty =>
                      // Deterministic iteration order: the `SortedSet` is `(shardId, disputedCheckpointHash)`-ordered, so the produced list
                      // order is identical on every node (it does not actually affect the result — `applyWatchtowerSlashes` re-sorts — but
                      // keep it canonical). The double-slash key is read off the fraud proof (`disputedCheckpointHash`), the SAME hash the
                      // validator binds the carried checkpoint to (step 4).
                      fraudProofs.toList.flatTraverse { evidence =>
                        val checkpointHash = evidence.fraudProof.disputedCheckpointHash
                        validator.validateAgainst(evidence, proposalParentSlashedReader).flatMap {
                          case Right(upheld) =>
                            upheld.fraudProof.submitterId.toAddress[F].map { submitterAddr =>
                              List(
                                WatchtowerSlashRequest(
                                  upheld.shardId,
                                  checkpointHash,
                                  upheld.slashTargets,
                                  submitter = Some(submitterAddr)
                                )
                              )
                            }
                          case Left(rejection) =>
                            // Honest-committee floor / frivolous / already-slashed / malformed — NO slash. Logged at info; no ledger effect.
                            loggerBundle.app
                              .info(
                                s"[ACCEPTANCE/SLASHING] ordinal=$ordinal WATCHTOWER fraud-proof NOT upheld (no slash): " +
                                  s"checkpoint=${checkpointHash.value.take(12)} shard=${evidence.shardId.value.value} " +
                                  s"mg=${evidence.metagraphAddress.value.value.take(10)} reason=$rejection"
                              )
                              .as(List.empty[WatchtowerSlashRequest])
                        }
                      }
                    case _ => Async[F].pure(List.empty[WatchtowerSlashRequest])
                  }

                // WATCHTOWER durable-slash requests = the GL0 self-detected replay mismatches (`submitter = None` ⇒ burn) UNION the
                // upheld watchtower fraud-proof disputes (`submitter = Some` ⇒ bounty). Both are applied to the stake maps below, before
                // cleaning, via the SAME pure `applyWatchtowerSlashes` fold. ALWAYS empty at numShards=1 (neither source produces a request
                // there). The `watchtowerEnabled` config gate makes the durable slash inert when off (drop the requests ⇒ `applyWatchtowerSlashes`
                // is a no-op ⇒ stake maps + `Slashings` partition + mptRoot unchanged) — a single deterministic kill-switch read at one site.
                adoptedSlashRequests =
                  if (invaliditySlashingConfig.watchtowerEnabled) adoptedResult._3 ++ fraudProofSlashRequests else Nil

                // CHANGE 3 — partition the raw `scEvents` by the deterministic static shard assignment. When sharding is
                // active (`numShards > 1` AND `shardAssignment` wired), a metagraph address that maps to a shard flows ONLY
                // via the adopt path (its committee checkpoint), so it is EXCLUDED from the base chain-link call below. This
                // removes the double-path: a sharded MG's binary is no longer processed by BOTH the adopt path AND the raw
                // chain-link path (which would otherwise leave a per-node pending-checkpoint divergence window and a fee-payer
                // `balanceUpdate` overlap). `shardIdFor` is `Hasher`-based and deterministic, so the partition is byte-identical
                // on every node. Because the static assignment is total, in practice EVERY metagraph SC event is excluded at
                // `numShards > 1`; only genuinely non-sharded events (none today) survive to the base path. At `numShards = 1`
                // (or `shardAssignment = None`) the filter is skipped and `baseScEvents == scEvents` — byte-identical to today.
                baseScEvents <-
                  (shardingConfig, shardAssignment) match {
                    case (Some(cfg), Some(assignment)) if cfg.numShards > 1 =>
                      // Route every event's address through the deterministic `shardIdFor` (the same `Hasher`-based map on
                      // every node) and KEEP only events whose address is NOT assigned to a shard. The static assignment is
                      // total — every metagraph address resolves to some shard — so this keeps NOTHING (`baseScEvents = Nil`)
                      // today; the partition is written generically so a future non-sharded carve-out would be honoured, and
                      // the `shardIdFor` call makes the exclusion a deterministic function of the address on every node.
                      scEvents.traverse(out => assignment.shardIdFor(out.address).map(_ => out -> true)).map { annotated =>
                        annotated.collect { case (out, isSharded) if !isSharded => out }
                      }
                    case _ => Async[F].pure(scEvents)
                  }

                // Standard chain-link path on the (DAG-layer / non-sharded) SC events. At numShards=1 `baseScEvents == scEvents`
                // and this is the only source of accepted SC snapshots, so the byte-identical regression bar holds:
                // `adoptedScSnapshots` is empty and the merge below is skipped, yielding exactly `baseAcceptance`.
                baseAcceptance <- processStateChannelEvents(
                  ordinal,
                  updatedGlobalBalances,
                  priorLastStateChannelSnapshotHashes,
                  priorLastCurrencySnapshots,
                  baseScEvents,
                  validationType,
                  getGlobalSnapshotByOrdinal
                )

                // Merge committee-selected SC snapshot bytes after currency derivation is rerun via
                // `deriveAdoptedCurrencyState`) with the standard chain-link result. The two MG sets are disjoint in normal
                // operation (a sharded MG flows only via its committee; a non-sharded/bootstrap MG only via raw scEvents), but
                // on the rare mixed-bootstrap overlap the globally recreated adopted entry wins (`++` right-biased). When
                // `adoptedScSnapshots` is empty (numShards=1 OR no checkpoints
                // this ord) the result IS `baseAcceptance` verbatim — no `deriveAdoptedCurrencyState` call, no merge.
                acceptanceAndVerifiedMetagraphs <-
                  if (adoptedScSnapshots.isEmpty)
                    Async[F].pure((baseAcceptance, Set.empty[Address]))
                  else
                    deriveAdoptedCurrencyState(
                      ordinal,
                      updatedGlobalBalances,
                      priorLastCurrencySnapshots,
                      adoptedScSnapshots,
                      adoptedRoots,
                      getGlobalSnapshotByOrdinal
                    ).map { adoptedAcceptance =>
                      (
                        StateChannelAcceptanceResult(
                          accepted = baseAcceptance.accepted ++ adoptedAcceptance.accepted,
                          // Both base + adopt return `priorLastCurrencySnapshots.concat(<states>)`. Merging them right-biased
                          // yields `prior ++ baseStates ++ adoptedStates` — the prior keys are overwritten with identical
                          // values, so this is exactly what a single combined `calculateLastCurrencySnapshots` would produce.
                          calculatedCurrencyState = baseAcceptance.calculatedCurrencyState ++ adoptedAcceptance.calculatedCurrencyState,
                          // Adopted snapshots are never "returned" (committee-accepted, not gl0-rejected); only the raw-event
                          // chain-link path can return events to a metagraph.
                          returned = baseAcceptance.returned ++ adoptedAcceptance.returned,
                          balanceUpdate = baseAcceptance.balanceUpdate ++ adoptedAcceptance.balanceUpdate,
                          incomingCurrencySnapshotsWithState =
                            baseAcceptance.incomingCurrencySnapshotsWithState ++ adoptedAcceptance.incomingCurrencySnapshotsWithState
                        ),
                        adoptedAcceptance.accepted.keySet
                      )
                    }
                (
                  StateChannelAcceptanceResult(
                    scSnapshots,
                    currencySnapshots,
                    returnedSCEvents,
                    currencyAcceptanceBalanceUpdate,
                    incomingCurrencySnapshots
                  ),
                  _
                ) = acceptanceAndVerifiedMetagraphs

                transactionsRefsDeltas <- transactionReferenceManager.acceptTransactionRefs(
                  initialData.blockResult.contextUpdate.lastTxRefs,
                  acceptedTransactions
                )

                // Source prior-ordinal `lastTxRefs` from the MPT instead of `lastSnapshotContext.lastTxRefs`. The rooted
                // ActiveAddressIndex (maintained on both delta and bootstrap paths) supplies the keyset; every indexed
                // target is required in the same authenticated view. Read happens before `syncFromStateChanges` so the
                // MPT still reflects the prior ordinal's state.
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

                // Prospective pending set for this exact acceptance. `currencySnapshots` already contains the globally re-executed owner
                // state. The prior acknowledgement queue is materialized from the rooted branch-aware MPT; `lastSnapshotContext` is not an
                // alternate authority. Remove only canonical in-band acknowledgements before deriving effective balances, so the overlay
                // applies while the owner has not processed a SpendAction and retires atomically when the owner's recreated raw balance does.
                priorMetagraphSyncData <- mpt.materializeMetagraphSyncData
                pendingGlobalChangeOrdinals =
                  priorMetagraphSyncData.iterator.map {
                    case (metagraphId, syncData) =>
                      metagraphId -> MetagraphSyncManager.pendingAfterAcknowledgements(
                        syncData.unappliedGlobalChangeOrdinals,
                        globalSnapshotsProcessed.getOrElse(metagraphId, List.empty)
                      )
                  }.toMap

                lastActiveAllowSpends <- allowSpendStateManager.materializeActiveAllowSpendsFromMpt

                // Per-metagraph PINNED global epoch (the epoch each MG used to expire its OWN allow-spends — its `globalSyncView.epochProgress`).
                // Hoisted here (pure fn of `currencySnapshots`) so it feeds BOTH the cross-shard effective-balance overlay below AND the
                // metagraph-scoped allow-spend expiry further down.
                metagraphPinnedEpochProgresses: Map[Address, EpochProgress] = currencySnapshots.toList.mapFilter {
                  case (metagraphId, Left(signed))       => signed.value.globalSyncView.map(gsv => metagraphId -> gsv.epochProgress)
                  case (metagraphId, Right((signed, _))) => signed.value.globalSyncView.map(gsv => metagraphId -> gsv.epochProgress)
                }.toMap

                // ── ATOMIC CROSS-SHARD ALLOW-SPEND SETTLEMENT — W3e (read-side EFFECTIVE-balance overlay at the validator) ──────────────────
                // The SpendActionValidator's same-shard balance check reads `currencyBalances` (the Some(M) attested per-MG balances). A
                // source whose EARLIER cross-shard consume of an allow-spend has expired sees M autonomously refund it (phantom +amount) — so
                // the attested balance would let it DOUBLE-CONSUME. Overlay the already-committed cross-shard spent-set onto each Some(M)
                // scope so the validator sees the ECONOMICALLY-EFFECTIVE balance (source still debited, destination credited). Derived from
                // committed consensus state (the nullifier partition); SATURATING (never raises). EMPTY spent-set (always at numShards=1) ⇒
                // effective == attested ⇒ validator input byte-identical. Gated `numShards > 1` so the spent-set read is skipped otherwise.
                consumedSpentSetForValidation <-
                  (shardingConfig, shardAssignment) match {
                    case (Some(cfg), Some(_)) if cfg.numShards > 1 =>
                      consumedAllowSpendStateManager.materializeConsumedAllowSpendsFromMpt
                    case _ => SortedMap.empty[Hash, ConsumedAllowSpend].pure[F]
                  }
                effectiveCurrencyBalances =
                  if (consumedSpentSetForValidation.isEmpty) currencyBalances
                  else
                    currencyBalances.map {
                      case (scope, attested) =>
                        scope -> consumedAllowSpendStateManager.effectiveCurrencyBalances(
                          attested,
                          scope,
                          consumedSpentSetForValidation,
                          pendingGlobalChangeOrdinals,
                          metagraphPinnedEpochProgresses,
                          epochProgress
                        )
                    }

                // ── W3c ACTIVATION — the per-accept CROSS-SHARD-capable SpendActionValidator ───────────────────────────────────
                // The same-shard balance path reads `effectiveCurrencyBalances` (overlay applied above). The CROSS-shard balance
                // path (a SpendTransaction whose `currencyId` is an MG on a DIFFERENT shard) reads the per-MG balance PROVEN from
                // that shard via the proof client, which the unsharded default validator can never reach. At `numShards > 1 ∧
                // shardAssignment.isDefined` build the sharded `SpendActionValidator` overload and BIND its W3c overlay to THIS
                // accept's consensus context — so the proven cross-shard balance is run through the SAME deterministic
                // `effectiveCurrencyBalances(spent-set, pinned-epochs, epochProgress)` correction before the balance check (a
                // cross-shard phantom-refund self-spend is rejected exactly as same-shard). The overlay's `(att, scope)` shape
                // closes over the per-accept spent-set/epochs; `effectiveCurrencyBalances` is pure+saturating and the spent-set is
                // gl0-finalized (cluster-uniform), so every honest node computes byte-identical effective balances. The proof
                // client defaults to the DETERMINISTIC `gl0Local` reader over the finalized MPT base — every gl0 node reads the
                // byte-identical value and an authorization created only on an unfinalized parent branch is not yet usable ⇒ no
                // fork; see the `crossShardSpendProofClient` param scaladoc for why a peer-fetch HTTP client is NOT used here.
                // At `numShards = 1` / `shardAssignment = None` we reuse the injected unsharded `spendActionValidator` verbatim ⇒
                // the cross-shard branch is unreachable, the gl0-local client is never constructed ⇒ byte-identical to the pre-W3c path.
                spendValidatorForAccept = (shardingConfig, shardAssignment) match {
                  case (Some(cfg), Some(assignment)) if cfg.numShards > 1 =>
                    val crossShardOverlay: SpendActionValidator.CrossShardEffectiveBalanceOverlay =
                      (attestedScopeBalances, ownerScope) =>
                        consumedAllowSpendStateManager.effectiveCurrencyBalances(
                          attestedScopeBalances,
                          ownerScope,
                          consumedSpentSetForValidation,
                          pendingGlobalChangeOrdinals,
                          metagraphPinnedEpochProgresses,
                          epochProgress
                        )
                    // GL0-LOCAL cross-shard read source (W3c read-source activation). gl0 is the GLOBAL mirror — it holds the
                    // finalized state of EVERY shard's metagraphs — so a cross-shard `AllowSpend`/`Balance` the validator needs is
                    // read DIRECTLY off gl0's finalized MPT base, NOT from the candidate's `parentTip` branch and NOT via a peer
                    // round-trip. DETERMINISM: the finalized base is cluster-uniform, so every gl0 node's reader returns the
                    // byte-identical value for the same key ⇒ the cross-shard spend-action result feeds the consensus mptRoot
                    // identically on every node (a live `ShardSubtreeProofClient.http` here would be node-local ⇒ fork). An explicit
                    // `crossShardSpendProofClient` (e.g. a test mock) still overrides; production leaves it `None` ⇒ gl0-local.
                    val crossShardClient: ShardSubtreeProofClient[F] =
                      crossShardSpendProofClient.getOrElse(ShardSubtreeProofClient.gl0Local[F](finalizedBaseReader))
                    SpendActionValidator.make[F](
                      crossShardClient,
                      assignment,
                      crossShardOverlay
                    )
                  case _ => spendActionValidator
                }

                ArtifactValidationResult(
                  acceptedSpendActions,
                  rejectedSpendActions,
                  acceptedPricingUpdates,
                  rejectedPricingUpdates
                ) <- validateArtifacts(
                  epochProgress,
                  spendActions,
                  pricingUpdates,
                  filterNativeAllowSpendsForEpoch(lastActiveAllowSpends, epochProgress),
                  effectiveCurrencyBalances,
                  globalBalances,
                  lastSnapshotContext,
                  spendValidatorForAccept
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

                // `metagraphPinnedEpochProgresses` (the per-MG pinned `globalSyncView.epochProgress`) is hoisted above (before
                // `validateArtifacts`) so the cross-shard effective-balance overlay and the metagraph-scoped allow-spend expiry share it.

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

                // `unappliedGlobalChangeOrdinals` is a lossless consensus acknowledgement queue. Never evict an unacknowledged ordinal to
                // satisfy its configured bound: doing so makes absence indistinguishable from owner application and can retire an economic
                // overlay early. Instead, deterministically backpressure only new currency-targeted transactions for an owner whose queue is
                // full. Transactions for the current already-tracked ordinal remain admissible because they do not grow the set.
                capacityCheckedSpendActions = pruneSpendActionsAtPendingCapacity(
                  acceptedSpendActions.toSortedMap,
                  pendingGlobalChangeOrdinals,
                  ordinal,
                  metagraphsSyncConfig.maxUnappliedGlobalChangeOrdinals.value
                )
                capacityRejectedSpendTransactionCount = acceptedSpendActions.values.flatten.map(_.spendTransactions.size).sum -
                  capacityCheckedSpendActions.values.flatten.map(_.spendTransactions.size).sum

                // Nullifier settlement is part of SpendAction acceptance, not advisory bookkeeping. Prune rejected references before
                // allow-spend removal, balance effects, sync emission, and the acceptance result are computed.
                crossShardEngineResult <-
                  (shardingConfig, shardAssignment) match {
                    case (Some(cfg), Some(assignment)) if cfg.numShards > 1 =>
                      CrossShardMessageEngine.settle[F](
                        crossShardMessageHandlers,
                        capacityCheckedSpendActions,
                        assignment,
                        ordinal
                      )
                    case _ => CrossShardMessageEngine.EngineResult.empty.pure[F]
                  }
                crossShardMarkers = crossShardEngineResult.markers
                settledSpendActions: SortedMap[Address, List[SpendAction]] =
                  pruneRejectedSpendActions(capacityCheckedSpendActions, crossShardEngineResult.rejected.toSet)
                consumedNativeAllowSpendRefs = nativeConsumedAllowSpendRefs(settledSpendActions)
                expiredNativeAllowSpendRefs <- expiredAllowSpendsHoisted.valuesIterator.flatten.toList
                  .traverse(_.toHashed.map(_.hash))
                  .map(hashes => SortedSet.from[Hash](hashes))
                _ <- Async[F].fromEither(
                  ensureNativeAllowSpendTerminalDisjointness(consumedNativeAllowSpendRefs, expiredNativeAllowSpendRefs)
                )
                _ <- Async[F].whenA(
                  capacityRejectedSpendTransactionCount > 0 || crossShardMarkers.nonEmpty || crossShardEngineResult.rejected.nonEmpty
                )(
                  loggerBundle.app.info(
                    s"[ACCEPTANCE/X-SHARD] ordinal=$ordinal cross-shard message settlement: " +
                      s"newMarkers=${crossShardMarkers.size} rejected=${crossShardEngineResult.rejected.size} " +
                      s"pendingCapacityRejected=$capacityRejectedSpendTransactionCount"
                  )
                )

                allAcceptedSpendTxns = settledSpendActions.values.flatten
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
                globalActiveTokenLocksByRefFromState <- tokenLockStateManager.buildActiveTokenLocksByRefFromMpt(
                  tokenLockLookupAddresses
                )
                // #186 in-round chain fix: a replacement accepted this round may target a lock ALSO accepted
                // this round (A→B inside one ordinal — `acceptReplacementTokenLocks` now admits that chain).
                // `generateTokenUnlocks` must resolve that in-round ref or the whole acceptance raises
                // ("Token lock not found for replacement ref" → RuntimeException below). Union the parent-state
                // map with this round's accepted locks. For rounds without an in-round chain none of the added
                // refs is ever looked up, so outputs stay byte-identical to the prior behavior. Deterministic:
                // `acceptedGlobalTokenLocks` is itself deterministic (derived from sorted block acceptance) and
                // the keys are content-addressed hashes.
                inRoundAcceptedTokenLocksByRef <- acceptedGlobalTokenLocks
                  .traverse(lock => lock.toHashed.map(hashed => hashed.hash -> lock))
                  .map(_.toMap)
                globalActiveTokenLocksByRef = globalActiveTokenLocksByRefFromState ++ inRoundAcceptedTokenLocksByRef

                globalLastAllowSpendRefs <- allowSpendStateManager.materializeLastAllowSpendRefsFromMpt
                globalLastTokenLockRefs <- tokenLockStateManager.materializeLastTokenLockRefsFromMpt

                allowSpendAcceptanceResult <- allowSpendStateManager.acceptAllowSpendsWithExpired(
                  epochProgress,
                  activeAllowSpendsFromCurrencySnapshots,
                  globalAllowSpends,
                  globalActiveAllowSpends,
                  allAcceptedSpendTxns,
                  expiredAllowSpendsHoisted,
                  metagraphPinnedEpochProgresses
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

                globalSpendTransactions = settledSpendActions.flatMap {
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
                    .leftMap(err => new RuntimeException(s"Spend-transaction balance application error: $err"))
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

                // WATCHTOWER durable slash (slashing part 3): apply every upheld invalid-state-proof dispute as a PURE deterministic fold
                // BEFORE `cleanStateMaps`, so the existing cleaning + removed-key derivation + GSI + accumulator all consume the post-slash
                // maps with no second code path. `applyWatchtowerSlashes` reduces/removes the slash targets' delegated-stake + collateral
                // records (100% tier ⇒ full removal), yields one `SlashedRegistryEntry` per `(operator, shard, checkpoint)` for the
                // `Slashings` MPT write, and (re-exec path: `submitter = None`) burns the entire pool (no bounty credit). Empty `requests`
                // (the common path; ALWAYS empty at numShards=1) ⇒ the prior maps verbatim + empty deltas ⇒ byte-identical to pre-slash.
                // Every ordinary balance effect for this ordinal has completed at this point. Materialize the complete view once so a
                // right-biased bounty update starts from the submitter's post-transfer/fee/lock/spend balance and cannot restore its
                // prior-ordinal balance. Addresses untouched this ordinal retain their MPT-materialized prior value.
                postEconomicBalances = priorBalances ++ updatedBalancesBySpendTransactions
                slashApplication = applyWatchtowerSlashes(
                  adoptedSlashRequests,
                  updatedCreateDelegatedStakes,
                  updatedCreateNodeCollaterals,
                  postEconomicBalances,
                  ordinal,
                  epochProgress,
                  invaliditySlashingConfig
                )
                slashRegistryEntries = slashApplication.registryEntries
                slashBountyBalanceDelta = slashApplication.bountyBalanceDelta
                _ <- Async[F].whenA(slashRegistryEntries.nonEmpty)(
                  loggerBundle.app.warn(
                    s"[ACCEPTANCE/SLASHING] ordinal=$ordinal applied watchtower invalid-state-proof slash: " +
                      s"records=${slashRegistryEntries.size} burned=${slashApplication.totalBurned} " +
                      s"bountyCredits=${slashBountyBalanceDelta.size}"
                  )
                )

                cleanedMapsResult = cleanStateMaps(
                  updatedAllowSpends,
                  updatedTokenLockBalances,
                  updatedGlobalTokenLocks,
                  slashApplication.slashedDelegatedStakes,
                  updatedWithdrawDelegatedStakes,
                  slashApplication.slashedNodeCollaterals,
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
                    priorMetagraphSyncData,
                    incomingCurrencySnapshots,
                    globalSnapshotsProcessed,
                    settledSpendActions,
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
                  // `slashBountyBalanceDelta` already carries the submitter's final credited post-economic balance, so the right-biased
                  // merge cannot erase an earlier same-ordinal debit. Empty on the reachable re-exec path (no submitter) ⇒ byte-identical.
                  postEconomicBalances ++ slashBountyBalanceDelta,
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
                  updatedAcceptedMetagraphSyncData,
                  updatedKesRegistrationCerts,
                  updatedLastKesRegistrationRefs,
                  parentTip,
                  pinnedBoundaryEta
                )
                gsi = gsiResult.gsi

                balanceChanges: SortedMap[Address, Balance] =
                  initialData.blockResult.contextUpdate.balances.toSortedMap ++
                    currencyAcceptanceBalanceUpdate.toSortedMap ++
                    rewardBalancesDelta ++
                    updatedBalancesByAllowSpendsDeltas ++
                    updatedBalancesByTokenLocksDeltas ++
                    updatedBalancesBySpendTransactionsDeltas ++
                    // WATCHTOWER slash bounty credit (final balance per submitter). Empty on the reachable re-exec path (no submitter) ⇒ the
                    // accumulator `balances` delta — and thus the MPT Balances partition + mptRoot — is byte-identical to pre-slash.
                    slashBountyBalanceDelta

                currencySnapshotsDeltas = incomingCurrencySnapshots.collect {
                  case (address, snapshots) if snapshots.nonEmpty => address -> snapshots.last
                }

                updateNodeParametersDelta = initialData.nodeParamsResult.view
                  .mapValues(unp => (unp, ordinal))
                  .to(SortedMap)

                nodeCollateralWithdrawalExpiryIndexDelta <-
                  nodeCollateralStateManager.materializeNodeCollateralWithdrawalsFromMpt.flatMap { priorWithdrawals =>
                    computeNodeCollateralWithdrawalExpiryIndexDelta(
                      priorWithdrawals,
                      updatedWithdrawNodeCollateralsCleaned,
                      withdrawalTimeLimit
                    )
                  }

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
                  removedNodeCollateralWithdrawalKeys = removedNodeCollateralWithdrawalKeys,
                  kesRegistrationCerts = kesRegistrationCertDeltas,
                  lastKesRegistrationRefs = lastKesRegistrationRefDeltas
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
                // per-branch ChangeSet). Field/insert order, rooted consensus-index maintenance and removal-key
                // derivation mirror legacy `mptStore.syncFromStateChanges` byte-for-byte —
                // `GsamWritePathParitySuite` (#107) is the regression contract.
                _ <- AcceptanceMptStateChanges.applyStateChanges[F](mpt, stateChangesAccumulator)

                // WATCHTOWER slash ledger MPT write (slashing part 3). The `Slashings` partition (fieldId 34) is NOT carried by
                // `StateChangesAccumulator` (whose shape lives in `GlobalStateConverter`, out of this change's scope), so write it DIRECTLY
                // through the same branch-aware writer algebra (`mpt.insert`) right after `applyStateChanges`. Encode the value via the
                // canonical `InvalidStateProofSlashedReader.entryCodec` — the SAME codec the reader decodes with — so the bytes are
                // deterministic and reproducible. Keys are the `(peerId, shardId, disputedCheckpointHash)` composite hash (sorted by the
                // entry tuple for a deterministic, order-independent write set). The matching entries are folded into the #107
                // verify-replay `expectedBytes` below so the writer-path `postBytes` and the replay agree (no false DIVERGED). EMPTY on the
                // reachable common path / always at numShards=1 ⇒ no key written ⇒ mptRoot byte-identical to the pre-slash path.
                slashingsMptEntries <- slashRegistryEntries
                  .sortBy(e => (e.peerId.value.value, e.shardId.value.value, e.disputedCheckpointHash.value))
                  .traverse { entry =>
                    GlobalStateKey
                      .slashingsKey[F](entry.peerId, entry.shardId, entry.disputedCheckpointHash)
                      .map(_ -> entry)
                  }
                _ <- Async[F].whenA(slashingsMptEntries.nonEmpty)(
                  mpt.insert[SlashedRegistryEntry](slashingsMptEntries.toMap)(InvalidStateProofSlashedReader.entryCodec)
                )
                // Hex-keyed byte view of the slash entries for the verify-replay fold (computed via the SAME `toHex` + `entryCodec` the
                // writer used, so the bytes are identical to those now in `postBytes`).
                slashingsReplayBytes <- slashingsMptEntries.traverse {
                  case (key, entry) =>
                    GlobalStateKey
                      .toHex[F](key)
                      .map(hex => hex -> InvalidStateProofSlashedReader.entryCodec.immutableBytes(entry).toArray)
                }.flatMap(PhysicalTrieKeyValidator.materializeEntries(_).liftTo[F])

                // ATOMIC CROSS-SHARD MESSAGE SETTLEMENT — marker WRITE (generic engine). Every registered handler's nullifier markers (their
                // partitions are NOT carried by `StateChangesAccumulator`) are written DIRECTLY through the same branch-aware writer algebra
                // (`mpt.insert`) right after `applyStateChanges` — exactly as the watchtower `Slashings` partition above — and the hex-keyed
                // byte view is folded into the #107 verify-replay `expectedBytes` below so the writer-path `postBytes` and the replay agree.
                // EMPTY at numShards=1 ⇒ no key written ⇒ mptRoot byte-identical to the pre-change path.
                consumedAllowSpendReplayBytes <- CrossShardMessageEngine.write[F](crossShardMarkers, mpt)
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
                    GlobalSnapshotInfo.mptStateProofFromBytes[F](postBytes)
                  else
                    builder.buildProof(gsi, ordinal)
                incrementalRoot = incrementalProof.mptRoot.map(_.show).getOrElse("none")

                stateProofBeforeSmt <-
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
                      // Fold the directly-written `Slashings` entries into the replay set: they are written via `mpt.insert` (not the
                      // accumulator), so `toAccumulatorHexDelta` does not see them — without this fold the writer-path `postBytes` would carry
                      // them while the replay would not, falsely tripping the #107 DIVERGED self-check. Right-biased `++` after the accumulator
                      // delta (the slash partition keys never collide with any accumulator key). Empty on the reachable path ⇒ no-op.
                      // Fold BOTH directly-written partitions (watchtower `Slashings` + cross-shard `ConsumedAllowSpends`) into the replay
                      // set: each is written via `mpt.insert` (not the accumulator), so `toAccumulatorHexDelta` doesn't see them — without
                      // this fold the writer-path `postBytes` would carry them while the replay would not, falsely tripping the #107 DIVERGED
                      // self-check. Keys in both partitions never collide with any accumulator key. Both EMPTY on the reachable common path
                      // (always at numShards=1) ⇒ no-op.
                      expectedBytes =
                        ((preSyncBytes -- deltaRemoves) ++ deltaUpserts) ++ slashingsReplayBytes ++ consumedAllowSpendReplayBytes
                      // The consensus global root commits every SystemNamespace economic index and excludes only the
                      // observation-dependent `MgGlobalSnapshotSyncView` (`GlobalStateKey.consensusRootEntries`).
                      // `incrementalProof.mptRoot` is computed over that same set; the independent verify-replay must use the same entries
                      // before rebuilding so a clean writer yields MATCH (a true writer bug still surfaces DIVERGED).
                      verifyEntries = io.constellationnetwork.schema.mpt.GlobalStateKey.consensusRootEntries(expectedBytes)
                      verifyTrie <- io.constellationnetwork.security.mpt.MerklePatriciaTrie
                        .makeParallelFromBytes[F](verifyEntries)
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
                          // [WRITER-DIVERGE-DIAG] Name the diverging field(s) + per-key byte shape so any future writer-vs-replay gap is
                          // pinned to the exact encoder immediately (incr=absent ⇒ writer removed it / repl=absent ⇒ replay kept it),
                          // not just the opaque roots. (This is how the run-372/435 `MgActiveTokenLocks` removal asymmetry was proven.)
                          val diagPostConsensus =
                            io.constellationnetwork.schema.mpt.GlobalStateKey.consensusRootEntries(postBytes)
                          val diagDivergedKeys = (diagPostConsensus.keySet ++ verifyEntries.keySet)
                            .filter(k => diagPostConsensus.get(k).map(_.toList) != verifyEntries.get(k).map(_.toList))
                          val diagByField = diagDivergedKeys.toList
                            .groupBy(k =>
                              io.constellationnetwork.schema.mpt.GlobalStateKey.fieldIdFromHex(k).map(_.toString).getOrElse("UNKNOWN")
                            )
                            .map { case (fid, ks) => s"$fid:${ks.size}" }
                            .mkString(",")
                          val diagSample = diagDivergedKeys.toList
                            .take(6)
                            .map { k =>
                              val fid = io.constellationnetwork.schema.mpt.GlobalStateKey.fieldIdFromHex(k).map(_.toString).getOrElse("?")
                              s"${k.value.take(18)}{f=$fid,incr=${diagPostConsensus.get(k).fold("absent")(b => s"${b.length}b")}," +
                                s"repl=${verifyEntries.get(k).fold("absent")(b => s"${b.length}b")}}"
                            }
                            .mkString(" ")
                          loggerBundle.app.error(
                            s"[WRITER-DIVERGE-DIAG] ordinal=$ordinal divergedFields=[$diagByField] " +
                              s"divergedKeyCount=${diagDivergedKeys.size} sample=[$diagSample]"
                          ) >>
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

                // Current active-era wiring passes no SMT store, so the signed proof remains `smtRoot = None`. Supplying a store executes
                // the staged future-era computation above, but the resulting artifact is rejected by the active-era boundary validators.
                stateProof <- attachSmtRoot(ordinal, stateProofBeforeSmt, getGlobalSnapshotByOrdinal)

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
                  settledSpendActions,
                  updatedUpdateNodeParameters.view.mapValues(_._1).toSortedMap,
                  (allowSpendsExpiredEvents ++ tokenUnlocksEvents ++ generatedTokenUnlockArtifacts).toSortedSet,
                  delegatorRewardsMap,
                  handle,
                  // Task #12 slice 2b — the typed per-ordinal delta this accept() applied (built at the
                  // `stateChangesAccumulator = StateChangesAccumulator(...)` step above, still in scope).
                  stateChangesAccumulator,
                  kesRegistrationAcceptanceResult
                )
            }
          }
        }
      }
    }
  }
}
