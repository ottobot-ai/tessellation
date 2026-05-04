package io.constellationnetwork.node.shared.infrastructure.snapshot

import cats.Parallel
import cats.data.Validated
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.util.control.NoStackTrace

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceManager
import io.constellationnetwork.node.shared.domain.snapshot.SnapshotContextFunctions
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.TimeTrigger
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.GlobalSnapshotAcceptanceManager
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.UpdateNodeCollateral
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.transaction.RewardTransaction
import io.constellationnetwork.security._
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelValidationType}

import derevo.cats.{eqv, show}
import derevo.derive
import eu.timepit.refined.types.all.NonNegLong
import io.circe.Json
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

abstract class GlobalSnapshotContextFunctions[F[_]] extends SnapshotContextFunctions[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]

object GlobalSnapshotContextFunctions {

  def make[F[_]: Async: Parallel: HasherSelector: SecurityProvider: JsonSerializer](
    snapshotAcceptanceManager: GlobalSnapshotAcceptanceManager[F],
    updateDelegatedStakeAcceptanceManager: UpdateDelegatedStakeAcceptanceManager[F],
    withdrawalTimeLimit: EpochProgress,
    tessellation3MigrationStartingOrdinal: SnapshotOrdinal,
    setSumFixOrdinal: SnapshotOrdinal,
    mptStore: MptStore[F, GlobalStateKey],
    incrementalDelegatedStakingStartingOrdinal: SnapshotOrdinal
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector
  ) =
    new GlobalSnapshotContextFunctions[F] {
      private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

      // Note: State proof validation is skipped for followers (currency-l0, dag-l1) because:
      // 1. The snapshot was already validated by dag-l0 majority consensus
      // 2. Full MPT sync on every ordinal is too expensive (~2 min for 800K entries)
      // The MPT store is synced once during initial download for query support.

      def createContext(
        context: GlobalSnapshotInfo,
        lastArtifact: Signed[GlobalIncrementalSnapshot],
        signedArtifact: Signed[GlobalIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[F]): F[GlobalSnapshotInfo] = for {
        lastActiveTips <- HasherSelector[F].forOrdinal(lastArtifact.ordinal)(implicit hasher => lastArtifact.activeTips)

        lastDeprecatedTips = lastArtifact.tips.deprecated

        blocksForAcceptance = signedArtifact.blocks.toList.map(_.block)
        allowSpendBlocksForAcceptance = signedArtifact.allowSpendBlocks.map(_.toList).getOrElse(List.empty)
        tokenLockBlocksForAcceptance = signedArtifact.tokenLockBlocks.map(_.toList).getOrElse(List.empty)

        scEvents = signedArtifact.stateChannelSnapshots.toList.flatMap {
          case (address, stateChannelBinaries) => stateChannelBinaries.map(StateChannelOutput(address, _)).toList
        }

        unpEventsForAcceptance = signedArtifact.updateNodeParameters
          .getOrElse(SortedMap.empty[Id, Signed[UpdateNodeParameters]])
          .values
          .toList

        cdsEventsForAcceptance = signedArtifact.activeDelegatedStakes
          .getOrElse(SortedMap.empty[Address, List[Signed[UpdateDelegatedStake.Create]]])
          .values
          .toList
          .flatten

        wdsEventsForAcceptance = signedArtifact.delegatedStakesWithdrawals
          .getOrElse(SortedMap.empty[Address, List[Signed[UpdateDelegatedStake.Withdraw]]])
          .values
          .toList
          .flatten

        cncEventsForAcceptance = signedArtifact.activeNodeCollaterals
          .getOrElse(SortedMap.empty[Address, List[Signed[UpdateNodeCollateral.Create]]])
          .values
          .toList
          .flatten

        wncEventsForAcceptance = signedArtifact.nodeCollateralWithdrawals
          .getOrElse(SortedMap.empty[Address, List[Signed[UpdateNodeCollateral.Withdraw]]])
          .values
          .toList
          .flatten

        // Capture the pre-accept MPT state so we can roll back on verify failure.
        // Without this, `syncFromStateChanges` inside `accept()` commits its delta before
        // we get a chance to compare against the peer-claimed stateProof; a mismatch would
        // leave the store with divergent bytes and the journal tip advanced.
        preAcceptSavepoint <- mptStore.savepoint

        // Derive updated stake/withdrawal maps inside calculateRewardsFn using the
        // `DelegateRewardsInput.psu` argument — that PartitionedStakeUpdates is produced by
        // the canonical `DelegatedStakeStateManager.processExistingDelegatedStakes` inside
        // `accept()`. The previous code ran a drifted-duplicate `acceptDelegatedStakes` helper
        // here and pre-computed these maps off its output; that helper only partitioned
        // withdrawals by epoch expiry, while the canonical fn also filters by whether the
        // associated token lock is still active. The divergence surfaced as StateProofMismatch
        // on followers whenever a withdrawal became epoch-expired AND its token lock had been
        // replaced/removed in the same ordinal (observed at ordinal 64 in cl1, e.g.
        // gl0.mptRoot=221e53b31179 vs cl1.mptRoot=6fc964752c15).
        (
          acceptanceResult,
          _,
          _,
          _,
          _,
          _,
          returnedSCEvents,
          acceptedRewardTxs,
          snapshotInfo,
          computedStateProof,
          _,
          _,
          _,
          _
        ) <-
          snapshotAcceptanceManager.accept(
            signedArtifact.ordinal,
            signedArtifact.epochProgress,
            lastArtifact.epochProgress,
            blocksForAcceptance,
            allowSpendBlocksForAcceptance,
            tokenLockBlocksForAcceptance,
            scEvents,
            unpEventsForAcceptance,
            cdsEventsForAcceptance,
            wdsEventsForAcceptance,
            cncEventsForAcceptance,
            wncEventsForAcceptance,
            context,
            lastActiveTips,
            lastDeprecatedTips,
            (input: RewardsInput) => {
              val rewardTxs = signedArtifact.rewards
              input match {
                case ClassicRewardsInput(_) =>
                  // Pre-tessellation3 rewards distribution — no stake-update plumbing applies.
                  DelegatedRewardsResult(
                    delegatorRewardsMap = SortedMap.empty,
                    updatedCreateDelegatedStakes = SortedMap.empty,
                    updatedWithdrawDelegatedStakes = SortedMap.empty,
                    nodeOperatorRewards = rewardTxs,
                    reservedAddressRewards = SortedSet.empty,
                    withdrawalRewardTxs = SortedSet.empty,
                    totalEmittedRewardsAmount =
                      Amount(NonNegLong.unsafeFrom(rewardTxs.toList.map(_.amount.value.value).distinct.sum)) // mimic incorrect behaviour
                  ).pure[F]

                case DelegateRewardsInput(udsar, psu, _) =>
                  for {
                    updatedCreateDelegatedStakes <- DelegatedRewardsDistributor.getUpdatedCreateDelegatedStakes(
                      signedArtifact.delegateRewards.getOrElse(SortedMap.empty[PeerId, Map[Address, Amount]]),
                      udsar,
                      psu
                    )
                    updatedWithdrawDelegatedStakes <- DelegatedRewardsDistributor.getUpdatedWithdrawalDelegatedStakes(
                      context,
                      udsar,
                      psu
                    )
                    transformedCreateDelegatedStakes =
                      if (signedArtifact.ordinal > incrementalDelegatedStakingStartingOrdinal)
                        updatedCreateDelegatedStakes.view.mapValues { records =>
                          records.map { r =>
                            r.copy(
                              currentTokenLockRef = r.currentTokenLockRef.orElse(r.tokenLockRef.some),
                              currentAmount = r.currentAmount.orElse(r.amount.some)
                            )
                          }
                        }.to(SortedMap)
                      else updatedCreateDelegatedStakes
                  } yield
                    if (signedArtifact.ordinal.value < setSumFixOrdinal.value)
                      DelegatedRewardsResult(
                        delegatorRewardsMap = signedArtifact.delegateRewards
                          .getOrElse(SortedMap.empty[PeerId, Map[Address, Amount]]),
                        updatedCreateDelegatedStakes = transformedCreateDelegatedStakes,
                        updatedWithdrawDelegatedStakes = updatedWithdrawDelegatedStakes,
                        nodeOperatorRewards = rewardTxs,
                        reservedAddressRewards = SortedSet.empty,
                        withdrawalRewardTxs = SortedSet.empty,
                        totalEmittedRewardsAmount = Amount(
                          NonNegLong.unsafeFrom(rewardTxs.toList.map(_.amount.value.value).distinct.sum)
                        ) // mimic incorrect behaviour
                      )
                    else
                      DelegatedRewardsResult(
                        delegatorRewardsMap = signedArtifact.delegateRewards
                          .getOrElse(SortedMap.empty[PeerId, Map[Address, Amount]]),
                        updatedCreateDelegatedStakes = transformedCreateDelegatedStakes,
                        updatedWithdrawDelegatedStakes = updatedWithdrawDelegatedStakes,
                        nodeOperatorRewards = rewardTxs,
                        reservedAddressRewards = SortedSet.empty,
                        withdrawalRewardTxs = SortedSet.empty,
                        totalEmittedRewardsAmount = Amount(NonNegLong.unsafeFrom(rewardTxs.toList.map(_.amount.value.value).sum))
                      )
              }
            },
            StateChannelValidationType.Historical,
            getGlobalSnapshotByOrdinal
          )

        // For followers (currency-l0, dag-l1, currency-l1), we log warnings instead of raising errors
        // for blocks, state channels, and rewards validation divergences.
        // The snapshot was already validated by dag-l0 majority consensus, so local acceptance
        // divergences on followers should not cause permanent stalls. The same rationale applies
        // as for skipping state proof validation below.
        _ <- logger
          .warn(
            s"Follower: ${acceptanceResult.notAccepted.size} blocks not accepted at ordinal=${signedArtifact.ordinal.show}. " +
              s"Reasons: ${acceptanceResult.notAccepted.map { case (_, reason) => reason }.mkString(", ")}. " +
              s"Continuing since snapshot was already validated by L0 majority consensus."
          )
          .whenA(acceptanceResult.notAccepted.nonEmpty)
        _ <- logger
          .warn(
            s"Follower: ${returnedSCEvents.size} state channels returned at ordinal=${signedArtifact.ordinal.show} " +
              s"for addresses: ${returnedSCEvents.toList.map(_.address).mkString(", ")}. " +
              s"Continuing since snapshot was already validated by L0 majority consensus."
          )
          .whenA(returnedSCEvents.nonEmpty)
        diffRewards = acceptedRewardTxs -- signedArtifact.rewards
        _ <- logger
          .warn(
            s"Follower: ${diffRewards.size} rewards not accepted at ordinal=${signedArtifact.ordinal.show}. " +
              s"Continuing since snapshot was already validated by L0 majority consensus."
          )
          .whenA(diffRewards.nonEmpty)
        // State-proof cross-check: compare the MPT root we computed locally (by applying
        // the same delta through accept()) against the MPT root the leader claimed in the
        // incoming signed artifact. A mismatch means either our state diverged or the claim
        // is wrong — either way we MUST NOT silently commit divergent state.
        //
        // Scope: under MPT-as-primary, the `mptRoot` is THE canonical state proof; the
        // legacy 16-field proofs are read-only artifacts carried for backward compat and
        // are not the source of truth post-migration, so we don't compare them. For
        // LegacyFormat ordinals both mptRoots are `None` and the check is a no-op.
        //
        // On mismatch we restore the MPT from the pre-accept savepoint BEFORE raising —
        // otherwise `syncFromStateChanges` has already committed the delta and the journal
        // tip has advanced, leaving the store with divergent bytes for upstream retries.
        //
        // The previous policy explicitly skipped this check entirely with the rationale
        // "too expensive" — which surfaced the class of silent-divergence bug a partition
        // test exposed. Now with typed-scodec MPT the cost is tolerable, and silent
        // divergence is not acceptable.
        _ <- {
          val mismatch = computedStateProof.mptRoot =!= signedArtifact.stateProof.mptRoot
          val restore = preAcceptSavepoint.restore
          val raise = Async[F].raiseError[Unit](
            StateProofMismatch(
              ordinal = signedArtifact.ordinal,
              computed = computedStateProof,
              claimed = signedArtifact.stateProof
            )
          )
          val perFieldDiffs = perFieldRootDiffs(computedStateProof, signedArtifact.stateProof)
          val diffSuffix = if (perFieldDiffs.isEmpty) "" else s" — diffs: ${perFieldDiffs.mkString(", ")}"
          (logger.error(
            s"StateProofMismatch at ordinal=${signedArtifact.ordinal.show}: " +
              s"computed.mptRoot=${computedStateProof.mptRoot.map(_.show.take(12)).getOrElse("none")} " +
              s"claimed.mptRoot=${signedArtifact.stateProof.mptRoot.map(_.show.take(12)).getOrElse("none")}" +
              diffSuffix +
              s" — rolling back MPT to pre-accept savepoint"
          ) >> restore >> raise).whenA(mismatch)
        }

      } yield snapshotInfo
    }

  @derive(eqv, show)
  case class CannotApplyBlocksError(reasons: List[BlockNotAcceptedReason]) extends NoStackTrace {

    override def getMessage: String =
      s"Cannot build global snapshot ${reasons.show}"
  }

  @derive(eqv)
  case class CannotApplyStateChannelsError(returnedStateChannels: Set[StateChannelOutput]) extends NoStackTrace {

    override def getMessage: String =
      s"Cannot build global snapshot because of returned StateChannels for addresses: ${returnedStateChannels.map(_.address).show}"
  }

  @derive(eqv, show)
  case class CannotApplyRewardsError(notAcceptedRewards: SortedSet[RewardTransaction]) extends NoStackTrace {

    override def getMessage: String =
      s"Cannot build global snapshot because of not accepted rewards: ${notAcceptedRewards.show}"
  }

  /** Local `accept()` computed a state proof that does not match the one the incoming snapshot claims. Rejecting this snapshot is the only
    * safe action: committing would silently carry a divergent MPT root forward, defeating peer attestation.
    */
  case class StateProofMismatch(
    ordinal: SnapshotOrdinal,
    computed: GlobalSnapshotStateProof,
    claimed: GlobalSnapshotStateProof
  ) extends NoStackTrace {
    override def getMessage: String = {
      val cRoot = computed.mptRoot.map(_.show.take(12)).getOrElse("none")
      val lRoot = claimed.mptRoot.map(_.show.take(12)).getOrElse("none")
      s"StateProofMismatch at ordinal=${ordinal.show}: computed.mptRoot=$cRoot claimed.mptRoot=$lRoot"
    }
  }

  // Field-level diff between two GlobalSnapshotStateProofs. Returns one entry per differing field so that the
  // mismatch log identifies WHICH subtree diverged, not just that the top-level mptRoot differs. This is a
  // pure helper — extracted so the unit tests (and operators reading logs) can correlate divergence to a
  // specific manager. Adding a new GlobalSnapshotStateProof field requires adding a row here.
  private[snapshot] def perFieldRootDiffs(
    computed: GlobalSnapshotStateProof,
    claimed: GlobalSnapshotStateProof
  ): List[String] = {
    def shortHash(h: io.constellationnetwork.security.hash.Hash): String = h.show.take(12)
    def shortMerkle(m: io.constellationnetwork.merkletree.MerkleRoot): String = m.show.take(12)
    def diffHash(
      label: String,
      a: io.constellationnetwork.security.hash.Hash,
      b: io.constellationnetwork.security.hash.Hash
    ): Option[String] =
      if (a === b) None else Some(s"$label(c=${shortHash(a)},l=${shortHash(b)})")
    def diffOptHash(
      label: String,
      a: Option[io.constellationnetwork.security.hash.Hash],
      b: Option[io.constellationnetwork.security.hash.Hash]
    ): Option[String] =
      if (a === b) None else Some(s"$label(c=${a.map(shortHash).getOrElse("none")},l=${b.map(shortHash).getOrElse("none")})")
    def diffOptMerkle(
      label: String,
      a: Option[io.constellationnetwork.merkletree.MerkleRoot],
      b: Option[io.constellationnetwork.merkletree.MerkleRoot]
    ): Option[String] =
      if (a === b) None else Some(s"$label(c=${a.map(shortMerkle).getOrElse("none")},l=${b.map(shortMerkle).getOrElse("none")})")
    List(
      diffHash("lastStateChannelSnapshotHashes", computed.lastStateChannelSnapshotHashesProof, claimed.lastStateChannelSnapshotHashesProof),
      diffHash("lastTxRefs", computed.lastTxRefsProof, claimed.lastTxRefsProof),
      diffHash("balances", computed.balancesProof, claimed.balancesProof),
      diffOptMerkle("lastCurrencySnapshots", computed.lastCurrencySnapshotsProof, claimed.lastCurrencySnapshotsProof),
      diffOptHash("activeAllowSpends", computed.activeAllowSpends, claimed.activeAllowSpends),
      diffOptHash("activeTokenLocks", computed.activeTokenLocks, claimed.activeTokenLocks),
      diffOptHash("tokenLockBalances", computed.tokenLockBalances, claimed.tokenLockBalances),
      diffOptHash("lastAllowSpendRefs", computed.lastAllowSpendRefs, claimed.lastAllowSpendRefs),
      diffOptHash("lastTokenLockRefs", computed.lastTokenLockRefs, claimed.lastTokenLockRefs),
      diffOptHash("updateNodeParameters", computed.updateNodeParameters, claimed.updateNodeParameters),
      diffOptHash("activeDelegatedStakes", computed.activeDelegatedStakes, claimed.activeDelegatedStakes),
      diffOptHash("delegatedStakesWithdrawals", computed.delegatedStakesWithdrawals, claimed.delegatedStakesWithdrawals),
      diffOptHash("activeNodeCollaterals", computed.activeNodeCollaterals, claimed.activeNodeCollaterals),
      diffOptHash("nodeCollateralWithdrawals", computed.nodeCollateralWithdrawals, claimed.nodeCollateralWithdrawals),
      diffOptHash("priceState", computed.priceState, claimed.priceState),
      diffOptHash("lastGlobalSnapshotsWithCurrency", computed.lastGlobalSnapshotsWithCurrency, claimed.lastGlobalSnapshotsWithCurrency)
    ).flatten
  }
}
