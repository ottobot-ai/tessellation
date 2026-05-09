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
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore, MptTxAction}
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
    incrementalDelegatedStakingStartingOrdinal: SnapshotOrdinal,
    overlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey]
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

        // Branch identity for the algebra: the parent snapshot's hash. Under the active
        // `OverlayMode.Passthrough` wiring (Phase D) this is ignored on every read/write; under
        // a future `MultiBranch` flip it identifies which pending branch's view to read at.
        lastArtifactHash <- HasherSelector[F].forOrdinal(lastArtifact.ordinal)(implicit h => h.hash(lastArtifact.value))

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

        // Wrap accept() + state-proof verification in a transaction. On mismatch we raise
        // StateProofMismatch; the bracket auto-rolls back the MPT mutations before re-raising.
        // On success the bracket commits.
        //
        // Why: `syncFromStateChanges` inside `accept()` commits its delta to MPT before we get
        // a chance to compare against the peer-claimed stateProof; a mismatch would leave the
        // store with divergent bytes and the journal tip advanced, contaminating future rounds.
        //
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
        snapshotInfo <- mptStore.withTransaction {
          snapshotAcceptanceManager
            .accept(
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
              getGlobalSnapshotByOrdinal,
              // Follower-side validation: branch identity is the parent's hash so the algebra reads at
              // the same parent view the proposer constructed against. Phase D (Passthrough) ignores
              // the BranchId on every read/write — the byte-parity contract from #107 covers the rewire.
              io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(lastArtifactHash)
            )
            .flatMap {
              case (
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
                    _,
                    overlayHandle
                  ) =>
                // For followers (currency-l0, dag-l1, currency-l1), we log warnings instead of raising errors
                // for blocks, state channels, and rewards validation divergences.
                // The snapshot was already validated by dag-l0 majority consensus, so local acceptance
                // divergences on followers should not cause permanent stalls. The same rationale applies
                // as for skipping state proof validation below.
                for {
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
                  // incoming signed artifact. A mismatch raises StateProofMismatch — the surrounding
                  // `mptStore.withTransaction` bracket auto-rolls back the MPT mutations on raise,
                  // so divergent bytes don't leak into future rounds.
                  //
                  // Scope: under MPT-as-primary, the `mptRoot` is THE canonical state proof; the
                  // legacy 16-field proofs are read-only artifacts carried for backward compat and
                  // are not the source of truth post-migration, so we don't compare them. For
                  // LegacyFormat ordinals both mptRoots are `None` and the check is a no-op.
                  mismatch = computedStateProof.mptRoot =!= signedArtifact.stateProof.mptRoot
                  _ <-
                    if (mismatch) {
                      val perFieldDiffs = perFieldRootDiffs(computedStateProof, signedArtifact.stateProof)
                      val diffSuffix = if (perFieldDiffs.isEmpty) "" else s" — diffs: ${perFieldDiffs.mkString(", ")}"
                      // #117 diag: when balances or lastStateChannelSnapshotHashes diverge, log the
                      // local-side delta vs prior context so we can correlate the divergence to a
                      // specific entry. The leader's full GSI isn't available locally — we can only
                      // log what THIS node computed; cross-correlating with another node's logs
                      // (via the same per-key fingerprints) localizes the offending key.
                      val priorBalances = context.balances
                      val computedBalances = snapshotInfo.balances
                      val balDeltaKeys = (priorBalances.keySet ++ computedBalances.keySet).filter { addr =>
                        priorBalances.get(addr) != computedBalances.get(addr)
                      }
                      val balDeltaSample = balDeltaKeys.toList.take(10).map { addr =>
                        val before = priorBalances.get(addr).map(_.value.value.toString).getOrElse("none")
                        val after = computedBalances.get(addr).map(_.value.value.toString).getOrElse("none")
                        s"${addr.value.value.take(10)}=$before->$after"
                      }
                      val priorScHashes = context.lastStateChannelSnapshotHashes
                      val computedScHashes = snapshotInfo.lastStateChannelSnapshotHashes
                      val scDeltaKeys = (priorScHashes.keySet ++ computedScHashes.keySet).filter { addr =>
                        priorScHashes.get(addr) != computedScHashes.get(addr)
                      }
                      val scDeltaSample = scDeltaKeys.toList.take(10).map { addr =>
                        val before = priorScHashes.get(addr).map(_.show.take(8)).getOrElse("none")
                        val after = computedScHashes.get(addr).map(_.show.take(8)).getOrElse("none")
                        s"${addr.value.value.take(10)}=$before->$after"
                      }
                      logger.error(
                        s"StateProofMismatch at ordinal=${signedArtifact.ordinal.show}: " +
                          s"computed.mptRoot=${computedStateProof.mptRoot.map(_.show.take(12)).getOrElse("none")} " +
                          s"claimed.mptRoot=${signedArtifact.stateProof.mptRoot.map(_.show.take(12)).getOrElse("none")}" +
                          diffSuffix +
                          s" — rolling back MPT (transaction will rollback)"
                      ) >> logger.error(
                        s"StateProofMismatch at ordinal=${signedArtifact.ordinal.show} #117 DIAG " +
                          s"prior.balances.size=${priorBalances.size} computed.balances.size=${computedBalances.size} " +
                          s"balDeltaCount=${balDeltaKeys.size} balDeltaSample=[${balDeltaSample.mkString(",")}] " +
                          s"prior.scHashes.size=${priorScHashes.size} computed.scHashes.size=${computedScHashes.size} " +
                          s"scDeltaCount=${scDeltaKeys.size} scDeltaSample=[${scDeltaSample.mkString(",")}] " +
                          s"signed.lastSnapshotHash=${signedArtifact.value.lastSnapshotHash.show.take(12)} " +
                          s"signed.scSnapshots.metagraphs=${signedArtifact.stateChannelSnapshots.size}"
                      ) >> Async[F].raiseError[(GlobalSnapshotInfo, MptTxAction)](
                        StateProofMismatch(
                          ordinal = signedArtifact.ordinal,
                          computed = computedStateProof,
                          claimed = signedArtifact.stateProof
                        )
                      )
                    } else Async[F].unit
                  // Phase J: hash the incoming signed artifact and commit the overlay handle under
                  // the resulting `BranchId(snapshotHash)`. This is the follower-side counterpart of
                  // the proposer's `overlay.commit(handle, BranchId(currentSnapshotHash), ordinal)`
                  // call — keeping both sides committed under the same id is what lets ChainSync
                  // pull a follower's pending branch by snapshot hash when sync clients ask for it.
                  // Done AFTER the mismatch check raises (so divergent state never gets registered
                  // under a real childTip), and BEFORE the transaction yields Commit (so failure
                  // here triggers the rollback path).
                  signedArtifactHash <- hasher.hash(signedArtifact.value)
                  branchId = io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(signedArtifactHash)
                  _ <- overlay.commit(overlayHandle, branchId, signedArtifact.ordinal)
                  // Followers (gl1/cl1/dl1) and gl0 history-replay paths process snapshots through
                  // createContext one at a time. The common case is a linear canonical stream;
                  // recovery flows (`setForRecovery` resetting LastSnapshotStorage to ord N < current
                  // and re-pulling) re-validate the same ordinal under a possibly different canonical
                  // hash. Without an immediate fold, pendingRef grows until eviction (cap=4) drops a
                  // parent ancestor, breaking the chain walk; subsequent reads fall through to a stale
                  // base, surfacing as systematic StateProofMismatch on followers (#113). The reorg-replay
                  // case is handled by `MptOverlay.finalizeBranch`'s reorg-replace path: a different
                  // canonical at the same ordinal updates the finality marker (idempotent if pending is
                  // already empty). gl0's live consensus path keeps overlay finalization in
                  // NakamotoSyncDaemon (k-confirmed via chain store) and does not call createContext,
                  // so this immediate fold only affects the follower / history-replay callers.
                  _ <- overlay.finalizeBranch(branchId, signedArtifact.ordinal)
                } yield (snapshotInfo, MptTxAction.Commit: MptTxAction)
            }
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
