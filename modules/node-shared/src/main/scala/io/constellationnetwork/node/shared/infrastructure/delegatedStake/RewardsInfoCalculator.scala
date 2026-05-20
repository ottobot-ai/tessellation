package io.constellationnetwork.node.shared.infrastructure.delegatedStake

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.math.BigDecimal.RoundingMode

import io.constellationnetwork.node.shared.infrastructure.snapshot.DelegatedRewardsDistributor
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  DelegatedStakeStateManager,
  SpendTransactionBalanceManager,
  UpdateNodeParametersStateReader
}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.PricingUpdate
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.priceOracle.{PriceRecord, TokenPair}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.utils.DecimalUtils

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosLong

trait RewardsInfoCalculator[F[_]] {
  def calculateRewardsInfo(
    lastSnapshot: GlobalIncrementalSnapshot,
    lastSnapshotInfo: GlobalSnapshotInfo
  )(implicit hasher: Hasher[F]): F[Option[RewardsInfo]]
}

object RewardsInfoCalculator {

  /** §G5 — MPT-primary RewardsInfo calculation.
    *
    * `delegatedStakeStateManager` provides per-address `activeDelegatedStakes` + per-address `delegatedStakesWithdrawals` full
    * materializers. `updateNodeParametersStateReader` provides the per-Id `updateNodeParameters` full materializer.
    * `spendTransactionBalanceManager` provides the full balances map (used to compute total spendable supply). All three reads come from a
    * branch-aware MPT reader bound at construction time — under MultiBranch the view is scoped to the gl0 best-tip, matching the same
    * convention as G1's `NodeStakeAggregator.cached`.
    */
  def make[F[_]: Async](
    delegatedRewardsDistributor: DelegatedRewardsDistributor[F],
    delegatedStakeStateManager: DelegatedStakeStateManager[F],
    updateNodeParametersStateReader: UpdateNodeParametersStateReader[F],
    spendTransactionBalanceManager: SpendTransactionBalanceManager[F]
  ): RewardsInfoCalculator[F] = {
    new RewardsInfoCalculator[F] {
      override def calculateRewardsInfo(
        lastSnapshot: GlobalIncrementalSnapshot,
        lastSnapshotInfo: GlobalSnapshotInfo
      )(implicit hasher: Hasher[F]): F[Option[RewardsInfo]] =
        if (lastSnapshot.delegateRewards.getOrElse(SortedMap.empty[PeerId, Map[Address, Amount]]).isEmpty) {
          Option.empty[RewardsInfo].pure[F]
        } else {
          for {
            latestDelegateRewardsNoCommission <- getLatestDelegateRewardTotal(lastSnapshot)

            (_, _, totalDelegateStake, currentTotalSupply) <- processDelegations

            currentPrice <- toAmount(getCurrentDagPrice(lastSnapshotInfo))
            nextPrice <- getNextDagPrice(lastSnapshotInfo)

            avgRewardAmount <- calculateAverageReward(latestDelegateRewardsNoCommission, totalDelegateStake)
            emissionConfig <- delegatedRewardsDistributor.getEmissionConfig(lastSnapshot.epochProgress)
            totalRewardsPerYear <- calculateAverageRewardOverAYear(avgRewardAmount, emissionConfig.epochsPerYear)
          } yield
            Some(
              RewardsInfo(
                epochsPerYear = emissionConfig.epochsPerYear,
                currentDagPrice = currentPrice,
                nextDagPrice = nextPrice,
                totalDelegatedAmount = totalDelegateStake,
                latestAverageRewardPerDag = avgRewardAmount,
                totalDagAmount = currentTotalSupply,
                totalRewardPerEpoch = latestDelegateRewardsNoCommission,
                totalRewardsPerYearEstimate = totalRewardsPerYear
              )
            )
        }

      // §G5: prior state for stake totals + balance supply is sourced from the MPT (via the
      // injected state managers) rather than `info.activeDelegatedStakes` / `info.balances`. The
      // MPT and GSI are in sync during a healthy boot, so the numerical totals are unchanged.
      private def processDelegations(implicit hasher: Hasher[F]): F[(Amount, Amount, Amount, Amount)] =
        for {
          activeDelegatedStakes <- delegatedStakeStateManager.materializeActiveDelegatedStakesFromMpt
          pendingWithdrawals <- delegatedStakeStateManager.materializeDelegatedStakeWithdrawalsFromMpt
          balances <- spendTransactionBalanceManager.materializeAllBalancesFromMpt

          totalSpendableSupply = balances.values.map(_.value.value).sum
          totalPendingSupply = pendingWithdrawals.values.flatten.map(_.rewards.value.value).sum

          (totalStakeLocked, totalActiveRewards) = activeDelegatedStakes.values.flatten.foldLeft((0L, 0L)) {
            case ((stakeAcc, rewardsAcc), record) =>
              val stakeAmount = record.event.value.amount.value.value
              val rewardsAmount = record.rewards.value
              (stakeAcc + stakeAmount, rewardsAcc + rewardsAmount)
          }

          result <- (
            toAmount(totalStakeLocked),
            toAmount(totalActiveRewards),
            toAmount(totalStakeLocked + totalActiveRewards), // totalDelegateStake
            toAmount(totalSpendableSupply + totalPendingSupply + totalActiveRewards) // currentTotalSupply
          ).mapN(Tuple4.apply)
        } yield result

      private def getLatestDelegateRewardTotal(snapshot: GlobalIncrementalSnapshot)(implicit hasher: Hasher[F]): F[Amount] = {
        val delegateRewards = snapshot.delegateRewards.getOrElse(SortedMap.empty[PeerId, Map[Address, Amount]])

        // §G5: `updateNodeParameters` sourced from the MPT via the dedicated state reader.
        updateNodeParametersStateReader.materializeUpdateNodeParametersFromMpt.flatMap { nodeParams =>
          val calcFullReward: (Long, (PeerId, Map[Address, Amount])) => Long = {
            case (acc, (peerId, rewards)) =>
              val nodeCommissionValue = nodeParams.get(peerId.toId).map(_._1.delegatedStakeRewardParameters.reward).getOrElse(0.0)
              val nodeCommission = BigDecimal(nodeCommissionValue)

              val delegatePortion = if (nodeCommission >= 1.0) BigDecimal(0.0) else BigDecimal(1.0) - nodeCommission

              val rewardsSum = rewards.values.map(_.value.value).sum
              val rewardsBigDecimal = BigDecimal(rewardsSum)

              if (delegatePortion == BigDecimal(0.0)) acc
              else
                acc + (rewardsBigDecimal / delegatePortion)
                  .setScale(0, RoundingMode.HALF_UP)
                  .longValue
          }

          delegateRewards
            .foldLeft(0L)(calcFullReward)
            .pure[F]
            .flatMap(toAmount)
        }
      }

      private def calculateAverageReward(latestRewards: Amount, totalStakedAmount: Amount): F[BigDecimal] =
        if (totalStakedAmount.value.value === 0) BigDecimal(0).pure[F]
        else
          (BigDecimal(latestRewards.value.value) / BigDecimal(totalStakedAmount.value.value)).pure[F]

      private def calculateAverageRewardOverAYear(avgReward: BigDecimal, epochsPerYear: PosLong): F[BigDecimal] =
        (avgReward * BigDecimal(epochsPerYear.value)).pure[F]

      private def toAmount(value: Long): F[Amount] =
        if (value == 0L) Amount.empty.pure[F]
        else
          PosLong
            .from(value)
            .pure[F]
            .map(_.leftMap(new IllegalArgumentException(_)))
            .flatMap(Async[F].fromEither(_))
            .map(Amount(_))

      private def priceToLong(pricingUpdate: PricingUpdate): Long =
        (pricingUpdate.price.value.toBigDecimal * DecimalUtils.DATUM_USD).setScale(0, RoundingMode.HALF_UP).longValue

      private def getCurrentDagPrice(info: GlobalSnapshotInfo): Long =
        info.priceState
          .getOrElse(SortedMap.empty[TokenPair, PriceRecord])
          .get(TokenPair.DAG_USD)
          .map(priceRecord => priceToLong(priceRecord.currentPrice))
          .getOrElse(0L)

      private def getNextDagPrice(info: GlobalSnapshotInfo): F[NextDagPrice] = {
        val maybePriceRecord = info.priceState
          .getOrElse(SortedMap.empty[TokenPair, PriceRecord])
          .get(TokenPair.DAG_USD)

        maybePriceRecord match {
          case Some(priceRecord) =>
            val priceValue = priceToLong(priceRecord.upcomingPrice)
            PosLong
              .from(priceValue)
              .fold(
                err => Async[F].raiseError(new IllegalArgumentException(s"Failed to create positive price: $err")),
                posLong =>
                  NextDagPrice(
                    price = Amount(posLong),
                    asOfEpoch = priceRecord.nextWindowChange
                  ).pure[F]
              )
          case None =>
            PosLong
              .from(1L)
              .fold(
                err => Async[F].raiseError(new IllegalArgumentException(s"Failed to create positive epoch: $err")),
                posLong =>
                  NextDagPrice(
                    price = Amount.empty,
                    asOfEpoch = EpochProgress(posLong)
                  ).pure[F]
              )
        }
      }
    }
  }
}
