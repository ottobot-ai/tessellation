package io.constellationnetwork.node.shared.domain.priceOracle

import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.node.shared.config.DelegatedRewardsConfigProvider
import io.constellationnetwork.node.shared.config.types.EmissionConfigEntry
import io.constellationnetwork.schema.NonNegFraction
import io.constellationnetwork.schema.artifact.PricingUpdate
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.priceOracle.TokenPair.DAG_USD
import io.constellationnetwork.schema.priceOracle.{PriceFraction, PriceRecord, TokenPair}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.serde.codecs.instances.PriceOracleCodecs.priceRecordImmutableCodec
import io.constellationnetwork.syntax.sortedCollection.sortedMapSyntax

import eu.timepit.refined.cats.posIntCommutativeSemigroup
import eu.timepit.refined.types.numeric.PosInt
import monocle.Monocle.toAppliedFocusOps

trait PriceStateUpdater[F[_]] {

  /** Given the prior full price state and this ordinal's pricing updates, return the **delta** map — only the `TokenPair`s whose record
    * changed. The caller merges with prior state where a full map is still needed (e.g. `buildGlobalSnapshotInfo`).
    */
  def updatePriceState(
    lastPriceState: SortedMap[TokenPair, PriceRecord],
    acceptedPricingUpdates: List[PricingUpdate],
    epochProgress: EpochProgress
  )(implicit hasher: Hasher[F]): F[SortedMap[TokenPair, PriceRecord]]

  /** Recover the full `priceState` map from MPT via prefix-scan over the `PriceState` partition. The TokenPair key isn't recoverable from
    * the hashed key bytes, so we extract it from each `PriceRecord.currentPrice.price.tokenPair` (the value carries its own pair).
    */
  def materializePriceStateFromMpt(implicit hasher: Hasher[F]): F[SortedMap[TokenPair, PriceRecord]]

}

object PriceStateUpdater {
  def make[F[_]: Async](
    environment: AppEnvironment,
    delegatedRewardsConfigProvider: DelegatedRewardsConfigProvider,
    mptStore: Option[MptStore[F, GlobalStateKey]] = None,
    shouldUseMptStore: Boolean = false
  ): PriceStateUpdater[F] = new PriceStateUpdater[F] {

    override def updatePriceState(
      lastPriceState: SortedMap[TokenPair, PriceRecord],
      acceptedPricingUpdates: List[PricingUpdate],
      epochProgress: EpochProgress
    )(implicit hasher: Hasher[F]): F[SortedMap[TokenPair, PriceRecord]] =
      if (acceptedPricingUpdates.isEmpty) {
        SortedMap.empty[TokenPair, PriceRecord].pure[F]
      } else {
        for {
          emissionConfig <- getEmissionConfig(epochProgress)
          deltas <- acceptedPricingUpdates
            .groupBy(_.tokenPair)
            .toList
            .traverse {
              case (tokenPair, updates) =>
                readPrior(tokenPair, lastPriceState).flatMap { prior =>
                  aggregateUpdates(NonEmptyList.fromListUnsafe(updates)).flatMap { aggregated =>
                    buildRecord(tokenPair, prior, aggregated, epochProgress, emissionConfig)
                      .map(rec => (tokenPair, rec))
                  }
                }
            }
            .map(_.toSortedMap)
        } yield deltas
      }

    private def readPrior(
      tokenPair: TokenPair,
      lastPriceState: SortedMap[TokenPair, PriceRecord]
    )(implicit hasher: Hasher[F]): F[Option[PriceRecord]] =
      if (shouldUseMptStore) mptStore.fold(Option.empty[PriceRecord].pure[F])(_.getPriceRecord(tokenPair))
      else lastPriceState.get(tokenPair).pure[F]

    override def materializePriceStateFromMpt(
      implicit hasher: Hasher[F]
    ): F[SortedMap[TokenPair, PriceRecord]] =
      mptStore match {
        case None => SortedMap.empty[TokenPair, PriceRecord].pure[F]
        case Some(store) =>
          for {
            prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.PriceState)
            entries <- store.getAllForPrefix[PriceRecord](prefix)
          } yield SortedMap.from(entries.values.map(rec => rec.currentPrice.price.tokenPair -> rec))
      }

    private def buildRecord(
      tokenPair: TokenPair,
      priorOpt: Option[PriceRecord],
      aggregated: PricingUpdate,
      epochProgress: EpochProgress,
      emissionConfig: EmissionConfigEntry
    ): F[PriceRecord] =
      priorOpt match {
        case None =>
          for {
            initialCurrentPrice <- initialPrice(tokenPair, emissionConfig)
            initialUpcomingPrice <- initialPrice(tokenPair, emissionConfig)
          } yield
            PriceRecord(
              currentPrice = initialCurrentPrice,
              upcomingPrice = initialUpcomingPrice,
              currentSum = aggregated,
              currentNumEvents = PosInt(1),
              nextWindowChange = epochProgress |+| EpochProgress(emissionConfig.epochsPerMonth),
              updatedAt = epochProgress
            )
        case Some(prior) =>
          if (prior.nextWindowChange <= epochProgress) {
            for {
              newUpcomingPrice <- aggregateUpdates(
                NonEmptyList.of(prior.currentSum),
                prior.currentNumEvents.some
              )
            } yield
              PriceRecord(
                currentPrice = prior.upcomingPrice,
                upcomingPrice = newUpcomingPrice,
                currentSum = aggregated,
                currentNumEvents = PosInt(1),
                nextWindowChange = prior.nextWindowChange |+| EpochProgress(emissionConfig.epochsPerMonth),
                updatedAt = epochProgress
              )
          } else {
            val newCurrentNumEvents = prior.currentNumEvents |+| PosInt(1)
            for {
              newCurrentSum <- addUpdates(prior.currentSum, aggregated)
            } yield
              prior
                .focus(_.currentSum)
                .replace(newCurrentSum)
                .focus(_.currentNumEvents)
                .replace(newCurrentNumEvents)
                .focus(_.updatedAt)
                .replace(epochProgress)
          }
      }

    def getEmissionConfig(epochProgress: EpochProgress): F[EmissionConfigEntry] =
      delegatedRewardsConfigProvider
        .getConfig()
        .emissionConfig
        .get(environment)
        .pure[F]
        .flatMap(Async[F].fromOption(_, new RuntimeException(s"Could not retrieve emission config for env: $environment")))
        .map(f => f(epochProgress))
  }

  def aggregateUpdates[F[_]: Async](updates: NonEmptyList[PricingUpdate], numEvents: Option[PosInt] = None): F[PricingUpdate] = {
    val n = numEvents.map(_.value).getOrElse(updates.size)
    if (n == 1) {
      updates.head.pure[F]
    } else {
      val sum = updates.toList.view.map(_.price.value.toBigDecimal).sum
      for {
        value <- NonNegFraction.fromBigDecimal(sum / n)
        price = PriceFraction(tokenPair = updates.head.tokenPair, value = value)
      } yield PricingUpdate(price)
    }
  }

  def addUpdates[F[_]: Async](update1: PricingUpdate, update2: PricingUpdate): F[PricingUpdate] = {
    val value1 = update1.price.value.toBigDecimal
    val value2 = update2.price.value.toBigDecimal
    for {
      value <- NonNegFraction.fromBigDecimal(value1 + value2)
      price = PriceFraction(tokenPair = update1.tokenPair, value = value)
    } yield PricingUpdate(price)
  }

  private def initialPrice[F[_]: Async](tokenPair: TokenPair, emConfig: EmissionConfigEntry): F[PricingUpdate] = {
    val dagPrices = emConfig.dagPrices
    for {
      _ <- if (tokenPair == DAG_USD) tokenPair.pure[F] else Async[F].raiseError(new RuntimeException(s"Unsupported token pair $tokenPair"))
      price <- if (dagPrices.isEmpty) NonNegFraction(0L, 1L) else dagPrices.values.headOption.getOrElse(dagPrices.head._2).pure[F]
    } yield PricingUpdate(PriceFraction(tokenPair, price))
  }

}
