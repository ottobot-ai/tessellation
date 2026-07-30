package io.constellationnetwork.dag.l0.infrastructure.rewards

import java.math.MathContext

import cats.effect.IO
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.math.BigDecimal.RoundingMode

import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.{DefaultDelegatedRewardsConfigProvider, MainnetRewardsConfig}
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.priceOracle.{PriceStateUpdater, PricingUpdateValidator}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  DelegatedStakeStateManager,
  UpdateNodeParametersStateReader
}
import io.constellationnetwork.schema.AmountOps._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.PricingUpdate
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.priceOracle.{PriceFraction, PriceRecord, TokenPair}
import io.constellationnetwork.schema.{GlobalSnapshotInfo, NonNegFraction}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.utils.DecimalUtils

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import weaver.SimpleIOSuite

/** Characterizes a consensus-visible divergence admitted by the current reward parameter types.
  *
  * This is evidence only. `StrictMath` is used as a second specification-permitted elementary-function implementation; this suite does not
  * endorse it as the replacement reward curve.
  */
object GlobalDelegatedRewardsMathDeterminismRedSuite extends SimpleIOSuite {

  private val mc = new MathContext(24, java.math.RoundingMode.HALF_UP)
  private val epoch = EpochProgress(NonNegLong.unsafeFrom(1001274L))
  private val firstUpdateEpoch = EpochProgress(NonNegLong.unsafeFrom(1001105L))
  private val secondUpdateEpoch = EpochProgress(NonNegLong.unsafeFrom(1001189L))
  private val thirdUpdateEpoch = EpochProgress(NonNegLong.unsafeFrom(1001273L))
  private val witnessPrice = NonNegFraction.unsafeFrom(92233722547509450L, Long.MaxValue)
  private val metagraphId = Address("DAG4QSG19fPchE5xVpEDA6Y1fE2F7XcSFXJvzvHo")
  private val config = DefaultDelegatedRewardsConfigProvider.getConfig()
  private val emission = config.emissionConfig(AppEnvironment.Testnet)(epoch)

  private def calculateWithStrictMath: Long = {
    val iTarget = emission.iTarget.toBigDecimal
    val iInitial = emission.iInitial.toBigDecimal
    val lambda = emission.lambda.toBigDecimal
    val iImpact = emission.iImpact.toBigDecimal
    val epochsPerYear = BigDecimal(emission.epochsPerYear.value, mc)
    val transitionEpoch = BigDecimal(emission.asOfEpoch.value.value, mc)
    val totalSupply = BigDecimal(emission.totalSupply.value.value, mc)
    val initialPrice = emission.dagPrices.head._2.toBigDecimal
    val currentPrice = witnessPrice.toBigDecimal
    val yearDiff =
      DecimalUtils.safeDivide(BigDecimal(epoch.value.value - transitionEpoch.toLong, mc), epochsPerYear)
    val priceRatio = initialPrice / currentPrice
    val priceImpact = BigDecimal(StrictMath.pow(priceRatio.toDouble, iImpact.toDouble), mc)
    val expArgument = -(lambda * yearDiff) * priceImpact
    val decay = BigDecimal(StrictMath.exp(expArgument.toDouble), mc)
    val uncappedInflationRate = iTarget + (iInitial - iTarget) * decay
    val annualInflationRate = uncappedInflationRate.min(BigDecimal("0.06", mc))
    val perEpochEmission = DecimalUtils.safeDivide(totalSupply * annualInflationRate, epochsPerYear)

    perEpochEmission.roundedHalfUp(0).longValue
  }

  test("reachable elementary-function paths must mint one integer amount") {
    for {
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO]
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
      reader = GlobalStateReader.empty[IO]
      updater = PriceStateUpdater.make[IO](AppEnvironment.Testnet, DefaultDelegatedRewardsConfigProvider, reader)
      validator = PricingUpdateValidator.make[IO](None, NonNegLong.unsafeFrom(0L))
      priceUpdate = PricingUpdate(PriceFraction(TokenPair.DAG_USD, witnessPrice))
      validFirst <- validator.validate(priceUpdate, metagraphId, GlobalSnapshotInfo.empty, firstUpdateEpoch)
      firstWindow <- updater.updatePriceState(
        SortedMap.empty,
        List(priceUpdate),
        firstUpdateEpoch
      )
      firstContext = GlobalSnapshotInfo.empty.copy(priceState = Some(firstWindow))
      validSecond <- validator.validate(priceUpdate, metagraphId, firstContext, secondUpdateEpoch)
      secondWindow <- updater.updatePriceState(
        firstWindow,
        List(priceUpdate),
        secondUpdateEpoch
      )
      secondContext = GlobalSnapshotInfo.empty.copy(priceState = Some(secondWindow))
      validThird <- validator.validate(priceUpdate, metagraphId, secondContext, thirdUpdateEpoch)
      promoted <- updater.updatePriceState(
        secondWindow,
        List(priceUpdate),
        thirdUpdateEpoch
      )
      context = GlobalSnapshotInfo.empty.copy(priceState = Some(promoted))
      stakeManager = DelegatedStakeStateManager.make[IO](reader)
      nodeParametersReader = UpdateNodeParametersStateReader.make[IO](reader)
      distributor =
        GlobalDelegatedRewardsDistributor.make[IO](
          AppEnvironment.Testnet,
          config,
          stakeManager,
          nodeParametersReader,
          reader
        )
      liveAmount <- distributor.calculateVariableInflation(epoch, context)
      strictAmount = calculateWithStrictMath
    } yield {
      val currentRecord = promoted(TokenPair.DAG_USD)
      val priceRatio = emission.dagPrices.head._2.toBigDecimal / witnessPrice.toBigDecimal
      val mathPriceImpact = Math.pow(priceRatio.toDouble, emission.iImpact.toBigDecimal.toDouble)
      val strictPriceImpact = StrictMath.pow(priceRatio.toDouble, emission.iImpact.toBigDecimal.toDouble)
      val yearDiff = DecimalUtils.safeDivide(
        BigDecimal(epoch.value.value - emission.asOfEpoch.value.value, mc),
        BigDecimal(emission.epochsPerYear.value, mc)
      )
      val mathArgument = -(emission.lambda.toBigDecimal * yearDiff) * BigDecimal(mathPriceImpact, mc)
      val strictArgument = -(emission.lambda.toBigDecimal * yearDiff) * BigDecimal(strictPriceImpact, mc)
      val mathDecay = Math.exp(mathArgument.toDouble)
      val strictDecay = StrictMath.exp(strictArgument.toDouble)
      val distribution = config.percentDistribution(AppEnvironment.Testnet)(epoch)
      val reservedWeight = distribution.weights.valuesIterator.map(_.toBigDecimal).sum
      val totalWeight =
        reservedWeight + distribution.validatorsWeight.toBigDecimal + distribution.delegatorsWeight.toBigDecimal
      val protocolWeight =
        distribution.weights(MainnetRewardsConfig.protocolWalletMetanomics).toBigDecimal
      def protocolWalletReward(total: Long): Long = {
        val reservedTotal = BigDecimal(total, mc) * (reservedWeight / totalWeight)
        ((protocolWeight / reservedWeight) * reservedTotal).roundedHalfUp(0).longValue
      }

      expect.all(
        validFirst.isValid,
        validSecond.isValid,
        validThird.isValid,
        firstWindow(TokenPair.DAG_USD).nextWindowChange == secondUpdateEpoch,
        secondWindow(TokenPair.DAG_USD).nextWindowChange == thirdUpdateEpoch,
        currentRecord.nextWindowChange == EpochProgress(NonNegLong.unsafeFrom(1001357L)),
        currentRecord.currentPrice.price.value == witnessPrice
      ).and(
        expect(
          java.lang.Double.doubleToRawLongBits(mathDecay) =!= java.lang.Double.doubleToRawLongBits(strictDecay),
          s"witness requires distinct elementary-function results, Math=$mathDecay StrictMath=$strictDecay"
        )
      ).and(
        expect.same(30031351592L, liveAmount.value.value)
      ).and(
        expect.same(30031351591L, strictAmount)
      ).and(
        expect.same(9009405478L, protocolWalletReward(liveAmount.value.value))
      ).and(
        expect.same(9009405477L, protocolWalletReward(strictAmount))
      ).and(
        expect(
          strictAmount == liveAmount.value.value,
          s"consensus-visible reward amount depends on the permitted elementary-function implementation: " +
            s"Math=$mathDecay/${liveAmount.value.value}, StrictMath=$strictDecay/$strictAmount"
        )
      )
    }
  }

  test("schema-admissible fractional-power overflow aborts reward calculation") {
    val overflowingEmission = emission.copy(
      asOfEpoch = EpochProgress.MinValue,
      iImpact = NonNegFraction.unsafeFrom(1024L, 1L),
      dagPrices = SortedMap(EpochProgress.MinValue -> NonNegFraction.unsafeFrom(2L, 1L))
    )
    val overflowingConfig = config.copy(
      emissionConfig = config.emissionConfig.updated(AppEnvironment.Dev, _ => overflowingEmission)
    )
    val overflowingContext = GlobalSnapshotInfo.empty.copy(
      priceState = Some(
        SortedMap(
          TokenPair.DAG_USD -> PriceRecord(
            currentPrice = PricingUpdate(PriceFraction(TokenPair.DAG_USD, NonNegFraction.one)),
            upcomingPrice = PricingUpdate.zero,
            currentSum = PricingUpdate.zero,
            currentNumEvents = PosInt.unsafeFrom(1),
            nextWindowChange = epoch,
            updatedAt = epoch
          )
        )
      )
    )

    for {
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO]
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
      reader = GlobalStateReader.empty[IO]
      stakeManager = DelegatedStakeStateManager.make[IO](reader)
      nodeParametersReader = UpdateNodeParametersStateReader.make[IO](reader)
      distributor =
        GlobalDelegatedRewardsDistributor.make[IO](
          AppEnvironment.Dev,
          overflowingConfig,
          stakeManager,
          nodeParametersReader,
          reader
        )
      result <- distributor.calculateVariableInflation(epoch, overflowingContext).attempt
    } yield expect(result.swap.exists(_.isInstanceOf[NumberFormatException]))
  }
}
