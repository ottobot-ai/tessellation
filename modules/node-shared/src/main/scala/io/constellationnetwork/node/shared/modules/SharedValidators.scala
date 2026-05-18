package io.constellationnetwork.node.shared.modules

import cats.data.NonEmptySet
import cats.effect.Async

import scala.collection.immutable.SortedMap

import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.{AddressesConfig, DelegatedStakingConfig, PriceOracleConfig}
import io.constellationnetwork.node.shared.domain.block.processing.BlockValidator
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeValidator
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.node.UpdateNodeParametersValidator
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralValidator
import io.constellationnetwork.node.shared.domain.priceOracle.PricingUpdateValidator
import io.constellationnetwork.node.shared.domain.statechannel.{FeeCalculator, FeeCalculatorConfig, StateChannelValidator}
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockValidator
import io.constellationnetwork.node.shared.domain.swap.{AllowSpendChainValidator, AllowSpendValidator, SpendActionValidator}
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockValidator
import io.constellationnetwork.node.shared.domain.tokenlock.{TokenLockChainValidator, TokenLockValidator}
import io.constellationnetwork.node.shared.domain.transaction.{FeeTransactionValidator, TransactionChainValidator, TransactionValidator}
import io.constellationnetwork.node.shared.infrastructure.block.processing.BlockValidator
import io.constellationnetwork.node.shared.infrastructure.gossip.RumorValidator
import io.constellationnetwork.node.shared.infrastructure.snapshot.{CurrencyMessageValidator, GlobalSnapshotSyncValidator}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.signature.SignedValidator
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import eu.timepit.refined.types.numeric.PosLong

object SharedValidators {

  def make[F[_]: Async: JsonSerializer: SecurityProvider: Hasher](
    environment: AppEnvironment,
    addressesCfg: AddressesConfig,
    l0Seedlist: Option[Set[SeedlistEntry]],
    seedlist: Option[Set[SeedlistEntry]],
    stateChannelAllowanceLists: Option[Map[Address, NonEmptySet[PeerId]]],
    feeConfigs: SortedMap[SnapshotOrdinal, FeeCalculatorConfig],
    maxBinarySizeInBytes: PosLong,
    txHasher: Hasher[F],
    delegatedStaking: DelegatedStakingConfig,
    priceOracleConfig: PriceOracleConfig,
    // #198: Class-2 validator path migrated to GlobalStateReader (#118 follow-up). On gl0,
    // pass `GlobalStateReader.pending(overlay, bestTipFn)` so HTTP intake + acceptance see the
    // chain's pending writes under MultiBranch. On followers (gl1/cl1/dl1/ml0) pass
    // `GlobalStateReader.finalized(mptStore)`. When `None`, the reader-dependent validators
    // fall through to the `lastContext`-based fallbacks (legacy/test path).
    //
    // On gl0 the natural construction point of `pending` is after `Services.make` builds the
    // overlay-aware reader — but `SharedValidators.make` runs earlier in startup. Callers
    // should pass `GlobalStateReader.fromMptStore(storages.mptStore)` here as a finalized
    // default, then call `withOverlayReader(services.pendingReader)` after `Services.make`
    // to swap in the overlay-aware reader before `HttpApi.make` consumes the validators.
    maybeReader: Option[GlobalStateReader[F]] = None
  ): SharedValidators[F] = {
    val signedValidator = SignedValidator.make[F]
    val transactionChainValidator = TransactionChainValidator.make[F](txHasher)
    val feeTransactionValidator = FeeTransactionValidator.make[F](signedValidator)
    val transactionValidator = TransactionValidator.make[F](addressesCfg, signedValidator, txHasher)
    val blockValidator = BlockValidator.make[F](signedValidator, transactionChainValidator, transactionValidator, txHasher)
    val currencyTransactionChainValidator = TransactionChainValidator.make[F](txHasher)
    val currencyTransactionValidator = TransactionValidator.make[F](addressesCfg, signedValidator, txHasher)
    val currencyBlockValidator = BlockValidator
      .make[F](signedValidator, currencyTransactionChainValidator, currencyTransactionValidator, txHasher)
    val rumorValidator = RumorValidator.make[F](seedlist, signedValidator)
    val feeCalculator = FeeCalculator.make(feeConfigs)
    val stateChannelValidator =
      StateChannelValidator.make[F](signedValidator, l0Seedlist, stateChannelAllowanceLists, maxBinarySizeInBytes, feeCalculator)
    val currencyMessageValidator = CurrencyMessageValidator.make[F](environment, signedValidator, stateChannelAllowanceLists, seedlist)
    val globalSnapshotSyncValidator = GlobalSnapshotSyncValidator.make[F](signedValidator, seedlist)
    val allowSpendChainValidator = AllowSpendChainValidator.make[F]
    val allowSpendValidator = AllowSpendValidator.make[F](addressesCfg, signedValidator)
    val allowSpendBlockValidator = AllowSpendBlockValidator.make[F](signedValidator, allowSpendChainValidator, allowSpendValidator)

    val tokenLockValidator = TokenLockValidator.make[F](addressesCfg, signedValidator)
    val tokenLockChainValidator = TokenLockChainValidator.make[F]
    val tokenLockBlockValidator = TokenLockBlockValidator.make[F](signedValidator, tokenLockChainValidator, tokenLockValidator)

    val updateNodeParametersValidator = UpdateNodeParametersValidator.make(
      signedValidator,
      delegatedStaking.minRewardFraction,
      delegatedStaking.maxRewardFraction,
      delegatedStaking.maxMetadataFieldsChars,
      l0Seedlist
    )
    val updateDelegatedStakeValidator = maybeReader match {
      case Some(reader) => UpdateDelegatedStakeValidator.make[F](signedValidator, l0Seedlist, reader)
      case None         => UpdateDelegatedStakeValidator.make[F](signedValidator, l0Seedlist)
    }
    val updateNodeCollateralValidator = maybeReader match {
      case Some(reader) => UpdateNodeCollateralValidator.make[F](signedValidator, l0Seedlist, reader)
      case None         => UpdateNodeCollateralValidator.rejectAll[F]
    }

    val spendActionValidator = SpendActionValidator.make[F]

    val pricingUpdateValidator =
      PricingUpdateValidator.make[F](priceOracleConfig.allowedMetagraphIds, priceOracleConfig.minEpochsBetweenUpdates)

    new SharedValidators[F](
      signedValidator,
      transactionChainValidator,
      transactionValidator,
      feeTransactionValidator,
      currencyTransactionChainValidator,
      currencyTransactionValidator,
      blockValidator,
      currencyBlockValidator,
      rumorValidator,
      stateChannelValidator,
      currencyMessageValidator,
      globalSnapshotSyncValidator,
      tokenLockBlockValidator,
      allowSpendBlockValidator,
      allowSpendValidator,
      tokenLockValidator,
      updateNodeParametersValidator,
      spendActionValidator,
      updateDelegatedStakeValidator,
      updateNodeCollateralValidator,
      pricingUpdateValidator
    ) {}
  }
}

sealed abstract class SharedValidators[F[_]] private (
  val signedValidator: SignedValidator[F],
  val transactionChainValidator: TransactionChainValidator[F],
  val transactionValidator: TransactionValidator[F],
  val feeTransactionValidator: FeeTransactionValidator[F],
  val currencyTransactionChainValidator: TransactionChainValidator[F],
  val currencyTransactionValidator: TransactionValidator[F],
  val blockValidator: BlockValidator[F],
  val currencyBlockValidator: BlockValidator[F],
  val rumorValidator: RumorValidator[F],
  val stateChannelValidator: StateChannelValidator[F],
  val currencyMessageValidator: CurrencyMessageValidator[F],
  val globalSnapshotSyncValidator: GlobalSnapshotSyncValidator[F],
  val tokenLockBlockValidator: TokenLockBlockValidator[F],
  val allowSpendBlockValidator: AllowSpendBlockValidator[F],
  val allowSpendValidator: AllowSpendValidator[F],
  val tokenLockValidator: TokenLockValidator[F],
  val updateNodeParametersValidator: UpdateNodeParametersValidator[F],
  val spendActionValidator: SpendActionValidator[F],
  val updateDelegatedStakeValidator: UpdateDelegatedStakeValidator[F],
  val updateNodeCollateralValidator: UpdateNodeCollateralValidator[F],
  val pricingUpdateValidator: PricingUpdateValidator[F]
) {

  /** #198: Class-2 validator path migrated to GlobalStateReader (#118 follow-up). Returns a copy of this `SharedValidators` with
    * `updateDelegatedStakeValidator` and `updateNodeCollateralValidator` rebuilt against the supplied `reader`. Used by gl0's `Main.scala`
    * to swap in the overlay-aware `pendingReader` after `Services.make` constructs it — the initial `SharedValidators.make` call happens
    * before the overlay exists, so it's seeded with the finalized adapter and upgraded here. The other validators are reused unchanged.
    *
    * The `l0Seedlist` arg must match the value passed to `SharedValidators.make`; the rebuild uses it for `validateAuthorizedNodeId`. Type-
    * class constraints `Async`, `SecurityProvider`, `Hasher` are not captured by the class (kept stateless / re-summoned per call) so the
    * caller re-supplies them at the swap point.
    */
  def withOverlayReader(reader: GlobalStateReader[F], l0Seedlist: Option[Set[SeedlistEntry]])(
    implicit F: Async[F],
    sp: SecurityProvider[F],
    hasher: Hasher[F]
  ): SharedValidators[F] = {
    val newDelegated = UpdateDelegatedStakeValidator.make[F](signedValidator, l0Seedlist, reader)
    val newCollateral = UpdateNodeCollateralValidator.make[F](signedValidator, l0Seedlist, reader)
    new SharedValidators[F](
      signedValidator,
      transactionChainValidator,
      transactionValidator,
      feeTransactionValidator,
      currencyTransactionChainValidator,
      currencyTransactionValidator,
      blockValidator,
      currencyBlockValidator,
      rumorValidator,
      stateChannelValidator,
      currencyMessageValidator,
      globalSnapshotSyncValidator,
      tokenLockBlockValidator,
      allowSpendBlockValidator,
      allowSpendValidator,
      tokenLockValidator,
      updateNodeParametersValidator,
      spendActionValidator,
      newDelegated,
      newCollateral,
      pricingUpdateValidator
    ) {}
  }
}
