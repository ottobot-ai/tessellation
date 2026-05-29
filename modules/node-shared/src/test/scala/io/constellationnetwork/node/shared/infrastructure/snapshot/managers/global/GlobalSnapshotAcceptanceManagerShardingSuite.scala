package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.delegatedStake.{UpdateDelegatedStakeAcceptanceManager, UpdateDelegatedStakeValidator}
import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.node.{UpdateNodeParametersAcceptanceManager, UpdateNodeParametersAcceptanceResult}
import io.constellationnetwork.node.shared.domain.nodeCollateral.{
  UpdateNodeCollateralAcceptanceManager,
  UpdateNodeCollateralAcceptanceResult
}
import io.constellationnetwork.node.shared.domain.priceOracle.PricingUpdateValidator.PricingUpdateValidationErrorOr
import io.constellationnetwork.node.shared.domain.priceOracle.{PriceStateUpdater, PricingUpdateValidator}
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator.SpendActionValidationErrorOr
import io.constellationnetwork.node.shared.domain.swap.block._
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.snapshot.DelegatedRewardsResult
import io.constellationnetwork.node.shared.logger.Slf4jLoggerBundle
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{PricingUpdate, SpendAction}
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nodeCollateral.UpdateNodeCollateral
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.swap.{AllowSpend, AllowSpendBlock}
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary, StateChannelValidationType}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import weaver.MutableIOSuite

/** Tests for the Axis 1a (#259 token-lock stall fix) sharding-aware ADOPT path in [[GlobalSnapshotAcceptanceManager.accept]].
  *
  * '''What changed from the original append-based slice.''' The pre-Axis-1a code converted accepted checkpoints' `includedSnapshots` into
  * `StateChannelOutput`s and APPENDED them to the raw `scEvents` list, re-running `onlyPossibleReferences` (the chain-link check) on them
  * via `process`. That re-route is the #259 stall: once gl0's own `lastStateChannelSnapshotHashes` tip freezes, the chain-link rejects
  * every checkpoint binary forever. Axis 1a ADOPTS the committee-attested `includedSnapshots` DIRECTLY as the accepted per-MG SC-snapshot
  * chain (bypassing chain-link) and runs only the unchanged currency derivation (`processCurrencySnapshots`) on them. So in the new design
  * the accepted shard binaries flow through `processCurrencySnapshots`, NOT `process`; `process` only ever sees the raw (DAG/non-sharded)
  * `scEvents`.
  *
  * '''Test plan''':
  *   1. '''numShards=1 / None / no-manager regression bars''' — the adopt gate never fires; `process` sees exactly the raw `scEvents`,
  *      `processCurrencySnapshots` sees the empty adopted map, and the stub `evaluate` is never called.
  *   1. '''numShards=4 with shard checkpoints''' — every checkpoint is evaluated and every accepted checkpoint's `includedSnapshots`
  *      arrives in the `processCurrencySnapshots` adopted map (NOT appended to `process`'s raw event list).
  *   1. '''CHANGE 3 filter''' — at numShards>1 the static assignment is total, so raw metagraph scEvents are EXCLUDED from the base
  *      chain-link `process` path (no double-path); sharded MGs flow only via the adopt path.
  *   1. '''Rejected / Pending shard checkpoint''' — only Accepted checkpoints' snapshots reach the adopted map.
  *   1. '''Three-paths byte-identity''' — produce / createContext / validateArtifact all funnel through `accept()` with the same embedded
  *      checkpoints + prior state ⇒ byte-identical accepted state + state proof (deterministic `verifyEmbedded`).
  *
  * '''Test fixture pattern''':
  *   - Uses a "captor" `GlobalSnapshotStateChannelEventsProcessor` that records BOTH the raw `process` event list AND the adopted
  *     `processCurrencySnapshots` snapshot map. These are the two observables for asserting which binaries took the chain-link path vs the
  *     adopt path.
  *   - The `ShardCheckpointGl0AcceptanceManager` is fully stubbed (returns a configurable result per call) — no real KES/Ed25519/VRF
  *     fixture is needed; the GSAM contract under test is "adopt accepted checkpoint outputs into the currency pipeline", not "verify
  *     checkpoint crypto" (which is [[ShardCheckpointGl0AcceptanceManagerSuite]]'s job).
  *   - `MetagraphSyncManager` is real (constructed from production `make`), so `consumeReceipts` actually drains into its internal
  *     accumulator.
  *
  * The byte-exactness equivalence between the adopt path and re-execution (the load-bearing #259 claim) is proven separately in
  * [[GlobalSnapshotAcceptanceManagerAdoptParitySuite]] with a real processor.
  */
object GlobalSnapshotAcceptanceManagerShardingSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
    } yield (h, sp)

  // Reused metrics — see Mocks.scala scaladoc for why NoOpMetrics is the test-time choice.
  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  // ============================================================================
  // Fixtures
  // ============================================================================

  /** Address generator — deterministic, distinct per test. Uses a unique seed string so per-shard MGs differ across tests. */
  private def mkAddress(label: String): Address =
    Address.fromBytes(label.getBytes("UTF-8"))

  private val genesisHash: Hash = Hash("0" * 64)
  private val epochZero: EtaPeriod = EtaPeriod(0L)

  /** Build a sentinel `Signed[StateChannelSnapshotBinary]` — only used to populate `includedSnapshots` in the test checkpoints. The captor
    * processor never inspects the content; it just records the address+binary pair.
    */
  private def mkSignedBinary(content: Array[Byte]): Signed[StateChannelSnapshotBinary] = {
    val sentinelProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70)))
    Signed(
      StateChannelSnapshotBinary(
        lastSnapshotHash = Hash("0" * 64),
        content = content,
        fee = SnapshotFee(NonNegLong.unsafeFrom(0L))
      ),
      NonEmptySet.of(sentinelProof)
    )
  }

  /** Build a `ShardDerivedStateDelta` with one MG carrying a head binary. */
  private def mkDelta(mg: Address, binary: Signed[StateChannelSnapshotBinary]): ShardDerivedStateDelta =
    ShardDerivedStateDelta(
      perMetagraphMptRoots = SortedMap(mg -> Hash("11" * 32)),
      includedSnapshots = SortedMap(mg -> NonEmptyList.of(binary)),
      tokenLockBalancesDelta = SortedMap.empty,
      perMetagraphArtifacts = SortedMap.empty,
      perMetagraphSyncDataDelta = SortedMap.empty
    )

  /** Build a minimal checkpoint shell. The signatures field is a placeholder — the stubbed manager doesn't verify it. */
  private def mkCheckpoint(
    shardId: ShardId,
    shardOrd: Long,
    gl0Anchor: Long,
    delta: ShardDerivedStateDelta,
    receipts: List[CrossShardReceipt] = List.empty
  ): ShardCheckpoint = {
    val placeholderPeerId = io.constellationnetwork.schema.peer.PeerId(Hex("ab" * 64))
    val placeholderSig = CommitteeMemberSignature(
      peerId = placeholderPeerId,
      vrfProof = Hex(""),
      ed25519Sig = Hex(""),
      kesProductSig = Hex(""),
      kesTreeStep = 0
    )
    ShardCheckpoint(
      shardId = shardId,
      parentCheckpointHash = genesisHash,
      shardOrdinal = ShardOrdinal(shardOrd),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(gl0Anchor)),
      derivedStateDelta = delta,
      emittedReceipts = receipts,
      committeeSignatures = NonEmptyList.of(placeholderSig),
      epoch = epochZero
    )
  }

  /** Captor processor that records BOTH the raw `process` event list AND the adopted `processCurrencySnapshots` snapshot map. Returns empty
    * results — the tests assert what flowed IN to each stage, not what comes back. (The adopt path's correctness against a real derivation
    * is [[GlobalSnapshotAcceptanceManagerAdoptParitySuite]]'s job.)
    *
    * @param capturedEventsRef
    *   records the raw `scEvents` list that reached `process` (the chain-link path — DAG/non-sharded events only under Axis 1a).
    * @param capturedAdoptedRef
    *   records the adopted per-MG snapshot map that reached `processCurrencySnapshots` (the bypass-chain-link adopt path).
    */
  private def mkCaptorProcessor(
    capturedEventsRef: Ref[IO, List[StateChannelOutput]],
    capturedAdoptedRef: Ref[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]]
  ): GlobalSnapshotStateChannelEventsProcessor[IO] =
    new GlobalSnapshotStateChannelEventsProcessor[IO] {
      override def process(
        snapshotOrdinal: SnapshotOrdinal,
        currentBalances: SortedMap[Address, Balance],
        priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        events: List[StateChannelOutput],
        validationType: StateChannelValidationType,
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[StateChannelAcceptanceResult] =
        capturedEventsRef
          .set(events)
          .as(
            StateChannelAcceptanceResult(
              accepted = SortedMap.empty[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
              calculatedCurrencyState = SortedMap.empty[Address, CurrencySnapshotWithState],
              returned = Set.empty[StateChannelOutput],
              balanceUpdate = SortedMap.empty[Address, Balance],
              incomingCurrencySnapshotsWithState = SortedMap.empty[Address, List[CurrencySnapshotWithState]]
            )
          )

      override def processCurrencySnapshots(
        snapshotOrdinal: SnapshotOrdinal,
        currentBalances: SortedMap[Address, Balance],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        events: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(
        implicit hasher: Hasher[IO]
      ): IO[SortedMap[Address, MetagraphAcceptanceResult]] =
        // GSAM only calls this from the adopt path (`deriveAdoptedCurrencyState`); capture the adopted snapshot map.
        capturedAdoptedRef.set(events).as(SortedMap.empty[Address, MetagraphAcceptanceResult])

      override def assembleAcceptanceResult(
        processed: SortedMap[Address, MetagraphAcceptanceResult],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        returned: Set[StateChannelOutput]
      ): StateChannelAcceptanceResult =
        // The captor's processCurrencySnapshots returns empty, so the adopt path's assembly is over an empty map ⇒ empty result.
        StateChannelAcceptanceResult(
          accepted = SortedMap.empty[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
          calculatedCurrencyState = priorLastCurrencySnapshots,
          returned = returned,
          balanceUpdate = SortedMap.empty[Address, Balance],
          incomingCurrencySnapshotsWithState = SortedMap.empty[Address, List[CurrencySnapshotWithState]]
        )

      override def deriveMetagraphRoot(
        metagraphAddress: Address,
        binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]],
        snapshotOrdinal: SnapshotOrdinal,
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[Hash] = Hash.empty.pure[IO]
    }

  /** Stubbed `ShardCheckpointGl0AcceptanceManager` — returns the configured result per checkpoint and records every call.
    *
    * The GSAM adopt path now calls [[verifyEmbedded]] (the deterministic adopt-verifier), so THAT is the method that records the calls and
    * applies the decision the tests assert on. `evaluate` (node-local selection path) delegates to the same decision so the stub stays
    * consistent if a test ever exercises it.
    */
  private final case class StubAcceptanceManager(
    callsRef: Ref[IO, List[ShardCheckpoint]],
    decision: ShardCheckpoint => ShardCheckpointAcceptResult
  ) extends ShardCheckpointGl0AcceptanceManager[IO] {
    override def evaluate(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
      callsRef.update(_ :+ checkpoint).as(decision(checkpoint))
    override def verifyEmbedded(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
      callsRef.update(_ :+ checkpoint).as(decision(checkpoint))
  }

  /** Build the manager-under-test with the Slice 13 sharding deps, using the captor processor (records `process` events + adopted
    * snapshots). Most tests use this.
    */
  private def mkSuiteManager(
    shardingConfig: Option[ShardingConfig],
    checkpointManager: Option[ShardCheckpointGl0AcceptanceManager[IO]],
    capturedEventsRef: Ref[IO, List[StateChannelOutput]],
    capturedAdoptedRef: Ref[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]]
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[GlobalSnapshotAcceptanceManager[IO]] =
    mkSuiteManagerWithProcessor(shardingConfig, checkpointManager, mkCaptorProcessor(capturedEventsRef, capturedAdoptedRef))

  /** Build the manager-under-test with an explicit `stateChannelEventsProcessor` — lets the three-paths-identity test inject a deriving
    * processor. Tests pass:
    *   - `shardingConfig`: typically [[mkShardingConfig]] with `numShards > 1`. Default `None` for the regression bar test.
    *   - `checkpointManager`: a [[StubAcceptanceManager]] with the desired per-checkpoint decisions.
    */
  private def mkSuiteManagerWithProcessor(
    shardingConfig: Option[ShardingConfig],
    checkpointManager: Option[ShardCheckpointGl0AcceptanceManager[IO]],
    stateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[IO]
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[GlobalSnapshotAcceptanceManager[IO]] = {
    // Mock dependencies — same shape as `Mocks.scala` for the existing GSAM tests, just inlined here so this suite is self-contained
    // and can swap in the captor processor without coupling to Mocks's mockStateChannelEventsProcessor.
    val mockBlockAcceptanceManager: BlockAcceptanceManager[IO] = new BlockAcceptanceManager[IO] {
      override def acceptBlocksIteratively(
        blocks: List[Signed[Block]],
        context: BlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldValidateCollateral: Boolean
      )(implicit hasher: Hasher[IO]): IO[BlockAcceptanceResult] =
        BlockAcceptanceResult(
          accepted = List.empty,
          notAccepted = List.empty,
          contextUpdate = BlockAcceptanceContextUpdate(
            balances = SortedMap.empty,
            lastTxRefs = SortedMap.empty,
            parentUsages = Map.empty
          )
        ).pure[IO]

      override def acceptBlock(
        block: Signed[Block],
        context: BlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldValidateCollateral: Boolean
      )(implicit hasher: Hasher[IO]): IO[Either[BlockNotAcceptedReason, (BlockAcceptanceContextUpdate, UsageCount)]] =
        (
          BlockAcceptanceContextUpdate(
            balances = SortedMap.empty,
            lastTxRefs = SortedMap.empty,
            parentUsages = Map.empty
          ),
          initUsageCount
        ).asRight.pure[IO]
    }

    val mockAllowSpendBlockAcceptanceManager: AllowSpendBlockAcceptanceManager[IO] = new AllowSpendBlockAcceptanceManager[IO] {
      override def acceptBlocksIteratively(
        blocks: List[Signed[AllowSpendBlock]],
        context: AllowSpendBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[AllowSpendBlockAcceptanceResult] =
        AllowSpendBlockAcceptanceResult(
          contextUpdate = AllowSpendBlockAcceptanceContextUpdate.empty,
          accepted = List.empty[Signed[AllowSpendBlock]],
          notAccepted = List.empty[(Signed[AllowSpendBlock], AllowSpendBlockNotAcceptedReason)]
        ).pure[IO]

      override def acceptBlock(
        block: Signed[AllowSpendBlock],
        context: AllowSpendBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[Either[AllowSpendBlockNotAcceptedReason, AllowSpendBlockAcceptanceContextUpdate]] =
        AllowSpendBlockAcceptanceContextUpdate.empty.asRight.pure[IO]
    }

    val mockTokenLockBlockAcceptanceManager: TokenLockBlockAcceptanceManager[IO] = new TokenLockBlockAcceptanceManager[IO] {
      override def acceptBlocksIteratively(
        blocks: List[Signed[TokenLockBlock]],
        context: TokenLockBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[TokenLockBlockAcceptanceResult] =
        TokenLockBlockAcceptanceResult(
          contextUpdate = TokenLockBlockAcceptanceContextUpdate.empty,
          accepted = blocks,
          notAccepted = List.empty[(Signed[TokenLockBlock], TokenLockBlockNotAcceptedReason)]
        ).pure[IO]

      override def acceptBlock(
        block: Signed[TokenLockBlock],
        context: TokenLockBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[Either[TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate]] =
        TokenLockBlockAcceptanceContextUpdate.empty.asRight.pure[IO]
    }

    val mockStateChannelEventsProcessor = stateChannelEventsProcessor

    val mockUpdateNodeParametersAcceptanceManager: UpdateNodeParametersAcceptanceManager[IO] =
      new UpdateNodeParametersAcceptanceManager[IO] {
        override def acceptUpdateNodeParameters(
          events: List[Signed[io.constellationnetwork.schema.node.UpdateNodeParameters]],
          lastSnapshotContext: GlobalSnapshotInfo
        ): IO[UpdateNodeParametersAcceptanceResult] =
          UpdateNodeParametersAcceptanceResult(accepted = List.empty, notAccepted = List.empty).pure[IO]
      }

    val updateDelegatedStakeValidator =
      UpdateDelegatedStakeValidator.make[IO](io.constellationnetwork.security.signature.SignedValidator.make[IO], None)
    val updateDelegatedStakeAcceptanceManager = UpdateDelegatedStakeAcceptanceManager.make[IO](updateDelegatedStakeValidator)

    val mockUpdateNodeCollateralAcceptanceManager: UpdateNodeCollateralAcceptanceManager[IO] =
      new UpdateNodeCollateralAcceptanceManager[IO] {
        override def accept(
          createEvents: List[Signed[UpdateNodeCollateral.Create]],
          withdrawEvents: List[Signed[UpdateNodeCollateral.Withdraw]],
          lastSnapshotContext: GlobalSnapshotInfo,
          epochProgress: EpochProgress,
          ordinal: SnapshotOrdinal,
          delegatedStakeAcceptanceResult: io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceResult
        ): IO[UpdateNodeCollateralAcceptanceResult] =
          UpdateNodeCollateralAcceptanceResult(
            acceptedCreates = SortedMap.empty,
            notAcceptedCreates = List.empty,
            acceptedWithdrawals = SortedMap.empty,
            notAcceptedWithdrawals = List.empty
          ).pure[IO]
      }

    val mockSpendActionValidator: SpendActionValidator[IO] = new SpendActionValidator[IO] {
      override def validate(
        spendAction: SpendAction,
        activeAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        allBalances: Map[Option[Address], SortedMap[Address, Balance]],
        currencyId: Address
      ): IO[SpendActionValidationErrorOr[SpendAction]] = spendAction.validNec.pure[IO]

      override def validateReturningAcceptedAndRejected(
        spendActions: Map[Address, List[SpendAction]],
        activeAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        allBalances: Map[Option[Address], SortedMap[Address, Balance]]
      ): IO[
        (Map[Address, List[SpendAction]], Map[Address, List[(SpendAction, List[SpendActionValidator.SpendActionValidationError])]])
      ] =
        (
          Map.empty[Address, List[SpendAction]],
          Map.empty[Address, List[(SpendAction, List[SpendActionValidator.SpendActionValidationError])]]
        ).pure[IO]
    }

    val mockPricingUpdateValidator: PricingUpdateValidator[IO] = new PricingUpdateValidator[IO] {
      override def validate(
        pricingUpdate: PricingUpdate,
        currencyId: Address,
        lastContext: GlobalSnapshotInfo,
        epochProgress: EpochProgress
      ): IO[PricingUpdateValidationErrorOr[PricingUpdate]] = pricingUpdate.validNec.pure[IO]

      override def validateReturningAcceptedAndRejected(
        pricingUpdates: Map[Address, List[PricingUpdate]],
        lastContext: GlobalSnapshotInfo,
        epochProgress: EpochProgress
      ): IO[(List[PricingUpdate], List[(PricingUpdate, List[PricingUpdateValidator.PricingUpdateValidationError])])] =
        (List.empty, List.empty).pure[IO]
    }

    val mockPriceStateUpdater: PriceStateUpdater[IO] = new PriceStateUpdater[IO] {
      override def updatePriceState(
        lastPriceState: SortedMap[priceOracle.TokenPair, priceOracle.PriceRecord],
        acceptedPricingUpdates: List[PricingUpdate],
        epochProgress: EpochProgress
      )(implicit hasher: Hasher[IO]): IO[SortedMap[priceOracle.TokenPair, priceOracle.PriceRecord]] =
        SortedMap.empty[priceOracle.TokenPair, priceOracle.PriceRecord].pure[IO]

      override def materializePriceStateFromMpt(
        implicit hasher: Hasher[IO]
      ): IO[SortedMap[priceOracle.TokenPair, priceOracle.PriceRecord]] =
        SortedMap.empty[priceOracle.TokenPair, priceOracle.PriceRecord].pure[IO]
    }

    implicit val hasherSelector: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(Long.MaxValue))

    JsonSerializer.forAsync[IO].flatMap { implicit j =>
      Slf4jLoggerBundle.makeUnsafe[IO].flatMap { loggerBundle =>
        InMemoryMerklePatriciaProducer.make[IO]().flatMap { mptProducer =>
          MptStore
            .make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
            .flatMap { mptStore =>
              for {
                pcTree <- io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree.make[IO]
                overlay = io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay
                  .passthrough[IO, GlobalStateKey](mptStore, pcTree)
                mgr <- GlobalSnapshotAcceptanceManager
                  .make[IO](
                    FieldsAddedOrdinals(
                      Map.empty,
                      Map.empty,
                      Map.empty,
                      Map.empty,
                      Map.empty,
                      Map.empty,
                      Map.empty,
                      Map.empty,
                      Map.empty,
                      Map.empty
                    ),
                    MetagraphsSyncConfig(PosInt(100)),
                    AppEnvironment.Dev,
                    blockAcceptanceManager = mockBlockAcceptanceManager,
                    allowSpendBlockAcceptanceManager = mockAllowSpendBlockAcceptanceManager,
                    tokenLockBlockAcceptanceManager = mockTokenLockBlockAcceptanceManager,
                    stateChannelEventsProcessor = mockStateChannelEventsProcessor,
                    updateNodeParametersAcceptanceManager = mockUpdateNodeParametersAcceptanceManager,
                    updateDelegatedStakeAcceptanceManager = updateDelegatedStakeAcceptanceManager,
                    updateNodeCollateralAcceptanceManager = mockUpdateNodeCollateralAcceptanceManager,
                    spendActionValidator = mockSpendActionValidator,
                    pricingUpdateValidator = mockPricingUpdateValidator,
                    priceStateUpdater = mockPriceStateUpdater,
                    collateral = Amount.empty,
                    withdrawalTimeLimit = EpochProgress(4L),
                    loggerBundle = loggerBundle,
                    overlay = overlay,
                    shardingConfig = shardingConfig,
                    shardCheckpointAcceptanceManager = checkpointManager,
                    shardAssignment = Some(ShardAssignment.make[IO](numShards = shardingConfig.map(_.numShards).getOrElse(1)))
                  )
              } yield mgr
            }
        }
      }
    }
  }

  /** Build a [[ShardingConfig]] with the supplied `numShards`. Other fields use representative defaults — only `numShards` is consulted by
    * the Slice 13 gate.
    */
  private def mkShardingConfig(numShards: Int): ShardingConfig =
    ShardingConfig(
      numShards = numShards,
      committeeKTarget = 4,
      finality = ShardFinalityConfig(k1Shard = 8L),
      checkpoint = ShardCheckpointConfig(tAliveMs = 10000L, tBurst = 100, binaryBufferCap = 4096),
      observability = ShardObservabilityConfig(tPartitionHardMs = 600000L),
      slashing = ShardSlashingConfig(maxMissedPctPerEpoch = 33, minDenominatorPerEpoch = 5L)
    )

  /** Shared no-op rewards function — none of the suite tests exercise the rewards path. */
  private val noopRewardsFn: io.constellationnetwork.node.shared.infrastructure.snapshot.RewardsInput => IO[DelegatedRewardsResult] =
    _ =>
      DelegatedRewardsResult(
        SortedMap.empty,
        SortedMap.empty,
        SortedMap.empty,
        SortedSet.empty,
        SortedSet.empty,
        SortedSet.empty,
        Amount.empty
      ).pure[IO]

  /** Invoke `accept()` with the standard empty/no-op inputs except for the SC events + shard checkpoints we're testing. */
  private def invokeAccept(
    mgr: GlobalSnapshotAcceptanceManager[IO],
    scEvents: List[StateChannelOutput],
    shardCheckpoints: SortedMap[ShardId, ShardCheckpoint],
    lastSnapshotInfo: GlobalSnapshotInfo
  ): IO[Unit] =
    mgr
      .accept(
        ordinal = SnapshotOrdinal(2L),
        epochProgress = EpochProgress(10L),
        previousEpochProgress = EpochProgress.MinValue,
        blocksForAcceptance = List.empty,
        allowSpendBlocksForAcceptance = List.empty,
        tokenLockBlocksForAcceptance = List.empty,
        scEvents = scEvents,
        unpEvents = List.empty,
        cdsEvents = List.empty,
        wdsEvents = List.empty,
        cncEvents = List.empty,
        wncEvents = List.empty,
        lastSnapshotContext = lastSnapshotInfo,
        lastActiveTips = SortedSet.empty,
        lastDeprecatedTips = SortedSet.empty,
        calculateRewardsFn = noopRewardsFn,
        validationType = StateChannelValidationType.Full,
        getGlobalSnapshotByOrdinal = _ => None.pure[IO],
        parentTip = io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId.passthrough,
        shardCheckpoints = shardCheckpoints
      )
      .void

  /** Like [[invokeAccept]] but returns the accepted `scSnapshots` map (6th tuple element) and the resulting GSI's mptRoot — the two
    * observables a follower/validator must reproduce byte-identically to the leader (`stateChannelSnapshots` + state proof). Used by the
    * three-paths-identical determinism test.
    */
  private def invokeAcceptCapturing(
    mgr: GlobalSnapshotAcceptanceManager[IO],
    scEvents: List[StateChannelOutput],
    shardCheckpoints: SortedMap[ShardId, ShardCheckpoint],
    lastSnapshotInfo: GlobalSnapshotInfo
  ): IO[(SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]], Option[Hash])] =
    mgr
      .accept(
        ordinal = SnapshotOrdinal(2L),
        epochProgress = EpochProgress(10L),
        previousEpochProgress = EpochProgress.MinValue,
        blocksForAcceptance = List.empty,
        allowSpendBlocksForAcceptance = List.empty,
        tokenLockBlocksForAcceptance = List.empty,
        scEvents = scEvents,
        unpEvents = List.empty,
        cdsEvents = List.empty,
        wdsEvents = List.empty,
        cncEvents = List.empty,
        wncEvents = List.empty,
        lastSnapshotContext = lastSnapshotInfo,
        lastActiveTips = SortedSet.empty,
        lastDeprecatedTips = SortedSet.empty,
        calculateRewardsFn = noopRewardsFn,
        validationType = StateChannelValidationType.Full,
        getGlobalSnapshotByOrdinal = _ => None.pure[IO],
        parentTip = io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId.passthrough,
        shardCheckpoints = shardCheckpoints
      )
      .map(result => (result._6, result._10.mptRoot))

  /** Minimal `GlobalSnapshotInfo` — only what's needed to satisfy `accept()`'s prior-state reads. */
  private val emptyGsi: GlobalSnapshotInfo =
    GlobalSnapshotInfo(
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      SortedMap.empty
    )

  // ============================================================================
  // Test 1: numShards=1 byte-equivalence — adopt gate inactive, raw scEvents flow through chain-link unchanged
  // ============================================================================

  test("numShards=1 byte-equivalence: shardingConfig.numShards=1 ⇒ adopt path never fires") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      capturedAdoptedRef <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      stubMgr = StubAcceptanceManager(checkpointCallsRef, _ => ShardCheckpointAcceptResult.Accepted)
      // shardingConfig = numShards=1; even if the caller passed a non-empty shardCheckpoints map, the gate
      // refuses to fire because numShards > 1 is the activation predicate. This is the regression bar's literal
      // assertion: at the production default, the adopt path is unreachable and `deriveAdoptedCurrencyState`
      // (hence `processCurrencySnapshots`) is never invoked.
      mgr <- mkSuiteManager(Some(mkShardingConfig(numShards = 1)), Some(stubMgr), capturedRef, capturedAdoptedRef)
      mg = mkAddress("mg-numshards-1")
      binary = mkSignedBinary("test-payload-1".getBytes("UTF-8"))
      delta = mkDelta(mg, binary)
      cp = mkCheckpoint(ShardId.unsafeApply(0), shardOrd = 1L, gl0Anchor = 2L, delta = delta)
      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = SortedMap(cp.shardId -> cp), lastSnapshotInfo = emptyGsi)
      captured <- capturedRef.get
      capturedAdopted <- capturedAdoptedRef.get
      checkpointCalls <- checkpointCallsRef.get
    } yield
      expect.all(
        captured.isEmpty, // raw scEvents was empty and flowed through `process` unchanged
        capturedAdopted.isEmpty, // adopt gate didn't fire ⇒ processCurrencySnapshots adopt-path not called
        checkpointCalls.isEmpty // gate didn't fire ⇒ stubMgr.evaluate was never called
      )
  }

  // ============================================================================
  // Test 2: shardingConfig = None ⇒ regression bar (no sharding deps wired at all)
  // ============================================================================

  test("regression bar: shardingConfig=None ⇒ adopt path never fires even with non-empty shardCheckpoints") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      capturedAdoptedRef <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      stubMgr = StubAcceptanceManager(checkpointCallsRef, _ => ShardCheckpointAcceptResult.Accepted)
      mgr <- mkSuiteManager(shardingConfig = None, checkpointManager = Some(stubMgr), capturedRef, capturedAdoptedRef)
      mg = mkAddress("mg-no-shardingcfg")
      binary = mkSignedBinary("test-payload-noscg".getBytes("UTF-8"))
      delta = mkDelta(mg, binary)
      cp = mkCheckpoint(ShardId.unsafeApply(0), shardOrd = 1L, gl0Anchor = 2L, delta = delta)
      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = SortedMap(cp.shardId -> cp), lastSnapshotInfo = emptyGsi)
      capturedAdopted <- capturedAdoptedRef.get
      checkpointCalls <- checkpointCallsRef.get
    } yield expect.all(checkpointCalls.isEmpty, capturedAdopted.isEmpty)
  }

  // ============================================================================
  // Test 3: shardCheckpointAcceptanceManager = None ⇒ regression bar (config says shards but no manager wired)
  // ============================================================================

  test("regression bar: shardCheckpointAcceptanceManager=None ⇒ adopt path never fires even at numShards=4") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      capturedAdoptedRef <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      mgr <- mkSuiteManager(Some(mkShardingConfig(numShards = 4)), checkpointManager = None, capturedRef, capturedAdoptedRef)
      mg = mkAddress("mg-no-mgrwired")
      binary = mkSignedBinary("test-payload-nomgr".getBytes("UTF-8"))
      delta = mkDelta(mg, binary)
      cp = mkCheckpoint(ShardId.unsafeApply(0), shardOrd = 1L, gl0Anchor = 2L, delta = delta)
      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = SortedMap(cp.shardId -> cp), lastSnapshotInfo = emptyGsi)
      captured <- capturedRef.get
      capturedAdopted <- capturedAdoptedRef.get
    } yield expect.all(captured.isEmpty, capturedAdopted.isEmpty)
  }

  // ============================================================================
  // Test 4: numShards=4 with shard checkpoints — adopts every accepted shard's includedSnapshots
  // ============================================================================

  test("numShards=4: 4 accepted shard checkpoints adopt 4 per-MG snapshot chains into the currency pipeline") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      capturedAdoptedRef <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      stubMgr = StubAcceptanceManager(checkpointCallsRef, _ => ShardCheckpointAcceptResult.Accepted)
      mgr <- mkSuiteManager(Some(mkShardingConfig(numShards = 4)), Some(stubMgr), capturedRef, capturedAdoptedRef)

      // Build 4 shard checkpoints, each with 1 MG carrying a unique binary. Per shard:
      //   shardId=0 ⇒ mg-A / binary-A
      //   shardId=1 ⇒ mg-B / binary-B
      //   shardId=2 ⇒ mg-C / binary-C
      //   shardId=3 ⇒ mg-D / binary-D
      mgA = mkAddress("shard-mg-A")
      mgB = mkAddress("shard-mg-B")
      mgC = mkAddress("shard-mg-C")
      mgD = mkAddress("shard-mg-D")
      binaryA = mkSignedBinary("shard-content-A".getBytes("UTF-8"))
      binaryB = mkSignedBinary("shard-content-B".getBytes("UTF-8"))
      binaryC = mkSignedBinary("shard-content-C".getBytes("UTF-8"))
      binaryD = mkSignedBinary("shard-content-D".getBytes("UTF-8"))

      shardCheckpoints = SortedMap[ShardId, ShardCheckpoint](
        ShardId.unsafeApply(0) -> mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, mkDelta(mgA, binaryA)),
        ShardId.unsafeApply(1) -> mkCheckpoint(ShardId.unsafeApply(1), 1L, 2L, mkDelta(mgB, binaryB)),
        ShardId.unsafeApply(2) -> mkCheckpoint(ShardId.unsafeApply(2), 1L, 2L, mkDelta(mgC, binaryC)),
        ShardId.unsafeApply(3) -> mkCheckpoint(ShardId.unsafeApply(3), 1L, 2L, mkDelta(mgD, binaryD))
      )

      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = shardCheckpoints, lastSnapshotInfo = emptyGsi)

      captured <- capturedRef.get
      capturedAdopted <- capturedAdoptedRef.get
      checkpointCalls <- checkpointCallsRef.get
    } yield
      expect.all(
        // Every checkpoint was evaluated by the stub manager
        checkpointCalls.size == 4,
        // The raw `process` chain-link path saw NO events (sharded binaries do NOT take the chain-link path under Axis 1a)
        captured.isEmpty,
        // The adopt path (processCurrencySnapshots) received all 4 per-MG snapshot chains, keyed by MG address
        capturedAdopted.keySet == Set(mgA, mgB, mgC, mgD),
        // Each adopted entry is the verbatim includedSnapshots NEL — sentinel content bytes survive the adopt path unchanged
        capturedAdopted.get(mgA).map(_.toList.map(_.value.content.toSeq)) == Some(List(binaryA.value.content.toSeq)),
        capturedAdopted.get(mgB).map(_.toList.map(_.value.content.toSeq)) == Some(List(binaryB.value.content.toSeq)),
        capturedAdopted.get(mgC).map(_.toList.map(_.value.content.toSeq)) == Some(List(binaryC.value.content.toSeq)),
        capturedAdopted.get(mgD).map(_.toList.map(_.value.content.toSeq)) == Some(List(binaryD.value.content.toSeq))
      )
  }

  // ============================================================================
  // Test 5: CHANGE 3 — at numShards>1 the static assignment is TOTAL, so raw scEvents for metagraph
  //         addresses are EXCLUDED from the base chain-link `process` path (no double-path); sharded MGs
  //         flow only via the adopt path. (Pre-CHANGE-3 this test asserted the raw event took the
  //         chain-link path; that double-path is exactly what CHANGE 3 removes for split-safety.)
  // ============================================================================

  test("CHANGE 3 filter: raw scEvents for metagraph addresses are excluded from the base path at numShards>1; sharded MGs adopt") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      capturedAdoptedRef <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      stubMgr = StubAcceptanceManager(checkpointCallsRef, _ => ShardCheckpointAcceptResult.Accepted)
      mgr <- mkSuiteManager(Some(mkShardingConfig(numShards = 4)), Some(stubMgr), capturedRef, capturedAdoptedRef)

      // A raw scEvent for a metagraph address AND a sharded MG via a checkpoint. With the total static assignment, the
      // raw event's address ALSO maps to a shard, so CHANGE 3 filters it out of the base chain-link path entirely.
      mgRaw = mkAddress("bootstrap-mg-raw")
      mgSharded = mkAddress("bootstrap-mg-sharded")
      binaryRaw = mkSignedBinary("raw-event-content".getBytes("UTF-8"))
      binarySharded = mkSignedBinary("sharded-content".getBytes("UTF-8"))

      rawEvent = StateChannelOutput(mgRaw, binaryRaw)
      shardCheckpoint = mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, mkDelta(mgSharded, binarySharded))

      _ <- invokeAccept(
        mgr,
        scEvents = List(rawEvent),
        shardCheckpoints = SortedMap(ShardId.unsafeApply(0) -> shardCheckpoint),
        lastSnapshotInfo = emptyGsi
      )

      captured <- capturedRef.get
      capturedAdopted <- capturedAdoptedRef.get
      checkpointCalls <- checkpointCallsRef.get
    } yield
      expect.all(
        checkpointCalls.size == 1,
        // CHANGE 3: the raw event was EXCLUDED from the base chain-link path (total static assignment ⇒ filtered) —
        // the base `process` saw NOTHING. This is the double-path removal that prevents the per-node divergence window.
        captured.isEmpty,
        // Sharded MG reached the adopt path — and ONLY the sharded MG.
        capturedAdopted.keySet == Set(mgSharded),
        capturedAdopted.get(mgSharded).map(_.toList.map(_.value.content.toSeq)) == Some(List(binarySharded.value.content.toSeq))
      )
  }

  // ============================================================================
  // Test 6: Rejected shard checkpoint — that shard's snapshots dropped; accepted shard's snapshots still adopted
  // ============================================================================

  test("rejected shard checkpoint: rejected shard's snapshots dropped; accepted shard's snapshots still adopted") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      capturedAdoptedRef <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)

      mgAccepted = mkAddress("accept-mg")
      mgRejected = mkAddress("reject-mg")
      binaryAccepted = mkSignedBinary("accepted-content".getBytes("UTF-8"))
      binaryRejected = mkSignedBinary("rejected-content".getBytes("UTF-8"))

      // Decision policy: shard 0 accepts; shard 1 rejects.
      decision = (cp: ShardCheckpoint) =>
        if (cp.shardId.value.value == 0) ShardCheckpointAcceptResult.Accepted
        else ShardCheckpointAcceptResult.Rejected("synthetic test rejection")
      stubMgr = StubAcceptanceManager(checkpointCallsRef, decision)

      mgr <- mkSuiteManager(Some(mkShardingConfig(numShards = 4)), Some(stubMgr), capturedRef, capturedAdoptedRef)

      shardCheckpoints = SortedMap[ShardId, ShardCheckpoint](
        ShardId.unsafeApply(0) -> mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, mkDelta(mgAccepted, binaryAccepted)),
        ShardId.unsafeApply(1) -> mkCheckpoint(ShardId.unsafeApply(1), 1L, 2L, mkDelta(mgRejected, binaryRejected))
      )

      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = shardCheckpoints, lastSnapshotInfo = emptyGsi)

      capturedAdopted <- capturedAdoptedRef.get
      checkpointCalls <- checkpointCallsRef.get
    } yield
      expect.all(
        // BOTH checkpoints are evaluated (rejection is per-checkpoint, not early-exit)
        checkpointCalls.size == 2,
        // Only the accepted shard's MG is in the adopted map
        capturedAdopted.keySet == Set(mgAccepted),
        capturedAdopted.get(mgAccepted).map(_.toList.map(_.value.content.toSeq)) == Some(List(binaryAccepted.value.content.toSeq)),
        // The rejected shard's MG didn't leak through
        !capturedAdopted.contains(mgRejected)
      )
  }

  // ============================================================================
  // Test 7: PendingMoreAttestations — defer the checkpoint, don't adopt, evaluate everything else
  // ============================================================================

  test("pending shard checkpoint: pending shard's snapshots deferred (not adopted); accepted shard's snapshots still adopted") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      capturedAdoptedRef <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)

      mgAccepted = mkAddress("accept-mg-pending")
      mgPending = mkAddress("pending-mg")
      binaryAccepted = mkSignedBinary("ok-content".getBytes("UTF-8"))
      binaryPending = mkSignedBinary("pending-content".getBytes("UTF-8"))

      decision = (cp: ShardCheckpoint) =>
        if (cp.shardId.value.value == 0) ShardCheckpointAcceptResult.Accepted
        else ShardCheckpointAcceptResult.PendingMoreAttestations
      stubMgr = StubAcceptanceManager(checkpointCallsRef, decision)

      mgr <- mkSuiteManager(Some(mkShardingConfig(numShards = 4)), Some(stubMgr), capturedRef, capturedAdoptedRef)

      shardCheckpoints = SortedMap[ShardId, ShardCheckpoint](
        ShardId.unsafeApply(0) -> mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, mkDelta(mgAccepted, binaryAccepted)),
        ShardId.unsafeApply(1) -> mkCheckpoint(ShardId.unsafeApply(1), 1L, 2L, mkDelta(mgPending, binaryPending))
      )

      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = shardCheckpoints, lastSnapshotInfo = emptyGsi)

      capturedAdopted <- capturedAdoptedRef.get
      checkpointCalls <- checkpointCallsRef.get
    } yield
      expect.all(
        checkpointCalls.size == 2,
        capturedAdopted.keySet == Set(mgAccepted),
        !capturedAdopted.contains(mgPending)
      )
  }

  // ============================================================================
  // Test 8: three-paths byte-identity — produce / createContext / validateArtifact all funnel through accept()
  //         with the SAME embedded checkpoints + prior state ⇒ byte-identical accepted state + state proof.
  // ============================================================================

  /** The central-invariant GSAM-level test. Produce embeds the `verifyEmbedded`-accepted checkpoints into the artifact; `createContext` and
    * `validateArtifact` read them back from `artifact.shardCheckpoints` and thread them into accept(). So all three paths invoke `accept()`
    * with the SAME `(shardCheckpoints, scEvents, lastSnapshotContext)`. Because the adopt path is a pure function of those inputs
    * (deterministic `verifyEmbedded` + the deterministic CHANGE-3 filter + the shared assembly), three INDEPENDENT GSAM instances (three
    * nodes) produce byte-identical accepted `scSnapshots` and state-proof `mptRoot`. This is the byte-identity the symmetric-adopt design
    * guarantees. The CHANGE-3 filter is also exercised: a raw `scEvent` for a sharded MG is excluded from the base path (it would otherwise
    * diverge per node), leaving the adopt path as the sole source for sharded MGs.
    */
  test("three paths byte-identical: accept() over the same embedded checkpoints + prior state is node-independent") { res =>
    implicit val (h, sp) = res

    // A processor that DERIVES a deterministic non-empty result from the adopted snapshots, so the comparison is meaningful
    // (not trivially-empty). `processCurrencySnapshots` echoes each adopted binary as a `(binary, None)` pair with an empty
    // balance update; `assembleAcceptanceResult` inlines the SAME pure projection production uses (deterministic by inputs).
    val derivingProcessor: GlobalSnapshotStateChannelEventsProcessor[IO] =
      new GlobalSnapshotStateChannelEventsProcessor[IO] {
        override def process(
          snapshotOrdinal: SnapshotOrdinal,
          currentBalances: SortedMap[Address, Balance],
          priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
          priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
            CurrencySnapshot
          ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
          events: List[StateChannelOutput],
          validationType: StateChannelValidationType,
          getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
        )(implicit hasher: Hasher[IO]): IO[StateChannelAcceptanceResult] =
          // Base path only ever sees the (filtered) non-sharded events; with a total assignment that's empty. Echo whatever
          // (non-sharded) events arrive as accepted so the base result is faithful; CHANGE 3 means this is empty in this test.
          StateChannelAcceptanceResult(
            accepted =
              SortedMap.from(events.groupBy(_.address).map { case (a, es) => a -> NonEmptyList.fromListUnsafe(es.map(_.snapshotBinary)) }),
            calculatedCurrencyState = SortedMap.empty[Address, CurrencySnapshotWithState],
            returned = Set.empty[StateChannelOutput],
            balanceUpdate = SortedMap.empty[Address, Balance],
            incomingCurrencySnapshotsWithState = SortedMap.empty[Address, List[CurrencySnapshotWithState]]
          ).pure[IO]

        override def processCurrencySnapshots(
          snapshotOrdinal: SnapshotOrdinal,
          currentBalances: SortedMap[Address, Balance],
          priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
            CurrencySnapshot
          ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
          events: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
          getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
        )(implicit hasher: Hasher[IO]): IO[SortedMap[Address, MetagraphAcceptanceResult]] =
          events.map {
            case (addr, bins) => addr -> ((bins.map(b => (b, none[CurrencySnapshotWithState])), SortedMap.empty[Address, Balance]))
          }.pure[IO]

        override def assembleAcceptanceResult(
          processed: SortedMap[Address, MetagraphAcceptanceResult],
          priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
            CurrencySnapshot
          ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
          returned: Set[StateChannelOutput]
        ): StateChannelAcceptanceResult = {
          // Inlined production projection (pure ⇒ deterministic by inputs); identical across all three node instances.
          val lastStatePerAddress: SortedMap[Address, CurrencySnapshotWithState] =
            processed.map { case (k, (v, _)) => k -> v.toList.flatMap(_._2).lastOption }.collect { case (key, Some(s)) => key -> s }
          val incoming: SortedMap[Address, List[CurrencySnapshotWithState]] =
            processed.map { case (k, (v, _)) => k -> v.toList.flatMap(_._2) }.filterNot { case (_, list) => list.isEmpty }
          StateChannelAcceptanceResult(
            accepted = processed.map { case (k, (v, _)) => k -> v.map(_._1) },
            calculatedCurrencyState = priorLastCurrencySnapshots.concat(lastStatePerAddress),
            returned = returned,
            balanceUpdate = processed.values.map(_._2).foldLeft(SortedMap.empty[Address, Balance])(_ ++ _),
            incomingCurrencySnapshotsWithState = incoming
          )
        }

        override def deriveMetagraphRoot(
          metagraphAddress: Address,
          binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]],
          snapshotOrdinal: SnapshotOrdinal,
          getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
        )(implicit hasher: Hasher[IO]): IO[Hash] = Hash.empty.pure[IO]
      }

    // Build a fresh GSAM instance whose processor is the deriving processor — represents one node. Each call is independent;
    // the verifyEmbedded-Accepted stub + the wired shardAssignment make this an end-to-end adopt path.
    def mkNodeManager(): IO[GlobalSnapshotAcceptanceManager[IO]] =
      for {
        checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
        stubMgr = StubAcceptanceManager(checkpointCallsRef, _ => ShardCheckpointAcceptResult.Accepted)
        mgr <- mkSuiteManagerWithProcessor(Some(mkShardingConfig(numShards = 4)), Some(stubMgr), derivingProcessor)
      } yield mgr

    for {
      mgSharded <- IO.pure(mkAddress("three-paths-sharded-mg"))
      binarySharded = mkSignedBinary("three-paths-content".getBytes("UTF-8"))
      cp = mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, mkDelta(mgSharded, binarySharded))
      embedded = SortedMap[ShardId, ShardCheckpoint](cp.shardId -> cp)
      // A raw scEvent for a (sharded) MG — CHANGE 3 must exclude it from the base path on every node.
      rawShardedEvent = StateChannelOutput(mkAddress("three-paths-raw-sharded"), mkSignedBinary("raw".getBytes("UTF-8")))

      // Three independent node instances all run accept() with the SAME embedded checkpoints + scEvents + prior state.
      mgrProduce <- mkNodeManager()
      mgrCreateContext <- mkNodeManager()
      mgrValidate <- mkNodeManager()
      produce <- invokeAcceptCapturing(mgrProduce, List(rawShardedEvent), embedded, emptyGsi)
      createCtx <- invokeAcceptCapturing(mgrCreateContext, List(rawShardedEvent), embedded, emptyGsi)
      validate <- invokeAcceptCapturing(mgrValidate, List(rawShardedEvent), embedded, emptyGsi)
    } yield
      expect.all(
        // The adopted sharded MG is in the accepted scSnapshots (adopt path produced it) ...
        produce._1.keySet.contains(mgSharded),
        // ... and the raw sharded event was EXCLUDED from the base path (CHANGE 3) — its address is not present.
        !produce._1.keySet.contains(rawShardedEvent.address),
        // All three paths: byte-identical accepted scSnapshots ...
        produce._1 == createCtx._1,
        produce._1 == validate._1,
        // ... and byte-identical state-proof mptRoot.
        produce._2 == createCtx._2,
        produce._2 == validate._2
      )
  }
}
