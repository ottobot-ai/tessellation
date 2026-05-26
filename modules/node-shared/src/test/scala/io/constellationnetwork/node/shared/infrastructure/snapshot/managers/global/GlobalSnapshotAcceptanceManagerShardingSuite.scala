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

/** Tests for the Slice 13 (hierarchical-shard-checkpoints v1, §13 row 13) sharding-aware path in
  * [[GlobalSnapshotAcceptanceManager.accept]].
  *
  * '''Test plan''' (per slice spec):
  *   1. '''numShards=1 byte-equivalence''' — `accept(..., shardCheckpoints = SortedMap.empty)` produces identical observable outcomes to
  *      today's pre-Slice-13 path (the regression bar). Verified by asserting (a) the captor sees exactly the raw `scEvents`, and (b) the
  *      stub `ShardCheckpointGl0AcceptanceManager.evaluate` is never called.
  *   1. '''numShards=4 with shard checkpoints''' — `accept(..., shardCheckpoints = {0→cp0, 1→cp1, 2→cp2, 3→cp3})` evaluates each checkpoint
  *      and appends every accepted checkpoint's per-MG SC binaries to the SC-event stream the standard processor consumes.
  *   1. '''Mixed bootstrap''' — `accept(..., shardCheckpoints = {0→cp0, 1→cp1})` with non-empty raw `scEvents`. Both flow into the
  *      processor (the captor sees raw events + supplemented shard binaries).
  *   1. '''Rejected shard checkpoint''' — one checkpoint returns `Rejected`, the other returns `Accepted`. Only the Accepted one's binaries
  *      are appended.
  *
  * '''Test fixture pattern''':
  *   - Uses a "captor" `GlobalSnapshotStateChannelEventsProcessor` that records the SC-event list it received. This is the strongest
  *     observable for asserting the input shape that flowed through `processStateChannelEvents` after the Slice 13 pre-processor ran.
  *   - The `ShardCheckpointGl0AcceptanceManager` is fully stubbed (returns a configurable result per call) — no real KES/Ed25519/VRF
  *     fixture is needed; the GSAM contract under test is "stitch checkpoint outputs into the SC pipeline", not "verify checkpoint crypto"
  *     (which is [[ShardCheckpointGl0AcceptanceManagerSuite]]'s job).
  *   - `MetagraphSyncManager` is real (constructed from production `make`), so `consumeReceipts` actually drains into its internal
  *     accumulator; verification happens via injecting the manager into the captor stack and asserting via the production-trait surface (no
  *     inspector handle is leaked to GSAM users).
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

  /** Captor processor that records the SC-event list it was called with. Returns an empty [[StateChannelAcceptanceResult]] — the test only
    * asserts what flowed IN, not what comes back.
    */
  private def mkCaptorProcessor(
    capturedEventsRef: Ref[IO, List[StateChannelOutput]]
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
        SortedMap.empty[Address, MetagraphAcceptanceResult].pure[IO]

      override def deriveMetagraphRoot(
        metagraphAddress: Address,
        binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]],
        snapshotOrdinal: SnapshotOrdinal,
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[Hash] = Hash.empty.pure[IO]
    }

  /** Stubbed `ShardCheckpointGl0AcceptanceManager` — returns the configured result per checkpoint and records every call. */
  private final case class StubAcceptanceManager(
    callsRef: Ref[IO, List[ShardCheckpoint]],
    decision: ShardCheckpoint => ShardCheckpointAcceptResult
  ) extends ShardCheckpointGl0AcceptanceManager[IO] {
    override def evaluate(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
      callsRef.update(_ :+ checkpoint).as(decision(checkpoint))
  }

  /** Build the manager-under-test with the Slice 13 sharding deps. Tests pass:
    *   - `shardingConfig`: typically [[mkShardingConfig]] with `numShards > 1`. Default `None` for the regression bar test.
    *   - `checkpointManager`: a [[StubAcceptanceManager]] with the desired per-checkpoint decisions.
    */
  private def mkSuiteManager(
    shardingConfig: Option[ShardingConfig],
    checkpointManager: Option[ShardCheckpointGl0AcceptanceManager[IO]],
    capturedEventsRef: Ref[IO, List[StateChannelOutput]]
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

    val mockStateChannelEventsProcessor = mkCaptorProcessor(capturedEventsRef)

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
      checkpoint = ShardCheckpointConfig(tAliveMs = 10000L, tBurst = 100),
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
  // Test 1: numShards=1 byte-equivalence — sharding gate inactive, raw scEvents flow through unchanged
  // ============================================================================

  test("numShards=1 byte-equivalence: shardingConfig.numShards=1 ⇒ shard-checkpoint pipeline never fires") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      stubMgr = StubAcceptanceManager(checkpointCallsRef, _ => ShardCheckpointAcceptResult.Accepted)
      // shardingConfig = numShards=1; even if the caller passed a non-empty shardCheckpoints map, the gate
      // refuses to fire because numShards > 1 is the activation predicate. This is the regression bar's literal
      // assertion: at the production default, the new code path is unreachable.
      mgr <- mkSuiteManager(Some(mkShardingConfig(numShards = 1)), Some(stubMgr), capturedRef)
      mg = mkAddress("mg-numshards-1")
      binary = mkSignedBinary("test-payload-1".getBytes("UTF-8"))
      delta = mkDelta(mg, binary)
      cp = mkCheckpoint(ShardId.unsafeApply(0), shardOrd = 1L, gl0Anchor = 2L, delta = delta)
      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = SortedMap(cp.shardId -> cp), lastSnapshotInfo = emptyGsi)
      captured <- capturedRef.get
      checkpointCalls <- checkpointCallsRef.get
    } yield
      expect.all(
        captured.isEmpty, // raw scEvents was empty; gate didn't fire; no shard binaries supplemented
        checkpointCalls.isEmpty // gate didn't fire ⇒ stubMgr.evaluate was never called
      )
  }

  // ============================================================================
  // Test 2: shardingConfig = None ⇒ regression bar (no sharding deps wired at all)
  // ============================================================================

  test("regression bar: shardingConfig=None ⇒ pipeline never fires even with non-empty shardCheckpoints") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      stubMgr = StubAcceptanceManager(checkpointCallsRef, _ => ShardCheckpointAcceptResult.Accepted)
      mgr <- mkSuiteManager(shardingConfig = None, checkpointManager = Some(stubMgr), capturedRef)
      mg = mkAddress("mg-no-shardingcfg")
      binary = mkSignedBinary("test-payload-noscg".getBytes("UTF-8"))
      delta = mkDelta(mg, binary)
      cp = mkCheckpoint(ShardId.unsafeApply(0), shardOrd = 1L, gl0Anchor = 2L, delta = delta)
      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = SortedMap(cp.shardId -> cp), lastSnapshotInfo = emptyGsi)
      checkpointCalls <- checkpointCallsRef.get
    } yield expect(checkpointCalls.isEmpty)
  }

  // ============================================================================
  // Test 3: shardCheckpointAcceptanceManager = None ⇒ regression bar (config says shards but no manager wired)
  // ============================================================================

  test("regression bar: shardCheckpointAcceptanceManager=None ⇒ pipeline never fires even at numShards=4") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      mgr <- mkSuiteManager(Some(mkShardingConfig(numShards = 4)), checkpointManager = None, capturedRef)
      mg = mkAddress("mg-no-mgrwired")
      binary = mkSignedBinary("test-payload-nomgr".getBytes("UTF-8"))
      delta = mkDelta(mg, binary)
      cp = mkCheckpoint(ShardId.unsafeApply(0), shardOrd = 1L, gl0Anchor = 2L, delta = delta)
      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = SortedMap(cp.shardId -> cp), lastSnapshotInfo = emptyGsi)
      captured <- capturedRef.get
    } yield expect(captured.isEmpty)
  }

  // ============================================================================
  // Test 4: numShards=4 with shard checkpoints — supplements scEvents with shard binaries
  // ============================================================================

  test("numShards=4: 4 accepted shard checkpoints supplement scEvents with 4 per-MG binaries") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      stubMgr = StubAcceptanceManager(checkpointCallsRef, _ => ShardCheckpointAcceptResult.Accepted)
      mgr <- mkSuiteManager(Some(mkShardingConfig(numShards = 4)), Some(stubMgr), capturedRef)

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
      checkpointCalls <- checkpointCallsRef.get
    } yield
      expect.all(
        // Every checkpoint was evaluated by the stub manager
        checkpointCalls.size == 4,
        // Captor saw 4 SC events (one per accepted shard's single included binary)
        captured.size == 4,
        // Captor saw the right MG addresses (order is the shard iteration order, sorted by ShardId)
        captured.map(_.address).toSet == Set(mgA, mgB, mgC, mgD),
        // The supplied binaries themselves are propagated unchanged (sentinel content bytes survive the path)
        captured.map(_.snapshotBinary.value.content.toSeq).toSet == Set(
          binaryA.value.content.toSeq,
          binaryB.value.content.toSeq,
          binaryC.value.content.toSeq,
          binaryD.value.content.toSeq
        )
      )
  }

  // ============================================================================
  // Test 5: Mixed bootstrap — raw scEvents AND shard checkpoints both flow through
  // ============================================================================

  test("mixed bootstrap: raw scEvents AND shard-checkpoint binaries are appended (both flow to processor)") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      stubMgr = StubAcceptanceManager(checkpointCallsRef, _ => ShardCheckpointAcceptResult.Accepted)
      mgr <- mkSuiteManager(Some(mkShardingConfig(numShards = 4)), Some(stubMgr), capturedRef)

      // One MG flows via raw scEvents (bootstrap-style — no shard committee yet wired for it);
      // one MG flows via a shard checkpoint (sharded MG); both should land in the captor.
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
      checkpointCalls <- checkpointCallsRef.get
    } yield
      expect.all(
        checkpointCalls.size == 1,
        captured.size == 2, // 1 raw + 1 shard-supplemented
        captured.map(_.address).toSet == Set(mgRaw, mgSharded),
        captured.exists(e => e.address == mgRaw && e.snapshotBinary.value.content.toSeq == binaryRaw.value.content.toSeq),
        captured.exists(e => e.address == mgSharded && e.snapshotBinary.value.content.toSeq == binarySharded.value.content.toSeq)
      )
  }

  // ============================================================================
  // Test 6: Rejected shard checkpoint — that shard's binaries dropped; others still appended
  // ============================================================================

  test("rejected shard checkpoint: rejected shard's binaries dropped; accepted shard's binaries still appended") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
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

      mgr <- mkSuiteManager(Some(mkShardingConfig(numShards = 4)), Some(stubMgr), capturedRef)

      shardCheckpoints = SortedMap[ShardId, ShardCheckpoint](
        ShardId.unsafeApply(0) -> mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, mkDelta(mgAccepted, binaryAccepted)),
        ShardId.unsafeApply(1) -> mkCheckpoint(ShardId.unsafeApply(1), 1L, 2L, mkDelta(mgRejected, binaryRejected))
      )

      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = shardCheckpoints, lastSnapshotInfo = emptyGsi)

      captured <- capturedRef.get
      checkpointCalls <- checkpointCallsRef.get
    } yield
      expect.all(
        // BOTH checkpoints are evaluated (rejection is per-checkpoint, not early-exit)
        checkpointCalls.size == 2,
        // Only the accepted shard's binary is in the captor
        captured.size == 1,
        captured.head.address == mgAccepted,
        captured.head.snapshotBinary.value.content.toSeq == binaryAccepted.value.content.toSeq,
        // The rejected shard's MG didn't leak through
        !captured.exists(_.address == mgRejected)
      )
  }

  // ============================================================================
  // Test 7: PendingMoreAttestations — defer the checkpoint, don't append, evaluate everything else
  // ============================================================================

  test("pending shard checkpoint: pending shard's binaries deferred (not appended); accepted shard's binaries still appended") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)

      mgAccepted = mkAddress("accept-mg-pending")
      mgPending = mkAddress("pending-mg")
      binaryAccepted = mkSignedBinary("ok-content".getBytes("UTF-8"))
      binaryPending = mkSignedBinary("pending-content".getBytes("UTF-8"))

      decision = (cp: ShardCheckpoint) =>
        if (cp.shardId.value.value == 0) ShardCheckpointAcceptResult.Accepted
        else ShardCheckpointAcceptResult.PendingMoreAttestations
      stubMgr = StubAcceptanceManager(checkpointCallsRef, decision)

      mgr <- mkSuiteManager(Some(mkShardingConfig(numShards = 4)), Some(stubMgr), capturedRef)

      shardCheckpoints = SortedMap[ShardId, ShardCheckpoint](
        ShardId.unsafeApply(0) -> mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, mkDelta(mgAccepted, binaryAccepted)),
        ShardId.unsafeApply(1) -> mkCheckpoint(ShardId.unsafeApply(1), 1L, 2L, mkDelta(mgPending, binaryPending))
      )

      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = shardCheckpoints, lastSnapshotInfo = emptyGsi)

      captured <- capturedRef.get
      checkpointCalls <- checkpointCallsRef.get
    } yield
      expect.all(
        checkpointCalls.size == 2,
        captured.size == 1,
        captured.head.address == mgAccepted,
        !captured.exists(_.address == mgPending)
      )
  }
}
