package io.constellationnetwork.dag.l0.infrastructure.snapshot

import cats.data.Validated.Valid
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect._
import cats.effect.std.Supervisor
import cats.implicits.none
import cats.syntax.applicative._
import cats.syntax.list._

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.reflect.runtime.universe.TypeTag

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.dag.l0.domain.snapshot.programs.{
  GlobalSnapshotEventCutter,
  SnapshotBinaryFeeCalculator,
  UpdateNodeParametersCutter
}
import io.constellationnetwork.dag.l0.infrastructure.rewards.RewardsService
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event.{GlobalSnapshotEvent, StateChannelEvent}
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.env.AppEnvironment.Dev
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.DelegatedRewardsConfigProvider
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.delegatedStake.{
  UpdateDelegatedStakeAcceptanceManager,
  UpdateDelegatedStakeAcceptanceResult,
  UpdateDelegatedStakeValidator
}
import io.constellationnetwork.node.shared.domain.fork.ForkInfo
import io.constellationnetwork.node.shared.domain.gossip.Gossip
import io.constellationnetwork.node.shared.domain.node.{UpdateNodeParametersAcceptanceManager, UpdateNodeParametersValidator}
import io.constellationnetwork.node.shared.domain.nodeCollateral.{UpdateNodeCollateralAcceptanceManager, UpdateNodeCollateralValidator}
import io.constellationnetwork.node.shared.domain.priceOracle.{PriceStateUpdater, PricingUpdateValidator}
import io.constellationnetwork.node.shared.domain.rewards.Rewards
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator
import io.constellationnetwork.node.shared.domain.swap.block._
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.{ConsensusTrigger, EventTrigger}
import io.constellationnetwork.node.shared.infrastructure.delegatedStake.{RewardsInfoCalculator, RewardsInfoStorage}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.snapshot._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  GlobalSnapshotAcceptanceManager,
  GlobalSnapshotStateChannelEventsProcessor
}
import io.constellationnetwork.node.shared.logger.Slf4jLoggerBundle
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.node.RewardFraction
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.tokenLock.TokenLockBlock
import io.constellationnetwork.schema.{GlobalStateProofSelector, _}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.SignedValidator.SignedValidationErrorOr
import io.constellationnetwork.security.signature.{Signed, SignedValidator}
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary, StateChannelValidationType}
import io.constellationnetwork.syntax.sortedCollection._

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import io.circe.{Encoder, Json}
import weaver.MutableIOSuite
import weaver.scalacheck.Checkers

object GlobalSnapshotConsensusFunctionsSuite extends MutableIOSuite with Checkers {
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  type Res = (Supervisor[IO], JsonSerializer[IO], Hasher[IO], SecurityProvider[IO], Metrics[IO])

  def mkMockGossip[B](spreadRef: Ref[IO, List[B]]): Gossip[IO] =
    new Gossip[IO] {
      override def spread[A: TypeTag: Encoder](rumorContent: A): IO[Unit] =
        spreadRef.update(rumorContent.asInstanceOf[B] :: _)

      override def spreadCommon[A: TypeTag: Encoder](rumorContent: A): IO[Unit] =
        IO.raiseError(new Exception("spreadCommon: Unexpected call"))

      override def spreadDirect[A: TypeTag: Encoder](rumorContent: A, targets: Set[PeerId]): IO[Unit] = IO.unit

      override def setDirectPushFn(fn: Gossip.DirectPushFn[IO]): IO[Unit] = IO.unit

      override def setSidecarPublishFn(fn: Gossip.SidecarPublishFn[IO]): IO[Unit] = IO.unit
    }

  def mkSignedArtifacts()(
    implicit sp: SecurityProvider[IO],
    h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[(Signed[GlobalSnapshotArtifact], Signed[GlobalSnapshot])] = for {
    keyPair <- KeyPairGenerator.makeKeyPair[IO]

    genesis = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue)
    signedGenesis <- Signed.forAsyncHasher[IO, GlobalSnapshot](genesis, keyPair)

    lastArtifact <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis.value)
    signedLastArtifact <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact, keyPair)
  } yield (signedLastArtifact, signedGenesis)

  def sharedResource: Resource[IO, Res] = for {
    supervisor <- Supervisor[IO]
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
    m <- Metrics.forAsync[IO](Seq((Metrics.unsafeLabelName("application"), name)))

  } yield (supervisor, j, h, sp, m)

  val bam: BlockAcceptanceManager[IO] = new BlockAcceptanceManager[IO] {

    override def acceptBlocksIteratively(
      blocks: List[Signed[Block]],
      context: BlockAcceptanceContext[IO],
      ordinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean = true
    )(implicit hasher: Hasher[F]): IO[BlockAcceptanceResult] =
      BlockAcceptanceResult(
        BlockAcceptanceContextUpdate.empty,
        List.empty,
        List.empty
      ).pure[IO]

    override def acceptBlock(
      block: Signed[Block],
      context: BlockAcceptanceContext[IO],
      ordinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean = true
    )(implicit hasher: Hasher[F]): IO[Either[BlockNotAcceptedReason, (BlockAcceptanceContextUpdate, UsageCount)]] = ???

  }

  val asbam: AllowSpendBlockAcceptanceManager[IO] = new AllowSpendBlockAcceptanceManager[IO] {
    override def acceptBlock(
      block: Signed[swap.AllowSpendBlock],
      context: AllowSpendBlockAcceptanceContext[IO],
      snapshotOrdinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean = true,
      lastGlobalSnapshotEpochProgress: Option[EpochProgress]
    )(implicit hasher: Hasher[IO]): IO[Either[AllowSpendBlockNotAcceptedReason, AllowSpendBlockAcceptanceContextUpdate]] = ???

    override def acceptBlocksIteratively(
      blocks: List[Signed[swap.AllowSpendBlock]],
      context: AllowSpendBlockAcceptanceContext[IO],
      snapshotOrdinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean = true,
      lastGlobalSnapshotEpochProgress: Option[EpochProgress]
    )(implicit hasher: Hasher[IO]): IO[AllowSpendBlockAcceptanceResult] =
      AllowSpendBlockAcceptanceResult(
        AllowSpendBlockAcceptanceContextUpdate.empty,
        List.empty,
        List.empty
      ).pure[IO]
  }

  val tlbam: TokenLockBlockAcceptanceManager[IO] = new TokenLockBlockAcceptanceManager[IO] {
    override def acceptBlock(
      block: Signed[TokenLockBlock],
      context: TokenLockBlockAcceptanceContext[IO],
      snapshotOrdinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean = true,
      lastGlobalSnapshotEpochProgress: Option[EpochProgress]
    )(implicit hasher: Hasher[IO]): IO[Either[TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate]] = ???

    override def acceptBlocksIteratively(
      blocks: List[Signed[TokenLockBlock]],
      context: TokenLockBlockAcceptanceContext[IO],
      snapshotOrdinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean = true,
      lastGlobalSnapshotEpochProgress: Option[EpochProgress]
    )(implicit hasher: Hasher[IO]): IO[TokenLockBlockAcceptanceResult] =
      TokenLockBlockAcceptanceResult(
        TokenLockBlockAcceptanceContextUpdate.empty,
        List.empty,
        List.empty
      ).pure[IO]
  }

  val scProcessor: GlobalSnapshotStateChannelEventsProcessor[IO] = new GlobalSnapshotStateChannelEventsProcessor[IO] {
    def process(
      snapshotOrdinal: GlobalSnapshotKey,
      currentBalances: SortedMap[Address, Balance],
      priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
      priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
        CurrencySnapshot
      ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
      events: List[StateChannelOutput],
      validationType: StateChannelValidationType,
      getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
    )(implicit hasher: Hasher[F]): IO[StateChannelAcceptanceResult] = IO(
      StateChannelAcceptanceResult(
        events.groupByNel(_.address).view.mapValues(_.map(_.snapshotBinary)).toSortedMap,
        SortedMap.empty,
        Set.empty,
        SortedMap.empty,
        SortedMap.empty
      )
    )

    def processCurrencySnapshots(
      snapshotOrdinal: SnapshotOrdinal,
      currentBalances: SortedMap[Address, Balance],
      priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
        CurrencySnapshot
      ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
      events: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
      getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
    )(implicit hasher: Hasher[F]): IO[
      SortedMap[
        Address,
        (NonEmptyList[(Signed[StateChannelSnapshotBinary], Option[CurrencySnapshotWithState])], SortedMap[Address, Balance])
      ]
    ] = ???

    def deriveMetagraphRoot(
      metagraphAddress: Address,
      binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]],
      snapshotOrdinal: SnapshotOrdinal,
      getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
    )(implicit hasher: Hasher[F]): IO[Hash] = ???

    def assembleAcceptanceResult(
      processed: SortedMap[
        Address,
        (NonEmptyList[(Signed[StateChannelSnapshotBinary], Option[CurrencySnapshotWithState])], SortedMap[Address, Balance])
      ],
      priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
        CurrencySnapshot
      ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
      returned: Set[StateChannelOutput]
    ): StateChannelAcceptanceResult = ???

  }

  private val signedValidator = new SignedValidator[IO] {
    override def validateSignatures[A: Encoder](signed: Signed[A])(implicit hasher: Hasher[IO]): IO[SignedValidationErrorOr[Signed[A]]] =
      IO.pure(Valid(signed))

    override def validateUniqueSigners[A: Encoder](signed: Signed[A]): SignedValidationErrorOr[Signed[A]] = ???

    override def validateMinSignatureCount[A: Encoder](signed: Signed[A], minSignatureCount: PosInt): SignedValidationErrorOr[Signed[A]] =
      ???

    override def validateMaxSignatureCount[A: Encoder](signed: Signed[A], maxSignatureCount: PosInt): SignedValidationErrorOr[Signed[A]] =
      ???

    override def isSignedBy[A: Encoder](signed: Signed[A], signerAddress: Address): IO[SignedValidationErrorOr[Signed[A]]] = ???

    override def isSignedExclusivelyBy[A: Encoder](signed: Signed[A], signerAddress: Address): IO[SignedValidationErrorOr[Signed[A]]] =
      ???

    override def validateSignaturesWithSeedlist[A <: AnyRef](
      seedlist: Option[Set[PeerId]],
      signed: Signed[A]
    ): SignedValidationErrorOr[Signed[A]] = ???

    override def validateSignedBySeedlistMajority[A](
      seedlist: Option[Set[PeerId]],
      signed: Signed[A]
    ): SignedValidationErrorOr[Signed[A]] = ???

    override def validateAtLeastOneSignatureInSeedlist[A](
      seedlist: Option[Set[PeerId]],
      signed: Signed[A]
    ): SignedValidationErrorOr[Signed[A]] = ???
  }

  def updateNodeParametersAcceptanceManager(implicit txHasher: Hasher[IO]) = {
    val updateNodeParametersValidator =
      UpdateNodeParametersValidator.make(signedValidator, RewardFraction(5_000_000), RewardFraction(10_000_000), PosInt(140), None)
    UpdateNodeParametersAcceptanceManager.make(updateNodeParametersValidator)
  }

  def updateDelegatedStakeAcceptanceManager(implicit hasher: Hasher[IO], sp: SecurityProvider[IO]) = {
    val validator =
      UpdateDelegatedStakeValidator.make(signedValidator, None)
    UpdateDelegatedStakeAcceptanceManager.make(validator)
  }

  def updateNodeCollateralAcceptanceManager(implicit hasher: Hasher[IO], sp: SecurityProvider[IO]) = {
    val validator =
      UpdateNodeCollateralValidator.make(signedValidator, None)
    UpdateNodeCollateralAcceptanceManager.make(validator)
  }

  val collateral: Amount = Amount.empty

  val classicRewards: Rewards[F, GlobalSnapshotStateProof, GlobalIncrementalSnapshot, GlobalSnapshotEvent] =
    (_, _, _, _, _, _) => IO(SortedSet.empty)

  val delegatorRewards: DelegatedRewardsDistributor[F] = new DelegatedRewardsDistributor[F] {
    def calculateVariableInflation(
      epochProgress: EpochProgress,
      lastSnapshotContext: GlobalSnapshotContext
    ): GlobalSnapshotConsensusFunctionsSuite.F[Amount] = Amount(100L).pure[F]

    def distribute(
      lastSnapshotContext: GlobalSnapshotContext,
      trigger: ConsensusTrigger,
      epochProgress: EpochProgress,
      facilitators: List[(Address, PeerId)],
      delegatedStakeDiffs: UpdateDelegatedStakeAcceptanceResult,
      partitionedRecords: PartitionedStakeUpdates
    ): GlobalSnapshotConsensusFunctionsSuite.F[DelegatedRewardsResult] =
      DelegatedRewardsResult(
        SortedMap.empty,
        SortedMap.empty,
        SortedMap.empty,
        SortedSet.empty,
        SortedSet.empty,
        SortedSet.empty,
        Amount.empty
      )
        .pure[F]

    def getEmissionConfig(epochProgress: EpochProgress): GlobalSnapshotConsensusFunctionsSuite.F[EmissionConfigEntry] =
      delegatedRewardsConfigProvider
        .getConfig()
        .emissionConfig
        .head
        ._2(epochProgress)
        .pure[F]
  }

  val delegatedRewardsConfigProvider: DelegatedRewardsConfigProvider = new DelegatedRewardsConfigProvider {
    def getConfig(): DelegatedRewardsConfig =
      DelegatedRewardsConfig(
        flatInflationRate = io.constellationnetwork.schema.NonNegFraction.unsafeFrom(0, 100),
        emissionConfig = Map.empty,
        percentDistribution = Map.empty,
        oneTimeRewards = Map.empty,
        priceOracleEpoch = Map.empty
      )
  }

  def mkGlobalSnapshotConsensusFunctions(
    shardAcceptanceDeps: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[IO]
    ] = None
  )(
    implicit j: JsonSerializer[IO],
    sp: SecurityProvider[IO],
    h: Hasher[IO],
    m: Metrics[IO]
  ): IO[GlobalSnapshotConsensusFunctions[IO]] = {
    implicit val hs = HasherSelector.forSyncAlwaysCurrent(h)

    val spendActionValidator = SpendActionValidator.make[IO]

    val pricingUpdateValidator = PricingUpdateValidator.make[IO](None, NonNegLong(0))
    val priceStateUpdater = PriceStateUpdater.make[IO](
      Dev,
      delegatedRewardsConfigProvider,
      io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.empty[IO]
    )

    val feeCalculator = new SnapshotBinaryFeeCalculator[IO] {
      override def calculateFee(
        event: StateChannelEvent,
        ordinal: SnapshotOrdinal
      ): IO[NonNegLong] =
        event.value.snapshotBinary.value.fee.value.pure[IO]
    }

    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](
        mptProducer,
        GlobalStateKey.toHex[IO]
      )
      pcTree <- io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree.make[IO]
      mptOverlay = io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay
        .passthrough[IO, GlobalStateKey](mptStore, pcTree)
      dbLogger <- Slf4jLoggerBundle.makeUnsafe[IO]
      snapshotAcceptanceManager <- GlobalSnapshotAcceptanceManager
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
          Dev,
          bam,
          asbam,
          tlbam,
          scProcessor,
          updateNodeParametersAcceptanceManager,
          updateDelegatedStakeAcceptanceManager,
          updateNodeCollateralAcceptanceManager,
          spendActionValidator,
          pricingUpdateValidator,
          priceStateUpdater,
          collateral,
          EpochProgress(NonNegLong(136080L)),
          mptOverlay,
          dbLogger
        )
      rewardsInfoStorage <- RewardsInfoStorage.make
      // §G5: wire the same MPT-backed state managers used by the production code path. The test's
      // `mptStore` is empty initially, so the materializers return empty maps — identical to the
      // GSI-driven prior behaviour where `info.activeDelegatedStakes` defaulted to `SortedMap.empty`.
      g5Reader = io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore[IO](mptStore)
      g5StakeManager = io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.DelegatedStakeStateManager
        .make[IO](g5Reader)
      g5UnpReader = io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.UpdateNodeParametersStateReader
        .make[IO](g5Reader)
      g5BalanceManager = io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.SpendTransactionBalanceManager
        .make[IO](g5Reader)
      rewardsInfoCalculator = RewardsInfoCalculator.make(delegatorRewards, g5StakeManager, g5UnpReader, g5BalanceManager)
      rewardsService = RewardsService[IO](classicRewards, delegatorRewards, rewardsInfoCalculator, rewardsInfoStorage)
      // Task #12 slice 2b — the producer's hash-keyed changeset-staging Ref (empty for this unit suite; the
      // promotion/finality path is exercised in the dag-l0 integration loop, not here).
      pendingAccumulatorsRef <- Ref.of[IO, Map[Hash, io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator]](
        Map.empty
      )
      globalSnapshotConsensusFunction = GlobalSnapshotConsensusFunctions
        .make[IO](
          snapshotAcceptanceManager,
          collateral,
          rewardsService,
          GlobalSnapshotEventCutter.make[IO](20_000_000, feeCalculator),
          UpdateNodeParametersCutter.make(100),
          AppEnvironment.Dev,
          delegatedRewardsConfigProvider,
          SnapshotOrdinal.MinValue,
          SnapshotOrdinal.MinValue,
          SnapshotOrdinal.MinValue,
          mptStore,
          mptOverlay,
          shardAcceptanceDeps,
          pendingAccumulatorsRef
        )
    } yield globalSnapshotConsensusFunction
  }

  def getTestData(
    implicit sp: SecurityProvider[F],
    j: JsonSerializer[F],
    h: Hasher[IO],
    m: Metrics[IO]
  ): IO[(GlobalSnapshotConsensusFunctions[IO], Set[PeerId], Signed[GlobalSnapshotArtifact], Signed[GlobalSnapshot], StateChannelEvent)] =
    for {
      keyPair <- KeyPairGenerator.makeKeyPair[F]

      gscf <- mkGlobalSnapshotConsensusFunctions()
      facilitators = Set.empty[PeerId]

      genesis = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue)
      signedGenesis <- Signed.forAsyncHasher[F, GlobalSnapshot](genesis, keyPair)

      lastArtifact <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis.value)
      signedLastArtifact <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact, keyPair)

      scEvent <- mkStateChannelEvent()
    } yield (gscf, facilitators, signedLastArtifact, signedGenesis, scEvent)

  test("validateArtifact - returns artifact for correct data") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      (gscf, facilitators, signedLastArtifact, signedGenesis, scEvent) <- getTestData

      (artifact, _, _) <- gscf.createProposalArtifact(
        SnapshotOrdinal.MinValue,
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        h,
        EventTrigger,
        Set(scEvent),
        facilitators,
        _ => None.pure[IO]
      )
      result <- gscf.validateArtifact(
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        EventTrigger,
        artifact,
        facilitators,
        _ => None.pure[IO]
      )

      expected = Right(NonEmptyList.one(scEvent.value.snapshotBinary))
      actual = result.map(_._1.stateChannelSnapshots(scEvent.value.address))
      expectation = expect.same(true, result.isRight) && expect.same(expected, actual)
    } yield expectation
  }

  test("validateArtifact - returns invalid artifact error for incorrect data") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      (gscf, facilitators, signedLastArtifact, signedGenesis, scEvent) <- getTestData

      (artifact, _, _) <- gscf.createProposalArtifact(
        SnapshotOrdinal.MinValue,
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        h,
        EventTrigger,
        Set(scEvent),
        facilitators,
        _ => None.pure[IO]
      )
      result <- gscf.validateArtifact(
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        EventTrigger,
        artifact.copy(ordinal = artifact.ordinal.next),
        facilitators,
        _ => None.pure[IO]
      )
    } yield expect.same(true, result.isLeft)
  }

  // ───────────────────────────────────────────────────────────────────────────────────────────────
  // R2 — follower-replay invariant (split-safety): `validateArtifact` always re-derives with
  // `sourceShardCheckpoints = false`. A follower whose shard deps are PRESENT (numShards > 1) but whose
  // local shard chain is EMPTY still validates a leader's artifact by REPLAYING the embedded
  // `stateChannelSnapshots` as ordinary SC events — it does NOT re-source node-local shard finality state.
  //
  // This is the entire reason the decoupled design is split-safe: followers reach byte-identical state
  // only by replaying the leader's embedded binaries, never by re-running their own shard sourcing on the
  // validate path. The invariant is statically guaranteed in source (`validateArtifact` → `usingJson` →
  // `createProposalArtifactInternal(..., sourceShardCheckpoints = false)`); this test exercises it through
  // a follower wired WITH active sharding deps so a regression that flipped the follower to source would
  // surface as a mismatch (the followers's empty-chain source would still be empty here, but the test pins
  // the contract that the validate path tolerates + ignores present sharding deps).
  // ───────────────────────────────────────────────────────────────────────────────────────────────
  test(
    "R2 follower-replay: validateArtifact (sourceShardCheckpoints=false) succeeds for a follower with active shard deps + EMPTY shard chain"
  ) { res =>
    implicit val (_, j, h, sp, m) = res

    val shardingCfg: ShardingConfig =
      ShardingConfig(
        numShards = 4,
        committeeKTarget = 4,
        finality = ShardFinalityConfig(k1Shard = 8L),
        checkpoint = ShardCheckpointConfig(tAliveMs = 10000L, tBurst = 100, binaryBufferCap = 4096),
        observability = ShardObservabilityConfig(tPartitionHardMs = 600000L),
        slashing = ShardSlashingConfig(maxMissedPctPerEpoch = 33, minDenominatorPerEpoch = 5L)
      )

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[F]
      selfId = PeerId.fromPublic(keyPair.getPublic)
      // Leader: NO sharding deps (numShards=1 regression-bar leader). It folds NO shard checkpoints, so
      // its artifact's stateChannelSnapshots come purely from the SC event we pass.
      leaderGscf <- mkGlobalSnapshotConsensusFunctions()
      // Follower: ACTIVE sharding deps (numShards=4) with a fresh, EMPTY per-shard chain registry.
      followerDeps <- io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
        .acceptanceDeps[IO](
          cfg = shardingCfg,
          selfPeerId = selfId,
          kesRegistry = io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry.empty[IO],
          vrfRegistry = io.constellationnetwork.node.shared.domain.nakamoto.VrfRegistry.empty[IO],
          activeValidators = IO.pure(Set(selfId)),
          etaForEpoch = (_: io.constellationnetwork.schema.nakamoto.EtaPeriod) => IO.pure(Array.fill[Byte](32)(0.toByte))
        )
      followerGscf <- mkGlobalSnapshotConsensusFunctions(followerDeps)

      facilitators = Set.empty[PeerId]
      genesis = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue)
      signedGenesis <- Signed.forAsyncHasher[F, GlobalSnapshot](genesis, keyPair)
      lastArtifact <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis.value)
      signedLastArtifact <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact, keyPair)
      scEvent <- mkStateChannelEvent()

      // Leader produces an artifact carrying the SC binary in `stateChannelSnapshots`.
      (artifact, _, _) <- leaderGscf.createProposalArtifact(
        SnapshotOrdinal.MinValue,
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        h,
        EventTrigger,
        Set(scEvent),
        facilitators,
        _ => None.pure[IO]
      )
      // Follower (active shard deps, empty shard chain) validates by REPLAYING the embedded SC binary —
      // `sourceShardCheckpoints = false` means it never consults its own shard chain on this path.
      result <- followerGscf.validateArtifact(
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        EventTrigger,
        artifact,
        facilitators,
        _ => None.pure[IO]
      )
      expected = Right(NonEmptyList.one(scEvent.value.snapshotBinary))
      actual = result.map(_._1.stateChannelSnapshots(scEvent.value.address))
    } yield expect.same(true, result.isRight) && expect.same(expected, actual)
  }

  test("gossip signed artifacts") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      gossiped <- Ref.of(List.empty[ForkInfo])
      mockGossip = mkMockGossip(gossiped)

      (signedLastArtifact, _) <- mkSignedArtifacts()

      _ <- SnapshotConsensusFunctions.gossipForkInfo(mockGossip, signedLastArtifact)

      expected <- h
        .hash(signedLastArtifact)
        .map { h =>
          List(ForkInfo(signedLastArtifact.value.ordinal, h))
        }
        .handleError(_ => List.empty)
      actual <- gossiped.get
    } yield expect.eql(expected, actual)
  }

  test("shouldUseDelegatedRewards - verifies reward selection logic based on ordinal and epoch thresholds") { res =>
    implicit val (_, j, h, sp, m) = res

    // Test the reward selection logic based on both conditions
    // This reproduces the logic in GlobalSnapshotConsensusFunctions
    def shouldUseDelegatedRewards(
      currentOrdinal: SnapshotOrdinal,
      currentEpochProgress: EpochProgress,
      v3MigrationOrdinal: SnapshotOrdinal,
      asOfEpoch: EpochProgress
    ): Boolean =
      currentOrdinal.value >= v3MigrationOrdinal.value &&
        currentEpochProgress.value.value >= asOfEpoch.value.value

    // Define test thresholds
    val migrationOrdinal = SnapshotOrdinal.unsafeApply(100L)
    val epochThreshold = EpochProgress(200L)

    // Define test cases - all combinations of before/after thresholds
    val beforeMigration = SnapshotOrdinal.unsafeApply(99L)
    val atMigration = SnapshotOrdinal.unsafeApply(100L)
    val afterMigration = SnapshotOrdinal.unsafeApply(101L)

    val beforeEpochThreshold = EpochProgress(199L)
    val atEpochThreshold = EpochProgress(200L)
    val afterEpochThreshold = EpochProgress(201L)

    // Determine which reward function would be called in each scenario
    val case1 = shouldUseDelegatedRewards(beforeMigration, beforeEpochThreshold, migrationOrdinal, epochThreshold)
    val case2 = shouldUseDelegatedRewards(afterMigration, beforeEpochThreshold, migrationOrdinal, epochThreshold)
    val case3 = shouldUseDelegatedRewards(beforeMigration, afterEpochThreshold, migrationOrdinal, epochThreshold)
    val case4 = shouldUseDelegatedRewards(afterMigration, afterEpochThreshold, migrationOrdinal, epochThreshold)

    // Edge cases at exact threshold values
    val atExactThresholds = shouldUseDelegatedRewards(atMigration, atEpochThreshold, migrationOrdinal, epochThreshold)

    // Check classic rewards selection (inverse of delegated)
    val useClassic1 = (beforeMigration.value < migrationOrdinal.value) ||
      (beforeEpochThreshold.value.value < epochThreshold.value.value)
    val useClassic4 = (afterMigration.value < migrationOrdinal.value) ||
      (afterEpochThreshold.value.value < epochThreshold.value.value)

    IO {
      // Case 1: Before migration, before epoch threshold - should NOT use delegated rewards
      expect(!case1) &&
      // Case 2: After migration, before epoch threshold - should NOT use delegated rewards
      expect(!case2) &&
      // Case 3: Before migration, after epoch threshold - should NOT use delegated rewards
      expect(!case3) &&
      // Case 4: After migration, after epoch threshold - should use delegated rewards
      expect(case4) &&
      // Exactly at thresholds - should use delegated rewards (inclusive thresholds)
      expect(atExactThresholds) &&
      // Verify classic rewards are used in cases 1-3 but not case 4
      expect(useClassic1) &&
      expect(!useClassic4)
    }
  }

//  ignore("rewards selection in GlobalSnapshotConsensusFunctions uses correct ordinal and epoch thresholds") { res =>
//    implicit val (_, ks, j, h, sp) = res
//
//    // Create a custom delegated rewards config provider with specific thresholds
//    val testEpochThreshold = EpochProgress(100L)
//    val testConfigProvider = new DelegatedRewardsConfigProvider {
//      def getConfig(): DelegatedRewardsConfig =
//        DelegatedRewardsConfig(
//          flatInflationRate = io.constellationnetwork.schema.NonNegFraction.unsafeFrom(0, 100),
//          emissionConfig = Map(
//            AppEnvironment.Dev -> EmissionConfigEntry(
//              epochsPerYear = eu.timepit.refined.types.numeric.PosLong(12L),
//              asOfEpoch = testEpochThreshold, // Set specific epoch threshold for testing
//              iTarget = io.constellationnetwork.schema.NonNegFraction.unsafeFrom(1, 100),
//              iInitial = io.constellationnetwork.schema.NonNegFraction.unsafeFrom(2, 100),
//              lambda = io.constellationnetwork.schema.NonNegFraction.unsafeFrom(1, 10),
//              iImpact = io.constellationnetwork.schema.NonNegFraction.unsafeFrom(5, 10),
//              totalSupply = Amount(100_00000000L),
//              dagPrices = SortedMap(
//                EpochProgress(100L) -> io.constellationnetwork.schema.NonNegFraction.unsafeFrom(10, 1)
//              )
//            )
//          ),
//          percentDistribution = Map.empty
//        )
//    }
//
//    // Create two implementations of Rewards to track which is called
//    var classicRewardsCalled = false
//    var delegatedRewardsCalled = false
//
//    val trackingClassicRewards: Rewards[F, GlobalSnapshotStateProof, GlobalIncrementalSnapshot, GlobalSnapshotEvent] =
//      (_, _, _, _, _, _) =>
//        IO {
//          classicRewardsCalled = true
//          SortedSet.empty
//        }
//
//    val trackingDelegatedRewards: DelegatedRewardsDistributor[F] = new DelegatedRewardsDistributor[F] {
//      def calculateTotalRewardsToMint(epochProgress: EpochProgress): F[Amount] = Amount(100L).pure[F]
//
//      def distribute(
//        lastSnapshotContext: GlobalSnapshotContext,
//        trigger: ConsensusTrigger,
//        epochProgress: EpochProgress,
//        facilitators: NonEmptySet[ID.Id],
//        delegatedStakeDiffs: UpdateDelegatedStakeAcceptanceResult,
//        partitionedRecords: PartitionedStakeUpdates
//      ): F[DelegationRewardsResult] = {
//        delegatedRewardsCalled = true
//        DelegationRewardsResult(
//          Map.empty,
//          SortedMap.empty,
//          SortedMap.empty,
//          SortedSet.empty,
//          SortedSet.empty,
//          Amount.empty
//        ).pure[F]
//      }
//    }
//
//    // Create a test-specific GlobalSnapshotConsensusFunctions with specific thresholds
//    def mkTestConsensusFunctions(migrationOrdinal: SnapshotOrdinal): GlobalSnapshotConsensusFunctions[IO] = {
//      implicit val hs = HasherSelector.forSyncAlwaysCurrent(h)
//
//      val spendActionValidator = SpendActionValidator.make[IO]
//
//      val snapshotAcceptanceManager: GlobalSnapshotAcceptanceManager[IO] =
//        GlobalSnapshotAcceptanceManager
//          .make[IO](
//            SnapshotOrdinal.MinValue,
//            bam,
//            asbam,
//            tlbam,
//            scProcessor,
//            updateNodeParametersAcceptanceManager,
//            updateDelegatedStakeAcceptanceManager,
//            updateNodeCollateralAcceptanceManager,
//            spendActionValidator,
//            collateral,
//            EpochProgress(NonNegLong(136080L))
//          )
//
//      val feeCalculator = new SnapshotBinaryFeeCalculator[IO] {
//        override def calculateFee(
//          event: StateChannelEvent,
//          info: GlobalSnapshotContext,
//          ordinal: SnapshotOrdinal
//        ): IO[NonNegLong] =
//          event.value.snapshotBinary.value.fee.value.pure[IO]
//      }
//
//      GlobalSnapshotConsensusFunctions
//        .make[IO](
//          snapshotAcceptanceManager,
//          collateral,
//          trackingClassicRewards,
//          trackingDelegatedRewards,
//          GlobalSnapshotEventCutter.make[IO](20_000_000, feeCalculator),
//          UpdateNodeParametersCutter.make(100),
//          AppEnvironment.Dev,
//          testConfigProvider,
//          migrationOrdinal // Use custom migration ordinal
//        )
//    }
//
//    // Setup test values
//    val migrationOrdinal = SnapshotOrdinal.unsafeApply(100L)
//    val gscf = mkTestConsensusFunctions(migrationOrdinal)
//
//    for {
//      // Create test artifacts for different scenarios
//      keyPair <- KeyPairGenerator.makeKeyPair[IO]
//      facilitators = Set.empty[PeerId]
//
//      // Scenario 1: Before migration ordinal, before epoch threshold
//      genesis1 = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress(50L))
//      signedGenesis1 <- Signed.forAsyncHasher[IO, GlobalSnapshot](genesis1, keyPair)
//      lastArtifact1 <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis1.value)
//      signedLastArtifact1 <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact1, keyPair)
//      // Ensure we're below the migration ordinal
//      beforeMigrationArtifact = signedLastArtifact1.copy(value = signedLastArtifact1.value.copy(ordinal = SnapshotOrdinal.unsafeApply(98L)))
//
//      // Scenario 2: After migration ordinal, after epoch threshold
//      genesis2 = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress(150L))
//      signedGenesis2 <- Signed.forAsyncHasher[IO, GlobalSnapshot](genesis2, keyPair)
//      lastArtifact2 <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis2.value)
//      signedLastArtifact2 <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact2, keyPair)
//      // Ensure we're above the migration ordinal
//      afterMigrationArtifact = signedLastArtifact2.copy(value = signedLastArtifact2.value.copy(ordinal = SnapshotOrdinal.unsafeApply(101L)))
//
//      // Create test event
//      scEvent <- mkStateChannelEvent()
//
//      // Reset tracking flags
//      _ = { classicRewardsCalled = false; delegatedRewardsCalled = false }
//
//      // Execute scenario 1 - Should use classic rewards
//      _ <- gscf.createProposalArtifact(
//        SnapshotOrdinal.MinValue,
//        beforeMigrationArtifact,
//        signedGenesis1.value.info,
//        h,
//        trigger.TimeTrigger,
//        Set(scEvent),
//        facilitators,
//        none,
//        _ => None.pure[IO]
//      )
//
//      classicCalledScenario1 = classicRewardsCalled
//      delegatedCalledScenario1 = delegatedRewardsCalled
//
//      // Reset tracking flags
//      _ = { classicRewardsCalled = false; delegatedRewardsCalled = false }
//
//      // Execute scenario 2 - Should use delegated rewards
//      _ <- gscf.createProposalArtifact(
//        SnapshotOrdinal.MinValue,
//        afterMigrationArtifact,
//        signedGenesis2.value.info,
//        h,
//        trigger.TimeTrigger,
//        Set(scEvent),
//        facilitators,
//        none,
//        _ => None.pure[IO]
//      )
//
//      classicCalledScenario2 = classicRewardsCalled
//      delegatedCalledScenario2 = delegatedRewardsCalled
//    } yield {
//      // Verify that the correct rewards function was called in each scenario
//      expect(classicCalledScenario1).description("Classic rewards should be called for before migration") &&
//        expect(!delegatedCalledScenario1).description("Delegated rewards should not be called for before migration") &&
//
//      expect(!classicCalledScenario2).description("Classic rewards should not be called for after migration and epoch") &&
//      expect(delegatedCalledScenario2).description("Delegated rewards should be called for after migration and epoch")
//    }
//  }

  test("createProposalArtifact - deterministic: two independent calls with same inputs produce identical artifacts") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]

      genesis = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue)
      signedGenesis <- Signed.forAsyncHasher[IO, GlobalSnapshot](genesis, keyPair)

      lastArtifact <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis.value)
      signedLastArtifact <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact, keyPair)

      scEvent <- mkStateChannelEvent()

      facilitators = Set.empty[PeerId]
      context = signedGenesis.value.info.toGlobalSnapshotInfo

      // Build two independent consensus function instances (each with its own MptStore)
      // to ensure no shared mutable state affects the output
      gscf1 <- mkGlobalSnapshotConsensusFunctions()
      gscf2 <- mkGlobalSnapshotConsensusFunctions()

      (artifact1, ctx1, _) <- gscf1.createProposalArtifact(
        SnapshotOrdinal.MinValue,
        signedLastArtifact,
        context,
        h,
        EventTrigger,
        Set(scEvent),
        facilitators,
        _ => None.pure[IO]
      )

      (artifact2, ctx2, _) <- gscf2.createProposalArtifact(
        SnapshotOrdinal.MinValue,
        signedLastArtifact,
        context,
        h,
        EventTrigger,
        Set(scEvent),
        facilitators,
        _ => None.pure[IO]
      )

      hash1 <- h.hash(artifact1)
      hash2 <- h.hash(artifact2)
    } yield
      expect.same(hash1, hash2) &&
        expect.same(artifact1.ordinal, artifact2.ordinal) &&
        expect.same(artifact1.stateProof, artifact2.stateProof)
  }

  def mkStateChannelEvent()(implicit S: SecurityProvider[IO], H: Hasher[IO]): IO[StateChannelEvent] = for {
    keyPair <- KeyPairGenerator.makeKeyPair[IO]
    binary = StateChannelSnapshotBinary(Hash.empty, "test".getBytes, SnapshotFee.MinValue)
    signedSC <- forAsyncHasher(binary, keyPair)
  } yield StateChannelEvent(StateChannelOutput(keyPair.getPublic.toAddress, signedSC))

}
