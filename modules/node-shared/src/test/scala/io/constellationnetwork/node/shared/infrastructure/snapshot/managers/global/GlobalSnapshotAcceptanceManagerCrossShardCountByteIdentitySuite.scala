package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import java.security.KeyPair

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
import io.constellationnetwork.node.shared.domain.statechannel._
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator.SpendActionValidationErrorOr
import io.constellationnetwork.node.shared.domain.swap.block._
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.snapshot.{CurrencySnapshotContextFunctions, DelegatedRewardsResult}
import io.constellationnetwork.node.shared.logger.Slf4jLoggerBundle
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{PricingUpdate, SpendAction}
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.nodeCollateral.UpdateNodeCollateral
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.swap.{AllowSpend, AllowSpendBlock}
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary, StateChannelValidationType}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import weaver.MutableIOSuite

/** TG-02 — the CROSS-SHARD-COUNT byte-identity gate (`numShards = 1` vs `numShards = K`).
  *
  * '''The gap this closes.''' Every pre-existing "numShards=1 byte-identity" test compares WITH-feature vs WITHOUT-feature at
  * `numShards = 1` (the regression bar that the sharding machinery is inert at the production default). None compares the STATE produced
  * at `numShards = 1` against the state produced at `numShards = K` for the SAME event stream — the headline non-regression guard for the
  * sharding hard fork: turning sharding ON must not change what state the cluster derives from the same metagraph binaries.
  *
  * '''What is compared.''' The SAME metagraph SC binaries are folded through [[GlobalSnapshotAcceptanceManager.accept]] twice, on two
  * independent manager instances (each with its own fresh MPT store):
  *
  *   - '''numShards = 1''' — the raw path: binaries arrive as ordinary `scEvents` and flow through the REAL chain-link
  *     (`GlobalSnapshotStateChannelAcceptanceManager.accept` / `onlyPossibleReferences`) + the REAL currency derivation
  *     (`processCurrencySnapshots`, `CurrencyAdoptionMode.Recreate` via `process`). `shardCheckpoints` is empty — no committees exist.
  *   - '''numShards = K (2 / 4)''' — the adopt path: the SAME binaries arrive inside mock quorum-accepted [[ShardCheckpoint]]s
  *     (`verifyEmbedded` stubbed `Accepted` — the committee-quorum decision is mocked; everything downstream of it is REAL), routed to
  *     the shard each MG's address statically maps to (`ShardAssignment.shardIdFor`). The SAME raw `scEvents` are ALSO passed so the
  *     CHANGE-3 filter (total static assignment ⇒ raw metagraph events excluded from the base path) is exercised — the input stream is
  *     genuinely identical, only the numShards wiring differs.
  *
  * '''The assertion''': byte-identical accepted `scSnapshots`, byte-identical [[GlobalSnapshotInfo]] (whole-object hash + targeted
  * per-field comparisons so a failure names the diverging field/partition), and — the headline — byte-identical
  * `stateProof` / `stateProof.mptRoot`.
  *
  * '''Scope (deliberate).''' Windows are currency GENESIS binaries (plus a non-decodable opaque child binary for the multi-binary-window
  * case). Both `CurrencyAdoptionMode`s decode a genesis full snapshot through the identical pure `deserialize[Signed[CurrencySnapshot]]`
  * branch, so the mode seam (`Recreate` at numShards=1 vs `AdoptFromSignedFields` on the adopt path) is NOT exercised for 2nd+
  * INCREMENTAL binaries here — that leg requires a real `CurrencySnapshotContextFunctions` (currency-l0 validator stack) which this
  * node-shared harness stubs as unreachable (same constraint as [[GlobalSnapshotAcceptanceManagerAdoptParitySuite]], whose processor
  * fixture this suite reuses). Incremental-window cross-count parity remains an open follow-up test.
  */
object GlobalSnapshotAcceptanceManagerCrossShardCountByteIdentitySuite extends MutableIOSuite {

  // MPT era ACTIVE (unlike the sibling GSAM suites' Long.MaxValue = LegacyFormat): the accept ordinal (2) is past the migration
  // boundary, so the state proof carries the SIGNED `mptRoot` — the observable this gate is about. Under LegacyFormat the proof
  // has NO mptRoot and a "mptRoot equality" assert would compare None == None (trivially green).
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal.MinValue)

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
    } yield (h, sp, j)

  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  // ============================================================================
  // Fixtures — mocks identical in shape to GlobalSnapshotAcceptanceManagerShardingSuite (inlined per the
  // established suite-self-containment pattern); the state-channel processor is REAL (AdoptParitySuite's).
  // ============================================================================

  private val mockBlockAcceptanceManager: BlockAcceptanceManager[IO] = new BlockAcceptanceManager[IO] {
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

  private val mockAllowSpendBlockAcceptanceManager: AllowSpendBlockAcceptanceManager[IO] = new AllowSpendBlockAcceptanceManager[IO] {
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

  private val mockTokenLockBlockAcceptanceManager: TokenLockBlockAcceptanceManager[IO] = new TokenLockBlockAcceptanceManager[IO] {
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

  private val mockUpdateNodeParametersAcceptanceManager: UpdateNodeParametersAcceptanceManager[IO] =
    new UpdateNodeParametersAcceptanceManager[IO] {
      override def acceptUpdateNodeParameters(
        events: List[Signed[io.constellationnetwork.schema.node.UpdateNodeParameters]],
        lastSnapshotContext: GlobalSnapshotInfo
      ): IO[UpdateNodeParametersAcceptanceResult] =
        UpdateNodeParametersAcceptanceResult(accepted = List.empty, notAccepted = List.empty).pure[IO]
    }

  private val mockUpdateNodeCollateralAcceptanceManager: UpdateNodeCollateralAcceptanceManager[IO] =
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

  private val mockSpendActionValidator: SpendActionValidator[IO] = new SpendActionValidator[IO] {
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

  private val mockPricingUpdateValidator: PricingUpdateValidator[IO] = new PricingUpdateValidator[IO] {
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

  private val mockPriceStateUpdater: PriceStateUpdater[IO] = new PriceStateUpdater[IO] {
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

  /** Stubbed committee-quorum decision: `verifyEmbedded` always `Accepted` (the mock quorum acceptance); everything downstream of the
    * decision — the chain-hole/anchor guard, the adopt merge, the currency derivation, the MPT fold — is REAL production code.
    */
  private final case class StubAcceptanceManager(
    callsRef: Ref[IO, List[ShardCheckpoint]]
  ) extends ShardCheckpointGl0AcceptanceManager[IO] {
    override def evaluate(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
      callsRef.update(_ :+ checkpoint).as(ShardCheckpointAcceptResult.Accepted)
    override def verifyEmbedded(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
      callsRef.update(_ :+ checkpoint).as(ShardCheckpointAcceptResult.Accepted)
    override def noteAdopted(shardId: ShardId, shardOrdinal: ShardOrdinal, checkpointHash: Hash): IO[Unit] = IO.unit
    override def lastAdoptedOrd(shardId: ShardId): IO[Option[ShardOrdinal]] = IO.pure(None)
    override def lastAdoptedAnchor(shardId: ShardId): IO[Option[Hash]] = IO.pure(None)
    override def watchtowerReExec(checkpoint: ShardCheckpoint): IO[List[WatchtowerMismatch]] = IO.pure(List.empty[WatchtowerMismatch])
  }

  /** REAL state-channel events processor — the exact fixture of [[GlobalSnapshotAcceptanceManagerAdoptParitySuite]]: real chain-link
    * `GlobalSnapshotStateChannelAcceptanceManager` (pull/purge delay 0), permissive `StateChannelValidator`, fee-free `FeeCalculator`,
    * and a no-op `CurrencySnapshotContextFunctions` (unreachable for genesis-window binaries — see the suite scaladoc's scope note).
    */
  private def mkRealProcessor(
    implicit h: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[GlobalSnapshotStateChannelEventsProcessor[IO]] = {
    val validator = new StateChannelValidator[IO] {
      def validate(
        output: StateChannelOutput,
        globalOrdinal: SnapshotOrdinal,
        snapshotFeesInfo: SnapshotFeesInfo
      )(implicit hasher: Hasher[IO]) = IO.pure(output.validNec)
      def validateHistorical(
        output: StateChannelOutput,
        globalOrdinal: SnapshotOrdinal,
        snapshotFeesInfo: SnapshotFeesInfo
      )(implicit hasher: Hasher[IO]) = IO.pure(output.validNec)
    }

    val noopContextFns: CurrencySnapshotContextFunctions[IO] = new CurrencySnapshotContextFunctions[IO] {
      def createContext(
        context: CurrencySnapshotContext,
        lastArtifact: Signed[CurrencyIncrementalSnapshot],
        signedArtifact: Signed[CurrencyIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[CurrencySnapshotContext] =
        IO.raiseError(new RuntimeException("noop CurrencySnapshotContextFunctions should not be reached for genesis-window binaries"))
    }

    for {
      manager <- GlobalSnapshotStateChannelAcceptanceManager
        .make[IO](None, pullDelay = NonNegLong.MinValue, purgeDelay = NonNegLong.MinValue)
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      reader = io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore(mptStore)
      feeCalculator = FeeCalculator.make[IO](SortedMap.empty)
      processor = GlobalSnapshotStateChannelEventsProcessor.make[IO](validator, manager, noopContextFns, feeCalculator, reader)
    } yield processor
  }

  private def mkShardingConfig(numShards: Int): ShardingConfig =
    ShardingConfig(
      numShards = numShards,
      finality = ShardFinalityConfig(k1Shard = 8L),
      checkpoint = ShardCheckpointConfig(tAliveMs = 10000L, tBurst = 100, binaryBufferCap = 4096),
      observability = ShardObservabilityConfig(tPartitionHardMs = 600000L),
      slashing = ShardSlashingConfig(maxMissedPctPerEpoch = 33, minDenominatorPerEpoch = 5L)
    )

  /** One independent "node": a fresh GSAM over a fresh MPT store + a fresh REAL processor. Returns the manager and the committee-stub
    * call recorder (so tests can assert the adopt path actually ran at numShards>1).
    */
  private def mkManager(
    numShards: Int
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], j: JsonSerializer[IO]): IO[(GlobalSnapshotAcceptanceManager[IO], Ref[IO, List[ShardCheckpoint]])] = {
    implicit val hasherSelector: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    val updateDelegatedStakeValidator =
      UpdateDelegatedStakeValidator.make[IO](io.constellationnetwork.security.signature.SignedValidator.make[IO], None)
    val updateDelegatedStakeAcceptanceManager = UpdateDelegatedStakeAcceptanceManager.make[IO](updateDelegatedStakeValidator)

    for {
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      stubMgr = StubAcceptanceManager(checkpointCallsRef)
      processor <- mkRealProcessor
      loggerBundle <- Slf4jLoggerBundle.makeUnsafe[IO]
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
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
          stateChannelEventsProcessor = processor,
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
          shardingConfig = Some(mkShardingConfig(numShards)),
          shardCheckpointAcceptanceManager = Some(stubMgr),
          shardAssignment = Some(ShardAssignment.make[IO](numShards = numShards)),
          etaRotationSnapshots = 2550L,
          invaliditySlashingConfig = InvalidStateProofSlashingConfig(
            watchtowerEnabled = true,
            slashFraction = io.constellationnetwork.numerics.Ratio.One,
            bountyFraction = io.constellationnetwork.numerics.Ratio(1, 20),
            cooldownEpochs = 100L
          )
        )
    } yield (mgr, checkpointCallsRef)
  }

  /** Minimal `GlobalSnapshotInfo` prior — same shape as the sibling GSAM suites. */
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

  // ============================================================================
  // Binary fixtures — REAL signed currency genesis binaries (the AdoptParitySuite /
  // ShardCommitteeReExecutionSuite shape) so the currency pipeline derives non-trivial state.
  // ============================================================================

  private def mkCurrencyGenesisBinary(
    mgKeyPair: KeyPair
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], j: JsonSerializer[IO]): IO[Signed[StateChannelSnapshotBinary]] = {
    val genesis: CurrencySnapshot = CurrencySnapshot.mkGenesis(Map.empty, None, None)
    for {
      signedGenesis <- forAsyncHasher(genesis, mgKeyPair)
      contentBytes <- JsonSerializer[IO].serialize(signedGenesis)
      binary = StateChannelSnapshotBinary(Hash.empty, contentBytes, SnapshotFee.MinValue)
      signedBinary <- forAsyncHasher(binary, mgKeyPair)
    } yield signedBinary
  }

  /** A chain-linked CHILD binary whose content is valid JSON but NOT a currency snapshot (the fee-free stateless-accept branch — mode
    * independent). `parentHash` must be the hash of the parent binary's UNSIGNED value (`Signed.toHashed` hashes `signed.value` — that is
    * what the real chain-link's `onlyPossibleReferences` links on).
    */
  private def mkOpaqueChildBinary(
    mgKeyPair: KeyPair,
    parentHash: Hash
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], j: JsonSerializer[IO]): IO[Signed[StateChannelSnapshotBinary]] =
    for {
      contentBytes <- JsonSerializer[IO].serialize("cross-count-opaque-child")
      binary = StateChannelSnapshotBinary(parentHash, contentBytes, SnapshotFee.MinValue)
      signedBinary <- forAsyncHasher(binary, mgKeyPair)
    } yield signedBinary

  private val genesisHash: Hash = Hash("0" * 64)
  private val epochZero: EtaPeriod = EtaPeriod(0L)

  /** Minimal quorum-accepted checkpoint shell carrying the given per-MG windows. Pure-genesis-window shape: NO per-MG diff and NO
    * attested root (the `(diff, root).tupled` STEP-6 pair is absent ⇒ the adopt path keeps the binary-decoded state verbatim — exactly
    * the production behavior for a genesis window, per `adoptShardCheckpoints`). The committee signature is a placeholder — the stubbed
    * `verifyEmbedded` (the mocked quorum decision) never inspects it.
    */
  private def mkCheckpoint(
    shardId: ShardId,
    windows: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
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
      shardOrdinal = ShardOrdinal(1L),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(2L)),
      slot = SlotT.unsafeApply(2L),
      derivedStateDelta = ShardDerivedStateDelta(
        perMetagraphMptRoots = SortedMap.empty,
        perMetagraphStateDiff = SortedMap.empty,
        includedSnapshots = windows,
        tokenLockBalancesDelta = SortedMap.empty,
        perMetagraphArtifacts = SortedMap.empty,
        perMetagraphSyncDataDelta = SortedMap.empty
      ),
      emittedReceipts = List.empty,
      committeeSignatures = NonEmptyList.of(placeholderSig),
      epoch = epochZero
    )
  }

  /** Group the per-MG windows into one checkpoint per statically-assigned shard — the same deterministic `shardIdFor` routing production
    * uses, so each MG's binaries arrive from the shard that would actually have produced them.
    */
  private def mkCheckpointsByAssignment(
    numShards: Int,
    windows: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
  )(implicit h: Hasher[IO]): IO[SortedMap[ShardId, ShardCheckpoint]] = {
    val assignment = ShardAssignment.make[IO](numShards = numShards)
    windows.toList
      .traverse { case (mg, nel) => assignment.shardIdFor(mg).map(sid => (sid, mg, nel)) }
      .map { routed =>
        val byShard = routed.groupBy(_._1)
        SortedMap.from(byShard.map {
          case (sid, entries) =>
            sid -> mkCheckpoint(sid, SortedMap.from(entries.map { case (_, mg, nel) => mg -> nel })(Address.OrderingInstance))
        })
      }
  }

  /** The three byte-identity observables: accepted `scSnapshots`, the derived `GlobalSnapshotInfo`, and the `GlobalSnapshotStateProof`. */
  private def invokeAccept(
    mgr: GlobalSnapshotAcceptanceManager[IO],
    scEvents: List[StateChannelOutput],
    shardCheckpoints: SortedMap[ShardId, ShardCheckpoint]
  ): IO[(SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]], GlobalSnapshotInfo, GlobalSnapshotStateProof)] =
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
        lastSnapshotContext = emptyGsi,
        lastActiveTips = SortedSet.empty,
        lastDeprecatedTips = SortedSet.empty,
        calculateRewardsFn = noopRewardsFn,
        validationType = StateChannelValidationType.Full,
        getGlobalSnapshotByOrdinal = _ => None.pure[IO],
        parentTip = io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId.passthrough,
        shardCheckpoints = shardCheckpoints
      )
      .map(result => (result._6, result._9, result._10))

  /** Structural (content-based) view of an accepted scSnapshots map — avoids `Array[Byte]` reference equality. */
  private def scView(
    sc: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
  ): SortedMap[Address, List[(Hash, Seq[Byte])]] =
    sc.map { case (mg, nel) => mg -> nel.toList.map(b => (b.value.lastSnapshotHash, b.value.content.toSeq)) }

  /** Run the SAME window set at `numShards = 1` (raw scEvents, no checkpoints) and at `numShards = k` (same raw scEvents + the
    * quorum-accepted checkpoints carrying the SAME binaries), then return both observable triples + the K-side committee call count.
    */
  private def runOneVsK(
    k: Int,
    windows: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], j: JsonSerializer[IO]): IO[
    (
      (SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]], GlobalSnapshotInfo, GlobalSnapshotStateProof),
      (SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]], GlobalSnapshotInfo, GlobalSnapshotStateProof),
      Int,
      Int
    )
  ] = {
    // The identical raw event stream: every binary as an ordinary SC event, oldest-first per MG.
    val rawEvents: List[StateChannelOutput] =
      windows.toList.flatMap { case (mg, nel) => nel.toList.map(StateChannelOutput(mg, _)) }
    for {
      pairOne <- mkManager(numShards = 1)
      (mgrOne, callsOneRef) = pairOne
      pairK <- mkManager(numShards = k)
      (mgrK, callsKRef) = pairK
      checkpoints <- mkCheckpointsByAssignment(k, windows)
      one <- invokeAccept(mgrOne, scEvents = rawEvents, shardCheckpoints = SortedMap.empty)
      kres <- invokeAccept(mgrK, scEvents = rawEvents, shardCheckpoints = checkpoints)
      callsOne <- callsOneRef.get
      callsK <- callsKRef.get
    } yield (one, kres, callsK.size, callsOne.size)
  }

  /** The shared assertion block: labeled per-field comparisons so a RED names the diverging field/partition directly. */
  private def expectByteIdentical(
    expectedMgs: Set[Address],
    one: (SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]], GlobalSnapshotInfo, GlobalSnapshotStateProof),
    k: (SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]], GlobalSnapshotInfo, GlobalSnapshotStateProof),
    committeeCallsAtK: Int,
    committeeCallsAtOne: Int,
    expectedCheckpointCount: Int,
    gsiHashOne: Hash,
    gsiHashK: Hash
  ) = {
    val (scOne, gsiOne, spOne) = one
    val (scK, gsiK, spK) = k
    expect.all(
      // Non-triviality: both paths accepted the SAME non-empty windows and derived currency state for every MG.
      scOne.nonEmpty,
      scView(scOne).keySet == expectedMgs,
      gsiOne.lastCurrencySnapshots.keySet == expectedMgs,
      // Path routing: the numShards=1 gate never consulted the committee stub; the K-side verified every checkpoint
      // through the (stubbed) quorum verifier — i.e. the K-side state genuinely came via the adopt path.
      committeeCallsAtOne == 0,
      committeeCallsAtK == expectedCheckpointCount,
      // Accepted SC windows byte-identical (content-based view).
      scView(scOne) == scView(scK),
      // Per-field GSI observables (diagnostic labels for a RED):
      gsiOne.lastStateChannelSnapshotHashes == gsiK.lastStateChannelSnapshotHashes, // SC-tip partition
      gsiOne.lastCurrencySnapshots.keySet == gsiK.lastCurrencySnapshots.keySet, // currency-state partition keys
      gsiOne.balances == gsiK.balances, // balances partition
      gsiOne.activeAllowSpends == gsiK.activeAllowSpends, // allow-spend partition
      gsiOne.activeTokenLocks == gsiK.activeTokenLocks, // token-lock partition
      gsiOne.tokenLockBalances == gsiK.tokenLockBalances, // token-lock balances partition
      gsiOne.lastTxRefs == gsiK.lastTxRefs, // tx-refs partition
      // Whole-GSI byte identity via canonical hash (catches any field not singled out above).
      gsiHashOne === gsiHashK,
      // THE DoD GATE — the signed state proof, and specifically the global mptRoot, is byte-identical 1-vs-K.
      spOne.mptRoot.isDefined,
      spOne.mptRoot === spK.mptRoot,
      spOne === spK
    )
  }

  // ============================================================================
  // TG-02 Test 1 — K=2, two MGs, single-genesis windows
  // ============================================================================

  test("TG-02 cross-count byte-identity: numShards=1 (raw/re-exec) == numShards=2 (committee adopt) — genesis windows, 2 MGs") { res =>
    implicit val (h, sp, j) = res
    for {
      mgKeyPairA <- KeyPairGenerator.makeKeyPair[IO]
      mgKeyPairB <- KeyPairGenerator.makeKeyPair[IO]
      mgA = PublicKeyOps(mgKeyPairA.getPublic).toAddress
      mgB = PublicKeyOps(mgKeyPairB.getPublic).toAddress
      binaryA <- mkCurrencyGenesisBinary(mgKeyPairA)
      binaryB <- mkCurrencyGenesisBinary(mgKeyPairB)
      windows = SortedMap(
        mgA -> NonEmptyList.of(binaryA),
        mgB -> NonEmptyList.of(binaryB)
      )(Address.OrderingInstance)
      checkpoints <- mkCheckpointsByAssignment(2, windows)
      out <- runOneVsK(2, windows)
      (one, kres, committeeCallsAtK, committeeCallsAtOne) = out
      gsiHashOne <- h.hash(one._2)
      gsiHashK <- h.hash(kres._2)
    } yield
      expectByteIdentical(
        Set(mgA, mgB),
        one,
        kres,
        committeeCallsAtK,
        committeeCallsAtOne,
        expectedCheckpointCount = checkpoints.size,
        gsiHashOne,
        gsiHashK
      )
  }

  // ============================================================================
  // TG-02 Test 2 — K=4, two MGs, one MULTI-BINARY window (genesis + chain-linked opaque child)
  // ============================================================================

  test("TG-02 cross-count byte-identity: numShards=1 == numShards=4 — multi-binary window (genesis + chained child)") { res =>
    implicit val (h, sp, j) = res
    for {
      mgKeyPairA <- KeyPairGenerator.makeKeyPair[IO]
      mgKeyPairB <- KeyPairGenerator.makeKeyPair[IO]
      mgA = PublicKeyOps(mgKeyPairA.getPublic).toAddress
      mgB = PublicKeyOps(mgKeyPairB.getPublic).toAddress
      genesisA <- mkCurrencyGenesisBinary(mgKeyPairA)
      // The chain-link reference is the hash of the parent binary's UNSIGNED value (Signed.toHashed semantics).
      genesisAValueHash <- h.hash(genesisA.value)
      childA <- mkOpaqueChildBinary(mgKeyPairA, genesisAValueHash)
      genesisB <- mkCurrencyGenesisBinary(mgKeyPairB)
      windows = SortedMap(
        mgA -> NonEmptyList.of(genesisA, childA), // oldest-first, the checkpoint window convention
        mgB -> NonEmptyList.of(genesisB)
      )(Address.OrderingInstance)
      checkpoints <- mkCheckpointsByAssignment(4, windows)
      out <- runOneVsK(4, windows)
      (one, kres, committeeCallsAtK, committeeCallsAtOne) = out
      gsiHashOne <- h.hash(one._2)
      gsiHashK <- h.hash(kres._2)
      // Window-shape sanity: the multi-binary MG really carried BOTH binaries through on the 1-side (chain-link unfolded the child).
      multiWindowLenOne = one._1.get(mgA).map(_.size).getOrElse(0)
    } yield
      expect(multiWindowLenOne == 2) and
        expectByteIdentical(
          Set(mgA, mgB),
          one,
          kres,
          committeeCallsAtK,
          committeeCallsAtOne,
          expectedCheckpointCount = checkpoints.size,
          gsiHashOne,
          gsiHashK
        )
  }
}
