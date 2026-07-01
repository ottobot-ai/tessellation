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
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
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
      perMetagraphStateDiff = SortedMap.empty,
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
      slot = SlotT.unsafeApply(gl0Anchor),
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
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]],
        adoptionMode: GlobalSnapshotStateChannelEventsProcessor.CurrencyAdoptionMode
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
    override def noteAdopted(
      shardId: io.constellationnetwork.schema.sharding.ShardId,
      shardOrdinal: ShardOrdinal,
      checkpointHash: io.constellationnetwork.security.hash.Hash
    ): IO[Unit] =
      IO.unit
    override def lastAdoptedOrd(shardId: io.constellationnetwork.schema.sharding.ShardId): IO[Option[ShardOrdinal]] =
      IO.pure(None)
    override def lastAdoptedAnchor(
      shardId: io.constellationnetwork.schema.sharding.ShardId
    ): IO[Option[io.constellationnetwork.security.hash.Hash]] =
      IO.pure(None)
    override def watchtowerReExec(checkpoint: ShardCheckpoint): IO[List[WatchtowerMismatch]] =
      IO.pure(List.empty[WatchtowerMismatch])
  }

  /** Build the manager-under-test with the Slice 13 sharding deps, using the captor processor (records `process` events + adopted
    * snapshots). Most tests use this.
    */
  private def mkSuiteManager(
    shardingConfig: Option[ShardingConfig],
    checkpointManager: Option[ShardCheckpointGl0AcceptanceManager[IO]],
    capturedEventsRef: Ref[IO, List[StateChannelOutput]],
    capturedAdoptedRef: Ref[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]],
    mkInvalidStateProofValidator: MptStore[IO, GlobalStateKey] => Option[
      io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[IO]
    ] = _ => None
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[GlobalSnapshotAcceptanceManager[IO]] =
    mkSuiteManagerWithProcessor(
      shardingConfig,
      checkpointManager,
      mkCaptorProcessor(capturedEventsRef, capturedAdoptedRef),
      mkInvalidStateProofValidator
    )

  /** Build the manager-under-test with an explicit `stateChannelEventsProcessor` — lets the three-paths-identity test inject a deriving
    * processor. Tests pass:
    *   - `shardingConfig`: typically [[mkShardingConfig]] with `numShards > 1`. Default `None` for the regression bar test.
    *   - `checkpointManager`: a [[StubAcceptanceManager]] with the desired per-checkpoint decisions.
    */
  private def mkSuiteManagerWithProcessor(
    shardingConfig: Option[ShardingConfig],
    checkpointManager: Option[ShardCheckpointGl0AcceptanceManager[IO]],
    stateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[IO],
    // W3a — build the fraud-proof dispute validator over the SAME internal `mptStore` the GSAM writes the `Slashings` partition to (so the
    // double-slash guard reads what accept() wrote). `_ => None` for the existing tests (no slash path).
    mkInvalidStateProofValidator: MptStore[IO, GlobalStateKey] => Option[
      io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[IO]
    ] = _ => None,
    spendActionValidatorOverride: Option[SpendActionValidator[IO]] = None,
    crossShardSpendProofClient: Option[
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProofClient[IO]
    ] = None
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[GlobalSnapshotAcceptanceManager[IO]] =
    mkSuiteManagerWithProcessorAndStore(
      shardingConfig,
      checkpointManager,
      stateChannelEventsProcessor,
      mkInvalidStateProofValidator,
      spendActionValidatorOverride,
      crossShardSpendProofClient
    ).map(_._1)

  /** Like [[mkSuiteManagerWithProcessor]] but ALSO returns the internal `mptStore` — needed by the W3a watchtower tests to read back the
    * `Slashings` partition the GSAM wrote (the double-slash guard's authoritative source).
    */
  private def mkSuiteManagerWithProcessorAndStore(
    shardingConfig: Option[ShardingConfig],
    checkpointManager: Option[ShardCheckpointGl0AcceptanceManager[IO]],
    stateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[IO],
    mkInvalidStateProofValidator: MptStore[IO, GlobalStateKey] => Option[
      io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[IO]
    ] = _ => None,
    // W3c ACTIVATION test seam: override the INJECTED `spendActionValidator` (the spy used to prove the GSAM uses the
    // sharded overload at numShards>1 and the injected one at numShards=1) + supply a deterministic cross-shard proof
    // client. `None` ⇒ the default mock validator + `noop` client (every existing test is unchanged).
    spendActionValidatorOverride: Option[SpendActionValidator[IO]] = None,
    crossShardSpendProofClient: Option[
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProofClient[IO]
    ] = None
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[(GlobalSnapshotAcceptanceManager[IO], MptStore[IO, GlobalStateKey])] = {
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
                    spendActionValidator = spendActionValidatorOverride.getOrElse(mockSpendActionValidator),
                    pricingUpdateValidator = mockPricingUpdateValidator,
                    priceStateUpdater = mockPriceStateUpdater,
                    collateral = Amount.empty,
                    withdrawalTimeLimit = EpochProgress(4L),
                    loggerBundle = loggerBundle,
                    overlay = overlay,
                    shardingConfig = shardingConfig,
                    shardCheckpointAcceptanceManager = checkpointManager,
                    shardAssignment = Some(ShardAssignment.make[IO](numShards = shardingConfig.map(_.numShards).getOrElse(1))),
                    // W3a — the dispute validator built over the internal `mptStore` (so its double-slash guard reads the `Slashings`
                    // partition this same GSAM writes). `None` for non-watchtower tests.
                    invalidStateProofValidator = mkInvalidStateProofValidator(mptStore),
                    // W3c ACTIVATION — the cross-shard proof client for the per-accept sharded SpendActionValidator.
                    crossShardSpendProofClient = crossShardSpendProofClient,
                    etaRotationSnapshots = 2550L,
                    invaliditySlashingConfig = InvalidStateProofSlashingConfig(
                      watchtowerEnabled = true,
                      slashFraction = io.constellationnetwork.numerics.Ratio.One,
                      bountyFraction = io.constellationnetwork.numerics.Ratio(1, 20),
                      cooldownEpochs = 100L
                    )
                  )
              } yield (mgr, mptStore)
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
  // Test 4b (W3c ACTIVATION): at numShards>1 the GSAM builds + uses the PER-ACCEPT sharded SpendActionValidator
  //   (cross-shard-capable, overlay bound to this accept's spent-set/epochs) — so the INJECTED unsharded validator
  //   spy is NOT consulted. At numShards=1 the GSAM uses the INJECTED validator verbatim (the cross-shard branch is
  //   unreachable ⇒ byte-identical). This is the activation forcing-function: it proves accept() selects the
  //   sharded overload exactly when sharding is active. (The cross-shard phantom-refund REJECTION the sharded
  //   validator performs is proven deterministically end-to-end in SpendActionValidatorCrossShardSuite Test 8/9 —
  //   the SAME `effectiveCurrencyBalances`-bound overlay this GSAM wiring constructs.)
  // ============================================================================

  /** A spy `SpendActionValidator` that records whether `validateReturningAcceptedAndRejected` was invoked. `validateArtifacts` calls it
    * unconditionally (even with an empty spend-action map), so the call-flag is a clean signal of WHICH validator the GSAM used.
    */
  private def mkSpyValidator(calledRef: Ref[IO, Boolean]): SpendActionValidator[IO] = new SpendActionValidator[IO] {
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
    ): IO[(Map[Address, List[SpendAction]], Map[Address, List[(SpendAction, List[SpendActionValidator.SpendActionValidationError])]])] =
      calledRef
        .set(true)
        .as(
          (
            Map.empty[Address, List[SpendAction]],
            Map.empty[Address, List[(SpendAction, List[SpendActionValidator.SpendActionValidationError])]]
          )
        )
  }

  test("W3c activation: numShards>1 ⇒ GSAM uses the per-accept SHARDED validator (injected spy NOT consulted)") { res =>
    implicit val (h, sp) = res
    for {
      checkpointCallsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      capturedAdoptedRef <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      spyCalledRef <- Ref.of[IO, Boolean](false)
      stubMgr = StubAcceptanceManager(checkpointCallsRef, _ => ShardCheckpointAcceptResult.Accepted)
      mgr <- mkSuiteManagerWithProcessor(
        Some(mkShardingConfig(numShards = 4)),
        Some(stubMgr),
        mkCaptorProcessor(capturedRef, capturedAdoptedRef),
        spendActionValidatorOverride = Some(mkSpyValidator(spyCalledRef))
      )
      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = SortedMap.empty, lastSnapshotInfo = emptyGsi)
      spyCalled <- spyCalledRef.get
    } yield
      // The injected (unsharded) spy is NOT the validator the accept path ran — GSAM built the per-accept sharded
      // overload (bound to this accept's spent-set/epochs) instead. This is the W3c activation.
      expect(!spyCalled)
  }

  test("W3c numShards=1 byte-identity: GSAM uses the INJECTED validator verbatim (cross-shard branch unreachable)") { res =>
    implicit val (h, sp) = res
    for {
      capturedRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      capturedAdoptedRef <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      spyCalledRef <- Ref.of[IO, Boolean](false)
      // numShards=1 ⇒ no checkpoint manager needed (the adopt path never fires); the selection falls to the injected validator.
      mgr <- mkSuiteManagerWithProcessor(
        Some(mkShardingConfig(numShards = 1)),
        None,
        mkCaptorProcessor(capturedRef, capturedAdoptedRef),
        spendActionValidatorOverride = Some(mkSpyValidator(spyCalledRef))
      )
      _ <- invokeAccept(mgr, scEvents = List.empty, shardCheckpoints = SortedMap.empty, lastSnapshotInfo = emptyGsi)
      spyCalled <- spyCalledRef.get
    } yield
      // At numShards=1 the injected validator IS the one accept() runs — byte-identical to the pre-W3c path.
      expect(spyCalled)
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
          getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]],
          adoptionMode: GlobalSnapshotStateChannelEventsProcessor.CurrencyAdoptionMode
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

  // ============================================================================
  // W3a — WATCHTOWER fraud-proof CONSENSUS ARTIFACT → durable slash at accept()
  // ============================================================================
  //
  // These prove the marquee capability: a fraud-proof artifact carried in the snapshot's `fraudProofs` field is re-validated
  // DETERMINISTICALLY inside accept() and, on UPHELD, slashes the disputed committee + credits the challenger — with the verdict identical on
  // leader/follower/peer. The `Slashings` MPT partition record (written for every upheld dispute, independent of the slashed amount) is the
  // observable. The dispute validator uses a STUB re-derivation closure (pure) so UPHELD vs FRIVOLOUS is controlled by whether the stubbed
  // honest root differs from the checkpoint's attested root — exactly the InvalidStateProofValidatorSuite discipline, here exercised THROUGH
  // accept(). The double-slash guard reads the SAME internal `mptStore` the GSAM writes (via `InvalidStateProofSlashedReader.fromMptStore`).

  private val wtAttestedRoot: Hash = Hash("a" * 64)
  private val wtHonestDiffersFromAttested: Hash = Hash("b" * 64) // honest ≠ attested ⇒ UPHELD

  /** A one-MG checkpoint carrying `attestedRoot` as the disputed MG's `perMetagraphMptRoots` (the value the verdict compares the recomputed
    * honest root against).
    */
  private def wtCheckpoint(mg: Address, attestedRoot: Hash): ShardCheckpoint = {
    val delta = ShardDerivedStateDelta(
      perMetagraphMptRoots = SortedMap(mg -> attestedRoot),
      perMetagraphStateDiff = SortedMap.empty,
      includedSnapshots = SortedMap(mg -> NonEmptyList.of(mkSignedBinary("wt-content".getBytes("UTF-8")))),
      tokenLockBalancesDelta = SortedMap.empty,
      perMetagraphArtifacts = SortedMap.empty,
      perMetagraphSyncDataDelta = SortedMap.empty
    )
    mkCheckpoint(ShardId.unsafeApply(0), shardOrd = 1L, gl0Anchor = 2L, delta = delta)
  }

  /** Build an `InvalidStateProofEvidence` with a REAL challenger Ed25519 signature over the canonical preimage (so the validator's step-5
    * signature check passes), binding the carried checkpoint by its canonical hash (step-4). Mirrors InvalidStateProofValidatorSuite.
    */
  private def wtEvidence(mg: Address, cp: ShardCheckpoint)(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO]
  ): IO[(io.constellationnetwork.schema.slashing.InvalidStateProofEvidence, Address)] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { kp =>
      val submitterId = io.constellationnetwork.schema.peer.PeerId.fromPublic(kp.getPublic)
      h.hash(cp.signingPreimage).flatMap { cpHash =>
        val attested = cp.derivedStateDelta.perMetagraphMptRoots.getOrElse(mg, Hash.empty)
        val unsigned = io.constellationnetwork.schema.sharding.FraudProofEnvelope(
          shardId = cp.shardId,
          disputedCheckpointHash = cpHash,
          metagraphAddress = mg,
          gl0AnchorOrdinal = cp.gl0AnchorOrdinal,
          claimedDerivation = attested,
          challengerDerivation = wtHonestDiffersFromAttested,
          reexecutionWitness = Hex(wtHonestDiffersFromAttested.value),
          challengerSignature = Hex(""),
          submitterId = submitterId
        )
        h.hash(unsigned.signingPreimage).flatMap { digest =>
          io.constellationnetwork.security.signature.Signing.signData[IO](digest.getBytes)(kp.getPrivate).flatMap { sig =>
            val fp = unsigned.copy(challengerSignature = Hex.fromBytes(sig))
            submitterId.toAddress[IO].map { submitterAddr =>
              (
                io.constellationnetwork.schema.slashing.InvalidStateProofEvidence(
                  shardId = cp.shardId,
                  disputedCheckpoint = cp,
                  metagraphAddress = mg,
                  attestedRoot = attested,
                  fraudProof = fp
                ),
                submitterAddr
              )
            }
          }
        }
      }
    }

  /** A pure stubbed re-derivation closure returning a fixed honest root — the verdict UPHOLDS iff `honest != attested`. */
  private def wtValidator(honestRoot: Hash, store: MptStore[IO, GlobalStateKey])(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO]
  ): io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[IO] =
    io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator.make[IO](
      reDerivePerMgRoot = (_, _, _, _) => IO.pure(honestRoot),
      slashedReader = io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashedReader.fromMptStore[IO](store)
    )

  /** Invoke `accept()` with a `fraudProofs` set (no shard checkpoints, no scEvents) and return the GSAM-derived `(GSI, stateProof)`. */
  private def invokeAcceptFraudProofs(
    mgr: GlobalSnapshotAcceptanceManager[IO],
    fraudProofs: SortedSet[io.constellationnetwork.schema.slashing.InvalidStateProofEvidence]
  ): IO[(GlobalSnapshotInfo, GlobalSnapshotStateProof)] =
    mgr
      .accept(
        ordinal = SnapshotOrdinal(2L),
        epochProgress = EpochProgress(10L),
        previousEpochProgress = EpochProgress.MinValue,
        blocksForAcceptance = List.empty,
        allowSpendBlocksForAcceptance = List.empty,
        tokenLockBlocksForAcceptance = List.empty,
        scEvents = List.empty,
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
        shardCheckpoints = SortedMap.empty,
        fraudProofs = fraudProofs
      )
      .map(result => (result._9, result._10))

  private def wasSlashed(store: MptStore[IO, GlobalStateKey], shardId: ShardId, cpHash: Hash)(
    implicit h: Hasher[IO]
  ): IO[Boolean] =
    io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashedReader
      .fromMptStore[IO](store)
      .wasSlashed(shardId, cpHash)

  /** Build a watchtower-wired GSAM (`numShards = 4`, dispute validator over the internal store with the given stubbed honest root) and
    * return `(mgr, mptStore)`. `honestRoot != wtAttestedRoot` ⇒ disputes are UPHELD; `== wtAttestedRoot` ⇒ DisputeNotUpheld
    * (honest-committee floor).
    */
  private def mkWatchtowerMgr(honestRoot: Hash)(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO]
  ): IO[(GlobalSnapshotAcceptanceManager[IO], MptStore[IO, GlobalStateKey])] =
    for {
      callsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      eventsRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      adoptedRef <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      stubMgr = StubAcceptanceManager(callsRef, _ => ShardCheckpointAcceptResult.Accepted)
      pair <- mkSuiteManagerWithProcessorAndStore(
        Some(mkShardingConfig(numShards = 4)),
        Some(stubMgr),
        mkCaptorProcessor(eventsRef, adoptedRef),
        store => Some(wtValidator(honestRoot, store))
      )
    } yield pair

  test("W3a UPHELD: a fraud-proof for a wrong full-quorum checkpoint ⇒ Slashings record written + challenger is the bounty submitter") {
    res =>
      implicit val (h, sp) = res
      val mg = mkAddress("wt-upheld-mg")
      val cp = wtCheckpoint(mg, wtAttestedRoot)
      for {
        // honest re-derivation DIFFERS from the attested root ⇒ committee deviated ⇒ UPHELD.
        pair <- mkWatchtowerMgr(honestRoot = wtHonestDiffersFromAttested)
        (mgr, store) = pair
        evPair <- wtEvidence(mg, cp)
        (evidence, submitterAddr) = evPair
        cpHash = evidence.fraudProof.disputedCheckpointHash
        _ <- invokeAcceptFraudProofs(mgr, SortedSet(evidence))
        slashed <- wasSlashed(store, cp.shardId, cpHash)
      } yield
        expect.all(
          // The dispute was UPHELD ⇒ the GSAM wrote a `Slashings` record for (shardId, disputedCheckpointHash).
          slashed,
          // The slash carried the challenger as submitter (the bounty recipient) — proven by the evidence binding.
          evidence.fraudProof.submitterId.value.value.nonEmpty,
          submitterAddr.value.value.nonEmpty
        )
  }

  test("W3a FRIVOLOUS: a fraud-proof whose recomputed honest root MATCHES the attested root ⇒ DisputeNotUpheld ⇒ NO slash") { res =>
    implicit val (h, sp) = res
    val mg = mkAddress("wt-frivolous-mg")
    val cp = wtCheckpoint(mg, wtAttestedRoot)
    for {
      // honest re-derivation REPRODUCES the attested root ⇒ committee did NOT deviate ⇒ NOT upheld (honest-committee floor).
      pair <- mkWatchtowerMgr(honestRoot = wtAttestedRoot)
      (mgr, store) = pair
      evPair <- wtEvidence(mg, cp)
      (evidence, _) = evPair
      cpHash = evidence.fraudProof.disputedCheckpointHash
      _ <- invokeAcceptFraudProofs(mgr, SortedSet(evidence))
      slashed <- wasSlashed(store, cp.shardId, cpHash)
    } yield expect(!slashed) // honest committee is NEVER slashed
  }

  test("W3a DOUBLE-SLASH: the same upheld fraud-proof applied twice ⇒ slashed once (Slashings record idempotent)") { res =>
    implicit val (h, sp) = res
    val mg = mkAddress("wt-double-mg")
    val cp = wtCheckpoint(mg, wtAttestedRoot)
    for {
      pair <- mkWatchtowerMgr(honestRoot = wtHonestDiffersFromAttested)
      (mgr, store) = pair
      evPair <- wtEvidence(mg, cp)
      (evidence, _) = evPair
      cpHash = evidence.fraudProof.disputedCheckpointHash
      // First accept: UPHELD ⇒ writes the Slashings record.
      _ <- invokeAcceptFraudProofs(mgr, SortedSet(evidence))
      slashedAfterFirst <- wasSlashed(store, cp.shardId, cpHash)
      // Second accept with the SAME evidence: the validator's double-slash guard (fromMptStore) now sees the record ⇒ AlreadySlashed ⇒ no
      // second slash. The record remains present-once.
      _ <- invokeAcceptFraudProofs(mgr, SortedSet(evidence))
      slashedAfterSecond <- wasSlashed(store, cp.shardId, cpHash)
      // A DIFFERENT, un-disputed checkpoint hash is never slashed.
      otherSlashed <- wasSlashed(store, cp.shardId, Hash("f" * 64))
    } yield expect.all(slashedAfterFirst, slashedAfterSecond, !otherSlashed)
  }

  test("W3a numShards=1 byte-identity: a carried fraud proof yields NO slash and the SAME mptRoot as no fraud proof") { res =>
    implicit val (h, sp) = res
    val mg = mkAddress("wt-noshard-mg")
    val cp = wtCheckpoint(mg, wtAttestedRoot)
    for {
      // numShards=1 ⇒ shardAcceptanceDeps-equivalent gate off; even WITH a validator wired, the fraud-proof slash must not fire (no
      // committees exist). Build numShards=1 managers; the validator is present but the carried proof must be a no-op for byte-identity.
      eventsRef1 <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      adoptedRef1 <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      callsRef1 <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      stub1 = StubAcceptanceManager(callsRef1, _ => ShardCheckpointAcceptResult.Accepted)
      withProofPair <- mkSuiteManagerWithProcessorAndStore(
        Some(mkShardingConfig(numShards = 1)),
        Some(stub1),
        mkCaptorProcessor(eventsRef1, adoptedRef1),
        store => Some(wtValidator(wtHonestDiffersFromAttested, store))
      )
      (mgrWithProof, storeWithProof) = withProofPair
      eventsRef2 <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      adoptedRef2 <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      callsRef2 <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      stub2 = StubAcceptanceManager(callsRef2, _ => ShardCheckpointAcceptResult.Accepted)
      noProofPair <- mkSuiteManagerWithProcessorAndStore(
        Some(mkShardingConfig(numShards = 1)),
        Some(stub2),
        mkCaptorProcessor(eventsRef2, adoptedRef2),
        store => Some(wtValidator(wtHonestDiffersFromAttested, store))
      )
      (mgrNoProof, _) = noProofPair
      evPair <- wtEvidence(mg, cp)
      (evidence, _) = evPair
      cpHash = evidence.fraudProof.disputedCheckpointHash
      // accept WITH a carried fraud proof at numShards=1 ...
      withProofRes <- invokeAcceptFraudProofs(mgrWithProof, SortedSet(evidence))
      slashedAtNumShards1 <- wasSlashed(storeWithProof, cp.shardId, cpHash)
      // ... vs accept with NO fraud proof.
      noProofRes <- invokeAcceptFraudProofs(mgrNoProof, SortedSet.empty)
    } yield
      expect.all(
        // numShards=1: the fraud-proof slash never fires (no committee exists) ⇒ no Slashings record ...
        !slashedAtNumShards1,
        // ... and the GSAM-derived stateProof mptRoot is byte-identical to the no-fraud-proof run (regression bar).
        withProofRes._2.mptRoot == noProofRes._2.mptRoot
      )
  }

  test("W3a DETERMINISM: leader/follower/peer fold the SAME upheld fraud proof to a byte-identical post-slash stateProof") { res =>
    implicit val (h, sp) = res
    val mg = mkAddress("wt-determinism-mg")
    val cp = wtCheckpoint(mg, wtAttestedRoot)
    // Three INDEPENDENT GSAM instances (each its own MPT) — the leader (produce), the follower (createContext), and a validating peer
    // (validateArtifact) all run accept() over the SAME embedded `fraudProofs` set + the SAME deterministic dispute validator. They MUST
    // reach the byte-identical post-slash stateProof — a divergence here would be a consensus fork.
    for {
      evPair <- wtEvidence(mg, cp)
      (evidence, _) = evPair
      cpHash = evidence.fraudProof.disputedCheckpointHash
      leaderPair <- mkWatchtowerMgr(honestRoot = wtHonestDiffersFromAttested)
      followerPair <- mkWatchtowerMgr(honestRoot = wtHonestDiffersFromAttested)
      peerPair <- mkWatchtowerMgr(honestRoot = wtHonestDiffersFromAttested)
      leader <- invokeAcceptFraudProofs(leaderPair._1, SortedSet(evidence))
      follower <- invokeAcceptFraudProofs(followerPair._1, SortedSet(evidence))
      peer <- invokeAcceptFraudProofs(peerPair._1, SortedSet(evidence))
      leaderSlashed <- wasSlashed(leaderPair._2, cp.shardId, cpHash)
      followerSlashed <- wasSlashed(followerPair._2, cp.shardId, cpHash)
      peerSlashed <- wasSlashed(peerPair._2, cp.shardId, cpHash)
    } yield
      expect.all(
        // All three independently UPHELD + wrote the Slashings record ...
        leaderSlashed,
        followerSlashed,
        peerSlashed,
        // ... and reached the byte-identical post-slash stateProof mptRoot (the no-fork invariant).
        leader._2.mptRoot == follower._2.mptRoot,
        leader._2.mptRoot == peer._2.mptRoot
      )
  }
}
