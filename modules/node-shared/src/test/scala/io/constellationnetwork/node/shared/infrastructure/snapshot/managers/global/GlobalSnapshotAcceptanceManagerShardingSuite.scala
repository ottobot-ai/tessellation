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
import io.constellationnetwork.node.shared.domain.nakamoto.{ShardAssignment, ShardWindowContinuation}
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
import io.constellationnetwork.node.shared.infrastructure.sharding.{RegisteredCheckpointSigner, TestCheckpointDutyValidator}
import io.constellationnetwork.node.shared.infrastructure.snapshot.DelegatedRewardsResult
import io.constellationnetwork.node.shared.logger.Slf4jLoggerBundle
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{PricingUpdate, SpendAction}
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
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
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.{addressSetImmutableCodec, signedCurrencySnapshotImmutableCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutableCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
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
  *   - `MetagraphSyncManager` is real (constructed from production `make`), so acknowledgement/pending-ordinal state uses the production
  *     transition rather than a receipt accumulator.
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
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal.MinValue)

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
      includedSnapshots = SortedMap(mg -> NonEmptyList.of(binary))
    )

  /** Build a minimal checkpoint shell. The signatures field is a placeholder — the stubbed manager doesn't verify it. */
  private def mkCheckpoint(
    shardId: ShardId,
    shardOrd: Long,
    gl0Anchor: Long,
    delta: ShardDerivedStateDelta
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
      committeeSignatures = NonEmptyList.of(placeholderSig),
      epoch = epochZero,
      executionBase = io.constellationnetwork.node.shared.ShardCheckpointTestFixtures.defaultExecutionBase
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
        capturedAdoptedRef.update(_ ++ events).as(SortedMap.empty[Address, MetagraphAcceptanceResult])

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

    }

  /** Deterministic processor fixture that reports a supplied recreated currency state for every replayed binary. */
  private def mkSuccessfulReplayProcessor(
    recreatedState: CurrencySnapshotWithState,
    acceptedPrefixLength: Option[Int] = None,
    onReplay: IO[Unit] = IO.unit
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
        IO.pure(
          StateChannelAcceptanceResult(
            SortedMap.empty,
            priorLastCurrencySnapshots,
            Set.empty,
            SortedMap.empty,
            SortedMap.empty
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
      )(implicit hasher: Hasher[IO]): IO[SortedMap[Address, MetagraphAcceptanceResult]] =
        onReplay.as(
          SortedMap.from(events.toList.flatMap {
            case (mg, newestFirst) =>
              val oldestFirst = newestFirst.reverse.toList
              val selected = acceptedPrefixLength.fold(oldestFirst)(oldestFirst.take)
              NonEmptyList
                .fromList(selected)
                .map(binaries => mg -> (binaries.map(_ -> recreatedState.some), SortedMap.empty[Address, Balance]))
          })
        )

      override def assembleAcceptanceResult(
        processed: SortedMap[Address, MetagraphAcceptanceResult],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        returned: Set[StateChannelOutput]
      ): StateChannelAcceptanceResult = {
        val recreated = SortedMap.from(processed.toList.flatMap { case (mg, (pairs, _)) => pairs.last._2.map(mg -> _) })
        StateChannelAcceptanceResult(
          accepted = processed.map { case (mg, (pairs, _)) => mg -> pairs.map(_._1) },
          calculatedCurrencyState = priorLastCurrencySnapshots ++ recreated,
          returned = returned,
          balanceUpdate = SortedMap.empty,
          incomingCurrencySnapshotsWithState = SortedMap.empty
        )
      }
    }

  /** Replay fixture that debits one shared payer per successfully assembled checkpoint group and records the balance each group observed.
    * Returning the debit inside each MG's `MetagraphAcceptanceResult` keeps failed-root behavior faithful to production assembly:
    * assembling an empty processed map must also discard that group's fee update.
    */
  private def mkSharedFeeReplayProcessor(
    recreatedState: CurrencySnapshotWithState,
    payer: Address,
    debit: Long,
    observedBalances: Ref[IO, List[(Address, Balance)]]
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
        IO.pure(
          StateChannelAcceptanceResult(
            SortedMap.empty,
            priorLastCurrencySnapshots,
            Set.empty,
            SortedMap.empty,
            SortedMap.empty
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
      )(implicit hasher: Hasher[IO]): IO[SortedMap[Address, MetagraphAcceptanceResult]] = {
        val before = currentBalances.getOrElse(payer, Balance.empty)
        val after = Balance(NonNegLong.unsafeFrom(before.value.value - debit))

        events.toList.traverse {
          case (mg, newestFirst) =>
            observedBalances
              .update(_ :+ (mg -> before))
              .as(
                mg -> (
                  newestFirst.reverse.map(_ -> recreatedState.some),
                  SortedMap(payer -> after)
                )
              )
        }
          .map(SortedMap.from(_))
      }

      override def assembleAcceptanceResult(
        processed: SortedMap[Address, MetagraphAcceptanceResult],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        returned: Set[StateChannelOutput]
      ): StateChannelAcceptanceResult = {
        val recreated = SortedMap.from(processed.toList.flatMap { case (mg, (pairs, _)) => pairs.last._2.map(mg -> _) })
        val balanceUpdate = SortedMap.from(processed.valuesIterator.flatMap(_._2).toList)

        StateChannelAcceptanceResult(
          accepted = processed.map { case (mg, (pairs, _)) => mg -> pairs.map(_._1) },
          calculatedCurrencyState = priorLastCurrencySnapshots ++ recreated,
          returned = returned,
          balanceUpdate = balanceUpdate,
          incomingCurrencySnapshotsWithState = SortedMap.empty
        )
      }
    }

  /** Stubbed `ShardCheckpointGl0AcceptanceManager` — returns the configured result per checkpoint and records every call.
    *
    * The GSAM adopt path now calls [[verifyEmbedded]] (the deterministic adopt-verifier), so THAT is the method that records the calls and
    * applies the decision the tests assert on. `evaluate` (node-local selection path) delegates to the same decision so the stub stays
    * consistent if a test ever exercises it.
    */
  private final case class StubAcceptanceManager(
    callsRef: Ref[IO, List[ShardCheckpoint]],
    decision: ShardCheckpoint => ShardCheckpointAcceptResult,
    adoptedRef: Option[Ref[IO, List[(ShardId, ShardOrdinal, Hash)]]] = None
  ) extends ShardCheckpointGl0AcceptanceManager[IO] {
    override def evaluate(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
      callsRef.update(_ :+ checkpoint).as(decision(checkpoint))
    override def evaluateForSigning(
      checkpoint: ShardCheckpoint
    ): IO[Either[VerifiedShardCheckpointFailure, VerifiedShardCheckpoint]] =
      IO.pure(Left(VerifiedShardCheckpointFailure.Rejected("test stub cannot mint signing capabilities")))
    override def verifyEmbedded(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
      callsRef.update(_ :+ checkpoint).as(decision(checkpoint))
    override def verifyExecutionCertificate(checkpoint: ShardCheckpoint): IO[Either[String, Unit]] =
      IO.pure(Left("test stub has no authenticated execution certificate"))
    override def verifyCommitteeSignature(
      checkpoint: ShardCheckpoint,
      signature: CommitteeMemberSignature
    ): IO[Either[String, Unit]] = IO.pure(Right(()))
    override def noteAdopted(
      shardId: io.constellationnetwork.schema.sharding.ShardId,
      shardOrdinal: ShardOrdinal,
      checkpointHash: io.constellationnetwork.security.hash.Hash
    ): IO[Unit] =
      adoptedRef.traverse_(_.update(_ :+ (shardId, shardOrdinal, checkpointHash)))
    override def lastAdoptedOrd(shardId: io.constellationnetwork.schema.sharding.ShardId): IO[Option[ShardOrdinal]] =
      IO.pure(None)
    override def lastAdoptedAnchor(
      shardId: io.constellationnetwork.schema.sharding.ShardId
    ): IO[Option[io.constellationnetwork.security.hash.Hash]] =
      IO.pure(None)
    override def lastAdoptedCheckpoint(
      shardId: io.constellationnetwork.schema.sharding.ShardId
    ): IO[Option[(ShardOrdinal, io.constellationnetwork.security.hash.Hash)]] =
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
      retention = ShardCheckpointRetentionConfig(retainedCheckpoints = 8L),
      checkpoint = ShardCheckpointConfig(binaryBufferCap = 4096)
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

  /** Like [[invokeAccept]] but returns the accepted `scSnapshots` map (6th tuple element), resulting state-proof MPT root, and resulting
    * GSI. These expose both byte identity and whether a rejected checkpoint leaked currency/tip state.
    */
  private def invokeAcceptCapturing(
    mgr: GlobalSnapshotAcceptanceManager[IO],
    scEvents: List[StateChannelOutput],
    shardCheckpoints: SortedMap[ShardId, ShardCheckpoint],
    lastSnapshotInfo: GlobalSnapshotInfo
  ): IO[(SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]], Option[Hash], GlobalSnapshotInfo)] =
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
      .map(result => (result._6, result._10.mptRoot, result._9))

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

  private def seedPriorFullCurrencyState(
    store: MptStore[IO, GlobalStateKey],
    mg: Address,
    snapshot: Signed[CurrencySnapshot],
    stateChannelTip: Hash
  )(implicit hasher: Hasher[IO]): IO[Unit] =
    for {
      currencyIndex <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.LastCurrencySnapshots)
      tipIndex <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.LastStateChannelSnapshotHashes)
      _ <- store.insert[Signed[CurrencySnapshot]](
        GlobalStateKey.metagraph(mg, GlobalStateFieldId.LastCurrencySnapshots),
        snapshot
      )
      _ <- store.insert[Hash](
        GlobalStateKey.metagraph(mg, GlobalStateFieldId.LastStateChannelSnapshotHashes),
        stateChannelTip
      )
      _ <- store.insert[SortedSet[Address]](currencyIndex, SortedSet(mg))
      _ <- store.insert[SortedSet[Address]](tipIndex, SortedSet(mg))
      _ <- store.commit(SnapshotOrdinal.MinValue)
    } yield ()

  private def seedPriorBalance(
    store: MptStore[IO, GlobalStateKey],
    address: Address,
    balance: Balance
  )(implicit hasher: Hasher[IO]): IO[Unit] =
    for {
      balanceIndex <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.Balances)
      _ <- store.insert[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, address), balance)
      _ <- store.insert[SortedSet[Address]](balanceIndex, SortedSet(address))
      _ <- store.commit(SnapshotOrdinal.MinValue)
    } yield ()

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

  test("tentative GSAM acceptance never advances the Phase-2 checkpoint anchor") { res =>
    implicit val (h, sp) = res
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      val stateProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("31" * 64)), Signature(Hex("32" * 70)))
      val recreatedState: CurrencySnapshotWithState =
        Left(Signed(CurrencySnapshot.mkGenesis(Map.empty, None, None), NonEmptySet.of(stateProof)))

      for {
        mg <- IO.pure(mkAddress("ack-after-replay"))
        binary = mkSignedBinary("ack-after-replay-content".getBytes("UTF-8"))
        root <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(mg -> recreatedState))
        delta = ShardDerivedStateDelta(
          perMetagraphMptRoots = SortedMap(mg -> root),
          includedSnapshots = SortedMap(mg -> NonEmptyList.of(binary))
        )
        cp = mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, delta)
        calls <- Ref.of[IO, List[ShardCheckpoint]](Nil)
        adoptions <- Ref.of[IO, List[(ShardId, ShardOrdinal, Hash)]](Nil)
        stub = StubAcceptanceManager(calls, _ => ShardCheckpointAcceptResult.Accepted, Some(adoptions))
        mgr <- mkSuiteManagerWithProcessor(
          Some(mkShardingConfig(numShards = 4)),
          Some(stub),
          mkSuccessfulReplayProcessor(recreatedState)
        )
        _ <- invokeAccept(mgr, Nil, SortedMap(cp.shardId -> cp), emptyGsi)
        recorded <- adoptions.get
      } yield expect(recorded.isEmpty)
    }
  }

  test("one failed MG root cannot advance the Phase-2 checkpoint anchor") { res =>
    implicit val (h, sp) = res
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      val stateProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("41" * 64)), Signature(Hex("42" * 70)))
      val recreatedState: CurrencySnapshotWithState =
        Left(Signed(CurrencySnapshot.mkGenesis(Map.empty, None, None), NonEmptySet.of(stateProof)))

      for {
        goodMg <- IO.pure(mkAddress("ack-partial-good"))
        badMg <- IO.pure(mkAddress("ack-partial-bad"))
        goodBinary = mkSignedBinary("ack-partial-good-content".getBytes("UTF-8"))
        badBinary = mkSignedBinary("ack-partial-bad-content".getBytes("UTF-8"))
        goodRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(goodMg -> recreatedState))
        badRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(badMg -> recreatedState))
        wrongBadRoot = if (badRoot === Hash("ff" * 32)) Hash("ee" * 32) else Hash("ff" * 32)
        delta = ShardDerivedStateDelta(
          perMetagraphMptRoots = SortedMap(goodMg -> goodRoot, badMg -> wrongBadRoot),
          includedSnapshots = SortedMap(goodMg -> NonEmptyList.of(goodBinary), badMg -> NonEmptyList.of(badBinary))
        )
        cp = mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, delta)
        calls <- Ref.of[IO, List[ShardCheckpoint]](Nil)
        adoptions <- Ref.of[IO, List[(ShardId, ShardOrdinal, Hash)]](Nil)
        stub = StubAcceptanceManager(calls, _ => ShardCheckpointAcceptResult.Accepted, Some(adoptions))
        mgr <- mkSuiteManagerWithProcessor(
          Some(mkShardingConfig(numShards = 4)),
          Some(stub),
          mkSuccessfulReplayProcessor(recreatedState)
        )
        accepted <- invokeAcceptCapturing(mgr, Nil, SortedMap(cp.shardId -> cp), emptyGsi)
        recorded <- adoptions.get
      } yield
        expect.all(
          recorded.isEmpty,
          !accepted._1.contains(goodMg),
          !accepted._1.contains(badMg),
          !accepted._3.lastCurrencySnapshots.contains(goodMg),
          !accepted._3.lastCurrencySnapshots.contains(badMg),
          !accepted._3.lastStateChannelSnapshotHashes.contains(goodMg),
          !accepted._3.lastStateChannelSnapshotHashes.contains(badMg)
        )
    }
  }

  test("an attested root matching only a valid prefix cannot adopt a checkpoint window with an invalid suffix") { res =>
    implicit val (h, sp) = res
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      val stateProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("61" * 64)), Signature(Hex("62" * 70)))
      val prefixState: CurrencySnapshotWithState =
        Left(Signed(CurrencySnapshot.mkGenesis(Map.empty, None, None), NonEmptySet.of(stateProof)))

      for {
        mg <- IO.pure(mkAddress("accepted-prefix-must-not-adopt"))
        validN = mkSignedBinary("valid-currency-N".getBytes("UTF-8"))
        validNHash <- Hasher[IO].hash(validN.value)
        nondecodableN1Base = mkSignedBinary(Array[Byte](0x01, 0x02, 0x03))
        nondecodableN1 = nondecodableN1Base.copy(
          value = nondecodableN1Base.value.copy(lastSnapshotHash = validNHash)
        )
        prefixRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(mg -> prefixState))
        delta = ShardDerivedStateDelta(
          // Adversarial claim: this root is exactly correct for N, but says nothing about the unprocessed N+1 input also covered by the
          // checkpoint signature. The processor fixture models the real prefix disposition proven with a nondecodable child in
          // GlobalSnapshotStateChannelEventsProcessorSuite.
          perMetagraphMptRoots = SortedMap(mg -> prefixRoot),
          includedSnapshots = SortedMap(mg -> NonEmptyList.of(validN, nondecodableN1))
        )
        cp = mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, delta)
        calls <- Ref.of[IO, List[ShardCheckpoint]](Nil)
        adoptions <- Ref.of[IO, List[(ShardId, ShardOrdinal, Hash)]](Nil)
        replayCalls <- Ref.of[IO, Int](0)
        stub = StubAcceptanceManager(calls, _ => ShardCheckpointAcceptResult.Accepted, Some(adoptions))
        prefixProcessor = mkSuccessfulReplayProcessor(
          prefixState,
          acceptedPrefixLength = Some(1),
          onReplay = replayCalls.update(_ + 1)
        )
        mgr <- mkSuiteManagerWithProcessor(Some(mkShardingConfig(numShards = 4)), Some(stub), prefixProcessor)
        accepted <- invokeAcceptCapturing(mgr, Nil, SortedMap(cp.shardId -> cp), emptyGsi)
        recordedAdoptions <- adoptions.get
        replayCount <- replayCalls.get
      } yield
        expect.all(
          replayCount == 1,
          !accepted._1.contains(mg),
          !accepted._3.lastCurrencySnapshots.contains(mg),
          !accepted._3.lastStateChannelSnapshotHashes.contains(mg),
          recordedAdoptions.isEmpty
        )
    }
  }

  test("second-pass replay mismatch rejects every checkpoint effect and cannot slash without portable fraud evidence") { res =>
    implicit val (h, sp) = res
    val mg = mkAddress("second-pass-mismatch")
    val signer = io.constellationnetwork.schema.peer.PeerId(Hex("cd" * 64))
    val binary = mkSignedBinary("second-pass-mismatch-content".getBytes("UTF-8"))
    val cp = mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, mkDelta(mg, binary))

    for {
      calls <- Ref.of[IO, List[ShardCheckpoint]](Nil)
      raw <- Ref.of[IO, List[StateChannelOutput]](Nil)
      replayed <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      stub = StubAcceptanceManager(
        calls,
        _ => ShardCheckpointAcceptResult.RejectedReExecutionMismatch("synthetic second-pass mismatch", List(signer))
      )
      pair <- mkSuiteManagerWithProcessorAndStore(
        Some(mkShardingConfig(numShards = 4)),
        Some(stub),
        mkCaptorProcessor(raw, replayed)
      )
      (mgr, store) = pair
      accepted <- invokeAcceptCapturing(mgr, Nil, SortedMap(cp.shardId -> cp), emptyGsi)
      cpHash <- Hasher[IO].hash(cp.signingPreimage)
      slashed <- wasSlashed(store, cp.shardId, cpHash)
      carryEligible <- ShardWindowContinuation.wasFullyAppliedF(
        cp.shardId,
        cp,
        SortedMap.empty,
        accepted._1
      )
      replayInputs <- replayed.get
    } yield
      expect.all(
        !carryEligible,
        !slashed,
        replayInputs.isEmpty,
        !accepted._1.contains(mg),
        !accepted._3.lastCurrencySnapshots.contains(mg),
        !accepted._3.lastStateChannelSnapshotHashes.contains(mg),
        accepted._3.balances.get(mg).isEmpty
      )
  }

  test("Continue+Defer within one checkpoint suppresses replay and every continuing sibling effect") { res =>
    implicit val (h, sp) = res
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      val stateProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("81" * 64)), Signature(Hex("82" * 70)))
      val recreatedState: CurrencySnapshotWithState =
        Left(Signed(CurrencySnapshot.mkGenesis(Map.empty, None, None), NonEmptySet.of(stateProof)))

      for {
        continueMg <- IO.pure(mkAddress("atomic-continue-before-defer"))
        deferMg <- IO.pure(mkAddress("atomic-deferred-sibling"))
        continueBinary = mkSignedBinary("atomic-continue-before-defer".getBytes("UTF-8"))
        deferBinaryBase = mkSignedBinary("atomic-deferred-sibling".getBytes("UTF-8"))
        deferBinary = deferBinaryBase.copy(
          value = deferBinaryBase.value.copy(lastSnapshotHash = Hash("99" * 32))
        )
        continueRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(continueMg -> recreatedState))
        deferRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(deferMg -> recreatedState))
        delta = ShardDerivedStateDelta(
          perMetagraphMptRoots = SortedMap(continueMg -> continueRoot, deferMg -> deferRoot),
          includedSnapshots = SortedMap(
            continueMg -> NonEmptyList.one(continueBinary),
            deferMg -> NonEmptyList.one(deferBinary)
          )
        )
        cp = mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, delta)
        calls <- Ref.of[IO, List[ShardCheckpoint]](Nil)
        replayCalls <- Ref.of[IO, Int](0)
        stub = StubAcceptanceManager(calls, _ => ShardCheckpointAcceptResult.Accepted)
        mgr <- mkSuiteManagerWithProcessor(
          Some(mkShardingConfig(numShards = 4)),
          Some(stub),
          mkSuccessfulReplayProcessor(recreatedState, onReplay = replayCalls.update(_ + 1))
        )
        accepted <- invokeAcceptCapturing(mgr, Nil, SortedMap(cp.shardId -> cp), emptyGsi)
        replayCount <- replayCalls.get
      } yield
        expect.all(
          replayCount == 0,
          !accepted._1.contains(continueMg),
          !accepted._1.contains(deferMg),
          !accepted._3.lastCurrencySnapshots.contains(continueMg),
          !accepted._3.lastCurrencySnapshots.contains(deferMg),
          !accepted._3.lastStateChannelSnapshotHashes.contains(continueMg),
          !accepted._3.lastStateChannelSnapshotHashes.contains(deferMg)
        )
    }
  }

  test("failed checkpoint group cannot leak a fee debit into a later valid group sharing the payer") { res =>
    implicit val (h, sp) = res
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      val stateProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("91" * 64)), Signature(Hex("92" * 70)))
      val recreatedState: CurrencySnapshotWithState =
        Left(Signed(CurrencySnapshot.mkGenesis(Map.empty, None, None), NonEmptySet.of(stateProof)))
      val failedMg = mkAddress("atomic-failed-fee-group")
      val validMg = mkAddress("atomic-valid-fee-group")
      val payer = mkAddress("atomic-shared-fee-payer")
      val startingBalance = Balance(NonNegLong.unsafeFrom(100L))
      val oneDebitBalance = Balance(NonNegLong.unsafeFrom(90L))
      val failedBinary = mkSignedBinary("atomic-failed-fee-group".getBytes("UTF-8"))
      val validBinary = mkSignedBinary("atomic-valid-fee-group".getBytes("UTF-8"))

      for {
        failedCorrectRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(failedMg -> recreatedState))
        validRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(validMg -> recreatedState))
        wrongFailedRoot = if (failedCorrectRoot === Hash("a9" * 32)) Hash("b9" * 32) else Hash("a9" * 32)
        failedCheckpoint = mkCheckpoint(
          ShardId.unsafeApply(0),
          1L,
          2L,
          ShardDerivedStateDelta(
            perMetagraphMptRoots = SortedMap(failedMg -> wrongFailedRoot),
            includedSnapshots = SortedMap(failedMg -> NonEmptyList.one(failedBinary))
          )
        )
        validCheckpoint = mkCheckpoint(
          ShardId.unsafeApply(1),
          1L,
          2L,
          ShardDerivedStateDelta(
            perMetagraphMptRoots = SortedMap(validMg -> validRoot),
            includedSnapshots = SortedMap(validMg -> NonEmptyList.one(validBinary))
          )
        )
        calls <- Ref.of[IO, List[ShardCheckpoint]](Nil)
        observed <- Ref.of[IO, List[(Address, Balance)]](Nil)
        stub = StubAcceptanceManager(calls, _ => ShardCheckpointAcceptResult.Accepted)
        pair <- mkSuiteManagerWithProcessorAndStore(
          Some(mkShardingConfig(numShards = 4)),
          Some(stub),
          mkSharedFeeReplayProcessor(recreatedState, payer, debit = 10L, observedBalances = observed)
        )
        (mgr, store) = pair
        _ <- seedPriorBalance(store, payer, startingBalance)
        accepted <- invokeAcceptCapturing(
          mgr,
          Nil,
          SortedMap(failedCheckpoint.shardId -> failedCheckpoint, validCheckpoint.shardId -> validCheckpoint),
          emptyGsi.copy(balances = SortedMap(payer -> startingBalance))
        )
        seen <- observed.get
      } yield
        expect.all(
          seen == List(failedMg -> startingBalance, validMg -> startingBalance),
          !accepted._1.contains(failedMg),
          accepted._1.keySet == Set(validMg),
          !accepted._3.lastCurrencySnapshots.contains(failedMg),
          accepted._3.lastCurrencySnapshots.contains(validMg),
          !accepted._3.lastStateChannelSnapshotHashes.contains(failedMg),
          accepted._3.lastStateChannelSnapshotHashes.contains(validMg),
          accepted._3.balances.get(payer).contains(oneDebitBalance)
        )
    }
  }

  test("Continue+Already checkpoint applies only when the already-consumed sibling root still matches current currency state") { res =>
    implicit val (h, sp) = res
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      val stateProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("71" * 64)), Signature(Hex("72" * 70)))
      val recreatedState: CurrencySnapshotWithState =
        Left(Signed(CurrencySnapshot.mkGenesis(Map.empty, None, None), NonEmptySet.of(stateProof)))
      val priorSnapshot = recreatedState.swap.toOption.get
      val alreadyMg = mkAddress("atomic-already-mg")
      val continueMg = mkAddress("atomic-continue-mg")
      val alreadyBinary = mkSignedBinary("atomic-already".getBytes("UTF-8"))
      val continueBinary = mkSignedBinary("atomic-continue".getBytes("UTF-8"))

      def run(validAlreadyRoot: Boolean): IO[(SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]], GlobalSnapshotInfo)] =
        for {
          alreadyTip <- Hasher[IO].hash(alreadyBinary.value)
          currentRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(alreadyMg -> recreatedState))
          continueRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(continueMg -> recreatedState))
          wrongRoot = if (currentRoot === Hash("f1" * 32)) Hash("f2" * 32) else Hash("f1" * 32)
          delta = ShardDerivedStateDelta(
            perMetagraphMptRoots = SortedMap(
              alreadyMg -> Option.when(validAlreadyRoot)(currentRoot).getOrElse(wrongRoot),
              continueMg -> continueRoot
            ),
            includedSnapshots = SortedMap(
              alreadyMg -> NonEmptyList.one(alreadyBinary),
              continueMg -> NonEmptyList.one(continueBinary)
            )
          )
          cp = mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, delta)
          calls <- Ref.of[IO, List[ShardCheckpoint]](Nil)
          stub = StubAcceptanceManager(calls, _ => ShardCheckpointAcceptResult.Accepted)
          pair <- mkSuiteManagerWithProcessorAndStore(
            Some(mkShardingConfig(numShards = 4)),
            Some(stub),
            mkSuccessfulReplayProcessor(recreatedState)
          )
          (mgr, store) = pair
          _ <- seedPriorFullCurrencyState(store, alreadyMg, priorSnapshot, alreadyTip)
          accepted <- invokeAcceptCapturing(mgr, Nil, SortedMap(cp.shardId -> cp), emptyGsi)
        } yield (accepted._1, accepted._3)

      for {
        valid <- run(validAlreadyRoot = true)
        invalid <- run(validAlreadyRoot = false)
        continueTip <- Hasher[IO].hash(continueBinary.value)
        alreadyTip <- Hasher[IO].hash(alreadyBinary.value)
      } yield
        expect.all(
          valid._1.keySet == Set(continueMg),
          valid._2.lastCurrencySnapshots.keySet.contains(alreadyMg),
          valid._2.lastCurrencySnapshots.keySet.contains(continueMg),
          valid._2.lastStateChannelSnapshotHashes.get(alreadyMg).contains(alreadyTip),
          valid._2.lastStateChannelSnapshotHashes.get(continueMg).contains(continueTip),
          !invalid._1.contains(continueMg),
          invalid._2.lastCurrencySnapshots.keySet.contains(alreadyMg),
          !invalid._2.lastCurrencySnapshots.keySet.contains(continueMg),
          invalid._2.lastStateChannelSnapshotHashes.get(alreadyMg).contains(alreadyTip),
          !invalid._2.lastStateChannelSnapshotHashes.keySet.contains(continueMg)
        )
    }
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
  // Test 8: independent-node accept byte-identity — the same embedded checkpoints + prior state produce
  //         byte-identical accepted state + state proof on independent manager instances.
  // ============================================================================

  /** GSAM accept-level determinism test. Three independent manager instances receive the same `(shardCheckpoints, scEvents,
    * lastSnapshotContext)` and must produce byte-identical accepted `scSnapshots` and state-proof `mptRoot`. This test does not exercise
    * the outer `produce`, `createContext`, or `validateArtifact` wiring. The CHANGE-3 filter is also exercised: a raw `scEvent` for a
    * sharded MG is excluded from the base path, leaving independently replayed checkpoint inputs as the sole source for sharded MGs.
    */
  test("three independent managers produce byte-identical accept results from the same checkpoint inputs") { res =>
    implicit val (h, sp) = res
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      val replayProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("51" * 64)), Signature(Hex("52" * 70)))
      val recreatedState: CurrencySnapshotWithState =
        Left(Signed(CurrencySnapshot.mkGenesis(Map.empty, None, None), NonEmptySet.of(replayProof)))

      // A processor that derives the same deterministic currency state from every adopted binary, so GL0 can independently
      // recreate and compare a non-empty state claim before including the metagraph in the accepted result.
      val derivingProcessor = mkSuccessfulReplayProcessor(recreatedState)

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
        recreatedRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(mgSharded -> recreatedState))
        delta = ShardDerivedStateDelta(
          perMetagraphMptRoots = SortedMap(mgSharded -> recreatedRoot),
          includedSnapshots = SortedMap(mgSharded -> NonEmptyList.of(binarySharded))
        )
        cp = mkCheckpoint(ShardId.unsafeApply(0), 1L, 2L, delta)
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
          // The adopted sharded MG is in the accepted scSnapshots only after recreated state matched the claimed root.
          produce._1.keySet.contains(mgSharded),
          // The raw sharded event was EXCLUDED from the base path (CHANGE 3) — its address is not present.
          !produce._1.keySet.contains(rawShardedEvent.address),
          // All three independent managers: byte-identical accepted scSnapshots ...
          produce._1 == createCtx._1,
          produce._1 == validate._1,
          // ... and byte-identical state-proof mptRoot.
          produce._2 == createCtx._2,
          produce._2 == validate._2
        )
    }
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

  private final case class WatchtowerCheckpointRig(
    checkpoint: ShardCheckpoint,
    checkpointSigner: RegisteredCheckpointSigner,
    acceptanceManager: ShardCheckpointGl0AcceptanceManager[IO]
  )

  /** Build a one-MG checkpoint carrying `attestedRoot` as the disputed MG's `perMetagraphMptRoots`. The sole execution signer is first
    * committed through the loader-validated period-zero KES+VRF registration path, then emits real Ed25519, KES, and VRF evidence. The
    * returned concrete manager resolves that same atomic pair and is the execution-certificate authority used by the fraud-proof verdict.
    */
  private def wtCheckpoint(mg: Address, attestedRoot: Hash)(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO]
  ): IO[WatchtowerCheckpointRig] =
    for {
      checkpointSigner <- RegisteredCheckpointSigner.make
      committeeKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      committeeId = io.constellationnetwork.schema.peer.PeerId.fromPublic(committeeKeyPair.getPublic)
      _ <- checkpointSigner.preregisterGenesis(committeeKeyPair, committeeId)
      shardAssignment = ShardAssignment.make[IO](numShards = 4)
      shardId <- shardAssignment.shardIdFor(mg)
      delta = ShardDerivedStateDelta(
        perMetagraphMptRoots = SortedMap(mg -> attestedRoot),
        includedSnapshots = SortedMap(mg -> NonEmptyList.of(mkSignedBinary("wt-content".getBytes("UTF-8"))))
      )
      shell = mkCheckpoint(shardId, shardOrd = 1L, gl0Anchor = 2L, delta = delta)
      signature <- checkpointSigner.sign(shell, committeeKeyPair, committeeId, checkpointSigner.defaultShardEta)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.one(signature))
      acceptanceManager <- ShardCheckpointGl0AcceptanceManager.make[IO](
        executionQuorum = 1,
        etaRotationSnapshots = 2550L,
        committeeMembership = (_, _) => IO.pure(Set(committeeId)),
        operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
        shardAssignment = shardAssignment,
        shardEtaFor = (_, _) => IO.pure(Some(checkpointSigner.defaultShardEta)),
        producerDutyValidator = TestCheckpointDutyValidator.allow[IO],
        reExecuteDerivation = (_, _, _, _) => IO.pure(attestedRoot)
      )
      certificate <- acceptanceManager.verifyExecutionCertificate(checkpoint)
      _ <- IO.fromEither(
        certificate.leftMap(reason => new IllegalStateException(s"watchtower checkpoint fixture certificate rejected: $reason"))
      )
    } yield WatchtowerCheckpointRig(checkpoint, checkpointSigner, acceptanceManager)

  /** Build an `InvalidStateProofEvidence` with a REAL challenger Ed25519 signature over the canonical preimage (so the validator's step-5
    * signature check passes), binding the carried checkpoint by its canonical hash (step-4). Mirrors InvalidStateProofValidatorSuite.
    */
  private def wtEvidence(mg: Address, cp: ShardCheckpoint, checkpointSigner: RegisteredCheckpointSigner)(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO]
  ): IO[(io.constellationnetwork.schema.slashing.InvalidStateProofEvidence, Address)] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { kp =>
      val submitterId = io.constellationnetwork.schema.peer.PeerId.fromPublic(kp.getPublic)
      checkpointSigner.preregisterGenesis(kp, submitterId) >> h.hash(cp.signingPreimage).flatMap { cpHash =>
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

  /** A pure stubbed re-derivation closure returning a fixed honest root — the verdict UPHOLDS iff `honest != attested`. Certificate
    * authentication remains concrete and resolves the checkpoint's preregistered atomic KES+VRF signer identity.
    */
  private def wtValidator(
    honestRoot: Hash,
    store: MptStore[IO, GlobalStateKey],
    verifyExecutionCertificate: ShardCheckpoint => IO[Either[String, Unit]]
  )(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO]
  ): io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[IO] =
    io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator.make[IO](
      replayCheckpoint = (windows, _, _) =>
        IO.pure(
          io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofBatchReplay.Reproduced(
            windows.keysIterator.map(_ -> honestRoot).to(SortedMap)
          )
        ),
      slashedReader = io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashedReader.fromMptStore[IO](store),
      verifyExecutionCertificate = verifyExecutionCertificate
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
  private def mkWatchtowerMgr(
    honestRoot: Hash,
    checkpointManager: ShardCheckpointGl0AcceptanceManager[IO],
    numShards: Int = 4
  )(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO]
  ): IO[(GlobalSnapshotAcceptanceManager[IO], MptStore[IO, GlobalStateKey])] =
    for {
      eventsRef <- Ref.of[IO, List[StateChannelOutput]](List.empty)
      adoptedRef <- Ref.of[IO, SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]](SortedMap.empty)
      pair <- mkSuiteManagerWithProcessorAndStore(
        Some(mkShardingConfig(numShards = numShards)),
        Some(checkpointManager),
        mkCaptorProcessor(eventsRef, adoptedRef),
        store => Some(wtValidator(honestRoot, store, checkpointManager.verifyExecutionCertificate))
      )
    } yield pair

  test("W3a UPHELD: a fraud-proof for a wrong full-quorum checkpoint ⇒ Slashings record written + challenger is the bounty submitter") {
    res =>
      implicit val (h, sp) = res
      val mg = mkAddress("wt-upheld-mg")
      for {
        rig <- wtCheckpoint(mg, wtAttestedRoot)
        cp = rig.checkpoint
        // honest re-derivation DIFFERS from the attested root ⇒ committee deviated ⇒ UPHELD.
        pair <- mkWatchtowerMgr(
          honestRoot = wtHonestDiffersFromAttested,
          checkpointManager = rig.acceptanceManager
        )
        (mgr, store) = pair
        evPair <- wtEvidence(mg, cp, rig.checkpointSigner)
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
    for {
      rig <- wtCheckpoint(mg, wtAttestedRoot)
      cp = rig.checkpoint
      // honest re-derivation REPRODUCES the attested root ⇒ committee did NOT deviate ⇒ NOT upheld (honest-committee floor).
      pair <- mkWatchtowerMgr(honestRoot = wtAttestedRoot, checkpointManager = rig.acceptanceManager)
      (mgr, store) = pair
      evPair <- wtEvidence(mg, cp, rig.checkpointSigner)
      (evidence, _) = evPair
      cpHash = evidence.fraudProof.disputedCheckpointHash
      _ <- invokeAcceptFraudProofs(mgr, SortedSet(evidence))
      slashed <- wasSlashed(store, cp.shardId, cpHash)
    } yield expect(!slashed) // honest committee is NEVER slashed
  }

  test("W3a DOUBLE-SLASH: the same upheld fraud-proof applied twice ⇒ slashed once (Slashings record idempotent)") { res =>
    implicit val (h, sp) = res
    val mg = mkAddress("wt-double-mg")
    for {
      rig <- wtCheckpoint(mg, wtAttestedRoot)
      cp = rig.checkpoint
      pair <- mkWatchtowerMgr(
        honestRoot = wtHonestDiffersFromAttested,
        checkpointManager = rig.acceptanceManager
      )
      (mgr, store) = pair
      evPair <- wtEvidence(mg, cp, rig.checkpointSigner)
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
    for {
      rig <- wtCheckpoint(mg, wtAttestedRoot)
      cp = rig.checkpoint
      // numShards=1 ⇒ shardAcceptanceDeps-equivalent gate off; even WITH a validator wired, the fraud-proof slash must not fire (no
      // committees exist). Build numShards=1 managers; the validator is present but the carried proof must be a no-op for byte-identity.
      withProofPair <- mkWatchtowerMgr(wtHonestDiffersFromAttested, rig.acceptanceManager, numShards = 1)
      (mgrWithProof, storeWithProof) = withProofPair
      noProofPair <- mkWatchtowerMgr(wtHonestDiffersFromAttested, rig.acceptanceManager, numShards = 1)
      (mgrNoProof, _) = noProofPair
      evPair <- wtEvidence(mg, cp, rig.checkpointSigner)
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
    // Three INDEPENDENT GSAM instances (each its own MPT) — the leader (produce), the follower (createContext), and a validating peer
    // (validateArtifact) all run accept() over the SAME embedded `fraudProofs` set + the SAME deterministic dispute validator. They MUST
    // reach the byte-identical post-slash stateProof — a divergence here would be a consensus fork.
    for {
      rig <- wtCheckpoint(mg, wtAttestedRoot)
      cp = rig.checkpoint
      evPair <- wtEvidence(mg, cp, rig.checkpointSigner)
      (evidence, _) = evPair
      cpHash = evidence.fraudProof.disputedCheckpointHash
      leaderPair <- mkWatchtowerMgr(
        honestRoot = wtHonestDiffersFromAttested,
        checkpointManager = rig.acceptanceManager
      )
      followerPair <- mkWatchtowerMgr(
        honestRoot = wtHonestDiffersFromAttested,
        checkpointManager = rig.acceptanceManager
      )
      peerPair <- mkWatchtowerMgr(
        honestRoot = wtHonestDiffersFromAttested,
        checkpointManager = rig.acceptanceManager
      )
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

  test("W3a UNAUTHENTICATED: a correctly bound fraud proof cannot slash signer IDs from an invalid execution certificate") { res =>
    implicit val (h, sp) = res
    val mg = mkAddress("wt-unauthenticated-mg")
    for {
      rig <- wtCheckpoint(mg, wtAttestedRoot)
      invalidSignature = rig.checkpoint.committeeSignatures.head.copy(ed25519Sig = Hex.fromBytes(Array.fill(64)(0.toByte)))
      invalidCheckpoint = rig.checkpoint.copy(committeeSignatures = NonEmptyList.one(invalidSignature))
      certificate <- rig.acceptanceManager.verifyExecutionCertificate(invalidCheckpoint)
      pair <- mkWatchtowerMgr(
        honestRoot = wtHonestDiffersFromAttested,
        checkpointManager = rig.acceptanceManager
      )
      (mgr, store) = pair
      evPair <- wtEvidence(mg, invalidCheckpoint, rig.checkpointSigner)
      (evidence, _) = evPair
      _ <- invokeAcceptFraudProofs(mgr, SortedSet(evidence))
      slashed <- wasSlashed(store, invalidCheckpoint.shardId, evidence.fraudProof.disputedCheckpointHash)
    } yield expect.all(certificate.isLeft, !slashed)
  }
}
