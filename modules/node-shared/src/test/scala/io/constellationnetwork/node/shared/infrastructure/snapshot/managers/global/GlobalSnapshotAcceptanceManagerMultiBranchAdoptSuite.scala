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
import io.constellationnetwork.node.shared.domain.nakamoto.overlay._
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
import io.constellationnetwork.node.shared.infrastructure.sharding.RegisteredCheckpointSigner
import io.constellationnetwork.node.shared.infrastructure.snapshot.DelegatedRewardsResult
import io.constellationnetwork.node.shared.logger.Slf4jLoggerBundle
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{PricingUpdate, SpendAction}
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nodeCollateral.UpdateNodeCollateral
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.swap.{AllowSpend, AllowSpendBlock}
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary, StateChannelValidationType}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import weaver.MutableIOSuite

/** Boundary tests for sharded currency adoption over a multi-branch global MPT.
  *
  * Checkpoints carry only included snapshot binaries and claimed roots. The only candidate state is the output of
  * `processCurrencySnapshots`, representing GL0 recreation. Adoption requires its per-MG root to match the checkpoint claim; missing or
  * mismatched roots fail closed. Branch/base fixtures prove local overlay position cannot replace replay with committee-carried state.
  */
object GlobalSnapshotAcceptanceManagerMultiBranchAdoptSuite extends MutableIOSuite {

  final case class RegisteredCommitteeIdentity(
    checkpointSigner: RegisteredCheckpointSigner,
    keyPair: KeyPair,
    peerId: PeerId
  )

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO], RegisteredCommitteeIdentity)

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(sp: SecurityProvider[IO]) <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
      checkpointSigner <- RegisteredCheckpointSigner.make.asResource
      committeeKeyPair <- KeyPairGenerator.makeKeyPair[IO].asResource
      committeeId = PeerId.fromPublic(committeeKeyPair.getPublic)
      _ <- checkpointSigner.preregisterGenesis(committeeKeyPair, committeeId).asResource
      committeeIdentity = RegisteredCommitteeIdentity(checkpointSigner, committeeKeyPair, committeeId)
    } yield (h, sp, j, committeeIdentity)

  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  // The state-proof selector must be active (low ordinal) so currencySnapshotFieldRoots / the GSI state-proof include the unrolled fields.
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(0L)))

  private val acceptOrdinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(2L))

  // ============================================================================
  // Address / hash / value fixtures
  // ============================================================================

  private def mkAddress(label: String): Address =
    Address.fromBytes(label.getBytes("UTF-8"))

  private val genesisHash: Hash = Hash("0" * 64)
  private val epochZero: EtaPeriod = EtaPeriod(0L)

  /** Sentinel `Signed[StateChannelSnapshotBinary]` — its `lastSnapshotHash = Hash.empty` so the GSAM CHAIN-HOLE GUARD finds it at idx 0
    * against the default `Hash.empty` SC tip (an unseeded MG) and adopts the window. The captor/deriving processor never inspects content.
    */
  private def mkSignedBinary(content: Array[Byte]): Signed[StateChannelSnapshotBinary] = {
    val sentinelProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70)))
    Signed(
      StateChannelSnapshotBinary(lastSnapshotHash = Hash.empty, content = content, fee = SnapshotFee(NonNegLong.unsafeFrom(0L))),
      NonEmptySet.of(sentinelProof)
    )
  }

  /** A `Signed[CurrencyIncrementalSnapshot]` built with a sentinel proof (no keypair needed — the GSAM only round-trips it through the
    * `LastIncrementalCurrencySnapshots` codec and re-hashes its bytes for the `incrementalRoot`; signature validity is irrelevant here).
    */
  private def mkSignedIncremental(
    snapOrdinal: Long,
    // TRACK-1 delete-override: the tip incremental's SIGNED stateProof is now load-bearing at the GSAM adopt path's re-grounded GAP-1
    // (the now-unconditional `balances`/`lastTxRefs` compare binds the reconstructed value to THIS proof). Default = the all-empty sentinel
    // for callers that only need the base-seed incremental (its stateProof is never GAP-1-checked — the base reconstructs from the `Mg*`
    // partitions). The ADOPT tests (A/B/C) pass `nextInfo.stateProof` so the tip proof is consistent with the committed post-state.
    stateProof: CurrencySnapshotStateProof = CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None)
  ): Signed[CurrencyIncrementalSnapshot] = {
    val sentinelProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("33" * 64)), Signature(Hex("44" * 70)))
    val snap = CurrencyIncrementalSnapshot(
      ordinal = SnapshotOrdinal.unsafeApply(snapOrdinal),
      height = Height.MinValue,
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      rewards = SortedSet.empty,
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = stateProof,
      epochProgress = EpochProgress.MinValue,
      dataApplication = None,
      messages = None,
      globalSnapshotSyncs = None,
      feeTransactions = None,
      artifacts = None,
      allowSpendBlocks = None,
      tokenLockBlocks = None,
      globalSyncView = None
    )
    Signed(snap, NonEmptySet.of(sentinelProof))
  }

  /** A `CurrencySnapshotInfo` carrying exactly one balance entry. It is used as both base and branch fixture state so the overlay readers
    * can prove they expose different priors for the same metagraph.
    */
  private def infoWithBalance(holder: Address, amount: Long): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap.empty,
      balances = SortedMap(holder -> Balance(NonNegLong.unsafeFrom(amount))),
      lastMessages = None,
      lastFeeTxRefs = None,
      lastAllowSpendRefs = None,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = None,
      activeTokenLocks = None
    )

  // ============================================================================
  // Checkpoint construction
  // ============================================================================

  /** Structural seed used only to construct the signature-excluded checkpoint preimage required by [[RegisteredCheckpointSigner]]. It is
    * replaced before the checkpoint leaves this helper and is never passed to [[StubAcceptanceManager]].
    */
  private def unsignedTemplateSeed(peerId: PeerId): CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = peerId,
      vrfProof = Hex(""),
      ed25519Sig = Hex(""),
      kesProductSig = Hex(""),
      kesTreeStep = 0
    )

  /** Build a checkpoint carrying one replayable binary plus the committee's claimed per-MG root. The execution base is signed metadata
    * consumed by `ShardCheckpointGl0AcceptanceManager`; GSAM receives only its accept/reject verdict and never applies committee state.
    */
  private def mkRightArmCheckpoint(
    mg: Address,
    binary: Signed[StateChannelSnapshotBinary],
    attestedRoot: Hash,
    executionBaseOrdinal: SnapshotOrdinal = SnapshotOrdinal.MinValue,
    includeAttestedRoot: Boolean = true
  )(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO],
    committeeIdentity: RegisteredCommitteeIdentity
  ): IO[ShardCheckpoint] = {
    val template = ShardCheckpoint(
      shardId = ShardId.unsafeApply(0),
      parentCheckpointHash = genesisHash,
      shardOrdinal = ShardOrdinal(1L),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(2L)),
      slot = io.constellationnetwork.schema.nakamoto.slot.Slot.unsafeApply(2L),
      derivedStateDelta = ShardDerivedStateDelta(
        perMetagraphMptRoots = if (includeAttestedRoot) SortedMap(mg -> attestedRoot) else SortedMap.empty,
        includedSnapshots = SortedMap(mg -> NonEmptyList.of(binary))
      ),
      committeeSignatures = NonEmptyList.one(unsignedTemplateSeed(committeeIdentity.peerId)),
      epoch = epochZero,
      executionBaseOrdinal = executionBaseOrdinal
    )

    committeeIdentity.checkpointSigner
      .sign(template, committeeIdentity.keyPair, committeeIdentity.peerId)
      .map(signature => template.copy(committeeSignatures = NonEmptyList.one(signature)))
  }

  // ============================================================================
  // GL0-recreated root (the exact value compared with the checkpoint claim)
  // ============================================================================

  private def recreatedRoot(
    mg: Address,
    inc: Signed[CurrencyIncrementalSnapshot],
    next: CurrencySnapshotInfo
  )(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[Hash] = {
    val nextState = Right((inc, next)): CurrencySnapshotWithState
    GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(mg -> nextState))
  }

  // ============================================================================
  // Seeding the finalized base / a branch with Right-arm per-MG state
  // ============================================================================

  /** Write the full Right-arm prior for `mg` (`info`) into the supplied `CurrencyInfoMpt` writer via the SAME production writer gl0 uses
    * (`GlobalStateConverter.writeCurrencyInfo`), PLUS the `LastIncrementalCurrencySnapshots` fieldId-5 key. This is exactly the on-disk
    * shape `accept()`'s `priorLastCurrencySnapshots` build reads back: a present fieldId-5 incremental ⇒ the `Right` arm, with the info
    * reconstructed from the unrolled `Mg*` prefix.
    */
  private def writeRightArm(
    writer: GlobalStateConverter.CurrencyInfoMpt[IO],
    insertInc: Map[GlobalStateKey, Signed[CurrencyIncrementalSnapshot]] => IO[Unit],
    mg: Address,
    inc: Signed[CurrencyIncrementalSnapshot],
    info: CurrencySnapshotInfo,
    priorInfoForRemovals: CurrencySnapshotInfo
  )(implicit h: Hasher[IO]): IO[Unit] =
    for {
      _ <- GlobalStateConverter.writeCurrencyInfo[IO](mg, info, priorInfoForRemovals, writer)
      _ <- insertInc(Map(GlobalStateKey.metagraph(mg, GlobalStateFieldId.LastIncrementalCurrencySnapshots) -> inc))
    } yield ()

  /** Build a fresh MptStore. */
  private def freshStore(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
    } yield store

  /** Seed the BASE store directly with the Right-arm info, fieldId-5 incremental, and active-address index for `mg`. Returns the
    * round-tripped prior GSAM will actually read during replay.
    */
  private def seedBaseAndRoundTrip(
    store: MptStore[IO, GlobalStateKey],
    mg: Address,
    inc: Signed[CurrencyIncrementalSnapshot],
    basePrior: CurrencySnapshotInfo
  )(implicit h: Hasher[IO]): IO[CurrencySnapshotInfo] = {
    val emptyInfo = infoWithBalance(mkAddress("__never__"), 0L).copy(balances = SortedMap.empty)
    val storeWriter = GlobalStateConverter.CurrencyInfoMpt.fromMptStore(store)
    for {
      _ <- writeRightArm(storeWriter, store.insert[Signed[CurrencyIncrementalSnapshot]](_), mg, inc, basePrior, emptyInfo)
      indexKey <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.LastCurrencySnapshots)
      _ <- store.insert[SortedSet[Address]](indexKey, SortedSet(mg))
      reader = GlobalStateConverter.CurrencyInfoMpt.fromMptStore(store)
      roundTripped <- GlobalStateConverter.reconstructCurrencyInfoFrom[IO](mg, reader)
    } yield roundTripped
  }

  /** Build the production MultiBranch overlay over the (already base-seeded) store. If `branchPrior` is supplied, an INTERVENING per-MG
    * incremental+info is committed into a child branch so the branch reader returns `branchPrior` for `mg` (overriding the base
    * `basePrior`); the returned `BranchId` is that child tip (threaded as `parentTip`). If `None`, the returned `BranchId` is `base`
    * (passthrough-equivalent) — the CONTROL where `branch == base`.
    */
  private def mkOverlayWithBranch(
    store: MptStore[IO, GlobalStateKey],
    mg: Address,
    inc: Signed[CurrencyIncrementalSnapshot],
    basePrior: CurrencySnapshotInfo,
    branchPrior: Option[CurrencySnapshotInfo]
  )(implicit h: Hasher[IO]): IO[(MptOverlay[IO, GlobalStateKey], BranchId)] =
    for {
      pcTree <- io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        underlying = store,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      parentTip <- branchPrior match {
        case None => BranchId.base.pure[IO]
        case Some(bp) =>
          val childTip = BranchId(Hash("c" * 64))
          for {
            handle <- overlay.checkout(BranchId.base)
            acceptanceMpt = AcceptanceMpt.fromOverlay[IO](overlay, BranchId.base, handle)
            branchWriter = CurrencyInfoMptAdapters.mptFor(acceptanceMpt)
            // Write the intervening committed incremental+info into the child branch: the branch prior OVERRIDES the base prior for `mg`.
            // The `priorInfoForRemovals = basePrior` issues the removal keys needed so the branch reflects `bp` (not a union with base).
            _ <- writeRightArm(branchWriter, acceptanceMpt.insert[Signed[CurrencyIncrementalSnapshot]](_), mg, inc, bp, basePrior)
            _ <- overlay.commit(handle, childTip, acceptOrdinal)
          } yield childTip
      }
    } yield (overlay, parentTip)

  // ============================================================================
  // The deriving processor — puts the MG in the RIGHT arm of calculatedCurrencyState
  // ============================================================================

  /** A processor boundary fixture whose output stands for GL0's recreated state. The adopter must use it verbatim and only compare its root
    * with the checkpoint claim.
    */
  private def derivingProcessor(
    inc: Signed[CurrencyIncrementalSnapshot],
    seedInfo: CurrencySnapshotInfo,
    balanceUpdate: SortedMap[Address, Balance] = SortedMap.empty,
    onReplay: IO[Unit] = IO.unit
  ): GlobalSnapshotStateChannelEventsProcessor[IO] =
    new GlobalSnapshotStateChannelEventsProcessor[IO] {
      override def process(
        snapshotOrdinal: SnapshotOrdinal,
        currentBalances: SortedMap[Address, Balance],
        priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
        priorLastCurrencySnapshots: SortedMap[Address, CurrencySnapshotWithState],
        events: List[StateChannelOutput],
        validationType: StateChannelValidationType,
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[StateChannelAcceptanceResult] =
        StateChannelAcceptanceResult(
          accepted = SortedMap.empty[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
          calculatedCurrencyState = SortedMap.empty[Address, CurrencySnapshotWithState],
          returned = Set.empty[StateChannelOutput],
          balanceUpdate = SortedMap.empty[Address, Balance],
          incomingCurrencySnapshotsWithState = SortedMap.empty[Address, List[CurrencySnapshotWithState]]
        ).pure[IO]

      override def processCurrencySnapshots(
        snapshotOrdinal: SnapshotOrdinal,
        currentBalances: SortedMap[Address, Balance],
        priorLastCurrencySnapshots: SortedMap[Address, CurrencySnapshotWithState],
        events: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[SortedMap[Address, MetagraphAcceptanceResult]] =
        onReplay.as {
          events.map {
            case (addr, bins) =>
              val pairs: NonEmptyList[BinaryCurrencyPair] =
                bins.map(b => (b, (Right((inc, seedInfo)): CurrencySnapshotWithState).some))
              addr -> ((pairs, balanceUpdate))
          }
        }

      override def assembleAcceptanceResult(
        processed: SortedMap[Address, MetagraphAcceptanceResult],
        priorLastCurrencySnapshots: SortedMap[Address, CurrencySnapshotWithState],
        returned: Set[StateChannelOutput]
      ): StateChannelAcceptanceResult = {
        val lastStatePerAddress: SortedMap[Address, CurrencySnapshotWithState] =
          processed.map { case (k, (v, _)) => k -> v.toList.flatMap(_._2).lastOption }.collect { case (key, Some(s)) => key -> s }
        val incoming: SortedMap[Address, List[CurrencySnapshotWithState]] =
          processed.map { case (k, (v, _)) => k -> v.toList.flatMap(_._2) }.filterNot { case (_, list) => list.isEmpty }
        val combinedBalanceUpdate = processed.values.map(_._2).foldLeft(SortedMap.empty[Address, Balance])(_ ++ _)
        StateChannelAcceptanceResult(
          accepted = processed.map { case (k, (v, _)) => k -> v.map(_._1) },
          calculatedCurrencyState = priorLastCurrencySnapshots.concat(lastStatePerAddress),
          returned = returned,
          balanceUpdate = combinedBalanceUpdate,
          incomingCurrencySnapshotsWithState = incoming
        )
      }

    }

  // ============================================================================
  // Stub checkpoint manager — captures the GSAM verifier boundary and can model a rejected execution base. Its authority stops at that
  // boundary: every checkpoint passed to it carries a real preregistered KES+VRF execution signature. Concrete certificate rejection is
  // covered by ShardCheckpointGl0AcceptanceManagerSuite.
  // ============================================================================

  private final case class StubAcceptanceManager(
    callsRef: Ref[IO, List[ShardCheckpoint]],
    verdict: ShardCheckpoint => ShardCheckpointAcceptResult = (_: ShardCheckpoint) => ShardCheckpointAcceptResult.Accepted
  ) extends ShardCheckpointGl0AcceptanceManager[IO] {
    override def evaluate(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
      callsRef.update(_ :+ checkpoint).as(verdict(checkpoint))
    override def evaluateForSigning(
      checkpoint: ShardCheckpoint
    ): IO[Either[VerifiedShardCheckpointFailure, VerifiedShardCheckpoint]] =
      IO.pure(Left(VerifiedShardCheckpointFailure.Rejected("test stub cannot mint signing capabilities")))
    override def verifyEmbedded(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
      callsRef.update(_ :+ checkpoint).as(verdict(checkpoint))
    override def verifyExecutionCertificate(checkpoint: ShardCheckpoint): IO[Either[String, Unit]] =
      IO.pure(Left("adoption-flow stub has no authenticated execution certificate"))
    override def verifyCommitteeSignature(
      checkpoint: ShardCheckpoint,
      signature: CommitteeMemberSignature
    ): IO[Either[String, Unit]] = IO.pure(Right(()))
    override def noteAdopted(shardId: ShardId, shardOrdinal: ShardOrdinal, checkpointHash: Hash): IO[Unit] = IO.unit
    override def lastAdoptedOrd(shardId: ShardId): IO[Option[ShardOrdinal]] = IO.pure(None)
    override def lastAdoptedAnchor(shardId: ShardId): IO[Option[Hash]] = IO.pure(None)
    override def lastAdoptedCheckpoint(shardId: ShardId): IO[Option[(ShardOrdinal, Hash)]] = IO.pure(None)
    override def watchtowerReExec(checkpoint: ShardCheckpoint): IO[List[WatchtowerMismatch]] =
      IO.pure(List.empty[WatchtowerMismatch])
  }

  // ============================================================================
  // GSAM-under-test wiring (over an explicit overlay + the deriving processor)
  // ============================================================================

  private def mkShardingConfig(numShards: Int): ShardingConfig =
    ShardingConfig(
      numShards = numShards,
      retention = ShardCheckpointRetentionConfig(retainedCheckpoints = 8L),
      checkpoint = ShardCheckpointConfig(binaryBufferCap = 4096)
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

  private def mkManager(
    overlay: MptOverlay[IO, GlobalStateKey],
    stateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[IO],
    checkpointManager: ShardCheckpointGl0AcceptanceManager[IO],
    numShards: Int = 4
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], j: JsonSerializer[IO]): IO[GlobalSnapshotAcceptanceManager[IO]] = {
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
          contextUpdate = BlockAcceptanceContextUpdate(SortedMap.empty, SortedMap.empty, Map.empty)
        ).pure[IO]
      override def acceptBlock(
        block: Signed[Block],
        context: BlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldValidateCollateral: Boolean
      )(implicit hasher: Hasher[IO]): IO[Either[BlockNotAcceptedReason, (BlockAcceptanceContextUpdate, UsageCount)]] =
        (BlockAcceptanceContextUpdate(SortedMap.empty, SortedMap.empty, Map.empty), initUsageCount).asRight.pure[IO]
    }

    val mockAllowSpendBlockAcceptanceManager: AllowSpendBlockAcceptanceManager[IO] = new AllowSpendBlockAcceptanceManager[IO] {
      override def acceptBlocksIteratively(
        blocks: List[Signed[AllowSpendBlock]],
        context: AllowSpendBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[AllowSpendBlockAcceptanceResult] =
        AllowSpendBlockAcceptanceResult(AllowSpendBlockAcceptanceContextUpdate.empty, List.empty, List.empty).pure[IO]
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
        TokenLockBlockAcceptanceResult(TokenLockBlockAcceptanceContextUpdate.empty, blocks, List.empty).pure[IO]
      override def acceptBlock(
        block: Signed[TokenLockBlock],
        context: TokenLockBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[Either[TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate]] =
        TokenLockBlockAcceptanceContextUpdate.empty.asRight.pure[IO]
    }

    val mockUpdateNodeParametersAcceptanceManager: UpdateNodeParametersAcceptanceManager[IO] =
      new UpdateNodeParametersAcceptanceManager[IO] {
        override def acceptUpdateNodeParameters(
          events: List[Signed[io.constellationnetwork.schema.node.UpdateNodeParameters]],
          lastSnapshotContext: GlobalSnapshotInfo
        ): IO[UpdateNodeParametersAcceptanceResult] =
          UpdateNodeParametersAcceptanceResult(List.empty, List.empty).pure[IO]
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
          UpdateNodeCollateralAcceptanceResult(SortedMap.empty, List.empty, SortedMap.empty, List.empty).pure[IO]
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
      ): IO[(Map[Address, List[SpendAction]], Map[Address, List[(SpendAction, List[SpendActionValidator.SpendActionValidationError])]])] =
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

    Slf4jLoggerBundle.makeUnsafe[IO].flatMap { loggerBundle =>
      GlobalSnapshotAcceptanceManager
        .make[IO](
          FieldsAddedOrdinals(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty),
          MetagraphsSyncConfig(PosInt(100)),
          AppEnvironment.Dev,
          blockAcceptanceManager = mockBlockAcceptanceManager,
          allowSpendBlockAcceptanceManager = mockAllowSpendBlockAcceptanceManager,
          tokenLockBlockAcceptanceManager = mockTokenLockBlockAcceptanceManager,
          stateChannelEventsProcessor = stateChannelEventsProcessor,
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
          shardCheckpointAcceptanceManager = Some(checkpointManager),
          shardAssignment = Some(ShardAssignment.make[IO](numShards = numShards)),
          etaRotationSnapshots = 2550L,
          invaliditySlashingConfig = InvalidStateProofSlashingConfig(
            watchtowerEnabled = true,
            slashFraction = io.constellationnetwork.numerics.Ratio.One,
            bountyFraction = io.constellationnetwork.numerics.Ratio(1, 20),
            cooldownEpochs = 100L
          )
        )
    }
  }

  /** A `GlobalSnapshotInfo` whose `lastCurrencySnapshots` already contains `mg -> Right((inc, gsiInfo))`. The GSAM unions this with the
    * MPT-derived prior (the #113 per-address fallback) — but the MPT (branch/base) is the authoritative source for our seeded MG, so the
    * branch-vs-base divergence is what `priorInfoOf` reads. We still populate it so the keyset/fallback path is exercised consistently.
    */
  private def gsiWith(
    mg: Address,
    inc: Signed[CurrencyIncrementalSnapshot],
    gsiInfo: CurrencySnapshotInfo
  ): GlobalSnapshotInfo =
    GlobalSnapshotInfo(
      lastStateChannelSnapshotHashes = SortedMap.empty,
      lastTxRefs = SortedMap.empty,
      balances = SortedMap.empty,
      lastCurrencySnapshots = SortedMap(mg -> (Right((inc, gsiInfo)): CurrencySnapshotWithState)),
      lastCurrencySnapshotsProofs = SortedMap.empty,
      activeAllowSpends = None,
      activeTokenLocks = None,
      tokenLockBalances = None,
      lastAllowSpendRefs = None,
      lastTokenLockRefs = None,
      updateNodeParameters = None,
      activeDelegatedStakes = None,
      delegatedStakesWithdrawals = None,
      activeNodeCollaterals = Some(SortedMap.empty),
      nodeCollateralWithdrawals = Some(SortedMap.empty),
      priceState = Some(SortedMap.empty),
      metagraphSyncData = Some(SortedMap.empty),
      historicalStakeSnapshots = SortedMap.empty
    )

  /** Run `accept()` and return the committed GSI (tuple element `_9`) so tests can inspect whether `mg`'s currency advanced. */
  private def runAccept(
    mgr: GlobalSnapshotAcceptanceManager[IO],
    checkpoint: ShardCheckpoint,
    parentTip: BranchId,
    lastSnapshotInfo: GlobalSnapshotInfo
  ): IO[GlobalSnapshotInfo] =
    mgr
      .accept(
        ordinal = acceptOrdinal,
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
        lastSnapshotContext = lastSnapshotInfo,
        lastActiveTips = SortedSet.empty,
        lastDeprecatedTips = SortedSet.empty,
        calculateRewardsFn = noopRewardsFn,
        validationType = StateChannelValidationType.Full,
        getGlobalSnapshotByOrdinal = _ => None.pure[IO],
        parentTip = parentTip,
        shardCheckpoints = SortedMap(checkpoint.shardId -> checkpoint)
      )
      .map(_._9)

  /** Did `mg`'s committed currency advance to `nextInfo`? True iff the GSI's `lastCurrencySnapshots(mg)` is a `Right` whose info balances
    * equal `nextInfo.balances` (the discriminating field — base/branch priors and `nextInfo` all carry a distinct single balance).
    */
  private def advancedTo(gsi: GlobalSnapshotInfo, mg: Address, nextInfo: CurrencySnapshotInfo): Boolean =
    gsi.lastCurrencySnapshots.get(mg) match {
      case Some(Right((_, info))) => info.balances == nextInfo.balances
      case _                      => false
    }

  // ============================================================================
  // Shared scenario builder: one MG past first incremental, base prior + a target next.
  // ============================================================================

  private val mg: Address = mkAddress("s0-spine-mg")
  private val holder: Address = mkAddress("s0-spine-holder")
  private val holderBranchExtra: Address = mkAddress("s0-spine-holder-branch-extra")
  private val inc: Signed[CurrencyIncrementalSnapshot] = mkSignedIncremental(snapOrdinal = 2L)

  // Finalized-base currency state: one holder.
  private val basePriorRaw: CurrencySnapshotInfo = infoWithBalance(holder, 100L)

  // State GL0 recreates by executing the included snapshot binary.
  private val nextInfo: CurrencySnapshotInfo = infoWithBalance(holder, 200L)

  // Intervening branch state. The extra holder proves the test is genuinely running above a divergent pending branch while checkpoint
  // adoption still derives its candidate exclusively from replayed binaries.
  private val branchPriorRaw: CurrencySnapshotInfo =
    basePriorRaw.copy(balances = basePriorRaw.balances + (holderBranchExtra -> Balance(NonNegLong.unsafeFrom(999L))))

  private val headBinary: Signed[StateChannelSnapshotBinary] = mkSignedBinary("s0-head-binary".getBytes("UTF-8"))

  // ============================================================================
  // HARNESS PRECONDITION: prove the MultiBranch overlay genuinely expresses branch != base. The same MG reconstructs a different prior at
  // the child branch than at the finalized base, so the replay-adoption test cannot pass on a passthrough-only fixture by accident.
  // ============================================================================

  test("HARNESS: branch reader reconstructs branchPrior, base reader reconstructs basePrior (branch != base is real)") { res =>
    implicit val (h, sp, j, committeeIdentity) = res
    for {
      store <- freshStore
      basePriorRT <- seedBaseAndRoundTrip(store, mg, inc, basePriorRaw)
      ob <- mkOverlayWithBranch(store, mg, inc, basePriorRT, branchPrior = Some(branchPriorRaw))
      (overlay, childTip) = ob
      baseReader = CurrencyInfoMptAdapters.readerFor(GlobalStateReader.fromOverlay[IO](overlay, BranchId.base))
      branchReader = CurrencyInfoMptAdapters.readerFor(GlobalStateReader.fromOverlay[IO](overlay, childTip))
      baseInfo <- GlobalStateConverter.reconstructCurrencyInfoFrom[IO](mg, baseReader)
      branchInfo <- GlobalStateConverter.reconstructCurrencyInfoFrom[IO](mg, branchReader)
    } yield
      expect.all(
        baseInfo.balances == basePriorRaw.balances,
        branchInfo.balances == branchPriorRaw.balances,
        baseInfo.balances != branchInfo.balances // the load-bearing inequality: branch > base
      )
  }

  // ============================================================================
  // Replay-only checkpoint adoption
  // ============================================================================

  test("globally recreated state is adopted on a divergent pending branch") { res =>
    implicit val (h, sp, j, committeeIdentity) = res
    for {
      tipProof <- nextInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))
      localInc = mkSignedIncremental(2L, tipProof)
      store <- freshStore
      basePrior <- seedBaseAndRoundTrip(store, mg, localInc, basePriorRaw)
      claimedRoot <- recreatedRoot(mg, localInc, nextInfo)
      checkpoint <- mkRightArmCheckpoint(mg, headBinary, claimedRoot)
      ob <- mkOverlayWithBranch(store, mg, localInc, basePrior, branchPrior = Some(branchPriorRaw))
      (overlay, childTip) = ob
      callsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      replayCalls <- Ref.of[IO, Int](0)
      mgr <- mkManager(
        overlay,
        derivingProcessor(localInc, nextInfo, onReplay = replayCalls.update(_ + 1)),
        StubAcceptanceManager(callsRef)
      )
      gsi <- runAccept(mgr, checkpoint, childTip, gsiWith(mg, localInc, branchPriorRaw))
      calls <- callsRef.get
      replayCount <- replayCalls.get
    } yield
      expect.all(
        calls == List(checkpoint),
        replayCount == 1,
        checkpoint.derivedStateDelta.includedSnapshots.keySet == Set(mg),
        checkpoint.derivedStateDelta.perMetagraphMptRoots == SortedMap(mg -> claimedRoot),
        advancedTo(gsi, mg, nextInfo),
        gsi.lastStateChannelSnapshotHashes.contains(mg)
      )
  }

  test("currency replay removes branch-only prior entries and converges to the same canonical MPT root") { res =>
    implicit val (h, sp, j, committeeIdentity) = res

    def runFrom(priorOnBranch: Option[CurrencySnapshotInfo]): IO[(GlobalSnapshotInfo, Option[Hash])] =
      for {
        tipProof <- nextInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))
        localInc = mkSignedIncremental(2L, tipProof)
        store <- freshStore
        basePrior <- seedBaseAndRoundTrip(store, mg, localInc, basePriorRaw)
        claimedRoot <- recreatedRoot(mg, localInc, nextInfo)
        checkpoint <- mkRightArmCheckpoint(mg, headBinary, claimedRoot)
        ob <- mkOverlayWithBranch(store, mg, localInc, basePrior, priorOnBranch)
        (overlay, parentTip) = ob
        callsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
        mgr <- mkManager(overlay, derivingProcessor(localInc, nextInfo), StubAcceptanceManager(callsRef))
        lastInfo = gsiWith(mg, localInc, priorOnBranch.getOrElse(basePriorRaw))
        out <- runAcceptWithRoot(mgr, checkpoint, parentTip, lastInfo)
      } yield out

    for {
      fromBase <- runFrom(None)
      fromBranch <- runFrom(Some(branchPriorRaw))
      (baseGsi, baseRoot) = fromBase
      (branchGsi, branchRoot) = fromBranch
    } yield
      expect.all(
        advancedTo(baseGsi, mg, nextInfo),
        advancedTo(branchGsi, mg, nextInfo),
        baseGsi.lastCurrencySnapshots == branchGsi.lastCurrencySnapshots,
        baseRoot.isDefined,
        branchRoot == baseRoot
      )
  }

  test("matching globally recreated root adopts at branch == base") { res =>
    implicit val (h, sp, j, committeeIdentity) = res
    for {
      tipProof <- nextInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))
      localInc = mkSignedIncremental(2L, tipProof)
      store <- freshStore
      basePrior <- seedBaseAndRoundTrip(store, mg, localInc, basePriorRaw)
      claimedRoot <- recreatedRoot(mg, localInc, nextInfo)
      checkpoint <- mkRightArmCheckpoint(mg, headBinary, claimedRoot)
      ob <- mkOverlayWithBranch(store, mg, localInc, basePrior, branchPrior = None)
      (overlay, baseTip) = ob
      callsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      mgr <- mkManager(overlay, derivingProcessor(localInc, nextInfo), StubAcceptanceManager(callsRef))
      gsi <- runAccept(mgr, checkpoint, baseTip, gsiWith(mg, localInc, basePriorRaw))
    } yield expect(advancedTo(gsi, mg, nextInfo))
  }

  test("mismatched claimed root drops globally recreated state and its balance update") { res =>
    implicit val (h, sp, j, committeeIdentity) = res
    for {
      tipProof <- nextInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))
      localInc = mkSignedIncremental(2L, tipProof)
      store <- freshStore
      basePrior <- seedBaseAndRoundTrip(store, mg, localInc, basePriorRaw)
      checkpoint <- mkRightArmCheckpoint(mg, headBinary, Hash("ff" * 32))
      ob <- mkOverlayWithBranch(store, mg, localInc, basePrior, branchPrior = Some(branchPriorRaw))
      (overlay, childTip) = ob
      callsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      replayCalls <- Ref.of[IO, Int](0)
      rejectedBalance = Balance(NonNegLong.unsafeFrom(999999L))
      mgr <- mkManager(
        overlay,
        derivingProcessor(
          localInc,
          nextInfo,
          balanceUpdate = SortedMap(holder -> rejectedBalance),
          onReplay = replayCalls.update(_ + 1)
        ),
        StubAcceptanceManager(callsRef)
      )
      gsi <- runAccept(mgr, checkpoint, childTip, gsiWith(mg, localInc, branchPriorRaw))
      replayCount <- replayCalls.get
    } yield
      expect.all(
        replayCount == 1,
        !advancedTo(gsi, mg, nextInfo),
        !gsi.lastStateChannelSnapshotHashes.contains(mg),
        !gsi.balances.get(holder).contains(rejectedBalance)
      )
  }

  test("missing claimed root drops globally recreated state") { res =>
    implicit val (h, sp, j, committeeIdentity) = res
    for {
      tipProof <- nextInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))
      localInc = mkSignedIncremental(2L, tipProof)
      store <- freshStore
      basePrior <- seedBaseAndRoundTrip(store, mg, localInc, basePriorRaw)
      claimedRoot <- recreatedRoot(mg, localInc, nextInfo)
      checkpoint <- mkRightArmCheckpoint(mg, headBinary, claimedRoot, includeAttestedRoot = false)
      ob <- mkOverlayWithBranch(store, mg, localInc, basePrior, branchPrior = None)
      (overlay, baseTip) = ob
      callsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      mgr <- mkManager(overlay, derivingProcessor(localInc, nextInfo), StubAcceptanceManager(callsRef))
      gsi <- runAccept(mgr, checkpoint, baseTip, gsiWith(mg, localInc, basePriorRaw))
    } yield
      expect.all(
        !advancedTo(gsi, mg, nextInfo),
        !gsi.lastStateChannelSnapshotHashes.contains(mg)
      )
  }

  // ============================================================================
  // Execution-base verifier boundary
  // ============================================================================

  private val executionBase: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))

  test("execution-base pin reaches the verifier unchanged before replay adoption") { res =>
    implicit val (h, sp, j, committeeIdentity) = res
    for {
      tipProof <- nextInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))
      localInc = mkSignedIncremental(2L, tipProof)
      store <- freshStore
      basePrior <- seedBaseAndRoundTrip(store, mg, localInc, basePriorRaw)
      _ <- store.commit(executionBase)
      claimedRoot <- recreatedRoot(mg, localInc, nextInfo)
      checkpoint <- mkRightArmCheckpoint(mg, headBinary, claimedRoot, executionBaseOrdinal = executionBase)
      ob <- mkOverlayWithBranch(store, mg, localInc, basePrior, branchPrior = None)
      (overlay, baseTip) = ob
      callsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      replayCalls <- Ref.of[IO, Int](0)
      verifier = StubAcceptanceManager(
        callsRef,
        cp =>
          if (cp.executionBaseOrdinal === executionBase) ShardCheckpointAcceptResult.Accepted
          else ShardCheckpointAcceptResult.Rejected("wrong execution base")
      )
      mgr <- mkManager(
        overlay,
        derivingProcessor(localInc, nextInfo, onReplay = replayCalls.update(_ + 1)),
        verifier
      )
      gsi <- runAccept(mgr, checkpoint, baseTip, gsiWith(mg, localInc, basePriorRaw))
      calls <- callsRef.get
      replayCount <- replayCalls.get
    } yield
      expect.all(
        calls.map(_.executionBaseOrdinal) == List(executionBase),
        replayCount == 1,
        advancedTo(gsi, mg, nextInfo)
      )
  }

  test("unavailable execution base rejects before replay and performs no metagraph write") { res =>
    implicit val (h, sp, j, committeeIdentity) = res
    for {
      tipProof <- nextInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))
      localInc = mkSignedIncremental(2L, tipProof)
      store <- freshStore
      basePrior <- seedBaseAndRoundTrip(store, mg, localInc, basePriorRaw)
      claimedRoot <- recreatedRoot(mg, localInc, nextInfo)
      checkpoint <- mkRightArmCheckpoint(mg, headBinary, claimedRoot, executionBaseOrdinal = executionBase)
      ob <- mkOverlayWithBranch(store, mg, localInc, basePrior, branchPrior = None)
      (overlay, baseTip) = ob
      callsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      verifier = StubAcceptanceManager(
        callsRef,
        _ => ShardCheckpointAcceptResult.Rejected("execution base unavailable")
      )
      failIfReplayed = IO.raiseError[Unit](new AssertionError("replay ran despite unavailable execution base"))
      mgr <- mkManager(
        overlay,
        derivingProcessor(localInc, nextInfo, onReplay = failIfReplayed),
        verifier
      )
      gsi <- runAccept(mgr, checkpoint, baseTip, gsiWith(mg, localInc, basePriorRaw))
      afterInfo <- GlobalStateConverter.reconstructCurrencyInfoFrom[IO](
        mg,
        CurrencyInfoMptAdapters.readerFor(GlobalStateReader.fromOverlay[IO](overlay, BranchId.base))
      )
      calls <- callsRef.get
    } yield
      expect.all(
        calls.map(_.executionBaseOrdinal) == List(executionBase),
        !advancedTo(gsi, mg, nextInfo),
        !gsi.lastStateChannelSnapshotHashes.contains(mg),
        afterInfo.balances == basePrior.balances
      )
  }

  private def runAcceptWithRoot(
    mgr: GlobalSnapshotAcceptanceManager[IO],
    checkpoint: ShardCheckpoint,
    parentTip: BranchId,
    lastSnapshotInfo: GlobalSnapshotInfo
  ): IO[(GlobalSnapshotInfo, Option[Hash])] =
    mgr
      .accept(
        ordinal = acceptOrdinal,
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
        lastSnapshotContext = lastSnapshotInfo,
        lastActiveTips = SortedSet.empty,
        lastDeprecatedTips = SortedSet.empty,
        calculateRewardsFn = noopRewardsFn,
        validationType = StateChannelValidationType.Full,
        getGlobalSnapshotByOrdinal = _ => None.pure[IO],
        parentTip = parentTip,
        shardCheckpoints = SortedMap(checkpoint.shardId -> checkpoint)
      )
      .map(result => (result._9, result._10.mptRoot))

  test("numShards=1 ignores checkpoint roots and never enters replay adoption") { res =>
    implicit val (h, sp, j, committeeIdentity) = res

    def runNode(claimedRoot: Hash): IO[(GlobalSnapshotInfo, Option[Hash], List[ShardCheckpoint])] =
      for {
        tipProof <- nextInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))
        localInc = mkSignedIncremental(2L, tipProof)
        store <- freshStore
        basePrior <- seedBaseAndRoundTrip(store, mg, localInc, basePriorRaw)
        checkpoint <- mkRightArmCheckpoint(mg, headBinary, claimedRoot, executionBaseOrdinal = executionBase)
        ob <- mkOverlayWithBranch(store, mg, localInc, basePrior, branchPrior = None)
        (overlay, baseTip) = ob
        callsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
        failIfReplayed = IO.raiseError[Unit](new AssertionError("replay adoption ran at numShards=1"))
        mgr <- mkManager(
          overlay,
          derivingProcessor(localInc, nextInfo, onReplay = failIfReplayed),
          StubAcceptanceManager(callsRef),
          numShards = 1
        )
        out <- runAcceptWithRoot(mgr, checkpoint, baseTip, gsiWith(mg, localInc, basePriorRaw))
        calls <- callsRef.get
      } yield (out._1, out._2, calls)

    for {
      a <- runNode(Hash("11" * 32))
      b <- runNode(Hash("22" * 32))
      (gsiA, rootA, callsA) = a
      (gsiB, rootB, callsB) = b
    } yield
      expect.all(
        callsA.isEmpty,
        callsB.isEmpty,
        gsiA == gsiB,
        rootA == rootB,
        rootA.isDefined,
        !advancedTo(gsiA, mg, nextInfo)
      )
  }

}
