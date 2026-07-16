package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotStateChannelEventsProcessorSuite
import io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotStateChannelEventsProcessorSuite.ProcessorHarness
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{GlobalStateReader, PinnedCurrencyInfoReader}
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.{
  InvalidStateProofBatchReplay,
  InvalidStateProofSlashedReader,
  InvalidStateProofValidator
}
import io.constellationnetwork.node.shared.domain.statechannel.FeeCalculatorConfig
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.TimeTrigger
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.sharding.{
  RegisteredCheckpointSigner,
  ShardCheckpointWiring,
  TestCheckpointDutyValidator
}
import io.constellationnetwork.node.shared.infrastructure.snapshot.CurrencySnapshotContextFunctions
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardCheckpointGl0AcceptanceManager
import io.constellationnetwork.node.shared.snapshot.currency.CurrencySnapshotEvent
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageOrdinal, MessageType}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, GlobalSnapshotStateRef}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.slashing.{InvalidStateProofEvidence, InvalidStateProofRejection}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.MptRoot
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.mpt.storages.MptStateStorage
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.addressSetImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutableCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.shared.sharedKryoRegistrar
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import fs2.io.file.Files
import weaver.MutableIOSuite

/** The checkpoint execution prior is the wire-carried `executionBase`, never a node's mutable live MPT view.
  *
  * Every window in this suite is a real currency transition: the next incremental is created by the same
  * [[io.constellationnetwork.node.shared.infrastructure.snapshot.CurrencySnapshotCreator]] used by the verifier, and both the incremental
  * and its state-channel envelope carry valid signatures from the metagraph key. This keeps the forcing assertions on the economic
  * recreation path instead of the `Hash.empty` cannot-derive sentinel.
  */
object ExecutionBasePinReExecutionSuite extends MutableIOSuite {

  override type Res = (KryoSerializer[IO], Hasher[IO], JsonSerializer[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (ks, h, j, sp)

  implicit val stateProofSelector: StateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(0L)))
  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  private def addr(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))
  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal.unsafeApply(n)

  private val account: Address = addr("execution-base-acct-1")
  private val executionBaseOrdinal: SnapshotOrdinal = ord(10L)
  private val liveAhead: SnapshotOrdinal = ord(13L)
  private val anchorOrd: SnapshotOrdinal = ord(1000L)
  private val shardZero: ShardId = ShardId.unsafeApply(0)
  private val epochZero: EtaPeriod = EtaPeriod(0L)
  private val baseStateChannelTip: Hash = Hash("a1" * 32)

  private def unsignedIncremental(snapOrdinal: Long): CurrencyIncrementalSnapshot =
    CurrencyIncrementalSnapshot(
      ordinal = SnapshotOrdinal.unsafeApply(snapOrdinal),
      height = Height.MinValue,
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      rewards = SortedSet.empty,
      tips = SnapshotTips(
        SortedSet.empty,
        SortedSet(
          ActiveTip(
            BlockReference(Height.MinValue, ProofsHash("0" * 64)),
            NonNegLong.MinValue,
            SnapshotOrdinal.MinValue
          )
        )
      ),
      stateProof = CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None),
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

  private def signedIncremental(
    snapOrdinal: Long,
    metagraphKey: KeyPair
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[Signed[CurrencyIncrementalSnapshot]] =
    forAsyncHasher[IO, CurrencyIncrementalSnapshot](unsignedIncremental(snapOrdinal), metagraphKey)

  private def info(balance: Long): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap.empty,
      balances = SortedMap(account -> Balance(NonNegLong.unsafeFrom(balance))),
      lastMessages = None,
      lastFeeTxRefs = None,
      lastAllowSpendRefs = None,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = None,
      activeTokenLocks = None
    )

  private val baseInfo: CurrencySnapshotInfo = info(100L)
  private val aheadInfo: CurrencySnapshotInfo = info(250L)

  private type MgState = SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]

  private final case class PinnedHistory(
    reader: PinnedCurrencyInfoReader[IO],
    stateRef: GlobalSnapshotStateRef
  )

  private def mgState(mg: Address, baseIncremental: Signed[CurrencyIncrementalSnapshot], currencyInfo: CurrencySnapshotInfo): MgState =
    SortedMap(mg -> Right((baseIncremental, currencyInfo)))

  private def mkLiveStore(
    state: MgState,
    atOrdinal: SnapshotOrdinal
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      bytes <- GlobalStateConverter.currencySnapshotMgEntries[IO](state)
      producer <- InMemoryMerklePatriciaProducer.make[IO](bytes)
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      mg = state.firstKey
      _ <- store.insert[Hash](
        GlobalStateKey.metagraph(mg, GlobalStateFieldId.LastStateChannelSnapshotHashes),
        baseStateChannelTip
      )
      indexKey <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.LastStateChannelSnapshotHashes)
      _ <- store.insert[SortedSet[Address]](indexKey, SortedSet(mg))
      currencyIndexKey <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.LastCurrencySnapshots)
      _ <- store.insert[SortedSet[Address]](currencyIndexKey, SortedSet(mg))
      _ <- store.commit(atOrdinal)
    } yield store

  private def mkHashed(ordinal: SnapshotOrdinal, mptRoot: Option[Hash])(implicit h: Hasher[IO]): IO[Hashed[GlobalIncrementalSnapshot]] = {
    val unsigned = GlobalIncrementalSnapshot(
      ordinal = ordinal,
      height = Height(NonNegLong(0L)),
      subHeight = SubHeight(NonNegLong(0L)),
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      shardCheckpoints = SortedMap.empty,
      rewards = SortedSet.empty,
      delegateRewards = None,
      epochProgress = EpochProgress(NonNegLong(0L)),
      nextFacilitators = NonEmptyList.of(PeerId(Hex("0d" * 64))),
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = GlobalSnapshotStateProof(
        Hash.empty,
        Hash.empty,
        Hash.empty,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        mptRoot,
        None,
        None
      ),
      allowSpendBlocks = None,
      tokenLockBlocks = None,
      spendActions = None,
      updateNodeParameters = None,
      artifacts = None,
      activeDelegatedStakes = None,
      delegatedStakesWithdrawals = None,
      activeNodeCollaterals = None,
      nodeCollateralWithdrawals = None,
      version = SnapshotVersion("0.0.1"),
      slotCertificate = None,
      eta = None
    )
    Signed(unsigned, NonEmptySet.of(SignatureProof(PeerId(Hex("0d" * 64)).toId, Signature(Hex("0e" * 64)))))
      .toHashed[IO]
  }

  private def mkPinnedHistoryForState(
    dir: fs2.io.file.Path,
    state: MgState,
    stateChannelTips: SortedMap[Address, Hash],
    stateChannelTipIndex: SortedSet[Address],
    currencyIndex: SortedSet[Address],
    globalBalances: SortedMap[Address, Balance]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[PinnedHistory] =
    for {
      baseBytes <- GlobalStateConverter.currencySnapshotMgEntries[IO](state)
      producer <- InMemoryMerklePatriciaProducer.make[IO](baseBytes)
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- stateChannelTips.toList.traverse_ {
        case (address, tip) =>
          store.insert[Hash](GlobalStateKey.metagraph(address, GlobalStateFieldId.LastStateChannelSnapshotHashes), tip)
      }
      stateChannelIndexKey <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.LastStateChannelSnapshotHashes)
      _ <- store.insert[SortedSet[Address]](stateChannelIndexKey, stateChannelTipIndex)
      currencyIndexKey <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.LastCurrencySnapshots)
      _ <- store.insert[SortedSet[Address]](currencyIndexKey, currencyIndex)
      balanceIndexKey <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.Balances)
      _ <- globalBalances.toList.traverse_ {
        case (address, balance) =>
          store.insert[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, address), balance)
      }
      _ <- store.insert[SortedSet[Address]](balanceIndexKey, globalBalances.keySet.to(SortedSet))
      pinnedBytes <- store.allEntriesAsBytes
      baseRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](pinnedBytes)
      byteStore <- MptStateStorage.make[IO](dir)
      _ <- byteStore.writeState(executionBaseOrdinal, pinnedBytes)
      pinnedSnap <- mkHashed(executionBaseOrdinal, Some(baseRoot))
      resolver = (o: SnapshotOrdinal) => (if (o === executionBaseOrdinal) pinnedSnap.some else none).pure[IO]
      reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
      stateRef = GlobalSnapshotStateRef(
        executionBaseOrdinal,
        pinnedSnap.hash,
        pinnedSnap.signed.value.lastSnapshotHash,
        MptRoot(baseRoot)
      )
    } yield PinnedHistory(reader, stateRef)

  private def mkPinnedHistory(
    dir: fs2.io.file.Path,
    mg: Address,
    baseIncremental: Signed[CurrencyIncrementalSnapshot],
    includeStateChannelTipTarget: Boolean = true,
    includeStateChannelTipIndexMember: Boolean = true,
    includeCurrencyIndexMember: Boolean = true,
    globalBalances: SortedMap[Address, Balance] = SortedMap.empty
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[PinnedHistory] =
    mkPinnedHistoryForState(
      dir,
      mgState(mg, baseIncremental, baseInfo),
      Option.when(includeStateChannelTipTarget)(mg -> baseStateChannelTip).toList.to(SortedMap),
      if (includeStateChannelTipIndexMember) SortedSet(mg) else SortedSet.empty,
      if (includeCurrencyIndexMember) SortedSet(mg) else SortedSet.empty,
      globalBalances
    )

  private def liveReaderAt(live: MptStore[IO, GlobalStateKey]): GlobalSnapshotStateRef => IO[Option[GlobalStateReader[IO]]] =
    _ => GlobalStateReader.fromMptStore[IO](live).some.pure[IO]

  private def productionReaderAt(pinned: PinnedHistory): GlobalSnapshotStateRef => IO[Option[GlobalStateReader[IO]]] =
    ShardCheckpointWiring.pinnedPriorReaderAt[IO](pinned.reader)

  private def globalSnapshotLookup(
    snapshot: Hashed[GlobalIncrementalSnapshot]
  ): SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]] =
    ordinal => Option.when(snapshot.ordinal === ordinal)(snapshot).pure[IO]

  private def mkReplay(
    harness: ProcessorHarness,
    readerAt: GlobalSnapshotStateRef => IO[Option[GlobalStateReader[IO]]]
  )(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, GlobalSnapshotStateRef) => IO[Option[Hash]] =
    ShardCheckpointWiring.reExecDerivationAtPinnedBase[IO](
      harness.processor,
      readerAt,
      globalSnapshotLookup(harness.initialGlobalSnapshot)
    )

  private def mkRealWindow(
    harness: ProcessorHarness,
    mg: Address,
    metagraphKey: KeyPair,
    baseIncremental: Signed[CurrencyIncrementalSnapshot]
  )(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO],
    sp: SecurityProvider[IO]
  ): IO[NonEmptyList[Signed[StateChannelSnapshotBinary]]] =
    for {
      creation <- harness.creator.createProposalArtifact(
        lastKey = baseIncremental.ordinal,
        lastArtifact = baseIncremental,
        lastContext = CurrencySnapshotContext(mg, baseInfo),
        lastArtifactHasher = Hasher.forJson[IO],
        trigger = TimeTrigger,
        events = Set.empty[CurrencySnapshotEvent],
        rewards = None,
        facilitators = Set(PeerId.fromPublic(metagraphKey.getPublic)),
        feeTransactionFn = None,
        artifactsFn = None,
        getGlobalSnapshotByOrdinal = globalSnapshotLookup(harness.initialGlobalSnapshot),
        shouldPerformMetagraphSpecificValidations = false,
        maybeCustomArtifacts = None,
        pinnedGlobalSyncView = None
      )
      signedNext <- forAsyncHasher[IO, CurrencyIncrementalSnapshot](creation.artifact, metagraphKey)
      contentBytes <- JsonSerializer[IO].serialize(signedNext)
      binary <- forAsyncHasher[IO, StateChannelSnapshotBinary](
        StateChannelSnapshotBinary(baseStateChannelTip, contentBytes, SnapshotFee.MinValue),
        metagraphKey
      )
    } yield NonEmptyList.one(binary)

  private def mkPositiveFeeWindow(
    metagraphKey: KeyPair,
    ownerMessage: Signed[CurrencyMessage],
    fee: SnapshotFee
  )(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO],
    sp: SecurityProvider[IO]
  ): IO[(Signed[CurrencyIncrementalSnapshot], NonEmptyList[Signed[StateChannelSnapshotBinary]])] =
    for {
      firstValue <- CurrencyIncrementalSnapshot.fromCurrencySnapshot[IO](CurrencySnapshot.mkGenesis(Map.empty, None, None))(
        implicitly[cats.Parallel[IO]],
        implicitly[cats.effect.Async[IO]],
        h,
        js,
        CurrencyStateProofSelector.instance
      )
      first <- forAsyncHasher[IO, CurrencyIncrementalSnapshot](firstValue, metagraphKey)
      firstHash <- first.toHashed[IO].map(_.hash)
      secondValue = firstValue.copy(
        ordinal = SnapshotOrdinal.unsafeApply(1L),
        lastSnapshotHash = firstHash,
        messages = Some(SortedSet(ownerMessage))
      )
      second <- forAsyncHasher[IO, CurrencyIncrementalSnapshot](secondValue, metagraphKey)
      content <- js.serialize(second)
      binary <- forAsyncHasher[IO, StateChannelSnapshotBinary](
        StateChannelSnapshotBinary(baseStateChannelTip, content, fee),
        metagraphKey
      )
    } yield first -> NonEmptyList.one(binary)

  private final case class RegisteredCheckpoint(
    checkpoint: ShardCheckpoint,
    checkpointSigner: RegisteredCheckpointSigner,
    acceptanceManager: ShardCheckpointGl0AcceptanceManager[IO]
  )

  private def mkCheckpoint(
    mg: Address,
    window: NonEmptyList[Signed[StateChannelSnapshotBinary]],
    attestedRoot: Hash,
    executionBase: GlobalSnapshotStateRef
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[RegisteredCheckpoint] =
    for {
      checkpointSigner <- RegisteredCheckpointSigner.make
      committeeKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      committeeId = PeerId.fromPublic(committeeKeyPair.getPublic)
      _ <- checkpointSigner.preregisterGenesis(committeeKeyPair, committeeId)
      placeholder = CommitteeMemberSignature(committeeId, Hex(""), Hex(""), Hex(""), 0)
      shell = ShardCheckpoint(
        shardId = shardZero,
        parentCheckpointHash = Hash("0" * 64),
        shardOrdinal = ShardOrdinal(1L),
        gl0AnchorOrdinal = anchorOrd,
        slot = SlotT.unsafeApply(1L),
        derivedStateDelta = ShardDerivedStateDelta(
          perMetagraphMptRoots = SortedMap(mg -> attestedRoot),
          includedSnapshots = SortedMap(mg -> window)
        ),
        committeeSignatures = NonEmptyList.one(placeholder),
        epoch = epochZero,
        executionBase = executionBase
      )
      signature <- checkpointSigner.sign(shell, committeeKeyPair, committeeId, checkpointSigner.defaultShardEta)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.one(signature))
      acceptanceManager <- ShardCheckpointGl0AcceptanceManager.make[IO](
        executionQuorum = 1,
        etaRotationSnapshots = 2550L,
        committeeMembership = (_, _) => IO.pure(Set(committeeId)),
        operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
        shardAssignment = ShardAssignment.make[IO](numShards = 1),
        shardEtaFor = (_, _) => IO.pure(Some(checkpointSigner.defaultShardEta)),
        producerDutyValidator = TestCheckpointDutyValidator.allow[IO],
        reExecuteDerivation = (_, _, _, _) => IO.pure(attestedRoot)
      )
      certificate <- acceptanceManager.verifyExecutionCertificate(checkpoint)
      _ <- IO.fromEither(
        certificate.leftMap(reason => new IllegalStateException(s"execution-base checkpoint fixture rejected: $reason"))
      )
    } yield RegisteredCheckpoint(checkpoint, checkpointSigner, acceptanceManager)

  private def mkEvidence(
    cp: ShardCheckpoint,
    mg: Address,
    challengerKp: KeyPair,
    submitterId: PeerId,
    claimed: Hash,
    challengerRoot: Hash
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[InvalidStateProofEvidence] =
    h.hash(cp.signingPreimage).flatMap { cpHash =>
      val unsigned = FraudProofEnvelope(
        shardId = cp.shardId,
        disputedCheckpointHash = cpHash,
        metagraphAddress = mg,
        gl0AnchorOrdinal = cp.gl0AnchorOrdinal,
        claimedDerivation = claimed,
        challengerDerivation = challengerRoot,
        reexecutionWitness = Hex(challengerRoot.value),
        challengerSignature = Hex(""),
        submitterId = submitterId
      )
      h.hash(unsigned.signingPreimage).flatMap { digest =>
        Signing.signData[IO](digest.getBytes)(challengerKp.getPrivate).map { sig =>
          InvalidStateProofEvidence(
            shardId = cp.shardId,
            disputedCheckpoint = cp,
            metagraphAddress = mg,
            attestedRoot = claimed,
            fraudProof = unsigned.copy(challengerSignature = Hex.fromBytes(sig))
          )
        }
      }
    }

  test("pinned execution base: nodes with different live balances recreate the same non-empty currency root") { res =>
    implicit val (ks, h, js, sp) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
        mg = PublicKeyOps(metagraphKey.getPublic).toAddress
        baseIncremental <- signedIncremental(5L, metagraphKey)
        harness <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessorHarness(Map.empty)
        window <- mkRealWindow(harness, mg, metagraphKey, baseIncremental)
        pinned <- mkPinnedHistory(dir, mg, baseIncremental)
        liveAtBase <- mkLiveStore(mgState(mg, baseIncremental, baseInfo), executionBaseOrdinal)
        liveAheadStore <- mkLiveStore(mgState(mg, baseIncremental, aheadInfo), liveAhead)
        liveBaseReplay = mkReplay(harness, liveReaderAt(liveAtBase))
        liveAheadReplay = mkReplay(harness, liveReaderAt(liveAheadStore))
        pinnedReplay = mkReplay(harness, productionReaderAt(pinned))
        liveBaseRoot <- liveBaseReplay(mg, window, anchorOrd, pinned.stateRef)
        liveAheadRoot <- liveAheadReplay(mg, window, anchorOrd, pinned.stateRef)
        pinnedBaseRoot <- pinnedReplay(mg, window, anchorOrd, pinned.stateRef)
        pinnedAheadRoot <- pinnedReplay(mg, window, anchorOrd, pinned.stateRef)
      } yield
        expect.all(
          liveBaseRoot.exists(_ =!= Hash.empty),
          liveBaseRoot =!= liveAheadRoot,
          pinnedBaseRoot.exists(_ =!= Hash.empty),
          pinnedAheadRoot.exists(_ =!= Hash.empty),
          pinnedBaseRoot === pinnedAheadRoot
        )
    }
  }

  test("checkpoint replay derives no root when a valid currency transition is followed by an undecodable signed child") { res =>
    implicit val (ks, h, js, sp) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
        mg = PublicKeyOps(metagraphKey.getPublic).toAddress
        baseIncremental <- signedIncremental(5L, metagraphKey)
        harness <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessorHarness(Map.empty)
        validWindow <- mkRealWindow(harness, mg, metagraphKey, baseIncremental)
        validBinaryHash <- Hasher[IO].hash(validWindow.last.value)
        invalidChild <- forAsyncHasher[IO, StateChannelSnapshotBinary](
          StateChannelSnapshotBinary(validBinaryHash, Array[Byte](0x01, 0x02, 0x03), SnapshotFee.MinValue),
          metagraphKey
        )
        incompleteWindow = NonEmptyList.fromListUnsafe(validWindow.toList :+ invalidChild)
        pinned <- mkPinnedHistory(dir, mg, baseIncremental)
        replay = mkReplay(harness, productionReaderAt(pinned))
        validRoot <- replay(mg, validWindow, anchorOrd, pinned.stateRef)
        incompleteRoot <- replay(mg, incompleteWindow, anchorOrd, pinned.stateRef)
      } yield
        expect.all(
          validRoot.exists(_ =!= Hash.empty),
          // The processor can recreate the valid prefix, but the shared checkpoint boundary withholds it from root/signature callers.
          incompleteRoot.isEmpty
        )
    }
  }

  test("mid-fold skew: live content ahead of its watermark cannot change the pinned replay root") { res =>
    implicit val (ks, h, js, sp) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
        mg = PublicKeyOps(metagraphKey.getPublic).toAddress
        baseIncremental <- signedIncremental(5L, metagraphKey)
        harness <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessorHarness(Map.empty)
        window <- mkRealWindow(harness, mg, metagraphKey, baseIncremental)
        pinned <- mkPinnedHistory(dir, mg, baseIncremental)
        skewedLive <- mkLiveStore(mgState(mg, baseIncremental, aheadInfo), executionBaseOrdinal)
        unpinnedReplay = mkReplay(harness, liveReaderAt(skewedLive))
        pinnedReplay = mkReplay(harness, productionReaderAt(pinned))
        unpinnedSkewRoot <- unpinnedReplay(mg, window, anchorOrd, pinned.stateRef)
        pinnedRoot <- pinnedReplay(mg, window, anchorOrd, pinned.stateRef)
      } yield
        expect.all(
          pinnedRoot.exists(_ =!= Hash.empty),
          unpinnedSkewRoot =!= pinnedRoot
        )
    }
  }

  test("honest checkpoint is not an InvalidStateProof target when the follower live tip is ahead of executionBase") { res =>
    implicit val (ks, h, js, sp) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
        mg = PublicKeyOps(metagraphKey.getPublic).toAddress
        baseIncremental <- signedIncremental(5L, metagraphKey)
        harness <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessorHarness(Map.empty)
        window <- mkRealWindow(harness, mg, metagraphKey, baseIncremental)
        pinned <- mkPinnedHistory(dir, mg, baseIncremental)
        followerLive <- mkLiveStore(mgState(mg, baseIncremental, aheadInfo), liveAhead)
        replay = mkReplay(harness, productionReaderAt(pinned))
        attestedRootOpt <- replay(mg, window, anchorOrd, pinned.stateRef)
        attestedRoot = attestedRootOpt.getOrElse(Hash.empty)
        checkpointRig <- mkCheckpoint(mg, window, attestedRoot, pinned.stateRef)
        checkpoint = checkpointRig.checkpoint
        followerReplay = ShardCheckpointWiring.reExecCheckpointAtPinnedBase[IO](
          harness.processor,
          productionReaderAt(pinned),
          globalSnapshotLookup(harness.initialGlobalSnapshot)
        )
        validator = InvalidStateProofValidator.make[IO](
          followerReplay,
          InvalidStateProofSlashedReader.neverSlashed[IO],
          checkpointRig.acceptanceManager.verifyExecutionCertificate
        )
        challengerKp <- KeyPairGenerator.makeKeyPair[IO]
        challengerId = PeerId.fromPublic(challengerKp.getPublic)
        _ <- checkpointRig.checkpointSigner.preregisterGenesis(challengerKp, challengerId)
        evidence <- mkEvidence(checkpoint, mg, challengerKp, challengerId, attestedRoot, Hash("b" * 64))
        verdict <- validator.validate(evidence)
        liveAheadOrdinal <- followerLive.lastPersistedOrdinal
      } yield
        expect.all(
          liveAheadOrdinal.contains(liveAhead),
          attestedRoot =!= Hash.empty,
          verdict == Left(InvalidStateProofRejection.DisputeNotUpheld(attestedRoot, attestedRoot))
        )
    }
  }

  test("indexed parent-tip gap defers replay and cannot become watchtower fraud or slash evidence") { res =>
    implicit val (ks, h, js, sp) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
        mg = PublicKeyOps(metagraphKey.getPublic).toAddress
        baseIncremental <- signedIncremental(5L, metagraphKey)
        harness <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessorHarness(Map.empty)
        window <- mkRealWindow(harness, mg, metagraphKey, baseIncremental)
        corruptPinned <- mkPinnedHistory(dir, mg, baseIncremental, includeStateChannelTipTarget = false)
        replay = mkReplay(harness, productionReaderAt(corruptPinned))
        replayed <- replay(mg, window, anchorOrd, corruptPinned.stateRef)
        attestedRoot = Hash("c3" * 32)
        checkpointRig <- mkCheckpoint(mg, window, attestedRoot, corruptPinned.stateRef)
        checkpoint = checkpointRig.checkpoint
        validator = InvalidStateProofValidator.make[IO](
          ShardCheckpointWiring.reExecCheckpointAtPinnedBase[IO](
            harness.processor,
            productionReaderAt(corruptPinned),
            globalSnapshotLookup(harness.initialGlobalSnapshot)
          ),
          InvalidStateProofSlashedReader.neverSlashed[IO],
          checkpointRig.acceptanceManager.verifyExecutionCertificate
        )
        challengerKp <- KeyPairGenerator.makeKeyPair[IO]
        challengerId = PeerId.fromPublic(challengerKp.getPublic)
        _ <- checkpointRig.checkpointSigner.preregisterGenesis(challengerKp, challengerId)
        evidence <- mkEvidence(checkpoint, mg, challengerKp, challengerId, attestedRoot, Hash("d4" * 32))
        verdict <- validator.validate(evidence)
      } yield
        expect.all(
          replayed.isEmpty,
          verdict == Left(InvalidStateProofRejection.CannotRederive(mg))
        )
    }
  }

  test("checkpoint fraud replay maps swallowed currency-recreation failures to Unavailable, never proven invalidity") { res =>
    implicit val (ks, h, js, sp) = res
    Files[IO].tempDirectory.use { dir =>
      val retainedInputFailure = new IllegalStateException("retained GL0 dependency unavailable")
      val failingContextFns = new CurrencySnapshotContextFunctions[IO] {
        def createContext(
          context: CurrencySnapshotContext,
          lastArtifact: Signed[CurrencyIncrementalSnapshot],
          signedArtifact: Signed[CurrencyIncrementalSnapshot],
          getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
        )(implicit hasher: Hasher[IO]): IO[CurrencySnapshotContext] = IO.raiseError(retainedInputFailure)
      }

      for {
        metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
        mg = PublicKeyOps(metagraphKey.getPublic).toAddress
        baseIncremental <- signedIncremental(5L, metagraphKey)
        creatorHarness <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessorHarness(Map.empty)
        window <- mkRealWindow(creatorHarness, mg, metagraphKey, baseIncremental)
        failingHarness <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessorHarness(
          Map.empty,
          contextFnsOverride = failingContextFns.some
        )
        pinned <- mkPinnedHistory(dir, mg, baseIncremental)
        replay = ShardCheckpointWiring.reExecCheckpointAtPinnedBase[IO](
          failingHarness.processor,
          productionReaderAt(pinned),
          globalSnapshotLookup(failingHarness.initialGlobalSnapshot)
        )
        result <- replay(SortedMap(mg -> window), anchorOrd, pinned.stateRef)
      } yield expect(result == InvalidStateProofBatchReplay.Unavailable)
    }
  }

  test("one-sided pinned currency/tip presence cannot enter shard currency replay") { res =>
    implicit val (ks, h, js, sp) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
        mg = PublicKeyOps(metagraphKey.getPublic).toAddress
        baseIncremental <- signedIncremental(5L, metagraphKey)
        harness <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessorHarness(Map.empty)
        window <- mkRealWindow(harness, mg, metagraphKey, baseIncremental)
        currencyOnly <- mkPinnedHistory(
          dir / "currency-only",
          mg,
          baseIncremental,
          includeStateChannelTipTarget = false,
          includeStateChannelTipIndexMember = false
        )
        tipOnly <- mkPinnedHistory(
          dir / "tip-only",
          mg,
          baseIncremental,
          includeCurrencyIndexMember = false
        )
        currencyOnlyReplay = mkReplay(harness, productionReaderAt(currencyOnly))
        tipOnlyReplay = mkReplay(harness, productionReaderAt(tipOnly))
        currencyOnlyRoot <- currencyOnlyReplay(mg, window, anchorOrd, currencyOnly.stateRef)
        tipOnlyRoot <- tipOnlyReplay(mg, window, anchorOrd, tipOnly.stateRef)
      } yield expect.all(currencyOnlyRoot.isEmpty, tipOnlyRoot.isEmpty)
    }
  }

  test("receiver batch replay strictly materializes one pinned base for the complete multi-MG checkpoint") { res =>
    implicit val (ks, h, js, sp) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
        mg = PublicKeyOps(metagraphKey.getPublic).toAddress
        otherMg = addr("batch-second-mg")
        baseIncremental <- signedIncremental(5L, metagraphKey)
        harness <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessorHarness(Map.empty)
        window <- mkRealWindow(harness, mg, metagraphKey, baseIncremental)
        pinned <- mkPinnedHistory(dir, mg, baseIncremental)
        delegateOpt <- productionReaderAt(pinned)(pinned.stateRef)
        delegate <- IO.fromOption(delegateOpt)(new IllegalStateException("missing retained test reader"))
        currencyIndex <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.LastCurrencySnapshots)
        tipIndex <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.LastStateChannelSnapshotHashes)
        balanceIndex <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.Balances)
        currencyIndexReads <- Ref.of[IO, Int](0)
        tipIndexReads <- Ref.of[IO, Int](0)
        balanceIndexReads <- Ref.of[IO, Int](0)
        counting = new GlobalStateReader[IO] {
          def get[V: ImmutableCodec](key: GlobalStateKey): IO[Option[V]] = delegate.get[V](key)
          def getStrict[V: ImmutableCodec](key: GlobalStateKey): IO[StrictMptRead[V]] =
            currencyIndexReads.update(_ + 1).whenA(key === currencyIndex) >>
              tipIndexReads.update(_ + 1).whenA(key === tipIndex) >>
              balanceIndexReads.update(_ + 1).whenA(key === balanceIndex) >>
              delegate.getStrict[V](key)
          def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): IO[Map[GlobalStateKey, V]] = delegate.getMany[V](keys)
          def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] = delegate.getAllForPrefix[V](prefix)
          def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): IO[List[StrictMptEntry[V]]] =
            delegate.getAllForPrefixStrict[V](prefix)
        }
        replay = ShardCheckpointWiring.reExecDerivationsAtPinnedBaseBatch[IO](
          harness.processor,
          _ => counting.some.pure[IO],
          globalSnapshotLookup(harness.initialGlobalSnapshot)
        )
        roots <- replay(SortedMap(mg -> window, otherMg -> window), anchorOrd, pinned.stateRef)
        currencyReads <- currencyIndexReads.get
        tipReads <- tipIndexReads.get
        balanceReads <- balanceIndexReads.get
      } yield
        expect.all(
          roots.keySet == Set(mg, otherMg),
          roots.get(mg).flatten.isEmpty,
          roots.get(otherMg).flatten.isEmpty,
          currencyReads == 1,
          tipReads == 1,
          balanceReads == 1
        )
    }
  }

  test("checkpoint batch serializes a shared positive-fee payer and withholds every root on overspend") { res =>
    implicit val (ks, h, js, sp) = res

    val fee = SnapshotFee(10L)
    val feeConfigs = SortedMap(
      SnapshotOrdinal.MinValue -> FeeCalculatorConfig(
        baseFee = 1L,
        stakingWeight = BigDecimal(0),
        computationalCost = 1L,
        proWeight = BigDecimal(0)
      )
    )
    val emptyInfo = CurrencySnapshotInfo(
      lastTxRefs = SortedMap.empty,
      balances = SortedMap.empty,
      lastMessages = None,
      lastFeeTxRefs = None,
      lastAllowSpendRefs = None,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = None,
      activeTokenLocks = None
    )
    val contextFns = new CurrencySnapshotContextFunctions[IO] {
      def createContext(
        context: CurrencySnapshotContext,
        lastArtifact: Signed[CurrencyIncrementalSnapshot],
        signedArtifact: Signed[CurrencyIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[CurrencySnapshotContext] = {
        val ownerMessage = signedArtifact.value.messages.flatMap(_.find(_.value.messageType === MessageType.Owner))
        val lastMessages = ownerMessage.map(message => SortedMap[MessageType, Signed[CurrencyMessage]](MessageType.Owner -> message))
        context.copy(snapshotInfo = context.snapshotInfo.copy(lastMessages = lastMessages)).pure[IO]
      }
    }

    Files[IO].tempDirectory.use { dir =>
      for {
        payerKey <- KeyPairGenerator.makeKeyPair[IO]
        metagraphKeyA <- KeyPairGenerator.makeKeyPair[IO]
        metagraphKeyB <- KeyPairGenerator.makeKeyPair[IO]
        payer = payerKey.getPublic.toAddress
        mgA = metagraphKeyA.getPublic.toAddress
        mgB = metagraphKeyB.getPublic.toAddress
        ownerMessageA <- forAsyncHasher[IO, CurrencyMessage](
          CurrencyMessage(MessageType.Owner, payer, mgA, MessageOrdinal.MinValue),
          payerKey
        )
        ownerMessageB <- forAsyncHasher[IO, CurrencyMessage](
          CurrencyMessage(MessageType.Owner, payer, mgB, MessageOrdinal.MinValue),
          payerKey
        )
        chainA <- mkPositiveFeeWindow(metagraphKeyA, ownerMessageA, fee)
        chainB <- mkPositiveFeeWindow(metagraphKeyB, ownerMessageB, fee)
        windows = SortedMap(mgA -> chainA._2, mgB -> chainB._2)
        prior: MgState = SortedMap(
          mgA -> Right((chainA._1, emptyInfo)),
          mgB -> Right((chainB._1, emptyInfo))
        )
        tips = windows.keysIterator.map(_ -> baseStateChannelTip).to(SortedMap)
        fundedPinned <- mkPinnedHistoryForState(
          dir / "funded",
          prior,
          tips,
          windows.keySet.to(SortedSet),
          windows.keySet.to(SortedSet),
          SortedMap(payer -> Balance(20L))
        )
        overspentPinned <- mkPinnedHistoryForState(
          dir / "overspent",
          prior,
          tips,
          windows.keySet.to(SortedSet),
          windows.keySet.to(SortedSet),
          SortedMap(payer -> Balance(10L))
        )
        harness <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessorHarness(
          Map.empty,
          feeConfigs = feeConfigs,
          contextFnsOverride = contextFns.some
        )
        fundedReplay = ShardCheckpointWiring.reExecDerivationsAtPinnedBaseBatch[IO](
          harness.processor,
          productionReaderAt(fundedPinned),
          globalSnapshotLookup(harness.initialGlobalSnapshot)
        )
        overspentReplay = ShardCheckpointWiring.reExecDerivationsAtPinnedBaseBatch[IO](
          harness.processor,
          productionReaderAt(overspentPinned),
          globalSnapshotLookup(harness.initialGlobalSnapshot)
        )
        fundedFraudReplay = ShardCheckpointWiring.reExecCheckpointAtPinnedBase[IO](
          harness.processor,
          productionReaderAt(fundedPinned),
          globalSnapshotLookup(harness.initialGlobalSnapshot)
        )
        overspentFraudReplay = ShardCheckpointWiring.reExecCheckpointAtPinnedBase[IO](
          harness.processor,
          productionReaderAt(overspentPinned),
          globalSnapshotLookup(harness.initialGlobalSnapshot)
        )
        fundedRoots <- fundedReplay(windows, anchorOrd, fundedPinned.stateRef)
        fundedFraudDisposition <- fundedFraudReplay(windows, anchorOrd, fundedPinned.stateRef)
        rawOverspent <- harness.processor.processCurrencySnapshots(
          anchorOrd,
          SortedMap(payer -> Balance(10L)),
          prior,
          windows.map { case (mg, binaries) => mg -> binaries.reverse },
          globalSnapshotLookup(harness.initialGlobalSnapshot)
        )
        overspentRoots <- overspentReplay(windows, anchorOrd, overspentPinned.stateRef)
        overspentFraudDisposition <- overspentFraudReplay(windows, anchorOrd, overspentPinned.stateRef)
        canonicalFirst = windows.firstKey
      } yield
        expect.all(
          fundedRoots.keySet == windows.keySet,
          fundedRoots.values.forall(_.exists(_ =!= Hash.empty)),
          rawOverspent.keySet == Set(canonicalFirst),
          fundedFraudDisposition match {
            case InvalidStateProofBatchReplay.Reproduced(roots) => roots.keySet == windows.keySet
            case _                                              => false
          },
          overspentRoots.keySet == windows.keySet,
          overspentRoots.values.forall(_.isEmpty),
          overspentFraudDisposition == InvalidStateProofBatchReplay.Unavailable
        )
    }
  }
}
