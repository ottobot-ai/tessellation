package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
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
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.{InvalidStateProofSlashedReader, InvalidStateProofValidator}
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.TimeTrigger
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.sharding.{
  RegisteredCheckpointSigner,
  ShardCheckpointWiring,
  TestCheckpointDutyValidator
}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardCheckpointGl0AcceptanceManager
import io.constellationnetwork.node.shared.snapshot.currency.CurrencySnapshotEvent
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.{GlobalStateConverter, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.slashing.{InvalidStateProofEvidence, InvalidStateProofRejection}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.mpt.storages.MptStateStorage
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.shared.sharedKryoRegistrar
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import fs2.io.file.Files
import weaver.MutableIOSuite

/** The checkpoint execution prior is the wire-carried `executionBaseOrdinal`, never a node's mutable live MPT view.
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
  private val executionBase: SnapshotOrdinal = ord(10L)
  private val liveAhead: SnapshotOrdinal = ord(13L)
  private val anchorOrd: SnapshotOrdinal = ord(1000L)
  private val shardZero: ShardId = ShardId.unsafeApply(0)
  private val epochZero: EtaPeriod = EtaPeriod(0L)

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

  private def mkPinnedHistory(
    dir: fs2.io.file.Path,
    mg: Address,
    baseIncremental: Signed[CurrencyIncrementalSnapshot]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[PinnedCurrencyInfoReader[IO]] =
    for {
      baseBytes <- GlobalStateConverter.currencySnapshotMgEntries[IO](mgState(mg, baseIncremental, baseInfo))
      baseRoot <- GlobalSnapshotInfo.sidecarFreeMptRoot[IO](baseBytes)
      byteStore <- MptStateStorage.make[IO](dir)
      _ <- byteStore.writeState(executionBase, baseBytes)
      pinnedSnap <- mkHashed(executionBase, Some(baseRoot))
      resolver = (o: SnapshotOrdinal) => (if (o === executionBase) pinnedSnap.some else none).pure[IO]
    } yield PinnedCurrencyInfoReader.make[IO](byteStore, resolver)

  private def liveReaderAt(live: MptStore[IO, GlobalStateKey]): SnapshotOrdinal => IO[Option[GlobalStateReader[IO]]] =
    _ => GlobalStateReader.fromMptStore[IO](live).some.pure[IO]

  private def productionReaderAt(pinned: PinnedCurrencyInfoReader[IO]): SnapshotOrdinal => IO[Option[GlobalStateReader[IO]]] =
    ShardCheckpointWiring.pinnedPriorReaderAt[IO](pinned)

  private def globalSnapshotLookup(
    snapshot: Hashed[GlobalIncrementalSnapshot]
  ): SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]] =
    ordinal => Option.when(snapshot.ordinal === ordinal)(snapshot).pure[IO]

  private def mkReplay(
    harness: ProcessorHarness,
    readerAt: SnapshotOrdinal => IO[Option[GlobalStateReader[IO]]]
  )(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Option[Hash]] =
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
        StateChannelSnapshotBinary(Hash.empty, contentBytes, SnapshotFee.MinValue),
        metagraphKey
      )
    } yield NonEmptyList.one(binary)

  private final case class RegisteredCheckpoint(
    checkpoint: ShardCheckpoint,
    checkpointSigner: RegisteredCheckpointSigner,
    acceptanceManager: ShardCheckpointGl0AcceptanceManager[IO]
  )

  private def mkCheckpoint(
    mg: Address,
    window: NonEmptyList[Signed[StateChannelSnapshotBinary]],
    attestedRoot: Hash
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
        executionBaseOrdinal = executionBase
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
        liveAtBase <- mkLiveStore(mgState(mg, baseIncremental, baseInfo), executionBase)
        liveAheadStore <- mkLiveStore(mgState(mg, baseIncremental, aheadInfo), liveAhead)
        liveBaseReplay = mkReplay(harness, liveReaderAt(liveAtBase))
        liveAheadReplay = mkReplay(harness, liveReaderAt(liveAheadStore))
        pinnedReplay = mkReplay(harness, productionReaderAt(pinned))
        liveBaseRoot <- liveBaseReplay(mg, window, anchorOrd, executionBase)
        liveAheadRoot <- liveAheadReplay(mg, window, anchorOrd, executionBase)
        pinnedBaseRoot <- pinnedReplay(mg, window, anchorOrd, executionBase)
        pinnedAheadRoot <- pinnedReplay(mg, window, anchorOrd, executionBase)
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
        skewedLive <- mkLiveStore(mgState(mg, baseIncremental, aheadInfo), executionBase)
        unpinnedReplay = mkReplay(harness, liveReaderAt(skewedLive))
        pinnedReplay = mkReplay(harness, productionReaderAt(pinned))
        unpinnedSkewRoot <- unpinnedReplay(mg, window, anchorOrd, executionBase)
        pinnedRoot <- pinnedReplay(mg, window, anchorOrd, executionBase)
      } yield
        expect.all(
          pinnedRoot.exists(_ =!= Hash.empty),
          unpinnedSkewRoot =!= pinnedRoot
        )
    }
  }

  test("honest checkpoint is not an InvalidStateProof target when the follower live tip is ahead of executionBaseOrdinal") { res =>
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
        attestedRootOpt <- replay(mg, window, anchorOrd, executionBase)
        attestedRoot = attestedRootOpt.getOrElse(Hash.empty)
        checkpointRig <- mkCheckpoint(mg, window, attestedRoot)
        checkpoint = checkpointRig.checkpoint
        followerReplay = mkReplay(harness, productionReaderAt(pinned))
        validator = InvalidStateProofValidator.make[IO](
          (a, bins, anchor, base) => followerReplay(a, bins, anchor, base).map(_.getOrElse(Hash.empty)),
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
}
