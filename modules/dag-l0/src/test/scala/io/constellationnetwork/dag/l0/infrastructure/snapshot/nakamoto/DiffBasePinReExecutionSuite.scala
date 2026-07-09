package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotStateChannelEventsProcessorSuite
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{ChangeSet, GlobalStateReader, PinnedCurrencyInfoReader}
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.{InvalidStateProofSlashedReader, InvalidStateProofValidator}
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageOrdinal, MessageType}
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
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.mpt.storages.MptStateStorage
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.shared.sharedKryoRegistrar
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import fs2.io.file.Files
import weaver.MutableIOSuite

/** Track-1 diff-base-pin — FINDING-B1 / R01 / 003 forcing suite: the re-exec rails must derive the per-MG root over the wire-carried,
  * committee-signed `diffBaseOrdinal`, NEVER over this node's LIVE finalized base.
  *
  * The re-derivation is base-DEPENDENT: `deriveAdoptedCurrencyInfo` folds cumulative balances / refs / active-sets onto the seed prior, and
  * `lastMessages` is a pure carry-forward of the prior (`GlobalSnapshotStateChannelEventsProcessor` `nextLastMessages` fold); the per-MG
  * root is then taken over that fold (`GlobalStateConverter.currencySnapshotMgRoot`). So two nodes at DIFFERENT live tips that re-derive
  * the SAME checkpoint window over their own live bases compute DIFFERENT roots — and the sub-quorum `reExecPath` turns that into
  * `RejectedReExecutionMismatch(reason, signers)` (a 100% false slash of an honest committee), while the follower
  * `createContextInvalidStateProofValidator` false-UPHOLDS a fraud proof (a slash written into the follower's consensus root that the
  * pinned leader didn't write ⇒ StateProofMismatch fork).
  *
  * Suite structure mirrors the `ShardCheckpointWiringSuite` "#261 eta axis" house pattern:
  *   - '''FAIL-BEFORE''' tests pin the bug shape: the pre-fix live-reader rail (SharedServices `liveReaderAt`) recomputes DIFFERENT roots /
  *     upholds a fraud proof against an honest committee. They stay green forever as documentation of why the pin exists.
  *   - '''PASS-AFTER''' (the forcing tests): the production reader-resolution derives EQUAL roots on nodes at different live tips, and a
  *     follower ahead of `diffBaseOrdinal` does NOT slash an honest committee.
  *
  * Harness reuse: the REAL `GlobalSnapshotStateChannelEventsProcessor` from `GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessor`
  * (the byte-identity contract requires the real derivation), the `PinnedCurrencyInfoReaderSuite` pinned-byte-store fixture recipe, and the
  * `InvalidStateProofValidatorSuite` evidence builder.
  */
object DiffBasePinReExecutionSuite extends MutableIOSuite {

  override type Res = (KryoSerializer[IO], Hasher[IO], JsonSerializer[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (ks, h, j, sp)

  // Low ordinal so `currencySnapshotMgEntries` includes the unrolled `Mg*` sub-fields (the same selector value producer + verifier share
  // in production; mirrors PinnedCurrencyInfoReaderSuite).
  implicit val stateProofSelector: StateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(0L)))

  private def addr(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))

  private val mg: Address = addr("mg-diff-base-pin")
  private val account: Address = addr("diff-base-acct-1")
  private val ownerAddr: Address = addr("diff-base-owner")

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal.unsafeApply(n)

  private val diffBase: SnapshotOrdinal = ord(10L) // the committee-signed pinned base D
  private val liveAhead: SnapshotOrdinal = ord(13L) // a follower's live tip AHEAD of D
  private val anchorOrd: SnapshotOrdinal = ord(1000L) // the checkpoint's gl0AnchorOrdinal (fee-cutover context only)

  private val shardZero: ShardId = ShardId.unsafeApply(0)
  private val epochZero: EtaPeriod = EtaPeriod(0L)

  private val fakeProof: SignatureProof =
    SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70)))

  /** A `Signed[CurrencyIncrementalSnapshot]` at metagraph-ordinal `snapOrdinal` carrying NO messages (the message-free window under test).
    * Mirrors PinnedCurrencyInfoReaderSuite's fixture.
    */
  private def mkSignedIncremental(snapOrdinal: Long): Signed[CurrencyIncrementalSnapshot] = {
    val snap = CurrencyIncrementalSnapshot(
      ordinal = SnapshotOrdinal.unsafeApply(snapOrdinal),
      height = Height.MinValue,
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      rewards = SortedSet.empty,
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
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
    Signed(snap, NonEmptySet.of(fakeProof))
  }

  /** The metagraph's Owner message — the canonical base-dependent `lastMessages` payload. The derivation never re-verifies message
    * signatures, so a sentinel proof suffices.
    */
  private def mkOwnerMessage: Signed[CurrencyMessage] =
    Signed(CurrencyMessage(MessageType.Owner, ownerAddr, mg, MessageOrdinal.MinValue), NonEmptySet.of(fakeProof))

  private def mkInfo(
    balances: SortedMap[Address, Balance],
    lastMessages: Option[SortedMap[MessageType, Signed[CurrencyMessage]]]
  ): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap.empty,
      balances = balances,
      lastMessages = lastMessages,
      lastFeeTxRefs = None,
      lastAllowSpendRefs = None,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = None,
      activeTokenLocks = None
    )

  private val baseBalances: SortedMap[Address, Balance] = SortedMap(account -> Balance(NonNegLong.unsafeFrom(100L)))

  /** The pinned base S(N) at `diffBase` — what the committee diffed over: no owner message yet, balance 100. */
  private def baseInfo: CurrencySnapshotInfo = mkInfo(baseBalances, None)

  /** A live tip AHEAD of the base whose per-MG prior differs ONLY in `lastMessages` (the owner message arrived after `diffBase`). */
  private def aheadInfoMessages: CurrencySnapshotInfo =
    mkInfo(baseBalances, Some(SortedMap[MessageType, Signed[CurrencyMessage]](MessageType.Owner -> mkOwnerMessage)))

  /** A live tip AHEAD of the base whose per-MG prior differs ONLY in `balances` (a later window moved the balance). */
  private def aheadInfoBalances: CurrencySnapshotInfo =
    mkInfo(SortedMap(account -> Balance(NonNegLong.unsafeFrom(250L))), None)

  private type MgState = SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]

  private def mgState(info: CurrencySnapshotInfo): MgState =
    SortedMap(mg -> Right((mkSignedIncremental(5L), info)))

  /** Build a LIVE finalized `MptStore` holding `state`, committed at `atOrdinal` (so `lastPersistedOrdinal = Some(atOrdinal)` — the exact
    * live-vs-pinned discriminator the production reader-resolution reads).
    */
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

  /** A minimal `Hashed[GlobalIncrementalSnapshot]` at `ordinal` whose `stateProof.mptRoot` is `mptRoot` — the canonical finalized snapshot
    * the pinned reader self-resolves the diff-base pin against. Copied from PinnedCurrencyInfoReaderSuite.
    */
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

  /** Stand up the cluster-uniform PINNED history at `diffBase`: a version-retained byte store holding the base state bytes + a finalized
    * snapshot at `diffBase` whose committed `mptRoot` those bytes reproduce. Every honest node retains this identically (it is finalized
    * history) — the fixture the pinned reader verifies against. Returns the pinned reader.
    */
  private def mkPinnedHistory(
    dir: fs2.io.file.Path
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[PinnedCurrencyInfoReader[IO]] =
    for {
      baseBytes <- GlobalStateConverter.currencySnapshotMgEntries[IO](mgState(baseInfo))
      baseRoot <- GlobalSnapshotInfo.sidecarFreeMptRoot[IO](baseBytes)
      byteStore <- MptStateStorage.make[IO](dir)
      _ <- byteStore.writeState(diffBase, baseBytes)
      pinnedSnap <- mkHashed(diffBase, Some(baseRoot))
      resolver = (o: SnapshotOrdinal) => (if (o === diffBase) pinnedSnap.some else none).pure[IO]
    } yield PinnedCurrencyInfoReader.make[IO](byteStore, resolver)

  /** The PRE-FIX SharedServices wiring shape (`liveReaderAt`, SharedServices.scala reExecuteDerivation + createContext validator): resolve
    * EVERY diff-base to this node's LIVE finalized base, ignoring the pinned ordinal. Kept as the FAIL-BEFORE model of the bug.
    */
  private def preFixLiveReaderAt(
    live: MptStore[IO, GlobalStateKey]
  ): SnapshotOrdinal => IO[Option[GlobalStateReader[IO]]] =
    (_: SnapshotOrdinal) => IO.pure(GlobalStateReader.fromMptStore[IO](live).some)

  /** THE PRODUCTION READER-RESOLUTION UNDER TEST — the SINGLE shared recipe both SharedServices rails (sub-quorum `reExecuteDerivation` +
    * `createContextInvalidStateProofValidator`) AND the gl0 produce/watchtower rail (`GlobalSnapshotConsensus.finalizedReaderAt`) wire:
    * ALWAYS the version-retained, root-verified pinned reader; `None` (fail-closed OMIT — never a live-base substitute) when the anchor is
    * unresolvable. The `live` store is taken ONLY to assert the production recipe ignores it: the removed fast path (live reader iff the
    * pinned ordinal == `lastPersistedOrdinal`) was UNSOUND — the live store's content is not pinned by its watermark (forcing (iii), the
    * 2026-07-08 mid-fold-skew wedge). The RED capture of this suite (2026-07-07) ran the SAME forcing tests against [[preFixLiveReaderAt]]
    * — the pre-fix SharedServices wiring — and they failed with divergent roots / a false-UPHELD verdict; keeping the call routed through
    * the production helper is what forces the pin.
    */
  private def productionPriorReaderAt(
    live: MptStore[IO, GlobalStateKey],
    pinned: PinnedCurrencyInfoReader[IO]
  ): SnapshotOrdinal => IO[Option[GlobalStateReader[IO]]] = {
    val _ = live // deliberately unused: the production recipe must never consult the live store
    ShardCheckpointWiring.pinnedPriorReaderAt[IO](pinned)
  }

  /** The production re-derivation closure shape (SharedServices `reExecuteDerivation` / `reDerive`): PIN-1 root half of
    * `reExecDerivationWithDiff`, `Hash.empty` on OMIT.
    */
  private def mkReDerive(
    readerAt: SnapshotOrdinal => IO[Option[GlobalStateReader[IO]]]
  )(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO],
    ks: KryoSerializer[IO],
    sp: SecurityProvider[IO]
  ): IO[(Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash]] =
    GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessor(Map.empty).map { processor =>
      val withDiff = ShardCheckpointWiring.reExecDerivationWithDiff[IO](processor, readerAt)
      (mgA: Address, bins: NonEmptyList[Signed[StateChannelSnapshotBinary]], anchor: SnapshotOrdinal, base: SnapshotOrdinal) =>
        withDiff(mgA, bins, anchor, base).map(_.map(_._1).getOrElse(Hash.empty))
    }

  /** Like [[mkReDerive]] but returns BOTH halves of the producer emission — the attested root AND the wire `ChangeSet` — so the
    * mid-fold-skew forcing test can replay the ADOPTER's apply-and-verify (`ChangeSet.reconstructInfoFromDiff` over the pinned prior,
    * re-rooted via `currencySnapshotMgRoot`) against exactly what the producer would have stamped on the checkpoint.
    */
  private def mkReDeriveWithDiff(
    readerAt: SnapshotOrdinal => IO[Option[GlobalStateReader[IO]]]
  )(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO],
    ks: KryoSerializer[IO],
    sp: SecurityProvider[IO]
  ): IO[(Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Option[(Hash, ChangeSet)]]] =
    GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessor(Map.empty).map { processor =>
      ShardCheckpointWiring.reExecDerivationWithDiff[IO](processor, readerAt)
    }

  /** The message-free checkpoint window: ONE incremental at metagraph-ordinal 6 chaining the base's ordinal-5 incremental. */
  private def mkWindow(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[NonEmptyList[Signed[StateChannelSnapshotBinary]]] =
    for {
      contentBytes <- JsonSerializer[IO].serialize(mkSignedIncremental(6L))
      binary = StateChannelSnapshotBinary(Hash.empty, contentBytes, SnapshotFee.MinValue)
    } yield NonEmptyList.of(Signed(binary, NonEmptySet.of(fakeProof)))

  /** A minimal one-MG checkpoint carrying `attestedRoot` + the window at the pinned `diffBase`. Committee signatures are structural fakes —
    * the dispute validator never re-verifies them (it re-derives the root instead). Mirrors InvalidStateProofValidatorSuite.
    */
  private def mkCheckpoint(
    window: NonEmptyList[Signed[StateChannelSnapshotBinary]],
    attestedRoot: Hash
  ): ShardCheckpoint =
    ShardCheckpoint(
      shardId = shardZero,
      parentCheckpointHash = Hash("0" * 64),
      shardOrdinal = ShardOrdinal(1L),
      gl0AnchorOrdinal = anchorOrd,
      slot = SlotT.unsafeApply(1L),
      derivedStateDelta = ShardDerivedStateDelta.empty.copy(
        perMetagraphMptRoots = SortedMap(mg -> attestedRoot),
        includedSnapshots = SortedMap(mg -> window)
      ),
      emittedReceipts = List.empty,
      committeeSignatures =
        NonEmptyList.of(CommitteeMemberSignature(PeerId(Hex("01" * 64)), Hex("aa" * 80), Hex("bb" * 64), Hex("cc" * 128), 0)),
      epoch = epochZero,
      diffBaseOrdinal = diffBase
    )

  /** Build a fraud proof + matching evidence with a REAL challenger Ed25519 signature over the canonical preimage. Mirrors
    * InvalidStateProofValidatorSuite.mkEvidence (private there).
    */
  private def mkEvidence(
    cp: ShardCheckpoint,
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
          val fp = unsigned.copy(challengerSignature = Hex.fromBytes(sig))
          InvalidStateProofEvidence(
            shardId = cp.shardId,
            disputedCheckpoint = cp,
            metagraphAddress = mg,
            attestedRoot = cp.derivedStateDelta.perMetagraphMptRoots.getOrElse(mg, Hash.empty),
            fraudProof = fp
          )
        }
      }
    }

  // ===========================================================================
  // FAIL-BEFORE (documentation): the live-reader rail is base-DEPENDENT — the false-slash vector
  // ===========================================================================

  test("FAIL-BEFORE: live-reader rail — node at diffBase vs node ahead (balances moved) recompute DIFFERENT roots for the same window") {
    res =>
      implicit val (ks, h, js, sp) = res
      // The RED capture of this suite (2026-07-07, pre-fix tree) demonstrated this SAME divergence for a lastMessages-only drift
      // (atBase=3191db9c… vs ahead=fd4a9b47…) — the two-part fix closes that vector twice over (the pin + the committed-proof-shaped
      // `candidateLastMessages`), so this permanent bug-shape documentation uses the BALANCES drift: `balances` flows through the seed
      // prior on every window (derived candidate and per-field fallback both read `lastState.balances`), so ONLY the diff-base pin —
      // never a shape guard — can neutralize it. This test proves the live rail stays base-dependent (why `pinnedPriorReaderAt` exists).
      for {
        liveAtBase <- mkLiveStore(mgState(baseInfo), diffBase)
        liveAheadStore <- mkLiveStore(mgState(aheadInfoBalances), liveAhead)
        window <- mkWindow
        reDeriveAtBase <- mkReDerive(preFixLiveReaderAt(liveAtBase))
        reDeriveAhead <- mkReDerive(preFixLiveReaderAt(liveAheadStore))
        rootAtBase <- reDeriveAtBase(mg, window, anchorOrd, diffBase)
        rootAhead <- reDeriveAhead(mg, window, anchorOrd, diffBase)
      } yield
        expect(rootAtBase =!= Hash.empty, "committee node at the base must derive a real root") &&
          expect(rootAhead =!= Hash.empty, "follower ahead of the base must derive a real root") &&
          expect(
            rootAtBase =!= rootAhead,
            "the PRE-FIX live rail is base-dependent: a node ahead of diffBaseOrdinal recomputes a DIFFERENT root — " +
              "this divergence is what falsely slashes an honest committee (kept as documentation of the bug shape)"
          )
  }

  test("collapse pin: lastMessages-only live drift with a None lastMessagesProof no longer moves the root (proof-shape-driven candidate)") {
    res =>
      implicit val (ks, h, js, sp) = res
      // The OPTIONAL half of the fix (GSCEP `candidateLastMessages` driven off the committed proof like its guarded siblings): a window
      // whose metagraph committed `lastMessagesProof = None` derives `lastMessages = None` REGARDLESS of what the local prior carries, so
      // a lastMessages-only live-tip drift cannot leak into the per-MG root even on the (pre-pin) live rail. The RED capture proved this
      // exact vector DID diverge before the fix. Balances remain live-rail-divergent (FAIL-BEFORE above) — the pin is still load-bearing.
      for {
        liveAtBase <- mkLiveStore(mgState(baseInfo), diffBase)
        liveAheadStore <- mkLiveStore(mgState(aheadInfoMessages), liveAhead)
        window <- mkWindow
        reDeriveAtBase <- mkReDerive(preFixLiveReaderAt(liveAtBase))
        reDeriveAhead <- mkReDerive(preFixLiveReaderAt(liveAheadStore))
        rootAtBase <- reDeriveAtBase(mg, window, anchorOrd, diffBase)
        rootAhead <- reDeriveAhead(mg, window, anchorOrd, diffBase)
      } yield
        expect(rootAtBase =!= Hash.empty, "node at the base must derive a real root") &&
          expect(
            rootAtBase === rootAhead,
            s"a None-proof window must derive lastMessages = None on ANY prior — the shape collapse closes this leak " +
              s"(got atBase=${rootAtBase.value.take(16)} ahead=${rootAhead.value.take(16)})"
          )
  }

  // ===========================================================================
  // FORCING (i): pinned rail — EQUAL roots across nodes at different live tips
  // ===========================================================================

  test("forcing (i): production rail — node at diffBase and node AHEAD (priors differ only in lastMessages) derive EQUAL roots") { res =>
    implicit val (ks, h, js, sp) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        pinned <- mkPinnedHistory(dir)
        liveAtBase <- mkLiveStore(mgState(baseInfo), diffBase)
        liveAheadStore <- mkLiveStore(mgState(aheadInfoMessages), liveAhead)
        window <- mkWindow
        reDeriveAtBase <- mkReDerive(productionPriorReaderAt(liveAtBase, pinned))
        reDeriveAhead <- mkReDerive(productionPriorReaderAt(liveAheadStore, pinned))
        rootAtBase <- reDeriveAtBase(mg, window, anchorOrd, diffBase)
        rootAhead <- reDeriveAhead(mg, window, anchorOrd, diffBase)
      } yield
        expect(rootAtBase =!= Hash.empty, "node at the base must derive a real root (not the OMIT sentinel)") &&
          expect(
            rootAtBase === rootAhead,
            s"the production rail must pin the derivation prior to diffBaseOrdinal so nodes at DIFFERENT live tips derive EQUAL " +
              s"per-MG roots (got atBase=${rootAtBase.value.take(16)} ahead=${rootAhead.value.take(16)})"
          )
    }
  }

  test("forcing (i-b): production rail — EQUAL roots when the ahead node's live prior differs only in balances") { res =>
    implicit val (ks, h, js, sp) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        pinned <- mkPinnedHistory(dir)
        liveAtBase <- mkLiveStore(mgState(baseInfo), diffBase)
        liveAheadStore <- mkLiveStore(mgState(aheadInfoBalances), liveAhead)
        window <- mkWindow
        reDeriveAtBase <- mkReDerive(productionPriorReaderAt(liveAtBase, pinned))
        reDeriveAhead <- mkReDerive(productionPriorReaderAt(liveAheadStore, pinned))
        rootAtBase <- reDeriveAtBase(mg, window, anchorOrd, diffBase)
        rootAhead <- reDeriveAhead(mg, window, anchorOrd, diffBase)
      } yield
        expect(rootAtBase =!= Hash.empty, "node at the base must derive a real root (not the OMIT sentinel)") &&
          expect(
            rootAtBase === rootAhead,
            s"balances flow through the seed prior — the pinned rail must neutralize a live-tip balance drift " +
              s"(got atBase=${rootAtBase.value.take(16)} ahead=${rootAhead.value.take(16)})"
          )
    }
  }

  // ===========================================================================
  // FORCING (iii): mid-fold watermark skew — the 2026-07-08 live wedge (token-lock e2e, shard-0 shardOrd=8)
  // ===========================================================================

  test(
    "forcing (iii): production rail — live watermark == diffBase but content already drifted (mid-fold skew) still derives the " +
      "pinned state@diffBase root, and the emitted diff re-applies onto the pinned prior to the attested root"
  ) { res =>
    implicit val (ks, h, js, sp) = res
    // THE 2026-07-08 LIVE WEDGE (2mg/2shard token-lock e2e). In Passthrough overlay mode the accept-path writes land in the live base
    // store THROUGHOUT an ordinal's processing and `MptStore.commit(ordinal)` bumps `lastPersistedOrdinal` only at the END — so
    // `lastPersistedOrdinal == N` does NOT imply "content == committed state@N". gl0-1 minted shard-0 shardOrd=8 stamped
    // `diffBaseOrdinal=18` while its live store already carried the shardOrd-7 adopt (prior read inc@10/bal=15; true state@18 was
    // inc@7/bal=14): the old fast path (`live == ord ⇒ serve the live store`) handed the derivation that POST-FOLD content, the diff was
    // cut over it, and every honest adopter — applying the wire diff onto the TRUE pinned state@18 — recomputed a root (4bfe2f1f…) that
    // never matched the attested one (b9787f2e…): the checkpoint re-offered and dropped EVERY ordinal, the per-MG mirror froze at
    // mgOrd=10, and the metagraph's later activeTokenLocks never reached gl0. This test models that exact skew: watermark == diffBase,
    // content = aheadInfoBalances. The production rail must IGNORE the live view and read the version-retained, root-VERIFIED bytes.
    Files[IO].tempDirectory.use { dir =>
      for {
        pinned <- mkPinnedHistory(dir)
        // Watermark says diffBase; content has ALREADY moved past it (the mid-fold/in-flight-adopt state).
        skewedLive <- mkLiveStore(mgState(aheadInfoBalances), diffBase)
        window <- mkWindow
        reDeriveSkewed <- mkReDeriveWithDiff(productionPriorReaderAt(skewedLive, pinned))
        emitted <- reDeriveSkewed(mg, window, anchorOrd, diffBase)
        (attestedRoot, wireDiff) = emitted.getOrElse((Hash.empty, ChangeSet.empty))
        // The ADOPTER side (GSAM `deriveAdoptedCurrencyState` PIN-1): apply the emitted diff onto the TRUE pinned state@diffBase and
        // re-root — byte-identical recompute of what every honest gl0 does with this checkpoint.
        pinnedPriorOpt <- pinned.readAtOrdinal(diffBase, mg)
        adopterRoot <- pinnedPriorOpt match {
          case Some(pinnedPrior) =>
            ChangeSet
              .reconstructInfoFromDiff[IO](mg, pinnedPrior, wireDiff)
              .flatMap { reconstructed =>
                GlobalStateConverter.currencySnapshotMgRoot[IO](
                  SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]](
                    mg -> Right((mkSignedIncremental(6L), reconstructed))
                  )
                )
              }
          case None => IO.pure(Hash.empty)
        }
      } yield
        expect(attestedRoot =!= Hash.empty, "the skewed-watermark node must still derive (not OMIT) — the pinned bytes are servable") &&
          expect(pinnedPriorOpt.isDefined, "the pinned prior at diffBase must reconstruct (fixture sanity)") &&
          expect(
            adopterRoot === attestedRoot,
            s"a checkpoint minted under mid-fold watermark skew must still be reconstructible by every honest adopter from the " +
              s"pinned state@diffBase — the live-view fast path poisons the attested root otherwise " +
              s"(attested=${attestedRoot.value.take(16)} adopterRecomputed=${adopterRoot.value.take(16)})"
          )
    }
  }

  // ===========================================================================
  // FORCING (ii): an honest committee re-validated by a follower ahead of diffBase must NOT be slashed
  // ===========================================================================

  test("forcing (ii): honest committee checkpoint, follower live tip > diffBaseOrdinal ⇒ dispute NOT upheld (no slash)") { res =>
    implicit val (ks, h, js, sp) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        pinned <- mkPinnedHistory(dir)
        // The HONEST COMMITTEE: derives + attests over its finalized base == diffBase (its live tip IS the pinned base).
        committeeLive <- mkLiveStore(mgState(baseInfo), diffBase)
        window <- mkWindow
        committeeReDerive <- mkReDerive(productionPriorReaderAt(committeeLive, pinned))
        attestedRoot <- committeeReDerive(mg, window, anchorOrd, diffBase)
        cp = mkCheckpoint(window, attestedRoot)

        // The FOLLOWER: live tip AHEAD of diffBase (owner message arrived after the base) — re-validates the carried fraud proof
        // through the SAME production rail the createContext GSAM wires.
        followerLive <- mkLiveStore(mgState(aheadInfoMessages), liveAhead)
        followerReDerive <- mkReDerive(productionPriorReaderAt(followerLive, pinned))
        validator = InvalidStateProofValidator.make[IO](followerReDerive, InvalidStateProofSlashedReader.neverSlashed[IO])

        challengerKp <- KeyPairGenerator.makeKeyPair[IO]
        challengerId = PeerId.fromPublic(challengerKp.getPublic)
        ev <- mkEvidence(cp, challengerKp, challengerId, claimed = attestedRoot, challengerRoot = Hash("b" * 64))
        verdict <- validator.validate(ev)
      } yield
        expect(attestedRoot =!= Hash.empty, "the honest committee must have derived a real root") &&
          expect(
            verdict.isLeft,
            s"an honest committee's checkpoint must NEVER be slashed by a follower whose live tip ran ahead of the pinned " +
              s"diffBaseOrdinal — the follower must re-derive over the PINNED base and reproduce the attested root (got $verdict)"
          ) &&
          expect(
            verdict == Left(InvalidStateProofRejection.DisputeNotUpheld(attestedRoot, attestedRoot)),
            s"expected DisputeNotUpheld(attested, attested) — the honest-committee floor (got $verdict)"
          )
    }
  }
}
