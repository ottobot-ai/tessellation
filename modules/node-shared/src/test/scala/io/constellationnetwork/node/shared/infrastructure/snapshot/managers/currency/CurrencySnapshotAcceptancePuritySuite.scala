package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSyncView
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.env.AppEnvironment.{Dev, Mainnet}
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage}
import io.constellationnetwork.node.shared.modules.SharedValidators
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SpendAction, SpendTransaction}
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.node.RewardFraction
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.swap.{CurrencyId, SwapAmount}
import io.constellationnetwork.schema.{GlobalStateProofSelector, _}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt, PosLong}
import fs2.concurrent.SignallingRef
import weaver.MutableIOSuite

/** Track-1 I-PIN determinism forcing-test (TDD target-lock).
  *
  * THE PROPERTY under test: two invocations of `CurrencySnapshotAcceptanceManager.accept` that differ ONLY in the node-local GL0 head
  * (`lastGlobalSnapshotStorage`) but carry the SAME recorded/pinned `globalSyncView` (validator path, `pinnedGlobalSyncView`) must produce
  * a BYTE-IDENTICAL `stateProof`. `CurrencySnapshotStateProof` has a derevo-derived `Eq` over its all-`Hash` fields, so structural `Eq` ==
  * byte identity; `expect.eql(rA.stateProof, rB.stateProof)` is therefore an exact byte-equality assertion.
  *
  * WHY IT IS RED TODAY (the I-PIN impurity): `accept` reads the node-local head at `CurrencySnapshotAcceptanceManager` (`getCombined` →
  * `lastUnsyncGlobalSnapshotInfo.metagraphSyncData`, CSAM:298) and threads it into `getLastGlobalSnapshotsSpendActions` (CSAM:456). That
  * head-sourced `metagraphSyncData` gates which unapplied cross-shard global ordinals are folded into `metagraphIdSpendTransactions`
  * (CSAM:467), whose SpendActions MOVE balances via `updateCurrencyBalancesBySpendTransactions` (CSAM:565) — and the balances land in
  * `csi.balances`, hence `stateProof.balancesProof`. So a node whose local head advertises an unapplied cross-shard ordinal computes a
  * DIFFERENT `stateProof` than a node whose local head does not — even though both pin the SAME anchor. That is the impurity I-PIN removes
  * by rerouting the read to the pinned anchor; once it lands, both invocations fold identical cross-shard state and this test goes GREEN.
  *
  * FORCING LEVER (per spec precondition #5): the reliable lever is the SpendAction VALUE carried via `metagraphSyncData` divergence, NOT a
  * bare head-ordinal difference (which spuriously passes pre-fix). Head A's `metagraphSyncData` names an unapplied global ordinal (= the
  * anchor ordinal) whose SpendAction debits a FUNDED source and credits a destination; head B's `metagraphSyncData` is `None`, so head B
  * applies nothing. The balance divergence is the RED.
  *
  * LOAD-BEARING PRECONDITIONS honored below:
  *   1. The SpendAction moves a REAL balance and `source` is FUNDED (`ctx.snapshotInfo.balances(source) >= amount`) so `Balance.minus`
  *      (CSAM:571 path) does not underflow-and-raise — the test asserts RED, it must not ERROR. 2. `pinnedGlobalSyncView` hash-pins the
  *      anchor that `getGlobalSnapshotByOrdinal` resolves, so the forced-view hash guard (CSAM:376-386) passes instead of raising. 3.
  *      `getGlobalSnapshotByOrdinal` (and the empty `getLastN`) are IDENTICAL across both managers — ONLY `lastGlobalSnapshotStorage`
  *      differs. 4. The SAME `Hasher` instance is threaded to both snapshot builders and both `accept` calls. 5. `FieldsAddedOrdinals` is
  *      all-empty ⇒ every threshold is `SnapshotOrdinal.MinValue`, so both head ordinals (150, 120) clear the tessellation-3 migration
  *      boundary and BOTH emit the extended `csi` fields — the field-gating head-leak (`snapshotOrdinalToCheckFields \=
  *      lastUnsyncGlobalSnapshot.ordinal`, CSAM:577) is INERT and cannot spuriously flip a field. The ONLY effective difference is
  *      balances.
  *
  * TWO SEPARATE managers: the head-storage `set()` guard forbids swapping divergent heads on one storage, and separate managers keep the
  * `globalSnapshotsAlreadyProcessed` cache from contaminating call 2.
  */
object CurrencySnapshotAcceptancePuritySuite extends MutableIOSuite {

  // Always-legacy selector: `select(ordinal)` yields LegacyFormat for every ordinal < Long.MaxValue, so the anchor/head
  // GlobalSnapshotInfo.stateProof needs no MptStore. (CurrencySnapshotInfo.stateProof — the one under test — ignores the selector entirely
  // and is a pure hash of its 9 fields.)
  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong.unsafeFrom(Long.MaxValue)))
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  override type Res = (Hasher[IO], JsonSerializer[IO], KryoSerializer[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] = for {
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (h, j, ks, sp)

  private def ord(v: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(v))

  private val anchorOrdinal: SnapshotOrdinal = ord(100L)
  private val headAOrdinal: SnapshotOrdinal = ord(150L)
  private val headBOrdinal: SnapshotOrdinal = ord(120L)
  private val cl0Ordinal: SnapshotOrdinal = ord(5L)

  private val fundedAmount: Balance = Balance(NonNegLong.unsafeFrom(1000L))
  private val spendAmount: SwapAmount = SwapAmount(PosLong.unsafeFrom(100L))

  // Gate 1 lever (precondition #5): all-empty ⇒ every threshold is MinValue ⇒ the field-gating head-leak is INERT.
  private val allEmptyFieldsAddedOrdinals: FieldsAddedOrdinals =
    FieldsAddedOrdinals(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty)

  // Gate 2: the REAL mainnet activation ordinals verbatim from `modules/node-shared/src/main/resources/application.conf`
  // (`fields-added-ordinals` block), so the head-ordinal-driven field-gating (`snapshotOrdinalToCheckFields` vs
  // `tessellation3Migration`, `metagraphSyncData`, `updatingCombineFunctionSpendActions`) is exercised at production thresholds. The GREEN
  // baseline picks head/anchor ordinals ABOVE every field-emission boundary so emission is head-invariant today.
  private val mainnetFieldsAddedOrdinals: FieldsAddedOrdinals =
    FieldsAddedOrdinals(
      tessellation3Migration = Map(Mainnet -> ord(4409045L)),
      tessellation301Migration = Map(Mainnet -> ord(4915254L)),
      checkSyncGlobalSnapshotField = Map(Mainnet -> ord(4488000L)),
      metagraphSyncData = Map(Mainnet -> ord(4915254L)),
      updatedLastSyncGlobalOrder = Map(Mainnet -> ord(4915254L)),
      updatedLastSyncGlobalFromPeersInConsensus = Map(Mainnet -> ord(4915254L)),
      updatingCombineFunctionSpendActions = Map(Mainnet -> ord(4957662L)),
      setSumFix = Map(Mainnet -> ord(9999999L))
    )

  /** A GlobalSnapshotInfo that is empty except for `metagraphSyncData` (position 17). This is the node-local head state; the two heads
    * differ ONLY here (head A: Some(unapplied ordinal); head B: None).
    */
  private def mkGlobalInfo(metagraphSyncData: Option[SortedMap[Address, MetagraphSyncDataInfo]]): GlobalSnapshotInfo =
    GlobalSnapshotInfo(
      SortedMap.empty, // lastStateChannelSnapshotHashes
      SortedMap.empty, // lastTxRefs
      SortedMap.empty, // balances
      SortedMap.empty, // lastCurrencySnapshots
      SortedMap.empty, // lastCurrencySnapshotsProofs
      None, // activeAllowSpends
      None, // activeTokenLocks
      None, // tokenLockBalances
      None, // lastAllowSpendRefs
      None, // lastTokenLockRefs
      Some(SortedMap.empty), // updateNodeParameters
      Some(SortedMap.empty), // activeDelegatedStakes
      Some(SortedMap.empty), // delegatedStakesWithdrawals
      Some(SortedMap.empty), // activeNodeCollaterals
      Some(SortedMap.empty), // nodeCollateralWithdrawals
      Some(SortedMap.empty), // priceState
      metagraphSyncData, // metagraphSyncData  <-- the node-local head leak lever
      SortedMap.empty // historicalStakeSnapshots
    )

  /** Build a `Hashed[GlobalIncrementalSnapshot]` at `ordinal` carrying `spendActions`. Mirrors
    * `GlobalSnapshotStateChannelEventsProcessorSuite.mkGlobalIncrementalSnapshot`, parameterized on ordinal + spendActions.
    */
  private def mkGlobalSnapshot(
    ordinal: SnapshotOrdinal,
    info: GlobalSnapshotInfo,
    spendActions: Option[SortedMap[Address, List[SpendAction]]]
  )(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[Hashed[GlobalIncrementalSnapshot]] =
    info.stateProof[IO](ordinal).flatMap { sp =>
      Signed(
        GlobalIncrementalSnapshot(
          ordinal,
          Height.MinValue,
          SubHeight.MinValue,
          Hash.empty,
          SortedSet.empty,
          SortedMap.empty,
          SortedMap.empty[io.constellationnetwork.schema.sharding.ShardId, io.constellationnetwork.schema.sharding.ShardCheckpoint],
          SortedSet.empty,
          None,
          EpochProgress.MinValue,
          NonEmptyList.of(PeerId(Hex(""))),
          SnapshotTips(SortedSet.empty, SortedSet.empty),
          stateProof = sp,
          Some(SortedSet.empty), // allowSpendBlocks
          Some(SortedSet.empty), // tokenLockBlocks
          spendActions, // spendActions  <-- cross-shard SpendAction injected here
          Some(SortedMap.empty), // updateNodeParameters
          Some(SortedSet.empty), // artifacts
          Some(SortedMap.empty), // activeDelegatedStakes
          Some(SortedMap.empty), // delegatedStakesWithdrawals
          Some(SortedMap.empty), // activeNodeCollaterals
          Some(SortedMap.empty) // nodeCollateralWithdrawals
        ),
        NonEmptySet.fromSetUnsafe(SortedSet(SignatureProof(ID.Id(Hex("")), Signature(Hex("")))))
      ).toHashed[IO]
    }

  /** Fresh CurrencySnapshotAcceptanceManager whose ONLY point of divergence is `lastGlobalSnapshotStorage`, seeded with `head`. Everything
    * else (validators, empty `getLastN`, config, all-empty FieldsAddedOrdinals) is identical across the two managers.
    */
  private def mkManager(
    environment: AppEnvironment,
    fieldsAddedOrdinals: FieldsAddedOrdinals,
    head: (Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo),
    lastN: SortedMap[SnapshotOrdinal, Hashed[GlobalIncrementalSnapshot]] = SortedMap.empty
  )(
    implicit h: Hasher[IO],
    j: JsonSerializer[IO],
    ks: KryoSerializer[IO],
    sp: SecurityProvider[IO]
  ): IO[CurrencySnapshotAcceptanceManager[IO]] = {
    implicit val csps: CurrencyStateProofSelector = CurrencyStateProofSelector.instance

    val validators = SharedValidators
      .make[IO](
        Dev,
        AddressesConfig(Set()),
        None,
        None,
        None,
        SortedMap.empty,
        Long.MaxValue,
        Hasher.forKryo[IO],
        DelegatedStakingConfig(
          RewardFraction(5_000_000),
          RewardFraction(10_000_000),
          PosInt(140),
          PosInt(10),
          PosLong((5000 * 1e8).toLong),
          Map(Dev -> EpochProgress(NonNegLong(7338977L)))
        ),
        PriceOracleConfig(None, NonNegLong(0))
      )

    val syncConfig = LastGlobalSnapshotsSyncConfig(NonNegLong(2L), PosInt.unsafeFrom(5))

    for {
      lastNSnapR <- SignallingRef.of[IO, Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]](None)
      incLastNSnapR <- SignallingRef.of[IO, SortedMap[SnapshotOrdinal, Hashed[GlobalIncrementalSnapshot]]](lastN)
      lastNSnapshotStorage = LastNGlobalSnapshotStorage.make[IO](syncConfig, lastNSnapR, incLastNSnapR)
      lastGlobalSnapshotStorage <- LastSnapshotStorage.make[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo](head.some)
      manager <- CurrencySnapshotAcceptanceManager.make[IO](
        fieldsAddedOrdinals,
        environment,
        LastGlobalSnapshotsSyncConfig(NonNegLong(2L), PosInt(10)),
        BlockAcceptanceManager.make[IO](validators.currencyBlockValidator, Hasher.forKryo[IO]),
        TokenLockBlockAcceptanceManager.make[IO](validators.tokenLockBlockValidator),
        AllowSpendBlockAcceptanceManager.make[IO](validators.allowSpendBlockValidator),
        Amount(0L),
        validators.currencyMessageValidator,
        validators.feeTransactionValidator,
        validators.globalSnapshotSyncValidator,
        lastNSnapshotStorage,
        lastGlobalSnapshotStorage
      )
    } yield manager
  }

  private def runAccept(
    manager: CurrencySnapshotAcceptanceManager[IO],
    ctx: CurrencySnapshotContext,
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]],
    anchorView: GlobalSyncView
  )(implicit h: Hasher[IO]): IO[CurrencySnapshotStateProof] =
    manager
      .accept(
        blocksForAcceptance = List.empty,
        tokenLockBlocksForAcceptance = List.empty,
        allowSpendBlocksForAcceptance = List.empty,
        messagesForAcceptance = List.empty,
        feeTransactionsForAcceptance = None,
        globalSnapshotSyncsForAcceptance = List.empty,
        sharedArtifactsForAcceptance = SortedSet.empty,
        lastSnapshotContext = ctx,
        snapshotOrdinal = cl0Ordinal,
        epochProgress = EpochProgress.MinValue,
        lastActiveTips = SortedSet.empty,
        lastDeprecatedTips = SortedSet.empty,
        calculateRewardsFn = _ => IO.pure(SortedSet.empty),
        facilitators = Set.empty,
        getGlobalSnapshotByOrdinal = getGlobalSnapshotByOrdinal,
        lastGlobalSyncView = anchorView.some,
        shouldPerformMetagraphSpecificValidations = false,
        lastArtifactProofs = NonEmptySet.fromSetUnsafe(SortedSet(SignatureProof(ID.Id(Hex("")), Signature(Hex(""))))),
        // blocker-1a: the set-valued in-band `P` (union of retained `GlobalSnapshotsProcessed.ordinals`). The forcing test carries no prior
        // processed history, so `P = ∅` — head A's `metagraphSyncData` still names ord=100 as unapplied (100 ∉ ∅), which is exactly why the
        // pre-I-PIN head read diverges. Added when copying the suite forward from the pre-1a worktree that authored it.
        alreadyProcessedGlobalOrdinals = SortedSet.empty,
        pinnedGlobalSyncView = anchorView.some
      )
      .map(_.stateProof)

  test(
    "I-PIN determinism: same pinned globalSyncView + divergent local heads (head A applies a cross-shard SpendAction via " +
      "metagraphSyncData, head B does not) => stateProof MUST be byte-identical"
  ) { res =>
    implicit val (h, j, ks, sp) = res

    for {
      mgKp <- KeyPairGenerator.makeKeyPair[IO]
      srcKp <- KeyPairGenerator.makeKeyPair[IO]
      dstKp <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = PublicKeyOps(mgKp.getPublic).toAddress
      source = PublicKeyOps(srcKp.getPublic).toAddress
      destination = PublicKeyOps(dstKp.getPublic).toAddress

      // Cross-shard SpendAction: debits `source` (FUNDED below), credits `destination`, tagged with this metagraph's currencyId so the
      // CSAM:467 filter keeps it. `allowSpendRef = None` ⇒ the direct source.minus / destination.plus branch (CSAM:565 path).
      spendTx = SpendTransaction(None, Some(CurrencyId(metagraphId)), spendAmount, source, destination)
      anchorSpendActions = Some(SortedMap(metagraphId -> List(SpendAction(NonEmptyList.of(spendTx)))))

      // The pinned anchor at ord=100 carries the cross-shard SpendAction. Both managers resolve it identically.
      anchor <- mkGlobalSnapshot(anchorOrdinal, mkGlobalInfo(None), anchorSpendActions)
      sameOrdinalSibling <- mkGlobalSnapshot(anchorOrdinal, mkGlobalInfo(None), None)
      anchorView = GlobalSyncView(anchorOrdinal, anchor.hash, anchor.epochProgress)
      getGlobalSnapshotByOrdinal = (o: SnapshotOrdinal) =>
        if (o === anchorOrdinal) anchor.some.pure[IO] else none[Hashed[GlobalIncrementalSnapshot]].pure[IO]

      // Prior currency context: `source` is funded so the head-A spend does not underflow-and-raise (precondition #1).
      ctx = CurrencySnapshotContext(
        metagraphId,
        CurrencySnapshotInfo(
          SortedMap.empty,
          SortedMap(source -> fundedAmount),
          None,
          None,
          None,
          None,
          None,
          None,
          None
        )
      )

      // Head A: local head at ord=150 whose metagraphSyncData advertises the anchor ordinal as an UNAPPLIED cross-shard change.
      headA <- mkGlobalSnapshot(
        headAOrdinal,
        mkGlobalInfo(
          Some(SortedMap(metagraphId -> MetagraphSyncDataInfo(anchorOrdinal, EpochProgress.MinValue, SortedSet(anchorOrdinal))))
        ),
        None
      )
      headAInfo = mkGlobalInfo(
        Some(SortedMap(metagraphId -> MetagraphSyncDataInfo(anchorOrdinal, EpochProgress.MinValue, SortedSet(anchorOrdinal))))
      )

      // Head B: local head at ord=120 with NO unapplied cross-shard change (metagraphSyncData = None).
      headB <- mkGlobalSnapshot(headBOrdinal, mkGlobalInfo(None), None)
      headBInfo = mkGlobalInfo(None)

      // A same-ordinal LastN sibling cannot outrank the caller resolver that returns `anchor`.
      managerA <- mkManager(
        Dev,
        allEmptyFieldsAddedOrdinals,
        (headA, headAInfo),
        SortedMap(anchorOrdinal -> sameOrdinalSibling)
      )
      managerB <- mkManager(Dev, allEmptyFieldsAddedOrdinals, (headB, headBInfo))

      rA <- runAccept(managerA, ctx, getGlobalSnapshotByOrdinal, anchorView)
      rB <- runAccept(managerB, ctx, getGlobalSnapshotByOrdinal, anchorView)
    } yield expect(anchor.hash =!= sameOrdinalSibling.hash) && expect.eql(rA, rB)
  }

  test(
    "Gate 2 baseline (numShards=1, REAL mainnet fields-added-ordinals): with NO cross-shard metagraphSyncData, a head-vs-anchor " +
      "ordinal difference produces byte-identical stateProof (GREEN today; guards I-PIN's field-emission re-key)"
  ) { res =>
    implicit val (h, j, ks, sp) = res

    // Anchor + both heads sit ABOVE every mainnet field-emission boundary (latest = updatingCombineFunctionSpendActions = 4957662,
    // below setSumFix = 9999999). numShards=1 ⇒ no cross-shard SpendActions exist, so this is the pre-sharding baseline.
    val g2AnchorOrdinal = ord(5100000L)
    val g2HeadAOrdinal = ord(5100050L)
    val g2HeadBOrdinal = ord(5100010L)

    for {
      mgKp <- KeyPairGenerator.makeKeyPair[IO]
      srcKp <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = PublicKeyOps(mgKp.getPublic).toAddress
      source = PublicKeyOps(srcKp.getPublic).toAddress

      // numShards=1: the anchor carries NO spendActions and neither head advertises unapplied cross-shard ordinals.
      anchor <- mkGlobalSnapshot(g2AnchorOrdinal, mkGlobalInfo(None), None)
      anchorView = GlobalSyncView(g2AnchorOrdinal, anchor.hash, anchor.epochProgress)
      getGlobalSnapshotByOrdinal = (o: SnapshotOrdinal) =>
        if (o === g2AnchorOrdinal) anchor.some.pure[IO] else none[Hashed[GlobalIncrementalSnapshot]].pure[IO]

      ctx = CurrencySnapshotContext(
        metagraphId,
        CurrencySnapshotInfo(SortedMap.empty, SortedMap(source -> fundedAmount), None, None, None, None, None, None, None)
      )

      // Two divergent local heads (ordinals differ) — BOTH metagraphSyncData = None (no cross-shard) and BOTH above every boundary.
      headA <- mkGlobalSnapshot(g2HeadAOrdinal, mkGlobalInfo(None), None)
      headB <- mkGlobalSnapshot(g2HeadBOrdinal, mkGlobalInfo(None), None)

      managerA <- mkManager(Mainnet, mainnetFieldsAddedOrdinals, (headA, mkGlobalInfo(None)))
      managerB <- mkManager(Mainnet, mainnetFieldsAddedOrdinals, (headB, mkGlobalInfo(None)))

      rA <- runAccept(managerA, ctx, getGlobalSnapshotByOrdinal, anchorView)
      rB <- runAccept(managerB, ctx, getGlobalSnapshotByOrdinal, anchorView)
    } yield expect.eql(rA, rB)
  }

  /** BLOCKER-1b: with a FIXED pinned anchor and two divergent heads that STRADDLE a fields-added boundary, the recomputed `stateProof` must
    * be byte-identical — proving the I-PIN re-key made `snapshotOrdinalToCheckFields` (the optional-CSI-field emission gate, CSAM ~621) a
    * pure function of the RECORDED `globalSyncView.ordinal`, NOT the node-local head. If it were still head-keyed, a head below the
    * boundary and a head above it would emit a different optional-field set at the SAME anchor and diverge. numShards=1 (no cross-shard).
    */
  private def runTwoHeadsAtAnchor(
    anchorOrd: SnapshotOrdinal,
    headAOrd: SnapshotOrdinal,
    headBOrd: SnapshotOrdinal
  )(implicit res: Res): IO[(CurrencySnapshotStateProof, CurrencySnapshotStateProof)] = {
    implicit val (h, j, ks, sp) = res
    for {
      mgKp <- KeyPairGenerator.makeKeyPair[IO]
      srcKp <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = PublicKeyOps(mgKp.getPublic).toAddress
      source = PublicKeyOps(srcKp.getPublic).toAddress

      anchor <- mkGlobalSnapshot(anchorOrd, mkGlobalInfo(None), None)
      anchorView = GlobalSyncView(anchorOrd, anchor.hash, anchor.epochProgress)
      getGlobalSnapshotByOrdinal = (o: SnapshotOrdinal) =>
        if (o === anchorOrd) anchor.some.pure[IO] else none[Hashed[GlobalIncrementalSnapshot]].pure[IO]

      ctx = CurrencySnapshotContext(
        metagraphId,
        CurrencySnapshotInfo(SortedMap.empty, SortedMap(source -> fundedAmount), None, None, None, None, None, None, None)
      )

      headA <- mkGlobalSnapshot(headAOrd, mkGlobalInfo(None), None)
      headB <- mkGlobalSnapshot(headBOrd, mkGlobalInfo(None), None)
      managerA <- mkManager(Mainnet, mainnetFieldsAddedOrdinals, (headA, mkGlobalInfo(None)))
      managerB <- mkManager(Mainnet, mainnetFieldsAddedOrdinals, (headB, mkGlobalInfo(None)))

      rA <- runAccept(managerA, ctx, getGlobalSnapshotByOrdinal, anchorView)
      rB <- runAccept(managerB, ctx, getGlobalSnapshotByOrdinal, anchorView)
    } yield (rA, rB)
  }

  test(
    "BLOCKER-1b (numShards=1, REAL mainnet fields-added-ordinals): a fixed pinned anchor with two heads STRADDLING each field-emission " +
      "boundary (tessellation3=4409045, checkSyncGlobalSnapshotField=4488000, metagraphSyncData=4915254) => byte-identical stateProof " +
      "(field emission keys off the recorded globalSyncView, not the head)"
  ) { implicit res =>
    // (anchorOrd, headAOrd below the boundary, headBOrd above the boundary). The anchor is placed on both sides of each boundary; the two
    // heads always straddle it. Post-I-PIN every case is head-invariant; a residual head-dependence in `snapshotOrdinalToCheckFields` would
    // flip an optional-field Option and break the eql.
    val cases: List[(SnapshotOrdinal, SnapshotOrdinal, SnapshotOrdinal)] = List(
      (ord(4409044L), ord(4409040L), ord(4409050L)), // anchor just below tessellation3
      (ord(4409045L), ord(4409040L), ord(4409050L)), // anchor at tessellation3
      (ord(4487999L), ord(4487990L), ord(4488010L)), // anchor just below checkSyncGlobalSnapshotField
      (ord(4488001L), ord(4487990L), ord(4488010L)), // anchor just above checkSyncGlobalSnapshotField
      (ord(4915253L), ord(4915250L), ord(4915260L)), // anchor just below metagraphSyncData
      (ord(4915255L), ord(4915250L), ord(4915260L)) // anchor just above metagraphSyncData
    )

    cases.traverse { case (a, hA, hB) => runTwoHeadsAtAnchor(a, hA, hB).map { case (rA, rB) => expect.eql(rA, rB) } }
      .map(_.combineAll)
  }
}
