package io.constellationnetwork.node.shared.domain.swap

import cats.data.NonEmptyList.{of => nelOf}
import cats.data.{NonEmptySet, Validated}
import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo, CurrencySnapshotStateProof}
import io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSyncView
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.{AllowSpendsConfig, MinMax}
import io.constellationnetwork.node.shared.domain.collateral.LatestBalances
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage}
import io.constellationnetwork.node.shared.domain.swap.AllowSpendValidator.AllowSpendValidationErrorOr
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import fs2.Stream
import weaver.MutableIOSuite

/** Regression suite for the `lastNGlobalSnapshotStorage` fallback in `AllowSpendService.offer`.
  *
  * Cite: docs/nakamoto/E2E-FLAKE-ANALYSIS.md Mode 2 / Priority 2.
  *
  * The bug: when cl1's latest currency snapshot has `globalSyncView=None` (cl0 produced it before
  * receiving its first gl0 snapshot for this metagraph), `offer` previously fell back to
  * `EpochProgress.MinValue`, producing `currentEpochProgress=1` even when cl1's own
  * `lastNGlobalSnapshotStorage` had a higher value. The validator then rejected with
  * `TooFarLastValidEpochProgress`.
  *
  * Fix: consult `lastNGlobalSnapshotStorage.get` before defaulting to MinValue.
  */
object AllowSpendServiceFallbackSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (j, h, sp)

  // ---- Fixtures ---------------------------------------------------------------------------------

  private val testAddress: Address = Address("DAG0y4eLqhhXUafeE3mgBstezPTnr8L3tZjAtMWB")
  private val testDestination: Address = Address("DAG07tqNLYW8jHU9emXcRTT3CfgCUoumwcLghopd")

  private val testSignatureProof = SignatureProof(Id(Hex("")), Signature(Hex("")))
  private val testProofs = NonEmptySet.one(testSignatureProof)

  private def mkAllowSpend(lastValidEpochProgress: EpochProgress): IO[Hashed[AllowSpend]] = {
    val tx = AllowSpend(
      source = testAddress,
      destination = testDestination,
      currencyId = None,
      amount = SwapAmount(100L),
      fee = AllowSpendFee(0L),
      parent = AllowSpendReference.empty,
      lastValidEpochProgress = lastValidEpochProgress,
      approvers = List.empty
    )
    IO.pure(Hashed(Signed(tx, testProofs), Hash.empty, ProofsHash("")))
  }

  // Minimal `Hashed[CurrencyIncrementalSnapshot]` with the desired `globalSyncView`.
  private def mkCurrencyIncrementalSnapshot(globalSyncView: Option[GlobalSyncView]): Hashed[CurrencyIncrementalSnapshot] = {
    val stateProof = CurrencySnapshotStateProof(
      lastTxRefsProof = Hash.empty,
      balancesProof = Hash.empty,
      lastMessagesProof = None,
      lastFeeTxRefsProof = None,
      lastAllowSpendRefsProof = None,
      activeAllowSpends = None,
      globalSnapshotSync = None,
      lastTokenLockRefsProof = None,
      activeTokenLocks = None
    )
    val cis = CurrencyIncrementalSnapshot(
      ordinal = SnapshotOrdinal(NonNegLong(10L)),
      height = Height(1L),
      subHeight = SubHeight(0L),
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      rewards = SortedSet.empty,
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = stateProof,
      epochProgress = EpochProgress(NonNegLong(7L)),
      dataApplication = None,
      messages = None,
      globalSnapshotSyncs = None,
      feeTransactions = None,
      artifacts = None,
      allowSpendBlocks = None,
      tokenLockBlocks = None,
      globalSyncView = globalSyncView
    )
    Hashed(Signed(cis, testProofs), Hash.empty, ProofsHash(""))
  }

  // Minimal `Hashed[GlobalIncrementalSnapshot]` with the desired epochProgress.
  private def mkGlobalIncrementalSnapshot(epochProgress: EpochProgress): Hashed[GlobalIncrementalSnapshot] = {
    val stateProof = GlobalSnapshotStateProof(
      lastStateChannelSnapshotHashesProof = Hash.empty,
      lastTxRefsProof = Hash.empty,
      balancesProof = Hash.empty,
      lastCurrencySnapshotsProof = None,
      activeAllowSpends = None,
      activeTokenLocks = None,
      tokenLockBalances = None,
      lastAllowSpendRefs = None,
      lastTokenLockRefs = None,
      updateNodeParameters = None,
      activeDelegatedStakes = None,
      delegatedStakesWithdrawals = None,
      activeNodeCollaterals = None,
      nodeCollateralWithdrawals = None,
      priceState = None,
      lastGlobalSnapshotsWithCurrency = None,
      mptRoot = None,
      historicalStakeSnapshots = None
    )
    val gis = GlobalIncrementalSnapshot(
      ordinal = SnapshotOrdinal(NonNegLong(100L)),
      height = Height(1L),
      subHeight = SubHeight(0L),
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      rewards = SortedSet.empty,
      delegateRewards = Some(SortedMap.empty),
      epochProgress = epochProgress,
      nextFacilitators = nelOf(PeerId(Hex("00"))),
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = stateProof,
      allowSpendBlocks = Some(SortedSet.empty),
      tokenLockBlocks = Some(SortedSet.empty),
      spendActions = Some(SortedMap.empty),
      updateNodeParameters = Some(SortedMap.empty),
      artifacts = Some(SortedSet.empty),
      activeDelegatedStakes = Some(SortedMap.empty),
      delegatedStakesWithdrawals = Some(SortedMap.empty),
      activeNodeCollaterals = Some(SortedMap.empty),
      nodeCollateralWithdrawals = Some(SortedMap.empty)
    )
    Hashed(Signed(gis, testProofs), Hash.empty, ProofsHash(""))
  }

  // ---- Stubs ------------------------------------------------------------------------------------

  /** Stub `LastSnapshotStorage` whose only meaningful operations are `get`/`getCombined`. The
    * remaining methods are no-ops (the `offer` codepath under test only consults `get`).
    */
  private def mkLastSnapshotStorage(
    initial: Option[(Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]
  ): LastSnapshotStorage[IO, CurrencyIncrementalSnapshot, CurrencySnapshotInfo] with LatestBalances[IO] =
    new LastSnapshotStorage[IO, CurrencyIncrementalSnapshot, CurrencySnapshotInfo] with LatestBalances[IO] {
      def set(snapshot: Hashed[CurrencyIncrementalSnapshot], state: CurrencySnapshotInfo): IO[Unit] = IO.unit
      def setInitial(snapshot: Hashed[CurrencyIncrementalSnapshot], state: CurrencySnapshotInfo): IO[Unit] = IO.unit
      def setForRecovery(snapshot: Hashed[CurrencyIncrementalSnapshot], state: CurrencySnapshotInfo): IO[Unit] = IO.unit
      def clear: IO[Unit] = IO.unit
      def get: IO[Option[Hashed[CurrencyIncrementalSnapshot]]] = IO.pure(initial.map(_._1))
      def getCombined: IO[Option[(Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]] = IO.pure(initial)
      def getCombinedStream: Stream[IO, Option[(Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]] =
        Stream.emit(initial)
      def getOrdinal: IO[Option[SnapshotOrdinal]] = IO.pure(initial.map(_._1.ordinal))
      def getHeight: IO[Option[io.constellationnetwork.schema.height.Height]] = IO.pure(initial.map(_._1.height))
      def getLatestBalances: IO[Option[Map[Address, Balance]]] = IO.pure(initial.map(_._2.balances.toMap))
      def getLatestBalancesStream: Stream[IO, Map[Address, Balance]] = Stream.empty
    }

  /** Stub `LastNGlobalSnapshotStorage` whose only meaningful operation is `get`. */
  private def mkLastNGlobalSnapshotStorage(initial: Option[Hashed[GlobalIncrementalSnapshot]]): LastNGlobalSnapshotStorage[IO] =
    new LastNGlobalSnapshotStorage[IO] {
      def set(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): IO[Unit] = IO.unit
      def setInitial(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): IO[Unit] = IO.unit
      def setForRecovery(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): IO[Unit] = IO.unit
      def clear: IO[Unit] = IO.unit
      def get: IO[Option[Hashed[GlobalIncrementalSnapshot]]] = IO.pure(initial)
      def getCombined: IO[Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] = IO.pure(None)
      def getCombinedStream: Stream[IO, Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] = Stream.empty
      def getOrdinal: IO[Option[SnapshotOrdinal]] = IO.pure(initial.map(_.ordinal))
      def getHeight: IO[Option[io.constellationnetwork.schema.height.Height]] = IO.pure(initial.map(_.height))
      def getLastN: IO[List[Hashed[GlobalIncrementalSnapshot]]] = IO.pure(initial.toList)
      def setInitialFetchingGL0(
        snapshot: Hashed[GlobalIncrementalSnapshot],
        state: GlobalSnapshotInfo,
        globalSnapshotFetcher: Option[GlobalFetcher],
        fetchGL0Function: Option[FetchFunction]
      ): IO[Unit] = IO.unit
    }

  /** Stub validator that captures the `lastGlobalSnapshotEpochProgress` argument and returns
    * Invalid (so `offer` exits before touching the snapshot stream / storage).
    */
  private def mkRecordingValidator(
    captured: Ref[IO, Option[Option[EpochProgress]]]
  ): AllowSpendValidator[IO] =
    new AllowSpendValidator[IO] {
      def validate(
        signedAllowSpend: Signed[AllowSpend],
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[AllowSpendValidationErrorOr[Signed[AllowSpend]]] =
        captured.set(lastGlobalSnapshotEpochProgress.some).as(
          Validated.invalidNec[AllowSpendValidator.AllowSpendValidationError, Signed[AllowSpend]](
            AllowSpendValidator.NotSignedBySourceAddressOwner
          )
        )
    }

  private val allowSpendsCfg = AllowSpendsConfig(
    lastValidEpochProgress = MinMax(min = NonNegLong(1L), max = NonNegLong(60L))
  )

  // ---- Tests ------------------------------------------------------------------------------------

  test("offer uses lastNGlobalSnapshotStorage epochProgress when cis.globalSyncView=None") { res =>
    implicit val (_, hasher, _) = res

    val gisEpoch = EpochProgress(NonNegLong(516L))
    val cisNoSync = mkCurrencyIncrementalSnapshot(globalSyncView = None)
    val gis = mkGlobalIncrementalSnapshot(gisEpoch)
    val cisInfo = CurrencySnapshotInfo(SortedMap.empty, SortedMap.empty, None, None, None, None, None, None, None)

    val lastSnapshotStorage = mkLastSnapshotStorage(Some((cisNoSync, cisInfo)))
    val lastNGlobalSnapshotStorage = mkLastNGlobalSnapshotStorage(Some(gis))

    for {
      captured <- Ref[IO].of(Option.empty[Option[EpochProgress]])
      contextualValidator = ContextualAllowSpendValidator.make(None, None, allowSpendsCfg)
      allowSpendStorage <- AllowSpendStorage.make[IO](AllowSpendReference.empty, contextualValidator)
      service = AllowSpendService.make[IO, CurrencySnapshotStateProof, CurrencyIncrementalSnapshot, CurrencySnapshotInfo](
        allowSpendStorage,
        lastSnapshotStorage,
        lastNGlobalSnapshotStorage,
        mkRecordingValidator(captured)
      )
      tx <- mkAllowSpend(lastValidEpochProgress = EpochProgress(NonNegLong(540L)))
      _ <- service.offer(tx)
      observed <- captured.get
    } yield expect.same(observed, Some(Some(gisEpoch)))
  }

  test("offer falls back to MinValue only when both lastSnapshotStorage and lastNGlobalSnapshotStorage are empty") { res =>
    implicit val (_, hasher, _) = res

    val lastSnapshotStorage = mkLastSnapshotStorage(None)
    val lastNGlobalSnapshotStorage = mkLastNGlobalSnapshotStorage(None)

    for {
      captured <- Ref[IO].of(Option.empty[Option[EpochProgress]])
      contextualValidator = ContextualAllowSpendValidator.make(None, None, allowSpendsCfg)
      allowSpendStorage <- AllowSpendStorage.make[IO](AllowSpendReference.empty, contextualValidator)
      service = AllowSpendService.make[IO, CurrencySnapshotStateProof, CurrencyIncrementalSnapshot, CurrencySnapshotInfo](
        allowSpendStorage,
        lastSnapshotStorage,
        lastNGlobalSnapshotStorage,
        mkRecordingValidator(captured)
      )
      tx <- mkAllowSpend(lastValidEpochProgress = EpochProgress(NonNegLong(540L)))
      _ <- service.offer(tx)
      observed <- captured.get
    } yield expect.same(observed, Some(Some(EpochProgress.MinValue)))
  }

  test("offer prefers cis.globalSyncView when it is set (regression: lastN must not override)") { res =>
    implicit val (_, hasher, _) = res

    val cisEpoch = EpochProgress(NonNegLong(42L))
    val gisEpoch = EpochProgress(NonNegLong(516L))
    val syncView = GlobalSyncView(SnapshotOrdinal(NonNegLong(50L)), Hash.empty, cisEpoch)
    val cisWithSync = mkCurrencyIncrementalSnapshot(globalSyncView = Some(syncView))
    val gis = mkGlobalIncrementalSnapshot(gisEpoch)
    val cisInfo = CurrencySnapshotInfo(SortedMap.empty, SortedMap.empty, None, None, None, None, None, None, None)

    val lastSnapshotStorage = mkLastSnapshotStorage(Some((cisWithSync, cisInfo)))
    val lastNGlobalSnapshotStorage = mkLastNGlobalSnapshotStorage(Some(gis))

    for {
      captured <- Ref[IO].of(Option.empty[Option[EpochProgress]])
      contextualValidator = ContextualAllowSpendValidator.make(None, None, allowSpendsCfg)
      allowSpendStorage <- AllowSpendStorage.make[IO](AllowSpendReference.empty, contextualValidator)
      service = AllowSpendService.make[IO, CurrencySnapshotStateProof, CurrencyIncrementalSnapshot, CurrencySnapshotInfo](
        allowSpendStorage,
        lastSnapshotStorage,
        lastNGlobalSnapshotStorage,
        mkRecordingValidator(captured)
      )
      tx <- mkAllowSpend(lastValidEpochProgress = EpochProgress(NonNegLong(60L)))
      _ <- service.offer(tx)
      observed <- captured.get
    } yield expect.same(observed, Some(Some(cisEpoch)))
  }
}
