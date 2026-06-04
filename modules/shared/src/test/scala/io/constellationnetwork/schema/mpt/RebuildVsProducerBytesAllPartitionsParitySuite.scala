package io.constellationnetwork.schema.mpt

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.merkletree.{Proof, ProofEntry}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.PricingUpdate
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.{StateChangesAccumulator, applyAccumulatorToGSI}
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, HistoricalStakeSnapshot, StakeDistribution}
import io.constellationnetwork.schema.node._
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.priceOracle._
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.MptRoot
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.{Signed, signature}

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.string.MatchesRegex
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt, PosLong}
import weaver.MutableIOSuite

/** Correct-by-construction guard for the ml0 resync verify gate (`recomputedRoot === signed mptRoot` in
  * `currency-l0/.../StateChannel.resyncToCanonical`): for the SAME logical post-state, the cumulative incremental writer
  * (`MptStore.syncFromStateChanges` — the producer's signed root) and the from-GSI rebuild (`MptStore.syncFromGlobalSnapshotInfo` — ml0's
  * bootstrap/recovery seeding) MUST land the same MPT root (read via `getRootHashForOrdinal`, the FULL in-store root the gate compares —
  * SystemIndex/ActiveAddressIndex partitions included, exactly as the producer signs over `overlay.allEntriesAsBytesWithHandle`).
  *
  * If the two paths diverge for ANY partition, ml0's rebuilt base ≠ the producer's signed base; the gate rejects gl0's GSI (prod: 94×
  * reject / 114× idle on the §3 NIPoPoW `historicalStakeSnapshots` partition, which `syncFromGlobalSnapshotInfo` was silently NOT writing).
  *
  * This suite drives one accumulator that populates EVERY partition (incl. `historicalStakeSnapshots`), derives the matching GSI via the
  * canonical pairing `applyAccumulatorToGSI`, and asserts the two store roots agree. A single full-population equality therefore catches
  * ANY future partition omission in `syncFromGlobalSnapshotInfo` — not just the historical-stake gap it was introduced for.
  */
object RebuildVsProducerBytesAllPartitionsParitySuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  // Node-collateral-withdrawal expiry index needs a limit so the NCW expiry bucket is populated on BOTH paths.
  private val ncwLimit: EpochProgress = EpochProgress(NonNegLong(100L))
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit(ncwLimit.some)

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val testSignature = signature.Signature(Hex(""))
  private val testSignatureProof = signature.SignatureProof(Id(Hex("")), testSignature)
  private val testProofs = NonEmptySet.one(testSignatureProof)
  private val ord = SnapshotOrdinal(NonNegLong(100L))
  private val v0 = SnapshotVersion(Refined.unsafeApply[String, MatchesRegex["^(0\\.0\\.1|1\\.0\\.0)$"]]("0.0.1"))

  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  private def freshStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
    } yield store

  // --- per-partition record builders (shapes mirror MptFieldCoverageSuite / IncrementalVsRebuildRemovalParitySuite) ---

  private def mkAllowSpend(source: Address, dest: Address, expireAt: EpochProgress, label: String): Signed[AllowSpend] =
    Signed(
      AllowSpend(
        source = source,
        destination = dest,
        currencyId = None,
        amount = SwapAmount(PosLong(100L)),
        fee = AllowSpendFee(NonNegLong(0L)),
        parent = AllowSpendReference(AllowSpendOrdinal(NonNegLong(0L)), testHash(s"as-parent-$label")),
        lastValidEpochProgress = expireAt,
        approvers = List.empty
      ),
      testProofs
    )

  private def mkTokenLock(source: Address, unlockAt: Option[EpochProgress], label: String): Signed[TokenLock] =
    Signed(
      TokenLock(
        source,
        TokenLockAmount(PosLong(200L)),
        TokenLockFee(NonNegLong(0L)),
        TokenLockReference(TokenLockOrdinal(NonNegLong(0L)), testHash(s"tl-parent-$label")),
        None,
        unlockAt,
        None
      ),
      testProofs
    )

  private def mkStake(source: Address, nodeId: peer.PeerId, label: String): DelegatedStakeRecord =
    DelegatedStakeRecord(
      Signed(
        UpdateDelegatedStake.Create(
          source,
          nodeId,
          DelegatedStakeAmount(NonNegLong(1000L)),
          DelegatedStakeFee(0L),
          testHash(s"ds-tlr-$label")
        ),
        NonEmptySet.one[signature.SignatureProof](signature.SignatureProof(nodeId.toId, signature.Signature(Hex(Hash.empty.value))))
      ),
      ord,
      Amount(NonNegLong(0L)),
      None,
      None
    )

  private def mkStakeWithdrawal(source: Address, nodeId: peer.PeerId, label: String): PendingDelegatedStakeWithdrawal =
    PendingDelegatedStakeWithdrawal(
      Signed(
        UpdateDelegatedStake.Create(
          source,
          nodeId,
          DelegatedStakeAmount(NonNegLong(1000L)),
          DelegatedStakeFee(0L),
          testHash(s"dsw-tlr-$label")
        ),
        NonEmptySet.one[signature.SignatureProof](signature.SignatureProof(nodeId.toId, signature.Signature(Hex(Hash.empty.value))))
      ),
      Amount(NonNegLong(0L)),
      ord,
      EpochProgress(NonNegLong(10L))
    )

  private def mkCollateral(source: Address, nodeId: peer.PeerId, label: String): NodeCollateralRecord =
    NodeCollateralRecord(
      Signed(
        UpdateNodeCollateral.Create(
          source,
          nodeId,
          NodeCollateralAmount(NonNegLong(1_000_000L)),
          NodeCollateralFee(NonNegLong(0L)),
          testHash(s"nc-tlr-$label")
        ),
        testProofs
      ),
      ord
    )

  private def mkNcWithdrawal(
    source: Address,
    nodeId: peer.PeerId,
    createdAt: EpochProgress,
    label: String
  ): PendingNodeCollateralWithdrawal =
    PendingNodeCollateralWithdrawal(
      Signed(
        UpdateNodeCollateral.Create(
          source,
          nodeId,
          NodeCollateralAmount(NonNegLong(1_000_000L)),
          NodeCollateralFee(NonNegLong(0L)),
          testHash(s"ncw-tlr-$label")
        ),
        testProofs
      ),
      ord,
      createdAt
    )

  private def mkUnp(source: Address, label: String): Signed[UpdateNodeParameters] =
    Signed(
      UpdateNodeParameters(
        source = source,
        delegatedStakeRewardParameters = DelegatedStakeRewardParameters(RewardFraction.unsafeFrom(10_000_000)),
        nodeMetadataParameters = NodeMetadataParameters(name = s"node-$label", description = s"desc-$label"),
        parent = UpdateNodeParametersReference(UpdateNodeParametersOrdinal(0L), testHash(s"unp-p-$label"))
      ),
      testProofs
    )

  private def mkPriceRecord(value: Long): PriceRecord = {
    val pu = PricingUpdate(PriceFraction(TokenPair.DAG_USD, NonNegFraction.unsafeFrom(value, 1L)))
    PriceRecord(
      currentPrice = pu,
      upcomingPrice = pu,
      currentSum = pu,
      currentNumEvents = PosInt(1),
      nextWindowChange = EpochProgress(NonNegLong(1000L)),
      updatedAt = EpochProgress(NonNegLong(500L))
    )
  }

  private def currencyInfo(addr: Address, balance: Long): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap(addr -> TransactionReference(TransactionOrdinal(NonNegLong(1L)), testHash(s"csi-tx-$balance"))),
      balances = SortedMap(addr -> Balance(NonNegLong.unsafeFrom(balance))),
      lastMessages = None,
      lastFeeTxRefs = None,
      lastAllowSpendRefs = None,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = None,
      activeTokenLocks = None
    )

  private def signedIncremental(
    snapOrdinal: Long
  )(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[Signed[CurrencyIncrementalSnapshot]] = {
    val snapshot = CurrencyIncrementalSnapshot(
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
      globalSyncView = None,
      version = v0
    )
    KeyPairGenerator.makeKeyPair[IO].flatMap(kp => Signed.forAsyncHasher[IO, CurrencyIncrementalSnapshot](snapshot, kp))
  }

  /** Build a `StateChangesAccumulator` populating EVERY partition with exactly one representative record, including
    * `historicalStakeSnapshots`. The expiry-index `SystemIndexDelta`s are matched to the active allow-spend / token-lock /
    * node-collateral-withdrawal records (keys hashed the same way the managers emit them) so the incremental path's SystemIndex writes
    * equal what the rebuild derives from the resulting GSI. Returns the accumulator (the GSI is derived from it via
    * `applyAccumulatorToGSI`).
    */
  private def fullyPopulatedAccumulator(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[StateChangesAccumulator] =
    for {
      mgAddr <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      a1 <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      a2 <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      holder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      nodeKp <- KeyPairGenerator.makeKeyPair[IO]
      nodeAddr = nodeKp.getPublic.toAddress
      nodeId = nodeKp.getPublic.toId
      peerId = nodeId.toPeerId

      // Active records + their expiry-index keys (hashed exactly as the managers do).
      asExpiry = EpochProgress(NonNegLong(500L))
      asG = mkAllowSpend(a1, a2, asExpiry, "g")
      asGHashed <- asG.toHashed
      asM = mkAllowSpend(a1, a2, EpochProgress(NonNegLong(600L)), "m")
      asMHashed <- asM.toHashed

      tlUnlock = EpochProgress(NonNegLong(700L))
      tl = mkTokenLock(a1, tlUnlock.some, "x")
      tlHashed <- tl.toHashed

      ncwCreatedAt = EpochProgress(NonNegLong(200L))
      ncw = mkNcWithdrawal(nodeAddr, peerId, ncwCreatedAt, "x")
      ncwHashed <- ncw.event.toHashed
      ncwExpiry = ncwCreatedAt |+| ncwLimit

      currencyEntry <- signedIncremental(7L).map(s => (s, currencyInfo(mgAddr, 555L)).asRight[Signed[CurrencySnapshot]])

      stakeDist = StakeDistribution(SortedMap(peerId -> BigInt(123456789L)))
    } yield
      StateChangesAccumulator(
        lastStateChannelSnapshotHashes = SortedMap(mgAddr -> testHash("scsh")),
        lastTxRefs = SortedMap(a1 -> TransactionReference(TransactionOrdinal(NonNegLong(3L)), testHash("txref"))),
        balances = SortedMap(a1 -> Balance(NonNegLong(1000L)), a2 -> Balance(NonNegLong(2000L))),
        lastCurrencySnapshots = SortedMap(mgAddr -> currencyEntry),
        lastCurrencySnapshotsProofs =
          SortedMap(mgAddr -> Proof(NonEmptyList.one(ProofEntry(testHash("cs-target"), Right(testHash("cs-sibling")))))),
        activeAllowSpends = SortedMap(
          Option.empty[Address] -> SortedMap(a1 -> SortedSet(asG)),
          mgAddr.some -> SortedMap(a1 -> SortedSet(asM))
        ),
        activeTokenLocks = SortedMap(a1 -> SortedSet(tl)),
        tokenLockBalances = SortedMap(mgAddr -> SortedMap(holder -> Balance(NonNegLong(777L)))),
        lastAllowSpendRefs = SortedMap(a1 -> AllowSpendReference(AllowSpendOrdinal(NonNegLong(2L)), testHash("lasr"))),
        lastTokenLockRefs = SortedMap(a1 -> TokenLockReference(TokenLockOrdinal(NonNegLong(2L)), testHash("ltlr"))),
        activeDelegatedStakes = SortedMap(a1 -> SortedSet(mkStake(a1, peerId, "x"))),
        delegatedStakesWithdrawals = SortedMap(a1 -> SortedSet(mkStakeWithdrawal(a1, peerId, "x"))),
        activeNodeCollaterals = SortedMap(nodeAddr -> SortedSet(mkCollateral(nodeAddr, peerId, "x"))),
        nodeCollateralWithdrawals = SortedMap(nodeAddr -> SortedSet(ncw)),
        metagraphSyncData = SortedMap(mgAddr -> MetagraphSyncDataInfo.empty),
        updateNodeParameters = SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)](nodeId -> ((mkUnp(nodeAddr, "u"), ord))),
        priceState = SortedMap[TokenPair, PriceRecord](TokenPair.DAG_USD -> mkPriceRecord(99L)),
        allowSpendExpiryIndex = SystemIndexDelta.EpochBucket[AllowSpendExpiryKey](
          adds = SortedMap(
            asExpiry -> Set(AllowSpendExpiryKey(None, a1, asGHashed.hash)),
            EpochProgress(NonNegLong(600L)) -> Set(AllowSpendExpiryKey(mgAddr.some, a1, asMHashed.hash))
          )
        ),
        tokenLockExpiryIndex = SystemIndexDelta.EpochBucket[TokenLockExpiryKey](
          adds = SortedMap(tlUnlock -> Set(TokenLockExpiryKey(a1, tlHashed.hash)))
        ),
        nodeCollateralWithdrawalExpiryIndex = SystemIndexDelta.EpochBucket[NodeCollateralWithdrawalExpiryKey](
          adds = SortedMap(ncwExpiry -> Set(NodeCollateralWithdrawalExpiryKey(nodeAddr, ncwHashed.hash)))
        ),
        historicalStakeSnapshots = SortedMap(EtaPeriod(1L) -> HistoricalStakeSnapshot(stakeDist, testHash("eta-period-1")))
      )

  /** Cumulative incremental writer root (producer's signed root) — apply the accumulator, read the FULL in-store root for `ord`. */
  private def incrementalRoot(acc: StateChangesAccumulator)(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[Option[MptRoot]] =
    for {
      store <- freshStore
      _ <- store.syncFromStateChanges(acc, ord)
      root <- store.underlying.getRootHashForOrdinal(ord)
    } yield root

  /** From-GSI rebuild root (ml0's resync seeding) — derive the GSI from the accumulator, seed, read the FULL in-store root for `ord`. */
  private def rebuildRoot(info: GlobalSnapshotInfo)(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[Option[MptRoot]] =
    for {
      store <- freshStore
      _ <- store.syncFromGlobalSnapshotInfo(info, ord)
      root <- store.underlying.getRootHashForOrdinal(ord)
    } yield root

  test("FULL post-state (every partition, incl. historicalStakeSnapshots): incremental writer root === from-GSI rebuild root") { res =>
    implicit val (h, sp, js) = res
    for {
      acc <- fullyPopulatedAccumulator
      info = applyAccumulatorToGSI(GlobalSnapshotInfo.empty, acc)
      incremental <- incrementalRoot(acc)
      rebuilt <- rebuildRoot(info)
    } yield
      expect.all(
        clue(incremental).isDefined,
        clue(rebuilt).isDefined,
        clue(incremental) === clue(rebuilt)
      )
  }

  test("historicalStakeSnapshots-only post-state (multiple periods): incremental writer root === from-GSI rebuild root") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      peerId = kp.getPublic.toId.toPeerId
      acc = StateChangesAccumulator(
        historicalStakeSnapshots = SortedMap(
          EtaPeriod(1L) -> HistoricalStakeSnapshot(StakeDistribution(SortedMap(peerId -> BigInt(111L))), testHash("eta-1")),
          EtaPeriod(2L) -> HistoricalStakeSnapshot(StakeDistribution(SortedMap(peerId -> BigInt(222L))), testHash("eta-2"))
        )
      )
      info = applyAccumulatorToGSI(GlobalSnapshotInfo.empty, acc)
      incremental <- incrementalRoot(acc)
      rebuilt <- rebuildRoot(info)
    } yield
      expect.all(
        clue(incremental).isDefined,
        clue(rebuilt).isDefined,
        clue(incremental) === clue(rebuilt)
      )
  }
}
