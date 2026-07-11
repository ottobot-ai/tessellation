package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.node.shared.domain.statechannel._
import io.constellationnetwork.node.shared.infrastructure.snapshot.CurrencySnapshotContextFunctions
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageOrdinal, MessageType}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** Regression guard for removal of authoritative currency adoption. This fixture makes recreation fail and proves the processor cannot
  * advance by trusting signed cumulative fields.
  */
object CurrencyAdoptRootsOnlyAdvanceSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val ordinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(2L))

  /** Build the processor with a `createContext` that always raises, proving there is no alternate adoption path around recreation.
    */
  private def mkProcessor(
    implicit h: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[GlobalSnapshotStateChannelEventsProcessor[IO]] = {
    val validator = new StateChannelValidator[IO] {
      def validate(
        output: io.constellationnetwork.statechannel.StateChannelOutput,
        globalOrdinal: SnapshotOrdinal,
        snapshotFeesInfo: SnapshotFeesInfo
      )(implicit hasher: Hasher[IO]) = IO.pure(output.validNec)
      def validateHistorical(
        output: io.constellationnetwork.statechannel.StateChannelOutput,
        globalOrdinal: SnapshotOrdinal,
        snapshotFeesInfo: SnapshotFeesInfo
      )(implicit hasher: Hasher[IO]) = IO.pure(output.validNec)
    }

    // Diverging stub: `createContext` raises exactly like the real recreate-against-genesis path does (the freeze trigger).
    val divergingContextFns: CurrencySnapshotContextFunctions[IO] = new CurrencySnapshotContextFunctions[IO] {
      def createContext(
        context: CurrencySnapshotContext,
        lastArtifact: Signed[CurrencyIncrementalSnapshot],
        signedArtifact: Signed[CurrencyIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[CurrencySnapshotContext] =
        IO.raiseError(new RuntimeException("createContext diverges (SnapshotDifferentThanExpected) — the #259 freeze trigger"))
    }

    for {
      manager <- GlobalSnapshotStateChannelAcceptanceManager
        .make[IO](None, pullDelay = NonNegLong.MinValue, purgeDelay = NonNegLong.MinValue)
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      reader = io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore(mptStore)
      feeCalculator = FeeCalculator.make[IO](SortedMap.empty) // isFeeRequired = false
      processor = GlobalSnapshotStateChannelEventsProcessor.make[IO](validator, manager, divergingContextFns, feeCalculator, reader)
    } yield processor
  }

  /** An empty-state `CurrencySnapshotInfo` carrying only an optional `lastMessages` — the roots-only shape gl0 commits on the adopt path.
    * Used as the prior (`Right`) Info so the adopted binary is a SECOND incremental (reaches `createContext`).
    */
  private def info(lastMessages: Option[SortedMap[MessageType, Signed[CurrencyMessage]]]): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap.empty,
      balances = SortedMap.empty,
      lastMessages = lastMessages,
      lastFeeTxRefs = None,
      lastAllowSpendRefs = None,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = None,
      activeTokenLocks = None
    )

  private def signedIncremental(
    snapOrdinal: Long,
    lastSnapshotHash: Hash,
    messages: Option[SortedSet[Signed[CurrencyMessage]]],
    mgKeyPair: KeyPair,
    blocks: SortedSet[BlockAsActiveTip] = SortedSet.empty,
    rewards: SortedSet[io.constellationnetwork.schema.transaction.RewardTransaction] = SortedSet.empty,
    stateProof: CurrencySnapshotStateProof = CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None),
    feeTransactions: Option[SortedSet[Signed[io.constellationnetwork.currency.dataApplication.FeeTransaction]]] = None,
    artifacts: Option[SortedSet[io.constellationnetwork.schema.artifact.SharedArtifact]] = None,
    tokenLockBlocks: Option[SortedSet[Signed[io.constellationnetwork.schema.tokenLock.TokenLockBlock]]] = None,
    allowSpendBlocks: Option[SortedSet[Signed[io.constellationnetwork.schema.swap.AllowSpendBlock]]] = None,
    globalSyncView: Option[io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSyncView] = None
  )(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[Signed[CurrencyIncrementalSnapshot]] = {
    val snapshot = CurrencyIncrementalSnapshot(
      SnapshotOrdinal.unsafeApply(snapOrdinal),
      Height.MinValue,
      SubHeight.MinValue,
      lastSnapshotHash,
      blocks,
      rewards,
      SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof,
      EpochProgress.MinValue,
      None,
      messages,
      None,
      feeTransactions,
      artifacts,
      allowSpendBlocks,
      tokenLockBlocks,
      globalSyncView
    )
    forAsyncHasher[IO, CurrencyIncrementalSnapshot](snapshot, mgKeyPair)
  }

  /** A signed `FeeTransaction` source → destination of `amount` — the data-application fee a metagraph's ml0 deducts via
    * `BalanceOpsManager.acceptFeeTxs`. Carried on-wire in `CurrencyIncrementalSnapshot.feeTransactions`, so gl0's adopt derivation can
    * replay it deterministically. Signed by the SOURCE key (the adopt derivation replays already-accepted events without re-validating
    * signatures).
    */
  private def signedFeeTransaction(
    source: Address,
    destination: Address,
    amount: Long,
    dataUpdateRef: Hash,
    sourceKeyPair: KeyPair
  )(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[Signed[io.constellationnetwork.currency.dataApplication.FeeTransaction]] =
    forAsyncHasher[IO, io.constellationnetwork.currency.dataApplication.FeeTransaction](
      io.constellationnetwork.currency.dataApplication.FeeTransaction(
        source = source,
        destination = destination,
        amount = io.constellationnetwork.schema.balance.Amount(NonNegLong.unsafeFrom(amount)),
        dataUpdateRef = dataUpdateRef
      ),
      sourceKeyPair
    )

  /** A signed plain transfer `source → destination` of `amount` (fee 0), parented at `emptyCurrency(metagraphIdentifier)` so the source ref
    * advances to ordinal 1 — the on-disk shape a fresh metagraph wallet's first tx carries. Signed by the SOURCE key (irrelevant to the
    * adopt derivation, which replays already-accepted events without re-validating signatures).
    */
  private def signedTransfer(
    source: Address,
    destination: Address,
    amount: Long,
    mgIdentifier: Hash,
    sourceKeyPair: KeyPair
  )(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[Signed[Transaction]] =
    forAsyncHasher[IO, Transaction](
      Transaction(
        source = source,
        destination = destination,
        amount = TransactionAmount(PosLong.unsafeFrom(amount)),
        fee = TransactionFee(NonNegLong(0L)),
        parent = TransactionReference(TransactionOrdinal(NonNegLong(0L)), mgIdentifier),
        salt = TransactionSalt(0L)
      ),
      sourceKeyPair
    )

  /** Wrap a single signed transaction into a `BlockAsActiveTip` (the on-wire shape `CurrencyIncrementalSnapshot.blocks` carries). The
    * parent BlockReference is arbitrary — the adopt derivation reads only the block's transactions.
    */
  private def blockOf(tx: Signed[Transaction]): BlockAsActiveTip =
    BlockAsActiveTip(
      Signed(
        Block(NonEmptyList.one(BlockReference(Height.MinValue, ProofsHash("0" * 64))), NonEmptySet.one(tx)),
        tx.proofs
      ),
      NonNegLong(0L)
    )

  /** A signed `TokenLock` from `source` locking `amount` (+ `fee`), expiring at `unlockAt`. Carried on-wire in a
    * `CurrencyIncrementalSnapshot.tokenLockBlocks` block; the metagraph's ml0 debits `−(amount + fee)` from the source via
    * `TokenLockOpsManager.updateBalancesByTokenLocks`, so gl0's adopt derivation replays the identical debit. Signed by the SOURCE key (the
    * adopt derivation replays already-accepted events without re-validating signatures).
    */
  private def signedTokenLock(
    source: Address,
    amount: Long,
    fee: Long,
    unlockAt: Option[EpochProgress],
    sourceKeyPair: KeyPair
  )(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[Signed[io.constellationnetwork.schema.tokenLock.TokenLock]] =
    forAsyncHasher[IO, io.constellationnetwork.schema.tokenLock.TokenLock](
      io.constellationnetwork.schema.tokenLock.TokenLock(
        source = source,
        amount = io.constellationnetwork.schema.tokenLock.TokenLockAmount(PosLong.unsafeFrom(amount)),
        fee = io.constellationnetwork.schema.tokenLock.TokenLockFee(NonNegLong.unsafeFrom(fee)),
        parent = io.constellationnetwork.schema.tokenLock
          .TokenLockReference(io.constellationnetwork.schema.tokenLock.TokenLockOrdinal(NonNegLong(0L)), Hash.empty),
        currencyId = None,
        unlockEpoch = unlockAt,
        replaceTokenLockRef = None
      ),
      sourceKeyPair
    )

  /** Wrap a signed token-lock into a `TokenLockBlock` (the on-wire shape `CurrencyIncrementalSnapshot.tokenLockBlocks` carries). The
    * roundId is arbitrary — the adopt derivation reads only the block's token-locks.
    */
  private def tokenLockBlockOf(
    tl: Signed[io.constellationnetwork.schema.tokenLock.TokenLock]
  ): Signed[io.constellationnetwork.schema.tokenLock.TokenLockBlock] =
    Signed(
      io.constellationnetwork.schema.tokenLock.TokenLockBlock(
        io.constellationnetwork.schema.round.RoundId(new java.util.UUID(0L, 0L)),
        NonEmptySet.one(tl)
      ),
      tl.proofs
    )

  /** A signed `AllowSpend` from `source` → `destination` of `amount` (+ `fee`), valid through `lastValidEpoch`. Lives in
    * `CurrencySnapshotInfo.activeAllowSpends` once accepted by ml0. The adopt derivation replays already-accepted events without
    * re-validating signatures, so the SOURCE key is sufficient.
    */
  private def signedAllowSpend(
    source: Address,
    destination: Address,
    amount: Long,
    fee: Long,
    lastValidEpoch: EpochProgress,
    sourceKeyPair: KeyPair
  )(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[Signed[io.constellationnetwork.schema.swap.AllowSpend]] =
    forAsyncHasher[IO, io.constellationnetwork.schema.swap.AllowSpend](
      io.constellationnetwork.schema.swap.AllowSpend(
        source = source,
        destination = destination,
        currencyId = None,
        amount = io.constellationnetwork.schema.swap.SwapAmount(PosLong.unsafeFrom(amount)),
        fee = io.constellationnetwork.schema.swap.AllowSpendFee(NonNegLong.unsafeFrom(fee)),
        parent = io.constellationnetwork.schema.swap
          .AllowSpendReference(io.constellationnetwork.schema.swap.AllowSpendOrdinal(NonNegLong(0L)), Hash.empty),
        lastValidEpochProgress = lastValidEpoch,
        approvers = List.empty
      ),
      sourceKeyPair
    )

  /** Serialize a signed incremental into an SC binary (the on-wire shape `processCurrencySnapshots` decodes). */
  private def binaryOf(
    signed: Signed[CurrencyIncrementalSnapshot],
    prevHash: Hash,
    mgKeyPair: KeyPair
  )(implicit sp: SecurityProvider[IO], h: Hasher[IO], j: JsonSerializer[IO]): IO[Signed[StateChannelSnapshotBinary]] =
    for {
      contentBytes <- JsonSerializer[IO].serialize(signed)
      binary = StateChannelSnapshotBinary(prevHash, contentBytes, SnapshotFee.MinValue)
      signedBinary <- forAsyncHasher(binary, mgKeyPair)
    } yield signedBinary

  /** Extract the per-MG committed-state ordinal + lastMessages from a `processCurrencySnapshots` result. */
  private def committed(
    result: SortedMap[
      Address,
      (NonEmptyList[(Signed[StateChannelSnapshotBinary], Option[CurrencySnapshotWithState])], SortedMap[Address, Balance])
    ],
    mgAddr: Address
  ): Option[(SnapshotOrdinal, Option[SortedMap[MessageType, Signed[CurrencyMessage]]])] =
    result
      .get(mgAddr)
      .flatMap { case (nel, _) => nel.toList.flatMap(_._2).lastOption }
      .collect { case Right((inc, ci)) => (inc.value.ordinal, ci.lastMessages) }

  /** Extract the FULL per-MG committed `CurrencySnapshotInfo` (balances/refs/...) from a `processCurrencySnapshots` result. */
  private def committedInfo(
    result: SortedMap[
      Address,
      (NonEmptyList[(Signed[StateChannelSnapshotBinary], Option[CurrencySnapshotWithState])], SortedMap[Address, Balance])
    ],
    mgAddr: Address
  ): Option[(SnapshotOrdinal, CurrencySnapshotInfo)] =
    result
      .get(mgAddr)
      .flatMap { case (nel, _) => nel.toList.flatMap(_._2).lastOption }
      .collect { case Right((inc, ci)) => (inc.value.ordinal, ci) }

  test("signed cumulative fields cannot bypass full currency recreation") { res =>
    implicit val (h, sp, j) = res
    for {
      processor <- mkProcessor
      mgKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      mgAddr = PublicKeyOps(mgKeyPair.getPublic).toAddress

      // Prior: a `Right` at ordinal 1 — the metagraph is already past genesis in gl0's commitment. This forces the adopted
      // binary below into the second-incremental `Right` branch (the only branch that reaches `createContext`).
      firstIncremental <- signedIncremental(1L, Hash.empty, None, mgKeyPair)
      firstHash <- firstIncremental.toHashed.map(_.hash)
      priorState = Right((firstIncremental, info(None))): CurrencySnapshotWithState
      prior: SortedMap[Address, CurrencySnapshotWithState] =
        SortedMap(mgAddr -> priorState)(Address.OrderingInstance)

      // A second incremental whose signed cumulative fields would have been accepted by the removed shortcut.
      ownerMsg <- forAsyncHasher[IO, CurrencyMessage](
        CurrencyMessage(MessageType.Owner, mgAddr, mgAddr, MessageOrdinal.MinValue),
        mgKeyPair
      )
      // Stamp the matching `stateProof` (lastMessagesProof = hash of the Owner-carrying lastMessages map), exactly as
      // `CurrencySnapshotAcceptanceManager` does (`csi.stateProof(ordinal)`). The adopt path's per-field gate only commits a
      // derived field when its hash equals the committed proof, so the fixture must carry the proof its `messages` imply.
      expectedMessages: SortedMap[MessageType, Signed[CurrencyMessage]] = SortedMap(MessageType.Owner -> ownerMsg)
      expectedInfo = info(Some(expectedMessages))
      committedProof <- expectedInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))
      secondIncremental <- signedIncremental(2L, firstHash, Some(SortedSet(ownerMsg)), mgKeyPair, stateProof = committedProof)
      secondBinary <- binaryOf(secondIncremental, firstHash, mgKeyPair)
      adopted = SortedMap(mgAddr -> NonEmptyList.of(secondBinary))(Address.OrderingInstance)

      result <- processor.processCurrencySnapshots(
        ordinal,
        SortedMap.empty[Address, Balance],
        prior,
        adopted,
        _ => None.pure[IO]
      )
      recreated = committed(result, mgAddr)
    } yield expect(recreated.isEmpty)
  }

}
