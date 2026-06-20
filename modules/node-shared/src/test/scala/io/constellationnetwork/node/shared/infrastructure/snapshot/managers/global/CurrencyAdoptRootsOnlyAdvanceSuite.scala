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
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.GlobalSnapshotStateChannelEventsProcessor.CurrencyAdoptionMode
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

/** #259 roots-only adopt — the per-MG currency commitment ADVANCES PAST GENESIS even when `createContext` WOULD diverge.
  *
  * This is the regression guard for the consensus freeze: at `numShards>1`, gl0's `lastCurrencySnapshots` was stuck at genesis for all
  * metagraphs because the adopt path re-derived each currency snapshot via `CurrencySnapshotContextFunctions.createContext` (recreate
  * proposal artifact + byte-equality) against gl0's FROZEN prior. The recreate produces ordinal 1, never matches the incoming ordinal N →
  * `SnapshotDifferentThanExpected` → `CannotCreateContext` → caught by `processCurrencySnapshots`' `handleErrorWith` → the
  * binary/incremental is dropped and the commitment never advances.
  *
  * The fix (`CurrencyAdoptionMode.AdoptFromSignedFields`) advances the commitment DIRECTLY from the adopted, committee-attested signed
  * binary's OWN committed fields — ordinal + owner/staking `messages` (carried forward) — with NO `createContext`. gl0 commits the
  * metagraph's state ROOT, not its full balance map (balances live at the metagraph's ml0).
  *
  * Setup that makes "createContext WOULD diverge" CONCRETE: the prior is a `Right` (a metagraph already past genesis in gl0's commitment,
  * at ordinal 1), and the adopted binary is the SECOND incremental (ordinal 2) — the exact case that reaches
  * `applyCurrencySnapshot`/`createContext`. The `CurrencySnapshotContextFunctions` stub RAISES on `createContext`, simulating the
  * `SnapshotDifferentThanExpected` divergence the real freeze hits. Then:
  *   - `Recreate` (the legacy default) calls the raising `createContext`; the error is caught and the commitment stays FROZEN at ordinal 1
  *     — reproducing the freeze.
  *   - `AdoptFromSignedFields` never calls `createContext`; the commitment ADVANCES to ordinal 2 and the owner message is carried forward
  *     into `lastMessages` (the genuine cross-MG fee/config dependency `getFeeAddresses` reads).
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

  /** Build the processor with a `createContext` that ALWAYS RAISES — modeling the `SnapshotDifferentThanExpected` / `CannotCreateContext`
    * divergence the freeze hits when gl0 recreates an ordinal-N currency snapshot against its genesis prior. Under `Recreate` this raise is
    * reached (and caught → commitment frozen); under `AdoptFromSignedFields` it is never reached.
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

  test("adopt path: second incremental advances the per-MG commitment past genesis where createContext diverges") { res =>
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

      // The adopted SECOND incremental (ordinal 2), carrying an Owner message — verifies roots-only carry-forward of lastMessages.
      ownerMsg <- forAsyncHasher[IO, CurrencyMessage](
        CurrencyMessage(MessageType.Owner, mgAddr, mgAddr, MessageOrdinal.MinValue),
        mgKeyPair
      )
      secondIncremental <- signedIncremental(2L, firstHash, Some(SortedSet(ownerMsg)), mgKeyPair)
      secondBinary <- binaryOf(secondIncremental, firstHash, mgKeyPair)
      adopted = SortedMap(mgAddr -> NonEmptyList.of(secondBinary))(Address.OrderingInstance)

      // Legacy `Recreate`: reaches the raising `createContext`; the error is caught → commitment FROZEN at the prior (ordinal 1).
      recreateResult <- processor.processCurrencySnapshots(
        ordinal,
        SortedMap.empty[Address, Balance],
        prior,
        adopted,
        _ => None.pure[IO],
        CurrencyAdoptionMode.Recreate
      )

      // Roots-only `AdoptFromSignedFields`: never calls `createContext`; commitment ADVANCES to ordinal 2.
      adoptResult <- processor.processCurrencySnapshots(
        ordinal,
        SortedMap.empty[Address, Balance],
        prior,
        adopted,
        _ => None.pure[IO],
        CurrencyAdoptionMode.AdoptFromSignedFields
      )

      recreateCommitted = committed(recreateResult, mgAddr)
      adoptCommitted = committed(adoptResult, mgAddr)
    } yield
      expect.all(
        // Recreate reproduces the freeze: createContext diverged, the second incremental was dropped, so the committed
        // state is NOT the advanced ordinal — either no new state was produced, or it stayed at the prior ordinal 1.
        recreateCommitted.forall { case (ord, _) => ord.value.value != 2L },
        // AdoptFromSignedFields advances the commitment to ordinal 2 — the freeze is broken.
        adoptCommitted.exists { case (ord, _) => ord.value.value == 2L },
        // The advanced commitment is the EXACT signed binary's ordinal (committed from the binary's own field, not recreated).
        adoptCommitted.map(_._1) == Some(SnapshotOrdinal(NonNegLong(2L))),
        // The owner message is carried forward into lastMessages — the genuine cross-MG fee/config dependency is preserved.
        adoptCommitted.flatMap(_._2).flatMap(_.get(MessageType.Owner)).map(_.value.address) == Some(mgAddr)
      )
  }

  test("adopt path: lastMessages carry-forward keeps a prior owner when the new snapshot has no messages") { res =>
    implicit val (h, sp, j) = res
    for {
      processor <- mkProcessor
      mgKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      mgAddr = PublicKeyOps(mgKeyPair.getPublic).toAddress

      // Prior `Right` at ordinal 1 ALREADY carries an Owner in lastMessages.
      priorOwner <- forAsyncHasher[IO, CurrencyMessage](
        CurrencyMessage(MessageType.Owner, mgAddr, mgAddr, MessageOrdinal.MinValue),
        mgKeyPair
      )
      priorMessages: SortedMap[MessageType, Signed[CurrencyMessage]] = SortedMap(MessageType.Owner -> priorOwner)
      firstIncremental <- signedIncremental(1L, Hash.empty, None, mgKeyPair)
      firstHash <- firstIncremental.toHashed.map(_.hash)
      priorState = Right((firstIncremental, info(Some(priorMessages)))): CurrencySnapshotWithState
      prior: SortedMap[Address, CurrencySnapshotWithState] =
        SortedMap(mgAddr -> priorState)(Address.OrderingInstance)

      // The adopted second incremental carries NO messages — the prior owner must survive.
      secondIncremental <- signedIncremental(2L, firstHash, None, mgKeyPair)
      secondBinary <- binaryOf(secondIncremental, firstHash, mgKeyPair)
      adopted = SortedMap(mgAddr -> NonEmptyList.of(secondBinary))(Address.OrderingInstance)

      adoptResult <- processor.processCurrencySnapshots(
        ordinal,
        SortedMap.empty[Address, Balance],
        prior,
        adopted,
        _ => None.pure[IO],
        CurrencyAdoptionMode.AdoptFromSignedFields
      )
      adoptCommitted = committed(adoptResult, mgAddr)
    } yield
      expect.all(
        adoptCommitted.map(_._1) == Some(SnapshotOrdinal(NonNegLong(2L))),
        // Prior owner carried forward despite the new snapshot's empty messages.
        adoptCommitted.flatMap(_._2).flatMap(_.get(MessageType.Owner)).map(_.value.address) == Some(mgAddr)
      )
  }

  test("adopt path: balances + lastTxRefs are MAINTAINED (non-empty, correct) after applying a transfer, root matches stateProof") { res =>
    implicit val (h, sp, j) = res
    // `CurrencySnapshotInfo.stateProof` ignores the selector value (always builds the 9-field currency proof), so the ambient
    // `globalStateProofSelector` yields the byte-identical proof the production `CurrencyStateProofSelector` does.
    for {
      processor <- mkProcessor
      mgKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      mgAddr = PublicKeyOps(mgKeyPair.getPublic).toAddress
      mgIdentifier <- TransactionReference.emptyCurrency[IO](mgAddr).map(_.hash)

      sourceKp <- KeyPairGenerator.makeKeyPair[IO]
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      source = PublicKeyOps(sourceKp.getPublic).toAddress
      dest = PublicKeyOps(destKp.getPublic).toAddress

      // Prior `Right` at ordinal 1 with a FUNDED source — this is the genesis-rooted balance state gl0 holds (e.g. via the
      // `Left(fullSnapshot)` adopt at GSCEP:377). The adopt derivation must apply the transfer forward onto this.
      priorBalances = SortedMap(source -> Balance(NonNegLong(100L)))(Address.OrderingInstance)
      priorInfo = info(None).copy(balances = priorBalances)
      firstIncremental <- signedIncremental(1L, Hash.empty, None, mgKeyPair)
      firstHash <- firstIncremental.toHashed.map(_.hash)
      prior: SortedMap[Address, CurrencySnapshotWithState] =
        SortedMap(mgAddr -> (Right((firstIncremental, priorInfo)): CurrencySnapshotWithState))(Address.OrderingInstance)

      // The adopted second incremental carries one accepted transfer: source → dest, amount 30, fee 0.
      transfer <- signedTransfer(source, dest, 30L, mgIdentifier, sourceKp)
      block = blockOf(transfer)
      transferRef <- TransactionReference.of[IO](transfer)

      // Compute the EXPECTED post-state Info the metagraph's ml0 would commit, and stamp its stateProof into the snapshot
      // (exactly what `CurrencySnapshotAcceptanceManager` does: `csi.stateProof(ordinal)`). source 100−30=70, dest 0+30=30;
      // source ref → ordinal 1 (the transfer), dest ref → emptyCurrency seed.
      expectedBalances = SortedMap(source -> Balance(NonNegLong(70L)), dest -> Balance(NonNegLong(30L)))(Address.OrderingInstance)
      expectedTxRefs = SortedMap(
        source -> transferRef,
        dest -> TransactionReference(TransactionOrdinal(NonNegLong(0L)), mgIdentifier)
      )(Address.OrderingInstance)
      expectedInfo = priorInfo.copy(balances = expectedBalances, lastTxRefs = expectedTxRefs)
      committedProof <- expectedInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))

      secondIncremental <- signedIncremental(
        2L,
        firstHash,
        None,
        mgKeyPair,
        blocks = SortedSet(block),
        stateProof = committedProof
      )
      secondBinary <- binaryOf(secondIncremental, firstHash, mgKeyPair)
      adopted = SortedMap(mgAddr -> NonEmptyList.of(secondBinary))(Address.OrderingInstance)

      adoptResult <- processor.processCurrencySnapshots(
        ordinal,
        SortedMap.empty[Address, Balance],
        prior,
        adopted,
        _ => None.pure[IO],
        CurrencyAdoptionMode.AdoptFromSignedFields
      )
      result = committedInfo(adoptResult, mgAddr)
    } yield
      expect.all(
        // Commitment advanced to ordinal 2 (freeze stays dead).
        result.map(_._1) == Some(SnapshotOrdinal(NonNegLong(2L))),
        // Balances are MAINTAINED and CORRECT — not empty. This is the cl1-bootstrap fix.
        result.map(_._2.balances) == Some(expectedBalances),
        // lastTxRefs maintained: source advanced to the transfer's ref, fresh dest seeded with emptyCurrency.
        result.map(_._2.lastTxRefs) == Some(expectedTxRefs),
        // The whole derived Info equals what the metagraph committed (root matched → derived adopted verbatim).
        result.map(_._2) == Some(expectedInfo)
      )
  }

  /** DATA-WITH-FEE correctness + determinism (the committee-state-diff / field-25 `MgBalances` fix).
    *
    * The metagraph's committed `balances` reflect a data-application FEE deducted by ml0 (`BalanceOpsManager.acceptFeeTxs`) — an effect gl0
    * previously did NOT replay, so its `derivedBalancesProof =!= committedBalancesProof`, the per-field gate
    * (`GlobalSnapshotStateChannelEventsProcessor.deriveAdoptedCurrencyInfo`) carried the node's OWN prior `balances` forward, and the fee
    * never mirrored (data-with-fee shard imbalance). The fee tx IS on the wire (`CurrencyIncrementalSnapshot.feeTransactions`), so gl0 can
    * now replay it deterministically and ADOPT the true fee-deducted balances.
    *
    * This asserts BOTH halves of the goal:
    *   - (b) CORRECTNESS — gl0 commits the metagraph's TRUE fee-deducted `balances` (== the committed `balancesProof`), NOT the stale
    *     prior.
    *   - (a) CONSISTENCY / no path-dependence — the derivation is a pure function of (signed binary, prior). We run it twice with the SAME
    *     finalized base prior but DIFFERENT node-local histories injected as the `currentBalances` accumulator (the only per-node-varying
    *     input to `processCurrencySnapshots`); both nodes commit the byte-identical fee-deducted `balances`. (The pre-fix behavior would
    *     instead commit each node's own carried-forward prior — here the prior is shared, but the regression guard below proves the gate
    *     ADOPTS the derived value rather than the prior, i.e. the carry-forward path is no longer taken for the fee case.)
    */
  test("adopt path (data-with-fee): a metagraph FeeTransaction is replayed; gl0 ADOPTS the fee-deducted balances deterministically") {
    res =>
      implicit val (h, sp, j) = res
      for {
        processor <- mkProcessor
        mgKeyPair <- KeyPairGenerator.makeKeyPair[IO]
        mgAddr = PublicKeyOps(mgKeyPair.getPublic).toAddress

        sourceKp <- KeyPairGenerator.makeKeyPair[IO]
        feeDestKp <- KeyPairGenerator.makeKeyPair[IO]
        source = PublicKeyOps(sourceKp.getPublic).toAddress
        feeDest = PublicKeyOps(feeDestKp.getPublic).toAddress

        // Prior `Right` at ordinal 1 with a FUNDED source — gl0's pre-fee mirror of the metagraph balances.
        priorBalances = SortedMap(source -> Balance(NonNegLong(100L)))(Address.OrderingInstance)
        priorInfo = info(None).copy(balances = priorBalances)
        firstIncremental <- signedIncremental(1L, Hash.empty, None, mgKeyPair)
        firstHash <- firstIncremental.toHashed.map(_.hash)
        prior: SortedMap[Address, CurrencySnapshotWithState] =
          SortedMap(mgAddr -> (Right((firstIncremental, priorInfo)): CurrencySnapshotWithState))(Address.OrderingInstance)

        // The adopted second incremental carries ONLY a data-application fee transaction (NO blocks/rewards) — the exact case the
        // block/reward-only replay could not reproduce. ml0 deducted fee 40: source 100−40=60, feeDest 0+40=40.
        feeTx <- signedFeeTransaction(source, feeDest, 40L, Hash("da7a" * 16), sourceKp)
        expectedBalances = SortedMap(source -> Balance(NonNegLong(60L)), feeDest -> Balance(NonNegLong(40L)))(Address.OrderingInstance)
        expectedInfo = priorInfo.copy(balances = expectedBalances)
        committedProof <- expectedInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))

        secondIncremental <- signedIncremental(
          2L,
          firstHash,
          None,
          mgKeyPair,
          stateProof = committedProof,
          feeTransactions = SortedSet(feeTx).some
        )
        secondBinary <- binaryOf(secondIncremental, firstHash, mgKeyPair)
        adopted = SortedMap(mgAddr -> NonEmptyList.of(secondBinary))(Address.OrderingInstance)

        // "Producer-role" node: derive with one node-local accumulator state.
        producerResult <- processor.processCurrencySnapshots(
          ordinal,
          SortedMap(source -> Balance(NonNegLong(999L)))(Address.OrderingInstance), // node-local accumulator A (irrelevant to per-MG info)
          prior,
          adopted,
          _ => None.pure[IO],
          CurrencyAdoptionMode.AdoptFromSignedFields
        )
        // "Follower-role" node: SAME finalized base prior + SAME signed binary, DIFFERENT node-local accumulator state.
        followerResult <- processor.processCurrencySnapshots(
          ordinal,
          SortedMap.empty[Address, Balance], // node-local accumulator B
          prior,
          adopted,
          _ => None.pure[IO],
          CurrencyAdoptionMode.AdoptFromSignedFields
        )
        producerCommitted = committedInfo(producerResult, mgAddr)
        followerCommitted = committedInfo(followerResult, mgAddr)
      } yield
        expect.all(
          // Commitment advanced to ordinal 2 (freeze stays dead).
          producerCommitted.map(_._1) == Some(SnapshotOrdinal(NonNegLong(2L))),
          // (b) CORRECTNESS: gl0 ADOPTED the metagraph's TRUE fee-deducted balances — the fee mirrored.
          producerCommitted.map(_._2.balances) == Some(expectedBalances),
          // The whole derived Info equals what the metagraph committed (root matched → adopted verbatim, not carried-forward).
          producerCommitted.map(_._2) == Some(expectedInfo),
          // REGRESSION GUARD: the committed balances are NOT the stale prior — the carry-forward path was NOT taken for the fee case.
          producerCommitted.map(_._2.balances) != Some(priorBalances),
          // (a) CONSISTENCY / no path-dependence: producer-role and follower-role nodes commit BYTE-IDENTICAL per-MG balances
          // despite different node-local accumulator histories.
          producerCommitted.map(_._2.balances) == followerCommitted.map(_._2.balances),
          producerCommitted.map(_._2) == followerCommitted.map(_._2)
        )
  }

  /** TOKEN-LOCK correctness + determinism (extends the data-with-fee fix to the token-locks workflow — the freeze this commit unblocks).
    *
    * The metagraph's committed `balances` reflect a TOKEN-LOCK debit (`−(TokenLockAmount + TokenLockFee)`) applied by ml0
    * (`TokenLockOpsManager.updateBalancesByTokenLocks`) — an effect gl0 previously did NOT replay, so its `derivedBalancesProof =!=
    * committedBalancesProof`, the per-field gate carried the node's OWN prior `balances` forward, AND (worse) the token-lock-shaped fields
    * never advanced → shard adoption FROZE at the token-locks workflow. The token-lock IS on the wire
    * (`CurrencyIncrementalSnapshot.tokenLockBlocks`) and the expiry epoch rides in `globalSyncView`, so gl0 can now replay it
    * deterministically and ADOPT the true token-locked balances + the active-lock set.
    *
    * Setup that makes "block/reward/fee replay cannot reproduce" CONCRETE: the adopted incremental carries NO blocks/rewards/fee-txs — ONLY
    * a token-lock. The pre-token-lock derivation would leave `balances` at the prior (100), the active-lock set absent, and BOTH proofs
    * would diverge from the committed (65 + the active lock) → carry-forward. With the fold, gl0 derives 65 + the active lock and adopts.
    */
  test("adopt path (token-lock): a metagraph token-lock is replayed; gl0 ADOPTS the locked balances + active locks deterministically") {
    res =>
      implicit val (h, sp, j) = res
      for {
        processor <- mkProcessor
        mgKeyPair <- KeyPairGenerator.makeKeyPair[IO]
        mgAddr = PublicKeyOps(mgKeyPair.getPublic).toAddress

        sourceKp <- KeyPairGenerator.makeKeyPair[IO]
        source = PublicKeyOps(sourceKp.getPublic).toAddress

        // Prior `Right` at ordinal 1 with a FUNDED source — gl0's pre-token-lock mirror of the metagraph balances.
        priorBalances = SortedMap(source -> Balance(NonNegLong(100L)))(Address.OrderingInstance)
        priorInfo = info(None).copy(balances = priorBalances)
        firstIncremental <- signedIncremental(1L, Hash.empty, None, mgKeyPair)
        firstHash <- firstIncremental.toHashed.map(_.hash)
        prior: SortedMap[Address, CurrencySnapshotWithState] =
          SortedMap(mgAddr -> (Right((firstIncremental, priorInfo)): CurrencySnapshotWithState))(Address.OrderingInstance)

        // The adopted second incremental carries ONLY a token-lock (amount 30, fee 5), NOT yet expired (unlockEpoch 600 > syncEpoch 100) —
        // the exact case block/reward/fee replay cannot reproduce. ml0 debited source 100 − 30 − 5 = 65 and tracked the lock as active.
        syncEpoch = EpochProgress(NonNegLong(100L))
        unlockEpoch = EpochProgress(NonNegLong(600L))
        gsv = io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSyncView(
          SnapshotOrdinal(NonNegLong(1L)),
          Hash.empty,
          syncEpoch
        )
        lock <- signedTokenLock(source, 30L, 5L, Some(unlockEpoch), sourceKp)
        lockBlock = tokenLockBlockOf(lock)
        lockRef <- io.constellationnetwork.schema.tokenLock.TokenLockReference.of[IO](lock)

        // The exact post-state Info ml0 commits: balances debited, the lock now active, its ref recorded.
        expectedBalances = SortedMap(source -> Balance(NonNegLong(65L)))(Address.OrderingInstance)
        expectedActiveLocks = SortedMap(source -> SortedSet(lock))(Address.OrderingInstance)
        expectedLockRefs = SortedMap(source -> lockRef)(Address.OrderingInstance)
        expectedInfo = priorInfo.copy(
          balances = expectedBalances,
          activeTokenLocks = Some(expectedActiveLocks),
          lastTokenLockRefs = Some(expectedLockRefs)
        )
        committedProof <- expectedInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))

        secondIncremental <- signedIncremental(
          2L,
          firstHash,
          None,
          mgKeyPair,
          stateProof = committedProof,
          tokenLockBlocks = SortedSet(lockBlock).some,
          globalSyncView = gsv.some
        )
        secondBinary <- binaryOf(secondIncremental, firstHash, mgKeyPair)
        adopted = SortedMap(mgAddr -> NonEmptyList.of(secondBinary))(Address.OrderingInstance)

        // Run twice with DIFFERENT node-local accumulator histories — the per-MG derived state must be byte-identical (split-safe).
        producerResult <- processor.processCurrencySnapshots(
          ordinal,
          SortedMap(source -> Balance(NonNegLong(999L)))(Address.OrderingInstance),
          prior,
          adopted,
          _ => None.pure[IO],
          CurrencyAdoptionMode.AdoptFromSignedFields
        )
        followerResult <- processor.processCurrencySnapshots(
          ordinal,
          SortedMap.empty[Address, Balance],
          prior,
          adopted,
          _ => None.pure[IO],
          CurrencyAdoptionMode.AdoptFromSignedFields
        )
        producerCommitted = committedInfo(producerResult, mgAddr)
        followerCommitted = committedInfo(followerResult, mgAddr)
      } yield
        expect.all(
          // Commitment advanced to ordinal 2 (the token-locks-workflow freeze is broken).
          producerCommitted.map(_._1) == Some(SnapshotOrdinal(NonNegLong(2L))),
          // CORRECTNESS: gl0 ADOPTED the metagraph's TRUE token-locked balances (100 − 30 − 5 = 65) — the lock debit mirrored.
          producerCommitted.map(_._2.balances) == Some(expectedBalances),
          // The active-lock set + refs are MAINTAINED (the lock is now tracked).
          producerCommitted.flatMap(_._2.activeTokenLocks) == Some(expectedActiveLocks),
          producerCommitted.flatMap(_._2.lastTokenLockRefs) == Some(expectedLockRefs),
          // The whole derived Info equals what the metagraph committed (root matched → adopted verbatim, not carried-forward).
          producerCommitted.map(_._2) == Some(expectedInfo),
          // REGRESSION GUARD: the committed balances are NOT the stale prior — the carry-forward path was NOT taken for the lock case.
          // (Removing the token-lock balance fold reverts `balances` to the prior 100 here, failing this expectation.)
          producerCommitted.map(_._2.balances) != Some(priorBalances),
          // DETERMINISM: producer-role and follower-role nodes commit BYTE-IDENTICAL per-MG state despite different accumulators.
          producerCommitted.map(_._2) == followerCommitted.map(_._2)
        )
  }

  /** TOKEN-LOCK EXPIRY-REFUND correctness — the other half of `updateBalancesByTokenLocks`. A prior active lock whose `unlockEpoch` is now
    * PAST `syncEpoch` is refunded (`+TokenLockAmount`) and dropped from the active set, exactly as ml0 does. Block/reward/fee replay cannot
    * reproduce this (no on-wire event at all describes the refund — it is a pure function of the prior active set + the GIVEN syncEpoch).
    */
  test("adopt path (token-lock): an expired prior lock is REFUNDED + dropped; gl0 ADOPTS the refunded balances deterministically") { res =>
    implicit val (h, sp, j) = res
    for {
      processor <- mkProcessor
      mgKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      mgAddr = PublicKeyOps(mgKeyPair.getPublic).toAddress

      sourceKp <- KeyPairGenerator.makeKeyPair[IO]
      source = PublicKeyOps(sourceKp.getPublic).toAddress

      // A prior active lock (amount 30) that expires at epoch 50; the incoming snapshot synced to epoch 100 ⇒ it is now expired.
      priorLockUnlock = EpochProgress(NonNegLong(50L))
      priorLock <- signedTokenLock(source, 30L, 0L, Some(priorLockUnlock), sourceKp)
      priorActiveLocks = SortedMap(source -> SortedSet(priorLock))(Address.OrderingInstance)

      // Prior balances already reflect the earlier debit (the source is "down" 30 from the lock). On expiry ml0 refunds +30.
      priorBalances = SortedMap(source -> Balance(NonNegLong(70L)))(Address.OrderingInstance)
      priorInfo = info(None).copy(balances = priorBalances, activeTokenLocks = Some(priorActiveLocks))
      firstIncremental <- signedIncremental(1L, Hash.empty, None, mgKeyPair)
      firstHash <- firstIncremental.toHashed.map(_.hash)
      prior: SortedMap[Address, CurrencySnapshotWithState] =
        SortedMap(mgAddr -> (Right((firstIncremental, priorInfo)): CurrencySnapshotWithState))(Address.OrderingInstance)

      // The adopted second incremental carries NO new locks — only the synced epoch (100), which expires the prior lock.
      syncEpoch = EpochProgress(NonNegLong(100L))
      gsv = io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSyncView(
        SnapshotOrdinal(NonNegLong(1L)),
        Hash.empty,
        syncEpoch
      )

      // ml0 refunds source +30 (back to 100) and drops the expired lock from the active set (→ empty, shape Some(empty)).
      expectedBalances = SortedMap(source -> Balance(NonNegLong(100L)))(Address.OrderingInstance)
      expectedActiveLocks = SortedMap.empty[Address, SortedSet[Signed[io.constellationnetwork.schema.tokenLock.TokenLock]]]
      expectedInfo = priorInfo.copy(balances = expectedBalances, activeTokenLocks = Some(expectedActiveLocks))
      committedProof <- expectedInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))

      secondIncremental <- signedIncremental(
        2L,
        firstHash,
        None,
        mgKeyPair,
        stateProof = committedProof,
        globalSyncView = gsv.some
      )
      secondBinary <- binaryOf(secondIncremental, firstHash, mgKeyPair)
      adopted = SortedMap(mgAddr -> NonEmptyList.of(secondBinary))(Address.OrderingInstance)

      adoptResult <- processor.processCurrencySnapshots(
        ordinal,
        SortedMap.empty[Address, Balance],
        prior,
        adopted,
        _ => None.pure[IO],
        CurrencyAdoptionMode.AdoptFromSignedFields
      )
      result = committedInfo(adoptResult, mgAddr)
    } yield
      expect.all(
        result.map(_._1) == Some(SnapshotOrdinal(NonNegLong(2L))),
        // CORRECTNESS: the expired lock was refunded (70 + 30 = 100) — block/reward/fee replay alone would leave balances at 70.
        result.map(_._2.balances) == Some(expectedBalances),
        // The expired lock was dropped from the active set.
        result.flatMap(_._2.activeTokenLocks) == Some(expectedActiveLocks),
        result.map(_._2) == Some(expectedInfo),
        // REGRESSION GUARD: NOT the stale prior balances (removing the expiry-refund fold reverts to 70 here).
        result.map(_._2.balances) != Some(priorBalances)
      )
  }

  test("adopt path: derived-root verification — matches on valid stateProof, FALLS BACK to prior balances on tampered stateProof") { res =>
    implicit val (h, sp, j) = res
    for {
      processor <- mkProcessor
      mgKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      mgAddr = PublicKeyOps(mgKeyPair.getPublic).toAddress
      mgIdentifier <- TransactionReference.emptyCurrency[IO](mgAddr).map(_.hash)

      sourceKp <- KeyPairGenerator.makeKeyPair[IO]
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      source = PublicKeyOps(sourceKp.getPublic).toAddress
      dest = PublicKeyOps(destKp.getPublic).toAddress

      priorBalances = SortedMap(source -> Balance(NonNegLong(100L)))(Address.OrderingInstance)
      priorInfo = info(None).copy(balances = priorBalances)
      firstIncremental <- signedIncremental(1L, Hash.empty, None, mgKeyPair)
      firstHash <- firstIncremental.toHashed.map(_.hash)
      prior: SortedMap[Address, CurrencySnapshotWithState] =
        SortedMap(mgAddr -> (Right((firstIncremental, priorInfo)): CurrencySnapshotWithState))(Address.OrderingInstance)

      transfer <- signedTransfer(source, dest, 30L, mgIdentifier, sourceKp)
      block = blockOf(transfer)
      transferRef <- TransactionReference.of[IO](transfer)
      expectedBalances = SortedMap(source -> Balance(NonNegLong(70L)), dest -> Balance(NonNegLong(30L)))(Address.OrderingInstance)
      expectedTxRefs = SortedMap(
        source -> transferRef,
        dest -> TransactionReference(TransactionOrdinal(NonNegLong(0L)), mgIdentifier)
      )(Address.OrderingInstance)
      expectedInfo = priorInfo.copy(balances = expectedBalances, lastTxRefs = expectedTxRefs)
      validProof <- expectedInfo.stateProof[IO](SnapshotOrdinal(NonNegLong(2L)))

      // VALID: stateProof matches the derived root → adopt the derived (correct) balances.
      validInc <- signedIncremental(2L, firstHash, None, mgKeyPair, blocks = SortedSet(block), stateProof = validProof)
      validBinary <- binaryOf(validInc, firstHash, mgKeyPair)
      validResult <- processor.processCurrencySnapshots(
        ordinal,
        SortedMap.empty[Address, Balance],
        prior,
        SortedMap(mgAddr -> NonEmptyList.of(validBinary))(Address.OrderingInstance),
        _ => None.pure[IO],
        CurrencyAdoptionMode.AdoptFromSignedFields
      )
      validCommitted = committedInfo(validResult, mgAddr)

      // TAMPERED: same accepted transfer, but the committed stateProof claims a different (lying) state root. The derived
      // root will NOT match → gl0 falls back to the PRIOR balances (carry forward), still advancing the ordinal.
      tamperedProof = validProof.copy(balancesProof = Hash("deadbeef" * 8))
      tamperedInc <- signedIncremental(2L, firstHash, None, mgKeyPair, blocks = SortedSet(block), stateProof = tamperedProof)
      tamperedBinary <- binaryOf(tamperedInc, firstHash, mgKeyPair)
      tamperedResult <- processor.processCurrencySnapshots(
        ordinal,
        SortedMap.empty[Address, Balance],
        prior,
        SortedMap(mgAddr -> NonEmptyList.of(tamperedBinary))(Address.OrderingInstance),
        _ => None.pure[IO],
        CurrencyAdoptionMode.AdoptFromSignedFields
      )
      tamperedCommitted = committedInfo(tamperedResult, mgAddr)
    } yield
      expect.all(
        // Valid: derived balances adopted.
        validCommitted.map(_._1) == Some(SnapshotOrdinal(NonNegLong(2L))),
        validCommitted.map(_._2.balances) == Some(expectedBalances),
        // Tampered: ordinal STILL advances (freeze stays dead) ...
        tamperedCommitted.map(_._1) == Some(SnapshotOrdinal(NonNegLong(2L))),
        // ... but gl0 does NOT commit the unverified derived balances — it carries forward the prior (last verified) balances.
        tamperedCommitted.map(_._2.balances) == Some(priorBalances),
        tamperedCommitted.map(_._2.balances) != Some(expectedBalances)
      )
  }
}
