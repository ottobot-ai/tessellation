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
  * This is the regression guard for the consensus freeze: at `numShards>1`, gl0's `lastCurrencySnapshots` was stuck at genesis
  * for all metagraphs because the adopt path re-derived each currency snapshot via
  * `CurrencySnapshotContextFunctions.createContext` (recreate proposal artifact + byte-equality) against gl0's FROZEN prior. The
  * recreate produces ordinal 1, never matches the incoming ordinal N → `SnapshotDifferentThanExpected` → `CannotCreateContext` →
  * caught by `processCurrencySnapshots`' `handleErrorWith` → the binary/incremental is dropped and the commitment never advances.
  *
  * The fix (`CurrencyAdoptionMode.AdoptFromSignedFields`) advances the commitment DIRECTLY from the adopted, committee-attested
  * signed binary's OWN committed fields — ordinal + owner/staking `messages` (carried forward) — with NO `createContext`. gl0
  * commits the metagraph's state ROOT, not its full balance map (balances live at the metagraph's ml0).
  *
  * Setup that makes "createContext WOULD diverge" CONCRETE: the prior is a `Right` (a metagraph already past genesis in gl0's
  * commitment, at ordinal 1), and the adopted binary is the SECOND incremental (ordinal 2) — the exact case that reaches
  * `applyCurrencySnapshot`/`createContext`. The `CurrencySnapshotContextFunctions` stub RAISES on `createContext`, simulating the
  * `SnapshotDifferentThanExpected` divergence the real freeze hits. Then:
  *   - `Recreate` (the legacy default) calls the raising `createContext`; the error is caught and the commitment stays FROZEN at
  *     ordinal 1 — reproducing the freeze.
  *   - `AdoptFromSignedFields` never calls `createContext`; the commitment ADVANCES to ordinal 2 and the owner message is carried
  *     forward into `lastMessages` (the genuine cross-MG fee/config dependency `getFeeAddresses` reads).
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

  /** Build the processor with a `createContext` that ALWAYS RAISES — modeling the `SnapshotDifferentThanExpected` /
    * `CannotCreateContext` divergence the freeze hits when gl0 recreates an ordinal-N currency snapshot against its genesis prior.
    * Under `Recreate` this raise is reached (and caught → commitment frozen); under `AdoptFromSignedFields` it is never reached.
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

  /** An empty-state `CurrencySnapshotInfo` carrying only an optional `lastMessages` — the roots-only shape gl0 commits on the
    * adopt path. Used as the prior (`Right`) Info so the adopted binary is a SECOND incremental (reaches `createContext`).
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
    stateProof: CurrencySnapshotStateProof = CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None)
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
      None,
      None,
      None,
      None,
      None
    )
    forAsyncHasher[IO, CurrencyIncrementalSnapshot](snapshot, mgKeyPair)
  }

  /** A signed plain transfer `source → destination` of `amount` (fee 0), parented at `emptyCurrency(metagraphIdentifier)` so the source
    * ref advances to ordinal 1 — the on-disk shape a fresh metagraph wallet's first tx carries. Signed by the SOURCE key (irrelevant to
    * the adopt derivation, which replays already-accepted events without re-validating signatures).
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
    * parent BlockReference is arbitrary — the adopt derivation reads only the block's transactions. */
  private def blockOf(tx: Signed[Transaction]): BlockAsActiveTip =
    BlockAsActiveTip(
      Signed(
        Block(NonEmptyList.one(BlockReference(Height.MinValue, ProofsHash("0" * 64))), NonEmptySet.one(tx)),
        tx.proofs
      ),
      NonNegLong(0L)
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
    result: SortedMap[Address, (NonEmptyList[(Signed[StateChannelSnapshotBinary], Option[CurrencySnapshotWithState])], SortedMap[Address, Balance])],
    mgAddr: Address
  ): Option[(SnapshotOrdinal, Option[SortedMap[MessageType, Signed[CurrencyMessage]]])] =
    result
      .get(mgAddr)
      .flatMap { case (nel, _) => nel.toList.flatMap(_._2).lastOption }
      .collect { case Right((inc, ci)) => (inc.value.ordinal, ci.lastMessages) }

  /** Extract the FULL per-MG committed `CurrencySnapshotInfo` (balances/refs/...) from a `processCurrencySnapshots` result. */
  private def committedInfo(
    result: SortedMap[Address, (NonEmptyList[(Signed[StateChannelSnapshotBinary], Option[CurrencySnapshotWithState])], SortedMap[Address, Balance])],
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
      priorState = (Right((firstIncremental, info(None))): CurrencySnapshotWithState)
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
      priorState = (Right((firstIncremental, info(Some(priorMessages)))): CurrencySnapshotWithState)
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

  test("adopt path: balances + lastTxRefs are MAINTAINED (non-empty, correct) after applying a transfer, root matches stateProof") {
    res =>
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

  test("adopt path: derived-root verification — matches on valid stateProof, FALLS BACK to prior balances on tampered stateProof") {
    res =>
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
