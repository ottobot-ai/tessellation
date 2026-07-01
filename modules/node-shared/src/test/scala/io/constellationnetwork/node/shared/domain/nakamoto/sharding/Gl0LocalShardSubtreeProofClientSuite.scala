package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ConsumedAllowSpendStateManager
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SpendAction, SpendTransaction}
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedAllowSpendSetCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegInt, NonNegLong, PosLong}
import weaver.MutableIOSuite

/** Coverage for the gl0-LOCAL cross-shard read client ([[ShardSubtreeProofClient.gl0Local]]) — the DETERMINISTIC cross-shard read source
  * the gl0 acceptance path (`GlobalSnapshotAcceptanceManager.accept`, W3c activation) supplies in place of a peer-fetch.
  *
  * The client reads the cross-shard `AllowSpend`/`Balance` value DIRECTLY off gl0's own consensus-pinned finalized state (a real MPT-backed
  * `GlobalStateReader.finalized`), never a peer. These tests prove the four properties from the slice task:
  *
  *   1. '''Cross-shard CONSUME accepted''' — an allow-spend present in gl0's finalized mirror, referenced by a spend in a DIFFERENT-shard
  *      MG, is ACCEPTED through `SpendActionValidator` via the gl0-local client (the unsharded validator / `noop` client previously
  *      rejected it as `CrossShardProofUnavailable`).
  *   1. '''Cross-shard phantom-refund balance-spend still REJECTED''' — the W3c effective-balance overlay fires on the gl0-LOCAL-read
  *      attested balance, so a no-`allowSpendRef` self-spend of a phantom expiry-refund is rejected; the same spend is accepted under the
  *      raw attested balance, proving the overlay is load-bearing over the local read.
  *   1. '''Determinism''' — two independently-constructed gl0-local clients over the SAME finalized state return the byte-identical value
  *      for the same key (the cluster-uniformity argument: every gl0 node reads the same finalized state ⇒ same bytes ⇒ same mptRoot).
  *   1. '''numShards = 1 byte-identity''' — at `numShards = 1` the cross-shard path is unreachable; the gl0-local client is never
  *      consulted, and the spend is validated off the in-process attested balance exactly as today.
  */
object Gl0LocalShardSubtreeProofClientSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  def sharedResource: Resource[IO, Res] =
    for {
      implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
    } yield (j, h, sp)

  /** A real MPT-backed finalized reader (the production determinism anchor shape — `GlobalStateReader.finalized` over an `MptStore`). */
  private def mkFinalizedReader(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[(MptStore[IO, GlobalStateKey], GlobalStateReader[IO])] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
    } yield (store, GlobalStateReader.finalized[IO](store))

  private def swapAmt(v: Long): SwapAmount = SwapAmount(PosLong.unsafeFrom(v))

  /** Scan `numShards` in [2, 64] for a value that places `addrA` and `addrB` in DIFFERENT shards (same trick the existing cross-shard
    * suites use) — keeps the test independent of which specific shard each addr maps to.
    */
  private def findNumShardsSplitting(addrA: Address, addrB: Address)(implicit hasher: Hasher[IO]): IO[Int] = {
    def tryN(n: Int): IO[Boolean] = {
      val a = ShardAssignment.make[IO](n)
      (a.shardIdFor(addrA), a.shardIdFor(addrB)).mapN(_ =!= _)
    }
    (2 to 64).toList
      .findM(tryN)
      .map(_.getOrElse(throw new AssertionError(s"Could not split $addrA and $addrB across any numShards in [2,64]")))
  }

  // ===========================================================================
  // Test 1: cross-shard CONSUME — allow-spend present in gl0's finalized mirror ⇒ ACCEPTED via gl0-local client
  // ===========================================================================

  test("cross-shard CONSUME: allow-spend present in gl0's finalized mirror is ACCEPTED through the validator via the gl0-local client") {
    res =>
      implicit val (js, hs, sp) = res

      for {
        keyPairSource <- KeyPairGenerator.makeKeyPair[IO]
        keyPairCurrentMg <- KeyPairGenerator.makeKeyPair[IO]
        keyPairTargetMg <- KeyPairGenerator.makeKeyPair[IO]
        source = keyPairSource.getPublic.toAddress
        currentMgAddr = keyPairCurrentMg.getPublic.toAddress
        targetMgAddr = keyPairTargetMg.getPublic.toAddress

        numShards <- findNumShardsSplitting(currentMgAddr, targetMgAddr)
        shardAssignment = ShardAssignment.make[IO](numShards)

        // The allow-spend lives in targetMg's scope; currencyId references targetMg so the SpendTransaction's
        // currencyId check passes. destination = approver = currentMg (the validator's current MG / emitter).
        targetCurrencyId = CurrencyId(targetMgAddr)
        allowSpend = AllowSpend(
          source,
          currentMgAddr,
          targetCurrencyId.some,
          SwapAmount(1L),
          AllowSpendFee(1L),
          AllowSpendReference.empty,
          EpochProgress(20L),
          List(currentMgAddr)
        )
        signedAllowSpend <- Signed.forAsyncHasher(allowSpend, keyPairSource)
        hashedAllowSpend <- signedAllowSpend.toHashed

        // Seed gl0's finalized mirror EXACTLY where the converter writes it: the fieldId-7 ActiveAllowSpends
        // partition under (Some(targetMg), source). This is the same key the validator's cross-shard path builds.
        storeAndReader <- mkFinalizedReader
        (store, reader) = storeAndReader
        allowSpendKey = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, targetMgAddr.some, source)
        _ <- store.insert[SortedSet[Signed[AllowSpend]]](allowSpendKey, SortedSet(signedAllowSpend))

        // The gl0-LOCAL client reads off the finalized reader — no peer.
        proofClient = ShardSubtreeProofClient.gl0Local[IO](reader)
        validator = SpendActionValidator.make[IO](proofClient, shardAssignment)

        // currentMg's in-process maps do NOT carry targetMg's slice — only the gl0-local read supplies the AllowSpend.
        userSpendTx =
          SpendTransaction(hashedAllowSpend.hash.some, targetCurrencyId.some, SwapAmount(1L), source, currentMgAddr)
        spendAction = SpendAction(NonEmptyList.of(userSpendTx))
        activeAllowSpends = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
        balances = Map.empty[Option[Address], SortedMap[Address, Balance]]

        result <- validator.validate(spendAction, activeAllowSpends, balances, currentMgAddr)
      } yield expect(result.isValid)
  }

  // ===========================================================================
  // Test 2: cross-shard phantom-refund balance-spend — REJECTED under W3c overlay over the gl0-LOCAL-read balance
  // ===========================================================================

  test(
    "cross-shard phantom-refund balance-spend: REJECTED under the W3c overlay over the gl0-local-read balance, ACCEPTED under raw attested"
  ) { res =>
    implicit val (js, hs, sp) = res

    for {
      mKp <- KeyPairGenerator.makeKeyPair[IO]
      mPrimeKp <- KeyPairGenerator.makeKeyPair[IO]
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      m = mKp.getPublic.toAddress // owner metagraph M (= targetMg of the cross-shard fetch)
      mPrime = mPrimeKp.getPublic.toAddress // spender M′ (= validator's currentMg, also the marker source)
      dest = destKp.getPublic.toAddress

      numShards <- findNumShardsSplitting(m, mPrime)
      shardAssignment = ShardAssignment.make[IO](numShards)

      // M's ATTESTED per-MG balance for M′ AFTER the refund = exactly X (phantom). Seed it into gl0's finalized
      // mirror under the Balances partition (Some(M), M′) — the exact key validateBalanceCrossShard builds.
      x = 100L
      provenAttestedBalance = Balance(NonNegLong.unsafeFrom(x))
      storeAndReader <- mkFinalizedReader
      (store, reader) = storeAndReader
      balanceKey = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, m.some, mPrime)
      _ <- store.insert[Balance](balanceKey, provenAttestedBalance)

      // The gl0-finalized ConsumedAllowSpends spent-set: M′'s earlier cross-shard consume of an allow-spend on M
      // (X=100, expiry E=200), now EXPIRED (live epoch 250 > 200) so M refunded M′ a phantom +X. Marker scope = M.
      expiry = 200L
      asHash = Hash("ab".padTo(64, '0').take(64))
      spentSet: SortedMap[Hash, ConsumedAllowSpend] = SortedMap(
        asHash -> ConsumedAllowSpend(
          allowSpendHash = asHash,
          source = mPrime,
          destination = dest,
          currencyId = CurrencyId(m).some,
          amount = swapAmt(x),
          lastValidEpochProgress = EpochProgress(NonNegLong.unsafeFrom(expiry)),
          consumedAtOrdinal = SnapshotOrdinal(NonNegLong(1L)),
          consumingSpendRef = asHash
        )
      )

      // The overlay REUSES ConsumedAllowSpendStateManager.effectiveCurrencyBalances verbatim, closing over the
      // finalized spent-set + the post-expiry epoch ⇒ M′'s effective balance = X − X = 0.
      mgr = ConsumedAllowSpendStateManager.make[IO](GlobalStateReader.empty[IO])
      effectiveOverlay: SpendActionValidator.CrossShardEffectiveBalanceOverlay =
        (attested, scope) =>
          mgr.effectiveCurrencyBalances(
            attested,
            scope,
            spentSet,
            Map.empty[Address, EpochProgress],
            EpochProgress(NonNegLong.unsafeFrom(250L))
          )

      // Both validators read the SAME gl0-local client (same finalized mirror); they differ only in the overlay.
      proofClient = ShardSubtreeProofClient.gl0Local[IO](reader)
      effectiveValidator = SpendActionValidator.make[IO](proofClient, shardAssignment, effectiveOverlay)
      rawValidator = SpendActionValidator.make[IO](proofClient, shardAssignment)

      selfSpend = SpendTransaction(none[Hash], CurrencyId(m).some, swapAmt(x), mPrime, dest)
      spendAction = SpendAction(NonEmptyList.of(selfSpend))
      activeAllowSpends = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      balances = Map.empty[Option[Address], SortedMap[Address, Balance]]

      resEffective <- effectiveValidator.validate(spendAction, activeAllowSpends, balances, mPrime)
      resRaw <- rawValidator.validate(spendAction, activeAllowSpends, balances, mPrime)
    } yield
      expect.all(
        // Under the EFFECTIVE overlay (M′ effective balance = 0) ⇒ REJECTED for insufficient balance.
        resEffective.isInvalid,
        resEffective.toEither.left.exists(_.exists {
          case NotEnoughCurrencyIdBalance(_) => true
          case _                             => false
        }),
        // …but ACCEPTED under the raw attested (phantom-refunded) balance ⇒ the overlay is load-bearing over the gl0-local read.
        resRaw.isValid
      )
  }

  // ===========================================================================
  // Test 3: DETERMINISM — two gl0-local clients over the same finalized state return byte-identical values
  // ===========================================================================

  test("determinism: two gl0-local clients over the SAME finalized state return the byte-identical value for the same key") { res =>
    implicit val (js, hs, sp) = res

    for {
      keyPairSource <- KeyPairGenerator.makeKeyPair[IO]
      keyPairTargetMg <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPairSource.getPublic.toAddress
      targetMgAddr = keyPairTargetMg.getPublic.toAddress
      targetShardId = ShardId(NonNegInt.unsafeFrom(0))

      allowSpend = AllowSpend(
        source,
        targetMgAddr,
        CurrencyId(targetMgAddr).some,
        SwapAmount(1L),
        AllowSpendFee(1L),
        AllowSpendReference.empty,
        EpochProgress(20L),
        List(targetMgAddr)
      )
      signedAllowSpend <- Signed.forAsyncHasher(allowSpend, keyPairSource)

      // Two SEPARATE stores, each seeded with the byte-identical state — modelling two distinct gl0 nodes that
      // each hold the same finalized snapshot. Each gets its own gl0-local client.
      sr1 <- mkFinalizedReader
      sr2 <- mkFinalizedReader
      (store1, reader1) = sr1
      (store2, reader2) = sr2
      allowSpendKey = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, targetMgAddr.some, source)
      balanceKey = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, targetMgAddr.some, source)
      _ <- store1.insert[SortedSet[Signed[AllowSpend]]](allowSpendKey, SortedSet(signedAllowSpend))
      _ <- store2.insert[SortedSet[Signed[AllowSpend]]](allowSpendKey, SortedSet(signedAllowSpend))
      _ <- store1.insert[Balance](balanceKey, Balance(NonNegLong(777L)))
      _ <- store2.insert[Balance](balanceKey, Balance(NonNegLong(777L)))

      client1 = ShardSubtreeProofClient.gl0Local[IO](reader1)
      client2 = ShardSubtreeProofClient.gl0Local[IO](reader2)

      asRes1 <- client1.fetchAndVerify(targetShardId, targetMgAddr, allowSpendKey)
      asRes2 <- client2.fetchAndVerify(targetShardId, targetMgAddr, allowSpendKey)
      balRes1 <- client1.fetchAndVerify(targetShardId, targetMgAddr, balanceKey)
      balRes2 <- client2.fetchAndVerify(targetShardId, targetMgAddr, balanceKey)

      // Compare the value bytes (the only thing the validator consumes) byte-for-byte.
      asBytes1 = asRes1.flatMap(_._1).map(_.toList)
      asBytes2 = asRes2.flatMap(_._1).map(_.toList)
      balBytes1 = balRes1.flatMap(_._1).map(_.toList)
      balBytes2 = balRes2.flatMap(_._1).map(_.toList)
    } yield
      expect.all(
        asBytes1.isDefined,
        asBytes1 === asBytes2,
        balBytes1.isDefined,
        balBytes1 === balBytes2
      )
  }

  // ===========================================================================
  // Test 3b: gl0-local client return shapes — absent key ⇒ Some((None, _)); unsupported fieldId ⇒ None
  // ===========================================================================

  test("gl0-local client: absent key ⇒ proven-absence Some((None, _)); unsupported fieldId ⇒ unavailable None") { res =>
    implicit val (js, hs, sp) = res

    for {
      keyPairMg <- KeyPairGenerator.makeKeyPair[IO]
      mg = keyPairMg.getPublic.toAddress
      targetShardId = ShardId(NonNegInt.unsafeFrom(0))

      storeAndReader <- mkFinalizedReader
      (_, reader) = storeAndReader
      client = ShardSubtreeProofClient.gl0Local[IO](reader)

      // Supported partition, but the key is ABSENT in the (empty) mirror ⇒ proven absence: Some((None, _)).
      absentAllowSpendKey = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, mg.some, mg)
      absentBalanceKey = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, mg.some, mg)
      absentAllowSpend <- client.fetchAndVerify(targetShardId, mg, absentAllowSpendKey)
      absentBalance <- client.fetchAndVerify(targetShardId, mg, absentBalanceKey)

      // A fieldId the validator never asks for ⇒ fail-closed unavailable: None.
      unsupportedKey = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, mg.some, mg)
      unsupported <- client.fetchAndVerify(targetShardId, mg, unsupportedKey)
    } yield
      expect.all(
        absentAllowSpend.isDefined,
        absentAllowSpend.flatMap(_._1).isEmpty, // Some((None, _))
        absentBalance.isDefined,
        absentBalance.flatMap(_._1).isEmpty, // Some((None, _))
        unsupported.isEmpty // None
      )
  }

  // ===========================================================================
  // Test 4: numShards = 1 byte-identity — cross-shard path unreachable, gl0-local client never consulted
  // ===========================================================================

  test("numShards=1: cross-shard path unreachable ⇒ gl0-local client never consulted; spend validated off in-process attested balance") {
    res =>
      implicit val (js, hs, sp) = res

      for {
        mKp <- KeyPairGenerator.makeKeyPair[IO]
        destKp <- KeyPairGenerator.makeKeyPair[IO]
        m = mKp.getPublic.toAddress
        dest = destKp.getPublic.toAddress

        shardAssignment = ShardAssignment.make[IO](numShards = 1)

        x = 100L
        // Seed gl0's finalized mirror with a DEBITED balance (0) under (Some(m), m). If the gl0-local client were
        // (wrongly) consulted at numShards=1, the cross-shard read would return 0 ⇒ the spend would be REJECTED.
        // It must NOT be consulted: the same-shard path reads the in-process attested balance (X) ⇒ ACCEPTED.
        storeAndReader <- mkFinalizedReader
        (store, reader) = storeAndReader
        balanceKey = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, m.some, m)
        _ <- store.insert[Balance](balanceKey, Balance.empty)

        proofClient = ShardSubtreeProofClient.gl0Local[IO](reader)
        validator = SpendActionValidator.make[IO](proofClient, shardAssignment)

        // Source = m (the validator's currencyId), currencyId = Some(m) (same MG) ⇒ Same ⇒ in-process map (attested = X).
        selfSpend = SpendTransaction(none[Hash], CurrencyId(m).some, swapAmt(x), m, dest)
        spendAction = SpendAction(NonEmptyList.of(selfSpend))
        activeAllowSpends = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
        inProcessBalances = Map(m.some -> (SortedMap(m -> Balance(NonNegLong.unsafeFrom(x))): SortedMap[Address, Balance]))

        result <- validator.validate(spendAction, activeAllowSpends, inProcessBalances, m)
      } yield
        // Accepted off the in-process attested balance (X), NOT the debited gl0-mirror value (0) ⇒ the cross-shard
        // read source is dead at numShards=1 (the same-shard fast path covers every read).
        expect(result.isValid)
  }
}
