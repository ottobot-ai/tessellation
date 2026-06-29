package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SpendTransaction, TokenUnlock}
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.{Signed, signature}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** Consensus-correctness gate for the ml0 adopt path: the incrementally-maintained MPT (`syncFromStateChanges`, the producer's signed root)
  * MUST be byte-identical to the from-active-records rebuild (`syncFromGlobalSnapshotInfo`, the ml0/bootstrap/recovery path) for the SAME
  * logical state — including state reached via EARLY removal of an indexed record (an allow-spend consumed by a spend-tx, or a token-lock
  * consumed by a token-unlock, BEFORE its future expiry/unlock epoch).
  *
  * If the incremental writer leaves an orphan index entry (an expiry-bucket entry at the record's future epoch, or an active-address-index
  * entry) that the rebuild — which derives every index partition purely from the active records — never has, the two roots diverge. ml0's
  * verify-before-adopt then fails, falls back to full re-execution, and the resulting StateProofMismatch cascades.
  *
  * Drives the REAL `AllowSpendStateManager` / `TokenLockStateManager` so the accumulator's expiry-index deltas + removal sets are exactly
  * what production's `accept()` produces.
  */
object IncrementalVsRebuildEarlyRemovalParitySuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

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

  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

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
        source = source,
        amount = TokenLockAmount(PosLong(200L)),
        fee = TokenLockFee(NonNegLong(0L)),
        parent = TokenLockReference(TokenLockOrdinal(NonNegLong(0L)), testHash(s"tl-parent-$label")),
        currencyId = None,
        unlockEpoch = unlockAt,
        replaceTokenLockRef = None
      ),
      testProofs
    )

  private def freshStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
    } yield store

  /** Seed a store at ordinal 1 from a full `GlobalSnapshotInfo` via the rebuild writer (the production bootstrap path). */
  private def seedFromInfo(store: MptStore[IO, GlobalStateKey], info: GlobalSnapshotInfo)(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[Unit] =
    store.syncFromGlobalSnapshotInfo(info, SnapshotOrdinal(NonNegLong(1L)))

  private def rootAndBytes(
    store: MptStore[IO, GlobalStateKey],
    ordinal: SnapshotOrdinal
  ): IO[(Option[Hash], Map[Hex, Vector[Byte]])] =
    for {
      trie <- store.build(ordinal)
      bytes <- store.allEntriesAsBytes
    } yield (trie.toOption.map(_.rootHash.value), bytes.view.mapValues(_.toVector).toMap)

  // ===========================================================================
  // Allow-spend EARLY removal (consumed by spend-tx before its future expiry)
  // ===========================================================================

  test("allow-spend consumed early by a spend-tx: incremental MPT root === from-GSI rebuild root (no orphan future-epoch bucket)") { res =>
    implicit val (h, sp, js) = res
    val ord2 = SnapshotOrdinal(NonNegLong(2L))
    val futureExpiry = EpochProgress(NonNegLong(500L)) // record's lastValidEpochProgress — its expiry bucket is at 500
    val currentEpoch = EpochProgress(NonNegLong(300L)) // we are at 300, well BEFORE 500
    val prevEpoch = EpochProgress(NonNegLong(299L)) // no natural-expiry sweep this step

    for {
      kpSrc <- KeyPairGenerator.makeKeyPair[IO]
      kpDst <- KeyPairGenerator.makeKeyPair[IO]
      src = kpSrc.getPublic.toAddress
      dst = kpDst.getPublic.toAddress

      as = mkAllowSpend(src, dst, futureExpiry, "early")
      asHashed <- as.toHashed

      // Prior state @ ord1: one active global allow-spend with a FUTURE expiry, plus the source balance.
      lastActiveGlobal = SortedMap(src -> SortedSet(as))
      priorInfo = GlobalSnapshotInfo.empty.copy(
        balances = SortedMap(src -> Balance(NonNegLong(1000L))),
        activeAllowSpends = SortedMap(Option.empty[Address] -> lastActiveGlobal).some
      )

      // --- PRODUCER PATH: seed, then run the manager, then write the delta incrementally ---
      prodStore <- freshStore
      _ <- seedFromInfo(prodStore, priorInfo)
      asMgr = AllowSpendStateManager.make[IO](GlobalStateReader.fromMptStore(prodStore))

      // A spend-tx that consumes `as` — this removes it from the active set EARLY (epoch 300 < 500).
      spendTx = SpendTransaction(
        allowSpendRef = asHashed.hash.some,
        currencyId = None,
        amount = SwapAmount(PosLong(100L)),
        source = src,
        destination = dst
      )

      asResult <- asMgr.acceptAllowSpends(
        currentEpoch,
        prevEpoch,
        SortedMap.empty[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]],
        SortedMap(Option.empty[Address] -> lastActiveGlobal),
        List(spendTx),
        Map.empty[Address, EpochProgress]
      )

      // Post-state full active map (what the rebuild will index from): the source's set is now empty → dropped.
      postActiveAllow: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]] =
        asResult.fullState.view.mapValues(_.filter(_._2.nonEmpty)).filter(_._2.nonEmpty).to(SortedMap)

      acc = StateChangesAccumulator(
        activeAllowSpends = asResult.deltas,
        removedAllowSpendKeys = asResult.removedKeys,
        allowSpendExpiryIndex = asResult.expiryIndexDelta
      )
      _ <- prodStore.syncFromStateChanges(acc, ord2)
      (incRoot, incBytes) <- rootAndBytes(prodStore, ord2)
      orphanBucket <- prodStore.getExpiryBucket[AllowSpendExpiryKey](SystemNamespaceLabel.ExpiryIndexAllowSpends, futureExpiry)

      // --- REBUILD PATH: fresh store, rebuild straight from the post-state GSI active records ---
      postInfo = priorInfo.copy(
        activeAllowSpends = postActiveAllow.some
      )
      rebuildStore <- freshStore
      _ <- rebuildStore.syncFromGlobalSnapshotInfo(postInfo, ord2)
      (rebuildRoot, rebuildBytes) <- rootAndBytes(rebuildStore, ord2)
    } yield {
      val onlyInInc = incBytes.keySet -- rebuildBytes.keySet
      val onlyInRebuild = rebuildBytes.keySet -- incBytes.keySet
      expect.all(
        clue(incRoot) === clue(rebuildRoot),
        clue(incBytes) == clue(rebuildBytes),
        clue(onlyInInc).isEmpty,
        clue(onlyInRebuild).isEmpty,
        // The future-epoch expiry bucket must NOT survive in the incremental MPT (orphan check).
        clue(orphanBucket).isEmpty
      )
    }
  }

  // ===========================================================================
  // Token-lock EARLY removal (consumed by token-unlock before its future unlock)
  // ===========================================================================

  test("token-lock unlocked early by a token-unlock: incremental MPT root === from-GSI rebuild root (no orphan future-epoch bucket)") {
    res =>
      implicit val (h, sp, js) = res
      val ord2 = SnapshotOrdinal(NonNegLong(2L))
      val futureUnlock = EpochProgress(NonNegLong(500L))
      val currentEpoch = EpochProgress(NonNegLong(300L))
      val prevEpoch = EpochProgress(NonNegLong(299L))

      for {
        kpSrc <- KeyPairGenerator.makeKeyPair[IO]
        src = kpSrc.getPublic.toAddress

        tl = mkTokenLock(src, futureUnlock.some, "early")
        tlHashed <- tl.toHashed

        lastActive = SortedMap(src -> SortedSet(tl))
        priorInfo = GlobalSnapshotInfo.empty.copy(
          balances = SortedMap(src -> Balance(NonNegLong(1000L))),
          activeTokenLocks = lastActive.some
        )

        // --- PRODUCER PATH ---
        prodStore <- freshStore
        _ <- seedFromInfo(prodStore, priorInfo)
        tlMgr = TokenLockStateManager.make[IO](GlobalStateReader.fromMptStore(prodStore))

        // A token-unlock referencing `tl` — removes it from active EARLY (epoch 300 < 500).
        tokenUnlock = TokenUnlock(
          tokenLockRef = tlHashed.hash,
          amount = tl.amount,
          currencyId = tl.currencyId,
          source = src
        )

        tlResult <- tlMgr.acceptTokenLocks(
          currentEpoch,
          prevEpoch,
          SortedMap.empty[Address, SortedSet[Signed[TokenLock]]],
          lastActive,
          Map(src -> List(tokenUnlock))
        )

        postActiveTl: SortedMap[Address, SortedSet[Signed[TokenLock]]] =
          tlResult.fullState.filter(_._2.nonEmpty)

        acc = StateChangesAccumulator(
          activeTokenLocks = tlResult.deltas,
          removedTokenLockKeys = tlResult.removedKeys,
          tokenLockExpiryIndex = tlResult.expiryIndexDelta
        )
        _ <- prodStore.syncFromStateChanges(acc, ord2)
        (incRoot, incBytes) <- rootAndBytes(prodStore, ord2)
        orphanBucket <- prodStore.getExpiryBucket[TokenLockExpiryKey](SystemNamespaceLabel.ExpiryIndexTokenLocks, futureUnlock)

        // --- REBUILD PATH ---
        postInfo = priorInfo.copy(activeTokenLocks = postActiveTl.some)
        rebuildStore <- freshStore
        _ <- rebuildStore.syncFromGlobalSnapshotInfo(postInfo, ord2)
        (rebuildRoot, rebuildBytes) <- rootAndBytes(rebuildStore, ord2)
      } yield {
        val onlyInInc = incBytes.keySet -- rebuildBytes.keySet
        val onlyInRebuild = rebuildBytes.keySet -- incBytes.keySet
        expect.all(
          clue(incRoot) === clue(rebuildRoot),
          clue(incBytes) == clue(rebuildBytes),
          clue(onlyInInc).isEmpty,
          clue(onlyInRebuild).isEmpty,
          clue(orphanBucket).isEmpty
        )
      }
  }

  // ===========================================================================
  // Active-address-index probe: balance driven to 0.
  //
  // The Balances active-address index is written with `removed = Set.empty` in `syncFromStateChanges`, so it NEVER shrinks. The rebuild
  // derives the index from `info.balances.keySet`. They stay in lockstep ONLY because the canonical `info.balances` also retains the
  // zero-balance key (it is itself sourced from the never-pruned index via `materializeAllBalancesFromMpt`). This test pins that
  // self-consistency: a 0-balance address must appear identically on both paths (key present, value = Balance.empty), with equal roots.
  // ===========================================================================

  test("balance driven to 0: incremental MPT root === from-GSI rebuild root (zero-balance key retained identically on both paths)") { res =>
    implicit val (h, sp, js) = res
    val ord1 = SnapshotOrdinal(NonNegLong(1L))
    val ord2 = SnapshotOrdinal(NonNegLong(2L))

    for {
      kpA <- KeyPairGenerator.makeKeyPair[IO]
      kpB <- KeyPairGenerator.makeKeyPair[IO]
      addrA = kpA.getPublic.toAddress
      addrB = kpB.getPublic.toAddress

      // PRODUCER PATH: ord1 seeds two non-zero balances incrementally (writing the Balances active-address index);
      // ord2 drives addrA to 0 via a balance delta (no removal set exists for balances — the index keeps addrA).
      prodStore <- freshStore
      _ <- prodStore.syncFromStateChanges(
        StateChangesAccumulator(balances = SortedMap(addrA -> Balance(NonNegLong(1000L)), addrB -> Balance(NonNegLong(2000L)))),
        ord1
      )
      _ <- prodStore.syncFromStateChanges(
        StateChangesAccumulator(balances = SortedMap(addrA -> Balance.empty)),
        ord2
      )
      (incRoot, incBytes) <- rootAndBytes(prodStore, ord2)

      // REBUILD PATH: the canonical post-state GSI retains addrA at Balance.empty (the producer's `info.balances` is
      // `priorBalances ++ delta` and never drops a key). Rebuild indexes from `info.balances.keySet`.
      postInfo = GlobalSnapshotInfo.empty.copy(
        balances = SortedMap(addrA -> Balance.empty, addrB -> Balance(NonNegLong(2000L)))
      )
      rebuildStore <- freshStore
      _ <- rebuildStore.syncFromGlobalSnapshotInfo(postInfo, ord2)
      (rebuildRoot, rebuildBytes) <- rootAndBytes(rebuildStore, ord2)
    } yield {
      val onlyInInc = incBytes.keySet -- rebuildBytes.keySet
      val onlyInRebuild = rebuildBytes.keySet -- incBytes.keySet
      expect.all(
        clue(incRoot) === clue(rebuildRoot),
        clue(incBytes) == clue(rebuildBytes),
        clue(onlyInInc).isEmpty,
        clue(onlyInRebuild).isEmpty
      )
    }
  }

  // ===========================================================================
  // Cross-ordinal accumulation: add at ord2, then early-remove at ord3, on the
  // SAME incremental store. Catches an orphan that only manifests once the
  // expiry bucket / active set was first written incrementally (not seeded).
  // ===========================================================================

  test("allow-spend added then consumed across ordinals (fully incremental): root === from-GSI rebuild of the post-state") { res =>
    implicit val (h, sp, js) = res
    val ord1 = SnapshotOrdinal(NonNegLong(1L))
    val ord2 = SnapshotOrdinal(NonNegLong(2L))
    val ord3 = SnapshotOrdinal(NonNegLong(3L))
    val futureExpiry = EpochProgress(NonNegLong(500L))
    val epochAtAdd = EpochProgress(NonNegLong(200L))
    val epochAtRemove = EpochProgress(NonNegLong(300L))

    for {
      kpSrc <- KeyPairGenerator.makeKeyPair[IO]
      kpDst <- KeyPairGenerator.makeKeyPair[IO]
      src = kpSrc.getPublic.toAddress
      dst = kpDst.getPublic.toAddress

      as = mkAllowSpend(src, dst, futureExpiry, "xacc")
      asHashed <- as.toHashed

      // ord1: balance only (empty active sets).
      prodStore <- freshStore
      _ <- prodStore.syncFromStateChanges(
        StateChangesAccumulator(balances = SortedMap(src -> Balance(NonNegLong(1000L)))),
        ord1
      )

      // ord2: the allow-spend ENTERS the active set incrementally — writes its expiry bucket at 500 + active set + index.
      asMgr2 = AllowSpendStateManager.make[IO](GlobalStateReader.fromMptStore(prodStore))
      addResult <- asMgr2.acceptAllowSpends(
        epochAtAdd,
        EpochProgress(NonNegLong(199L)),
        SortedMap.empty[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        SortedMap(src -> SortedSet(as)), // incoming global allow-spend
        SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        List.empty,
        Map.empty[Address, EpochProgress]
      )
      _ <- prodStore.syncFromStateChanges(
        StateChangesAccumulator(
          activeAllowSpends = addResult.deltas,
          removedAllowSpendKeys = addResult.removedKeys,
          allowSpendExpiryIndex = addResult.expiryIndexDelta
        ),
        ord2
      )

      // ord3: a spend-tx consumes it EARLY (epoch 300 < 500).
      spendTx = SpendTransaction(asHashed.hash.some, None, SwapAmount(PosLong(100L)), src, dst)
      asMgr3 = AllowSpendStateManager.make[IO](GlobalStateReader.fromMptStore(prodStore))
      removeResult <- asMgr3.acceptAllowSpends(
        epochAtRemove,
        EpochProgress(NonNegLong(299L)),
        SortedMap.empty[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]],
        SortedMap(Option.empty[Address] -> SortedMap(src -> SortedSet(as))), // last active = the record added at ord2
        List(spendTx),
        Map.empty[Address, EpochProgress]
      )
      _ <- prodStore.syncFromStateChanges(
        StateChangesAccumulator(
          activeAllowSpends = removeResult.deltas,
          removedAllowSpendKeys = removeResult.removedKeys,
          allowSpendExpiryIndex = removeResult.expiryIndexDelta
        ),
        ord3
      )
      (incRoot, incBytes) <- rootAndBytes(prodStore, ord3)
      orphanBucket <- prodStore.getExpiryBucket[AllowSpendExpiryKey](SystemNamespaceLabel.ExpiryIndexAllowSpends, futureExpiry)

      // REBUILD: post-state has NO active allow-spends (consumed), just the source balance.
      postInfo = GlobalSnapshotInfo.empty.copy(balances = SortedMap(src -> Balance(NonNegLong(1000L))))
      rebuildStore <- freshStore
      _ <- rebuildStore.syncFromGlobalSnapshotInfo(postInfo, ord3)
      (rebuildRoot, rebuildBytes) <- rootAndBytes(rebuildStore, ord3)
    } yield {
      val onlyInInc = incBytes.keySet -- rebuildBytes.keySet
      val onlyInRebuild = rebuildBytes.keySet -- incBytes.keySet
      expect.all(
        clue(incRoot) === clue(rebuildRoot),
        clue(incBytes) == clue(rebuildBytes),
        clue(onlyInInc).isEmpty,
        clue(onlyInRebuild).isEmpty,
        clue(orphanBucket).isEmpty
      )
    }
  }

  // ===========================================================================
  // CurrencySnapshotInfo `Mg*` sub-entry EARLY removal: incremental `syncFromStateChanges` ===
  // delta-replay `toAccumulatorHexDelta` (the GSAM `mptConsistency` self-check path — NOT the
  // from-GSI rebuild the tests above use). A token-lock expiring INSIDE a metagraph's
  // CurrencySnapshotInfo drops an `MgActiveTokenLocks` entry: the incremental writer
  // (`writeCurrencyInfo` -> `infoRemovalKeys`) removes the stale `Mg*` MPT entry, but the delta-replay
  // (`toAccumulatorHexDelta` -> `toAccumulatorRemovalKeys`) historically omitted the `Mg*` removals, so
  // `(preSyncBytes -- removes) ++ upserts` kept the stale entry -> false-positive `mptConsistency=DIVERGED`
  // -> producer crash -> global halt (the ord-435 token-lock-expiry crash). RED before the replay learns the
  // `Mg*` removals; GREEN after.
  // ===========================================================================

  private def mkIncremental(ordinal: Long)(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[Signed[CurrencyIncrementalSnapshot]] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { kp =>
      Signed.forAsyncHasher[IO, CurrencyIncrementalSnapshot](
        CurrencyIncrementalSnapshot(
          SnapshotOrdinal.unsafeApply(ordinal),
          Height.MinValue,
          SubHeight.MinValue,
          Hash.empty,
          SortedSet.empty,
          SortedSet.empty,
          SnapshotTips(SortedSet.empty, SortedSet.empty),
          CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None),
          EpochProgress.MinValue,
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None
        ),
        kp
      )
    }

  test(
    "CurrencySnapshotInfo Mg* removal (in-metagraph token-lock expiry): incremental syncFromStateChanges === " +
      "delta-replay toAccumulatorHexDelta — no stale MgActiveTokenLocks survives the replay"
  ) { res =>
    implicit val (h, sp, js) = res
    type CurrencyArm = Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]
    val ord1 = SnapshotOrdinal(NonNegLong(1L))
    val ord2 = SnapshotOrdinal(NonNegLong(2L))
    for {
      kpMg <- KeyPairGenerator.makeKeyPair[IO]
      kpHolder <- KeyPairGenerator.makeKeyPair[IO]
      mgAddr = kpMg.getPublic.toAddress
      holder = kpHolder.getPublic.toAddress
      priorInfo = CurrencySnapshotInfo(
        lastTxRefs = SortedMap.empty,
        balances = SortedMap.empty,
        lastMessages = None,
        lastFeeTxRefs = None,
        lastAllowSpendRefs = None,
        activeAllowSpends = None,
        globalSnapshotSyncView = None,
        lastTokenLockRefs = None,
        activeTokenLocks = SortedMap(holder -> SortedSet(mkTokenLock(holder, None, "lock1"))).some
      )
      // Next ordinal: the holder's lock has expired -> dropped from activeTokenLocks (an MgActiveTokenLocks removal).
      nextInfo = priorInfo.copy(activeTokenLocks = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]].some)
      inc1 <- mkIncremental(1L)
      inc2 <- mkIncremental(2L)
      accPrior = StateChangesAccumulator(lastCurrencySnapshots = SortedMap[Address, CurrencyArm](mgAddr -> Right((inc1, priorInfo))))
      accNext = StateChangesAccumulator(lastCurrencySnapshots = SortedMap[Address, CurrencyArm](mgAddr -> Right((inc2, nextInfo))))
      store <- freshStore
      _ <- store.syncFromStateChanges(accPrior, ord1)
      preSyncBytes <- store.allEntriesAsBytes
      // INCREMENTAL writer: drops the stale MgActiveTokenLocks entry (correct).
      _ <- store.syncFromStateChanges(accNext, ord2)
      postBytes <- store.allEntriesAsBytes
      // DELTA-REPLAY (the GSAM self-check path): (preSyncBytes -- removes) ++ upserts.
      delta <- GlobalStateConverter.toAccumulatorHexDelta[IO](accNext, preSyncBytes)
      (deltaUpserts, deltaRemoves) = delta
      expectedBytes = (preSyncBytes -- deltaRemoves) ++ deltaUpserts
      postNonSys = GlobalStateKey.nonSystemNamespaceEntries(postBytes).view.mapValues(_.toVector).toMap
      replNonSys = GlobalStateKey.nonSystemNamespaceEntries(expectedBytes).view.mapValues(_.toVector).toMap
    } yield {
      // Keys the replay KEEPS but the (correct) incremental writer dropped — must be empty.
      val staleInReplay = replNonSys.keySet -- postNonSys.keySet
      val missingInReplay = postNonSys.keySet -- replNonSys.keySet
      expect.all(
        clue(staleInReplay).isEmpty,
        clue(missingInReplay).isEmpty,
        clue(replNonSys) == clue(postNonSys)
      )
    }
  }
}
