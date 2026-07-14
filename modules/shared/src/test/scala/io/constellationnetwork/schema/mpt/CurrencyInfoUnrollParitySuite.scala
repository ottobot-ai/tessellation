package io.constellationnetwork.schema.mpt

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageOrdinal, MessageType}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.{Signed, signature}
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs._
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedAllowSpendSetCodec

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.MatchesRegex
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** Step-2 guard for `docs/nakamoto/UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md`: the unrolled per-metagraph `CurrencySnapshotInfo` `infoRoot`
  * (the union over the 8 `Mg*` infoSubFields) is byte-identical across the THREE paths that compute it:
  *   - producer `GlobalSnapshotInfo.mptStateProofFromBytes` — union from the actual `entries` (`perFieldGrouping`),
  *   - follower `GlobalStateConverter.currencySnapshotFieldRoots` — union from the SHARED `currencySnapshotEntryBytes` encoder. These are
  *     DIFFERENT code paths; this suite proves they agree (the I2 split-safety invariant — the consensus-critical core of step 2).
  *
  * Also asserts the encoder DROPPED the monolithic `LastCurrencySnapshotInfo` blob (fieldId 6) and now emits per-entry `Mg*` keys.
  */
object CurrencyInfoUnrollParitySuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private implicit val selector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal.unsafeApply(Long.MaxValue))
  private implicit val withdrawalTimeLimit: WithdrawalTimeLimit = WithdrawalTimeLimit(EpochProgress(NonNegLong(100L)).some)

  private val ord: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(100L))
  private val v0 = SnapshotVersion(Refined.unsafeApply[String, MatchesRegex["^(0\\.0\\.1|1\\.0\\.0)$"]]("0.0.1"))
  private val testProofs = NonEmptySet.one(signature.SignatureProof(Id(Hex("")), signature.Signature(Hex(""))))

  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  private def mkTokenLock(source: Address, label: String): Signed[TokenLock] =
    Signed(
      TokenLock(
        source,
        TokenLockAmount(PosLong(200L)),
        TokenLockFee(NonNegLong(0L)),
        TokenLockReference(TokenLockOrdinal(NonNegLong(0L)), testHash(s"tl-parent-$label")),
        None,
        EpochProgress(NonNegLong(700L)).some,
        None
      ),
      testProofs
    )

  /** A `CurrencySnapshotInfo` populating 6 of the 8 unrolled `infoSubFields` (the Address-keyed ones; the two hashed-key fields
    * `lastMessages`/`globalSnapshotSyncView` need signing fixtures and are covered by the step-3 round-trip). `activeAllowSpends` is left
    * `None` — it stays in fieldId-7, NOT an `infoSubField`, so it does not contribute to `infoRoot`.
    */
  private def richInfo(holder: Address): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap(holder -> TransactionReference(TransactionOrdinal(NonNegLong(1L)), testHash("csi-tx"))),
      balances = SortedMap(holder -> Balance(NonNegLong(555L))),
      lastMessages = None,
      lastFeeTxRefs = SortedMap(holder -> TransactionReference(TransactionOrdinal(NonNegLong(2L)), testHash("csi-fee"))).some,
      lastAllowSpendRefs = SortedMap(holder -> AllowSpendReference(AllowSpendOrdinal(NonNegLong(3L)), testHash("csi-as"))).some,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = SortedMap(holder -> TokenLockReference(TokenLockOrdinal(NonNegLong(4L)), testHash("csi-tlr"))).some,
      activeTokenLocks = SortedMap(holder -> SortedSet(mkTokenLock(holder, "x"))).some
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

  private def infoFor(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[GlobalSnapshotInfo] =
    for {
      mgAddr <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      holder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      inc <- signedIncremental(7L)
    } yield
      GlobalSnapshotInfo.empty.copy(
        lastCurrencySnapshots = SortedMap(mgAddr -> (inc, richInfo(holder)).asRight[Signed[CurrencySnapshot]])
      )

  test("infoRoot: producer mptStateProof (union-from-entries) === follower currencySnapshotFieldRoots (shared encoder)") { res =>
    implicit val (h, sp, js) = res
    for {
      info <- infoFor
      proof <- GlobalSnapshotInfo.mptStateProof[IO](info)
      (followerInc, followerInfo) <- GlobalStateConverter.currencySnapshotFieldRoots[IO](info.lastCurrencySnapshots)
    } yield
      expect.all(
        clue(proof.lastCurrencySnapshotsProof).isDefined,
        proof.lastCurrencySnapshotsProof.map(_.incrementalRoot) === clue(followerInc).some,
        proof.lastCurrencySnapshotsProof.map(_.infoRoot) === clue(followerInfo).some,
        // the union over 6 populated infoSubFields is non-empty — proves the union actually covered the unrolled entries
        clue(followerInfo) =!= Hash.empty
      )
  }

  test("encoder: drops the fieldId-6 blob and emits per-entry Mg* keys") { res =>
    implicit val (h, sp, js) = res
    for {
      info <- infoFor
      entries <- GlobalStateConverter.currencySnapshotEntryBytes[IO](info.lastCurrencySnapshots)
      fieldIds = entries.keys.map(_.fieldId).toSet
    } yield
      expect.all(
        // the monolithic blob is gone
        !fieldIds.contains(GlobalStateFieldId.LastCurrencySnapshotInfo),
        // the incremental partition stays
        fieldIds.contains(GlobalStateFieldId.LastIncrementalCurrencySnapshots),
        // the 6 populated unrolled sub-fields are present
        fieldIds.contains(GlobalStateFieldId.MgBalances),
        fieldIds.contains(GlobalStateFieldId.MgLastTxRefs),
        fieldIds.contains(GlobalStateFieldId.MgLastFeeTxRefs),
        fieldIds.contains(GlobalStateFieldId.MgLastAllowSpendRefs),
        fieldIds.contains(GlobalStateFieldId.MgLastTokenLockRefs),
        fieldIds.contains(GlobalStateFieldId.MgActiveTokenLocks),
        // every emitted key is either the incremental or an infoSubField (no stray partitions)
        clue(fieldIds).forall(f => f == GlobalStateFieldId.LastIncrementalCurrencySnapshots || GlobalStateFieldId.infoSubFields.contains(f))
      )
  }

  private def freshStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    InMemoryMerklePatriciaProducer.make[IO]().flatMap(p => MptStore.make[IO, GlobalStateKey](p, GlobalStateKey.toHex[IO]))

  private def mkAllowSpend(source: Address, label: String, currencyId: Option[CurrencyId]): Signed[AllowSpend] =
    Signed(
      AllowSpend(
        source = source,
        destination = source,
        currencyId = currencyId,
        amount = SwapAmount(PosLong(100L)),
        fee = AllowSpendFee(NonNegLong(0L)),
        parent = AllowSpendReference(AllowSpendOrdinal(NonNegLong(0L)), testHash(s"as-parent-$label")),
        lastValidEpochProgress = EpochProgress(NonNegLong(500L)),
        approvers = List.empty
      ),
      testProofs
    )

  private def mkCurrencyMessage(addr: Address, mgAddr: Address): Signed[CurrencyMessage] =
    Signed(CurrencyMessage(MessageType.Staking, addr, mgAddr, MessageOrdinal(NonNegLong(0L))), testProofs)

  /** Every optional field `Some(_)` (the post-tess3 convention) and every unrolled partition populated — incl. `lastMessages` (hashed key)
    * and `activeAllowSpends` (fieldId-7, holder = allow-spend `source`). The exact shape reconstruction must reproduce (I1).
    */
  private def allSomeInfo(mgAddr: Address, holder: Address): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap(holder -> TransactionReference(TransactionOrdinal(NonNegLong(1L)), testHash("csi-tx"))),
      balances = SortedMap(holder -> Balance(NonNegLong(555L))),
      lastMessages = SortedMap[MessageType, Signed[CurrencyMessage]](MessageType.Staking -> mkCurrencyMessage(holder, mgAddr)).some,
      lastFeeTxRefs = SortedMap(holder -> TransactionReference(TransactionOrdinal(NonNegLong(2L)), testHash("csi-fee"))).some,
      lastAllowSpendRefs = SortedMap(holder -> AllowSpendReference(AllowSpendOrdinal(NonNegLong(3L)), testHash("csi-as"))).some,
      activeAllowSpends = SortedMap(holder -> SortedSet(mkAllowSpend(holder, "x", CurrencyId(mgAddr).some))).some,
      globalSnapshotSyncView = Some(SortedMap.empty),
      lastTokenLockRefs = SortedMap(holder -> TokenLockReference(TokenLockOrdinal(NonNegLong(4L)), testHash("csi-tlr"))).some,
      activeTokenLocks = SortedMap(holder -> SortedSet(mkTokenLock(holder, "x"))).some
    )

  /** Write the unrolled storage for one MG: the 8 `Mg*` partitions (mirrors `infoEntryBytes`) + the fieldId-7 metagraph-scope
    * `ActiveAllowSpends`. Uses the SAME tuple codecs the encoder uses, so the stored bytes equal `currencySnapshotEntryBytes`'s.
    */
  private def writeUnrolledInfo(store: MptStore[IO, GlobalStateKey], mgAddr: Address, info: CurrencySnapshotInfo)(
    implicit h: Hasher[IO]
  ): IO[Unit] = {
    import GlobalStateFieldId._
    def opt[K, V](m: Option[SortedMap[K, V]]): List[(K, V)] = m.fold(List.empty[(K, V)])(_.toList)
    for {
      _ <- info.balances.toList.traverse_ { case (a, b) => store.insert(GlobalStateKey.metagraphEntry(mgAddr, MgBalances, a), (a, b)) }
      _ <- info.lastTxRefs.toList.traverse_ { case (a, r) => store.insert(GlobalStateKey.metagraphEntry(mgAddr, MgLastTxRefs, a), (a, r)) }
      _ <- opt(info.lastFeeTxRefs).traverse_ {
        case (a, r) => store.insert(GlobalStateKey.metagraphEntry(mgAddr, MgLastFeeTxRefs, a), (a, r))
      }
      _ <- opt(info.lastAllowSpendRefs).traverse_ {
        case (a, r) => store.insert(GlobalStateKey.metagraphEntry(mgAddr, MgLastAllowSpendRefs, a), (a, r))
      }
      _ <- opt(info.lastTokenLockRefs).traverse_ {
        case (a, r) => store.insert(GlobalStateKey.metagraphEntry(mgAddr, MgLastTokenLockRefs, a), (a, r))
      }
      _ <- opt(info.activeTokenLocks).traverse_ {
        case (a, s) => store.insert(GlobalStateKey.metagraphEntry(mgAddr, MgActiveTokenLocks, a), (a, s))
      }
      _ <- opt(info.lastMessages).traverse_ {
        case (mt, m) => GlobalStateKey.metagraphEntryHashed[IO](mgAddr, MgLastMessages, mt.value).flatMap(store.insert(_, (mt, m)))
      }
      _ <- opt(info.globalSnapshotSyncView).traverse_ {
        case (p, s) =>
          GlobalStateKey.metagraphEntryHashed[IO](mgAddr, MgGlobalSnapshotSyncView, p.value.value).flatMap(store.insert(_, (p, s)))
      }
      _ <- opt(info.activeAllowSpends).traverse_ {
        case (holder, set) => store.insert(GlobalStateKey.hypergraph(ActiveAllowSpends, mgAddr.some, holder), set)
      }
    } yield ()
  }

  test("round-trip (I1): reconstructCurrencySnapshotInfo(write(info)) == info — all 9 fields incl. hashed-key + fieldId-7") { res =>
    implicit val (h, sp, js) = res
    for {
      mgAddr <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      holder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      info = allSomeInfo(mgAddr, holder)
      store <- freshStore
      _ <- writeUnrolledInfo(store, mgAddr, info)
      reconstructed <- store.reconstructCurrencySnapshotInfo(mgAddr)
    } yield expect(clue(reconstructed) == clue(info))
  }

  test("per-metagraph reconstruction rejects an active allow-spend stored under the wrong source key") { res =>
    implicit val (h, sp, js) = res
    for {
      mgAddr <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      valueSource <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      keySource <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      store <- freshStore
      value = mkAllowSpend(valueSource, "wrong-key", CurrencyId(mgAddr).some)
      _ <- store.insert(GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, mgAddr.some, keySource), SortedSet(value))
      result <- store.reconstructCurrencySnapshotInfo(mgAddr).attempt
    } yield expect(result.left.exists(_.getMessage.contains("key/value mismatch")))
  }

  test("per-metagraph reconstruction rejects an active allow-spend with a non-matching embedded scope") { res =>
    implicit val (h, sp, js) = res
    for {
      mgAddr <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      source <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      store <- freshStore
      value = mkAllowSpend(source, "wrong-scope", none)
      _ <- store.insert(GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, mgAddr.some, source), SortedSet(value))
      result <- store.reconstructCurrencySnapshotInfo(mgAddr).attempt
    } yield expect(result.left.exists(_.getMessage.contains("unexpected scope")))
  }

  test("per-metagraph reconstruction rejects an empty active allow-spend set") { res =>
    implicit val (h, sp, js) = res
    for {
      mgAddr <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      source <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      store <- freshStore
      _ <- store.insert(
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, mgAddr.some, source),
        SortedSet.empty[Signed[AllowSpend]]
      )
      result <- store.reconstructCurrencySnapshotInfo(mgAddr).attempt
    } yield expect(result.left.exists(_.getMessage.contains("empty set")))
  }

  test("per-metagraph reconstruction rejects an active allow-spend set with mixed embedded sources") { res =>
    implicit val (h, sp, js) = res
    for {
      mgAddr <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      sourceA <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      sourceB <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      store <- freshStore
      values = SortedSet(
        mkAllowSpend(sourceA, "mixed-a", CurrencyId(mgAddr).some),
        mkAllowSpend(sourceB, "mixed-b", CurrencyId(mgAddr).some)
      )
      _ <- store.insert(GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, mgAddr.some, sourceA), values)
      result <- store.reconstructCurrencySnapshotInfo(mgAddr).attempt
    } yield expect(result.left.exists(_.getMessage.contains("mixed scope/source")))
  }
}
