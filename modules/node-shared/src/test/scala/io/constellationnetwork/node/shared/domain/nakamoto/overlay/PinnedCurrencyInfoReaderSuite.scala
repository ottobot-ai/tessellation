package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps.GlobalStateReaderTypedOps
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.mpt.storages.MptStateStorage
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hashed, Hasher, SecurityProvider}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import fs2.io.file.Files
import weaver.MutableIOSuite

/** Determinism + retention unit suite for [[PinnedCurrencyInfoReader]] (Track-1 blocker-2a).
  *
  * Fixture strategy mirrors `ShardSubtreeProofServiceSuite` (real `GlobalStateConverter.currencySnapshotMgEntries` for the per-MG byte map,
  * so the producer/follower byte path is exercised end-to-end) and `LastNGlobalSnapshotStorageSuite` (a minimal
  * `Hashed[GlobalIncrementalSnapshot]` carrying a chosen `stateProof.mptRoot`). The ORACLE for the happy path is a DIRECT
  * `getCurrencySnapshotInfo` reconstruction over the SAME committed bytes — so the assertion is "reader == a live finalized-reader read of
  * the pinned committed bytes", robust to the `.some`-empty Option shapes `reconstructCurrencyInfoFrom` produces.
  *
  * Coverage (the hard-reject contract):
  *   1. HAPPY — hash + mptRoot both match, bytes retained ⇒ `Some(info)` == the direct reconstruction (correct hash-verified per-MG Info at
  *      a past ordinal).
  *   1. HASH-MISMATCH — the pinned snapshot resolves but its hash ≠ `expectedGlobalSnapshotHash` ⇒ `None` (a fork's snapshot at the same
  *      ordinal is rejected; no HEAD fallback).
  *   1. EVICTED/MISSING BYTES — snapshot + hash pin OK, but no retained state bytes at the anchor ⇒ `None` (retention doesn't reach the
  *      depth; NO head fallback).
  *   1. ROOT-MISMATCH — snapshot + hash pin OK, bytes retained, but the pinned snapshot's committed `mptRoot` ≠ `sidecarFreeMptRoot(bytes)`
  *      ⇒ `None` (retained bytes don't reproduce the pinned committed root).
  *   1. NO SNAPSHOT — nothing resolvable at the anchor ordinal ⇒ `None`.
  */
object PinnedCurrencyInfoReaderSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  // Low ordinal so `currencySnapshotMgEntries` includes the unrolled `Mg*` fields (same as ShardSubtreeProofServiceSuite).
  implicit val stateProofSelector: StateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(0L)))

  private def addr(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))

  private val mg: Address = addr("mg-pinned-reader")
  private val account: Address = addr("pinned-acct-1")

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal.unsafeApply(n)

  /** A `Signed[CurrencyIncrementalSnapshot]` — only its bytes feed the fieldId-5 incremental leaf (gates `getCurrencySnapshotInfo` to
    * Some).
    */
  private def mkSignedIncremental(snapOrdinal: Long): Signed[CurrencyIncrementalSnapshot] = {
    val proof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("33" * 64)), Signature(Hex("44" * 70)))
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
    Signed(snap, NonEmptySet.of(proof))
  }

  private def infoWithBalances(balances: (Address, Long)*): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap.empty,
      balances = SortedMap.from(balances.map { case (a, v) => a -> Balance(NonNegLong.unsafeFrom(v)) }),
      lastMessages = None,
      lastFeeTxRefs = None,
      lastAllowSpendRefs = None,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = None,
      activeTokenLocks = None
    )

  /** A minimal `Hashed[GlobalIncrementalSnapshot]` at `ordinal` whose `stateProof.mptRoot` is `mptRoot`. The outer `.hash` is the REAL
    * content hash (`toHashed`), so tests pin against it directly and force a mismatch by passing a different expected hash.
    */
  private def mkHashed(ordinal: Long, mptRoot: Option[Hash])(implicit h: Hasher[IO]): IO[Hashed[GlobalIncrementalSnapshot]] = {
    val unsigned = GlobalIncrementalSnapshot(
      ordinal = ord(ordinal),
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

  /** Produce the per-MG committed byte map, its consensus `mptRoot`, and the ORACLE (a direct reconstruction over the same bytes). */
  private def buildBytesAndOracle(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[(Map[Hex, Array[Byte]], Hash, Option[CurrencySnapshotInfo])] = {
    val state: SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]] =
      SortedMap(mg -> Right((mkSignedIncremental(5L), infoWithBalances(account -> 100L, addr("pinned-acct-2") -> 250L))))
    for {
      bytes <- GlobalStateConverter.currencySnapshotMgEntries[IO](state)
      root <- GlobalSnapshotInfo.sidecarFreeMptRoot[IO](bytes)
      producer <- InMemoryMerklePatriciaProducer.make[IO](bytes)
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      oracle <- GlobalStateReader.fromMptStore[IO](store).getCurrencySnapshotInfo(mg)
    } yield (bytes, root, oracle)
  }

  test("HAPPY: returns the hash-verified per-MG Info at a past ordinal (== direct reconstruction of the pinned committed bytes)") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, oracle) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(10L), bytes)
        pinned <- mkHashed(10L, Some(root))
        resolver = (o: SnapshotOrdinal) => (if (o === ord(10L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        got <- reader.readAt(ord(10L), pinned.hash, mg)
      } yield
        expect(oracle.isDefined) &&
          expect(oracle.exists(_.balances.get(account).contains(Balance(NonNegLong(100L))))) &&
          expect.same(got, oracle)
    }
  }

  test("HASH-MISMATCH: pinned snapshot resolves but its hash ≠ expected ⇒ None (no HEAD fallback)") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(10L), bytes)
        pinned <- mkHashed(10L, Some(root))
        resolver = (o: SnapshotOrdinal) => (if (o === ord(10L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        wrongHash = Hash("de" * 32)
        got <- reader.readAt(ord(10L), wrongHash, mg)
      } yield expect(wrongHash =!= pinned.hash) && expect(got.isEmpty)
    }
  }

  test("EVICTED/MISSING BYTES: hash + mptRoot pin OK but no retained state bytes at the anchor ⇒ None (no HEAD fallback)") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (_, root, _) = boe
        // Byte store deliberately EMPTY at ordinal 20 (simulates retention eviction / a depth the store can't reach).
        byteStore <- MptStateStorage.make[IO](dir)
        pinned <- mkHashed(20L, Some(root))
        resolver = (o: SnapshotOrdinal) => (if (o === ord(20L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        got <- reader.readAt(ord(20L), pinned.hash, mg)
      } yield expect(got.isEmpty)
    }
  }

  test("ROOT-MISMATCH: retained bytes do not reproduce the pinned snapshot's committed mptRoot ⇒ None") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(30L), bytes)
        wrongRoot = Hash("00" * 32)
        pinned <- mkHashed(30L, Some(wrongRoot))
        resolver = (o: SnapshotOrdinal) => (if (o === ord(30L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        got <- reader.readAt(ord(30L), pinned.hash, mg)
      } yield expect(wrongRoot =!= root) && expect(got.isEmpty)
    }
  }

  test("NO SNAPSHOT: nothing resolvable at the anchor ordinal ⇒ None") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(40L), bytes)
        resolver = (_: SnapshotOrdinal) => none[Hashed[GlobalIncrementalSnapshot]].pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        got <- reader.readAt(ord(40L), Hash("ab" * 32), mg)
      } yield expect(root.value.nonEmpty) && expect(got.isEmpty)
    }
  }
}
