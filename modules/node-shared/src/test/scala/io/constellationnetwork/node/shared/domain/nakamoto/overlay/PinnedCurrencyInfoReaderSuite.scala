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
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MptRoot
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
  *   1. ROOT-MISMATCH — snapshot + hash pin OK, bytes retained, but the pinned snapshot's committed `mptRoot` ≠ `consensusMptRoot(bytes)` ⇒
  *      `None` (retained bytes don't reproduce the pinned committed root).
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

  private def executionBase(snapshot: Hashed[GlobalIncrementalSnapshot], mptRoot: Hash): GlobalSnapshotStateRef =
    GlobalSnapshotStateRef(
      ordinal = snapshot.signed.value.ordinal,
      hash = snapshot.hash,
      parentHash = snapshot.signed.value.lastSnapshotHash,
      mptRoot = MptRoot(mptRoot)
    )

  /** Produce the per-MG committed byte map, its consensus `mptRoot`, and the ORACLE (a direct reconstruction over the same bytes). */
  private def buildBytesAndOracle(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[(Map[Hex, Array[Byte]], Hash, Option[CurrencySnapshotInfo])] = {
    val state: SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]] =
      SortedMap(mg -> Right((mkSignedIncremental(5L), infoWithBalances(account -> 100L, addr("pinned-acct-2") -> 250L))))
    for {
      bytes <- GlobalStateConverter.currencySnapshotMgEntries[IO](state)
      root <- GlobalSnapshotInfo.consensusMptRoot[IO](bytes)
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

  import PinnedCurrencyInfoReader.PinnedAnchorRead

  // ===========================================================================
  // Track-1 execution-base-pin: exact signed GlobalSnapshotStateRef + pinnedReaderAt
  // ===========================================================================

  test("execution-base-pin readAtExecutionBase HAPPY: verifies the full signed base reference and returns the oracle") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, oracle) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(10L), bytes)
        pinned <- mkHashed(10L, Some(root))
        base = executionBase(pinned, root)
        resolver = (o: SnapshotOrdinal) => (if (o === ord(10L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        got <- reader.readAtExecutionBase(base, mg)
      } yield
        expect(oracle.isDefined) &&
          expect.same(got, oracle)
    }
  }

  test("execution-base-pin readAtExecutionBase NO-SNAPSHOT: exact base unavailable ⇒ None (never a head fallback)") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(10L), bytes)
        resolver = (_: SnapshotOrdinal) => none[Hashed[GlobalIncrementalSnapshot]].pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        base = GlobalSnapshotStateRef(ord(10L), Hash("ab" * 32), Hash("cd" * 32), MptRoot(root))
        got <- reader.readAtExecutionBase(base, mg)
      } yield expect(got.isEmpty)
    }
  }

  test("execution-base-pin pinnedReaderAt HAPPY: whole-global reader over the pinned bytes reconstructs mg's Info == oracle") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, oracle) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(10L), bytes)
        pinned <- mkHashed(10L, Some(root))
        base = executionBase(pinned, root)
        resolver = (o: SnapshotOrdinal) => (if (o === ord(10L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        // The committee/watchtower `reExecDerivationAtPinnedBase` prior reader — a full GlobalStateReader pinned at the execution base.
        pinnedReaderOpt <- reader.pinnedReaderAt(base)
        gotInfo <- pinnedReaderOpt.traverse(_.getCurrencySnapshotInfo(mg))
      } yield
        expect(pinnedReaderOpt.isDefined) &&
          expect.same(gotInfo.flatten, oracle)
    }
  }

  test("execution-base-pin pinnedReaderAt ROOT-MISMATCH: retained bytes don't reproduce the pinned committed root ⇒ None") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(10L), bytes)
        wrongRoot = Hash("00" * 32)
        pinned <- mkHashed(10L, Some(wrongRoot))
        base = executionBase(pinned, wrongRoot)
        resolver = (o: SnapshotOrdinal) => (if (o === ord(10L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        got <- reader.pinnedReaderAt(base)
      } yield expect(wrongRoot =!= root) && expect(got.isEmpty)
    }
  }

  test("execution-base exact identity: same-ordinal wrong hash, parent, or root fails closed") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(10L), bytes)
        pinned <- mkHashed(10L, Some(root))
        base = executionBase(pinned, root)
        wrongHash = Hash("de" * 32)
        wrongParent = Hash("ca" * 32)
        wrongRoot = MptRoot(Hash("00" * 32))
        resolver = (o: SnapshotOrdinal) => (if (o === ord(10L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        hashResult <- reader.readAtExecutionBaseVerified(base.copy(hash = wrongHash), mg)
        parentResult <- reader.readAtExecutionBaseVerified(base.copy(parentHash = wrongParent), mg)
        rootResult <- reader.readAtExecutionBaseVerified(base.copy(mptRoot = wrongRoot), mg)
        wholeReaders <- List(
          base.copy(hash = wrongHash),
          base.copy(parentHash = wrongParent),
          base.copy(mptRoot = wrongRoot)
        ).traverse(reader.pinnedReaderAt)
      } yield
        expect.all(
          wrongHash =!= base.hash,
          wrongParent =!= base.parentHash,
          wrongRoot =!= base.mptRoot,
          hashResult == PinnedAnchorRead.AnchorUnreadable,
          parentResult == PinnedAnchorRead.AnchorUnreadable,
          rootResult == PinnedAnchorRead.AnchorUnreadable,
          wholeReaders.forall(_.isEmpty)
        )
    }
  }

  // ===========================================================================
  // Track-1 execution-base-pin GENESIS SEAM: readAtExecutionBaseVerified — the THREE-VALUED read for the byteDiff-adopt consumer.
  // The anchor-vs-absent split: every anchor failure ⇒ AnchorUnreadable (fail-closed); a clean verify carries the per-MG
  // reconstruction's own Option verbatim (None = the MG has no committed state under the VERIFIED root — a pinned fact,
  // the brand-new-MG first advance the old Option view conflated with the failures).
  // ===========================================================================

  test("readAtExecutionBaseVerified VERIFIED-PRESENT: exact base + MG committed ⇒ AnchorVerified(Some(oracle))") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, oracle) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(10L), bytes)
        pinned <- mkHashed(10L, Some(root))
        base = executionBase(pinned, root)
        resolver = (o: SnapshotOrdinal) => (if (o === ord(10L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        got <- reader.readAtExecutionBaseVerified(base, mg)
      } yield
        expect(oracle.isDefined) &&
          expect(got == PinnedAnchorRead.AnchorVerified(oracle))
    }
  }

  test("readAtExecutionBaseVerified VERIFIED-ABSENT: clean verify but queried MG has no state ⇒ AnchorVerified(None)") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      val mgAbsent = addr("mg-never-seen-at-anchor")
      for {
        boe <- buildBytesAndOracle // bytes carry ONLY `mg` — the anchor verifies with real content, `mgAbsent` is not in it
        (bytes, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(10L), bytes)
        pinned <- mkHashed(10L, Some(root))
        base = executionBase(pinned, root)
        resolver = (o: SnapshotOrdinal) => (if (o === ord(10L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        got <- reader.readAtExecutionBaseVerified(base, mgAbsent)
        // BLAST-RADIUS GUARD: the legacy Option view still collapses this to None (I-PIN-style consumers unchanged).
        gotOption <- reader.readAtExecutionBase(base, mgAbsent)
      } yield
        expect(got == PinnedAnchorRead.AnchorVerified(none[CurrencySnapshotInfo])) &&
          expect(gotOption.isEmpty)
    }
  }

  test("readAtExecutionBaseVerified UNREADABLE (no snapshot): exact base unavailable ⇒ AnchorUnreadable") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(10L), bytes)
        resolver = (_: SnapshotOrdinal) => none[Hashed[GlobalIncrementalSnapshot]].pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        base = GlobalSnapshotStateRef(ord(10L), Hash("ab" * 32), Hash("cd" * 32), MptRoot(root))
        got <- reader.readAtExecutionBaseVerified(base, mg)
      } yield expect(got == PinnedAnchorRead.AnchorUnreadable)
    }
  }

  test("readAtExecutionBaseVerified UNREADABLE (no mptRoot): snapshot has no committed root ⇒ AnchorUnreadable") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(10L), bytes)
        pinned <- mkHashed(10L, mptRoot = None)
        base = executionBase(pinned, root)
        resolver = (o: SnapshotOrdinal) => (if (o === ord(10L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        got <- reader.readAtExecutionBaseVerified(base, mg)
      } yield expect(got == PinnedAnchorRead.AnchorUnreadable)
    }
  }

  test("readAtExecutionBaseVerified UNREADABLE (evicted bytes): no retained bytes ⇒ AnchorUnreadable, NOT verified-absent") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (_, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir) // deliberately EMPTY at the anchor
        pinned <- mkHashed(20L, Some(root))
        base = executionBase(pinned, root)
        resolver = (o: SnapshotOrdinal) => (if (o === ord(20L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        got <- reader.readAtExecutionBaseVerified(base, mg)
      } yield expect(got == PinnedAnchorRead.AnchorUnreadable)
    }
  }

  test("readAtExecutionBaseVerified UNREADABLE (root mismatch): retained bytes don't reproduce the pinned root ⇒ AnchorUnreadable") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        _ <- byteStore.writeState(ord(30L), bytes)
        wrongRoot = Hash("00" * 32)
        pinned <- mkHashed(30L, Some(wrongRoot))
        base = executionBase(pinned, wrongRoot)
        resolver = (o: SnapshotOrdinal) => (if (o === ord(30L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        got <- reader.readAtExecutionBaseVerified(base, mg)
      } yield expect(wrongRoot =!= root) && expect(got == PinnedAnchorRead.AnchorUnreadable)
    }
  }

  // ===========================================================================
  // Signed-byte-store read-time BACKFILL (2026-07-09) — the hole healer. A creation-side staging race (fail-closed reorg adopt /
  // same-ordinal proposal-race loss / catch-up jump) leaves an ordinal MISSING from the signed store even though the node's own
  // canonical chain finalized it; a shard checkpoint stamping that exact `executionBase` then fail-closes until the bytes are recovered (the
  // 2mg/2shard token-lock mirror freeze: hole at ord 227 ⇒ `pinned ANCHOR ... unreadable` ×106). The backfill fetches the byte map
  // from a peer AT READ TIME, verifies it against the LOCALLY-committed `stateProof.mptRoot`, strips to `consensusRootEntries`,
  // persists, and serves. Wrong-root / missing peer bytes stay fail-closed with the store UNTOUCHED.
  // ===========================================================================

  import PinnedCurrencyInfoReader.PinnedByteBackfill

  private def backfillOf(f: SnapshotOrdinal => IO[Option[Map[Hex, Array[Byte]]]]): PinnedByteBackfill[IO] =
    new PinnedByteBackfill[IO] {
      def fetch(ordinal: SnapshotOrdinal): IO[Option[Map[Hex, Array[Byte]]]] = f(ordinal)
    }

  test(
    "BACKFILL RED→GREEN: hole at a stamped execution-base ⇒ AnchorUnreadable without backfill; WITH backfill serving root-correct " +
      "bytes the SAME read heals — AnchorVerified(oracle), bytes persisted, subsequent reads need no peer"
  ) { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, oracle) = boe
        // The HOLE: the local signed store has NOTHING at the stamped execution-base ordinal (creation-side staging race), but the
        // node's own finalized chain DOES resolve the snapshot there (root = the local verification anchor).
        byteStore <- MptStateStorage.make[IO](dir)
        pinned <- mkHashed(227L, Some(root))
        base = executionBase(pinned, root)
        resolver = (o: SnapshotOrdinal) => (if (o === ord(227L)) pinned.some else none).pure[IO]

        // RED — the pre-backfill behavior (exactly what froze the mirror): fail-closed unreadable at the hole.
        readerNoBackfill = PinnedCurrencyInfoReader.make[IO](byteStore, resolver)
        red <- readerNoBackfill.readAtExecutionBaseVerified(base, mg)

        // GREEN — same store, same resolver, backfill wired: the peer serves the finalized bytes for ord 227.
        fetches <- IO.ref(0)
        backfill = backfillOf(o => fetches.update(_ + 1) *> (if (o === ord(227L)) bytes.some else none).pure[IO])
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver, backfill = Some(backfill))
        green <- reader.readAtExecutionBaseVerified(base, mg)

        // Healed ON DISK: the store now serves the ordinal, so a later read works with NO backfill at all.
        persisted <- byteStore.readState(ord(227L))
        afterHeal <- readerNoBackfill.readAtExecutionBaseVerified(base, mg)
        // And the whole-global pinned replay reader resolves too.
        pinnedReaderOpt <- reader.pinnedReaderAt(base)
        fetchCount <- fetches.get
      } yield
        expect(red == PinnedAnchorRead.AnchorUnreadable) &&
          expect(oracle.isDefined) &&
          expect(green == PinnedAnchorRead.AnchorVerified(oracle)) &&
          expect(persisted.isDefined) &&
          expect(afterHeal == PinnedAnchorRead.AnchorVerified(oracle)) &&
          expect(pinnedReaderOpt.isDefined) &&
          // The store hit path serves post-heal reads — the single RED+GREEN read pair cost exactly one fetch.
          expect.same(1, fetchCount)
    }
  }

  test(
    "BACKFILL NEGATIVE (wrong root): peer bytes that do NOT reproduce the locally-committed root are REJECTED — " +
      "AnchorUnreadable, store untouched, still fail-closed on the next read"
  ) { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (_, root, _) = boe
        // Adversarial/stale peer state: OTHER content (different balances ⇒ different consensus root).
        wrongState = SortedMap(
          mg -> (Right((mkSignedIncremental(5L), infoWithBalances(account -> 666L))): Either[
            Signed[CurrencySnapshot],
            (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)
          ])
        )
        wrongBytes <- GlobalStateConverter.currencySnapshotMgEntries[IO](wrongState)
        wrongBytesRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](wrongBytes)
        byteStore <- MptStateStorage.make[IO](dir) // hole at the anchor
        pinned <- mkHashed(227L, Some(root)) // the local committed fixture root does not authorize `wrongBytes`
        base = executionBase(pinned, root)
        resolver = (o: SnapshotOrdinal) => (if (o === ord(227L)) pinned.some else none).pure[IO]
        backfill = backfillOf(_ => wrongBytes.some.pure[IO])
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver, backfill = Some(backfill))
        got <- reader.readAtExecutionBaseVerified(base, mg)
        persisted <- byteStore.readState(ord(227L))
        gotAgain <- reader.readAtExecutionBaseVerified(base, mg)
      } yield
        expect(wrongBytesRoot =!= root) && // the adversarial map genuinely recomputes a different root
          expect(got == PinnedAnchorRead.AnchorUnreadable) && // rejected, fail-closed
          expect(persisted.isEmpty) && // NEVER staged — the store stays untouched by unverified bytes
          expect(gotAgain == PinnedAnchorRead.AnchorUnreadable) // and stays fail-closed (defer), not poisoned
    }
  }

  test("BACKFILL ROOT BINDING: a changed SystemNamespace entry changes the root and the store remains untouched") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, _) = boe
        // A malicious/rich peer response: honest consensus bytes plus an attacker-chosen SystemNamespace entry.
        // SystemNamespace indices are economic state and must be committed, not stripped as a cache.
        poisonKey = Hex("03" + "ab" * 24)
        fetched = bytes + (poisonKey -> Array[Byte](1, 2, 3))
        fetchedRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](fetched)
        byteStore <- MptStateStorage.make[IO](dir)
        pinned <- mkHashed(227L, Some(root))
        base = executionBase(pinned, root)
        resolver = (o: SnapshotOrdinal) => (if (o === ord(227L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver, backfill = Some(backfillOf(_ => fetched.some.pure[IO])))
        got <- reader.readAtExecutionBaseVerified(base, mg)
        persisted <- byteStore.readState(ord(227L))
      } yield
        expect(fetchedRoot =!= root) &&
          expect(got == PinnedAnchorRead.AnchorUnreadable) &&
          expect(persisted.isEmpty)
    }
  }

  test("BACKFILL absent-peer: fetch returns None ⇒ AnchorUnreadable (fail-closed defer), store untouched") { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (_, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        pinned <- mkHashed(227L, Some(root))
        base = executionBase(pinned, root)
        resolver = (o: SnapshotOrdinal) => (if (o === ord(227L)) pinned.some else none).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](byteStore, resolver, backfill = Some(backfillOf(_ => none.pure[IO])))
        got <- reader.readAtExecutionBaseVerified(base, mg)
        persisted <- byteStore.readState(ord(227L))
      } yield expect(got == PinnedAnchorRead.AnchorUnreadable) && expect(persisted.isEmpty)
    }
  }

  test(
    "BACKFILL no-local-anchor: an ordinal the node cannot resolve locally NEVER fetches (no root to verify against) ⇒ AnchorUnreadable"
  ) { res =>
    implicit val (h, _, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        boe <- buildBytesAndOracle
        (bytes, root, _) = boe
        byteStore <- MptStateStorage.make[IO](dir)
        resolver = (_: SnapshotOrdinal) => none[Hashed[GlobalIncrementalSnapshot]].pure[IO]
        fetches <- IO.ref(0)
        reader = PinnedCurrencyInfoReader.make[IO](
          byteStore,
          resolver,
          backfill = Some(backfillOf(_ => fetches.update(_ + 1) *> bytes.some.pure[IO]))
        )
        base = GlobalSnapshotStateRef(ord(999L), Hash("ab" * 32), Hash("cd" * 32), MptRoot(root))
        got <- reader.readAtExecutionBaseVerified(base, mg)
        fetchCount <- fetches.get
      } yield expect(got == PinnedAnchorRead.AnchorUnreadable) && expect.same(0, fetchCount)
    }
  }

  test(
    "PinnedByteBackfill.deduplicated: one in-flight fetch per ordinal — the concurrent loser fails closed (None) and the " +
      "underlying runs once; after completion the ordinal is fetchable again; errors totalize to None"
  ) { _ =>
    for {
      started <- cats.effect.Deferred[IO, Unit]
      gate <- cats.effect.Deferred[IO, Unit]
      calls <- IO.ref(0)
      underlying = (_: SnapshotOrdinal) => calls.update(_ + 1) *> started.complete(()) *> gate.get.as(Map.empty[Hex, Array[Byte]].some)
      bf <- PinnedByteBackfill.deduplicated[IO](underlying)
      winner <- bf.fetch(ord(50L)).start
      _ <- started.get // the winner is INSIDE the underlying fetch now
      loser <- bf.fetch(ord(50L)) // in-flight dedup: immediate None, no second underlying call
      _ <- gate.complete(())
      winnerResult <- winner.joinWithNever
      callsDuringOverlap <- calls.get
      again <- bf.fetch(ord(50L)) // released: the ordinal is fetchable again
      callsAfter <- calls.get
      erroring <- PinnedByteBackfill.deduplicated[IO]((_: SnapshotOrdinal) => IO.raiseError(new RuntimeException("boom")))
      errFetch <- erroring.fetch(ord(51L))
    } yield
      expect(loser.isEmpty) &&
        expect(winnerResult.isDefined) &&
        expect.same(1, callsDuringOverlap) &&
        expect(again.isDefined) &&
        expect.same(2, callsAfter) &&
        expect(errFetch.isEmpty)
  }
}
