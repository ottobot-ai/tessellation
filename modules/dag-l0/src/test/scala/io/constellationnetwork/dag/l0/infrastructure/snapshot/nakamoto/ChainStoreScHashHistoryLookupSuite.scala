package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.std.Supervisor
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.metrics.NoOpMetrics
import io.constellationnetwork.node.shared.nodeSharedKryoRegistrar
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.transaction.RewardTransaction
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.MutableIOSuite

/** Tests for `ChainStoreScHashHistoryLookup` — the chain-store-backed `HistoryLookup` impl used in production by
  * `MetagraphParentOrdinalResolver.resolve`.
  *
  * The existing `MetagraphParentOrdinalResolverSuite` (in `node-shared`) covers the resolver's dispatch logic with a `StubHistory`. This
  * suite exercises the actual chain-walk against a real `NakamotoChainStore.make` with a seeded canonical chain — the consensus-critical hot
  * path on SC binary admission.
  *
  * '''Coverage map.'''
  *   1. ''bestTip → parent → parent walk finds target'' — Forward-canonical walk over a 10-snapshot chain locates a known parent hash 5 hops
  *      back and returns the correct metagraph ordinal.
  *   1. ''maxDepth exceeded'' — A target 24 hops back from bestTip with `maxDepth=5` returns `None` (walk gives up cleanly).
  *   1. ''parent miss (eviction boundary)'' — Walking past the in-memory retention window short-circuits without exception when `chainStore.get`
  *      returns `None` for an evicted parent.
  *   1. ''multi-metagraph isolation'' — A binary from metagraph X whose `parentHash` equals metagraph Y's recorded tip does NOT cross-resolve;
  *      the walk continues past the wrong-metagraph entry.
  *   1. ''concurrent finalize during walk (best-effort safety)'' — Stepping through the walk while `finalize` prunes orphan branches returns a
  *      deterministic result (Some(ord) OR None) without exception.
  *   1. ''disk-fallback placeholder GSI doesn't trip walk'' — The placeholder GSI returned by `getWithOrdinalFallback` has empty
  *      `lastStateChannelSnapshotHashes`; the walk MUST traverse it via plain `chainStore.get` (in-memory only), so evicted ords are absent and
  *      the walk terminates rather than seeing a spurious empty-GSI false negative as authoritative.
  *   1. ''empty chain store / pre-bootstrap'' — `bestTip = None` returns `None` without exception.
  */
object ChainStoreScHashHistoryLookupSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  implicit val metrics: io.constellationnetwork.node.shared.infrastructure.metrics.Metrics[IO] = NoOpMetrics.make

  type Res = (Supervisor[IO], KryoSerializer[IO], JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  def sharedResource: cats.effect.Resource[IO, Res] = for {
    supervisor <- Supervisor[IO]
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](nodeSharedKryoRegistrar)
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (supervisor, ks, j, h, sp)

  private implicit val logger: SelfAwareStructuredLogger[IO] =
    Slf4jLogger.getLoggerFromName[IO]("ChainStoreScHashHistoryLookupSuite")

  // -------------- test fixtures --------------

  private val mgA: Address = Address("DAG0y4eLqhhXUafeE3mgBstezPTnr8L3tZjAtMWB")
  private val mgB: Address = Address("DAG07tqNLYW8jHU9emXcRTT3CfgCUoumwcLghopd")

  /** Synthetic `Hash` carrying a 64-hex-digit string derived from a stable seed. The chain store doesn't verify these against any real
    * payload (the resolver only looks them up in GSI maps), so a deterministic synthetic-hash factory is sufficient.
    */
  private def mkScHash(seed: String): Hash = Hash(seed.padTo(64, '0').take(64))

  private def mkScHashForOrd(mg: Address, ord: Long): Hash =
    mkScHash(s"sc-${mg.value.value.take(6)}-$ord")

  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  /** Minimal in-memory SnapshotStorage stub — the chain-walk impl reads only from `chainStore.get(hash)` (in-memory `byHash`), never from
    * disk-backed storage. This stub is the safe default; tests that need a populated disk for the placeholder-GSI test wire `diskBackedStorage`
    * directly.
    */
  private def stubStorage: SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] =
    new SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {
      def prepend(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Boolean] = IO.pure(true)
      def head: IO[Option[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] = IO.pure(None)
      def headSnapshot: IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(None)
      def get(ordinal: SnapshotOrdinal): IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(None)
      def getHashed(ordinal: SnapshotOrdinal)(
        implicit hasher: Hasher[IO]
      ): IO[Option[Hashed[GlobalIncrementalSnapshot]]] = IO.pure(None)
      def get(hash: Hash): IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(None)
      def getHash(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[IO]): IO[Option[Hash]] = IO.pure(None)
      def setHeadForRecovery(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
      def setTentativeHead(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
      def confirmHead(hash: Hash): IO[Unit] = IO.unit
      def pruneTentative(finalizedOrdinal: SnapshotOrdinal): IO[Unit] = IO.unit
      def writeForBackfill(snapshot: Signed[GlobalIncrementalSnapshot])(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
    }

  /** In-memory disk-simulator that returns whatever the tests pre-populate. Mirrors the pattern from `NakamotoChainStoreSuite.diskBackedStorage`
    * — needed by the placeholder-GSI test to demonstrate the disk-fallback path produces an empty GSI that the walk handles safely.
    */
  private def diskBackedStorage(
    disk: Ref[IO, Map[SnapshotOrdinal, Signed[GlobalIncrementalSnapshot]]],
    diskByHash: Ref[IO, Map[Hash, Signed[GlobalIncrementalSnapshot]]]
  ): SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] =
    new SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {
      def prepend(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Boolean] = IO.pure(true)
      def head: IO[Option[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] = IO.pure(None)
      def headSnapshot: IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(None)
      def get(ordinal: SnapshotOrdinal): IO[Option[Signed[GlobalIncrementalSnapshot]]] =
        disk.get.map(_.get(ordinal))
      def getHashed(ordinal: SnapshotOrdinal)(
        implicit hasher: Hasher[IO]
      ): IO[Option[Hashed[GlobalIncrementalSnapshot]]] =
        disk.get.flatMap(_.get(ordinal).traverse(_.toHashed[IO]))
      def get(hash: Hash): IO[Option[Signed[GlobalIncrementalSnapshot]]] =
        diskByHash.get.map(_.get(hash))
      def getHash(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[IO]): IO[Option[Hash]] =
        disk.get.flatMap(_.get(ordinal).traverse(_.toHashed[IO].map(_.hash)))
      def setHeadForRecovery(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
      def setTentativeHead(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
      def confirmHead(hash: Hash): IO[Unit] = IO.unit
      def pruneTentative(finalizedOrdinal: SnapshotOrdinal): IO[Unit] = IO.unit
      def writeForBackfill(snapshot: Signed[GlobalIncrementalSnapshot])(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
    }

  // -------------- chain-store factories --------------

  /** Build a real `NakamotoChainStore.make` over `stubStorage`. The chain-walk impl reads only from in-memory `byHash`, so a stub-storage is
    * the appropriate disk shape for most tests. Returns the chain store plus the finalized-ordinal Ref so tests can drive `finalize`.
    */
  private def mkChainStore(
    keepDepthBehindFinalized: Long = NakamotoChainStore.DefaultKeepDepthBehindFinalized
  )(
    implicit hs: HasherSelector[IO]
  ): IO[(NakamotoChainStore.NakamotoChainStoreAlgebra[IO], Ref[IO, SnapshotOrdinal])] =
    for {
      stakeRegistry <- StakeRegistry.equalWeight[IO]
      _ <- stakeRegistry.updateValidators(Set(pid("self")))
      tipTracker <- TipTracker.make[IO](stakeRegistry)
      finalizedRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue)
      chainSelection = ChainSelection.make[IO](tipTracker, _ => IO.pure(None))
      chainStore <- NakamotoChainStore.make[IO](stubStorage, chainSelection, tipTracker, finalizedRef, keepDepthBehindFinalized)
    } yield (chainStore, finalizedRef)

  /** Variant with disk-backed storage so the placeholder-GSI test can engage `getWithOrdinalFallback`. */
  private def mkChainStoreWithDisk(
    keepDepthBehindFinalized: Long
  )(
    implicit hs: HasherSelector[IO]
  ): IO[
    (
      NakamotoChainStore.NakamotoChainStoreAlgebra[IO],
      Ref[IO, SnapshotOrdinal],
      Ref[IO, Map[SnapshotOrdinal, Signed[GlobalIncrementalSnapshot]]],
      Ref[IO, Map[Hash, Signed[GlobalIncrementalSnapshot]]]
    )
  ] =
    for {
      stakeRegistry <- StakeRegistry.equalWeight[IO]
      _ <- stakeRegistry.updateValidators(Set(pid("self")))
      tipTracker <- TipTracker.make[IO](stakeRegistry)
      finalizedRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue)
      diskRef <- Ref.of[IO, Map[SnapshotOrdinal, Signed[GlobalIncrementalSnapshot]]](Map.empty)
      diskByHashRef <- Ref.of[IO, Map[Hash, Signed[GlobalIncrementalSnapshot]]](Map.empty)
      storage = diskBackedStorage(diskRef, diskByHashRef)
      chainSelection = ChainSelection.make[IO](tipTracker, _ => IO.pure(None))
      chainStore <- NakamotoChainStore.make[IO](storage, chainSelection, tipTracker, finalizedRef, keepDepthBehindFinalized)
    } yield (chainStore, finalizedRef, diskRef, diskByHashRef)

  // -------------- GSI + snapshot construction --------------

  /** Build a `GlobalSnapshotInfo` whose `lastStateChannelSnapshotHashes` and `lastCurrencySnapshots` reflect the per-metagraph (sc-hash,
    * mg-ordinal) pairs the caller supplies. The chain-walk's two GSI fields are populated; every other field is empty / None / SortedMap.empty.
    *
    * Each `(mg, scHash, mgOrd)` entry produces:
    *   - `lastStateChannelSnapshotHashes(mg) = scHash`
    *   - `lastCurrencySnapshots(mg) = Right(mkSignedIncrementalSnapshot(mgOrd), mkCurrencySnapshotInfo)` — picked up by `extractMetagraphOrdinal`
    *     via the Right branch.
    */
  private def mkGsiWithMgEntries(entries: List[(Address, Hash, Long)]): GlobalSnapshotInfo = {
    val scHashes: SortedMap[Address, Hash] = SortedMap.from(entries.map { case (mg, h, _) => mg -> h })
    val currencySnapshots: SortedMap[
      Address,
      Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]
    ] = SortedMap.from(entries.map {
      case (mg, _, ord) => mg -> Right((mkSignedIncrementalSnapshot(ord), mkCurrencySnapshotInfo))
    })
    GlobalSnapshotInfo(
      lastStateChannelSnapshotHashes = scHashes,
      lastTxRefs = SortedMap.empty,
      balances = SortedMap.empty,
      lastCurrencySnapshots = currencySnapshots,
      lastCurrencySnapshotsProofs = SortedMap.empty,
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
      metagraphSyncData = None,
      historicalStakeSnapshots = SortedMap.empty
    )
  }

  /** Minimal `Signed[CurrencyIncrementalSnapshot]` exposing only the ordinal — `extractMetagraphOrdinal` reads
    * `.value.ordinal.value.value`, so everything else stays at sensible defaults. (Mirrors the pattern in
    * `MetagraphParentOrdinalResolverSuite.mkSignedIncrementalSnapshot`.)
    */
  private def mkSignedIncrementalSnapshot(ord: Long): Signed[CurrencyIncrementalSnapshot] = {
    val testProofs = cats.data.NonEmptySet.one(
      io.constellationnetwork.security.signature.signature.SignatureProof(
        io.constellationnetwork.schema.ID.Id(Hex("")),
        io.constellationnetwork.security.signature.signature.Signature(Hex(""))
      )
    )
    val inc = CurrencyIncrementalSnapshot(
      ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(ord)),
      height = Height.MinValue,
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      rewards = SortedSet.empty[RewardTransaction],
      tips = io.constellationnetwork.schema.SnapshotTips(SortedSet.empty, SortedSet.empty),
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
      version = io.constellationnetwork.schema.semver.SnapshotVersion("0.0.1")
    )
    Signed(inc, testProofs)
  }

  private def mkCurrencySnapshotInfo: CurrencySnapshotInfo =
    CurrencySnapshotInfo(SortedMap.empty, SortedMap.empty, None, None, None, None, None, None, None)

  /** Build a unique `Signed[GlobalSnapshot]` parameterized by `epoch` — distinct epochs give distinct snapshot hashes (mirrors the
    * `NakamotoChainStoreSuite.mkSnapshotWithEpoch` pattern). Returns the underlying signed incremental snapshot only — the caller pairs it with
    * a custom GSI built via `mkGsiWithMgEntries`, so the genesis-info's GSI is discarded.
    */
  private def mkSignedIncrementalGlobalSnapshot(epoch: Long)(
    implicit H: Hasher[IO],
    S: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[Signed[GlobalIncrementalSnapshot]] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { keyPair =>
      Signed
        .forAsyncHasher[IO, GlobalSnapshot](
          GlobalSnapshot.mkGenesis(Map.empty, EpochProgress(NonNegLong.unsafeFrom(epoch))),
          keyPair
        )
        .flatMap { genesis =>
          GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](genesis).flatMap { incremental =>
            Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](incremental, keyPair)
          }
        }
    }

  /** Seed `chainStore` with `n` parent-linked snapshots at ordinals 1..n. Each snapshot's `GlobalSnapshotInfo.lastStateChannelSnapshotHashes`
    * is populated per the `gsiFor` callback: `gsiFor(ord)` returns the list of `(mg, sc-hash, mg-ord)` tuples to encode into that snapshot's
    * GSI. Returns the list of stored gl0 snapshot hashes (indexed by `ord - 1`).
    *
    * Parent-linking: each snapshot's `parentHash` is the previous ord's gl0 snapshot hash; ord=1's parent is `Hash.empty`.
    */
  private def seedChain(
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[IO],
    n: Int,
    gsiFor: Long => List[(Address, Hash, Long)]
  )(
    implicit H: Hasher[IO],
    S: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[List[Hash]] =
    (1 to n).toList
      .foldLeftM[IO, (List[Hash], Hash)]((List.empty[Hash], Hash.empty)) {
        case ((acc, parent), ord) =>
          mkSignedIncrementalGlobalSnapshot(epoch = ord.toLong).flatMap { signed =>
            signed.toHashed[IO].flatMap { hashed =>
              val gsi = mkGsiWithMgEntries(gsiFor(ord.toLong))
              chainStore
                .store(signed, gsi, ordinal = ord.toLong, slot = ord.toLong, parentHash = parent, vrfOutput = Array.empty)
                .as((acc :+ hashed.hash, hashed.hash))
            }
          }
      }
      .map(_._1)

  // -------------- tests --------------

  test("(1) bestTip → parent → parent walk finds target at depth 5") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    // Seed a 10-snapshot chain. Each snapshot's GSI records (mgA, mkScHashForOrd(mgA, ord), ord) — i.e. the
    // metagraph's "tip" at gl0-ord N is the SC hash whose seed embeds N. The walk from bestTip (ord=10)
    // back to ord=5 should match on the ord=5 GSI entry.
    val targetOrd = 5L
    val targetScHash = mkScHashForOrd(mgA, targetOrd)
    for {
      cs <- mkChainStore()
      (chainStore, _) = cs
      _ <- seedChain(chainStore, n = 10, gsiFor = ord => List((mgA, mkScHashForOrd(mgA, ord), ord)))
      lookup = ChainStoreScHashHistoryLookup.make[IO](chainStore, maxDepth = 20)
      resolved <- lookup.lookup(mgA, targetScHash)
    } yield expect.eql(Some(targetOrd), resolved)
  }

  test("(2) maxDepth exceeded — walk gives up cleanly without finding target") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    // Chain of 30 snapshots; target is at ord=1 (29 hops back from bestTip=30). maxDepth=5 ⇒ walk should
    // return None after exhausting 5 hops without matching.
    val targetOrd = 1L
    val targetScHash = mkScHashForOrd(mgA, targetOrd)
    for {
      cs <- mkChainStore()
      (chainStore, _) = cs
      _ <- seedChain(chainStore, n = 30, gsiFor = ord => List((mgA, mkScHashForOrd(mgA, ord), ord)))
      lookup = ChainStoreScHashHistoryLookup.make[IO](chainStore, maxDepth = 5)
      resolved <- lookup.lookup(mgA, targetScHash)
    } yield expect.eql(None, resolved)
  }

  test("(3) parent miss (eviction boundary) — walk short-circuits cleanly, no exception") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    // Seed 15 snapshots, then finalize at ord=15 with keepDepth=5 → keepFloor=10, ords 1..9 evicted. The
    // walk from bestTip=15 visits the in-memory keep-window (ords 10..15), then hits a miss on
    // `chainStore.get(ord=10's parentHash)` and short-circuits to None. Target is at the evicted ord=2.
    val targetOrd = 2L
    val targetScHash = mkScHashForOrd(mgA, targetOrd)
    for {
      cs <- mkChainStore(keepDepthBehindFinalized = 5L)
      (chainStore, _) = cs
      hashes <- seedChain(chainStore, n = 15, gsiFor = ord => List((mgA, mkScHashForOrd(mgA, ord), ord)))
      _ <- chainStore.finalize(hashes.last, ordinal = 15L)
      // Confirm eviction took effect — ord 2 absent in-memory.
      ord2Absent <- chainStore.get(hashes(1)).map(_.isEmpty)
      ord10Present <- chainStore.get(hashes(9)).map(_.isDefined)
      lookup = ChainStoreScHashHistoryLookup.make[IO](chainStore, maxDepth = 100)
      // maxDepth high enough that without eviction we'd reach ord 2. With eviction, walk terminates at
      // the keepFloor and returns None — verifying the walk handles `chainStore.get → None` gracefully.
      resolved <- lookup.lookup(mgA, targetScHash)
    } yield
      expect.all(
        ord2Absent,
        ord10Present,
        resolved.isEmpty
      )
  }

  test("(4) multi-metagraph isolation against real chain — walk past wrong-metagraph entry") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    // Seed a 10-snapshot chain whose ord=5 GSI records BOTH (mgA, hashA5) AND (mgB, hashB5). Querying for
    // (mgA, hashB5) — the right hash, wrong metagraph — must NOT match at ord=5 (the resolver keys on
    // mgA's tip entry; the GSI value `lastStateChannelSnapshotHashes(mgA) = hashA5` ≠ hashB5). The walk
    // continues past ord=5 and finds no match (hashB5 isn't recorded for mgA anywhere in the chain).
    val hashA5 = mkScHashForOrd(mgA, 5L)
    val hashB5 = mkScHashForOrd(mgB, 5L)
    for {
      cs <- mkChainStore()
      (chainStore, _) = cs
      _ <- seedChain(
        chainStore,
        n = 10,
        gsiFor = ord => {
          if (ord == 5L) List((mgA, hashA5, 5L), (mgB, hashB5, 5L))
          else List.empty
        }
      )
      lookup = ChainStoreScHashHistoryLookup.make[IO](chainStore, maxDepth = 20)
      // Query mgA for hashB5 — must NOT cross-resolve to mgB's ordinal.
      resolved <- lookup.lookup(mgA, hashB5)
    } yield expect.eql(None, resolved)
  }

  test("(5) concurrent finalize during walk — deterministic result, no exception") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    // Best-effort safety: each `chainStore.bestTip`/`chainStore.get` reads via `stateRef.get`, which under
    // cats-effect `Ref` returns a CONSISTENT atomic snapshot of the byHash map for THAT call. If finalize
    // prunes between two consecutive get() calls, the second call sees the post-prune state and may return
    // None — in which case `stepParent` returns None cleanly. We verify two invariants:
    //   - the walk does NOT throw under any concurrent finalize timing (no NoSuchElementException, no
    //     null deref, no stale-state mix)
    //   - the result is in {Some(targetOrd), None} — i.e. a deterministic value, not garbage
    //
    // Setup: chain of 15 with mgA recording (mgA, hashForOrd) at each ord; target is ord 5 (10 hops back).
    val targetOrd = 5L
    val targetScHash = mkScHashForOrd(mgA, targetOrd)
    for {
      cs <- mkChainStore(keepDepthBehindFinalized = 3L)
      (chainStore, _) = cs
      hashes <- seedChain(chainStore, n = 15, gsiFor = ord => List((mgA, mkScHashForOrd(mgA, ord), ord)))
      lookup = ChainStoreScHashHistoryLookup.make[IO](chainStore, maxDepth = 30)
      // Race the walk against a finalize that evicts most of the chain. With keepDepth=3 + finalize at
      // ord=15, only ords 12..15 survive — ord 5 will likely be evicted before the walk reaches it.
      // Acceptable outcomes:
      //   - Walk completed before finalize: Some(5L)
      //   - Finalize pruned ord 5's parent chain mid-walk: None (the walk hits chainStore.get → None
      //     once the eviction has cleared the byHash entry)
      // Either way the result is deterministic w.r.t. its own observed state; no exception.
      raceResult <- IO.both(
        lookup.lookup(mgA, targetScHash).attempt,
        chainStore.finalize(hashes.last, ordinal = 15L).attempt
      )
      (walkResult, finalizeResult) = raceResult
    } yield
      expect.all(
        // Walk completed (no exception thrown).
        walkResult.isRight,
        // Finalize completed.
        finalizeResult.isRight,
        // Walk result is in the acceptable set {Some(targetOrd), None} — no garbage.
        walkResult.toOption.exists(r => r.isEmpty || r.contains(targetOrd))
      )
  }

  test("(6) disk-fallback placeholder GSI doesn't trip walk — evicted-zone returns None, not false positive") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    // The walk's `stepParent` uses `chainStore.get(parentHash)` (in-memory only) — NOT
    // `getWithOrdinalFallback`. So the disk-fallback path's empty-GSI placeholder doesn't enter the walk.
    // This test pins that behaviour: even with a populated disk holding evicted ords (via `getWithOrdinalFallback`-aware
    // mirroring), the walk's in-memory `get` misses, terminates at the keep-window, and returns None.
    //
    // Spuriously matching against placeholder GSI would be unsafe: the placeholder has `lastStateChannelSnapshotHashes =
    // SortedMap.empty`, so `gsi.lastStateChannelSnapshotHashes.get(mg) = None` — even on the "match" pathway
    // the resolver returns no ordinal. But if some future change routed `stepParent` through `getWithOrdinalFallback`,
    // the placeholder GSI could still trip behaviour by extending the walk past the consensus-safe horizon. This test
    // is a regression-prevention pin for that direction of change.
    //
    // We use a high maxDepth (30) — would walk past the eviction floor if disk-fallback were engaged.
    val targetOrd = 3L
    val targetScHash = mkScHashForOrd(mgA, targetOrd)
    for {
      r <- mkChainStoreWithDisk(keepDepthBehindFinalized = 5L)
      (chainStore, _, diskRef, diskByHashRef) = r
      // Seed 20 snapshots, mirror every one to disk (so placeholder-GSI fallback IS engageable if
      // anything starts using it).
      hashes <- (1 to 20).toList
        .foldLeftM[IO, (List[Hash], Hash)]((List.empty[Hash], Hash.empty)) {
          case ((acc, parent), ord) =>
            mkSignedIncrementalGlobalSnapshot(epoch = ord.toLong).flatMap { signed =>
              signed.toHashed[IO].flatMap { hashed =>
                val gsi = mkGsiWithMgEntries(List((mgA, mkScHashForOrd(mgA, ord.toLong), ord.toLong)))
                chainStore
                  .store(signed, gsi, ordinal = ord.toLong, slot = ord.toLong, parentHash = parent, vrfOutput = Array.empty) >>
                  diskRef.update(_.updated(SnapshotOrdinal(NonNegLong.unsafeFrom(ord.toLong)), signed)) >>
                  diskByHashRef.update(_.updated(hashed.hash, signed)).as((acc :+ hashed.hash, hashed.hash))
              }
            }
        }
        .map(_._1)
      // Finalize at ord=20 with keepDepth=5 ⇒ keepFloor=15. Ords 1..14 evicted from byHash; ords 15..20 retained.
      _ <- chainStore.finalize(hashes.last, ordinal = 20L)
      // Sanity-check eviction state.
      ord3Absent <- chainStore.get(hashes(2)).map(_.isEmpty)
      ord15Present <- chainStore.get(hashes(14)).map(_.isDefined)
      // ord 3 IS on disk (we mirrored everything), so `getWithOrdinalFallback` would return a disk record
      // with placeholder GSI. Verify we get a non-empty disk fallback (the placeholder GSI is engaged).
      ord3DiskFallback <- chainStore.getWithOrdinalFallback(hashes(2), expectedOrdinal = 3L)
      // The walk: from bestTip=20, walks back through in-memory ords 20..15 (the keep-window). At ord=15
      // the walk attempts `chainStore.get(parent=ord14)` which returns None (ord 14 evicted). Walk terminates
      // with None even though ord 3 IS reachable via disk-fallback if the walk used that path.
      lookup = ChainStoreScHashHistoryLookup.make[IO](chainStore, maxDepth = 30)
      resolved <- lookup.lookup(mgA, targetScHash)
    } yield
      expect.all(
        ord3Absent, // in-memory miss
        ord15Present, // kept window
        ord3DiskFallback.isDefined, // disk fallback DOES find ord 3 (verifies the test setup is non-trivial)
        // Critical assertion: even with ord 3 on disk + a high maxDepth, the walk does NOT match it because
        // `stepParent` uses `chainStore.get` (in-memory), not `getWithOrdinalFallback`. Walk terminates at
        // the keepFloor cleanly with None — no spurious match against the placeholder GSI's empty
        // lastStateChannelSnapshotHashes.
        resolved.isEmpty
      )
  }

  test("(7) empty chain store / pre-bootstrap — bestTip None returns None, no exception") { res =>
    val (_, _, _, h, _) = res
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    // Fresh chain store — no `store` calls. `chainStore.bestTip` returns None. The walk MUST short-circuit
    // with None per `ChainStoreScHashHistoryLookup.lookup`'s `case None =>` branch (lines 56-59), not throw.
    for {
      cs <- mkChainStore()
      (chainStore, _) = cs
      preBestTip <- chainStore.bestTip
      lookup = ChainStoreScHashHistoryLookup.make[IO](chainStore, maxDepth = 20)
      resolved <- lookup.lookup(mgA, mkScHash("any-hash")).attempt
    } yield
      expect.all(
        preBestTip.isEmpty,
        resolved.isRight, // no exception
        resolved == Right(None)
      )
  }
}
