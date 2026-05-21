package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.std.Supervisor
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.metrics.NoOpMetrics
import io.constellationnetwork.node.shared.nodeSharedKryoRegistrar
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for `NakamotoChainStore` divergent-self-finalize signal + `unsafe_clearFinality` (P-11, task #141).
  *
  * The store also has rich logic for fork-aware chain storage (ChainSelection-driven reorgs, finalize pruning, etc.) but those paths are
  * exercised through `NakamotoSyncDaemon` integration sites; this suite focuses on the new P-11 surface.
  */
object NakamotoChainStoreSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  // Required implicit for `GlobalIncrementalSnapshot.fromGlobalSnapshot`.
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  // Minimal Metrics for the chain store + (transitively, downstream) the orchestrator. Tests
  // don't assert counters; behavior tests live below.
  implicit val metrics: io.constellationnetwork.node.shared.infrastructure.metrics.Metrics[IO] = NoOpMetrics.make

  type Res = (Supervisor[IO], KryoSerializer[IO], JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  def sharedResource: cats.effect.Resource[IO, Res] = for {
    supervisor <- Supervisor[IO]
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](nodeSharedKryoRegistrar)
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (supervisor, ks, j, h, sp)

  // Minimal in-memory SnapshotStorage stub. The chain store calls only `prepend`,
  // `setHeadForRecovery`, and `getHash` (the latter only during `walkBackTo` fallback).
  // Returns no-op success for writes; reads return None. Good enough for the P-11 surface,
  // which doesn't depend on file-system persistence.
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

  // Test fixture: NakamotoChainStore + the finalized-ordinal Ref it gates against.
  private def mkChainStore(
    keepDepthBehindFinalized: Long = NakamotoChainStore.DefaultKeepDepthBehindFinalized
  )(
    implicit hs: HasherSelector[IO]
  ): IO[
    (NakamotoChainStore.NakamotoChainStoreAlgebra[IO], Ref[IO, SnapshotOrdinal], TipTracker[IO], StakeRegistry[IO])
  ] =
    for {
      stakeRegistry <- StakeRegistry.equalWeight[IO]
      _ <- stakeRegistry.updateValidators(Set(pid("self")))
      tipTracker <- TipTracker.make[IO](stakeRegistry)
      finalizedRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue)
      // ChainSelection wants a fetchParent — for our P-11 surface tests we never trigger fork
      // selection, so a None-returning stub suffices.
      chainSelection = ChainSelection.make[IO](tipTracker, _ => IO.pure(None))
      chainStore <- NakamotoChainStore.make[IO](stubStorage, chainSelection, tipTracker, finalizedRef, keepDepthBehindFinalized)
    } yield (chainStore, finalizedRef, tipTracker, stakeRegistry)

  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  // Real signed snapshots are expensive to build (need genesis + key pairs + hash). For
  // the divergent-refuse-counter surface we just need to call `chainStore.store` with
  // sentinel parameters that drive the safety-gate path. The gate inspects `ordinal` and
  // `parentHash`; the signed-snapshot body is hashed lazily inside `store` and never
  // re-read by the gate logic. We construct minimal real snapshots via the standard
  // genesis fixture from SnapshotStorageSuite.
  private def mkGenesis(
    implicit H: Hasher[IO],
    S: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { keyPair =>
      Signed
        .forAsyncHasher[IO, GlobalSnapshot](
          GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue),
          keyPair
        )
        .flatMap { genesis =>
          GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](genesis).flatMap { incremental =>
            Signed
              .forAsyncHasher[IO, GlobalIncrementalSnapshot](incremental, keyPair)
              .map((_, genesis.info.toGlobalSnapshotInfo))
          }
        }
    }

  // Build a SECOND signed snapshot at the same ordinal=1 but with different content — this
  // simulates the "canonical chain has a different hash at the ordinal we already finalized"
  // scenario. We change a non-essential field (epoch progress) so the hash differs.
  private def mkAltSnapshot(
    implicit H: Hasher[IO],
    S: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)] =
    mkSnapshotWithEpoch(epoch = 1L)

  /** Build a signed snapshot whose content hash varies with `epoch`. Lets a single test build a sequence of `N` distinct-hash snapshots
    * without colliding on bytes. Heap-leak Fix B tests use this to seed `byHash` with N entries and then assert eviction below the
    * keep-window.
    */
  private def mkSnapshotWithEpoch(epoch: Long)(
    implicit H: Hasher[IO],
    S: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { keyPair =>
      Signed
        .forAsyncHasher[IO, GlobalSnapshot](
          GlobalSnapshot.mkGenesis(Map.empty, EpochProgress(NonNegLong.unsafeFrom(epoch))),
          keyPair
        )
        .flatMap { genesis =>
          GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](genesis).flatMap { incremental =>
            Signed
              .forAsyncHasher[IO, GlobalIncrementalSnapshot](incremental, keyPair)
              .map((_, genesis.info.toGlobalSnapshotInfo))
          }
        }
    }

  /** Seed a chain of `n` distinct-hash snapshots at ordinals 1..n into `chainStore`, parent-linked left-to-right. Returns the hash at each
    * ordinal. The actual chain-linking via `parentHash` isn't tied to any production semantics here — we just need `byHash` populated;
    * `chainStore`'s canonical-walk in `finalize` follows the parent chain so we link each new store to the prior.
    */
  private def seedChainOfLength(
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[IO],
    n: Int
  )(
    implicit H: Hasher[IO],
    S: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[List[Hash]] =
    (1 to n).toList
      .foldLeftM[IO, (List[Hash], Hash)]((List.empty[Hash], Hash.empty)) {
        case ((acc, parent), ord) =>
          mkSnapshotWithEpoch(epoch = ord.toLong).flatMap {
            case (signed, ctx) =>
              signed.toHashed[IO].flatMap { hashed =>
                chainStore
                  .store(signed, ctx, ordinal = ord.toLong, slot = ord.toLong, parentHash = parent, vrfOutput = Array.empty)
                  .as((acc :+ hashed.hash, hashed.hash))
              }
          }
      }
      .map(_._1)

  test("divergentRefuseCount starts at 0 and divergentRefuseSample is None") { res =>
    implicit val (_, _, _, h, _) = res
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, _, _, _) = r
      count <- chainStore.divergentRefuseCount
      sample <- chainStore.divergentRefuseSample
    } yield expect.same(0L, count) && expect.same(None, sample)
  }

  test("divergentRefuseCount increments when store refuses a different-hash at-or-below-finalized write") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, finalizedRef, _, _) = r
      pair1 <- mkGenesis
      (s1, ctx1) = pair1
      // Store it at ordinal=1 — this seeds byHash[h1.hash] -> ord=1.
      stored1 <- chainStore.store(s1, ctx1, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      // Simulate finalization: bump the finalized-ordinal Ref so the safety gate engages.
      _ <- finalizedRef.set(SnapshotOrdinal(NonNegLong(1L)))
      // Now try to store a DIFFERENT signed snapshot at the same ordinal — this is the
      // canonical chain trying to overwrite our local-finalized hash. The gate must refuse.
      pair2 <- mkAltSnapshot
      (s2, ctx2) = pair2
      stored2 <- chainStore.store(s2, ctx2, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      h2 <- s2.toHashed[IO]
      count <- chainStore.divergentRefuseCount
      sample <- chainStore.divergentRefuseSample
    } yield
      expect.all(
        stored1, // first store succeeded
        !stored2, // second store refused
        count == 1L, // counter incremented
        sample.contains((1L, h2.hash))
      )
  }

  test("divergentRefuseCount: same-hash re-delivery at-or-below-finalized does NOT increment") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, finalizedRef, _, _) = r
      pair <- mkGenesis
      (s1, ctx1) = pair
      _ <- chainStore.store(s1, ctx1, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      _ <- finalizedRef.set(SnapshotOrdinal(NonNegLong(1L)))
      // Re-deliver the SAME snapshot — should fall through to the tryStore path and be a duplicate.
      _ <- chainStore.store(s1, ctx1, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      count <- chainStore.divergentRefuseCount
    } yield expect.same(0L, count)
  }

  test("unsafe_clearFinality: clears byHash, bestTip, lastFinalizedOrdinal, refuse counter, refuse sample") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, finalizedRef, _, _) = r
      pair1 <- mkGenesis
      (s1, ctx1) = pair1
      _ <- chainStore.store(s1, ctx1, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      _ <- finalizedRef.set(SnapshotOrdinal(NonNegLong(1L)))
      pair2 <- mkAltSnapshot
      (s2, ctx2) = pair2
      _ <- chainStore.store(s2, ctx2, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      // Pre-reset: state has refuse count 1, sample set, finalized ord = 1, bestTip = h1.
      preCount <- chainStore.divergentRefuseCount
      preSample <- chainStore.divergentRefuseSample
      preBestTip <- chainStore.bestTip
      preChainSize <- chainStore.size
      preFinalizedOrd <- chainStore.lastFinalizedOrdinal
      // Reset.
      cleared <- chainStore.unsafe_clearFinality
      // Post-reset assertions.
      postCount <- chainStore.divergentRefuseCount
      postSample <- chainStore.divergentRefuseSample
      postBestTip <- chainStore.bestTip
      postChainSize <- chainStore.size
      postFinalizedOrd <- chainStore.lastFinalizedOrdinal
      postFinalizedRef <- finalizedRef.get
    } yield
      expect.all(
        // Pre-state sanity.
        preCount == 1L,
        preSample.isDefined,
        preBestTip.isDefined,
        preChainSize == 1,
        preFinalizedOrd == 0L, // finalize was never called — it's the finalizedRef that's bumped
        // Reset returned true since state was cleared.
        cleared,
        // Post-reset: all state zero / empty / None.
        postCount == 0L,
        postSample.isEmpty,
        postBestTip.isEmpty,
        postChainSize == 0,
        postFinalizedOrd == 0L,
        postFinalizedRef == SnapshotOrdinal.MinValue
      )
  }

  test("unsafe_clearFinality on empty store: returns false (no-op)") { res =>
    implicit val (_, _, _, h, _) = res
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, _, _, _) = r
      cleared <- chainStore.unsafe_clearFinality
    } yield expect.same(false, cleared)
  }

  test("unsafe_clearFinality: post-reset, a previously-refused canonical hash CAN now be stored") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, finalizedRef, _, _) = r
      // Seed local-finalized hash h1 at ord=1.
      pair1 <- mkGenesis
      (s1, ctx1) = pair1
      _ <- chainStore.store(s1, ctx1, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      _ <- finalizedRef.set(SnapshotOrdinal(NonNegLong(1L)))
      // Canonical h2 attempts to store — refused.
      pair2 <- mkAltSnapshot
      (s2, ctx2) = pair2
      preRefused <- chainStore.store(s2, ctx2, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      preCount <- chainStore.divergentRefuseCount
      // Re-bootstrap fires — call unsafe_clearFinality.
      _ <- chainStore.unsafe_clearFinality
      // Now retry storing h2 — should succeed because finalized is reset to MinValue.
      postStored <- chainStore.store(s2, ctx2, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      h2 <- s2.toHashed[IO]
      postBestTip <- chainStore.bestTip.map(_.map(_.hash))
    } yield
      expect.all(
        // Pre-reset: refused.
        !preRefused,
        preCount == 1L,
        // Post-reset: accepted.
        postStored,
        // The previously-refused canonical is now the bestTip (the core P-11 contract — the
        // reset unblocks the chain).
        postBestTip.contains(h2.hash)
      )
  }

  // ============================================================
  // Decision logic tests for the orchestrator's pure decide() function.
  // No I/O — exercises the threshold + cooldown logic directly.
  // ============================================================

  pureTest("RebootstrapOrchestrator.decide: below threshold returns Quiet") {
    val d = RebootstrapOrchestrator.decide(
      refuseCount = 2L,
      threshold = 3L,
      lastResetAtMs = None,
      nowMs = 1000L,
      cooldownMs = 5000L
    )
    expect.same(RebootstrapOrchestrator.Decision.Quiet, d)
  }

  pureTest("RebootstrapOrchestrator.decide: at threshold with no prior reset returns Trigger") {
    val d = RebootstrapOrchestrator.decide(
      refuseCount = 3L,
      threshold = 3L,
      lastResetAtMs = None,
      nowMs = 1000L,
      cooldownMs = 5000L
    )
    expect.same(RebootstrapOrchestrator.Decision.Trigger, d)
  }

  pureTest("RebootstrapOrchestrator.decide: at threshold within cooldown returns Cooldown") {
    val d = RebootstrapOrchestrator.decide(
      refuseCount = 10L,
      threshold = 3L,
      lastResetAtMs = Some(900L),
      nowMs = 1000L, // only 100ms ago
      cooldownMs = 5000L
    )
    expect.same(RebootstrapOrchestrator.Decision.Cooldown, d)
  }

  pureTest("RebootstrapOrchestrator.decide: at threshold past cooldown returns Trigger") {
    val d = RebootstrapOrchestrator.decide(
      refuseCount = 10L,
      threshold = 3L,
      lastResetAtMs = Some(900L),
      nowMs = 10000L, // 9.1s ago, past 5s cooldown
      cooldownMs = 5000L
    )
    expect.same(RebootstrapOrchestrator.Decision.Trigger, d)
  }

  // ============================================================
  // Heap-leak Fix B: byHash retention bounded by keepDepthBehindFinalized.
  // ============================================================

  test("Fix B: byHash retains all entries when chain length below keepDepthBehindFinalized") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      // keepDepth=10, chain length=5 (well below). No eviction should fire.
      r <- mkChainStore(keepDepthBehindFinalized = 10L)
      (chainStore, _, _, _) = r
      hashes <- seedChainOfLength(chainStore, n = 5)
      preSize <- chainStore.size
      // Finalize the tip — at ord=5, keepFloor = max(0, 5-10) = 0, so all canonical entries stay.
      _ <- chainStore.finalize(hashes.last, ordinal = 5L)
      postSize <- chainStore.size
      tipPresent <- chainStore.get(hashes.last).map(_.isDefined)
      genesisPresent <- chainStore.get(hashes.head).map(_.isDefined)
    } yield
      expect.all(
        preSize == 5,
        postSize == 5, // nothing evicted
        tipPresent,
        genesisPresent // genesis still in byHash because keepFloor=0
      )
  }

  test("Fix B: byHash evicts canonical entries below finalizedOrd - keepDepthBehindFinalized on finalize") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      // keepDepth=3, chain length=10. Finalize at ord=10 ⇒ keepFloor=7. Ords 1..6 evicted.
      r <- mkChainStore(keepDepthBehindFinalized = 3L)
      (chainStore, _, _, _) = r
      hashes <- seedChainOfLength(chainStore, n = 10)
      preSize <- chainStore.size
      _ <- chainStore.finalize(hashes.last, ordinal = 10L)
      postSize <- chainStore.size
      // Entries at ords 7..10 (the keep-window) should remain. 4 entries: 10, 9, 8, 7.
      tipPresent <- chainStore.get(hashes(9)).map(_.isDefined) // ord 10
      keepFloorPresent <- chainStore.get(hashes(6)).map(_.isDefined) // ord 7
      belowKeepFloorAbsent <- chainStore.get(hashes(5)).map(_.isEmpty) // ord 6
      genesisAbsent <- chainStore.get(hashes.head).map(_.isEmpty) // ord 1
    } yield
      expect.all(
        preSize == 10,
        postSize == 4, // 10, 9, 8, 7 retained
        tipPresent,
        keepFloorPresent,
        belowKeepFloorAbsent,
        genesisAbsent
      )
  }

  test("Fix B: subsequent finalize advances eviction floor (sliding window)") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore(keepDepthBehindFinalized = 3L)
      (chainStore, _, _, _) = r
      hashes <- seedChainOfLength(chainStore, n = 15)
      // First finalize at ord=10 ⇒ keepFloor=7; entries 7..15 retained (9 entries).
      _ <- chainStore.finalize(hashes(9), ordinal = 10L)
      sizeAfterFirst <- chainStore.size
      // Second finalize at ord=15 ⇒ keepFloor=12; entries 12..15 retained (4 entries).
      _ <- chainStore.finalize(hashes(14), ordinal = 15L)
      sizeAfterSecond <- chainStore.size
      tipPresent <- chainStore.get(hashes(14)).map(_.isDefined) // ord 15
      newFloorPresent <- chainStore.get(hashes(11)).map(_.isDefined) // ord 12
      belowNewFloorAbsent <- chainStore.get(hashes(10)).map(_.isEmpty) // ord 11
      priorFloorAbsent <- chainStore.get(hashes(6)).map(_.isEmpty) // ord 7
    } yield
      expect.all(
        sizeAfterFirst == 9, // 7..15
        sizeAfterSecond == 4, // 12..15
        tipPresent,
        newFloorPresent,
        belowNewFloorAbsent,
        priorFloorAbsent
      )
  }

  test("Fix B: keepDepthBehindFinalized > 0 with finalizedOrd <= keepDepth is a no-op (genesis bootstrap)") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      // Production-ish: keepDepth=255. Chain shorter than that — keepFloor stays at 0.
      r <- mkChainStore(keepDepthBehindFinalized = 255L)
      (chainStore, _, _, _) = r
      hashes <- seedChainOfLength(chainStore, n = 20)
      _ <- chainStore.finalize(hashes.last, ordinal = 20L)
      postSize <- chainStore.size
      genesisPresent <- chainStore.get(hashes.head).map(_.isDefined)
    } yield expect.all(postSize == 20, genesisPresent)
  }

  pureTest("Fix B: DefaultKeepDepthBehindFinalized is 255 (matches operational confirmation depth k₁)") {
    expect.same(255L, NakamotoChainStore.DefaultKeepDepthBehindFinalized)
  }
}
