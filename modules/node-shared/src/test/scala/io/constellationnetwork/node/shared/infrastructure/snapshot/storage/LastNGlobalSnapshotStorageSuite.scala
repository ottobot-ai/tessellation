package io.constellationnetwork.node.shared.infrastructure.snapshot.storage

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.LastGlobalSnapshotsSyncConfig
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastNGlobalSnapshotStorage
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.security.signature.{Signed, signature}
import io.constellationnetwork.security.{Hashed, Hasher}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import fs2.concurrent.SignallingRef
import weaver.MutableIOSuite

/** Unit suite for [[LastNGlobalSnapshotStorage.registerFinalized]] — the gl1 inclusion-proof follow deadlock-breaker.
  *
  * The deadlock (see `GlobalSnapshotAlignment.performSnapshotsBatchProcessing`): gl1's GSI comes from gl0's LATEST-finalized slice (ordinal
  * M = the pulled batch's tip), but `DAGSnapshotProcessor.applyGlobalSnapshotFn` resolves M's signed `stateProof` from the by-ordinal lastN
  * index via `getByOrdinal(M)`, and the per-ordinal `set` only seeds that index AFTER a successful apply — so while the FIRST batch ordinal
  * is processed, `getByOrdinal(M)` is None and the slice verify defers forever. `registerFinalized` seeds the index up-front from the
  * already-pulled finalized batch so `getByOrdinal(M)` resolves.
  *
  * Asserts the contract `applyGlobalSnapshotFn` relies on: (1) empty input is a no-op; (2) the by-ordinal index is populated
  * idempotently/order-independently so `getByOrdinal(tip)` resolves (the deadlock-breaker); (3) trimming to
  * `maxLastGlobalSnapshotsInMemory` ALWAYS retains the highest ordinal M (the verify always anchors to the slice tip); (4) the combined
  * `(snapshot, state)` pointer (`get`/`getLastN`/`getLatestBalances`, currency-consumed) is left UNTOUCHED.
  */
object LastNGlobalSnapshotStorageSuite extends MutableIOSuite {

  override type Res = Hasher[IO]

  override def sharedResource: Resource[IO, Res] =
    Resource.eval(JsonSerializer.forAsync[IO]).map { implicit js =>
      Hasher.forJson[IO]
    }

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  private def h(s: String): Hash =
    Hash(s.getBytes("UTF-8").map(b => f"${b & 0xff}%02x").mkString.padTo(64, '0').take(64))

  /** Minimal `Signed[GlobalIncrementalSnapshot]` — only `ordinal` is load-bearing for `registerFinalized` (index keying + trim). */
  private def syntheticSnapshot(ordinal: SnapshotOrdinal): Signed[GlobalIncrementalSnapshot] = {
    val unsigned = GlobalIncrementalSnapshot(
      ordinal = ordinal,
      height = Height(NonNegLong(0L)),
      subHeight = SubHeight(NonNegLong(0L)),
      lastSnapshotHash = h(s"parent-$ordinal"),
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      shardCheckpoints = SortedMap.empty,
      rewards = SortedSet.empty,
      delegateRewards = None,
      epochProgress = EpochProgress(NonNegLong(0L)),
      nextFacilitators = NonEmptyList.of(PeerId(Hex("0d" * 64))),
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = GlobalSnapshotStateProof(
        h("00"),
        h("00"),
        h("00"),
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
        Some(h("00")),
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
    Signed(
      unsigned,
      NonEmptySet.of(SignatureProof(PeerId(Hex("0d" * 64)).toId, signature.Signature(Hex("0e" * 64))))
    )
  }

  private def hashed(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[IO]): IO[Hashed[GlobalIncrementalSnapshot]] =
    syntheticSnapshot(ordinal).toHashed

  private def mkStorage(maxInMemory: Int)(implicit hasher: Hasher[IO]): IO[
    (
      LastNGlobalSnapshotStorage[IO],
      SignallingRef[IO, Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]],
      SignallingRef[IO, SortedMap[SnapshotOrdinal, Hashed[GlobalIncrementalSnapshot]]]
    )
  ] =
    for {
      combinedR <- SignallingRef.of[IO, Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]](None)
      incR <- SignallingRef.of[IO, SortedMap[SnapshotOrdinal, Hashed[GlobalIncrementalSnapshot]]](SortedMap.empty)
      storage = LastNGlobalSnapshotStorage.make[IO](
        LastGlobalSnapshotsSyncConfig(NonNegLong(2L), PosInt.unsafeFrom(maxInMemory)),
        combinedR,
        incR
      )
    } yield (storage, combinedR, incR)

  test("registerFinalized on an empty list is a no-op") { implicit hasher =>
    for {
      sc <- mkStorage(maxInMemory = 5)
      (storage, _, _) = sc
      _ <- storage.registerFinalized(List.empty)
      lastN <- storage.getLastN
    } yield expect(lastN.isEmpty)
  }

  test("registerFinalized seeds the by-ordinal index so getByOrdinal(tip) resolves (the deadlock-breaker)") { implicit hasher =>
    for {
      sc <- mkStorage(maxInMemory = 5)
      (storage, _, _) = sc
      // The pulled finalized batch (lastOrd, finalizedOrd] = (10, 13], ascending/contiguous as the real pull returns.
      batch <- List(11L, 12L, 13L).traverse(o => hashed(ord(o)))
      // Precondition: before seeding, the batch tip (M=13) is NOT resolvable (the deadlock — getByOrdinal(M) = None).
      tipBefore <- storage.getByOrdinal(ord(13))
      _ <- storage.registerFinalized(batch)
      // After seeding, every ordinal of the batch — crucially the tip M=13 the verify anchors to — resolves.
      tip <- storage.getByOrdinal(ord(13))
      mid <- storage.getByOrdinal(ord(12))
      bottom <- storage.getByOrdinal(ord(11))
    } yield
      expect(tipBefore.isEmpty) &&
        expect.same(
          (tip.map(_.ordinal), mid.map(_.ordinal), bottom.map(_.ordinal)),
          (ord(13).some, ord(12).some, ord(11).some)
        )
  }

  test("registerFinalized is idempotent by ordinal and order-independent") { implicit hasher =>
    for {
      sc <- mkStorage(maxInMemory = 5)
      (storage, _, incR) = sc
      batch <- List(11L, 12L, 13L).traverse(o => hashed(ord(o)))
      // Register out of order, with a duplicate — the resulting index is keyed by ordinal regardless.
      _ <- storage.registerFinalized(List(batch(2), batch(0), batch(1), batch(2)))
      afterFirst <- incR.get
      // Re-register the same batch — no corruption, no growth, no error.
      _ <- storage.registerFinalized(batch)
      afterSecond <- incR.get
    } yield
      expect.same(afterFirst.keySet, Set(ord(11), ord(12), ord(13))) &&
        expect.same(afterFirst.keySet, afterSecond.keySet)
  }

  test("registerFinalized trims to maxLastGlobalSnapshotsInMemory and ALWAYS keeps the highest ordinal (the slice tip)") {
    implicit hasher =>
      for {
        sc <- mkStorage(maxInMemory = 2)
        (storage, _, incR) = sc
        // A large batch — the verify anchors to the TIP (M = 15); the trim must never drop it.
        batch <- (11L to 15L).toList.traverse(o => hashed(ord(o)))
        _ <- storage.registerFinalized(batch)
        idx <- incR.get
        tip <- storage.getByOrdinal(ord(15))
      } yield
        // Trimmed to the 2 highest ordinals; the tip (M) survives so getByOrdinal(M) resolves.
        expect.same(idx.keySet, Set(ord(14), ord(15))) &&
          expect.same(tip.map(_.ordinal), ord(15).some)
  }

  test("registerFinalized does NOT touch the combined (snapshot, state) chain pointer / getLastN window") { implicit hasher =>
    for {
      sc <- mkStorage(maxInMemory = 5)
      (storage, combinedR, _) = sc
      batch <- List(11L, 12L, 13L).traverse(o => hashed(ord(o)))
      _ <- storage.registerFinalized(batch)
      // The combined pointer (source of get / getLatestBalances and the currency-consumed contiguous getLastN window) is
      // advanced ONLY by `set`/`setForRecovery`, never by `registerFinalized`.
      combinedAfter <- combinedR.get
      headAfter <- storage.get
    } yield expect(combinedAfter.isEmpty) && expect(headAfter.isEmpty)
  }
}
