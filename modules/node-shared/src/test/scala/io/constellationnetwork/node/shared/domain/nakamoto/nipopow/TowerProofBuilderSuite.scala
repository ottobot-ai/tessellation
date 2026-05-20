package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.security.signature.{Signed, signature}
import io.constellationnetwork.security.{Hashed, Hasher}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

/** §3 NIPoPoW S4.2 — [[TowerProofBuilder]] suite.
  *
  * Builds proofs against synthetic tower stores + dummy snapshot storage; asserts:
  *   - L0 suffix length honored.
  *   - Per-level chain materialized from tower entries.
  *   - Empty store and tip-only edge cases.
  *   - `since` parameter filters lower-bound.
  */
object TowerProofBuilderSuite extends SimpleIOSuite {

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  /** Lowercase-hex 64-char hash from a short label. */
  private def h(s: String): Hash =
    Hash(s.getBytes("UTF-8").map(b => f"${b & 0xff}%02x").mkString.padTo(64, '0').take(64))

  private def trial(level: Int, passed: Boolean): LevelTrial =
    LevelTrial(level, Ratio.Zero, Ratio.Zero, passed)

  private def trials(levels: Set[Int]): Vector[LevelTrial] =
    (1 to SuperLevelParams.SuperLevelCount).toVector.map(l => trial(l, levels.contains(l)))

  /** Build a synthetic `Signed[GlobalIncrementalSnapshot]` with the minimum fields the builder reads. The slotCertificate is the
    * load-bearing field; everything else is filler-but-typed.
    */
  private def syntheticSnapshot(
    ordinal: SnapshotOrdinal,
    slot: Long,
    parentSlot: Long,
    parentHash: Hash,
    subchainLevelCounts: Vector[Long] = SlotCertificate.ZeroSubchainLevelCounts
  ): Signed[GlobalIncrementalSnapshot] = {
    val cert = SlotCertificate(
      slot = Slot.unsafeApply(slot),
      parentSlot = Slot.unsafeApply(parentSlot),
      vrfProof = VrfProof(Hex("0a" * 80)),
      vrfOutput = VrfOutput(Hex("0b" * 64)),
      vrfPublicKey = VrfPublicKey(Hex("0c" * 32)),
      eta = h(s"eta-$ordinal"),
      activePoolSize = 8,
      activePoolHash = h(s"pool-$ordinal"),
      subchainLevelCounts = subchainLevelCounts
    )
    val unsigned = GlobalIncrementalSnapshot(
      ordinal = ordinal,
      height = Height(NonNegLong(0L)),
      subHeight = SubHeight(NonNegLong(0L)),
      lastSnapshotHash = parentHash,
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
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
      slotCertificate = Some(cert),
      eta = Some(h(s"eta-$ordinal"))
    )
    Signed(
      unsigned,
      NonEmptySet.of(
        SignatureProof(
          PeerId(Hex("0d" * 64)).toId,
          signature.Signature(Hex("0e" * 64))
        )
      )
    )
  }

  /** In-memory dummy SnapshotStorage with `get(ordinal)` + `head` populated; all other methods unused. */
  private def dummyStorage(
    snapshots: Map[SnapshotOrdinal, Signed[GlobalIncrementalSnapshot]],
    headOpt: Option[Signed[GlobalIncrementalSnapshot]]
  ): SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] =
    new SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {
      override def prepend(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Boolean] = IO.pure(true)
      override def head: IO[Option[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] =
        IO.pure(headOpt.map(s => s -> dummyGsi))
      override def headSnapshot: IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(headOpt)
      override def get(ordinal: SnapshotOrdinal): IO[Option[Signed[GlobalIncrementalSnapshot]]] =
        IO.pure(snapshots.get(ordinal))
      override def getHashed(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[IO]): IO[Option[Hashed[GlobalIncrementalSnapshot]]] =
        IO.raiseError(new NotImplementedError("getHashed unused by TowerProofBuilder"))
      override def get(hash: Hash): IO[Option[Signed[GlobalIncrementalSnapshot]]] =
        IO.pure(snapshots.values.find(_.value.lastSnapshotHash == hash))
      override def getHash(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[IO]): IO[Option[Hash]] = IO.pure(None)
      override def setHeadForRecovery(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
      override def setTentativeHead(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
      override def confirmHead(hash: Hash): IO[Unit] = IO.unit
      override def pruneTentative(finalizedOrdinal: SnapshotOrdinal): IO[Unit] = IO.unit
      override def writeForBackfill(snapshot: Signed[GlobalIncrementalSnapshot])(implicit hasher: Hasher[IO]): IO[Unit] = IO.unit
    }

  /** Constructed once — every test that needs a `head` returns this `GlobalSnapshotInfo`. The builder ignores the state component, so a
    * shared sentinel is fine.
    */
  private def dummyGsi: GlobalSnapshotInfo =
    GlobalSnapshotInfo.empty

  /** Build a chain of n+1 snapshots ordinal 0..n. Each snapshot's `lastSnapshotHash = h(s"snap-$prevOrdinal")`. */
  private def chain(n: Int): Map[SnapshotOrdinal, Signed[GlobalIncrementalSnapshot]] = {
    (0 to n).map { i =>
      val prev = if (i == 0) h("genesis") else h(s"snap-${i - 1}")
      ord(i.toLong) -> syntheticSnapshot(ord(i.toLong), slot = (i * 10L) + 1L, parentSlot = math.max(0L, i * 10L - 9L), parentHash = prev)
    }.toMap
  }

  /** Build the chain map plus head. Head is the snapshot at ordinal `n`. */
  private def chainWithHead(n: Int): (Map[SnapshotOrdinal, Signed[GlobalIncrementalSnapshot]], Signed[GlobalIncrementalSnapshot]) = {
    val m = chain(n)
    (m, m(ord(n.toLong)))
  }

  test("empty storage — builder returns canonical empty proof") {
    for {
      tower <- TowerStore.inMemory[IO]
      builder = TowerProofBuilder.make[IO](tower, dummyStorage(Map.empty, None))
      proof <- builder.buildFromGenesis
    } yield expect(proof == TowerProof.Empty)
  }

  test("L0 suffix length honored — k=5 returns most-recent 5 headers in ascending order") {
    val (snaps, headSnap) = chainWithHead(10)
    for {
      tower <- TowerStore.inMemory[IO]
      builder = TowerProofBuilder.make[IO](tower, dummyStorage(snaps, Some(headSnap)))
      proof <- builder.build(SnapshotOrdinal.MinValue, k = 5)
    } yield
      expect(proof.level0Suffix.size == 5)
        .and(expect(proof.level0Suffix.map(_.ordinal.value.value) == Vector(6L, 7L, 8L, 9L, 10L)))
        .and(expect(proof.tipOrdinal == ord(10L)))
  }

  test("L0 suffix shorter than k when tip < k — suffix is whatever's available") {
    val (snaps, headSnap) = chainWithHead(3)
    for {
      tower <- TowerStore.inMemory[IO]
      builder = TowerProofBuilder.make[IO](tower, dummyStorage(snaps, Some(headSnap)))
      proof <- builder.build(SnapshotOrdinal.MinValue, k = 10)
    } yield
      expect(proof.level0Suffix.size == 4) // ordinals 0..3
        .and(expect(proof.level0Suffix.map(_.ordinal.value.value) == Vector(0L, 1L, 2L, 3L)))
  }

  test("level-µ chain materialized from tower entries — L1 hit at ord=5 surfaces in proof") {
    val (snaps, headSnap) = chainWithHead(10)
    for {
      tower <- TowerStore.inMemory[IO]
      _ <- tower.appendAtFinality(ord(5L), h("snap-5-content"), trials(Set(1, 3)))
      builder = TowerProofBuilder.make[IO](tower, dummyStorage(snaps, Some(headSnap)))
      proof <- builder.buildFromGenesis
    } yield
      expect(proof.levelChains.contains(1))
        .and(expect(proof.levelChains.contains(3)))
        .and(expect(!proof.levelChains.contains(2)))
        .and(expect(proof.levelChains(1).size == 1))
        .and(expect(proof.levelChains(1).head.ordinal == ord(5L)))
        .and(expect(proof.levelChains(1).head.snapshotHash == h("snap-5-content")))
  }

  test("`since` filters lower-bound — entries below since are excluded from levelChains") {
    val (snaps, headSnap) = chainWithHead(20)
    for {
      tower <- TowerStore.inMemory[IO]
      _ <- tower.appendAtFinality(ord(3L), h("snap-3-content"), trials(Set(1)))
      _ <- tower.appendAtFinality(ord(7L), h("snap-7-content"), trials(Set(1)))
      _ <- tower.appendAtFinality(ord(15L), h("snap-15-content"), trials(Set(1)))
      builder = TowerProofBuilder.make[IO](tower, dummyStorage(snaps, Some(headSnap)))
      proof <- builder.build(ord(7L), k = 3)
    } yield
      // since=7 inclusive: should include ord 7 and 15, exclude ord 3
      expect(proof.levelChains(1).map(_.ordinal.value.value) == Vector(7L, 15L))
        .and(expect(proof.since == ord(7L)))
  }

  test("missing snapshot in storage — header silently skipped, chain shorter") {
    val (full, headSnap) = chainWithHead(10)
    // Drop ord=5 from storage to simulate a backfill gap.
    val sparse = full - ord(5L)
    for {
      tower <- TowerStore.inMemory[IO]
      _ <- tower.appendAtFinality(ord(3L), h("snap-3-content"), trials(Set(1)))
      _ <- tower.appendAtFinality(ord(5L), h("snap-5-content"), trials(Set(1))) // tower has it
      _ <- tower.appendAtFinality(ord(8L), h("snap-8-content"), trials(Set(1)))
      builder = TowerProofBuilder.make[IO](tower, dummyStorage(sparse, Some(headSnap)))
      proof <- builder.buildFromGenesis
    } yield
      expect(proof.levelChains(1).map(_.ordinal.value.value) == Vector(3L, 8L)) // ord=5 skipped
        .and(expect(proof.levelChains(1).size == 2))
  }

  test("pre-activation snapshot (no slotCertificate) skipped in L0 suffix") {
    val (snaps, headSnap) = chainWithHead(5)
    // Replace ord=2 with a snapshot that has no slot certificate.
    val noCert = snaps(ord(2L)).copy(value = snaps(ord(2L)).value.copy(slotCertificate = None))
    val patched = snaps.updated(ord(2L), noCert)
    for {
      tower <- TowerStore.inMemory[IO]
      builder = TowerProofBuilder.make[IO](tower, dummyStorage(patched, Some(headSnap)))
      proof <- builder.build(SnapshotOrdinal.MinValue, k = 6)
    } yield
      // Ord 0..5 minus ord 2 = 5 headers.
      expect(proof.level0Suffix.size == 5)
        .and(expect(proof.level0Suffix.map(_.ordinal.value.value).toSet == Set(0L, 1L, 3L, 4L, 5L)))
  }

  test("totalHeaderCount + allHeadersByOrdinal — dedup across levels, sort ascending") {
    val (snaps, headSnap) = chainWithHead(10)
    for {
      tower <- TowerStore.inMemory[IO]
      _ <- tower.appendAtFinality(ord(2L), h("snap-2-content"), trials(Set(1, 2)))
      _ <- tower.appendAtFinality(ord(4L), h("snap-4-content"), trials(Set(1)))
      builder = TowerProofBuilder.make[IO](tower, dummyStorage(snaps, Some(headSnap)))
      proof <- builder.build(SnapshotOrdinal.MinValue, k = 3) // suffix = ords 8, 9, 10
    } yield {
      val ords = proof.allHeadersByOrdinal.map(_.ordinal.value.value)
      // levelChains: L1 has {2,4}, L2 has {2}. Distinct: {2,4}.
      // L0 suffix: {8,9,10}. Total distinct: {2,4,8,9,10}.
      expect(ords == Vector(2L, 4L, 8L, 9L, 10L))
        .and(expect(proof.totalHeaderCount == 3 + 2 + 1)) // suffix(3) + L1(2) + L2(1)
    }
  }

}
