package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.{IO, Resource}

import scala.collection.immutable.SortedSet

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.MetagraphsSyncConfig
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.sharding.{CrossShardReceipt, ShardId}
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import weaver.MutableIOSuite

/** Tests for [[MetagraphSyncManager.consumeReceipts]] — Slice 12 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §8.4.
  *
  * '''Required coverage''' (per slice spec):
  *   1. '''Single receipt applied''' — `MetagraphSyncDataWrite(targetMg=Y, payload=P)` updates Y's accumulator to `P`
  *   1. '''Multiple receipts applied in order''' — three receipts to three different MGs all land
  *   1. '''Same receipt twice is no-op''' — duplicate apply does not double-update
  *   1. '''Empty receipt list''' — `consumeReceipts(Nil)` is a no-op and doesn't crash
  *   1. '''Receipt for unknown target MG''' — handled by initialising from `MetagraphSyncDataInfo.empty` (same pattern as the
  *      existing `updateFromSpendActions` path)
  *
  * '''Test fixture pattern.''' Uses [[MetagraphSyncManager.makeWithInspector]] which returns a `Built` handle bundling the manager
  * plus reads of its internal Refs. The Refs are normally hidden from production callers (Slice 13 will drain them through a
  * different surface inside GSAM); the inspector handle exists solely to let these tests assert state-after-consume cleanly
  * without poking at private fields via reflection.
  *
  * Address fixtures use deterministic well-formed DAG strings — same pattern as
  * [[io.constellationnetwork.node.shared.domain.nakamoto.MetagraphOrphanBufferSuite]] and
  * [[GlobalSnapshotAcceptanceManagerSuite]].
  */
object MetagraphSyncManagerSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  // ============================================================================
  // Fixtures
  // ============================================================================

  private val cfg: MetagraphsSyncConfig = MetagraphsSyncConfig(PosInt(100))

  // Three distinct, well-formed metagraph addresses for the multi-receipt scenario. The exact string prefix doesn't matter for
  // the consumer — it never inspects the address value, only uses it as a `SortedMap[Address, _]` key. Re-uses the same DAG
  // strings the in-tree tests use to keep fixture conventions consistent.
  private val mgA: Address = Address("DAG0y4eLqhhXUafeE3mgBstezPTnr8L3tZjAtMWB")
  private val mgB: Address = Address("DAG07tqNLYW8jHU9emXcRTT3CfgCUoumwcLghopd")
  private val mgC: Address = Address("DAG0y4eLqhhXUafeE3mgBstezPTnr8L3tZjAtMWC")

  // Source metagraph and shard for the receipts — the values are observability-only on the consumer side (the consumer cares
  // only about `targetMetagraph` + `increment`); using distinct values keeps the receipt hashes distinct under the idempotence
  // gate even when the increments collide.
  private val sourceMg: Address = Address("DAG0y4eLqhhXUafeE3mgBstezPTnr8L3tZjAtMWA")
  private val sourceShard: ShardId = ShardId.unsafeApply(0)
  private val targetShard: ShardId = ShardId.unsafeApply(1)

  // Deterministic Hashes for `sourceCheckpointHash` — UTF-8-padded labels per the convention used by `TokenLockStateManagerSuite`.
  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  /** Build an `increment` payload. Distinct `globalOrdinalLastAcceptedOn` per call so the merge semantics test (multiple
    * receipts to the same target) can distinguish ordering: the highest-watermark increment wins on the scalar field while the
    * `unappliedGlobalChangeOrdinals` union accumulates.
    *
    * Three explicit parameters (no defaults) — scalafix's `NoDefaultArgs` rule for private helpers (the rule guards against
    * unused defaults silently drifting) is satisfied by requiring every call site to pass `epoch` and `unapplied` explicitly.
    */
  private def mkIncrement(
    ord: Long,
    epoch: Long,
    unapplied: SortedSet[SnapshotOrdinal]
  ): MetagraphSyncDataInfo =
    MetagraphSyncDataInfo(
      globalOrdinalLastAcceptedOn = SnapshotOrdinal(NonNegLong.unsafeFrom(ord)),
      globalEpochProgressLastAcceptedOn = EpochProgress(NonNegLong.unsafeFrom(epoch)),
      unappliedGlobalChangeOrdinals = unapplied
    )

  /** Build a `MetagraphSyncDataWrite` receipt. The `sourceCheckpointHash` distinguishes receipts under the seen-set hash
    * (Circe encodes the field directly, so two receipts with otherwise-identical fields but different `sourceCheckpointHash`
    * values hash differently and both go through the apply path).
    */
  private def mkReceipt(
    target: Address,
    increment: MetagraphSyncDataInfo,
    sourceHashLabel: String
  ): CrossShardReceipt =
    CrossShardReceipt.MetagraphSyncDataWrite(
      sourceShardId = sourceShard,
      sourceMetagraph = sourceMg,
      sourceCheckpointHash = testHash(sourceHashLabel),
      targetShardId = targetShard,
      targetMetagraph = target,
      increment = increment
    )

  // ============================================================================
  // Tests
  // ============================================================================

  test("consumeReceipts — single receipt applied: targetMg's accumulator reflects the increment") { res =>
    implicit val (h, _, _) = res
    val increment = mkIncrement(ord = 42L, epoch = 7L, unapplied = SortedSet.empty)
    val receipt = mkReceipt(mgA, increment, "cp-single")

    for {
      built <- MetagraphSyncManager.makeWithInspector[IO](cfg)
      _ <- built.manager.consumeReceipts(List(receipt))
      pending <- built.pendingCrossShardWrites
      seen <- built.seenReceiptHashes
    } yield expect.all(
      pending.size == 1,
      pending.get(mgA).contains(increment),
      seen.size == 1
    )
  }

  test("consumeReceipts — multiple receipts to different MGs all apply in order") { res =>
    implicit val (h, _, _) = res
    val incA = mkIncrement(ord = 10L, epoch = 1L, unapplied = SortedSet.empty)
    val incB = mkIncrement(ord = 20L, epoch = 2L, unapplied = SortedSet.empty)
    val incC = mkIncrement(ord = 30L, epoch = 3L, unapplied = SortedSet.empty)
    val receipts = List(
      mkReceipt(mgA, incA, "cp-multi-A"),
      mkReceipt(mgB, incB, "cp-multi-B"),
      mkReceipt(mgC, incC, "cp-multi-C")
    )

    for {
      built <- MetagraphSyncManager.makeWithInspector[IO](cfg)
      _ <- built.manager.consumeReceipts(receipts)
      pending <- built.pendingCrossShardWrites
      seen <- built.seenReceiptHashes
    } yield expect.all(
      pending.size == 3,
      pending.get(mgA).contains(incA),
      pending.get(mgB).contains(incB),
      pending.get(mgC).contains(incC),
      seen.size == 3
    )
  }

  test("consumeReceipts — same receipt twice is no-op: state matches single-apply") { res =>
    implicit val (h, _, _) = res
    val increment = mkIncrement(ord = 42L, epoch = 7L, unapplied = SortedSet.empty)
    val receipt = mkReceipt(mgA, increment, "cp-idem")

    for {
      // Apply once
      single <- MetagraphSyncManager.makeWithInspector[IO](cfg)
      _ <- single.manager.consumeReceipts(List(receipt))
      pendingSingle <- single.pendingCrossShardWrites
      seenSingle <- single.seenReceiptHashes

      // Apply twice on a fresh manager
      twice <- MetagraphSyncManager.makeWithInspector[IO](cfg)
      _ <- twice.manager.consumeReceipts(List(receipt))
      _ <- twice.manager.consumeReceipts(List(receipt))
      pendingTwice <- twice.pendingCrossShardWrites
      seenTwice <- twice.seenReceiptHashes
    } yield expect.all(
      // Single-apply baseline
      pendingSingle.size == 1,
      pendingSingle.get(mgA).contains(increment),
      seenSingle.size == 1,
      // Idempotence: double-apply must equal single-apply
      pendingTwice == pendingSingle,
      seenTwice.size == 1
    )
  }

  test("consumeReceipts — empty receipt list is a no-op and doesn't crash") { res =>
    implicit val (h, _, _) = res

    for {
      built <- MetagraphSyncManager.makeWithInspector[IO](cfg)
      _ <- built.manager.consumeReceipts(List.empty)
      pending <- built.pendingCrossShardWrites
      seen <- built.seenReceiptHashes
    } yield expect.all(
      pending.isEmpty,
      seen.isEmpty
    )
  }

  test("consumeReceipts — receipt for previously-unknown target MG starts from `empty` and folds the increment") { res =>
    implicit val (h, _, _) = res
    // No prior state for mgB; the receipt lands and we expect mgB's accumulator to equal the increment exactly (because the
    // merge with `MetagraphSyncDataInfo.empty` is a no-op for the prior-state side at every field).
    val increment = mkIncrement(ord = 99L, epoch = 5L, unapplied = SortedSet(SnapshotOrdinal(NonNegLong.unsafeFrom(11L))))
    val receipt = mkReceipt(mgB, increment, "cp-unknown-target")

    for {
      built <- MetagraphSyncManager.makeWithInspector[IO](cfg)
      preState <- built.pendingCrossShardWrites
      _ <- built.manager.consumeReceipts(List(receipt))
      postState <- built.pendingCrossShardWrites
    } yield expect.all(
      // Confirm we really started from nothing.
      preState.isEmpty,
      // Post-consume: exactly one entry, equal to the increment.
      postState.size == 1,
      postState.get(mgB).contains(increment)
    )
  }

  test("consumeReceipts — two receipts to the same target merge monotonically (max ord, max epoch, union of ordinals)") { res =>
    implicit val (h, _, _) = res
    // First receipt establishes a baseline; second receipt has a higher ord+epoch and a different unapplied-ordinal entry.
    // Expected merge: max ord (200), max epoch (9), union of the two `unappliedGlobalChangeOrdinals`.
    val low = mkIncrement(ord = 100L, epoch = 3L, unapplied = SortedSet(SnapshotOrdinal(NonNegLong.unsafeFrom(1L))))
    val high = mkIncrement(ord = 200L, epoch = 9L, unapplied = SortedSet(SnapshotOrdinal(NonNegLong.unsafeFrom(2L))))

    val expectedMerged = MetagraphSyncDataInfo(
      globalOrdinalLastAcceptedOn = SnapshotOrdinal(NonNegLong.unsafeFrom(200L)),
      globalEpochProgressLastAcceptedOn = EpochProgress(NonNegLong.unsafeFrom(9L)),
      unappliedGlobalChangeOrdinals = SortedSet(
        SnapshotOrdinal(NonNegLong.unsafeFrom(1L)),
        SnapshotOrdinal(NonNegLong.unsafeFrom(2L))
      )
    )

    for {
      built <- MetagraphSyncManager.makeWithInspector[IO](cfg)
      _ <- built.manager.consumeReceipts(List(mkReceipt(mgA, low, "cp-merge-low"), mkReceipt(mgA, high, "cp-merge-high")))
      pending <- built.pendingCrossShardWrites
    } yield expect.all(
      pending.size == 1,
      pending.get(mgA).contains(expectedMerged)
    )
  }

  test("consumeReceipts — out-of-order receipts still produce monotone result (low after high doesn't roll back)") { res =>
    implicit val (h, _, _) = res
    // Reverse order from the prior test: apply the high-watermark increment first, then the low. Expected: high-watermark
    // scalar fields stick; ordinals union additively. Confirms the merge is order-insensitive for the scalar fields.
    val low = mkIncrement(ord = 100L, epoch = 3L, unapplied = SortedSet(SnapshotOrdinal(NonNegLong.unsafeFrom(1L))))
    val high = mkIncrement(ord = 200L, epoch = 9L, unapplied = SortedSet(SnapshotOrdinal(NonNegLong.unsafeFrom(2L))))

    val expectedMerged = MetagraphSyncDataInfo(
      globalOrdinalLastAcceptedOn = SnapshotOrdinal(NonNegLong.unsafeFrom(200L)),
      globalEpochProgressLastAcceptedOn = EpochProgress(NonNegLong.unsafeFrom(9L)),
      unappliedGlobalChangeOrdinals = SortedSet(
        SnapshotOrdinal(NonNegLong.unsafeFrom(1L)),
        SnapshotOrdinal(NonNegLong.unsafeFrom(2L))
      )
    )

    for {
      built <- MetagraphSyncManager.makeWithInspector[IO](cfg)
      _ <- built.manager.consumeReceipts(List(mkReceipt(mgA, high, "cp-reverse-high"), mkReceipt(mgA, low, "cp-reverse-low")))
      pending <- built.pendingCrossShardWrites
    } yield expect.all(
      pending.size == 1,
      pending.get(mgA).contains(expectedMerged)
    )
  }
}
