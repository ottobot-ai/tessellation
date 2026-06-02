package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.IO

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.GlobalChangeSetService
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

/** Test (c) of task #12 slice 3 — the PRODUCE → FINALIZE → RING regression (the slice-2b blocker guard).
  *
  * The slice-2b functional defect was: the gl0 producer STAGES the per-ordinal accumulator under the RAW artifact hash (pre
  * slotCertificate+eta), but the finalize-sink promotion looks it up under the WITH-CERT canonical hash. Without the produce-path
  * raw→with-cert rekey beside the overlay rekey, the served changeset ring NEVER populates for self-produced snapshots, so
  * `GlobalChangeSetService.changeSetSince` returns nothing useful — a defect compilation cannot catch.
  *
  * This suite drives the EXACT production helpers `SnapshotLeaderLoop.rekeyStagedAccumulator` (the produce-path rekey) and
  * `SnapshotLeaderLoop.ringInsertTrimmed` (the finalize-sink ring insert) — the same functions both production sites now call — through the
  * full path: stage-under-raw → rekey → promote (pull-from-staging by with-cert hash + ring-insert) → surface via `changeSetSince`. The
  * load-bearing assertion is that the promoted delta is found under the WITH-CERT hash and reaches the served ring; a guard test asserts
  * the lookup FAILS without the rekey (so the rekey is genuinely what makes the path work).
  *
  * '''Why a helper extraction and not a full SnapshotLeaderLoop integration test''': the rekey + promote logic lives deep inside the leader
  * loop's `mptStore.withTransaction` block and finalize-sink stream (a multi-hundred-line consensus pipeline requiring a real chain store,
  * MPT overlay, KES signer, gossip, supervisor, and wall-clock slot timing). Standing that up in a unit test is impractical within reason;
  * the slice-5 e2e is the full integration proof. Extracting the two pure ref-manipulation steps into testable helpers — which the
  * production sites now invoke verbatim — gives genuine coverage of the regression-prone logic without faking the pipeline.
  */
object ChangeSetRingPromotionSuite extends SimpleIOSuite {

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  private val addr: Address = Address.fromBytes("changeset-ring-promotion-suite-seed".getBytes("UTF-8"))

  private def acc(seed: Long): StateChangesAccumulator =
    StateChangesAccumulator(balances = SortedMap(addr -> Balance(NonNegLong.unsafeFrom(1000L + seed))))

  // Two distinct hashes standing in for the raw artifact hash (what the producer stages under) and the
  // with-cert canonical hash (what the finalize sink looks up under). They MUST differ — that difference is
  // the whole bug class.
  private val rawHash: Hash = Hash("11" * 32)
  private val withCertHash: Hash = Hash("22" * 32)

  test("produce→finalize→ring: staged-under-raw accumulator surfaces via changeSetSince after rekey+promote") {
    val ordinal = ord(5L)
    val a = acc(5L)

    // 1) PRODUCER stages the accumulator under the RAW artifact hash.
    val stagedAtProduce: Map[Hash, StateChangesAccumulator] = Map(rawHash -> a)

    // 2) PRODUCE-PATH rekey raw → with-cert (the real `SnapshotLeaderLoop.rekeyStagedAccumulator`).
    val rekeyed = SnapshotLeaderLoop.rekeyStagedAccumulator(stagedAtProduce, rawHash, withCertHash)

    // 3) FINALIZE-SINK promote: pull from staging by the WITH-CERT hash, ring-insert (the real
    //    `SnapshotLeaderLoop.ringInsertTrimmed`). This mirrors `recordFinalizedAccumulator`'s atomic
    //    modify/update exactly.
    val pulled = rekeyed.get(withCertHash)
    val ring0 = SortedMap.empty[SnapshotOrdinal, StateChangesAccumulator]
    val ringAfter = pulled.fold(ring0)(p => SnapshotLeaderLoop.ringInsertTrimmed(ring0, ordinal, p))

    // 4) The served changeset service reads THIS ring.
    val service = GlobalChangeSetService.make[IO](IO.pure(ringAfter))

    for {
      // A follower at ordinal-1 must receive the just-finalized delta.
      changeSet <- service.changeSetSince(ord(4L))
    } yield
      expect(pulled.isDefined) &&                                   // rekey made the with-cert lookup succeed
      expect.same(rekeyed.get(rawHash), None) &&                    // raw key removed by the rekey (no leak)
      expect.same(ringAfter.get(ordinal), Some(a)) &&               // promoted into the served ring
      expect(changeSet.isDefined) &&
      expect.same(changeSet.flatMap(_.baseOrdinal), Some(ord(4L))) &&
      expect.same(changeSet.map(_.latestOrdinal), Some(ordinal)) &&
      expect.same(changeSet.map(_.deltas), Some(List(ordinal -> a)))
  }

  test("guard: WITHOUT the produce-path rekey, the finalize-sink lookup misses and the ring stays empty") {
    val ordinal = ord(5L)
    val a = acc(5L)

    // Producer stages under raw, but we SKIP the rekey — the slice-2b defect.
    val stagedAtProduce: Map[Hash, StateChangesAccumulator] = Map(rawHash -> a)

    // Finalize sink looks up under the with-cert hash → miss → nothing promoted.
    val pulled = stagedAtProduce.get(withCertHash)
    val ring0 = SortedMap.empty[SnapshotOrdinal, StateChangesAccumulator]
    val ringAfter = pulled.fold(ring0)(p => SnapshotLeaderLoop.ringInsertTrimmed(ring0, ordinal, p))

    val service = GlobalChangeSetService.make[IO](IO.pure(ringAfter))

    for {
      changeSet <- service.changeSetSince(ord(4L))
    } yield
      expect.same(pulled, None) &&            // the bug: with-cert lookup misses the raw-staged entry
      expect(ringAfter.isEmpty) &&            // ring never populates
      expect.same(changeSet, None)            // service has nothing to serve (cold ring)
  }

  test("ringInsertTrimmed bounds the served ring to recentAccumulatorsToKeep, dropping the lowest ordinals") {
    val keep = GlobalChangeSetService.recentAccumulatorsToKeep
    // Insert keep+1 ordinals (1..keep+1); the lowest (ordinal 1) must be evicted, the rest retained.
    val ring = (1L to (keep.toLong + 1L)).foldLeft(SortedMap.empty[SnapshotOrdinal, StateChangesAccumulator]) {
      case (r, n) => SnapshotLeaderLoop.ringInsertTrimmed(r, ord(n), acc(n))
    }
    IO.pure(
      expect.same(ring.size, keep) &&
        expect.same(ring.contains(ord(1L)), false) &&
        expect.same(ring.contains(ord(keep.toLong + 1L)), true)
    )
  }
}
