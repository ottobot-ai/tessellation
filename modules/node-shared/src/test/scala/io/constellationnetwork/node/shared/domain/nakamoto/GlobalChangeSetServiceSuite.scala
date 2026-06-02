package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Id

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.nakamoto.follow.GlobalChangeSetResponse

import weaver.FunSuite

/** Unit suite for [[GlobalChangeSetService]] — the changeSetSince ring-slice logic (task #12, slice 2). */
object GlobalChangeSetServiceSuite extends FunSuite {

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal.unsafeApply(n)
  private val emptyAcc: StateChangesAccumulator = StateChangesAccumulator()

  private val ring: SortedMap[SnapshotOrdinal, StateChangesAccumulator] =
    SortedMap(ord(100) -> emptyAcc, ord(101) -> emptyAcc, ord(102) -> emptyAcc, ord(103) -> emptyAcc)

  private def svc(r: SortedMap[SnapshotOrdinal, StateChangesAccumulator]) =
    GlobalChangeSetService.make[Id](r)

  test("empty ring → None") {
    expect(svc(SortedMap.empty[SnapshotOrdinal, StateChangesAccumulator]).changeSetSince(ord(100)).isEmpty)
  }

  test("since == latest → no-op (empty deltas, baseOrdinal = Some(since))") {
    expect(svc(ring).changeSetSince(ord(103)) == Some(GlobalChangeSetResponse(ord(103), Some(ord(103)), Nil)))
  }

  test("since within ring → contiguous deltas (since, latest], baseOrdinal = Some(since)") {
    val r = svc(ring).changeSetSince(ord(101)).get
    expect(r.latestOrdinal == ord(103))
      .and(expect(r.baseOrdinal == Some(ord(101))))
      .and(expect(r.deltas.map(_._1) == List(ord(102), ord(103))))
  }

  test("since == ring.min - 1 → full contiguous ring served") {
    val r = svc(ring).changeSetSince(ord(99)).get
    expect(r.baseOrdinal == Some(ord(99)))
      .and(expect(r.deltas.map(_._1) == List(ord(100), ord(101), ord(102), ord(103))))
  }

  test("since older than ring (gap) → baseOrdinal = None (full-GSI fallback)") {
    val r = svc(ring).changeSetSince(ord(50)).get
    expect(r.baseOrdinal.isEmpty).and(expect(r.deltas.isEmpty))
  }
}
