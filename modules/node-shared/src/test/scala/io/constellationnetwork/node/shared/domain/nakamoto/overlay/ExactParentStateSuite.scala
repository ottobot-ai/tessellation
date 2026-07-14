package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.ParentStateError._
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.ParentStateUnavailableReason._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot

import weaver.SimpleIOSuite

object ExactParentStateSuite extends SimpleIOSuite {

  private val limit = 8

  private def branch(n: Int): BranchId = BranchId(Hash(f"$n%064x"))
  private def root(n: Int): MptRoot = MptRoot(Hash(f"${10000 + n}%064x"))

  private def state(ordinal: Long, id: Int, parentId: Int, rootId: Int = -1): GlobalSnapshotStateRef =
    GlobalSnapshotStateRef(
      SnapshotOrdinal.unsafeApply(ordinal),
      branch(id).value,
      branch(parentId).value,
      root(if (rootId < 0) id else rootId)
    )

  private def key(ref: GlobalSnapshotStateRef): BranchId = BranchId(ref.hash)

  private val base = state(10L, 10, 9)

  pureTest("exact base identity resolves as structural data without an implicit sentinel") {
    val result = ExactParentResolver.resolve(base, Map.empty, base, limit)

    expect(result.exists(lineage => lineage.parent == base && lineage.base == base && lineage.lineage.isEmpty))
  }

  pureTest("unknown requested parent is typed unavailable and never resolves to base") {
    val requested = state(11L, 11, 10)
    val result = ExactParentResolver.resolve(base, Map.empty, requested, limit)

    expect(
      result.left.exists {
        case ParentStateUnavailable(`requested`, missing, RequestedParentMissing) => missing == key(requested)
        case _                                                                    => false
      }
    )
  }

  pureTest("missing middle ancestor identifies the exact missing hash") {
    val middle = state(11L, 11, 10)
    val requested = state(12L, 12, 11)
    val result = ExactParentResolver.resolve(base, Map(key(requested) -> requested), requested, limit)

    expect(
      result.left.exists {
        case ParentStateUnavailable(`requested`, missing, MissingAncestorOf(child)) =>
          missing == key(middle) && child == requested
        case _ => false
      }
    )
  }

  pureTest("complete contiguous lineage is returned base-exclusive in execution order") {
    val a11 = state(11L, 11, 10)
    val a12 = state(12L, 12, 11)
    val a13 = state(13L, 13, 12)
    val pending = Vector(a13, a11, a12).map(ref => key(ref) -> ref).toMap

    val result = ExactParentResolver.resolve(base, pending, a13, limit)

    expect(result.exists(_.lineage == Vector(a11, a12, a13)))
  }

  pureTest("same hash with different exact identity rejects") {
    val stored = state(11L, 11, 10)
    val requested = stored.copy(mptRoot = root(999))
    val result = ExactParentResolver.resolve(base, Map(key(stored) -> stored), requested, limit)

    expect(result.left.toOption.contains(ParentStateMismatch(requested, stored)))
  }

  pureTest("ordinal discontinuity rejects before a lineage is returned") {
    val requested = state(12L, 12, 10)
    val result = ExactParentResolver.resolve(base, Map(key(requested) -> requested), requested, limit)

    expect(result.left.toOption.contains(OrdinalDiscontinuity(base, requested)))
  }

  pureTest("self-parent and multi-entry cycles reject deterministically") {
    val self = state(11L, 11, 11)
    val a = state(12L, 12, 11)
    val b = state(11L, 11, 12)

    val selfResult = ExactParentResolver.resolve(base, Map(key(self) -> self), self, limit)
    val cycleResult = ExactParentResolver.resolve(base, Map(key(a) -> a, key(b) -> b), a, limit)

    expect(selfResult.left.exists(_.isInstanceOf[AncestryCycle]))
      .and(expect(cycleResult.left.exists(_.isInstanceOf[AncestryCycle])))
  }

  pureTest("pending map key must equal the stored snapshot hash on the requested path") {
    val requested = state(11L, 11, 10)
    val misindexed = state(11L, 12, 10)
    val result = ExactParentResolver.resolve(base, Map(key(requested) -> misindexed), requested, limit)

    expect(result.left.toOption.contains(PendingIndexMismatch(key(requested), misindexed)))
  }

  pureTest("Hash.empty and non-canonical identities reject at the structural boundary") {
    val reserved = base.copy(hash = Hash.empty)
    val badBase = base.copy(hash = Hash("not-a-hash"))
    val badRequested = state(11L, 11, 10).copy(parentHash = Hash("ABC"))

    expect(ExactParentResolver.resolve(reserved, Map.empty, reserved, limit).left.toOption.contains(ReservedBaseSentinel(reserved)))
      .and(
        expect(
          ExactParentResolver
            .resolve(badBase, Map.empty, badBase, limit)
            .left
            .exists(_.isInstanceOf[NonCanonicalStateIdentity])
        )
      )
      .and(
        expect(
          ExactParentResolver
            .resolve(base, Map.empty, badRequested, limit)
            .left
            .exists(_.isInstanceOf[NonCanonicalStateIdentity])
        )
      )
  }

  pureTest("Hash.empty cannot be smuggled into an intermediate pending lineage") {
    val sentinelParent = state(11L, 11, 0)
    val requested = state(12L, 12, 11)
    val result = ExactParentResolver.resolve(
      base,
      Map(key(sentinelParent) -> sentinelParent, key(requested) -> requested),
      requested,
      limit
    )

    expect(result.left.toOption.contains(ReservedBaseSentinel(sentinelParent)))
  }

  pureTest("pending state cannot shadow the supplied base hash") {
    val shadow = base.copy(parentHash = branch(8).value)
    val result = ExactParentResolver.resolve(base, Map(key(base) -> shadow), base, limit)

    expect(result.left.toOption.contains(PendingBaseCollision(base, shadow)))
  }

  pureTest("lineage traversal is explicitly bounded") {
    val a11 = state(11L, 11, 10)
    val a12 = state(12L, 12, 11)
    val pending = Map(key(a11) -> a11, key(a12) -> a12)

    val invalid = ExactParentResolver.resolve(base, pending, a12, maxSteps = 0)
    val exhausted = ExactParentResolver.resolve(base, pending, a12, maxSteps = 1)

    expect
      .same(Left(InvalidLineageLimit(0)), invalid)
      .and(expect(exhausted.left.exists(_.isInstanceOf[LineageLimitExceeded])))
  }

  pureTest("an unrelated malformed sibling cannot poison a valid requested path") {
    val requested = state(11L, 11, 10)
    val malformedSibling = state(11L, 12, 10).copy(mptRoot = MptRoot(Hash("BAD")))
    val pending = Map(key(requested) -> requested, key(malformedSibling) -> malformedSibling)

    val result = ExactParentResolver.resolve(base, pending, requested, limit)

    expect(result.exists(_.lineage == Vector(requested)))
  }
}
