package io.constellationnetwork.schema.nakamoto.follow

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.GlobalStateFieldId
import io.constellationnetwork.schema.swap.{AllowSpendOrdinal, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock.{TokenLockOrdinal, TokenLockReference}
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.syntax._
import weaver.SimpleIOSuite

/** Pure unit tests for [[ConsumedFieldDelta.diff]] (the #287 "send diffs" core) and the [[ConsumedFieldDelta]] Circe codec.
  *
  * `diff(prev, curr)` treats both inputs as FULL-from-empty projections and returns the genuine incremental delta: an entry is an UPSERT
  * iff absent from `prev` or value-changed; an `Address` present in `prev` but absent from `curr` is a per-field REMOVAL. These tests cover
  * add / change / remove across the consumed fields, the `diff(x, x) == empty` identity, and that the codec round-trips a delta carrying a
  * non-empty `removals` map (the part with the hand-rolled fieldId-keyed association-list encoding).
  *
  * The diff↔[[FollowVerifyCore.verifyFieldRoots]] round-trip (apply `diff(prev, curr)` on `prev` ⇒ `curr`'s field roots) needs a `Hasher` +
  * reference MPT and lives in `GlobalFollowSliceServiceSuite` (node-shared); here we keep the pure map algebra.
  */
object ConsumedFieldDeltaSuite extends SimpleIOSuite {

  private def addr(seed: Int): Address = Address.fromBytes(s"consumed-field-delta-suite-seed-$seed".getBytes("UTF-8"))
  private def bal(n: Long): Balance = Balance(NonNegLong.unsafeFrom(n))
  private def txRef(n: Long): TransactionReference = TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))
  private def allowRef(n: Long): AllowSpendReference = AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))
  private def lockRef(n: Long): TokenLockReference = TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))

  pureTest("diff(x, x) == empty for a non-trivial projection") {
    val x = ConsumedFieldDelta.empty.copy(
      balances = SortedMap(addr(1) -> bal(100), addr(2) -> bal(200)),
      lastTxRefs = SortedMap(addr(1) -> txRef(1)),
      lastAllowSpendRefs = SortedMap(addr(3) -> allowRef(3)),
      lastTokenLockRefs = SortedMap(addr(4) -> lockRef(4))
    )
    expect(ConsumedFieldDelta.diff(x, x) == ConsumedFieldDelta.empty)
  }

  pureTest("diff: an ADDED entry (absent in prev) is an upsert, no removals") {
    val prev = ConsumedFieldDelta.empty.copy(balances = SortedMap(addr(1) -> bal(100)))
    val curr = ConsumedFieldDelta.empty.copy(balances = SortedMap(addr(1) -> bal(100), addr(2) -> bal(200)))
    val d = ConsumedFieldDelta.diff(prev, curr)
    expect.all(
      d.balances == SortedMap(addr(2) -> bal(200)),
      d.removals.isEmpty
    )
  }

  pureTest("diff: a CHANGED value is an upsert of the new value, no removals") {
    val prev = ConsumedFieldDelta.empty.copy(balances = SortedMap(addr(1) -> bal(100)))
    val curr = ConsumedFieldDelta.empty.copy(balances = SortedMap(addr(1) -> bal(999)))
    val d = ConsumedFieldDelta.diff(prev, curr)
    expect.all(
      d.balances == SortedMap(addr(1) -> bal(999)),
      d.removals.isEmpty
    )
  }

  pureTest("diff: an entry present in prev but absent in curr is a per-field removal") {
    val prev = ConsumedFieldDelta.empty.copy(balances = SortedMap(addr(1) -> bal(100), addr(2) -> bal(200)))
    val curr = ConsumedFieldDelta.empty.copy(balances = SortedMap(addr(1) -> bal(100)))
    val d = ConsumedFieldDelta.diff(prev, curr)
    expect.all(
      d.balances.isEmpty,
      d.removals.get(GlobalStateFieldId.Balances) == Some(Set(addr(2)))
    )
  }

  pureTest("diff: add/change/remove combine across all five consumed fields; only non-empty removal sets are kept") {
    val prev = ConsumedFieldDelta.empty.copy(
      balances = SortedMap(addr(1) -> bal(100), addr(2) -> bal(200)), // addr2 removed
      lastTxRefs = SortedMap(addr(1) -> txRef(1)), // addr1 changed
      lastAllowSpendRefs = SortedMap(addr(3) -> allowRef(3)), // unchanged
      lastTokenLockRefs = SortedMap(addr(4) -> lockRef(4)) // addr4 removed
    )
    val curr = ConsumedFieldDelta.empty.copy(
      balances = SortedMap(addr(1) -> bal(100), addr(5) -> bal(500)), // addr5 added, addr2 gone
      lastTxRefs = SortedMap(addr(1) -> txRef(9)), // changed
      lastAllowSpendRefs = SortedMap(addr(3) -> allowRef(3)), // unchanged ⇒ no upsert
      lastTokenLockRefs = SortedMap.empty // addr4 gone
    )
    val d = ConsumedFieldDelta.diff(prev, curr)
    expect.all(
      d.balances == SortedMap(addr(5) -> bal(500)),
      d.lastTxRefs == SortedMap(addr(1) -> txRef(9)),
      d.lastAllowSpendRefs.isEmpty,
      d.lastTokenLockRefs.isEmpty,
      // removals carry ONLY the non-empty per-field address sets
      d.removals == SortedMap[GlobalStateFieldId, Set[Address]](
        GlobalStateFieldId.Balances -> Set(addr(2)),
        GlobalStateFieldId.LastTokenLockRefs -> Set(addr(4))
      )
    )
  }

  pureTest("ConsumedFieldDelta Circe codec round-trips a delta with a non-empty removals map") {
    val d = ConsumedFieldDelta.empty.copy(
      balances = SortedMap(addr(5) -> bal(500)),
      lastTxRefs = SortedMap(addr(1) -> txRef(9)),
      removals = SortedMap[GlobalStateFieldId, Set[Address]](
        GlobalStateFieldId.Balances -> Set(addr(2), addr(3)),
        GlobalStateFieldId.LastTokenLockRefs -> Set(addr(4))
      )
    )
    expect(d.asJson.as[ConsumedFieldDelta] == Right(d))
  }
}
