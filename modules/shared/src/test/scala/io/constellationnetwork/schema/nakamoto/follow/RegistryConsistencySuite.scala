package io.constellationnetwork.schema.nakamoto.follow

import cats.data.NonEmptySet
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.GlobalSnapshotInfo
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap.{AllowSpendOrdinal, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.SimpleIOSuite

/** Drift guard for the uniform-5 follow-sync field set ([[SyncedField.baseRegistry]]).
  *
  * This is the compile/CI tripwire that makes "which gl0 fields a follower syncs" single-sourced: if anyone adds, removes, or reorders a
  * uniform field in [[SyncedField.baseRegistry]] without keeping it in lockstep with [[FollowVerifyCore.consumedFields]] — or breaks the
  * `readFromGsi`/`writeToGsi` symmetry for a field — one of these assertions fails. The cryptographic byte-identity of the recompute is
  * exercised end-to-end by `GlobalFollowSliceServiceSuite` (node-shared, with a `Hasher` + reference MPT); here we keep the pure structural
  * invariants that need no `Hasher`.
  *
  * Coverage:
  *   1. registry fieldIds == the legacy literal [[FollowVerifyCore.consumedFields]] list (same elements, SAME ORDER);
  *   1. registry fieldIds are distinct (no duplicate partition);
  *   1. per field, `readFromGsi` ∘ `writeToGsi(empty, _)` ∘ `readFromGsi` round-trips on a sample GSI (the write puts the field back where
  *      the read finds it — the symmetry the slice-producer + follower-assembly depend on).
  */
object RegistryConsistencySuite extends SimpleIOSuite {

  private def addr(seed: Int): Address = Address.fromBytes(s"registry-consistency-suite-seed-$seed".getBytes("UTF-8"))
  private def bal(n: Long): Balance = Balance(NonNegLong.unsafeFrom(n))
  private def txRef(n: Long): TransactionReference = TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))
  private def allowRef(n: Long): AllowSpendReference = AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))
  private def lockRef(n: Long): TokenLockReference = TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))

  private def signedTokenLock(seed: Int, amount: Long): Signed[TokenLock] =
    Signed(
      TokenLock(
        source = addr(seed),
        amount = TokenLockAmount(PosLong.unsafeFrom(amount)),
        fee = TokenLockFee(NonNegLong(1L)),
        parent = TokenLockReference(TokenLockOrdinal(NonNegLong(1L)), Hash(f"$seed%064d")),
        currencyId = none,
        unlockEpoch = EpochProgress(NonNegLong(1000L)).some,
        replaceTokenLockRef = none
      ),
      NonEmptySet.one(SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70))))
    )

  // A sample GSI with all five uniform consumed fields populated (the three Option-typed ones lifted into Some).
  private val sampleGsi: GlobalSnapshotInfo =
    GlobalSnapshotInfo.empty.copy(
      balances = SortedMap(addr(1) -> bal(1000), addr(2) -> bal(2000)),
      lastTxRefs = SortedMap(addr(1) -> txRef(1), addr(2) -> txRef(5)),
      lastAllowSpendRefs = SortedMap(addr(1) -> allowRef(10)).some,
      lastTokenLockRefs = SortedMap(addr(1) -> lockRef(101)).some,
      activeTokenLocks = SortedMap(addr(1) -> SortedSet(signedTokenLock(1, 600))).some
    )

  pureTest("baseRegistry.map(_.fieldId) == FollowVerifyCore.consumedFields (same elements, same order)") {
    expect(SyncedField.baseRegistry.map(_.fieldId) == FollowVerifyCore.consumedFields)
  }

  pureTest("baseRegistry fieldIds are distinct (no duplicate partition)") {
    val ids = SyncedField.baseRegistry.map(_.fieldId)
    expect(ids.distinct == ids)
  }

  pureTest("per field: readFromGsi ∘ writeToGsi(empty, _) ∘ readFromGsi round-trips on a sample GSI") {
    val roundTripOk: Boolean = SyncedField.baseRegistry.forall { sf =>
      // The read produces a path-dependent `sf.V`-keyed map; writing it back and re-reading must reproduce it.
      // Both reads share the same `sf`, so the value type is fixed within each iteration (no cast leaks `V`).
      val read = sf.readFromGsi(sampleGsi)
      val roundTripped = sf.readFromGsi(sf.writeToGsi(GlobalSnapshotInfo.empty, read))
      read == roundTripped
    }
    expect.all(
      roundTripOk,
      // sanity: the sample populated every field, so each read is actually non-empty (the round-trip isn't trivially empty==empty)
      SyncedField.baseRegistry.forall(sf => sf.readFromGsi(sampleGsi).nonEmpty)
    )
  }
}
