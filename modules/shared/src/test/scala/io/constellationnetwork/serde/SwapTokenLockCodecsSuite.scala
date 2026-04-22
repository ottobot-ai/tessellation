package io.constellationnetwork.serde

import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.AllowSpendCodec.{immutableCodec => allowSpendImmutable}
import io.constellationnetwork.serde.codecs.instances.TokenLockCodec.{immutableCodec => tokenLockImmutable}
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.FunSuite

/** Round-trip suite for `AllowSpend` and `TokenLock`. Exercises the full stack:
  *   - Option (currencyId, unlockEpoch, replaceTokenLockRef),
  *   - List (approvers),
  *   - primitive newtypes (SwapAmount, AllowSpendFee, TokenLockAmount, TokenLockFee),
  *   - the reference codecs (AllowSpendReference, TokenLockReference).
  */
object SwapTokenLockCodecsSuite extends FunSuite {

  private val srcAddr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")
  private val dstAddr = AddressCodec.unsafeFromLiteral("DAG1UUPsDext9pvuoiyNTM72SX4t1xyod4Q1uXiM")

  test("AllowSpend round-trips with empty approvers + no currencyId") {
    val sample = AllowSpend(
      source = srcAddr,
      destination = dstAddr,
      currencyId = None,
      amount = SwapAmount(PosLong.unsafeFrom(100L)),
      fee = AllowSpendFee(NonNegLong.unsafeFrom(1L)),
      parent = AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(0L)), Hash("0" * 64)),
      lastValidEpochProgress = EpochProgress(NonNegLong.unsafeFrom(200L)),
      approvers = Nil
    )
    expect(allowSpendImmutable.immutableBytes(sample).fromImmutableBytes[AllowSpend](allowSpendImmutable) == Right(sample))
  }

  test("AllowSpend round-trips with non-empty approvers + currencyId") {
    val sample = AllowSpend(
      source = srcAddr,
      destination = dstAddr,
      currencyId = Some(CurrencyId(srcAddr)),
      amount = SwapAmount(PosLong.unsafeFrom(9999L)),
      fee = AllowSpendFee(NonNegLong.unsafeFrom(7L)),
      parent = AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(42L)), Hash("a" * 64)),
      lastValidEpochProgress = EpochProgress(NonNegLong.unsafeFrom(200L)),
      approvers = List(srcAddr, dstAddr)
    )
    expect(allowSpendImmutable.immutableBytes(sample).fromImmutableBytes[AllowSpend](allowSpendImmutable) == Right(sample))
  }

  test("TokenLock round-trips with all three optional fields absent") {
    val sample = TokenLock(
      source = srcAddr,
      amount = TokenLockAmount(PosLong.unsafeFrom(500L)),
      fee = TokenLockFee(NonNegLong.unsafeFrom(0L)),
      parent = TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(1L)), Hash("1" * 64)),
      currencyId = None,
      unlockEpoch = None,
      replaceTokenLockRef = None
    )
    expect(tokenLockImmutable.immutableBytes(sample).fromImmutableBytes[TokenLock](tokenLockImmutable) == Right(sample))
  }

  test("TokenLock round-trips with all three optional fields present") {
    val sample = TokenLock(
      source = srcAddr,
      amount = TokenLockAmount(PosLong.unsafeFrom(500L)),
      fee = TokenLockFee(NonNegLong.unsafeFrom(3L)),
      parent = TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(1L)), Hash("1" * 64)),
      currencyId = Some(CurrencyId(srcAddr)),
      unlockEpoch = Some(EpochProgress(NonNegLong.unsafeFrom(9_999_999L))),
      replaceTokenLockRef = Some(Hash("2" * 64))
    )
    expect(tokenLockImmutable.immutableBytes(sample).fromImmutableBytes[TokenLock](tokenLockImmutable) == Right(sample))
  }

  test("AllowSpend approvers order is preserved (List, not Set)") {
    val base = AllowSpend(
      source = srcAddr,
      destination = dstAddr,
      currencyId = None,
      amount = SwapAmount(PosLong.unsafeFrom(1L)),
      fee = AllowSpendFee(NonNegLong.unsafeFrom(0L)),
      parent = AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(0L)), Hash("0" * 64)),
      lastValidEpochProgress = EpochProgress(NonNegLong.unsafeFrom(0L)),
      approvers = List(srcAddr, dstAddr)
    )
    val swapped = base.copy(approvers = List(dstAddr, srcAddr))
    expect(allowSpendImmutable.immutableBytes(base) != allowSpendImmutable.immutableBytes(swapped))
  }
}
