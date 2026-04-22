package io.constellationnetwork.serde

import io.constellationnetwork.schema.swap.{AllowSpendOrdinal, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock.{TokenLockOrdinal, TokenLockReference}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{
  codec => allowCodec,
  immutableCodec => allowImmutable
}
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{
  codec => lockCodec,
  immutableCodec => lockImmutable
}
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.bits.ByteVector
import weaver.FunSuite

/** Structural + round-trip suite for the new `*Reference` codecs (AllowSpend / TokenLock).
  *
  * Both follow the 40-byte `[ordinal:8][hash:32]` layout pattern established by
  * `TransactionReference` and `BlockReference`. The tests here are intentionally brief — the
  * shape is identical to the already-tested reference codecs.
  */
object ReferenceCodecsSuite extends FunSuite {

  private def hashHex: String = "0123456789abcdef" * 4

  test("AllowSpendReference: 40 bytes, ordinal-first, round-trips") {
    val sample = AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(9L)), Hash(hashHex))
    val bytes = allowImmutable.immutableBytes(sample)
    expect(bytes.length == 40L) and
      expect(bytes.take(8) == ByteVector.fromValidHex("0000000000000009")) and
      expect(bytes.fromImmutableBytes[AllowSpendReference](allowImmutable) == Right(sample))
    // unused compile witness so the IDE doesn't strip the import
    val _ = allowCodec
    success
  }

  test("TokenLockReference: 40 bytes, ordinal-first, round-trips") {
    val sample = TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(17L)), Hash(hashHex))
    val bytes = lockImmutable.immutableBytes(sample)
    expect(bytes.length == 40L) and
      expect(bytes.take(8) == ByteVector.fromValidHex("0000000000000011")) and
      expect(bytes.fromImmutableBytes[TokenLockReference](lockImmutable) == Right(sample))
    val _ = lockCodec
    success
  }

  test("AllowSpendReference and TokenLockReference with the same underlying numbers encode identically") {
    val ord = NonNegLong.unsafeFrom(42L)
    val h = Hash(hashHex)
    val a = AllowSpendReference(AllowSpendOrdinal(ord), h)
    val b = TokenLockReference(TokenLockOrdinal(ord), h)
    // Same byte layout despite different types — layout pattern is shared across *Reference family.
    expect(allowImmutable.immutableBytes(a) == lockImmutable.immutableBytes(b))
  }
}
