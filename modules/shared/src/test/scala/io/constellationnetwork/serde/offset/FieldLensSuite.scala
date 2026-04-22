package io.constellationnetwork.serde.offset

import io.constellationnetwork.schema.BlockReference
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.swap.{AllowSpendOrdinal, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock.{TokenLockOrdinal, TokenLockReference}
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.serde.codecs.instances._
import io.constellationnetwork.serde.codecs.offset._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Byte-offset lens suite for the `*Reference` family — all 40 bytes fixed-width. */
object ReferenceLensSuite extends FunSuite {

  private def hashA = "aa" * 32
  private def hashB = "bb" * 32

  // ---- BlockReference -----------------------------------------------------

  test("BlockReferenceLenses read agrees with full decode") {
    val sample = BlockReference(Height(NonNegLong.unsafeFrom(42L)), ProofsHash(hashA))
    val bytes = BlockReferenceCodec.codec.encode(sample).require.toByteVector

    expect(BlockReferenceLenses.height.read(bytes).require == sample.height)
      .and(expect(BlockReferenceLenses.hash.read(bytes).require == sample.hash))
      .and(expect(BlockReferenceLenses.height.offset == 0L))
      .and(expect(BlockReferenceLenses.hash.offset == 8L))
  }

  test("BlockReferenceLenses rejects truncated bytes") {
    val sample = BlockReference(Height(NonNegLong.unsafeFrom(1L)), ProofsHash(hashA))
    val bytes = BlockReferenceCodec.codec.encode(sample).require.toByteVector
    val truncated = bytes.take(20) // only 20 of 40 bytes
    expect(BlockReferenceLenses.hash.read(truncated).isFailure)
  }

  // ---- TransactionReference ----------------------------------------------

  test("TransactionReferenceLenses read agrees with full decode") {
    val sample = TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(99L)), Hash(hashB))
    val bytes = TransactionReferenceCodec.codec.encode(sample).require.toByteVector

    expect(TransactionReferenceLenses.ordinal.read(bytes).require == sample.ordinal)
      .and(expect(TransactionReferenceLenses.hash.read(bytes).require == sample.hash))
  }

  // ---- AllowSpendReference -----------------------------------------------

  test("AllowSpendReferenceLenses read agrees with full decode") {
    val sample = AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(5L)), Hash(hashA))
    val bytes = AllowSpendReferenceCodec.codec.encode(sample).require.toByteVector

    expect(AllowSpendReferenceLenses.ordinal.read(bytes).require == sample.ordinal)
      .and(expect(AllowSpendReferenceLenses.hash.read(bytes).require == sample.hash))
  }

  // ---- TokenLockReference ------------------------------------------------

  test("TokenLockReferenceLenses read agrees with full decode") {
    val sample = TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(123L)), Hash(hashB))
    val bytes = TokenLockReferenceCodec.codec.encode(sample).require.toByteVector

    expect(TokenLockReferenceLenses.ordinal.read(bytes).require == sample.ordinal)
      .and(expect(TokenLockReferenceLenses.hash.read(bytes).require == sample.hash))
  }

  // ---- Structural ---------------------------------------------------------

  test("Reference lenses all produce byte layouts compatible with the 40-byte shape") {
    val a = BlockReferenceCodec.codec.encode(BlockReference(Height(NonNegLong.unsafeFrom(1L)), ProofsHash(hashA))).require.toByteVector
    val b = TransactionReferenceCodec.codec
      .encode(
        TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(1L)), Hash(hashA))
      )
      .require
      .toByteVector
    val c = AllowSpendReferenceCodec.codec
      .encode(
        AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(1L)), Hash(hashA))
      )
      .require
      .toByteVector
    val d = TokenLockReferenceCodec.codec
      .encode(
        TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(1L)), Hash(hashA))
      )
      .require
      .toByteVector

    // Same numeric inputs → same bytes, across all four reference types.
    expect(a == b).and(expect(b == c)).and(expect(c == d)).and(expect(a.length == 40L))
  }
}
