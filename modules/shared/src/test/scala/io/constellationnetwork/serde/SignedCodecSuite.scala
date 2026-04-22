package io.constellationnetwork.serde

import cats.data.NonEmptySet

import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.SignedCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.bits.ByteVector
import weaver.FunSuite

/** Round-trip + wire-format tests for `Signed[T]` — the parameterized universal signed-envelope.
  *
  * Two dimensions of coverage:
  *   - Implicit resolution actually produces `Codec[Signed[T]]` for a `T` that has a `Codec[T]` in scope (via the newtype shape derivation
  *     for Balance). This is the first typeclass-dispatch test for the library.
  *   - Wire layout: value bytes then proofs with uint16 count prefix, proofs themselves being `(Id, Signature)` pairs with each hex field
  *     length-prefixed.
  */
object SignedCodecSuite extends FunSuite {

  // Use deliberately short hex strings for the test samples — keeps the byte
  // layout easy to reason about. Real signatures are 64-128 bytes hex.
  private def mkProof(idHex: String, sigHex: String): SignatureProof =
    SignatureProof(Id(Hex(idHex)), Signature(Hex(sigHex)))

  private def sampleBalance: Balance = Balance(NonNegLong.unsafeFrom(1_000L))
  private def sampleProof1: SignatureProof = mkProof("ab", "cd")
  private def sampleProof2: SignatureProof = mkProof("ef", "01")

  private def singleProofSample: Signed[Balance] =
    Signed(sampleBalance, NonEmptySet.of(sampleProof1))

  private def twoProofSample: Signed[Balance] =
    Signed(sampleBalance, NonEmptySet.of(sampleProof1, sampleProof2))

  test("Signed[Balance] round-trips with a single proof") {
    val bytes = singleProofSample.immutableBytes
    val decoded = bytes.fromImmutableBytes[Signed[Balance]]
    expect(decoded == Right(singleProofSample))
  }

  test("Signed[Balance] round-trips with two proofs") {
    val bytes = twoProofSample.immutableBytes
    val decoded = bytes.fromImmutableBytes[Signed[Balance]]
    expect(decoded == Right(twoProofSample))
  }

  test("Signed layout: value bytes first, then uint16 proof count, then sorted proofs") {
    // Balance(1000L) = 0x00000000000003E8 (8 bytes)
    // Then count: 0x0001 (uint16 = 1)
    // Then 1 proof: (Id hex "ab" → prefix 0x0001 + 0xab)
    //                (Signature hex "cd" → prefix 0x0001 + 0xcd)
    val bytes = singleProofSample.immutableBytes
    val expected = ByteVector.fromValidHex(
      "00000000000003e8" + // value: Balance(1000)
        "0001" + // proof count: 1
        "0001ab" + // Id hex: length 1 + byte 0xab
        "0001cd" // Signature hex: length 1 + byte 0xcd
    )
    expect(bytes == expected)
  }

  test("Signed proofs are sorted on encode (NonEmptySet determinism)") {
    // Construct the proofs in REVERSE order; encoding should still sort them.
    val reversed = Signed(sampleBalance, NonEmptySet.of(sampleProof2, sampleProof1))
    val forward = twoProofSample
    // The NonEmptySet itself normalises; the bytes must match regardless of
    // how the set was built.
    expect(reversed.immutableBytes == forward.immutableBytes)
  }

  test("Decoding a zero-proof-count prefix yields SerdeError.ScodecFailure (NES must be non-empty)") {
    // Valid Balance bytes + proof count = 0 → invalid (NonEmptySet cannot be empty).
    val bogus = ByteVector.fromValidHex("00000000000003e8" + "0000")
    val result = bogus.fromImmutableBytes[Signed[Balance]]
    result match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure, got $other")
    }
  }

  test("Codec[Signed[Balance]] resolves via implicit derivation (no explicit codec)") {
    // The point of this test: Codec[Signed[Balance]] should resolve because
    // Codec[Balance] is in scope (via NewtypeLongShapes) and SignedCodec
    // provides the derivation. No explicit `SignedCodec.codecFor(...)` call
    // site needed.
    val codec = scodec.Codec[Signed[Balance]]
    val bits = codec.encode(singleProofSample).require
    val decoded = codec.decodeValue(bits).require
    expect(decoded == singleProofSample)
  }

  test("Nested Signed[Signed[Balance]] resolves through chained derivation") {
    // Signed[Signed[T]] — Codec[Signed[Signed[Balance]]] requires
    // Codec[Signed[Balance]], which itself requires Codec[Balance]. Both
    // derivations should compose automatically.
    val doubleSigned: Signed[Signed[Balance]] =
      Signed(singleProofSample, NonEmptySet.of(sampleProof2))
    val bytes = doubleSigned.immutableBytes
    val decoded = bytes.fromImmutableBytes[Signed[Signed[Balance]]]
    expect(decoded == Right(doubleSigned))
  }
}
