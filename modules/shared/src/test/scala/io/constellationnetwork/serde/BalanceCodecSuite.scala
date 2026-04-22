package io.constellationnetwork.serde

import scala.util.Random

import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.serde.codecs.instances.BalanceCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** First consensus-type pilot for the scodec era.
  *
  * Tests three invariants:
  *   - round-trip: decode(encode(x)) == x
  *   - canonicality: encode(decode(encode(x))) == encode(x)
  *   - golden byte compatibility: encode(knownValue) == known bytes
  *
  * The golden test is the canary. If a PR changes `Balance` or its codec such that
  * the known-value bytes drift, this test fails and the change must be made
  * explicit (via a new type `BalanceV2` or equivalent) rather than silently
  * breaking every historical signature and hash.
  */
object BalanceCodecSuite extends FunSuite {

  // The exact value that produces the bytes in golden/serde/Balance-scodec-v1.hex.
  // 0x0123456789ABCDEFL = 81985529216486895 decimal.
  private val goldenValue: Balance = Balance(NonNegLong.unsafeFrom(0x0123456789abcdefL))
  private val goldenBytes = GoldenVectors.load("Balance-scodec-v1")

  test("Balance encodes to the golden byte vector") {
    val encoded = goldenValue.immutableBytes
    expect(encoded == goldenBytes)
  }

  test("Balance decodes the golden byte vector back to the expected value") {
    val decoded = goldenBytes.fromImmutableBytes[Balance]
    expect(decoded == Right(goldenValue))
  }

  test("Balance round-trips for zero") {
    val zero = Balance(NonNegLong.unsafeFrom(0L))
    val bytes = zero.immutableBytes
    val decoded = bytes.fromImmutableBytes[Balance]
    expect(decoded == Right(zero))
  }

  test("Balance round-trips for Long.MaxValue") {
    val max = Balance(NonNegLong.unsafeFrom(Long.MaxValue))
    val bytes = max.immutableBytes
    val decoded = bytes.fromImmutableBytes[Balance]
    expect(decoded == Right(max))
  }

  test("Balance round-trips for a random sample") {
    val rng = new Random(seed = 12345L)
    val samples = (1 to 50).map { _ =>
      val v = math.abs(rng.nextLong()) // ensures non-negative
      Balance(NonNegLong.unsafeFrom(v))
    }
    val results = samples.map { b =>
      val bytes = b.immutableBytes
      bytes.fromImmutableBytes[Balance]
    }
    expect(results == samples.map(Right(_)))
  }

  test("Balance encoding is canonical (encode ∘ decode ∘ encode == encode)") {
    val bytes1 = goldenValue.immutableBytes
    val bytes2 = bytes1.fromImmutableBytes[Balance].map(_.immutableBytes)
    expect(bytes2 == Right(bytes1))
  }

  test("Decoding negative int64 bytes as Balance fails with ScodecFailure") {
    val negativeLongBytes = scodec.bits.ByteVector.fromValidHex("ffffffffffffffff") // -1 as int64
    val decoded = negativeLongBytes.fromImmutableBytes[Balance]
    decoded match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure, got $other")
    }
  }

  test("Balance bytes are exactly 8 (fixed-width big-endian int64)") {
    val bytes = goldenValue.immutableBytes
    expect(bytes.length == 8L)
  }
}
