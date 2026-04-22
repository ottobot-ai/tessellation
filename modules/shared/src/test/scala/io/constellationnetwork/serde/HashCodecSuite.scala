package io.constellationnetwork.serde

import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.HashCodec._
import io.constellationnetwork.serde.implicits._

import scodec.bits.ByteVector
import weaver.FunSuite

/** Golden + round-trip suite for Hash. Wire layout: 32 raw bytes, fixed-width.
  *
  * The legacy JSON / Kryo eras travelled Hash over the wire as 64 ASCII hex chars (64 bytes). The scodec era uses the raw 32-byte digest.
  * Legacy bytes are still decodable via `legacy.JsonBridge` / `legacy.KryoBridge`; scodec-era writes produce only the 32-byte form.
  */
object HashCodecSuite extends FunSuite {

  // Hash.empty is `"0" * 64` — 64 zero hex chars → 32 zero bytes on the wire.
  private def emptySample: Hash = Hash.empty
  private def emptyGolden = GoldenVectors.load("Hash-scodec-v1")

  // Non-zero sample: use a recognisable hex pattern.
  private def sampleHex: String = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  private def sample: Hash = Hash(sampleHex)

  test("empty Hash encodes to 32 zero bytes (golden)") {
    expect(emptySample.immutableBytes == emptyGolden)
  }

  test("empty Hash decodes golden bytes back to Hash.empty") {
    expect(emptyGolden.fromImmutableBytes[Hash] == Right(Hash.empty))
  }

  test("non-zero Hash round-trips through 32-byte encoding") {
    val bytes = sample.immutableBytes
    val decoded = bytes.fromImmutableBytes[Hash]
    expect(decoded == Right(sample)).and(expect(bytes.length == 32L))
  }

  test("every Hash encodes to exactly 32 bytes") {
    val samples = Seq(
      Hash.empty,
      sample,
      Hash("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
      Hash("deadbeef" * 8)
    )
    val lengths = samples.map(_.immutableBytes.length).toSet
    expect(lengths == Set(32L))
  }

  test("encoded bytes equal ByteVector.fromHex(hash.value)") {
    // This is the consensus contract: the 32-byte scodec encoding IS the
    // hex-decoded form of the Scala string value. Anything else breaks
    // interop with external systems that content-address by the same digest.
    val bytes = sample.immutableBytes
    val expected = ByteVector.fromValidHex(sampleHex)
    expect(bytes == expected)
  }

  test("Decoding bytes of wrong length fails") {
    val tooShort = ByteVector.fromValidHex("deadbeef")
    val result = tooShort.fromImmutableBytes[Hash]
    result match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure, got $other")
    }
  }

  test("Encoding a Hash whose string value is malformed fails at encode") {
    // A malformed Hash (non-hex, wrong length) going through the codec should
    // raise an IllegalArgumentException per the `fromScodecCodec` contract —
    // it's a codec-contract violation, not a recoverable decode error.
    val bad = Hash("not-real-hex-just-some-string-that-is-the-wrong-length")
    val threw =
      try { val _ = bad.immutableBytes; false }
      catch { case _: IllegalArgumentException => true }
    expect(threw)
  }
}
