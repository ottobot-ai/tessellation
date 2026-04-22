package io.constellationnetwork.serde

import io.constellationnetwork.schema.StardustCollective
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.AddressCodec._
import io.constellationnetwork.serde.implicits._

import scodec.bits.ByteVector
import weaver.FunSuite

/** Golden + round-trip + validator-rejection tests for `Address`.
  *
  * `DAGAddressRefined` is a non-trivial refiner: length=40, `DAG` prefix, digit-sum parity, base58 suffix, plus the Stardust Collective
  * whitelist exception. On decode the raw bytes are re-routed through `refineV` — malformed bytes must produce `SerdeError.ScodecFailure`,
  * not silent acceptance.
  */
object AddressCodecSuite extends FunSuite {

  // A known-valid 40-char DAG address (genesis.csv uses it).
  private val normalAddrLiteral = "DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt"
  private def normalSample = AddressCodec.unsafeFromLiteral(normalAddrLiteral)
  private def normalGolden = GoldenVectors.load("Address-scodec-v1")

  private def stardustSample = AddressCodec.unsafeFromLiteral(StardustCollective.address)

  test("normal 40-char DAG address encodes to golden bytes") {
    expect(normalSample.immutableBytes == normalGolden)
  }

  test("normal 40-char DAG address round-trips") {
    val decoded = normalSample.immutableBytes.fromImmutableBytes[io.constellationnetwork.schema.address.Address]
    expect(decoded == Right(normalSample))
  }

  test("normal-address wire format is 1-byte length prefix + 40 ASCII bytes") {
    val bytes = normalSample.immutableBytes
    expect(bytes.length == 41L)
      .and(expect(bytes.head == 0x28.toByte))
      .and(expect(new String(bytes.tail.toArray, "US-ASCII") == normalAddrLiteral))
  }

  test("Stardust Collective 51-char address round-trips") {
    val bytes = stardustSample.immutableBytes
    val decoded = bytes.fromImmutableBytes[io.constellationnetwork.schema.address.Address]
    expect(decoded == Right(stardustSample)).and(expect(bytes.length == 52L)) // 1 length byte + 51 ASCII bytes
  }

  test("decoding bytes that ASCII-parse but fail the refiner yields SerdeError.ScodecFailure") {
    // Build a length-prefixed ASCII string that's 40 chars but doesn't pass the
    // DAG-prefix + parity check. "XYZ...DAGxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    // length-prefix 0x28 + 40 ASCII chars not starting with "DAG".
    val bogusAscii = "XYZ4" + "0" * 36 // 40 chars, wrong prefix
    val bogusBytes = ByteVector(0x28.toByte) ++ ByteVector(bogusAscii.getBytes("US-ASCII"))
    val result = bogusBytes.fromImmutableBytes[io.constellationnetwork.schema.address.Address]
    result match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure on bad address, got $other")
    }
  }

  test("decoding a length-prefix of 0 yields a ScodecFailure (empty string fails refiner)") {
    val emptyPrefixed = ByteVector(0x00.toByte)
    val result = emptyPrefixed.fromImmutableBytes[io.constellationnetwork.schema.address.Address]
    result match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure on empty payload, got $other")
    }
  }

  test("decoding truncated bytes (prefix says 40, only 10 provided) yields ScodecFailure") {
    val truncated = ByteVector(0x28.toByte) ++ ByteVector("tooShort".getBytes("US-ASCII"))
    val result = truncated.fromImmutableBytes[io.constellationnetwork.schema.address.Address]
    result match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure on truncated input, got $other")
    }
  }

  test("encoded bytes are plain ASCII payload after the length prefix (not UTF-8 BOM)") {
    val bytes = normalSample.immutableBytes
    val payloadRange = bytes.drop(1)
    // Every byte in a valid DAG address is ASCII (< 0x80); no UTF-8 continuation
    // bytes should ever appear.
    expect(payloadRange.toArray.forall(b => (b & 0x80) == 0))
  }
}
