package io.constellationnetwork.node.shared.domain.snapshot.finality

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.ByteBuffer

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityDurableEnvelopeError._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityDurableEnvelopeKind._

import scodec.codecs.{bool, uint8}
import weaver.FunSuite

object FinalityDurableEnvelopeSuite extends FunSuite {

  private val payload: Array[Byte] = Array[Byte](1, 2, 3)
  private val payloadLimit: Long = 1024L
  private val headerBytes: Int = 24

  private def encoded(
    kind: FinalityDurableEnvelopeKind = CoordinatorHead,
    bytes: Array[Byte] = payload,
    maximum: Long = payloadLimit
  ): Array[Byte] =
    FinalityDurableEnvelope.encode(kind, bytes, maximum).fold(throw _, identity)

  private def decoded(
    bytes: Array[Byte],
    expectedKind: FinalityDurableEnvelopeKind = CoordinatorHead,
    maximum: Long = payloadLimit
  ): Either[FinalityDurableEnvelopeError, FinalityDurableEnvelope.Decoded] =
    FinalityDurableEnvelope.decode(new ByteArrayInputStream(bytes), expectedKind, maximum)

  private def isError[A <: FinalityDurableEnvelopeError: reflect.ClassTag](
    result: Either[FinalityDurableEnvelopeError, _]
  ): Boolean =
    result.swap.toOption.exists(reflect.classTag[A].runtimeClass.isInstance)

  test("canonical envelope round-trips its exact kind and payload") {
    val result = decoded(encoded())

    expect.all(
      result.toOption.exists(_.kind == CoordinatorHead),
      result.toOption.exists(_.payload.sameElements(payload))
    )
  }

  test("magic, version, known kind, and expected kind are independently enforced") {
    val badMagic = encoded().clone()
    badMagic(0) = (badMagic(0) ^ 0x01).toByte

    val badVersion = encoded().clone()
    ByteBuffer.wrap(badVersion).putInt(8, 2)

    val unknownKind = encoded().clone()
    ByteBuffer.wrap(unknownKind).putInt(12, 255)

    expect.all(
      isError[InvalidMagic](decoded(badMagic)),
      isError[UnsupportedVersion](decoded(badVersion)),
      isError[UnknownKind](decoded(unknownKind)),
      isError[UnexpectedKind](decoded(encoded(CoordinatorHead), EffectOutboxHead))
    )
  }

  test("checksum corruption and trailing bytes fail closed") {
    val badChecksum = encoded().clone()
    badChecksum(badChecksum.length - 1) = (badChecksum.last ^ 0x01).toByte
    val withTrailingByte = encoded() ++ Array[Byte](0x7f)

    expect.all(
      isError[ChecksumMismatch.type](decoded(badChecksum)),
      isError[TrailingBytes.type](decoded(withTrailingByte))
    )
  }

  test("truncated header, payload, and checksum identify the incomplete section") {
    val complete = encoded()
    val truncatedHeader = complete.take(10)
    val truncatedPayload = complete.take(headerBytes + payload.length - 1)
    val truncatedChecksum = complete.dropRight(1)

    def section(result: Either[FinalityDurableEnvelopeError, _]): Option[String] =
      result.swap.toOption.collect { case Truncated(value, _, _) => value }

    expect.all(
      section(decoded(truncatedHeader)).contains("header"),
      section(decoded(truncatedPayload)).contains("payload"),
      section(decoded(truncatedChecksum)).contains("checksum")
    )
  }

  test("configured and wire payload bounds reject negative, zero, and overflow values before allocation") {
    val negativeLength = encoded().clone()
    ByteBuffer.wrap(negativeLength).putLong(16, -1L)

    val overflowLength = encoded().clone()
    ByteBuffer.wrap(overflowLength).putLong(16, Long.MaxValue)

    expect.all(
      isError[InvalidLimit](FinalityDurableEnvelope.encode(CoordinatorHead, payload, -1L)),
      isError[InvalidLimit](FinalityDurableEnvelope.encode(CoordinatorHead, payload, 0L)),
      isError[InvalidLimit](FinalityDurableEnvelope.encode(CoordinatorHead, payload, Long.MaxValue)),
      isError[InvalidLimit](decoded(encoded(), maximum = 0L)),
      isError[PayloadTooLarge](FinalityDurableEnvelope.encode(CoordinatorHead, payload, 2L)),
      isError[InvalidPayloadLength](decoded(negativeLength)),
      isError[PayloadTooLarge](decoded(overflowLength))
    )
  }

  test("payload codecs require byte alignment and complete Scodec consumption") {
    expect.all(
      FinalityDurableEnvelope.encodePayload("uint8", uint8, 1).toOption.exists(_.sameElements(Array[Byte](1))),
      FinalityDurableEnvelope.decodePayload("uint8", uint8, Array[Byte](1)).contains(1),
      isError[PayloadDecodingFailed](FinalityDurableEnvelope.decodePayload("uint8", uint8, Array[Byte](1, 2))),
      isError[PayloadDecodingFailed](FinalityDurableEnvelope.decodePayload("uint8", uint8, Array.emptyByteArray)),
      isError[PayloadEncodingFailed](FinalityDurableEnvelope.encodePayload("boolean", bool, true))
    )
  }

  test("zero-length bulk reads cannot truncate or spin a valid stream") {
    val stream = new ZeroBulkReadInputStream(encoded())
    val result = FinalityDurableEnvelope.decode(stream, CoordinatorHead, payloadLimit)

    expect.all(
      result.toOption.exists(_.kind == CoordinatorHead),
      result.toOption.exists(_.payload.sameElements(payload))
    )
  }

  private final class ZeroBulkReadInputStream(bytes: Array[Byte]) extends InputStream {
    private var offset: Int = 0

    override def read(): Int =
      if (offset >= bytes.length) -1
      else {
        val value = bytes(offset) & 0xff
        offset += 1
        value
      }

    override def read(target: Array[Byte], targetOffset: Int, length: Int): Int =
      if (offset >= bytes.length) -1 else 0
  }
}
