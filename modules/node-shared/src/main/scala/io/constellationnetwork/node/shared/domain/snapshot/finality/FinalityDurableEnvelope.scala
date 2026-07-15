package io.constellationnetwork.node.shared.domain.snapshot.finality

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Arrays

import scala.util.control.NonFatal

import scodec.bits.BitVector
import scodec.{Attempt, Codec}

sealed trait FinalityDurableEnvelopeKind extends Product with Serializable {
  def tag: Int
  def label: String
}

object FinalityDurableEnvelopeKind {
  case object CoordinatorHead extends FinalityDurableEnvelopeKind {
    val tag: Int = 1
    val label: String = "coordinator-head"
  }

  case object EffectOutboxHead extends FinalityDurableEnvelopeKind {
    val tag: Int = 2
    val label: String = "effect-outbox-head"
  }

  case object ImmutableArtifact extends FinalityDurableEnvelopeKind {
    val tag: Int = 3
    val label: String = "immutable-artifact"
  }

  case object EffectManifest extends FinalityDurableEnvelopeKind {
    val tag: Int = 4
    val label: String = "effect-manifest"
  }

  case object AuditRecord extends FinalityDurableEnvelopeKind {
    val tag: Int = 5
    val label: String = "audit-record"
  }

  case object RecoveryRecord extends FinalityDurableEnvelopeKind {
    val tag: Int = 6
    val label: String = "recovery-record"
  }

  case object EffectReceipt extends FinalityDurableEnvelopeKind {
    val tag: Int = 7
    val label: String = "effect-receipt"
  }

  case object InitializationMarker extends FinalityDurableEnvelopeKind {
    val tag: Int = 8
    val label: String = "initialization-marker"
  }

  val all: List[FinalityDurableEnvelopeKind] =
    List(
      CoordinatorHead,
      EffectOutboxHead,
      ImmutableArtifact,
      EffectManifest,
      AuditRecord,
      RecoveryRecord,
      EffectReceipt,
      InitializationMarker
    )

  def fromTag(tag: Int): Option[FinalityDurableEnvelopeKind] = all.find(_.tag == tag)
}

sealed abstract class FinalityDurableEnvelopeError(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

object FinalityDurableEnvelopeError {
  final case class InvalidLimit(maxPayloadBytes: Long)
      extends FinalityDurableEnvelopeError(
        s"Finality envelope payload bound must fit the canonical in-memory envelope, got $maxPayloadBytes"
      )

  final case class PayloadTooLarge(maximum: Long, actual: Long)
      extends FinalityDurableEnvelopeError(s"Finality envelope payload exceeds configured bound: maximum=$maximum actual=$actual")

  final case class InvalidMagic(observed: Array[Byte])
      extends FinalityDurableEnvelopeError(s"Invalid finality envelope magic: ${toHex(observed)}")

  final case class UnsupportedVersion(observed: Int)
      extends FinalityDurableEnvelopeError(s"Unsupported finality envelope format version: $observed")

  final case class UnknownKind(observed: Int) extends FinalityDurableEnvelopeError(s"Unknown finality envelope kind tag: $observed")

  final case class UnexpectedKind(expected: FinalityDurableEnvelopeKind, actual: FinalityDurableEnvelopeKind)
      extends FinalityDurableEnvelopeError(s"Unexpected finality envelope kind: expected=${expected.label} actual=${actual.label}")

  final case class InvalidPayloadLength(observed: Long)
      extends FinalityDurableEnvelopeError(s"Invalid finality envelope payload length: $observed")

  final case class Truncated(section: String, expected: Long, actual: Long)
      extends FinalityDurableEnvelopeError(
        s"Truncated finality envelope $section: expected=$expected actual=$actual"
      )

  case object TrailingBytes extends FinalityDurableEnvelopeError("Finality envelope contains trailing bytes")

  case object ChecksumMismatch extends FinalityDurableEnvelopeError("Finality envelope checksum mismatch")

  final case class PayloadEncodingFailed(valueType: String, detail: String)
      extends FinalityDurableEnvelopeError(s"Unable to encode $valueType as a complete ScodecV1 payload: $detail")

  final case class PayloadDecodingFailed(valueType: String, detail: String)
      extends FinalityDurableEnvelopeError(s"Unable to decode $valueType as a complete ScodecV1 payload: $detail")

  final case class IoFailure(detail: String, cause0: Throwable)
      extends FinalityDurableEnvelopeError(s"Unable to read finality envelope: $detail", cause0)

  private def toHex(bytes: Array[Byte]): String = bytes.iterator.map(byte => f"${byte & 0xff}%02x").mkString
}

/** Canonical local durability envelope for the greenfield ScodecV1 finality store.
  *
  * The checksum covers magic, format version, kind, payload length, and payload. Decoding is exact: bounded payloads, complete Scodec
  * consumption, and EOF after the checksum are all mandatory.
  */
object FinalityDurableEnvelope {
  import FinalityDurableEnvelopeError._

  private val Magic: Array[Byte] = "TSFNSCV1".getBytes(StandardCharsets.US_ASCII)
  private val FormatVersion: Int = 1
  private val ChecksumBytes: Int = 32
  private val HeaderBytes: Int = Magic.length + java.lang.Integer.BYTES * 2 + java.lang.Long.BYTES
  val MaxPayloadBytes: Int = Int.MaxValue - HeaderBytes - ChecksumBytes

  final case class Decoded(kind: FinalityDurableEnvelopeKind, payload: Array[Byte])

  def encode(
    kind: FinalityDurableEnvelopeKind,
    payload: Array[Byte],
    maxPayloadBytes: Long
  ): Either[FinalityDurableEnvelopeError, Array[Byte]] =
    for {
      _ <- validateLimit(maxPayloadBytes)
      _ <- Either.cond(payload.length.toLong <= maxPayloadBytes, (), PayloadTooLarge(maxPayloadBytes, payload.length.toLong))
    } yield {
      val body = new ByteArrayOutputStream(HeaderBytes + payload.length)
      val output = new DataOutputStream(body)
      output.write(Magic)
      output.writeInt(FormatVersion)
      output.writeInt(kind.tag)
      output.writeLong(payload.length.toLong)
      output.write(payload)
      output.flush()

      val bodyBytes = body.toByteArray
      val checksum = sha256(bodyBytes)
      val envelope = Arrays.copyOf(bodyBytes, bodyBytes.length + ChecksumBytes)
      System.arraycopy(checksum, 0, envelope, bodyBytes.length, ChecksumBytes)
      envelope
    }

  def decode(
    input: InputStream,
    expectedKind: FinalityDurableEnvelopeKind,
    maxPayloadBytes: Long
  ): Either[FinalityDurableEnvelopeError, Decoded] =
    try
      for {
        _ <- validateLimit(maxPayloadBytes)
        header <- readExact(input, HeaderBytes, "header")
        parsed <- parseHeader(header, expectedKind, maxPayloadBytes)
        payload <- readExact(input, parsed._2.toInt, "payload")
        expectedChecksum <- readExact(input, ChecksumBytes, "checksum")
        _ <- Either.cond(input.read() == -1, (), TrailingBytes)
        actualChecksum = sha256(concat(header, payload))
        _ <- Either.cond(MessageDigest.isEqual(expectedChecksum, actualChecksum), (), ChecksumMismatch)
      } yield Decoded(parsed._1, payload)
    catch {
      case error: FinalityDurableEnvelopeError => Left(error)
      case _: EOFException                     => Left(Truncated("stream", 1L, 0L))
      case NonFatal(error)                     => Left(IoFailure(error.getMessage, error))
    }

  def encodePayload[A](valueType: String, codec: Codec[A], value: A): Either[FinalityDurableEnvelopeError, Array[Byte]] =
    codec.encode(value) match {
      case Attempt.Successful(bits) if bits.size % 8L == 0L => Right(bits.toByteArray)
      case Attempt.Successful(bits) =>
        Left(PayloadEncodingFailed(valueType, s"non-byte-aligned payload of ${bits.size} bits"))
      case Attempt.Failure(error) => Left(PayloadEncodingFailed(valueType, error.messageWithContext))
    }

  def decodePayload[A](valueType: String, codec: Codec[A], payload: Array[Byte]): Either[FinalityDurableEnvelopeError, A] =
    codec.decode(BitVector(payload)) match {
      case Attempt.Successful(result) if result.remainder.isEmpty => Right(result.value)
      case Attempt.Successful(result) =>
        Left(PayloadDecodingFailed(valueType, s"${result.remainder.size} trailing bits"))
      case Attempt.Failure(error) => Left(PayloadDecodingFailed(valueType, error.messageWithContext))
    }

  def payloadDigest(payload: Array[Byte]): Array[Byte] = sha256(payload)

  private def parseHeader(
    header: Array[Byte],
    expectedKind: FinalityDurableEnvelopeKind,
    maxPayloadBytes: Long
  ): Either[FinalityDurableEnvelopeError, (FinalityDurableEnvelopeKind, Long)] = {
    val buffer = ByteBuffer.wrap(header)
    val magic = new Array[Byte](Magic.length)
    buffer.get(magic)
    val version = buffer.getInt()
    val kindTag = buffer.getInt()
    val payloadLength = buffer.getLong()

    for {
      _ <- Either.cond(Arrays.equals(magic, Magic), (), InvalidMagic(magic))
      _ <- Either.cond(version == FormatVersion, (), UnsupportedVersion(version))
      kind <- FinalityDurableEnvelopeKind.fromTag(kindTag).toRight(UnknownKind(kindTag))
      _ <- Either.cond(kind == expectedKind, (), UnexpectedKind(expectedKind, kind))
      _ <- Either.cond(payloadLength >= 0L, (), InvalidPayloadLength(payloadLength))
      _ <- Either.cond(payloadLength <= maxPayloadBytes, (), PayloadTooLarge(maxPayloadBytes, payloadLength))
      _ <- Either.cond(payloadLength <= Int.MaxValue.toLong, (), InvalidPayloadLength(payloadLength))
    } yield kind -> payloadLength
  }

  private def validateLimit(maxPayloadBytes: Long): Either[FinalityDurableEnvelopeError, Unit] =
    Either.cond(
      maxPayloadBytes >= 1L && maxPayloadBytes <= MaxPayloadBytes.toLong,
      (),
      InvalidLimit(maxPayloadBytes)
    )

  private def readExact(
    input: InputStream,
    length: Int,
    section: String
  ): Either[FinalityDurableEnvelopeError, Array[Byte]] = {
    val bytes = new Array[Byte](length)
    var offset = 0
    while (offset < length) {
      val read = input.read(bytes, offset, length - offset)
      if (read < 0) return Left(Truncated(section, length.toLong, offset.toLong))
      if (read == 0) {
        val single = input.read()
        if (single < 0) return Left(Truncated(section, length.toLong, offset.toLong))
        bytes(offset) = single.toByte
        offset += 1
      } else offset += read
    }
    Right(bytes)
  }

  private def sha256(bytes: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(bytes)

  private def concat(left: Array[Byte], right: Array[Byte]): Array[Byte] = {
    val result = Arrays.copyOf(left, left.length + right.length)
    System.arraycopy(right, 0, result, left.length, right.length)
    result
  }
}
