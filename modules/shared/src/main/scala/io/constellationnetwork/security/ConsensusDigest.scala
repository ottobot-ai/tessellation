package io.constellationnetwork.security

import cats.{Eq, Show}

import scodec.bits.ByteVector

/** Validated, immutable raw digest bytes for ScodecV1 consensus operations.
  *
  * This is intentionally not the legacy hexadecimal-string `Hash`. Signatures and verifiers can consume [[toByteVector]] without converting
  * the digest to UTF-8 hexadecimal text.
  */
final class ConsensusDigest private (private val value: ByteVector) {

  def toByteVector: ByteVector = value

  def toHexString: String = value.toHex

  override def equals(other: Any): Boolean =
    other match {
      case that: ConsensusDigest => value == that.value
      case _                     => false
    }

  override def hashCode(): Int = value.hashCode

  override def toString: String = s"ConsensusDigest(${toHexString})"
}

object ConsensusDigest {

  val Length: Long = 32L

  implicit val eq: Eq[ConsensusDigest] = Eq.fromUniversalEquals
  implicit val show: Show[ConsensusDigest] = Show.show(_.toString)

  sealed abstract class ValidationError(val message: String) extends Product with Serializable

  case object NullDigest extends ValidationError("Consensus digest cannot be null")

  final case class InvalidLength(actualLength: Long)
      extends ValidationError(s"Consensus digest must contain exactly $Length bytes, got $actualLength")

  def fromByteVector(value: ByteVector): Either[ValidationError, ConsensusDigest] =
    if (value eq null) Left(NullDigest)
    else if (value.length != Length) Left(InvalidLength(value.length))
    else {
      val copied = ByteVector.view(value.toArray.clone())
      Right(new ConsensusDigest(copied))
    }

  def fromByteArray(value: Array[Byte]): Either[ValidationError, ConsensusDigest] =
    if (value eq null) Left(NullDigest)
    else fromByteVector(ByteVector.view(value))
}
