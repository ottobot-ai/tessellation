package io.constellationnetwork.security

import java.nio.charset.StandardCharsets

import io.constellationnetwork.serde.ImmutableCodec

import scodec.bits.ByteVector

/** Audited ScodecV1 hashing capability for one consensus type.
  *
  * A schema binds the exact immutable codec, static domain frame, and maximum encoded value size as one capability. Digest callers cannot
  * independently select or override those components. Production schema capabilities still require a source allowlist because an
  * in-repository caller could shadow the complete implicit capability.
  *
  * Network, genesis, era, parameter, parent, and other exact runtime context belongs in the versioned value `A`, not in the static domain.
  */
final class ConsensusHashSchema[A] private (
  private[security] val codec: ImmutableCodec[A],
  private[security] val lengthDelimitedDomain: ByteVector,
  private[security] val maxEncodedBytes: Long
)

object ConsensusHashSchema {

  val MaxDomainLength: Int = 255
  private val CanonicalDomain =
    """tessellation/[a-z0-9]+(?:-[a-z0-9]+)*(?:/[a-z0-9]+(?:-[a-z0-9]+)*)*/v[1-9][0-9]*""".r

  sealed abstract class ValidationError(val message: String) extends Product with Serializable

  case object NullCodec extends ValidationError("Consensus hash schema codec cannot be null")

  case object NullDomain extends ValidationError("Consensus hash domain cannot be null")

  case object EmptyDomain extends ValidationError("Consensus hash domain cannot be empty")

  final case class DomainTooLong(actualLength: Int)
      extends ValidationError(s"Consensus hash domain length must be at most $MaxDomainLength bytes, got $actualLength")

  final case class NonPrintableAscii(index: Int, value: Int)
      extends ValidationError(f"Consensus hash domain contains non-printable ASCII at index $index: 0x$value%04x")

  final case class NonCanonicalDomain(value: String)
      extends ValidationError(
        s"Consensus hash domain must match tessellation/<lowercase-segments>/vN with N >= 1, got $value"
      )

  final case class InvalidMaxEncodedBytes(value: Long)
      extends ValidationError(s"Consensus hash schema maximum encoded byte count must be positive, got $value")

  def apply[A](implicit schema: ConsensusHashSchema[A]): ConsensusHashSchema[A] = schema

  /** Validated schema construction is intentionally restricted to reviewed schema inventories in the security package. There is no public
    * unsafe domain or generic derivation API.
    */
  private[security] def make[A](
    codec: ImmutableCodec[A],
    domain: String,
    maxEncodedBytes: Long
  ): Either[ValidationError, ConsensusHashSchema[A]] =
    if (codec eq null) Left(NullCodec)
    else if (domain eq null) Left(NullDomain)
    else if (domain.isEmpty) Left(EmptyDomain)
    else if (domain.length > MaxDomainLength) Left(DomainTooLong(domain.length))
    else if (maxEncodedBytes <= 0L) Left(InvalidMaxEncodedBytes(maxEncodedBytes))
    else
      domain.indexWhere(character => character < 0x20 || character > 0x7e) match {
        case index if index >= 0 =>
          Left(NonPrintableAscii(index, domain.charAt(index).toInt))
        case _ if !CanonicalDomain.pattern.matcher(domain).matches() =>
          Left(NonCanonicalDomain(domain))
        case _ =>
          val bytes = ByteVector.view(domain.getBytes(StandardCharsets.US_ASCII))
          val framed = ByteVector(bytes.length.toByte) ++ bytes
          Right(new ConsensusHashSchema[A](codec, framed, maxEncodedBytes))
      }
}
