package io.constellationnetwork.security

import java.security.MessageDigest

import cats.effect.kernel.Sync

/** Typed ScodecV1 consensus hasher.
  *
  * The only digest preimage is:
  *
  * `uint8(domainLength) || printableAsciiDomain || immutableSchemaBytes(value)`
  *
  * There is deliberately no untyped byte operation and no Circe, JSON, Kryo,
  * ordinal, or serializer fallback. An `ImmutableCodec` encoding exception is
  * captured by `F`; it fails the digest operation instead of selecting another
  * representation.
  */
final class ScodecV1Hasher[F[_]] private[security] (implicit F: Sync[F]) {

  def digest[A: ConsensusHashSchema](value: A): F[ConsensusDigest] =
    F.delay {
      val schema = ConsensusHashSchema[A]
      val encoded = schema.codec.immutableBytes(value)

      if (encoded.length > schema.maxEncodedBytes)
        throw ScodecV1Hasher.EncodedValueTooLarge(encoded.length, schema.maxEncodedBytes)

      val preimage = schema.lengthDelimitedDomain ++ encoded
      val sha256 = MessageDigest.getInstance("SHA-256")

      ConsensusDigest
        .fromByteArray(sha256.digest(preimage.toArray))
        .fold(error => throw new IllegalStateException(error.message), identity)
    }
}

object ScodecV1Hasher {

  final case class EncodedValueTooLarge(actualBytes: Long, maximumBytes: Long)
      extends IllegalArgumentException(s"Consensus value encoded to $actualBytes bytes, exceeding schema maximum $maximumBytes")
}
