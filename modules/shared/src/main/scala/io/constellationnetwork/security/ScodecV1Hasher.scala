package io.constellationnetwork.security

import java.security.{KeyPair, MessageDigest}

import cats.effect.kernel.{Async, Sync}

import io.constellationnetwork.security.signature.signature
import io.constellationnetwork.security.signature.signature.SignatureProof

/** Typed ScodecV1 consensus hasher.
  *
  * The only digest preimage is:
  *
  * `uint8(domainLength) || printableAsciiDomain || immutableSchemaBytes(value)`
  *
  * There is deliberately no untyped byte operation and no Circe, JSON, Kryo, ordinal, or serializer fallback. An `ImmutableCodec` encoding
  * exception is captured by `F`; it fails the digest operation instead of selecting another representation.
  */
final class ScodecV1Hasher[F[_]] private[security] (implicit F: Sync[F]) {

  /** Canonical content identity for one schema-bound value. */
  def contentId[A: ConsensusHashSchema](value: A): F[ConsensusDigest] =
    digest(value)

  /** Signs only the raw 32-byte canonical content identity. */
  def sign[A: ConsensusHashSchema](
    value: A,
    keyPair: KeyPair
  )(implicit async: Async[F], securityProvider: SecurityProvider[F]): F[SignatureProof] =
    async.flatMap(contentId(value))(SignatureProof.fromDigest(keyPair, _))

  /** Verifies a proof against only the raw 32-byte canonical content identity. */
  def verify[A: ConsensusHashSchema](
    value: A,
    proof: SignatureProof
  )(implicit async: Async[F], securityProvider: SecurityProvider[F]): F[Boolean] =
    async.flatMap(contentId(value))(digest => signature.verifySignatureProof(digest, proof))

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
