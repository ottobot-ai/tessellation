package io.constellationnetwork.tools.migration.v4

import cats.data.NonEmptySet
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.json.JsonBrotliBinarySerializer.{BrotliDecodeLimits, CompressedContentTooLarge}
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.signature.{SignatureProof, verifySignatureProof}
import io.constellationnetwork.tools.migration.v4.V4CurrencyJsonEnvelopeVerificationError._
import io.constellationnetwork.tools.migration.v4.V4SourceEncoding.V4BrotliJson

sealed trait V4CurrencyJsonEnvelopeVerificationError extends Product with Serializable

object V4CurrencyJsonEnvelopeVerificationError {
  final case class WrongSourceEra(context: V4SourceContext, required: V4SourceEncoding)
      extends V4CurrencyJsonEnvelopeVerificationError
  case object MissingSourceContext extends V4CurrencyJsonEnvelopeVerificationError
  case object MissingCompressedSource extends V4CurrencyJsonEnvelopeVerificationError
  case object MissingExpectedValueHash extends V4CurrencyJsonEnvelopeVerificationError
  final case class InvalidDecodeLimits(limits: Option[BrotliDecodeLimits]) extends V4CurrencyJsonEnvelopeVerificationError
  final case class DecodeFailure(cause: Throwable) extends V4CurrencyJsonEnvelopeVerificationError
  case object NonCanonicalEnvelopeBytes extends V4CurrencyJsonEnvelopeVerificationError
  final case class OrdinalMismatch(expected: SnapshotOrdinal, actual: SnapshotOrdinal)
      extends V4CurrencyJsonEnvelopeVerificationError
  final case class HashMismatch(expected: Hash, actual: Hash) extends V4CurrencyJsonEnvelopeVerificationError
  final case class InvalidSignatures(proofs: NonEmptySet[SignatureProof]) extends V4CurrencyJsonEnvelopeVerificationError
}

/** Cryptographic stage-1 evidence for a canonical envelope with the frozen upstream-v4 top-level currency key grammar.
  *
  * This result does not claim full recursive v4 schema validity, authorized source operators or threshold, source finality, root validity,
  * or replay. It exposes neither the parsed JSON nor an active runtime type. Full typed migration requires a separately frozen transitive
  * v4 schema graph and an upstream-generated golden corpus. `valueHashPreimageBytes` is the exact Brotli-JSON byte array that
  * `Hasher.forJson` hashes, retained so later stages never need to recompress authority-bearing source data.
  */
final class CryptographicallyValidV4CurrencyJsonEnvelope private (
  private val retainedOriginalCompressedBytes: Array[Byte],
  private val retainedValueHashPreimageBytes: Array[Byte],
  val legacyProofs: NonEmptySet[SignatureProof],
  val valueHash: Hash,
  val context: V4SourceContext
) {
  def originalCompressedBytes: Array[Byte] = retainedOriginalCompressedBytes.clone()
  def valueHashPreimageBytes: Array[Byte] = retainedValueHashPreimageBytes.clone()
}

private object CryptographicallyValidV4CurrencyJsonEnvelope {
  def make(
    retainedOriginalCompressedBytes: Array[Byte],
    retainedValueHashPreimageBytes: Array[Byte],
    envelope: V4CurrencyJsonEnvelope,
    valueHash: Hash,
    context: V4SourceContext
  ): CryptographicallyValidV4CurrencyJsonEnvelope =
    new CryptographicallyValidV4CurrencyJsonEnvelope(
      retainedOriginalCompressedBytes,
      retainedValueHashPreimageBytes,
      envelope.proofs,
      valueHash,
      context
    )
}

object V4CurrencyBrotliJsonEnvelopeVerifier {
  private val MaximumDecodeLimit = Int.MaxValue.toLong

  private def reject[F[_]: Async](
    error: V4CurrencyJsonEnvelopeVerificationError
  ): F[Either[V4CurrencyJsonEnvelopeVerificationError, CryptographicallyValidV4CurrencyJsonEnvelope]] =
    error.asLeft[CryptographicallyValidV4CurrencyJsonEnvelope].pure[F]

  def verify[F[_]: Async: JsonSerializer: SecurityProvider](
    originalCompressedBytes: Array[Byte],
    context: V4SourceContext,
    expectedValueHash: Hash,
    limits: BrotliDecodeLimits
  ): F[Either[V4CurrencyJsonEnvelopeVerificationError, CryptographicallyValidV4CurrencyJsonEnvelope]] =
    Async[F].defer {
      if (context eq null) reject(MissingSourceContext)
      else if (context.encoding != V4BrotliJson) reject(WrongSourceEra(context, V4BrotliJson))
      else if (limits eq null) reject(InvalidDecodeLimits(None))
      else if (
        limits.maxCompressedBytes <= 0L || limits.maxCompressedBytes > MaximumDecodeLimit ||
        limits.maxDecompressedBytes <= 0L || limits.maxDecompressedBytes > MaximumDecodeLimit
      ) reject(InvalidDecodeLimits(Some(limits)))
      else if (originalCompressedBytes eq null) reject(MissingCompressedSource)
      else if (expectedValueHash.asInstanceOf[AnyRef] eq null) reject(MissingExpectedValueHash)
      else if (originalCompressedBytes.length.toLong > limits.maxCompressedBytes)
        reject(DecodeFailure(CompressedContentTooLarge(limits.maxCompressedBytes, originalCompressedBytes.length.toLong)))
      else {
        val retainedSourceBytes = originalCompressedBytes.clone()

        JsonSerializer[F]
          .deserializeBounded[V4CurrencyJsonEnvelope](retainedSourceBytes, limits)
          .flatMap {
            case Left(error) => reject(DecodeFailure(error))
            case Right(envelope) if envelope.ordinal =!= context.ordinal =>
              reject(OrdinalMismatch(context.ordinal, envelope.ordinal))
            case Right(envelope) =>
              for {
                canonicalEnvelopeBytes <- JsonSerializer[F].serialize(envelope)
                valueHashPreimageBytes <- JsonSerializer[F].serialize(envelope.value)
                actualHash <- Hash.fromBytesForSync[F](valueHashPreimageBytes)
                result <-
                  if (!canonicalEnvelopeBytes.sameElements(retainedSourceBytes)) reject(NonCanonicalEnvelopeBytes)
                  else if (actualHash =!= expectedValueHash) reject(HashMismatch(expectedValueHash, actualHash))
                  else
                    envelope.proofs.toNonEmptyList
                      .traverse(proof => verifySignatureProof(actualHash, proof).map(proof -> _))
                      .map { checked =>
                        NonEmptySet
                          .fromSet(scala.collection.immutable.SortedSet.from(checked.collect { case (proof, false) => proof }))
                          .fold[Either[V4CurrencyJsonEnvelopeVerificationError, CryptographicallyValidV4CurrencyJsonEnvelope]](
                            CryptographicallyValidV4CurrencyJsonEnvelope
                              .make(retainedSourceBytes, valueHashPreimageBytes, envelope, actualHash, context)
                              .asRight[V4CurrencyJsonEnvelopeVerificationError]
                          )(InvalidSignatures(_).asLeft[CryptographicallyValidV4CurrencyJsonEnvelope])
                      }
              } yield result
          }
      }
    }
}
