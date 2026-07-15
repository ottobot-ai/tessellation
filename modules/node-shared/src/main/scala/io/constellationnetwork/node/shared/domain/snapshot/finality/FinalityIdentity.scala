package io.constellationnetwork.node.shared.domain.snapshot.finality

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityBaseCodecs._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoordinatorCodecs.{
  coordinatorAuditRecordPayloadCodec,
  recoveryRecordPayloadCodec
}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoreCodecs.intentScopePayloadCodec
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityEffectCodecs.{
  effectCommandIdentityCodec,
  finalityEffectManifestPayloadCodec
}
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptActivePublication

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.bits.ByteVector
import scodec.{Attempt, Codec}

sealed abstract class FinalityIdentityError(message: String) extends RuntimeException(message)

object FinalityIdentityError {
  final case class EncodingFailed(valueType: String, detail: String)
      extends FinalityIdentityError(s"Unable to encode $valueType canonically: $detail")

  final case class MalformedHash(label: String, value: Hash)
      extends FinalityIdentityError(s"$label is not a canonical 32-byte hash: ${value.value}")

  final case class ArtifactPointerMismatch(expected: ImmutableArtifactPointer, actual: ImmutableArtifactPointer)
      extends FinalityIdentityError(s"Artifact pointer mismatch: expected=$expected actual=$actual")

  final case class EffectManifestPointerMismatch(expected: EffectManifestPointer, actual: EffectManifestPointer)
      extends FinalityIdentityError(s"Effect-manifest pointer mismatch: expected=$expected actual=$actual")

  final case class AuditPointerMismatch(expected: AuditPointer, actual: AuditPointer)
      extends FinalityIdentityError(s"Audit pointer mismatch: expected=$expected actual=$actual")

  final case class RecoveryPointerMismatch(expected: RecoveryRecordPointer, actual: RecoveryRecordPointer)
      extends FinalityIdentityError(s"Recovery pointer mismatch: expected=$expected actual=$actual")

  final case class EmptyArtifact(kind: FinalityArtifactKind)
      extends FinalityIdentityError(s"Finality artifact payload must be nonempty: kind=$kind")

  case object PathEntryCountExhausted extends FinalityIdentityError("Path entry count exhausted the non-negative Long range")
}

/** Canonical, domain-separated identities for the greenfield ScodecV1 finality durability layer.
  *
  * Every identity is derived from complete Scodec payload bytes. Domain and field lengths are included before their bytes, so concatenation
  * ambiguity cannot produce the same preimage. This object does not decide finality or canonicality.
  */
object FinalityIdentity {
  import FinalityIdentityError._

  private val IntentDomain = "tessellation/finality/v1/intent"
  private val ArtifactEncodingDomain = "tessellation/finality/v1/scodec-encoding"
  private val ArtifactIdDomain = "tessellation/finality/v1/artifact-id"
  private val EffectManifestIdDomain = "tessellation/finality/v1/effect-manifest-id"
  private val EffectIdDomain = "tessellation/finality/v1/effect-id"
  private val EffectIdempotencyDomain = "tessellation/finality/v1/effect-idempotency-key"
  private val AuditIdDomain = "tessellation/finality/v1/audit-id"
  private val RecoveryIdDomain = "tessellation/finality/v1/recovery-id"
  private val PublicationMismatchDomain = "tessellation/finality/v1/startup-publication-mismatch"
  private val TransitionDomain = "tessellation/finality/v1/transition"
  private val PathSeedDomain = "tessellation/finality/v1/path-seed"
  private val PathEntryDomain = "tessellation/finality/v1/path-entry"
  private val PathFinalDomain = "tessellation/finality/v1/path-final"

  val ScodecV1Encoding: ArtifactEncoding = ArtifactEncoding(domainHash(ArtifactEncodingDomain))

  def intentId(scope: IntentScope): Either[FinalityIdentityError, IntentId] =
    encode("IntentScope", intentScopePayloadCodec, scope).map(bytes => IntentId(domainHash(IntentDomain, bytes)))

  def transitionDigest(shape: TransitionShape): Either[FinalityIdentityError, Hash] =
    encode("TransitionShape", transitionShapeCodec, shape).map(bytes => domainHash(TransitionDomain, bytes))

  /** Derive the only valid identity for an exact durable sink command. */
  def effectId(identity: EffectCommandIdentity): Either[FinalityIdentityError, EffectId] =
    encode("EffectCommandIdentity", effectCommandIdentityCodec, identity)
      .map(bytes => EffectId(domainHash(EffectIdDomain, bytes)))

  /** Local sink retry key derived from, and therefore no more caller-controlled than, the effect ID. */
  def effectIdempotencyKey(effectId: EffectId): Either[FinalityIdentityError, EffectIdempotencyKey] =
    rawHash("effect id", effectId.value)
      .map(bytes => EffectIdempotencyKey(domainHash(EffectIdempotencyDomain, bytes)))

  def artifactPointer[A](
    kind: FinalityArtifactKind,
    codec: Codec[A],
    value: A
  ): Either[FinalityIdentityError, ImmutableArtifactPointer] =
    encode(kind.toString, codec, value).flatMap(bytes => artifactPointerFromBytes(kind, bytes))

  def artifactPointerFromBytes(
    kind: FinalityArtifactKind,
    bytes: ByteVector
  ): Either[FinalityIdentityError, ImmutableArtifactPointer] =
    artifactPointerFromBytes(kind, ScodecV1Encoding, bytes)

  /** Derive a pointer for externally encoded evidence or payload bytes.
    *
    * The encoding identifier must be fixed by the artifact's verifier; it is committed into the ID and cannot be swapped after hashing.
    */
  def artifactPointerFromBytes(
    kind: FinalityArtifactKind,
    encoding: ArtifactEncoding,
    bytes: ByteVector
  ): Either[FinalityIdentityError, ImmutableArtifactPointer] =
    for {
      _ <- Either.cond(bytes.nonEmpty, (), EmptyArtifact(kind))
      digest = ArtifactDigest(Hash.fromBytes(bytes.toArray))
      byteLength = NonNegLong.unsafeFrom(bytes.length)
      kindBytes <- encode("FinalityArtifactKind", finalityArtifactKindCodec, kind)
      encodingBytes <- rawHash("artifact encoding", encoding.value)
      digestBytes <- rawHash("artifact digest", digest.value)
      id = ArtifactId(
        domainHash(ArtifactIdDomain, kindBytes, encodingBytes, digestBytes, longBytes(byteLength.value))
      )
    } yield ImmutableArtifactPointer(kind, encoding, id, digest, byteLength)

  def verifyArtifactPointer[A](
    pointer: ImmutableArtifactPointer,
    kind: FinalityArtifactKind,
    codec: Codec[A],
    value: A
  ): Either[FinalityIdentityError, Unit] =
    artifactPointer(kind, codec, value).flatMap { expected =>
      Either.cond(expected == pointer, (), ArtifactPointerMismatch(expected, pointer))
    }

  def effectManifestPointer(manifest: FinalityEffectManifest): Either[FinalityIdentityError, EffectManifestPointer] =
    encode("FinalityEffectManifest", finalityEffectManifestPayloadCodec, manifest).flatMap { bytes =>
      val digest = EffectManifestDigest(Hash.fromBytes(bytes.toArray))
      rawHash("effect manifest digest", digest.value).map { digestBytes =>
        val id = EffectManifestId(
          domainHash(
            EffectManifestIdDomain,
            longBytes(manifest.scope.generation.value.value),
            digestBytes,
            longBytes(bytes.length)
          )
        )
        EffectManifestPointer(manifest.scope.generation, id, digest)
      }
    }

  def verifyEffectManifestPointer(
    pointer: EffectManifestPointer,
    manifest: FinalityEffectManifest
  ): Either[FinalityIdentityError, Unit] =
    effectManifestPointer(manifest).flatMap { expected =>
      Either.cond(expected == pointer, (), EffectManifestPointerMismatch(expected, pointer))
    }

  def auditPointer(record: CoordinatorAuditRecord): Either[FinalityIdentityError, AuditPointer] =
    encode("CoordinatorAuditRecord", coordinatorAuditRecordPayloadCodec, record).flatMap { bytes =>
      val digest = AuditRecordDigest(Hash.fromBytes(bytes.toArray))
      rawHash("audit digest", digest.value).map { digestBytes =>
        val id = AuditRecordId(
          domainHash(AuditIdDomain, longBytes(record.after.revision.value.value), digestBytes, longBytes(bytes.length))
        )
        AuditPointer(id, digest)
      }
    }

  def verifyAuditPointer(
    pointer: AuditPointer,
    record: CoordinatorAuditRecord
  ): Either[FinalityIdentityError, Unit] =
    auditPointer(record).flatMap { expected =>
      Either.cond(expected == pointer, (), AuditPointerMismatch(expected, pointer))
    }

  def recoveryPointer(record: RecoveryRecord): Either[FinalityIdentityError, RecoveryRecordPointer] =
    encode("RecoveryRecord", recoveryRecordPayloadCodec, record).flatMap { bytes =>
      val digest = RecoveryRecordDigest(Hash.fromBytes(bytes.toArray))
      rawHash("recovery digest", digest.value).map { digestBytes =>
        val id = RecoveryRecordId(
          domainHash(RecoveryIdDomain, longBytes(record.enteredAt.value.value), digestBytes, longBytes(bytes.length))
        )
        RecoveryRecordPointer(id, digest)
      }
    }

  def verifyRecoveryPointer(
    pointer: RecoveryRecordPointer,
    record: RecoveryRecord
  ): Either[FinalityIdentityError, Unit] =
    recoveryPointer(record).flatMap { expected =>
      Either.cond(expected == pointer, (), RecoveryPointerMismatch(expected, pointer))
    }

  /** Commit to the complete coordinator and verified-active MPT cursors without ambiguous concatenation. */
  def publicationMismatchDigest(
    coordinatorPublication: MptActivePublication,
    observedPublication: MptActivePublication
  ): Either[FinalityIdentityError, Hash] =
    for {
      coordinatorBytes <- encode("CoordinatorMptActivePublication", mptActivePublicationCodec, coordinatorPublication)
      observedBytes <- encode("ObservedMptActivePublication", mptActivePublicationCodec, observedPublication)
    } yield domainHash(PublicationMismatchDomain, coordinatorBytes, observedBytes)

  /** Streaming accumulator for a framing-independent oldest-to-newest path root. */
  final case class PathEntriesAccumulator private (entryCount: Long, private val rolling: Hash) {
    def append(entry: GlobalSnapshotStateRef): Either[FinalityIdentityError, PathEntriesAccumulator] =
      if (entryCount == Long.MaxValue) Left(PathEntryCountExhausted)
      else
        for {
          rollingBytes <- rawHash("path accumulator", rolling)
          entryBytes <- encode("GlobalSnapshotStateRef", globalSnapshotStateRefCodec, entry)
        } yield PathEntriesAccumulator(entryCount + 1L, domainHash(PathEntryDomain, rollingBytes, entryBytes))

    def root: Either[FinalityIdentityError, Hash] =
      rawHash("path accumulator", rolling).map(bytes => domainHash(PathFinalDomain, longBytes(entryCount), bytes))
  }

  object PathEntriesAccumulator {
    val empty: PathEntriesAccumulator = PathEntriesAccumulator(0L, domainHash(PathSeedDomain))
  }

  def pathEntriesRoot(entries: Iterable[GlobalSnapshotStateRef]): Either[FinalityIdentityError, Hash] =
    entries.iterator
      .foldLeft[Either[FinalityIdentityError, PathEntriesAccumulator]](Right(PathEntriesAccumulator.empty)) {
        case (accumulator, entry) => accumulator.flatMap(_.append(entry))
      }
      .flatMap(_.root)

  private def encode[A](label: String, codec: Codec[A], value: A): Either[FinalityIdentityError, ByteVector] =
    codec.encode(value) match {
      case Attempt.Successful(bits) if bits.bytes.size * 8L == bits.size => Right(bits.toByteVector)
      case Attempt.Successful(bits) => Left(EncodingFailed(label, s"non-byte-aligned payload of ${bits.size} bits"))
      case Attempt.Failure(error)   => Left(EncodingFailed(label, error.messageWithContext))
    }

  private def rawHash(label: String, hash: Hash): Either[FinalityIdentityError, ByteVector] =
    if (
      hash == Hash.empty ||
      hash.value.length != 64 ||
      !hash.value.forall(character => character >= '0' && character <= '9' || character >= 'a' && character <= 'f')
    ) Left(MalformedHash(label, hash))
    else
      ByteVector
        .fromHexDescriptive(hash.value)
        .left
        .map(_ => MalformedHash(label, hash))
        .filterOrElse(_.length == 32L, MalformedHash(label, hash))

  private def longBytes(value: Long): ByteVector =
    ByteVector.view(ByteBuffer.allocate(java.lang.Long.BYTES).putLong(value).array())

  private def domainHash(domain: String, fields: ByteVector*): Hash = {
    val digest = MessageDigest.getInstance("SHA-256")
    val domainBytes = domain.getBytes(StandardCharsets.US_ASCII)

    digest.update(ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(domainBytes.length).array())
    digest.update(domainBytes)
    fields.foreach { field =>
      digest.update(ByteBuffer.allocate(java.lang.Long.BYTES).putLong(field.length).array())
      digest.update(field.toArray)
    }

    Hash(ByteVector.view(digest.digest()).toHex)
  }
}
