package io.constellationnetwork.schema.slashing

import cats.{Eq, Show}

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.hash.Hash

import scodec.bits.ByteVector

/** Half-open committee-exclusion interval `[excludedFromPeriod, eligibleAgainAtPeriod)`.
  *
  * Both endpoints are artifact eta periods. Negative, empty, reversed, and arithmetic-overflow intervals are unrepresentable through the
  * public factories.
  */
final class EtaPeriodExclusionV1 private (
  val excludedFromPeriod: EtaPeriod,
  val eligibleAgainAtPeriod: EtaPeriod
) extends Serializable {

  def excludes(period: EtaPeriod): Boolean =
    (period ne null) &&
      excludedFromPeriod.value <= period.value &&
      period.value < eligibleAgainAtPeriod.value

  override def equals(other: Any): Boolean =
    other match {
      case that: EtaPeriodExclusionV1 =>
        excludedFromPeriod == that.excludedFromPeriod &&
        eligibleAgainAtPeriod == that.eligibleAgainAtPeriod
      case _ => false
    }

  override def hashCode(): Int = (excludedFromPeriod, eligibleAgainAtPeriod).hashCode()
  override def toString: String =
    s"EtaPeriodExclusionV1([${excludedFromPeriod.value},${eligibleAgainAtPeriod.value}))"
}

object EtaPeriodExclusionV1 {

  sealed abstract class ValidationError(val message: String) extends Product with Serializable

  case object NullExcludedFromPeriod extends ValidationError("excludedFromPeriod cannot be null")

  case object NullEligibleAgainAtPeriod extends ValidationError("eligibleAgainAtPeriod cannot be null")

  final case class NegativeExcludedFromPeriod(value: Long) extends ValidationError(s"excludedFromPeriod must be non-negative, got $value")

  final case class NegativeEligibleAgainAtPeriod(value: Long)
      extends ValidationError(s"eligibleAgainAtPeriod must be non-negative, got $value")

  final case class NonIncreasingInterval(excludedFrom: Long, eligibleAgainAt: Long)
      extends ValidationError(
        s"eligibleAgainAtPeriod must be greater than excludedFromPeriod, got [$excludedFrom,$eligibleAgainAt)"
      )

  final case class NonPositivePeriodCount(value: Long) extends ValidationError(s"excluded period count must be positive, got $value")

  final case class PeriodOverflow(excludedFrom: Long, periodCount: Long)
      extends ValidationError(
        s"excluded period interval overflows EtaPeriod: start=$excludedFrom count=$periodCount"
      )

  def from(
    excludedFromPeriod: EtaPeriod,
    eligibleAgainAtPeriod: EtaPeriod
  ): Either[ValidationError, EtaPeriodExclusionV1] =
    if (excludedFromPeriod eq null) Left(NullExcludedFromPeriod)
    else if (eligibleAgainAtPeriod eq null) Left(NullEligibleAgainAtPeriod)
    else if (excludedFromPeriod.value < 0L) Left(NegativeExcludedFromPeriod(excludedFromPeriod.value))
    else if (eligibleAgainAtPeriod.value < 0L) Left(NegativeEligibleAgainAtPeriod(eligibleAgainAtPeriod.value))
    else if (eligibleAgainAtPeriod.value <= excludedFromPeriod.value)
      Left(NonIncreasingInterval(excludedFromPeriod.value, eligibleAgainAtPeriod.value))
    else Right(new EtaPeriodExclusionV1(excludedFromPeriod, eligibleAgainAtPeriod))

  def fromStartAndPeriodCount(
    excludedFromPeriod: EtaPeriod,
    periodCount: Long
  ): Either[ValidationError, EtaPeriodExclusionV1] =
    if (excludedFromPeriod eq null) Left(NullExcludedFromPeriod)
    else if (excludedFromPeriod.value < 0L) Left(NegativeExcludedFromPeriod(excludedFromPeriod.value))
    else if (periodCount <= 0L) Left(NonPositivePeriodCount(periodCount))
    else
      try from(excludedFromPeriod, EtaPeriod(Math.addExact(excludedFromPeriod.value, periodCount)))
      catch {
        case _: ArithmeticException => Left(PeriodOverflow(excludedFromPeriod.value, periodCount))
      }

  implicit val eq: Eq[EtaPeriodExclusionV1] = Eq.fromUniversalEquals
  implicit val show: Show[EtaPeriodExclusionV1] = Show.show(_.toString)
}

/** Opaque fixed-width SHA-256 output committed by a field-34 record.
  *
  * O22 has not frozen the complete `InvalidStateProofEvidenceV1` preimage. Consequently this companion intentionally exposes no operation
  * from evidence to digest and no evidence hash domain. It can only validate already-computed 32-byte values for codec work.
  */
final class InvalidStateProofEvidenceDigestV1 private (private val value: ByteVector) {
  def toByteVector: ByteVector = value
  def toHexString: String = value.toHex

  override def equals(other: Any): Boolean =
    other match {
      case that: InvalidStateProofEvidenceDigestV1 => value == that.value
      case _                                       => false
    }

  override def hashCode(): Int = value.hashCode
  override def toString: String = s"InvalidStateProofEvidenceDigestV1(${value.toHex})"
}

object InvalidStateProofEvidenceDigestV1 {
  val Length: Long = 32L

  sealed abstract class ValidationError(val message: String) extends Product with Serializable
  case object NullDigest extends ValidationError("evidence digest cannot be null")
  final case class InvalidLength(actual: Long) extends ValidationError(s"evidence digest must contain exactly $Length bytes, got $actual")

  def fromByteVector(value: ByteVector): Either[ValidationError, InvalidStateProofEvidenceDigestV1] =
    if (value eq null) Left(NullDigest)
    else if (value.length != Length) Left(InvalidLength(value.length))
    else Right(new InvalidStateProofEvidenceDigestV1(ByteVector.view(value.toArray.clone())))

  implicit val eq: Eq[InvalidStateProofEvidenceDigestV1] = Eq.fromUniversalEquals
  implicit val show: Show[InvalidStateProofEvidenceDigestV1] = Show.show(_.toString)
}

/** Exact candidate logical identity for a field-34 invalid-state-proof record.
  *
  * Its Scodec bytes are recorded by the candidate grammar. Physical-key hashing remains `SchemaOpen`; this type has no digest wrapper, hash
  * domain, or production hash schema.
  */
final class InvalidStateProofSlashLogicalIdV1 private (
  val peerId: PeerId,
  val shardId: ShardId,
  val disputedCheckpointHash: Hash
) extends Serializable {

  override def equals(other: Any): Boolean =
    other match {
      case that: InvalidStateProofSlashLogicalIdV1 =>
        peerId == that.peerId &&
        shardId == that.shardId &&
        disputedCheckpointHash == that.disputedCheckpointHash
      case _ => false
    }

  override def hashCode(): Int = (peerId, shardId, disputedCheckpointHash).hashCode()
  override def toString: String =
    s"InvalidStateProofSlashLogicalIdV1($peerId,$shardId,$disputedCheckpointHash)"
}

object InvalidStateProofSlashLogicalIdV1 {

  val PeerIdHexCharacters: Int = 128

  sealed abstract class ValidationError(val message: String) extends Product with Serializable

  case object NullPeerId extends ValidationError("peerId cannot be null")
  case object NullPeerIdHex extends ValidationError("peerId hex cannot be null")
  final case class InvalidPeerIdHex(value: String)
      extends ValidationError("peerId must contain exactly 128 canonical lowercase hex characters")
  case object NullShardId extends ValidationError("shardId cannot be null")
  final case class NegativeShardId(value: Int) extends ValidationError(s"shardId must be non-negative, got $value")
  case object NullDisputedCheckpointHash extends ValidationError("disputedCheckpointHash cannot be null")
  final case class InvalidDisputedCheckpointHash(value: String)
      extends ValidationError("disputedCheckpointHash must contain exactly 64 canonical lowercase hex characters")
  case object EmptyDisputedCheckpointHash extends ValidationError("disputedCheckpointHash cannot be Hash.empty")

  def from(
    peerId: PeerId,
    shardId: ShardId,
    disputedCheckpointHash: Hash
  ): Either[ValidationError, InvalidStateProofSlashLogicalIdV1] =
    validatePeerId(peerId)
      .flatMap(_ => validateShardId(shardId))
      .flatMap(_ => validateCheckpointHash(disputedCheckpointHash))
      .map(_ => new InvalidStateProofSlashLogicalIdV1(peerId, shardId, disputedCheckpointHash))

  private def validatePeerId(peerId: PeerId): Either[ValidationError, Unit] =
    if (peerId eq null) Left(NullPeerId)
    else if (peerId.value == null) Left(NullPeerIdHex)
    else if (!isCanonicalLowerHex(peerId.value.value, PeerIdHexCharacters))
      Left(InvalidPeerIdHex(peerId.value.value))
    else Right(())

  private def validateShardId(shardId: ShardId): Either[ValidationError, Unit] =
    if (shardId eq null) Left(NullShardId)
    else if (shardId.value.value < 0) Left(NegativeShardId(shardId.value.value))
    else Right(())

  private def validateCheckpointHash(hash: Hash): Either[ValidationError, Unit] =
    if (hash == null) Left(NullDisputedCheckpointHash)
    else if (!isCanonicalLowerHex(hash.value, 64)) Left(InvalidDisputedCheckpointHash(hash.value))
    else if (hash == Hash.empty) Left(EmptyDisputedCheckpointHash)
    else Right(())

  private def isCanonicalLowerHex(value: String, expectedCharacters: Int): Boolean =
    (value ne null) &&
      value.length == expectedCharacters &&
      value.forall(character =>
        (character >= '0' && character <= '9') ||
          (character >= 'a' && character <= 'f')
      )

  implicit val eq: Eq[InvalidStateProofSlashLogicalIdV1] = Eq.fromUniversalEquals
  implicit val show: Show[InvalidStateProofSlashLogicalIdV1] = Show.show(_.toString)
}

/** Closed field-34 record family for the candidate ordinal-zero ScodecV1 launch grammar. */
sealed trait SlashRecordV1 extends Serializable {
  def peerId: PeerId
  def eventOrdinal: SnapshotOrdinal
  def exclusion: EtaPeriodExclusionV1
}

object SlashRecordV1 {
  implicit val eq: Eq[SlashRecordV1] = Eq.fromUniversalEquals
  implicit val show: Show[SlashRecordV1] = Show.fromToString
}

/** The only accepted field-34 V1 variant.
  *
  * Field 34 records culpability, deduplication, evidence commitment, and committee exclusion. Bond liability, debit, bounty, burn, balance,
  * and supply effects belong to separate O23 state and are intentionally absent.
  */
final class InvalidStateProofSlashRecordV1 private (
  val logicalId: InvalidStateProofSlashLogicalIdV1,
  val eventOrdinal: SnapshotOrdinal,
  val exclusion: EtaPeriodExclusionV1,
  val evidenceDigest: InvalidStateProofEvidenceDigestV1
) extends SlashRecordV1 {

  def peerId: PeerId = logicalId.peerId
  def shardId: ShardId = logicalId.shardId
  def disputedCheckpointHash: Hash = logicalId.disputedCheckpointHash

  override def equals(other: Any): Boolean =
    other match {
      case that: InvalidStateProofSlashRecordV1 =>
        logicalId == that.logicalId &&
        eventOrdinal == that.eventOrdinal &&
        exclusion == that.exclusion &&
        evidenceDigest == that.evidenceDigest
      case _ => false
    }

  override def hashCode(): Int = (logicalId, eventOrdinal, exclusion, evidenceDigest).hashCode()
  override def toString: String =
    s"InvalidStateProofSlashRecordV1($logicalId,$eventOrdinal,$exclusion,$evidenceDigest)"
}

object InvalidStateProofSlashRecordV1 {

  sealed abstract class ValidationError(val message: String) extends Product with Serializable

  final case class InvalidLogicalId(error: InvalidStateProofSlashLogicalIdV1.ValidationError) extends ValidationError(error.message)
  case object NullLogicalId extends ValidationError("logicalId cannot be null")
  case object NullEventOrdinal extends ValidationError("eventOrdinal cannot be null")
  final case class NegativeEventOrdinal(value: Long) extends ValidationError(s"eventOrdinal must be non-negative, got $value")
  case object NullExclusion extends ValidationError("exclusion cannot be null")
  case object NullEvidenceDigest extends ValidationError("evidenceDigest cannot be null")

  def from(
    peerId: PeerId,
    shardId: ShardId,
    disputedCheckpointHash: Hash,
    eventOrdinal: SnapshotOrdinal,
    exclusion: EtaPeriodExclusionV1,
    evidenceDigest: InvalidStateProofEvidenceDigestV1
  ): Either[ValidationError, InvalidStateProofSlashRecordV1] =
    InvalidStateProofSlashLogicalIdV1
      .from(peerId, shardId, disputedCheckpointHash)
      .left
      .map(InvalidLogicalId)
      .flatMap(logicalId => fromLogicalId(logicalId, eventOrdinal, exclusion, evidenceDigest))

  def fromLogicalId(
    logicalId: InvalidStateProofSlashLogicalIdV1,
    eventOrdinal: SnapshotOrdinal,
    exclusion: EtaPeriodExclusionV1,
    evidenceDigest: InvalidStateProofEvidenceDigestV1
  ): Either[ValidationError, InvalidStateProofSlashRecordV1] =
    if (logicalId eq null) Left(NullLogicalId)
    else
      InvalidStateProofSlashLogicalIdV1
        .from(logicalId.peerId, logicalId.shardId, logicalId.disputedCheckpointHash)
        .left
        .map(InvalidLogicalId)
        .flatMap { validatedLogicalId =>
          if (eventOrdinal eq null) Left(NullEventOrdinal)
          else if (eventOrdinal.value.value < 0L) Left(NegativeEventOrdinal(eventOrdinal.value.value))
          else if (exclusion eq null) Left(NullExclusion)
          else if (evidenceDigest eq null) Left(NullEvidenceDigest)
          else Right(new InvalidStateProofSlashRecordV1(validatedLogicalId, eventOrdinal, exclusion, evidenceDigest))
        }

  implicit val eq: Eq[InvalidStateProofSlashRecordV1] = Eq.fromUniversalEquals
  implicit val show: Show[InvalidStateProofSlashRecordV1] = Show.show(_.toString)
}
