package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.schema.slashing._
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.Primitives.nonNegLongCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec.{HashByteLength, codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}

import scodec.codecs._
import scodec.{Attempt, Codec, Err}
import shapeless.{::, HNil}

/** Dark ScodecV1 grammar for the O20 invalid-state-proof-only field-34 record.
  *
  * Peer and checkpoint identities compose the canonical shared `PeerIdCodec` and `HashCodec`; this codec does not define competing
  * encodings. There is no JSON projection, unknown/future reason fallback, optional context, O23 economic field, or runtime writer/reader
  * activation.
  */
object InvalidStateProofSlashRecordV1Codec {

  val InvalidStateProofTag: Int = 1
  val PeerIdPayloadBytes: Int = 64
  val PeerIdEncodedBytes: Int = 2 + PeerIdPayloadBytes
  val DigestBytes: Int = HashByteLength
  val LogicalIdEncodedBytes: Long = 1L + PeerIdEncodedBytes + 4L + DigestBytes
  val RecordEncodedBytes: Long =
    LogicalIdEncodedBytes + 8L + 8L + 8L + DigestBytes

  private val shardIdCodec: Codec[ShardId] =
    int32.exmap(
      value =>
        ShardId(value).fold[Attempt[ShardId]](
          Attempt.failure(Err(s"ShardId decode: value must be non-negative, got $value"))
        )(Attempt.successful),
      shardId =>
        if (shardId eq null) Attempt.failure(Err("ShardId cannot be null"))
        else if (shardId.value.value < 0)
          Attempt.failure(Err(s"ShardId encode: value must be non-negative, got ${shardId.value.value}"))
        else Attempt.successful(shardId.value.value)
    )

  private val snapshotOrdinalCodec: Codec[SnapshotOrdinal] =
    nonNegLongCodec.exmap[SnapshotOrdinal](
      value => Attempt.successful(SnapshotOrdinal(value)),
      ordinal =>
        if (ordinal eq null) Attempt.failure(Err("eventOrdinal cannot be null"))
        else if (ordinal.value.value < 0L)
          Attempt.failure(Err(s"eventOrdinal must be non-negative, got ${ordinal.value.value}"))
        else Attempt.successful(ordinal.value)
    )

  private val etaPeriodCodec: Codec[EtaPeriod] =
    int64.exmap(
      value => Attempt.successful(EtaPeriod(value)),
      period =>
        if (period eq null) Attempt.failure(Err("EtaPeriod cannot be null"))
        else Attempt.successful(period.value)
    )

  private val invalidStateProofTagCodec: Codec[Unit] =
    uint8.exmap(
      tag =>
        if (tag == InvalidStateProofTag) Attempt.successful(())
        else Attempt.failure(Err(s"SlashRecordV1: unknown variant tag $tag")),
      _ => Attempt.successful(InvalidStateProofTag)
    )

  implicit val exclusionCodec: Codec[EtaPeriodExclusionV1] =
    (etaPeriodCodec :: etaPeriodCodec).exmap[EtaPeriodExclusionV1](
      {
        case excludedFrom :: eligibleAgainAt :: HNil =>
          EtaPeriodExclusionV1
            .from(excludedFrom, eligibleAgainAt)
            .fold(error => Attempt.failure(Err(error.message)), Attempt.successful)
      },
      interval =>
        if (interval eq null) Attempt.failure(Err("EtaPeriodExclusionV1 cannot be null"))
        else
          EtaPeriodExclusionV1
            .from(interval.excludedFromPeriod, interval.eligibleAgainAtPeriod)
            .fold(
              error => Attempt.failure(Err(error.message)),
              valid =>
                Attempt.successful(
                  valid.excludedFromPeriod ::
                    valid.eligibleAgainAtPeriod ::
                    HNil
                )
            )
    )

  implicit val evidenceDigestCodec: Codec[InvalidStateProofEvidenceDigestV1] =
    bytes(DigestBytes).exmap(
      value =>
        InvalidStateProofEvidenceDigestV1
          .fromByteVector(value)
          .fold(error => Attempt.failure(Err(error.message)), Attempt.successful),
      digest =>
        if (digest eq null) Attempt.failure(Err("evidence digest cannot be null"))
        else Attempt.successful(digest.toByteVector)
    )

  implicit val logicalIdCodec: Codec[InvalidStateProofSlashLogicalIdV1] =
    (invalidStateProofTagCodec :: peerIdCodec :: shardIdCodec :: hashCodec)
      .exmap[InvalidStateProofSlashLogicalIdV1](
        {
          case _ :: peerId :: shardId :: checkpointHash :: HNil =>
            InvalidStateProofSlashLogicalIdV1
              .from(peerId, shardId, checkpointHash)
              .fold(error => Attempt.failure(Err(error.message)), Attempt.successful)
        },
        identity =>
          if (identity eq null) Attempt.failure(Err("InvalidStateProofSlashLogicalIdV1 cannot be null"))
          else
            InvalidStateProofSlashLogicalIdV1
              .from(identity.peerId, identity.shardId, identity.disputedCheckpointHash)
              .fold(
                error => Attempt.failure(Err(error.message)),
                valid =>
                  Attempt.successful(
                    () ::
                      valid.peerId ::
                      valid.shardId ::
                      valid.disputedCheckpointHash ::
                      HNil
                  )
              )
      )

  implicit val invalidStateProofRecordCodec: Codec[InvalidStateProofSlashRecordV1] =
    (logicalIdCodec :: snapshotOrdinalCodec :: exclusionCodec :: evidenceDigestCodec)
      .exmap[InvalidStateProofSlashRecordV1](
        {
          case logicalId :: eventOrdinal :: exclusion :: evidenceDigest :: HNil =>
            InvalidStateProofSlashRecordV1
              .fromLogicalId(logicalId, eventOrdinal, exclusion, evidenceDigest)
              .fold(error => Attempt.failure(Err(error.message)), Attempt.successful)
        },
        record =>
          if (record eq null) Attempt.failure(Err("InvalidStateProofSlashRecordV1 cannot be null"))
          else
            InvalidStateProofSlashRecordV1
              .from(
                record.peerId,
                record.shardId,
                record.disputedCheckpointHash,
                record.eventOrdinal,
                record.exclusion,
                record.evidenceDigest
              )
              .fold(
                error => Attempt.failure(Err(error.message)),
                valid =>
                  Attempt.successful(
                    valid.logicalId ::
                      valid.eventOrdinal ::
                      valid.exclusion ::
                      valid.evidenceDigest ::
                      HNil
                  )
              )
      )

  implicit val codec: Codec[SlashRecordV1] =
    invalidStateProofRecordCodec.exmap[SlashRecordV1](
      Attempt.successful,
      {
        case record: InvalidStateProofSlashRecordV1 => Attempt.successful(record)
        case null                                   => Attempt.failure(Err("SlashRecordV1 cannot be null"))
      }
    )

  implicit val exclusionImmutableCodec: ImmutableCodec[EtaPeriodExclusionV1] =
    ImmutableCodec.fromScodecCodec(exclusionCodec)

  implicit val evidenceDigestImmutableCodec: ImmutableCodec[InvalidStateProofEvidenceDigestV1] =
    ImmutableCodec.fromScodecCodec(evidenceDigestCodec)

  implicit val logicalIdImmutableCodec: ImmutableCodec[InvalidStateProofSlashLogicalIdV1] =
    ImmutableCodec.fromScodecCodec(logicalIdCodec)

  implicit val invalidStateProofRecordImmutableCodec: ImmutableCodec[InvalidStateProofSlashRecordV1] =
    ImmutableCodec.fromScodecCodec(invalidStateProofRecordCodec)

  implicit val immutableCodec: ImmutableCodec[SlashRecordV1] =
    ImmutableCodec.fromScodecCodec(codec)
}
