package io.constellationnetwork.serde

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.schema.slashing._
import io.constellationnetwork.security.ConsensusHashSchema
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.codecs.instances.InvalidStateProofSlashRecordV1Codec._

import scodec.bits.{BitVector, ByteVector}
import scodec.codecs.{int32, int64, uint8}
import weaver.FunSuite

object InvalidStateProofSlashRecordV1CodecSuite extends FunSuite {

  private sealed trait NotGiven[A]
  private object NotGiven {
    implicit def absent[A]: NotGiven[A] = new NotGiven[A] {}
    implicit def ambiguousWhenPresent1[A](implicit present: A): NotGiven[A] = new NotGiven[A] {}
    implicit def ambiguousWhenPresent2[A](implicit present: A): NotGiven[A] = new NotGiven[A] {}
  }

  private val peerId = PeerId(Hex("11" * 64))
  private val shardId = ShardId.unsafeApply(2)
  private val checkpointHash = Hash("22" * 32)
  private val evidenceDigest =
    InvalidStateProofEvidenceDigestV1
      .fromByteVector(ByteVector.fromValidHex("33" * 32))
      .fold(error => throw new Error(error.message), identity)
  private val exclusion =
    EtaPeriodExclusionV1
      .from(EtaPeriod(3L), EtaPeriod(7L))
      .fold(error => throw new Error(error.message), identity)
  private val logicalId =
    InvalidStateProofSlashLogicalIdV1
      .from(peerId, shardId, checkpointHash)
      .fold(error => throw new Error(error.message), identity)
  private val record =
    InvalidStateProofSlashRecordV1
      .from(
        peerId,
        shardId,
        checkpointHash,
        SnapshotOrdinal.unsafeApply(9L),
        exclusion,
        evidenceDigest
      )
      .fold(error => throw new Error(error.message), identity)

  private val logicalIdGolden =
    "01" +
      "0040" +
      "1111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111" +
      "00000002" +
      "2222222222222222222222222222222222222222222222222222222222222222"

  private val recordGolden =
    logicalIdGolden +
      "0000000000000009" +
      "0000000000000003" +
      "0000000000000007" +
      "3333333333333333333333333333333333333333333333333333333333333333"

  test("freezes the canonical-composition logical identity and record candidate vectors") {
    val logicalBytes = logicalIdCodec.encode(logicalId).require.toByteVector
    val recordBytes = invalidStateProofRecordCodec.encode(record).require.toByteVector

    expect.eql(logicalBytes.toHex, logicalIdGolden) &&
    expect.eql(recordBytes.toHex, recordGolden) &&
    expect.eql(logicalBytes.length, LogicalIdEncodedBytes) &&
    expect.eql(recordBytes.length, RecordEncodedBytes) &&
    expect.eql(PeerIdEncodedBytes, 66)
  }

  test("round-trips the refined record through both concrete and closed-family codecs") {
    val encoded = invalidStateProofRecordCodec.encode(record).require
    val concrete = invalidStateProofRecordCodec.complete.decodeValue(encoded).require
    val family = codec.complete.decodeValue(encoded).require

    expect.eql(concrete, record) &&
    expect.eql(family, record: SlashRecordV1) &&
    expect.eql(record.logicalId, logicalId)
  }

  test("rejects all 255 unknown variant tags") {
    val encoded = ByteVector.fromValidHex(recordGolden).bits
    val unknownTags = (0 to 255).filterNot(_ == InvalidStateProofTag)

    expect(unknownTags.forall { tag =>
      val candidate = uint8.encode(tag).require ++ encoded.drop(8L)
      codec.complete.decodeValue(candidate).toEither.isLeft
    })
  }

  test("rejects trailing bytes, truncation, negative fields, and Hash.empty on decode") {
    val encoded = ByteVector.fromValidHex(recordGolden).bits
    val shardOffset = (1L + PeerIdEncodedBytes) * 8L
    val checkpointHashOffset = shardOffset + 4L * 8L
    val ordinalOffset = LogicalIdEncodedBytes * 8L
    val exclusionStartOffset = ordinalOffset + 8L * 8L
    val exclusionEndOffset = exclusionStartOffset + 8L * 8L

    def replace(bits: BitVector, offset: Long, width: Long, value: BitVector): BitVector =
      bits.take(offset) ++ value ++ bits.drop(offset + width)

    val negativeShard = replace(encoded, shardOffset, 32L, int32.encode(-1).require)
    val emptyCheckpointHash = replace(encoded, checkpointHashOffset, DigestBytes * 8L, BitVector.low(DigestBytes * 8L))
    val negativeOrdinal = replace(encoded, ordinalOffset, 64L, int64.encode(-1L).require)
    val negativeStart = replace(encoded, exclusionStartOffset, 64L, int64.encode(-1L).require)
    val equalEnd = replace(encoded, exclusionEndOffset, 64L, int64.encode(3L).require)
    val reversedEnd = replace(encoded, exclusionEndOffset, 64L, int64.encode(2L).require)

    expect(codec.complete.decodeValue(encoded ++ BitVector.low(8L)).toEither.isLeft) &&
    expect(codec.complete.decodeValue(encoded.dropRight(8L)).toEither.isLeft) &&
    expect(invalidStateProofRecordCodec.complete.decodeValue(negativeShard).toEither.isLeft) &&
    expect(invalidStateProofRecordCodec.complete.decodeValue(emptyCheckpointHash).toEither.isLeft) &&
    expect(invalidStateProofRecordCodec.complete.decodeValue(negativeOrdinal).toEither.isLeft) &&
    expect(invalidStateProofRecordCodec.complete.decodeValue(negativeStart).toEither.isLeft) &&
    expect(invalidStateProofRecordCodec.complete.decodeValue(equalEnd).toEither.isLeft) &&
    expect(invalidStateProofRecordCodec.complete.decodeValue(reversedEnd).toEither.isLeft)
  }

  test("logical identity construction rejects malformed, null, negative, and empty components") {
    val invalidPeerIds = List(
      null.asInstanceOf[PeerId],
      PeerId(null.asInstanceOf[Hex]),
      PeerId(Hex("11" * 63)),
      PeerId(Hex("AA" * 64))
    )
    val invalidShardIds = List(null, ShardId.unsafeApply(-1))
    val invalidHashes = List(
      null.asInstanceOf[Hash],
      Hash.empty,
      Hash("22" * 31),
      Hash("BB" * 32)
    )

    expect(invalidPeerIds.forall(value => InvalidStateProofSlashLogicalIdV1.from(value, shardId, checkpointHash).isLeft)) &&
    expect(invalidShardIds.forall(value => InvalidStateProofSlashLogicalIdV1.from(peerId, value, checkpointHash).isLeft)) &&
    expect(invalidHashes.forall(value => InvalidStateProofSlashLogicalIdV1.from(peerId, shardId, value).isLeft))
  }

  test("accepts zero-based shard zero") {
    val shardZeroIdentity =
      InvalidStateProofSlashLogicalIdV1
        .from(peerId, ShardId.unsafeApply(0), checkpointHash)
        .fold(error => throw new Error(error.message), identity)
    val encoded = logicalIdCodec.encode(shardZeroIdentity).require

    expect.eql(shardZeroIdentity.shardId, ShardId.unsafeApply(0)) &&
    expect.eql(logicalIdCodec.complete.decodeValue(encoded).require, shardZeroIdentity)
  }

  test("record construction rejects null and forged-negative components") {
    val negativeOrdinal = SnapshotOrdinal.unsafeApply(-1L)

    expect(
      InvalidStateProofSlashRecordV1
        .from(peerId, shardId, checkpointHash, null, exclusion, evidenceDigest)
        .isLeft
    ) &&
    expect(
      InvalidStateProofSlashRecordV1
        .from(peerId, shardId, checkpointHash, negativeOrdinal, exclusion, evidenceDigest)
        .isLeft
    ) &&
    expect(
      InvalidStateProofSlashRecordV1
        .from(peerId, shardId, checkpointHash, SnapshotOrdinal.unsafeApply(9L), null, evidenceDigest)
        .isLeft
    ) &&
    expect(
      InvalidStateProofSlashRecordV1
        .from(peerId, shardId, checkpointHash, SnapshotOrdinal.unsafeApply(9L), exclusion, null)
        .isLeft
    ) &&
    expect(
      InvalidStateProofSlashRecordV1
        .fromLogicalId(null, SnapshotOrdinal.unsafeApply(9L), exclusion, evidenceDigest)
        .isLeft
    )
  }

  test("refined interval factories reject invalid and overflowing ranges") {
    val maxEndpoint = EtaPeriodExclusionV1.from(EtaPeriod(Long.MaxValue - 1L), EtaPeriod(Long.MaxValue))
    val maxByCount = EtaPeriodExclusionV1.fromStartAndPeriodCount(EtaPeriod(Long.MaxValue - 1L), 1L)

    expect(EtaPeriodExclusionV1.from(null, EtaPeriod(1L)).isLeft) &&
    expect(EtaPeriodExclusionV1.from(EtaPeriod(0L), null).isLeft) &&
    expect(EtaPeriodExclusionV1.from(EtaPeriod(-1L), EtaPeriod(1L)).isLeft) &&
    expect(EtaPeriodExclusionV1.from(EtaPeriod(0L), EtaPeriod(-1L)).isLeft) &&
    expect(EtaPeriodExclusionV1.from(EtaPeriod(3L), EtaPeriod(3L)).isLeft) &&
    expect(EtaPeriodExclusionV1.from(EtaPeriod(4L), EtaPeriod(3L)).isLeft) &&
    expect(EtaPeriodExclusionV1.fromStartAndPeriodCount(EtaPeriod(3L), 0L).isLeft) &&
    expect(EtaPeriodExclusionV1.fromStartAndPeriodCount(EtaPeriod(3L), -1L).isLeft) &&
    expect(EtaPeriodExclusionV1.fromStartAndPeriodCount(EtaPeriod(Long.MaxValue), 1L).isLeft) &&
    expect(EtaPeriodExclusionV1.fromStartAndPeriodCount(EtaPeriod(Long.MaxValue - 1L), 2L).isLeft) &&
    expect(maxEndpoint.isRight) &&
    expect(maxByCount == maxEndpoint)
  }

  test("the exclusion interval has exact half-open boundaries, including Long.MaxValue") {
    val maximum =
      EtaPeriodExclusionV1
        .from(EtaPeriod(Long.MaxValue - 1L), EtaPeriod(Long.MaxValue))
        .fold(error => throw new Error(error.message), identity)

    expect(!exclusion.excludes(EtaPeriod(2L))) &&
    expect(exclusion.excludes(EtaPeriod(3L))) &&
    expect(exclusion.excludes(EtaPeriod(6L))) &&
    expect(!exclusion.excludes(EtaPeriod(7L))) &&
    expect(!exclusion.excludes(EtaPeriod(8L))) &&
    expect(!exclusion.excludes(null)) &&
    expect(maximum.excludes(EtaPeriod(Long.MaxValue - 1L))) &&
    expect(!maximum.excludes(EtaPeriod(Long.MaxValue)))
  }

  test("private refined values expose no case-class copy escape") {
    expect(!classOf[EtaPeriodExclusionV1].getMethods.exists(_.getName == "copy")) &&
    expect(!classOf[InvalidStateProofSlashLogicalIdV1].getMethods.exists(_.getName == "copy")) &&
    expect(!classOf[InvalidStateProofSlashRecordV1].getMethods.exists(_.getName == "copy"))
  }

  test("encoders reject null refined values without throwing") {
    expect(invalidStateProofRecordCodec.encode(null).toEither.isLeft) &&
    expect(codec.encode(null).toEither.isLeft) &&
    expect(logicalIdCodec.encode(null).toEither.isLeft) &&
    expect(exclusionCodec.encode(null).toEither.isLeft) &&
    expect(evidenceDigestCodec.encode(null).toEither.isLeft)
  }

  test("the O22 evidence digest remains a distinct opaque fixed-width value") {
    val bytes = ByteVector.fill(32)(0x44.toByte)
    val evidence = InvalidStateProofEvidenceDigestV1.fromByteVector(bytes)
    val _ = implicitly[NotGiven[ConsensusHashSchema[InvalidStateProofEvidenceDigestV1]]]

    expect(evidence.exists(_.toByteVector == bytes)) &&
    expect(evidence.exists(value => evidenceDigestCodec.encode(value).require.toByteVector == bytes)) &&
    expect(evidence.exists(value => evidenceDigestCodec.complete.decodeValue(bytes.bits).require == value)) &&
    expect(InvalidStateProofEvidenceDigestV1.fromByteVector(bytes.drop(1L)).isLeft) &&
    expect(InvalidStateProofEvidenceDigestV1.fromByteVector(bytes ++ ByteVector(0)).isLeft) &&
    expect(
      InvalidStateProofEvidenceDigestV1.fromByteVector(null) ==
        Left(InvalidStateProofEvidenceDigestV1.NullDigest)
    )
  }
}
