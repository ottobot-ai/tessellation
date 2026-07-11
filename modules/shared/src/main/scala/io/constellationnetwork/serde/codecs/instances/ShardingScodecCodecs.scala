package io.constellationnetwork.serde.codecs.instances

import cats.data.NonEmptyList

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.slashing.InvalidStateProofEvidence
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.NonEmptyListCodec.nonEmptyList
import io.constellationnetwork.serde.codecs.SortedMapCodec.sortedMap
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.HexContentCodec.{codec => hexCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}
import io.constellationnetwork.serde.codecs.instances.StateChannelSnapshotBinaryCodec.{codec => stateChannelBinaryCodec}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import scodec.codecs.{int32, int64}
import scodec.{Attempt, Codec, Err}
import shapeless.{::, HNil}

/** Canonical scodec encodings for the current greenfield shard-checkpoint and invalid-state-proof schema.
  *
  * These codecs deliberately have no legacy/default branch. Every field is encoded in its case-class order, including the replayable
  * state-channel binaries that GL0 re-executes and the full disputed checkpoint carried by invalid-state-proof evidence.
  */
object ShardingScodecCodecs {

  implicit val shardIdCodec: Codec[ShardId] =
    int32.exmap(
      value =>
        ShardId(value).fold[Attempt[ShardId]](
          Attempt.failure(Err(s"ShardId decode: value must be non-negative, got $value"))
        )(Attempt.successful),
      shardId => Attempt.successful(shardId.value.value)
    )

  implicit val shardIdImmutableCodec: ImmutableCodec[ShardId] = ImmutableCodec.fromScodecCodec(shardIdCodec)

  implicit val shardOrdinalCodec: Codec[ShardOrdinal] =
    int64.xmap(ShardOrdinal(_), _.value)

  implicit val shardOrdinalImmutableCodec: ImmutableCodec[ShardOrdinal] = ImmutableCodec.fromScodecCodec(shardOrdinalCodec)

  implicit val etaPeriodCodec: Codec[EtaPeriod] =
    int64.xmap(EtaPeriod(_), _.value)

  private val snapshotOrdinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]
  private val slotCodec: Codec[Slot] = Codec[Slot]

  implicit val committeeMemberSignatureCodec: Codec[CommitteeMemberSignature] =
    (peerIdCodec :: hexCodec :: hexCodec :: hexCodec :: int32)
      .xmap[CommitteeMemberSignature](
        {
          case peerId :: vrfProof :: ed25519Sig :: kesProductSig :: kesTreeStep :: HNil =>
            CommitteeMemberSignature(peerId, vrfProof, ed25519Sig, kesProductSig, kesTreeStep)
        },
        signature =>
          signature.peerId ::
            signature.vrfProof ::
            signature.ed25519Sig ::
            signature.kesProductSig ::
            signature.kesTreeStep ::
            HNil
      )

  implicit val committeeMemberSignatureImmutableCodec: ImmutableCodec[CommitteeMemberSignature] =
    ImmutableCodec.fromScodecCodec(committeeMemberSignatureCodec)

  private val signedStateChannelBinaryCodec: Codec[Signed[StateChannelSnapshotBinary]] = signedCodecFor(stateChannelBinaryCodec)
  private val perMetagraphRootsCodec: Codec[SortedMap[Address, Hash]] = sortedMap(addressCodec, hashCodec)
  private val includedSnapshotsCodec: Codec[SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]] =
    sortedMap(addressCodec, nonEmptyList(signedStateChannelBinaryCodec))

  implicit val shardDerivedStateDeltaCodec: Codec[ShardDerivedStateDelta] =
    (perMetagraphRootsCodec :: includedSnapshotsCodec)
      .xmap[ShardDerivedStateDelta](
        { case roots :: snapshots :: HNil => ShardDerivedStateDelta(roots, snapshots) },
        delta => delta.perMetagraphMptRoots :: delta.includedSnapshots :: HNil
      )

  implicit val shardDerivedStateDeltaImmutableCodec: ImmutableCodec[ShardDerivedStateDelta] =
    ImmutableCodec.fromScodecCodec(shardDerivedStateDeltaCodec)

  private val committeeSignaturesCodec: Codec[NonEmptyList[CommitteeMemberSignature]] =
    nonEmptyList(committeeMemberSignatureCodec)

  implicit val shardCheckpointCodec: Codec[ShardCheckpoint] =
    (shardIdCodec ::
      hashCodec ::
      shardOrdinalCodec ::
      snapshotOrdinalCodec ::
      slotCodec ::
      shardDerivedStateDeltaCodec ::
      committeeSignaturesCodec ::
      etaPeriodCodec ::
      snapshotOrdinalCodec)
      .xmap[ShardCheckpoint](
        {
          case shardId :: parentHash :: shardOrdinal :: gl0AnchorOrdinal :: slot :: delta :: signatures :: epoch ::
              executionBaseOrdinal :: HNil =>
            ShardCheckpoint(
              shardId,
              parentHash,
              shardOrdinal,
              gl0AnchorOrdinal,
              slot,
              delta,
              signatures,
              epoch,
              executionBaseOrdinal
            )
        },
        checkpoint =>
          checkpoint.shardId ::
            checkpoint.parentCheckpointHash ::
            checkpoint.shardOrdinal ::
            checkpoint.gl0AnchorOrdinal ::
            checkpoint.slot ::
            checkpoint.derivedStateDelta ::
            checkpoint.committeeSignatures ::
            checkpoint.epoch ::
            checkpoint.executionBaseOrdinal ::
            HNil
      )

  implicit val shardCheckpointImmutableCodec: ImmutableCodec[ShardCheckpoint] =
    ImmutableCodec.fromScodecCodec(shardCheckpointCodec)

  implicit val fraudProofEnvelopeCodec: Codec[FraudProofEnvelope] =
    (shardIdCodec ::
      hashCodec ::
      addressCodec ::
      snapshotOrdinalCodec ::
      hashCodec ::
      hashCodec ::
      hexCodec ::
      hexCodec ::
      peerIdCodec)
      .xmap[FraudProofEnvelope](
        {
          case shardId :: disputedHash :: metagraphAddress :: gl0AnchorOrdinal :: claimed :: challenger :: witness :: signature ::
              submitterId :: HNil =>
            FraudProofEnvelope(
              shardId,
              disputedHash,
              metagraphAddress,
              gl0AnchorOrdinal,
              claimed,
              challenger,
              witness,
              signature,
              submitterId
            )
        },
        fraudProof =>
          fraudProof.shardId ::
            fraudProof.disputedCheckpointHash ::
            fraudProof.metagraphAddress ::
            fraudProof.gl0AnchorOrdinal ::
            fraudProof.claimedDerivation ::
            fraudProof.challengerDerivation ::
            fraudProof.reexecutionWitness ::
            fraudProof.challengerSignature ::
            fraudProof.submitterId ::
            HNil
      )

  implicit val fraudProofEnvelopeImmutableCodec: ImmutableCodec[FraudProofEnvelope] =
    ImmutableCodec.fromScodecCodec(fraudProofEnvelopeCodec)

  implicit val invalidStateProofEvidenceCodec: Codec[InvalidStateProofEvidence] =
    (shardIdCodec :: shardCheckpointCodec :: addressCodec :: hashCodec :: fraudProofEnvelopeCodec)
      .xmap[InvalidStateProofEvidence](
        {
          case shardId :: checkpoint :: metagraphAddress :: attestedRoot :: fraudProof :: HNil =>
            InvalidStateProofEvidence(shardId, checkpoint, metagraphAddress, attestedRoot, fraudProof)
        },
        evidence =>
          evidence.shardId ::
            evidence.disputedCheckpoint ::
            evidence.metagraphAddress ::
            evidence.attestedRoot ::
            evidence.fraudProof ::
            HNil
      )

  implicit val invalidStateProofEvidenceImmutableCodec: ImmutableCodec[InvalidStateProofEvidence] =
    ImmutableCodec.fromScodecCodec(invalidStateProofEvidenceCodec)
}
