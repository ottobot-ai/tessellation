package io.constellationnetwork.node.shared.infrastructure.sharding

import java.nio.charset.StandardCharsets

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{FraudProofEnvelope, ShardId}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import com.google.protobuf.ByteString
import eu.timepit.refined.refineV
import eu.timepit.refined.types.numeric.NonNegLong

/** Scala ⇄ protobuf wire codec for WATCHTOWER [[FraudProofEnvelope]] gossip — the analog of [[ShardCheckpointWireCodecs]] for the
  * gl0-wide `fraud-proof` topic.
  *
  * '''All fields structured (no opaque JSON).''' Unlike the checkpoint codec (which carries heavy `Signed[SCSB]`/delta JSON), the fraud
  * proof is a small fixed-field message; the dispute consumer needs every field typed to recompute the verdict, so each maps to a proto
  * scalar. Conventions match the existing codec: `Hash` ⇄ UTF-8 of the canonical hex, `PeerId` ⇄ raw bytes (hex-encoded back), sig/witness
  * `Hex` ⇄ raw bytes.
  *
  * '''Why no `Hasher[F]` here.''' Pure structural translation; the canonical fraud-proof hash (for the challenger signature) is computed at
  * the producer/verdict sites via `Hasher[F].hash(FraudProofSigPreimage)` (`feedback_use_hasher_no_manual_serialize`).
  */
object FraudProofWireCodecs {

  private def hashToBytes(h: Hash): ByteString =
    ByteString.copyFrom(h.value, StandardCharsets.UTF_8)

  private def bytesToHash(b: ByteString): Hash =
    Hash(new String(b.toByteArray, StandardCharsets.UTF_8))

  private def peerIdToBytes(p: PeerId): ByteString =
    ByteString.copyFrom(p.value.toBytes)

  private def bytesToPeerId(b: ByteString): PeerId =
    PeerId(Hex.fromBytes(b.toByteArray))

  private def addressFromString(s: String): Either[String, Address] =
    refineV[DAGAddressRefined](s).map(refined => Address(refined))

  def toWire(fp: FraudProofEnvelope): pb.FraudProofEnvelopeWire =
    pb.FraudProofEnvelopeWire(
      shardId = fp.shardId.value.value,
      disputedCheckpointHash = hashToBytes(fp.disputedCheckpointHash),
      metagraphAddress = fp.metagraphAddress.value.value,
      gl0AnchorOrdinal = fp.gl0AnchorOrdinal.value.value,
      claimedDerivation = hashToBytes(fp.claimedDerivation),
      challengerDerivation = hashToBytes(fp.challengerDerivation),
      reexecutionWitness = ByteString.copyFrom(fp.reexecutionWitness.toBytes),
      challengerSignature = ByteString.copyFrom(fp.challengerSignature.toBytes),
      submitterId = peerIdToBytes(fp.submitterId)
    )

  def fromWire[F[_]: Async](w: pb.FraudProofEnvelopeWire): F[FraudProofEnvelope] =
    (ShardId(w.shardId), NonNegLong.from(w.gl0AnchorOrdinal).toOption.map(SnapshotOrdinal(_)), addressFromString(w.metagraphAddress)) match {
      case (None, _, _) =>
        Async[F].raiseError[FraudProofEnvelope](
          new RuntimeException(s"FraudProofEnvelopeWire: invalid shard_id ${w.shardId} (must be non-negative)")
        )
      case (_, None, _) =>
        Async[F].raiseError[FraudProofEnvelope](
          new RuntimeException(s"FraudProofEnvelopeWire: invalid gl0_anchor_ordinal ${w.gl0AnchorOrdinal} (must be non-negative)")
        )
      case (_, _, Left(err)) =>
        Async[F].raiseError[FraudProofEnvelope](
          new RuntimeException(s"FraudProofEnvelopeWire: invalid metagraph_address '${w.metagraphAddress}' ($err)")
        )
      case (Some(sid), Some(anchor), Right(mg)) =>
        Async[F].pure(
          FraudProofEnvelope(
            shardId = sid,
            disputedCheckpointHash = bytesToHash(w.disputedCheckpointHash),
            metagraphAddress = mg,
            gl0AnchorOrdinal = anchor,
            claimedDerivation = bytesToHash(w.claimedDerivation),
            challengerDerivation = bytesToHash(w.challengerDerivation),
            reexecutionWitness = Hex.fromBytes(w.reexecutionWitness.toByteArray),
            challengerSignature = Hex.fromBytes(w.challengerSignature.toByteArray),
            submitterId = bytesToPeerId(w.submitterId)
          )
        )
    }
}
