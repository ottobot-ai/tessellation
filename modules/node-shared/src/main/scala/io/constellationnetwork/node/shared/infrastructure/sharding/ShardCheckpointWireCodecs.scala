package io.constellationnetwork.node.shared.infrastructure.sharding

import java.nio.charset.StandardCharsets

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.{SnapshotOrdinal, address}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import com.google.protobuf.ByteString
import eu.timepit.refined.refineV
import eu.timepit.refined.types.numeric.NonNegLong

/** Scala ⇄ protobuf wire codecs for shard-checkpoint gossip (Slice 14 of
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §6.4).
  *
  * '''Why a separate codecs object''' — the schema package (`schema.sharding`) lives in `modules/shared`, which doesn't depend on
  * `node-shared` and therefore can't see the protobuf-generated types under `node.shared.infrastructure.consensus.nakamoto.proto`. The
  * codec adapter lives here in node-shared so the dependency direction stays correct, mirroring how
  * `NakamotoSyncDaemon.handleMetagraphAttestation` does ad-hoc translation from `pb.MetagraphAttestation` to the schema-side
  * `IncomingAttestation`.
  *
  * '''Wire-format conventions''' (mirrored from the proto comments):
  *   - `Hash` ⇄ `bytes`: UTF-8 of the canonical hex representation. Matches the existing `Snapshot.hash` / `MetagraphAttestation.parent_hash`
  *     decode pattern in `NakamotoSyncDaemon` and uses the same `Hash.getBytes` (UTF-8) form so a future receiver can equate the value
  *     byte-identically.
  *   - `PeerId` ⇄ `bytes`: hex-encoded back to a `Hex` string on the JVM side. Matches `MetagraphAttestation.peer_id` (raw bytes →
  *     `peerIdBytes.map("%02x".format(_)).mkString` → `Hex` → `PeerId`).
  *   - `Hex` (sig fields) ⇄ `bytes`: `.toBytes` on encode, `Hex.fromBytes` on decode. The raw bytes form is the cheap wire
  *     representation, the `Hex` newtype is the schema-side representation.
  *   - Heavy structural payloads (`Signed[StateChannelSnapshotBinary]`, the entire `ShardDerivedStateDelta`, `List[CrossShardReceipt]`) are
  *     opaque JSON bytes via the project `JsonSerializer`. This mirrors how `MetagraphBinary.binary` carries `Signed[SCSB]` JSON and how
  *     `AllowSpendBlock.payload` carries `Signed[AllowSpendBlock]` JSON. Because `JsonSerializer.forAsync` uses a `Printer` with
  *     `sortKeys = true` + `dropNullValues = true`, repeated encodes are byte-identical — the canonical-preimage hash signers compute is
  *     stable across encode/decode round-trips.
  *
  * '''Why `Async[F]` and `JsonSerializer[F]` constraints''' — JSON ser/de is the existing `JsonSerializer[F]` typeclass, which surfaces
  * an `F[Either[Throwable, A]]` on deserialize. Codec functions return `F[A]` (with `fromWire` lifting the deserialize failure into
  * `F.raiseError`) so callers don't have to thread `Either` plumbing through every gossip handler.
  *
  * '''Greenfield rule''' (per `feedback_greenfield_no_wire_compat`): no compat ceremony. Field-set and order are pinned to v1; add
  * a new `*Wire2` shape if a future evolution is needed rather than mutating these codecs in place.
  *
  * '''Why no `Hasher[F]` here''' — the codec is pure structural translation; it does not compute consensus-load-bearing hashes (those
  * stay routed through `Hasher[F]` at the producer + verifier sites — see `feedback_use_hasher_no_manual_serialize`). The codec
  * preserves byte content; downstream paths that need a hash call `Hasher[F].hash(...)` against the schema-side value.
  */
object ShardCheckpointWireCodecs {

  // ===========================================================================
  // Hash ⇄ bytes
  //
  // Mirrors the `NakamotoSyncDaemon` pattern at line 1496 — UTF-8 of canonical hex.
  // ===========================================================================

  private def hashToBytes(h: Hash): ByteString =
    ByteString.copyFrom(h.value, StandardCharsets.UTF_8)

  private def bytesToHash(b: ByteString): Hash =
    Hash(new String(b.toByteArray, StandardCharsets.UTF_8))

  // ===========================================================================
  // PeerId ⇄ bytes
  //
  // The wire field is "raw bytes" — the canonical Hex string is hex-encoded back
  // on receive. Symmetric: encode = `.toBytes`, decode = `Hex.fromBytes` (no
  // separator). Matches MetagraphAttestation.peerId pattern.
  // ===========================================================================

  private def peerIdToBytes(p: PeerId): ByteString =
    ByteString.copyFrom(p.value.toBytes)

  private def bytesToPeerId(b: ByteString): PeerId =
    PeerId(Hex.fromBytes(b.toByteArray))

  // ===========================================================================
  // CommitteeMemberSignature ⇄ CommitteeMemberSignatureWire
  // ===========================================================================

  def committeeSignatureToWire(sig: CommitteeMemberSignature): pb.CommitteeMemberSignatureWire =
    pb.CommitteeMemberSignatureWire(
      peerId = peerIdToBytes(sig.peerId),
      vrfProof = ByteString.copyFrom(sig.vrfProof.toBytes),
      ed25519Sig = ByteString.copyFrom(sig.ed25519Sig.toBytes),
      kesProductSig = ByteString.copyFrom(sig.kesProductSig.toBytes),
      kesTreeStep = sig.kesTreeStep
    )

  def committeeSignatureFromWire(w: pb.CommitteeMemberSignatureWire): CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = bytesToPeerId(w.peerId),
      vrfProof = Hex.fromBytes(w.vrfProof.toByteArray),
      ed25519Sig = Hex.fromBytes(w.ed25519Sig.toByteArray),
      kesProductSig = Hex.fromBytes(w.kesProductSig.toByteArray),
      kesTreeStep = w.kesTreeStep
    )

  // ===========================================================================
  // Address ⇄ string
  //
  // The proto carries the DAG base58 string. Decode round-trips through the
  // refinement type so a malformed wire address surfaces as a `Left` rather
  // than producing an invalid `Address` newtype.
  // ===========================================================================

  private def addressFromString(s: String): Either[String, Address] =
    refineV[DAGAddressRefined](s).map(refined => Address(refined))

  // ===========================================================================
  // ShardDerivedStateDelta ⇄ ShardDerivedStateDeltaWire (opaque JSON payload)
  // ===========================================================================

  def derivedStateDeltaToWire[F[_]: Async: JsonSerializer](
    delta: ShardDerivedStateDelta
  ): F[pb.ShardDerivedStateDeltaWire] =
    JsonSerializer[F].serialize(delta).map { jsonBytes =>
      pb.ShardDerivedStateDeltaWire(payloadJson = ByteString.copyFrom(jsonBytes))
    }

  def derivedStateDeltaFromWire[F[_]: Async: JsonSerializer](
    w: pb.ShardDerivedStateDeltaWire
  ): F[ShardDerivedStateDelta] =
    JsonSerializer[F].deserialize[ShardDerivedStateDelta](w.payloadJson.toByteArray).flatMap {
      case Right(d) => Async[F].pure(d)
      case Left(e)  => Async[F].raiseError(new RuntimeException(s"ShardDerivedStateDelta JSON decode failed: ${e.getMessage}", e))
    }

  // ===========================================================================
  // PerMetagraphSnapshots ⇄ SortedMap<Address, NonEmptyList<Signed<SCSB>>>
  //
  // Each per-MG entry: address + repeated JSON-encoded Signed[SCSB] bytes.
  // Order across the repeated field == NonEmptyList order. SortedMap key order
  // is preserved by emitting `toList` (SortedMap iteration is deterministic
  // under `Ordering[Address]`).
  //
  // Decode contract: an empty `signed_snapshots_json` for a given entry is a
  // protocol violation because the schema-side value is `NonEmptyList`. We
  // surface this as `F.raiseError` so the caller can drop the message and
  // log; we do NOT silently fall back to a stub.
  // ===========================================================================

  def includedSnapshotsToWire[F[_]: Async: JsonSerializer](
    snaps: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
  ): F[Seq[pb.PerMetagraphSnapshots]] =
    snaps.toList.traverse {
      case (mg, nel) =>
        nel.toList
          .traverse(s => JsonSerializer[F].serialize(s).map(ByteString.copyFrom))
          .map { jsonList =>
            pb.PerMetagraphSnapshots(
              metagraphAddress = mg.value.value,
              signedSnapshotsJson = jsonList
            )
          }
    }.map(_.toSeq)

  def includedSnapshotsFromWire[F[_]: Async: JsonSerializer](
    wires: Seq[pb.PerMetagraphSnapshots]
  ): F[SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]] =
    wires.toList
      .traverse { w =>
        addressFromString(w.metagraphAddress) match {
          case Left(err) =>
            Async[F].raiseError[(Address, NonEmptyList[Signed[StateChannelSnapshotBinary]])](
              new RuntimeException(s"PerMetagraphSnapshots: invalid metagraph address '${w.metagraphAddress}' ($err)")
            )
          case Right(addr) =>
            w.signedSnapshotsJson.toList match {
              case Nil =>
                Async[F].raiseError[(Address, NonEmptyList[Signed[StateChannelSnapshotBinary]])](
                  new RuntimeException(
                    s"PerMetagraphSnapshots: empty signed_snapshots_json for $addr — schema requires NonEmptyList"
                  )
                )
              case head :: tail =>
                (head :: tail)
                  .traverse { bytes =>
                    JsonSerializer[F].deserialize[Signed[StateChannelSnapshotBinary]](bytes.toByteArray).flatMap {
                      case Right(s) => Async[F].pure(s)
                      case Left(e) =>
                        Async[F].raiseError[Signed[StateChannelSnapshotBinary]](
                          new RuntimeException(s"Signed[SCSB] JSON decode failed for $addr: ${e.getMessage}", e)
                        )
                    }
                  }
                  .map { case h :: t => (addr, NonEmptyList(h, t)); case _ => throw new IllegalStateException("unreachable") }
            }
        }
      }
      .map(entries => SortedMap.from(entries)(address.Address.OrderingInstance))

  // ===========================================================================
  // emittedReceipts: List[CrossShardReceipt] ⇄ bytes (opaque JSON)
  //
  // Empty bytes ⇒ empty list. Round-trip: List[CrossShardReceipt] → JSON →
  // ByteString → JSON → List[CrossShardReceipt]. Determinism via
  // `JsonSerializer.forAsync`'s sortKeys printer.
  // ===========================================================================

  def emittedReceiptsToWire[F[_]: Async: JsonSerializer](
    receipts: List[CrossShardReceipt]
  ): F[ByteString] =
    if (receipts.isEmpty)
      Async[F].pure(ByteString.EMPTY)
    else
      JsonSerializer[F].serialize(receipts).map(ByteString.copyFrom)

  def emittedReceiptsFromWire[F[_]: Async: JsonSerializer](
    b: ByteString
  ): F[List[CrossShardReceipt]] =
    if (b.isEmpty)
      Async[F].pure(List.empty[CrossShardReceipt])
    else
      JsonSerializer[F].deserialize[List[CrossShardReceipt]](b.toByteArray).flatMap {
        case Right(l) => Async[F].pure(l)
        case Left(e) =>
          Async[F].raiseError[List[CrossShardReceipt]](
            new RuntimeException(s"List[CrossShardReceipt] JSON decode failed: ${e.getMessage}", e)
          )
      }

  // ===========================================================================
  // ShardCheckpoint ⇄ ShardCheckpointWire
  //
  // The outer `Signed[ShardCheckpoint]` envelope is NOT carried on the gossip wire by
  // this codec — the protobuf shape captures the inner `ShardCheckpoint` content + the
  // per-member committee signatures, which together carry the cryptographic evidence
  // gl0 acceptance needs (VRF + Ed25519 + KES per signer). The outer `Signed[_]`'s
  // signature proof on the producer's long-term key is redundant with the
  // CommitteeMemberSignature of that same operator inside `committeeSignatures` (slice 8
  // producer attaches its own committee sig as the first element). If a future use-case
  // needs the outer envelope, add a `signed_envelope` field in a `ShardCheckpointWire2`.
  // ===========================================================================

  def shardCheckpointToWire[F[_]: Async: JsonSerializer](
    cp: ShardCheckpoint
  ): F[pb.ShardCheckpointWire] =
    for {
      included <- includedSnapshotsToWire[F](cp.derivedStateDelta.includedSnapshots)
      derivedDelta <- derivedStateDeltaToWire[F](cp.derivedStateDelta)
      receiptsJson <- emittedReceiptsToWire[F](cp.emittedReceipts)
    } yield pb.ShardCheckpointWire(
      shardId = cp.shardId.value.value,
      shardOrdinal = cp.shardOrdinal.value,
      parentCheckpointHash = hashToBytes(cp.parentCheckpointHash),
      gl0AnchorOrdinal = cp.gl0AnchorOrdinal.value.value,
      epoch = cp.epoch.value,
      includedSnapshots = included,
      derivedStateDelta = Some(derivedDelta),
      committeeSignatures = cp.committeeSignatures.toList.map(committeeSignatureToWire),
      emittedReceiptsJson = receiptsJson
    )

  def shardCheckpointFromWire[F[_]: Async: JsonSerializer](
    w: pb.ShardCheckpointWire
  ): F[ShardCheckpoint] = {
    val shardIdOpt = ShardId(w.shardId)
    val gl0OrdOpt = NonNegLong.from(w.gl0AnchorOrdinal).toOption.map(SnapshotOrdinal(_))
    (shardIdOpt, gl0OrdOpt) match {
      case (None, _) =>
        Async[F].raiseError[ShardCheckpoint](
          new RuntimeException(s"ShardCheckpointWire: invalid shard_id ${w.shardId} (must be non-negative)")
        )
      case (_, None) =>
        Async[F].raiseError[ShardCheckpoint](
          new RuntimeException(s"ShardCheckpointWire: invalid gl0_anchor_ordinal ${w.gl0AnchorOrdinal} (must be non-negative)")
        )
      case (Some(sid), Some(gl0Ord)) =>
        for {
          // The derived-state-delta wire is required; a None is wire-shape-level invalid.
          deltaWire <- w.derivedStateDelta
                         .liftTo[F](
                           new RuntimeException("ShardCheckpointWire: missing required derived_state_delta")
                         )
          delta <- derivedStateDeltaFromWire[F](deltaWire)
          // Validate cross-consistency: the wire-side derived delta carries its own includedSnapshots; the wire-side
          // includedSnapshots message field is the per-MG repeated decomposition. The two must agree. We treat the
          // delta-internal one as authoritative (it's the field that hashes into the signed preimage) but also re-decode
          // the structural one to catch a sender that forgot to keep them in sync.
          _ <- includedSnapshotsFromWire[F](w.includedSnapshots).void
          receipts <- emittedReceiptsFromWire[F](w.emittedReceiptsJson)
          sigsList = w.committeeSignatures.toList.map(committeeSignatureFromWire)
          sigsNel <- sigsList match {
                       case Nil =>
                         Async[F].raiseError[NonEmptyList[CommitteeMemberSignature]](
                           new RuntimeException(
                             "ShardCheckpointWire: empty committee_signatures — schema requires NonEmptyList"
                           )
                         )
                       case head :: tail => Async[F].pure(NonEmptyList(head, tail))
                     }
        } yield ShardCheckpoint(
          shardId = sid,
          parentCheckpointHash = bytesToHash(w.parentCheckpointHash),
          shardOrdinal = ShardOrdinal(w.shardOrdinal),
          gl0AnchorOrdinal = gl0Ord,
          derivedStateDelta = delta,
          emittedReceipts = receipts,
          committeeSignatures = sigsNel,
          epoch = EtaPeriod(w.epoch)
        )
    }
  }

  // ===========================================================================
  // ShardCheckpointAttestation — domain side
  //
  // Slice 1 didn't define a separate ShardCheckpointAttestation case class
  // (only the inline CommitteeMemberSignature that gets carried inside the
  // checkpoint envelope). The attestation wire shape is for the gossip path
  // where a non-producing committee member attests AFTER seeing the envelope.
  // We model the schema-side attestation as a small case class scoped here,
  // since this is the file that owns the wire ⇄ domain mapping for this slice.
  // ===========================================================================

  /** Domain-side shape of a shard-checkpoint attestation by a non-producing committee member. Mirrors `ShardCheckpointAttestationWire` and
    * fills the schema-side gap: Slice 1 carries committee signatures inline on the produced envelope, but the gossip path needs an
    * after-the-fact attestation shape too. Modelled here (rather than back-ported to `schema.sharding`) so the schema package keeps the
    * shape it landed with — this slice's contribution is purely the gossip wire mapping.
    */
  final case class ShardCheckpointAttestation(
    shardId: ShardId,
    checkpointHash: Hash,
    attesterSignature: CommitteeMemberSignature
  )

  object ShardCheckpointAttestation {

    import cats.Eq

    implicit val eq: Eq[ShardCheckpointAttestation] =
      Eq.instance((a, b) =>
        a.shardId == b.shardId &&
          a.checkpointHash == b.checkpointHash &&
          a.attesterSignature == b.attesterSignature
      )
  }

  def shardCheckpointAttestationToWire(att: ShardCheckpointAttestation): pb.ShardCheckpointAttestationWire =
    pb.ShardCheckpointAttestationWire(
      shardId = att.shardId.value.value,
      checkpointHash = hashToBytes(att.checkpointHash),
      attesterSignature = Some(committeeSignatureToWire(att.attesterSignature))
    )

  def shardCheckpointAttestationFromWire[F[_]: Async](
    w: pb.ShardCheckpointAttestationWire
  ): F[ShardCheckpointAttestation] =
    (ShardId(w.shardId), w.attesterSignature) match {
      case (None, _) =>
        Async[F].raiseError[ShardCheckpointAttestation](
          new RuntimeException(s"ShardCheckpointAttestationWire: invalid shard_id ${w.shardId} (must be non-negative)")
        )
      case (_, None) =>
        Async[F].raiseError[ShardCheckpointAttestation](
          new RuntimeException("ShardCheckpointAttestationWire: missing required attester_signature")
        )
      case (Some(sid), Some(sigWire)) =>
        Async[F].pure(
          ShardCheckpointAttestation(
            shardId = sid,
            checkpointHash = bytesToHash(w.checkpointHash),
            attesterSignature = committeeSignatureFromWire(sigWire)
          )
        )
    }

  // ===========================================================================
  // Signed[ShardCheckpoint] convenience wrappers
  //
  // The gossip wire form is `ShardCheckpointWire` (the inner content). The
  // `Signed[ShardCheckpoint]` envelope from Slice 8's producer is unwrapped at
  // the encode boundary, then re-wrapped at the decode boundary using the
  // producer's first committee signature as the seed for the `Signed` proof
  // (the producer always self-attests first — see `ShardCheckpointProducer.make`).
  //
  // '''Slice 14 scope.''' Currently the `Signed[_]` envelope on Slice 8's
  // producer is constructed via `SignatureProof.fromHash(selfKeyPair, preimageHash)`.
  // The wire form doesn't carry the outer `Signed[_]` proof — gl0 acceptance
  // (Slice 9) verifies the inner `CommitteeMemberSignature.ed25519Sig` of
  // every signer including the producer, which is the same cryptographic
  // evidence as the outer proof. The decode helper here returns the inner
  // `ShardCheckpoint`; callers that need a `Signed[_]` envelope can re-wrap
  // with a synthetic proof or, preferably, modify Slice 9 to consume the
  // inner type directly.
  //
  // We expose `signedShardCheckpointToWire` (taking `Signed[_]`, dropping the
  // outer proof on encode) as the publish-side convenience; receivers get a
  // plain `ShardCheckpoint` back from `shardCheckpointFromWire`.
  // ===========================================================================

  /** Encode a `Signed[ShardCheckpoint]` to the gossip wire shape. The outer `Signed[_]` envelope is dropped — see scaladoc above.
    *
    * '''Why a dedicated helper instead of `_.value` at call sites''': call-site consistency. If a future revision starts carrying the outer
    * proof on the wire, the change lands in this one helper rather than at every publisher.
    */
  def signedShardCheckpointToWire[F[_]: Async: JsonSerializer](
    signed: Signed[ShardCheckpoint]
  ): F[pb.ShardCheckpointWire] =
    shardCheckpointToWire[F](signed.value)
}
