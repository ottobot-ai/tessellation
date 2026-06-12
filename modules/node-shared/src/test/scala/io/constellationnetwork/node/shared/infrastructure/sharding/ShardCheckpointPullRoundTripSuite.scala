package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.syntax._
import weaver.MutableIOSuite

/** Correctness guard for the shard-checkpoint chain-sync PULL path (run-20, task #A —
  * `docs/nakamoto/SHARD-CHECKPOINT-CHAINSYNC-DESIGN.md`).
  *
  * The serve route returns a `Signed[ShardCheckpoint]` as JSON; the puller decodes it and the daemon converts it to the wire form before
  * re-feeding it through `handleShardCheckpoint` (which decodes the wire and hashes the `signingPreimage`). For a pulled checkpoint to reach
  * the SAME canonical hash a gossiped one does — the hard requirement for attestations to concentrate and quorum to form — both round-trips
  * must preserve the checkpoint identity:
  *   1. JSON encode → decode (the serve route ↔ puller transport), and
  *   2. `signedShardCheckpointToWire` → `shardCheckpointFromWire` (the gossip transport, already proven hash-stable in production).
  *
  * This asserts the FULL pull path `signed → JSON → signed → wire → checkpoint` recomputes the identical `signingPreimage` hash. A regression
  * here would silently fork a pulled checkpoint into a different `byHash` key and the pull would never help quorum.
  */
object ShardCheckpointPullRoundTripSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def sentinelProof: SignatureProof =
    SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70)))

  private def mkCommitteeSig(peerByte: Int): CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = PeerId(Hex(f"$peerByte%02x" * 64)),
      vrfProof = Hex("aa" * 80),
      ed25519Sig = Hex("bb" * 64),
      kesProductSig = Hex("cc" * 128),
      kesTreeStep = 3
    )

  private def mkSignedCheckpoint(ord: Long, parent: io.constellationnetwork.security.hash.Hash, gl0Anchor: Long): Signed[ShardCheckpoint] = {
    val cp = ShardCheckpoint(
      shardId = ShardId.unsafeApply(1),
      parentCheckpointHash = parent,
      shardOrdinal = ShardOrdinal(ord),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(gl0Anchor)),
      slot = SlotT.unsafeApply(gl0Anchor),
      derivedStateDelta = ShardDerivedStateDelta.empty,
      emittedReceipts = List.empty,
      committeeSignatures = NonEmptyList.of(mkCommitteeSig(7), mkCommitteeSig(9)),
      epoch = EtaPeriod(0L)
    )
    Signed(cp, NonEmptySet.of(sentinelProof))
  }

  test("pull round-trip: signed → JSON → signed → wire → checkpoint preserves the signingPreimage hash") { res =>
    implicit val (h, sp, j) = res

    val original = mkSignedCheckpoint(2L, io.constellationnetwork.security.hash.Hash("deadbeef" * 8), 42L)

    for {
      // The hash the gossip/producer path computes for the original.
      originalHash <- h.hash(original.value.signingPreimage)
      // (1) JSON round-trip — the serve route encodes, the puller decodes.
      decoded <- IO.fromEither(original.asJson.as[Signed[ShardCheckpoint]])
      // (2) wire round-trip — the daemon converts to wire, handleShardCheckpoint decodes the wire.
      wire <- ShardCheckpointWireCodecs.signedShardCheckpointToWire[IO](decoded)
      reconstructed <- ShardCheckpointWireCodecs.shardCheckpointFromWire[IO](wire)
      pulledHash <- h.hash(reconstructed.signingPreimage)
    } yield
      expect.all(
        decoded.value == original.value, // JSON round-trip is field-exact
        pulledHash == originalHash // the full pull path reaches the identical canonical hash
      )
  }
}
