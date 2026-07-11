package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.kernel.Eq

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWireCodecs.ShardCheckpointAttestation
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import com.google.protobuf.ByteString
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Wire-format round-trip suite for [[ShardCheckpointWireCodecs]] — Slice 14 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md`
  * §6.4.
  *
  * '''Property under test (round-trip).''' For every fixture value `v`:
  *   - encode via the codec ⇒ `pb.<Wire>` proto
  *   - serialize that proto to bytes via the scalapb runtime
  *   - parse the bytes back via the scalapb runtime
  *   - decode via the codec ⇒ schema-side value `v'`
  *   - assert `v === v'` via the cats `Eq` instance (NOT scala `==` — the schema types carry `Array[Byte]` content that fails Scala
  *     reference-equality after round-trip; cats `Eq` routes through the project's `arrayEq` instance).
  *
  * This is the canonical wire-format reservation per the task spec: any future regression that silently drops a field, reorders, or
  * inflates/deflates the JSON-blob fields surfaces as a clear failure here rather than as a runtime divergence between operators on a live
  * cluster.
  *
  * '''Why `MutableIOSuite`''' — the codec needs a `JsonSerializer[IO]` (for the JSON-bytes fields) and a `Hasher[IO]` (not strictly needed
  * for codec round-trip, but mirrored from `ShardCheckpointProducerSuite` so the fixture layer is consistent). Both come from the shared
  * resource. The Circe-codec round-trip tests in [[ShardingCodecsSuite]] use `FunSuite` because they don't need `IO`; this suite does.
  *
  * '''What this suite covers''' (per task brief):
  *   - Wire round-trip: `ShardCheckpoint → ShardCheckpointWire → bytes → ShardCheckpointWire → ShardCheckpoint` equality.
  *   - NonEmptyList preservation: single-element NEL, multi-element NEL with key ordering, both at the outer SortedMap key set and the
  *     inner per-MG NEL.
  *   - `CommitteeMemberSignatureWire` per-signature round-trip (with the multi-signature aggregate exercised inside the checkpoint
  *     round-trip).
  *   - `ShardCheckpointAttestationWire` round-trip.
  *
  * '''What this suite does NOT cover''' — wire-byte determinism (the JSON-blob fields are deterministic via `JsonSerializer.forAsync`'s
  * sortKeys+dropNullValues printer, exercised in `ShardingCodecsSuite`; the proto-level encoding is scalapb's default — order-preserving on
  * `repeated` fields, no key reshuffling on `message`), and signature verification (separate concern; Slice 9 acceptance manager).
  */
object ShardCheckpointWireCodecsSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, j)

  // ===========================================================================
  // Fixtures — same shape as `ShardingCodecsSuite` for parity with Slice 1 tests
  //
  // Deterministic, distinct values per field so a "field swap" bug surfaces as a value mismatch rather than silent success.
  // ===========================================================================

  private def hex(s: String): Hex = Hex(s)
  private def hash(seed: Char): Hash = Hash(seed.toString * 64) // 64 hex chars = 32 bytes
  private def addr(tag: String): Address = Address.fromBytes(tag.getBytes("UTF-8"))

  private val mgAddrA: Address = addr("mg-aaa")
  private val mgAddrB: Address = addr("mg-bbb")

  private def peerIdN(n: Int): PeerId =
    PeerId(Hex(n.toHexString.padTo(2, '0') * 64))

  private val shardZero: ShardId = ShardId.unsafeApply(0)
  private val shardOne: ShardId = ShardId.unsafeApply(1)

  private def mkSig(peerN: Int, step: Int): CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = peerIdN(peerN),
      vrfProof = hex("aa" * 80),
      ed25519Sig = hex("bb" * 64),
      kesProductSig = hex("cc" * 128),
      kesTreeStep = step
    )

  private def mkScsb(lastHashSeed: Char, contentByte: Byte, fee: Long): StateChannelSnapshotBinary =
    StateChannelSnapshotBinary(
      lastSnapshotHash = hash(lastHashSeed),
      content = Array.fill(8)(contentByte),
      fee = SnapshotFee(NonNegLong.unsafeFrom(fee))
    )

  private def mkSigned[A](value: A): Signed[A] = {
    val proof = SignatureProof(Id(hex("11" * 64)), Signature(hex("22" * 70)))
    Signed(value, NonEmptySet.of(proof))
  }

  private def mkSignedBinary(seed: Char, contentByte: Byte, fee: Long): Signed[StateChannelSnapshotBinary] =
    mkSigned(mkScsb(seed, contentByte, fee))

  // A multi-element delta that exercises both replay-authorized fields of `ShardDerivedStateDelta`.
  private def sampleDelta: ShardDerivedStateDelta =
    ShardDerivedStateDelta(
      perMetagraphMptRoots = SortedMap(
        mgAddrA -> hash('a'),
        mgAddrB -> hash('b')
      ),
      includedSnapshots = SortedMap(
        mgAddrA -> NonEmptyList.of(mkSignedBinary('1', 0x01, 10L), mkSignedBinary('2', 0x02, 20L)),
        mgAddrB -> NonEmptyList.of(mkSignedBinary('3', 0x03, 30L))
      )
    )

  private def sampleCheckpoint: ShardCheckpoint =
    ShardCheckpoint(
      shardId = shardZero,
      parentCheckpointHash = hash('p'),
      shardOrdinal = ShardOrdinal(42L),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong(99L)),
      slot = SlotT.unsafeApply(99L),
      derivedStateDelta = sampleDelta,
      committeeSignatures = NonEmptyList.of(mkSig(1, 7), mkSig(2, 8), mkSig(3, 9)),
      epoch = EtaPeriod(5L)
    )

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /** Serialise a scalapb message to bytes and parse it back via the same generated companion. Lets each round-trip test exercise the full
    * wire encoding path, not just the in-memory codec adapters.
    */
  private def viaProtoBytes[A <: scalapb.GeneratedMessage](
    msg: A,
    companion: scalapb.GeneratedMessageCompanion[A]
  ): A = {
    val bytes = msg.toByteArray
    companion.parseFrom(bytes)
  }

  // Lightweight expect helper that pretty-prints a side-by-side mismatch
  private def assertEq[A: Eq](label: String, expected: A, actual: A): weaver.Expectations =
    expect(Eq[A].eqv(expected, actual))
      .traced(weaver.SourceLocation.fromContext)
      .or(failure(s"$label: round-trip mismatch.\nExpected: $expected\nActual:   $actual"))

  // ===========================================================================
  // Test 1 — ShardCheckpoint full round-trip
  // ===========================================================================

  test("ShardCheckpoint round-trips via codec → proto → bytes → proto → codec") { res =>
    implicit val (_, j) = res
    val cp = sampleCheckpoint
    for {
      wire <- ShardCheckpointWireCodecs.shardCheckpointToWire[IO](cp)
      reparsed = viaProtoBytes(wire, pb.ShardCheckpointWire)
      decoded <- ShardCheckpointWireCodecs.shardCheckpointFromWire[IO](reparsed)
    } yield assertEq("ShardCheckpoint", cp, decoded)
  }

  // ===========================================================================
  // Test 5 — CommitteeMemberSignatureWire per-signature round-trip
  //
  // Standalone signature round-trip: encode one CommitteeMemberSignature, serialize to bytes, parse back, decode, assert equality. Covers
  // the wire shape's individual fields (peerId, vrfProof, ed25519Sig, kesProductSig, kesTreeStep) independently of the surrounding
  // checkpoint.
  // ===========================================================================

  test("CommitteeMemberSignature round-trips via codec → proto → bytes → proto → codec") { _ =>
    val sig = mkSig(42, 17)
    val wire = ShardCheckpointWireCodecs.committeeSignatureToWire(sig)
    val reparsed = viaProtoBytes(wire, pb.CommitteeMemberSignatureWire)
    val decoded = ShardCheckpointWireCodecs.committeeSignatureFromWire(reparsed)
    IO.pure(assertEq("CommitteeMemberSignature", sig, decoded))
  }

  test("CommitteeMemberSignature round-trips with empty Hex fields") { _ =>
    // Edge case: pre-Slice-9 KES rollout may produce empty kesProductSig. The wire codec must not corrupt the empty Hex; round-trip yields
    // the same empty Hex (note: scalapb `bytes` field defaults to ByteString.EMPTY which decodes back to `Hex("")` via
    // `Hex.fromBytes(Array.empty[Byte])` → empty string).
    val sigEmpty = CommitteeMemberSignature(
      peerId = peerIdN(0),
      vrfProof = Hex(""),
      ed25519Sig = Hex(""),
      kesProductSig = Hex(""),
      kesTreeStep = 0
    )
    val wire = ShardCheckpointWireCodecs.committeeSignatureToWire(sigEmpty)
    val reparsed = viaProtoBytes(wire, pb.CommitteeMemberSignatureWire)
    val decoded = ShardCheckpointWireCodecs.committeeSignatureFromWire(reparsed)
    IO.pure(
      expect.all(
        decoded.peerId == sigEmpty.peerId,
        decoded.vrfProof.value == "",
        decoded.ed25519Sig.value == "",
        decoded.kesProductSig.value == "",
        decoded.kesTreeStep == 0
      )
    )
  }

  // ===========================================================================
  // Test 6 — ShardCheckpointAttestation round-trip
  // ===========================================================================

  test("ShardCheckpointAttestation round-trips via codec → proto → bytes → proto → codec") { _ =>
    val att = ShardCheckpointAttestation(
      shardId = shardOne,
      checkpointHash = hash('z'),
      attesterSignature = mkSig(99, 12)
    )
    val wire = ShardCheckpointWireCodecs.shardCheckpointAttestationToWire(att)
    val reparsed = viaProtoBytes(wire, pb.ShardCheckpointAttestationWire)
    for {
      decoded <- ShardCheckpointWireCodecs.shardCheckpointAttestationFromWire[IO](reparsed)
    } yield assertEq("ShardCheckpointAttestation", att, decoded)
  }

  test("ShardCheckpointAttestation: missing attester_signature on decode raises") { _ =>
    val bogus = pb.ShardCheckpointAttestationWire(
      shardId = 0,
      checkpointHash = ByteString.copyFrom(Array.fill[Byte](32)(0x00)),
      attesterSignature = None
    )
    ShardCheckpointWireCodecs
      .shardCheckpointAttestationFromWire[IO](bogus)
      .attempt
      .map(res => expect(res.isLeft))
  }

  // ===========================================================================
  // Test 7 — ShardCheckpoint: missing derived_state_delta on decode raises
  //
  // The schema-side `ShardCheckpoint` has a required `derivedStateDelta` field. Receivers seeing a wire envelope without this field must
  // reject — silent fallback to `ShardDerivedStateDelta.empty` would let a malformed sender bypass the consensus contract.
  // ===========================================================================

  test("ShardCheckpoint: missing derived_state_delta on decode raises") { res =>
    implicit val (_, j) = res
    val bogus = pb.ShardCheckpointWire(
      shardId = 0,
      shardOrdinal = 0L,
      parentCheckpointHash = ByteString.EMPTY,
      gl0AnchorOrdinal = 0L,
      epoch = 0L,
      derivedStateDelta = None,
      committeeSignatures = Seq(
        ShardCheckpointWireCodecs.committeeSignatureToWire(mkSig(0, 0))
      )
    )
    ShardCheckpointWireCodecs
      .shardCheckpointFromWire[IO](bogus)
      .attempt
      .map(res => expect(res.isLeft))
  }

  test("ShardCheckpoint: empty committee_signatures on decode raises") { res =>
    implicit val (_, j) = res
    for {
      delta <- ShardCheckpointWireCodecs.derivedStateDeltaToWire[IO](ShardDerivedStateDelta.empty)
      bogus = pb.ShardCheckpointWire(
        shardId = 0,
        shardOrdinal = 0L,
        parentCheckpointHash = ByteString.EMPTY,
        gl0AnchorOrdinal = 0L,
        epoch = 0L,
        derivedStateDelta = Some(delta),
        committeeSignatures = Seq.empty // <-- intentional violation
      )
      attempted <- ShardCheckpointWireCodecs.shardCheckpointFromWire[IO](bogus).attempt
    } yield expect(attempted.isLeft)
  }

  // ===========================================================================
  // Test 9 — ShardDerivedStateDelta opaque JSON round-trip
  //
  // The wire shape carries the whole delta as one JSON blob. Encode → bytes → decode must preserve every nested field; this exercises the
  // `ShardDerivedStateDeltaWire` codec independently of the surrounding checkpoint.
  // ===========================================================================

  test("ShardDerivedStateDelta round-trips via wire (opaque JSON)") { res =>
    implicit val (_, j) = res
    for {
      wire <- ShardCheckpointWireCodecs.derivedStateDeltaToWire[IO](sampleDelta)
      reparsed = viaProtoBytes(wire, pb.ShardDerivedStateDeltaWire)
      decoded <- ShardCheckpointWireCodecs.derivedStateDeltaFromWire[IO](reparsed)
    } yield expect(Eq[ShardDerivedStateDelta].eqv(decoded, sampleDelta))
  }
}
