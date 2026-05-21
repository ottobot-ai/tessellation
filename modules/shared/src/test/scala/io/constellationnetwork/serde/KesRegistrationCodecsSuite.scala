package io.constellationnetwork.serde

import cats.data.NonEmptySet

import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.KesRegistrationCodecs._
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.bits.ByteVector
import weaver.FunSuite

/** §1.2 Slice 10 (#179) — wire-format canary suite for the KES runtime-registration codec family.
  *
  * Pins the on-wire shape of every codec exported by `KesRegistrationCodecs`:
  *
  *   - `KesRegistrationOrdinal` — newtype-over-NonNegLong, 8 bytes big-endian.
  *   - `KesRegistrationReference` — `(ordinal:8 | hash:32)`, 40 bytes total, mirroring the `*Reference` family layout.
  *   - `KesRegistrationCert` — full operator-cert body.
  *   - `Signed[KesRegistrationCert]` — universal signed-envelope around the body.
  *   - `KesRegistrationRecord` — `Signed[KesRegistrationCert] + SnapshotOrdinal`, the persisted record shape.
  *   - `SortedSet[KesRegistrationRecord]` — composes through `SortedSetCodec` (validated indirectly via the state-manager suite, which
  *     round-trips set-of-records through `MptStore`).
  *
  * '''Why golden hex vectors?''' The assertion's purpose is to LOCK the wire shape. Any silent change to one of these codecs — or to a
  * transitively-referenced codec (`hashCodec`, `peerIdCodec`, `signedCodec`, ...) — will break the corresponding test. A contributor who
  * intentionally adds a field has to update the golden vector explicitly; the version bump is then visible in code review.
  *
  * '''Why inline `ByteVector.fromValidHex(...)` instead of separate hex files in `golden/serde/`?''' The Slice 10 cert family hasn't been
  * frozen on chain yet (this is the first persistence iteration). Once the first accepted runtime cert lands at network birth and we want
  * to defend cross-version compatibility, the goldens can be promoted to files following the README at
  * `test/resources/golden/serde/README.md`. Until then, inline literals keep the test self-contained and reviewable in one place.
  */
object KesRegistrationCodecsSuite extends FunSuite {

  // -----------------------------------------------------------------------------------------------
  // Sample constructors. Deterministic — no random / time-dependent fields.
  // -----------------------------------------------------------------------------------------------

  private def opPeerId: PeerId = PeerId(Hex("aabb"))
  private def vkBytes: Hex = Hex("cafebabe")
  private def proof: SignatureProof = SignatureProof(Id(Hex("11")), Signature(Hex("22")))

  private def sampleOrdinal: KesRegistrationOrdinal = KesRegistrationOrdinal(NonNegLong.unsafeFrom(7L))

  private def sampleReference: KesRegistrationReference =
    KesRegistrationReference(KesRegistrationOrdinal(NonNegLong.unsafeFrom(42L)), Hash("a" * 64))

  private def sampleCert: KesRegistrationCert =
    KesRegistrationCert(
      operatorPeerId = opPeerId,
      kesMasterVK = vkBytes,
      kesMasterVKStep = 3,
      offset = 5L,
      effectiveFromEpoch = EpochProgress(NonNegLong.unsafeFrom(100L)),
      ordinal = sampleOrdinal,
      parent = KesRegistrationReference.empty
    )

  private def sampleSigned: Signed[KesRegistrationCert] =
    Signed(sampleCert, NonEmptySet.of(proof))

  private def sampleRecord: KesRegistrationRecord =
    KesRegistrationRecord(sampleSigned, SnapshotOrdinal(NonNegLong.unsafeFrom(11L)))

  // -----------------------------------------------------------------------------------------------
  // KesRegistrationOrdinal — newtype over NonNegLong. 8-byte big-endian.
  // -----------------------------------------------------------------------------------------------

  test("KesRegistrationOrdinal: 8 bytes big-endian, round-trips, golden vector") {
    val bytes = sampleOrdinal.immutableBytes
    // 7L big-endian uint64 = 0x0000000000000007
    val golden = ByteVector.fromValidHex("0000000000000007")
    expect(bytes.length == 8L)
      .and(expect(bytes == golden))
      .and(expect(bytes.fromImmutableBytes[KesRegistrationOrdinal] == Right(sampleOrdinal)))
  }

  test("KesRegistrationOrdinal: zero encodes to 8 zero bytes") {
    val zero = KesRegistrationOrdinal(NonNegLong.MinValue)
    expect(zero.immutableBytes == ByteVector.fromValidHex("0000000000000000"))
  }

  // -----------------------------------------------------------------------------------------------
  // KesRegistrationReference — (ordinal:8 | hash:32), 40 bytes. Mirrors AllowSpendReference layout.
  // -----------------------------------------------------------------------------------------------

  test("KesRegistrationReference: 40 bytes, ordinal-first, round-trips, golden vector") {
    val bytes = sampleReference.immutableBytes
    // ordinal=42 → 0x000000000000002a, then hash 32 bytes (all 0xaa).
    val golden = ByteVector.fromValidHex("000000000000002a" + "aa" * 32)
    expect(bytes.length == 40L)
      .and(expect(bytes == golden))
      .and(expect(bytes.fromImmutableBytes[KesRegistrationReference] == Right(sampleReference)))
  }

  test("KesRegistrationReference.empty: 40 bytes of zeros") {
    val bytes = KesRegistrationReference.empty.immutableBytes
    val golden = ByteVector.fromValidHex("00" * 40)
    expect(bytes == golden)
      .and(expect(bytes.fromImmutableBytes[KesRegistrationReference] == Right(KesRegistrationReference.empty)))
  }

  // -----------------------------------------------------------------------------------------------
  // KesRegistrationCert — operator body. Layout:
  //   [peerId hex-len-prefixed | kesMasterVK hex-len-prefixed | vkStep:4 | offset:8 |
  //    effectiveFromEpoch:8 | ordinal:8 | parent:40]
  // -----------------------------------------------------------------------------------------------

  test("KesRegistrationCert: round-trips") {
    val bytes = sampleCert.immutableBytes
    val decoded = bytes.fromImmutableBytes[KesRegistrationCert]
    expect(decoded == Right(sampleCert))
  }

  test("KesRegistrationCert: golden vector locks the wire layout") {
    val bytes = sampleCert.immutableBytes
    // Build the expected bytes piecewise and assert structural correctness.
    // PeerId.codec writes a uint16 length prefix + hex bytes: "aabb" → "0002 aabb"
    // Hex codec same shape: "cafebabe" → "0004 cafebabe"
    val expected = ByteVector.fromValidHex(
      "0002aabb" +              // peerId: len=2 + bytes aabb
        "0004cafebabe" +        // kesMasterVK: len=4 + bytes cafebabe
        "00000003" +            // kesMasterVKStep: int32 = 3
        "0000000000000005" +    // offset: int64 = 5
        "0000000000000064" +    // effectiveFromEpoch: 100
        "0000000000000007" +    // ordinal: 7
        ("00" * 40)             // parent: KesRegistrationReference.empty
    )
    expect(bytes == expected)
  }

  test("KesRegistrationCert: changing a field changes the bytes") {
    val mutated = sampleCert.copy(kesMasterVKStep = 4)
    expect(sampleCert.immutableBytes != mutated.immutableBytes)
  }

  // -----------------------------------------------------------------------------------------------
  // Signed[KesRegistrationCert] — universal signed envelope: cert bytes + proofs.
  // -----------------------------------------------------------------------------------------------

  test("Signed[KesRegistrationCert]: round-trips with a single proof") {
    val bytes = sampleSigned.immutableBytes
    val decoded = bytes.fromImmutableBytes[Signed[KesRegistrationCert]]
    expect(decoded == Right(sampleSigned))
  }

  test("Signed[KesRegistrationCert]: golden vector locks the envelope shape") {
    val bytes = sampleSigned.immutableBytes
    // Cert bytes + uint16 proof count (1) + proof: (Id "11" → 0001 11) (Signature "22" → 0001 22)
    val certBytes =
      "0002aabb" +
        "0004cafebabe" +
        "00000003" +
        "0000000000000005" +
        "0000000000000064" +
        "0000000000000007" +
        ("00" * 40)
    val proofsBytes = "0001" + "000111" + "000122"
    val expected = ByteVector.fromValidHex(certBytes + proofsBytes)
    expect(bytes == expected)
  }

  // -----------------------------------------------------------------------------------------------
  // KesRegistrationRecord — Signed[KesRegistrationCert] + SnapshotOrdinal:8.
  // -----------------------------------------------------------------------------------------------

  test("KesRegistrationRecord: round-trips") {
    val bytes = sampleRecord.immutableBytes
    val decoded = bytes.fromImmutableBytes[KesRegistrationRecord]
    expect(decoded == Right(sampleRecord))
  }

  test("KesRegistrationRecord: golden vector — signed cert + acceptedAt int64") {
    val bytes = sampleRecord.immutableBytes
    val signedBytes = sampleSigned.immutableBytes
    val acceptedAtBytes = ByteVector.fromValidHex("000000000000000b") // SnapshotOrdinal(11)
    val expected = signedBytes ++ acceptedAtBytes
    expect(bytes == expected)
  }

  // -----------------------------------------------------------------------------------------------
  // Cross-codec sanity: encoding is sensitive to every field.
  // -----------------------------------------------------------------------------------------------

  test("KesRegistrationRecord: changing acceptedAt produces different bytes") {
    val mutated = sampleRecord.copy(acceptedAt = SnapshotOrdinal(NonNegLong.unsafeFrom(12L)))
    expect(sampleRecord.immutableBytes != mutated.immutableBytes)
  }

  test("Signed[KesRegistrationCert]: adding a second proof changes the bytes") {
    val proof2 = SignatureProof(Id(Hex("33")), Signature(Hex("44")))
    val withTwo = Signed(sampleCert, NonEmptySet.of(proof, proof2))
    expect(sampleSigned.immutableBytes != withTwo.immutableBytes)
  }
}
