package io.constellationnetwork.serde

import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.smt.{AbsenceWitness, SmtProof, SmtSibling}
import io.constellationnetwork.serde.codecs.instances.SmtProofCodec
import io.constellationnetwork.serde.codecs.instances.SmtProofCodec._
import io.constellationnetwork.serde.implicits._

import scodec.bits.ByteVector
import weaver.FunSuite

/** Round-trip + discriminator-byte suite for the EXPLICIT scodec [[SmtProofCodec]] (Slice B). The codec is carried per-ordinal on the gl0 →
  * ml0 changeset wire; this suite locks its byte layout:
  *   - `SmtProof`: uint8 discriminator {Inclusion=0, Absence=1}.
  *   - `AbsenceWitness`: uint8 discriminator {Default=0, OtherLeaf=1}.
  *
  * Round-trip is asserted via the canonicality law (`decode(encode(x))` re-encodes to the same bytes) — `SmtProof.Inclusion.value:
  * Array[Byte]` has reference equality, so a structural `==` would spuriously fail; the suite additionally uses `SmtProof.eq` (content-wise
  * value compare) for the structural check.
  */
object SmtProofCodecSuite extends FunSuite {

  private def h(n: Int): Hash = Hash("00000000000000000000000000000000000000000000000000000000000000" + f"$n%02x")

  private def roundTrips(p: SmtProof): Boolean = {
    val bytes = SmtProofCodec.codec.encode(p).require
    SmtProofCodec.codec.decode(bytes).require.value match {
      case decoded => SmtProof.eq.eqv(decoded, p) && SmtProofCodec.codec.encode(decoded).require == bytes
    }
  }

  test("Inclusion (with empty-hash sibling + non-trivial value bytes) round-trips and tags 0x00") {
    val p: SmtProof = SmtProof.Inclusion(
      key = Hex("00000000000000ab"),
      value = Array[Byte](1, 2, 3, -1, 0, 127),
      valueDigest = h(7),
      siblings = List(SmtSibling(h(1)), SmtSibling(Hash.empty), SmtSibling(h(3)))
    )
    expect(p.immutableBytes.head == 0x00.toByte) && expect(roundTrips(p))
  }

  test("Inclusion with empty value + no siblings round-trips") {
    val p: SmtProof = SmtProof.Inclusion(
      key = Hex("0000000000000001"),
      value = Array.emptyByteArray,
      valueDigest = Hash.empty,
      siblings = Nil
    )
    expect(roundTrips(p))
  }

  test("Absence/Default round-trips and tags 0x01 with witness sub-tag 0x00") {
    val p: SmtProof = SmtProof.Absence(
      key = Hex("00000000000000cd"),
      witness = AbsenceWitness.Default,
      siblings = List(SmtSibling(h(9)))
    )
    val bytes = p.immutableBytes
    // byte 0 = SmtProof tag (0x01 Absence); then key (uint16-len + bytes); then witness tag (0x00 Default).
    expect(bytes.head == 0x01.toByte) &&
    expect(roundTrips(p))
  }

  test("Absence/OtherLeaf round-trips") {
    val p: SmtProof = SmtProof.Absence(
      key = Hex("00000000000000ef"),
      witness = AbsenceWitness.OtherLeaf(occupyingKey = Hex("00000000000000aa"), occupyingDataDigest = h(42)),
      siblings = List(SmtSibling(h(5)), SmtSibling(h(6)))
    )
    expect(roundTrips(p))
  }

  test("AbsenceWitness.Default encodes as single byte 0x00") {
    expect(SmtProofCodec.absenceWitnessCodec.encode(AbsenceWitness.Default).require.toByteVector == ByteVector.fromValidHex("00"))
  }

  test("AbsenceWitness.OtherLeaf tags 0x01") {
    val w: AbsenceWitness = AbsenceWitness.OtherLeaf(Hex("00000000000000aa"), h(3))
    val bytes = SmtProofCodec.absenceWitnessCodec.encode(w).require.toByteVector
    expect(bytes.head == 0x01.toByte)
  }
}
