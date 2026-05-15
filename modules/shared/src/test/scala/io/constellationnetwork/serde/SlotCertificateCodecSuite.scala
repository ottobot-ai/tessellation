package io.constellationnetwork.serde

import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.codecs.instances.NakamotoSlotCodecs._
import io.constellationnetwork.serde.implicits._

import weaver.FunSuite

/** Round-trip suite for `SlotCertificate`.
  *
  * Guards the field layout — every certificate field, *including* `parentSlot`, must survive an encode/decode cycle byte-for-byte. The
  * verifier reconstructs slotGap from `cert.parentSlot`; if the codec ever drops or transposes this field the LDD eligibility check would
  * silently use a stale gap value.
  */
object SlotCertificateCodecSuite extends FunSuite {

  private def sampleCert(slot: Long, parentSlot: Long): SlotCertificate =
    SlotCertificate(
      slot = Slot.unsafeApply(slot),
      parentSlot = Slot.unsafeApply(parentSlot),
      vrfProof = VrfProof(Hex("0a" * 80)),
      vrfOutput = VrfOutput(Hex("0b" * 64)),
      vrfPublicKey = VrfPublicKey(Hex("0c" * 32)),
      eta = Hash("d" * 64),
      activePoolSize = 7,
      activePoolHash = Hash("e" * 64)
    )

  test("SlotCertificate round-trips a non-genesis parentSlot through scodec") {
    val cert = sampleCert(slot = 1234L, parentSlot = 1229L)
    val decoded = cert.immutableBytes.fromImmutableBytes[SlotCertificate]
    expect(decoded == Right(cert))
  }

  test("SlotCertificate.parentSlot is preserved across the round-trip (regression for #134)") {
    // If `parentSlot` were ever silently rewritten to Slot.MinValue or transposed
    // with `slot`, the verifier would compute slotGap = cert.slot - 0 instead of
    // cert.slot - cert.parentSlot — accepting (or rejecting) snapshots under the
    // wrong LDD threshold. Pin both directions.
    val cert = sampleCert(slot = 5000L, parentSlot = 4995L)
    val decoded = cert.immutableBytes.fromImmutableBytes[SlotCertificate]
    decoded match {
      case Right(c) =>
        expect(c.slot == cert.slot)
          .and(expect(c.parentSlot == cert.parentSlot))
          .and(expect(c.slot != c.parentSlot))
      case Left(err) =>
        failure(s"unexpected decode failure: $err")
    }
  }

  test("SlotCertificate round-trips genesis parentSlot=0 too (post-fix backward compat)") {
    // Pre-fix, all certs were emitted with parentSlot=0 (Slot.MinValue). Decoding
    // an old wire payload must still produce a valid certificate, even though the
    // slotGap reconstruction would be wrong for non-genesis snapshots.
    val cert = sampleCert(slot = 1L, parentSlot = 0L)
    val decoded = cert.immutableBytes.fromImmutableBytes[SlotCertificate]
    expect(decoded == Right(cert))
  }
}
