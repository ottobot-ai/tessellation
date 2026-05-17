package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar.{Snapshot, TipAttestation}

import com.google.protobuf.ByteString
import weaver.SimpleIOSuite

/** §1.2 Slice 4 — schema-only roundtrip checks for the new `kes_signature` field on `pb.TipAttestation` and `pb.Snapshot`. The sender side
  * does not populate this field yet (Slice 6 will); the receiver side does not verify it yet (Slice 5 will). All we assert here is that the
  * scalapb-generated codec preserves the field across `toByteArray` / `parseFrom` so that future sender + receiver paths can rely on it.
  */
object SidecarProtoKesSignatureSuite extends SimpleIOSuite {

  pureTest("TipAttestation roundtrip preserves kesSignature when populated") {
    val att = TipAttestation(
      tipHash = ByteString.copyFromUtf8("h" * 32),
      tipSlot = 7L,
      tipOrdinal = 11L,
      attestedAt = 42L,
      attesterId = ByteString.copyFromUtf8("a" * 16),
      signature = ByteString.copyFromUtf8("sig"),
      kesSignature = ByteString.copyFromUtf8("kes-product-sig-bytes")
    )
    val roundtripped = TipAttestation.parseFrom(att.toByteArray)
    expect.all(
      roundtripped.kesSignature == att.kesSignature,
      roundtripped == att
    )
  }

  pureTest("TipAttestation roundtrip preserves empty kesSignature (pre-§1.2 sender)") {
    val att = TipAttestation(
      tipHash = ByteString.copyFromUtf8("h" * 32),
      tipSlot = 7L,
      tipOrdinal = 11L,
      attestedAt = 42L,
      attesterId = ByteString.copyFromUtf8("a" * 16),
      signature = ByteString.copyFromUtf8("sig")
      // kesSignature omitted ⇒ default ByteString.EMPTY
    )
    val roundtripped = TipAttestation.parseFrom(att.toByteArray)
    expect.all(
      roundtripped.kesSignature == ByteString.EMPTY,
      roundtripped == att
    )
  }

  pureTest("Snapshot roundtrip preserves kesSignature when populated") {
    val snap = Snapshot(
      hash = ByteString.copyFromUtf8("h" * 32),
      slot = 9L,
      ordinal = 3L,
      parentHash = ByteString.copyFromUtf8("p" * 32),
      vrfProof = ByteString.copyFromUtf8("vrf"),
      vrfPublicKey = ByteString.copyFromUtf8("vk"),
      eta = ByteString.copyFromUtf8("eta"),
      payload = ByteString.copyFromUtf8("payload"),
      producerId = ByteString.copyFromUtf8("prod"),
      parentSlot = 8L,
      kesSignature = ByteString.copyFromUtf8("kes-product-sig-bytes")
    )
    val roundtripped = Snapshot.parseFrom(snap.toByteArray)
    expect.all(
      roundtripped.kesSignature == snap.kesSignature,
      roundtripped == snap
    )
  }

  pureTest("Snapshot roundtrip preserves empty kesSignature (pre-§1.2 sender)") {
    val snap = Snapshot(
      hash = ByteString.copyFromUtf8("h" * 32),
      slot = 9L,
      ordinal = 3L,
      parentHash = ByteString.copyFromUtf8("p" * 32),
      payload = ByteString.copyFromUtf8("payload")
      // kesSignature omitted ⇒ default ByteString.EMPTY
    )
    val roundtripped = Snapshot.parseFrom(snap.toByteArray)
    expect.all(
      roundtripped.kesSignature == ByteString.EMPTY,
      roundtripped == snap
    )
  }
}
