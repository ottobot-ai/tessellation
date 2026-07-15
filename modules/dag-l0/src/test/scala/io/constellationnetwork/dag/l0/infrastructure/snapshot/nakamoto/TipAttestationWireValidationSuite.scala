package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar.TipAttestation

import com.google.protobuf.ByteString
import weaver.FunSuite

object TipAttestationWireValidationSuite extends FunSuite {

  private def att(
    slot: Long = 1L,
    ordinal: Long = 1L,
    attestedAt: Long = 1L,
    attesterId: Array[Byte] = Array.fill[Byte](64)(1)
  ): TipAttestation =
    TipAttestation(
      tipHash = ByteString.copyFromUtf8("00" * 32),
      tipSlot = slot,
      tipOrdinal = ordinal,
      attestedAt = attestedAt,
      attesterId = ByteString.copyFrom(attesterId),
      signature = ByteString.copyFromUtf8("signature")
    )

  test("SER-002G: negative tip slot is a typed rejection, not an unsafe refined-constructor throw") {
    expect.same(Left("negative tipSlot=-1"), NakamotoSyncDaemon.decodeTipAttestation(att(slot = -1L)).map(_.domain))
  }

  test("SER-002G: negative tip ordinal is rejected before hashing and signature verification") {
    expect.same(Left("negative tipOrdinal=-1"), NakamotoSyncDaemon.decodeTipAttestation(att(ordinal = -1L)).map(_.domain))
  }

  test("SER-002G: negative attestation timestamp is rejected before tracker mutation") {
    expect.same(Left("negative attestedAt=-1"), NakamotoSyncDaemon.decodeTipAttestation(att(attestedAt = -1L)).map(_.domain))
  }

  test("SER-002G: attester_id must contain exactly 64 raw public-key bytes") {
    val invalidLengths = List(0, 63, 65)
    val results = invalidLengths.map { length =>
      length -> NakamotoSyncDaemon.decodeTipAttestation(att(attesterId = Array.fill[Byte](length)(1)))
    }

    expect.all(
      results.forall { case (_, result) => result.isLeft },
      results.forall { case (length, result) => result.left.exists(_.contains(s"length=$length")) }
    )
  }

  test("SER-002G: nonnegative wire coordinates decode") {
    val decoded = NakamotoSyncDaemon.decodeTipAttestation(att(slot = 2L, ordinal = 3L, attestedAt = 4L))
    expect.all(
      decoded.exists(_.domain.tipSlot.value.value == 2L),
      decoded.exists(_.domain.tipOrdinal == 3L),
      decoded.exists(_.domain.attestedAt == 4L)
    )
  }
}
