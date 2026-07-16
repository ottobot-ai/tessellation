package io.constellationnetwork.node.shared.domain.snapshot.finality

import java.security.MessageDigest

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityBaseCodecs._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCodecFixtures._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoordinatorCodecs._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoreCodecs._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityEffectCodecs._

import scodec.Codec
import weaver.FunSuite

object FinalityScodecV1VectorsSuite extends FunSuite {

  private final case class FrozenVector(
    name: String,
    bytes: Array[Byte],
    expectedLength: Int,
    expectedSha256: String
  )

  private def encode[A](codec: Codec[A], value: A): Array[Byte] =
    codec.encode(value).require.toByteArray

  private def sha256(bytes: Array[Byte]): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(bytes)
      .iterator
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  private val vectors: List[FrozenVector] =
    List(
      FrozenVector(
        "PathManifestPayload",
        encode(pathManifestPayloadCodec, pathManifestPayload),
        249,
        "b57b59e67e175fd2252972ed4343fce6a34438b40d267e1435b1ada13cc24fe3"
      ),
      FrozenVector(
        "PathChunkPayload",
        encode(pathChunkPayloadCodec, pathChunk),
        283,
        "18f4b7da951cfc3d0afafe3e7b054503286555ca70de56da1804a1ca31400bd2"
      ),
      FrozenVector(
        "IntentScopePayload",
        encode(intentScopePayloadCodec, intentScope),
        6949,
        "48421fa22242232e1f7a35d9f5908554bb1809525fd3ca2957878e44859d2d3c"
      ),
      FrozenVector(
        "FinalityCoreBatchPayload",
        encode(finalityCoreBatchPayloadCodec, coreBatch),
        8921,
        "238de2d25e63fdd787a47a1cf56debb75fb52b84cf7dcaa9e420c36fc8b11ce0"
      ),
      FrozenVector(
        "ReleasedCoreRecordPayload",
        encode(releasedCoreRecordPayloadCodec, releasedRecordPayload),
        1103,
        "8e2a1818b965d68429150f6b1f63552a86c4460482c7f9d7ab697a959246090b"
      ),
      FrozenVector(
        "EffectCommandIdentity",
        encode(effectCommandIdentityCodec, effectCommands.head.identityPreimage),
        532,
        "1539c7567683f592c1ae33d2c06e106f2c314a20223c61fce962430355d203a6"
      ),
      FrozenVector(
        "FinalityEffectManifestPayload",
        encode(finalityEffectManifestPayloadCodec, effectManifest),
        12298,
        "25beb0e7aa289464eb21e586ae0df506ec95de1358a12c64f21e984fea11b57b"
      ),
      FrozenVector(
        "TerminalEffectReceiptPayload",
        encode(terminalEffectReceiptPayloadCodec, terminalEffectReceipt),
        185,
        "f65d5ea2a78e32096250e8cad373844c3701424d84fe15e338524b5e095c7474"
      ),
      FrozenVector(
        "FinalityEffectOutboxHeadPayload",
        encode(finalityEffectOutboxHeadPayloadCodec, effectOutboxHead),
        90,
        "f807b1722f5fe1398896599925a9eb07c6cba6ae1f28359d6039f8a638f4ef79"
      ),
      FrozenVector(
        "RecoveryRecordPayload",
        encode(recoveryRecordPayloadCodec, recoveryRecord),
        9765,
        "5aba40ef8417d55c373b50ae76d25a8a808316285b7a6a72eca7914d95c3917f"
      ),
      FrozenVector(
        "CoordinatorHeadPayload",
        encode(coordinatorHeadPayloadCodec, coordinatorHead),
        9772,
        "fd8bc51996bf66262b6ab68d10b0f4363aebaa7e8d3663165d1d4060f09e8eb0"
      ),
      FrozenVector(
        "CoordinatorAuditRecordPayload",
        encode(coordinatorAuditRecordPayloadCodec, auditRecord),
        19481,
        "cf7f35ecf5d0c231b0eece88403d8440cece5fb557dd6517a56fe2874f265d8a"
      )
    )

  // These vectors detect accidental local schema drift; they are not independent-implementation proofs.
  vectors.foreach { vector =>
    test(s"${vector.name} retains its frozen ScodecV1 length and digest") {
      expect.all(
        clue(vector.bytes.length) == vector.expectedLength,
        clue(sha256(vector.bytes)) == vector.expectedSha256
      )
    }
  }
}
