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
        6981,
        "564de07fcf5f57afa0521bd9c238dc52a65005ca631cf9787970b8c195b6969c"
      ),
      FrozenVector(
        "FinalityCoreBatchPayload",
        encode(finalityCoreBatchPayloadCodec, coreBatch),
        8953,
        "762107de6b224fbf9be0b6f7540066a92c8b6d7016f39fa5a147b9eb6f2bcf32"
      ),
      FrozenVector(
        "ReleasedCoreRecordPayload",
        encode(releasedCoreRecordPayloadCodec, releasedRecordPayload),
        1103,
        "2a9352ba60c5ad3a0a5ae9597e2d16c2ba674e9bd8e6518b84633d5b567f7f82"
      ),
      FrozenVector(
        "EffectCommandIdentity",
        encode(effectCommandIdentityCodec, effectCommands.head.identityPreimage),
        564,
        "8f4d24e2b06ce52f9e966d5460e166cf8fcfc6d15e669fc95ce385c6eae02527"
      ),
      FrozenVector(
        "FinalityEffectManifestPayload",
        encode(finalityEffectManifestPayloadCodec, effectManifest),
        12906,
        "4ec6dd9095bacd2c85537d534048a27179385202df5f16d40fd31ba943e07a11"
      ),
      FrozenVector(
        "TerminalEffectReceiptPayload",
        encode(terminalEffectReceiptPayloadCodec, terminalEffectReceipt),
        185,
        "609fa6a2dae5004d7709920b3a293407b11be0c6e7defedce805527842cef2bf"
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
        9797,
        "3ee689f2f73985ca6a2e2a95e4147b9abc82de748ab7aaf413666e77348c7a3a"
      ),
      FrozenVector(
        "CoordinatorHeadPayload",
        encode(coordinatorHeadPayloadCodec, coordinatorHead),
        9804,
        "f30fd099da37f481c8f8e6839608681e3cd2737a4a69f19f1c91c6a4156b65fc"
      ),
      FrozenVector(
        "CoordinatorAuditRecordPayload",
        encode(coordinatorAuditRecordPayloadCodec, auditRecord),
        19545,
        "744cf687c3f72ef8dc276e2651fdb07effa6c29ff87510c5eb42782e7dd7ad3f"
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
