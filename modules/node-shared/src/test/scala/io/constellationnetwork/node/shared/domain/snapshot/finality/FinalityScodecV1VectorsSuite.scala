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
        "056ffdde2f035094a92731cb70961bb01d20a82146a7d4af375a37d706f29356"
      ),
      FrozenVector(
        "FinalityCoreBatchPayload",
        encode(finalityCoreBatchPayloadCodec, coreBatch),
        8921,
        "ad400432e434196d6f1c57f584adbdb8282bed64d0b3d565a7e3cc7ea8515d1c"
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
        532,
        "3582c2f536526a2156e10cea422e4e304a2f8528e81fbf05d3f020d8b5f956b4"
      ),
      FrozenVector(
        "FinalityEffectManifestPayload",
        encode(finalityEffectManifestPayloadCodec, effectManifest),
        12298,
        "2ddf0ee8bf992a04ae848d04b30964358c69c674200c4b769162b517096d1d73"
      ),
      FrozenVector(
        "TerminalEffectReceiptPayload",
        encode(terminalEffectReceiptPayloadCodec, terminalEffectReceipt),
        185,
        "e7dc26412dbb13854f0dd01dc2c780e6621cad5efbb041eee61c5786fd401be1"
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
        "d257744df0d3ffdba09b331abc5392cef1c89222fa35d43ac8cbf8f85fe8e146"
      ),
      FrozenVector(
        "CoordinatorHeadPayload",
        encode(coordinatorHeadPayloadCodec, coordinatorHead),
        9772,
        "4ed39d8be8f36451a86e78296ca97d1acb4f6636fbd7fbfb990e66c86761f2de"
      ),
      FrozenVector(
        "CoordinatorAuditRecordPayload",
        encode(coordinatorAuditRecordPayloadCodec, auditRecord),
        19481,
        "d0ac748ff3ab830b4c3da9efea63c876d2b1292563d76210e290c2b82c6322ca"
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
