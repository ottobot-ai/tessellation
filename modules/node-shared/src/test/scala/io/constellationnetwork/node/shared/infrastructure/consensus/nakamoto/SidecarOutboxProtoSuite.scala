package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._

import com.google.protobuf.ByteString
import weaver.SimpleIOSuite

/** Task #196 — schema-only roundtrip checks for the new durable-outbox surface:
  *
  *   - `pb.AllowSpendBlock` (the new gossip variant)
  *   - `pb.GossipMessage.allow_spend_block` oneof variant
  *   - `pb.ConfirmFinalizedRequest` / `pb.ConfirmFinalizedResponse`
  *   - `pb.PeerCountResponse.mesh_allow_spend_blocks`
  *
  * Schema-level guards so a typo or proto-renumber on either side of the wire shows up as a failing test
  * here rather than at runtime in an e2e. Mirrors the pattern used by [[SidecarProtoKesSignatureSuite]].
  */
object SidecarOutboxProtoSuite extends SimpleIOSuite {

  pureTest("AllowSpendBlock roundtrip preserves payload bytes") {
    val asb = AllowSpendBlock(payload = ByteString.copyFromUtf8("opaque-signed-bytes"))
    val rt = AllowSpendBlock.parseFrom(asb.toByteArray)
    expect.all(
      rt.payload == asb.payload,
      rt == asb
    )
  }

  pureTest("AllowSpendBlock roundtrip preserves empty payload (defensive)") {
    val asb = AllowSpendBlock()
    val rt = AllowSpendBlock.parseFrom(asb.toByteArray)
    expect.all(
      rt.payload == ByteString.EMPTY,
      rt == asb
    )
  }

  pureTest("GossipMessage.AllowSpendBlock oneof variant roundtrips") {
    val asb = AllowSpendBlock(payload = ByteString.copyFromUtf8("payload"))
    val gm = GossipMessage(body = GossipMessage.Body.AllowSpendBlock(asb))
    val rt = GossipMessage.parseFrom(gm.toByteArray)
    expect.all(
      rt == gm,
      rt.body.isAllowSpendBlock,
      rt.body.allowSpendBlock.exists(_.payload == asb.payload)
    )
  }

  pureTest("ConfirmFinalizedRequest preserves topic + message_ids in order") {
    val req = ConfirmFinalizedRequest(
      topic = "allow-spend-block",
      messageIds = Seq(
        ByteString.copyFrom(Array.fill[Byte](32)(0x01)),
        ByteString.copyFrom(Array.fill[Byte](32)(0x02))
      )
    )
    val rt = ConfirmFinalizedRequest.parseFrom(req.toByteArray)
    expect.all(
      rt == req,
      rt.topic == "allow-spend-block",
      rt.messageIds.size == 2
    )
  }

  pureTest("ConfirmFinalizedResponse preserves dropped count") {
    val resp = ConfirmFinalizedResponse(dropped = 7)
    val rt = ConfirmFinalizedResponse.parseFrom(resp.toByteArray)
    expect(rt.dropped == 7).and(expect(rt == resp))
  }

  pureTest("PeerCountResponse.meshAllowSpendBlocks roundtrips") {
    // Forward-compat assertion: the new field must survive a serialize/parse
    // cycle alongside the existing mesh counters so an updated sidecar can
    // ship the value without a coordinated client rev-bump.
    val resp = PeerCountResponse(
      total = 10,
      meshSnapshots = 3,
      meshAttestations = 4,
      meshRumors = 5,
      meshMetagraphBinaries = 2,
      meshMetagraphAttestations = 1,
      meshAllowSpendBlocks = 6
    )
    val rt = PeerCountResponse.parseFrom(resp.toByteArray)
    expect.all(
      rt == resp,
      rt.meshAllowSpendBlocks == 6
    )
  }
}
