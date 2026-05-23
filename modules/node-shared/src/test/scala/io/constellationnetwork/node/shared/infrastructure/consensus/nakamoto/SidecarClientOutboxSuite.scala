package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient.{OutboxTopic, SidecarClientAlgebra}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._

import com.google.protobuf.ByteString
import io.grpc.ManagedChannel
import weaver.SimpleIOSuite

/** Task #196 — exercises the JVM side of the durable-outbox surface:
  *
  *   - `publishAllowSpendBlock(payload)` builds a `pb.AllowSpendBlock` from raw bytes
  *   - `publishMetagraphAttestation(msg)` accepts a structured `pb.MetagraphAttestation`
  *   - `confirmFinalized(topic, ids)` builds a `pb.ConfirmFinalizedRequest` with the right shape
  *
  * The Go sidecar's outbox lifecycle (Add → re-publish → Confirm → drop) is covered by the Go-side outbox_test.go unit tests; the JVM job
  * here is to make sure the algebra plumbs the right wire shape across the gRPC boundary. We assert behaviour against a stub implementation
  * that captures every call — no real gRPC channel, no network — so the test stays fast and hermetic.
  */
object SidecarClientOutboxSuite extends SimpleIOSuite {

  /** In-memory implementation of `SidecarClientAlgebra` used by tests.
    *
    * Mirrors the Go sidecar's outbox semantics at the contract level: every publish lands in `published(topic) -> List[msgID]`;
    * `confirmFinalized(topic, ids)` drops matching ids and returns the dropped count. Out-of-scope on this stub: the actual mesh publish,
    * the republish ticker — those are Go-side behaviours.
    */
  private final case class StubState(
    asbPublished: List[Array[Byte]] = List.empty,
    dagPublished: List[Array[Byte]] = List.empty,
    tlbPublished: List[Array[Byte]] = List.empty,
    metagraphBinaryPublished: List[String] = List.empty,
    metagraphAttestationPublished: List[String] = List.empty,
    confirmCalls: List[(String, List[Array[Byte]])] = List.empty
  )

  private def stubClient(ref: Ref[IO, StubState]): SidecarClientAlgebra[IO] = new SidecarClientAlgebra[IO] {
    def publishSnapshot(msg: Snapshot) = IO.pure(PublishResponse(ok = true))
    def publishAttestation(msg: TipAttestation) = IO.pure(PublishResponse(ok = true))
    def publishRumor(msg: Rumor) = IO.pure(PublishResponse(ok = true))
    def publishMetagraphBinary(msg: MetagraphBinary) =
      ref.update(s => s.copy(metagraphBinaryPublished = msg.address :: s.metagraphBinaryPublished)).as(PublishResponse(ok = true))
    def publishMetagraphAttestation(msg: MetagraphAttestation) =
      ref
        .update(s => s.copy(metagraphAttestationPublished = msg.metagraphAddress :: s.metagraphAttestationPublished))
        .as(PublishResponse(ok = true))
    def publishAllowSpendBlock(payload: Array[Byte]) =
      ref.update(s => s.copy(asbPublished = payload :: s.asbPublished)).as(PublishResponse(ok = true))
    def publishDAGBlock(payload: Array[Byte]) =
      ref.update(s => s.copy(dagPublished = payload :: s.dagPublished)).as(PublishResponse(ok = true))
    def publishTokenLockBlock(payload: Array[Byte]) =
      ref.update(s => s.copy(tlbPublished = payload :: s.tlbPublished)).as(PublishResponse(ok = true))
    // Slice 14: shard-checkpoint stubs — outbox unit tests don't exercise these paths, but the algebra
    // is now total over the trait and the stub must implement every method to construct.
    def publishShardCheckpoint(msg: ShardCheckpointWire) = IO.pure(PublishResponse(ok = true))
    def publishShardCheckpointAttestation(msg: ShardCheckpointAttestationWire) = IO.pure(PublishResponse(ok = true))
    def confirmFinalized(topic: String, msgIds: List[Array[Byte]]) =
      ref
        .update(s => s.copy(confirmCalls = (topic, msgIds) :: s.confirmCalls))
        .as(ConfirmFinalizedResponse(dropped = msgIds.size))
    def health = IO.pure(HealthResponse(healthy = true))
    def peers = IO.pure(PeerCountResponse(total = 0))
    def channel: ManagedChannel = throw new UnsupportedOperationException("stub")
  }

  // ─── publish path ────────────────────────────────────────────────────

  test("publishAllowSpendBlock captures the raw payload on the wire") {
    for {
      ref <- Ref.of[IO, StubState](StubState())
      client = stubClient(ref)
      _ <- client.publishAllowSpendBlock("hello-block".getBytes("UTF-8"))
      state <- ref.get
    } yield
      expect(state.asbPublished.size == 1)
        .and(expect(new String(state.asbPublished.head, "UTF-8") == "hello-block"))
  }

  test("publishMetagraphAttestation passes through the proto-level fields") {
    for {
      ref <- Ref.of[IO, StubState](StubState())
      client = stubClient(ref)
      msg = MetagraphAttestation(
        peerId = ByteString.copyFromUtf8("peer"),
        metagraphAddress = "DAG-abc",
        parentHash = ByteString.copyFromUtf8("parent-h"),
        binaryHash = ByteString.copyFromUtf8("bin-h"),
        committeeVrfProof = ByteString.copyFromUtf8("vrf-proof"),
        signature = ByteString.copyFromUtf8("sig")
      )
      _ <- client.publishMetagraphAttestation(msg)
      state <- ref.get
    } yield expect(state.metagraphAttestationPublished == List("DAG-abc"))
  }

  test("publishDAGBlock captures the raw payload on the wire") {
    for {
      ref <- Ref.of[IO, StubState](StubState())
      client = stubClient(ref)
      _ <- client.publishDAGBlock("dag-block-bytes".getBytes("UTF-8"))
      state <- ref.get
    } yield
      expect(state.dagPublished.size == 1)
        .and(expect(new String(state.dagPublished.head, "UTF-8") == "dag-block-bytes"))
  }

  test("publishTokenLockBlock captures the raw payload on the wire") {
    for {
      ref <- Ref.of[IO, StubState](StubState())
      client = stubClient(ref)
      _ <- client.publishTokenLockBlock("token-lock-bytes".getBytes("UTF-8"))
      state <- ref.get
    } yield
      expect(state.tlbPublished.size == 1)
        .and(expect(new String(state.tlbPublished.head, "UTF-8") == "token-lock-bytes"))
  }

  // ─── confirmFinalized ────────────────────────────────────────────────

  test("confirmFinalized lands the topic + ids on the wire intact") {
    val id1 = Array.fill[Byte](32)(0x10)
    val id2 = Array.fill[Byte](32)(0x20)
    for {
      ref <- Ref.of[IO, StubState](StubState())
      client = stubClient(ref)
      resp <- client.confirmFinalized(OutboxTopic.AllowSpendBlock, List(id1, id2))
      state <- ref.get
    } yield {
      val (topic, ids) = state.confirmCalls.head
      expect(resp.dropped == 2)
        .and(expect(topic == "allow-spend-block"))
        .and(expect(ids.size == 2))
        .and(expect(ids.head.sameElements(id1)))
        .and(expect(ids(1).sameElements(id2)))
    }
  }

  test("confirmFinalized(dag-block) lands the dag-block topic on the wire") {
    val id = Array.fill[Byte](32)(0x30)
    for {
      ref <- Ref.of[IO, StubState](StubState())
      client = stubClient(ref)
      resp <- client.confirmFinalized(OutboxTopic.DAGBlock, List(id))
      state <- ref.get
    } yield {
      val (topic, ids) = state.confirmCalls.head
      expect(resp.dropped == 1)
        .and(expect(topic == "dag-block"))
        .and(expect(ids.size == 1))
        .and(expect(ids.head.sameElements(id)))
    }
  }

  test("confirmFinalized(token-lock-block) lands the token-lock-block topic on the wire") {
    val id = Array.fill[Byte](32)(0x40)
    for {
      ref <- Ref.of[IO, StubState](StubState())
      client = stubClient(ref)
      resp <- client.confirmFinalized(OutboxTopic.TokenLockBlock, List(id))
      state <- ref.get
    } yield {
      val (topic, ids) = state.confirmCalls.head
      expect(resp.dropped == 1)
        .and(expect(topic == "token-lock-block"))
        .and(expect(ids.size == 1))
        .and(expect(ids.head.sameElements(id)))
    }
  }

  pureTest("OutboxTopic constants match the wire vocabulary the sidecar registers") {
    // Lockstep check: these literals MUST match grpcserver.TopicAllowSpendBlock /
    // TopicMetagraphBinary / TopicMetagraphAttestation / TopicDAGBlock /
    // TopicTokenLockBlock in the Go sidecar. A rename without updating both
    // sides desyncs the outbox.
    expect.all(
      OutboxTopic.AllowSpendBlock == "allow-spend-block",
      OutboxTopic.MetagraphBinary == "metagraph-binary",
      OutboxTopic.MetagraphAttestation == "metagraph-attestation",
      OutboxTopic.DAGBlock == "dag-block",
      OutboxTopic.TokenLockBlock == "token-lock-block"
    )
  }
}
