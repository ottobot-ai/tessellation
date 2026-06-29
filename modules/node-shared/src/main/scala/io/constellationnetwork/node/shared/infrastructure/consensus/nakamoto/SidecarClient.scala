package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import java.util.concurrent.TimeUnit

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._

import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

/** gRPC client for the Go libp2p sidecar.
  *
  * The sidecar manages GossipSub mesh networking. This client talks to it over localhost gRPC to publish snapshots/attestations and
  * subscribe to incoming gossip messages.
  */
object SidecarClient {

  final case class SidecarConfig(
    host: String = "127.0.0.1",
    grpcPort: Int = 50051
  )

  trait SidecarClientAlgebra[F[_]] {
    def publishSnapshot(msg: Snapshot): F[PublishResponse]
    def publishAttestation(msg: TipAttestation): F[PublishResponse]
    def publishRumor(msg: Rumor): F[PublishResponse]
    def publishMetagraphBinary(msg: MetagraphBinary): F[PublishResponse]

    /** Publish a per-metagraph committee VRF attestation (Slice S2/S3). The wire payload is a proto-encoded `MetagraphAttestation` produced
      * by the JVM; the sidecar treats it as opaque, gossips it on the metagraph-attestation topic, and adds it to the durable-publish
      * outbox (#196). Caller surface used by `MetagraphCommitteeGate` in S3 once the sender path goes load-bearing.
      */
    def publishMetagraphAttestation(msg: MetagraphAttestation): F[PublishResponse]

    /** Publish a Signed[AllowSpendBlock] to all GL0 nodes via the durable- publish outbox. Replaces the single-peer HTTP POST path from
      * `Swap.sendBlockToL0` (#196).
      *
      * @param payload
      *   serialized `Signed[AllowSpendBlock]` bytes (via JsonSerializer); opaque to the sidecar but used by the sidecar's outbox to derive
      *   the message id (sha256 first 32 bytes).
      */
    def publishAllowSpendBlock(payload: Array[Byte]): F[PublishResponse]

    /** Publish a Signed[Block] (DAG block) to all GL0 nodes via the durable-publish outbox. Extends the #196 pattern to the DAG-block hop —
      * replaces the single-peer HTTP POST path from `StateChannel.sendBlockToL0` (`p2pClient.l0BlockOutputClient.sendL1Output` → POST
      * `/dag/l1-output`).
      *
      * @param payload
      *   serialized `Signed[Block]` bytes (via JsonSerializer); opaque to the sidecar but used by the sidecar's outbox to derive the
      *   message id (sha256 first 32 bytes).
      */
    def publishDAGBlock(payload: Array[Byte]): F[PublishResponse]

    /** Publish a Signed[TokenLockBlock] to all GL0 nodes via the durable-publish outbox. Extends the #196 pattern to the token-lock-block
      * hop — replaces the single-peer HTTP POST path from `TokenLock.sendBlockToL0`.
      *
      * @param payload
      *   serialized `Signed[TokenLockBlock]` bytes (via JsonSerializer); opaque to the sidecar but used by the sidecar's outbox to derive
      *   the message id (sha256 first 32 bytes).
      */
    def publishTokenLockBlock(payload: Array[Byte]): F[PublishResponse]

    /** Slice 14: publish a shard checkpoint on the per-shard topic `shard-checkpoint-<shardId>`. The wire payload is already a
      * proto-encoded `ShardCheckpointWire` produced via `ShardCheckpointWireCodecs.shardCheckpointToWire`; the sidecar treats it as opaque
      * and gossips on the per-shard topic indicated by `msg.shardId`. The fully-signed (post-threshold) checkpoint envelope flow rides on
      * top of this primitive; the gossip topic name routing is the sidecar's responsibility.
      */
    def publishShardCheckpoint(msg: ShardCheckpointWire): F[PublishResponse]

    /** Slice 14: publish a shard-checkpoint attestation (non-producing committee member's attestation, post-envelope-observation). Same
      * per-shard topic as `publishShardCheckpoint`. Receivers tally toward the `≥ ⌈2/3 K_S⌉` quorum (Slice 9).
      */
    def publishShardCheckpointAttestation(msg: ShardCheckpointAttestationWire): F[PublishResponse]

    /** WATCHTOWER: publish a fraud proof on the gl0-wide `fraud-proof` topic. The wire payload is a proto-encoded `FraudProofEnvelopeWire`
      * produced via `FraudProofWireCodecs.toWire`; the sidecar gossips it to every gl0 so each independently re-runs the deterministic
      * dispute verdict. Failures are swallowed by the caller (the watchtower must not block the accept path on a publish failure).
      */
    def publishFraudProof(msg: FraudProofEnvelopeWire): F[PublishResponse]

    /** Ack to the sidecar that the listed message ids on `topic` have reached Phase-3 finality and may be dropped from the outbox. Each id
      * is the sha256 (first 32 bytes) of the same payload bytes the JVM published.
      *
      * For AllowSpendBlock + MetagraphBinary: id = sha256(serializedSigned), computed via `Hasher[F].hashBytes` so the Hasher typeclass is
      * the single source of hash truth (per project rule `feedback_use_hasher_no_manual_serialize`). For MetagraphAttestation: id =
      * sha256(wire-level proto bytes).
      *
      * Topic literals come from [[OutboxTopic]] — DO NOT pass string literals directly so a future rename can't drift the JVM/sidecar pair.
      */
    def confirmFinalized(topic: String, msgIds: List[Array[Byte]]): F[ConfirmFinalizedResponse]

    def health: F[HealthResponse]
    def peers: F[PeerCountResponse]
    def channel: ManagedChannel
  }

  /** Outbox topic labels — kept in lockstep with the Go sidecar's [[grpcserver.TopicAllowSpendBlock]] et al. so a typo here can't desync
    * the wire-format vocabulary. Distinct from the GossipSub topic strings (which carry version paths); these are short stable labels.
    */
  object OutboxTopic {
    val AllowSpendBlock = "allow-spend-block"
    val MetagraphBinary = "metagraph-binary"
    val MetagraphAttestation = "metagraph-attestation"
    val DAGBlock = "dag-block"
    val TokenLockBlock = "token-lock-block"

    /** Slice 14: shard checkpoint outbox label. The per-shard GossipSub topic the sidecar publishes on is `shard-checkpoint-<shardId>`
      * (with the shard-id suffix); this label is the short stable family-prefix shared across all per-shard topics. The sidecar reads
      * `msg.shardId` to derive the actual topic string at publish time. Matches the design doc §6.4 routing convention.
      */
    val ShardCheckpoint = "shard-checkpoint"

    /** Slice 14: shard-checkpoint attestation outbox label — same per-shard topic family as `ShardCheckpoint`
      * (`shard-checkpoint-<shardId>`). Attestations and the envelopes themselves ride the same topic so receivers can join the per-shard
      * mesh once and process both.
      */
    val ShardCheckpointAttestation = "shard-checkpoint-attestation"

    /** WATCHTOWER: gl0-wide fraud-proof topic. Unlike the per-shard checkpoint topics, fraud proofs gossip to EVERY gl0 (every node must
      * independently re-run the dispute verdict + apply the slash), so this is a single cluster-wide topic with no shard-id suffix.
      */
    val FraudProof = "fraud-proof"
  }

  /** Create a gRPC client Resource that opens a channel and cleans up on release. */
  def makeResource[F[_]: Async](config: SidecarConfig): Resource[F, SidecarClientAlgebra[F]] =
    Resource
      .make(
        Async[F].delay(
          ManagedChannelBuilder
            .forAddress(config.host, config.grpcPort)
            .usePlaintext()
            .keepAliveTime(30, TimeUnit.SECONDS)
            .keepAliveTimeout(10, TimeUnit.SECONDS)
            .keepAliveWithoutCalls(true)
            .build()
        )
      )(ch => Async[F].delay(ch.shutdown()).void)
      .map(ch => fromChannel[F](ch))

  /** Build algebra from an existing channel. */
  def fromChannel[F[_]: Async](ch: ManagedChannel): SidecarClientAlgebra[F] = {
    val stub = SidecarServiceGrpc.stub(ch)

    new SidecarClientAlgebra[F] {
      private def liftFuture[A](fa: => scala.concurrent.Future[A]): F[A] =
        Async[F].fromFuture(Async[F].delay(fa))

      def publishSnapshot(msg: Snapshot): F[PublishResponse] =
        liftFuture(stub.publishSnapshot(msg))

      def publishAttestation(msg: TipAttestation): F[PublishResponse] =
        liftFuture(stub.publishAttestation(msg))

      def publishRumor(msg: Rumor): F[PublishResponse] =
        liftFuture(stub.publishRumor(msg))

      def publishMetagraphBinary(msg: MetagraphBinary): F[PublishResponse] =
        liftFuture(stub.publishMetagraphBinary(msg))

      def publishMetagraphAttestation(msg: MetagraphAttestation): F[PublishResponse] =
        liftFuture(stub.publishMetagraphAttestation(msg))

      def publishAllowSpendBlock(payload: Array[Byte]): F[PublishResponse] =
        liftFuture(stub.publishAllowSpendBlock(AllowSpendBlock(payload = ByteString.copyFrom(payload))))

      def publishDAGBlock(payload: Array[Byte]): F[PublishResponse] =
        liftFuture(stub.publishDAGBlock(DAGBlock(payload = ByteString.copyFrom(payload))))

      def publishTokenLockBlock(payload: Array[Byte]): F[PublishResponse] =
        liftFuture(stub.publishTokenLockBlock(TokenLockBlock(payload = ByteString.copyFrom(payload))))

      def publishShardCheckpoint(msg: ShardCheckpointWire): F[PublishResponse] =
        liftFuture(stub.publishShardCheckpoint(msg))

      def publishShardCheckpointAttestation(msg: ShardCheckpointAttestationWire): F[PublishResponse] =
        liftFuture(stub.publishShardCheckpointAttestation(msg))

      def publishFraudProof(msg: FraudProofEnvelopeWire): F[PublishResponse] =
        liftFuture(stub.publishFraudProof(msg))

      def confirmFinalized(topic: String, msgIds: List[Array[Byte]]): F[ConfirmFinalizedResponse] =
        liftFuture(
          stub.confirmFinalized(
            ConfirmFinalizedRequest(
              topic = topic,
              messageIds = msgIds.map(ByteString.copyFrom)
            )
          )
        )

      def health: F[HealthResponse] =
        liftFuture(stub.health(HealthRequest()))

      def peers: F[PeerCountResponse] =
        liftFuture(stub.peerCount(PeerCountRequest()))

      def channel: ManagedChannel = ch
    }
  }

  // ─── Helpers for constructing proto messages ───

  def mkSnapshot(
    hash: Array[Byte],
    slot: Long,
    ordinal: Long,
    parentHash: Array[Byte],
    vrfProof: Array[Byte],
    vrfPublicKey: Array[Byte],
    eta: Array[Byte],
    payload: Array[Byte],
    producerId: Array[Byte],
    parentSlot: Long = 0L,
    // §1.2 Slice 6: optional KES parallel signature over the snapshot artifact hash.
    // Empty by default (pre-Slice-6 callers + sender-side signAt failures both
    // produce an empty wire field; receivers tolerate empty in Slice 5/6 — load-bearing
    // verification lands in Slice 9).
    kesSignature: Array[Byte] = Array.empty[Byte]
  ): Snapshot =
    Snapshot(
      hash = ByteString.copyFrom(hash),
      slot = slot,
      ordinal = ordinal,
      parentHash = ByteString.copyFrom(parentHash),
      vrfProof = ByteString.copyFrom(vrfProof),
      vrfPublicKey = ByteString.copyFrom(vrfPublicKey),
      eta = ByteString.copyFrom(eta),
      payload = ByteString.copyFrom(payload),
      producerId = ByteString.copyFrom(producerId),
      parentSlot = parentSlot,
      kesSignature = ByteString.copyFrom(kesSignature)
    )

  def mkRumor(
    signedRumorBytes: Array[Byte],
    contentType: String,
    originId: Array[Byte]
  ): Rumor =
    Rumor(
      signedRumorBytes = ByteString.copyFrom(signedRumorBytes),
      contentType = contentType,
      originId = ByteString.copyFrom(originId)
    )

  def mkAttestation(
    tipHash: Array[Byte],
    tipSlot: Long,
    tipOrdinal: Long,
    attestedAt: Long,
    attesterId: Array[Byte],
    signature: Array[Byte],
    // §1.2 Slice 5: optional KES parallel signature over the attestation hash.
    // Empty by default (pre-Slice-5 callers + sender-side signAt failures both
    // produce an empty wire field; receivers tolerate empty in Slice 5/6 — load-bearing
    // verification lands in Slice 9).
    kesSignature: Array[Byte] = Array.empty[Byte]
  ): TipAttestation =
    TipAttestation(
      tipHash = ByteString.copyFrom(tipHash),
      tipSlot = tipSlot,
      tipOrdinal = tipOrdinal,
      attestedAt = attestedAt,
      attesterId = ByteString.copyFrom(attesterId),
      signature = ByteString.copyFrom(signature),
      kesSignature = ByteString.copyFrom(kesSignature)
    )

  def mkMetagraphBinary(
    address: String,
    binary: Array[Byte]
  ): MetagraphBinary =
    MetagraphBinary(
      address = address,
      binary = ByteString.copyFrom(binary)
    )

  /** Slice S2/S3 helper: construct a `MetagraphAttestation` proto from raw byte fields. The fields' meanings match the proto definition in
    * `p2p/proto/sidecar.proto` — kept here so callers don't have to import the proto types directly. `kesSignature` is REQUIRED in S3
    * (load-bearing flip); empty by default for tests that exercise the "reject empty KES" path.
    */
  def mkMetagraphAttestation(
    peerIdBytes: Array[Byte],
    metagraphAddress: String,
    parentHash: Array[Byte],
    binaryHash: Array[Byte],
    committeeVrfProof: Array[Byte],
    signature: Array[Byte],
    kesSignature: Array[Byte] = Array.empty[Byte],
    vrfPublicKey: Array[Byte] = Array.empty[Byte],
    senderTreeStep: Int = 0
  ): MetagraphAttestation =
    MetagraphAttestation(
      peerId = ByteString.copyFrom(peerIdBytes),
      metagraphAddress = metagraphAddress,
      parentHash = ByteString.copyFrom(parentHash),
      binaryHash = ByteString.copyFrom(binaryHash),
      committeeVrfProof = ByteString.copyFrom(committeeVrfProof),
      signature = ByteString.copyFrom(signature),
      kesSignature = ByteString.copyFrom(kesSignature),
      vrfPublicKey = ByteString.copyFrom(vrfPublicKey),
      senderTreeStep = senderTreeStep
    )
}
