package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.std.{Queue, Supervisor}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient.SubscribeTopics
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._
import io.constellationnetwork.schema.gossip.RumorRaw
import io.constellationnetwork.security.{Hashed, Hasher, HasherSelector}

import io.grpc._
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import weaver.MutableIOSuite

/** FINDING-F1 (EPIC-9-NET M1) — the JVM half of the Subscribe topology contract.
  *
  * The sidecar's shard-checkpoint families ride SHARED node-lifetime fan-in channels that tolerate exactly ONE drainer. The JVM holds TWO
  * concurrent Subscribe streams ([[SidecarRumorBridge]] + `NakamotoSyncDaemon`); pre-fix both subscribed to EVERYTHING (empty
  * `SubscribeRequest.topics`), so they race-drained the shard channels and the bridge silently discarded the shard checkpoints it won
  * (~half — reproduced in the Go-side `TestSubscribe_TopicFilter_DualStreamShardCheckpointDelivery`). This suite pins the JVM call shapes
  * that make the race structurally impossible:
  *
  *   - every `GossipStream.subscribe` carries an explicit topic filter on the wire;
  *   - the rumor bridge requests exactly the rumor family;
  *   - the daemon's topic set covers every non-rumor family (including shard checkpoints and fraud proofs) and excludes rumor — so the
  *     shared shard channels have exactly one drainer;
  *   - the underlying gRPC call is CANCELLED when the fs2 scope closes (the zombie-stream guard: a leaked server-side subscription would
  *     re-enter the same race through a single logical consumer).
  */
object GossipStreamTopologySuite extends MutableIOSuite {

  override type Res = Hasher[IO]

  override def sharedResource: Resource[IO, Res] =
    JsonSerializer.forAsync[IO].asResource.map { implicit j =>
      Hasher.forJson[IO]
    }

  // ─── recording in-process sidecar service ─────────────────────────

  private def unimpl[A]: Future[A] = Future.failed(new UnsupportedOperationException("not under test"))

  /** Records every SubscribeRequest's topics + whether the client cancelled the call; emits `toSend` then holds the stream open (like the
    * real sidecar, whose Subscribe only ends on error/reconnect).
    */
  private final class RecordingService(
    captured: ConcurrentLinkedQueue[Seq[String]],
    cancelled: AtomicBoolean,
    toSend: Seq[GossipMessage]
  ) extends SidecarServiceGrpc.SidecarService {
    def publishSnapshot(request: Snapshot): Future[PublishResponse] = unimpl
    def publishAttestation(request: TipAttestation): Future[PublishResponse] = unimpl
    def publishRumor(request: Rumor): Future[PublishResponse] = unimpl
    def publishMetagraphBinary(request: MetagraphBinary): Future[PublishResponse] = unimpl
    def publishMetagraphAttestation(request: MetagraphAttestation): Future[PublishResponse] = unimpl
    def publishAllowSpendBlock(request: AllowSpendBlock): Future[PublishResponse] = unimpl
    def publishDAGBlock(request: DAGBlock): Future[PublishResponse] = unimpl
    def publishTokenLockBlock(request: TokenLockBlock): Future[PublishResponse] = unimpl
    def publishShardCheckpoint(request: ShardCheckpointWire): Future[PublishResponse] = unimpl
    def publishShardCheckpointAttestation(request: ShardCheckpointAttestationWire): Future[PublishResponse] = unimpl
    def publishFraudProof(request: FraudProofEnvelopeWire): Future[PublishResponse] = unimpl
    def confirmFinalized(request: ConfirmFinalizedRequest): Future[ConfirmFinalizedResponse] = unimpl
    def peerCount(request: PeerCountRequest): Future[PeerCountResponse] = unimpl
    def health(request: HealthRequest): Future[HealthResponse] = unimpl

    def subscribe(request: SubscribeRequest, responseObserver: StreamObserver[GossipMessage]): Unit = {
      captured.add(request.topics)
      responseObserver match {
        case sso: ServerCallStreamObserver[GossipMessage @unchecked] =>
          sso.setOnCancelHandler(() => cancelled.set(true))
        case _ => ()
      }
      toSend.foreach(responseObserver.onNext)
      // stream intentionally left open — see class doc
    }
  }

  private final case class Harness(
    channel: ManagedChannel,
    captured: ConcurrentLinkedQueue[Seq[String]],
    cancelled: AtomicBoolean
  )

  private def harness(toSend: Seq[GossipMessage]): Resource[IO, Harness] = {
    val captured = new ConcurrentLinkedQueue[Seq[String]]()
    val cancelled = new AtomicBoolean(false)
    for {
      server <- Resource.make(
        IO.blocking(
          ServerBuilder
            .forPort(0)
            .addService(
              SidecarServiceGrpc.bindService(
                new RecordingService(captured, cancelled, toSend),
                scala.concurrent.ExecutionContext.global
              )
            )
            .build()
            .start()
        )
      )(s => IO.blocking(s.shutdownNow()).void)
      channel <- Resource.make(
        IO.blocking(
          ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort).usePlaintext().build(): ManagedChannel
        )
      )(ch => IO.blocking(ch.shutdownNow()).void)
    } yield Harness(channel, captured, cancelled)
  }

  private def awaitTrue(what: String, timeout: FiniteDuration)(cond: IO[Boolean]): IO[Unit] = {
    def loop(remaining: Int): IO[Unit] =
      cond.flatMap {
        case true                    => IO.unit
        case false if remaining <= 0 => IO.raiseError(new RuntimeException(s"timed out waiting for: $what"))
        case false                   => IO.sleep(50.millis) >> loop(remaining - 1)
      }
    loop((timeout / 50.millis).toInt.max(1))
  }

  private def rumorMsg(contentType: String): GossipMessage =
    GossipMessage(body = GossipMessage.Body.Rumor(Rumor(contentType = contentType)))

  private def checkpointMsg(shardOrdinal: Long): GossipMessage =
    GossipMessage(body = GossipMessage.Body.ShardCheckpoint(ShardCheckpointWire(shardId = 0, shardOrdinal = shardOrdinal)))

  // ─── the wire-level filter ─────────────────────────────────────────

  test("GossipStream.subscribe puts the requested topic filter on the wire (rumor-only)") { _ =>
    harness(toSend = Seq(rumorMsg("t"))).use { h =>
      GossipStream
        .subscribe[IO](h.channel, SubscribeTopics.rumorOnly)
        .take(1)
        .compile
        .drain
        .timeout(15.seconds) >>
        IO(h.captured.asScala.toList).map { reqs =>
          expect.same(List(SubscribeTopics.rumorOnly), reqs)
        }
    }
  }

  test("GossipStream.subscribe(daemonTopics) delivers non-rumor bodies (shard checkpoint round-trips)") { _ =>
    harness(toSend = Seq(checkpointMsg(42L))).use { h =>
      GossipStream
        .subscribe[IO](h.channel, SubscribeTopics.daemonTopics)
        .take(1)
        .compile
        .lastOrError
        .timeout(15.seconds)
        .map { msg =>
          expect(msg.body.isShardCheckpoint)
            .and(expect.same(42L, msg.getShardCheckpoint.shardOrdinal))
            .and(expect.same(List(SubscribeTopics.daemonTopics), h.captured.asScala.toList))
        }
    }
  }

  // ─── the two consumers' declared sets ─────────────────────────────

  test("SidecarRumorBridge.receive subscribes RUMOR-ONLY (the F1 fix: it must never compete for the shard channels)") { h0 =>
    implicit val hasher: Hasher[IO] = h0
    implicit val hasherSelector: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(hasher)
    harness(toSend = Seq.empty).use { h =>
      Supervisor[IO].use { implicit sup =>
        for {
          rumorQueue <- Queue.bounded[IO, Hashed[RumorRaw]](8)
          _ <- SidecarRumorBridge.receive[IO](h.channel, rumorQueue)
          _ <- awaitTrue("bridge subscribe request captured", 15.seconds)(IO(!h.captured.isEmpty))
          reqs <- IO(h.captured.asScala.toList)
        } yield expect.same(List(SubscribeTopics.rumorOnly), reqs)
      }
    }
  }

  pureTest("daemonTopics covers every non-rumor GossipMessage family exactly once and excludes rumor (lockstep with the Go vocabulary)") {
    val expected = Set(
      "snapshot",
      "attestation",
      "metagraph-binary",
      "metagraph-attestation",
      "allow-spend-block",
      "dag-block",
      "token-lock-block",
      "shard-checkpoint",
      "shard-checkpoint-attestation",
      "fraud-proof"
    )
    // Lockstep check: these literals MUST match grpcserver.Topic* in the Go
    // sidecar (which REJECTS unknown labels). One label per GossipMessage.body
    // arm; rumor belongs to the bridge.
    expect
      .same(expected, SubscribeTopics.daemonTopics.toSet)
      .and(expect.same(SubscribeTopics.daemonTopics.size, SubscribeTopics.daemonTopics.distinct.size))
      .and(expect(!SubscribeTopics.daemonTopics.contains(SubscribeTopics.Rumor)))
      .and(expect.same(Seq("rumor"), SubscribeTopics.rumorOnly))
  }

  // ─── the zombie-stream guard ───────────────────────────────────────

  test("the underlying gRPC call is CANCELLED when the subscriber's fs2 scope closes (no zombie server-side stream)") { _ =>
    harness(toSend = Seq(rumorMsg("only"))).use { h =>
      for {
        // take(1) ends the fs2 scope while the server still holds the stream
        // open — pre-fix the gRPC call leaked (the server kept feeding a dead
        // observer; for a daemon restart that leaked stream kept race-draining
        // the shared shard channels forever).
        _ <- GossipStream.subscribe[IO](h.channel, SubscribeTopics.rumorOnly).take(1).compile.drain.timeout(15.seconds)
        _ <- awaitTrue("server observed client cancellation", 10.seconds)(IO(h.cancelled.get()))
      } yield success
    }
  }
}
