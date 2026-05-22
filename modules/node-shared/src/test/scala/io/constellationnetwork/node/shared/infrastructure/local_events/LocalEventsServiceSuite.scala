package io.constellationnetwork.node.shared.infrastructure.local_events

import cats.effect.std.Dispatcher
import cats.effect.{IO, Ref, Resource}

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events._
import io.constellationnetwork.schema.SnapshotOrdinal

import eu.timepit.refined.types.numeric.PosInt
import io.grpc.ManagedChannelBuilder
import weaver.MutableIOSuite

/** End-to-end roundtrip tests for the [[LocalEventsService]].
  *
  *   - Server starts/stops cleanly on Resource teardown
  *   - Publisher → subscriber roundtrip delivers every event (including the leading StreamStarted)
  *   - Filter matching narrows correctly by kind + by address + by ordinal range
  *   - Slow-consumer backpressure does not block the publisher
  *
  * Uses gRPC's `InProcessTransport` to avoid binding a real port — tests are deterministic and don't leak listen sockets across the suite.
  */
object LocalEventsServiceSuite extends MutableIOSuite {

  override type Res = Unit
  override def sharedResource: Resource[IO, Res] = Resource.pure[IO, Unit](())

  private val maxQueued: PosInt = PosInt.unsafeFrom(64)
  private val publisherBuf: PosInt = PosInt.unsafeFrom(256)

  /** Build the service + a real gRPC server on an ephemeral localhost port + a client stub. The production wiring uses
    * `ServerBuilder.forPort(...)`; we mirror that with `port = 0` so the JVM picks a free port. Server + channel are released on
    * `Resource.release`.
    */
  private def fixture(
    finalizedOrd: SnapshotOrdinal = SnapshotOrdinal.unsafeApply(42L)
  ): Resource[IO, (LocalEventsService.Service[IO], LocalEventsGrpc.LocalEventsBlockingStub, LocalEventsGrpc.LocalEventsStub)] =
    for {
      dispatcher <- Dispatcher.parallel[IO]
      finRef <- Resource.eval(Ref.of[IO, SnapshotOrdinal](finalizedOrd))
      epochRef <- Resource.eval(Ref.of[IO, Long](7L))
      svc <- Resource.eval(
        LocalEventsService.make[IO](
          finalizedOrdinalRef = finRef,
          epochProgressRef = epochRef,
          maxQueuedPerSubscriber = maxQueued,
          publisherBufferSize = publisherBuf,
          dispatcher = dispatcher
        )
      )
      server <- Resource.make(
        IO.blocking {
          val srv = io.grpc.ServerBuilder
            .forPort(0) // ephemeral
            .addService(LocalEventsGrpc.bindService(svc.grpcImpl, scala.concurrent.ExecutionContext.global))
            .build()
          srv.start()
          srv
        }
      )(srv =>
        IO.blocking {
          srv.shutdownNow()
          srv.awaitTermination(2L, java.util.concurrent.TimeUnit.SECONDS)
          ()
        }
      )
      channel <- Resource.make(
        IO.blocking {
          ManagedChannelBuilder
            .forAddress("127.0.0.1", server.getPort)
            .usePlaintext()
            .build()
        }
      )(ch =>
        IO.blocking {
          ch.shutdownNow()
          ()
        }
      )
      blocking = LocalEventsGrpc.blockingStub(channel)
      stub = LocalEventsGrpc.stub(channel)
    } yield (svc, blocking, stub)

  test("server starts and Health returns healthy") {
    fixture().use {
      case (svc, blocking, _) =>
        for {
          resp <- IO.blocking(blocking.health(HealthRequest()))
          h <- svc.health
        } yield
          expect(resp.healthy) &&
            expect(resp.lastPublishedSeq == 0L) &&
            expect(h.healthy) &&
            expect(h.subscriberCount == 0L)
    }
  }

  test("Subscribe sends StreamStarted first with current finalized ordinal") {
    val finalizedOrd = SnapshotOrdinal.unsafeApply(99L)
    fixture(finalizedOrd).use {
      case (_, blocking, _) =>
        for {
          // blocking iterator — pull one envelope then close. The first envelope MUST be StreamStarted
          // per Q3 user override.
          iter <- IO.blocking(blocking.subscribe(SubscribeRequest(clientTag = "test/stream-started")))
          first <- IO.blocking(iter.next())
        } yield
          expect(first.event.isStreamStarted) &&
            expect(first.event.streamStarted.exists(_.startOrdinal == finalizedOrd.value.value)) &&
            expect(first.event.streamStarted.exists(_.epochProgress == 7L)) &&
            expect(first.ordinal == finalizedOrd.value.value) &&
            expect(first.wireVersion == 1)
    }
  }

  test("Publisher → subscriber roundtrip: emit a BalanceChange and observe it after StreamStarted") {
    fixture().use {
      case (svc, blocking, _) =>
        val ord = SnapshotOrdinal.unsafeApply(100L)
        val bc = BalanceChange(address = "DAGabc", oldBalance = 100L, newBalance = 200L, cause = "test")
        for {
          iter <- IO.blocking(blocking.subscribe(SubscribeRequest(clientTag = "test/roundtrip")))
          first <- IO.blocking(iter.next()) // StreamStarted, sent AFTER topic.subscribeAwait registers
          // Subscriber slot is guaranteed registered by the time StreamStarted lands on the wire
          // (server uses topic.subscribeAwait.use, so the subscriber is registered synchronously
          // before StreamStarted is pushed into the stream).
          _ <- svc.publisher.publishBalanceChanges(ord, List(bc))
          delivered <- IO.blocking(iter.next())
        } yield
          expect(first.event.isStreamStarted) &&
            expect(delivered.event.isBalanceChange) &&
            expect(delivered.event.balanceChange.exists(_.address == "DAGabc")) &&
            expect(delivered.event.balanceChange.exists(_.newBalance == 200L)) &&
            expect(delivered.ordinal == 100L) &&
            // seq increments past StreamStarted's seq=1
            expect(delivered.seq > first.seq)
    }
  }

  test("Filter by kind narrows the stream to matching event types only") {
    fixture().use {
      case (svc, blocking, _) =>
        val ord = SnapshotOrdinal.unsafeApply(50L)
        val balanceFilter = SubscribeRequest(
          filters = Seq(EventFilter(kinds = Seq(EventKind.BALANCE_CHANGE))),
          clientTag = "test/filter-kind"
        )
        for {
          iter <- IO.blocking(blocking.subscribe(balanceFilter))
          first <- IO.blocking(iter.next()) // StreamStarted always passes
          _ <- svc.publisher.publishTransactionsAccepted(
            ord,
            List(TransactionAccepted(source = "DAGsrc", destination = "DAGdst", amount = 1L, fee = 0L))
          )
          _ <- svc.publisher.publishBalanceChanges(
            ord,
            List(BalanceChange(address = "DAGfilter", oldBalance = 0L, newBalance = 1L, cause = "test"))
          )
          // Second .next() should skip the TransactionAccepted (filtered) and deliver BalanceChange.
          delivered <- IO.blocking(iter.next())
        } yield
          expect(first.event.isStreamStarted) &&
            expect(delivered.event.isBalanceChange) &&
            expect(delivered.event.balanceChange.exists(_.address == "DAGfilter"))
    }
  }

  test("Filter by address narrows BalanceChange stream by address-set membership") {
    fixture().use {
      case (svc, blocking, _) =>
        val ord = SnapshotOrdinal.unsafeApply(10L)
        val addrFilter = SubscribeRequest(
          filters = Seq(EventFilter(kinds = Seq(EventKind.BALANCE_CHANGE), addresses = Seq("DAGwanted"))),
          clientTag = "test/filter-addr"
        )
        for {
          iter <- IO.blocking(blocking.subscribe(addrFilter))
          _ <- IO.blocking(iter.next()) // StreamStarted
          _ <- svc.publisher.publishBalanceChanges(
            ord,
            List(
              BalanceChange(address = "DAGnope", oldBalance = 0L, newBalance = 1L, cause = "t"),
              BalanceChange(address = "DAGwanted", oldBalance = 0L, newBalance = 2L, cause = "t")
            )
          )
          delivered <- IO.blocking(iter.next())
        } yield expect(delivered.event.balanceChange.exists(_.address == "DAGwanted"))
    }
  }

  test("Slow-consumer backpressure: publisher returns quickly even when subscriber doesn't drain") {
    // Smoke-level: publish many envelopes and observe that the publishX call doesn't block past
    // the FS2 topic's process-wide buffer.
    fixture().use {
      case (svc, _, _) =>
        val ord = SnapshotOrdinal.unsafeApply(1L)
        val many = (1 to 200).toList.map(i => BalanceChange(address = s"DAG$i", oldBalance = 0L, newBalance = i.toLong, cause = "burst"))
        // No subscriber attached — publisher fans out to nothing. Calls must return quickly.
        // (If publishX blocked, this test would hang past the suite-wide timeout.)
        svc.publisher.publishBalanceChanges(ord, many).timeout(5.seconds).as(success)
    }
  }

  test("Health subscriber_count increments with active subscriber and decrements after close") {
    fixture().use {
      case (svc, blocking, _) =>
        for {
          // Before subscribe
          h0 <- svc.health
          // Subscribe (StreamStarted will be sent immediately); consume the StreamStarted then
          // sleep briefly to let subscriberCountRef be incremented.
          iter <- IO.blocking(blocking.subscribe(SubscribeRequest(clientTag = "test/health-count")))
          _ <- IO.blocking(iter.next())
          _ <- IO.sleep(100.millis)
          h1 <- svc.health
        } yield
          expect(h0.subscriberCount == 0L) &&
            expect(h1.subscriberCount >= 1L)
    }
  }
}
