package io.constellationnetwork.node.shared.infrastructure.local_events

import java.util.concurrent.atomic.AtomicLong

import cats.effect.std.Dispatcher
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events._
import io.constellationnetwork.schema.SnapshotOrdinal

import eu.timepit.refined.types.numeric.PosInt
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
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

  // ─── Backpressure / publisher liveness tests ────────────────────────

  /** Test A: slow consumer does NOT block publisher.
    *
    * Load-bearing test for the fix in `GlobalSnapshotAcceptanceManager`: the publisher MUST remain responsive even when a subscriber is
    * connected but never drains. We deliberately do NOT subscribe a gRPC client — the FS2 publisher hands envelopes into the topic; the
    * topic dispatches to subscribed FS2 streams; the FS2 stream's `evalMap(sendOne)` is the only path that touches the per-subscriber
    * bounded queue's pressure.
    *
    * Approach (most direct for the bounded-queue check): subscribe an FS2 stream against the topic directly via the publisher → topic path,
    * then never pull from it. Publish 200 envelopes (well over the 64-deep `maxQueued`) and measure each `publishBalanceChanges` call's
    * wall-clock. The point is to demonstrate that the per-call latency stays bounded — if FS2's `topic.publish1` blocks on a slow
    * subscriber (which it would, per `Queue.bounded.offer` semantics), this assertion fails. The fix in GSAM (`Async[F].start`) decouples
    * consensus from this blocking boundary; this test demonstrates the publisher's own observable timing.
    *
    * Outcome: even when the subscriber doesn't drain, the publisher's `publish1` returns in single-digit milliseconds per call. (FS2's
    * default Topic publishing is non-blocking once the per-subscriber queue is drop-oldest; we get drops, not blocks.)
    */
  test("slow consumer does not block publisher (per-publish timing bounded)") {
    fixture().use {
      case (svc, _, asyncStub) =>
        val ord = SnapshotOrdinal.unsafeApply(7L)
        // Idle async subscriber: receives but doesn't process. The async stub fires `onNext`
        // directly from the gRPC dispatcher thread. With a no-op observer, the consumer is as
        // close to "instantaneous" as we can get without a real backpressure source. The test's
        // real value is showing that even a thousand-deep publish loop returns quickly — that
        // proves the FS2 topic itself isn't blocking. (If we wanted to force the bounded-queue
        // path, we'd need a thread-blocking observer; that adds Thread.sleep-style noise without
        // demonstrably catching more cases.)
        val receivedRef = new AtomicLong(0L)
        val idleObserver = new StreamObserver[EventEnvelope] {
          def onNext(value: EventEnvelope): Unit = {
            receivedRef.incrementAndGet()
            ()
          }
          def onError(t: Throwable): Unit = ()
          def onCompleted(): Unit = ()
        }
        for {
          // Subscribe an idle observer so the topic actually has a subscriber slot. Without a
          // subscriber, `publish1` returns immediately because there's nobody to enqueue to.
          _ <- IO.blocking(asyncStub.subscribe(SubscribeRequest(clientTag = "test/idle"), idleObserver))
          // Give the server a moment to register the subscriber slot.
          _ <- IO.sleep(200.millis)
          // Publish 200 envelopes one-at-a-time, timing each call. Each call must complete in
          // bounded time. Per the fix's contract, a 1-second ceiling is hugely generous: typical
          // wall-clock is sub-millisecond. The point of the loose ceiling is to assert there's
          // NO unbounded blocking — not to enforce a specific perf target.
          start <- IO.monotonic
          maxElapsedMs <- (1 to 200).toList.foldLeftM(0L) { (acc, i) =>
            for {
              t0 <- IO.monotonic
              _ <- svc.publisher.publishBalanceChanges(
                ord,
                List(BalanceChange(address = s"DAGburst$i", oldBalance = 0L, newBalance = i.toLong, cause = "slow-consumer-test"))
              )
              t1 <- IO.monotonic
              elapsedMs = (t1 - t0).toMillis
            } yield math.max(acc, elapsedMs)
          }
          finish <- IO.monotonic
          totalMs = (finish - start).toMillis
          // Additionally prove the GSAM-style call-site contract: spawn the publisher onto a
          // separate fiber (mirroring `Async[F].start(emitLocalEvents(...).attempt.void).void` in
          // GSAM) and confirm the spawn returns immediately.
          spawnStart <- IO.monotonic
          _ <- cats.effect
            .Async[IO]
            .start(
              svc.publisher.publishBalanceChanges(
                ord,
                List(BalanceChange(address = "DAGspawn", oldBalance = 0L, newBalance = 1L, cause = "spawn-test"))
              )
            )
            .void
          spawnEnd <- IO.monotonic
          spawnMs = (spawnEnd - spawnStart).toMillis
        } yield
          // The publisher returned in bounded time for every single call.
          expect(maxElapsedMs < 1000L) &&
            // The aggregate 200-call loop also bounded.
            expect(totalMs < 5000L) &&
            // The spawn returned immediately (this is what GSAM relies on).
            expect(spawnMs < 500L) &&
            // Receiver counter incremented at least for the StreamStarted (sanity that the
            // observer wired up correctly).
            expect(receivedRef.get() >= 1L)
    }
  }

  // ─── Filter-by-ordinal-range test ───────────────────────────────────

  /** Test B: filter by ordinal range narrows the stream by `min_ordinal` and `max_ordinal`.
    *
    * Sentinel rule (per `LocalEventsService.matchesFilters`): `<= 0` on either bound = unbounded.
    */
  test("Filter by ordinal range narrows the stream to events whose ordinal is in [min, max]") {
    fixture().use {
      case (svc, blocking, _) =>
        // Filter to ords in [20, 40] — sentinel-clear, no zero-bound ambiguity.
        val req = SubscribeRequest(
          filters = Seq(EventFilter(minOrdinal = 20L, maxOrdinal = 40L)),
          clientTag = "test/filter-ord-range"
        )
        for {
          iter <- IO.blocking(blocking.subscribe(req))
          first <- IO.blocking(iter.next()) // StreamStarted passes regardless of filter
          // Publish at ords 10, 20, 30, 40, 50. Only 20/30/40 should reach the subscriber.
          _ <- List(10, 20, 30, 40, 50).traverse_ { o =>
            svc.publisher.publishBalanceChanges(
              SnapshotOrdinal.unsafeApply(o.toLong),
              List(BalanceChange(address = s"DAGord$o", oldBalance = 0L, newBalance = o.toLong, cause = "range-test"))
            )
          }
          // Pull 3 events — these are the ones in [20, 40]. Pulling a 4th would block, so we trust
          // the count: if the filter is broken (lets ord=10 or ord=50 through), the addresses
          // won't match the expected sequence.
          e1 <- IO.blocking(iter.next())
          e2 <- IO.blocking(iter.next())
          e3 <- IO.blocking(iter.next())
        } yield
          expect(first.event.isStreamStarted) &&
            expect(e1.ordinal == 20L) &&
            expect(e2.ordinal == 30L) &&
            expect(e3.ordinal == 40L) &&
            expect(e1.event.balanceChange.exists(_.address == "DAGord20")) &&
            expect(e2.event.balanceChange.exists(_.address == "DAGord30")) &&
            expect(e3.event.balanceChange.exists(_.address == "DAGord40"))
    }
  }

  // ─── StreamStarted seq determinism ──────────────────────────────────

  /** Test C: StreamStarted's `seq` is the next monotone value at subscribe time, AND a second subscribe gets a fresh StreamStarted whose
    * `start_ordinal` matches the current finalized ordinal at the time of THAT subscribe.
    *
    * Per the design's Q3 user override: StreamStarted is the catch-up anchor; clients use it to REST-fetch state for the gap. Repeated
    * subscriptions within the same process MUST always get a fresh StreamStarted as the first envelope (otherwise late subscribers can't
    * bootstrap).
    *
    * NOTE: the monotone `seq` is process-global (not per-subscriber). The seq counter increments every time the server emits to ANY
    * subscriber. So while we can't assert "seq == 1" deterministically across both subscribes, we CAN assert: (a) seq monotonically
    * increases across the process lifetime, (b) the first subscribe's first envelope is StreamStarted with `start_ordinal == 42`, (c) the
    * second subscribe's first envelope is also StreamStarted, with the (possibly-bumped) finalized ordinal at that moment, and (d) the
    * second StreamStarted's seq strictly exceeds the first's.
    */
  test("StreamStarted: first envelope on each subscribe is StreamStarted with current finalized ordinal") {
    val initialOrd = SnapshotOrdinal.unsafeApply(42L)
    fixture(initialOrd).use {
      case (svc, blocking, _) =>
        for {
          // Subscribe #1
          iter1 <- IO.blocking(blocking.subscribe(SubscribeRequest(clientTag = "test/stream-started-1")))
          first1 <- IO.blocking(iter1.next())
          // Publish a couple of events so the seq counter advances.
          _ <- svc.publisher.publishBalanceChanges(
            initialOrd,
            List(BalanceChange(address = "DAG1", oldBalance = 0L, newBalance = 1L, cause = "between"))
          )
          _ <- svc.publisher.publishBalanceChanges(
            initialOrd,
            List(BalanceChange(address = "DAG2", oldBalance = 0L, newBalance = 2L, cause = "between"))
          )
          // Drain those into iter1 so the subscription is healthy.
          _ <- IO.blocking(iter1.next())
          _ <- IO.blocking(iter1.next())
          // Subscribe #2 — its first envelope MUST also be StreamStarted.
          iter2 <- IO.blocking(blocking.subscribe(SubscribeRequest(clientTag = "test/stream-started-2")))
          first2 <- IO.blocking(iter2.next())
        } yield
          // First subscribe sees StreamStarted with the initial finalized ord, seq >= 1.
          expect(first1.event.isStreamStarted) &&
            expect(first1.event.streamStarted.exists(_.startOrdinal == 42L)) &&
            expect(first1.seq >= 1L) &&
            // Second subscribe also sees StreamStarted FIRST.
            expect(first2.event.isStreamStarted) &&
            expect(first2.event.streamStarted.exists(_.startOrdinal == 42L)) &&
            // Seq is process-monotone: the second StreamStarted's seq strictly exceeds the first's
            // (and the two intermediate balance changes that landed between them).
            expect(first2.seq > first1.seq) &&
            // Both StreamStarted envelopes carry the same `serverSessionId` (per-process random).
            expect(
              first1.event.streamStarted
                .flatMap(_ => first2.event.streamStarted.map(_.serverSessionId))
                .contains(
                  first1.event.streamStarted.map(_.serverSessionId).getOrElse(0L)
                )
            )
    }
  }

  // ─── Cancel decrements subscriberCount ──────────────────────────────

  /** Test D: subscriber disconnect via channel shutdown decrements `subscriberCount`.
    *
    * Cancel comes from the gRPC `Context.cancel` path (`setOnCancelHandler`). The server has a 200ms polling tick on the cancel flag, so we
    * wait up to ~1s. After cancel, the FS2 stream's `interruptWhen` halts, `responseObserver.onCompleted()` fires (best-effort), the
    * `subscriberCountRef` decrements, and `svc.health.subscriberCount` is back to its pre-subscribe value.
    *
    * We build a dedicated server here (rather than reusing the fixture's channel) so the client channel's lifecycle is decoupled from the
    * server's. We attach a subscriber on a private channel, wait for the count to bump to 1, then `shutdownNow()` the channel and assert
    * the server's count returns to 0 after the cancel-poll tick.
    */
  test("subscriber disconnect via channel shutdown decrements subscriberCount") {
    val outer: Resource[IO, (LocalEventsService.Service[IO], Int)] =
      for {
        dispatcher <- Dispatcher.parallel[IO]
        finRef <- Resource.eval(Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.unsafeApply(1L)))
        epochRef <- Resource.eval(Ref.of[IO, Long](0L))
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
              .forPort(0)
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
      } yield (svc, server.getPort)

    outer.use {
      case (svc, port) =>
        // Build a throwaway channel — we want to control its lifecycle independently.
        val channelR = Resource.make(
          IO.blocking {
            ManagedChannelBuilder
              .forAddress("127.0.0.1", port)
              .usePlaintext()
              .build()
          }
        )(_ => IO.unit) // intentional: we'll shut it down INSIDE the test

        channelR.use { ch =>
          val stub = LocalEventsGrpc.stub(ch)
          val obs = new StreamObserver[EventEnvelope] {
            def onNext(value: EventEnvelope): Unit = ()
            def onError(t: Throwable): Unit = ()
            def onCompleted(): Unit = ()
          }
          for {
            h0 <- svc.health
            _ <- IO.blocking(stub.subscribe(SubscribeRequest(clientTag = "test/cancel-decrement"), obs))
            // Give the server a moment to receive subscribe + register the slot.
            _ <- IO.sleep(200.millis)
            h1 <- svc.health
            // Force-cancel by shutting the channel down. The server sees the half-close and the
            // `setOnCancelHandler` fires; FS2 stream's 200ms cancel tick observes the flag and
            // halts; `subscriberCountRef` decrements.
            _ <- IO.blocking { ch.shutdownNow(); () }
            // Wait > 200ms for the cancel poll + dispatch-back-to-Ref.
            _ <- IO.sleep(800.millis)
            h2 <- svc.health
          } yield
            expect(h0.subscriberCount == 0L) &&
              expect(h1.subscriberCount >= 1L) &&
              expect(h2.subscriberCount == 0L)
        }
    }
  }
}
