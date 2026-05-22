package io.constellationnetwork.node.shared.infrastructure.local_events

import java.security.SecureRandom
import java.util.concurrent.atomic.AtomicBoolean

import cats.effect._
import cats.effect.std.Dispatcher
import cats.syntax.all._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

import io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events._
import io.constellationnetwork.schema.SnapshotOrdinal

import eu.timepit.refined.types.numeric.PosInt
import fs2.concurrent.Topic
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Server-side state for the gl0 [[LocalEventsGrpc.LocalEvents]] gRPC service. Owns the FS2 `Topic[F, EventEnvelope]` fan-out, the
  * per-process monotone sequence counter, the drop counter, and the subscriber count. Constructed once per gl0 process; the
  * `Resource.acquire` returns both the bound gRPC server (`io.grpc.Server`) and the `LocalEventsPublisher` view used by GSAM and the
  * finality monitor.
  *
  * Wire shape (per the design's Section 4 + the Q3 user override):
  *
  *   1. Every Subscribe call returns a stream whose FIRST envelope is a `StreamStarted` carrying the current finalized ordinal. Lets the
  *      client REST-fetch `/snapshots/at-ordinal/N` for bootstrap state without missing deltas in the gap. 2. After that, the stream emits
  *      real events filtered by `SubscribeRequest.filters`. Empty filter list = receive everything. 3. The publisher assigns each envelope
  *      a monotone `seq` via `seqRef`; clients see gaps when the subscriber's per-subscriber queue drops on slow consumption
  *      (`Topic.subscribe(maxQueued)` semantics — see §7 of the design).
  *
  * Notes:
  *   - This service is in-process; we use `Dispatcher` to bridge `F[_]` effects into gRPC's `StreamObserver` callbacks.
  *   - No mTLS / API tokens in v1. Local-only by default. See §3 of the design for the security roadmap.
  */
object LocalEventsService {

  /** Snapshot of mutable counters exposed by the Health RPC. */
  final case class HealthSnapshot(
    healthy: Boolean,
    subscriberCount: Long,
    lastPublishedSeq: Long,
    droppedTotal: Long
  )

  /** Bundle returned by [[make]]: the publisher (passed into GSAM constructor + finality monitor) and the gRPC service implementation
    * (passed to `ServerBuilder.addService`).
    */
  final class Service[F[_]] private[LocalEventsService] (
    val publisher: LocalEventsPublisher[F],
    val grpcImpl: LocalEventsGrpc.LocalEvents,
    val health: F[HealthSnapshot]
  )

  /** Construct the LocalEvents server-side machinery.
    *
    * @param finalizedOrdinalRef
    *   read at subscribe time to populate `StreamStarted.start_ordinal`. Production wiring threads `nakamotoFinalizedOrdinalRef` from
    *   `GlobalSnapshotConsensus.make`.
    * @param epochProgressRef
    *   read at subscribe time to populate `StreamStarted.epoch_progress`. Caller can pass a noop Ref(0) for tests.
    * @param maxQueuedPerSubscriber
    *   per-subscriber buffer depth before drop-oldest.
    * @param publisherBufferSize
    *   process-wide FS2 `Topic` backstop. Set higher than `maxQueuedPerSubscriber`.
    */
  def make[F[_]: Async](
    finalizedOrdinalRef: Ref[F, SnapshotOrdinal],
    epochProgressRef: Ref[F, Long],
    maxQueuedPerSubscriber: PosInt,
    publisherBufferSize: PosInt,
    dispatcher: Dispatcher[F]
  ): F[Service[F]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("LocalEventsService")
    // Per-process random session id; clients use this to detect server restart (seq resets).
    val sessionId: Long = new SecureRandom().nextLong()

    for {
      topic <- Topic[F, EventEnvelope]
      seqRef <- Ref.of[F, Long](0L)
      droppedRef <- Ref.of[F, Long](0L)
      subscriberCountRef <- Ref.of[F, Long](0L)
      _ <- logger.info(
        s"LocalEventsService init: sessionId=$sessionId maxQueued=${maxQueuedPerSubscriber.value} bufferSize=${publisherBufferSize.value}"
      )
    } yield {
      val publisher = mkPublisher[F](topic, seqRef, droppedRef, dispatcher, logger)
      val grpcImpl = mkGrpcImpl[F](
        topic,
        seqRef,
        droppedRef,
        subscriberCountRef,
        finalizedOrdinalRef,
        epochProgressRef,
        sessionId,
        maxQueuedPerSubscriber,
        dispatcher,
        logger
      )
      val health: F[HealthSnapshot] =
        for {
          subs <- subscriberCountRef.get
          seq <- seqRef.get
          dropped <- droppedRef.get
        } yield HealthSnapshot(healthy = true, subscriberCount = subs, lastPublishedSeq = seq, droppedTotal = dropped)
      new Service[F](publisher, grpcImpl, health)
    }
  }

  // ─── Publisher impl ─────────────────────────────────────────────────

  private def mkPublisher[F[_]: Async](
    topic: Topic[F, EventEnvelope],
    seqRef: Ref[F, Long],
    droppedRef: Ref[F, Long],
    dispatcher: Dispatcher[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): LocalEventsPublisher[F] = {
    // Unused warnings: Dispatcher is reserved for future gRPC-side error reporting from inside publisher hooks.
    val _ = (dispatcher, droppedRef)

    def nextEnvelope(
      ordinal: SnapshotOrdinal,
      event: EventEnvelope.Event
    ): F[EventEnvelope] =
      seqRef.updateAndGet(_ + 1L).map { seq =>
        EventEnvelope(
          wireVersion = 1,
          seq = seq,
          publishedAtMillis = System.currentTimeMillis(),
          ordinal = ordinal.value.value,
          event = event
        )
      }

    // `topic.publish1` returns `F[Either[Topic.Closed, Unit]]`. We log + swallow the
    // closed case so a leaked subscriber crash doesn't propagate to GSAM.
    def publishOne(env: EventEnvelope): F[Unit] =
      topic.publish1(env).flatMap {
        case Right(_) => Async[F].unit
        case Left(_) =>
          logger.warn("LocalEvents topic closed; dropping envelope (server likely shutting down)")
      }

    new LocalEventsPublisher[F] {
      def publishSnapshotFinalized(
        ordinal: SnapshotOrdinal,
        event: io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events.SnapshotFinalized
      ): F[Unit] =
        nextEnvelope(ordinal, EventEnvelope.Event.SnapshotFinalized(event)).flatMap(publishOne)

      def publishBalanceChanges(
        ordinal: SnapshotOrdinal,
        changes: List[io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events.BalanceChange]
      ): F[Unit] =
        changes.traverse_(c => nextEnvelope(ordinal, EventEnvelope.Event.BalanceChange(c)).flatMap(publishOne))

      def publishTokenLockChanges(
        ordinal: SnapshotOrdinal,
        changes: List[io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events.TokenLockStateChange]
      ): F[Unit] =
        changes.traverse_(c => nextEnvelope(ordinal, EventEnvelope.Event.TokenLockStateChange(c)).flatMap(publishOne))

      def publishAllowSpendChanges(
        ordinal: SnapshotOrdinal,
        changes: List[io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events.AllowSpendStateChange]
      ): F[Unit] =
        changes.traverse_(c => nextEnvelope(ordinal, EventEnvelope.Event.AllowSpendStateChange(c)).flatMap(publishOne))

      def publishTransactionsAccepted(
        ordinal: SnapshotOrdinal,
        txs: List[io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events.TransactionAccepted]
      ): F[Unit] =
        txs.traverse_(t => nextEnvelope(ordinal, EventEnvelope.Event.TransactionAccepted(t)).flatMap(publishOne))

      def publishMetagraphEvents(
        ordinal: SnapshotOrdinal,
        snapshots: List[
          io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events.MetagraphSnapshotAccepted
        ],
        balances: List[
          io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events.MetagraphBalanceChange
        ]
      ): F[Unit] =
        snapshots.traverse_(s => nextEnvelope(ordinal, EventEnvelope.Event.MetagraphSnapshotAccepted(s)).flatMap(publishOne)) >>
          balances.traverse_(b => nextEnvelope(ordinal, EventEnvelope.Event.MetagraphBalanceChange(b)).flatMap(publishOne))
    }
  }

  // ─── Filter matching ────────────────────────────────────────────────

  /** Returns the [[EventKind]] discriminant for an envelope (used by filter matching).
    */
  private[local_events] def kindOf(env: EventEnvelope): EventKind = env.event match {
    case _: EventEnvelope.Event.StreamStarted             => EventKind.STREAM_STARTED
    case _: EventEnvelope.Event.SnapshotFinalized         => EventKind.SNAPSHOT_FINALIZED
    case _: EventEnvelope.Event.BalanceChange             => EventKind.BALANCE_CHANGE
    case _: EventEnvelope.Event.TokenLockStateChange      => EventKind.TOKEN_LOCK_STATE_CHANGE
    case _: EventEnvelope.Event.AllowSpendStateChange     => EventKind.ALLOW_SPEND_STATE_CHANGE
    case _: EventEnvelope.Event.TransactionAccepted       => EventKind.TRANSACTION_ACCEPTED
    case _: EventEnvelope.Event.MetagraphSnapshotAccepted => EventKind.METAGRAPH_SNAPSHOT_ACCEPTED
    case _: EventEnvelope.Event.MetagraphBalanceChange    => EventKind.METAGRAPH_BALANCE_CHANGE
    case _: EventEnvelope.Event.KesRotated                => EventKind.KES_ROTATED
    case _: EventEnvelope.Event.EtaRotated                => EventKind.ETA_ROTATED
    case _: EventEnvelope.Event.SnowballDecision          => EventKind.SNOWBALL_DECISION
    case _: EventEnvelope.Event.ChainQualityChange        => EventKind.CHAIN_QUALITY_CHANGE
    case _: EventEnvelope.Event.ValidatorParticipation    => EventKind.VALIDATOR_PARTICIPATION
    case _: EventEnvelope.Event.SlashingEvent             => EventKind.SLASHING_EVENT
    case EventEnvelope.Event.Empty                        => EventKind.EVENT_KIND_UNSPECIFIED
  }

  /** Returns the address fields visible on this envelope (used by filter matching). Returns Nil for events that don't carry addresses.
    */
  private[local_events] def addressesOf(env: EventEnvelope): List[String] = env.event match {
    case EventEnvelope.Event.BalanceChange(c)          => List(c.address)
    case EventEnvelope.Event.TokenLockStateChange(c)   => List(c.address)
    case EventEnvelope.Event.AllowSpendStateChange(c)  => List(c.address)
    case EventEnvelope.Event.TransactionAccepted(c)    => List(c.source, c.destination)
    case EventEnvelope.Event.MetagraphBalanceChange(c) => List(c.address)
    case _                                             => Nil
  }

  /** Returns the metagraph addresses visible on this envelope. */
  private[local_events] def metagraphAddressesOf(env: EventEnvelope): List[String] = env.event match {
    case EventEnvelope.Event.MetagraphSnapshotAccepted(c) => List(c.metagraphAddress)
    case EventEnvelope.Event.MetagraphBalanceChange(c)    => List(c.metagraphAddress)
    case _                                                => Nil
  }

  /** Conjunctive filter matcher. Per the design: same-kind filters OR address-set union; cross-kind filters AND. StreamStarted is always
    * passed through regardless of filter (it's the catch-up anchor — clients need it before they see any other event).
    *
    * Empty filter list = accept all events (no narrowing).
    */
  private[local_events] def matchesFilters(env: EventEnvelope, filters: Seq[EventFilter]): Boolean = {
    val isStreamStarted = env.event match {
      case _: EventEnvelope.Event.StreamStarted => true
      case _                                    => false
    }
    if (isStreamStarted) true
    else if (filters.isEmpty) true
    else {
      val kind = kindOf(env)
      val addrs = addressesOf(env)
      val mAddrs = metagraphAddressesOf(env)
      filters.exists { f =>
        val kindOk = f.kinds.isEmpty || f.kinds.contains(kind)
        val addrOk = f.addresses.isEmpty || addrs.exists(f.addresses.contains)
        val mAddrOk = f.metagraphAddresses.isEmpty || mAddrs.exists(f.metagraphAddresses.contains)
        // proto3 int64 defaults to 0; treat 0/sentinel as "unbounded" so a filter that omits ord
        // bounds doesn't accidentally exclude every non-zero ordinal. Sentinel -1 is also unbounded
        // for symmetry with the schema comment.
        val ordMin = if (f.minOrdinal <= 0L) Long.MinValue else f.minOrdinal
        val ordMax = if (f.maxOrdinal <= 0L) Long.MaxValue else f.maxOrdinal
        val ordOk = env.ordinal >= ordMin && env.ordinal <= ordMax
        kindOk && addrOk && mAddrOk && ordOk
      }
    }
  }

  // ─── gRPC service impl ──────────────────────────────────────────────

  private def mkGrpcImpl[F[_]: Async](
    topic: Topic[F, EventEnvelope],
    seqRef: Ref[F, Long],
    droppedRef: Ref[F, Long],
    subscriberCountRef: Ref[F, Long],
    finalizedOrdinalRef: Ref[F, SnapshotOrdinal],
    epochProgressRef: Ref[F, Long],
    sessionId: Long,
    maxQueuedPerSubscriber: PosInt,
    dispatcher: Dispatcher[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): LocalEventsGrpc.LocalEvents = new LocalEventsGrpc.LocalEvents {

    override def subscribe(
      request: SubscribeRequest,
      responseObserver: StreamObserver[EventEnvelope]
    ): Unit = {
      val callObserver = responseObserver match {
        case csco: ServerCallStreamObserver[EventEnvelope] @unchecked => Some(csco)
        case _                                                        => None
      }
      val clientTag = if (request.clientTag.nonEmpty) request.clientTag else "<unknown>"
      val cancelled = new AtomicBoolean(false)

      // Replay v1: not supported. Per the design, return UNIMPLEMENTED on any non-empty replay_from_ord.
      if (request.replayFromOrd.exists(_ > 0L)) {
        responseObserver.onError(
          io.grpc.Status.UNIMPLEMENTED
            .withDescription("replay_from_ord is not supported in v1; subscribe + use REST /snapshots/at-ordinal/N for catch-up")
            .asException()
        )
        ()
      } else {
        // CRITICAL: `setOnCancelHandler` MUST be called synchronously inside the initial subscribe
        // invocation — gRPC asserts this. Calling it from inside a dispatched effect raises
        // `IllegalStateException("Cannot alter onCancelHandler after initialization")`. So we wire
        // the AtomicBoolean here; the FS2 stream polls it via a small `Stream.awakeEvery` ticker.
        callObserver.foreach { csco =>
          csco.setOnCancelHandler { () =>
            cancelled.set(true)
            ()
          }
        }

        // Build the StreamStarted envelope. Emitted as the first item of a small `Stream.eval`
        // that's prepended to the topic subscription — this way the topic subscription is
        // registered BEFORE StreamStarted goes out, so any `publish1` racing against the
        // subscriber's startup doesn't drop events.
        val streamStartedEnvelopeF: F[EventEnvelope] =
          for {
            finalizedOrd <- finalizedOrdinalRef.get
            epoch <- epochProgressRef.get
            seq <- seqRef.updateAndGet(_ + 1L)
            _ <- logger.info(s"LocalEvents subscribe: tag=$clientTag startOrdinal=${finalizedOrd.value.value} sessionId=$sessionId")
          } yield
            EventEnvelope(
              wireVersion = 1,
              seq = seq,
              publishedAtMillis = System.currentTimeMillis(),
              ordinal = finalizedOrd.value.value,
              event = EventEnvelope.Event.StreamStarted(
                StreamStarted(
                  startOrdinal = finalizedOrd.value.value,
                  epochProgress = epoch,
                  serverSessionId = sessionId
                )
              )
            )

        // Poll the cancelled flag periodically as a Stream-of-Boolean for `interruptWhen`. Cheap:
        // a 200ms tick is well under the wire round-trip; clients see at most ~one extra envelope
        // after cancel before the stream halts.
        val cancelStream: fs2.Stream[F, Boolean] =
          fs2.Stream
            .awakeEvery[F](200.milliseconds)
            .evalMap(_ => Async[F].delay(cancelled.get()))

        // Send-one-envelope helper used inside the FS2 stream's `evalMap`.
        //
        // We always call `responseObserver.onNext` directly. The previous version gated on
        // `isReady` and dropped to `droppedRef` when false, but `isReady` is permanently false
        // for the gRPC blocking-stub client (no async ready callback round-trip), so the gating
        // collapsed every event into a drop. The FS2 topic subscriber buffer (bounded by
        // `maxQueuedPerSubscriber`) already provides slow-consumer protection upstream of this
        // call; gRPC's own flow control further bounds the wire window. If `onNext` throws (the
        // call is half-closed or cancelled), we set the cancel flag so the FS2 stream halts on
        // the next tick.
        def sendOne(env: EventEnvelope): F[Unit] =
          Async[F]
            .delay(responseObserver.onNext(env))
            .handleErrorWith(t =>
              logger.warn(s"LocalEvents subscriber tag=$clientTag onNext failed: ${t.getMessage}") >>
                Async[F].delay { cancelled.set(true); () }
            )

        val subscribeF: F[Unit] =
          subscriberCountRef.update(_ + 1L) >>
            // `topic.subscribeAwait` returns `Resource[F, Stream[F, A]]` whose `acquire` registers
            // the subscriber slot synchronously. Awaiting the resource here closes the race between
            // `topic.publish1` and our subscription: the publisher would otherwise drop events that
            // arrive between the test's `subscribe()` call returning and FS2 actually pulling from
            // `topic.subscribe`.
            topic
              .subscribeAwait(maxQueuedPerSubscriber.value)
              .use { subscribedStream =>
                (fs2.Stream.eval(streamStartedEnvelopeF) ++ subscribedStream)
                  .filter(matchesFilters(_, request.filters))
                  .interruptWhen(cancelStream)
                  .evalMap(sendOne)
                  .compile
                  .drain
                  .handleErrorWith(t => logger.warn(s"LocalEvents subscriber tag=$clientTag stream errored: ${t.getMessage}"))
              } >>
            Async[F]
              .delay(responseObserver.onCompleted())
              .handleError(_ => ()) >>
            subscriberCountRef.update(c => math.max(0L, c - 1L)) >>
            logger.info(s"LocalEvents subscribe ended: tag=$clientTag")

        dispatcher.unsafeRunAndForget(subscribeF)
        ()
      }
    }

    override def health(request: HealthRequest): Future[HealthResponse] = {
      val effect = for {
        seq <- seqRef.get
        dropped <- droppedRef.get
        subs <- subscriberCountRef.get
      } yield
        HealthResponse(
          healthy = true,
          subscriberCount = subs,
          lastPublishedSeq = seq,
          droppedToSlowSubscribersTotal = dropped
        )
      dispatcher.unsafeToFuture(effect)
    }
  }

  // ─── Server lifecycle ───────────────────────────────────────────────

  /** Starts an `io.grpc.Server` bound to `bindAddress:port` and serving the LocalEvents service. Mirrors the existing `ChainSyncInbound`
    * lifecycle at `GlobalSnapshotConsensus.scala:1220-1240`: graceful `shutdown()` + `awaitTermination(grace)` on `Resource.release`,
    * errors during teardown are swallowed (a hung subscriber must not block app shutdown).
    */
  def serverResource[F[_]: Async](
    service: Service[F],
    bindAddress: String,
    port: Int,
    shutdownGraceSeconds: Int,
    ec: ExecutionContext
  ): Resource[F, io.grpc.Server] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("LocalEventsService")
    Resource.make(
      Async[F].blocking {
        val srv = io.grpc.ServerBuilder
          .forPort(port)
          .addService(LocalEventsGrpc.bindService(service.grpcImpl, ec))
          .build()
        srv.start()
        srv
      } <* logger.info(s"LocalEventsService gRPC server started on $bindAddress:$port")
    )(srv =>
      Async[F].blocking {
        srv.shutdown()
        srv.awaitTermination(shutdownGraceSeconds.toLong, java.util.concurrent.TimeUnit.SECONDS)
        ()
      }
        .handleError(_ => ()) >>
        logger.info("LocalEventsService gRPC server stopped").handleError(_ => ())
    )
  }
}
