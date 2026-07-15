package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import cats.effect.kernel.{Async, Deferred, Ref}
import cats.effect.std.{Dispatcher, Queue, Semaphore}
import cats.effect.syntax.all._
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._

import fs2.{Pull, Stream}
import io.grpc.ManagedChannel
import io.grpc.stub.{ClientCallStreamObserver, ClientResponseObserver}

/** Converts the sidecar's server-streaming Subscribe RPC into an fs2.Stream.
  *
  * Uses cats-effect Dispatcher to safely bridge gRPC's StreamObserver callbacks into the F effect system.
  *
  * '''Topic filter (FINDING-F1).''' Every subscription MUST declare the message families it consumes ([[SidecarClient.SubscribeTopics]]):
  * the sidecar's shard-checkpoint families ride SHARED node-lifetime fan-in channels that tolerate exactly ONE drainer — two subscribe-all
  * streams race-drained them and the rumor bridge silently discarded the shard checkpoints it won (~half). The parameter is deliberately
  * required (no default): an accidental subscribe-all is exactly the bug this fixes. The sidecar validates labels and fails the stream on
  * an unknown one.
  *
  * '''Cancel-on-close (zombie-stream guard).''' The underlying gRPC call is cancelled when the fs2 stream's scope closes. Without this, a
  * JVM-side stream failure (e.g. an `evalMap` error triggering the caller's reconnect) leaked the old server-side subscription, which kept
  * draining the shared shard channels while its client-side observer dropped everything — the same F1 race re-entering through a single
  * logical consumer.
  */
object GossipStream {

  final case class SubscriptionProtocolError(detail: String) extends IllegalStateException(s"Invalid sidecar Subscribe handshake: $detail")

  final case class InboundBufferError(detail: String) extends IllegalStateException(s"Invalid sidecar inbound stream: $detail")

  /** Local decoded-envelope bounds for one Subscribe generation. These limits do not claim that the other transport hops accept envelopes
    * this large: grpc-java, Go gRPC, and GossipSub retain their own independent message-size contracts.
    *
    * `maxMessageBytes` is also the reservation charged for every outstanding manual gRPC credit. Consequently the live invariant is
    * `queuedBytes + outstandingCredits * maxMessageBytes <= maxQueuedBytes`, even before the next envelope's exact encoded size is known.
    */
  final case class BufferLimits(maxQueuedItems: Int, maxQueuedBytes: Long, maxMessageBytes: Int)

  object BufferLimits {
    val Default: BufferLimits = BufferLimits(
      maxQueuedItems = 64,
      maxQueuedBytes = 64L * 1024L * 1024L,
      maxMessageBytes = 32 * 1024 * 1024
    )
  }

  private final case class BufferedMessage(message: GossipMessage, encodedBytes: Long)

  private final case class FlowState(
    queuedItems: Int,
    queuedBytes: Long,
    outstandingCredits: Int,
    terminated: Boolean
  )

  private object FlowState {
    val empty: FlowState = FlowState(queuedItems = 0, queuedBytes = 0L, outstandingCredits = 0, terminated = false)
  }

  private sealed trait Admission
  private object Admission {
    case object Ignore extends Admission
    final case class Accept(buffered: BufferedMessage) extends Admission
    final case class Reject(error: Throwable) extends Admission
  }

  private sealed trait Terminal
  private object Terminal {
    final case class Failed(error: Throwable) extends Terminal
    case object Completed extends Terminal
  }

  /** Subscribe to incoming gossip messages from the sidecar as an fs2.Stream, filtered to `topics` (labels from
    * [[SidecarClient.SubscribeTopics]]).
    */
  def subscribe[F[_]: Async](
    channel: ManagedChannel,
    profile: SidecarClient.SubscriptionProfile,
    readiness: SidecarSubscriptionReadiness[F],
    limits: BufferLimits = BufferLimits.Default
  ): Stream[F, GossipMessage] = {
    val stub = SidecarServiceGrpc.stub(channel)

    Stream.eval(validateLimits[F](limits)) >> Stream.bracket(readiness.begin(profile.lane))(readiness.release).flatMap { attempt =>
      Stream.resource(Dispatcher.sequential[F]).flatMap { dispatcher =>
        Stream
          .eval(
            (
              Queue.bounded[F, BufferedMessage](limits.maxQueuedItems),
              Ref.of[F, FlowState](FlowState.empty),
              Deferred[F, Terminal],
              Semaphore[F](1L)
            ).tupled
          )
          .flatMap {
            case (queue, flowState, terminal, lifecycle) =>
              val callRef = new AtomicReference[Option[ClientCallStreamObserver[SubscribeRequest]]](None)
              val active = new AtomicBoolean(true)

              def cancelCall(reason: String, cause: Throwable): F[Unit] =
                Async[F].delay(callRef.get().foreach(_.cancel(reason, cause))).voidError

              def signalTerminal(outcome: Terminal, cancel: Boolean): F[Unit] =
                flowState.modify { state =>
                  if (state.terminated) state -> false
                  else state.copy(terminated = true) -> true
                }.flatMap {
                  case false => Async[F].unit
                  case true =>
                    val cancelF = outcome match {
                      case Terminal.Failed(error) if cancel => cancelCall("invalid subscriber stream", error)
                      case _                                => Async[F].unit
                    }
                    cancelF >> readiness.release(attempt).guarantee(terminal.complete(outcome).void)
                }

              def reserveAndRequest(requestStream: ClientCallStreamObserver[SubscribeRequest]): F[Unit] =
                flowState.modify { state =>
                  if (state.terminated) state -> 0
                  else {
                    val freeItems = limits.maxQueuedItems - state.queuedItems - state.outstandingCredits
                    val reservedBytes = state.outstandingCredits.toLong * limits.maxMessageBytes.toLong
                    val freeBytes = limits.maxQueuedBytes - state.queuedBytes - reservedBytes
                    val byteCredits = (freeBytes / limits.maxMessageBytes.toLong).max(0L)
                    val credits = math.min(freeItems.toLong, byteCredits).max(0L).toInt
                    state.copy(outstandingCredits = state.outstandingCredits + credits) -> credits
                  }
                }.flatMap { credits =>
                  Async[F]
                    .delay(requestStream.request(credits))
                    .handleErrorWith(error => signalTerminal(Terminal.Failed(error), cancel = true))
                    .whenA(credits > 0)
                }

              def reserveAndRequestCurrent: F[Unit] =
                callRef
                  .get()
                  .liftTo[F](InboundBufferError("gRPC call handle was unavailable while replenishing demand"))
                  .flatMap(reserveAndRequest)

              def admit(value: GossipMessage): F[Unit] = {
                val encodedBytes = value.serializedSize.toLong
                flowState.modify { state =>
                  if (state.terminated) state -> Admission.Ignore
                  else if (encodedBytes > limits.maxMessageBytes.toLong)
                    state -> Admission.Reject(
                      InboundBufferError(
                        s"encoded envelope exceeded maxMessageBytes: actual=$encodedBytes limit=${limits.maxMessageBytes}"
                      )
                    )
                  else if (state.outstandingCredits <= 0)
                    state -> Admission.Reject(InboundBufferError("received an envelope without an outstanding manual-flow-control credit"))
                  else {
                    val next = state.copy(
                      queuedItems = state.queuedItems + 1,
                      queuedBytes = state.queuedBytes + encodedBytes,
                      outstandingCredits = state.outstandingCredits - 1
                    )
                    val reservedBytes = next.outstandingCredits.toLong * limits.maxMessageBytes.toLong
                    if (
                      next.queuedItems + next.outstandingCredits > limits.maxQueuedItems ||
                      next.queuedBytes + reservedBytes > limits.maxQueuedBytes
                    )
                      state -> Admission.Reject(InboundBufferError("manual-flow-control reservation invariant was exceeded"))
                    else next -> Admission.Accept(BufferedMessage(value, encodedBytes))
                  }
                }.flatMap {
                  case Admission.Ignore        => Async[F].unit
                  case Admission.Reject(error) => signalTerminal(Terminal.Failed(error), cancel = true)
                  case Admission.Accept(buffered) =>
                    queue.tryOffer(buffered).flatMap {
                      case true => reserveAndRequestCurrent
                      case false =>
                        signalTerminal(
                          Terminal.Failed(InboundBufferError("bounded data queue rejected an envelope covered by a reserved credit")),
                          cancel = true
                        )
                    }
                }
              }

              def withLiveGeneration(action: F[Unit]): F[Unit] =
                lifecycle.permit.use(_ => Async[F].delay(active.get()).ifM(action, Async[F].unit))

              // The synchronous active check avoids filling a closing Dispatcher's task queue. The
              // effectful check under `lifecycle` is the actual generation fence.
              def safeRun(action: F[Unit]): Unit =
                if (active.get())
                  try dispatcher.unsafeRunAndForget(withLiveGeneration(action))
                  catch { case _: IllegalStateException => () }

              val observer = new ClientResponseObserver[SubscribeRequest, GossipMessage] {
                override def beforeStart(requestStream: ClientCallStreamObserver[SubscribeRequest]): Unit = {
                  // `disableAutoInboundFlowControl()` still grants one implicit initial credit in
                  // grpc-java. Start at zero so every delivered callback is covered by the byte/item
                  // reservation recorded immediately before our explicit `request(n)`.
                  requestStream.disableAutoRequestWithInitial(0)
                  callRef.set(Some(requestStream))
                }

                override def onNext(value: GossipMessage): Unit = safeRun(admit(value))

                override def onError(t: Throwable): Unit = safeRun(signalTerminal(Terminal.Failed(t), cancel = false))

                override def onCompleted(): Unit = safeRun(signalTerminal(Terminal.Completed, cancel = false))
              }

              val startSubscription: F[Unit] =
                validateProfile[F](profile) >> Async[F].delay {
                  stub.subscribe(SubscribeRequest(topics = profile.topics, role = profile.role), observer)
                } >> reserveAndRequestCurrent

              def releaseBuffered(buffered: BufferedMessage): F[Unit] =
                flowState.modify { state =>
                  val next = state.copy(
                    queuedItems = state.queuedItems - 1,
                    queuedBytes = state.queuedBytes - buffered.encodedBytes
                  )
                  val valid = next.queuedItems >= 0 && next.queuedBytes >= 0L
                  if (valid) next -> none[Throwable]
                  else state.copy(terminated = true) -> InboundBufferError("bounded data queue accounting underflow").some
                }.flatMap {
                  case Some(error) => terminal.complete(Terminal.Failed(error)).void
                  case None        => reserveAndRequestCurrent
                }

              def terminalResult(outcome: Terminal): F[Option[GossipMessage]] = outcome match {
                case Terminal.Failed(error) => Async[F].raiseError(error)
                case Terminal.Completed =>
                  queue.tryTake.flatMap {
                    case Some(buffered) => withLiveGeneration(releaseBuffered(buffered)).as(buffered.message.some)
                    case None           => none[GossipMessage].pure[F]
                  }
              }

              def nextResponse: F[Option[GossipMessage]] =
                terminal.tryGet.flatMap {
                  case Some(outcome) => terminalResult(outcome)
                  case None =>
                    Async[F].race(terminal.get, queue.take).flatMap {
                      case Left(outcome) => terminalResult(outcome)
                      case Right(buffered) =>
                        withLiveGeneration(releaseBuffered(buffered)) >> terminal.tryGet.flatMap {
                          case Some(Terminal.Failed(error)) => Async[F].raiseError(error)
                          case None                         => buffered.message.some.pure[F]
                          case Some(Terminal.Completed)     => buffered.message.some.pure[F]
                        }
                    }
                }

              val stopSubscription: F[Unit] = Async[F].uncancelable { _ =>
                lifecycle.permit.use { _ =>
                  Async[F].delay(active.getAndSet(false)).flatMap {
                    case true  => flowState.update(_.copy(terminated = true)) >> cancelCall("subscriber stream scope closed", null)
                    case false => Async[F].unit
                  }
                }
              }

              val responses = Stream.bracket(Async[F].unit)(_ => stopSubscription).flatMap { _ =>
                Stream.eval(startSubscription) >> Stream.repeatEval(nextResponse).unNoneTerminate
              }

              validateStartedFirst(responses, profile, attempt, readiness)
          }
      }
    }
  }

  private def validateLimits[F[_]: Async](limits: BufferLimits): F[Unit] = {
    val valid =
      limits.maxQueuedItems > 0 && limits.maxQueuedBytes > 0L && limits.maxMessageBytes > 0 &&
        limits.maxMessageBytes.toLong <= limits.maxQueuedBytes
    Async[F]
      .raiseError[Unit](
        InboundBufferError(
          s"invalid buffer limits: maxQueuedItems=${limits.maxQueuedItems}, maxQueuedBytes=${limits.maxQueuedBytes}, maxMessageBytes=${limits.maxMessageBytes}"
        )
      )
      .unlessA(valid)
  }

  private def validateProfile[F[_]: Async](profile: SidecarClient.SubscriptionProfile): F[Unit] =
    Async[F]
      .raiseError[Unit](SubscriptionProtocolError("topics must be explicit, nonempty, and distinct"))
      .unlessA(
        profile.topics.nonEmpty && profile.topics.distinct.size === profile.topics.size
      ) >>
      Async[F]
        .raiseError[Unit](SubscriptionProtocolError("subscriber role must be explicit"))
        .whenA(profile.role.isSubscriberRoleUnspecified || profile.role.isUnrecognized)

  private def validateStartedFirst[F[_]: Async](
    responses: Stream[F, GossipMessage],
    profile: SidecarClient.SubscriptionProfile,
    attempt: SidecarSubscriptionReadiness.Attempt,
    readiness: SidecarSubscriptionReadiness[F]
  ): Stream[F, GossipMessage] = {
    def rejectLaterStarted(message: GossipMessage): F[GossipMessage] = message.body match {
      case _: GossipMessage.Body.Started =>
        Async[F].raiseError(SubscriptionProtocolError("SubscribeStarted appeared more than once"))
      case _ => message.pure[F]
    }

    responses.pull.uncons1.flatMap {
      case None => Pull.raiseError(SubscriptionProtocolError("stream ended before SubscribeStarted"))
      case Some((first, tail)) =>
        first.body match {
          case GossipMessage.Body.Started(started) =>
            Pull.eval(
              validateStarted(profile, started) >> readiness.acknowledge(
                attempt,
                SidecarSubscriptionReadiness.Acknowledgement(started.sidecarSessionId, started.streamGeneration)
              )
            ) >> tail.evalMap(rejectLaterStarted).pull.echo
          case other =>
            Pull.raiseError(SubscriptionProtocolError(s"first response was ${other.getClass.getSimpleName}, not SubscribeStarted"))
        }
    }.stream
  }

  private def validateStarted[F[_]: Async](
    profile: SidecarClient.SubscriptionProfile,
    started: SubscribeStarted
  ): F[Unit] =
    Async[F]
      .raiseError[Unit](
        SubscriptionProtocolError(s"role mismatch: requested=${profile.role.name}, acknowledged=${started.role.name}")
      )
      .unlessA(started.role.value === profile.role.value) >>
      Async[F]
        .raiseError[Unit](
          SubscriptionProtocolError(
            s"topic mismatch: requested=${profile.topics.mkString("[", ",", "]")}, acknowledged=${started.topics.mkString("[", ",", "]")}"
          )
        )
        .unlessA(started.topics === profile.topics) >>
      Async[F]
        .raiseError[Unit](SubscriptionProtocolError("sidecar_session_id was empty"))
        .whenA(started.sidecarSessionId.trim.isEmpty) >>
      Async[F]
        .raiseError[Unit](SubscriptionProtocolError("stream_generation must be positive"))
        .whenA(started.streamGeneration <= 0L)
}
