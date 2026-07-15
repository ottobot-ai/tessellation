package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import cats.effect.{Deferred, IO}
import cats.syntax.all._

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._

import com.google.protobuf.ByteString
import io.grpc._
import weaver.SimpleIOSuite

object GossipStreamFlowControlSuite extends SimpleIOSuite {

  private val profile = SidecarClient.SubscriptionProfile.rumorBridge

  private final case class Script(
    messages: Vector[GossipMessage],
    terminalAfter: Option[Int] = None,
    terminalStatus: Status = Status.OK,
    completeOnExhaustion: Boolean = false,
    failRequestCall: Option[Int] = None
  )

  private final class ScriptedCall(script: Script) extends ClientCall[SubscribeRequest, GossipMessage] {
    private val listener = new AtomicReference[ClientCall.Listener[GossipMessage]]()
    private val closed = new AtomicBoolean(false)
    private val nextIndex = new AtomicInteger(0)
    private val requestCalls = new AtomicInteger(0)

    val cancelled = new AtomicBoolean(false)
    val delivered = new AtomicInteger(0)
    val requested = new AtomicInteger(0)
    val subscribeRequest = new AtomicReference[SubscribeRequest]()

    override def start(responseListener: ClientCall.Listener[GossipMessage], headers: Metadata): Unit = synchronized {
      listener.set(responseListener)
    }

    override def request(numMessages: Int): Unit = synchronized {
      if (!closed.get()) {
        if (listener.get() eq null) throw new IllegalStateException("Not started")
        val requestCall = requestCalls.incrementAndGet()
        if (script.failRequestCall.contains(requestCall)) throw new IllegalStateException("scripted request failure")
        requested.addAndGet(numMessages)
        pump(numMessages)
      }
    }

    override def cancel(message: String, cause: Throwable): Unit = {
      cancelled.set(true)
      close(Status.CANCELLED.withDescription(message).withCause(cause))
    }

    override def halfClose(): Unit = ()

    override def sendMessage(message: SubscribeRequest): Unit = subscribeRequest.set(message)

    def emitLate(message: GossipMessage): Unit = Option(listener.get()).foreach(_.onMessage(message))

    private def pump(numMessages: Int): Unit = {
      var remaining = numMessages
      while (remaining > 0 && nextIndex.get() < script.messages.size && !closed.get()) {
        val message = script.messages(nextIndex.getAndIncrement())
        delivered.incrementAndGet()
        listener.get().onMessage(message)
        remaining -= 1
        if (script.terminalAfter.contains(delivered.get())) close(script.terminalStatus)
      }
      if (script.completeOnExhaustion && nextIndex.get() === script.messages.size) close(script.terminalStatus)
    }

    private def close(status: Status): Unit =
      if (closed.compareAndSet(false, true)) Option(listener.get()).foreach(_.onClose(status, new Metadata()))
  }

  private final class ScriptedChannel(scripts: List[Script]) extends ManagedChannel {
    private val remaining = new ConcurrentLinkedQueue[Script](scripts.asJava)
    private val stopped = new AtomicBoolean(false)
    val calls = new ConcurrentLinkedQueue[ScriptedCall]()

    override def newCall[RequestT, ResponseT](method: MethodDescriptor[RequestT, ResponseT], options: CallOptions)
      : ClientCall[RequestT, ResponseT] = {
      val script = Option(remaining.poll()).getOrElse(throw new IllegalStateException("no scripted Subscribe call remained"))
      val call = new ScriptedCall(script)
      calls.add(call)
      call.asInstanceOf[ClientCall[RequestT, ResponseT]]
    }

    override def authority(): String = "scripted-sidecar"

    override def shutdown(): ManagedChannel = {
      stopped.set(true)
      this
    }

    override def isShutdown(): Boolean = stopped.get()

    override def isTerminated(): Boolean = stopped.get()

    override def shutdownNow(): ManagedChannel = shutdown()

    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = stopped.get()
  }

  private def started(generation: Long, session: String = "sidecar-session"): GossipMessage =
    GossipMessage(
      GossipMessage.Body.Started(
        SubscribeStarted(profile.topics, profile.role, streamGeneration = generation, sidecarSessionId = session)
      )
    )

  private def rumor(id: Int, payloadBytes: Int = 0): GossipMessage =
    GossipMessage(
      GossipMessage.Body.Rumor(
        Rumor(
          signedRumorBytes = ByteString.copyFrom(Array.fill[Byte](payloadBytes)((id & 0xff).toByte)),
          contentType = s"rumor-$id"
        )
      )
    )

  private def makeReadiness: IO[SidecarSubscriptionReadiness[IO]] =
    ProductionGate.make[IO].flatMap(SidecarSubscriptionReadiness.make[IO])

  private def awaitCondition(label: String, condition: IO[Boolean], remaining: Int = 200): IO[Unit] =
    condition.flatMap {
      case true                    => IO.unit
      case false if remaining <= 0 => IO.raiseError(new RuntimeException(s"timed out waiting for $label"))
      case false                   => IO.sleep(10.millis) >> awaitCondition(label, condition, remaining - 1)
    }

  test("clean completion drains every admitted envelope in order") {
    val payloads = Vector.tabulate(6)(rumor(_))
    val script = Script(started(1L) +: payloads, completeOnExhaustion = true)
    val channel = new ScriptedChannel(script :: Nil)
    val largest = script.messages.map(_.serializedSize).max
    val limits = GossipStream.BufferLimits(maxQueuedItems = 2, maxQueuedBytes = largest.toLong * 2L, maxMessageBytes = largest)

    for {
      readiness <- makeReadiness
      received <- GossipStream.subscribe[IO](channel, profile, readiness, limits).compile.toVector.timeout(3.seconds)
      status <- readiness.current
    } yield
      expect.same(payloads.map(_.getRumor.contentType), received.map(_.getRumor.contentType))
        .and(expect(status.rumor.isEmpty))
  }

  test("manual demand stops an item flood and cancellation closes the exact call") {
    val messages = started(1L) +: Vector.tabulate(100)(rumor(_))
    val channel = new ScriptedChannel(Script(messages) :: Nil)
    val largest = messages.map(_.serializedSize).max
    val limits = GossipStream.BufferLimits(maxQueuedItems = 2, maxQueuedBytes = largest.toLong * 20L, maxMessageBytes = largest)

    for {
      readiness <- makeReadiness
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      fiber <- GossipStream
        .subscribe[IO](channel, profile, readiness, limits)
        .evalMap(_ => entered.complete(()).void >> release.get)
        .compile
        .drain
        .start
      _ <- entered.get.timeout(3.seconds)
      call <- IO(channel.calls.element())
      _ <- awaitCondition("item buffer to fill", IO(call.delivered.get() >= 4))
      stableAt <- IO(call.delivered.get()) <* IO.sleep(100.millis)
      stillStable <- IO(call.delivered.get())
      _ <- fiber.cancel
      _ <- awaitCondition("call cancellation", IO(call.cancelled.get()))
    } yield
      expect.same(4, stableAt)
        .and(expect.same(stableAt, stillStable))
        .and(expect(call.requested.get() < messages.size))
        .and(expect(call.cancelled.get()))
  }

  test("encoded-byte reservations stop a large-envelope flood and oversize input fails closed") {
    val large = rumor(1, payloadBytes = 4096)
    val floodMessages = started(1L) +: Vector.fill(20)(large)
    val largest = floodMessages.map(_.serializedSize).max
    val floodLimits = GossipStream.BufferLimits(maxQueuedItems = 10, maxQueuedBytes = largest.toLong * 2L, maxMessageBytes = largest)
    val floodChannel = new ScriptedChannel(Script(floodMessages) :: Nil)

    val validStart = started(2L)
    val oversize = rumor(2, payloadBytes = validStart.serializedSize + 256)
    val oversizeLimit = validStart.serializedSize
    val oversizeLimits = GossipStream.BufferLimits(2, oversizeLimit.toLong * 2L, oversizeLimit)
    val oversizeChannel = new ScriptedChannel(Script(Vector(validStart, oversize)) :: Nil)

    for {
      readiness <- makeReadiness
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      floodFiber <- GossipStream
        .subscribe[IO](floodChannel, profile, readiness, floodLimits)
        .evalMap(_ => entered.complete(()).void >> release.get)
        .compile
        .drain
        .start
      _ <- entered.get.timeout(3.seconds)
      floodCall <- IO(floodChannel.calls.element())
      _ <- awaitCondition("byte buffer to fill", IO(floodCall.delivered.get() >= 4))
      stableAt <- IO(floodCall.delivered.get()) <* IO.sleep(100.millis)
      stillStable <- IO(floodCall.delivered.get())
      _ <- floodFiber.cancel
      oversizeReadiness <- makeReadiness
      oversizeResult <- GossipStream
        .subscribe[IO](oversizeChannel, profile, oversizeReadiness, oversizeLimits)
        .compile
        .drain
        .attempt
        .timeout(3.seconds)
      oversizeCall <- IO(oversizeChannel.calls.element())
    } yield
      expect.same(4, stableAt)
        .and(expect.same(stableAt, stillStable))
        .and(expect(oversizeResult.left.exists(_.isInstanceOf[GossipStream.InboundBufferError])))
        .and(expect(oversizeCall.cancelled.get()))
  }

  test("terminal error bypasses a full data queue and releases readiness before the consumer resumes") {
    val payloads = Vector.tabulate(3)(rumor(_))
    val unavailable = Status.UNAVAILABLE.withDescription("scripted sidecar failure")
    val messages = started(1L) +: payloads
    val channel = new ScriptedChannel(
      Script(messages, terminalAfter = 4.some, terminalStatus = unavailable, completeOnExhaustion = false) :: Nil
    )
    val largest = messages.map(_.serializedSize).max
    val limits = GossipStream.BufferLimits(2, largest.toLong * 20L, largest)

    for {
      readiness <- makeReadiness
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      fiber <- GossipStream
        .subscribe[IO](channel, profile, readiness, limits)
        .evalMap(_ => entered.complete(()).void >> release.get)
        .compile
        .drain
        .attempt
        .start
      _ <- entered.get.timeout(3.seconds)
      call <- IO(channel.calls.element())
      _ <- awaitCondition("scripted terminal callback", IO(call.delivered.get() === 4))
      _ <- awaitCondition("prompt readiness release", readiness.current.map(_.rumor.isEmpty))
      _ <- release.complete(())
      result <- fiber.joinWithNever.timeout(3.seconds)
    } yield
      expect.same(4, call.delivered.get())
        .and(expect.same(Status.Code.UNAVAILABLE.some, result.left.toOption.map(Status.fromThrowable).map(_.getCode)))
  }

  test("post-start request failure cancels promptly and releases an acknowledged lane") {
    val payloads = Vector.tabulate(3)(rumor(_))
    val messages = started(1L) +: payloads
    val channel = new ScriptedChannel(Script(messages, failRequestCall = 4.some) :: Nil)
    val largest = messages.map(_.serializedSize).max
    val limits = GossipStream.BufferLimits(1, largest.toLong * 10L, largest)

    for {
      readiness <- makeReadiness
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      fiber <- GossipStream
        .subscribe[IO](channel, profile, readiness, limits)
        .evalMap(_ => entered.complete(()).void >> release.get)
        .compile
        .drain
        .attempt
        .start
      _ <- entered.get.timeout(3.seconds)
      acknowledged <- readiness.current
      _ <- release.complete(())
      result <- fiber.joinWithNever.timeout(3.seconds)
      call <- IO(channel.calls.element())
      _ <- awaitCondition("request-failure cancellation", IO(call.cancelled.get()))
      released <- readiness.current
    } yield
      expect(acknowledged.rumor.nonEmpty)
        .and(expect(result.left.exists(_.getMessage === "scripted request failure")))
        .and(expect(call.cancelled.get()))
        .and(expect(released.rumor.isEmpty))
  }

  test("clean completion drains a full admitted tail after the blocked consumer resumes") {
    val payloads = Vector.tabulate(3)(rumor(_))
    val messages = started(1L) +: payloads
    val channel = new ScriptedChannel(
      Script(messages, terminalAfter = messages.size.some, terminalStatus = Status.OK) :: Nil
    )
    val largest = messages.map(_.serializedSize).max
    val limits = GossipStream.BufferLimits(2, largest.toLong * 20L, largest)
    val blockedFirst = new AtomicBoolean(false)

    for {
      readiness <- makeReadiness
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      fiber <- GossipStream
        .subscribe[IO](channel, profile, readiness, limits)
        .evalMap { message =>
          IO(blockedFirst.compareAndSet(false, true)).flatMap {
            case true  => entered.complete(()).void >> release.get.as(message)
            case false => message.pure[IO]
          }
        }
        .compile
        .toVector
        .start
      _ <- entered.get.timeout(3.seconds)
      call <- IO(channel.calls.element())
      _ <- awaitCondition("clean terminal with full admitted tail", IO(call.delivered.get() === messages.size))
      _ <- awaitCondition("clean-terminal readiness release", readiness.current.map(_.rumor.isEmpty))
      _ <- release.complete(())
      received <- fiber.joinWithNever.timeout(3.seconds)
    } yield
      expect.same(payloads.map(_.getRumor.contentType), received.map(_.getRumor.contentType))
        .and(expect.same(messages.size, call.delivered.get()))
  }

  test("a cancelled generation cannot acknowledge a late callback after reconnect") {
    val firstPayload = rumor(1)
    val secondPayload = rumor(2)
    val channel = new ScriptedChannel(
      List(
        Script(Vector(started(1L), firstPayload)),
        Script(Vector(started(2L), secondPayload))
      )
    )

    for {
      readiness <- makeReadiness
      first <- GossipStream.subscribe[IO](channel, profile, readiness).take(1).compile.lastOrError.timeout(3.seconds)
      firstCall <- IO(channel.calls.asScala.head)
      _ <- awaitCondition("first generation cancellation", IO(firstCall.cancelled.get()))
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      secondFiber <- GossipStream
        .subscribe[IO](channel, profile, readiness)
        .evalMap(_ => entered.complete(()).void >> release.get)
        .compile
        .drain
        .start
      _ <- entered.get.timeout(3.seconds)
      beforeLate <- readiness.current
      _ <- IO(firstCall.emitLate(started(99L, session = "stale-session")))
      _ <- IO.sleep(100.millis)
      afterLate <- readiness.current
      _ <- secondFiber.cancel
    } yield
      expect.same("rumor-1", first.getRumor.contentType)
        .and(expect.same(2L.some, beforeLate.rumor.map(_.streamGeneration)))
        .and(expect.same(beforeLate.rumor, afterLate.rumor))
  }
}
