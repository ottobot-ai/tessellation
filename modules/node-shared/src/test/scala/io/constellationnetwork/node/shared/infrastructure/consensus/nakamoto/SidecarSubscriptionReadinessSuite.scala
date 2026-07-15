package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import cats.effect.kernel.Ref
import cats.effect.{Deferred, IO}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarSubscriptionReadiness._

import weaver.SimpleIOSuite

object SidecarSubscriptionReadinessSuite extends SimpleIOSuite {

  private final case class PauseBlock(entered: Deferred[IO, Unit], continue: Deferred[IO, Unit])

  private final case class GateHarness(
    gate: ProductionGate[IO],
    armPause: IO[PauseBlock]
  )

  private def gateHarness: IO[GateHarness] =
    for {
      reasons <- Ref.of[IO, Set[String]](Set.empty)
      nextPause <- Ref.of[IO, Option[PauseBlock]](None)
    } yield GateHarness(
      new ProductionGate[IO] {
        def pause(reason: String): IO[Unit] =
          nextPause.getAndSet(None).flatMap(_.traverse_(block => block.entered.complete(()) >> block.continue.get)) >>
            reasons.update(_ + reason)

        def resume(reason: String): IO[Unit] = reasons.update(_ - reason)

        def isOpen: IO[Boolean] = reasons.get.map(_.isEmpty)

        def pauseReasons: IO[Set[String]] = reasons.get
      },
      (Deferred[IO, Unit], Deferred[IO, Unit]).tupled.flatMap { case (entered, continue) =>
        val block = PauseBlock(entered, continue)
        nextPause.set(block.some).as(block)
      }
    )

  private def ack(session: String, generation: Long): Acknowledgement = Acknowledgement(session, generation)

  private def makeReadiness: IO[SidecarSubscriptionReadiness[IO]] =
    ProductionGate.make[IO].flatMap(SidecarSubscriptionReadiness.make[IO])

  private def awaitCondition(condition: IO[Boolean], remaining: Int = 100): IO[Unit] =
    condition.flatMap {
      case true                    => IO.unit
      case false if remaining <= 0 => IO.raiseError(new RuntimeException("condition did not become true"))
      case false                   => IO.sleep(10.millis) >> awaitCondition(condition, remaining - 1)
    }

  test("both lanes become ready only under the same nonempty sidecar session") {
    for {
      readiness <- makeReadiness
      rumor <- readiness.begin(Lane.RumorBridge)
      _ <- readiness.acknowledge(rumor, ack("session-a", 11L))
      nakamoto <- readiness.begin(Lane.NakamotoSync)
      _ <- readiness.acknowledge(nakamoto, ack("session-b", 12L))
      mixed <- readiness.current
      timedOut <- readiness.awaitBoth.timeoutTo(100.millis, IO.pure(Ready("timeout", 0L, 0L))).map(_.sidecarSessionId === "timeout")
      nakamoto2 <- readiness.begin(Lane.NakamotoSync)
      _ <- readiness.acknowledge(nakamoto2, ack("session-a", 13L))
      ready <- readiness.awaitBoth.timeout(1.second)
    } yield expect(mixed.ready.isEmpty)
      .and(expect(timedOut))
      .and(expect.same(Ready("session-a", 11L, 13L), ready))
  }

  test("stale acknowledgement and teardown cannot overwrite or clear a newer lane attempt") {
    for {
      readiness <- makeReadiness
      old <- readiness.begin(Lane.RumorBridge)
      _ <- readiness.acknowledge(old, ack("old-session", 1L))
      current <- readiness.begin(Lane.RumorBridge)
      _ <- readiness.acknowledge(current, ack("new-session", 2L))
      staleAck <- readiness.acknowledge(old, ack("old-session", 3L)).attempt
      _ <- readiness.release(old)
      afterStaleRelease <- readiness.current
      _ <- readiness.release(current)
      afterCurrentRelease <- readiness.current
    } yield expect(staleAck.left.exists(_.isInstanceOf[StaleAttempt]))
      .and(expect.same(ack("new-session", 2L).some, afterStaleRelease.rumor))
      .and(expect(afterCurrentRelease.rumor.isEmpty))
  }

  test("beginning a replacement attempt invalidates readiness before it is acknowledged") {
    for {
      readiness <- makeReadiness
      rumor <- readiness.begin(Lane.RumorBridge)
      _ <- readiness.acknowledge(rumor, ack("session", 1L))
      nakamoto <- readiness.begin(Lane.NakamotoSync)
      _ <- readiness.acknowledge(nakamoto, ack("session", 2L))
      before <- readiness.current
      _ <- readiness.begin(Lane.NakamotoSync)
      after <- readiness.current
    } yield expect(before.ready.nonEmpty).and(expect(after.ready.isEmpty)).and(expect(after.nakamoto.isEmpty))
  }

  test("a terminated attempt cannot be re-acknowledged by an already queued Started callback") {
    for {
      readiness <- makeReadiness
      attempt <- readiness.begin(Lane.RumorBridge)
      _ <- readiness.release(attempt)
      result <- readiness.acknowledge(attempt, ack("session", 1L)).attempt
      status <- readiness.current
    } yield expect(result.left.exists(_.isInstanceOf[StaleAttempt])).and(expect(status.rumor.isEmpty))
  }

  test("empty session and nonpositive generation acknowledgements fail closed") {
    for {
      readiness <- makeReadiness
      emptySessionAttempt <- readiness.begin(Lane.RumorBridge)
      emptySession <- readiness.acknowledge(emptySessionAttempt, ack("", 1L)).attempt
      zeroGenerationAttempt <- readiness.begin(Lane.RumorBridge)
      zeroGeneration <- readiness.acknowledge(zeroGenerationAttempt, ack("session", 0L)).attempt
      status <- readiness.current
    } yield expect(emptySession.left.exists(_.isInstanceOf[InvalidAcknowledgement]))
      .and(expect(zeroGeneration.left.exists(_.isInstanceOf[InvalidAcknowledgement])))
      .and(expect(status.rumor.isEmpty))
  }

  test("same-session stream generation must strictly increase per lane across reconnects") {
    for {
      readiness <- makeReadiness
      first <- readiness.begin(Lane.RumorBridge)
      _ <- readiness.acknowledge(first, ack("session", 5L))
      _ <- readiness.release(first)
      replay <- readiness.begin(Lane.RumorBridge)
      replayResult <- readiness.acknowledge(replay, ack("session", 5L)).attempt
      lower <- readiness.begin(Lane.RumorBridge)
      lowerResult <- readiness.acknowledge(lower, ack("session", 4L)).attempt
      higher <- readiness.begin(Lane.RumorBridge)
      higherResult <- readiness.acknowledge(higher, ack("session", 6L)).attempt
    } yield expect(replayResult.left.exists(_.isInstanceOf[ReplayedGeneration]))
      .and(expect(lowerResult.left.exists(_.isInstanceOf[ReplayedGeneration])))
      .and(expect(higherResult.isRight))
  }

  test("the two current lanes cannot reuse one process-global stream generation") {
    for {
      readiness <- makeReadiness
      rumor <- readiness.begin(Lane.RumorBridge)
      _ <- readiness.acknowledge(rumor, ack("session", 7L))
      nakamoto <- readiness.begin(Lane.NakamotoSync)
      duplicate <- readiness.acknowledge(nakamoto, ack("session", 7L)).attempt
      status <- readiness.current
    } yield expect(duplicate.left.exists(_.isInstanceOf[DuplicateCurrentGeneration])).and(expect(status.ready.isEmpty))
  }

  test("production starts fenced, opens for both same-session lanes, and pauses again on current lane loss") {
    for {
      productionGate <- ProductionGate.make[IO]
      readiness <- SidecarSubscriptionReadiness.make[IO](productionGate)
      initiallyOpen <- productionGate.isOpen
      rumor <- readiness.begin(Lane.RumorBridge)
      _ <- readiness.acknowledge(rumor, ack("session", 1L))
      nakamoto <- readiness.begin(Lane.NakamotoSync)
      _ <- readiness.acknowledge(nakamoto, ack("session", 2L))
      _ <- awaitCondition(productionGate.isOpen)
      openWithBoth <- productionGate.isOpen
      _ <- readiness.release(rumor)
      _ <- awaitCondition(productionGate.isOpen.map(!_))
      openAfterLoss <- productionGate.isOpen
    } yield expect(!initiallyOpen).and(expect(openWithBoth)).and(expect(!openAfterLoss))
  }

  test("release owns the lifecycle mutex: a blocked pause completes before invalidation and a waiting ack cannot reopen") {
    for {
      harness <- gateHarness
      readiness <- SidecarSubscriptionReadiness.make[IO](harness.gate)
      rumor <- readiness.begin(Lane.RumorBridge)
      _ <- readiness.acknowledge(rumor, ack("session", 1L))
      nakamoto <- readiness.begin(Lane.NakamotoSync)
      _ <- readiness.acknowledge(nakamoto, ack("session", 2L))
      block <- harness.armPause
      releaseFiber <- readiness.release(rumor).start
      _ <- block.entered.get.timeout(1.second)
      whilePauseBlocked <- readiness.current
      ackCompleted <- Ref.of[IO, Boolean](false)
      ackFiber <- readiness
        .acknowledge(rumor, ack("session", 3L))
        .attempt
        .guarantee(ackCompleted.set(true))
        .start
      _ <- IO.sleep(50.millis)
      ackBeforePauseCompletes <- ackCompleted.get
      _ <- block.continue.complete(())
      _ <- releaseFiber.joinWithNever
      ackResult <- ackFiber.joinWithNever
      after <- readiness.current
      gateOpen <- harness.gate.isOpen
    } yield expect(whilePauseBlocked.ready.nonEmpty)
      .and(expect(!ackBeforePauseCompletes))
      .and(expect(ackResult.left.exists(_.isInstanceOf[StaleAttempt])))
      .and(expect(after.ready.isEmpty))
      .and(expect(!gateOpen))
  }

  test("awaitBothAndRun rechecks after waiting for the mutex and retries instead of publishing from a stale await") {
    for {
      harness <- gateHarness
      readiness <- SidecarSubscriptionReadiness.make[IO](harness.gate)
      rumor <- readiness.begin(Lane.RumorBridge)
      _ <- readiness.acknowledge(rumor, ack("session", 1L))
      nakamoto <- readiness.begin(Lane.NakamotoSync)
      _ <- readiness.acknowledge(nakamoto, ack("session", 2L))
      block <- harness.armPause
      replacementFiber <- readiness.begin(Lane.RumorBridge).start
      _ <- block.entered.get.timeout(1.second)
      published <- Ref.of[IO, Int](0)
      readyFiber <- readiness.awaitBothAndRun(_ => published.update(_ + 1)).start
      _ <- IO.sleep(50.millis)
      _ <- block.continue.complete(())
      replacement <- replacementFiber.joinWithNever
      _ <- IO.sleep(50.millis)
      beforeReplacementAck <- published.get
      _ <- readiness.acknowledge(replacement, ack("session", 3L))
      _ <- readyFiber.joinWithNever.timeout(1.second)
      afterReplacementAck <- published.get
    } yield expect.same(0, beforeReplacementAck).and(expect.same(1, afterReplacementAck))
  }

  test("cancellation cannot split pause from lane invalidation inside a serialized transition") {
    for {
      harness <- gateHarness
      readiness <- SidecarSubscriptionReadiness.make[IO](harness.gate)
      rumor <- readiness.begin(Lane.RumorBridge)
      _ <- readiness.acknowledge(rumor, ack("session", 1L))
      nakamoto <- readiness.begin(Lane.NakamotoSync)
      _ <- readiness.acknowledge(nakamoto, ack("session", 2L))
      block <- harness.armPause
      releaseFiber <- readiness.release(rumor).start
      _ <- block.entered.get.timeout(1.second)
      cancelFiber <- releaseFiber.cancel.start
      _ <- block.continue.complete(())
      _ <- cancelFiber.joinWithNever.timeout(1.second)
      status <- readiness.current
      gateOpen <- harness.gate.isOpen
    } yield expect(status.ready.isEmpty).and(expect(!gateOpen))
  }
}
