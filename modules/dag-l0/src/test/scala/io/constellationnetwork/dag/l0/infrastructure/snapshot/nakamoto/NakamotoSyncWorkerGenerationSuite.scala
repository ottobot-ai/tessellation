package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.Outcome
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoSyncDaemon.{
  GenerationWorkerLane,
  GenerationWorkerOfferResult
}

import weaver.SimpleIOSuite

object NakamotoSyncWorkerGenerationSuite extends SimpleIOSuite {

  private def requireAccepted(result: GenerationWorkerOfferResult): IO[Unit] =
    IO.raiseUnless(result == GenerationWorkerOfferResult.Accepted)(
      new AssertionError(s"expected worker admission, got $result")
    )

  test("a serial lane preserves FIFO receipt order") {
    for {
      observed <- Ref.of[IO, Vector[Int]](Vector.empty)
      completed <- Deferred[IO, Unit]
      lane <- GenerationWorkerLane.bounded[IO, Int](
        capacity = 8,
        maxConcurrent = 1,
        maxQueuedBytes = 8L,
        maxMessageBytes = 1L
      )(_ => 1L)(value => observed.updateAndGet(_ :+ value).flatMap(values => completed.complete(()).void.whenA(values.size == 3)))
      admissions <- List(1, 2, 3).traverse(lane.tryOffer)
      fiber <- lane.run.compile.drain.start
      _ <- completed.get.timeout(5.seconds)
      values <- observed.get
      _ <- fiber.cancel
    } yield expect.all(
      admissions.forall(_ == GenerationWorkerOfferResult.Accepted),
      values == Vector(1, 2, 3)
    )
  }

  test("parallel lane never exceeds declared handler concurrency") {
    for {
      active <- Ref.of[IO, Int](0)
      maximum <- Ref.of[IO, Int](0)
      started <- Ref.of[IO, Int](0)
      twoStarted <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      completed <- Ref.of[IO, Int](0)
      allCompleted <- Deferred[IO, Unit]
      lane <- GenerationWorkerLane.bounded[IO, Int](
        capacity = 8,
        maxConcurrent = 2,
        maxQueuedBytes = 8L,
        maxMessageBytes = 1L
      )(_ => 1L) { _ =>
        active.updateAndGet(_ + 1).flatMap(now => maximum.update(_.max(now))) >>
          started.updateAndGet(_ + 1).flatMap(now => twoStarted.complete(()).void.whenA(now == 2)) >>
          release.get.guarantee(active.update(_ - 1)) >>
          completed.updateAndGet(_ + 1).flatMap(now => allCompleted.complete(()).void.whenA(now == 4))
      }
      admissions <- List.range(0, 4).traverse(lane.tryOffer)
      fiber <- lane.run.compile.drain.start
      _ <- twoStarted.get.timeout(5.seconds)
      observedMaximum <- maximum.get
      _ <- release.complete(())
      _ <- allCompleted.get.timeout(5.seconds)
      _ <- fiber.cancel
    } yield expect.all(
      admissions.forall(_ == GenerationWorkerOfferResult.Accepted),
      observedMaximum == 2
    )
  }

  test("item capacity rejects without blocking the shared demultiplexer") {
    for {
      lane <- GenerationWorkerLane.bounded[IO, Int](
        capacity = 2,
        maxConcurrent = 1,
        maxQueuedBytes = 100L,
        maxMessageBytes = 10L
      )(_ => 1L)(_ => IO.unit)
      first <- lane.tryOffer(1)
      second <- lane.tryOffer(2)
      overflow <- lane.tryOffer(3)
    } yield expect.all(
      first == GenerationWorkerOfferResult.Accepted,
      second == GenerationWorkerOfferResult.Accepted,
      overflow == GenerationWorkerOfferResult.ItemCapacityExceeded(2)
    )
  }

  test("encoded-byte budget includes queued and in-flight messages and releases after completion") {
    for {
      processed <- Deferred[IO, Unit]
      lane <- GenerationWorkerLane.bounded[IO, Long](
        capacity = 100,
        maxConcurrent = 1,
        maxQueuedBytes = 10L,
        maxMessageBytes = 8L
      )(identity)(size => processed.complete(()).void.as(size).void)
      flood <- List.fill(100)(6L).traverse(lane.tryOffer)
      acceptedBeforeDrain = flood.count(_ == GenerationWorkerOfferResult.Accepted)
      byteRejected = flood.count(_ == GenerationWorkerOfferResult.ByteCapacityExceeded(6L, 10L))
      worker <- lane.run.compile.drain.start
      _ <- processed.get.timeout(5.seconds)
      admittedAfterRelease <- lane.tryOffer(6L)
      _ <- worker.cancel
    } yield expect.all(
      acceptedBeforeDrain == 1,
      byteRejected == 99,
      admittedAfterRelease == GenerationWorkerOfferResult.Accepted
    )
  }

  test("one message larger than the lane maximum never reserves queue or byte capacity") {
    for {
      lane <- GenerationWorkerLane.bounded[IO, Long](
        capacity = 1,
        maxConcurrent = 1,
        maxQueuedBytes = 10L,
        maxMessageBytes = 8L
      )(identity)(_ => IO.unit)
      oversized <- lane.tryOffer(9L)
      valid <- lane.tryOffer(8L)
    } yield expect.all(
      oversized == GenerationWorkerOfferResult.MessageTooLarge(9L, 8L),
      valid == GenerationWorkerOfferResult.Accepted
    )
  }

  test("clean source completion seals admission and drains every accepted item") {
    for {
      started <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      observed <- Ref.of[IO, Set[Int]](Set.empty)
      lane <- GenerationWorkerLane.bounded[IO, Int](8, 2, 8L, 1L)(_ => 1L) { value =>
        started.complete(()).void >> release.get >> observed.update(_ + value)
      }
      source = fs2.Stream.eval(List(1, 2, 3).traverse_(lane.tryOffer(_).flatMap(requireAccepted)))
      generation <- NakamotoSyncDaemon.runWorkerGeneration(source, List(lane)).compile.drain.start
      _ <- started.get.timeout(5.seconds)
      completionBeforeRelease <- generation.join.map(_.some).timeoutTo(100.millis, IO.pure(none))
      rejectedAfterSeal <- lane.tryOffer(4)
      _ <- release.complete(())
      outcome <- generation.join.timeout(5.seconds)
      values <- observed.get
    } yield outcome match {
      case Outcome.Succeeded(_) =>
        expect.all(
          completionBeforeRelease.isEmpty,
          rejectedAfterSeal == GenerationWorkerOfferResult.GenerationClosed,
          values == Set(1, 2, 3)
        )
      case other => failure(s"expected successfully drained generation, got $other")
    }
  }

  test("source error is rethrown only after accepted work drains") {
    val sourceError = new IllegalStateException("source failed")

    for {
      started <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      handled <- Ref.of[IO, Boolean](false)
      lane <- GenerationWorkerLane.bounded[IO, Int](1, 1, 1L, 1L)(_ => 1L)(_ =>
        started.complete(()).void >> release.get >> handled.set(true)
      )
      source = fs2.Stream.eval(lane.tryOffer(1).flatMap(requireAccepted)) ++ fs2.Stream.raiseError[IO](sourceError)
      generation <- NakamotoSyncDaemon.runWorkerGeneration(source, List(lane)).compile.drain.start
      _ <- started.get.timeout(5.seconds)
      completionBeforeRelease <- generation.join.map(_.some).timeoutTo(100.millis, IO.pure(none))
      _ <- release.complete(())
      outcome <- generation.join.timeout(5.seconds)
      wasHandled <- handled.get
    } yield outcome match {
      case Outcome.Errored(error) =>
        expect.all(completionBeforeRelease.isEmpty, wasHandled, error eq sourceError)
      case other => failure(s"expected errored generation after drain, got $other")
    }
  }

  test("offer and seal are linearizable and every accepted reservation drains exactly once") {
    val values = List.range(0, 200)

    for {
      observed <- Ref.of[IO, Set[Int]](Set.empty)
      startRace <- Deferred[IO, Unit]
      lane <- GenerationWorkerLane.bounded[IO, Int](values.size, 8, values.size.toLong, 1L)(_ => 1L)(value =>
        observed.update(_ + value)
      )
      worker <- lane.run.compile.drain.start
      offers <- (startRace.get >> values.parTraverse(value => lane.tryOffer(value).tupleLeft(value))).start
      sealing <- (startRace.get >> lane.seal).start
      _ <- startRace.complete(())
      results <- offers.joinWithNever
      _ <- sealing.joinWithNever
      _ <- lane.awaitDrained.timeout(5.seconds)
      processed <- observed.get
      closed <- lane.tryOffer(Int.MaxValue)
      _ <- worker.cancel
      accepted = results.collect { case (value, GenerationWorkerOfferResult.Accepted) => value }.toSet
      dispositionsValid = results.forall {
        case (_, GenerationWorkerOfferResult.Accepted | GenerationWorkerOfferResult.GenerationClosed) => true
        case _                                                                                          => false
      }
    } yield expect.all(
      dispositionsValid,
      processed == accepted,
      closed == GenerationWorkerOfferResult.GenerationClosed
    )
  }

  test("a sealed lane rejects without reserving queue, byte, or outstanding capacity") {
    for {
      observed <- Ref.of[IO, Vector[Int]](Vector.empty)
      lane <- GenerationWorkerLane.bounded[IO, Int](1, 1, 1L, 1L)(_ => 1L)(value => observed.update(_ :+ value))
      accepted <- lane.tryOffer(1)
      _ <- lane.seal
      closed <- List.range(2, 102).traverse(lane.tryOffer)
      worker <- lane.run.compile.drain.start
      _ <- lane.awaitDrained.timeout(5.seconds)
      processed <- observed.get
      _ <- worker.cancel
    } yield expect.all(
      accepted == GenerationWorkerOfferResult.Accepted,
      closed.forall(_ == GenerationWorkerOfferResult.GenerationClosed),
      processed == Vector(1)
    )
  }

  test("a replacement generation cannot start until the prior clean generation drains") {
    for {
      oldActive <- Ref.of[IO, Boolean](false)
      oldStarted <- Deferred[IO, Unit]
      releaseOld <- Deferred[IO, Unit]
      replacementStarted <- Deferred[IO, Boolean]
      oldLane <- GenerationWorkerLane.bounded[IO, Int](1, 1, 1L, 1L)(_ => 1L)(_ =>
        oldActive.set(true) >> oldStarted.complete(()).void >> releaseOld.get.guarantee(oldActive.set(false))
      )
      replacementLane <- GenerationWorkerLane.bounded[IO, Int](1, 1, 1L, 1L)(_ => 1L)(_ =>
        oldActive.get.flatMap(replacementStarted.complete).void
      )
      oldSource = fs2.Stream.eval(oldLane.tryOffer(1).flatMap(requireAccepted))
      replacementSource = fs2.Stream.eval(replacementLane.tryOffer(2).flatMap(requireAccepted))
      generations =
        NakamotoSyncDaemon.runWorkerGeneration(oldSource, List(oldLane)) ++
          NakamotoSyncDaemon.runWorkerGeneration(replacementSource, List(replacementLane))
      fiber <- generations.compile.drain.start
      _ <- oldStarted.get.timeout(5.seconds)
      replacementBeforeRelease <- replacementStarted.tryGet
      _ <- releaseOld.complete(())
      overlapped <- replacementStarted.get.timeout(5.seconds)
      outcome <- fiber.join.timeout(5.seconds)
    } yield outcome match {
      case Outcome.Succeeded(_) => expect.all(replacementBeforeRelease.isEmpty, !overlapped)
      case other                => failure(s"expected both generations to complete, got $other")
    }
  }

  test("canceling a subscription generation cancels old in-flight work before a replacement runs") {
    def source(lane: NakamotoSyncDaemon.GenerationWorkerLane[IO, Int], value: Int): fs2.Stream[IO, Unit] =
      fs2.Stream.eval(lane.tryOffer(value).flatMap(requireAccepted)) ++ fs2.Stream.never[IO]

    for {
      oldStarted <- Deferred[IO, Unit]
      oldCanceled <- Deferred[IO, Unit]
      oldLane <- GenerationWorkerLane.bounded[IO, Int](1, 1, 1L, 1L)(_ => 1L)(_ =>
        (oldStarted.complete(()).void >> IO.never[Unit]).onCancel(oldCanceled.complete(()).void)
      )
      oldGeneration <- NakamotoSyncDaemon.runWorkerGeneration(source(oldLane, 1), List(oldLane)).compile.drain.start
      _ <- oldStarted.get.timeout(5.seconds)
      _ <- oldGeneration.cancel
      _ <- oldCanceled.get.timeout(5.seconds)
      replacementCompleted <- Deferred[IO, Int]
      replacementLane <- GenerationWorkerLane.bounded[IO, Int](1, 1, 1L, 1L)(_ => 1L)(value =>
        replacementCompleted.complete(value).void
      )
      replacementGeneration <- NakamotoSyncDaemon
        .runWorkerGeneration(source(replacementLane, 2), List(replacementLane))
        .compile
        .drain
        .start
      replacementValue <- replacementCompleted.get.timeout(5.seconds)
      _ <- replacementGeneration.cancel
    } yield expect(replacementValue == 2)
  }

  test("an unexpected handler failure terminates the generation and cancels sibling workers") {
    val workerError = new IllegalStateException("worker failed")

    for {
      failStarted <- Deferred[IO, Unit]
      siblingStarted <- Deferred[IO, Unit]
      siblingCanceled <- Deferred[IO, Unit]
      failNow <- Deferred[IO, Unit]
      failingLane <- GenerationWorkerLane.bounded[IO, Int](1, 1, 2L, 1L)(_ => 1L)(_ =>
        failStarted.complete(()).void >> failNow.get >> IO.raiseError[Unit](workerError)
      )
      siblingLane <- GenerationWorkerLane.bounded[IO, Int](1, 1, 2L, 1L)(_ => 1L)(_ =>
        (siblingStarted.complete(()).void >> IO.never[Unit]).onCancel(siblingCanceled.complete(()).void)
      )
      source = fs2.Stream.eval(
        failingLane.tryOffer(1).flatMap(requireAccepted) >> siblingLane.tryOffer(2).flatMap(requireAccepted)
      ) ++ fs2.Stream.never[IO]
      generation <- NakamotoSyncDaemon
        .runWorkerGeneration(source, List(failingLane, siblingLane))
        .compile
        .drain
        .start
      _ <- failStarted.get.timeout(5.seconds)
      _ <- siblingStarted.get.timeout(5.seconds)
      _ <- failNow.complete(())
      outcome <- generation.join.timeout(5.seconds)
      _ <- siblingCanceled.get.timeout(5.seconds)
    } yield outcome match {
      case Outcome.Errored(NakamotoSyncDaemon.GenerationWorkerFailed(error)) => expect(error eq workerError)
      case other => failure(s"expected errored generation, got $other")
    }
  }

  test("a failing final handler cannot be masked by clean-source drain completion") {
    val handlerError = new IllegalStateException("final handler failed")

    for {
      handlerStarted <- Deferred[IO, Unit]
      failHandler <- Deferred[IO, Unit]
      lane <- GenerationWorkerLane.bounded[IO, Int](1, 1, 1L, 1L)(_ => 1L)(_ =>
        handlerStarted.complete(()).void >> failHandler.get >> IO.raiseError[Unit](handlerError)
      )
      source = fs2.Stream.eval(lane.tryOffer(1).flatMap(requireAccepted))
      generation <- NakamotoSyncDaemon.runWorkerGeneration(source, List(lane)).compile.drain.start
      _ <- handlerStarted.get.timeout(5.seconds)
      _ <- lane.tryOffer(2).iterateUntil(_ == GenerationWorkerOfferResult.GenerationClosed).timeout(5.seconds)
      _ <- failHandler.complete(())
      outcome <- generation.join.timeout(5.seconds)
    } yield outcome match {
      case Outcome.Errored(NakamotoSyncDaemon.GenerationWorkerFailed(error)) => expect(error eq handlerError)
      case other => failure(s"expected final handler failure after clean source completion, got $other")
    }
  }
}
