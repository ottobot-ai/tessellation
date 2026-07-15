package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.std.Semaphore
import cats.effect.{Deferred, IO, Ref}

import io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate

import weaver.SimpleIOSuite

object RebootstrapOrchestratorConcurrencySuite extends SimpleIOSuite {

  private final case class RecordingGate(
    gate: ProductionGate[IO],
    paused: Deferred[IO, Unit],
    reasons: Ref[IO, Set[String]]
  )

  private def recordingGate(events: Ref[IO, Vector[String]]): IO[RecordingGate] =
    for {
      paused <- Deferred[IO, Unit]
      reasons <- Ref.of[IO, Set[String]](Set.empty)
    } yield {
      val gate = new ProductionGate[IO] {
        def pause(reason: String): IO[Unit] =
          reasons.update(_ + reason) >> events.update(_ :+ s"pause:$reason") >> paused.complete(()).void

        def resume(reason: String): IO[Unit] =
          reasons.update(_ - reason) >> events.update(_ :+ s"resume:$reason")

        def isOpen: IO[Boolean] = reasons.get.map(_.isEmpty)

        def pauseReasons: IO[Set[String]] = reasons.get
      }
      RecordingGate(gate, paused, reasons)
    }

  test("successful reset retains the shared semaphore and enters RecoveryRequired") {
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      gate <- recordingGate(events)
      semaphore <- Semaphore[IO](1L)
      _ <- semaphore.acquire
      resetStarted <- Deferred[IO, Unit]
      resetRelease <- Deferred[IO, Unit]
      reset = events.update(_ :+ "reset-start") >> resetStarted.complete(()).void >> resetRelease.get >>
        events.update(_ :+ "reset-end")
      fiber <- RebootstrapOrchestrator
        .withPausedSnapshotSemaphore(semaphore, gate.gate, IO.unit)(reset)
        .start
      _ <- gate.paused.get
      startedWhilePermitHeld <- resetStarted.tryGet
      reasonsWhileWaiting <- gate.reasons.get
      _ <- semaphore.release
      _ <- resetStarted.get
      _ <- resetRelease.complete(())
      _ <- fiber.joinWithNever
      observed <- events.get
      gateOpen <- gate.gate.isOpen
      reasons <- gate.reasons.get
      permitAvailable <- semaphore.tryAcquire
      _ <- IO.whenA(permitAvailable)(semaphore.release)
    } yield
      expect.all(
        startedWhilePermitHeld.isEmpty,
        reasonsWhileWaiting.contains(RebootstrapOrchestrator.RebootstrapInProgress),
        !reasons.contains(RebootstrapOrchestrator.RebootstrapInProgress),
        reasons.contains(RebootstrapOrchestrator.RecoveryRequired),
        observed == Vector(
          s"pause:${RebootstrapOrchestrator.RebootstrapInProgress}",
          "reset-start",
          "reset-end",
          s"pause:${RebootstrapOrchestrator.RecoveryRequired}",
          s"resume:${RebootstrapOrchestrator.RebootstrapInProgress}"
        ),
        !gateOpen,
        !permitAvailable
      )
  }

  test("cancellation while waiting for the shared semaphore leaves production paused") {
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      gate <- recordingGate(events)
      semaphore <- Semaphore[IO](1L)
      _ <- semaphore.acquire
      resetRan <- Ref.of[IO, Boolean](false)
      acquireCanceled <- Deferred[IO, Unit]
      fiber <- RebootstrapOrchestrator
        .withPausedSnapshotSemaphore(semaphore, gate.gate, acquireCanceled.complete(()).void)(resetRan.set(true))
        .start
      _ <- gate.paused.get
      _ <- fiber.cancel
      _ <- acquireCanceled.get
      outcome <- fiber.join
      didResetRun <- resetRan.get
      reasons <- gate.reasons.get
      observed <- events.get
      _ <- semaphore.release
    } yield
      expect.all(
        outcome.isCanceled,
        !didResetRun,
        reasons.contains(RebootstrapOrchestrator.RebootstrapInProgress),
        observed == Vector(s"pause:${RebootstrapOrchestrator.RebootstrapInProgress}")
      )
  }

  test("cancellation after semaphore acquisition cannot interrupt reset or skip RecoveryRequired") {
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      gate <- recordingGate(events)
      semaphore <- Semaphore[IO](1L)
      resetStarted <- Deferred[IO, Unit]
      resetRelease <- Deferred[IO, Unit]
      cancelReturned <- Deferred[IO, Unit]
      reset = events.update(_ :+ "reset-start") >> resetStarted.complete(()).void >> resetRelease.get >>
        events.update(_ :+ "reset-end")
      fiber <- RebootstrapOrchestrator
        .withPausedSnapshotSemaphore(semaphore, gate.gate, IO.unit)(reset)
        .start
      _ <- resetStarted.get
      cancelFiber <- (fiber.cancel >> cancelReturned.complete(()).void).start
      _ <- IO.cede
      canceledBeforeResetFinished <- cancelReturned.tryGet
      _ <- resetRelease.complete(())
      _ <- cancelFiber.joinWithNever
      _ <- fiber.join
      reasons <- gate.reasons.get
      observed <- events.get
      permitAvailable <- semaphore.tryAcquire
      _ <- IO.whenA(permitAvailable)(semaphore.release)
    } yield
      expect.all(
        canceledBeforeResetFinished.isEmpty,
        !reasons.contains(RebootstrapOrchestrator.RebootstrapInProgress),
        reasons.contains(RebootstrapOrchestrator.RecoveryRequired),
        observed == Vector(
          s"pause:${RebootstrapOrchestrator.RebootstrapInProgress}",
          "reset-start",
          "reset-end",
          s"pause:${RebootstrapOrchestrator.RecoveryRequired}",
          s"resume:${RebootstrapOrchestrator.RebootstrapInProgress}"
        ),
        !permitAvailable
      )
  }

  test("reset failure retains the semaphore and leaves every canonical mutation path paused") {
    val failure = new RuntimeException("injected reset failure")

    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      gate <- recordingGate(events)
      semaphore <- Semaphore[IO](1L)
      result <- RebootstrapOrchestrator
        .withPausedSnapshotSemaphore(semaphore, gate.gate, IO.unit)(IO.raiseError(failure))
        .attempt
      reasons <- gate.reasons.get
      observed <- events.get
      permitAvailable <- semaphore.tryAcquire
      _ <- IO.whenA(permitAvailable)(semaphore.release)
    } yield
      expect.all(
        result == Left(failure),
        reasons.contains(RebootstrapOrchestrator.RebootstrapInProgress),
        !reasons.contains(RebootstrapOrchestrator.RecoveryRequired),
        observed == Vector(s"pause:${RebootstrapOrchestrator.RebootstrapInProgress}"),
        !permitAvailable
      )
  }
}
