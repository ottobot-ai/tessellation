package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.util.concurrent.CancellationException

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all._

import weaver.SimpleIOSuite

object ConsensusInputGateSuite extends SimpleIOSuite {

  test("network effects remain inert until bootstrap releases the gate") {
    for {
      control <- ConsensusInputGate.make[IO]
      entered <- Deferred[IO, Unit]
      completed <- Deferred[IO, Unit]
      runs <- Ref.of[IO, Int](0)
      fiber <- (entered.complete(()).void >> control.readOnly.awaitBootstrap >> runs.update(_ + 1) >> completed.complete(()).void).start
      _ <- entered.get
      before <- completed.tryGet
      runsBefore <- runs.get
      localStateRelease <- control.markLocalStateReady
      seedVisible <- control.chainSeed.awaitLocalState.as(true)
      seedRelease <- control.chainSeed.completeChainSeed(Right(()))
      _ <- control.awaitChainSeed
      firstRelease <- control.releaseInput
      _ <- completed.get
      runsAfter <- runs.get
      secondRelease <- control.releaseInput
      _ <- fiber.joinWithNever
    } yield expect.all(
      before.isEmpty,
      runsBefore == 0,
      localStateRelease,
      seedVisible,
      seedRelease,
      firstRelease,
      runsAfter == 1,
      !secondRelease
    )
  }

  test("one release wakes every await-only consumer") {
    for {
      control <- ConsensusInputGate.make[IO]
      entered <- Ref.of[IO, Int](0)
      completed <- Ref.of[IO, Int](0)
      allEntered <- Deferred[IO, Unit]
      run = entered
        .updateAndGet(_ + 1)
        .flatMap(count => allEntered.complete(()).void.whenA(count == 2)) >>
        control.readOnly.awaitBootstrap >>
        completed.update(_ + 1)
      first <- run.start
      second <- run.start
      _ <- allEntered.get
      before <- completed.get
      _ <- control.markLocalStateReady
      _ <- control.chainSeed.completeChainSeed(Right(()))
      _ <- control.awaitChainSeed
      released <- control.releaseInput
      _ <- first.joinWithNever
      _ <- second.joinWithNever
      after <- completed.get
    } yield expect.all(before == 0, released, after == 2)
  }

  test("chain seed failure propagates to Main and never opens input") {
    val failure = new IllegalStateException("invalid restored head")

    for {
      control <- ConsensusInputGate.make[IO]
      localStateRelease <- control.markLocalStateReady
      _ <- control.chainSeed.awaitLocalState
      seedRelease <- control.chainSeed.completeChainSeed(Left(failure))
      result <- control.awaitChainSeed.attempt
      inputRelease <- control.releaseInput.attempt
    } yield expect.all(
      localStateRelease,
      seedRelease,
      result == Left(failure),
      inputRelease.isLeft
    )
  }

  test("out-of-order chain seed and input release fail closed") {
    for {
      control <- ConsensusInputGate.make[IO]
      seedBeforeState <- control.chainSeed.completeChainSeed(Right(())).attempt
      inputBeforeSeed <- control.releaseInput.attempt
      localStateRelease <- control.markLocalStateReady
      inputBeforeSuccessfulSeed <- control.releaseInput.attempt
    } yield expect.all(
      seedBeforeState.isLeft,
      inputBeforeSeed.isLeft,
      localStateRelease,
      inputBeforeSuccessfulSeed.isLeft
    )
  }

  test("chain seed cancellation wakes Main with failure") {
    val cancelled = new CancellationException("seed cancelled")

    for {
      control <- ConsensusInputGate.make[IO]
      seedFiber <- (control.chainSeed.awaitLocalState >> IO.never[Unit])
        .onCancel(control.chainSeed.completeChainSeed(Left(cancelled)).void)
        .start
      _ <- seedFiber.cancel
      observed <- control.awaitChainSeed.attempt
      inputRelease <- control.releaseInput.attempt
    } yield expect.all(
      observed == Left(cancelled),
      inputRelease.isLeft
    )
  }
}
