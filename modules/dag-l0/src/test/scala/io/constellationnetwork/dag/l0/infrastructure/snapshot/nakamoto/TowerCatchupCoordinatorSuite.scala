package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore._
import io.constellationnetwork.node.shared.domain.nakamoto.nipopow._
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object TowerCatchupCoordinatorSuite extends SimpleIOSuite {
  import TowerCatchupCursor._
  import TowerCatchupProcessError._
  import TowerCatchupProcessOutcome._
  import TowerCatchupRebuildReason._
  import TowerCatchupState._

  private def ord(value: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(value))

  private def hash(branch: Int, ordinal: Long): Hash = {
    val value = (BigInt(branch) * BigInt(1000000L) + BigInt(ordinal) + 1L).toString(16)
    Hash(value.reverse.padTo(64, '0').reverse)
  }

  private def position(branch: Int, ordinal: Long): ExactWalkPosition =
    ExactWalkPosition(hash(branch, ordinal), ord(ordinal))

  private def committed(
    outcome: TowerCatchupProcessOutcome,
    observe: IO[Unit] = IO.unit
  ): Either[TowerCatchupProcessError, CommittedTowerCatchup[IO]] =
    Right(CommittedTowerCatchup(outcome, observe))

  private def rejected(error: TowerCatchupProcessError): Either[TowerCatchupProcessError, CommittedTowerCatchup[IO]] =
    Left(error)

  private def path(branch: Int, from: Long, through: Long): Vector[ExactWalkLink] =
    (from to through by -1L).toVector.map { ordinal =>
      ExactWalkLink(position(branch, ordinal), if (ordinal == 0L) Hash.empty else hash(branch, ordinal - 1L))
    }

  private val completeWalker: (ExactWalkPosition, SnapshotOrdinal, Int) => IO[Either[ExactWalkError, ExactWalkResult]] =
    (start, target, maxSteps) => {
      val branch = if (start.hash == hash(1, start.ordinal.value.value)) 1 else 2
      val required = start.ordinal.value.value - target.value.value + 1L
      if (required > maxSteps.toLong)
        IO.pure(Left(ExactWalkError.RequiredStepsExceedLimit(BigInt(required), maxSteps)))
      else IO.pure(Right(ExactWalkResult.Complete(path(branch, start.ordinal.value.value, target.value.value))))
    }

  private def recordingProcessor(
    processed: Ref[IO, Vector[Long]],
    failAt: Ref[IO, Option[Long]]
  ): TowerCatchupProcessor[IO] = new TowerCatchupProcessor[IO] {
    def prepare(link: ExactWalkLink): IO[Either[TowerCatchupProcessError, PreparedTowerCatchup[IO]]] = {
      val commit = {
        val ordinal = link.position.ordinal.value.value
        failAt.get.flatMap {
          case Some(value) if value == ordinal => IO.pure(rejected(FinalizerFailed(link.position, "injected")))
          case _                               => processed.update(_ :+ ordinal).as(committed(ProcessedWithCertificate(link.position)))
        }
      }
      IO.pure(Right(PreparedTowerCatchup(link.position, commit)))
    }
  }

  private def cursorOrdinal(state: TowerCatchupState): Long = state.cursor match {
    case BeforeFirst         => 0L
    case Processed(position) => position.ordinal.value.value
  }

  test("bounded catch-up processes a 250-entry suffix oldest-first without omission") {
    for {
      processed <- Ref.of[IO, Vector[Long]](Vector.empty)
      failAt <- Ref.of[IO, Option[Long]](none)
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, recordingProcessor(processed, failAt))
      target = TowerCatchupTarget(position(1, 250L), ord(250L))
      first <- coordinator.advance(target)
      readyAfterFirst <- coordinator.isReady
      second <- coordinator.advance(target)
      third <- coordinator.advance(target)
      readyAfterThird <- coordinator.isReady
      observed <- processed.get
    } yield
      expect(first.outcomes.size == 100)
        .and(expect(cursorOrdinal(first.state) == 100L))
        .and(expect(!readyAfterFirst))
        .and(expect(second.outcomes.size == 100))
        .and(expect(cursorOrdinal(second.state) == 200L))
        .and(expect(third.outcomes.size == 50))
        .and(expect(third.state.isInstanceOf[Ready]))
        .and(expect(readyAfterThird))
        .and(expect(observed == (1L to 250L).toVector))
  }

  test("first processing failure stops the chunk and retry resumes at the exact failed position") {
    for {
      processed <- Ref.of[IO, Vector[Long]](Vector.empty)
      failAt <- Ref.of[IO, Option[Long]](37L.some)
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](
        completeWalker,
        recordingProcessor(processed, failAt),
        maxPerRun = 100
      )
      target = TowerCatchupTarget(position(1, 50L), ord(50L))
      failed <- coordinator.advance(target)
      afterFailure <- processed.get
      _ <- failAt.set(none)
      retried <- coordinator.advance(target)
      afterRetry <- processed.get
    } yield
      expect(failed.state.isInstanceOf[Waiting])
        .and(expect(cursorOrdinal(failed.state) == 36L))
        .and(expect(afterFailure == (1L to 36L).toVector))
        .and(expect(retried.state.isInstanceOf[Ready]))
        .and(expect(afterRetry == (1L to 50L).toVector))
  }

  test("cancellation leaves the cursor before the interrupted item and a later call retries it") {
    for {
      entered <- Deferred[IO, Unit]
      block <- Ref.of[IO, Boolean](true)
      processed <- Ref.of[IO, Vector[Long]](Vector.empty)
      processor = new TowerCatchupProcessor[IO] {
        def prepare(link: ExactWalkLink): IO[Either[TowerCatchupProcessError, PreparedTowerCatchup[IO]]] =
          block.get.flatMap {
            case true => entered.complete(()).void >> IO.never
            case false =>
              IO.pure(
                Right(
                  PreparedTowerCatchup(
                    link.position,
                    processed
                      .update(_ :+ link.position.ordinal.value.value)
                      .as(committed(ProcessedWithCertificate(link.position)))
                  )
                )
              )
          }
      }
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, processor)
      target = TowerCatchupTarget(position(1, 3L), ord(3L))
      fiber <- coordinator.advance(target).start
      _ <- entered.get
      _ <- fiber.cancel
      interrupted <- coordinator.state
      _ <- block.set(false)
      retried <- coordinator.advance(target)
      observed <- processed.get
    } yield
      expect(cursorOrdinal(interrupted) == 0L)
        .and(expect(retried.state.isInstanceOf[Ready]))
        .and(expect(observed == Vector(1L, 2L, 3L)))
  }

  test("cancellation during a prepared commit publishes its exact cursor before the request terminates") {
    for {
      enteredCommit <- Deferred[IO, Unit]
      releaseCommit <- Deferred[IO, Unit]
      commits <- Ref.of[IO, Vector[ExactWalkPosition]](Vector.empty)
      processor = new TowerCatchupProcessor[IO] {
        def prepare(link: ExactWalkLink): IO[Either[TowerCatchupProcessError, PreparedTowerCatchup[IO]]] =
          IO.pure(
            Right(
              PreparedTowerCatchup(
                link.position,
                commits.update(_ :+ link.position) >>
                  enteredCommit.complete(()).void >>
                  releaseCommit.get.as(committed(ProcessedWithCertificate(link.position)))
              )
            )
          )
      }
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, processor)
      target = TowerCatchupTarget(position(1, 1L), ord(1L))
      advanceFiber <- coordinator.advance(target).start
      _ <- enteredCommit.get
      cancelFiber <- advanceFiber.cancel.start
      _ <- IO.sleep(20.millis)
      whileMasked <- coordinator.state
      _ <- releaseCommit.complete(())
      _ <- cancelFiber.joinWithNever
      canceled <- advanceFiber.join
      afterCancellation <- coordinator.state
      retried <- coordinator.advance(target)
      observedCommits <- commits.get
      wasCanceled = canceled match {
        case cats.effect.Outcome.Canceled() => true
        case _                              => false
      }
    } yield
      expect(whileMasked.isInstanceOf[CatchingUp])
        .and(expect(wasCanceled))
        .and(expect(cursorOrdinal(afterCancellation) == 1L))
        .and(expect(retried.state.isInstanceOf[Ready]))
        .and(expect(observedCommits == Vector(position(1, 1L))))
  }

  test("observation failure cannot roll back the exact cursor or repeat the committed mutation") {
    val observationFailure = new RuntimeException("injected observation failure")

    for {
      commits <- Ref.of[IO, Int](0)
      processor = new TowerCatchupProcessor[IO] {
        def prepare(link: ExactWalkLink): IO[Either[TowerCatchupProcessError, PreparedTowerCatchup[IO]]] =
          IO.pure(
            Right(
              PreparedTowerCatchup(
                link.position,
                commits
                  .update(_ + 1)
                  .as(committed(ProcessedWithCertificate(link.position), IO.raiseError(observationFailure)))
              )
            )
          )
      }
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, processor)
      target = TowerCatchupTarget(position(1, 1L), ord(1L))
      first <- coordinator.advance(target)
      second <- coordinator.advance(target)
      observedCommits <- commits.get
    } yield
      expect(first.state.isInstanceOf[Ready])
        .and(expect(second.state.isInstanceOf[Ready]))
        .and(expect(cursorOrdinal(second.state) == 1L))
        .and(expect(observedCommits == 1))
  }

  test("observation remains cancelable after the exact cursor is published") {
    for {
      observationStarted <- Deferred[IO, Unit]
      commits <- Ref.of[IO, Int](0)
      processor = new TowerCatchupProcessor[IO] {
        def prepare(link: ExactWalkLink): IO[Either[TowerCatchupProcessError, PreparedTowerCatchup[IO]]] =
          IO.pure(
            Right(
              PreparedTowerCatchup(
                link.position,
                commits
                  .update(_ + 1)
                  .as(
                    committed(
                      ProcessedWithCertificate(link.position),
                      observationStarted.complete(()).void >> IO.never
                    )
                  )
              )
            )
          )
      }
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, processor)
      target = TowerCatchupTarget(position(1, 1L), ord(1L))
      first <- coordinator.advance(target).start
      _ <- observationStarted.get
      _ <- first.cancel
      canceled <- first.join
      afterCancellation <- coordinator.state
      retried <- coordinator.advance(target)
      observedCommits <- commits.get
      wasCanceled = canceled match {
        case cats.effect.Outcome.Canceled() => true
        case _                              => false
      }
    } yield
      expect(wasCanceled)
        .and(expect(cursorOrdinal(afterCancellation) == 1L))
        .and(expect(retried.state.isInstanceOf[Ready]))
        .and(expect(observedCommits == 1))
  }

  test("missing exact ancestry waits without processing a disconnected suffix and retries after restoration") {
    for {
      missing <- Ref.of[IO, Option[Long]](37L.some)
      processed <- Ref.of[IO, Vector[Long]](Vector.empty)
      failAt <- Ref.of[IO, Option[Long]](none)
      walker = (start: ExactWalkPosition, target: SnapshotOrdinal, maxSteps: Int) =>
        missing.get.flatMap {
          case Some(value) if value >= target.value.value && value <= start.ordinal.value.value =>
            IO.pure(
              Right(
                ExactWalkResult.Incomplete(
                  path(1, start.ordinal.value.value, value + 1L),
                  position(1, value),
                  ExactWalkIncompleteReason.NotFound
                )
              )
            )
          case _ => completeWalker(start, target, maxSteps)
        }
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](walker, recordingProcessor(processed, failAt))
      target = TowerCatchupTarget(position(1, 50L), ord(50L))
      waiting <- coordinator.advance(target)
      beforeRestore <- processed.get
      _ <- missing.set(none)
      ready <- coordinator.advance(target)
      afterRestore <- processed.get
    } yield
      expect(waiting.state.isInstanceOf[Waiting])
        .and(expect(waiting.state.cursor == BeforeFirst))
        .and(expect(beforeRestore.isEmpty))
        .and(expect(ready.state.isInstanceOf[Ready]))
        .and(expect(afterRestore == (1L to 50L).toVector))
  }

  test("concurrent advances are serialized and preserve one oldest-first stream") {
    for {
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      active <- Ref.of[IO, Int](0)
      maxActive <- Ref.of[IO, Int](0)
      processed <- Ref.of[IO, Vector[Long]](Vector.empty)
      processor = new TowerCatchupProcessor[IO] {
        def prepare(link: ExactWalkLink): IO[Either[TowerCatchupProcessError, PreparedTowerCatchup[IO]]] = {
          val ordinal = link.position.ordinal.value.value
          val pause = if (ordinal == 1L) entered.complete(()).void >> release.get else IO.unit
          val commit =
            active.updateAndGet(_ + 1).flatMap(current => maxActive.update(_.max(current))) >>
              pause >>
              IO.sleep(2.millis) >>
              processed.update(_ :+ ordinal) >>
              active.update(_ - 1) >>
              IO.pure(committed(ProcessedWithCertificate(link.position)))
          IO.pure(Right(PreparedTowerCatchup(link.position, commit)))
        }
      }
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, processor)
      firstFiber <- coordinator.advance(TowerCatchupTarget(position(1, 5L), ord(5L))).start
      _ <- entered.get
      secondFiber <- coordinator.advance(TowerCatchupTarget(position(1, 10L), ord(10L))).start
      _ <- IO.sleep(10.millis)
      _ <- release.complete(())
      first <- firstFiber.joinWithNever
      second <- secondFiber.joinWithNever
      observed <- processed.get
      observedMax <- maxActive.get
    } yield
      expect(first.state.isInstanceOf[Ready])
        .and(expect(second.state.isInstanceOf[Ready]))
        .and(expect(observed == (1L to 10L).toVector))
        .and(expect(observedMax == 1))
  }

  test("no-certificate input has a typed skip outcome and still advances the exact cursor") {
    val processor = new TowerCatchupProcessor[IO] {
      def prepare(link: ExactWalkLink): IO[Either[TowerCatchupProcessError, PreparedTowerCatchup[IO]]] =
        IO.pure(
          Right(
            PreparedTowerCatchup(
              link.position,
              IO.pure(
                committed(
                  if (link.position.ordinal.value.value == 2L) SkippedNoCertificate(link.position)
                  else ProcessedWithCertificate(link.position)
                )
              )
            )
          )
        )
    }

    for {
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, processor)
      result <- coordinator.advance(TowerCatchupTarget(position(1, 3L), ord(3L)))
    } yield
      expect(result.state.isInstanceOf[Ready])
        .and(expect(cursorOrdinal(result.state) == 3L))
        .and(expect(result.outcomes.collect { case skipped: SkippedNoCertificate => skipped.position.ordinal.value.value } == Vector(2L)))
  }

  test("same-ordinal branch replacement is rebuild-required and disables readiness") {
    for {
      processed <- Ref.of[IO, Vector[Long]](Vector.empty)
      failAt <- Ref.of[IO, Option[Long]](none)
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, recordingProcessor(processed, failAt))
      initial <- coordinator.advance(TowerCatchupTarget(position(1, 5L), ord(5L)))
      replacement <- coordinator.advance(TowerCatchupTarget(position(2, 5L), ord(5L)))
      ready <- coordinator.isReady
      observed <- processed.get
    } yield
      expect(initial.state.isInstanceOf[Ready])
        .and(expect(replacement.state.isInstanceOf[RebuildRequired]))
        .and(
          expect(
            replacement.state match {
              case RebuildRequired(_, _, _: CursorNotOnTargetBranch) => true
              case _                                                 => false
            }
          )
        )
        .and(expect(!ready))
        .and(expect(observed == (1L to 5L).toVector))
  }

  test("target behind the exact cursor is rebuild-required without mutating later tower work") {
    for {
      processed <- Ref.of[IO, Vector[Long]](Vector.empty)
      failAt <- Ref.of[IO, Option[Long]](none)
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, recordingProcessor(processed, failAt))
      _ <- coordinator.advance(TowerCatchupTarget(position(1, 5L), ord(5L)))
      behind <- coordinator.advance(TowerCatchupTarget(position(1, 5L), ord(4L)))
      observed <- processed.get
    } yield
      expect(
        behind.state match {
          case RebuildRequired(_, _, _: TargetBehindCursor) => true
          case _                                            => false
        }
      ).and(expect(observed == (1L to 5L).toVector))
  }

  test("non-retryable processor mismatch enters absorbing recovery") {
    for {
      calls <- Ref.of[IO, Int](0)
      proofReads <- Ref.of[IO, Int](0)
      processor = new TowerCatchupProcessor[IO] {
        def prepare(link: ExactWalkLink): IO[Either[TowerCatchupProcessError, PreparedTowerCatchup[IO]]] =
          IO.pure(
            Right(
              PreparedTowerCatchup(
                link.position,
                calls.update(_ + 1).as(rejected(ContentHashMismatch(link.position, hash(2, link.position.ordinal.value.value))))
              )
            )
          )
      }
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, processor)
      target = TowerCatchupTarget(position(1, 3L), ord(3L))
      first <- coordinator.advance(target)
      second <- coordinator.advance(target)
      proofRead <- coordinator.whenReady(proofReads.update(_ + 1))
      observedCalls <- calls.get
      observedProofReads <- proofReads.get
    } yield
      expect(first.state.isInstanceOf[RecoveryRequired])
        .and(expect(second.state == first.state))
        .and(expect(observedCalls == 1))
        .and(expect(proofRead == Left(NipopowProofUnavailable.RecoveryRequired)))
        .and(expect(observedProofReads == 0))
  }

  test("a complete exact walk that omits the requested lower boundary is rejected before processing") {
    val truncatedWalker: (ExactWalkPosition, SnapshotOrdinal, Int) => IO[Either[ExactWalkError, ExactWalkResult]] =
      (start, _, _) => IO.pure(Right(ExactWalkResult.Complete(path(1, start.ordinal.value.value, 5L))))

    for {
      processed <- Ref.of[IO, Vector[Long]](Vector.empty)
      failAt <- Ref.of[IO, Option[Long]](none)
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](
        truncatedWalker,
        recordingProcessor(processed, failAt)
      )
      result <- coordinator.advance(TowerCatchupTarget(position(1, 10L), ord(10L)))
      observed <- processed.get
    } yield expect(result.state.isInstanceOf[RecoveryRequired]).and(expect(observed.isEmpty))
  }

  test("provider withdrawal closes the internal gate before a retained provider can read") {
    for {
      mutations <- Ref.of[IO, Vector[Long]](Vector.empty)
      failAt <- Ref.of[IO, Option[Long]](none)
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, recordingProcessor(mutations, failAt))
      _ <- coordinator.advance(TowerCatchupTarget(position(1, 1L), ord(1L)))
      reads <- Ref.of[IO, Int](0)
      _ <- coordinator.withdrawProofReads
      retainedRead <- coordinator.whenReady(reads.update(_ + 1).as("proof"))
      observedReads <- reads.get
    } yield
      expect(retainedRead == Left(NipopowProofUnavailable.CatchupNotReady))
        .and(expect(observedReads == 0))
  }

  test("an in-flight proof build and catch-up mutation are linearized by the same semaphore") {
    for {
      mutations <- Ref.of[IO, Vector[Long]](Vector.empty)
      failAt <- Ref.of[IO, Option[Long]](none)
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, recordingProcessor(mutations, failAt))
      _ <- coordinator.advance(TowerCatchupTarget(position(1, 1L), ord(1L)))
      _ <- mutations.set(Vector.empty)
      proofStarted <- Deferred[IO, Unit]
      releaseProof <- Deferred[IO, Unit]
      builder = new TowerProofBuilder[IO] {
        def build(since: SnapshotOrdinal, k: Int): IO[TowerProof] =
          proofStarted.complete(()).void >> releaseProof.get.as(TowerProof.Empty)
      }
      verifier = new TowerVerifier[IO] {
        def verify(
          proof: TowerProof,
          genesisEta: Array[Byte],
          etaRotationSnapshots: Long,
          lddConfig: LddConfig
        ): IO[Either[ProofError, Unit]] = IO.pure(Right(()))
      }
      provider = NipopowProofProvider.make[IO](
        builder,
        verifier,
        coordinator,
        genesisEta = Array.emptyByteArray,
        etaRotationSnapshots = 1L,
        lddConfig = LddConfig(1, 0, Ratio.Zero, Ratio.Zero)
      )
      proofFiber <- provider.build(ord(0L), 1).start
      _ <- proofStarted.get
      advanceFiber <- coordinator.advance(TowerCatchupTarget(position(1, 2L), ord(2L))).start
      _ <- IO.sleep(20.millis)
      whileProofHeld <- mutations.get
      _ <- releaseProof.complete(())
      proofResult <- proofFiber.joinWithNever
      advanceResult <- advanceFiber.joinWithNever
      afterRelease <- mutations.get
    } yield
      expect(whileProofHeld.isEmpty)
        .and(expect(proofResult == Right(TowerProof.Empty)))
        .and(expect(advanceResult.state.isInstanceOf[Ready]))
        .and(expect(afterRelease == Vector(2L)))
  }

  test("non-Ready states return typed proof unavailability without starting the read") {
    for {
      reads <- Ref.of[IO, Int](0)
      processed <- Ref.of[IO, Vector[Long]](Vector.empty)
      failAt <- Ref.of[IO, Option[Long]](none)
      coordinator <- TowerCatchupCoordinator.makeWithProcessor[IO](completeWalker, recordingProcessor(processed, failAt))
      idle <- coordinator.whenReady(reads.update(_ + 1).as("proof"))
      _ <- coordinator.advance(TowerCatchupTarget(position(1, 1L), ord(1L)))
      _ <- coordinator.advance(TowerCatchupTarget(position(2, 1L), ord(1L)))
      rebuild <- coordinator.whenReady(reads.update(_ + 1).as("proof"))
      observedReads <- reads.get
    } yield
      expect(idle == Left(NipopowProofUnavailable.CatchupNotReady))
        .and(expect(rebuild == Left(NipopowProofUnavailable.RebuildRequired)))
        .and(expect(observedReads == 0))
  }
}
