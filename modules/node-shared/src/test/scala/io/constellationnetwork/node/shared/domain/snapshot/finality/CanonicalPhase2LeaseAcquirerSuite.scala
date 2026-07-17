package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.effect.std.Semaphore
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityConsumerLeaseFailure._
import io.constellationnetwork.node.shared.domain.snapshot.finality.Phase2UseScope.CurrencySnapshotReplay
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object CanonicalPhase2LeaseAcquirerSuite extends SimpleIOSuite {
  private val released = FinalityCodecFixtures.releasedCore
  private val target = released.pointer.target
  private val prior = FinalityCodecFixtures.priorState
  private val scope = CurrencySnapshotReplay(Address.fromBytes("lease-acquirer-mg".getBytes("UTF-8")), hash(700))

  private def hash(seed: Int): Hash = FinalityCodecFixtures.hash(seed)
  private def revision(value: Long): CanonicalBranchRevision = CanonicalBranchRevision(NonNegLong.unsafeFrom(value))
  private def lineage(value: Long): CanonicalLineageRevision = CanonicalLineageRevision(NonNegLong.unsafeFrom(value))

  private def selection(
    exactTarget: GlobalSnapshotStateRef,
    path: Vector[GlobalSnapshotStateRef],
    branchRevision: Long
  ): CanonicalSelectionToken = {
    val tip = path.last
    val commitment = PathCommitment(
      PathSummary(PathRole.CanonicalLineage, exactTarget, tip, NonNegLong.unsafeFrom(path.size.toLong)),
      FinalityCodecFixtures.pathManifestArtifact,
      FinalityIdentity.pathEntriesRoot(path).fold(throw _, identity)
    )

    CanonicalSelectionToken(
      revision(branchRevision),
      tip,
      exactTarget,
      ForkChoiceDecision(FinalityCodecFixtures.selectionEvidenceArtifact),
      commitment
    )
  }

  private def state(
    branchRevision: Long = 1L,
    lineageRevision: Long = 0L,
    canonical: Vector[GlobalSnapshotStateRef] = Vector(prior, target),
    releases: Map[GlobalSnapshotStateRef, ReleasedCore] = Map(target -> released)
  ): FinalityConsumerLeaseState =
    FinalityConsumerLeaseState(
      CoordinatorMode.Running,
      revision(branchRevision),
      lineage(lineageRevision),
      canonical,
      releases.keysIterator.map { exactTarget =>
        val targetIndex = canonical.indexOf(exactTarget)
        exactTarget -> selection(exactTarget, canonical.drop(targetIndex), branchRevision)
      }.toMap,
      releases,
      Phase2ConsumerSink.empty
    )

  private def exactReadbacks(value: ReleasedCore, selected: CanonicalSelectionToken): Phase2LocalReadbacks = {
    val qualification = value.payload.qualification
    val receipt = value.payload.receipt

    Phase2LocalReadbacks(
      released = Some(value),
      qualification = Some(qualification.scope),
      qualificationEvidence = Some(qualification.evidence.artifact),
      decisionEvidence = Some(selected.decision.evidence),
      lineage = Some(selected.lineage),
      publication = Some(receipt.activePublication),
      semanticReceipt = Some(receipt.semanticReceipt),
      authenticatedAnchorReceipt = Some(receipt.authenticatedAnchorReceipt)
    )
  }

  private final class TestBoundary private (
    current: Ref[IO, FinalityConsumerLeaseState],
    mutex: Semaphore[IO]
  ) extends FinalityLeaseCoordinatorBoundary[IO] {
    def capture(
      useScope: Phase2UseScope,
      exactTarget: GlobalSnapshotStateRef
    ): IO[Either[FinalityConsumerLeaseFailure, Phase2Acquisition]] =
      mutex.permit.use(_ => current.get.map(FinalityConsumerLeaseKernel.capture(_, useScope, exactTarget)))

    def acquireIfCurrent(
      verified: VerifiedPhase2Acquisition
    ): IO[Either[FinalityConsumerLeaseFailure, CanonicalPhase2Lease]] =
      mutex.permit.use(_ => current.get.map(FinalityConsumerLeaseKernel.acquireIfCurrent(_, verified)))

    def replace(next: FinalityConsumerLeaseState): IO[Unit] =
      mutex.permit.use(_ => current.set(next))
  }

  private object TestBoundary {
    def create(initial: FinalityConsumerLeaseState): IO[TestBoundary] =
      (Ref.of[IO, FinalityConsumerLeaseState](initial), Semaphore[IO](1L)).mapN(new TestBoundary(_, _))
  }

  private def readbackSource(
    runReadback: Phase2Acquisition => IO[Either[FinalityConsumerLeaseFailure, Phase2LocalReadbacks]]
  ): AuthenticatedPhase2ReadbackSource[IO] =
    new AuthenticatedPhase2ReadbackSource[IO] {
      def read(
        acquisition: Phase2Acquisition
      ): IO[Either[FinalityConsumerLeaseFailure, Phase2LocalReadbacks]] = runReadback(acquisition)
    }

  // Unsafe test fixture: these identity-equal values exercise orchestration/CAS only. They do not prove independent authentication.
  private def identityEqualFixtureReadbacks(base: FinalityConsumerLeaseState): AuthenticatedPhase2ReadbackSource[IO] =
    readbackSource(_ =>
      IO.pure(
        Right(
          exactReadbacks(
            base.releasedByTarget(target),
            base.currentSelectionByTarget(target)
          )
        )
      )
    )

  test("capture, identity-equal fixture readback, verification, and reacquisition mint one exact lease") {
    val base = state()

    for {
      boundary <- TestBoundary.create(base)
      result <- new CanonicalPhase2LeaseAcquirer[IO](boundary, identityEqualFixtureReadbacks(base)).acquire(scope, target)
    } yield
      expect(result.exists(_.scope == scope)) &&
        expect(result.exists(_.target == target))
  }

  test("same-ordinal replacement during unlocked readback invalidates the acquisition by lineage generation") {
    val base = state()
    val oldReadbacks = exactReadbacks(base.releasedByTarget(target), base.currentSelectionByTarget(target))
    val replacement = target.copy(hash = hash(710), mptRoot = target.mptRoot.copy(value = hash(711)))
    val replaced = state(
      branchRevision = 2L,
      lineageRevision = 1L,
      canonical = Vector(prior, replacement),
      releases = Map.empty
    )

    for {
      boundary <- TestBoundary.create(base)
      entered <- Deferred[IO, Unit]
      continue <- Deferred[IO, Unit]
      source = readbackSource(_ => entered.complete(()).void >> continue.get.as(Right(oldReadbacks)))
      fiber <- new CanonicalPhase2LeaseAcquirer[IO](boundary, source).acquire(scope, target).start
      _ <- entered.get.timeout(1.second)
      _ <- boundary.replace(replaced).timeout(1.second)
      _ <- continue.complete(())
      result <- fiber.joinWithNever.timeout(1.second)
    } yield expect(result == Left(LineageReplaced(lineage(0L), lineage(1L))))
  }

  test("recovery entered during unlocked readback prevents reacquisition") {
    val base = state()
    val oldReadbacks = exactReadbacks(base.releasedByTarget(target), base.currentSelectionByTarget(target))
    val recovery = CoordinatorMode.RecoveryRequired(FinalityCodecFixtures.recoveryPointer)

    for {
      boundary <- TestBoundary.create(base)
      entered <- Deferred[IO, Unit]
      continue <- Deferred[IO, Unit]
      source = readbackSource(_ => entered.complete(()).void >> continue.get.as(Right(oldReadbacks)))
      fiber <- new CanonicalPhase2LeaseAcquirer[IO](boundary, source).acquire(scope, target).start
      _ <- entered.get.timeout(1.second)
      _ <- boundary.replace(base.copy(mode = recovery)).timeout(1.second)
      _ <- continue.complete(())
      result <- fiber.joinWithNever.timeout(1.second)
    } yield expect(result == Left(FinalityConsumerLeaseFailure.RecoveryRequired(recovery)))
  }

  test("a valid descendant extension during unlocked readback makes the captured acquisition stale") {
    val base = state()
    val oldReadbacks = exactReadbacks(base.releasedByTarget(target), base.currentSelectionByTarget(target))
    val descendant = FinalityCodecFixtures.state(42L, 720, 41).copy(parentHash = target.hash)
    val extension = state(branchRevision = 2L, canonical = Vector(prior, target, descendant))

    for {
      boundary <- TestBoundary.create(base)
      entered <- Deferred[IO, Unit]
      continue <- Deferred[IO, Unit]
      source = readbackSource(_ => entered.complete(()).void >> continue.get.as(Right(oldReadbacks)))
      fiber <- new CanonicalPhase2LeaseAcquirer[IO](boundary, source).acquire(scope, target).start
      _ <- entered.get.timeout(1.second)
      _ <- boundary.replace(extension).timeout(1.second)
      _ <- continue.complete(())
      result <- fiber.joinWithNever.timeout(1.second)
    } yield
      expect(
        result == Left(
          AcquisitionStale(
            revision(1L),
            revision(2L),
            Phase2ConsumerSink.empty.revision,
            Phase2ConsumerSink.empty.revision
          )
        )
      )
  }

  test("a raised readback error propagates and does not prevent a later acquisition") {
    val base = state()
    val failure = new RuntimeException("authenticated readback failed")

    for {
      boundary <- TestBoundary.create(base)
      failing = readbackSource(_ => IO.raiseError(failure))
      failed <- new CanonicalPhase2LeaseAcquirer[IO](boundary, failing).acquire(scope, target).attempt
      recovered <-
        new CanonicalPhase2LeaseAcquirer[IO](boundary, identityEqualFixtureReadbacks(base)).acquire(scope, target).timeout(1.second)
    } yield expect(failed == Left(failure)) && expect(recovered.isRight)
  }

  test("cancelling an unlocked readback does not retain the coordinator boundary") {
    val base = state()

    for {
      boundary <- TestBoundary.create(base)
      entered <- Deferred[IO, Unit]
      never <- Deferred[IO, Unit]
      blocked = readbackSource(_ => entered.complete(()).void >> never.get.as(Right(exactReadbacks(released, base.currentSelectionByTarget(target)))))
      fiber <- new CanonicalPhase2LeaseAcquirer[IO](boundary, blocked).acquire(scope, target).start
      _ <- entered.get.timeout(1.second)
      _ <- fiber.cancel.timeout(1.second)
      acquired <-
        new CanonicalPhase2LeaseAcquirer[IO](boundary, identityEqualFixtureReadbacks(base)).acquire(scope, target).timeout(1.second)
    } yield expect(acquired.isRight)
  }
}
