package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.Monad
import cats.data.EitherT

import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef

/** Short serialized coordinator operations required by the dark lease-acquisition flow.
  *
  * No production implementation exists in this slice. A future implementation must capture and reacquire under the finality coordinator's
  * serialization boundary without holding that boundary while local artifacts are read and authenticated.
  */
private[finality] trait FinalityLeaseCoordinatorBoundary[F[_]] {
  def capture(
    scope: Phase2UseScope,
    target: GlobalSnapshotStateRef
  ): F[Either[FinalityConsumerLeaseFailure, Phase2Acquisition]]

  def acquireIfCurrent(
    verified: VerifiedPhase2Acquisition
  ): F[Either[FinalityConsumerLeaseFailure, CanonicalPhase2Lease]]
}

/** Source of independently authenticated and reproduced exact local readbacks.
  *
  * `Authenticated` is a caller obligation, not authority conferred by this interface. This dark slice deliberately provides no production
  * implementation or issuer. Returning values copied from an acquisition descriptor, or unauthenticated identity-equal values, is invalid.
  */
private[finality] trait AuthenticatedPhase2ReadbackSource[F[_]] {
  def read(
    acquisition: Phase2Acquisition
  ): F[Either[FinalityConsumerLeaseFailure, Phase2LocalReadbacks]]
}

/** Effectful owner of the capture/readback/verify/reacquire sequence.
  *
  * Readback and authentication run between two short coordinator operations. Raised errors and cancellation are deliberately propagated;
  * neither can be converted into a lease or a successful identity comparison.
  */
private[finality] final class CanonicalPhase2LeaseAcquirer[F[_]: Monad] private[finality] (
  boundary: FinalityLeaseCoordinatorBoundary[F],
  readbacks: AuthenticatedPhase2ReadbackSource[F]
) {
  def acquire(
    scope: Phase2UseScope,
    target: GlobalSnapshotStateRef
  ): F[Either[FinalityConsumerLeaseFailure, CanonicalPhase2Lease]] =
    (for {
      acquisition <- EitherT(boundary.capture(scope, target))
      observed <- EitherT(readbacks.read(acquisition))
      verified <- EitherT.fromEither[F](FinalityConsumerLeaseKernel.verifyReadbacks(acquisition, observed))
      lease <- EitherT(boundary.acquireIfCurrent(verified))
    } yield lease).value
}
