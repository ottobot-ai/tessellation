package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.peer.PeerId

/** Read-only view of validator stake for Nakamoto consensus.
  *
  * Used by EligibilityChecker to determine threshold scaling. Phase 3: equal weight (1/N). Future: stake-proportional.
  *
  * Stakes are returned as exact `Ratio` so eligibility and finality threshold computations are byte-identical across all JVMs/CPUs (no IEEE
  * 754 sum-order or rounding non-determinism).
  *
  * Supports optimistic finality: tracks observed active peers (those that have attested recently) and computes finality weight against the
  * active set rather than the full seedlist. This allows the cluster to finalize when some seedlist peers are offline, without blocking the
  * online majority.
  */
trait StakeRegistry[F[_]] {

  /** Get the relative stake [0,1] for a peer. Returns 0 if peer is not a validator. */
  def relativeStake(peerId: PeerId): F[Ratio]

  /** Get all active validators and their relative stakes */
  def allStakes: F[Map[PeerId, Ratio]]

  /** Total number of validators in the full seedlist */
  def validatorCount: F[Int]

  /** Number of observed active validators (have attested recently) */
  def observedActiveCount: F[Int]

  /** Get the set of active validator PeerIds (full seedlist) */
  def activeValidators: F[Set[PeerId]]

  /** Get the set of observed active PeerIds */
  def observedActive: F[Set[PeerId]]

  /** Update the full validator set from seedlist (called on startup) */
  def updateValidators(validators: Set[PeerId]): F[Unit]

  /** Mark a peer as observed active (called when attestation received) */
  def markActive(peerId: PeerId): F[Unit]

  /** Mark a peer as inactive (called on timeout/disconnect) */
  def markInactive(peerId: PeerId): F[Unit]

  /** Get relative stake computed against observed active peers only (for optimistic finality). Returns 0 if peer is not active.
    */
  def optimisticRelativeStake(peerId: PeerId): F[Ratio]
}

object StakeRegistry {

  /** Minimum fraction of seedlist that must be observed-active before optimistic finality kicks in. Below this, fall back to full-seedlist
    * weight (depth-based finality only). Prevents 2/2 online nodes finalizing in a 100-node network.
    *
    * Default: 1/2. Override via `NAKAMOTO_OPTIMISTIC_MIN_FRACTION` (parsed Double, locked into Ratio at boot). Lower for small clusters;
    * raise for stricter participation requirements.
    */
  val MinActiveQuorumFraction: Ratio =
    sys.env
      .get("NAKAMOTO_OPTIMISTIC_MIN_FRACTION")
      .flatMap(_.toDoubleOption)
      .map(Ratio(_, 18))
      .getOrElse(Ratio(1, 2))

  /** Equal-weight stake registry with optimistic active tracking. Every validator in seedlist gets 1/N for VRF eligibility. Finality weight
    * computed against observed active peers.
    */
  def equalWeight[F[_]: Sync]: F[StakeRegistry[F]] =
    (Ref.of[F, Set[PeerId]](Set.empty), Ref.of[F, Set[PeerId]](Set.empty)).mapN { (validatorsRef, activeRef) =>
      new StakeRegistry[F] {
        def relativeStake(peerId: PeerId): F[Ratio] =
          validatorsRef.get.map { validators =>
            if (validators.contains(peerId) && validators.nonEmpty)
              Ratio(1, validators.size)
            else Ratio.Zero
          }

        def allStakes: F[Map[PeerId, Ratio]] =
          validatorsRef.get.map { validators =>
            if (validators.isEmpty) Map.empty
            else {
              val stake = Ratio(1, validators.size)
              validators.map(_ -> stake).toMap
            }
          }

        def validatorCount: F[Int] =
          validatorsRef.get.map(_.size)

        def observedActiveCount: F[Int] =
          activeRef.get.map(_.size)

        def activeValidators: F[Set[PeerId]] =
          validatorsRef.get

        def observedActive: F[Set[PeerId]] =
          activeRef.get

        def updateValidators(validators: Set[PeerId]): F[Unit] =
          validatorsRef.set(validators)

        def markActive(peerId: PeerId): F[Unit] =
          validatorsRef.get.flatMap { validators =>
            // Only track peers that are in the seedlist
            activeRef.update(_ + peerId).whenA(validators.contains(peerId))
          }

        def markInactive(peerId: PeerId): F[Unit] =
          activeRef.update(_ - peerId)

        def optimisticRelativeStake(peerId: PeerId): F[Ratio] =
          (validatorsRef.get, activeRef.get).mapN { (validators, active) =>
            // Only use optimistic weight if we have enough active peers
            val effectiveActive = active.intersect(validators)
            val meetsQuorum = validators.nonEmpty &&
              Ratio(effectiveActive.size, validators.size) >= MinActiveQuorumFraction

            if (meetsQuorum && effectiveActive.contains(peerId))
              Ratio(1, effectiveActive.size)
            else if (validators.contains(peerId) && validators.nonEmpty)
              Ratio(1, validators.size) // fallback to full seedlist weight
            else Ratio.Zero
          }
      }
    }

  // Future: Stake-weighted registry that reads from GlobalSnapshotInfo. Stub for now — will use activeDelegatedStakes +
  // activeNodeCollaterals.
  // def stakeWeighted[F[_]: Sync](snapshotInfo: GlobalSnapshotInfo): F[StakeRegistry[F]] = ???
}
