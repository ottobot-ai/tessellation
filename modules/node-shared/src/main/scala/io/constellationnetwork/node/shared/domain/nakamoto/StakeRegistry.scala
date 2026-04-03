package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.schema.peer.PeerId

/** Read-only view of validator stake for Nakamoto consensus.
  *
  * Used by EligibilityChecker to determine threshold scaling. Phase 3: equal weight (1/N). Future: stake-proportional.
  */
trait StakeRegistry[F[_]] {

  /** Get the relative stake [0,1] for a peer. Returns 0 if peer is not a validator. */
  def relativeStake(peerId: PeerId): F[Double]

  /** Get all active validators and their relative stakes */
  def allStakes: F[Map[PeerId, Double]]

  /** Total number of active validators */
  def validatorCount: F[Int]

  /** Update the validator set (called when new finalized snapshot arrives) */
  def updateValidators(validators: Set[PeerId]): F[Unit]
}

object StakeRegistry {

  /** Equal-weight stake registry. Every active validator gets 1/N.
    */
  def equalWeight[F[_]: Sync]: F[StakeRegistry[F]] =
    Ref.of[F, Set[PeerId]](Set.empty).map { validatorsRef =>
      new StakeRegistry[F] {
        def relativeStake(peerId: PeerId): F[Double] =
          validatorsRef.get.map { validators =>
            if (validators.contains(peerId) && validators.nonEmpty)
              1.0 / validators.size.toDouble
            else 0.0
          }

        def allStakes: F[Map[PeerId, Double]] =
          validatorsRef.get.map { validators =>
            if (validators.isEmpty) Map.empty
            else {
              val stake = 1.0 / validators.size.toDouble
              validators.map(_ -> stake).toMap
            }
          }

        def validatorCount: F[Int] =
          validatorsRef.get.map(_.size)

        def updateValidators(validators: Set[PeerId]): F[Unit] =
          validatorsRef.set(validators)
      }
    }

  /** Future: Stake-weighted registry that reads from GlobalSnapshotInfo. Stub for now — will use activeDelegatedStakes +
    * activeNodeCollaterals.
    */
  // def stakeWeighted[F[_]: Sync](snapshotInfo: GlobalSnapshotInfo): F[StakeRegistry[F]] = ???
}
