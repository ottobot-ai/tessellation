package io.constellationnetwork.node.shared.domain.nodeCollateral

import cats.data.NonEmptyChain
import cats.data.Validated.{Invalid, Valid}
import cats.effect.Async
import cats.syntax.all._

import scala.collection.MapView

import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceResult
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralValidator.{
  DuplicatedCreate,
  UpdateNodeCollateralValidationError,
  UpdateNodeCollateralValidationErrorOr
}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake.UpdateDelegatedStake
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralReference, UpdateNodeCollateral}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.syntax.sortedCollection.sortedMapSyntax

import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Accepts or rejects node collateral create/withdraw events for inclusion in a global snapshot.
  *
  * '''Determinism''': Creates are sorted and processed with a first-wins acceptance context. Withdrawals use `traverse` for validation and
  * `foldLeft` for partitioning accepted/rejected results.
  */
trait UpdateNodeCollateralAcceptanceManager[F[_]] {

  def accept(
    creates: List[Signed[UpdateNodeCollateral.Create]],
    withdrawals: List[Signed[UpdateNodeCollateral.Withdraw]],
    lastSnapshotContext: GlobalSnapshotInfo,
    lastGlobalEpochProgress: EpochProgress,
    lastSnapshotOrdinal: SnapshotOrdinal,
    updateDelegatedStakeAcceptanceResult: UpdateDelegatedStakeAcceptanceResult
  ): F[UpdateNodeCollateralAcceptanceResult]

}

object UpdateNodeCollateralAcceptanceManager {

  private case class CreateAcceptanceResult(
    accepted: List[Signed[UpdateNodeCollateral.Create]],
    rejected: List[(Signed[UpdateNodeCollateral.Create], NonEmptyChain[UpdateNodeCollateralValidationError])],
    acceptedSources: Set[Address],
    acceptedTokenLockRefs: Set[Hash],
    acceptedParents: Set[(Address, NodeCollateralReference)],
    acceptedNodes: Set[(Address, PeerId)]
  )

  private object CreateAcceptanceResult {
    val empty: CreateAcceptanceResult = CreateAcceptanceResult(List.empty, List.empty, Set.empty, Set.empty, Set.empty, Set.empty)
  }

  def make[F[_]: Async: SecurityProvider](validator: UpdateNodeCollateralValidator[F]) =
    new UpdateNodeCollateralAcceptanceManager[F] {
      private val logger = Slf4jLogger.getLoggerFromClass[F](getClass)

      private def processCreateValidation(
        acc: CreateAcceptanceResult,
        signed: Signed[UpdateNodeCollateral.Create],
        validated: UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Create]]
      ): CreateAcceptanceResult = {
        val duplicatesAcceptedContext =
          acc.acceptedSources(signed.source) ||
            acc.acceptedTokenLockRefs(signed.tokenLockRef) ||
            acc.acceptedParents((signed.source, signed.parent)) ||
            acc.acceptedNodes((signed.source, signed.nodeId))

        validated match {
          case Valid(_) if duplicatesAcceptedContext =>
            CreateAcceptanceResult(
              acc.accepted,
              (
                signed,
                NonEmptyChain.of(DuplicatedCreate(signed.source, signed.nodeId, signed.tokenLockRef, signed.parent))
              ) :: acc.rejected,
              acc.acceptedSources,
              acc.acceptedTokenLockRefs,
              acc.acceptedParents,
              acc.acceptedNodes
            )
          case Valid(accepted) =>
            CreateAcceptanceResult(
              accepted :: acc.accepted,
              acc.rejected,
              acc.acceptedSources + accepted.source,
              acc.acceptedTokenLockRefs + accepted.tokenLockRef,
              acc.acceptedParents + ((accepted.source, accepted.parent)),
              acc.acceptedNodes + ((accepted.source, accepted.nodeId))
            )
          case Invalid(errors) =>
            CreateAcceptanceResult(
              acc.accepted,
              (signed, errors) :: acc.rejected,
              acc.acceptedSources,
              acc.acceptedTokenLockRefs,
              acc.acceptedParents,
              acc.acceptedNodes
            )
        }
      }

      def accept(
        creates: List[Signed[UpdateNodeCollateral.Create]],
        withdrawals: List[Signed[UpdateNodeCollateral.Withdraw]],
        lastSnapshotContext: GlobalSnapshotInfo,
        lastGlobalEpochProgress: EpochProgress,
        lastSnapshotOrdinal: SnapshotOrdinal,
        updateDelegatedStakeAcceptanceResult: UpdateDelegatedStakeAcceptanceResult
      ): F[UpdateNodeCollateralAcceptanceResult] = {

        def partitionAccepted[A](validated: List[UpdateNodeCollateralValidationErrorOr[A]], signed: List[A]) =
          validated
            .zip(signed)
            .foldLeft(
              (
                List.empty[A],
                List.empty[(A, NonEmptyChain[UpdateNodeCollateralValidationError])]
              )
            ) {
              case ((accepted, notAccepted), (validated, signed)) =>
                validated match {
                  case Valid(a)   => (a :: accepted, notAccepted)
                  case Invalid(e) => (accepted, (signed, e) :: notAccepted)
                }
            }

        // Defensive sort for deterministic partition ordering across all peers.
        val sortedCreates = creates.sortBy(_.show)
        val sortedWithdrawals = withdrawals.sortBy(_.show)
        for {
          createResult <- sortedCreates.foldLeftM(CreateAcceptanceResult.empty) { (acc, signed) =>
            validator.validateCreateNodeCollateral(signed, lastSnapshotContext).map(processCreateValidation(acc, signed, _))
          }
          validatedWithdrawals <- sortedWithdrawals.traverse(signed =>
            validator.validateWithdrawNodeCollateral(signed, lastSnapshotContext)
          )
          acceptedCreates = createResult.accepted
          notAcceptedCreates = createResult.rejected
          (acceptedWithdrawals, notAcceptedWithdrawals) = partitionAccepted(validatedWithdrawals, sortedWithdrawals)

          delegatedStakeTokenLockReferences = updateDelegatedStakeAcceptanceResult.acceptedCreates.values
            .flatMap(_.map(_._1.tokenLockRef))
            .toSet

          acceptedCreatesMap <- acceptedCreates
            .filterNot(c => delegatedStakeTokenLockReferences(c.tokenLockRef))
            .map(c => (c, lastSnapshotOrdinal))
            .traverse { case (signed, ord) => signed.proofs.head.id.toAddress.map((_, (signed, ord))) }
            .map(_.groupBy(_._1).view.mapValues(_.map(_._2)).toSortedMap)

          acceptedWithdrawalsMap <- acceptedWithdrawals
            .map(w => (w, lastGlobalEpochProgress))
            .traverse { case (signed, epoch) => signed.proofs.head.id.toAddress.map((_, (signed, epoch))) }
            .map(_.groupBy(_._1).view.mapValues(_.map(_._2)).toSortedMap)

          filteredByDelegStake = acceptedCreates.size - acceptedCreatesMap.values.map(_.size).sum
          _ <- logger.info(
            s"[NODE_COLLATERAL] ordinal=${lastSnapshotOrdinal.show} " +
              s"input: creates=${creates.size} withdrawals=${withdrawals.size} " +
              s"delegStakeTokenLockRefs=${delegatedStakeTokenLockReferences.size} | " +
              s"result: acceptedCreates=${acceptedCreatesMap.values.map(_.size).sum} " +
              s"rejectedCreates=${notAcceptedCreates.size} filteredByDelegStake=$filteredByDelegStake " +
              s"acceptedWithdrawals=${acceptedWithdrawalsMap.values.map(_.size).sum} " +
              s"rejectedWithdrawals=${notAcceptedWithdrawals.size}"
          )
        } yield
          UpdateNodeCollateralAcceptanceResult(
            acceptedCreatesMap,
            notAcceptedCreates,
            acceptedWithdrawalsMap,
            notAcceptedWithdrawals
          )
      }
    }
}
