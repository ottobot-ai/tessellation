package io.constellationnetwork.node.shared.domain.nodeCollateral

import cats.Order
import cats.data.NonEmptyChain
import cats.data.Validated.{Invalid, Valid}
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceResult
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralValidator._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralReference, UpdateNodeCollateral}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.syntax.sortedCollection.sortedMapSyntax

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Accepts or rejects node collateral create/withdraw events for inclusion in a global snapshot.
  *
  * '''Determinism''': Creates and withdrawals are sorted by the protocol `Signed` ordering and processed with first-wins acceptance
  * contexts. Only accepted events reserve duplicate keys.
  */
trait UpdateNodeCollateralAcceptanceManager[F[_]] {

  def accept(
    creates: List[Signed[UpdateNodeCollateral.Create]],
    withdrawals: List[Signed[UpdateNodeCollateral.Withdraw]],
    parentStateReader: GlobalStateReader[F],
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

  private case class WithdrawalAcceptanceResult(
    accepted: List[Signed[UpdateNodeCollateral.Withdraw]],
    rejected: List[(Signed[UpdateNodeCollateral.Withdraw], NonEmptyChain[UpdateNodeCollateralValidationError])],
    acceptedReferences: Set[(Address, Hash)]
  )

  private object WithdrawalAcceptanceResult {
    val empty: WithdrawalAcceptanceResult = WithdrawalAcceptanceResult(List.empty, List.empty, Set.empty)
  }

  def make[F[_]: Async: SecurityProvider](validator: UpdateNodeCollateralValidator[F]) =
    new UpdateNodeCollateralAcceptanceManager[F] {
      private val logger = Slf4jLogger.getLoggerFromClass[F](getClass)

      private def processCreateValidation(
        acc: CreateAcceptanceResult,
        signed: Signed[UpdateNodeCollateral.Create],
        validated: UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Create]],
        delegatedStakeTokenLockReferences: Set[Hash]
      ): CreateAcceptanceResult = {
        val duplicatesAcceptedContext =
          acc.acceptedSources(signed.source) ||
            acc.acceptedTokenLockRefs(signed.tokenLockRef) ||
            acc.acceptedParents((signed.source, signed.parent)) ||
            acc.acceptedNodes((signed.source, signed.nodeId))

        validated match {
          case Valid(_) if delegatedStakeTokenLockReferences(signed.tokenLockRef) =>
            acc.copy(
              rejected = (signed, NonEmptyChain.of(DelegatedStakeTokenLockConflict(signed.tokenLockRef))) :: acc.rejected
            )
          case Valid(_) if duplicatesAcceptedContext =>
            acc.copy(
              rejected = (
                signed,
                NonEmptyChain.of(DuplicatedCreate(signed.source, signed.nodeId, signed.tokenLockRef, signed.parent))
              ) :: acc.rejected
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

      private def processWithdrawalValidation(
        acc: WithdrawalAcceptanceResult,
        signed: Signed[UpdateNodeCollateral.Withdraw],
        validated: UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Withdraw]]
      ): WithdrawalAcceptanceResult = {
        val reference = signed.source -> signed.collateralRef

        validated match {
          case Valid(_) if acc.acceptedReferences(reference) =>
            acc.copy(
              rejected = (signed, NonEmptyChain.of(DuplicatedWithdrawal(signed.source, signed.collateralRef))) :: acc.rejected
            )
          case Valid(accepted) =>
            acc.copy(
              accepted = accepted :: acc.accepted,
              acceptedReferences = acc.acceptedReferences + reference
            )
          case Invalid(errors) =>
            acc.copy(rejected = (signed, errors) :: acc.rejected)
        }
      }

      def accept(
        creates: List[Signed[UpdateNodeCollateral.Create]],
        withdrawals: List[Signed[UpdateNodeCollateral.Withdraw]],
        parentStateReader: GlobalStateReader[F],
        lastGlobalEpochProgress: EpochProgress,
        lastSnapshotOrdinal: SnapshotOrdinal,
        updateDelegatedStakeAcceptanceResult: UpdateDelegatedStakeAcceptanceResult
      ): F[UpdateNodeCollateralAcceptanceResult] = {
        // Defensive sort for deterministic partition ordering across all peers.
        val sortedCreates = creates.sorted(Signed.ordering(Order[UpdateNodeCollateral.Create].toOrdering))
        val sortedWithdrawals = withdrawals.sorted(Signed.ordering(Order[UpdateNodeCollateral.Withdraw].toOrdering))
        val delegatedStakeTokenLockReferences = updateDelegatedStakeAcceptanceResult.acceptedCreates.values
          .flatMap(_.map(_._1.tokenLockRef))
          .toSet

        for {
          createResult <- sortedCreates.foldLeftM(CreateAcceptanceResult.empty) { (acc, signed) =>
            validator
              .validateCreateNodeCollateral(signed, parentStateReader)
              .map(processCreateValidation(acc, signed, _, delegatedStakeTokenLockReferences))
          }
          withdrawalResult <- sortedWithdrawals.foldLeftM(WithdrawalAcceptanceResult.empty) { (acc, signed) =>
            validator
              .validateWithdrawNodeCollateral(signed, parentStateReader)
              .map(processWithdrawalValidation(acc, signed, _))
          }
          acceptedCreates = createResult.accepted
          notAcceptedCreates = createResult.rejected

          acceptedCreatesMap <- acceptedCreates
            .map(c => (c, lastSnapshotOrdinal))
            .traverse { case (signed, ord) => signed.proofs.head.id.toAddress.map((_, (signed, ord))) }
            .map(_.groupBy(_._1).view.mapValues(_.map(_._2)).toSortedMap)

          acceptedWithdrawalsMap <- withdrawalResult.accepted
            .map(w => (w, lastGlobalEpochProgress))
            .traverse { case (signed, epoch) => signed.proofs.head.id.toAddress.map((_, (signed, epoch))) }
            .map(_.groupBy(_._1).view.mapValues(_.map(_._2)).toSortedMap)

          _ <- logger.info(
            s"[NODE_COLLATERAL] ordinal=${lastSnapshotOrdinal.show} " +
              s"input: creates=${creates.size} withdrawals=${withdrawals.size} " +
              s"delegStakeTokenLockRefs=${delegatedStakeTokenLockReferences.size} | " +
              s"result: acceptedCreates=${acceptedCreatesMap.values.map(_.size).sum} " +
              s"rejectedCreates=${notAcceptedCreates.size} " +
              s"acceptedWithdrawals=${acceptedWithdrawalsMap.values.map(_.size).sum} " +
              s"rejectedWithdrawals=${withdrawalResult.rejected.size}"
          )
        } yield
          UpdateNodeCollateralAcceptanceResult(
            acceptedCreatesMap,
            notAcceptedCreates,
            acceptedWithdrawalsMap,
            withdrawalResult.rejected
          )
      }
    }
}
