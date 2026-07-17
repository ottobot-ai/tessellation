package io.constellationnetwork.node.shared.domain.delegatedStake

import cats.Order
import cats.data.NonEmptyChain
import cats.data.Validated.{Invalid, Valid}
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeValidator._
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{GlobalStateReader, StakeCollateralMptReader}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, DelegatedStakeReference, UpdateDelegatedStake}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, SecurityProvider}
import io.constellationnetwork.syntax.sortedCollection.sortedMapSyntax

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Accepts or rejects delegated stake create/withdraw events for inclusion in a global snapshot.
  *
  * '''Determinism''': Uses `foldLeftM` with first-wins duplicate tracking (`parentRefsSeen`, `tokenLockRefsSeen`, `stakeRefsSeen`). Creates
  * are processed before withdrawals, and each type is internally sorted by the protocol `Signed` ordering. If two creates from the same
  * source share a parent, only the first valid event is accepted. An accepted successor create reserves its source/backing-token-lock
  * against a same-batch withdrawal of the existing stake backed by that lock. Rejected events never reserve a parent, token-lock, or stake
  * reference against a later valid event.
  */
trait UpdateDelegatedStakeAcceptanceManager[F[_]] {

  def accept(
    creates: List[Signed[UpdateDelegatedStake.Create]],
    withdrawals: List[Signed[UpdateDelegatedStake.Withdraw]],
    parentStateReader: GlobalStateReader[F],
    currentGlobalEpochProgress: EpochProgress,
    currentSnapshotOrdinal: SnapshotOrdinal,
    acceptedTokenLocks: List[Signed[TokenLock]]
  )(implicit hasher: Hasher[F]): F[UpdateDelegatedStakeAcceptanceResult]

}

object UpdateDelegatedStakeAcceptanceManager {

  private case class CreateDelegatedStakeAcceptanceResult(
    accepted: List[Signed[UpdateDelegatedStake.Create]],
    rejected: List[(Signed[UpdateDelegatedStake.Create], NonEmptyChain[UpdateDelegatedStakeValidationError])],
    parentRefsSeen: Set[(Address, DelegatedStakeReference)],
    tokenLockRefsSeen: Set[Hash]
  )
  private object CreateDelegatedStakeAcceptanceResult {
    val empty = CreateDelegatedStakeAcceptanceResult(List.empty, List.empty, Set.empty, Set.empty)
  }

  private case class WithdrawDelegatedStakeAcceptanceResult(
    accepted: List[Signed[UpdateDelegatedStake.Withdraw]],
    rejected: List[(Signed[UpdateDelegatedStake.Withdraw], NonEmptyChain[UpdateDelegatedStakeValidationError])],
    stakeRefsSeen: Set[Hash]
  )
  private object WithdrawDelegatedStakeAcceptanceResult {
    val empty = WithdrawDelegatedStakeAcceptanceResult(List.empty, List.empty, Set.empty)
  }

  def make[F[_]: Async: SecurityProvider](validator: UpdateDelegatedStakeValidator[F]) =
    new UpdateDelegatedStakeAcceptanceManager[F] {
      private val logger = Slf4jLogger.getLoggerFromClass[F](getClass)

      private def processCreateValidation(
        acc: CreateDelegatedStakeAcceptanceResult,
        signed: Signed[UpdateDelegatedStake.Create],
        validated: UpdateDelegatedStakeValidationErrorOr[Signed[UpdateDelegatedStake.Create]],
        acceptedTokenLocks: List[Signed[TokenLock]]
      ): CreateDelegatedStakeAcceptanceResult = {
        def reject(error: UpdateDelegatedStakeValidationError) =
          (acc.accepted, (signed, NonEmptyChain.of(error)) :: acc.rejected)

        validated match {
          case Valid(_) if acc.parentRefsSeen((signed.source, signed.parent)) =>
            val (accepted, rejected) = reject(DuplicatedParent(signed.parent))
            acc.copy(accepted = accepted, rejected = rejected)
          case Valid(_) if acc.tokenLockRefsSeen(signed.tokenLockRef) =>
            val (accepted, rejected) = reject(DuplicatedTokenLock(signed.tokenLockRef))
            acc.copy(accepted = accepted, rejected = rejected)
          case Valid(_) if acceptedTokenLocks.exists(_.value.replaceTokenLockRef == signed.tokenLockRef.some) =>
            val (accepted, rejected) = reject(OutdatedTokenLock(signed.tokenLockRef))
            acc.copy(accepted = accepted, rejected = rejected)
          case Valid(a) =>
            acc.copy(
              accepted = a :: acc.accepted,
              parentRefsSeen = acc.parentRefsSeen + ((signed.source, signed.parent)),
              tokenLockRefsSeen = acc.tokenLockRefsSeen + signed.tokenLockRef
            )
          case Invalid(errors) =>
            acc.copy(rejected = (signed, errors) :: acc.rejected)
        }
      }

      private def processWithdrawValidation(
        acc: WithdrawDelegatedStakeAcceptanceResult,
        signed: Signed[UpdateDelegatedStake.Withdraw],
        validated: UpdateDelegatedStakeValidationErrorOr[Signed[UpdateDelegatedStake.Withdraw]],
        hashedExistingDelegatedStakes: Map[(Address, Hash), DelegatedStakeRecord],
        acceptedTokenLocks: List[Signed[TokenLock]],
        acceptedCreateTokenLocks: Set[(Address, Hash)]
      ): WithdrawDelegatedStakeAcceptanceResult = {
        def reject(error: UpdateDelegatedStakeValidationError) =
          (acc.accepted, (signed, NonEmptyChain.of(error)) :: acc.rejected)

        def hasOutdatedTokenLock(maybeRecord: Option[DelegatedStakeRecord]): Boolean =
          acceptedTokenLocks.exists { lock =>
            lock.value.replaceTokenLockRef.isDefined &&
            lock.value.replaceTokenLockRef == maybeRecord.map(_.tokenLockRef)
          }

        val maybeExistingDelegatedStake = hashedExistingDelegatedStakes.get(signed.source -> signed.stakeRef)

        validated match {
          case Valid(_) if maybeExistingDelegatedStake.exists(record => acceptedCreateTokenLocks((signed.source, record.tokenLockRef))) =>
            val (accepted, rejected) = reject(ConflictingStakeTransition(signed.stakeRef))
            acc.copy(accepted = accepted, rejected = rejected)
          case Valid(_) if acc.stakeRefsSeen(signed.stakeRef) =>
            val (accepted, rejected) = reject(DuplicatedStake(signed.stakeRef))
            acc.copy(accepted = accepted, rejected = rejected)
          case Valid(_) if hasOutdatedTokenLock(maybeExistingDelegatedStake) =>
            val (accepted, rejected) = reject(OutdatedTokenLock(maybeExistingDelegatedStake.map(_.tokenLockRef).getOrElse(Hash.empty)))
            acc.copy(accepted = accepted, rejected = rejected)
          case Valid(a) =>
            acc.copy(accepted = a :: acc.accepted, stakeRefsSeen = acc.stakeRefsSeen + signed.stakeRef)
          case Invalid(errors) =>
            acc.copy(rejected = (signed, errors) :: acc.rejected)
        }
      }

      def accept(
        creates: List[Signed[UpdateDelegatedStake.Create]],
        withdrawals: List[Signed[UpdateDelegatedStake.Withdraw]],
        parentStateReader: GlobalStateReader[F],
        currentGlobalEpochProgress: EpochProgress,
        currentSnapshotOrdinal: SnapshotOrdinal,
        acceptedTokenLocks: List[Signed[TokenLock]]
      )(implicit hasher: Hasher[F]): F[UpdateDelegatedStakeAcceptanceResult] =
        for {
          existingDelegatedStakes <- StakeCollateralMptReader.materializeActiveDelegatedStakes(parentStateReader)
          hashedExistingDelegatedStakes <- existingDelegatedStakes.toList.flatTraverse {
            case (source, records) =>
              records.toList.traverse(record => record.event.toHashed.map(hashed => (source -> hashed.hash) -> record))
          }
            .map(_.toMap)

          // Defensive sort: ensure deterministic first-wins duplicate resolution.
          // foldLeftM with parentRefsSeen/tokenLockRefsSeen is order-dependent.
          sortedCreates = creates.sorted(Signed.ordering(Order[UpdateDelegatedStake.Create].toOrdering))
          sortedWithdrawals = withdrawals.sorted(Signed.ordering(Order[UpdateDelegatedStake.Withdraw].toOrdering))

          createResult <- sortedCreates.foldLeftM(CreateDelegatedStakeAcceptanceResult.empty) { (acc, signed) =>
            validator
              .validateCreateDelegatedStake(signed, parentStateReader)
              .map(processCreateValidation(acc, signed, _, acceptedTokenLocks))
          }
          acceptedCreateTokenLocks = createResult.accepted.iterator.map(create => create.source -> create.tokenLockRef).toSet

          withdrawResult <- sortedWithdrawals.foldLeftM(WithdrawDelegatedStakeAcceptanceResult.empty) { (acc, signed) =>
            validator
              .validateWithdrawDelegatedStake(signed, parentStateReader)
              .map { validated =>
                processWithdrawValidation(
                  acc,
                  signed,
                  validated,
                  hashedExistingDelegatedStakes,
                  acceptedTokenLocks,
                  acceptedCreateTokenLocks
                )
              }
          }

          acceptedCreatesMap <- createResult.accepted
            .map(c => (c, currentSnapshotOrdinal))
            .traverse { case (signed, ord) => signed.proofs.head.id.toAddress.map((_, (signed, ord))) }
            .map(_.groupBy(_._1).view.mapValues(_.map(_._2)).toSortedMap)

          acceptedWithdrawalsMap <- withdrawResult.accepted
            .map(w => (w, currentGlobalEpochProgress))
            .traverse { case (signed, epoch) => signed.proofs.head.id.toAddress.map((_, (signed, epoch))) }
            .map(_.groupBy(_._1).view.mapValues(_.map(_._2)).toSortedMap)

          _ <- logger.debug(
            s"[DELEG_STAKE] ordinal=${currentSnapshotOrdinal.show} " +
              s"input: creates=${creates.size} withdrawals=${withdrawals.size} " +
              s"existing=${hashedExistingDelegatedStakes.size} acceptedTokenLocks=${acceptedTokenLocks.size} | " +
              s"result: acceptedCreates=${createResult.accepted.size} rejectedCreates=${createResult.rejected.size} " +
              s"acceptedWithdrawals=${withdrawResult.accepted.size} rejectedWithdrawals=${withdrawResult.rejected.size} " +
              s"duplicateParents=${createResult.parentRefsSeen.size} duplicateTokenLocks=${createResult.tokenLockRefsSeen.size} " +
              s"duplicateStakes=${withdrawResult.stakeRefsSeen.size}" +
              (if (createResult.rejected.nonEmpty)
                 s" rejectReasons=[${createResult.rejected.map(_._2.head.getClass.getSimpleName).distinct.mkString(",")}]"
               else "") +
              (if (withdrawResult.rejected.nonEmpty)
                 s" withdrawRejectReasons=[${withdrawResult.rejected.map(_._2.head.getClass.getSimpleName).distinct.mkString(",")}]"
               else "")
          )
        } yield
          UpdateDelegatedStakeAcceptanceResult(
            acceptedCreatesMap,
            createResult.rejected,
            acceptedWithdrawalsMap,
            withdrawResult.rejected
          )
    }
}
