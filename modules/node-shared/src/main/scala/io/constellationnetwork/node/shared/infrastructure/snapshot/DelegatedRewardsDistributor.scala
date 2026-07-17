package io.constellationnetwork.node.shared.infrastructure.snapshot

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.util.control.NoStackTrace

import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceResult
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{GlobalStateReader, StakeCollateralMptReader}
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.ConsensusTrigger
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.transaction.RewardTransaction
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.syntax.sortedCollection.{sortedMapSyntax, sortedSetSyntax}

import eu.timepit.refined.types.numeric.NonNegLong

case class DelegatedRewardsResult(
  delegatorRewardsMap: SortedMap[PeerId, Map[Address, Amount]],
  updatedCreateDelegatedStakes: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
  updatedWithdrawDelegatedStakes: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
  nodeOperatorRewards: SortedSet[RewardTransaction],
  reservedAddressRewards: SortedSet[RewardTransaction],
  withdrawalRewardTxs: SortedSet[RewardTransaction],
  totalEmittedRewardsAmount: Amount
)

case class PartitionedStakeUpdates(
  unexpiredCreateDelegatedStakes: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
  unexpiredWithdrawalsDelegatedStaking: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
  expiredWithdrawalsDelegatedStaking: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]]
)

trait DelegatedRewardsDistributor[F[_]] {

  def getEmissionConfig(epochProgress: EpochProgress): F[EmissionConfigEntry]

  def calculateVariableInflation(epochProgress: EpochProgress, lastSnapshotContext: GlobalSnapshotInfo): F[Amount]

  def distribute(
    lastSnapshotContext: GlobalSnapshotInfo,
    trigger: ConsensusTrigger,
    epochProgress: EpochProgress,
    facilitators: List[(Address, PeerId)],
    delegatedStakeDiffs: UpdateDelegatedStakeAcceptanceResult,
    partitionedRecords: PartitionedStakeUpdates
  ): F[DelegatedRewardsResult]
}

object DelegatedRewardsDistributor {

  final case class ConflictingAcceptedStakeTransitions(source: Address, stakeRef: Hash, tokenLockRef: Hash)
      extends RuntimeException(
        s"Accepted delegated-stake successor and withdrawal share one backing lock: source=${source.show} " +
          s"stakeRef=${stakeRef.value} tokenLockRef=${tokenLockRef.value}"
      )
      with NoStackTrace

  private def validateAcceptedStakeTransitionExclusivity[F[_]: Async: Hasher](
    delegatedStakeDiffs: UpdateDelegatedStakeAcceptanceResult,
    partitionedRecords: PartitionedStakeUpdates
  ): F[Unit] = {
    val acceptedCreateTokenLocks = delegatedStakeDiffs.acceptedCreates.iterator.flatMap {
      case (source, creates) => creates.iterator.map { case (create, _) => source -> create.tokenLockRef }
    }.toSet

    for {
      existingByReference <- partitionedRecords.unexpiredCreateDelegatedStakes.toList.flatTraverse {
        case (source, records) =>
          records.toList.traverse(record => DelegatedStakeReference.of[F](record.event).map(ref => (source -> ref.hash) -> record))
      }
        .map(_.toMap)
      conflicts = delegatedStakeDiffs.acceptedWithdrawals.iterator.flatMap {
        case (source, withdrawals) =>
          withdrawals.iterator.flatMap {
            case (withdrawal, _) =>
              existingByReference
                .get(source -> withdrawal.stakeRef)
                .filter(record => acceptedCreateTokenLocks(source -> record.tokenLockRef))
                .map(record => ConflictingAcceptedStakeTransitions(source, withdrawal.stakeRef, record.tokenLockRef))
          }
      }.toList
        .sortBy(conflict => (conflict.source.show, conflict.stakeRef.value, conflict.tokenLockRef.value))
      _ <- conflicts.headOption.traverse_(Async[F].raiseError[Unit])
    } yield ()
  }

  /** Identifies which stakes are being modified (have matching tokenLockRef in both existing records and acceptedCreates). Returns a Set of
    * (Address, TokenLockRef) tuples representing the modified stakes.
    */
  def identifyModifiedStakes(
    existingRecords: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    acceptedCreates: SortedMap[Address, List[(Signed[UpdateDelegatedStake.Create], SnapshotOrdinal)]]
  ): Set[(Address, Hash)] =
    acceptedCreates.toSeq.flatMap {
      case (addr, creates) =>
        creates.flatMap {
          case (ev, _) =>
            existingRecords
              .get(addr)
              .filter(_.exists(_.tokenLockRef === ev.tokenLockRef))
              .map(_ => addr -> ev.tokenLockRef)
        }
    }.toSet

  /** Returns a filtered version of existingRecords with modified stakes removed. This is used to exclude modified stakes from reward
    * calculations.
    */
  def filterOutModifiedStakes(
    existingRecords: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    modifiedStakes: Set[(Address, Hash)]
  ): SortedMap[Address, SortedSet[DelegatedStakeRecord]] =
    existingRecords.iterator.flatMap {
      case (address, records) =>
        val filtered = records.filterNot { record =>
          modifiedStakes.contains(
            (address, record.tokenLockRef)
          )
        }

        if (filtered.nonEmpty) Some(address -> filtered)
        else None
    }.toSortedMap

  def getUpdatedCreateDelegatedStakes[F[_]: Async: Hasher](
    delegatorRewardsMap: Map[PeerId, Map[Address, Amount]],
    delegatedStakeDiffs: UpdateDelegatedStakeAcceptanceResult,
    partitionedRecords: PartitionedStakeUpdates
  ): F[SortedMap[Address, SortedSet[DelegatedStakeRecord]]] = {
    val existingRecords = partitionedRecords.unexpiredCreateDelegatedStakes
    val modifiedStakes = identifyModifiedStakes(existingRecords, delegatedStakeDiffs.acceptedCreates)

    for {
      _ <- validateAcceptedStakeTransitionExclusivity(delegatedStakeDiffs, partitionedRecords)
      newRecordsWithRewards <- delegatedStakeDiffs.acceptedCreates.toList.traverse {
        case (addr, stakeList) =>
          val existingRecordsForAddr = existingRecords.getOrElse(addr, List.empty)

          stakeList.traverse {
            case (ev, ord) =>
              val matchingExistingRecord = existingRecordsForAddr.find { record =>
                record.tokenLockRef === ev.tokenLockRef
              }

              DelegatedStakeRecord(
                ev,
                ord,
                matchingExistingRecord.map(_.rewards).getOrElse(Balance.empty),
                matchingExistingRecord.flatMap(_.currentTokenLockRef),
                matchingExistingRecord.flatMap(_.currentAmount)
              ).pure[F]
          }.map(records => addr -> records.toSortedSet)
      }.map(_.toSortedMap)

      filteredExistingRecords = filterOutModifiedStakes(existingRecords, modifiedStakes)

      mergedRecords = filteredExistingRecords |+| newRecordsWithRewards

      activeStakes <- {
        val withdrawnStakes = delegatedStakeDiffs.acceptedWithdrawals.flatMap(_._2.map(_._1.stakeRef)).toSet

        mergedRecords.toList.traverse {
          case (addr, records) =>
            records.toList.traverse { record =>
              for {
                ref <- DelegatedStakeReference.of[F](record.event)
                isWithdrawn = withdrawnStakes.contains(ref.hash)
              } yield (record, isWithdrawn)
            }.map { recordsWithWithdrawnFlag =>
              val keptRecords = recordsWithWithdrawnFlag
                .filterNot(_._2) // Remove records that are withdrawn
                .map(_._1) // Get just the records without the flags

              (addr, keptRecords)
            }
        }.map(_.toMap)
      }.map(_.map {
        case (addr, recs) =>
          addr -> recs.map { record =>
            val isModified = modifiedStakes.contains((addr, record.tokenLockRef))

            val nodeSpecificReward =
              if (isModified) Amount.empty
              else
                delegatorRewardsMap
                  .get(record.event.nodeId)
                  .flatMap(_.get(addr))
                  .getOrElse(Amount.empty)

            // ensure we're not accidentally zeroing out rewards
            val disbursedBalance =
              if (isModified) record.rewards
              else record.rewards.plus(nodeSpecificReward).toOption.getOrElse(Amount.empty)

            DelegatedStakeRecord(record.event, record.createdAt, disbursedBalance, record.currentTokenLockRef, record.currentAmount)
          }.toSortedSet
      }.filterNot(_._2.isEmpty).toSortedMap)
    } yield activeStakes
  }

  /** §G5 — MPT-backed equivalent of the legacy GSI-walked variant.
    *
    * For each accepted withdrawal `(ev, ep)` at address `addr`, find the matching record in `activeDelegatedStakes(addr)` by
    * stake-reference hash. Reads come from `reader.get` against the `ActiveDelegatedStakes` partition keyed by `addr` (per-address point
    * read; same partition the writer populates via `acc.activeDelegatedStakes.iterator.map { case (addr, s) => ... }`).
    *
    * Byte-equivalent to the legacy `lastSnapshotContext.activeDelegatedStakes` path: each per-address `SortedSet[DelegatedStakeRecord]`
    * decodes via `delegatedStakeRecordSetCodec` which is the same codec used by the writer.
    */
  def getUpdatedWithdrawalDelegatedStakes[F[_]: Async: Hasher](
    reader: GlobalStateReader[F],
    delegatedStakeDiffs: UpdateDelegatedStakeAcceptanceResult,
    partitionedRecords: PartitionedStakeUpdates
  ): F[SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]]] =
    validateAcceptedStakeTransitionExclusivity(delegatedStakeDiffs, partitionedRecords) >>
      delegatedStakeDiffs.acceptedWithdrawals.toList.traverse {
        case (addr, acceptedWithdrawls) =>
          acceptedWithdrawls.traverse {
            case (ev, ep) =>
              StakeCollateralMptReader
                .readActiveDelegatedStakes(reader, addr)
                .flatMap { maybeStakes =>
                  maybeStakes.flatTraverse {
                    _.findM { s =>
                      DelegatedStakeReference.of(s.event).map(_.hash === ev.stakeRef)
                    }.map(
                      _.map(rec =>
                        PendingDelegatedStakeWithdrawal(
                          rec.event,
                          rec.rewards,
                          rec.createdAt,
                          ep,
                          rec.currentTokenLockRef,
                          rec.currentAmount
                        )
                      )
                    )
                  }
                }
                .flatMap(Async[F].fromOption(_, new RuntimeException("Unexpected None when processing user delegations")))
          }.map { records =>
            addr -> records.toSortedSet
          }
      }.map(records => SortedMap.from(records))
        .map(partitionedRecords.unexpiredWithdrawalsDelegatedStaking |+| _)
        .map(_.filterNot(_._2.isEmpty))

  def sumMintedAmount[F[_]: Async](
    reservedAddressRewards: SortedSet[RewardTransaction],
    nodeOperatorRewards: SortedSet[RewardTransaction],
    delegatorRewardsMap: Map[PeerId, Map[Address, Amount]]
  ): F[Amount] = {
    val reservedEmittedAmount = reservedAddressRewards.toList.map(_.amount.value.value).sum
    val validatorsEmittedAmount = nodeOperatorRewards.toList.map(_.amount.value.value).sum
    val delegatorsEmittedAmount = delegatorRewardsMap.map(_._2.values.map(_.value.value).sum).sum
    val totalEmitted = reservedEmittedAmount + validatorsEmittedAmount + delegatorsEmittedAmount
    NonNegLong
      .from(totalEmitted)
      .bimap(
        new IllegalArgumentException(_),
        Amount(_)
      )
      .pure[F]
      .flatMap(Async[F].fromEither(_))
  }
}
