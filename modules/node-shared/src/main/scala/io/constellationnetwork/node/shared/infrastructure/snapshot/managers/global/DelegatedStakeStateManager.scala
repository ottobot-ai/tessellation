package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.{
  delegatedStakeRecordSetCodec,
  pendingDelegatedStakeWithdrawalSetCodec
}
import io.constellationnetwork.syntax.sortedCollection.sortedMapSyntax

trait DelegatedStakeStateManager[F[_]] {
  def processExistingDelegatedStakes(
    lastSnapshotContext: GlobalSnapshotInfo,
    epochProgress: EpochProgress,
    acceptedTokenLocks: List[Signed[TokenLock]],
    withdrawalTimeLimit: EpochProgress
  )(implicit hasher: Hasher[F]): F[PartitionedRecords[SortedSet[DelegatedStakeRecord], SortedSet[PendingDelegatedStakeWithdrawal]]]

  def materializeActiveDelegatedStakeAddressesFromMpt(implicit hasher: Hasher[F]): F[Set[Address]]
  def materializeDelegatedStakeWithdrawalAddressesFromMpt(implicit hasher: Hasher[F]): F[Set[Address]]
}

object DelegatedStakeStateManager {

  def make[F[_]: Async](mptStore: MptStore[F, GlobalStateKey]): DelegatedStakeStateManager[F] = new DelegatedStakeStateManager[F] {

    override def processExistingDelegatedStakes(
      lastSnapshotContext: GlobalSnapshotInfo,
      epochProgress: EpochProgress,
      acceptedTokenLocks: List[Signed[TokenLock]],
      withdrawalTimeLimit: EpochProgress
    )(implicit hasher: Hasher[F]): F[PartitionedRecords[SortedSet[DelegatedStakeRecord], SortedSet[PendingDelegatedStakeWithdrawal]]] = {
      def isWithdrawalExpired(withdrawalEpoch: EpochProgress): Boolean =
        (withdrawalEpoch |+| withdrawalTimeLimit) <= epochProgress

      val existingDelegatedStakes = lastSnapshotContext.activeDelegatedStakes.getOrElse(
        SortedMap.empty[Address, SortedSet[DelegatedStakeRecord]]
      )

      val existingWithdrawals = lastSnapshotContext.delegatedStakesWithdrawals.getOrElse(
        SortedMap.empty[Address, SortedSet[PendingDelegatedStakeWithdrawal]]
      )

      for {
        hashedReplacementTokenLocks <- acceptedTokenLocks.filter(_.replaceTokenLockRef.isDefined).traverse(_.toHashed)
        replacementTokenLocks = hashedReplacementTokenLocks.mapFilter(tl => tl.replaceTokenLockRef.tupleRight(tl)).toMap

        // Build a map of active token locks by reference for checking if token locks are still active.
        // Scoped to the addresses that own withdrawals — the only consumers of `activeTokenLocksByRef`
        // (lines below) iterate `existingWithdrawals` / `expiredWithdrawals` whose keys ARE the
        // staker addresses, and a token lock's source equals the staker. Reading from MPT here
        // (instead of `lastSnapshotContext.activeTokenLocks`) is part of #11 — drop GSI materialization.
        activeTokenLocksByRef <- existingWithdrawals.keySet.toList.flatTraverse { addr =>
          mptStore.getActiveTokenLocks(addr).flatMap {
            case Some(locks) => locks.toList.traverse(_.toHashed)
            case None        => List.empty[Hashed[TokenLock]].pure[F]
          }
        }.map(_.map(hashed => hashed.hash -> hashed).toMap)

        updatedExistingDelegatedStakes = existingDelegatedStakes.view
          .mapValues(_.map { record =>
            replacementTokenLocks.get(record.tokenLockRef).fold(record) { hashedTokenLock =>
              record.copy(
                currentTokenLockRef = hashedTokenLock.hash.some,
                currentAmount = DelegatedStakeAmount.fromTokenLockAmount(hashedTokenLock.amount).some
              )
            }
          })
          .toSortedMap

        updatedExistingWithdrawals = existingWithdrawals.view
          .mapValues(_.map { record =>
            replacementTokenLocks
              .get(record.tokenLockRef)
              .filter(_ => activeTokenLocksByRef.contains(record.tokenLockRef))
              .fold(record) { hashedTokenLock =>
                record.copy(
                  currentTokenLockRef = hashedTokenLock.hash.some,
                  currentAmount = DelegatedStakeAmount.fromTokenLockAmount(hashedTokenLock.amount).some
                )
              }
          })
          .toSortedMap

        unexpiredWithdrawals = updatedExistingWithdrawals.map {
          case (address, withdrawals) =>
            address -> withdrawals.filterNot {
              case PendingDelegatedStakeWithdrawal(_, _, _, withdrawalEpoch, _, _) =>
                isWithdrawalExpired(withdrawalEpoch)
            }
        }.filter { case (_, withdrawalList) => withdrawalList.nonEmpty }

        expiredWithdrawals = updatedExistingWithdrawals.map {
          case (address, withdrawals) =>
            address -> withdrawals.filter {
              case PendingDelegatedStakeWithdrawal(_, _, _, withdrawalEpoch, _, _) =>
                isWithdrawalExpired(withdrawalEpoch)
            }
        }.filter { case (_, withdrawalList) => withdrawalList.nonEmpty }

        // Keep expired withdrawals in pending state if their token lock is no longer active
        finalUnexpiredWithdrawals = unexpiredWithdrawals |+| expiredWithdrawals.map {
          case (address, withdrawals) =>
            address -> withdrawals.filter { withdrawal =>
              !activeTokenLocksByRef.contains(withdrawal.tokenLockRef)
            }
        }.filter { case (_, withdrawalList) => withdrawalList.nonEmpty }

        finalExpiredWithdrawals = expiredWithdrawals.map {
          case (address, withdrawals) =>
            address -> withdrawals.filter { withdrawal =>
              activeTokenLocksByRef.contains(withdrawal.tokenLockRef)
            }
        }.filter { case (_, withdrawalList) => withdrawalList.nonEmpty }

      } yield
        PartitionedRecords(
          updatedExistingDelegatedStakes,
          finalUnexpiredWithdrawals,
          finalExpiredWithdrawals
        )
    }

    def materializeActiveDelegatedStakeAddressesFromMpt(implicit hasher: Hasher[F]): F[Set[Address]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveDelegatedStakes)
        entries <- mptStore.getAllForPrefix[SortedSet[DelegatedStakeRecord]](prefix)
      } yield entries.values.toList.mapFilter(s => s.headOption.map(_.event.value.source)).toSet

    def materializeDelegatedStakeWithdrawalAddressesFromMpt(implicit hasher: Hasher[F]): F[Set[Address]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.DelegatedStakesWithdrawals)
        entries <- mptStore.getAllForPrefix[SortedSet[PendingDelegatedStakeWithdrawal]](prefix)
      } yield entries.values.toList.mapFilter(s => s.headOption.map(_.event.value.source)).toSet

  }
}
