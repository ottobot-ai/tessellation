package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.{
  delegatedStakeRecordSetCodec,
  pendingDelegatedStakeWithdrawalSetCodec,
  signedTokenLockSetCodec
}
import io.constellationnetwork.syntax.sortedCollection.sortedMapSyntax

trait DelegatedStakeStateManager[F[_]] {
  def processExistingDelegatedStakes(
    epochProgress: EpochProgress,
    acceptedTokenLocks: List[Signed[TokenLock]],
    withdrawalTimeLimit: EpochProgress
  )(implicit hasher: Hasher[F]): F[PartitionedRecords[SortedSet[DelegatedStakeRecord], SortedSet[PendingDelegatedStakeWithdrawal]]]

  def materializeActiveDelegatedStakeAddressesFromMpt(implicit hasher: Hasher[F]): F[Set[Address]]
  def materializeDelegatedStakeWithdrawalAddressesFromMpt(implicit hasher: Hasher[F]): F[Set[Address]]

  /** §G5 — Full-structure materializer for the `ActiveDelegatedStakes` partition.
    *
    * Returns the same `SortedMap[Address, SortedSet[DelegatedStakeRecord]]` shape that `info.activeDelegatedStakes` carries on the GSI
    * side. Used by reward calculation paths (`GlobalDelegatedRewardsDistributor`, `RewardsInfoCalculator`) that need per-delegator-address
    * + per-record-fee detail. When the MPT and GSI are in sync, the bytes are byte-equivalent (each prefix entry decodes via
    * `delegatedStakeRecordSetCodec` which is the same codec the writer uses, keyed by source address derived from
    * `record.event.value.source`).
    *
    * Empty result is returned via `SortedMap.empty` when the prefix scan returns no entries.
    */
  def materializeActiveDelegatedStakesFromMpt(
    implicit hasher: Hasher[F]
  ): F[SortedMap[Address, SortedSet[DelegatedStakeRecord]]]

  /** §G5 — Full-structure materializer for the `DelegatedStakesWithdrawals` partition.
    *
    * Symmetric sibling of `materializeActiveDelegatedStakesFromMpt`. Returns `SortedMap[Address,
    * SortedSet[PendingDelegatedStakeWithdrawal]]` matching the GSI shape. Used by
    * `DelegatedRewardsDistributor.getUpdatedWithdrawalDelegatedStakes` which needs per-address withdrawal records to resolve `stakeRef` →
    * record on the MPT-primary path.
    */
  def materializeDelegatedStakeWithdrawalsFromMpt(
    implicit hasher: Hasher[F]
  ): F[SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]]]
}

object DelegatedStakeStateManager {

  def make[F[_]: Async](reader: GlobalStateReader[F]): DelegatedStakeStateManager[F] = new DelegatedStakeStateManager[F] {

    override def processExistingDelegatedStakes(
      epochProgress: EpochProgress,
      acceptedTokenLocks: List[Signed[TokenLock]],
      withdrawalTimeLimit: EpochProgress
    )(implicit hasher: Hasher[F]): F[PartitionedRecords[SortedSet[DelegatedStakeRecord], SortedSet[PendingDelegatedStakeWithdrawal]]] = {
      def isWithdrawalExpired(withdrawalEpoch: EpochProgress): Boolean =
        (withdrawalEpoch |+| withdrawalTimeLimit) <= epochProgress

      for {
        // §G5/#11 — read existing active stakes + withdrawals from the MPT (not the GSI
        // `lastSnapshotContext.activeDelegatedStakes` / `.delegatedStakesWithdrawals`). This completes
        // the GSI→MPT migration: the `existing` records returned here feed the active-REMOVAL half
        // (`getUpdatedCreateDelegatedStakes`, via `PartitionedStakeUpdates.unexpiredCreateDelegatedStakes`),
        // so it now reads the SAME MPT source as the sibling pending-withdrawal half
        // (`getUpdatedWithdrawalDelegatedStakes`, which already point-reads `ActiveDelegatedStakes` from
        // the MPT). When GSI≠MPT for an updated stake, the two halves operated on different record-sets,
        // so an accepted withdrawal was never removed from the active set (active stayed at 2).
        existingDelegatedStakes <- materializeActiveDelegatedStakesFromMpt
        existingWithdrawals <- materializeDelegatedStakeWithdrawalsFromMpt

        hashedReplacementTokenLocks <- acceptedTokenLocks.filter(_.replaceTokenLockRef.isDefined).traverse(_.toHashed)
        replacementTokenLocks = hashedReplacementTokenLocks.mapFilter(tl => tl.replaceTokenLockRef.tupleRight(tl)).toMap

        // Build a map of active token locks by reference for checking if token locks are still active.
        // Scoped to the addresses that own withdrawals — the only consumers of `activeTokenLocksByRef`
        // (lines below) iterate `existingWithdrawals` / `expiredWithdrawals` whose keys ARE the
        // staker addresses, and a token lock's source equals the staker. Reading from MPT here
        // (instead of `lastSnapshotContext.activeTokenLocks`) is part of #11 — drop GSI materialization.
        activeTokenLocksByRef <- existingWithdrawals.keySet.toList.flatTraverse { addr =>
          reader.get[SortedSet[Signed[TokenLock]]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, addr)).flatMap {
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
        entries <- reader.getAllForPrefix[SortedSet[DelegatedStakeRecord]](prefix)
      } yield entries.values.toList.mapFilter(s => s.headOption.map(_.event.value.source)).toSet

    def materializeDelegatedStakeWithdrawalAddressesFromMpt(implicit hasher: Hasher[F]): F[Set[Address]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.DelegatedStakesWithdrawals)
        entries <- reader.getAllForPrefix[SortedSet[PendingDelegatedStakeWithdrawal]](prefix)
      } yield entries.values.toList.mapFilter(s => s.headOption.map(_.event.value.source)).toSet

    def materializeActiveDelegatedStakesFromMpt(
      implicit hasher: Hasher[F]
    ): F[SortedMap[Address, SortedSet[DelegatedStakeRecord]]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveDelegatedStakes)
        entries <- reader.getAllForPrefix[SortedSet[DelegatedStakeRecord]](prefix)
      } yield
        SortedMap.from(
          entries.values.toList.mapFilter(set => set.headOption.map(_.event.value.source -> set)).filter(_._2.nonEmpty)
        )

    def materializeDelegatedStakeWithdrawalsFromMpt(
      implicit hasher: Hasher[F]
    ): F[SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.DelegatedStakesWithdrawals)
        entries <- reader.getAllForPrefix[SortedSet[PendingDelegatedStakeWithdrawal]](prefix)
      } yield
        SortedMap.from(
          entries.values.toList.mapFilter(set => set.headOption.map(_.event.value.source -> set)).filter(_._2.nonEmpty)
        )

  }
}
