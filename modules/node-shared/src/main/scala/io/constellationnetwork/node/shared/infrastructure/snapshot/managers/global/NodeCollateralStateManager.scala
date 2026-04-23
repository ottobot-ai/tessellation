package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralAcceptanceResult
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.nodeCollateralWithdrawalExpiryKeySetImmutableCodec
import io.constellationnetwork.syntax.sortedCollection.sortedSetSyntax

import eu.timepit.refined.types.numeric.NonNegLong

trait NodeCollateralStateManager[F[_]] {
  def acceptNodeCollaterals(
    lastSnapshotContext: GlobalSnapshotInfo,
    epochProgress: EpochProgress,
    previousEpochProgress: EpochProgress,
    withdrawalTimeLimit: EpochProgress
  )(implicit hasher: Hasher[F]): F[
    (
      SortedMap[Address, SortedSet[NodeCollateralRecord]],
      SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]],
      SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]
    )
  ]

  /** Index-driven equivalent of the expired-withdrawals filter in `acceptNodeCollaterals`.
    *
    * Sweeps the NC-withdrawal expiry index for buckets `[previousEpochProgress + 1 .. epochProgress]` — the range covering withdrawals
    * whose expiry epoch (`createdAt + withdrawalTimeLimit`) fell into the past since the previous accept. The bucket keys already encode
    * the expiry epoch, so `withdrawalTimeLimit` is not passed here — the index was maintained using the same config at write-time, so its
    * contents are authoritative. Resolves each expiring key's hash against the passed-in in-memory map (`lastActiveWithdrawals`) to
    * reconstruct the `PendingNodeCollateralWithdrawal` value.
    *
    * Note the sweep bounds differ from AllowSpend/TokenLock: NC's legacy predicate is `<=` (expiry_epoch <= currentEpoch), so the delta
    * window is `(prevEpoch, curEpoch]` — inclusive upper bound, exclusive lower. The other two are `<` so the window is `[prevEpoch,
    * curEpoch)`.
    *
    * Equivalence is verified by `NodeCollateralExpirySweepEquivalenceSuite`.
    */
  def findExpiredWithdrawalsViaIndex(
    previousEpochProgress: EpochProgress,
    epochProgress: EpochProgress,
    lastActiveWithdrawals: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]
  )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]]

  /** #86 MPT-backed variant — drops the `lastActiveWithdrawals` map input. Resolves each expiring key's hash via
    * `mptStore.getNodeCollateralWithdrawals(addr)` instead of an in-memory lookup. Equivalence is verified by
    * `NodeCollateralExpirySweepEquivalenceSuite`.
    */
  def findExpiredWithdrawalsViaIndexFromMpt(
    previousEpochProgress: EpochProgress,
    epochProgress: EpochProgress
  )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]]

  def getUpdatedCreateNodeCollaterals(
    nodeCollateralAcceptanceResult: UpdateNodeCollateralAcceptanceResult,
    unexpiredCreateNodeCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]]
  )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[NodeCollateralRecord]]]

  def getUpdatedWithdrawNodeCollaterals(
    nodeCollateralAcceptanceResult: UpdateNodeCollateralAcceptanceResult,
    unexpiredWithdrawNodeCollaterals: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]],
    lastSnapshotContext: GlobalSnapshotInfo
  )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]]
}

object NodeCollateralStateManager {

  def make[F[_]: Async](
    mptStore: MptStore[F, GlobalStateKey],
    shouldUseMptStore: Boolean = false,
    // #86: when true, `acceptNodeCollaterals` delegates its expiry discovery to
    // `findExpiredWithdrawalsViaIndexFromMpt` (drops `lastActiveWithdrawals` map dependency inside the sweep).
    // Default false until testnet validation proves the new path end-to-end. Flip in
    // `GlobalSnapshotAcceptanceManager.make` when ready.
    useMptBackedAcceptPath: Boolean = false
  ): NodeCollateralStateManager[F] = new NodeCollateralStateManager[F] {

    def acceptNodeCollaterals(
      lastSnapshotContext: GlobalSnapshotInfo,
      epochProgress: EpochProgress,
      previousEpochProgress: EpochProgress,
      withdrawalTimeLimit: EpochProgress
    )(implicit hasher: Hasher[F]): F[
      (
        SortedMap[Address, SortedSet[NodeCollateralRecord]],
        SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]],
        SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]
      )
    ] = {
      val existingNodeCollaterals =
        lastSnapshotContext.activeNodeCollaterals.getOrElse(SortedMap.empty[Address, SortedSet[NodeCollateralRecord]])
      val existingWithdrawals =
        lastSnapshotContext.nodeCollateralWithdrawals.getOrElse(SortedMap.empty[Address, SortedSet[PendingNodeCollateralWithdrawal]])

      def isWithdrawalExpired(withdrawalEpoch: EpochProgress): Boolean =
        (withdrawalEpoch |+| withdrawalTimeLimit) <= epochProgress

      // Phase 2b: index sweep when flag on (equivalence proven by NodeCollateralExpirySweepEquivalenceSuite).
      // #86: if `useMptBackedAcceptPath` is true, sweep resolution also reads from MPT per-address instead of
      // the in-memory `existingWithdrawals` map. Both branches produce the same (address, hash) set under the
      // phase-2a invariant; the MPT-backed variant is proven equivalent in NodeCollateralExpirySweepEquivalenceSuite.
      val expiredWithdrawalsF: F[SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]] =
        if (useMptBackedAcceptPath)
          findExpiredWithdrawalsViaIndexFromMpt(previousEpochProgress, epochProgress)
        else if (shouldUseMptStore)
          findExpiredWithdrawalsViaIndex(previousEpochProgress, epochProgress, existingWithdrawals)
        else {
          val legacy = existingWithdrawals.map {
            case (address, withdrawals) =>
              address -> withdrawals.filter {
                case PendingNodeCollateralWithdrawal(_, _, withdrawalEpoch) =>
                  isWithdrawalExpired(withdrawalEpoch)
              }
          }.filter { case (_, withdrawalList) => withdrawalList.nonEmpty }
          legacy.pure[F]
        }

      expiredWithdrawalsF.map { expiredWithdrawals =>
        // Compute unexpired as the set-difference: keep originals that aren't in expired.
        val expiredPairs: Set[(Address, PendingNodeCollateralWithdrawal)] =
          expiredWithdrawals.flatMap { case (a, ws) => ws.map(w => (a, w)) }.toSet
        val unexpiredWithdrawals = existingWithdrawals.map {
          case (address, withdrawals) =>
            address -> withdrawals.filterNot(w => expiredPairs.contains((address, w)))
        }.filter { case (_, withdrawalList) => withdrawalList.nonEmpty }
        (existingNodeCollaterals, unexpiredWithdrawals, expiredWithdrawals)
      }
    }

    def findExpiredWithdrawalsViaIndexFromMpt(
      previousEpochProgress: EpochProgress,
      epochProgress: EpochProgress
    )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]] = {
      val fromL = previousEpochProgress.value.value + 1L
      val toL = epochProgress.value.value
      if (fromL > toL)
        SortedMap.empty[Address, SortedSet[PendingNodeCollateralWithdrawal]].pure[F]
      else {
        val epochs: List[EpochProgress] =
          (fromL to toL).toList.map(v => EpochProgress(NonNegLong.unsafeFrom(v)))

        for {
          buckets <- epochs.traverse { e =>
            mptStore
              .getExpiryBucket[NodeCollateralWithdrawalExpiryKey](SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals, e)
              .map(_.getOrElse(SortedSet.empty[NodeCollateralWithdrawalExpiryKey]))
          }
          allKeys = buckets.flatten.toSet
          byAddress = allKeys.groupBy(_.address)
          resolved <- byAddress.toList.traverse {
            case (addr, expiryKeys) =>
              mptStore.getNodeCollateralWithdrawals(addr).flatMap { addrSetOpt =>
                val addrSet = addrSetOpt.getOrElse(SortedSet.empty[PendingNodeCollateralWithdrawal])
                val expectedHashes = expiryKeys.map(_.hash)
                addrSet.toList.traverse(w => w.event.toHashed.map(h => (h.hash, w))).map { hashed =>
                  val matched = hashed.collect { case (h, w) if expectedHashes.contains(h) => w }.to(SortedSet)
                  addr -> matched
                }
              }
          }
        } yield SortedMap.from(resolved).filter(_._2.nonEmpty)
      }
    }

    def findExpiredWithdrawalsViaIndex(
      previousEpochProgress: EpochProgress,
      epochProgress: EpochProgress,
      lastActiveWithdrawals: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]
    )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]] = {
      // NC predicate is `<=` (inclusive), so the sweep window is (prevEpoch, epochProgress] — fromL = prevEpoch + 1, toL = epochProgress.
      val fromL = previousEpochProgress.value.value + 1L
      val toL = epochProgress.value.value
      if (fromL > toL)
        SortedMap.empty[Address, SortedSet[PendingNodeCollateralWithdrawal]].pure[F]
      else {
        val epochs: List[EpochProgress] =
          (fromL to toL).toList.map(v => EpochProgress(NonNegLong.unsafeFrom(v)))

        for {
          buckets <- epochs.traverse { e =>
            mptStore
              .getExpiryBucket[NodeCollateralWithdrawalExpiryKey](SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals, e)
              .map(_.getOrElse(SortedSet.empty[NodeCollateralWithdrawalExpiryKey]))
          }
          allKeys = buckets.flatten.toSet
          byAddress = allKeys.groupBy(_.address)
          resolved <- byAddress.toList.traverse {
            case (addr, expiryKeys) =>
              val addrSet = lastActiveWithdrawals.getOrElse(addr, SortedSet.empty[PendingNodeCollateralWithdrawal])
              val expectedHashes = expiryKeys.map(_.hash)
              addrSet.toList.traverse(w => w.event.toHashed.map(h => (h.hash, w))).map { hashed =>
                val matched = hashed.collect { case (h, w) if expectedHashes.contains(h) => w }.to(SortedSet)
                addr -> matched
              }
          }
        } yield SortedMap.from(resolved).filter(_._2.nonEmpty)
      }
    }

    def getUpdatedCreateNodeCollaterals(
      nodeCollateralAcceptanceResult: UpdateNodeCollateralAcceptanceResult,
      unexpiredCreateNodeCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]]
    )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[NodeCollateralRecord]]] = {

      val acceptedTokenLockRefs = nodeCollateralAcceptanceResult.acceptedCreates.map {
        case (addr, creates) => (addr, creates.map(_._1.tokenLockRef).toSet)
      }
      val filteredUnexpiredCreateNodeCollaterals = unexpiredCreateNodeCollaterals.map {
        case (addr, creates) =>
          val tokenLocks = acceptedTokenLockRefs.getOrElse(addr, Set.empty)
          (addr, creates.filterNot(c => tokenLocks(c.event.tokenLockRef)))
      }
      val acceptedCreates = nodeCollateralAcceptanceResult.acceptedCreates.map {
        case (addr, cs) => addr -> cs.map(c => NodeCollateralRecord(c._1, c._2)).toSortedSet
      }
      val activeCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]] =
        filteredUnexpiredCreateNodeCollaterals |+| acceptedCreates
      // remove withdrawn stakes from the active list
      val withdrawnCollaterals = nodeCollateralAcceptanceResult.acceptedWithdrawals.flatMap(_._2.map(_._1.collateralRef)).toSet
      activeCollaterals.toList.traverse {
        case (addr, records) =>
          records.toList.traverse { record =>
            NodeCollateralReference.of(record.event).map(ref => (record, withdrawnCollaterals(ref.hash)))
          }.map(records => (addr, records.filterNot(_._2).map(_._1).toSortedSet))
      }
        .map(_.filterNot(_._2.isEmpty))
        .map(SortedMap.from(_))
    }

    def getUpdatedWithdrawNodeCollaterals(
      nodeCollateralAcceptanceResult: UpdateNodeCollateralAcceptanceResult,
      unexpiredWithdrawNodeCollaterals: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]],
      lastSnapshotContext: GlobalSnapshotInfo
    )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]] =
      nodeCollateralAcceptanceResult.acceptedWithdrawals.toList.traverse {
        case (addr, acceptedWithdrawls) =>
          acceptedWithdrawls.traverse {
            case (ev, ep) =>
              mptStore
                .getNodeCollaterals(addr)
                .flatMap { maybeCollaterals =>
                  maybeCollaterals.flatTraverse {
                    _.findM { s =>
                      NodeCollateralReference.of(s.event).map(_.hash === ev.collateralRef)
                    }.map(_.map(rec => PendingNodeCollateralWithdrawal(rec.event, rec.createdAt, ep)))
                  }
                }
                .flatMap(Async[F].fromOption(_, new RuntimeException("Unexpected None when processing node collaterals")))
          }.map(pending => addr -> pending.toSortedSet)
      }.map(SortedMap.from(_))
        .map(unexpiredWithdrawNodeCollaterals |+| _)
        .map(_.filterNot(_._2.isEmpty))
  }
}
