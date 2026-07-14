package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralAcceptanceResult
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.{
  nodeCollateralRecordSetCodec,
  nodeCollateralWithdrawalExpiryKeySetImmutableCodec,
  pendingNodeCollateralWithdrawalSetCodec
}
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

  /** Index-driven sweep of expired NC withdrawals.
    *
    * Sweeps the NC-withdrawal expiry index for buckets `[previousEpochProgress + 1 .. epochProgress]` — the range covering withdrawals
    * whose expiry epoch (`createdAt + withdrawalTimeLimit`) fell into the past since the previous accept. Each bucket epoch is checked
    * against that target-derived expiry before the withdrawal can expire; the rooted index is not independently authoritative. Resolves
    * each expiring key's hash via `mptStore.getNodeCollateralWithdrawals(addr)` instead of iterating an in-memory full map.
    *
    * Note the sweep bounds differ from AllowSpend/TokenLock: NC's legacy predicate is `<=` (expiry_epoch <= currentEpoch), so the delta
    * window is `(prevEpoch, curEpoch]` — inclusive upper bound, exclusive lower. The other two are `<` so the window is `[prevEpoch,
    * curEpoch)`.
    */
  def findExpiredWithdrawalsViaIndexFromMpt(
    previousEpochProgress: EpochProgress,
    epochProgress: EpochProgress,
    withdrawalTimeLimit: EpochProgress
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

  def materializeActiveNodeCollateralAddressesFromMpt(implicit hasher: Hasher[F]): F[Set[Address]]
  def materializeNodeCollateralWithdrawalAddressesFromMpt(implicit hasher: Hasher[F]): F[Set[Address]]

  def materializeNodeCollateralWithdrawalsFromMpt(
    implicit hasher: Hasher[F]
  ): F[SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]]

  /** §G5 — Full-structure materializer for the `ActiveNodeCollaterals` partition.
    *
    * Returns the same `SortedMap[Address, SortedSet[NodeCollateralRecord]]` shape that `info.activeNodeCollaterals` carries on the GSI
    * side. Used by reward calculation paths that need per-record detail. Byte-equivalent to the GSI map when MPT/GSI are in sync: each
    * prefix entry decodes via `nodeCollateralRecordSetCodec` (same codec the writer uses) and is keyed by the source address from
    * `record.event.value.source`.
    *
    * Empty result is returned via `SortedMap.empty` when the prefix scan returns no entries.
    */
  def materializeActiveNodeCollateralsFromMpt(
    implicit hasher: Hasher[F]
  ): F[SortedMap[Address, SortedSet[NodeCollateralRecord]]]
}

object NodeCollateralStateManager {

  def make[F[_]: Async](
    reader: GlobalStateReader[F]
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

      findExpiredWithdrawalsViaIndexFromMpt(previousEpochProgress, epochProgress, withdrawalTimeLimit).map { expiredWithdrawals =>
        // NB: `SortedMap.flatMap { case (a, ws) => ws.map(w => (a, w)) }` would build a `Map` keyed by `a`,
        // dropping all but one `(a, w)` per address. Use `.iterator.flatMap` so the result preserves every pair.
        val expiredPairs: Set[(Address, PendingNodeCollateralWithdrawal)] =
          expiredWithdrawals.iterator.flatMap { case (a, ws) => ws.iterator.map(w => (a, w)) }.toSet
        val unexpiredWithdrawals = existingWithdrawals.map {
          case (address, withdrawals) =>
            address -> withdrawals.filterNot(w => expiredPairs.contains((address, w)))
        }.filter { case (_, withdrawalList) => withdrawalList.nonEmpty }
        (existingNodeCollaterals, unexpiredWithdrawals, expiredWithdrawals)
      }
    }

    def findExpiredWithdrawalsViaIndexFromMpt(
      previousEpochProgress: EpochProgress,
      epochProgress: EpochProgress,
      withdrawalTimeLimit: EpochProgress
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
            for {
              key <- GlobalStateKey.expiryIndexKey[F](SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals, e)
              hexKey <- GlobalStateKey.toHex[F](key)
              bucket <- StrictMptRead.valueOrElseF(
                reader.getStrict[SortedSet[NodeCollateralWithdrawalExpiryKey]](key),
                SortedSet.empty[NodeCollateralWithdrawalExpiryKey],
                s"sweep ExpiryIndex(label=${SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals.canonicalName},epoch=${e.value.value})",
                hexKey
              )
            } yield e -> bucket
          }
          indexedKeys = buckets.flatMap { case (bucketEpoch, bucket) => bucket.toList.map(bucketEpoch -> _) }
          byAddress = indexedKeys.groupBy(_._2.address)
          resolved <- byAddress.toList.sortBy(_._1).traverse {
            case (addr, indexedExpiryKeys) =>
              val targetKey = GlobalStateKey.hypergraph(GlobalStateFieldId.NodeCollateralWithdrawals, addr)
              val expectedEpochsByHash = indexedExpiryKeys.groupMap(_._2.hash)(_._1).view.mapValues(_.toSet).toMap
              val expectedHashes = expectedEpochsByHash.keySet
              for {
                targetHex <- GlobalStateKey.toHex[F](targetKey)
                addrSet <- StrictMptRead.requirePresentF(
                  reader.getStrict[SortedSet[PendingNodeCollateralWithdrawal]](targetKey),
                  s"expiry target NodeCollateralWithdrawals(address=$addr)",
                  targetHex
                )
                hashed <- addrSet.toList.traverse(w => w.event.toHashed.map(h => (h.hash, w)))
                actualHashes = hashed.iterator.map(_._1).toSet
                missingHashes = expectedHashes -- actualHashes
                wrongEpochs = hashed.collect {
                  case (hash, withdrawal)
                      if expectedEpochsByHash.contains(hash) &&
                        expectedEpochsByHash(hash) != Set(withdrawal.createdAt |+| withdrawalTimeLimit) =>
                    val indexed = expectedEpochsByHash(hash).toList.map(_.value.value).sorted.mkString("/")
                    val actual = (withdrawal.createdAt |+| withdrawalTimeLimit).value.value
                    s"${hash.value}:indexed=$indexed,actual=$actual"
                }.sorted
                _ <- Async[F]
                  .raiseError[Unit](
                    StrictMptRead.InconsistentConsensusMptIndex(
                      s"expiry target NodeCollateralWithdrawals(address=$addr)",
                      targetHex,
                      s"bucket hashes absent from decoded target: ${missingHashes.toList.map(_.value).sorted.mkString(",")}"
                    )
                  )
                  .whenA(missingHashes.nonEmpty)
                _ <- Async[F]
                  .raiseError[Unit](
                    StrictMptRead.InconsistentConsensusMptIndex(
                      s"expiry target NodeCollateralWithdrawals(address=$addr)",
                      targetHex,
                      s"bucket epoch differs from target expiry: ${wrongEpochs.mkString(",")}"
                    )
                  )
                  .whenA(wrongEpochs.nonEmpty)
                matched = hashed.collect { case (h, w) if expectedHashes.contains(h) => w }.to(SortedSet)
              } yield addr -> matched
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
              reader
                .get[SortedSet[NodeCollateralRecord]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveNodeCollaterals, addr))
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

    def materializeActiveNodeCollateralAddressesFromMpt(implicit hasher: Hasher[F]): F[Set[Address]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveNodeCollaterals)
        entries <- reader.getAllForPrefix[SortedSet[NodeCollateralRecord]](prefix)
      } yield entries.values.toList.mapFilter(s => s.headOption.map(_.event.value.source)).toSet

    def materializeNodeCollateralWithdrawalAddressesFromMpt(implicit hasher: Hasher[F]): F[Set[Address]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.NodeCollateralWithdrawals)
        entries <- reader.getAllForPrefix[SortedSet[PendingNodeCollateralWithdrawal]](prefix)
      } yield entries.values.toList.mapFilter(s => s.headOption.map(_.event.value.source)).toSet

    def materializeNodeCollateralWithdrawalsFromMpt(
      implicit hasher: Hasher[F]
    ): F[SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.NodeCollateralWithdrawals)
        entries <- reader.getAllForPrefix[SortedSet[PendingNodeCollateralWithdrawal]](prefix)
      } yield
        SortedMap.from(
          entries.values.toList
            .mapFilter(set => set.headOption.map(h => h.event.value.source -> set))
        )

    def materializeActiveNodeCollateralsFromMpt(
      implicit hasher: Hasher[F]
    ): F[SortedMap[Address, SortedSet[NodeCollateralRecord]]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveNodeCollaterals)
        entries <- reader.getAllForPrefix[SortedSet[NodeCollateralRecord]](prefix)
      } yield
        SortedMap.from(
          entries.values.toList.mapFilter(set => set.headOption.map(_.event.value.source -> set)).filter(_._2.nonEmpty)
        )
  }
}
