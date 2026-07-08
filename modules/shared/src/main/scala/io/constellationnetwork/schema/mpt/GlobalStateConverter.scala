package io.constellationnetwork.schema.mpt

import cats.effect.{Async, Sync}
import cats.syntax.all._
import cats.{Order, Parallel}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSnapshotSync
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.merkletree.Proof
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageType}
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.MptStore
import io.constellationnetwork.schema.mpt.PartitionNamespace.{AddressNamespace, MetagraphNamespace}
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, HistoricalStakeSnapshot}
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.priceOracle.{PriceRecord, TokenPair}
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.swap.{AllowSpend, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal, StateProofSelector}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.{MerklePatriciaError, StatefulMerklePatriciaProducer}
import io.constellationnetwork.security.mpt.{MerklePatriciaTrie, MptRoot}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendRefImmutable}
import io.constellationnetwork.serde.codecs.instances.CompatCodecs._
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs._
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutable}
import io.constellationnetwork.serde.codecs.instances.MerkleTreeCodecs.proofImmutableCodec
import io.constellationnetwork.serde.codecs.instances.MetagraphSyncDataInfoCodec.{immutableCodec => metagraphSyncImmutable}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.PriceOracleCodecs.priceRecordImmutableCodec
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scodec.bits.ByteVector

object GlobalStateConverter {

  case class StateChangesAccumulator(
    lastStateChannelSnapshotHashes: SortedMap[Address, Hash] = SortedMap.empty,
    lastTxRefs: SortedMap[Address, TransactionReference] = SortedMap.empty,
    balances: SortedMap[Address, Balance] = SortedMap.empty,
    lastCurrencySnapshots: SortedMap[Address, Either[Signed[
      CurrencySnapshot
    ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]] = SortedMap.empty,
    lastCurrencySnapshotsProofs: SortedMap[Address, Proof] = SortedMap.empty,
    activeAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]] = SortedMap.empty,
    activeTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]] = SortedMap.empty,
    tokenLockBalances: SortedMap[Address, SortedMap[Address, Balance]] = SortedMap.empty,
    lastAllowSpendRefs: SortedMap[Address, AllowSpendReference] = SortedMap.empty,
    lastTokenLockRefs: SortedMap[Address, TokenLockReference] = SortedMap.empty,
    activeDelegatedStakes: SortedMap[Address, SortedSet[DelegatedStakeRecord]] = SortedMap.empty,
    delegatedStakesWithdrawals: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]] = SortedMap.empty,
    activeNodeCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]] = SortedMap.empty,
    nodeCollateralWithdrawals: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]] = SortedMap.empty,
    metagraphSyncData: SortedMap[Address, MetagraphSyncDataInfo] = SortedMap.empty,
    updateNodeParameters: SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)] = SortedMap.empty,
    priceState: SortedMap[TokenPair, PriceRecord] = SortedMap.empty,
    allowSpendExpiryIndex: SystemIndexDelta[AllowSpendExpiryKey] = SystemIndexDelta.empty[AllowSpendExpiryKey],
    tokenLockExpiryIndex: SystemIndexDelta[TokenLockExpiryKey] = SystemIndexDelta.empty[TokenLockExpiryKey],
    nodeCollateralWithdrawalExpiryIndex: SystemIndexDelta[NodeCollateralWithdrawalExpiryKey] =
      SystemIndexDelta.empty[NodeCollateralWithdrawalExpiryKey],
    removedAllowSpendKeys: Set[(Option[Address], Address)] = Set.empty,
    removedTokenLockKeys: Set[Address] = Set.empty,
    removedTokenLockBalanceKeys: Set[(Address, Address)] = Set.empty,
    removedDelegatedStakeKeys: Set[Address] = Set.empty,
    removedDelegatedStakeWithdrawalKeys: Set[Address] = Set.empty,
    removedNodeCollateralKeys: Set[Address] = Set.empty,
    removedNodeCollateralWithdrawalKeys: Set[Address] = Set.empty,
    // §3 NIPoPoW S0.4 boundary delta. Non-empty only at boundary ordinals
    // (`ord % etaRotationSnapshots == etaRotationSnapshots - 1L`); empty otherwise.
    // `historicalStakeSnapshots` carries the new entry(ies) appended at this boundary — each carries
    // BOTH stake_N and eta_N (Path 1 of the heap-leak workstream; the eta half is a disk-immune cache
    // for `EtaStateManager.getEta(period)` against chainStore eviction).
    // `removedHistoricalStakeSnapshotKeys` carries the period keys evicted by retention.
    historicalStakeSnapshots: SortedMap[EtaPeriod, HistoricalStakeSnapshot] = SortedMap.empty,
    removedHistoricalStakeSnapshotKeys: Set[EtaPeriod] = Set.empty
  )

  /** Apply a per-ordinal typed [[StateChangesAccumulator]] delta to a prior [[GlobalSnapshotInfo]], yielding the GSI the producer's
    * `GlobalSnapshotAcceptanceManager.accept()` would have built for that ordinal. PURE data logic — no codec, no auto-derivation, no MPT.
    *
    * This is the typed mirror of `MptStore.syncFromStateChanges` (which applies the SAME accumulator to the MPT byte store). The two stay
    * in lockstep: for every field, the merge rule here is byte-for-byte the same shape the MPT writer applies, so a follower that adopts
    * this GSI and recomputes the MPT root over its bytes lands on the producer's signed `mptRoot` (the verify-before-adopt anchor). 1:1 GSI
    * ↔ accumulator field correspondence (~18 per-field merges):
    *
    *   - '''Always-present overwrite''' (`prior.field ++ delta.field` — the delta carries the COMPLETE new value per touched key): the five
    *     mandatory maps (`lastStateChannelSnapshotHashes`, `lastTxRefs`, `balances`, `lastCurrencySnapshots`,
    *     `lastCurrencySnapshotsProofs`) plus the per-key-additive Option maps (`lastAllowSpendRefs`, `lastTokenLockRefs`,
    *     `metagraphSyncData`, `updateNodeParameters`, `priceState`) and the bare `historicalStakeSnapshots` (with its retention removals).
    *   - '''Removal-set fields''' (`(prior.field -- removedXKeys) ++ delta.field`): `activeTokenLocks`(`removedTokenLockKeys`),
    *     `tokenLockBalances`(`removedTokenLockBalanceKeys`), `activeDelegatedStakes`(`removedDelegatedStakeKeys`),
    *     `delegatedStakesWithdrawals`(`removedDelegatedStakeWithdrawalKeys`), `activeNodeCollaterals`(`removedNodeCollateralKeys`),
    *     `nodeCollateralWithdrawals`(`removedNodeCollateralWithdrawalKeys`).
    *   - '''Nested allow-spends''' (`activeAllowSpends`): merged at the FLATTENED `(optAddr, addr)` granularity — exactly how the MPT
    *     writer keys it. Per `(optAddr, addr)` key: the delta's complete `SortedSet` value overwrites; `removedAllowSpendKeys` deletes. The
    *     inner map is NOT wholesale-replaced at the top-level `optAddr` key, and an `optAddr` bucket left empty after removals is dropped.
    *
    * '''Option-emptiness convention.''' Post-tessellation3 (the steady-state ml0 follow path) the producer wraps every optional field in
    * `Some(_)` even when the resulting map is empty (`Era.postTess3`). So when `prior.field` is `Some(_)` we keep `Some(merged)` (matching
    * createContext); when `prior.field` is `None` we lift to `Some(merged)` only if the delta/removal touched it, else preserve `None`. The
    * Some-vs-None distinction is MPT-invisible (both encode zero entries for an empty field), so the root-match cannot police it — getting
    * it right here keeps the GSI ml0 KEEPS for reads + the data-application context faithful to the producer.
    *
    * Edge (accepted, not policed): `priceState` / `metagraphSyncData` activate at the LATER `postTess301` / `postMetagraphSync` thresholds,
    * not `postTess3`. At a sub-threshold-boundary ordinal the producer may emit `None` where prior was `Some(empty)` (or vice versa). This
    * is MPT-invisible / root-equivalent (verify still passes) and the candidate GSI is consumed by ml0 for READS + the data-application
    * only (never consensus bytes), so the brief Some-vs-None drift on these two fields is harmless and intentionally left un-gated.
    */
  def applyAccumulatorToGSI(prior: GlobalSnapshotInfo, delta: StateChangesAccumulator): GlobalSnapshotInfo = {
    // Always-present (mandatory) maps — overwrite per touched key.
    val lastStateChannelSnapshotHashes = prior.lastStateChannelSnapshotHashes ++ delta.lastStateChannelSnapshotHashes
    val lastTxRefs = prior.lastTxRefs ++ delta.lastTxRefs
    val balances = prior.balances ++ delta.balances
    val lastCurrencySnapshots = prior.lastCurrencySnapshots ++ delta.lastCurrencySnapshots
    val lastCurrencySnapshotsProofs = prior.lastCurrencySnapshotsProofs ++ delta.lastCurrencySnapshotsProofs

    // Bare (non-Option) historicalStakeSnapshots — apply retention removals then overlay boundary adds.
    val historicalStakeSnapshots =
      (prior.historicalStakeSnapshots -- delta.removedHistoricalStakeSnapshotKeys) ++ delta.historicalStakeSnapshots

    // Option helper for per-key-additive optional maps (no removal set): result Some iff prior was Some or the delta is non-empty.
    def mergeOptAdditive[K, V](
      priorOpt: Option[SortedMap[K, V]],
      deltaMap: SortedMap[K, V]
    ): Option[SortedMap[K, V]] =
      priorOpt match {
        case Some(p) => Some(p ++ deltaMap)
        case None    => if (deltaMap.nonEmpty) Some(deltaMap) else None
      }

    // Option helper for removal-set optional maps: `(prior -- removed) ++ delta`. Some iff prior was Some or the delta/removal touched it.
    def mergeOptWithRemovals[K, V](
      priorOpt: Option[SortedMap[K, V]],
      deltaMap: SortedMap[K, V],
      removedKeys: Set[K]
    ): Option[SortedMap[K, V]] =
      priorOpt match {
        case Some(p) => Some((p -- removedKeys) ++ deltaMap)
        case None    => if (deltaMap.nonEmpty || removedKeys.nonEmpty) Some(deltaMap) else None
      }

    // Generic two-level merge for a nested `Option[SortedMap[OuterK, SortedMap[InnerK, V]]]` whose MPT partition is keyed at the FLATTENED
    // `(OuterK, InnerK)` granularity (activeAllowSpends, tokenLockBalances). Per flattened key: the delta's complete value overwrites and
    // `removedFlatKeys` deletes; an outer bucket left empty after removals is dropped. Mirrors the MPT writer's per-flat-key insert/remove.
    def mergeNestedFlat[OuterK: Ordering, InnerK: Ordering, V](
      priorOpt: Option[SortedMap[OuterK, SortedMap[InnerK, V]]],
      deltaMap: SortedMap[OuterK, SortedMap[InnerK, V]],
      removedFlatKeys: Set[(OuterK, InnerK)]
    ): Option[SortedMap[OuterK, SortedMap[InnerK, V]]] = {
      val touched = deltaMap.nonEmpty || removedFlatKeys.nonEmpty
      priorOpt match {
        case None if !touched => None
        case _ =>
          val base = priorOpt.getOrElse(SortedMap.empty[OuterK, SortedMap[InnerK, V]])
          val afterRemovals: Map[(OuterK, InnerK), V] =
            base.iterator.flatMap {
              case (outer, inner) => inner.iterator.map { case (innerK, v) => (outer, innerK) -> v }
            }.toMap -- removedFlatKeys
          val afterDelta: Map[(OuterK, InnerK), V] =
            afterRemovals ++ deltaMap.iterator.flatMap {
              case (outer, inner) => inner.iterator.map { case (innerK, v) => (outer, innerK) -> v }
            }
          val regrouped: SortedMap[OuterK, SortedMap[InnerK, V]] =
            SortedMap.from(
              afterDelta.toList
                .groupBy(_._1._1)
                .view
                .mapValues(entries => SortedMap.from(entries.map { case ((_, innerK), v) => innerK -> v }))
                .filter { case (_, inner) => inner.nonEmpty }
                .toMap
            )
          Some(regrouped)
      }
    }

    GlobalSnapshotInfo(
      lastStateChannelSnapshotHashes = lastStateChannelSnapshotHashes,
      lastTxRefs = lastTxRefs,
      balances = balances,
      lastCurrencySnapshots = lastCurrencySnapshots,
      lastCurrencySnapshotsProofs = lastCurrencySnapshotsProofs,
      activeAllowSpends = mergeNestedFlat(prior.activeAllowSpends, delta.activeAllowSpends, delta.removedAllowSpendKeys),
      activeTokenLocks = mergeOptWithRemovals(prior.activeTokenLocks, delta.activeTokenLocks, delta.removedTokenLockKeys),
      tokenLockBalances = mergeNestedFlat(prior.tokenLockBalances, delta.tokenLockBalances, delta.removedTokenLockBalanceKeys),
      lastAllowSpendRefs = mergeOptAdditive(prior.lastAllowSpendRefs, delta.lastAllowSpendRefs),
      lastTokenLockRefs = mergeOptAdditive(prior.lastTokenLockRefs, delta.lastTokenLockRefs),
      updateNodeParameters = mergeOptAdditive(prior.updateNodeParameters, delta.updateNodeParameters),
      activeDelegatedStakes =
        mergeOptWithRemovals(prior.activeDelegatedStakes, delta.activeDelegatedStakes, delta.removedDelegatedStakeKeys),
      delegatedStakesWithdrawals = mergeOptWithRemovals(
        prior.delegatedStakesWithdrawals,
        delta.delegatedStakesWithdrawals,
        delta.removedDelegatedStakeWithdrawalKeys
      ),
      activeNodeCollaterals =
        mergeOptWithRemovals(prior.activeNodeCollaterals, delta.activeNodeCollaterals, delta.removedNodeCollateralKeys),
      nodeCollateralWithdrawals = mergeOptWithRemovals(
        prior.nodeCollateralWithdrawals,
        delta.nodeCollateralWithdrawals,
        delta.removedNodeCollateralWithdrawalKeys
      ),
      priceState = mergeOptAdditive(prior.priceState, delta.priceState),
      metagraphSyncData = mergeOptAdditive(prior.metagraphSyncData, delta.metagraphSyncData),
      historicalStakeSnapshots = historicalStakeSnapshots
    )
  }

  /** The exact O(changes) sidecar/index keys `toAccumulatorHexDelta`'s replay reads back from the store: the touched expiry-index epoch
    * buckets (from the accumulator's three `SystemIndexDelta`s) and the active-address / address-pair index entries for the fields whose
    * keyset this ordinal touched. Returns the HEX keys; the caller reads ONLY these from the store (no full-state scan).
    *
    * Keep in lockstep with `toAccumulatorHexDelta` — every key it `preSyncBytes.get(_)`s for must appear here, or the replayed bucket/index
    * bytes silently diverge from the in-store sync and the recomputed root mismatches (which the verify gate then rejects, falling back to
    * the full path — safe, but defeats the adopt fast-path).
    */
  def changeSetPreSyncHexKeys[F[_]: Async: Hasher](acc: StateChangesAccumulator): F[Set[Hex]] = {
    import io.constellationnetwork.schema.mpt.GlobalStateFieldId._

    def expiryKeys(label: SystemNamespaceLabel, delta: SystemIndexDelta[_]): F[List[GlobalStateKey]] =
      delta match {
        case eb: SystemIndexDelta.EpochBucket[_] =>
          eb.touchedEpochs.toList.traverse(epoch => GlobalStateKey.expiryIndexKey[F](label, epoch))
      }

    // Active-address-index fields whose keyset this ordinal touched (additive — refs/balances/etc. that don't embed their address).
    val activeAddressIndexFields: List[GlobalStateFieldId] =
      List(LastAllowSpendRefs, LastTokenLockRefs, LastTxRefs, Balances, LastStateChannelSnapshotHashes, LastCurrencySnapshots)

    val touchedActiveAddressFields: List[GlobalStateFieldId] = activeAddressIndexFields.filter {
      case LastAllowSpendRefs             => acc.lastAllowSpendRefs.nonEmpty
      case LastTokenLockRefs              => acc.lastTokenLockRefs.nonEmpty
      case LastTxRefs                     => acc.lastTxRefs.nonEmpty
      case Balances                       => acc.balances.nonEmpty
      case LastStateChannelSnapshotHashes => acc.lastStateChannelSnapshotHashes.nonEmpty
      case LastCurrencySnapshots          => acc.lastCurrencySnapshots.nonEmpty
      case _                              => false
    }

    val tokenLockBalanceTouched =
      acc.tokenLockBalances.nonEmpty || acc.removedTokenLockBalanceKeys.nonEmpty

    for {
      asExpiry <- expiryKeys(SystemNamespaceLabel.ExpiryIndexAllowSpends, acc.allowSpendExpiryIndex)
      tlExpiry <- expiryKeys(SystemNamespaceLabel.ExpiryIndexTokenLocks, acc.tokenLockExpiryIndex)
      ncwExpiry <- expiryKeys(SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals, acc.nodeCollateralWithdrawalExpiryIndex)
      addrIdx <- touchedActiveAddressFields.traverse(fid => GlobalStateKey.activeAddressIndexKey[F](fid))
      pairIdx <-
        if (tokenLockBalanceTouched) GlobalStateKey.activeAddressIndexKey[F](TokenLockBalances).map(List(_))
        else List.empty[GlobalStateKey].pure[F]
      allKeys = asExpiry ++ tlExpiry ++ ncwExpiry ++ addrIdx ++ pairIdx
      hexKeys <- allKeys.traverse(GlobalStateKey.toHex[F])
    } yield hexKeys.toSet
  }

  /** ml0 (currency-l0) global-FOLLOW adopt-and-verify decision for ONE ordinal `N` (task #12). The verify-before-adopt safety gate,
    * isolated from `StateChannel` so it is unit-testable without the full snapshot-processing loop.
    *
    * Given the prior GSI (ml0's `lastState`), the per-ordinal `delta`, the signed snapshot's claimed `signedMptRoot` for `N`, and `N`:
    *
    *   1. Builds `candidateGSI = applyAccumulatorToGSI(prior, delta)` (the typed GSI ml0 would keep). 2. Reads ONLY the O(changes)
    *      sidecar/index keys the hex-delta replay needs (`changeSetPreSyncHexKeys`) from the store as `preSyncBytes`. 3. Derives `(hexUp,
    *      hexRem) = toAccumulatorHexDelta(delta, preSyncBytes)` — the SAME typed→hex derivation the producer uses. 4. Inside
    *      `mptStore.withTransaction`: applies `hexRem` then `hexUp` to the producer incrementally, builds at `N` to get `newRoot`, and — if
    *      `Some(newRoot) === signedMptRoot` — yields `(Some(candidateGSI), Commit)`; otherwise `(None, Rollback)`.
    *
    * The `withTransaction` Commit/Rollback bracket is THE enforcement: on any mismatch (wrong/tampered delta, incomplete preSyncBytes,
    * evicted base) the MPT mutations are rolled back and `None` is returned — the caller MUST then fall back to the full path and never
    * advance ml0 state. `Some(gsi)` is returned ONLY when the recomputed root equals the signed root, so adoption is correct by
    * construction.
    *
    * Returns `None` (caller falls back) on: root mismatch, OR `signedMptRoot = None` (legacy-format ordinal — no MPT anchor to verify
    * against, so the cheap adopt path is not safe; defer to createContext).
    */
  def adoptAndVerifyChangeSetDelta[F[_]: Async: Parallel: Hasher: JsonSerializer](
    mptStore: MptStore[F, GlobalStateKey],
    prior: GlobalSnapshotInfo,
    delta: StateChangesAccumulator,
    signedMptRoot: Option[Hash],
    ordinal: SnapshotOrdinal
  )(implicit stateProofSelector: StateProofSelector): F[Option[GlobalSnapshotInfo]] =
    signedMptRoot match {
      case None =>
        // Legacy-format ordinal: no signed mptRoot to anchor verification → adopt path unsafe, fall back.
        none[GlobalSnapshotInfo].pure[F]
      case Some(expectedRoot) =>
        val candidateGSI = applyAccumulatorToGSI(prior, delta)
        for {
          preSyncKeys <- changeSetPreSyncHexKeys[F](delta)
          storeBytes <- mptStore.allEntriesAsBytes
          preSyncBytes = storeBytes.view.filterKeys(preSyncKeys).toMap
          hexDelta <- toAccumulatorHexDelta[F](delta, preSyncBytes)
          (hexUp, hexRem) = hexDelta
          result <- mptStore.withTransaction {
            for {
              _ <- mptStore.underlying.remove(hexRem.toList).whenA(hexRem.nonEmpty)
              _ <- mptStore.underlying.insertBytes(hexUp).whenA(hexUp.nonEmpty)
              _ <- mptStore.underlying.buildForOrdinal(ordinal)
              // `expectedRoot` (the signed `mptRoot`) excludes path-dependent SystemNamespace sidecars
              // (`GlobalSnapshotInfo.mptStateProofFromBytes`); `getRootHashForOrdinal` is the producer's root over
              // ALL stored bytes (sidecars included). Recompute sidecar-free so this adopt-verify gate compares
              // apples-to-apples — otherwise EVERY ChangeSet delta adoption would mismatch and fall back.
              afterBytes <- mptStore.underlying.entries
              newRoot <- io.constellationnetwork.schema.GlobalSnapshotInfo.sidecarFreeMptRoot[F](afterBytes)
              matches = newRoot === expectedRoot
              out <-
                if (matches)
                  // On the verified-match branch ONLY, do the SAME tail work the retained
                  // createContext -> syncFromStateChanges -> store.commit(ordinal) path does:
                  // advance the store's last-synced ordinal + persist (build + persist + bookkeeping).
                  // Without this the in-memory trie is correct but lastSyncedOrdinal stays stale and the
                  // on-disk MPT lags across restarts. Kept inside this branch so a mismatch rolls back with
                  // NO persist and ml0 never advances on an unverified delta.
                  mptStore.commit(ordinal).as((candidateGSI.some, MptTxAction.Commit: MptTxAction))
                else
                  (none[GlobalSnapshotInfo], MptTxAction.Rollback: MptTxAction).pure[F]
            } yield out
          }
        } yield result
    }

  /** Apply a `(added, removed)` delta to the `ActiveAddressIndex` partition for `fieldId`. Read-modify-write on the single MPT entry that
    * holds the `SortedSet[Address]` for that field — no-ops when the resulting set is unchanged, deletes the entry when it becomes empty.
    *
    * The index lets manager `materializeXFromMpt` paths recover keys for `Map[Address, V]`-shaped fields whose value type doesn't carry the
    * address (refs, balances). For fields where the value embeds `source: Address` (AllowSpend/TokenLock/etc.), use the prefix-scan +
    * value-decode path instead — no index needed.
    */
  private[mpt] def applyActiveAddressIndexDelta[F[_]: Async: Hasher](
    store: MptStore[F, GlobalStateKey],
    fieldId: GlobalStateFieldId,
    added: Set[Address],
    removed: Set[Address]
  ): F[Unit] =
    if (added.isEmpty && removed.isEmpty) Async[F].unit
    else
      GlobalStateKey.activeAddressIndexKey[F](fieldId).flatMap { key =>
        for {
          existing <- store.get[SortedSet[Address]](key).map(_.getOrElse(SortedSet.empty[Address]))
          merged = (existing ++ added) -- removed
          _ <-
            if (merged.isEmpty && existing.nonEmpty) store.remove(key)
            else if (merged.nonEmpty && merged != existing) store.insert[SortedSet[Address]](key, merged)
            else Async[F].unit
        } yield ()
      }

  /** Apply a `(added, removed)` delta to the address-pair index partition for `fieldId`. Same read-modify-write pattern as
    * `applyActiveAddressIndexDelta`, but the entry value is a `SortedSet[(Address, Address)]` — used by `tokenLockBalances`-style fields
    * whose canonical key is `(metagraphAddr, holderAddr)` and whose `Balance` value type carries neither address.
    */
  private[mpt] def applyAddressPairIndexDelta[F[_]: Async: Hasher](
    store: MptStore[F, GlobalStateKey],
    fieldId: GlobalStateFieldId,
    added: Set[(Address, Address)],
    removed: Set[(Address, Address)]
  ): F[Unit] =
    if (added.isEmpty && removed.isEmpty) Async[F].unit
    else
      GlobalStateKey.activeAddressIndexKey[F](fieldId).flatMap { key =>
        for {
          existing <- store
            .get[SortedSet[(Address, Address)]](key)
            .map(_.getOrElse(SortedSet.empty[(Address, Address)]))
          merged = (existing ++ added) -- removed
          _ <-
            if (merged.isEmpty && existing.nonEmpty) store.remove(key)
            else if (merged.nonEmpty && merged != existing) store.insert[SortedSet[(Address, Address)]](key, merged)
            else Async[F].unit
        } yield ()
      }

  /** Apply a `SystemIndexDelta[K]` to the given `label`'s partition. Each `EpochBucket` delta triggers per-epoch read-modify-write on the
    * bucket: read the current `SortedSet[K]`, merge adds, apply removes, write back — or delete the bucket entirely if it becomes empty.
    * Future `SystemIndexDelta` ADT variants add their own dispatch cases here.
    */
  private[mpt] def applySystemIndexDelta[F[_]: Async: Hasher, K: Order](
    store: MptStore[F, GlobalStateKey],
    label: SystemNamespaceLabel,
    delta: SystemIndexDelta[K]
  )(implicit codec: ImmutableCodec[SortedSet[K]]): F[Unit] = delta match {
    case eb: SystemIndexDelta.EpochBucket[K] if eb.isEmpty => Async[F].unit
    case eb: SystemIndexDelta.EpochBucket[K] =>
      implicit val ordering: Ordering[K] = Order[K].toOrdering
      eb.touchedEpochs.toList.traverse_ { epoch =>
        GlobalStateKey.expiryIndexKey[F](label, epoch).flatMap { key =>
          for {
            existing <- store.get[SortedSet[K]](key).map(_.getOrElse(SortedSet.empty[K]))
            toAdd = eb.adds.getOrElse(epoch, Set.empty[K])
            toRemove = eb.removes.getOrElse(epoch, Set.empty[K])
            merged = (existing ++ toAdd) -- toRemove
            _ <-
              if (merged.isEmpty && existing.nonEmpty) store.remove(key)
              else if (merged.nonEmpty && merged != existing) store.insert[SortedSet[K]](key, merged)
              else Async[F].unit
          } yield ()
        }
      }
  }

  private def convertRequiredHypergraph[F[_]: Sync: Parallel, A: Encoder](
    data: SortedMap[Address, A],
    fieldId: GlobalStateFieldId
  ): F[Map[GlobalStateKey, Json]] =
    data.toSeq.parTraverse {
      case (addr, value) =>
        (GlobalStateKey.hypergraph(fieldId, addr) -> value.asJson).pure[F]
    }
      .map(_.toMap)

  private def convertRequiredMetagraph[F[_]: Sync: Parallel, A: Encoder](
    data: SortedMap[Address, A],
    fieldId: GlobalStateFieldId
  ): F[Map[GlobalStateKey, Json]] =
    data.toSeq.parTraverse {
      case (addr, value) =>
        (GlobalStateKey.metagraph(addr, fieldId) -> value.asJson).pure[F]
    }
      .map(_.toMap)

  private def convertOptionalHypergraph[F[_]: Sync, A: Encoder](
    dataOpt: Option[SortedMap[Address, A]],
    fieldId: GlobalStateFieldId
  ): F[Map[GlobalStateKey, Json]] =
    dataOpt
      .map(_.toSeq.map {
        case (addr, value) =>
          GlobalStateKey.hypergraph(fieldId, addr) -> value.asJson
      }.toMap)
      .getOrElse(Map.empty)
      .pure[F]

  private def convertCurrencySnapshots[F[_]: Async: Parallel: Hasher: JsonSerializer](
    data: SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]
  )(
    implicit stateProofSelector: StateProofSelector
  ): F[Map[GlobalStateKey, Json]] =
    data.toSeq.parTraverse {
      case (metagraphAddr, Left(fullSnapshot)) =>
        for {
          currencyIncrementalSnapshot <- CurrencyIncrementalSnapshot.fromCurrencySnapshot(fullSnapshot.value)
          infoEntries <- infoEntryJson[F](metagraphAddr, fullSnapshot.info.toCurrencySnapshotInfo)
        } yield
          (GlobalStateKey.metagraph(metagraphAddr, GlobalStateFieldId.LastIncrementalCurrencySnapshots) -> Signed(
            currencyIncrementalSnapshot,
            fullSnapshot.proofs
          ).asJson) :: infoEntries

      case (metagraphAddr, Right((incrementalSnapshot, snapshotInfo))) =>
        infoEntryJson[F](metagraphAddr, snapshotInfo).map { infoEntries =>
          (GlobalStateKey.metagraph(metagraphAddr, GlobalStateFieldId.LastIncrementalCurrencySnapshots) ->
            incrementalSnapshot.asJson) :: infoEntries
        }
    }
      .map(_.flatten.toMap)

  private def convertActiveAllowSpends[F[_]: Sync](
    dataOpt: Option[SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]]
  ): F[Map[GlobalStateKey, Json]] =
    dataOpt
      .map(_.toSeq.flatMap {
        case (optAddr, innerMap) =>
          innerMap.toSeq.map {
            case (addr, allowSpends) =>
              GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, optAddr, addr) -> allowSpends.asJson
          }
      }.toMap)
      .getOrElse(Map.empty)
      .pure[F]

  private def convertTokenLockBalances[F[_]: Sync](
    dataOpt: Option[SortedMap[Address, SortedMap[Address, Balance]]]
  ): F[Map[GlobalStateKey, Json]] =
    dataOpt
      .map(_.toSeq.flatMap {
        case (tokenAddr, innerMap) =>
          innerMap.toSeq.map {
            case (holderAddr, balance) =>
              GlobalStateKey.hypergraph(GlobalStateFieldId.TokenLockBalances, tokenAddr, holderAddr) -> balance.asJson
          }
      }.toMap)
      .getOrElse(Map.empty)
      .pure[F]

  def toStateKeyValuePairsFromAccumulator[F[_]: Async: Parallel: Hasher: JsonSerializer](
    acc: StateChangesAccumulator
  )(
    implicit stateProofSelector: StateProofSelector
  ): F[Map[GlobalStateKey, Json]] =
    (
      convertRequiredMetagraph(acc.lastStateChannelSnapshotHashes, GlobalStateFieldId.LastStateChannelSnapshotHashes),
      convertRequiredHypergraph(acc.lastTxRefs, GlobalStateFieldId.LastTxRefs),
      convertRequiredHypergraph(acc.balances, GlobalStateFieldId.Balances),
      convertCurrencySnapshots(acc.lastCurrencySnapshots),
      convertRequiredMetagraph(acc.lastCurrencySnapshotsProofs, GlobalStateFieldId.LastCurrencySnapshotsProofs),
      convertActiveAllowSpends(if (acc.activeAllowSpends.nonEmpty) acc.activeAllowSpends.some else none),
      convertOptionalHypergraph(
        if (acc.activeTokenLocks.nonEmpty) acc.activeTokenLocks.some else none,
        GlobalStateFieldId.ActiveTokenLocks
      ),
      convertTokenLockBalances(if (acc.tokenLockBalances.nonEmpty) acc.tokenLockBalances.some else none),
      convertOptionalHypergraph(
        if (acc.lastAllowSpendRefs.nonEmpty) acc.lastAllowSpendRefs.some else none,
        GlobalStateFieldId.LastAllowSpendRefs
      ),
      convertOptionalHypergraph(
        if (acc.lastTokenLockRefs.nonEmpty) acc.lastTokenLockRefs.some else none,
        GlobalStateFieldId.LastTokenLockRefs
      ),
      convertOptionalHypergraph(
        if (acc.activeDelegatedStakes.nonEmpty) acc.activeDelegatedStakes.some else none,
        GlobalStateFieldId.ActiveDelegatedStakes
      ),
      convertOptionalHypergraph(
        if (acc.delegatedStakesWithdrawals.nonEmpty) acc.delegatedStakesWithdrawals.some else none,
        GlobalStateFieldId.DelegatedStakesWithdrawals
      ),
      convertOptionalHypergraph(
        if (acc.activeNodeCollaterals.nonEmpty) acc.activeNodeCollaterals.some else none,
        GlobalStateFieldId.ActiveNodeCollaterals
      ),
      convertOptionalHypergraph(
        if (acc.nodeCollateralWithdrawals.nonEmpty) acc.nodeCollateralWithdrawals.some else none,
        GlobalStateFieldId.NodeCollateralWithdrawals
      ),
      convertOptionalHypergraph(
        if (acc.metagraphSyncData.nonEmpty) acc.metagraphSyncData.some else none,
        GlobalStateFieldId.MetagraphSyncData
      )
    ).parMapN { (m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15) =>
      m1 ++ m2 ++ m3 ++ m4 ++ m5 ++ m6 ++ m7 ++ m8 ++ m9 ++ m10 ++ m11 ++ m12 ++ m13 ++ m14 ++ m15
    }

  def toAllStateKeyValuePairs[F[_]: Async: Parallel: Hasher: JsonSerializer](
    info: GlobalSnapshotInfo
  )(
    implicit stateProofSelector: StateProofSelector
  ): F[Map[GlobalStateKey, Json]] =
    (
      convertRequiredMetagraph(info.lastStateChannelSnapshotHashes, GlobalStateFieldId.LastStateChannelSnapshotHashes),
      convertRequiredHypergraph(info.lastTxRefs, GlobalStateFieldId.LastTxRefs),
      convertRequiredHypergraph(info.balances, GlobalStateFieldId.Balances),
      convertCurrencySnapshots(info.lastCurrencySnapshots),
      convertRequiredMetagraph(info.lastCurrencySnapshotsProofs, GlobalStateFieldId.LastCurrencySnapshotsProofs),
      convertActiveAllowSpends(info.activeAllowSpends),
      convertOptionalHypergraph(info.activeTokenLocks, GlobalStateFieldId.ActiveTokenLocks),
      convertTokenLockBalances(info.tokenLockBalances),
      convertOptionalHypergraph(info.lastAllowSpendRefs, GlobalStateFieldId.LastAllowSpendRefs),
      convertOptionalHypergraph(info.lastTokenLockRefs, GlobalStateFieldId.LastTokenLockRefs),
      convertOptionalHypergraph(info.activeDelegatedStakes, GlobalStateFieldId.ActiveDelegatedStakes),
      convertOptionalHypergraph(info.delegatedStakesWithdrawals, GlobalStateFieldId.DelegatedStakesWithdrawals),
      convertOptionalHypergraph(info.activeNodeCollaterals, GlobalStateFieldId.ActiveNodeCollaterals),
      convertOptionalHypergraph(info.nodeCollateralWithdrawals, GlobalStateFieldId.NodeCollateralWithdrawals),
      convertOptionalHypergraph(info.metagraphSyncData, GlobalStateFieldId.MetagraphSyncData)
    ).parMapN { (m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15) =>
      // Merge all maps - O(n) instead of O(n log n) foldLeft
      val allMaps = List(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15)
      val expectedSize = allMaps.map(_.size).sum
      val merged = allMaps.foldLeft(Map.empty[GlobalStateKey, Json])(_ ++ _)

      if (merged.size == expectedSize) Right(merged)
      else Left(new IllegalStateException(s"Duplicate keys found: expected $expectedSize entries but got ${merged.size}"))
    }.flatMap(_.liftTo[F])

  /** Typed scodec-encoded entries for a `GlobalSnapshotInfo`. Each field is encoded via its canonical `ImmutableCodec`, matching the bytes
    * the MptStore writes on `insert[V]` for that type. Used by `mptStateProof` so that the verification root matches the in-store root
    * (previously computed from JSON-via-JsonSerializer bytes, which produced a different root than the scodec-typed store).
    */
  def toAllStateKeyValueBytes[F[_]: Async: Parallel: Hasher: JsonSerializer](
    info: GlobalSnapshotInfo
  )(
    implicit stateProofSelector: StateProofSelector,
    withdrawalTimeLimitCtx: WithdrawalTimeLimit
  ): F[Map[GlobalStateKey, Array[Byte]]] = {
    val withdrawalTimeLimit: Option[EpochProgress] = withdrawalTimeLimitCtx.value
    import io.constellationnetwork.schema.mpt.GlobalStateFieldId._
    import io.constellationnetwork.serde.ImmutableCodec
    import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendRefImmutable}
    import io.constellationnetwork.serde.codecs.instances.CompatCodecs._
    import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
    import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutable}
    import io.constellationnetwork.serde.codecs.instances.MerkleTreeCodecs.proofImmutableCodec
    import io.constellationnetwork.serde.codecs.instances.MetagraphSyncDataInfoCodec.{immutableCodec => metagraphSyncImmutable}
    import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
    import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
    import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

    def enc[V](v: V)(implicit c: ImmutableCodec[V]): Array[Byte] = c.immutableBytes(v).toArray

    def hypergraph1[V: ImmutableCodec](
      data: Iterable[(Address, V)],
      fieldId: GlobalStateFieldId
    ): Iterable[(GlobalStateKey, Array[Byte])] =
      data.map { case (addr, v) => GlobalStateKey.hypergraph(fieldId, addr) -> enc(v) }

    def metagraph1[V: ImmutableCodec](
      data: Iterable[(Address, V)],
      fieldId: GlobalStateFieldId
    ): Iterable[(GlobalStateKey, Array[Byte])] =
      data.map { case (addr, v) => GlobalStateKey.metagraph(addr, fieldId) -> enc(v) }

    val stateChanHashes = metagraph1(info.lastStateChannelSnapshotHashes, LastStateChannelSnapshotHashes)
    val txRefs = hypergraph1(info.lastTxRefs, LastTxRefs)
    val balances = hypergraph1(info.balances, Balances)
    val currencyProofs = metagraph1(info.lastCurrencySnapshotsProofs, LastCurrencySnapshotsProofs)
    val activeAllowSpends = info.activeAllowSpends.toList.flatMap(_.toList).flatMap {
      case (optAddr, inner) =>
        inner.toList.map {
          case (addr, s) =>
            GlobalStateKey.hypergraph(ActiveAllowSpends, optAddr, addr) ->
              enc[SortedSet[Signed[io.constellationnetwork.schema.swap.AllowSpend]]](s)
        }
    }
    val activeTokenLocks = info.activeTokenLocks.toList.flatMap(_.toList).map {
      case (addr, s) =>
        GlobalStateKey.hypergraph(ActiveTokenLocks, addr) -> enc[SortedSet[Signed[io.constellationnetwork.schema.tokenLock.TokenLock]]](s)
    }
    val tokenLockBalances = info.tokenLockBalances.toList.flatMap(_.toList).flatMap {
      case (tokenAddr, inner) =>
        inner.toList.map { case (holder, bal) => GlobalStateKey.hypergraph(TokenLockBalances, tokenAddr, holder) -> enc(bal) }
    }
    val lastAllowSpendRefs = info.lastAllowSpendRefs.toList.flatMap(_.toList).map {
      case (addr, r) => GlobalStateKey.hypergraph(LastAllowSpendRefs, addr) -> enc(r)
    }
    val lastTokenLockRefs = info.lastTokenLockRefs.toList.flatMap(_.toList).map {
      case (addr, r) => GlobalStateKey.hypergraph(LastTokenLockRefs, addr) -> enc(r)
    }
    val activeDelegatedStakes = info.activeDelegatedStakes.toList.flatMap(_.toList).map {
      case (addr, s) =>
        GlobalStateKey.hypergraph(ActiveDelegatedStakes, addr) -> enc[SortedSet[DelegatedStakeRecord]](s)
    }
    val delegatedStakesWithdrawals = info.delegatedStakesWithdrawals.toList.flatMap(_.toList).map {
      case (addr, s) =>
        GlobalStateKey.hypergraph(DelegatedStakesWithdrawals, addr) -> enc[SortedSet[PendingDelegatedStakeWithdrawal]](s)
    }
    val activeNodeCollaterals = info.activeNodeCollaterals.toList.flatMap(_.toList).map {
      case (addr, s) =>
        GlobalStateKey.hypergraph(ActiveNodeCollaterals, addr) -> enc[SortedSet[NodeCollateralRecord]](s)
    }
    val nodeCollateralWithdrawals = info.nodeCollateralWithdrawals.toList.flatMap(_.toList).map {
      case (addr, s) =>
        GlobalStateKey.hypergraph(NodeCollateralWithdrawals, addr) -> enc[SortedSet[PendingNodeCollateralWithdrawal]](s)
    }
    val metagraphSyncData = info.metagraphSyncData.toList.flatMap(_.toList).map {
      case (addr, d) => GlobalStateKey.hypergraph(MetagraphSyncData, addr) -> enc(d)
    }

    val updateNodeParametersF: F[List[(GlobalStateKey, Array[Byte])]] =
      info.updateNodeParameters.toList.flatMap(_.toList).parTraverse {
        case (id, rec) =>
          GlobalStateKey.updateNodeParametersKey[F](id).map(k => k -> enc[(Signed[UpdateNodeParameters], SnapshotOrdinal)](rec))
      }

    val priceStateF: F[List[(GlobalStateKey, Array[Byte])]] =
      info.priceState.toList.flatMap(_.toList).parTraverse {
        case (tp, rec) => GlobalStateKey.priceStateKey[F](tp).map(k => k -> enc[PriceRecord](rec))
      }

    // Allow-spend expiry index: one bucket per `lastValidEpochProgress` with the set of records expiring at that epoch.
    val allowSpendExpiryIndexF: F[List[(GlobalStateKey, Array[Byte])]] = {
      val flat = info.activeAllowSpends.toList.flatMap(_.toList).flatMap {
        case (mid, innerMap) => innerMap.toList.flatMap { case (addr, set) => set.toList.map(s => (mid, addr, s)) }
      }
      flat.parTraverse {
        case (mid, addr, s) => s.toHashed.map(h => (s.lastValidEpochProgress, AllowSpendExpiryKey(mid, addr, h.hash)))
      }.flatMap { entries =>
        val byEpoch: Map[EpochProgress, SortedSet[AllowSpendExpiryKey]] =
          entries.groupMap(_._1)(_._2).view.mapValues(_.to(SortedSet)).toMap
        byEpoch.toList.parTraverse {
          case (epoch, bucket) =>
            GlobalStateKey
              .expiryIndexKey[F](SystemNamespaceLabel.ExpiryIndexAllowSpends, epoch)
              .map(k => k -> enc[SortedSet[AllowSpendExpiryKey]](bucket))
        }
      }
    }

    // Node-collateral-withdrawal expiry index: expiry = `createdAt + withdrawalTimeLimit`. Skipped entirely when the limit isn't provided.
    val nodeCollateralWithdrawalExpiryIndexF: F[List[(GlobalStateKey, Array[Byte])]] = withdrawalTimeLimit match {
      case None => List.empty[(GlobalStateKey, Array[Byte])].pure[F]
      case Some(limit) =>
        val flat = info.nodeCollateralWithdrawals.toList.flatMap(_.toList).flatMap {
          case (addr, set) => set.toList.map(w => (addr, w))
        }
        flat.parTraverse {
          case (addr, w) =>
            w.event.toHashed.map(h => (w.createdAt |+| limit, NodeCollateralWithdrawalExpiryKey(addr, h.hash)))
        }.flatMap { entries =>
          val byEpoch: Map[EpochProgress, SortedSet[NodeCollateralWithdrawalExpiryKey]] =
            entries.groupMap(_._1)(_._2).view.mapValues(_.to(SortedSet)).toMap
          byEpoch.toList.parTraverse {
            case (epoch, bucket) =>
              GlobalStateKey
                .expiryIndexKey[F](SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals, epoch)
                .map(k => k -> enc[SortedSet[NodeCollateralWithdrawalExpiryKey]](bucket))
          }
        }
    }

    // §3 NIPoPoW S0 historical stake snapshots: one entry per stored eta-period (retention cap = 4).
    // Each entry's value is the scodec-encoded `HistoricalStakeSnapshot` — the combined stake +
    // eta record (Path 1, heap-leak workstream). Covered by `mptRoot` and gets its own per-field
    // subtree root via `FId.HistoricalStakeSnapshots` for efficient NIPoPoW Merkle proofs.
    val historicalStakeSnapshotsF: F[List[(GlobalStateKey, Array[Byte])]] = {
      import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{
        historicalImmutableCodec => historicalStakeSnapshotImmutable
      }
      info.historicalStakeSnapshots.toList.parTraverse {
        case (period, entry) =>
          GlobalStateKey
            .historicalStakeSnapshotsKey[F](period)
            .map(k => k -> enc[HistoricalStakeSnapshot](entry)(historicalStakeSnapshotImmutable))
      }
    }

    // Token-lock expiry index: one bucket per `unlockEpoch` (records with `None` unlock aren't indexed).
    val tokenLockExpiryIndexF: F[List[(GlobalStateKey, Array[Byte])]] = {
      val flat = info.activeTokenLocks.toList.flatMap(_.toList).flatMap {
        case (addr, set) => set.toList.map(l => (addr, l))
      }
      flat.parTraverse {
        case (addr, l) =>
          l.unlockEpoch match {
            case Some(epoch) =>
              l.toHashed.map[Option[(EpochProgress, TokenLockExpiryKey)]](h => Some((epoch, TokenLockExpiryKey(addr, h.hash))))
            case None => Option.empty[(EpochProgress, TokenLockExpiryKey)].pure[F]
          }
      }.map(_.flatten).flatMap { entries =>
        val byEpoch: Map[EpochProgress, SortedSet[TokenLockExpiryKey]] =
          entries.groupMap(_._1)(_._2).view.mapValues(_.to(SortedSet)).toMap
        byEpoch.toList.parTraverse {
          case (epoch, bucket) =>
            GlobalStateKey
              .expiryIndexKey[F](SystemNamespaceLabel.ExpiryIndexTokenLocks, epoch)
              .map(k => k -> enc[SortedSet[TokenLockExpiryKey]](bucket))
        }
      }
    }

    // Currency snapshots encode as two separate keys (Signed[CurrencyIncrementalSnapshot] + CurrencySnapshotInfo) per
    // metagraph address. Delegated to the shared `currencySnapshotEntryBytes` so the producer and the gl1-style follow
    // verifier (`FollowVerifyCore` / `GlobalStateConverter.currencySnapshotFieldRoots`) encode these bytes through ONE code
    // path — byte-identity by construction, not by two implementations kept in lockstep.
    val currencyEntriesF = currencySnapshotEntryBytes[F](info.lastCurrencySnapshots)

    (
      currencyEntriesF,
      updateNodeParametersF,
      priceStateF,
      allowSpendExpiryIndexF,
      tokenLockExpiryIndexF,
      nodeCollateralWithdrawalExpiryIndexF,
      historicalStakeSnapshotsF
    ).mapN {
      (currencyEntries, unpEntries, priceEntries, allowSpendExpiryEntries, tokenLockExpiryEntries, ncwExpiryEntries, histStakeEntries) =>
        val all: Iterable[(GlobalStateKey, Array[Byte])] =
          stateChanHashes ++ txRefs ++ balances ++ currencyProofs ++
            activeAllowSpends ++ activeTokenLocks ++ tokenLockBalances ++
            lastAllowSpendRefs ++ lastTokenLockRefs ++
            activeDelegatedStakes ++ delegatedStakesWithdrawals ++
            activeNodeCollaterals ++ nodeCollateralWithdrawals ++
            metagraphSyncData ++ currencyEntries ++ unpEntries ++ priceEntries ++
            allowSpendExpiryEntries ++ tokenLockExpiryEntries ++ ncwExpiryEntries ++
            histStakeEntries
        all.toMap
    }
  }

  /** Typed scodec-encoded entries for a `StateChangesAccumulator` — the canonical delta form the acceptance manager produces per ordinal.
    *
    * Each field is encoded via its `ImmutableCodec[V]`, the same encoder `syncFromStateChanges` uses when writing to the store. This is the
    * delta-as-bytes representation: the upsert half of `store.update(upserts, removes)` under the MPT-as-primary model. Removals are
    * carried by the accumulator's `removed*Keys` fields; callers combine both halves.
    *
    * Consumes the same codec set as `toAllStateKeyValueBytes(info)` so the two paths produce identical bytes for the same post-state.
    */
  def toAccumulatorBytesDelta[F[_]: Async: Parallel: Hasher: JsonSerializer](
    acc: StateChangesAccumulator
  )(implicit stateProofSelector: StateProofSelector): F[Map[GlobalStateKey, Array[Byte]]] = {
    import io.constellationnetwork.schema.mpt.GlobalStateFieldId._
    import io.constellationnetwork.serde.ImmutableCodec
    import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendRefImmutable}
    import io.constellationnetwork.serde.codecs.instances.CompatCodecs._
    import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
    import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutable}
    import io.constellationnetwork.serde.codecs.instances.MerkleTreeCodecs.proofImmutableCodec
    import io.constellationnetwork.serde.codecs.instances.MetagraphSyncDataInfoCodec.{immutableCodec => metagraphSyncImmutable}
    import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
    import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
    import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

    def enc[V](v: V)(implicit c: ImmutableCodec[V]): Array[Byte] = c.immutableBytes(v).toArray

    val stateChanHashes = acc.lastStateChannelSnapshotHashes.iterator.map {
      case (addr, h) => GlobalStateKey.metagraph(addr, LastStateChannelSnapshotHashes) -> enc(h)
    }.toList
    val txRefs = acc.lastTxRefs.iterator.map { case (addr, r) => GlobalStateKey.hypergraph(LastTxRefs, addr) -> enc(r) }.toList
    val balances = acc.balances.iterator.map { case (addr, b) => GlobalStateKey.hypergraph(Balances, addr) -> enc(b) }.toList
    val currencyProofs = acc.lastCurrencySnapshotsProofs.iterator.map {
      case (addr, p) => GlobalStateKey.metagraph(addr, LastCurrencySnapshotsProofs) -> enc(p)
    }.toList
    val activeAllowSpends = acc.activeAllowSpends.toList.flatMap {
      case (optAddr, inner) =>
        inner.toList.map {
          case (addr, s) =>
            GlobalStateKey.hypergraph(ActiveAllowSpends, optAddr, addr) ->
              enc[SortedSet[Signed[io.constellationnetwork.schema.swap.AllowSpend]]](s)
        }
    }
    val activeTokenLocks = acc.activeTokenLocks.iterator.map {
      case (addr, s) =>
        GlobalStateKey.hypergraph(ActiveTokenLocks, addr) -> enc[SortedSet[Signed[io.constellationnetwork.schema.tokenLock.TokenLock]]](s)
    }.toList
    val tokenLockBalances = acc.tokenLockBalances.toList.flatMap {
      case (tokenAddr, inner) =>
        inner.toList.map { case (holder, bal) => GlobalStateKey.hypergraph(TokenLockBalances, tokenAddr, holder) -> enc(bal) }
    }
    val lastAllowSpendRefs = acc.lastAllowSpendRefs.iterator.map {
      case (addr, r) => GlobalStateKey.hypergraph(LastAllowSpendRefs, addr) -> enc(r)
    }.toList
    val lastTokenLockRefs = acc.lastTokenLockRefs.iterator.map {
      case (addr, r) => GlobalStateKey.hypergraph(LastTokenLockRefs, addr) -> enc(r)
    }.toList
    val activeDelegatedStakes = acc.activeDelegatedStakes.iterator.map {
      case (addr, s) => GlobalStateKey.hypergraph(ActiveDelegatedStakes, addr) -> enc[SortedSet[DelegatedStakeRecord]](s)
    }.toList
    val delegatedStakesWithdrawals = acc.delegatedStakesWithdrawals.iterator.map {
      case (addr, s) =>
        GlobalStateKey.hypergraph(DelegatedStakesWithdrawals, addr) -> enc[SortedSet[PendingDelegatedStakeWithdrawal]](s)
    }.toList
    val activeNodeCollaterals = acc.activeNodeCollaterals.iterator.map {
      case (addr, s) => GlobalStateKey.hypergraph(ActiveNodeCollaterals, addr) -> enc[SortedSet[NodeCollateralRecord]](s)
    }.toList
    val nodeCollateralWithdrawals = acc.nodeCollateralWithdrawals.iterator.map {
      case (addr, s) =>
        GlobalStateKey.hypergraph(NodeCollateralWithdrawals, addr) -> enc[SortedSet[PendingNodeCollateralWithdrawal]](s)
    }.toList
    val metagraphSyncData = acc.metagraphSyncData.iterator.map {
      case (addr, d) => GlobalStateKey.hypergraph(MetagraphSyncData, addr) -> enc(d)
    }.toList

    val currencyEntriesF = acc.lastCurrencySnapshots.toList.parTraverse {
      case (metagraphAddr, Left(fullSnapshot)) =>
        for {
          inc <- CurrencyIncrementalSnapshot.fromCurrencySnapshot(fullSnapshot.value)
          infoEntries <- infoEntryBytes[F](metagraphAddr, fullSnapshot.info.toCurrencySnapshotInfo)
        } yield
          (GlobalStateKey.metagraph(metagraphAddr, LastIncrementalCurrencySnapshots) ->
            enc[Signed[CurrencyIncrementalSnapshot]](Signed(inc, fullSnapshot.proofs))) :: infoEntries
      case (metagraphAddr, Right((inc, snInfo))) =>
        infoEntryBytes[F](metagraphAddr, snInfo).map { infoEntries =>
          (GlobalStateKey.metagraph(metagraphAddr, LastIncrementalCurrencySnapshots) ->
            enc[Signed[CurrencyIncrementalSnapshot]](inc)) :: infoEntries
        }
    }

    val updateNodeParametersF: F[List[(GlobalStateKey, Array[Byte])]] =
      acc.updateNodeParameters.toList.parTraverse {
        case (id, rec) =>
          GlobalStateKey.updateNodeParametersKey[F](id).map(k => k -> enc[(Signed[UpdateNodeParameters], SnapshotOrdinal)](rec))
      }

    val priceStateF: F[List[(GlobalStateKey, Array[Byte])]] =
      acc.priceState.toList.parTraverse {
        case (tp, rec) => GlobalStateKey.priceStateKey[F](tp).map(k => k -> enc[PriceRecord](rec))
      }

    // §3 NIPoPoW S0 historical-stake-snapshots delta: empty on non-boundary ordinals, otherwise the new
    // entry(ies) appended at this boundary. Mirrors the projection in `toAllStateKeyValueBytes`'s
    // `historicalStakeSnapshotsF` so the writer-bytes match the rebuild-bytes byte-for-byte.
    val historicalStakeSnapshotsF: F[List[(GlobalStateKey, Array[Byte])]] = {
      import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{
        historicalImmutableCodec => historicalStakeSnapshotImmutable
      }
      acc.historicalStakeSnapshots.toList.parTraverse {
        case (period, entry) =>
          GlobalStateKey
            .historicalStakeSnapshotsKey[F](period)
            .map(k => k -> enc[HistoricalStakeSnapshot](entry)(historicalStakeSnapshotImmutable))
      }
    }

    (currencyEntriesF, updateNodeParametersF, priceStateF, historicalStakeSnapshotsF).mapN {
      (currencyEntries, unpEntries, priceEntries, histStakeEntries) =>
        val all: Iterable[(GlobalStateKey, Array[Byte])] =
          stateChanHashes ++ txRefs ++ balances ++ currencyProofs ++
            activeAllowSpends ++ activeTokenLocks ++ tokenLockBalances ++
            lastAllowSpendRefs ++ lastTokenLockRefs ++
            activeDelegatedStakes ++ delegatedStakesWithdrawals ++
            activeNodeCollaterals ++ nodeCollateralWithdrawals ++
            metagraphSyncData ++ currencyEntries.flatten ++ unpEntries ++ priceEntries ++
            histStakeEntries
        all.toMap
    }
  }

  /** Hex-keyed delta derived from a `StateChangesAccumulator` — pairs well with `MptStore.allEntriesAsBytes` (which is `Map[Hex,
    * Array[Byte]]`) for independent state-replay verification: `expected = (prevBytes -- removes) ++ upserts`.
    *
    * `preSyncBytes` is required because the system expiry-index partitions are written via per-bucket read-modify-write (see
    * `applySystemIndexDelta`) — the accumulator only carries the per-epoch *delta* (adds/removes), not the resulting bucket bytes. Without
    * the pre-sync map this helper would miss any bucket changes and produce a state-replay root that disagrees with the actual MPT after
    * `syncFromStateChanges` runs.
    */
  def toAccumulatorHexDelta[F[_]: Async: Parallel: Hasher: JsonSerializer](
    acc: StateChangesAccumulator,
    preSyncBytes: Map[Hex, Array[Byte]],
    // S1 BASE-ANCHORING for the `Mg*` removal set — MUST mirror the writer's `AcceptanceMptStateChanges.applyStateChanges`
    // `currencyInfoRemovalPrior` (GSAM passes `Some(overlay.base.allEntriesAsBytes)` in `shardedInfoMode`, else `None`). The per-MG
    // `Mg*` removals are computed against THIS prior (the FINALIZED BASE), not `preSyncBytes` (the branch tip). `None` ⇒ branch==base
    // (pipelineDepth=1 / numShards=1, the production regime), where it equals `preSyncBytes`.
    mgRemovalPriorBytes: Option[Map[Hex, Array[Byte]]] = None
  )(implicit stateProofSelector: StateProofSelector): F[(Map[Hex, Array[Byte]], Set[Hex])] =
    for {
      typedUpserts <- toAccumulatorBytesDelta[F](acc)
      upsertsHex <- typedUpserts.toList.parTraverse { case (k, v) => GlobalStateKey.toHex[F](k).map(_ -> v) }.map(_.toMap)
      removalKeys <- toAccumulatorRemovalKeys[F](acc)
      removalsHex <- removalKeys.toList.parTraverse(GlobalStateKey.toHex[F]).map(_.toSet)
      asExp <- replayExpiryIndexDelta[F, AllowSpendExpiryKey](
        SystemNamespaceLabel.ExpiryIndexAllowSpends,
        acc.allowSpendExpiryIndex,
        preSyncBytes
      )
      tlExp <- replayExpiryIndexDelta[F, TokenLockExpiryKey](
        SystemNamespaceLabel.ExpiryIndexTokenLocks,
        acc.tokenLockExpiryIndex,
        preSyncBytes
      )
      ncwExp <- replayExpiryIndexDelta[F, NodeCollateralWithdrawalExpiryKey](
        SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals,
        acc.nodeCollateralWithdrawalExpiryIndex,
        preSyncBytes
      )
      asAddrIdx <- replayActiveAddressIndexDelta[F](
        GlobalStateFieldId.LastAllowSpendRefs,
        acc.lastAllowSpendRefs.keySet.toSet,
        Set.empty,
        preSyncBytes
      )
      tlAddrIdx <- replayActiveAddressIndexDelta[F](
        GlobalStateFieldId.LastTokenLockRefs,
        acc.lastTokenLockRefs.keySet.toSet,
        Set.empty,
        preSyncBytes
      )
      txAddrIdx <- replayActiveAddressIndexDelta[F](
        GlobalStateFieldId.LastTxRefs,
        acc.lastTxRefs.keySet.toSet,
        Set.empty,
        preSyncBytes
      )
      balAddrIdx <- replayActiveAddressIndexDelta[F](
        GlobalStateFieldId.Balances,
        acc.balances.keySet.toSet,
        Set.empty,
        preSyncBytes
      )
      scHashesAddrIdx <- replayActiveAddressIndexDelta[F](
        GlobalStateFieldId.LastStateChannelSnapshotHashes,
        acc.lastStateChannelSnapshotHashes.keySet.toSet,
        Set.empty,
        preSyncBytes
      )
      currSnapsAddrIdx <- replayActiveAddressIndexDelta[F](
        GlobalStateFieldId.LastCurrencySnapshots,
        acc.lastCurrencySnapshots.keySet.toSet,
        Set.empty,
        preSyncBytes
      )
      tlbAddrPairIdx <- replayAddressPairIndexDelta[F](
        GlobalStateFieldId.TokenLockBalances,
        acc.tokenLockBalances.iterator.flatMap {
          case (mid, inner) => inner.keysIterator.map(holder => (mid, holder))
        }.toSet,
        acc.removedTokenLockBalanceKeys,
        preSyncBytes
      )
      // [Mg* REMOVAL PARITY] Mirror the incremental writer's per-MG `CurrencySnapshotInfo` sub-entry removals in the verify-replay.
      // `writeCurrencyInfo` removes the `Mg*` keys dropped this ordinal (`infoRemovalKeys` vs the prior info — e.g. an
      // `MgActiveTokenLocks` entry when an in-metagraph token-lock expires). `toAccumulatorRemovalKeys` above emits only the TOP-LEVEL
      // global removals, so without this the replay keeps the stale `Mg*` entry and `(preSyncBytes -- removes) ++ upserts` diverges from
      // the (correct) incremental `postBytes` → false-positive `mptConsistency=DIVERGED` crash (proven live: ord-372/435
      // `divergedFields=[MgActiveTokenLocks:1]`). Computed as a pure HEX-KEY-SET DIFF (no decode / reconstruction — robust): for each MG,
      // its `Mg*` keys present in `preSyncBytes` but ABSENT from the new upserts (`upsertsHex`, written via the single `infoEntryBytes`
      // encoder) are exactly the dropped sub-entries — the same set `infoRemovalKeys` computes, without re-decoding the prior bytes.
      // `activeAllowSpends` (fieldId-7) is intentionally excluded — its removals are the top-level `removedAllowSpendKeys` path, matching
      // `infoRemovalKeys`'s exclusion. Verify-side only — no consensus bytes move (the writer / `postBytes` were always correct).
      infoRemovalsHex <- acc.lastCurrencySnapshots.keys.toList.parTraverse { mgAddr =>
        List(
          GlobalStateFieldId.MgBalances,
          GlobalStateFieldId.MgLastTxRefs,
          GlobalStateFieldId.MgLastFeeTxRefs,
          GlobalStateFieldId.MgLastAllowSpendRefs,
          GlobalStateFieldId.MgLastTokenLockRefs,
          GlobalStateFieldId.MgActiveTokenLocks,
          GlobalStateFieldId.MgLastMessages,
          GlobalStateFieldId.MgGlobalSnapshotSyncView
        ).traverse(sub => GlobalStateKey.metagraphFieldPrefix[F](mgAddr, sub)).map { prefixes =>
          val isMgEntry = (k: Hex) => prefixes.exists(p => k.value.startsWith(p.value))
          // Anchor the prior `Mg*` key set on the SAME prior the writer used (base in shardedInfoMode, else branch==preSyncBytes).
          mgRemovalPriorBytes.getOrElse(preSyncBytes).keySet.filter(isMgEntry) -- upsertsHex.keySet.filter(isMgEntry)
        }
      }.map(_.flatten.toSet)
    } yield
      (
        upsertsHex ++ asExp._1 ++ tlExp._1 ++ ncwExp._1 ++ asAddrIdx._1 ++ tlAddrIdx._1 ++ txAddrIdx._1 ++ balAddrIdx._1 ++
          scHashesAddrIdx._1 ++ currSnapsAddrIdx._1 ++ tlbAddrPairIdx._1,
        removalsHex ++ asExp._2 ++ tlExp._2 ++ ncwExp._2 ++ asAddrIdx._2 ++ tlAddrIdx._2 ++ txAddrIdx._2 ++ balAddrIdx._2 ++
          scHashesAddrIdx._2 ++ currSnapsAddrIdx._2 ++ tlbAddrPairIdx._2 ++ infoRemovalsHex
      )

  /** Mirror of `applyActiveAddressIndexDelta`'s read-modify-write for the verify replay path. Decode the pre-sync sidecar entry, apply
    * adds/removes, decide upsert / remove / no-op the same way the in-store sync does. Skip-on-noop matches the writer so the resulting
    * `(prevBytes -- removes) ++ upserts` equals the post-sync entry set bit-for-bit.
    */
  private def replayActiveAddressIndexDelta[F[_]: Async: Hasher](
    fieldId: GlobalStateFieldId,
    added: Set[Address],
    removed: Set[Address],
    preSyncBytes: Map[Hex, Array[Byte]]
  ): F[(Map[Hex, Array[Byte]], Set[Hex])] =
    if (added.isEmpty && removed.isEmpty)
      (Map.empty[Hex, Array[Byte]], Set.empty[Hex]).pure[F]
    else
      for {
        key <- GlobalStateKey.activeAddressIndexKey[F](fieldId)
        hexKey <- GlobalStateKey.toHex[F](key)
      } yield {
        val existing: SortedSet[Address] = preSyncBytes.get(hexKey) match {
          case Some(bytes) =>
            addressSetImmutableCodec.fromImmutableBytes(ByteVector.view(bytes)).getOrElse(SortedSet.empty[Address])
          case None => SortedSet.empty[Address]
        }
        val merged: SortedSet[Address] = (existing ++ added) -- removed
        if (merged.isEmpty && existing.nonEmpty)
          (Map.empty[Hex, Array[Byte]], Set(hexKey))
        else if (merged.nonEmpty && merged != existing)
          (Map(hexKey -> addressSetImmutableCodec.immutableBytes(merged).toArray), Set.empty[Hex])
        else
          (Map.empty[Hex, Array[Byte]], Set.empty[Hex])
      }

  /** Mirror of `applyAddressPairIndexDelta`'s read-modify-write for the verify replay path. Decode the pre-sync sidecar entry, apply
    * adds/removes, decide upsert / remove / no-op identically to the in-store writer so `(prevBytes -- removes) ++ upserts` matches the
    * post-sync entry set bit-for-bit.
    */
  private def replayAddressPairIndexDelta[F[_]: Async: Hasher](
    fieldId: GlobalStateFieldId,
    added: Set[(Address, Address)],
    removed: Set[(Address, Address)],
    preSyncBytes: Map[Hex, Array[Byte]]
  ): F[(Map[Hex, Array[Byte]], Set[Hex])] =
    if (added.isEmpty && removed.isEmpty)
      (Map.empty[Hex, Array[Byte]], Set.empty[Hex]).pure[F]
    else
      for {
        key <- GlobalStateKey.activeAddressIndexKey[F](fieldId)
        hexKey <- GlobalStateKey.toHex[F](key)
      } yield {
        val existing: SortedSet[(Address, Address)] = preSyncBytes.get(hexKey) match {
          case Some(bytes) =>
            addressPairSetImmutableCodec
              .fromImmutableBytes(ByteVector.view(bytes))
              .getOrElse(SortedSet.empty[(Address, Address)])
          case None => SortedSet.empty[(Address, Address)]
        }
        val merged: SortedSet[(Address, Address)] = (existing ++ added) -- removed
        if (merged.isEmpty && existing.nonEmpty)
          (Map.empty[Hex, Array[Byte]], Set(hexKey))
        else if (merged.nonEmpty && merged != existing)
          (Map(hexKey -> addressPairSetImmutableCodec.immutableBytes(merged).toArray), Set.empty[Hex])
        else
          (Map.empty[Hex, Array[Byte]], Set.empty[Hex])
      }

  /** Mirror of `applySystemIndexDelta`'s read-modify-write for the verify replay path. For each touched epoch bucket: decode the pre-sync
    * bytes (if any), apply adds/removes, and decide upsert / remove / no-op the same way the in-store sync does. Skip-on-noop matches the
    * writer so the resulting `(prevBytes -- removes) ++ upserts` equals the post-sync entry set bit-for-bit.
    */
  private def replayExpiryIndexDelta[F[_]: Async: Parallel: Hasher, K: Order](
    label: SystemNamespaceLabel,
    delta: SystemIndexDelta[K],
    preSyncBytes: Map[Hex, Array[Byte]]
  )(implicit codec: ImmutableCodec[SortedSet[K]]): F[(Map[Hex, Array[Byte]], Set[Hex])] = delta match {
    case eb: SystemIndexDelta.EpochBucket[K] if eb.isEmpty =>
      (Map.empty[Hex, Array[Byte]], Set.empty[Hex]).pure[F]
    case eb: SystemIndexDelta.EpochBucket[K] =>
      implicit val ordering: Ordering[K] = Order[K].toOrdering
      eb.touchedEpochs.toList.parTraverse { epoch =>
        for {
          key <- GlobalStateKey.expiryIndexKey[F](label, epoch)
          hexKey <- GlobalStateKey.toHex[F](key)
        } yield {
          val existing: SortedSet[K] = preSyncBytes.get(hexKey) match {
            case Some(bytes) =>
              codec.fromImmutableBytes(ByteVector.view(bytes)).getOrElse(SortedSet.empty[K])
            case None => SortedSet.empty[K]
          }
          val toAdd = eb.adds.getOrElse(epoch, Set.empty[K])
          val toRemove = eb.removes.getOrElse(epoch, Set.empty[K])
          val merged: SortedSet[K] = (existing ++ toAdd) -- toRemove
          if (merged.isEmpty && existing.nonEmpty)
            (Map.empty[Hex, Array[Byte]], Set(hexKey))
          else if (merged.nonEmpty && merged != existing)
            (Map(hexKey -> codec.immutableBytes(merged).toArray), Set.empty[Hex])
          else
            (Map.empty[Hex, Array[Byte]], Set.empty[Hex])
        }
      }.map { results =>
        val merged = results.foldLeft((Map.empty[Hex, Array[Byte]], Set.empty[Hex])) {
          case ((accU, accR), (u, r)) => (accU ++ u, accR ++ r)
        }
        merged
      }
  }

  /** Removal keys derived from a `StateChangesAccumulator` — the delete half of `store.update(upserts, removes)` under MPT-as-primary.
    *
    * Lifted into `F` so historical-stake-snapshot keys (which require `Hasher[F]` to derive their `userNamespace` hash via
    * `historicalStakeSnapshotsKey[F]`) can be folded into the result set alongside the F-free keys.
    */
  def toAccumulatorRemovalKeys[F[_]: Async: Parallel: Hasher](acc: StateChangesAccumulator): F[Set[GlobalStateKey]] = {
    import io.constellationnetwork.schema.mpt.GlobalStateFieldId._
    val allowSpendKeys = acc.removedAllowSpendKeys.toList.map {
      case (metagraphIdOpt, address) => GlobalStateKey.hypergraph(ActiveAllowSpends, metagraphIdOpt, address)
    }
    val tokenLockKeys = acc.removedTokenLockKeys.toList.map(addr => GlobalStateKey.hypergraph(ActiveTokenLocks, addr))
    val tokenLockBalanceKeys = acc.removedTokenLockBalanceKeys.toList.map {
      case (mid, holder) => GlobalStateKey.hypergraph(TokenLockBalances, mid, holder)
    }
    val delegatedStakeKeys = acc.removedDelegatedStakeKeys.toList.map(addr => GlobalStateKey.hypergraph(ActiveDelegatedStakes, addr))
    val delegatedStakeWithdrawalKeys =
      acc.removedDelegatedStakeWithdrawalKeys.toList.map(addr => GlobalStateKey.hypergraph(DelegatedStakesWithdrawals, addr))
    val nodeCollateralKeys = acc.removedNodeCollateralKeys.toList.map(addr => GlobalStateKey.hypergraph(ActiveNodeCollaterals, addr))
    val nodeCollateralWithdrawalKeys =
      acc.removedNodeCollateralWithdrawalKeys.toList.map(addr => GlobalStateKey.hypergraph(NodeCollateralWithdrawals, addr))
    val pureKeys = (allowSpendKeys ++ tokenLockKeys ++ tokenLockBalanceKeys ++
      delegatedStakeKeys ++ delegatedStakeWithdrawalKeys ++
      nodeCollateralKeys ++ nodeCollateralWithdrawalKeys).toSet
    acc.removedHistoricalStakeSnapshotKeys.toList
      .parTraverse(period => GlobalStateKey.historicalStakeSnapshotsKey[F](period))
      .map(historicalKeys => pureKeys ++ historicalKeys.toSet)
  }

  /** Canonical per-field MPT subtree-root computation, shared by EVERY path that derives a `stateProof.<field>Proof` value: the producer
    * path (`GlobalSnapshotInfo.stateProofBuilder` → [[buildPerFieldMptRoots]]), the overlay/GSAM path
    * (`GlobalSnapshotInfo.mptStateProofFromBytes`), and the gl1 follow-mirror verifier
    * (`io.constellationnetwork.schema.nakamoto.follow.FollowVerifyCore.verifyFieldRoots`).
    *
    * A field's subtree root is '''not''' an extraction from the global trie — it is the `rootHash` of a standalone MPT built from ONLY that
    * field's `(hex key → value bytes)` entries via `MerklePatriciaTrie.makeParallelFromBytes`. Centralizing it here means the three callers
    * can never drift on either the build algorithm or the empty-field convention, which is the byte-identity contract the follow path's
    * FIELD-ROOT-MATCH verify depends on (see `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`, "correct-by-design contract").
    *
    * '''Empty-field convention (determinism-load-bearing).''' An empty field maps to [[Hash.empty]] — NOT to
    * `makeParallelFromBytes(Map.empty).rootHash` (which is the digest of an empty `Branch`, a different value). Both gl0 paths
    * short-circuit the empty case to `Hash.empty` (producer: this guard; overlay: absent fields default via `getOrElse(_, Hash.empty)`), so
    * the verifier must reproduce exactly that to match a signed root for a field that became empty.
    */
  def fieldRootFromBytes[F[_]: Parallel: Async: Hasher: JsonSerializer](fieldEntries: Map[Hex, Array[Byte]]): F[Hash] =
    if (fieldEntries.isEmpty) Hash.empty.pure[F]
    else MerklePatriciaTrie.makeParallelFromBytes[F](fieldEntries).map(_.rootHash.value)

  /** Emit the UNROLLED per-entry MPT key→bytes for ONE metagraph's `CurrencySnapshotInfo` — the 8 `Mg*` sub-field partitions that REPLACE
    * the monolithic fieldId-6 blob (`docs/nakamoto/UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md`). One entry per account / holder / messageType
    * / peer, keyed `metagraphEntry(mgAddr, MgXxx, key)`; each value carries its own typed entry key `(key, value)` because `toHex` HASHES
    * the entry key (lossy), so reconstruction recovers the logical key from the value via a prefix scan.
    *
    * '''`activeAllowSpends` is NOT emitted here''' — it stays in the fieldId-7 `ActiveAllowSpends` partition (written from the GSI's
    * top-level `activeAllowSpends`, already per-MG unrolled + read cross-shard by `SpendActionValidator`). So `infoRoot` covers these 8
    * sub-fields; `activeAllowSpends` is committed separately via the fieldId-7 `activeAllowSpends` state-proof slot.
    *
    * '''Single source of truth.''' This is the ONE encoder for the unrolled info bytes; every writer (full-state bytes/JSON, bootstrap
    * seed, incremental delta, overlay) routes through it so the bytes — and therefore `infoRoot` — are byte-identical on every path
    * (split-safety).
    */
  def infoEntryBytes[F[_]: Sync: Hasher](
    mgAddr: Address,
    info: CurrencySnapshotInfo
  ): F[List[(GlobalStateKey, Array[Byte])]] = {
    import GlobalStateFieldId._
    def enc[V](v: V)(implicit c: ImmutableCodec[V]): Array[Byte] = c.immutableBytes(v).toArray
    def opt[K, V](m: Option[SortedMap[K, V]]): List[(K, V)] = m.fold(List.empty[(K, V)])(_.toList)

    // Address-keyed sub-fields — `metagraphEntry` is pure (hashing deferred to `toHex`). The value type drives implicit codec resolution.
    val addressKeyed: List[(GlobalStateKey, Array[Byte])] =
      info.balances.toList.map { case (a, b) => GlobalStateKey.metagraphEntry(mgAddr, MgBalances, a) -> enc((a, b)) } ++
        info.lastTxRefs.toList.map { case (a, r) => GlobalStateKey.metagraphEntry(mgAddr, MgLastTxRefs, a) -> enc((a, r)) } ++
        opt(info.lastFeeTxRefs).map { case (a, r) => GlobalStateKey.metagraphEntry(mgAddr, MgLastFeeTxRefs, a) -> enc((a, r)) } ++
        opt(info.lastAllowSpendRefs).map {
          case (a, r) => GlobalStateKey.metagraphEntry(mgAddr, MgLastAllowSpendRefs, a) -> enc((a, r))
        } ++
        opt(info.lastTokenLockRefs).map {
          case (a, r) => GlobalStateKey.metagraphEntry(mgAddr, MgLastTokenLockRefs, a) -> enc((a, r))
        } ++
        opt(info.activeTokenLocks).map { case (a, s) => GlobalStateKey.metagraphEntry(mgAddr, MgActiveTokenLocks, a) -> enc((a, s)) }

    // Non-Address-keyed sub-fields — `metagraphEntryHashed` folds the canonical-string entry key into the user slot (needs F to hash).
    val messagesF: F[List[(GlobalStateKey, Array[Byte])]] =
      opt(info.lastMessages).traverse {
        case (mt, m) => GlobalStateKey.metagraphEntryHashed[F](mgAddr, MgLastMessages, mt.value).map(_ -> enc((mt, m)))
      }
    val syncViewF: F[List[(GlobalStateKey, Array[Byte])]] =
      opt(info.globalSnapshotSyncView).traverse {
        case (p, s) => GlobalStateKey.metagraphEntryHashed[F](mgAddr, MgGlobalSnapshotSyncView, p.value.value).map(_ -> enc((p, s)))
      }

    (messagesF, syncViewF).mapN((m, s) => addressKeyed ++ m ++ s)
  }

  /** JSON twin of [[infoEntryBytes]] for the legacy `Map[GlobalStateKey, Json]` full-state path ([[convertCurrencySnapshots]] →
    * [[toAllStateKeyValuePairs]] → `MptBuilderOps.buildMpt`). Emits the IDENTICAL `Mg*` keys (one entry per account / holder / messageType
    * / peer) with each value as the same typed `(entryKey, value)` tuple, `.asJson`. The JSON build path is a separate root computation
    * (NOT byte-compared against the typed `insert[V]` root — see `syncFromGlobalSnapshotInfo`'s scaladoc), so this need only key-mirror the
    * bytes encoder for consistency; the key construction is structurally identical so the two cannot drift. `activeAllowSpends` stays
    * fieldId-7.
    */
  def infoEntryJson[F[_]: Sync: Hasher](
    mgAddr: Address,
    info: CurrencySnapshotInfo
  ): F[List[(GlobalStateKey, Json)]] = {
    import GlobalStateFieldId._
    def opt[K, V](m: Option[SortedMap[K, V]]): List[(K, V)] = m.fold(List.empty[(K, V)])(_.toList)

    val addressKeyed: List[(GlobalStateKey, Json)] =
      info.balances.toList.map { case (a, b) => GlobalStateKey.metagraphEntry(mgAddr, MgBalances, a) -> (a, b).asJson } ++
        info.lastTxRefs.toList.map { case (a, r) => GlobalStateKey.metagraphEntry(mgAddr, MgLastTxRefs, a) -> (a, r).asJson } ++
        opt(info.lastFeeTxRefs).map { case (a, r) => GlobalStateKey.metagraphEntry(mgAddr, MgLastFeeTxRefs, a) -> (a, r).asJson } ++
        opt(info.lastAllowSpendRefs).map {
          case (a, r) => GlobalStateKey.metagraphEntry(mgAddr, MgLastAllowSpendRefs, a) -> (a, r).asJson
        } ++
        opt(info.lastTokenLockRefs).map {
          case (a, r) => GlobalStateKey.metagraphEntry(mgAddr, MgLastTokenLockRefs, a) -> (a, r).asJson
        } ++
        opt(info.activeTokenLocks).map { case (a, s) => GlobalStateKey.metagraphEntry(mgAddr, MgActiveTokenLocks, a) -> (a, s).asJson }

    val messagesF: F[List[(GlobalStateKey, Json)]] =
      opt(info.lastMessages).traverse {
        case (mt, m) => GlobalStateKey.metagraphEntryHashed[F](mgAddr, MgLastMessages, mt.value).map(_ -> (mt, m).asJson)
      }
    val syncViewF: F[List[(GlobalStateKey, Json)]] =
      opt(info.globalSnapshotSyncView).traverse {
        case (p, s) => GlobalStateKey.metagraphEntryHashed[F](mgAddr, MgGlobalSnapshotSyncView, p.value.value).map(_ -> (p, s).asJson)
      }

    (messagesF, syncViewF).mapN((m, s) => addressKeyed ++ m ++ s)
  }

  /** Canonical typed MPT entries for the `lastCurrencySnapshots` GSI field — the EXACT producer encoding gl0 writes in
    * [[toAllStateKeyValueBytes]] / [[convertCurrencySnapshots]]: each `Address` entry expands to `LastIncrementalCurrencySnapshots`
    * (fieldId 5, the `Signed[CurrencyIncrementalSnapshot]`) PLUS the UNROLLED per-entry `Mg*` info sub-fields (via [[infoEntryBytes]],
    * replacing the old monolithic `LastCurrencySnapshotInfo` blob). A `Left` (genesis full snapshot) is reduced to its incremental form
    * exactly as gl0 does (`CurrencyIncrementalSnapshot.fromCurrencySnapshot` + `info.toCurrencySnapshotInfo`), then its info unrolls the
    * same way.
    *
    * Factored out of [[toAllStateKeyValueBytes]] so the gl1-style follow verifier
    * ([[io.constellationnetwork.schema.nakamoto.follow.FollowVerifyCore.verifyFieldRoots]]) recomputes the currency-snapshot subtree roots
    * through gl0's EXACT byte-production path rather than a re-implementation — the byte-identity contract the field-root-match verify
    * depends on (see `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`). `toAllStateKeyValueBytes`'s inline `currencyEntriesF` MUST stay
    * in lockstep with this method.
    *
    * `lastCurrencySnapshots` SPLITS into the signed `GlobalSnapshotStateProof` field-4 slot `lastCurrencySnapshotsProof`
    * (`CurrencySnapshotMptRoots`): `incrementalRoot` (fieldId 5) + `infoRoot` (the UNION over the 8 `Mg*` `infoSubFields`); both are also
    * covered transitively by the global `mptRoot`. This callable exists so the follower can recompute those bytes byte-identically to gl0.
    */
  def currencySnapshotEntryBytes[F[_]: Async: Parallel: Hasher: JsonSerializer](
    data: SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]
  )(
    implicit stateProofSelector: StateProofSelector
  ): F[Map[GlobalStateKey, Array[Byte]]] = {
    def enc[V](v: V)(implicit c: ImmutableCodec[V]): Array[Byte] = c.immutableBytes(v).toArray
    data.toList.parTraverse {
      case (metagraphAddr, Left(fullSnapshot)) =>
        for {
          inc <- CurrencyIncrementalSnapshot.fromCurrencySnapshot(fullSnapshot.value)
          infoEntries <- infoEntryBytes[F](metagraphAddr, fullSnapshot.info.toCurrencySnapshotInfo)
        } yield
          (GlobalStateKey.metagraph(metagraphAddr, GlobalStateFieldId.LastIncrementalCurrencySnapshots) ->
            enc[Signed[CurrencyIncrementalSnapshot]](Signed(inc, fullSnapshot.proofs))) :: infoEntries
      case (metagraphAddr, Right((inc, snInfo))) =>
        infoEntryBytes[F](metagraphAddr, snInfo).map { infoEntries =>
          (GlobalStateKey.metagraph(metagraphAddr, GlobalStateFieldId.LastIncrementalCurrencySnapshots) ->
            enc[Signed[CurrencyIncrementalSnapshot]](inc)) :: infoEntries
        }
    }.map(_.flatten.toMap)

  }

  /** The pair of MPT subtree roots for a `lastCurrencySnapshots` map: `incrementalRoot` (fieldId 5) and `infoRoot` (the UNION over the 8
    * `Mg*` `infoSubFields`), computed by encoding via [[currencySnapshotEntryBytes]] (gl0's exact producer bytes), grouping by `fieldId`,
    * hexing the keys, and routing each group through [[fieldRootFromBytes]] — the SAME callable that backs every other per-field root.
    * Empty info ⇒ `infoRoot = Hash.empty` (the [[fieldRootFromBytes]] empty convention). Used by the follow verifier to recompute-match the
    * cl1/dl1-consumed `lastCurrencySnapshots` field deterministically. The union grouping MUST stay identical to the producer-side
    * `GlobalSnapshotInfo.stateProofBuilder` / `mptStateProofFromBytes` infoRoot computation.
    */
  def currencySnapshotFieldRoots[F[_]: Async: Parallel: Hasher: JsonSerializer](
    data: SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]
  )(
    implicit stateProofSelector: StateProofSelector
  ): F[(Hash, Hash)] =
    currencySnapshotEntryBytes[F](data).flatMap { typed =>
      def rootForFields(pred: GlobalStateFieldId => Boolean): F[Hash] =
        typed.toList.filter { case (k, _) => pred(k.fieldId) }.parTraverse { case (k, v) => GlobalStateKey.toHex[F](k).map(_ -> v) }
          .map(_.toMap)
          .flatMap(fieldRootFromBytes[F])
      (
        rootForFields(_ == GlobalStateFieldId.LastIncrementalCurrencySnapshots),
        rootForFields(GlobalStateFieldId.infoSubFields.contains)
      ).tupled
    }

  /** True iff a `GlobalStateKey.fieldId` belongs to the per-MG shard-checkpoint commitment ([[currencySnapshotMgEntries]] /
    * [[currencySnapshotMgRoot]]): the fieldId-5 incremental PLUS the 8 `infoSubFields` `Mg*` partitions — i.e. the SAME field set the flat
    * `(incrementalRoot, infoRoot)` pair covered (`incrementalRoot` = fieldId-5, `infoRoot` = UNION over `infoSubFields`). DELIBERATELY
    * excludes `MgGlobalSnapshotSyncView` (fieldId 32): it is NOT in `infoSubFields` and is observation-dependent (the producer accumulates
    * it under the full committee, a re-deriving verifier under only the 2/3 signers — #259/cause-2), so folding it into the consensus root
    * makes the root non-deterministic across nodes and re-froze the sharded per-MG adoption. Keeping the field set identical to
    * `currencySnapshotFieldRoots` is what preserves PIN-1's determinism contract under the new commitment shape. See `infoSubFields`.
    */
  private def isCurrencyMgCommitmentField(fieldId: GlobalStateFieldId): Boolean =
    fieldId == GlobalStateFieldId.LastIncrementalCurrencySnapshots || GlobalStateFieldId.infoSubFields.contains(fieldId)

  /** The hex-keyed component entries committed by the per-MG shard-checkpoint root ([[currencySnapshotMgRoot]]). Encodes via
    * [[currencySnapshotEntryBytes]] (gl0's EXACT producer bytes — the single byte source `currencySnapshotFieldRoots` also uses), hexes
    * each key through `GlobalStateKey.toHex`, and keeps ONLY the [[isCurrencyMgCommitmentField]] entries (fieldId-5 incremental + the 8
    * `infoSubFields` `Mg*` sub-fields; field-32 sync-view dropped for determinism). Each surviving entry is a distinct full-length MPT key
    * — one leaf per `(metagraphAddr, subField, account)` plus the per-MG incremental — so the resulting trie is COMPONENT-ADDRESSABLE: a
    * single account in a single field is an independently-provable leaf.
    *
    * This is the single source of truth for the bytes that back BOTH the committed root (producer + follower) AND the inclusion-proof trie
    * (`ShardSubtreeProofService.generateProofForMetagraph` builds the proof over a trie made from these SAME bytes) — byte-identity by
    * construction, not by two implementations kept in lockstep.
    */
  def currencySnapshotMgEntries[F[_]: Async: Parallel: Hasher: JsonSerializer](
    data: SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]
  )(
    implicit stateProofSelector: StateProofSelector
  ): F[Map[Hex, Array[Byte]]] =
    currencySnapshotEntryBytes[F](data).flatMap { typed =>
      typed.toList.filter { case (k, _) => isCurrencyMgCommitmentField(k.fieldId) }.parTraverse {
        case (k, v) => GlobalStateKey.toHex[F](k).map(_ -> v)
      }
        .map(_.toMap)
    }

  /** PIN-1 component-addressable per-MG shard-checkpoint root: the `rootHash` of a standalone MPT built from [[currencySnapshotMgEntries]]
    * (the MG's fieldId-5 incremental + `infoSubFields` `Mg*` entries, one leaf per account). This REPLACES the layer-mismatched flat
    * `Hasher.hash((incrementalRoot, infoRoot))` that `perMetagraphMptRoots(mg)` previously carried: that flat 2-tuple hash could not back a
    * single-leaf inclusion proof (the proof's witness chain hashes up to an MPT root, not to a hash-of-a-pair), so
    * `ShardSubtreeProofService.verifyProof` could never witness a leaf against it. The new root IS a real MPT root, so a standard
    * `MerklePatriciaInclusionProof` over the MG sub-trie witnesses any single `(field, account)` leaf against it — and the per-field
    * subtree root is an internal node digest committed transitively, so the spec's "a single field-root (and a single account within it)"
    * holds in ONE proof.
    *
    * '''Determinism (the byte-identity contract).''' Routes through [[fieldRootFromBytes]] — the SAME
    * `MerklePatriciaTrie.makeParallelFromBytes` build + `Hash.empty`-on-empty convention every other per-field / global root uses — over
    * [[currencySnapshotMgEntries]] (gl0's exact producer bytes, field-32-filtered). No node-local state enters, so producer + every
    * verifier compute the byte-identical root. The THREE PIN-1 sites (producer `ShardCheckpointWiring.reExecDerivationWithDiff`, follower
    * `GlobalSnapshotAcceptanceManager.deriveAdoptedCurrencyState`, proof verifier/generator `ShardSubtreeProofService`) MUST all call THIS
    * method so they stay byte-identical.
    *
    * '''numShards = 1 is untouched.''' At the production default `numShards = 1` gl0 re-executes and never builds `perMetagraphMptRoots`
    * (`ShardCheckpointWiring.acceptanceDeps` returns `None`); this method is on the sharded path only.
    */
  def currencySnapshotMgRoot[F[_]: Async: Parallel: Hasher: JsonSerializer](
    data: SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]
  )(
    implicit stateProofSelector: StateProofSelector
  ): F[Hash] =
    currencySnapshotMgEntries[F](data).flatMap(fieldRootFromBytes[F])

  /** DIAGNOSTIC-ONLY (version-model per-MG-root debug): per-sub-field root breakdown of a per-MG currency root, so an ADOPT-VERIFY /
    * re-exec root MISMATCH can be pinned to the EXACT diverging half (incremental vs info) and `Mg*` sub-field. NOT consensus — pure
    * logging. Mirrors [[currencySnapshotFieldRoots]]'s byte path so committee + gl0 outputs are directly comparable.
    */
  def currencySnapshotFieldRootsDiag[F[_]: Async: Parallel: Hasher: JsonSerializer](
    data: SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]
  )(
    implicit stateProofSelector: StateProofSelector
  ): F[String] =
    currencySnapshotEntryBytes[F](data).flatMap { typed =>
      val entries = typed.toList
      def rootFor(pred: GlobalStateFieldId => Boolean): F[Hash] =
        entries.filter { case (k, _) => pred(k.fieldId) }.parTraverse { case (k, v) => GlobalStateKey.toHex[F](k).map(_ -> v) }
          .map(_.toMap)
          .flatMap(fieldRootFromBytes[F])
      def cnt(pred: GlobalStateFieldId => Boolean): Int = entries.count { case (k, _) => pred(k.fieldId) }
      for {
        incR <- rootFor(_ == GlobalStateFieldId.LastIncrementalCurrencySnapshots)
        infoR <- rootFor(GlobalStateFieldId.infoSubFields.contains)
        perSub <- GlobalStateFieldId.infoSubFields.toList.sortBy(_.toInt).traverse { s =>
          rootFor(_ == s).map(r => s"${s.toInt}=${r.value.take(8)}(${cnt(_ == s)})")
        }
      } yield
        s"inc=${incR.value.take(8)}(${cnt(_ == GlobalStateFieldId.LastIncrementalCurrencySnapshots)}) " +
          s"info=${infoR.value.take(8)} | ${perSub.mkString(" ")}"
    }

  /** Read-only capability over the unrolled per-metagraph `CurrencySnapshotInfo` partitions — just the prefix scan
    * [[reconstructCurrencyInfoFrom]] needs. Split out from [[CurrencyInfoMpt]] so read-only callers (reconstruction, getAll*) take only
    * this and cannot reach the write methods (compile-time bypass-proof). Both `MptStore` and `GlobalStateReader` already expose
    * `getAllForPrefix`; adapt via [[CurrencyInfoMpt.fromMptStore]] (here, also a reader) / `CurrencyInfoMptAdapters.readerFor`
    * (node-shared).
    */
  trait CurrencyInfoReader[F[_]] {
    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]]
  }

  /** Minimal read+write capability over the unrolled per-metagraph `CurrencySnapshotInfo` partitions, shared by BOTH the `MptStore` writers
    * (`syncFromStateChanges` / `syncFromGlobalSnapshotInfo`, shared) and the overlay `AcceptanceMpt` writer (`applyStateChanges`,
    * node-shared) so the reconstruction + write + removal logic lives in ONE place (docs/nakamoto/UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md
    * §6). Extends [[CurrencyInfoReader]] with the two write methods. Both `MptStore` and `AcceptanceMpt` already expose these three
    * methods; adapt via [[CurrencyInfoMpt.fromMptStore]] (here) / `CurrencyInfoMptAdapters.mptFor` (node-shared).
    */
  trait CurrencyInfoMpt[F[_]] extends CurrencyInfoReader[F] {
    def insert[V: ImmutableCodec](entries: Map[GlobalStateKey, V]): F[Unit]
    def remove(keys: List[GlobalStateKey]): F[Unit]
  }

  object CurrencyInfoMpt {
    def fromMptStore[F[_]](store: MptStore[F, GlobalStateKey]): CurrencyInfoMpt[F] = new CurrencyInfoMpt[F] {
      def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]] = store.getAllForPrefix[V](prefix)
      def insert[V: ImmutableCodec](entries: Map[GlobalStateKey, V]): F[Unit] = store.insert[V](entries)
      def remove(keys: List[GlobalStateKey]): F[Unit] = store.remove(keys)
    }
  }

  /** Standalone reconstruction (the inverse of [[infoEntryBytes]]) over any [[CurrencyInfoMpt]] — see the `MptStoreReadOps`
    * `reconstructCurrencySnapshotInfo` scaladoc for the per-field / Option-emptiness / fieldId-7 contract.
    */
  def reconstructCurrencyInfoFrom[F[_]: Sync: Hasher](
    metagraphAddress: Address,
    reader: CurrencyInfoReader[F]
  ): F[CurrencySnapshotInfo] = {
    import GlobalStateFieldId._
    def scan[K, V](sub: GlobalStateFieldId)(implicit c: ImmutableCodec[(K, V)], o: Ordering[K]): F[SortedMap[K, V]] =
      GlobalStateKey
        .metagraphFieldPrefix[F](metagraphAddress, sub)
        .flatMap(reader.getAllForPrefix[(K, V)])
        .map(e => SortedMap.from(e.values))
    for {
      balances <- scan[Address, Balance](MgBalances)
      lastTxRefs <- scan[Address, TransactionReference](MgLastTxRefs)
      lastFeeTxRefs <- scan[Address, TransactionReference](MgLastFeeTxRefs)
      lastAllowSpendRefs <- scan[Address, AllowSpendReference](MgLastAllowSpendRefs)
      lastTokenLockRefs <- scan[Address, TokenLockReference](MgLastTokenLockRefs)
      activeTokenLocks <- scan[Address, SortedSet[Signed[TokenLock]]](MgActiveTokenLocks)
      lastMessages <- scan[MessageType, Signed[CurrencyMessage]](MgLastMessages)
      globalSyncView <- scan[PeerId, Signed[GlobalSnapshotSync]](MgGlobalSnapshotSyncView)
      activeAllowSpends <- GlobalStateKey
        .hypergraphFieldPrefix[F](ActiveAllowSpends, metagraphAddress.some)
        .flatMap(reader.getAllForPrefix[SortedSet[Signed[AllowSpend]]])
        .map(e => SortedMap.from(e.values.toList.flatMap(s => s.headOption.map(_.value.source -> s))))
    } yield
      CurrencySnapshotInfo(
        lastTxRefs = lastTxRefs,
        balances = balances,
        lastMessages = lastMessages.some,
        lastFeeTxRefs = lastFeeTxRefs.some,
        lastAllowSpendRefs = lastAllowSpendRefs.some,
        activeAllowSpends = activeAllowSpends.some,
        globalSnapshotSyncView = globalSyncView.some,
        lastTokenLockRefs = lastTokenLockRefs.some,
        activeTokenLocks = activeTokenLocks.some
      )
  }

  /** The `Mg*` keys present in `prior` but absent in `next` — the per-entry removals the delta writers must issue so an entry dropped from
    * the info (expired allow-spend, pruned balance) does not leave a stale unrolled key (the I5 invariant). `activeAllowSpends` is EXCLUDED
    * — its fieldId-7 removals are the existing `removedAllowSpendKeys` accumulator path, not this one.
    */
  def infoRemovalKeys[F[_]: Sync: Hasher](
    metagraphAddress: Address,
    prior: CurrencySnapshotInfo,
    next: CurrencySnapshotInfo
  ): F[List[GlobalStateKey]] = {
    import GlobalStateFieldId._
    def addrRem[A, B](sub: GlobalStateFieldId, p: SortedMap[Address, A], n: SortedMap[Address, B]): List[GlobalStateKey] =
      (p.keySet -- n.keySet).toList.map(a => GlobalStateKey.metagraphEntry(metagraphAddress, sub, a))
    def optRem[A, B](sub: GlobalStateFieldId, p: Option[SortedMap[Address, A]], n: Option[SortedMap[Address, B]]): List[GlobalStateKey] =
      addrRem(sub, p.getOrElse(SortedMap.empty[Address, A]), n.getOrElse(SortedMap.empty[Address, B]))
    val pureRem =
      addrRem(MgBalances, prior.balances, next.balances) ++
        addrRem(MgLastTxRefs, prior.lastTxRefs, next.lastTxRefs) ++
        optRem(MgLastFeeTxRefs, prior.lastFeeTxRefs, next.lastFeeTxRefs) ++
        optRem(MgLastAllowSpendRefs, prior.lastAllowSpendRefs, next.lastAllowSpendRefs) ++
        optRem(MgLastTokenLockRefs, prior.lastTokenLockRefs, next.lastTokenLockRefs) ++
        optRem(MgActiveTokenLocks, prior.activeTokenLocks, next.activeTokenLocks)
    val removedMsgs = prior.lastMessages.getOrElse(SortedMap.empty[MessageType, Signed[CurrencyMessage]]).keySet --
      next.lastMessages.getOrElse(SortedMap.empty[MessageType, Signed[CurrencyMessage]]).keySet
    val removedSync = prior.globalSnapshotSyncView.getOrElse(SortedMap.empty[PeerId, Signed[GlobalSnapshotSync]]).keySet --
      next.globalSnapshotSyncView.getOrElse(SortedMap.empty[PeerId, Signed[GlobalSnapshotSync]]).keySet
    for {
      msgRem <- removedMsgs.toList.traverse(mt => GlobalStateKey.metagraphEntryHashed[F](metagraphAddress, MgLastMessages, mt.value))
      syncRem <- removedSync.toList.traverse(p =>
        GlobalStateKey.metagraphEntryHashed[F](metagraphAddress, MgGlobalSnapshotSyncView, p.value.value)
      )
    } yield pureRem ++ msgRem ++ syncRem
  }

  /** Write one MG's unrolled `CurrencySnapshotInfo`: upsert the 8 `Mg*` partitions (typed inserts — byte-identical to [[infoEntryBytes]]
    * via the same codecs; unchanged entries are MPT no-ops) and remove the dropped entries ([[infoRemovalKeys]] vs `priorInfo`). Does NOT
    * touch `activeAllowSpends` (fieldId-7, the existing accumulator path) nor the incremental (fieldId-5). The SINGLE typed write path for
    * `applyStateChanges` / `syncFromStateChanges` / `syncFromGlobalSnapshotInfo`.
    */
  def writeCurrencyInfo[F[_]: Sync: Hasher](
    metagraphAddress: Address,
    newInfo: CurrencySnapshotInfo,
    priorInfo: CurrencySnapshotInfo,
    mpt: CurrencyInfoMpt[F]
  ): F[Unit] = {
    import GlobalStateFieldId._
    def addrMap[V](sub: GlobalStateFieldId, m: SortedMap[Address, V]): Map[GlobalStateKey, (Address, V)] =
      m.iterator.map { case (a, v) => GlobalStateKey.metagraphEntry(metagraphAddress, sub, a) -> ((a, v)) }.toMap
    def optMap[V](sub: GlobalStateFieldId, m: Option[SortedMap[Address, V]]): Map[GlobalStateKey, (Address, V)] =
      addrMap(sub, m.getOrElse(SortedMap.empty[Address, V]))
    for {
      msgEntries <- newInfo.lastMessages
        .getOrElse(SortedMap.empty[MessageType, Signed[CurrencyMessage]])
        .toList
        .traverse {
          case (mt, m) => GlobalStateKey.metagraphEntryHashed[F](metagraphAddress, MgLastMessages, mt.value).map(_ -> ((mt, m)))
        }
        .map(_.toMap)
      syncEntries <- newInfo.globalSnapshotSyncView
        .getOrElse(SortedMap.empty[PeerId, Signed[GlobalSnapshotSync]])
        .toList
        .traverse {
          case (p, s) =>
            GlobalStateKey.metagraphEntryHashed[F](metagraphAddress, MgGlobalSnapshotSyncView, p.value.value).map(_ -> ((p, s)))
        }
        .map(_.toMap)
      removals <- infoRemovalKeys[F](metagraphAddress, priorInfo, newInfo)
      _ <- mpt.insert[(Address, Balance)](addrMap(MgBalances, newInfo.balances))
      _ <- mpt.insert[(Address, TransactionReference)](addrMap(MgLastTxRefs, newInfo.lastTxRefs))
      _ <- mpt.insert[(Address, TransactionReference)](optMap(MgLastFeeTxRefs, newInfo.lastFeeTxRefs))
      _ <- mpt.insert[(Address, AllowSpendReference)](optMap(MgLastAllowSpendRefs, newInfo.lastAllowSpendRefs))
      _ <- mpt.insert[(Address, TokenLockReference)](optMap(MgLastTokenLockRefs, newInfo.lastTokenLockRefs))
      _ <- mpt.insert[(Address, SortedSet[Signed[TokenLock]])](optMap(MgActiveTokenLocks, newInfo.activeTokenLocks))
      _ <- mpt.insert[(MessageType, Signed[CurrencyMessage])](msgEntries)
      _ <- mpt.insert[(PeerId, Signed[GlobalSnapshotSync])](syncEntries)
      _ <- if (removals.nonEmpty) mpt.remove(removals) else Sync[F].unit
    } yield ()
  }

  object syntax {
    implicit class GlobalSnapshotInfoMptOps(val info: GlobalSnapshotInfo) extends AnyVal {
      def allStateEntries[F[_]: Async: Parallel: Hasher: JsonSerializer](
        implicit stateProofSelector: StateProofSelector
      ): F[Map[GlobalStateKey, Json]] =
        toAllStateKeyValuePairs(info)

      def allStateEntriesAsBytes[F[_]: Async: Parallel: Hasher: JsonSerializer](
        implicit stateProofSelector: StateProofSelector,
        withdrawalTimeLimit: WithdrawalTimeLimit
      ): F[Map[GlobalStateKey, Array[Byte]]] =
        toAllStateKeyValueBytes(info)
    }

    implicit class StateChangesAccumulatorMptOps(val acc: StateChangesAccumulator) extends AnyVal {
      def toStateEntries[F[_]: Async: Parallel: Hasher: JsonSerializer](
        implicit stateProofSelector: StateProofSelector
      ): F[Map[GlobalStateKey, Json]] =
        toStateKeyValuePairsFromAccumulator(acc)
    }

    implicit class MptBytesBuilderOps[F[_]: Parallel: Async: Hasher: JsonSerializer](
      kvPairsF: F[Map[GlobalStateKey, Array[Byte]]]
    ) {

      /** Build an MPT root from pre-encoded bytes. Matches the root produced by `MptStore.build` when fed the same bytes — i.e., when using
        * `toAllStateKeyValueBytes`, this equals the in-store root for a state snapshot written through the typed `insert[V]` path.
        */
      def buildMptFromBytes: F[MptRoot] =
        for {
          kvPairs <- kvPairsF
          mptRoot <-
            if (kvPairs.isEmpty) MptRoot(Hash.empty).pure[F]
            else
              kvPairs.toList.parTraverse { case (k, v) => GlobalStateKey.toHex[F](k).map(_ -> v) }
                .flatMap(pairs => MerklePatriciaTrie.makeParallelFromBytes[F](pairs.toMap).map(_.rootHash))
        } yield mptRoot

      /** Per-fieldId MPT root over the same key+value bytes as the global root. For each `fieldId` present in the input, builds a separate
        * MPT containing only that field's hex-encoded keys + value bytes and returns its rootHash. Deterministic across nodes because (a)
        * `fieldId` is a structural prefix of the encoded `GlobalStateKey` and (b) the underlying value encodings are the same as those that
        * feed the global root. FieldIds with no entries are omitted from the result; callers default to `Hash.empty`.
        */
      def buildPerFieldMptRoots: F[Map[GlobalStateFieldId, Hash]] =
        for {
          kvPairs <- kvPairsF
          grouped = kvPairs.groupBy(_._1.fieldId).toList
          perField <- grouped.parTraverse {
            case (fieldId, entries) =>
              entries.toList.parTraverse { case (k, v) => GlobalStateKey.toHex[F](k).map(_ -> v) }
                .flatMap(pairs => fieldRootFromBytes[F](pairs.toMap).tupleLeft(fieldId))
          }
        } yield perField.toMap
    }

    implicit class MptBuilderOps[F[_]: Parallel: Async: Hasher](kvPairsF: F[Map[GlobalStateKey, Json]]) {

      private val BatchSize = 5000
      private val LogProgressEvery = 50000

      def buildMpt(implicit stateProofSelector: StateProofSelector, j: JsonSerializer[F]): F[MptRoot] = {
        val logger = Slf4jLogger.getLoggerFromName[F]("MPT.BuildMpt")

        def toHexMap(kvPairs: Map[GlobalStateKey, Json]): F[Map[Hex, Json]] = {
          def convertBatch(batch: List[(GlobalStateKey, Json)]): F[List[(Hex, Json)]] =
            batch.parTraverse {
              case (key, value) =>
                GlobalStateKey.toHex[F](key).map(_ -> value)
            }

          if (kvPairs.size <= BatchSize)
            convertBatch(kvPairs.toList).map(_.toMap)
          else
            kvPairs.toList
              .grouped(BatchSize)
              .toList
              .foldLeftM(Map.empty[Hex, Json]) { (acc, batch) =>
                convertBatch(batch).map(acc ++ _) <* Async[F].cede
              }
        }

        def buildMptRoot(hexMap: Map[Hex, Json]): F[MptRoot] =
          for {
            root <- MerklePatriciaTrie.makeParallel[F, Json](hexMap).map(_.rootHash)
          } yield root

        for {
          kvPairs <- kvPairsF
          mptRoot <-
            if (kvPairs.isEmpty)
              logger.info("Empty map, returning empty hash").as(MptRoot(Hash.empty))
            else
              toHexMap(kvPairs).flatMap(buildMptRoot)
        } yield mptRoot

      }
    }

    implicit class MptStoreReadOps[F[_]: Async](val store: MptStore[F, GlobalStateKey]) {

      def getBalance(address: Address): F[Option[Balance]] =
        store
          .get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, address))

      def getBalances(addresses: List[Address]): F[Map[Address, Balance]] = {
        val keys = addresses.map(addr => GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr))
        store.getMany[Balance](keys).map { results =>
          results.flatMap {
            case (key, balance) =>
              key.userNamespace match {
                case AddressNamespace(addr) => Some(addr -> balance)
                case _                      => None
              }
          }
        }
      }

      def getTxRef(address: Address): F[Option[TransactionReference]] =
        store
          .get[TransactionReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, address))

      def getStateChannelHash(metagraphAddress: Address): F[Option[Hash]] =
        store
          .get[Hash](GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastStateChannelSnapshotHashes))

      def getAllowSpendRef(address: Address): F[Option[AllowSpendReference]] =
        store
          .get[AllowSpendReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, address))

      def getActiveAllowSpends(metagraphId: Option[Address], address: Address): F[Option[SortedSet[Signed[AllowSpend]]]] =
        store
          .get[SortedSet[Signed[AllowSpend]]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, metagraphId, address))

      def getTokenLockRef(address: Address): F[Option[TokenLockReference]] =
        store
          .get[TokenLockReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, address))

      def getActiveTokenLocks(address: Address): F[Option[SortedSet[Signed[TokenLock]]]] =
        store
          .get[SortedSet[Signed[TokenLock]]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, address))

      def getTokenLockBalance(tokenAddress: Address, holderAddress: Address): F[Option[Balance]] =
        store
          .get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.TokenLockBalances, tokenAddress, holderAddress))

      def getDelegatedStakes(address: Address): F[Option[SortedSet[DelegatedStakeRecord]]] =
        store
          .get[SortedSet[DelegatedStakeRecord]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveDelegatedStakes, address))

      def getDelegatedStakeWithdrawals(address: Address): F[Option[SortedSet[PendingDelegatedStakeWithdrawal]]] =
        store
          .get[SortedSet[PendingDelegatedStakeWithdrawal]](
            GlobalStateKey.hypergraph(GlobalStateFieldId.DelegatedStakesWithdrawals, address)
          )

      def getNodeCollaterals(address: Address): F[Option[SortedSet[NodeCollateralRecord]]] =
        store
          .get[SortedSet[NodeCollateralRecord]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveNodeCollaterals, address))

      def getNodeCollateralWithdrawals(address: Address): F[Option[SortedSet[PendingNodeCollateralWithdrawal]]] =
        store
          .get[SortedSet[PendingNodeCollateralWithdrawal]](GlobalStateKey.hypergraph(GlobalStateFieldId.NodeCollateralWithdrawals, address))

      def getCurrencySnapshot(metagraphAddress: Address): F[Option[Signed[CurrencySnapshot]]] =
        store
          .get[Signed[CurrencySnapshot]](GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastCurrencySnapshots))

      def getIncrementalCurrencySnapshot(metagraphAddress: Address): F[Option[Signed[CurrencyIncrementalSnapshot]]] =
        store
          .get[Signed[CurrencyIncrementalSnapshot]](
            GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastIncrementalCurrencySnapshots)
          )

      /** Reconstruct a metagraph's `CurrencySnapshotInfo` from the UNROLLED per-entry `Mg*` partitions — the inverse of [[infoEntryBytes]],
        * replacing the monolithic fieldId-6 blob read (docs/nakamoto/UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md). Each `Mg*` partition is
        * prefix-scanned (`metagraphFieldPrefix`); the entry key is recovered from the VALUE (the value-carries-key layout, since `toHex`
        * hashes the key). `activeAllowSpends` is NOT an `Mg*` partition — it is read from the fieldId-7 `ActiveAllowSpends` metagraph-scope
        * partition, recovering each holder from its allow-spends' `source`.
        *
        * '''Option-emptiness:''' the seven optional fields reconstruct as `Some(scanned)` (the post-tess3 always-`Some` convention — an
        * empty scan ⇒ `Some(empty)`). This is MPT-invisible (an empty optional encodes to zero entries, same as `None`) and read-safe
        * (consumers use `.getOrElse`); for the steady-state post-tess3 producer it round-trips exactly (the I1 invariant).
        */
      def reconstructCurrencySnapshotInfo(metagraphAddress: Address)(implicit H: Hasher[F]): F[CurrencySnapshotInfo] =
        reconstructCurrencyInfoFrom[F](metagraphAddress, CurrencyInfoMpt.fromMptStore(store))

      /** A metagraph has currency state iff its incremental-snapshot partition (fieldId 5) is present (written in lockstep with the info on
        * every path), so this gates `None`/`Some` exactly as the old blob-presence read did, then reconstructs from the unrolled
        * partitions.
        */
      def getCurrencySnapshotInfo(metagraphAddress: Address)(implicit H: Hasher[F]): F[Option[CurrencySnapshotInfo]] =
        store
          .contains(GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastIncrementalCurrencySnapshots))
          .flatMap {
            case true  => reconstructCurrencySnapshotInfo(metagraphAddress).map(_.some)
            case false => none[CurrencySnapshotInfo].pure[F]
          }

      def getCurrencySnapshotProof(metagraphAddress: Address): F[Option[Proof]] =
        store
          .get[Proof](GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastCurrencySnapshotsProofs))

      def getMetagraphSyncData(metagraphAddress: Address): F[Option[MetagraphSyncDataInfo]] =
        store
          .get[MetagraphSyncDataInfo](GlobalStateKey.hypergraph(GlobalStateFieldId.MetagraphSyncData, metagraphAddress))

      /** Materialize the full `Address → Hash` view of `lastStateChannelSnapshotHashes` via the `ActiveAddressIndex` sidecar.
        * Metagraph-keyed fields hash the address into the partition key, so we recover the keyset from the sidecar and `getMany` the
        * values; the returned map's `K` preserves the original `MetagraphNamespace(addr)` so we pattern-match the address back out.
        */
      def getAllLastStateChannelSnapshotHashes(
        implicit H: Hasher[F]
      ): F[SortedMap[Address, Hash]] =
        for {
          indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastStateChannelSnapshotHashes)
          addrSet <- store.get[SortedSet[Address]](indexKey).map(_.getOrElse(SortedSet.empty[Address]))
          keys = addrSet.toList.map(addr => GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastStateChannelSnapshotHashes))
          values <- store.getMany[Hash](keys)
        } yield
          SortedMap.from(values.toList.flatMap {
            case (key, h) =>
              key.networkNamespace match {
                case MetagraphNamespace(addr) => List(addr -> h)
                case _                        => Nil
              }
          })

      /** Materialize the full `lastCurrencySnapshots` view. The value is `Either[Signed[CurrencySnapshot],
        * (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]` — Left and Right are stored in disjoint partitions per address, so
        * for each metagraph address we try the Left partition first and fall back to the Right pair. The address keyset is sourced from the
        * `ActiveAddressIndex` sidecar tracking `LastCurrencySnapshots` (covers both modes since both populations mark the same address).
        */
      def getAllLastCurrencySnapshots(
        implicit H: Hasher[F]
      ): F[SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]] =
        for {
          indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastCurrencySnapshots)
          addrSet <- store.get[SortedSet[Address]](indexKey).map(_.getOrElse(SortedSet.empty[Address]))
          addrList = addrSet.toList
          leftKeys = addrList.map(addr => GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastCurrencySnapshots))
          incKeys = addrList.map(addr => GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastIncrementalCurrencySnapshots))
          lefts <- store.getMany[Signed[CurrencySnapshot]](leftKeys)
          incs <- store.getMany[Signed[CurrencyIncrementalSnapshot]](incKeys)
          // Right-arm info is RECONSTRUCTED from the unrolled `Mg*` partitions (per-MG, hence the traverse — the O(MGs)×O(entries)
          // materialization cost is the tracked lazy-reads follow-up; see UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md §11).
          entries <- addrList.traverse { addr =>
            val leftKey = GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastCurrencySnapshots)
            val incKey = GlobalStateKey.metagraph(addr, GlobalStateFieldId.LastIncrementalCurrencySnapshots)
            lefts.get(leftKey) match {
              case Some(snap) =>
                Option(
                  addr -> Left(snap): (
                    Address,
                    Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]
                  )
                ).pure[F]
              case None =>
                incs.get(incKey) match {
                  case Some(inc) =>
                    reconstructCurrencySnapshotInfo(addr).map(info =>
                      Option(
                        addr -> Right((inc, info)): (
                          Address,
                          Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]
                        )
                      )
                    )
                  case None =>
                    none[(Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)])].pure[F]
                }
            }
          }
        } yield SortedMap.from(entries.flatten)

      def getUpdateNodeParameters(
        id: Id
      )(implicit H: Hasher[F]): F[Option[(Signed[UpdateNodeParameters], SnapshotOrdinal)]] =
        GlobalStateKey.updateNodeParametersKey[F](id).flatMap(store.get[(Signed[UpdateNodeParameters], SnapshotOrdinal)])

      /** Materialize the full `Id → (Signed[UpdateNodeParameters], SnapshotOrdinal)` view via prefix-scan. The MPT key is a hash of the
        * `Id`, but the signed value carries the signer's `Id` in `proofs.head.id`, which by GSAM convention matches the map's keying `Id`.
        * No sidecar needed.
        */
      def getAllUpdateNodeParameters(
        implicit H: Hasher[F]
      ): F[SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)]] =
        for {
          prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.UpdateNodeParameters)
          entries <- store.getAllForPrefix[(Signed[UpdateNodeParameters], SnapshotOrdinal)](prefix)
        } yield SortedMap.from(entries.values.map { case (signed, ord) => signed.proofs.head.id -> (signed, ord) })

      def getPriceRecord(tokenPair: TokenPair)(implicit H: Hasher[F]): F[Option[PriceRecord]] =
        GlobalStateKey.priceStateKey[F](tokenPair).flatMap(store.get[PriceRecord])

      /** Read the contents of a system-namespaced epoch-bucket. Returns `None` if the bucket is empty (not present in MPT). */
      def getExpiryBucket[K](
        label: SystemNamespaceLabel,
        epoch: EpochProgress
      )(implicit H: Hasher[F], C: ImmutableCodec[SortedSet[K]]): F[Option[SortedSet[K]]] =
        GlobalStateKey.expiryIndexKey[F](label, epoch).flatMap(store.get[SortedSet[K]])
    }

    implicit class MptStoreGlobalSnapshotOps[F[_]: Async: Parallel: Hasher: JsonSerializer](
      val store: MptStore[F, GlobalStateKey]
    ) {

      /** Seed / reset the MptStore from a `GlobalSnapshotInfo`.
        *
        * Writes each typed field via its canonical scodec `ImmutableCodec`, matching the encoding used by the typed read methods
        * (`getBalance`, `getActiveTokenLocks`, etc) and the incremental `syncFromStateChanges` writer. Use this for every bootstrap /
        * resync / peer-download path — `allStateEntries → syncFull[Json]` (JSON bytes) must NOT be mixed in, since it produces different
        * bytes for the same logical state and the resulting mptRoot will diverge from peers that bootstrapped via the typed path.
        *
        * '''FINDING-S01 — MPT-native consensus partitions are PRESERVED, not wiped.''' `GlobalStateFieldId.mptNativeConsensusFields`
        * (`ConsumedAllowSpends` 33, `Slashings` 34) are in the signed consensus `mptRoot` but have NO `GlobalSnapshotInfo` field, so a
        * from-GSI rebuild used to silently DROP them — wiping the cross-shard nullifier (re-opening consumed allow-spends for a
        * double-spend) and committing a root that diverges from the signed `stateProof.mptRoot`. This method now carries those partitions'
        * raw bytes verbatim across the clear→rebuild. Both partitions are empty at `numShards = 1` (byte-identical no-op). Consensus adopt
        * sites that hold the SIGNED root should prefer [[syncFromGlobalSnapshotInfoVerified]], which reconciles {with, without} the
        * preserved bytes against the signed root BEFORE writing and fails closed on neither matching.
        */
      def syncFromGlobalSnapshotInfo(
        info: GlobalSnapshotInfo,
        snapshotOrdinal: SnapshotOrdinal
      )(
        implicit stateProofSelector: StateProofSelector,
        withdrawalTimeLimitCtx: WithdrawalTimeLimit
      ): F[Unit] =
        syncFromGlobalSnapshotInfoImpl(info, snapshotOrdinal, preserveMptNative = true)

      /** ROOT-VERIFIED GSI adopt — the fail-closed variant for every consensus adopt site that holds the target snapshot's SIGNED
        * `stateProof.mptRoot` (reorg self-heal, reward-realign, gossip catch-up fallback, cold restart, peer download). Reconciles, BEFORE
        * any store write, which rebuild candidate reproduces the signed root:
        *
        *   1. '''with-preserve''' — GSI entries ∪ the store's current `mptNativeConsensusFields` (33/34) bytes. Matches whenever the local
        *      spent-set/slash-ledger equals the target's (the plain-reorg / restart common case: the markers were written at finalized
        *      ordinals shared by both branches).
        *   1. '''without-preserve''' — GSI entries alone. Matches when the target's 33/34 partitions are EMPTY (always at `numShards = 1`;
        *      also the cross-chain adopt case where OUR local markers are stale and must NOT be carried into the adopted state).
        *   1. '''neither''' — the target's 33/34 content differs from ours and is NOT reconstructible from the GSI (it has no field for
        *      them). Returns `false` WITHOUT touching the store — the caller must fail closed (do not adopt canonical state; recover via
        *      the byte-faithful `loadBytes` path, e.g. `NakamotoSyncDaemon.seedMptByteFaithful`).
        *
        * The candidate roots are computed over `toAllStateKeyValueBytes(info)` — byte-identical to what the typed-insert rebuild writes for
        * every user field (the standing rebuild-vs-producer parity contract, `RebuildVsProducerBytesAllPartitionsParitySuite`) — so a
        * `true` verdict guarantees the post-rebuild store root equals the signed root. `signedMptRoot = None` (pre-MPT legacy snapshot)
        * returns `false`: there is nothing sound to verify against.
        *
        * @return
        *   `true` iff the store was rebuilt AND its sidecar-free consensus root equals `signedMptRoot`; `false` ⇒ NOTHING was written.
        */
      def syncFromGlobalSnapshotInfoVerified(
        info: GlobalSnapshotInfo,
        snapshotOrdinal: SnapshotOrdinal,
        signedMptRoot: Option[Hash]
      )(
        implicit stateProofSelector: StateProofSelector,
        withdrawalTimeLimitCtx: WithdrawalTimeLimit
      ): F[Boolean] =
        signedMptRoot match {
          case None => false.pure[F]
          case Some(expected) =>
            for {
              preserved <- store.underlying.entries.map(_.filter {
                case (hex, _) => GlobalStateKey.fieldIdFromHex(hex).exists(GlobalStateFieldId.mptNativeConsensusFields.contains)
              })
              gsiBytes <- toAllStateKeyValueBytes[F](info)
              gsiHex <- gsiBytes.toList.parTraverse { case (k, v) => GlobalStateKey.toHex[F](k).map(_ -> v) }.map(_.toMap)
              // Key sets are disjoint by construction: `gsiHex` never contains an mptNative fieldId (the GSI has no field for them)
              // and `preserved` contains ONLY mptNative fieldIds — so `++` is a pure union, no overwrites.
              rootWith <- io.constellationnetwork.schema.GlobalSnapshotInfo.sidecarFreeMptRoot[F](gsiHex ++ preserved)
              adopted <-
                if (rootWith === expected)
                  syncFromGlobalSnapshotInfoImpl(info, snapshotOrdinal, preserveMptNative = true).as(true)
                else
                  io.constellationnetwork.schema.GlobalSnapshotInfo.sidecarFreeMptRoot[F](gsiHex).flatMap { rootWithout =>
                    if (rootWithout === expected)
                      syncFromGlobalSnapshotInfoImpl(info, snapshotOrdinal, preserveMptNative = false).as(true)
                    else
                      false.pure[F]
                  }
            } yield adopted
        }

      /** ROOT-VERIFIED byte-faithful reload of the node's OWN persisted MPT at `snapshotOrdinal` — the PREFERRED boot/download seed
        * (FINDING-S01 completion). The persisted byte map carries the MPT-native consensus partitions (`ConsumedAllowSpends` 33 /
        * `Slashings` 34) verbatim — the partitions a from-GSI rebuild structurally cannot reconstruct — so a node restarting with its own
        * persisted MPT reproduces the signed `stateProof.mptRoot` BY CONSTRUCTION and does not need to re-bootstrap from a peer.
        *
        * Semantics: load the persisted state (if any), recompute the sidecar-free consensus root, and keep the load ONLY when it equals
        * `signedMptRoot`. On no-persistence / nothing-persisted / root mismatch (stale or corrupt bytes) the pre-call store state is
        * restored via savepoint and `false` is returned — callers then fall back to [[syncFromGlobalSnapshotInfoVerified]] (and fail
        * closed, or degrade per the site's documented contract, when that also cannot reproduce the signed root). `signedMptRoot = None`
        * (pre-MPT legacy snapshot) returns `false` without touching disk: there is nothing sound to verify against.
        *
        * @return
        *   `true` iff the persisted bytes were adopted AND their sidecar-free consensus root equals `signedMptRoot`; `false` ⇒ the store is
        *   byte-identical to its pre-call state.
        */
      def syncFromPersistedMptVerified(
        snapshotOrdinal: SnapshotOrdinal,
        signedMptRoot: Option[Hash]
      ): F[Boolean] =
        signedMptRoot match {
          case None => false.pure[F]
          case Some(expected) =>
            for {
              sp <- store.savepoint
              loaded <- store.loadPersisted(snapshotOrdinal)
              adopted <-
                if (!loaded) false.pure[F]
                else
                  store.underlying.entries
                    .flatMap(io.constellationnetwork.schema.GlobalSnapshotInfo.sidecarFreeMptRoot[F](_))
                    .flatMap { loadedRoot =>
                      if (loadedRoot === expected) true.pure[F]
                      else sp.restore.as(false)
                    }
            } yield adopted
        }

      private def syncFromGlobalSnapshotInfoImpl(
        info: GlobalSnapshotInfo,
        snapshotOrdinal: SnapshotOrdinal,
        preserveMptNative: Boolean
      )(
        implicit stateProofSelector: StateProofSelector,
        withdrawalTimeLimitCtx: WithdrawalTimeLimit
      ): F[Unit] = {
        val withdrawalTimeLimit: Option[EpochProgress] = withdrawalTimeLimitCtx.value
        import io.constellationnetwork.schema.ID.Id
        import io.constellationnetwork.schema.mpt.GlobalStateFieldId._
        import io.constellationnetwork.schema.mpt.PartitionNamespace.AddressNamespace
        import io.constellationnetwork.security.hash.Hash
        import io.constellationnetwork.security.signature.Signed
        import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendRefImmutable}
        import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
        import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutable}
        import io.constellationnetwork.serde.codecs.instances.MerkleTreeCodecs.proofImmutableCodec
        import io.constellationnetwork.serde.codecs.instances.MetagraphSyncDataInfoCodec.{immutableCodec => metagraphSyncImmutable}
        import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
        // §3 NIPoPoW S0 historical-stake-snapshots codec — the SAME instance the producer's
        // `toAllStateKeyValueBytes` passes explicitly (`enc[HistoricalStakeSnapshot](entry)(historicalStakeSnapshotImmutable)`)
        // and the delta writer `syncFromStateChanges` resolves for its `store.insert[HistoricalStakeSnapshot]`. Bringing it into
        // implicit scope here makes this rebuild's bytes byte-identical to the producer's signed root for this partition.
        import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{
          historicalImmutableCodec => historicalStakeSnapshotImmutable
        }
        import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
        import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}
        val _ = historicalStakeSnapshotImmutable // resolves `store.insert[HistoricalStakeSnapshot]` below

        // Build the key-to-value maps for each typed field. Each field's codec produces
        // scodec-encoded bytes consistent with what the read methods decode.
        val stateChanHashes: Map[GlobalStateKey, Hash] = info.lastStateChannelSnapshotHashes.iterator.map {
          case (addr, h) => GlobalStateKey.metagraph(addr, LastStateChannelSnapshotHashes) -> h
        }.toMap
        val txRefs: Map[GlobalStateKey, io.constellationnetwork.schema.transaction.TransactionReference] =
          info.lastTxRefs.iterator.map {
            case (addr, r) => GlobalStateKey.hypergraph(LastTxRefs, addr) -> r
          }.toMap
        val balances: Map[GlobalStateKey, Balance] = info.balances.iterator.map {
          case (addr, b) => GlobalStateKey.hypergraph(Balances, addr) -> b
        }.toMap
        val currencyProofs: Map[GlobalStateKey, io.constellationnetwork.merkletree.Proof] =
          info.lastCurrencySnapshotsProofs.iterator.map {
            case (addr, p) => GlobalStateKey.metagraph(addr, LastCurrencySnapshotsProofs) -> p
          }.toMap

        // Currency snapshots — Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]
        // splits into the fieldId-5 incremental key PLUS the UNROLLED per-entry `Mg*` info partitions. The incremental map is
        // inserted here; each MG's `CurrencySnapshotInfo` is written via `writeCurrencyInfo` (drops the monolithic fieldId-6 blob).
        def buildCurrencySnapshotEntries: F[
          (
            Map[GlobalStateKey, Signed[io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot]],
            List[(Address, io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo)]
          )
        ] =
          info.lastCurrencySnapshots.toList.parTraverse {
            case (metagraphAddr, Left(fullSnapshot)) =>
              io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
                .fromCurrencySnapshot(fullSnapshot.value)
                .map { inc =>
                  (
                    GlobalStateKey.metagraph(metagraphAddr, LastIncrementalCurrencySnapshots) -> Signed(inc, fullSnapshot.proofs),
                    metagraphAddr -> fullSnapshot.info.toCurrencySnapshotInfo
                  )
                }
            case (metagraphAddr, Right((inc, snInfo))) =>
              (
                (GlobalStateKey.metagraph(metagraphAddr, LastIncrementalCurrencySnapshots) -> inc) ->
                  (metagraphAddr -> snInfo)
              ).pure[F]
          }.map { paired =>
            val snapshotEntries = paired.map(_._1).toMap
            val infoEntries = paired.map(_._2)
            (snapshotEntries, infoEntries)
          }

        // Optional-field maps.
        val activeAllowSpends: Map[GlobalStateKey, SortedSet[Signed[io.constellationnetwork.schema.swap.AllowSpend]]] =
          info.activeAllowSpends.toList
            .flatMap(_.toList)
            .flatMap {
              case (optAddr, innerMap) =>
                innerMap.toList.map { case (addr, s) => GlobalStateKey.hypergraph(ActiveAllowSpends, optAddr, addr) -> s }
            }
            .toMap
        val activeTokenLocks: Map[GlobalStateKey, SortedSet[Signed[io.constellationnetwork.schema.tokenLock.TokenLock]]] =
          info.activeTokenLocks.toList
            .flatMap(_.toList)
            .map {
              case (addr, s) => GlobalStateKey.hypergraph(ActiveTokenLocks, addr) -> s
            }
            .toMap
        val tokenLockBalances: Map[GlobalStateKey, Balance] =
          info.tokenLockBalances.toList
            .flatMap(_.toList)
            .flatMap {
              case (tokenAddr, inner) =>
                inner.toList.map { case (holder, bal) => GlobalStateKey.hypergraph(TokenLockBalances, tokenAddr, holder) -> bal }
            }
            .toMap
        val lastAllowSpendRefs: Map[GlobalStateKey, io.constellationnetwork.schema.swap.AllowSpendReference] =
          info.lastAllowSpendRefs.toList
            .flatMap(_.toList)
            .map {
              case (addr, r) => GlobalStateKey.hypergraph(LastAllowSpendRefs, addr) -> r
            }
            .toMap
        val lastTokenLockRefs: Map[GlobalStateKey, io.constellationnetwork.schema.tokenLock.TokenLockReference] =
          info.lastTokenLockRefs.toList
            .flatMap(_.toList)
            .map {
              case (addr, r) => GlobalStateKey.hypergraph(LastTokenLockRefs, addr) -> r
            }
            .toMap
        val activeDelegatedStakes: Map[GlobalStateKey, SortedSet[io.constellationnetwork.schema.delegatedStake.DelegatedStakeRecord]] =
          info.activeDelegatedStakes.toList
            .flatMap(_.toList)
            .map {
              case (addr, s) => GlobalStateKey.hypergraph(ActiveDelegatedStakes, addr) -> s
            }
            .toMap
        val delegatedStakesWithdrawals
          : Map[GlobalStateKey, SortedSet[io.constellationnetwork.schema.delegatedStake.PendingDelegatedStakeWithdrawal]] =
          info.delegatedStakesWithdrawals.toList
            .flatMap(_.toList)
            .map {
              case (addr, s) => GlobalStateKey.hypergraph(DelegatedStakesWithdrawals, addr) -> s
            }
            .toMap
        val activeNodeCollaterals: Map[GlobalStateKey, SortedSet[io.constellationnetwork.schema.nodeCollateral.NodeCollateralRecord]] =
          info.activeNodeCollaterals.toList
            .flatMap(_.toList)
            .map {
              case (addr, s) => GlobalStateKey.hypergraph(ActiveNodeCollaterals, addr) -> s
            }
            .toMap
        val nodeCollateralWithdrawals
          : Map[GlobalStateKey, SortedSet[io.constellationnetwork.schema.nodeCollateral.PendingNodeCollateralWithdrawal]] =
          info.nodeCollateralWithdrawals.toList
            .flatMap(_.toList)
            .map {
              case (addr, s) => GlobalStateKey.hypergraph(NodeCollateralWithdrawals, addr) -> s
            }
            .toMap
        val metagraphSyncData: Map[GlobalStateKey, MetagraphSyncDataInfo] =
          info.metagraphSyncData.toList
            .flatMap(_.toList)
            .map {
              case (addr, d) => GlobalStateKey.hypergraph(MetagraphSyncData, addr) -> d
            }
            .toMap

        // Keys for UpdateNodeParameters (Id-keyed) and PriceState (TokenPair-keyed) are hashed
        // async via Hasher, so their entry maps are built in F.
        val updateNodeParametersEntriesF: F[Map[GlobalStateKey, (Signed[UpdateNodeParameters], SnapshotOrdinal)]] =
          info.updateNodeParameters.toList
            .flatMap(_.toList)
            .parTraverse {
              case (id, rec) => GlobalStateKey.updateNodeParametersKey[F](id).map(_ -> rec)
            }
            .map(_.toMap)

        val priceStateEntriesF: F[Map[GlobalStateKey, PriceRecord]] =
          info.priceState.toList
            .flatMap(_.toList)
            .parTraverse {
              case (tp, rec) => GlobalStateKey.priceStateKey[F](tp).map(_ -> rec)
            }
            .map(_.toMap)

        // §3 NIPoPoW S0 historical-stake-snapshots rebuild: one entry per stored eta-period. Key derived via
        // `historicalStakeSnapshotsKey[F]` (hashed eta-period), value = the `HistoricalStakeSnapshot` (combined stake + eta) —
        // EXACTLY the key shape + type the producer's `toAllStateKeyValueBytes` (`historicalStakeSnapshotsF`) and the delta writer
        // `syncFromStateChanges` (`historicalStakeEntriesF`) use, so all three rebuild paths land byte-identical bytes under
        // `GlobalStateFieldId.HistoricalStakeSnapshots`.
        val historicalStakeEntriesF: F[Map[GlobalStateKey, HistoricalStakeSnapshot]] =
          info.historicalStakeSnapshots.toList.parTraverse {
            case (period, entry) => GlobalStateKey.historicalStakeSnapshotsKey[F](period).map(_ -> entry)
          }.map(_.toMap)

        // Reconstruct the allow-spend expiry index from `info.activeAllowSpends`: each active record contributes one entry in
        // `index[lastValidEpochProgress]`. Buckets are `SortedSet[AllowSpendExpiryKey]`; empty buckets are omitted entirely.
        val allowSpendExpiryBucketsF: F[Map[GlobalStateKey, SortedSet[AllowSpendExpiryKey]]] = {
          val flat = info.activeAllowSpends.toList.flatMap(_.toList).flatMap {
            case (mid, innerMap) => innerMap.toList.flatMap { case (addr, set) => set.toList.map(s => (mid, addr, s)) }
          }
          flat.parTraverse {
            case (mid, addr, s) =>
              s.toHashed.map(h => (s.lastValidEpochProgress, AllowSpendExpiryKey(mid, addr, h.hash)))
          }.flatMap { entries =>
            val byEpoch: Map[EpochProgress, SortedSet[AllowSpendExpiryKey]] =
              entries.groupMap(_._1)(_._2).view.mapValues(_.to(SortedSet)).toMap
            byEpoch.toList.parTraverse {
              case (epoch, bucket) =>
                GlobalStateKey.expiryIndexKey[F](SystemNamespaceLabel.ExpiryIndexAllowSpends, epoch).map(_ -> bucket)
            }
              .map(_.toMap)
          }
        }

        // Reconstruct the node-collateral-withdrawal expiry index. Expiry = `createdAt + withdrawalTimeLimit`; skipped when the caller
        // didn't provide `withdrawalTimeLimit` (test contexts that don't exercise the index).
        val nodeCollateralWithdrawalExpiryBucketsF: F[Map[GlobalStateKey, SortedSet[NodeCollateralWithdrawalExpiryKey]]] =
          withdrawalTimeLimit match {
            case None => Map.empty[GlobalStateKey, SortedSet[NodeCollateralWithdrawalExpiryKey]].pure[F]
            case Some(limit) =>
              val flat = info.nodeCollateralWithdrawals.toList.flatMap(_.toList).flatMap {
                case (addr, set) => set.toList.map(w => (addr, w))
              }
              flat.parTraverse {
                case (addr, w) =>
                  w.event.toHashed.map(h => (w.createdAt |+| limit, NodeCollateralWithdrawalExpiryKey(addr, h.hash)))
              }.flatMap { entries =>
                val byEpoch: Map[EpochProgress, SortedSet[NodeCollateralWithdrawalExpiryKey]] =
                  entries.groupMap(_._1)(_._2).view.mapValues(_.to(SortedSet)).toMap
                byEpoch.toList.parTraverse {
                  case (epoch, bucket) =>
                    GlobalStateKey
                      .expiryIndexKey[F](SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals, epoch)
                      .map(_ -> bucket)
                }.map(_.toMap)
              }
          }

        // Reconstruct the token-lock expiry index. Only records with `unlockEpoch.isDefined` contribute — records with `None` unlock
        // never expire and aren't indexed.
        val tokenLockExpiryBucketsF: F[Map[GlobalStateKey, SortedSet[TokenLockExpiryKey]]] = {
          val flat = info.activeTokenLocks.toList.flatMap(_.toList).flatMap {
            case (addr, set) => set.toList.map(l => (addr, l))
          }
          flat.parTraverse {
            case (addr, l) =>
              l.unlockEpoch match {
                case Some(epoch) =>
                  l.toHashed.map[Option[(EpochProgress, TokenLockExpiryKey)]](h => Some((epoch, TokenLockExpiryKey(addr, h.hash))))
                case None => Option.empty[(EpochProgress, TokenLockExpiryKey)].pure[F]
              }
          }.map(_.flatten).flatMap { entries =>
            val byEpoch: Map[EpochProgress, SortedSet[TokenLockExpiryKey]] =
              entries.groupMap(_._1)(_._2).view.mapValues(_.to(SortedSet)).toMap
            byEpoch.toList.parTraverse {
              case (epoch, bucket) =>
                GlobalStateKey.expiryIndexKey[F](SystemNamespaceLabel.ExpiryIndexTokenLocks, epoch).map(_ -> bucket)
            }
              .map(_.toMap)
          }
        }

        // We avoid per-field `sync` (each would trigger its own trie build). Instead: clear,
        // insert each typed batch, then build once at the end.
        // Caller-serialized — see MptStore.withTransaction. Bootstrap/download paths are
        // single-fiber; production-time reorg-adoption callers (NakamotoSyncDaemon catch-up
        // and storeForkBranch) run under `snapshotSemaphore`. accept() callers run under
        // `mptStore.withTransaction`'s savepoint scope, also `snapshotSemaphore`-serialized.
        for {
          // FINDING-S01: capture the MPT-NATIVE consensus partitions (ConsumedAllowSpends 33 / Slashings 34 — in the signed
          // consensus root but with NO GlobalSnapshotInfo field) BEFORE the clear, and re-insert them verbatim below. Without
          // this the rebuild silently wipes the cross-shard spent-set (double-spend re-open) and commits a root that diverges
          // from the signed stateProof.mptRoot. Empty at numShards = 1 ⇒ byte-identical no-op.
          preservedMptNative <-
            if (preserveMptNative)
              store.underlying.entries.map(_.filter {
                case (hex, _) => GlobalStateKey.fieldIdFromHex(hex).exists(GlobalStateFieldId.mptNativeConsensusFields.contains)
              })
            else Map.empty[Hex, Array[Byte]].pure[F]
          _ <- store.clear
          currency <- buildCurrencySnapshotEntries
          updateNodeParametersEntries <- updateNodeParametersEntriesF
          priceStateEntries <- priceStateEntriesF
          allowSpendExpiryBuckets <- allowSpendExpiryBucketsF
          tokenLockExpiryBuckets <- tokenLockExpiryBucketsF
          nodeCollateralWithdrawalExpiryBuckets <- nodeCollateralWithdrawalExpiryBucketsF
          historicalStakeEntries <- historicalStakeEntriesF
          _ <- store.insert[Hash](stateChanHashes)
          _ <- store.insert[io.constellationnetwork.schema.transaction.TransactionReference](txRefs)
          _ <- store.insert[Balance](balances)
          _ <- store.insert[Signed[io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot]](currency._1)
          // Unrolled per-metagraph info — the 8 `Mg*` partitions replace the monolithic fieldId-6 blob. `store.clear` above leaves an
          // empty store, so the reconstructed prior is empty ⇒ no removals; the upserts are the full info per MG.
          _ <- currency._2.traverse_ {
            case (metagraphAddr, newInfo) =>
              reconstructCurrencyInfoFrom[F](metagraphAddr, CurrencyInfoMpt.fromMptStore(store)).flatMap { priorInfo =>
                writeCurrencyInfo[F](metagraphAddr, newInfo, priorInfo, CurrencyInfoMpt.fromMptStore(store))
              }
          }
          _ <- store.insert[io.constellationnetwork.merkletree.Proof](currencyProofs)
          _ <- store.insert[SortedSet[Signed[io.constellationnetwork.schema.swap.AllowSpend]]](activeAllowSpends)
          _ <- store.insert[SortedSet[Signed[io.constellationnetwork.schema.tokenLock.TokenLock]]](activeTokenLocks)
          _ <- store.insert[Balance](tokenLockBalances)
          _ <- store.insert[io.constellationnetwork.schema.swap.AllowSpendReference](lastAllowSpendRefs)
          _ <- store.insert[io.constellationnetwork.schema.tokenLock.TokenLockReference](lastTokenLockRefs)
          _ <- store.insert[SortedSet[io.constellationnetwork.schema.delegatedStake.DelegatedStakeRecord]](activeDelegatedStakes)
          _ <- store
            .insert[SortedSet[io.constellationnetwork.schema.delegatedStake.PendingDelegatedStakeWithdrawal]](delegatedStakesWithdrawals)
          _ <- store.insert[SortedSet[io.constellationnetwork.schema.nodeCollateral.NodeCollateralRecord]](activeNodeCollaterals)
          _ <- store
            .insert[SortedSet[io.constellationnetwork.schema.nodeCollateral.PendingNodeCollateralWithdrawal]](nodeCollateralWithdrawals)
          _ <- store.insert[MetagraphSyncDataInfo](metagraphSyncData)
          _ <- store.insert[(Signed[UpdateNodeParameters], SnapshotOrdinal)](updateNodeParametersEntries)
          _ <- store.insert[PriceRecord](priceStateEntries)
          _ <- store.insert[HistoricalStakeSnapshot](historicalStakeEntries)
          _ <- store.insert[SortedSet[AllowSpendExpiryKey]](allowSpendExpiryBuckets)
          _ <- store.insert[SortedSet[TokenLockExpiryKey]](tokenLockExpiryBuckets)
          _ <- store.insert[SortedSet[NodeCollateralWithdrawalExpiryKey]](nodeCollateralWithdrawalExpiryBuckets)
          // ActiveAddressIndex bootstrap: rebuild from `info.<field>.keySet` for the indexed fields. Must mirror the
          // delta-path maintenance in `syncFromStateChanges` so a node that bootstraps via `syncFromGlobalSnapshotInfo`
          // converges to the same mptRoot as one that processed every ordinal incrementally.
          activeAddressIndexEntries <- {
            val sets: List[(GlobalStateFieldId, SortedSet[Address])] = List(
              (LastAllowSpendRefs, info.lastAllowSpendRefs.fold(SortedSet.empty[Address])(_.keySet.to(SortedSet))),
              (LastTokenLockRefs, info.lastTokenLockRefs.fold(SortedSet.empty[Address])(_.keySet.to(SortedSet))),
              (LastTxRefs, info.lastTxRefs.keySet.to(SortedSet)),
              (Balances, info.balances.keySet.to(SortedSet)),
              (LastStateChannelSnapshotHashes, info.lastStateChannelSnapshotHashes.keySet.to(SortedSet)),
              (LastCurrencySnapshots, info.lastCurrencySnapshots.keySet.to(SortedSet))
            )
            sets
              .filter(_._2.nonEmpty)
              .parTraverse {
                case (fieldId, addrSet) =>
                  GlobalStateKey.activeAddressIndexKey[F](fieldId).map(_ -> addrSet)
              }
              .map(_.toMap)
          }
          _ <- store.insert[SortedSet[Address]](activeAddressIndexEntries)
          // Address-pair index bootstrap for `tokenLockBalances`. Mirrors the delta-path writer in
          // `syncFromStateChanges` so bootstrap and incremental paths converge to the same mptRoot.
          tokenLockBalancePairs: SortedSet[(Address, Address)] =
            info.tokenLockBalances.fold(SortedSet.empty[(Address, Address)])(_.iterator.flatMap {
              case (mid, inner) => inner.keysIterator.map(h => (mid, h))
            }.to(SortedSet))
          addressPairIndexEntries <-
            if (tokenLockBalancePairs.isEmpty)
              Map.empty[GlobalStateKey, SortedSet[(Address, Address)]].pure[F]
            else
              GlobalStateKey
                .activeAddressIndexKey[F](TokenLockBalances)
                .map(k => Map(k -> tokenLockBalancePairs))
          _ <- store.insert[SortedSet[(Address, Address)]](addressPairIndexEntries)
          // FINDING-S01: restore the MPT-native consensus partitions captured above — raw bytes, verbatim (no codec round-trip),
          // exactly as the byte-faithful `loadBytes` path would carry them. Keys cannot collide with any GSI-derived insert
          // (disjoint fieldIds).
          _ <- store.underlying.insertBytes(preservedMptNative).flatMap(_.liftTo[F]).whenA(preservedMptNative.nonEmpty)
          _ <- store.build(snapshotOrdinal).void
        } yield ()
      }

      def syncFromStateChanges(acc: StateChangesAccumulator, snapshotOrdinal: SnapshotOrdinal)(
        implicit stateProofSelector: StateProofSelector
      ): F[Unit] = {
        // Caller-serialized — see MptStore.withTransaction.
        import io.constellationnetwork.schema.mpt.GlobalStateFieldId._
        // §3 NIPoPoW S0 historical-stake-snapshots writer: each MPT entry value is the scodec-encoded
        // `HistoricalStakeSnapshot` (combined stake + eta) keyed by `historicalStakeSnapshotsKey[F](period)` —
        // same codec / same key shape as `toAllStateKeyValueBytes` so producer and verifier roots agree
        // byte-for-byte.
        import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{
          historicalImmutableCodec => historicalStakeSnapshotImmutable
        }
        val _ = historicalStakeSnapshotImmutable // resolves `store.insert[HistoricalStakeSnapshot]` below

        val syncLogger = Slf4jLogger.getLoggerFromName[F]("MPT.Sync")

        // Convert removal keys from accumulator to GlobalStateKey. Lifted to F because the §3 NIPoPoW
        // historical-stake-snapshot removal keys are F-effecting (`historicalStakeSnapshotsKey[F]` hashes
        // the eta-period via `Hasher[F]`); all the other partitions are F-free.
        def toRemovalGlobalStateKeys: F[Set[GlobalStateKey]] = {
          val allowSpendKeys = acc.removedAllowSpendKeys.map {
            case (metagraphIdOpt, address) =>
              GlobalStateKey.hypergraph(ActiveAllowSpends, metagraphIdOpt, address)
          }
          val tokenLockKeys = acc.removedTokenLockKeys.map { address =>
            GlobalStateKey.hypergraph(ActiveTokenLocks, address)
          }
          val tokenLockBalanceKeys = acc.removedTokenLockBalanceKeys.map {
            case (mid, holder) => GlobalStateKey.hypergraph(TokenLockBalances, mid, holder)
          }
          val delegatedStakeKeys = acc.removedDelegatedStakeKeys.map { address =>
            GlobalStateKey.hypergraph(ActiveDelegatedStakes, address)
          }
          val delegatedStakeWithdrawalKeys = acc.removedDelegatedStakeWithdrawalKeys.map { address =>
            GlobalStateKey.hypergraph(DelegatedStakesWithdrawals, address)
          }
          val nodeCollateralKeys = acc.removedNodeCollateralKeys.map { address =>
            GlobalStateKey.hypergraph(ActiveNodeCollaterals, address)
          }
          val nodeCollateralWithdrawalKeys = acc.removedNodeCollateralWithdrawalKeys.map { address =>
            GlobalStateKey.hypergraph(NodeCollateralWithdrawals, address)
          }
          val pureKeys =
            allowSpendKeys ++ tokenLockKeys ++ tokenLockBalanceKeys ++
              delegatedStakeKeys ++ delegatedStakeWithdrawalKeys ++
              nodeCollateralKeys ++ nodeCollateralWithdrawalKeys
          acc.removedHistoricalStakeSnapshotKeys.toList
            .parTraverse(period => GlobalStateKey.historicalStakeSnapshotsKey[F](period))
            .map(historicalKeys => pureKeys ++ historicalKeys.toSet)
        }

        // Per-field typed entry maps — writes go through `ImmutableCodec[V]` so bytes match
        // the typed reads (`getActiveTokenLocks`, `getBalance`, …). No JSON intermediate.
        val stateChanHashes: Map[GlobalStateKey, Hash] = acc.lastStateChannelSnapshotHashes.iterator.map {
          case (addr, h) => GlobalStateKey.metagraph(addr, LastStateChannelSnapshotHashes) -> h
        }.toMap
        val txRefs: Map[GlobalStateKey, io.constellationnetwork.schema.transaction.TransactionReference] =
          acc.lastTxRefs.iterator.map { case (addr, r) => GlobalStateKey.hypergraph(LastTxRefs, addr) -> r }.toMap
        val balances: Map[GlobalStateKey, Balance] =
          acc.balances.iterator.map { case (addr, b) => GlobalStateKey.hypergraph(Balances, addr) -> b }.toMap
        val currencyProofs: Map[GlobalStateKey, io.constellationnetwork.merkletree.Proof] =
          acc.lastCurrencySnapshotsProofs.iterator.map {
            case (addr, p) => GlobalStateKey.metagraph(addr, LastCurrencySnapshotsProofs) -> p
          }.toMap

        // fieldId-5 incremental map + the typed per-MG `CurrencySnapshotInfo` (written via `writeCurrencyInfo` into the unrolled `Mg*`
        // partitions — the monolithic fieldId-6 blob is dropped). The info write is delta-aware: it reconstructs the prior info and
        // removes entries dropped this ordinal (`infoRemovalKeys`), so a node converges to the same `infoRoot` as the producer.
        def buildCurrencySnapshotEntries: F[
          (
            Map[GlobalStateKey, Signed[CurrencyIncrementalSnapshot]],
            List[(Address, CurrencySnapshotInfo)]
          )
        ] =
          acc.lastCurrencySnapshots.toList.parTraverse {
            case (metagraphAddr, Left(fullSnapshot)) =>
              CurrencyIncrementalSnapshot
                .fromCurrencySnapshot(fullSnapshot.value)
                .map { inc =>
                  (
                    GlobalStateKey.metagraph(metagraphAddr, LastIncrementalCurrencySnapshots) -> Signed(inc, fullSnapshot.proofs),
                    metagraphAddr -> fullSnapshot.info.toCurrencySnapshotInfo
                  )
                }
            case (metagraphAddr, Right((inc, snInfo))) =>
              (
                (GlobalStateKey.metagraph(metagraphAddr, LastIncrementalCurrencySnapshots) -> inc) ->
                  (metagraphAddr -> snInfo)
              ).pure[F]
          }.map { paired =>
            (paired.map(_._1).toMap, paired.map(_._2))
          }

        val activeAllowSpends: Map[GlobalStateKey, SortedSet[Signed[io.constellationnetwork.schema.swap.AllowSpend]]] =
          acc.activeAllowSpends.toList.flatMap {
            case (optAddr, inner) =>
              inner.toList.map { case (addr, s) => GlobalStateKey.hypergraph(ActiveAllowSpends, optAddr, addr) -> s }
          }.toMap
        val activeTokenLocksEntries: Map[GlobalStateKey, SortedSet[Signed[io.constellationnetwork.schema.tokenLock.TokenLock]]] =
          acc.activeTokenLocks.iterator.map { case (addr, s) => GlobalStateKey.hypergraph(ActiveTokenLocks, addr) -> s }.toMap
        val tokenLockBalancesEntries: Map[GlobalStateKey, Balance] =
          acc.tokenLockBalances.toList.flatMap {
            case (tokenAddr, inner) =>
              inner.toList.map { case (holder, bal) => GlobalStateKey.hypergraph(TokenLockBalances, tokenAddr, holder) -> bal }
          }.toMap
        val lastAllowSpendRefsEntries: Map[GlobalStateKey, io.constellationnetwork.schema.swap.AllowSpendReference] =
          acc.lastAllowSpendRefs.iterator.map { case (addr, r) => GlobalStateKey.hypergraph(LastAllowSpendRefs, addr) -> r }.toMap
        val lastTokenLockRefsEntries: Map[GlobalStateKey, io.constellationnetwork.schema.tokenLock.TokenLockReference] =
          acc.lastTokenLockRefs.iterator.map { case (addr, r) => GlobalStateKey.hypergraph(LastTokenLockRefs, addr) -> r }.toMap
        val activeDelegatedStakesEntries
          : Map[GlobalStateKey, SortedSet[io.constellationnetwork.schema.delegatedStake.DelegatedStakeRecord]] =
          acc.activeDelegatedStakes.iterator.map {
            case (addr, s) => GlobalStateKey.hypergraph(ActiveDelegatedStakes, addr) -> s
          }.toMap
        val delegatedStakesWithdrawalsEntries
          : Map[GlobalStateKey, SortedSet[io.constellationnetwork.schema.delegatedStake.PendingDelegatedStakeWithdrawal]] =
          acc.delegatedStakesWithdrawals.iterator.map {
            case (addr, s) => GlobalStateKey.hypergraph(DelegatedStakesWithdrawals, addr) -> s
          }.toMap
        val activeNodeCollateralsEntries
          : Map[GlobalStateKey, SortedSet[io.constellationnetwork.schema.nodeCollateral.NodeCollateralRecord]] =
          acc.activeNodeCollaterals.iterator.map {
            case (addr, s) => GlobalStateKey.hypergraph(ActiveNodeCollaterals, addr) -> s
          }.toMap
        val nodeCollateralWithdrawalsEntries
          : Map[GlobalStateKey, SortedSet[io.constellationnetwork.schema.nodeCollateral.PendingNodeCollateralWithdrawal]] =
          acc.nodeCollateralWithdrawals.iterator.map {
            case (addr, s) => GlobalStateKey.hypergraph(NodeCollateralWithdrawals, addr) -> s
          }.toMap
        val metagraphSyncDataEntries: Map[GlobalStateKey, MetagraphSyncDataInfo] =
          acc.metagraphSyncData.iterator.map { case (addr, d) => GlobalStateKey.hypergraph(MetagraphSyncData, addr) -> d }.toMap

        val updateNodeParametersEntriesF: F[Map[GlobalStateKey, (Signed[UpdateNodeParameters], SnapshotOrdinal)]] =
          acc.updateNodeParameters.toList.parTraverse { case (id, rec) => GlobalStateKey.updateNodeParametersKey[F](id).map(_ -> rec) }
            .map(_.toMap)

        val priceStateEntriesF: F[Map[GlobalStateKey, PriceRecord]] =
          acc.priceState.toList.parTraverse { case (tp, rec) => GlobalStateKey.priceStateKey[F](tp).map(_ -> rec) }
            .map(_.toMap)

        // §3 NIPoPoW S0 historical-stake-snapshot upserts. Each entry's key is derived via
        // `historicalStakeSnapshotsKey[F]` (hashed eta-period); the value is the scodec-encoded
        // `HistoricalStakeSnapshot` (combined stake + eta). Non-empty only at boundary ordinals.
        val historicalStakeEntriesF: F[Map[GlobalStateKey, HistoricalStakeSnapshot]] =
          acc.historicalStakeSnapshots.toList.parTraverse {
            case (period, entry) => GlobalStateKey.historicalStakeSnapshotsKey[F](period).map(_ -> entry)
          }.map(_.toMap)

        val totalEntries =
          stateChanHashes.size + txRefs.size + balances.size + currencyProofs.size +
            acc.lastCurrencySnapshots.size * 2 +
            activeAllowSpends.size + activeTokenLocksEntries.size + tokenLockBalancesEntries.size +
            lastAllowSpendRefsEntries.size + lastTokenLockRefsEntries.size +
            activeDelegatedStakesEntries.size + delegatedStakesWithdrawalsEntries.size +
            activeNodeCollateralsEntries.size + nodeCollateralWithdrawalsEntries.size +
            metagraphSyncDataEntries.size +
            acc.updateNodeParameters.size + acc.priceState.size +
            acc.historicalStakeSnapshots.size

        for {
          t0 <- Async[F].monotonic.map(_.toMillis)
          keysToRemove <- toRemovalGlobalStateKeys

          _ <- syncLogger.debug(
            s"[MPT.Sync] ordinal=$snapshotOrdinal delta: " +
              s"scHashes=${acc.lastStateChannelSnapshotHashes.size} " +
              s"txRefs=${acc.lastTxRefs.size} " +
              s"balances=${acc.balances.size} " +
              s"currencySnapshots=${acc.lastCurrencySnapshots.size} " +
              s"currencyProofs=${acc.lastCurrencySnapshotsProofs.size} " +
              s"allowSpends=${acc.activeAllowSpends.values.map(_.values.map(_.size).sum).sum} " +
              s"tokenLocks=${acc.activeTokenLocks.values.map(_.size).sum} " +
              s"tokenLockBal=${acc.tokenLockBalances.size} " +
              s"delegStakes=${acc.activeDelegatedStakes.size} " +
              s"delegWithdrawals=${acc.delegatedStakesWithdrawals.size} " +
              s"nodeCollaterals=${acc.activeNodeCollaterals.size} " +
              s"collateralWithdrawals=${acc.nodeCollateralWithdrawals.size} " +
              s"metagraphSync=${acc.metagraphSyncData.size} " +
              s"historicalStake=${acc.historicalStakeSnapshots.size} " +
              s"totalEntries=$totalEntries removals=${keysToRemove.size}"
          )

          // Remove stale keys first (entries that are now empty: AllowSpends, TokenLocks,
          // TokenLockBalances, DelegatedStakes, DelegatedStakeWithdrawals, NodeCollaterals, NodeCollateralWithdrawals)
          _ <- store.remove(keysToRemove.toList).whenA(keysToRemove.nonEmpty)

          currency <- buildCurrencySnapshotEntries
          _ <- store.insert[Hash](stateChanHashes)
          _ <- store.insert[io.constellationnetwork.schema.transaction.TransactionReference](txRefs)
          _ <- store.insert[Balance](balances)
          _ <- store.insert[Signed[CurrencyIncrementalSnapshot]](currency._1)
          // Unrolled per-metagraph info: reconstruct the prior `Mg*` state, then upsert the new info + remove dropped entries
          // (`writeCurrencyInfo` → `infoRemovalKeys`). Replaces the monolithic fieldId-6 blob insert. fieldId-7 `activeAllowSpends`
          // is untouched here (its removals are the accumulator's `removedAllowSpendKeys`, applied in `keysToRemove` above).
          _ <- currency._2.traverse_ {
            case (metagraphAddr, newInfo) =>
              reconstructCurrencyInfoFrom[F](metagraphAddr, CurrencyInfoMpt.fromMptStore(store)).flatMap { priorInfo =>
                writeCurrencyInfo[F](metagraphAddr, newInfo, priorInfo, CurrencyInfoMpt.fromMptStore(store))
              }
          }
          _ <- store.insert[io.constellationnetwork.merkletree.Proof](currencyProofs)
          _ <- store.insert[SortedSet[Signed[io.constellationnetwork.schema.swap.AllowSpend]]](activeAllowSpends)
          _ <- store.insert[SortedSet[Signed[io.constellationnetwork.schema.tokenLock.TokenLock]]](activeTokenLocksEntries)
          _ <- store.insert[Balance](tokenLockBalancesEntries)
          _ <- store.insert[io.constellationnetwork.schema.swap.AllowSpendReference](lastAllowSpendRefsEntries)
          _ <- store.insert[io.constellationnetwork.schema.tokenLock.TokenLockReference](lastTokenLockRefsEntries)
          _ <- store.insert[SortedSet[io.constellationnetwork.schema.delegatedStake.DelegatedStakeRecord]](activeDelegatedStakesEntries)
          _ <- store.insert[SortedSet[io.constellationnetwork.schema.delegatedStake.PendingDelegatedStakeWithdrawal]](
            delegatedStakesWithdrawalsEntries
          )
          _ <- store.insert[SortedSet[io.constellationnetwork.schema.nodeCollateral.NodeCollateralRecord]](activeNodeCollateralsEntries)
          _ <- store.insert[SortedSet[io.constellationnetwork.schema.nodeCollateral.PendingNodeCollateralWithdrawal]](
            nodeCollateralWithdrawalsEntries
          )
          _ <- store.insert[MetagraphSyncDataInfo](metagraphSyncDataEntries)
          updateNodeParametersEntries <- updateNodeParametersEntriesF
          priceStateEntries <- priceStateEntriesF
          historicalStakeEntries <- historicalStakeEntriesF
          _ <- store.insert[(Signed[UpdateNodeParameters], SnapshotOrdinal)](updateNodeParametersEntries)
          _ <- store.insert[PriceRecord](priceStateEntries)
          _ <- store.insert[HistoricalStakeSnapshot](historicalStakeEntries)
          _ <- applySystemIndexDelta[F, AllowSpendExpiryKey](
            store,
            SystemNamespaceLabel.ExpiryIndexAllowSpends,
            acc.allowSpendExpiryIndex
          )
          _ <- applySystemIndexDelta[F, TokenLockExpiryKey](
            store,
            SystemNamespaceLabel.ExpiryIndexTokenLocks,
            acc.tokenLockExpiryIndex
          )
          _ <- applySystemIndexDelta[F, NodeCollateralWithdrawalExpiryKey](
            store,
            SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals,
            acc.nodeCollateralWithdrawalExpiryIndex
          )

          // ActiveAddressIndex maintenance — pilot scope: ref-maps that don't carry an address in their value type.
          // Refs only grow (never removed), so `removed = empty`. Adds = the keyset of this ordinal's delta. The
          // verify replay path mirrors this in `replayActiveAddressIndexDelta` so `expectedBytes == storeBytes`.
          _ <- applyActiveAddressIndexDelta[F](store, LastAllowSpendRefs, acc.lastAllowSpendRefs.keySet.toSet, Set.empty)
          _ <- applyActiveAddressIndexDelta[F](store, LastTokenLockRefs, acc.lastTokenLockRefs.keySet.toSet, Set.empty)
          _ <- applyActiveAddressIndexDelta[F](store, LastTxRefs, acc.lastTxRefs.keySet.toSet, Set.empty)
          _ <- applyActiveAddressIndexDelta[F](store, Balances, acc.balances.keySet.toSet, Set.empty)
          _ <- applyActiveAddressIndexDelta[F](
            store,
            LastStateChannelSnapshotHashes,
            acc.lastStateChannelSnapshotHashes.keySet.toSet,
            Set.empty
          )
          _ <- applyActiveAddressIndexDelta[F](
            store,
            LastCurrencySnapshots,
            acc.lastCurrencySnapshots.keySet.toSet,
            Set.empty
          )
          // Address-pair index for `tokenLockBalances` — `(metagraphAddr, holderAddr)` pairs. Adds come from this
          // ordinal's deltas; removes come from the manager's pair-shaped `removedTokenLockBalanceKeys` so the
          // sidecar prunes in lock-step with the actual MPT entry deletes (otherwise materialize keeps re-reading
          // the stale pair and the diff loop re-emits the same removal every ordinal).
          _ <- applyAddressPairIndexDelta[F](
            store,
            TokenLockBalances,
            acc.tokenLockBalances.iterator.flatMap {
              case (mid, inner) => inner.keysIterator.map(holder => (mid, holder))
            }.toSet,
            acc.removedTokenLockBalanceKeys
          )

          _ <- store.commit(snapshotOrdinal)
          t2 <- Async[F].monotonic.map(_.toMillis)

          // Log total MPT entry count after sync for cross-node comparison
          totalMptEntries <- store.underlying.entries.map(_.size)
          rootHash <- store.underlying.getRootHashForOrdinal(snapshotOrdinal)
          _ <- syncLogger.info(
            s"[MPT.Sync] ordinal=$snapshotOrdinal AFTER: totalMptEntries=$totalMptEntries " +
              s"rootHash=${rootHash.map(_.show.take(12)).getOrElse("none")} " +
              s"totalMs=${t2 - t0}"
          )

        } yield ()
      }
    }
  }
}
