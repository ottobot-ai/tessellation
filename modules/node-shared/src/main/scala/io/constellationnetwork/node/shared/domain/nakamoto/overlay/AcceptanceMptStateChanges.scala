package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.Async
import cats.syntax.all._
import cats.{Order, Parallel}

import scala.collection.immutable.SortedSet

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.merkletree.Proof
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nakamoto.StakeDistribution
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
import io.constellationnetwork.schema.priceOracle.PriceRecord
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.swap.{AllowSpend, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.schema.{SnapshotOrdinal, StateProofSelector}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendRefImmutable}
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs.currencySnapshotInfoImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutable}
import io.constellationnetwork.serde.codecs.instances.MerkleTreeCodecs.proofImmutableCodec
import io.constellationnetwork.serde.codecs.instances.MetagraphSyncDataInfoCodec.{immutableCodec => metagraphSyncImmutable}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.PriceOracleCodecs.priceRecordImmutableCodec
import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{immutableCodec => stakeDistributionImmutable}
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

/** Writer-algebra companion to `GlobalStateConverter.syncFromStateChanges`.
  *
  * Mirrors that method's per-field insert order, sidecar maintenance, and removal-key derivation, but routes every mutation through
  * `AcceptanceMpt[F]` (i.e. through the overlay's `BranchHandle`) instead of writing directly to `MptStore`. Under
  * `MptOverlay.OverlayMode.Passthrough` the bytes land immediately in the underlying `MptStore` and the per-key/per-root parity contract
  * from `GsamWritePathParitySuite` (#107) preserves byte-equivalence with the legacy writer. Under `MptOverlay.OverlayMode.MultiBranch` the
  * mutations accumulate in a per-branch `ChangeSet` and are folded into the base by `MptOverlay.finalizeBranch`.
  *
  * '''Sidecar reads''': `applyActiveAddressIndexDelta` / `applyAddressPairIndexDelta` / `applySystemIndexDelta` are read-modify-write on
  * the sidecar partition. Reads go through `mpt.get` — branch-aware — so a multi-branch view sees the parent branch's sidecar state, not
  * the finalized base. Writes route through `mpt.insert`/`mpt.remove`, accumulating in the same branch-local handle.
  */
object AcceptanceMptStateChanges {

  /** Apply `acc`'s deltas through the writer algebra in the same field/insert order as `GlobalStateConverter.syncFromStateChanges`.
    * Identical inputs produce identical bytes — that is the parity contract enforced by `GsamWritePathParitySuite`.
    *
    * Does NOT call `mptStore.commit(snapshotOrdinal)`; the per-ordinal trie checkpoint is the overlay's responsibility (Passthrough's
    * `commit` checkpoints inline; MultiBranch's `finalizeBranch.foldIntoBase` checkpoints at finalize time).
    */
  def applyStateChanges[F[_]: Async: Parallel: Hasher: JsonSerializer](
    mpt: AcceptanceMpt[F],
    acc: StateChangesAccumulator
  )(implicit stateProofSelector: StateProofSelector): F[Unit] = {
    import io.constellationnetwork.schema.mpt.GlobalStateFieldId._

    // `stateProofSelector` is consumed by `CurrencyIncrementalSnapshot.fromCurrencySnapshot` below
    // (transitively) — keeping the binding makes that dependency explicit at the helper boundary.
    // `stakeDistributionImmutable` resolves `mpt.insert[StakeDistribution]` (§3 NIPoPoW S0).
    val _ = (stateProofSelector, stakeDistributionImmutable)

    // Lifted to F because the §3 NIPoPoW historical-stake-snapshot keys hash the eta-period via
    // `Hasher[F]` inside `historicalStakeSnapshotsKey[F]`. All other partitions are F-free.
    def toRemovalKeys: F[Set[GlobalStateKey]] = {
      val allowSpendKeys = acc.removedAllowSpendKeys.map {
        case (metagraphIdOpt, address) =>
          GlobalStateKey.hypergraph(ActiveAllowSpends, metagraphIdOpt, address)
      }
      val tokenLockKeys = acc.removedTokenLockKeys.map(GlobalStateKey.hypergraph(ActiveTokenLocks, _))
      val tokenLockBalanceKeys = acc.removedTokenLockBalanceKeys.map {
        case (mid, holder) => GlobalStateKey.hypergraph(TokenLockBalances, mid, holder)
      }
      val delegatedStakeKeys = acc.removedDelegatedStakeKeys.map(GlobalStateKey.hypergraph(ActiveDelegatedStakes, _))
      val delegatedStakeWithdrawalKeys =
        acc.removedDelegatedStakeWithdrawalKeys.map(GlobalStateKey.hypergraph(DelegatedStakesWithdrawals, _))
      val nodeCollateralKeys = acc.removedNodeCollateralKeys.map(GlobalStateKey.hypergraph(ActiveNodeCollaterals, _))
      val nodeCollateralWithdrawalKeys =
        acc.removedNodeCollateralWithdrawalKeys.map(GlobalStateKey.hypergraph(NodeCollateralWithdrawals, _))
      val pureKeys =
        allowSpendKeys ++ tokenLockKeys ++ tokenLockBalanceKeys ++
          delegatedStakeKeys ++ delegatedStakeWithdrawalKeys ++
          nodeCollateralKeys ++ nodeCollateralWithdrawalKeys
      acc.removedHistoricalStakeSnapshotKeys.toList
        .parTraverse(period => GlobalStateKey.historicalStakeSnapshotsKey[F](period))
        .map(historicalKeys => pureKeys ++ historicalKeys.toSet)
    }

    val stateChanHashes: Map[GlobalStateKey, Hash] = acc.lastStateChannelSnapshotHashes.iterator.map {
      case (addr, h) => GlobalStateKey.metagraph(addr, LastStateChannelSnapshotHashes) -> h
    }.toMap
    val txRefs: Map[GlobalStateKey, io.constellationnetwork.schema.transaction.TransactionReference] =
      acc.lastTxRefs.iterator.map { case (addr, r) => GlobalStateKey.hypergraph(LastTxRefs, addr) -> r }.toMap
    val balances: Map[GlobalStateKey, Balance] =
      acc.balances.iterator.map { case (addr, b) => GlobalStateKey.hypergraph(Balances, addr) -> b }.toMap
    val currencyProofs: Map[GlobalStateKey, Proof] =
      acc.lastCurrencySnapshotsProofs.iterator.map {
        case (addr, p) => GlobalStateKey.metagraph(addr, LastCurrencySnapshotsProofs) -> p
      }.toMap

    def buildCurrencySnapshotEntries: F[
      (
        Map[GlobalStateKey, Signed[CurrencyIncrementalSnapshot]],
        Map[GlobalStateKey, CurrencySnapshotInfo]
      )
    ] =
      acc.lastCurrencySnapshots.toList.parTraverse {
        case (metagraphAddr, Left(fullSnapshot)) =>
          CurrencyIncrementalSnapshot
            .fromCurrencySnapshot(fullSnapshot.value)
            .map { inc =>
              (
                GlobalStateKey.metagraph(metagraphAddr, LastIncrementalCurrencySnapshots) -> Signed(inc, fullSnapshot.proofs),
                GlobalStateKey.metagraph(metagraphAddr, LastCurrencySnapshotInfo) -> fullSnapshot.info.toCurrencySnapshotInfo
              )
            }
        case (metagraphAddr, Right((inc, snInfo))) =>
          (
            (GlobalStateKey.metagraph(metagraphAddr, LastIncrementalCurrencySnapshots) -> inc) ->
              (GlobalStateKey.metagraph(metagraphAddr, LastCurrencySnapshotInfo) -> snInfo)
          ).pure[F]
      }.map { paired =>
        (paired.map(_._1).toMap, paired.map(_._2).toMap)
      }

    val activeAllowSpends: Map[GlobalStateKey, SortedSet[Signed[AllowSpend]]] =
      acc.activeAllowSpends.toList.flatMap {
        case (optAddr, inner) =>
          inner.toList.map { case (addr, s) => GlobalStateKey.hypergraph(ActiveAllowSpends, optAddr, addr) -> s }
      }.toMap
    val activeTokenLocksEntries: Map[GlobalStateKey, SortedSet[Signed[TokenLock]]] =
      acc.activeTokenLocks.iterator.map { case (addr, s) => GlobalStateKey.hypergraph(ActiveTokenLocks, addr) -> s }.toMap
    val tokenLockBalancesEntries: Map[GlobalStateKey, Balance] =
      acc.tokenLockBalances.toList.flatMap {
        case (tokenAddr, inner) =>
          inner.toList.map { case (holder, bal) => GlobalStateKey.hypergraph(TokenLockBalances, tokenAddr, holder) -> bal }
      }.toMap
    val lastAllowSpendRefsEntries: Map[GlobalStateKey, AllowSpendReference] =
      acc.lastAllowSpendRefs.iterator.map { case (addr, r) => GlobalStateKey.hypergraph(LastAllowSpendRefs, addr) -> r }.toMap
    val lastTokenLockRefsEntries: Map[GlobalStateKey, TokenLockReference] =
      acc.lastTokenLockRefs.iterator.map { case (addr, r) => GlobalStateKey.hypergraph(LastTokenLockRefs, addr) -> r }.toMap
    val activeDelegatedStakesEntries: Map[GlobalStateKey, SortedSet[DelegatedStakeRecord]] =
      acc.activeDelegatedStakes.iterator.map {
        case (addr, s) => GlobalStateKey.hypergraph(ActiveDelegatedStakes, addr) -> s
      }.toMap
    val delegatedStakesWithdrawalsEntries: Map[GlobalStateKey, SortedSet[PendingDelegatedStakeWithdrawal]] =
      acc.delegatedStakesWithdrawals.iterator.map {
        case (addr, s) => GlobalStateKey.hypergraph(DelegatedStakesWithdrawals, addr) -> s
      }.toMap
    val activeNodeCollateralsEntries: Map[GlobalStateKey, SortedSet[NodeCollateralRecord]] =
      acc.activeNodeCollaterals.iterator.map {
        case (addr, s) => GlobalStateKey.hypergraph(ActiveNodeCollaterals, addr) -> s
      }.toMap
    val nodeCollateralWithdrawalsEntries: Map[GlobalStateKey, SortedSet[PendingNodeCollateralWithdrawal]] =
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

    // §3 NIPoPoW S0 historical-stake-snapshot upserts. Non-empty only at boundary ordinals; the value is
    // the scodec-encoded `StakeDistribution` keyed by `historicalStakeSnapshotsKey[F](period)`.
    val historicalStakeEntriesF: F[Map[GlobalStateKey, StakeDistribution]] =
      acc.historicalStakeSnapshots.toList.parTraverse {
        case (period, dist) => GlobalStateKey.historicalStakeSnapshotsKey[F](period).map(_ -> dist)
      }.map(_.toMap)

    for {
      keysToRemove <- toRemovalKeys
      // Remove stale keys first — same as the legacy syncFromStateChanges (line 1597).
      _ <- if (keysToRemove.nonEmpty) mpt.remove(keysToRemove.toList) else Async[F].unit

      currency <- buildCurrencySnapshotEntries
      _ <- mpt.insert[Hash](stateChanHashes)
      _ <- mpt.insert[io.constellationnetwork.schema.transaction.TransactionReference](txRefs)
      _ <- mpt.insert[Balance](balances)
      _ <- mpt.insert[Signed[CurrencyIncrementalSnapshot]](currency._1)
      _ <- mpt.insert[CurrencySnapshotInfo](currency._2)
      _ <- mpt.insert[Proof](currencyProofs)
      _ <- mpt.insert[SortedSet[Signed[AllowSpend]]](activeAllowSpends)
      _ <- mpt.insert[SortedSet[Signed[TokenLock]]](activeTokenLocksEntries)
      _ <- mpt.insert[Balance](tokenLockBalancesEntries)
      _ <- mpt.insert[AllowSpendReference](lastAllowSpendRefsEntries)
      _ <- mpt.insert[TokenLockReference](lastTokenLockRefsEntries)
      _ <- mpt.insert[SortedSet[DelegatedStakeRecord]](activeDelegatedStakesEntries)
      _ <- mpt.insert[SortedSet[PendingDelegatedStakeWithdrawal]](delegatedStakesWithdrawalsEntries)
      _ <- mpt.insert[SortedSet[NodeCollateralRecord]](activeNodeCollateralsEntries)
      _ <- mpt.insert[SortedSet[PendingNodeCollateralWithdrawal]](nodeCollateralWithdrawalsEntries)
      _ <- mpt.insert[MetagraphSyncDataInfo](metagraphSyncDataEntries)
      updateNodeParametersEntries <- updateNodeParametersEntriesF
      priceStateEntries <- priceStateEntriesF
      historicalStakeEntries <- historicalStakeEntriesF
      _ <- mpt.insert[(Signed[UpdateNodeParameters], SnapshotOrdinal)](updateNodeParametersEntries)
      _ <- mpt.insert[PriceRecord](priceStateEntries)
      _ <- mpt.insert[StakeDistribution](historicalStakeEntries)

      _ <- applySystemIndexDeltaViaMpt[F, AllowSpendExpiryKey](
        mpt,
        SystemNamespaceLabel.ExpiryIndexAllowSpends,
        acc.allowSpendExpiryIndex
      )
      _ <- applySystemIndexDeltaViaMpt[F, TokenLockExpiryKey](
        mpt,
        SystemNamespaceLabel.ExpiryIndexTokenLocks,
        acc.tokenLockExpiryIndex
      )
      _ <- applySystemIndexDeltaViaMpt[F, NodeCollateralWithdrawalExpiryKey](
        mpt,
        SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals,
        acc.nodeCollateralWithdrawalExpiryIndex
      )

      _ <- applyActiveAddressIndexDeltaViaMpt[F](mpt, LastAllowSpendRefs, acc.lastAllowSpendRefs.keySet.toSet, Set.empty)
      _ <- applyActiveAddressIndexDeltaViaMpt[F](mpt, LastTokenLockRefs, acc.lastTokenLockRefs.keySet.toSet, Set.empty)
      _ <- applyActiveAddressIndexDeltaViaMpt[F](mpt, LastTxRefs, acc.lastTxRefs.keySet.toSet, Set.empty)
      _ <- applyActiveAddressIndexDeltaViaMpt[F](mpt, Balances, acc.balances.keySet.toSet, Set.empty)
      _ <- applyActiveAddressIndexDeltaViaMpt[F](
        mpt,
        LastStateChannelSnapshotHashes,
        acc.lastStateChannelSnapshotHashes.keySet.toSet,
        Set.empty
      )
      _ <- applyActiveAddressIndexDeltaViaMpt[F](
        mpt,
        LastCurrencySnapshots,
        acc.lastCurrencySnapshots.keySet.toSet,
        Set.empty
      )
      _ <- applyAddressPairIndexDeltaViaMpt[F](
        mpt,
        TokenLockBalances,
        acc.tokenLockBalances.iterator.flatMap {
          case (mid, inner) => inner.keysIterator.map(holder => (mid, holder))
        }.toSet,
        acc.removedTokenLockBalanceKeys
      )
    } yield ()
  }

  /** `ActiveAddressIndex` partition RMW for `fieldId`, against the writer-algebra. Mirrors
    * `GlobalStateConverter.applyActiveAddressIndexDelta` exactly — the ONLY difference is the read source (overlay branch view via
    * `mpt.get`) and the write source (handle accumulation via `mpt.insert` / `mpt.remove`).
    */
  private def applyActiveAddressIndexDeltaViaMpt[F[_]: Async: Hasher](
    mpt: AcceptanceMpt[F],
    fieldId: GlobalStateFieldId,
    added: Set[Address],
    removed: Set[Address]
  ): F[Unit] =
    if (added.isEmpty && removed.isEmpty) Async[F].unit
    else
      GlobalStateKey.activeAddressIndexKey[F](fieldId).flatMap { key =>
        for {
          existing <- mpt.get[SortedSet[Address]](key).map(_.getOrElse(SortedSet.empty[Address]))
          merged = (existing ++ added) -- removed
          _ <-
            if (merged.isEmpty && existing.nonEmpty) mpt.remove(key)
            else if (merged.nonEmpty && merged != existing) mpt.insert[SortedSet[Address]](key, merged)
            else Async[F].unit
        } yield ()
      }

  /** Address-pair index RMW. Mirrors `GlobalStateConverter.applyAddressPairIndexDelta`. */
  private def applyAddressPairIndexDeltaViaMpt[F[_]: Async: Hasher](
    mpt: AcceptanceMpt[F],
    fieldId: GlobalStateFieldId,
    added: Set[(Address, Address)],
    removed: Set[(Address, Address)]
  ): F[Unit] =
    if (added.isEmpty && removed.isEmpty) Async[F].unit
    else
      GlobalStateKey.activeAddressIndexKey[F](fieldId).flatMap { key =>
        for {
          existing <- mpt.get[SortedSet[(Address, Address)]](key).map(_.getOrElse(SortedSet.empty[(Address, Address)]))
          merged = (existing ++ added) -- removed
          _ <-
            if (merged.isEmpty && existing.nonEmpty) mpt.remove(key)
            else if (merged.nonEmpty && merged != existing) mpt.insert[SortedSet[(Address, Address)]](key, merged)
            else Async[F].unit
        } yield ()
      }

  /** `SystemIndexDelta` per-epoch bucket RMW. Mirrors `GlobalStateConverter.applySystemIndexDelta`. */
  private def applySystemIndexDeltaViaMpt[F[_]: Async: Hasher, K: Order](
    mpt: AcceptanceMpt[F],
    label: SystemNamespaceLabel,
    delta: SystemIndexDelta[K]
  )(implicit codec: ImmutableCodec[SortedSet[K]]): F[Unit] = delta match {
    case eb: SystemIndexDelta.EpochBucket[K] if eb.isEmpty => Async[F].unit
    case eb: SystemIndexDelta.EpochBucket[K] =>
      implicit val ordering: Ordering[K] = Order[K].toOrdering
      val _ = (ordering, codec) // implicit binders below resolve via context-bound + this method's `codec` param
      eb.touchedEpochs.toList.traverse_ { (epoch: EpochProgress) =>
        val toAdd = eb.adds.getOrElse(epoch, Set.empty[K])
        val toRemove = eb.removes.getOrElse(epoch, Set.empty[K])
        GlobalStateKey.expiryIndexKey[F](label, epoch).flatMap { key =>
          mpt.get[SortedSet[K]](key).map(_.getOrElse(SortedSet.empty[K])).flatMap { existing =>
            val merged = (existing ++ toAdd) -- toRemove
            if (merged.isEmpty && existing.nonEmpty) mpt.remove(key)
            else if (merged.nonEmpty && merged != existing) mpt.insert[SortedSet[K]](key, merged)
            else Async[F].unit
          }
        }
      }
  }
}
