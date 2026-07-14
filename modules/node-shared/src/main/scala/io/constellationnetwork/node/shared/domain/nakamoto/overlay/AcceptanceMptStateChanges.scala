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
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nakamoto.HistoricalStakeSnapshot
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
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutable}
import io.constellationnetwork.serde.codecs.instances.KesRegistrationCodecs.kesRegistrationReferenceImmutableCodec
import io.constellationnetwork.serde.codecs.instances.MerkleTreeCodecs.proofImmutableCodec
import io.constellationnetwork.serde.codecs.instances.MetagraphSyncDataInfoCodec.{immutableCodec => metagraphSyncImmutable}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.PriceOracleCodecs.priceRecordImmutableCodec
import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{historicalImmutableCodec => historicalStakeSnapshotImmutable}
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

/** Writer-algebra companion to `GlobalStateConverter.syncFromStateChanges`.
  *
  * Mirrors that method's per-field insert order, rooted consensus-index maintenance, and removal-key derivation, but routes every mutation
  * through `AcceptanceMpt[F]` (i.e. through the overlay's `BranchHandle`) instead of writing directly to `MptStore`. Under
  * `MptOverlay.OverlayMode.Passthrough` the bytes land immediately in the underlying `MptStore` and the per-key/per-root parity contract
  * from `GsamWritePathParitySuite` (#107) preserves byte-equivalence with the legacy writer. Under `MptOverlay.OverlayMode.MultiBranch` the
  * mutations accumulate in a per-branch `ChangeSet` and are folded into the base by `MptOverlay.finalizeBranch`.
  *
  * '''Consensus-index reads''': `applyActiveAddressIndexDelta` / `applyAddressPairIndexDelta` / `applySystemIndexDelta` are
  * read-modify-write on the rooted System partition. Reads go through `mpt.getStrict` — branch-aware and decode-strict — so a multi-branch
  * view sees the parent branch's index state, not the finalized base. Writes route through `mpt.insert`/`mpt.remove`, accumulating in the
  * same branch-local handle.
  */
object AcceptanceMptStateChanges {

  /** Apply `acc`'s deltas through the writer algebra in the same field/insert order as `GlobalStateConverter.syncFromStateChanges`.
    * Identical inputs produce identical bytes — that is the parity contract enforced by `GsamWritePathParitySuite`.
    *
    * Does NOT call `mptStore.commit(snapshotOrdinal)`; the per-ordinal trie checkpoint is the overlay's responsibility (Passthrough's
    * `commit` checkpoints inline; MultiBranch's `finalizeBranch.foldIntoBase` checkpoints at finalize time).
    *
    * Currency-info removals are derived from this same branch-aware `mpt` before writes are applied. The read prior, removal prior, and
    * write target therefore always describe one checked-out parent branch.
    */
  def applyStateChanges[F[_]: Async: Parallel: Hasher: JsonSerializer](
    mpt: AcceptanceMpt[F],
    acc: StateChangesAccumulator
  )(implicit stateProofSelector: StateProofSelector): F[Unit] = {
    import io.constellationnetwork.schema.mpt.GlobalStateFieldId._

    // `stateProofSelector` is consumed by `CurrencyIncrementalSnapshot.fromCurrencySnapshot` below
    // (transitively) — keeping the binding makes that dependency explicit at the helper boundary.
    // `historicalStakeSnapshotImmutable` resolves `mpt.insert[HistoricalStakeSnapshot]` (§3 NIPoPoW S0;
    // Path 1 heap-leak workstream extended the partition value to (stakes, eta)).
    val _ = (stateProofSelector, historicalStakeSnapshotImmutable)

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

    // fieldId-5 incremental entries (unchanged) PLUS the per-MG `(mgAddr, newInfo)` the unrolled `Mg*` info
    // write needs (HARD RULE 3: `Right` -> the carried `snInfo`; `Left`/genesis -> `info.toCurrencySnapshotInfo`).
    // The monolithic fieldId-6 `LastCurrencySnapshotInfo` blob is NO LONGER written — see `writeCurrencyInfo` below.
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
    // the scodec-encoded `HistoricalStakeSnapshot` (combined stake + eta record) keyed by
    // `historicalStakeSnapshotsKey[F](period)`.
    val historicalStakeEntriesF: F[Map[GlobalStateKey, HistoricalStakeSnapshot]] =
      acc.historicalStakeSnapshots.toList.parTraverse {
        case (period, entry) => GlobalStateKey.historicalStakeSnapshotsKey[F](period).map(_ -> entry)
      }.map(_.toMap)

    val kesRegistrationCertEntriesF: F[Map[GlobalStateKey, SortedSet[KesRegistrationRecord]]] =
      acc.kesRegistrationCerts.toList.parTraverse {
        case (peerId, records) => GlobalStateKey.kesRegistrationCertsKey[F](peerId).map(_ -> records)
      }.map(_.toMap)

    val lastKesRegistrationRefEntriesF: F[Map[GlobalStateKey, KesRegistrationReference]] =
      acc.lastKesRegistrationRefs.toList.parTraverse {
        case (peerId, ref) => GlobalStateKey.lastKesRegistrationRefsKey[F](peerId).map(_ -> ref)
      }.map(_.toMap)

    for {
      // Fail before staging any branch mutation when a rooted System index is present but undecodable. The RMW helpers repeat the strict
      // read at their write site; this preflight gives applyStateChanges an all-or-nothing malformed-input boundary even for callers that
      // inspect or accidentally reuse a failed handle.
      _ <- preflightSystemIndexReads(mpt, acc)
      keysToRemove <- toRemovalKeys
      // Remove stale keys first — same as the legacy syncFromStateChanges (line 1597).
      _ <- if (keysToRemove.nonEmpty) mpt.remove(keysToRemove.toList) else Async[F].unit

      currency <- buildCurrencySnapshotEntries
      _ <- mpt.insert[Hash](stateChanHashes)
      _ <- mpt.insert[io.constellationnetwork.schema.transaction.TransactionReference](txRefs)
      _ <- mpt.insert[Balance](balances)
      _ <- mpt.insert[Signed[CurrencyIncrementalSnapshot]](currency._1)
      // fieldId-6 monolithic blob REPLACED by the unrolled per-entry `Mg*` partitions (UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN §6):
      // for each MG, reconstruct the prior info from the branch-aware overlay view (the same `getAllForPrefix` the writer accumulates
      // into), then upsert all eight serialized `Mg*` partitions (seven rooted fields plus the transitional root-excluded sync view) and
      // remove dropped entries via the shared `writeCurrencyInfo` (byte-identical to `infoEntryBytes`).
      // Does NOT touch fieldId-5 (above) nor activeAllowSpends/fieldId-7 (the `removedAllowSpendKeys` path).
      _ <- currency._2.traverse_ {
        case (metagraphAddr, newInfo) =>
          val infoMpt = CurrencyInfoMptAdapters.mptFor[F](mpt)
          GlobalStateConverter
            .reconstructCurrencyInfoFrom[F](metagraphAddr, infoMpt)
            .flatMap(priorInfo => GlobalStateConverter.writeCurrencyInfo[F](metagraphAddr, newInfo, priorInfo, infoMpt))
      }
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
      kesRegistrationCertEntries <- kesRegistrationCertEntriesF
      lastKesRegistrationRefEntries <- lastKesRegistrationRefEntriesF
      _ <- mpt.insert[(Signed[UpdateNodeParameters], SnapshotOrdinal)](updateNodeParametersEntries)
      _ <- mpt.insert[PriceRecord](priceStateEntries)
      _ <- mpt.insert[HistoricalStakeSnapshot](historicalStakeEntries)
      _ <- mpt.insert[SortedSet[KesRegistrationRecord]](kesRegistrationCertEntries)
      _ <- mpt.insert[KesRegistrationReference](lastKesRegistrationRefEntries)

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
      _ <- applyActiveAddressIndexDeltaViaMpt[F](
        mpt,
        LastCurrencySnapshotsProofs,
        acc.lastCurrencySnapshotsProofs.keySet.toSet,
        Set.empty
      )
      _ <- applyActiveAddressIndexDeltaViaMpt[F](
        mpt,
        MetagraphSyncData,
        acc.metagraphSyncData.keySet.toSet,
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

  private def preflightSystemIndexReads[F[_]: Async: Hasher](
    mpt: AcceptanceMpt[F],
    acc: StateChangesAccumulator
  ): F[Unit] = {
    import GlobalStateFieldId._

    def activeAddress(fieldId: GlobalStateFieldId, touched: Boolean): F[Unit] =
      if (!touched) Async[F].unit
      else
        for {
          key <- GlobalStateKey.activeAddressIndexKey[F](fieldId)
          hexKey <- GlobalStateKey.toHex[F](key)
          _ <- StrictMptRead
            .valueOrElseF(
              mpt.getStrict[SortedSet[Address]](key),
              SortedSet.empty[Address],
              s"overlay preflight ActiveAddressIndex(fieldId=${fieldId.toInt})",
              hexKey
            )
            .void
        } yield ()

    def addressPairs(touched: Boolean): F[Unit] =
      if (!touched) Async[F].unit
      else
        for {
          key <- GlobalStateKey.activeAddressIndexKey[F](TokenLockBalances)
          hexKey <- GlobalStateKey.toHex[F](key)
          _ <- StrictMptRead
            .valueOrElseF(
              mpt.getStrict[SortedSet[(Address, Address)]](key),
              SortedSet.empty[(Address, Address)],
              s"overlay preflight ActiveAddressIndexPair(fieldId=${TokenLockBalances.toInt})",
              hexKey
            )
            .void
        } yield ()

    def expiry[K: Order](
      label: SystemNamespaceLabel,
      delta: SystemIndexDelta[K]
    )(implicit codec: ImmutableCodec[SortedSet[K]]): F[Unit] = delta match {
      case eb: SystemIndexDelta.EpochBucket[K] =>
        implicit val ordering: Ordering[K] = Order[K].toOrdering
        eb.touchedEpochs.toList.traverse_ { epoch =>
          for {
            key <- GlobalStateKey.expiryIndexKey[F](label, epoch)
            hexKey <- GlobalStateKey.toHex[F](key)
            _ <- StrictMptRead
              .valueOrElseF(
                mpt.getStrict[SortedSet[K]](key),
                SortedSet.empty[K],
                s"overlay preflight ExpiryIndex(label=${label.canonicalName},epoch=${epoch.value.value})",
                hexKey
              )
              .void
          } yield ()
        }
    }

    for {
      _ <- expiry(SystemNamespaceLabel.ExpiryIndexAllowSpends, acc.allowSpendExpiryIndex)
      _ <- expiry(SystemNamespaceLabel.ExpiryIndexTokenLocks, acc.tokenLockExpiryIndex)
      _ <- expiry(SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals, acc.nodeCollateralWithdrawalExpiryIndex)
      _ <- activeAddress(LastAllowSpendRefs, acc.lastAllowSpendRefs.nonEmpty)
      _ <- activeAddress(LastTokenLockRefs, acc.lastTokenLockRefs.nonEmpty)
      _ <- activeAddress(LastTxRefs, acc.lastTxRefs.nonEmpty)
      _ <- activeAddress(Balances, acc.balances.nonEmpty)
      _ <- activeAddress(LastStateChannelSnapshotHashes, acc.lastStateChannelSnapshotHashes.nonEmpty)
      _ <- activeAddress(LastCurrencySnapshots, acc.lastCurrencySnapshots.nonEmpty)
      _ <- activeAddress(LastCurrencySnapshotsProofs, acc.lastCurrencySnapshotsProofs.nonEmpty)
      _ <- activeAddress(MetagraphSyncData, acc.metagraphSyncData.nonEmpty)
      _ <- addressPairs(acc.tokenLockBalances.nonEmpty || acc.removedTokenLockBalanceKeys.nonEmpty)
    } yield ()
  }

  /** `ActiveAddressIndex` partition RMW for `fieldId`, against the writer-algebra. Mirrors
    * `GlobalStateConverter.applyActiveAddressIndexDelta` exactly — the ONLY difference is the read source (overlay branch view via
    * `mpt.getStrict`) and the write source (handle accumulation via `mpt.insert` / `mpt.remove`).
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
          hexKey <- GlobalStateKey.toHex[F](key)
          existing <- StrictMptRead.valueOrElseF(
            mpt.getStrict[SortedSet[Address]](key),
            SortedSet.empty[Address],
            s"overlay ActiveAddressIndex(fieldId=${fieldId.toInt})",
            hexKey
          )
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
          hexKey <- GlobalStateKey.toHex[F](key)
          existing <- StrictMptRead.valueOrElseF(
            mpt.getStrict[SortedSet[(Address, Address)]](key),
            SortedSet.empty[(Address, Address)],
            s"overlay ActiveAddressIndexPair(fieldId=${fieldId.toInt})",
            hexKey
          )
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
      // Resolve every touched bucket before staging any write, so one malformed bucket cannot leave an earlier bucket partially changed.
      eb.touchedEpochs.toList.traverse { (epoch: EpochProgress) =>
        GlobalStateKey.expiryIndexKey[F](label, epoch).flatMap { key =>
          GlobalStateKey.toHex[F](key).flatMap { hexKey =>
            StrictMptRead
              .valueOrElseF(
                mpt.getStrict[SortedSet[K]](key),
                SortedSet.empty[K],
                s"overlay ExpiryIndex(label=${label.canonicalName},epoch=${epoch.value.value})",
                hexKey
              )
              .map { existing =>
                val toAdd = eb.adds.getOrElse(epoch, Set.empty[K])
                val toRemove = eb.removes.getOrElse(epoch, Set.empty[K])
                (key, existing, (existing ++ toAdd) -- toRemove)
              }
          }
        }
      }
        .flatMap(
          _.traverse_ {
            case (key, existing, merged) =>
              if (merged.isEmpty && existing.nonEmpty) mpt.remove(key)
              else if (merged.nonEmpty && merged != existing) mpt.insert[SortedSet[K]](key, merged)
              else Async[F].unit
          }
        )
  }
}
