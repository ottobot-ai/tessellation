package io.constellationnetwork.schema.mpt

import cats.effect.{Async, Sync}
import cats.syntax.all._
import cats.{Order, Parallel}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.merkletree.Proof
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.MptStore
import io.constellationnetwork.schema.mpt.PartitionNamespace.AddressNamespace
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
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
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs.currencySnapshotInfoImmutableCodec
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
    removedTokenLockBalanceKeys: Set[Address] = Set.empty,
    removedDelegatedStakeKeys: Set[Address] = Set.empty,
    removedDelegatedStakeWithdrawalKeys: Set[Address] = Set.empty,
    removedNodeCollateralKeys: Set[Address] = Set.empty,
    removedNodeCollateralWithdrawalKeys: Set[Address] = Set.empty
  )

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
        CurrencyIncrementalSnapshot.fromCurrencySnapshot(fullSnapshot.value).map { currencyIncrementalSnapshot =>
          List(
            GlobalStateKey.metagraph(metagraphAddr, GlobalStateFieldId.LastIncrementalCurrencySnapshots) -> Signed(
              currencyIncrementalSnapshot,
              fullSnapshot.proofs
            ).asJson,
            GlobalStateKey
              .metagraph(metagraphAddr, GlobalStateFieldId.LastCurrencySnapshotInfo) -> fullSnapshot.info.toCurrencySnapshotInfo.asJson
          )
        }

      case (metagraphAddr, Right((incrementalSnapshot, snapshotInfo))) =>
        List(
          GlobalStateKey.metagraph(metagraphAddr, GlobalStateFieldId.LastIncrementalCurrencySnapshots) -> incrementalSnapshot.asJson,
          GlobalStateKey.metagraph(metagraphAddr, GlobalStateFieldId.LastCurrencySnapshotInfo) -> snapshotInfo.asJson
        ).pure[F]
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
    import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs.currencySnapshotInfoImmutableCodec
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

    // Currency snapshots encode as two separate keys (Signed[CurrencyIncrementalSnapshot] + CurrencySnapshotInfo)
    val currencyEntriesF = info.lastCurrencySnapshots.toList.parTraverse {
      case (metagraphAddr, Left(fullSnapshot)) =>
        CurrencyIncrementalSnapshot.fromCurrencySnapshot(fullSnapshot.value).map { inc =>
          List(
            GlobalStateKey.metagraph(metagraphAddr, LastIncrementalCurrencySnapshots) ->
              enc[Signed[CurrencyIncrementalSnapshot]](Signed(inc, fullSnapshot.proofs)),
            GlobalStateKey.metagraph(metagraphAddr, LastCurrencySnapshotInfo) ->
              enc[CurrencySnapshotInfo](fullSnapshot.info.toCurrencySnapshotInfo)
          )
        }
      case (metagraphAddr, Right((inc, snInfo))) =>
        List(
          GlobalStateKey.metagraph(metagraphAddr, LastIncrementalCurrencySnapshots) -> enc[Signed[CurrencyIncrementalSnapshot]](inc),
          GlobalStateKey.metagraph(metagraphAddr, LastCurrencySnapshotInfo) -> enc[CurrencySnapshotInfo](snInfo)
        ).pure[F]
    }

    (
      currencyEntriesF,
      updateNodeParametersF,
      priceStateF,
      allowSpendExpiryIndexF,
      tokenLockExpiryIndexF,
      nodeCollateralWithdrawalExpiryIndexF
    ).mapN { (currencyEntries, unpEntries, priceEntries, allowSpendExpiryEntries, tokenLockExpiryEntries, ncwExpiryEntries) =>
      val all: Iterable[(GlobalStateKey, Array[Byte])] =
        stateChanHashes ++ txRefs ++ balances ++ currencyProofs ++
          activeAllowSpends ++ activeTokenLocks ++ tokenLockBalances ++
          lastAllowSpendRefs ++ lastTokenLockRefs ++
          activeDelegatedStakes ++ delegatedStakesWithdrawals ++
          activeNodeCollaterals ++ nodeCollateralWithdrawals ++
          metagraphSyncData ++ currencyEntries.flatten ++ unpEntries ++ priceEntries ++
          allowSpendExpiryEntries ++ tokenLockExpiryEntries ++ ncwExpiryEntries
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
    import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs.currencySnapshotInfoImmutableCodec
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
        CurrencyIncrementalSnapshot.fromCurrencySnapshot(fullSnapshot.value).map { inc =>
          List(
            GlobalStateKey.metagraph(metagraphAddr, LastIncrementalCurrencySnapshots) ->
              enc[Signed[CurrencyIncrementalSnapshot]](Signed(inc, fullSnapshot.proofs)),
            GlobalStateKey.metagraph(metagraphAddr, LastCurrencySnapshotInfo) ->
              enc[CurrencySnapshotInfo](fullSnapshot.info.toCurrencySnapshotInfo)
          )
        }
      case (metagraphAddr, Right((inc, snInfo))) =>
        List(
          GlobalStateKey.metagraph(metagraphAddr, LastIncrementalCurrencySnapshots) -> enc[Signed[CurrencyIncrementalSnapshot]](inc),
          GlobalStateKey.metagraph(metagraphAddr, LastCurrencySnapshotInfo) -> enc[CurrencySnapshotInfo](snInfo)
        ).pure[F]
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

    (currencyEntriesF, updateNodeParametersF, priceStateF).mapN { (currencyEntries, unpEntries, priceEntries) =>
      val all: Iterable[(GlobalStateKey, Array[Byte])] =
        stateChanHashes ++ txRefs ++ balances ++ currencyProofs ++
          activeAllowSpends ++ activeTokenLocks ++ tokenLockBalances ++
          lastAllowSpendRefs ++ lastTokenLockRefs ++
          activeDelegatedStakes ++ delegatedStakesWithdrawals ++
          activeNodeCollaterals ++ nodeCollateralWithdrawals ++
          metagraphSyncData ++ currencyEntries.flatten ++ unpEntries ++ priceEntries
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
    preSyncBytes: Map[Hex, Array[Byte]]
  )(implicit stateProofSelector: StateProofSelector): F[(Map[Hex, Array[Byte]], Set[Hex])] =
    for {
      typedUpserts <- toAccumulatorBytesDelta[F](acc)
      upsertsHex <- typedUpserts.toList.parTraverse { case (k, v) => GlobalStateKey.toHex[F](k).map(_ -> v) }.map(_.toMap)
      removalsHex <- toAccumulatorRemovalKeys(acc).toList.parTraverse(GlobalStateKey.toHex[F]).map(_.toSet)
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
      tlbAddrPairIdx <- replayAddressPairIndexDelta[F](
        GlobalStateFieldId.TokenLockBalances,
        acc.tokenLockBalances.iterator.flatMap {
          case (mid, inner) => inner.keysIterator.map(holder => (mid, holder))
        }.toSet,
        Set.empty,
        preSyncBytes
      )
    } yield
      (
        upsertsHex ++ asExp._1 ++ tlExp._1 ++ ncwExp._1 ++ asAddrIdx._1 ++ tlAddrIdx._1 ++ txAddrIdx._1 ++ tlbAddrPairIdx._1,
        removalsHex ++ asExp._2 ++ tlExp._2 ++ ncwExp._2 ++ asAddrIdx._2 ++ tlAddrIdx._2 ++ txAddrIdx._2 ++ tlbAddrPairIdx._2
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
    */
  def toAccumulatorRemovalKeys(acc: StateChangesAccumulator): Set[GlobalStateKey] = {
    import io.constellationnetwork.schema.mpt.GlobalStateFieldId._
    val allowSpendKeys = acc.removedAllowSpendKeys.toList.map {
      case (metagraphIdOpt, address) => GlobalStateKey.hypergraph(ActiveAllowSpends, metagraphIdOpt, address)
    }
    val tokenLockKeys = acc.removedTokenLockKeys.toList.map(addr => GlobalStateKey.hypergraph(ActiveTokenLocks, addr))
    val tokenLockBalanceKeys = acc.removedTokenLockBalanceKeys.toList.map(addr => GlobalStateKey.hypergraph(TokenLockBalances, addr))
    val delegatedStakeKeys = acc.removedDelegatedStakeKeys.toList.map(addr => GlobalStateKey.hypergraph(ActiveDelegatedStakes, addr))
    val delegatedStakeWithdrawalKeys =
      acc.removedDelegatedStakeWithdrawalKeys.toList.map(addr => GlobalStateKey.hypergraph(DelegatedStakesWithdrawals, addr))
    val nodeCollateralKeys = acc.removedNodeCollateralKeys.toList.map(addr => GlobalStateKey.hypergraph(ActiveNodeCollaterals, addr))
    val nodeCollateralWithdrawalKeys =
      acc.removedNodeCollateralWithdrawalKeys.toList.map(addr => GlobalStateKey.hypergraph(NodeCollateralWithdrawals, addr))
    (allowSpendKeys ++ tokenLockKeys ++ tokenLockBalanceKeys ++
      delegatedStakeKeys ++ delegatedStakeWithdrawalKeys ++
      nodeCollateralKeys ++ nodeCollateralWithdrawalKeys).toSet
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
              if (entries.isEmpty) (fieldId -> Hash.empty).pure[F]
              else
                entries.toList.parTraverse { case (k, v) => GlobalStateKey.toHex[F](k).map(_ -> v) }
                  .flatMap(pairs => MerklePatriciaTrie.makeParallelFromBytes[F](pairs.toMap).map(t => fieldId -> t.rootHash.value))
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

      def getCurrencySnapshotInfo(metagraphAddress: Address): F[Option[CurrencySnapshotInfo]] =
        store
          .get[CurrencySnapshotInfo](GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastCurrencySnapshotInfo))

      def getCurrencySnapshotProof(metagraphAddress: Address): F[Option[Proof]] =
        store
          .get[Proof](GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastCurrencySnapshotsProofs))

      def getMetagraphSyncData(metagraphAddress: Address): F[Option[MetagraphSyncDataInfo]] =
        store
          .get[MetagraphSyncDataInfo](GlobalStateKey.hypergraph(GlobalStateFieldId.MetagraphSyncData, metagraphAddress))

      def getUpdateNodeParameters(
        id: Id
      )(implicit H: Hasher[F]): F[Option[(Signed[UpdateNodeParameters], SnapshotOrdinal)]] =
        GlobalStateKey.updateNodeParametersKey[F](id).flatMap(store.get[(Signed[UpdateNodeParameters], SnapshotOrdinal)])

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
        */
      def syncFromGlobalSnapshotInfo(
        info: GlobalSnapshotInfo,
        snapshotOrdinal: SnapshotOrdinal
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
        import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs.currencySnapshotInfoImmutableCodec
        import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
        import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutable}
        import io.constellationnetwork.serde.codecs.instances.MerkleTreeCodecs.proofImmutableCodec
        import io.constellationnetwork.serde.codecs.instances.MetagraphSyncDataInfoCodec.{immutableCodec => metagraphSyncImmutable}
        import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
        import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
        import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

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
        // is stored as two separate keys (LastIncrementalCurrencySnapshots + LastCurrencySnapshotInfo).
        def buildCurrencySnapshotEntries: F[
          (
            Map[GlobalStateKey, Signed[io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot]],
            Map[GlobalStateKey, io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo]
          )
        ] =
          info.lastCurrencySnapshots.toList.parTraverse {
            case (metagraphAddr, Left(fullSnapshot)) =>
              io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
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
            val snapshotEntries = paired.map(_._1).toMap
            val infoEntries = paired.map(_._2).toMap
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
        // insert each typed batch, then build once under the exclusive lock at the end.
        store.withExclusiveLock {
          for {
            _ <- store.clear
            currency <- buildCurrencySnapshotEntries
            updateNodeParametersEntries <- updateNodeParametersEntriesF
            priceStateEntries <- priceStateEntriesF
            allowSpendExpiryBuckets <- allowSpendExpiryBucketsF
            tokenLockExpiryBuckets <- tokenLockExpiryBucketsF
            nodeCollateralWithdrawalExpiryBuckets <- nodeCollateralWithdrawalExpiryBucketsF
            _ <- store.insert[Hash](stateChanHashes)
            _ <- store.insert[io.constellationnetwork.schema.transaction.TransactionReference](txRefs)
            _ <- store.insert[Balance](balances)
            _ <- store.insert[Signed[io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot]](currency._1)
            _ <- store.insert[io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo](currency._2)
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
                (LastTxRefs, info.lastTxRefs.keySet.to(SortedSet))
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
            _ <- store.build(snapshotOrdinal).void
          } yield ()
        }
      }

      def syncFromStateChanges(acc: StateChangesAccumulator, snapshotOrdinal: SnapshotOrdinal)(
        implicit stateProofSelector: StateProofSelector
      ): F[Unit] = store.withExclusiveLock {
        import io.constellationnetwork.schema.mpt.GlobalStateFieldId._

        val syncLogger = Slf4jLogger.getLoggerFromName[F]("MPT.Sync")

        // Convert removal keys from accumulator to GlobalStateKey
        def toRemovalGlobalStateKeys: Set[GlobalStateKey] = {
          val allowSpendKeys = acc.removedAllowSpendKeys.map {
            case (metagraphIdOpt, address) =>
              GlobalStateKey.hypergraph(ActiveAllowSpends, metagraphIdOpt, address)
          }
          val tokenLockKeys = acc.removedTokenLockKeys.map { address =>
            GlobalStateKey.hypergraph(ActiveTokenLocks, address)
          }
          val tokenLockBalanceKeys = acc.removedTokenLockBalanceKeys.map { metagraphAddress =>
            GlobalStateKey.hypergraph(TokenLockBalances, metagraphAddress)
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
          allowSpendKeys ++ tokenLockKeys ++ tokenLockBalanceKeys ++
            delegatedStakeKeys ++ delegatedStakeWithdrawalKeys ++
            nodeCollateralKeys ++ nodeCollateralWithdrawalKeys
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

        val keysToRemove = toRemovalGlobalStateKeys
        val totalEntries =
          stateChanHashes.size + txRefs.size + balances.size + currencyProofs.size +
            acc.lastCurrencySnapshots.size * 2 +
            activeAllowSpends.size + activeTokenLocksEntries.size + tokenLockBalancesEntries.size +
            lastAllowSpendRefsEntries.size + lastTokenLockRefsEntries.size +
            activeDelegatedStakesEntries.size + delegatedStakesWithdrawalsEntries.size +
            activeNodeCollateralsEntries.size + nodeCollateralWithdrawalsEntries.size +
            metagraphSyncDataEntries.size +
            acc.updateNodeParameters.size + acc.priceState.size

        for {
          t0 <- Async[F].monotonic.map(_.toMillis)

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
          _ <- store.insert[CurrencySnapshotInfo](currency._2)
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
          _ <- store.insert[(Signed[UpdateNodeParameters], SnapshotOrdinal)](updateNodeParametersEntries)
          _ <- store.insert[PriceRecord](priceStateEntries)
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
          // Address-pair index for `tokenLockBalances` — `(metagraphAddr, holderAddr)` pairs. Same append-only
          // discipline; materializeTokenLockBalancesFromMpt point-reads each pair and skips Nones, so stale
          // pairs in the sidecar self-prune at the read boundary.
          _ <- applyAddressPairIndexDelta[F](
            store,
            TokenLockBalances,
            acc.tokenLockBalances.iterator.flatMap {
              case (mid, inner) => inner.keysIterator.map(holder => (mid, holder))
            }.toSet,
            Set.empty
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
