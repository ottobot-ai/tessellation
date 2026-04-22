package io.constellationnetwork.schema.mpt

import cats.Parallel
import cats.effect.{Async, Sync}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.merkletree.Proof
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.mpt.MptStore
import io.constellationnetwork.schema.mpt.PartitionNamespace.AddressNamespace
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
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

import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.typelevel.log4cats.slf4j.Slf4jLogger

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
    removedAllowSpendKeys: Set[(Option[Address], Address)] = Set.empty,
    removedTokenLockKeys: Set[Address] = Set.empty,
    removedTokenLockBalanceKeys: Set[Address] = Set.empty,
    removedDelegatedStakeKeys: Set[Address] = Set.empty,
    removedDelegatedStakeWithdrawalKeys: Set[Address] = Set.empty,
    removedNodeCollateralKeys: Set[Address] = Set.empty,
    removedNodeCollateralWithdrawalKeys: Set[Address] = Set.empty
  )

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

    // Currency snapshots encode as two separate keys (Signed[CurrencyIncrementalSnapshot] + CurrencySnapshotInfo)
    info.lastCurrencySnapshots.toList.parTraverse {
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
    }.map { currencyEntries =>
      val all: Iterable[(GlobalStateKey, Array[Byte])] =
        stateChanHashes ++ txRefs ++ balances ++ currencyProofs ++
          activeAllowSpends ++ activeTokenLocks ++ tokenLockBalances ++
          lastAllowSpendRefs ++ lastTokenLockRefs ++
          activeDelegatedStakes ++ delegatedStakesWithdrawals ++
          activeNodeCollaterals ++ nodeCollateralWithdrawals ++
          metagraphSyncData ++ currencyEntries.flatten
      all.toMap
    }
  }

  object syntax {
    implicit class GlobalSnapshotInfoMptOps(val info: GlobalSnapshotInfo) extends AnyVal {
      def allStateEntries[F[_]: Async: Parallel: Hasher: JsonSerializer](
        implicit stateProofSelector: StateProofSelector
      ): F[Map[GlobalStateKey, Json]] =
        toAllStateKeyValuePairs(info)

      def allStateEntriesAsBytes[F[_]: Async: Parallel: Hasher: JsonSerializer](
        implicit stateProofSelector: StateProofSelector
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
    }

    implicit class MptStoreGlobalSnapshotOps[F[_]: Async: Parallel: Hasher: JsonSerializer](
      val store: MptStore[F, GlobalStateKey]
    ) {

      /** Seed / reset the MptStore from a `GlobalSnapshotInfo`.
        *
        * Writes each typed field via its canonical scodec `ImmutableCodec`, matching the encoding used by the typed read methods
        * (`getBalance`, `getActiveTokenLocks`, etc). Previously this went through `allStateEntries → syncFull[Json]` which produced UTF-8
        * JSON bytes — inconsistent with the scodec-typed reads after the Phase 3a MptStore migration. Tests that round-tripped via this
        * helper + typed reads would see empty reads because the scodec decoder rejected JSON bytes.
        *
        * Consensus note: the production state-proof verification path (`GlobalSnapshotInfo.mptStateProof` → `allStateEntries.buildMpt`)
        * still uses the JSON-bytes-in-MPT construction and will produce a different root than this scodec-byte MptStore. Aligning the two
        * is the remaining piece of the Phase 3c work; callers that depend on hash-level agreement between `sync` and `buildMpt` need the
        * follow-up migration of `makeParallel` / `buildMpt` to scodec.
        */
      def syncFromGlobalSnapshotInfo(info: GlobalSnapshotInfo, snapshotOrdinal: SnapshotOrdinal)(
        implicit stateProofSelector: StateProofSelector
      ): F[Unit] = {
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

        // We avoid per-field `sync` (each would trigger its own trie build). Instead: clear,
        // insert each typed batch, then build once under the exclusive lock at the end.
        store.withExclusiveLock {
          for {
            _ <- store.clear
            currency <- buildCurrencySnapshotEntries
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

        val keysToRemove = toRemovalGlobalStateKeys
        val totalEntries =
          stateChanHashes.size + txRefs.size + balances.size + currencyProofs.size +
            acc.lastCurrencySnapshots.size * 2 +
            activeAllowSpends.size + activeTokenLocksEntries.size + tokenLockBalancesEntries.size +
            lastAllowSpendRefsEntries.size + lastTokenLockRefsEntries.size +
            activeDelegatedStakesEntries.size + delegatedStakesWithdrawalsEntries.size +
            activeNodeCollateralsEntries.size + nodeCollateralWithdrawalsEntries.size +
            metagraphSyncDataEntries.size

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
