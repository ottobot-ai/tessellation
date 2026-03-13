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

import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.typelevel.log4cats.slf4j.Slf4jLogger

object GlobalStateConverter {

  private val MptDebugEnabled: Boolean =
    sys.env.get("CL_MPT_DEBUG_DUMP").exists(_.toLowerCase == "true")

  private val MptDebugDir: java.nio.file.Path =
    java.nio.file.Paths.get(sys.env.getOrElse("CL_MPT_DEBUG_DIR", "/tmp/mpt-debug"))

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

  /** Temporary debug function: dumps MPT delta inputs and full state fingerprint to disk for cross-node comparison. Enable with
    * CL_MPT_DEBUG_DUMP=true env var. Output dir: CL_MPT_DEBUG_DIR (default /tmp/mpt-debug).
    *
    * Files per ordinal:
    *   - {ordinal}-delta.txt: sorted hex_key + value_hash + fieldId for entries being upserted
    *   - {ordinal}-removals.txt: sorted hex_key + fieldId for keys being removed
    *   - {ordinal}-full-state.txt: sorted hex_key + value_hash for ALL entries after sync
    *
    * Diff these files between nodes to find exactly which entries diverge.
    */
  private def dumpMptDebugSnapshot[F[_]: Async: Parallel: Hasher: JsonSerializer](
    snapshotOrdinal: SnapshotOrdinal,
    store: MptStore[F, GlobalStateKey],
    deltaEntries: Map[GlobalStateKey, Json],
    removalKeys: Set[GlobalStateKey]
  ): F[Unit] = {
    val debugLogger = Slf4jLogger.getLoggerFromName[F]("MPT.Debug")
    val ordStr = snapshotOrdinal.value.value.toString

    def writeFile(name: String, content: String): F[Unit] =
      Async[F].blocking {
        val _ = java.nio.file.Files.createDirectories(MptDebugDir)
        val _ = java.nio.file.Files.writeString(
          MptDebugDir.resolve(name),
          content,
          java.nio.charset.StandardCharsets.UTF_8
        )
      }

    (for {
      _ <- debugLogger.info(s"[MPT.Debug] Dumping debug snapshot for ordinal=$ordStr")

      // 1. Delta entries: hex_key value_hash fieldId
      deltaLines <- deltaEntries.toList.parTraverse {
        case (key, json) =>
          for {
            hex <- GlobalStateKey.toHex[F](key)
            bytes <- JsonSerializer[F].serialize(json)
            hash <- Hasher[F].hashBytes(bytes)
          } yield s"${hex.value} ${hash.value} ${key.fieldId}"
      }
      _ <- writeFile(
        s"$ordStr-delta.txt",
        s"# ordinal=$ordStr upserts=${deltaLines.size}\n" +
          deltaLines.sorted.mkString("\n") + "\n"
      )

      // 2. Removal keys: hex_key fieldId
      removalLines <- removalKeys.toList.parTraverse { key =>
        GlobalStateKey.toHex[F](key).map(hex => s"${hex.value} ${key.fieldId}")
      }
      _ <- writeFile(
        s"$ordStr-removals.txt",
        s"# ordinal=$ordStr removals=${removalLines.size}\n" +
          removalLines.sorted.mkString("\n") + "\n"
      )

      // 3. Full state fingerprint after sync: hex_key value_hash
      allEntries <- store.underlying.entries
      fullStateLines <- allEntries.toList.parTraverse {
        case (hex, bytes) =>
          Hasher[F].hashBytes(bytes).map(hash => s"${hex.value} ${hash.value}")
      }
      rootHash <- store.underlying.getRootHashForOrdinal(snapshotOrdinal)
      _ <- writeFile(
        s"$ordStr-full-state.txt",
        s"# ordinal=$ordStr totalEntries=${allEntries.size} rootHash=${rootHash.map(_.value.show).getOrElse("none")}\n" +
          fullStateLines.sorted.mkString("\n") + "\n"
      )

      _ <- debugLogger.info(
        s"[MPT.Debug] Dumped ordinal=$ordStr: delta=${deltaLines.size} removals=${removalLines.size} fullState=${allEntries.size}"
      )
    } yield ()).handleErrorWith { err =>
      debugLogger.error(err)(s"[MPT.Debug] Failed to dump debug snapshot for ordinal=$ordStr")
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

  object syntax {
    implicit class GlobalSnapshotInfoMptOps(val info: GlobalSnapshotInfo) extends AnyVal {
      def allStateEntries[F[_]: Async: Parallel: Hasher: JsonSerializer](
        implicit stateProofSelector: StateProofSelector
      ): F[Map[GlobalStateKey, Json]] =
        toAllStateKeyValuePairs(info)
    }

    implicit class StateChangesAccumulatorMptOps(val acc: StateChangesAccumulator) extends AnyVal {
      def toStateEntries[F[_]: Async: Parallel: Hasher: JsonSerializer](
        implicit stateProofSelector: StateProofSelector
      ): F[Map[GlobalStateKey, Json]] =
        toStateKeyValuePairsFromAccumulator(acc)
    }

    implicit class MptBuilderOps[F[_]: Parallel: Async: Hasher](kvPairsF: F[Map[GlobalStateKey, Json]]) {

      private val BatchSize = 5000
      private val LogProgressEvery = 50000

      def buildMpt(implicit stateProofSelector: StateProofSelector, j: JsonSerializer[F]): F[MptRoot] = {
        val logger = org.typelevel.log4cats.slf4j.Slf4jLogger.getLoggerFromName[F]("MPT.BuildMpt")

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
      def syncFromGlobalSnapshotInfo(info: GlobalSnapshotInfo, snapshotOrdinal: SnapshotOrdinal)(
        implicit stateProofSelector: StateProofSelector
      ): F[Unit] =
        info.allStateEntries[F].flatMap(store.syncFull[Json](_, snapshotOrdinal))

      def syncFromStateChanges(acc: StateChangesAccumulator, snapshotOrdinal: SnapshotOrdinal)(
        implicit stateProofSelector: StateProofSelector
      ): F[Unit] = {
        val syncLogger = org.typelevel.log4cats.slf4j.Slf4jLogger.getLoggerFromName[F]("MPT.Sync")
        val BatchSize = 5000

        // Convert removal keys from accumulator to GlobalStateKey
        def toRemovalGlobalStateKeys: Set[GlobalStateKey] = {
          val allowSpendKeys = acc.removedAllowSpendKeys.map {
            case (metagraphIdOpt, address) =>
              GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, metagraphIdOpt, address)
          }
          val tokenLockKeys = acc.removedTokenLockKeys.map { address =>
            GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, address)
          }
          val tokenLockBalanceKeys = acc.removedTokenLockBalanceKeys.map { metagraphAddress =>
            GlobalStateKey.hypergraph(GlobalStateFieldId.TokenLockBalances, metagraphAddress)
          }
          val delegatedStakeKeys = acc.removedDelegatedStakeKeys.map { address =>
            GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveDelegatedStakes, address)
          }
          val delegatedStakeWithdrawalKeys = acc.removedDelegatedStakeWithdrawalKeys.map { address =>
            GlobalStateKey.hypergraph(GlobalStateFieldId.DelegatedStakesWithdrawals, address)
          }
          val nodeCollateralKeys = acc.removedNodeCollateralKeys.map { address =>
            GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveNodeCollaterals, address)
          }
          val nodeCollateralWithdrawalKeys = acc.removedNodeCollateralWithdrawalKeys.map { address =>
            GlobalStateKey.hypergraph(GlobalStateFieldId.NodeCollateralWithdrawals, address)
          }
          allowSpendKeys ++ tokenLockKeys ++ tokenLockBalanceKeys ++
            delegatedStakeKeys ++ delegatedStakeWithdrawalKeys ++
            nodeCollateralKeys ++ nodeCollateralWithdrawalKeys
        }

        for {
          entries <- acc.toStateEntries[F]
          keysToRemove = toRemovalGlobalStateKeys

          // Log per-category entry counts for divergence diagnosis
          _ <- syncLogger.info(
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
              s"totalEntries=${entries.size} removals=${keysToRemove.size}"
          )

          // Remove stale keys first (entries that are now empty: AllowSpends, TokenLocks,
          // TokenLockBalances, DelegatedStakes, DelegatedStakeWithdrawals, NodeCollaterals, NodeCollateralWithdrawals)
          _ <- store.remove(keysToRemove.toList).whenA(keysToRemove.nonEmpty)
          // Insert entries in batches, then sync (persist + build) once at the end.
          // Using store.insert for batches avoids firing N background persists that race with each other.
          _ <-
            if (entries.size <= BatchSize) {
              store.sync[Json](entries, snapshotOrdinal)
            } else {
              val batches = entries.toList.grouped(BatchSize).toList
              if (batches.isEmpty) Async[F].unit
              else
                batches.init.traverse_ { batch =>
                  store.insert[Json](batch.toMap) >> Async[F].cede
                } >> store.sync[Json](batches.last.toMap, snapshotOrdinal)
            }

          // Log total MPT entry count after sync for cross-node comparison
          totalMptEntries <- store.underlying.entries.map(_.size)
          rootHash <- store.underlying.getRootHashForOrdinal(snapshotOrdinal)
          _ <- syncLogger.info(
            s"[MPT.Sync] ordinal=$snapshotOrdinal AFTER: totalMptEntries=$totalMptEntries " +
              s"rootHash=${rootHash.map(_.show.take(12)).getOrElse("none")}"
          )

          // Dump debug snapshot to disk when CL_MPT_DEBUG_DUMP=true
          _ <- dumpMptDebugSnapshot(snapshotOrdinal, store, entries, keysToRemove).whenA(MptDebugEnabled)
        } yield ()
      }
    }
  }
}
