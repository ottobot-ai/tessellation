package io.constellationnetwork.schema

import cats.effect.{Async, Sync}
import cats.syntax.all._
import cats.{MonadThrow, Parallel}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.json.StreamingJsonCodecs._
import io.constellationnetwork.merkletree.syntax._
import io.constellationnetwork.merkletree.{MerkleRoot, MerkleTree, Proof}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, StakeDistribution}
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
import io.constellationnetwork.schema.priceOracle.{PriceRecord, TokenPair}
import io.constellationnetwork.schema.snapshot.{MetagraphSyncDataInfo, SnapshotInfo, StateProof}
import io.constellationnetwork.schema.stateproof.StateProofBuilder
import io.constellationnetwork.schema.swap.{AllowSpend, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MerklePatriciaTrie
import io.constellationnetwork.security.mpt.producer.StatefulMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import eu.timepit.refined.types.numeric.NonNegInt
import io.circe._
import io.circe.disjunctionCodecs._
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.EncoderOps

@derive(encoder, decoder, eqv, show)
case class GlobalSnapshotInfoV1(
  lastStateChannelSnapshotHashes: SortedMap[Address, Hash],
  lastTxRefs: SortedMap[Address, TransactionReference],
  balances: SortedMap[Address, Balance]
) extends SnapshotInfo[GlobalSnapshotStateProofV1] {
  def toGlobalSnapshotInfo: GlobalSnapshotInfo =
    GlobalSnapshotInfo(
      lastStateChannelSnapshotHashes,
      lastTxRefs,
      balances,
      SortedMap.empty,
      SortedMap.empty,
      Some(SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]),
      Some(SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      SortedMap.empty
    )

  def stateProof[F[_]: Parallel: Async: Hasher: JsonSerializer](ordinal: SnapshotOrdinal)(
    implicit stateProofSelector: StateProofSelector
  ): F[GlobalSnapshotStateProofV1] =
    GlobalSnapshotInfo
      .legacyStateProof[F](toGlobalSnapshotInfo, None)
      .map(GlobalSnapshotStateProofV1.fromGlobalSnapshotStateProof)
}

@derive(encoder, decoder, eqv, show)
case class GlobalSnapshotInfoV2(
  lastStateChannelSnapshotHashes: SortedMap[Address, Hash],
  lastTxRefs: SortedMap[Address, TransactionReference],
  balances: SortedMap[Address, Balance],
  lastCurrencySnapshots: SortedMap[Address, Either[Signed[
    CurrencySnapshot
  ], (Signed[CurrencyIncrementalSnapshotV1], CurrencySnapshotInfoV1)]],
  lastCurrencySnapshotsProofs: SortedMap[Address, Proof]
) extends SnapshotInfo[GlobalSnapshotStateProof] {
  def toGlobalSnapshotInfo: GlobalSnapshotInfo =
    GlobalSnapshotInfo(
      lastStateChannelSnapshotHashes,
      lastTxRefs,
      balances,
      lastCurrencySnapshots.view.mapValues {
        _.map { case (Signed(inc, proofs), info) => (Signed(inc.toCurrencyIncrementalSnapshot, proofs), info.toCurrencySnapshotInfo) }
      }.to(lastCurrencySnapshots.sortedMapFactory),
      lastCurrencySnapshotsProofs,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      SortedMap.empty
    )

  def stateProof[F[_]: Parallel: Async: Hasher: JsonSerializer](ordinal: SnapshotOrdinal)(
    implicit stateProofSelector: StateProofSelector
  ): F[GlobalSnapshotStateProof] =
    lastCurrencySnapshots.merkleTree[F].flatMap(stateProof(_))

  def stateProof[F[_]: Parallel: Async: Hasher](lastCurrencySnapshots: Option[MerkleTree]): F[GlobalSnapshotStateProof] =
    GlobalSnapshotInfo.legacyStateProof[F](toGlobalSnapshotInfo, lastCurrencySnapshots)
}

object GlobalSnapshotInfoV2 {
  def fromGlobalSnapshotInfo(gs: GlobalSnapshotInfo): GlobalSnapshotInfoV2 =
    GlobalSnapshotInfoV2(
      gs.lastStateChannelSnapshotHashes,
      gs.lastTxRefs,
      gs.balances,
      gs.lastCurrencySnapshots.view.mapValues {
        _.map {
          case (Signed(inc, proofs), info) =>
            (
              Signed(CurrencyIncrementalSnapshotV1.fromCurrencyIncrementalSnapshot(inc), proofs),
              CurrencySnapshotInfoV1.fromCurrencySnapshotInfo(info)
            )
        }
      }.to(gs.lastCurrencySnapshots.sortedMapFactory),
      gs.lastCurrencySnapshotsProofs
    )
}

@derive(encoder, decoder, eqv, show)
case class GlobalSnapshotInfo(
  lastStateChannelSnapshotHashes: SortedMap[Address, Hash],
  lastTxRefs: SortedMap[Address, TransactionReference],
  balances: SortedMap[Address, Balance],
  lastCurrencySnapshots: SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
  lastCurrencySnapshotsProofs: SortedMap[Address, Proof],
  activeAllowSpends: Option[SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]],
  activeTokenLocks: Option[SortedMap[Address, SortedSet[Signed[TokenLock]]]],
  tokenLockBalances: Option[SortedMap[Address, SortedMap[Address, Balance]]],
  lastAllowSpendRefs: Option[SortedMap[Address, AllowSpendReference]],
  lastTokenLockRefs: Option[SortedMap[Address, TokenLockReference]],
  updateNodeParameters: Option[SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)]],
  activeDelegatedStakes: Option[SortedMap[Address, SortedSet[DelegatedStakeRecord]]],
  delegatedStakesWithdrawals: Option[SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]]],
  activeNodeCollaterals: Option[SortedMap[Address, SortedSet[NodeCollateralRecord]]],
  nodeCollateralWithdrawals: Option[SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]],
  priceState: Option[SortedMap[TokenPair, PriceRecord]],
  metagraphSyncData: Option[SortedMap[Address, MetagraphSyncDataInfo]],
  // §3 NIPoPoW S0: stake distribution snapshotted at each eta-period boundary, indexed by the
  // closing period. Used by `StakeRegistry.relativeStakeAt(_, N-2)` for Cardano-style mark/set/go
  // slot-leader eligibility. Updated by GSAM.accept() at boundary ordinals (`ord % R == R - 1`);
  // retention pruning keeps the last 4 periods (algorithm needs N-2; extra grace for reorgs).
  // Empty on V1/V2 upgrade and on genesis until the loader seeds it; eligibility reads fall through
  // to the warmup branch (current GSI) in that case.
  historicalStakeSnapshots: SortedMap[EtaPeriod, StakeDistribution]
) extends SnapshotInfo[GlobalSnapshotStateProof] {

  def toGlobalSnapshotInfo: GlobalSnapshotInfo =
    GlobalSnapshotInfo(
      lastStateChannelSnapshotHashes,
      lastTxRefs,
      balances,
      lastCurrencySnapshots,
      lastCurrencySnapshotsProofs,
      activeAllowSpends,
      activeTokenLocks,
      tokenLockBalances,
      lastAllowSpendRefs,
      lastTokenLockRefs,
      updateNodeParameters,
      activeDelegatedStakes,
      delegatedStakesWithdrawals,
      activeNodeCollaterals,
      nodeCollateralWithdrawals,
      priceState,
      metagraphSyncData,
      historicalStakeSnapshots
    )

  def stateProof[F[_]: Parallel: Async: Hasher: JsonSerializer](ordinal: SnapshotOrdinal)(
    implicit stateProofSelector: StateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): F[GlobalSnapshotStateProof] =
    stateProofSelector.select(ordinal) match {
      case LegacyFormat         => lastCurrencySnapshots.merkleTree[F].flatMap(stateProof(_))
      case MerklePatriciaFormat => GlobalSnapshotInfo.mptStateProof[F](this)
    }

  def stateProof[F[_]: Parallel: Async: Hasher](lastCurrencySnapshots: Option[MerkleTree]): F[GlobalSnapshotStateProof] =
    GlobalSnapshotInfo.legacyStateProof[F](toGlobalSnapshotInfo, lastCurrencySnapshots)

  override def getActiveTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]] = activeTokenLocks.getOrElse(SortedMap.empty)

  override def getActiveDelegatedStakes: SortedMap[Address, SortedSet[DelegatedStakeRecord]] =
    activeDelegatedStakes.getOrElse(SortedMap.empty)
}

object GlobalSnapshotInfo {

  /** Creates a StateProofBuilder for GlobalSnapshotInfo (no producer).
    *
    * Uses legacy Merkle trees for pre-MPT ordinals, rebuilds MPT from state for post-migration ordinals. For efficient proof building with
    * a pre-built trie, use `stateProofBuilder(Some(producer))`.
    */
  def stateProofBuilder[F[_]: Async: Parallel: JsonSerializer](
    implicit selector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): StateProofBuilder[F, GlobalSnapshotInfo, GlobalSnapshotStateProof] =
    stateProofBuilder(None)

  /** Creates a StateProofBuilder with optional MPT producer.
    *
    * @param producer
    *   Optional MPT producer for efficient proof building from pre-built trie. When None, MPT proofs are rebuilt from snapshot state.
    */
  def stateProofBuilder[F[_]: Async: Parallel: JsonSerializer](
    producer: Option[StatefulMerklePatriciaProducer[F]]
  )(
    implicit selector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): StateProofBuilder[F, GlobalSnapshotInfo, GlobalSnapshotStateProof] =
    StateProofBuilder.instance { (info, ordinal, hasher) =>
      implicit val h: Hasher[F] = hasher
      implicit val s: StateProofSelector = selector
      selector.select(ordinal) match {
        case LegacyFormat =>
          info.lastCurrencySnapshots.merkleTree[F].flatMap { lastCurrencySnapshotsMerkle =>
            legacyStateProof[F](info, lastCurrencySnapshotsMerkle)
          }
        case MerklePatriciaFormat =>
          producer match {
            case Some(p) =>
              p.buildForOrdinal(ordinal).flatMap {
                case Left(err) => err.raiseError[F, GlobalSnapshotStateProof]
                case Right(_) =>
                  p.getRootHashForOrdinal(ordinal).flatMap {
                    case Some(value) =>
                      // The producer's `value` is the canonical global mptRoot (incrementally maintained).
                      // Per-field subtree roots are computed from the same byte map the producer was fed —
                      // not from a fresh global rebuild — and slot into the case class's per-field proof slots
                      // so the state proof carries verifiable per-fieldId roots, not Hash.empty placeholders.
                      // Keep in lockstep with `mptStateProof[F]` below: same fieldRoot mapping, same Option
                      // lifting from the GSI's per-field presence.
                      info.allStateEntriesAsBytes.buildPerFieldMptRoots.map { perField =>
                        val FId = io.constellationnetwork.schema.mpt.GlobalStateFieldId

                        def fieldRoot(id: io.constellationnetwork.schema.mpt.GlobalStateFieldId): Hash =
                          perField.getOrElse(id, Hash.empty)

                        GlobalSnapshotStateProof(
                          lastStateChannelSnapshotHashesProof = fieldRoot(FId.LastStateChannelSnapshotHashes),
                          lastTxRefsProof = fieldRoot(FId.LastTxRefs),
                          balancesProof = fieldRoot(FId.Balances),
                          lastCurrencySnapshotsProof = None,
                          activeAllowSpends = info.activeAllowSpends.map(_ => fieldRoot(FId.ActiveAllowSpends)),
                          activeTokenLocks = info.activeTokenLocks.map(_ => fieldRoot(FId.ActiveTokenLocks)),
                          tokenLockBalances = info.tokenLockBalances.map(_ => fieldRoot(FId.TokenLockBalances)),
                          lastAllowSpendRefs = info.lastAllowSpendRefs.map(_ => fieldRoot(FId.LastAllowSpendRefs)),
                          lastTokenLockRefs = info.lastTokenLockRefs.map(_ => fieldRoot(FId.LastTokenLockRefs)),
                          updateNodeParameters = info.updateNodeParameters.map(_ => fieldRoot(FId.UpdateNodeParameters)),
                          activeDelegatedStakes = info.activeDelegatedStakes.map(_ => fieldRoot(FId.ActiveDelegatedStakes)),
                          delegatedStakesWithdrawals = info.delegatedStakesWithdrawals.map(_ => fieldRoot(FId.DelegatedStakesWithdrawals)),
                          activeNodeCollaterals = info.activeNodeCollaterals.map(_ => fieldRoot(FId.ActiveNodeCollaterals)),
                          nodeCollateralWithdrawals = info.nodeCollateralWithdrawals.map(_ => fieldRoot(FId.NodeCollateralWithdrawals)),
                          priceState = info.priceState.map(_ => fieldRoot(FId.PriceState)),
                          lastGlobalSnapshotsWithCurrency = None,
                          mptRoot = Some(value.value),
                          historicalStakeSnapshots =
                            if (info.historicalStakeSnapshots.isEmpty) None
                            else Some(fieldRoot(FId.HistoricalStakeSnapshots))
                        )
                      }
                    case None => MonadThrow[F].raiseError(new RuntimeException(s"Could not get mptRootHash for ordinal $ordinal"))
                  }
              }
            case None =>
              mptStateProof[F](info)
          }
      }
    }

  def mptStateProof[F[_]: Parallel: Async: Hasher: JsonSerializer](info: GlobalSnapshotInfo)(
    implicit stateProofSelector: StateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): F[GlobalSnapshotStateProof] =
    info.allStateEntriesAsBytes.flatMap { entries =>
      // Re-hex the keys to feed `mptStateProofFromBytes`, which is the canonical helper now.
      // Hex-keyed bytes is what the overlay produces, so making that the helper's input shape lets
      // GSAM's Phase J path and this rebuild path share one implementation.
      entries.toList.parTraverse {
        case (k, v) => io.constellationnetwork.schema.mpt.GlobalStateKey.toHex[F](k).map(_ -> v)
      }
        .flatMap(pairs => mptStateProofFromBytes[F](info, pairs.toMap))
    }

  /** Build an MPT state proof from a pre-computed byte map, sharing the per-field-root + GlobalSnapshotStateProof shape with
    * `mptStateProof[F](info)`. Used when the byte source is NOT `info.allStateEntriesAsBytes` — specifically GSAM's accept() pipeline under
    * `OverlayMode.MultiBranch`, where the proof's `mptRoot` must reflect the post-write overlay+handle view (base + parent chain +
    * uncommitted handle accumulator), not the producer's stale per-ordinal root nor a fresh GSI re-encode that may diverge from the
    * actually-committed bytes under sidecar/expiry-index drift.
    *
    * Caller is responsible for ensuring `entries` matches the consensus-relevant post-acceptance state. The parity gate (#107) is the
    * byte-equivalence contract between `info.allStateEntriesAsBytes` and overlay-derived bytes.
    */
  def mptStateProofFromBytes[F[_]: Parallel: Async: Hasher: JsonSerializer](
    info: GlobalSnapshotInfo,
    entries: Map[io.constellationnetwork.security.hex.Hex, Array[Byte]]
  )(
    implicit stateProofSelector: StateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): F[GlobalSnapshotStateProof] = {
    val FId = io.constellationnetwork.schema.mpt.GlobalStateFieldId

    // Group hex-keyed entries by `fieldId` parsed from the network-namespace prefix
    // (`GlobalStateKey.fieldIdFromHex`). Entries with malformed prefixes are dropped — they
    // wouldn't survive the producer's typed encoding either, so they can't influence consensus.
    val perFieldGrouping
      : Map[io.constellationnetwork.schema.mpt.GlobalStateFieldId, Map[io.constellationnetwork.security.hex.Hex, Array[Byte]]] =
      entries.toList.flatMap {
        case (hex, bytes) =>
          io.constellationnetwork.schema.mpt.GlobalStateKey.fieldIdFromHex(hex).map(fid => fid -> (hex -> bytes))
      }.groupMap(_._1)(_._2).view.mapValues(_.toMap).toMap

    for {
      // Compute the global mptRoot and per-fieldId subtree roots in parallel from the same byte map.
      // Per-field roots are deterministic across nodes because fieldId is a structural prefix of the
      // encoded GlobalStateKey and value bytes use the same canonical codecs as the global root.
      // Each subtree root is the rootHash of an MPT built from only that fieldId's entries — gives
      // the case class's per-field Option[Hash] slots a well-defined, verifiable value (instead of None).
      results <- (
        io.constellationnetwork.security.mpt.MerklePatriciaTrie.makeParallelFromBytes[F](entries).map(_.rootHash),
        perFieldGrouping.toList.parTraverse {
          case (fieldId, fieldEntries) =>
            io.constellationnetwork.security.mpt.MerklePatriciaTrie
              .makeParallelFromBytes[F](fieldEntries)
              .map(t => fieldId -> t.rootHash.value)
        }
      ).parTupled
      (mptRoot, perFieldList) = results
      perField = perFieldList.toMap
    } yield {
      def fieldRoot(id: io.constellationnetwork.schema.mpt.GlobalStateFieldId): Hash =
        perField.getOrElse(id, Hash.empty)

      GlobalSnapshotStateProof(
        lastStateChannelSnapshotHashesProof = fieldRoot(FId.LastStateChannelSnapshotHashes),
        lastTxRefsProof = fieldRoot(FId.LastTxRefs),
        balancesProof = fieldRoot(FId.Balances),
        // Currency-snapshots slot is a Merkle tree (not MPT); the MPT format tracks
        // LastIncrementalCurrencySnapshots/LastCurrencySnapshotInfo via the global mptRoot.
        lastCurrencySnapshotsProof = None,
        activeAllowSpends = info.activeAllowSpends.map(_ => fieldRoot(FId.ActiveAllowSpends)),
        activeTokenLocks = info.activeTokenLocks.map(_ => fieldRoot(FId.ActiveTokenLocks)),
        tokenLockBalances = info.tokenLockBalances.map(_ => fieldRoot(FId.TokenLockBalances)),
        lastAllowSpendRefs = info.lastAllowSpendRefs.map(_ => fieldRoot(FId.LastAllowSpendRefs)),
        lastTokenLockRefs = info.lastTokenLockRefs.map(_ => fieldRoot(FId.LastTokenLockRefs)),
        updateNodeParameters = info.updateNodeParameters.map(_ => fieldRoot(FId.UpdateNodeParameters)),
        activeDelegatedStakes = info.activeDelegatedStakes.map(_ => fieldRoot(FId.ActiveDelegatedStakes)),
        delegatedStakesWithdrawals = info.delegatedStakesWithdrawals.map(_ => fieldRoot(FId.DelegatedStakesWithdrawals)),
        activeNodeCollaterals = info.activeNodeCollaterals.map(_ => fieldRoot(FId.ActiveNodeCollaterals)),
        nodeCollateralWithdrawals = info.nodeCollateralWithdrawals.map(_ => fieldRoot(FId.NodeCollateralWithdrawals)),
        priceState = info.priceState.map(_ => fieldRoot(FId.PriceState)),
        lastGlobalSnapshotsWithCurrency = None,
        mptRoot = Some(mptRoot.value),
        historicalStakeSnapshots =
          if (info.historicalStakeSnapshots.isEmpty) None
          else Some(fieldRoot(FId.HistoricalStakeSnapshots))
      )
    }
  }

  def legacyStateProof[F[_]: Parallel: Sync: Hasher](
    info: GlobalSnapshotInfo,
    lastCurrencySnapshots: Option[MerkleTree]
  ): F[GlobalSnapshotStateProof] =
    (
      info.lastStateChannelSnapshotHashes.hash,
      info.lastTxRefs.hash,
      info.balances.hash,
      info.activeAllowSpends.traverse(_.hash),
      info.activeTokenLocks.traverse(_.hash),
      info.tokenLockBalances.traverse(_.hash),
      info.lastAllowSpendRefs.traverse(_.hash),
      info.lastTokenLockRefs.traverse(_.hash),
      info.updateNodeParameters.traverse(_.hash),
      info.activeDelegatedStakes.traverse(_.hash),
      info.delegatedStakesWithdrawals.traverse(_.hash),
      info.activeNodeCollaterals.traverse(_.hash),
      info.nodeCollateralWithdrawals.traverse(_.hash),
      info.priceState.traverse(_.hash),
      info.metagraphSyncData.traverse(_.hash)
    ).mapN(GlobalSnapshotStateProof.apply(_, _, _, lastCurrencySnapshots.map(_.getRoot), _, _, _, _, _, _, _, _, _, _, _, _, None, None))

  def empty: GlobalSnapshotInfo = GlobalSnapshotInfo(
    SortedMap.empty,
    SortedMap.empty,
    SortedMap.empty,
    SortedMap.empty,
    SortedMap.empty,
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    SortedMap.empty
  )

  implicit val optionAddressKeyEncoder: KeyEncoder[Option[Address]] = new KeyEncoder[Option[Address]] {
    def apply(key: Option[Address]): String = key match {
      case None          => KeyEncoder[String].apply("")
      case Some(address) => KeyEncoder[Address].apply(address)
    }
  }

  implicit val optionAddressKeyDecoder: KeyDecoder[Option[Address]] = new KeyDecoder[Option[Address]] {
    def apply(key: String): Option[Option[Address]] =
      if (key === "") Some(None) else KeyDecoder[Address].apply(key).map(_.some)
  }
}
