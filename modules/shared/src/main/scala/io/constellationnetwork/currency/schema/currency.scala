package io.constellationnetwork.currency.schema

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.dataApplication.FeeTransaction
import io.constellationnetwork.currency.schema.globalSnapshotSync.{GlobalSnapshotSync, GlobalSyncView}
import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SharedArtifact
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageType}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.snapshot._
import io.constellationnetwork.schema.stateproof.StateProofBuilder
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.mpt.producer.StatefulMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.syntax.sortedCollection._

import derevo.cats.{eqv, order, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import derevo.scalacheck.arbitrary
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.cats._
import eu.timepit.refined.scalacheck.all._
import eu.timepit.refined.string.MatchesRegex
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import io.estatico.newtype.macros.newtype

object currency {

  @newtype
  case class TokenSymbol(symbol: String Refined MatchesRegex["[A-Z]+"])

  @derive(encoder, decoder, eqv, show)
  case class CurrencySnapshotStateProof(
    lastTxRefsProof: Hash,
    balancesProof: Hash,
    lastMessagesProof: Option[Hash],
    lastFeeTxRefsProof: Option[Hash],
    lastAllowSpendRefsProof: Option[Hash],
    activeAllowSpends: Option[Hash],
    globalSnapshotSync: Option[Hash],
    lastTokenLockRefsProof: Option[Hash],
    activeTokenLocks: Option[Hash]
  ) extends StateProof

  object CurrencySnapshotStateProof {
    def apply(
      a: (Hash, Hash, Option[Hash], Option[Hash], Option[Hash], Option[Hash], Option[Hash], Option[Hash], Option[Hash])
    ): CurrencySnapshotStateProof =
      CurrencySnapshotStateProof(a._1, a._2, a._3, a._4, a._5, a._6, a._7, a._8, a._9)
  }

  @derive(encoder, decoder, eqv, show)
  case class CurrencySnapshotStateProofV1(
    lastTxRefsProof: Hash,
    balancesProof: Hash
  ) extends StateProof {
    def toCurrencySnapshotStateProof: CurrencySnapshotStateProof =
      CurrencySnapshotStateProof(lastTxRefsProof, balancesProof, None, None, None, None, None, None, None)
  }

  object CurrencySnapshotStateProofV1 {
    def apply(a: (Hash, Hash)): CurrencySnapshotStateProofV1 =
      CurrencySnapshotStateProofV1(a._1, a._2)

    def fromCurrencySnapshotStateProof(proof: CurrencySnapshotStateProof): CurrencySnapshotStateProofV1 =
      CurrencySnapshotStateProofV1(proof.lastTxRefsProof, proof.balancesProof)
  }

  @derive(encoder, decoder, eqv, show)
  case class CurrencySnapshotInfo(
    lastTxRefs: SortedMap[Address, TransactionReference],
    balances: SortedMap[Address, Balance],
    lastMessages: Option[SortedMap[MessageType, Signed[CurrencyMessage]]],
    lastFeeTxRefs: Option[SortedMap[Address, TransactionReference]],
    lastAllowSpendRefs: Option[SortedMap[Address, AllowSpendReference]],
    activeAllowSpends: Option[SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    globalSnapshotSyncView: Option[SortedMap[PeerId, Signed[GlobalSnapshotSync]]],
    lastTokenLockRefs: Option[SortedMap[Address, TokenLockReference]],
    activeTokenLocks: Option[SortedMap[Address, SortedSet[Signed[TokenLock]]]]
  ) extends SnapshotInfo[CurrencySnapshotStateProof] {
    def stateProof[F[_]: Parallel: Async: Hasher: JsonSerializer](ordinal: SnapshotOrdinal)(
      implicit stateProofSelector: StateProofSelector
    ): F[CurrencySnapshotStateProof] =
      (
        lastTxRefs.hash,
        balances.hash,
        lastMessages.traverse(_.hash),
        lastFeeTxRefs.traverse(_.hash),
        lastAllowSpendRefs.traverse(_.hash),
        activeAllowSpends.traverse(_.hash),
        globalSnapshotSyncView.traverse(_.hash),
        lastTokenLockRefs.traverse(_.hash),
        activeTokenLocks.traverse(_.hash)
      ).tupled
        .map(CurrencySnapshotStateProof.apply)

    override def getActiveTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]] = activeTokenLocks.getOrElse(SortedMap.empty)
  }

  object CurrencySnapshotInfo {

    /** Creates a StateProofBuilder for CurrencySnapshotInfo.
      *
      * Currency snapshots always use legacy Merkle tree format.
      */
    def stateProofBuilder[F[_]: Async: Parallel: JsonSerializer](
      implicit selector: CurrencyStateProofSelector
    ): StateProofBuilder[F, CurrencySnapshotInfo, CurrencySnapshotStateProof] =
      StateProofBuilder.instance { (info, _, hasher) =>
        implicit val h: Hasher[F] = hasher
        (
          info.lastTxRefs.hash,
          info.balances.hash,
          info.lastMessages.traverse(_.hash),
          info.lastFeeTxRefs.traverse(_.hash),
          info.lastAllowSpendRefs.traverse(_.hash),
          info.activeAllowSpends.traverse(_.hash),
          info.globalSnapshotSyncView.traverse(_.hash),
          info.lastTokenLockRefs.traverse(_.hash),
          info.activeTokenLocks.traverse(_.hash)
        ).tupled.map(CurrencySnapshotStateProof.apply)
      }
  }

  @derive(encoder, decoder, eqv, show)
  case class CurrencySnapshotInfoV1(
    lastTxRefs: SortedMap[Address, TransactionReference],
    balances: SortedMap[Address, Balance]
  ) extends SnapshotInfo[CurrencySnapshotStateProofV1] {
    def stateProof[F[_]: Parallel: Async: Hasher: JsonSerializer](ordinal: SnapshotOrdinal)(
      implicit stateProofSelector: StateProofSelector
    ): F[CurrencySnapshotStateProofV1] =
      (lastTxRefs.hash, balances.hash).tupled.map(CurrencySnapshotStateProofV1.apply)

    def toCurrencySnapshotInfo: CurrencySnapshotInfo =
      CurrencySnapshotInfo(lastTxRefs, balances, None, None, None, None, None, None, None)
  }

  object CurrencySnapshotInfoV1 {
    def fromCurrencySnapshotInfo(info: CurrencySnapshotInfo): CurrencySnapshotInfoV1 =
      CurrencySnapshotInfoV1(
        info.lastTxRefs,
        info.balances
      )
  }

  @derive(decoder, encoder, order, show, arbitrary)
  @newtype
  case class SnapshotFee(value: NonNegLong)

  object SnapshotFee {
    implicit def toAmount(fee: SnapshotFee): Amount = Amount(fee.value)

    val MinValue: SnapshotFee = SnapshotFee(0L)
  }

  @derive(eqv, show, encoder, decoder)
  case class DataApplicationPartV1(
    onChainState: Array[Byte],
    blocks: List[Array[Byte]],
    calculatedStateProof: Hash
  ) {
    def toDataApplicationPart: DataApplicationPart =
      DataApplicationPart(onChainState, blocks, calculatedStateProof, None)
  }

  object DataApplicationPartV1 {
    def empty: DataApplicationPartV1 = DataApplicationPartV1(Array.empty, List.empty, Hash.empty)
  }

  @derive(eqv, show, encoder, decoder)
  case class DataApplicationPart(
    onChainState: Array[Byte],
    blocks: List[Array[Byte]],
    calculatedStateProof: Hash,
    updateHashes: Option[SortedSet[Hash]] = None
  )

  object DataApplicationPart {
    def empty: DataApplicationPart = DataApplicationPart(Array.empty, List.empty, Hash.empty, None)

    def fromDataApplicationPartV1(part: DataApplicationPartV1): DataApplicationPart =
      DataApplicationPart(part.onChainState, part.blocks, part.calculatedStateProof, None)
  }

  @derive(eqv, show, encoder, decoder)
  case class CurrencySnapshot(
    ordinal: SnapshotOrdinal,
    height: Height,
    subHeight: SubHeight,
    lastSnapshotHash: Hash,
    blocks: SortedSet[BlockAsActiveTip],
    rewards: SortedSet[RewardTransaction],
    tips: SnapshotTips,
    info: CurrencySnapshotInfoV1,
    epochProgress: EpochProgress,
    dataApplication: Option[DataApplicationPartV1] = None,
    globalSyncView: Option[GlobalSyncView] = None,
    version: SnapshotVersion = SnapshotVersion("0.0.1")
  ) extends FullSnapshot[CurrencySnapshotStateProofV1, CurrencySnapshotInfoV1]

  @derive(eqv, show, encoder, decoder)
  case class CurrencyIncrementalSnapshot(
    ordinal: SnapshotOrdinal,
    height: Height,
    subHeight: SubHeight,
    lastSnapshotHash: Hash,
    blocks: SortedSet[BlockAsActiveTip],
    rewards: SortedSet[RewardTransaction],
    tips: SnapshotTips,
    stateProof: CurrencySnapshotStateProof,
    epochProgress: EpochProgress,
    dataApplication: Option[DataApplicationPart],
    messages: Option[SortedSet[Signed[CurrencyMessage]]],
    globalSnapshotSyncs: Option[SortedSet[Signed[GlobalSnapshotSync]]],
    feeTransactions: Option[SortedSet[Signed[FeeTransaction]]],
    artifacts: Option[SortedSet[SharedArtifact]],
    allowSpendBlocks: Option[SortedSet[Signed[AllowSpendBlock]]],
    tokenLockBlocks: Option[SortedSet[Signed[TokenLockBlock]]],
    globalSyncView: Option[GlobalSyncView],
    // The metagraph's OWN authoritative cumulative balance map — the EXACT `CurrencySnapshotInfo.balances` the signed
    // `stateProof.balancesProof` is computed over (so the field and the proof are byte-consistent: hash(authoritativeBalances) ===
    // stateProof.balancesProof is the security anchor). Under roots-only sharding gl0 cannot reproduce this map by re-deriving from its
    // path-dependent base, so ml0 PUSHES it here; the producer puts it in the per-MG checkpoint diff and gl0 ADOPTS it after verifying it
    // against the metagraph-signed `balancesProof` (no re-derive carry-forward for balances in the sharded path). `None` for genesis and
    // pre-this-field snapshots (greenfield: Option, no wire back-compat — all nodes rebuild together).
    authoritativeBalances: Option[SortedMap[Address, Balance]] = None,
    // The metagraph's OWN authoritative active-allow-spend / active-token-lock maps — the EXACT
    // `CurrencySnapshotInfo.activeAllowSpends` / `activeTokenLocks` the signed `stateProof.activeAllowSpends` /
    // `stateProof.activeTokenLocks` (`activeAllowSpends.traverse(_.hash)` / `activeTokenLocks.traverse(_.hash)`) are computed over (so
    // each field and its proof are byte-consistent — the security anchor). These are reduced by cross-shard SPEND transactions whose
    // input is global-snapshot-sourced (empty in gl0's split-safe replay), so gl0's re-derivation RETAINS an allow-spend/token-lock the
    // metagraph already consumed; ml0 PUSHES the authoritative (reduced) sets here, the producer puts them in the per-MG checkpoint diff,
    // and gl0 ADOPTS them after verifying each against the metagraph-signed proof (no re-derive carry-forward for these in the sharded
    // path). `None` for genesis and pre-this-field snapshots (greenfield: Option, no wire back-compat — all nodes rebuild together).
    authoritativeActiveAllowSpends: Option[SortedMap[Address, SortedSet[Signed[AllowSpend]]]] = None,
    authoritativeActiveTokenLocks: Option[SortedMap[Address, SortedSet[Signed[TokenLock]]]] = None,
    // The metagraph's OWN authoritative cumulative last-transaction-reference map — the EXACT `CurrencySnapshotInfo.lastTxRefs` the signed
    // `stateProof.lastTxRefsProof` is computed over (so the field and the proof are byte-consistent — the security anchor). `lastTxRefs` is a
    // CUMULATIVE per-source map carried across the metagraph's whole history; under roots-only sharding gl0 only sees the per-incremental
    // `AdoptFromSignedFields` replay (the blocks in THIS signed incremental), so it cannot reproduce a ref set onto its own path-dependent
    // carry-forward prior — once gl0 carries a stale `lastTxRefs` forward, every later per-incremental derive compounds the divergence and
    // `lastTxRefsProof` never re-converges (the cl1 `/transactions/last-reference` freeze: derivedProof.lastTxRefsProof never equals the
    // committed proof, so the per-field gate carries the prior forever). ml0 PUSHES the authoritative map here, the producer puts it in the
    // per-MG checkpoint diff, and gl0 ADOPTS it after verifying it against the metagraph-signed `lastTxRefsProof` (no re-derive carry-forward
    // for lastTxRefs in the sharded path — symmetric with `authoritativeBalances`). `None` for genesis and pre-this-field snapshots
    // (greenfield: Option, no wire back-compat — all nodes rebuild together).
    authoritativeLastTxRefs: Option[SortedMap[Address, TransactionReference]] = None,
    // The metagraph's OWN authoritative cumulative last-fee-tx-ref / last-allow-spend-ref / last-token-lock-ref / last-message maps —
    // the EXACT `CurrencySnapshotInfo.{lastFeeTxRefs, lastAllowSpendRefs, lastTokenLockRefs, lastMessages}` the signed
    // `stateProof.{lastFeeTxRefsProof, lastAllowSpendRefsProof, lastTokenLockRefsProof, lastMessagesProof}` are hashed over (so each field
    // and its proof are byte-consistent — the security anchor). Like `authoritativeLastTxRefs`, these are CUMULATIVE per-source / per-type
    // maps carried across the metagraph's whole history; gl0's per-incremental `AdoptFromSignedFields` replay rebuilds them onto its OWN
    // path-dependent carry-forward prior, so once a single ordinal's events (fee-txs / allow-spends / token-locks / messages) land outside
    // the incremental gl0 replays, the per-field gate carries a stale prior and the proof never re-converges. ml0 PUSHES the authoritative
    // maps here, the producer puts them in the per-MG checkpoint diff, and gl0 ADOPTS each after verifying it against the metagraph-signed
    // proof (no re-derive carry-forward in the sharded path). Each is Option-shaped exactly like the corresponding `CurrencySnapshotInfo`
    // field (`None` pre-migration / when empty). `None` for genesis and pre-this-field snapshots (greenfield: Option, no wire back-compat).
    authoritativeLastFeeTxRefs: Option[SortedMap[Address, TransactionReference]] = None,
    authoritativeLastAllowSpendRefs: Option[SortedMap[Address, AllowSpendReference]] = None,
    authoritativeLastTokenLockRefs: Option[SortedMap[Address, TokenLockReference]] = None,
    authoritativeLastMessages: Option[SortedMap[MessageType, Signed[CurrencyMessage]]] = None,
    version: SnapshotVersion = SnapshotVersion("0.0.1")
  ) extends IncrementalSnapshot[CurrencySnapshotStateProof]

  object CurrencyIncrementalSnapshot {
    def fromCurrencySnapshot[F[_]: Parallel: Async: Hasher: JsonSerializer](snapshot: CurrencySnapshot)(
      implicit stateProofSelector: StateProofSelector
    ): F[CurrencyIncrementalSnapshot] =
      snapshot.info.stateProof[F](snapshot.ordinal).map { stateProof =>
        CurrencyIncrementalSnapshot(
          snapshot.ordinal,
          snapshot.height,
          snapshot.subHeight,
          snapshot.lastSnapshotHash,
          snapshot.blocks,
          snapshot.rewards,
          snapshot.tips,
          stateProof.toCurrencySnapshotStateProof,
          snapshot.epochProgress,
          snapshot.dataApplication.map(_.toDataApplicationPart),
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None, // authoritativeBalances — None for the genesis→first-incremental transition (populated by the production creator)
          None, // authoritativeActiveAllowSpends — None for the genesis→first-incremental transition
          None, // authoritativeActiveTokenLocks — None for the genesis→first-incremental transition
          None, // authoritativeLastTxRefs — None for the genesis→first-incremental transition
          None, // authoritativeLastFeeTxRefs — None for the genesis→first-incremental transition
          None, // authoritativeLastAllowSpendRefs — None for the genesis→first-incremental transition
          None, // authoritativeLastTokenLockRefs — None for the genesis→first-incremental transition
          None, // authoritativeLastMessages — None for the genesis→first-incremental transition
          snapshot.version
        )
      }
  }

  @derive(eqv, show, encoder, decoder)
  case class CurrencyIncrementalSnapshotV1(
    ordinal: SnapshotOrdinal,
    height: Height,
    subHeight: SubHeight,
    lastSnapshotHash: Hash,
    blocks: SortedSet[BlockAsActiveTip],
    rewards: SortedSet[RewardTransaction],
    tips: SnapshotTips,
    stateProof: CurrencySnapshotStateProofV1,
    epochProgress: EpochProgress,
    dataApplication: Option[DataApplicationPartV1] = None,
    version: SnapshotVersion = SnapshotVersion("0.0.1")
  ) extends IncrementalSnapshot[CurrencySnapshotStateProofV1] {
    def toCurrencyIncrementalSnapshot: CurrencyIncrementalSnapshot =
      CurrencyIncrementalSnapshot(
        ordinal,
        height,
        subHeight,
        lastSnapshotHash,
        blocks,
        rewards,
        tips,
        stateProof.toCurrencySnapshotStateProof,
        epochProgress,
        dataApplication.map(_.toDataApplicationPart),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None, // authoritativeBalances — legacy V1 carries no authoritative balance map
        None, // authoritativeActiveAllowSpends — legacy V1 carries no authoritative active-allow-spend map
        None, // authoritativeActiveTokenLocks — legacy V1 carries no authoritative active-token-lock map
        None, // authoritativeLastTxRefs — legacy V1 carries no authoritative last-tx-ref map
        None, // authoritativeLastFeeTxRefs — legacy V1 carries no authoritative last-fee-tx-ref map
        None, // authoritativeLastAllowSpendRefs — legacy V1 carries no authoritative last-allow-spend-ref map
        None, // authoritativeLastTokenLockRefs — legacy V1 carries no authoritative last-token-lock-ref map
        None, // authoritativeLastMessages — legacy V1 carries no authoritative last-message map
        version
      )
  }

  object CurrencyIncrementalSnapshotV1 {
    def fromCurrencyIncrementalSnapshot(snapshot: CurrencyIncrementalSnapshot): CurrencyIncrementalSnapshotV1 =
      CurrencyIncrementalSnapshotV1(
        snapshot.ordinal,
        snapshot.height,
        snapshot.subHeight,
        snapshot.lastSnapshotHash,
        snapshot.blocks,
        snapshot.rewards,
        snapshot.tips,
        CurrencySnapshotStateProofV1.fromCurrencySnapshotStateProof(snapshot.stateProof),
        snapshot.epochProgress,
        snapshot.dataApplication.flatMap(part => Some(DataApplicationPartV1(part.onChainState, part.blocks, part.calculatedStateProof))),
        snapshot.version
      )
  }

  object CurrencySnapshot {
    def mkGenesis(
      balances: Map[Address, Balance],
      dataApplicationPart: Option[DataApplicationPartV1],
      latestGlobalSnapshot: Option[Hashed[GlobalIncrementalSnapshot]]
    ): CurrencySnapshot =
      CurrencySnapshot(
        SnapshotOrdinal.MinValue,
        Height.MinValue,
        SubHeight.MinValue,
        Hash.empty,
        SortedSet.empty,
        SortedSet.empty,
        SnapshotTips(SortedSet.empty, mkActiveTips(8)),
        CurrencySnapshotInfoV1(SortedMap.empty, SortedMap.from(balances)),
        EpochProgress.MinValue,
        dataApplicationPart,
        latestGlobalSnapshot match {
          case Some(value) => GlobalSyncView(value.ordinal, value.hash, value.epochProgress).some
          case None        => none
        }
      )

    def mkFirstIncrementalSnapshot[F[_]: Parallel: Async: Hasher: JsonSerializer](
      genesis: Hashed[CurrencySnapshot]
    )(implicit stateProofSelector: CurrencyStateProofSelector): F[CurrencyIncrementalSnapshot] =
      genesis.info.stateProof[F](genesis.ordinal).map { stateProof =>
        CurrencyIncrementalSnapshot(
          genesis.ordinal.next,
          genesis.height,
          genesis.subHeight.next,
          genesis.hash,
          SortedSet.empty,
          SortedSet.empty,
          genesis.tips,
          stateProof.toCurrencySnapshotStateProof,
          genesis.epochProgress,
          genesis.dataApplication.map(_.toDataApplicationPart),
          None,
          None,
          None,
          None,
          None,
          None,
          genesis.globalSyncView,
          None, // authoritativeBalances — genesis→first-incremental; the production creator populates it on subsequent incrementals
          None, // authoritativeActiveAllowSpends — genesis→first-incremental; populated on subsequent incrementals
          None, // authoritativeActiveTokenLocks — genesis→first-incremental; populated on subsequent incrementals
          None, // authoritativeLastTxRefs — genesis→first-incremental; populated on subsequent incrementals
          None, // authoritativeLastFeeTxRefs — genesis→first-incremental; populated on subsequent incrementals
          None, // authoritativeLastAllowSpendRefs — genesis→first-incremental; populated on subsequent incrementals
          None, // authoritativeLastTokenLockRefs — genesis→first-incremental; populated on subsequent incrementals
          None, // authoritativeLastMessages — genesis→first-incremental; populated on subsequent incrementals
          genesis.version
        )
      }

    private def mkActiveTips(n: PosInt): SortedSet[ActiveTip] =
      List
        .range(0, n.value)
        .map { i =>
          ActiveTip(BlockReference(Height.MinValue, ProofsHash(s"%064d".format(i))), 0L, SnapshotOrdinal.MinValue)
        }
        .toSortedSet
  }

  @derive(eqv, encoder, decoder)
  case class CurrencySnapshotContext(address: Address, snapshotInfo: CurrencySnapshotInfo)
}
