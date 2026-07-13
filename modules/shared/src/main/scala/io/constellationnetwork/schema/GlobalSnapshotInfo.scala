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
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.nakamoto._
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
import io.constellationnetwork.schema.peer.PeerId
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
  // §3 NIPoPoW S0: per-period boundary record (stake distribution + eta randomness), indexed by the
  // closing period. Stake half: used by `StakeRegistry.relativeStakeAt(_, N-2)` for Cardano-style
  // mark/set/go slot-leader eligibility. Eta half: deterministic snapshot of the randomness used by
  // period N's slot leaders — read by `EtaStateManager.getEta(period)` as the disk-immune cache
  // (Path 1 of the heap-leak workstream; chainStore eviction can truncate the VRF chain walk that
  // would otherwise recompute eta). Updated by GSAM.accept() at boundary ordinals (`ord % R == R - 1`);
  // retention pruning keeps the last 4 periods (algorithm needs N-2; extra grace for reorgs).
  // Empty on V1/V2 upgrade and on genesis until the loader seeds it; eligibility / eta reads fall
  // through to the warmup branch (current GSI / genesis eta) in that case.
  historicalStakeSnapshots: SortedMap[EtaPeriod, HistoricalStakeSnapshot],
  // Canonical unified operator-key registry. Per-operator histories and exact latest pointers are rooted in MPT fields 22/23.
  // Carrying both in the current context makes full-state rebuild/bootstrap byte-equivalent to incremental acceptance.
  kesRegistrationCerts: SortedMap[PeerId, SortedSet[KesRegistrationRecord]] = SortedMap.empty,
  lastKesRegistrationRefs: SortedMap[PeerId, KesRegistrationReference] = SortedMap.empty,
  // Immutable period-zero identity anchor. These signed atomic KES+VRF records live in rooted MPT field 24 and are never runtime
  // registration history or an eligibility roster.
  genesisOperatorKeys: SortedMap[PeerId, GenesisOperatorConsensusKey] = SortedMap.empty
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
      historicalStakeSnapshots,
      kesRegistrationCerts,
      lastKesRegistrationRefs,
      genesisOperatorKeys
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
              // Keep the producer's per-ordinal trie warm (its root-hash cache feeds downstream reads), but DERIVE
              // the consensus proof from the producer's OWN byte map (`p.entries`, straight from the MPT store) —
              // NOT from `info.allStateEntriesAsBytes`. `mptStateProofFromBytes` excludes the path-dependent
              // SystemNamespace sidecars (`GlobalStateKey.isSystemNamespaceHex`), so download/traverse/sync compute a
              // byte-identical, deterministic global root to the one the accept path bakes into the signed artifact.
              // `info` is used ONLY for the present-only per-field Option lifting (exactly as the accept path passes
              // `gsi` to the same helper) — never as a byte source. `p.entries` is already `Hex`-keyed, so no toHex.
              p.buildForOrdinal(ordinal).flatMap {
                case Left(err) => err.raiseError[F, GlobalSnapshotStateProof]
                case Right(_)  => p.entries.flatMap(bytes => mptStateProofFromBytes[F](info, bytes))
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

    // Consensus determinism (fork-recovery mptRoot storm fix): the global root must be a pure function of the
    // user-field KV state. SystemNamespace sidecar entries (ActiveAddressIndex, expiry buckets) are local
    // read-acceleration indices with no per-field proof slot; the ActiveAddressIndex partition is maintained
    // append-only on the accept path, so its contents are path-dependent (a function of the delta history, not
    // the current state) and differ across nodes that processed different-but-equivalent ordinal streams or that
    // rebuilt from current keysets. Folding them into the global root produced `stateProof[mptRoot]`-only
    // divergence. Excluding every `03…` entry makes the root deterministic. See `GlobalStateKey.isSystemNamespaceHex`.
    val userEntries = io.constellationnetwork.schema.mpt.GlobalStateKey.consensusRootEntries(entries)

    for {
      // Compute the global mptRoot and per-fieldId subtree roots in parallel from the same byte map.
      // Per-field roots are deterministic across nodes because fieldId is a structural prefix of the
      // encoded GlobalStateKey and value bytes use the same canonical codecs as the global root.
      // Each subtree root is the rootHash of an MPT built from only that fieldId's entries — gives
      // the case class's per-field Option[Hash] slots a well-defined, verifiable value (instead of None).
      results <- (
        io.constellationnetwork.security.mpt.MerklePatriciaTrie.makeParallelFromBytes[F](userEntries).map(_.rootHash),
        perFieldGrouping.toList.parTraverse {
          case (fieldId, fieldEntries) =>
            // Shared per-field root computation — keep in lockstep with the producer path
            // (`stateProofBuilder` → `buildPerFieldMptRoots`) and the gl1 follow verifier; all three
            // route through `GlobalStateConverter.fieldRootFromBytes` so a field's signed root can never
            // depend on which path produced it.
            io.constellationnetwork.schema.mpt.GlobalStateConverter.fieldRootFromBytes[F](fieldEntries).tupleLeft(fieldId)
        },
        // infoRoot = UNION over the 8 unrolled `Mg*` infoSubFields (replaces the old single fieldId-6 lookup). Computed from the SAME
        // `entries` (via `perFieldGrouping`) as the global mptRoot, so it stays consistent with mptRoot; byte-identical to the follower's
        // `currencySnapshotFieldRoots` recompute by the round-trip + parity contracts (docs/nakamoto/UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md).
        io.constellationnetwork.schema.mpt.GlobalStateConverter.fieldRootFromBytes[F](
          FId.infoSubFields.toList.flatMap(perFieldGrouping.getOrElse(_, Map.empty)).toMap
        )
      ).parTupled
      (mptRoot, perFieldList, currencyInfoRoot) = results
      perField = perFieldList.toMap
    } yield {
      def fieldRoot(id: io.constellationnetwork.schema.mpt.GlobalStateFieldId): Hash =
        perField.getOrElse(id, Hash.empty)

      GlobalSnapshotStateProof(
        lastStateChannelSnapshotHashesProof = fieldRoot(FId.LastStateChannelSnapshotHashes),
        lastTxRefsProof = fieldRoot(FId.LastTxRefs),
        balancesProof = fieldRoot(FId.Balances),
        // SIGNED currency-snapshots per-field roots: `incrementalRoot` from the `perField` map (fieldId 5, still a single partition) +
        // `currencyInfoRoot` = the UNION over the 8 unrolled `Mg*` infoSubFields computed above (the fieldId-6 blob is gone). `Some` iff the
        // currency map is non-empty (matches the `info.<field>.map(_ => …)` present-only convention). Byte-identical to the follower's
        // `GlobalStateConverter.currencySnapshotFieldRoots` recompute (same `fieldRootFromBytes` path, same union). This is field 4.
        lastCurrencySnapshotsProof =
          if (info.lastCurrencySnapshots.isEmpty) None
          else Some(CurrencySnapshotMptRoots(fieldRoot(FId.LastIncrementalCurrencySnapshots), currencyInfoRoot)),
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
          else Some(fieldRoot(FId.HistoricalStakeSnapshots)),
        // smtRoot intentionally None on this byte-rebuild path — see the producer-path note above. GSAM's accept() overrides it with the
        // maintained store's cutoff-root after calling this helper (smtRoot is set on the returned proof, not derived from `entries`).
        smtRoot = None
      )
    }
  }

  /** Recompute the consensus global `mptRoot` from a hex-keyed byte map, EXCLUDING the path-dependent SystemNamespace sidecars AND the
    * observation-dependent `MgGlobalSnapshotSyncView` (`GlobalStateKey.consensusRootEntries`) — the EXACT global-root computation
    * `mptStateProofFromBytes` performs (`makeParallelFromBytes(consensusRootEntries(...))`). Use at every adopt/verify site that compares a
    * locally-rebuilt store root against a signed `stateProof.mptRoot`, so the comparison stays apples-to-apples. Keep in lockstep with the
    * `userEntries` global-root line in `mptStateProofFromBytes`.
    */
  def sidecarFreeMptRoot[F[_]: Async: Parallel: Hasher: JsonSerializer](
    entries: Map[io.constellationnetwork.security.hex.Hex, Array[Byte]]
  ): F[Hash] =
    io.constellationnetwork.security.mpt.MerklePatriciaTrie
      .makeParallelFromBytes[F](io.constellationnetwork.schema.mpt.GlobalStateKey.consensusRootEntries(entries))
      .map(_.rootHash.value)

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
    ).mapN(
      // Field 4 on V2 is now the SIGNED currency MPT partition roots; the legacy separate-Merkle-tree currency root
      // (`lastCurrencySnapshots.map(_.getRoot)`) has no MPT-partition representation, so this legacy-format path drops it to `None`
      // (consistent with `GlobalSnapshotStateProofV1.toGlobalSnapshotStateProof`). The MerkleTree arg is now unused here.
      GlobalSnapshotStateProof
        .apply(_, _, _, None, _, _, _, _, _, _, _, _, _, _, _, _, None, None, None)
    )

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
