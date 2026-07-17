package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.economics.StakeBackingValidator
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{ActiveTokenLockMptReader, GlobalStateReader}
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.TokenUnlock
import io.constellationnetwork.schema.balance._
import io.constellationnetwork.schema.delegatedStake.PendingDelegatedStakeWithdrawal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nodeCollateral.PendingNodeCollateralWithdrawal
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockReferenceImmutableCodec}

import eu.timepit.refined.types.numeric.NonNegLong

/** Result of token lock acceptance containing full state, deltas, and removed keys */
case class TokenLockAcceptanceResult(
  fullState: SortedMap[Address, SortedSet[Signed[TokenLock]]],
  deltas: SortedMap[Address, SortedSet[Signed[TokenLock]]],
  removedKeys: Set[Address] = Set.empty,
  // Expiry-index delta: adds for records newly entering active (with `unlockEpoch.isDefined`), removes for records
  // leaving active. Records with `unlockEpoch = None` never expire and aren't indexed.
  expiryIndexDelta: SystemIndexDelta[TokenLockExpiryKey] = SystemIndexDelta.empty[TokenLockExpiryKey]
)

/** Deltas-only result of the MPT-backed accept path (#85).
  *
  * Missing on purpose: `fullState`. Caller must reconstruct the post-ordinal full map from `(lastFromSnapshotContext -- removedKeys) ++
  * deltas` if needed. This is what drops the manager's dependency on a pre-materialized full map.
  */
case class TokenLockAcceptanceDeltas(
  deltas: SortedMap[Address, SortedSet[Signed[TokenLock]]],
  removedKeys: Set[Address] = Set.empty,
  expiryIndexDelta: SystemIndexDelta[TokenLockExpiryKey] = SystemIndexDelta.empty[TokenLockExpiryKey]
)

/** Result of token lock balance update containing full state, deltas, and removed keys.
  *
  * `removedKeys` is a `Set[(metagraphAddr, holderAddr)]` — pairs whose balance entry was present in the prior MPT state but is absent from
  * the post-accept full state. Pair shape mirrors the actual MPT key (`GlobalStateKey.hypergraph(TokenLockBalances, mid, holder)`), so the
  * syncFromStateChanges removal targets a real key and the rooted address-pair index can prune the same pair. Using a mid-only
  * `Set[Address]` would target nothing and the entry would persist forever, surfacing as gl1 verify-replay mptRoot drift on every sc-event
  * ordinal.
  */
case class TokenLockBalanceResult(
  fullState: SortedMap[Address, SortedMap[Address, Balance]],
  deltas: SortedMap[Address, SortedMap[Address, Balance]],
  removedKeys: Set[(Address, Address)] = Set.empty
)

/** Deltas-only result for the MPT-backed token-lock-balances path (#85). */
case class TokenLockBalanceDeltas(
  deltas: SortedMap[Address, SortedMap[Address, Balance]],
  removedKeys: Set[(Address, Address)] = Set.empty
)

trait TokenLockStateManager[F[_]] {
  def acceptTokenLocks(
    epochProgress: EpochProgress,
    previousEpochProgress: EpochProgress,
    acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    lastActiveGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]]
  )(implicit hasher: Hasher[F]): F[TokenLockAcceptanceResult]

  /** Hoisted variant of `acceptTokenLocks` that takes a pre-computed `expiredGlobalTokenLocks` set, avoiding the redundant
    * `findExpiredGlobalTokenLocksViaIndexFromMpt` call. Used by GSAM's local-events hoist (Q2 user override): the expired set is computed
    * once per accept() and threaded into both this and `updateGlobalBalancesByTokenLocksWithExpired`, then used as the source of
    * EXPIRED-transition events.
    */
  def acceptTokenLocksWithExpired(
    epochProgress: EpochProgress,
    acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    lastActiveGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]],
    expiredGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]]
  )(implicit hasher: Hasher[F]): F[TokenLockAcceptanceResult]

  def acceptReplacementTokenLocks(
    acceptedTokenLocks: List[Signed[TokenLock]],
    lastSnapshotContext: GlobalSnapshotInfo
  )(implicit hasher: Hasher[F]): F[List[Signed[TokenLock]]]

  def acceptReplacementTokenLocks(
    acceptedTokenLocks: List[Signed[TokenLock]],
    lastSnapshotContext: GlobalSnapshotInfo,
    parentBackingState: Option[StakeBackingValidator.ValidatedBackingState]
  )(implicit hasher: Hasher[F]): F[List[Signed[TokenLock]]]

  def acceptTokenLockRefs(
    lastTokenLockRefs: SortedMap[Address, TokenLockReference],
    lastTokenLockContextUpdate: Map[Address, TokenLockReference]
  ): SortedMap[Address, TokenLockReference]

  def filterExpiredTokenLocks(
    tokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    epochProgress: EpochProgress
  ): SortedMap[Address, SortedSet[Signed[TokenLock]]]

  def updateTokenLockBalances(
    currencySnapshots: SortedMap[Address, CurrencySnapshotWithState],
    maybeLastTokenLockBalances: Option[SortedMap[Address, SortedMap[Address, Balance]]]
  ): TokenLockBalanceResult

  def updateGlobalBalancesByTokenLocks(
    epochProgress: EpochProgress,
    previousEpochProgress: EpochProgress,
    currentBalances: SortedMap[Address, Balance],
    acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]]
  )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]]

  /** Hoisted variant of `updateGlobalBalancesByTokenLocks` that takes a pre-computed expired set. See `acceptTokenLocksWithExpired`. */
  def updateGlobalBalancesByTokenLocksWithExpired(
    epochProgress: EpochProgress,
    currentBalances: SortedMap[Address, Balance],
    acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]],
    expiredGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]]
  )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]]

  /** MPT-backed `acceptTokenLocks`. Reads current active sets per-address through the strict native field-8 reader instead of iterating an
    * in-memory map. Returns deltas + removedKeys + expiry-index delta; caller reconstructs full state if needed.
    */
  def acceptTokenLocksFromMpt(
    epochProgress: EpochProgress,
    previousEpochProgress: EpochProgress,
    acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]]
  )(implicit hasher: Hasher[F]): F[TokenLockAcceptanceDeltas]

  /** Hoisted variant of `acceptTokenLocksFromMpt` that takes a pre-computed `expiredGlobalTokenLocks`. */
  def acceptTokenLocksFromMptWithExpired(
    epochProgress: EpochProgress,
    acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]],
    expiredGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]]
  )(implicit hasher: Hasher[F]): F[TokenLockAcceptanceDeltas]

  /** Hoisted variant of `updateGlobalBalancesByTokenLocksFromMpt`. */
  def updateGlobalBalancesByTokenLocksFromMptWithExpired(
    epochProgress: EpochProgress,
    currentBalances: SortedMap[Address, Balance],
    acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]],
    expiredGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]]
  )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]]

  /** MPT-backed `findExpiredGlobalTokenLocksViaIndex`. Resolves each expiring hash through the strict native field-8 reader instead of an
    * in-memory map lookup.
    */
  def findExpiredGlobalTokenLocksViaIndexFromMpt(
    previousEpochProgress: EpochProgress,
    epochProgress: EpochProgress
  )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[Signed[TokenLock]]]]

  /** MPT-backed `updateGlobalBalancesByTokenLocks`. */
  def updateGlobalBalancesByTokenLocksFromMpt(
    epochProgress: EpochProgress,
    previousEpochProgress: EpochProgress,
    currentBalances: SortedMap[Address, Balance],
    acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]]
  )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]]

  /** MPT-backed `updateTokenLockBalances`. F-returning because removal detection requires per-metagraph point reads. Emits deltas +
    * removedKeys only.
    */
  def updateTokenLockBalancesFromMpt(
    currencySnapshots: SortedMap[Address, CurrencySnapshotWithState]
  )(implicit hasher: Hasher[F]): F[TokenLockBalanceDeltas]

  def generateTokenUnlocks(
    expiredWithdrawals: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
    acceptedTokenLocks: List[Signed[TokenLock]],
    globalActiveTokenLocksByRef: Map[Hash, Signed[TokenLock]]
  ): Either[String, Map[Address, List[TokenUnlock]]] =
    generateTokenUnlocks(
      expiredWithdrawals,
      SortedMap.empty,
      acceptedTokenLocks,
      globalActiveTokenLocksByRef
    )

  def generateTokenUnlocks(
    expiredDelegatedWithdrawals: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
    expiredCollateralWithdrawals: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]],
    acceptedTokenLocks: List[Signed[TokenLock]],
    globalActiveTokenLocksByRef: Map[Hash, Signed[TokenLock]]
  ): Either[String, Map[Address, List[TokenUnlock]]]

  /** Build a hash-keyed lookup of active token locks for a given set of addresses, sourced from the MPT. Used by the acceptance pipeline so
    * `generateTokenUnlocks` reads from the same store as `acceptReplacementTokenLocks`. Reading the same source eliminates the divergence
    * that previously stalled chain-sync replay when the GSI's `activeTokenLocks` view disagreed with the MPT view.
    */
  def buildActiveTokenLocksByRefFromMpt(
    addresses: Set[Address]
  )(implicit hasher: Hasher[F]): F[Map[Hash, Signed[TokenLock]]]

  /** Materialize the full `address → active-token-lock-set` view by prefix-scanning the MPT under `(HypergraphNamespace,
    * fieldId=ActiveTokenLocks)`. Replaces `lastSnapshotContext.activeTokenLocks` reads in the GSAM hot path so the accept pipeline no
    * longer depends on the inbound GSI carrying that field.
    *
    * The MPT hex key hashes the address one-way, so addresses are recovered from each value's `Signed[TokenLock].value.source` (every
    * member of a per-address set shares one source by construction). Empty sets are filtered out — they shouldn't be persisted but the
    * filter is a cheap safety net for partially-cleaned state.
    */
  def materializeActiveTokenLocksFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[Signed[TokenLock]]]]

  /** Materialize the full `address → TokenLockReference` view via the rooted `ActiveAddressIndex`. The reference value type doesn't carry
    * `source: Address`, so prefix-scan can't recover keys; the index supplies the keyset and every indexed target must be present and
    * decodable in the same authenticated view.
    */
  def materializeLastTokenLockRefsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, TokenLockReference]]

  /** Materialize the nested `metagraphAddr → holderAddr → Balance` view from the rooted address-pair consensus-index partition. `Balance`
    * carries no source, so the index supplies the `(metagraphAddr, holderAddr)` pairs and every indexed target must be present and
    * decodable in the same authenticated view before the values are grouped by metagraph address.
    */
  def materializeTokenLockBalancesFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedMap[Address, Balance]]]
}

object TokenLockStateManager {

  def make[F[_]: Async](
    reader: GlobalStateReader[F]
  ): TokenLockStateManager[F] =
    new TokenLockStateManager[F] {

      def acceptTokenLocks(
        epochProgress: EpochProgress,
        previousEpochProgress: EpochProgress,
        acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        lastActiveGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]]
      )(implicit hasher: Hasher[F]): F[TokenLockAcceptanceResult] =
        findExpiredGlobalTokenLocksViaIndexFromMpt(previousEpochProgress, epochProgress).flatMap { expired =>
          acceptTokenLocksWithExpired(
            epochProgress,
            acceptedGlobalTokenLocks,
            lastActiveGlobalTokenLocks,
            generatedTokenUnlocksByAddress,
            expired
          )
        }

      def acceptTokenLocksWithExpired(
        epochProgress: EpochProgress,
        acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        lastActiveGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]],
        expiredGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]]
      )(implicit hasher: Hasher[F]): F[TokenLockAcceptanceResult] =
        // No map iteration inside the manager; reconstruct fullState from caller's `lastActive` for API compat.
        // Future #91/#77 work removes `fullState` from the return contract entirely.
        acceptTokenLocksFromMptWithExpired(
          epochProgress,
          acceptedGlobalTokenLocks,
          generatedTokenUnlocksByAddress,
          expiredGlobalTokenLocks
        ).map { d =>
          val reconstructed: SortedMap[Address, SortedSet[Signed[TokenLock]]] =
            (lastActiveGlobalTokenLocks -- d.removedKeys) ++ d.deltas
          val cleaned = reconstructed.filter(_._2.nonEmpty)
          TokenLockAcceptanceResult(
            fullState = cleaned,
            deltas = d.deltas,
            removedKeys = d.removedKeys,
            expiryIndexDelta = d.expiryIndexDelta
          )
        }

      private def acceptReplacementTokenLocksWithParent(
        acceptedTokenLocks: List[Signed[TokenLock]],
        lastSnapshotContext: GlobalSnapshotInfo,
        suppliedParentBackingState: Option[StakeBackingValidator.ValidatedBackingState]
      )(implicit hasher: Hasher[F]): F[List[Signed[TokenLock]]] = {
        // #186 in-round chain fix (2026-07-09): a replacement B whose `replaceTokenLockRef` targets a lock A
        // accepted EARLIER IN THIS SAME LIST used to be dropped silently — the candidate set was read only from
        // the parent-state MPT (`ActiveTokenLocks`), which cannot contain A yet. Block-level acceptance
        // (`TokenLockBlockAcceptanceLogic.processLastTxRefs`) chains same-round blocks through `contextUpdate`
        // and has already advanced `lastTokenLockRefs` past B, so the drop was PERMANENT: any resubmission of B
        // rejects with `ParentOrdinalBelowLastTxOrdinal` and the address's replacement chain is wedged.
        //
        // The fold now additionally threads:
        //   - `inRoundBySource`: DAG (currencyId-empty) locks accepted earlier in this fold, so a later tx in
        //     the same round can replace them exactly as it can replace a parent-state lock;
        //   - `deltaBySource`: the net spendable-balance effect of the txs accepted earlier in this fold
        //     (+replaced.amount − tx.amount − tx.fee for replacements; −tx.amount − tx.fee for plain DAG locks),
        //     so the funding conjunct evaluates against the balance as it will stand AFTER those txs apply,
        //     not the stale parent-state balance.
        //
        // Determinism: this stays a pure fold over the caller-ordered list (GSAM derives it from
        // `tokenLockBlockAcceptanceResult.accepted`, itself produced from `blocks.sorted`) plus reads through
        // the same pinned branch-aware reader — every honest node computes the same include/drop set. For any
        // input without an in-round chain the accepted list is unchanged; the only behavioral deltas are
        // (a) in-round chains are now included, (b) the funding conjunct also counts earlier same-round,
        // same-source debits/credits (strictly consistent with the downstream balance fold in
        // `updateGlobalBalancesByTokenLocksFromMptWithExpired`, which would otherwise be able to fail).
        final case class FoldSt(
          result: List[Signed[TokenLock]],
          seen: Set[Hash],
          inRoundBySource: Map[Address, List[(TokenLockReference, Signed[TokenLock])]],
          deltaBySource: Map[Address, BigInt]
        )

        def recordAccepted(st: FoldSt, tx: Signed[TokenLock], netDelta: BigInt, seenAdd: Option[Hash]): F[FoldSt] =
          // Only DAG locks are replacement targets; currency locks are never candidates.
          if (tx.currencyId.isEmpty)
            TokenLockReference.of(tx).map { txRef =>
              st.copy(
                result = st.result :+ tx,
                seen = st.seen ++ seenAdd,
                inRoundBySource = st.inRoundBySource.updatedWith(tx.source) {
                  case Some(list) => Some(list :+ (txRef, tx))
                  case None       => Some(List((txRef, tx)))
                },
                deltaBySource = st.deltaBySource.updated(tx.source, st.deltaBySource.getOrElse(tx.source, BigInt(0)) + netDelta)
              )
            }
          else
            st.copy(result = st.result :+ tx, seen = st.seen ++ seenAdd).pure[F]

        val needsBackingState = acceptedTokenLocks.exists(_.replaceTokenLockRef.nonEmpty)
        val parentBackingStateF =
          if (!needsBackingState) suppliedParentBackingState.pure[F]
          else suppliedParentBackingState.fold(StakeBackingValidator.validateParent(reader).map(_.some))(_.some.pure[F])

        parentBackingStateF.flatMap { parentBackingState =>
          acceptedTokenLocks
            .foldLeftM(FoldSt(List.empty, Set.empty, Map.empty, Map.empty)) { (st, tx) =>
              tx.replaceTokenLockRef match {
                case Some(replaceTokenLockRef) =>
                  if (tx.currencyId.nonEmpty) {
                    // we can only replace DAG token locks
                    st.pure[F]
                  } else if (st.seen(replaceTokenLockRef)) {
                    st.pure[F]
                  } else {
                    for {
                      activeTokenLocks <- ActiveTokenLockMptReader
                        .readNative(reader, tx.source)
                        .map(_.getOrElse(SortedSet.empty[Signed[TokenLock]]))
                      balance <- reader
                        .get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, tx.source))
                        .map(_.getOrElse(Balance.empty))
                      existingWithRefs <- activeTokenLocks.toList
                        .filter(_.currencyId.isEmpty) // we can only replace DAG token locks
                        .traverse(existing => TokenLockReference.of(existing).map(ref => (ref, existing)))
                      // Parent-state candidates ∪ locks accepted earlier in this same round.
                      candidates = existingWithRefs ++ st.inRoundBySource.getOrElse(tx.source, List.empty)
                      delta = st.deltaBySource.getOrElse(tx.source, BigInt(0))
                      matched = candidates.find {
                        case (ref, existing) =>
                          ref.hash === replaceTokenLockRef &&
                          existing.source == tx.source &&
                          existing.amount < tx.amount &&
                          BigInt(balance.value.value) + delta + BigInt(existing.amount.value.value) >=
                            BigInt(tx.amount.value.value) + BigInt(tx.fee.value.value)
                      }
                      newSt <- matched match {
                        case Some((_, replaced)) =>
                          recordAccepted(
                            st,
                            tx,
                            netDelta = BigInt(replaced.amount.value.value) - BigInt(tx.amount.value.value) - BigInt(tx.fee.value.value),
                            seenAdd = Some(replaceTokenLockRef)
                          )
                        case None => st.pure[F]
                      }
                    } yield newSt
                  }
                case None =>
                  recordAccepted(
                    st,
                    tx,
                    netDelta = -BigInt(tx.amount.value.value) - BigInt(tx.fee.value.value),
                    seenAdd = None
                  )
              }
            }
            .map(_.result)
            .flatTap(result => parentBackingState.traverse_(StakeBackingValidator.validateAcceptedReplacements(_, result)))
        }
      }

      def acceptReplacementTokenLocks(
        acceptedTokenLocks: List[Signed[TokenLock]],
        lastSnapshotContext: GlobalSnapshotInfo
      )(implicit hasher: Hasher[F]): F[List[Signed[TokenLock]]] =
        acceptReplacementTokenLocksWithParent(acceptedTokenLocks, lastSnapshotContext, None)

      override def acceptReplacementTokenLocks(
        acceptedTokenLocks: List[Signed[TokenLock]],
        lastSnapshotContext: GlobalSnapshotInfo,
        parentBackingState: Option[StakeBackingValidator.ValidatedBackingState]
      )(implicit hasher: Hasher[F]): F[List[Signed[TokenLock]]] =
        acceptReplacementTokenLocksWithParent(acceptedTokenLocks, lastSnapshotContext, parentBackingState)

      def acceptTokenLockRefs(
        lastTokenLockRefs: SortedMap[Address, TokenLockReference],
        lastTokenLockContextUpdate: Map[Address, TokenLockReference]
      ): SortedMap[Address, TokenLockReference] =
        lastTokenLockRefs ++ lastTokenLockContextUpdate

      def filterExpiredTokenLocks(
        tokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        epochProgress: EpochProgress
      ): SortedMap[Address, SortedSet[Signed[TokenLock]]] =
        tokenLocks.view.mapValues(_.filter(_.unlockEpoch.exists(_ < epochProgress))).to(SortedMap)

      def findExpiredGlobalTokenLocksViaIndexFromMpt(
        previousEpochProgress: EpochProgress,
        epochProgress: EpochProgress
      )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[Signed[TokenLock]]]] =
        if (previousEpochProgress.value.value >= epochProgress.value.value)
          SortedMap.empty[Address, SortedSet[Signed[TokenLock]]].pure[F]
        else {
          val fromL = previousEpochProgress.value.value
          val toL = epochProgress.value.value - 1L
          val epochs: List[EpochProgress] =
            (fromL to toL).toList.map(v => EpochProgress(NonNegLong.unsafeFrom(v)))

          for {
            buckets <- epochs.traverse { e =>
              for {
                key <- GlobalStateKey.expiryIndexKey[F](SystemNamespaceLabel.ExpiryIndexTokenLocks, e)
                hexKey <- GlobalStateKey.toHex[F](key)
                bucket <- StrictMptRead.valueOrElseF(
                  reader.getStrict[SortedSet[TokenLockExpiryKey]](key),
                  SortedSet.empty[TokenLockExpiryKey],
                  s"sweep ExpiryIndex(label=${SystemNamespaceLabel.ExpiryIndexTokenLocks.canonicalName},epoch=${e.value.value})",
                  hexKey
                )
              } yield e -> bucket
            }
            indexedKeys = buckets.flatMap { case (bucketEpoch, bucket) => bucket.toList.map(bucketEpoch -> _) }
            byAddress = indexedKeys.groupBy(_._2.address)
            resolved <- byAddress.toList.sortBy(_._1).traverse {
              case (addr, indexedExpiryKeys) =>
                val targetKey = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, addr)
                val expectedEpochsByHash = indexedExpiryKeys.groupMap(_._2.hash)(_._1).view.mapValues(_.toSet).toMap
                val expectedHashes = expectedEpochsByHash.keySet
                for {
                  targetHex <- GlobalStateKey.toHex[F](targetKey)
                  addrSet <- ActiveTokenLockMptReader
                    .readNative(reader, addr)
                    .flatMap(
                      _.liftTo[F](
                        StrictMptRead.MissingConsensusMptValue(
                          s"expiry target ActiveTokenLocks(address=$addr)",
                          targetHex
                        )
                      )
                    )
                  hashed <- addrSet.toList.traverse(s => s.toHashed.map(h => (h.hash, s)))
                  actualHashes = hashed.iterator.map(_._1).toSet
                  missingHashes = expectedHashes -- actualHashes
                  wrongEpochs = hashed.collect {
                    case (hash, lock)
                        if expectedEpochsByHash.contains(hash) &&
                          expectedEpochsByHash(hash) != lock.value.unlockEpoch.toSet =>
                      val indexed = expectedEpochsByHash(hash).toList.map(_.value.value).sorted.mkString("/")
                      val actual = lock.value.unlockEpoch.fold("none")(_.value.value.toString)
                      s"${hash.value}:indexed=$indexed,actual=$actual"
                  }.sorted
                  _ <- Async[F]
                    .raiseError[Unit](
                      StrictMptRead.InconsistentConsensusMptIndex(
                        s"expiry target ActiveTokenLocks(address=$addr)",
                        targetHex,
                        s"bucket hashes absent from decoded target: ${missingHashes.toList.map(_.value).sorted.mkString(",")}"
                      )
                    )
                    .whenA(missingHashes.nonEmpty)
                  _ <- Async[F]
                    .raiseError[Unit](
                      StrictMptRead.InconsistentConsensusMptIndex(
                        s"expiry target ActiveTokenLocks(address=$addr)",
                        targetHex,
                        s"bucket epoch differs from target expiry: ${wrongEpochs.mkString(",")}"
                      )
                    )
                    .whenA(wrongEpochs.nonEmpty)
                  matched = hashed.collect { case (h, s) if expectedHashes.contains(h) => s }.to(SortedSet)
                } yield addr -> matched
            }
          } yield SortedMap.from(resolved).filter(_._2.nonEmpty)
        }

      def acceptTokenLocksFromMpt(
        epochProgress: EpochProgress,
        previousEpochProgress: EpochProgress,
        acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]]
      )(implicit hasher: Hasher[F]): F[TokenLockAcceptanceDeltas] =
        findExpiredGlobalTokenLocksViaIndexFromMpt(previousEpochProgress, epochProgress).flatMap { expired =>
          acceptTokenLocksFromMptWithExpired(epochProgress, acceptedGlobalTokenLocks, generatedTokenUnlocksByAddress, expired)
        }

      def acceptTokenLocksFromMptWithExpired(
        epochProgress: EpochProgress,
        acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]],
        expiredGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]]
      )(implicit hasher: Hasher[F]): F[TokenLockAcceptanceDeltas] =
        Async[F].pure(expiredGlobalTokenLocks).flatMap { expiredGlobalTokenLocks =>
          // Touched addresses = everything that might change this ordinal:
          //   - addresses with incoming locks (`acceptedGlobalTokenLocks`)
          //   - addresses with expiring locks (`expiredGlobalTokenLocks`)
          //   - addresses with TokenUnlocks (which remove matching refs even with no other change)
          val touchedAddresses: Set[Address] =
            acceptedGlobalTokenLocks.keySet ++ expiredGlobalTokenLocks.keySet ++ generatedTokenUnlocksByAddress.keySet

          touchedAddresses.toList
            .foldLeftM[F, (SortedMap[Address, SortedSet[Signed[TokenLock]]], Set[Address])](
              (SortedMap.empty[Address, SortedSet[Signed[TokenLock]]], Set.empty[Address])
            ) {
              case ((deltasAcc, removedAcc), address) =>
                for {
                  currentActiveOpt <- ActiveTokenLockMptReader.readNative(reader, address)
                  currentActive = currentActiveOpt.getOrElse(SortedSet.empty[Signed[TokenLock]])
                  incomingLocks = acceptedGlobalTokenLocks.getOrElse(address, SortedSet.empty[Signed[TokenLock]])
                  unexpired = (currentActive ++ incomingLocks).filter(_.unlockEpoch.forall(_ >= epochProgress))
                  addressTokenUnlocks = generatedTokenUnlocksByAddress.getOrElse(address, List.empty)
                  unlocksRefs = addressTokenUnlocks.map(_.tokenLockRef)
                  filteredLocks <- unexpired.foldM(SortedSet.empty[Signed[TokenLock]]) { (innerAcc, tokenLock) =>
                    tokenLock.toHashed.map { tlh =>
                      if (unlocksRefs.contains(tlh.hash)) innerAcc
                      else innerAcc + tokenLock
                    }
                  }
                } yield
                  if (currentActive == filteredLocks) (deltasAcc, removedAcc) // no-op
                  else if (filteredLocks.isEmpty) (deltasAcc, removedAcc + address)
                  else (deltasAcc.updated(address, filteredLocks), removedAcc)
            }
            .flatMap {
              case (deltas, removedKeys) =>
                // Build `newSetByAddress` for the index-delta computation: emptied addrs contribute ∅.
                val newSetByAddress: SortedMap[Address, SortedSet[Signed[TokenLock]]] =
                  deltas ++ SortedMap.from(removedKeys.toList.map(_ -> SortedSet.empty[Signed[TokenLock]]))

                // Diff per touched address: before = MPT read, after = newSetByAddress entry.
                def hashIndexable(addr: Address, locks: SortedSet[Signed[TokenLock]]): F[List[(EpochProgress, TokenLockExpiryKey)]] =
                  locks.toList.mapFilter(l => l.unlockEpoch.map(e => (e, l))).traverse {
                    case (epoch, l) => l.toHashed.map(h => (epoch, TokenLockExpiryKey(addr, h.hash)))
                  }

                type Entries = List[(EpochProgress, TokenLockExpiryKey)]
                touchedAddresses.toList
                  .foldLeftM[F, (Entries, Entries)]((List.empty, List.empty)) {
                    case ((accAdds, accRemoves), addr) =>
                      ActiveTokenLockMptReader
                        .readNative(reader, addr)
                        .flatMap { oldSetOpt =>
                          val oldSet = oldSetOpt.getOrElse(SortedSet.empty[Signed[TokenLock]])
                          val newSet = newSetByAddress.getOrElse(addr, oldSet) // untouched-but-present means no diff → same old set
                          val added = newSet.diff(oldSet)
                          val removed = oldSet.diff(newSet)
                          for {
                            addedEntries <- hashIndexable(addr, added)
                            removedEntries <- hashIndexable(addr, removed)
                          } yield (accAdds ++ addedEntries, accRemoves ++ removedEntries)
                        }
                  }
                  .map {
                    case (adds, removes) =>
                      val addsMap: SortedMap[EpochProgress, Set[TokenLockExpiryKey]] =
                        adds.groupMap(_._1)(_._2).view.mapValues(_.toSet).to(SortedMap)
                      val removesMap: SortedMap[EpochProgress, Set[TokenLockExpiryKey]] =
                        removes.groupMap(_._1)(_._2).view.mapValues(_.toSet).to(SortedMap)
                      TokenLockAcceptanceDeltas(
                        deltas = deltas,
                        removedKeys = removedKeys,
                        expiryIndexDelta = SystemIndexDelta.EpochBucket[TokenLockExpiryKey](addsMap, removesMap)
                      )
                  }
            }
        }

      def updateGlobalBalancesByTokenLocksFromMpt(
        epochProgress: EpochProgress,
        previousEpochProgress: EpochProgress,
        currentBalances: SortedMap[Address, Balance],
        acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]]
      )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]] =
        findExpiredGlobalTokenLocksViaIndexFromMpt(previousEpochProgress, epochProgress).flatMap { expired =>
          updateGlobalBalancesByTokenLocksFromMptWithExpired(
            epochProgress,
            currentBalances,
            acceptedGlobalTokenLocks,
            generatedTokenUnlocksByAddress,
            expired
          )
        }

      def updateGlobalBalancesByTokenLocksFromMptWithExpired(
        epochProgress: EpochProgress,
        currentBalances: SortedMap[Address, Balance],
        acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]],
        expiredGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]]
      )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]] =
        for {
          _ <- Async[F].raiseWhen(
            acceptedGlobalTokenLocks.valuesIterator.flatten.exists(_.unlockEpoch.exists(_ < epochProgress))
          )(
            new IllegalStateException(
              s"Newly accepted token lock is already expired at execution epoch=${epochProgress.value.value}"
            )
          )
          canonicalGeneratedUnlocksByAddress <- generatedTokenUnlocksByAddress.toList.flatMap {
            case (declaredAddress, unlocks) => unlocks.map(declaredAddress -> _)
          }
            .groupBy(_._2.tokenLockRef)
            .toList
            .sortBy(_._1)
            .foldLeftM(Map.empty[Address, List[TokenUnlock]]) {
              case (canonical, (tokenLockRef, entries)) =>
                val (_, expected) = entries.head
                if (entries.forall { case (declaredAddress, unlock) => declaredAddress === unlock.source && unlock === expected })
                  canonical
                    .updatedWith(expected.source) {
                      case Some(unlocks) => Some(unlocks :+ expected)
                      case None          => Some(List(expected))
                    }
                    .pure[F]
                else
                  Async[F].raiseError[Map[Address, List[TokenUnlock]]](
                    new IllegalStateException(s"Conflicting token unlock payloads for ref=$tokenLockRef")
                  )
            }
          expiredUnlockEntries <- expiredGlobalTokenLocks.toList.flatTraverse {
            case (address, locks) =>
              locks.toList.traverse { lock =>
                lock.toHashed.map { hashed =>
                  hashed.hash -> (
                    address,
                    TokenUnlock(hashed.hash, lock.amount, lock.currencyId, lock.source)
                  )
                }
              }
          }
          expiredUnlocksByRef <- expiredUnlockEntries.sortBy(_._1).foldLeftM(Map.empty[Hash, TokenUnlock]) {
            case (canonical, (tokenLockRef, (declaredAddress, unlock))) =>
              canonical.get(tokenLockRef) match {
                case _ if declaredAddress =!= unlock.source =>
                  Async[F].raiseError[Map[Hash, TokenUnlock]](
                    new IllegalStateException(s"Expired token lock address mismatch for ref=$tokenLockRef")
                  )
                case Some(existing) if existing =!= unlock =>
                  Async[F].raiseError[Map[Hash, TokenUnlock]](
                    new IllegalStateException(s"Conflicting expired token lock payloads for ref=$tokenLockRef")
                  )
                case Some(_) => canonical.pure[F]
                case None    => canonical.updated(tokenLockRef, unlock).pure[F]
              }
          }
          _ <- canonicalGeneratedUnlocksByAddress.values.toList.flatten.traverse_ { unlock =>
            expiredUnlocksByRef.get(unlock.tokenLockRef).traverse_ { expiredUnlock =>
              Async[F].raiseUnless(unlock === expiredUnlock)(
                new IllegalStateException(s"Generated unlock conflicts with expired token lock ref=${unlock.tokenLockRef}")
              )
            }
          }
          expiredRefsByAddress = expiredUnlocksByRef.values.toList
            .groupBy(_.source)
            .view
            .mapValues(_.map(_.tokenLockRef).toSet)
            .toMap
          result <-
            (acceptedGlobalTokenLocks.keySet ++ expiredGlobalTokenLocks.keySet ++ canonicalGeneratedUnlocksByAddress.keySet).toList.sorted
              .foldM[F, Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]](
                Right((currentBalances, SortedMap.empty[Address, Balance]))
              ) {
                case (Left(err), _) =>
                  (Left(err): Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]).pure[F]
                case (Right((balances, balancesDelta)), address) =>
                  readBalance(address, balances).map { initialBalance =>
                    val tokenLocks = acceptedGlobalTokenLocks.getOrElse(address, SortedSet.empty[Signed[TokenLock]]) ++
                      expiredGlobalTokenLocks.getOrElse(address, SortedSet.empty[Signed[TokenLock]])
                    val expiredRefs = expiredRefsByAddress.getOrElse(address, Set.empty)
                    val uniqueNonExpiredUnlocks = canonicalGeneratedUnlocksByAddress
                      .getOrElse(address, List.empty)
                      .filterNot(unlock => expiredRefs.contains(unlock.tokenLockRef))
                    val unlockCredit = uniqueNonExpiredUnlocks
                      .foldLeft(BigInt(0))((sum, unlock) => sum + BigInt(unlock.amount.value.value))
                    val expiredCredit = tokenLocks
                      .filter(_.unlockEpoch.exists(_ < epochProgress))
                      .foldLeft(BigInt(0))((sum, lock) => sum + BigInt(lock.amount.value.value))
                    val unexpiredDebit = tokenLocks
                      .filter(_.unlockEpoch.forall(_ >= epochProgress))
                      .foldLeft(BigInt(0)) { (sum, lock) =>
                        sum + BigInt(lock.amount.value.value) + BigInt(lock.fee.value.value)
                      }
                    val finalValue = BigInt(initialBalance.value.value) + unlockCredit + expiredCredit - unexpiredDebit

                    val nextBalance: Either[BalanceArithmeticError, Balance] =
                      if (finalValue < 0) AmountUnderflow.asLeft
                      else if (finalValue > BigInt(Long.MaxValue)) AmountOverflow.asLeft
                      else Balance(NonNegLong.unsafeFrom(finalValue.longValue)).asRight

                    nextBalance.map { finalBalance =>
                      (balances.updated(address, finalBalance), balancesDelta.updated(address, finalBalance))
                    }
                  }
              }
        } yield result

      def updateTokenLockBalancesFromMpt(
        currencySnapshots: SortedMap[Address, CurrencySnapshotWithState]
      )(implicit hasher: Hasher[F]): F[TokenLockBalanceDeltas] =
        // Iterate currencySnapshots only — mids NOT in this input keep their existing balances in the MPT unchanged.
        // For each mid, compute the new per-holder balance map from its currency snapshot's activeTokenLocks.
        // Emit `deltas` for mids whose new map differs from the mid's current MPT state. Detect "was non-empty, now empty"
        // via per-holder point reads against the mid's currencySnapshot holders ∪ MPT-known holders... but we don't have a
        // cheap way to enumerate MPT-known holders for a mid without a prefix scan. For now, emit `(mid → new state)` for
        // every mid in currencySnapshots; the caller reconciles with lastContext to detect actual removals.
        currencySnapshots.toList.traverse {
          case (metagraphId, currencySnapshotWithState) =>
            val activeTokenLocks = currencySnapshotWithState match {
              case Left(_)          => SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]
              case Right((_, info)) => info.activeTokenLocks.getOrElse(SortedMap.empty[Address, SortedSet[Signed[TokenLock]]])
            }
            val metagraphTokenLocksAmounts = activeTokenLocks.foldLeft(SortedMap.empty[Address, Balance]) {
              case (accBalances, addressTokenLocks) =>
                val (address, tokenLocks) = addressTokenLocks
                val amount = NonNegLong.unsafeFrom(tokenLocks.toList.map(_.amount.value.value).sum)
                accBalances.updated(address, Balance(amount))
            }
            (metagraphId, metagraphTokenLocksAmounts).pure[F]
        }.map { entries =>
          val deltasMap = SortedMap.from(entries.filter(_._2.nonEmpty))
          // Pair-shaped removals require knowing prior holders for each mid; this F-returning helper doesn't carry
          // that input today. Left empty until a caller actually consumes this and threads in prior MPT state.
          TokenLockBalanceDeltas(deltas = deltasMap, removedKeys = Set.empty)
        }

      def updateTokenLockBalances(
        currencySnapshots: SortedMap[Address, CurrencySnapshotWithState],
        maybeLastTokenLockBalances: Option[SortedMap[Address, SortedMap[Address, Balance]]]
      ): TokenLockBalanceResult = {
        val lastTokenLockBalances = maybeLastTokenLockBalances.getOrElse(SortedMap.empty[Address, SortedMap[Address, Balance]])

        val (fullState, deltas) =
          currencySnapshots.foldLeft((lastTokenLockBalances, SortedMap.empty[Address, SortedMap[Address, Balance]])) {
            case ((accTokenLockBalances, accDeltas), (metagraphId, currencySnapshotWithState)) =>
              val activeTokenLocks = currencySnapshotWithState match {
                case Left(_)          => SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]
                case Right((_, info)) => info.activeTokenLocks.getOrElse(SortedMap.empty[Address, SortedSet[Signed[TokenLock]]])
              }

              val metagraphTokenLocksAmounts = activeTokenLocks.foldLeft(SortedMap.empty[Address, Balance]) {
                case (accBalances, addressTokenLocks) =>
                  val (address, tokenLocks) = addressTokenLocks
                  val amount = NonNegLong.unsafeFrom(tokenLocks.toList.map(_.amount.value.value).sum)
                  accBalances.updated(address, Balance(amount))
              }

              val previousMetagraphBalances = accTokenLockBalances.getOrElse(metagraphId, SortedMap.empty[Address, Balance])
              val hasChanged = previousMetagraphBalances != metagraphTokenLocksAmounts

              val newAccTokenLockBalances = accTokenLockBalances + (metagraphId -> metagraphTokenLocksAmounts)
              val newAccDeltas = if (hasChanged) accDeltas + (metagraphId -> metagraphTokenLocksAmounts) else accDeltas

              (newAccTokenLockBalances, newAccDeltas)
          }

        // Clean empty entries from fullState
        val cleanedFullState = fullState.filter { case (_, balances) => balances.nonEmpty }
        val cleanedDeltas = deltas.filter { case (_, balances) => balances.nonEmpty }

        // Pair-shaped removals: enumerate every (mid, holder) that was present in the prior state but is
        // absent from the post-accept state. Mirrors AllowSpend's `(metagraphIdOpt, address)` enumeration —
        // mid-only removals would target the wrong MPT key shape (writes use `hypergraph(TokenLockBalances,
        // mid, holder)`) and the entries would persist, replaying as a stale "removed" delta every ordinal.
        val removedKeys: Set[(Address, Address)] = lastTokenLockBalances.iterator.flatMap {
          case (metagraphId, priorBalances) =>
            val updatedBalances = cleanedFullState.getOrElse(metagraphId, SortedMap.empty[Address, Balance])
            priorBalances.iterator.collect {
              case (holder, _) if !updatedBalances.contains(holder) => (metagraphId, holder)
            }
        }.toSet

        TokenLockBalanceResult(cleanedFullState, cleanedDeltas, removedKeys)
      }

      def updateGlobalBalancesByTokenLocks(
        epochProgress: EpochProgress,
        previousEpochProgress: EpochProgress,
        currentBalances: SortedMap[Address, Balance],
        acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]]
      )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]] =
        updateGlobalBalancesByTokenLocksFromMpt(
          epochProgress,
          previousEpochProgress,
          currentBalances,
          acceptedGlobalTokenLocks,
          generatedTokenUnlocksByAddress
        )

      def updateGlobalBalancesByTokenLocksWithExpired(
        epochProgress: EpochProgress,
        currentBalances: SortedMap[Address, Balance],
        acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]],
        expiredGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]]
      )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]] =
        updateGlobalBalancesByTokenLocksFromMptWithExpired(
          epochProgress,
          currentBalances,
          acceptedGlobalTokenLocks,
          generatedTokenUnlocksByAddress,
          expiredGlobalTokenLocks
        )

      private def readBalance(
        address: Address,
        deltas: SortedMap[Address, Balance]
      )(implicit hasher: Hasher[F]): F[Balance] =
        deltas.get(address) match {
          case Some(b) => b.pure[F]
          case None => reader.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, address)).map(_.getOrElse(Balance.empty))
        }

      def generateTokenUnlocks(
        expiredDelegatedWithdrawals: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
        expiredCollateralWithdrawals: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]],
        acceptedTokenLocks: List[Signed[TokenLock]],
        globalActiveTokenLocksByRef: Map[Hash, Signed[TokenLock]]
      ): Either[String, Map[Address, List[TokenUnlock]]] = {

        val increasedTokenLockUnlocks = acceptedTokenLocks
          .mapFilter(tl => tl.replaceTokenLockRef.tupleLeft(tl))
          .traverse {
            case (tokenLock, replaceRef) =>
              globalActiveTokenLocksByRef
                .get(replaceRef)
                .toRight(s"Token lock not found for replacement ref: $replaceRef")
                .map { activeTokenLock =>
                  (
                    tokenLock.source,
                    TokenUnlock(
                      replaceRef,
                      activeTokenLock.amount,
                      activeTokenLock.currencyId,
                      activeTokenLock.source
                    )
                  )
                }
          }
          .map(_.groupBy { case (address, _) => address }.view.mapValues(_.map { case (_, tokenUnlock) => tokenUnlock }).toMap)

        val expiredEncumbrances: List[(Address, Hash)] =
          expiredDelegatedWithdrawals.toList.flatMap {
            case (address, withdrawals) => withdrawals.toList.map(withdrawal => address -> withdrawal.tokenLockRef)
          } ++
            expiredCollateralWithdrawals.toList.flatMap {
              case (address, withdrawals) => withdrawals.toList.map(withdrawal => address -> withdrawal.event.tokenLockRef)
            }

        val duplicateExpiredRefs =
          expiredEncumbrances.groupMapReduce(_._2)(_ => 1)(_ + _).collect { case (ref, count) if count > 1 => ref }.toList.sorted

        val expiredWithdrawalUnlocks =
          Either
            .cond(
              duplicateExpiredRefs.isEmpty,
              (),
              s"Backing token lock appears in multiple expired withdrawals: ${duplicateExpiredRefs.mkString(",")}"
            )
            .flatMap { _ =>
              expiredEncumbrances.sortBy { case (address, ref) => (address, ref) }.traverse {
                case (address, ref) =>
                  for {
                    activeTokenLock <- globalActiveTokenLocksByRef
                      .get(ref)
                      .toRight(s"Token lock not found for ref: $ref")
                    _ <- Either.cond(
                      activeTokenLock.source === address,
                      (),
                      s"Backing token lock source mismatch for ref=$ref expected=$address actual=${activeTokenLock.source}"
                    )
                    _ <- Either.cond(
                      activeTokenLock.currencyId.isEmpty,
                      (),
                      s"Backing token lock is not native for ref=$ref"
                    )
                  } yield
                    address -> TokenUnlock(
                      ref,
                      activeTokenLock.amount,
                      activeTokenLock.currencyId,
                      activeTokenLock.source
                    )
              }
                .map(
                  _.groupMap(_._1)(_._2).view
                    .mapValues(_.sortBy(_.tokenLockRef))
                    .toMap
                )
            }

        for {
          withdrawalUnlocks <- expiredWithdrawalUnlocks
          replacedUnlocks <- increasedTokenLockUnlocks
        } yield {
          val allAddresses = withdrawalUnlocks.keySet ++ replacedUnlocks.keySet
          allAddresses.map { address =>
            val withdrawalList = withdrawalUnlocks.getOrElse(address, List.empty)
            val replacedList = replacedUnlocks.getOrElse(address, List.empty)
            address -> (withdrawalList ++ replacedList)
          }.toMap
        }
      }

      def buildActiveTokenLocksByRefFromMpt(
        addresses: Set[Address]
      )(implicit hasher: Hasher[F]): F[Map[Hash, Signed[TokenLock]]] =
        addresses.toList.flatTraverse { addr =>
          ActiveTokenLockMptReader
            .readNative(reader, addr)
            .map(_.fold(List.empty[Signed[TokenLock]])(_.toList))
        }.flatMap { locks =>
          locks.traverse(lock => lock.toHashed.map(h => h.hash -> lock)).map(_.toMap)
        }

      def materializeActiveTokenLocksFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[Signed[TokenLock]]]] =
        ActiveTokenLockMptReader.materializeNative(reader)

      def materializeLastTokenLockRefsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, TokenLockReference]] =
        for {
          indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastTokenLockRefs)
          indexHex <- GlobalStateKey.toHex[F](indexKey)
          addrSet <- StrictMptRead.valueOrElseF(
            reader.getStrict[SortedSet[Address]](indexKey),
            SortedSet.empty[Address],
            "materialize LastTokenLockRefs ActiveAddressIndex",
            indexHex
          )
          entries <- addrSet.toList.traverse { addr =>
            val targetKey = GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, addr)
            for {
              targetHex <- GlobalStateKey.toHex[F](targetKey)
              value <- StrictMptRead.requirePresentF(
                reader.getStrict[TokenLockReference](targetKey),
                s"materialize LastTokenLockRefs indexed target(address=$addr)",
                targetHex
              )
            } yield addr -> value
          }
        } yield SortedMap.from(entries)

      def materializeTokenLockBalancesFromMpt(
        implicit hasher: Hasher[F]
      ): F[SortedMap[Address, SortedMap[Address, Balance]]] =
        for {
          indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.TokenLockBalances)
          indexHex <- GlobalStateKey.toHex[F](indexKey)
          pairSet <- StrictMptRead.valueOrElseF(
            reader.getStrict[SortedSet[(Address, Address)]](indexKey),
            SortedSet.empty[(Address, Address)],
            "materialize TokenLockBalances ActiveAddressIndex pair set",
            indexHex
          )
          flat <- pairSet.toList.traverse {
            case (mid, holder) =>
              val targetKey = GlobalStateKey.hypergraph(GlobalStateFieldId.TokenLockBalances, mid, holder)
              for {
                targetHex <- GlobalStateKey.toHex[F](targetKey)
                value <- StrictMptRead.requirePresentF(
                  reader.getStrict[Balance](targetKey),
                  s"materialize TokenLockBalances indexed target(metagraph=$mid,holder=$holder)",
                  targetHex
                )
              } yield (mid, holder, value)
          }
        } yield
          flat
            .groupBy(_._1)
            .view
            .mapValues(entries => SortedMap.from(entries.map { case (_, h, b) => h -> b }))
            .filter { case (_, inner) => inner.nonEmpty }
            .to(SortedMap)
    }
}
