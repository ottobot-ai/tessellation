package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.TokenUnlock
import io.constellationnetwork.schema.balance.{Balance, BalanceArithmeticError}
import io.constellationnetwork.schema.delegatedStake.PendingDelegatedStakeWithdrawal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
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
  * syncFromStateChanges removal targets a real key and the address-pair sidecar can prune the same pair. Using a mid-only `Set[Address]`
  * would target nothing and the entry would persist forever, surfacing as gl1 verify-replay mptRoot drift on every sc-event ordinal.
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

  /** MPT-backed `acceptTokenLocks`. Reads current active sets per-address via `mptStore.getActiveTokenLocks` instead of iterating an
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

  /** MPT-backed `findExpiredGlobalTokenLocksViaIndex`. Resolves each expiring hash via
    * `reader.get[SortedSet[Signed[TokenLock]]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, addr))` instead of an
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

  /** Materialize the full `address → TokenLockReference` view via the `ActiveAddressIndex` sidecar. The reference value type doesn't carry
    * `source: Address`, so prefix-scan can't recover keys; instead we read the address set from the sidecar and batch point-read values.
    */
  def materializeLastTokenLockRefsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, TokenLockReference]]

  /** Materialize the nested `metagraphAddr → holderAddr → Balance` view from the address-pair sidecar partition. `Balance` carries no
    * source so we read the `(metagraphAddr, holderAddr)` pairs from the sidecar, batch point-read each pair's balance, then group by
    * metagraphAddr. The sidecar is append-only at the writer; pairs whose underlying entry has been removed return `None` and are filtered
    * out here.
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

      def acceptReplacementTokenLocks(
        acceptedTokenLocks: List[Signed[TokenLock]],
        lastSnapshotContext: GlobalSnapshotInfo
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
                    activeTokenLocks <- reader
                      .get[SortedSet[Signed[TokenLock]]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, tx.source))
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
      }

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
              GlobalStateKey
                .expiryIndexKey[F](io.constellationnetwork.schema.mpt.SystemNamespaceLabel.ExpiryIndexTokenLocks, e)
                .flatMap(reader.get[SortedSet[TokenLockExpiryKey]])
                .map(_.getOrElse(SortedSet.empty[TokenLockExpiryKey]))
            }
            allKeys = buckets.flatten.toSet
            byAddress = allKeys.groupBy(_.address)
            resolved <- byAddress.toList.traverse {
              case (addr, expiryKeys) =>
                reader.get[SortedSet[Signed[TokenLock]]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, addr)).flatMap {
                  addrSetOpt =>
                    val addrSet = addrSetOpt.getOrElse(SortedSet.empty[Signed[TokenLock]])
                    val expectedHashes = expiryKeys.map(_.hash)
                    addrSet.toList.traverse(s => s.toHashed.map(h => (h.hash, s))).map { hashed =>
                      val matched = hashed.collect { case (h, s) if expectedHashes.contains(h) => s }.to(SortedSet)
                      addr -> matched
                    }
                }
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
                  currentActiveOpt <- reader.get[SortedSet[Signed[TokenLock]]](
                    GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, address)
                  )
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
                      reader
                        .get[SortedSet[Signed[TokenLock]]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, addr))
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
        Async[F].pure(expiredGlobalTokenLocks).flatMap { expiredGlobalTokenLocks =>
          val afterTokenLocksF = (acceptedGlobalTokenLocks |+| expiredGlobalTokenLocks).toList
            .foldM[F, Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]](
              Right((currentBalances, SortedMap.empty[Address, Balance]))
            ) {
              case (Left(err), _) =>
                (Left(err): Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]).pure[F]
              case (Right((balances, balancesDelta)), (address, tokenLocks)) =>
                readBalance(address, balances).map { initialBalance =>
                  val addressTokenUnlocks = generatedTokenUnlocksByAddress.getOrElse(address, List.empty)
                  val result: Either[BalanceArithmeticError, Balance] = for {
                    unlocked <- addressTokenUnlocks.foldLeft[Either[BalanceArithmeticError, Balance]](Right(initialBalance)) {
                      case (currentBalanceEither, tokenUnlock) =>
                        for {
                          currentBalance <- currentBalanceEither
                          balanceAfterUnlock <- currentBalance.plus(TokenLockAmount.toAmount(tokenUnlock.amount))
                        } yield balanceAfterUnlock
                    }
                    expired <- {
                      val expiredLocks = tokenLocks.filter(_.unlockEpoch.exists(_ < epochProgress))
                      expiredLocks.foldLeft[Either[BalanceArithmeticError, Balance]](Right(unlocked)) { (currentBalanceEither, tokenLock) =>
                        for {
                          currentBalance <- currentBalanceEither
                          balanceAfterExpiredAmount <- currentBalance.plus(TokenLockAmount.toAmount(tokenLock.amount))
                        } yield balanceAfterExpiredAmount
                      }
                    }
                    finalBalance <- {
                      val unexpired = tokenLocks.filter(_.unlockEpoch.forall(_ >= epochProgress))
                      unexpired.foldLeft[Either[BalanceArithmeticError, Balance]](Right(expired)) { (currentBalanceEither, tokenLock) =>
                        for {
                          currentBalance <- currentBalanceEither
                          balanceAfterAmount <- currentBalance.minus(TokenLockAmount.toAmount(tokenLock.amount))
                          balanceAfterFee <- balanceAfterAmount.minus(TokenLockFee.toAmount(tokenLock.fee))
                        } yield balanceAfterFee
                      }
                    }
                  } yield finalBalance

                  result.map { finalBalance =>
                    (balances.updated(address, finalBalance), balancesDelta.updated(address, finalBalance))
                  }
                }
            }

          afterTokenLocksF.flatMap {
            case Left(err) =>
              (Left(err): Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]).pure[F]
            case Right(balancesAfter) =>
              val addressesWithTokenLocks = acceptedGlobalTokenLocks.keySet ++ expiredGlobalTokenLocks.keySet
              val addressesWithTokenUnlocksOnly = generatedTokenUnlocksByAddress.keySet -- addressesWithTokenLocks

              addressesWithTokenUnlocksOnly.toList
                .foldM[F, Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]](
                  Right(balancesAfter)
                ) {
                  case (Left(err), _) =>
                    (Left(err): Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]).pure[F]
                  case (Right((balances, balancesDelta)), address) =>
                    readBalance(address, balances).map { initialBalance =>
                      val addressTokenUnlocks = generatedTokenUnlocksByAddress.getOrElse(address, List.empty)
                      val result = addressTokenUnlocks.foldLeft[Either[BalanceArithmeticError, Balance]](Right(initialBalance)) {
                        case (currentBalanceEither, tokenUnlock) =>
                          for {
                            currentBalance <- currentBalanceEither
                            balanceAfterUnlock <- currentBalance.plus(TokenLockAmount.toAmount(tokenUnlock.amount))
                          } yield balanceAfterUnlock
                      }
                      result.map { finalBalance =>
                        (balances.updated(address, finalBalance), balancesDelta.updated(address, finalBalance))
                      }
                    }
                }
          }
        }

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
        expiredWithdrawals: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
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

        val expiredWithdrawalUnlocks = expiredWithdrawals.toList.traverse {
          case (address, withdrawals) =>
            withdrawals.toList.traverse { pw: PendingDelegatedStakeWithdrawal =>
              for {
                activeTokenLock <- globalActiveTokenLocksByRef
                  .get(pw.tokenLockRef)
                  .toRight(s"Token lock not found for ref: ${pw.tokenLockRef}")
              } yield
                TokenUnlock(
                  pw.tokenLockRef,
                  activeTokenLock.amount,
                  activeTokenLock.currencyId,
                  activeTokenLock.source
                )
            }.map(tokenUnlocks => address -> tokenUnlocks)
        }.map(_.toMap)

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
          reader
            .get[SortedSet[Signed[TokenLock]]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, addr))
            .map(_.fold(List.empty[Signed[TokenLock]])(_.toList))
        }.flatMap { locks =>
          locks.traverse(lock => lock.toHashed.map(h => h.hash -> lock)).map(_.toMap)
        }

      def materializeActiveTokenLocksFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[Signed[TokenLock]]]] =
        for {
          prefix <- GlobalStateKey.hypergraphFieldPrefix[F](GlobalStateFieldId.ActiveTokenLocks)
          entries <- reader.getAllForPrefix[SortedSet[Signed[TokenLock]]](prefix)
        } yield
          SortedMap.from(
            entries.values.toList.mapFilter(set => set.headOption.map(_.value.source -> set)).filter(_._2.nonEmpty)
          )

      def materializeLastTokenLockRefsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Address, TokenLockReference]] =
        for {
          indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastTokenLockRefs)
          addrSet <- reader.get[SortedSet[Address]](indexKey).map(_.getOrElse(SortedSet.empty[Address]))
          addrList = addrSet.toList
          keys = addrList.map(addr => GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, addr))
          values <- reader.getMany[TokenLockReference](keys)
        } yield
          SortedMap.from(addrList.flatMap { addr =>
            val key = GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, addr)
            values.get(key).map(addr -> _)
          })

      def materializeTokenLockBalancesFromMpt(
        implicit hasher: Hasher[F]
      ): F[SortedMap[Address, SortedMap[Address, Balance]]] =
        for {
          indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.TokenLockBalances)
          pairSet <- reader
            .get[SortedSet[(Address, Address)]](indexKey)
            .map(_.getOrElse(SortedSet.empty[(Address, Address)]))
          pairList = pairSet.toList
          keys = pairList.map { case (mid, holder) => GlobalStateKey.hypergraph(GlobalStateFieldId.TokenLockBalances, mid, holder) }
          values <- reader.getMany[Balance](keys)
        } yield {
          val flat = pairList.flatMap {
            case (mid, holder) =>
              val k = GlobalStateKey.hypergraph(GlobalStateFieldId.TokenLockBalances, mid, holder)
              values.get(k).map(bal => (mid, holder, bal))
          }
          flat
            .groupBy(_._1)
            .view
            .mapValues(entries => SortedMap.from(entries.map { case (_, h, b) => h -> b }))
            .filter { case (_, inner) => inner.nonEmpty }
            .to(SortedMap)
        }
    }
}
