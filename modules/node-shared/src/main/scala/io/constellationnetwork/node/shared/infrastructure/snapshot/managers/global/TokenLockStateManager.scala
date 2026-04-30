package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.TokenUnlock
import io.constellationnetwork.schema.balance.{Balance, BalanceArithmeticError}
import io.constellationnetwork.schema.delegatedStake.PendingDelegatedStakeWithdrawal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.tokenLockExpiryKeySetImmutableCodec

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

/** Result of token lock balance update containing full state, deltas, and removed keys */
case class TokenLockBalanceResult(
  fullState: SortedMap[Address, SortedMap[Address, Balance]],
  deltas: SortedMap[Address, SortedMap[Address, Balance]],
  removedKeys: Set[Address] = Set.empty
)

/** Deltas-only result for the MPT-backed token-lock-balances path (#85). */
case class TokenLockBalanceDeltas(
  deltas: SortedMap[Address, SortedMap[Address, Balance]],
  removedKeys: Set[Address] = Set.empty
)

trait TokenLockStateManager[F[_]] {
  def acceptTokenLocks(
    epochProgress: EpochProgress,
    previousEpochProgress: EpochProgress,
    acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    lastActiveGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]]
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

  /** MPT-backed `acceptTokenLocks`. Reads current active sets per-address via `mptStore.getActiveTokenLocks` instead of iterating an
    * in-memory map. Returns deltas + removedKeys + expiry-index delta; caller reconstructs full state if needed.
    */
  def acceptTokenLocksFromMpt(
    epochProgress: EpochProgress,
    previousEpochProgress: EpochProgress,
    acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]]
  )(implicit hasher: Hasher[F]): F[TokenLockAcceptanceDeltas]

  /** MPT-backed `findExpiredGlobalTokenLocksViaIndex`. Resolves each expiring hash via `mptStore.getActiveTokenLocks(addr)` instead of an
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
}

object TokenLockStateManager {

  def make[F[_]: Async](
    mptStore: MptStore[F, GlobalStateKey]
  ): TokenLockStateManager[F] =
    new TokenLockStateManager[F] {

      def acceptTokenLocks(
        epochProgress: EpochProgress,
        previousEpochProgress: EpochProgress,
        acceptedGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        lastActiveGlobalTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
        generatedTokenUnlocksByAddress: Map[Address, List[TokenUnlock]]
      )(implicit hasher: Hasher[F]): F[TokenLockAcceptanceResult] =
        // No map iteration inside the manager; reconstruct fullState from caller's `lastActive` for API compat.
        // Future #91/#77 work removes `fullState` from the return contract entirely.
        acceptTokenLocksFromMpt(epochProgress, previousEpochProgress, acceptedGlobalTokenLocks, generatedTokenUnlocksByAddress).map { d =>
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
      )(implicit hasher: Hasher[F]): F[List[Signed[TokenLock]]] =
        acceptedTokenLocks
          .foldLeftM((List.empty[Signed[TokenLock]], Set.empty[Hash])) {
            case ((result, seen), tx) =>
              tx.replaceTokenLockRef match {
                case Some(replaceTokenLockRef) =>
                  if (tx.currencyId.nonEmpty) {
                    // we can only replace DAG token locks
                    (result, seen).pure[F]
                  } else if (seen(replaceTokenLockRef)) {
                    (result, seen).pure[F]
                  } else {
                    for {
                      activeTokenLocks <- mptStore.getActiveTokenLocks(tx.source).map(_.getOrElse(SortedSet.empty[Signed[TokenLock]]))
                      balance <- mptStore.getBalance(tx.source).map(_.getOrElse(Balance.empty))
                      existingWithRefs <- activeTokenLocks.toList
                        .filter(_.currencyId.isEmpty) // we can only replace DAG token locks
                        .traverse(existing => TokenLockReference.of(existing).map(ref => (ref, existing)))
                    } yield {
                      val shouldInclude = existingWithRefs.exists {
                        case (ref, existing) =>
                          ref.hash === replaceTokenLockRef &&
                          existing.source == tx.source &&
                          existing.amount < tx.amount &&
                          balance.value.value + existing.amount.value.value >= tx.amount.value.value + tx.fee.value.value
                      }
                      if (shouldInclude) (result :+ tx, seen + replaceTokenLockRef) else (result, seen)
                    }
                  }
                case None => (result :+ tx, seen).pure[F]
              }
          }
          .map(_._1)

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
              mptStore
                .getExpiryBucket[TokenLockExpiryKey](io.constellationnetwork.schema.mpt.SystemNamespaceLabel.ExpiryIndexTokenLocks, e)
                .map(_.getOrElse(SortedSet.empty[TokenLockExpiryKey]))
            }
            allKeys = buckets.flatten.toSet
            byAddress = allKeys.groupBy(_.address)
            resolved <- byAddress.toList.traverse {
              case (addr, expiryKeys) =>
                mptStore.getActiveTokenLocks(addr).flatMap { addrSetOpt =>
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
        findExpiredGlobalTokenLocksViaIndexFromMpt(previousEpochProgress, epochProgress).flatMap { expiredGlobalTokenLocks =>
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
                  currentActiveOpt <- mptStore.getActiveTokenLocks(address)
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
                      mptStore.getActiveTokenLocks(addr).flatMap { oldSetOpt =>
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
        findExpiredGlobalTokenLocksViaIndexFromMpt(previousEpochProgress, epochProgress).flatMap { expiredGlobalTokenLocks =>
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
          val removedMids: Set[Address] = entries.collect { case (mid, balances) if balances.isEmpty => mid }.toSet
          TokenLockBalanceDeltas(deltas = deltasMap, removedKeys = removedMids)
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

        // Compute removed keys: metagraph addresses that had balances but now have empty or missing maps
        val removedKeys: Set[Address] = lastTokenLockBalances.collect {
          case (metagraphId, balances) if balances.nonEmpty && !cleanedFullState.get(metagraphId).exists(_.nonEmpty) =>
            metagraphId
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

      private def readBalance(
        address: Address,
        deltas: SortedMap[Address, Balance]
      )(implicit hasher: Hasher[F]): F[Balance] =
        deltas.get(address) match {
          case Some(b) => b.pure[F]
          case None    => mptStore.getBalance(address).map(_.getOrElse(Balance.empty))
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
          mptStore.getActiveTokenLocks(addr).map(_.fold(List.empty[Signed[TokenLock]])(_.toList))
        }.flatMap { locks =>
          locks.traverse(lock => lock.toHashed.map(h => h.hash -> lock)).map(_.toMap)
        }
    }
}
