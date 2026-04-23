package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendTransaction
import io.constellationnetwork.schema.balance.{Amount, Balance, BalanceArithmeticError}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.allowSpendExpiryKeySetImmutableCodec
import io.constellationnetwork.syntax.sortedCollection.sortedSetSyntax

/** Result of allow spend acceptance containing full state, deltas, and removed keys */
case class AllowSpendAcceptanceResult(
  fullState: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
  deltas: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
  removedKeys: Set[(Option[Address], Address)] = Set.empty,
  /** Expiry-index delta: adds for records that just entered the active set, removes for records that left it (consumed by a spend
    * transaction this ordinal, or whose `lastValidEpochProgress` is now in the past).
    */
  expiryIndexDelta: SystemIndexDelta[AllowSpendExpiryKey] = SystemIndexDelta.empty[AllowSpendExpiryKey]
)

trait AllowSpendStateManager[F[_]] {
  def acceptAllowSpends(
    epochProgress: EpochProgress,
    previousEpochProgress: EpochProgress,
    activeAllowSpendsFromCurrencySnapshots: SortedMap[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    allAcceptedSpendTxns: List[SpendTransaction]
  )(implicit hasher: Hasher[F]): F[AllowSpendAcceptanceResult]

  def acceptAllowSpendRefs(
    lastAllowSpendRefs: SortedMap[Address, AllowSpendReference],
    lastAllowSpendContextUpdate: Map[Address, AllowSpendReference]
  ): SortedMap[Address, AllowSpendReference]

  def filterExpiredAllowSpends(
    allowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    epochProgress: EpochProgress
  ): SortedMap[Address, SortedSet[Signed[AllowSpend]]]

  /** Index-driven equivalent of `filterExpiredAllowSpends` for the global partition (metagraphId = None).
    *
    * Sweeps the allow-spend expiry index for epoch buckets `[previousEpochProgress .. epochProgress - 1]` — the range covering records that
    * became expired since the previous accept. Resolves each expiring key's hash against the passed-in in-memory map
    * (`lastActiveGlobalAllowSpends`) to reconstruct the `Signed[AllowSpend]` value. Under the phase-2a invariant (index maintained on every
    * accept), the output set of `(address, hash)` pairs is equivalent to `filterExpiredAllowSpends(lastActiveGlobalAllowSpends,
    * epochProgress)` with empty-set entries elided.
    *
    * Primary benefit is architectural: once the in-memory `lastActiveGlobalAllowSpends` is no longer materialized (task #91), the index is
    * the only way to find expiring records in sub-O(N) work. Keep both paths until the flip proves itself end-to-end; equivalence is
    * verified by `AllowSpendExpirySweepEquivalenceSuite`.
    */
  def findExpiredGlobalAllowSpendsViaIndex(
    previousEpochProgress: EpochProgress,
    epochProgress: EpochProgress,
    lastActiveGlobalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]]
  )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[Signed[AllowSpend]]]]

  def updateGlobalBalancesByAllowSpends(
    epochProgress: EpochProgress,
    previousEpochProgress: EpochProgress,
    currentBalances: SortedMap[Address, Balance],
    globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
  )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]]
}

object AllowSpendStateManager {

  def make[F[_]: Async](
    mptStore: Option[MptStore[F, GlobalStateKey]] = None,
    shouldUseMptStore: Boolean = false
  ): AllowSpendStateManager[F] = new AllowSpendStateManager[F] {

    def acceptAllowSpends(
      epochProgress: EpochProgress,
      previousEpochProgress: EpochProgress,
      activeAllowSpendsFromCurrencySnapshots: SortedMap[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
      lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      allAcceptedSpendTxns: List[SpendTransaction]
    )(implicit hasher: Hasher[F]): F[AllowSpendAcceptanceResult] = {
      val allAcceptedSpendTxnsAllowSpendsRefs =
        allAcceptedSpendTxns
          .flatMap(_.allowSpendRef)

      val lastActiveGlobalAllowSpends = lastActiveAllowSpends.getOrElse(None, SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])

      // Phase 2b: route the expiry discovery through the index when shouldUseMptStore=true.
      // Semantically identical to filterExpiredAllowSpends (proven by AllowSpendExpirySweepEquivalenceSuite)
      // but touches only addresses with expiring records rather than every address in lastActive.
      val expiredGlobalAllowSpendsF: F[SortedMap[Address, SortedSet[Signed[AllowSpend]]]] =
        if (shouldUseMptStore && mptStore.isDefined)
          findExpiredGlobalAllowSpendsViaIndex(previousEpochProgress, epochProgress, lastActiveGlobalAllowSpends)
        else
          filterExpiredAllowSpends(lastActiveGlobalAllowSpends, epochProgress).pure[F]

      expiredGlobalAllowSpendsF.flatMap { expiredGlobalAllowSpends =>
        val unexpiredGlobalAllowSpends = (globalAllowSpends |+| expiredGlobalAllowSpends).foldLeft(lastActiveGlobalAllowSpends) {
          case (acc, (address, allowSpends)) =>
            val lastAddressAllowSpends = acc.getOrElse(address, SortedSet.empty[Signed[AllowSpend]])
            val unexpired = (lastAddressAllowSpends ++ allowSpends).filter(_.lastValidEpochProgress >= epochProgress)
            acc + (address -> unexpired)
        }

        val unexpiredGlobalWithoutSpendTransactionsF =
          unexpiredGlobalAllowSpends.toList.foldLeftM(unexpiredGlobalAllowSpends) {
            case (acc, (address, allowSpends)) =>
              allowSpends.toList.traverse(_.toHashed).map { hashedAllowSpends =>
                val validAllowSpends = hashedAllowSpends
                  .filterNot(h => allAcceptedSpendTxnsAllowSpendsRefs.contains(h.hash))
                  .map(_.signed)
                  .to(SortedSet)

                acc + (address -> validAllowSpends)
              }
          }

        def processMetagraphAllowSpends(
          metagraphId: Address,
          metagraphAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
          accAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
          accDeltas: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
        ): F[
          (
            SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
            SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
          )
        ] = {
          val lastActiveMetagraphAllowSpends =
            accAllowSpends.getOrElse(metagraphId.some, SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])

          metagraphAllowSpends.toList.traverse {
            case (address, addressAllowSpends) =>
              val lastAddressAllowSpends = lastActiveMetagraphAllowSpends.getOrElse(address, SortedSet.empty[Signed[AllowSpend]])

              val unexpired = (lastAddressAllowSpends ++ addressAllowSpends)
                .filter(_.lastValidEpochProgress >= epochProgress)

              val unexpiredWithoutSpendTransactions = unexpired.toList
                .traverse(_.toHashed)
                .map { hashedAllowSpends =>
                  hashedAllowSpends.filterNot(h => allAcceptedSpendTxnsAllowSpendsRefs.contains(h.hash))
                }
                .map(_.map(_.signed).toSortedSet)

              unexpiredWithoutSpendTransactions.map { validAllowSpends =>
                val hasChanged = lastAddressAllowSpends != validAllowSpends
                (address, validAllowSpends, hasChanged)
              }
          }.map { updatedMetagraphAllowSpends =>
            val fullStateMap = SortedMap(updatedMetagraphAllowSpends.map { case (addr, spends, _) => addr -> spends }: _*)
            // Filter out empty sets - those are removals tracked separately in removedKeys
            val deltasMap = SortedMap(updatedMetagraphAllowSpends.collect {
              case (addr, spends, true) if spends.nonEmpty => addr -> spends
            }: _*)

            val updatedFullState = accAllowSpends + (metagraphId.some -> fullStateMap)
            val updatedDeltas = if (deltasMap.nonEmpty) {
              accDeltas + (metagraphId.some -> deltasMap)
            } else {
              accDeltas
            }

            (updatedFullState, updatedDeltas)
          }
        }

        // Process metagraph allow spends and track deltas
        val processedMetagraphsF = activeAllowSpendsFromCurrencySnapshots.toList
          .foldLeft((lastActiveAllowSpends, SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]).pure[F]) {
            case (accF, (metagraphId, metagraphAllowSpends)) =>
              for {
                (accFullState, accDeltas) <- accF
                (updatedFullState, updatedDeltas) <- processMetagraphAllowSpends(metagraphId, metagraphAllowSpends, accFullState, accDeltas)
              } yield (updatedFullState, updatedDeltas)
          }

        for {
          (updatedCurrencyAllowSpends, currencyDeltas) <- processedMetagraphsF
          validGlobalAllowSpends <- unexpiredGlobalWithoutSpendTransactionsF

          globalDeltas: SortedMap[Address, SortedSet[Signed[AllowSpend]]] =
            validGlobalAllowSpends.filter {
              case (address, allowSpends) =>
                allowSpends.nonEmpty && !lastActiveGlobalAllowSpends.get(address).contains(allowSpends)
            }

          fullState =
            if (validGlobalAllowSpends.nonEmpty) updatedCurrencyAllowSpends + (None -> validGlobalAllowSpends)
            else updatedCurrencyAllowSpends

          deltas = if (globalDeltas.nonEmpty) currencyDeltas + (None -> globalDeltas) else currencyDeltas

          removedKeys: Set[(Option[Address], Address)] = lastActiveAllowSpends.flatMap {
            case (metagraphIdOpt, innerMap) =>
              innerMap.collect {
                case (address, spends)
                    if spends.nonEmpty &&
                      !fullState.get(metagraphIdOpt).flatMap(_.get(address)).exists(_.nonEmpty) =>
                  (metagraphIdOpt, address)
              }
          }.toSet

          expiryIndexDelta <- computeExpiryIndexDelta(lastActiveAllowSpends, fullState)
        } yield AllowSpendAcceptanceResult(fullState, deltas, removedKeys, expiryIndexDelta)
      }
    }

    /** Compute the `SystemIndexDelta[AllowSpendExpiryKey]` by diffing the per-`(mid, addr)` sets of `Signed[AllowSpend]` in `before` vs
      * `after`. Added records contribute to `adds` at their `lastValidEpochProgress`; removed records contribute to `removes` at the same
      * epoch. Hashing is needed for the key; runs in parallel across (mid, addr) pairs.
      */
    private def computeExpiryIndexDelta(
      before: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      after: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
    )(implicit hasher: Hasher[F]): F[SystemIndexDelta[AllowSpendExpiryKey]] = {
      val pairKeys: Set[(Option[Address], Address)] =
        before.flatMap { case (m, inner) => inner.keys.map(a => (m, a)) }.toSet ++
          after.flatMap { case (m, inner) => inner.keys.map(a => (m, a)) }.toSet

      def hashEntries(
        mid: Option[Address],
        addr: Address,
        allowSpends: SortedSet[Signed[AllowSpend]]
      ): F[List[(EpochProgress, AllowSpendExpiryKey)]] =
        allowSpends.toList.traverse { s =>
          s.toHashed.map(h => (s.lastValidEpochProgress, AllowSpendExpiryKey(mid, addr, h.hash)))
        }

      type Entries = List[(EpochProgress, AllowSpendExpiryKey)]
      val empty: (Entries, Entries) = (List.empty, List.empty)

      pairKeys.toList
        .foldLeftM[F, (Entries, Entries)](empty) {
          case ((accAdds, accRemoves), (mid, addr)) =>
            val oldInner = before.getOrElse(mid, SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])
            val newInner = after.getOrElse(mid, SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])
            val oldSet = oldInner.getOrElse(addr, SortedSet.empty[Signed[AllowSpend]])
            val newSet = newInner.getOrElse(addr, SortedSet.empty[Signed[AllowSpend]])
            val added = newSet.diff(oldSet)
            val removed = oldSet.diff(newSet)
            for {
              addedEntries <- hashEntries(mid, addr, added)
              removedEntries <- hashEntries(mid, addr, removed)
            } yield (accAdds ++ addedEntries, accRemoves ++ removedEntries)
        }
        .map {
          case (adds, removes) =>
            val addsMap: SortedMap[EpochProgress, Set[AllowSpendExpiryKey]] =
              adds.groupMap(_._1)(_._2).view.mapValues(_.toSet).to(SortedMap)
            val removesMap: SortedMap[EpochProgress, Set[AllowSpendExpiryKey]] =
              removes.groupMap(_._1)(_._2).view.mapValues(_.toSet).to(SortedMap)
            SystemIndexDelta.EpochBucket[AllowSpendExpiryKey](addsMap, removesMap)
        }
    }

    def acceptAllowSpendRefs(
      lastAllowSpendRefs: SortedMap[Address, AllowSpendReference],
      lastAllowSpendContextUpdate: Map[Address, AllowSpendReference]
    ): SortedMap[Address, AllowSpendReference] =
      lastAllowSpendRefs ++ lastAllowSpendContextUpdate

    def filterExpiredAllowSpends(
      allowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
      epochProgress: EpochProgress
    ): SortedMap[Address, SortedSet[Signed[AllowSpend]]] =
      allowSpends.view.mapValues(_.filter(_.lastValidEpochProgress < epochProgress)).to(SortedMap)

    def findExpiredGlobalAllowSpendsViaIndex(
      previousEpochProgress: EpochProgress,
      epochProgress: EpochProgress,
      lastActiveGlobalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]]
    )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[Signed[AllowSpend]]]] = mptStore match {
      case None =>
        // No MPT available; fall back to the legacy full-scan filter, dropping empty-set entries
        // to match the index-path output shape.
        filterExpiredAllowSpends(lastActiveGlobalAllowSpends, epochProgress).filter(_._2.nonEmpty).pure[F]
      case Some(store) =>
        if (previousEpochProgress.value.value >= epochProgress.value.value)
          SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]].pure[F]
        else {
          val fromL = previousEpochProgress.value.value
          val toL = epochProgress.value.value - 1L
          val epochs: List[EpochProgress] =
            (fromL to toL).toList.map(v => EpochProgress(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(v)))

          for {
            buckets <- epochs.traverse { e =>
              store
                .getExpiryBucket[AllowSpendExpiryKey](SystemNamespaceLabel.ExpiryIndexAllowSpends, e)
                .map(_.getOrElse(SortedSet.empty[AllowSpendExpiryKey]))
            }
            // Flatten + filter to global-only (metagraphId = None)
            allKeys = buckets.flatten.toSet.filter(_.metagraphId.isEmpty)
            byAddress = allKeys.groupBy(_.address)
            resolved <- byAddress.toList.traverse {
              case (addr, expiryKeys) =>
                val addrSet = lastActiveGlobalAllowSpends.getOrElse(addr, SortedSet.empty[Signed[AllowSpend]])
                val expectedHashes = expiryKeys.map(_.hash)
                addrSet.toList.traverse(s => s.toHashed.map(h => (h.hash, s))).map { hashed =>
                  val matched = hashed.collect { case (h, s) if expectedHashes.contains(h) => s }.to(SortedSet)
                  addr -> matched
                }
            }
          } yield SortedMap.from(resolved).filter(_._2.nonEmpty)
        }
    }

    def updateGlobalBalancesByAllowSpends(
      epochProgress: EpochProgress,
      previousEpochProgress: EpochProgress,
      currentBalances: SortedMap[Address, Balance],
      globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
      lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
    )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]] = {
      val lastActiveGlobalAllowSpends = lastActiveAllowSpends.getOrElse(None, SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])

      // Same dual-path as acceptAllowSpends: index sweep under the flag, legacy filter otherwise.
      val expiredGlobalAllowSpendsF: F[SortedMap[Address, SortedSet[Signed[AllowSpend]]]] =
        if (shouldUseMptStore && mptStore.isDefined)
          findExpiredGlobalAllowSpendsViaIndex(previousEpochProgress, epochProgress, lastActiveGlobalAllowSpends)
        else
          filterExpiredAllowSpends(lastActiveGlobalAllowSpends, epochProgress).pure[F]

      expiredGlobalAllowSpendsF.flatMap { expiredGlobalAllowSpends =>
        (globalAllowSpends |+| expiredGlobalAllowSpends).toList
          .foldM[F, Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]](
            Right((currentBalances, SortedMap.empty[Address, Balance]))
          ) {
            case (Left(err), _) =>
              (Left(err): Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]).pure[F]
            case (Right((balances, balancesDelta)), (address, allowSpends)) =>
              readBalance(address, balances).map { initialBalance =>
                val unexpiredBalance: Either[BalanceArithmeticError, Balance] = {
                  val unexpired = allowSpends.filter(_.lastValidEpochProgress >= epochProgress)
                  unexpired.foldLeft[Either[BalanceArithmeticError, Balance]](Right(initialBalance)) { (currentBalanceEither, allowSpend) =>
                    for {
                      currentBalance <- currentBalanceEither
                      balanceAfterAmount <- currentBalance.minus(SwapAmount.toAmount(allowSpend.amount))
                      balanceAfterFee <- balanceAfterAmount.minus(AllowSpendFee.toAmount(allowSpend.fee))
                    } yield balanceAfterFee
                  }
                }

                for {
                  unexpired <- unexpiredBalance
                  expired <- {
                    val expiredSet = allowSpends.filter(_.lastValidEpochProgress < epochProgress)
                    expiredSet.foldLeft[Either[BalanceArithmeticError, Balance]](Right(unexpired)) { (currentBalanceEither, allowSpend) =>
                      for {
                        currentBalance <- currentBalanceEither
                        balanceAfterExpiredAmount <- currentBalance.plus(SwapAmount.toAmount(allowSpend.amount))
                      } yield balanceAfterExpiredAmount
                    }
                  }
                } yield
                  (
                    balances.updated(address, expired),
                    balancesDelta.updated(address, expired)
                  )
              }
          }
      }
    }

    private def readBalance(
      address: Address,
      deltas: SortedMap[Address, Balance]
    )(implicit hasher: Hasher[F]): F[Balance] =
      deltas.get(address) match {
        case Some(b) => b.pure[F]
        case None =>
          if (shouldUseMptStore) mptStore.fold(Balance.empty.pure[F])(_.getBalance(address).map(_.getOrElse(Balance.empty)))
          else Balance.empty.pure[F]
      }
  }
}
