package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendTransaction
import io.constellationnetwork.schema.balance.{Amount, Balance, BalanceArithmeticError}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendReferenceImmutableCodec}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.{
  addressSetImmutableCodec,
  allowSpendExpiryKeySetImmutableCodec,
  signedAllowSpendSetCodec
}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.syntax.sortedCollection.sortedSetSyntax

/** Result of allow spend acceptance containing full state, deltas, and removed keys */
case class AllowSpendAcceptanceResult(
  fullState: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
  deltas: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
  removedKeys: Set[(Option[Address], Address)] = Set.empty,
  // Expiry-index delta: adds for records that just entered the active set, removes for records that left it
  // (consumed by a spend transaction this ordinal, or whose `lastValidEpochProgress` is now in the past).
  expiryIndexDelta: SystemIndexDelta[AllowSpendExpiryKey] = SystemIndexDelta.empty[AllowSpendExpiryKey]
)

trait AllowSpendStateManager[F[_]] {
  def acceptAllowSpends(
    epochProgress: EpochProgress,
    previousEpochProgress: EpochProgress,
    activeAllowSpendsFromCurrencySnapshots: SortedMap[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    allAcceptedSpendTxns: List[SpendTransaction],
    metagraphPinnedEpochProgresses: Map[Address, EpochProgress]
  )(implicit hasher: Hasher[F]): F[AllowSpendAcceptanceResult]

  /** Hoisted variant of `acceptAllowSpends` that takes a pre-computed `expiredGlobalAllowSpends` set, avoiding the redundant
    * `findExpiredGlobalAllowSpendsViaIndexFromMpt` call inside the manager. Used by GSAM's local-events hoist (Q2 user override): the
    * expired set is computed once per accept(), threaded into both this and `updateGlobalBalancesByAllowSpendsWithExpired`, and used as the
    * source of EXPIRED-transition events.
    *
    * Behavioural equivalent of `acceptAllowSpends(...)` when the passed `expiredGlobalAllowSpends` matches
    * `findExpiredGlobalAllowSpendsViaIndexFromMpt(previousEpochProgress, epochProgress)`.
    *
    * `metagraphPinnedEpochProgresses` maps a metagraph address to the epoch the metagraph itself used when it expired its OWN allow-spends
    * — its pinned `globalSyncView.epochProgress` (the SAME value `CurrencySnapshotAcceptanceManager` reads as
    * `lastGlobalSnapshotEpochProgress \= lastSyncGlobalSnapshot.epochProgress`). The METAGRAPH-scoped expiry filter
    * (`processMetagraphAllowSpends`) uses this pinned epoch per `Some(mg)`, NOT the live global `epochProgress`, so a metagraph trailing
    * the global tip doesn't have its allow-spends over-pruned (the "m0 frozen" wedge). A metagraph absent from the map (genesis /
    * pre-`globalSyncView` snapshot) falls back to the live `epochProgress` — the prior behaviour. The DAG-global scope (`None`) is
    * unaffected and keeps the live global `epochProgress`.
    */
  def acceptAllowSpendsWithExpired(
    epochProgress: EpochProgress,
    activeAllowSpendsFromCurrencySnapshots: SortedMap[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    allAcceptedSpendTxns: List[SpendTransaction],
    expiredGlobalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    metagraphPinnedEpochProgresses: Map[Address, EpochProgress]
  )(implicit hasher: Hasher[F]): F[AllowSpendAcceptanceResult]

  def acceptAllowSpendRefs(
    lastAllowSpendRefs: SortedMap[Address, AllowSpendReference],
    lastAllowSpendContextUpdate: Map[Address, AllowSpendReference]
  ): SortedMap[Address, AllowSpendReference]

  def filterExpiredAllowSpends(
    allowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    epochProgress: EpochProgress
  ): SortedMap[Address, SortedSet[Signed[AllowSpend]]]

  /** Index-driven expiry sweep for the global partition (metagraphId = None).
    *
    * Sweeps the allow-spend expiry index for epoch buckets `[previousEpochProgress .. epochProgress - 1]` — the range covering records that
    * became expired since the previous accept. Resolves each expiring key's hash via `mptStore.getActiveAllowSpends(None, addr)` instead of
    * iterating an in-memory full map. Output is equivalent to `filterExpiredAllowSpends(globalActiveAllowSpends, epochProgress)` with
    * empty-set entries elided, under the invariant that the index is maintained on every accept.
    */
  def findExpiredGlobalAllowSpendsViaIndexFromMpt(
    previousEpochProgress: EpochProgress,
    epochProgress: EpochProgress
  )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[Signed[AllowSpend]]]]

  def updateGlobalBalancesByAllowSpends(
    epochProgress: EpochProgress,
    previousEpochProgress: EpochProgress,
    currentBalances: SortedMap[Address, Balance],
    globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
  )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]]

  /** Hoisted variant of `updateGlobalBalancesByAllowSpends` that takes a pre-computed expired set. See `acceptAllowSpendsWithExpired` for
    * the Q2 user-override hoist rationale.
    */
  def updateGlobalBalancesByAllowSpendsWithExpired(
    epochProgress: EpochProgress,
    currentBalances: SortedMap[Address, Balance],
    globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
    expiredGlobalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]]
  )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]]

  /** Materialize the full `Option[contract] → user → active-allow-spend-set` view by prefix-scanning the MPT under `(HypergraphNamespace,
    * fieldId=ActiveAllowSpends)` across all contract scopes. Replaces `lastSnapshotContext.activeAllowSpends` reads in the GSAM hot path so
    * the accept pipeline no longer depends on the inbound GSI carrying that field.
    *
    * Both dimensions of the outer map are recovered from the value: `head.value.currencyId.map(_.value)` for the contract scope (None =
    * DAG-global, Some(addr) = metagraph-scoped) and `head.value.source` for the user. Every member of a per-key SortedSet shares one
    * `(currencyId, source)` pair by construction, so taking the head is safe.
    */
  def materializeActiveAllowSpendsFromMpt(
    implicit hasher: Hasher[F]
  ): F[SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]]

  /** Materialize the full `address → AllowSpendReference` view via the `ActiveAddressIndex` sidecar. The reference value type doesn't carry
    * `source: Address`, so prefix-scan can't recover keys; instead we read the address set from the sidecar and batch point-read values.
    */
  def materializeLastAllowSpendRefsFromMpt(
    implicit hasher: Hasher[F]
  ): F[SortedMap[Address, AllowSpendReference]]
}

object AllowSpendStateManager {

  def make[F[_]: Async](
    reader: GlobalStateReader[F]
  ): AllowSpendStateManager[F] = new AllowSpendStateManager[F] {

    def acceptAllowSpends(
      epochProgress: EpochProgress,
      previousEpochProgress: EpochProgress,
      activeAllowSpendsFromCurrencySnapshots: SortedMap[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
      lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      allAcceptedSpendTxns: List[SpendTransaction],
      metagraphPinnedEpochProgresses: Map[Address, EpochProgress]
    )(implicit hasher: Hasher[F]): F[AllowSpendAcceptanceResult] =
      findExpiredGlobalAllowSpendsViaIndexFromMpt(previousEpochProgress, epochProgress).flatMap { expired =>
        acceptAllowSpendsWithExpired(
          epochProgress,
          activeAllowSpendsFromCurrencySnapshots,
          globalAllowSpends,
          lastActiveAllowSpends,
          allAcceptedSpendTxns,
          expired,
          metagraphPinnedEpochProgresses
        )
      }

    def acceptAllowSpendsWithExpired(
      epochProgress: EpochProgress,
      activeAllowSpendsFromCurrencySnapshots: SortedMap[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
      lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      allAcceptedSpendTxns: List[SpendTransaction],
      expiredGlobalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
      metagraphPinnedEpochProgresses: Map[Address, EpochProgress]
    )(implicit hasher: Hasher[F]): F[AllowSpendAcceptanceResult] = {
      val allAcceptedSpendTxnsAllowSpendsRefs =
        allAcceptedSpendTxns
          .flatMap(_.allowSpendRef)

      val lastActiveGlobalAllowSpends = lastActiveAllowSpends.getOrElse(None, SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])

      Async[F].pure(expiredGlobalAllowSpends).flatMap { expiredGlobalAllowSpends =>
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

          // Expire this metagraph's allow-spends against the epoch the METAGRAPH itself pinned (its
          // `globalSyncView.epochProgress`, == ml0's `lastGlobalSnapshotEpochProgress`), NOT gl0's live global epoch.
          // A metagraph trailing the global tip pins a LOWER epoch; using the live (higher) epoch over-prunes its
          // active allow-spends and wedges its currency fold. Absent (genesis / pre-`globalSyncView`) ⇒ live epoch.
          val metagraphEpochProgress = metagraphPinnedEpochProgresses.getOrElse(metagraphId, epochProgress)

          metagraphAllowSpends.toList.traverse {
            case (address, addressAllowSpends) =>
              val lastAddressAllowSpends = lastActiveMetagraphAllowSpends.getOrElse(address, SortedSet.empty[Signed[AllowSpend]])

              val unexpired = (lastAddressAllowSpends ++ addressAllowSpends)
                .filter(_.lastValidEpochProgress >= metagraphEpochProgress)

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
      // NB: `SortedMap.flatMap { ... map(a => (m, a)) }` would build a `Map` keyed by `m` (the outer key),
      // dropping all but one `(m, a)` per `m` due to key-collision overwrite. Iterate explicitly via
      // `.iterator` so the result is `Iterator[(Option[Address], Address)]`, preserving every `(mid, addr)` pair.
      val pairKeys: Set[(Option[Address], Address)] =
        (before.iterator.flatMap { case (m, inner) => inner.keysIterator.map(a => (m, a)) } ++
          after.iterator.flatMap { case (m, inner) => inner.keysIterator.map(a => (m, a)) }).toSet

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

    def findExpiredGlobalAllowSpendsViaIndexFromMpt(
      previousEpochProgress: EpochProgress,
      epochProgress: EpochProgress
    )(implicit hasher: Hasher[F]): F[SortedMap[Address, SortedSet[Signed[AllowSpend]]]] =
      if (previousEpochProgress.value.value >= epochProgress.value.value)
        SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]].pure[F]
      else {
        val fromL = previousEpochProgress.value.value
        val toL = epochProgress.value.value - 1L
        val epochs: List[EpochProgress] =
          (fromL to toL).toList.map(v => EpochProgress(eu.timepit.refined.types.numeric.NonNegLong.unsafeFrom(v)))

        for {
          buckets <- epochs.traverse { e =>
            GlobalStateKey
              .expiryIndexKey[F](SystemNamespaceLabel.ExpiryIndexAllowSpends, e)
              .flatMap(reader.get[SortedSet[AllowSpendExpiryKey]])
              .map(_.getOrElse(SortedSet.empty[AllowSpendExpiryKey]))
          }
          // Filter to global-only (metagraphId = None) and group by address.
          allKeys = buckets.flatten.toSet.filter(_.metagraphId.isEmpty)
          byAddress = allKeys.groupBy(_.address)
          resolved <- byAddress.toList.traverse {
            case (addr, expiryKeys) =>
              reader
                .get[SortedSet[Signed[AllowSpend]]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, None, addr))
                .flatMap { addrSetOpt =>
                  val addrSet = addrSetOpt.getOrElse(SortedSet.empty[Signed[AllowSpend]])
                  val expectedHashes = expiryKeys.map(_.hash)
                  addrSet.toList.traverse(s => s.toHashed.map(h => (h.hash, s))).map { hashed =>
                    val matched = hashed.collect { case (h, s) if expectedHashes.contains(h) => s }.to(SortedSet)
                    addr -> matched
                  }
                }
          }
        } yield SortedMap.from(resolved).filter(_._2.nonEmpty)
      }

    def updateGlobalBalancesByAllowSpends(
      epochProgress: EpochProgress,
      previousEpochProgress: EpochProgress,
      currentBalances: SortedMap[Address, Balance],
      globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
      lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
    )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]] = {
      val _ = lastActiveAllowSpends // unused; preserved for API compat
      findExpiredGlobalAllowSpendsViaIndexFromMpt(previousEpochProgress, epochProgress).flatMap { expired =>
        updateGlobalBalancesByAllowSpendsWithExpired(epochProgress, currentBalances, globalAllowSpends, expired)
      }
    }

    def updateGlobalBalancesByAllowSpendsWithExpired(
      epochProgress: EpochProgress,
      currentBalances: SortedMap[Address, Balance],
      globalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]],
      expiredGlobalAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]]
    )(implicit hasher: Hasher[F]): F[Either[BalanceArithmeticError, (SortedMap[Address, Balance], SortedMap[Address, Balance])]] =
      Async[F].pure(expiredGlobalAllowSpends).flatMap { expiredGlobalAllowSpends =>
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

    private def readBalance(
      address: Address,
      deltas: SortedMap[Address, Balance]
    )(implicit hasher: Hasher[F]): F[Balance] =
      deltas.get(address) match {
        case Some(b) => b.pure[F]
        case None =>
          reader.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, address)).map(_.getOrElse(Balance.empty))
      }

    def materializeActiveAllowSpendsFromMpt(
      implicit hasher: Hasher[F]
    ): F[SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveAllowSpends)
        entries <- reader.getAllForPrefix[SortedSet[Signed[AllowSpend]]](prefix)
      } yield
        entries.values.toList
          .mapFilter(set => set.headOption.map(h => (h.value.currencyId.map(_.value), h.value.source, set)))
          .filter(_._3.nonEmpty)
          .foldLeft(SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]) {
            case (acc, (contract, source, set)) =>
              val inner = acc.getOrElse(contract, SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])
              acc.updated(contract, inner.updated(source, set))
          }

    def materializeLastAllowSpendRefsFromMpt(
      implicit hasher: Hasher[F]
    ): F[SortedMap[Address, AllowSpendReference]] =
      for {
        indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastAllowSpendRefs)
        addrSet <- reader.get[SortedSet[Address]](indexKey).map(_.getOrElse(SortedSet.empty[Address]))
        addrList = addrSet.toList
        keys = addrList.map(addr => GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, addr))
        values <- reader.getMany[AllowSpendReference](keys)
      } yield
        SortedMap.from(addrList.flatMap { addr =>
          val key = GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, addr)
          values.get(key).map(addr -> _)
        })
  }
}
