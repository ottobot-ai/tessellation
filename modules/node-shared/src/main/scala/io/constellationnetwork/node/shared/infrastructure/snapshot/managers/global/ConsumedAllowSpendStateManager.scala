package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SpendAction, SpendTransaction}
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.ConsumedAllowSpendCodec.{immutableCodec => consumedAllowSpendImmutableCodec}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedAllowSpendSetCodec

/** A cross-shard consume candidate enumerated from the gl0 fold's `acceptedSpendActions`. Carries the spend's `allowSpendRef` (= the content
  * `Hash` of the consumed allow-spend — the single-use I-ONCE identity), the OWNER metagraph `M` (`currencyId`, the metagraph whose ledger
  * holds the reserved funds), the PRODUCING metagraph `M′` (the `acceptedSpendActions` key — the shard that processed the spend), plus the
  * `source`/`destination`/`amount` the settlement moves. A consume is cross-shard iff `shardIdFor(M) ≠ shardIdFor(M′)`.
  */
final case class CrossShardConsume(
  allowSpendHash: Hash,
  currencyId: Option[CurrencyId],
  producingMetagraph: Address,
  source: Address,
  destination: Address,
  amount: SwapAmount
)

/** Result of the W3d nullifier pass over the cross-shard consume candidates: the new spent-set markers to write this ordinal (one per
  * ACCEPTED cross-shard consume), and the set of allow-spend hashes that were REJECTED (failed the include or absence check — double-consume
  * / no-such-reservation). The markers are keyed by `allowSpendHash` (the consumedAllowSpends key identity).
  */
final case class CrossShardSettlementResult(
  newMarkers: SortedMap[Hash, ConsumedAllowSpend],
  rejected: SortedSet[Hash]
)

/** gl0-side ATOMIC cross-shard allow-spend settlement ("I-ONCE", Option 2: read-side effective-balance overlay).
  *
  * An allow-spend `AS` on currency `M` reserves its source's funds at creation (`M` debits source `−amount −fee` in M's own currency ledger)
  * and `M` autonomously REFUNDS `+amount` on expiry (fee permanently taken). gl0 ADOPTS `M`'s authoritative pushed balances into the per-MG
  * `MgBalances` mirror — it does NOT re-derive them (the data-with-fee fix) and MUST NOT mutate them (the committee-attested per-MG root /
  * PIN-1 adopt-verify would fail). When a CROSS-shard spend (processed in metagraph `M′` on a different shard) consumes `AS`, `M` never
  * witnesses the consume and refunds anyway → the source's ATTESTED `MgBalances` value gets a PHANTOM `+amount` it could double-spend
  * (INFLATION). This manager prevents that with two uncoupled pieces:
  *
  *   1. ['''W3d''' — nullifier / I-ONCE] recording each accepted cross-shard consume in the global `ConsumedAllowSpends` spent-set (fieldId
  *      33, consensus-load-bearing, in `consensusRootEntries`), and REJECTING a consume whose `hash(AS)` is already present (double-consume /
  *      cross-snapshot replay) or whose `AS` is absent from `M`'s active-allow-spend mirror (no such reservation). This is the WRITE side,
  *      driven through the generic [[CrossShardMessageHandler]] seam by [[AllowSpendConsumeHandler]].
  *   2. ['''W3e''' — read-side EFFECTIVE-balance overlay, [[effectiveCurrencyBalances]]] at every gl0 site that reads `M`'s currency balances
  *      for ECONOMIC value (the `SpendActionValidator`'s same-shard balance check + the `GL0CurrencyBalanceRoutes` serving route), the
  *      ATTESTED balances are overlaid with the already-committed spent-set: credit each consume's destination `+amount`, and once the
  *      allow-spend's expiry passes (so `M` has refunded) re-subtract `−amount` from the source (SATURATING). So a no-`allowSpendRef`
  *      self-spend of the phantom-refunded amount is REJECTED for insufficient balance. The attested `MgBalances` partition is NEVER
  *      written — the correction is a pure read-time view derived from committed consensus state. NO inflation.
  *
  * '''Scalability (v1 cost).''' The overlay reads the spent-set (`materializeConsumedAllowSpendsFromMpt`) and folds it per source/destination
  * for the queried scope — an O(|spent-set|) prefix scan + fold. The spent-set holds ONLY cross-shard consumes (it is EMPTY at
  * `numShards = 1`), so it is tiny in practice and the effective view is DERIVED (rebuildable from the committed spent-set), NOT stored and
  * NOT in any consensus root.
  *
  * '''Determinism.''' Every iteration is over `SortedMap`/`SortedSet`; same-snapshot collisions (two shards consuming one `AS`) are resolved
  * by processing candidates in `allowSpendHash`-sorted order — the first passes the absence check, settles + marks; the rest fail absence →
  * rejected. All hashing routes through `Hasher[F]` (via `ShardAssignment.shardIdFor`). No clock / env. Every honest node computes
  * byte-identical post-state.
  *
  * '''`numShards = 1` byte-identity.''' At `numShards = 1` every metagraph maps to shard 0, so `shardIdFor(M) == shardIdFor(M′)` for every
  * consume ⇒ [[classifyCrossShardConsumes]] returns `Nil` ⇒ no marker is ever written, the spent-set stays empty, and
  * [[effectiveCurrencyBalances]] returns the attested map unchanged (the identity) ⇒ the validator input + the served balance + the `mptRoot`
  * are byte-identical to the pre-change path. The GSAM caller additionally gates the spent-set read behind `numShards > 1`.
  */
trait ConsumedAllowSpendStateManager[F[_]] {

  /** Materialize the full `hash(AS) → ConsumedAllowSpend` spent-set by prefix-scanning the MPT under
    * `(HypergraphNamespace, fieldId=ConsumedAllowSpends)`. The map key is recovered from the value's `allowSpendHash` (the
    * value-carries-key pattern — `consumedAllowSpendKey` hashes the hash into the user slot, lossy like every hashed key).
    */
  def materializeConsumedAllowSpendsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Hash, ConsumedAllowSpend]]

  /** Materialize the full `Option[contract] → source → active-allow-spend-set` mirror (the `ActiveAllowSpends` partition, fieldId 7) — the
    * include-check input for [[settleCrossShardConsumes]]. Identical to `AllowSpendStateManager.materializeActiveAllowSpendsFromMpt`; exposed
    * here so the cross-shard handler is self-contained over a single reader.
    */
  def materializeActiveAllowSpendsFromMpt(
    implicit hasher: Hasher[F]
  ): F[SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]]

  /** Enumerate the CROSS-SHARD consume candidates from this ordinal's `acceptedSpendActions` (keyed by the PRODUCING metagraph `M′`). Each
    * spend transaction carrying an `allowSpendRef` is a consume; it is CROSS-SHARD iff `shardIdFor(M) ≠ shardIdFor(M′)` where `M` =
    * `spend.currencyId` (the allow-spend's currency / owner metagraph) and `M′` = the producing key. Same-shard consumes (including ALL
    * consumes at `numShards = 1`) are dropped here and left to the existing local path. Output is sorted by `allowSpendHash` for a
    * deterministic same-snapshot collision order.
    */
  def classifyCrossShardConsumes(
    acceptedSpendActions: SortedMap[Address, List[SpendAction]],
    shardAssignment: ShardAssignment[F]
  )(implicit hasher: Hasher[F]): F[List[CrossShardConsume]]

  /** W3d nullifier pass. For each cross-shard consume (processed in `allowSpendHash`-sorted order):
    *   - REJECT if `hash(AS)` is already in `existingSpentSet` (double-consume / cross-snapshot replay) OR already accepted earlier this
    *     ordinal (same-snapshot collision — the sorted-first winner already marked it).
    *   - REJECT if `AS` is absent from `M`'s active-allow-spend mirror (`lastActiveAllowSpends` under the `M` scope, matched by
    *     `hash(AS)`) — no such reservation.
    *   - Otherwise ACCEPT: emit a `ConsumedAllowSpend` marker carrying everything the overlay needs self-containedly.
    */
  def settleCrossShardConsumes(
    candidates: List[CrossShardConsume],
    existingSpentSet: SortedMap[Hash, ConsumedAllowSpend],
    lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    consumedAtOrdinal: SnapshotOrdinal
  )(implicit hasher: Hasher[F]): F[CrossShardSettlementResult]

  /** W3e read-side EFFECTIVE-balance overlay for ONE metagraph-currency scope (`currencyId` = owner metagraph `M`, or `None` for the
    * DAG-global scope which is never cross-shard). Given `M`'s ATTESTED currency balances (the value gl0 mirrors + verifies against the
    * committee-attested per-MG root — NEVER mutated), returns the ECONOMICALLY-EFFECTIVE balances that reflect the already-committed
    * cross-shard spent-set:
    *   - CREDIT each marker's destination `+amount` for `markers whose currencyId == this scope` (the cross-shard consume moved `M`'s
    *     reserved funds to the destination — neither `M` nor `M′` re-pushes this credit, so it is overlaid at read time), and
    *   - once `epochFor(M) > marker.lastValidEpochProgress` (so `M` has autonomously refunded the source `+amount`), re-subtract `−amount`
    *     from the source — SATURATING (clamp to `Balance.empty` on underflow; NEVER raise — the F1 liveness fix). So the source's effective
    *     balance stays debited (`B−amount−fee`) and a replayed cross-shard consume sees insufficient funds.
    *
    * `epochFor(M)` = `metagraphPinnedEpochProgresses(M)` (the owner metagraph's pinned `globalSyncView.epochProgress`, consistent with the
    * R1 fix) else `liveEpochProgress`. Pure, total, DERIVED from the committed spent-set — it is NOT stored and NOT in any consensus root.
    * Empty spent-set (always at `numShards = 1`) ⇒ effective == attested ⇒ the validator/read sees byte-identical input.
    */
  def effectiveCurrencyBalances(
    attested: SortedMap[Address, Balance],
    currencyId: Option[Address],
    spentSet: SortedMap[Hash, ConsumedAllowSpend],
    metagraphPinnedEpochProgresses: Map[Address, EpochProgress],
    liveEpochProgress: EpochProgress
  ): SortedMap[Address, Balance]
}

object ConsumedAllowSpendStateManager {

  def make[F[_]: Async](
    reader: GlobalStateReader[F]
  ): ConsumedAllowSpendStateManager[F] = new ConsumedAllowSpendStateManager[F] {

    def materializeConsumedAllowSpendsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[Hash, ConsumedAllowSpend]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ConsumedAllowSpends)
        entries <- reader.getAllForPrefix[ConsumedAllowSpend](prefix)
      } yield entries.values.toList.foldLeft(SortedMap.empty[Hash, ConsumedAllowSpend]) {
        case (acc, value) => acc.updated(value.allowSpendHash, value)
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

    def classifyCrossShardConsumes(
      acceptedSpendActions: SortedMap[Address, List[SpendAction]],
      shardAssignment: ShardAssignment[F]
    )(implicit hasher: Hasher[F]): F[List[CrossShardConsume]] = {
      // Flatten while PRESERVING the producing-metagraph key M′ (do NOT collapse it — it is one half of the cross-shard test).
      val candidates: List[(Address, SpendTransaction)] =
        acceptedSpendActions.toList.flatMap {
          case (producingMg, spendActions) =>
            spendActions.flatMap(_.spendTransactions.toList).map(producingMg -> _)
        }

      candidates
        .traverseFilter {
          case (producingMg, txn) =>
            txn.allowSpendRef match {
              case None => Option.empty[CrossShardConsume].pure[F]
              case Some(asHash) =>
                // The allow-spend's currency = the OWNER metagraph M. A consume without a currencyId targets the DAG-global
                // allow-spend partition (no metagraph owner), which is never cross-shard — drop it (local path owns it).
                txn.currencyId match {
                  case None => Option.empty[CrossShardConsume].pure[F]
                  case Some(currencyId) =>
                    val ownerMetagraph = currencyId.value
                    (shardAssignment.shardIdFor(ownerMetagraph), shardAssignment.shardIdFor(producingMg)).mapN {
                      (ownerShard, producingShard) =>
                        if (ownerShard =!= producingShard)
                          CrossShardConsume(
                            allowSpendHash = asHash,
                            currencyId = currencyId.some,
                            producingMetagraph = producingMg,
                            source = txn.source,
                            destination = txn.destination,
                            amount = txn.amount
                          ).some
                        else
                          Option.empty[CrossShardConsume]
                    }
                }
            }
        }
        // Deterministic same-snapshot collision order: sort by the single-use identity (allowSpendHash), then by producing MG
        // for the rare case of two distinct candidates sharing a hash via different producers (still resolved first-wins).
        .map(_.sortBy(c => (c.allowSpendHash.value, c.producingMetagraph.value.value)))
    }

    def settleCrossShardConsumes(
      candidates: List[CrossShardConsume],
      existingSpentSet: SortedMap[Hash, ConsumedAllowSpend],
      lastActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      consumedAtOrdinal: SnapshotOrdinal
    )(implicit hasher: Hasher[F]): F[CrossShardSettlementResult] = {
      // Resolve, per candidate, whether its allow-spend is present in M's active-allow-spend mirror (matched by hash(AS)) AND fetch the
      // authoritative AS fields (lastValidEpochProgress) from that mirror. The mirror is keyed by Option[metagraph] → source → set; for an
      // owner metagraph M the scope key is Some(M). We hash each candidate-scope's active set ONCE and look the hash up.
      def activeSetFor(currencyId: Option[CurrencyId]): SortedMap[Address, SortedSet[Signed[AllowSpend]]] =
        lastActiveAllowSpends.getOrElse(currencyId.map(_.value), SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])

      // Build a per-scope hash → Signed[AllowSpend] index lazily/once per distinct scope encountered, so repeated candidates in the same
      // scope don't re-hash the whole set.
      val distinctScopes: List[Option[CurrencyId]] = candidates.map(_.currencyId).distinct

      distinctScopes
        .traverse { scope =>
          activeSetFor(scope).values.toList.flatten
            .traverse(s => s.toHashed.map(h => h.hash -> s))
            .map(pairs => scope -> pairs.toMap)
        }
        .map(_.toMap)
        .map { scopeIndex =>
          // Single deterministic left-fold over the sorted candidates: accumulate accepted markers + rejected hashes; an in-fold
          // "seen" set lets the sorted-first consumer of a colliding hash win and the rest fail absence.
          val seenSpent: Set[Hash] = existingSpentSet.keySet
          candidates.foldLeft(CrossShardSettlementResult(SortedMap.empty[Hash, ConsumedAllowSpend], SortedSet.empty[Hash])) {
            case (acc, c) =>
              val alreadyConsumed = seenSpent.contains(c.allowSpendHash) || acc.newMarkers.contains(c.allowSpendHash)
              val includedAs: Option[Signed[AllowSpend]] = scopeIndex.getOrElse(c.currencyId, Map.empty).get(c.allowSpendHash)
              (alreadyConsumed, includedAs) match {
                case (false, Some(as)) =>
                  val marker = ConsumedAllowSpend(
                    allowSpendHash = c.allowSpendHash,
                    source = c.source,
                    destination = c.destination,
                    currencyId = c.currencyId,
                    amount = c.amount,
                    lastValidEpochProgress = as.value.lastValidEpochProgress,
                    consumedAtOrdinal = consumedAtOrdinal,
                    consumingSpendRef = c.allowSpendHash
                  )
                  acc.copy(newMarkers = acc.newMarkers.updated(c.allowSpendHash, marker))
                case _ =>
                  // Double-consume (present in spent-set or already marked this ordinal) OR no-such-reservation (absent from M's mirror).
                  acc.copy(rejected = acc.rejected + c.allowSpendHash)
              }
          }
        }
    }

    def effectiveCurrencyBalances(
      attested: SortedMap[Address, Balance],
      currencyId: Option[Address],
      spentSet: SortedMap[Hash, ConsumedAllowSpend],
      metagraphPinnedEpochProgresses: Map[Address, EpochProgress],
      liveEpochProgress: EpochProgress
    ): SortedMap[Address, Balance] = {
      val epochFor: EpochProgress =
        currencyId.flatMap(metagraphPinnedEpochProgresses.get).getOrElse(liveEpochProgress)

      // Only this currency-scope's markers contribute. Aggregate per-destination credit and per-source expired-debit deterministically over
      // the sorted spent-set. `Amount.plus` is associative+commutative, so the (extremely unlikely) overflow saturates to the max amount
      // rather than raising — the effective balance is a read-side advisory derived from committed state, never an accept-path failure.
      type AmtMap = SortedMap[Address, Amount]
      val empty: AmtMap = SortedMap.empty[Address, Amount]

      def addAmountSat(m: AmtMap, addr: Address, amt: Amount): AmtMap = {
        val current = m.getOrElse(addr, Amount.empty)
        val next = current.plus(amt).getOrElse(Amount(eu.timepit.refined.types.numeric.NonNegLong.MaxValue))
        m.updated(addr, next)
      }

      val (credits, debits): (AmtMap, AmtMap) =
        spentSet.toList.foldLeft((empty, empty)) {
          case (acc @ (cr, db), (_, marker)) =>
            if (marker.currencyId.map(_.value) =!= currencyId) acc
            else {
              val markerAmount: Amount = SwapAmount.toAmount(marker.amount)
              val crNext = addAmountSat(cr, marker.destination, markerAmount)
              // Re-subtract the refund ONLY once the owner metagraph's pinned epoch has passed the allow-spend's expiry
              // (M has refunded the source +amount; the overlay cancels it so the source stays permanently debited).
              val dbNext =
                if (epochFor.value.value > marker.lastValidEpochProgress.value.value)
                  addAmountSat(db, marker.source, markerAmount)
                else db
              (crNext, dbNext)
            }
        }

      if (credits.isEmpty && debits.isEmpty) attested
      else {
        val affected: SortedSet[Address] = (credits.keySet ++ debits.keySet).to(SortedSet)
        affected.foldLeft(attested) { (acc, addr) =>
          val base: Balance = acc.getOrElse(addr, Balance.empty)
          val credit: Amount = credits.getOrElse(addr, Amount.empty)
          val debit: Amount = debits.getOrElse(addr, Amount.empty)
          // SATURATING (F1): credit can overflow → clamp to max Balance; debit can underflow → clamp to Balance.empty. NEVER raise.
          val credited: Balance = base.plus(credit).getOrElse(Balance(eu.timepit.refined.types.numeric.NonNegLong.MaxValue))
          val debited: Balance = credited.minus(debit).getOrElse(Balance.empty)
          acc.updated(addr, debited)
        }
      }
    }
  }
}
