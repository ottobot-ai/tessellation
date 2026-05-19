package io.constellationnetwork.node.shared.domain.nakamoto

import cats.kernel.Order
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.GlobalSnapshotInfo
import io.constellationnetwork.schema.peer.PeerId

import derevo.cats.{eqv, show}
import derevo.derive

/** Stake-distribution snapshot at an eta-period boundary, used by the §3 NIPoPoW N-2 lookback rule.
  *
  * '''Why raw amounts, not pre-normalized ratios.''' The denominator of `peerStake / totalStake` depends on which peer subset the caller
  * considers. The seedlist may change between snapshot time and read time (peer registration, slashing); the observed-active set is
  * runtime-only and is meaningless to a verifier replaying history. Storing raw `BigInt` amounts lets the caller pick its own denominator at
  * read time — the same way `StakeRegistry.stakeWeighted.totalStakeOver` does today against the live GSI.
  *
  * '''Why summed delegated + collateral.''' Matches the §1.1 stake definition used in production (`StakeRegistry.stakeOf`,
  * `StakeRegistry.scala:183`). Both tiers contribute to leader eligibility today, and any historical reconstruction must agree byte-for-byte
  * with the producer's view at that moment.
  *
  * '''Identity at zero stake.''' A peer that registered no delegation and no collateral simply isn't in the map. `stakeOf` returns
  * `BigInt(0)` for missing keys — that's the same null behavior the live registry has when an unstaked peer is queried.
  */
@derive(eqv, show)
final case class StakeDistribution(stakes: Map[PeerId, BigInt]) {

  /** Raw stake amount for a single peer. Returns 0 if the peer has no record. */
  def stakeOf(p: PeerId): BigInt = stakes.getOrElse(p, BigInt(0))

  /** Total stake summed across an arbitrary peer set. Peers missing from `stakes` contribute 0; off-set peers in `stakes` are ignored. This
    * is the canonical denominator for "what fraction of `peers` does `p` hold" queries.
    */
  def totalOver(peers: Set[PeerId]): BigInt =
    peers.foldLeft(BigInt(0))((acc, p) => acc + stakeOf(p))

  /** Relative stake `p` holds against a denominator peer set. Returns `Ratio.Zero` if the denominator is zero, `p` is not in the denominator
    * set, or `p` has no stake recorded — same fail-closed semantics as `StakeRegistry.relativeStake`.
    */
  def relativeStakeAgainst(p: PeerId, denominator: Set[PeerId]): Ratio =
    if (!denominator.contains(p)) Ratio.Zero
    else {
      val total = totalOver(denominator)
      if (total == BigInt(0)) Ratio.Zero
      else Ratio(stakeOf(p), total)
    }
}

object StakeDistribution {

  /** The empty distribution — no peer has any stake. Read-side fallback for periods earlier than the first recorded snapshot. */
  val Empty: StakeDistribution = StakeDistribution(Map.empty)
}

/** Eta-period index. Period N spans ordinals `[N · etaRotationSnapshots, (N+1) · etaRotationSnapshots)`. Computed via
  * `EtaCalculation.rotationPeriod(ordinal, etaRotationSnapshots)`.
  *
  * '''Why a Long, not a `NonNegLong`.''' Periods can go negative during eligibility queries (`currentEtaPeriod - 2` is negative for periods
  * 0 and 1). Lookups against a negative period fall through to the genesis stake distribution. Modelling that as Option-at-the-boundary
  * would force partial semantics through every call site for a transient bootstrap-only edge case.
  */
@derive(eqv, show)
final case class EtaPeriod(value: Long) {

  /** Period that is `n` boundaries earlier (may be negative; see class docs). */
  def minus(n: Long): EtaPeriod = EtaPeriod(value - n)

  /** Period that is `n` boundaries later. */
  def plus(n: Long): EtaPeriod = EtaPeriod(value + n)
}

object EtaPeriod {
  val Zero: EtaPeriod = EtaPeriod(0L)

  implicit val ordering: Order[EtaPeriod] = Order.by(_.value)
}

/** Pure computation that lifts a `GlobalSnapshotInfo`'s `activeDelegatedStakes + activeNodeCollaterals` into a `StakeDistribution`.
  *
  * Run at every eta-period boundary on the snapshot whose ordinal is the final ordinal of the closing period — the resulting
  * `StakeDistribution` is committed into `GlobalSnapshotInfo.historicalStakeSnapshots[periodN]` and becomes the basis for slot-leader
  * eligibility two periods later (Cardano-style mark/set/go).
  *
  * '''Determinism.''' Identical inputs produce identical output. No `F[_]`, no Refs, no implicit state. Verifier-side reconstruction reads
  * the same GSI and runs the same function to get the same `StakeDistribution` — load-bearing for NIPoPoW tower verification.
  */
object EpochStakeSnapshotter {

  /** Build a stake-distribution from a GSI. Sums delegated-stake + node-collateral amounts per `nodeId`. Iteration order is irrelevant
    * because `Map[PeerId, BigInt]` is keyed by content; addition is associative+commutative on `BigInt`.
    */
  def snapshot(info: GlobalSnapshotInfo): StakeDistribution = {
    val delegated: Map[PeerId, BigInt] =
      info.activeDelegatedStakes.toIterable.flatMap(_.valuesIterator).flatMap(_.iterator).foldLeft(Map.empty[PeerId, BigInt]) {
        (acc, record) =>
          val p = record.event.value.nodeId
          val amt = BigInt(record.amount.value.value)
          acc.updated(p, acc.getOrElse(p, BigInt(0)) + amt)
      }
    val collateral: Map[PeerId, BigInt] =
      info.activeNodeCollaterals.toIterable.flatMap(_.valuesIterator).flatMap(_.iterator).foldLeft(Map.empty[PeerId, BigInt]) {
        (acc, record) =>
          val p = record.event.value.nodeId
          val amt = BigInt(record.event.value.amount.value.value)
          acc.updated(p, acc.getOrElse(p, BigInt(0)) + amt)
      }
    val merged: Map[PeerId, BigInt] =
      delegated.keySet.union(collateral.keySet).iterator
        .map(p => p -> (delegated.getOrElse(p, BigInt(0)) + collateral.getOrElse(p, BigInt(0))))
        .toMap
    StakeDistribution(merged)
  }
}
