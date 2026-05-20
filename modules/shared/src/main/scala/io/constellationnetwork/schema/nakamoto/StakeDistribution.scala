package io.constellationnetwork.schema.nakamoto

import cats.Show
import cats.kernel.Order

import scala.collection.immutable.SortedMap

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.GlobalSnapshotInfo
import io.constellationnetwork.schema.peer.PeerId

import derevo.cats.eqv
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import io.circe._

/** Stake-distribution snapshot at an eta-period boundary, used by the §3 NIPoPoW N-2 lookback rule.
  *
  * '''Why raw amounts, not pre-normalized ratios.''' The denominator of `peerStake / totalStake` depends on which peer subset the caller
  * considers. The seedlist may change between snapshot time and read time (peer registration, slashing); the observed-active set is
  * runtime-only and is meaningless to a verifier replaying history. Storing raw `BigInt` amounts lets the caller pick its own denominator
  * at read time — the same way `StakeRegistry.stakeWeighted.totalStakeOver` does today against the live GSI.
  *
  * '''Why summed delegated + collateral.''' Matches the §1.1 stake definition used in production (`StakeRegistry.stakeOf`). Both tiers
  * contribute to leader eligibility today, and any historical reconstruction must agree byte-for-byte with the producer's view at that
  * moment.
  *
  * '''Identity at zero stake.''' A peer that registered no delegation and no collateral simply isn't in the map. `stakeOf` returns
  * `BigInt(0)` for missing keys — that's the same null behavior the live registry has when an unstaked peer is queried.
  */
@derive(encoder, decoder, eqv)
final case class StakeDistribution(stakes: SortedMap[PeerId, BigInt]) {

  /** Raw stake amount for a single peer. Returns 0 if the peer has no record. */
  def stakeOf(p: PeerId): BigInt = stakes.getOrElse(p, BigInt(0))

  /** Total stake summed across an arbitrary peer set. Peers missing from `stakes` contribute 0; off-set peers in `stakes` are ignored. */
  def totalOver(peers: Set[PeerId]): BigInt =
    peers.foldLeft(BigInt(0))((acc, p) => acc + stakeOf(p))

  /** Relative stake `p` holds against a denominator peer set. Returns `Ratio.Zero` if the denominator is zero, `p` is not in the
    * denominator set, or `p` has no stake recorded — same fail-closed semantics as `StakeRegistry.relativeStake`.
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

  /** The empty distribution — no peer has any stake. */
  val Empty: StakeDistribution = StakeDistribution(SortedMap.empty[PeerId, BigInt])

  /** Manual `Show` to avoid the cats / `OrphanInstances.showSortedMapAsList` ambiguity that bites every `@derive(show)` over a
    * `SortedMap[K, V]` inside the `io.constellationnetwork.schema` package. `Show.fromToString` is fine — this is only used for diagnostic
    * output.
    */
  implicit val show: Show[StakeDistribution] = Show.fromToString
}

/** Eta-period index. Period N spans ordinals `[N · etaRotationSnapshots, (N+1) · etaRotationSnapshots)`. Computed via
  * `EtaCalculation.rotationPeriod(ordinal, etaRotationSnapshots)`.
  *
  * '''Why a Long, not a `NonNegLong`.''' Periods can go negative during eligibility queries (`currentEtaPeriod - 2` is negative for periods
  * 0 and 1). Lookups against a negative period fall through to the genesis stake distribution. Modelling that as Option-at-the-boundary
  * would force partial semantics through every call site for a transient bootstrap-only edge case.
  */
@derive(eqv)
final case class EtaPeriod(value: Long) {

  /** Period that is `n` boundaries earlier (may be negative; see class docs). */
  def minus(n: Long): EtaPeriod = EtaPeriod(value - n)

  /** Period that is `n` boundaries later. */
  def plus(n: Long): EtaPeriod = EtaPeriod(value + n)
}

object EtaPeriod {
  val Zero: EtaPeriod = EtaPeriod(0L)

  implicit val order: Order[EtaPeriod] = Order.by(_.value)
  implicit val show: Show[EtaPeriod] = Show.show(p => s"EtaPeriod(${p.value})")

  // Backing Scala Ordering — required to instantiate `SortedMap[EtaPeriod, _]`.
  implicit val scalaOrdering: scala.math.Ordering[EtaPeriod] = scala.math.Ordering.by(_.value)

  // JSON encoders for use both as a value and as a map key.
  implicit val encoder: Encoder[EtaPeriod] = Encoder[Long].contramap(_.value)
  implicit val decoder: Decoder[EtaPeriod] = Decoder[Long].map(EtaPeriod(_))
  implicit val keyEncoder: KeyEncoder[EtaPeriod] = KeyEncoder.instance(_.value.toString)
  implicit val keyDecoder: KeyDecoder[EtaPeriod] =
    KeyDecoder.instance(s => scala.util.Try(s.toLong).toOption.map(EtaPeriod(_)))
}

/** Pure computation that lifts a `GlobalSnapshotInfo`'s `activeDelegatedStakes + activeNodeCollaterals` into a `StakeDistribution`.
  *
  * Run at every eta-period boundary on the snapshot whose ordinal is the final ordinal of the closing period — the resulting
  * `StakeDistribution` is committed into `GlobalSnapshotInfo.historicalStakeSnapshots[periodN]` and becomes the basis for slot-leader
  * eligibility two periods later (Cardano-style mark/set/go).
  *
  * '''Determinism.''' Identical inputs produce identical output. No `F[_]`, no Refs, no implicit state. Verifier-side reconstruction reads
  * the same GSI and runs the same function to get the same `StakeDistribution`.
  */
object EpochStakeSnapshotter {

  /** Build a stake-distribution from a GSI. Sums delegated-stake + node-collateral amounts per `nodeId`.
    *
    * Materializes into `SortedMap` so the binary encoding (used by the MPT/Brotli state-proof codec) is deterministic — two nodes building
    * the snapshot from the same GSI produce bit-identical bytes.
    */
  def snapshot(info: GlobalSnapshotInfo): StakeDistribution = {
    val delegated: Map[PeerId, BigInt] =
      info.activeDelegatedStakes.iterator.flatMap(_.valuesIterator).flatMap(_.iterator).foldLeft(Map.empty[PeerId, BigInt]) {
        (acc, record) =>
          val p = record.event.value.nodeId
          val amt = BigInt(record.amount.value.value)
          acc.updated(p, acc.getOrElse(p, BigInt(0)) + amt)
      }
    val collateral: Map[PeerId, BigInt] =
      info.activeNodeCollaterals.iterator.flatMap(_.valuesIterator).flatMap(_.iterator).foldLeft(Map.empty[PeerId, BigInt]) {
        (acc, record) =>
          val p = record.event.value.nodeId
          val amt = BigInt(record.event.value.amount.value.value)
          acc.updated(p, acc.getOrElse(p, BigInt(0)) + amt)
      }
    val merged: SortedMap[PeerId, BigInt] =
      delegated.keySet
        .union(collateral.keySet)
        .iterator
        .map(p => p -> (delegated.getOrElse(p, BigInt(0)) + collateral.getOrElse(p, BigInt(0))))
        .to(SortedMap)
    StakeDistribution(merged)
  }

  /** §3 NIPoPoW S0.5 — genesis backfill. Stamps the just-built genesis stake distribution under the three eta-period keys `{-2, -1, 0}` so
    * that the N-2 lookback (`relativeStakeAt(_, currentPeriod - 2)`) returns the genesis distribution during the first three eta periods
    * after boot — periods 0, 1, and 2.
    *
    * '''Why these three keys.''' The producer's lookup at ordinal in period `N` reads `historicalStakeSnapshots[N - 2]`. The GSAM boundary
    * write at `ord % R == R - 1` writes `historicalStakeSnapshots[N]` at the close of period N — too late to serve the first three periods
    * of slot-leader eligibility. Without backfill, the registry's `historicalDistributionFor` returns `None` and the fallback path lands on
    * the current GSI (correct only by coincidence — the genesis distribution is unchanged during warmup). Stamping explicitly closes the
    * gap: every periodic lookup returns the genesis distribution with no fall-through.
    *
    * '''Retention compatibility.''' GSAM prunes entries below `currentPeriod - 3` at each boundary. The backfill writes the exact set that
    * retention would keep at the start of period 0 (`[currentPeriod - 3, currentPeriod] = [-3, 0]` excluding the un-needed `-3`). At the
    * close of period 0, GSAM overwrites `0` with the actual end-of-period-0 distribution (no change if no stake events fired); at close of
    * period 1, retention drops `-2`; at close of period 2, drops `-1`. So the backfill entries are pruned exactly as they become
    * unreachable by lookback.
    *
    * '''Idempotent on re-load.''' If a node restarts and re-loads genesis after some boundary writes have already occurred, the backfill is
    * applied to the genesis GSI before any boundary-write history exists — so the three keys are written unconditionally. The first
    * acceptance after restart re-runs the producer's pipeline and overwrites at its boundary cadence; backfilled keys that have already
    * been pruned in the on-disk store will be re-introduced on this path. This is load-bearing only for the boot path of a fresh cluster.
    */
  def backfillGenesisStakeSnapshots(info: GlobalSnapshotInfo): GlobalSnapshotInfo = {
    val genesisDistribution = snapshot(info)
    val seeded: SortedMap[EtaPeriod, StakeDistribution] =
      List(EtaPeriod(-2L), EtaPeriod(-1L), EtaPeriod(0L))
        .foldLeft(info.historicalStakeSnapshots)((acc, k) => acc.updated(k, genesisDistribution))
    info.copy(historicalStakeSnapshots = seeded)
  }
}
