package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import org.typelevel.log4cats.Logger

/** In-memory store of stake distributions at eta-period boundaries.
  *
  * '''Why in-memory, not GSI-persisted.''' `StakeDistribution` is a deterministic function of `activeDelegatedStakes + activeNodeCollaterals`
  * at the closing-ordinal of an eta period. Both sides of any cross-node MPT check would derive the same value from the same primary state,
  * so a `historicalStakeSnapshots` field on `GlobalSnapshotInfo` would be redundant for in-cluster correctness — at the cost of ~200 LOC of
  * GlobalStateConverter / field-id / per-field-root plumbing. We defer that integration until the NIPoPoW verifier (Slice S4) actually
  * needs the value to be part of the state-proof shape.
  *
  * '''Restart semantics.''' On boot, the store is empty. Either:
  *   - The caller seeds it from the genesis distribution for periods `[−1, 0, 1]` (Slice S0.5), so the first 2 eta periods of consensus
  *     have a deterministic answer before any period-boundary commit has fired.
  *   - The caller calls [[backfillFromChain]] to walk back through finalized snapshots and rebuild the last few periods.
  *
  * Either path produces the SAME entries on every node — backfill is deterministic in the chain it reads.
  *
  * '''Retention.''' Entries older than `(currentPeriod - retentionPeriods)` are pruned on every write. Default retention is 4 (keeps
  * `(N, N-1, N-2, N-3)`), giving 1 period of grace beyond the N-2 lookback the algorithm requires. Bounded memory: `retentionPeriods · |
  * validators| · ~16 bytes` ≈ small even with 1000 validators.
  */
trait EpochStakeHistory[F[_]] {

  /** Read the distribution recorded at the boundary of `period`. Returns `None` if no entry was recorded for that period (negative period,
    * pre-genesis warmup, or pruned away).
    */
  def get(period: EtaPeriod): F[Option[StakeDistribution]]

  /** Commit the distribution for an eta-period boundary. Idempotent on repeat with the same `(period, distribution)`; later writes for the
    * same period overwrite earlier ones (chain reorgs may reapply the boundary). After writing, entries older than
    * `(period - retentionPeriods)` are pruned.
    */
  def record(period: EtaPeriod, distribution: StakeDistribution): F[Unit]

  /** Current size — number of period entries retained. For tests and metrics. */
  def size: F[Int]
}

object EpochStakeHistory {

  /** Default retention: 4 periods. Algorithm needs N-2; the extra 2 periods give grace for chain reorgs that re-finalize a different
    * boundary snapshot and for the period-boundary write being slightly out-of-order with the eligibility read.
    */
  val DefaultRetentionPeriods: Long = 4L

  def make[F[_]: Async](logger: Logger[F], retentionPeriods: Long = DefaultRetentionPeriods): F[EpochStakeHistory[F]] =
    Ref.of[F, Map[EtaPeriod, StakeDistribution]](Map.empty).map { ref =>
      new EpochStakeHistory[F] {

        def get(period: EtaPeriod): F[Option[StakeDistribution]] =
          ref.get.map(_.get(period))

        def record(period: EtaPeriod, distribution: StakeDistribution): F[Unit] =
          ref.update { hist =>
            val withNew = hist.updated(period, distribution)
            val cutoff = period.minus(retentionPeriods)
            withNew.filter { case (p, _) => p.value >= cutoff.value }
          } >>
            logger.info(
              s"📊 EpochStakeHistory: recorded period=${period.value} (${distribution.stakes.size} stakers); retention<${(period.minus(retentionPeriods)).value}"
            )

        def size: F[Int] = ref.get.map(_.size)
      }
    }
}
