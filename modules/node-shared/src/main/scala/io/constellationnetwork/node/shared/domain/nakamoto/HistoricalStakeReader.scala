package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, HistoricalStakeSnapshot}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{historicalImmutableCodec => historicalStakeSnapshotImmutable}

/** §G3 — MPT-primary reader for `historicalStakeSnapshots`.
  *
  * Looks up a single eta-period boundary's [[HistoricalStakeSnapshot]] (the combined stake + eta record; Path 1 of the heap-leak
  * workstream) from the global-state MPT by composing `GlobalStateKey.historicalStakeSnapshotsKey[F](period)` and routing through the
  * supplied [[GlobalStateReader]] (typically `GlobalStateReader.pending` at chain bestTip, or `finalized` for follower paths). One MPT
  * entry per stored period (last 4 under retention).
  *
  * '''Why an MPT point read, not GSI iteration.''' Under the GSI-to-MPT migration the MPT is the canonical store; the GSI's
  * `historicalStakeSnapshots` map is reconstructed from the MPT at boot, so reading from the MPT directly removes a redundant in-memory
  * mirror and closes the byte-determinism gap between independent node MPT builds. The G1 reader migrated the live stake aggregate; G3
  * extends the same principle to the per-period historical snapshots used by the §3 NIPoPoW N-2 lookback in
  * [[StakeRegistry.stakeWeightedMpt]]'s `historicalDistributionFor` callback.
  *
  * '''Point read, not prefix scan.''' Each `EtaPeriod` lookup is a single key derived from the hashed period value; there's no need to
  * enumerate the partition. The retention horizon (4 periods) keeps the partition small but `lookup` is O(1) MPT-get regardless.
  *
  * '''Pre-boundary fall-through.''' Reads before the first boundary write at this period return `None`; the caller is responsible for the
  * warmup fallback. [[StakeRegistry.stakeWeightedMpt]] falls through to the live MPT aggregate via 1/N when the stake half is missing;
  * [[EtaStateManager]] falls through to a chainStore VRF walk + recompute when the eta half is missing.
  */
trait HistoricalStakeReader[F[_]] {

  /** Look up the [[HistoricalStakeSnapshot]] recorded at the boundary of `period`. Returns `None` when the period was never written
    * (pre-genesis, retention-evicted, or pre-boundary lookup).
    */
  def lookup(period: EtaPeriod)(implicit hasher: Hasher[F]): F[Option[HistoricalStakeSnapshot]]
}

object HistoricalStakeReader {

  /** Construct a reader against a [[GlobalStateReader]]. The reader's branch view (pending vs finalized) is the caller's choice — under
    * MultiBranch the consensus hot path uses `pending` at chain bestTip so the historical distribution observed for the current period
    * matches what the leader's parent-branch view committed.
    *
    * The `Hasher[F]` parameter on `lookup` is the same one that produced the boundary write inside
    * `AcceptanceMptStateChanges.applyStateChanges` — using a different hasher would compute a different MPT key and read miss every time.
    * In production both producers route through `HasherSelector[F].getCurrent`; in tests pass `Hasher.forJson[F]`.
    */
  def make[F[_]: Async](reader: GlobalStateReader[F]): HistoricalStakeReader[F] = new HistoricalStakeReader[F] {

    def lookup(period: EtaPeriod)(implicit hasher: Hasher[F]): F[Option[HistoricalStakeSnapshot]] = {
      // The codec import is consumed by the implicit search for `reader.get[HistoricalStakeSnapshot]`.
      // The `_ = historicalStakeSnapshotImmutable` binder makes that explicit at the helper boundary.
      val _ = historicalStakeSnapshotImmutable
      GlobalStateKey.historicalStakeSnapshotsKey[F](period).flatMap { key =>
        reader.get[HistoricalStakeSnapshot](key)
      }
    }
  }
}
