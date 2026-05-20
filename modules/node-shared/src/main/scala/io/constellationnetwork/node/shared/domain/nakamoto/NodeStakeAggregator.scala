package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.delegatedStake.DelegatedStakeRecord
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.nakamoto.{EpochStakeSnapshotter, StakeDistribution}
import io.constellationnetwork.schema.nodeCollateral.NodeCollateralRecord
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.{delegatedStakeRecordSetCodec, nodeCollateralRecordSetCodec}

import eu.timepit.refined.auto._

/** G1 — MPT-backed per-node stake aggregator.
  *
  * Sums `delegated-stake + node-collateral` amounts across both partitions, keyed by `record.event.value.nodeId`. Returns `Map[PeerId,
  * BigInt]` of two-tier combined stake.
  *
  * Reads from the global-state MPT via prefix-scan against the two stake partitions (`ActiveDelegatedStakes` field id 13,
  * `ActiveNodeCollaterals` field id 15). Records pointing at the same `nodeId` from different source addresses (or from a mix of delegated
  * + collateral) sum into one aggregate per node.
  *
  * '''Why MPT prefix-scan, not GSI iteration.''' The legacy `StakeRegistry.stakeWeighted` walks `info.activeDelegatedStakes` /
  * `info.activeNodeCollaterals` in-memory maps which are GSI-primary. Under the GSI-to-MPT migration the MPT is the canonical store; the
  * GSI gets reconstructed from the MPT at boot, so reading from the MPT directly removes a redundant in-memory mirror and avoids the
  * byte-determinism gap between two independent node MPT builds (closes a class of #218-style divergence bugs).
  *
  * '''Empty-MPT semantics.''' If the prefix scan returns no entries (pre-genesis / boot path, or a snapshot at ordinal 0 with no stake
  * records yet), `aggregateFromMpt` returns an empty map. Caller is responsible for the 1/N fallback (current
  * `StakeRegistry.stakeWeightedMpt` impl handles this).
  *
  * '''Caching.''' This trait exposes the raw, uncached read. For the consensus hot path — `EligibilityChecker.relativeStake` fires per-slot
  * — wrap in [[NodeStakeAggregator.cached]] which memoizes the result keyed on `SnapshotOrdinal` and invalidates whenever the parent
  * ordinal advances. The pattern mirrors `materializeActiveTokenLocksFromMpt` ("read once per accept") but with finer-grained per-ordinal
  * invalidation since `relativeStake` is read-side, not write-side.
  */
trait NodeStakeAggregator[F[_]] {

  /** Aggregate (delegated stake + node collateral) per `nodeId`, reading from MPT prefix-scan.
    *
    * Returns an empty map if the MPT has no entries.
    */
  def aggregateFromMpt(implicit hasher: Hasher[F]): F[Map[PeerId, BigInt]]
}

object NodeStakeAggregator {

  /** §G2 — MPT-primary equivalent of `EpochStakeSnapshotter.snapshot(info)`.
    *
    * Reads the per-node aggregate via `aggregator.aggregateFromMpt` and wraps it in a `StakeDistribution` through
    * `EpochStakeSnapshotter.fromCombined`. The wrapping uses the same `SortedMap` materialization as the GSI-primary path — so when MPT and
    * GSI are in sync (which is the case during `accept()` — the GSI is built from the same accepted records that the MPT writer then
    * syncs), the resulting `StakeDistribution` bytes are identical to those produced by `EpochStakeSnapshotter.snapshot(info)`. This
    * byte-equivalence is the §G2 migration contract; see `NodeStakeAggregatorSuite` "snapshotFromMpt parity" test.
    *
    * Effectful because the underlying MPT prefix-scan is `F[]`. Callers in `GlobalSnapshotAcceptanceManager.accept()` already run under an
    * `implicit hasher: Hasher[F]` (set at the top of `accept()` from `HasherSelector.getForOrdinal(ordinal)`), so the additional `flatMap`
    * lifts cleanly into the existing for-comprehension.
    */
  def snapshotFromMpt[F[_]: Async](aggregator: NodeStakeAggregator[F])(implicit hasher: Hasher[F]): F[StakeDistribution] =
    aggregator.aggregateFromMpt.map(EpochStakeSnapshotter.fromCombined)

  /** Uncached pass-through implementation. Every call re-runs two MPT prefix scans + an in-memory fold. Suitable for low-frequency callers
    * (boot, diagnostics, tests). Production hot-path uses [[cached]] which wraps this and serves repeated calls within the same snapshot
    * ordinal from memory.
    *
    * '''Latency observability.''' Each call records its wall-clock to the `dag_nakamoto_stake_aggregator_mpt_scan_ms` distribution — gives
    * the operator a single Grafana series for "how expensive is the MPT prefix-scan in this run." Compared to a counter, the distribution
    * exposes p50/p99 which is what matters when the per-slot hot path fires this thousands of times. Recorded on every call (including the
    * cache-miss path inside `cached`, by virtue of the wrapper calling `underlying.aggregateFromMpt`).
    */
  def make[F[_]: Async: Metrics](reader: GlobalStateReader[F]): NodeStakeAggregator[F] = new NodeStakeAggregator[F] {

    def aggregateFromMpt(implicit hasher: Hasher[F]): F[Map[PeerId, BigInt]] =
      for {
        startNanos <- Async[F].monotonic.map(_.toNanos)
        delegatedPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveDelegatedStakes)
        delegatedEntries <- reader.getAllForPrefix[SortedSet[DelegatedStakeRecord]](delegatedPrefix)
        collateralPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveNodeCollaterals)
        collateralEntries <- reader.getAllForPrefix[SortedSet[NodeCollateralRecord]](collateralPrefix)
        endNanos <- Async[F].monotonic.map(_.toNanos)
        elapsedMs = (endNanos - startNanos) / 1000000L
        _ <- Metrics[F].recordDistribution("dag_nakamoto_stake_aggregator_mpt_scan_ms", elapsedMs)
      } yield {
        // Each prefix entry is a SortedSet of records keyed by source address. Multiple sets across
        // addresses can point at the same nodeId — combine all amounts into a single Map[PeerId, BigInt].
        val empty = Map.empty[PeerId, BigInt]

        val withDelegated = delegatedEntries.valuesIterator
          .flatMap(_.iterator)
          .foldLeft(empty) { (acc, record) =>
            val nodeId = record.event.value.nodeId
            val amt = BigInt(record.amount.value.value)
            acc.updated(nodeId, acc.getOrElse(nodeId, BigInt(0)) + amt)
          }

        val withBoth = collateralEntries.valuesIterator
          .flatMap(_.iterator)
          .foldLeft(withDelegated) { (acc, record) =>
            val nodeId = record.event.value.nodeId
            val amt = BigInt(record.event.value.amount.value.value)
            acc.updated(nodeId, acc.getOrElse(nodeId, BigInt(0)) + amt)
          }

        withBoth
      }
  }

  /** Per-snapshot-ordinal cached wrapper.
    *
    * Caching question — three options:
    *   - (a) Pure pass-through: every call re-reads MPT. Simple and correct but expensive when `relativeStake` fires per slot at 1 Hz × N
    *     validators × per-leader-election × per- attestation-verify. Two prefix scans per call.
    *   - (b) '''Per-snapshot cache (this implementation).''' Hold a `Ref[F, Option[(SnapshotOrdinal, Map[PeerId, BigInt])]]` and refresh
    *     when the snapshot ordinal advances. Matches `materializeActiveTokenLocksFromMpt` semantics (read once per accept) but with finer-
    *     grained per-ordinal invalidation. Trade-off: a stale read within the same snapshot ordinal is by construction correct because the
    *     MPT view at that ordinal is immutable; stake mutations only land at snapshot acceptance.
    *   - (c) Caller passes the aggregate as a pure `Map[PeerId, BigInt]`, computed once. Requires plumbing changes at every call site and
    *     loses the lazy boot path.
    *
    * Choice rationale: (b) hits the hot path without a plumbing rewrite. (a) is too expensive at 8gl0+4mg+4shards (sims show ~200
    * calls/sec). (c) is correct but invasive.
    *
    * The `currentOrdinalF` callback resolves the "view ordinal" — typically `lastGlobalSnapshotStorage.getOrdinal`. `None` (pre-genesis)
    * defeats caching (always reads).
    */
  def cached[F[_]: Async](
    underlying: NodeStakeAggregator[F],
    currentOrdinalF: F[Option[SnapshotOrdinal]]
  ): F[NodeStakeAggregator[F]] =
    Ref.of[F, Option[(SnapshotOrdinal, Map[PeerId, BigInt])]](None).map { cacheR =>
      new NodeStakeAggregator[F] {
        def aggregateFromMpt(implicit hasher: Hasher[F]): F[Map[PeerId, BigInt]] =
          currentOrdinalF.flatMap {
            case None =>
              // No ordinal yet → always read uncached; pre-genesis path with no MPT entries returns
              // empty map immediately.
              underlying.aggregateFromMpt
            case Some(ord) =>
              cacheR.get.flatMap {
                case Some((cachedOrd, cachedMap)) if cachedOrd === ord =>
                  Async[F].pure(cachedMap)
                case _ =>
                  for {
                    fresh <- underlying.aggregateFromMpt
                    _ <- cacheR.set(Some((ord, fresh)))
                  } yield fresh
              }
          }
      }
    }
}
