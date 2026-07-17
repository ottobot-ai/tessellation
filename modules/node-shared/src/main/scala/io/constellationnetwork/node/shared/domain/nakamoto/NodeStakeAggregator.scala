package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, MptOverlay, StakeCollateralMptReader}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nakamoto.{EpochStakeSnapshotter, StakeDistribution}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex

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
  * — wrap in [[NodeStakeAggregator.cached]], keyed by the exact overlay `BranchId`. Ordinal-only caching is unsafe because a density reorg
  * can replace the selected branch at the same height.
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
    * GSI describe the same committed state, the resulting `StakeDistribution` bytes are identical to those produced by
    * `EpochStakeSnapshotter.snapshot(info)`. This byte-equivalence is the §G2 migration contract; see `NodeStakeAggregatorSuite`
    * "snapshotFromMpt parity" test. A closing-period boundary cannot use the parent MPT for this projection because its current-ordinal
    * state changes have not been applied yet; that path projects the exact post-transition `GlobalSnapshotInfo` instead.
    *
    * Effectful because the underlying MPT prefix-scan is `F[]`.
    */
  def snapshotFromMpt[F[_]: Async](aggregator: NodeStakeAggregator[F])(implicit hasher: Hasher[F]): F[StakeDistribution] =
    aggregator.aggregateFromMpt.map(EpochStakeSnapshotter.fromCombined)

  /** Uncached implementation over one raw multi-prefix capture. Every call captures delegated stake and collateral from the same MPT image,
    * then validates and folds both partitions. Reading the prefixes independently is unsafe: finalization can replace the base between
    * scans and create a delegated(A) + collateral(B) hybrid stake distribution.
    *
    * '''Latency observability.''' Each call records its wall-clock to the `dag_nakamoto_stake_aggregator_mpt_scan_ms` distribution — gives
    * the operator a single Grafana series for "how expensive is the MPT prefix-scan in this run." Compared to a counter, the distribution
    * exposes p50/p99 which is what matters when the per-slot hot path fires this thousands of times. Recorded on every call (including the
    * cache-miss path inside `cached`, by virtue of the wrapper calling `underlying.aggregateFromMpt`).
    */
  private[nakamoto] def fromRawPrefixSnapshot[F[_]: Async: Metrics](
    capture: List[Hex] => F[Map[Hex, List[StrictMptRawEntry]]]
  ): NodeStakeAggregator[F] = new NodeStakeAggregator[F] {

    def aggregateFromMpt(implicit hasher: Hasher[F]): F[Map[PeerId, BigInt]] =
      for {
        startNanos <- Async[F].monotonic.map(_.toNanos)
        delegatedPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveDelegatedStakes)
        collateralPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveNodeCollaterals)
        rawEntries <- capture(List(delegatedPrefix, collateralPrefix))
        delegatedEntries <- StakeCollateralMptReader.materializeActiveDelegatedStakesFromRaw(
          rawEntries.getOrElse(delegatedPrefix, List.empty)
        )
        collateralEntries <- StakeCollateralMptReader.materializeActiveNodeCollateralsFromRaw(
          rawEntries.getOrElse(collateralPrefix, List.empty)
        )
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

  /** Finalized-base/bootstrap factory. The store lock makes the two scans one captured image for callers whose mutations also respect
    * `MptStore.withExclusiveLock`. Production GL0 consensus uses [[atBranch]], not this adapter.
    */
  def fromMptStore[F[_]: Async: Metrics](store: MptStore[F, GlobalStateKey]): NodeStakeAggregator[F] =
    fromRawPrefixSnapshot { prefixes =>
      store.withExclusiveLock {
        prefixes.distinct.traverse(prefix => store.rawEntriesForPrefixStrict(prefix).map(prefix -> _)).map(_.toMap)
      }
    }

  /** Requested overlay branch capture. MultiBranch holds its branch/finalization mutex for the complete two-prefix snapshot. The overlay's
    * legacy absent-branch-to-base behavior still applies; exact candidate-parent authentication remains a separate consensus requirement.
    */
  def atBranch[F[_]: Async: Metrics](
    overlay: MptOverlay[F, GlobalStateKey],
    branch: BranchId
  ): NodeStakeAggregator[F] =
    fromRawPrefixSnapshot(prefixes => overlay.rawEntriesForPrefixesStrict(branch, prefixes))

  /** Branch-identity cached wrapper.
    *
    * Caching question — three options:
    *   - (a) Pure pass-through: every call re-reads MPT. Simple and correct but expensive when `relativeStake` fires per slot at 1 Hz × N
    *     validators × per-leader-election × per- attestation-verify. Two prefix scans per call.
    *   - (b) '''Per-branch cache (this implementation).''' Hold a `Ref[F, Option[(BranchId, Map[PeerId, BigInt])]]` and refresh when the
    *     selected branch changes. The miss path receives the requested branch identity and captures both partitions together, so one
    *     aggregate cannot mix delegated stake from one tip with collateral from another. Authentication that an absent requested branch
    *     really names the current base is a separate candidate-parent binding requirement.
    *   - (c) Caller passes the aggregate as a pure `Map[PeerId, BigInt]`, computed once. Requires plumbing changes at every call site and
    *     loses the lazy boot path.
    *
    * Choice rationale: (b) hits the hot path without a plumbing rewrite. (a) is too expensive at 8gl0+4mg+4shards (sims show ~200
    * calls/sec). (c) is correct but invasive.
    *
    * The `currentBranchF` callback must be the same selected-tip source used by the live overlay reader. `None` (pre-bootstrap) defeats
    * caching and reads the finalized base through `BranchId.base`.
    */
  def cached[F[_]: Async](
    underlyingAt: BranchId => NodeStakeAggregator[F],
    currentBranchF: F[Option[BranchId]]
  ): F[NodeStakeAggregator[F]] =
    Ref.of[F, Option[(BranchId, Map[PeerId, BigInt])]](None).map { cacheR =>
      new NodeStakeAggregator[F] {
        def aggregateFromMpt(implicit hasher: Hasher[F]): F[Map[PeerId, BigInt]] =
          currentBranchF.flatMap {
            case None =>
              underlyingAt(BranchId.base).aggregateFromMpt.flatMap { fresh =>
                currentBranchF.flatMap {
                  case None    => fresh.pure[F]
                  case Some(_) => Async[F].defer(aggregateFromMpt)
                }
              }
            case Some(branch) =>
              cacheR.get.flatMap {
                case Some((cachedBranch, cachedMap)) if cachedBranch == branch =>
                  currentBranchF.flatMap {
                    case Some(stillSelected) if stillSelected == branch => cachedMap.pure[F]
                    case _                                              => Async[F].defer(aggregateFromMpt)
                  }
                case _ =>
                  for {
                    fresh <- underlyingAt(branch).aggregateFromMpt
                    selectedAfter <- currentBranchF
                    result <- selectedAfter match {
                      case Some(stillSelected) if stillSelected == branch =>
                        cacheR.set(Some((branch, fresh))).as(fresh)
                      case _ =>
                        Async[F].defer(aggregateFromMpt)
                    }
                  } yield result
              }
          }
      }
    }
}
