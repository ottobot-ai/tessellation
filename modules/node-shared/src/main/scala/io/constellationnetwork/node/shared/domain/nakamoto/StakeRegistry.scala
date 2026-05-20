package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Async, Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.GlobalSnapshotInfo
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, StakeDistribution}
import io.constellationnetwork.schema.peer.PeerId

/** Read-only view of validator stake for Nakamoto consensus.
  *
  * Used by EligibilityChecker to determine threshold scaling. Phase 3: equal weight (1/N). §1.1: stake-proportional via `stakeWeighted` —
  * combined `delegatedStake + nodeCollateral` per the strategic-signals memo (two-tier stake is the unit).
  *
  * Stakes are returned as exact `Ratio` so eligibility and finality threshold computations are byte-identical across all JVMs/CPUs (no IEEE
  * 754 sum-order or rounding non-determinism).
  *
  * Supports optimistic finality: tracks observed active peers (those that have attested recently) and computes finality weight against the
  * active set rather than the full seedlist. This allows the cluster to finalize when some seedlist peers are offline, without blocking the
  * online majority.
  */
trait StakeRegistry[F[_]] {

  /** Get the relative stake [0,1] for a peer. Returns 0 if peer is not a validator. */
  def relativeStake(peerId: PeerId): F[Ratio]

  /** Get all active validators and their relative stakes */
  def allStakes: F[Map[PeerId, Ratio]]

  /** Total number of validators in the full seedlist */
  def validatorCount: F[Int]

  /** Number of observed active validators (have attested recently) */
  def observedActiveCount: F[Int]

  /** Get the set of active validator PeerIds (full seedlist) */
  def activeValidators: F[Set[PeerId]]

  /** Get the set of observed active PeerIds */
  def observedActive: F[Set[PeerId]]

  /** Update the full validator set from seedlist (called on startup) */
  def updateValidators(validators: Set[PeerId]): F[Unit]

  /** Mark a peer as observed active (called when attestation received) */
  def markActive(peerId: PeerId): F[Unit]

  /** Mark a peer as inactive (called on timeout/disconnect) */
  def markInactive(peerId: PeerId): F[Unit]

  /** Get relative stake computed against observed active peers only (for optimistic finality). Returns 0 if peer is not active.
    */
  def optimisticRelativeStake(peerId: PeerId): F[Ratio]

  /** Stable σ for committee sortition — `1 / |validators|` if `peerId` is a validator, else `0`. Independent of GSI evolution and observed-
    * active set churn, so sender and receiver always agree on the same value within an eta period. The committee VRF assumes σ_sender =
    * σ_receiver for the threshold `K · σ` to evaluate identically on both sides; the `optimisticRelativeStake` path drifts (active set
    * changes, GSI totalStake grows) and broke the gate's verify-receive consistently (#216).
    *
    * v1 trade-off: equal weight per validator instead of stake-weighted. Loses Algorand's stake-weighted sampling property but gains
    * cluster-wide determinism without historical-stake-lookback infrastructure. Stake-weighted committee sortition (anchored to eta-period
    * boundary) is the §3/NIPoPoW path.
    */
  def committeeStake(peerId: PeerId): F[Ratio]

  /** §3 NIPoPoW N-2 lookback: relative stake as recorded at the boundary of eta-period `etaPeriod`, against the **current** validator set.
    *
    * The verifier needs this view to deterministically reconstruct who held what stake at the snapshot's production time. Today's snapshot
    * at ordinal X is produced under the stake distribution that was finalized at the boundary of `(X.etaPeriod - 2)` — Cardano's
    * mark/set/go pipeline applied here.
    *
    * '''Fallback semantics (load-bearing for boot):'''
    *   - If `etaPeriod` is negative or the historical snapshot for that period is not yet recorded (pre-genesis warmup, the first 2 eta
    *     periods after genesis), the impl returns the genesis distribution's view. The boot path stays correct as long as the
    *     genesis-loader stamps period -1 / 0 / 1 with the same starting distribution (§ S0.5).
    *   - If `peerId` is not in the current validator set, returns `Ratio.Zero` regardless of history. Slashed/removed validators get 0 even
    *     if they had stake in the lookback period.
    *
    * '''Why against the current validator set, not the historical one.''' Validator membership is governed by the seedlist, not by stake.
    * Slashing and registration mutate the validator set at the moment of acceptance, not at the eta-period boundary. The historical stake
    * distribution captures *amounts*; the current seedlist is the gate.
    */
  def relativeStakeAt(peerId: PeerId, etaPeriod: EtaPeriod): F[Ratio]
}

object StakeRegistry {

  /** Minimum '''stake fraction''' of the seedlist that must be observed-active before optimistic finality kicks in. Below this, fall back
    * to full-seedlist weight (depth-based finality only). Prevents 2/2 online nodes finalizing a network where the offline majority holds
    * most of the stake.
    *
    * §1.1 semantic change: the comparison is now a stake fraction (Σ stake of observed-active ÷ Σ stake of full seedlist) instead of a
    * count fraction (# observed-active ÷ # seedlist). The env var name `NAKAMOTO_OPTIMISTIC_MIN_FRACTION` is unchanged — interface stable,
    * semantics moved with the stake-weighted VRF election. For the legacy `equalWeight` registry the comparison remains count-fraction
    * (since every peer carries 1/N) and the two definitions coincide.
    *
    * Default: 1/2. Override via `NAKAMOTO_OPTIMISTIC_MIN_FRACTION` (parsed Double, locked into Ratio at boot). Lower for small clusters;
    * raise for stricter participation requirements.
    */
  val MinActiveQuorumFraction: Ratio =
    sys.env
      .get("NAKAMOTO_OPTIMISTIC_MIN_FRACTION")
      .flatMap(_.toDoubleOption)
      .map(Ratio(_, 18))
      .getOrElse(Ratio(1, 2))

  /** Equal-weight stake registry with optimistic active tracking. Every validator in seedlist gets 1/N for VRF eligibility. Finality weight
    * computed against observed active peers.
    *
    * For the equal-weight registry the optimistic quorum check is a count-fraction comparison; since every peer carries 1/N this is also
    * the stake-fraction. Retained as the boot path and test fixture.
    */
  def equalWeight[F[_]: Sync]: F[StakeRegistry[F]] =
    (Ref.of[F, Set[PeerId]](Set.empty), Ref.of[F, Set[PeerId]](Set.empty)).mapN { (validatorsRef, activeRef) =>
      new StakeRegistry[F] {
        def relativeStake(peerId: PeerId): F[Ratio] =
          validatorsRef.get.map { validators =>
            if (validators.contains(peerId) && validators.nonEmpty)
              Ratio(1, validators.size)
            else Ratio.Zero
          }

        def allStakes: F[Map[PeerId, Ratio]] =
          validatorsRef.get.map { validators =>
            if (validators.isEmpty) Map.empty
            else {
              val stake = Ratio(1, validators.size)
              validators.map(_ -> stake).toMap
            }
          }

        def validatorCount: F[Int] =
          validatorsRef.get.map(_.size)

        def observedActiveCount: F[Int] =
          activeRef.get.map(_.size)

        def activeValidators: F[Set[PeerId]] =
          validatorsRef.get

        def observedActive: F[Set[PeerId]] =
          activeRef.get

        def updateValidators(validators: Set[PeerId]): F[Unit] =
          validatorsRef.set(validators)

        def markActive(peerId: PeerId): F[Unit] =
          validatorsRef.get.flatMap { validators =>
            // Only track peers that are in the seedlist
            activeRef.update(_ + peerId).whenA(validators.contains(peerId))
          }

        def markInactive(peerId: PeerId): F[Unit] =
          activeRef.update(_ - peerId)

        def optimisticRelativeStake(peerId: PeerId): F[Ratio] =
          (validatorsRef.get, activeRef.get).mapN { (validators, active) =>
            // Only use optimistic weight if we have enough active peers
            val effectiveActive = active.intersect(validators)
            val meetsQuorum = validators.nonEmpty &&
              Ratio(effectiveActive.size, validators.size) >= MinActiveQuorumFraction

            if (meetsQuorum && effectiveActive.contains(peerId))
              Ratio(1, effectiveActive.size)
            else if (validators.contains(peerId) && validators.nonEmpty)
              Ratio(1, validators.size) // fallback to full seedlist weight
            else Ratio.Zero
          }

        def committeeStake(peerId: PeerId): F[Ratio] =
          validatorsRef.get.map { validators =>
            if (validators.contains(peerId) && validators.nonEmpty) Ratio(1, validators.size)
            else Ratio.Zero
          }

        // Equal-weight is history-independent: every validator gets 1/N regardless of period.
        // N-2 staggering is moot when there are no stake-amount variations to stagger.
        def relativeStakeAt(peerId: PeerId, etaPeriod: EtaPeriod): F[Ratio] =
          relativeStake(peerId)
      }
    }

  /** Stake-weighted registry sourced from `GlobalSnapshotInfo.activeDelegatedStakes + activeNodeCollaterals`.
    *
    * Weight per peer = (Σ delegated-stake amount with `nodeId == peer`) + (Σ node-collateral amount with `nodeId == peer`), both rolled
    * into a single `BigInt` numerator. Relative stake = peer-weight ÷ Σ(weight over seedlist).
    *
    * Seedlist membership is the gate: stake records pointing at a `nodeId` outside the seedlist are dropped from both numerator and
    * denominator. The set of validators and the observed-active set are held in `Ref`s, mirroring [[equalWeight]] — `updateValidators`,
    * `markActive`, `markInactive` are byte-identical to that path.
    *
    * Boot fallback: when `snapshotInfoR` returns `None` (the node has not loaded a GSI yet), relative stake falls back to `Ratio(1,
    * validators.size)` so leader-loop wiring before genesis still elects.
    *
    * §3 NIPoPoW N-2 lookback: `historicalDistributionFor` resolves a previously-recorded stake snapshot for an eta period. Used by
    * [[StakeRegistry.relativeStakeAt]]. Returning `None` means "not yet recorded"; the caller falls through to the current GSI as a warmup
    * default (correct only during the first 2 eta periods, by which time `historicalDistributionFor` MUST be returning real data).
    */
  def stakeWeighted[F[_]: Async](
    snapshotInfoR: F[Option[GlobalSnapshotInfo]],
    historicalDistributionFor: EtaPeriod => F[Option[StakeDistribution]]
  ): F[StakeRegistry[F]] =
    (Ref.of[F, Set[PeerId]](Set.empty), Ref.of[F, Set[PeerId]](Set.empty)).mapN { (validatorsRef, activeRef) =>
      new StakeRegistry[F] {

        // Sum delegated + collateral amounts for every record in the GSI whose `nodeId == p`.
        // Both tiers contribute as a single BigInt — matches the §1.1 plan's "combined stake" definition.
        private def stakeOf(info: GlobalSnapshotInfo, p: PeerId): BigInt = {
          val delegated: BigInt = info.activeDelegatedStakes.map { byAddr =>
            byAddr.valuesIterator.flatMap(_.iterator).foldLeft(BigInt(0)) { (acc, record) =>
              if (record.event.value.nodeId == p) acc + BigInt(record.amount.value.value)
              else acc
            }
          }
            .getOrElse(BigInt(0))

          val collateral: BigInt = info.activeNodeCollaterals.map { byAddr =>
            byAddr.valuesIterator.flatMap(_.iterator).foldLeft(BigInt(0)) { (acc, record) =>
              if (record.event.value.nodeId == p) acc + BigInt(record.event.value.amount.value.value)
              else acc
            }
          }
            .getOrElse(BigInt(0))

          delegated + collateral
        }

        // Σ stake over a peer set, dropping off-set node-ids from numerator AND denominator.
        private def totalStakeOver(info: GlobalSnapshotInfo, peers: Set[PeerId]): BigInt =
          peers.foldLeft(BigInt(0))((acc, p) => acc + stakeOf(info, p))

        def relativeStake(peerId: PeerId): F[Ratio] =
          (snapshotInfoR, validatorsRef.get).flatMapN {
            case (None, validators) =>
              // Boot path: no GSI yet — fall back to 1/N so leader-loop wiring before genesis still elects.
              Async[F].pure {
                if (validators.contains(peerId) && validators.nonEmpty) Ratio(1, validators.size)
                else Ratio.Zero
              }
            case (Some(info), validators) =>
              Async[F].pure {
                if (!validators.contains(peerId) || validators.isEmpty) Ratio.Zero
                else {
                  val total = totalStakeOver(info, validators)
                  if (total == BigInt(0)) Ratio.Zero
                  else Ratio(stakeOf(info, peerId), total)
                }
              }
          }

        def allStakes: F[Map[PeerId, Ratio]] =
          (snapshotInfoR, validatorsRef.get).flatMapN {
            case (None, validators) =>
              Async[F].pure {
                if (validators.isEmpty) Map.empty[PeerId, Ratio]
                else {
                  val stake = Ratio(1, validators.size)
                  validators.map(_ -> stake).toMap
                }
              }
            case (Some(info), validators) =>
              Async[F].pure {
                if (validators.isEmpty) Map.empty[PeerId, Ratio]
                else {
                  val total = totalStakeOver(info, validators)
                  if (total == BigInt(0)) Map.empty[PeerId, Ratio]
                  else validators.iterator.map(p => p -> Ratio(stakeOf(info, p), total)).toMap
                }
              }
          }

        def validatorCount: F[Int] =
          validatorsRef.get.map(_.size)

        def observedActiveCount: F[Int] =
          activeRef.get.map(_.size)

        def activeValidators: F[Set[PeerId]] =
          validatorsRef.get

        def observedActive: F[Set[PeerId]] =
          activeRef.get

        def updateValidators(validators: Set[PeerId]): F[Unit] =
          validatorsRef.set(validators)

        def markActive(peerId: PeerId): F[Unit] =
          validatorsRef.get.flatMap { validators =>
            activeRef.update(_ + peerId).whenA(validators.contains(peerId))
          }

        def markInactive(peerId: PeerId): F[Unit] =
          activeRef.update(_ - peerId)

        def optimisticRelativeStake(peerId: PeerId): F[Ratio] =
          (snapshotInfoR, validatorsRef.get, activeRef.get).flatMapN {
            case (None, validators, _) =>
              // Boot path: same fallback as relativeStake — 1/N.
              Async[F].pure {
                if (validators.contains(peerId) && validators.nonEmpty) Ratio(1, validators.size)
                else Ratio.Zero
              }
            case (Some(info), validators, active) =>
              Async[F].pure {
                val effectiveActive = active.intersect(validators)
                val totalSeedlist = totalStakeOver(info, validators)
                val totalActive = totalStakeOver(info, effectiveActive)
                val activeStakeFraction =
                  if (totalSeedlist == BigInt(0)) Ratio.Zero
                  else Ratio(totalActive, totalSeedlist)
                val meetsQuorum = validators.nonEmpty && activeStakeFraction >= MinActiveQuorumFraction

                if (meetsQuorum && effectiveActive.contains(peerId) && totalActive != BigInt(0))
                  Ratio(stakeOf(info, peerId), totalActive)
                else if (validators.contains(peerId) && validators.nonEmpty) {
                  // Full-seedlist fallback. Mirrors the relativeStake path so behavior is identical when below quorum
                  // or when the peer is not in the observed-active set.
                  if (totalSeedlist == BigInt(0)) Ratio.Zero
                  else Ratio(stakeOf(info, peerId), totalSeedlist)
                } else Ratio.Zero
              }
          }

        def committeeStake(peerId: PeerId): F[Ratio] =
          validatorsRef.get.map { validators =>
            if (validators.contains(peerId) && validators.nonEmpty) Ratio(1, validators.size)
            else Ratio.Zero
          }

        // §3 NIPoPoW N-2: read the stake distribution that was finalized at the boundary of `etaPeriod`,
        // compute relativeStake against the current validator set. Fallback chain:
        //   1. historical snapshot exists → use it (post-warmup steady state)
        //   2. period < 0 OR no historical record yet → fall through to current GSI (warmup; valid for
        //      periods 0 and 1 because the genesis loader stamps those with the genesis distribution)
        //   3. GSI also unavailable (very early boot) → 1/N fallback (mirrors relativeStake's bootstrap path)
        def relativeStakeAt(peerId: PeerId, etaPeriod: EtaPeriod): F[Ratio] =
          (historicalDistributionFor(etaPeriod), validatorsRef.get).flatMapN {
            case (Some(distribution), validators) =>
              Async[F].pure {
                if (!validators.contains(peerId) || validators.isEmpty) Ratio.Zero
                else distribution.relativeStakeAgainst(peerId, validators)
              }
            case (None, validators) =>
              // Warmup fall-through: use the current GSI as the stake distribution. Correct during
              // the first 2 eta periods after genesis because the genesis distribution is unchanged.
              snapshotInfoR.map {
                case None =>
                  if (validators.contains(peerId) && validators.nonEmpty) Ratio(1, validators.size)
                  else Ratio.Zero
                case Some(info) =>
                  if (!validators.contains(peerId) || validators.isEmpty) Ratio.Zero
                  else {
                    val total = totalStakeOver(info, validators)
                    if (total == BigInt(0)) Ratio.Zero
                    else Ratio(stakeOf(info, peerId), total)
                  }
              }
          }
      }
    }

  /** §G1 — MPT-primary stake-weighted registry.
    *
    * Identical surface to [[stakeWeighted]] but the per-node aggregate is sourced from the MPT prefix-scan via [[NodeStakeAggregator]]
    * instead of iterating `GlobalSnapshotInfo`'s in-memory `activeDelegatedStakes` / `activeNodeCollaterals` maps. Production wiring should
    * pass a [[NodeStakeAggregator.cached]] aggregator so the hot path (per-slot VRF eligibility) doesn't re-prefix-scan every call — see
    * the caching trade-off discussion on [[NodeStakeAggregator.cached]].
    *
    * '''Boot fallback.''' When `aggregator.aggregateFromMpt` returns an empty map (the MPT has no stake entries — pre-genesis warmup or a
    * tip whose ancestor's MPT hasn't been hydrated yet), relative stake falls back to `Ratio(1, validators.size)`. This matches
    * [[stakeWeighted]]'s `snapshotInfoR = None` boot path so the leader-loop wiring elects pre-genesis just like before the migration.
    *
    * '''§3 NIPoPoW N-2 lookback.''' `relativeStakeAt` is unchanged from [[stakeWeighted]] — the historical-distribution path doesn't read
    * from the live MPT; it consults `historicalDistributionFor(period)` (which today still reads from
    * `GlobalSnapshotInfo.historicalStakeSnapshots`). The warmup fall-through reads the current MPT-derived aggregate via the same fallback
    * chain — empty aggregate → 1/N.
    */
  def stakeWeightedMpt[F[_]: Async](
    aggregator: NodeStakeAggregator[F],
    historicalDistributionFor: EtaPeriod => F[Option[StakeDistribution]]
  )(implicit hasher: io.constellationnetwork.security.Hasher[F]): F[StakeRegistry[F]] =
    (Ref.of[F, Set[PeerId]](Set.empty), Ref.of[F, Set[PeerId]](Set.empty)).mapN { (validatorsRef, activeRef) =>
      new StakeRegistry[F] {

        // Combined-stake aggregate over an explicit peer set. Mirrors `totalStakeOver` in the
        // [[stakeWeighted]] impl but reads from the pre-computed Map[PeerId, BigInt] aggregate
        // instead of iterating GSI partitions; off-set node-ids are dropped from both numerator AND
        // denominator (the aggregate is keyed by nodeId but we restrict to peers ∈ seedlist).
        private def totalStakeOver(agg: Map[PeerId, BigInt], peers: Set[PeerId]): BigInt =
          peers.foldLeft(BigInt(0))((acc, p) => acc + agg.getOrElse(p, BigInt(0)))

        def relativeStake(peerId: PeerId): F[Ratio] =
          (aggregator.aggregateFromMpt, validatorsRef.get).flatMapN { (agg, validators) =>
            Async[F].pure {
              if (!validators.contains(peerId) || validators.isEmpty) Ratio.Zero
              else if (agg.isEmpty) {
                // Boot fallback — MPT has no stake records yet. Mirror the [[stakeWeighted]]
                // snapshotInfoR=None path so pre-genesis leader-loop elects via 1/N.
                Ratio(1, validators.size)
              } else {
                val total = totalStakeOver(agg, validators)
                if (total == BigInt(0)) Ratio.Zero
                else Ratio(agg.getOrElse(peerId, BigInt(0)), total)
              }
            }
          }

        def allStakes: F[Map[PeerId, Ratio]] =
          (aggregator.aggregateFromMpt, validatorsRef.get).flatMapN { (agg, validators) =>
            Async[F].pure {
              if (validators.isEmpty) Map.empty[PeerId, Ratio]
              else if (agg.isEmpty) {
                val stake = Ratio(1, validators.size)
                validators.map(_ -> stake).toMap
              } else {
                val total = totalStakeOver(agg, validators)
                if (total == BigInt(0)) Map.empty[PeerId, Ratio]
                else validators.iterator.map(p => p -> Ratio(agg.getOrElse(p, BigInt(0)), total)).toMap
              }
            }
          }

        def validatorCount: F[Int] =
          validatorsRef.get.map(_.size)

        def observedActiveCount: F[Int] =
          activeRef.get.map(_.size)

        def activeValidators: F[Set[PeerId]] =
          validatorsRef.get

        def observedActive: F[Set[PeerId]] =
          activeRef.get

        def updateValidators(validators: Set[PeerId]): F[Unit] =
          validatorsRef.set(validators)

        def markActive(peerId: PeerId): F[Unit] =
          validatorsRef.get.flatMap { validators =>
            activeRef.update(_ + peerId).whenA(validators.contains(peerId))
          }

        def markInactive(peerId: PeerId): F[Unit] =
          activeRef.update(_ - peerId)

        def optimisticRelativeStake(peerId: PeerId): F[Ratio] =
          (aggregator.aggregateFromMpt, validatorsRef.get, activeRef.get).flatMapN { (agg, validators, active) =>
            Async[F].pure {
              if (agg.isEmpty) {
                // Pre-genesis fallback — equal-weight identical to [[stakeWeighted]] None path.
                if (validators.contains(peerId) && validators.nonEmpty) Ratio(1, validators.size)
                else Ratio.Zero
              } else {
                val effectiveActive = active.intersect(validators)
                val totalSeedlist = totalStakeOver(agg, validators)
                val totalActive = totalStakeOver(agg, effectiveActive)
                val activeStakeFraction =
                  if (totalSeedlist == BigInt(0)) Ratio.Zero
                  else Ratio(totalActive, totalSeedlist)
                val meetsQuorum = validators.nonEmpty && activeStakeFraction >= MinActiveQuorumFraction

                if (meetsQuorum && effectiveActive.contains(peerId) && totalActive != BigInt(0))
                  Ratio(agg.getOrElse(peerId, BigInt(0)), totalActive)
                else if (validators.contains(peerId) && validators.nonEmpty) {
                  if (totalSeedlist == BigInt(0)) Ratio.Zero
                  else Ratio(agg.getOrElse(peerId, BigInt(0)), totalSeedlist)
                } else Ratio.Zero
              }
            }
          }

        def committeeStake(peerId: PeerId): F[Ratio] =
          validatorsRef.get.map { validators =>
            if (validators.contains(peerId) && validators.nonEmpty) Ratio(1, validators.size)
            else Ratio.Zero
          }

        // §3 NIPoPoW N-2: identical fallback chain to [[stakeWeighted]] but the warmup
        // fall-through reads the live MPT aggregate (via `aggregator`) instead of the GSI maps.
        def relativeStakeAt(peerId: PeerId, etaPeriod: EtaPeriod): F[Ratio] =
          (historicalDistributionFor(etaPeriod), validatorsRef.get).flatMapN {
            case (Some(distribution), validators) =>
              Async[F].pure {
                if (!validators.contains(peerId) || validators.isEmpty) Ratio.Zero
                else distribution.relativeStakeAgainst(peerId, validators)
              }
            case (None, validators) =>
              // Warmup fall-through: use the live MPT aggregate as the stake distribution. Correct
              // during the first 2 eta periods after genesis because the genesis distribution is
              // unchanged.
              aggregator.aggregateFromMpt.map { agg =>
                if (!validators.contains(peerId) || validators.isEmpty) Ratio.Zero
                else if (agg.isEmpty) {
                  // Very-early boot: no MPT records yet → 1/N fallback.
                  Ratio(1, validators.size)
                } else {
                  val total = totalStakeOver(agg, validators)
                  if (total == BigInt(0)) Ratio.Zero
                  else Ratio(agg.getOrElse(peerId, BigInt(0)), total)
                }
              }
          }
      }
    }
}
