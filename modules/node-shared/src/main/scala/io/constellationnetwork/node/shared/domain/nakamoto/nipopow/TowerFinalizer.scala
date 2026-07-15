package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.effect.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.auto._
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Local NIPoPoW updater that grows [[TowerStore]] from tower-eligible snapshots.
  *
  * This component is staged but deliberately not wired into the live finality monitor. Production activation requires a bounded,
  * branch-relative exact-hash catch-up target and durable tower/cursor recovery. For each super-level µ in 1..L-1:
  *
  *   - looks up `tower.latestAt(µ).ordinal` (or 0 if none — every snapshot starts with `g_µ = ord - 0`)
  *   - derives `g_µ = ord - lastLevelMuOrdinal` (snapshots since previous level-µ hit, in ordinal-units NOT slot-units)
  *   - derives `δ_S = slot - parentSlot` from the snapshot's slotCertificate
  *   - runs `LevelTrialComputer.runAll(vrfOutput, gapsPerLevel, δ_S, γ)`
  *   - appends per-level passes via `tower.appendAtFinality(ord, snapshotHash, trials)`
  *
  * '''Strict-forward, idempotent.''' The staged exact-hash coordinator supplies one branch in monotonically increasing ordinal order. The
  * local high-water mark makes a replay at or below the latest committed ordinal a no-op. A same-ordinal branch replacement is not an
  * ordinal replay: the coordinator enters `RebuildRequired` before invoking this component.
  *
  * '''No-cert snapshots are skipped.''' Pre-activation snapshots have `slotCertificate = None` and `eta = None`; we skip them with a debug
  * log line — they contribute no level-µ hits because there's no `ρ_S` to rehash.
  */
trait TowerFinalizer[F[_]] {

  /** Perform all cancelable reads and level-trial computation without mutating tower state. The returned commit is the minimal local
    * publication step which the tower coordinator masks together with its exact-hash cursor.
    */
  def prepare(snapshot: Hashed[GlobalIncrementalSnapshot]): F[PreparedTowerFinalization[F]]

  /** Compute and append super-level passes for a single finalized snapshot. See class docstring. */
  def finalize(snapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit]

  def prepareFromParts(
    ordinal: SnapshotOrdinal,
    snapshotHash: Hash,
    vrfOutput: Array[Byte],
    deltaSlot: Long
  ): F[PreparedTowerFinalization[F]]

  /** Pure inner method exposed for unit testing — operates directly on the ingredients the wire-level [[finalize]] extracts from a
    * `Hashed[GlobalIncrementalSnapshot]`. Synthetic snapshot tests use this to bypass the full schema construction.
    *
    *   - `ordinal` — this snapshot's ordinal
    *   - `snapshotHash` — content-address of this snapshot, used as the per-level entry value
    *   - `vrfOutput` — `ρ_S` (64 bytes); equivalent to `slotCertificate.vrfOutput.toBytes`
    *   - `deltaSlot` — `cert.slot - cert.parentSlot`
    */
  def finalizeFromParts(
    ordinal: SnapshotOrdinal,
    snapshotHash: Hash,
    vrfOutput: Array[Byte],
    deltaSlot: Long
  ): F[Vector[LevelTrial]]
}

/** A non-mutating tower computation and its minimal publication action. `commit` is idempotent against the finalizer's current high-water
  * mark and returns the new level-0 count only when it publishes this item. `observe` is non-authoritative telemetry which callers run
  * after publishing their matching exact-hash cursor.
  */
final case class PreparedTowerFinalization[F[_]](
  trials: Vector[LevelTrial],
  commit: F[Option[Long]],
  observe: Long => F[Unit]
)

object TowerFinalizer {

  /** Construct a [[TowerFinalizer]] backed by a [[TowerStore]] and a [[LevelTrialComputer]].
    *
    * @param tower
    *   sink — the local per-node store the finalizer writes into.
    * @param computer
    *   pure computer for the L-1 level-µ trials. Same instance the producer can use offline; the trial is deterministic given `(ρ_S, g_µ,
    *   δ_S, γ)`.
    * @param lddCutoff
    *   `γ` for the L0 slot-gap gating multiplier `min(1, δ_S/γ)`. Sourced from `LddConfig.lddCutoff` (the bound consensus config; no
    *   source-level default).
    */
  def make[F[_]: Async: Metrics](
    tower: TowerStore[F],
    computer: LevelTrialComputer[F],
    lddCutoff: Long
  ): F[TowerFinalizer[F]] =
    (Ref.of[F, Option[SnapshotOrdinal]](none), Ref.of[F, Long](0L)).tupled.map {
      case (highWaterMarkRef, finalizeCountRef) =>
        new TowerFinalizer[F] {

          private val logger = Slf4jLogger.getLoggerFromName[F]("TowerFinalizer")

          /** Per-super-level trial outcome counters — drive the dashboard's NIPoPoW level-µ panel. Two counters per level: `trial_total`
            * (denominator) + `trial_pass_total` (numerator) → trivial Prometheus rate-ratio for the per-level pass-rate. Cardinality:
            * `SuperLevelCount × cluster-size` = 6 × 8 = 48 series for the cluster — negligible.
            *
            * The `snapshot_ordinal` label is intentionally OMITTED here. NIPoPoW pass-rates are a *statistical* property of the chain —
            * pinning a series per ordinal would explode cardinality (8000+ ordinals/run) for no benefit beyond what an aggregate
            * rate-over-window already gives.
            */
          private def emitTrialMetrics(trials: Vector[LevelTrial]): F[Unit] =
            trials.traverse_ { t =>
              val levelTag = Seq(Metrics.unsafeLabelName("level") -> t.level.toString)
              Metrics[F].incrementCounter("dag_nakamoto_nipopow_level_trial_total", levelTag) >>
                (if (t.passed)
                   Metrics[F].incrementCounter("dag_nakamoto_nipopow_level_trial_pass_total", levelTag)
                 else Async[F].unit)
            } >> {
              val passedCount = trials.count(_.passed)
              Metrics[F].updateGauge("dag_nakamoto_nipopow_levels_passed", passedCount.toLong)
            }

          /** Per-super-level tower density gauge — drives the dashboard `dag_nakamoto_tower_density_relative_error{level}` panel deferred
            * from the S5 dashboard revamp (`docs/nakamoto/METRICS-DASHBOARD-REVAMP.md` § "Deferred (follow-ups)").
            *
            * Sampled once per archival-finalize call (immediately after `tower.appendAtFinality`) — that's exactly when the tower state has
            * advanced, so the gauge always reflects the most-recent cumulative-count snapshot.
            *
            * '''Density definition''' (proposal §5.3): `observedDensity = cumulativeCount(µ) / level0Reference`; `targetDensity =
            * SuperLevelParams.Levels(µ-1).targetDensity = 1/2^µ`. The relative error is the [[DensityChecker.relativeError]] of the two.
            *
            * '''L0 reference choice.''' We use the *finalize count* (number of monotonic-forward `finalizeFromParts` calls so far, after
            * the current call). This matches "every snapshot is a level-0 hit by chain construction" — pre-activation snapshots without
            * slotCertificates flow through `finalize` and are excluded at the `slotCertificate=None` branch, never reaching
            * `finalizeFromParts`, so they aren't counted in the L0 reference either.
            *
            * '''Edge case — empty tower.''' For the first few finalizes a level-µ count is typically 0 with a non-zero target → the
            * relative error is `target/target = 1.0` (100% miss). That's [[DensityChecker.relativeError]]'s intended defensive contract; we
            * report it as-is. The dashboard's 5% threshold annotation will visibly mark this as "warm-up" until the chain has enough length
            * for the level-µ trials to converge.
            *
            * '''Cardinality.''' `SuperLevelCount × cluster-size` = 9 × 8 = 72 series for an 8-node cluster — negligible. No
            * `snapshot_ordinal` label.
            *
            * @param totalLevel0
            *   the L0 reference length to divide by — `finalizeCount` after this finalize. Must be `≥ 1` (we only call this method on the
            *   non-skipped path, so the counter has already been incremented past 0).
            */
          private def emitDensityMetrics(totalLevel0: Long): F[Unit] =
            (1 to SuperLevelParams.SuperLevelCount).toVector.traverse_ { µ =>
              val param = SuperLevelParams.Levels(µ - 1)
              val levelTag = Seq(Metrics.unsafeLabelName("level") -> µ.toString)
              tower.cumulativeCount(µ).flatMap { count =>
                val relErr = DensityChecker.relativeError(count, totalLevel0, param.targetDensity)
                Metrics[F].updateGauge("dag_nakamoto_tower_density_relative_error", relErr.toDouble, levelTag)
              }
            }

          def prepare(snapshot: Hashed[GlobalIncrementalSnapshot]): F[PreparedTowerFinalization[F]] =
            snapshot.signed.value.slotCertificate match {
              case None =>
                logger
                  .debug(
                    s"[TowerFinalizer] Skip ord=${snapshot.ordinal.value.value} hash=${snapshot.hash.value.take(16)} — no slotCertificate (pre-activation snapshot)"
                  )
                  .attempt
                  .void
                  .as(PreparedTowerFinalization(Vector.empty, none[Long].pure[F], _ => Async[F].unit))
              case Some(cert) =>
                val deltaSlot = cert.slot.value.value - cert.parentSlot.value.value
                prepareFromParts(snapshot.ordinal, snapshot.hash, cert.vrfOutput.toBytes, deltaSlot)
            }

          def finalize(snapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit] =
            prepare(snapshot).flatMap(runPrepared).void

          def prepareFromParts(
            ordinal: SnapshotOrdinal,
            snapshotHash: Hash,
            vrfOutput: Array[Byte],
            deltaSlot: Long
          ): F[PreparedTowerFinalization[F]] =
            highWaterMarkRef.get.flatMap { lastFinalized =>
              if (lastFinalized.exists(last => ordinal.value.value <= last.value.value))
                logger
                  .debug(
                    s"[TowerFinalizer] Skip ord=${ordinal.value.value} — already at-or-below high-water-mark ${lastFinalized.map(_.value.value).getOrElse(0L)}"
                  )
                  .attempt
                  .void
                  .as(PreparedTowerFinalization(Vector.empty, none[Long].pure[F], _ => Async[F].unit))
              else
                for {
                  // Per-level base-block gap g_µ = ord - (last level-µ ordinal, or 0 if none).
                  //
                  // Important: gaps are computed in ordinal units, not slot units (proposal §2.1).
                  // The producer-side equivalent in the next slice (S2 phase 2c, deferred) will track these gaps
                  // incrementally; here we recompute from the store on each finalize, which is fine because
                  // The staged catch-up coordinator is the intended single writer and visits an exact branch
                  // oldest-first; this read remains a fixed set of per-level index lookups.
                  gaps <- (1 to SuperLevelParams.SuperLevelCount).toVector.traverse { µ =>
                    tower.latestAt(µ).map { latest =>
                      val baseOrd = latest.map(_.ordinal.value.value).getOrElse(0L)
                      ordinal.value.value - baseOrd
                    }
                  }
                  trials <- computer.runAll(vrfOutput, gaps, deltaSlot, lddCutoff)
                  commit = highWaterMarkRef.get.flatMap { current =>
                    if (current.exists(last => ordinal.value.value <= last.value.value))
                      none[Long].pure[F]
                    else {
                      val anyPassed = trials.exists(_.passed)
                      tower.appendAtFinality(ordinal, snapshotHash, trials).whenA(anyPassed) >>
                        highWaterMarkRef.set(ordinal.some) >>
                        finalizeCountRef.updateAndGet(_ + 1L).map(_.some)
                    }
                  }
                  observe = (level0Ref: Long) =>
                    emitTrialMetrics(trials) >>
                      emitDensityMetrics(level0Ref) >>
                      logger
                        .debug(
                          s"[TowerFinalizer] Appended ord=${ordinal.value.value} levels=${trials.collect {
                              case t if t.passed => t.level
                            }.mkString(",")} " +
                            s"deltaSlot=$deltaSlot gaps=${gaps.mkString(",")}"
                        )
                        .whenA(trials.exists(_.passed))
                } yield PreparedTowerFinalization(trials, commit, observe)
            }

          def finalizeFromParts(
            ordinal: SnapshotOrdinal,
            snapshotHash: Hash,
            vrfOutput: Array[Byte],
            deltaSlot: Long
          ): F[Vector[LevelTrial]] =
            prepareFromParts(ordinal, snapshotHash, vrfOutput, deltaSlot).flatMap(runPrepared)

          private def runPrepared(prepared: PreparedTowerFinalization[F]): F[Vector[LevelTrial]] =
            Async[F]
              .uncancelable(_ => prepared.commit)
              .flatMap {
                case None              => Vector.empty[LevelTrial].pure[F]
                case Some(level0Count) => prepared.observe(level0Count).attempt.void.as(prepared.trials)
              }
        }
    }

  /** No-op finalizer for layers that do not maintain the local tower partition. Allows the call site to wire a single value without
    * importing the trait directly.
    */
  def noop[F[_]: Async]: TowerFinalizer[F] = new TowerFinalizer[F] {
    def prepare(snapshot: Hashed[GlobalIncrementalSnapshot]): F[PreparedTowerFinalization[F]] = {
      val _ = snapshot
      PreparedTowerFinalization(Vector.empty, none[Long].pure[F], _ => Async[F].unit).pure[F]
    }

    def finalize(snapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit] = {
      val _ = snapshot
      Async[F].unit
    }

    def prepareFromParts(
      ordinal: SnapshotOrdinal,
      snapshotHash: Hash,
      vrfOutput: Array[Byte],
      deltaSlot: Long
    ): F[PreparedTowerFinalization[F]] = {
      val _ = (ordinal, snapshotHash, vrfOutput, deltaSlot)
      PreparedTowerFinalization(Vector.empty, none[Long].pure[F], _ => Async[F].unit).pure[F]
    }

    def finalizeFromParts(
      ordinal: SnapshotOrdinal,
      snapshotHash: Hash,
      vrfOutput: Array[Byte],
      deltaSlot: Long
    ): F[Vector[LevelTrial]] = {
      val _ = (ordinal, snapshotHash, vrfOutput, deltaSlot)
      Vector.empty[LevelTrial].pure[F]
    }
  }
}
