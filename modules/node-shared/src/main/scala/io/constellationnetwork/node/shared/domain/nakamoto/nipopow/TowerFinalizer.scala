package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.effect.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.auto._
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** §3 NIPoPoW S3 — Phase-3 sink that grows the local [[TowerStore]] from finalized snapshots.
  *
  * Called at the `T_depth2.advance` boundary in `SnapshotLeaderLoop.finalityMonitor` for every newly archival-finalized
  * `GlobalIncrementalSnapshot`. For each super-level µ ∈ {1..L-1}:
  *
  *   - looks up `tower.latestAt(µ).ordinal` (or 0 if none — every snapshot starts with `g_µ = ord - 0`)
  *   - derives `g_µ = ord - lastLevelMuOrdinal` (snapshots since previous level-µ hit, in ordinal-units NOT slot-units)
  *   - derives `δ_S = slot - parentSlot` from the snapshot's slotCertificate
  *   - runs `LevelTrialComputer.runAll(vrfOutput, gapsPerLevel, δ_S, γ)`
  *   - appends per-level passes via `tower.appendAtFinality(ord, snapshotHash, trials)`
  *
  * '''Strict-forward, idempotent.''' The finalizer is called with monotonically-increasing ordinals (driven by `lastArchivalOrdinalRef`
  * advancing strictly forward). A snapshot replayed against a tower that already includes its passes is a no-op because the underlying MPT
  * `insert` with the same `(level, ord)` key just overwrites with the same `snapshotHash` value, and the per-level computations are
  * deterministic.
  *
  * '''No-cert snapshots are skipped.''' Pre-activation snapshots have `slotCertificate = None` and `eta = None`; we skip them with a debug
  * log line — they contribute no level-µ hits because there's no `ρ_S` to rehash.
  */
trait TowerFinalizer[F[_]] {

  /** Compute and append super-level passes for a single finalized snapshot. See class docstring. */
  def finalize(snapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit]

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

object TowerFinalizer {

  /** Construct a [[TowerFinalizer]] backed by a [[TowerStore]] and a [[LevelTrialComputer]].
    *
    * @param tower
    *   sink — the local per-node store the finalizer writes into.
    * @param computer
    *   pure computer for the L-1 level-µ trials. Same instance the producer can use offline; the trial is deterministic given `(ρ_S, g_µ,
    *   δ_S, γ)`.
    * @param lddCutoff
    *   `γ` for the L0 slot-gap gating multiplier `min(1, δ_S/γ)`. Sourced from `LddConfig.lddCutoff` at construction (typically 15).
    */
  def make[F[_]: Async: Metrics](
    tower: TowerStore[F],
    computer: LevelTrialComputer[F],
    lddCutoff: Long
  ): F[TowerFinalizer[F]] =
    Ref.of[F, SnapshotOrdinal](SnapshotOrdinal.MinValue).map { highWaterMarkRef =>
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

        def finalize(snapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit] =
          snapshot.signed.value.slotCertificate match {
            case None =>
              logger.debug(
                s"[TowerFinalizer] Skip ord=${snapshot.ordinal.value.value} hash=${snapshot.hash.value.take(16)} — no slotCertificate (pre-activation snapshot)"
              )
            case Some(cert) =>
              val deltaSlot = cert.slot.value.value - cert.parentSlot.value.value
              finalizeFromParts(snapshot.ordinal, snapshot.hash, cert.vrfOutput.toBytes, deltaSlot).void
          }

        def finalizeFromParts(
          ordinal: SnapshotOrdinal,
          snapshotHash: Hash,
          vrfOutput: Array[Byte],
          deltaSlot: Long
        ): F[Vector[LevelTrial]] =
          highWaterMarkRef.get.flatMap { lastFinalized =>
            if (ordinal.value.value <= lastFinalized.value.value && lastFinalized.value.value > 0L)
              logger
                .debug(
                  s"[TowerFinalizer] Skip ord=${ordinal.value.value} — already at-or-below high-water-mark ${lastFinalized.value.value}"
                )
                .as(Vector.empty[LevelTrial])
            else
              for {
                // Per-level base-block gap g_µ = ord - (last level-µ ordinal, or 0 if none).
                //
                // Important: gaps are computed in ordinal units, not slot units (proposal §2.1).
                // The producer-side equivalent in the next slice (S2 phase 2c, deferred) will track these gaps
                // incrementally; here we recompute from the store on each finalize, which is fine because
                // T_depth2 fires once per snapshot at Phase-3 and the read is a fixed L-1 prefix-scans.
                gaps <- (1 to SuperLevelParams.SuperLevelCount).toVector.traverse { µ =>
                  tower.latestAt(µ).map { latest =>
                    val baseOrd = latest.map(_.ordinal.value.value).getOrElse(0L)
                    ordinal.value.value - baseOrd
                  }
                }
                trials <- computer.runAll(vrfOutput, gaps, deltaSlot, lddCutoff)
                anyPassed = trials.exists(_.passed)
                _ <- tower.appendAtFinality(ordinal, snapshotHash, trials).whenA(anyPassed)
                _ <- highWaterMarkRef.set(ordinal)
                _ <- emitTrialMetrics(trials)
                _ <- logger
                  .debug(
                    s"[TowerFinalizer] Appended ord=${ordinal.value.value} levels=${trials.collect { case t if t.passed => t.level }
                        .mkString(",")} " +
                      s"deltaSlot=$deltaSlot gaps=${gaps.mkString(",")}"
                  )
                  .whenA(anyPassed)
              } yield trials
          }
      }
    }

  /** No-op finalizer for layers that don't run the Phase-3 sink (cl0/dl1/etc). Allows the call site to wire a single value without
    * importing the trait directly.
    */
  def noop[F[_]: Async]: TowerFinalizer[F] = new TowerFinalizer[F] {
    def finalize(snapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit] = {
      val _ = snapshot
      Async[F].unit
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
