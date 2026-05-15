package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.effect.std.Semaphore
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.domain.nakamoto.TipTracker
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics._

import eu.timepit.refined.auto._
import fs2.Stream
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Re-bootstrap orchestrator (P-11, task #141). The full node-level recovery path for the
  * divergent-self-finalize lock-out documented in `project_117_path_b_fork_recovery_deadlock`.
  *
  * == The bug ==
  *
  * When a node self-finalizes a divergent fork (its local view said hash H_local at ord N,
  * but the cluster's canonical view is H_canonical), the finality-safety gate in
  * `NakamotoChainStore.store` will permanently REFUSE to overwrite the locally-finalized
  * H_local with H_canonical. P-11b (commit `95471c7f`) reduced the likelihood by excluding
  * self-attestation from the own threshold, but cannot prevent the case where:
  *
  *   - A small-cluster validator hit threshold from a SLIGHTLY-skewed set of peer
  *     attestations whose hash matched its own locally-built (but cluster-divergent) hash.
  *   - A flaky peer momentarily attested the wrong hash and was the swing vote that pushed
  *     this node over threshold on a divergent fork.
  *
  * Both manifest as a node that keeps emitting "REFUSED store" warnings every time a
  * canonical-chain peer's snapshot arrives at the already-locally-finalized ordinal.
  *
  * == Detection ==
  *
  * We use the '''refuse-counter approach''' described in task #141's "Simpler alternative":
  * every refused different-hash write at-or-below the finalized ordinal increments
  * `chainStore.divergentRefuseCount`. When the counter exceeds `RebootstrapTriggerThreshold`
  * over a single observation window, the node has clearly been locked out (a single random
  * peer-replay would generate at most one increment; sustained sustained increments imply
  * many peers all delivering canonical that we keep refusing).
  *
  * No fancy peer-ChainSync-probe is needed: the refuse-counter is a precise, conservative
  * signal because the safety gate fires ONLY on different-hash writes at-or-below finalized.
  * There is no false-positive scenario for the gate itself; the only ambiguity is whether
  * one such refuse means "transient peer flake" or "this node is stuck". Requiring K
  * sustained refuses crosses that threshold cleanly.
  *
  * == Reset sequence ==
  *
  *   1. Pause production via `productionGate.pause(RebootstrapInProgress)`.
  *   2. Reset `tipTracker.unsafe_reset` — drop ALL attestations + last-finalized marker.
  *   3. Reset `mptOverlay.unsafe_reset` — drop pending branches, finalized markers, undo journal.
  *   4. Reset `chainStore.unsafe_clearFinality` — drop byHash, bestTip, lastFinalizedOrdinal,
  *      refuse counter, refuse sample.
  *
  * After reset, normal gossip + ChainSync re-seeds the chain store with canonical snapshots.
  * Production resumes once the divergent-refuse counter stays at 0 across the cooldown window
  * (`RebootstrapCooldown`) — preventing flap if the bug recurs immediately.
  *
  * NOTE: the orchestrator does NOT itself touch `lastGlobalSnapshotStorage`, `snapshotStorage`,
  * or the `MptStore` base. The follow-on `NakamotoSyncDaemon.handleSnapshot` catch-up path is
  * responsible for those once a canonical snapshot is delivered. Resetting them here would
  * race the daemon's own canonical-rewrite path.
  *
  * == Default-OFF ==
  *
  * Gated by env `NAKAMOTO_REBOOTSTRAP_ENABLED` (default `false`). False positives are costly
  * (full chain resync), so we ship the recovery path itself in production but leave it OFF
  * until iter-level e2e validates it doesn't spuriously fire. Operators can flip it ON per-
  * node by setting the env to `true`.
  */
object RebootstrapOrchestrator {

  /** Number of sustained `divergentRefuseCount` increments over the observation window required
    * to trigger reset. Default 3 — a single isolated refuse can happen during transient
    * fork-recovery (e.g. peer momentarily attests the wrong branch and gossips its snapshot);
    * three in the same window is strong evidence we're permanently locked out. Override via
    * `NAKAMOTO_REBOOTSTRAP_REFUSE_THRESHOLD`.
    */
  val RefuseThreshold: Long =
    sys.env
      .get("NAKAMOTO_REBOOTSTRAP_REFUSE_THRESHOLD")
      .flatMap(_.toLongOption)
      .getOrElse(3L)

  /** Cooldown after a reset before another reset can fire. Prevents flap if the bug recurs
    * (e.g. ChainSync re-seeds something divergent immediately). Default 5 minutes — long
    * enough for a peer's canonical chain to refill us, short enough that operators don't
    * have to restart the node if a second divergence happens. Override via
    * `NAKAMOTO_REBOOTSTRAP_COOLDOWN_MS`.
    */
  val CooldownMs: Long =
    sys.env
      .get("NAKAMOTO_REBOOTSTRAP_COOLDOWN_MS")
      .flatMap(_.toLongOption)
      .getOrElse(300000L) // 5 min

  /** Ticker cadence. 30 s is well above gossip-RTT so a divergent node's refuse counter
    * has time to accumulate before we observe; well below the typical 5-min mean time between
    * fork-recoveries in healthy operation so a stuck node doesn't stay stuck for long.
    */
  val TickIntervalMs: Long =
    sys.env
      .get("NAKAMOTO_REBOOTSTRAP_TICK_MS")
      .flatMap(_.toLongOption)
      .getOrElse(30000L)

  /** Master switch. Default `false`. Production deployments turn this on per-node once iter-
    * level e2e proves it doesn't spuriously fire under normal small-cluster operation.
    */
  val Enabled: Boolean =
    sys.env
      .get("NAKAMOTO_REBOOTSTRAP_ENABLED")
      .flatMap(_.toBooleanOption)
      .getOrElse(false)

  /** Well-known pause reason for the ProductionGate. */
  val RebootstrapInProgress: String = "rebootstrap-in-progress"

  /** Orchestrator state. `lastResetAtMs` is wall-clock; absent until first reset. */
  private final case class State(
    lastResetAtMs: Option[Long],
    totalResets: Long
  )

  private object State {
    val initial: State = State(None, 0L)
  }

  /** Decision branch for a single tick. Pure (no side effects); side-effectful steps live
    * in `runReset`. Surfaced as a sealed ADT so tests can drive the decision logic
    * without standing up the full reset pipeline.
    */
  sealed trait Decision extends Product with Serializable
  object Decision {
    case object Quiet extends Decision
    case object Cooldown extends Decision
    case object Trigger extends Decision
  }

  /** Pure decision function — kept package-private so tests can exercise the threshold +
    * cooldown logic without the F[_] side-effecting pipeline.
    *
    * Returns:
    *   - `Quiet` if `refuseCount < threshold` (not enough evidence)
    *   - `Cooldown` if `refuseCount >= threshold` BUT a reset fired recently (within
    *     `cooldownMs`)
    *   - `Trigger` if `refuseCount >= threshold` AND cooldown is clear
    */
  private[nakamoto] def decide(
    refuseCount: Long,
    threshold: Long,
    lastResetAtMs: Option[Long],
    nowMs: Long,
    cooldownMs: Long
  ): Decision =
    if (refuseCount < threshold) Decision.Quiet
    else
      lastResetAtMs match {
        case Some(t) if (nowMs - t) < cooldownMs => Decision.Cooldown
        case _                                   => Decision.Trigger
      }

  /** Run the orchestrator as a fs2.Stream. Wires the tick → decision → optional reset
    * sequence under a single mutex so two concurrent ticks (e.g. if a tick takes longer
    * than `TickIntervalMs`) cannot interleave a reset with itself.
    *
    * The reset sequence does the following under the gate-pause:
    *   1. `productionGate.pause(RebootstrapInProgress)`
    *   2. `tipTracker.unsafe_reset`
    *   3. `mptOverlay.unsafe_reset`
    *   4. `chainStore.unsafe_clearFinality`
    *   5. Bump `dag_nakamoto_rebootstrap_initiated_total`
    *   6. `productionGate.resume(RebootstrapInProgress)` — production reopens immediately;
    *      the snapshot-leader-loop's slot-win guard already refuses to produce at-or-below
    *      finalized, and after the reset finalized=0 with chainStore empty, so production
    *      cannot mint anything until gossip / ChainSync delivers genesis-like state.
    *
    * The post-reset chain re-seed is the responsibility of `NakamotoSyncDaemon.handleSnapshot`:
    * the next gossip snapshot will trigger its catch-up path (parent-not-found → Tier 3 →
    * `catchUpFromGossip` → full state resync).
    *
    * When `Enabled == false`, returns an empty Stream — the orchestrator is wired but
    * dormant. This is the safe default until production validates the trigger is precise.
    */
  def run[F[_]: Async: Metrics](
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    tipTracker: TipTracker[F],
    mptOverlay: MptOverlay[F, io.constellationnetwork.schema.mpt.GlobalStateKey],
    productionGate: io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate[F]
  ): Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("RebootstrapOrchestrator")

    if (!Enabled)
      Stream.eval(
        logger.info(
          "RebootstrapOrchestrator is DISABLED via NAKAMOTO_REBOOTSTRAP_ENABLED=false (default). " +
            "Divergent self-finalize will surface as REFUSED store warnings without triggering reset."
        )
      ) ++ Stream.empty
    else
      Stream.eval(
        (
          Ref.of[F, State](State.initial),
          Semaphore[F](1L),
          logger
            .info(
              s"RebootstrapOrchestrator is ENABLED: tick=${TickIntervalMs}ms threshold=${RefuseThreshold} " +
                s"cooldown=${CooldownMs}ms"
            )
            .as(())
        ).tupled
      ).flatMap {
        case (stateRef, mutex, _) =>
          Stream
            .awakeEvery[F](FiniteDuration(TickIntervalMs, MILLISECONDS))
            .evalMap { _ =>
              // Serialize ticks so two overlapping invocations (e.g. ticker fired during a
              // long reset) can't double-reset.
              mutex.permit.use { _ =>
                for {
                  refuseCount <- chainStore.divergentRefuseCount
                  sample <- chainStore.divergentRefuseSample
                  st <- stateRef.get
                  now <- Async[F].realTime.map(_.toMillis)
                  d = decide(refuseCount, RefuseThreshold, st.lastResetAtMs, now, CooldownMs)
                  _ <- d match {
                    case Decision.Quiet =>
                      if (refuseCount > 0)
                        logger.debug(
                          s"REBOOTSTRAP tick: refuseCount=$refuseCount below threshold=$RefuseThreshold; no action"
                        )
                      else
                        Async[F].unit
                    case Decision.Cooldown =>
                      logger.info(
                        s"REBOOTSTRAP tick: refuseCount=$refuseCount >= threshold=$RefuseThreshold " +
                          s"but in cooldown (last reset at ${st.lastResetAtMs.getOrElse(0L)}, " +
                          s"now=$now, cooldown=${CooldownMs}ms); skipping"
                      )
                    case Decision.Trigger =>
                      runReset(
                        chainStore,
                        tipTracker,
                        mptOverlay,
                        productionGate,
                        refuseCount,
                        sample,
                        now,
                        stateRef,
                        logger
                      )
                  }
                } yield ()
              }
            }
      }
  }

  /** Execute the reset sequence. Side-effecting; called under the orchestrator's mutex.
    *
    * Layout matches the design in task #141:
    *   1. WARN log surfaces the divergent self-finalize event (ord + canonical hash).
    *   2. INFO log marks the reset start.
    *   3. Production gate paused (other components may already be reading this; the pause
    *      is idempotent so re-entrant pauses don't break anything).
    *   4. `tipTracker.unsafe_reset` first — stops further attestations from polluting the
    *      tracker before we've cleared chain state.
    *   5. `mptOverlay.unsafe_reset` next — drops in-memory pending state. Base MptStore is
    *      NOT touched; the post-reset gossip catch-up will resync base.
    *   6. `chainStore.unsafe_clearFinality` last — clears the refuse counter so a future
    *      different-hash store doesn't immediately re-trip the trigger; also resets
    *      `nakamotoFinalizedOrdinalRef` to MinValue so production's "at-or-below finalized"
    *      guard treats any post-catch-up ordinal as fresh.
    *   7. INFO log marks the reset complete.
    *   8. Production gate resumed — the slot-win guard handles the rest.
    *
    * If any step throws, the gate is left paused (the orchestrator's resume() is in the
    * happy path). Operators will see production stalled and can intervene; that's safer
    * than auto-resuming with half-reset state.
    */
  private def runReset[F[_]: Async: Metrics](
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    tipTracker: TipTracker[F],
    mptOverlay: MptOverlay[F, io.constellationnetwork.schema.mpt.GlobalStateKey],
    productionGate: io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate[F],
    refuseCount: Long,
    sample: Option[(Long, io.constellationnetwork.security.hash.Hash)],
    nowMs: Long,
    stateRef: Ref[F, State],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    val sampleStr =
      sample.fold("none") { case (ord, h) => s"ord=$ord canonical=${h.value.take(12)}" }
    for {
      _ <- logger.warn(
        s"⚠️ DIVERGENT-SELF-FINALIZE detected: refuseCount=$refuseCount >= threshold=$RefuseThreshold " +
          s"(sample: $sampleStr) — triggering RE-BOOTSTRAP"
      )
      _ <- Metrics[F].incrementCounter("dag_nakamoto_rebootstrap_initiated_total")
      _ <- productionGate.pause(RebootstrapInProgress)
      _ <- logger.info(s"RE-BOOTSTRAP started ($sampleStr)")
      _ <- tipTracker.unsafe_reset
      _ <- mptOverlay.unsafe_reset
      _ <- chainStore.unsafe_clearFinality
      _ <- stateRef.update(s => s.copy(lastResetAtMs = Some(nowMs), totalResets = s.totalResets + 1L))
      _ <- logger.info(
        s"RE-BOOTSTRAP complete — chain store + tipTracker + overlay reset; awaiting canonical " +
          s"chain re-seed via gossip/ChainSync. Production gate will reopen after this method returns."
      )
      _ <- productionGate.resume(RebootstrapInProgress)
    } yield ()
  }
}
