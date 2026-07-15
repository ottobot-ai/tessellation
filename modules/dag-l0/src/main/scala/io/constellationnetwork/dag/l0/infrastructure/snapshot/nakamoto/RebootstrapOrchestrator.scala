package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.effect.std.Semaphore
import cats.effect.syntax.all._
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.domain.nakamoto.TipTracker
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics

import eu.timepit.refined.auto._
import fs2.Stream
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Transitional re-bootstrap reset trigger (P-11, task #141) for the divergent-self-finalize lock-out documented in
  * `project_117_path_b_fork_recovery_deadlock`.
  *
  * This is not the target recovery protocol. A refuse counter is not objective fork-choice evidence, and this reset does not atomically
  * reconstruct durable selection/finality state or quarantine every follower-serving projection. Target recovery authenticates and
  * objectively selects an exact tine, journals abandonment, reconstructs from verified history, and remains fail-stopped until every
  * canonical consumer is reconciled. Keep those gaps visible when this legacy switch is enabled.
  *
  * ==The bug==
  *
  * When a node self-finalizes a divergent fork (its local view said hash H_local at ord N, but the cluster's canonical view is
  * H_canonical), the finality-safety gate in `NakamotoChainStore.store` will permanently REFUSE to overwrite the locally-finalized H_local
  * with H_canonical. P-11b (commit `95471c7f`) reduced the likelihood by excluding self-attestation from the own threshold, but cannot
  * prevent the case where:
  *
  *   - A small-cluster validator hit threshold from a SLIGHTLY-skewed set of peer attestations whose hash matched its own locally-built
  *     (but cluster-divergent) hash.
  *   - A flaky peer momentarily attested the wrong hash and was the swing vote that pushed this node over threshold on a divergent fork.
  *
  * Both manifest as a node that keeps emitting "REFUSED store" warnings every time a canonical-chain peer's snapshot arrives at the
  * already-locally-finalized ordinal.
  *
  * ==Detection==
  *
  * We use the '''refuse-counter approach''' described in task #141's "Simpler alternative": every refused different-hash write at-or-below
  * the finalized ordinal increments `chainStore.divergentRefuseCount`. When the counter exceeds `RebootstrapTriggerThreshold` over a single
  * observation window, the node has clearly been locked out (a single random peer-replay would generate at most one increment; sustained
  * sustained increments imply many peers all delivering canonical that we keep refusing).
  *
  * The counter proves only that replay-valid conflicting input reached this node below its local floor. Repeated delivery or equivocation
  * can increase it without proving that the local tine lost objective fork choice. The threshold is therefore a transitional operational
  * heuristic, not portable consensus evidence.
  *
  * ==Reset sequence==
  *
  *   1. Pause production via `productionGate.pause(RebootstrapInProgress)`. 2. Reset `tipTracker.unsafe_reset` — drop ALL attestations +
  *      last-finalized marker. 3. Reset `mptOverlay.unsafe_reset` — drop pending branches, finalized markers, undo journal. 4. Reset
  *      `chainStore.unsafe_clearFinality` — drop byHash, bestTip, lastFinalizedOrdinal, refuse counter, refuse sample.
  *
  * After reset, ordinary gossip and ChainSync cannot mutate state because the shared snapshot semaphore remains held. This legacy
  * orchestrator has no authenticated reconstruction verifier, so it remains fail-stopped. A future recovery coordinator must reconstruct
  * exact authenticated history in an isolated workspace, atomically promote the complete result, and only then release the semaphore and
  * clear the `RecoveryRequired` gate.
  *
  * NOTE: the orchestrator does NOT itself touch `lastGlobalSnapshotStorage`, `snapshotStorage`, the `MptStore` base, or every served
  * finalized projection. A delivered peer tip or self-consistent root is not authority to rewrite those stores. Until a durable recovery
  * coordinator owns isolated replay plus atomic promotion of every projection, this mechanism is containment rather than complete recovery.
  *
  * ==Enablement (currently ON; target OFF once density-past-k₁ (S3) lands + is e2e-validated)==
  *
  * Gated by typed HOCON `SharedConfig.nakamoto.rebootstrapEnabled` (`application.conf` `rebootstrap-enabled`, default `true`; env override
  * `${?NAKAMOTO_REBOOTSTRAP_ENABLED}`), passed as the `enabled` param to `run`. It ships ON as fail-stop containment: a locked-out node is
  * prevented from continuing to mutate a divergent global root, but it does not automatically recover. A complete isolated reconstruction
  * coordinator must replace this reset before permissionless operation. Once objective density recovery is end-to-end validated, this
  * heuristic reset should default OFF and remain only an explicitly unsafe/manual diagnostic trigger.
  */
object RebootstrapOrchestrator {

  /** Number of sustained `divergentRefuseCount` increments over the observation window required to trigger reset. Default 3 — a single
    * isolated refuse can happen during transient fork-recovery (e.g. peer momentarily attests the wrong branch and gossips its snapshot);
    * three in the same window is strong evidence we're permanently locked out. Override via `NAKAMOTO_REBOOTSTRAP_REFUSE_THRESHOLD`.
    */
  val RefuseThreshold: Long =
    sys.env
      .get("NAKAMOTO_REBOOTSTRAP_REFUSE_THRESHOLD")
      .flatMap(_.toLongOption)
      .getOrElse(3L)

  /** Legacy cooldown used by the trigger decision. It never resumes recovery or releases the retained semaphore. While this containment
    * remains fail-stopped after a successful reset, a second reset cannot execute; the value remains only for pre-reset/restart
    * diagnostics. Override via `NAKAMOTO_REBOOTSTRAP_COOLDOWN_MS`.
    */
  val CooldownMs: Long =
    sys.env
      .get("NAKAMOTO_REBOOTSTRAP_COOLDOWN_MS")
      .flatMap(_.toLongOption)
      .getOrElse(300000L) // 5 min

  /** Ticker cadence. 30 s is well above gossip-RTT so a divergent node's refuse counter has time to accumulate before we observe; well
    * below the typical 5-min mean time between fork-recoveries in healthy operation so a stuck node doesn't stay stuck for long.
    */
  val TickIntervalMs: Long =
    sys.env
      .get("NAKAMOTO_REBOOTSTRAP_TICK_MS")
      .flatMap(_.toLongOption)
      .getOrElse(30000L)

  // Master switch migrated to typed HOCON (`SharedConfig.nakamoto.rebootstrapEnabled`, default TRUE) — passed as the
  // `enabled` param to `run` below. Was `NAKAMOTO_REBOOTSTRAP_ENABLED` (default false); see types.scala / application.conf.

  /** Well-known pause reason for the ProductionGate. */
  val RebootstrapInProgress: String = "rebootstrap-in-progress"

  /** Fail-closed state entered after the legacy reset has discarded canonical in-memory authority. Only a future authenticated forward
    * replay coordinator may clear this reason and release the shared snapshot-mutation semaphore.
    */
  val RecoveryRequired: String = "recovery-required"

  /** Orchestrator state. `lastResetAtMs` is wall-clock; absent until first reset. */
  private final case class State(
    lastResetAtMs: Option[Long],
    totalResets: Long
  )

  private object State {
    val initial: State = State(None, 0L)
  }

  /** Decision branch for a single tick. Pure (no side effects); side-effectful steps live in `runReset`. Surfaced as a sealed ADT so tests
    * can drive the decision logic without standing up the full reset pipeline.
    */
  sealed trait Decision extends Product with Serializable
  object Decision {
    case object Quiet extends Decision
    case object Cooldown extends Decision
    case object Trigger extends Decision
  }

  /** Pure decision function — kept package-private so tests can exercise the threshold + cooldown logic without the F[_] side-effecting
    * pipeline.
    *
    * Returns:
    *   - `Quiet` if `refuseCount < threshold` (not enough evidence)
    *   - `Cooldown` if `refuseCount >= threshold` BUT a reset fired recently (within `cooldownMs`)
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

  /** Run the orchestrator as a fs2.Stream. Wires the tick → decision → optional reset sequence under a single mutex so two concurrent ticks
    * (e.g. if a tick takes longer than `TickIntervalMs`) cannot interleave a reset with itself.
    *
    * The reset sequence does the following under the gate-pause:
    *   1. `productionGate.pause(RebootstrapInProgress)` 2. `tipTracker.unsafe_reset` 3. `mptOverlay.unsafe_reset` 4.
    *      `chainStore.unsafe_clearFinality` 5. Bump `dag_nakamoto_rebootstrap_initiated_total` 6. Enter [[RecoveryRequired]] while
    *      retaining the shared snapshot-mutation semaphore. Production and ordinary gossip adoption remain fail-stopped until a future
    *      recovery coordinator reconstructs exact authenticated history, atomically promotes it, releases the semaphore, and clears the
    *      recovery gate.
    *
    * There is intentionally no ordinary post-reset re-seed: the retained semaphore blocks `NakamotoSyncDaemon` mutation. A future recovery
    * coordinator must fetch and fully replay exact ancestry in isolation, atomically promote it, then explicitly release recovery.
    *
    * When `enabled == false`, returns an empty Stream — the orchestrator is wired but dormant. NOTE: `false` is NOT the current default
    * (live default is `true`; see the enablement note above). It should default off once complete objective density recovery is
    * e2e-validated.
    */
  def run[F[_]: Async: Metrics](
    enabled: Boolean,
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    tipTracker: TipTracker[F],
    mptOverlay: MptOverlay[F, io.constellationnetwork.schema.mpt.GlobalStateKey],
    productionGate: io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate[F],
    snapshotSemaphore: Semaphore[F]
  ): Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("RebootstrapOrchestrator")

    if (!enabled)
      Stream.eval(
        logger.info(
          "RebootstrapOrchestrator is DISABLED via rebootstrap-enabled=false (NON-default; live default is true). " +
            "Divergent self-finalize will surface as REFUSED store warnings without triggering reset."
        )
      ) ++ Stream.empty
    else
      Stream
        .eval(
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
        )
        .flatMap {
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
                          snapshotSemaphore,
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
    *   1. WARN log surfaces the divergent self-finalize event (ord + canonical hash). 2. INFO log marks the reset start. 3. Production gate
    *      paused (other components may already be reading this; the pause is idempotent so re-entrant pauses don't break anything). 4.
    *      `tipTracker.unsafe_reset` first — stops further attestations from polluting the tracker before we've cleared chain state. 5.
    *      `mptOverlay.unsafe_reset` next — drops in-memory pending state. Base MptStore is NOT touched; later replayed transitions must
    *      reconcile it through the ordinary validator. 6. `chainStore.unsafe_clearFinality` last — clears the refuse counter so a future
    *      different-hash store doesn't immediately re-trip the trigger; also resets `nakamotoFinalizedOrdinalRef` to MinValue so
    *      production's "at-or-below finalized" guard treats any post-recovery ordinal as fresh. 7. INFO log marks the reset complete. 8.
    *      Enter `RecoveryRequired` and retain the shared mutation permit. This legacy reset cannot itself prove a replacement branch, so
    *      reopening production or ordinary adoption here would reintroduce unauthenticated authority.
    *
    * Whether reset succeeds or fails, ordinary production/adoption remains paused. Success enters `RecoveryRequired`; failure retains the
    * rebootstrap pause. Operators will see production stalled until authenticated recovery is available.
    */
  private def runReset[F[_]: Async: Metrics](
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    tipTracker: TipTracker[F],
    mptOverlay: MptOverlay[F, io.constellationnetwork.schema.mpt.GlobalStateKey],
    productionGate: io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate[F],
    snapshotSemaphore: Semaphore[F],
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
      _ <- withPausedSnapshotSemaphore(
        snapshotSemaphore,
        productionGate,
        logger.error(
          "RE-BOOTSTRAP canceled while waiting for snapshot semaphore; production remains paused for operator/recovery intervention"
        )
      ) {
        logger.info(s"RE-BOOTSTRAP started ($sampleStr)") >>
          tipTracker.unsafe_reset >>
          mptOverlay.unsafe_reset >>
          chainStore.unsafe_clearFinality >>
          stateRef.update(s => s.copy(lastResetAtMs = Some(nowMs), totalResets = s.totalResets + 1L)) >>
          logger.info(
            s"RE-BOOTSTRAP reset complete — chain store + tipTracker + overlay cleared; entering RecoveryRequired. " +
              s"Production and ordinary snapshot mutation remain paused until authenticated forward reconstruction."
          )
      }
    } yield ()
  }

  /** Pause production, wait for the shared snapshot mutation barrier, and run one reset atomically with respect to production, gossip
    * adoption, and finality effects.
    *
    * Cancellation while waiting for the semaphore is allowed, but deliberately leaves production paused. Once the permit is acquired, reset
    * and the success path are uncancelable. A reset failure deliberately retains the permit and leaves production paused: the producer,
    * gossip adopter, and finalizer must all fail-stop after a partial reset until operator recovery or process restart.
    *
    * Lock order is load-bearing: shared `snapshotSemaphore` first, then the component-internal locks acquired by `reset`.
    */
  private[nakamoto] def withPausedSnapshotSemaphore[F[_]: Async](
    snapshotSemaphore: Semaphore[F],
    productionGate: io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate[F],
    onAcquireCanceled: F[Unit]
  )(reset: F[Unit]): F[Unit] =
    Async[F].uncancelable { poll =>
      productionGate.pause(RebootstrapInProgress) >>
        poll(snapshotSemaphore.acquire)
          .onCancel(onAcquireCanceled)
          .flatMap { _ =>
            reset.attempt.flatMap {
              case Right(_)    => productionGate.pause(RecoveryRequired) >> productionGate.resume(RebootstrapInProgress)
              case Left(error) =>
                // Intentional permit retention. `ProductionGate` alone does not stop gossip adoption.
                Async[F].raiseError[Unit](error)
            }
          }
    }
}
