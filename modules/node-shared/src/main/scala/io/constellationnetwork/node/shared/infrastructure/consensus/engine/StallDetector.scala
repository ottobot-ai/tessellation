package io.constellationnetwork.node.shared.infrastructure.consensus.engine

import cats.Eq
import cats.effect.kernel._
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.infrastructure.consensus.state._
import io.constellationnetwork.node.shared.infrastructure.consensus.{ConsensusLog, ConsensusResources}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.peer.{PeerId, PeerResponsiveness, Unresponsive}

import eu.timepit.refined.auto._

/** Monitors a consensus round for stalls and manages recovery.
  *
  * ==Architecture==
  *
  * StallDetector is the orchestrator that polls state periodically and delegates to focused components:
  *   - '''ViewChangeManager''': deterministic leader re-election on proposal stalls
  *   - '''AbandonmentTracker''': consecutive failure tracking, resource cleanup, recovery download
  *   - '''ConsensusHealthStatus''': observable health snapshot for HTTP endpoint + metrics
  *
  * ==Stall Detection Flow==
  * {{{
  *   Poll (100ms-1000ms adaptive)
  *     → Detect status/resource changes → queue CheckUpdate
  *     → Calculate phase-adaptive timeout
  *     → If leader unresponsive → early view change (ViewChangeManager)
  *     → If timeout exceeded:
  *         → Proposal phase: view change (ViewChangeManager)
  *         → Other phases: count toward abandon
  *     → After maxStallCycles or maxRoundDuration → abandon (AbandonmentTracker)
  *     → Update health snapshot on each cycle
  * }}}
  */
@scala.annotation.nowarn("msg=type parameter Outcome.*shadows")
class StallDetector[F[_]: Async: Metrics, Event, Key: Eq, Artifact, Ctx, Status, Outcome, Kind](
  ctx: ConsensusEngineContext[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind],
  viewChangeManager: ViewChangeManager[F, Key, Status, Outcome, Kind],
  abandonmentTracker: AbandonmentTracker[F, Event, Key, Artifact, Ctx, Status, Outcome, Kind],
  healthRef: Ref[F, ConsensusHealthStatus]
) {

  import ctx.{clusterStorage, config, logger, ops, peerQualityTracker, queue, storage}

  private case class MonitorState(
    lastResourcesHash: Int,
    lastStatus: Option[Status],
    statusStartTime: FiniteDuration,
    roundStartTime: FiniteDuration,
    noChangeCount: Int,
    stallCount: Int,
    lastSummaryTime: FiniteDuration,
    lastScoreLogTime: FiniteDuration
  )

  private val basePollInterval = 100L
  private val maxPollInterval = 1000L

  private case class ResourcesInfo(hash: Int, declaredCount: Int, activeCount: Int, missingPeerIds: Set[String], missingPeers: Set[PeerId])

  def monitor(key: Key, cancelSignal: Deferred[F, Unit]): F[Unit] =
    for {
      now <- Async[F].monotonic
      _ <- Async[F].race(
        cancelSignal.get,
        Async[F].tailRecM(
          MonitorState(
            lastResourcesHash = 0,
            lastStatus = None,
            statusStartTime = now,
            roundStartTime = now,
            noChangeCount = 0,
            stallCount = 0,
            lastSummaryTime = now,
            lastScoreLogTime = now
          )
        )(monitorStep(key, _))
      )
    } yield ()

  private def monitorStep(key: Key, ms: MonitorState): F[Either[MonitorState, Unit]] =
    storage.getState(key).flatMap {
      case None =>
        ConsensusLog.debug(logger, ConsensusLog.Lifecycle, key.toString, "n/a", "event" -> "MONITOR_STATE_GONE") >>
          healthRef.update(_.copy(isRunning = false, key = None, phase = None)) >>
          Async[F].pure(Right(()))

      case Some(state) =>
        ctx.advancer.getConsensusOutcome(state) match {
          case Some(_) =>
            ConsensusLog.debug(logger, ConsensusLog.Lifecycle, key.toString, "n/a", "event" -> "MONITOR_OUTCOME_READY") >>
              Async[F].pure(Right(()))

          case None =>
            runMonitorCycle(key, ms, state)
        }
    }

  /** Core monitoring cycle: detect changes, check timeouts, handle stalls, update health. */
  private def runMonitorCycle(
    key: Key,
    ms: MonitorState,
    state: ConsensusState[Key, Status, Outcome, Kind]
  ): F[Either[MonitorState, Unit]] =
    for {
      now <- Async[F].monotonic
      resources <- storage.getResources(key)

      info = getResourcesInfo(state, resources)
      statusChanged = !ms.lastStatus.contains(state.status)
      resourcesChanged = info.hash != ms.lastResourcesHash

      newStatusStartTime = if (statusChanged) now else ms.statusStartTime
      statusDuration = now - newStatusStartTime
      newStallCount = if (statusChanged) 0 else ms.stallCount

      _ <- queue.offer(ConsensusCommand.CheckUpdate(key)).whenA(resourcesChanged || statusChanged)

      // --- Timeout calculation ---
      effectiveTimeout <- calculateTimeout(ms.stallCount, info, state)

      // --- Early view change for unresponsive leader ---
      leaderUnresponsive <- isLeaderUnresponsive(state.leader)
      earlyViewChange = leaderUnresponsive && ops.isProposalPhase(state.status) && ms.stallCount == 0
      _ <- (
        ConsensusLog.warn(
          logger,
          ConsensusLog.Stall,
          key.toString,
          "n/a",
          "event" -> "EARLY_VIEW_CHANGE",
          "leader" -> ConsensusLog.pid(state.leader),
          "reason" -> "leader_unresponsive"
        ) >>
          viewChangeManager.performViewChange(key, state)
      ).whenA(earlyViewChange)

      // --- Handle stall: view change for proposal phase, count toward abandon for others ---
      didStall <-
        if (earlyViewChange) true.pure[F]
        else
          handleStall(
            key = key,
            state = state,
            declarationTimeout = effectiveTimeout,
            statusDuration = statusDuration,
            declaredCount = info.declaredCount,
            activeCount = info.activeCount,
            missingPeerIds = info.missingPeerIds
          )

      adjustedStatusStartTime = if (didStall) now else newStatusStartTime
      finalStallCount = if (didStall) newStallCount + 1 else newStallCount

      _ <- Metrics[F].updateGauge("dag_consensus_stall_cycle", finalStallCount)

      declarationProgress = if (info.activeCount > 0) info.declaredCount.toDouble / info.activeCount else 0.0
      _ <- Metrics[F].updateGauge("dag_consensus_stall_declaration_progress", declarationProgress)

      // --- Round timeout / abandon check ---
      roundElapsed = now - ms.roundStartTime
      _ <- Metrics[F].updateGauge("dag_consensus_round_elapsed_seconds", roundElapsed.toSeconds.toInt)
      roundTimedOut = config.maxRoundDuration.exists(roundElapsed >= _)
      shouldAbandon = finalStallCount >= config.maxStallCycles || roundTimedOut

      abandonReason =
        if (roundTimedOut)
          s"round timed out after ${roundElapsed.toSeconds}s (max=${config.maxRoundDuration.map(_.toSeconds)}s)"
        else s"stuck after $finalStallCount stall cycles"
      abandonReasonLabel = if (roundTimedOut) "timeout" else "max_stalls"

      _ <- (
        peerQualityTracker.recordAbandonedMissingPeers(info.missingPeers).whenA(info.missingPeers.nonEmpty) >>
          ConsensusLog
            .info(
              logger,
              ConsensusLog.Facilitator,
              key.toString,
              "n/a",
              "event" -> "RECORDING_MISSING_PEERS",
              "count" -> info.missingPeers.size.toString,
              "peers" -> s"[${info.missingPeers.toList.map(ConsensusLog.pid).mkString(",")}]"
            )
            .whenA(info.missingPeers.nonEmpty) >>
          abandonmentTracker.abandonRound(key, abandonReason) >>
          Metrics[F].incrementCounter(
            "dag_consensus_stall_abandon_reason",
            Seq((Metrics.unsafeLabelName("reason"), abandonReasonLabel))
          )
      ).whenA(shouldAbandon)

      // --- Update health snapshot ---
      statusName = state.status.getClass.getSimpleName.stripSuffix("$")
      _ <- healthRef.update(
        _.copy(
          key = key.toString.some,
          phase = statusName.some,
          phaseIndex = ops.phaseIndex(state.status).some,
          facilitatorCount = state.facilitators.value.size,
          declaredCount = info.declaredCount,
          activeCount = info.activeCount,
          leader = ConsensusLog.pid(state.leader).some,
          viewNumber = state.viewNumber,
          roundElapsedMs = roundElapsed.toMillis,
          phaseElapsedMs = statusDuration.toMillis,
          stallCount = finalStallCount,
          isRunning = true,
          missingPeers = info.missingPeers.toList.map(ConsensusLog.pid),
          facilitatorIds = state.facilitators.value.map(ConsensusLog.pid)
        )
      )

      // --- Periodic summary logging ---
      timeSinceLastSummary = now - ms.lastSummaryTime
      shouldLogSummary = statusChanged || (timeSinceLastSummary >= config.monitorSummaryInterval && info.declaredCount < info.activeCount)
      newSummaryTime = if (shouldLogSummary) now else ms.lastSummaryTime
      _ <- logSummary(key, state, info, statusDuration, roundElapsed, finalStallCount, statusName)
        .whenA(shouldLogSummary && !shouldAbandon)

      // --- Periodic peer quality score logging ---
      timeSinceLastScoreLog = now - ms.lastScoreLogTime
      shouldLogScores = timeSinceLastScoreLog >= config.peerScoreLogInterval
      newScoreLogTime = if (shouldLogScores) now else ms.lastScoreLogTime
      _ <- logPeerQualityScores(key).whenA(shouldLogScores && !shouldAbandon)

      // --- Adaptive sleep ---
      changed = resourcesChanged || statusChanged || didStall
      newNoChangeCount = if (changed) 0 else ms.noChangeCount + 1
      sleepMs = if (changed) basePollInterval else math.min(basePollInterval * (newNoChangeCount + 1), maxPollInterval)
      _ <- Temporal[F].sleep(sleepMs.millis).unlessA(shouldAbandon)

    } yield
      if (shouldAbandon)
        Right(())
      else
        Left(
          MonitorState(
            lastResourcesHash = info.hash,
            lastStatus = Some(state.status),
            statusStartTime = adjustedStatusStartTime,
            roundStartTime = ms.roundStartTime,
            noChangeCount = newNoChangeCount,
            stallCount = finalStallCount,
            lastSummaryTime = newSummaryTime,
            lastScoreLogTime = newScoreLogTime
          )
        )

  // ── Timeout Calculation ───────────────────────────────────────────

  private def calculateTimeout(
    stallCount: Int,
    info: ResourcesInfo,
    state: ConsensusState[Key, Status, Outcome, Kind]
  ): F[FiniteDuration] =
    getCurrentDeclarationTimeout.map { declarationTimeout =>
      val baseTimeout =
        if (stallCount > 0)
          config.reStallTimeout.getOrElse(declarationTimeout)
        else if (info.declaredCount == 0)
          config.noProgressTimeout.getOrElse(declarationTimeout)
        else
          declarationTimeout

      val declarationProgress = if (info.activeCount > 0) info.declaredCount.toDouble / info.activeCount else 0.0
      val nearCompletion = declarationProgress >= 0.75 && info.declaredCount < info.activeCount
      val baseEffective =
        if (nearCompletion && stallCount == 0)
          baseTimeout + (baseTimeout / 2)
        else baseTimeout

      val phaseMultiplier = ops.phaseIndex(state.status) match {
        case 0 => config.facilitiesTimeoutMultiplier
        case 1 => config.proposalsTimeoutMultiplier
        case 2 => config.signaturesTimeoutMultiplier
        case _ => 1.0
      }
      FiniteDuration((baseEffective.toMillis * phaseMultiplier).toLong, MILLISECONDS)
    }

  // ── Stall Handling ────────────────────────────────────────────────

  /** Handle a stall condition. Returns true if a stall was detected (view change or non-proposal timeout). */
  private def handleStall(
    key: Key,
    state: ConsensusState[Key, Status, Outcome, Kind],
    declarationTimeout: FiniteDuration,
    statusDuration: FiniteDuration,
    declaredCount: Int,
    activeCount: Int,
    missingPeerIds: Set[String]
  ): F[Boolean] =
    if (statusDuration >= declarationTimeout) {
      val statusName = state.status.getClass.getSimpleName.stripSuffix("$")
      val phaseLabel = Seq((Metrics.unsafeLabelName("phase"), statusName))
      val missingDisplay = if (missingPeerIds.nonEmpty) s"[${missingPeerIds.mkString(",")}]" else "none"

      if (ops.isProposalPhase(state.status)) {
        ConsensusLog.warn(
          logger,
          ConsensusLog.Stall,
          key.toString,
          "n/a",
          "event" -> "LEADER_STALL",
          "status" -> statusName,
          "elapsed" -> s"${statusDuration.toSeconds}s",
          "timeout" -> s"${declarationTimeout.toSeconds}s",
          "progress" -> s"$declaredCount/$activeCount",
          "leader" -> ConsensusLog.pid(state.leader),
          "view" -> state.viewNumber.toString,
          "missing" -> missingDisplay
        ) >>
          Metrics[F].incrementCounter("dag_consensus_view_change") >>
          Metrics[F].incrementCounter("dag_consensus_stall_phase", phaseLabel) >>
          viewChangeManager.performViewChange(key, state).as(true)
      } else {
        ConsensusLog.warn(
          logger,
          ConsensusLog.Stall,
          key.toString,
          "n/a",
          "event" -> "STALL_DETECTED",
          "status" -> statusName,
          "elapsed" -> s"${statusDuration.toSeconds}s",
          "timeout" -> s"${declarationTimeout.toSeconds}s",
          "progress" -> s"$declaredCount/$activeCount",
          "missing" -> missingDisplay
        ) >>
          Metrics[F].incrementCounter("dag_consensus_stall_detected") >>
          Metrics[F].incrementCounter("dag_consensus_stall_phase", phaseLabel) >>
          true.pure[F]
      }
    } else {
      false.pure[F]
    }

  // ── Resource Info ─────────────────────────────────────────────────

  private def getResourcesInfo(
    state: ConsensusState[Key, Status, Outcome, Kind],
    resources: ConsensusResources[Artifact, Kind]
  ): ResourcesInfo = {
    val active = state.facilitators.value.toSet -- state.withdrawnFacilitators.value
    ops.maybeCollectingKind(state.status) match {
      case Some(kind) =>
        val getter = ops.kindGetter(kind)
        val respondedPeers = resources.peerDeclarationsMap.collect {
          case (pid, decls) if active.contains(pid) && getter(decls).isDefined => pid
        }.toSet
        val missing = active -- respondedPeers
        ResourcesInfo(
          hash = respondedPeers.hashCode(),
          declaredCount = respondedPeers.size,
          activeCount = active.size,
          missingPeerIds = missing.toList.map(_.value.value.take(8)).toSet,
          missingPeers = missing
        )
      case None =>
        ResourcesInfo(
          hash = resources.peerDeclarationsMap.keySet.hashCode(),
          declaredCount = resources.peerDeclarationsMap.size,
          activeCount = active.size,
          missingPeerIds = Set.empty,
          missingPeers = Set.empty
        )
    }
  }

  // ── Helpers ───────────────────────────────────────────────────────

  private def isLeaderUnresponsive(leader: PeerId): F[Boolean] =
    if (leader == ctx.selfId)
      false.pure[F] // Local node is always responsive to itself
    else
      clusterStorage.getPeer(leader).map {
        case Some(peer) => peer.responsiveness === (Unresponsive: PeerResponsiveness)
        case None       => true
      }

  private def getCurrentDeclarationTimeout: F[FiniteDuration] =
    ctx.nodeStorage.isInJoiningGracePeriod.map { isInJoiningGracePeriod =>
      if (isInJoiningGracePeriod) config.timeTriggerInterval else config.declarationTimeout
    }

  // ── Logging ───────────────────────────────────────────────────────

  private def logSummary(
    key: Key,
    state: ConsensusState[Key, Status, Outcome, Kind],
    info: ResourcesInfo,
    statusDuration: FiniteDuration,
    roundElapsed: FiniteDuration,
    stallCount: Int,
    statusName: String
  ): F[Unit] =
    peerQualityTracker.getQualityScores.flatMap { scores =>
      val withdrawnCount = state.withdrawnFacilitators.value.size

      // Build facilitator roster: each peer shows declared status and quality
      val roster = state.facilitators.value.map { pid =>
        val tag = ConsensusLog.pid(pid)
        val isLeader = pid == state.leader
        val isDeclared = !info.missingPeers.contains(pid)
        val isWithdrawn = state.withdrawnFacilitators.value.contains(pid)
        val score = scores.get(pid).map(s => f"$s%.2f").getOrElse("?")
        val status =
          if (isWithdrawn) "W"
          else if (isDeclared) "ok"
          else "MISS"
        val leaderMark = if (isLeader) "*" else ""
        s"$tag$leaderMark($status,$score)"
      }

      // Missing peers with their quality scores for quick identification
      val missingWithScores = info.missingPeers.toList.map { pid =>
        val score = scores.get(pid).map(s => f"$s%.2f").getOrElse("?")
        s"${ConsensusLog.pid(pid)}(q=$score)"
      }

      val summaryPairs = Seq(
        "event" -> "ROUND_MONITOR",
        "status" -> statusName,
        "progress" -> s"${info.declaredCount}/${info.activeCount}",
        "phaseElapsed" -> s"${statusDuration.toSeconds}s",
        "roundElapsed" -> s"${roundElapsed.toSeconds}s",
        "stallCount" -> stallCount.toString,
        "leader" -> ConsensusLog.pid(state.leader),
        "peers" -> s"[${roster.mkString(" ")}]"
      ) ++
        (if (state.viewNumber > 0) Seq("view" -> state.viewNumber.toString) else Seq.empty) ++
        (if (withdrawnCount > 0) Seq("withdrawn" -> withdrawnCount.toString) else Seq.empty) ++
        (if (missingWithScores.nonEmpty) Seq("unhealthy" -> s"[${missingWithScores.mkString(",")}]") else Seq.empty)
      ConsensusLog.info(logger, ConsensusLog.Stall, key.toString, "n/a", summaryPairs: _*)
    }

  private def logPeerQualityScores(key: Key): F[Unit] =
    peerQualityTracker.getQualityScores.flatMap { scores =>
      if (scores.nonEmpty) {
        val sorted = scores.toList.sortBy(-_._2)
        val total = sorted.size
        val healthy = sorted.count(_._2 >= 0.7)
        val degraded = sorted.count(s => s._2 >= 0.3 && s._2 < 0.7)
        val unhealthy = sorted.count(_._2 < 0.3)
        val top3 = sorted.take(3).map { case (pid, score) => s"${ConsensusLog.pid(pid)}:${f"$score%.2f"}" }
        val bottom3 = sorted.takeRight(3).map { case (pid, score) => s"${ConsensusLog.pid(pid)}:${f"$score%.2f"}" }
        val topIds = sorted.take(3).map(_._1).toSet
        val bottomEntries =
          if (total > 6) bottom3.filterNot(e => topIds.exists(id => e.startsWith(ConsensusLog.pid(id)))) else Nil
        val display =
          if (bottomEntries.nonEmpty) s"best=[${top3.mkString(",")}],worst=[${bottomEntries.mkString(",")}]"
          else s"[${top3.mkString(",")}]"
        ConsensusLog.info(
          logger,
          ConsensusLog.Facilitator,
          key.toString,
          "n/a",
          "event" -> "PEER_QUALITY",
          "scores" -> display,
          "summary" -> s"healthy=$healthy,degraded=$degraded,unhealthy=$unhealthy",
          "trackedPeers" -> total.toString
        )
      } else
        ConsensusLog
          .debug(logger, ConsensusLog.Facilitator, key.toString, "n/a", "event" -> "PEER_QUALITY", "trackedPeers" -> "0")
    }
}
