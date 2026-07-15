package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import cats.effect.IO

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import weaver.SimpleIOSuite

/** Source/API tripwire for the temporary depth-only GL0 finality boundary.
  *
  * This does not prove the future optimistic protocol. It prevents the removed naked signing and receiver-local cumulative-weight paths
  * from silently regaining authority before the replay-capability and sampled-Snowball implementation replaces this containment.
  */
object GlobalOptimisticFinalityContainmentSuite extends SimpleIOSuite {

  private val nakedEmitterDefinition: Regex =
    "(?m)^\\s*def\\s+emit(?:Tip)?Attestation\\b".r
  private val attestationPublishCall: Regex =
    "(?m)^\\s*(?:[A-Za-z0-9_.]+\\.)?publishAttestation\\s*\\(".r
  private val attestationConstructorCall: Regex =
    "(?m)^\\s*(?:[A-Za-z0-9_.]+\\.)?mkAttestation\\s*\\(".r
  private val legacyCumulativeCall: Regex =
    "(?m)^\\s*(?:def\\s+|[A-Za-z0-9_.]+\\s*\\.)highestFinalizedOrdinal\\s*\\(".r
  private val gl0TrackerMutation: Regex =
    "(?m)^\\s*(?:_\\s*<-\\s*)?tipTracker\\.recordAttestation\\s*\\(".r
  private val finalizeCall: Regex =
    "(?m)^\\s*chainStore\\.finalizeSelectedAt\\s*\\(".r
  private val legacyFinalizeCall: Regex =
    "(?m)^\\s*chainStore\\.finalize\\s*\\(".r
  private val overlayPruneCall: Regex =
    "(?m)^\\s*(?:_\\s*<-\\s*)?mptOverlay\\.pruneBelow\\s*\\(".r
  private val readyMutation: Regex =
    "(?m)^\\s*nodeStorage\\.setNodeState\\(NodeState\\.Ready\\)".r
  private val processValidCall: Regex =
    "(?m)^\\s*processValidSnapshot\\s*\\(".r
  private val drainPendingCall: Regex =
    "(?m)^\\s*drainPendingChildren\\s*\\(".r
  private val gatedProcessValidCall: Regex =
    "(?s)Async\\[F\\]\\.whenA\\(storeAccepted\\)\\s*\\{\\s*processValidSnapshot\\s*\\(".r
  private val gatedDrainPendingCall: Regex =
    "(?s)Async\\[F\\]\\.whenA\\(storeAccepted\\)\\s*\\{\\s*drainPendingChildren\\s*\\(".r

  test("RTA-RED-002/012/020: unsafe GL0 optimistic authority remains absent") {
    readSources.map {
      case (syncDaemon, leaderLoop, tipTracker, dagL0Main) =>
        val methods = NakamotoSyncDaemon.getClass.getMethods.iterator.map(_.getName).toSet
        val gl0TrackerMutations = gl0TrackerMutation.findAllIn(syncDaemon).size
        val finalizeCalls = finalizeCall.findAllIn(dagL0Main).size

        expect.all(
          !methods.contains("emitAttestation"),
          !methods.contains("emitTipAttestation"),
          nakedEmitterDefinition.findFirstIn(syncDaemon).isEmpty,
          attestationPublishCall.findFirstIn(syncDaemon).isEmpty,
          attestationPublishCall.findFirstIn(leaderLoop).isEmpty,
          attestationPublishCall.findFirstIn(dagL0Main).isEmpty,
          attestationConstructorCall.findFirstIn(dagL0Main).isEmpty,
          legacyCumulativeCall.findFirstIn(syncDaemon).isEmpty,
          legacyCumulativeCall.findFirstIn(leaderLoop).isEmpty,
          legacyCumulativeCall.findFirstIn(tipTracker).isEmpty,
          gl0TrackerMutations == 1,
          !syncDaemon.contains("tipTracker.recordAttestation(producerId"),
          !leaderLoop.contains("emitTipAttestation"),
          leaderLoop.contains("depthQualifying <- tDepth1.latestQualifyingOrdinal"),
          legacyFinalizeCall.findFirstIn(dagL0Main).isEmpty,
          finalizeCalls == 1
        )
    }
  }

  test("depth sink derives its target from the selected tip and sticky trigger ordinals cannot prune") {
    readSources.map {
      case (_, leaderLoop, _, _) =>
        val depthSink = sliceBetween(
          leaderLoop,
          "finalizedOutbox <- selectedTip match",
          "// Sidecar acknowledgement is transport cleanup"
        )
        val successfulFinalize = sliceBetween(
          depthSink,
          "case NakamotoChainStore.FinalizeOutcome.Finalized(",
          "case NakamotoChainStore.FinalizeOutcome.StaleSelection"
        )

        expect.all(
          leaderLoop.contains("depthQualifying <- tDepth1.latestQualifyingOrdinal"),
          leaderLoop.contains("archivalQualifying <- tDepth2.latestQualifyingOrdinal"),
          depthSink.contains(
            "val finalizeAtOrdinal = math.max(0L, expected.snapshot.ordinal - ConfirmationDepthK)"
          ),
          depthSink.contains("chainStore.finalizeSelectedAt(expected, finalizeAtOrdinal)"),
          !depthSink.contains("depthQualifying"),
          !depthSink.contains("archivalQualifying"),
          overlayPruneCall.findAllIn(leaderLoop).size == 1,
          overlayPruneCall.findAllIn(successfulFinalize).size == 1
        )
    }
  }

  test("snapshot receive records catch-up inside the exact selected-tip projection CAS but cannot publish lifecycle Ready") {
    readSources.map {
      case (syncDaemon, _, _, _) =>
        val processValid = sliceBetween(
          syncDaemon,
          "private def processValidSnapshot[",
          "private def handleAttestation["
        )
        val selectedProjection = bracedBlock(
          processValid,
          "canonicalOutcome <- selected.traverse { expected =>"
        )._1
        val exactSelectionCas = bracedBlock(
          selectedProjection,
          "chainStore.runCanonicalEffectsIfCurrent(expected) { canonical =>"
        )._1

        expect.all(
          readyMutation.findAllIn(processValid).isEmpty,
          readyMutation.findAllIn(selectedProjection).isEmpty,
          readyMutation.findAllIn(exactSelectionCas).isEmpty,
          exactSelectionCas.contains("localTipOrdinal = canonical.ordinal")
        )
    }
  }

  test("a healthy quiet Subscribe stream is not expired by message silence") {
    readSources.map {
      case (syncDaemon, _, _, _) =>
        expect(!syncDaemon.contains("Gossip idle timeout"))
          .and(expect(!syncDaemon.contains("Gossip stream idle for")))
          .and(expect(!syncDaemon.contains("concurrently(watchdog)")))
          .and(expect(!syncDaemon.contains("lastMsgRef")))
    }
  }

  test("sidecar outbox confirmation remains outside the canonical snapshot semaphore") {
    readSources.map {
      case (_, leaderLoop, _, _) =>
        val depthAndOutbox = sliceBetween(
          leaderLoop,
          "finalizedOutbox <- selectedTip match",
          "// Optimistic Phase 2 is deliberately dark here"
        )
        val (criticalSection, criticalSectionEnd) = bracedBlock(
          depthAndOutbox,
          "snapshotSemaphore.permit.use { _ =>"
        )
        val afterCriticalSection = depthAndOutbox.substring(criticalSectionEnd)

        expect.all(
          criticalSection.contains("chainStore.finalizeSelectedAt(expected, finalizeAtOrdinal)"),
          !criticalSection.contains("confirmSnapshotOutbox"),
          afterCriticalSection.contains("finalizedOutbox.traverse_"),
          afterCriticalSection.contains("confirmSnapshotOutbox(canonicalSnapshot, sidecarClient, logger)")
        )
    }
  }

  test("rejected replay-valid receives discard exact staged state and cannot process or drain") {
    readSources.map {
      case (syncDaemon, _, _, _) =>
        val replayStore = sliceBetween(
          syncDaemon,
          "replayCommitted <- commitReplayValidated(validationResult) { (validSnapshot, validContext) =>",
          "_ <- Async[F].unlessA(replayCommitted)"
        )
        val rejectedBranch = sliceBetween(
          replayStore,
          "case NakamotoChainStore.StoreOutcome.Rejected(reason, _) =>",
          "case _ => Async[F].unit"
        )
        val discardIndex = rejectedBranch.indexOf("mptOverlay.discardBranch")
        val accumulatorIndex = rejectedBranch.indexOf("pendingAccumulatorsRef.update(_ - validHash)")
        val postBytesIndex = rejectedBranch.indexOf("pendingPostBytesRef.update(_ - validHash)")
        val logIndex = rejectedBranch.indexOf("logger.warn")

        expect.all(
          replayStore.contains("case _: NakamotoChainStore.StoreOutcome.Rejected => false"),
          rejectedBranch.contains(
            "io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(validHash)"
          ),
          discardIndex >= 0,
          accumulatorIndex > discardIndex,
          postBytesIndex > accumulatorIndex,
          logIndex > postBytesIndex,
          processValidCall.findFirstIn(rejectedBranch).isEmpty,
          drainPendingCall.findFirstIn(rejectedBranch).isEmpty,
          processValidCall.findAllIn(replayStore).size == 1,
          gatedProcessValidCall.findAllIn(replayStore).size == 1,
          drainPendingCall.findAllIn(replayStore).size == 1,
          gatedDrainPendingCall.findAllIn(replayStore).size == 1
        )
    }
  }

  test("Phase-2 consumers run only after local serve state and the public watermark are ready") {
    readSources.map {
      case (_, leaderLoop, _, _) =>
        val successfulFinalize = sliceBetween(
          leaderLoop,
          "case NakamotoChainStore.FinalizeOutcome.Finalized(",
          "case NakamotoChainStore.FinalizeOutcome.StaleSelection"
        )
        val rangePromotion = successfulFinalize.indexOf("finalizedHashes <- recordFinalizedRange(")
        val watermark = successfulFinalize.indexOf("_ <- nakamotoFinalizedOrdinalRef")
        val consumers = successfulFinalize.indexOf("_ <- finalizedHashes.traverse_")
        val rangeHelper = sliceBetween(
          leaderLoop,
          "def recordFinalizedRange(",
          "// Use shared genesis time"
        )

        expect.all(
          rangePromotion >= 0,
          watermark > rangePromotion,
          consumers > watermark,
          !rangeHelper.contains("onFinalize(")
        )
    }
  }

  private def readSources: IO[(String, String, String, String)] =
    IO.blocking {
      val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
      def read(relative: String): String =
        new String(Files.readAllBytes(root.resolve(relative)), StandardCharsets.UTF_8)
      def readScalaTree(relative: String): String = {
        val paths = Files.walk(root.resolve(relative))
        try
          paths
            .iterator()
            .asScala
            .filter(path => Files.isRegularFile(path) && path.getFileName.toString.endsWith(".scala"))
            .map(path => new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
            .mkString("\n")
        finally paths.close()
      }

      (
        read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala"),
        read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala"),
        read("modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/TipTracker.scala"),
        readScalaTree("modules/dag-l0/src/main/scala")
      )
    }

  private def sliceBetween(source: String, startAnchor: String, endAnchor: String): String = {
    val start = source.indexOf(startAnchor)
    require(start >= 0, s"Missing source anchor: $startAnchor")
    val end = source.indexOf(endAnchor, start + startAnchor.length)
    require(end >= 0, s"Missing source anchor after '$startAnchor': $endAnchor")
    source.substring(start, end)
  }

  /** Returns the first balanced `{...}` block at or after `anchor`, plus its exclusive end offset in `source`. This is intentionally a
    * narrow structural check rather than a Scala parser: the guarded source contains balanced interpolation braces, and the test fails
    * closed if the named control-flow anchor disappears.
    */
  private def bracedBlock(source: String, anchor: String): (String, Int) = {
    val anchorStart = source.indexOf(anchor)
    require(anchorStart >= 0, s"Missing source anchor: $anchor")
    val open = source.indexOf('{', anchorStart)
    require(open >= 0, s"Missing opening brace after source anchor: $anchor")

    var cursor = open
    var depth = 0
    var end = -1
    while (cursor < source.length && end < 0) {
      source.charAt(cursor) match {
        case '{' => depth += 1
        case '}' =>
          depth -= 1
          if (depth == 0) end = cursor + 1
        case _ => ()
      }
      cursor += 1
    }
    require(end >= 0, s"Unbalanced source block after anchor: $anchor")
    (source.substring(open, end), end)
  }

  @annotation.tailrec
  private def repositoryRoot(candidate: Path): Path =
    if (Files.isDirectory(candidate.resolve("modules/dag-l0")) && Files.isRegularFile(candidate.resolve("build.sbt"))) candidate
    else
      Option(candidate.getParent) match {
        case Some(parent) => repositoryRoot(parent)
        case None         => throw new IllegalStateException(s"Unable to locate repository root from ${sys.props("user.dir")}")
      }
}
