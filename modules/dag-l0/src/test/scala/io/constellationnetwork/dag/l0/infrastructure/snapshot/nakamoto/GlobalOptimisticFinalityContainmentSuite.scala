package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import cats.effect.IO

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import weaver.SimpleIOSuite

/** Source/API tripwire for the temporary depth-only GL0 finality boundary.
  *
  * This does not prove the future optimistic protocol. It prevents the removed naked signing and
  * receiver-local cumulative-weight paths from silently regaining authority before the
  * replay-capability and sampled-Snowball implementation replaces this containment.
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
    "(?m)^\\s*chainStore\\.finalize\\s*\\(".r

  test("RTA-RED-002/012/020: unsafe GL0 optimistic authority remains absent") {
    readSources.map {
      case (syncDaemon, leaderLoop, tipTracker, dagL0Main) =>
        val methods = NakamotoSyncDaemon.getClass.getMethods.iterator.map(_.getName).toSet
        val gl0TrackerMutations = gl0TrackerMutation.findAllIn(syncDaemon).size
        val finalizeCalls = finalizeCall.findAllIn(leaderLoop).size

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
          finalizeCalls == 1
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
          paths.iterator().asScala
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

  @annotation.tailrec
  private def repositoryRoot(candidate: Path): Path =
    if (Files.isDirectory(candidate.resolve("modules/dag-l0")) && Files.isRegularFile(candidate.resolve("build.sbt"))) candidate
    else
      Option(candidate.getParent) match {
        case Some(parent) => repositoryRoot(parent)
        case None         => throw new IllegalStateException(s"Unable to locate repository root from ${sys.props("user.dir")}")
      }
}
