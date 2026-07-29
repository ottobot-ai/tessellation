package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import cats.effect.std.Semaphore
import cats.effect.{IO, Ref}

import io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate

import weaver.SimpleIOSuite

/** RED boundary for restart and rebootstrap authority.
  *
  * A persisted GL0 head is recovery input, not proof that its transition was authenticated and executed in this process. These checks do
  * not change GL0 execution responsibility: every native GL1/DAG-token transition still belongs to the complete GL0 replay that upgrades
  * recovery bytes into selected executed state.
  */
object RestartAuthorityRedSuite extends SimpleIOSuite {

  private val RecoveryRequiredReason = "recovery-required"
  private val RawStorePattern = "(?s)chainStore\\s*\\.\\s*store\\s*\\(".r

  private final case class RecordingGate(
    gate: ProductionGate[IO],
    reasons: Ref[IO, Set[String]],
    events: Ref[IO, Vector[String]]
  )

  private def recordingGate: IO[RecordingGate] =
    for {
      reasons <- Ref.of[IO, Set[String]](Set.empty)
      events <- Ref.of[IO, Vector[String]](Vector.empty)
    } yield {
      val gate = new ProductionGate[IO] {
        def pause(reason: String): IO[Unit] =
          reasons.update(_ + reason) >> events.update(_ :+ s"pause:$reason")

        def resume(reason: String): IO[Unit] =
          reasons.update(_ - reason) >> events.update(_ :+ s"resume:$reason")

        def isOpen: IO[Boolean] = reasons.get.map(_.isEmpty)

        def pauseReasons: IO[Set[String]] = reasons.get
      }
      RecordingGate(gate, reasons, events)
    }

  test("RA-01: a restored disk head is recovery-only and cannot become selected or finality authority") {
    readLeaderLoop.map { leaderLoop =>
      val startupSeed = sliceBetween(
        leaderLoop,
        "val seedChainStore: Stream[F, Unit]",
        "// Slot duration:"
      )

      expect.all(
        startupSeed.contains("snapshotStorage.head"),
        startupSeed.contains("chainStore.seedUnattestableForRecovery"),
        RawStorePattern.findFirstIn(startupSeed).isEmpty,
        !startupSeed.contains("chainStore.storeValidated"),
        !startupSeed.contains("BecameSelected"),
        !startupSeed.contains("PreferredExecutedTip"),
        !startupSeed.contains("finalizeSelectedAt"),
        !startupSeed.contains("nakamotoFinalizedOrdinalRef")
      )
    }
  }

  test("RA-01: consensus resource startup cannot raw-select the restored disk head") {
    readGlobalSnapshotConsensus.map { consensus =>
      val startupSeed = sliceBetween(
        consensus,
        "// Seed chain store with the current head snapshot",
        "lastKnownSlotRef <-"
      )

      expect.all(
        startupSeed.contains("globalSnapshotStorage.head"),
        startupSeed.contains("chainStore.seedUnattestableForRecovery"),
        RawStorePattern.findFirstIn(startupSeed).isEmpty,
        !startupSeed.contains("chainStore.storeValidated"),
        !startupSeed.contains("BecameSelected"),
        !startupSeed.contains("PreferredExecutedTip"),
        !startupSeed.contains("finalizeSelectedAt")
      )
    }
  }

  test("RA-02: slot production has no snapshotStorage.head fallback when selected executed authority is absent") {
    readLeaderLoop.map { leaderLoop =>
      val onSlotWon = sliceBetween(
        leaderLoop,
        "private def onSlotWon[",
        "} // withCurrent"
      )
      val selectedTipRead = onSlotWon.indexOf("chainStore.selectedTip")
      val execution = onSlotWon.indexOf("consensusFns.createProposalArtifact")
      val authorityToExecution =
        if (selectedTipRead >= 0 && execution > selectedTipRead) onSlotWon.substring(selectedTipRead, execution)
        else ""

      expect.all(
        !onSlotWon.contains("snapshotStorage.head"),
        selectedTipRead >= 0,
        execution > selectedTipRead,
        authorityToExecution.contains("case None"),
        !authorityToExecution.contains("setHeadForRecovery")
      )
    }
  }

  test("RA-02: a completed unauthenticated rebootstrap remains fail-stopped in RecoveryRequired") {
    for {
      gate <- recordingGate
      semaphore <- Semaphore[IO](1L)
      resetRan <- Ref.of[IO, Boolean](false)
      result <- RebootstrapOrchestrator
        .withPausedSnapshotSemaphore(
          semaphore,
          gate.gate,
          IO.unit
        )(resetRan.set(true))
        .attempt
      didResetRun <- resetRan.get
      reasons <- gate.reasons.get
      events <- gate.events.get
      gateOpen <- gate.gate.isOpen
      permitAvailable <- semaphore.tryAcquire
      _ <- IO.whenA(permitAvailable)(semaphore.release)
    } yield
      expect.all(
        result.isRight,
        didResetRun,
        reasons.contains(RecoveryRequiredReason),
        !gateOpen,
        !permitAvailable,
        !events.contains(s"resume:$RecoveryRequiredReason"),
        events.contains(s"resume:${RebootstrapOrchestrator.RebootstrapInProgress}")
      )
  }

  private def readLeaderLoop: IO[String] =
    readRepositoryFile(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala"
    )

  private def readGlobalSnapshotConsensus: IO[String] =
    readRepositoryFile(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala"
    )

  private def readRepositoryFile(relativePath: String): IO[String] =
    IO.blocking {
      val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
      val path = root.resolve(relativePath)
      new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
    }

  private def sliceBetween(source: String, startMarker: String, endMarker: String): String = {
    val start = source.indexOf(startMarker)
    val end = source.indexOf(endMarker, math.max(0, start + startMarker.length))
    if (start < 0 || end < 0)
      throw new AssertionError(s"Unable to locate source slice: start=$startMarker end=$endMarker")
    source.substring(start, end)
  }

  private def repositoryRoot(start: Path): Path = {
    val candidates = Iterator.iterate(Option(start))(_.flatMap(path => Option(path.getParent))).takeWhile(_.nonEmpty).flatten
    candidates
      .find(path => Files.isDirectory(path.resolve("modules/dag-l0")))
      .getOrElse(throw new IllegalStateException(s"Unable to locate repository root from $start"))
  }
}
