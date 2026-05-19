package io.constellationnetwork.node.shared.infrastructure.consensus.engine

import cats.effect.IO
import cats.effect.std.Queue

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.config.types.{ConsensusConfig, EventCutterConfig}
import io.constellationnetwork.node.shared.domain.node.{NodeStorage => NodeStorageT}
import io.constellationnetwork.node.shared.infrastructure.consensus.ConsensusStorage
import io.constellationnetwork.node.shared.infrastructure.node.NodeStorage
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.node.NodeState

import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import io.circe.Encoder
import monocle.Lens
import weaver.SimpleIOSuite

/** Regression test for the iter-198std-v2 4-mg E2E stall (#145 reoccurrence at 4-mg scale).
  *
  * Symptom (observed on ml0-m0-1 and ml0-m1-1 in 2026-05-18 8gl0+4mg run):
  *   - First Download.observe completes (possibly with partial observation), registerForConsensus(K1) succeeds, setting observationKeyR :=
  *     Some(K1) and FSM := Observing.
  *   - InitializeFromDownload exhausts its 20 fetch retries, escalating to the `ConsensusEventLoop` error handler which transitions FSM
  *     back to WaitingForDownload.
  *   - DownloadDaemon picks up the state event, downloads again, calls observe, then tries registerForConsensus(K2) with a fresh
  *     observation limit.
  *   - registerForConsensus delegates to storage.trySetObservationKey(K2) which uses `observationKeyR.getAndUpdate(_.orElse(K2.some))`
  *     semantics — it keeps the EXISTING Some(K1) and returns `false` (was-empty check fails).
  *   - The `.ifM(ifTrue, ifFalse)` branch raises: `Throwable("Registration failed: already registered at different key")`.
  *   - The daemon's retry loop catches it, sleeps 10s, re-reads the FSM state which is now stuck in WaitingForObserving (the tryModifyState
  *     already advanced it before registerForConsensus was even invoked). The daemon's `case other => abort` branch fires and the node
  *     freezes.
  *
  * Without the fix in ConsensusEventLoop.scala:208-260, the recovery path transitions the FSM but does NOT reset the consensus storage
  * observation key — so every subsequent attempt hits the same collision.
  */
object InitFromDownloadRecoverySuite extends SimpleIOSuite {

  private type Event = Unit
  private type Key = SnapshotOrdinal
  private type Artifact = String
  private type Context = Unit
  private type Status = Unit
  private type Outcome = SnapshotOrdinal
  private type Kind = Unit

  private val testConfig: ConsensusConfig = ConsensusConfig(
    timeTriggerInterval = 5.seconds,
    declarationTimeout = 60.seconds,
    declarationRangeLimit = NonNegLong(10L),
    lockDuration = 10.seconds,
    eventCutter = EventCutterConfig(maxBinarySizeBytes = PosInt(1_000_000), maxUpdateNodeParametersSize = PosInt(1_000_000))
  )

  private implicit val outcomeKeyLens: Lens[Outcome, Key] = Lens[Outcome, Key](identity)(o => _ => o)
  private implicit val artifactEncoder: Encoder[Artifact] = Encoder.encodeString

  private def mkStorage: IO[ConsensusStorage[IO, Event, Key, Artifact, Context, Status, Outcome, Kind]] =
    ConsensusStorage.make[IO, Event, Key, Artifact, Context, Status, Outcome, Kind](testConfig)

  private def mkManager(
    storage: ConsensusStorage[IO, Event, Key, Artifact, Context, Status, Outcome, Kind]
  ): IO[(ConsensusManager[IO, Event, Key, Artifact, Context, Status, Outcome, Kind], NodeStorageT[IO])] =
    for {
      queue <- Queue.unbounded[IO, ConsensusCommand]
      nodeStorage <- NodeStorage.make[IO]
      manager <- ConsensusManager.make[IO, Event, Key, Artifact, Context, Status, Outcome, Kind](queue, storage, nodeStorage)
    } yield (manager, nodeStorage)

  test("trySetObservationKey returns false when an existing key is set (the bug surface)") {
    for {
      storage <- mkStorage
      first <- storage.trySetObservationKey(SnapshotOrdinal.unsafeApply(6L))
      second <- storage.trySetObservationKey(SnapshotOrdinal.unsafeApply(14L))
      stored <- storage.getObservationKey
    } yield expect(first).and(expect(!second)).and(expect(stored.contains(SnapshotOrdinal.unsafeApply(6L))))
  }

  test("clearObservationKey allows trySetObservationKey to succeed with a different key (the fix)") {
    for {
      storage <- mkStorage
      _ <- storage.trySetObservationKey(SnapshotOrdinal.unsafeApply(6L))
      _ <- storage.clearObservationKey
      second <- storage.trySetObservationKey(SnapshotOrdinal.unsafeApply(14L))
      stored <- storage.getObservationKey
    } yield expect(second).and(expect(stored.contains(SnapshotOrdinal.unsafeApply(14L))))
  }

  test("ConsensusManager.registerForConsensus raises when an old key blocks a new key without reset") {
    for {
      storage <- mkStorage
      managerAndNode <- mkManager(storage)
      (manager, nodeStorage) = managerAndNode
      // Simulate that a previous download set the observation key (manager's ifTrue branch
      // also advances the FSM, so we need WaitingForObserving for the first registration).
      _ <- nodeStorage.setNodeState(NodeState.WaitingForObserving)
      _ <- manager.registerForConsensus(SnapshotOrdinal.unsafeApply(6L))
      // ConsensusManager.registerForConsensus advanced WaitingForObserving → Observing; revert to
      // WaitingForObserving so the second attempt could in principle progress past the FSM check.
      _ <- nodeStorage.setNodeState(NodeState.WaitingForObserving)
      result <- manager.registerForConsensus(SnapshotOrdinal.unsafeApply(14L)).attempt
    } yield
      expect(result.isLeft).and(
        expect(
          result.left.toOption.exists(_.getMessage.contains("Registration failed: already registered at different key"))
        )
      )
  }

  test("ConsensusManager.resetForRecovery clears observation key, enabling re-registration (the fix contract)") {
    for {
      storage <- mkStorage
      managerAndNode <- mkManager(storage)
      (manager, nodeStorage) = managerAndNode
      _ <- nodeStorage.setNodeState(NodeState.WaitingForObserving)
      _ <- manager.registerForConsensus(SnapshotOrdinal.unsafeApply(6L))
      // Simulate the InitializeFromDownload recovery path now wired into ConsensusEventLoop:
      // reset storage before transitioning back to WaitingForDownload → WaitingForObserving.
      _ <- manager.resetForRecovery
      _ <- nodeStorage.setNodeState(NodeState.WaitingForObserving)
      result <- manager.registerForConsensus(SnapshotOrdinal.unsafeApply(14L)).attempt
      stored <- storage.getObservationKey
    } yield expect(result.isRight).and(expect(stored.contains(SnapshotOrdinal.unsafeApply(14L))))
  }
}
