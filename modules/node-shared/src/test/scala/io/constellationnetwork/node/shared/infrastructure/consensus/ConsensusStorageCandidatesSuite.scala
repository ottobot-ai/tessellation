package io.constellationnetwork.node.shared.infrastructure.consensus

import cats.effect.IO
import cats.syntax.eq._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.config.types.{ConsensusConfig, EventCutterConfig}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import monocle.Lens
import weaver.SimpleIOSuite

/** Pins the WINDOW semantics of `ConsensusStorage.getCandidates` (the ml0 cohort boot-race fork fix, 2026-06-11): a peer registered at key
  * K must be a candidate for EVERY round key ≥ K, not only at exactly K. The prior exact-key match required the incumbent's round pointer
  * to land on the registered key — a race that in practice never resolved (registrations fired, candidates stayed 0, the 2-node ml0 cohort
  * forked permanently).
  */
object ConsensusStorageCandidatesSuite extends SimpleIOSuite {

  case class StubOutcome(key: SnapshotOrdinal)

  implicit val keyLens: Lens[StubOutcome, SnapshotOrdinal] =
    Lens[StubOutcome, SnapshotOrdinal](_.key)(k => o => o.copy(key = k))

  private val config = ConsensusConfig(
    timeTriggerInterval = 43.seconds,
    declarationTimeout = 50.seconds,
    declarationRangeLimit = NonNegLong(10L),
    lockDuration = 10.seconds,
    eventCutter = EventCutterConfig(maxBinarySizeBytes = 20971520, maxUpdateNodeParametersSize = 100)
  )

  private def mkStorage =
    ConsensusStorage.make[IO, Unit, SnapshotOrdinal, Unit, Unit, Unit, StubOutcome, Unit](config)

  private val peerA = PeerId(Hex("ab" * 64))
  private val peerB = PeerId(Hex("cd" * 64))

  private def ord(n: Long) = SnapshotOrdinal.unsafeApply(n)

  test("a registration at key K is a candidate for every key >= K (window match, not exact)") {
    for {
      storage <- mkStorage
      _ <- storage.registerPeer(peerA, ord(5))
      atRegistered <- storage.getCandidates(ord(5))
      afterRegistered <- storage.getCandidates(ord(9))
      farAfter <- storage.getCandidates(ord(1000))
    } yield
      expect(atRegistered.value == Set(peerA)).and(expect(afterRegistered.value == Set(peerA))).and(expect(farAfter.value == Set(peerA)))
  }

  test("a registration at key K is NOT a candidate for keys below K") {
    for {
      storage <- mkStorage
      _ <- storage.registerPeer(peerA, ord(7))
      before <- storage.getCandidates(ord(6))
    } yield expect(before.value.isEmpty)
  }

  test("re-registration keeps the highest key (latest intent wins; candidacy starts there)") {
    for {
      storage <- mkStorage
      _ <- storage.registerPeer(peerA, ord(9))
      _ <- storage.registerPeer(peerA, ord(4))
      atLow <- storage.getCandidates(ord(5))
      atHigh <- storage.getCandidates(ord(9))
    } yield expect(atLow.value.isEmpty).and(expect(atHigh.value == Set(peerA)))
  }

  test("multiple peers window-match independently") {
    for {
      storage <- mkStorage
      _ <- storage.registerPeer(peerA, ord(3))
      _ <- storage.registerPeer(peerB, ord(8))
      mid <- storage.getCandidates(ord(5))
      late <- storage.getCandidates(ord(8))
    } yield expect(mid.value == Set(peerA)).and(expect(late.value == Set(peerA, peerB)))
  }
}
