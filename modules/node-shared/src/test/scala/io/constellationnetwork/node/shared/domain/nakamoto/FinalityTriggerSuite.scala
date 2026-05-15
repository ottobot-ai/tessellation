package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.node.shared.infrastructure.metrics.NoOpMetrics
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.TipAttestation
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

/** Tests for [[FinalityTrigger]] and its concrete implementations (`TWeightTrigger`, `TDepth1Trigger`).
  *
  * `T_count` and `T_depth2` (Kind enum entries reserved for #136 / #137) have no `make` builder yet and are NOT exercised here.
  */
object FinalityTriggerSuite extends SimpleIOSuite {

  // No-op Metrics typeclass — `TipTracker.make` requires one but these tests don't assert on counters.
  implicit private val metrics: io.constellationnetwork.node.shared.infrastructure.metrics.Metrics[IO] = NoOpMetrics.make

  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  private def slot(n: Long): Slot = Slot(NonNegLong.unsafeFrom(n))

  private def hash(s: String): Hash = Hash(s.padTo(64, '0'))

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal.unsafeApply(n)

  private def att(tipHash: Hash, tipSlot: Slot, tipOrdinal: Long, attestedAt: Long): TipAttestation =
    TipAttestation(tipHash, tipSlot, tipOrdinal, attestedAt)

  // Convenience constructor for `ConsensusState[IO]` with the canonical-hash lookup mocked from a static map.
  private def state(
    selfId: PeerId,
    bestOrd: Long,
    bestHash: Hash,
    canonicalChain: Map[Long, Hash] = Map.empty
  ): FinalityTrigger.ConsensusState[IO] =
    FinalityTrigger.ConsensusState[IO](
      selfId = selfId,
      bestTipOrdinal = ord(bestOrd),
      bestTipHash = bestHash,
      canonicalHashAt = (o: Long) => IO.pure(canonicalChain.get(o))
    )

  private def setupTipTracker(validators: Set[PeerId]): IO[TipTracker[IO]] =
    for {
      registry <- StakeRegistry.equalWeight[IO]
      _ <- registry.updateValidators(validators)
      tracker <- TipTracker.make[IO](registry)
    } yield tracker

  // Helper: record an attestation using its own `attestedAt` as `now` so the skew gate is trivially satisfied.
  private def record(tracker: TipTracker[IO], peerId: PeerId, attestation: TipAttestation): IO[Unit] =
    tracker.recordAttestation(peerId, attestation, attestation.attestedAt)

  test("TWeight: empty attestations → MinValue qualifying ordinal") {
    val self = pid("self")
    val tipHash = hash("tip")
    for {
      tracker <- setupTipTracker(Set(self, pid("peer1"), pid("peer2")))
      trigger <- TWeightTrigger.make[IO](tracker, TipTracker.FinalityThreshold)
      st = state(self, 100L, tipHash, Map(100L -> tipHash))
      result <- trigger.evaluate(st)
    } yield expect.same(SnapshotOrdinal.MinValue, result)
  }

  test("TWeight: 2/3 weight on canonical hash → returns qualifying ordinal") {
    val self = pid("self")
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val tipHash = hash("tip-at-50")
    // Two non-self peers each attest the canonical tip. With self-exclusion the contributing set
    // is {peer1, peer2}; under optimistic stake-weighting they're 1.0 of the active set, which
    // trivially exceeds 2/3.
    for {
      tracker <- setupTipTracker(Set(self, peer1, peer2))
      _ <- record(tracker, peer1, att(tipHash, slot(50), 50L, 1000L))
      _ <- record(tracker, peer2, att(tipHash, slot(50), 50L, 1000L))
      trigger <- TWeightTrigger.make[IO](tracker, TipTracker.FinalityThreshold)
      st = state(self, 50L, tipHash, Map(50L -> tipHash))
      result <- trigger.evaluate(st)
    } yield expect.same(ord(50L), result)
  }

  test("TWeight: attestations on non-canonical fork return MinValue (cross-fork attestation filtered)") {
    val self = pid("self")
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val canonicalTipHash = hash("canonical")
    val forkedTipHash = hash("forked")
    // Peers attest a DIFFERENT hash at ord=50; canonical chain has canonicalTipHash there.
    // The canonical-hash filter zeroes both attestations → no qualifying ordinal.
    for {
      tracker <- setupTipTracker(Set(self, peer1, peer2))
      _ <- record(tracker, peer1, att(forkedTipHash, slot(50), 50L, 1000L))
      _ <- record(tracker, peer2, att(forkedTipHash, slot(50), 50L, 1000L))
      trigger <- TWeightTrigger.make[IO](tracker, TipTracker.FinalityThreshold)
      st = state(self, 50L, canonicalTipHash, Map(50L -> canonicalTipHash))
      result <- trigger.evaluate(st)
    } yield expect.same(SnapshotOrdinal.MinValue, result)
  }

  test("TDepth1: bestTipOrdinal - k when chain is past depth") {
    val self = pid("self")
    val tipHash = hash("tip")
    val k = 10L
    for {
      trigger <- TDepth1Trigger.make[IO](k)
      st = state(self, 100L, tipHash)
      result <- trigger.evaluate(st)
    } yield expect.same(ord(90L), result)
  }

  test("TDepth1: returns MinValue while chain shorter than k") {
    val self = pid("self")
    val tipHash = hash("tip")
    val k = 100L
    for {
      trigger <- TDepth1Trigger.make[IO](k)
      st = state(self, 50L, tipHash) // chain shorter than k
      result <- trigger.evaluate(st)
    } yield expect.same(SnapshotOrdinal.MinValue, result)
  }

  test("evaluateAndAdvance is monotone: never decreases the Ref") {
    // Build a stub trigger via `fromRef` whose eval result is dictated by a mutable Ref. This isolates
    // the monotonicity contract from concrete TWeight/TDepth1 semantics.
    import cats.effect.kernel.Ref
    val self = pid("self")
    val tipHash = hash("tip")
    for {
      evalRef <- Ref.of[IO, Long](42L)
      trigger <- FinalityTrigger.fromRef[IO](FinalityTrigger.Kind.TDepth1, SnapshotOrdinal.MinValue) { _ =>
        evalRef.get.map(SnapshotOrdinal.unsafeApply)
      }
      st = state(self, 0L, tipHash)
      // First evaluate at 42 → Ref advances
      r1 <- trigger.evaluateAndAdvance(st)
      latestAfter1 <- trigger.latestQualifyingOrdinal
      // Now make eval regress to 10 → Ref must NOT decrease
      _ <- evalRef.set(10L)
      r2 <- trigger.evaluateAndAdvance(st)
      latestAfter2 <- trigger.latestQualifyingOrdinal
      // Advance eval forward to 100 → Ref jumps to 100
      _ <- evalRef.set(100L)
      r3 <- trigger.evaluateAndAdvance(st)
      latestAfter3 <- trigger.latestQualifyingOrdinal
    } yield
      expect.same(ord(42L), r1) &&
        expect.same(ord(42L), latestAfter1) &&
        // Eval returned 10 but the Ref-tracked latest stayed at 42 (monotone)
        expect.same(ord(42L), r2) &&
        expect.same(ord(42L), latestAfter2) &&
        expect.same(ord(100L), r3) &&
        expect.same(ord(100L), latestAfter3)
  }

  test("triggersFor: returns triggers whose latestQualifyingOrdinal >= ord") {
    val self = pid("self")
    val tipHash = hash("tip")
    for {
      // Pre-advance T_weight to ord=50 and T_depth1 to ord=90 via two evaluateAndAdvance calls.
      tracker <- setupTipTracker(Set(self, pid("peer1"), pid("peer2")))
      _ <- record(tracker, pid("peer1"), att(tipHash, slot(50), 50L, 1000L))
      _ <- record(tracker, pid("peer2"), att(tipHash, slot(50), 50L, 1000L))
      tWeight <- TWeightTrigger.make[IO](tracker, TipTracker.FinalityThreshold)
      tDepth1 <- TDepth1Trigger.make[IO](10L)
      st = state(self, 100L, tipHash, Map(50L -> tipHash))
      _ <- tWeight.evaluateAndAdvance(st)
      _ <- tDepth1.evaluateAndAdvance(st)
      triggers = List(tWeight, tDepth1)
      atOrd50 <- FinalityTrigger.triggersFor(triggers, ord(50L))
      atOrd90 <- FinalityTrigger.triggersFor(triggers, ord(90L))
      atOrd100 <- FinalityTrigger.triggersFor(triggers, ord(100L))
    } yield
      // ord=50: T_weight (latest=50) qualifies. T_depth1 (latest=90) qualifies (90 >= 50).
      expect.same(Set[FinalityTrigger.Kind](FinalityTrigger.Kind.TWeight, FinalityTrigger.Kind.TDepth1), atOrd50) &&
        // ord=90: T_weight (latest=50) does NOT qualify. T_depth1 (latest=90) does.
        expect.same(Set[FinalityTrigger.Kind](FinalityTrigger.Kind.TDepth1), atOrd90) &&
        // ord=100: neither qualifies.
        expect.same(Set.empty[FinalityTrigger.Kind], atOrd100)
  }

  test("triggersFor: empty trigger list → empty set") {
    for {
      result <- FinalityTrigger.triggersFor[IO](Nil, ord(100L))
    } yield expect.same(Set.empty[FinalityTrigger.Kind], result)
  }

  test("maxLatestQualifyingOrdinal: returns max across all triggers (today's max(t_weight, t_depth1) semantics)") {
    val self = pid("self")
    val tipHash = hash("tip")
    for {
      tracker <- setupTipTracker(Set(self, pid("peer1"), pid("peer2")))
      _ <- record(tracker, pid("peer1"), att(tipHash, slot(50), 50L, 1000L))
      _ <- record(tracker, pid("peer2"), att(tipHash, slot(50), 50L, 1000L))
      tWeight <- TWeightTrigger.make[IO](tracker, TipTracker.FinalityThreshold)
      tDepth1 <- TDepth1Trigger.make[IO](10L)
      st = state(self, 100L, tipHash, Map(50L -> tipHash))
      _ <- tWeight.evaluateAndAdvance(st)
      _ <- tDepth1.evaluateAndAdvance(st)
      maxOrd <- FinalityTrigger.maxLatestQualifyingOrdinal[IO](List(tWeight, tDepth1))
    } yield expect.same(ord(90L), maxOrd) // T_depth1=90 > T_weight=50
  }

  test("maxLatestQualifyingOrdinal: empty list returns MinValue") {
    for {
      maxOrd <- FinalityTrigger.maxLatestQualifyingOrdinal[IO](Nil)
    } yield expect.same(SnapshotOrdinal.MinValue, maxOrd)
  }

  pureTest("Kind.all enumerates all four trigger kinds") {
    expect.same(
      List[FinalityTrigger.Kind](
        FinalityTrigger.Kind.TWeight,
        FinalityTrigger.Kind.TCount,
        FinalityTrigger.Kind.TDepth1,
        FinalityTrigger.Kind.TDepth2
      ),
      FinalityTrigger.Kind.all
    )
  }
}
