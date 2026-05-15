package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.metrics.NoOpMetrics
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.TipAttestation
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

/** Tests for [[FinalityTrigger]] and its concrete implementations (`TWeightTrigger`, `TCountTrigger`, `TDepth1Trigger`, `TDepth2Trigger`).
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
    setupTracker(validators).flatMap { case (tracker, _) => IO.pure(tracker) }

  // Variant that also exposes the StakeRegistry — needed by TCountTrigger.make.
  private def setupTracker(validators: Set[PeerId]): IO[(TipTracker[IO], StakeRegistry[IO])] =
    for {
      registry <- StakeRegistry.equalWeight[IO]
      _ <- registry.updateValidators(validators)
      tracker <- TipTracker.make[IO](registry)
    } yield (tracker, registry)

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

  test("TCount: evaluates to highest ordinal with ≥ 2/3 distinct attester count on canonical hash") {
    // 4-peer cluster (self + 3 others). 2/3 of 4 = 2.67 → ceil → 3 required non-self attesters.
    // All three non-self peers attest the canonical tip at ord=50. Count = 3 ≥ 3 → qualifies ord=50.
    val self = pid("self")
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipHash = hash("tip-at-50")
    for {
      (tracker, registry) <- setupTracker(Set(self, peer1, peer2, peer3))
      _ <- record(tracker, peer1, att(tipHash, slot(50), 50L, 1000L))
      _ <- record(tracker, peer2, att(tipHash, slot(50), 50L, 1000L))
      _ <- record(tracker, peer3, att(tipHash, slot(50), 50L, 1000L))
      trigger <- TCountTrigger.make[IO](tracker, registry, TipTracker.FinalityThreshold)
      st = state(self, 50L, tipHash, Map(50L -> tipHash))
      result <- trigger.evaluate(st)
    } yield expect.same(ord(50L), result)
  }

  test("TCount: attestations on non-canonical fork return MinValue (cross-fork filter)") {
    val self = pid("self")
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val canonicalTipHash = hash("canonical")
    val forkedTipHash = hash("forked")
    // Three non-self peers attest a DIFFERENT hash at ord=50 than what's canonical on our chain.
    // Canonical filter zeroes all three counts → no ordinal reaches the threshold.
    for {
      (tracker, registry) <- setupTracker(Set(self, peer1, peer2, peer3))
      _ <- record(tracker, peer1, att(forkedTipHash, slot(50), 50L, 1000L))
      _ <- record(tracker, peer2, att(forkedTipHash, slot(50), 50L, 1000L))
      _ <- record(tracker, peer3, att(forkedTipHash, slot(50), 50L, 1000L))
      trigger <- TCountTrigger.make[IO](tracker, registry, TipTracker.FinalityThreshold)
      st = state(self, 50L, canonicalTipHash, Map(50L -> canonicalTipHash))
      result <- trigger.evaluate(st)
    } yield expect.same(SnapshotOrdinal.MinValue, result)
  }

  test("TCount: self-excludes selfId from the attester count") {
    // 3-peer cluster: self + peer1 + peer2. 2/3 of 3 = 2 required. If self is NOT excluded,
    // {self, peer1, peer2} = 3 attesters → qualifies. With self-exclusion, {peer1, peer2} = 2
    // attesters → still qualifies. To exercise the exclusion strictly we want a setup where
    // including self would fire but excluding it would NOT: a 4-peer cluster (self + 3 others)
    // where only 2 non-self peers attest, plus self also attests. ceil(2/3 * 4) = 3 required.
    // Non-self count = 2 (peer1, peer2). With self included naively count would be 3 → qualifies.
    // With self excluded, count = 2 < 3 → MinValue.
    val self = pid("self")
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipHash = hash("tip-at-50")
    for {
      (tracker, registry) <- setupTracker(Set(self, peer1, peer2, peer3))
      _ <- record(tracker, self, att(tipHash, slot(50), 50L, 1000L)) // self attests
      _ <- record(tracker, peer1, att(tipHash, slot(50), 50L, 1000L))
      _ <- record(tracker, peer2, att(tipHash, slot(50), 50L, 1000L))
      // peer3 does NOT attest
      trigger <- TCountTrigger.make[IO](tracker, registry, TipTracker.FinalityThreshold)
      st = state(self, 50L, tipHash, Map(50L -> tipHash))
      result <- trigger.evaluate(st)
    } yield expect.same(SnapshotOrdinal.MinValue, result)
  }

  test("TCount: ties with TWeight under equal stake (sanity check)") {
    // Sanity check: under equal-weight stake, T_count and T_weight should qualify the same
    // ordinal whenever the cluster's attestation set is uniform. 3-peer cluster, both non-self
    // peers attest the canonical tip — T_weight sees 2/2 active = 1.0 ≥ 2/3 → qualifies ord=50.
    // T_count sees 2 non-self attesters, ceil(2/3 * 3) = 2 required → qualifies ord=50.
    val self = pid("self")
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val tipHash = hash("tip-at-50")
    for {
      (tracker, registry) <- setupTracker(Set(self, peer1, peer2))
      _ <- record(tracker, peer1, att(tipHash, slot(50), 50L, 1000L))
      _ <- record(tracker, peer2, att(tipHash, slot(50), 50L, 1000L))
      tWeight <- TWeightTrigger.make[IO](tracker, TipTracker.FinalityThreshold)
      tCount <- TCountTrigger.make[IO](tracker, registry, TipTracker.FinalityThreshold)
      st = state(self, 50L, tipHash, Map(50L -> tipHash))
      weightResult <- tWeight.evaluate(st)
      countResult <- tCount.evaluate(st)
    } yield expect.same(weightResult, countResult) && expect.same(ord(50L), countResult)
  }

  test("TCount: returns MinValue when count is below threshold") {
    // 4-peer cluster, ceil(2/3 * 4) = 3 required. Only 2 non-self peers attest → count = 2 < 3.
    val self = pid("self")
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipHash = hash("tip-at-50")
    for {
      (tracker, registry) <- setupTracker(Set(self, peer1, peer2, peer3))
      _ <- record(tracker, peer1, att(tipHash, slot(50), 50L, 1000L))
      _ <- record(tracker, peer2, att(tipHash, slot(50), 50L, 1000L))
      // peer3 does NOT attest
      trigger <- TCountTrigger.make[IO](tracker, registry, TipTracker.FinalityThreshold)
      st = state(self, 50L, tipHash, Map(50L -> tipHash))
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

  test("TDepth2: evaluates to bestTipOrdinal - k₂") {
    // Mirrors TDepth1 structurally: pure subtraction of bestTipOrdinal minus the depth constant.
    // Use a small k₂ here for test ergonomics; the production default is 65536.
    val self = pid("self")
    val tipHash = hash("tip")
    val k2 = 1000L
    for {
      trigger <- TDepth2Trigger.make[IO](k2)
      st = state(self, 5000L, tipHash)
      result <- trigger.evaluate(st)
    } yield expect.same(ord(4000L), result)
  }

  test("TDepth2: returns MinValue when chain is shorter than k₂") {
    // At the production k₂ = 65536 the chain will be shorter than k₂ for the first ~tens of thousands
    // of ordinals; clamp to MinValue so Phase-3 sinks never see a wraparound or negative ordinal.
    val self = pid("self")
    val tipHash = hash("tip")
    val k2 = 65536L
    for {
      trigger <- TDepth2Trigger.make[IO](k2)
      st = state(self, 100L, tipHash) // bestTip far below k₂
      result <- trigger.evaluate(st)
    } yield expect.same(SnapshotOrdinal.MinValue, result)
  }

  test("TDepth2 with k₂=65536 fires later than TDepth1 with k=255 (Phase 2 → Phase 3 strictly after Phase 1 → Phase 2)") {
    // The 4-phase finality model requires that ARCHIVAL (Phase 3) qualification trails SETTLED
    // (Phase 2) qualification — once a snapshot is depth-k₂ deep, it has trivially been depth-k₁
    // deep for tens of thousands of snapshots already. This is the structural invariant that lets
    // Phase-3 sinks (overlay history pruning, future Mithril cert, light-client anchor) safely
    // assume Phase 2 finality has already fired. Sanity-check: at any bestTip > k₂, the TDepth1
    // qualifying ordinal strictly exceeds the TDepth2 qualifying ordinal.
    val self = pid("self")
    val tipHash = hash("tip")
    val k1 = 255L
    val k2 = 65536L
    for {
      tDepth1 <- TDepth1Trigger.make[IO](k1)
      tDepth2 <- TDepth2Trigger.make[IO](k2)
      // bestTip well beyond k₂ so both triggers produce a real ordinal.
      st = state(self, 100000L, tipHash)
      depth1Result <- tDepth1.evaluate(st)
      depth2Result <- tDepth2.evaluate(st)
    } yield
      expect.same(ord(99745L), depth1Result) &&
        expect.same(ord(34464L), depth2Result) &&
        // Strict ordering: TDepth2 qualifies a LOWER ordinal (older snapshots), which is the same as
        // saying TDepth2 "fires later" in time — for any given snapshot N, we cross k₁ depth before
        // we cross k₂ depth.
        expect(depth2Result.value.value < depth1Result.value.value)
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

  test("triggersFor over all four triggers — only the triggers that qualify N are returned (#138 building block)") {
    // Scenario for the chain-quality observable (task #138): at finalize time we want to know
    // exactly which subset of the four triggers (T_weight, T_count, T_depth1, T_depth2) has
    // qualified the just-finalized ordinal. This test sets up a state where:
    //   - T_weight qualifies ord=50 (3/4 peers on canonical chain @ ord=50, exceeds 2/3)
    //   - T_count qualifies ord=50 (3 non-self peers, ceil(2/3 * 4) = 3 → exactly meets)
    //   - T_depth1 qualifies ord=90 (bestTip=100, k=10)
    //   - T_depth2 qualifies MinValue (bestTip=100 < k₂=1000)
    // Then we query triggersFor at three ordinals: 50 (all three Phase-1→2 fire), 90 (only
    // T_depth1), 99 (none).
    val self = pid("self")
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipHash50 = hash("tip-at-50")
    val tipHash100 = hash("tip-at-100")
    val canonical = Map(50L -> tipHash50, 100L -> tipHash100)
    for {
      (tracker, registry) <- setupTracker(Set(self, peer1, peer2, peer3))
      _ <- record(tracker, peer1, att(tipHash50, slot(50), 50L, 1000L))
      _ <- record(tracker, peer2, att(tipHash50, slot(50), 50L, 1000L))
      _ <- record(tracker, peer3, att(tipHash50, slot(50), 50L, 1000L))
      tWeight <- TWeightTrigger.make[IO](tracker, TipTracker.FinalityThreshold)
      tCount <- TCountTrigger.make[IO](tracker, registry, TipTracker.FinalityThreshold)
      tDepth1 <- TDepth1Trigger.make[IO](10L)
      tDepth2 <- TDepth2Trigger.make[IO](1000L)
      all = List[FinalityTrigger[IO]](tWeight, tCount, tDepth1, tDepth2)
      st = state(self, 100L, tipHash100, canonical)
      // Advance every trigger
      _ <- all.traverse_(_.evaluateAndAdvance(st))
      at50 <- FinalityTrigger.triggersFor(all, ord(50L))
      at90 <- FinalityTrigger.triggersFor(all, ord(90L))
      at99 <- FinalityTrigger.triggersFor(all, ord(99L))
      // Sanity-check the individual latest ordinals match the design above.
      latestWeight <- tWeight.latestQualifyingOrdinal
      latestCount <- tCount.latestQualifyingOrdinal
      latestDepth1 <- tDepth1.latestQualifyingOrdinal
      latestDepth2 <- tDepth2.latestQualifyingOrdinal
    } yield
      // Pre-flight: triggers landed where we set them up.
      expect.same(ord(50L), latestWeight) &&
        expect.same(ord(50L), latestCount) &&
        expect.same(ord(90L), latestDepth1) &&
        expect.same(SnapshotOrdinal.MinValue, latestDepth2) &&
        // ord=50: all three Phase-1→2 triggers qualify (T_weight=50, T_count=50, T_depth1=90 ≥ 50).
        // T_depth2 still at MinValue (chain too short), so it does NOT qualify.
        expect.same(
          Set[FinalityTrigger.Kind](FinalityTrigger.Kind.TWeight, FinalityTrigger.Kind.TCount, FinalityTrigger.Kind.TDepth1),
          at50
        ) &&
        // ord=90: T_weight (50) and T_count (50) do not qualify. T_depth1 (90) qualifies. T_depth2 at MinValue.
        expect.same(Set[FinalityTrigger.Kind](FinalityTrigger.Kind.TDepth1), at90) &&
        // ord=99: T_depth1 only at 90 — too low. None qualify.
        expect.same(Set.empty[FinalityTrigger.Kind], at99)
  }

  test("FinalityTriggerView.fromTriggers wraps a trigger list and answers triggersFor correctly") {
    // Smoke test for the #138 view shim — confirms the wrapper preserves
    // FinalityTrigger.triggersFor semantics without caching or staleness.
    val self = pid("self")
    val tipHash = hash("tip")
    for {
      tracker <- setupTipTracker(Set(self, pid("peer1"), pid("peer2")))
      _ <- record(tracker, pid("peer1"), att(tipHash, slot(50), 50L, 1000L))
      _ <- record(tracker, pid("peer2"), att(tipHash, slot(50), 50L, 1000L))
      tWeight <- TWeightTrigger.make[IO](tracker, TipTracker.FinalityThreshold)
      tDepth1 <- TDepth1Trigger.make[IO](10L)
      view = FinalityTriggerView.fromTriggers[IO](List(tWeight, tDepth1))
      st = state(self, 100L, tipHash, Map(50L -> tipHash))
      // Initially no trigger has advanced — view returns empty.
      empty <- view.triggersFor(ord(50L))
      _ <- tWeight.evaluateAndAdvance(st)
      _ <- tDepth1.evaluateAndAdvance(st)
      // After advance, view reflects both.
      both <- view.triggersFor(ord(50L))
      depthOnly <- view.triggersFor(ord(90L))
    } yield
      expect.same(Set.empty[FinalityTrigger.Kind], empty) &&
        expect.same(Set[FinalityTrigger.Kind](FinalityTrigger.Kind.TWeight, FinalityTrigger.Kind.TDepth1), both) &&
        expect.same(Set[FinalityTrigger.Kind](FinalityTrigger.Kind.TDepth1), depthOnly)
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
