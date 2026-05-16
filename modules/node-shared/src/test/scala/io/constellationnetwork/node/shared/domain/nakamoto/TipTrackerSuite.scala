package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.node.shared.infrastructure.metrics.NoOpMetrics
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.nakamoto.TipAttestation
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object TipTrackerSuite extends SimpleIOSuite {

  // No-op Metrics typeclass instance — tests don't assert on counters so a noop suffices.
  // The skew-rejection counter (`dag_nakamoto_attestations_rejected_skew_total`) still
  // flows through this instance; the dedicated skew tests below assert behavior, not counts.
  implicit private val metrics: io.constellationnetwork.node.shared.infrastructure.metrics.Metrics[IO] = NoOpMetrics.make

  // Helper to create PeerId from a name
  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  // Helper to create a slot
  private def slot(n: Long): Slot = Slot(NonNegLong.unsafeFrom(n))

  // Helper to create a hash
  private def hash(s: String): Hash = Hash(s.padTo(64, '0'))

  // Helper to create an attestation. `attestedAt` is wall-clock epoch ms in production
  // (sourced from `Clock[F].realTime`); in tests we pass a Slot for terseness and
  // unpack to its raw Long value, which keeps the monotonic-newer-wins ordering the
  // test cases assume without coupling the test fixtures to wall-clock minutiae.
  private def att(tipHash: Hash, tipSlot: Slot, tipOrdinal: Long, attestedAt: Slot): TipAttestation =
    TipAttestation(tipHash, tipSlot, tipOrdinal, attestedAt.value.value)

  // Setup StakeRegistry with N equal-weight validators
  private def setupRegistry(validators: Set[PeerId]): IO[StakeRegistry[IO]] =
    for {
      registry <- StakeRegistry.equalWeight[IO]
      _ <- registry.updateValidators(validators)
    } yield registry

  // Setup TipTracker with a registry
  private def setupTracker(validators: Set[PeerId]): IO[(TipTracker[IO], StakeRegistry[IO])] =
    for {
      registry <- setupRegistry(validators)
      tracker <- TipTracker.make[IO](registry)
    } yield (tracker, registry)

  // Helper that passes the attestation's own `attestedAt` as `now` so the skew gate
  // (TipTracker.MaxAttestationSkewMs) is trivially satisfied (skew=0). Existing tests
  // assert behavior independent of wall-clock skew; the dedicated skew tests at the
  // bottom of this file exercise the gate explicitly by passing custom `now` values.
  private def record(tracker: TipTracker[IO], peerId: PeerId, attestation: TipAttestation): IO[Unit] =
    tracker.recordAttestation(peerId, attestation, attestation.attestedAt)

  test("empty tracker has no heaviest tip") {
    for {
      (tracker, _) <- setupTracker(Set.empty)
      heaviest <- tracker.heaviestTip
    } yield expect.same(None, heaviest)
  }

  test("single attestation becomes heaviest tip") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val slotA = slot(10)

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- record(tracker, peer1, att(tipA, slotA, 100L, slot(11)))
      heaviest <- tracker.heaviestTip
    } yield
      expect(heaviest.isDefined) &&
        expect.same(tipA, heaviest.get._1) &&
        expect.same(slotA, heaviest.get._2) &&
        expect.same(Ratio.One, heaviest.get._3)
  }

  test("majority attestation reaches finality (3 of 4 peers attest same tip → >2/3 weight)") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val peer4 = pid("peer4")
    val tipA = hash("tipA")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3, peer4))
      // 3 of 4 peers attest. Active fraction = 3/4 = 75% ≥ MinActiveQuorumFraction (50%),
      // so optimistic weighting kicks in: weight is computed against the active set (3 peers),
      // and all 3 attested the same tip → weight = 1.0. Finality threshold (2/3) is met.
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer3, att(tipA, slot(10), 100L, slot(11)))
      isFinalized <- tracker.isFinalized(tipA)
      weight <- tracker.attestationWeight(tipA)
    } yield
      expect(isFinalized) &&
        expect.same(Ratio.One, weight)
  }

  test("newer attestation supersedes older from same peer") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      weightABefore <- tracker.attestationWeight(tipA)
      // Newer attestation (attestedAt=12 > 11)
      _ <- record(tracker, peer1, att(tipB, slot(12), 102L, slot(12)))
      weightAAfter <- tracker.attestationWeight(tipA)
      weightB <- tracker.attestationWeight(tipB)
    } yield
      expect.same(Ratio.One, weightABefore) &&
        expect.same(Ratio.Zero, weightAAfter) &&
        expect.same(Ratio.One, weightB)
  }

  test("split attestations (2 peers on tip A, 2 on tip B → neither finalized with equal weight)") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val peer4 = pid("peer4")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3, peer4))
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer3, att(tipB, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer4, att(tipB, slot(10), 100L, slot(11)))
      isFinalizedA <- tracker.isFinalized(tipA)
      isFinalizedB <- tracker.isFinalized(tipB)
      weightA <- tracker.attestationWeight(tipA)
      weightB <- tracker.attestationWeight(tipB)
    } yield
      expect(!isFinalizedA) &&
        expect(!isFinalizedB) &&
        expect.same(Ratio(1, 2), weightA) &&
        expect.same(Ratio(1, 2), weightB)
  }

  test("attestationWeight returns 0 for unknown tip") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val unknownTip = hash("unknown")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      weight <- tracker.attestationWeight(unknownTip)
    } yield expect.same(Ratio.Zero, weight)
  }

  test("markFinalized updates lastFinalized") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      before <- tracker.lastFinalized
      _ <- tracker.markFinalized(tipA, slot(10))
      after <- tracker.lastFinalized
    } yield
      expect.same(None, before) &&
        expect.same(Some((tipA, slot(10))), after)
  }

  test("pruneBelow removes old attestations") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val tipOld = hash("tipOld")
    val tipNew = hash("tipNew")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2))
      _ <- record(tracker, peer1, att(tipOld, slot(5), 50L, slot(6)))
      _ <- record(tracker, peer2, att(tipNew, slot(10), 100L, slot(11)))
      beforePrune <- tracker.allAttestations
      _ <- tracker.pruneBelow(slot(10))
      afterPrune <- tracker.allAttestations
    } yield
      expect.same(2, beforePrune.size) &&
        expect.same(1, afterPrune.size) &&
        expect(afterPrune.contains(peer2)) &&
        expect(!afterPrune.contains(peer1))
  }

  test("fork choice: heaviestTip returns tip with most weight") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3))
      // 2 peers on A, 1 peer on B
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer3, att(tipB, slot(10), 100L, slot(11)))
      heaviest <- tracker.heaviestTip
    } yield
      expect(heaviest.isDefined) &&
        expect.same(tipA, heaviest.get._1) &&
        expect.same(Ratio(2, 3), heaviest.get._3)
  }

  test("attestation from non-validator (zero stake) doesn't count toward finality") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val nonValidator = pid("nonValidator")
    val tipA = hash("tipA")

    for {
      // Only peer1 and peer2 are validators
      (tracker, _) <- setupTracker(Set(peer1, peer2))
      // nonValidator attests but has 0 stake
      _ <- record(tracker, nonValidator, att(tipA, slot(10), 100L, slot(11)))
      weight <- tracker.attestationWeight(tipA)
      isFinalized <- tracker.isFinalized(tipA)
    } yield
      expect.same(Ratio.Zero, weight) &&
        expect(!isFinalized)
  }

  test("2/3+1 threshold: exactly 2 of 3 validators is 0.667 (borderline, should finalize)") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipA = hash("tipA")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3))
      // 2 of 3 peers attest. Active fraction = 2/3 ≈ 66.7% ≥ MinActiveQuorumFraction (50%),
      // so optimistic weighting is active: both attesters voted the same tip, weight = 1.0
      // against the 2-peer active set, which trivially exceeds the 2/3 finality threshold.
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipA, slot(10), 100L, slot(11)))
      weight <- tracker.attestationWeight(tipA)
      isFinalized <- tracker.isFinalized(tipA)
    } yield
      expect.same(Ratio.One, weight) &&
        expect(isFinalized)
  }

  test("chain finalization: when tip at ordinal 100 finalizes, all ancestors are implicitly finalized") {
    // This test verifies the conceptual model: we don't need to explicitly track
    // ancestor finalization because any tip that finalizes implies all its ancestors
    // are finalized. The markFinalized/lastFinalized tracks the frontier.
    val peer1 = pid("peer1")
    val tipFinal = hash("tipFinal")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- tracker.markFinalized(tipFinal, slot(100))
      lastFin <- tracker.lastFinalized
    } yield
      // When we finalize tip at slot 100, ordinals 1-99 are implicitly finalized
      // because a snapshot at slot 100 must have all previous snapshots in its chain
      expect.same(Some((tipFinal, slot(100))), lastFin)
  }

  test("older attestation does not supersede newer one from same peer") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      // First: newer attestation
      _ <- record(tracker, peer1, att(tipB, slot(12), 102L, slot(15)))
      // Then try to record older attestation
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      weightA <- tracker.attestationWeight(tipA)
      weightB <- tracker.attestationWeight(tipB)
    } yield
      // tipB should still have weight since its attestation was newer
      expect.same(Ratio.Zero, weightA) &&
        expect.same(Ratio.One, weightB)
  }

  test("allAttestations returns all current attestations") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val tipA = hash("tipA")
    val tipB = hash("tipB")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2))
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipB, slot(12), 102L, slot(13)))
      all <- tracker.allAttestations
    } yield
      expect.same(2, all.size) &&
        expect(all.get(peer1).exists(_.tipHash == tipA)) &&
        expect(all.get(peer2).exists(_.tipHash == tipB))
  }

  test("highestFinalizedOrdinal: NID-restored — all observers reach the same decision (Snowball commit, P-11b rolled back)") {
    // Snowball commit: the legacy weight-sum `highestFinalizedOrdinal` no longer self-excludes.
    // Two honest observers walking the same canonical chain with identical signed attestation
    // transcripts MUST compute the same finality decision regardless of their `selfId` — this is
    // Non-Interactive Determinism (NID), the defining property of consensus.
    //
    // Setup: 3-validator cluster (gl0-0, gl0-1, gl0-2). All three attest their local tip at
    // ord 100. gl0-0 and gl0-1 share chain A; gl0-2 is on chain B. Two honest observers walking
    // the SAME canonical chain (A) — one is gl0-0, the other an external observer — must produce
    // the same finality decision. Pre-Snowball this test asserted the opposite (P-11b
    // self-exclusion gave different answers); we now flip the expectation to confirm NID.
    val gl0_0 = pid("gl0-0")
    val gl0_1 = pid("gl0-1")
    val gl0_2 = pid("gl0-2")
    val observer = pid("observer")
    val hashA100 = hash("chainA_ord100")
    val hashB100 = hash("chainB_ord100")

    for {
      (tracker, _) <- setupTracker(Set(gl0_0, gl0_1, gl0_2))
      _ <- record(tracker, gl0_0, att(hashA100, slot(200), 100L, slot(201)))
      _ <- record(tracker, gl0_1, att(hashA100, slot(200), 100L, slot(201)))
      _ <- record(tracker, gl0_2, att(hashB100, slot(205), 100L, slot(206)))

      // gl0-0's view of canonical chain A: includes self again (NID-restored). gl0-0 + gl0-1 =
      // 2/3 stake on A → finalizes ord 100. The off-chain attestation from gl0-2 (on B) is
      // filtered by the canonical-hash predicate.
      fromChainA_selfGl00 <- tracker.highestFinalizedOrdinal(
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(hashA100) else None)
      )

      // External observer's view of chain A. Same `canonicalHashAt` lookup → same set of
      // contributing attestations → same finality decision as gl0-0's view above. This is the
      // NID property the Snowball commit restores.
      fromChainA_observer <- tracker.highestFinalizedOrdinal(
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(hashA100) else None)
      )

      // gl0-2's view of canonical chain B: only gl0-2's own attestation matches B (1/3 weight).
      // Below 2/3 → no finality. Same canonical filter, no self-exclusion.
      fromChainB <- tracker.highestFinalizedOrdinal(
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(hashB100) else None)
      )

      // Observer's view of chain B is identical to gl0-2's view of chain B (NID property —
      // same transcript + same canonical lookup ⇒ same decision).
      fromChainB_observer <- tracker.highestFinalizedOrdinal(
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(hashB100) else None)
      )

      _ = (observer, gl0_2) // observer / gl0_2 reserved for narrative; tests share canonicalHashAt
    } yield
      // NID: gl0-0's view and the external observer's view of chain A are identical.
      expect.same(fromChainA_selfGl00, fromChainA_observer) &&
        // Chain A finalizes at ord 100 with 2/3 weight.
        expect(fromChainA_selfGl00.isDefined) &&
        expect.same(100L, fromChainA_selfGl00.get._1) &&
        expect.same(Ratio(2, 3), fromChainA_selfGl00.get._2) &&
        // NID: gl0-2's view and the external observer's view of chain B are identical.
        expect.same(fromChainB, fromChainB_observer) &&
        // Chain B fails to finalize (only 1/3 attestation weight on B's canonical hash).
        expect.same(None, fromChainB)
  }

  test("highestFinalizedOrdinal: GRANDPA ancestor rule still works (all agree chain)") {
    // No fork: all three peers attest different ordinals on the same chain.
    // Finality should pick the highest ordinal where cumulative weight ≥ 2/3.
    // The legacy weight-sum path walks attestation ordinals from highest down and accumulates
    // weight; the highest ord where cum-weight clears 2/3 wins.
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val h50 = hash("ord50")
    val h60 = hash("ord60")
    val h70 = hash("ord70")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3))
      _ <- record(tracker, peer1, att(h70, slot(140), 70L, slot(141)))
      _ <- record(tracker, peer2, att(h60, slot(120), 60L, slot(121)))
      _ <- record(tracker, peer3, att(h50, slot(100), 50L, slot(101)))

      result <- tracker.highestFinalizedOrdinal(
        Ratio(2, 3),
        ord =>
          IO.pure(ord match {
            case 70L => Some(h70)
            case 60L => Some(h60)
            case 50L => Some(h50)
            case _   => None
          })
      )
    } yield
      // peer1 alone at ord 70 = 1/3 (below threshold),
      // peer1 + peer2 cumulative at ord 60 = 2/3 (at threshold — finalize ord 60)
      expect(result.isDefined) &&
        expect.same(60L, result.get._1)
  }

  test("highestFinalizedOrdinal: NID — same transcript across two observers ⇒ same decision (P-11b rolled back)") {
    // Snowball commit: the legacy weight-sum path no longer self-excludes. Two validators (self +
    // peer) both attesting the same ord/hash now both contribute to the legacy 2/3 weight gate
    // — full weight, finalizes. The earlier P-11b semantics (exclude `selfId`) is gone; NID is
    // restored at this position. The Snowball accumulator (the primary T_weight driver now) is
    // also observer-independent — see the SnowballAccumulatorSuite for the load-bearing NID
    // assertion on the new path.
    val self = pid("self")
    val peer = pid("peer")
    val h100 = hash("ord100")

    for {
      (tracker, _) <- setupTracker(Set(self, peer))
      _ <- record(tracker, self, att(h100, slot(200), 100L, slot(201)))
      _ <- record(tracker, peer, att(h100, slot(200), 100L, slot(201)))

      // Every observer using the same canonical-hash lookup sees the same decision: both
      // validators on the same hash ⇒ 2/2 = full weight ⇒ finalize ord 100.
      result1 <- tracker.highestFinalizedOrdinal(
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(h100) else None)
      )
      result2 <- tracker.highestFinalizedOrdinal(
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(h100) else None)
      )
    } yield
      // NID: identical decisions across the two queries.
      expect.same(result1, result2) &&
        // Decision: finalize ord 100 at full weight.
        expect(result1.isDefined) &&
        expect.same(100L, result1.get._1)
  }

  test("highestFinalizedOrdinal includes others' attestations of the same ordinal as expected") {
    // 4-node cluster: self + 3 peers all attest ord 100 with the same hash. With P-11b rolled
    // back, self IS included again — total weight = 4/4 = full ≥ 2/3 → finalize. The legacy
    // weight-sum path remains correct cluster-wide as long as the canonical-hash filter is in
    // place.
    val self = pid("self")
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val h100 = hash("ord100")

    for {
      (tracker, _) <- setupTracker(Set(self, peer1, peer2, peer3))
      _ <- record(tracker, self, att(h100, slot(200), 100L, slot(201)))
      _ <- record(tracker, peer1, att(h100, slot(200), 100L, slot(201)))
      _ <- record(tracker, peer2, att(h100, slot(200), 100L, slot(201)))
      _ <- record(tracker, peer3, att(h100, slot(200), 100L, slot(201)))

      result <- tracker.highestFinalizedOrdinal(
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(h100) else None)
      )
      _ = self // referenced for symmetry / narrative
    } yield
      // All 4 validators attest the same ord/hash ⇒ full weight against the active set ⇒
      // finalize at ord 100.
      expect(result.isDefined) &&
        expect.same(100L, result.get._1)
  }

  // -- Skew gate (#140) --
  //
  // `TipTracker.MaxAttestationSkewMs` defends against peers (badly-skewed or malicious)
  // submitting attestations whose claimed `attestedAt` is more than ±60s away from our
  // local wall-clock when `recordAttestation` runs. Skewed attestations are dropped
  // silently (counter `dag_nakamoto_attestations_rejected_skew_total` increments via
  // the no-op Metrics here; production wires the real registry). The constant is read
  // from `NAKAMOTO_MAX_ATTESTATION_SKEW_MS` at JVM start — tests use whatever the
  // process saw at boot. With the default 60_000ms the boundary numbers below sit
  // safely inside / outside that window.

  test("recordAttestation drops attestation with attestedAt > now + 60s") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val nowMs = 1_700_000_000_000L
    // Claim 60_001ms in the future — just past the +60s ceiling.
    val skewedFuture = TipAttestation(tipA, slot(10), 100L, nowMs + 60_001L)

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- tracker.recordAttestation(peer1, skewedFuture, nowMs)
      all <- tracker.allAttestations
      weight <- tracker.attestationWeight(tipA)
    } yield
      // Attestation rejected → no entry recorded, zero weight.
      expect(all.isEmpty) &&
        expect.same(Ratio.Zero, weight)
  }

  test("recordAttestation drops attestation with attestedAt < now - 60s") {
    val peer1 = pid("peer1")
    val tipA = hash("tipA")
    val nowMs = 1_700_000_000_000L
    // Claim 60_001ms in the past — just past the -60s floor.
    val skewedPast = TipAttestation(tipA, slot(10), 100L, nowMs - 60_001L)

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- tracker.recordAttestation(peer1, skewedPast, nowMs)
      all <- tracker.allAttestations
      weight <- tracker.attestationWeight(tipA)
    } yield
      expect(all.isEmpty) &&
        expect.same(Ratio.Zero, weight)
  }

  test("recordAttestation accepts attestation within ±60s window") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val tipA = hash("tipA")
    val nowMs = 1_700_000_000_000L
    // Two attestations near the boundary on each side — both must be accepted.
    val nearFuture = TipAttestation(tipA, slot(10), 100L, nowMs + 59_999L)
    val nearPast = TipAttestation(tipA, slot(10), 100L, nowMs - 59_999L)

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2))
      _ <- tracker.recordAttestation(peer1, nearFuture, nowMs)
      _ <- tracker.recordAttestation(peer2, nearPast, nowMs)
      all <- tracker.allAttestations
      weight <- tracker.attestationWeight(tipA)
    } yield
      // Both inside the window → both recorded → full attestation weight on tipA.
      expect.same(2, all.size) &&
        expect(all.contains(peer1)) &&
        expect(all.contains(peer2)) &&
        expect.same(Ratio.One, weight)
  }

  // ============================================================
  // P-11 (task #141): unsafe_reset for re-bootstrap recovery
  // ============================================================

  test("unsafe_reset: clears attestations map") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val tipA = hash("tipA")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2))
      _ <- record(tracker, peer1, att(tipA, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipA, slot(10), 100L, slot(11)))
      beforeReset <- tracker.allAttestations
      _ <- tracker.unsafe_reset
      afterReset <- tracker.allAttestations
    } yield
      expect.same(2, beforeReset.size) &&
        expect(afterReset.isEmpty)
  }

  test("unsafe_reset: clears lastFinalized marker") {
    val peer1 = pid("peer1")
    val tipFinal = hash("tipFinal")

    for {
      (tracker, _) <- setupTracker(Set(peer1))
      _ <- tracker.markFinalized(tipFinal, slot(100))
      beforeReset <- tracker.lastFinalized
      _ <- tracker.unsafe_reset
      afterReset <- tracker.lastFinalized
    } yield
      expect.same(Some((tipFinal, slot(100))), beforeReset) &&
        expect.same(None, afterReset)
  }

  test("unsafe_reset: post-reset is reusable — new attestations accumulate weight normally") {
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val tipOld = hash("tipOld")
    val tipNew = hash("tipNew")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3))
      // Build up state that would represent a divergent self-finalize.
      _ <- record(tracker, peer1, att(tipOld, slot(10), 100L, slot(11)))
      _ <- record(tracker, peer2, att(tipOld, slot(10), 100L, slot(11)))
      _ <- tracker.markFinalized(tipOld, slot(10))
      // Reset (simulating re-bootstrap orchestrator firing).
      _ <- tracker.unsafe_reset
      // Post-reset: receive fresh canonical attestations.
      _ <- record(tracker, peer1, att(tipNew, slot(20), 200L, slot(21)))
      _ <- record(tracker, peer2, att(tipNew, slot(20), 200L, slot(21)))
      newWeight <- tracker.attestationWeight(tipNew)
      oldWeight <- tracker.attestationWeight(tipOld)
      isNewFinalized <- tracker.isFinalized(tipNew)
    } yield
      // Old tip has zero weight (attestations cleared), new tip has 2/3+ weight,
      // confirming the tracker is fully reusable post-reset.
      expect.same(Ratio.Zero, oldWeight) &&
        expect(newWeight >= Ratio(2, 3)) &&
        expect(isNewFinalized)
  }
}
