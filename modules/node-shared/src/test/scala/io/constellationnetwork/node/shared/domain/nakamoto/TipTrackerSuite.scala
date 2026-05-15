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

  test("highestFinalizedOrdinal skips attestations not on our canonical chain (fork scenario)") {
    // Simulates 3-node cluster where gl0-2 forked. gl0-0/gl0-1 share chain A;
    // gl0-2 is on chain B. All three attest their local tip at ordinal 100 with
    // different hashes. For a gl0-0 caller (canonical chain = A, self=gl0-0),
    // only gl0-1's attestation counts (gl0-0 is excluded as self, gl0-2 is
    // off-chain) → 1/3 < 2/3, no finality. For a gl0-2 caller (canonical chain
    // = B, self=gl0-2), no peer attestations match B → no finality. The third
    // viewpoint — an external observer who is none of the validators — sees
    // gl0-0 + gl0-1 = 2/3 on A → finality on A. This combines two safety rules:
    // the hash-agnostic-fork bug AND the self-finalization (#119/#133) bug.
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

      // gl0-0's view: canonical chain A → ord 100 canonical hash = hashA100.
      // gl0-0 is excluded as self; gl0-1's matching attestation alone = 1/3 < 2/3.
      fromChainA_selfGl00 <- tracker.highestFinalizedOrdinal(
        gl0_0,
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(hashA100) else None)
      )

      // External observer's view of chain A: no self-exclusion (observer is not
      // a validator), gl0-0 + gl0-1 = 2/3 stake agree on A's ord 100 → finalize.
      fromChainA_observer <- tracker.highestFinalizedOrdinal(
        observer,
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(hashA100) else None)
      )

      // gl0-2's view: canonical chain B → ordinal 100 canonical hash = hashB100.
      // gl0-2 is excluded as self; no peer attestations match B → no finality.
      fromChainB <- tracker.highestFinalizedOrdinal(
        gl0_2,
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(hashB100) else None)
      )
    } yield
      // gl0-0's self-view of chain A: 1/3 only (gl0-1 alone after self-exclusion)
      expect.same(None, fromChainA_selfGl00) &&
        // Observer (no self-exclusion): sees gl0-0 + gl0-1 = 2/3 on A, finalizes
        expect(fromChainA_observer.isDefined) &&
        expect.same(100L, fromChainA_observer.get._1) &&
        expect.same(Ratio(2, 3), fromChainA_observer.get._2) &&
        // gl0-2's view of chain B: self-excluded + no peers attest B = no finality
        expect.same(None, fromChainB)
  }

  test("highestFinalizedOrdinal: GRANDPA ancestor rule still works (all agree chain)") {
    // No fork: all three peers attest different ordinals on the same chain.
    // Finality should pick the highest ordinal where cumulative weight ≥ 2/3.
    // From an external observer's viewpoint (selfId not in validator set), all
    // three attestations count and finality picks the highest ord where weight
    // cumulates past 2/3.
    val peer1 = pid("peer1")
    val peer2 = pid("peer2")
    val peer3 = pid("peer3")
    val observer = pid("observer")
    val h50 = hash("ord50")
    val h60 = hash("ord60")
    val h70 = hash("ord70")

    for {
      (tracker, _) <- setupTracker(Set(peer1, peer2, peer3))
      _ <- record(tracker, peer1, att(h70, slot(140), 70L, slot(141)))
      _ <- record(tracker, peer2, att(h60, slot(120), 60L, slot(121)))
      _ <- record(tracker, peer3, att(h50, slot(100), 50L, slot(101)))

      result <- tracker.highestFinalizedOrdinal(
        observer,
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

  test("highestFinalizedOrdinal excludes self-attestation — node attesting its own ordinal doesn't count toward its threshold") {
    // Task #133: a node MUST NOT count its own attestation toward its own
    // finality threshold. With 2 validators (self + peer) attesting the same
    // ordinal on the same hash, an external observer would see 2/2 = full
    // weight, but the self-view must drop its own attestation, leaving only
    // 1/2 — below the 2/3 threshold. This prevents self-finalization on a
    // divergent fork that would then trip the finality-safety gate of #119.
    val self = pid("self")
    val peer = pid("peer")
    val h100 = hash("ord100")

    for {
      (tracker, _) <- setupTracker(Set(self, peer))
      _ <- record(tracker, self, att(h100, slot(200), 100L, slot(201)))
      _ <- record(tracker, peer, att(h100, slot(200), 100L, slot(201)))

      // Self-view: self-attestation excluded, peer alone = 1/2 < 2/3 → no finality
      selfView <- tracker.highestFinalizedOrdinal(
        self,
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(h100) else None)
      )

      // Peer's view: self-attestation (its own) excluded but the OTHER
      // validator's attestation still counts → 1/2 also below 2/3.
      peerView <- tracker.highestFinalizedOrdinal(
        peer,
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(h100) else None)
      )

      // Observer's view: no self-exclusion. Both attestations of the same hash
      // give 2/2 = full weight — past 2/3, finalize ord 100.
      observerView <- tracker.highestFinalizedOrdinal(
        pid("observer"),
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(h100) else None)
      )
    } yield
      expect.same(None, selfView) &&
        expect.same(None, peerView) &&
        expect(observerView.isDefined) &&
        expect.same(100L, observerView.get._1)
  }

  test("highestFinalizedOrdinal includes others' attestations of the same ordinal as expected") {
    // Task #133 sibling: confirm that excluding self does NOT over-exclude.
    // 4-node cluster: self + 3 peers all attest ord 100 with the same hash.
    // Self-view: self-attestation dropped, 3 peers = 3/4 ≥ 2/3 → finalize.
    // This proves the self-filter is precise — it only drops the self entry,
    // not other attestations on the same ordinal/hash.
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

      selfView <- tracker.highestFinalizedOrdinal(
        self,
        Ratio(2, 3),
        ord => IO.pure(if (ord == 100L) Some(h100) else None)
      )
    } yield
      // 3 peers attesting the same ord/hash = full weight against the 3-peer
      // (post-self-exclusion) active set under optimistic weighting → finalize.
      expect(selfView.isDefined) &&
        expect.same(100L, selfView.get._1)
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
}
