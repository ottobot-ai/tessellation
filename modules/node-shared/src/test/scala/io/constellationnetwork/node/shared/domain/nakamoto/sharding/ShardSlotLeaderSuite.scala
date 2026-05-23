package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import java.security.SecureRandom

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.vrf.EcVrf25519

import weaver.MutableIOSuite

/** Tests for [[ShardSlotLeader]] — the per-shard slot leader VRF wrapper around
  * [[io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker]] (slice 7 of hierarchical-shard-checkpoints v1, §5.3 + §5.6).
  *
  * Coverage:
  *   1. '''`computeShardEta` determinism''' — same `(shardId, gl0Eta)` ⇒ identical bytes across invocations.
  *   2. '''`computeShardEta` domain separation''' — two different `shardId`s with same `gl0Eta` produce different shardEtas (§5.6 — the
  *      load-bearing property that per-shard slot lotteries don't collide).
  *   3. '''`computeShardEta` length invariant''' — output is exactly 32 bytes (matches the invariant `EligibilityChecker.vrfProofForSlot`
  *      asserts on its `eta` argument; a regression here would surface as an IllegalArgumentException at the first `isLeader` call).
  *   4. '''`isLeader` expected fire rate''' — over a sweep of 1000 slots with K_S=4 committee members at σ=1/4 each, the per-validator
  *      fire count matches the LDD-derived expectation within a generous tolerance (sanity bound, not Chernoff-tight).
  *   5. '''`isLeader` ↔ `verifyLeader` round-trip''' — a leader's proof verifies under their VK for the same `(shardEta, slot, σ, ldd)`.
  *   6. '''`verifyLeader` rejects on wrong shardEta''' — a proof for shardEta_X does NOT verify under shardEta_Y (cross-shard isolation
  *      at the verifier; a defensive check that catches both eta-derivation drift and a confused-deputy attack across shards).
  */
object ShardSlotLeaderSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], EligibilityChecker[IO], ShardSlotLeader[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      // Match Bifrost prod precision (log1p=8, exp=38) so the test thresholds we compute below are byte-identical to what runtime
      // would compute — the fire-rate sanity test depends on threshold-arithmetic determinism.
      log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8).asResource
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38).asResource
      ec = EligibilityChecker.make[IO](log1p, exp)
      ssl = ShardSlotLeader.make[IO](ec)
    } yield (h, ec, ssl)

  private val vrf = new EcVrf25519()

  // Deterministic SHA1PRNG so the fire-rate test is reproducible across CI runs. `setSeed` BEFORE `nextBytes` makes the byte stream a
  // pure function of the seed (default constructor mixes in /dev/(u)random first, which would only append entropy). Matches the seeding
  // pattern in CommitteeSortitionSuite and ShardAssignmentSuite.
  private val random = {
    val r = SecureRandom.getInstance("SHA1PRNG")
    r.setSeed(0x53_48_41_52_44_53_4cL) // ASCII "SHARDSL"
    r
  }

  private def randomSk(): Array[Byte] = {
    val sk = new Array[Byte](32)
    random.nextBytes(sk)
    sk
  }

  private def randomGl0Eta(): Array[Byte] = {
    val eta = new Array[Byte](32)
    random.nextBytes(eta)
    eta
  }

  // ============ §1 computeShardEta determinism =============================

  test("computeShardEta is deterministic — same (shardId, gl0Eta) ⇒ identical bytes across calls") {
    case (hasher, _, ssl) =>
      implicit val h: Hasher[IO] = hasher
      val shardId = ShardId.unsafeApply(3)
      val gl0Eta = randomGl0Eta()

      for {
        eta1 <- ssl.computeShardEta(shardId, gl0Eta)
        eta2 <- ssl.computeShardEta(shardId, gl0Eta)
        eta3 <- ssl.computeShardEta(shardId, gl0Eta)
      } yield
        expect(java.util.Arrays.equals(eta1, eta2))
          .and(expect(java.util.Arrays.equals(eta2, eta3)))
  }

  // ============ §2 computeShardEta domain separation across shards ==========

  test("computeShardEta domain-separates across shards — different shardIds with same gl0Eta ⇒ different shardEtas") {
    case (hasher, _, ssl) =>
      implicit val h: Hasher[IO] = hasher
      val gl0Eta = randomGl0Eta()
      val shardA = ShardId.unsafeApply(0)
      val shardB = ShardId.unsafeApply(1)
      val shardC = ShardId.unsafeApply(99)

      for {
        etaA <- ssl.computeShardEta(shardA, gl0Eta)
        etaB <- ssl.computeShardEta(shardB, gl0Eta)
        etaC <- ssl.computeShardEta(shardC, gl0Eta)
      } yield
        expect(!java.util.Arrays.equals(etaA, etaB))
          .and(expect(!java.util.Arrays.equals(etaA, etaC)))
          .and(expect(!java.util.Arrays.equals(etaB, etaC)))
  }

  test("computeShardEta also varies with gl0Eta at fixed shardId") {
    case (hasher, _, ssl) =>
      implicit val h: Hasher[IO] = hasher
      val shardId = ShardId.unsafeApply(7)
      val gl0EtaA = randomGl0Eta()
      val gl0EtaB = randomGl0Eta()

      for {
        etaA <- ssl.computeShardEta(shardId, gl0EtaA)
        etaB <- ssl.computeShardEta(shardId, gl0EtaB)
      } yield expect(!java.util.Arrays.equals(etaA, etaB))
  }

  // ============ §3 computeShardEta length invariant =========================

  test("computeShardEta produces a 32-byte eta — matches EligibilityChecker.vrfProofForSlot invariant") {
    case (hasher, _, ssl) =>
      implicit val h: Hasher[IO] = hasher
      val gl0Eta = randomGl0Eta()
      // Sweep across several shardIds because the encoding shape depends on Circe's Int rendering width — a regression in the encoder
      // would not necessarily affect every shardId equally.
      val shardIds = List(0, 1, 7, 99, 1000, Int.MaxValue).map(ShardId.unsafeApply)
      for {
        etas <- shardIds.traverse(ssl.computeShardEta(_, gl0Eta))
      } yield expect(etas.forall(_.length == ShardSlotLeader.EtaLength))
  }

  test("computeShardEta rejects wrong-length gl0Eta") {
    case (hasher, _, ssl) =>
      implicit val h: Hasher[IO] = hasher
      val shardId = ShardId.unsafeApply(0)
      val shortEta = new Array[Byte](16)
      val longEta = new Array[Byte](64)
      for {
        shortAttempt <- ssl.computeShardEta(shardId, shortEta).attempt
        longAttempt <- ssl.computeShardEta(shardId, longEta).attempt
      } yield
        expect(shortAttempt.isLeft)
          .and(expect(longAttempt.isLeft))
  }

  // ============ §4 isLeader fire-rate sanity check ==========================

  test("isLeader fire rate over 1000 slots tracks LDD threshold expectation at K_S=4, σ=1/4") {
    case (hasher, ec, ssl) =>
      implicit val h: Hasher[IO] = hasher
      // 4-member committee at uniform σ. Default LDD config: fA=1/2, fB=1/20, ψ=1, γ=15. We use a fixed slotGap = 100 ⇒ recovery
      // region holds (slotGap > γ) ⇒ difficulty = fB = 1/20. At σ=1/4 the per-slot threshold is
      //   t = 1 - (1 - 1/20)^(1/4) = 1 - (19/20)^(1/4) ≈ 1 - 0.987342 = 0.012658
      // Expected fires over 1000 slots ≈ 1000 · 0.012658 ≈ 12.66. With Bernoulli variance σ²_fires ≈ N·t·(1-t) ≈ 12.50 ⇒ sd ≈ 3.54.
      // A ±50% window (≈ ±6.33 ≈ 1.79σ) is the sanity bound (Chernoff-tight is not the goal here).
      val cfg = LddConfig.Default
      val shardId = ShardId.unsafeApply(0)
      val gl0Eta = randomGl0Eta()
      val sigma = Ratio(1, 4)
      val slotGap = 100L
      val numSlots = 1000

      val sk = randomSk()
      val totalExpectedFiresPerValidator = for {
        threshold <- ec.threshold(sigma, slotGap, cfg)
        // Convert exact Ratio to Double only to set up the test tolerance — the threshold ITSELF in the runtime path stays exact.
      } yield (numSlots * threshold.toDouble)

      for {
        shardEta <- ssl.computeShardEta(shardId, gl0Eta)
        expectedFires <- totalExpectedFiresPerValidator
        // Iterate 1..numSlots and count fires. Use IO.iterate for stack safety on long sweeps.
        fires <- (1L to numSlots.toLong).toList.traverse { i =>
          ssl.isLeader(sk, shardEta, Slot.unsafeApply(i), slotGap, sigma, cfg).map(_.isDefined)
        }.map(_.count(identity))
        // Acceptance window: ±50% of the expectation. With expectation ≈ 12.66 ⇒ [6.33, 18.99] ⇒ [6, 19] after rounding down/up.
        // _, _ minimums avoid an empty window for tiny expectations (defensive against future config changes).
        lower = math.max(0, math.floor(expectedFires * 0.5).toInt)
        upper = math.ceil(expectedFires * 1.5).toInt
      } yield
        // Validators ALSO need to satisfy: expected window is non-trivial (lower < upper). If a future LDD-config edit makes the
        // expectation tiny, this assertion guards against a vacuously-passing test.
        expect(upper > lower, s"Test setup error: trivial acceptance window [$lower, $upper] for expected ≈ $expectedFires")
          .and(
            expect(
              fires >= lower && fires <= upper,
              s"Observed $fires fires over $numSlots slots; expected ≈ $expectedFires (window [$lower, $upper], 4 K_S, σ=1/4, slotGap=100)"
            )
          )
  }

  test("isLeader fires zero times when sigmaInCommittee = 0") {
    case (hasher, _, ssl) =>
      implicit val h: Hasher[IO] = hasher
      val cfg = LddConfig.Default
      val sk = randomSk()
      val gl0Eta = randomGl0Eta()
      // σ=0 ⇒ threshold = 1 - (1-f)^0 = 0 ⇒ nothing ever fires. This is the contract `EligibilityChecker.threshold` provides and
      // we mirror it here to guard against a wrapper that accidentally forces a default σ.
      for {
        shardEta <- ssl.computeShardEta(ShardId.unsafeApply(0), gl0Eta)
        fires <- (1L to 100L).toList.traverse { i =>
          ssl.isLeader(sk, shardEta, Slot.unsafeApply(i), 100L, Ratio.Zero, cfg).map(_.isDefined)
        }
      } yield expect(fires.forall(_ == false))
  }

  // ============ §5 isLeader ↔ verifyLeader round-trip =======================

  test("isLeader output verifies under verifyLeader for the same (shardEta, slot, σ, ldd)") {
    case (hasher, _, ssl) =>
      implicit val h: Hasher[IO] = hasher
      val cfg = LddConfig.Default
      val sigma = Ratio(1, 2) // higher σ ⇒ more wins per slot, so we find a winning slot faster
      val slotGap = 100L
      val sk = randomSk()
      val vk = vrf.getVerificationKey(sk)
      val gl0Eta = randomGl0Eta()

      // Loop until we find one winning slot, up to N attempts.
      def loop(shardEta: Array[Byte], attempt: Int): IO[Boolean] =
        if (attempt >= 1000) IO.pure(false)
        else {
          val slot = Slot.unsafeApply(attempt.toLong)
          ssl.isLeader(sk, shardEta, slot, slotGap, sigma, cfg).flatMap {
            case Some((proof, _)) =>
              ssl.verifyLeader(vk, shardEta, slot, slotGap, sigma, cfg, proof).flatMap {
                case true  => IO.pure(true)
                case false => loop(shardEta, attempt + 1) // verification failed — keep searching (should not happen for a valid round-trip)
              }
            case None => loop(shardEta, attempt + 1)
          }
        }

      for {
        shardEta <- ssl.computeShardEta(ShardId.unsafeApply(0), gl0Eta)
        verified <- loop(shardEta, 0)
      } yield expect(verified, "round-trip should find at least one winning slot in 1000 attempts at σ=1/2")
  }

  // ============ §6 verifyLeader rejects wrong shardEta ======================

  test("verifyLeader rejects a leader's proof when checked against a different shardEta") {
    case (hasher, _, ssl) =>
      implicit val h: Hasher[IO] = hasher
      val cfg = LddConfig.Default
      val sigma = Ratio(1, 2)
      val slotGap = 100L
      val sk = randomSk()
      val vk = vrf.getVerificationKey(sk)
      val gl0Eta = randomGl0Eta()
      val shardX = ShardId.unsafeApply(1)
      val shardY = ShardId.unsafeApply(2)

      // Find a slot where this operator wins under shardX, then verify the proof against shardY's eta — must reject.
      def loop(shardEtaX: Array[Byte], shardEtaY: Array[Byte], attempt: Int): IO[Option[(Slot, Array[Byte])]] =
        if (attempt >= 1000) IO.pure(None)
        else {
          val slot = Slot.unsafeApply(attempt.toLong)
          ssl.isLeader(sk, shardEtaX, slot, slotGap, sigma, cfg).flatMap {
            case Some((proof, _)) => IO.pure(Some((slot, proof)))
            case None             => loop(shardEtaX, shardEtaY, attempt + 1)
          }
        }

      for {
        shardEtaX <- ssl.computeShardEta(shardX, gl0Eta)
        shardEtaY <- ssl.computeShardEta(shardY, gl0Eta)
        // Defensive precondition: the two etas must actually differ (already covered by §2 but worth a local sanity check).
        _ = require(!java.util.Arrays.equals(shardEtaX, shardEtaY), "test setup: shardEtaX and shardEtaY must differ")
        winOpt <- loop(shardEtaX, shardEtaY, 0)
        outcome <- winOpt match {
          case Some((slot, proof)) =>
            for {
              verifiedUnderX <- ssl.verifyLeader(vk, shardEtaX, slot, slotGap, sigma, cfg, proof)
              verifiedUnderY <- ssl.verifyLeader(vk, shardEtaY, slot, slotGap, sigma, cfg, proof)
            } yield expect(verifiedUnderX, "control: proof verifies under shardEtaX")
              .and(expect(!verifiedUnderY, "cross-shard isolation: proof for shardX MUST NOT verify under shardY's eta"))
          case None =>
            IO.pure(failure("did not find a winning slot for the cross-shard isolation test in 1000 attempts"))
        }
      } yield outcome
  }

  test("verifyLeader rejects a proof signed under a different VK") {
    case (hasher, _, ssl) =>
      implicit val h: Hasher[IO] = hasher
      val cfg = LddConfig.Default
      val sigma = Ratio(1, 2)
      val slotGap = 100L
      val sk1 = randomSk()
      val sk2 = randomSk()
      val vkOther = vrf.getVerificationKey(sk2)
      val gl0Eta = randomGl0Eta()

      def loop(shardEta: Array[Byte], attempt: Int): IO[Option[(Slot, Array[Byte])]] =
        if (attempt >= 1000) IO.pure(None)
        else {
          val slot = Slot.unsafeApply(attempt.toLong)
          ssl.isLeader(sk1, shardEta, slot, slotGap, sigma, cfg).flatMap {
            case Some((proof, _)) => IO.pure(Some((slot, proof)))
            case None             => loop(shardEta, attempt + 1)
          }
        }

      for {
        shardEta <- ssl.computeShardEta(ShardId.unsafeApply(0), gl0Eta)
        winOpt <- loop(shardEta, 0)
        outcome <- winOpt match {
          case Some((slot, proof)) =>
            ssl.verifyLeader(vkOther, shardEta, slot, slotGap, sigma, cfg, proof).map(v => expect(!v))
          case None => IO.pure(failure("did not find a winning slot for the wrong-VK test in 1000 attempts"))
        }
      } yield outcome
  }
}
