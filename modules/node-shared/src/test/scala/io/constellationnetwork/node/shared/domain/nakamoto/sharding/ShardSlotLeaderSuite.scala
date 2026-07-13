package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import java.security.SecureRandom

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.{CanonicalOperatorConsensusFixture, EligibilityChecker}
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.vrf.EcVrf25519

import weaver.MutableIOSuite

/** Tests for [[ShardSlotLeader]] deterministic shard eta and VRF membership proofs.
  *
  * Coverage:
  *   1. '''`computeShardEta` determinism''' — same `(shardId, gl0Eta)` ⇒ identical bytes across invocations. 2. '''`computeShardEta` domain
  *      separation''' — two different `shardId`s with same `gl0Eta` produce different shardEtas (§5.6 — the load-bearing property that
  *      per-shard slot lotteries don't collide). 3. '''`computeShardEta` length invariant''' — output is exactly 32 bytes (matches the
  *      invariant `EligibilityChecker.vrfProofForSlot` asserts on its `eta` argument). 4. '''Membership proof domain''' — proofs verify
  *      only under the matching registered key, shard eta, and eta period.
  */
object ShardSlotLeaderSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], EligibilityChecker[IO], ShardSlotLeader[IO], CanonicalOperatorConsensusFixture)

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      // ShardSlotLeader uses EligibilityChecker only for its canonical VRF proof primitive. Its arithmetic dependencies remain part of
      // EligibilityChecker construction even though shard checkpoint production does not use an LDD threshold.
      log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8).asResource
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38).asResource
      ec = EligibilityChecker.make[IO](log1p, exp)
      ssl = ShardSlotLeader.make[IO](ec)
      operator <- CanonicalOperatorConsensusFixture.make
    } yield (h, ec, ssl, operator)

  private val vrf = new EcVrf25519()

  private val staircasePeers: List[PeerId] =
    List(1, 2, 3).map(n => PeerId(Hex(f"$n%02x" * 64)))

  // Deterministic SHA1PRNG so proof tests are reproducible across CI runs. `setSeed` BEFORE `nextBytes` makes the byte stream a
  // pure function of the seed (default constructor mixes in /dev/(u)random first, which would only append entropy). Matches the seeding
  // pattern in CommitteeSortitionSuite and ShardAssignmentSuite.
  private val random = {
    val r = SecureRandom.getInstance("SHA1PRNG")
    r.setSeed(0x53_48_41_52_44_53_4cL) // ASCII "SHARDSL"
    r
  }

  private def randomGl0Eta(): Array[Byte] = {
    val eta = new Array[Byte](32)
    random.nextBytes(eta)
    eta
  }

  // ============ §1 computeShardEta determinism =============================

  test("scheduledDuty follows exact delta windows, wraps, and widens only genesis") { _ =>
    val parent = Slot.unsafeApply(100L)
    val nonGenesis = List(101L, 105L, 106L, 110L, 111L, 115L, 116L).map { slot =>
      ShardSlotLeader.scheduledDuty(staircasePeers, Slot.unsafeApply(slot), parent.some, staircaseDeltaSlots = 5)
    }
    val genesisBeforeHandoff = ShardSlotLeader.scheduledDuty(staircasePeers, Slot.unsafeApply(60L), None, staircaseDeltaSlots = 5)
    val genesisAfterHandoff = ShardSlotLeader.scheduledDuty(staircasePeers, Slot.unsafeApply(61L), None, staircaseDeltaSlots = 5)

    (expect.same(List(0, 0, 1, 1, 2, 2, 0), nonGenesis.flatMap(_.toOption.map(_.rank))) &&
      expect.same(0, genesisBeforeHandoff.toOption.map(_.rank).get) &&
      expect.same(1, genesisAfterHandoff.toOption.map(_.rank).get)).pure[IO]
  }

  test("scheduledDuty rejects empty committees, invalid delta, and non-monotone child slots") { _ =>
    val slot = Slot.unsafeApply(100L)
    (expect(ShardSlotLeader.scheduledDuty(Nil, slot, None, 5).isLeft) &&
      expect(ShardSlotLeader.scheduledDuty(staircasePeers, slot, None, 0).isLeft) &&
      expect(ShardSlotLeader.scheduledDuty(staircasePeers, slot, slot.some, 5).isLeft) &&
      expect(ShardSlotLeader.scheduledDuty(staircasePeers, Slot.unsafeApply(99L), slot.some, 5).isLeft)).pure[IO]
  }

  test("SHARD-C-011 remains RED: identical parent, roster, and slot select different duty under local delta 5 vs 10") { _ =>
    val parent = Slot.unsafeApply(100L)
    val child = Slot.unsafeApply(106L)
    val delta5 = ShardSlotLeader.scheduledDuty(staircasePeers, child, parent.some, staircaseDeltaSlots = 5)
    val delta10 = ShardSlotLeader.scheduledDuty(staircasePeers, child, parent.some, staircaseDeltaSlots = 10)

    (expect.same(1, delta5.toOption.map(_.rank).get) &&
      expect.same(0, delta10.toOption.map(_.rank).get) &&
      expect(delta5.toOption.map(_.peerId) =!= delta10.toOption.map(_.peerId))).pure[IO]
  }

  test("computeShardEta is deterministic — same (shardId, gl0Eta) ⇒ identical bytes across calls") {
    case (hasher, _, ssl, _) =>
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
    case (hasher, _, ssl, _) =>
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
    case (hasher, _, ssl, _) =>
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
    case (hasher, _, ssl, _) =>
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
    case (hasher, _, ssl, _) =>
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

  // ============ §4 per-period eta rotation + producer/verifier agreement ====
  //
  // Slice S4 makes `shardEta` rotate per eta-period instead of being pinned to genesis randomness. The producer/verifier
  // wiring resolves `shardEta = computeShardEta(shardId, etaForPeriod(epoch))` keyed on the CHECKPOINT'S epoch. These tests
  // model that resolver (a period → gl0Eta map) at the `ShardSlotLeader` level and pin the two load-bearing properties:
  //   (a) ROTATION: distinct periods (distinct gl0 etas) ⇒ distinct shard etas, so membership proofs change across eta rotations.
  //   (b) DETERMINISM INVARIANT: producer + verifier, both keying the SAME resolver on the SAME `(shardId, epoch)`, derive a
  //       byte-identical shardEta and a membership proof round-trips; keying on the WRONG epoch derives a different eta under
  //       which the proof does NOT verify (the exact failure mode that would stall a shard chain if a verifier used the
  //       wall-clock period instead of `checkpoint.epoch`).

  /** A per-period gl0-eta resolver — the unit-test analog of production's `etaForPeriodCallback` (`EtaStateManager.getEta`). Each period
    * maps to a distinct 32-byte gl0 eta; the shard layer derives `computeShardEta(shardId, etaForPeriod(epoch))`.
    */
  private def etaForPeriod(period: EtaPeriod): IO[Array[Byte]] = IO.pure {
    val eta = new Array[Byte](ShardSlotLeader.EtaLength)
    // Deterministic, period-dependent fill — distinct per period so two periods give distinct gl0 etas (and therefore,
    // by §2's domain-separation property, distinct shard etas). Not a real eta derivation; just a stable per-period seed.
    java.util.Arrays.fill(eta, (0x10 + period.value.toInt).toByte)
    eta
  }

  /** The production resolver shape: `(shardId, epoch) => F[shardEta]` = `computeShardEta(shardId, etaForPeriod(epoch))`. */
  private def resolveShardEta(ssl: ShardSlotLeader[IO], shardId: ShardId, epoch: EtaPeriod)(implicit h: Hasher[IO]): IO[Array[Byte]] =
    etaForPeriod(epoch).flatMap(gl0Eta => ssl.computeShardEta(shardId, gl0Eta))

  test("S4 rotation: computeShardEta over two distinct period etas yields distinct shard etas") {
    case (hasher, _, ssl, _) =>
      implicit val h: Hasher[IO] = hasher
      val shardId = ShardId.unsafeApply(2)
      for {
        etaP0 <- resolveShardEta(ssl, shardId, EtaPeriod(0L))
        etaP1 <- resolveShardEta(ssl, shardId, EtaPeriod(1L))
        etaP2 <- resolveShardEta(ssl, shardId, EtaPeriod(2L))
        // Same period ⇒ identical (the resolver is pure), confirming the rotation is keyed on the period, not nondeterminism.
        etaP1again <- resolveShardEta(ssl, shardId, EtaPeriod(1L))
      } yield
        expect(!java.util.Arrays.equals(etaP0, etaP1), "period 0 and period 1 shard etas must differ (rotation)")
          .and(expect(!java.util.Arrays.equals(etaP1, etaP2), "period 1 and period 2 shard etas must differ (rotation)"))
          .and(expect(!java.util.Arrays.equals(etaP0, etaP2), "period 0 and period 2 shard etas must differ (rotation)"))
          .and(expect(java.util.Arrays.equals(etaP1, etaP1again), "same period ⇒ identical shard eta (deterministic)"))
  }

  test(
    "producer and verifier keyed on the same (shardId, epoch) derive identical shardEta and a membership proof round-trips"
  ) {
    case (hasher, _, ssl, operator) =>
      implicit val h: Hasher[IO] = hasher
      val shardId = ShardId.unsafeApply(5)
      val epoch = EtaPeriod(7L)
      val slot = Slot.unsafeApply(42L)
      val sk = operator.localVrfSecret
      val vk = operator.resolvedPair.vrfPublicKey.toBytes

      for {
        registered <- operator.operatorKeyRegistry.get(operator.resolvedPair.operatorPeerId)
        producerEta <- resolveShardEta(ssl, shardId, epoch)
        verifierEta <- resolveShardEta(ssl, shardId, epoch)
        proof <- ssl.membershipProof(sk, producerEta, slot)
        message = verifierEta ++ java.nio.ByteBuffer.allocate(8).putLong(slot.value.value).array()
      } yield
        expect(registered.exists(_.operatorPeerId == operator.resolvedPair.operatorPeerId), "operator pair must resolve from genesis")
          .and(
            expect(
              java.util.Arrays.equals(producerEta, verifierEta),
              "producer and verifier shard etas for the same epoch must be byte-identical"
            )
          )
          .and(expect(vrf.vrfVerify(vk, message, proof), "membership proof verifies under the registered same-epoch key"))
  }

  test("a verifier keyed on the wrong epoch derives a different eta and rejects the membership proof") {
    case (hasher, _, ssl, operator) =>
      implicit val h: Hasher[IO] = hasher
      val shardId = ShardId.unsafeApply(5)
      val producerEpoch = EtaPeriod(7L)
      val wrongEpoch = EtaPeriod(8L)
      val slot = Slot.unsafeApply(42L)
      val sk = operator.localVrfSecret
      val vk = operator.resolvedPair.vrfPublicKey.toBytes

      for {
        producerEta <- resolveShardEta(ssl, shardId, producerEpoch)
        wrongEpochEta <- resolveShardEta(ssl, shardId, wrongEpoch)
        _ = require(!java.util.Arrays.equals(producerEta, wrongEpochEta), "test setup: the two epoch etas must differ")
        proof <- ssl.membershipProof(sk, producerEta, slot)
        producerMessage = producerEta ++ java.nio.ByteBuffer.allocate(8).putLong(slot.value.value).array()
        wrongEpochMessage = wrongEpochEta ++ java.nio.ByteBuffer.allocate(8).putLong(slot.value.value).array()
      } yield
        expect(vrf.vrfVerify(vk, producerMessage, proof), "control: proof verifies under the producer's epoch eta")
          .and(
            expect(
              !vrf.vrfVerify(vk, wrongEpochMessage, proof),
              "proof for the producer's epoch must not verify under a wrong-epoch eta"
            )
          )
  }
}
