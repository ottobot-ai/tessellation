package io.constellationnetwork.node.shared.domain.nakamoto

import java.security.SecureRandom

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.vrf.EcVrf25519

import weaver.MutableIOSuite

/** Tests for [[CommitteeSortition]] — the per-metagraph committee VRF-threshold primitive.
  *
  * Coverage matches §10 of the design doc (`docs/nakamoto/COMMITTEE-SORTITION-DESIGN.md`):
  *   1. Determinism — same inputs → same proof bytes & in/out result. 2. Domain separation — committee VRF doesn't collide with leader VRF
  *      (same `(eta, sk)` but different consensus role). 3. Per-metagraph isolation — same `(eta, sk, parentHash)` across different
  *      metagraphs gives independent draws. 4. Per-parent rotation — committee changes per parent snapshot hash. 5. Verify roundtrip —
  *      proof produced by `isInCommittee` passes `verifyMembership`. 6. Negative tests — wrong VK / tampered proof / wrong metagraph /
  *      wrong eta → reject. 7. Threshold edge cases — σ=0 always out, K·σ ≥ 1 always in, K=0 forbidden. 8. Statistical inclusion rate —
  *      over many draws, mean committee size ≈ K · Σσ.
  */
object CommitteeSortitionSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], CommitteeSortition[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, CommitteeSortition.make[IO])

  private val vrf = new EcVrf25519()

  // Deterministic SHA1PRNG so the statistical inclusion-rate test is reproducible across CI runs.
  // `SecureRandom.getInstance("SHA1PRNG")` then setSeed BEFORE any draw makes the byte stream a pure
  // function of the seed (default constructor mixes in /dev/(u)random first, which would only
  // append). The VRF determinism tests don't depend on this, but the Chernoff-sanity test at K=50
  // has a ~10⁻⁵ tail-deviation probability per run; pinning the seed keeps the tolerance tight.
  private val random = {
    val r = SecureRandom.getInstance("SHA1PRNG")
    r.setSeed(0x53_4f_52_54_49_54_49L) // ASCII "SORTITI"
    r
  }

  private def randomSk(): Array[Byte] = {
    val sk = new Array[Byte](32)
    random.nextBytes(sk)
    sk
  }

  private def randomEta(): Array[Byte] = {
    val eta = new Array[Byte](32)
    random.nextBytes(eta)
    eta
  }

  /** Synthesize a test metagraph address from a deterministic byte tag. */
  private def metagraphAddr(tag: String): Address =
    Address.fromBytes(tag.getBytes("UTF-8"))

  /** Synthesize a test parent hash from a deterministic byte tag. SHA-256s the tag so we get a real Hash. */
  private def parent(tag: String): Hash =
    Hash.fromBytes(tag.getBytes("UTF-8"))

  // ============ Determinism ============

  test("isInCommittee is deterministic for identical inputs") {
    case (_, sortition) =>
      val sk = randomSk()
      val eta = randomEta()
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-A")
      val sigma = Ratio(1, 8)
      val k = 100

      for {
        r1 <- sortition.isInCommittee(sk, eta, addr, ph, sigma, k)
        r2 <- sortition.isInCommittee(sk, eta, addr, ph, sigma, k)
      } yield
        expect(r1.isDefined == r2.isDefined).and {
          (r1, r2) match {
            case (Some((p1, o1)), Some((p2, o2))) =>
              expect(p1.sameElements(p2)).and(expect(o1.sameElements(o2)))
            case _ => success
          }
        }
  }

  test("message bytes are deterministic and length-stable") {
    case (hasher, _) =>
      implicit val h: Hasher[IO] = hasher
      val eta = randomEta()
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-A")
      for {
        m1 <- CommitteeSortition.message[IO](eta, addr, ph)
        m2 <- CommitteeSortition.message[IO](eta, addr, ph)
      } yield expect(m1.sameElements(m2)).and(expect(m1.length == 64)) // SHA-256 hex string → 64 UTF-8 bytes
  }

  // ============ Domain separation (committee vs leader VRF) ============

  test("committee VRF output differs from a leader-VRF style output for the same (sk, eta)") {
    case (hasher, _) =>
      implicit val h: Hasher[IO] = hasher
      // The leader VRF hashes `eta ‖ slot` (per EligibilityChecker.vrfProofForSlot). The committee
      // VRF hashes `Hasher.hash(CommitteeVrfInput("committee", eta, addr, parentHash))`. These are distinct
      // messages — the proofs (and thus outputs) must differ for the same SK.
      val sk = randomSk()
      val eta = randomEta()
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-A")
      val slot = 50L

      val leaderMsg = eta ++ java.nio.ByteBuffer.allocate(8).putLong(slot).array()
      val leaderProof = vrf.vrfProof(sk, leaderMsg)

      for {
        commMsg <- CommitteeSortition.message[IO](eta, addr, ph)
        commProof = vrf.vrfProof(sk, commMsg)
      } yield expect(!commProof.sameElements(leaderProof))
  }

  // ============ Per-metagraph isolation ============

  test("different metagraph addresses produce independent committee draws") {
    case (hasher, _) =>
      implicit val h: Hasher[IO] = hasher
      val eta = randomEta()
      val addrA = metagraphAddr("metagraph-A")
      val addrB = metagraphAddr("metagraph-B")
      val ph = parent("parent-A")

      for {
        mA <- CommitteeSortition.message[IO](eta, addrA, ph)
        mB <- CommitteeSortition.message[IO](eta, addrB, ph)
      } yield expect(!mA.sameElements(mB))
  }

  // ============ Per-parent rotation ============

  test("different parent hashes produce different VRF outputs") {
    case (_, sortition) =>
      val sk = randomSk()
      val eta = randomEta()
      val addr = metagraphAddr("metagraph-A")
      val sigma = Ratio(1, 4)
      val k = 100

      for {
        r1 <- sortition.isInCommittee(sk, eta, addr, parent("p1"), sigma, k)
        r2 <- sortition.isInCommittee(sk, eta, addr, parent("p2"), sigma, k)
      } yield
        (r1, r2) match {
          case (Some((_, o1)), Some((_, o2))) => expect(!o1.sameElements(o2))
          case _ => success // not both selected — that's fine, this test only asserts output divergence when both fire
        }
  }

  // ============ Verify roundtrip ============

  test("isInCommittee + verifyMembership roundtrip succeeds for an in-committee key") {
    case (_, sortition) =>
      val eta = randomEta()
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-roundtrip")
      val sigma = Ratio.One // forces selection; K·σ = K ≥ 1 saturates threshold to 1
      val k = 100

      val sk = randomSk()
      val vk = vrf.getVerificationKey(sk)

      for {
        out <- sortition.isInCommittee(sk, eta, addr, ph, sigma, k)
        verified <- out match {
          case Some((proof, _)) =>
            sortition.verifyMembership(vk, eta, addr, ph, sigma, k, proof)
          case None => IO.pure(false)
        }
      } yield expect(out.isDefined).and(expect(verified, "verifyMembership must succeed for an in-committee key"))
  }

  // ============ Negative tests ============

  test("verifyMembership fails with wrong VRF VK") {
    case (_, sortition) =>
      val sk1 = randomSk()
      val sk2 = randomSk()
      val vk2 = vrf.getVerificationKey(sk2)
      val eta = randomEta()
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-neg-vk")
      val sigma = Ratio.One
      val k = 100

      for {
        out <- sortition.isInCommittee(sk1, eta, addr, ph, sigma, k)
        verified <- out match {
          case Some((proof, _)) => sortition.verifyMembership(vk2, eta, addr, ph, sigma, k, proof)
          case None             => IO.pure(false)
        }
      } yield expect(out.isDefined).and(expect(!verified))
  }

  test("verifyMembership fails with tampered proof") {
    case (_, sortition) =>
      val sk = randomSk()
      val vk = vrf.getVerificationKey(sk)
      val eta = randomEta()
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-neg-tamper")
      val sigma = Ratio.One
      val k = 100

      for {
        out <- sortition.isInCommittee(sk, eta, addr, ph, sigma, k)
        verified <- out match {
          case Some((proof, _)) =>
            val tampered = proof.clone()
            tampered(0) = (tampered(0) ^ 0xff.toByte).toByte
            sortition.verifyMembership(vk, eta, addr, ph, sigma, k, tampered)
          case None => IO.pure(false)
        }
      } yield expect(out.isDefined).and(expect(!verified))
  }

  test("verifyMembership fails with wrong metagraph address") {
    case (_, sortition) =>
      val sk = randomSk()
      val vk = vrf.getVerificationKey(sk)
      val eta = randomEta()
      val addrA = metagraphAddr("metagraph-A")
      val addrB = metagraphAddr("metagraph-B")
      val ph = parent("parent-neg-mg")
      val sigma = Ratio.One
      val k = 100

      for {
        out <- sortition.isInCommittee(sk, eta, addrA, ph, sigma, k)
        verified <- out match {
          case Some((proof, _)) => sortition.verifyMembership(vk, eta, addrB, ph, sigma, k, proof)
          case None             => IO.pure(false)
        }
      } yield expect(out.isDefined).and(expect(!verified))
  }

  test("verifyMembership fails with wrong eta") {
    case (_, sortition) =>
      val sk = randomSk()
      val vk = vrf.getVerificationKey(sk)
      val eta = randomEta()
      val wrongEta = randomEta()
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-neg-eta")
      val sigma = Ratio.One
      val k = 100

      for {
        out <- sortition.isInCommittee(sk, eta, addr, ph, sigma, k)
        verified <- out match {
          case Some((proof, _)) => sortition.verifyMembership(vk, wrongEta, addr, ph, sigma, k, proof)
          case None             => IO.pure(false)
        }
      } yield expect(out.isDefined).and(expect(!verified))
  }

  test("verifyMembership fails with wrong parent hash") {
    case (_, sortition) =>
      val sk = randomSk()
      val vk = vrf.getVerificationKey(sk)
      val eta = randomEta()
      val addr = metagraphAddr("metagraph-A")
      val phA = parent("parent-neg-A")
      val phB = parent("parent-neg-B")
      val sigma = Ratio.One
      val k = 100

      for {
        out <- sortition.isInCommittee(sk, eta, addr, phA, sigma, k)
        verified <- out match {
          case Some((proof, _)) => sortition.verifyMembership(vk, eta, addr, phB, sigma, k, proof)
          case None             => IO.pure(false)
        }
      } yield expect(out.isDefined).and(expect(!verified))
  }

  // ============ Threshold edge cases ============

  test("threshold scales linearly with σ when K · σ < 1") { _ =>
    // K=10, σ=1/100 → 10/100 = 0.1
    val t = CommitteeSortition.threshold(10, Ratio(1, 100))
    IO.pure(expect(t == Ratio(1, 10)))
  }

  test("threshold saturates at 1 when K · σ ≥ 1") { _ =>
    // K=10, σ=1/4 → 10·(1/4) = 5/2 → clamped to 1
    val t = CommitteeSortition.threshold(10, Ratio(1, 4))
    IO.pure(expect(t == Ratio.One))
  }

  test("σ=0 → threshold=0 → always out of committee") {
    case (_, sortition) =>
      val sk = randomSk()
      val eta = randomEta()
      val addr = metagraphAddr("metagraph-A")
      val k = 100

      // Check several parents; σ=0 must always exclude.
      for {
        results <- (1 to 20).toList.traverse { i =>
          sortition.isInCommittee(sk, eta, addr, parent(s"sigma-zero-$i"), Ratio.Zero, k)
        }
      } yield expect(results.forall(_.isEmpty))
  }

  test("σ ≥ 1/K (saturated) → always in committee") {
    case (_, sortition) =>
      val sk = randomSk()
      val eta = randomEta()
      val addr = metagraphAddr("metagraph-A")
      val k = 5
      val sigma = Ratio(1, 4) // 5 · 1/4 = 5/4 > 1, saturates

      for {
        results <- (1 to 20).toList.traverse { i =>
          sortition.isInCommittee(sk, eta, addr, parent(s"sigma-sat-$i"), sigma, k)
        }
      } yield expect(results.forall(_.isDefined))
  }

  test("K=0 is rejected by threshold") { _ =>
    IO.delay {
      val caught =
        try { CommitteeSortition.threshold(0, Ratio.One); false }
        catch { case _: IllegalArgumentException => true }
      expect(caught)
    }
  }

  // ============ Statistical inclusion-rate sanity check ============

  test("inclusion rate over many operators tracks K · σ (Chernoff sanity)") {
    case (_, sortition) =>
      // 1000 operator keys, each with σ = 1/1000 (so total stake = 1). K_target = 50.
      // Expected committee size = K · Σσ = 50. Chernoff variance ≤ K = 50.
      // We test that the observed rate is within a generous ±50% window (acceptance bound = 25..75)
      // — this is a sanity check on the primitive, not a tight bound.
      val n = 1000
      val k = 50
      val sigma = Ratio(1, n)
      val eta = randomEta()
      val addr = metagraphAddr("statistical-test")
      val ph = parent("statistical-parent")

      val sks = (1 to n).map(_ => randomSk()).toList

      for {
        selected <- sks.traverse(sortition.isInCommittee(_, eta, addr, ph, sigma, k))
        committeeSize = selected.count(_.isDefined)
      } yield expect(committeeSize >= 25 && committeeSize <= 75, s"Expected ~50 selected, got $committeeSize")
  }
}
