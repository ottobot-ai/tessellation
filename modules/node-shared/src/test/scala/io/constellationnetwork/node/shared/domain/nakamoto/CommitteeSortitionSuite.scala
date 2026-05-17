package io.constellationnetwork.node.shared.domain.nakamoto

import java.security.SecureRandom

import cats.effect.IO
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.vrf.EcVrf25519

import weaver.SimpleIOSuite

/** Tests for [[CommitteeSortition]] — the per-metagraph committee VRF-threshold primitive.
  *
  * Coverage matches §10 of the design doc (`docs/nakamoto/COMMITTEE-SORTITION-DESIGN.md`):
  *   1. Determinism — same inputs → same proof bytes & in/out result.
  *   2. Domain separation — committee VRF doesn't collide with leader VRF (same `(eta, sk)` but different
  *      consensus role).
  *   3. Per-metagraph isolation — same `(eta, sk, snapshotOrd)` across different metagraphs gives
  *      independent draws.
  *   4. Per-snapshot rotation — committee changes per snapshot ordinal.
  *   5. Verify roundtrip — proof produced by `isInCommittee` passes `verifyMembership`.
  *   6. Negative tests — wrong VK / tampered proof / wrong metagraph / wrong eta → reject.
  *   7. Threshold edge cases — σ=0 always out, K·σ ≥ 1 always in, K=0 forbidden.
  *   8. Statistical inclusion rate — over many draws, mean committee size ≈ K · Σσ.
  */
object CommitteeSortitionSuite extends SimpleIOSuite {

  private val vrf = new EcVrf25519()
  private val sortition: CommitteeSortition[IO] = CommitteeSortition.make[IO]
  private val random = new SecureRandom()

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

  // ============ Determinism ============

  test("isInCommittee is deterministic for identical inputs") {
    val sk = randomSk()
    val eta = randomEta()
    val addr = metagraphAddr("metagraph-A")
    val sigma = Ratio(1, 8)
    val k = 100

    for {
      r1 <- sortition.isInCommittee(sk, eta, addr, 42L, sigma, k)
      r2 <- sortition.isInCommittee(sk, eta, addr, 42L, sigma, k)
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
    val eta = randomEta()
    val addr = metagraphAddr("metagraph-A")
    val m1 = CommitteeSortition.message(eta, addr, 100L)
    val m2 = CommitteeSortition.message(eta, addr, 100L)
    IO.pure(expect(m1.sameElements(m2)).and(expect(m1.length == 32)))
  }

  // ============ Domain separation (committee vs leader VRF) ============

  test("committee VRF output differs from a leader-VRF style output for the same (sk, eta)") {
    // The leader VRF hashes `eta ‖ slot` (per EligibilityChecker.vrfProofForSlot). The committee
    // VRF hashes `Blake2b(eta ‖ addr ‖ ord ‖ "committee")`. These are distinct messages — the proofs
    // (and thus outputs) must differ for the same SK.
    val sk = randomSk()
    val eta = randomEta()
    val addr = metagraphAddr("metagraph-A")
    val snapshotOrd = 50L

    val commMsg = CommitteeSortition.message(eta, addr, snapshotOrd)
    val commProof = vrf.vrfProof(sk, commMsg)

    val leaderMsg = eta ++ java.nio.ByteBuffer.allocate(8).putLong(snapshotOrd).array()
    val leaderProof = vrf.vrfProof(sk, leaderMsg)

    IO.pure(expect(!commProof.sameElements(leaderProof)))
  }

  // ============ Per-metagraph isolation ============

  test("different metagraph addresses produce independent committee draws") {
    val sk = randomSk()
    val eta = randomEta()
    val addrA = metagraphAddr("metagraph-A")
    val addrB = metagraphAddr("metagraph-B")

    val mA = CommitteeSortition.message(eta, addrA, 100L)
    val mB = CommitteeSortition.message(eta, addrB, 100L)

    IO.pure(expect(!mA.sameElements(mB)))
  }

  // ============ Per-snapshot rotation ============

  test("different snapshot ordinals produce different VRF outputs") {
    val sk = randomSk()
    val eta = randomEta()
    val addr = metagraphAddr("metagraph-A")
    val sigma = Ratio(1, 4)
    val k = 100

    for {
      r1 <- sortition.isInCommittee(sk, eta, addr, 1L, sigma, k)
      r2 <- sortition.isInCommittee(sk, eta, addr, 2L, sigma, k)
    } yield (r1, r2) match {
      case (Some((_, o1)), Some((_, o2))) => expect(!o1.sameElements(o2))
      case _ => success // not both selected — that's fine, this test only asserts output divergence when both fire
    }
  }

  // ============ Verify roundtrip ============

  test("isInCommittee + verifyMembership roundtrip succeeds for an in-committee key") {
    val eta = randomEta()
    val addr = metagraphAddr("metagraph-A")
    val sigma = Ratio.One // forces selection; K·σ = K ≥ 1 saturates threshold to 1
    val k = 100

    val sk = randomSk()
    val vk = vrf.getVerificationKey(sk)

    for {
      out <- sortition.isInCommittee(sk, eta, addr, 7L, sigma, k)
      verified <- out match {
        case Some((proof, _)) =>
          sortition.verifyMembership(vk, eta, addr, 7L, sigma, k, proof)
        case None => IO.pure(false)
      }
    } yield expect(out.isDefined).and(expect(verified, "verifyMembership must succeed for an in-committee key"))
  }

  // ============ Negative tests ============

  test("verifyMembership fails with wrong VRF VK") {
    val sk1 = randomSk()
    val sk2 = randomSk()
    val vk2 = vrf.getVerificationKey(sk2)
    val eta = randomEta()
    val addr = metagraphAddr("metagraph-A")
    val sigma = Ratio.One
    val k = 100

    for {
      out <- sortition.isInCommittee(sk1, eta, addr, 1L, sigma, k)
      verified <- out match {
        case Some((proof, _)) => sortition.verifyMembership(vk2, eta, addr, 1L, sigma, k, proof)
        case None             => IO.pure(false)
      }
    } yield expect(out.isDefined).and(expect(!verified))
  }

  test("verifyMembership fails with tampered proof") {
    val sk = randomSk()
    val vk = vrf.getVerificationKey(sk)
    val eta = randomEta()
    val addr = metagraphAddr("metagraph-A")
    val sigma = Ratio.One
    val k = 100

    for {
      out <- sortition.isInCommittee(sk, eta, addr, 1L, sigma, k)
      verified <- out match {
        case Some((proof, _)) =>
          val tampered = proof.clone()
          tampered(0) = (tampered(0) ^ 0xff.toByte).toByte
          sortition.verifyMembership(vk, eta, addr, 1L, sigma, k, tampered)
        case None => IO.pure(false)
      }
    } yield expect(out.isDefined).and(expect(!verified))
  }

  test("verifyMembership fails with wrong metagraph address") {
    val sk = randomSk()
    val vk = vrf.getVerificationKey(sk)
    val eta = randomEta()
    val addrA = metagraphAddr("metagraph-A")
    val addrB = metagraphAddr("metagraph-B")
    val sigma = Ratio.One
    val k = 100

    for {
      out <- sortition.isInCommittee(sk, eta, addrA, 1L, sigma, k)
      verified <- out match {
        case Some((proof, _)) => sortition.verifyMembership(vk, eta, addrB, 1L, sigma, k, proof)
        case None             => IO.pure(false)
      }
    } yield expect(out.isDefined).and(expect(!verified))
  }

  test("verifyMembership fails with wrong eta") {
    val sk = randomSk()
    val vk = vrf.getVerificationKey(sk)
    val eta = randomEta()
    val wrongEta = randomEta()
    val addr = metagraphAddr("metagraph-A")
    val sigma = Ratio.One
    val k = 100

    for {
      out <- sortition.isInCommittee(sk, eta, addr, 1L, sigma, k)
      verified <- out match {
        case Some((proof, _)) => sortition.verifyMembership(vk, wrongEta, addr, 1L, sigma, k, proof)
        case None             => IO.pure(false)
      }
    } yield expect(out.isDefined).and(expect(!verified))
  }

  // ============ Threshold edge cases ============

  test("threshold scales linearly with σ when K · σ < 1") {
    // K=10, σ=1/100 → 10/100 = 0.1
    val t = CommitteeSortition.threshold(10, Ratio(1, 100))
    IO.pure(expect(t == Ratio(1, 10)))
  }

  test("threshold saturates at 1 when K · σ ≥ 1") {
    // K=10, σ=1/4 → 10·(1/4) = 5/2 → clamped to 1
    val t = CommitteeSortition.threshold(10, Ratio(1, 4))
    IO.pure(expect(t == Ratio.One))
  }

  test("σ=0 → threshold=0 → always out of committee") {
    val sk = randomSk()
    val eta = randomEta()
    val addr = metagraphAddr("metagraph-A")
    val k = 100

    // Check several snapshot ords; σ=0 must always exclude.
    for {
      results <- (1 to 20).toList.traverse { ord =>
        sortition.isInCommittee(sk, eta, addr, ord.toLong, Ratio.Zero, k)
      }
    } yield expect(results.forall(_.isEmpty))
  }

  test("σ ≥ 1/K (saturated) → always in committee") {
    val sk = randomSk()
    val eta = randomEta()
    val addr = metagraphAddr("metagraph-A")
    val k = 5
    val sigma = Ratio(1, 4) // 5 · 1/4 = 5/4 > 1, saturates

    for {
      results <- (1 to 20).toList.traverse { ord =>
        sortition.isInCommittee(sk, eta, addr, ord.toLong, sigma, k)
      }
    } yield expect(results.forall(_.isDefined))
  }

  test("K=0 is rejected by threshold") {
    IO.delay {
      val caught =
        try { CommitteeSortition.threshold(0, Ratio.One); false }
        catch { case _: IllegalArgumentException => true }
      expect(caught)
    }
  }

  // ============ Statistical inclusion-rate sanity check ============

  test("inclusion rate over many operators tracks K · σ (Chernoff sanity)") {
    // 1000 operator keys, each with σ = 1/1000 (so total stake = 1). K_target = 50.
    // Expected committee size = K · Σσ = 50. Chernoff variance ≤ K = 50.
    // We test that the observed rate is within a generous ±50% window (acceptance bound = 25..75)
    // — this is a sanity check on the primitive, not a tight bound.
    val n = 1000
    val k = 50
    val sigma = Ratio(1, n)
    val eta = randomEta()
    val addr = metagraphAddr("statistical-test")

    val sks = (1 to n).map(_ => randomSk()).toList

    for {
      selected <- sks.traverse(sortition.isInCommittee(_, eta, addr, 1L, sigma, k))
      committeeSize = selected.count(_.isDefined)
    } yield expect(committeeSize >= 25 && committeeSize <= 75, s"Expected ~50 selected, got $committeeSize")
  }
}
