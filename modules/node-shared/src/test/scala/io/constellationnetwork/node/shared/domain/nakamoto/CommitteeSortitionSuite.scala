package io.constellationnetwork.node.shared.domain.nakamoto

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash

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

  override type Res =
    (Hasher[IO], CommitteeSortition[IO], EligibilityChecker[IO], CanonicalOperatorConsensusPopulation)

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8).asResource
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38).asResource
      eligibility = EligibilityChecker.make[IO](log1p, exp)
      operators <- CanonicalOperatorConsensusFixture.makePopulation(8)
    } yield (h, CommitteeSortition.make[IO], eligibility, operators)

  private def eta(tag: String): Array[Byte] =
    MessageDigest.getInstance("SHA-256").digest(tag.getBytes(StandardCharsets.UTF_8))

  /** Synthesize a test metagraph address from a deterministic byte tag. */
  private def metagraphAddr(tag: String): Address =
    Address.fromBytes(tag.getBytes("UTF-8"))

  /** Synthesize a test parent hash from a deterministic byte tag. SHA-256s the tag so we get a real Hash. */
  private def parent(tag: String): Hash =
    Hash.fromBytes(tag.getBytes("UTF-8"))

  // ============ Determinism ============

  test("isInCommittee is deterministic for identical inputs") {
    case (_, sortition, _, operators) =>
      val sk = operators.operators.head.localVrfSecret
      val etaBytes = eta("determinism")
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-A")
      val sigma = Ratio(1, 8)
      val k = 100

      for {
        r1 <- sortition.isInCommittee(sk, etaBytes, addr, ph, sigma, k)
        r2 <- sortition.isInCommittee(sk, etaBytes, addr, ph, sigma, k)
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
    case (hasher, _, _, _) =>
      implicit val h: Hasher[IO] = hasher
      val etaBytes = eta("message-determinism")
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-A")
      for {
        m1 <- CommitteeSortition.message[IO](etaBytes, addr, ph)
        m2 <- CommitteeSortition.message[IO](etaBytes, addr, ph)
      } yield expect(m1.sameElements(m2)).and(expect(m1.length == 64)) // SHA-256 hex string → 64 UTF-8 bytes
  }

  // ============ Domain separation (committee vs leader VRF) ============

  test("committee VRF output differs from a leader-VRF style output for the same (sk, eta)") {
    case (_, sortition, eligibility, operators) =>
      // The leader VRF hashes `eta ‖ slot` (per EligibilityChecker.vrfProofForSlot). The committee
      // VRF hashes `Hasher.hash(CommitteeVrfInput("committee", eta, addr, parentHash))`. These are distinct
      // messages — the proofs (and thus outputs) must differ for the same SK.
      val sk = operators.operators.head.localVrfSecret
      val etaBytes = eta("domain-separation")
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-A")
      val leaderProof = eligibility.vrfProofForSlot(sk, Slot.unsafeApply(50L), etaBytes)

      for {
        committeeResult <- sortition.isInCommittee(sk, etaBytes, addr, ph, Ratio.One, kDraw = 1)
        committeeProof <- IO.fromOption(committeeResult.map(_._1))(new IllegalStateException("saturated registered draw was not selected"))
      } yield expect(!committeeProof.sameElements(leaderProof))
  }

  // ============ Per-metagraph isolation ============

  test("different metagraph addresses produce independent committee draws") {
    case (hasher, _, _, _) =>
      implicit val h: Hasher[IO] = hasher
      val etaBytes = eta("metagraph-isolation")
      val addrA = metagraphAddr("metagraph-A")
      val addrB = metagraphAddr("metagraph-B")
      val ph = parent("parent-A")

      for {
        mA <- CommitteeSortition.message[IO](etaBytes, addrA, ph)
        mB <- CommitteeSortition.message[IO](etaBytes, addrB, ph)
      } yield expect(!mA.sameElements(mB))
  }

  // ============ Per-parent rotation ============

  test("different parent hashes produce different VRF outputs") {
    case (_, sortition, _, operators) =>
      val sk = operators.operators.head.localVrfSecret
      val etaBytes = eta("parent-rotation")
      val addr = metagraphAddr("metagraph-A")
      val sigma = Ratio(1, 4)
      val k = 100

      for {
        r1 <- sortition.isInCommittee(sk, etaBytes, addr, parent("p1"), sigma, k)
        r2 <- sortition.isInCommittee(sk, etaBytes, addr, parent("p2"), sigma, k)
      } yield
        (r1, r2) match {
          case (Some((_, o1)), Some((_, o2))) => expect(!o1.sameElements(o2))
          case _ => success // not both selected — that's fine, this test only asserts output divergence when both fire
        }
  }

  // ============ Verify roundtrip ============

  test("isInCommittee + verifyMembership roundtrip succeeds for an in-committee key") {
    case (_, sortition, _, operators) =>
      val operator = operators.operators.head
      val etaBytes = eta("roundtrip")
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-roundtrip")
      val sigma = Ratio.One // forces selection; K·σ = K ≥ 1 saturates threshold to 1
      val k = 100

      val sk = operator.localVrfSecret
      val vk = operator.resolvedPair.vrfPublicKey.toBytes

      for {
        out <- sortition.isInCommittee(sk, etaBytes, addr, ph, sigma, k)
        verified <- out match {
          case Some((proof, _)) =>
            sortition.verifyMembership(vk, etaBytes, addr, ph, sigma, k, proof)
          case None => IO.pure(false)
        }
      } yield expect(out.isDefined).and(expect(verified, "verifyMembership must succeed for an in-committee key"))
  }

  // ============ Negative tests ============

  test("verifyMembership fails with wrong VRF VK") {
    case (_, sortition, _, operators) =>
      val producer = operators.operators.head
      val otherRegisteredOperator = operators.operators.tail.head
      val sk1 = producer.localVrfSecret
      val vk2 = otherRegisteredOperator.resolvedPair.vrfPublicKey.toBytes
      val etaBytes = eta("wrong-vk")
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-neg-vk")
      val sigma = Ratio.One
      val k = 100

      for {
        out <- sortition.isInCommittee(sk1, etaBytes, addr, ph, sigma, k)
        verified <- out match {
          case Some((proof, _)) => sortition.verifyMembership(vk2, etaBytes, addr, ph, sigma, k, proof)
          case None             => IO.pure(false)
        }
      } yield expect(out.isDefined).and(expect(!verified))
  }

  test("verifyMembership fails with tampered proof") {
    case (_, sortition, _, operators) =>
      val operator = operators.operators.head
      val sk = operator.localVrfSecret
      val vk = operator.resolvedPair.vrfPublicKey.toBytes
      val etaBytes = eta("tampered-proof")
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-neg-tamper")
      val sigma = Ratio.One
      val k = 100

      for {
        out <- sortition.isInCommittee(sk, etaBytes, addr, ph, sigma, k)
        verified <- out match {
          case Some((proof, _)) =>
            val tampered = proof.clone()
            tampered(0) = (tampered(0) ^ 0xff.toByte).toByte
            sortition.verifyMembership(vk, etaBytes, addr, ph, sigma, k, tampered)
          case None => IO.pure(false)
        }
      } yield expect(out.isDefined).and(expect(!verified))
  }

  test("verifyMembership fails with wrong metagraph address") {
    case (_, sortition, _, operators) =>
      val operator = operators.operators.head
      val sk = operator.localVrfSecret
      val vk = operator.resolvedPair.vrfPublicKey.toBytes
      val etaBytes = eta("wrong-metagraph")
      val addrA = metagraphAddr("metagraph-A")
      val addrB = metagraphAddr("metagraph-B")
      val ph = parent("parent-neg-mg")
      val sigma = Ratio.One
      val k = 100

      for {
        out <- sortition.isInCommittee(sk, etaBytes, addrA, ph, sigma, k)
        verified <- out match {
          case Some((proof, _)) => sortition.verifyMembership(vk, etaBytes, addrB, ph, sigma, k, proof)
          case None             => IO.pure(false)
        }
      } yield expect(out.isDefined).and(expect(!verified))
  }

  test("verifyMembership fails with wrong eta") {
    case (_, sortition, _, operators) =>
      val operator = operators.operators.head
      val sk = operator.localVrfSecret
      val vk = operator.resolvedPair.vrfPublicKey.toBytes
      val etaBytes = eta("correct-eta")
      val wrongEta = eta("wrong-eta")
      val addr = metagraphAddr("metagraph-A")
      val ph = parent("parent-neg-eta")
      val sigma = Ratio.One
      val k = 100

      for {
        out <- sortition.isInCommittee(sk, etaBytes, addr, ph, sigma, k)
        verified <- out match {
          case Some((proof, _)) => sortition.verifyMembership(vk, wrongEta, addr, ph, sigma, k, proof)
          case None             => IO.pure(false)
        }
      } yield expect(out.isDefined).and(expect(!verified))
  }

  test("verifyMembership fails with wrong parent hash") {
    case (_, sortition, _, operators) =>
      val operator = operators.operators.head
      val sk = operator.localVrfSecret
      val vk = operator.resolvedPair.vrfPublicKey.toBytes
      val etaBytes = eta("wrong-parent")
      val addr = metagraphAddr("metagraph-A")
      val phA = parent("parent-neg-A")
      val phB = parent("parent-neg-B")
      val sigma = Ratio.One
      val k = 100

      for {
        out <- sortition.isInCommittee(sk, etaBytes, addr, phA, sigma, k)
        verified <- out match {
          case Some((proof, _)) => sortition.verifyMembership(vk, etaBytes, addr, phB, sigma, k, proof)
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
    case (_, sortition, _, operators) =>
      val sk = operators.operators.head.localVrfSecret
      val etaBytes = eta("zero-threshold")
      val addr = metagraphAddr("metagraph-A")
      val k = 100

      // Check several parents; σ=0 must always exclude.
      for {
        results <- (1 to 20).toList.traverse { i =>
          sortition.isInCommittee(sk, etaBytes, addr, parent(s"sigma-zero-$i"), Ratio.Zero, k)
        }
      } yield expect(results.forall(_.isEmpty))
  }

  test("σ ≥ 1/K (saturated) → always in committee") {
    case (_, sortition, _, operators) =>
      val sk = operators.operators.head.localVrfSecret
      val etaBytes = eta("saturated-threshold")
      val addr = metagraphAddr("metagraph-A")
      val k = 5
      val sigma = Ratio(1, 4) // 5 · 1/4 = 5/4 > 1, saturates

      for {
        results <- (1 to 20).toList.traverse { i =>
          sortition.isInCommittee(sk, etaBytes, addr, parent(s"sigma-sat-$i"), sigma, k)
        }
      } yield expect(results.forall(_.isDefined))
  }

  test("kDraw=0 is rejected by threshold") { _ =>
    IO.delay {
      val caught =
        try { CommitteeSortition.threshold(0, Ratio.One); false }
        catch { case _: IllegalArgumentException => true }
      expect(caught)
    }
  }

  // ============ Draw/quorum decouple ============
  //
  // `threshold` is keyed on the DRAW target `kDraw` (decoupled from the admit quorum). These pin the exact-Ratio behaviour the
  // decouple relies on: the threshold uses `kDraw` (not some quorum), and at the 8-node testnet default it saturates so the
  // committee is everyone — guaranteeing P(|committee| ≥ kQuorum) = 1.

  test("threshold uses kDraw (exact Ratio): threshold(kDraw, σ) = kDraw·σ when below 1") { _ =>
    // Two different kDraw values at the same σ produce thresholds in the kDraw ratio — proving kDraw (not a quorum) drives it.
    val tDraw4 = CommitteeSortition.threshold(4, Ratio(1, 100)) // 4/100
    val tDraw6 = CommitteeSortition.threshold(6, Ratio(1, 100)) // 6/100
    IO.pure(
      // exact-Ratio, no Double — `Ratio(4, 100)` and `Ratio(6, 100)` reduce canonically (1/25 and 3/50).
      expect(tDraw4 == Ratio(4, 100))
        .and(expect(tDraw6 == Ratio(6, 100)))
        // kDraw scales the threshold: 6/100 = 3/50 differs from 4/100 = 1/25 — the larger kDraw gives the larger threshold.
        .and(expect(tDraw6 != tDraw4))
    )
  }

  test("testnet default kDraw=8 at σ=1/N (N=8) saturates threshold to Ratio.One (committee = everyone ⇒ ≥ kQuorum=6)") { _ =>
    // kDraw = N = 8, σ = 1/8 ⇒ kDraw·σ = 8·(1/8) = 1 ⇒ saturates to Ratio.One ⇒ every operator is in committee.
    // Expected committee size = N = 8 ≥ kQuorum = 6, so the gate's quorum is always reachable.
    val n = 8
    val kDraw = 8
    val kQuorum = 6
    val t = CommitteeSortition.threshold(kDraw, Ratio(1, n))
    IO.pure(
      expect(t == Ratio.One)
        // The decouple invariant the default encodes: expected committee (= kDraw at saturation) ≥ admit quorum.
        .and(expect(kDraw >= kQuorum))
    )
  }

  // ============ Statistical inclusion-rate sanity check ============

  test("aggregate inclusion over registered operators tracks kDraw per independent draw") {
    case (_, sortition, _, population) =>
      // Eight loader-validated period-zero identities participate in 125 independent committee
      // elections. With uniform sigma=1/8 and kDraw=1, each election has expected committee size
      // one, so the 1,000 registered proof evaluations have 125 expected selections.
      val drawCount = 125
      val k = 1
      val sigma = Ratio(1, population.operators.size)
      val addr = metagraphAddr("statistical-test")

      for {
        selected <- (1 to drawCount).toList.flatTraverse { draw =>
          population.operators.traverse { operator =>
            sortition.isInCommittee(
              operator.localVrfSecret,
              eta(s"statistical-eta-$draw"),
              addr,
              parent(s"statistical-parent-$draw"),
              sigma,
              k
            )
          }
        }
        committeeSize = selected.count(_.isDefined)
      } yield
        expect(
          committeeSize >= 80 && committeeSize <= 170,
          s"Expected approximately $drawCount aggregate selections, got $committeeSize"
        )
  }
}
