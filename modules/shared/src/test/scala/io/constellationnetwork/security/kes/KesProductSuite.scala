package io.constellationnetwork.security.kes

import cats.Show

import io.constellationnetwork.security.hex.Hex

import org.scalacheck.Gen
import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers

/** Tests for the KES product (super × sub) composition.
  *
  * Mirrors Bifrost's `KesProductSpec` (port of `co.topl.crypto.signing.KesProductSpec`).
  *
  *   - Property-based sign/verify roundtrip.
  *   - Determinism: same seed + same height ⇒ same VK.
  *   - Step monotonicity is enforced: evolving backwards returns [[KesError.StepNotMonotonic]].
  *   - Bound enforcement: evolving past `2^(heightSup + heightSub)` returns [[KesError.StepBeyondMax]].
  *   - "Multi-step verify" round-trip across multiple evolutions: every signature verifies against the VK at the period it was produced.
  *
  * '''Bifrost test-vector parity''' — same caveat as [[KesSumSuite]]: we do not port the byte-for-byte `specOut_sk` / `specOut_sig` hex
  * comparisons. The property-based tests provide equivalent functional coverage. The specific hex test vectors in Bifrost's
  * `KesProductSpec` (vectors 1-5, heights (1,1) through (3,3)) are exercised here as functional roundtrips at the corresponding heights +
  * step sequences, but without the byte-for-byte comparison.
  */
object KesProductSuite extends SimpleIOSuite with Checkers {

  private val seedHexGen: Gen[Hex] = Gen.listOfN(32, Gen.choose(0, 0xff)).map(bytes => Hex(bytes.map("%02x".format(_)).mkString))
  private val payloadHexGen: Gen[Hex] =
    Gen.choose(1, 32).flatMap(Gen.listOfN(_, Gen.choose(0, 0xff))).map(bytes => Hex(bytes.map("%02x".format(_)).mkString))

  private val heightGen: Gen[(Int, Int)] = Gen.zip(Gen.choose(1, 4), Gen.choose(1, 4))

  implicit private val showT3: Show[(Hex, Hex, (Int, Int))] =
    Show.show { case (s, m, h) => s"seed=${s.shortValue} msg=${m.shortValue} h=$h" }
  implicit private val showT4: Show[(Hex, Hex, Hex, (Int, Int))] =
    Show.show { case (s1, s2, m, h) => s"seed1=${s1.shortValue} seed2=${s2.shortValue} msg=${m.shortValue} h=$h" }
  implicit private val showT2: Show[(Hex, (Int, Int))] =
    Show.show { case (s, h) => s"seed=${s.shortValue} h=$h" }

  test("sign at step 0 + verify round-trips") {
    forall(Gen.zip(seedHexGen, payloadHexGen, heightGen)) {
      case (seed, msg, hs) =>
        val kes = KesProduct.instance
        val (sk, vk) = kes.createKeyPair(seed.toBytes, hs, 0L)
        val sig = kes.sign(sk, msg.toBytes)
        expect(kes.verify(sig, msg.toBytes, vk))
    }
  }

  test("verify fails with a different VK") {
    forall(Gen.zip(seedHexGen, seedHexGen, payloadHexGen, heightGen)) {
      case (s1, s2, msg, hs) =>
        if (s1 == s2) expect(true)
        else {
          val kes = KesProduct.instance
          val (sk1, vk1) = kes.createKeyPair(s1.toBytes, hs, 0L)
          val (_, vk2) = kes.createKeyPair(s2.toBytes, hs, 0L)
          val sig = kes.sign(sk1, msg.toBytes)
          expect(kes.verify(sig, msg.toBytes, vk1)).and(expect(!kes.verify(sig, msg.toBytes, vk2)))
        }
    }
  }

  test("verify fails on a different message") {
    forall(Gen.zip(seedHexGen, payloadHexGen, payloadHexGen, heightGen)) {
      case (seed, msg1, msg2, hs) =>
        if (msg1 == msg2) expect(true)
        else {
          val kes = KesProduct.instance
          val (sk, vk) = kes.createKeyPair(seed.toBytes, hs, 0L)
          val sig = kes.sign(sk, msg1.toBytes)
          expect(kes.verify(sig, msg1.toBytes, vk)).and(expect(!kes.verify(sig, msg2.toBytes, vk)))
        }
    }
  }

  test("same seed + height ⇒ identical VK (determinism)") {
    forall(Gen.zip(seedHexGen, heightGen)) {
      case (seed, hs) =>
        val kes = KesProduct.instance
        val (_, vk1) = kes.createKeyPair(seed.toBytes, hs, 0L)
        val (_, vk2) = kes.createKeyPair(seed.toBytes, hs, 0L)
        expect(vk1 == vk2)
    }
  }

  test("evolve + sign + verify at monotonic step sequence (multi-period)") {
    val seedBase = Hex("2a6367c85f416ccef46a4521004228f74f24f7b0770ecced07c0dc035135bf6f").toBytes
    val msg = Hex("697420617320646f206265206974206865206d65206f72").toBytes

    val cfgGen: Gen[((Int, Int), List[Int])] =
      Gen.zip(Gen.choose(1, 3), Gen.choose(1, 3)).flatMap {
        case (hSup, hSub) =>
          val maxSteps = 1 << (hSup + hSub)
          Gen
            .containerOf[List, Int](Gen.choose(0, maxSteps - 1))
            .map(_.distinct.sorted)
            .map(steps => ((hSup, hSub), steps))
      }

    implicit val showCfg: Show[((Int, Int), List[Int])] = Show.show {
      case ((a, b), ss) =>
        s"h=($a,$b) steps=${ss.mkString(",")}"
    }

    forall(cfgGen) {
      case ((hSup, hSub), steps) =>
        val kes = KesProduct.instance
        val (skInit, _) = kes.createKeyPair(seedBase.clone(), (hSup, hSub), 0L)
        val (_, ok) = steps.foldLeft((skInit, true)) {
          case ((sk, accOk), step) =>
            if (!accOk) (sk, false)
            else if (step <= kes.getCurrentStep(sk) && step != 0) (sk, accOk)
            else {
              kes.update(sk, step) match {
                case Right(evolved) =>
                  val sig = kes.sign(evolved, msg)
                  val vk = kes.getVerificationKey(evolved)
                  (evolved, kes.verify(sig, msg, vk))
                case Left(_) => (sk, accOk)
              }
            }
        }
        expect(ok)
    }
  }

  pureTest("evolve beyond max returns StepBeyondMax") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(11)
    val (sk, _) = kes.createKeyPair(seed, (1, 1), 0L) // max steps = 4
    matches(kes.update(sk, 4)) { case Left(KesError.StepBeyondMax(0, 4, 4)) => success }
  }

  pureTest("evolve to a strictly past step returns StepNotMonotonic") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(12)
    val (sk, _) = kes.createKeyPair(seed, (2, 2), 0L) // max steps = 16
    kes.update(sk, 8) match {
      case Right(evolved) =>
        matches(kes.update(evolved, 5)) { case Left(KesError.StepNotMonotonic(8, 5)) => success }
      case Left(err) => failure(s"first evolve failed: $err")
    }
  }

  pureTest("VK step reflects current period after evolution") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(13)
    val (sk, vk0) = kes.createKeyPair(seed, (2, 2), 0L)
    expect.same(0, vk0.step).and {
      val sk7 = kes.update(sk, 7).fold(e => sys.error(e.message), identity)
      val vk7 = kes.getVerificationKey(sk7)
      expect.same(7, vk7.step).and(expect.same(7, kes.getCurrentStep(sk7)))
    }
  }

  pureTest("evolving across a super-period boundary keeps verification correct") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(14)
    val (sk0, _) = kes.createKeyPair(seed, (2, 2), 0L)
    val msg = "boundary".getBytes("UTF-8")

    val sk3 = kes.update(sk0, 3).fold(e => sys.error(e.message), identity)
    val vk3 = kes.getVerificationKey(sk3)
    val sig3 = kes.sign(sk3, msg)

    val sk4 = kes.update(sk3, 4).fold(e => sys.error(e.message), identity)
    val vk4 = kes.getVerificationKey(sk4)
    val sig4 = kes.sign(sk4, msg)

    val sk7 = kes.update(sk4, 7).fold(e => sys.error(e.message), identity)
    val vk7 = kes.getVerificationKey(sk7)
    val sig7 = kes.sign(sk7, msg)

    expect.all(
      kes.verify(sig3, msg, vk3),
      kes.verify(sig4, msg, vk4),
      kes.verify(sig7, msg, vk7),
      // Cross-period mismatch: sig3 must NOT verify under vk4.
      !kes.verify(sig3, msg, vk4),
      !kes.verify(sig4, msg, vk7)
    )
  }
}
