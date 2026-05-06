package io.constellationnetwork.numerics

import cats.effect.IO

import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}

import weaver.SimpleIOSuite

object RatioNumericsSuite extends SimpleIOSuite {

  // Bifrost production config: log1p prec=8, exp prec=38, maxIter=10_000.
  // We use slightly tighter precision in tests so any drift shows up here, not in production.
  private val MaxIter = 10000
  private val Log1pPrec = 16
  private val ExpPrec = 38

  // Convenience for pulling the IO[Ratio] into a Ratio inline.
  private def log1pSync(x: Ratio): IO[Ratio] = Log1pInterpreter.make[IO](MaxIter, Log1pPrec).flatMap(_.evaluate(x))
  private def expSync(x: Ratio): IO[Ratio] = ExpInterpreter.make[IO](MaxIter, ExpPrec).flatMap(_.evaluate(x))

  test("Ratio — gcd-reduces at construction (canonical form)") {
    IO {
      expect(Ratio(6, 9) == Ratio(2, 3))
        .and(expect(Ratio(6, 9).numerator == BigInt(2)))
        .and(expect(Ratio(6, 9).denominator == BigInt(3)))
        .and(expect(Ratio(0, 5) == Ratio.Zero))
    }
  }

  test("Ratio — arithmetic operators are exact") {
    IO {
      val a = Ratio(1, 3)
      val b = Ratio(1, 6)
      // Double would compute 1/3 + 1/6 = 0.5000000000000001 occasionally; Ratio is exact.
      expect((a + b) == Ratio(1, 2))
        .and(expect((a - b) == Ratio(1, 6)))
        .and(expect((a * b) == Ratio(1, 18)))
        .and(expect((a / b) == Ratio(2, 1)))
    }
  }

  test("Ratio — comparison cross-multiplies, no Double loss") {
    IO {
      val small = Ratio(1, BigInt(10).pow(20))
      val tinier = Ratio(1, BigInt(10).pow(40))
      expect(small > tinier)
        .and(expect(tinier < small))
        .and(expect(small > Ratio.Zero))
    }
  }

  test("Ratio — Double construction stable to declared precision") {
    IO {
      val r = Ratio(0.05, 18)
      // 0.05 cannot be exactly represented in IEEE 754, but at prec=18 we lock in the truncated decimal
      // representation. Reproducible across all JVMs/CPUs.
      expect(r.denominator == BigInt(10).pow(18))
        .and(expect((r - Ratio(5, 100)).abs < Ratio(1, BigInt(10).pow(15))))
    }
  }

  // ------------------------------------------------------------------------------------------ Lentz numerics

  test("Log1p — log1p(0) = 0") {
    log1pSync(Ratio.Zero).map(r => expect(r == Ratio.Zero))
  }

  test("Exp — exp(0) = 1") {
    expSync(Ratio.Zero).map(r => expect(r == Ratio.One))
  }

  test("Lentz — exp(log1p(-x)) ≈ 1 - x on the LDD parameter regime") {
    // x ∈ {0.001, 0.01, 0.05 (baseline), 0.10, 0.22 (≈ amplitude × 6/15), 0.40}
    val xs = List(
      Ratio(1, 1000),
      Ratio(1, 100),
      Ratio(1, 20),
      Ratio(1, 10),
      Ratio(22, 100),
      Ratio(2, 5)
    )
    val tolerance = Ratio(1, BigInt(10).pow(7)) // 10^-7 — well within Bifrost's prod precision

    xs.traverse_test(x =>
      for {
        coefficient <- log1pSync(-x)
        result <- expSync(coefficient)
        expected = Ratio.One - x
        diff = (result - expected).abs
      } yield
        expect(
          diff < tolerance,
          s"exp(log1p(-$x)) = $result, expected ${expected}, diff $diff exceeds $tolerance"
        )
    )
  }

  test("Lentz — Taktikos threshold (1 - (1-f)^stake) on equal-weight 8-node config") {
    // 8 validators, equal stake = 1/8.
    val stake = Ratio(1, 8)
    // slotGap = 7 (the median observed in the 8-node soak), LDD: amplitude=1/2, offset=1, cutoff=16.
    val gap = 7
    val offset = 1
    val cutoff = 16
    val amplitude = Ratio(1, 2)
    val difficulty = Ratio(BigInt(gap - offset), BigInt(cutoff - offset)) * amplitude

    val tolerance = Ratio(1, BigInt(10).pow(6))
    for {
      coefficient <- log1pSync(-difficulty)
      result <- expSync(coefficient * stake)
      threshold = Ratio.One - result
      // Sanity: same value computed via Double to within tolerance.
      doubleThresh = 1.0 - math.pow(1.0 - difficulty.toDouble, stake.toDouble)
      diff = (threshold - Ratio(doubleThresh, 12)).abs
    } yield
      expect(
        threshold > Ratio.Zero,
        s"threshold should be positive, got $threshold"
      ).and(expect(threshold < Ratio.One, s"threshold should be < 1, got $threshold"))
        .and(
          expect(
            diff < tolerance,
            s"Ratio threshold $threshold disagrees with Double approximation $doubleThresh by $diff"
          )
        )
  }

  test("Lentz — pure-determinism: identical inputs ⇒ byte-identical Ratio output") {
    // Run twice; numerator and denominator must match exactly (no IEEE 754 last-ulp drift).
    val x = Ratio(7, 100)
    val stake = Ratio(1, 8)
    for {
      r1 <- log1pSync(-x).flatMap(c => expSync(c * stake)).map(Ratio.One - _)
      r2 <- log1pSync(-x).flatMap(c => expSync(c * stake)).map(Ratio.One - _)
    } yield expect(r1.numerator == r2.numerator).and(expect(r1.denominator == r2.denominator))
  }

  // Tiny extension: traverse Ratio inputs returning a single combined Expectation result.
  private implicit class TraverseExpectations[A](xs: List[A]) {
    def traverse_test(f: A => IO[weaver.Expectations]): IO[weaver.Expectations] =
      xs.foldLeft(IO.pure(weaver.Expectations.Helpers.success)) { (accIO, a) =>
        for {
          acc <- accIO
          e <- f(a)
        } yield acc.and(e)
      }
  }
}
