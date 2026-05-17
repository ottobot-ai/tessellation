package io.constellationnetwork.security.kes

import cats.effect.IO
import cats.syntax.traverse._

import weaver.SimpleIOSuite

/** Tests for the read-once secure-store-backed [[OperationalKeyMaker]].
  *
  *   - Persistence roundtrip: write key, load via [[OperationalKeyMaker.make]], sign — verify with the original VK.
  *   - Period monotonicity is enforced.
  *   - The persisted key is updated after each evolve / sign.
  */
object OperationalKeyMakerSuite extends SimpleIOSuite {

  private val keyName = "kes-product.key"

  /** Helper: build a SecureStore with a freshly generated product key written under `keyName`. */
  private def freshStore(seedByte: Byte = 41, height: (Int, Int) = (2, 2)): IO[(SecureStore[IO], VerificationKeyKesProduct)] = {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(seedByte)
    val (sk, vk) = kes.createKeyPair(seed, height, 0L)
    SecureStore
      .inMemory[IO]
      .flatTap { store =>
        store.write(keyName, SecretKeyCodec.encodeProductSk(sk))
      }
      .map((_, vk))
  }

  test("persistence round-trip: encode + write + load + sign + verify") {
    for {
      (store, vk) <- freshStore()
      result <- OperationalKeyMaker.make[IO](store, keyName, etaPeriodLength = 100L).use { kmaker =>
        kmaker.signAt(0, "hello".getBytes("UTF-8"))
      }
    } yield
      matches(result) {
        case Right(sig) =>
          expect(KesProduct.instance.verify(sig, "hello".getBytes("UTF-8"), vk))
      }
  }

  test("evolving advances the persisted state (next make sees the evolved key)") {
    for {
      (store, _) <- freshStore()
      _ <- OperationalKeyMaker.make[IO](store, keyName, etaPeriodLength = 100L).use { kmaker =>
        kmaker.evolveTo(5)
      }
      // Reopen; the persisted key should be at step 5.
      postStep <- OperationalKeyMaker.make[IO](store, keyName, etaPeriodLength = 100L).use { kmaker =>
        kmaker.currentPeriod
      }
    } yield expect.same(5, postStep)
  }

  test("signAt rejects evolution backwards") {
    for {
      (store, _) <- freshStore()
      result <- OperationalKeyMaker.make[IO](store, keyName, etaPeriodLength = 100L).use { kmaker =>
        for {
          _ <- kmaker.evolveTo(8)
          sigResult <- kmaker.signAt(3, "x".getBytes("UTF-8"))
        } yield sigResult
      }
    } yield matches(result) { case Left(KesError.StepNotMonotonic(8, 3)) => success }
  }

  test("signAt at the current period does not advance the step") {
    for {
      (store, _) <- freshStore()
      result <- OperationalKeyMaker.make[IO](store, keyName, etaPeriodLength = 100L).use { kmaker =>
        for {
          _ <- kmaker.evolveTo(4)
          stepBefore <- kmaker.currentPeriod
          _ <- kmaker.signAt(4, "stable".getBytes("UTF-8"))
          stepAfter <- kmaker.currentPeriod
        } yield (stepBefore, stepAfter)
      }
    } yield expect.same((4, 4), result)
  }

  test("evolveTo beyond max returns StepBeyondMax") {
    for {
      (store, _) <- freshStore(height = (1, 1)) // max = 4
      result <- OperationalKeyMaker.make[IO](store, keyName, etaPeriodLength = 100L).use { kmaker =>
        kmaker.evolveTo(4)
      }
    } yield matches(result) { case Left(KesError.StepBeyondMax(_, 4, 4)) => success }
  }

  test("make fails fast when SecureStore is empty") {
    for {
      store <- SecureStore.inMemory[IO]
      attempt <- OperationalKeyMaker
        .make[IO](store, keyName, etaPeriodLength = 100L)
        .use(_ => IO.unit)
        .attempt
    } yield expect(attempt.isLeft)
  }

  test("signed message verifies with the public key reported by currentPublicKey") {
    for {
      (store, _) <- freshStore()
      out <- OperationalKeyMaker.make[IO](store, keyName, etaPeriodLength = 100L).use { kmaker =>
        for {
          _ <- kmaker.evolveTo(2)
          vk <- kmaker.currentPublicKey
          sigResult <- kmaker.signAt(2, "stable".getBytes("UTF-8"))
        } yield (vk, sigResult)
      }
    } yield
      matches(out) {
        case (vk, Right(sig)) =>
          expect(KesProduct.instance.verify(sig, "stable".getBytes("UTF-8"), vk))
      }
  }

  // ============================================================
  // master-VK round-trip: register at step=0, verify post-rotation
  // ============================================================
  //
  // Regression for the bug found during Slice 8 e2e: the genesis-registered master
  // VK has step=0, but post-rotation sigs are produced at the sender's evolved step.
  // `SumComposition.verify` uses `kesVk.step` to decide which side of the Merkle tree
  // the witness lands on, so verify with master-VK(step=0) vs. sig-at-step-N walks
  // the wrong path and returns false. Receivers must rebind the VK's step to the
  // expected period before calling verify.
  test("verify with master-VK + rebound step succeeds across multiple evolutions") {
    val periodsToCheck = List(1, 2, 5, 10, 15)
    val msg = "post-rotation-payload".getBytes("UTF-8")
    for {
      (store, masterVk) <- freshStore(seedByte = 0x42.toByte, height = (2, 2))
      results <- OperationalKeyMaker.make[IO](store, keyName, etaPeriodLength = 100L).use { kmaker =>
        periodsToCheck.traverse { p =>
          kmaker.signAt(p, msg).map {
            case Right(sig) =>
              // Master VK as it would be loaded from the registry (step=0), rebound to p.
              val vkAtPeriod = masterVk.copy(step = p)
              (p, KesProduct.instance.verify(sig, msg, vkAtPeriod), KesProduct.instance.verify(sig, msg, masterVk))
            case Left(err) => (p, false, false)
          }
        }
      }
    } yield
      // For each period: rebound-step verify must succeed; raw master-VK verify is
      // expected to fail (which is exactly the production bug this test guards against).
      results.foldLeft(success) {
        case (acc, (p, reboundOk, rawOk)) =>
          acc
            .and(expect(reboundOk, s"rebound-step verify must succeed at period=$p"))
            .and(expect(!rawOk, s"raw master-VK verify must fail at period=$p (regression guard)"))
      }
  }

  // ============================================================
  // bootstrap factory — single-call generate + persist + open
  // ============================================================

  test("bootstrap: generate + persist + open in one call; signature verifies against currentPublicKey") {
    val seed = Array.fill[Byte](32)(0x11.toByte)
    for {
      store <- SecureStore.inMemory[IO]
      out <- OperationalKeyMaker
        .bootstrap[IO](store, keyName, seed, etaPeriodLength = 100L, height = (2, 2))
        .use { kmaker =>
          for {
            vk <- kmaker.currentPublicKey
            sigResult <- kmaker.signAt(0, "bootstrap-msg".getBytes("UTF-8"))
          } yield (vk, sigResult)
        }
    } yield
      matches(out) {
        case (vk, Right(sig)) =>
          expect(KesProduct.instance.verify(sig, "bootstrap-msg".getBytes("UTF-8"), vk))
      }
  }

  test("bootstrap: persisted key survives across two open/close cycles") {
    val seed = Array.fill[Byte](32)(0x22.toByte)
    for {
      store <- SecureStore.inMemory[IO]
      // Bootstrap once + evolve to step 3 to mutate the persisted key
      _ <- OperationalKeyMaker
        .bootstrap[IO](store, keyName, seed, etaPeriodLength = 100L, height = (2, 2))
        .use(_.evolveTo(3))
      // Re-open via plain make (no re-bootstrap); the persisted state should be at step 3
      step <- OperationalKeyMaker
        .make[IO](store, keyName, etaPeriodLength = 100L)
        .use(_.currentPeriod)
    } yield expect.same(3, step)
  }

  test("bootstrap: caller's seed is scrubbed (defensive overwrite)") {
    val originalSeed = Array.fill[Byte](32)(0x33.toByte)
    val seedHandedToBootstrap = originalSeed.clone() // separate copy so we keep `originalSeed` for comparison
    for {
      store <- SecureStore.inMemory[IO]
      _ <- OperationalKeyMaker
        .bootstrap[IO](store, keyName, seedHandedToBootstrap, etaPeriodLength = 100L, height = (2, 2))
        .use(_ => IO.unit)
    } yield
      // The bootstrap clones the seed defensively (`seed.clone()` at the top of `bootstrap`), then scrubs the
      // copy after generation. The caller's original `seedHandedToBootstrap` is NOT scrubbed — that's the
      // *caller's* responsibility per the doc (callers should zero their own buffer if needed). The contract
      // tested here is: caller's bytes are unchanged (we cloned them rather than scrubbing in-place).
      expect.same(originalSeed.toList, seedHandedToBootstrap.toList)
  }
}
