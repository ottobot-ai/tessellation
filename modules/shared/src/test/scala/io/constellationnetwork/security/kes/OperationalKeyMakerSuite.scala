package io.constellationnetwork.security.kes

import cats.effect.IO

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
}
