package io.constellationnetwork.security.kes

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Resource}
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._

/** Stateful interpreter that wraps `KesProduct` in a [[OperationalKeyMakerAlgebra]] with read-once persistence.
  *
  * Lifecycle:
  *
  *   1. At construction, the interpreter reads the single key file from `secureStore` (under `keyName`). It is an error for the store to be
  *      empty or contain more than one entry under `keyName` at startup.
  *   1. On each [[OperationalKeyMakerAlgebra.signAt]] or [[OperationalKeyMakerAlgebra.evolveTo]] call, the in-memory key is evolved to the
  *      target period. The pre-evolution bytes are destroyed by `ProductComposition.eraseOldNode` and
  *      `ProductComposition.eraseLeafSecretKey` during the evolve.
  *   1. After every evolution, the new key is encoded and re-written to `secureStore` (the old persisted bytes are overwritten by
  *      [[SecureStore.write]]'s internal scrub).
  *   1. All mutations are serialized by a `Semaphore` (one permit) so concurrent callers see a consistent step progression.
  *
  * '''Period alignment''': the `etaPeriodLength` parameter is carried in this interpreter as configuration only. It is intended as
  * documentation that, in the wired system, KES periods will align with eta rotation cadence (per [[project_consensus_epoch_staggering]]).
  * This algebra does not itself drive period advancement; the caller decides which period to sign at.
  *
  *   - `etaPeriodLength: Long` — number of slots per eta period, as configured by the consensus layer.
  *   - `keyName: String` — the file/entry name in the [[SecureStore]]. A single-key invariant is enforced.
  */
object OperationalKeyMaker {

  /** Default tree heights `(superHeight, subHeight)` for `bootstrap`. `(7, 7)` gives `2^14 = 16384` total steps — comfortably more than any
    * realistic eta-period count over the lifetime of a single operator key. Smaller is faster to generate but harder to right-size; larger
    * costs more boot-time CPU + memory.
    */
  val DefaultHeight: (Int, Int) = (7, 7)

  /** Construct an [[OperationalKeyMakerAlgebra]] backed by `secureStore`. The store must contain exactly one entry under `keyName` at
    * startup.
    *
    * The underlying KES product scheme is fixed to `KesProduct.instance` (package-private). Callers outside the package cannot inject an
    * alternate scheme; the interface boundary is the F[_]-typed algebra.
    */
  def make[F[_]: Async](
    secureStore: SecureStore[F],
    keyName: String,
    etaPeriodLength: Long
  ): Resource[F, OperationalKeyMakerAlgebra[F]] = {
    val kesProduct: KesProduct = KesProduct.instance
    val acquire: F[OperationalKeyMakerAlgebra[F]] =
      for {
        _ <- Async[F].pure(etaPeriodLength) // documented config knob, kept on the interpreter for callers to inspect
        bytes <- secureStore.consume(keyName).flatMap {
          case Some(b) => Async[F].pure(b)
          case None =>
            Async[F].raiseError[Array[Byte]](
              new IllegalStateException(s"OperationalKeyMaker.make: SecureStore has no entry '$keyName'")
            )
        }
        loaded <- SecretKeyCodec.decodeProductSk(bytes) match {
          case Right(sk) => Async[F].pure(sk)
          case Left(err) => Async[F].raiseError[SecretKeyKesProduct](new IllegalStateException(err.message))
        }
        state <- Ref.of[F, SecretKeyKesProduct](loaded)
        lock <- Semaphore[F](1L)
        // Persist immediately so we recover after a crash even before the first signAt.
        _ <- secureStore.write(keyName, SecretKeyCodec.encodeProductSk(loaded))
      } yield new Impl[F](secureStore, keyName, etaPeriodLength, kesProduct, state, lock)

    Resource.eval(acquire)
  }

  private final class Impl[F[_]: Async](
    secureStore: SecureStore[F],
    keyName: String,
    val etaPeriodLength: Long,
    kesProduct: KesProduct,
    state: Ref[F, SecretKeyKesProduct],
    lock: Semaphore[F]
  ) extends OperationalKeyMakerAlgebra[F] {

    override def currentPublicKey: F[VerificationKeyKesProduct] =
      state.get.map(kesProduct.getVerificationKey)

    override def currentPeriod: F[Int] =
      state.get.map(kesProduct.getCurrentStep)

    override def signAt(period: Int, message: Array[Byte]): F[Either[KesError, SignatureKesProduct]] =
      lock.permit.use { _ =>
        state.get.flatMap { sk =>
          evolveAndPersistUnsafe(sk, period).flatMap {
            case Right(evolved) =>
              val sig = kesProduct.sign(evolved, message)
              Async[F].pure(sig.asRight[KesError])
            case Left(err) => Async[F].pure(err.asLeft[SignatureKesProduct])
          }
        }
      }

    override def evolveTo(period: Int): F[Either[KesError, Unit]] =
      lock.permit.use { _ =>
        state.get.flatMap { sk =>
          evolveAndPersistUnsafe(sk, period).map(_.map(_ => ()))
        }
      }

    /** Evolve and persist. Must be called under [[lock]]; updates [[state]] on success. */
    private def evolveAndPersistUnsafe(
      sk: SecretKeyKesProduct,
      period: Int
    ): F[Either[KesError, SecretKeyKesProduct]] = {
      val currentStep = kesProduct.getCurrentStep(sk)
      if (period == currentStep) {
        Async[F].pure(sk.asRight[KesError])
      } else if (period < currentStep) {
        Async[F].pure(KesError.StepNotMonotonic(currentStep, period).asLeft[SecretKeyKesProduct])
      } else {
        kesProduct.update(sk, period) match {
          case Left(err) => Async[F].pure(err.asLeft[SecretKeyKesProduct])
          case Right(evolved) =>
            for {
              _ <- secureStore.write(keyName, SecretKeyCodec.encodeProductSk(evolved))
              _ <- state.set(evolved)
            } yield evolved.asRight[KesError]
        }
      }
    }
  }

  /** Generate a fresh KES product keypair from `seed`, persist the secret-key bytes to `secureStore` under `keyName`, and open an
    * [[OperationalKeyMakerAlgebra]] over it. Equivalent to: caller manually invokes `KesProduct.createKeyPair` (package-private) + writes
    * the encoded secret-key bytes to `secureStore` + calls [[make]]. Provided as a single entry point because `KesProduct` and
    * `SecretKeyCodec` are both package-private, so external callers can't otherwise reach the generator.
    *
    *   - `seed` — entropy for the KES tree. Must not be reused across operators. Caller owns the bytes; they are overwritten on success
    *     (defensive scrub before returning). For deterministic per-test setups, derive from the operator's long-term key (e.g.
    *     `blake2b(ed25519_sk || "kes")`); for production, use [[java.security.SecureRandom.nextBytes]].
    *   - `height` — `(superHeight, subHeight)` controlling expressible periods. Default [[DefaultHeight]] = `(7, 7)` → 16384 periods.
    *   - `keyName` — entry name in the [[SecureStore]]. Same convention as [[make]] (one entry per operator).
    *   - `etaPeriodLength` — carried-only metadata; see [[make]] for the alignment story.
    */
  def bootstrap[F[_]: Async](
    secureStore: SecureStore[F],
    keyName: String,
    seed: Array[Byte],
    etaPeriodLength: Long,
    height: (Int, Int) = DefaultHeight
  ): Resource[F, OperationalKeyMakerAlgebra[F]] = {
    val seedCopy = seed.clone()
    val acquire: F[Unit] =
      Async[F].delay {
        val (sk, _) = KesProduct.instance.createKeyPair(seedCopy, height, offset = 0L)
        SecretKeyCodec.encodeProductSk(sk)
      }
        .flatMap(bytes => secureStore.write(keyName, bytes))
        .flatMap(_ => Async[F].delay(java.util.Arrays.fill(seedCopy, 0.toByte)))

    Resource.eval(acquire) >> make(secureStore, keyName, etaPeriodLength)
  }

  /** Generate a fresh KES product keypair from `seed` at offset `0`, and return the encoded secret-key bytes together with the master
    * verification key at step 0. Genesis-generator helper: the Tier-1 generator embeds the master VK in the L0 genesis fixture as part of
    * the per-operator KES registration record, and writes the encoded SK bytes to a per-operator `kes-sk.bin` file that the gl0 container
    * will later mount and load via a disk-backed [[SecureStore]] (Slice 3d / Slice 4).
    *
    * The returned SK bytes are the same format `OperationalKeyMaker.make` consumes from a [[SecureStore]] — i.e. the output of
    * [[SecretKeyCodec.encodeProductSk]]. The caller owns the returned bytes; they must be securely written to disk + scrubbed from memory
    * after use. Same `seed.clone()` defensive copy + scrub as [[bootstrap]] for symmetry.
    *
    *   - `seed` — entropy for the KES tree. Must not be reused across operators.
    *   - `height` — `(superHeight, subHeight)` tree shape. Default [[DefaultHeight]] = `(7, 7)` → 16384 periods.
    */
  def generateFreshKesKeyMaterial[F[_]: Async](
    seed: Array[Byte],
    height: (Int, Int) = DefaultHeight
  ): F[(Array[Byte], VerificationKeyKesProduct)] = {
    val seedCopy = seed.clone()
    Async[F].delay {
      val (sk, vk) = KesProduct.instance.createKeyPair(seedCopy, height, offset = 0L)
      val encoded = SecretKeyCodec.encodeProductSk(sk)
      java.util.Arrays.fill(seedCopy, 0.toByte)
      (encoded, vk)
    }
  }

  /** Pure verify helper exposed for callers outside the `kes` package. `KesProduct` itself is package-private so the underlying
    * `KesProduct.instance.verify` would be unreachable from `dag-l0` (Slice 5 receiver path). This forwards to that method without widening
    * the package surface. The function is pure (no F[_]) because verify performs no I/O — it's a Merkle-path reconstruction + Ed25519
    * verify, all CPU-only.
    */
  def verify(
    signature: SignatureKesProduct,
    message: Array[Byte],
    verifyKey: VerificationKeyKesProduct
  ): Boolean =
    KesProduct.instance.verify(signature, message, verifyKey)

  /** Encode a KES product signature to its on-the-wire byte representation. Forwarder for [[SignatureCodec.encodeSignature]], exposed
    * because [[SignatureCodec]] is package-private (the secret-key bits should stay scoped). Slice 5/6 senders call this to fill the
    * `kes_signature` field on `pb.TipAttestation` / `pb.Snapshot`.
    */
  def encodeSignature(signature: SignatureKesProduct): Array[Byte] =
    SignatureCodec.encodeSignature(signature)

  /** Decode a KES product signature from its on-the-wire byte representation. Forwarder for [[SignatureCodec.decodeSignature]]; see
    * [[encodeSignature]] for the rationale. Slice 5/6 receivers call this on the `kes_signature` field bytes; an empty input yields
    * `Left(KesError.MalformedTree(...))` rather than a special "no-signature" sentinel — callers handle the empty case upstream by
    * short-circuiting on `bytes.isEmpty` before invoking decode.
    */
  def decodeSignature(bytes: Array[Byte]): Either[KesError, SignatureKesProduct] =
    SignatureCodec.decodeSignature(bytes)
}
