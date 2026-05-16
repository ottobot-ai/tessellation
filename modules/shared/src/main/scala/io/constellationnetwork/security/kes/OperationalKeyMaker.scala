package io.constellationnetwork.security.kes

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Resource}
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._

/** Stateful interpreter that wraps [[KesProduct]] in a [[OperationalKeyMakerAlgebra]] with read-once persistence.
  *
  * Lifecycle:
  *
  *   1. At construction, the interpreter reads the single key file from `secureStore` (under `keyName`). It is an error for the store to be
  *      empty or contain more than one entry under `keyName` at startup.
  *   1. On each [[OperationalKeyMakerAlgebra.signAt]] or [[OperationalKeyMakerAlgebra.evolveTo]] call, the in-memory key is evolved to the
  *      target period. The pre-evolution bytes are destroyed by [[ProductComposition.eraseOldNode]] and
  *      [[ProductComposition.eraseLeafSecretKey]] during the evolve.
  *   1. After every evolution, the new key is encoded and re-written to `secureStore` (the old persisted bytes are overwritten by
  *      [[SecureStore.write]]'s internal scrub).
  *   1. All mutations are serialized by a [[Semaphore]] (one permit) so concurrent callers see a consistent step progression.
  *
  * '''Period alignment''': the `etaPeriodLength` parameter is carried in this interpreter as configuration only. It is intended as
  * documentation that, in the wired system, KES periods will align with eta rotation cadence (per [[project_consensus_epoch_staggering]]).
  * This algebra does not itself drive period advancement; the caller decides which period to sign at.
  *
  *   - `etaPeriodLength: Long` — number of slots per eta period, as configured by the consensus layer.
  *   - `keyName: String` — the file/entry name in the [[SecureStore]]. A single-key invariant is enforced.
  */
object OperationalKeyMaker {

  /** Construct an [[OperationalKeyMakerAlgebra]] backed by `secureStore`. The store must contain exactly one entry under `keyName` at
    * startup.
    *
    * The underlying KES product scheme is fixed to [[KesProduct.instance]] (package-private). Callers outside the package cannot inject an
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
}
