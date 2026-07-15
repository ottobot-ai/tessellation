package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.{Concurrent, Deferred}
import cats.syntax.all._

/** One-shot process-lifecycle gate for network input that can reach GL0 consensus state.
  *
  * Main owns the [[ConsensusInputGate.Control]] and releases it only after rollback, restart, or genesis reconstruction has completed.
  * Consensus services receive only the await-only [[ConsensusInputGate]] capability, so network-facing code cannot declare local state
  * ready or bypass bootstrap ordering.
  *
  * This is not a finality rule and does not change execution authority. Native GL1/DAG transitions remain universally executed by GL0;
  * sharded CL1 transitions retain their replay-certificate and global settlement-kernel path. Releasing the gate proves lifecycle ordering,
  * not that a disk-restored chain head has authenticated ancestry or replay provenance.
  */
trait ConsensusInputGate[F[_]] {
  def awaitBootstrap: F[Unit]
}

object ConsensusInputGate {

  /** Read-only seed status exposed to ChainSync serving code. */
  trait ChainSeedStatus[F[_]] {
    def chainSeedResult: F[Option[Either[Throwable, Unit]]]
  }

  /** Capability owned by the leader-loop bootstrap fiber. It cannot release network input. */
  trait ChainSeedGate[F[_]] extends ChainSeedStatus[F] {
    def awaitLocalState: F[Unit]
    def awaitChainSeed: F[Unit]
    def completeChainSeed(result: Either[Throwable, Unit]): F[Boolean]
  }

  /** Write capability retained by Main. Each transition returns `true` only on its first successful completion. */
  trait Control[F[_]] {
    def readOnly: ConsensusInputGate[F]
    def chainSeed: ChainSeedGate[F]
    def markLocalStateReady: F[Boolean]
    def awaitChainSeed: F[Unit]
    def releaseInput: F[Boolean]
  }

  def make[F[_]: Concurrent]: F[Control[F]] =
    (Deferred[F, Unit], Deferred[F, Either[Throwable, Unit]], Deferred[F, Unit]).mapN {
      case (localStateReady, chainSeedReady, inputReady) =>
        new Control[F] {
          val readOnly: ConsensusInputGate[F] = new ConsensusInputGate[F] {
            def awaitBootstrap: F[Unit] = inputReady.get
          }

          val chainSeed: ChainSeedGate[F] = new ChainSeedGate[F] {
            def awaitLocalState: F[Unit] = localStateReady.get
            def awaitChainSeed: F[Unit] = chainSeedReady.get.flatMap(_.liftTo[F])
            def chainSeedResult: F[Option[Either[Throwable, Unit]]] = chainSeedReady.tryGet
            def completeChainSeed(result: Either[Throwable, Unit]): F[Boolean] =
              result match {
                case Left(_) => chainSeedReady.complete(result)
                case Right(_) =>
                  localStateReady.tryGet.flatMap {
                    case Some(_) => chainSeedReady.complete(result)
                    case None =>
                      Concurrent[F].raiseError(new IllegalStateException("Cannot seed GL0 chain before local state is ready"))
                  }
              }
          }

          def markLocalStateReady: F[Boolean] = localStateReady.complete(())
          def awaitChainSeed: F[Unit] = chainSeed.awaitChainSeed
          def releaseInput: F[Boolean] =
            chainSeedReady.tryGet.flatMap {
              case Some(Right(_)) => inputReady.complete(())
              case Some(Left(error)) =>
                Concurrent[F].raiseError(
                  new IllegalStateException("Cannot release GL0 consensus input after chain seed failure", error)
                )
              case None =>
                Concurrent[F].raiseError(new IllegalStateException("Cannot release GL0 consensus input before chain seed succeeds"))
            }
        }
    }
}
