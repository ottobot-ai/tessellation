package io.constellationnetwork.dag.l1

import java.security.KeyPair

import cats.data.OptionT
import cats.effect.Async
import cats.effect.std.{Random, Semaphore}
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.order._
import cats.syntax.show._
import cats.syntax.traverse._

import scala.concurrent.duration.DurationInt

import io.constellationnetwork.dag.l1.config.types.AppConfig
import io.constellationnetwork.dag.l1.domain.block.BlockService
import io.constellationnetwork.dag.l1.domain.consensus.block.BlockConsensusInput._
import io.constellationnetwork.dag.l1.domain.consensus.block.BlockConsensusOutput.{CleanedConsensuses, FinalBlock, NoData}
import io.constellationnetwork.dag.l1.domain.consensus.block.Validator.{canStartInspectionTrigger, canStartOwnConsensus, isPeerInputValid}
import io.constellationnetwork.dag.l1.domain.consensus.block._
import io.constellationnetwork.dag.l1.http.p2p.P2PClient
import io.constellationnetwork.dag.l1.modules._
import io.constellationnetwork.ext.fs2.StreamOps
import io.constellationnetwork.kernel.CellError
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.domain.block.processing.BlockNotAcceptedReason
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.Block
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo, StateProof}
import io.constellationnetwork.security._
import io.constellationnetwork.security.signature.Signed

import fs2.{Pipe, Stream}
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class StateChannel[
  F[_]: Async: HasherSelector: SecurityProvider: Random,
  P <: StateProof,
  S <: Snapshot,
  SI <: SnapshotInfo[P],
  R <: CliMethod
](
  appConfig: AppConfig,
  blockAcceptanceS: Semaphore[F],
  blockCreationS: Semaphore[F],
  blockStoringS: Semaphore[F],
  keyPair: KeyPair,
  p2PClient: P2PClient[F],
  programs: Programs[F, P, S, SI],
  queues: Queues[F],
  selfId: PeerId,
  services: Services[F, P, S, SI, R],
  storages: Storages[F, P, S, SI],
  validators: Validators[F],
  txHasher: Hasher[F],
  // (#196 follow-up) Per-call-site dispatch for the L0 send hop. dl1 → gl0
  // passes a sidecar-publish lambda (durable outbox); cl1 → cl0 keeps the
  // single-peer HTTP POST shape (cl0 has no sidecar wiring). Decoupling here
  // means the dl1 → gl0 fragility is fixed without changing the cl1 → cl0
  // semantics that aren't broken. Errors are logged + swallowed inside this
  // pipe; the lambda itself can no-op + log on failure or fail loudly — the
  // contract here is "fire and observe".
  sendBlockToL0Fn: Signed[Block] => F[Unit]
) {

  private implicit val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  private val blockConsensusContext =
    BlockConsensusContext[F](
      p2PClient.blockConsensus,
      storages.block,
      validators.block,
      storages.cluster,
      appConfig.consensus,
      storages.consensus,
      keyPair,
      selfId,
      storages.transaction,
      validators.transaction,
      txHasher
    )

  private val inspectionTriggerInput: Stream[F, OwnerBlockConsensusInput] = Stream
    .awakeEvery(5.seconds)
    .evalMap { _ =>
      canStartInspectionTrigger(
        storages.lastSnapshot.getOrdinal
      ).handleErrorWith { e =>
        logger.warn(e)("Failure checking if inspection trigger consensus can be kicked off!").map(_ => false)
      }
    }
    .filter(identity)
    .as(InspectionTrigger)

  private val ownRoundTriggerInput: Stream[F, OwnerBlockConsensusInput] = Stream
    .awakeEvery(5.seconds)
    .evalMapLocked(blockCreationS) { _ =>
      canStartOwnConsensus(
        storages.consensus,
        storages.node,
        storages.cluster,
        storages.block,
        storages.transaction,
        appConfig.consensus.peersCount,
        appConfig.consensus.tipsCount
      ).handleErrorWith { e =>
        logger.warn(e)("Failure checking if own consensus can be kicked off!").map(_ => false)
      }
    }
    .filter(identity)
    .as(OwnRoundTrigger)

  private val ownerBlockConsensusInputs: Stream[F, OwnerBlockConsensusInput] =
    inspectionTriggerInput.merge(ownRoundTriggerInput)

  private val peerBlockConsensusInputs: Stream[F, PeerBlockConsensusInput] = Stream
    .fromQueueUnterminated(queues.peerBlockConsensusInput)
    .evalFilter(in => HasherSelector[F].withCurrent(implicit hasher => isPeerInputValid(in)))
    .map(_.value)

  private val blockConsensusInputs: Stream[F, BlockConsensusInput] =
    ownerBlockConsensusInputs.merge(peerBlockConsensusInputs)

  private val runConsensus: Pipe[F, BlockConsensusInput, FinalBlock] =
    _.evalTap(input => logger.debug(s"Received block consensus input to process: ${input.show}"))
      .evalMap(blockConsensusInput =>
        OptionT(storages.lastSnapshot.getOrdinal)
          .getOrRaise(new IllegalStateException("Could not find the latest snapshot ordinal"))
          .flatMap(ordinal =>
            HasherSelector[F].withCurrent { implicit hasher =>
              new BlockConsensusCell[F](blockConsensusInput, blockConsensusContext, ordinal).run()
            }
          )
          .handleErrorWith(e => CellError(e.getMessage).asLeft[BlockConsensusOutput].pure[F])
      )
      .flatMap {
        case Left(ce) =>
          Stream.eval(logger.warn(ce)(s"Error occurred during some step of block consensus.")) >>
            Stream.empty
        case Right(ohm) =>
          ohm match {
            case fb @ FinalBlock(hashedBlock) =>
              Stream
                .eval(logger.debug(s"Block created! Hash=${hashedBlock.hash} ProofsHash=${hashedBlock.proofsHash}"))
                .as(fb)
            case CleanedConsensuses(ids) =>
              Stream.eval(logger.warn(s"Cleaned following timed-out consensuses: $ids")) >>
                Stream.empty
            case NoData => Stream.empty
          }
      }

  private val gossipBlock: Pipe[F, FinalBlock, FinalBlock] =
    _.evalTap { fb =>
      services.gossip
        .spreadCommon(fb.hashedBlock.signed)
        .handleErrorWith(e => logger.warn(e)("Block gossip spread failed!"))
    }

  private val peerBlocks: Stream[F, FinalBlock] = Stream
    .fromQueueUnterminated(queues.peerBlock)
    .evalMap(block => HasherSelector[F].withCurrent(implicit hasher => block.toHashedWithSignatureCheck))
    .evalTap {
      case Left(e)  => logger.warn(e)(s"Received an invalidly signed peer block!")
      case Right(_) => Async[F].unit
    }
    .collect {
      case Right(hashedBlock) => FinalBlock(hashedBlock)
    }

  private val storeBlock: Pipe[F, FinalBlock, Unit] =
    _.evalMapLocked(blockStoringS) { fb =>
      storages.lastSnapshot.getHeight.map(_.getOrElse(Height.MinValue)).flatMap { lastSnapshotHeight =>
        if (lastSnapshotHeight < fb.hashedBlock.height)
          storages.block.store(fb.hashedBlock).handleErrorWith(e => logger.debug(e)("Block storing failed."))
        else
          logger.debug(
            s"Block can't be stored! Block height not above last snapshot height! block:${fb.hashedBlock.height} <= snapshot: $lastSnapshotHeight"
          )
      }
    }

  // (#196 follow-up) Delegates the L0 send hop to the call-site-supplied
  // `sendBlockToL0Fn`. dl1 → gl0 uses a sidecar-publish lambda (durable
  // outbox path); cl1 → cl0 keeps the previous HTTP POST shape (cl0 doesn't
  // run a sidecar). The pipe only sequences + logs at the outer layer; the
  // lambda owns its own error handling so we don't blanket-eat sidecar
  // errors into the cl0 path.
  private val sendBlockToL0: Pipe[F, FinalBlock, FinalBlock] =
    _.evalTap(fb => sendBlockToL0Fn(fb.hashedBlock.signed))

  private val blockAcceptance: Stream[F, Unit] = Stream
    .awakeEvery(1.seconds)
    .evalMapLocked(blockAcceptanceS) { _ =>
      storages.block.getWaiting.flatTap { awaiting =>
        if (awaiting.nonEmpty) logger.debug(s"Pulled following blocks for acceptance ${awaiting.keySet}")
        else Async[F].unit
      }.flatMap(
        _.toList
          .sortBy(_._2.value.height)
          .traverse {
            case (hash, signedBlock) =>
              logger.debug(s"Acceptance of a block $hash starts!") >>
                HasherSelector[F].withCurrent { implicit hasher =>
                  services.block
                    .accept(signedBlock)
                }.handleErrorWith {
                  // Permanent rejection (e.g. lost-consensus orphan whose parent ordinal sits below the chain's
                  // current lastTxOrdinal): the block was already dropped from Waiting in `processAcceptanceError`,
                  // and forcing a global redownload won't help — the chain state is fine, our local block was bad.
                  case e: BlockService.BlockAcceptanceError if BlockNotAcceptedReason.isPermanent(e.reason) =>
                    logger.warn(s"Permanently rejected block ${hash.show}: ${e.reason} — dropped, no redownload")
                  case error =>
                    for {
                      _ <- logger.warn(error)(s"Failed acceptance of a block with ${hash.show}")
                      _ <- storages.globalL0Alignment.updateShouldRedownload(
                        value = true,
                        reasons = List(s"Block acceptance failed for ${hash.show}: ${error.getMessage}")
                      )
                    } yield ()
                }
          }
          .void
      )
    }

  private val blockConsensus: Stream[F, Unit] =
    blockConsensusInputs
      .through(runConsensus)
      .through(gossipBlock)
      .through(sendBlockToL0)
      .merge(peerBlocks)
      .through(storeBlock)

  val runtime: Stream[F, Unit] =
    blockConsensus
      .merge(blockAcceptance)

}

object StateChannel {

  def make[
    F[_]: Async: HasherSelector: SecurityProvider: Random,
    P <: StateProof,
    S <: Snapshot,
    SI <: SnapshotInfo[P],
    R <: CliMethod
  ](
    appConfig: AppConfig,
    keyPair: KeyPair,
    p2PClient: P2PClient[F],
    programs: Programs[F, P, S, SI],
    queues: Queues[F],
    selfId: PeerId,
    services: Services[F, P, S, SI, R],
    storages: Storages[F, P, S, SI],
    validators: Validators[F],
    txHasher: Hasher[F],
    sendBlockToL0Fn: Signed[Block] => F[Unit]
  ): F[StateChannel[F, P, S, SI, R]] =
    for {
      blockAcceptanceS <- Semaphore(1)
      blockCreationS <- Semaphore(1)
      blockStoringS <- Semaphore(1)
    } yield
      new StateChannel[F, P, S, SI, R](
        appConfig,
        blockAcceptanceS,
        blockCreationS,
        blockStoringS,
        keyPair,
        p2PClient,
        programs,
        queues,
        selfId,
        services,
        storages,
        validators,
        txHasher,
        sendBlockToL0Fn
      )
}
