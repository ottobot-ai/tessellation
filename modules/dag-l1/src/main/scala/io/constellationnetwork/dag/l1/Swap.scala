package io.constellationnetwork.dag.l1

import java.security.KeyPair

import cats.Applicative
import cats.effect.kernel.Async
import cats.effect.std.Random
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.currency.swap.ConsensusInput.OwnerConsensusInput
import io.constellationnetwork.currency.swap.{ConsensusInput, ConsensusOutput}
import io.constellationnetwork.dag.l1.domain.swap.block.AllowSpendBlockService
import io.constellationnetwork.dag.l1.modules.{Queues, Services}
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage
import io.constellationnetwork.node.shared.domain.consensus.config.SwapConsensusConfig
import io.constellationnetwork.node.shared.domain.globalAlignment.GlobalL0AlignmentStorage
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.domain.swap.block.{AllowSpendBlockNotAcceptedReason, AllowSpendBlockStorage}
import io.constellationnetwork.node.shared.domain.swap.consensus.Validator.{
  canStartOwnSwapConsensus,
  isLastGlobalSnapshotPresent,
  isPeerInputValid
}
import io.constellationnetwork.node.shared.domain.swap.consensus.{ConsensusClient, ConsensusState, Engine}
import io.constellationnetwork.node.shared.domain.swap.{AllowSpendStorage, AllowSpendValidator}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo, StateProof}
import io.constellationnetwork.schema.swap.AllowSpendBlock
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import fs2.{Pipe, Stream}
import org.typelevel.log4cats.slf4j.Slf4jLogger

object Swap {
  def run[
    F[_]: Async: Hasher: SecurityProvider: Random,
    P <: StateProof,
    S <: Snapshot,
    SI <: SnapshotInfo[P],
    R <: CliMethod
  ](
    swapConsensusCfg: SwapConsensusConfig,
    clusterStorage: ClusterStorage[F],
    lastGlobalSnapshot: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    nodeStorage: NodeStorage[F],
    // (#196 follow-up) Per-call-site dispatch for the L0 send hop. dl1 → gl0
    // passes a sidecar-publish lambda (durable outbox; the prior #196 path);
    // cl1 → cl0 keeps the single-peer HTTP POST shape (cl0 has no sidecar).
    // The lambda owns its own error handling.
    sendBlockToL0Fn: Signed[AllowSpendBlock] => F[Unit],
    consensusClient: ConsensusClient[F],
    services: Services[F, P, S, SI, R],
    allowSpendStorage: AllowSpendStorage[F],
    allowSpendBlockStorage: AllowSpendBlockStorage[F],
    queues: Queues[F],
    allowSpendValidator: AllowSpendValidator[F],
    selfKeyPair: KeyPair,
    selfId: PeerId,
    globalL0AlignmentStorage: GlobalL0AlignmentStorage[F]
  ): Stream[F, Unit] = {

    def logger = Slf4jLogger.getLogger[F]

    def inspectionTrigger: Stream[F, OwnerConsensusInput] =
      Stream
        .awakeEvery(5.seconds)
        .evalFilter(_ => isLastGlobalSnapshotPresent(lastGlobalSnapshot))
        .as(ConsensusInput.InspectionTrigger)

    def ownRoundTrigger: Stream[F, OwnerConsensusInput] =
      Stream
        .awakeEvery(5.seconds)
        .evalFilter { _ =>
          canStartOwnSwapConsensus(
            nodeStorage,
            clusterStorage,
            lastGlobalSnapshot,
            swapConsensusCfg.peersCount,
            allowSpendStorage
          ).handleErrorWith { e =>
            logger.warn(e)("Failure checking if own consensus can be kicked off!").as(false)
          }
        }
        .as(ConsensusInput.OwnRoundTrigger)

    def ownerBlockConsensusInputs =
      inspectionTrigger.merge(ownRoundTrigger)

    def peerBlockConsensusInputs =
      Stream
        .fromQueueUnterminated(queues.swapPeerConsensusInput)
        .evalFilter(isPeerInputValid(_))
        .evalFilter(_ => isLastGlobalSnapshotPresent(lastGlobalSnapshot))
        .map(_.value)

    def blockConsensusInputs =
      ownerBlockConsensusInputs.merge(peerBlockConsensusInputs)

    def runConsensus: Pipe[F, ConsensusInput, ConsensusOutput.FinalBlock] =
      _.evalMapAccumulate(ConsensusState.Empty) {
        Engine
          .fsm(
            swapConsensusCfg,
            clusterStorage,
            lastGlobalSnapshot,
            consensusClient,
            allowSpendValidator,
            allowSpendStorage,
            selfId,
            selfKeyPair
          )
          .run
      }.handleErrorWith { e =>
        Stream.eval(logger.error(e)("Error during swap block creation")) >> Stream.empty
      }.flatMap {
        case (_, fb @ ConsensusOutput.FinalBlock(hashedBlock)) =>
          Stream
            .eval(logger.debug(s"Swap block created! Hash=${hashedBlock.hash}, ProofsHash=${hashedBlock.proofsHash}"))
            .as(fb)
        case (_, ConsensusOutput.CleanedConsensuses(ids)) =>
          Stream.eval(logger.debug(s"Cleaned consensuses ids=$ids")) >> Stream.empty
        case (_, ConsensusOutput.Noop) => Stream.empty
      }

    // (#196 follow-up) Delegate the L0 send hop to the call-site-supplied
    // `sendBlockToL0Fn`. dl1 → gl0 uses a sidecar-publish lambda (the prior
    // #196 durable-outbox path); cl1 → cl0 keeps the previous HTTP POST
    // shape (cl0 doesn't run a sidecar). The pipe only sequences; the
    // lambda owns error handling.
    def sendBlockToL0: Pipe[F, ConsensusOutput.FinalBlock, ConsensusOutput.FinalBlock] =
      _.evalTap(fb => sendBlockToL0Fn(fb.hashedBlock.signed))

    def gossipBlock: Pipe[F, ConsensusOutput.FinalBlock, ConsensusOutput.FinalBlock] =
      _.evalTap { fb =>
        services.gossip
          .spreadCommon(fb.hashedBlock.signed)
          .handleErrorWith(e => logger.warn(e)("AllowSpendBlock gossip spread failed!"))
      }

    def peerBlocks: Stream[F, ConsensusOutput.FinalBlock] = Stream
      .fromQueueUnterminated(queues.allowSpendBlocks)
      .evalMap(_.toHashedWithSignatureCheck)
      .evalTap {
        case Left(e)  => logger.warn(e)("Received an invalidly signed allow spend block!")
        case Right(_) => Async[F].unit
      }
      .collect {
        case Right(hashedBlock) => ConsensusOutput.FinalBlock(hashedBlock)
      }

    def storeBlock: Pipe[F, ConsensusOutput.FinalBlock, Unit] =
      _.evalMap { fb =>
        allowSpendBlockStorage.store(fb.hashedBlock).handleErrorWith(e => logger.debug(e)("AllowSpendBlock storing failed."))
      }

    def blockAcceptance: Stream[F, Unit] = Stream
      .awakeEvery(1.seconds)
      .evalMap { _ =>
        lastGlobalSnapshot.getOrdinal.flatMap {
          case Some(snapshotOrdinal) =>
            allowSpendBlockStorage.getWaiting.flatTap { awaiting =>
              Applicative[F].whenA(awaiting.nonEmpty) {
                logger.debug(s"Pulled following allow spend blocks for acceptance ${awaiting.keySet}")
              }
            }.flatMap {
              _.toList.traverse {
                case (hash, signedBlock) =>
                  services.allowSpendBlock
                    .accept(signedBlock, snapshotOrdinal)
                    .handleErrorWith {
                      // Permanent rejection — block was already dropped from Waiting in `processAcceptanceError`;
                      // no point forcing a global redownload (chain state is fine, our local block was bad).
                      case e: AllowSpendBlockService.AllowSpendBlockAcceptanceError
                          if AllowSpendBlockNotAcceptedReason.isPermanent(e.reason) =>
                        logger.warn(s"Permanently rejected allow spend block ${hash.show}: ${e.reason} — dropped, no redownload")
                      case error =>
                        for {
                          _ <- logger.warn(error)(s"Failed acceptance of an allow spend block with ${hash.show}")
                          _ <- globalL0AlignmentStorage.updateShouldRedownload(
                            value = true,
                            reasons = List(s"Allow spend block acceptance failed for ${hash.show}: ${error.getMessage}")
                          )
                        } yield ()
                    }
              }
            }.void
          case None => ().pure[F]
        }
      }

    def blockConsensus: Stream[F, Unit] =
      blockConsensusInputs
        .through(runConsensus)
        .through(gossipBlock)
        .through(sendBlockToL0)
        .merge(peerBlocks)
        .through(storeBlock)

    blockConsensus
      .merge(blockAcceptance)

  }
}
