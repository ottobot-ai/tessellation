package io.constellationnetwork.dag.l1

import java.security.KeyPair

import cats.Applicative
import cats.effect.kernel.Async
import cats.effect.std.Random
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.currency.tokenlock.ConsensusInput.OwnerConsensusInput
import io.constellationnetwork.currency.tokenlock.{ConsensusInput, ConsensusOutput}
import io.constellationnetwork.dag.l1.domain.tokenlock.block.TokenLockBlockService
import io.constellationnetwork.dag.l1.modules.{Queues, Services}
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage
import io.constellationnetwork.node.shared.domain.globalAlignment.GlobalL0AlignmentStorage
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.domain.tokenlock.block.{TokenLockBlockNotAcceptedReason, TokenLockBlockStorage}
import io.constellationnetwork.node.shared.domain.tokenlock.consensus.Validator._
import io.constellationnetwork.node.shared.domain.tokenlock.consensus.config.TokenLockConsensusConfig
import io.constellationnetwork.node.shared.domain.tokenlock.consensus.{ConsensusClient, ConsensusState, Engine}
import io.constellationnetwork.node.shared.domain.tokenlock.{TokenLockStorage, TokenLockValidator}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo, StateProof}
import io.constellationnetwork.schema.tokenLock.TokenLockBlock
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import fs2.{Pipe, Stream}
import org.typelevel.log4cats.slf4j.Slf4jLogger

object TokenLock {
  def run[
    F[_]: Async: Hasher: SecurityProvider: Random,
    P <: StateProof,
    S <: Snapshot,
    SI <: SnapshotInfo[P],
    R <: CliMethod
  ](
    tokenLockConsensusConfig: TokenLockConsensusConfig,
    clusterStorage: ClusterStorage[F],
    lastGlobalSnapshot: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    nodeStorage: NodeStorage[F],
    // (#196 follow-up) Per-call-site dispatch for the L0 send hop. dl1 → gl0
    // passes a sidecar-publish lambda (durable outbox); cl1 → cl0 keeps the
    // single-peer HTTP POST shape (cl0 has no sidecar wiring). The lambda
    // owns its own error handling.
    sendBlockToL0Fn: Signed[TokenLockBlock] => F[Unit],
    consensusClient: ConsensusClient[F],
    services: Services[F, P, S, SI, R],
    tokenLockStorage: TokenLockStorage[F],
    tokenLockBlockStorage: TokenLockBlockStorage[F],
    queues: Queues[F],
    tokenLockValidator: TokenLockValidator[F],
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
          canStartOwnTokenLockConsensus(
            nodeStorage,
            clusterStorage,
            lastGlobalSnapshot,
            tokenLockConsensusConfig.peersCount,
            tokenLockStorage
          ).handleErrorWith { e =>
            logger.warn(e)("Failure checking if token lock own consensus can be kicked off!").as(false)
          }
        }
        .as(ConsensusInput.OwnRoundTrigger)

    def ownerBlockConsensusInputs =
      inspectionTrigger.merge(ownRoundTrigger)

    def peerBlockConsensusInputs =
      Stream
        .fromQueueUnterminated(queues.tokenLockConsensusInput)
        .evalFilter(isPeerInputValid(_))
        .evalFilter(_ => isLastGlobalSnapshotPresent(lastGlobalSnapshot))
        .map(_.value)

    def blockConsensusInputs =
      ownerBlockConsensusInputs.merge(peerBlockConsensusInputs)

    def runConsensus: Pipe[F, ConsensusInput, ConsensusOutput.FinalBlock] =
      _.evalMapAccumulate(ConsensusState.Empty) {
        Engine
          .fsm(
            tokenLockConsensusConfig,
            clusterStorage,
            lastGlobalSnapshot,
            consensusClient,
            tokenLockValidator,
            tokenLockStorage,
            selfId,
            selfKeyPair
          )
          .run
      }.handleErrorWith { e =>
        Stream.eval(logger.error(e)("Error during token lock block creation")) >> Stream.empty
      }.flatMap {
        case (_, fb @ ConsensusOutput.FinalBlock(hashedBlock)) =>
          Stream
            .eval(logger.debug(s"Token lock block created! Hash=${hashedBlock.hash}, ProofsHash=${hashedBlock.proofsHash}"))
            .as(fb)
        case (_, ConsensusOutput.CleanedConsensuses(ids)) =>
          Stream.eval(logger.debug(s"Cleaned consensuses ids=$ids")) >> Stream.empty
        case (_, ConsensusOutput.Noop) => Stream.empty
      }

    // (#196 follow-up) Delegate the L0 send hop to the call-site-supplied
    // `sendBlockToL0Fn`. dl1 → gl0 uses a sidecar-publish lambda (durable
    // outbox path); cl1 → cl0 keeps the previous HTTP POST shape (cl0
    // doesn't run a sidecar). The pipe only sequences; the lambda owns
    // error handling so we don't blanket-eat sidecar errors into the cl0
    // path.
    def sendBlockToL0: Pipe[F, ConsensusOutput.FinalBlock, ConsensusOutput.FinalBlock] =
      _.evalTap(fb => sendBlockToL0Fn(fb.hashedBlock.signed))

    def gossipBlock: Pipe[F, ConsensusOutput.FinalBlock, ConsensusOutput.FinalBlock] =
      _.evalTap { fb =>
        services.gossip
          .spreadCommon(fb.hashedBlock.signed)
          .handleErrorWith(e => logger.warn(e)("TokenLockBlock gossip spread failed!"))
      }

    def peerBlocks: Stream[F, ConsensusOutput.FinalBlock] = Stream
      .fromQueueUnterminated(queues.tokenLocksBlocks)
      .evalMap(_.toHashedWithSignatureCheck)
      .evalTap {
        case Left(e)  => logger.warn(e)("Received an invalidly signed token lock block!")
        case Right(_) => Async[F].unit
      }
      .collect {
        case Right(hashedBlock) => ConsensusOutput.FinalBlock(hashedBlock)
      }

    def storeBlock: Pipe[F, ConsensusOutput.FinalBlock, Unit] =
      _.evalMap { fb =>
        tokenLockBlockStorage.store(fb.hashedBlock).handleErrorWith(e => logger.debug(e)("TokenLockBlock storing failed."))
      }

    def blockAcceptance: Stream[F, Unit] = Stream
      .awakeEvery(1.second)
      .evalMap { _ =>
        tokenLockBlockStorage.getWaiting.flatTap { awaiting =>
          Applicative[F].whenA(awaiting.nonEmpty) {
            logger.debug(s"Pulled following token lock blocks for acceptance ${awaiting.keySet}")
          }
        }.flatMap {
          _.toList.traverse {
            case (hash, signedBlock) =>
              services.tokenLockBlock
                .accept(signedBlock, SnapshotOrdinal.MinValue)
                .handleErrorWith {
                  // Permanent rejection — block was already dropped from Waiting in `processAcceptanceError`.
                  case e: TokenLockBlockService.TokenLockBlockAcceptanceError if TokenLockBlockNotAcceptedReason.isPermanent(e.reason) =>
                    logger.warn(s"Permanently rejected token lock block ${hash.show}: ${e.reason} — dropped, no redownload")
                  case error =>
                    for {
                      _ <- logger.warn(error)(s"Failed acceptance of a token lock block with ${hash.show}")
                      _ <- globalL0AlignmentStorage.updateShouldRedownload(
                        value = true,
                        reasons = List(s"Token Lock block acceptance failed for ${hash.show}: ${error.getMessage}")
                      )
                    } yield ()
                }
          }
        }.void
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
