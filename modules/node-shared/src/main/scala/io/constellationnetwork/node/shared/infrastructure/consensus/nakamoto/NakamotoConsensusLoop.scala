package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import java.security.KeyPair

import cats.effect.kernel.{Async, Ref}
import cats.effect.std.{Queue, Random}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash

import fs2.Stream
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** The Nakamoto slot-based consensus loop.
  *
  * Replaces ConsensusEventLoop's round-based FSM with a simple slot clock: every slot tick, check VRF eligibility, produce snapshot if
  * winner, collect attestations, apply fork choice.
  *
  * This is the core integration piece that wires together:
  *   - SlotClock (1s ticks)
  *   - EligibilityChecker (VRF + LDD threshold)
  *   - NakamotoProposer (snapshot production)
  *   - TipTracker (attestation collection + finality)
  *   - ChainSelection (fork choice)
  *   - gRPC sidecar client (publish/subscribe)
  */
object NakamotoConsensusLoop {

  /** Slot loop state — what the consensus loop tracks between ticks. */
  final case class LoopState(
    currentSlot: Long,
    lastProducedSlot: Option[Long],
    lastFinalizedOrdinal: SnapshotOrdinal,
    lastFinalizedHash: Hash,
    consecutiveEmptySlots: Long,
    totalProduced: Long,
    totalFinalized: Long,
    isActive: Boolean
  )

  object LoopState {
    def initial(startOrdinal: SnapshotOrdinal, startHash: Hash): LoopState =
      LoopState(
        currentSlot = 0L,
        lastProducedSlot = None,
        lastFinalizedOrdinal = startOrdinal,
        lastFinalizedHash = startHash,
        consecutiveEmptySlots = 0L,
        totalProduced = 0L,
        totalFinalized = 0L,
        isActive = false
      )
  }

  /** Commands the loop can receive from external sources (HTTP, download, recovery). */
  sealed trait LoopCommand
  object LoopCommand {
    case object Activate extends LoopCommand
    case object Deactivate extends LoopCommand
    case class ReceiveSnapshot(ordinal: SnapshotOrdinal, hash: Hash, slot: Long, fromPeer: PeerId) extends LoopCommand
    case class ReceiveAttestation(tipHash: Hash, tipSlot: Long, fromPeer: PeerId) extends LoopCommand
    case object SlotTick extends LoopCommand
  }

  /** Result of a single slot evaluation. */
  sealed trait SlotResult
  object SlotResult {
    case object NotActive extends SlotResult
    case object NotEligible extends SlotResult
    case class Won(slot: Long, ordinal: SnapshotOrdinal) extends SlotResult
    case class Error(slot: Long, cause: String) extends SlotResult
  }

  /** Build the slot-based consensus stream.
    *
    * The stream ticks every slotDurationMs (default 1000ms) and evaluates VRF eligibility. External events (received snapshots,
    * attestations) arrive via the command queue.
    */
  def build[F[_]: Async: Random](
    selfId: PeerId,
    keyPair: KeyPair,
    startOrdinal: SnapshotOrdinal,
    startHash: Hash,
    slotDurationMs: Long = 1000L,
    genesisTimeMs: Long
  ): F[BuiltLoop[F]] =
    for {
      implicit0(logger: SelfAwareStructuredLogger[F]) <- Slf4jLogger.create[F]
      stateRef <- Ref.of[F, LoopState](LoopState.initial(startOrdinal, startHash))
      commandQueue <- Queue.unbounded[F, LoopCommand]
    } yield {
      // Slot clock tick stream
      val slotTicks: Stream[F, Unit] =
        Stream
          .awakeEvery[F](slotDurationMs.millis)
          .evalMap(_ => commandQueue.offer(LoopCommand.SlotTick))

      // Command processing stream
      val commandStream: Stream[F, Unit] =
        Stream
          .fromQueueUnterminated(commandQueue)
          .evalMap { cmd =>
            stateRef.get.flatMap { state =>
              cmd match {
                case LoopCommand.Activate =>
                  logger.info("Nakamoto consensus activated") >>
                    stateRef.update(_.copy(isActive = true))

                case LoopCommand.Deactivate =>
                  logger.info("Nakamoto consensus deactivated") >>
                    stateRef.update(_.copy(isActive = false))

                case LoopCommand.SlotTick if !state.isActive =>
                  Async[F].unit // skip when not active

                case LoopCommand.SlotTick =>
                  val wallClockMs = System.currentTimeMillis()
                  val currentSlot = (wallClockMs - genesisTimeMs) / slotDurationMs
                  val slotGap = state.lastProducedSlot.fold(currentSlot)(currentSlot - _)

                  stateRef.update(_.copy(currentSlot = currentSlot)) >>
                    logger.debug(s"Slot $currentSlot (gap=$slotGap)")

                // TODO Phase 7: Wire EligibilityChecker.checkEligibility here
                // val eligible = eligibilityChecker.checkEligibility(currentSlot, slotGap, selfStake)
                // if eligible → call proposer to drain mempool and build snapshot
                // then publish via sidecar gRPC client
                // then update state (lastProducedSlot, totalProduced, reset consecutiveEmptySlots)

                case LoopCommand.ReceiveSnapshot(ordinal, hash, slot, fromPeer) =>
                  logger.info(s"Received snapshot ordinal=$ordinal slot=$slot from=$fromPeer") >>
                    stateRef.update(
                      _.copy(
                        consecutiveEmptySlots = 0L
                      )
                    )

                // TODO Phase 7: Verify SlotCertificate, apply fork choice via ChainSelection
                // Update TipTracker with new chain tip candidate
                // Create own TipAttestation if valid
                // Publish attestation via sidecar

                case LoopCommand.ReceiveAttestation(tipHash, tipSlot, fromPeer) =>
                  logger.debug(s"Received attestation tip=$tipHash slot=$tipSlot from=$fromPeer")

                // TODO Phase 7: Feed to TipTracker.addAttestation
                // Check if threshold reached → finalize
                // Update lastFinalizedOrdinal/Hash on finalization
              }
            }
          }

      // Combined stream: slot ticks + command processing in parallel
      val run: Stream[F, Unit] = slotTicks.merge(commandStream)

      BuiltLoop(run, stateRef, commandQueue)
    }

  final case class BuiltLoop[F[_]](
    run: Stream[F, Unit],
    stateRef: Ref[F, LoopState],
    commandQueue: Queue[F, LoopCommand]
  )
}
