package io.constellationnetwork.node.shared.domain.nakamoto

import java.nio.ByteBuffer
import java.security.MessageDigest

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.schema.nakamoto.slot.Slot

/**
 * Mutable state tracking the current Nakamoto epoch.
 *
 * Tracks:
 * - Current epoch randomness (eta)
 * - Last slot that produced a snapshot (for computing slot gap / δ)
 * - VRF outputs collected in this epoch (for next eta computation)
 */
trait EpochState[F[_]] {

  /** Current epoch randomness (32 bytes) */
  def currentEta: F[Array[Byte]]

  /** Slot of the last produced snapshot (for computing δ = current - lastProduced) */
  def lastProducedSlot: F[Slot]

  /** Record that a snapshot was produced at the given slot */
  def recordProduction(slot: Slot, vrfOutput: Array[Byte]): F[Unit]

  /** Rotate epoch: compute next eta from accumulated VRF outputs, reset accumulator */
  def rotateEpoch(epochNumber: Long): F[Array[Byte]]

  /** Get accumulated VRF outputs count (for testing) */
  def accumulatedCount: F[Int]
}

object EpochState {

  case class Config(
    slotsPerEpoch: Long = 60L // epoch progress ticks every 60 slots (60 seconds)
  )

  def make[F[_]: Sync](
    genesisEta: Array[Byte],
    config: Config = Config()
  ): F[EpochState[F]] =
    for {
      etaRef      <- Ref.of[F, Array[Byte]](genesisEta)
      lastSlotRef <- Ref.of[F, Slot](Slot.MinValue)
      vrfAccRef   <- Ref.of[F, List[Array[Byte]]](List.empty)
    } yield new EpochState[F] {

      def currentEta: F[Array[Byte]] = etaRef.get

      def lastProducedSlot: F[Slot] = lastSlotRef.get

      def recordProduction(slot: Slot, vrfOutput: Array[Byte]): F[Unit] =
        lastSlotRef.set(slot) >> vrfAccRef.update(vrfOutput :: _)

      def rotateEpoch(epochNumber: Long): F[Array[Byte]] =
        for {
          prevEta <- etaRef.get
          outputs <- vrfAccRef.getAndSet(List.empty)
          // Use first 2/3 of epoch's VRF outputs for eta
          twoThirds = outputs.reverse.take((outputs.size * 2 / 3).max(1))
          nextEta   = computeNextEta(prevEta, epochNumber, twoThirds)
          _         <- etaRef.set(nextEta)
        } yield nextEta

      def accumulatedCount: F[Int] = vrfAccRef.get.map(_.size)
    }

  /**
   * Compute next epoch's eta (randomness) from previous eta, epoch number, and VRF outputs.
   *
   * Uses SHA-256 to hash: prevEta || epochNumber || concat(vrfOutputs)
   * This follows the Ouroboros Praos approach to epoch randomness.
   */
  def computeNextEta(prevEta: Array[Byte], epochNumber: Long, vrfOutputs: List[Array[Byte]]): Array[Byte] = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(prevEta)
    digest.update(ByteBuffer.allocate(8).putLong(epochNumber).array())
    vrfOutputs.foreach(digest.update)
    digest.digest()
  }
}
