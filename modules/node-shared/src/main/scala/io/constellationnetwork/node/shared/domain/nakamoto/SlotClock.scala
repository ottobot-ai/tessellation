package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Clock, Sync}
import cats.syntax.all._

import io.constellationnetwork.schema.nakamoto.slot.Slot

/**
 * Slot clock for Nakamoto consensus.
 *
 * Each slot is 1 second. The genesis slot starts at a configured epoch time.
 * Nodes use local system clock (NTP-synced assumed) with configurable skew tolerance.
 */
trait SlotClock[F[_]] {

  /** Current slot based on wall clock */
  def currentSlot: F[Slot]

  /** Wall-clock time when a given slot starts (epoch millis) */
  def slotStartTime(slot: Slot): Long

  /** Wall-clock time when a given slot ends (epoch millis) */
  def slotEndTime(slot: Slot): Long

  /** Whether the given slot is within acceptable skew of current time */
  def isSlotCurrent(slot: Slot): F[Boolean]
}

object SlotClock {

  case class Config(
    genesisTimeMs: Long,            // Unix epoch millis when slot 0 starts
    slotDurationMs: Long = 1000L,   // 1 second per slot
    skewToleranceMs: Long = 1000L   // ±1 second tolerance
  )

  def make[F[_]: Sync: Clock](config: Config): SlotClock[F] = new SlotClock[F] {

    def currentSlot: F[Slot] =
      Clock[F].realTime.map { duration =>
        val nowMs = duration.toMillis
        val elapsed = nowMs - config.genesisTimeMs
        if (elapsed < 0) Slot.MinValue
        else Slot.unsafeApply(elapsed / config.slotDurationMs)
      }

    def slotStartTime(slot: Slot): Long =
      config.genesisTimeMs + (slot.value.value * config.slotDurationMs)

    def slotEndTime(slot: Slot): Long =
      slotStartTime(slot) + config.slotDurationMs

    def isSlotCurrent(slot: Slot): F[Boolean] =
      Clock[F].realTime.map { duration =>
        val nowMs = duration.toMillis
        val start = slotStartTime(slot)
        val end = slotEndTime(slot)
        nowMs >= (start - config.skewToleranceMs) && nowMs <= (end + config.skewToleranceMs)
      }
  }
}
