package io.constellationnetwork.serde.era

import cats.syntax.all._

/** Dispatches (ordinal → SerdeEra) based on a configured list of half-open ranges.
  *
  * Replaces the pattern-match in `Hasher.scala:50-58` with a data-driven registry so
  * adding a third (or fourth) era is a config change, not a code change.
  *
  * Construction validates:
  *   - ranges are non-overlapping
  *   - ranges cover `[0, +∞)` with no gaps (the last range must be open-ended)
  *
  * Use `EraCodecRegistry.fromRanges(...)` from config at startup; pass the registry
  * wherever era-based dispatch is needed (snapshot decode, legacy bridge, etc.).
  */
final class EraCodecRegistry private (ranges: List[(OrdinalRange, SerdeEra)]) {

  /** The era active for a given ordinal, or `None` if the config has a gap
    * (shouldn't happen — the builder validates coverage). */
  def eraFor(ordinal: Long): Option[SerdeEra] =
    ranges.collectFirst { case (r, era) if r.contains(ordinal) => era }

  /** Era active at a given ordinal. Throws if the config is broken — validated at
    * startup, so a runtime miss is a bug. */
  def eraForOrDie(ordinal: Long): SerdeEra =
    eraFor(ordinal).getOrElse(
      throw new IllegalStateException(
        s"EraCodecRegistry has no era for ordinal $ordinal — config coverage is broken."
      )
    )

  /** List of configured (range, era) pairs in ordinal order. */
  def entries: List[(OrdinalRange, SerdeEra)] = ranges
}

object EraCodecRegistry {

  /** Build from a config-driven list of (range, era) pairs. Validates:
    *   - list is non-empty
    *   - no overlapping ranges
    *   - contiguous coverage from 0 upward
    *   - last range is open-ended (covers future ordinals)
    */
  def fromRanges(raw: List[(OrdinalRange, SerdeEra)]): Either[String, EraCodecRegistry] = {
    if (raw.isEmpty) Left("EraCodecRegistry requires at least one (range, era) entry").asRight.swap.leftMap(_ => "").swap.flatMap { _ =>
      Left("EraCodecRegistry requires at least one (range, era) entry")
    } else {
      val sorted = raw.sortBy(_._1.from)

      // 1. No overlaps.
      val overlap = sorted
        .sliding(2)
        .collectFirst {
          case List((a, eraA), (b, eraB)) if a.overlaps(b) =>
            s"Overlapping eras: ${eraA.name}=$a and ${eraB.name}=$b"
        }

      // 2. Contiguous coverage starting at 0.
      val firstStart = sorted.head._1.from
      val startsAtZero = firstStart == 0L

      val gap = sorted
        .sliding(2)
        .collectFirst {
          case List((a, eraA), (b, eraB)) if a.until.exists(_ != b.from) =>
            s"Gap between ${eraA.name} (ends at ${a.until.get}) and ${eraB.name} (starts at ${b.from})"
        }

      // 3. Last range must be open.
      val lastOpen = sorted.last._1.until.isEmpty

      val errors = List(
        overlap,
        if (!startsAtZero) Some(s"First era must start at ordinal 0, got $firstStart") else None,
        gap,
        if (!lastOpen) Some("Last era must be open-ended (no `until` bound)") else None
      ).flatten

      if (errors.nonEmpty) Left(errors.mkString("; "))
      else Right(new EraCodecRegistry(sorted))
    }
  }

  /** Default all-Scodec registry — one era covering [0, ∞). Useful for tests or for
    * a fresh chain with no historical data. Production clusters configure the full
    * kryo → json → scodec progression via HOCON. */
  val defaultScodec: EraCodecRegistry =
    fromRanges(List(OrdinalRange.from(0L) -> SerdeEra.Scodec))
      .getOrElse(throw new IllegalStateException("defaultScodec registry failed to construct"))
}
