package io.constellationnetwork.serde.era

/** A half-open ordinal range `[from, until)` with optional open upper bound.
  *
  * Eras are configured as a *list* of these; every observable ordinal must fall inside exactly one era for a given (type, purpose). The
  * registry validates non-overlap and contiguous coverage at startup.
  */
final case class OrdinalRange(from: Long, until: Option[Long]) {
  require(from >= 0L, s"OrdinalRange.from must be non-negative, got $from")
  require(until.forall(_ > from), s"OrdinalRange.until must be > from; from=$from until=$until")

  def contains(ordinal: Long): Boolean =
    ordinal >= from && until.forall(ordinal < _)

  def overlaps(other: OrdinalRange): Boolean = {
    val thisStart = from
    val thisEnd = until.getOrElse(Long.MaxValue)
    val otherStart = other.from
    val otherEnd = other.until.getOrElse(Long.MaxValue)
    thisStart < otherEnd && otherStart < thisEnd
  }
}

object OrdinalRange {

  /** Open-ended range covering [from, ∞). */
  def from(start: Long): OrdinalRange = OrdinalRange(start, None)

  /** Closed-open range [from, until). */
  def apply(from: Long, until: Long): OrdinalRange = OrdinalRange(from, Some(until))
}
