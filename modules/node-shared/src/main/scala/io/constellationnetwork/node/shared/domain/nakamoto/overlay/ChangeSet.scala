package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.Monoid

import io.constellationnetwork.security.hex.Hex

/** A delta over an MPT base trie: keys to insert/update with their pre-serialized value bytes, plus keys to remove.
  *
  * Used by `MptOverlay` to accumulate per-branch mutations over the finalized base. Applying `a.merge(b)` is equivalent to applying `a` and
  * then `b` to the same base. Removals are materialized before upserts, so an upsert wins if a key appears in both sets.
  */
final case class ChangeSet(
  upserts: Map[Hex, Array[Byte]],
  removals: Set[Hex]
) {

  /** Compose `other` after this change set. Later mutations win. */
  def merge(other: ChangeSet): ChangeSet = {
    val newUpserts = (upserts -- other.removals) ++ other.upserts
    val newRemovals = (removals -- other.upserts.keySet) ++ other.removals
    ChangeSet(newUpserts, newRemovals)
  }

  def isEmpty: Boolean = upserts.isEmpty && removals.isEmpty

  def size: Int = upserts.size + removals.size
}

object ChangeSet {
  val empty: ChangeSet = ChangeSet(Map.empty, Set.empty)

  /** Monoid over `merge`. It is intentionally non-commutative. */
  implicit val monoid: Monoid[ChangeSet] = new Monoid[ChangeSet] {
    def empty: ChangeSet = ChangeSet.empty
    def combine(a: ChangeSet, b: ChangeSet): ChangeSet = a.merge(b)
  }
}
