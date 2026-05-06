package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.Monoid

import io.constellationnetwork.security.hex.Hex

/** A delta over an MPT base trie: keys to insert/update with their pre-serialized value bytes, plus keys to remove.
  *
  * Used by `MptOverlay` (#56.4) to accumulate per-branch mutations as in-memory diffs over the finalized base. Multiple change sets compose
  * via `merge`: applying `a.merge(b)` is semantically equivalent to applying `a` then `b` to the same base trie.
  *
  * Apply semantics: when this ChangeSet is materialized against a base trie via `MerklePatriciaTrie.withChanges(upserts, removals)`,
  * removals are applied first, then upserts. So if a key appears in both `upserts` and `removals`, the upsert wins. Direct construction may
  * leave such overlap; `merge` strips it under "later wins" semantics.
  *
  * Value bytes use the same canonical form as `MptStore.insertBytes` / `producer.entries` — pre-serialized via `ImmutableCodec`. Keep them
  * here as `Array[Byte]` for direct producer-layer consumption.
  *
  * Note on equality: `Array[Byte]` uses reference equality. `equals`/`hashCode` on this case class are NOT structural over the byte arrays.
  * Tests that compare ChangeSets must compare upserts.mapValues(_.toSeq) explicitly. The overlay never compares ChangeSets directly.
  */
final case class ChangeSet(
  upserts: Map[Hex, Array[Byte]],
  removals: Set[Hex]
) {

  /** Apply `other` on top of this — semantically `(this then other)` against the same base.
    *
    * Conflict resolution ("later wins"):
    *   - A key in `other.removals` cancels a matching `this.upserts` entry (the new state is "removed").
    *   - A key in `other.upserts` cancels a matching `this.removals` entry (the new state is "set to other.value").
    *   - Where both sets upsert the same key, `other`'s value wins.
    *
    * The result has no overlap between `upserts` keys and `removals`.
    */
  def merge(other: ChangeSet): ChangeSet = {
    val newUpserts = (upserts -- other.removals) ++ other.upserts
    val newRemovals = (removals -- other.upserts.keySet) ++ other.removals
    ChangeSet(newUpserts, newRemovals)
  }

  def isEmpty: Boolean = upserts.isEmpty && removals.isEmpty

  /** Total count of mutations — useful for memory-budget telemetry (#56.9). */
  def size: Int = upserts.size + removals.size
}

object ChangeSet {

  val empty: ChangeSet = ChangeSet(Map.empty, Set.empty)

  /** Monoid over `merge`. NOT commutative — `combine(a, b) = a.merge(b)` applies `b` after `a`. */
  implicit val monoid: Monoid[ChangeSet] = new Monoid[ChangeSet] {
    def empty: ChangeSet = ChangeSet.empty
    def combine(a: ChangeSet, b: ChangeSet): ChangeSet = a.merge(b)
  }
}
