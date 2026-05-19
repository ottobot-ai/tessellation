package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, order}
import derevo.derive

/** One entry in the §3 NIPoPoW superblock tower (paper §3.4): a finalized snapshot that hit super-level `µ` at ordinal `ordinal`,
  * identified by `snapshotHash`.
  *
  * Ordered by `(ordinal, level)` so the tower preserves snapshot order; within a single ordinal multiple levels can co-exist.
  *
  * '''Level-0 NOT stored.''' Level-0 hits are implicit (every finalized snapshot is a level-0 hit by chain construction). The
  * tower only persists level-µ hits for µ ∈ {1..L-1}.
  */
@derive(eqv, order)
final case class TowerEntry(
  level: Int,
  ordinal: SnapshotOrdinal,
  snapshotHash: Hash
)
