package io.constellationnetwork.schema.nakamoto

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot

/** Claimed structural identity of one global snapshot's canonical MPT state.
  *
  * This value does not authenticate the snapshot, its parent, finality phase, or
  * MPT root. A consumer must establish those facts before using the referenced
  * state for execution, adoption, or recovery.
  */
final case class GlobalSnapshotStateRef(
  ordinal: SnapshotOrdinal,
  hash: Hash,
  parentHash: Hash,
  mptRoot: MptRoot
)
