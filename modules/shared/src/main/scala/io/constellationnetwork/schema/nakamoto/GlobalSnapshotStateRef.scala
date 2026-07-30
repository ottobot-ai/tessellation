package io.constellationnetwork.schema.nakamoto

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** Claimed structural identity of one global snapshot's canonical MPT state.
  *
  * This value does not authenticate the snapshot, its parent, finality phase, or MPT root. A consumer must establish those facts before
  * using the referenced state for execution, adoption, or recovery. Its canonical Scodec representation requires `parentHash == Hash.empty`
  * exactly when `ordinal == SnapshotOrdinal.MinValue`; the global-chain genesis sentinel is not optional at ordinal zero.
  */
@derive(encoder, decoder, eqv, show)
final case class GlobalSnapshotStateRef(
  ordinal: SnapshotOrdinal,
  hash: Hash,
  parentHash: Hash,
  mptRoot: MptRoot
)
