package io.constellationnetwork.schema.nakamoto

import io.constellationnetwork.schema.nakamoto.slot.{Slot, VrfOutput}
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** Represents a chain tip with all information needed for fork choice.
  *
  * Used by ChainSelection to compare competing chain tips and determine which one to follow. Contains:
  *   - hash: The snapshot hash (unique identifier)
  *   - slot: The slot in which this snapshot was produced
  *   - ordinal: The chain height (longer chains are preferred when weight is equal)
  *   - parentHash: Hash of the parent snapshot (for chain traversal)
  *   - vrfOutput: The VRF output from the slot certificate (for deterministic tiebreaking)
  */
@derive(decoder, encoder, eqv, show)
case class ChainTip(
  hash: Hash,
  slot: Slot,
  ordinal: Long,
  parentHash: Hash,
  vrfOutput: VrfOutput
)
