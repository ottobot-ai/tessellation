package io.constellationnetwork.schema.nakamoto

import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** A validator's endorsement of a chain tip.
  *
  * Replaces the 4 BFT declaration types (Facility/Proposal/MajoritySignature/BinarySignature) with a simpler Nakamoto-style attestation.
  * Validators broadcast these after verifying a SlotCertificate to endorse the chain tip they consider canonical.
  *
  * GRANDPA-style finality: attestations finalize chains, not individual snapshots. When a tip accumulates ≥ 2/3+1 attestation weight, it
  * and all its ancestors back to the last finalized tip become finalized.
  *
  * TipAttestations are wrapped in Signed[TipAttestation] for transport and verification.
  */
@derive(decoder, encoder, eqv, show)
case class TipAttestation(
  tipHash: Hash, // hash of the endorsed snapshot
  tipSlot: Slot, // slot of that snapshot
  tipOrdinal: Long, // ordinal of that snapshot (Long to avoid circular deps with SnapshotOrdinal)
  attestedAt: Slot // slot when this attestation was created
)
