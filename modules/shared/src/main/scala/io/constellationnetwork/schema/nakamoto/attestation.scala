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
  *
  * `attestedAt` is wall-clock epoch milliseconds at the time of attestation, sourced via `Clock[F].realTime` (NEVER
  * `System.currentTimeMillis()`). It is **NOT** a consensus slot — wall-clock semantics here are deliberate, to keep the door open for a
  * future Ouroboros Chronos-style time-sync layer that reuses the attestation gossip topic as a timestamp-claim transport. Today it is only
  * used by `TipTracker.recordAttestation` for the "newer wins" rule, which compares Longs.
  */
@derive(decoder, encoder, eqv, show)
case class TipAttestation(
  tipHash: Hash, // hash of the endorsed snapshot
  tipSlot: Slot, // slot of that snapshot
  tipOrdinal: Long, // ordinal of that snapshot (Long to avoid circular deps with SnapshotOrdinal)
  attestedAt: Long // wall-clock epoch ms when this attestation was created (Clock[F].realTime, NOT a slot)
)
