package io.constellationnetwork.schema.nakamoto

import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** A validator's endorsement of a chain tip.
  *
  * GL0 uses this Nakamoto/Snow-family evidence instead of the inherited global BFT declaration lifecycle. A validator may emit one only
  * after authenticating and locally executing the exact snapshot candidate, not merely after receiving a tip or verifying its slot proof.
  *
  * The attestation identifies an exact tip and therefore its ancestor prefix. It is input to the intended K/alpha/beta optimistic Phase-2
  * cascade; it is not itself a BFT vote, lock, quorum certificate, or finality proof. The live cumulative-2/3 sink is transitional protocol
  * debt and must not be inferred from this schema.
  *
  * TipAttestations are wrapped in Signed[TipAttestation] for transport and verification.
  *
  * `attestedAt` is wall-clock epoch milliseconds at the time of attestation, sourced via `Clock[F].realTime` (NEVER
  * `System.currentTimeMillis()`). It is **NOT** a consensus slot — wall-clock semantics here are deliberate, to keep the door open for a
  * future Ouroboros Chronos-style time-sync layer that reuses the attestation gossip topic as a timestamp-claim transport. Today it is used
  * by the transitional `TipTracker.recordAttestation` "newer wins" rule. Receiver-local wall-clock ordering cannot be a target consensus
  * input.
  */
@derive(decoder, encoder, eqv, show)
case class TipAttestation(
  tipHash: Hash, // hash of the endorsed snapshot
  tipSlot: Slot, // slot of that snapshot
  tipOrdinal: Long, // ordinal of that snapshot (Long to avoid circular deps with SnapshotOrdinal)
  attestedAt: Long // wall-clock epoch ms when this attestation was created (Clock[F].realTime, NOT a slot)
)
