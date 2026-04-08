package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.schema.nakamoto.TipAttestation
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash

/** Tracks attestations from validators and determines finality.
  *
  * GRANDPA-inspired: attestations finalize chains, not individual snapshots. Each peer's latest attestation supersedes their previous
  * (PeerRumor ordering). When a tip accumulates ≥ 2/3+1 of total stake weight, it's finalized along with all ancestors.
  *
  * Production continues regardless of finality status. If attestation stalls, builders continue on the longest chain.
  */
trait TipTracker[F[_]] {

  /** Record an attestation from a peer. Newer attestations supersede older ones. */
  def recordAttestation(peerId: PeerId, attestation: TipAttestation): F[Unit]

  /** Get the current attestation weight for a tip hash. Returns stake fraction [0,1]. */
  def attestationWeight(tipHash: Hash): F[Double]

  /** Check if a tip has reached finality threshold (≥ 2/3+1 weight). */
  def isFinalized(tipHash: Hash): F[Boolean]

  /** Get the tip with the most attestation weight (fork choice). */
  def heaviestTip: F[Option[(Hash, Slot, Double)]]

  /** Get all current attestations (latest per peer). */
  def allAttestations: F[Map[PeerId, TipAttestation]]

  /** Get the last finalized tip hash and slot. */
  def lastFinalized: F[Option[(Hash, Slot)]]

  /** Mark a tip as finalized. Called when threshold is reached. */
  def markFinalized(tipHash: Hash, tipSlot: Slot): F[Unit]

  /** Clear attestations for tips that are ancestors of a finalized tip. */
  def pruneBelow(finalizedSlot: Slot): F[Unit]
}

object TipTracker {

  /** Attestation finality threshold — fraction of total stake that must attest to a tip for it to finalize.
    *
    * Default: 2/3 (BFT-classic). Override via `NAKAMOTO_ATTESTATION_THRESHOLD` (e.g. `0.5` for half-honest small clusters, `0.8` for more
    * conservative finality).
    *
    * Both the attestation gate (this threshold) and the depth gate (`NAKAMOTO_CONFIRMATION_DEPTH`) always run; whichever fires first
    * finalizes. There is no "mode" — just knobs.
    */
  val FinalityThreshold: Double =
    sys.env.get("NAKAMOTO_ATTESTATION_THRESHOLD").flatMap(_.toDoubleOption).getOrElse(2.0 / 3.0)

  def make[F[_]: Sync](stakeRegistry: StakeRegistry[F]): F[TipTracker[F]] =
    for {
      attestationsRef <- Ref.of[F, Map[PeerId, TipAttestation]](Map.empty)
      finalizedRef <- Ref.of[F, Option[(Hash, Slot)]](None)
    } yield
      new TipTracker[F] {

        def recordAttestation(peerId: PeerId, attestation: TipAttestation): F[Unit] =
          attestationsRef.update { current =>
            current.get(peerId) match {
              case Some(existing) if existing.attestedAt.value.value >= attestation.attestedAt.value.value =>
                // Existing attestation is same or newer, keep it
                current
              case _ =>
                // New or newer attestation, record it
                current.updated(peerId, attestation)
            }
          } >> stakeRegistry.markActive(peerId) // Track this peer as actively participating

        def attestationWeight(tipHash: Hash): F[Double] =
          for {
            attestations <- attestationsRef.get
            weights <- attestations.toList.traverse {
              case (peerId, att) =>
                if (att.tipHash === tipHash)
                  stakeRegistry.optimisticRelativeStake(peerId) // Use optimistic weight (active peers only)
                else
                  0.0.pure[F]
            }
          } yield weights.sum

        def isFinalized(tipHash: Hash): F[Boolean] =
          attestationWeight(tipHash).map(_ >= FinalityThreshold)

        def heaviestTip: F[Option[(Hash, Slot, Double)]] =
          for {
            attestations <- attestationsRef.get
            tipHashes = attestations.values.map(a => (a.tipHash, a.tipSlot)).toSet
            weighted <- tipHashes.toList.traverse {
              case (hash, slot) =>
                attestationWeight(hash).map(w => (hash, slot, w))
            }
          } yield weighted.maxByOption(_._3).filter(_._3 > 0.0)

        def allAttestations: F[Map[PeerId, TipAttestation]] =
          attestationsRef.get

        def lastFinalized: F[Option[(Hash, Slot)]] =
          finalizedRef.get

        def markFinalized(tipHash: Hash, tipSlot: Slot): F[Unit] =
          finalizedRef.set(Some((tipHash, tipSlot)))

        def pruneBelow(finalizedSlot: Slot): F[Unit] =
          attestationsRef.update { attestations =>
            attestations.filter {
              case (_, att) =>
                att.tipSlot.value.value >= finalizedSlot.value.value
            }
          }
      }
}
