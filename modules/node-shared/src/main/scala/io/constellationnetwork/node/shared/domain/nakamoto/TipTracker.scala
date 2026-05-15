package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
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
  *
  * All weights are exact `Ratio`. Attestation sums and the 2/3 threshold comparison are byte-identical across all JVMs/CPUs — closes the
  * latent finality-split risk that Double summation order would introduce on N-not-power-of-2 clusters (e.g. 7-node cluster where 1/7 isn't
  * exactly representable in IEEE 754).
  */
trait TipTracker[F[_]] {

  /** Record an attestation from a peer. Newer attestations supersede older ones. */
  def recordAttestation(peerId: PeerId, attestation: TipAttestation): F[Unit]

  /** Get the current attestation weight for a tip hash. Returns stake fraction [0,1]. */
  def attestationWeight(tipHash: Hash): F[Ratio]

  /** Check if a tip has reached finality threshold (≥ 2/3+1 weight). */
  def isFinalized(tipHash: Hash): F[Boolean]

  /** Get the tip with the most attestation weight (fork choice). */
  def heaviestTip: F[Option[(Hash, Slot, Ratio)]]

  /** GRANDPA-style chain finality, chain-aware: find the highest ordinal where cumulative attestation weight on OUR canonical chain >=
    * threshold.
    *
    * Attesting to ordinal N with hash H implicitly attests to all ancestors of H. Walk attestation ordinals from highest to lowest,
    * counting weight ONLY for attestations whose tipHash is reachable from our local tip (i.e., `canonicalHashAt(ord) == att.tipHash`).
    *
    * '''Why hash-aware''': attesting to ordinal N on fork A must not add weight for finalizing ordinal N on fork B. The hash-agnostic
    * predecessor silently let forked chains each "finalize" their local fork (observed in a 3-node cluster where gl0-2 forked: all three
    * nodes logged ATTEST-FINALIZED at the same ordinals with weight=0.67, yet their mptRoots at each ordinal were permanently different).
    *
    * '''Why self-exclusion''' (task #133): a node MUST NOT count its own attestation toward its own finality threshold. Otherwise it can
    * self-finalize a divergent fork, and once `chainStore.finalize` records the local hash, the finality-safety gate (`NakamotoChainStore`)
    * will permanently refuse the canonical chain's hash — the "fork-recovery deadlock" of #119. Under equal-stake `1/N` this is rare (a
    * single attestation is `1/N` of the threshold), but once stake-weighted VRF lands a single high-stake validator could hit the 2/3
    * threshold purely from its own attestation. The producer's contribution is still recorded (other peers' views of this node's
    * attestation count normally — only the self-view filters it out).
    *
    * @param selfId
    *   this node's `PeerId`. Attestations stored under `selfId` are excluded from the weight sum to avoid self-finalization on divergent
    *   forks. Peer (observer) views — e.g. validating someone else's chain — should pass a different identity here, or use a sentinel that
    *   never matches a real peer.
    * @param threshold
    *   cumulative stake fraction required (e.g. 2/3)
    * @param canonicalHashAt
    *   lookup function that returns the canonical hash at a given ordinal on OUR local best chain (typically
    *   `chainStore.walkBackTo(localTip.hash, ord)`). Attestations whose tipHash doesn't match are discarded from the weight sum.
    */
  def highestFinalizedOrdinal(
    selfId: PeerId,
    threshold: Ratio,
    canonicalHashAt: Long => F[Option[Hash]]
  ): F[Option[(Long, Ratio)]]

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
    * conservative finality). Env-var Doubles are locked to `Ratio` at boot.
    *
    * Both the attestation gate (this threshold) and the depth gate (`NAKAMOTO_CONFIRMATION_DEPTH`) always run; whichever fires first
    * finalizes. There is no "mode" — just knobs.
    */
  val FinalityThreshold: Ratio =
    sys.env
      .get("NAKAMOTO_ATTESTATION_THRESHOLD")
      .flatMap(_.toDoubleOption)
      .map(Ratio(_, 18))
      .getOrElse(Ratio(2, 3))

  def make[F[_]: Sync](stakeRegistry: StakeRegistry[F]): F[TipTracker[F]] =
    for {
      attestationsRef <- Ref.of[F, Map[PeerId, TipAttestation]](Map.empty)
      finalizedRef <- Ref.of[F, Option[(Hash, Slot)]](None)
    } yield
      new TipTracker[F] {

        def recordAttestation(peerId: PeerId, attestation: TipAttestation): F[Unit] =
          attestationsRef.update { current =>
            current.get(peerId) match {
              case Some(existing) if existing.attestedAt >= attestation.attestedAt =>
                // Existing attestation is same or newer, keep it
                current
              case _ =>
                // New or newer attestation, record it
                current.updated(peerId, attestation)
            }
          } >> stakeRegistry.markActive(peerId) // Track this peer as actively participating

        def attestationWeight(tipHash: Hash): F[Ratio] =
          for {
            attestations <- attestationsRef.get
            weights <- attestations.toList.traverse {
              case (peerId, att) =>
                if (att.tipHash === tipHash)
                  stakeRegistry.optimisticRelativeStake(peerId) // Use optimistic weight (active peers only)
                else
                  Ratio.Zero.pure[F]
            }
          } yield weights.foldLeft(Ratio.Zero)(_ + _)

        def isFinalized(tipHash: Hash): F[Boolean] =
          attestationWeight(tipHash).map(_ >= FinalityThreshold)

        def heaviestTip: F[Option[(Hash, Slot, Ratio)]] =
          for {
            attestations <- attestationsRef.get
            tipHashes = attestations.values.map(a => (a.tipHash, a.tipSlot)).toSet
            weighted <- tipHashes.toList.traverse {
              case (hash, slot) =>
                attestationWeight(hash).map(w => (hash, slot, w))
            }
          } yield weighted.filter(_._3 > Ratio.Zero).maxByOption { case (_, _, w) => (w.numerator, w.denominator) }

        def highestFinalizedOrdinal(
          selfId: PeerId,
          threshold: Ratio,
          canonicalHashAt: Long => F[Option[Hash]]
        ): F[Option[(Long, Ratio)]] =
          for {
            attestations <- attestationsRef.get
            // Filter each attestation against our canonical chain: only count weight if the peer
            // attested to the hash that's actually on OUR chain at that ordinal. Attestations on
            // other forks (different hash at same ordinal) contribute zero weight to finalizing
            // our chain.
            //
            // Self-exclusion (task #133): drop the entry keyed by `selfId` BEFORE the canonical
            // filter. A node must not count its own attestation toward its own finality threshold,
            // otherwise — combined with `chainStore.finalize`'s finality-safety gate — it can
            // self-finalize a divergent fork and then permanently refuse the canonical chain (the
            // "fork-recovery deadlock" of #119).
            onChain <- attestations.iterator.filter { case (peerId, _) => peerId =!= selfId }.toList.traverse[F, Option[(Long, Ratio)]] {
              case (peerId, att) =>
                canonicalHashAt(att.tipOrdinal).flatMap {
                  case Some(localHash) if localHash === att.tipHash =>
                    stakeRegistry.optimisticRelativeStake(peerId).map(w => Option((att.tipOrdinal, w)))
                  case _ =>
                    Option.empty[(Long, Ratio)].pure[F]
                }
            }
          } yield {
            val sorted = onChain.flatten.filter(_._2 > Ratio.Zero).sortBy(-_._1)
            // Walk down, accumulating weight. Attesting to ordinal N with a hash that's on our
            // canonical chain implies attestation to all ancestors (GRANDPA property) — and since
            // they're all our hashes, no cross-fork contamination.
            var cumWeight: Ratio = Ratio.Zero
            sorted.collectFirst {
              case (ordinal, weight) if { cumWeight = cumWeight + weight; cumWeight >= threshold } =>
                (ordinal, cumWeight)
            }
          }

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
