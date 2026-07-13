package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.nakamoto.TipAttestation
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.auto._
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Tracks latest validator tip attestations and transitional finality telemetry.
  *
  * The intended GL0 optimistic Phase-2 rail is a K/alpha/beta exact-hash Snowball cascade, not GRANDPA and not a one-round 2/3 vote/QC. The
  * cumulative-weight methods below remain executable legacy debt; the live leader loop still uses them as a state-changing sink.
  *
  * Production continues regardless of finality status. If attestation stalls, builders continue on the longest chain.
  *
  * All weights are exact `Ratio`. Attestation sums and the 2/3 threshold comparison are byte-identical across all JVMs/CPUs — closes the
  * latent finality-split risk that Double summation order would introduce on N-not-power-of-2 clusters (e.g. 7-node cluster where 1/7 isn't
  * exactly representable in IEEE 754).
  *
  * Each accepted attestation is also forwarded into [[SnowballAccumulator]]. Despite its name, that sibling currently records a sticky
  * margin over each peer's latest color; it has no K/alpha query cascade or portable decision evidence and is arrival-order sensitive. Its
  * result is evaluated as `T_weight` telemetry, but the live state-changing sink still calls [[highestFinalizedOrdinal]].
  */
trait TipTracker[F[_]] {

  /** Record an attestation from a peer. The transitional map lets a newer claimed wall-clock supersede an older one.
    *
    * `now` is the receiver's local wall-clock in epoch milliseconds (`Clock[F].realTime.toMillis`) — used to defend against badly-skewed
    * peers (or attackers) submitting attestations with absurd `attestedAt` values. Attestations whose `|attestation.attestedAt - now|`
    * exceeds `TipTracker.MaxAttestationSkewMs` are dropped (with a counter increment + WARN log) so they cannot pollute the `T_count`
    * legacy finality sum. Self-attestations on this node always pass the gate because the emit sites (`SnapshotLeaderLoop.onSlotWon`,
    * `NakamotoSyncDaemon.emitTipAttestation`) source `attestedAt` from the same `Clock[F].realTime` that's threaded through `now` here.
    *
    * Accepted attestations are also forwarded into the sibling [[SnowballAccumulator]]. Because `now` and receipt order are node-local,
    * neither this overwrite rule nor the sibling's sticky margin can be the target consensus decision transcript.
    */
  def recordAttestation(peerId: PeerId, attestation: TipAttestation, now: Long): F[Unit]

  /** Get the current attestation weight for a tip hash. Returns stake fraction [0,1]. */
  def attestationWeight(tipHash: Hash): F[Ratio]

  /** Legacy check for cumulative weight at a tip. This is not the target Phase-2 predicate. */
  def isFinalized(tipHash: Hash): F[Boolean]

  /** Legacy heaviest-attestation diagnostic. GL0 fork choice remains maxvalid-tk/density. */
  def heaviestTip: F[Option[(Hash, Slot, Ratio)]]

  /** Legacy chain-aware cumulative-weight calculation: find the highest ordinal where weight on our current canonical chain reaches the
    * supplied threshold. It is not GRANDPA, a QC, or the target Snowball decision.
    *
    * Attesting to ordinal N with hash H implicitly attests to all ancestors of H. Walk attestation ordinals from highest to lowest,
    * counting weight ONLY for attestations whose tipHash is reachable from our local tip (i.e., `canonicalHashAt(ord) == att.tipHash`).
    *
    * '''Why hash-aware''': attesting to ordinal N on fork A must not add weight for finalizing ordinal N on fork B. The hash-agnostic
    * predecessor silently let forked chains each "finalize" their local fork (observed in a 3-node cluster where gl0-2 forked: all three
    * nodes logged ATTEST-FINALIZED at the same ordinals with weight=0.67, yet their mptRoots at each ordinal were permanently different).
    *
    * This method includes self and depends on each node's locally retained latest-attestation map. The sibling accumulator does not make
    * this calculation safe or portable. The live call from `SnapshotLeaderLoop` is a target violation, not a valid fallback rail.
    *
    * @param threshold
    *   cumulative stake fraction required (e.g. 2/3)
    * @param canonicalHashAt
    *   lookup function that returns the canonical hash at a given ordinal on OUR local best chain (typically
    *   `chainStore.walkBackTo(localTip.hash, ord)`). Attestations whose tipHash doesn't match are discarded from the weight sum.
    */
  def highestFinalizedOrdinal(
    threshold: Ratio,
    canonicalHashAt: Long => F[Option[Hash]]
  ): F[Option[(Long, Ratio)]]

  /** Highest ordinal where the sibling [[SnowballAccumulator]] has a sticky margin decision on our canonical-chain hash.
    *
    * This takes no `selfId`, but the stored result remains arrival-order sensitive and is not the completed target cascade.
    */
  def highestSnowballDecidedOrdinal(canonicalHashAt: Long => F[Option[Hash]]): F[Option[Long]]

  /** Diagnostic / observability — read the sibling Snowball accumulator's per-hash counts at an ordinal. */
  def snowballAccumAt(ordinal: Long): F[Map[Hash, Int]]

  /** Get all current attestations (latest per peer). */
  def allAttestations: F[Map[PeerId, TipAttestation]]

  /** Get the last finalized tip hash and slot. */
  def lastFinalized: F[Option[(Hash, Slot)]]

  /** Mark a tip as finalized. Called when threshold is reached. */
  def markFinalized(tipHash: Hash, tipSlot: Slot): F[Unit]

  /** Clear attestations for tips that are ancestors of a finalized tip. */
  def pruneBelow(finalizedSlot: Slot): F[Unit]

  /** Re-bootstrap escape hatch (P-11, task #141). Clear `attestationsRef` and `finalizedRef` unconditionally — drop ALL peer attestations
    * and the last-finalized marker. Called from the divergent-self-finalize recovery path (`RebootstrapOrchestrator`) when this node has
    * permanently locked itself out of canonical finalization and needs a complete reset.
    *
    * Breaks the monotonicity invariant of `markFinalized` (which only ever sets a finalized tip) — the prefix `unsafe_` signals that
    * callers must hold the right gates (production paused, chain-sync about to fire) before invoking. Do NOT call from the consensus path.
    */
  def unsafe_reset: F[Unit]
}

object TipTracker {

  /** Legacy attestation weight/count threshold retained by compatibility calculators and telemetry.
    *
    * Default: 2/3. This is not a global BFT quorum or the target optimistic P2 predicate: target `T_weight` consumes an exact-hash
    * Avalanche/Snowball decision, while canonical k1 depth is the fallback. Env-var Doubles are locked to `Ratio` at boot.
    *
    * Live legacy weight/count paths still read this value and must be removed or kept observational when the locked FinalityGate lands.
    */
  val FinalityThreshold: Ratio =
    sys.env
      .get("NAKAMOTO_ATTESTATION_THRESHOLD")
      .flatMap(_.toDoubleOption)
      .map(Ratio(_, 18))
      .getOrElse(Ratio(2, 3))

  /** Maximum allowed clock skew (epoch ms) between a peer's claimed `attestedAt` and our local `Clock[F].realTime` when `recordAttestation`
    * runs.
    *
    * Attestations outside `±MaxAttestationSkewMs` are dropped (counter `dag_nakamoto_attestations_rejected_skew_total` + WARN log) so a
    * badly-skewed or malicious peer cannot pollute `T_count` finality (#136). The bound is intentionally generous (default 60s) for today's
    * loosely-coordinated clocks; it can be tightened in production after the Ouroboros-Chronos timestamp-gossip work lands, because
    * `attestedAt` is wall-clock epoch ms (not a consensus slot) precisely to leave that future surface open.
    *
    * Default: 60_000L ms. Override via env `NAKAMOTO_MAX_ATTESTATION_SKEW_MS` (e.g. `30000` for tighter, `120000` for looser). Read once at
    * JVM start so flipping in-flight requires a restart.
    */
  val MaxAttestationSkewMs: Long =
    sys.env
      .get("NAKAMOTO_MAX_ATTESTATION_SKEW_MS")
      .flatMap(_.toLongOption)
      .getOrElse(60000L)

  def make[F[_]: Sync: Metrics](
    stakeRegistry: StakeRegistry[F],
    // Transitional latest-attestation margin (beta). HOCON owns the production wiring value; the intended K/alpha cascade is absent.
    snowballBeta: Int = SnowballAccumulator.Beta
  ): F[TipTracker[F]] =
    SnowballAccumulator.make[F](snowballBeta).flatMap { snowball =>
      makeWithAccumulator(stakeRegistry, snowball)
    }

  /** Variant for tests / callers that want to inject a pre-built [[SnowballAccumulator]] (e.g. with a custom β). */
  def makeWithAccumulator[F[_]: Sync: Metrics](
    stakeRegistry: StakeRegistry[F],
    snowball: SnowballAccumulator[F]
  ): F[TipTracker[F]] =
    for {
      attestationsRef <- Ref.of[F, Map[PeerId, TipAttestation]](Map.empty)
      finalizedRef <- Ref.of[F, Option[(Hash, Slot)]](None)
      logger = Slf4jLogger.getLoggerFromName[F]("TipTracker")
    } yield
      new TipTracker[F] {

        def recordAttestation(peerId: PeerId, attestation: TipAttestation, now: Long): F[Unit] = {
          val skew = math.abs(attestation.attestedAt - now)
          if (skew > MaxAttestationSkewMs)
            // Chronos-prep defensive layer: reject attestations whose claimed wall-clock is more than
            // ±MaxAttestationSkewMs away from our local clock. Without this gate a single peer with a
            // badly-misconfigured system clock (or one actively forging `attestedAt`) would be free to
            // submit attestations that fall in the future / far past, polluting `T_count` finality
            // (#136) and bypassing the eventual timestamp-gossip aggregation. `stakeRegistry.markActive`
            // is intentionally NOT called on rejection — we don't want a skewed peer to count toward
            // the active-quorum fraction either.
            logger.warn(
              s"⚠️ Rejecting attestation from peer=${peerId.value.value.take(16)}... " +
                s"ordinal=${attestation.tipOrdinal} attestedAt=${attestation.attestedAt} " +
                s"now=$now skew=${skew}ms (max=${MaxAttestationSkewMs}ms)"
            ) >> Metrics[F].incrementCounter("dag_nakamoto_attestations_rejected_skew_total")
          else
            attestationsRef.update { current =>
              current.get(peerId) match {
                case Some(existing) if existing.attestedAt >= attestation.attestedAt =>
                  // Existing attestation is same or newer, keep it
                  current
                case _ =>
                  // New or newer attestation, record it
                  current.updated(peerId, attestation)
              }
            } >> stakeRegistry.markActive(peerId) >> // Track this peer as actively participating
              // Forward the same attestation into the transitional margin accumulator. Its
              // `recordAttestation` handles the per-peer at-most-one-per-ordinal invariant internally
              // (if the same peer flips hashes at this ordinal, the prior contribution is moved to the
              // new hash, so this is a latest-color count rather than lifetime confidence). Calling unconditionally on the path that also writes
              // `attestationsRef` keeps both views in lockstep.
              snowball.recordAttestation(peerId, attestation.tipOrdinal, attestation.tipHash)
        }

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
            // This legacy query no longer self-excludes. Including self removes an identity-based
            // asymmetry from one local calculation, but does not make independently received
            // attestation maps or receiver-local active-set denominators equal. The live use of
            // this cumulative sum as an optimistic Phase-2 sink is a target violation.
            onChain <- attestations.toList.traverse[F, Option[(Long, Ratio)]] {
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
            // canonical chain identifies that tip's ancestor prefix. This is ordinary hash-chain ancestry,
            // not a GRANDPA/BFT voting rule.
            var cumWeight: Ratio = Ratio.Zero
            sorted.collectFirst {
              case (ordinal, weight) if { cumWeight = cumWeight + weight; cumWeight >= threshold } =>
                (ordinal, cumWeight)
            }
          }

        def highestSnowballDecidedOrdinal(canonicalHashAt: Long => F[Option[Hash]]): F[Option[Long]] =
          snowball.highestDecidedOnCanonical(canonicalHashAt)

        def snowballAccumAt(ordinal: Long): F[Map[Hash, Int]] =
          snowball.accumAt(ordinal)

        def allAttestations: F[Map[PeerId, TipAttestation]] =
          attestationsRef.get

        def lastFinalized: F[Option[(Hash, Slot)]] =
          finalizedRef.get

        def markFinalized(tipHash: Hash, tipSlot: Slot): F[Unit] =
          finalizedRef.set(Some((tipHash, tipSlot)))

        def pruneBelow(finalizedSlot: Slot): F[Unit] =
          attestationsRef.modify { attestations =>
            val (kept, dropped) = attestations.partition {
              case (_, att) => att.tipSlot.value.value >= finalizedSlot.value.value
            }
            // The ordinal floor used by the current accumulator is the *highest* ordinal among
            // dropped attestations (or, equivalently, the lowest ordinal of kept attestations minus 1).
            // Use the dropped set's max ordinal as the inclusive upper bound — pruneBelow keeps strictly >=.
            // Target exact-hash Phase-2 reorg handling must retain or reconstruct any evidence this drops.
            val pruneOrdFloor: Option[Long] =
              if (dropped.isEmpty) None
              else Some(dropped.values.map(_.tipOrdinal).max + 1L)
            (kept, pruneOrdFloor)
          }.flatMap {
            case Some(floor) => snowball.pruneBelow(floor)
            case None        => Sync[F].unit
          }

        // Re-bootstrap reset (P-11). Wipes attestation map + last-finalized marker so a
        // post-reset chain-resync starts with no prior state. Caller (RebootstrapOrchestrator)
        // pauses production + attestation emit before calling.
        def unsafe_reset: F[Unit] =
          attestationsRef.set(Map.empty) >>
            finalizedRef.set(None) >>
            snowball.unsafe_reset >>
            logger.warn("⚠️ TipTracker.unsafe_reset: cleared attestations + lastFinalized + Snowball accumulator (re-bootstrap)")
      }
}
