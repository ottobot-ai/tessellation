package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Monad
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.nipopow.HistoricalCommitmentSmtStore
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.nakamoto.follow.{GlobalChangeSetDelta, GlobalChangeSetResponse}
import io.constellationnetwork.security.smt.SmtProof

import eu.timepit.refined.types.numeric.NonNegLong

/** gl0-side CHANGESET PRODUCER for the currency-l0 (ml0) adopt-and-verify follow path (task #12, slice 2; see the
  * `project_ml0_diff_adopt_design` notes). SMT proofs added in Slice B.
  *
  * ml0 is a FULL-state follower (it persists the whole GSI and serves the hosted data-application), so — unlike the gl1 own-slice follower
  * ([[GlobalFollowSliceService]]) — it cannot reduce to a 5-field projection. Instead it adopts each finalized ordinal's typed
  * [[StateChangesAccumulator]] (the exact delta gl0 applied, INCLUDING the system expiry-index changes a GSI-diff would miss), derives the
  * MPT leaf changes, applies them incrementally (`MerklePatriciaTrie.withChanges`), and matches the recomputed root against the signed
  * `mptRoot` — NO re-execution.
  *
  * This service serves those deltas from a BOUNDED ring of recent FINALIZED accumulators that `SnapshotLeaderLoop` fills at its finalize
  * sinks (parallel to the #287 [[GlobalFollowSliceService]] projection ring). Each served delta additionally carries the §3-NIPoPoW staged
  * historical-commitment SMT proofs ([[HistoricalCommitmentSmtStore.proveAt]]). The current active era requires signed `smtRoot = None`, so
  * these proof fields are not consensus authority and cannot activate the tower commitment. Additive: a pure read of finalized state; it
  * never feeds back into consensus.
  */
trait GlobalChangeSetService[F[_]] {

  /** The contiguous per-ordinal accumulators a follower holding finalized state AT `since` needs to reach the latest finalized ordinal.
    * Reads the producer's bounded accumulator ring:
    *   - ring empty (cold start) ⇒ `None`;
    *   - `since == latest` ⇒ no-op (`deltas = Nil`, `baseOrdinal = Some(since)`);
    *   - ring contiguously covers `(since, latest]` ⇒ those deltas in order, `baseOrdinal = Some(since)`;
    *   - `since` older than the ring (gap) ⇒ `deltas = Nil`, `baseOrdinal = None` (follower falls back to a full-GSI adopt).
    *
    * The cryptographic anchor is the signed `mptRoot` at each delta's ordinal; `baseOrdinal` only states which prior state to apply on, and
    * a wrong base surfaces as a root mismatch on the verify side, never as silently-advanced state.
    */
  def changeSetSince(since: SnapshotOrdinal): F[Option[GlobalChangeSetResponse]]
}

object GlobalChangeSetService {

  /** Bound on the producer's recent-accumulator ring. A follower more than this many finalized ordinals behind re-fetches via a full-GSI
    * adopt — the per-ordinal deltas are an optimization, so this is a memory bound, not a consensus parameter. Sized like the #287
    * projection ring ([[GlobalFollowSliceService.recentProjectionsToKeep]]).
    */
  val recentAccumulatorsToKeep: Int = 256

  /** @param recentAccumulators
    *   thunk yielding the producer's bounded ring of recent FINALIZED per-ordinal accumulators keyed by ordinal — production reads the Ref
    *   `SnapshotLeaderLoop` updates at its finalize sinks. Empty until the first finalize.
    * @param historicalCommitmentSmtStore
    *   staged GL0 NIPoPoW historical-commitment SMT store. `None` produces no SMT proof fields. Current active-era adoption is always
    *   mptRoot-bound and rejects a populated signed `smtRoot`; these optional proofs cannot weaken or replace that check.
    * @param confirmationDepthK
    *   the confirmation-depth cutoff `k`: the eligible ordinal a proof targets is `ordinal − k` (the SAME cutoff `accept()` uses to anchor
    *   `smtRoot(ordinal)`). For `ordinal ≤ k` no `smtRoot` exists (genesis/warmup) ⇒ both proofs `None`.
    */
  def make[F[_]: Monad](
    recentAccumulators: F[SortedMap[SnapshotOrdinal, StateChangesAccumulator]],
    historicalCommitmentSmtStore: Option[HistoricalCommitmentSmtStore[F]],
    confirmationDepthK: Long
  ): GlobalChangeSetService[F] = new GlobalChangeSetService[F] {

    /** Build the two SMT proofs for one served ordinal. `None` for both when no store is wired or the snapshot is in the warmup window
      * (`ordinal ≤ k`, no `smtRoot`). A `proveAt` `Left` (e.g. the eligible ordinal's root fell out of retention) maps to `None` for that
      * proof — the follower degrades that ordinal to mptRoot-only. Never fails the changeset.
      */
    private def proofsFor(ordinal: SnapshotOrdinal): F[(Option[SmtProof], Option[SmtProof])] =
      historicalCommitmentSmtStore match {
        case None => (none[SmtProof], none[SmtProof]).pure[F]
        case Some(store) =>
          val ordL = ordinal.value.value
          // eligible = ordinal − k, parent = ordinal − 1; both are well-defined (≥ 0) iff ordinal > k.
          (NonNegLong.from(ordL - confirmationDepthK).toOption, NonNegLong.from(ordL - 1L).toOption)
            .mapN((eligible, parent) => (SnapshotOrdinal(eligible), SnapshotOrdinal(parent))) match {
            case None if ordL > confirmationDepthK =>
              // Shouldn't happen (ordinal > k ⇒ both refinements succeed); be safe and degrade.
              (none[SmtProof], none[SmtProof]).pure[F]
            case None =>
              // Warmup window: ordinal ≤ k ⇒ no smtRoot anchored ⇒ no proof to build.
              (none[SmtProof], none[SmtProof]).pure[F]
            case Some((eligibleOrd, parentOrd)) =>
              for {
                inclusion <- store.proveAt(ordinal, eligibleOrd).map(_.toOption)
                // Absence-at-parent: prove the eligible commitment against smtRoot(ordinal − 1). Populated for the FUTURE
                // transition-verification variant; ml0 ignores it this slice. Degrade to None on any Left.
                absenceAtParent <- store.proveAt(parentOrd, eligibleOrd).map(_.toOption)
              } yield (inclusion, absenceAtParent)
          }
      }

    def changeSetSince(since: SnapshotOrdinal): F[Option[GlobalChangeSetResponse]] =
      recentAccumulators.flatMap { ring =>
        ring.lastOption match {
          case None => none[GlobalChangeSetResponse].pure[F]
          case Some((latestOrdinal, _)) =>
            if (since === latestOrdinal)
              GlobalChangeSetResponse(latestOrdinal, since.some, Nil).some.pure[F]
            else {
              val rawDeltas = ring.iterator.collect {
                case (o, acc) if o.value.value > since.value.value => (o, acc)
              }.toList
              // Contiguity: the first served delta must be exactly `since + 1`, else `since + 1` was evicted (a gap the
              // follower can't bridge incrementally) → signal a full-GSI fallback.
              val contiguous = rawDeltas.headOption.forall(_._1.value.value == since.value.value + 1L)
              if (contiguous)
                rawDeltas.traverse {
                  case (o, acc) =>
                    proofsFor(o).map { case (incl, abs) => GlobalChangeSetDelta(o, acc, incl, abs) }
                }.map(deltas => GlobalChangeSetResponse(latestOrdinal, since.some, deltas).some)
              else
                GlobalChangeSetResponse(latestOrdinal, none, Nil).some.pure[F]
            }
        }
      }
  }
}
