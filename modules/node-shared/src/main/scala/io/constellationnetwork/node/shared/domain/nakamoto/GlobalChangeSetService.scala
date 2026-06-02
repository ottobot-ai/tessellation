package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Monad
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.nakamoto.follow.GlobalChangeSetResponse

/** gl0-side CHANGESET PRODUCER for the currency-l0 (ml0) adopt-and-verify follow path (task #12, slice 2; see the
  * `project_ml0_diff_adopt_design` notes).
  *
  * ml0 is a FULL-state follower (it persists the whole GSI and serves the hosted data-application), so — unlike the gl1 own-slice follower
  * ([[GlobalFollowSliceService]]) — it cannot reduce to a 5-field projection. Instead it adopts each finalized ordinal's typed
  * [[StateChangesAccumulator]] (the exact delta gl0 applied, INCLUDING the system expiry-index changes a GSI-diff would miss), derives the
  * MPT leaf changes, applies them incrementally (`MerklePatriciaTrie.withChanges`), and matches the recomputed root against the signed
  * `mptRoot` — NO re-execution.
  *
  * This service serves those deltas from a BOUNDED ring of recent FINALIZED accumulators that `SnapshotLeaderLoop` fills at its finalize
  * sinks (parallel to the #287 [[GlobalFollowSliceService]] projection ring). Additive: a pure read of finalized state; it never feeds back
  * into consensus.
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
    */
  def make[F[_]: Monad](
    recentAccumulators: F[SortedMap[SnapshotOrdinal, StateChangesAccumulator]]
  ): GlobalChangeSetService[F] = new GlobalChangeSetService[F] {

    def changeSetSince(since: SnapshotOrdinal): F[Option[GlobalChangeSetResponse]] =
      recentAccumulators.map { ring =>
        ring.lastOption.map {
          case (latestOrdinal, _) =>
            if (since === latestOrdinal)
              GlobalChangeSetResponse(latestOrdinal, since.some, Nil)
            else {
              val deltas = ring.iterator.collect {
                case (o, acc) if o.value.value > since.value.value => (o, acc)
              }.toList
              // Contiguity: the first served delta must be exactly `since + 1`, else `since + 1` was evicted (a gap the
              // follower can't bridge incrementally) → signal a full-GSI fallback.
              val contiguous = deltas.headOption.forall(_._1.value.value == since.value.value + 1L)
              if (contiguous)
                GlobalChangeSetResponse(latestOrdinal, since.some, deltas)
              else
                GlobalChangeSetResponse(latestOrdinal, none, Nil)
            }
        }
      }
  }
}
