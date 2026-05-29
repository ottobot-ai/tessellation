package io.constellationnetwork.validator

import cats.Eq
import cats.syntax.eq._

import io.constellationnetwork.schema.GlobalSnapshotStateProof
import io.constellationnetwork.schema.snapshot.StateProof

/** How [[StateProofValidator.validateProof]] decides a recomputed state proof matches the one carried in a signed snapshot.
  *
  * For every state-proof type this is plain structural equality ([[Eq]]). The ONE exception is `GlobalSnapshotStateProof`'s `smtRoot`
  * (the §3 NIPoPoW historical-commitment SMT root): it is NOT reproducible on the proof-build paths `validateProof` uses
  * (`GlobalSnapshotInfo.stateProofBuilder` / `mptStateProofFromBytes`), because those rebuild from `GlobalSnapshotInfo` alone and the
  * accumulated SMT lives in a SEPARATE maintained on-disk store keyed by finalized ordinal — not in the GSI. `smtRoot` is instead
  * populated + cross-checked on the producer/follower-symmetric GSAM accept path (where the maintained store is threaded), so excluding it
  * from the `===` here keeps the download/traverse/sync `validateProof` paths passing for every OTHER (GSI-derived) field while leaving
  * `smtRoot`'s integrity to (a) its inclusion in the signed snapshot and (b) the dedicated chain-replay check at the finalize sink.
  *
  * This mirrors the live follower path's existing precedent (`GlobalSnapshotContextFunctions` compares only the consensus-canonical
  * `mptRoot`, not all 18 legacy per-field roots): the project already runs two comparison strictnesses, and `smtRoot` joins the
  * not-recomputable-here set.
  */
trait StateProofComparison[P <: StateProof] {

  /** True iff `recomputed` matches `claimed` for the purposes of state-proof validation. */
  def equivalent(recomputed: P, claimed: P): Boolean
}

object StateProofComparison extends StateProofComparisonLowPriority {

  def apply[P <: StateProof](implicit ev: StateProofComparison[P]): StateProofComparison[P] = ev

  /** `GlobalSnapshotStateProof` comparison: structural equality with `smtRoot` normalized away on BOTH sides (see trait docstring). Every
    * other field — including `mptRoot` and the per-field subtree roots — is compared exactly.
    */
  implicit val globalSnapshotStateProof: StateProofComparison[GlobalSnapshotStateProof] =
    new StateProofComparison[GlobalSnapshotStateProof] {
      def equivalent(recomputed: GlobalSnapshotStateProof, claimed: GlobalSnapshotStateProof): Boolean =
        recomputed.copy(smtRoot = None) === claimed.copy(smtRoot = None)
    }
}

private[validator] trait StateProofComparisonLowPriority {

  /** Default for any state-proof type: plain structural equality. Lower priority than the `GlobalSnapshotStateProof` instance so the
    * `smtRoot`-aware comparison wins for that type while every other type (e.g. `CurrencySnapshotStateProof`) keeps full `Eq`.
    */
  implicit def fromEq[P <: StateProof: Eq]: StateProofComparison[P] =
    new StateProofComparison[P] {
      def equivalent(recomputed: P, claimed: P): Boolean = recomputed === claimed
    }
}
