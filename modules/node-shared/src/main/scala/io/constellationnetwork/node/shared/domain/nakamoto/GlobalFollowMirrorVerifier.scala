package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Parallel
import cats.effect.Async

import scala.collection.immutable.SortedMap

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.GlobalStateFieldId
import io.constellationnetwork.schema.nakamoto.follow._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash

/** gl1-side FIELD-ROOT-MATCH verifier for the own-slice mirror follow path (Axis 2 — see
  * `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`).
  *
  * Verifies gl0's claimed Address-keyed slice against the signed per-field roots by recompute-and-match, delegating the cryptographic check
  * to the pure core [[io.constellationnetwork.schema.nakamoto.follow.FollowVerifyCore.verifyFieldRoots]]. This is the FIELD-ROOT-EQUALITY
  * counterpart to [[GlobalFollowProofService]]'s per-key range-proof prover.
  *
  * '''Stateless over the GSI-sourced slice (own-slice rework, 2026-05-28).''' The producer ([[GlobalFollowSliceService]]) now serves the
  * latest-finalized FULL slice (Address-keyed, delta-from-empty) projected from gl0's `GlobalSnapshotInfo`, so the verifier no longer reads
  * the follower's MPT branch to obtain a prior state — it takes the follower's current verified `ConsumedFieldState` mirror directly
  * (`ConsumedFieldState.empty` for the bootstrap fetch). The verify is a pure forward-hash: apply the Address-keyed delta on top of
  * `prior`, forward-hash each `(Address, value)` to its MPT leaf the way gl0's writer does, recompute each field's subtree root via gl0's
  * exact `GlobalStateConverter.fieldRootFromBytes`, and match against the signed `stateProof.<field>Proof`. No `MptStore` / `MptOverlay`
  * dependency.
  *
  * '''Why field-root equality, not the range proof.''' For a holder of the full content of the five consumed fields, completeness is
  * correct-by-design: a wrong / missing / extra entry yields a different subtree root ⇒ mismatch. The range verifier (S1) cannot catch an
  * omitted entry dropped together with its inclusion proof; root equality catches every divergence. S1's per-key inclusion path is
  * unchanged — it stays the mechanism for cross-shard / light-client consumers that do NOT hold the field content.
  *
  * '''Additive, no wiring.''' This verifier + the shared field-root core + tests only. Nothing here is wired into Main /
  * `GlobalSnapshotAlignment` / `DAGSnapshotProcessor` / gl1 runtime, and `createContext` re-execution is NOT removed — those are later
  * slices.
  */
trait GlobalFollowMirrorVerifier[F[_]] {

  /** Verify gl0's claimed `delta` against the signed per-field roots, on top of the follower's current verified consumed-field state.
    *
    *   - `prior` — the follower's current verified [[ConsumedFieldState]] (Address-keyed) BEFORE this ordinal's writes;
    *     [[ConsumedFieldState.empty]] for the bootstrap (latest-finalized full slice) fetch.
    *   - `ordinal` — the finalized global ordinal being verified. Bookkeeping anchor only (the cryptographic anchor is `signedFieldRoots`).
    *   - `delta` — gl0's claimed Address-keyed slice (from [[GlobalFollowSliceService.latestSlice]]).
    *   - `signedFieldRoots` — the `stateProof.<field>Proof` values from the signed, finality-gated snapshot the follower trusts.
    *
    * Returns [[Verified]]`[`[[ConsumedFieldState]]`]` (the post-delta Address-keyed maps) on full match, else the first
    * [[FollowVerificationError.FieldRootMismatch]].
    */
  def verifyByFieldRoot(
    prior: ConsumedFieldState,
    ordinal: SnapshotOrdinal,
    delta: ConsumedFieldDelta,
    signedFieldRoots: SortedMap[GlobalStateFieldId, Hash]
  ): F[Either[FollowVerificationError, Verified[ConsumedFieldState]]]
}

object GlobalFollowMirrorVerifier {

  def make[F[_]: Async: Parallel: Hasher: JsonSerializer]: GlobalFollowMirrorVerifier[F] = new GlobalFollowMirrorVerifier[F] {

    def verifyByFieldRoot(
      prior: ConsumedFieldState,
      ordinal: SnapshotOrdinal,
      delta: ConsumedFieldDelta,
      signedFieldRoots: SortedMap[GlobalStateFieldId, Hash]
    ): F[Either[FollowVerificationError, Verified[ConsumedFieldState]]] =
      FollowVerifyCore.verifyFieldRoots[F](prior, delta, signedFieldRoots)
  }
}
