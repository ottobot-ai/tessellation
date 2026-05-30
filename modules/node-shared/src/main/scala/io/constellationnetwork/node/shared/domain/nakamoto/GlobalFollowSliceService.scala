package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Monad
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.follow.{ConsumedFieldDelta, GlobalFollowSliceResponse}
import io.constellationnetwork.schema.swap.AllowSpendReference
import io.constellationnetwork.schema.tokenLock.TokenLockReference
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}

/** gl0-side SLICE PRODUCER for the gl1 own-slice follow path (Axis 2 — see `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`).
  *
  * gl1 holds ONLY its slice — the five consumed fields ([[io.constellationnetwork.schema.nakamoto.follow.FollowVerifyCore.consumedFields]]:
  * `Balances`, `LastTxRefs`, `LastAllowSpendRefs`, `LastTokenLockRefs`, `ActiveTokenLocks`) — and verifies by recompute-and-match
  * (field-root equality). This service is the gl0 half that PRODUCES the slice a follower applies + recompute-matches against the signed
  * `stateProof.<field>Proof`.
  *
  * '''Sourced from the GSI, not the MPT (own-slice rework, 2026-05-28).''' The slice is projected directly from gl0's latest-FINALIZED
  * `GlobalSnapshotInfo`, captured by `SnapshotLeaderLoop` at its finalize sink (the `StoredSnapshot.context` of the snapshot it just
  * finalized) into a `Ref` this service reads. It must be the FINALIZED GSI, NOT `LastNGlobalSnapshotStorage.getCombined` /
  * `combinedSnapshotsR` (which holds the latest PRODUCED GSI — ahead of the finalized watermark and therefore unresolvable by a
  * finality-gated (#122) gl1 follower, the alignment bug this closes). The five consumed fields are already Address-keyed typed maps there
  * (`gsi.balances: SortedMap[Address, Balance]`, `gsi.lastTxRefs`, `gsi.lastAllowSpendRefs`, `gsi.lastTokenLockRefs`,
  * `gsi.getActiveTokenLocks`), so the producer hands them back unchanged. This drops the prior Hex-MPT read path entirely: the follower's
  * verifier forward-hashes each `(Address, value)` to its MPT leaf (`toHex(hypergraph(field, address))` + `immutableBytes(value)`) to
  * recompute the field roots, so byte-identity with gl0's leaves is reproduced on the verify side, not extracted from the store here.
  * Address-keyed keeps gl1's downstream (`TransactionService` / `Collateral` / `CollateralDaemon` / `mptStore.syncFromGlobalSnapshotInfo`)
  * unchanged.
  *
  * '''Latest-finalized FULL slice + opt-in diff''' (locked decision 1, "Transfer model"; #287 "send diffs"): the `MptOverlay` cannot read
  * value-bytes at an arbitrary historical ordinal, so per-ordinal historical deltas aren't available — and gl1 only needs the latest
  * finalized state for tx validation. So [[latestSlice]] serves the latest-finalized full slice as a delta-from-empty (every entry an
  * upsert, no removals) and the follower recompute-matches it — the bootstrap / fallback path. As the #287 optimization, the producer also
  * retains a BOUNDED ring of recent finalized 5-field projections (keyed by ordinal) and [[sliceSince]] serves the INCREMENTAL diff between
  * the follower's last-verified tip and the latest finalized ordinal (`ConsumedFieldDelta.diff`), transferring O(changes) instead of
  * O(slice). A diff is only an optimization: a follower that holds the stated base reaches the latest state by applying it, and field-root
  * equality on the verify side rejects any wrong/stale base — so a miss falls back to the full slice with no correctness risk.
  *
  * '''Additive, no wiring change to consensus state.''' The slice is a pure projection of finalized state read through the injected GSI
  * thunk + projection ring; it never feeds back into consensus.
  */
trait GlobalFollowSliceService[F[_]] {

  /** The full consumed-field slice at the latest finalized global ordinal, as a [[ConsumedFieldDelta]] of all-upserts (removals empty). A
    * gl1 follower applies it on top of its current verified mirror (empty on bootstrap) and recompute-matches each field root against the
    * matching signed snapshot's `stateProof.<field>Proof`. `None` when no global ordinal has finalized yet (cold start). This is the
    * bootstrap / fallback transport — `baseOrdinal` is always `None` in the response built from it.
    */
  def latestSlice: F[Option[(SnapshotOrdinal, ConsumedFieldDelta)]]

  /** The #287 incremental transport: serve the change-set a follower holding the consumed-field state AT `since` needs to reach the latest
    * finalized ordinal `L`. Reads the producer's bounded projection ring:
    *   - latest projection absent (cold start) ⇒ `None`;
    *   - `since == L` ⇒ a NO-CHANGE response (empty delta, `baseOrdinal = Some(since)`, `ordinal = L`);
    *   - ring holds projection@`since` ⇒ `ConsumedFieldDelta.diff(proj@since, proj@L)`, `baseOrdinal = Some(since)`, `ordinal = L`;
    *   - otherwise (`since` evicted / never seen) ⇒ the FULL `proj@L`, `baseOrdinal = None`, `ordinal = L` (the follower falls back to a
    *     from-empty apply).
    *
    * The cryptographic anchor is always the signed snapshot at `ordinal = L`; `baseOrdinal` only tells the follower which prior state to
    * apply the delta on, and a wrong base surfaces as a field-root mismatch, never as silently-advanced state.
    */
  def sliceSince(since: SnapshotOrdinal): F[Option[GlobalFollowSliceResponse]]
}

object GlobalFollowSliceService {

  /** Bound on the producer's recent-projection ring used by [[sliceSince]] (#287). A follower that fell more than `recentProjectionsToKeep`
    * finalized ordinals behind its last-verified tip simply re-fetches the full slice ([[ConsumedFieldDelta]] from-empty) — the diff is a
    * pure optimization, so this is a memory bound, not a consensus parameter. Sized to the depth-k finality window (a follower polling
    * every ~10s catches up well within this many ordinals). Named constant, not `sys.env` (per `[[feedback-prefer-hocon-over-sysenv]]`); a
    * config field can replace it if the ring ever needs to be operator-tunable.
    */
  val recentProjectionsToKeep: Int = 256

  /** @param latestFinalized
    *   thunk yielding the latest-FINALIZED `(SnapshotOrdinal, GlobalSnapshotInfo)` — production reads the Ref `SnapshotLeaderLoop` updates
    *   at its finalize sink (`latestFinalizedSliceSourceRef.get`). This is the finalized GSI, NOT the latest-produced
    *   `LastNGlobalSnapshotStorage.getCombined`: a finality-gated (#122) gl1 follower can only resolve a snapshot at the slice's ordinal if
    *   that ordinal is finalized. `None` while nothing has finalized (cold start).
    * @param recentProjections
    *   thunk yielding the producer's bounded ring of recent finalized 5-field projections keyed by ordinal — production reads the Ref
    *   `SnapshotLeaderLoop` updates at the SAME finalize sinks (storing [[sliceFromGsi]] of each finalized GSI, trimmed to the last
    *   [[recentProjectionsToKeep]]). Used by [[sliceSince]] to compute the incremental diff (#287). Empty until the first finalize.
    */
  def make[F[_]: Monad](
    latestFinalized: F[Option[(SnapshotOrdinal, GlobalSnapshotInfo)]],
    recentProjections: F[SortedMap[SnapshotOrdinal, ConsumedFieldDelta]]
  ): GlobalFollowSliceService[F] = new GlobalFollowSliceService[F] {

    def latestSlice: F[Option[(SnapshotOrdinal, ConsumedFieldDelta)]] =
      latestFinalized.map(_.map { case (ordinal, gsi) => ordinal -> sliceFromGsi(gsi) })

    def sliceSince(since: SnapshotOrdinal): F[Option[GlobalFollowSliceResponse]] =
      recentProjections.map { ring =>
        ring.lastOption.map {
          case (latestOrdinal, latestProj) =>
            if (since === latestOrdinal)
              // Caller already holds the latest finalized state — nothing to transfer, but still anchor the
              // (empty) verify at `latestOrdinal` so a steady-state follower re-confirms its tip cheaply.
              GlobalFollowSliceResponse(latestOrdinal, ConsumedFieldDelta.empty, since.some)
            else
              ring.get(since) match {
                case Some(sinceProj) =>
                  GlobalFollowSliceResponse(latestOrdinal, ConsumedFieldDelta.diff(sinceProj, latestProj), since.some)
                case None =>
                  // `since` evicted from the ring (or never seen) — fall back to the full from-empty projection.
                  GlobalFollowSliceResponse(latestOrdinal, latestProj, none)
              }
        }
      }
  }

  /** Project a finalized [[GlobalSnapshotInfo]] onto the five consumed fields as a delta-from-empty: every Address-keyed entry an upsert,
    * no removals. The `lastAllowSpendRefs` / `lastTokenLockRefs` / `activeTokenLocks` fields are `Option`-typed on the GSI (absent ⇒
    * empty); `balances` / `lastTxRefs` are always present. `activeTokenLocks` is sourced via `gsi.getActiveTokenLocks` (the same
    * `getOrElse(empty)` accessor `TokenLockService` reads), so the slice carries exactly what gl1's token-lock-replacement validator
    * consumes. Pure; public so tests exercise it directly, the F-bound `make` stays a thin GSI-read shell over it, AND the gl0
    * `SnapshotLeaderLoop` finalize sinks can project each finalized GSI into the #287 recent-projection ring (a different `nakamoto`
    * package, so a `private[nakamoto]` qualifier would not reach it).
    */
  def sliceFromGsi(gsi: GlobalSnapshotInfo): ConsumedFieldDelta =
    ConsumedFieldDelta(
      balances = gsi.balances,
      lastTxRefs = gsi.lastTxRefs,
      lastAllowSpendRefs = gsi.lastAllowSpendRefs.getOrElse(SortedMap.empty[Address, AllowSpendReference]),
      lastTokenLockRefs = gsi.lastTokenLockRefs.getOrElse(SortedMap.empty[Address, TokenLockReference]),
      activeTokenLocks = gsi.getActiveTokenLocks,
      removals = SortedMap.empty
    )
}
