package io.constellationnetwork.node.shared.domain.snapshot.storage

import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed

trait LastNGlobalSnapshotStorage[F[_]] extends LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {
  type GlobalFetcher = Either[GlobalL0Service[F], SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]]
  type FetchFunction = (Option[Hash], SnapshotOrdinal) => F[Signed[GlobalIncrementalSnapshot]]

  def getLastN: F[List[Hashed[GlobalIncrementalSnapshot]]]

  /** Look up a retained finalized signed snapshot by its ordinal from the in-memory lastN window. Used by the gl1 inclusion-proof follow
    * (`DAGSnapshotProcessor.applyGlobalSnapshotFn`) to anchor the slice verify against gl1's OWN local copy of the finalized snapshot at
    * the slice's ordinal — the same signed snapshot gl0 projected the slice from — instead of re-pulling it from a peer. `None` when that
    * ordinal is not (yet) in the window (gl1's follow has not reached it, or it has been trimmed out of the lastN window).
    */
  def getByOrdinal(ordinal: SnapshotOrdinal): F[Option[Hashed[GlobalIncrementalSnapshot]]]

  /** Register already-pulled FINALIZED signed snapshots into the by-ordinal lastN index WITHOUT requiring a per-ordinal
    * `GlobalSnapshotInfo`.
    *
    * This is the gl1 inclusion-proof follow's deadlock-breaker (see `GlobalSnapshotAlignment.performSnapshotsBatchProcessing`): gl1's GSI
    * comes from gl0's LATEST-finalized slice (ordinal M), but the follow loop walks the finalized chain per-ordinal, and `set` only
    * advances the by-ordinal index AFTER a successful apply. So while processing the first ordinal of a batch, `getByOrdinal(M)` (M = the
    * batch tip) returns `None` and the slice verify defers forever. Populating the index up-front from the pulled finalized batch lets
    * `getByOrdinal(M)` resolve so the verify succeeds and the walk advances.
    *
    * Only the signed snapshot is stored — `getByOrdinal` (the slice verify's only need: `snapshot.stateProof`) reads ONLY this index, never
    * a per-ordinal state. The `(snapshot, state)` combined pointer (`combinedSnapshotsR`, source of `getLastN` / `getLatestBalances`) is
    * left UNTOUCHED; this method does not advance the chain pointer or the currency-consumed contiguous window. Idempotent by ordinal (a
    * plain map upsert) and order-independent; the index is trimmed to `maxLastGlobalSnapshotsInMemory`. Callers MUST pass only
    * finality-gated snapshots (#122).
    */
  def registerFinalized(snapshots: List[Hashed[GlobalIncrementalSnapshot]]): F[Unit]

  def setForRecovery(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit]
  def clear: F[Unit]
  def setInitialFetchingGL0(
    snapshot: Hashed[GlobalIncrementalSnapshot],
    state: GlobalSnapshotInfo,
    globalSnapshotFetcher: Option[GlobalFetcher],
    fetchGL0Function: Option[FetchFunction]
  ): F[Unit]
}
