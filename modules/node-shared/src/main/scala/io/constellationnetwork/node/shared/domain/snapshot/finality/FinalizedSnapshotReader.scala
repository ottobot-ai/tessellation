package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.http.routes.CachedCombinedResponse
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.{
  CombinedSnapshotCheckpointFileSystemStorage,
  LastCheckpointInfo
}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo}

import org.http4s._
import org.http4s.headers.`Content-Type`

/** How a node exposes the latest *finalized* combined snapshot (signed + info) to external consumers.
  *
  * Two operating modes with very different semantics, split into named factories rather than a runtime flag:
  *
  *   - `bft` — every snapshot is final on arrival (head == finalized). Reads straight from in-memory `SnapshotStorage.head` and
  *     serializes via the shared `CachedCombinedResponse`. No disk indirection, no staleness window.
  *   - `nakamoto` — head may run ahead of finality. Reads from the on-disk checkpoint file at or below the finalized ordinal so that
  *     tentative, pre-finality state never leaves the node. Inherits the `checkpointIntervalEpochs` staleness of
  *     `CombinedSnapshotCheckpointFileSystemStorage`, which is acceptable for finality-gated output.
  *
  * `FinalityGate[F]` handles *ordinal-level* gating (`finalizedOrdinal`, `isServable`) and remains a separate concern — many call sites
  * (e.g. state-channel routes) need only gating and don't depend on `S`/`SI`, so carrying those type parameters on the gate would be noise.
  */
trait FinalizedSnapshotReader[F[_], S <: Snapshot, SI <: SnapshotInfo[_]] {

  /** HTTP response carrying the latest combined snapshot that may safely leave this node under the active finality mode. `None` if no
    * servable snapshot exists yet (e.g. fresh node, or finality hasn't caught up past any written checkpoint).
    */
  def latestCombinedResponse: F[Option[Response[F]]]

  /** Metadata for the latest combined checkpoint servable under finality. BFT derives it from head; Nakamoto returns the tracked on-disk
    * checkpoint info if it's at-or-below finalized.
    */
  def latestCheckpointInfo: F[Option[LastCheckpointInfo]]

  /** Combined snapshot response at a specific ordinal, iff that ordinal is at-or-below finalized. Used by the `/latest/combined/checkpoint/:ordinal`
    * route for consumers that want to pin against a specific checkpoint.
    */
  def combinedCheckpointAt(ordinal: SnapshotOrdinal): F[Option[Response[F]]]
}

object FinalizedSnapshotReader {

  /** BFT implementation: read from in-memory head. Safe because every snapshot is final on arrival in BFT mode, so head IS the finalized
    * state — no need to go to disk.
    */
  def bft[F[_]: Async, S <: Snapshot, SI <: SnapshotInfo[_]](
    snapshotStorage: SnapshotStorage[F, S, SI],
    cached: CachedCombinedResponse[F, S, SI],
    fileStorage: CombinedSnapshotCheckpointFileSystemStorage[F, S, SI]
  ): FinalizedSnapshotReader[F, S, SI] = new FinalizedSnapshotReader[F, S, SI] {

    private def respOf(bytes: Array[Byte]): Response[F] =
      Response[F](Status.Ok)
        .withEntity(bytes)
        .putHeaders(`Content-Type`(MediaType.application.json))

    def latestCombinedResponse: F[Option[Response[F]]] =
      snapshotStorage.head.flatMap {
        case Some((snapshot, info)) =>
          cached.get(snapshot.value.ordinal, snapshot, info).map(bytes => respOf(bytes).some)
        case None => Option.empty[Response[F]].pure[F]
      }

    // Checkpoint info / by-ordinal endpoints are about the on-disk checkpoint files (preserved snapshots consumers can pin to). Even in
    // BFT mode we keep them disk-backed — `/latest/combined` serves fresh from head, but `/checkpoint/*` specifically means "the on-disk
    // reference point". `isServable` is always true in BFT (FinalityGate.passThrough), so no gating.
    def latestCheckpointInfo: F[Option[LastCheckpointInfo]] =
      fileStorage.getLatestCheckpointInfo.map(_.some)

    def combinedCheckpointAt(ordinal: SnapshotOrdinal): F[Option[Response[F]]] =
      fileStorage.getAsHttpResponse(ordinal)
  }

  /** Nakamoto implementation: serve only at-or-below the finalized ordinal, read bytes from the on-disk checkpoint. Tentative (pre-finality)
    * state never leaves via these endpoints; that channel is reserved for the sidecar GossipSub transport.
    */
  def nakamoto[F[_]: Async, S <: Snapshot, SI <: SnapshotInfo[_]](
    finalityGate: FinalityGate[F],
    fileStorage: CombinedSnapshotCheckpointFileSystemStorage[F, S, SI]
  ): FinalizedSnapshotReader[F, S, SI] = new FinalizedSnapshotReader[F, S, SI] {

    def latestCombinedResponse: F[Option[Response[F]]] =
      finalityGate.finalizedOrdinal.flatMap {
        case None => Option.empty[Response[F]].pure[F]
        case Some(finalized) =>
          fileStorage.getAsHttpResponse(finalized).flatMap {
            case Some(resp) => (resp.some: Option[Response[F]]).pure[F]
            // Checkpoint files are written sparsely (every N epochs). If none exists at the exact finalized ordinal, fall back to the
            // most recent checkpoint at-or-below finalized — never a post-finality checkpoint, even if one exists on disk.
            case None => fileStorage.getLatestAsHttpResponseAtOrBelow(finalized)
          }
      }

    def latestCheckpointInfo: F[Option[LastCheckpointInfo]] =
      fileStorage.getLatestCheckpointInfo.flatMap { info =>
        finalityGate.isServable(info.ordinal).map(s => if (s) info.some else None)
      }

    def combinedCheckpointAt(ordinal: SnapshotOrdinal): F[Option[Response[F]]] =
      finalityGate.isServable(ordinal).flatMap {
        case false => Option.empty[Response[F]].pure[F]
        case true  => fileStorage.getAsHttpResponse(ordinal)
      }
  }
}
