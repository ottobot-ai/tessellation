package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.http.routes.CachedCombinedResponse
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.{CombinedSnapshotCheckpointFileSystemStorage, LastCheckpointInfo}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo}
import io.constellationnetwork.security.mpt.storages.MptStateStorage

import fs2.io.file.Path
import fs2.text
import io.circe.syntax._
import io.circe.{Json, parser}
import org.http4s._
import org.http4s.headers.`Content-Type`

/** How a node exposes the latest *finalized* combined snapshot (signed + info) to external consumers.
  *
  * Two operating modes with very different semantics, split into named factories rather than a runtime flag:
  *
  *   - `bft` — every snapshot is final on arrival (head == finalized). Reads straight from in-memory `SnapshotStorage.head` and serializes
  *     via the shared `CachedCombinedResponse`. No disk indirection, no staleness window.
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

  /** 3c-A serve side (`docs/serde/FINISH-3C-EXECUTION-PLAN.md` §3c-A) — the latest finalized combined snapshot PLUS gl0's signed MPT byte
    * map at that same finalized ordinal, as the JSON triple `[ Signed[S], SI, Map[Hex, Array[Byte]] ]`. Finality-gated identically to
    * [[latestCombinedResponse]]: it resolves the SAME servable ordinal (finalized, or the most recent checkpoint at-or-below finalized) and
    * appends the signed byte map read VERBATIM from `MptStateStorage.readState(thatOrdinal)` — the exact bytes gl0 signed, never a
    * re-encode. So a follower that loads the third element via `MptStore.loadBytes` obtains `sidecarFreeMptRoot(entries) === signed
    * mptRoot` BY CONSTRUCTION (the drift the `syncFromGlobalSnapshotInfo` re-encode path exhibited). `None` when no servable combined
    * snapshot exists yet, OR when the signed byte file for the resolved ordinal is absent (e.g. MPT cutoff pruned it) — in both cases the
    * route returns the same not-servable status as `/latest/combined`. GLOBAL-only: implementations without an `MptStateStorage` (BFT /
    * non-global layers) return `None`, leaving their routes byte-identical.
    */
  def latestMptEntriesResponse: F[Option[Response[F]]]

  /** Metadata for the latest combined checkpoint servable under finality. BFT derives it from head; Nakamoto returns the tracked on-disk
    * checkpoint info if it's at-or-below finalized.
    */
  def latestCheckpointInfo: F[Option[LastCheckpointInfo]]

  /** Combined snapshot response at a specific ordinal, iff that ordinal is at-or-below finalized. Used by the
    * `/latest/combined/checkpoint/:ordinal` route for consumers that want to pin against a specific checkpoint.
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

    // 3c-A: serving gl0's signed global MPT byte map is GLOBAL-specific (the byte store is `GlobalStateKey`-keyed). BFT / non-global
    // layers (e.g. currency-l0) have no such store to serve, so this is `None` here and the `mpt-entries` route 404s on those layers —
    // leaving their behavior byte-identical.
    def latestMptEntriesResponse: F[Option[Response[F]]] =
      Option.empty[Response[F]].pure[F]

    // Checkpoint info / by-ordinal endpoints are about the on-disk checkpoint files (preserved snapshots consumers can pin to). Even in
    // BFT mode we keep them disk-backed — `/latest/combined` serves fresh from head, but `/checkpoint/*` specifically means "the on-disk
    // reference point". `isServable` is always true in BFT (FinalityGate.passThrough), so no gating.
    def latestCheckpointInfo: F[Option[LastCheckpointInfo]] =
      fileStorage.getLatestCheckpointInfo.map(_.some)

    def combinedCheckpointAt(ordinal: SnapshotOrdinal): F[Option[Response[F]]] =
      fileStorage.getAsHttpResponse(ordinal)
  }

  /** Nakamoto implementation: serve only at-or-below the finalized ordinal, read bytes from the on-disk checkpoint. Tentative
    * (pre-finality) state never leaves via these endpoints; that channel is reserved for the sidecar GossipSub transport.
    */
  def nakamoto[F[_]: Async, S <: Snapshot, SI <: SnapshotInfo[_]](
    finalityGate: FinalityGate[F],
    fileStorage: CombinedSnapshotCheckpointFileSystemStorage[F, S, SI],
    // 3c-A serve side: the GLOBAL signed MPT byte store, read-only, used ONLY by `latestMptEntriesResponse`. `None` for any non-global
    // Nakamoto reader (then the `mpt-entries` route 404s, identical to BFT). gl0 wiring passes a read-only `MptStateStorage` pointed at the
    // SIGNED byte store (`<mptSnapshotInfoPath>_signed`) — the finalize sink in `GlobalSnapshotConsensus` writes the EXACT bytes `accept()`
    // signed there, so `readState(ordinal)` returns a map that reproduces the signed `mptRoot` by construction (NOT the producer's
    // `mpt_snapshot_info/<ordinal>` re-fold, which can diverge under MultiBranch).
    mptStateStorage: Option[MptStateStorage[F]] = None
  ): FinalizedSnapshotReader[F, S, SI] = new FinalizedSnapshotReader[F, S, SI] {

    // Used only by `latestMptEntriesResponse` (the triple is materialized in-memory, not streamed from a file). The combined/checkpoint
    // endpoints serve `Response[F]` objects straight from `fileStorage`, so they don't need this.
    private def respOf(json: Json): Response[F] =
      Response[F](Status.Ok)
        .withEntity(json.noSpaces)
        .putHeaders(`Content-Type`(MediaType.application.json))

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

    // 3c-A: append the signed MPT byte map to the SAME finalized combined pair `/latest/combined` serves. We resolve the servable
    // checkpoint ordinal with the IDENTICAL fallback (`getLatestOrdinalAtOrBelow(finalized)`), read that checkpoint file's raw JSON
    // (the 2-array `[snapshot, state]` — `fileStorage` holds Encoders only, so we splice the JSON verbatim rather than re-encode S/SI),
    // and read `mptStateStorage.readState(thatSameOrdinal)`. All three elements are pinned to ONE ordinal so the follower's
    // `loadBytes(entries, ord)` → `sidecarFreeMptRoot === snapshot.stateProof.mptRoot` holds by construction. `None` (→ 404, same as the
    // combined path) when no store is wired, no servable checkpoint exists, or the signed byte file for that ordinal is absent.
    def latestMptEntriesResponse: F[Option[Response[F]]] =
      mptStateStorage match {
        case None => Option.empty[Response[F]].pure[F]
        case Some(byteStore) =>
          finalityGate.finalizedOrdinal.flatMap {
            case None => Option.empty[Response[F]].pure[F]
            case Some(finalized) =>
              fileStorage.getLatestOrdinalAtOrBelow(finalized).flatMap {
                case None => Option.empty[Response[F]].pure[F]
                case Some(servableOrdinal) =>
                  (fileStorage.getAsStream(servableOrdinal), byteStore.readState(servableOrdinal)).tupled.flatMap {
                    case (Some(combinedStream), Some(entries)) =>
                      combinedStream
                        .through(text.utf8.decode)
                        .compile
                        .string
                        .flatMap { combinedJson =>
                          Async[F].fromEither(parser.decode[List[Json]](combinedJson)).flatMap {
                            // The combined checkpoint file is the JSON 2-array `[snapshot, state]`. Append the signed entries as the
                            // third element to form the `[snapshot, state, entries]` triple the client (`SnapshotClient.getLatestMptEntries`)
                            // decodes positionally. The entries element uses the shared anchor codec so wire == on-disk byte-form.
                            case snapshotAndState if snapshotAndState.sizeIs == 2 =>
                              val entriesJson: Json = entries.asJson(MptStateStorage.mptEntriesEncoder)
                              val triple = Json.arr((snapshotAndState :+ entriesJson): _*)
                              (respOf(triple).some: Option[Response[F]]).pure[F]
                            case other =>
                              Async[F].raiseError[Option[Response[F]]](
                                new RuntimeException(
                                  s"Unexpected combined checkpoint JSON structure at ordinal=$servableOrdinal: expected a 2-element [snapshot, state] array, got ${other.size} elements"
                                )
                              )
                          }
                        }
                    // Either the combined checkpoint or the signed byte file is missing at the resolved ordinal (e.g. MPT cutoff pruned
                    // the bytes while the checkpoint survives, or vice versa). Fail not-servable — the follower falls back to the legacy
                    // combined path. The verify gate downstream NEVER adopts unverified state, so this is safe.
                    case _ => Option.empty[Response[F]].pure[F]
                  }
              }
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

  /** 3c-A GLOBAL wiring helper. Builds a READ-ONLY `MptStateStorage` at the given `mptSnapshotInfoPath` and constructs the Nakamoto reader
    * with it so `/latest/combined/mpt-entries` can serve the signed byte map. gl0 passes the SIGNED byte store (`<...>_signed`) the
    * finalize sink in `GlobalSnapshotConsensus` writes — so the served bytes reproduce the signed `mptRoot` by construction. Effectful only
    * because `MptStateStorage.make` creates/validates the directory; read-only access to the finality-gated files is race-free with the
    * sink's writes. Use this at the gl0 HTTP wiring site; non-global Nakamoto readers keep using [[nakamoto]] (which defaults
    * `mptStateStorage = None`, so their `mpt-entries` route 404s).
    */
  def nakamotoF[F[_]: Async: JsonSerializer, S <: Snapshot, SI <: SnapshotInfo[_]](
    finalityGate: FinalityGate[F],
    fileStorage: CombinedSnapshotCheckpointFileSystemStorage[F, S, SI],
    mptSnapshotInfoPath: Path
  ): F[FinalizedSnapshotReader[F, S, SI]] =
    MptStateStorage.make[F](mptSnapshotInfoPath).map { byteStore =>
      nakamoto[F, S, SI](finalityGate, fileStorage, byteStore.some)
    }
}
