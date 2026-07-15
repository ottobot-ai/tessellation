package io.constellationnetwork.node.shared.domain.snapshot.finality

import java.nio.charset.StandardCharsets.UTF_8

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.http.routes.CachedCombinedResponse
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.{CombinedSnapshotCheckpointFileSystemStorage, LastCheckpointInfo}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.storages.MptStateStorage
import io.constellationnetwork.security.signature.Signed

import fs2.io.file.Path
import fs2.{Pipe, Stream}
import io.circe.jawn.CirceSupportParser
import io.circe.syntax._
import io.circe.{Decoder, Json}
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.typelevel.jawn.Facade
import org.typelevel.jawn.fs2._

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
    * appends the claimed signed byte map read VERBATIM from `MptStateStorage.readState(thatOrdinal)`, never a re-encode. A follower that
    * loads the third element via `MptStore.loadBytes` must recompute the consensus root and compare it with the authenticated snapshot's
    * signed root. Byte fidelity prevents codec drift but is not authentication. `None` when no servable combined snapshot exists yet, OR
    * when the signed byte file for the resolved ordinal is absent (e.g. MPT cutoff pruned it) — in both cases the route returns the same
    * not-servable status as `/latest/combined`. GLOBAL-only: implementations without an `MptStateStorage` (BFT / non-global layers) return
    * `None`, leaving their routes byte-identical.
    */
  def latestMptEntriesResponse: F[Option[Response[F]]]

  /** Signed-byte-store backfill serve side (2026-07-09) — gl0's signed MPT byte map at an EXACT `ordinal`, iff that ordinal is at-or-below
    * finalized AND the signed store holds bytes there. The by-ordinal sibling of [[latestMptEntriesResponse]] serving ONLY the entries map
    * (no snapshot/GSI: the puller verifies against its OWN locally-committed `stateProof.mptRoot` at that ordinal — see
    * `PinnedCurrencyInfoReader.PinnedByteBackfill`). The finality gate limits serving to eligible ordinals; the puller's root comparison
    * authenticates the returned bytes and catches corrupt, stale, or mis-associated store content. `None` (→ 404) on a hole / pruned
    * ordinal / non-global layer (BFT + readers without an `MptStateStorage`), leaving those routes byte-identical.
    */
  def mptEntriesAt(ordinal: SnapshotOrdinal): F[Option[Response[F]]]

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

    // Same reasoning as `latestMptEntriesResponse`: no signed byte store on BFT / non-global layers ⇒ the by-ordinal route 404s.
    def mptEntriesAt(ordinal: SnapshotOrdinal): F[Option[Response[F]]] =
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
  def nakamoto[F[_]: Async, S <: Snapshot: Decoder, SI <: SnapshotInfo[_]](
    finalityGate: FinalityGate[F],
    fileStorage: CombinedSnapshotCheckpointFileSystemStorage[F, S, SI],
    activeEraSnapshotAt: SnapshotOrdinal => F[Boolean],
    // 3c-A serve side: the GLOBAL signed MPT byte store, read-only, used ONLY by `latestMptEntriesResponse`. `None` for any non-global
    // Nakamoto reader (then the `mpt-entries` route 404s, identical to BFT). gl0 wiring passes a read-only `MptStateStorage` pointed at the
    // SIGNED byte store (`<mptSnapshotInfoPath>_signed`) — the finalize sink in `GlobalSnapshotConsensus` writes the bytes used by
    // `accept()` there. Consumers still authenticate the snapshot and compare its signed `mptRoot` with a root recomputed from
    // `readState(ordinal)` (unlike the producer's `mpt_snapshot_info/<ordinal>` re-fold, which can diverge under MultiBranch).
    mptStateStorage: Option[MptStateStorage[F]] = None
  ): FinalizedSnapshotReader[F, S, SI] = new FinalizedSnapshotReader[F, S, SI] {

    implicit private val circeFacade: Facade[Json] = new CirceSupportParser(None, false).facade

    // Byte-only responses remain materialized from the signed byte-store map; combined checkpoint bodies stream from one validated handle.
    private def respOf(json: Json): Response[F] =
      Response[F](Status.Ok)
        .withEntity(json.noSpaces)
        .putHeaders(`Content-Type`(MediaType.application.json))

    private def hasExactPairShape(bytes: Stream[F, Byte]): F[Boolean] = {
      implicit val discardFacade: Facade[Unit] = Facade.NullFacade
      bytes.chunks.unwrapJsonArray[Unit].compile.count.map(_ === 2L)
    }

    /** Validate a combined checkpoint without materializing the full file or state-info JSON. The first pass checks the entire JSON array
      * with Jawn's discarding facade. The second pass materializes only the signed snapshot element; its size is the remaining per-request
      * decode bound until the protocol ratifies a signed-artifact byte limit. Requiring `]` as the final byte preserves the storage
      * writer's canonical compact shape and makes the streaming MPT-triple extension unambiguous.
      */
    private def validateActiveCombined(ordinal: SnapshotOrdinal)(bytes: Stream[F, Byte]): F[Boolean] =
      (for {
        exactPair <- hasExactPairShape(bytes)
        finalByte <- bytes.takeRight(1).compile.last
        signed <- bytes.chunks.unwrapJsonArray[Json].take(1).compile.lastOrError.flatMap(_.as[Signed[S]].liftTo[F])
      } yield
        exactPair && finalByte.contains(']'.toByte) && signed.value.ordinal === ordinal && (signed.value match {
          case global: io.constellationnetwork.schema.GlobalIncrementalSnapshot =>
            io.constellationnetwork.validator.GlobalSnapshotActiveEraValidator.validate(global).isRight
          case _ => true
        })).handleError(_ => false)

    private def combinedAtIfActive(ordinal: SnapshotOrdinal): F[Option[Response[F]]] =
      fileStorage.getAsValidatedHttpResponse(ordinal, validateActiveCombined(ordinal))

    def latestCombinedResponse: F[Option[Response[F]]] =
      finalityGate.finalizedOrdinal.flatMap {
        case None => Option.empty[Response[F]].pure[F]
        case Some(finalized) =>
          fileStorage.exists(finalized).flatMap {
            // An exact checkpoint that violates the active-era rule is not equivalent to a sparse-store miss. Fail closed rather than
            // silently masking it by serving an older checkpoint.
            case true => combinedAtIfActive(finalized)
            // Checkpoint files are written sparsely (every N epochs). Only an actual absence may fall back to the most recent checkpoint
            // at-or-below finalized — never a post-finality checkpoint, even if one exists on disk.
            case false =>
              fileStorage.getLatestOrdinalAtOrBelow(finalized).flatMap {
                case Some(ordinal) => combinedAtIfActive(ordinal)
                case None          => Option.empty[Response[F]].pure[F]
              }
          }
      }

    // 3c-A: append the signed MPT byte map to the SAME open finalized combined file `/latest/combined` validates. The canonical combined
    // writer ends its compact two-element array at the final byte, so dropping that `]` and appending the third element is streaming and
    // does not materialize the potentially large state-info JSON.
    private def appendEntries(entries: Map[Hex, Array[Byte]]): Pipe[F, Byte, Byte] = { combined =>
      val entriesJson: Json = entries.asJson(MptStateStorage.mptEntriesEncoder)
      combined.dropRight(1) ++ Stream.emits(s",${entriesJson.noSpaces}]".getBytes(UTF_8)).covary[F]
    }

    private def tripleAtIfActive(
      ordinal: SnapshotOrdinal,
      entries: Map[Hex, Array[Byte]]
    ): F[Option[Response[F]]] =
      fileStorage.getAsValidatedHttpResponse(ordinal, validateActiveCombined(ordinal), appendEntries(entries))

    // Highest ordinal at-or-below `ceiling` that is present in BOTH the combined-checkpoint store AND the signed-bytes
    // store. Belt-and-suspenders for `latestMptEntriesResponse`: the resolved checkpoint ordinal's signed bytes are
    // guaranteed present by the signed store's contiguous retention on the HAPPY path, but a node that never staged that
    // exact ordinal (mid-reorg gap) would otherwise 404. Walking down to the latest COMMON ordinal keeps the
    // snapshot↔entries pairing intact (both served from the same ordinal) while letting the fast path still fire. Bounded:
    // checkpoint ordinals are sparse (≤ `maxCheckpointsStored`) and signed ordinals are a contiguous recent window.
    private def latestCommonAtOrBelow(
      byteStore: MptStateStorage[F],
      ceiling: SnapshotOrdinal
    ): F[Option[SnapshotOrdinal]] =
      (
        fileStorage.listStoredOrdinals.flatMap(_.compile.toList),
        byteStore.listStoredOrdinals
      ).tupled.flatMap {
        case (checkpointOrdinals, signedOrdinals) =>
          val signedSet = signedOrdinals.toSet
          val latestCommon = checkpointOrdinals
            .filter(o => o.value.value <= ceiling.value.value && signedSet.contains(o))
            .maxOption
          latestCommon.pure[F]
      }

    def latestMptEntriesResponse: F[Option[Response[F]]] =
      mptStateStorage match {
        case None => Option.empty[Response[F]].pure[F]
        case Some(byteStore) =>
          finalityGate.finalizedOrdinal.flatMap {
            case None            => Option.empty[Response[F]].pure[F]
            case Some(finalized) =>
              // Resolve the servable checkpoint ordinal IDENTICALLY to `/latest/combined`
              // (`getLatestOrdinalAtOrBelow(finalized)`), then build the triple there.
              fileStorage.getLatestOrdinalAtOrBelow(finalized).flatMap {
                case None => Option.empty[Response[F]].pure[F]
                case Some(servableOrdinal) =>
                  byteStore.readState(servableOrdinal).flatMap {
                    case Some(entries) => tripleAtIfActive(servableOrdinal, entries)
                    case None          =>
                      // A present selected checkpoint that is forbidden or corrupt cannot be masked by an older checkpoint merely
                      // because its sibling byte image is missing.
                      fileStorage
                        .validateExact(servableOrdinal, validateActiveCombined(servableOrdinal))
                        .ifM(
                          latestCommonAtOrBelow(byteStore, servableOrdinal).flatMap {
                            case Some(commonOrdinal) if commonOrdinal =!= servableOrdinal =>
                              byteStore
                                .readState(commonOrdinal)
                                .flatMap(_.traverse(entries => tripleAtIfActive(commonOrdinal, entries)).map(_.flatten))
                            case _ => Option.empty[Response[F]].pure[F]
                          },
                          Option.empty[Response[F]].pure[F]
                        )
                  }
              }
          }
      }

    // Signed-byte-store backfill serve side: the exact-ordinal read. Finality-gated (belt-and-braces — the signed store only ever
    // holds finalized-branch bytes) and `None` on a hole or a reader without a byte store, so a puller's 404 is indistinguishable
    // from "this peer can't serve it" and it simply tries the next peer.
    def mptEntriesAt(ordinal: SnapshotOrdinal): F[Option[Response[F]]] =
      mptStateStorage match {
        case None => Option.empty[Response[F]].pure[F]
        case Some(byteStore) =>
          finalityGate.isServable(ordinal).flatMap {
            case false => Option.empty[Response[F]].pure[F]
            case true =>
              activeEraSnapshotAt(ordinal).ifM(
                byteStore
                  .readState(ordinal)
                  .map(_.map(entries => respOf(entries.asJson(MptStateStorage.mptEntriesEncoder)))),
                Option.empty[Response[F]].pure[F]
              )
          }
      }

    def latestCheckpointInfo: F[Option[LastCheckpointInfo]] =
      fileStorage.getLatestCheckpointInfo.flatMap { info =>
        (finalityGate.isServable(info.ordinal), fileStorage.validateExact(info.ordinal, validateActiveCombined(info.ordinal))).mapN(
          (servable, active) => if (servable && active) info.some else None
        )
      }

    def combinedCheckpointAt(ordinal: SnapshotOrdinal): F[Option[Response[F]]] =
      finalityGate.isServable(ordinal).flatMap {
        case false => Option.empty[Response[F]].pure[F]
        case true  => combinedAtIfActive(ordinal)
      }
  }

  /** 3c-A GLOBAL wiring helper. Builds a READ-ONLY `MptStateStorage` at the given `mptSnapshotInfoPath` and constructs the Nakamoto reader
    * with it so `/latest/combined/mpt-entries` can serve the claimed signed byte map. gl0 passes the SIGNED byte store (`<...>_signed`) the
    * finalize sink in `GlobalSnapshotConsensus` writes; consumers must still authenticate the snapshot and root-check the served bytes.
    * Effectful only because `MptStateStorage.make` creates/validates the directory; read-only access to the finality-gated files is
    * race-free with the sink's writes. Use this at the gl0 HTTP wiring site; non-global Nakamoto readers keep using [[nakamoto]] (which
    * defaults `mptStateStorage = None`, so their `mpt-entries` route 404s).
    */
  def nakamotoF[F[_]: Async: JsonSerializer, S <: Snapshot: Decoder, SI <: SnapshotInfo[_]](
    finalityGate: FinalityGate[F],
    fileStorage: CombinedSnapshotCheckpointFileSystemStorage[F, S, SI],
    activeEraSnapshotAt: SnapshotOrdinal => F[Boolean],
    mptSnapshotInfoPath: Path
  ): F[FinalizedSnapshotReader[F, S, SI]] =
    MptStateStorage.make[F](mptSnapshotInfoPath).map { byteStore =>
      nakamoto[F, S, SI](finalityGate, fileStorage, activeEraSnapshotAt, byteStore.some)
    }
}
